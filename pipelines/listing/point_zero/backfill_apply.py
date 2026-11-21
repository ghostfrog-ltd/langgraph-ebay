"""
Apply historic-listing finalization using eBay Trading API calls.

This module is intentionally narrower than the normal ended-listing pipeline:
it checks past-end rows that lack both a confirmed final price and a conclusive
Trading audit, regardless of their legacy local status. The apply path drains
that queue in `auction_listings.id` order so catch-up work is predictable.
"""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from typing import Any, Optional

import requests
from psycopg2.extras import RealDictCursor, execute_batch

from pipelines.listing.ended.graph import (
    TRADING_USAGE_LIMIT_ERROR_CODES,
    _call_trading_get_item,
    _extract_numeric_item_id,
    _parse_trading_get_item,
)
from pipelines.listing.point_zero.backfill_plan import ensure_backfill_tracking_columns
from utils.db_schema import get_fresh_connection
from utils.ebay_api_accounting import get_bucket_limit
from utils.logger import get_logger
from utils.usage_tracker import increment_api_usage

logger = get_logger(__name__)

BACKFILL_LOCK_KEY = 260531001
DEFAULT_DAILY_BACKFILL_BUDGET = 1000
DEFAULT_BACKFILL_BATCH_SIZE = 100
BACKFILL_COMMIT_CHUNK_SIZE = 50
TRADING_DELETE_CODES = {"17", "1505", "21920397"}


def _trading_usage_limit_reason(parsed: Optional[dict[str, Any]]) -> str | None:
    if not parsed:
        return None
    code = str(parsed.get("code") or "")
    if code in TRADING_USAGE_LIMIT_ERROR_CODES:
        return f"trading_usage_limit_{code}"
    return None


def _try_acquire_lock(conn) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (BACKFILL_LOCK_KEY,))
        row = cur.fetchone()
    conn.commit()
    return bool(row and row[0])


def _release_lock(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (BACKFILL_LOCK_KEY,))
    conn.commit()


def is_backfill_running() -> bool:
    """
    Return True when another PostgreSQL session holds the backfill worker lock.

    Heartbeat uses this as a cheap pre-spawn check. The worker still acquires
    the same lock itself, so this check is only an optimisation, not the safety
    boundary.
    """
    conn = get_fresh_connection()
    try:
        acquired = _try_acquire_lock(conn)
        if acquired:
            _release_lock(conn)
        return not acquired
    finally:
        conn.close()


def backfill_checks_today() -> int:
    """Count Trading final-price calls already spent today."""
    conn = get_fresh_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(SUM(call_count), 0)::int AS checked_today
                FROM api_usage
                WHERE date = CURRENT_DATE
                  AND service = 'ebay_ended_v1'
                """
            )
            row = cur.fetchone()
        conn.commit()
    finally:
        conn.close()
    return int((row or [0])[0] or 0)


@dataclass
class BackfillOutcome:
    """
    Result for one historic-listing finalization candidate.

    `new_status` is intentionally narrow:
    - `sold` means Trading returned an ended listing with one or more bids and a final price.
    - `ended` means Trading confirmed no sale.
    - `deleted` means eBay says the item is not recoverable and the local row should be removed.
    - `live` means Trading says the listing is still open and should be restored.
    - `skipped` means the listing should remain unchanged for now.
    - `error` means the candidate failed in a way that needs operator review.
    """

    listing_id: int
    external_id: str
    source: str
    model_key: str
    title: str
    action: str
    new_status: str
    final_price: Optional[float]
    bid_count: int
    reason: str
    ebay_end_time: Optional[str] = None


def _classify_trading_snapshot(
    *,
    listing_id: int,
    external_id: str,
    source: str,
    model_key: str,
    title: str,
    parsed: Optional[dict[str, Any]],
) -> BackfillOutcome:
    """
    Convert a parsed Trading GetItem response into a database-safe outcome.

    This mirrors the existing ended pipeline: bid_count > 0 with a usable
    final price becomes `sold`; confirmed ended/no bids becomes `ended`; item
    not found is deleted locally because it cannot produce confirmed sale
    evidence; not-ended rows are restored to live.
    """
    if not parsed:
        return BackfillOutcome(
            listing_id=listing_id,
            external_id=external_id,
            source=source,
            model_key=model_key,
            title=title,
            action="skip",
            new_status="error",
            final_price=None,
            bid_count=0,
            reason="unparseable_trading_response",
        )

    code = str(parsed.get("code") or "")
    if code in TRADING_DELETE_CODES:
        return BackfillOutcome(
            listing_id=listing_id,
            external_id=external_id,
            source=source,
            model_key=model_key,
            title=title,
            action="delete",
            new_status="deleted",
            final_price=None,
            bid_count=0,
            reason=f"trading_item_not_found_{code}",
        )

    if code:
        return BackfillOutcome(
            listing_id=listing_id,
            external_id=external_id,
            source=source,
            model_key=model_key,
            title=title,
            action="skip",
            new_status="error",
            final_price=None,
            bid_count=0,
            reason=f"trading_error_{code}",
        )

    if not bool(parsed.get("ended")):
        return BackfillOutcome(
            listing_id=listing_id,
            external_id=external_id,
            source=source,
            model_key=model_key,
            title=title,
            action="restore",
            new_status="live",
            final_price=None,
            bid_count=int(parsed.get("bid_count") or 0),
            reason="trading_says_not_ended",
            ebay_end_time=parsed.get("ebay_end_time"),
        )

    bid_count = int(parsed.get("bid_count") or 0)
    quantity_sold = int(parsed.get("quantity_sold") or 0)
    sold_flag = bool(parsed.get("sold_flag"))
    raw_final_price = parsed.get("final_price")
    final_price = float(raw_final_price) if sold_flag and raw_final_price is not None else None

    if final_price is not None:
        if bid_count > 0:
            reason = "ended_with_bids"
        elif quantity_sold > 0:
            reason = "fixed_price_quantity_sold"
        else:
            reason = "ended_with_confirmed_sale"
        return BackfillOutcome(
            listing_id=listing_id,
            external_id=external_id,
            source=source,
            model_key=model_key,
            title=title,
            action="finalize",
            new_status="sold",
            final_price=final_price,
            bid_count=bid_count,
            reason=reason,
        )

    return BackfillOutcome(
        listing_id=listing_id,
        external_id=external_id,
        source=source,
        model_key=model_key,
        title=title,
        action="finalize",
        new_status="ended",
        final_price=None,
        bid_count=bid_count,
        reason="ended_without_confirmed_sale",
    )


def _temporary_trading_failure_outcome(
    *,
    listing_id: int,
    external_id: str,
    source: str,
    model_key: str,
    title: str,
    reason: str,
) -> BackfillOutcome:
    """Represent a retryable Trading API failure without changing listing state."""

    return BackfillOutcome(
        listing_id=listing_id,
        external_id=external_id,
        source=source,
        model_key=model_key,
        title=title,
        action="skip",
        new_status="error",
        final_price=None,
        bid_count=0,
        reason=reason,
    )


def _fetch_table_order_backfill_candidates(cur, *, limit: int) -> list[dict[str, Any]]:
    """
    Fetch old listings that have never had a conclusive final-price check.

    This is deliberately plain: no source priority, no comp-demand priority, no
    planner exclusions, and no reliance on the local status label. A historic
    `ended` or `live` label is not proof that Trading checked the outcome. Rows
    with a conclusive recorded check are excluded so genuine no-sales are not
    queried forever. Retryable API failures become eligible again after a day.
    """
    if limit <= 0:
        return []
    select_columns = """
        SELECT
            id,
            external_id,
            source,
            status AS prior_status,
            finalized AS prior_finalized,
            COALESCE(NULLIF(model_key, ''), 'unknown') AS model_key,
            title,
            price_current,
            bids_count,
            price_bid_current,
            sale_type,
            end_time,
            last_seen_at,
            backfill_checked_at,
            backfill_check_count,
            backfill_last_reason
        FROM auction_listings
    """
    common_where = """
        WHERE (final_price IS NULL OR final_price <= 0)
          AND end_time IS NOT NULL
          AND end_time <= (now() AT TIME ZONE 'utc') - interval '1 day'
          AND external_id ~ '[0-9]'
    """

    rows: list[dict[str, Any]] = []
    cur.execute(
        f"""
        {select_columns}
        {common_where}
          AND backfill_checked_at IS NULL
        ORDER BY id ASC
        LIMIT %s
        """,
        (limit,),
    )
    rows.extend(dict(row) for row in (cur.fetchall() or []))

    remaining = limit - len(rows)
    if remaining <= 0:
        return rows

    cur.execute(
        f"""
        {select_columns}
        {common_where}
          AND backfill_checked_at IS NOT NULL
          AND backfill_checked_at <= CURRENT_TIMESTAMP - interval '1 day'
          AND NOT (status = 'ended' AND COALESCE(finalized, FALSE) = TRUE)
          AND COALESCE(backfill_last_reason, '') <> 'invalid_external_id'
        ORDER BY id ASC
        LIMIT %s
        """,
        (remaining,),
    )
    rows.extend(dict(row) for row in (cur.fetchall() or []))
    return rows


def _load_candidates_in_table_order(*, limit: int) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Load backfill candidates directly from PostgreSQL in `auction_listings.id` order."""
    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_backfill_tracking_columns(cur)
            candidates = _fetch_table_order_backfill_candidates(cur, limit=limit)
        conn.commit()
    finally:
        conn.close()

    shortfall = max(limit - len(candidates), 0)
    return candidates, {
        "candidate_limit_source": "operator_limit",
        "candidate_shortfall_reason": "direct_backfill_selector_returned_fewer_rows_than_requested"
        if shortfall > 0
        else None,
        "selection": "table_order_unchecked_past_end_missing_final_price",
    }


def _apply_outcomes(outcomes: list[BackfillOutcome]) -> int:
    """
    Persist finalization outcomes to PostgreSQL.

    `finalize` outcomes change listing status/final price. `delete` outcomes
    remove local junk rows that eBay confirms are inaccessible/deleted and have
    no final price. `restore` outcomes move rows back to live when Trading says
    they are not ended. Skipped/error outcomes leave commercial state untouched.
    All checked rows receive local backfill audit fields for operator visibility.
    """
    checked_rows = [
        (outcome.reason, outcome.listing_id)
        for outcome in outcomes
        if outcome.action in {"finalize", "skip", "delete", "restore"}
        and outcome.reason != "daily_trading_budget_exhausted"
    ]
    sold_rows = [
        (outcome.final_price, outcome.bid_count, outcome.listing_id)
        for outcome in outcomes
        if outcome.action == "finalize" and outcome.new_status == "sold"
    ]
    ended_rows = [
        (outcome.bid_count, outcome.listing_id)
        for outcome in outcomes
        if outcome.action == "finalize" and outcome.new_status == "ended"
    ]
    delete_rows = [
        (outcome.external_id, outcome.listing_id)
        for outcome in outcomes
        if outcome.action == "delete" and outcome.new_status == "deleted"
    ]
    restore_rows = [
        (outcome.bid_count, outcome.ebay_end_time, outcome.listing_id)
        for outcome in outcomes
        if outcome.action == "restore" and outcome.new_status == "live"
    ]

    if not checked_rows and not sold_rows and not ended_rows and not delete_rows and not restore_rows:
        return 0

    conn = get_fresh_connection()
    try:
        with conn.cursor() as cur:
            ensure_backfill_tracking_columns(cur)
            if checked_rows:
                execute_batch(
                    cur,
                    """
                    UPDATE auction_listings
                    SET backfill_checked_at = CURRENT_TIMESTAMP,
                        backfill_check_count = COALESCE(backfill_check_count, 0) + 1,
                        backfill_last_reason = %s
                    WHERE id = %s
                      AND (final_price IS NULL OR final_price <= 0)
                    """,
                    checked_rows,
                    page_size=100,
                )
            if sold_rows:
                execute_batch(
                    cur,
                    """
                    UPDATE auction_listings
                    SET finalized = TRUE,
                        status = 'sold',
                        final_price = %s,
                        bids_count = %s
                    WHERE id = %s
                      AND (final_price IS NULL OR final_price <= 0)
                    """,
                    sold_rows,
                    page_size=100,
                )
            if ended_rows:
                execute_batch(
                    cur,
                    """
                    UPDATE auction_listings
                    SET finalized = TRUE,
                        status = 'ended',
                        bids_count = %s
                    WHERE id = %s
                      AND (final_price IS NULL OR final_price <= 0)
                    """,
                    ended_rows,
                    page_size=100,
                )
            if delete_rows:
                execute_batch(
                    cur,
                    """
                    DELETE FROM auction_price_history
                    WHERE external_id = %s
                    """,
                    [(external_id,) for external_id, _listing_id in delete_rows],
                    page_size=100,
                )
                execute_batch(
                    cur,
                    """
                    DELETE FROM auction_listings
                    WHERE id = %s
                      AND external_id = %s
                      AND (final_price IS NULL OR final_price <= 0)
                    """,
                    [(listing_id, external_id) for external_id, listing_id in delete_rows],
                    page_size=100,
                )
            if restore_rows:
                execute_batch(
                    cur,
                    """
                    UPDATE auction_listings
                    SET status = 'live',
                        finalized = FALSE,
                        bids_count = %s,
                        end_time = COALESCE(%s, end_time)
                    WHERE id = %s
                      AND (final_price IS NULL OR final_price <= 0)
                    """,
                    restore_rows,
                    page_size=100,
                )
        conn.commit()
    finally:
        conn.close()

    return len(sold_rows) + len(ended_rows) + len(delete_rows) + len(restore_rows)


def run_backfill(
    *,
    limit: int,
    apply: bool,
    daily_cap: int | None = None,
    safety_reserve: int = 500,
    daily_backfill_budget: int = DEFAULT_DAILY_BACKFILL_BUDGET,
    include_vehicle_sources: bool = False,
) -> dict[str, Any]:
    """
    Run a historic-listing final-price batch.

    Dry-run mode returns the direct table-order candidates without spending API
    calls. Apply mode performs read-only Trading API calls and then updates only
    selected PostgreSQL rows to `live`, `sold`, `ended`, or local deletion.
    """
    daily_cap = get_bucket_limit("trading") if daily_cap is None else int(daily_cap)
    logger.info(
        "[point_zero.backfill_apply] start mode=%s limit=%d daily_backfill_budget=%d daily_cap=%d safety_reserve=%d",
        "apply" if apply else "dry_run",
        limit,
        daily_backfill_budget,
        daily_cap,
        safety_reserve,
    )
    checked_today = backfill_checks_today()
    remaining_backfill_budget = max(int(daily_backfill_budget) - checked_today, 0)
    effective_limit = min(limit, remaining_backfill_budget)
    planner_daily_cap = daily_cap
    logger.info(
        "[point_zero.backfill_apply] budget checked_today=%d remaining=%d effective_limit=%d",
        checked_today,
        remaining_backfill_budget,
        effective_limit,
    )

    logger.info("[point_zero.backfill_apply] loading table-order candidates limit=%d", effective_limit)
    candidates, plan = _load_candidates_in_table_order(limit=effective_limit)
    logger.info(
        "[point_zero.backfill_apply] loaded candidates=%d shortfall=%d reason=%s",
        len(candidates),
        max(effective_limit - len(candidates), 0),
        plan.get("candidate_shortfall_reason"),
    )

    if not apply:
        return {
            "mode": "dry_run",
            "limit": limit,
            "effective_limit": effective_limit,
            "budget_shortfall": max(limit - effective_limit, 0),
            "candidate_count": len(candidates),
            "candidate_limit_source": plan.get("candidate_limit_source"),
            "selection": plan.get("selection"),
            "candidate_shortfall": max(effective_limit - len(candidates), 0),
            "candidate_shortfall_reason": plan.get("candidate_shortfall_reason"),
            "daily_backfill_budget": daily_backfill_budget,
            "planner_daily_cap": planner_daily_cap,
            "backfill_checked_today": checked_today,
            "remaining_backfill_budget": remaining_backfill_budget,
            "api_budget": plan.get("api_budget") or {},
            "candidates": candidates,
        }

    outcomes: list[BackfillOutcome] = []
    pending_outcomes: list[BackfillOutcome] = []
    applied = 0
    remaining_trading_calls = remaining_backfill_budget
    total_candidates = len(candidates)
    progress_started_at = time.monotonic()
    progress_counts = {
        "sold": 0,
        "ended": 0,
        "deleted": 0,
        "restored": 0,
        "errors": 0,
    }

    def record_outcome(outcome: BackfillOutcome) -> None:
        nonlocal applied
        outcomes.append(outcome)
        pending_outcomes.append(outcome)
        if outcome.action == "finalize" and outcome.new_status == "sold":
            progress_counts["sold"] += 1
        elif outcome.action == "finalize" and outcome.new_status == "ended":
            progress_counts["ended"] += 1
        elif outcome.action == "delete":
            progress_counts["deleted"] += 1
        elif outcome.action == "restore":
            progress_counts["restored"] += 1
        else:
            progress_counts["errors"] += 1

        processed = len(outcomes)
        if processed <= 5 or processed % 25 == 0 or processed == total_candidates:
            logger.info(
                "[point_zero.backfill_apply] resolved=%d/%d sold=%d ended=%d deleted=%d restored=%d errors=%d remaining_calls=%d elapsed=%.0fs",
                processed,
                total_candidates,
                progress_counts["sold"],
                progress_counts["ended"],
                progress_counts["deleted"],
                progress_counts["restored"],
                progress_counts["errors"],
                remaining_trading_calls,
                time.monotonic() - progress_started_at,
            )
        if len(pending_outcomes) >= BACKFILL_COMMIT_CHUNK_SIZE:
            applied += _apply_outcomes(pending_outcomes)
            pending_outcomes.clear()

    stop_reason: str | None = None
    for index, row in enumerate(candidates, start=1):
        if index == 1 or index % 50 == 0 or index == total_candidates:
            logger.info(
                "[point_zero.backfill_apply] progress processing=%d/%d remaining_calls=%d",
                index,
                total_candidates,
                remaining_trading_calls,
            )
        listing_id = int(row.get("id") or 0)
        external_id = str(row.get("external_id") or "")
        source = str(row.get("source") or "")
        model_key = str(row.get("model_key") or "")
        title = str(row.get("title") or "")

        item_id = _extract_numeric_item_id(external_id)
        if not item_id:
            record_outcome(
                BackfillOutcome(
                    listing_id=listing_id,
                    external_id=external_id,
                    source=source,
                    model_key=model_key,
                    title=title,
                    action="skip",
                    new_status="error",
                    final_price=None,
                    bid_count=0,
                    reason="invalid_external_id",
                )
            )
            continue

        if remaining_trading_calls <= 0:
            record_outcome(
                BackfillOutcome(
                    listing_id=listing_id,
                    external_id=external_id,
                    source=source,
                    model_key=model_key,
                    title=title,
                    action="skip",
                    new_status="skipped",
                    final_price=None,
                    bid_count=0,
                    reason="daily_trading_budget_exhausted",
                )
            )
            continue

        increment_api_usage("ebay_ended_v1")
        remaining_trading_calls -= 1
        try:
            response = _call_trading_get_item(item_id)
        except requests.Timeout as exc:
            logger.warning(
                "[point_zero.backfill_apply] Trading timeout item_id=%s external_id=%s: %s",
                item_id,
                external_id,
                exc,
            )
            record_outcome(
                _temporary_trading_failure_outcome(
                    listing_id=listing_id,
                    external_id=external_id,
                    source=source,
                    model_key=model_key,
                    title=title,
                    reason="trading_api_timeout",
                )
            )
            continue
        except requests.ConnectionError as exc:
            logger.warning(
                "[point_zero.backfill_apply] Trading connection error item_id=%s external_id=%s: %s",
                item_id,
                external_id,
                exc,
            )
            record_outcome(
                _temporary_trading_failure_outcome(
                    listing_id=listing_id,
                    external_id=external_id,
                    source=source,
                    model_key=model_key,
                    title=title,
                    reason="trading_api_connection_error",
                )
            )
            continue
        except requests.RequestException as exc:
            logger.warning(
                "[point_zero.backfill_apply] Trading request error item_id=%s external_id=%s: %s",
                item_id,
                external_id,
                exc,
            )
            record_outcome(
                _temporary_trading_failure_outcome(
                    listing_id=listing_id,
                    external_id=external_id,
                    source=source,
                    model_key=model_key,
                    title=title,
                    reason="trading_api_request_error",
                )
            )
            continue
        parsed = _parse_trading_get_item(response.text)
        usage_limit_reason = _trading_usage_limit_reason(parsed)
        if usage_limit_reason:
            logger.warning(
                "[point_zero.backfill_apply] Trading usage limit reached item_id=%s external_id=%s reason=%s; stopping batch",
                item_id,
                external_id,
                usage_limit_reason,
            )
            record_outcome(
                _temporary_trading_failure_outcome(
                    listing_id=listing_id,
                    external_id=external_id,
                    source=source,
                    model_key=model_key,
                    title=title,
                    reason=usage_limit_reason,
                )
            )
            stop_reason = usage_limit_reason
            break
        record_outcome(
            _classify_trading_snapshot(
                listing_id=listing_id,
                external_id=external_id,
                source=source,
                model_key=model_key,
                title=title,
                parsed=parsed,
            )
        )

    if pending_outcomes:
        applied += _apply_outcomes(pending_outcomes)
    counts: dict[str, int] = {}
    for outcome in outcomes:
        key = f"{outcome.action}:{outcome.new_status}:{outcome.reason}"
        counts[key] = counts.get(key, 0) + 1

    logger.info(
        "[point_zero.backfill_apply] candidates=%d outcomes=%d applied=%d",
        len(candidates),
        len(outcomes),
        applied,
    )

    return {
        "mode": "apply",
        "limit": limit,
        "effective_limit": effective_limit,
        "budget_shortfall": max(limit - effective_limit, 0),
        "daily_backfill_budget": daily_backfill_budget,
        "planner_daily_cap": planner_daily_cap,
        "backfill_checked_today": checked_today,
        "remaining_backfill_budget": remaining_backfill_budget,
        "candidate_count": len(candidates),
        "candidate_limit_source": plan.get("candidate_limit_source"),
        "selection": plan.get("selection"),
        "candidate_shortfall": max(effective_limit - len(candidates), 0),
        "candidate_shortfall_reason": plan.get("candidate_shortfall_reason"),
        "outcome_count": len(outcomes),
        "status": "stopped" if stop_reason else "completed",
        "stop_reason": stop_reason,
        "applied": applied,
        "counts": counts,
        "outcomes": [asdict(outcome) for outcome in outcomes],
    }


def _summary_for_cli(summary: dict[str, Any], *, include_outcomes: bool = False) -> dict[str, Any]:
    """Keep terminal output readable for large backfill batches."""
    if include_outcomes:
        return summary
    compact = dict(summary)
    candidates = list(compact.pop("candidates", []) or [])
    compact["sample_candidates"] = candidates[:20]
    compact["sample_candidate_count"] = min(len(candidates), 20)
    outcomes = list(compact.pop("outcomes", []) or [])
    compact["sample_outcomes"] = outcomes[:20]
    compact["sample_outcome_count"] = min(len(outcomes), 20)
    return compact


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply a historic-listing final-price batch in table order.")
    parser.add_argument("--limit", type=int, default=DEFAULT_BACKFILL_BATCH_SIZE)
    parser.add_argument("--daily-cap", type=int, default=get_bucket_limit("trading"))
    parser.add_argument("--safety-reserve", type=int, default=500)
    parser.add_argument(
        "--daily-limit",
        "--daily-backfill-budget",
        dest="daily_limit",
        type=int,
        default=DEFAULT_DAILY_BACKFILL_BUDGET,
        help="Maximum local final-price checks this worker may spend per day.",
    )
    parser.add_argument(
        "--include-vehicle-sources",
        action="store_true",
        help="Compatibility option; the direct table-order queue already includes all sources.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Spend Trading API calls and update eligible past-end rows. Omit for dry-run.",
    )
    parser.add_argument(
        "--verbose-outcomes",
        action="store_true",
        help="Print every per-listing outcome in the final JSON. Default prints a compact summary.",
    )
    args = parser.parse_args()

    lock_conn = get_fresh_connection()
    if not _try_acquire_lock(lock_conn):
        logger.info("[point_zero.backfill_apply] another backfill worker is already running; exiting")
        print(
            json.dumps(
                {
                    "mode": "apply" if args.apply else "dry_run",
                    "status": "skipped_already_running",
                    "limit": max(args.limit, 0),
                },
                indent=2,
                default=str,
            )
        )
        lock_conn.close()
        return

    try:
        summary = run_backfill(
            limit=max(args.limit, 0),
            apply=bool(args.apply),
            daily_cap=args.daily_cap,
            safety_reserve=args.safety_reserve,
            daily_backfill_budget=max(args.daily_limit, 0),
            include_vehicle_sources=args.include_vehicle_sources,
        )
    finally:
        _release_lock(lock_conn)
        lock_conn.close()
    print(json.dumps(_summary_for_cli(summary, include_outcomes=args.verbose_outcomes), indent=2, default=str))


if __name__ == "__main__":
    main()
