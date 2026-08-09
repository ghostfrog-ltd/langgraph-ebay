"""
Apply targeted unknown-listing finalization using spare eBay Trading API calls.

This module is intentionally narrower than the normal ended-listing pipeline:
it only considers candidates selected by the read-only Point Zero planner, and
it only mutates rows that are still `status = 'unknown'` and not finalized. Dry
run is the default so operators can review candidate quality before spending
API calls or changing PostgreSQL state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from typing import Any, Optional

import requests
from psycopg2.extras import execute_batch

from pipelines.listing.ended.graph import (
    TRADING_USAGE_LIMIT_ERROR_CODES,
    _call_trading_get_item,
    _extract_numeric_item_id,
    _parse_trading_get_item,
)
from pipelines.listing.point_zero.backfill_plan import build_plan, ensure_backfill_tracking_columns
from utils.db_schema import get_fresh_connection
from utils.logger import get_logger
from utils.usage_tracker import increment_api_usage

logger = get_logger(__name__)

BACKFILL_LOCK_KEY = 260531001
DEFAULT_DAILY_BACKFILL_BUDGET = 1000
DEFAULT_BACKFILL_BATCH_SIZE = 100
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
    """Count local unknown-listing final-price checks already attempted today."""
    conn = get_fresh_connection()
    try:
        with conn.cursor() as cur:
            ensure_backfill_tracking_columns(cur)
            cur.execute(
                """
                SELECT GREATEST(
                    (
                        SELECT COUNT(*)::int
                        FROM auction_listings
                        WHERE backfill_checked_at::date = CURRENT_DATE
                    ),
                    (
                        SELECT COALESCE(SUM(call_count), 0)::int
                        FROM api_usage
                        WHERE date = CURRENT_DATE
                          AND service = 'ebay_ended_v1'
                    )
                ) AS checked_today
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
    Result for one targeted unknown-listing finalization candidate.

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


def _load_candidates_from_plan(
    *,
    limit: int,
    daily_cap: int,
    safety_reserve: int,
    include_vehicle_sources: bool,
    recommended_backfill_override: int | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Ask the read-only Point Zero planner for the exact unknown rows to process.

    The planner already applies the API budget, excludes broad/unsafe model
    keys, and filters obvious parts/accessories. This runner does not select
    its own arbitrary unknown rows.
    """
    plan = build_plan(
        daily_cap=daily_cap,
        safety_reserve=safety_reserve,
        max_backfill_per_day=limit,
        candidate_detail_limit=limit,
        include_vehicle_sources=include_vehicle_sources,
        recommended_backfill_override=recommended_backfill_override,
    )
    candidates = list(plan.get("recommended_unknown_candidate_preview") or [])
    return candidates[:limit], plan


def _apply_outcomes(outcomes: list[BackfillOutcome]) -> int:
    """
    Persist finalization outcomes to PostgreSQL.

    `finalize` outcomes change listing status/final price. `delete` outcomes
    remove local junk rows that eBay confirms are inaccessible/deleted and have
    no final price. `restore` outcomes move rows back to live when Trading says
    they are not ended. Skipped/error outcomes leave commercial state untouched.
    All checked rows receive local backfill audit fields so the planner does
    not spend repeated API calls on the same uncertain listing every run.
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
                      AND status = 'unknown'
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
                      AND status = 'unknown'
                      AND COALESCE(finalized, FALSE) = FALSE
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
                      AND status = 'unknown'
                      AND COALESCE(finalized, FALSE) = FALSE
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
                      AND COALESCE(finalized, FALSE) = FALSE
                      AND final_price IS NULL
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
                      AND status = 'unknown'
                      AND COALESCE(finalized, FALSE) = FALSE
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
    daily_cap: int = 5000,
    safety_reserve: int = 500,
    daily_backfill_budget: int = DEFAULT_DAILY_BACKFILL_BUDGET,
    include_vehicle_sources: bool = False,
) -> dict[str, Any]:
    """
    Run a targeted unknown-listing final-price batch.

    Dry-run mode returns planner candidates without spending API calls. Apply
    mode performs read-only Trading API calls and then updates only selected
    unknown PostgreSQL rows to `live`, `sold`, `ended`, or local deletion.
    """
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

    logger.info("[point_zero.backfill_apply] planning candidates limit=%d", effective_limit)
    candidates, plan = _load_candidates_from_plan(
        limit=effective_limit,
        daily_cap=planner_daily_cap,
        safety_reserve=safety_reserve,
        include_vehicle_sources=include_vehicle_sources,
        recommended_backfill_override=effective_limit,
    )
    logger.info(
        "[point_zero.backfill_apply] planned candidates=%d shortfall=%d reason=%s",
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
    remaining_trading_calls = remaining_backfill_budget
    stop_reason: str | None = None
    total_candidates = len(candidates)
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
            outcomes.append(
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
            outcomes.append(
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
            outcomes.append(
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
            outcomes.append(
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
            outcomes.append(
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
            outcomes.append(
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
        outcomes.append(
            _classify_trading_snapshot(
                listing_id=listing_id,
                external_id=external_id,
                source=source,
                model_key=model_key,
                title=title,
                parsed=parsed,
            )
        )

    applied = _apply_outcomes(outcomes)
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
        "candidate_shortfall": max(effective_limit - len(candidates), 0),
        "candidate_shortfall_reason": plan.get("candidate_shortfall_reason"),
        "outcome_count": len(outcomes),
        "status": "stopped" if stop_reason else "completed",
        "stop_reason": stop_reason,
        "applied": applied,
        "counts": counts,
        "outcomes": [asdict(outcome) for outcome in outcomes],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply a targeted unknown-listing final-price batch.")
    parser.add_argument("--limit", type=int, default=DEFAULT_BACKFILL_BATCH_SIZE)
    parser.add_argument("--daily-cap", type=int, default=5000)
    parser.add_argument("--safety-reserve", type=int, default=500)
    parser.add_argument(
        "--daily-limit",
        "--daily-backfill-budget",
        dest="daily_limit",
        type=int,
        default=DEFAULT_DAILY_BACKFILL_BUDGET,
        help="Maximum local unknown-listing final-price checks this worker may spend per day.",
    )
    parser.add_argument(
        "--include-vehicle-sources",
        action="store_true",
        help="Include vehicle/parts-heavy sources that the planner excludes by default.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Spend Trading API calls and update unknown rows. Omit for dry-run.",
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
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
