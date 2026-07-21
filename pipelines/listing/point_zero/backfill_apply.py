"""
Apply targeted stale-listing finalization using spare eBay Trading API calls.

This module is intentionally narrower than the normal ended-listing pipeline:
it only considers candidates selected by the read-only Point Zero planner, and
it only mutates rows that are still `status = 'stale'` and not finalized. Dry
run is the default so operators can review candidate quality before spending
API calls or changing PostgreSQL state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from typing import Any, Optional

from psycopg2.extras import execute_batch

from pipelines.listing.ended.graph import (
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
    """Count local stale-finalization checks already attempted today."""
    conn = get_fresh_connection()
    try:
        with conn.cursor() as cur:
            ensure_backfill_tracking_columns(cur)
            cur.execute(
                """
                SELECT COUNT(*)::int
                FROM auction_listings
                WHERE backfill_checked_at::date = CURRENT_DATE
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
    Result for one targeted stale-finalization candidate.

    `new_status` is intentionally narrow:
    - `sold` means Trading returned an ended listing with one or more bids and a final price.
    - `ended` means Trading confirmed no sale, or the item can no longer be fetched.
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
    not found is also finalized as `ended` because it cannot produce confirmed
    sale evidence.
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

    if parsed.get("code") in {"17", "1505", "21920397"}:
        return BackfillOutcome(
            listing_id=listing_id,
            external_id=external_id,
            source=source,
            model_key=model_key,
            title=title,
            action="finalize",
            new_status="ended",
            final_price=None,
            bid_count=0,
            reason=f"trading_item_not_found_{parsed.get('code')}",
        )

    if not bool(parsed.get("ended")):
        return BackfillOutcome(
            listing_id=listing_id,
            external_id=external_id,
            source=source,
            model_key=model_key,
            title=title,
            action="skip",
            new_status="skipped",
            final_price=None,
            bid_count=int(parsed.get("bid_count") or 0),
            reason="trading_says_not_ended",
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


def _load_candidates_from_plan(
    *,
    limit: int,
    daily_cap: int,
    safety_reserve: int,
    include_vehicle_sources: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """
    Ask the read-only Point Zero planner for the exact stale rows to process.

    The planner already applies the API budget, excludes broad/unsafe model
    keys, and filters obvious parts/accessories. This runner does not select
    its own arbitrary stale rows.
    """
    plan = build_plan(
        daily_cap=daily_cap,
        safety_reserve=safety_reserve,
        max_backfill_per_day=limit,
        candidate_detail_limit=limit,
        include_vehicle_sources=include_vehicle_sources,
    )
    candidates = list(plan.get("recommended_stale_candidate_preview") or [])
    return candidates[:limit], plan


def _apply_outcomes(outcomes: list[BackfillOutcome]) -> int:
    """
    Persist finalization outcomes to PostgreSQL.

    Only `action == "finalize"` outcomes change listing status or final price.
    Skipped/error outcomes leave commercial state untouched. All checked rows
    receive local backfill audit fields so the planner does not spend repeated
    API calls on the same uncertain listing every run.
    """
    checked_rows = [
        (outcome.reason, outcome.listing_id)
        for outcome in outcomes
        if outcome.action in {"finalize", "skip"}
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

    if not checked_rows and not sold_rows and not ended_rows:
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
                      AND status = 'stale'
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
                      AND status = 'stale'
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
                      AND status = 'stale'
                      AND COALESCE(finalized, FALSE) = FALSE
                    """,
                    ended_rows,
                    page_size=100,
                )
        conn.commit()
    finally:
        conn.close()

    return len(sold_rows) + len(ended_rows)


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
    Run a targeted stale-finalization backfill batch.

    Dry-run mode returns planner candidates without spending API calls. Apply
    mode performs read-only Trading API calls and then updates only selected
    stale PostgreSQL rows to `sold` or `ended`.
    """
    checked_today = backfill_checks_today()
    remaining_backfill_budget = max(int(daily_backfill_budget) - checked_today, 0)
    effective_limit = min(limit, remaining_backfill_budget) if apply else limit

    if apply and effective_limit <= 0:
        return {
            "mode": "apply",
            "status": "skipped_daily_backfill_budget_exhausted",
            "limit": limit,
            "daily_backfill_budget": daily_backfill_budget,
            "backfill_checked_today": checked_today,
            "remaining_backfill_budget": 0,
            "candidate_count": 0,
            "outcome_count": 0,
            "applied": 0,
            "counts": {},
            "outcomes": [],
        }

    candidates, plan = _load_candidates_from_plan(
        limit=effective_limit,
        daily_cap=daily_cap,
        safety_reserve=safety_reserve,
        include_vehicle_sources=include_vehicle_sources,
    )

    if not apply:
        return {
            "mode": "dry_run",
            "limit": limit,
            "candidate_count": len(candidates),
            "daily_backfill_budget": daily_backfill_budget,
            "backfill_checked_today": checked_today,
            "remaining_backfill_budget": remaining_backfill_budget,
            "api_budget": plan.get("api_budget") or {},
            "candidates": candidates,
        }

    outcomes: list[BackfillOutcome] = []
    for row in candidates:
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

        increment_api_usage("ebay_ended_v1")
        response = _call_trading_get_item(item_id)
        parsed = _parse_trading_get_item(response.text)
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
        "daily_backfill_budget": daily_backfill_budget,
        "backfill_checked_today": checked_today,
        "remaining_backfill_budget": remaining_backfill_budget,
        "candidate_count": len(candidates),
        "outcome_count": len(outcomes),
        "applied": applied,
        "counts": counts,
        "outcomes": [asdict(outcome) for outcome in outcomes],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Apply a targeted stale-finalization backfill batch.")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--daily-cap", type=int, default=5000)
    parser.add_argument("--safety-reserve", type=int, default=500)
    parser.add_argument(
        "--daily-backfill-budget",
        type=int,
        default=DEFAULT_DAILY_BACKFILL_BUDGET,
        help="Maximum local stale-finalization checks this worker may spend per day.",
    )
    parser.add_argument(
        "--include-vehicle-sources",
        action="store_true",
        help="Include vehicle/parts-heavy sources that the planner excludes by default.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Spend Trading API calls and update stale rows. Omit for dry-run.",
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
            daily_backfill_budget=max(args.daily_backfill_budget, 0),
            include_vehicle_sources=args.include_vehicle_sources,
        )
    finally:
        _release_lock(lock_conn)
        lock_conn.close()
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
