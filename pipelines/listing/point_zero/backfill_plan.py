"""
Plan targeted unknown-listing final-price checks under the daily eBay API budget.

The planner is read-only with respect to marketplace state. Its only database
side effect is ensuring local audit columns exist so apply batches can avoid
rechecking the same unknown rows too frequently.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from typing import Any

from psycopg2.extras import RealDictCursor

from utils.db_schema import get_fresh_connection
from utils.condition import has_investible_product_identity
from utils.ebay_api_accounting import build_api_accounting

DEFAULT_EXCLUDED_BACKFILL_SOURCES = (
    "ebay-honda-msx125",
    "ebay-honda-nc750",
    "ebay-mini-cooper",
    "ebay-motors",
    "motomine",
)

DEFAULT_BACKFILL_SOURCE_PRIORITY = (
    "ebay-apple",
    "ebay-samsung",
    "ebay-watches",
    "ebay-lego",
    "ebay-tools",
    "ebay-actioncams",
    "ebay-headphones",
    "ebay-pokemon",
    "ebay-consoles",
)

BACKFILL_RECHECK_COOLDOWN = "7 days"
BACKFILL_CANDIDATE_PAGE_SIZE = 5000
BACKFILL_MAX_SCAN_ROWS = 250000
BACKFILL_RECENT_ENDED_WINDOW = "30 days"


@dataclass
class ApiBudget:
    """API budget snapshot used to cap targeted backfill work."""

    daily_cap: int
    safety_reserve: int
    max_backfill_per_day: int
    today_calls: int
    avg_total_7d: int
    max_total_7d: int
    planned_normal_calls: int
    theoretical_spare: int
    recommended_backfill_calls: int
    budget_bucket: str


def ensure_backfill_tracking_columns(cur) -> None:
    """
    Create local audit columns used by Point Zero backfill batches.

    These fields do not change marketplace state. They record that an unknown row
    was checked, why it was or was not finalized, and when it can safely be
    considered again without burning the same API call every run.
    """
    cur.execute(
        """
        ALTER TABLE auction_listings
            ADD COLUMN IF NOT EXISTS backfill_checked_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS backfill_check_count INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS backfill_last_reason TEXT
        """
    )


def _fetch_one(cur, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    cur.execute(sql, params)
    row = cur.fetchone()
    return dict(row or {})


def _fetch_all(cur, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    return [dict(row) for row in (cur.fetchall() or [])]


def compute_api_budget(
    cur,
    *,
    daily_cap: int,
    safety_reserve: int,
    max_backfill_per_day: int,
) -> ApiBudget:
    today_rows = _fetch_all(
        cur,
        """
        SELECT service, call_count::int AS call_count
        FROM api_usage
        WHERE date = CURRENT_DATE
        """,
    )
    today_accounting = build_api_accounting(
        {str(row.get("service")): int(row.get("call_count") or 0) for row in today_rows}
    )

    recent_rows = _fetch_all(
        cur,
        """
        SELECT
            date,
            service,
            call_count::int AS call_count
        FROM api_usage
        WHERE date >= CURRENT_DATE - interval '7 days'
        """,
    )

    totals_by_date: dict[Any, dict[str, int]] = defaultdict(dict)
    for row in recent_rows:
        totals_by_date[row.get("date")][str(row.get("service"))] = int(row.get("call_count") or 0)

    recent_trading_totals = [
        int(build_api_accounting(usage)["buckets"]["trading"]["used"])
        for usage in totals_by_date.values()
    ]

    today_calls = int(today_accounting["buckets"]["trading"]["used"])
    avg_total_7d = (
        int(round(sum(recent_trading_totals) / len(recent_trading_totals)))
        if recent_trading_totals
        else 0
    )
    max_total_7d = max(recent_trading_totals) if recent_trading_totals else 0
    planned_normal_calls = max(today_calls, avg_total_7d, max_total_7d)
    theoretical_spare = max(daily_cap - planned_normal_calls - safety_reserve, 0)
    recommended = min(theoretical_spare, max_backfill_per_day)

    return ApiBudget(
        daily_cap=daily_cap,
        safety_reserve=safety_reserve,
        max_backfill_per_day=max_backfill_per_day,
        today_calls=today_calls,
        avg_total_7d=avg_total_7d,
        max_total_7d=max_total_7d,
        planned_normal_calls=planned_normal_calls,
        theoretical_spare=theoretical_spare,
        recommended_backfill_calls=recommended,
        budget_bucket="trading",
    )


def live_unknown_model_key_summary(cur) -> list[dict[str, Any]]:
    return _fetch_all(
        cur,
        """
        SELECT source, COUNT(*)::int AS live_unknown_model_key_count
        FROM auction_listings
        WHERE status = 'live'
          AND (model_key IS NULL OR model_key = '' OR LOWER(model_key) = 'unknown')
        GROUP BY source
        ORDER BY live_unknown_model_key_count DESC, source
        """,
    )


def unknown_backfill_source_summary(cur) -> list[dict[str, Any]]:
    return _fetch_all(
        cur,
        """
        WITH live_keys AS (
            SELECT
                source,
                model_key,
                COALESCE(condition_grade, 'B') AS condition_grade,
                COUNT(*)::int AS live_count
            FROM auction_listings
            WHERE status = 'live'
              AND model_key IS NOT NULL
              AND model_key <> ''
              AND LOWER(model_key) <> 'unknown'
              AND LOWER(model_key) NOT LIKE 'unknown%%'
              AND LOWER(model_key) NOT LIKE '%%generic%%'
            GROUP BY source, model_key, COALESCE(condition_grade, 'B')
        ),
        needed_keys AS (
            SELECT
                lk.source,
                lk.model_key,
                lk.condition_grade,
                lk.live_count,
                COALESCE(c.samples, 0)::int AS comp_samples
            FROM live_keys lk
            LEFT JOIN latest_comps c
              ON c.model_key = lk.model_key
             AND c.condition_grade = lk.condition_grade
            WHERE COALESCE(c.samples, 0) < 3
        ),
        unknown_pool AS (
            SELECT
                l.source,
                l.model_key,
                COALESCE(l.condition_grade, 'B') AS condition_grade,
                COUNT(*)::int AS unknown_count,
                MAX(l.last_seen_at) AS newest_unknown
            FROM auction_listings l
            JOIN needed_keys nk
              ON nk.source = l.source
             AND nk.model_key = l.model_key
             AND nk.condition_grade = COALESCE(l.condition_grade, 'B')
            WHERE l.status = 'unknown'
              AND COALESCE(l.finalized, FALSE) = FALSE
              AND l.end_time <= (now() AT TIME ZONE 'utc') - interval '1 day'
            GROUP BY l.source, l.model_key, COALESCE(l.condition_grade, 'B')
        )
        SELECT
            nk.source,
            COUNT(*)::int AS model_keys_needing_comps,
            SUM(nk.live_count)::int AS live_listings_needing_comps,
            SUM(COALESCE(up.unknown_count, 0))::int AS unknown_backfill_pool,
            MAX(up.newest_unknown) AS newest_unknown
        FROM needed_keys nk
        LEFT JOIN unknown_pool up
          ON up.source = nk.source
         AND up.model_key = nk.model_key
         AND up.condition_grade = nk.condition_grade
        GROUP BY nk.source
        ORDER BY
            CASE nk.source
                WHEN 'ebay-apple' THEN 0
                WHEN 'ebay-samsung' THEN 1
                WHEN 'ebay-watches' THEN 2
                WHEN 'ebay-lego' THEN 3
                WHEN 'ebay-tools' THEN 4
                WHEN 'ebay-actioncams' THEN 5
                WHEN 'ebay-headphones' THEN 6
                WHEN 'ebay-pokemon' THEN 7
                WHEN 'ebay-consoles' THEN 8
                ELSE 99
            END,
            live_listings_needing_comps DESC,
            unknown_backfill_pool DESC
        """,
    )


def unknown_backfill_candidates(
    cur,
    *,
    limit: int,
    excluded_sources: tuple[str, ...],
) -> list[dict[str, Any]]:
    if limit <= 0:
        return []

    # The queue is filtered again by Python product-identity guardrails. Page
    # through the ordered backlog until the requested limit is filled or the
    # bounded SQL candidate set is genuinely exhausted.
    filtered = []
    offset = 0
    page_size = max(BACKFILL_CANDIDATE_PAGE_SIZE, min(limit * 2, 20000))
    max_scan_rows = max(BACKFILL_CANDIDATE_PAGE_SIZE, min(BACKFILL_MAX_SCAN_ROWS, limit * 1000))

    while len(filtered) < limit and offset < max_scan_rows:
        rows = _fetch_unknown_backfill_candidate_page(
            cur,
            limit=min(page_size, max_scan_rows - offset),
            offset=offset,
            excluded_sources=excluded_sources,
        )
        if not rows:
            break

        for row in rows:
            if not has_investible_product_identity(
                str(row.get("title") or ""),
                source=str(row.get("source") or ""),
                model_key=str(row.get("model_key") or ""),
            ):
                continue
            filtered.append(row)
            if len(filtered) >= limit:
                break

        offset += len(rows)

    return filtered


def _fetch_unknown_backfill_candidate_page(
    cur,
    *,
    limit: int,
    offset: int,
    excluded_sources: tuple[str, ...],
) -> list[dict[str, Any]]:
    return _fetch_all(
        cur,
        """
        WITH live_keys AS (
            SELECT
                source,
                model_key,
                COALESCE(condition_grade, 'B') AS condition_grade,
                COUNT(*)::int AS live_count
            FROM auction_listings
            WHERE status = 'live'
              AND model_key IS NOT NULL
              AND model_key <> ''
              AND LOWER(model_key) <> 'unknown'
              AND LOWER(model_key) NOT LIKE 'unknown%%'
              AND LOWER(model_key) NOT LIKE '%%generic%%'
            GROUP BY source, model_key, COALESCE(condition_grade, 'B')
        ),
        needed_keys AS (
            SELECT
                lk.source,
                lk.model_key,
                lk.condition_grade,
                lk.live_count,
                COALESCE(c.samples, 0)::int AS comp_samples
            FROM live_keys lk
            LEFT JOIN latest_comps c
              ON c.model_key = lk.model_key
             AND c.condition_grade = lk.condition_grade
            WHERE COALESCE(c.samples, 0) < 3
        ),
        eligible AS (
            SELECT
                l.id,
                l.external_id,
                l.source,
                l.model_key,
                COALESCE(l.condition_grade, 'B') AS condition_grade,
                COALESCE(nk.live_count, 0)::int AS live_count,
                COALESCE(nk.comp_samples, c.samples, 0)::int AS comp_samples,
                l.title,
                l.price_current,
                l.bids_count,
                l.price_bid_current,
                l.sale_type,
                l.end_time,
                l.last_seen_at,
                l.backfill_checked_at,
                l.backfill_check_count,
                l.backfill_last_reason,
                CASE
                    WHEN nk.model_key IS NOT NULL THEN 'known_key_live_demand_under_3_comps'
                    WHEN l.source = ANY(%s) THEN 'known_key_high_value_source'
                    WHEN l.end_time >= (now() AT TIME ZONE 'utc') - (%s)::interval THEN 'known_key_recent_ended'
                    ELSE 'known_key_backlog'
                END AS backfill_priority,
                CASE
                    WHEN nk.model_key IS NOT NULL THEN 1
                    WHEN l.source = ANY(%s) THEN 2
                    WHEN l.end_time >= (now() AT TIME ZONE 'utc') - (%s)::interval THEN 3
                    ELSE 4
                END AS tier_rank
            FROM auction_listings l
            LEFT JOIN needed_keys nk
              ON nk.source = l.source
             AND nk.model_key = l.model_key
             AND nk.condition_grade = COALESCE(l.condition_grade, 'B')
            LEFT JOIN latest_comps c
              ON c.model_key = l.model_key
             AND c.condition_grade = COALESCE(l.condition_grade, 'B')
            WHERE l.status = 'unknown'
              AND COALESCE(l.finalized, FALSE) = FALSE
              AND l.end_time <= (now() AT TIME ZONE 'utc') - interval '1 day'
              AND l.external_id LIKE 'v1|%%|%%'
              AND l.model_key IS NOT NULL
              AND l.model_key <> ''
              AND LOWER(l.model_key) <> 'unknown'
              AND LOWER(l.model_key) NOT LIKE 'unknown%%'
              AND LOWER(l.model_key) NOT LIKE '%%generic%%'
              AND (
                  l.backfill_checked_at IS NULL
                  OR l.backfill_checked_at <= now() - (%s)::interval
              )
              AND NOT (l.source = ANY(%s))
        )
        SELECT
            id,
            external_id,
            source,
            model_key,
            condition_grade,
            live_count,
            comp_samples,
            title,
            price_current,
            bids_count,
            price_bid_current,
            sale_type,
            backfill_priority,
            backfill_priority AS selection_reason,
            tier_rank::int AS backfill_priority_tier,
            end_time,
            last_seen_at,
            backfill_checked_at,
            backfill_check_count,
            backfill_last_reason
        FROM eligible
        ORDER BY
            tier_rank,
            CASE source
                WHEN 'ebay-apple' THEN 0
                WHEN 'ebay-samsung' THEN 1
                WHEN 'ebay-watches' THEN 2
                WHEN 'ebay-lego' THEN 3
                WHEN 'ebay-tools' THEN 4
                WHEN 'ebay-actioncams' THEN 5
                WHEN 'ebay-headphones' THEN 6
                WHEN 'ebay-pokemon' THEN 7
                WHEN 'ebay-consoles' THEN 8
                ELSE 99
            END,
            CASE WHEN tier_rank = 1 THEN comp_samples ELSE NULL END ASC NULLS LAST,
            CASE WHEN tier_rank = 1 THEN live_count ELSE NULL END DESC NULLS LAST,
            CASE WHEN tier_rank = 2 THEN COALESCE(price_current, price_bid_current, 0) ELSE NULL END DESC NULLS LAST,
            CASE WHEN COALESCE(bids_count, 0) > 0 THEN 0 ELSE 1 END,
            last_seen_at DESC NULLS LAST,
            end_time DESC NULLS LAST,
            backfill_checked_at ASC NULLS FIRST
        LIMIT %s
        OFFSET %s
        """,
        (
            list(DEFAULT_BACKFILL_SOURCE_PRIORITY),
            BACKFILL_RECENT_ENDED_WINDOW,
            list(DEFAULT_BACKFILL_SOURCE_PRIORITY),
            BACKFILL_RECENT_ENDED_WINDOW,
            BACKFILL_RECHECK_COOLDOWN,
            list(excluded_sources),
            limit,
            offset,
        ),
    )


def build_plan(
    *,
    daily_cap: int,
    safety_reserve: int,
    max_backfill_per_day: int,
    candidate_detail_limit: int,
    include_vehicle_sources: bool,
    recommended_backfill_override: int | None = None,
) -> dict[str, Any]:
    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_backfill_tracking_columns(cur)
            conn.commit()
            budget = compute_api_budget(
                cur,
                daily_cap=daily_cap,
                safety_reserve=safety_reserve,
                max_backfill_per_day=max_backfill_per_day,
            )
            if recommended_backfill_override is None:
                candidate_limit = max(budget.recommended_backfill_calls, 0)
                candidate_limit_source = "api_budget"
            else:
                candidate_limit = max(recommended_backfill_override, 0)
                candidate_limit_source = "operator_limit"
            detail_limit = min(candidate_limit, max(candidate_detail_limit, 0))
            excluded_sources = () if include_vehicle_sources else DEFAULT_EXCLUDED_BACKFILL_SOURCES
            candidates = (
                unknown_backfill_candidates(
                    cur,
                    limit=detail_limit,
                    excluded_sources=excluded_sources,
                )
                if detail_limit > 0
                else []
            )
            candidate_shortfall = max(detail_limit - len(candidates), 0)
            return {
                "mode": "read_only_backfill_plan",
                "api_budget": asdict(budget),
                "excluded_backfill_sources": list(excluded_sources),
                "source_priority": list(DEFAULT_BACKFILL_SOURCE_PRIORITY),
                "unknown_model_key_work": live_unknown_model_key_summary(cur),
                "unknown_backfill_source_summary": unknown_backfill_source_summary(cur),
                "candidate_detail_limit": detail_limit,
                "candidate_limit_source": candidate_limit_source,
                "candidate_count": len(candidates),
                "candidate_shortfall": candidate_shortfall,
                "candidate_shortfall_reason": "eligible_backfill_queue_exhausted_after_filters"
                if candidate_shortfall > 0
                else None,
                "recommended_unknown_candidate_preview": candidates,
                "strategy": [
                    "Do not backfill unknown model_key rows with API calls until model-key extraction is fixed.",
                    "Priority 1: known model_key rows with live demand and fewer than 3 same-grade comps.",
                    "Priority 2: known model_key rows from high-value sources we are likely to trade.",
                    "Priority 3: known model_key rows that ended recently.",
                    "Priority 4: unknown model_key rows only after model-key repair gives them a usable key.",
                    "Priority 5: inaccessible/deleted Trading rows are deleted locally by apply; do not recheck them forever.",
                    "Exclude vehicle/parts-heavy sources by default until model keys separate complete vehicles from parts.",
                    "Do not wait for the full historical unknown backlog; backfill is an incremental enrichment loop for active opportunity decisions.",
                    "Start below the theoretical spare budget, then increase only after API usage and outcome quality are reviewed.",
                    "After each backfill batch, recompute comps and ROI before judging Buy Desk readiness.",
                ],
            }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Plan API-budgeted unknown listing final-price checks.")
    parser.add_argument("--daily-cap", type=int, default=5000)
    parser.add_argument("--safety-reserve", type=int, default=500)
    parser.add_argument("--max-backfill-per-day", type=int, default=1000)
    parser.add_argument("--candidate-detail-limit", type=int, default=50)
    parser.add_argument(
        "--include-vehicle-sources",
        action="store_true",
        help="Include vehicle/parts-heavy sources that are excluded by default.",
    )
    args = parser.parse_args()

    plan = build_plan(
        daily_cap=args.daily_cap,
        safety_reserve=args.safety_reserve,
        max_backfill_per_day=args.max_backfill_per_day,
        candidate_detail_limit=args.candidate_detail_limit,
        include_vehicle_sources=args.include_vehicle_sources,
    )
    print(json.dumps(plan, indent=2, default=str))


if __name__ == "__main__":
    main()
