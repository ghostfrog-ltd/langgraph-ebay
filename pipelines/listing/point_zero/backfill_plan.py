"""
Plan targeted stale-listing backfills under the daily eBay API budget.

The planner is read-only with respect to marketplace state. Its only database
side effect is ensuring local audit columns exist so apply batches can avoid
rechecking the same stale rows too frequently.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from typing import Any

from psycopg2.extras import RealDictCursor

from utils.db_schema import get_fresh_connection
from utils.condition import looks_like_partial_or_accessory_listing

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


def ensure_backfill_tracking_columns(cur) -> None:
    """
    Create local audit columns used by Point Zero backfill batches.

    These fields do not change marketplace state. They record that a stale row
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
    today = _fetch_one(
        cur,
        """
        SELECT COALESCE(SUM(call_count), 0)::int AS today_calls
        FROM api_usage
        WHERE date = CURRENT_DATE
        """,
    )
    recent = _fetch_one(
        cur,
        """
        SELECT
            COALESCE(ROUND(AVG(total_calls)), 0)::int AS avg_total_7d,
            COALESCE(MAX(total_calls), 0)::int AS max_total_7d
        FROM (
            SELECT date, SUM(call_count)::int AS total_calls
            FROM api_usage
            WHERE date >= CURRENT_DATE - interval '7 days'
            GROUP BY date
        ) d
        """,
    )

    today_calls = int(today.get("today_calls") or 0)
    avg_total_7d = int(recent.get("avg_total_7d") or 0)
    max_total_7d = int(recent.get("max_total_7d") or 0)
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


def stale_backfill_source_summary(cur) -> list[dict[str, Any]]:
    return _fetch_all(
        cur,
        """
        WITH live_keys AS (
            SELECT source, model_key, COUNT(*)::int AS live_count
            FROM auction_listings
            WHERE status = 'live'
              AND model_key IS NOT NULL
              AND model_key <> ''
              AND LOWER(model_key) <> 'unknown'
              AND LOWER(model_key) NOT LIKE 'unknown%%'
              AND LOWER(model_key) NOT LIKE '%%generic%%'
            GROUP BY source, model_key
        ),
        needed_keys AS (
            SELECT
                lk.source,
                lk.model_key,
                lk.live_count,
                COALESCE(c.samples, 0)::int AS comp_samples
            FROM live_keys lk
            LEFT JOIN latest_comps c ON c.model_key = lk.model_key
            WHERE COALESCE(c.samples, 0) < 3
        ),
        stale_pool AS (
            SELECT
                l.source,
                l.model_key,
                COUNT(*)::int AS stale_count,
                MAX(l.last_seen_at) AS newest_stale
            FROM auction_listings l
            JOIN needed_keys nk
              ON nk.source = l.source
             AND nk.model_key = l.model_key
            WHERE l.status = 'stale'
              AND COALESCE(l.finalized, FALSE) = FALSE
              AND l.end_time <= (now() AT TIME ZONE 'utc') - interval '1 day'
            GROUP BY l.source, l.model_key
        )
        SELECT
            nk.source,
            COUNT(*)::int AS model_keys_needing_comps,
            SUM(nk.live_count)::int AS live_listings_needing_comps,
            SUM(COALESCE(sp.stale_count, 0))::int AS stale_backfill_pool,
            MAX(sp.newest_stale) AS newest_stale
        FROM needed_keys nk
        LEFT JOIN stale_pool sp
          ON sp.source = nk.source
         AND sp.model_key = nk.model_key
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
            stale_backfill_pool DESC
        """,
    )


def stale_backfill_candidates(
    cur,
    *,
    limit: int,
    excluded_sources: tuple[str, ...],
) -> list[dict[str, Any]]:
    raw_limit = max(limit * 5, limit)
    rows = _fetch_all(
        cur,
        """
        WITH live_keys AS (
            SELECT source, model_key, COUNT(*)::int AS live_count
            FROM auction_listings
            WHERE status = 'live'
              AND model_key IS NOT NULL
              AND model_key <> ''
              AND LOWER(model_key) <> 'unknown'
              AND LOWER(model_key) NOT LIKE 'unknown%%'
              AND LOWER(model_key) NOT LIKE '%%generic%%'
            GROUP BY source, model_key
        ),
        needed_keys AS (
            SELECT
                lk.source,
                lk.model_key,
                lk.live_count,
                COALESCE(c.samples, 0)::int AS comp_samples
            FROM live_keys lk
            LEFT JOIN latest_comps c ON c.model_key = lk.model_key
            WHERE COALESCE(c.samples, 0) < 3
        )
        SELECT
            l.id,
            l.external_id,
            l.source,
            l.model_key,
            nk.live_count,
            nk.comp_samples,
            l.title,
            l.price_current,
            l.end_time,
            l.last_seen_at,
            l.backfill_checked_at,
            l.backfill_check_count,
            l.backfill_last_reason
        FROM auction_listings l
        JOIN needed_keys nk
          ON nk.source = l.source
         AND nk.model_key = l.model_key
        WHERE l.status = 'stale'
          AND COALESCE(l.finalized, FALSE) = FALSE
          AND l.end_time <= (now() AT TIME ZONE 'utc') - interval '1 day'
          AND l.external_id LIKE 'v1|%%|%%'
          AND (
              l.backfill_checked_at IS NULL
              OR l.backfill_checked_at <= now() - (%s)::interval
          )
          AND NOT (l.source = ANY(%s))
        ORDER BY
            CASE l.source
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
            l.backfill_checked_at ASC NULLS FIRST,
            l.last_seen_at DESC NULLS LAST,
            l.end_time DESC NULLS LAST,
            nk.comp_samples ASC,
            nk.live_count DESC
        LIMIT %s
        """,
        (BACKFILL_RECHECK_COOLDOWN, list(excluded_sources), raw_limit),
    )

    filtered = []
    for row in rows:
        if looks_like_partial_or_accessory_listing(
            str(row.get("title") or ""),
            source=str(row.get("source") or ""),
            model_key=str(row.get("model_key") or ""),
        ):
            continue
        filtered.append(row)
        if len(filtered) >= limit:
            break

    return filtered


def build_plan(
    *,
    daily_cap: int,
    safety_reserve: int,
    max_backfill_per_day: int,
    candidate_detail_limit: int,
    include_vehicle_sources: bool,
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
            candidate_limit = max(budget.recommended_backfill_calls, 0)
            detail_limit = min(candidate_limit, max(candidate_detail_limit, 0))
            excluded_sources = () if include_vehicle_sources else DEFAULT_EXCLUDED_BACKFILL_SOURCES
            return {
                "mode": "read_only_backfill_plan",
                "api_budget": asdict(budget),
                "excluded_backfill_sources": list(excluded_sources),
                "source_priority": list(DEFAULT_BACKFILL_SOURCE_PRIORITY),
                "unknown_model_key_work": live_unknown_model_key_summary(cur),
                "stale_backfill_source_summary": stale_backfill_source_summary(cur),
                "candidate_detail_limit": detail_limit,
                "recommended_stale_candidate_preview": stale_backfill_candidates(
                    cur,
                    limit=detail_limit,
                    excluded_sources=excluded_sources,
                )
                if detail_limit > 0
                else [],
                "strategy": [
                    "Do not backfill unknown model_key rows with API calls until model-key extraction is fixed.",
                    "Prioritise Apple final-price evidence first, then Samsung, because those lanes have the strongest current buy-side learning value.",
                    "Exclude vehicle/parts-heavy sources by default until model keys separate complete vehicles from parts.",
                    "Use spare calls first on newest stale rows whose model_key appears in current live listings with fewer than 3 comp samples.",
                    "Do not wait for the full historical stale backlog; backfill is an incremental enrichment loop for active opportunity decisions.",
                    "Start below the theoretical spare budget, then increase only after API usage and outcome quality are reviewed.",
                    "After each backfill batch, recompute comps and ROI before judging Buy Desk readiness.",
                ],
            }
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Plan API-budgeted stale listing backfill.")
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
