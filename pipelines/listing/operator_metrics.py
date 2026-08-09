from __future__ import annotations

import os
import time
from typing import Any

from pipelines.listing.point_zero.backfill_apply import is_backfill_running
from pipelines.listing.point_zero.backfill_plan import build_plan as build_backfill_plan
from utils.db_schema import connection
from utils.ebay_api_accounting import build_api_accounting


def _query_one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return {}
        names = [desc[0] for desc in cur.description]
    connection.commit()
    return dict(zip(names, row))


def _query_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connection.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        names = [desc[0] for desc in cur.description]
    connection.commit()
    return [dict(zip(names, row)) for row in rows]


def _model_key_inventory(limit_per_market: int) -> list[dict[str, Any]]:
    return _query_all(
        """
        WITH key_evidence AS (
            SELECT
                source,
                model_key,
                COUNT(*)::int AS listing_count,
                COUNT(*) FILTER (WHERE status = 'live')::int AS live_count,
                0::int AS final_price_count
            FROM auction_listings
            WHERE source IS NOT NULL
              AND source <> ''
              AND model_key IS NOT NULL
              AND model_key <> ''
              AND model_key <> 'unknown'
              AND model_key NOT LIKE 'unknown%%'
              AND model_key NOT LIKE '%%generic%%'
              AND status = 'live'
            GROUP BY source, model_key

            UNION ALL

            SELECT
                source,
                model_key,
                COUNT(*)::int AS listing_count,
                0::int AS live_count,
                COUNT(*)::int AS final_price_count
            FROM auction_listings
            WHERE source IS NOT NULL
              AND source <> ''
              AND model_key IS NOT NULL
              AND model_key <> ''
              AND model_key <> 'unknown'
              AND model_key NOT LIKE 'unknown%%'
              AND model_key NOT LIKE '%%generic%%'
              AND status = 'sold'
              AND final_price IS NOT NULL
              AND final_price > 0
            GROUP BY source, model_key
        ),
        key_counts AS (
            SELECT
                source,
                model_key,
                SUM(listing_count)::int AS listing_count,
                SUM(live_count)::int AS live_count,
                SUM(final_price_count)::int AS final_price_count
            FROM key_evidence
            GROUP BY source, model_key
        ),
        source_totals AS (
            SELECT
                source,
                COUNT(*)::int AS source_total_rows,
                COUNT(*) FILTER (
                    WHERE final_price IS NULL OR final_price <= 0
                )::int AS source_without_final_price_count
            FROM auction_listings
            WHERE source IS NOT NULL
              AND source <> ''
            GROUP BY source
        ),
        ranked AS (
            SELECT
                key_counts.*,
                COUNT(*) OVER (PARTITION BY source)::int AS source_key_count,
                SUM(listing_count) OVER (PARTITION BY source)::int AS source_listing_count,
                SUM(live_count) OVER (PARTITION BY source)::int AS source_live_count,
                SUM(final_price_count) OVER (PARTITION BY source)::int AS source_final_price_count,
                ROW_NUMBER() OVER (
                    PARTITION BY source
                    ORDER BY final_price_count DESC, live_count DESC, listing_count DESC, model_key
                )::int AS source_rank
            FROM key_counts
        )
        SELECT
            source,
            model_key,
            listing_count,
            live_count,
            final_price_count,
            source_key_count,
            source_listing_count,
            source_live_count,
            source_final_price_count,
            source_total_rows,
            source_without_final_price_count,
            source_rank
        FROM ranked
        JOIN source_totals USING (source)
        WHERE source_rank <= %s
        ORDER BY source, source_rank
        """,
        (limit_per_market,),
    )


def build_operator_metrics(
    *,
    include_model_key_inventory: bool = False,
    model_keys_per_market: int | None = None,
    backfill_daily_cap: int = 5000,
    backfill_safety_reserve: int = 500,
    backfill_max_per_day: int = 1000,
    include_vehicle_sources: bool = False,
) -> dict[str, Any]:
    """Build shared read-only operator metrics for email and `ebay stats`."""

    metric_started = time.perf_counter()
    timings: dict[str, float] = {}

    def mark(name: str, started: float) -> None:
        timings[name] = round(time.perf_counter() - started, 3)

    t = time.perf_counter()
    listing_counts = _query_one(
        """
        SELECT
            COUNT(*)::int AS total,
            COUNT(*) FILTER (WHERE status = 'live')::int AS live,
            COUNT(*) FILTER (WHERE status = 'sold')::int AS sold,
            COUNT(*) FILTER (WHERE status = 'ended')::int AS ended,
            COUNT(*) FILTER (WHERE status = 'unknown')::int AS unknown,
            COUNT(*) FILTER (WHERE status = 'parse_failed')::int AS parse_failed,
            COUNT(*) FILTER (WHERE status = 'unknown' AND COALESCE(finalized, FALSE) = FALSE)::int AS unknown_unfinalized,
            COUNT(*) FILTER (
                WHERE status = 'unknown'
                  AND COALESCE(finalized, FALSE) = FALSE
                  AND end_time IS NOT NULL
                  AND end_time <= (now() AT TIME ZONE 'utc') - interval '1 day'
            )::int AS unknown_backfill_due,
            COUNT(*) FILTER (
                WHERE status = 'unknown'
                  AND COALESCE(finalized, FALSE) = FALSE
                  AND end_time IS NOT NULL
                  AND end_time > (now() AT TIME ZONE 'utc') - interval '1 day'
            )::int AS unknown_not_due,
            COUNT(*) FILTER (WHERE final_price IS NOT NULL AND final_price > 0)::int AS final_prices,
            COUNT(*) FILTER (
                WHERE final_price IS NOT NULL
                  AND final_price > 0
                  AND model_key IS NOT NULL
                  AND model_key <> ''
                  AND LOWER(model_key) <> 'unknown'
                  AND LOWER(model_key) NOT LIKE 'unknown%%'
            )::int AS final_prices_with_known_model_key,
            COUNT(*) FILTER (
                WHERE model_key IS NULL
                   OR model_key = ''
                   OR LOWER(model_key) = 'unknown'
                   OR LOWER(model_key) LIKE 'unknown%%'
            )::int AS missing_model_key
        FROM auction_listings
        """
    )
    mark("listing_counts", t)

    t = time.perf_counter()
    comps = _query_one(
        """
        SELECT
            COUNT(*)::int AS comp_keys,
            COALESCE(SUM(samples), 0)::int AS comp_samples,
            COALESCE(COUNT(*) FILTER (WHERE samples >= 3), 0)::int AS comp_keys_3plus,
            MAX(computed_at) AS computed_at
        FROM latest_comps
        """
    )
    mark("comps", t)

    t = time.perf_counter()
    live_comp_coverage = _query_one(
        """
        SELECT
            COUNT(*)::int AS live_total,
            COUNT(*) FILTER (WHERE c.model_key IS NOT NULL)::int AS live_with_comps,
            COUNT(*) FILTER (WHERE c.samples >= 3)::int AS live_with_3plus_comps,
            COUNT(*) FILTER (
                WHERE a.model_key IS NULL
                   OR a.model_key = ''
                   OR LOWER(a.model_key) = 'unknown'
                   OR LOWER(a.model_key) LIKE 'unknown%%'
            )::int AS live_unknown_model_key
        FROM auction_listings AS a
        LEFT JOIN latest_comps c
          ON c.model_key = a.model_key
         AND c.condition_grade = COALESCE(a.condition_grade, 'B')
        WHERE a.status = 'live'
        """
    )
    mark("live_comp_coverage", t)

    t = time.perf_counter()
    api_today = _query_all(
        """
        SELECT service, call_count::int AS call_count
        FROM api_usage
        WHERE date = CURRENT_DATE
        ORDER BY call_count DESC, service
        """
    )
    api_total = sum(int(row.get("call_count") or 0) for row in api_today)
    api_accounting = build_api_accounting(
        {str(row.get("service")): int(row.get("call_count") or 0) for row in api_today}
    )
    mark("api_today", t)

    t = time.perf_counter()
    high_risk_roi = _query_one(
        """
        SELECT
            COUNT(*) FILTER (WHERE o.status = 'live' AND s.roi_estimate > 0.15)::int AS live_roi_gt_15,
            COUNT(*) FILTER (WHERE o.status = 'live' AND s.roi_estimate > 1.0)::int AS live_roi_gt_100,
            COUNT(*) FILTER (WHERE o.status = 'live' AND s.roi_estimate > 10.0)::int AS live_roi_gt_1000
        FROM listing_outcome_v AS o
        JOIN listing_score_v AS s
          ON s.listing_id = o.listing_id
        """
    )
    mark("high_risk_roi", t)

    t = time.perf_counter()
    try:
        backfill_plan = build_backfill_plan(
            daily_cap=backfill_daily_cap,
            safety_reserve=backfill_safety_reserve,
            max_backfill_per_day=backfill_max_per_day,
            candidate_detail_limit=3,
            include_vehicle_sources=include_vehicle_sources,
        )
    except Exception as exc:
        backfill_plan = {"error": str(exc)}
    mark("backfill_plan", t)

    t = time.perf_counter()
    backfill_activity = _query_one(
        """
        WITH audit AS (
            SELECT
                COUNT(*) FILTER (
                    WHERE backfill_checked_at::date = CURRENT_DATE
                )::int AS checked_rows_today,
                COUNT(*) FILTER (
                    WHERE backfill_checked_at >= CURRENT_TIMESTAMP - interval '24 hours'
                )::int AS checked_rows_24h,
                COUNT(*) FILTER (
                    WHERE backfill_checked_at::date = CURRENT_DATE
                      AND finalized IS TRUE
                      AND status IN ('sold', 'ended')
                )::int AS finalized_today,
                COUNT(*) FILTER (
                    WHERE backfill_checked_at::date = CURRENT_DATE
                      AND status = 'sold'
                )::int AS sold_today,
                MAX(backfill_checked_at) AS latest_checked_at
            FROM listing_backfill_audit_v
            WHERE backfill_checked_at IS NOT NULL
        ),
        usage AS (
            SELECT COALESCE(SUM(call_count), 0)::int AS trading_checks_today
            FROM api_usage
            WHERE date = CURRENT_DATE
              AND service = 'ebay_ended_v1'
        )
        SELECT
            GREATEST(audit.checked_rows_today, usage.trading_checks_today)::int AS checked_today,
            audit.checked_rows_24h AS checked_24h,
            audit.checked_rows_today,
            usage.trading_checks_today,
            audit.finalized_today,
            audit.sold_today,
            audit.latest_checked_at
        FROM audit, usage
        """
    )
    mark("backfill_activity", t)

    t = time.perf_counter()
    backfill_reason_counts = _query_all(
        """
        SELECT
            COALESCE(backfill_last_reason, 'unknown') AS reason,
            COUNT(*)::int AS checks
        FROM listing_backfill_audit_v
        WHERE backfill_checked_at::date = CURRENT_DATE
        GROUP BY COALESCE(backfill_last_reason, 'unknown')
        ORDER BY checks DESC, reason
        LIMIT 8
        """
    )
    mark("backfill_reason_counts", t)

    t = time.perf_counter()
    try:
        backfill_worker_running = is_backfill_running()
    except Exception:
        backfill_worker_running = None
    mark("backfill_worker_running", t)

    metrics = {
        "listing_counts": listing_counts,
        "comps": comps,
        "live_comp_coverage": live_comp_coverage,
        "api_today": api_today,
        "api_total": api_total,
        "api_accounting": api_accounting,
        "high_risk_roi": high_risk_roi,
        "backfill_plan": backfill_plan,
        "backfill_activity": backfill_activity,
        "backfill_reason_counts": backfill_reason_counts,
        "backfill_worker_running": backfill_worker_running,
    }

    if include_model_key_inventory:
        t = time.perf_counter()
        limit = int(
            model_keys_per_market
            if model_keys_per_market is not None
            else os.getenv("GF_HEARTBEAT_MODEL_KEYS_PER_MARKET", "80")
        )
        metrics["model_key_inventory"] = _model_key_inventory(limit)
        mark("model_key_inventory", t)

    timings["total"] = round(time.perf_counter() - metric_started, 3)
    metrics["metric_timings"] = timings
    return metrics
