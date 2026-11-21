from __future__ import annotations

import os
import re
import time
from typing import Any

from pipelines.listing.point_zero.backfill_apply import is_backfill_running
from pipelines.listing.point_zero.backfill_plan import build_plan as build_backfill_plan
from utils.db_schema import connection
from utils.ebay_api_accounting import build_api_accounting, get_bucket_limit


def _query_one(sql: str, params: tuple[Any, ...] = (), *, timeout_ms: int | None = None) -> dict[str, Any]:
    try:
        with connection.cursor() as cur:
            if timeout_ms is not None:
                cur.execute("SET LOCAL lock_timeout = %s", (min(timeout_ms, 2500),))
                cur.execute("SET LOCAL statement_timeout = %s", (timeout_ms,))
            cur.execute(sql, params)
            row = cur.fetchone()
            if not row:
                return {}
            names = [desc[0] for desc in cur.description]
        connection.commit()
        return dict(zip(names, row))
    except Exception:
        connection.rollback()
        raise


def _query_all(sql: str, params: tuple[Any, ...] = (), *, timeout_ms: int | None = None) -> list[dict[str, Any]]:
    try:
        with connection.cursor() as cur:
            if timeout_ms is not None:
                cur.execute("SET LOCAL lock_timeout = %s", (min(timeout_ms, 2500),))
                cur.execute("SET LOCAL statement_timeout = %s", (timeout_ms,))
            cur.execute(sql, params)
            rows = cur.fetchall() or []
            names = [desc[0] for desc in cur.description]
        connection.commit()
        return [dict(zip(names, row)) for row in rows]
    except Exception:
        connection.rollback()
        raise


def _estimate_table_rows(table_name: str) -> int:
    row = _query_one(
        """
        SELECT GREATEST(COALESCE(reltuples, 0), 0)::bigint AS rows
        FROM pg_class
        WHERE oid = %s::regclass
        """,
        (table_name,),
    )
    return int(row.get("rows") or 0)


def _estimate_index_rows(index_name: str) -> int:
    row = _query_one(
        """
        SELECT GREATEST(COALESCE(reltuples, 0), 0)::bigint AS rows
        FROM pg_class
        WHERE oid = to_regclass(%s)
        """,
        (index_name,),
    )
    return int(row.get("rows") or 0)


def _index_exists(index_name: str) -> bool:
    row = _query_one(
        """
        SELECT to_regclass(%s) IS NOT NULL AS exists
        """,
        (index_name,),
    )
    return bool(row.get("exists"))


def _parse_pg_array_text(value: object) -> list[str]:
    text = str(value or "").strip()
    if not text.startswith("{") or not text.endswith("}"):
        return []
    return [part.strip().strip('"') for part in text[1:-1].split(",") if part.strip()]


def _status_count_estimates(total_rows: int) -> dict[str, int]:
    row = _query_one(
        """
        SELECT most_common_vals::text AS vals, most_common_freqs::text AS freqs
        FROM pg_stats
        WHERE schemaname = current_schema()
          AND tablename = 'auction_listings'
          AND attname = 'status'
        """
    )
    vals = _parse_pg_array_text(row.get("vals"))
    freqs = [float(part) for part in re.findall(r"[-+]?(?:\d*\.\d+|\d+)", str(row.get("freqs") or ""))]
    return {
        status: max(int(round(total_rows * freq)), 0)
        for status, freq in zip(vals, freqs)
    }


def _column_null_fraction(column_name: str) -> float:
    row = _query_one(
        """
        SELECT COALESCE(null_frac, 0)::float AS null_frac
        FROM pg_stats
        WHERE schemaname = current_schema()
          AND tablename = 'auction_listings'
          AND attname = %s
        """,
        (column_name,),
    )
    return float(row.get("null_frac") or 0.0)


def _estimated_listing_counts(reason: str) -> dict[str, Any]:
    total = _estimate_table_rows("auction_listings")
    by_status = _status_count_estimates(total)
    live_known = _estimate_index_rows("idx_auction_live_model_key_source")
    final_prices_with_known_model_key = _estimate_index_rows("idx_auction_model_condition_sold_final")
    final_price_null_frac = _column_null_fraction("final_price")
    model_key_null_frac = _column_null_fraction("model_key")
    direct_indexes_available = _index_exists("idx_auction_direct_backfill_unchecked_id") and _index_exists(
        "idx_auction_direct_backfill_retryable_id"
    )
    if direct_indexes_available:
        unknown_due = (
            _estimate_index_rows("idx_auction_direct_backfill_unchecked_id")
            + _estimate_index_rows("idx_auction_direct_backfill_retryable_id")
        )
    else:
        unknown_due = _estimate_index_rows("idx_auction_unknown_backfill_candidates")
    unknown = int(by_status.get("unknown") or 0)
    final_prices = max(int(round(total * (1.0 - final_price_null_frac))), final_prices_with_known_model_key)
    missing_model_key = max(int(round(total * model_key_null_frac)), 0)

    return {
        "total": total,
        "live": max(int(by_status.get("live") or 0), live_known),
        "sold": max(int(by_status.get("sold") or 0), final_prices_with_known_model_key),
        "ended": int(by_status.get("ended") or 0),
        "unknown": unknown,
        "parse_failed": int(by_status.get("parse_failed") or 0),
        "unknown_unfinalized": unknown,
        "unknown_backfill_due": unknown_due,
        "direct_backfill_due": unknown_due,
        "unknown_not_due": max(unknown - unknown_due, 0),
        "final_prices": final_prices,
        "final_prices_with_known_model_key": final_prices_with_known_model_key,
        "missing_model_key": missing_model_key,
        "_estimated": True,
        "_estimate_reason": reason,
    }


def _exact_listing_counts(*, timeout_ms: int | None = None) -> dict[str, Any]:
    by_status = {
        status: _status_count(status, timeout_ms=timeout_ms)
        for status in ("ended", "live", "sold", "unknown", "parse_failed")
    }

    final_price_count = _paged_id_count(
        """
        SELECT id
        FROM auction_listings
        WHERE id > %s
          AND final_price IS NOT NULL
          AND final_price > 0
        ORDER BY id ASC
        LIMIT %s
        """,
        timeout_ms=timeout_ms,
    )
    final_prices_with_known_model_key_count = _paged_id_count(
        """
        SELECT id
        FROM auction_listings
        WHERE id > %s
          AND final_price IS NOT NULL
          AND final_price > 0
          AND model_key IS NOT NULL
          AND model_key <> ''
          AND LOWER(model_key) <> 'unknown'
          AND LOWER(model_key) NOT LIKE 'unknown%%'
        ORDER BY id ASC
        LIMIT %s
        """,
        timeout_ms=timeout_ms,
    )
    missing_model_key_count = _paged_id_count(
        """
        SELECT id
        FROM auction_listings
        WHERE id > %s
          AND (
              model_key IS NULL
              OR model_key = ''
              OR LOWER(model_key) = 'unknown'
              OR LOWER(model_key) LIKE 'unknown%%'
          )
        ORDER BY id ASC
        LIMIT %s
        """,
        timeout_ms=timeout_ms,
    )
    direct_backfill_due = _direct_backfill_due_count(timeout_ms=timeout_ms)
    return {
        "total": sum(by_status.values()),
        "live": int(by_status.get("live") or 0),
        "sold": int(by_status.get("sold") or 0),
        "ended": int(by_status.get("ended") or 0),
        "unknown": int(by_status.get("unknown") or 0),
        "parse_failed": int(by_status.get("parse_failed") or 0),
        "unknown_unfinalized": int(by_status.get("unknown") or 0),
        "unknown_backfill_due": direct_backfill_due,
        "direct_backfill_due": direct_backfill_due,
        "unknown_not_due": max(int(by_status.get("unknown") or 0) - direct_backfill_due, 0),
        "final_prices": final_price_count,
        "final_prices_with_known_model_key": final_prices_with_known_model_key_count,
        "missing_model_key": missing_model_key_count,
    }


def _status_count(status: str, *, timeout_ms: int | None = None) -> int:
    return _paged_id_count(
        """
        SELECT id
        FROM auction_listings
        WHERE id > %s
          AND status = %s
        ORDER BY id ASC
        LIMIT %s
        """,
        extra_params=(status,),
        timeout_ms=timeout_ms,
    )


def _paged_id_count(
    sql: str,
    *,
    extra_params: tuple[Any, ...] = (),
    timeout_ms: int | None = None,
    chunk_size: int = 1000,
) -> int:
    total = 0
    last_id = 0
    while True:
        rows = _query_all(sql, (last_id, *extra_params, chunk_size), timeout_ms=timeout_ms)
        if not rows:
            break
        total += len(rows)
        last_id = int(rows[-1].get("id") or last_id)
        if len(rows) < chunk_size:
            break
    return total


def _direct_backfill_due_count(*, timeout_ms: int | None = None) -> int:
    total = 0
    chunk_size = 1000
    selectors = (
        """
        SELECT id
        FROM auction_listings
        WHERE id > %s
          AND (final_price IS NULL OR final_price <= 0)
          AND end_time IS NOT NULL
          AND end_time <= (now() AT TIME ZONE 'utc') - interval '1 day'
          AND external_id ~ '[0-9]'
          AND backfill_checked_at IS NULL
        ORDER BY id ASC
        LIMIT %s
        """,
        """
        SELECT id
        FROM auction_listings
        WHERE id > %s
          AND (final_price IS NULL OR final_price <= 0)
          AND end_time IS NOT NULL
          AND end_time <= (now() AT TIME ZONE 'utc') - interval '1 day'
          AND external_id ~ '[0-9]'
          AND backfill_checked_at IS NOT NULL
          AND backfill_checked_at <= CURRENT_TIMESTAMP - interval '1 day'
          AND NOT (status = 'ended' AND COALESCE(finalized, FALSE) = TRUE)
          AND COALESCE(backfill_last_reason, '') <> 'invalid_external_id'
        ORDER BY id ASC
        LIMIT %s
        """,
    )
    for sql in selectors:
        total += _paged_id_count(sql, timeout_ms=timeout_ms, chunk_size=chunk_size)
    return total


def _quick_api_budget(
    *,
    daily_cap: int,
    safety_reserve: int,
    max_backfill_per_day: int,
) -> dict[str, Any]:
    today_rows = _query_all(
        """
        SELECT service, call_count::int AS call_count
        FROM api_usage
        WHERE date = CURRENT_DATE
        """
    )
    recent_rows = _query_all(
        """
        SELECT
            date,
            service,
            call_count::int AS call_count
        FROM api_usage
        WHERE date >= CURRENT_DATE - interval '7 days'
        """
    )
    totals_by_date: dict[Any, dict[str, int]] = {}
    for row in recent_rows:
        totals_by_date.setdefault(row.get("date"), {})[str(row.get("service"))] = int(row.get("call_count") or 0)

    recent_trading_totals = [
        int(build_api_accounting(usage)["buckets"]["trading"]["used"])
        for usage in totals_by_date.values()
    ]
    today_calls = int(
        build_api_accounting(
            {str(row.get("service")): int(row.get("call_count") or 0) for row in today_rows}
        )["buckets"]["trading"]["used"]
    )
    avg_total_7d = (
        int(round(sum(recent_trading_totals) / len(recent_trading_totals)))
        if recent_trading_totals
        else 0
    )
    max_total_7d = max(recent_trading_totals) if recent_trading_totals else 0
    planned_normal_calls = max(today_calls, avg_total_7d, max_total_7d)
    theoretical_spare = max(daily_cap - planned_normal_calls - safety_reserve, 0)
    return {
        "daily_cap": daily_cap,
        "safety_reserve": safety_reserve,
        "max_backfill_per_day": max_backfill_per_day,
        "today_calls": today_calls,
        "avg_total_7d": avg_total_7d,
        "max_total_7d": max_total_7d,
        "planned_normal_calls": planned_normal_calls,
        "theoretical_spare": theoretical_spare,
        "recommended_backfill_calls": min(theoretical_spare, max_backfill_per_day),
        "budget_bucket": "trading",
    }


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
    backfill_daily_cap: int | None = None,
    backfill_safety_reserve: int = 500,
    backfill_max_per_day: int = 1000,
    include_vehicle_sources: bool = False,
    include_backfill_plan: bool = True,
    query_timeout_ms: int | None = None,
    secondary_query_timeout_ms: int | None = 3_000,
    exact_listing_summary_timeout_ms: int | None = 10_000,
    approximate_listing_counts_on_timeout: bool = False,
    use_estimated_listing_counts: bool = False,
) -> dict[str, Any]:
    """Build shared read-only operator metrics for email and `ebay stats`."""

    metric_started = time.perf_counter()
    backfill_daily_cap = get_bucket_limit("trading") if backfill_daily_cap is None else int(backfill_daily_cap)
    timings: dict[str, float] = {}

    def mark(name: str, started: float) -> None:
        timings[name] = round(time.perf_counter() - started, 3)

    metric_errors: dict[str, str] = {}

    def error_summary(exc: Exception) -> str:
        return str(exc).splitlines()[0]

    def safe_one(
        name: str,
        sql: str,
        default: dict[str, Any],
        *,
        timeout_ms: int | None = secondary_query_timeout_ms,
    ) -> dict[str, Any]:
        try:
            return _query_one(sql, timeout_ms=timeout_ms)
        except Exception as exc:
            metric_errors[name] = error_summary(exc)
            return default

    def safe_all(name: str, sql: str, default: list[dict[str, Any]]) -> list[dict[str, Any]]:
        try:
            return _query_all(sql, timeout_ms=secondary_query_timeout_ms)
        except Exception as exc:
            metric_errors[name] = error_summary(exc)
            return default

    quick_stats_mode = use_estimated_listing_counts

    t = time.perf_counter()
    if use_estimated_listing_counts:
        listing_counts = _estimated_listing_counts("quick stats mode")
        exact_summary = safe_one(
            "listing_summary",
            """
            SELECT
                COUNT(*)::int AS total,
                COUNT(*) FILTER (WHERE final_price IS NOT NULL AND final_price > 0)::int AS final_prices
            FROM auction_listings
            """,
            {},
            timeout_ms=exact_listing_summary_timeout_ms,
        )
        if exact_summary.get("total") is not None:
            listing_counts["total"] = int(exact_summary["total"])
        if exact_summary.get("final_prices") is not None:
            listing_counts["final_prices"] = int(exact_summary["final_prices"])
            listing_counts["sold"] = int(exact_summary["final_prices"])
        listing_counts["_summary_exact"] = bool(exact_summary)
    else:
        try:
            listing_counts = _exact_listing_counts(timeout_ms=query_timeout_ms)
        except Exception as exc:
            metric_errors["listing_counts"] = error_summary(exc)
            if approximate_listing_counts_on_timeout:
                listing_counts = _estimated_listing_counts(metric_errors["listing_counts"])
                quick_stats_mode = True
            else:
                raise
    mark("listing_counts", t)

    t = time.perf_counter()
    comps = safe_one(
        "comps",
        """
        SELECT
            COUNT(*)::int AS comp_keys,
            COALESCE(SUM(samples), 0)::int AS comp_samples,
            COALESCE(COUNT(*) FILTER (WHERE samples >= 3), 0)::int AS comp_keys_3plus,
            MAX(computed_at) AS computed_at
        FROM latest_comps
        """,
        {},
    )
    mark("comps", t)

    t = time.perf_counter()
    if quick_stats_mode:
        live_comp_coverage = {
            "live_total": int(listing_counts.get("live") or 0),
            "live_with_comps": 0,
            "live_with_3plus_comps": 0,
            "live_unknown_model_key": 0,
            "_skipped": "quick_stats_mode",
        }
    else:
        live_comp_coverage = safe_one(
            "live_comp_coverage",
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
            LEFT JOIN latest_listing_visual_conditions AS vc
              ON vc.listing_id = a.id
            LEFT JOIN latest_listing_text_conditions AS tc
              ON tc.listing_id = a.id
            CROSS JOIN LATERAL (
                SELECT CASE
                    WHEN 'D' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'D'
                    WHEN 'C' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'C'
                    WHEN 'B' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'B'
                    ELSE 'A'
                END AS effective_condition_grade
            ) AS grade
            LEFT JOIN latest_comps c
              ON c.model_key = a.model_key
             AND c.condition_grade = grade.effective_condition_grade
            WHERE a.status = 'live'
            """,
            {},
        )
    mark("live_comp_coverage", t)

    t = time.perf_counter()
    api_today = safe_all(
        "api_today",
        """
        SELECT service, call_count::int AS call_count
        FROM api_usage
        WHERE date = CURRENT_DATE
        ORDER BY call_count DESC, service
        """,
        [],
    )
    api_total = sum(int(row.get("call_count") or 0) for row in api_today)
    api_accounting = build_api_accounting(
        {str(row.get("service")): int(row.get("call_count") or 0) for row in api_today}
    )
    mark("api_today", t)

    t = time.perf_counter()
    if quick_stats_mode:
        high_risk_roi = {
            "live_roi_gt_15": 0,
            "live_roi_gt_100": 0,
            "live_roi_gt_1000": 0,
            "_skipped": "quick_stats_mode",
        }
    else:
        high_risk_roi = safe_one(
            "high_risk_roi",
            """
            SELECT
                COUNT(*) FILTER (WHERE o.status = 'live' AND s.roi_estimate > 0.15)::int AS live_roi_gt_15,
                COUNT(*) FILTER (WHERE o.status = 'live' AND s.roi_estimate > 1.0)::int AS live_roi_gt_100,
                COUNT(*) FILTER (WHERE o.status = 'live' AND s.roi_estimate > 10.0)::int AS live_roi_gt_1000
            FROM listing_outcome_v AS o
            JOIN listing_score_v AS s
              ON s.listing_id = o.listing_id
            """,
            {},
        )
    mark("high_risk_roi", t)

    t = time.perf_counter()
    if include_backfill_plan:
        try:
            backfill_plan = build_backfill_plan(
                daily_cap=backfill_daily_cap,
                safety_reserve=backfill_safety_reserve,
                max_backfill_per_day=backfill_max_per_day,
                candidate_detail_limit=3,
                include_vehicle_sources=include_vehicle_sources,
            )
        except Exception as exc:
            metric_errors["backfill_plan"] = error_summary(exc)
            backfill_plan = {"error": str(exc)}
    else:
        try:
            backfill_plan = {
                "mode": "quick_api_budget_only",
                "api_budget": _quick_api_budget(
                    daily_cap=backfill_daily_cap,
                    safety_reserve=backfill_safety_reserve,
                    max_backfill_per_day=backfill_max_per_day,
                ),
            }
        except Exception as exc:
            metric_errors["backfill_plan"] = error_summary(exc)
            backfill_plan = {"error": str(exc)}
    mark("backfill_plan", t)

    t = time.perf_counter()
    if quick_stats_mode:
        trading_checks_today = sum(
            int(row.get("call_count") or 0)
            for row in api_today
            if str(row.get("service")) == "ebay_ended_v1"
        )
        activity_today = safe_one(
            "backfill_activity",
            """
            SELECT
                COUNT(*)::int AS checked_rows_today,
                COUNT(*) FILTER (
                    WHERE finalized IS TRUE
                      AND status IN ('sold', 'ended')
                )::int AS finalized_today,
                COUNT(*) FILTER (
                    WHERE status = 'sold'
                      AND final_price IS NOT NULL
                      AND final_price > 0
                )::int AS sold_today,
                MAX(backfill_checked_at) AS latest_checked_at
            FROM auction_listings
            WHERE backfill_checked_at >= CURRENT_DATE
              AND backfill_checked_at < CURRENT_DATE + interval '1 day'
            """,
            {},
        )
        checked_rows_today = int(activity_today.get("checked_rows_today") or 0)
        backfill_activity = {
            "checked_today": max(trading_checks_today, checked_rows_today),
            "checked_24h": 0,
            "checked_rows_today": checked_rows_today,
            "trading_checks_today": trading_checks_today,
            "finalized_today": int(activity_today.get("finalized_today") or 0),
            "sold_today": int(activity_today.get("sold_today") or 0),
            "latest_checked_at": activity_today.get("latest_checked_at"),
        }
    else:
        backfill_activity = safe_one(
            "backfill_activity",
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
            """,
            {},
        )
    mark("backfill_activity", t)

    t = time.perf_counter()
    if quick_stats_mode:
        backfill_reason_counts = []
    else:
        backfill_reason_counts = safe_all(
            "backfill_reason_counts",
            """
            SELECT
                COALESCE(backfill_last_reason, 'unknown') AS reason,
                COUNT(*)::int AS checks
            FROM listing_backfill_audit_v
            WHERE backfill_checked_at::date = CURRENT_DATE
            GROUP BY COALESCE(backfill_last_reason, 'unknown')
            ORDER BY checks DESC, reason
            LIMIT 8
            """,
            [],
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
        "metric_errors": metric_errors,
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
