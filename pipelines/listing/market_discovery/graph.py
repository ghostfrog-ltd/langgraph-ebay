"""
Read-only live listing potentials from local PostgreSQL evidence.

This module answers one plain question:

Which live eBay listings are priced below our safe buy price for the same
`model_key`, using confirmed sold comps only?
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
import os
from typing import Any, Mapping

from psycopg2.extras import RealDictCursor

import utils.db_schema as schema
from utils.condition import effective_condition_grade, has_investible_product_identity
from pipelines.listing.review_actions import ensure_review_table
from .pricing import (
    CATEGORY_MIN_CANDIDATE_COMPS,
    FEE_RATE,
    INBOUND_SHIP_GBP,
    MIN_CANDIDATE_COMPS,
    MIN_PROFIT_GBP,
    OUTBOUND_SHIP_GBP,
    PACKAGING_GBP,
    SELLING_RESERVE_GBP,
    effective_min_comps_for_source,
    expected_profit as _expected_profit,
    max_safe_buy as _max_safe_buy,
    money as _money,
)

MIN_ROI = 0.0
DEFAULT_LIVE_LISTING_MAX_AGE_HOURS = 24
NEAR_MISS_WINDOW_GBP = 10.0


def live_listing_max_age_hours() -> int:
    try:
        return max(1, int(os.getenv("GHOSTFROG_REMOTE_SYNC_LIVE_MAX_AGE_HOURS", DEFAULT_LIVE_LISTING_MAX_AGE_HOURS)))
    except ValueError:
        return DEFAULT_LIVE_LISTING_MAX_AGE_HOURS


@dataclass
class ListingPotential:
    """One live listing compared directly against same-model sold comps."""

    source: str
    external_id: str
    title: str
    url: str
    model_key: str
    condition_grade: str
    text_condition_grade: str
    text_scan_grade: str | None
    text_scan_checked: bool
    text_scan_review_required: bool
    text_scan_risk_flags: list[str]
    text_scan_model_name: str | None
    text_scan_checked_at: Any
    visual_condition_grade: str | None
    visual_condition_checked: bool
    visual_review_required: bool
    visual_damage_flags: list[str]
    visual_model_name: str | None
    visual_checked_at: Any
    sale_type: str
    end_time: Any
    current_price: float
    sold_median: float
    comp_samples: int
    max_safe_buy: float
    expected_profit: float
    expected_roi: float
    price_gap_to_max_buy: float
    effective_min_comps: int
    signal: str


def potential_from_row(
    row: dict[str, Any],
    *,
    min_profit: float = MIN_PROFIT_GBP,
    min_comps_override: int | None = None,
) -> ListingPotential:
    """
    Convert one live listing + comp row into deterministic buy maths.

    No LLM judgement, no lane scoring, no status buckets.
    """
    current_price = float(row.get("price_current") or 0.0)
    sold_median = float(row.get("median_final_price") or 0.0)
    comp_samples = int(row.get("samples") or 0)
    effective_min_comps = effective_min_comps_for_source(row.get("source"), override=min_comps_override)
    profit = _expected_profit(current_price=current_price, sold_median=sold_median)
    roi = 0.0 if current_price <= 0 else profit / current_price
    max_buy = _max_safe_buy(sold_median=sold_median, min_profit=min_profit)
    gap = _money(max_buy - current_price)

    return ListingPotential(
        source=str(row.get("source") or ""),
        external_id=str(row.get("external_id") or ""),
        title=str(row.get("title") or ""),
        url=str(row.get("url") or ""),
        model_key=str(row.get("model_key") or ""),
        condition_grade=effective_condition_grade(
            row.get("condition_grade") or row.get("text_condition_grade") or "B",
            row.get("visual_condition_grade"),
            row.get("text_scan_grade"),
        ),
        text_condition_grade=str(row.get("text_condition_grade") or row.get("condition_grade") or "B"),
        text_scan_grade=(
            str(row.get("text_scan_grade"))
            if row.get("text_scan_grade") is not None
            else None
        ),
        text_scan_checked=bool(row.get("text_scan_checked", row.get("text_scan_checked_at") is not None)),
        text_scan_review_required=bool(row.get("text_scan_review_required", False)),
        text_scan_risk_flags=list(row.get("text_scan_risk_flags") or []),
        text_scan_model_name=(
            str(row.get("text_scan_model_name"))
            if row.get("text_scan_model_name") is not None
            else None
        ),
        text_scan_checked_at=row.get("text_scan_checked_at"),
        visual_condition_grade=(
            str(row.get("visual_condition_grade"))
            if row.get("visual_condition_grade") is not None
            else None
        ),
        visual_condition_checked=bool(row.get("visual_condition_checked", row.get("visual_checked_at") is not None)),
        visual_review_required=bool(row.get("visual_review_required", False)),
        visual_damage_flags=list(row.get("visual_damage_flags") or []),
        visual_model_name=(
            str(row.get("visual_model_name"))
            if row.get("visual_model_name") is not None
            else None
        ),
        visual_checked_at=row.get("visual_checked_at"),
        sale_type=str(row.get("sale_type") or ""),
        end_time=row.get("end_time"),
        current_price=_money(current_price),
        sold_median=_money(sold_median),
        comp_samples=comp_samples,
        max_safe_buy=max_buy,
        expected_profit=profit,
        expected_roi=round(roi, 4),
        price_gap_to_max_buy=gap,
        effective_min_comps=effective_min_comps,
        signal=(
            f"current £{_money(current_price)} vs max buy £{max_buy}; "
            f"sold median £{_money(sold_median)} from {comp_samples} same-grade comps "
            f"(floor {effective_min_comps}); "
            f"expected profit £{profit}"
        ),
    )


def _fetch_live_listing_comp_rows(
    cur,
    *,
    include_future_unknown: bool = False,
    source: str | None = None,
) -> list[dict[str, Any]]:
    status_predicate = "a.status = 'live'"
    if include_future_unknown:
        status_predicate = """
          (
            a.status = 'live'
            OR (
              a.status = 'unknown'
              AND a.end_time IS NOT NULL
              AND a.end_time > (now() AT TIME ZONE 'utc')
            )
          )
        """

    source_predicate = ""
    params: list[Any] = [str(live_listing_max_age_hours())]
    if source:
        source_predicate = "AND a.source = %s"
        params.append(source)

    cur.execute(
        f"""
        SELECT
            a.source,
            a.external_id,
            a.title,
            a.url,
            a.model_key,
            grade.effective_condition_grade AS condition_grade,
            grade.text_condition_grade,
            tc.text_grade AS text_scan_grade,
            (tc.id IS NOT NULL) AS text_scan_checked,
            COALESCE(tc.review_required, false) AS text_scan_review_required,
            COALESCE(tc.risk_flags, ARRAY[]::text[]) AS text_scan_risk_flags,
            tc.model_name AS text_scan_model_name,
            tc.checked_at AS text_scan_checked_at,
            vc.visual_grade AS visual_condition_grade,
            (vc.id IS NOT NULL) AS visual_condition_checked,
            COALESCE(vc.review_required, false) AS visual_review_required,
            COALESCE(vc.damage_flags, ARRAY[]::text[]) AS visual_damage_flags,
            vc.model_name AS visual_model_name,
            vc.checked_at AS visual_checked_at,
            a.sale_type,
            a.raw_attrs,
            a.end_time,
            COALESCE(a.price_current, 0) AS price_current,
            lc.samples,
            lc.median_final_price
        FROM auction_listings AS a
        LEFT JOIN latest_listing_visual_conditions AS vc
          ON vc.listing_id = a.id
        LEFT JOIN latest_listing_text_conditions AS tc
          ON tc.listing_id = a.id
        CROSS JOIN LATERAL (
            SELECT
                COALESCE(a.condition_grade, 'B') AS text_condition_grade,
                CASE
                    WHEN 'D' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'D'
                    WHEN 'C' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'C'
                    WHEN 'B' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'B'
                    ELSE 'A'
                END AS effective_condition_grade
        ) AS grade
        JOIN latest_comps AS lc
          ON lc.model_key = a.model_key
         AND lc.condition_grade = grade.effective_condition_grade
        WHERE {status_predicate}
          AND COALESCE(a.last_seen_at, a.first_seen) >= now() - (%s || ' hours')::interval
          {source_predicate}
          AND a.price_current IS NOT NULL
          AND a.price_current > 0
          AND a.model_key IS NOT NULL
          AND a.model_key <> ''
          AND a.model_key <> 'unknown'
          AND COALESCE(lc.samples, 0) > 0
          AND COALESCE(lc.median_final_price, 0) > 0
          AND NOT EXISTS (
              SELECT 1
              FROM (
                  SELECT DISTINCT ON (external_id)
                      external_id,
                      action
                  FROM listing_review_actions
                  ORDER BY external_id, created_at DESC, id DESC
              ) AS review
              WHERE review.external_id = a.external_id
                AND review.action = 'down'
          )
        """,
        params,
    )
    return [dict(row) for row in cur.fetchall()]


def build_potentials(
    rows: list[dict[str, Any]],
    *,
    min_profit: float = MIN_PROFIT_GBP,
    min_roi: float = MIN_ROI,
    min_comps: int | None = None,
    now: datetime | None = None,
) -> tuple[list[ListingPotential], list[ListingPotential], dict[str, int]]:
    """
    Return rows that clear the buy maths, plus closest misses for debugging.

    `potentials` are live rows where current price is below max safe buy,
    expected profit clears `min_profit`, and comp depth clears the category floor
    unless an explicit `min_comps` override is supplied.
    ROI is returned for context and can be used as an optional extra gate.
    """
    stats = {
        "live_rows_with_comps": 0,
        "blocked_identity": 0,
        "below_min_comps": 0,
        "checked": 0,
    }
    all_checked: list[ListingPotential] = []
    potentials: list[ListingPotential] = []

    for row in rows:
        stats["live_rows_with_comps"] += 1
        attrs = row.get("raw_attrs") if isinstance(row.get("raw_attrs"), Mapping) else {}
        if not has_investible_product_identity(
            str(row.get("title") or ""),
            attrs=attrs,
            source=str(row.get("source") or ""),
            model_key=str(row.get("model_key") or ""),
        ):
            stats["blocked_identity"] += 1
            continue

        potential = potential_from_row(row, min_profit=min_profit, min_comps_override=min_comps)
        if potential.comp_samples < potential.effective_min_comps:
            stats["below_min_comps"] += 1
            continue

        stats["checked"] += 1
        all_checked.append(potential)
        if (
            potential.current_price <= potential.max_safe_buy
            and potential.expected_profit >= min_profit
            and (min_roi <= 0 or potential.expected_roi >= min_roi)
        ):
            potentials.append(potential)

    potentials.sort(key=lambda item: (item.expected_profit, item.expected_roi), reverse=True)
    all_checked.sort(key=lambda item: (item.price_gap_to_max_buy, item.expected_profit), reverse=True)
    return potentials, all_checked, stats


def useful_near_misses(
    rows: list[Mapping[str, Any]],
    *,
    min_profit: float,
    limit: int,
    near_window: float = NEAR_MISS_WINDOW_GBP,
) -> list[Mapping[str, Any]]:
    """
    Filter raw closest rows down to human-useful near misses.

    A raw closest row can be mathematically closest while still being useless,
    e.g. expected profit of £17 against a £25 gate when the current bid is
    already above our max buy. A useful near miss must still be at or below the
    max-buy ceiling, within `near_window` of the gate, and have a positive max
    safe buy.
    """

    useful: list[Mapping[str, Any]] = []
    min_near_profit = max(float(min_profit or 0.0) - float(near_window or 0.0), 0.0)
    for row in rows:
        try:
            profit = float(row.get("expected_profit") or 0.0)
            max_safe_buy = float(row.get("max_safe_buy") or 0.0)
            current_price = float(row.get("current_price") or 0.0)
        except (TypeError, ValueError):
            continue
        if profit < min_near_profit or max_safe_buy < 0 or current_price > max_safe_buy:
            continue
        useful.append(row)
        if limit and len(useful) >= limit:
            break
    return useful


def run(
    *,
    limit: int = 20,
    min_score: float = 0.0,
    min_profit: float = MIN_PROFIT_GBP,
    min_roi: float = MIN_ROI,
    min_comps: int | None = None,
    include_future_unknown: bool = False,
    source: str | None = None,
) -> dict[str, Any]:
    """
    Run the read-only potentials query.

    `min_score` is accepted for old callers but ignored.
    """
    ensure_review_table()
    conn = schema.get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            schema.ensure_utc_session(cur)
            rows = _fetch_live_listing_comp_rows(
                cur,
                include_future_unknown=include_future_unknown,
                source=source,
            )
    finally:
        conn.close()

    potentials, checked, stats = build_potentials(
        rows,
        min_profit=min_profit,
        min_roi=min_roi,
        min_comps=min_comps,
    )
    limited_potentials = potentials[:limit] if limit else potentials
    closest = checked[:limit] if limit else checked

    return {
        "workflow": "live_listing_potentials",
        "mode": "read_only_sql_math",
        "thresholds": {
            "min_profit_gbp": min_profit,
            "min_roi": min_roi,
            "min_comps": min_comps,
            "min_comps_mode": "override" if min_comps is not None else "category",
            "category_min_comps": dict(CATEGORY_MIN_CANDIDATE_COMPS),
            "default_min_comps": MIN_CANDIDATE_COMPS,
            "source": source,
            "fee_rate": FEE_RATE,
            "outbound_shipping_gbp": OUTBOUND_SHIP_GBP,
            "inbound_shipping_gbp": INBOUND_SHIP_GBP,
            "packaging_gbp": PACKAGING_GBP,
            "selling_reserve_gbp": SELLING_RESERVE_GBP,
        },
        "summary": {
            **stats,
            "potentials": len(potentials),
        },
        "potentials": [asdict(row) for row in limited_potentials],
        "closest": [asdict(row) for row in closest],
    }
