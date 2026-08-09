"""
Read-only live listing potentials from local PostgreSQL evidence.

This module answers one plain question:

Which live eBay listings are priced below our safe buy price for the same
`model_key`, using confirmed sold comps only?
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping

from psycopg2.extras import RealDictCursor

import utils.db_schema as schema
from utils.condition import has_investible_product_identity
from pipelines.listing.review_actions import ensure_review_table

FEE_RATE = 0.13
OUTBOUND_SHIP_GBP = 7.0
INBOUND_SHIP_GBP = 5.0
PACKAGING_GBP = 2.0
SELLING_RESERVE_GBP = 3.0
MIN_PROFIT_GBP = 50.0
MIN_ROI = 0.0
MIN_CANDIDATE_COMPS = 15
NEAR_MISS_WINDOW_GBP = 10.0


@dataclass
class ListingPotential:
    """One live listing compared directly against same-model sold comps."""

    source: str
    external_id: str
    title: str
    url: str
    model_key: str
    condition_grade: str
    sale_type: str
    end_time: Any
    current_price: float
    sold_median: float
    comp_samples: int
    max_safe_buy: float
    expected_profit: float
    expected_roi: float
    price_gap_to_max_buy: float
    signal: str


def _money(value: float) -> float:
    return round(float(value or 0.0) + 1e-9, 2)


def _expected_profit(*, current_price: float, sold_median: float) -> float:
    return _money(
        sold_median * (1.0 - FEE_RATE)
        - OUTBOUND_SHIP_GBP
        - INBOUND_SHIP_GBP
        - PACKAGING_GBP
        - SELLING_RESERVE_GBP
        - current_price
    )


def _max_safe_buy(*, sold_median: float, min_profit: float) -> float:
    return _money(
        sold_median * (1.0 - FEE_RATE)
        - OUTBOUND_SHIP_GBP
        - INBOUND_SHIP_GBP
        - PACKAGING_GBP
        - SELLING_RESERVE_GBP
        - min_profit
    )


def potential_from_row(
    row: dict[str, Any],
    *,
    min_profit: float = MIN_PROFIT_GBP,
) -> ListingPotential:
    """
    Convert one live listing + comp row into deterministic buy maths.

    No LLM judgement, no lane scoring, no status buckets.
    """
    current_price = float(row.get("price_current") or 0.0)
    sold_median = float(row.get("median_final_price") or 0.0)
    comp_samples = int(row.get("samples") or 0)
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
        condition_grade=str(row.get("condition_grade") or "B"),
        sale_type=str(row.get("sale_type") or ""),
        end_time=row.get("end_time"),
        current_price=_money(current_price),
        sold_median=_money(sold_median),
        comp_samples=comp_samples,
        max_safe_buy=max_buy,
        expected_profit=profit,
        expected_roi=round(roi, 4),
        price_gap_to_max_buy=gap,
        signal=(
            f"current £{_money(current_price)} vs max buy £{max_buy}; "
            f"sold median £{_money(sold_median)} from {comp_samples} same-grade comps; "
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
    params: list[Any] = []
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
            COALESCE(a.condition_grade, 'B') AS condition_grade,
            a.sale_type,
            a.raw_attrs,
            a.end_time,
            COALESCE(a.price_current, 0) AS price_current,
            lc.samples,
            lc.median_final_price
        FROM auction_listings AS a
        JOIN latest_comps AS lc
          ON lc.model_key = a.model_key
         AND lc.condition_grade = COALESCE(a.condition_grade, 'B')
        WHERE {status_predicate}
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
    min_comps: int = MIN_CANDIDATE_COMPS,
) -> tuple[list[ListingPotential], list[ListingPotential], dict[str, int]]:
    """
    Return rows that clear the buy maths, plus closest misses for debugging.

    `potentials` are live rows where current price is below max safe buy,
    expected profit clears `min_profit`, and comp depth is at least `min_comps`.
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

        potential = potential_from_row(row, min_profit=min_profit)
        if potential.comp_samples < min_comps:
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
    min_comps: int = MIN_CANDIDATE_COMPS,
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
