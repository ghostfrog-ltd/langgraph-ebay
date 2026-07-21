"""
Rank candidate eBay market lanes from local PostgreSQL evidence.

This workflow is signal-only. It does not call eBay, create sources, bid, buy,
message sellers, relist items, or mutate marketplace state. It exists to decide
which eBay lanes have enough evidence to become reviewable buy leads by
comparing confirmed comp depth, active supply, post-cost spread, and identity
risk.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from statistics import median
from typing import Any

from psycopg2.extras import RealDictCursor

import utils.db_schema as schema
from utils.condition import has_investible_product_identity

FEE_RATE = 0.13
OUTBOUND_SHIP_GBP = 7.0
INBOUND_SHIP_GBP = 5.0
PACKAGING_GBP = 2.0
SELLING_RESERVE_GBP = 3.0
VEHICLE_LIKE_SOURCES = {
    "ebay-honda-msx125",
    "ebay-honda-nc750",
    "ebay-mini-cooper",
    "ebay-motors",
    "motomine",
}
STATUS_PRIORITY = {
    "buy_candidate": 4,
    "data_gap": 3,
    "parked": 2,
    "rejected": 1,
}


@dataclass
class MarketLane:
    """Aggregated local evidence for one source/model-key market lane."""

    source: str
    model_key: str
    active_count: int
    active_median_price: float
    active_min_price: float
    comp_samples: int
    comp_median_price: float
    guardrail_blocked_count: int


@dataclass
class MarketDiscoveryScore:
    """Review-only score and recommendation for a candidate market lane."""

    source: str
    model_key: str
    score: float
    status: str
    recommendation: str
    active_count: int
    active_median_price: float
    active_min_price: float
    comp_samples: int
    comp_median_price: float
    estimated_net_spread: float
    spread_ratio: float
    guardrail_blocked_share: float
    risk_flags: list[str]


def _money(value: float) -> float:
    return round(float(value or 0.0) + 1e-9, 2)


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def score_lane(lane: MarketLane) -> MarketDiscoveryScore:
    """
    Convert aggregated lane evidence into a conservative buy-signal score.

    The score is not permission to buy. It answers whether this lane has enough
    clean spread, comps, and supply to review as a possible eBay flip.
    """
    risk_flags: list[str] = []
    if not lane.model_key or lane.model_key.lower() == "unknown":
        risk_flags.append("unknown_model_key")
    if lane.comp_samples < 3:
        risk_flags.append("thin_confirmed_comps")
    if lane.active_count < 2:
        risk_flags.append("thin_active_supply")

    guardrail_share = (
        lane.guardrail_blocked_count / lane.active_count
        if lane.active_count > 0
        else 1.0
    )
    if guardrail_share >= 0.25:
        risk_flags.append("high_false_positive_risk")
    if lane.source in VEHICLE_LIKE_SOURCES:
        risk_flags.append("vehicle_logistics_not_modelled")

    estimated_net_spread = _money(
        lane.comp_median_price * (1.0 - FEE_RATE)
        - OUTBOUND_SHIP_GBP
        - INBOUND_SHIP_GBP
        - PACKAGING_GBP
        - SELLING_RESERVE_GBP
        - lane.active_median_price
    )
    spread_ratio = 0.0 if lane.active_median_price <= 0 else estimated_net_spread / lane.active_median_price
    if estimated_net_spread < 20:
        risk_flags.append("weak_post_cost_spread")
    if lane.active_median_price >= 300:
        risk_flags.append("higher_capital_required")

    comp_depth_score = _clamp(lane.comp_samples / 30.0) * 25.0
    supply_score = _clamp(lane.active_count / 10.0) * 20.0
    spread_score = _clamp(estimated_net_spread / 120.0) * 30.0
    identity_score = (1.0 - _clamp(guardrail_share)) * 15.0
    capital_score = (1.0 - _clamp((lane.active_median_price - 50.0) / 350.0)) * 10.0
    score = _money(comp_depth_score + supply_score + spread_score + identity_score + capital_score)

    if "vehicle_logistics_not_modelled" in risk_flags:
        status = "parked"
        recommendation = "Keep parked until transport, inspection, repair, storage, and paperwork reserves exist."
    elif "unknown_model_key" in risk_flags or "thin_confirmed_comps" in risk_flags:
        status = "data_gap"
        recommendation = "Data gap: clean model key or confirmed final-price coverage is too weak."
    elif "high_false_positive_risk" in risk_flags:
        status = "parked"
        recommendation = "Keep parked: identity/accessory risk is too high."
    elif estimated_net_spread >= 50 and lane.comp_samples >= 10 and lane.active_count >= 2:
        status = "buy_candidate"
        recommendation = "Review as a possible buy candidate; human approval still required."
    elif estimated_net_spread >= 20:
        status = "data_gap"
        recommendation = "Data gap: spread is possible, but supply or final-price depth is incomplete."
    else:
        status = "parked"
        recommendation = "Spread is too thin after costs for current assumptions."

    return MarketDiscoveryScore(
        source=lane.source,
        model_key=lane.model_key,
        score=score,
        status=status,
        recommendation=recommendation,
        active_count=lane.active_count,
        active_median_price=_money(lane.active_median_price),
        active_min_price=_money(lane.active_min_price),
        comp_samples=lane.comp_samples,
        comp_median_price=_money(lane.comp_median_price),
        estimated_net_spread=estimated_net_spread,
        spread_ratio=round(spread_ratio, 4),
        guardrail_blocked_share=round(guardrail_share, 4),
        risk_flags=risk_flags,
    )


def _load_active_rows(cur) -> list[dict[str, Any]]:
    """
    Load active listing evidence through the normalized read-only view surface.

    This signal report is intentionally read-only, so it should be one of the
    first consumers to move away from the wide `auction_listings` table.
    """
    cur.execute(
        """
        SELECT
            identity.source_label AS source,
            attrs.model_key,
            identity.title,
            observation.price_current
        FROM listing_identity_v AS identity
        JOIN listing_attribute_v AS attrs
          ON attrs.listing_id = identity.listing_id
        JOIN listing_observation_v AS observation
          ON observation.listing_id = identity.listing_id
        JOIN listing_outcome_v AS outcome
          ON outcome.listing_id = identity.listing_id
        WHERE outcome.is_live_like = TRUE
          AND observation.price_current IS NOT NULL
          AND attrs.model_key IS NOT NULL
          AND attrs.model_key <> ''
          AND LOWER(attrs.model_key) <> 'unknown'
        """
    )
    return [dict(row) for row in cur.fetchall()]


def _load_comp_map(cur) -> dict[str, dict[str, Any]]:
    cur.execute(
        """
        SELECT model_key, samples, median_final_price
        FROM latest_comps
        WHERE model_key IS NOT NULL
          AND COALESCE(samples, 0) > 0
          AND COALESCE(median_final_price, 0) > 0
        """
    )
    return {str(row["model_key"]): dict(row) for row in cur.fetchall()}


def build_lanes(active_rows: list[dict[str, Any]], comp_map: dict[str, dict[str, Any]]) -> list[MarketLane]:
    """
    Aggregate active listings into source/model-key lanes.

    Guardrail-blocked rows remain in the lane count because their presence is a
    useful market-quality signal: lanes full of accessories or repair-only rows
    are expensive to automate safely.
    """
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    for row in active_rows:
        source = str(row.get("source") or "")
        model_key = str(row.get("model_key") or "")
        price = float(row.get("price_current") or 0.0)
        if price <= 0:
            continue

        key = (source, model_key)
        bucket = grouped.setdefault(
            key,
            {
                "prices": [],
                "active_count": 0,
                "guardrail_blocked_count": 0,
            },
        )
        if not has_investible_product_identity(
            str(row.get("title") or ""),
            source=source,
            model_key=model_key,
        ):
            bucket["guardrail_blocked_count"] += 1
        else:
            bucket["prices"].append(price)
        bucket["active_count"] += 1

    lanes: list[MarketLane] = []
    for (source, model_key), data in grouped.items():
        comp = comp_map.get(model_key)
        prices = list(data["prices"])
        if not comp or not prices:
            continue
        lanes.append(
            MarketLane(
                source=source,
                model_key=model_key,
                active_count=int(data["active_count"]),
                active_median_price=float(median(prices)),
                active_min_price=float(min(prices)),
                comp_samples=int(comp.get("samples") or 0),
                comp_median_price=float(comp.get("median_final_price") or 0.0),
                guardrail_blocked_count=int(data["guardrail_blocked_count"]),
            )
        )
    return lanes


def run(*, limit: int = 20, min_score: float = 0.0) -> dict[str, Any]:
    """
    Build a ranked buy-signal report from local database evidence only.

    The report is intended for operator review and does not persist results.
    """
    conn = schema.get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            schema.ensure_utc_session(cur)
            active_rows = _load_active_rows(cur)
            comp_map = _load_comp_map(cur)
    finally:
        conn.close()

    lanes = build_lanes(active_rows, comp_map)
    scores = [score_lane(lane) for lane in lanes]
    scores = [score for score in scores if score.score >= min_score]
    scores.sort(key=lambda item: (STATUS_PRIORITY.get(item.status, 0), item.score), reverse=True)
    limited = scores[:limit] if limit else scores

    status_counts: dict[str, int] = {}
    for score in scores:
        status_counts[score.status] = status_counts.get(score.status, 0) + 1

    return {
        "workflow": "buy_signal_lanes",
        "mode": "read_only_local_evidence",
        "lanes_scored": len(scores),
        "status_counts": status_counts,
        "assumptions": {
            "fee_rate": FEE_RATE,
            "outbound_shipping_gbp": OUTBOUND_SHIP_GBP,
            "inbound_shipping_gbp": INBOUND_SHIP_GBP,
            "packaging_gbp": PACKAGING_GBP,
            "selling_reserve_gbp": SELLING_RESERVE_GBP,
        },
        "top_lanes": [asdict(score) for score in limited],
    }
