"""
Persist and update paper-trading predictions for current ROI candidates.

Paper trading is the learning loop before any real buy desk labels. This module
records what the system would have considered interesting at a point in time,
then later compares those predictions with observed listing outcomes in
PostgreSQL. It never bids, buys, messages, lists, or writes to eBay.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import os
from typing import Any, Optional, TypedDict

from langgraph.graph import END, StateGraph
from psycopg2.extras import RealDictCursor, execute_batch

import utils.db_schema as schema
from pipelines.listing.ended.graph import (
    _call_trading_get_item,
    _extract_numeric_item_id,
    _parse_trading_get_item,
)
from pipelines.listing.roi.graph import (
    MIN_PROFIT_GBP,
    Opportunity,
    _comps_lookup,
    _fetch_active_listings,
    _money,
    _shortlist,
)
from utils.condition import looks_like_partial_or_accessory_listing
from utils.logger import get_logger
from utils.usage_tracker import increment_api_usage

logger = get_logger(__name__)

TERMINAL_STATUSES = {"ended", "sold", "complete", "completed"}
DEFAULT_SNAPSHOT_LIMIT = int(os.getenv("GF_PAPER_TRADE_LIMIT", "25"))
DEFAULT_OUTCOME_RESOLVE_LIMIT = int(os.getenv("GF_PAPER_OUTCOME_RESOLVE_LIMIT", "5"))
OUTCOME_RESOLVE_GRACE_MINUTES = int(os.getenv("GF_PAPER_OUTCOME_GRACE_MINUTES", "60"))
OUTCOME_RECHECK_COOLDOWN_HOURS = int(os.getenv("GF_PAPER_OUTCOME_RECHECK_HOURS", "6"))
PAPER_INBOUND_SHIP_DEFAULT_GBP = float(os.getenv("GF_PAPER_INBOUND_SHIP_DEFAULT_GBP", "5.0"))
PAPER_PACKAGING_DEFAULT_GBP = float(os.getenv("GF_PAPER_PACKAGING_DEFAULT_GBP", "2.0"))
PAPER_SELLING_RESERVE_DEFAULT_GBP = float(os.getenv("GF_PAPER_SELLING_RESERVE_DEFAULT_GBP", "3.0"))


class PaperTradingState(TypedDict, total=False):
    """LangGraph-compatible state container for one paper-trading pass."""

    limit: int
    snapshot: bool
    resolve_outcomes: bool
    resolve_limit: int
    result: dict[str, Any]


@dataclass
class PaperTradeSnapshot:
    """One persisted review-only prediction for later outcome comparison."""

    external_id: str
    source: str
    title: str
    url: str
    model_key: Optional[str]
    current_price: float
    predicted_resale_price: float
    predicted_fees: float
    predicted_outbound_shipping: float
    predicted_inbound_shipping: float
    predicted_packaging: float
    predicted_selling_reserve: float
    predicted_profit: float
    predicted_roi: float
    recommended_max_bid: float
    comp_samples: int
    end_time: Any
    time_left_s: Optional[int]


@dataclass
class ObservedOutcome:
    """Computed paper-trade outcome from the latest local listing state."""

    prediction_id: int
    decision_status: str
    observed_status: str
    observed_final_price: Optional[float]
    observed_purchase_price: Optional[float]
    would_have_bid: Optional[bool]
    would_have_won: Optional[bool]
    observed_price_error: Optional[float]
    observed_paper_profit: Optional[float]
    observed_paper_roi: Optional[float]
    outcome_reason: str


@dataclass
class PaperOutcomeResolution:
    """
    Result of a Trading API outcome check for one overdue paper prediction.

    This is internal state only. It may update local PostgreSQL rows, but it
    never writes to eBay or turns a prediction into an executable buy signal.
    """

    prediction_id: int
    external_id: str
    listing_id: Optional[int]
    action: str
    listing_status: Optional[str]
    final_price: Optional[float]
    bid_count: int
    reason: str


@dataclass
class PaperTradeLesson:
    """
    Human-readable learning item derived from one paper-trade prediction.

    Lessons are advisory and review-only. They explain what the system should
    inspect or tune next; they are not buy labels and must not trigger capital
    deployment.
    """

    external_id: str
    source: str
    title: str
    model_key: Optional[str]
    decision_status: str
    observed_status: Optional[str]
    predicted_profit: float
    observed_paper_profit: Optional[float]
    observed_price_error: Optional[float]
    lesson_category: str
    learning_hint: str


def _paper_profit(
    *,
    resale_price: float,
    fees: float,
    outbound_shipping: float,
    inbound_shipping: float,
    packaging: float,
    selling_reserve: float,
    purchase_price: float,
) -> float:
    """
    Calculate paper profit using the same side of the trade as a real buy.

    `purchase_price` is the auction price we would have paid. Resale remains a
    conservative sold-comp estimate because paper trading has not actually
    bought, inspected, relisted, or resold the item.
    """
    return _money(
        resale_price
        - fees
        - outbound_shipping
        - inbound_shipping
        - packaging
        - selling_reserve
        - purchase_price
    )


def _paper_roi(*, profit: float, purchase_price: float, inbound_shipping: float, packaging: float) -> float:
    """
    Calculate ROI against cash tied up before resale.

    Selling fees and outbound postage are still charged in profit, but the
    denominator focuses on the money committed to acquire and prepare the item.
    """
    basis = purchase_price + inbound_shipping + packaging
    return 0.0 if basis <= 0 else profit / basis


def _recommended_max_bid(opportunity: Opportunity) -> float:
    """
    Derive a conservative paper max bid from the same evidence used by ROI.

    This is review-only and intentionally keeps at least the configured minimum
    profit between resale estimate and bid ceiling before any future real-money
    safety gate is allowed to use it.
    """
    cap = (
        opportunity.comps_median
        - opportunity.fees
        - opportunity.outbound_ship
        - PAPER_INBOUND_SHIP_DEFAULT_GBP
        - PAPER_PACKAGING_DEFAULT_GBP
        - PAPER_SELLING_RESERVE_DEFAULT_GBP
        - MIN_PROFIT_GBP
    )
    return _money(max(cap, 0.0))


def classify_trading_resolution(
    *,
    prediction_id: int,
    external_id: str,
    listing_id: Optional[int],
    parsed: Optional[dict[str, Any]],
) -> PaperOutcomeResolution:
    """
    Convert a Trading GetItem snapshot into a paper-outcome database action.

    The resolver is deliberately conservative:
    - confirmed ended with bids/final price becomes a local `sold` outcome;
    - confirmed ended without final price becomes a local `ended` outcome;
    - item-not-found becomes `ended` with no final price;
    - still-live or unparseable responses only audit the check and keep watching.
    """
    if not parsed:
        return PaperOutcomeResolution(
            prediction_id=prediction_id,
            external_id=external_id,
            listing_id=listing_id,
            action="keep_watching",
            listing_status=None,
            final_price=None,
            bid_count=0,
            reason="unparseable_trading_response",
        )

    if parsed.get("code") in {"1505", "21920397"}:
        return PaperOutcomeResolution(
            prediction_id=prediction_id,
            external_id=external_id,
            listing_id=listing_id,
            action="close",
            listing_status="ended",
            final_price=None,
            bid_count=0,
            reason=f"trading_item_not_found_{parsed.get('code')}",
        )

    if not bool(parsed.get("ended")):
        return PaperOutcomeResolution(
            prediction_id=prediction_id,
            external_id=external_id,
            listing_id=listing_id,
            action="keep_watching",
            listing_status=None,
            final_price=None,
            bid_count=int(parsed.get("bid_count") or 0),
            reason="trading_says_not_ended",
        )

    bid_count = int(parsed.get("bid_count") or 0)
    raw_final_price = parsed.get("final_price")
    sold_flag = bool(parsed.get("sold_flag"))
    final_price = float(raw_final_price) if sold_flag and raw_final_price is not None else None

    if final_price is not None:
        return PaperOutcomeResolution(
            prediction_id=prediction_id,
            external_id=external_id,
            listing_id=listing_id,
            action="close",
            listing_status="sold",
            final_price=final_price,
            bid_count=bid_count,
            reason="ended_with_confirmed_final_price",
        )

    return PaperOutcomeResolution(
        prediction_id=prediction_id,
        external_id=external_id,
        listing_id=listing_id,
        action="close",
        listing_status="ended",
        final_price=None,
        bid_count=bid_count,
        reason="ended_without_confirmed_sale",
    )


def classify_paper_trade_lesson(row: dict[str, Any]) -> PaperTradeLesson:
    """
    Convert a persisted prediction row into a compact learning classification.

    The thresholds are intentionally simple and conservative. For new paper
    rows, the observed eBay final price is treated as the hypothetical purchase
    price. We only call a trade profitable when that price stayed inside the
    stored max bid and the expected resale still clears all paper cost reserves.
    """
    predicted_profit = float(row.get("predicted_profit") or 0.0)
    predicted_resale = float(row.get("predicted_resale_price") or 0.0)
    observed_profit_raw = row.get("observed_paper_profit")
    price_error_raw = row.get("observed_price_error")
    observed_profit = float(observed_profit_raw) if observed_profit_raw is not None else None
    price_error = float(price_error_raw) if price_error_raw is not None else None
    decision_status = str(row.get("decision_status") or "watching")
    observed_status = row.get("observed_status")
    outcome_reason = str(row.get("outcome_reason") or "")
    would_have_won = row.get("would_have_won")

    if decision_status != "closed":
        lesson_category = "watching"
        learning_hint = "Wait for a terminal outcome before tuning from this prediction."
    elif outcome_reason == "guardrail_invalidated_before_outcome":
        lesson_category = "guardrail_invalidated"
        learning_hint = (
            "Current deterministic guardrails now block this prediction; keep it out of outcome "
            "resolution and review why it originally entered the shortlist."
        )
    elif outcome_reason == "terminal_without_confirmed_sale":
        lesson_category = "no_confirmed_sale"
        learning_hint = (
            "Closed without final price; do not use this as a resale comp. "
            "Prioritise final-price backfill only if the model key is active and comp-poor."
        )
    elif outcome_reason == "final_price_above_max_bid" or would_have_won is False:
        lesson_category = "outbid_above_ceiling"
        learning_hint = (
            "The auction closed above the conservative max bid; treat this as a disciplined no-buy, "
            "not a missed profit."
        )
    elif observed_profit is not None and observed_profit < 0:
        lesson_category = "paper_loss"
        learning_hint = (
            "Would-have-won purchase became a paper loss after fees, inbound postage, outbound "
            "postage, packaging, and selling reserves; inspect condition language and comp match."
        )
    elif (
        outcome_reason == "confirmed_final_price"
        and price_error is not None
        and predicted_resale > 0
        and price_error <= -(predicted_resale * 0.20)
    ):
        lesson_category = "overestimated_resale"
        learning_hint = (
            "Observed final price was more than 20% below the resale estimate; tighten comp grouping "
            "or add condition/cost reserves for this model key."
        )
    elif observed_profit is not None and observed_profit < MIN_PROFIT_GBP:
        lesson_category = "thin_margin"
        learning_hint = (
            "Paper profit landed below the minimum profit target; raise reserves or require stronger "
            "comp evidence before surfacing similar rows."
        )
    elif (
        outcome_reason == "confirmed_final_price"
        and price_error is not None
        and predicted_resale > 0
        and price_error >= predicted_resale * 0.20
    ):
        lesson_category = "underestimated_resale"
        learning_hint = (
            "Observed price beat the estimate by more than 20%; review whether the model key is too "
            "broad or the opportunity threshold is overly conservative."
        )
    else:
        lesson_category = "prediction_ok"
        learning_hint = "Prediction stayed inside current paper-trading tolerances."

    return PaperTradeLesson(
        external_id=str(row.get("external_id") or ""),
        source=str(row.get("source") or ""),
        title=str(row.get("title") or ""),
        model_key=row.get("model_key"),
        decision_status=decision_status,
        observed_status=str(observed_status) if observed_status is not None else None,
        predicted_profit=_money(predicted_profit),
        observed_paper_profit=_money(observed_profit) if observed_profit is not None else None,
        observed_price_error=_money(price_error) if price_error is not None else None,
        lesson_category=lesson_category,
        learning_hint=learning_hint,
    )


def snapshot_from_opportunity(opportunity: Opportunity) -> PaperTradeSnapshot:
    """
    Convert an ROI opportunity into an auditable paper-trade snapshot.

    Paper trading is deliberately stricter than the first-pass ROI row. It adds
    reserves for inbound postage, packaging, and listing/promoted/relist risk so
    that learning rows model the real buy-check-ship loop more closely.
    """
    current_price = _money(opportunity.purchase_cost)
    predicted_profit = _paper_profit(
        resale_price=float(opportunity.comps_median),
        fees=float(opportunity.fees),
        outbound_shipping=float(opportunity.outbound_ship),
        inbound_shipping=PAPER_INBOUND_SHIP_DEFAULT_GBP,
        packaging=PAPER_PACKAGING_DEFAULT_GBP,
        selling_reserve=PAPER_SELLING_RESERVE_DEFAULT_GBP,
        purchase_price=current_price,
    )
    predicted_roi = _paper_roi(
        profit=predicted_profit,
        purchase_price=current_price,
        inbound_shipping=PAPER_INBOUND_SHIP_DEFAULT_GBP,
        packaging=PAPER_PACKAGING_DEFAULT_GBP,
    )

    return PaperTradeSnapshot(
        external_id=opportunity.external_id,
        source=opportunity.source,
        title=opportunity.title,
        url=opportunity.url,
        model_key=opportunity.model_key,
        current_price=current_price,
        predicted_resale_price=_money(opportunity.comps_median),
        predicted_fees=_money(opportunity.fees),
        predicted_outbound_shipping=_money(opportunity.outbound_ship),
        predicted_inbound_shipping=_money(PAPER_INBOUND_SHIP_DEFAULT_GBP),
        predicted_packaging=_money(PAPER_PACKAGING_DEFAULT_GBP),
        predicted_selling_reserve=_money(PAPER_SELLING_RESERVE_DEFAULT_GBP),
        predicted_profit=predicted_profit,
        predicted_roi=float(predicted_roi),
        recommended_max_bid=_recommended_max_bid(opportunity),
        comp_samples=int(opportunity.comps_samples),
        end_time=opportunity.end_time,
        time_left_s=opportunity.time_left_s,
    )


def classify_observed_outcome(row: dict[str, Any]) -> ObservedOutcome:
    """
    Classify one prediction against current local listing state.

    A sold listing with `final_price` gives us the would-have-paid purchase
    price. It is only a profitable paper trade if that price stayed under the
    stored max bid and the expected resale clears the full paper cost stack.
    Ended rows without confirmed final price remain no-evidence closes.
    """
    prediction_id = int(row["id"])
    status = str(row.get("listing_status") or "").lower()
    final_price_raw = row.get("final_price")
    final_price = float(final_price_raw) if final_price_raw is not None else None
    predicted_resale = float(row.get("predicted_resale_price") or 0.0)
    predicted_fees = float(row.get("predicted_fees") or 0.0)
    outbound_shipping = float(row.get("predicted_outbound_shipping") or 0.0)
    inbound_shipping = float(row.get("predicted_inbound_shipping") or 0.0)
    packaging = float(row.get("predicted_packaging") or 0.0)
    selling_reserve = float(row.get("predicted_selling_reserve") or 0.0)
    recommended_max_bid = float(row.get("recommended_max_bid") or 0.0)

    if looks_like_partial_or_accessory_listing(
        str(row.get("title") or ""),
        source=str(row.get("source") or ""),
        model_key=str(row.get("model_key") or ""),
    ):
        return ObservedOutcome(
            prediction_id=prediction_id,
            decision_status="closed",
            observed_status="guardrail_blocked",
            observed_final_price=None,
            observed_purchase_price=None,
            would_have_bid=False,
            would_have_won=False,
            observed_price_error=None,
            observed_paper_profit=None,
            observed_paper_roi=None,
            outcome_reason="guardrail_invalidated_before_outcome",
        )

    if final_price is not None:
        would_have_bid = recommended_max_bid > 0
        would_have_won = would_have_bid and final_price <= recommended_max_bid
        observed_profit = (
            _paper_profit(
                resale_price=predicted_resale,
                fees=predicted_fees,
                outbound_shipping=outbound_shipping,
                inbound_shipping=inbound_shipping,
                packaging=packaging,
                selling_reserve=selling_reserve,
                purchase_price=final_price,
            )
            if would_have_won
            else None
        )
        observed_roi = (
            _paper_roi(
                profit=observed_profit,
                purchase_price=final_price,
                inbound_shipping=inbound_shipping,
                packaging=packaging,
            )
            if observed_profit is not None
            else None
        )
        return ObservedOutcome(
            prediction_id=prediction_id,
            decision_status="closed",
            observed_status="sold",
            observed_final_price=_money(final_price),
            observed_purchase_price=_money(final_price),
            would_have_bid=would_have_bid,
            would_have_won=would_have_won,
            observed_price_error=_money(final_price - recommended_max_bid),
            observed_paper_profit=observed_profit,
            observed_paper_roi=observed_roi,
            outcome_reason=(
                "confirmed_purchase_price_within_max_bid"
                if would_have_won
                else "final_price_above_max_bid"
            ),
        )

    if status in TERMINAL_STATUSES:
        return ObservedOutcome(
            prediction_id=prediction_id,
            decision_status="closed",
            observed_status=status or "ended",
            observed_final_price=None,
            observed_purchase_price=None,
            would_have_bid=None,
            would_have_won=None,
            observed_price_error=None,
            observed_paper_profit=None,
            observed_paper_roi=None,
            outcome_reason="terminal_without_confirmed_sale",
        )

    return ObservedOutcome(
        prediction_id=prediction_id,
        decision_status="watching",
        observed_status=status or "unknown",
        observed_final_price=None,
        observed_purchase_price=None,
        would_have_bid=None,
        would_have_won=None,
        observed_price_error=None,
        observed_paper_profit=None,
        observed_paper_roi=None,
        outcome_reason="listing_still_open",
    )


def ensure_tables(cur) -> None:
    """
    Create the paper-trading persistence contract in PostgreSQL.

    The unique `(external_id, prediction_date)` key prevents hourly pipeline
    runs from flooding the table with duplicate same-day predictions while still
    preserving a daily time series if a listing remains interesting.
    """
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_trade_predictions (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
            prediction_date DATE NOT NULL DEFAULT CURRENT_DATE,
            external_id TEXT NOT NULL,
            source TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT,
            model_key TEXT,
            current_price NUMERIC(12, 2) NOT NULL,
            predicted_resale_price NUMERIC(12, 2) NOT NULL,
            predicted_fees NUMERIC(12, 2) NOT NULL,
            predicted_outbound_shipping NUMERIC(12, 2) NOT NULL,
            predicted_inbound_shipping NUMERIC(12, 2) NOT NULL DEFAULT 0,
            predicted_packaging NUMERIC(12, 2) NOT NULL DEFAULT 0,
            predicted_selling_reserve NUMERIC(12, 2) NOT NULL DEFAULT 0,
            predicted_profit NUMERIC(12, 2) NOT NULL,
            predicted_roi NUMERIC(10, 4) NOT NULL,
            recommended_max_bid NUMERIC(12, 2) NOT NULL,
            comp_samples INTEGER NOT NULL,
            end_time TIMESTAMPTZ,
            time_left_s INTEGER,
            decision_status TEXT NOT NULL DEFAULT 'watching',
            observed_status TEXT,
            observed_final_price NUMERIC(12, 2),
            observed_purchase_price NUMERIC(12, 2),
            would_have_bid BOOLEAN,
            would_have_won BOOLEAN,
            observed_at TIMESTAMPTZ,
            observed_price_error NUMERIC(12, 2),
            observed_paper_profit NUMERIC(12, 2),
            observed_paper_roi NUMERIC(10, 4),
            outcome_reason TEXT,
            outcome_checked_at TIMESTAMPTZ,
            outcome_check_count INTEGER NOT NULL DEFAULT 0,
            outcome_last_reason TEXT,
            UNIQUE (external_id, prediction_date)
        )
        """
    )
    cur.execute(
        """
        ALTER TABLE paper_trade_predictions
            ADD COLUMN IF NOT EXISTS predicted_inbound_shipping NUMERIC(12, 2) NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS predicted_packaging NUMERIC(12, 2) NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS predicted_selling_reserve NUMERIC(12, 2) NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS observed_purchase_price NUMERIC(12, 2),
            ADD COLUMN IF NOT EXISTS would_have_bid BOOLEAN,
            ADD COLUMN IF NOT EXISTS would_have_won BOOLEAN,
            ADD COLUMN IF NOT EXISTS outcome_checked_at TIMESTAMPTZ,
            ADD COLUMN IF NOT EXISTS outcome_check_count INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN IF NOT EXISTS outcome_last_reason TEXT
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS paper_trade_predictions_status_idx
            ON paper_trade_predictions (decision_status, created_at)
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS paper_trade_predictions_outcome_due_idx
            ON paper_trade_predictions (decision_status, end_time, outcome_checked_at)
        """
    )


def collect_current_snapshots(limit: int) -> list[PaperTradeSnapshot]:
    """
    Build paper-trade snapshots from the current deterministic ROI shortlist.

    We refresh every current ROI shortlist row, even if the stricter paper cost
    reserves pull it below the future buy-signal threshold. That keeps same-day
    rows from carrying old assumptions and gives the learning loop visibility
    into which candidates are only marginal after real-world costs.
    """
    listings = _fetch_active_listings()
    comps_by_model = _comps_lookup()
    opportunities = _shortlist(listings, comps_by_model)
    return [snapshot_from_opportunity(opportunity) for opportunity in opportunities[:limit]]


def persist_snapshots(snapshots: list[PaperTradeSnapshot]) -> int:
    """
    Insert or refresh today's paper-trade predictions.

    Re-running the pipeline on the same day updates price/evidence fields for
    the same listing instead of duplicating it.
    """
    if not snapshots:
        return 0

    rows = [
        (
            snap.external_id,
            snap.source,
            snap.title,
            snap.url,
            snap.model_key,
            snap.current_price,
            snap.predicted_resale_price,
            snap.predicted_fees,
            snap.predicted_outbound_shipping,
            snap.predicted_inbound_shipping,
            snap.predicted_packaging,
            snap.predicted_selling_reserve,
            snap.predicted_profit,
            snap.predicted_roi,
            snap.recommended_max_bid,
            snap.comp_samples,
            snap.end_time,
            snap.time_left_s,
        )
        for snap in snapshots
    ]

    conn = schema.get_fresh_connection()
    try:
        with conn.cursor() as cur:
            schema.ensure_utc_session(cur)
            ensure_tables(cur)
            execute_batch(
                cur,
                """
                INSERT INTO paper_trade_predictions (
                    external_id,
                    source,
                    title,
                    url,
                    model_key,
                    current_price,
                    predicted_resale_price,
                    predicted_fees,
                    predicted_outbound_shipping,
                    predicted_inbound_shipping,
                    predicted_packaging,
                    predicted_selling_reserve,
                    predicted_profit,
                    predicted_roi,
                    recommended_max_bid,
                    comp_samples,
                    end_time,
                    time_left_s
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s
                )
                ON CONFLICT (external_id, prediction_date)
                DO UPDATE SET
                    updated_at = CURRENT_TIMESTAMP,
                    source = EXCLUDED.source,
                    title = EXCLUDED.title,
                    url = EXCLUDED.url,
                    model_key = EXCLUDED.model_key,
                    current_price = EXCLUDED.current_price,
                    predicted_resale_price = EXCLUDED.predicted_resale_price,
                    predicted_fees = EXCLUDED.predicted_fees,
                    predicted_outbound_shipping = EXCLUDED.predicted_outbound_shipping,
                    predicted_inbound_shipping = EXCLUDED.predicted_inbound_shipping,
                    predicted_packaging = EXCLUDED.predicted_packaging,
                    predicted_selling_reserve = EXCLUDED.predicted_selling_reserve,
                    predicted_profit = EXCLUDED.predicted_profit,
                    predicted_roi = EXCLUDED.predicted_roi,
                    recommended_max_bid = EXCLUDED.recommended_max_bid,
                    comp_samples = EXCLUDED.comp_samples,
                    end_time = EXCLUDED.end_time,
                    time_left_s = EXCLUDED.time_left_s
                """,
                rows,
                page_size=100,
            )
        conn.commit()
    finally:
        conn.close()

    return len(snapshots)


def update_observations() -> dict[str, int]:
    """Update open paper predictions with the latest observed listing outcome."""
    conn = schema.get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            schema.ensure_utc_session(cur)
            ensure_tables(cur)
            cur.execute(
                """
                SELECT
                    p.id,
                    p.current_price,
                    p.predicted_resale_price,
                    p.predicted_fees,
                    p.predicted_outbound_shipping,
                    p.predicted_inbound_shipping,
                    p.predicted_packaging,
                    p.predicted_selling_reserve,
                    p.recommended_max_bid,
                    p.source,
                    p.title,
                    p.model_key,
                    l.status AS listing_status,
                    l.final_price
                FROM paper_trade_predictions p
                LEFT JOIN auction_listings l
                  ON l.external_id = p.external_id
                WHERE p.decision_status <> 'closed'
                ORDER BY p.created_at ASC
                """
            )
            outcomes = [classify_observed_outcome(dict(row)) for row in cur.fetchall()]

            if outcomes:
                execute_batch(
                    cur,
                    """
                    UPDATE paper_trade_predictions
                    SET decision_status = %s,
                        observed_status = %s,
                        observed_final_price = %s,
                        observed_purchase_price = %s,
                        would_have_bid = %s,
                        would_have_won = %s,
                        observed_at = CASE
                            WHEN %s = 'closed' THEN CURRENT_TIMESTAMP
                            ELSE observed_at
                        END,
                        observed_price_error = %s,
                        observed_paper_profit = %s,
                        observed_paper_roi = %s,
                        outcome_reason = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    [
                        (
                            outcome.decision_status,
                            outcome.observed_status,
                            outcome.observed_final_price,
                            outcome.observed_purchase_price,
                            outcome.would_have_bid,
                            outcome.would_have_won,
                            outcome.decision_status,
                            outcome.observed_price_error,
                            outcome.observed_paper_profit,
                            outcome.observed_paper_roi,
                            outcome.outcome_reason,
                            outcome.prediction_id,
                        )
                        for outcome in outcomes
                    ],
                    page_size=100,
                )
        conn.commit()
    finally:
        conn.close()

    counts: dict[str, int] = {}
    for outcome in outcomes:
        key = f"{outcome.decision_status}:{outcome.outcome_reason}"
        counts[key] = counts.get(key, 0) + 1
    return counts


def _load_overdue_outcome_candidates(limit: int) -> list[dict[str, Any]]:
    """
    Select watched predictions that are past end time and due for API resolution.

    The cooldown avoids repeatedly spending Trading calls when eBay still
    reports a listing as not ended despite the local scheduled end time.
    """
    if limit <= 0:
        return []

    conn = schema.get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            schema.ensure_utc_session(cur)
            ensure_tables(cur)
            cur.execute(
                """
                SELECT
                    p.id AS prediction_id,
                    p.external_id,
                    p.end_time,
                    p.outcome_checked_at,
                    l.id AS listing_id
                FROM paper_trade_predictions p
                LEFT JOIN auction_listings l
                  ON l.external_id = p.external_id
                WHERE p.decision_status = 'watching'
                  AND p.end_time IS NOT NULL
                  AND p.end_time <= CURRENT_TIMESTAMP - (%s || ' minutes')::interval
                  AND (
                      p.outcome_checked_at IS NULL
                      OR p.outcome_checked_at <= CURRENT_TIMESTAMP - (%s || ' hours')::interval
                  )
                ORDER BY p.end_time ASC, p.created_at ASC
                LIMIT %s
                """,
                (OUTCOME_RESOLVE_GRACE_MINUTES, OUTCOME_RECHECK_COOLDOWN_HOURS, limit),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _apply_outcome_resolutions(resolutions: list[PaperOutcomeResolution]) -> int:
    """
    Persist local outcome resolution data to PostgreSQL.

    Closed resolutions update the local `auction_listings` row when present and
    the paper-trade row. Keep-watching resolutions only record audit fields so
    they are not rechecked immediately.
    """
    if not resolutions:
        return 0

    conn = schema.get_fresh_connection()
    try:
        with conn.cursor() as cur:
            schema.ensure_utc_session(cur)
            ensure_tables(cur)
            execute_batch(
                cur,
                """
                UPDATE paper_trade_predictions
                SET outcome_checked_at = CURRENT_TIMESTAMP,
                    outcome_check_count = COALESCE(outcome_check_count, 0) + 1,
                    outcome_last_reason = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
                """,
                [(resolution.reason, resolution.prediction_id) for resolution in resolutions],
                page_size=100,
            )

            sold_listing_rows = [
                (resolution.final_price, resolution.bid_count, resolution.listing_id)
                for resolution in resolutions
                if resolution.action == "close"
                and resolution.listing_status == "sold"
                and resolution.listing_id is not None
            ]
            ended_listing_rows = [
                (resolution.bid_count, resolution.listing_id)
                for resolution in resolutions
                if resolution.action == "close"
                and resolution.listing_status == "ended"
                and resolution.listing_id is not None
            ]
            closed_prediction_rows = [
                (
                    resolution.listing_status,
                    resolution.final_price,
                    resolution.prediction_id,
                )
                for resolution in resolutions
                if resolution.action == "close"
            ]

            if sold_listing_rows:
                execute_batch(
                    cur,
                    """
                    UPDATE auction_listings
                    SET finalized = TRUE,
                        status = 'sold',
                        final_price = %s,
                        bids_count = %s
                    WHERE id = %s
                    """,
                    sold_listing_rows,
                    page_size=100,
                )

            if ended_listing_rows:
                execute_batch(
                    cur,
                    """
                    UPDATE auction_listings
                    SET finalized = TRUE,
                        status = 'ended',
                        bids_count = %s
                    WHERE id = %s
                    """,
                    ended_listing_rows,
                    page_size=100,
                )

            if closed_prediction_rows:
                execute_batch(
                    cur,
                    """
                    UPDATE paper_trade_predictions
                    SET decision_status = 'closed',
                        observed_status = %s,
                        observed_final_price = %s,
                        observed_purchase_price = %s,
                        would_have_bid = CASE
                            WHEN %s IS NULL THEN NULL
                            ELSE recommended_max_bid > 0
                        END,
                        would_have_won = CASE
                            WHEN %s IS NULL THEN NULL
                            ELSE recommended_max_bid > 0 AND %s <= recommended_max_bid
                        END,
                        observed_at = CURRENT_TIMESTAMP,
                        observed_price_error = CASE
                            WHEN %s IS NULL THEN NULL
                            ELSE %s - recommended_max_bid
                        END,
                        observed_paper_profit = CASE
                            WHEN %s IS NULL OR %s > recommended_max_bid THEN NULL
                            ELSE predicted_resale_price
                                - predicted_fees
                                - predicted_outbound_shipping
                                - COALESCE(predicted_inbound_shipping, 0)
                                - COALESCE(predicted_packaging, 0)
                                - COALESCE(predicted_selling_reserve, 0)
                                - %s
                        END,
                        observed_paper_roi = CASE
                            WHEN %s IS NULL
                              OR %s > recommended_max_bid
                              OR (%s + COALESCE(predicted_inbound_shipping, 0) + COALESCE(predicted_packaging, 0)) <= 0
                            THEN NULL
                            ELSE (
                                predicted_resale_price
                                - predicted_fees
                                - predicted_outbound_shipping
                                - COALESCE(predicted_inbound_shipping, 0)
                                - COALESCE(predicted_packaging, 0)
                                - COALESCE(predicted_selling_reserve, 0)
                                - %s
                            ) / (%s + COALESCE(predicted_inbound_shipping, 0) + COALESCE(predicted_packaging, 0))
                        END,
                        outcome_reason = CASE
                            WHEN %s IS NULL THEN 'terminal_without_confirmed_sale'
                            WHEN %s <= recommended_max_bid THEN 'confirmed_purchase_price_within_max_bid'
                            ELSE 'final_price_above_max_bid'
                        END,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    [
                        (
                            status,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            final_price,
                            prediction_id,
                        )
                        for status, final_price, prediction_id in closed_prediction_rows
                    ],
                    page_size=100,
                )

        conn.commit()
    finally:
        conn.close()

    return sum(1 for resolution in resolutions if resolution.action == "close")


def resolve_overdue_outcomes(*, limit: int = DEFAULT_OUTCOME_RESOLVE_LIMIT) -> dict[str, Any]:
    """
    Resolve overdue watched paper trades using a small capped Trading API batch.

    This is the paper-trading equivalent of targeted final-price backfill. It
    spends calls only on predictions whose end time has passed, then feeds the
    local result back into the existing observation and lesson pipeline.
    """
    candidates = _load_overdue_outcome_candidates(limit)
    resolutions: list[PaperOutcomeResolution] = []

    for row in candidates:
        prediction_id = int(row.get("prediction_id") or 0)
        external_id = str(row.get("external_id") or "")
        listing_id = row.get("listing_id")
        item_id = _extract_numeric_item_id(external_id)
        if not item_id:
            resolutions.append(
                PaperOutcomeResolution(
                    prediction_id=prediction_id,
                    external_id=external_id,
                    listing_id=int(listing_id) if listing_id is not None else None,
                    action="keep_watching",
                    listing_status=None,
                    final_price=None,
                    bid_count=0,
                    reason="invalid_external_id",
                )
            )
            continue

        increment_api_usage("ebay_ended_v1")
        response = _call_trading_get_item(item_id)
        parsed = _parse_trading_get_item(response.text)
        resolutions.append(
            classify_trading_resolution(
                prediction_id=prediction_id,
                external_id=external_id,
                listing_id=int(listing_id) if listing_id is not None else None,
                parsed=parsed,
            )
        )

    closed = _apply_outcome_resolutions(resolutions)
    counts: dict[str, int] = {}
    for resolution in resolutions:
        key = f"{resolution.action}:{resolution.listing_status or 'watching'}:{resolution.reason}"
        counts[key] = counts.get(key, 0) + 1

    return {
        "candidate_count": len(candidates),
        "checked": len(resolutions),
        "closed": closed,
        "counts": counts,
        "outcomes": [asdict(resolution) for resolution in resolutions],
    }


def summary() -> dict[str, Any]:
    """Return compact paper-trading health metrics for CLI/email reporting."""
    conn = schema.get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            schema.ensure_utc_session(cur)
            ensure_tables(cur)
            cur.execute(
                """
                SELECT
                    COUNT(*)::int AS total_predictions,
                    COUNT(*) FILTER (WHERE decision_status = 'watching')::int AS watching,
                    COUNT(*) FILTER (
                        WHERE decision_status = 'watching'
                          AND end_time IS NOT NULL
                          AND end_time <= CURRENT_TIMESTAMP
                    )::int AS watching_past_end,
                    COUNT(*) FILTER (
                        WHERE outcome_checked_at::date = CURRENT_DATE
                    )::int AS checked_today,
                    COUNT(*) FILTER (
                        WHERE decision_status = 'closed'
                          AND observed_at::date = CURRENT_DATE
                    )::int AS closed_today,
                    COUNT(*) FILTER (WHERE decision_status = 'closed')::int AS closed,
                    COUNT(*) FILTER (WHERE observed_status = 'sold')::int AS sold,
                    COUNT(*) FILTER (
                        WHERE observed_status = 'sold'
                          AND observed_paper_profit > 0
                    )::int AS profitable_on_paper,
                    COUNT(*) FILTER (WHERE would_have_won IS TRUE)::int AS would_have_won,
                    COUNT(*) FILTER (
                        WHERE outcome_reason = 'final_price_above_max_bid'
                    )::int AS outbid_above_ceiling,
                    AVG(observed_price_error) FILTER (
                        WHERE observed_price_error IS NOT NULL
                    )::numeric AS avg_price_error,
                    AVG(observed_paper_roi) FILTER (
                        WHERE observed_paper_roi IS NOT NULL
                    )::numeric AS avg_observed_roi
                FROM paper_trade_predictions
                """
            )
            totals = dict(cur.fetchone() or {})
            cur.execute(
                """
                SELECT source, COUNT(*)::int AS predictions
                FROM paper_trade_predictions
                WHERE prediction_date = CURRENT_DATE
                GROUP BY source
                ORDER BY predictions DESC, source
                """
            )
            today_by_source = [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

    return {
        "totals": totals,
        "today_by_source": today_by_source,
    }


def lesson_report(limit: int = 10) -> dict[str, Any]:
    """
    Build the current paper-trading miss/lesson report.

    Closed predictions are ordered first because they carry evidence. Watching
    rows are included only as context when there are few or no closed lessons.
    """
    conn = schema.get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            schema.ensure_utc_session(cur)
            ensure_tables(cur)
            cur.execute(
                """
                SELECT
                    external_id,
                    source,
                    title,
                    model_key,
                    predicted_resale_price,
                    predicted_profit,
                    decision_status,
                    observed_status,
                    would_have_won,
                    observed_price_error,
                    observed_paper_profit,
                    outcome_reason,
                    observed_at,
                    created_at
                FROM paper_trade_predictions
                ORDER BY
                    CASE WHEN decision_status = 'closed' THEN 0 ELSE 1 END,
                    observed_at DESC NULLS LAST,
                    created_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            lessons = [classify_paper_trade_lesson(dict(row)) for row in cur.fetchall()]
    finally:
        conn.close()

    counts: dict[str, int] = {}
    for lesson in lessons:
        counts[lesson.lesson_category] = counts.get(lesson.lesson_category, 0) + 1

    return {
        "lesson_counts": counts,
        "lessons": [asdict(lesson) for lesson in lessons],
    }


def run(
    *,
    limit: int = 25,
    snapshot: bool = True,
    resolve_outcomes: bool = True,
    resolve_limit: int = DEFAULT_OUTCOME_RESOLVE_LIMIT,
) -> dict[str, Any]:
    """Run one deterministic paper-trading pipeline pass."""
    snapshots = collect_current_snapshots(limit) if snapshot else []
    inserted_or_updated = persist_snapshots(snapshots) if snapshot else 0
    observation_counts = update_observations()
    resolution_summary = (
        resolve_overdue_outcomes(limit=resolve_limit)
        if resolve_outcomes
        else {"candidate_count": 0, "checked": 0, "closed": 0, "counts": {}, "outcomes": []}
    )
    metrics = summary()
    lessons = lesson_report(limit=10)

    logger.info(
        "[listing.paper_trading] snapshots=%d observation_groups=%s",
        inserted_or_updated,
        observation_counts,
    )

    return {
        "snapshots": inserted_or_updated,
        "observation_counts": observation_counts,
        "outcome_resolution": resolution_summary,
        "snapshot_preview": [asdict(snap) for snap in snapshots[:10]],
        "lesson_report": lessons,
        **metrics,
    }


def _node_run(state: PaperTradingState) -> PaperTradingState:
    """LangGraph node wrapper for the deterministic paper-trading pass."""
    limit = int(state.get("limit") or DEFAULT_SNAPSHOT_LIMIT)
    snapshot = bool(state.get("snapshot", True))
    resolve_outcomes = bool(state.get("resolve_outcomes", True))
    resolve_limit = int(state.get("resolve_limit") or DEFAULT_OUTCOME_RESOLVE_LIMIT)
    return {
        "result": run(
            limit=limit,
            snapshot=snapshot,
            resolve_outcomes=resolve_outcomes,
            resolve_limit=resolve_limit,
        )
    }


def build_graph():
    """Build the single-pass paper-trading LangGraph pipeline."""
    graph = StateGraph(PaperTradingState)
    graph.add_node("paper_trading", _node_run)
    graph.set_entry_point("paper_trading")
    graph.add_edge("paper_trading", END)
    return graph.compile()
