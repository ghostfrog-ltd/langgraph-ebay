"""Read approved GhostFrog reports via stdio MCP or a manual JSON export."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from contextlib import closing, contextmanager
from datetime import datetime, timezone
import json
import re
from statistics import median
from typing import Annotated, Any, Literal

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
import psycopg2
from psycopg2.extras import RealDictCursor
from pydantic import Field

from pipelines.listing.attributes.mk import normalise_model
from pipelines.listing.attributes.mk.apple import macbook_specs_conflict
from pipelines.listing.market_discovery.pricing import (
    MIN_PROFIT_GBP,
    effective_min_comps_for_source,
    expected_profit,
    max_safe_buy,
)
from utils.condition import effective_condition_grade, looks_like_partial_or_accessory_listing

SOURCE = "ebay-apple"
ROLE = "ghostfrog_mcp"
CONNECT_TIMEOUT_SECONDS = 3
STATEMENT_TIMEOUT_MS = 5000
COMP_DAYS = 90
COMP_CANDIDATE_LIMIT = 200
STRICT_MACBOOK_KEY = re.compile(
    r"apple-macbook(?:-air|-pro)?-a[0-9]{4}-[0-9]{2,3}in-"
    r"(?:m[1-9](?:pro|max|ultra)?|core-[im][3579])-[0-9]{1,3}gb-[0-9]{1,4}(?:gb|tb)"
)
MCP = FastMCP(
    "GhostFrog",
    instructions="Use these read-only tools for live GhostFrog PostgreSQL evidence. "
    "Explain missing evidence and use the returned timestamps and provenance.",
)
READ_ONLY = ToolAnnotations(readOnlyHint=True, destructiveHint=False, idempotentHint=True, openWorldHint=False)
RowLimit = Annotated[int, Field(strict=True, ge=1, le=25)]
ItemID = Annotated[str, Field(pattern=r"^[0-9]{12}$")]

LISTING_FILTER = """
    SELECT a.id, a.external_id, a.title, a.raw_attrs, a.model_key, a.condition_grade,
           a.status, a.price_current, a.final_price, a.bids_count, a.end_time, a.last_seen_at
    FROM public.auction_listings a
    WHERE a.source = 'ebay-apple'
      AND a.external_id ~ '^(v1[|][0-9]{12}[|]0|[0-9]{12})$'
"""
LISTING_DETAILS = """
    SELECT a.*, tc.text_grade, vc.visual_grade,
           COALESCE(tc.review_required, false) OR COALESCE(vc.review_required, false) AS review_required,
           (SELECT r.action FROM public.listing_review_actions r
            WHERE r.external_id = a.external_id
            ORDER BY r.created_at DESC, r.id DESC LIMIT 1) AS review_action
    FROM candidates a
    LEFT JOIN LATERAL (
        SELECT text_grade, review_required FROM public.latest_listing_text_conditions
        WHERE listing_id = a.id LIMIT 1
    ) tc ON true
    LEFT JOIN LATERAL (
        SELECT visual_grade, review_required FROM public.latest_listing_visual_conditions
        WHERE listing_id = a.id LIMIT 1
    ) vc ON true
"""
CONFIRMED_SALE = """
      AND a.status = 'sold' AND a.finalized IS TRUE AND a.final_price > 0
      AND a.end_time <= now()
"""
LIVE_AUCTION = """
      AND a.status = 'live' AND a.sale_type = 'auction' AND a.end_time > now()
      AND a.last_seen_at >= now() - interval '24 hours'
"""
LIVE_ORDER = " ORDER BY a.last_seen_at DESC, a.id DESC"
SOLD_ORDER = " ORDER BY a.end_time DESC, a.id DESC"


@contextmanager
def _read_cursor():
    try:
        with closing(psycopg2.connect(
            service=ROLE,
            user=ROLE,
            host="127.0.0.1",
            hostaddr="127.0.0.1",
            connect_timeout=CONNECT_TIMEOUT_SECONDS,
            options=f"-c statement_timeout={STATEMENT_TIMEOUT_MS} "
            "-c lock_timeout=1500 -c search_path=pg_catalog -c timezone=UTC",
        )) as conn:
            conn.set_session(readonly=True)
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                yield cursor
    except psycopg2.Error:
        raise ToolError("GhostFrog PostgreSQL is unavailable or the read timed out.") from None


def _result(relations: str, **values: Any) -> dict[str, Any]:
    return {
        "queried_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "provenance": f"GhostFrog live PostgreSQL / {relations}",
        **values,
    }


def _select_listings(cur, predicate: str, ordering: str, params: tuple, limit: int) -> None:
    # Bound candidates before consulting the condition and review records.
    cur.execute(
        "WITH candidates AS MATERIALIZED (" + LISTING_FILTER + predicate + ordering + " LIMIT %s) "
        + LISTING_DETAILS + ordering,
        (*params, limit),
    )


def _identity_and_reject(row: Mapping[str, Any]) -> tuple[str, str | None]:
    title = row["title"] or ""
    attrs = row["raw_attrs"] if isinstance(row["raw_attrs"], Mapping) else {}
    stored_key = row["model_key"] or ""
    if looks_like_partial_or_accessory_listing(title, attrs, source=SOURCE, model_key=stored_key):
        return "unknown", "parts_accessory_repair_or_lock_risk"
    if (
        not STRICT_MACBOOK_KEY.fullmatch(stored_key)
        or normalise_model(title, attrs, SOURCE) != stored_key
        or macbook_specs_conflict(attrs, title)
    ):
        return "unknown", "missing_or_conflicting_model_identity"
    if effective_condition_grade(row["condition_grade"], row["visual_grade"], row["text_grade"]) == "D":
        return stored_key, "functional_risk"
    if row["review_required"] or row["review_action"] == "down":
        return stored_key, "condition_or_operator_review"
    return stored_key, None


def _listing(row: Mapping[str, Any]) -> dict[str, Any]:
    external_id = row["external_id"]
    model_key, reject_reason = _identity_and_reject(row)
    return {
        "item_id": external_id.split("|")[1] if external_id.startswith("v1|") else external_id,
        "source": SOURCE,
        "model_key": model_key,
        "condition_grade": effective_condition_grade(row["condition_grade"], row["visual_grade"], row["text_grade"]),
        "status": row["status"],
        "current_price_gbp": row["price_current"],
        "final_price_gbp": float(row["final_price"]) if row["status"] == "sold" else None,
        "bids_count": row["bids_count"],
        "end_time": row["end_time"],
        "last_seen_at": row["last_seen_at"],
        "reject_reason": reject_reason,
    }


@MCP.tool(annotations=READ_ONLY)
def ghostfrog_source_coverage(limit: RowLimit = 25) -> dict[str, Any]:
    """List configured eBay sources, enabled status, and last scrape timestamps."""
    with _read_cursor() as cur:
        cur.execute(
            """
            SELECT name AS source, enabled, last_scraped_at, scrape_interval_seconds
            FROM public.sources
            WHERE name ~ '^ebay-[a-z0-9-]{1,50}$'
            ORDER BY name
            LIMIT %s
            """,
            (limit,),
        )
        return _result("sources", rows=cur.fetchall())


@MCP.tool(annotations=READ_ONLY)
def ghostfrog_macbook_listings(
    kind: Literal["live_auctions", "confirmed_sales"], limit: RowLimit = 10,
) -> dict[str, Any]:
    """List recent MacBook auctions seen live or confirmed sales, with evidence and reject reasons."""
    if kind == "live_auctions":
        selection, order = LIVE_AUCTION, LIVE_ORDER
    else:
        selection, order = CONFIRMED_SALE + "AND a.end_time >= now() - interval '30 days'", SOLD_ORDER
    with _read_cursor() as cur:
        _select_listings(cur, " AND a.title ILIKE %s " + selection, order, ("%macbook%",), limit)
        return _result("auction_listings / condition and review records", rows=[_listing(row) for row in cur])


@MCP.tool(annotations=READ_ONLY)
def ghostfrog_macbook_bid_history(item_id: ItemID, limit: RowLimit = 25) -> dict[str, Any]:
    """Get the newest recorded price and total bid count observations for a MacBook eBay item."""
    with _read_cursor() as cur:
        cur.execute(
            """
            SELECT h.recorded_at, h.price AS observed_price_gbp, h.bids_count
            FROM public.auction_price_history h
            JOIN public.auction_listings a USING (external_id)
            WHERE a.source = 'ebay-apple' AND a.title ILIKE %s
              AND a.sale_type = 'auction'
              AND h.external_id IN (%s, %s) AND h.recorded_at IS NOT NULL
            ORDER BY h.recorded_at DESC, h.id DESC
            LIMIT %s
            """,
            ("%macbook%", item_id, f"v1|{item_id}|0", limit),
        )
        return _result("auction_price_history / auction_listings", item_id=item_id, rows=cur.fetchall())


@MCP.tool(annotations=READ_ONLY)
def ghostfrog_macbook_valuation(item_id: ItemID, limit: RowLimit = 10) -> dict[str, Any]:
    """Find exact-model, same-grade confirmed MacBook sales and estimate a live auction's safe buy price."""
    provenance = "auction_listings / condition and review records"
    with _read_cursor() as cur:
        ids = (item_id, f"v1|{item_id}|0")
        _select_listings(
            cur, LIVE_AUCTION + " AND a.external_id IN (%s, %s) AND a.title ILIKE %s",
            LIVE_ORDER, (*ids, "%macbook%"), 1,
        )
        current = cur.fetchone()
        if current is None:
            return _result(provenance, item_id=item_id, reject_reason="no_recent_live_auction")
        listing = _listing(current)
        if listing["reject_reason"] or not listing["current_price_gbp"] or listing["current_price_gbp"] <= 0:
            return _result(provenance, listing=listing, reject_reason=listing["reject_reason"] or "no_current_price")
        _select_listings(
            cur, CONFIRMED_SALE + """
              AND a.model_key = %s AND a.end_time >= now() - make_interval(days => %s)
              AND a.external_id NOT IN (%s, %s)
            """, SOLD_ORDER, (listing["model_key"], COMP_DAYS, *ids), COMP_CANDIDATE_LIMIT + 1,
        )
        candidates = cur.fetchall()

    comps = {}
    for row in candidates[:COMP_CANDIDATE_LIMIT]:
        comp = _listing(row)
        if not comp["reject_reason"] and comp["condition_grade"] == listing["condition_grade"]:
            comps.setdefault(comp["item_id"], comp)

    minimum_comps = effective_min_comps_for_source(SOURCE)
    result = _result(
        provenance, listing=listing, comp_window_days=COMP_DAYS, comp_count=len(comps),
        minimum_comp_count=minimum_comps, sold_comps=list(comps.values())[:limit],
        sold_median_gbp=None, max_safe_buy_gbp=None, expected_profit_gbp=None, expected_roi=None,
    )
    if len(candidates) > COMP_CANDIDATE_LIMIT:
        return dict(result, reject_reason="comp_candidate_limit_reached")
    if len(comps) < minimum_comps:
        return dict(result, reject_reason="insufficient_same_grade_comps")

    sold_median = median(comp["final_price_gbp"] for comp in comps.values())
    price = listing["current_price_gbp"]
    ceiling = max_safe_buy(sold_median=sold_median, min_profit=MIN_PROFIT_GBP)
    profit = expected_profit(current_price=price, sold_median=sold_median)
    return dict(
        result, sold_median_gbp=sold_median, max_safe_buy_gbp=max(0, ceiling),
        expected_profit_gbp=profit, expected_roi=round(profit / price, 4),
        reject_reason="above_max_safe_buy" if price > ceiling or profit < MIN_PROFIT_GBP else None,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--coverage-json", action="store_true", help="Print a redacted source coverage report.")
    args = parser.parse_args()
    if not args.coverage_json:
        MCP.run(transport="stdio")
        return
    try:
        print(json.dumps(ghostfrog_source_coverage(), default=lambda value: value.isoformat(), indent=2))
    except ToolError as exc:
        parser.exit(1, f"{exc}\n")


if __name__ == "__main__":
    main()
