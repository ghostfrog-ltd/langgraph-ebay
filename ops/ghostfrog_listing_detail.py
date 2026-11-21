"""Bounded single-item access over the dedicated PostgreSQL projections."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import re

from mcp.server.fastmcp.exceptions import ToolError
from psycopg2.extras import RealDictCursor

from ops.ghostfrog_diagnostics import _connection
from pipelines.listing.market_discovery.pricing import (
    FEE_RATE, INBOUND_SHIP_GBP, MIN_PROFIT_GBP, OUTBOUND_SHIP_GBP, PACKAGING_GBP,
    SELLING_RESERVE_GBP, effective_min_comps_for_source, expected_profit, max_safe_buy,
)
from utils.condition import has_investible_product_identity

EVIDENCE_SECTIONS = ("product", "description", "photos", "seller_terms", "checks")
OVERVIEW_LIMITS = {
    "product": (10, 3000), "description": (4, 4200), "photos": (6, 3000),
    "seller_terms": (12, 3800), "checks": (5, 3800), "history": (6, 2200),
    "reviews": (4, 1200), "comparables": (5, 4000),
}
MAX_PAGE_BYTES = 37_000
STALE_AFTER = timedelta(hours=24)
ITEM_ID_PATTERN = re.compile(r"[0-9]{12}")
COMPARE_KEY_PATTERN = re.compile(r"[a-z0-9][a-z0-9.\-]+")
MISSING_BY_SECTION = {
    "description": {
        "description": {"browse description", "description"},
        "short_description": {"browse shortdescription", "shortdescription", "short description"},
        "subtitle": {"browse subtitle", "subtitle"},
        "condition_description": {"browse conditiondescription", "conditiondescription", "condition description"},
        "notes": {"notes"},
    },
    "seller_terms": {
        "seller_username": {"browse seller username"},
        "feedback_score": {"browse seller feedbackscore"},
        "feedback_percentage": {"browse seller feedbackpercentage"},
        "shipping": {"browse shippingoptions"},
        "returns": {"browse returnterms returnsaccepted", "browse returnterms refundmethod",
                    "browse returnterms returnperiod", "browse returnterms returnmethod",
                    "browse returnterms returnshippingcostpayer"},
        "location": {"browse itemlocation country", "browse itemlocation city",
                     "browse itemlocation stateorprovince"},
        "public_condition": {"browse condition"},
        "buying_options": {"browse buyingoptions"},
        "availability": {"browse availability", "browse estimatedavailabilities"},
    },
}
CHECK_FIELDS = ("text", "visual", "listing_assessment")


def _wire(value):
    encoded = json.dumps(value, default=str, allow_nan=False)
    if len(encoded.encode()) > 48_000:
        raise ToolError("The response exceeds the byte limit. Read one named section with a smaller limit.")
    return json.loads(encoded)


def _json_bytes(value) -> int:
    return len(json.dumps(value, default=str, allow_nan=False).encode())


def _page(rows, *, limit: int, budget: int, after: int, evidence: bool = False):
    items = []
    used = 0
    for row in rows[:limit]:
        size = _json_bytes(row)
        if used + size > budget:
            break
        used += size
        items.append(row)
    more = len(items) < len(rows)
    if more and not items:
        raise ToolError("A listing evidence row exceeds the bounded response size.")
    next_after = None
    if more:
        next_after = items[-1]["evidence_seq"] if evidence else after + len(items)
    return {
        "status": "available" if items else ("end_of_section" if after else "not_collected"),
        "rows": items, "truncated": more, "next_after": next_after,
    }


def _get_listing(cursor, item_id: str, listing_id: int | None):
    prefix = f"v1|{item_id}|"
    # Match the legacy item ID and all numeric variation IDs with indexable
    # range bounds. The parsed public item ID is verified on the projection.
    cursor.execute(
        """
        SELECT * FROM public.listing_inspection_v
        WHERE item_id = %s AND (
            external_id = %s OR external_id BETWEEN %s AND %s
        ) AND (%s::integer IS NULL OR listing_id = %s)
        ORDER BY last_seen_at DESC NULLS LAST, listing_id DESC LIMIT 11
        """,
        (item_id, item_id, prefix + "0", prefix + "999999999999", listing_id, listing_id),
    )
    return cursor.fetchall()


def _check_records(cursor, listing_id: int):
    cursor.execute(
        """
        SELECT kind, checked_at, grade, confidence, review_required, model_name, verdict, recommended_max_bid
        FROM public.listing_inspection_checks_v WHERE listing_id = %s ORDER BY kind
        """,
        (listing_id,),
    )
    records = {record["kind"]: dict(record) for record in cursor.fetchall()}
    return {kind: {"status": "checked", **records[kind]} if kind in records else {"status": "not_checked"}
            for kind in CHECK_FIELDS}


def _latest_review(cursor, listing, item_id):
    cursor.execute(
        """
        SELECT public.ghostfrog_product_text(action) AS action, created_at
        FROM public.listing_review_actions WHERE source = %s AND external_id IN (%s, %s)
        ORDER BY created_at DESC, id DESC LIMIT 1
        """,
        (listing["source"], listing["external_id"], item_id),
    )
    row = cursor.fetchone()
    return {"status": "recorded", **row} if row else {"status": "not_checked"}


def _inventory(cursor, listing_id: int):
    cursor.execute(
        """
        SELECT section, field, count(*) AS chunks, count(*) FILTER (WHERE withheld) AS withheld_chunks
        FROM public.listing_inspection_evidence_v WHERE listing_id = %s
        GROUP BY section, field ORDER BY section, field
        """,
        (listing_id,),
    )
    by_section = {name: [] for name in EVIDENCE_SECTIONS}
    for row in cursor.fetchall():
        by_section[row["section"]].append({
            "field": row["field"], "chunks": row["chunks"], "withheld_chunks": row["withheld_chunks"],
        })
    return by_section


def _evidence(cursor, section: str, listing: dict, after: int, limit: int, budget: int, inventory: dict):
    cursor.execute(
        """
        SELECT evidence_seq, field, value_path, part_index, part_count, value, withheld
        FROM public.listing_inspection_evidence_v
        WHERE listing_id = %s AND section = %s AND evidence_seq > %s
        ORDER BY evidence_seq LIMIT %s
        """,
        (listing["listing_id"], section, after, limit + 1),
    )
    result = _page(cursor.fetchall(), limit=limit, budget=budget, after=after, evidence=True)
    fields = inventory[section]
    result["available_fields"] = fields[:20]
    result["field_inventory_truncated"] = len(fields) > 20
    expected = MISSING_BY_SECTION.get(section)
    if expected:
        available = {
            row["field"].lower() for row in fields if row["chunks"] > row["withheld_chunks"]
        }
        present = {row["field"].lower() for row in fields}
        result["field_status"] = {
            name: "collected" if available & aliases else "withheld" if present & aliases else "not_collected"
            for name, aliases in expected.items()
        }
    return result


def _history_or_reviews(cursor, section: str, listing: dict, after: int, limit: int, budget: int):
    ids = (listing["external_id"], listing["item_id"])
    if section == "history":
        cursor.execute(
            """
            SELECT id, price, bids_count, timestamp, recorded_at FROM public.auction_price_history
            WHERE external_id IN (%s, %s)
            ORDER BY recorded_at DESC NULLS LAST, id DESC OFFSET %s LIMIT %s
            """,
            (*ids, after, limit + 1),
        )
    else:
        cursor.execute(
            """
            SELECT id, public.ghostfrog_product_text(action) AS action, created_at
            FROM public.listing_review_actions
            WHERE external_id IN (%s, %s) AND source = %s
            ORDER BY created_at DESC, id DESC OFFSET %s LIMIT %s
            """,
            (*ids, listing["source"], after, limit + 1),
        )
    return _page(cursor.fetchall(), limit=limit, budget=budget, after=after)


def _pricing(cursor, listing, review, now):
    key = listing["model_key"]
    grade = listing["effective_condition_grade"]
    snapshot = None
    if key and COMPARE_KEY_PATTERN.fullmatch(key) and not key.startswith("unknown"):
        cursor.execute(
            """
            SELECT model_key, condition_grade, samples, median_final_price, mean_final_price, computed_at
            FROM public.latest_comps WHERE model_key = %s AND condition_grade = %s
            """,
            (key, grade),
        )
        snapshot = cursor.fetchone()
    floor = effective_min_comps_for_source(listing["source"])
    assumptions = {
        "currency": "GBP", "platform_fee_rate": FEE_RATE, "inbound_postage_gbp": INBOUND_SHIP_GBP,
        "outbound_postage_gbp": OUTBOUND_SHIP_GBP, "packaging_gbp": PACKAGING_GBP,
        "selling_reserve_gbp": SELLING_RESERVE_GBP, "minimum_profit_gbp": MIN_PROFIT_GBP,
        "minimum_same_grade_comps": floor,
    }
    reason = None
    if listing["identity_risk"] or not has_investible_product_identity(
        listing["title"] or "", source=listing["source"], model_key=key,
    ):
        reason = "unresolved_identity_or_product_risk"
    elif grade == "D" or listing["text_review_required"] or listing["visual_review_required"] or review.get("action") == "down":
        reason = "condition_or_operator_review"
    elif listing["status"] != "live" or (listing["end_time"] and listing["end_time"] <= now):
        reason = "listing_not_currently_live_in_database"
    elif listing["observation_state"] != "recent":
        reason = "observation_stale_or_unavailable"
    elif not snapshot or snapshot["samples"] < floor or snapshot["median_final_price"] <= 0:
        reason = "insufficient_same_grade_comps"
    elif now - snapshot["computed_at"] > STALE_AFTER:
        reason = "comp_snapshot_stale"

    ceiling = profit = None
    if reason is None:
        median = float(snapshot["median_final_price"])
        ceiling = max(0, max_safe_buy(sold_median=median, min_profit=MIN_PROFIT_GBP))
        if listing["price_current"] is not None:
            profit = expected_profit(current_price=float(listing["price_current"]), sold_median=median)
    return {
        "snapshot": snapshot, "assumptions": assumptions, "reason": reason,
        "snapshot_stale": None if snapshot is None else now - snapshot["computed_at"] > STALE_AFTER,
        "indicative_max_purchase_gbp": ceiling, "expected_profit_at_observed_price_gbp": profit,
        "condition_checks_complete": bool(listing["text_condition_checked"] and listing["visual_condition_checked"]),
        "provenance": "latest_comps; stored aggregate with no per-row snapshot membership",
        "shipping_basis": "fixed pricing assumptions; stored shipping offers are in seller_terms",
    }


def _comparables(cursor, listing, after, limit, budget, pricing):
    cursor.execute(
        "SELECT * FROM public.ghostfrog_listing_comparables(%s, %s, %s)",
        (listing["listing_id"], after, limit + 1),
    )
    result = _page(cursor.fetchall(), limit=limit, budget=budget, after=after)
    result["pricing"] = pricing
    if not result["rows"] and not after:
        result["status"] = "no_matching_confirmed_sales"
    result["provenance"] = (
        "Currently matching same-source/model/effective-grade confirmed sales within 365 days. "
        "The stored comp snapshot does not retain historical row membership."
    )
    result["drill_down"] = "Call ghostfrog_listing_detail with a returned item_id and listing_id."
    return result


def listing_detail(item_id, section, after, limit, listing_id=None):
    if not isinstance(item_id, str) or not ITEM_ID_PATTERN.fullmatch(item_id):
        raise ToolError("Supply a 12-digit public eBay item ID.")
    if section != "overview" and section not in OVERVIEW_LIMITS:
        raise ToolError("Unknown listing evidence section.")
    if type(after) is not int or not 0 <= after <= 10000 or type(limit) is not int or not 1 <= limit <= 50:
        raise ToolError("Use after from 0 to 10000 and limit from 1 to 50.")
    if listing_id is not None and (type(listing_id) is not int or not 1 <= listing_id <= 2147483647):
        raise ToolError("Invalid listing ID.")
    if section == "overview" and after:
        raise ToolError("Select a section to continue a page.")
    now = datetime.now(timezone.utc)
    response = {
        "item_id": item_id, "queried_at_utc": now.isoformat(timespec="seconds"),
        "provenance": "GhostFrog live PostgreSQL / stored eBay listing evidence",
    }
    with _connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cursor:
        # All named sections in a response see the same database snapshot.
        cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        matches = _get_listing(cursor, item_id, listing_id)
        if not matches:
            return {**response, "status": "not_collected"}
        if len(matches) > 1:
            return _wire({
                **response, "status": "select_listing_id", "truncated": len(matches) > 10,
                "choices": [{key: row[key] for key in ("listing_id", "external_id", "source", "status")}
                            for row in matches[:10]],
            })
        listing = dict(matches[0])
        record_id = listing["listing_id"]
        seen = listing["last_seen_at"] or listing["fetched_at"] or listing["first_seen"]
        listing["observation_state"] = (
            "not_collected" if seen is None else "stale" if now - seen > STALE_AFTER else "recent"
        )
        listing["availability_provenance"] = "Stored observation; current eBay availability is unverified."
        listing["not_collected_or_unconfirmed"] = [
            key for key, value in listing.items() if value is None
        ]
        if _json_bytes(listing.get("title")) > 6000:
            listing["title"] = None
            listing["title_continuation"] = "Read the title field in the description section."
        response |= {"status": "available", "listing_id": record_id}
        inventory = _inventory(cursor, record_id) if section == "overview" or section in EVIDENCE_SECTIONS else {}
        review = _latest_review(cursor, listing, item_id) if section in ("overview", "checks", "comparables") else {}
        pricing = _pricing(cursor, listing, review, now) if section in ("overview", "comparables") else None
        checks = _check_records(cursor, record_id) if section in ("overview", "checks") else None

        def read_section(name, count, budget):
            if name in EVIDENCE_SECTIONS:
                result = _evidence(cursor, name, listing, after, count, budget, inventory)
                if name == "checks":
                    if not after and not result["rows"] and (
                        any(check["status"] == "checked" for check in checks.values())
                        or review["status"] == "recorded"
                    ):
                        result["status"] = "available"
                    result |= {"latest_assessments": checks, "latest_operator_review": review}
                return result
            if name == "comparables":
                return _comparables(cursor, listing, after, count, budget, pricing)
            return _history_or_reviews(cursor, name, listing, after, count, budget)

        if section == "overview":
            response |= {
                "listing": listing,
                "sections": {name: read_section(name, count, budget)
                             for name, (count, budget) in OVERVIEW_LIMITS.items()},
                "collection_limits": {
                    "browse_descriptions_may_be_capped_at_chars": 4000,
                    "stored_gallery_may_be_capped_at_photos": 12,
                    "stored_shipping_may_be_capped_at_options": 3,
                    "scope": "Only evidence stored by the collector is shown.",
                },
            }
        else:
            response |= {"section": section, **read_section(section, limit, MAX_PAGE_BYTES - 5000)}

    return _wire(response)
