from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, TypedDict, Literal

import requests
import xml.etree.ElementTree as ET

from utils.db_schema import get_connection
from utils.usage_tracker import increment_api_usage
from utils.logger import get_logger
from utils.condition import _derive_condition_grade

from pipelines.listing.attributes.mk import normalise_model

# LangGraph
from langgraph.graph import StateGraph, END

logger = get_logger(__name__)

EBAY_TRADING_ENDPOINT = "https://api.ebay.com/ws/api.dll"
EBAY_SITE_ID = "3"          # UK
EBAY_COMPAT_LEVEL = "967"   # compat level for GetItem

UNKNOWN_KEY = "unknown"
TRADING_ENRICHMENT_STATUS_KEY = "_trading_enrichment_status"
TRADING_ENRICHED_AT_KEY = "_trading_enriched_at"
MAX_REASONABLE_STORAGE_GB = 65_536
_STORAGE_WITH_UNIT_RE = re.compile(
    r"(?<![A-Za-z0-9])(\d+(?:[.,]\d+)?)\s*"
    r"(tb|terabytes?|gb|gigabytes?|mb|megabytes?)\b",
    re.IGNORECASE,
)

# -------------------------------------------------
# Helpers (KEEP IN THIS FILE)
# -------------------------------------------------
def _extract_numeric_item_id(raw_id: str | None) -> Optional[str]:
    if not raw_id:
        return None

    if raw_id.startswith(("v1|", "v2|")):
        parts = raw_id.split("|")
        if len(parts) >= 2 and parts[1].isdigit():
            return parts[1]

    if "|" in raw_id:
        for p in raw_id.split("|"):
            if p.isdigit():
                return p

    if raw_id.isdigit():
        return raw_id

    return None


def _call_trading_get_item(item_id: str) -> Optional[str]:
    token = os.getenv("EBAY_TRADING_TOKEN", "").strip()
    if not token:
        logger.error("[maint.attributes] EBAY_TRADING_TOKEN not set")
        return None

    headers = {
        "Content-Type": "text/xml",
        "X-EBAY-API-CALL-NAME": "GetItem",
        "X-EBAY-API-SITEID": EBAY_SITE_ID,
        "X-EBAY-API-COMPATIBILITY-LEVEL": EBAY_COMPAT_LEVEL,
        "X-EBAY-API-IAF-TOKEN": token,
    }

    body = f"""<?xml version="1.0" encoding="utf-8"?>
<GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <ItemID>{item_id}</ItemID>
  <DetailLevel>ReturnAll</DetailLevel>
  <IncludeItemSpecifics>true</IncludeItemSpecifics>
</GetItemRequest>
"""

    logger.info(f"[maint.attributes] Calling Trading GetItem for item_id={item_id}")
    try:
        resp = requests.post(
            EBAY_TRADING_ENDPOINT,
            data=body.encode("utf-8"),
            headers=headers,
            timeout=(4, 8),
        )
    except Exception as e:
        logger.error(f"[maint.attributes] Trading request error for {item_id}: {e}")
        return None

    logger.info(f"[maint.attributes] HTTP {resp.status_code} for item_id={item_id}")

    if resp.status_code != 200:
        logger.error(resp.text[:1000])
        return None

    try:
        increment_api_usage("ebay_attributes_v1")
    except Exception as e:
        logger.warning(f"[maint.attributes] increment_api_usage failed: {e}")

    return resp.text


def _parse_storage_gb(raw_val: Optional[str]) -> Optional[int]:
    """
    Parse eBay storage attributes into integer GB without trusting arbitrary digits.

    Trading item specifics sometimes put catalogue codes, part numbers, or mixed
    RAM/storage phrases in fields named like "Storage Capacity".  Persisting the
    old all-digits parse could overflow the PostgreSQL INTEGER column and poison
    the heartbeat, so this only accepts unit-bearing values or short numeric
    values that fit a conservative consumer-device storage range.
    """
    if not raw_val:
        return None

    txt = str(raw_val).strip().lower()
    if not txt:
        return None

    parsed_values: List[int] = []
    for match in _STORAGE_WITH_UNIT_RE.finditer(txt):
        raw_number, unit = match.groups()
        try:
            value = float(raw_number.replace(",", "."))
        except ValueError:
            continue

        unit = unit.lower()
        if unit.startswith("tb") or unit.startswith("terabyte"):
            gb = int(round(value * 1024))
        elif unit.startswith("mb") or unit.startswith("megabyte"):
            gb = max(1, int(round(value / 1024)))
        else:
            gb = int(round(value))

        if 0 < gb <= MAX_REASONABLE_STORAGE_GB:
            parsed_values.append(gb)

    if parsed_values:
        return max(parsed_values)

    if re.fullmatch(r"\d{1,6}", txt):
        gb = int(txt)
        if 0 < gb <= MAX_REASONABLE_STORAGE_GB:
            return gb

    return None


def _extract_from_trading(xml_text: str) -> tuple[Dict[str, Optional[Any]], Dict[str, Any]]:
    attrs: Dict[str, Optional[Any]] = {
        "brand": None,
        "model_name": None,
        "storage_gb": None,
        "colour": None,
        "epid": None,
    }
    raw_map: Dict[str, Any] = {}

    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError as e:
        logger.error(f"[maint.attributes] XML parse error: {e}")
        return attrs, raw_map

    ns = {"ns": "urn:ebay:apis:eBLBaseComponents"}

    item_node = root.find(".//ns:Item", ns)
    if item_node is None:
        item_node = root.find(".//Item")
    if item_node is None:
        logger.error("[maint.attributes] No <Item> node found in Trading response")
        return attrs, raw_map

    # --- ProductListingDetails (for ePID + Brand) --------------------
    pld = item_node.find("./ns:ProductListingDetails", ns)
    if pld is not None:
        # ePID
        for pid in pld.findall("./ns:ProductID", ns):
            text = (pid.text or "").strip()
            if text:
                attrs["epid"] = text
                raw_map["epid"] = text
                break

        # Brand from BrandMPN
        bmpn_brand = pld.find("./ns:BrandMPN/ns:Brand", ns)
        if bmpn_brand is not None and bmpn_brand.text:
            brand_text = bmpn_brand.text.strip()
            attrs["brand"] = attrs["brand"] or brand_text
            raw_map.setdefault("Brand", brand_text)

    # --- ItemSpecifics -----------------------------------------------
    specs_node = item_node.find("./ns:ItemSpecifics", ns)
    if specs_node is not None:
        for nvl in specs_node.findall("./ns:NameValueList", ns):
            name_el = nvl.find("./ns:Name", ns)
            if name_el is None or not name_el.text:
                continue
            name_raw = name_el.text.strip()
            if not name_raw:
                continue

            name_lower = name_raw.lower()

            values = [
                (v.text or "").strip()
                for v in nvl.findall("./ns:Value", ns)
                if v.text
            ]
            if not values:
                continue

            val_single = values[0]
            joined_vals = ", ".join(values)

            raw_map[name_raw] = values if len(values) > 1 else val_single

            if name_lower == "brand" and not attrs["brand"]:
                attrs["brand"] = val_single

            elif name_lower == "model" and not attrs["model_name"]:
                attrs["model_name"] = val_single

            elif name_lower in ("colour", "color") and not attrs["colour"]:
                attrs["colour"] = val_single

            elif name_lower in ("storage capacity", "storage", "hard drive capacity", "memory"):
                if attrs["storage_gb"] is None:
                    attrs["storage_gb"] = _parse_storage_gb(val_single or joined_vals)

    return attrs, raw_map


def _trading_marker(status: str) -> Dict[str, str]:
    return {
        TRADING_ENRICHMENT_STATUS_KEY: status,
        TRADING_ENRICHED_AT_KEY: datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z"),
    }


def _with_trading_marker(raw_map: Optional[Dict[str, Any]], status: str) -> Dict[str, Any]:
    marked: Dict[str, Any] = {}
    for key, value in (raw_map or {}).items():
        if value is None or value == "":
            continue
        marked[key] = value
    marked.update(_trading_marker(status))
    return marked


def _merge_raw_attrs(existing: Any, incoming: Any) -> Any:
    if incoming is None:
        return existing
    if existing is None or existing is False:
        return incoming
    if incoming is False:
        return existing
    if isinstance(existing, dict) and isinstance(incoming, dict):
        merged = dict(existing)
        for key, value in incoming.items():
            if value is None or value == "":
                continue
            merged[key] = value
        return merged
    return incoming


def _riskier_condition_grade(existing: Optional[str], incoming: Optional[str]) -> Optional[str]:
    if not incoming:
        return existing
    if not existing:
        return incoming
    risk = {"A": 0, "B": 1, "C": 2, "D": 3}
    existing_norm = str(existing).strip().upper()
    incoming_norm = str(incoming).strip().upper()
    if risk.get(existing_norm, -1) >= risk.get(incoming_norm, -1):
        return existing
    return incoming


def _load_candidates(limit: int) -> List[Tuple[int, str, str, str]]:
    sql = """
        SELECT id, external_id, source, title
        FROM auction_listings
        WHERE raw_attrs IS NULL
           OR (
                jsonb_typeof(raw_attrs) = 'object'
                AND NOT (raw_attrs ? %s)
                AND brand IS NULL
                AND model_name IS NULL
                AND storage_gb IS NULL
                AND colour IS NULL
                AND epid IS NULL
                AND NOT EXISTS (
                    SELECT 1
                    FROM jsonb_object_keys(raw_attrs) AS attr_key(key)
                    WHERE attr_key.key NOT LIKE 'Browse %%'
                )
           )
        ORDER BY id DESC
        LIMIT %s
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (TRADING_ENRICHMENT_STATUS_KEY, limit))
            rows = cur.fetchall()
        conn.commit()

    return rows


def _requested_id_variants(raw_id: str) -> set[str]:
    value = str(raw_id or "").strip()
    if not value:
        return set()
    variants = {value}
    numeric = _extract_numeric_item_id(value)
    if numeric:
        variants.update({numeric, f"v1|{numeric}|0", f"v2|{numeric}|0"})
    return variants


def _load_matching_external_ids_for_ids(item_ids: List[str]) -> List[str]:
    variants = sorted({variant for item_id in item_ids for variant in _requested_id_variants(item_id)})
    if not variants:
        return []

    sql = """
        SELECT external_id
        FROM auction_listings
        WHERE external_id = ANY(%s)
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (variants,))
            rows = cur.fetchall()
        conn.commit()

    return [str(row[0]) for row in rows]


def _load_candidates_for_ids(item_ids: List[str], *, force: bool = False) -> List[Tuple[int, str, str, str]]:
    variants = sorted({variant for item_id in item_ids for variant in _requested_id_variants(item_id)})
    if not variants:
        return []

    eligibility_sql = """
        AND (
              %s
              OR raw_attrs IS NULL
              OR (
                    jsonb_typeof(raw_attrs) = 'object'
                    AND NOT (raw_attrs ? %s)
                    AND brand IS NULL
                    AND model_name IS NULL
                    AND storage_gb IS NULL
                    AND colour IS NULL
                    AND epid IS NULL
                    AND NOT EXISTS (
                        SELECT 1
                        FROM jsonb_object_keys(raw_attrs) AS attr_key(key)
                        WHERE attr_key.key NOT LIKE 'Browse %%'
                    )
              )
        )
    """

    sql = f"""
        SELECT id, external_id, source, title
        FROM auction_listings
        WHERE external_id = ANY(%s)
        {eligibility_sql}
        ORDER BY array_position(%s::text[], external_id), id DESC
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (variants, force, TRADING_ENRICHMENT_STATUS_KEY, variants))
            rows = cur.fetchall()
        conn.commit()

    return rows


def _apply_attributes(
    auction_id: int,
    raw_attrs: Optional[Any],
    brand: Optional[str],
    model_name: Optional[str],
    storage_gb: Optional[int],
    colour: Optional[str],
    epid: Optional[str],
    model_key: Optional[str],
    condition_grade: Optional[str] = None,
) -> None:
    fields: List[str] = []
    values: List[Any] = []

    with get_connection() as conn:
        if raw_attrs is not None or condition_grade is not None:
            existing_raw_attrs: Any = None
            existing_condition_grade: Optional[str] = None
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT raw_attrs, condition_grade
                    FROM auction_listings
                    WHERE id = %s
                    """,
                    (auction_id,),
                )
                existing = cur.fetchone()
            if existing:
                existing_raw_attrs = existing[0]
                existing_condition_grade = existing[1]
            raw_attrs = _merge_raw_attrs(existing_raw_attrs, raw_attrs)
            condition_grade = _riskier_condition_grade(existing_condition_grade, condition_grade)

        # raw_attrs can be a dict OR the legacy sentinel False -> jsonb 'false'.
        if raw_attrs is not None:
            fields.append("raw_attrs = %s")
            values.append(json.dumps(raw_attrs))

        if brand is not None:
            fields.append("brand = %s")
            values.append(brand)

        if model_name is not None:
            fields.append("model_name = %s")
            values.append(model_name)

        if storage_gb is not None:
            fields.append("storage_gb = %s")
            values.append(storage_gb)

        if colour is not None:
            fields.append("colour = %s")
            values.append(colour)

        if epid is not None:
            fields.append("epid = %s")
            values.append(epid)

        if model_key is not None:
            fields.append("model_key = %s")
            values.append(model_key)

        if condition_grade is not None:
            fields.append("condition_grade = %s")
            values.append(condition_grade)

        if not fields:
            return

        sql = f"""
            UPDATE auction_listings
            SET {", ".join(fields)}
            WHERE id = %s
        """
        values.append(auction_id)

        with conn.cursor() as cur:
            cur.execute(sql, values)
        conn.commit()


# -------------------------------------------------
# LangGraph state + nodes
# -------------------------------------------------
class AttributesState(TypedDict, total=False):
    # inputs
    limit: int
    enable_api: bool
    target_ids: List[str]
    target_force: bool

    # work
    rows: List[Tuple[int, str, str, str]]
    idx: int

    auction_id: int
    external_id: str
    source: str
    title: str
    item_id: Optional[str]

    xml_text: Optional[str]
    attrs: Dict[str, Optional[Any]]
    raw_map: Dict[str, Any]

    # stats
    processed: int
    marked_false: int
    retryable_failures: int
    outcomes: List[Dict[str, Any]]


def _node_start(state: AttributesState) -> AttributesState:
    logger.info(
        f"[maint.attributes] run(limit={state.get('limit')}, enable_api={state.get('enable_api')}) starting"
    )
    if not state.get("enable_api", True):
        logger.info("[maint.attributes] enable_api=False, nothing to do")
        # returning state; router will end
        return state

    target_ids = [str(item_id) for item_id in (state.get("target_ids") or []) if str(item_id).strip()]
    if target_ids:
        state["rows"] = _load_candidates_for_ids(
            target_ids,
            force=bool(state.get("target_force", False)),
        )
    else:
        state["rows"] = _load_candidates(int(state.get("limit", 20)))
    state["idx"] = 0
    state["processed"] = 0
    state["marked_false"] = 0
    state["retryable_failures"] = 0
    state["outcomes"] = []

    logger.info(f"[maint.attributes] loaded {len(state['rows'])} candidate listings")
    return state


def _route_after_start(state: AttributesState) -> Literal["next", "end"]:
    if not state.get("enable_api", True):
        return "end"
    if not state.get("rows"):
        return "end"
    return "next"


def _node_next_candidate(state: AttributesState) -> AttributesState:
    i = int(state.get("idx", 0))
    rows = state.get("rows") or []
    if i >= len(rows):
        return state

    auction_id, external_id, source, title = rows[i]
    state["auction_id"] = int(auction_id)
    state["external_id"] = str(external_id) if external_id is not None else ""
    state["source"] = str(source) if source is not None else ""
    state["title"] = str(title) if title is not None else ""
    state["item_id"] = None
    state["xml_text"] = None
    state["attrs"] = {}
    state["raw_map"] = {}

    logger.info(
        f"[maint.attributes] processing auction_id={state['auction_id']} external_id={state['external_id']}"
    )
    return state


def _node_extract_item_id(state: AttributesState) -> AttributesState:
    external_id = state.get("external_id") or ""
    item_id = _extract_numeric_item_id(external_id)
    state["item_id"] = item_id
    logger.info(
        f"[maint.attributes] external_id={external_id} -> item_id={item_id}"
    )
    return state


def _route_item_id(state: AttributesState) -> Literal["call_api", "mark_false"]:
    return "call_api" if state.get("item_id") else "mark_false"


def _node_call_api(state: AttributesState) -> AttributesState:
    item_id = state.get("item_id")
    if not item_id:
        return state
    state["xml_text"] = _call_trading_get_item(item_id)
    if not state["xml_text"]:
        logger.warning(f"[maint.attributes] no XML returned for item_id={item_id}")
    return state


def _route_after_call_api(state: AttributesState) -> Literal["extract", "mark_false"]:
    return "extract" if state.get("xml_text") else "mark_false"


def _node_extract_attrs(state: AttributesState) -> AttributesState:
    xml_text = state.get("xml_text")
    if not xml_text:
        return state

    attrs, raw_map = _extract_from_trading(xml_text)
    state["attrs"] = attrs
    state["raw_map"] = raw_map

    logger.info(
        f"[maint.attributes] extracted attrs for id={state.get('auction_id')}: "
        f"brand={attrs.get('brand')!r}, model={attrs.get('model_name')!r}, "
        f"storage={attrs.get('storage_gb')!r}, colour={attrs.get('colour')!r}, "
        f"epid={attrs.get('epid')!r}"
    )
    logger.info(
        f"[maint.attributes] raw_attrs keys for id={state.get('auction_id')}: {list(raw_map.keys())}"
    )
    return state


def _route_after_extract(state: AttributesState) -> Literal["apply", "mark_false"]:
    attrs = state.get("attrs") or {}
    raw_map = state.get("raw_map") or {}

    # Keep identical logic: if no raw_map AND no attrs values => mark false
    if not raw_map and not any(attrs.values()):
        logger.warning(
            f"[maint.attributes] Trading returned no usable attributes for auction_id={state.get('auction_id')} "
            f"– marking Trading enrichment as no_attrs"
        )
        return "mark_false"

    return "apply"


def _node_apply(state: AttributesState) -> AttributesState:
    auction_id = int(state.get("auction_id", 0))
    attrs = state.get("attrs") or {}
    raw_map = state.get("raw_map") or {}
    source = state.get("source")
    title = state.get("title")
    # Apple identity needs the original item specifics for conflict detection.
    identity_attrs = {**attrs, **raw_map} if source == "ebay-apple" else attrs
    key = normalise_model(title=title, attrs=identity_attrs, source=source) or UNKNOWN_KEY
    condition_grade = _derive_condition_grade(raw_map or attrs, title or "")

    _apply_attributes(
        auction_id=auction_id,
        raw_attrs=_with_trading_marker(raw_map, "complete"),
        brand=attrs.get("brand"),
        model_name=attrs.get("model_name"),
        storage_gb=attrs.get("storage_gb"),
        colour=attrs.get("colour"),
        epid=attrs.get("epid"),
        model_key=key,
        condition_grade=condition_grade,
    )

    state["processed"] = int(state.get("processed", 0)) + 1
    outcomes = state.setdefault("outcomes", [])
    outcomes.append(
        {
            "external_id": state.get("external_id"),
            "item_id": state.get("item_id"),
            "status": "complete",
            "raw_attrs_added": sorted(raw_map.keys()),
        }
    )
    return state


def _node_mark_false(state: AttributesState) -> AttributesState:
    auction_id = int(state.get("auction_id", 0))
    external_id = state.get("external_id")
    status = "no_attrs"

    if not state.get("item_id"):
        logger.error(f"[maint.attributes] invalid numeric item_id from {external_id}")
        status = "invalid_item_id"
    elif not state.get("xml_text"):
        status = "no_response"

    if status == "no_response":
        logger.warning(
            "[maint.attributes] retryable no-response for auction_id=%s external_id=%s; leaving enrichment unmarked",
            auction_id,
            external_id,
        )
        state["processed"] = int(state.get("processed", 0)) + 1
        state["retryable_failures"] = int(state.get("retryable_failures", 0)) + 1
        outcomes = state.setdefault("outcomes", [])
        outcomes.append(
            {
                "external_id": external_id,
                "item_id": state.get("item_id"),
                "status": "retryable_no_response",
                "raw_attrs_added": [],
            }
        )
        return state

    _apply_attributes(
        auction_id=auction_id,
        raw_attrs=_with_trading_marker({}, status),
        brand=None,
        model_name=None,
        storage_gb=None,
        colour=None,
        epid=None,
        model_key=None,
        condition_grade=None,
    )

    state["processed"] = int(state.get("processed", 0)) + 1
    state["marked_false"] = int(state.get("marked_false", 0)) + 1
    outcomes = state.setdefault("outcomes", [])
    outcomes.append(
        {
            "external_id": external_id,
            "item_id": state.get("item_id"),
            "status": status,
            "raw_attrs_added": [],
        }
    )
    return state


def _node_advance(state: AttributesState) -> AttributesState:
    state["idx"] = int(state.get("idx", 0)) + 1
    return state


def _route_loop(state: AttributesState) -> Literal["next", "end"]:
    i = int(state.get("idx", 0))
    rows = state.get("rows") or []
    return "next" if i < len(rows) else "end"


def build_graph():
    g = StateGraph(AttributesState)

    g.add_node("start", _node_start)
    g.add_node("next_candidate", _node_next_candidate)
    g.add_node("extract_item_id", _node_extract_item_id)
    g.add_node("call_api", _node_call_api)
    g.add_node("extract_attrs", _node_extract_attrs)
    g.add_node("apply", _node_apply)
    g.add_node("mark_false", _node_mark_false)
    g.add_node("advance", _node_advance)

    g.set_entry_point("start")

    g.add_conditional_edges("start", _route_after_start, {"next": "next_candidate", "end": END})

    g.add_edge("next_candidate", "extract_item_id")
    g.add_conditional_edges("extract_item_id", _route_item_id, {"call_api": "call_api", "mark_false": "mark_false"})
    g.add_conditional_edges("call_api", _route_after_call_api, {"extract": "extract_attrs", "mark_false": "mark_false"})
    g.add_conditional_edges("extract_attrs", _route_after_extract, {"apply": "apply", "mark_false": "mark_false"})

    g.add_edge("apply", "advance")
    g.add_edge("mark_false", "advance")

    g.add_conditional_edges("advance", _route_loop, {"next": "next_candidate", "end": END})

    return g.compile()


def save_graph_diagram(path: str = "attributes_graph.mmd") -> None:
    """
    Write a Mermaid diagram of the graph to a file.
    This matches your --diagram CLI flow.
    """
    app = build_graph()
    mermaid = app.get_graph().draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)


# -------------------------------------------------
# Public entry
# -------------------------------------------------
def run(limit: int = 20, enable_api: bool = True) -> None:
    """
    Run one bounded attributes maintenance pass.

    The graph intentionally loops over up to `limit` listings and then exits.
    LangGraph's default recursion limit is too low for ordinary batches, so the
    public CLI uses the same raised limit as the main heartbeat wrapper.
    """
    app = build_graph()
    final = app.invoke(
        {"limit": int(limit), "enable_api": bool(enable_api)},
        config={"recursion_limit": 500},
    )

    logger.info(
        "[maint.attributes] run complete "
        f"(processed={final.get('processed', 0)}, marked_false={final.get('marked_false', 0)}, "
        f"retryable_failures={final.get('retryable_failures', 0)})"
    )


def run_for_ids(item_ids: List[str], enable_api: bool = True, *, force: bool = False) -> Dict[str, Any]:
    """
    Run Trading enrichment only for explicitly requested eBay item IDs.
    """
    requested = [str(item_id).strip() for item_id in item_ids if str(item_id).strip()]
    app = build_graph()
    final = app.invoke(
        {
            "limit": len(requested),
            "enable_api": bool(enable_api),
            "target_ids": requested,
            "target_force": bool(force),
        },
        config={"recursion_limit": max(50, len(requested) * 8 + 20)},
    )

    rows = final.get("rows") or []
    found_numeric = {
        _extract_numeric_item_id(str(row[1])) or str(row[1])
        for row in rows
    }
    matched_external_ids = _load_matching_external_ids_for_ids(requested)
    matched_numeric = {
        _extract_numeric_item_id(external_id) or external_id
        for external_id in matched_external_ids
    }
    skipped = []
    for item_id in requested:
        requested_key = _extract_numeric_item_id(item_id) or item_id
        if requested_key in found_numeric:
            continue
        skipped.append(
            {
                "item_id": item_id,
                "reason": "already_enriched_or_not_eligible"
                if requested_key in matched_numeric
                else "not_found",
            }
        )

    result = {
        "requested": requested,
        "found": [str(row[1]) for row in rows],
        "force": bool(force),
        "processed": int(final.get("processed", 0)),
        "complete": sum(1 for outcome in final.get("outcomes", []) if outcome.get("status") == "complete"),
        "terminal_failures": sum(
            1
            for outcome in final.get("outcomes", [])
            if outcome.get("status") in {"no_attrs", "invalid_item_id"}
        ),
        "retryable_failures": int(final.get("retryable_failures", 0)),
        "skipped": skipped,
        "outcomes": final.get("outcomes", []),
    }
    logger.info("[maint.attributes] targeted run result=%s", result)
    return result
