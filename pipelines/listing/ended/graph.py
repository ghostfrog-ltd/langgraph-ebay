from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Sequence, List, Tuple, TypedDict, Any

import requests
from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.db_schema import get_connection
from utils.timez import now_utc
from utils.usage_tracker import increment_api_usage

logger = get_logger(__name__)

EBAY_TRADING_ENDPOINT = "https://api.ebay.com/ws/api.dll"
EBAY_SITE_ID = "3"  # UK (keep as per your old implementation)
EBAY_COMPAT_LEVEL = "967"


# ----------------------------
# Legacy-compatible helpers
# ----------------------------

def _extract_numeric_item_id(external_id: str) -> Optional[str]:
    """
    Expected external_id like: v1|358057732129|0
    Returns numeric item id as string.
    """
    if not external_id:
        return None
    # fastest + safest: split and take middle chunk if numeric-ish
    parts = external_id.split("|")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[1]
    # fallback: scan for digits chunk
    digits = "".join(ch for ch in external_id if ch.isdigit())
    return digits or None


def _call_trading_get_item(item_id: str) -> requests.Response:
    """
    Trading API call: MUST use EBAY_TRADING_TOKEN (IAF token) from environment.
    """
    trading_token = os.getenv("EBAY_TRADING_TOKEN", "").strip()
    if not trading_token:
        raise RuntimeError("EBAY_TRADING_TOKEN is not set in env")

    headers = {
        "Content-Type": "text/xml",
        "X-EBAY-API-CALL-NAME": "GetItem",
        "X-EBAY-API-SITEID": EBAY_SITE_ID,
        "X-EBAY-API-COMPATIBILITY-LEVEL": EBAY_COMPAT_LEVEL,
        "X-EBAY-API-IAF-TOKEN": trading_token,
    }

    body = f"""<?xml version="1.0" encoding="utf-8"?>
<GetItemRequest xmlns="urn:ebay:apis:eBLBaseComponents">
  <ItemID>{item_id}</ItemID>
  <DetailLevel>ReturnAll</DetailLevel>
  <IncludeWatchCount>true</IncludeWatchCount>
</GetItemRequest>
"""

    resp = requests.post(
        EBAY_TRADING_ENDPOINT,
        data=body.encode("utf-8"),
        headers=headers,
        timeout=(4, 6),
    )

    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        logger.warning(
            "[listing.ended] Trading HTTP error status=%s for item_id=%s: %s",
            resp.status_code,
            item_id,
            e,
        )

    return resp


def _xml_text(node: ET.Element, path: str, ns: dict[str, str]) -> Optional[str]:
    el = node.find(path, ns)
    if el is None or el.text is None:
        return None
    return el.text


@dataclass
class TradingSnapshot:
    ended: bool
    sold_flag: bool
    final_price: Optional[float]
    bid_count: int
    ebay_end_time: Optional[str]


def _parse_trading_get_item(xml: str) -> Optional[dict]:
    """
    Parse Trading GetItem XML.
    Returns:
      - {"code": "<errcode>"} for "not found" style errors (1505/21920397)
      - None for other failures
      - dict with snapshot fields for success
    """
    try:
        root = ET.fromstring(xml)
    except Exception as e:
        logger.error("[listing.ended] could not parse Trading XML: %s", e)
        return None

    ns = {"ns": "urn:ebay:apis:eBLBaseComponents"}

    ack = _xml_text(root, "./ns:Ack", ns)
    if not ack or ack.strip().lower() != "success":
        short_msg = _xml_text(root, "./ns:Errors/ns:ShortMessage", ns) or ""
        long_msg = _xml_text(root, "./ns:Errors/ns:LongMessage", ns) or ""
        code = _xml_text(root, "./ns:Errors/ns:ErrorCode", ns) or ""
        logger.warning(
            "[listing.ended] Trading error ack=%s code=%s short=%s long=%s",
            ack,
            code,
            short_msg,
            long_msg,
        )

        # Handle Item Not Found explicitly (keep old behaviour)
        if code in ("1505", "21920397"):
            return {"code": code}

        return None

    item_node = root.find(".//ns:Item", ns)
    if item_node is None:
        logger.error("[listing.ended] No <Item> node in successful Trading response")
        return None

    listing_status = (
            (_xml_text(item_node, "./ns:ListingStatus", ns) or "")
            or (_xml_text(item_node, "./ns:SellingStatus/ns:ListingStatus", ns) or "")
    ).lower()

    ended = listing_status in ("completed", "ended", "completedwithsales", "completedwithoutsales")

    # SellingStatus bits
    bid_count_txt = _xml_text(item_node, "./ns:SellingStatus/ns:BidCount", ns) or "0"
    try:
        bid_count = int(bid_count_txt)
    except Exception:
        bid_count = 0

    current_price_txt = _xml_text(item_node, "./ns:SellingStatus/ns:CurrentPrice", ns)
    final_price: Optional[float] = None
    if current_price_txt is not None:
        try:
            final_price = float(current_price_txt)
        except Exception:
            final_price = None

    # If bid_count > 0 treat as sold_flag (keeps the “sold vs ended” split simple)
    sold_flag = bid_count > 0

    ebay_end_time = _xml_text(item_node, "./ns:ListingDetails/ns:EndTime", ns)

    return {
        "ended": ended,
        "sold_flag": sold_flag,
        "final_price": final_price,
        "bid_count": bid_count,
        "ebay_end_time": ebay_end_time,
    }


def _load_candidates(limit: int, grace_minutes: int) -> List[Tuple[int, str, str, Any]]:
    """
    Returns rows like:
      (auction_id, external_id, source, end_time)
    This mirrors your previous pipeline expectation.
    """
    cutoff = now_utc() - timedelta(minutes=grace_minutes)
    logger.info(
        "[listing.ended] selecting up to %s candidates with end_time <= %s",
        limit,
        cutoff,
    )

    '''
    SELECT id, external_id, source, end_time
    FROM auction_listings
    WHERE
    (
        finalized = FALSE
        AND status IN ('active', 'ending', 'live')
        AND end_time IS NOT NULL
        AND end_time <= %s
    )
    OR
    (
        finalized = TRUE
        AND status = 'parse_failed'
    )
    ORDER BY end_time ASC
    LIMIT %s;         
    '''

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, external_id, source, end_time
                FROM auction_listings
                WHERE
                (
                    finalized = FALSE
                    AND status IN ('active', 'ending', 'live')
                    AND end_time IS NOT NULL
                    AND end_time <= %s
                )
                ORDER BY end_time ASC
                LIMIT %s;
                """,
                (cutoff, limit),
            )
            rows = cur.fetchall() or []

    return rows


def _apply_updates(updates: Sequence[Tuple[int, Optional[float], int]]) -> int:
    """
    updates tuples: (auction_id, final_price_or_none, bid_count)
    Mirrors your old behaviour:
      - final_price is None => ended
      - final_price set => sold
    """
    if not updates:
        return 0

    with get_connection() as conn:
        with conn.cursor() as cur:
            for auction_id, final_price, bid_count in updates:
                if final_price is None:
                    cur.execute(
                        """
                        UPDATE auction_listings
                        SET finalized = TRUE,
                            status = 'ended',
                            bids_count = %s
                        WHERE id = %s
                        """,
                        (bid_count, auction_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE auction_listings
                        SET finalized = TRUE,
                            status = 'sold',
                            final_price = %s,
                            bids_count = %s
                        WHERE id = %s
                        """,
                        (final_price, bid_count, auction_id),
                    )
        conn.commit()

    return len(updates)


# ----------------------------
# LangGraph orchestration
# ----------------------------

class EndedState(TypedDict, total=False):
    limit: int
    grace_minutes: int
    candidates: List[Tuple[int, str, str, Any]]
    idx: int
    processed: int
    updates: List[Tuple[int, Optional[float], int]]
    applied: int
    skipped: int
    errors: int


def node_select_candidates(state: EndedState) -> EndedState:
    limit = int(state.get("limit", 50))
    grace = int(state.get("grace_minutes", 30))

    rows = _load_candidates(limit=limit, grace_minutes=grace)
    logger.info("[listing.ended] candidates=%s", len(rows))

    return {
        **state,
        "candidates": rows,
        "idx": 0,
        "processed": 0,
        "updates": [],
        "applied": 0,
        "skipped": 0,
        "errors": 0,
    }


def node_process_next_candidate(state: EndedState) -> EndedState:
    candidates = state.get("candidates", [])
    idx = int(state.get("idx", 0))

    if idx >= len(candidates):
        return state

    row = candidates[idx]
    auction_id, external_id, source, end_time = row

    item_id = _extract_numeric_item_id(str(external_id))
    if not item_id:
        logger.error(
            "[listing.ended] could not extract item_id auction_id=%s external_id=%s; marking parse_failed+finalized",
            auction_id,
            external_id,
        )
        _mark_parse_failed(auction_id)
        return _bump(state, idx_inc=1, processed_inc=1, errors_inc=1)

    try:
        # rate/usage tracking (keeps your old habit)
        increment_api_usage("ebay_trading_get_item")

        resp = _call_trading_get_item(item_id)
        parsed = _parse_trading_get_item(resp.text)

        if not parsed:
            logger.error(
                "[listing.ended] could not parse Trading response auction_id=%s external_id=%s item_id=%s; marking parse_failed+finalized",
                auction_id,
                external_id,
                item_id,
            )
            _mark_parse_failed(auction_id)
            return _bump(state, idx_inc=1, processed_inc=1, errors_inc=1)

        # Item not found / already gone => finalize as ended (old behaviour)
        if parsed.get("code") in ("17", "1505", "21920397"):
            _finalize_not_found(auction_id)
            return _bump(state, idx_inc=1, processed_inc=1)

        ended = bool(parsed.get("ended"))
        ebay_end_time = parsed.get("ebay_end_time")

        # If Trading says NOT ended, refresh end_time and skip finalization (old behaviour)
        if not ended:
            if ebay_end_time:
                _refresh_end_time(auction_id, ebay_end_time)
            return _bump(state, idx_inc=1, processed_inc=1, skipped_inc=1)

        bid_count = int(parsed.get("bid_count") or 0)
        sold_flag = bool(parsed.get("sold_flag"))
        final_price = float(parsed["final_price"]) if sold_flag and parsed.get("final_price") is not None else None

        updates = list(state.get("updates", []))
        updates.append((auction_id, final_price, bid_count))

        return {
            **state,
            "idx": idx + 1,
            "processed": int(state.get("processed", 0)) + 1,
            "updates": updates,
        }

    except Exception as e:
        logger.exception("[listing.ended] fetch/build failed for row=%s: %s", row, e)
        return _bump(state, idx_inc=1, processed_inc=1, errors_inc=1)


def _route_after_process(state: EndedState) -> str:
    candidates = state.get("candidates", [])
    idx = int(state.get("idx", 0))
    return "process_next_candidate" if idx < len(candidates) else "apply_updates"


def node_apply_updates(state: EndedState) -> EndedState:
    updates = state.get("updates", [])
    applied = _apply_updates(updates)
    logger.info("[listing.ended] applied=%s", applied)

    return {**state, "applied": applied}


def _mark_parse_failed(auction_id: int) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auction_listings
                SET status = 'parse_failed',
                    finalized = TRUE
                WHERE id = %s
                """,
                (auction_id,),
            )
        conn.commit()


def _finalize_not_found(auction_id: int) -> None:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE auction_listings
                SET finalized = TRUE,
                    status = 'ended'
                WHERE id = %s
                """,
                (auction_id,),
            )
        conn.commit()


def _refresh_end_time(auction_id: int, ebay_end_time: str) -> None:
    # ebay_end_time is ISO-ish from Trading. Store as-is; DB adapter can coerce if needed.
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "UPDATE auction_listings SET end_time = %s WHERE id = %s",
                    (ebay_end_time, auction_id),
                )
            conn.commit()
        logger.info(
            "[listing.ended] refreshed end_time from Trading for auction_id=%s new_end_time=%s",
            auction_id,
            ebay_end_time,
        )
    except Exception:
        logger.exception(
            "[listing.ended] failed refreshing end_time for auction_id=%s end_time=%s",
            auction_id,
            ebay_end_time,
        )


def _bump(
        state: EndedState,
        *,
        idx_inc: int = 0,
        processed_inc: int = 0,
        skipped_inc: int = 0,
        errors_inc: int = 0,
) -> EndedState:
    return {
        **state,
        "idx": int(state.get("idx", 0)) + idx_inc,
        "processed": int(state.get("processed", 0)) + processed_inc,
        "skipped": int(state.get("skipped", 0)) + skipped_inc,
        "errors": int(state.get("errors", 0)) + errors_inc,
    }


def build_graph():
    sg = StateGraph(EndedState)

    sg.add_node("select_candidates", node_select_candidates)
    sg.add_node("process_next_candidate", node_process_next_candidate)
    sg.add_node("apply_updates", node_apply_updates)

    sg.set_entry_point("select_candidates")
    sg.add_edge("select_candidates", "process_next_candidate")

    sg.add_conditional_edges(
        "process_next_candidate",
        _route_after_process,
        {
            "process_next_candidate": "process_next_candidate",
            "apply_updates": "apply_updates",
        },
    )

    sg.add_edge("apply_updates", END)

    return sg.compile()


_GRAPH = build_graph()


def save_graph_diagram(path: str = "ended_graph.mmd") -> None:
    graph = build_graph()
    g = graph.get_graph()

    # Mermaid text (works everywhere)
    mermaid = g.draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)


def run(*, limit: int = 50, grace_minutes: int = 30) -> dict:
    """
    Public API for CLI / cron runner.
    Returns a summary dict (stable shape).
    """
    state: EndedState = {"limit": int(limit), "grace_minutes": int(grace_minutes)}
    final = _GRAPH.invoke(state, config={"recursion_limit": 500})

    # langgraph returns a dict-like state, not a dataclass
    summary = {
        "limit": int(limit),
        "grace_minutes": int(grace_minutes),
        "candidates": len(final.get("candidates", []) or []),
        "processed": int(final.get("processed", 0)),
        "updates": len(final.get("updates", []) or []),
        "applied": int(final.get("applied", 0)),
        "skipped": int(final.get("skipped", 0)),
        "errors": int(final.get("errors", 0)),
    }

    logger.info(
        "[listing.ended] processed=%s updates=%s skipped=%s errors=%s",
        summary["processed"],
        summary["updates"],
        summary["skipped"],
        summary["errors"],
    )

    return summary
