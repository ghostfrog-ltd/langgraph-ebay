from __future__ import annotations

import json
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.db_schema import connection

# Adjust this import to match where you put the helper you pasted
from pipelines.listing.assess.model_client import post_to_model

logger = get_logger(__name__)


class AssessState(TypedDict, total=False):
    limit: int
    # loaded candidates
    candidates: List[Dict[str, Any]]
    idx: int
    # results / stats
    results: List[Dict[str, Any]]


# -----------------------------
# DB access
# -----------------------------
def _load_candidates(limit: int) -> List[Dict[str, Any]]:
    """
    Load listings that still need an LLM assessment.

    Strategy:
      - status='live'
      - No existing row in listing_assessments
      - Oldest end_time first (so we prioritise near-ending stuff)
    """
    sql = """
        SELECT
            l.id,
            l.external_id,
            l.source,
            l.title,
            l.price_current,
            l.price_bid_current,
            l.bids_count,
            l.end_time,
            l.status,
            l.url,
            l.first_seen,
            l.fetched_at,
            l.roi_estimate,
            l.max_bid,
            l.notes,
            l.source_id,
            l.final_price,
            l.sale_type,
            l.model_key,
            l.time_left_s,
            l.finalized,
            l.brand,
            l.product_family,
            l.model_name,
            l.storage_gb,
            l.colour,
            l.epid,
            l.raw_attrs,
            l.last_seen_at,
            l.bucket_key
        FROM auction_listings AS l
        LEFT JOIN listing_assessments AS a
          ON a.listing_id = l.id
        WHERE l.status = 'live'
          AND COALESCE(l.finalized, FALSE) = FALSE
          AND a.id IS NULL
        ORDER BY l.end_time ASC NULLS LAST
        LIMIT %s
    """

    with connection.cursor() as cur:
        cur.execute(sql, (limit,))
        rows = cur.fetchall()

    candidates: List[Dict[str, Any]] = []
    for row in rows:
        (
            listing_id,
            external_id,
            source,
            title,
            price_current,
            price_bid_current,
            bids_count,
            end_time,
            status,
            url,
            first_seen,
            fetched_at,
            roi_estimate,
            max_bid,
            notes,
            source_id,
            final_price,
            sale_type,
            model_key,
            time_left_s,
            finalized,
            brand,
            product_family,
            model_name,
            storage_gb,
            colour,
            epid,
            raw_attrs,
            last_seen_at,
            bucket_key,
        ) = row

        candidates.append(
            {
                "listing_id": listing_id,
                "external_id": external_id,
                "source": source,
                "title": title,
                "price_current": float(price_current) if price_current is not None else None,
                "price_bid_current": float(price_bid_current) if price_bid_current is not None else None,
                "bids_count": int(bids_count) if bids_count is not None else None,
                "end_time": end_time.isoformat() if end_time else None,
                "status": status,
                "url": url,
                "first_seen": first_seen.isoformat() if first_seen else None,
                "fetched_at": fetched_at.isoformat() if fetched_at else None,
                "roi_estimate": float(roi_estimate) if roi_estimate is not None else None,
                "max_bid": float(max_bid) if max_bid is not None else None,
                "notes": notes,
                "source_id": source_id,
                "final_price": float(final_price) if final_price is not None else None,
                "sale_type": sale_type,
                "model_key": model_key,
                "time_left_s": int(time_left_s) if time_left_s is not None else None,
                "finalized": bool(finalized) if finalized is not None else None,
                "brand": brand,
                "product_family": product_family,
                "model_name": model_name,
                "storage_gb": float(storage_gb) if storage_gb is not None else None,
                "colour": colour,
                "epid": epid,
                "raw_attrs": raw_attrs,  # JSON/text as-is; model can inspect
                "last_seen_at": last_seen_at.isoformat() if last_seen_at else None,
                "bucket_key": bucket_key,
            }
        )

    logger.info("[assess] loaded %s listings for LLM assessment", len(candidates))
    return candidates


def _normalise_assessment(raw: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure we always work with a flat assessment object:
    {verdict, confidence, recommended_max_bid, facts, ...}

    If the model returns {"listing": {...}}, strip that wrapper.
    """
    if not isinstance(raw, dict):
        return {}

    # If it's wrapped like {"listing": {...}}, unwrap it
    listing = raw.get("listing")
    if isinstance(listing, dict):
        raw = listing

    return raw


def _save_assessment(listing_id: int, assessment: Dict[str, Any]) -> None:
    """
    Persist a single LLM assessment to listing_assessments.
    Also denormalise key fields into dedicated columns for easy querying.
    """
    payload = json.dumps(assessment)

    verdict = assessment.get("verdict")
    confidence = assessment.get("confidence")
    recommended_max_bid = assessment.get("recommended_max_bid")

    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO listing_assessments (
                listing_id,
                assessment,
                verdict,
                confidence,
                recommended_max_bid,
                created_at
            )
            VALUES (
                %s,  -- listing_id
                %s,  -- assessment (JSON)
                %s,  -- verdict
                %s,  -- confidence
                %s,  -- recommended_max_bid
                (now() AT TIME ZONE 'utc')
            )
            """,
            (
                listing_id,
                payload,
                verdict,
                confidence,
                recommended_max_bid,
            ),
        )
    connection.commit()


# -----------------------------
# Nodes
# -----------------------------
def init_state(state: AssessState) -> AssessState:
    limit = int(state.get("limit", 20))
    candidates = _load_candidates(limit)

    state["limit"] = limit
    state["candidates"] = candidates
    state["idx"] = 0
    state.setdefault("results", [])

    return state


def assess_current(state: AssessState) -> AssessState:
    idx = int(state.get("idx", 0))
    candidates = state.get("candidates") or []

    if idx >= len(candidates):
        # Nothing left to do
        return state

    entry = candidates[idx]
    listing_id = entry["listing_id"]

    try:
        logger.info(
            "[assess] listing_id=%s external_id=%s title=%r",
            listing_id,
            entry.get("external_id"),
            (entry.get("title") or "")[:80],
        )

        # Call your existing model helper
        assessment_raw = post_to_model(entry)
        assessment = _normalise_assessment(assessment_raw)

        _save_assessment(listing_id, assessment)

        state.setdefault("results", []).append(
            {
                "listing_id": listing_id,
                "status": "assessed",
                "verdict": assessment.get("verdict"),
                "confidence": assessment.get("confidence"),
                "recommended_max_bid": assessment.get("recommended_max_bid"),
            }
        )
    except Exception as e:
        logger.warning(
            "[assess] listing_id=%s failed: %s\n%s",
            listing_id,
            e,
            traceback.format_exc(),
        )
        # roll back any partial write
        connection.rollback()
        state.setdefault("results", []).append(
            {
                "listing_id": listing_id,
                "status": "failed",
                "error": str(e),
            }
        )

    # Move to next
    state["idx"] = idx + 1
    return state


def should_continue(state: AssessState) -> str:
    idx = int(state.get("idx", 0))
    candidates = state.get("candidates") or []
    return "continue" if idx < len(candidates) else "done"


# -----------------------------
# Build graph
# -----------------------------
def build_graph():
    g = StateGraph(AssessState)

    g.add_node("init", init_state)
    g.add_node("assess", assess_current)

    g.set_entry_point("init")
    g.add_edge("init", "assess")

    g.add_conditional_edges(
        "assess",
        should_continue,
        {
            "continue": "assess",
            "done": END,
        },
    )

    return g.compile()


def save_graph_diagram(path: str = "assess_graph.mmd") -> None:
    graph = build_graph()
    g = graph.get_graph()

    mermaid = g.draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)

    logger.info("[assess] wrote graph mermaid to %s", path)


def run(limit: int = 20) -> AssessState:
    """
    Run a batch of LLM assessments for listings needing assessment.

    limit: max number of listings to process in this run
    """
    graph = build_graph()
    initial: AssessState = {"limit": limit}

    logger.info("[assess] Begin LLM assessment batch (limit=%s)", limit)
    out: AssessState = graph.invoke(initial, config={"recursion_limit": 500})
    logger.info("[assess] End LLM assessment batch")

    return out
