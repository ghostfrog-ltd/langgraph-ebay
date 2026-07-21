from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.db_schema import connection

ASSESS_ENABLED = os.getenv("GF_ASSESS_ENABLED", "0") == "1"

# Keep the Gemini wiring in place for later, but only import it when assess is enabled.
# This avoids touching the API path while the feature is paused.
if ASSESS_ENABLED:
    from pipelines.listing.assess.gemini_model_client import post_to_gemini as post_to_model
else:
    post_to_model = None

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
    Load LLM assessment candidates through normalized listing projections.

    The assessment feature is disabled by default, but when enabled it expects a
    wide candidate payload. Keep the output shape stable while moving the read
    contract away from direct `auction_listings` access.
    """
    sql = """
        SELECT
            identity.listing_id AS id,
            identity.external_id,
            identity.source_label AS source,
            identity.title,
            identity.url,
            identity.sale_type,
            identity.first_seen,
            identity.source_id,
            observation.price_current,
            observation.price_bid_current,
            observation.bids_count,
            observation.end_time,
            observation.time_left_s,
            observation.fetched_at,
            observation.last_seen_at,
            outcome.status,
            outcome.finalized,
            outcome.final_price,
            attrs.model_key,
            attrs.brand,
            attrs.product_family,
            attrs.model_name,
            attrs.storage_gb,
            attrs.colour,
            attrs.epid,
            attrs.raw_attrs,
            score.roi_estimate,
            score.max_bid,
            score.bucket_key,
            score.notes,
            c.median_final_price,
            c.mean_final_price,
            c.samples AS comp_samples
        FROM listing_identity_v AS identity
        JOIN listing_observation_v AS observation
          ON observation.listing_id = identity.listing_id
        JOIN listing_outcome_v AS outcome
          ON outcome.listing_id = identity.listing_id
        JOIN listing_attribute_v AS attrs
          ON attrs.listing_id = identity.listing_id
        JOIN listing_score_v AS score
          ON score.listing_id = identity.listing_id
        LEFT JOIN latest_comps AS c
          ON attrs.model_key = c.model_key
        LEFT JOIN listing_assessments AS a
          ON a.listing_id = identity.listing_id
        WHERE outcome.status = 'live'
          AND COALESCE(outcome.finalized, FALSE) = FALSE
          AND a.id IS NULL
        ORDER BY observation.end_time ASC NULLS LAST, identity.listing_id ASC
        LIMIT %s
    """

    # Using RealDictCursor (if using psycopg2) makes this much cleaner:
    # from psycopg2.extras import RealDictCursor
    # with connection.cursor(cursor_factory=RealDictCursor) as cur:

    with connection.cursor() as cur:
        cur.execute(sql, (limit,))
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    candidates: List[Dict[str, Any]] = []
    for row in rows:
        # Create a dict from row/colnames to preserve everything
        d = dict(zip(colnames, row))

        # Clean up types for the LLM / JSON state
        candidates.append({
            "listing_id": d["id"],
            "external_id": d["external_id"],
            "source": d["source"],
            "title": d["title"],
            "price_current": float(d["price_current"]) if d["price_current"] is not None else None,
            "price_bid_current": float(d["price_bid_current"]) if d["price_bid_current"] is not None else None,
            "bids_count": int(d["bids_count"]) if d["bids_count"] is not None else None,
            "end_time": d["end_time"].isoformat() if d["end_time"] else None,
            "status": d["status"],
            "url": d["url"],
            "first_seen": d["first_seen"].isoformat() if d["first_seen"] else None,
            "fetched_at": d["fetched_at"].isoformat() if d["fetched_at"] else None,
            "roi_estimate": float(d["roi_estimate"]) if d["roi_estimate"] is not None else None,
            "max_bid": float(d["max_bid"]) if d["max_bid"] is not None else None,
            "notes": d["notes"],
            "source_id": d["source_id"],
            "final_price": float(d["final_price"]) if d["final_price"] is not None else None,
            "sale_type": d["sale_type"],
            "model_key": d["model_key"],
            "time_left_s": int(d["time_left_s"]) if d["time_left_s"] is not None else None,
            "finalized": bool(d["finalized"]),
            "brand": d["brand"],
            "product_family": d["product_family"],
            "model_name": d["model_name"],
            "storage_gb": float(d["storage_gb"]) if d["storage_gb"] is not None else None,
            "colour": d["colour"],
            "epid": d["epid"],
            "raw_attrs": d["raw_attrs"],
            "last_seen_at": d["last_seen_at"].isoformat() if d["last_seen_at"] else None,
            "bucket_key": d["bucket_key"],
            # NEW MARKET DATA FIELDS
            "market_median": float(d["median_final_price"]) if d["median_final_price"] is not None else None,
            "market_mean": float(d["mean_final_price"]) if d["mean_final_price"] is not None else None,
            "comp_samples": d["comp_samples"] or 0
        })

    logger.info("[assess] loaded %s listings with full metadata and comps", len(candidates))
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

    if not ASSESS_ENABLED:
        logger.info("[assess] Gemini assess is disabled; returning no candidates")
        state["limit"] = limit
        state["candidates"] = []
        state["idx"] = 0
        state.setdefault("results", [])
        return state

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
