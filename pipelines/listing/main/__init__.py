from __future__ import annotations

import os
from typing import TypedDict, Any, Dict

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.auth import get_auth
from utils.usage_tracker import get_all_api_usage_today

from pipelines.listing.ended.graph import build_graph as build_ended
from pipelines.listing.retrieve.graph import build_graph as build_retrieve
from pipelines.listing.pph.graph import build_graph as build_pph
from pipelines.listing.comps.graph import build_graph as build_comps
from pipelines.listing.attributes.graph import build_graph as build_attributes
from pipelines.listing.hot.graph import build_graph as build_hot
from pipelines.listing.roi.graph import build_graph as build_roi
from pipelines.listing.new.graph import build_graph as build_new

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Global daily limit across *all* services tracked in api_usage
DEFAULT_DAILY_LIMIT = 5000


class MainState(TypedDict, total=False):
    # shared inputs
    ebay_token: str

    # limit / skip info
    skip: bool
    limit_info: Dict[str, Any]

    # subgraph outputs (namespaced so nothing collides)
    ended_out: Dict[str, Any]
    retrieve_out: Dict[str, Any]
    pph_out: Dict[str, Any]
    comps_out: Dict[str, Any]
    attributes_out: Dict[str, Any]
    hot_out: Dict[str, Any]
    roi_out: Dict[str, Any]
    new_out: Dict[str, Any]


def _get_daily_limit() -> int:
    """
    Resolve the daily limit from EBAY_DAILY_LIMIT env var, with a sane default.
    (Name kept for now, but this is a *global* cap for all services.)
    """
    raw = os.getenv("EBAY_DAILY_LIMIT")
    if raw is None:
        return DEFAULT_DAILY_LIMIT

    try:
        value = int(raw)
    except ValueError:
        logger.warning(
            "[main] Invalid EBAY_DAILY_LIMIT=%r; falling back to default %d",
            raw,
            DEFAULT_DAILY_LIMIT,
        )
        return DEFAULT_DAILY_LIMIT

    if value <= 0:
        logger.warning(
            "[main] Non-positive EBAY_DAILY_LIMIT=%d; falling back to default %d",
            value,
            DEFAULT_DAILY_LIMIT,
        )
        return DEFAULT_DAILY_LIMIT

    return value


def init(state: MainState) -> MainState:
    """
    Entry node:
    - Check today's *total* API usage across all services vs daily limit
    - Either mark run as 'skip' or acquire a shared ebay_token
    """
    daily_limit = _get_daily_limit()

    # Full picture (for logging / debugging)
    all_usage = get_all_api_usage_today()
    used_today = sum(all_usage.values())

    state["limit_info"] = {
        "mode": "global_total",
        "used_today": used_today,
        "daily_limit": daily_limit,
        "all_usage": all_usage,
    }

    if used_today >= daily_limit:
        logger.warning(
            "[main] DAILY GLOBAL API LIMIT REACHED: used=%d / limit=%d. "
            "Skipping heartbeat pipeline.",
            used_today,
            daily_limit,
        )
        state["skip"] = True
        return state

    logger.info(
        "[main] Daily global API usage: used=%d / limit=%d – proceeding with run",
        used_today,
        daily_limit,
    )

    # acquire token once for the whole run
    state["ebay_token"] = get_auth().get_token()
    state["skip"] = False
    return state


def should_continue_after_init(state: MainState) -> str:
    """
    Conditional routing after init:
    - If skip=True → short-circuit to END
    - Else → continue into 'ended'
    """
    if state.get("skip"):
        return "skip"
    return "run"


def run_subgraph(name: str, build_graph_fn):
    """
    Adapter node factory: calls compiled subgraph and stores output under <name>_out.
    """
    def _node(state: MainState) -> MainState:
        graph = build_graph_fn()
        logger.info(f"[main] -> {name} begin")
        out = graph.invoke(dict(state), config={"recursion_limit": 500})
        state[f"{name}_out"] = out
        logger.info(f"[main] -> {name} end")
        return state

    return _node


def build_graph():
    g = StateGraph(MainState)

    # Nodes
    g.add_node("init", init)

    g.add_node("ended", run_subgraph("ended", build_ended))
    g.add_node("retrieve", run_subgraph("retrieve", build_retrieve))
    g.add_node("pph", run_subgraph("pph", build_pph))
    g.add_node("comps", run_subgraph("comps", build_comps))
    g.add_node("attributes", run_subgraph("attributes", build_attributes))
    g.add_node("hot", run_subgraph("hot", build_hot))
    g.add_node("roi", run_subgraph("roi", build_roi))
    g.add_node("new", run_subgraph("new", build_new))

    g.set_entry_point("init")

    # Conditional branch right after init:
    # - "skip" → END
    # - "run"  → proceed into the normal chain starting at "ended"
    g.add_conditional_edges(
        "init",
        should_continue_after_init,
        {
            "skip": END,
            "run": "ended",
        },
    )

    # Straight chain with PPH slotted after retrieve
    g.add_edge("ended", "retrieve")
    g.add_edge("retrieve", "pph")
    g.add_edge("pph", "comps")
    g.add_edge("comps", "attributes")
    g.add_edge("attributes", "hot")
    g.add_edge("hot", "roi")
    g.add_edge("roi", "new")
    g.add_edge("new", END)

    return g.compile()


def save_graph_diagram(path: str = "main_graph.mmd") -> None:
    graph = build_graph()
    g = graph.get_graph()

    mermaid = g.draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)

    logger.info(f"[main] wrote graph mermaid to {path}")


def run() -> MainState:
    graph = build_graph()
    logger.info("[main] heartbeat run begin")
    out: MainState = graph.invoke({}, config={"recursion_limit": 800})

    if out.get("skip"):
        info = out.get("limit_info") or {}
        logger.info(
            "[main] heartbeat SKIPPED due to daily GLOBAL API limit "
            "(used=%s / limit=%s; all=%s)",
            info.get("used_today"),
            info.get("daily_limit"),
            info.get("all_usage"),
        )
    else:
        logger.info("[main] heartbeat run end")

    return out
