from __future__ import annotations

from typing import TypedDict, Any, Dict, Optional
from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.auth import get_auth

from pipelines.listing.ended.graph import build_graph as build_ended
from pipelines.listing.retrieve.graph import build_graph as build_retrieve
from pipelines.listing.comps.graph import build_graph as build_comps
from pipelines.listing.attributes.graph import build_graph as build_attributes
from pipelines.listing.hot.graph import build_graph as build_hot
from pipelines.listing.roi.graph import build_graph as build_roi
from pipelines.listing.new.graph import build_graph as build_new

logger = get_logger(__name__)


class MainState(TypedDict, total=False):
    # shared inputs
    ebay_token: str

    # subgraph outputs (namespaced so nothing collides)
    ended_out: Dict[str, Any]
    retrieve_out: Dict[str, Any]
    comps_out: Dict[str, Any]
    attributes_out: Dict[str, Any]
    hot_out: Dict[str, Any]
    roi_out: Dict[str, Any]
    new_out: Dict[str, Any]


def init(state: MainState) -> MainState:
    # acquire token once for the whole run
    state["ebay_token"] = get_auth().get_token()
    return state


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

    g.add_node("init", init)

    g.add_node("ended", run_subgraph("ended", build_ended))
    g.add_node("retrieve", run_subgraph("retrieve", build_retrieve))
    g.add_node("comps", run_subgraph("comps", build_comps))
    g.add_node("attributes", run_subgraph("attributes", build_attributes))
    g.add_node("hot", run_subgraph("hot", build_hot))
    g.add_node("roi", run_subgraph("roi", build_roi))
    g.add_node("new", run_subgraph("new", build_new))

    g.set_entry_point("init")

    # Pattern A: straight chain
    g.add_edge("init", "ended")
    g.add_edge("ended", "retrieve")
    g.add_edge("retrieve", "comps")
    g.add_edge("comps", "attributes")
    g.add_edge("attributes", "hot")
    g.add_edge("hot", "roi")
    g.add_edge("roi", "new")
    g.add_edge("new", END)

    return g.compile()

def save_graph_diagram(path: str = "main_graph.mmd") -> None:
    graph = build_graph()
    g = graph.get_graph()

    # Mermaid text (works everywhere)
    mermaid = g.draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)

def run() -> MainState:
    graph = build_graph()
    logger.info("[main] heartbeat run begin")
    out: MainState = graph.invoke({}, config={"recursion_limit": 800})
    logger.info("[main] heartbeat run end")
    return out
