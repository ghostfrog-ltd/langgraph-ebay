from __future__ import annotations

import traceback
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.db_schema import connection

# Import adapters (classes)
from .adapters.motomine import Adapter as MotoMineAdapter
from .adapters.consoles import Adapter as ConsolesAdapter
from .adapters.retro_pc import Adapter as RetroPcAdapter
from .adapters.actioncams import Adapter as ActionCamAdapter
from .adapters.watches import Adapter as WatchAdapter
from .adapters.apple import Adapter as AppleAdapter
from .adapters.tools import Adapter as ToolAdapter
from .adapters.motors import Adapter as MotorsAdapter
from .adapters.lego import Adapter as LegoAdapter
from .adapters.pokemon import Adapter as PokemonAdapter
from .adapters.samsung import Adapter as SamsungAdapter
from .adapters.headphones import Adapter as HeadphonesAdapter
from .adapters.hondaNc750 import Adapter as Nc750Adapter

logger = get_logger(__name__)

# -----------------------------
# Adapter registry (serializable names)
# -----------------------------
ADAPTERS: Dict[str, Any] = {
    "motomine": MotoMineAdapter,
    "apple": AppleAdapter,
    "consoles": ConsolesAdapter,
    "retro_pc": RetroPcAdapter,
    "actioncams": ActionCamAdapter,
    "watches": WatchAdapter,
    "tools": ToolAdapter,
    "motors": MotorsAdapter,
    "lego": LegoAdapter,
    "pokemon": PokemonAdapter,
    "samsung": SamsungAdapter,
    "headphones": HeadphonesAdapter,
    "nc750": Nc750Adapter,
}

DEFAULT_ADAPTER_ORDER: List[str] = [
    "motomine",
    "apple",
    "consoles",
    "retro_pc",
    "actioncams",
    "watches",
    "tools",
    "motors",
    "lego",
    "pokemon",
    "samsung",
    "headphones",
    "nc750",
]


class RetrieveState(TypedDict, total=False):
    ebay_token: str
    adapter_names: List[str]
    idx: int
    current_adapter_name: Optional[str]
    results: List[Dict[str, Any]]


def _gate_should_run(domain: str) -> Dict[str, Any]:
    """
    Returns dict:
      - ok: bool
      - reason: str
      - interval: int
      - last_run: datetime|None
      - next_time: datetime|None
    """
    now = datetime.now(timezone.utc)

    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT scrape_interval_seconds, last_scraped_at
            FROM sources
            WHERE domain = %s AND enabled = TRUE
            LIMIT 1
            """,
            (domain,),
        )
        row = cur.fetchone()

    if not row:
        return {
            "ok": False,
            "reason": "no_source_record_or_disabled",
            "interval": 0,
            "last_run": None,
            "next_time": None,
        }

    interval, last_run = row
    interval = int(interval or 0)

    if last_run and getattr(last_run, "tzinfo", None) is None:
        last_run = last_run.replace(tzinfo=timezone.utc)

    if last_run and interval > 0:
        elapsed = (now - last_run).total_seconds()
        if elapsed < interval:
            next_time = last_run + timedelta(seconds=interval)
            return {
                "ok": False,
                "reason": "gated",
                "interval": interval,
                "last_run": last_run,
                "next_time": next_time,
            }

    return {
        "ok": True,
        "reason": "allowed",
        "interval": interval,
        "last_run": last_run,
        "next_time": None,
    }


def _mark_last_scraped(domain: str) -> None:
    with connection.cursor() as cur:
        cur.execute(
            """
            UPDATE sources
            SET last_scraped_at = (now() AT TIME ZONE 'utc')
            WHERE domain = %s
            """,
            (domain,),
        )
    connection.commit()


# -----------------------------
# Nodes
# -----------------------------
def init_state(state: RetrieveState) -> RetrieveState:
    state.setdefault("adapter_names", DEFAULT_ADAPTER_ORDER)
    state.setdefault("idx", 0)
    state.setdefault("results", [])
    state["current_adapter_name"] = None
    return state


def pick_next_adapter(state: RetrieveState) -> RetrieveState:
    idx = int(state.get("idx", 0))
    names = state.get("adapter_names") or []
    if idx >= len(names):
        state["current_adapter_name"] = None
        return state

    state["current_adapter_name"] = names[idx]
    return state


def run_current_adapter(state: RetrieveState) -> RetrieveState:
    name = state.get("current_adapter_name")
    if not name:
        return state

    adapter_cls = ADAPTERS.get(name)
    if adapter_cls is None:
        state["results"].append(
            {"adapter": name, "status": "skipped", "reason": "unknown_adapter_name"}
        )
        state["idx"] = int(state.get("idx", 0)) + 1
        return state

    adapter = adapter_cls()
    domain = getattr(adapter, "DOMAIN", name)

    gate = _gate_should_run(domain)

    if not gate["ok"]:
        if gate["reason"] == "gated":
            logger.info(
                f"[scrape:{domain}] gated → next allowed run at "
                f"{gate['next_time'].strftime('%H:%M:%S')} (interval={gate['interval']}s)"
            )
        else:
            logger.warning(f"[scrape:{domain}] skipped (no source record or disabled)")

        state["results"].append(
            {
                "adapter": name,
                "domain": domain,
                "status": "gated" if gate["reason"] == "gated" else "skipped",
                "reason": gate["reason"],
                "interval": gate["interval"],
                "last_run": gate["last_run"].isoformat() if gate["last_run"] else None,
                "next_time": gate["next_time"].isoformat() if gate["next_time"] else None,
            }
        )
        state["idx"] = int(state.get("idx", 0)) + 1
        return state

    # allowed -> run
    try:
        logger.info(f"[scrape:{domain}] begin API fetch")
        adapter.fetch_listings_api(state["ebay_token"])
        logger.info(f"[scrape:{domain}] API fetch complete")

        _mark_last_scraped(domain)

        state["results"].append({"adapter": name, "domain": domain, "status": "ran"})
    except Exception as e:
        logger.warning(f"[scrape:{domain}] API fetch failed: {e}\n{traceback.format_exc()}")
        connection.rollback()
        state["results"].append(
            {"adapter": name, "domain": domain, "status": "failed", "error": str(e)}
        )

    state["idx"] = int(state.get("idx", 0)) + 1
    return state


def should_continue(state: RetrieveState) -> str:
    idx = int(state.get("idx", 0))
    names = state.get("adapter_names") or []
    return "continue" if idx < len(names) else "done"


# -----------------------------
# Build graph
# -----------------------------
def build_retrieve_graph():
    g = StateGraph(RetrieveState)

    g.add_node("init", init_state)
    g.add_node("pick_next", pick_next_adapter)
    g.add_node("run_adapter", run_current_adapter)

    g.set_entry_point("init")
    g.add_edge("init", "pick_next")
    g.add_edge("pick_next", "run_adapter")

    g.add_conditional_edges(
        "run_adapter",
        should_continue,
        {
            "continue": "pick_next",
            "done": END,
        },
    )

    return g.compile()

def save_graph_diagram(path: str = "retrieve_graph.mmd") -> None:
    graph = build_retrieve_graph()
    g = graph.get_graph()

    # Mermaid text (works everywhere)
    mermaid = g.draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)

    logger.info(f"[scrape] wrote graph mermaid to {path}")

def run(*, ebay_token: str, adapter_names: Optional[List[str]] = None) -> RetrieveState:
    graph = build_retrieve_graph()

    initial: RetrieveState = {"ebay_token": ebay_token}
    if adapter_names is not None:
        initial["adapter_names"] = adapter_names

    logger.info("[scrape] Begin scrape (LangGraph)")
    out: RetrieveState = graph.invoke(initial, config={"recursion_limit": 200})
    logger.info("[scrape] End scrape (LangGraph)")
    return out
