from __future__ import annotations

import os
import sys
import subprocess
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import TypedDict, Any, Dict

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.auth import get_auth
from utils.usage_tracker import get_all_api_usage_today
from utils.db_schema import connection

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
# API usage config
# ---------------------------------------------------------------------------

API_USAGE_SERVICE = os.getenv("EBAY_USAGE_SERVICE", "ebay")
DEFAULT_DAILY_LIMIT = 5000

# ---------------------------------------------------------------------------
# LLM assess trigger config
# ---------------------------------------------------------------------------

ASSESS_ENABLED = os.getenv("GF_ASSESS_ENABLED", "1") == "1"
ASSESS_TZ = ZoneInfo(os.getenv("GF_ASSESS_TZ", "Europe/London"))

# Night-time window for LLM assess. Default: 23:00–07:00 local time.
ASSESS_START_HOUR = int(os.getenv("GF_ASSESS_START_HOUR", "23"))
ASSESS_END_HOUR = int(os.getenv("GF_ASSESS_END_HOUR", "7"))  # wraps past midnight

# Max number of listings for each assess run (passed via env to the CLI)
ASSESS_LIMIT_DEFAULT = int(os.getenv("GF_ASSESS_LIMIT_DEFAULT", "3"))


class MainState(TypedDict, total=False):
    # shared inputs
    ebay_token: str

    # limit / skip info
    skip: bool
    limit_info: Dict[str, int]

    # subgraph outputs (namespaced so nothing collides)
    ended_out: Dict[str, Any]
    retrieve_out: Dict[str, Any]
    pph_out: Dict[str, Any]
    comps_out: Dict[str, Any]
    attributes_out: Dict[str, Any]
    hot_out: Dict[str, Any]
    roi_out: Dict[str, Any]
    new_out: Dict[str, Any]


# ---------------------------------------------------------------------------
# Daily API usage limit
# ---------------------------------------------------------------------------

def _get_daily_limit() -> int:
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
    daily_limit = _get_daily_limit()
    raw_usage = get_all_api_usage_today()

    breakdown: Dict[str, int] = {}

    # New behaviour: dict of {service: count}
    if isinstance(raw_usage, dict):
        breakdown = {str(k): int(v or 0) for k, v in raw_usage.items()}
        used_today = sum(breakdown.values())
    else:
        # Backwards-compatible: plain int
        try:
            used_today = int(raw_usage or 0)
        except Exception:
            logger.warning(
                "[main] Unexpected usage type from get_all_api_usage_today(): %r",
                type(raw_usage),
            )
            used_today = 0

    state["limit_info"] = {
        "used_today": used_today,
        "daily_limit": daily_limit,
        # if you ever want it later, it's here:
        # "breakdown": breakdown,
    }

    if used_today >= daily_limit:
        logger.warning(
            "[main] DAILY API LIMIT REACHED: used=%d / limit=%d. Skipping heartbeat pipeline. Breakdown=%r",
            used_today,
            daily_limit,
            breakdown,
        )
        state["skip"] = True
        return state

    logger.info(
        "[main] Daily API usage: used=%d / limit=%d – proceeding with run. Breakdown=%r",
        used_today,
        daily_limit,
        breakdown,
    )

    state["ebay_token"] = get_auth().get_token()
    state["skip"] = False
    return state


def should_continue_after_init(state: MainState) -> str:
    if state.get("skip"):
        return "skip"
    return "run"


# ---------------------------------------------------------------------------
# Subgraph adapter
# ---------------------------------------------------------------------------

def run_subgraph(name: str, build_graph_fn):
    def _node(state: MainState) -> MainState:
        graph = build_graph_fn()
        logger.info(f"[main] -> {name} begin")
        out = graph.invoke(dict(state), config={"recursion_limit": 500})
        state[f"{name}_out"] = out
        logger.info(f"[main] -> {name} end")
        return state

    return _node


# ---------------------------------------------------------------------------
# LLM assess trigger helpers
# ---------------------------------------------------------------------------

def _is_assess_window(now_utc: datetime) -> bool:
    local = now_utc.astimezone(ASSESS_TZ)
    h = local.hour
    # Default: wrapping window, e.g. 23–7
    if ASSESS_START_HOUR < ASSESS_END_HOUR:
        # Non-wrapping, if someone configures e.g. 1–5
        return ASSESS_START_HOUR <= h < ASSESS_END_HOUR
    # Wrapping (e.g. 23–7)
    return h >= ASSESS_START_HOUR or h < ASSESS_END_HOUR


def _has_assess_candidates() -> bool:
    """
    Cheap existence check: are there any live auctions with no assessment yet?
    This must mirror the WHERE logic in assess/_load_candidates() reasonably closely.
    """
    sql = """
        SELECT 1
        FROM auction_listings AS l
        LEFT JOIN listing_assessments AS a
          ON a.listing_id = l.id
        WHERE l.status = 'live'
          AND COALESCE(l.finalized, FALSE) = FALSE
          AND l.sale_type = 'auction'
          AND a.id IS NULL
        LIMIT 1
    """
    with connection.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    connection.commit()
    return row is not None


def _spawn_assess_subprocess(limit: int) -> None:
    """
    Fire-and-forget: start `python -m pipelines.listing.assess` in a new process,
    with GF_ASSESS_LIMIT in env so it knows how many to do.
    The assess CLI itself will enforce a 'single job at a time' lock.
    """
    env = os.environ.copy()
    env.setdefault("GF_ASSESS_LIMIT", str(limit))

    try:
        subprocess.Popen(
            [sys.executable, "-m", "pipelines.listing.assess"],
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        logger.info(
            "[main] spawned background LLM assess subprocess (limit=%s)", limit
        )
    except Exception as e:
        logger.warning("[main] failed to spawn LLM assess subprocess: %s", e)


def assess_trigger(state: MainState) -> MainState:
    """
    Node at the END of the main graph:
    - Only runs if assess is enabled
    - Only inside the configured night window
    - Only if there are unassessed candidates
    - Spawns a separate process and then returns immediately.
    """
    if not ASSESS_ENABLED:
        return state

    now = datetime.now(timezone.utc)
    if not _is_assess_window(now):
        logger.info("[main] assess: outside allowed window, not spawning")
        return state

    if not _has_assess_candidates():
        logger.info("[main] assess: no unassessed candidates, not spawning")
        return state

    _spawn_assess_subprocess(ASSESS_LIMIT_DEFAULT)
    return state


# ---------------------------------------------------------------------------
# Build graph
# ---------------------------------------------------------------------------

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

    # assess trigger node
    g.add_node("assess_trigger", assess_trigger)

    g.set_entry_point("init")

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

    # After all main work is done, optionally spawn assess job, then finish
    g.add_edge("new", "assess_trigger")
    g.add_edge("assess_trigger", END)

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
            "[main] heartbeat SKIPPED due to daily API limit: used=%s / limit=%s",
            info.get("used_today"),
            info.get("daily_limit"),
        )
    else:
        logger.info("[main] heartbeat run end")

    return out
