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

# Which service name from get_all_api_usage_today() this heartbeat cares about.
# e.g. "ebay", "ebay_trading", etc.
API_USAGE_SERVICE = os.getenv("EBAY_USAGE_SERVICE", "ebay")

# This limit applies PER SERVICE (API_USAGE_SERVICE), not across all services.
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
    skip: bool              # kept for backwards-compat logging ("api limit hit")
    api_blocked: bool       # True if API_USAGE_SERVICE over its daily limit
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
    """
    Initialise shared state:

    - Fetch per-service API usage for today.
    - Decide whether API calls for API_USAGE_SERVICE are allowed (api_blocked).
    - Always fetch ebay_token so subgraphs that need it still have it.
    """
    daily_limit = _get_daily_limit()
    raw_usage = get_all_api_usage_today()

    breakdown: Dict[str, int] = {}
    used_today = 0

    # New behaviour: dict of {service: count}
    if isinstance(raw_usage, dict):
        breakdown = {str(k): int(v or 0) for k, v in raw_usage.items()}
        used_today = breakdown.get(API_USAGE_SERVICE, 0)
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

    api_blocked = used_today >= daily_limit

    state["limit_info"] = {
        "used_today": used_today,
        "daily_limit": daily_limit,
        "service": API_USAGE_SERVICE,
        "breakdown": breakdown,
    }
    # 'skip' kept for backwards-compat; now means "API calls are blocked"
    state["skip"] = api_blocked
    state["api_blocked"] = api_blocked

    if api_blocked:
        logger.warning(
            "[main] DAILY API LIMIT REACHED for service %r: used=%d / limit=%d. "
            "API-using subgraphs will be skipped, DB-only work will still run. Breakdown=%r",
            API_USAGE_SERVICE,
            used_today,
            daily_limit,
            breakdown,
        )
    else:
        logger.info(
            "[main] Daily API usage for %r: used=%d / limit=%d – API-using subgraphs allowed. Breakdown=%r",
            API_USAGE_SERVICE,
            used_today,
            daily_limit,
            breakdown,
        )

    # We still fetch the token so any subgraph that needs it can use it.
    state["ebay_token"] = get_auth().get_token()

    return state


# ---------------------------------------------------------------------------
# Subgraph adapter
# ---------------------------------------------------------------------------

def run_subgraph(name: str, build_graph_fn, uses_api: bool = True):
    """
    Wrap a subgraph so we can:

    - Share MainState through it.
    - Optionally skip it entirely when API usage for API_USAGE_SERVICE is blocked.
    """
    def _node(state: MainState) -> MainState:
        if uses_api and state.get("api_blocked"):
            logger.info(
                "[main] -> %s skipped (API daily limit reached for service %r)",
                name,
                API_USAGE_SERVICE,
            )
            state[f"{name}_out"] = {
                "status": "skipped_due_to_api_limit",
                "reason": "api_daily_limit_reached",
                "service": API_USAGE_SERVICE,
            }
            return state

        graph = build_graph_fn()
        logger.info("[main] -> %s begin", name)
        out = graph.invoke(dict(state), config={"recursion_limit": 500})
        state[f"{name}_out"] = out
        logger.info("[main] -> %s end", name)
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

    # Mark which subgraphs actually hit the eBay API.
    # Adjust uses_api=True/False as needed if any of these assumptions change.
    g.add_node("ended",     run_subgraph("ended",     build_ended,     uses_api=False))
    g.add_node("retrieve",  run_subgraph("retrieve",  build_retrieve,  uses_api=True))
    g.add_node("pph",       run_subgraph("pph",       build_pph,       uses_api=False))
    g.add_node("comps",     run_subgraph("comps",     build_comps,     uses_api=True))
    g.add_node("attributes",run_subgraph("attributes",build_attributes,uses_api=True))
    g.add_node("hot",       run_subgraph("hot",       build_hot,       uses_api=False))
    g.add_node("roi",       run_subgraph("roi",       build_roi,       uses_api=False))
    g.add_node("new",       run_subgraph("new",       build_new,       uses_api=False))

    # assess trigger node (no eBay calls; it has its own limits/window)
    g.add_node("assess_trigger", assess_trigger)

    g.set_entry_point("init")

    # Straight chain: init always runs once per heartbeat.
    # API-using nodes may be skipped internally based on api_blocked flag.
    g.add_edge("init", "ended")
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

    info = out.get("limit_info") or {}
    if out.get("api_blocked"):
        logger.info(
            "[main] heartbeat completed with API limit reached for %r: "
            "used=%s / limit=%s. API-using subgraphs were skipped.",
            info.get("service"),
            info.get("used_today"),
            info.get("daily_limit"),
        )
    else:
        logger.info("[main] heartbeat run end")

    return out
