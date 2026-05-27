from __future__ import annotations

import os
import sys
import subprocess
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import TypedDict, Any, Dict, List

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.auth import get_auth
from utils.usage_tracker import get_all_api_usage_today
from utils.db_schema import connection
from utils.pipeline_control import is_heartbeat_paused
from utils.emailer import send_email

from pipelines.listing.ended.graph import build_graph as build_ended
from pipelines.listing.retrieve.graph import build_graph as build_retrieve
from pipelines.listing.pph.graph import build_graph as build_pph
from pipelines.listing.comps.graph import build_graph as build_comps
from pipelines.listing.attributes.graph import build_graph as build_attributes
from pipelines.listing.hot.graph import build_graph as build_hot
from pipelines.listing.roi.graph import build_graph as build_roi
from pipelines.listing.new.graph import build_graph as build_new
from pipelines.listing.project_mini.graph import build_graph as build_project_mini
from pipelines.listing.project_msx125.graph import build_graph as build_project_msx125

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# API usage config
# ---------------------------------------------------------------------------

# Default limits if no env override is provided.
# Based on what you said:
# - trading: 5000/day
# - browse:  5000/day
# - auth:    1000/day
DEFAULT_DAILY_LIMIT = 5000

SERVICE_LIMIT_DEFAULTS: Dict[str, int] = {
    # Trading API
    "ebay_ended_v1": 5000,
    "ebay_attributes_v1": 5000,
    # Browse API
    "ebay_base_fetch_items_by_id_v1": 5000,
    "ebay_base_fetch_category_items_v1": 5000,
    # Auth
    "ebay_auth_v1": 1000,
}


def _limit_env_key(service: str) -> str:
    """
    Turn 'ebay_ended_v1' into 'EBAY_LIMIT_EBAY_ENDED_V1'
    so you can override limits via env, e.g.:
        EBAY_LIMIT_EBAY_BASE_FETCH_ITEMS_BY_ID_V1=4500
    """
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in service.upper())
    return f"EBAY_LIMIT_{cleaned}"


def _get_limit_for_service(service: str) -> int:
    env_key = _limit_env_key(service)
    raw = os.getenv(env_key)
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
            logger.warning(
                "[main] Non-positive %s=%r; falling back to default for %s",
                env_key,
                raw,
                service,
            )
        except ValueError:
            logger.warning(
                "[main] Invalid %s=%r; falling back to default for %s",
                env_key,
                raw,
                service,
            )
    return SERVICE_LIMIT_DEFAULTS.get(service, DEFAULT_DAILY_LIMIT)


# ---------------------------------------------------------------------------
# LLM assess trigger config
# ---------------------------------------------------------------------------
#
# Gemini assessment is intentionally disabled by default for now.
# To re-enable later, set GF_ASSESS_ENABLED=1 in the environment.
ASSESS_ENABLED = os.getenv("GF_ASSESS_ENABLED", "0") == "1"
ASSESS_TZ = ZoneInfo(os.getenv("GF_ASSESS_TZ", "Europe/London"))

# Night-time window for LLM assess. Default: 23:00–07:00 local time.
ASSESS_START_HOUR = int(os.getenv("GF_ASSESS_START_HOUR", "23"))
ASSESS_END_HOUR = int(os.getenv("GF_ASSESS_END_HOUR", "7"))  # wraps past midnight

# Max number of listings for each assess run (passed via env to the CLI)
ASSESS_LIMIT_DEFAULT = int(os.getenv("GF_ASSESS_LIMIT_DEFAULT", "3"))

HEARTBEAT_EMAIL_NAME = "listing_system_hourly_heartbeat"
HEARTBEAT_EMAIL_INTERVAL = timedelta(hours=int(os.getenv("GF_HEARTBEAT_EMAIL_HOURS", "1")))
HEARTBEAT_EMAIL_RECIPIENTS = [
    email.strip()
    for email in os.getenv(
        "GF_HEARTBEAT_EMAIL_RECIPIENTS",
        "garyconstable80@gmail.com,info@ghostfrog.co.uk",
    ).split(",")
    if email.strip()
]


class MainState(TypedDict, total=False):
    # shared inputs
    ebay_token: str

    # limit / skip info
    skip: bool  # kept for backwards-compat logging only
    limit_info: Dict[str, Any]
    blocked_services: List[str]
    maintenance_skip: bool
    maintenance_info: Dict[str, Any]

    # subgraph outputs (namespaced so nothing collides)
    ended_out: Dict[str, Any]
    retrieve_out: Dict[str, Any]
    pph_out: Dict[str, Any]
    comps_out: Dict[str, Any]
    attributes_out: Dict[str, Any]
    hot_out: Dict[str, Any]
    roi_out: Dict[str, Any]
    new_out: Dict[str, Any]
    project_mini_out: Dict[str, Any]
    project_msx125_out: Dict[str, Any]
    heartbeat_email_out: Dict[str, Any]


# ---------------------------------------------------------------------------
# Daily API usage limit (per service)
# ---------------------------------------------------------------------------

def init(state: MainState) -> MainState:
    """
    Initialise shared state:

    - Fetch per-service API usage for today from usage tracker.
    - Compare against per-service limits.
    - Record which services are blocked (over their individual limit).
    - Always fetch ebay_token (the auth API has its own cap).
    """
    paused, pause_info = is_heartbeat_paused()
    if paused:
        state["maintenance_skip"] = True
        state["maintenance_info"] = pause_info or {}
        state["blocked_services"] = []
        state["skip"] = True
        state["limit_info"] = {
            "mode": "maintenance_pause",
            "services": {},
            "raw_breakdown": {},
        }
        logger.warning(
            "[main] heartbeat skipped because maintenance pause is active: %s",
            (pause_info or {}).get("payload", {}).get("reason", "unspecified"),
        )
        return state

    raw_usage = get_all_api_usage_today()

    breakdown: Dict[str, int] = {}
    limit_info_services: Dict[str, Dict[str, Any]] = {}
    blocked_services: List[str] = []

    if isinstance(raw_usage, dict):
        # Normal case: dict of {service: count}
        breakdown = {str(k): int(v or 0) for k, v in raw_usage.items()}
        for service, default_limit in SERVICE_LIMIT_DEFAULTS.items():
            used = breakdown.get(service, 0)
            limit = _get_limit_for_service(service)
            blocked = used >= limit
            if blocked:
                blocked_services.append(service)
            limit_info_services[service] = {
                "used": used,
                "limit": limit,
                "blocked": blocked,
            }

        logger.info(
            "[main] API usage today: %s",
            ", ".join(
                f"{svc}={info['used']}/{info['limit']}"
                for svc, info in limit_info_services.items()
            ),
        )
    else:
        # Backwards-compat: if usage tracker returns a single int, treat it as
        # one generic counter and block ALL known services when over a generic limit.
        try:
            used_total = int(raw_usage or 0)
        except Exception:
            logger.warning(
                "[main] Unexpected usage type from get_all_api_usage_today(): %r",
                type(raw_usage),
            )
            used_total = 0

        generic_limit = DEFAULT_DAILY_LIMIT
        blocked = used_total >= generic_limit
        if blocked:
            blocked_services = list(SERVICE_LIMIT_DEFAULTS.keys())

        for service, default_limit in SERVICE_LIMIT_DEFAULTS.items():
            limit_info_services[service] = {
                "used": used_total,
                "limit": generic_limit,
                "blocked": blocked,
            }

        logger.warning(
            "[main] usage tracker returned non-dict; using generic limit %d with "
            "used_total=%d. All services share this cap.",
            generic_limit,
            used_total,
        )

    state["blocked_services"] = blocked_services
    state["skip"] = bool(blocked_services)  # legacy semantics: "some API is blocked"
    state["limit_info"] = {
        "mode": "per_service" if isinstance(raw_usage, dict) else "generic",
        "services": limit_info_services,
        "raw_breakdown": breakdown,
    }

    if blocked_services:
        logger.warning(
            "[main] services blocked due to daily API limits: %s",
            ", ".join(sorted(blocked_services)),
        )
    else:
        logger.info("[main] no services blocked by daily API limits")

    # We still fetch the token so any subgraph that needs it can use it.
    state["ebay_token"] = get_auth().get_token()

    return state


# ---------------------------------------------------------------------------
# Subgraph adapter with per-service gating
# ---------------------------------------------------------------------------

def run_subgraph(name: str, build_graph_fn, services: List[str] | None = None):
    """
    Wrap a subgraph so we can:

    - Share MainState through it.
    - Optionally skip it when any of its services are blocked.
    """
    services = services or []

    def _node(state: MainState) -> MainState:
        if state.get("maintenance_skip"):
            logger.info("[main] -> %s skipped (maintenance pause active)", name)
            state[f"{name}_out"] = {
                "status": "skipped_due_to_maintenance",
                "reason": "maintenance_pause_active",
                "maintenance_info": state.get("maintenance_info") or {},
            }
            return state

        blocked_services = set(state.get("blocked_services") or [])
        affected = blocked_services.intersection(services)

        if affected:
            logger.info(
                "[main] -> %s skipped (blocked services: %s)",
                name,
                ", ".join(sorted(affected)),
            )
            state[f"{name}_out"] = {
                "status": "skipped_due_to_api_limit",
                "reason": "api_daily_limit_reached",
                "blocked_services": sorted(affected),
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

    if state.get("maintenance_skip"):
        logger.info("[main] assess: skipped due to maintenance pause")
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


def _get_last_heartbeat_email_sent() -> datetime | None:
    with connection.cursor() as cur:
        cur.execute(
            "SELECT last_sent_at FROM alert_state WHERE name = %s LIMIT 1",
            (HEARTBEAT_EMAIL_NAME,),
        )
        row = cur.fetchone()
    connection.commit()
    return row[0] if row else None


def _set_last_heartbeat_email_sent(when: datetime) -> None:
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alert_state (name, last_sent_at)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE
            SET last_sent_at = EXCLUDED.last_sent_at
            """,
            (HEARTBEAT_EMAIL_NAME, when),
        )
    connection.commit()


def _summarise_node(state: MainState, key: str) -> str:
    out = state.get(key)
    if not isinstance(out, dict):
        return "not run"

    status = out.get("status")
    if status:
        return str(status)
    if out.get("results") is not None:
        try:
            return f"ok ({len(out.get('results') or [])} results)"
        except Exception:
            return "ok"
    if out.get("emails_sent") is not None:
        return f"ok ({out.get('emails_sent')} emails)"
    return "ok"


def hourly_status_email(state: MainState) -> MainState:
    if state.get("maintenance_skip"):
        state["heartbeat_email_out"] = {
            "status": "skipped_due_to_maintenance",
            "reason": "maintenance_pause_active",
        }
        return state

    now = datetime.now(timezone.utc)
    last_sent = _get_last_heartbeat_email_sent()
    if last_sent is not None:
        last_sent_aware = last_sent if last_sent.tzinfo else last_sent.replace(tzinfo=timezone.utc)
        if (now - last_sent_aware) < HEARTBEAT_EMAIL_INTERVAL:
            state["heartbeat_email_out"] = {
                "status": "skipped_recently_sent",
                "last_sent_at": last_sent_aware.isoformat(),
            }
            return state

    subject = f"[GhostFrog] Listing pipeline heartbeat OK — {now.strftime('%Y-%m-%d %H:%M UTC')}"
    body = (
        "GhostFrog listing pipeline heartbeat\n\n"
        f"Time: {now.isoformat()}\n"
        f"Blocked services: {', '.join(state.get('blocked_services') or []) or 'none'}\n\n"
        "Pipeline summary\n"
        f"- ended: {_summarise_node(state, 'ended_out')}\n"
        f"- retrieve: {_summarise_node(state, 'retrieve_out')}\n"
        f"- pph: {_summarise_node(state, 'pph_out')}\n"
        f"- comps: {_summarise_node(state, 'comps_out')}\n"
        f"- attributes: {_summarise_node(state, 'attributes_out')}\n"
        f"- hot: {_summarise_node(state, 'hot_out')}\n"
        f"- roi: {_summarise_node(state, 'roi_out')}\n"
        f"- new: {_summarise_node(state, 'new_out')}\n"
        f"- project_mini: {_summarise_node(state, 'project_mini_out')}\n"
        f"- project_msx125: {_summarise_node(state, 'project_msx125_out')}\n"
    )

    sent_to: list[str] = []
    try:
        for recipient in HEARTBEAT_EMAIL_RECIPIENTS:
            send_email(subject=subject, body=body, to_addr=recipient, is_html=False)
            sent_to.append(recipient)

        _set_last_heartbeat_email_sent(now)
        state["heartbeat_email_out"] = {
            "status": "sent",
            "sent_to": sent_to,
            "sent_at": now.isoformat(),
        }
        logger.info("[main] hourly heartbeat email sent to %s", ", ".join(sent_to))
    except Exception as exc:
        state["heartbeat_email_out"] = {
            "status": "failed",
            "error": str(exc),
            "sent_to": sent_to,
        }
        logger.warning("[main] hourly heartbeat email failed: %s", exc)

    return state


# ---------------------------------------------------------------------------
# Build graph
# ---------------------------------------------------------------------------

def build_graph():
    g = StateGraph(MainState)

    # Nodes
    g.add_node("init", init)

    # Map each subgraph to the services it uses.
    # Adjust these lists if you change which APIs each pipeline hits.
    g.add_node(
        "ended",
        run_subgraph(
            "ended",
            build_ended,
            services=["ebay_ended_v1"],
        ),
    )
    g.add_node(
        "retrieve",
        run_subgraph(
            "retrieve",
            build_retrieve,
            services=[
                "ebay_base_fetch_items_by_id_v1",
                "ebay_base_fetch_category_items_v1",
            ],
        ),
    )
    g.add_node(
        "pph",
        run_subgraph(
            "pph",
            build_pph,
            services=[],
        ),
    )
    g.add_node(
        "comps",
        run_subgraph(
            "comps",
            build_comps,
            services=["ebay_base_fetch_items_by_id_v1"],
        ),
    )
    g.add_node(
        "attributes",
        run_subgraph(
            "attributes",
            build_attributes,
            services=[
                "ebay_attributes_v1",
                "ebay_base_fetch_items_by_id_v1",
            ],
        ),
    )
    g.add_node(
        "hot",
        run_subgraph(
            "hot",
            build_hot,
            services=[],
        ),
    )
    g.add_node(
        "roi",
        run_subgraph(
            "roi",
            build_roi,
            services=[],
        ),
    )
    g.add_node(
        "new",
        run_subgraph(
            "new",
            build_new,
            services=[],
        ),
    )
    g.add_node(
        "project_mini",
        run_subgraph(
            "project_mini",
            build_project_mini,
            services=[],
        ),
    )
    g.add_node(
        "project_msx125",
        run_subgraph(
            "project_msx125",
            build_project_msx125,
            services=[],
        ),
    )

    # assess trigger node
    g.add_node("assess_trigger", assess_trigger)
    g.add_node("heartbeat_email", hourly_status_email)

    g.set_entry_point("init")

    # Straight chain; API-using nodes may skip internally.
    g.add_edge("init", "ended")
    g.add_edge("ended", "retrieve")
    g.add_edge("retrieve", "pph")
    g.add_edge("pph", "comps")
    g.add_edge("comps", "attributes")
    g.add_edge("attributes", "hot")
    g.add_edge("hot", "roi")
    g.add_edge("roi", "new")
    g.add_edge("new", "project_mini")
    g.add_edge("project_mini", "project_msx125")

    # After all main work is done, optionally spawn assess job, then finish
    g.add_edge("project_msx125", "assess_trigger")
    g.add_edge("assess_trigger", "heartbeat_email")
    g.add_edge("heartbeat_email", END)

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

    if out.get("maintenance_skip"):
        logger.info("[main] heartbeat run skipped due to maintenance pause")
        return out

    blocked = out.get("blocked_services") or []
    if blocked:
        logger.info(
            "[main] heartbeat completed with some services blocked by API limits: %s",
            ", ".join(sorted(blocked)),
        )
    else:
        logger.info("[main] heartbeat run end")

    return out
