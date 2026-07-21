from __future__ import annotations

import os
import sys
import subprocess
from pathlib import Path
from dataclasses import asdict
from html import escape
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
from utils.slack_notifier import send_slack_message, slack_notifications_enabled

from pipelines.listing.ended.graph import build_graph as build_ended
from pipelines.listing.retrieve.graph import build_graph as build_retrieve
from pipelines.listing.pph.graph import build_graph as build_pph
from pipelines.listing.comps.graph import build_graph as build_comps
from pipelines.listing.attributes.graph import build_graph as build_attributes
from pipelines.listing.hot.graph import build_graph as build_hot
from pipelines.listing.roi.graph import build_graph as build_roi
from pipelines.listing.paper_trading.graph import build_graph as build_paper_trading
from pipelines.listing.paper_trading.graph import ensure_tables as ensure_paper_trade_tables
from pipelines.listing.paper_trading.graph import lesson_report as build_paper_lesson_report
from pipelines.listing.new.graph import build_graph as build_new
from pipelines.listing.project_mini.graph import build_graph as build_project_mini
from pipelines.listing.project_msx125.graph import build_graph as build_project_msx125
from pipelines.listing.point_zero.backfill_apply import backfill_checks_today, is_backfill_running
from pipelines.listing.point_zero.backfill_plan import build_plan as build_backfill_plan
from pipelines.listing.market_discovery.graph import run as run_market_discovery
from pipelines.listing.opportunity_digest import (
    build_review_auction_digest,
    format_review_auction_digest_for_slack,
)

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

BACKFILL_ENABLED = os.getenv("GF_BACKFILL_ENABLED", "1") == "1"
BACKFILL_LIMIT_PER_TICK = int(os.getenv("GF_BACKFILL_LIMIT_PER_TICK", "50"))
BACKFILL_DAILY_BUDGET = int(os.getenv("GF_BACKFILL_DAILY_BUDGET", "1000"))
BACKFILL_DAILY_CAP = int(os.getenv("GF_BACKFILL_DAILY_CAP", "5000"))
BACKFILL_SAFETY_RESERVE = int(os.getenv("GF_BACKFILL_SAFETY_RESERVE", "500"))
BACKFILL_INCLUDE_VEHICLE_SOURCES = os.getenv("GF_BACKFILL_INCLUDE_VEHICLE_SOURCES", "0") == "1"

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
HEARTBEAT_SLACK_NAME = "listing_system_hourly_slack"
HEARTBEAT_SLACK_INTERVAL = timedelta(
    minutes=int(os.getenv("GF_HEARTBEAT_SLACK_MINUTES", "60"))
)
HEARTBEAT_SLACK_AUCTION_DIGEST_LIMIT = int(
    os.getenv("GF_HEARTBEAT_SLACK_AUCTION_DIGEST_LIMIT", "4")
)
HEARTBEAT_LAUNCHD_LABEL = os.getenv(
    "GF_HEARTBEAT_LAUNCHD_LABEL",
    "com.ghostfrog.langgraph-ebay.python-heartbeat",
)
HEARTBEAT_SCHEDULE_MINUTES = int(os.getenv("GF_HEARTBEAT_SCHEDULE_MINUTES", "5"))


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
    paper_trading_out: Dict[str, Any]
    new_out: Dict[str, Any]
    project_mini_out: Dict[str, Any]
    project_msx125_out: Dict[str, Any]
    backfill_trigger_out: Dict[str, Any]
    scheduler_status_out: Dict[str, Any]
    heartbeat_email_out: Dict[str, Any]
    heartbeat_slack_out: Dict[str, Any]


# ---------------------------------------------------------------------------
# Daily API usage limit (per service)
# ---------------------------------------------------------------------------

def _heartbeat_scheduler_status(now: datetime) -> Dict[str, Any]:
    """
    Describe how the current heartbeat tick was started for operator alerts.

    The check is intentionally local and deterministic: the installed launchd
    wrapper exports GF_HEARTBEAT_LAUNCHED_BY=launchd before starting Python.
    XPC_SERVICE_NAME is kept only as a fallback because macOS may expose it as
    "0" inside the child process even when launchctl prints the service label.
    Slack can then tell the operator whether the currently executing heartbeat
    came through the expected five-minute LaunchAgent without shelling out or
    exposing any credentials.
    """

    launched_by = os.getenv("GF_HEARTBEAT_LAUNCHED_BY", "")
    xpc_service = os.getenv("XPC_SERVICE_NAME", "")
    running_via_launchd = launched_by == "launchd" or xpc_service == HEARTBEAT_LAUNCHD_LABEL
    observed_label = HEARTBEAT_LAUNCHD_LABEL if launched_by == "launchd" else xpc_service or None
    return {
        "status": "running" if running_via_launchd else "manual",
        "expected_label": HEARTBEAT_LAUNCHD_LABEL,
        "observed_label": observed_label,
        "launched_by": launched_by or None,
        "schedule_minutes": HEARTBEAT_SCHEDULE_MINUTES,
        "checked_at": now.isoformat(),
        "message": (
            f"running via launchd every {HEARTBEAT_SCHEDULE_MINUTES} min"
            if running_via_launchd
            else "heartbeat is running, but this tick was not started by the expected LaunchAgent"
        ),
    }


def init(state: MainState) -> MainState:
    """
    Initialise shared state:

    - Fetch per-service API usage for today from usage tracker.
    - Compare against per-service limits.
    - Record which services are blocked (over their individual limit).
    - Always fetch ebay_token (the auth API has its own cap).
    """
    state["scheduler_status_out"] = _heartbeat_scheduler_status(datetime.now(timezone.utc))

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

    This gate controls whether a separate LLM assessment subprocess is spawned,
    so keep the behavior conservative while reading listing state through the
    normalized projections. It must mirror the WHERE logic in
    assess/_load_candidates() reasonably closely.
    """
    sql = """
        SELECT 1
        FROM listing_identity_v AS identity
        JOIN listing_outcome_v AS outcome
          ON outcome.listing_id = identity.listing_id
        LEFT JOIN listing_assessments AS a
          ON a.listing_id = identity.listing_id
        WHERE outcome.status = 'live'
          AND COALESCE(outcome.finalized, FALSE) = FALSE
          AND identity.sale_type = 'auction'
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


def _spawn_backfill_subprocess(*, limit: int, daily_budget: int) -> int | None:
    """
    Start the targeted final-price backfill worker and return immediately.

    The worker owns the PostgreSQL advisory lock and daily budget enforcement.
    Heartbeat performs only a quick preflight so status ticks stay short.
    """
    env = os.environ.copy()
    log_dir = Path(env.get("GF_LOG_DIR") or os.path.expanduser("~/Library/Logs/ghostfrog-langgraph-ebay"))
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / "point-zero-backfill.log"

    args = [
        sys.executable,
        "-m",
        "pipelines.listing.point_zero.backfill_apply",
        "--apply",
        "--limit",
        str(limit),
        "--daily-cap",
        str(BACKFILL_DAILY_CAP),
        "--safety-reserve",
        str(BACKFILL_SAFETY_RESERVE),
        "--daily-backfill-budget",
        str(daily_budget),
    ]
    if BACKFILL_INCLUDE_VEHICLE_SOURCES:
        args.append("--include-vehicle-sources")

    try:
        log_file = log_path.open("ab")
        process = subprocess.Popen(
            args,
            env=env,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        log_file.close()
        logger.info(
            "[main] spawned background final-price backfill subprocess pid=%s limit=%s daily_budget=%s",
            process.pid,
            limit,
            daily_budget,
        )
        return int(process.pid)
    except Exception as e:
        logger.warning("[main] failed to spawn final-price backfill subprocess: %s", e)
        return None


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


def backfill_trigger(state: MainState) -> MainState:
    """
    Spawn a small, separately locked final-price backfill worker when useful.

    This node deliberately does not call eBay itself. It only checks whether a
    worker is already active, whether today's dedicated backfill budget remains,
    and whether the planner can see at least one useful stale candidate.
    """
    if not BACKFILL_ENABLED:
        state["backfill_trigger_out"] = {"status": "disabled"}
        return state

    if state.get("maintenance_skip"):
        state["backfill_trigger_out"] = {"status": "skipped_due_to_maintenance"}
        return state

    if "ebay_ended_v1" in set(state.get("blocked_services") or []):
        state["backfill_trigger_out"] = {"status": "skipped_api_limit_blocked"}
        return state

    if is_backfill_running():
        state["backfill_trigger_out"] = {"status": "skipped_already_running"}
        return state

    checked_today = backfill_checks_today()
    remaining_budget = max(BACKFILL_DAILY_BUDGET - checked_today, 0)
    if remaining_budget <= 0:
        state["backfill_trigger_out"] = {
            "status": "skipped_daily_budget_exhausted",
            "checked_today": checked_today,
            "daily_budget": BACKFILL_DAILY_BUDGET,
        }
        return state

    limit = max(min(BACKFILL_LIMIT_PER_TICK, remaining_budget), 0)
    if limit <= 0:
        state["backfill_trigger_out"] = {"status": "skipped_zero_limit"}
        return state

    try:
        plan = build_backfill_plan(
            daily_cap=BACKFILL_DAILY_CAP,
            safety_reserve=BACKFILL_SAFETY_RESERVE,
            max_backfill_per_day=limit,
            candidate_detail_limit=1,
            include_vehicle_sources=BACKFILL_INCLUDE_VEHICLE_SOURCES,
        )
    except Exception as exc:
        logger.warning("[main] backfill trigger plan failed: %s", exc)
        state["backfill_trigger_out"] = {"status": "failed_plan", "error": str(exc)}
        return state

    api_budget = plan.get("api_budget") or {}
    recommended_calls = int(api_budget.get("recommended_backfill_calls") or 0)
    candidates = plan.get("recommended_stale_candidate_preview") or []
    if recommended_calls <= 0:
        state["backfill_trigger_out"] = {
            "status": "skipped_no_api_budget",
            "checked_today": checked_today,
            "daily_budget": BACKFILL_DAILY_BUDGET,
        }
        return state
    if not candidates:
        state["backfill_trigger_out"] = {
            "status": "skipped_no_candidates",
            "checked_today": checked_today,
            "daily_budget": BACKFILL_DAILY_BUDGET,
            "recommended_calls": recommended_calls,
        }
        return state

    spawn_limit = min(limit, recommended_calls)
    pid = _spawn_backfill_subprocess(limit=spawn_limit, daily_budget=BACKFILL_DAILY_BUDGET)
    state["backfill_trigger_out"] = {
        "status": "spawned" if pid is not None else "failed_spawn",
        "pid": pid,
        "limit": spawn_limit,
        "checked_today": checked_today,
        "daily_budget": BACKFILL_DAILY_BUDGET,
        "remaining_budget": remaining_budget,
    }
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


def _get_last_heartbeat_slack_sent() -> datetime | None:
    with connection.cursor() as cur:
        cur.execute(
            "SELECT last_sent_at FROM alert_state WHERE name = %s LIMIT 1",
            (HEARTBEAT_SLACK_NAME,),
        )
        row = cur.fetchone()
    connection.commit()
    return row[0] if row else None


def _set_last_heartbeat_slack_sent(when: datetime) -> None:
    with connection.cursor() as cur:
        cur.execute(
            """
            INSERT INTO alert_state (name, last_sent_at)
            VALUES (%s, %s)
            ON CONFLICT (name) DO UPDATE
            SET last_sent_at = EXCLUDED.last_sent_at
            """,
            (HEARTBEAT_SLACK_NAME, when),
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


def _fmt_int(value: Any) -> str:
    try:
        return f"{int(value or 0):,}"
    except Exception:
        return "0"


def _fmt_pct(value: Any) -> str:
    try:
        return f"{float(value or 0):.0f}%"
    except Exception:
        return "0%"


def _fmt_num(value: Any) -> str:
    try:
        return f"{float(value or 0):.2f}"
    except Exception:
        return "0.00"


def _fmt_dt(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(ASSESS_TZ).strftime("%d %b %Y %H:%M")
    return str(value)


def _query_one(sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    with connection.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        if not row:
            return {}
        names = [desc[0] for desc in cur.description]
    connection.commit()
    return dict(zip(names, row))


def _query_all(sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with connection.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall() or []
        names = [desc[0] for desc in cur.description]
    connection.commit()
    return [dict(zip(names, row)) for row in rows]


def _heartbeat_metrics() -> dict[str, Any]:
    """Build read-only operator heartbeat metrics from normalized projections."""
    listing_counts = _query_one(
        """
        SELECT
            COUNT(*)::int AS total,
            COUNT(*) FILTER (WHERE o.status = 'live')::int AS live,
            COUNT(*) FILTER (WHERE o.status = 'sold')::int AS sold,
            COUNT(*) FILTER (WHERE o.status = 'ended')::int AS ended,
            COUNT(*) FILTER (WHERE o.status = 'stale')::int AS stale,
            COUNT(*) FILTER (WHERE o.status = 'parse_failed')::int AS parse_failed,
            COUNT(*) FILTER (WHERE o.status = 'stale' AND COALESCE(o.finalized, FALSE) = FALSE)::int AS stale_unfinalized,
            COUNT(*) FILTER (WHERE o.final_price IS NOT NULL)::int AS final_prices,
            COUNT(*) FILTER (WHERE a.model_key IS NULL OR a.model_key = '' OR LOWER(a.model_key) = 'unknown')::int AS missing_model_key
        FROM listing_outcome_v AS o
        JOIN listing_attribute_v AS a
          ON a.listing_id = o.listing_id
        """
    )
    comps = _query_one(
        """
        SELECT
            COUNT(*)::int AS comp_keys,
            COALESCE(SUM(samples), 0)::int AS comp_samples,
            COALESCE(COUNT(*) FILTER (WHERE samples >= 3), 0)::int AS comp_keys_3plus,
            MAX(computed_at) AS computed_at
        FROM latest_comps
        """
    )
    live_comp_coverage = _query_one(
        """
        SELECT
            COUNT(*)::int AS live_total,
            COUNT(*) FILTER (WHERE c.model_key IS NOT NULL)::int AS live_with_comps,
            COUNT(*) FILTER (WHERE c.samples >= 3)::int AS live_with_3plus_comps,
            COUNT(*) FILTER (WHERE a.model_key IS NULL OR a.model_key = '' OR LOWER(a.model_key) = 'unknown')::int AS live_unknown_model_key
        FROM listing_outcome_v AS o
        JOIN listing_attribute_v AS a
          ON a.listing_id = o.listing_id
        LEFT JOIN latest_comps c ON c.model_key = a.model_key
        WHERE o.status = 'live'
        """
    )
    api_today = _query_all(
        """
        SELECT service, call_count::int AS call_count
        FROM api_usage
        WHERE date = CURRENT_DATE
        ORDER BY call_count DESC, service
        """
    )
    api_total = sum(int(row.get("call_count") or 0) for row in api_today)
    source_freshness = _query_all(
        """
        SELECT
            domain,
            last_scraped_at,
            GREATEST(EXTRACT(EPOCH FROM (now() - last_scraped_at))::int, 0) AS age_seconds
        FROM sources
        WHERE enabled = TRUE
          AND domain IS NOT NULL
          AND domain <> ''
        ORDER BY last_scraped_at NULLS FIRST
        LIMIT 8
        """
    )
    high_risk_roi = _query_one(
        """
        SELECT
            COUNT(*) FILTER (WHERE o.status = 'live' AND s.roi_estimate > 0.15)::int AS live_roi_gt_15,
            COUNT(*) FILTER (WHERE o.status = 'live' AND s.roi_estimate > 1.0)::int AS live_roi_gt_100,
            COUNT(*) FILTER (WHERE o.status = 'live' AND s.roi_estimate > 10.0)::int AS live_roi_gt_1000
        FROM listing_outcome_v AS o
        JOIN listing_score_v AS s
          ON s.listing_id = o.listing_id
        """
    )
    paper_table = _query_one(
        "SELECT to_regclass('public.paper_trade_predictions') IS NOT NULL AS exists"
    )
    if paper_table.get("exists"):
        with connection.cursor() as cur:
            ensure_paper_trade_tables(cur)
        connection.commit()
        try:
            paper_lessons = build_paper_lesson_report(limit=6)
        except Exception as exc:
            paper_lessons = {
                "lesson_counts": {"report_error": 1},
                "lessons": [
                    {
                        "source": "n/a",
                        "title": "Paper-trading lesson report failed",
                        "lesson_category": "report_error",
                        "learning_hint": str(exc),
                    }
                ],
            }
        paper_trading = _query_one(
            """
            SELECT
                COUNT(*)::int AS total_predictions,
                COUNT(*) FILTER (WHERE decision_status = 'watching')::int AS watching,
                COUNT(*) FILTER (
                    WHERE decision_status = 'watching'
                      AND end_time IS NOT NULL
                      AND end_time <= CURRENT_TIMESTAMP
                )::int AS watching_past_end,
                COUNT(*) FILTER (
                    WHERE outcome_checked_at::date = CURRENT_DATE
                )::int AS checked_today,
                COUNT(*) FILTER (
                    WHERE decision_status = 'closed'
                      AND observed_at::date = CURRENT_DATE
                )::int AS closed_today,
                COUNT(*) FILTER (WHERE decision_status = 'closed')::int AS closed,
                COUNT(*) FILTER (WHERE observed_status = 'sold')::int AS sold,
                COUNT(*) FILTER (
                    WHERE observed_status = 'sold'
                      AND observed_paper_profit > 0
                )::int AS profitable_on_paper,
                COUNT(*) FILTER (WHERE would_have_won IS TRUE)::int AS would_have_won,
                COUNT(*) FILTER (
                    WHERE outcome_reason = 'final_price_above_max_bid'
                )::int AS outbid_above_ceiling,
                AVG(observed_price_error) FILTER (
                    WHERE observed_price_error IS NOT NULL
                )::numeric AS avg_price_error,
                AVG(observed_paper_roi) FILTER (
                    WHERE observed_paper_roi IS NOT NULL
                )::numeric AS avg_observed_roi
            FROM paper_trade_predictions
            """
        )
        paper_by_source = _query_all(
            """
            SELECT source, COUNT(*)::int AS predictions
            FROM paper_trade_predictions
            WHERE prediction_date = CURRENT_DATE
            GROUP BY source
            ORDER BY predictions DESC, source
            LIMIT 8
            """
        )
    else:
        paper_trading = {
            "total_predictions": 0,
            "watching": 0,
            "watching_past_end": 0,
            "checked_today": 0,
            "closed_today": 0,
            "closed": 0,
            "sold": 0,
            "profitable_on_paper": 0,
            "would_have_won": 0,
            "outbid_above_ceiling": 0,
            "avg_price_error": None,
            "avg_observed_roi": None,
        }
        paper_by_source = []
        paper_lessons = {"lesson_counts": {}, "lessons": []}

    try:
        backfill_plan = build_backfill_plan(
            daily_cap=5000,
            safety_reserve=500,
            max_backfill_per_day=1000,
            candidate_detail_limit=3,
            include_vehicle_sources=False,
        )
    except Exception as exc:
        backfill_plan = {"error": str(exc)}

    backfill_activity = _query_one(
        """
        SELECT
            COUNT(*) FILTER (
                WHERE backfill_checked_at::date = CURRENT_DATE
            )::int AS checked_today,
            COUNT(*) FILTER (
                WHERE backfill_checked_at >= CURRENT_TIMESTAMP - interval '24 hours'
            )::int AS checked_24h,
            COUNT(*) FILTER (
                WHERE backfill_checked_at::date = CURRENT_DATE
                  AND finalized IS TRUE
                  AND status IN ('sold', 'ended')
            )::int AS finalized_today,
            COUNT(*) FILTER (
                WHERE backfill_checked_at::date = CURRENT_DATE
                  AND status = 'sold'
            )::int AS sold_today,
            MAX(backfill_checked_at) AS latest_checked_at
        FROM listing_backfill_audit_v
        WHERE backfill_checked_at IS NOT NULL
        """
    )
    backfill_reason_counts = _query_all(
        """
        SELECT
            COALESCE(backfill_last_reason, 'unknown') AS reason,
            COUNT(*)::int AS checks
        FROM listing_backfill_audit_v
        WHERE backfill_checked_at::date = CURRENT_DATE
        GROUP BY COALESCE(backfill_last_reason, 'unknown')
        ORDER BY checks DESC, reason
        LIMIT 8
        """
    )
    try:
        backfill_worker_running = is_backfill_running()
    except Exception:
        backfill_worker_running = None

    try:
        market_discovery = run_market_discovery(limit=6, min_score=20.0)
    except Exception as exc:
        market_discovery = {"error": str(exc), "top_lanes": [], "status_counts": {}}

    try:
        review_auctions = build_review_auction_digest(
            limit=HEARTBEAT_SLACK_AUCTION_DIGEST_LIMIT
        )
    except Exception as exc:
        review_auctions = {"error": str(exc), "items": [], "count": 0}

    return {
        "listing_counts": listing_counts,
        "comps": comps,
        "live_comp_coverage": live_comp_coverage,
        "api_today": api_today,
        "api_total": api_total,
        "source_freshness": source_freshness,
        "high_risk_roi": high_risk_roi,
        "paper_trading": paper_trading,
        "paper_by_source": paper_by_source,
        "paper_lessons": paper_lessons,
        "backfill_plan": backfill_plan,
        "backfill_activity": backfill_activity,
        "backfill_reason_counts": backfill_reason_counts,
        "backfill_worker_running": backfill_worker_running,
        "market_discovery": market_discovery,
        "review_auctions": review_auctions,
    }


def _status_badge(label: str, tone: str = "ok") -> str:
    colors = {
        "ok": ("#dcfce7", "#166534"),
        "warn": ("#fef3c7", "#92400e"),
        "bad": ("#fee2e2", "#991b1b"),
        "muted": ("#e5e7eb", "#374151"),
    }
    bg, fg = colors.get(tone, colors["muted"])
    return (
        f'<span style="display:inline-block;padding:4px 9px;border-radius:999px;'
        f'background:{bg};color:{fg};font-size:12px;font-weight:700;">'
        f'{escape(label)}</span>'
    )


def _metric_card(label: str, value: str, detail: str = "", tone: str = "neutral") -> str:
    border = {
        "good": "#22c55e",
        "warn": "#f59e0b",
        "bad": "#ef4444",
        "neutral": "#0ea5e9",
    }.get(tone, "#0ea5e9")
    return (
        '<td style="width:25%;padding:8px;vertical-align:top;">'
        f'<div style="border:1px solid #dbeafe;border-left:5px solid {border};'
        'border-radius:12px;background:#ffffff;padding:14px;min-height:86px;">'
        f'<div style="font-size:12px;letter-spacing:.04em;text-transform:uppercase;'
        f'color:#64748b;font-weight:700;">{escape(label)}</div>'
        f'<div style="font-size:26px;line-height:1.2;color:#0f172a;font-weight:800;'
        f'margin-top:8px;">{escape(value)}</div>'
        f'<div style="font-size:13px;line-height:1.35;color:#475569;margin-top:6px;">'
        f'{escape(detail)}</div>'
        '</div></td>'
    )


def _section(title: str, body: str) -> str:
    return (
        '<div style="background:#ffffff;border:1px solid #e2e8f0;border-radius:14px;'
        'padding:18px;margin:14px 0;">'
        f'<h2 style="margin:0 0 12px 0;color:#0f172a;font-size:18px;line-height:1.25;">'
        f'{escape(title)}</h2>'
        f'{body}</div>'
    )


def _table(headers: list[str], rows: list[list[str]]) -> str:
    head_html = "".join(
        f'<th style="text-align:left;padding:9px 10px;border-bottom:1px solid #cbd5e1;'
        f'font-size:12px;color:#475569;text-transform:uppercase;letter-spacing:.04em;">'
        f'{escape(h)}</th>'
        for h in headers
    )
    row_html = ""
    for row in rows:
        row_html += "<tr>" + "".join(
            f'<td style="padding:10px;border-bottom:1px solid #eef2f7;'
            f'font-size:13px;color:#334155;vertical-align:top;">{cell}</td>'
            for cell in row
        ) + "</tr>"
    return (
        '<table role="presentation" cellpadding="0" cellspacing="0" width="100%" '
        'style="border-collapse:collapse;">'
        f'<thead><tr>{head_html}</tr></thead><tbody>{row_html}</tbody></table>'
    )


def _email_link(title: Any, url: Any, *, max_chars: int = 96) -> str:
    """Render a safe HTML link for marketplace rows in operator email."""

    cleaned_title = " ".join(str(title or "").split())
    if len(cleaned_title) > max_chars:
        cleaned_title = f"{cleaned_title[: max_chars - 3].rstrip()}..."
    clean_url = str(url or "").strip()
    label = escape(cleaned_title or "Untitled listing")
    if not clean_url:
        return label
    return f'<a href="{escape(clean_url)}" style="color:#0369a1;text-decoration:none;">{label}</a>'


def _pipeline_table(state: MainState) -> str:
    keys = [
        ("Ended", "ended_out"),
        ("Retrieve", "retrieve_out"),
        ("PPH", "pph_out"),
        ("Comps", "comps_out"),
        ("Attributes", "attributes_out"),
        ("Hot", "hot_out"),
        ("ROI", "roi_out"),
        ("Paper Trading", "paper_trading_out"),
        ("New", "new_out"),
        ("Project Mini", "project_mini_out"),
        ("Project MSX125", "project_msx125_out"),
        ("Backfill Spawn", "backfill_trigger_out"),
    ]
    rows: list[list[str]] = []
    for label, key in keys:
        out = state.get(key)
        status = _summarise_node(state, key)
        tone = "ok" if status.startswith("ok") or status in {"done", "sent"} else "muted"
        detail = ""
        if isinstance(out, dict):
            details = []
            nested_result = out.get("result") if isinstance(out.get("result"), dict) else {}
            if nested_result:
                for field in ("snapshots",):
                    if field in nested_result:
                        details.append(f"{field}: {_fmt_int(nested_result.get(field))}")
                totals = nested_result.get("totals") if isinstance(nested_result.get("totals"), dict) else {}
                if totals:
                    details.append(f"watching: {_fmt_int(totals.get('watching'))}")
                    details.append(f"closed: {_fmt_int(totals.get('closed'))}")
            for field in ("candidates", "processed", "applied", "skipped", "errors", "emails_sent", "limit", "checked_today"):
                if field in out:
                    details.append(f"{field}: {_fmt_int(out.get(field))}")
            if out.get("results") is not None:
                try:
                    details.append(f"results: {_fmt_int(len(out.get('results') or []))}")
                except Exception:
                    pass
            detail = ", ".join(details) or "completed"
        rows.append([escape(label), _status_badge(status, tone), escape(detail or "n/a")])
    return _table(["Stage", "Status", "Detail"], rows)


def _slack_field(label: str, value: str) -> dict[str, str]:
    """Build a compact Slack mrkdwn field."""

    return {
        "type": "mrkdwn",
        "text": f"*{label}*\n{value}",
    }


def _market_discovery_slack_summary(market: dict[str, Any]) -> tuple[str, str]:
    """
    Build a safe Slack summary for read-only market discovery output.

    The wording is advisory and review-only: it reports lane status and a next
    review step, but never emits buy/bid/action labels.
    """

    if market.get("error"):
        return "Report failed", "Check local logs before using discovery output."

    counts = market.get("status_counts") or {}
    top_lanes = market.get("top_lanes") or []
    top_research = next(
        (lane for lane in top_lanes if lane.get("status") == "research"),
        None,
    )
    counts_text = (
        f"{_fmt_int(counts.get('research'))} research, "
        f"{_fmt_int(counts.get('parked'))} parked, "
        f"{_fmt_int(counts.get('rejected'))} rejected, "
        f"{_fmt_int(counts.get('paper_trade'))} paper-trade ready"
    )

    if top_research:
        top_text = (
            f"{top_research.get('source')} / {top_research.get('model_key')} "
            f"(score {_fmt_num(top_research.get('score'))}, "
            f"spread £{_fmt_num(top_research.get('estimated_net_spread'))})"
        )
    else:
        top_text = "No research lane above threshold yet."

    next_step = "Review the top lane and wait for paper-trade outcomes before loosening gates."
    return counts_text, f"{top_text}\n{next_step}"


def _pipeline_slack_summary(state: MainState) -> str:
    """
    Summarise the current LangGraph pipeline run for Slack.

    The summary is intentionally operational, not commercial: it exposes stage
    health and paper-trading learning state without producing buy instructions.
    """

    keys = [
        ("Ended", "ended_out"),
        ("Retrieve", "retrieve_out"),
        ("Comps", "comps_out"),
        ("Attributes", "attributes_out"),
        ("ROI", "roi_out"),
        ("Paper", "paper_trading_out"),
        ("New", "new_out"),
        ("Mini", "project_mini_out"),
        ("MSX125", "project_msx125_out"),
    ]
    parts = [f"{label}: {_summarise_node(state, key)}" for label, key in keys]
    return " | ".join(parts)


def _build_heartbeat_slack_message(
    state: MainState,
    metrics: dict[str, Any],
    now: datetime,
) -> tuple[str, list[dict[str, Any]]]:
    """
    Build a concise Slack heartbeat from the same metrics as the email report.

    Slack receives a short operator-facing status update. Detailed audit data
    remains in PostgreSQL and the existing heartbeat email.
    """

    counts = metrics["listing_counts"]
    comps = metrics["comps"]
    coverage = metrics["live_comp_coverage"]
    paper = metrics["paper_trading"]
    market = metrics.get("market_discovery") or {}
    review_auctions = metrics.get("review_auctions") or {}
    api_total = int(metrics["api_total"] or 0)
    blocked = state.get("blocked_services") or []
    scheduler = state.get("scheduler_status_out") or _heartbeat_scheduler_status(now)
    scheduler_status = str(scheduler.get("status") or "unknown").title()
    scheduler_message = str(scheduler.get("message") or "scheduler status unknown")

    live_total = int(coverage.get("live_total") or 0)
    live_with_3plus = int(coverage.get("live_with_3plus_comps") or 0)
    coverage_pct = (live_with_3plus / live_total * 100.0) if live_total else 0.0
    status = "Blocked" if blocked else "Running"
    lead = (
        f"*GhostFrog Listing Pipeline* - {status}\n"
        f"{now.astimezone(ASSESS_TZ).strftime('%d %b %Y %H:%M %Z')}"
    )
    if blocked:
        lead += f"\nBlocked services: {', '.join(blocked)}"

    text = (
        f"GhostFrog listing heartbeat: {status}. "
        f"Scheduler {scheduler_status.lower()}: {scheduler_message}. "
        f"{_fmt_int(counts.get('live'))} live listings, "
        f"{_fmt_int(paper.get('watching'))} paper trades watching, "
        f"{_fmt_int(api_total)} API calls today."
    )
    market_counts_text, market_next_text = _market_discovery_slack_summary(market)

    blocks: list[dict[str, Any]] = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": lead,
            },
        },
        {
            "type": "section",
            "fields": [
                _slack_field("Live listings", _fmt_int(counts.get("live"))),
                _slack_field("API calls today", _fmt_int(api_total)),
                _slack_field(
                    "Confirmed comp keys",
                    f"{_fmt_int(comps.get('comp_keys'))} ({_fmt_int(comps.get('comp_samples'))} samples)",
                ),
                _slack_field(
                    "Live 3+ comp coverage",
                    f"{_fmt_pct(coverage_pct)} ({_fmt_int(live_with_3plus)}/{_fmt_int(live_total)})",
                ),
                _slack_field("Scheduler", f"{scheduler_status}: {scheduler_message}"),
                _slack_field(
                    "Paper trades",
                    f"{_fmt_int(paper.get('watching'))} watching / {_fmt_int(paper.get('closed'))} closed",
                ),
                _slack_field(
                    "Paper outcomes",
                    f"{_fmt_int(paper.get('sold'))} sold / {_fmt_int(paper.get('profitable_on_paper'))} profitable",
                ),
            ],
        },
        {
            "type": "section",
            "fields": [
                _slack_field("Market discovery", market_counts_text),
                _slack_field("Review next", market_next_text),
            ],
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": format_review_auction_digest_for_slack(review_auctions),
            },
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Pipeline stages*\n{_pipeline_slack_summary(state)}",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": "Observer only: no bids, buys, seller messages, relists, or marketplace writes are made from Slack.",
                }
            ],
        },
    ]
    return text, blocks


def _build_heartbeat_email_html(state: MainState, metrics: dict[str, Any], now: datetime) -> str:
    counts = metrics["listing_counts"]
    comps = metrics["comps"]
    coverage = metrics["live_comp_coverage"]
    api_total = int(metrics["api_total"] or 0)
    blocked = state.get("blocked_services") or []
    backfill = metrics.get("backfill_plan") or {}
    backfill_activity = metrics.get("backfill_activity") or {}
    backfill_reasons = metrics.get("backfill_reason_counts") or []
    backfill_trigger_out = state.get("backfill_trigger_out") or {}
    backfill_worker_running = metrics.get("backfill_worker_running")
    review_auctions = metrics.get("review_auctions") or {}
    api_budget = (backfill.get("api_budget") or {}) if isinstance(backfill, dict) else {}
    roi = metrics["high_risk_roi"]
    paper = metrics["paper_trading"]

    stale_unfinalized = int(counts.get("stale_unfinalized") or 0)
    live_total = int(coverage.get("live_total") or 0)
    live_with_3plus = int(coverage.get("live_with_3plus_comps") or 0)
    coverage_pct = (live_with_3plus / live_total * 100.0) if live_total else 0.0

    health_tone = "ok" if not blocked else "bad"
    health_label = "Healthy" if not blocked else "Blocked"
    if stale_unfinalized > 0 or coverage_pct < 25:
        health_tone = "warn" if not blocked else "bad"
        health_label = "Needs attention" if not blocked else "Blocked"

    api_rows = [
        [
            escape(str(row.get("service") or "")),
            escape(_fmt_int(row.get("call_count"))),
            escape(_fmt_pct((int(row.get("call_count") or 0) / 5000) * 100)),
        ]
        for row in metrics["api_today"][:8]
    ]
    source_rows = [
        [
            escape(str(row.get("domain") or "n/a")),
            escape(_fmt_dt(row.get("last_scraped_at"))),
            escape(
                "n/a"
                if row.get("age_seconds") is None
                else f"{int((row.get('age_seconds') or 0) / 60):,} min"
            ),
        ]
        for row in metrics["source_freshness"]
    ]
    listing_rows = [
        [escape("All tracked listings"), escape(_fmt_int(counts.get("total")))],
        [escape("Live listings"), escape(_fmt_int(counts.get("live")))],
        [escape("Sold with confirmed final_price"), escape(_fmt_int(counts.get("sold")))],
        [escape("Ended without final_price"), escape(_fmt_int(counts.get("ended")))],
        [escape("Stale listings"), escape(_fmt_int(counts.get("stale")))],
        [escape("Stale and not finalized"), escape(_fmt_int(stale_unfinalized))],
        [escape("Rows with final_price"), escape(_fmt_int(counts.get("final_prices")))],
        [escape("Unknown or missing model_key"), escape(_fmt_int(counts.get("missing_model_key")))],
    ]
    backfill_source_rows = []
    if isinstance(backfill, dict):
        for row in (backfill.get("stale_backfill_source_summary") or [])[:8]:
            backfill_source_rows.append([
                escape(str(row.get("source") or "")),
                escape(_fmt_int(row.get("model_keys_needing_comps"))),
                escape(_fmt_int(row.get("live_listings_needing_comps"))),
                escape(_fmt_int(row.get("stale_backfill_pool"))),
                escape(_fmt_dt(row.get("newest_stale"))),
            ])
    if not backfill_source_rows:
        backfill_source_rows = [["n/a", "0", "0", "0", "n/a"]]
    unknown_model_rows = []
    if isinstance(backfill, dict):
        for row in (backfill.get("unknown_model_key_work") or [])[:8]:
            unknown_model_rows.append([
                escape(str(row.get("source") or "")),
                escape(_fmt_int(row.get("live_unknown_model_key_count"))),
            ])
    if not unknown_model_rows:
        unknown_model_rows = [["n/a", "0"]]
    candidate_rows = []
    if isinstance(backfill, dict):
        for row in backfill.get("recommended_stale_candidate_preview") or []:
            candidate_rows.append([
                escape(str(row.get("source") or "")),
                escape(str(row.get("model_key") or "")),
                escape(str(row.get("title") or "")[:95]),
                f"&pound;{float(row.get('price_current') or 0):.2f}",
            ])
    if not candidate_rows:
        candidate_rows = [["n/a", "n/a", escape("No safe preview candidates or planner failed."), "n/a"]]

    live_review_rows = []
    for item in (review_auctions.get("items") or [])[:6]:
        notes = ", ".join(item.get("risk_notes") or ["manual review required"])
        live_review_rows.append([
            escape(str(item.get("source") or "")),
            _email_link(item.get("title"), item.get("url")),
            f"&pound;{float(item.get('current_price') or 0):.2f}",
            f"&pound;{float(item.get('estimated_profit') or 0):.2f}",
            escape(_fmt_int(item.get("comp_samples"))),
            escape(str(item.get("ends") or "n/a")),
            escape(notes[:140]),
        ])
    if not live_review_rows:
        live_review_rows = [[
            "n/a",
            escape("No fresh profit-evidence rows right now."),
            "n/a",
            "n/a",
            "0",
            "n/a",
            escape("Keep collecting; the digest only shows recently seen live listings."),
        ]]

    scout_rows = []
    for scout in (review_auctions.get("scout_items") or [])[:6]:
        samples = scout.get("comp_samples")
        scout_rows.append([
            escape(str(scout.get("source") or "")),
            _email_link(scout.get("title"), scout.get("url")),
            f"&pound;{float(scout.get('current_price') or 0):.2f}",
            escape("no comps" if samples is None else f"{int(samples or 0)} comps"),
            escape(str(scout.get("ends") or "n/a")),
            escape(str(scout.get("reason") or "manual scout")[:120]),
        ])
    if not scout_rows:
        scout_rows = [[
            "n/a",
            escape("No fresh scout rows right now."),
            "n/a",
            "n/a",
            "n/a",
            escape("Scout rows require recent live evidence too."),
        ]]

    paper_rows = [
        [escape("Total predictions"), escape(_fmt_int(paper.get("total_predictions")))],
        [escape("Watching"), escape(_fmt_int(paper.get("watching")))],
        [escape("Watching past end time"), escape(_fmt_int(paper.get("watching_past_end")))],
        [escape("Outcome checks today"), escape(_fmt_int(paper.get("checked_today")))],
        [escape("Closed"), escape(_fmt_int(paper.get("closed")))],
        [escape("Closed today"), escape(_fmt_int(paper.get("closed_today")))],
        [escape("Sold with observed final price"), escape(_fmt_int(paper.get("sold")))],
        [escape("Would have won under max bid"), escape(_fmt_int(paper.get("would_have_won")))],
        [escape("Closed above max bid"), escape(_fmt_int(paper.get("outbid_above_ceiling")))],
        [escape("Profitable on paper"), escape(_fmt_int(paper.get("profitable_on_paper")))],
        [escape("Average purchase vs max bid"), escape("n/a" if paper.get("avg_price_error") is None else f"GBP {float(paper.get('avg_price_error') or 0):.2f}")],
        [escape("Average observed ROI"), escape("n/a" if paper.get("avg_observed_roi") is None else _fmt_pct(float(paper.get("avg_observed_roi") or 0) * 100))],
    ]
    paper_source_rows = [
        [escape(str(row.get("source") or "")), escape(_fmt_int(row.get("predictions")))]
        for row in metrics.get("paper_by_source", [])
    ]
    if not paper_source_rows:
        paper_source_rows = [["n/a", "0"]]
    paper_lesson_report = metrics.get("paper_lessons") or {}
    paper_lesson_counts = paper_lesson_report.get("lesson_counts") or {}
    paper_lesson_rows = [
        [
            escape(str(lesson.get("source") or "")),
            escape(str(lesson.get("lesson_category") or "")),
            escape(str(lesson.get("title") or "")[:72]),
            escape(str(lesson.get("learning_hint") or "")[:140]),
        ]
        for lesson in (paper_lesson_report.get("lessons") or [])[:6]
    ]
    if not paper_lesson_rows:
        paper_lesson_rows = [[
            "n/a",
            "waiting",
            escape("No paper-trade lessons yet"),
            escape("The loop needs closed predictions before it can produce miss evidence."),
        ]]
    paper_lesson_count_rows = [
        [escape(str(category)), escape(_fmt_int(count))]
        for category, count in sorted(paper_lesson_counts.items())
    ]
    if not paper_lesson_count_rows:
        paper_lesson_count_rows = [["waiting", "0"]]

    normal_calls = int(api_budget.get("planned_normal_calls") or 0)
    spare_calls = int(api_budget.get("theoretical_spare") or 0)
    recommended_backfill = int(api_budget.get("recommended_backfill_calls") or 0)
    daily_cap = int(api_budget.get("daily_cap") or 5000)
    reserve = int(api_budget.get("safety_reserve") or 500)
    today_calls = int(api_budget.get("today_calls") or api_total)
    avg_calls = int(api_budget.get("avg_total_7d") or 0)
    max_calls = int(api_budget.get("max_total_7d") or 0)
    excluded_sources = ", ".join((backfill.get("excluded_backfill_sources") or [])[:6]) if isinstance(backfill, dict) else ""
    trigger_status = str(backfill_trigger_out.get("status") or "not run")
    worker_text = (
        "running"
        if backfill_worker_running is True
        else "not running"
        if backfill_worker_running is False
        else "unknown"
    )
    backfill_activity_rows = [
        [escape("Heartbeat trigger status"), escape(trigger_status)],
        [escape("Worker lock status"), escape(worker_text)],
        [escape("Rows checked today"), escape(_fmt_int(backfill_activity.get("checked_today")))],
        [escape("Rows checked in last 24h"), escape(_fmt_int(backfill_activity.get("checked_24h")))],
        [escape("Rows finalized today"), escape(_fmt_int(backfill_activity.get("finalized_today")))],
        [escape("Sold rows confirmed today"), escape(_fmt_int(backfill_activity.get("sold_today")))],
        [escape("Latest backfill check"), escape(_fmt_dt(backfill_activity.get("latest_checked_at")))],
        [escape("Remaining local daily budget"), escape(_fmt_int(backfill_trigger_out.get("remaining_budget")))],
    ]
    backfill_reason_rows = [
        [escape(str(row.get("reason") or "unknown")), escape(_fmt_int(row.get("checks")))]
        for row in backfill_reasons
    ]
    if not backfill_reason_rows:
        backfill_reason_rows = [["n/a", "0"]]

    lead = (
        "Pipeline is running from live PostgreSQL data. Backfill is incremental; "
        "Buy Desk readiness depends on active-niche comp quality, model-key repair, "
        "and guardrails, not the full stale backlog."
    )
    if blocked:
        lead = f"Blocked services: {', '.join(blocked)}."

    return f"""<!doctype html>
<html>
<body style="margin:0;background:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Arial,sans-serif;color:#0f172a;">
  <div style="max-width:920px;margin:0 auto;padding:28px 14px;">
    <div style="background:#0f172a;border-radius:18px 18px 0 0;padding:26px 28px;color:#ffffff;">
      <div style="font-size:12px;letter-spacing:.12em;text-transform:uppercase;color:#93c5fd;font-weight:800;">GhostFrog Listing Pipeline</div>
      <h1 style="margin:8px 0 6px;font-size:30px;line-height:1.15;">Hourly heartbeat</h1>
      <p style="margin:0;color:#cbd5e1;font-size:15px;line-height:1.5;">{escape(lead)}</p>
      <div style="margin-top:16px;">
        {_status_badge(health_label, health_tone)}
        <span style="display:inline-block;margin-left:8px;color:#cbd5e1;font-size:13px;">{escape(now.astimezone(ASSESS_TZ).strftime('%d %b %Y %H:%M %Z'))}</span>
      </div>
    </div>

    <div style="background:#f8fafc;border:1px solid #cbd5e1;border-top:0;padding:18px 18px 24px;border-radius:0 0 18px 18px;">
      <table role="presentation" cellpadding="0" cellspacing="0" width="100%"><tr>
        {_metric_card("Live listings", _fmt_int(counts.get("live")), f"{_fmt_int(counts.get('total'))} total tracked", "neutral")}
        {_metric_card("Confirmed comps", _fmt_int(comps.get("comp_keys")), f"{_fmt_int(comps.get('comp_samples'))} final-price samples", "good")}
        {_metric_card("Live comp coverage", _fmt_pct(coverage_pct), f"{_fmt_int(live_with_3plus)} of {_fmt_int(live_total)} have 3+ comps", "warn" if coverage_pct < 25 else "good")}
        {_metric_card("API today", _fmt_int(api_total), f"{_fmt_int(normal_calls)} planned normal / {_fmt_int(spare_calls)} spare", "neutral")}
      </tr></table>
      <table role="presentation" cellpadding="0" cellspacing="0" width="100%"><tr>
        {_metric_card("Paper trades", _fmt_int(paper.get("total_predictions")), f"{_fmt_int(paper.get('watching'))} watching / {_fmt_int(paper.get('closed'))} closed", "neutral")}
        {_metric_card("Paper sold", _fmt_int(paper.get("sold")), f"{_fmt_int(paper.get('profitable_on_paper'))} profitable on paper", "good" if int(paper.get("profitable_on_paper") or 0) else "neutral")}
        {_metric_card("Avg price error", "n/a" if paper.get("avg_price_error") is None else f"GBP {float(paper.get('avg_price_error') or 0):.0f}", "Actual final price minus predicted resale", "neutral")}
        {_metric_card("Current ROI rows", _fmt_int(roi.get("live_roi_gt_15")), f"{_fmt_int(roi.get('live_roi_gt_100'))} above 100% ROI", "warn" if int(roi.get("live_roi_gt_100") or 0) else "neutral")}
      </tr></table>

      {_section("What Matters Right Now", f'''
        <ul style="margin:0;padding-left:20px;color:#334155;font-size:14px;line-height:1.6;">
          <li><strong>Live data:</strong> review rows below require recent live evidence, a future end time, and fresh local observation before they appear.</li>
          <li><strong>Backfill budget:</strong> planner recommends {_fmt_int(recommended_backfill)} stale finalization calls/day right now, with a 500-call reserve; the full stale backlog is not a launch blocker.</li>
          <li><strong>Backfill activity:</strong> trigger is <code>{escape(trigger_status)}</code>, worker is <code>{escape(worker_text)}</code>, latest check {_fmt_dt(backfill_activity.get("latest_checked_at"))}, {_fmt_int(backfill_activity.get("checked_today"))} rows checked today.</li>
          <li><strong>Stale backlog:</strong> {_fmt_int(stale_unfinalized)} unfinalized stale rows need controlled resolution.</li>
          <li><strong>Comps:</strong> now based only on confirmed <code>final_price</code>; {_fmt_int(comps.get("comp_keys_3plus"))} model keys have 3+ samples; latest recompute {_fmt_dt(comps.get("computed_at"))}.</li>
          <li><strong>Model-key repair:</strong> {_fmt_int(coverage.get("live_unknown_model_key"))} live listings still have unknown model keys.</li>
          <li><strong>ROI caution:</strong> stored ROI still has {_fmt_int(roi.get("live_roi_gt_1000"))} live rows above 1000%; treat ROI as provisional until recompute and guardrails.</li>
          <li><strong>Learning loop:</strong> {_fmt_int(paper.get("total_predictions"))} paper-trade predictions are now persisted; {_fmt_int(paper.get("watching"))} are still watching for an observed outcome.</li>
        </ul>
      ''')}

      {_section("Paper Trading Loop", f'''
        <p style="margin:0 0 12px;color:#334155;font-size:14px;line-height:1.55;">
          These are review-only snapshots of what the system would have considered. No bids or purchases are made.
          Closed predictions compare the observed final price against the predicted resale estimate.
        </p>
        {_table(["Metric", "Value"], paper_rows)}
        <div style="height:12px;"></div>
        {_table(["Today's source", "Predictions"], paper_source_rows)}
        <div style="height:12px;"></div>
        {_table(["Lesson category", "Count"], paper_lesson_count_rows)}
        <div style="height:12px;"></div>
        {_table(["Source", "Lesson", "Listing", "Learning hint"], paper_lesson_rows)}
      ''')}

      {_section("Fresh Live Review Data", f'''
        <p style="margin:0 0 12px;color:#334155;font-size:14px;line-height:1.55;">
          These rows come from the same fresh-live digest as Slack: local status must still be live-like,
          end time must be in the future, and <code>last_seen_at</code> must be inside the configured freshness window.
          They are inspection prompts only.
        </p>
        {_table(["Source", "Listing", "Current", "Est. profit", "Comps", "Ends", "Notes"], live_review_rows)}
        <div style="height:12px;"></div>
        {_table(["Scout source", "Listing", "Current", "Comp state", "Ends", "Reason"], scout_rows)}
      ''')}

      {_section("Backfill Position", f'''
        <p style="margin:0 0 12px;color:#334155;font-size:14px;line-height:1.55;">
          Today has used <strong>{_fmt_int(today_calls)}</strong> API calls. The planner protects a daily cap of <strong>{_fmt_int(daily_cap)}</strong>,
          keeps <strong>{_fmt_int(reserve)}</strong> calls in reserve, and uses the worst of today, 7-day average ({_fmt_int(avg_calls)}),
          and 7-day max ({_fmt_int(max_calls)}) to avoid spending the whole allowance too early.
          Vehicle/parts-heavy sources are excluded for now: <strong>{escape(excluded_sources or "none")}</strong>.
        </p>
        {_table(["Activity", "Value"], backfill_activity_rows)}
        <div style="height:12px;"></div>
        {_table(["Today's backfill reason", "Checks"], backfill_reason_rows)}
        <div style="height:12px;"></div>
        {_table(["Source", "Keys needing comps", "Live listings", "Stale pool", "Newest stale"], backfill_source_rows)}
      ''')}

      {_section("Listing Store", _table(["Metric", "Count"], listing_rows))}

      {_section("Model-Key Work", _table(["Source", "Live unknown model keys"], unknown_model_rows))}

      {_section("Pipeline Run", _pipeline_table(state))}

      {_section("API Usage Today", _table(["Service", "Calls", "Of 5k cap"], api_rows or [["none", "0", "0%"]]))}

      {_section("Backfill Preview", _table(["Source", "Model key", "Candidate", "Last price"], candidate_rows))}

      {_section("Oldest Enabled Sources", _table(["Source", "Last scraped", "Age"], source_rows))}

      <div style="background:#0f172a;color:#cbd5e1;border-radius:14px;padding:16px 18px;margin-top:16px;font-size:13px;line-height:1.5;">
        <strong style="color:#ffffff;">Footer:</strong>
        Python/LangGraph remains the source of truth. No bidding, buying, relisting, or marketplace writes are performed by this heartbeat.
        Backups run weekly; final-price backfill runs as a separately locked, budget-capped background worker.
      </div>
    </div>
  </div>
</body>
</html>"""


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

    metrics = _heartbeat_metrics()
    blocked = state.get("blocked_services") or []
    subject_status = "blocked" if blocked else "heartbeat"
    subject = (
        f"[GhostFrog] Listing pipeline {subject_status} - "
        f"{metrics.get('api_total', 0):,} API calls today"
    )
    body = _build_heartbeat_email_html(state, metrics, now)

    sent_to: list[str] = []
    try:
        for recipient in HEARTBEAT_EMAIL_RECIPIENTS:
            send_email(subject=subject, body=body, to_addr=recipient, is_html=True)
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


def hourly_status_slack(state: MainState) -> MainState:
    """
    Send a throttled Slack heartbeat for the LangGraph pipeline.

    Slack is a low-risk notification channel and intentionally cannot trigger
    commercial or marketplace actions. Failures are logged in state but do not
    fail the main heartbeat.
    """

    if state.get("maintenance_skip"):
        state["heartbeat_slack_out"] = {
            "status": "skipped_due_to_maintenance",
            "reason": "maintenance_pause_active",
        }
        return state

    if not slack_notifications_enabled():
        state["heartbeat_slack_out"] = {
            "status": "skipped",
            "reason": "disabled",
        }
        return state

    now = datetime.now(timezone.utc)
    last_sent = _get_last_heartbeat_slack_sent()
    if last_sent is not None:
        last_sent_aware = last_sent if last_sent.tzinfo else last_sent.replace(tzinfo=timezone.utc)
        if (now - last_sent_aware) < HEARTBEAT_SLACK_INTERVAL:
            state["heartbeat_slack_out"] = {
                "status": "skipped_recently_sent",
                "last_sent_at": last_sent_aware.isoformat(),
            }
            return state

    metrics = _heartbeat_metrics()
    text, blocks = _build_heartbeat_slack_message(state, metrics, now)
    result = send_slack_message(text=text, blocks=blocks)

    if result.status == "sent":
        _set_last_heartbeat_slack_sent(now)
        logger.info("[main] hourly heartbeat Slack notification sent")
    elif result.status == "failed":
        logger.warning("[main] hourly heartbeat Slack notification failed: %s", result.error)

    state["heartbeat_slack_out"] = asdict(result)
    if result.status == "sent":
        state["heartbeat_slack_out"]["sent_at"] = now.isoformat()
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
        "paper_trading",
        run_subgraph(
            "paper_trading",
            build_paper_trading,
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
    g.add_node("backfill_trigger", backfill_trigger)
    g.add_node("heartbeat_email", hourly_status_email)
    g.add_node("heartbeat_slack", hourly_status_slack)

    g.set_entry_point("init")

    # Straight chain; API-using nodes may skip internally.
    g.add_edge("init", "ended")
    g.add_edge("ended", "retrieve")
    g.add_edge("retrieve", "pph")
    g.add_edge("pph", "comps")
    g.add_edge("comps", "attributes")
    g.add_edge("attributes", "hot")
    g.add_edge("hot", "roi")
    g.add_edge("roi", "paper_trading")
    g.add_edge("paper_trading", "new")
    g.add_edge("new", "project_mini")
    g.add_edge("project_mini", "project_msx125")

    # After all main work is done, optionally spawn background jobs, then finish.
    g.add_edge("project_msx125", "assess_trigger")
    g.add_edge("assess_trigger", "backfill_trigger")
    g.add_edge("backfill_trigger", "heartbeat_email")
    g.add_edge("heartbeat_email", "heartbeat_slack")
    g.add_edge("heartbeat_slack", END)

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
