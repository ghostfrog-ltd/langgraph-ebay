from __future__ import annotations

import os
import sys
import subprocess
import time
from pathlib import Path
from dataclasses import asdict
from html import escape
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from typing import TypedDict, Any, Dict, List, Sequence

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.auth import get_auth
from utils.ebay_api_accounting import SERVICE_LIMIT_DEFAULTS, build_api_accounting
from utils.usage_tracker import get_all_api_usage_today
from utils.db_schema import connection
from utils.pipeline_control import is_heartbeat_paused
from utils.emailer import send_email
from utils.slack_notifier import send_slack_message, slack_notifications_enabled

from pipelines.listing.ended.graph import build_graph as build_ended
from pipelines.listing.retrieve.graph import build_graph as build_retrieve
from pipelines.listing.retrieve.graph import ADAPTERS as RETRIEVE_ADAPTERS
from pipelines.listing.pph.graph import build_graph as build_pph
from pipelines.listing.comps.graph import build_graph as build_comps
from pipelines.listing.attributes.graph import build_graph as build_attributes
from pipelines.listing.hot.graph import build_graph as build_hot
from pipelines.listing.roi.graph import build_graph as build_roi
from pipelines.listing.new.graph import build_graph as build_new
from pipelines.listing.project_mini.graph import build_graph as build_project_mini
from pipelines.listing.project_msx125.graph import build_graph as build_project_msx125
from pipelines.listing.point_zero.backfill_apply import backfill_checks_today, is_backfill_running
from pipelines.listing.point_zero.backfill_plan import build_plan as build_backfill_plan
from pipelines.listing.market_discovery.graph import (
    MIN_CANDIDATE_COMPS,
    run as run_market_discovery,
    useful_near_misses,
)
from pipelines.listing.operator_metrics import build_operator_metrics
from pipelines.listing.opportunity_digest import (
    build_review_auction_digest,
    format_review_auction_digest_for_slack,
)
from pipelines.listing.review_actions import list_favourites, review_url

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# API usage config
# ---------------------------------------------------------------------------

# Local service counters are grouped into real eBay quota buckets by
# utils.ebay_api_accounting before anything is blocked.
DEFAULT_DAILY_LIMIT = 5000


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
HEARTBEAT_MODEL_KEYS_PER_MARKET = int(os.getenv("GF_HEARTBEAT_MODEL_KEYS_PER_MARKET", "80"))

HEARTBEAT_EMAIL_NAME = "listing_system_hourly_heartbeat"
HEARTBEAT_EMAIL_INTERVAL = timedelta(hours=int(os.getenv("GF_HEARTBEAT_EMAIL_HOURS", "1")))
HEARTBEAT_EMAIL_RECIPIENTS = [
    email.strip()
    for email in os.getenv(
        "GF_HEARTBEAT_EMAIL_RECIPIENTS",
        "garyconstable80@gmail.com,info@ghostfrog.co.uk,sarahmatthews2001@yahoo.co.uk",
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
HEARTBEAT_SLACK_REPORT_LIMIT = int(os.getenv("GF_HEARTBEAT_SLACK_REPORT_LIMIT", "6"))
HEARTBEAT_LAUNCHD_LABEL = os.getenv(
    "GF_HEARTBEAT_LAUNCHD_LABEL",
    "com.ghostfrog.langgraph-ebay.python-heartbeat",
)
HEARTBEAT_SCHEDULE_MINUTES = int(os.getenv("GF_HEARTBEAT_SCHEDULE_MINUTES", "5"))
HEARTBEAT_STAGE_WARN_SECONDS = float(os.getenv("GF_HEARTBEAT_STAGE_WARN_SECONDS", "30"))
HEARTBEAT_EMAIL_REPORT_LIMIT = 0
HEARTBEAT_CANDIDATE_REFRESH_LIMIT = int(
    os.getenv("GF_HEARTBEAT_CANDIDATE_REFRESH_LIMIT", str(HEARTBEAT_EMAIL_REPORT_LIMIT))
)
HEARTBEAT_POKEMON_PROFIT_GBP = float(os.getenv("GF_HEARTBEAT_POKEMON_PROFIT_GBP", "20"))
HEARTBEAT_POKEMON_MIN_COMPS = int(os.getenv("GF_HEARTBEAT_POKEMON_MIN_COMPS", "3"))
REVIEW_SERVER_URL = os.getenv("GF_REVIEW_SERVER_URL", "http://127.0.0.1:8765")


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
    backfill_trigger_out: Dict[str, Any]
    scheduler_status_out: Dict[str, Any]
    heartbeat_metrics_out: Dict[str, Any]
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

    if isinstance(raw_usage, dict):
        # Normal case: dict of {service: count}
        breakdown = {str(k): int(v or 0) for k, v in raw_usage.items()}
        api_accounting = build_api_accounting(breakdown)
        blocked_services = list(api_accounting["blocked_services"])

        logger.info(
            "[main] API bucket usage today: %s",
            ", ".join(
                f"{bucket}={info['used']}/{info['limit']}"
                for bucket, info in api_accounting["buckets"].items()
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

        breakdown = {"unknown": used_total}
        api_accounting = build_api_accounting(breakdown)
        api_accounting["blocked_services"] = blocked_services
        for service in SERVICE_LIMIT_DEFAULTS:
            api_accounting["services"][service] = {
                "used": used_total,
                "limit": generic_limit,
                "blocked": blocked,
                "bucket": "generic",
                "bucket_label": "Generic API",
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
        "mode": "bucketed" if isinstance(raw_usage, dict) else "generic",
        "services": api_accounting["services"],
        "buckets": api_accounting["buckets"],
        "blocked_buckets": api_accounting["blocked_buckets"],
        "raw_breakdown": breakdown,
    }

    if blocked_services:
        logger.warning(
            "[main] API buckets blocked: %s; services affected: %s",
            ", ".join(sorted(api_accounting["blocked_buckets"])) or "generic",
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
        started = time.perf_counter()
        out = graph.invoke(dict(state), config={"recursion_limit": 500})
        duration = time.perf_counter() - started
        if isinstance(out, dict):
            out["duration_seconds"] = round(duration, 3)
        state[f"{name}_out"] = out
        if duration >= HEARTBEAT_STAGE_WARN_SECONDS:
            logger.warning("[main] -> %s end after %.2fs", name, duration)
        else:
            logger.info("[main] -> %s end after %.2fs", name, duration)
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
    and whether the planner can see at least one useful unknown candidate.
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
    candidates = plan.get("recommended_unknown_candidate_preview") or []
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


def _metrics_for_heartbeat(state: MainState) -> dict[str, Any]:
    """Return this tick's heartbeat metrics, computing them at most once."""

    cached = state.get("heartbeat_metrics_out")
    if isinstance(cached, dict):
        return cached

    metrics = _heartbeat_metrics()
    state["heartbeat_metrics_out"] = metrics
    return metrics


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


def _api_bucket_summary(api_accounting: Any, *, long: bool = False) -> str:
    if not isinstance(api_accounting, dict):
        return "no bucket data"
    buckets = api_accounting.get("buckets")
    if not isinstance(buckets, dict):
        return "no bucket data"
    parts: list[str] = []
    for bucket, short_label in (("trading", "T"), ("browse", "B"), ("identity", "I")):
        info = buckets.get(bucket)
        if not isinstance(info, dict):
            continue
        used = _fmt_int(info.get("used"))
        limit = _fmt_int(info.get("limit"))
        label = str(info.get("label") or bucket)
        status = " blocked" if info.get("blocked") else ""
        if long:
            parts.append(f"{label}: {used}/{limit}{status}")
        else:
            parts.append(f"{short_label} {used}/{limit}{status}")
    return " | ".join(parts) if parts else "no bucket data"


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


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        raw = str(value).strip()
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _email_end_time(value: Any) -> str:
    parsed = _parse_dt(value)
    if parsed is None:
        return "n/a"
    return escape(parsed.astimezone(ASSESS_TZ).strftime("%d %b %H:%M"))


def _email_end_sort_key(item: dict[str, Any]) -> tuple[int, datetime]:
    parsed = _parse_dt(item.get("end_time"))
    if parsed is None:
        return (1, datetime.max.replace(tzinfo=timezone.utc))
    return (0, parsed.astimezone(timezone.utc))


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
    shared_started = time.perf_counter()
    metrics = build_operator_metrics(
        include_model_key_inventory=True,
        model_keys_per_market=HEARTBEAT_MODEL_KEYS_PER_MARKET,
        backfill_daily_cap=5000,
        backfill_safety_reserve=500,
        backfill_max_per_day=1000,
        include_vehicle_sources=False,
    )
    metric_timings = metrics.get("metric_timings")
    if not isinstance(metric_timings, dict):
        metric_timings = {}
        metrics["metric_timings"] = metric_timings

    def mark_shared(name: str, started: float) -> None:
        metric_timings[name] = round(time.perf_counter() - started, 3)

    t = time.perf_counter()
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
    metrics["source_freshness"] = source_freshness
    mark_shared("source_freshness", t)

    t = time.perf_counter()
    try:
        market_discovery = run_market_discovery(
            limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
            min_profit=50.0,
            min_comps=MIN_CANDIDATE_COMPS,
            include_future_unknown=True,
        )
    except Exception as exc:
        market_discovery = {"error": str(exc), "top_lanes": [], "status_counts": {}}

    try:
        market_discovery_punt = run_market_discovery(
            limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
            min_profit=25.0,
            min_comps=MIN_CANDIDATE_COMPS,
            include_future_unknown=True,
        )
    except Exception as exc:
        market_discovery_punt = {"error": str(exc), "potentials": [], "closest": []}
    try:
        market_discovery_pokemon = run_market_discovery(
            limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
            min_profit=HEARTBEAT_POKEMON_PROFIT_GBP,
            min_comps=HEARTBEAT_POKEMON_MIN_COMPS,
            include_future_unknown=True,
            source="ebay-pokemon",
        )
    except Exception as exc:
        market_discovery_pokemon = {"error": str(exc), "potentials": [], "closest": []}
    mark_shared("market_discovery_initial", t)

    t = time.perf_counter()
    if (
        "error" not in market_discovery
        or "error" not in market_discovery_punt
        or "error" not in market_discovery_pokemon
    ):
        try:
            candidate_refresh = _refresh_candidate_prices_for_email(
                market_discovery,
                market_discovery_punt,
                market_discovery_pokemon,
            )
        except Exception as exc:
            logger.warning("[heartbeat] candidate price refresh failed: %s", exc)
            candidate_refresh = {"status": "error", "error": str(exc)}
    else:
        candidate_refresh = {
            "status": "skipped",
            "reason": "market_discovery_error",
        }
    metrics["candidate_price_refresh"] = candidate_refresh
    mark_shared("candidate_price_refresh", t)

    if int(candidate_refresh.get("refreshed") or 0) > 0:
        t = time.perf_counter()
        try:
            market_discovery = run_market_discovery(
                limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
                min_profit=50.0,
                min_comps=MIN_CANDIDATE_COMPS,
            )
        except Exception as exc:
            market_discovery = {"error": str(exc), "top_lanes": [], "status_counts": {}}
        try:
            market_discovery_punt = run_market_discovery(
                limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
                min_profit=25.0,
                min_comps=MIN_CANDIDATE_COMPS,
            )
        except Exception as exc:
            market_discovery_punt = {"error": str(exc), "potentials": [], "closest": []}
        try:
            market_discovery_pokemon = run_market_discovery(
                limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
                min_profit=HEARTBEAT_POKEMON_PROFIT_GBP,
                min_comps=HEARTBEAT_POKEMON_MIN_COMPS,
                source="ebay-pokemon",
            )
        except Exception as exc:
            market_discovery_pokemon = {"error": str(exc), "potentials": [], "closest": []}
        mark_shared("market_discovery_after_refresh", t)

    metrics["market_discovery"] = market_discovery
    metrics["market_discovery_punt"] = market_discovery_punt
    metrics["market_discovery_pokemon"] = market_discovery_pokemon

    t = time.perf_counter()
    try:
        metrics["listing_favourites"] = list_favourites(limit=100)
    except Exception as exc:
        metrics["listing_favourites"] = {"error": str(exc)}
    mark_shared("listing_favourites", t)

    t = time.perf_counter()
    try:
        review_auctions = build_review_auction_digest(
            limit=HEARTBEAT_SLACK_AUCTION_DIGEST_LIMIT
        )
    except Exception as exc:
        review_auctions = {"error": str(exc), "items": [], "count": 0}
    metrics["review_auctions"] = review_auctions
    mark_shared("review_auctions", t)
    metric_timings["total"] = round(time.perf_counter() - shared_started, 3)
    return metrics


def _candidate_rows_for_refresh(*reports: dict[str, Any], limit: int) -> list[dict[str, Any]]:
    """Collect unique rows worth spending a pre-email getItem call on."""

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    capped_limit = max(0, int(limit or 0))

    def add_row(row: dict[str, Any]) -> bool:
        external_id = str(row.get("external_id") or "").strip()
        source = str(row.get("source") or "").strip()
        if not external_id or not source:
            return False
        unique_key = f"{source}:{external_id}"
        if unique_key in seen:
            return False
        seen.add(unique_key)
        rows.append(row)
        return bool(capped_limit and len(rows) >= capped_limit)

    for report in reports:
        if not isinstance(report, dict):
            continue
        potentials = report.get("potentials")
        if isinstance(potentials, list):
            for row in potentials:
                if isinstance(row, dict) and add_row(row):
                    return rows

        thresholds = report.get("thresholds")
        min_profit = 0.0
        if isinstance(thresholds, dict):
            try:
                min_profit = float(thresholds.get("min_profit_gbp") or 0.0)
            except (TypeError, ValueError):
                min_profit = 0.0
        closest = report.get("closest")
        if isinstance(closest, list):
            near_misses = useful_near_misses(
                [row for row in closest if isinstance(row, dict)],
                min_profit=min_profit,
                limit=0,
            )
            for row in near_misses:
                if add_row(dict(row)):
                    return rows

    return rows


def _refresh_candidate_prices_for_email(*reports: dict[str, Any]) -> dict[str, Any]:
    """
    Refresh exact shortlisted live listings before they reach the email.

    Discovery prices are allowed to be stale. Email candidates are not: this
    calls Browse getItem only for the small candidate/near-miss shortlist, lets
    the normal adapter upsert update price/bids/end time, and then callers rerun
    the deterministic potential query.
    """

    rows = _candidate_rows_for_refresh(
        *reports,
        limit=HEARTBEAT_CANDIDATE_REFRESH_LIMIT,
    )
    if not rows:
        return {
            "status": "skipped",
            "reason": "no_shortlist_rows",
            "requested": 0,
            "refreshed_sources": {},
        }

    adapter_by_source = {
        getattr(adapter_cls, "DOMAIN", ""): adapter_cls
        for adapter_cls in RETRIEVE_ADAPTERS.values()
    }
    ids_by_source: dict[str, list[str]] = {}
    skipped_sources: dict[str, int] = {}
    for row in rows:
        source = str(row.get("source") or "").strip()
        external_id = str(row.get("external_id") or "").strip()
        if source not in adapter_by_source:
            skipped_sources[source] = skipped_sources.get(source, 0) + 1
            continue
        ids_by_source.setdefault(source, []).append(external_id)

    if not ids_by_source:
        return {
            "status": "skipped",
            "reason": "no_supported_sources",
            "requested": len(rows),
            "refreshed_sources": {},
            "skipped_sources": skipped_sources,
        }

    token = get_auth().get_token()
    refreshed_sources: dict[str, int] = {}
    errors: dict[str, str] = {}
    for source, external_ids in ids_by_source.items():
        adapter_cls = adapter_by_source[source]
        try:
            adapter = adapter_cls()
            adapter.refresh_items_price(token, external_ids)
            refreshed_sources[source] = len(external_ids)
        except Exception as exc:
            logger.warning(
                "[heartbeat] candidate price refresh failed for %s (%d ids): %s",
                source,
                len(external_ids),
                exc,
            )
            errors[source] = str(exc)

    status = "ok" if refreshed_sources else "error"
    return {
        "status": status,
        "requested": len(rows),
        "refreshed": sum(refreshed_sources.values()),
        "refreshed_sources": refreshed_sources,
        "skipped_sources": skipped_sources,
        "errors": errors,
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


def _listing_column_explainer() -> str:
    items = [
        ("Type", "Auction, Buy Now, or Auction + BIN."),
        ("Ends", "eBay end time in UK time. Soonest-ending rows are shown first."),
        ("Current", "Latest price we have after the email candidate refresh."),
        ("Max buy", "Highest safe buy price after resale costs and target profit."),
        ("Profit", "Expected profit if bought at the current price: sold median minus costs minus current price."),
        ("Sold median", "Median final sold price from same model key and condition-grade comps."),
        ("Comps / grade", "Number of sold comps used, plus the condition grade used for matching."),
        ("Next", "Manual suggestion: bid/watch/buy-now/avoid, plus suggested BIN relist price."),
        ("Review", "Up keeps it in favourites; Down suppresses it from future candidate lists."),
    ]
    cards = ""
    for label, detail in items:
        cards += (
            '<div style="display:inline-block;width:30%;min-width:190px;vertical-align:top;'
            'margin:0 10px 10px 0;padding:9px 10px;border:1px solid #dbeafe;'
            'border-radius:10px;background:#f8fafc;">'
            f'<div style="font-size:12px;font-weight:800;color:#0f172a;margin-bottom:3px;">{escape(label)}</div>'
            f'<div style="font-size:12px;line-height:1.35;color:#475569;">{escape(detail)}</div>'
            '</div>'
        )
    return (
        '<p style="margin:0 0 12px;color:#475569;font-size:13px;line-height:1.45;">'
        'These are read-only buy signals. The system does not bid, buy, or relist anything; '
        'it only shows the maths so you can open eBay and decide.</p>'
        f'<div>{cards}</div>'
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


def _review_links_for_email(item: dict[str, Any]) -> str:
    """Render local thumbs-up/down links for one email listing row."""

    external_id = str(item.get("external_id") or "").strip()
    if not external_id:
        return "n/a"
    up_url = review_url(
        base_url=REVIEW_SERVER_URL,
        action="up",
        source=item.get("source"),
        external_id=external_id,
        title=item.get("title"),
        url=item.get("url"),
    )
    down_url = review_url(
        base_url=REVIEW_SERVER_URL,
        action="down",
        source=item.get("source"),
        external_id=external_id,
        title=item.get("title"),
        url=item.get("url"),
    )
    link_style = "color:#0369a1;text-decoration:none;font-weight:700;"
    down_style = "color:#b91c1c;text-decoration:none;font-weight:700;"
    return (
        f'<a href="{escape(up_url)}" style="{link_style}">Up</a>'
        ' <span style="color:#94a3b8;">|</span> '
        f'<a href="{escape(down_url)}" style="{down_style}">Down</a>'
    )


def _email_sale_type(value: Any) -> str:
    raw = str(value or "").strip().lower().replace("_", "-")
    if raw in {"auction", "bid"}:
        return "Auction"
    if raw in {"bin", "buy-now", "buy-it-now", "fixed-price", "fixedprice"}:
        return "Buy Now"
    if raw in {"both", "auction-bin", "auction+bin", "auction-fixed-price"}:
        return "Auction + BIN"
    return "n/a"


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _suggested_relist_bin_price(sold_median: Any) -> str:
    median = _as_float(sold_median)
    if median <= 0:
        return "n/a"
    return f"&pound;{round(median * 0.95):.0f}"


def _hours_until_end(value: Any) -> float | None:
    if isinstance(value, datetime):
        end = value
    else:
        raw = str(value or "").strip()
        if not raw:
            return None
        try:
            end = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if end.tzinfo is None:
        end = end.replace(tzinfo=timezone.utc)
    return (end.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds() / 3600.0


def _next_action_for_email(item: dict[str, Any]) -> str:
    sale_type = _email_sale_type(item.get("sale_type"))
    current = _as_float(item.get("current_price"))
    max_buy = _as_float(item.get("max_safe_buy"))
    relist_price = _suggested_relist_bin_price(item.get("sold_median"))

    if max_buy <= 0 or current > max_buy:
        return "Avoid"

    max_text = f"&pound;{max_buy:.2f}"
    relist_text = f"relist BIN {relist_price}" if relist_price != "n/a" else "relist BIN n/a"
    hours_left = _hours_until_end(item.get("end_time"))

    if sale_type == "Auction":
        if hours_left is not None and hours_left > 24 and current < (max_buy * 0.35):
            return f"Watch; bid late to {max_text}; {relist_text}"
        return f"Bid to {max_text}; {relist_text}"

    if sale_type == "Buy Now":
        return f"Buy now if <= {max_text}; {relist_text}"

    if sale_type == "Auction + BIN":
        return f"Bid/BIN if <= {max_text}; {relist_text}"

    return f"Buy if <= {max_text}; {relist_text}"


def _potential_rows_for_email(
    report: dict[str, Any],
    *,
    item_key: str = "potentials",
    exclude_external_ids: set[str] | None = None,
    limit: int = 8,
    min_comps: int = MIN_CANDIDATE_COMPS,
    empty_message: str = "No live candidates cleared this gate.",
) -> tuple[list[list[str]], set[str]]:
    """Render live potential or closest-miss rows for the heartbeat email."""

    exclude_external_ids = exclude_external_ids or set()
    used_external_ids: set[str] = set()
    rows: list[list[str]] = []
    if report.get("error"):
        return [[escape("Report failed"), escape(str(report.get("error"))[:180]), "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"]], used_external_ids

    items = [
        item
        for item in (report.get(item_key) or [])
        if isinstance(item, dict)
    ]
    items.sort(key=_email_end_sort_key)

    for item in items:
        if int(item.get("comp_samples") or 0) < min_comps:
            continue
        external_id = str(item.get("external_id") or "")
        if external_id and external_id in exclude_external_ids:
            continue
        used_external_ids.add(external_id)
        rows.append([
            escape(str(item.get("source") or "")),
            _email_link(item.get("title"), item.get("url")),
            escape(_email_sale_type(item.get("sale_type"))),
            _email_end_time(item.get("end_time")),
            f"&pound;{float(item.get('current_price') or 0):.2f}",
            f"&pound;{float(item.get('max_safe_buy') or 0):.2f}",
            f"&pound;{float(item.get('expected_profit') or 0):.2f}",
            f"&pound;{float(item.get('sold_median') or 0):.2f}",
            escape(f"{int(item.get('comp_samples') or 0)} / {item.get('condition_grade') or 'n/a'}"),
            _next_action_for_email(item),
            _review_links_for_email(item),
        ])
        if limit and len(rows) >= limit:
            break

    if not rows:
        rows = [["n/a", escape(empty_message), "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a", "n/a"]]
    return rows, used_external_ids


def _favourite_rows_for_email(favourites: Any) -> list[list[str]]:
    if isinstance(favourites, dict) and favourites.get("error"):
        return [["n/a", escape(str(favourites.get("error"))[:180]), "n/a", "n/a", "n/a", "n/a"]]
    if not isinstance(favourites, list) or not favourites:
        return [["n/a", "No favourites yet.", "n/a", "n/a", "n/a", "n/a"]]

    rows: list[list[str]] = []
    for item in favourites:
        if not isinstance(item, dict):
            continue
        try:
            price = f"&pound;{float(item.get('price_current') or 0):.2f}"
        except (TypeError, ValueError):
            price = "n/a"
        rows.append(
            [
                escape(str(item.get("source") or "")),
                _email_link(item.get("title"), item.get("url")),
                escape(str(item.get("status") or "unknown")),
                price,
                escape(str(item.get("model_key") or "n/a")),
                _review_links_for_email(item),
            ]
        )
    return rows or [["n/a", "No favourites yet.", "n/a", "n/a", "n/a", "n/a"]]


def _market_label(source: Any) -> str:
    label = str(source or "unknown").strip()
    if label.startswith("ebay-"):
        label = label[5:]
    label = label.replace("-", " ").replace("_", " ").strip()
    return label.title() if label else "Unknown"


def _model_key_inventory_for_email(rows: list[dict[str, Any]]) -> str:
    if not rows:
        return (
            '<p style="margin:0;color:#475569;font-size:13px;line-height:1.45;">'
            'No clean model keys found yet.</p>'
        )

    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(str(row.get("source") or "unknown"), []).append(row)

    cards = (
        '<p style="margin:0 0 12px;color:#475569;font-size:13px;line-height:1.45;">'
        'This is the usable model-key evidence only: confirmed final-price rows plus current live rows. '
        'It does not include the unknown past-end backfill queue.</p>'
    )
    for source in sorted(grouped, key=_market_label):
        source_rows = grouped[source]
        first = source_rows[0]
        total_keys = int(first.get("source_key_count") or len(source_rows))
        shown_keys = len(source_rows)
        listing_count = int(first.get("source_listing_count") or 0)
        live_count = int(first.get("source_live_count") or 0)
        final_price_count = int(first.get("source_final_price_count") or 0)
        without_final_price_count = int(first.get("source_without_final_price_count") or 0)
        hidden_count = max(total_keys - shown_keys, 0)

        chips = ""
        for key_row in source_rows:
            key = str(key_row.get("model_key") or "").strip()
            final_count = int(key_row.get("final_price_count") or 0)
            live_for_key = int(key_row.get("live_count") or 0)
            detail_parts = []
            if final_count:
                detail_parts.append(f"{_fmt_int(final_count)} final")
            if live_for_key:
                detail_parts.append(f"{_fmt_int(live_for_key)} live")
            detail = f" <span style=\"color:#64748b;\">{' / '.join(detail_parts)}</span>" if detail_parts else ""
            chips += (
                '<span style="display:inline-block;margin:0 6px 7px 0;padding:6px 8px;'
                'border:1px solid #cbd5e1;border-radius:999px;background:#f8fafc;'
                'font-size:12px;line-height:1.2;color:#0f172a;font-family:Menlo,Consolas,monospace;">'
                f'{escape(key)}{detail}</span>'
            )
        if hidden_count:
            chips += (
                '<span style="display:inline-block;margin:0 6px 7px 0;padding:6px 8px;'
                'border:1px solid #fed7aa;border-radius:999px;background:#fff7ed;'
                'font-size:12px;line-height:1.2;color:#9a3412;font-weight:700;">'
                f'+{_fmt_int(hidden_count)} more</span>'
            )

        cards += (
            '<div style="border:1px solid #dbeafe;border-radius:12px;background:#ffffff;'
            'padding:14px;margin:10px 0;">'
            '<div style="display:block;margin-bottom:10px;">'
            f'<div style="font-size:15px;font-weight:800;color:#0f172a;">{escape(_market_label(source))}</div>'
            f'<div style="font-size:12px;color:#64748b;margin-top:3px;">'
            f'{_fmt_int(total_keys)} keys &middot; {_fmt_int(listing_count)} usable rows &middot; '
            f'{_fmt_int(final_price_count)} final prices &middot; '
            f'{_fmt_int(without_final_price_count)} without final price &middot; '
            f'{_fmt_int(live_count)} live'
            '</div></div>'
            f'<div>{chips}</div>'
            '</div>'
        )

    return cards


def _pipeline_table(state: MainState) -> str:
    keys = [
        ("Ended", "ended_out"),
        ("Retrieve", "retrieve_out"),
        ("PPH", "pph_out"),
        ("Comps", "comps_out"),
        ("Attributes", "attributes_out"),
        ("Hot", "hot_out"),
        ("ROI", "roi_out"),
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
    Build a safe Slack summary for read-only live-listing potential output.

    This is plain maths: live listing price versus same-model confirmed sold
    comps and max safe buy price. It never performs marketplace actions.
    """

    if market.get("error"):
        return "Report failed", "Check local logs before using potential output."

    summary = market.get("summary") or {}
    potentials = market.get("potentials") or []
    closest = market.get("closest") or []
    counts_text = (
        f"{_fmt_int(summary.get('potentials'))} potentials, "
        f"{_fmt_int(summary.get('checked'))} checked, "
        f"{_fmt_int(summary.get('live_rows_with_comps'))} live rows with comps"
    )

    if potentials:
        top = potentials[0]
        top_text = (
            f"Top potential: {_short_slack_title(top.get('title') or top.get('source') or 'listing')} "
            f"current £{_fmt_num(top.get('current_price'))}, "
            f"max £{_fmt_num(top.get('max_safe_buy'))}, "
            f"profit £{_fmt_num(top.get('expected_profit'))}"
        )
    elif closest:
        top = closest[0]
        top_text = (
            f"Closest row: {_short_slack_title(top.get('title') or top.get('source') or 'listing')} "
            f"current £{_fmt_num(top.get('current_price'))}, "
            f"max £{_fmt_num(top.get('max_safe_buy'))}, "
            f"gap £{_fmt_num(top.get('price_gap_to_max_buy'))}"
        )
    else:
        top_text = "No live row with clean model key and comps."

    return counts_text, top_text


def _slack_escape(value: Any) -> str:
    return str(value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _short_slack_title(value: Any, *, limit: int = 82) -> str:
    cleaned = " ".join(str(value or "").split())
    if len(cleaned) <= limit:
        return cleaned or "Untitled listing"
    return f"{cleaned[: limit - 3].rstrip()}..."


def _slack_listing_link(title: Any, url: Any) -> str:
    label = _slack_escape(_short_slack_title(title))
    clean_url = str(url or "").strip()
    if not clean_url:
        return label
    return f"<{clean_url}|{label}>"


def _slack_review_commands(item: dict[str, Any]) -> str:
    external_id = str(item.get("external_id") or "").strip()
    if not external_id:
        return "review: n/a"
    return f"review: `up {external_id}` / `down {external_id}`"


def _slack_listing_price_line(item: dict[str, Any], *, favourite: bool = False) -> str:
    if favourite:
        price = item.get("price_current")
        status = _slack_escape(item.get("status") or "unknown")
        return f"   Current £{_fmt_num(price)} | status {status} | {_slack_review_commands(item)}"

    parts = [
        f"Current £{_fmt_num(item.get('current_price'))}",
        f"max £{_fmt_num(item.get('max_safe_buy'))}",
        f"profit £{_fmt_num(item.get('expected_profit'))}",
        f"sold £{_fmt_num(item.get('sold_median'))}",
        f"comps {_fmt_int(item.get('comp_samples'))}",
    ]
    sale_type = _email_sale_type(item.get("sale_type"))
    if sale_type != "n/a":
        parts.append(sale_type)
    ends = _email_end_time(item.get("end_time"))
    if ends != "n/a":
        parts.append(f"ends {ends}")
    return f"   {' | '.join(parts)} | {_slack_review_commands(item)}"


def _slack_rows_from_report(
    report: dict[str, Any],
    *,
    item_key: str = "potentials",
    exclude_external_ids: set[str] | None = None,
    limit: int = HEARTBEAT_SLACK_REPORT_LIMIT,
    min_comps: int = MIN_CANDIDATE_COMPS,
) -> tuple[list[str], set[str]]:
    exclude_external_ids = exclude_external_ids or set()
    used_external_ids: set[str] = set()
    if report.get("error"):
        return ["Report failed; check local logs before relying on this section."], used_external_ids

    rows: list[str] = []
    items = [
        item
        for item in (report.get(item_key) or [])
        if isinstance(item, dict)
    ]
    items.sort(key=_email_end_sort_key)
    for index, item in enumerate(items, start=1):
        if int(item.get("comp_samples") or 0) < min_comps:
            continue
        external_id = str(item.get("external_id") or "")
        if external_id and external_id in exclude_external_ids:
            continue
        used_external_ids.add(external_id)
        source = _slack_escape(_market_label(item.get("source")))
        rows.append(
            "\n".join(
                [
                    f"{len(rows) + 1}. `{source}` {_slack_listing_link(item.get('title'), item.get('url'))}",
                    _slack_listing_price_line(item),
                ]
            )
        )
        if limit and len(rows) >= limit:
            break
    return rows, used_external_ids


def _slack_favourite_rows(favourites: Any, *, limit: int = HEARTBEAT_SLACK_REPORT_LIMIT) -> list[str]:
    if isinstance(favourites, dict) and favourites.get("error"):
        return [f"Favourites unavailable: {_slack_escape(str(favourites.get('error'))[:180])}"]
    if not isinstance(favourites, list) or not favourites:
        return ["No favourites yet."]

    rows: list[str] = []
    for item in favourites:
        if not isinstance(item, dict):
            continue
        source = _slack_escape(_market_label(item.get("source")))
        rows.append(
            "\n".join(
                [
                    f"{len(rows) + 1}. `{source}` {_slack_listing_link(item.get('title'), item.get('url'))}",
                    _slack_listing_price_line(item, favourite=True),
                ]
            )
        )
        if limit and len(rows) >= limit:
            break
    return rows or ["No favourites yet."]


def _slack_listing_section(title: str, rows: list[str], empty_message: str) -> dict[str, Any]:
    body = "\n\n".join(rows) if rows else empty_message
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": f"*{title}*\n{body}",
        },
    }


def _heartbeat_timing_summary(metrics: dict[str, Any]) -> str:
    timings = metrics.get("metric_timings") or {}
    if not isinstance(timings, dict) or not timings:
        return "n/a"

    total = float(timings.get("total") or 0.0)
    slow = sorted(
        (
            (name, float(seconds))
            for name, seconds in timings.items()
            if name != "total"
        ),
        key=lambda item: item[1],
        reverse=True,
    )
    if not slow:
        return f"{total:.1f}s total"
    name, seconds = slow[0]
    return f"{total:.1f}s total; slowest {name} {seconds:.1f}s"


def _pipeline_slack_summary(state: MainState) -> str:
    """
    Summarise the current LangGraph pipeline run for Slack.

    The summary is intentionally operational, not commercial: it exposes stage
    health and candidate-check state without producing buy instructions.
    """

    keys = [
        ("Ended", "ended_out"),
        ("Retrieve", "retrieve_out"),
        ("Comps", "comps_out"),
        ("Attributes", "attributes_out"),
        ("ROI", "roi_out"),
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
    market = metrics.get("market_discovery") or {}
    punt_report = metrics.get("market_discovery_punt") or {}
    favourites = metrics.get("listing_favourites") or []
    review_auctions = metrics.get("review_auctions") or {}
    api_total = int(metrics["api_total"] or 0)
    api_accounting = metrics.get("api_accounting") or {}
    api_summary = _api_bucket_summary(api_accounting, long=True)
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
        f"{_fmt_int(api_total)} local API calls today."
    )
    market_counts_text, market_next_text = _market_discovery_slack_summary(market)
    timing_text = _heartbeat_timing_summary(metrics)
    serious_rows, serious_ids = _slack_rows_from_report(
        market,
        limit=HEARTBEAT_SLACK_REPORT_LIMIT,
    )
    serious_near_misses = useful_near_misses(
        market.get("closest") if isinstance(market.get("closest"), list) else [],
        min_profit=50.0,
        limit=HEARTBEAT_SLACK_REPORT_LIMIT,
    )
    serious_miss_rows, _serious_miss_ids = _slack_rows_from_report(
        {"closest": serious_near_misses},
        item_key="closest",
        exclude_external_ids=serious_ids,
        limit=HEARTBEAT_SLACK_REPORT_LIMIT,
    )
    punt_rows, punt_ids = _slack_rows_from_report(
        punt_report,
        exclude_external_ids=serious_ids,
        limit=HEARTBEAT_SLACK_REPORT_LIMIT,
    )
    punt_near_misses = useful_near_misses(
        punt_report.get("closest") if isinstance(punt_report.get("closest"), list) else [],
        min_profit=25.0,
        limit=HEARTBEAT_SLACK_REPORT_LIMIT,
    )
    punt_miss_rows, _punt_miss_ids = _slack_rows_from_report(
        {"closest": punt_near_misses},
        item_key="closest",
        exclude_external_ids=serious_ids.union(punt_ids),
        limit=HEARTBEAT_SLACK_REPORT_LIMIT,
    )
    favourite_rows = _slack_favourite_rows(
        favourites,
        limit=HEARTBEAT_SLACK_REPORT_LIMIT,
    )

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
                _slack_field("API buckets today", api_summary),
                _slack_field(
                    "Confirmed comp keys",
                    f"{_fmt_int(comps.get('comp_keys'))} ({_fmt_int(comps.get('comp_samples'))} samples)",
                ),
                _slack_field(
                    "Live 3+ comp coverage",
                    f"{_fmt_pct(coverage_pct)} ({_fmt_int(live_with_3plus)}/{_fmt_int(live_total)})",
                ),
                _slack_field("Scheduler", f"{scheduler_status}: {scheduler_message}"),
                _slack_field("Metrics timing", timing_text),
            ],
        },
        {
            "type": "section",
            "fields": [
                _slack_field("Buy signals", market_counts_text),
                _slack_field("Data next", market_next_text),
            ],
        },
        _slack_listing_section(
            "GBP 50 Profit Candidates",
            serious_rows,
            "No live candidates cleared the GBP 50 gate.",
        ),
        _slack_listing_section(
            "GBP 25 Profit Candidates",
            punt_rows,
            "No live candidates cleared the GBP 25 gate.",
        ),
        _slack_listing_section(
            "GBP 50 Closest Misses",
            serious_miss_rows,
            "No actionable near misses at or below max buy for the GBP 50 gate.",
        ),
        _slack_listing_section(
            "GBP 25 Closest Misses",
            punt_miss_rows,
            "No actionable near misses at or below max buy for the GBP 25 gate.",
        ),
        _slack_listing_section(
            "Favourites",
            favourite_rows,
            "No favourites yet.",
        ),
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
    api_accounting = metrics.get("api_accounting") or {}
    api_bucket_summary = _api_bucket_summary(api_accounting)
    blocked = state.get("blocked_services") or []
    backfill_activity = metrics.get("backfill_activity") or {}
    roi = metrics["high_risk_roi"]
    serious_report = metrics.get("market_discovery") or {}
    punt_report = metrics.get("market_discovery_punt") or {}
    pokemon_report = metrics.get("market_discovery_pokemon") or {}
    model_key_inventory = metrics.get("model_key_inventory") or []
    favourites = metrics.get("listing_favourites") or []

    unknown_unfinalized = int(counts.get("unknown_unfinalized") or 0)
    unknown_backfill_due = (
        int(counts.get("unknown_backfill_due"))
        if counts.get("unknown_backfill_due") is not None
        else unknown_unfinalized
    )
    live_total = int(coverage.get("live_total") or 0)
    live_with_3plus = int(coverage.get("live_with_3plus_comps") or 0)
    coverage_pct = (live_with_3plus / live_total * 100.0) if live_total else 0.0

    health_tone = "ok" if not blocked else "bad"
    health_label = "Healthy" if not blocked else "Blocked"
    if unknown_backfill_due > 0 or coverage_pct < 25:
        health_tone = "warn" if not blocked else "bad"
        health_label = "Needs attention" if not blocked else "Blocked"

    serious_rows, serious_ids = _potential_rows_for_email(
        serious_report,
        limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
    )
    serious_near_misses = useful_near_misses(
        serious_report.get("closest") if isinstance(serious_report.get("closest"), list) else [],
        min_profit=50.0,
        limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
    )
    serious_miss_rows, _serious_miss_ids = _potential_rows_for_email(
        {"closest": serious_near_misses},
        item_key="closest",
        exclude_external_ids=serious_ids,
        limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
        empty_message="No actionable near misses at or below max buy for the GBP 50 gate.",
    )
    punt_rows, _punt_ids = _potential_rows_for_email(
        punt_report,
        exclude_external_ids=serious_ids,
        limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
    )
    pokemon_rows, _pokemon_ids = _potential_rows_for_email(
        pokemon_report,
        limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
        min_comps=HEARTBEAT_POKEMON_MIN_COMPS,
        empty_message=(
            "No live Pokemon candidates cleared the GBP "
            f"{HEARTBEAT_POKEMON_PROFIT_GBP:.0f} gate."
        ),
    )
    punt_near_misses = useful_near_misses(
        punt_report.get("closest") if isinstance(punt_report.get("closest"), list) else [],
        min_profit=25.0,
        limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
    )
    punt_miss_rows, _punt_miss_ids = _potential_rows_for_email(
        {"closest": punt_near_misses},
        item_key="closest",
        exclude_external_ids=serious_ids.union(_punt_ids),
        limit=HEARTBEAT_EMAIL_REPORT_LIMIT,
        empty_message="No actionable near misses at or below max buy for the GBP 25 gate.",
    )
    favourite_rows = _favourite_rows_for_email(favourites)

    lead = (
        "Pipeline is running from live PostgreSQL data. Backfill is incremental; "
        "Buy Desk readiness depends on active-niche comp quality, model-key repair, "
        "and guardrails, not the full unknown listing queue."
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
        {_metric_card("API today", _fmt_int(api_total), api_bucket_summary, "neutral")}
      </tr></table>
      <table role="presentation" cellpadding="0" cellspacing="0" width="100%"><tr>
        {_metric_card("Final prices", _fmt_int(counts.get("final_prices")), "confirmed sold evidence rows", "good")}
        {_metric_card("Unknown queue", _fmt_int(unknown_backfill_due), "past-end rows for Trading resolution", "warn" if unknown_backfill_due else "good")}
        {_metric_card("Backfill today", _fmt_int(backfill_activity.get("checked_today")), f"{_fmt_int(backfill_activity.get('sold_today'))} sold confirmed", "neutral")}
        {_metric_card("Current ROI rows", _fmt_int(roi.get("live_roi_gt_15")), f"{_fmt_int(roi.get('live_roi_gt_100'))} above 100% ROI", "warn" if int(roi.get("live_roi_gt_100") or 0) else "neutral")}
      </tr></table>

      {_section("How To Read Listing Rows", _listing_column_explainer())}

      {_section("GBP 50 Profit Candidates", _table(["Source", "Listing", "Type", "Ends", "Current", "Max buy", "Profit", "Sold median", "Comps / grade", "Next", "Review"], serious_rows))}

      {_section("GBP 25 Profit Candidates", _table(["Source", "Listing", "Type", "Ends", "Current", "Max buy", "Profit", "Sold median", "Comps / grade", "Next", "Review"], punt_rows))}

      {_section(f"Pokemon GBP {HEARTBEAT_POKEMON_PROFIT_GBP:.0f} Profit Candidates", _table(["Source", "Listing", "Type", "Ends", "Current", "Max buy", "Profit", "Sold median", "Comps / grade", "Next", "Review"], pokemon_rows))}

      {_section("GBP 50 Closest Misses", _table(["Source", "Listing", "Type", "Ends", "Current", "Max buy", "Profit", "Sold median", "Comps / grade", "Next", "Review"], serious_miss_rows))}

      {_section("GBP 25 Closest Misses", _table(["Source", "Listing", "Type", "Ends", "Current", "Max buy", "Profit", "Sold median", "Comps / grade", "Next", "Review"], punt_miss_rows))}

      {_section("Favourites", _table(["Source", "Listing", "Status", "Current", "Model key", "Review"], favourite_rows))}

      {_section("Model Keys By Market", _model_key_inventory_for_email(model_key_inventory))}
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

    result = send_heartbeat_email_now(state=state, record_sent=True, now=now)
    state["heartbeat_email_out"] = result
    if result.get("status") == "sent":
        logger.info("[main] hourly heartbeat email sent to %s", ", ".join(result.get("sent_to") or []))
    else:
        logger.warning("[main] hourly heartbeat email failed: %s", result.get("error"))

    return state


def send_heartbeat_email_now(
    *,
    state: MainState | None = None,
    recipients: Sequence[str] | None = None,
    record_sent: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Send the current heartbeat email immediately, bypassing hourly throttle."""

    state = state or {}
    now = now or datetime.now(timezone.utc)
    metrics = _metrics_for_heartbeat(state)
    blocked = state.get("blocked_services") or []
    subject_status = "blocked" if blocked else "heartbeat"
    subject = (
        f"[GhostFrog] Listing pipeline {subject_status} - "
        f"{metrics.get('api_total', 0):,} API calls today"
    )
    body = _build_heartbeat_email_html(state, metrics, now)
    target_recipients = [email.strip() for email in (recipients or HEARTBEAT_EMAIL_RECIPIENTS) if email.strip()]

    sent_to: list[str] = []
    try:
        for recipient in target_recipients:
            send_email(subject=subject, body=body, to_addr=recipient, is_html=True)
            sent_to.append(recipient)

        if record_sent:
            _set_last_heartbeat_email_sent(now)
        return {
            "status": "sent",
            "sent_to": sent_to,
            "sent_at": now.isoformat(),
            "subject": subject,
        }
    except Exception as exc:
        return {
            "status": "failed",
            "error": str(exc),
            "sent_to": sent_to,
            "subject": subject,
        }


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

    metrics = _metrics_for_heartbeat(state)
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
                "ebay_base_seller_items_v1",
            ],
        ),
    )
    g.add_node(
        "pph",
        run_subgraph(
            "pph",
            build_pph,
            services=["ebay_base_fetch_items_by_id_v1"],
        ),
    )
    g.add_node(
        "comps",
        run_subgraph(
            "comps",
            build_comps,
            services=[],
        ),
    )
    g.add_node(
        "attributes",
        run_subgraph(
            "attributes",
            build_attributes,
            services=["ebay_attributes_v1"],
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
    g.add_edge("roi", "new")
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
