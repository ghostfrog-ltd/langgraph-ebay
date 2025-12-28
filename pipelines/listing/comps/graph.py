from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, TypedDict, cast

from langgraph.graph import END, StateGraph

from utils.db_schema import ensure_utc_session
from utils import db_connection
from utils.logger import get_logger

logger = get_logger(__name__)
connection = db_connection.connection

# ---------------------------------
# Tunable knobs
# ---------------------------------
# Default logical window; can be overridden at runtime via GF_COMPS_WINDOW_DAYS
COMPS_WINDOW_DAYS: int = 30           # how many days of history to aggregate
COMPS_MIN_INTERVAL_HOURS: int = 6     # minimum time between full recomputes
COMPS_KEEP_PER_KEY: int = 60          # how many snapshots per model_key to retain

# Legacy: we used to bucket NULL model_keys into "unknown".
# We now avoid doing that for comps so we don't contaminate ROI.
NO_KEY_BUCKET: str = "unknown"


# ---------------------------------
# Local DB helpers (UNCHANGED)
# ---------------------------------
def _ensure_utc_session(cur) -> None:
    """Best-effort session timezone to UTC (local helper)."""
    try:
        cur.execute("SET TIME ZONE 'UTC'")
    except Exception:
        # Not fatal; best-effort only
        pass


def _get_last_run() -> Optional[datetime]:
    """
    Look at comps and ask: when did we last compute anything?
    We use MAX(computed_at) as the "last run time".
    """
    sql = "SELECT MAX(computed_at) FROM comps"
    with connection.cursor() as cur:
        _ensure_utc_session(cur)
        cur.execute(sql)
        row = cur.fetchone()
    if not row:
        return None
    return row[0]


def _get_window_days() -> int:
    """
    Resolve the effective window in days, allowing an override via
    the GF_COMPS_WINDOW_DAYS environment variable.

    - If GF_COMPS_WINDOW_DAYS is a positive int, we use that.
    - Otherwise we fall back to COMPS_WINDOW_DAYS.
    """
    env_val = os.getenv("GF_COMPS_WINDOW_DAYS")
    if env_val:
        try:
            days = int(env_val)
            if days > 0:
                return days
        except ValueError:
            logger.warning(
                "[process.comps] invalid GF_COMPS_WINDOW_DAYS=%r (must be positive int); "
                "falling back to COMPS_WINDOW_DAYS=%d",
                env_val,
                COMPS_WINDOW_DAYS,
            )
    return COMPS_WINDOW_DAYS


def _truncate_comps() -> None:
    """
    Completely clear the comps table before recomputing.
    Ensures we always have a single fresh snapshot.
    """
    logger.info("[process.comps] truncating comps table before recompute")
    with connection, connection.cursor() as cur:
        _ensure_utc_session(cur)
        cur.execute("TRUNCATE TABLE comps;")


def _compute_daily_comps(days: Optional[int] = None) -> None:
    """
    Insert new per-model_key stats for the last N days of ended/sold listings.

    - Uses COALESCE(final_price, price_current) as the realized sale price.
    - Includes both 'sold' and 'ended' statuses.
    - Only aggregates rows where model_key is non-null and not 'unknown'
      to avoid contaminating comps with garbage buckets.
    """
    if days is None:
        days = _get_window_days()

    logger.info(
        "[process.comps] computing daily comps for last %s days "
        "(statuses IN ('sold','ended'), model_key NOT NULL/unknown, "
        "price = COALESCE(final_price, price_current))",
        days,
    )

    sql = """
        INSERT INTO comps (model_key, median_final_price, mean_final_price, samples, computed_at)
        SELECT
            model_key,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY COALESCE(final_price, price_current)
            )::numeric AS median_final_price,
            AVG(COALESCE(final_price, price_current))::numeric AS mean_final_price,
            COUNT(*)::int AS samples,
            (now() AT TIME ZONE 'utc') AS computed_at
        FROM auction_listings
        WHERE status IN ('sold', 'ended')
          AND COALESCE(final_price, price_current) IS NOT NULL
          AND end_time >= (now() AT TIME ZONE 'utc' - (%s || ' days')::interval)
          AND model_key IS NOT NULL
          AND LOWER(model_key) <> 'unknown'
        GROUP BY model_key
    """

    with connection, connection.cursor() as cur:
        _ensure_utc_session(cur)
        cur.execute(sql, (str(days),))

    # Debug: how many comps did we just insert?
    with connection.cursor() as cur:
        _ensure_utc_session(cur)
        cur.execute("SELECT COUNT(*) FROM comps;")
        (count_after,) = cur.fetchone()
        logger.info("[process.comps] comps rows after compute_daily_comps = %s", count_after)


def _prune_old_comps(keep_per_key: int = COMPS_KEEP_PER_KEY) -> None:
    """
    Keep only the newest N rows per model_key in comps.
    Stops comps table from growing forever.
    """
    logger.info("[process.comps] pruning old comps (keep_per_key=%s)", keep_per_key)
    sql = """
        WITH ranked AS (
          SELECT model_key, computed_at,
                 ROW_NUMBER() OVER (
                     PARTITION BY model_key
                     ORDER BY computed_at DESC
                 ) AS rn
          FROM comps
        )
        DELETE FROM comps c
        USING ranked r
        WHERE c.model_key = r.model_key
          AND c.computed_at = r.computed_at
          AND r.rn > %s
    """
    with connection, connection.cursor() as cur:
        _ensure_utc_session(cur)
        cur.execute(sql, (keep_per_key,))


def refresh_latest_comps_matview():
    """Refresh the latest_comps materialized view (best-effort)."""
    with connection, connection.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute("REFRESH MATERIALIZED VIEW latest_comps")


# ---------------------------------
# LangGraph wrapper (NEW)
# ---------------------------------
class CompsState(TypedDict, total=False):
    force: bool
    now_utc: datetime
    last_run: Optional[datetime]
    age: timedelta
    should_run: bool
    window_days: int


def _node_decide_should_run(state: CompsState) -> CompsState:
    logger.info("[process.comps] >>> USING NEW COMPS VERSION <<<")

    now_utc = datetime.now(timezone.utc)
    last_run = _get_last_run()
    if last_run:
        age = now_utc - last_run
    else:
        age = timedelta.max  # "never run" => effectively infinite age

    force = bool(state.get("force", False))
    should_run = force or (age >= timedelta(hours=COMPS_MIN_INTERVAL_HOURS))

    if not should_run:
        logger.debug(
            "[process.comps] skip: last_run=%s age=%s (min_interval=%sh)",
            last_run,
            age,
            COMPS_MIN_INTERVAL_HOURS,
        )
        return {
            **state,
            "now_utc": now_utc,
            "last_run": last_run,
            "age": age,
            "should_run": False,
        }

    window_days = _get_window_days()
    logger.info(
        "[process.comps] starting recompute (force=%s, last_run=%s, age=%s, window_days=%s)",
        force,
        last_run,
        age,
        window_days,
    )

    return {
        **state,
        "now_utc": now_utc,
        "last_run": last_run,
        "age": age,
        "should_run": True,
        "window_days": window_days,
    }


def _route_after_decide(state: CompsState) -> str:
    return "truncate" if state.get("should_run") else "done"


def _node_truncate(state: CompsState) -> CompsState:
    _truncate_comps()
    return state


def _node_compute(state: CompsState) -> CompsState:
    days = cast(int, state.get("window_days", _get_window_days()))
    _compute_daily_comps(days)
    return state


def _node_prune_best_effort(state: CompsState) -> CompsState:
    try:
        _prune_old_comps(COMPS_KEEP_PER_KEY)
    except Exception as e:
        logger.warning("[process.comps] prune_old_comps failed: %s", e)
    return state


def _node_refresh_matview_best_effort(state: CompsState) -> CompsState:
    try:
        refresh_latest_comps_matview()
    except Exception as e:
        logger.warning("[process.comps] refresh_latest_comps_matview failed: %s", e)
    return state


def _node_done(state: CompsState) -> CompsState:
    # Keep the same final log position as your original run()
    if state.get("should_run"):
        logger.info("[process.comps] done")
    return state


def _build_graph():
    g = StateGraph(CompsState)

    g.add_node("decide", _node_decide_should_run)
    g.add_node("truncate", _node_truncate)
    g.add_node("compute", _node_compute)
    g.add_node("prune", _node_prune_best_effort)
    g.add_node("refresh_matview", _node_refresh_matview_best_effort)
    g.add_node("done", _node_done)

    g.set_entry_point("decide")

    g.add_conditional_edges(
        "decide",
        _route_after_decide,
        {
            "truncate": "truncate",
            "done": "done",
        },
    )

    g.add_edge("truncate", "compute")
    g.add_edge("compute", "prune")
    g.add_edge("prune", "refresh_matview")
    g.add_edge("refresh_matview", "done")
    g.add_edge("done", END)

    return g.compile()

def save_graph_diagram(path: str = "comps_graph.mmd") -> None:
    graph = _build_graph()
    g = graph.get_graph()

    # Mermaid text (works everywhere)
    mermaid = g.draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)

# ---------------------------------
# Public entrypoint (same signature)
# ---------------------------------
def run(force: bool = False) -> None:
    """
    Action entrypoint.

    - If force=False, only runs if last run was >= COMPS_MIN_INTERVAL_HOURS ago
      (or comps is empty).
    - If force=True, always recomputes regardless of last_run timestamp.

    Effective window:
      - COMPS_WINDOW_DAYS by default
      - or GF_COMPS_WINDOW_DAYS env var if set to a positive integer
    """
    try:
        graph = _build_graph()
        graph.invoke({"force": force})
    except Exception as e:
        logger.exception("[process.comps] failed: %s", e)
