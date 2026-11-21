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

COMPS_BLOCKED_TEXT_PATTERNS: tuple[str, ...] = (
    "%for parts%",
    "%parts only%",
    "%parts missing%",
    "%for spares%",
    "%spares only%",
    "%spares or repair%",
    "%spares or repairs%",
    "%spares & repairs%",
    "%spares repairs%",
    "%spares / repair%",
    "%spares/repair%",
    "%repair only%",
    "%for repair%",
    "%needs repair%",
    "%faulty%",
    "%not working%",
    "%not starting%",
    "%dead%",
    "%scrap%",
    "%scraps%",
    "%selling for scraps%",
    "%badly damaged%",
    "%screen damage%",
    "%damaged screen%",
    "%screen is damaged%",
    "%screen badly damaged%",
    "%badly damaged screen%",
    "%broken screen%",
    "%cracked screen%",
    "%broken lcd%",
    "%broken display%",
    "%untested%",
    "%as is%",
    "%as-is%",
    "%read description%",
    "%please read%",
    "%grade d%",
    "%box only%",
    "%empty box%",
    "%empty original box%",
    "%box & inserts%",
    "%box and inserts%",
    "%manual only%",
    "%instructions and stickers only%",
    "%stickers only%",
    "%charger only%",
    "%battery only%",
    "%case only%",
    "%housing only%",
    "%shell only%",
    "%cable only%",
    "%adapter only%",
    "%replacement part%",
    "%logic board%",
    "%logic board missing%",
    "%missing logic board%",
    "%screen for%",
    "%joy-con%",
    "%joy con%",
    "%joycon%",
    "%usb-c cable%",
    "%usb cable%",
    "%usb-a to usb-c%",
    "%stealth case%",
    "%carrying case%",
    "%carrying cases%",
    "%protective case%",
    "%travel case%",
    "%touch id issue%",
    "%touchid issue%",
    "%touch id missing%",
    "%screen issue%",
    "%screen & mic issue%",
    "%mic issue%",
    "%microphone issue%",
    "%job lot%",
    "%joblot%",
    "%unit only%",
    "%no accessories%",
    "%macbook neo%",
)

CONSOLE_COMP_NON_HARDWARE_PATTERNS: tuple[str, ...] = (
    "%code in a box%",
    "%code-in-a-box%",
    "%download code%",
    "%digital code%",
    "%game code%",
    "%software only%",
    "%console game%",
    "%game only%",
    "%game cartridge%",
    "%game card%",
    "%case game holder%",
)

CONSOLE_COMP_ACCESSORY_PATTERNS: tuple[str, ...] = (
    "%wall charger%",
    "%usb-c charger%",
    "%usb c charger%",
    "%charger cable%",
    "%charging cable%",
    "%charging lead%",
    "%charging dock%",
    "%usb-c cable%",
    "%usb c cable%",
    "%cable for%",
    "%wireless controller%",
    "%pro controller%",
    "%controller for%",
    "%gamepad for%",
    "%carry case%",
    "%carrying case%",
    "%protective case%",
    "%messenger bag%",
    "%travel bag%",
    "%hard case%",
    "%case for%",
    "%bag for%",
    "%dock only%",
    "%dock for%",
    "%screen protector%",
    "%sd card%",
    "%memory card%",
    "%stand for%",
    "%grip for%",
    "%replacement part%",
    "%replacement shell%",
    "%replacement case%",
)

CONSOLE_COMP_HARDWARE_TITLE_PATTERN = (
    r"\m(console|system|handheld)\M|"
    r"\m(disc|disk|digital|blu[- ]?ray)\s+edition\M|"
    r"\mwith\s+controller\M|"
    r"\mconsole\s+only\M|"
    r"\moled\s+model\M|"
    r"\m(128|256|500|512|825)\s?gb\M|"
    r"\m(1|2)\s?tb\M"
)

CONSOLE_COMP_HARDWARE_BLOB_PATTERN = r"\m(with|including|incl\.?)\s+(a\s+)?(console|system)\M"

CONSOLE_COMP_ACCESSORY_ATTR_PATTERN = (
    r"\m(usb\s*-?\s*cable|cable|charger|charging\s+dock|controller|gamepad|case|bag|"
    r"dock|screen\s+protector|sd\s+card|memory\s+card|stand|grip|replacement\s+part|"
    r"game|software)\M"
)

CONSOLE_COMP_ACCESSORY_FOR_PATTERN = (
    r"\m(charger|cable|controller|gamepad|case|bag|dock|stand|grip|game|software)\M"
    r".{0,40}\m(for|compatible with)\M"
)

ELECTRONICS_DAMAGE_WORD_PATTERN = (
    r"\m(damaged|damage|broken|cracked|crack|smashed|scratch|scratched|scrathed|"
    r"scratches|dent|dented|chip|chipped)\M"
)

ELECTRONICS_DAMAGE_NEGATION_PATTERN = (
    r"\m(no|not|without|free from)\s+(visible\s+|significant\s+|major\s+)?"
    r"(damage|damaged|crack|cracked|scratch|scratched|scratches|broken|breaks)\M"
)

LEGO_WHOLE_SET_TEXT_PATTERNS: tuple[str, ...] = (
    "%complete set%",
    "%complete lego set%",
    "%full set%",
    "%sealed set%",
    "%new sealed%",
    "%new and sealed%",
    "%brand new sealed%",
)

APPLE_MAC_ALLOWED_CPU_PATTERN = r"-(m[1-9](pro|max|ultra)?|intel|core-i[3579]|core-m[357])(-|_)"

# ---------------------------------
# Tunable knobs
# ---------------------------------
# Default logical window; can be overridden at runtime via GF_COMPS_WINDOW_DAYS
COMPS_WINDOW_DAYS: int = 365           # how many days of history to aggregate
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


def _compute_daily_comps(
    days: Optional[int] = None, *, model_keys: list[str] | None = None,
) -> None:
    """
    Insert new per-model_key stats for the last N days of listings with
    confirmed realized sale prices.

    - Uses final_price only; price_current is not a realized sale price.
    - Includes any terminal status only when final_price is present.
    - Only aggregates rows where model_key is non-null and not 'unknown'
      to avoid contaminating comps with garbage buckets.
    """
    if model_keys == []:
        return
    if days is None:
        days = _get_window_days()

    logger.info(
        "[process.comps] computing daily comps for last %s days "
        "(final_price IS NOT NULL, model_key NOT NULL/unknown)",
        days,
    )

    scope = "AND graded.model_key = ANY(%s)" if model_keys is not None else ""
    sql = f"""
        INSERT INTO comps (model_key, condition_grade, median_final_price, mean_final_price, samples, computed_at)
        SELECT
            graded.model_key,
            graded.effective_condition_grade AS condition_grade,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY graded.final_price)::numeric AS median_final_price,
            AVG(graded.final_price)::numeric AS mean_final_price,
            COUNT(*)::int AS samples,
            (now() AT TIME ZONE 'utc') AS computed_at
        FROM (
            SELECT
                a.*,
                CASE
                    WHEN 'D' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'D'
                    WHEN 'C' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'C'
                    WHEN 'B' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'B'
                    ELSE 'A'
                END AS effective_condition_grade
            FROM auction_listings AS a
            LEFT JOIN latest_listing_visual_conditions AS vc
              ON vc.listing_id = a.id
            LEFT JOIN latest_listing_text_conditions AS tc
              ON tc.listing_id = a.id
        ) AS graded
        WHERE graded.final_price IS NOT NULL
          AND (
              LOWER(graded.model_key) NOT LIKE 'apple-%%'
              OR (
                  graded.source = 'ebay-apple'
                  AND graded.status = 'sold'
                  AND graded.final_price > 0
              )
          )
          AND graded.end_time >= (now() AT TIME ZONE 'utc' - (%s || ' days')::interval)
          AND graded.model_key IS NOT NULL
          AND LOWER(graded.model_key) <> 'unknown'
          AND LOWER(graded.model_key) NOT LIKE 'unknown%%'
          AND LOWER(graded.model_key) NOT LIKE '%%generic%%'
          AND LOWER(graded.model_key) NOT LIKE '%%gen-unk%%'
          AND LOWER(graded.model_key) !~ '_[a-d](-[a-d])?$'
          AND NOT (
              LOWER(graded.model_key) LIKE 'apple-iphone-%%'
              AND LOWER(graded.model_key) !~ '^apple-iphone-(se|x|xr|xs|[0-9]+[es]?)(-|_)'
          )
          AND NOT (
              LOWER(graded.model_key) ~ '^apple-(macbook|imac|mac-mini|mac-studio)'
              AND LOWER(graded.model_key) !~ %s
          )
          AND NOT (
              LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) LIKE ANY(%s)
              OR (
                  graded.source = 'ebay-apple'
                  AND LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) LIKE '%%macbook%%'
                  AND LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) ~ '\\ma[0-9]{{1,2}}\\s*(pro|max|ultra)?\\M'
              )
              OR (
                  graded.source = 'ebay-apple'
                  AND LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) ~ '\\mlocked\\M'
              )
              OR (
                  graded.source = 'ebay-consoles'
                  AND (
                      LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) LIKE ANY(%s)
                      OR (
                          LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) LIKE ANY(%s)
                          AND NOT (
                              LOWER(COALESCE(graded.title, '')) ~ %s
                              OR LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) ~ %s
                          )
                      )
                      OR (
                          LOWER(COALESCE(graded.raw_attrs::text, '')) ~ %s
                          AND NOT (
                              LOWER(COALESCE(graded.title, '')) ~ %s
                              OR LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) ~ %s
                          )
                      )
                      OR (
                          LOWER(COALESCE(graded.title, '')) ~ %s
                          AND NOT (
                              LOWER(COALESCE(graded.title, '')) ~ %s
                              OR LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) ~ %s
                          )
                      )
                  )
              )
              OR (
                  graded.source IN (
                      'ebay-apple',
                      'ebay-samsung',
                      'ebay-consoles',
                      'ebay-actioncams',
                      'ebay-graphics-cards'
                  )
                  AND LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) ~ %s
                  AND LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) !~ %s
              )
              OR (
                  graded.source = 'ebay-lego'
                  AND LOWER(COALESCE(graded.title, '') || ' ' || COALESCE(graded.raw_attrs::text, '')) LIKE '%%castle byers%%'
                  AND LOWER(COALESCE(graded.title, '')) NOT LIKE ALL(%s)
              )
          )
          {scope}
        GROUP BY graded.model_key, graded.effective_condition_grade
        ON CONFLICT (model_key, condition_grade) DO UPDATE SET
            median_final_price = EXCLUDED.median_final_price,
            mean_final_price = EXCLUDED.mean_final_price,
            samples = EXCLUDED.samples,
            computed_at = EXCLUDED.computed_at
    """

    with connection, connection.cursor() as cur:
        _ensure_utc_session(cur)
        if model_keys is not None:
            cur.execute("DELETE FROM comps WHERE model_key = ANY(%s)", (model_keys,))
        cur.execute(
            sql,
            (
                str(days),
                APPLE_MAC_ALLOWED_CPU_PATTERN,
                list(COMPS_BLOCKED_TEXT_PATTERNS),
                list(CONSOLE_COMP_NON_HARDWARE_PATTERNS),
                list(CONSOLE_COMP_ACCESSORY_PATTERNS),
                CONSOLE_COMP_HARDWARE_TITLE_PATTERN,
                CONSOLE_COMP_HARDWARE_BLOB_PATTERN,
                CONSOLE_COMP_ACCESSORY_ATTR_PATTERN,
                CONSOLE_COMP_HARDWARE_TITLE_PATTERN,
                CONSOLE_COMP_HARDWARE_BLOB_PATTERN,
                CONSOLE_COMP_ACCESSORY_FOR_PATTERN,
                CONSOLE_COMP_HARDWARE_TITLE_PATTERN,
                CONSOLE_COMP_HARDWARE_BLOB_PATTERN,
                ELECTRONICS_DAMAGE_WORD_PATTERN,
                ELECTRONICS_DAMAGE_NEGATION_PATTERN,
                list(LEGO_WHOLE_SET_TEXT_PATTERNS),
                *((model_keys,) if model_keys is not None else ()),
            ),
        )

    if model_keys is not None:
        return
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
        # _prune_old_comps(COMPS_KEEP_PER_KEY)
        abc = 'easy'
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


def build_graph():
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
    graph = build_graph()
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
        graph = build_graph()
        graph.invoke({"force": force})
    except Exception as e:
        logger.exception("[process.comps] failed: %s", e)
