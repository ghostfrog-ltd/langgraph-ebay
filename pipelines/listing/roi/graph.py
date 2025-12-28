from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple, TypedDict
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime, timezone, timedelta

from psycopg2.extras import RealDictCursor

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
import utils.db_schema as schema
from utils.emailer import send_email

logger = get_logger(__name__)

# --------------------------------
# Tunable thresholds / assumptions
# --------------------------------
MIN_PROFIT_GBP: float = 50.0  # minimum £ profit you care about
MIN_ROI: float = 0.25  # minimum ROI (0.25 = 25%)
FEE_RATE: float = 0.13  # assumed selling fee rate on resale
INBOUND_SHIP_DEFAULT_GBP: float = 0.0
OUTBOUND_SHIP_DEFAULT_GBP: float = 7.0

# Per-source overrides (optional; e.g. consoles ship cheaper)
PER_SOURCE: Dict[str, Dict[str, float]] = {
    # "ebay-consoles": {
    #     "min_profit": 50.0,
    #     "min_roi": 0.25,
    #     "outbound_ship": 6.0,
    #     "fee_rate": 0.13,
    # },
}

# --------------------------------
# Milestone / siren behaviour
# --------------------------------

# Bucket size for milestone alerts (25% steps → bucket_1, bucket_2, ...)
BUCKET_STEP: float = 0.25  # 0.25 = 25% ROI per bucket

# "NEW insane item" alert: first time we see something this good
NEW_HIGH_ROI: float = 3.0  # 3.0 = 300% ROI
NEW_HIGH_PROFIT_GBP: float = 100.0  # at least £100 profit

# Last-hour "spam me" window
ENDGAME_WINDOW: timedelta = timedelta(hours=1)
ENDGAME_MIN_ROI: float = 0.25  # only spam if still a decent deal
ENDGAME_MIN_PROFIT_GBP: float = 50.0

# Siren cooldown: at most one siren email per listing per 5 minutes
SIREN_COOLDOWN: timedelta = timedelta(minutes=5)

# --------------------------------
# Alert / email behaviour
# --------------------------------
RECORD_ALERTS: bool = True
SEND_EMAIL_DIGEST: bool = True

ALERT_NAME: str = "roi_listings_digest"  # used in alert_state to track last-sent
TO_EMAIL: str = "info@ghostfrog.co.uk"
MAX_EMAIL_ITEMS: int = 20  # cap items in a single email
EMAIL_COOLDOWN = timedelta(minutes=30)  # don't email more often than this

# --------------------------------
# Grade weightings (relative value)
# --------------------------------
GRADE_WEIGHTS: Dict[str, float] = {
    "A": 1.00,  # like new
    "B": 0.85,  # standard used
    "C": 0.70,  # rough but working
    "D": 0.50,  # faulty / spares
    "b": 0.80,  # bikes Cat N baseline
}


# ============================================================
# Time helpers
# ============================================================
def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_aware_utc(dt: Optional[datetime]) -> Optional[datetime]:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _humanise_time_left(end_time: Optional[datetime]) -> str:
    if end_time is None:
        return "unknown"

    now = _now_utc()
    delta = end_time - now
    total_minutes = int(delta.total_seconds() // 60)

    if total_minutes <= 0:
        return "expired"
    if total_minutes < 60:
        return f"{total_minutes} mins"

    hours = total_minutes // 60
    mins = total_minutes % 60
    if mins == 0:
        return f"{hours}h"
    return f"{hours}h {mins}m"


# ============================================================
# Data model
# ============================================================
@dataclass
class Opportunity:
    source: str
    external_id: str
    title: str
    url: str
    model_key: Optional[str]
    comps_samples: int
    comps_median: float
    purchase_cost: float
    outbound_ship: float
    fees: float
    profit: float
    roi: float
    end_time: Optional[datetime] = None
    time_left_s: Optional[int] = None

    def as_log(self) -> str:
        return (
            f"[ROI] {self.title[:80]} "
            f"| buy £{self.purchase_cost:.2f} → sell £{self.comps_median:.2f} "
            f"| fees £{self.fees:.2f} | ship £{self.outbound_ship:.2f} "
            f"| PROFIT £{self.profit:.2f} ({self.roi * 100:.1f}% ROI) "
            f"| comps n={self.comps_samples} | {self.url}"
        )


# ============================================================
# Pure helpers (no DB touched)
# ============================================================
def _money(v: float) -> float:
    return float(Decimal(v).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _split_model_key_grade(model_key: str) -> tuple[str, Optional[str]]:
    if not model_key:
        return "", None

    s = str(model_key).strip()
    if "_" not in s:
        return s, None

    base, suffix = s.rsplit("_", 1)
    suffix = suffix.strip()
    if suffix in GRADE_WEIGHTS:
        return base, suffix

    return s, None


def _get_comp_with_grade_adjustment(
    model_key: str,
    comps_by_model: Dict[str, Dict[str, Any]],
) -> tuple[Optional[Dict[str, Any]], Optional[float]]:
    if not model_key:
        return None, None

    base, listing_grade = _split_model_key_grade(model_key)

    # 1) Exact match
    comp = comps_by_model.get(model_key)
    comp_key = model_key

    # 2) Fallback: same base, different grade
    if not comp and base:
        grade_search_order = ["A", "B", "C", "D", "b"]
        for g in grade_search_order:
            alt_key = f"{base}_{g}"
            if alt_key == model_key:
                continue
            if alt_key in comps_by_model:
                comp = comps_by_model[alt_key]
                comp_key = alt_key
                break

    if not comp:
        return None, None

    median = float(comp.get("median_final_price") or 0.0)
    if median <= 0.0:
        return None, None

    _, comp_grade = _split_model_key_grade(comp_key)

    if (
        listing_grade
        and comp_grade
        and listing_grade in GRADE_WEIGHTS
        and comp_grade in GRADE_WEIGHTS
    ):
        factor = GRADE_WEIGHTS[listing_grade] / GRADE_WEIGHTS[comp_grade]
        median *= factor

    return comp, median


def _is_investible_model_key(model_key: Optional[str]) -> bool:
    if not model_key:
        return False
    mk = str(model_key).strip()
    if not mk:
        return False
    if mk.lower() == "unknown":
        return False
    return True


def _source_cfg(source: Optional[str]) -> Tuple[float, float, float, float]:
    if not source:
        return MIN_PROFIT_GBP, MIN_ROI, OUTBOUND_SHIP_DEFAULT_GBP, FEE_RATE
    cfg = PER_SOURCE.get(source, {})
    return (
        cfg.get("min_profit", MIN_PROFIT_GBP),
        cfg.get("min_roi", MIN_ROI),
        cfg.get("outbound_ship", OUTBOUND_SHIP_DEFAULT_GBP),
        cfg.get("fee_rate", FEE_RATE),
    )


def _estimate_profit(
    *,
    ask_price: float,
    comps_median: float,
    fee_rate: float,
    outbound_ship: float,
    inbound_ship: float,
) -> Tuple[float, float, float]:
    fees = _money(comps_median * fee_rate)
    purchase_cost = ask_price + inbound_ship
    profit = _money(comps_median - fees - outbound_ship - purchase_cost)
    roi = 0.0 if purchase_cost <= 0 else (profit / purchase_cost)
    return fees, profit, roi


# ============================================================
# DB helpers
# ============================================================
def _fetch_active_listings() -> List[Dict[str, Any]]:
    q = """
        SELECT
            source,
            external_id,
            title,
            url,
            model_key,
            COALESCE(price_current, 0) AS price_current,
            status,
            end_time,
            time_left_s
        FROM auction_listings
        WHERE LOWER(status) IN ('active','live','open','ending_soon')
          AND price_current IS NOT NULL
    """
    with schema.connection.cursor(cursor_factory=RealDictCursor) as cur:
        schema.ensure_utc_session(cur)
        cur.execute(q)
        return list(cur.fetchall())


def latest_comps_map() -> Dict[str, Dict[str, Any]]:
    conn = schema.get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            WITH lc AS (
              SELECT DISTINCT ON (model_key)
                     model_key, median_final_price, mean_final_price, samples, computed_at
              FROM comps
              ORDER BY model_key, computed_at DESC
            )
            SELECT * FROM lc
        """
        )
        rows = cur.fetchall()
        return {r["model_key"]: r for r in rows}


def _comps_lookup() -> Dict[str, Dict[str, Any]]:
    try:
        m = latest_comps_map()
        size = len(m)
        sample_keys = ", ".join(list(m.keys())[:5])
        logger.info(
            "[listing.roi] comps loaded %d model_keys (sample: %s)",
            size,
            sample_keys,
        )
        return m
    except Exception as e:
        logger.warning("[listing.roi] latest_comps_map() failed: %s", e)
        return {}


def record_alert(external_id: str, score: float, max_bid: float) -> tuple[bool, int | None]:
    logger.info(
        "[listing.roi.record_alert] ext=%s score=%.2f max_bid=%.2f",
        external_id,
        score,
        max_bid,
    )

    conn = schema.get_connection()
    try:
        with conn.cursor() as cur:
            schema.ensure_utc_session(cur)
            cur.execute(
                """
                INSERT INTO alerts (external_id, score, max_bid, created_at, updated_at)
                VALUES (%s, %s, %s, (now() AT TIME ZONE 'utc'), (now() AT TIME ZONE 'utc'))
                ON CONFLICT (external_id) DO UPDATE
                    SET score = EXCLUDED.score,
                        max_bid = EXCLUDED.max_bid,
                        updated_at = (now() AT TIME ZONE 'utc')
                RETURNING id, (xmax = 0) AS inserted;
                """,
                (external_id, score, max_bid),
            )
            row = cur.fetchone()
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    if not row:
        return False, None

    created_now = bool(row[1])
    return created_now, row[0]


def _maybe_record_alert(op: Opportunity) -> tuple[bool, Optional[int]]:
    if not RECORD_ALERTS:
        return False, None

    try:
        est_cap = op.comps_median - op.fees - op.outbound_ship
        created_now, alert_id = record_alert(
            op.external_id,
            score=float(op.profit),
            max_bid=float(_money(est_cap)),
        )
        return created_now, alert_id
    except Exception as e:
        logger.warning(
            "[listing.roi] record_alert failed for external_id=%s: %s",
            op.external_id,
            e,
        )
        return False, None


def set_alert_last_sent(name: str, when: Optional[datetime] = None) -> None:
    conn = schema.get_connection()
    ts = _to_aware_utc(when) if when is not None else _now_utc()

    try:
        with conn.cursor() as cur:
            schema.ensure_utc_session(cur)
            cur.execute(
                """
                INSERT INTO alert_state (name, last_sent_at)
                VALUES (%s, %s)
                ON CONFLICT (name)
                DO UPDATE SET last_sent_at = EXCLUDED.last_sent_at
                """,
                (name, ts),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise


def get_alert_last_sent(name: str) -> Optional[datetime]:
    with schema.get_connection().cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute("SELECT last_sent_at FROM alert_state WHERE name=%s", (name,))
        row = cur.fetchone()
        return row[0] if row else None


# ============================================================
# Email helpers
# ============================================================
def _send_email_digest(new_ops: List[Opportunity]) -> None:
    if not new_ops:
        return

    subject = (
        f"🐸 ROI Listings: {len(new_ops)} new high-ROI deals "
        f"(≥ £{MIN_PROFIT_GBP:.0f})"
    )

    rows_html: List[str] = []
    for op in new_ops[:MAX_EMAIL_ITEMS]:
        rows_html.append(
            (
                '<p style="margin-bottom:12px;font-family:system-ui,Arial,sans-serif;'
                'font-size:14px;line-height:1.4;">'
                f'<a href="{op.url}" '
                'style="color:#0b65c2;text-decoration:none;font-weight:600;">'
                f'{op.title}</a><br>'
                f'Buy £{op.purchase_cost:.2f} → Sell £{op.comps_median:.2f} '
                f'| Fees £{op.fees:.2f} | Ship £{op.outbound_ship:.2f} '
                f'| <strong>Profit £{op.profit:.2f}</strong> '
                f'({op.roi * 100:.0f}% ROI) '
                f'| comps n={op.comps_samples}'
                '</p>'
            )
        )

    if len(new_ops) > MAX_EMAIL_ITEMS:
        rows_html.append(
            f'<p style="font-family:system-ui,Arial,sans-serif;'
            f'font-size:13px;color:#666;">... and {len(new_ops) - MAX_EMAIL_ITEMS} more.</p>'
        )

    body_html = (
        '<div style="font-family:system-ui,Arial,sans-serif;'
        'color:#111;font-size:14px;line-height:1.45;">'
        '<h2 style="margin:0 0 16px;font-size:16px;line-height:1.3;">'
        "High-ROI listings 🐸</h2>"
        + "".join(rows_html)
        + "</div>"
    )

    try:
        send_email(subject=subject, body=body_html, to_addr=TO_EMAIL, is_html=True)
        set_alert_last_sent(ALERT_NAME)
        logger.info("[listing.roi] email sent to %s with %d items", TO_EMAIL, len(new_ops))
    except Exception as e:
        logger.error("[listing.roi] email send failed: %s", e)


# ============================================================
# ROI snapshots + marker helpers
# ============================================================
def _ensure_support_tables(cur) -> None:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS roi_snapshots (
            id BIGSERIAL PRIMARY KEY,
            external_id TEXT NOT NULL,
            source TEXT NOT NULL,
            model_key TEXT,
            current_price NUMERIC(12,2) NOT NULL,
            roi_estimate NUMERIC(8,4) NOT NULL,
            profit_estimate NUMERIC(12,2) NOT NULL,
            ends_at TIMESTAMPTZ,
            time_left_s INTEGER,
            created_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS roi_alert_markers (
            external_id TEXT NOT NULL,
            marker TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
            PRIMARY KEY (external_id, marker)
        )
        """
    )


def _record_roi_snapshot(cur, op: Opportunity) -> None:
    cur.execute(
        """
        INSERT INTO roi_snapshots (
            external_id,
            source,
            model_key,
            current_price,
            roi_estimate,
            profit_estimate,
            ends_at,
            time_left_s
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            op.external_id,
            op.source,
            op.model_key,
            op.purchase_cost,
            op.roi,
            op.profit,
            _to_aware_utc(op.end_time) if op.end_time else None,
            op.time_left_s,
        ),
    )


def _marker_last_created_at(cur, external_id: str, marker: str) -> Optional[datetime]:
    cur.execute(
        """
        SELECT created_at
        FROM roi_alert_markers
        WHERE external_id = %s AND marker = %s
        LIMIT 1
        """,
        (external_id, marker),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _marker_exists(cur, external_id: str, marker: str) -> bool:
    cur.execute(
        "SELECT 1 FROM roi_alert_markers WHERE external_id=%s AND marker=%s",
        (external_id, marker),
    )
    return cur.fetchone() is not None


def _insert_marker(cur, external_id: str, marker: str) -> None:
    cur.execute(
        """
        INSERT INTO roi_alert_markers (external_id, marker, created_at)
        VALUES (%s, %s, (now() AT TIME ZONE 'utc'))
        ON CONFLICT (external_id, marker) DO UPDATE
            SET created_at = EXCLUDED.created_at
        """,
        (external_id, marker),
    )


def _send_new_high_email(op: Opportunity, time_left_str: str) -> None:
    roi_pct = op.roi * 100.0
    subject = f"🔥 NEW {roi_pct:.0f}% ROI (£{op.profit:.0f}) – {op.title[:80]}"

    body = (
        f"{op.title}\n\n"
        f"Source: {op.source}\n"
        f"URL: {op.url}\n\n"
        f"Current price: £{op.purchase_cost:.2f}\n"
        f"Median comps (resale): £{op.comps_median:.2f}\n"
        f"Fees: £{op.fees:.2f} | Outbound ship: £{op.outbound_ship:.2f}\n"
        f"Estimated profit: £{op.profit:.2f}\n"
        f"ROI: {roi_pct:.1f}%\n"
        f"Time left: {time_left_str}\n"
        f"Model key: {op.model_key or '-'}\n"
        f"Comps samples: {op.comps_samples}\n"
    )

    try:
        send_email(subject=subject, body=body, to_addr=TO_EMAIL, is_html=False)
        logger.info("[listing.roi][new_high] emailed for %s (%s)", op.external_id, subject)
    except Exception as e:
        logger.warning("[listing.roi][new_high] email failed: %s", e)


def _send_bucket_email(op: Opportunity, bucket: int, time_left_str: str) -> None:
    roi_pct = op.roi * 100.0
    bucket_min = bucket * BUCKET_STEP * 100.0
    bucket_max = bucket_min + BUCKET_STEP * 100.0

    subject = f"📈 ROI milestone {roi_pct:.0f}% (£{op.profit:.0f}) – {op.title[:80]}"

    body = (
        f"{op.title}\n\n"
        f"Bucket: {bucket} "
        f"(covers roughly {bucket_min:.0f}%–{bucket_max:.0f}% ROI steps of {BUCKET_STEP * 100:.0f}%)\n"
        f"Source: {op.source}\n"
        f"URL: {op.url}\n\n"
        f"Current price: £{op.purchase_cost:.2f}\n"
        f"Median comps (resale): £{op.comps_median:.2f}\n"
        f"Fees: £{op.fees:.2f} | Outbound ship: £{op.outbound_ship:.2f}\n"
        f"Estimated profit: £{op.profit:.2f}\n"
        f"ROI: {roi_pct:.1f}%\n"
        f"Time left: {time_left_str}\n"
        f"Model key: {op.model_key or '-'}\n"
        f"Comps samples: {op.comps_samples}\n"
    )

    try:
        send_email(subject=subject, body=body, to_addr=TO_EMAIL, is_html=False)
        logger.info("[listing.roi][bucket] emailed bucket_%d for %s", bucket, op.external_id)
    except Exception as e:
        logger.warning("[listing.roi][bucket] email failed: %s", e)


def _send_siren_email(op: Opportunity, time_left_str: str) -> None:
    roi_pct = op.roi * 100.0
    subject = (
        f"🚨 {roi_pct:.0f}% ROI (£{op.profit:.0f}) – {op.title[:80]} – "
        f"ends in {time_left_str} – BID NOW"
    )

    body = (
        f"{op.title}\n\n"
        f"Source: {op.source}\n"
        f"URL: {op.url}\n\n"
        f"Current price: £{op.purchase_cost:.2f}\n"
        f"Median comps (resale): £{op.comps_median:.2f}\n"
        f"Fees: £{op.fees:.2f} | Outbound ship: £{op.outbound_ship:.2f}\n"
        f"Estimated profit: £{op.profit:.2f}\n"
        f"ROI: {roi_pct:.1f}%\n"
        f"Time left: {time_left_str}\n"
        f"Model key: {op.model_key or '-'}\n"
        f"Comps samples: {op.comps_samples}\n"
    )

    try:
        send_email(subject=subject, body=body, to_addr=TO_EMAIL, is_html=False)
        logger.info("[listing.roi][siren] emailed for %s (%s)", op.external_id, subject)
    except Exception as e:
        logger.warning("[listing.roi][siren] email failed: %s", e)


def _process_roi_alerts(opps: List[Opportunity]) -> None:
    if not opps:
        return

    conn = schema.get_connection()
    now = _now_utc()

    try:
        with conn.cursor() as cur:
            schema.ensure_utc_session(cur)
            _ensure_support_tables(cur)

            for op in opps:
                # 1) Snapshot every run
                try:
                    _record_roi_snapshot(cur, op)
                except Exception as e:
                    logger.warning(
                        "[listing.roi] failed to record roi_snapshot for %s: %s",
                        op.external_id,
                        e,
                    )

                end_time = _to_aware_utc(op.end_time) if op.end_time else None

                # Don't email ended listings
                if end_time is not None and end_time <= now:
                    continue

                time_left_str = _humanise_time_left(end_time)
                if time_left_str == "expired":
                    continue

                # 3) NEW insane item (one-shot)
                if op.profit >= NEW_HIGH_PROFIT_GBP and op.roi >= NEW_HIGH_ROI:
                    marker = "new_high"
                    if not _marker_exists(cur, op.external_id, marker):
                        _insert_marker(cur, op.external_id, marker)
                        _send_new_high_email(op, time_left_str)

                # 4) Bucket milestones (25%, 50%, 75%...)
                if op.profit >= MIN_PROFIT_GBP and op.roi >= MIN_ROI:
                    bucket = int(op.roi // BUCKET_STEP)
                    marker = f"bucket_{bucket}"
                    if not _marker_exists(cur, op.external_id, marker):
                        _insert_marker(cur, op.external_id, marker)
                        _send_bucket_email(op, bucket, time_left_str)

                # 5) Last-hour siren, cooldown
                if end_time is not None:
                    if (
                        end_time - now <= ENDGAME_WINDOW
                        and op.profit >= ENDGAME_MIN_PROFIT_GBP
                        and op.roi >= ENDGAME_MIN_ROI
                    ):
                        marker = "siren"
                        last_siren = _marker_last_created_at(cur, op.external_id, marker)

                        allowed = False
                        if last_siren is None:
                            allowed = True
                        else:
                            last_siren_aware = _to_aware_utc(last_siren)
                            if last_siren_aware is None or (now - last_siren_aware) >= SIREN_COOLDOWN:
                                allowed = True

                        if allowed:
                            _insert_marker(cur, op.external_id, marker)
                            _send_siren_email(op, time_left_str)

        conn.commit()
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("[listing.roi] _process_roi_alerts failed: %s", e)


def _update_roi_estimates(opps: List[Opportunity]) -> None:
    if not opps:
        return

    try:
        from engine.core.scoring.snipe import suggest_max_bid as _suggest_max_bid  # type: ignore
        have_snipe = True
    except ModuleNotFoundError:
        have_snipe = False
        logger.warning(
            "[listing.roi] engine.core.scoring.snipe not available; "
            "falling back to max_bid = 0.8 * comps_median"
        )

    conn = schema.get_connection()
    try:
        with conn.cursor() as cur:
            schema.ensure_utc_session(cur)
            rows = []
            for op in opps:
                if have_snipe:
                    max_bid = float(_suggest_max_bid(op.comps_median))
                else:
                    max_bid = float(_money(op.comps_median * 0.8))
                rows.append((float(op.roi), max_bid, op.external_id))

            cur.executemany(
                "UPDATE auction_listings "
                "SET roi_estimate = %s, max_bid = %s "
                "WHERE external_id = %s",
                rows,
            )
        conn.commit()
        logger.info("[listing.roi] updated roi_estimate + max_bid for %d listings", len(opps))
    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.warning("[listing.roi] failed to update roi_estimate/max_bid: %s", e)


# ============================================================
# Core shortlist logic
# ============================================================
def _build_all_opps_for_roi(
    listings: List[Dict[str, Any]],
    comps_by_model: Dict[str, Dict[str, Any]],
) -> List[Opportunity]:
    out: List[Opportunity] = []

    for li in listings:
        source = li.get("source") or ""
        external_id = li.get("external_id") or ""
        title = li.get("title") or ""
        url = li.get("url") or ""
        model_key = li.get("model_key")
        ask_price = float(li.get("price_current") or 0.0)
        end_time = li.get("end_time")
        time_left_s = li.get("time_left_s")

        if not _is_investible_model_key(model_key):
            continue

        comp, comps_median = _get_comp_with_grade_adjustment(str(model_key), comps_by_model)
        if not comp or comps_median is None:
            continue

        comps_samples = int(comp.get("samples") or 0)
        if comps_samples < 3 or comps_median <= 0.0:
            continue

        _min_profit, _min_roi, outbound_ship, fee_rate = _source_cfg(source)

        fees, profit, roi = _estimate_profit(
            ask_price=ask_price,
            comps_median=comps_median,
            fee_rate=fee_rate,
            outbound_ship=outbound_ship,
            inbound_ship=INBOUND_SHIP_DEFAULT_GBP,
        )

        out.append(
            Opportunity(
                source=source,
                external_id=external_id,
                title=title,
                url=url,
                model_key=model_key,
                comps_samples=comps_samples,
                comps_median=_money(comps_median),
                purchase_cost=_money(ask_price + INBOUND_SHIP_DEFAULT_GBP),
                outbound_ship=_money(outbound_ship),
                fees=_money(fees),
                profit=_money(profit),
                roi=roi,
                end_time=_to_aware_utc(end_time) if end_time else None,
                time_left_s=int(time_left_s) if time_left_s is not None else None,
            )
        )

    return out


def _shortlist(
    listings: List[Dict[str, Any]],
    comps_by_model: Dict[str, Dict[str, Any]],
) -> List[Opportunity]:
    out: List[Opportunity] = []

    total = 0
    investible = 0
    with_comps = 0
    with_enough_samples = 0
    passed_thresholds = 0

    for li in listings:
        total += 1

        source = li.get("source") or ""
        external_id = li.get("external_id") or ""
        title = li.get("title") or ""
        url = li.get("url") or ""
        model_key = li.get("model_key")
        ask_price = float(li.get("price_current") or 0.0)
        end_time = li.get("end_time")
        time_left_s = li.get("time_left_s")

        if not _is_investible_model_key(model_key):
            continue
        investible += 1

        comp, comps_median = _get_comp_with_grade_adjustment(str(model_key), comps_by_model)
        if not comp or comps_median is None:
            continue
        with_comps += 1

        comps_samples = int(comp.get("samples") or 0)
        if comps_samples < 3 or comps_median <= 0.0:
            continue
        with_enough_samples += 1

        min_profit, min_roi, outbound_ship, fee_rate = _source_cfg(source)
        fees, profit, roi = _estimate_profit(
            ask_price=ask_price,
            comps_median=comps_median,
            fee_rate=fee_rate,
            outbound_ship=outbound_ship,
            inbound_ship=INBOUND_SHIP_DEFAULT_GBP,
        )

        if profit >= min_profit and roi >= min_roi:
            passed_thresholds += 1
            out.append(
                Opportunity(
                    source=source,
                    external_id=external_id,
                    title=title,
                    url=url,
                    model_key=model_key,
                    comps_samples=comps_samples,
                    comps_median=_money(comps_median),
                    purchase_cost=_money(ask_price + INBOUND_SHIP_DEFAULT_GBP),
                    outbound_ship=_money(outbound_ship),
                    fees=_money(fees),
                    profit=_money(profit),
                    roi=roi,
                    end_time=_to_aware_utc(end_time) if end_time else None,
                    time_left_s=int(time_left_s) if time_left_s is not None else None,
                )
            )

    out.sort(key=lambda o: (o.profit, o.roi), reverse=True)

    logger.info(
        "[listing.roi] shortlist filter counts: total=%d investible=%d with_comps=%d with_enough_samples=%d passed_thresholds=%d",
        total,
        investible,
        with_comps,
        with_enough_samples,
        passed_thresholds,
    )

    return out


# ============================================================
# LangGraph wiring
# ============================================================
class ROIState(TypedDict, total=False):
    limit_output: int
    listings: List[Dict[str, Any]]
    comps_by_model: Dict[str, Dict[str, Any]]
    all_for_roi: List[Opportunity]
    opps: List[Opportunity]
    top: List[Opportunity]
    newly_created: List[Opportunity]


def _node_load_listings(state: ROIState) -> ROIState:
    listings = _fetch_active_listings()
    logger.info("[listing.roi] loaded %d active listings", len(listings))
    return {"listings": listings}


def _node_load_comps(state: ROIState) -> ROIState:
    comps_by_model = _comps_lookup()
    return {"comps_by_model": comps_by_model}


def _node_compute_all_for_roi(state: ROIState) -> ROIState:
    listings = state.get("listings") or []
    comps_by_model = state.get("comps_by_model") or {}
    all_for_roi = _build_all_opps_for_roi(listings, comps_by_model)
    logger.info("[listing.roi] built %d opportunities for roi_estimate update", len(all_for_roi))
    return {"all_for_roi": all_for_roi}


def _node_persist_roi_estimates(state: ROIState) -> ROIState:
    all_for_roi = state.get("all_for_roi") or []
    _update_roi_estimates(all_for_roi)
    return {}


def _node_process_roi_alerts(state: ROIState) -> ROIState:
    all_for_roi = state.get("all_for_roi") or []
    _process_roi_alerts(all_for_roi)
    return {}


def _node_shortlist(state: ROIState) -> ROIState:
    listings = state.get("listings") or []
    comps_by_model = state.get("comps_by_model") or {}
    opps = _shortlist(listings, comps_by_model)
    return {"opps": opps}


def _node_log_top(state: ROIState) -> ROIState:
    opps = state.get("opps") or []
    limit_output = int(state.get("limit_output") or 20)

    if not opps:
        logger.info(
            "[listing.roi] no opportunities ≥ £%.2f / ROI ≥ %.0f%%",
            MIN_PROFIT_GBP,
            MIN_ROI * 100,
        )
        return {"top": []}

    top = opps[:limit_output]
    logger.info("[listing.roi] %d opportunities found (showing %d)", len(opps), len(top))
    for op in top:
        logger.info(op.as_log())

    return {"top": top}


def _node_record_alerts_and_email(state: ROIState) -> ROIState:
    opps = state.get("opps") or []
    if not opps:
        return {"newly_created": []}

    newly_created: List[Opportunity] = []
    if RECORD_ALERTS or SEND_EMAIL_DIGEST:
        for op in opps:
            created_now, _alert_id = _maybe_record_alert(op)
            if created_now:
                newly_created.append(op)

    # Keep the behaviour exactly as your pasted code, even though it looks backwards:
    # it filters to *past* end_time (< now) before emailing.
    if SEND_EMAIL_DIGEST and newly_created:
        now = _now_utc()
        newly_created = [op for op in newly_created if op.end_time is not None and op.end_time < now]

        if not newly_created:
            logger.info("[listing.roi] no newly created opportunities with past end_time to email")
        else:
            last_sent = get_alert_last_sent(ALERT_NAME)
            if last_sent is None:
                _send_email_digest(newly_created)
            else:
                last_sent_aware = _to_aware_utc(last_sent)
                if last_sent_aware is None:
                    _send_email_digest(newly_created)
                else:
                    since = _now_utc() - last_sent_aware
                    if since >= EMAIL_COOLDOWN:
                        _send_email_digest(newly_created)
                    else:
                        logger.info(
                            "[listing.roi] skipping email (cooldown %.0f min not reached)",
                            EMAIL_COOLDOWN.total_seconds() / 60.0,
                        )

    return {"newly_created": newly_created}


def build_graph():
    g = StateGraph(ROIState)

    g.add_node("load_listings", _node_load_listings)
    g.add_node("load_comps", _node_load_comps)
    g.add_node("compute_all_for_roi", _node_compute_all_for_roi)
    g.add_node("persist_roi_estimates", _node_persist_roi_estimates)
    g.add_node("process_roi_alerts", _node_process_roi_alerts)
    g.add_node("shortlist", _node_shortlist)
    g.add_node("log_top", _node_log_top)
    g.add_node("record_alerts_and_email", _node_record_alerts_and_email)

    g.set_entry_point("load_listings")
    g.add_edge("load_listings", "load_comps")
    g.add_edge("load_comps", "compute_all_for_roi")
    g.add_edge("compute_all_for_roi", "persist_roi_estimates")
    g.add_edge("persist_roi_estimates", "process_roi_alerts")
    g.add_edge("process_roi_alerts", "shortlist")
    g.add_edge("shortlist", "log_top")
    g.add_edge("log_top", "record_alerts_and_email")
    g.add_edge("record_alerts_and_email", END)

    return g.compile()


def save_graph_diagram(path: str = "roi_graph.mmd") -> None:
    graph = build_graph()
    g = graph.get_graph()

    # Mermaid text (works everywhere)
    mermaid = g.draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)


# ============================================================
# Public entry points (same behaviour as old module)
# ============================================================
def get_top_roi_opportunities(limit: int = 20) -> List[Opportunity]:
    try:
        listings = _fetch_active_listings()
    except Exception as e:
        logger.error("[listing.roi] fetch active listings failed in get_top_roi_opportunities: %s", e)
        return []

    comps_by_model = _comps_lookup()
    opps = _shortlist(listings, comps_by_model)
    return opps[:limit]


def run(limit_output: int = 20) -> List[Opportunity]:
    """
    Main entry point:
      - same behaviour as the old code, but executed via LangGraph.
    """
    app = build_graph()
    final_state = app.invoke({"limit_output": int(limit_output)})
    return list(final_state.get("opps") or [])
