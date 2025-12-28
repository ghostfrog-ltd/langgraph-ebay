from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Iterable, Optional, TypedDict, Any, cast

from langgraph.graph import StateGraph, END

from utils.logger import get_logger
from utils.db_schema import connection
from utils.emailer import send_email
from utils.timez import now_utc, to_aware_utc

logger = get_logger(__name__)

ALERT_NAME = "alert_new_listings_digest"

WINDOW_MINUTES = 60
MAX_ITEMS = 50
SOURCES_FILTER: Optional[Iterable[str]] = None
ASSUME_PENNIES = False
MAX_BODY_CHARS = 12000
FIRST_RUN_LOOKBACK_HOURS = 24  # or 168 for a full week

TO_EMAIL = "info@ghostfrog.co.uk"


# ----------------------------
# DB helpers (unchanged logic)
# ----------------------------
def _get_last_sent_at(cur) -> datetime | None:
    cur.execute("SELECT last_sent_at FROM alert_state WHERE name=%s", (ALERT_NAME,))
    row = cur.fetchone()
    return to_aware_utc(row[0]) if row and row[0] else None


def _set_last_sent_at(cur, ts: datetime) -> None:
    cur.execute(
        """
        INSERT INTO alert_state (name, last_sent_at)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET last_sent_at=EXCLUDED.last_sent_at
        """,
        (ALERT_NAME, to_aware_utc(ts)),
    )


# ----------------------------
# URL helpers (unchanged logic)
# ----------------------------
def _extract_numeric_item_id(raw_id: str | None) -> str | None:
    """
    eBay Browse API itemIds often look like 'v1|123456789012|0'.
    We only want the numeric middle bit for a public /itm/ URL.

    If it's already just digits, keep it. If it's None or weird, return None.
    """
    if not raw_id:
        return None

    # common case: v1|123456789012|0
    if "|" in raw_id:
        parts = raw_id.split("|")
        # usually parts[1] is the numeric ID
        for p in parts:
            if p.isdigit():
                return p

    # fallback: if the whole thing is digits already
    if raw_id.isdigit():
        return raw_id

    # couldn't get anything clean
    return None


def _build_uk_url(raw_external_id: str | None) -> str:
    """
    Force eBay UK domain so we land on GBP / UK context instead of USD.
    We ignore whatever 'url' the API gave us (which is usually .com).
    """
    numeric_id = _extract_numeric_item_id(raw_external_id)
    if not numeric_id:
        # fallback: just return something so the email doesn't explode
        return "https://www.ebay.co.uk/"

    return f"https://www.ebay.co.uk/itm/{numeric_id}"


# ----------------------------
# Query + formatting (unchanged logic)
# ----------------------------
def _fetch_new_listings(cur, cutoff: datetime):
    """
    Grab recent listings since cutoff.
    We now also select external_id so we can build a .co.uk URL ourselves.
    """
    cutoff = to_aware_utc(cutoff)

    if SOURCES_FILTER:
        cur.execute(
            f"""
            SELECT
                source,
                title,
                price_current,
                url,
                external_id,
                first_seen
            FROM auction_listings
            WHERE first_seen > %s
              AND source = ANY(%s)
            ORDER BY first_seen ASC
            LIMIT {MAX_ITEMS}
            """,
            (cutoff, list(SOURCES_FILTER)),
        )
    else:
        cur.execute(
            f"""
            SELECT
                source,
                title,
                price_current,
                url,
                external_id,
                first_seen
            FROM auction_listings
            WHERE first_seen > %s
            ORDER BY first_seen ASC
            LIMIT {MAX_ITEMS}
            """,
            (cutoff,),
        )

    return cur.fetchall()


def _format_money(v) -> str:
    if v is None:
        return "£—"
    try:
        return f"£{(int(v) / 100):.2f}" if ASSUME_PENNIES else f"£{float(v):.2f}"
    except Exception:
        return f"£{v}"


# ----------------------------
# LangGraph state + graph
# ----------------------------
class NewListingsState(TypedDict, total=False):
    # inputs / shared
    now: datetime

    # db/watermark
    last_sent: datetime | None
    cutoff: datetime
    newest_seen: datetime

    # data
    rows: list[tuple[Any, ...]]
    by_source_count: dict[str, int]
    lines_html: list[str]

    # email
    subject: str
    body_html: str

    # control
    should_send: bool
    emailed_count: int


def _node_init(state: NewListingsState) -> NewListingsState:
    conn = connection
    cur = conn.cursor()

    # try to force UTC for comparisons / ISO timestamps
    try:
        cur.execute("SET TIME ZONE 'UTC'")
    except Exception:
        pass

    now_aware = to_aware_utc(state.get("now") or now_utc())
    last_sent = _get_last_sent_at(cur)

    # first run looks back a big window (24h default)
    default_cut = (
        now_aware - timedelta(hours=FIRST_RUN_LOOKBACK_HOURS)
        if last_sent is None
        else (now_aware - timedelta(minutes=WINDOW_MINUTES))
    )

    # cutoff = later of (default_cut, last_sent), unless no last_sent at all
    cutoff = max(default_cut, last_sent) if last_sent else default_cut

    return {
        "now": now_aware,
        "last_sent": last_sent,
        "cutoff": cutoff,
        "newest_seen": cutoff,  # will be advanced during build
        "by_source_count": {},
        "lines_html": [],
        "rows": [],
        "should_send": False,
        "emailed_count": 0,
    }


def _node_fetch(state: NewListingsState) -> NewListingsState:
    conn = connection
    cur = conn.cursor()

    cutoff = to_aware_utc(state["cutoff"])
    rows = _fetch_new_listings(cur, cutoff)

    if not rows:
        # nothing to do this run
        return {"rows": [], "should_send": False, "emailed_count": 0}

    return {"rows": rows, "should_send": True}


def _node_build_email(state: NewListingsState) -> NewListingsState:
    rows = state.get("rows") or []
    cutoff = to_aware_utc(state["cutoff"])

    by_source_count: dict[str, int] = {}
    newest_seen = cutoff
    lines_html: list[str] = []

    # rows are:
    #   (source, title, price_current, url, external_id, first_seen)
    for source, title, price, _raw_url, external_id, first_seen in rows:
        first_seen_aware = to_aware_utc(first_seen)

        by_source_count[source] = by_source_count.get(source, 0) + 1

        # watermark so we don't resend the same stuff next run
        if first_seen_aware and first_seen_aware > newest_seen:
            newest_seen = first_seen_aware

        safe_title = (title or "").strip().replace("\n", " ")

        # ignore stored URL; always generate a UK URL from the eBay item ID
        uk_url = _build_uk_url(cast(Optional[str], external_id))

        lines_html.append(
            (
                '<li style="margin-bottom:8px;">'
                f'<strong>[{source}]</strong> '
                f'<a href="{uk_url}" target="_blank" '
                'style="color:#0b65c2;text-decoration:none;font-weight:600;">'
                f"{safe_title}</a> — {_format_money(price)}"
                "</li>"
            )
        )

    subject = "🕹 New listings ({}) — {}".format(
        len(rows),
        ", ".join(f"{s}:{c}" for s, c in sorted(by_source_count.items())),
    )

    body_html = f"""
    <div style="font-family:system-ui,Arial,sans-serif;
                font-size:14px;
                line-height:1.45;
                color:#111;">
      <p style="margin:0 0 12px 0;">
        New listings since {cutoff.isoformat()}:
      </p>
      <ul style="margin:0 0 16px 20px;padding:0;">
        {''.join(lines_html)}
      </ul>
      <p style="margin:0;font-size:12px;color:#666;">
        (Showing up to {MAX_ITEMS} this run; older items will follow next.)
      </p>
    </div>
    """

    if len(body_html) > MAX_BODY_CHARS:
        body_html = (
                body_html[:MAX_BODY_CHARS]
                + '<p style="font-size:12px;color:#666;">…(truncated)</p>'
        )

    return {
        "by_source_count": by_source_count,
        "newest_seen": newest_seen,
        "lines_html": lines_html,
        "subject": subject,
        "body_html": body_html,
    }


def _node_send_and_persist(state: NewListingsState) -> NewListingsState:
    conn = connection
    cur = conn.cursor()

    subject = state["subject"]
    body_html = state["body_html"]
    newest_seen = to_aware_utc(state["newest_seen"])
    rows = state.get("rows") or []

    try:
        # IMPORTANT: match your emailer signature
        send_email(
            subject=subject,
            body=body_html,
            to_addr=TO_EMAIL,
            is_html=True,
        )

        _set_last_sent_at(cur, newest_seen)
        conn.commit()

        logger.info(
            "[alert_new_listings] emailed %d items; watermark -> %s",
            len(rows),
            newest_seen.isoformat(),
        )

        return {"emailed_count": len(rows)}

    except Exception as e:
        conn.rollback()
        logger.error("[alert_new_listings] failed to send or persist state: %s", e)
        # keep original behaviour: fail silently (no re-raise), but record 0
        return {"emailed_count": 0}


def _route_after_fetch(state: NewListingsState) -> str:
    return "build_email" if state.get("should_send") else "end"


def build_graph():
    g = StateGraph(NewListingsState)

    g.add_node("init", _node_init)
    g.add_node("fetch", _node_fetch)
    g.add_node("build_email", _node_build_email)
    g.add_node("send_and_persist", _node_send_and_persist)

    g.set_entry_point("init")
    g.add_edge("init", "fetch")

    g.add_conditional_edges(
        "fetch",
        _route_after_fetch,
        {
            "build_email": "build_email",
            "end": END,
        },
    )

    g.add_edge("build_email", "send_and_persist")
    g.add_edge("send_and_persist", END)

    return g.compile()


def save_graph_diagram(path: str = "new_graph.mmd") -> None:
    graph = build_graph()
    g = graph.get_graph()

    # Mermaid text (works everywhere)
    mermaid = g.draw_mermaid()
    with open(path, "w", encoding="utf-8") as f:
        f.write(mermaid)


def run() -> None:
    """
    Keep behaviour the same as before:
    - if no rows: return (no email, no state update)
    - if rows: email + watermark update
    """
    graph = build_graph()
    out = graph.invoke({"now": now_utc()})

    # preserve old behaviour: do nothing when no rows
    if int(out.get("emailed_count", 0) or 0) <= 0:
        return
    return
