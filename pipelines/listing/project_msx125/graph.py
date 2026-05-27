from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Mapping, Optional, TypedDict

from langgraph.graph import END, StateGraph
from psycopg2.extras import Json, RealDictCursor

from utils.db_schema import connection, ensure_utc_session
from utils.emailer import send_email
from utils.logger import get_logger

logger = get_logger(__name__)

ALERT_EMAILS = [
    email.strip()
    for email in os.getenv(
        "GF_MSX125_EMAILS",
        "garyconstable80@gmail.com,info@ghostfrog.co.uk",
    ).split(",")
    if email.strip()
]

SOURCES = ("ebay-honda-msx125", "motomine")
LIVE_STATUSES = ("live", "active", "open", "ending_soon")
MIN_WHOLE_BIKE_PRICE = float(os.getenv("GF_MSX125_MIN_PRICE", "500"))

PART_ONLY_TERMS = (
    "bolt",
    "washer",
    "screen protector",
    "sump",
    "gasket",
    "tail tidy",
    "fender eliminator",
    "tank grip",
    "tank grips",
    "grip",
    "grips",
    "bracket",
    "mount",
    "air box",
    "throttle",
    "caliper",
    "mudguard",
    "visor",
    "lock set",
    "engine",
    "wiring loom",
    "loom",
    "clocks",
    "brake assembly",
    "drive gear",
    "switch",
    "ecu",
    "cdi",
    "tool",
    "socket",
    "kit",
    "seat cowl",
    "fairing",
    "panel",
    "exhaust",
    "indicator",
    "levers",
    "lever",
    "mirror",
    "mirrors",
    "handlebar",
    "tyre",
    "tire",
    "wheel",
    "wheels",
    "brake pads",
    "chain",
    "sprocket",
    "filter",
    "cover",
    "sticker",
    "decal",
    "manual",
    "brochure",
)

WHOLE_BIKE_SIGNALS = (
    "mot",
    "v5",
    "mileage",
    "miles",
    "hpi",
    "registration",
    "registered",
    "runs",
    "starts",
    "spares/repair",
    "spares or repair",
    "non-structural",
    "cat n",
    "cat s",
    "salvage",
)


class Msx125State(TypedDict, total=False):
    candidates_found: int
    emails_sent: int
    results: list[dict[str, Any]]


@dataclass
class Candidate:
    external_id: str
    source: str
    title: str
    url: str
    price_current: float
    status: str
    model_key: Optional[str]
    reasons: list[str]
    raw_attrs: dict[str, Any]


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(_text(item) for item in value if item is not None).strip()
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=True)
    return str(value).strip()


def _blob(attrs: Mapping[str, Any], title: str) -> str:
    return " ".join(
        [
            title,
            _text(attrs.get("Manufacturer")),
            _text(attrs.get("Make")),
            _text(attrs.get("Model")),
            _text(attrs.get("Engine Size")),
            _text(attrs.get("Capacity (cc)")),
            _text(attrs.get("Fuel Type")),
            _text(attrs.get("Description")),
        ]
    ).lower()


def _looks_like_msx125(attrs: Mapping[str, Any], title: str) -> bool:
    blob = _blob(attrs, title)
    return "honda" in blob and any(
        marker in blob
        for marker in (
            "msx125",
            "msx 125",
            "msx-125",
            "grom",
        )
    )


def _looks_part_only(attrs: Mapping[str, Any], title: str) -> bool:
    blob = _blob(attrs, title)
    return any(term in blob for term in PART_ONLY_TERMS)


def _looks_like_whole_bike(
    attrs: Mapping[str, Any],
    title: str,
    source: str,
    price_current: float,
) -> bool:
    if source == "motomine":
        return True

    blob = _blob(attrs, title)
    if any(signal in blob for signal in WHOLE_BIKE_SIGNALS):
        return True

    # Expensive MSX/Grom listings are usually whole bikes; parts/accessories are
    # still filtered by _looks_part_only().
    return price_current >= MIN_WHOLE_BIKE_PRICE


def _candidate_from_row(row: Mapping[str, Any]) -> Optional[Candidate]:
    attrs = row.get("raw_attrs") if isinstance(row.get("raw_attrs"), dict) else {}
    title = _text(row.get("title"))
    source = _text(row.get("source"))
    price_current = float(row.get("price_current") or 0)

    if source not in SOURCES:
        return None
    if not _looks_like_msx125(attrs, title):
        return None
    if price_current <= 0:
        return None
    if not _looks_like_whole_bike(attrs, title, source, price_current):
        return None
    if _looks_part_only(attrs, title):
        return None

    blob = _blob(attrs, title)
    reasons = ["Honda MSX 125 / Grom signal found", "whole-bike listing candidate"]
    if source == "motomine":
        reasons.append("MotoMine salvage/project source")
    if any(term in blob for term in ("spares/repair", "spares or repair", "non-structural", "salvage", "cat n", "cat s")):
        reasons.append("project/salvage wording found")

    return Candidate(
        external_id=_text(row.get("external_id")),
        source=source,
        title=title,
        url=_text(row.get("url")),
        price_current=price_current,
        status=_text(row.get("status")),
        model_key=_text(row.get("model_key")) or None,
        reasons=reasons,
        raw_attrs=attrs,
    )


def _ensure_table() -> None:
    with connection.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS msx125_alerts (
                external_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                model_key TEXT,
                price_current NUMERIC(10,2),
                first_detected_at TIMESTAMPTZ NOT NULL,
                last_detected_at TIMESTAMPTZ NOT NULL,
                email_sent_at TIMESTAMPTZ,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """
        )
    connection.commit()


def _fetch_live_rows() -> list[dict[str, Any]]:
    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            SELECT source, external_id, title, url, price_current, status, model_key, raw_attrs
            FROM auction_listings
            WHERE source = ANY(%s)
              AND LOWER(status) = ANY(%s)
              AND (
                    LOWER(title) LIKE '%%msx125%%'
                 OR LOWER(title) LIKE '%%msx 125%%'
                 OR LOWER(title) LIKE '%%msx-125%%'
                 OR LOWER(title) LIKE '%%grom%%'
              )
            ORDER BY price_current ASC, last_seen_at DESC NULLS LAST
            """,
            (list(SOURCES), list(LIVE_STATUSES)),
        )
        return list(cur.fetchall())


def _load_existing(external_id: str) -> Optional[dict[str, Any]]:
    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            SELECT *
            FROM msx125_alerts
            WHERE external_id = %s
            LIMIT 1
            """,
            (external_id,),
        )
        return cur.fetchone()


def _upsert_candidate(candidate: Candidate) -> None:
    payload = {
        "source": candidate.source,
        "title": candidate.title,
        "url": candidate.url,
        "price_current": candidate.price_current,
        "status": candidate.status,
        "model_key": candidate.model_key,
        "reasons": candidate.reasons,
        "raw_attrs": candidate.raw_attrs,
    }
    with connection.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            INSERT INTO msx125_alerts (
                external_id, title, source, model_key, price_current,
                first_detected_at, last_detected_at, payload
            )
            VALUES (%s, %s, %s, %s, %s, now(), now(), %s)
            ON CONFLICT (external_id) DO UPDATE
            SET title = EXCLUDED.title,
                source = EXCLUDED.source,
                model_key = EXCLUDED.model_key,
                price_current = EXCLUDED.price_current,
                last_detected_at = now(),
                payload = EXCLUDED.payload
            """,
            (
                candidate.external_id,
                candidate.title,
                candidate.source,
                candidate.model_key,
                candidate.price_current,
                Json(payload),
            ),
        )
    connection.commit()


def _mark_email_sent(external_id: str) -> None:
    with connection.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute("UPDATE msx125_alerts SET email_sent_at = now() WHERE external_id = %s", (external_id,))
    connection.commit()


def _render_candidate(candidate: Candidate) -> str:
    reasons = "".join(f"<li>{reason}</li>" for reason in candidate.reasons)
    return f"""
    <div style="margin:0 0 18px;padding:16px;border:1px solid #e5e7eb;border-radius:12px;">
      <p style="margin:0 0 6px;"><strong>{candidate.title}</strong></p>
      <p style="margin:0 0 10px;">
        <strong>Price:</strong> £{candidate.price_current:,.0f}<br>
        <strong>Status:</strong> {candidate.status}<br>
        <strong>Source:</strong> {candidate.source}<br>
        <strong>Model key:</strong> {candidate.model_key or 'unknown'}
      </p>
      <p style="margin:0 0 10px;"><a href="{candidate.url}">Open on eBay</a></p>
      <p><strong>Why it matched</strong></p>
      <ul>{reasons}</ul>
    </div>
    """


def _send_digest_email(candidates: list[Candidate]) -> bool:
    subject = f"[GhostFrog MSX125 Alert] {len(candidates)} Honda MSX 125 candidate{'' if len(candidates) == 1 else 's'}"
    body = f"""
    <div style="font-family:system-ui,Arial,sans-serif;color:#111827;font-size:14px;line-height:1.45;">
      <h1 style="margin:0 0 12px;">Honda MSX 125 / Grom Candidates</h1>
      <p style="margin:0 0 18px;">
        New live Honda MSX 125 / Grom whole-bike/project listings found by GhostFrog.
        Minimum alert price: £{MIN_WHOLE_BIKE_PRICE:,.0f}.
      </p>
      {''.join(_render_candidate(candidate) for candidate in candidates)}
    </div>
    """
    for recipient in ALERT_EMAILS:
        send_email(subject=subject, body=body, to_addr=recipient, is_html=True)
    return True


def _run_detection(state: Msx125State) -> Msx125State:
    _ensure_table()

    all_candidates: list[Candidate] = []
    new_candidates: list[Candidate] = []
    emails_sent = 0

    for row in _fetch_live_rows():
        candidate = _candidate_from_row(row)
        if not candidate:
            continue

        existing = _load_existing(candidate.external_id)
        _upsert_candidate(candidate)
        all_candidates.append(candidate)

        if not existing or not existing.get("email_sent_at"):
            new_candidates.append(candidate)

    if new_candidates:
        try:
            _send_digest_email(new_candidates)
            for candidate in new_candidates:
                _mark_email_sent(candidate.external_id)
            emails_sent = len(ALERT_EMAILS)
            logger.info("[project_msx125] digest emailed with %d candidate(s)", len(new_candidates))
        except Exception as exc:
            logger.warning("[project_msx125] digest email failed: %s", exc)

    state["candidates_found"] = len(all_candidates)
    state["emails_sent"] = emails_sent
    state["results"] = [
        {
            "external_id": candidate.external_id,
            "source": candidate.source,
            "title": candidate.title,
            "price_current": candidate.price_current,
            "model_key": candidate.model_key,
        }
        for candidate in all_candidates
    ]
    return state


def build_graph():
    graph = StateGraph(Msx125State)
    graph.add_node("detect", _run_detection)
    graph.set_entry_point("detect")
    graph.add_edge("detect", END)
    return graph.compile()


def run(state: Optional[Msx125State] = None) -> Msx125State:
    graph = build_graph()
    return graph.invoke(state or {}, config={"recursion_limit": 50})
