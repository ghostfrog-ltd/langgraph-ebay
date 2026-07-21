from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Optional, TypedDict

import requests
from langgraph.graph import END, StateGraph
from psycopg2.extras import Json, RealDictCursor

from utils.db_schema import connection, ensure_utc_session
from utils.emailer import send_email
from utils.logger import get_logger

logger = get_logger(__name__)

ALERT_EMAILS = [
    email.strip()
    for email in os.getenv(
        "GF_PROJECT_MINI_EMAILS",
        "garyconstable80@gmail.com,info@ghostfrog.co.uk",
    ).split(",")
    if email.strip()
]
TELEGRAM_BOT_TOKEN = os.getenv("GF_PROJECT_MINI_TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("GF_PROJECT_MINI_TELEGRAM_CHAT_ID", "").strip()

MIN_PRICE = 700.0
MAX_PRICE = 1300.0
WINDOW_START = datetime(2004, 7, 1, tzinfo=timezone.utc)
WINDOW_END = datetime(2006, 10, 31, 23, 59, 59, tzinfo=timezone.utc)

GENERATION_TOKENS = ("r50", "r53")
MODEL_TOKENS = ("cooper", "one")
EXCLUDED_MODEL_TERMS = (
    "cooper s",
    "cooper sd",
    "cooper d",
    "john cooper works",
    "jcw",
)
EXCLUDED_BODY_TERMS = (
    "clubman",
    "countryman",
    "paceman",
    "roadster",
    "coupe",
    "convertible",
    "cabrio",
    "estate",
)
LATER_GENERATION_TERMS = (
    "r55",
    "r56",
    "r57",
    "r58",
    "r59",
    "r60",
    "r61",
    "f54",
    "f55",
    "f56",
    "f57",
    "f60",
)
DEAL_BREAKER_TERMS = (
    "emissions failure",
    "emissions failed",
    "limp mode",
    "high hydrocarbons",
    "hydrocarbons",
)
WHOLE_CAR_SIGNALS = (
    "mot",
    "manual",
    "petrol",
    "ulez",
    "mileage",
    "private seller",
    "number of doors",
    "v5c",
    "starts and drives",
    "ideal first car",
    "service history",
)
PART_ONLY_TERMS = (
    "headlight",
    "headlamp",
    "tail light",
    "taillight",
    "bumper",
    "trim ring",
    "badge",
    "emblem",
    "cup holder",
    "cupholder",
    "coaster",
    "mat",
    "mats",
    "dipstick",
    "door handle",
    "door pin",
    "charger",
    "projection lamp",
    "welcome light",
    "tool",
    "storage box",
    "subframe",
    "wing mirror",
    "tailgate handle",
    "centre cap",
    "hub centre cap",
    "window regulator",
    "wrench",
    "socket",
    "lock",
    "valve",
    "pcv",
)
ACCEPTABLE_FLAW_TERMS = (
    "broken spring",
    "broken springs",
    "coil spring",
    "coil springs",
    "age related marks",
    "age-related marks",
    "lacquer peel",
    "minor welding needed",
    "welding needed",
    "sill",
    "boot floor",
)
CHILI_TERMS = (
    "chili pack",
    "chilli pack",
    "sports seats",
    "sport seats",
    "half leather",
    "half-leather",
)

MAX_ALTERNATIVES_IN_EMAIL = 8


class ProjectMiniState(TypedDict, total=False):
    candidates_found: int
    exact_matches_found: int
    alternatives_found: int
    emails_sent: int
    telegram_sent: int
    results: list[dict[str, Any]]


@dataclass
class Candidate:
    external_id: str
    title: str
    url: str
    price_current: float
    status: str
    model_key: Optional[str]
    registration_date: Optional[datetime]
    raw_attrs: dict[str, Any]
    match_type: str
    reasons: list[str]
    miss_reasons: list[str]
    manual_checks: list[str]
    chili_pack: bool
    acceptable_flaws: list[str]
    priority_score: int


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(_text(v) for v in value if v is not None).strip()
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=True)
    return str(value).strip()


def _blob(attrs: Mapping[str, Any], title: str) -> str:
    joined = " ".join(
        [
            title,
            _text(attrs.get("Model")),
            _text(attrs.get("Manufacturer")),
            _text(attrs.get("Transmission")),
            _text(attrs.get("Fuel Type")),
            _text(attrs.get("Engine")),
            _text(attrs.get("Engine Size")),
            _text(attrs.get("Features")),
        ]
    )
    return joined.lower()


def _parse_registration_date(attrs: Mapping[str, Any], title: str) -> Optional[datetime]:
    candidates = [
        _text(attrs.get("Date of 1st Registration")),
        _text(attrs.get("Year")),
        title,
    ]
    for value in candidates:
        clean = value.strip()
        if not clean:
            continue

        compact = re.search(r"\b(20\d{2})(\d{2})(\d{2})\b", clean)
        if compact:
            year, month, day = map(int, compact.groups())
            try:
                return datetime(year, month, day, tzinfo=timezone.utc)
            except ValueError:
                pass

        year_only = re.search(r"\b(19\d{2}|20\d{2})\b", clean)
        if year_only:
            try:
                return datetime(int(year_only.group(1)), 1, 1, tzinfo=timezone.utc)
            except ValueError:
                pass

    return None


def _in_facelift_window(registration_date: Optional[datetime]) -> bool:
    if registration_date is None:
        return False
    return WINDOW_START <= registration_date <= WINDOW_END


def _looks_like_whole_car(attrs: Mapping[str, Any], title: str) -> bool:
    blob = _blob(attrs, title)
    if any(term in blob for term in WHOLE_CAR_SIGNALS):
        return True

    attrs_blob = " ".join(
        _text(attrs.get(key))
        for key in (
            "Number of Doors",
            "Number of Seats",
            "MOT Expiry",
            "Transmission",
            "Fuel Type",
            "V5C Registration Document",
        )
    ).lower()
    return bool(attrs_blob.strip())


def _is_part_only(attrs: Mapping[str, Any], title: str) -> bool:
    blob = _blob(attrs, title)
    if _looks_like_whole_car(attrs, title):
        return False
    return any(term in blob for term in PART_ONLY_TERMS)


def _engine_ok(attrs: Mapping[str, Any], title: str) -> bool:
    blob = _blob(attrs, title)
    if "diesel" in blob:
        return False
    return any(token in blob for token in ("1.6", "1598", "1600")) and "petrol" in blob


def _transmission_ok(attrs: Mapping[str, Any], title: str) -> bool:
    blob = _blob(attrs, title)
    return "manual" in blob and all(term not in blob for term in ("automatic", " auto ", "cvt"))


def _body_ok(attrs: Mapping[str, Any], title: str, model_key: Optional[str]) -> bool:
    blob = f"{_blob(attrs, title)} {(model_key or '').lower()}"
    if any(term in blob for term in EXCLUDED_BODY_TERMS):
        return False

    body = _text(attrs.get("Body Type")).lower()
    doors = _text(attrs.get("Number of Doors")).lower()

    hatch_like = any(token in blob for token in ("hatch", "hatchback"))
    if body:
        hatch_like = hatch_like or "hatch" in body
    if not hatch_like:
        return False

    if doors and not doors.startswith("3"):
        return False

    return True


def _generation_ok(
    attrs: Mapping[str, Any],
    title: str,
    model_key: Optional[str],
    registration_date: Optional[datetime],
) -> bool:
    blob = f"{_blob(attrs, title)} {(model_key or '').lower()}"
    if any(term in blob for term in LATER_GENERATION_TERMS):
        return False

    if any(token in blob for token in GENERATION_TOKENS):
        return True

    # Sellers often omit R50/R53, so infer first-gen facelift hatch when the
    # registration date is in range and the body shape still matches.
    if registration_date and _in_facelift_window(registration_date):
        return _body_ok(attrs, title, model_key)

    return False


def _model_ok(attrs: Mapping[str, Any], title: str) -> bool:
    blob = _blob(attrs, title)
    if any(term in blob for term in EXCLUDED_MODEL_TERMS):
        return False
    return any(token in blob for token in MODEL_TOKENS)


def _price_ok(price_current: float) -> bool:
    return MIN_PRICE <= price_current <= MAX_PRICE


def _deal_breaker(attrs: Mapping[str, Any], title: str) -> Optional[str]:
    blob = f"{_blob(attrs, title)} {_text(attrs)}".lower()
    for term in DEAL_BREAKER_TERMS:
        if term in blob:
            return term
    return None


def _acceptable_flaws(attrs: Mapping[str, Any], title: str) -> list[str]:
    blob = f"{_blob(attrs, title)} {_text(attrs)}".lower()
    return [term for term in ACCEPTABLE_FLAW_TERMS if term in blob]


def _has_chili_pack(attrs: Mapping[str, Any], title: str) -> bool:
    blob = f"{_blob(attrs, title)} {_text(attrs.get('Features'))}".lower()
    return any(term in blob for term in CHILI_TERMS)


def _priority_score(price_current: float, chili_pack: bool, acceptable_flaws: list[str]) -> int:
    score = 0
    if chili_pack:
        score += 30
    if acceptable_flaws:
        score += 10
    if price_current <= 1000:
        score += 10
    return score


def _exact_miss_reasons(
    attrs: Mapping[str, Any],
    title: str,
    model_key: Optional[str],
    registration_date: Optional[datetime],
) -> list[str]:
    misses: list[str] = []

    if not _generation_ok(attrs, title, model_key, registration_date):
        misses.append("not clearly first-generation R50/R53")
    if not _in_facelift_window(registration_date):
        misses.append("registration date outside Jul 2004 to Oct 2006")
    if not _model_ok(attrs, title):
        misses.append("not a Mini Cooper / Mini One target trim")
    if not _body_ok(attrs, title, model_key):
        misses.append("not a 3-door hatch target body")
    if not _engine_ok(attrs, title):
        misses.append("not a 1.6 petrol")
    if not _transmission_ok(attrs, title):
        misses.append("not a manual / possible auto-CVT")

    return misses


def _candidate_from_row(row: Mapping[str, Any]) -> Optional[Candidate]:
    attrs = row.get("raw_attrs") if isinstance(row.get("raw_attrs"), dict) else {}
    title = _text(row.get("title"))
    model_key = _text(row.get("model_key")) or None
    price_current = float(row.get("price_current") or 0.0)
    registration_date = _parse_registration_date(attrs, title)

    if row.get("source") != "ebay-mini-cooper":
        return None
    if _is_part_only(attrs, title):
        return None
    if not _price_ok(price_current):
        return None
    if not _engine_ok(attrs, title):
        return None
    if not _transmission_ok(attrs, title):
        return None

    deal_breaker = _deal_breaker(attrs, title)
    if deal_breaker:
        return None

    flaws = _acceptable_flaws(attrs, title)
    chili_pack = _has_chili_pack(attrs, title)
    miss_reasons = _exact_miss_reasons(attrs, title, model_key, registration_date)
    is_exact_match = len(miss_reasons) == 0

    if is_exact_match:
        reasons = [
            "R50/R53 generation match",
            "facelift window match (Jul 2004 to Oct 2006)",
            "1.6L petrol match",
            "manual gearbox match",
            "Mini Cooper / Mini One match",
            f"budget match (£{price_current:,.0f})",
        ]
    else:
        reasons = [
            "Same-budget Mini alternative",
            "whole-car listing",
            "manual gearbox match",
            "1.6 petrol match",
            "worth a look if no exact facelift target is live",
        ]
    if chili_pack:
        reasons.append("Chili/Chilli Pack or sports-seat signal found")
    if flaws:
        reasons.append("acceptable project flaws: " + ", ".join(flaws))

    manual_checks = [
        "Verify bonnet-integrated headlights in photos",
        "Verify single central reverse light in rear bumper",
        "Verify facelift grille with 3 horizontal slats",
    ]

    return Candidate(
        external_id=_text(row.get("external_id")),
        title=title,
        url=_text(row.get("url")),
        price_current=price_current,
        status=_text(row.get("status")),
        model_key=model_key,
        registration_date=registration_date,
        raw_attrs=attrs,
        match_type="exact" if is_exact_match else "alternative",
        reasons=reasons,
        miss_reasons=miss_reasons,
        manual_checks=manual_checks,
        chili_pack=chili_pack,
        acceptable_flaws=flaws,
        priority_score=_priority_score(price_current, chili_pack, flaws),
    )


def _ensure_table() -> None:
    with connection.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS mini_project_alerts (
                external_id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                source TEXT NOT NULL,
                model_key TEXT,
                price_current NUMERIC(10,2),
                first_detected_at TIMESTAMPTZ NOT NULL,
                last_detected_at TIMESTAMPTZ NOT NULL,
                email_sent_at TIMESTAMPTZ,
                telegram_sent_at TIMESTAMPTZ,
                payload JSONB NOT NULL DEFAULT '{}'::jsonb
            )
            """
        )
    connection.commit()


def _fetch_live_rows() -> list[dict[str, Any]]:
    """Load live Mini project rows through the normalized listing read contracts.

    The alert workflow still stores detections in `mini_project_alerts`, but its
    listing evidence should come from the view layer so future table splitting
    can happen without changing advisory report logic.
    """
    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            SELECT identity.source_label AS source,
                   identity.external_id,
                   identity.title,
                   identity.url,
                   observation.price_current,
                   outcome.status,
                   attributes.model_key,
                   attributes.raw_attrs
            FROM listing_identity_v AS identity
            JOIN listing_observation_v AS observation USING (listing_id)
            JOIN listing_outcome_v AS outcome USING (listing_id)
            JOIN listing_attribute_v AS attributes USING (listing_id)
            WHERE identity.source_label = 'ebay-mini-cooper'
              AND LOWER(outcome.status) IN ('live', 'active', 'open', 'ending_soon')
              AND observation.price_current BETWEEN %s AND %s
            ORDER BY observation.price_current ASC, observation.last_seen_at DESC
            """,
            (MIN_PRICE, MAX_PRICE),
        )
        return list(cur.fetchall())


def _load_existing(external_id: str) -> Optional[dict[str, Any]]:
    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            SELECT *
            FROM mini_project_alerts
            WHERE external_id = %s
            LIMIT 1
            """,
            (external_id,),
        )
        return cur.fetchone()


def _upsert_candidate(candidate: Candidate) -> None:
    payload = {
        "title": candidate.title,
        "url": candidate.url,
        "price_current": candidate.price_current,
        "status": candidate.status,
        "model_key": candidate.model_key,
        "registration_date": candidate.registration_date.isoformat() if candidate.registration_date else None,
        "match_type": candidate.match_type,
        "reasons": candidate.reasons,
        "miss_reasons": candidate.miss_reasons,
        "manual_checks": candidate.manual_checks,
        "chili_pack": candidate.chili_pack,
        "acceptable_flaws": candidate.acceptable_flaws,
        "priority_score": candidate.priority_score,
        "raw_attrs": candidate.raw_attrs,
    }
    with connection.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            INSERT INTO mini_project_alerts (
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
                "ebay-mini-cooper",
                candidate.model_key,
                candidate.price_current,
                Json(payload),
            ),
        )
    connection.commit()


def _mark_email_sent(external_id: str) -> None:
    with connection.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute(
            "UPDATE mini_project_alerts SET email_sent_at = now() WHERE external_id = %s",
            (external_id,),
        )
    connection.commit()


def _mark_telegram_sent(external_id: str) -> None:
    with connection.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute(
            "UPDATE mini_project_alerts SET telegram_sent_at = now() WHERE external_id = %s",
            (external_id,),
        )
    connection.commit()


def _compose_email_subject(exact_matches: list[Candidate], alternatives: list[Candidate]) -> str:
    return (
        "[GhostFrog Mini Alert] "
        f"{len(exact_matches)} exact match"
        f"{'' if len(exact_matches) == 1 else 'es'}, "
        f"{len(alternatives)} alternative"
        f"{'' if len(alternatives) == 1 else 's'}"
    )


def _render_candidate(candidate: Candidate) -> str:
    reg = candidate.registration_date.strftime("%Y-%m-%d") if candidate.registration_date else "unknown"
    reasons = "".join(f"<li>{reason}</li>" for reason in candidate.reasons)
    misses = "".join(f"<li>{reason}</li>" for reason in candidate.miss_reasons)
    checks = "".join(f"<li>{check}</li>" for check in candidate.manual_checks)

    match_badge = (
        '<span style="display:inline-block;padding:4px 8px;border-radius:999px;'
        'background:#d1fae5;color:#065f46;font-size:12px;font-weight:700;">Exact match</span>'
        if candidate.match_type == "exact"
        else
        '<span style="display:inline-block;padding:4px 8px;border-radius:999px;'
        'background:#fef3c7;color:#92400e;font-size:12px;font-weight:700;">Alternative</span>'
    )

    miss_block = ""
    if candidate.miss_reasons:
        miss_block = (
            "<p><strong>Why it is not exact</strong></p>"
            f"<ul>{misses}</ul>"
        )

    checks_block = ""
    if candidate.match_type == "exact":
        checks_block = (
            "<p><strong>Manual photo checks still required</strong></p>"
            f"<ul>{checks}</ul>"
        )

    return f"""
    <div style="margin:0 0 18px;padding:16px;border:1px solid #e5e7eb;border-radius:12px;">
      <div style="margin:0 0 8px;">{match_badge}</div>
      <p style="margin:0 0 6px;"><strong>{candidate.title}</strong></p>
      <p style="margin:0 0 10px;">
        <strong>Price:</strong> £{candidate.price_current:,.0f}<br>
        <strong>Status:</strong> {candidate.status}<br>
        <strong>Model key:</strong> {candidate.model_key or 'unknown'}<br>
        <strong>Registration date:</strong> {reg}<br>
        <strong>Priority score:</strong> {candidate.priority_score}<br>
        <strong>Chili Pack signal:</strong> {"Yes" if candidate.chili_pack else "No"}
      </p>
      <p style="margin:0 0 10px;"><a href="{candidate.url}">Open on eBay</a></p>
      <p><strong>Why it is interesting</strong></p>
      <ul>{reasons}</ul>
      {miss_block}
      {checks_block}
    </div>
    """


def _compose_email_body(exact_matches: list[Candidate], alternatives: list[Candidate]) -> str:
    exact_html = "".join(_render_candidate(candidate) for candidate in exact_matches)
    alt_html = "".join(_render_candidate(candidate) for candidate in alternatives[:MAX_ALTERNATIVES_IN_EMAIL])

    if exact_matches:
        exact_section = (
            "<h2 style=\"margin:0 0 12px;\">Exact matches</h2>"
            "<p style=\"margin:0 0 16px;\">These match the strict facelift R50/R53 project-car brief.</p>"
            f"{exact_html}"
        )
    else:
        exact_section = (
            "<h2 style=\"margin:0 0 12px;\">Exact matches</h2>"
            "<p style=\"margin:0 0 16px;\">No exact facelift R50/R53 matches are live right now.</p>"
        )

    alt_intro = (
        "<h2 style=\"margin:24px 0 12px;\">How about these instead?</h2>"
        "<p style=\"margin:0 0 16px;\">These are close-enough manual petrol Mini listings in budget, "
        "but they miss one or more of the strict target rules.</p>"
    )
    if not alternatives:
        alt_html = (
            "<p style=\"margin:0 0 16px;\">No close-enough alternatives are live right now either.</p>"
        )
    alt_more = ""
    if len(alternatives) > MAX_ALTERNATIVES_IN_EMAIL:
        alt_more = (
            f"<p style=\"margin:12px 0 0;color:#6b7280;\">"
            f"...and {len(alternatives) - MAX_ALTERNATIVES_IN_EMAIL} more alternatives in the live feed."
            "</p>"
        )

    return f"""
    <div style="font-family:system-ui,Arial,sans-serif;color:#111827;font-size:14px;line-height:1.45;">
      <h1 style="margin:0 0 12px;">Mini Project Candidate Digest</h1>
      <p style="margin:0 0 18px;">
        Budget window: £{MIN_PRICE:,.0f} to £{MAX_PRICE:,.0f}. Manual petrol whole-car Minis only.
      </p>
      {exact_section}
      {alt_intro}
      {alt_html}
      {alt_more}
    </div>
    """


def _compose_telegram_message(candidate: Candidate) -> str:
    reg = candidate.registration_date.strftime("%Y-%m-%d") if candidate.registration_date else "unknown"
    return (
        "GhostFrog Mini Project Candidate\n"
        f"{candidate.title}\n"
        f"Price: £{candidate.price_current:,.0f}\n"
        f"Reg: {reg}\n"
        f"Model key: {candidate.model_key or 'unknown'}\n"
        f"Why: {', '.join(candidate.reasons)}\n"
        f"Manual checks: {', '.join(candidate.manual_checks)}\n"
        f"{candidate.url}"
    )


def _send_digest_email(exact_matches: list[Candidate], alternatives: list[Candidate]) -> bool:
    subject = _compose_email_subject(exact_matches, alternatives)
    body = _compose_email_body(exact_matches, alternatives)
    for recipient in ALERT_EMAILS:
        send_email(subject=subject, body=body, to_addr=recipient, is_html=True)
    return True


def _send_candidate_telegram(candidate: Candidate) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.info("[project_mini] telegram not configured; skipping for %s", candidate.external_id)
        return False

    response = requests.post(
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": _compose_telegram_message(candidate),
        },
        timeout=15,
    )
    response.raise_for_status()
    return True


def _run_detection(state: ProjectMiniState) -> ProjectMiniState:
    _ensure_table()

    all_candidates: list[Candidate] = []
    new_exact_matches: list[Candidate] = []
    new_alternatives: list[Candidate] = []
    emails_sent = 0
    telegram_sent = 0

    for row in _fetch_live_rows():
        candidate = _candidate_from_row(row)
        if not candidate:
            continue

        existing = _load_existing(candidate.external_id)
        _upsert_candidate(candidate)

        email_already_sent = bool(existing and existing.get("email_sent_at"))
        telegram_already_sent = bool(existing and existing.get("telegram_sent_at"))

        all_candidates.append(candidate)

        if not email_already_sent:
            if candidate.match_type == "exact":
                new_exact_matches.append(candidate)
            else:
                new_alternatives.append(candidate)

        if candidate.match_type == "exact" and not telegram_already_sent:
            try:
                if _send_candidate_telegram(candidate):
                    _mark_telegram_sent(candidate.external_id)
                    telegram_sent += 1
                    logger.info("[project_mini] telegram sent for %s", candidate.external_id)
            except Exception as exc:
                logger.warning("[project_mini] telegram failed for %s: %s", candidate.external_id, exc)

    current_exact_matches = [candidate for candidate in all_candidates if candidate.match_type == "exact"]
    current_alternatives = [candidate for candidate in all_candidates if candidate.match_type == "alternative"]

    if new_exact_matches or new_alternatives:
        try:
            _send_digest_email(current_exact_matches, current_alternatives)
            for candidate in [*new_exact_matches, *new_alternatives]:
                _mark_email_sent(candidate.external_id)
            emails_sent = len(ALERT_EMAILS)
            logger.info(
                "[project_mini] digest emailed with %d exact matches and %d alternatives",
                len(current_exact_matches),
                len(current_alternatives),
            )
        except Exception as exc:
            logger.warning("[project_mini] digest email failed: %s", exc)

    all_candidates.sort(
        key=lambda candidate: (
            0 if candidate.match_type == "exact" else 1,
            len(candidate.miss_reasons),
            -candidate.priority_score,
            candidate.price_current,
            candidate.title.lower(),
        )
    )

    state["candidates_found"] = len(all_candidates)
    state["exact_matches_found"] = sum(1 for candidate in all_candidates if candidate.match_type == "exact")
    state["alternatives_found"] = sum(1 for candidate in all_candidates if candidate.match_type == "alternative")
    state["emails_sent"] = emails_sent
    state["telegram_sent"] = telegram_sent
    state["results"] = [
        {
            "external_id": candidate.external_id,
            "title": candidate.title,
            "price_current": candidate.price_current,
            "model_key": candidate.model_key,
            "priority_score": candidate.priority_score,
            "chili_pack": candidate.chili_pack,
            "match_type": candidate.match_type,
            "miss_reasons": candidate.miss_reasons,
        }
        for candidate in all_candidates
    ]
    return state


def build_graph():
    graph = StateGraph(ProjectMiniState)
    graph.add_node("detect", _run_detection)
    graph.set_entry_point("detect")
    graph.add_edge("detect", END)
    return graph.compile()


def run(state: Optional[ProjectMiniState] = None) -> ProjectMiniState:
    graph = build_graph()
    return graph.invoke(state or {}, config={"recursion_limit": 50})
