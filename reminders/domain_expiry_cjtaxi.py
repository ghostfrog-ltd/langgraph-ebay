from __future__ import annotations

import json
import os
import sys
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from utils.emailer import send_email

load_dotenv()

ENABLED = os.getenv("GF_CJTAXI_REMINDER_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
DOMAIN = os.getenv("GF_CJTAXI_REMINDER_DOMAIN", "cjtaxi.co.uk").strip()
EXPIRY_DATE = date.fromisoformat(os.getenv("GF_CJTAXI_REMINDER_EXPIRY", "2026-05-09"))
TIMEZONE = ZoneInfo(os.getenv("GF_CJTAXI_REMINDER_TZ", "Europe/London"))
RECIPIENTS = [
    email.strip()
    for email in os.getenv(
        "GF_CJTAXI_REMINDER_RECIPIENTS",
        "garyconstable80@gmail.com,info@ghostfrog.co.uk",
    ).split(",")
    if email.strip()
]
SLOTS = [
    slot.strip()
    for slot in os.getenv(
        "GF_CJTAXI_REMINDER_SLOTS",
        "09:00,12:00,15:00,18:00,21:00",
    ).split(",")
    if slot.strip()
]
STATE_PATH = Path(
    os.getenv(
        "GF_CJTAXI_REMINDER_STATE_FILE",
        "/tmp/ghostfrog-cjtaxi-domain-reminder-state.json",
    )
)


def _expiry_status(today: date) -> str:
    days_remaining = (EXPIRY_DATE - today).days

    if days_remaining > 1:
        return f"expires in {days_remaining} days"
    if days_remaining == 1:
        return "expires tomorrow"
    if days_remaining == 0:
        return "expires today"
    if days_remaining == -1:
        return "expired yesterday"
    return f"expired {abs(days_remaining)} days ago"


def _subject(now: datetime) -> str:
    return f"[Reminder] {DOMAIN} expires on 09.05.2026 ({_expiry_status(now.date())})"


def _body(now: datetime, slot_label: str) -> str:
    return (
        "Domain expiry reminder\n\n"
        f"Domain: {DOMAIN}\n"
        "Expiry date: 09.05.2026\n"
        f"Status: {_expiry_status(now.date())}\n"
        f"Scheduled slot: {slot_label}\n"
        f"Reminder sent at: {now.isoformat()}\n\n"
        "This is a scheduled reminder so the renewal does not get missed.\n"
    )


def _load_state() -> dict[str, list[str]]:
    try:
        if STATE_PATH.exists():
            raw = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return {
                    str(day): [str(slot) for slot in slots]
                    for day, slots in raw.items()
                    if isinstance(slots, list)
                }
    except Exception:
        pass
    return {}


def _save_state(state: dict[str, list[str]]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    trimmed = dict(sorted(state.items())[-14:])
    STATE_PATH.write_text(json.dumps(trimmed, indent=2), encoding="utf-8")


def _due_slots(now: datetime, sent_slots: set[str]) -> list[str]:
    due: list[str] = []
    for slot in SLOTS:
        hour, minute = slot.split(":")
        slot_dt = now.replace(
            hour=int(hour),
            minute=int(minute),
            second=0,
            microsecond=0,
        )
        if now >= slot_dt and slot not in sent_slots:
            due.append(slot)
    return due


def send_if_due(now: datetime | None = None, *, force: bool = False) -> dict[str, object]:
    now = now or datetime.now(TIMEZONE)
    date_key = now.date().isoformat()

    if not ENABLED:
        return {
            "status": "disabled",
            "date": date_key,
            "domain": DOMAIN,
        }

    state = _load_state()
    sent_slots = set(state.get(date_key, []))
    due_slots = _due_slots(now, sent_slots)

    if not force and not due_slots:
        return {
            "status": "skipped_not_due",
            "date": date_key,
            "sent_slots": sorted(sent_slots),
        }

    if force:
        slot_label = "manual"
        slots_to_mark = set(sent_slots)
    else:
        slot_label = due_slots[-1]
        slots_to_mark = sent_slots.union(due_slots)

    subject = _subject(now)
    body = _body(now, slot_label=slot_label)

    sent_to: list[str] = []
    for recipient in RECIPIENTS:
        send_email(subject=subject, body=body, to_addr=recipient, is_html=False)
        sent_to.append(recipient)

    state[date_key] = sorted(slots_to_mark)
    _save_state(state)

    return {
        "status": "sent",
        "date": date_key,
        "slot": slot_label,
        "sent_slots": state[date_key],
        "sent_to": sent_to,
        "sent_at": now.isoformat(),
    }


def main() -> None:
    force = "--force" in sys.argv
    print(json.dumps(send_if_due(force=force), indent=2))


if __name__ == "__main__":
    main()
