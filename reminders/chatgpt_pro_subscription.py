from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
from html import escape
from pathlib import Path
from zoneinfo import ZoneInfo

from dotenv import load_dotenv

from utils.emailer import send_email

load_dotenv()

ENABLED = os.getenv("GF_CHATGPT_PRO_REMINDER_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
PLAN_NAME = os.getenv("GF_CHATGPT_PRO_REMINDER_PLAN", "ChatGPT Pro").strip()
RENEWAL_DAY = int(os.getenv("GF_CHATGPT_PRO_REMINDER_DAY", "8"))
TIMEZONE = ZoneInfo(os.getenv("GF_CHATGPT_PRO_REMINDER_TZ", "Europe/London"))
RECIPIENTS = [
    email.strip()
    for email in os.getenv(
        "GF_CHATGPT_PRO_REMINDER_RECIPIENTS",
        "garyconstable80@gmail.com,info@ghostfrog.co.uk",
    ).split(",")
    if email.strip()
]
SLOTS = [
    slot.strip()
    for slot in os.getenv(
        "GF_CHATGPT_PRO_REMINDER_SLOTS",
        "08:00,10:00,12:00,14:00,16:00,18:00,20:00,22:00",
    ).split(",")
    if slot.strip()
]
STATE_PATH = Path(
    os.getenv(
        "GF_CHATGPT_PRO_REMINDER_STATE_FILE",
        "/tmp/ghostfrog-chatgpt-pro-reminder-state.json",
    )
)


def _renewal_date_for(today: date) -> date:
    if today.day <= RENEWAL_DAY:
        return date(today.year, today.month, RENEWAL_DAY)

    year = today.year + 1 if today.month == 12 else today.year
    month = 1 if today.month == 12 else today.month + 1
    return date(year, month, RENEWAL_DAY)


def _format_renewal_date(value: date) -> str:
    return value.strftime("%B %-d, %Y")


def _countdown_label(days_until_renewal: int) -> str:
    if days_until_renewal == 0:
        return "renews today"
    if days_until_renewal == 1:
        return "1 day left to renew"
    return f"{days_until_renewal} days left to renew"


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
    trimmed = dict(sorted(state.items())[-18:])
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


def _subject(renewal_date: date, days_until_renewal: int) -> str:
    countdown = _countdown_label(days_until_renewal)
    return f"[Reminder] {countdown}! {PLAN_NAME} renews on {_format_renewal_date(renewal_date)}"


def _body(
    now: datetime,
    renewal_date: date,
    slot_label: str,
    days_until_renewal: int,
) -> str:
    countdown = _countdown_label(days_until_renewal)
    safe_plan = escape(PLAN_NAME)
    safe_countdown = escape(f"{countdown}!")
    safe_renewal = escape(_format_renewal_date(renewal_date))
    safe_slot = escape(slot_label)
    safe_sent_at = escape(now.isoformat())
    return (
        "<!doctype html>"
        "<html>"
        "<body style=\"margin:0;padding:24px;font-family:Arial,Helvetica,sans-serif;"
        "background:#f6f7f9;color:#111827;\">"
        "<div style=\"max-width:680px;margin:0 auto;background:#ffffff;"
        "border:1px solid #d1d5db;border-radius:8px;padding:28px;\">"
        "<div style=\"font-size:14px;font-weight:700;text-transform:uppercase;"
        "letter-spacing:0.08em;color:#374151;margin-bottom:12px;\">"
        "Subscription renewal reminder"
        "</div>"
        "<div style=\"font-size:42px;line-height:1.05;font-weight:900;"
        "color:#b91c1c;margin:0 0 22px 0;\">"
        f"Countdown: {safe_countdown}"
        "</div>"
        "<div style=\"font-size:20px;line-height:1.45;margin-bottom:18px;\">"
        f"<strong>{safe_plan}</strong> auto-renews on "
        f"<strong>{safe_renewal}</strong>."
        "</div>"
        "<table style=\"width:100%;border-collapse:collapse;font-size:15px;"
        "line-height:1.5;\">"
        "<tr>"
        "<td style=\"padding:8px 0;color:#6b7280;width:140px;\">Plan</td>"
        f"<td style=\"padding:8px 0;font-weight:700;\">{safe_plan}</td>"
        "</tr>"
        "<tr>"
        "<td style=\"padding:8px 0;color:#6b7280;\">Renewal date</td>"
        f"<td style=\"padding:8px 0;font-weight:700;\">{safe_renewal}</td>"
        "</tr>"
        "<tr>"
        "<td style=\"padding:8px 0;color:#6b7280;\">Scheduled slot</td>"
        f"<td style=\"padding:8px 0;\">{safe_slot}</td>"
        "</tr>"
        "<tr>"
        "<td style=\"padding:8px 0;color:#6b7280;\">Sent at</td>"
        f"<td style=\"padding:8px 0;\">{safe_sent_at}</td>"
        "</tr>"
        "</table>"
        "<p style=\"margin:24px 0 0 0;font-size:14px;color:#4b5563;\">"
        "This is a scheduled reminder so the subscription renewal is not missed."
        "</p>"
        "</div>"
        "</body>"
        "</html>"
    )


def send_if_due(now: datetime | None = None, *, force: bool = False) -> dict[str, object]:
    now = now or datetime.now(TIMEZONE)
    today = now.date()
    date_key = today.isoformat()
    renewal_date = _renewal_date_for(today)
    days_until_renewal = (renewal_date - today).days

    if not ENABLED:
        return {
            "status": "disabled",
            "date": date_key,
            "plan": PLAN_NAME,
        }

    state = _load_state()
    sent_slots = set(state.get(date_key, []))
    due_slots = _due_slots(now, sent_slots)

    if not force and not due_slots:
        return {
            "status": "skipped_not_due",
            "date": date_key,
            "plan": PLAN_NAME,
            "renewal_date": renewal_date.isoformat(),
            "days_until_renewal": days_until_renewal,
            "sent_slots": sorted(sent_slots),
        }

    if force:
        slot_label = "manual"
        slots_to_mark = set(sent_slots)
    else:
        slot_label = due_slots[-1]
        slots_to_mark = sent_slots.union(due_slots)

    subject = _subject(renewal_date, days_until_renewal=days_until_renewal)
    body = _body(
        now,
        renewal_date=renewal_date,
        slot_label=slot_label,
        days_until_renewal=days_until_renewal,
    )

    sent_to: list[str] = []
    for recipient in RECIPIENTS:
        send_email(subject=subject, body=body, to_addr=recipient, is_html=True)
        sent_to.append(recipient)

    state[date_key] = sorted(slots_to_mark)
    _save_state(state)

    return {
        "status": "sent",
        "date": date_key,
        "plan": PLAN_NAME,
        "renewal_date": renewal_date.isoformat(),
        "days_until_renewal": days_until_renewal,
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
