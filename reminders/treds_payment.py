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

ENABLED = os.getenv("GF_TREDS_PAYMENT_REMINDER_ENABLED", "1").strip().lower() not in {
    "0",
    "false",
    "no",
    "off",
}
PAYMENT_NAME = os.getenv("GF_TREDS_PAYMENT_REMINDER_NAME", "Treds").strip()
AMOUNT_LABEL = os.getenv("GF_TREDS_PAYMENT_REMINDER_AMOUNT", "\u00a354").strip()
PAYMENT_DAY = int(os.getenv("GF_TREDS_PAYMENT_REMINDER_DAY", "22"))
FINAL_PAYMENT_DATE = date.fromisoformat(
    os.getenv("GF_TREDS_PAYMENT_REMINDER_FINAL_DATE", "2027-06-22")
)
TIMEZONE = ZoneInfo(os.getenv("GF_TREDS_PAYMENT_REMINDER_TZ", "Europe/London"))
RECIPIENTS = [
    email.strip()
    for email in os.getenv(
        "GF_TREDS_PAYMENT_REMINDER_RECIPIENTS",
        "garyconstable80@gmail.com,info@ghostfrog.co.uk",
    ).split(",")
    if email.strip()
]
SLOTS = [
    slot.strip()
    for slot in os.getenv(
        "GF_TREDS_PAYMENT_REMINDER_SLOTS",
        "08:00,10:00,12:00,14:00,16:00,18:00,20:00,22:00",
    ).split(",")
    if slot.strip()
]
STATE_PATH = Path(
    os.getenv(
        "GF_TREDS_PAYMENT_REMINDER_STATE_FILE",
        "/tmp/ghostfrog-treds-payment-reminder-state.json",
    )
)


def _next_payment_date(today: date) -> date:
    if today.day <= PAYMENT_DAY:
        return date(today.year, today.month, PAYMENT_DAY)

    year = today.year + 1 if today.month == 12 else today.year
    month = 1 if today.month == 12 else today.month + 1
    return date(year, month, PAYMENT_DAY)


def _remaining_monthly_payments(next_payment_date: date) -> int:
    if next_payment_date > FINAL_PAYMENT_DATE:
        return 0
    return (
        (FINAL_PAYMENT_DATE.year - next_payment_date.year) * 12
        + FINAL_PAYMENT_DATE.month
        - next_payment_date.month
        + 1
    )


def _format_date(value: date) -> str:
    return value.strftime("%B %-d, %Y")


def _days_label(days_until_payment: int) -> str:
    if days_until_payment == 0:
        return "payment due today"
    if days_until_payment == 1:
        return "1 day until payment"
    return f"{days_until_payment} days until payment"


def _payments_label(remaining_payments: int) -> str:
    if remaining_payments == 1:
        return "1 monthly payment remaining"
    return f"{remaining_payments} monthly payments remaining"


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
    trimmed = dict(sorted(state.items())[-45:])
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


def _subject(
    next_payment_date: date,
    days_until_payment: int,
    remaining_payments: int,
) -> str:
    return (
        f"[Reminder] {AMOUNT_LABEL} for {PAYMENT_NAME}: "
        f"{_days_label(days_until_payment)}, {_payments_label(remaining_payments)}"
    )


def _body(
    now: datetime,
    next_payment_date: date,
    slot_label: str,
    days_until_payment: int,
    remaining_payments: int,
) -> str:
    safe_name = escape(PAYMENT_NAME)
    safe_amount = escape(AMOUNT_LABEL)
    safe_days = escape(_days_label(days_until_payment))
    safe_remaining = escape(_payments_label(remaining_payments))
    safe_next_payment = escape(_format_date(next_payment_date))
    safe_final_payment = escape(_format_date(FINAL_PAYMENT_DATE))
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
        "Payment reminder"
        "</div>"
        "<div style=\"font-size:42px;line-height:1.05;font-weight:900;"
        "color:#b91c1c;margin:0 0 18px 0;\">"
        f"{safe_amount} for {safe_name}"
        "</div>"
        "<div style=\"font-size:30px;line-height:1.15;font-weight:800;"
        "color:#111827;margin:0 0 22px 0;\">"
        f"{safe_days}"
        "</div>"
        "<div style=\"font-size:20px;line-height:1.45;margin-bottom:18px;\">"
        f"<strong>{safe_remaining}</strong> until the final payment in "
        f"<strong>{safe_final_payment}</strong>."
        "</div>"
        "<table style=\"width:100%;border-collapse:collapse;font-size:15px;"
        "line-height:1.5;\">"
        "<tr>"
        "<td style=\"padding:8px 0;color:#6b7280;width:160px;\">Next payment</td>"
        f"<td style=\"padding:8px 0;font-weight:700;\">{safe_next_payment}</td>"
        "</tr>"
        "<tr>"
        "<td style=\"padding:8px 0;color:#6b7280;\">Amount</td>"
        f"<td style=\"padding:8px 0;font-weight:700;\">{safe_amount}</td>"
        "</tr>"
        "<tr>"
        "<td style=\"padding:8px 0;color:#6b7280;\">Final payment</td>"
        f"<td style=\"padding:8px 0;\">{safe_final_payment}</td>"
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
        "This is a scheduled reminder so the monthly payment is not missed."
        "</p>"
        "</div>"
        "</body>"
        "</html>"
    )


def send_if_due(now: datetime | None = None, *, force: bool = False) -> dict[str, object]:
    now = now or datetime.now(TIMEZONE)
    today = now.date()
    date_key = today.isoformat()
    next_payment_date = _next_payment_date(today)
    days_until_payment = (next_payment_date - today).days
    remaining_payments = _remaining_monthly_payments(next_payment_date)

    if not ENABLED:
        return {
            "status": "disabled",
            "date": date_key,
            "payment": PAYMENT_NAME,
        }

    if remaining_payments <= 0 and not force:
        return {
            "status": "skipped_completed",
            "date": date_key,
            "payment": PAYMENT_NAME,
            "final_payment_date": FINAL_PAYMENT_DATE.isoformat(),
        }

    state = _load_state()
    sent_slots = set(state.get(date_key, []))
    due_slots = _due_slots(now, sent_slots)

    if not force and not due_slots:
        return {
            "status": "skipped_not_due",
            "date": date_key,
            "payment": PAYMENT_NAME,
            "next_payment_date": next_payment_date.isoformat(),
            "days_until_payment": days_until_payment,
            "remaining_payments": remaining_payments,
            "sent_slots": sorted(sent_slots),
        }

    if force:
        slot_label = "manual"
        slots_to_mark = set(sent_slots)
    else:
        slot_label = due_slots[-1]
        slots_to_mark = sent_slots.union(due_slots)

    subject = _subject(
        next_payment_date,
        days_until_payment=days_until_payment,
        remaining_payments=remaining_payments,
    )
    body = _body(
        now,
        next_payment_date=next_payment_date,
        slot_label=slot_label,
        days_until_payment=days_until_payment,
        remaining_payments=remaining_payments,
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
        "payment": PAYMENT_NAME,
        "amount": AMOUNT_LABEL,
        "next_payment_date": next_payment_date.isoformat(),
        "days_until_payment": days_until_payment,
        "remaining_payments": remaining_payments,
        "final_payment_date": FINAL_PAYMENT_DATE.isoformat(),
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
