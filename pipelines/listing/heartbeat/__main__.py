from __future__ import annotations

import json
import os
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict

from reminders.chatgpt_pro_subscription import (
    send_if_due as send_chatgpt_pro_reminder_if_due,
)
from reminders.domain_expiry_cjtaxi import send_if_due as send_cjtaxi_reminder_if_due
from reminders.treds_payment import send_if_due as send_treds_payment_reminder_if_due
from utils.db_connection import create_connection
from utils.emailer import send_email
from utils.logger import get_logger

# Main orchestrator graph (sub graphs inside)
from pipelines.listing.main import run as run_main

logger = get_logger(__name__)

FAILURE_EMAIL_RECIPIENTS = [
    email.strip()
    for email in os.getenv(
        "GF_HEARTBEAT_FAILURE_RECIPIENTS",
        "garyconstable80@gmail.com,info@ghostfrog.co.uk",
    ).split(",")
    if email.strip()
]
FAILURE_EMAIL_INTERVAL = timedelta(
    minutes=int(os.getenv("GF_HEARTBEAT_FAILURE_EMAIL_MINUTES", "15"))
)
FAILURE_STATE_PATH = Path(
    os.getenv(
        "GF_HEARTBEAT_FAILURE_STATE_FILE",
        "/tmp/ghostfrog-listing-heartbeat-failure-state.json",
    )
)


def _load_env() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, indent=2, default=str)


def _load_failure_state() -> dict[str, Any]:
    try:
        if FAILURE_STATE_PATH.exists():
            return json.loads(FAILURE_STATE_PATH.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("[heartbeat] failed to read failure state file: %s", exc)
    return {}


def _save_failure_state(state: dict[str, Any]) -> None:
    try:
        FAILURE_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        FAILURE_STATE_PATH.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as exc:
        logger.warning("[heartbeat] failed to write failure state file: %s", exc)


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _send_alert_email(subject: str, body: str) -> list[str]:
    sent_to: list[str] = []
    for recipient in FAILURE_EMAIL_RECIPIENTS:
        send_email(subject=subject, body=body, to_addr=recipient, is_html=False)
        sent_to.append(recipient)
    return sent_to


def _maybe_send_failure_alert(
    *,
    category: str,
    headline: str,
    started: str,
    finished: str,
    error: str,
    tb: str | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    state = _load_failure_state()
    active = state.get("active_failure") or {}
    last_sent_at = _parse_dt(active.get("last_alert_sent_at"))

    state["active_failure"] = {
        "category": category,
        "headline": headline,
        "started_at": active.get("started_at") or started,
        "last_seen_at": finished,
        "last_error": error,
        "last_traceback": tb or "",
        "last_alert_sent_at": active.get("last_alert_sent_at"),
    }

    if last_sent_at is not None and (now - last_sent_at) < FAILURE_EMAIL_INTERVAL:
        _save_failure_state(state)
        return {
            "status": "suppressed_recently_sent",
            "category": category,
            "last_alert_sent_at": last_sent_at.isoformat(),
        }

    subject = f"[GhostFrog] URGENT: listing database / heartbeat failure — {headline}"
    body = (
        "GhostFrog listing pipeline failure alert\n\n"
        f"Category: {category}\n"
        f"Headline: {headline}\n"
        f"Started: {started}\n"
        f"Detected: {finished}\n"
        f"Repeat cadence: every {int(FAILURE_EMAIL_INTERVAL.total_seconds() // 60)} minutes while failing\n\n"
        f"Error:\n{error}\n\n"
        "Traceback:\n"
        f"{tb or 'n/a'}\n"
    )

    sent_to = _send_alert_email(subject=subject, body=body)
    state["active_failure"]["last_alert_sent_at"] = now.isoformat()
    _save_failure_state(state)
    logger.warning("[heartbeat] failure alert email sent to %s", ", ".join(sent_to))
    return {
        "status": "sent",
        "category": category,
        "sent_to": sent_to,
        "sent_at": now.isoformat(),
    }


def _maybe_send_recovery_alert(finished: str) -> dict[str, Any] | None:
    state = _load_failure_state()
    active = state.get("active_failure")
    if not active:
        return None

    now = datetime.now(timezone.utc)
    subject = "[GhostFrog] RECOVERY: listing database / heartbeat restored"
    body = (
        "GhostFrog listing pipeline recovery alert\n\n"
        f"Failure category: {active.get('category', 'unknown')}\n"
        f"Failure started: {active.get('started_at', 'unknown')}\n"
        f"Recovered at: {finished}\n\n"
        "The heartbeat completed successfully again.\n"
    )

    sent_to = _send_alert_email(subject=subject, body=body)
    state["last_recovery_at"] = now.isoformat()
    state.pop("active_failure", None)
    _save_failure_state(state)
    logger.info("[heartbeat] recovery email sent to %s", ", ".join(sent_to))
    return {
        "status": "sent",
        "sent_to": sent_to,
        "sent_at": now.isoformat(),
    }


def _probe_database() -> tuple[bool, str | None]:
    conn = None
    try:
        conn = create_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        conn.close()
        return True, None
    except Exception as exc:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass
        return False, str(exc)


def run_once() -> Dict[str, Any]:
    started = datetime.now(timezone.utc).isoformat()
    logger.info(f"[heartbeat] tick start {started}")

    reminder_out: dict[str, Any] | None = None
    try:
        reminder_out = send_cjtaxi_reminder_if_due()
    except Exception as exc:
        reminder_out = {
            "status": "failed",
            "error": str(exc),
        }
        logger.warning("[heartbeat] cjtaxi reminder failed: %s", exc)

    chatgpt_pro_reminder_out: dict[str, Any] | None = None
    try:
        chatgpt_pro_reminder_out = send_chatgpt_pro_reminder_if_due()
    except Exception as exc:
        chatgpt_pro_reminder_out = {
            "status": "failed",
            "error": str(exc),
        }
        logger.warning("[heartbeat] ChatGPT Pro reminder failed: %s", exc)

    treds_payment_reminder_out: dict[str, Any] | None = None
    try:
        treds_payment_reminder_out = send_treds_payment_reminder_if_due()
    except Exception as exc:
        treds_payment_reminder_out = {
            "status": "failed",
            "error": str(exc),
        }
        logger.warning("[heartbeat] Treds payment reminder failed: %s", exc)

    db_ok, db_error = _probe_database()
    if not db_ok:
        finished = datetime.now(timezone.utc).isoformat()
        logger.error("[heartbeat] database preflight failed %s: %s", finished, db_error)
        alert = _maybe_send_failure_alert(
            category="database_unavailable",
            headline="Postgres unavailable before heartbeat run",
            started=started,
            finished=finished,
            error=db_error or "unknown database error",
            tb=None,
        )
        return {
            "ok": False,
            "started": started,
            "finished": finished,
            "error": db_error or "unknown database error",
            "category": "database_unavailable",
            "alert_email": alert,
            "domain_reminder": reminder_out,
            "chatgpt_pro_reminder": chatgpt_pro_reminder_out,
            "treds_payment_reminder": treds_payment_reminder_out,
        }

    try:
        out = run_main()
        finished = datetime.now(timezone.utc).isoformat()
        recovery = _maybe_send_recovery_alert(finished)
        if out.get("maintenance_skip"):
            logger.info(f"[heartbeat] tick skipped {finished} (maintenance pause active)")
        else:
            logger.info(f"[heartbeat] tick end {finished}")
        return {
            "ok": True,
            "started": started,
            "finished": finished,
            "out": out,
            "recovery_email": recovery,
            "domain_reminder": reminder_out,
            "chatgpt_pro_reminder": chatgpt_pro_reminder_out,
            "treds_payment_reminder": treds_payment_reminder_out,
        }
    except Exception as e:
        finished = datetime.now(timezone.utc).isoformat()
        tb = traceback.format_exc()
        logger.error(f"[heartbeat] tick failed {finished}: {e}\n{tb}")
        alert = _maybe_send_failure_alert(
            category="heartbeat_failure",
            headline="Heartbeat raised an exception during pipeline execution",
            started=started,
            finished=finished,
            error=str(e),
            tb=tb,
        )
        return {
            "ok": False,
            "started": started,
            "finished": finished,
            "error": str(e),
            "traceback": tb,
            "category": "heartbeat_failure",
            "alert_email": alert,
            "domain_reminder": reminder_out,
            "chatgpt_pro_reminder": chatgpt_pro_reminder_out,
            "treds_payment_reminder": treds_payment_reminder_out,
        }


def main() -> None:
    _load_env()

    # This module can run once OR loop depending on env/args.
    # For now: run once (cron-style).
    result = run_once()
    print(_safe_json(result))


if __name__ == "__main__":
    main()
