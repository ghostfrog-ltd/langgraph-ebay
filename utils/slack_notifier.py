"""
Send low-risk operational notifications to Slack.

Slack is an observer for the pipeline, not an execution channel. This module
only posts configured messages to an incoming webhook; it does not read Slack,
accept commands, mutate marketplace state, or expose webhook secrets in errors.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any

from dotenv import load_dotenv
import requests

load_dotenv()


TRUE_VALUES = {"1", "true", "yes", "on"}
DEFAULT_TIMEOUT_SECONDS = 10


@dataclass(frozen=True)
class SlackNotificationResult:
    """Auditable result for one Slack notification attempt."""

    status: str
    enabled: bool
    reason: str | None = None
    status_code: int | None = None
    error: str | None = None


def _env_enabled(name: str, default: str = "0") -> bool:
    """Read a boolean feature flag from environment variables."""

    return os.getenv(name, default).strip().lower() in TRUE_VALUES


def slack_notifications_enabled() -> bool:
    """Return whether Slack notifications are explicitly enabled."""

    return _env_enabled("SLACK_NOTIFICATIONS_ENABLED")


def _webhook_url() -> str:
    """Fetch the Slack incoming webhook URL without ever logging it."""

    return os.getenv("SLACK_WEBHOOK_URL", "").strip()


def _timeout_seconds() -> int:
    """Read Slack request timeout, falling back to a short safe default."""

    raw = os.getenv("SLACK_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS)).strip()
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS
    return value if value > 0 else DEFAULT_TIMEOUT_SECONDS


def _safe_error(exc: Exception) -> str:
    """
    Return a secret-safe exception summary.

    `requests` exceptions can include the full request URL. Incoming webhook
    URLs are bearer secrets, so only the exception type is returned here.
    """

    return f"{exc.__class__.__name__}: Slack webhook request failed"


def send_slack_message(
    *,
    text: str,
    blocks: list[dict[str, Any]] | None = None,
) -> SlackNotificationResult:
    """
    Post one message to the configured Slack incoming webhook.

    Side effects:
    - Sends an HTTPS POST to Slack only when `SLACK_NOTIFICATIONS_ENABLED=1`
      and `SLACK_WEBHOOK_URL` is present.
    - Returns a structured result instead of raising so notification failures
      cannot break the LangGraph heartbeat.
    """

    if not slack_notifications_enabled():
        return SlackNotificationResult(
            status="skipped",
            enabled=False,
            reason="disabled",
        )

    url = _webhook_url()
    if not url:
        return SlackNotificationResult(
            status="skipped",
            enabled=True,
            reason="missing_webhook_url",
        )

    payload: dict[str, Any] = {
        "text": text,
        "unfurl_links": False,
        "unfurl_media": False,
    }
    if blocks:
        payload["blocks"] = blocks

    try:
        response = requests.post(url, json=payload, timeout=_timeout_seconds())
    except Exception as exc:
        return SlackNotificationResult(
            status="failed",
            enabled=True,
            error=_safe_error(exc),
        )

    if 200 <= response.status_code < 300:
        return SlackNotificationResult(
            status="sent",
            enabled=True,
            status_code=response.status_code,
        )

    return SlackNotificationResult(
        status="failed",
        enabled=True,
        status_code=response.status_code,
        error=f"Slack webhook returned HTTP {response.status_code}",
    )
