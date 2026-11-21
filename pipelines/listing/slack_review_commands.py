from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import os
import re
import shlex
import time
from typing import Any

import requests
from dotenv import load_dotenv

from pipelines.listing.review_actions import record_review_action
from utils.pipeline_control import get_flag, set_flag

load_dotenv()


TRUE_VALUES = {"1", "true", "yes", "on"}
CURSOR_FLAG_NAME = "listing_slack_review_command_cursor"
DEFAULT_TIMEOUT_SECONDS = 10
DEFAULT_POLL_SECONDS = 60
DEFAULT_SLACK_BACKFILL_LIMIT = 100
DEFAULT_SLACK_BACKFILL_DAILY_LIMIT = 1000
VALID_ACTIONS = {"up", "down"}
HELP_COMMANDS = {"ebay", "ebay help", "ebay commands", "gf ebay", "gf commands", "commands", "help"}
SLACK_OPERATOR_COMMANDS = {
    "heartbeat",
    "slack-heartbeat",
    "serious",
    "punt",
    "stats",
    "favourites",
    "favorites",
    "reviews",
}


@dataclass(frozen=True)
class SlackReviewCommand:
    action: str
    external_id: str


@dataclass(frozen=True)
class SlackOperatorCommand:
    name: str
    item: str | None = None
    show_all: bool = False
    limit: int | None = None
    daily_limit: int | None = None
    include_vehicle_sources: bool = False
    error: str | None = None


@dataclass(frozen=True)
class SlackReviewSyncResult:
    status: str
    enabled: bool
    fetched: int = 0
    processed: int = 0
    ignored: int = 0
    replied: int = 0
    cursor: str | None = None
    reason: str | None = None
    error: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


def slack_review_commands_enabled() -> bool:
    return os.getenv("SLACK_REVIEW_COMMANDS_ENABLED", "0").strip().lower() in TRUE_VALUES


def _bot_token() -> str:
    return os.getenv("SLACK_BOT_TOKEN", "").strip()


def _channel_id() -> str:
    return os.getenv("SLACK_REVIEW_CHANNEL_ID", "").strip()


def _timeout_seconds() -> int:
    raw = os.getenv("SLACK_TIMEOUT_SECONDS", str(DEFAULT_TIMEOUT_SECONDS)).strip()
    try:
        parsed = int(raw)
    except ValueError:
        return DEFAULT_TIMEOUT_SECONDS
    return parsed if parsed > 0 else DEFAULT_TIMEOUT_SECONDS


def _safe_error(exc: Exception) -> str:
    message = str(exc)
    if message.startswith("Slack "):
        return message
    return f"{exc.__class__.__name__}: Slack review command request failed"


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_bot_token()}"}


def _clean_slack_token(value: str) -> str:
    token = str(value or "").strip()
    if token.startswith("<") and token.endswith(">"):
        token = token[1:-1]
    if "|" in token and token.startswith(("http://", "https://")):
        token = token.split("|", 1)[0]
    return token


def _clean_slack_command_text(text: str) -> str:
    raw = str(text or "").strip()
    raw = re.sub(r"^<@[^>]+>\s*", "", raw)
    raw = re.sub(r"^</?(?:pre|code)>\s*", "", raw, flags=re.IGNORECASE)
    raw = re.sub(r"\s*</(?:pre|code)>$", "", raw, flags=re.IGNORECASE)
    if raw.startswith("```") and raw.endswith("```"):
        raw = raw[3:-3].strip()
        lines = raw.splitlines()
        if len(lines) > 1 and re.fullmatch(r"[A-Za-z0-9_+-]+", lines[0].strip()):
            raw = "\n".join(lines[1:]).strip()
    if raw.startswith("`") and raw.endswith("`"):
        raw = raw[1:-1].strip()
    cleaned = " ".join(raw.split())
    cleaned = re.sub(r"^<@[^>]+>\s*", "", cleaned)
    return cleaned


def parse_slack_help_command(text: str) -> bool:
    cleaned = _clean_slack_command_text(text).lower()
    return cleaned in HELP_COMMANDS


def _parse_positive_int(value: str) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _parse_slack_backfill_apply_command(cleaned: str) -> SlackOperatorCommand:
    try:
        tokens = shlex.split(cleaned)
    except ValueError as exc:
        return SlackOperatorCommand(name="backfill", error=f"cannot parse command: {exc}")

    limit = DEFAULT_SLACK_BACKFILL_LIMIT
    daily_limit = DEFAULT_SLACK_BACKFILL_DAILY_LIMIT
    include_vehicle_sources = False
    index = 1
    while index < len(tokens):
        token = tokens[index]
        if token in {"--limit", "--daily-limit", "--daily-backfill-budget"}:
            if index + 1 >= len(tokens):
                return SlackOperatorCommand(name="backfill", error=f"{token} needs a number")
            parsed = _parse_positive_int(tokens[index + 1])
            if parsed is None:
                return SlackOperatorCommand(name="backfill", error=f"{token} must be a positive number")
            if token == "--limit":
                limit = parsed
            else:
                daily_limit = parsed
            index += 2
            continue
        if token.startswith("--limit="):
            parsed = _parse_positive_int(token.split("=", 1)[1])
            if parsed is None:
                return SlackOperatorCommand(name="backfill", error="--limit must be a positive number")
            limit = parsed
            index += 1
            continue
        if token.startswith("--daily-limit=") or token.startswith("--daily-backfill-budget="):
            parsed = _parse_positive_int(token.split("=", 1)[1])
            if parsed is None:
                return SlackOperatorCommand(name="backfill", error="--daily-limit must be a positive number")
            daily_limit = parsed
            index += 1
            continue
        if token == "--include-vehicle-sources":
            include_vehicle_sources = True
            index += 1
            continue
        return SlackOperatorCommand(name="backfill", error=f"unknown option: {token}")

    return SlackOperatorCommand(
        name="backfill",
        limit=limit,
        daily_limit=daily_limit,
        include_vehicle_sources=include_vehicle_sources,
    )


def parse_slack_operator_command(text: str) -> SlackOperatorCommand | None:
    cleaned = _clean_slack_command_text(text)
    lower = cleaned.lower()
    if lower.startswith("gf "):
        cleaned = cleaned[3:].strip()
        lower = cleaned.lower()
    if lower.startswith("ebay "):
        cleaned = cleaned[5:].strip()
        lower = cleaned.lower()
    if lower in SLACK_OPERATOR_COMMANDS:
        name = "favourites" if lower == "favorites" else lower
        name = "heartbeat" if lower == "slack-heartbeat" else name
        return SlackOperatorCommand(name=name)
    if lower in {"backfill", "backfill-apply"} or lower.startswith(("backfill ", "backfill-apply ")):
        return _parse_slack_backfill_apply_command(cleaned)
    if lower == "explain":
        return SlackOperatorCommand(name="explain")
    if lower.startswith("explain "):
        item: str | None = None
        show_all = False
        for token in cleaned.split()[1:]:
            if token.lower() == "--all":
                show_all = True
                continue
            if token.startswith("-"):
                return None
            if item is None:
                item = _clean_slack_token(token)
                continue
            return None
        if item:
            return SlackOperatorCommand(name="explain", item=item, show_all=show_all)
    return None


def parse_slack_review_command(text: str) -> SlackReviewCommand | None:
    """
    Parse a deliberately small command surface.

    Accepted examples:
    - up 206446095377
    - down https://www.ebay.co.uk/itm/206446095377
    - gf up v1|206446095377|0
    """

    cleaned = _clean_slack_command_text(text)
    if not cleaned:
        return None

    if cleaned.lower().startswith("gf "):
        cleaned = cleaned[3:].strip()

    match = re.match(r"^(up|down)\s+(\S+)", cleaned, flags=re.IGNORECASE)
    if not match:
        return None

    action = match.group(1).lower()
    external_id = _clean_slack_token(match.group(2))
    if action not in VALID_ACTIONS or not external_id:
        return None
    return SlackReviewCommand(action=action, external_id=external_id)


def _slack_escape(value: Any) -> str:
    return str(value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def format_slack_operator_command_menu() -> str:
    lines = [
        "*GhostFrog eBay commands*",
        "_Slack runs only this small safe subset._",
        "",
        "*Slack commands*",
        "- `ebay heartbeat` - post the current Slack heartbeat now",
        "- `ebay serious` - show GBP 50 live candidates",
        "- `ebay punt` - show GBP 25 live candidates",
        "- `ebay stats` - show compact local data health",
        "- `ebay explain 206446095377` - show the comp evidence behind one profit estimate",
        "- `ebay explain 206446095377 --all` - show every exact sold comp",
        "- `ebay backfill --limit 4000 --daily-limit 10000` - spend eBay calls and update local unknown rows",
        "- `ebay favourites` - show thumbed-up listings",
        "- `ebay reviews` - show recent up/down actions",
        "- `up 206446095377` - mark a listing as a local favourite",
        "- `down 206446095377` - exclude a listing from future candidate lists",
        "- `up https://www.ebay.co.uk/itm/206446095377` - URL form is OK too",
        "",
        "_Local console commands are intentionally omitted here. Still observer-first: no bids, buys, seller messages, relists, refunds, or shipping actions from Slack._",
    ]
    return "\n".join(lines)


def _money(value: Any) -> str:
    try:
        return f"GBP {float(value):,.2f}"
    except (TypeError, ValueError):
        return "GBP n/a"


def _short(value: Any, *, limit: int = 88) -> str:
    cleaned = " ".join(str(value or "").split())
    if len(cleaned) <= limit:
        return cleaned or "Untitled listing"
    return f"{cleaned[: limit - 3].rstrip()}..."


def _listing_link(title: Any, url: Any) -> str:
    label = _slack_escape(_short(title))
    clean_url = str(url or "").strip()
    return f"<{clean_url}|{label}>" if clean_url else label


def _format_potentials_for_slack(report: dict[str, Any], *, title: str) -> str:
    if report.get("error"):
        return f"*{title}*\nReport failed; check local logs."
    rows = [item for item in (report.get("potentials") or []) if isinstance(item, dict)]
    if not rows:
        return f"*{title}*\nNo live candidates cleared this gate."
    lines = [f"*{title}*"]
    for index, item in enumerate(rows[:8], start=1):
        external_id = str(item.get("external_id") or "").strip()
        review = f"`up {external_id}` / `down {external_id}`" if external_id else "review n/a"
        lines.append(
            "\n".join(
                [
                    f"{index}. `{_slack_escape(item.get('source'))}` {_listing_link(item.get('title'), item.get('url'))}",
                    (
                        f"   current {_money(item.get('current_price'))} | "
                        f"max {_money(item.get('max_safe_buy'))} | "
                        f"profit {_money(item.get('expected_profit'))} | "
                        f"comps {int(item.get('comp_samples') or 0)} | {review}"
                    ),
                ]
            )
        )
    return "\n\n".join(lines)


def _format_stats_for_slack(stats: dict[str, Any]) -> str:
    return "\n".join(
        [
            "*GhostFrog eBay stats*",
            f"Live listings: `{_slack_escape(stats.get('live_listings'))}`",
            f"Confirmed final prices: `{_slack_escape(stats.get('confirmed_final_prices'))}`",
            f"Comp buckets: `{_slack_escape(stats.get('comp_buckets'))}`",
            f"Comp samples: `{_slack_escape(stats.get('comp_samples'))}`",
            f"Live with 3+ same-grade comps: `{_slack_escape(stats.get('live_with_3plus_same_grade_comps'))}`",
            f"Unknown queue: `{_slack_escape(stats.get('unknown_queue'))}`",
            f"API calls today: `{_slack_escape(stats.get('today_calls'))}`",
        ]
    )


def _format_review_rows_for_slack(rows: list[dict[str, Any]], *, title: str, limit: int = 8) -> str:
    if not rows:
        return f"*{title}*\nNo rows yet."
    lines = [f"*{title}*"]
    for index, row in enumerate(rows[:limit], start=1):
        external_id = str(row.get("external_id") or "")
        review = f"`up {external_id}` / `down {external_id}`" if external_id else "review n/a"
        bits = [str(row.get("source") or "unknown")]
        if row.get("status"):
            bits.append(f"status {row.get('status')}")
        price = row.get("price_current")
        if price is not None:
            bits.append(f"current {_money(price)}")
        if row.get("action"):
            bits.append(f"action {row.get('action')}")
        lines.append(
            "\n".join(
                [
                    f"{index}. {_listing_link(row.get('title') or external_id, row.get('url'))}",
                    f"   {' | '.join(_slack_escape(bit) for bit in bits)} | {review}",
                ]
            )
        )
    return "\n\n".join(lines)


def _format_listing_explanation_for_slack(
    explanation: Any,
    *,
    limit: int = 6,
    show_all: bool = False,
) -> str:
    target = explanation.target or {}
    lines = ["*Listing profit explanation*"]
    if explanation.error:
        if target:
            title = target.get("title") or target.get("external_id")
            lines.append(_listing_link(title, target.get("url")))
            if target.get("model_key"):
                lines.append(
                    f"`{_slack_escape(target.get('model_key'))}` | grade `{_slack_escape(target.get('condition_grade') or 'B')}`"
                )
        lines.append(f"Cannot explain: {_slack_escape(explanation.error)}")
        return "\n".join(lines)

    potential = explanation.potential or {}
    external_id = str(target.get("external_id") or "").strip()
    lines.extend(
        [
            f"*{_listing_link(target.get('title'), target.get('url'))}*",
            (
                f"`{_slack_escape(external_id)}` | `{_slack_escape(target.get('source'))}` | "
                f"status `{_slack_escape(target.get('status'))}`"
            ),
            (
                f"bucket `{_slack_escape(target.get('model_key'))}` | "
                f"grade `{_slack_escape(target.get('condition_grade') or 'B')}`"
            ),
            (
                f"current {_money(potential.get('current_price'))} | "
                f"estimated resale median {_money(potential.get('sold_median'))} | "
                f"comps `{_slack_escape(explanation.comp_count)}` | "
                f"window `{_slack_escape(explanation.comp_window_days)}d`"
            ),
            (
                f"max safe buy {_money(potential.get('max_safe_buy'))} | "
                f"profit {_money(potential.get('expected_profit'))} | "
                f"ROI `{_slack_escape(round(float(potential.get('expected_roi') or 0) * 100))}%`"
            ),
        ]
    )
    if external_id:
        lines.append(f"review: `up {external_id}` / `down {external_id}`")

    rows = list(explanation.comps or [])
    if not show_all:
        rows = rows[:limit]
    hidden = max(len(explanation.comps or []) - len(rows), 0)
    lines.append("")
    lines.append(
        f"*Exact sold comps nearest to the estimate median* ({len(rows)} shown"
        f"{f', {hidden} hidden' if hidden else ''})"
    )
    if not rows:
        lines.append("No matching sold rows found, even though latest_comps has a bucket.")
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        bits = [
            str(row.get("source") or "unknown"),
            str(row.get("external_id") or ""),
            f"type {row.get('sale_type') or 'unknown'}",
            f"bids {int(row.get('bids_count') or 0)}",
            f"last {_money(row.get('price_current'))}",
            f"exact sold {_money(row.get('final_price'))}",
        ]
        if row.get("end_time"):
            bits.append(f"ended {row.get('end_time')}")
        lines.append(f"{index}. {_listing_link(row.get('title'), row.get('url'))}")
        lines.append(f"   {' | '.join(_slack_escape(bit) for bit in bits)}")

    return "\n\n".join(lines)


def _format_backfill_summary_for_slack(summary: dict[str, Any]) -> str:
    counts = summary.get("counts") or {}
    sold = 0
    ended = 0
    deleted = 0
    restored = 0
    skipped_or_errors = 0
    for key, value in counts.items():
        amount = int(value or 0)
        parts = str(key).split(":", 2)
        action = parts[0] if parts else ""
        status = parts[1] if len(parts) > 1 else ""
        if action == "finalize" and status == "sold":
            sold += amount
        elif action == "finalize" and status == "ended":
            ended += amount
        elif action == "delete":
            deleted += amount
        elif action == "restore":
            restored += amount
        else:
            skipped_or_errors += amount

    lines = [
        "*Backfill apply finished*",
        f"Status: `{_slack_escape(summary.get('status'))}`",
        (
            f"Limit `{_slack_escape(summary.get('limit'))}` | "
            f"effective `{_slack_escape(summary.get('effective_limit'))}` | "
            f"daily limit `{_slack_escape(summary.get('daily_backfill_budget'))}`"
        ),
        (
            f"Checked today before run `{_slack_escape(summary.get('backfill_checked_today'))}` | "
            f"remaining before run `{_slack_escape(summary.get('remaining_backfill_budget'))}`"
        ),
        (
            f"Candidates `{_slack_escape(summary.get('candidate_count'))}` | "
            f"outcomes `{_slack_escape(summary.get('outcome_count'))}` | "
            f"local rows changed `{_slack_escape(summary.get('applied'))}`"
        ),
        (
            f"sold `{sold}` | ended `{ended}` | deleted `{deleted}` | "
            f"restored `{restored}` | skipped/errors `{skipped_or_errors}`"
        ),
    ]
    if summary.get("budget_shortfall"):
        lines.append(f"Budget shortfall: `{_slack_escape(summary.get('budget_shortfall'))}`")
    if summary.get("candidate_shortfall"):
        reason = summary.get("candidate_shortfall_reason") or "not enough eligible candidates"
        lines.append(f"Candidate shortfall: `{_slack_escape(summary.get('candidate_shortfall'))}` ({_slack_escape(reason)})")
    if summary.get("stop_reason"):
        lines.append(f"Stopped because: `{_slack_escape(summary.get('stop_reason'))}`")

    examples = list(summary.get("outcomes") or [])[:5]
    if examples:
        lines.append("")
        lines.append("*First outcomes*")
        for row in examples:
            bits = [
                str(row.get("new_status") or row.get("action") or "unknown"),
                str(row.get("reason") or ""),
            ]
            if row.get("final_price") is not None:
                bits.append(f"final {_money(row.get('final_price'))}")
            lines.append(
                f"- `{_slack_escape(row.get('external_id'))}` {_slack_escape(row.get('title'))} | "
                f"{' | '.join(_slack_escape(bit) for bit in bits if bit)}"
            )
    return "\n".join(lines)


def _post_channel_message(*, channel_id: str, text: str, blocks: list[dict[str, Any]] | None = None) -> bool:
    payload: dict[str, Any] = {
        "channel": channel_id,
        "text": text,
        "unfurl_links": False,
        "unfurl_media": False,
    }
    if blocks:
        payload["blocks"] = blocks
    response = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={**_headers(), "Content-Type": "application/json; charset=utf-8"},
        json=payload,
        timeout=_timeout_seconds(),
    )
    return bool(response.json().get("ok"))


def run_slack_operator_command(command: SlackOperatorCommand, *, channel_id: str) -> bool:
    if command.error:
        return _post_channel_message(
            channel_id=channel_id,
            text=(
                f"Cannot run `ebay {command.name}`: {_slack_escape(command.error)}\n"
                "Use `ebay backfill --limit 4000 --daily-limit 10000`."
            ),
        )

    if command.name == "heartbeat":
        from pipelines.listing.main import _build_heartbeat_slack_message, _metrics_for_heartbeat

        state: dict[str, Any] = {}
        metrics = _metrics_for_heartbeat(state)
        text, blocks = _build_heartbeat_slack_message(state, metrics, datetime.now(timezone.utc))
        return _post_channel_message(channel_id=channel_id, text=text, blocks=blocks)

    if command.name in {"serious", "punt"}:
        from pipelines.listing.market_discovery.graph import run as run_market_discovery

        min_profit = 50.0 if command.name == "serious" else 25.0
        report = run_market_discovery(limit=8, min_profit=min_profit)
        text = _format_potentials_for_slack(
            report,
            title=f"GBP {int(min_profit)} Profit Candidates",
        )
        return _post_channel_message(channel_id=channel_id, text=text)

    if command.name == "stats":
        from pipelines.listing.operator_console import build_stats

        return _post_channel_message(
            channel_id=channel_id,
            text=_format_stats_for_slack(build_stats()),
        )

    if command.name == "favourites":
        from pipelines.listing.review_actions import list_favourites

        return _post_channel_message(
            channel_id=channel_id,
            text=_format_review_rows_for_slack(list_favourites(limit=8), title="Local favourites"),
        )

    if command.name == "reviews":
        from pipelines.listing.review_actions import latest_review_actions

        return _post_channel_message(
            channel_id=channel_id,
            text=_format_review_rows_for_slack(latest_review_actions(limit=8), title="Latest review actions"),
        )

    if command.name == "explain":
        if not command.item:
            return _post_channel_message(
                channel_id=channel_id,
                text="Use `ebay explain 206446095377` or `ebay explain https://www.ebay.co.uk/itm/206446095377`.",
            )

        from pipelines.listing.explain import build_listing_explanation

        explanation = build_listing_explanation(command.item)
        return _post_channel_message(
            channel_id=channel_id,
            text=_format_listing_explanation_for_slack(explanation, show_all=command.show_all),
        )

    if command.name in {"backfill", "backfill-apply"}:
        from pipelines.listing.point_zero.backfill_apply import (
            _release_lock,
            _try_acquire_lock,
            get_fresh_connection,
            run_backfill,
        )

        lock_conn = get_fresh_connection()
        if not _try_acquire_lock(lock_conn):
            lock_conn.close()
            return _post_channel_message(
                channel_id=channel_id,
                text="Backfill is already running locally; skipping this Slack request.",
            )

        try:
            summary = run_backfill(
                limit=int(command.limit or DEFAULT_SLACK_BACKFILL_LIMIT),
                apply=True,
                daily_backfill_budget=int(command.daily_limit or DEFAULT_SLACK_BACKFILL_DAILY_LIMIT),
                include_vehicle_sources=bool(command.include_vehicle_sources),
            )
        finally:
            _release_lock(lock_conn)
            lock_conn.close()

        return _post_channel_message(
            channel_id=channel_id,
            text=_format_backfill_summary_for_slack(summary),
        )

    return False


def _last_cursor() -> str | None:
    flag = get_flag(CURSOR_FLAG_NAME)
    if not flag:
        return None
    payload = flag.get("payload") or {}
    value = str(payload.get("last_ts") or "").strip()
    return value or None


def _save_cursor(last_ts: str | None) -> None:
    if last_ts:
        set_flag(CURSOR_FLAG_NAME, True, {"last_ts": last_ts})


def _fetch_messages(*, channel_id: str, oldest: str | None, limit: int) -> list[dict[str, Any]]:
    params: dict[str, Any] = {
        "channel": channel_id,
        "limit": limit,
    }
    if oldest:
        params["oldest"] = oldest
        params["inclusive"] = "false"

    response = requests.get(
        "https://slack.com/api/conversations.history",
        headers=_headers(),
        params=params,
        timeout=_timeout_seconds(),
    )
    payload = response.json()
    if not payload.get("ok"):
        raise RuntimeError(f"Slack conversations.history failed: {payload.get('error') or response.status_code}")
    messages = payload.get("messages") or []
    return [message for message in messages if isinstance(message, dict)]


def _post_reply(*, channel_id: str, thread_ts: str, text: str) -> bool:
    response = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={**_headers(), "Content-Type": "application/json; charset=utf-8"},
        json={
            "channel": channel_id,
            "thread_ts": thread_ts,
            "reply_broadcast": True,
            "text": text,
            "unfurl_links": False,
            "unfurl_media": False,
        },
        timeout=_timeout_seconds(),
    )
    payload = response.json()
    return bool(payload.get("ok"))


def process_slack_review_commands_once(*, limit: int = 50, reply: bool = True) -> SlackReviewSyncResult:
    if not slack_review_commands_enabled():
        return SlackReviewSyncResult(status="skipped", enabled=False, reason="disabled")

    channel_id = _channel_id()
    if not channel_id:
        return SlackReviewSyncResult(
            status="skipped",
            enabled=True,
            reason="missing_channel_id",
        )
    if not _bot_token():
        return SlackReviewSyncResult(
            status="skipped",
            enabled=True,
            reason="missing_bot_token",
        )

    try:
        cursor = _last_cursor()
        messages = _fetch_messages(channel_id=channel_id, oldest=cursor, limit=limit)
    except Exception as exc:
        return SlackReviewSyncResult(
            status="failed",
            enabled=True,
            error=_safe_error(exc),
        )

    newest_ts = cursor
    processed = 0
    ignored = 0
    replied = 0

    for message in sorted(messages, key=lambda item: str(item.get("ts") or "")):
        ts = str(message.get("ts") or "").strip()
        if ts and (not newest_ts or ts > newest_ts):
            newest_ts = ts

        subtype = str(message.get("subtype") or "")
        if subtype:
            ignored += 1
            continue

        text = str(message.get("text") or "")
        if parse_slack_help_command(text):
            processed += 1
            if reply and ts:
                if _post_reply(
                    channel_id=channel_id,
                    thread_ts=ts,
                    text=format_slack_operator_command_menu(),
                ):
                    replied += 1
            continue

        operator_command = parse_slack_operator_command(text)
        if operator_command:
            processed += 1
            if reply:
                if ts:
                    display = f"ebay {operator_command.name}"
                    if operator_command.item:
                        display = f"{display} {operator_command.item}"
                    if operator_command.show_all:
                        display = f"{display} --all"
                    if operator_command.name in {"backfill", "backfill-apply"}:
                        display = (
                            f"ebay backfill --limit {operator_command.limit or DEFAULT_SLACK_BACKFILL_LIMIT} "
                            f"--daily-limit {operator_command.daily_limit or DEFAULT_SLACK_BACKFILL_DAILY_LIMIT}"
                        )
                        if operator_command.include_vehicle_sources:
                            display = f"{display} --include-vehicle-sources"
                    _post_reply(
                        channel_id=channel_id,
                        thread_ts=ts,
                        text=f"Running `{display}`...",
                    )
                    if operator_command.name in {"backfill", "backfill-apply"}:
                        _save_cursor(newest_ts)
                if run_slack_operator_command(operator_command, channel_id=channel_id):
                    replied += 1
                elif ts and _post_reply(
                    channel_id=channel_id,
                    thread_ts=ts,
                    text=f"Slack command failed: `ebay {operator_command.name}`. Check local logs.",
                ):
                    replied += 1
            continue

        command = parse_slack_review_command(text)
        if not command:
            ignored += 1
            continue

        try:
            row = record_review_action(
                external_id=command.external_id,
                action=command.action,
            )
        except Exception as exc:
            ignored += 1
            if reply and ts:
                if _post_reply(
                    channel_id=channel_id,
                    thread_ts=ts,
                    text=f"Review command failed: {exc}",
                ):
                    replied += 1
            continue

        processed += 1
        if reply and ts:
            label = "favourited" if row.get("action") == "up" else "excluded"
            title = row.get("title") or row.get("external_id")
            if _post_reply(
                channel_id=channel_id,
                thread_ts=ts,
                text=f"{label}: {title}",
            ):
                replied += 1

    _save_cursor(newest_ts)
    return SlackReviewSyncResult(
        status="ok",
        enabled=True,
        fetched=len(messages),
        processed=processed,
        ignored=ignored,
        replied=replied,
        cursor=newest_ts,
    )


def run_slack_review_command_loop(
    *,
    poll_seconds: int = DEFAULT_POLL_SECONDS,
    limit: int = 50,
    reply: bool = True,
) -> None:
    while True:
        result = process_slack_review_commands_once(limit=limit, reply=reply)
        print(result.as_dict(), flush=True)
        time.sleep(max(int(poll_seconds), 1))
