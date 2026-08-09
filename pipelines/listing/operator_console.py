"""Small operator console for the eBay side-hustle pipeline."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from pipelines.listing.market_discovery.graph import useful_near_misses
from pipelines.listing.market_discovery.graph import MIN_CANDIDATE_COMPS
from utils.ebay_api_accounting import (
    BUCKET_LABELS,
    flatten_rate_limits,
    fetch_ebay_rate_limits,
)


ROOT = Path(__file__).resolve().parents[2]
PYTHON = Path(sys.executable)
DEFAULT_BACKFILL_BATCH_SIZE = 100


@dataclass(frozen=True)
class ConsoleCommand:
    """One command exposed by the local operator console."""

    name: str
    description: str
    safety: str
    example: str


COMMANDS: tuple[ConsoleCommand, ...] = (
    ConsoleCommand(
        "serious",
        "Show live potentials at the normal GBP 50 profit gate.",
        "read-only",
        "ebay serious",
    ),
    ConsoleCommand(
        "punt",
        "Show live potentials at the GBP 25 punt gate.",
        "read-only",
        "ebay punt",
    ),
    ConsoleCommand(
        "potentials",
        "Show live potentials with custom profit/comps limits.",
        "read-only",
        "ebay potentials --min-profit 35 --limit 20",
    ),
    ConsoleCommand(
        "explain",
        "Show the sold comps behind one listing's profit estimate.",
        "read-only",
        "ebay explain 168181595699",
    ),
    ConsoleCommand(
        "stats",
        "Show model-key, final-price, comp, and live coverage counts.",
        "read-only",
        "ebay stats",
    ),
    ConsoleCommand(
        "api-health",
        "Show eBay API usage grouped by real quota bucket.",
        "read-only; --live-ebay spends one Analytics API call",
        "ebay api-health",
    ),
    ConsoleCommand(
        "backfill-plan",
        "Show which unknown rows the final-price backfill would spend calls on.",
        "read-only",
        "ebay backfill-plan",
    ),
    ConsoleCommand(
        "backfill-dry-run",
        "Classify a small unknown batch without writing outcomes.",
        "spends eBay API calls",
        "ebay backfill-dry-run --limit 250 --daily-limit 5000",
    ),
    ConsoleCommand(
        "backfill-apply",
        "Recover final prices and resolve local unknown rows.",
        "spends eBay API calls + writes local DB",
        "ebay backfill-apply --limit 250 --daily-limit 5000",
    ),
    ConsoleCommand(
        "comps",
        "Rebuild confirmed sold comps from local final prices.",
        "writes local DB",
        "ebay comps",
    ),
    ConsoleCommand(
        "model-keys-dry-run",
        "Preview all-source model-key and condition-grade rebuild.",
        "read-only",
        "ebay model-keys-dry-run",
    ),
    ConsoleCommand(
        "model-keys-apply",
        "Apply all-source model-key and condition-grade rebuild.",
        "writes local DB",
        "ebay model-keys-apply",
    ),
    ConsoleCommand(
        "apple-keys-dry-run",
        "Preview strict Apple/Mac model-key and condition-grade repair.",
        "read-only",
        "ebay apple-keys-dry-run",
    ),
    ConsoleCommand(
        "apple-keys-apply",
        "Apply strict Apple/Mac model-key and condition-grade repair.",
        "writes local DB",
        "ebay apple-keys-apply",
    ),
    ConsoleCommand(
        "actioncam-keys-dry-run",
        "Preview strict Actioncam model-key and condition-grade repair.",
        "read-only",
        "ebay actioncam-keys-dry-run",
    ),
    ConsoleCommand(
        "actioncam-keys-apply",
        "Apply strict Actioncam model-key and condition-grade repair.",
        "writes local DB",
        "ebay actioncam-keys-apply",
    ),
    ConsoleCommand(
        "lego-keys-dry-run",
        "Preview strict LEGO model-key and condition-grade repair.",
        "read-only",
        "ebay lego-keys-dry-run",
    ),
    ConsoleCommand(
        "lego-keys-apply",
        "Apply strict LEGO model-key and condition-grade repair.",
        "writes local DB",
        "ebay lego-keys-apply",
    ),
    ConsoleCommand(
        "migrations",
        "Check pending database migrations.",
        "read-only",
        "ebay migrations",
    ),
    ConsoleCommand(
        "test",
        "Run the unit test suite.",
        "read-only",
        "ebay test",
    ),
    ConsoleCommand(
        "heartbeat-once",
        "Run one full heartbeat tick.",
        "may hit APIs/send notifications",
        "ebay heartbeat-once",
    ),
    ConsoleCommand(
        "heartbeat-email",
        "Send the current heartbeat email now.",
        "sends email",
        "ebay heartbeat-email",
    ),
    ConsoleCommand(
        "review-server",
        "Run the local Up/Down click handler for heartbeat emails.",
        "writes local DB",
        "ebay review-server",
    ),
    ConsoleCommand(
        "review-up",
        "Mark a listing as a local favourite.",
        "writes local DB",
        "ebay review-up 168181595699",
    ),
    ConsoleCommand(
        "review-down",
        "Exclude a listing from future local candidate lists.",
        "writes local DB",
        "ebay review-down 168181595699",
    ),
    ConsoleCommand(
        "favourites",
        "Show locally thumbed-up listings.",
        "read-only",
        "ebay favourites",
    ),
    ConsoleCommand(
        "reviews",
        "Show latest local up/down actions.",
        "read-only",
        "ebay reviews",
    ),
    ConsoleCommand(
        "slack-review-once",
        "Poll Slack once for up/down review commands.",
        "reads Slack + writes local DB",
        "ebay slack-review-once",
    ),
    ConsoleCommand(
        "slack-review-loop",
        "Continuously poll Slack for up/down review commands.",
        "reads Slack + writes local DB",
        "ebay slack-review-loop",
    ),
)


def command_menu() -> str:
    """Return the help text printed by `ebay` with no args."""

    lines = [
        "GhostFrog eBay console",
        "",
        "Mission: buy cheap from eBay to resell on eBay for profit.",
        "",
        "Useful commands:",
    ]
    width = max(len(command.name) for command in COMMANDS)
    for command in COMMANDS:
        lines.append(
            f"  {command.name.ljust(width)}  {command.description} [{command.safety}]"
        )
    lines.extend(
        [
            "",
            "Examples:",
            "  ebay serious",
            "  ebay punt",
            "  ebay stats",
            "  ebay explain 168181595699",
            "  ebay api-health",
            "  ebay api-health --live-ebay",
            "  ebay potentials --min-profit 35 --min-comps 15 --limit 20",
            "  ebay backfill-plan",
            "  ebay backfill-dry-run --limit 250 --daily-limit 5000",
            "  ebay backfill-apply --limit 250 --daily-limit 5000",
            "  ebay model-keys-dry-run",
            "  ebay model-keys-apply",
            "  ebay actioncam-keys-dry-run",
            "  ebay actioncam-keys-apply",
            "  ebay lego-keys-dry-run",
            "  ebay lego-keys-apply",
            "  ebay heartbeat-email",
            "  ebay review-server",
            "  ebay review-down 168181595699",
            "  ebay slack-review-once",
            "  ebay favourites",
            "  ebay reviews",
            "",
            "Run `ebay <command> --help` for command options.",
        ]
    )
    return "\n".join(lines)


def _run(args: Sequence[str]) -> int:
    env = os.environ.copy()
    return subprocess.run(list(args), cwd=ROOT, env=env, check=False).returncode


def _python_module(module: str, *args: str) -> int:
    return _run([str(PYTHON), "-m", module, *args])


def _money(value: object) -> str:
    try:
        return f"GBP {float(value):,.2f}"
    except (TypeError, ValueError):
        return "GBP n/a"


def _pct(value: object) -> str:
    try:
        return f"{float(value) * 100:.0f}%"
    except (TypeError, ValueError):
        return "0%"


def _max_safe_buy(value: object) -> str:
    try:
        amount = float(value)
    except (TypeError, ValueError):
        return "max GBP n/a"
    if amount < 0:
        return "max no positive buy"
    return f"max {_money(amount)}"


def _shortfall_to_gate(row: dict[str, object]) -> str:
    try:
        profit = float(row.get("expected_profit"))
        max_safe_buy = float(row.get("max_safe_buy"))
    except (TypeError, ValueError):
        return ""
    if profit < 0 or max_safe_buy < 0:
        return ""
    try:
        gap = float(row.get("price_gap_to_max_buy"))
    except (TypeError, ValueError):
        return ""
    if gap >= 0:
        return ""
    return f" | short {_money(abs(gap))}"


def _short(text: object, limit: int = 96) -> str:
    cleaned = " ".join(str(text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 3].rstrip()}..."


def print_potentials_report(report: dict[str, object]) -> None:
    """Print the live potentials report in a human operator format."""

    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    thresholds = report.get("thresholds") if isinstance(report.get("thresholds"), dict) else {}
    potentials = report.get("potentials") if isinstance(report.get("potentials"), list) else []
    closest = report.get("closest") if isinstance(report.get("closest"), list) else []

    print("Live eBay potentials")
    print(
        "Gate: "
        f"profit >= {_money(thresholds.get('min_profit_gbp'))}, "
        f"comps >= {int(thresholds.get('min_comps') or 0)}"
    )
    print(
        "Checked: "
        f"{int(summary.get('checked') or 0)} rows; "
        f"{int(summary.get('potentials') or 0)} potentials; "
        f"{int(summary.get('live_rows_with_comps') or 0)} live rows with comps"
    )
    print("")

    if potentials:
        print("Potentials:")
        for index, row in enumerate(potentials, start=1):
            if not isinstance(row, dict):
                continue
            print(f"{index}. {_short(row.get('title'))}")
            print(
                f"   key {row.get('model_key')} | grade {row.get('condition_grade') or 'n/a'} | "
                f"current {_money(row.get('current_price'))} | sold {_money(row.get('sold_median'))} | "
                f"comps {int(row.get('comp_samples') or 0)}"
            )
            print(
                f"   max {_money(row.get('max_safe_buy'))} | "
                f"profit {_money(row.get('expected_profit'))} | ROI {_pct(row.get('expected_roi'))}"
            )
            print(f"   {row.get('url')}")
            print("")
        return

    print("No potentials cleared the gate.")
    min_profit = float(thresholds.get("min_profit_gbp") or 0)
    near_misses = useful_near_misses(
        [row for row in closest if isinstance(row, dict)],
        min_profit=min_profit,
        limit=10,
    )
    if near_misses:
        print("")
        print("Near misses:")
        for index, row in enumerate(near_misses, start=1):
            if not isinstance(row, dict):
                continue
            print(
                f"{index}. {_short(row.get('title'))} | current {_money(row.get('current_price'))} | "
                f"{_max_safe_buy(row.get('max_safe_buy'))} | "
                f"profit {_money(row.get('expected_profit'))}{_shortfall_to_gate(row)}"
            )
            print(f"   {row.get('url')}")
            print("")
    else:
        print("")
        print(
            "No actionable near misses within GBP 10.00 of the "
            f"GBP {min_profit:.2f} gate at or below max buy."
        )


def run_potentials(*, limit: int, min_profit: float, min_comps: int, min_roi: float, as_json: bool) -> int:
    from pipelines.listing.market_discovery.graph import run as run_market_discovery

    report = run_market_discovery(
        limit=limit,
        min_profit=min_profit,
        min_comps=min_comps,
        min_roi=min_roi,
    )
    if as_json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print_potentials_report(report)
    return 0


def _metric_int(mapping: object, key: str) -> int:
    if not isinstance(mapping, dict):
        return 0
    try:
        return int(mapping.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def build_stats() -> dict[str, Any]:
    """Map shared heartbeat metrics into the operator console stats shape."""

    from pipelines.listing.operator_metrics import build_operator_metrics

    metrics = build_operator_metrics()
    counts = metrics.get("listing_counts") or {}
    comps = metrics.get("comps") or {}
    coverage = metrics.get("live_comp_coverage") or {}
    backfill_activity = metrics.get("backfill_activity") or {}
    roi = metrics.get("high_risk_roi") or {}
    backfill = metrics.get("backfill_plan") or {}
    api_budget = backfill.get("api_budget") if isinstance(backfill, dict) else {}
    api_budget = api_budget if isinstance(api_budget, dict) else {}
    api_accounting = metrics.get("api_accounting") if isinstance(metrics.get("api_accounting"), dict) else {}

    total = _metric_int(counts, "total")
    missing_keys = _metric_int(counts, "missing_model_key")
    live_total = _metric_int(coverage, "live_total")
    live_unknown_keys = _metric_int(coverage, "live_unknown_model_key")

    return {
        "total_listings": total,
        "known_model_keys": max(total - missing_keys, 0),
        "unknown_model_keys": missing_keys,
        "confirmed_final_prices": _metric_int(counts, "final_prices"),
        "final_prices_with_known_model_key": _metric_int(counts, "final_prices_with_known_model_key"),
        "comp_buckets": _metric_int(comps, "comp_keys"),
        "comp_samples": _metric_int(comps, "comp_samples"),
        "comp_buckets_3plus": _metric_int(comps, "comp_keys_3plus"),
        "live_listings": live_total,
        "unknown_listings": _metric_int(counts, "unknown"),
        "unknown_queue": _metric_int(
            counts,
            "unknown_backfill_due" if counts.get("unknown_backfill_due") is not None else "unknown_unfinalized",
        ),
        "live_with_known_key": max(live_total - live_unknown_keys, 0),
        "live_with_same_grade_comps": _metric_int(coverage, "live_with_comps"),
        "live_with_3plus_same_grade_comps": _metric_int(coverage, "live_with_3plus_comps"),
        "today_calls": _metric_int(metrics, "api_total"),
        "api_accounting": api_accounting,
        "planned_normal_calls": _metric_int(api_budget, "planned_normal_calls"),
        "spare_calls": _metric_int(api_budget, "theoretical_spare"),
        "backfill_checked_today": _metric_int(backfill_activity, "checked_today"),
        "backfill_sold_today": _metric_int(backfill_activity, "sold_today"),
        "current_roi_rows": _metric_int(roi, "live_roi_gt_15"),
        "current_roi_rows_above_100": _metric_int(roi, "live_roi_gt_100"),
    }


def _coverage_pct(stats: dict[str, int]) -> float:
    live = int(stats.get("live_listings") or 0)
    covered = int(stats.get("live_with_3plus_same_grade_comps") or 0)
    return (covered / live * 100.0) if live else 0.0


def _fmt(value: object) -> str:
    try:
        return f"{int(value or 0):,}"
    except (TypeError, ValueError):
        return "0"


def _print_rule(title: str) -> None:
    print("")
    print(f"== {title} ==")


def _print_stat(label: str, value: object, note: str = "") -> None:
    suffix = f"  {note}" if note else ""
    print(f"  {label.ljust(30)} {_fmt(value).rjust(10)}{suffix}")


def _bucket_line(info: dict[str, Any]) -> str:
    used = int(info.get("used") or 0)
    limit = int(info.get("limit") or 0)
    remaining = int(info.get("remaining") or 0)
    status = "BLOCKED" if info.get("blocked") else "ok"
    return f"{_fmt(used)} / {_fmt(limit)} used, {_fmt(remaining)} left, {status}"


def _api_bucket_subtitle(api_accounting: object) -> str:
    if not isinstance(api_accounting, dict):
        return "no bucket data"
    buckets = api_accounting.get("buckets")
    if not isinstance(buckets, dict):
        return "no bucket data"
    trading = buckets.get("trading") if isinstance(buckets.get("trading"), dict) else {}
    browse = buckets.get("browse") if isinstance(buckets.get("browse"), dict) else {}
    return f"T {_fmt(trading.get('used'))}/{_fmt(trading.get('limit'))} | B {_fmt(browse.get('used'))}/{_fmt(browse.get('limit'))}"


def _card_lines(title: str, value: str, subtitle: str) -> list[str]:
    width = 35
    return [
        "+" + "-" * width + "+",
        f"| {title.upper().ljust(width - 2)} |",
        f"| {value.ljust(width - 2)} |",
        f"| {subtitle[: width - 2].ljust(width - 2)} |",
        "+" + "-" * width + "+",
    ]


def _print_card_grid(cards: list[tuple[str, str, str]]) -> None:
    rendered = [_card_lines(title, value, subtitle) for title, value, subtitle in cards]
    for row_start in range(0, len(rendered), 2):
        row = rendered[row_start : row_start + 2]
        for line_index in range(len(row[0])):
            print("  ".join(card[line_index] for card in row))
        print("")


def print_stats(stats: dict[str, int]) -> None:
    """Print the compact data-health block the operator asked for."""

    print("GhostFrog eBay stats")
    print("Mission: buy cheap from eBay to resell on eBay for profit.")
    print("")
    _print_card_grid(
        [
            ("Live listings", _fmt(stats["live_listings"]), f"{_fmt(stats['total_listings'])} total tracked"),
            ("Confirmed comps", _fmt(stats["comp_buckets"]), f"{_fmt(stats['comp_samples'])} final-price samples"),
            (
                "Live comp coverage",
                f"{_coverage_pct(stats):.0f}%",
                f"{_fmt(stats['live_with_3plus_same_grade_comps'])} of {_fmt(stats['live_listings'])} have 3+ comps",
            ),
            (
                "API today",
                _fmt(stats["today_calls"]),
                _api_bucket_subtitle(stats.get("api_accounting")),
            ),
            (
                "Final prices",
                _fmt(stats["confirmed_final_prices"]),
                "confirmed sold evidence rows",
            ),
            (
                "Unknown queue",
                _fmt(stats["unknown_queue"]),
                "past-end rows for Trading",
            ),
            (
                "Backfill today",
                _fmt(stats["backfill_checked_today"]),
                f"{_fmt(stats['backfill_sold_today'])} sold confirmed",
            ),
            (
                "Current ROI rows",
                _fmt(stats["current_roi_rows"]),
                f"{_fmt(stats['current_roi_rows_above_100'])} above 100% ROI",
            ),
        ]
    )
    _print_rule("Data Quality")
    _print_stat("Total listings", stats["total_listings"])
    _print_stat("Known model keys", stats["known_model_keys"])
    _print_stat("Unknown/missing keys", stats["unknown_model_keys"])
    _print_stat("Confirmed final prices", stats["confirmed_final_prices"])
    _print_stat("Final prices with known key", stats["final_prices_with_known_model_key"])

    _print_rule("Comps And Live Coverage")
    _print_stat("Comp buckets", stats["comp_buckets"])
    _print_stat("Comp buckets with 3+ sold", stats["comp_buckets_3plus"])
    _print_stat("Live listings", stats["live_listings"])
    _print_stat("Unknown listings", stats["unknown_listings"])
    _print_stat("Live with known key", stats["live_with_known_key"])
    _print_stat("Live with same-grade comps", stats["live_with_same_grade_comps"])
    _print_stat("Live with 3+ same-grade comps", stats["live_with_3plus_same_grade_comps"])

    _print_api_accounting(stats.get("api_accounting"))


def _print_api_accounting(api_accounting: object) -> None:
    _print_rule("API Buckets")
    if not isinstance(api_accounting, dict) or not isinstance(api_accounting.get("buckets"), dict):
        print("  No bucket accounting data available.")
        return

    buckets = api_accounting["buckets"]
    for bucket in ("trading", "browse", "identity", "developer_analytics"):
        info = buckets.get(bucket)
        if not isinstance(info, dict):
            continue
        label = str(info.get("label") or BUCKET_LABELS.get(bucket, bucket))
        print(f"  {label.ljust(30)} {_bucket_line(info)}")

    unknown_services = api_accounting.get("unknown_services") or []
    if unknown_services:
        print("")
        print(f"  Unknown local counters: {', '.join(str(s) for s in unknown_services)}")


def run_stats(*, as_json: bool = False) -> int:
    stats = build_stats()
    if as_json:
        print(json.dumps(stats, indent=2, default=str))
    else:
        print_stats(stats)
    return 0


def _live_rate_row_interesting(row: dict[str, Any]) -> bool:
    context = str(row.get("api_context") or "").lower()
    api_name = str(row.get("api_name") or "").lower()
    resource = str(row.get("resource") or "").lower()
    if api_name == "tradingapi" and resource == "getitem":
        return True
    if context == "buy" and api_name == "browse":
        return True
    if context == "commerce" and "identity" in api_name:
        return True
    if context == "developer" and "analytics" in api_name:
        return True
    return False


def run_api_health(*, live_ebay: bool = False, as_json: bool = False) -> int:
    from pipelines.listing.operator_metrics import build_operator_metrics

    live_rows: list[dict[str, Any]] = []
    if live_ebay:
        from utils.auth import get_auth

        api_base = os.getenv("EBAY_API_BASE", "https://api.ebay.com")
        live_payload = fetch_ebay_rate_limits(token=get_auth().get_token(), api_base=api_base)
        live_rows = [
            row for row in flatten_rate_limits(live_payload)
            if _live_rate_row_interesting(row)
        ]

    metrics = build_operator_metrics()
    api_accounting = metrics.get("api_accounting") or {}
    payload: dict[str, Any] = {
        "local": api_accounting,
        "api_today": metrics.get("api_today") or [],
    }
    if live_ebay:
        payload["ebay_live"] = live_rows

    if as_json:
        print(json.dumps(payload, indent=2, default=str))
        return 0

    print("GhostFrog eBay API health")
    print("Local counters grouped into real eBay quota buckets.")
    print("Local counters use the DB calendar day; live eBay rows show eBay's own reset window.")
    _print_api_accounting(api_accounting)

    print("")
    _print_rule("Local Counters")
    for row in metrics.get("api_today") or []:
        if not isinstance(row, dict):
            continue
        print(f"  {str(row.get('service') or '').ljust(38)} {_fmt(row.get('call_count')).rjust(10)}")

    if live_ebay:
        print("")
        _print_rule("eBay Live Limits")
        if not live_rows:
            print("  No matching live eBay rate-limit rows returned.")
        for row in live_rows:
            name = " ".join(
                str(row.get(key) or "")
                for key in ("api_context", "api_name", "resource")
            ).strip()
            limit = _fmt(row.get("limit"))
            count = _fmt(row.get("count"))
            remaining = _fmt(row.get("remaining"))
            reset = str(row.get("reset") or "n/a")
            print(f"  {name[:38].ljust(38)} {count}/{limit} used, {remaining} left, reset {reset}")
    else:
        print("")
        print("Run `ebay api-health --live-ebay` to ask eBay's Developer Analytics API for the live quota view.")

    return 0


def _print_review_rows(rows: list[dict[str, object]]) -> None:
    if not rows:
        print("No review rows yet.")
        return
    for index, row in enumerate(rows, start=1):
        title = _short(row.get("title") or row.get("external_id"), limit=90)
        bits = [
            str(row.get("source") or "unknown"),
            str(row.get("external_id") or ""),
        ]
        if row.get("status"):
            bits.append(f"status {row.get('status')}")
        if row.get("price_current") is not None:
            bits.append(f"current {_money(row.get('price_current'))}")
        print(f"{index}. {title}")
        print(f"   {' | '.join(bits)}")
        if row.get("url"):
            print(f"   {row.get('url')}")
        print("")


def record_review_from_cli(item_id: str, action: str) -> int:
    from pipelines.listing.review_actions import record_review_action

    row = record_review_action(external_id=item_id, action=action)
    label = "favourited" if action == "up" else "excluded"
    print(f"{label}: {row.get('external_id')}")
    if row.get("title"):
        print(row.get("title"))
    return 0


def run_favourites(*, limit: int, as_json: bool = False) -> int:
    from pipelines.listing.review_actions import list_favourites

    rows = list_favourites(limit=limit)
    if as_json:
        print(json.dumps(rows, indent=2, default=str))
    else:
        print("Local favourites")
        _print_review_rows(rows)
    return 0


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be zero or greater")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ebay",
        description="Operator console for the local eBay opportunity system.",
    )
    sub = parser.add_subparsers(dest="command")

    for name, default_profit in (("serious", 50.0), ("punt", 25.0)):
        p = sub.add_parser(name, help=f"Show GBP {default_profit:.0f}+ live potentials.")
        p.add_argument("--limit", type=_positive_int, default=20)
        p.add_argument("--min-comps", type=_positive_int, default=MIN_CANDIDATE_COMPS)
        p.add_argument("--json", action="store_true", help="Print raw JSON instead of the operator list.")
        p.set_defaults(default_profit=default_profit)

    p = sub.add_parser("potentials", help="Show live potentials with custom thresholds.")
    p.add_argument("--limit", type=_positive_int, default=20)
    p.add_argument("--min-profit", type=_non_negative_float, default=50.0)
    p.add_argument("--min-comps", type=_positive_int, default=MIN_CANDIDATE_COMPS)
    p.add_argument("--min-roi", type=_non_negative_float, default=0.0)
    p.add_argument("--json", action="store_true", help="Print raw JSON instead of the operator list.")

    p = sub.add_parser("explain", help="Show the sold comps behind one listing's profit estimate.")
    p.add_argument("item", help="eBay item URL, Browse ID, or bare numeric item ID.")
    p.add_argument("--limit", type=_positive_int, default=25)
    p.add_argument("--all", action="store_true", help="Show every matching sold comp row.")

    p = sub.add_parser("stats", help="Show model-key/final-price/comp coverage counts.")
    p.add_argument("--json", action="store_true", help="Print raw JSON instead of text.")

    p = sub.add_parser("api-health", help="Show eBay API usage by real quota bucket.")
    p.add_argument("--json", action="store_true", help="Print raw JSON instead of text.")
    p.add_argument(
        "--live-ebay",
        action="store_true",
        help="Also fetch eBay's Developer Analytics rate-limit view. Spends one Analytics API call.",
    )

    p = sub.add_parser("backfill-plan", help="Show targeted final-price backfill plan.")
    p.add_argument("--candidate-detail-limit", type=_positive_int, default=10)
    p.add_argument("--max-backfill-per-day", type=_positive_int, default=1000)
    p.add_argument("--include-vehicle-sources", action="store_true")

    for name, apply in (("backfill-dry-run", False), ("backfill-apply", True)):
        p = sub.add_parser(name, help="Run a bounded final-price backfill batch.")
        p.add_argument("--limit", type=_positive_int, default=DEFAULT_BACKFILL_BATCH_SIZE)
        p.add_argument(
            "--daily-limit",
            "--daily-backfill-budget",
            dest="daily_limit",
            type=_positive_int,
            default=1000,
            help="Maximum eBay ended-item checks this command may spend today.",
        )
        p.add_argument("--include-vehicle-sources", action="store_true")
        p.set_defaults(apply=apply)

    sub.add_parser("comps", help="Rebuild confirmed sold comps.")

    for name, dry_run in (("model-keys-dry-run", True), ("model-keys-apply", False)):
        p = sub.add_parser(name, help="Rebuild model keys and condition grades across all sources.")
        p.add_argument("--limit", type=_positive_int, default=200000)
        p.add_argument("--source", default=None, help="Optional source filter, e.g. ebay-apple.")
        p.set_defaults(dry_run=dry_run)

    for name, dry_run in (("apple-keys-dry-run", True), ("apple-keys-apply", False)):
        p = sub.add_parser(name, help="Repair Apple/Mac keys and condition grades.")
        p.add_argument("--limit", type=_positive_int, default=40000)
        p.set_defaults(dry_run=dry_run)

    for name, dry_run in (("actioncam-keys-dry-run", True), ("actioncam-keys-apply", False)):
        p = sub.add_parser(name, help="Repair Actioncam keys and condition grades.")
        p.add_argument("--limit", type=_positive_int, default=10000)
        p.set_defaults(dry_run=dry_run)

    for name, dry_run in (("lego-keys-dry-run", True), ("lego-keys-apply", False)):
        p = sub.add_parser(name, help="Repair LEGO keys and condition grades.")
        p.add_argument("--limit", type=_positive_int, default=30000)
        p.set_defaults(dry_run=dry_run)

    sub.add_parser("migrations", help="Check pending database migrations.")
    sub.add_parser("test", help="Run unit tests.")
    sub.add_parser("heartbeat-once", help="Run one full heartbeat tick.")
    p = sub.add_parser("heartbeat-email", help="Send the current heartbeat email now.")
    p.add_argument(
        "--to",
        action="append",
        default=[],
        help="Send to this address instead of the configured heartbeat recipients. Repeat for multiple recipients.",
    )
    p = sub.add_parser("review-server", help="Run the local Up/Down click handler.")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=_positive_int, default=8765)

    for name, action in (("review-up", "up"), ("review-down", "down")):
        p = sub.add_parser(name, help=f"Record a local {action} review action.")
        p.add_argument("item", help="eBay item URL, Browse ID, or bare numeric item ID.")
        p.set_defaults(review_action=action)

    p = sub.add_parser("favourites", help="Show locally thumbed-up listings.")
    p.add_argument("--limit", type=_positive_int, default=100)
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("reviews", help="Show latest local review actions.")
    p.add_argument("--limit", type=_positive_int, default=50)
    p.add_argument("--json", action="store_true")

    p = sub.add_parser("slack-review-once", help="Poll Slack once for up/down review commands.")
    p.add_argument("--limit", type=_positive_int, default=50)
    p.add_argument("--no-reply", action="store_true", help="Do not reply to processed Slack messages.")

    p = sub.add_parser("slack-review-loop", help="Continuously poll Slack for up/down review commands.")
    p.add_argument("--limit", type=_positive_int, default=50)
    p.add_argument("--poll-seconds", type=_positive_int, default=60)
    p.add_argument("--no-reply", action="store_true", help="Do not reply to processed Slack messages.")

    sub.add_parser("commands", help="Print the command menu.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args_list = list(sys.argv[1:] if argv is None else argv)
    if not args_list or args_list[0] in {"help", "--help", "-h", "commands"}:
        print(command_menu())
        return 0

    parser = build_parser()
    args = parser.parse_args(args_list)

    if args.command in {"serious", "punt"}:
        return run_potentials(
            limit=args.limit,
            min_profit=args.default_profit,
            min_comps=args.min_comps,
            min_roi=0.0,
            as_json=bool(args.json),
        )

    if args.command == "potentials":
        return run_potentials(
            limit=args.limit,
            min_profit=args.min_profit,
            min_comps=args.min_comps,
            min_roi=args.min_roi,
            as_json=bool(args.json),
        )

    if args.command == "explain":
        from pipelines.listing.explain import explain_listing

        print(explain_listing(args.item, limit=args.limit, show_all=bool(args.all)))
        return 0

    if args.command == "stats":
        return run_stats(as_json=bool(args.json))

    if args.command == "api-health":
        return run_api_health(live_ebay=bool(args.live_ebay), as_json=bool(args.json))

    if args.command == "backfill-plan":
        cmd = [
            "pipelines.listing.point_zero",
            "--candidate-detail-limit",
            str(args.candidate_detail_limit),
            "--max-backfill-per-day",
            str(args.max_backfill_per_day),
        ]
        if args.include_vehicle_sources:
            cmd.append("--include-vehicle-sources")
        return _python_module(*cmd)

    if args.command in {"backfill-dry-run", "backfill-apply"}:
        cmd = [
            "pipelines.listing.point_zero.backfill_apply",
            "--limit",
            str(args.limit),
            "--daily-limit",
            str(args.daily_limit),
        ]
        if args.include_vehicle_sources:
            cmd.append("--include-vehicle-sources")
        if args.apply:
            cmd.append("--apply")
        return _python_module(*cmd)

    if args.command == "comps":
        return _python_module("pipelines.listing.comps")

    if args.command in {"model-keys-dry-run", "model-keys-apply"}:
        cmd = [
            "pipelines.listing.attributes.rebuild_model_keys",
            "--all",
            "--limit",
            str(args.limit),
        ]
        if args.source:
            cmd.extend(["--source", args.source])
        if args.dry_run:
            cmd.append("--dry-run")
        return _python_module(*cmd)

    if args.command in {"apple-keys-dry-run", "apple-keys-apply"}:
        cmd = [
            "pipelines.listing.attributes.rebuild_model_keys",
            "--all",
            "--source",
            "ebay-apple",
            "--limit",
            str(args.limit),
        ]
        if args.dry_run:
            cmd.append("--dry-run")
        return _python_module(*cmd)

    if args.command in {"actioncam-keys-dry-run", "actioncam-keys-apply"}:
        cmd = [
            "pipelines.listing.attributes.rebuild_model_keys",
            "--all",
            "--source",
            "ebay-actioncams",
            "--limit",
            str(args.limit),
        ]
        if args.dry_run:
            cmd.append("--dry-run")
        return _python_module(*cmd)

    if args.command in {"lego-keys-dry-run", "lego-keys-apply"}:
        cmd = [
            "pipelines.listing.attributes.rebuild_model_keys",
            "--all",
            "--source",
            "ebay-lego",
            "--limit",
            str(args.limit),
        ]
        if args.dry_run:
            cmd.append("--dry-run")
        return _python_module(*cmd)

    if args.command == "migrations":
        return _run([str(PYTHON), "scripts/run_migrations.py", "--dry-run"])

    if args.command == "test":
        return _python_module("unittest", "discover", "tests")

    if args.command == "heartbeat-once":
        return _python_module("pipelines.listing.heartbeat")

    if args.command == "heartbeat-email":
        from pipelines.listing.main import send_heartbeat_email_now

        result = send_heartbeat_email_now(recipients=args.to or None, record_sent=True)
        print(json.dumps(result, indent=2, default=str))
        return 0 if result.get("status") == "sent" else 1

    if args.command == "review-server":
        from pipelines.listing.review_actions import start_review_server

        start_review_server(host=args.host, port=args.port)
        return 0

    if args.command in {"review-up", "review-down"}:
        return record_review_from_cli(args.item, args.review_action)

    if args.command == "favourites":
        return run_favourites(limit=args.limit, as_json=bool(args.json))

    if args.command == "reviews":
        from pipelines.listing.review_actions import latest_review_actions

        rows = latest_review_actions(limit=args.limit)
        if args.json:
            print(json.dumps(rows, indent=2, default=str))
        else:
            print("Latest review actions")
            _print_review_rows(rows)
        return 0

    if args.command == "slack-review-once":
        from pipelines.listing.slack_review_commands import process_slack_review_commands_once

        result = process_slack_review_commands_once(limit=args.limit, reply=not args.no_reply)
        print(json.dumps(result.as_dict(), indent=2, default=str))
        return 0 if result.status in {"ok", "skipped"} else 1

    if args.command == "slack-review-loop":
        from pipelines.listing.slack_review_commands import run_slack_review_command_loop

        run_slack_review_command_loop(
            poll_seconds=args.poll_seconds,
            limit=args.limit,
            reply=not args.no_reply,
        )
        return 0

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
