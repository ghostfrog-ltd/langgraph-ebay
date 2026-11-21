"""Maintain a local, redacted snapshot of OpenAI Platform organization costs."""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import fcntl
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
from tempfile import NamedTemporaryFile
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener
from xml.etree import ElementTree


COSTS_URL = "https://api.openai.com/v1/organization/costs"
FX_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
DAY = 86400
MAX_AGE_SECONDS = 75 * 60
GBP_THRESHOLD = Decimal("5")
PROVENANCE = "OpenAI Platform Costs API; organization-wide recorded API costs"


class Unavailable(ValueError):
    """The current cost reading could not be verified."""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def _request(url: str, headers: dict[str, str] | None = None) -> bytes:
    # Never forward an organization credential to a redirect target.
    with build_opener(NoRedirect()).open(Request(url, headers=headers or {}), timeout=10) as response:
        payload = response.read(2_000_001)
    if len(payload) > 2_000_000:
        raise Unavailable("Response exceeded the size limit")
    return payload


def _decimal(value) -> Decimal:
    if isinstance(value, bool):
        raise Unavailable("Non-numeric value")
    number = Decimal(value)
    if not number.is_finite():
        raise Unavailable("Non-finite value")
    return number


def _costs(key: str, organization: str, start: int, end: int) -> tuple[str, Decimal, int]:
    params = {"start_time": start, "end_time": end, "bucket_width": "1d", "limit": 180}
    headers = {"Authorization": f"Bearer {key}", "OpenAI-Organization": organization}
    currency = None
    total = Decimal(0)
    bucket_starts = set()
    cursors = set()
    final_day = (end - 1) // DAY * DAY
    closed_days = set(range(start, end // DAY * DAY, DAY))
    for _ in range(12):
        page = json.loads(_request(f"{COSTS_URL}?{urlencode(params)}", headers), parse_float=Decimal)
        if page["object"] != "page" or not isinstance(page["data"], list):
            raise Unavailable("Unexpected Costs API response")
        for bucket in page["data"]:
            bucket_start = bucket["start_time"]
            if (
                bucket["object"] != "bucket" or type(bucket_start) is not int
                or not start <= bucket_start <= final_day or bucket_start % DAY
                or bucket_start in bucket_starts
                or bucket["end_time"] not in (min(bucket_start + DAY, end), bucket_start + DAY)
                or not isinstance(bucket["results"], list)
            ):
                raise Unavailable("Incomplete or invalid daily cost buckets")
            bucket_starts.add(bucket_start)
            for item in bucket["results"]:
                if item["object"] != "organization.costs.result":
                    raise Unavailable("Invalid cost entry")
                amount = item["amount"]
                if type(amount["value"]) not in (int, Decimal):
                    raise Unavailable("Invalid cost amount")
                unit = amount["currency"]
                if not isinstance(unit, str):
                    raise Unavailable("Missing billing currency")
                unit = unit.upper()
                if unit not in {"USD", "GBP"} or currency not in (None, unit):
                    raise Unavailable("Unsupported or mixed billing currencies")
                currency = unit
                total += _decimal(amount["value"])
        if page["has_more"] is False:
            if not bucket_starts or not closed_days.issubset(bucket_starts):
                raise Unavailable("Costs API did not return all requested closed days")
            # OpenAI can omit the current UTC day. Only the returned days contribute.
            coverage_end = min(max(bucket_starts) + DAY, end)
            return currency or "USD", total, coverage_end
        cursor = page["next_page"]
        if page["has_more"] is not True or not isinstance(cursor, str) or not cursor or cursor in cursors:
            raise Unavailable("Incomplete Costs API pagination")
        cursors.add(cursor)
        params["page"] = cursor
    raise Unavailable("Costs API exceeded the page limit")


def _exchange_rate(currency: str, today: date) -> tuple[Decimal, str | None]:
    if currency == "GBP":
        return Decimal(1), None
    root = ElementTree.fromstring(_request(FX_URL))
    daily = next(cube for cube in root.iter() if "time" in cube.attrib)
    quote_date = date.fromisoformat(daily.attrib["time"])
    rates = {cube.attrib["currency"]: _decimal(cube.attrib["rate"]) for cube in daily}
    if not 0 <= (today - quote_date).days <= 7 or min(rates["USD"], rates["GBP"]) <= 0:
        raise Unavailable("ECB reference rate is unavailable or stale")
    return rates["GBP"] / rates["USD"], quote_date.isoformat()


def _iso(timestamp: int) -> str:
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat(timespec="seconds")


def _update(previous: dict, organization: str, now: int, total: Decimal, currency: str,
            rate: Decimal, rate_date: str | None, *, start: int, coverage_end: int) -> dict:
    if previous and (previous["organization"] != organization or previous["currency"] != currency):
        raise Unavailable("Billing organization or currency changed")
    if previous and coverage_end < previous["coverage_end"]:
        raise Unavailable("Costs API coverage regressed")
    baseline = _decimal(previous["baseline"]) if previous else total
    first_charge = previous.get("first_charge_at")
    if first_charge is None and total > baseline:
        first_charge = now
    return {
        "organization": organization,
        "currency": currency,
        "start_time": start,
        "coverage_end": coverage_end,
        "baseline": str(baseline),
        "baseline_at": previous.get("baseline_at", now),
        "total": str(total),
        "last_verified": now,
        "rate": str(rate),
        "rate_date": rate_date,
        "first_charge_at": first_charge,
        "acknowledged_total": previous.get("acknowledged_total", str(baseline)),
        "failed": False,
    }


def _summary(state: dict, now: int) -> dict:
    if state.get("failed") or not 0 <= now - state["last_verified"] <= MAX_AGE_SECONDS:
        raise Unavailable("No current verified cost reading")
    total = _decimal(state["total"])
    baseline = _decimal(state["baseline"])
    rate = _decimal(state["rate"])
    increase = total - baseline
    gbp = increase * rate
    acknowledged = _decimal(state["acknowledged_total"])
    first = state["first_charge_at"]
    alert_id = None
    if gbp > GBP_THRESHOLD and total > acknowledged:
        digest = sha256(str(total.normalize()).encode()).hexdigest()[:12]
        alert_id = f'{state["start_time"]}-{digest}'
    return {
        "status": "verified",
        "provenance": PROVENANCE,
        "currency": state["currency"],
        "total": str(total),
        "baseline_total": str(baseline),
        "baseline_at_utc": _iso(state["baseline_at"]),
        "period_start_utc": _iso(state["start_time"]),
        "period_end_utc": _iso(state["coverage_end"]),
        "current_utc_day_included": state["coverage_end"] > now // DAY * DAY,
        "checked_at_utc": _iso(state["last_verified"]),
        "increase": str(increase),
        "increase_gbp_estimate": str(gbp.quantize(Decimal("0.0001"))),
        "fx_rate_date": state["rate_date"],
        "fx_provenance": FX_URL if state["rate_date"] else None,
        "first_charge_event": f"first-{first}" if first else None,
        "repeat_alert_id": alert_id,
    }


def spend_directory() -> Path:
    return Path.home() / ".config" / "ghostfrog-spend"


def _private(path: Path, *, directory: bool = False) -> None:
    info = path.lstat()
    check_type = stat.S_ISDIR if directory else stat.S_ISREG
    if not check_type(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
        raise Unavailable("Spend settings must be private and user-owned")


def _load(root: Path) -> dict:
    file = root / "state.json"
    _private(file)
    state = json.loads(file.read_text())
    if not isinstance(state, dict):
        raise Unavailable("Invalid spend state")
    return state


def _write(root: Path, state: dict) -> None:
    with NamedTemporaryFile(mode="w", dir=root, delete=False) as file:
        json.dump(state, file)
        temp = Path(file.name)
    try:
        temp.replace(root / "state.json")
    finally:
        temp.unlink(missing_ok=True)


@contextmanager
def _lock(root: Path):
    _private(root, directory=True)
    with open(root / ".lock", "a", opener=lambda path, flags: os.open(path, flags | os.O_NOFOLLOW, 0o600)) as file:
        fcntl.flock(file, fcntl.LOCK_EX)
        yield


READ_ERRORS = (
    OSError, ValueError, KeyError, TypeError, AttributeError, OverflowError,
    InvalidOperation, ElementTree.ParseError, StopIteration,
)


def current_summary(root: Path | None = None, *, now: int | None = None) -> dict:
    root = root if root is not None else spend_directory()
    try:
        _private(root, directory=True)
        return _summary(_load(root), int(datetime.now(timezone.utc).timestamp()) if now is None else now)
    except READ_ERRORS:
        return {"status": "monitoring unavailable", "provenance": PROVENANCE}


def refresh(root: Path, now: int) -> None:
    with _lock(root):
        try:
            state = _load(root)
        except FileNotFoundError:
            state = {}
        try:
            key_path = root / "admin-key"
            organization_path = root / "organization-id"
            _private(key_path)
            _private(organization_path)
            key = key_path.read_text().strip()
            organization = organization_path.read_text().strip()
            if not re.fullmatch(r"org-[A-Za-z0-9]+", organization) or not re.fullmatch(r"sk-[A-Za-z0-9_-]+", key):
                raise Unavailable("Invalid local billing configuration")
            start = state.get("start_time", now // DAY * DAY - DAY)
            currency, cost, coverage_end = _costs(key, organization, start, now)
            quote, quote_date = _exchange_rate(currency, datetime.fromtimestamp(now, timezone.utc).date())
            new_state = _update(state, organization, now, cost, currency, quote, quote_date,
                                start=start, coverage_end=coverage_end)
        except READ_ERRORS as exc:
            if state:
                _write(root, dict(state, failed=True))
            code = getattr(exc, "code", None)
            message = f"Costs or FX endpoint returned HTTP {code}" if code in {400, 401, 403, 429, 500, 503} else "Unable to verify costs and FX"
            raise Unavailable(message) from None
        _write(root, new_state)


def acknowledge(root: Path, alert_id: str, now: int) -> None:
    with _lock(root):
        previous = _load(root)
        summary = _summary(previous, now)
        if not alert_id or alert_id != summary["repeat_alert_id"]:
            raise Unavailable("Alert is stale or already acknowledged")
        _write(root, dict(previous, acknowledged_total=previous["total"]))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("refresh", help="Verify current costs and write a private summary")
    actions.add_parser("status", help="Print the current summary in your terminal")
    ack = actions.add_parser("acknowledge", help="Acknowledge the current repeat alert")
    ack.add_argument("alert_id")
    args = parser.parse_args()
    now = int(datetime.now(timezone.utc).timestamp())
    root = spend_directory()
    try:
        if args.action == "refresh":
            refresh(root, now)
            print("Verified organization costs and updated the local snapshot.")
        elif args.action == "status":
            print(json.dumps(current_summary(root, now=now), indent=2))
        else:
            acknowledge(root, args.alert_id, now)
            print("Current repeat alert acknowledged.")
    except READ_ERRORS as exc:
        message = str(exc) if isinstance(exc, Unavailable) else "Unable to read private spend state"
        parser.exit(1, f"Monitoring unavailable. {message}\n")


if __name__ == "__main__":
    main()
