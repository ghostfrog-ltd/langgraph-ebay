"""
Build a review-only auction digest for operator notifications.

The digest turns the current deterministic ROI shortlist into concise Slack
content. It is deliberately observer-only: it reads local PostgreSQL through
the existing ROI helpers, applies the same guardrails, and never places bids,
buys items, messages sellers, or writes to marketplace systems.
"""

from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

from psycopg2.extras import RealDictCursor

from pipelines.listing.paper_trading.graph import snapshot_from_opportunity
from pipelines.listing.roi.graph import (
    _build_all_opps_for_roi,
    _comps_lookup,
)
import utils.db_schema as schema


MAX_TITLE_CHARS = 82
MAX_PER_SOURCE = 2
FRESHNESS_MINUTES = int(os.getenv("GF_SLACK_AUCTION_FRESHNESS_MINUTES", "60"))
SCOUT_SOURCES = tuple(
    source.strip()
    for source in os.getenv(
        "GF_SLACK_SCOUT_SOURCES",
        "ebay-watches,motomine,ebay-lego,ebay-samsung,ebay-headphones,ebay-actioncams,ebay-tools,ebay-pokemon",
    ).split(",")
    if source.strip()
)
SCOUT_LIMIT = int(os.getenv("GF_SLACK_SCOUT_LIMIT", "4"))
MAX_FRESH_LISTINGS = int(os.getenv("GF_SLACK_MAX_FRESH_LISTINGS", "500"))
MIN_MAIN_PROFIT_GBP = float(os.getenv("GF_SLACK_MIN_MAIN_PROFIT_GBP", "0"))
LOW_INTEREST_COMPUTER_TERMS = (
    "pentium",
    "celeron",
    "core 2 duo",
    "core2duo",
    "dual core",
)
HIGH_RISK_CONDITION_TERMS = (
    "faulty",
    "spares",
    "spares or repair",
    "for parts",
    "not working",
    "untested",
)
LEGACY_APPLE_LAPTOP_TERMS = (
    "a1181",
    "a1278",
    "a1286",
    "a1297",
    "a1342",
    "a1369",
    "a1370",
    "a1398",
    "a1425",
    "a1465",
    "a1466",
    "a1502",
    "a1534",
)
LOW_INTEREST_APPLE_DEVICE_TERMS = (
    "ipad 1st generation",
    "ipad 2 ",
    "ipad 2nd generation",
    "ipad 3rd generation",
    "ipad 4th generation",
)


@dataclass(frozen=True)
class ReviewAuctionDigestItem:
    """One review-only auction surfaced to the operator."""

    source: str
    title: str
    url: str
    model_key: str | None
    current_price: float
    resale_estimate: float
    estimated_profit: float
    roi_percent: float
    review_ceiling: float
    comp_samples: int
    ends: str
    risk_notes: list[str]


@dataclass(frozen=True)
class ScoutDigestItem:
    """One fresh source row shown for manual scouting rather than ROI evidence."""

    source: str
    title: str
    url: str
    current_price: float
    model_key: str | None
    comp_samples: int | None
    reason: str
    ends: str


def _field(row: Any, name: str, default: Any = None) -> Any:
    """Read a field from either a dict-like row or an object/dataclass."""

    if isinstance(row, dict):
        return row.get(name, default)
    return getattr(row, name, default)


def _money(value: Any) -> str:
    """Format a GBP amount without depending on locale settings."""

    try:
        return f"GBP {float(value):,.0f}"
    except (TypeError, ValueError):
        return "GBP n/a"


def _time_left(end_time: datetime | None, now: datetime) -> str:
    """Return a compact human-readable time-to-close string."""

    if end_time is None:
        return "end time unknown"

    aware_end = end_time if end_time.tzinfo else end_time.replace(tzinfo=timezone.utc)
    delta = aware_end.astimezone(timezone.utc) - now.astimezone(timezone.utc)
    minutes = int(delta.total_seconds() // 60)
    if minutes <= 0:
        return "ended or due for refresh"
    if minutes < 60:
        return f"{minutes}m"
    hours = minutes // 60
    remainder = minutes % 60
    if hours < 48:
        return f"{hours}h {remainder}m" if remainder else f"{hours}h"
    days = hours // 24
    return f"{days}d {hours % 24}h"


def _to_aware_utc(value: Any) -> datetime | None:
    """Normalize optional database datetimes for freshness checks."""

    if value is None:
        return None
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _is_fresh_live_listing_for_slack(listing: dict[str, Any], now: datetime) -> bool:
    """
    Require recent live evidence before Slack surfaces an auction.

    eBay listings can end early while local rows still say `live`. The digest is
    an interruption channel, so it should only show rows that were seen alive in
    the recent retrieval window and whose local end time has not passed.
    """

    status = str(listing.get("status") or "").strip().lower()
    if status not in {"active", "live", "open", "ending_soon"}:
        return False

    end_time = _to_aware_utc(listing.get("end_time"))
    if end_time is None or end_time <= now:
        return False

    last_seen_at = _to_aware_utc(listing.get("last_seen_at"))
    if last_seen_at is None:
        return False

    return last_seen_at >= now - timedelta(minutes=FRESHNESS_MINUTES)


def _risk_notes(*, title: str, model_key: str | None, comp_samples: int, ends: str) -> list[str]:
    """
    Explain why the operator still needs to inspect the listing manually.

    These notes keep the Slack digest from looking like an instruction to act.
    They call out thin evidence, broad identity keys, and common condition
    hazards that the deterministic shortlist does not fully resolve.
    """

    notes = ["review photos/condition"]
    lower_title = title.lower()
    lower_key = (model_key or "").lower()

    if comp_samples < 5:
        notes.append("thin comp sample")
    if "gen-unk" in lower_key or lower_key.endswith("_b"):
        notes.append("confirm exact model/grade")
    if any(token in lower_title for token in ("dent", "crack", "fault", "repair", "spares")):
        notes.append("condition risk")
    if "ended" in ends or "unknown" in ends:
        notes.append("refresh timing")

    return notes[:4]


def _is_low_interest_for_slack(opportunity: Any) -> bool:
    """
    Suppress rows that match the operator's stated low-interest taste profile.

    The ROI and paper-trading loops can still track these rows for learning, but
    Slack should only interrupt the operator with listings that are plausible to
    inspect. Old Intel-era MacBooks, Pentium/Celeron/Core-era laptops, and
    clearly ancient Apple tablets are intentionally kept out of the digest.
    """

    title = str(_field(opportunity, "title", "") or "").lower()
    model_key = str(_field(opportunity, "model_key", "") or "").lower()
    blob = f"{title} {model_key}"

    if any(term in blob for term in LOW_INTEREST_COMPUTER_TERMS):
        return True

    if any(term in blob for term in HIGH_RISK_CONDITION_TERMS):
        return True

    is_mac_laptop = "macbook" in blob or "apple-macbook" in model_key
    if is_mac_laptop:
        if "-intel-" in model_key or " intel " in f" {title} ":
            return True
        if any(term in blob for term in LEGACY_APPLE_LAPTOP_TERMS):
            return True
        # MacBooks before Apple Silicon are not useful Slack interruptions for
        # this operator unless future preference rules explicitly allow them.
        if not any(chip in blob for chip in (" m1", " m2", " m3", " m4", "-m1-", "-m2-", "-m3-", "-m4-")):
            return True

    if any(term in blob for term in LOW_INTEREST_APPLE_DEVICE_TERMS):
        return True

    return False


def _item_from_opportunity(opportunity: Any, now: datetime) -> ReviewAuctionDigestItem:
    """Convert an ROI opportunity into digest fields using paper-trade costs."""

    paper = snapshot_from_opportunity(opportunity)
    ends = _time_left(opportunity.end_time, now)
    return ReviewAuctionDigestItem(
        source=str(opportunity.source or ""),
        title=str(opportunity.title or ""),
        url=str(opportunity.url or ""),
        model_key=opportunity.model_key,
        current_price=float(paper.current_price),
        resale_estimate=float(paper.predicted_resale_price),
        estimated_profit=float(paper.predicted_profit),
        roi_percent=float(paper.predicted_roi) * 100.0,
        review_ceiling=float(paper.recommended_max_bid),
        comp_samples=int(paper.comp_samples),
        ends=ends,
        risk_notes=_risk_notes(
            title=str(opportunity.title or ""),
            model_key=opportunity.model_key,
            comp_samples=int(paper.comp_samples),
            ends=ends,
        ),
    )


def _rank_key(opportunity: Any) -> tuple[float, float]:
    """Rank by estimated profit first, then ROI."""

    return (
        float(_field(opportunity, "profit", 0.0) or 0.0),
        float(_field(opportunity, "roi", 0.0) or 0.0),
    )


def _select_diverse_opportunities(opportunities: list[Any], limit: int) -> list[Any]:
    """
    Pick a mixed shortlist instead of letting one source dominate Slack.

    The underlying ROI/paper-trading loops can remain stricter. Slack is an
    operator attention surface, so it is more useful when it includes the best
    guarded rows from several sources, even if some are exploratory rather than
    strict profit-threshold rows.
    """

    selected: list[Any] = []
    selected_ids: set[str] = set()
    per_source: dict[str, int] = {}
    ranked = sorted(opportunities, key=_rank_key, reverse=True)

    for opportunity in ranked:
        if len(selected) >= limit:
            break

        source = str(_field(opportunity, "source", "") or "unknown")
        if per_source.get(source, 0) >= MAX_PER_SOURCE:
            continue

        external_id = str(_field(opportunity, "external_id", "") or id(opportunity))
        selected.append(opportunity)
        selected_ids.add(external_id)
        per_source[source] = per_source.get(source, 0) + 1

    if len(selected) >= limit:
        return selected

    for opportunity in ranked:
        if len(selected) >= limit:
            break
        external_id = str(_field(opportunity, "external_id", "") or id(opportunity))
        if external_id in selected_ids:
            continue
        selected.append(opportunity)
        selected_ids.add(external_id)

    return selected


def _scout_reason(listing: dict[str, Any], comps_by_model: dict[str, Any]) -> tuple[str, int | None]:
    """Explain why a fresh source row is not being shown as a profit row."""

    model_key = str(listing.get("model_key") or "").strip()
    if not model_key or model_key.lower() == "unknown":
        return "needs model key", None

    comp = comps_by_model.get(model_key)
    if not comp:
        return "needs sold comps", None

    samples = int(comp.get("samples") or 0)
    if samples < 3:
        return f"thin comps ({samples})", samples

    try:
        current = float(listing.get("price_current") or 0.0)
        median = float(comp.get("median_final_price") or 0.0)
    except (TypeError, ValueError):
        return f"has comps ({samples})", samples

    if median > 0 and current >= median:
        return f"above comps ({samples})", samples
    return f"manual scout ({samples} comps)", samples


def _build_scout_items(
    *,
    listings: list[dict[str, Any]],
    comps_by_model: dict[str, Any],
    selected_opportunities: list[Any],
    now: datetime,
    limit: int,
) -> list[ScoutDigestItem]:
    """
    Add fresh non-ROI source rows so Slack shows what the wider system is seeing.

    These rows are intentionally separated from the ROI shortlist. They are for
    human taste discovery and data-quality steering, not profit claims.
    """

    if limit <= 0:
        return []

    selected_ids = {
        str(_field(opportunity, "external_id", "") or "")
        for opportunity in selected_opportunities
    }
    scouts: list[ScoutDigestItem] = []
    seen_sources: set[str] = set()

    candidates = sorted(
        [
            listing
            for listing in listings
            if str(listing.get("source") or "") in SCOUT_SOURCES
            and str(listing.get("external_id") or "") not in selected_ids
            and not _is_low_interest_for_slack(listing)
        ],
        key=lambda row: float(row.get("price_current") or 0.0),
        reverse=True,
    )

    for listing in candidates:
        source = str(listing.get("source") or "")
        if source in seen_sources:
            continue
        reason, comp_samples = _scout_reason(listing, comps_by_model)
        scouts.append(
            ScoutDigestItem(
                source=source,
                title=str(listing.get("title") or ""),
                url=str(listing.get("url") or ""),
                current_price=float(listing.get("price_current") or 0.0),
                model_key=listing.get("model_key"),
                comp_samples=comp_samples,
                reason=reason,
                ends=_time_left(_to_aware_utc(listing.get("end_time")), now),
            )
        )
        seen_sources.add(source)
        if len(scouts) >= limit:
            break

    return scouts


def _fetch_active_listings(now: datetime | None = None, *, limit: int = MAX_FRESH_LISTINGS) -> list[dict[str, Any]]:
    """
    Load only fresh live digest candidates.

    The heartbeat runs every five minutes, so do not pull the full live table
    and filter it in Python. Push the freshness/end-time gate into PostgreSQL
    and keep the result capped.
    """

    now = now or datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=FRESHNESS_MINUTES)
    capped_limit = max(1, min(int(limit), 2000))
    query = """
        SELECT
            source,
            external_id,
            title,
            url,
            model_key,
            COALESCE(condition_grade, 'B') AS condition_grade,
            COALESCE(price_current, 0) AS price_current,
            status,
            end_time,
            time_left_s,
            last_seen_at
        FROM auction_listings
        WHERE status = 'live'
          AND price_current IS NOT NULL
          AND end_time IS NOT NULL
          AND end_time > %s
          AND last_seen_at IS NOT NULL
          AND last_seen_at >= %s
        ORDER BY last_seen_at DESC, end_time ASC, price_current DESC
        LIMIT %s
    """
    with schema.connection.cursor(cursor_factory=RealDictCursor) as cur:
        schema.ensure_utc_session(cur)
        cur.execute(query, (now, cutoff, capped_limit))
        return [dict(row) for row in cur.fetchall()]


def build_review_auction_digest(limit: int = 4, now: datetime | None = None) -> dict[str, Any]:
    """
    Build the current review-only auction digest from local data.

    Side effects:
    - Reads normalized listing views and `latest_comps`.
    - Does not write to PostgreSQL or any external marketplace.
    """

    now = now or datetime.now(timezone.utc)
    capped_limit = max(0, min(int(limit), 10))
    if capped_limit <= 0:
        return {"status": "ok", "count": 0, "items": []}

    listings = [
        listing
        for listing in _fetch_active_listings(now)
        if _is_fresh_live_listing_for_slack(listing, now)
    ]
    comps_by_model = _comps_lookup()
    opportunities = _build_all_opps_for_roi(listings, comps_by_model)
    filtered_opportunities = [
        opportunity
        for opportunity in opportunities
        if not _is_low_interest_for_slack(opportunity)
        and float(_field(opportunity, "profit", 0.0) or 0.0) > MIN_MAIN_PROFIT_GBP
    ]
    selected_opportunities = _select_diverse_opportunities(
        filtered_opportunities,
        capped_limit * 3,
    )
    items: list[ReviewAuctionDigestItem] = []
    displayed_opportunities: list[Any] = []
    for opportunity in selected_opportunities:
        item = _item_from_opportunity(opportunity, now)
        if item.estimated_profit <= 0:
            continue
        items.append(item)
        displayed_opportunities.append(opportunity)
        if len(items) >= capped_limit:
            break

    scout_items = _build_scout_items(
        listings=listings,
        comps_by_model=comps_by_model,
        selected_opportunities=displayed_opportunities,
        now=now,
        limit=SCOUT_LIMIT,
    )
    return {
        "status": "ok",
        "count": len(items),
        "filtered_low_interest": len(opportunities) - len(filtered_opportunities),
        "eligible_sources": sorted(
            {
                str(_field(opportunity, "source", "") or "unknown")
                for opportunity in filtered_opportunities
            }
        ),
        "items": [asdict(item) for item in items],
        "scout_items": [asdict(item) for item in scout_items],
    }


def _slack_escape(value: Any) -> str:
    """Escape Slack mrkdwn control characters in user/data text."""

    return str(value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _short_title(title: str) -> str:
    """Trim long marketplace titles so Slack blocks stay readable."""

    cleaned = " ".join(str(title or "").split())
    if len(cleaned) <= MAX_TITLE_CHARS:
        return cleaned
    return f"{cleaned[: MAX_TITLE_CHARS - 3].rstrip()}..."


def _slack_link(title: str, url: str) -> str:
    """Render a safe Slack link, falling back to plain text when URL is absent."""

    label = _slack_escape(_short_title(title))
    clean_url = str(url or "").strip()
    if not clean_url:
        return label
    return f"<{clean_url}|{label}>"


def format_review_auction_digest_for_slack(digest: dict[str, Any]) -> str:
    """
    Format digest rows as Slack mrkdwn.

    The copy avoids buy/bid/action labels. The operator gets enough data to
    inspect promising auctions while the system remains safely advisory.
    """

    if digest.get("error"):
        return "Review-only auction shortlist unavailable; check local logs before relying on auction output."

    items = digest.get("items") or []
    scouts = digest.get("scout_items") or []
    if not items and not scouts:
        return "No review-only auction shortlist rows right now. Keep collecting and improving model keys/comps."

    lines = ["*Review-only auction shortlist*"]
    item_blocks: list[str] = []
    for index, item in enumerate(items[:6], start=1):
        notes = ", ".join(item.get("risk_notes") or ["manual review required"])
        item_blocks.append(
            "\n".join(
                [
                    f"{index}. {_slack_link(item.get('title', ''), item.get('url', ''))}",
                    (
                        f"   Current {_money(item.get('current_price'))} | "
                        f"resale {_money(item.get('resale_estimate'))} | "
                        f"est profit {_money(item.get('estimated_profit'))} | "
                        f"ROI {float(item.get('roi_percent') or 0):.0f}%"
                    ),
                    (
                        f"   Review cap {_money(item.get('review_ceiling'))} | "
                        f"comps {int(item.get('comp_samples') or 0)} | "
                        f"ends {_slack_escape(item.get('ends'))} | "
                        f"notes: {_slack_escape(notes)}"
                    ),
                ]
            )
        )

    if item_blocks:
        lines.append("\n\n".join(item_blocks))
    else:
        lines.append("No profit-evidence rows right now.")

    if scouts:
        scout_lines = ["*Source scouts - not profit-scored yet*"]
        for scout in scouts[:6]:
            samples = scout.get("comp_samples")
            comp_text = "no comps" if samples is None else f"{int(samples)} comps"
            scout_lines.append(
                (
                    f"- `{_slack_escape(scout.get('source'))}` "
                    f"{_slack_link(scout.get('title', ''), scout.get('url', ''))}\n"
                    f"   Current {_money(scout.get('current_price'))} | {comp_text} | "
                    f"ends {_slack_escape(scout.get('ends'))} | reason: {_slack_escape(scout.get('reason'))}"
                )
            )
        lines.append("")
        lines.append("\n\n".join(scout_lines))

    lines.append("")
    lines.append("_Observer only: inspect manually; no marketplace action is triggered from Slack._")
    return "\n".join(lines)
