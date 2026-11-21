from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from psycopg2.extras import RealDictCursor

from pipelines.listing.comps.graph import (
    APPLE_MAC_ALLOWED_CPU_PATTERN,
    COMPS_BLOCKED_TEXT_PATTERNS,
    _get_window_days,
)
from pipelines.listing.market_discovery.graph import potential_from_row
from pipelines.listing.review_actions import normalise_external_id
from utils.db_schema import ensure_utc_session, get_fresh_connection


DEFAULT_COMP_LIMIT = 25


@dataclass(frozen=True)
class ListingExplanation:
    target: dict[str, Any]
    potential: dict[str, Any] | None
    comps: list[dict[str, Any]]
    comp_count: int
    comp_window_days: int
    error: str | None = None


def _money(value: Any) -> str:
    try:
        return f"GBP {float(value):,.2f}"
    except (TypeError, ValueError):
        return "GBP n/a"


def _pct(value: Any) -> str:
    try:
        return f"{float(value) * 100:.0f}%"
    except (TypeError, ValueError):
        return "0%"


def _short(value: Any, *, limit: int = 108) -> str:
    cleaned = " ".join(str(value or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[: limit - 3].rstrip()}..."


def _fetch_target(cur: Any, external_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT
            a.source,
            a.external_id,
            a.title,
            a.url,
            a.status,
            a.model_key,
            grade.effective_condition_grade AS condition_grade,
            a.sale_type,
            a.end_time,
            a.last_seen_at,
            COALESCE(a.price_current, 0) AS price_current,
            lc.samples,
            lc.median_final_price,
            lc.mean_final_price,
            lc.computed_at
        FROM auction_listings AS a
        LEFT JOIN latest_listing_visual_conditions AS vc
          ON vc.listing_id = a.id
        LEFT JOIN latest_listing_text_conditions AS tc
          ON tc.listing_id = a.id
        CROSS JOIN LATERAL (
            SELECT CASE
                WHEN 'D' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'D'
                WHEN 'C' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'C'
                WHEN 'B' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'B'
                ELSE 'A'
            END AS effective_condition_grade
        ) AS grade
        LEFT JOIN latest_comps AS lc
          ON lc.model_key = a.model_key
         AND lc.condition_grade = grade.effective_condition_grade
        WHERE a.external_id = %s
        LIMIT 1
        """,
        (external_id,),
    )
    row = cur.fetchone()
    return dict(row) if row else None


def _fetch_comp_rows(
    cur: Any,
    *,
    model_key: str,
    condition_grade: str,
    days: int,
) -> list[dict[str, Any]]:
    cur.execute(
        """
        SELECT
            source,
            external_id,
            title,
            url,
            status,
            sale_type,
            end_time,
            price_current,
            final_price,
            bids_count,
            condition_grade,
            model_key
        FROM auction_listings
        WHERE final_price IS NOT NULL
          AND final_price > 0
          AND end_time >= (now() AT TIME ZONE 'utc' - (%s || ' days')::interval)
          AND model_key = %s
          AND COALESCE(condition_grade, 'B') = %s
          AND LOWER(COALESCE(title, '') || ' ' || COALESCE(raw_attrs::text, '')) NOT LIKE ALL(%s)
          AND NOT (
              source = 'ebay-apple'
              AND LOWER(COALESCE(title, '') || ' ' || COALESCE(raw_attrs::text, '')) LIKE '%%macbook%%'
              AND LOWER(COALESCE(title, '') || ' ' || COALESCE(raw_attrs::text, '')) ~ '\\ma[0-9]{1,2}\\s*(pro|max|ultra)?\\M'
          )
          AND NOT (
              source = 'ebay-apple'
              AND LOWER(COALESCE(title, '') || ' ' || COALESCE(raw_attrs::text, '')) ~ '\\mlocked\\M'
          )
          AND NOT (
              LOWER(model_key) LIKE 'apple-iphone-%%'
              AND LOWER(model_key) !~ '^apple-iphone-(se|x|xr|xs|[0-9]+)(-|_)'
          )
          AND NOT (
              LOWER(model_key) ~ '^apple-(macbook|imac|mac-mini|mac-studio)'
              AND LOWER(model_key) !~ %s
          )
        ORDER BY final_price ASC, end_time DESC NULLS LAST, external_id
        """,
        (
            str(days),
            model_key,
            condition_grade,
            list(COMPS_BLOCKED_TEXT_PATTERNS),
            APPLE_MAC_ALLOWED_CPU_PATTERN,
        ),
    )
    return [dict(row) for row in cur.fetchall()]


def build_listing_explanation(identifier: str) -> ListingExplanation:
    days = _get_window_days()
    try:
        external_id = normalise_external_id(identifier)
    except ValueError as exc:
        return ListingExplanation({}, None, [], 0, days, error=str(exc))

    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_utc_session(cur)
            target = _fetch_target(cur, external_id)
            if not target:
                return ListingExplanation(
                    {"external_id": external_id},
                    None,
                    [],
                    0,
                    days,
                    error="listing not found in local auction_listings",
                )

            if not target.get("model_key") or str(target.get("model_key")).lower() == "unknown":
                return ListingExplanation(
                    target,
                    None,
                    [],
                    0,
                    days,
                    error="listing has no clean model_key, so no same-product comps can explain it",
                )

            if not target.get("median_final_price") or not target.get("samples"):
                return ListingExplanation(
                    target,
                    None,
                    [],
                    0,
                    days,
                    error="listing has no latest same-product/condition comp bucket",
                )

            potential = potential_from_row(target)
            comps = _fetch_comp_rows(
                cur,
                model_key=str(target.get("model_key")),
                condition_grade=str(target.get("condition_grade") or "B"),
                days=days,
            )
            median = float(target.get("median_final_price") or 0)
            comps.sort(
                key=lambda row: (
                    abs(float(row.get("final_price") or 0) - median),
                    float(row.get("final_price") or 0),
                )
            )
            return ListingExplanation(
                target,
                potential.__dict__,
                comps,
                int(target.get("samples") or len(comps)),
                days,
            )
    finally:
        conn.close()


def format_listing_explanation(
    explanation: ListingExplanation,
    *,
    limit: int = DEFAULT_COMP_LIMIT,
    show_all: bool = False,
) -> str:
    target = explanation.target
    lines = ["Listing profit explanation"]
    if explanation.error:
        if target:
            lines.append(f"Listing: {target.get('external_id')}")
            if target.get("title"):
                lines.append(f"Title: {_short(target.get('title'))}")
            if target.get("model_key"):
                lines.append(
                    f"Bucket: {target.get('model_key')} | grade {target.get('condition_grade') or 'B'}"
                )
        lines.append(f"Cannot explain: {explanation.error}")
        return "\n".join(lines)

    potential = explanation.potential or {}
    lines.extend(
        [
            f"Listing: {_short(target.get('title'))}",
            f"  id: {target.get('external_id')} | source: {target.get('source')} | status: {target.get('status')}",
            f"  bucket: {target.get('model_key')} | grade {target.get('condition_grade') or 'B'}",
            (
                f"  current {_money(potential.get('current_price'))} | "
                f"estimated resale median {_money(potential.get('sold_median'))} | "
                f"comps {explanation.comp_count} | window {explanation.comp_window_days}d"
            ),
            (
                f"  max safe buy {_money(potential.get('max_safe_buy'))} | "
                f"expected profit {_money(potential.get('expected_profit'))} | "
                f"ROI {_pct(potential.get('expected_roi'))}"
            ),
        ]
    )
    if target.get("url"):
        lines.append(f"  {target.get('url')}")

    rows = explanation.comps if show_all else explanation.comps[:limit]
    hidden = max(len(explanation.comps) - len(rows), 0)
    lines.append("")
    lines.append(
        f"Exact sold comps nearest to the estimate median ({len(rows)} shown"
        f"{f', {hidden} hidden' if hidden else ''}):"
    )
    if not rows:
        lines.append("  No matching sold rows found, even though latest_comps has a bucket.")
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        branch = "`-" if index == len(rows) else "|-"
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
        lines.append(f"  {branch} {_short(row.get('title'))}")
        lines.append(f"     {' | '.join(bits)}")
        if row.get("url"):
            lines.append(f"     {row.get('url')}")

    return "\n".join(lines)


def explain_listing(identifier: str, *, limit: int = DEFAULT_COMP_LIMIT, show_all: bool = False) -> str:
    return format_listing_explanation(
        build_listing_explanation(identifier),
        limit=limit,
        show_all=show_all,
    )
