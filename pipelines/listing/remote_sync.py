"""Publish a safe, read-only listings snapshot to the remote Laravel app."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
import hashlib
import hmac
import json
import os
import math
from pathlib import Path
from typing import Any, Callable
from uuid import uuid4

import requests
from psycopg2.extras import RealDictCursor

from pipelines.listing.comps.graph import (
    APPLE_MAC_ALLOWED_CPU_PATTERN,
    COMPS_BLOCKED_TEXT_PATTERNS,
    _get_window_days,
)
from pipelines.listing.explain import ListingExplanation, build_listing_explanation
from pipelines.listing.market_discovery.graph import run as run_potentials
from pipelines.listing.market_discovery.graph import potential_from_row
from utils.condition import has_investible_product_identity
from utils.db_schema import ensure_utc_session, get_fresh_connection


LISTING_FIELDS = (
    "source",
    "external_id",
    "title",
    "url",
    "model_key",
    "condition_grade",
    "text_condition_grade",
    "text_scan_grade",
    "text_scan_checked",
    "text_scan_review_required",
    "text_scan_risk_flags",
    "text_scan_model_name",
    "text_scan_checked_at",
    "visual_condition_grade",
    "visual_condition_checked",
    "visual_review_required",
    "visual_damage_flags",
    "visual_model_name",
    "visual_checked_at",
    "sale_type",
    "end_time",
    "current_price",
    "sold_median",
    "comp_samples",
    "max_safe_buy",
    "expected_profit",
    "expected_roi",
    "price_gap_to_max_buy",
    "effective_min_comps",
    "signal",
)
COMP_FIELDS = (
    "source",
    "external_id",
    "title",
    "url",
    "sale_type",
    "end_time",
    "final_price",
    "bids_count",
    "condition_grade",
    "model_key",
    "text_scan_grade",
    "text_scan_checked",
    "text_scan_review_required",
    "text_scan_risk_flags",
    "text_scan_model_name",
    "text_scan_checked_at",
    "visual_condition_grade",
    "visual_condition_checked",
    "visual_review_required",
    "visual_damage_flags",
    "visual_model_name",
    "visual_checked_at",
)
SUMMARY_FIELDS = (
    "live_rows_with_comps",
    "blocked_identity",
    "below_min_comps",
    "checked",
    "potentials",
    "published",
    "publish_min_profit",
)
DEFAULT_LIVE_LISTING_MAX_AGE_HOURS = 24
MODEL_KEY_COMP_FIELDS = (
    "source",
    "model_key",
    "condition_grade",
    "samples",
    "median_final_price",
    "computed_at",
)
MODEL_KEY_COMP_ROW_FIELDS = (
    "source",
    "external_id",
    "title",
    "url",
    "sale_type",
    "end_time",
    "final_price",
    "bids_count",
    "text_scan_grade",
    "text_scan_checked",
    "text_scan_review_required",
    "text_scan_risk_flags",
    "text_scan_model_name",
    "text_scan_checked_at",
    "visual_condition_grade",
    "visual_condition_checked",
    "visual_review_required",
    "visual_damage_flags",
    "visual_model_name",
    "visual_checked_at",
)
SOLD_EVIDENCE_FIELDS = (
    "source",
    "external_id",
    "title",
    "url",
    "sale_type",
    "end_time",
    "final_price",
    "bids_count",
    "condition_grade",
    "model_key",
    "text_scan_grade",
    "text_scan_checked",
    "text_scan_review_required",
    "text_scan_risk_flags",
    "text_scan_model_name",
    "text_scan_checked_at",
    "visual_condition_grade",
    "visual_condition_checked",
    "visual_review_required",
    "visual_damage_flags",
    "visual_model_name",
    "visual_checked_at",
)


def _allowlist(row: dict[str, Any], fields: tuple[str, ...]) -> dict[str, Any]:
    return {field: row.get(field) for field in fields}


def _visual_sort_value(row: dict[str, Any]) -> tuple[int, int]:
    checked = bool(row.get("visual_condition_checked", row.get("visual_checked_at") is not None))
    review = bool(row.get("visual_review_required", False))
    return (0 if checked else 1, 1 if review else 0)


def _normalise_visual_fields(row: dict[str, Any]) -> dict[str, Any]:
    row["text_scan_checked"] = bool(row.get("text_scan_checked", row.get("text_scan_checked_at") is not None))
    row["text_scan_review_required"] = bool(row.get("text_scan_review_required", False))
    row["text_scan_risk_flags"] = list(row.get("text_scan_risk_flags") or [])
    row["visual_condition_checked"] = bool(
        row.get("visual_condition_checked", row.get("visual_checked_at") is not None)
    )
    row["visual_review_required"] = bool(row.get("visual_review_required", False))
    row["visual_damage_flags"] = list(row.get("visual_damage_flags") or [])
    return row


def _source_for_model_key(model_key: str) -> str:
    key = model_key.strip().lower()
    if key.startswith("apple-"):
        return "ebay-apple"
    if key.startswith("samsung-"):
        return "ebay-samsung"
    if key.startswith(("nvidia-", "amd-radeon-")):
        return "ebay-graphics-cards"
    if key.startswith(("amd-ryzen-", "intel-core-")):
        return "ebay-cpus"
    if key.startswith("lego-"):
        return "ebay-lego"
    if key.startswith(("nintendo-", "sony-playstation", "xbox-")):
        return "ebay-consoles"
    return "ebay-apple"


def live_listing_max_age_hours() -> int:
    try:
        return max(1, int(os.getenv("GHOSTFROG_REMOTE_SYNC_LIVE_MAX_AGE_HOURS", DEFAULT_LIVE_LISTING_MAX_AGE_HOURS)))
    except ValueError:
        return DEFAULT_LIVE_LISTING_MAX_AGE_HOURS


def build_sync_payload(
    report: dict[str, Any],
    *,
    explain: Callable[[str], ListingExplanation] = build_listing_explanation,
    evidence: dict[tuple[str, str], list[dict[str, Any]]] | None = None,
    comp_limit: int = 25,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the complete remote snapshot from decision-ready checked rows."""

    listings: list[dict[str, Any]] = []
    for row in report.get("closest", []):
        if not isinstance(row, dict) or not row.get("external_id"):
            continue
        if evidence is None:
            explanation = explain(str(row["external_id"]))
            if explanation.error:
                continue
            evidence_rows = explanation.comps
        else:
            key = (str(row.get("model_key") or ""), str(row.get("condition_grade") or "B"))
            evidence_rows = evidence.get(key, [])
        comps = [
            _allowlist(_normalise_visual_fields(dict(comp)), COMP_FIELDS)
            for comp in evidence_rows[: max(comp_limit, 0)]
        ]
        listings.append({**_allowlist(row, LISTING_FIELDS), "comps": comps})

    generated = generated_at or datetime.now(UTC)
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    return {
        "schema_version": 1,
        "batch_id": str(uuid4()),
        "generated_at": generated.isoformat(),
        "summary": _allowlist(summary, SUMMARY_FIELDS),
        "listings": listings,
    }


def listing_payload_hash(listing: dict[str, Any]) -> str:
    """Stable hash for deciding whether a published listing really changed."""

    body = json.dumps(listing, default=str, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(body.encode("utf-8")).hexdigest()


def build_listing_sync_state(payload: dict[str, Any], *, endpoint: str | None = None) -> dict[str, Any]:
    listings = payload.get("listings", [])
    hashes = {
        str(listing.get("external_id")): listing_payload_hash(listing)
        for listing in listings
        if isinstance(listing, dict) and listing.get("external_id")
    }
    return {
        "schema_version": 1,
        "endpoint": endpoint,
        "generated_at": payload.get("generated_at"),
        "listings": dict(sorted(hashes.items())),
    }


def build_delta_payload(
    payload: dict[str, Any],
    *,
    previous_state: dict[str, Any] | None = None,
    include_current_ids: bool = True,
    bootstrap_without_upserts: bool = False,
    force_upserts: bool = False,
) -> dict[str, Any]:
    """Build a website delta: changed rows plus IDs Laravel should stop showing."""

    previous_hashes = {}
    if isinstance(previous_state, dict) and isinstance(previous_state.get("listings"), dict):
        previous_hashes = {
            str(external_id): str(row_hash)
            for external_id, row_hash in previous_state["listings"].items()
        }

    listings = [
        listing
        for listing in payload.get("listings", [])
        if isinstance(listing, dict) and listing.get("external_id")
    ]
    current_hashes = {
        str(listing.get("external_id")): listing_payload_hash(listing)
        for listing in listings
    }
    is_bootstrap = not previous_hashes
    if force_upserts:
        changed = list(listings)
    elif is_bootstrap and bootstrap_without_upserts:
        changed = []
    else:
        changed = [
            listing
            for listing in listings
            if previous_hashes.get(str(listing.get("external_id"))) != current_hashes[str(listing.get("external_id"))]
        ]
    expired = sorted(set(previous_hashes) - set(current_hashes))
    summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
    delta_summary = {
        **summary,
        "sync_mode": "delta",
        "delta_upserts": len(changed),
        "delta_expired": len(expired),
        "delta_current_ids": len(current_hashes),
        "delta_bootstrap": is_bootstrap,
        "delta_bootstrap_without_upserts": bool(is_bootstrap and bootstrap_without_upserts),
        "delta_force_upserts": force_upserts,
    }
    delta = {
        "schema_version": payload.get("schema_version", 1),
        "batch_id": str(uuid4()),
        "generated_at": payload.get("generated_at") or datetime.now(UTC).isoformat(),
        "mode": "delta",
        "summary": delta_summary,
        "listings": changed,
        "expired_external_ids": expired,
    }
    if include_current_ids:
        delta["current_external_ids"] = sorted(current_hashes)
    return delta


def attach_comp_evidence_to_listings(
    listings: list[dict[str, Any]],
    *,
    comp_limit: int = 25,
) -> list[dict[str, Any]]:
    if not listings:
        return []

    evidence = fetch_comp_evidence(listings)
    enriched: list[dict[str, Any]] = []
    for row in listings:
        row_copy = dict(row)
        key = (str(row_copy.get("model_key") or ""), str(row_copy.get("condition_grade") or "B"))
        median = float(row_copy.get("sold_median") or 0)
        evidence_rows = sorted(
            evidence.get(key, []),
            key=lambda comp: (
                *_visual_sort_value(comp),
                abs(float(comp.get("final_price") or 0) - median),
                float(comp.get("final_price") or 0),
            ),
        )
        row_copy["comps"] = [
            _allowlist(_normalise_visual_fields(dict(comp)), COMP_FIELDS)
            for comp in evidence_rows[: max(comp_limit, 0)]
        ]
        enriched.append(row_copy)
    return enriched


def sync_state_path(*, endpoint: str, state_dir: str | Path) -> Path:
    endpoint_key = hashlib.sha256(endpoint.rstrip("/").encode("utf-8")).hexdigest()[:16]
    return Path(state_dir) / f"listings-{endpoint_key}.json"


def load_sync_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        loaded = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return loaded if isinstance(loaded, dict) else {}


def save_sync_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(state, indent=2, sort_keys=True, default=str) + "\n")
    tmp_path.replace(path)


def fetch_model_key_comp_summaries(model_keys: list[str]) -> list[dict[str, Any]]:
    requested = sorted({key.strip().lower() for key in model_keys if key.strip()})
    if not requested:
        return []

    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_utc_session(cur)
            cur.execute(
                """
                SELECT
                    model_key,
                    COALESCE(condition_grade, 'B') AS condition_grade,
                    samples,
                    median_final_price,
                    computed_at
                FROM latest_comps
                WHERE LOWER(model_key) = ANY(%s)
                  AND COALESCE(samples, 0) > 0
                  AND COALESCE(median_final_price, 0) > 0
                ORDER BY model_key, condition_grade
                """,
                (requested,),
            )
            rows = []
            for result in cur.fetchall():
                row = dict(result)
                model_key = str(row.get("model_key") or "")
                rows.append({
                    "source": _source_for_model_key(model_key),
                    **_allowlist(row, MODEL_KEY_COMP_FIELDS[1:]),
                })
            return rows
    finally:
        conn.close()


def build_model_key_comp_payload(
    model_keys: list[str],
    *,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    summaries = fetch_model_key_comp_summaries(model_keys)
    evidence = fetch_comp_evidence(summaries)
    for summary in summaries:
        key = (str(summary.get("model_key") or ""), str(summary.get("condition_grade") or "B"))
        median = float(summary.get("median_final_price") or 0)
        comps = sorted(
            evidence.get(key, []),
            key=lambda comp: (
                *_visual_sort_value(comp),
                abs(float(comp.get("final_price") or 0) - median),
                float(comp.get("final_price") or 0),
            ),
        )
        summary["comps"] = [
            _allowlist(_normalise_visual_fields(dict(comp)), MODEL_KEY_COMP_ROW_FIELDS)
            for comp in comps
        ]

    return {
        "schema_version": 1,
        "batch_id": str(uuid4()),
        "generated_at": generated.isoformat(),
        "model_key_comps": summaries,
    }


def fetch_sold_evidence_rows(*, since_days: int | None = None) -> list[dict[str, Any]]:
    """Fetch every confirmed local sold row for the broad historical evidence mirror."""

    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_utc_session(cur)
            since_filter = ""
            params: list[Any] = []
            if since_days is not None:
                since_filter = "AND a.end_time >= (now() AT TIME ZONE 'utc' - (%s || ' days')::interval)"
                params.append(str(max(since_days, 0)))

            cur.execute(
                f"""
                SELECT
                    a.source,
                    a.external_id,
                    a.title,
                    a.url,
                    a.sale_type,
                    a.end_time,
                    a.final_price,
                    a.bids_count,
                    graded.effective_condition_grade AS condition_grade,
                    a.model_key,
                    tc.text_grade AS text_scan_grade,
                    (tc.id IS NOT NULL) AS text_scan_checked,
                    COALESCE(tc.review_required, false) AS text_scan_review_required,
                    COALESCE(tc.risk_flags, ARRAY[]::text[]) AS text_scan_risk_flags,
                    tc.model_name AS text_scan_model_name,
                    tc.checked_at AS text_scan_checked_at,
                    vc.visual_grade AS visual_condition_grade,
                    (vc.id IS NOT NULL) AS visual_condition_checked,
                    COALESCE(vc.review_required, false) AS visual_review_required,
                    COALESCE(vc.damage_flags, ARRAY[]::text[]) AS visual_damage_flags,
                    vc.model_name AS visual_model_name,
                    vc.checked_at AS visual_checked_at
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
                ) AS graded
                WHERE a.final_price IS NOT NULL
                  AND a.final_price > 0
                  AND a.external_id IS NOT NULL
                  AND a.external_id <> ''
                  AND a.title IS NOT NULL
                  AND a.title <> ''
                  {since_filter}
                ORDER BY a.end_time DESC NULLS LAST, a.external_id
                """,
                params,
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def build_sold_evidence_payload(
    *,
    since_days: int | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    generated = generated_at or datetime.now(UTC)
    rows = fetch_sold_evidence_rows(since_days=since_days)
    return {
        "schema_version": 1,
        "batch_id": str(uuid4()),
        "generated_at": generated.isoformat(),
        "full_snapshot": since_days is None,
        "sold_evidence": [
            _allowlist(_normalise_visual_fields(dict(row)), SOLD_EVIDENCE_FIELDS)
            for row in rows
        ],
    }


def fetch_comp_evidence(
    rows: list[dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    """Fetch all requested same-key/same-grade sold evidence in one query."""

    pairs = sorted(
        {
            (str(row.get("model_key") or ""), str(row.get("condition_grade") or "B"))
            for row in rows
            if row.get("model_key")
        }
    )
    if not pairs:
        return {}

    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_utc_session(cur)
            requested_values = b", ".join(cur.mogrify("(%s, %s)", pair) for pair in pairs).decode()
            cur.execute(
                f"""
                WITH requested(model_key, condition_grade) AS (
                    VALUES {requested_values}
                )
                SELECT
                    a.source,
                    a.external_id,
                    a.title,
                    a.url,
                    a.sale_type,
                    a.end_time,
                    a.final_price,
                    a.bids_count,
                    graded.effective_condition_grade AS condition_grade,
                    a.model_key,
                    tc.text_grade AS text_scan_grade,
                    (tc.id IS NOT NULL) AS text_scan_checked,
                    COALESCE(tc.review_required, false) AS text_scan_review_required,
                    COALESCE(tc.risk_flags, ARRAY[]::text[]) AS text_scan_risk_flags,
                    tc.model_name AS text_scan_model_name,
                    tc.checked_at AS text_scan_checked_at,
                    vc.visual_grade AS visual_condition_grade,
                    (vc.id IS NOT NULL) AS visual_condition_checked,
                    COALESCE(vc.review_required, false) AS visual_review_required,
                    COALESCE(vc.damage_flags, ARRAY[]::text[]) AS visual_damage_flags,
                    vc.model_name AS visual_model_name,
                    vc.checked_at AS visual_checked_at
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
                ) AS graded
                JOIN requested AS r
                  ON r.model_key = a.model_key
                 AND r.condition_grade = graded.effective_condition_grade
                WHERE a.final_price IS NOT NULL
                  AND a.final_price > 0
                  AND a.end_time >= (now() AT TIME ZONE 'utc' - (%s || ' days')::interval)
                  AND LOWER(COALESCE(a.title, '') || ' ' || COALESCE(a.raw_attrs::text, '')) NOT LIKE ALL(%s)
                  AND NOT (
                      a.source = 'ebay-apple'
                      AND LOWER(COALESCE(a.title, '') || ' ' || COALESCE(a.raw_attrs::text, '')) LIKE '%%macbook%%'
                      AND LOWER(COALESCE(a.title, '') || ' ' || COALESCE(a.raw_attrs::text, '')) ~ '\\ma[0-9]{{1,2}}\\s*(pro|max|ultra)?\\M'
                  )
                  AND NOT (
                      a.source = 'ebay-apple'
                      AND LOWER(COALESCE(a.title, '') || ' ' || COALESCE(a.raw_attrs::text, '')) ~ '\\mlocked\\M'
                  )
                  AND NOT (
                      LOWER(a.model_key) LIKE 'apple-iphone-%%'
                      AND LOWER(a.model_key) !~ '^apple-iphone-(se|x|xr|xs|[0-9]+)(-|_)'
                  )
                  AND NOT (
                      LOWER(a.model_key) ~ '^apple-(macbook|imac|mac-mini|mac-studio)'
                      AND LOWER(a.model_key) !~ %s
                  )
                ORDER BY a.model_key, condition_grade, a.final_price, a.end_time DESC NULLS LAST
                """,
                (
                    str(_get_window_days()),
                    list(COMPS_BLOCKED_TEXT_PATTERNS),
                    APPLE_MAC_ALLOWED_CPU_PATTERN,
                ),
            )
            evidence: dict[tuple[str, str], list[dict[str, Any]]] = {}
            for result in cur.fetchall():
                row = dict(result)
                row = _normalise_visual_fields(row)
                key = (str(row.get("model_key") or ""), str(row.get("condition_grade") or "B"))
                evidence.setdefault(key, []).append(row)
            return evidence
    finally:
        conn.close()


def fetch_live_listing_rows() -> list[dict[str, Any]]:
    """Fetch all live, priced listings for the website's browse views."""

    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_utc_session(cur)
            cur.execute(
                """
                SELECT
                    a.source,
                    a.external_id,
                    a.title,
                    a.url,
                    a.model_key,
                    grade.effective_condition_grade AS condition_grade,
                    grade.text_condition_grade,
                    tc.text_grade AS text_scan_grade,
                    (tc.id IS NOT NULL) AS text_scan_checked,
                    COALESCE(tc.review_required, false) AS text_scan_review_required,
                    COALESCE(tc.risk_flags, ARRAY[]::text[]) AS text_scan_risk_flags,
                    tc.model_name AS text_scan_model_name,
                    tc.checked_at AS text_scan_checked_at,
                    vc.visual_grade AS visual_condition_grade,
                    (vc.id IS NOT NULL) AS visual_condition_checked,
                    COALESCE(vc.review_required, false) AS visual_review_required,
                    COALESCE(vc.damage_flags, ARRAY[]::text[]) AS visual_damage_flags,
                    vc.model_name AS visual_model_name,
                    vc.checked_at AS visual_checked_at,
                    a.sale_type,
                    a.raw_attrs,
                    a.end_time,
                    COALESCE(a.price_current, 0) AS price_current,
                    COALESCE(lc.samples, 0) AS samples,
                    COALESCE(lc.median_final_price, 0) AS median_final_price
                FROM auction_listings AS a
                LEFT JOIN latest_listing_visual_conditions AS vc
                  ON vc.listing_id = a.id
                LEFT JOIN latest_listing_text_conditions AS tc
                  ON tc.listing_id = a.id
                CROSS JOIN LATERAL (
                    SELECT
                        COALESCE(a.condition_grade, 'B') AS text_condition_grade,
                        CASE
                            WHEN 'D' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'D'
                            WHEN 'C' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'C'
                            WHEN 'B' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, '')), UPPER(COALESCE(vc.visual_grade, ''))) THEN 'B'
                            ELSE 'A'
                        END AS effective_condition_grade
                ) AS grade
                LEFT JOIN latest_comps AS lc
                  ON lc.model_key = a.model_key
                 AND lc.condition_grade = grade.effective_condition_grade
                WHERE a.status = 'live'
                  AND a.price_current IS NOT NULL
                  AND a.price_current > 0
                  AND a.external_id IS NOT NULL
                  AND a.external_id <> ''
                  AND a.title IS NOT NULL
                  AND a.title <> ''
                  AND a.url IS NOT NULL
                  AND a.url <> ''
                  AND COALESCE(a.last_seen_at, a.first_seen) >= now() - (%s || ' hours')::interval
                ORDER BY a.source, a.end_time ASC NULLS LAST, a.external_id
                """,
                (str(live_listing_max_age_hours()),),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def _browse_listing_from_row(row: dict[str, Any], *, min_profit: float) -> dict[str, Any]:
    attrs = row.get("raw_attrs") if isinstance(row.get("raw_attrs"), dict) else {}
    has_comps = int(row.get("samples") or 0) > 0 and float(row.get("median_final_price") or 0) > 0
    has_clean_identity = has_investible_product_identity(
        str(row.get("title") or ""),
        attrs=attrs,
        source=str(row.get("source") or ""),
        model_key=str(row.get("model_key") or ""),
    )

    if has_comps and has_clean_identity:
        return potential_from_row(row, min_profit=min_profit).__dict__

    reason = "no same-grade sold evidence yet"
    if has_comps and not has_clean_identity:
        reason = "browse-only row; blocked from buy maths by identity guardrails"

    return {
        "source": str(row.get("source") or ""),
        "external_id": str(row.get("external_id") or ""),
        "title": str(row.get("title") or ""),
        "url": str(row.get("url") or ""),
        "model_key": str(row.get("model_key") or "unknown"),
        "condition_grade": str(row.get("condition_grade") or "B"),
        "text_condition_grade": str(row.get("text_condition_grade") or row.get("condition_grade") or "B"),
        "text_scan_grade": row.get("text_scan_grade"),
        "text_scan_checked": bool(row.get("text_scan_checked", row.get("text_scan_checked_at") is not None)),
        "text_scan_review_required": bool(row.get("text_scan_review_required", False)),
        "text_scan_risk_flags": list(row.get("text_scan_risk_flags") or []),
        "text_scan_model_name": row.get("text_scan_model_name"),
        "text_scan_checked_at": row.get("text_scan_checked_at"),
        "visual_condition_grade": row.get("visual_condition_grade"),
        "visual_condition_checked": bool(row.get("visual_condition_checked", row.get("visual_checked_at") is not None)),
        "visual_review_required": bool(row.get("visual_review_required", False)),
        "visual_damage_flags": list(row.get("visual_damage_flags") or []),
        "visual_model_name": row.get("visual_model_name"),
        "visual_checked_at": row.get("visual_checked_at"),
        "sale_type": str(row.get("sale_type") or ""),
        "end_time": row.get("end_time"),
        "current_price": round(float(row.get("price_current") or 0.0), 2),
        "sold_median": 0.0,
        "comp_samples": 0,
        "max_safe_buy": 0.0,
        "expected_profit": 0.0,
        "expected_roi": 0.0,
        "price_gap_to_max_buy": 0.0,
        "effective_min_comps": 1,
        "signal": reason,
    }


def create_snapshot(
    *,
    comp_limit: int = 25,
    publish_min_profit: float = 15.0,
    include_all_live: bool = True,
    include_comps: bool = True,
) -> dict[str, Any]:
    report = run_potentials(limit=0, min_profit=50.0, min_comps=None)
    checked = [
        row
        for row in report.get("closest", [])
        if isinstance(row, dict)
        and float(row.get("expected_profit") or 0) >= publish_min_profit
    ]

    if include_all_live:
        existing_ids = {str(row.get("external_id") or "") for row in checked}
        for row in fetch_live_listing_rows():
            external_id = str(row.get("external_id") or "")
            if not external_id or external_id in existing_ids:
                continue
            checked.append(_browse_listing_from_row(row, min_profit=50.0))
            existing_ids.add(external_id)

    checked.sort(key=lambda row: (
        str(row.get("source") or ""),
        row.get("end_time") is None,
        row.get("end_time") or datetime.max,
        str(row.get("external_id") or ""),
    ))

    report = {**report, "closest": checked}
    report["summary"] = {
        **(report.get("summary") or {}),
        "published": len(checked),
        "publish_min_profit": publish_min_profit,
        "include_all_live": include_all_live,
    }
    if not include_comps:
        return build_sync_payload(report, comp_limit=0, evidence={})

    evidence = fetch_comp_evidence(checked)
    for row in checked:
        key = (str(row.get("model_key") or ""), str(row.get("condition_grade") or "B"))
        median = float(row.get("sold_median") or 0)
        evidence[key] = sorted(
            evidence.get(key, []),
            key=lambda comp: (
                *_visual_sort_value(comp),
                abs(float(comp.get("final_price") or 0) - median),
                float(comp.get("final_price") or 0),
            ),
        )
    return build_sync_payload(report, comp_limit=comp_limit, evidence=evidence)


def send_snapshot(
    payload: dict[str, Any],
    *,
    endpoint: str,
    secret: str,
    timeout: int = 300,
) -> dict[str, Any]:
    """POST one HMAC-signed snapshot to Laravel."""

    body = json.dumps(payload, default=str, separators=(",", ":"))
    timestamp = str(int(datetime.now(UTC).timestamp()))
    signature = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.{body}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    response = requests.post(
        f"{endpoint.rstrip('/')}/api/v1/sync/listings",
        data=body.encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-GhostFrog-Timestamp": timestamp,
            "X-GhostFrog-Signature": signature,
        },
        timeout=timeout,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        raise requests.HTTPError(
            f"{exc}; response body: {response.text[:2000]}",
            response=response,
        ) from exc
    result = response.json()
    return result if isinstance(result, dict) else {"result": result}


def send_model_key_comp_snapshot(
    payload: dict[str, Any],
    *,
    endpoint: str,
    secret: str,
    timeout: int = 120,
) -> dict[str, Any]:
    """POST one HMAC-signed model-key comp summary snapshot to Laravel."""

    body = json.dumps(payload, default=str, separators=(",", ":"))
    timestamp = str(int(datetime.now(UTC).timestamp()))
    signature = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.{body}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    response = requests.post(
        f"{endpoint.rstrip('/')}/api/v1/sync/model-key-comps",
        data=body.encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-GhostFrog-Timestamp": timestamp,
            "X-GhostFrog-Signature": signature,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    result = response.json()
    return result if isinstance(result, dict) else {"result": result}


def send_sold_evidence_snapshot(
    payload: dict[str, Any],
    *,
    endpoint: str,
    secret: str,
    timeout: int = 120,
) -> dict[str, Any]:
    """POST one HMAC-signed historical sold evidence snapshot to Laravel."""

    body = json.dumps(payload, default=str, separators=(",", ":"))
    timestamp = str(int(datetime.now(UTC).timestamp()))
    signature = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.{body}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    response = requests.post(
        f"{endpoint.rstrip('/')}/api/v1/sync/sold-evidence",
        data=body.encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-GhostFrog-Timestamp": timestamp,
            "X-GhostFrog-Signature": signature,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    result = response.json()
    return result if isinstance(result, dict) else {"result": result}


def send_snapshot_chunked(
    payload: dict[str, Any],
    *,
    endpoint: str,
    secret: str,
    chunk_size: int = 500,
    timeout: int = 120,
) -> dict[str, Any]:
    """POST a large snapshot as smaller signed chunks with one final swap."""

    listings = payload.get("listings", [])
    if not isinstance(listings, list) or len(listings) <= chunk_size:
        return send_snapshot(payload, endpoint=endpoint, secret=secret, timeout=timeout)

    total_chunks = max(1, math.ceil(len(listings) / chunk_size))
    result: dict[str, Any] = {}
    for index in range(total_chunks):
        start = index * chunk_size
        end = start + chunk_size
        chunk_payload = {
            **payload,
            "listings": listings[start:end],
            "chunk": {
                "index": index + 1,
                "total": total_chunks,
                "final": index + 1 == total_chunks,
            },
        }
        result = send_snapshot(
            chunk_payload,
            endpoint=endpoint,
            secret=secret,
            timeout=timeout,
        )
        print(
            f"Synced chunk {index + 1}/{total_chunks}: "
            f"{result.get('listings', len(chunk_payload['listings']))} listings, "
            f"{result.get('comps', 0)} comps."
        )
    return result


def send_delta_chunked(
    payload: dict[str, Any],
    *,
    endpoint: str,
    secret: str,
    chunk_size: int = 500,
    timeout: int = 120,
) -> dict[str, Any]:
    """POST delta upserts in chunks while pruning only after the final chunk."""

    listings = payload.get("listings", [])
    if not isinstance(listings, list) or len(listings) <= chunk_size:
        return send_snapshot(payload, endpoint=endpoint, secret=secret, timeout=timeout)

    total_chunks = max(1, math.ceil(len(listings) / chunk_size))
    result: dict[str, Any] = {}
    sent = 0
    for index in range(total_chunks):
        start = index * chunk_size
        end = start + chunk_size
        is_final = index + 1 == total_chunks
        chunk_payload = {
            **payload,
            "batch_id": str(uuid4()),
            "listings": listings[start:end],
            "expired_external_ids": payload.get("expired_external_ids", []) if is_final else [],
            "chunk": {
                "index": index + 1,
                "total": total_chunks,
                "final": is_final,
            },
        }
        if is_final and "current_external_ids" in payload:
            chunk_payload["current_external_ids"] = payload["current_external_ids"]
        else:
            chunk_payload.pop("current_external_ids", None)

        result = send_snapshot(
            chunk_payload,
            endpoint=endpoint,
            secret=secret,
            timeout=timeout,
        )
        sent += len(chunk_payload["listings"])
        print(
            f"Synced delta chunk {index + 1}/{total_chunks}: "
            f"{len(chunk_payload['listings'])} upserts, "
            f"{len(chunk_payload.get('expired_external_ids', []))} expired.",
            flush=True,
        )
    return {**result, "upserts": sent}


def send_sold_evidence_chunked(
    payload: dict[str, Any],
    *,
    endpoint: str,
    secret: str,
    chunk_size: int = 1000,
    timeout: int = 120,
) -> dict[str, Any]:
    """POST the broad historical evidence mirror in signed chunks."""

    rows = payload.get("sold_evidence", [])
    if not isinstance(rows, list) or len(rows) <= chunk_size:
        return send_sold_evidence_snapshot(payload, endpoint=endpoint, secret=secret, timeout=timeout)

    total_chunks = max(1, math.ceil(len(rows) / chunk_size))
    result: dict[str, Any] = {}
    sent = 0
    for index in range(total_chunks):
        start = index * chunk_size
        end = start + chunk_size
        chunk_rows = rows[start:end]
        chunk_payload = {
            **payload,
            "sold_evidence": chunk_rows,
            "chunk": {
                "index": index + 1,
                "total": total_chunks,
                "final": index + 1 == total_chunks,
            },
        }
        result = send_sold_evidence_snapshot(
            chunk_payload,
            endpoint=endpoint,
            secret=secret,
            timeout=timeout,
        )
        sent += len(chunk_rows)
        print(
            f"Synced sold-evidence chunk {index + 1}/{total_chunks}: "
            f"{result.get('sold_evidence', len(chunk_rows))} rows."
        )
    return {**result, "sold_evidence": sent}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish decision-ready listings and sold evidence to Laravel."
    )
    parser.add_argument(
        "--send",
        action="store_true",
        help="Send the snapshot. Without this flag the command is a read-only dry run.",
    )
    parser.add_argument("--endpoint", default=os.getenv("GHOSTFROG_WEB_URL", ""))
    parser.add_argument("--secret", default=os.getenv("GHOSTFROG_SYNC_SECRET", ""))
    parser.add_argument("--comp-limit", type=int, default=25)
    parser.add_argument("--chunk-size", type=int, default=500)
    parser.add_argument(
        "--full-snapshot",
        action="store_true",
        help="Replace the whole remote live-listing table. Default is delta sync.",
    )
    parser.add_argument(
        "--sync-state-dir",
        default=os.getenv("GHOSTFROG_REMOTE_SYNC_STATE_DIR", "logs/remote-sync-state"),
        help="Directory used to remember listing hashes for delta sync.",
    )
    parser.add_argument(
        "--skip-bootstrap-upserts",
        action="store_true",
        help="When no delta state exists, send IDs only. Use only if Laravel is already fully seeded.",
    )
    parser.add_argument(
        "--force-upserts",
        action="store_true",
        help="Send every current publishable listing as delta upserts, in chunks if needed.",
    )
    parser.add_argument(
        "--model-key",
        action="append",
        default=[],
        help="Model key to publish to the remote market-evidence search.",
    )
    parser.add_argument(
        "--model-key-only",
        action="store_true",
        help="Only publish the requested model-key comp summaries.",
    )
    parser.add_argument(
        "--publish-min-profit",
        type=float,
        default=15.0,
        help="Publish rows at or above this expected-profit floor (default GBP 15).",
    )
    parser.add_argument(
        "--shortlist-only",
        action="store_true",
        help="Only publish the historical profit shortlist. By default all live priced rows are mirrored.",
    )
    parser.add_argument(
        "--sold-evidence-only",
        action="store_true",
        help="Only publish confirmed historical sold rows to the broad evidence mirror.",
    )
    parser.add_argument(
        "--sold-evidence-since-days",
        type=int,
        default=None,
        help="Only publish sold evidence whose end time is within this many days. Omit to mirror all sold evidence.",
    )
    parser.add_argument("--json", action="store_true", help="Print the safe payload.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.sold_evidence_only:
        payload = build_sold_evidence_payload(since_days=args.sold_evidence_since_days)
        if args.json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            window = (
                f"last {args.sold_evidence_since_days} days"
                if args.sold_evidence_since_days is not None
                else "all time"
            )
            print(
                f"Remote sold-evidence snapshot: "
                f"{len(payload['sold_evidence'])} confirmed sold rows ({window})."
            )

        if not payload["sold_evidence"]:
            print("No matching sold evidence rows found; nothing was sent.")
            return 1
        if not args.send:
            print("Dry run only; nothing was sent.")
            return 0
        if not args.endpoint or not args.secret:
            print("Send requires GHOSTFROG_WEB_URL and GHOSTFROG_SYNC_SECRET.")
            return 2

        result = send_sold_evidence_chunked(
            payload,
            endpoint=args.endpoint,
            secret=args.secret,
            chunk_size=max(args.chunk_size, 1),
        )
        print(
            f"Published sold-evidence batch {result.get('batch_id', payload['batch_id'])}: "
            f"{result.get('sold_evidence', 0)} rows."
        )
        return 0

    if args.model_key_only:
        if not args.model_key:
            print("--model-key-only requires at least one --model-key value.")
            return 2
        payload = build_model_key_comp_payload(args.model_key)
        if args.json:
            print(json.dumps(payload, indent=2, default=str))
        else:
            print(
                f"Remote model-key snapshot: "
                f"{len(payload['model_key_comps'])} comp summary rows."
            )
            for row in payload["model_key_comps"]:
                print(
                    f"  {row['model_key']} | grade {row['condition_grade']} | "
                    f"{row['samples']} comps | median £{float(row['median_final_price']):.2f} | "
                    f"{len(row.get('comps', []))} rows"
                )
        if not payload["model_key_comps"]:
            print("No matching latest_comps rows found; nothing was sent.")
            return 1
        if not args.send:
            print("Dry run only; nothing was sent.")
            return 0
        if not args.endpoint or not args.secret:
            print("Send requires GHOSTFROG_WEB_URL and GHOSTFROG_SYNC_SECRET.")
            return 2
        result = send_model_key_comp_snapshot(
            payload,
            endpoint=args.endpoint,
            secret=args.secret,
        )
        print(
            f"Published model-key batch {result.get('batch_id', payload['batch_id'])}: "
            f"{result.get('model_key_comps', 0)} comp summaries, "
            f"{result.get('comp_rows', 0)} comp rows."
        )
        return 0

    payload = create_snapshot(
        comp_limit=max(args.comp_limit, 0),
        publish_min_profit=args.publish_min_profit,
        include_all_live=not args.shortlist_only,
        include_comps=args.full_snapshot,
    )
    state_path = sync_state_path(
        endpoint=args.endpoint or "dry-run",
        state_dir=args.sync_state_dir,
    )
    previous_state = load_sync_state(state_path)
    delta_payload = build_delta_payload(
        payload,
        previous_state=previous_state,
        bootstrap_without_upserts=args.skip_bootstrap_upserts,
        force_upserts=args.force_upserts,
    )
    if not args.full_snapshot:
        delta_payload["listings"] = attach_comp_evidence_to_listings(
            delta_payload["listings"],
            comp_limit=max(args.comp_limit, 0),
        )

    if args.json:
        printable = payload if args.full_snapshot else delta_payload
        print(json.dumps(printable, indent=2, default=str))
    else:
        comp_count = sum(len(row.get("comps", [])) for row in payload["listings"])
        if args.full_snapshot:
            print(
                f"Remote web full snapshot: {len(payload['listings'])} listings, "
                f"{comp_count} confirmed sold evidence rows."
            )
        else:
            delta_comp_count = sum(len(row.get("comps", [])) for row in delta_payload["listings"])
            print(
                f"Remote web delta: {len(delta_payload['listings'])} changed listings, "
                f"{len(delta_payload['expired_external_ids'])} expired IDs, "
                f"{len(delta_payload.get('current_external_ids', []))} current IDs, "
                f"{delta_comp_count} changed-row comps.",
                flush=True,
            )

    if not args.send:
        print("Dry run only; nothing was sent.")
        return 0
    if not args.endpoint or not args.secret:
        print("Send requires GHOSTFROG_WEB_URL and GHOSTFROG_SYNC_SECRET.")
        return 2

    if args.full_snapshot:
        result = send_snapshot_chunked(
            payload,
            endpoint=args.endpoint,
            secret=args.secret,
            chunk_size=max(args.chunk_size, 1),
        )
        save_sync_state(state_path, build_listing_sync_state(payload, endpoint=args.endpoint))
        print(
            f"Published full batch {result.get('batch_id', payload['batch_id'])}: "
            f"{result.get('listings', 0)} listings, {result.get('comps', 0)} comps."
        )
        return 0

    result = send_delta_chunked(
        delta_payload,
        endpoint=args.endpoint,
        secret=args.secret,
        chunk_size=max(args.chunk_size, 1),
    )
    save_sync_state(state_path, build_listing_sync_state(payload, endpoint=args.endpoint))
    print(
        f"Published delta {result.get('batch_id', delta_payload['batch_id'])}: "
        f"{result.get('upserts', 0)} upserts, {result.get('expired', 0)} expired, "
        f"{result.get('listings', 0)} listings live on site, {result.get('comps', 0)} comps."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
