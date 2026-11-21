from __future__ import annotations

import argparse
import json
import os
import time
from dataclasses import asdict, dataclass
from typing import Any, Mapping

from psycopg2.extras import Json, RealDictCursor

from pipelines.listing.comps.graph import (
    APPLE_MAC_ALLOWED_CPU_PATTERN,
    COMPS_BLOCKED_TEXT_PATTERNS,
    _get_window_days,
    run as run_comps,
)
from pipelines.listing.market_discovery.graph import (
    FEE_RATE,
    INBOUND_SHIP_GBP,
    MIN_PROFIT_GBP,
    OUTBOUND_SHIP_GBP,
    PACKAGING_GBP,
    SELLING_RESERVE_GBP,
)
from pipelines.listing.text_condition.ollama_client import (
    DEFAULT_MODEL,
    assess_text,
)
from utils import db_schema as schema

WEB_VISIBLE_MIN_COMPS = max(1, int(os.getenv("GF_HEARTBEAT_BUY_SIGNAL_MIN_COMPS", "50")))


@dataclass
class TextCandidate:
    listing_id: int
    external_id: str
    source: str
    title: str
    url: str
    model_key: str
    condition_grade: str
    status: str
    raw_attrs: dict[str, Any]
    priority_flag: str = "backfill"
    queue_id: int | None = None


def ensure_auction_listing_text_flags() -> None:
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'auction_listings'
              AND column_name IN ('text_condition_checked', 'text_condition_checked_at')
            """
        )
        found = {str(row[0]) for row in cur.fetchall()}
    missing = {"text_condition_checked", "text_condition_checked_at"} - found
    if missing:
        raise RuntimeError(
            "Missing auction_listings text condition columns; run migrations before starting text condition worker: "
            + ", ".join(sorted(missing))
        )


def ensure_text_condition_table() -> None:
    ensure_auction_listing_text_flags()
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_text_conditions (
                id BIGSERIAL PRIMARY KEY,
                listing_id BIGINT NOT NULL REFERENCES auction_listings(id) ON DELETE CASCADE,
                external_id TEXT NOT NULL,
                model_name TEXT NOT NULL,
                assessment JSONB NOT NULL,
                text_grade TEXT,
                confidence NUMERIC,
                risk_flags TEXT[] NOT NULL DEFAULT '{}'::text[],
                review_required BOOLEAN NOT NULL DEFAULT TRUE,
                checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_text_conditions_listing_latest_idx
                ON listing_text_conditions (listing_id, checked_at DESC, id DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_text_conditions_external_latest_idx
                ON listing_text_conditions (external_id, checked_at DESC, id DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_text_conditions_review_idx
                ON listing_text_conditions (review_required, checked_at DESC)
            """
        )
        cur.execute("SELECT to_regclass('latest_listing_text_conditions') IS NOT NULL")
        if not cur.fetchone()[0]:
            raise RuntimeError(
                "Missing latest_listing_text_conditions view; run migrations before starting text condition worker."
            )


def ensure_text_condition_queue_table() -> None:
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_text_condition_queue (
                id BIGSERIAL PRIMARY KEY,
                listing_id BIGINT NOT NULL REFERENCES auction_listings(id) ON DELETE CASCADE,
                external_id TEXT NOT NULL,
                source TEXT NOT NULL,
                status_name TEXT NOT NULL,
                priority_flag TEXT NOT NULL DEFAULT 'backfill',
                priority BIGINT NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                queued_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
                locked_at TIMESTAMP WITHOUT TIME ZONE,
                processed_at TIMESTAMP WITHOUT TIME ZONE
            )
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS listing_text_condition_queue_listing_idx
                ON listing_text_condition_queue (listing_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_text_condition_queue_pending_idx
                ON listing_text_condition_queue (status, priority DESC, queued_at ASC)
            """
        )


def _product_predicate(product: str, *, alias: str = "a") -> tuple[str, list[Any]]:
    product = product.strip().lower()
    if product == "iphone":
        return f"AND ({alias}.title ILIKE %s OR {alias}.model_key ILIKE %s)", ["%iphone%", "apple-iphone%"]
    if product == "galaxy":
        return f"AND ({alias}.title ILIKE %s OR {alias}.model_key ILIKE %s)", ["%galaxy%", "%samsung-galaxy%"]
    if product:
        return f"AND ({alias}.title ILIKE %s OR {alias}.model_key ILIKE %s)", [f"%{product}%", f"%{product}%"]
    return "", []


def _status_predicate(status: str, *, alias: str = "a") -> tuple[str, list[Any]]:
    status = status.strip().lower()
    if status == "all":
        return "", []
    if status not in {"live", "sold", "ended", "unknown"}:
        raise SystemExit(f"Invalid status {status!r}")
    return f"AND {alias}.status = %s", [status]


def enqueue_text_candidates(
    *,
    limit: int = 1000,
    source: str = "",
    product: str = "",
    status: str = "all",
    max_attempts: int = 3,
    new_listing_minutes: int = 360,
) -> int:
    ensure_text_condition_table()
    ensure_text_condition_queue_table()
    product_predicate, product_params = _product_predicate(product)
    status_predicate, status_params = _status_predicate(status)
    source_predicate = "AND a.source = %s" if source else ""
    def resale_net_expr(alias: str) -> str:
        return (
            f"(COALESCE({alias}.median_final_price, 0) * {1.0 - FEE_RATE} "
            f"- {OUTBOUND_SHIP_GBP} - {INBOUND_SHIP_GBP} "
            f"- {PACKAGING_GBP} - {SELLING_RESERVE_GBP})"
        )

    main_resale_net_expr = resale_net_expr("lc")
    expected_profit_expr = f"({main_resale_net_expr} - COALESCE(a.price_current, 0))"
    max_safe_buy_expr = f"({main_resale_net_expr} - {MIN_PROFIT_GBP})"
    potential_resale_net_expr = resale_net_expr("live_lc")
    potential_expected_profit_expr = f"({potential_resale_net_expr} - COALESCE(live.price_current, 0))"
    potential_max_safe_buy_expr = f"({potential_resale_net_expr} - {MIN_PROFIT_GBP})"
    dashboard_potential_predicate = (
        f"COALESCE(lc.samples, 0) >= {WEB_VISIBLE_MIN_COMPS} "
        f"AND {expected_profit_expr} >= {MIN_PROFIT_GBP} "
        f"AND COALESCE(a.price_current, 0) <= {max_safe_buy_expr}"
    )
    potential_pair_predicate = (
        f"COALESCE(live_lc.samples, 0) >= {WEB_VISIBLE_MIN_COMPS} "
        f"AND {potential_expected_profit_expr} >= {MIN_PROFIT_GBP} "
        f"AND COALESCE(live.price_current, 0) <= {potential_max_safe_buy_expr}"
    )
    source_gate_predicate = (
        "COALESCE(lc.samples, 0) > 0 "
        f"AND COALESCE(a.price_current, 0) <= {max_safe_buy_expr}"
    )
    source_visible_predicate = (
        "COALESCE(lc.samples, 0) > 0 "
        f"AND {max_safe_buy_expr} >= 0"
    )
    params: list[Any] = [
        max(new_listing_minutes, 1),
        max(new_listing_minutes, 1),
        str(_get_window_days()),
        list(COMPS_BLOCKED_TEXT_PATTERNS),
        APPLE_MAC_ALLOWED_CPU_PATTERN,
    ]
    if source:
        params.append(source)
    params.extend(product_params)
    params.extend(status_params)
    params.extend([max(limit, 1), max_attempts])

    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            UPDATE listing_text_condition_queue
               SET status = 'pending',
                   locked_at = NULL,
                   last_error = 'worker lock expired'
             WHERE status = 'processing'
               AND attempts < %s
               AND locked_at < ((now() AT TIME ZONE 'utc') - interval '30 minutes')
            """,
            (max_attempts,),
        )
        cur.execute(
            f"""
            WITH current_potential_pairs AS (
                SELECT DISTINCT
                    live.model_key,
                    live_grade.effective_condition_grade AS condition_grade
                FROM auction_listings AS live
                LEFT JOIN latest_listing_visual_conditions AS live_vc
                  ON live_vc.listing_id = live.id
                LEFT JOIN latest_listing_text_conditions AS live_tc
                  ON live_tc.listing_id = live.id
                CROSS JOIN LATERAL (
                    SELECT CASE
                        WHEN 'D' IN (UPPER(COALESCE(live.condition_grade, 'B')), UPPER(COALESCE(live_tc.text_grade, '')), UPPER(COALESCE(live_vc.visual_grade, ''))) THEN 'D'
                        WHEN 'C' IN (UPPER(COALESCE(live.condition_grade, 'B')), UPPER(COALESCE(live_tc.text_grade, '')), UPPER(COALESCE(live_vc.visual_grade, ''))) THEN 'C'
                        WHEN 'B' IN (UPPER(COALESCE(live.condition_grade, 'B')), UPPER(COALESCE(live_tc.text_grade, '')), UPPER(COALESCE(live_vc.visual_grade, ''))) THEN 'B'
                        ELSE 'A'
                    END AS effective_condition_grade
                ) AS live_grade
                JOIN latest_comps AS live_lc
                  ON live_lc.model_key = live.model_key
                 AND live_lc.condition_grade = live_grade.effective_condition_grade
                WHERE live.status = 'live'
                  AND live.price_current IS NOT NULL
                  AND live.price_current > 0
                  AND live.model_key IS NOT NULL
                  AND live.model_key <> ''
                  AND live.model_key <> 'unknown'
                  AND live.external_id IS NOT NULL
                  AND live.external_id <> ''
                  AND live.title IS NOT NULL
                  AND live.title <> ''
                  AND {potential_pair_predicate}
            )
            INSERT INTO listing_text_condition_queue (
                listing_id,
                external_id,
                source,
                status_name,
                priority_flag,
                priority,
                status,
                queued_at
            )
            SELECT
                a.id,
                a.external_id,
                a.source,
                COALESCE(a.status, ''),
                CASE
                    WHEN a.status = 'live' AND {dashboard_potential_predicate}
                        THEN 'dashboard_potential'
                    WHEN a.final_price IS NOT NULL
                         AND potential_pair.model_key IS NOT NULL
                        THEN 'potential_comp_evidence'
                    WHEN a.status = 'live' AND {source_gate_predicate}
                        THEN 'source_gate'
                    WHEN a.status = 'live' AND {source_visible_predicate}
                        THEN 'source_visible'
                    WHEN a.status = 'live'
                         AND a.first_seen IS NOT NULL
                         AND a.first_seen >= ((now() AT TIME ZONE 'utc') - (%s * interval '1 minute'))
                        THEN 'new_listing'
                    WHEN a.status = 'live' THEN 'live'
                    WHEN a.final_price IS NOT NULL THEN 'sold_comp'
                    ELSE 'backfill'
                END,
                CASE
                    WHEN a.status = 'live' AND {dashboard_potential_predicate}
                        THEN 6000000000000
                    WHEN a.final_price IS NOT NULL
                         AND potential_pair.model_key IS NOT NULL
                        THEN 5500000000000
                    WHEN a.status = 'live' AND {source_gate_predicate}
                        THEN 5000000000000
                    WHEN a.status = 'live' AND {source_visible_predicate}
                        THEN 4000000000000
                    WHEN a.status = 'live'
                         AND a.first_seen IS NOT NULL
                         AND a.first_seen >= ((now() AT TIME ZONE 'utc') - (%s * interval '1 minute'))
                        THEN 3000000000000
                    WHEN a.status = 'live' THEN 2000000000000
                    WHEN a.final_price IS NOT NULL THEN 1000000000000
                    ELSE 0
                END + COALESCE(EXTRACT(EPOCH FROM a.first_seen)::bigint, EXTRACT(EPOCH FROM a.end_time)::bigint, EXTRACT(EPOCH FROM a.last_seen_at)::bigint, 0),
                'pending',
                (now() AT TIME ZONE 'utc')
            FROM auction_listings AS a
            LEFT JOIN latest_listing_text_conditions AS tc
              ON tc.listing_id = a.id
            LEFT JOIN latest_listing_visual_conditions AS vc
              ON vc.listing_id = a.id
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
            LEFT JOIN current_potential_pairs AS potential_pair
              ON potential_pair.model_key = a.model_key
             AND potential_pair.condition_grade = grade.effective_condition_grade
            WHERE a.external_id IS NOT NULL
              AND a.external_id <> ''
              AND a.title IS NOT NULL
              AND a.title <> ''
              AND tc.id IS NULL
              AND (
                  a.final_price IS NULL
                  OR (
                      a.final_price > 0
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
                          LOWER(COALESCE(a.model_key, '')) LIKE 'apple-iphone-%%'
                          AND LOWER(COALESCE(a.model_key, '')) !~ '^apple-iphone-(se|x|xr|xs|[0-9]+)(-|_)'
                      )
                      AND NOT (
                          LOWER(COALESCE(a.model_key, '')) ~ '^apple-(macbook|imac|mac-mini|mac-studio)'
                          AND LOWER(COALESCE(a.model_key, '')) !~ %s
                      )
                  )
              )
              {source_predicate}
              {product_predicate}
              {status_predicate}
            ORDER BY
                6 DESC,
                CASE WHEN a.status = 'live' THEN 0 WHEN a.final_price IS NOT NULL THEN 1 ELSE 2 END,
                a.first_seen DESC NULLS LAST,
                a.end_time DESC NULLS LAST,
                a.id DESC
            LIMIT %s
            ON CONFLICT (listing_id) DO UPDATE
                SET priority = GREATEST(listing_text_condition_queue.priority, EXCLUDED.priority),
                    priority_flag = CASE
                        WHEN EXCLUDED.priority > listing_text_condition_queue.priority
                            THEN EXCLUDED.priority_flag
                        ELSE listing_text_condition_queue.priority_flag
                    END,
                    source = EXCLUDED.source,
                    status_name = EXCLUDED.status_name,
                    status = CASE
                        WHEN listing_text_condition_queue.status = 'error'
                             AND listing_text_condition_queue.attempts < %s
                            THEN 'pending'
                        ELSE listing_text_condition_queue.status
                    END
            RETURNING id
            """,
            params,
        )
        return len(cur.fetchall())


def load_candidates(
    *,
    limit: int,
    source: str = "",
    product: str = "",
    status: str = "live",
    include_checked: bool = False,
) -> list[TextCandidate]:
    ensure_text_condition_table()
    product_predicate, product_params = _product_predicate(product)
    status_predicate, status_params = _status_predicate(status)
    source_predicate = "AND a.source = %s" if source else ""
    checked_predicate = "" if include_checked else "AND tc.id IS NULL"
    params: list[Any] = []
    if source:
        params.append(source)
    params.extend(product_params)
    params.extend(status_params)
    params.append(max(limit, 1))

    conn = schema.get_connection()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            f"""
            SELECT
                a.id AS listing_id,
                a.external_id,
                a.source,
                a.title,
                a.url,
                COALESCE(a.model_key, 'unknown') AS model_key,
                COALESCE(a.condition_grade, 'B') AS condition_grade,
                a.status,
                COALESCE(a.raw_attrs, '{{}}'::jsonb) AS raw_attrs
            FROM auction_listings AS a
            LEFT JOIN latest_listing_text_conditions AS tc
              ON tc.listing_id = a.id
            WHERE a.external_id IS NOT NULL
              AND a.external_id <> ''
              AND a.title IS NOT NULL
              AND a.title <> ''
              {source_predicate}
              {product_predicate}
              {status_predicate}
              {checked_predicate}
            ORDER BY a.end_time DESC NULLS LAST, a.first_seen DESC NULLS LAST, a.id DESC
            LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()

    candidates: list[TextCandidate] = []
    for row in rows:
        raw_attrs = row.get("raw_attrs") if isinstance(row.get("raw_attrs"), dict) else {}
        candidates.append(
            TextCandidate(
                listing_id=int(row["listing_id"]),
                external_id=str(row.get("external_id") or ""),
                source=str(row.get("source") or ""),
                title=str(row.get("title") or ""),
                url=str(row.get("url") or ""),
                model_key=str(row.get("model_key") or ""),
                condition_grade=str(row.get("condition_grade") or "B"),
                status=str(row.get("status") or ""),
                raw_attrs=raw_attrs,
            )
        )
    return candidates


def claim_text_candidates(*, limit: int, max_attempts: int = 3) -> list[TextCandidate]:
    ensure_text_condition_table()
    ensure_text_condition_queue_table()
    conn = schema.get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            schema.ensure_utc_session(cur)
            cur.execute(
                """
                WITH claimed AS (
                    SELECT
                        q.id AS queue_id,
                        q.priority_flag,
                        a.id AS listing_id,
                        a.external_id,
                        a.source,
                        a.title,
                        a.url,
                        COALESCE(a.model_key, 'unknown') AS model_key,
                        COALESCE(a.condition_grade, 'B') AS condition_grade,
                        a.status,
                        COALESCE(a.raw_attrs, '{}'::jsonb) AS raw_attrs
                    FROM listing_text_condition_queue AS q
                    JOIN auction_listings AS a
                      ON a.id = q.listing_id
                    LEFT JOIN latest_listing_text_conditions AS tc
                      ON tc.listing_id = a.id
                    WHERE q.status = 'pending'
                      AND q.attempts < %s
                      AND tc.id IS NULL
                      AND a.external_id IS NOT NULL
                      AND a.external_id <> ''
                      AND a.title IS NOT NULL
                      AND a.title <> ''
                    ORDER BY q.priority DESC, q.queued_at ASC, q.id ASC
                    LIMIT %s
                    FOR UPDATE OF q SKIP LOCKED
                )
                UPDATE listing_text_condition_queue AS q
                   SET status = 'processing',
                       attempts = q.attempts + 1,
                       locked_at = (now() AT TIME ZONE 'utc'),
                       last_error = NULL
                  FROM claimed
                 WHERE q.id = claimed.queue_id
                RETURNING
                    claimed.queue_id,
                    claimed.priority_flag,
                    claimed.listing_id,
                    claimed.external_id,
                    claimed.source,
                    claimed.title,
                    claimed.url,
                    claimed.model_key,
                    claimed.condition_grade,
                    claimed.status,
                    claimed.raw_attrs
                """,
                (max_attempts, max(limit, 1)),
            )
            rows = [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

    candidates: list[TextCandidate] = []
    for row in rows:
        raw_attrs = row.get("raw_attrs") if isinstance(row.get("raw_attrs"), dict) else {}
        candidates.append(
            TextCandidate(
                listing_id=int(row["listing_id"]),
                external_id=str(row.get("external_id") or ""),
                source=str(row.get("source") or ""),
                title=str(row.get("title") or ""),
                url=str(row.get("url") or ""),
                model_key=str(row.get("model_key") or ""),
                condition_grade=str(row.get("condition_grade") or "B"),
                status=str(row.get("status") or ""),
                raw_attrs=raw_attrs,
                priority_flag=str(row.get("priority_flag") or "backfill"),
                queue_id=int(row["queue_id"]) if row.get("queue_id") is not None else None,
            )
        )
    return candidates


def save_text_condition(candidate: TextCandidate, assessment: Mapping[str, Any], *, model: str) -> None:
    ensure_auction_listing_text_flags()
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            INSERT INTO listing_text_conditions (
                listing_id,
                external_id,
                model_name,
                assessment,
                text_grade,
                confidence,
                risk_flags,
                review_required,
                checked_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, (now() AT TIME ZONE 'utc'))
            """,
            (
                candidate.listing_id,
                candidate.external_id,
                model,
                Json(dict(assessment)),
                assessment.get("text_grade"),
                assessment.get("confidence"),
                list(assessment.get("risk_flags") or []),
                bool(assessment.get("review_required", True)),
            ),
        )
        cur.execute(
            """
            UPDATE auction_listings
               SET text_condition_checked = true,
                   text_condition_checked_at = (now() AT TIME ZONE 'utc')
             WHERE id = %s
            """,
            (candidate.listing_id,),
        )


def mark_text_queue_done(queue_id: int) -> None:
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            UPDATE listing_text_condition_queue
               SET status = 'done',
                   locked_at = NULL,
                   processed_at = (now() AT TIME ZONE 'utc'),
                   last_error = NULL
             WHERE id = %s
            """,
            (queue_id,),
        )


def mark_text_queue_error(queue_id: int, error: Exception, *, max_attempts: int) -> None:
    message = str(error)[:1000]
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            UPDATE listing_text_condition_queue
               SET status = CASE WHEN attempts >= %s THEN 'error' ELSE 'pending' END,
                   locked_at = NULL,
                   last_error = %s
             WHERE id = %s
            """,
            (max_attempts, message, queue_id),
        )


def process_queue_batch(
    *,
    model: str,
    batch_size: int,
    max_attempts: int,
) -> dict[str, Any]:
    candidates = claim_text_candidates(limit=batch_size, max_attempts=max_attempts)
    results: list[dict[str, Any]] = []
    saved = 0
    for candidate in candidates:
        try:
            assessment = assess_text(
                title=candidate.title,
                raw_attrs=candidate.raw_attrs,
                model_key=candidate.model_key,
                source=candidate.source,
                model=model,
            )
            save_text_condition(candidate, assessment, model=model)
            if candidate.queue_id is not None:
                mark_text_queue_done(candidate.queue_id)
            saved += 1
            results.append(
                {
                    **asdict(candidate),
                    "assessment": assessment,
                    "stored": True,
                }
            )
        except Exception as exc:
            if candidate.queue_id is not None:
                mark_text_queue_error(candidate.queue_id, exc, max_attempts=max_attempts)
            results.append(
                {
                    **asdict(candidate),
                    "error": str(exc),
                    "stored": False,
                }
            )

    return {
        "status": "ok",
        "mode": "queue-batch",
        "model": model,
        "claimed": len(candidates),
        "saved": saved,
        "results": results,
    }


def run_queue_worker(
    *,
    model: str,
    batch_size: int,
    enqueue_limit: int,
    poll_seconds: int,
    max_attempts: int,
    new_listing_minutes: int,
    source: str,
    product: str,
    status: str,
    once: bool = False,
) -> int:
    ensure_text_condition_table()
    ensure_text_condition_queue_table()

    while True:
        enqueued = enqueue_text_candidates(
            limit=enqueue_limit,
            source=source,
            product=product,
            status=status,
            max_attempts=max_attempts,
            new_listing_minutes=new_listing_minutes,
        )
        batch = process_queue_batch(
            model=model,
            batch_size=batch_size,
            max_attempts=max_attempts,
        )
        if int(batch.get("saved") or 0) > 0:
            run_comps(force=True)

        tick = {
            "status": "ok",
            "mode": "worker-tick",
            "model": model,
            "source": source or "all",
            "product": product or "all",
            "listing_status": status,
            "enqueued": enqueued,
            **batch,
        }
        print(json.dumps(tick, indent=2, default=str), flush=True)

        if once:
            return 0
        sleep_for = 1 if int(batch.get("claimed") or 0) > 0 else max(poll_seconds, 1)
        time.sleep(sleep_for)


def run_candidate_checks(
    *,
    limit: int,
    source: str,
    product: str,
    status: str,
    model: str,
    include_checked: bool,
    apply: bool,
    rebuild_comps: bool,
) -> dict[str, Any]:
    candidates = load_candidates(
        limit=limit,
        source=source,
        product=product,
        status=status,
        include_checked=include_checked,
    )
    results: list[dict[str, Any]] = []
    saved = 0
    for candidate in candidates:
        assessment = assess_text(
            title=candidate.title,
            raw_attrs=candidate.raw_attrs,
            model_key=candidate.model_key,
            source=candidate.source,
            model=model,
        )
        if apply:
            save_text_condition(candidate, assessment, model=model)
            saved += 1
        results.append(
            {
                **asdict(candidate),
                "assessment": assessment,
                "stored": apply,
            }
        )

    if apply and rebuild_comps and saved:
        run_comps(force=True)

    return {
        "status": "ok",
        "mode": "apply" if apply else "dry-run",
        "model": model,
        "count": len(results),
        "saved": saved,
        "results": results,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run local Ollama text condition checks.")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--source", default="")
    parser.add_argument("--product", default="")
    parser.add_argument("--status", default="live", choices=["live", "sold", "ended", "unknown", "all"])
    parser.add_argument("--include-checked", action="store_true")
    parser.add_argument("--apply", action="store_true", help="Persist text checks to local PostgreSQL.")
    parser.add_argument("--no-rebuild-comps", action="store_true", help="Skip rebuilding comps after stored checks.")
    parser.add_argument("--queue-worker", action="store_true", help="Run the always-on text condition queue worker.")
    parser.add_argument("--queue-once", action="store_true", help="Run one queue worker tick, then exit.")
    parser.add_argument("--queue-batch-size", type=int, default=4)
    parser.add_argument("--enqueue-limit", type=int, default=1000)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--new-listing-minutes", type=int, default=360)
    parser.add_argument("--title", default="", help="Listing title for a model smoke test.")
    parser.add_argument("--raw-attrs-json", default="", help="Raw attrs JSON for a model smoke test.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.title or args.raw_attrs_json:
        raw_attrs = json.loads(args.raw_attrs_json) if args.raw_attrs_json else {}
        result = assess_text(title=args.title, raw_attrs=raw_attrs, model=args.model)
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.queue_worker or args.queue_once:
        return run_queue_worker(
            model=args.model,
            batch_size=args.queue_batch_size,
            enqueue_limit=args.enqueue_limit,
            poll_seconds=args.poll_seconds,
            max_attempts=args.max_attempts,
            new_listing_minutes=args.new_listing_minutes,
            source=args.source,
            product=args.product,
            status=args.status,
            once=args.queue_once,
        )

    result = run_candidate_checks(
        limit=args.limit,
        source=args.source,
        product=args.product,
        status=args.status,
        model=args.model,
        include_checked=args.include_checked,
        apply=args.apply,
        rebuild_comps=not args.no_rebuild_comps,
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
