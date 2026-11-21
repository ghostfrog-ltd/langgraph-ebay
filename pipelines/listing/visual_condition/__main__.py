from __future__ import annotations

import argparse
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Mapping

from psycopg2.extras import Json, RealDictCursor

from pipelines.listing.market_discovery.graph import (
    FEE_RATE,
    INBOUND_SHIP_GBP,
    OUTBOUND_SHIP_GBP,
    PACKAGING_GBP,
    SELLING_RESERVE_GBP,
    effective_min_comps_for_source,
)
from pipelines.listing.review_actions import ensure_review_table
from pipelines.listing.visual_condition.ollama_client import (
    DEFAULT_MODEL,
    assess_images,
    image_urls_from_attrs,
)
from utils import db_schema as schema


@dataclass
class VisualCandidate:
    listing_id: int
    external_id: str
    source: str
    title: str
    url: str
    model_key: str
    condition_grade: str
    price_current: float
    median_final_price: float
    comp_samples: int
    expected_profit: float
    image_urls: list[str]
    priority_flag: str = "backfill"
    queue_id: int | None = None


def _expected_profit(current_price: float, sold_median: float) -> float:
    return round(
        sold_median * (1.0 - FEE_RATE)
        - OUTBOUND_SHIP_GBP
        - INBOUND_SHIP_GBP
        - PACKAGING_GBP
        - SELLING_RESERVE_GBP
        - current_price,
        2,
    )


def ensure_auction_listing_visual_flags() -> None:
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = 'auction_listings'
              AND column_name IN ('visual_condition_checked', 'visual_condition_checked_at')
            """
        )
        found = {str(row[0]) for row in cur.fetchall()}
    missing = {"visual_condition_checked", "visual_condition_checked_at"} - found
    if missing:
        raise RuntimeError(
            "Missing auction_listings visual condition columns; run migrations before starting visual condition worker: "
            + ", ".join(sorted(missing))
        )


def ensure_visual_condition_table() -> None:
    ensure_auction_listing_visual_flags()
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_visual_conditions (
                id BIGSERIAL PRIMARY KEY,
                listing_id BIGINT NOT NULL REFERENCES auction_listings(id) ON DELETE CASCADE,
                external_id TEXT NOT NULL,
                model_name TEXT NOT NULL,
                image_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
                assessment JSONB NOT NULL,
                visual_grade TEXT,
                confidence NUMERIC,
                damage_flags TEXT[] NOT NULL DEFAULT '{}'::text[],
                review_required BOOLEAN NOT NULL DEFAULT TRUE,
                checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_visual_conditions_listing_latest_idx
                ON listing_visual_conditions (listing_id, checked_at DESC, id DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_visual_conditions_external_latest_idx
                ON listing_visual_conditions (external_id, checked_at DESC, id DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_visual_conditions_review_idx
                ON listing_visual_conditions (review_required, checked_at DESC)
            """
        )
        cur.execute("SELECT to_regclass('latest_listing_visual_conditions') IS NOT NULL")
        if not cur.fetchone()[0]:
            raise RuntimeError(
                "Missing latest_listing_visual_conditions view; run migrations before starting visual condition worker."
            )


def ensure_visual_condition_queue_table() -> None:
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_visual_condition_queue (
                id BIGSERIAL PRIMARY KEY,
                listing_id BIGINT NOT NULL REFERENCES auction_listings(id) ON DELETE CASCADE,
                external_id TEXT NOT NULL,
                source TEXT NOT NULL,
                product TEXT NOT NULL,
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
            ALTER TABLE listing_visual_condition_queue
                ADD COLUMN IF NOT EXISTS priority_flag TEXT NOT NULL DEFAULT 'backfill'
            """
        )
        cur.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS listing_visual_condition_queue_listing_idx
                ON listing_visual_condition_queue (listing_id)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_visual_condition_queue_pending_idx
                ON listing_visual_condition_queue (status, priority DESC, queued_at ASC)
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


def enqueue_visual_candidates(
    *,
    source: str,
    product: str,
    limit: int = 250,
    max_attempts: int = 3,
    new_listing_minutes: int = 360,
) -> int:
    ensure_visual_condition_table()
    ensure_visual_condition_queue_table()
    product_predicate, product_params = _product_predicate(product)
    source_predicate = "AND a.source = %s" if source and source.lower() != "all" else ""
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            UPDATE listing_visual_condition_queue
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
            INSERT INTO listing_visual_condition_queue (
                listing_id,
                external_id,
                source,
                product,
                priority_flag,
                priority,
                status,
                queued_at
            )
            SELECT
                a.id,
                a.external_id,
                a.source,
                %s,
                CASE
                    WHEN a.first_seen IS NOT NULL
                         AND a.first_seen >= ((now() AT TIME ZONE 'utc') - (%s * interval '1 minute'))
                        THEN 'new_listing'
                    ELSE 'backfill'
                END,
                CASE
                    WHEN a.first_seen IS NOT NULL
                         AND a.first_seen >= ((now() AT TIME ZONE 'utc') - (%s * interval '1 minute'))
                        THEN 2000000000000
                    ELSE 0
                END + COALESCE(EXTRACT(EPOCH FROM a.first_seen)::bigint, EXTRACT(EPOCH FROM a.last_seen_at)::bigint, 0),
                'pending',
                (now() AT TIME ZONE 'utc')
            FROM auction_listings AS a
            WHERE a.status = 'live'
              {source_predicate}
              {product_predicate}
              AND a.raw_attrs IS NOT NULL
              AND a.raw_attrs ? 'Browse imageUrls'
              AND a.external_id IS NOT NULL
              AND a.external_id <> ''
              AND a.title IS NOT NULL
              AND a.title <> ''
              AND a.visual_condition_checked = false
              AND NOT EXISTS (
                  SELECT 1
                    FROM listing_visual_conditions vc
                   WHERE vc.listing_id = a.id
              )
            ORDER BY a.first_seen DESC NULLS LAST, a.last_seen_at DESC NULLS LAST, a.end_time ASC NULLS LAST
            LIMIT %s
            ON CONFLICT (listing_id) DO UPDATE
                SET priority = GREATEST(listing_visual_condition_queue.priority, EXCLUDED.priority),
                    priority_flag = CASE
                        WHEN EXCLUDED.priority > listing_visual_condition_queue.priority
                            THEN EXCLUDED.priority_flag
                        ELSE listing_visual_condition_queue.priority_flag
                    END,
                    source = EXCLUDED.source,
                    product = EXCLUDED.product,
                    status = CASE
                        WHEN listing_visual_condition_queue.status = 'error'
                             AND listing_visual_condition_queue.attempts < %s
                            THEN 'pending'
                        ELSE listing_visual_condition_queue.status
                    END
            RETURNING id
            """,
            [
                product,
                max(new_listing_minutes, 1),
                max(new_listing_minutes, 1),
                *([source] if source and source.lower() != "all" else []),
                *product_params,
                max(limit, 1),
                max_attempts,
            ],
        )
        return len(cur.fetchall())


def claim_visual_candidates(*, limit: int, max_attempts: int = 3) -> list[VisualCandidate]:
    ensure_visual_condition_table()
    ensure_visual_condition_queue_table()
    conn = schema.get_connection()
    try:
        with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
            schema.ensure_utc_session(cur)
            cur.execute(
                """
                WITH claimed AS (
                    SELECT
                        q.id AS queue_id,
                        a.id AS listing_id,
                        a.external_id,
                        a.source,
                        q.priority_flag,
                        a.title,
                        a.url,
                        COALESCE(a.model_key, 'unknown') AS model_key,
                        COALESCE(a.condition_grade, 'B') AS condition_grade,
                        COALESCE(a.price_current, 0) AS price_current,
                        a.raw_attrs,
                        COALESCE(lc.median_final_price, 0) AS median_final_price,
                        COALESCE(lc.samples, 0) AS comp_samples
                    FROM listing_visual_condition_queue AS q
                    JOIN auction_listings AS a
                      ON a.id = q.listing_id
                    LEFT JOIN latest_listing_text_conditions AS tc
                      ON tc.listing_id = a.id
                    CROSS JOIN LATERAL (
                        SELECT CASE
                            WHEN 'D' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, ''))) THEN 'D'
                            WHEN 'C' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, ''))) THEN 'C'
                            WHEN 'B' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, ''))) THEN 'B'
                            ELSE 'A'
                        END AS effective_condition_grade
                    ) AS grade
                    LEFT JOIN latest_comps AS lc
                      ON lc.model_key = a.model_key
                     AND lc.condition_grade = grade.effective_condition_grade
                    WHERE q.status = 'pending'
                      AND q.attempts < %s
                      AND a.status = 'live'
                      AND a.visual_condition_checked = false
                      AND a.raw_attrs IS NOT NULL
                      AND a.raw_attrs ? 'Browse imageUrls'
                      AND NOT EXISTS (
                          SELECT 1
                            FROM listing_visual_conditions vc
                           WHERE vc.listing_id = a.id
                      )
                    ORDER BY q.priority DESC, q.queued_at ASC, q.id ASC
                    LIMIT %s
                    FOR UPDATE OF q SKIP LOCKED
                )
                UPDATE listing_visual_condition_queue AS q
                   SET status = 'processing',
                       attempts = q.attempts + 1,
                       locked_at = (now() AT TIME ZONE 'utc'),
                       last_error = NULL
                  FROM claimed
                 WHERE q.id = claimed.queue_id
                RETURNING
                    claimed.queue_id,
                    claimed.listing_id,
                    claimed.external_id,
                    claimed.source,
                    claimed.priority_flag,
                    claimed.title,
                    claimed.url,
                    claimed.model_key,
                    claimed.condition_grade,
                    claimed.price_current,
                    claimed.raw_attrs,
                    claimed.median_final_price,
                    claimed.comp_samples
                """,
                (max_attempts, max(limit, 1)),
            )
            rows = [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

    candidates: list[VisualCandidate] = []
    for row in rows:
        raw_attrs = row.get("raw_attrs") if isinstance(row.get("raw_attrs"), Mapping) else {}
        image_urls = image_urls_from_attrs(raw_attrs)
        if not image_urls:
            mark_visual_queue_error(
                int(row["queue_id"]),
                "no valid image urls",
                max_attempts=max_attempts,
            )
            continue
        current = float(row.get("price_current") or 0)
        median = float(row.get("median_final_price") or 0)
        candidates.append(
            VisualCandidate(
                listing_id=int(row["listing_id"]),
                external_id=str(row.get("external_id") or ""),
                source=str(row.get("source") or ""),
                priority_flag=str(row.get("priority_flag") or "backfill"),
                title=str(row.get("title") or ""),
                url=str(row.get("url") or ""),
                model_key=str(row.get("model_key") or ""),
                condition_grade=str(row.get("condition_grade") or "B"),
                price_current=current,
                median_final_price=median,
                comp_samples=int(row.get("comp_samples") or 0),
                expected_profit=_expected_profit(current, median),
                image_urls=image_urls,
                queue_id=int(row["queue_id"]),
            )
        )
    return candidates


def mark_visual_queue_done(queue_id: int) -> None:
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            UPDATE listing_visual_condition_queue
               SET status = 'done',
                   locked_at = NULL,
                   processed_at = (now() AT TIME ZONE 'utc')
             WHERE id = %s
            """,
            (queue_id,),
        )


def mark_visual_queue_error(queue_id: int, error: Exception | str, *, max_attempts: int = 3) -> None:
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            UPDATE listing_visual_condition_queue
               SET status = CASE WHEN attempts >= %s THEN 'error' ELSE 'pending' END,
                   locked_at = NULL,
                   last_error = %s
             WHERE id = %s
            """,
            (max_attempts, str(error)[:1000], queue_id),
        )


def release_processing_locks(*, max_attempts: int = 3) -> int:
    ensure_visual_condition_queue_table()
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            UPDATE listing_visual_condition_queue
               SET status = 'pending',
                   locked_at = NULL,
                   last_error = 'worker restart released lock'
             WHERE status = 'processing'
               AND attempts < %s
            RETURNING id
            """,
            (max_attempts,),
        )
        return len(cur.fetchall())


def sync_listing_visual_checked_flags() -> int:
    ensure_visual_condition_table()
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            UPDATE auction_listings AS a
               SET visual_condition_checked = true,
                   visual_condition_checked_at = COALESCE(a.visual_condition_checked_at, vc.checked_at)
              FROM latest_listing_visual_conditions AS vc
             WHERE vc.listing_id = a.id
               AND a.visual_condition_checked = false
            RETURNING a.id
            """
        )
        return len(cur.fetchall())


def load_candidates(
    *,
    limit: int,
    source: str,
    product: str,
    min_profit: float,
    min_comps: int | None,
    include_checked: bool,
    all_live: bool = False,
) -> list[VisualCandidate]:
    ensure_review_table()
    ensure_visual_condition_table()

    product = product.strip().lower()
    params: list[Any] = [source]
    product_predicate = ""
    if product == "iphone":
        product_predicate = "AND (a.title ILIKE %s OR a.model_key ILIKE %s)"
        params.extend(["%iphone%", "apple-iphone%"])
    elif product:
        product_predicate = "AND (a.title ILIKE %s OR a.model_key ILIKE %s)"
        params.extend([f"%{product}%", f"%{product}%"])

    checked_predicate = ""
    if not include_checked:
        checked_predicate = """
          AND NOT EXISTS (
              SELECT 1
                FROM listing_visual_conditions vc
               WHERE vc.listing_id = a.id
          )
        """
    if all_live:
        order_clause = """
                    a.first_seen DESC NULLS LAST,
                    a.last_seen_at DESC NULLS LAST,
                    a.end_time ASC NULLS LAST
        """
    else:
        order_clause = """
                    (
                        lc.median_final_price * %s
                        - %s - %s - %s - %s
                        - COALESCE(a.price_current, 0)
                    ) DESC,
                    a.end_time ASC NULLS LAST
        """

    query_params = [
        *params,
    ]
    if not all_live:
        query_params.extend(
            [
                1.0 - FEE_RATE,
                OUTBOUND_SHIP_GBP,
                INBOUND_SHIP_GBP,
                PACKAGING_GBP,
                SELLING_RESERVE_GBP,
            ]
        )
    query_params.append(limit)
    conn = schema.get_fresh_connection()
    try:
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
                    COALESCE(a.price_current, 0) AS price_current,
                    a.raw_attrs,
                    COALESCE(lc.median_final_price, 0) AS median_final_price,
                    COALESCE(lc.samples, 0) AS comp_samples
                FROM auction_listings AS a
                LEFT JOIN latest_listing_text_conditions AS tc
                  ON tc.listing_id = a.id
                CROSS JOIN LATERAL (
                    SELECT CASE
                        WHEN 'D' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, ''))) THEN 'D'
                        WHEN 'C' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, ''))) THEN 'C'
                        WHEN 'B' IN (UPPER(COALESCE(a.condition_grade, 'B')), UPPER(COALESCE(tc.text_grade, ''))) THEN 'B'
                        ELSE 'A'
                    END AS effective_condition_grade
                ) AS grade
                LEFT JOIN latest_comps AS lc
                  ON lc.model_key = a.model_key
                 AND lc.condition_grade = grade.effective_condition_grade
                WHERE a.status = 'live'
                  AND a.source = %s
                  {product_predicate}
                  AND a.raw_attrs IS NOT NULL
                  AND a.raw_attrs ? 'Browse imageUrls'
                  AND a.price_current IS NOT NULL
                  AND a.price_current > 0
                  AND a.external_id IS NOT NULL
                  AND a.external_id <> ''
                  AND a.title IS NOT NULL
                  AND a.title <> ''
                  AND NOT EXISTS (
                      SELECT 1
                      FROM (
                          SELECT DISTINCT ON (external_id)
                              external_id,
                              action
                          FROM listing_review_actions
                          ORDER BY external_id, created_at DESC, id DESC
                      ) AS review
                      WHERE review.external_id = a.external_id
                        AND review.action = 'down'
                  )
                  {checked_predicate}
                ORDER BY
                  {order_clause}
                LIMIT %s
                """,
                query_params,
            )
            rows = [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()

    candidates: list[VisualCandidate] = []
    for row in rows:
        comp_samples = int(row.get("comp_samples") or 0)
        current = float(row.get("price_current") or 0)
        median = float(row.get("median_final_price") or 0)
        profit = _expected_profit(current, median)
        if not all_live:
            floor = effective_min_comps_for_source(row.get("source"), override=min_comps)
            if comp_samples < floor:
                continue
            if profit < min_profit:
                continue

        raw_attrs = row.get("raw_attrs") if isinstance(row.get("raw_attrs"), Mapping) else {}
        image_urls = image_urls_from_attrs(raw_attrs)
        if not image_urls:
            continue

        candidates.append(
            VisualCandidate(
                listing_id=int(row["listing_id"]),
                external_id=str(row.get("external_id") or ""),
                source=str(row.get("source") or ""),
                title=str(row.get("title") or ""),
                url=str(row.get("url") or ""),
                model_key=str(row.get("model_key") or ""),
                condition_grade=str(row.get("condition_grade") or "B"),
                price_current=current,
                median_final_price=median,
                comp_samples=comp_samples,
                expected_profit=profit,
                image_urls=image_urls,
            )
        )
    return candidates[:limit]


def save_visual_condition(candidate: VisualCandidate, assessment: Mapping[str, Any], *, model: str) -> None:
    ensure_auction_listing_visual_flags()
    conn = schema.get_connection()
    with conn, conn.cursor() as cur:
        schema.ensure_utc_session(cur)
        cur.execute(
            """
            INSERT INTO listing_visual_conditions (
                listing_id,
                external_id,
                model_name,
                image_urls,
                assessment,
                visual_grade,
                confidence,
                damage_flags,
                review_required,
                checked_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, (now() AT TIME ZONE 'utc'))
            """,
            (
                candidate.listing_id,
                candidate.external_id,
                model,
                Json(candidate.image_urls),
                Json(dict(assessment)),
                assessment.get("visual_grade"),
                assessment.get("confidence"),
                list(assessment.get("damage_flags") or []),
                bool(assessment.get("review_required", True)),
            ),
        )
        cur.execute(
            """
            UPDATE auction_listings
               SET visual_condition_checked = true,
                   visual_condition_checked_at = (now() AT TIME ZONE 'utc')
             WHERE id = %s
            """,
            (candidate.listing_id,),
        )


def run_candidate_checks(
    *,
    limit: int,
    source: str,
    product: str,
    model: str,
    min_profit: float,
    min_comps: int | None,
    include_checked: bool,
    all_live: bool,
    max_images: int,
    apply: bool,
) -> dict[str, Any]:
    candidates = load_candidates(
        limit=limit,
        source=source,
        product=product,
        min_profit=min_profit,
        min_comps=min_comps,
        include_checked=include_checked,
        all_live=all_live,
    )
    results: list[dict[str, Any]] = []
    for candidate in candidates:
        assessment = assess_images(
            title=candidate.title,
            image_urls=candidate.image_urls,
            model=model,
            max_images=max_images,
        )
        if apply:
            save_visual_condition(candidate, assessment, model=model)
        results.append(
            {
                **asdict(candidate),
                "image_urls": candidate.image_urls[:max_images],
                "assessment": assessment,
                "stored": apply,
            }
        )
    return {
        "status": "ok",
        "mode": "apply" if apply else "dry-run",
        "model": model,
        "count": len(results),
        "results": results,
    }


def parse_targets(values: list[str]) -> list[tuple[str, str]]:
    raw_targets = values or ["all:all"]
    targets: list[tuple[str, str]] = []
    for value in raw_targets:
        if ":" not in value:
            raise SystemExit(f"Invalid target {value!r}; expected source:product")
        source, product = value.split(":", 1)
        source = source.strip()
        product = product.strip()
        if not source or not product:
            raise SystemExit(f"Invalid target {value!r}; expected source:product")
        if source.lower() == "all":
            source = ""
        if product.lower() == "all":
            product = ""
        targets.append((source, product))
    return targets


def process_queue_batch(
    *,
    model: str,
    max_images: int,
    batch_size: int,
    max_attempts: int,
) -> dict[str, Any]:
    candidates = claim_visual_candidates(limit=batch_size, max_attempts=max_attempts)
    results: list[dict[str, Any]] = []
    saved = 0
    for candidate in candidates:
        try:
            assessment = assess_images(
                title=candidate.title,
                image_urls=candidate.image_urls,
                model=model,
                max_images=max_images,
            )
            save_visual_condition(candidate, assessment, model=model)
            if candidate.queue_id is not None:
                mark_visual_queue_done(candidate.queue_id)
            saved += 1
            results.append(
                {
                    **asdict(candidate),
                    "image_urls": candidate.image_urls[:max_images],
                    "assessment": assessment,
                    "stored": True,
                }
            )
        except Exception as exc:
            if candidate.queue_id is not None:
                mark_visual_queue_error(candidate.queue_id, exc, max_attempts=max_attempts)
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


def rebuild_comps_after_visual_change() -> None:
    from pipelines.listing.comps.graph import run as run_comps

    run_comps(force=True)


def run_queue_worker(
    *,
    targets: list[tuple[str, str]],
    model: str,
    max_images: int,
    batch_size: int,
    enqueue_limit: int,
    poll_seconds: int,
    max_attempts: int,
    new_listing_minutes: int,
    once: bool = False,
) -> int:
    ensure_visual_condition_table()
    ensure_visual_condition_queue_table()

    while True:
        enqueued = 0
        for source, product in targets:
            enqueued += enqueue_visual_candidates(
                source=source,
                product=product,
                limit=enqueue_limit,
                max_attempts=max_attempts,
                new_listing_minutes=new_listing_minutes,
            )

        batch = process_queue_batch(
            model=model,
            max_images=max_images,
            batch_size=batch_size,
            max_attempts=max_attempts,
        )
        if int(batch.get("saved") or 0) > 0:
            rebuild_comps_after_visual_change()

        tick = {
            "status": "ok",
            "mode": "worker-tick",
            "model": model,
            "targets": [f"{source}:{product}" for source, product in targets],
            "enqueued": enqueued,
            **batch,
        }
        print(json.dumps(tick, indent=2, default=str), flush=True)

        if once:
            return 0
        sleep_for = max(poll_seconds, 1)
        if int(batch.get("claimed") or 0) > 0:
            sleep_for = 1
        time.sleep(sleep_for)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run local Ollama visual condition checks.")
    parser.add_argument("--model", default=DEFAULT_MODEL)
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--source", default="ebay-apple")
    parser.add_argument("--product", default="iphone")
    parser.add_argument("--min-profit", type=float, default=25.0)
    parser.add_argument("--min-comps", type=int, default=None)
    parser.add_argument("--include-checked", action="store_true")
    parser.add_argument("--all-live", action="store_true", help="Grade live image-backed rows even before comps/profit evidence exists.")
    parser.add_argument("--max-images", type=int, default=6)
    parser.add_argument("--apply", action="store_true", help="Persist visual checks to local PostgreSQL.")
    parser.add_argument("--image", action="append", default=[], help="Local image path for model smoke tests.")
    parser.add_argument("--title", default="Apple iPhone listing")
    parser.add_argument("--queue-worker", action="store_true", help="Run the always-on visual grading queue worker.")
    parser.add_argument("--queue-once", action="store_true", help="Run one queue worker tick, then exit.")
    parser.add_argument("--release-processing-locks", action="store_true", help="Move in-flight queue rows back to pending after a worker restart.")
    parser.add_argument("--sync-listing-flags", action="store_true", help="Resync auction_listings visual checked flags from latest visual rows.")
    parser.add_argument("--target", action="append", default=[], help="Queue target as source:product. Repeat for multiple targets.")
    parser.add_argument("--queue-batch-size", type=int, default=4)
    parser.add_argument("--enqueue-limit", type=int, default=250)
    parser.add_argument("--poll-seconds", type=int, default=10)
    parser.add_argument("--max-attempts", type=int, default=3)
    parser.add_argument("--new-listing-minutes", type=int, default=360)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.image:
        result = assess_images(
            title=args.title,
            image_paths=[Path(path) for path in args.image],
            model=args.model,
            max_images=max(args.max_images, 1),
        )
        print(json.dumps(result, indent=2, default=str))
        return 0

    if args.queue_worker or args.queue_once:
        return run_queue_worker(
            targets=parse_targets(args.target),
            model=args.model,
            max_images=max(args.max_images, 1),
            batch_size=max(args.queue_batch_size, 1),
            enqueue_limit=max(args.enqueue_limit, 1),
            poll_seconds=max(args.poll_seconds, 1),
            max_attempts=max(args.max_attempts, 1),
            new_listing_minutes=max(args.new_listing_minutes, 1),
            once=bool(args.queue_once),
        )

    if args.release_processing_locks or args.sync_listing_flags:
        released = release_processing_locks(max_attempts=max(args.max_attempts, 1)) if args.release_processing_locks else 0
        synced = sync_listing_visual_checked_flags() if args.sync_listing_flags else 0
        print(json.dumps({"status": "ok", "released_processing_locks": released, "synced_listing_flags": synced}, indent=2))
        return 0

    result = run_candidate_checks(
        limit=max(args.limit, 1),
        source=args.source,
        product=args.product,
        model=args.model,
        min_profit=max(args.min_profit, 0.0),
        min_comps=args.min_comps,
        include_checked=bool(args.include_checked),
        all_live=bool(args.all_live),
        max_images=max(args.max_images, 1),
        apply=bool(args.apply),
    )
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
