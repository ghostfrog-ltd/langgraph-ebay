#!/usr/bin/env python3
"""Read-only PostgreSQL schema audit for the eBay opportunity system.

The audit intentionally avoids importing project logging helpers because those
can write outside the repository sandbox. It uses the same `.env` database
variables as the pipeline and forces the session into read-only mode before
running metadata and aggregate queries.
"""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg2
from psycopg2 import sql

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - project requirements include dotenv.
    load_dotenv = None


KEY_TABLES = (
    "auction_listings",
    "auction_price_history",
    "auction_price_history_orphans",
    "alerts",
    "listing_statuses",
    "listing_assessments",
    "paper_trade_predictions",
    "roi_snapshots",
    "sources",
    "comps",
)


@dataclass(frozen=True)
class DbConfig:
    """Connection settings loaded from the local environment."""

    database: str
    user: str
    password: str
    host: str
    port: str
    connect_timeout: int


def load_db_config() -> DbConfig:
    """Load database settings without printing secrets."""

    if load_dotenv is not None:
        load_dotenv()

    missing = [
        name
        for name in ("DB_NAME", "DB_USER", "DB_PASS", "DB_HOST", "DB_PORT")
        if not os.getenv(name)
    ]
    if missing:
        raise RuntimeError(f"Missing database environment variables: {', '.join(missing)}")

    return DbConfig(
        database=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASS"],
        host=os.environ["DB_HOST"],
        port=os.environ["DB_PORT"],
        connect_timeout=int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
    )


def json_safe(value: Any) -> Any:
    """Convert psycopg2 values into JSON-friendly values."""

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Path):
        return str(value)
    return str(value)


def fetch_all(cur, query: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    """Return query rows as dictionaries keyed by column name."""

    cur.execute(query, params)
    names = [desc[0] for desc in cur.description]
    return [dict(zip(names, row)) for row in cur.fetchall()]


def fetch_scalar(cur, query: sql.SQL | str, params: tuple[Any, ...] = ()) -> Any:
    """Return the first value from a query."""

    cur.execute(query, params)
    row = cur.fetchone()
    return row[0] if row else None


def table_exists(cur, table_name: str) -> bool:
    """Check for a public table or materialized view."""

    return bool(
        fetch_scalar(
            cur,
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public'
                  AND c.relname = %s
                  AND c.relkind IN ('r', 'm')
            )
            """,
            (table_name,),
        )
    )


def table_count(cur, table_name: str) -> int:
    """Count rows in a public table using safe identifier interpolation."""

    return int(
        fetch_scalar(
            cur,
            sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name)),
        )
        or 0
    )


def column_non_null_counts(cur, table_name: str) -> dict[str, int]:
    """Return total and per-column non-null counts for a table."""

    columns = fetch_all(
        cur,
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table_name,),
    )
    column_names = [row["column_name"] for row in columns]
    if not column_names:
        return {}

    pieces = [sql.SQL("COUNT(*) AS total")]
    for column in column_names:
        pieces.append(sql.SQL("COUNT({}) AS {}").format(sql.Identifier(column), sql.Identifier(column)))

    cur.execute(
        sql.SQL("SELECT {} FROM {}").format(
            sql.SQL(", ").join(pieces),
            sql.Identifier(table_name),
        )
    )
    names = [desc[0] for desc in cur.description]
    values = cur.fetchone()
    return {name: int(value or 0) for name, value in zip(names, values)}


def sparse_columns(non_null_counts: dict[str, dict[str, int]], threshold: float = 0.01) -> list[dict[str, Any]]:
    """Identify columns with no data or very sparse data."""

    sparse: list[dict[str, Any]] = []
    for table_name, counts in non_null_counts.items():
        total = int(counts.get("total") or 0)
        if total <= 0:
            continue
        for column, count in counts.items():
            if column == "total":
                continue
            ratio = count / total
            if count == 0 or ratio <= threshold:
                sparse.append(
                    {
                        "table": table_name,
                        "column": column,
                        "non_null": count,
                        "total": total,
                        "ratio": round(ratio, 6),
                    }
                )
    return sparse


def run_audit() -> dict[str, Any]:
    """Collect read-only schema health signals."""

    cfg = load_db_config()
    conn = psycopg2.connect(
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
        connect_timeout=cfg.connect_timeout,
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SET default_transaction_read_only = on")
            cur.execute("SET statement_timeout = '30s'")

            tables = fetch_all(
                cur,
                """
                SELECT table_name, table_type
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name
                """,
            )
            matviews = fetch_all(
                cur,
                """
                SELECT matviewname, definition
                FROM pg_matviews
                WHERE schemaname = 'public'
                ORDER BY matviewname
                """,
            )
            views = fetch_all(
                cur,
                """
                SELECT table_name AS view_name
                FROM information_schema.views
                WHERE table_schema = 'public'
                ORDER BY table_name
                """,
            )
            sizes = fetch_all(
                cur,
                """
                SELECT
                    relname AS name,
                    pg_size_pretty(pg_total_relation_size(oid)) AS total_size,
                    pg_total_relation_size(oid) AS bytes
                FROM pg_class
                WHERE relnamespace = 'public'::regnamespace
                  AND relkind IN ('r', 'm')
                ORDER BY pg_total_relation_size(oid) DESC
                """,
            )
            foreign_keys = fetch_all(
                cur,
                """
                SELECT
                    conrelid::regclass::text AS table_name,
                    conname AS constraint_name,
                    confrelid::regclass::text AS referenced_table,
                    convalidated AS validated
                FROM pg_constraint
                WHERE contype = 'f'
                  AND connamespace = 'public'::regnamespace
                ORDER BY table_name, constraint_name
                """,
            )

            row_counts = {
                row["table_name"]: table_count(cur, row["table_name"])
                for row in tables
                if row["table_type"] == "BASE TABLE"
            }
            view_counts: dict[str, int] = {}
            for row in views:
                view_name = str(row["view_name"])
                if view_name.startswith("listing_"):
                    view_counts[view_name] = table_count(cur, view_name)

            existing_key_tables = [table for table in KEY_TABLES if table_exists(cur, table)]
            non_null = {
                table_name: column_non_null_counts(cur, table_name)
                for table_name in existing_key_tables
            }

            integrity: dict[str, Any] = {}
            if table_exists(cur, "auction_listings") and table_exists(cur, "sources"):
                integrity["auction_source_id_null"] = fetch_scalar(
                    cur,
                    "SELECT COUNT(*) FROM auction_listings WHERE source_id IS NULL",
                )
                integrity["auction_bad_source_id_refs"] = fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM auction_listings l
                    LEFT JOIN sources s ON l.source_id = s.id
                    WHERE l.source_id IS NOT NULL
                      AND s.id IS NULL
                    """,
                )
                integrity["bad_source_id_groups"] = fetch_all(
                    cur,
                    """
                    SELECT source_id, source, COUNT(*) AS rows
                    FROM auction_listings l
                    LEFT JOIN sources s ON l.source_id = s.id
                    WHERE l.source_id IS NOT NULL
                      AND s.id IS NULL
                    GROUP BY source_id, source
                    ORDER BY rows DESC
                    LIMIT 20
                    """,
                )
                source_label_drift = fetch_all(
                    cur,
                    """
                    WITH expected AS (
                        SELECT
                            l.id,
                            l.source,
                            l.source_id,
                            s_current.name AS current_source_name,
                            s_expected.id AS expected_source_id,
                            s_expected.name AS expected_source_name,
                            COUNT(s_expected.id) OVER (PARTITION BY l.id) AS match_count
                        FROM auction_listings AS l
                        LEFT JOIN sources AS s_current
                          ON s_current.id = l.source_id
                        LEFT JOIN sources AS s_expected
                          ON s_expected.name = l.source
                          OR s_expected.domain = l.source
                    )
                    SELECT
                        source,
                        source_id,
                        current_source_name,
                        expected_source_id,
                        expected_source_name,
                        COUNT(*) AS rows
                    FROM expected
                    WHERE match_count = 1
                      AND source_id IS DISTINCT FROM expected_source_id
                    GROUP BY
                        source,
                        source_id,
                        current_source_name,
                        expected_source_id,
                        expected_source_name
                    ORDER BY rows DESC, source
                    LIMIT 20
                    """,
                )
                integrity["auction_source_label_id_mismatches"] = sum(
                    int(row["rows"] or 0) for row in source_label_drift
                )
                integrity["source_label_id_mismatch_groups"] = source_label_drift

            if table_exists(cur, "listing_assessments") and table_exists(cur, "auction_listings"):
                integrity["assessment_bad_listing_refs"] = fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM listing_assessments a
                    LEFT JOIN auction_listings l ON a.listing_id = l.id
                    WHERE l.id IS NULL
                    """,
                )

            if table_exists(cur, "auction_price_history") and table_exists(cur, "auction_listings"):
                integrity["price_history_orphans"] = fetch_scalar(
                    cur,
                    """
                    SELECT COUNT(*)
                    FROM auction_price_history ph
                    LEFT JOIN auction_listings l ON ph.external_id = l.external_id
                    WHERE l.external_id IS NULL
                    """,
                )
                if table_exists(cur, "auction_price_history_orphans"):
                    integrity["archived_price_history_orphans"] = fetch_scalar(
                        cur,
                        "SELECT COUNT(*) FROM auction_price_history_orphans",
                    )

            listing_status_counts: list[dict[str, Any]] = []
            final_price_by_status: list[dict[str, Any]] = []
            if table_exists(cur, "auction_listings"):
                listing_status_counts = fetch_all(
                    cur,
                    """
                    SELECT status, COUNT(*) AS rows
                    FROM auction_listings
                    GROUP BY status
                    ORDER BY rows DESC
                    """,
                )
                final_price_by_status = fetch_all(
                    cur,
                    """
                    SELECT
                        status,
                        COUNT(*) FILTER (WHERE final_price IS NOT NULL) AS final_price_rows,
                        COUNT(*) AS rows
                    FROM auction_listings
                    GROUP BY status
                    ORDER BY status
                    """,
                )

            return {
                "tables": tables,
                "matviews": matviews,
                "views": views,
                "sizes": sizes,
                "foreign_keys": foreign_keys,
                "row_counts": row_counts,
                "view_counts": view_counts,
                "non_null_counts": non_null,
                "sparse_columns": sparse_columns(non_null),
                "integrity": integrity,
                "listing_status_counts": listing_status_counts,
                "final_price_by_status": final_price_by_status,
            }
    finally:
        conn.close()


def print_markdown(report: dict[str, Any]) -> None:
    """Render a compact operator-facing audit report."""

    print("# Schema Audit")
    print()
    print("Read-only PostgreSQL schema and data-shape checks.")
    print()

    print("## Row Counts")
    for table_name, count in sorted(report["row_counts"].items(), key=lambda item: item[0]):
        print(f"- `{table_name}`: {count:,}")
    print()

    if report.get("views"):
        print("## Views")
        for row in report["views"]:
            count = (report.get("view_counts") or {}).get(row["view_name"])
            if count is None:
                print(f"- `{row['view_name']}`")
            else:
                print(f"- `{row['view_name']}`: {count:,}")
        print()

    print("## Largest Relations")
    for row in report["sizes"][:10]:
        print(f"- `{row['name']}`: {row['total_size']}")
    print()

    print("## Integrity Signals")
    integrity = report.get("integrity") or {}
    for key, value in integrity.items():
        if key in {"bad_source_id_groups", "source_label_id_mismatch_groups"}:
            continue
        print(f"- `{key}`: {value:,}" if isinstance(value, int) else f"- `{key}`: {value}")
    for row in integrity.get("bad_source_id_groups") or []:
        print(
            "- bad source group: "
            f"source=`{row['source']}`, source_id=`{row['source_id']}`, rows={row['rows']:,}"
        )
    for row in integrity.get("source_label_id_mismatch_groups") or []:
        print(
            "- source/source_id mismatch: "
            f"source=`{row['source']}`, source_id=`{row['source_id']}`, "
            f"current=`{row['current_source_name']}`, "
            f"expected_id=`{row['expected_source_id']}`, "
            f"expected=`{row['expected_source_name']}`, rows={row['rows']:,}"
        )
    print()

    if report.get("foreign_keys"):
        print("## Foreign Keys")
        for row in report["foreign_keys"]:
            validated = "validated" if row["validated"] else "not validated"
            print(
                f"- `{row['table_name']}.{row['constraint_name']}` -> "
                f"`{row['referenced_table']}` ({validated})"
            )
        print()

    print("## Sparse Columns")
    for row in report["sparse_columns"][:60]:
        print(
            f"- `{row['table']}.{row['column']}`: "
            f"{row['non_null']:,}/{row['total']:,} populated"
        )
    print()

    if report.get("listing_status_counts"):
        print("## Listing Status Counts")
        for row in report["listing_status_counts"]:
            print(f"- `{row['status']}`: {row['rows']:,}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--json", action="store_true", help="Emit full JSON report.")
    mode.add_argument("--markdown", action="store_true", help="Emit compact Markdown report.")
    args = parser.parse_args()

    report = run_audit()
    if args.json:
        print(json.dumps(report, default=json_safe, indent=2, sort_keys=True))
    else:
        print_markdown(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
