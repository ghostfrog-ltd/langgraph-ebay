#!/usr/bin/env python3
"""Apply Python/PostgreSQL SQL migrations for the operational schema.

Migrations live in the repository-level `migrations/` directory and are applied
in filename order. Each file runs in its own transaction and is recorded in
`schema_migrations` with a SHA-256 checksum. This gives the Python/LangGraph
database a first-class migration path instead of relying on ad hoc runtime
`CREATE TABLE` and `ALTER TABLE` statements.
"""

from __future__ import annotations

import argparse
import hashlib
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import psycopg2
from psycopg2.extensions import connection as PgConnection

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - project requirements include dotenv.
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MIGRATIONS_DIR = PROJECT_ROOT / "migrations"


@dataclass(frozen=True)
class DbConfig:
    """Database settings loaded from the same environment as the pipelines."""

    database: str
    user: str
    password: str
    host: str
    port: str
    connect_timeout: int


@dataclass(frozen=True)
class Migration:
    """One SQL migration file and its immutable checksum."""

    name: str
    path: Path
    checksum: str
    sql_text: str


def load_db_config() -> DbConfig:
    """Load required database connection settings without printing secrets."""

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


def create_connection() -> PgConnection:
    """Create a direct psycopg2 connection without importing project logging."""

    cfg = load_db_config()
    return psycopg2.connect(
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
        host=cfg.host,
        port=cfg.port,
        connect_timeout=cfg.connect_timeout,
    )


def discover_migrations(migrations_dir: Path) -> list[Migration]:
    """Load SQL migration files in deterministic filename order."""

    if not migrations_dir.exists():
        raise RuntimeError(f"Migrations directory does not exist: {migrations_dir}")

    migrations: list[Migration] = []
    for path in sorted(migrations_dir.glob("*.sql")):
        sql_text = path.read_text(encoding="utf-8")
        checksum = hashlib.sha256(sql_text.encode("utf-8")).hexdigest()
        migrations.append(
            Migration(
                name=path.name,
                path=path,
                checksum=checksum,
                sql_text=sql_text,
            )
        )
    return migrations


def ensure_schema_migrations(conn: PgConnection) -> None:
    """Create the schema migration ledger if it does not already exist."""

    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                name TEXT PRIMARY KEY,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    conn.commit()


def applied_migrations(conn: PgConnection) -> dict[str, str]:
    """Return already-applied migration checksums keyed by filename."""

    with conn.cursor() as cur:
        cur.execute("SELECT name, checksum FROM schema_migrations ORDER BY name")
        return {str(name): str(checksum) for name, checksum in cur.fetchall()}


def pending_migrations(
    migrations: Iterable[Migration],
    applied: dict[str, str],
) -> list[Migration]:
    """Validate checksums and return migrations that still need to run."""

    pending: list[Migration] = []
    for migration in migrations:
        recorded_checksum = applied.get(migration.name)
        if recorded_checksum is None:
            pending.append(migration)
            continue
        if recorded_checksum != migration.checksum:
            raise RuntimeError(
                f"Migration checksum mismatch for {migration.name}: "
                f"database={recorded_checksum} file={migration.checksum}"
            )
    return pending


def apply_migration(conn: PgConnection, migration: Migration) -> None:
    """Apply one migration and record it atomically."""

    with conn.cursor() as cur:
        cur.execute("SET LOCAL statement_timeout = '60s'")
        cur.execute(migration.sql_text)
        cur.execute(
            """
            INSERT INTO schema_migrations (name, checksum, applied_at)
            VALUES (%s, %s, %s)
            """,
            (migration.name, migration.checksum, datetime.now(timezone.utc)),
        )
    conn.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=DEFAULT_MIGRATIONS_DIR,
        help="Directory containing .sql migration files.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List pending migrations without applying them.",
    )
    args = parser.parse_args()

    migrations = discover_migrations(args.migrations_dir)
    conn = create_connection()
    try:
        ensure_schema_migrations(conn)
        applied = applied_migrations(conn)
        pending = pending_migrations(migrations, applied)

        if args.dry_run:
            if pending:
                print("Pending migrations:")
                for migration in pending:
                    print(f"- {migration.name}")
            else:
                print("No pending migrations.")
            return 0

        if not pending:
            print("No pending migrations.")
            return 0

        for migration in pending:
            print(f"Applying {migration.name}...")
            try:
                apply_migration(conn, migration)
            except Exception:
                conn.rollback()
                raise
            print(f"Applied {migration.name}.")

        print(f"Applied {len(pending)} migration(s).")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
