from __future__ import annotations

import json
from typing import Any, Optional

from utils.db_schema import get_connection, ensure_utc_session

HEARTBEAT_LOCK_NAME = "listing_heartbeat_pause"


def _ensure_table() -> None:
    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            ensure_utc_session(cur)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_runtime_flags (
                    name TEXT PRIMARY KEY,
                    is_active BOOLEAN NOT NULL DEFAULT FALSE,
                    payload JSONB,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
                )
                """
            )


def set_flag(name: str, is_active: bool, payload: Optional[dict[str, Any]] = None) -> None:
    _ensure_table()

    conn = get_connection()
    with conn:
        with conn.cursor() as cur:
            ensure_utc_session(cur)
            cur.execute(
                """
                INSERT INTO pipeline_runtime_flags (name, is_active, payload, updated_at)
                VALUES (%s, %s, %s::jsonb, (now() AT TIME ZONE 'utc'))
                ON CONFLICT (name) DO UPDATE
                SET is_active = EXCLUDED.is_active,
                    payload = EXCLUDED.payload,
                    updated_at = EXCLUDED.updated_at
                """,
                (name, is_active, json.dumps(payload or {})),
            )


def get_flag(name: str) -> Optional[dict[str, Any]]:
    _ensure_table()

    conn = get_connection()
    with conn.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            SELECT name, is_active, payload, updated_at
            FROM pipeline_runtime_flags
            WHERE name = %s
            LIMIT 1
            """,
            (name,),
        )
        row = cur.fetchone()

    if not row:
        return None

    return {
        "name": row[0],
        "is_active": bool(row[1]),
        "payload": row[2] or {},
        "updated_at": row[3],
    }


def is_heartbeat_paused() -> tuple[bool, Optional[dict[str, Any]]]:
    flag = get_flag(HEARTBEAT_LOCK_NAME)
    if not flag or not flag["is_active"]:
        return False, flag
    return True, flag


def pause_heartbeat(reason: str, extra: Optional[dict[str, Any]] = None) -> None:
    payload = {"reason": reason, **(extra or {})}
    set_flag(HEARTBEAT_LOCK_NAME, True, payload)


def resume_heartbeat() -> None:
    set_flag(HEARTBEAT_LOCK_NAME, False, {})
