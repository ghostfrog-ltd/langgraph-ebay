from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from utils.logger import get_logger
from utils.db_schema import connection
from pipelines.listing.assess.graph import run

logger = get_logger(__name__)

# Arbitrary 64-bit key; just keep it unique-ish for this purpose
ASSESS_LOCK_KEY = 987654321


def _try_acquire_lock() -> bool:
    with connection.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_lock(%s)", (ASSESS_LOCK_KEY,))
        row = cur.fetchone()
    connection.commit()
    return bool(row[0])


def _release_lock() -> None:
    with connection.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (ASSESS_LOCK_KEY,))
    connection.commit()


def main() -> None:
    if not _try_acquire_lock():
        logger.info("[assess] another assess job appears to be running; exiting")
        return

    try:
        limit = int(os.getenv("GF_ASSESS_LIMIT", "3"))

        started = datetime.now(timezone.utc).isoformat()
        logger.info(f"[assess] CLI start {started} (limit={limit})")

        out = run(limit=limit)
        print(json.dumps(out, indent=2, default=str))

        finished = datetime.now(timezone.utc).isoformat()
        logger.info(f"[assess] CLI end {finished}")
    finally:
        _release_lock()


if __name__ == "__main__":
    main()
