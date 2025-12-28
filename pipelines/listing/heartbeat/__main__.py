from __future__ import annotations

import json
import time
import traceback
from datetime import datetime, timezone
from typing import Any, Dict

from utils.logger import get_logger

# Main orchestrator graph (sub graphs inside)
from pipelines.listing.main import run as run_main

logger = get_logger(__name__)


def _load_env() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


def _safe_json(obj: Any) -> str:
    return json.dumps(obj, indent=2, default=str)


def run_once() -> Dict[str, Any]:
    started = datetime.now(timezone.utc).isoformat()
    logger.info(f"[heartbeat] tick start {started}")

    try:
        out = run_main()
        finished = datetime.now(timezone.utc).isoformat()
        logger.info(f"[heartbeat] tick end {finished}")
        return {"ok": True, "started": started, "finished": finished, "out": out}
    except Exception as e:
        finished = datetime.now(timezone.utc).isoformat()
        logger.error(f"[heartbeat] tick failed {finished}: {e}\n{traceback.format_exc()}")
        return {
            "ok": False,
            "started": started,
            "finished": finished,
            "error": str(e),
            "traceback": traceback.format_exc(),
        }


def main() -> None:
    _load_env()

    # This module can run once OR loop depending on env/args.
    # For now: run once (cron-style).
    result = run_once()
    print(_safe_json(result))


if __name__ == "__main__":
    main()
