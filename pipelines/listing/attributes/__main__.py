from __future__ import annotations

import sys
from datetime import datetime, timezone

from utils.logger import get_logger
from pipelines.listing.attributes.graph import (
    run,
    save_graph_diagram,
)

logger = get_logger(__name__)


def _load_env() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


def _parse_int_flag(name: str, default: int) -> int:
    if name not in sys.argv:
        return default
    try:
        i = sys.argv.index(name)
        return int(sys.argv[i + 1])
    except Exception:
        logger.warning(
            f"[listing.attributes] invalid {name} value; using default={default}"
        )
        return default


def main() -> None:
    _load_env()

    started = datetime.now(timezone.utc).isoformat()
    logger.info(f"[listing.attributes] CLI start {started}")

    # Diagram-only mode
    if "--diagram" in sys.argv:
        path = "attributes_graph.mmd"
        save_graph_diagram(path)
        logger.info(
            f"[listing.attributes] wrote graph diagram to {path}"
        )

        finished = datetime.now(timezone.utc).isoformat()
        logger.info(f"[listing.attributes] CLI end {finished}")
        return

    limit = _parse_int_flag("--limit", 20)
    enable_api = "--no-api" not in sys.argv

    run(limit=limit, enable_api=enable_api)

    finished = datetime.now(timezone.utc).isoformat()
    logger.info(f"[listing.attributes] CLI end {finished}")


if __name__ == "__main__":
    main()
