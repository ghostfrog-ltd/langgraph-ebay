from __future__ import annotations

import sys
from datetime import datetime, timezone

from utils.logger import get_logger
from pipelines.listing.comps.graph import run, save_graph_diagram

logger = get_logger(__name__)


def _load_env() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


def main() -> None:
    _load_env()

    started = datetime.now(timezone.utc).isoformat()
    logger.info(f"[listing.comps] CLI start {started}")

    # Generate diagram only
    if "--diagram" in sys.argv:
        path = "comps_graph.mmd"
        save_graph_diagram(path)
        logger.info(f"[listing.comps] wrote graph diagram to {path}")

        finished = datetime.now(timezone.utc).isoformat()
        logger.info(f"[listing.comps] CLI end {finished}")
        return

    # Default: force recompute comps
    run(force=True)

    finished = datetime.now(timezone.utc).isoformat()
    logger.info(f"[listing.comps] CLI end {finished}")


if __name__ == "__main__":
    main()
