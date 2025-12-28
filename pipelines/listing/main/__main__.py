from __future__ import annotations

import sys
from datetime import datetime, timezone

from utils.logger import get_logger
from pipelines.listing.main import save_graph_diagram

logger = get_logger(__name__)


def _load_env() -> None:
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


def main() -> None:
    _load_env()

    path = "main_graph.mmd"
    save_graph_diagram(path)

    finished = datetime.now(timezone.utc).isoformat()
    logger.info(f"[listing.comps] CLI end {finished}")

    return


if __name__ == "__main__":
    main()
