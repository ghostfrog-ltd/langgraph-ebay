from __future__ import annotations

import json
from datetime import datetime, timezone

from utils.logger import get_logger
from utils.auth import get_auth  # you said this exists
from .graph import run

logger = get_logger(__name__)


def main() -> None:
    started = datetime.now(timezone.utc).isoformat()
    logger.info(f"[scrape] CLI start {started}")

    ebay_token = get_auth().get_token()

    out = run(ebay_token=ebay_token)

    # Print something useful to the terminal
    print(json.dumps(out, indent=2, default=str))

    finished = datetime.now(timezone.utc).isoformat()
    logger.info(f"[scrape] CLI end {finished}")


if __name__ == "__main__":
    main()
