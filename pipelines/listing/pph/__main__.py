from __future__ import annotations

import json
from datetime import datetime, timezone

from utils.logger import get_logger
from utils.auth import get_auth
from pipelines.listing.pph.graph import run

logger = get_logger(__name__)


def _load_env() -> None:
    """
    Load .env if python-dotenv is installed.
    Safe no-op if not installed (assumes env already set).
    """
    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        return
    load_dotenv()


def main() -> None:
    _load_env()

    started = datetime.now(timezone.utc).isoformat()
    logger.info(f"[listing.pph] CLI start {started}")

    ebay_token = get_auth().get_token()
    out = run(ebay_token=ebay_token)

    print(json.dumps(out, indent=2, default=str))

    finished = datetime.now(timezone.utc).isoformat()
    logger.info(f"[listing.pph] CLI end {finished}")


if __name__ == "__main__":
    main()
