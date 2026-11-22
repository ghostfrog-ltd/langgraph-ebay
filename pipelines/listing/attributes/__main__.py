from __future__ import annotations

import json
import sys
from datetime import datetime, timezone

from utils.logger import get_logger
from pipelines.listing.attributes.graph import (
    run,
    run_for_ids,
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


def _parse_ids_flag(name: str = "--ids") -> list[str]:
    if name not in sys.argv:
        return []
    i = sys.argv.index(name) + 1
    values: list[str] = []
    while i < len(sys.argv) and not sys.argv[i].startswith("--"):
        for part in sys.argv[i].split(","):
            part = part.strip()
            if part:
                values.append(part)
        i += 1
    return values


def main() -> None:
    if "--help" in sys.argv or "-h" in sys.argv:
        print("Usage: python -m pipelines.listing.attributes [--apple] [--limit N] [--no-api]\n"
              "  --apple, -apple  Enrich unkeyed Apple listings without an enrichment status, live first\n"
              "  --limit N        Maximum listings in a batch (default 20)\n"
              "  --ids ID[,ID]    Enrich specific eBay items; use --force to recheck them\n"
              "  --no-api         Disable eBay requests\n"
              "  --diagram        Save a diagram of the enrichment graph")
        return

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
    force = "--force" in sys.argv
    item_ids = _parse_ids_flag()
    apple_only = "--apple" in sys.argv or "-apple" in sys.argv
    if apple_only and ("--ids" in sys.argv or force or limit < 1):
        raise SystemExit("--apple requires a positive --limit and selects its own item IDs.")

    if item_ids:
        result = run_for_ids(item_ids, enable_api=enable_api, force=force)
        print(json.dumps(result, indent=2, default=str))
    else:
        run(limit=limit, enable_api=enable_api, apple_only=apple_only)

    finished = datetime.now(timezone.utc).isoformat()
    logger.info(f"[listing.attributes] CLI end {finished}")


if __name__ == "__main__":
    main()
