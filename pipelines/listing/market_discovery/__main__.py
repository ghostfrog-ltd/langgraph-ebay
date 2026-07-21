"""Command-line entry point for the read-only market discovery report."""

from __future__ import annotations

import argparse
import json

from pipelines.listing.market_discovery.graph import run


def main() -> None:
    """Build one market discovery report and print JSON for operator review."""
    parser = argparse.ArgumentParser(description="Rank existing eBay market lanes from local evidence.")
    parser.add_argument("--limit", type=int, default=20, help="Maximum ranked lanes to print.")
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.0,
        help="Minimum discovery score to include in the report.",
    )
    args = parser.parse_args()

    report = run(limit=max(args.limit, 0), min_score=max(args.min_score, 0.0))
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()

