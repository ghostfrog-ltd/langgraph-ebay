"""Command-line entry point for the paper-trading learning loop."""

from __future__ import annotations

import argparse
import json

from pipelines.listing.paper_trading.graph import run


def main() -> None:
    """Run one paper-trading snapshot/update pass and print a JSON summary."""
    parser = argparse.ArgumentParser(description="Snapshot and update paper-trading candidates.")
    parser.add_argument("--limit", type=int, default=25, help="Maximum current shortlist candidates to snapshot.")
    parser.add_argument(
        "--no-snapshot",
        action="store_true",
        help="Only update existing prediction outcomes; do not snapshot current candidates.",
    )
    parser.add_argument(
        "--no-resolve",
        action="store_true",
        help="Do not spend Trading API calls on overdue watched predictions.",
    )
    parser.add_argument(
        "--resolve-limit",
        type=int,
        default=5,
        help="Maximum overdue watched predictions to resolve through Trading API.",
    )
    args = parser.parse_args()

    summary = run(
        limit=max(args.limit, 0),
        snapshot=not args.no_snapshot,
        resolve_outcomes=not args.no_resolve,
        resolve_limit=max(args.resolve_limit, 0),
    )
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
