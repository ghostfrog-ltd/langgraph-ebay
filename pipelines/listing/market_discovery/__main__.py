"""Command-line entry point for the read-only live listing potentials report."""

from __future__ import annotations

import argparse
import json

from pipelines.listing.market_discovery.graph import run


def main() -> None:
    """Build one potentials report and print JSON for operator review."""
    parser = argparse.ArgumentParser(description="List live eBay potentials from model keys and sold comps.")
    parser.add_argument("--limit", type=int, default=20, help="Maximum potentials and closest misses to print.")
    parser.add_argument("--min-profit", type=float, default=50.0, help="Minimum expected profit in GBP.")
    parser.add_argument(
        "--min-roi",
        type=float,
        default=0.0,
        help="Optional minimum ROI as a decimal. Default 0 means do not block on ROI.",
    )
    parser.add_argument(
        "--min-comps",
        type=int,
        default=None,
        help="Override the category-specific minimum confirmed sold comps floor.",
    )
    parser.add_argument(
        "--min-score",
        type=float,
        default=0.0,
        help="Legacy no-op argument retained for old callers.",
    )
    args = parser.parse_args()

    report = run(
        limit=max(args.limit, 0),
        min_score=max(args.min_score, 0.0),
        min_profit=max(args.min_profit, 0.0),
        min_roi=max(args.min_roi, 0.0),
        min_comps=max(args.min_comps, 1) if args.min_comps is not None else None,
    )
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
