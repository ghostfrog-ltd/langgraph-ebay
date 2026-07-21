# GhostFrog eBay

Mission: **buy cheap from eBay to resell on eBay for profit**.

This repo is the data engine for that side hustle. It collects eBay listings,
recovers final prices, builds product comps, filters bad listings, calculates
safe max buy prices, and produces a small human review shortlist.

Read first: [PURPOSE.md](PURPOSE.md).

## Stack

- Python pipeline engine
- PostgreSQL operational database
- LangGraph orchestration where useful
- FastAPI only for future operator/API surfaces

Laravel/Herd was an experiment and is not part of the active system.

## Current Priority

1. Fix slow heartbeat/database queries.
2. Recover final sold prices in useful markets.
3. Clean product identity and parts/accessory filters.
4. Rebuild comps from confirmed sold prices only.
5. Produce a tiny daily shortlist worth manual inspection.

## Safety

The system can recommend and explain. It must not bid, buy, message, relist, or
change eBay/account state without explicit human approval.

## Useful Commands

```bash
.venv-mac/bin/python -m unittest discover tests
.venv-mac/bin/python scripts/run_migrations.py --dry-run
.venv-mac/bin/python -m pipelines.listing.market_discovery --limit 12 --min-score 20
.venv-mac/bin/python -m pipelines.listing.paper_trading --limit 25 --no-resolve
```
