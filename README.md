# GhostFrog eBay

Mission: **buy cheap from eBay to resell on eBay for profit**.

This repo is the data engine for that side hustle. It collects eBay listings,
recovers final prices, builds product-and-condition comps, filters bad listings,
calculates safe max buy prices, and prints live listing potentials plus closest
misses.

Read first: [PURPOSE.md](PURPOSE.md).

## Stack

- Python pipeline engine
- PostgreSQL operational database
- LangGraph orchestration where useful

## Current Priority

1. Recover final sold prices in useful markets.
2. Keep product identity, condition grades, and parts/accessory filters clean.
3. Rebuild comps from confirmed sold prices only.
4. Run the live-listing potentials query.
5. Let the human decide whether near-misses are worth a punt.

The core report is not an LLM judgement. It is plain maths:

`listing | model_key | condition_grade | current_price | sold_median | comp_count | max_safe_buy | expected_profit | signal`

Default gate: expected profit >= £50 and at least 3 confirmed sold comps for the
same clean `model_key` and condition grade. ROI is displayed as context, not
used as the default blocker.

## Safety

The system can recommend and explain. It must not bid, buy, message, relist, or
change eBay/account state without explicit human approval.

## Useful Commands

```bash
ebay
ebay stats
ebay serious
ebay punt
ebay backfill-plan
.venv-mac/bin/python -m unittest discover tests
.venv-mac/bin/python scripts/run_migrations.py --dry-run
.venv-mac/bin/python -m pipelines.listing.market_discovery --limit 20 --min-profit 50 --min-comps 15
.venv-mac/bin/python -m pipelines.listing.paper_trading --limit 25 --no-resolve
```

The repo also keeps the wrapper at `./ebay`. Plain `ebay` works when
`/opt/homebrew/bin/ebay` points to that wrapper.

```bash
/opt/homebrew/bin/ebay -> /Volumes/Bob/www/ghostfrog-langgraph-ebay/ebay
```
