# Schema

PostgreSQL is the live source of truth.

## Core Tables / Views

- `auction_listings`: collected listings, current state, product identity,
  prices, finalization, ROI fields.
- `auction_price_history`: observed price/bid history.
- `sources`: configured collection sources.
- `comps`: confirmed-final-price comp summaries by `model_key`.
- `listing_assessments`: optional structured assessment history.
- `paper_trade_predictions`: candidate-validation snapshots and outcomes.
- `alert_state`: email/Slack throttle state.
- `schema_migrations`: applied SQL migrations and checksums.

Useful read views include:

- `listing_identity_v`
- `listing_observation_v`
- `listing_outcome_v`
- `listing_attribute_v`
- `listing_score_v`
- `listing_backfill_audit_v`

## Important Fields

For buy/resale decisions, preserve:

- external id and URL,
- source/market,
- product/model key,
- current price,
- final price when confirmed,
- status/finalized state,
- comp samples and resale estimate,
- fees/postage/packaging/repair/risk reserves,
- expected profit,
- max buy price,
- red flags and decision status.

## Migration Rule

Use SQL files under `migrations/` and apply with:

```bash
.venv-mac/bin/python scripts/run_migrations.py
```

Check pending migrations with:

```bash
.venv-mac/bin/python scripts/run_migrations.py --dry-run
```
