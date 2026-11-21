# Schema

PostgreSQL is the live source of truth.

## Core Tables / Views

- `auction_listings`: collected listings, current state, product identity,
  condition grade, prices, finalization, ROI fields.
- `auction_price_history`: observed price/bid history.
- `sources`: configured collection sources.
- `comps`: confirmed-final-price comp summaries by `model_key` and
  `condition_grade`.
- `listing_text_conditions`: local LLM text-condition evidence for captured
  listing text.
- `listing_text_condition_queue`: local work queue for text-condition checks.
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
- `latest_listing_text_conditions`

## Important Fields

For buy/resale decisions, preserve:

- external id and URL,
- source/market,
- product/model key,
- condition grade,
- current price,
- final price when confirmed,
- status/finalized state,
- comp samples and resale estimate,
- fees/postage/packaging/repair/risk reserves,
- expected profit,
- max buy price,
- red flags and decision status.

Important listing statuses:

- `live`: currently seen as buyable/open.
- `unknown`: no longer seen locally, but eBay has not confirmed the outcome.
- `sold`: ended with confirmed `final_price`.
- `ended`: ended without confirmed sale evidence.

Do not use a `stale` business state. "Not seen recently" means `unknown`.

## Migration Rule

Use SQL files under `migrations/` and apply with:

```bash
.venv-mac/bin/python scripts/run_migrations.py
```

Check pending migrations with:

```bash
.venv-mac/bin/python scripts/run_migrations.py --dry-run
```
