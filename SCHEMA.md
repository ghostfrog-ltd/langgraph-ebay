# SCHEMA.md

## Persistence Contracts

PostgreSQL is the live source of truth for operational data. The primary operational schema belongs to the Python/LangGraph pipeline database.

Laravel and Laravel Herd are deprecated/disposable experiments. Their local
SQLite/bootstrap database, framework tables, schedulers, ingestion endpoints,
and sync jobs are not operational contracts. Any useful UI/API/communication
behavior should be rebuilt in Python/FastAPI against PostgreSQL.

## Existing Operational Tables and Views

The current Python database uses these core objects:

- `auction_listings`: raw and enriched listing records.
- `auction_price_history`: active price/bid observations for known listings.
- `auction_price_history_orphans`: archived price/bid observations whose
  `external_id` no longer exists in `auction_listings`; rows are preserved here
  before foreign-key cleanup.
- `listing_statuses`: allowed listing status values and lifecycle semantics.
- `latest_comps`: latest comp summary by `model_key`.
- `listing_assessments`: structured AI or rule assessment history.
- `sources`: source configuration and niche metadata.
- `alert_state`: notification throttle state keyed by `name`, including heartbeat email and Slack rows such as `listing_system_hourly_heartbeat` and `listing_system_hourly_slack`.

The following read-only views project the wide `auction_listings` table into
narrower contracts without changing the current write path:

- `listing_identity_v`: stable listing identity and canonical source metadata.
- `listing_observation_v`: current price, bid, end-time, and freshness fields.
- `listing_outcome_v`: status, lifecycle stage, finalization, and final-price fields.
- `listing_attribute_v`: extracted product identity and raw attribute fields.
- `listing_score_v`: legacy ROI, max bid, bucket, and note fields.
- `listing_backfill_audit_v`: targeted backfill audit fields.

`auction_listings.source` remains a denormalized compatibility label because
current pipeline code routes model-key extraction, ROI rules, scout digests, and
vehicle review guards by source text. `auction_listings.source_id` is the
canonical relational link and must agree with the `sources` row whose `name` or
`domain` matches that label. Collector upserts must update `source` and
`source_id` together so cross-source sightings do not leave stale labels beside
new canonical source IDs.

Point Zero backfill adds these local audit fields to `auction_listings`:

- `backfill_checked_at`: last time a stale row was checked through the targeted backfill loop.
- `backfill_check_count`: number of targeted backfill checks attempted for the row.
- `backfill_last_reason`: latest deterministic outcome reason, such as `trading_says_not_ended`.

Paper trading persists review-only learning data in `paper_trade_predictions`:

- `external_id`, `source`, `title`, `url`, `model_key`: listing identity captured at prediction time.
- `current_price`, `predicted_resale_price`, `predicted_fees`, `predicted_outbound_shipping`, `predicted_inbound_shipping`, `predicted_packaging`, `predicted_selling_reserve`, `predicted_profit`, `predicted_roi`, `recommended_max_bid`, `comp_samples`: the deterministic economic prediction snapshot.
- `decision_status`, `observed_status`, `observed_final_price`, `observed_purchase_price`, `would_have_bid`, `would_have_won`, `observed_at`, `observed_price_error`, `observed_paper_profit`, `observed_paper_roi`, `outcome_reason`: later observed outcome fields.
- `outcome_checked_at`, `outcome_check_count`, `outcome_last_reason`: audit fields for capped Trading API outcome checks on watched predictions past `end_time`.
- `UNIQUE (external_id, prediction_date)`: prevents hourly duplicate snapshots while retaining a daily series.

Paper-trade accounting treats a confirmed eBay final price as the hypothetical purchase price for the watched auction. Expected resale still comes from confirmed sold comps. A closed paper row is profitable only when the final auction price stayed within the stored max bid and expected resale clears predicted fees, inbound postage, outbound postage, packaging, selling/relist reserve, and the purchase price.

Deprecated Laravel UI tables such as `ebay_listings`, `users`, `sessions`, and
`jobs` are not part of the Python/PostgreSQL operational contract. Do not use
them for new communication, notification, or decision flows.

Migration `20260531_008_drop_deprecated_laravel_sync_fields.sql` removed the
deprecated `listing_ui_sync_v` projection and the `auction_listings`
`laravel_sync_state`, `laravel_synced_at`, `laravel_sync_error`, and
`laravel_sync_payload` columns after a fresh PostgreSQL backup.

## Applied Integrity Constraints

The Python/PostgreSQL schema now enforces these core relationships:

- `auction_listings.source_id` references `sources.id`.
- `auction_listings.status` references `listing_statuses.status`.
- `auction_listings.status` must not be null.
- `auction_price_history.external_id` references `auction_listings.external_id`.
- `listing_assessments.listing_id` references `auction_listings.id`.

## Required Decision Fields

Any buy-desk or assessment persistence should be queryable by:

- source listing id
- external eBay id
- source/niche
- product identity key
- current price
- conservative resale estimate
- inbound shipping estimate
- outbound shipping estimate
- packaging estimate
- repair/refurb reserve
- marketplace/platform fee estimate
- risk reserve
- expected net profit
- ROI percentage
- recommended max bid
- comp sample count
- comp freshness
- confidence
- decision status
- red flags
- model and usage metadata when LLMs are used
- created and updated timestamps

## Proposed Tables

These tables document the intended Python/PostgreSQL contracts. Add a real migration or SQL install step before depending on them in live code.

```sql
CREATE TABLE IF NOT EXISTS agent_decision_audit (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    workflow_name TEXT NOT NULL,
    actor TEXT NOT NULL,
    decision_type TEXT NOT NULL,
    subject_type TEXT NOT NULL,
    subject_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    confidence NUMERIC(5, 4),
    rule_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
    summary TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS model_cost_ledger (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    workflow_name TEXT NOT NULL,
    model_id TEXT NOT NULL,
    prompt_tokens INTEGER,
    completion_tokens INTEGER,
    estimated_cost_usd NUMERIC(12, 6),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS buy_desk_candidates (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    listing_id INTEGER NOT NULL,
    external_id TEXT NOT NULL,
    source TEXT NOT NULL,
    product_key TEXT,
    status TEXT NOT NULL,
    current_price NUMERIC(12, 2) NOT NULL,
    resale_estimate NUMERIC(12, 2),
    expected_net_profit NUMERIC(12, 2),
    roi_percentage NUMERIC(8, 2),
    recommended_max_bid NUMERIC(12, 2),
    comp_samples INTEGER,
    confidence NUMERIC(5, 4),
    red_flags JSONB NOT NULL DEFAULT '[]'::jsonb,
    cost_assumptions JSONB NOT NULL DEFAULT '{}'::jsonb,
    decision_summary TEXT
);

CREATE TABLE IF NOT EXISTS purchase_lifecycle_events (
    id BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    candidate_id BIGINT,
    external_id TEXT NOT NULL,
    state TEXT NOT NULL,
    actor TEXT NOT NULL,
    amount NUMERIC(12, 2),
    notes TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);
```

## Migration Rule

`SCHEMA.md` is a contract, not an applied migration. Before code writes to a proposed table, add the matching Python/PostgreSQL migration under `migrations/` and apply it with:

```bash
.venv-mac/bin/python scripts/run_migrations.py
```

Applied migrations are recorded in `schema_migrations` with a SHA-256 checksum. Do not add new runtime-only schema mutations to pipeline modules when a migration can define the contract explicitly.

As of 2026-05-31, the local PostgreSQL schema has 8 recorded migrations,
including normalized listing views, recurring source-label reconciliation, and
deprecated Laravel sync field removal.

If a UI screen needs the same data, build it in the Python/FastAPI stack or make
it read from PostgreSQL through an explicit Python-owned contract.
