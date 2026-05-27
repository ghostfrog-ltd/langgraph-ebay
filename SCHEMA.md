# SCHEMA.md

## Persistence Contracts

PostgreSQL is the live source of truth for operational data. SQLite is only for local Laravel bootstrap and automated tests.

## Existing Operational Tables and Views

The current Python database uses these core objects:

- `auction_listings`: raw and enriched listing records.
- `latest_comps`: latest comp summary by `model_key`.
- `listing_assessments`: structured AI or rule assessment history.
- `sources`: source configuration and niche metadata.

The Laravel application database uses:

- `ebay_listings`: operator-facing synchronized listing records.
- `users`, `sessions`, `jobs`, and framework tables for the Laravel app.

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

These tables document the intended contracts. Add migrations before depending on them in live code.

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

`SCHEMA.md` is a contract, not an applied migration. Before code writes to a proposed table, add the matching Laravel migration, Python migration, or SQL migration and verify it against PostgreSQL.

