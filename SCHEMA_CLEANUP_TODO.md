# Schema Cleanup TODO

This is the working cleanup list from the 2026-05-31 read-only PostgreSQL audit.
The goal is to make the operational database auditable before any real Buy Desk,
purchase, check, ship, or relist ledger depends on it.

## Ground Rules

- Do not drop columns or tables until a fresh PostgreSQL backup exists.
- Keep the Python/LangGraph PostgreSQL schema as the source of truth.
- Treat Laravel/Herd as deprecated and disposable. Do not add new Laravel sync,
  scheduler, ingestion, notification, or communication paths.
- Prefer additive migrations and compatibility views before destructive cleanup.
- No marketplace writes, bids, purchases, messages, listings, or relists are part
  of this cleanup.

## Phase 0 - Baseline and Safety

- [x] Convert schema audit findings into this tracked TODO.
- [x] Add a repeatable read-only schema audit command.
- [x] Run a fresh PostgreSQL backup before any schema mutation.
  Backup created: `backups/postgres/gf_aab_prod_20260531T081216Z.dump`.
- [x] Save an audit baseline report after the backup.
  Baseline saved: `backups/postgres/schema_audit_20260531T081216Z.json`.
- [x] Add a real migrations directory and migration-runner convention for the
  Python/PostgreSQL schema.
  Use `scripts/run_migrations.py` with SQL files in `migrations/`.
- [ ] Stop adding new schema mutations ad hoc inside pipeline runtime modules.

## Phase 1 - Integrity Before Normalization

- [x] Fix `auction_listings.source_id` inconsistencies before adding a foreign key.
  The audit found 245 `motomine` rows pointing at missing `source_id = 13`.
  Migration `20260531_001_reconcile_auction_listing_source_ids.sql` reduced
  `auction_bad_source_id_refs` to 0.
- [x] Decide whether `auction_listings.source` remains a denormalized label or is
  replaced by `source_id` plus a compatibility view.
  Decision: keep `auction_listings.source` as a denormalized compatibility label
  during cleanup because many pipelines route by source text. Treat `source_id`
  as the canonical relational link. Migration
  `20260531_005_reconcile_source_label_to_source_id.sql` repaired 22 valid-but-
  wrong source links, and the audit now reports
  `auction_source_label_id_mismatches=0`.
- [x] Prevent recurring `source`/`source_id` drift during collector upserts.
  Migration `20260531_007_repair_recurring_source_label_mismatches.sql`
  repaired 1 new mismatch found during live collection, and the eBay collector
  upsert now updates the denormalized `source` label together with `source_id`.
- [x] Reconcile `auction_price_history` orphan rows. The audit found 16,629 history
  rows whose `external_id` no longer exists in `auction_listings`.
  Migration `20260531_002_archive_orphan_auction_price_history.sql` preserved
  them in `auction_price_history_orphans` and reduced active
  `price_history_orphans` to 0.
- [x] Add foreign keys only after data has been reconciled:
  - `auction_listings.source_id -> sources.id`
  - `auction_price_history.external_id -> auction_listings.external_id`
  - keep existing `listing_assessments.listing_id -> auction_listings.id`
  Migration `20260531_003_add_listing_integrity_foreign_keys.sql` added and
  validated the two newly clean foreign keys.
- [x] Add check constraints or reference tables for listing status values.
  Migration `20260531_004_add_listing_status_reference.sql` created
  `listing_statuses`, added a validated `auction_listings.status` foreign key,
  and added a validated non-null status check.

## Phase 2 - Split the Wide Listing Table

- [x] Keep `auction_listings` as a compatibility surface while introducing
  normalized read contracts.
  Migration `20260531_006_create_normalized_listing_views.sql` created
  read-only normalized views while preserving the existing write path.
- [x] Introduce stable identity fields through a listing identity contract:
  `listing_identity_v` exposes `external_id`, `source_id`, `title`, `url`,
  `sale_type`, `first_seen`, and source metadata.
- [x] Introduce live observation fields through a listing observation contract:
  `listing_observation_v` exposes `price_current`, `bids_count`, `end_time`,
  `last_seen_at`, `time_left_s`, and related freshness fields.
- [x] Introduce terminal outcome fields through a listing outcome contract:
  `listing_outcome_v` exposes
  `status`, `finalized`, `final_price`, final bid count, outcome source,
  outcome checked timestamp.
- [x] Introduce extracted product metadata through a listing attributes contract:
  `listing_attribute_v` exposes `brand`, `model_name`, `storage_gb`, `colour`,
  `epid`, `raw_attrs`, and model key fields.
- [x] Introduce scoring fields through a decision/snapshot read contract:
  `listing_score_v` exposes `roi_estimate`, `max_bid`, `bucket_key`, `notes`,
  and model key fields.
- [x] Introduce Point Zero backfill audit fields through a read contract:
  `listing_backfill_audit_v` exposes `backfill_checked_at`,
  `backfill_check_count`, and `backfill_last_reason`.
- [x] Introduce Laravel sync fields through a sidecar UI read contract:
  `listing_ui_sync_v` exposes `laravel_sync_state`, `laravel_synced_at`,
  `laravel_sync_error`, and `laravel_sync_payload`.
  This compatibility contract was later removed after Laravel/Herd deprecation.
- [x] Remove Python-to-Laravel opportunity communication.
  `pipelines.listing.roi` no longer posts ROI opportunities to Laravel or
  maintains a pending Laravel sync queue; Python-native email/Slack/PostgreSQL
  surfaces are the communication path.
- [x] Archive or drop deprecated Laravel sync columns and `listing_ui_sync_v`
  after a fresh audit confirms no Python code uses them.
  A fresh backup was created at
  `backups/postgres/gf_aab_prod_20260531T101007Z.dump`. Migration
  `20260531_008_drop_deprecated_laravel_sync_fields.sql` dropped
  `listing_ui_sync_v` and the four `laravel_sync_*` columns. The follow-up
  audit reports `schema_migrations=8`, no pending migrations, no Laravel sync
  columns, and no `listing_ui_sync_v`.
- [x] Delete the deprecated Laravel/Herd application trees after confirming no
  live process, cron, or operator workflow depends on them.
  Removed `laravel/`, `laravel_breeze_backup_20260310/`, the Laravel launchd
  template files, and the installed Laravel scheduler LaunchAgent/script.
- [ ] Plan the write-path split from `auction_listings` into real normalized
  tables after consumers have been moved to the read-only views.
- [x] Move the first low-risk consumer to normalized listing views.
  `pipelines.listing.market_discovery` now reads active listing evidence from
  `listing_identity_v`, `listing_attribute_v`, `listing_observation_v`, and
  `listing_outcome_v` instead of directly from `auction_listings`.
- [x] Move heartbeat listing summary metrics to normalized listing views.
  `pipelines.listing.main._heartbeat_metrics` now uses `listing_outcome_v`,
  `listing_attribute_v`, and `listing_score_v` for operator counts, comp
  coverage, and ROI risk buckets.
- [x] Move the review-only Slack auction digest to normalized listing views.
  `pipelines.listing.opportunity_digest` now loads digest candidates from
  `listing_identity_v`, `listing_attribute_v`, `listing_observation_v`, and
  `listing_outcome_v`, while reusing ROI pure scoring helpers.
- [x] Move the heartbeat LLM assessment candidate gate to normalized listing
  views. `pipelines.listing.main._has_assess_candidates` now checks live,
  unfinalized auction candidates through `listing_identity_v` and
  `listing_outcome_v`; the old and new live database counts matched at 118.
- [x] Move the actual LLM assessment candidate loader to normalized listing
  views. Preserve the existing candidate payload shape, compare old/new counts
  and sample rows, and do not trigger live model assessment during verification.
  `pipelines.listing.assess.graph._load_candidates` now reads from the
  normalized listing views; old/new counts matched at 1,204 and the first five
  stable-ordered sample rows matched exactly.
- [x] Move advisory project report loaders (`project_mini`, `project_msx125`)
  to normalized listing views.
  Both advisory loaders now read identity, observation, outcome, and attribute
  evidence through the listing view layer while preserving the existing alert
  candidate payload shape. Read-only parity checks matched the old direct-table
  ID sets: Mini 6/6 rows and MSX125/Grom 63/63 rows.
- [ ] Move read-only Point Zero backfill planning queries to
  `listing_backfill_audit_v`, `listing_outcome_v`, `listing_identity_v`,
  `listing_attribute_v`, and `listing_observation_v` where practical.
- [ ] Leave active collector writes, ended-finalization writes, ROI write paths,
  and paper-trading outcome writes on `auction_listings` until the read
  consumers are stable.
- [ ] Plan the real split-table migration only after read consumers are moved
  and a fresh backup/audit confirms compatibility.

## Phase 3 - Drop or Archive Candidates

Do not remove these until code references and a live audit agree they are safe.

- [ ] `auction_listings.price_bid_current`: 0 populated rows in the audit.
- [ ] `auction_listings.fetched_at`: 194 / 115,689 populated rows.
- [ ] `auction_listings.notes`: all rows empty in the audit.
- [ ] `auction_listings.product_family`: 1 populated row.
- [ ] `auction_listings.epid`: 1 populated row.
- [ ] `auction_listings.bucket_key`: sparse on listings and unused in
  `roi_snapshots`.
- [x] `auction_listings.laravel_sync_state`, `laravel_synced_at`,
  `laravel_sync_error`, and `laravel_sync_payload`: removed by migration
  `20260531_008_drop_deprecated_laravel_sync_fields.sql`.
- [ ] `alerts.listing_external_id`, `alerts.snipe_score`, `alerts.outcome`,
  `alerts.roi_net`, `alerts.reason`, `alerts.model_key`: 0 populated rows.
- [ ] Legacy/non-eBay tables: `articles`, `prices`, `reem_reminder_log`,
  `scrape_state`.

## Phase 4 - Comps and ROI Storage

- [ ] Decide whether `comps` is current-only or historical.
- [ ] If current-only, remove the redundant `latest_comps` materialized view or
  replace it with a plain compatibility view.
- [ ] If historical, create `comp_snapshots` and make `latest_comps` select from it.
- [ ] Add comp freshness and source/model-key coverage fields needed by Buy Desk
  gates.
- [ ] Add retention or compaction for `roi_snapshots`. The audit found roughly
  5 million rows, 777 MB, and only about 1,350 distinct `external_id` values.
- [ ] Add useful indexes for ROI history queries before relying on the table for
  operations.

## Phase 5 - Money and Units

- [ ] Standardize monetary values as `NUMERIC(12, 2)` or integer minor units, but
  do not mix both without explicit conversion.
- [ ] Stop rounding current prices during ingestion before persistence.
- [ ] Record currency where a table can contain marketplace-derived prices.
- [ ] Add tests around price parsing and pence preservation.

## Phase 6 - Buy/Check/Ship Readiness

- [ ] Implement `agent_decision_audit` or equivalent before any new automated
  decisioning surface.
- [ ] Implement `model_cost_ledger` or equivalent for paid model workflows.
- [ ] Implement `buy_desk_candidates` only after normalized listing, outcome,
  comp, and decision evidence contracts exist.
- [ ] Implement `purchase_lifecycle_events` only behind explicit human approval
  gates.
- [ ] Keep paper trading and Slack wording review-only until the deterministic
  Buy Desk gate is implemented and accepted.

## Commands

Run the repeatable read-only schema audit:

```bash
.venv-mac/bin/python scripts/schema_audit.py --markdown
```

Save a JSON baseline:

```bash
.venv-mac/bin/python scripts/schema_audit.py --json > schema_audit_baseline.json
```

List pending PostgreSQL migrations:

```bash
.venv-mac/bin/python scripts/run_migrations.py --dry-run
```

Apply pending PostgreSQL migrations:

```bash
.venv-mac/bin/python scripts/run_migrations.py
```
