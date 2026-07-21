-- Create read-only normalized projections over the wide auction_listings table.
--
-- These views are the first Phase 2 compatibility surface. Existing pipelines
-- continue writing to auction_listings while new reads can target narrower,
-- auditable contracts. The joins make these views non-updatable in PostgreSQL,
-- which keeps them projection-only until real split tables are introduced.

CREATE OR REPLACE VIEW listing_identity_v AS
SELECT
    l.id AS listing_id,
    l.external_id,
    l.source_id,
    l.source AS source_label,
    s.name AS canonical_source_name,
    s.domain AS canonical_source_domain,
    s.niche AS source_niche,
    l.title,
    l.url,
    l.sale_type,
    l.first_seen
FROM auction_listings AS l
JOIN sources AS s
  ON s.id = l.source_id;

COMMENT ON VIEW listing_identity_v IS
    'Read-only listing identity projection over auction_listings and sources.';

CREATE OR REPLACE VIEW listing_observation_v AS
SELECT
    l.id AS listing_id,
    l.external_id,
    l.source_id,
    l.source AS source_label,
    l.price_current,
    l.price_bid_current,
    l.bids_count,
    l.end_time,
    l.time_left_s,
    l.first_seen,
    l.last_seen_at,
    l.fetched_at
FROM auction_listings AS l
JOIN sources AS s
  ON s.id = l.source_id;

COMMENT ON VIEW listing_observation_v IS
    'Read-only current observation projection for listing price, bid, and freshness fields.';

CREATE OR REPLACE VIEW listing_outcome_v AS
SELECT
    l.id AS listing_id,
    l.external_id,
    l.source_id,
    l.source AS source_label,
    l.status,
    st.lifecycle_stage,
    st.is_live_like,
    st.is_terminal,
    l.finalized,
    l.final_price,
    l.bids_count AS final_or_latest_bids_count,
    l.end_time
FROM auction_listings AS l
JOIN listing_statuses AS st
  ON st.status = l.status
JOIN sources AS s
  ON s.id = l.source_id;

COMMENT ON VIEW listing_outcome_v IS
    'Read-only outcome projection for status, finalization, and final-price fields.';

CREATE OR REPLACE VIEW listing_attribute_v AS
SELECT
    l.id AS listing_id,
    l.external_id,
    l.source_id,
    l.source AS source_label,
    l.model_key,
    l.brand,
    l.product_family,
    l.model_name,
    l.storage_gb,
    l.colour,
    l.epid,
    l.raw_attrs
FROM auction_listings AS l
JOIN sources AS s
  ON s.id = l.source_id;

COMMENT ON VIEW listing_attribute_v IS
    'Read-only extracted product attribute projection over auction_listings.';

CREATE OR REPLACE VIEW listing_score_v AS
SELECT
    l.id AS listing_id,
    l.external_id,
    l.source_id,
    l.source AS source_label,
    l.model_key,
    l.roi_estimate,
    l.max_bid,
    l.bucket_key,
    l.notes
FROM auction_listings AS l
JOIN sources AS s
  ON s.id = l.source_id;

COMMENT ON VIEW listing_score_v IS
    'Read-only scoring projection for legacy ROI and max-bid fields.';

CREATE OR REPLACE VIEW listing_backfill_audit_v AS
SELECT
    l.id AS listing_id,
    l.external_id,
    l.source_id,
    l.source AS source_label,
    l.status,
    l.finalized,
    l.backfill_checked_at,
    l.backfill_check_count,
    l.backfill_last_reason
FROM auction_listings AS l
JOIN sources AS s
  ON s.id = l.source_id;

COMMENT ON VIEW listing_backfill_audit_v IS
    'Read-only Point Zero backfill audit projection over auction_listings.';

CREATE OR REPLACE VIEW listing_ui_sync_v AS
SELECT
    l.id AS listing_id,
    l.external_id,
    l.source_id,
    l.source AS source_label,
    l.laravel_sync_state,
    l.laravel_synced_at,
    l.laravel_sync_error,
    l.laravel_sync_payload
FROM auction_listings AS l
JOIN sources AS s
  ON s.id = l.source_id;

COMMENT ON VIEW listing_ui_sync_v IS
    'Read-only sidecar UI sync projection for Laravel compatibility fields.';
