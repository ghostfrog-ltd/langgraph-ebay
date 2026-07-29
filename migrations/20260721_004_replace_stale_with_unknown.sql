-- Replace the legacy `stale` listing status with `unknown`.
--
-- `stale` mixed two meanings: "not seen recently" and "ended". The collector
-- now marks not-seen listings as `unknown`; Trading final-price checks decide
-- whether an unknown row is still live, sold, ended without sale, or deleted.

INSERT INTO listing_statuses (
    status,
    lifecycle_stage,
    is_live_like,
    is_terminal,
    description
)
VALUES (
    'unknown',
    'pending_outcome',
    FALSE,
    FALSE,
    'Listing is no longer seen locally, but eBay has not confirmed whether it is live, sold, ended, or deleted.'
)
ON CONFLICT (status) DO UPDATE
SET lifecycle_stage = EXCLUDED.lifecycle_stage,
    is_live_like = EXCLUDED.is_live_like,
    is_terminal = EXCLUDED.is_terminal,
    description = EXCLUDED.description;

UPDATE auction_listings
SET status = 'unknown'
WHERE status = 'stale';

DELETE FROM listing_statuses
WHERE status = 'stale';

DROP INDEX IF EXISTS idx_auction_stale_backfill_candidates;

CREATE INDEX IF NOT EXISTS idx_auction_unknown_backfill_candidates
    ON auction_listings (
        source,
        model_key,
        end_time DESC,
        backfill_checked_at,
        last_seen_at DESC
    )
    INCLUDE (
        external_id,
        title,
        price_current,
        bids_count,
        price_bid_current,
        sale_type,
        backfill_check_count,
        backfill_last_reason
    )
    WHERE status = 'unknown'
      AND COALESCE(finalized, FALSE) = FALSE;
