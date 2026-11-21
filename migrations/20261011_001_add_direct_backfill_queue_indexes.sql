-- Speed up the direct historic final-price backfill queue.
--
-- The current direct backfill worker intentionally scans beyond status='unknown'
-- so it can repair legacy live/ended/parse_failed rows that never received a
-- conclusive Trading check. These partial indexes match that broader queue and
-- keep the ORDER BY id ASC LIMIT N candidate load from walking the wide table.

SET LOCAL statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_auction_direct_backfill_unchecked_id
    ON auction_listings (id)
    INCLUDE (
        external_id,
        source,
        status,
        finalized,
        model_key,
        title,
        price_current,
        bids_count,
        price_bid_current,
        sale_type,
        end_time,
        last_seen_at,
        backfill_check_count,
        backfill_last_reason
    )
    WHERE (final_price IS NULL OR final_price <= 0)
      AND end_time IS NOT NULL
      AND backfill_checked_at IS NULL
      AND external_id ~ '[0-9]';

CREATE INDEX IF NOT EXISTS idx_auction_direct_backfill_retryable_id
    ON auction_listings (id)
    INCLUDE (
        external_id,
        source,
        status,
        finalized,
        model_key,
        title,
        price_current,
        bids_count,
        price_bid_current,
        sale_type,
        end_time,
        last_seen_at,
        backfill_checked_at,
        backfill_check_count,
        backfill_last_reason
    )
    WHERE (final_price IS NULL OR final_price <= 0)
      AND end_time IS NOT NULL
      AND backfill_checked_at IS NOT NULL
      AND NOT (status = 'ended' AND COALESCE(finalized, FALSE) = TRUE)
      AND COALESCE(backfill_last_reason, '') <> 'invalid_external_id'
      AND external_id ~ '[0-9]';

CREATE INDEX IF NOT EXISTS idx_auction_final_price_positive_id
    ON auction_listings (id)
    WHERE final_price IS NOT NULL
      AND final_price > 0;

CREATE INDEX IF NOT EXISTS idx_auction_missing_model_key_id
    ON auction_listings (id)
    WHERE model_key IS NULL
       OR model_key = ''
       OR LOWER(model_key) = 'unknown'
       OR LOWER(model_key) LIKE 'unknown%';

CREATE INDEX IF NOT EXISTS idx_auction_unknown_unfinalized_id
    ON auction_listings (id)
    WHERE status = 'unknown'
      AND COALESCE(finalized, FALSE) = FALSE;

CREATE INDEX IF NOT EXISTS idx_auction_unknown_not_due_id
    ON auction_listings (id)
    WHERE status = 'unknown'
      AND COALESCE(finalized, FALSE) = FALSE
      AND end_time IS NOT NULL;
