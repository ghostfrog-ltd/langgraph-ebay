-- Speed up exact operator stats for the never-checked direct backfill queue.

SET LOCAL statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_auction_direct_backfill_unchecked_end_time
    ON auction_listings (end_time)
    WHERE (final_price IS NULL OR final_price <= 0)
      AND end_time IS NOT NULL
      AND backfill_checked_at IS NULL
      AND external_id ~ '[0-9]';
