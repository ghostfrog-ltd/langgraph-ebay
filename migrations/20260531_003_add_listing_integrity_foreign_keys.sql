-- Enforce cleaned listing/source and price-history/listing relationships.
--
-- The prior migrations reconciled auction_listings.source_id and archived
-- orphan auction_price_history rows. This migration adds foreign keys so those
-- integrity issues cannot quietly return.

CREATE INDEX IF NOT EXISTS auction_listings_source_id_idx
    ON auction_listings (source_id);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'auction_listings_source_id_fkey'
          AND conrelid = 'auction_listings'::regclass
    ) THEN
        ALTER TABLE auction_listings
            ADD CONSTRAINT auction_listings_source_id_fkey
            FOREIGN KEY (source_id)
            REFERENCES sources (id)
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'auction_price_history_external_id_fkey'
          AND conrelid = 'auction_price_history'::regclass
    ) THEN
        ALTER TABLE auction_price_history
            ADD CONSTRAINT auction_price_history_external_id_fkey
            FOREIGN KEY (external_id)
            REFERENCES auction_listings (external_id)
            NOT VALID;
    END IF;
END $$;

ALTER TABLE auction_listings
    VALIDATE CONSTRAINT auction_listings_source_id_fkey;

ALTER TABLE auction_price_history
    VALIDATE CONSTRAINT auction_price_history_external_id_fkey;
