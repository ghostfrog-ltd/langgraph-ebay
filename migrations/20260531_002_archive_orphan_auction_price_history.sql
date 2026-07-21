-- Archive orphan auction_price_history rows before enforcing integrity.
--
-- Orphan price history can still be useful forensic evidence, so this migration
-- preserves each row in auction_price_history_orphans before removing it from
-- the active history table. It only touches local PostgreSQL tables and does
-- not call eBay or mutate marketplace state.

CREATE TABLE IF NOT EXISTS auction_price_history_orphans (
    original_id INTEGER PRIMARY KEY,
    price BIGINT,
    bids_count INTEGER,
    "timestamp" TIMESTAMP WITHOUT TIME ZONE,
    external_id TEXT,
    recorded_at TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    archive_reason TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS auction_price_history_orphans_external_id_idx
    ON auction_price_history_orphans (external_id);

CREATE INDEX IF NOT EXISTS auction_price_history_orphans_recorded_at_idx
    ON auction_price_history_orphans (recorded_at DESC);

DO $$
DECLARE
    orphan_rows_before INTEGER;
    newly_archived_rows INTEGER;
    removed_rows INTEGER;
    orphan_rows_after INTEGER;
BEGIN
    SELECT COUNT(*) INTO orphan_rows_before
    FROM auction_price_history AS ph
    LEFT JOIN auction_listings AS l
      ON l.external_id = ph.external_id
    WHERE l.external_id IS NULL;

    WITH orphan_rows AS (
        SELECT
            ph.id,
            ph.price,
            ph.bids_count,
            ph."timestamp",
            ph.external_id,
            ph.recorded_at
        FROM auction_price_history AS ph
        LEFT JOIN auction_listings AS l
          ON l.external_id = ph.external_id
        WHERE l.external_id IS NULL
    ),
    archived AS (
        INSERT INTO auction_price_history_orphans (
            original_id,
            price,
            bids_count,
            "timestamp",
            external_id,
            recorded_at,
            archive_reason
        )
        SELECT
            id,
            price,
            bids_count,
            "timestamp",
            external_id,
            recorded_at,
            'missing auction_listings.external_id'
        FROM orphan_rows
        ON CONFLICT (original_id) DO NOTHING
        RETURNING original_id
    )
    SELECT COUNT(*) INTO newly_archived_rows
    FROM archived;

    WITH removed AS (
        DELETE FROM auction_price_history AS ph
        USING auction_price_history_orphans AS archive
        WHERE archive.original_id = ph.id
          AND NOT EXISTS (
              SELECT 1
              FROM auction_listings AS l
              WHERE l.external_id = ph.external_id
          )
        RETURNING ph.id
    )
    SELECT COUNT(*) INTO removed_rows
    FROM removed;

    SELECT COUNT(*) INTO orphan_rows_after
    FROM auction_price_history AS ph
    LEFT JOIN auction_listings AS l
      ON l.external_id = ph.external_id
    WHERE l.external_id IS NULL;

    IF orphan_rows_after > 0 THEN
        RAISE EXCEPTION
            'auction_price_history orphan archive left % active orphan rows unrepaired',
            orphan_rows_after;
    END IF;

    RAISE NOTICE
        'auction_price_history orphan archive: orphan_rows_before=%, newly_archived_rows=%, removed_rows=%, orphan_rows_after=%',
        orphan_rows_before,
        newly_archived_rows,
        removed_rows,
        orphan_rows_after;
END $$;
