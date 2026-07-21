-- Reconcile auction_listings.source_id against the canonical sources table.
--
-- This is intentionally limited to local PostgreSQL data integrity. It does
-- not call eBay, mutate marketplace state, or alter listing business status.
-- It repairs rows where source_id points at a missing sources.id by matching
-- auction_listings.source to either sources.name or sources.domain.

DO $$
DECLARE
    unresolved_before INTEGER;
    repaired_rows INTEGER;
    unresolved_after INTEGER;
BEGIN
    SELECT COUNT(*) INTO unresolved_before
    FROM auction_listings AS l
    LEFT JOIN sources AS current_source
      ON current_source.id = l.source_id
    WHERE l.source_id IS NOT NULL
      AND current_source.id IS NULL;

    WITH source_matches AS (
        SELECT
            l.id AS listing_id,
            s.id AS resolved_source_id,
            COUNT(*) OVER (PARTITION BY l.id) AS match_count
        FROM auction_listings AS l
        JOIN sources AS s
          ON s.name = l.source
          OR s.domain = l.source
        LEFT JOIN sources AS current_source
          ON current_source.id = l.source_id
        WHERE l.source_id IS NULL
           OR current_source.id IS NULL
    ),
    deterministic_matches AS (
        SELECT listing_id, resolved_source_id
        FROM source_matches
        WHERE match_count = 1
    ),
    repaired AS (
        UPDATE auction_listings AS l
        SET source_id = d.resolved_source_id
        FROM deterministic_matches AS d
        WHERE l.id = d.listing_id
          AND l.source_id IS DISTINCT FROM d.resolved_source_id
        RETURNING l.id
    )
    SELECT COUNT(*) INTO repaired_rows
    FROM repaired;

    SELECT COUNT(*) INTO unresolved_after
    FROM auction_listings AS l
    LEFT JOIN sources AS current_source
      ON current_source.id = l.source_id
    WHERE l.source_id IS NOT NULL
      AND current_source.id IS NULL;

    IF unresolved_after > 0 THEN
        RAISE EXCEPTION
            'source_id reconciliation left % unresolved auction_listings rows unrepaired',
            unresolved_after;
    END IF;

    RAISE NOTICE
        'source_id reconciliation: unresolved_before=%, repaired_rows=%, unresolved_after=%',
        unresolved_before,
        repaired_rows,
        unresolved_after;
END $$;
