-- Reconcile valid-but-wrong auction_listings.source_id values.
--
-- `auction_listings.source` remains as a denormalized compatibility label for
-- existing pipeline code. The canonical relational link is `source_id`, which
-- must point to the unique sources row whose name or domain matches that label.

DO $$
DECLARE
    mismatch_rows_before INTEGER;
    no_match_rows INTEGER;
    ambiguous_match_rows INTEGER;
    repaired_rows INTEGER;
    mismatch_rows_after INTEGER;
BEGIN
    WITH expected AS (
        SELECT
            l.id,
            l.source_id,
            s_expected.id AS expected_source_id,
            COUNT(s_expected.id) OVER (PARTITION BY l.id) AS match_count
        FROM auction_listings AS l
        LEFT JOIN sources AS s_expected
          ON s_expected.name = l.source
          OR s_expected.domain = l.source
    )
    SELECT
        COUNT(*) FILTER (
            WHERE match_count = 1
              AND source_id IS DISTINCT FROM expected_source_id
        ),
        COUNT(*) FILTER (
            WHERE match_count = 0
               OR expected_source_id IS NULL
        ),
        COUNT(*) FILTER (WHERE match_count > 1)
    INTO mismatch_rows_before, no_match_rows, ambiguous_match_rows
    FROM expected;

    IF no_match_rows > 0 THEN
        RAISE EXCEPTION
            'cannot reconcile source labels: % auction_listings rows have no matching sources.name/domain',
            no_match_rows;
    END IF;

    IF ambiguous_match_rows > 0 THEN
        RAISE EXCEPTION
            'cannot reconcile source labels: % auction_listings rows match multiple sources rows',
            ambiguous_match_rows;
    END IF;

    WITH source_matches AS (
        SELECT
            l.id AS listing_id,
            s_expected.id AS expected_source_id,
            COUNT(s_expected.id) OVER (PARTITION BY l.id) AS match_count
        FROM auction_listings AS l
        JOIN sources AS s_expected
          ON s_expected.name = l.source
          OR s_expected.domain = l.source
    ),
    deterministic_matches AS (
        SELECT listing_id, expected_source_id
        FROM source_matches
        WHERE match_count = 1
    ),
    repaired AS (
        UPDATE auction_listings AS l
        SET source_id = d.expected_source_id
        FROM deterministic_matches AS d
        WHERE l.id = d.listing_id
          AND l.source_id IS DISTINCT FROM d.expected_source_id
        RETURNING l.id
    )
    SELECT COUNT(*) INTO repaired_rows
    FROM repaired;

    WITH expected AS (
        SELECT
            l.id,
            l.source_id,
            s_expected.id AS expected_source_id,
            COUNT(s_expected.id) OVER (PARTITION BY l.id) AS match_count
        FROM auction_listings AS l
        LEFT JOIN sources AS s_expected
          ON s_expected.name = l.source
          OR s_expected.domain = l.source
    )
    SELECT COUNT(*) INTO mismatch_rows_after
    FROM expected
    WHERE match_count = 1
      AND source_id IS DISTINCT FROM expected_source_id;

    IF mismatch_rows_after > 0 THEN
        RAISE EXCEPTION
            'source label reconciliation left % deterministic mismatches unrepaired',
            mismatch_rows_after;
    END IF;

    RAISE NOTICE
        'source label reconciliation: mismatch_rows_before=%, repaired_rows=%, mismatch_rows_after=%',
        mismatch_rows_before,
        repaired_rows,
        mismatch_rows_after;
END $$;
