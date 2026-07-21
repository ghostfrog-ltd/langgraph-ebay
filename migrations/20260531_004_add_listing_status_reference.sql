-- Define and enforce allowed auction_listings.status values.
--
-- A reference table is used instead of a hard-coded CHECK so operators can
-- inspect status semantics and future migrations can add statuses explicitly.

CREATE TABLE IF NOT EXISTS listing_statuses (
    status TEXT PRIMARY KEY,
    lifecycle_stage TEXT NOT NULL,
    is_live_like BOOLEAN NOT NULL DEFAULT FALSE,
    is_terminal BOOLEAN NOT NULL DEFAULT FALSE,
    description TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO listing_statuses (
    status,
    lifecycle_stage,
    is_live_like,
    is_terminal,
    description
)
VALUES
    (
        'active',
        'live',
        TRUE,
        FALSE,
        'Live-like compatibility status used by older queries.'
    ),
    (
        'live',
        'live',
        TRUE,
        FALSE,
        'Listing has been seen active by the collector and can still change.'
    ),
    (
        'open',
        'live',
        TRUE,
        FALSE,
        'Live-like compatibility status for open listings.'
    ),
    (
        'ending',
        'live',
        TRUE,
        FALSE,
        'Live-like compatibility status for listings near end time.'
    ),
    (
        'ending_soon',
        'live',
        TRUE,
        FALSE,
        'Live-like compatibility status for listings near end time.'
    ),
    (
        'stale',
        'pending_outcome',
        FALSE,
        FALSE,
        'Listing stopped appearing live locally and needs outcome finalization.'
    ),
    (
        'ended',
        'terminal',
        FALSE,
        TRUE,
        'Listing ended without a confirmed sale price.'
    ),
    (
        'sold',
        'terminal',
        FALSE,
        TRUE,
        'Listing ended with a confirmed final sale price.'
    ),
    (
        'parse_failed',
        'terminal',
        FALSE,
        TRUE,
        'Outcome or listing data could not be parsed and was finalized conservatively.'
    )
ON CONFLICT (status) DO UPDATE
SET lifecycle_stage = EXCLUDED.lifecycle_stage,
    is_live_like = EXCLUDED.is_live_like,
    is_terminal = EXCLUDED.is_terminal,
    description = EXCLUDED.description;

DO $$
DECLARE
    unknown_statuses INTEGER;
    null_statuses INTEGER;
BEGIN
    SELECT COUNT(*) INTO unknown_statuses
    FROM auction_listings AS l
    LEFT JOIN listing_statuses AS s
      ON s.status = l.status
    WHERE l.status IS NOT NULL
      AND s.status IS NULL;

    SELECT COUNT(*) INTO null_statuses
    FROM auction_listings
    WHERE status IS NULL;

    IF unknown_statuses > 0 THEN
        RAISE EXCEPTION
            'listing_statuses reference is missing % auction_listings.status value(s)',
            unknown_statuses;
    END IF;

    IF null_statuses > 0 THEN
        RAISE EXCEPTION
            'auction_listings contains % NULL status value(s)',
            null_statuses;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'auction_listings_status_not_null'
          AND conrelid = 'auction_listings'::regclass
    ) THEN
        ALTER TABLE auction_listings
            ADD CONSTRAINT auction_listings_status_not_null
            CHECK (status IS NOT NULL)
            NOT VALID;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'auction_listings_status_fkey'
          AND conrelid = 'auction_listings'::regclass
    ) THEN
        ALTER TABLE auction_listings
            ADD CONSTRAINT auction_listings_status_fkey
            FOREIGN KEY (status)
            REFERENCES listing_statuses (status)
            NOT VALID;
    END IF;
END $$;

ALTER TABLE auction_listings
    VALIDATE CONSTRAINT auction_listings_status_not_null;

ALTER TABLE auction_listings
    VALIDATE CONSTRAINT auction_listings_status_fkey;
