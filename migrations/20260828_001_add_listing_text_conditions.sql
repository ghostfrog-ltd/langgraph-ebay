ALTER TABLE auction_listings
    ADD COLUMN IF NOT EXISTS text_condition_checked BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE auction_listings
    ADD COLUMN IF NOT EXISTS text_condition_checked_at TIMESTAMP WITHOUT TIME ZONE;

CREATE INDEX IF NOT EXISTS auction_listings_text_condition_checked_idx
    ON auction_listings (text_condition_checked, first_seen DESC)
    WHERE status = 'live';

CREATE TABLE IF NOT EXISTS listing_text_conditions (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES auction_listings(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    assessment JSONB NOT NULL,
    text_grade TEXT,
    confidence NUMERIC,
    risk_flags TEXT[] NOT NULL DEFAULT '{}'::text[],
    review_required BOOLEAN NOT NULL DEFAULT TRUE,
    checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX IF NOT EXISTS listing_text_conditions_listing_latest_idx
    ON listing_text_conditions (listing_id, checked_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS listing_text_conditions_external_latest_idx
    ON listing_text_conditions (external_id, checked_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS listing_text_conditions_review_idx
    ON listing_text_conditions (review_required, checked_at DESC);

CREATE OR REPLACE VIEW latest_listing_text_conditions AS
SELECT DISTINCT ON (listing_id)
       id,
       listing_id,
       external_id,
       model_name,
       assessment,
       text_grade,
       confidence,
       risk_flags,
       review_required,
       checked_at
  FROM listing_text_conditions
 ORDER BY listing_id, checked_at DESC, id DESC;

CREATE TABLE IF NOT EXISTS listing_text_condition_queue (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES auction_listings(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL,
    source TEXT NOT NULL,
    status_name TEXT NOT NULL,
    priority_flag TEXT NOT NULL DEFAULT 'backfill',
    priority BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    queued_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    locked_at TIMESTAMP WITHOUT TIME ZONE,
    processed_at TIMESTAMP WITHOUT TIME ZONE
);

CREATE UNIQUE INDEX IF NOT EXISTS listing_text_condition_queue_listing_idx
    ON listing_text_condition_queue (listing_id);

CREATE INDEX IF NOT EXISTS listing_text_condition_queue_pending_idx
    ON listing_text_condition_queue (status, priority DESC, queued_at ASC);
