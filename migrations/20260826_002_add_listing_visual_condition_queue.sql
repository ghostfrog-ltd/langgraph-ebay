CREATE TABLE IF NOT EXISTS listing_visual_condition_queue (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES auction_listings(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL,
    source TEXT NOT NULL,
    product TEXT NOT NULL,
    priority_flag TEXT NOT NULL DEFAULT 'backfill',
    priority BIGINT NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    queued_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc'),
    locked_at TIMESTAMP WITHOUT TIME ZONE,
    processed_at TIMESTAMP WITHOUT TIME ZONE
);

ALTER TABLE listing_visual_condition_queue
    ADD COLUMN IF NOT EXISTS priority_flag TEXT NOT NULL DEFAULT 'backfill';

CREATE UNIQUE INDEX IF NOT EXISTS listing_visual_condition_queue_listing_idx
    ON listing_visual_condition_queue (listing_id);

CREATE INDEX IF NOT EXISTS listing_visual_condition_queue_pending_idx
    ON listing_visual_condition_queue (status, priority DESC, queued_at ASC);
