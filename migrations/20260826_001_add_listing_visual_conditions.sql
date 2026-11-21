CREATE TABLE IF NOT EXISTS listing_visual_conditions (
    id BIGSERIAL PRIMARY KEY,
    listing_id BIGINT NOT NULL REFERENCES auction_listings(id) ON DELETE CASCADE,
    external_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    image_urls JSONB NOT NULL DEFAULT '[]'::jsonb,
    assessment JSONB NOT NULL,
    visual_grade TEXT,
    confidence NUMERIC,
    damage_flags TEXT[] NOT NULL DEFAULT '{}'::text[],
    review_required BOOLEAN NOT NULL DEFAULT TRUE,
    checked_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX IF NOT EXISTS listing_visual_conditions_listing_latest_idx
    ON listing_visual_conditions (listing_id, checked_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS listing_visual_conditions_external_latest_idx
    ON listing_visual_conditions (external_id, checked_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS listing_visual_conditions_review_idx
    ON listing_visual_conditions (review_required, checked_at DESC);

CREATE OR REPLACE VIEW latest_listing_visual_conditions AS
SELECT DISTINCT ON (listing_id)
       id,
       listing_id,
       external_id,
       model_name,
       image_urls,
       assessment,
       visual_grade,
       confidence,
       damage_flags,
       review_required,
       checked_at
  FROM listing_visual_conditions
 ORDER BY listing_id, checked_at DESC, id DESC;
