-- Store listing condition separately from product identity.
--
-- Model keys should answer "what exact product is this?" Condition grade
-- answers "what state is it in?" This prevents condition suffixes from
-- fragmenting otherwise identical Apple/Mac identities.

ALTER TABLE auction_listings
    ADD COLUMN IF NOT EXISTS condition_grade TEXT;

ALTER TABLE comps
    ADD COLUMN IF NOT EXISTS condition_grade TEXT NOT NULL DEFAULT 'B';

CREATE INDEX IF NOT EXISTS idx_auction_model_condition_sold_final
    ON auction_listings (model_key, condition_grade)
    WHERE status = 'sold'
      AND final_price IS NOT NULL
      AND final_price > 0
      AND model_key IS NOT NULL
      AND model_key <> ''
      AND LOWER(model_key) <> 'unknown';

CREATE INDEX IF NOT EXISTS idx_auction_live_model_condition
    ON auction_listings (model_key, condition_grade)
    WHERE status = 'live'
      AND price_current IS NOT NULL
      AND price_current > 0
      AND model_key IS NOT NULL
      AND model_key <> ''
      AND LOWER(model_key) <> 'unknown';

DROP MATERIALIZED VIEW IF EXISTS latest_comps;

CREATE MATERIALIZED VIEW latest_comps AS
SELECT DISTINCT ON (model_key, condition_grade)
       model_key,
       condition_grade,
       median_final_price,
       mean_final_price,
       samples,
       computed_at
  FROM comps
 ORDER BY model_key, condition_grade, computed_at DESC;

CREATE INDEX IF NOT EXISTS idx_latest_comps_model_condition
    ON latest_comps (model_key, condition_grade);
