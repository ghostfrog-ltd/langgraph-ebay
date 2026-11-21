-- Speed up exact operator stats for final-price rows with usable product keys.

SET LOCAL statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_auction_final_price_known_model_key_id
    ON auction_listings (id)
    WHERE final_price IS NOT NULL
      AND final_price > 0
      AND model_key IS NOT NULL
      AND model_key <> ''
      AND LOWER(model_key) <> 'unknown'
      AND LOWER(model_key) NOT LIKE 'unknown%';
