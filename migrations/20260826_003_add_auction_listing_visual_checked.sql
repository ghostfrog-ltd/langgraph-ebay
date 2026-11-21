ALTER TABLE auction_listings
    ADD COLUMN IF NOT EXISTS visual_condition_checked BOOLEAN NOT NULL DEFAULT false;

ALTER TABLE auction_listings
    ADD COLUMN IF NOT EXISTS visual_condition_checked_at TIMESTAMP WITHOUT TIME ZONE;

UPDATE auction_listings AS a
   SET visual_condition_checked = true,
       visual_condition_checked_at = vc.checked_at
  FROM latest_listing_visual_conditions AS vc
 WHERE vc.listing_id = a.id
   AND a.visual_condition_checked = false;

CREATE INDEX IF NOT EXISTS auction_listings_visual_condition_checked_idx
    ON auction_listings (visual_condition_checked, first_seen DESC)
    WHERE status = 'live';
