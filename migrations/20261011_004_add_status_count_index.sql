-- Speed up exact operator status counts without scanning the wider status/end index.

SET LOCAL statement_timeout = 0;

CREATE INDEX IF NOT EXISTS idx_auction_status_id
    ON auction_listings (status, id);
