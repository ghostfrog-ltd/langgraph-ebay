-- Speed up the five-minute listing heartbeat.
--
-- These indexes target the read paths that can otherwise scan the wide
-- auction_listings table during every heartbeat: missing attributes, ended
-- listing checks, stale final-price recovery planning, and live refresh/stale
-- marking. They do not change data or marketplace state.

CREATE INDEX IF NOT EXISTS idx_auction_raw_attrs_null_id_desc
    ON auction_listings (id DESC)
    INCLUDE (external_id, source, title)
    WHERE raw_attrs IS NULL;

CREATE INDEX IF NOT EXISTS idx_auction_unfinalized_live_end_time
    ON auction_listings (end_time ASC, id)
    INCLUDE (external_id, source)
    WHERE finalized = FALSE
      AND status IN ('active', 'ending', 'live')
      AND end_time IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_auction_live_model_key_source
    ON auction_listings (source, model_key)
    WHERE status = 'live'
      AND model_key IS NOT NULL
      AND model_key <> '';

CREATE INDEX IF NOT EXISTS idx_auction_stale_backfill_candidates
    ON auction_listings (
        source,
        model_key,
        backfill_checked_at ASC NULLS FIRST,
        last_seen_at DESC NULLS LAST,
        end_time DESC NULLS LAST
    )
    INCLUDE (
        id,
        external_id,
        title,
        price_current,
        backfill_check_count,
        backfill_last_reason
    )
    WHERE status = 'stale'
      AND COALESCE(finalized, FALSE) = FALSE
      AND end_time IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_auction_backfill_checked_at
    ON auction_listings (backfill_checked_at DESC)
    INCLUDE (status, finalized, backfill_last_reason)
    WHERE backfill_checked_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_auction_live_refresh_end_seen
    ON auction_listings (end_time ASC, last_seen_at, source_id)
    INCLUDE (external_id)
    WHERE status = 'live'
      AND COALESCE(finalized, FALSE) = FALSE
      AND end_time IS NOT NULL
      AND last_seen_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_auction_live_last_seen
    ON auction_listings (last_seen_at)
    WHERE status = 'live'
      AND last_seen_at IS NOT NULL;
