-- Provision the grants only. The operator enables this role's login separately.
CREATE ROLE ghostfrog_mcp NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 4;

ALTER ROLE ghostfrog_mcp SET default_transaction_read_only = on;
ALTER ROLE ghostfrog_mcp SET statement_timeout = '5s';

GRANT USAGE ON SCHEMA public TO ghostfrog_mcp;
GRANT SELECT (
    id, external_id, source, title, raw_attrs, model_key, sale_type, condition_grade,
    status, finalized, end_time, last_seen_at, price_current, bids_count, final_price
) ON public.auction_listings TO ghostfrog_mcp;
GRANT SELECT (id, external_id, price, bids_count, recorded_at)
    ON public.auction_price_history TO ghostfrog_mcp;
GRANT SELECT (name, enabled, last_scraped_at, scrape_interval_seconds)
    ON public.sources TO ghostfrog_mcp;
GRANT SELECT (listing_id, text_grade, review_required)
    ON public.latest_listing_text_conditions TO ghostfrog_mcp;
GRANT SELECT (listing_id, visual_grade, review_required)
    ON public.latest_listing_visual_conditions TO ghostfrog_mcp;
GRANT SELECT (id, external_id, action, created_at)
    ON public.listing_review_actions TO ghostfrog_mcp;
