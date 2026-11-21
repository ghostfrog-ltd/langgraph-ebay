-- Grant only diagnostic read privileges. No business tables or rows are changed.
-- Login is configured separately on the Mac mini after review.
CREATE ROLE ghostfrog_diagnostics NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS CONNECTION LIMIT 3;
ALTER ROLE ghostfrog_diagnostics SET default_transaction_read_only = on;
ALTER ROLE ghostfrog_diagnostics SET statement_timeout = '8s';
ALTER ROLE ghostfrog_diagnostics SET temp_file_limit = '64MB';
GRANT USAGE ON SCHEMA public TO ghostfrog_diagnostics;

-- Older databases may omit some operational projections or optional columns.
DO $grants$
DECLARE
    target record;
    visible_columns text;
BEGIN
    FOR target IN SELECT * FROM (VALUES
        ('auction_listings', 'id source_id source external_id sale_type status finalized model_key condition_grade price_current price_bid_current bids_count final_price end_time time_left_s first_seen last_seen_at fetched_at roi_estimate max_bid storage_gb backfill_checked_at backfill_check_count backfill_last_reason visual_condition_checked visual_condition_checked_at text_condition_checked text_condition_checked_at'),
        ('auction_price_history', 'id external_id price bids_count timestamp recorded_at'),
        ('auction_price_history_orphans', 'original_id external_id price bids_count timestamp recorded_at archived_at archive_reason'),
        ('sources', 'id name type enabled created_at_utc updated_at_utc scrape_interval_seconds last_scraped_at'),
        ('listing_statuses', 'status lifecycle_stage is_live_like is_terminal created_at'),
        ('comps', 'model_key condition_grade median_final_price mean_final_price samples computed_at'),
        ('latest_comps', 'model_key condition_grade median_final_price mean_final_price samples computed_at'),
        ('listing_identity_v', 'listing_id external_id source_id source_label canonical_source_name sale_type first_seen'),
        ('listing_attribute_v', 'listing_id external_id source_id source_label model_key storage_gb'),
        ('listing_observation_v', 'listing_id external_id source_id source_label price_current price_bid_current bids_count end_time time_left_s first_seen last_seen_at fetched_at'),
        ('listing_outcome_v', 'listing_id external_id source_id source_label status lifecycle_stage is_live_like is_terminal finalized final_price final_or_latest_bids_count end_time'),
        ('listing_backfill_audit_v', 'listing_id external_id source_id source_label status finalized backfill_checked_at backfill_check_count backfill_last_reason'),
        ('listing_score_v', 'listing_id external_id source_id source_label model_key roi_estimate max_bid'),
        ('listing_ui_sync_v', 'listing_id external_id source_id source_label laravel_sync_state laravel_synced_at'),
        ('listing_text_conditions', 'id listing_id external_id text_grade confidence review_required checked_at'),
        ('listing_visual_conditions', 'id listing_id external_id visual_grade confidence review_required checked_at'),
        ('latest_listing_text_conditions', 'id listing_id external_id text_grade confidence review_required checked_at'),
        ('latest_listing_visual_conditions', 'id listing_id external_id visual_grade confidence review_required checked_at'),
        ('listing_text_condition_queue', 'id listing_id external_id source status_name priority_flag priority status attempts queued_at locked_at processed_at'),
        ('listing_visual_condition_queue', 'id listing_id external_id source priority_flag priority status attempts queued_at locked_at processed_at'),
        ('listing_assessments', 'id listing_id created_at verdict confidence recommended_max_bid'),
        ('listing_review_actions', 'id source external_id action created_at'),
        ('api_usage', 'id service call_count date created_at updated_at'),
        ('scrape_state', 'id last_scrape created_at updated_at'),
        ('pipeline_runtime_flags', 'name is_active updated_at'),
        ('roi_snapshots', 'id external_id source model_key current_price roi_estimate profit_estimate ends_at time_left_s created_at'),
        ('paper_trade_predictions', 'id external_id source model_key created_at updated_at prediction_date current_price predicted_resale_price predicted_fees predicted_outbound_shipping predicted_inbound_shipping predicted_packaging predicted_selling_reserve predicted_profit predicted_roi recommended_max_bid comp_samples end_time time_left_s decision_status observed_status observed_final_price observed_at observed_price_error observed_paper_profit observed_paper_roi outcome_reason outcome_checked_at outcome_check_count outcome_last_reason observed_purchase_price would_have_bid would_have_won'),
        ('alerts', 'id listing_external_id external_id snipe_score max_bid sent_at outcome roi_net model_key score created_at updated_at'),
        ('alert_state', 'name last_sent_at'),
        ('roi_alert_markers', 'external_id marker created_at'),
        ('mini_project_alerts', 'external_id source model_key price_current first_detected_at last_detected_at email_sent_at telegram_sent_at'),
        ('msx125_alerts', 'external_id source model_key price_current first_detected_at last_detected_at email_sent_at'),
        ('articles', 'id source_id published_at fetched_at created_at_utc updated_at_utc'),
        ('prices', 'id source_id symbol price currency fetched_at created_at_utc updated_at_utc'),
        ('schema_migrations', 'name applied_at')
    ) AS allowed(relation_name, column_names)
    LOOP
        SELECT string_agg(format('%I', a.attname), ', ' ORDER BY a.attnum)
        INTO visible_columns
        FROM pg_catalog.pg_class c
        JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
        WHERE n.nspname = 'public'
          AND c.relname = target.relation_name
          AND c.relkind IN ('r', 'p', 'v', 'm')
          AND a.attnum > 0 AND NOT a.attisdropped
          AND a.attname::text = ANY(string_to_array(target.column_names, ' '));
        IF visible_columns IS NOT NULL THEN
            EXECUTE format('GRANT SELECT (%s) ON public.%I TO ghostfrog_diagnostics',
                           visible_columns, target.relation_name);
        END IF;
    END LOOP;
END
$grants$;
