"""Column-level allowlist for GhostFrog operational PostgreSQL diagnostics."""

# Values are physical columns. Text is coded except in the sanitized evidence view.
RELATIONS = {
    "auction_listings": """
        id source_id source external_id sale_type status finalized model_key condition_grade
        price_current price_bid_current bids_count final_price end_time time_left_s first_seen
        last_seen_at fetched_at roi_estimate max_bid storage_gb backfill_checked_at
        backfill_check_count backfill_last_reason visual_condition_checked
        visual_condition_checked_at text_condition_checked text_condition_checked_at
    """,
    "auction_price_history": "id external_id price bids_count timestamp recorded_at",
    "apple_listing_evidence_v": """
        listing_id external_id source status model_key condition_grade final_price end_time
        last_seen_at fetched_at evidence_seq field value_index part_index part_count product_value
    """,
    "listing_inspection_v": """
        listing_id external_id item_id source source_id title url url_withheld sale_type status finalized
        first_seen last_seen_at fetched_at price_current price_bid_current bids_count end_time
        condition_grade effective_condition_grade final_price model_key storage_gb brand product_family
        model_name colour roi_estimate max_bid backfill_checked_at backfill_check_count backfill_last_reason
        text_condition_checked text_condition_checked_at visual_condition_checked visual_condition_checked_at
        text_grade visual_grade text_review_required visual_review_required identity_risk
        browse_description_chars stored_gallery_count stored_shipping_option_count
    """,
    "listing_inspection_evidence_v": """
        listing_id section evidence_seq field value_path_text part_index part_count value_text withheld
    """,
    "listing_inspection_checks_v": """
        listing_id kind checked_at grade confidence review_required model_name verdict recommended_max_bid
    """,
    "auction_price_history_orphans": """
        original_id external_id price bids_count timestamp recorded_at archived_at archive_reason
    """,
    "sources": """
        id name type enabled created_at_utc updated_at_utc scrape_interval_seconds last_scraped_at
    """,
    "listing_statuses": "status lifecycle_stage is_live_like is_terminal created_at",
    "comps": "model_key condition_grade median_final_price mean_final_price samples computed_at",
    "latest_comps": "model_key condition_grade median_final_price mean_final_price samples computed_at",
    "listing_identity_v": "listing_id external_id source_id source_label canonical_source_name sale_type first_seen",
    "listing_attribute_v": "listing_id external_id source_id source_label model_key storage_gb",
    "listing_observation_v": """
        listing_id external_id source_id source_label price_current price_bid_current bids_count
        end_time time_left_s first_seen last_seen_at fetched_at
    """,
    "listing_outcome_v": """
        listing_id external_id source_id source_label status lifecycle_stage is_live_like is_terminal
        finalized final_price final_or_latest_bids_count end_time
    """,
    "listing_backfill_audit_v": """
        listing_id external_id source_id source_label status finalized backfill_checked_at
        backfill_check_count backfill_last_reason
    """,
    "listing_score_v": "listing_id external_id source_id source_label model_key roi_estimate max_bid",
    "listing_ui_sync_v": "listing_id external_id source_id source_label laravel_sync_state laravel_synced_at",
    "listing_text_conditions": "id listing_id external_id text_grade confidence review_required checked_at",
    "listing_visual_conditions": "id listing_id external_id visual_grade confidence review_required checked_at",
    "latest_listing_text_conditions": "id listing_id external_id text_grade confidence review_required checked_at",
    "latest_listing_visual_conditions": "id listing_id external_id visual_grade confidence review_required checked_at",
    "listing_text_condition_queue": """
        id listing_id external_id source status_name priority_flag priority status attempts queued_at locked_at processed_at
    """,
    "listing_visual_condition_queue": """
        id listing_id external_id source priority_flag priority status attempts queued_at locked_at processed_at
    """,
    "listing_assessments": "id listing_id created_at verdict confidence recommended_max_bid",
    "listing_review_actions": "id source external_id action created_at",
    "api_usage": "id service call_count date created_at updated_at",
    "scrape_state": "id last_scrape created_at updated_at",
    "pipeline_runtime_flags": "name is_active updated_at",
    "roi_snapshots": """
        id external_id source model_key current_price roi_estimate profit_estimate ends_at time_left_s created_at
    """,
    "paper_trade_predictions": """
        id external_id source model_key created_at updated_at prediction_date current_price predicted_resale_price
        predicted_fees predicted_outbound_shipping predicted_inbound_shipping predicted_packaging
        predicted_selling_reserve predicted_profit predicted_roi recommended_max_bid comp_samples end_time
        time_left_s decision_status observed_status observed_final_price observed_at observed_price_error
        observed_paper_profit observed_paper_roi outcome_reason outcome_checked_at outcome_check_count
        outcome_last_reason observed_purchase_price would_have_bid would_have_won
    """,
    "alerts": """
        id listing_external_id external_id snipe_score max_bid sent_at outcome roi_net model_key score created_at updated_at
    """,
    "alert_state": "name last_sent_at",
    "roi_alert_markers": "external_id marker created_at",
    "mini_project_alerts": """
        external_id source model_key price_current first_detected_at last_detected_at email_sent_at telegram_sent_at
    """,
    "msx125_alerts": "external_id source model_key price_current first_detected_at last_detected_at email_sent_at",
    "articles": "id source_id published_at fetched_at created_at_utc updated_at_utc",
    "prices": "id source_id symbol price currency fetched_at created_at_utc updated_at_utc",
    "schema_migrations": "name applied_at",
}

# PostgreSQL sanitizes these specific columns before they are visible to the login.
READABLE_TEXT = {
    "apple_listing_evidence_v": {"field", "product_value"},
    "listing_inspection_v": {"item_id", "title", "url", "brand", "product_family", "model_name", "colour"},
    "listing_inspection_evidence_v": {"field", "value_path_text", "value_text"},
    "listing_inspection_checks_v": {"model_name", "verdict"},
}

# Composite joins shown here describe how the stored keys are used. For actual
# constraints the catalog tool separately reads PostgreSQL's foreign keys.
LOGICAL_RELATIONSHIPS = (
    ("auction_listings", ("status",), "listing_statuses", ("status",)),
    ("auction_price_history", ("external_id",), "auction_listings", ("external_id",)),
    ("auction_price_history_orphans", ("external_id",), "auction_listings", ("external_id",)),
    ("auction_listings", ("model_key", "condition_grade"), "latest_comps", ("model_key", "condition_grade")),
    ("apple_listing_evidence_v", ("listing_id",), "auction_listings", ("id",)),
    ("listing_inspection_v", ("listing_id",), "auction_listings", ("id",)),
    ("listing_inspection_evidence_v", ("listing_id",), "listing_inspection_v", ("listing_id",)),
    ("listing_inspection_checks_v", ("listing_id",), "listing_inspection_v", ("listing_id",)),
    ("listing_text_conditions", ("listing_id",), "auction_listings", ("id",)),
    ("listing_visual_conditions", ("listing_id",), "auction_listings", ("id",)),
    ("listing_text_condition_queue", ("listing_id",), "auction_listings", ("id",)),
    ("listing_visual_condition_queue", ("listing_id",), "auction_listings", ("id",)),
    ("listing_assessments", ("listing_id",), "auction_listings", ("id",)),
    ("listing_review_actions", ("external_id",), "auction_listings", ("external_id",)),
    ("paper_trade_predictions", ("external_id",), "auction_listings", ("external_id",)),
    ("roi_snapshots", ("external_id",), "auction_listings", ("external_id",)),
)
