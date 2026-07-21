-- Drop deprecated Laravel/Herd synchronization state.
--
-- The Python ROI pipeline no longer posts opportunities to Laravel or keeps a
-- Laravel pending-sync queue. The Laravel application trees and scheduler have
-- also been removed, so these columns are no longer an operational contract.

DROP VIEW IF EXISTS listing_ui_sync_v;

ALTER TABLE auction_listings
    DROP COLUMN IF EXISTS laravel_sync_state,
    DROP COLUMN IF EXISTS laravel_synced_at,
    DROP COLUMN IF EXISTS laravel_sync_error,
    DROP COLUMN IF EXISTS laravel_sync_payload;
