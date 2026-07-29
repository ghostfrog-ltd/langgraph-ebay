CREATE TABLE IF NOT EXISTS listing_review_actions (
    id BIGSERIAL PRIMARY KEY,
    source TEXT,
    external_id TEXT NOT NULL,
    title TEXT,
    url TEXT,
    action TEXT NOT NULL CHECK (action IN ('up', 'down')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX IF NOT EXISTS listing_review_actions_latest_idx
    ON listing_review_actions (external_id, created_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS listing_review_actions_action_idx
    ON listing_review_actions (action, created_at DESC);
