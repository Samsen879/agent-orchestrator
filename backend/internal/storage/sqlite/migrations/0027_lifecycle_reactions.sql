-- +goose Up
CREATE TABLE lifecycle_reactions (
    event_id TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    project_id TEXT NOT NULL,
    source_session_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    issue_id TEXT NOT NULL DEFAULT '',
    issue_url TEXT NOT NULL DEFAULT '',
    pr_url TEXT NOT NULL DEFAULT '',
    pr_number INTEGER NOT NULL DEFAULT 0,
    repo TEXT NOT NULL DEFAULT '',
    branch TEXT NOT NULL DEFAULT '',
    head_sha TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMP NOT NULL,
    expires_at TIMESTAMP NOT NULL,
    successor_session_id TEXT NOT NULL DEFAULT '',
    supersedes_event_id TEXT NOT NULL DEFAULT '',
    idempotency_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'delivered', 'superseded', 'expired', 'target_missing')),
    reason TEXT NOT NULL DEFAULT '',
    delivered_at TIMESTAMP
);

CREATE INDEX idx_lifecycle_reactions_source
    ON lifecycle_reactions(source_session_id, source_generation, created_at);

-- +goose Down
DROP TABLE lifecycle_reactions;
