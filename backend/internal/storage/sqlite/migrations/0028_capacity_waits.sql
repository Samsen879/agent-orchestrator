-- +goose Up
CREATE TABLE capacity_waits (
    session_id TEXT PRIMARY KEY REFERENCES sessions(id) ON DELETE CASCADE,
    episode_id TEXT NOT NULL UNIQUE,
    project_id TEXT NOT NULL,
    source_generation TEXT NOT NULL,
    agent_session_id TEXT NOT NULL,
    workspace_path TEXT NOT NULL,
    branch TEXT NOT NULL,
    profile_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('capacity_wait', 'scheduled_probe', 'same_session_resume', 'recovered', 'failed_closed')),
    error_class TEXT NOT NULL,
    source_error TEXT NOT NULL,
    runtime_handle_id TEXT NOT NULL,
    output_fingerprint TEXT NOT NULL,
    next_probe_at TIMESTAMP NOT NULL,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    notified_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_capacity_waits_due ON capacity_waits(state, next_probe_at);

-- +goose StatementBegin
CREATE TRIGGER capacity_waits_cdc_insert
AFTER INSERT ON capacity_waits
BEGIN
    INSERT INTO change_log (project_id, session_id, event_type, payload, created_at)
    VALUES (NEW.project_id, NEW.session_id, 'session_updated',
        json_object('id', NEW.session_id, 'capacityWaitState', NEW.state), NEW.updated_at);
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER capacity_waits_cdc_update
AFTER UPDATE ON capacity_waits
WHEN OLD.state <> NEW.state OR OLD.next_probe_at <> NEW.next_probe_at OR OLD.attempt_count <> NEW.attempt_count
BEGIN
    INSERT INTO change_log (project_id, session_id, event_type, payload, created_at)
    VALUES (NEW.project_id, NEW.session_id, 'session_updated',
        json_object('id', NEW.session_id, 'capacityWaitState', NEW.state), NEW.updated_at);
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER capacity_waits_cdc_update;
DROP TRIGGER capacity_waits_cdc_insert;
DROP TABLE capacity_waits;
