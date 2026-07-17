-- +goose Up
CREATE TABLE human_gates (
    gate_id TEXT PRIMARY KEY,
    dedupe_key TEXT NOT NULL UNIQUE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    source_generation TEXT NOT NULL,
    profile_hash TEXT NOT NULL,
    reason TEXT NOT NULL,
    required_decision TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    affected_task_id TEXT NOT NULL,
    dependency_edges_json TEXT NOT NULL,
    allowed_actions_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open', 'resolved', 'resumed')),
    detected_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    notified_at TIMESTAMP,
    reminder_interval_seconds INTEGER NOT NULL DEFAULT 0,
    next_reminder_at TIMESTAMP,
    escalation_at TIMESTAMP,
    reminder_count INTEGER NOT NULL DEFAULT 0,
    last_reminded_at TIMESTAMP,
    resolution_actor TEXT NOT NULL DEFAULT '',
    resolution_actor_class TEXT NOT NULL DEFAULT '',
    resolution_provenance TEXT NOT NULL DEFAULT '',
    resolution_action TEXT NOT NULL DEFAULT '',
    resolution_decision TEXT NOT NULL DEFAULT '',
    resolved_at TIMESTAMP,
    resulting_state TEXT NOT NULL DEFAULT '',
    resolution_failure TEXT NOT NULL DEFAULT ''
);

CREATE INDEX idx_human_gates_open ON human_gates(state, detected_at, session_id);
CREATE UNIQUE INDEX idx_human_gates_one_open_per_session ON human_gates(session_id) WHERE state = 'open';

-- Add human_gate to the locked notification type constraint.
ALTER TABLE notifications RENAME TO notifications_old;
CREATE TABLE notifications (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    pr_url TEXT NOT NULL DEFAULT '',
    type TEXT NOT NULL CHECK (type IN ('needs_input', 'ready_to_merge', 'pr_merged', 'pr_closed_unmerged', 'capacity_wait', 'human_gate')),
    title TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'unread' CHECK (status IN ('read', 'unread')),
    created_at TIMESTAMP NOT NULL
);
INSERT INTO notifications SELECT * FROM notifications_old;
DROP TABLE notifications_old;
CREATE INDEX idx_notifications_status ON notifications(status, created_at DESC);
CREATE UNIQUE INDEX idx_notifications_unread_dedupe ON notifications(session_id, type, pr_url) WHERE status = 'unread';

-- +goose StatementBegin
CREATE TRIGGER human_gates_cdc_insert
AFTER INSERT ON human_gates
BEGIN
    INSERT INTO change_log (project_id, session_id, event_type, payload, created_at)
    VALUES (NEW.project_id, NEW.session_id, 'session_updated', json_object('id', NEW.session_id, 'humanGateState', NEW.state), NEW.updated_at);
END;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TRIGGER human_gates_cdc_update
AFTER UPDATE ON human_gates
WHEN OLD.state <> NEW.state OR OLD.updated_at <> NEW.updated_at
BEGIN
    INSERT INTO change_log (project_id, session_id, event_type, payload, created_at)
    VALUES (NEW.project_id, NEW.session_id, 'session_updated', json_object('id', NEW.session_id, 'humanGateState', NEW.state), NEW.updated_at);
END;
-- +goose StatementEnd

-- +goose Down
DROP TRIGGER human_gates_cdc_update;
DROP TRIGGER human_gates_cdc_insert;
DELETE FROM notifications WHERE type = 'human_gate';
ALTER TABLE notifications RENAME TO notifications_new;
CREATE TABLE notifications (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    pr_url TEXT NOT NULL DEFAULT '',
    type TEXT NOT NULL CHECK (type IN ('needs_input', 'ready_to_merge', 'pr_merged', 'pr_closed_unmerged', 'capacity_wait')),
    title TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'unread' CHECK (status IN ('read', 'unread')),
    created_at TIMESTAMP NOT NULL
);
INSERT INTO notifications SELECT * FROM notifications_new;
DROP TABLE notifications_new;
CREATE INDEX idx_notifications_status ON notifications(status, created_at DESC);
CREATE UNIQUE INDEX idx_notifications_unread_dedupe ON notifications(session_id, type, pr_url) WHERE status = 'unread';
DROP INDEX idx_human_gates_open;
DROP INDEX idx_human_gates_one_open_per_session;
DROP TABLE human_gates;
