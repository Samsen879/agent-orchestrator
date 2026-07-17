-- +goose Up
-- 0029 is already shipped and immutable. Rebuild forward with explicit column
-- lists while adding the gate-scoped notification dedupe key.
DROP INDEX idx_notifications_unread_dedupe;
DROP INDEX idx_notifications_status;
ALTER TABLE notifications RENAME TO notifications_without_dedupe_key;
CREATE TABLE notifications (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    pr_url TEXT NOT NULL DEFAULT '',
    dedupe_key TEXT NOT NULL DEFAULT '',
    type TEXT NOT NULL CHECK (type IN ('needs_input', 'ready_to_merge', 'pr_merged', 'pr_closed_unmerged', 'capacity_wait', 'human_gate')),
    title TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'unread' CHECK (status IN ('read', 'unread')),
    created_at TIMESTAMP NOT NULL
);
INSERT INTO notifications (
    id, session_id, project_id, pr_url, dedupe_key, type, title, body, status, created_at
)
SELECT
    id, session_id, project_id, pr_url, '', type, title, body, status, created_at
FROM notifications_without_dedupe_key;
DROP TABLE notifications_without_dedupe_key;
CREATE INDEX idx_notifications_status ON notifications(status, created_at DESC);
CREATE UNIQUE INDEX idx_notifications_unread_dedupe
    ON notifications(session_id, type, pr_url, dedupe_key)
    WHERE status = 'unread';

-- +goose Down
DROP INDEX idx_notifications_unread_dedupe;
DROP INDEX idx_notifications_status;
ALTER TABLE notifications RENAME TO notifications_with_dedupe_key;
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
INSERT INTO notifications (
    id, session_id, project_id, pr_url, type, title, body, status, created_at
)
SELECT
    id, session_id, project_id, pr_url, type, title, body, status, created_at
FROM notifications_with_dedupe_key;
DROP TABLE notifications_with_dedupe_key;
DELETE FROM notifications AS notification
WHERE notification.status = 'unread'
  AND EXISTS (
      SELECT 1
      FROM notifications AS newer
      WHERE newer.session_id = notification.session_id
        AND newer.type = notification.type
        AND newer.pr_url = notification.pr_url
        AND newer.status = 'unread'
        AND (
            newer.created_at > notification.created_at
            OR (newer.created_at = notification.created_at AND newer.id > notification.id)
        )
  );
CREATE INDEX idx_notifications_status ON notifications(status, created_at DESC);
CREATE UNIQUE INDEX idx_notifications_unread_dedupe
    ON notifications(session_id, type, pr_url)
    WHERE status = 'unread';
