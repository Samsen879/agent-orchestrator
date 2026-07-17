-- name: InsertLifecycleReaction :execrows
INSERT INTO lifecycle_reactions (
    event_id, version, project_id, source_session_id, source_generation,
    issue_id, issue_url, pr_url, pr_number, repo, branch, head_sha,
    created_at, expires_at, successor_session_id, supersedes_event_id,
    idempotency_key, state, reason
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', '')
ON CONFLICT DO NOTHING;

-- name: GetLifecycleReactionByEventID :one
SELECT * FROM lifecycle_reactions WHERE event_id = ?;

-- name: GetLifecycleReactionByIdempotencyKey :one
SELECT * FROM lifecycle_reactions WHERE idempotency_key = ?;

-- name: SetLifecycleReactionState :execrows
UPDATE lifecycle_reactions
SET state = ?, reason = ?, successor_session_id = ?, delivered_at = ?
WHERE event_id = ? AND state = 'pending';

-- name: SupersedeLifecycleReaction :execrows
UPDATE lifecycle_reactions
SET state = 'superseded', reason = ?, successor_session_id = ?
WHERE event_id = ? AND state = 'pending';
