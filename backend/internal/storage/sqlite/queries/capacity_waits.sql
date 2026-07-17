-- name: UpsertCapacityWait :exec
INSERT INTO capacity_waits (
    session_id, episode_id, project_id, source_generation, agent_session_id,
    workspace_path, branch, profile_hash, state, error_class, source_error,
    runtime_handle_id, output_fingerprint, next_probe_at, attempt_count,
    notified_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(session_id) DO UPDATE SET
    episode_id = excluded.episode_id,
    project_id = excluded.project_id,
    source_generation = excluded.source_generation,
    agent_session_id = excluded.agent_session_id,
    workspace_path = excluded.workspace_path,
    branch = excluded.branch,
    profile_hash = excluded.profile_hash,
    state = excluded.state,
    error_class = excluded.error_class,
    source_error = excluded.source_error,
    runtime_handle_id = excluded.runtime_handle_id,
    output_fingerprint = excluded.output_fingerprint,
    next_probe_at = excluded.next_probe_at,
    attempt_count = excluded.attempt_count,
    notified_at = excluded.notified_at,
    created_at = excluded.created_at,
    updated_at = excluded.updated_at;

-- name: GetCapacityWait :one
SELECT * FROM capacity_waits WHERE session_id = ?;

-- name: ListActiveCapacityWaits :many
SELECT * FROM capacity_waits
WHERE state IN ('capacity_wait', 'scheduled_probe', 'same_session_resume')
ORDER BY next_probe_at, session_id;
