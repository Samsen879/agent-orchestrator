-- name: UpsertHumanGate :exec
INSERT INTO human_gates (
    gate_id, dedupe_key, project_id, session_id, source_generation, profile_hash,
    reason, required_decision, evidence_json, affected_task_id, dependency_edges_json,
    allowed_actions_json, state, detected_at, updated_at, notified_at,
    reminder_interval_seconds, next_reminder_at, escalation_at, reminder_count,
    last_reminded_at, resolution_actor, resolution_actor_class, resolution_provenance,
    resolution_action, resolution_decision, resolved_at, resulting_state, resolution_failure
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(gate_id) DO UPDATE SET
    source_generation = excluded.source_generation,
    profile_hash = excluded.profile_hash,
    required_decision = excluded.required_decision,
    evidence_json = excluded.evidence_json,
    dependency_edges_json = excluded.dependency_edges_json,
    allowed_actions_json = excluded.allowed_actions_json,
    state = excluded.state,
    updated_at = excluded.updated_at,
    notified_at = excluded.notified_at,
    reminder_interval_seconds = excluded.reminder_interval_seconds,
    next_reminder_at = excluded.next_reminder_at,
    escalation_at = excluded.escalation_at,
    reminder_count = excluded.reminder_count,
    last_reminded_at = excluded.last_reminded_at,
    resolution_actor = excluded.resolution_actor,
    resolution_actor_class = excluded.resolution_actor_class,
    resolution_provenance = excluded.resolution_provenance,
    resolution_action = excluded.resolution_action,
    resolution_decision = excluded.resolution_decision,
    resolved_at = excluded.resolved_at,
    resulting_state = excluded.resulting_state,
    resolution_failure = excluded.resolution_failure;

-- name: GetHumanGate :one
SELECT * FROM human_gates WHERE gate_id = ?;

-- name: GetOpenHumanGateForSession :one
SELECT * FROM human_gates WHERE session_id = ? AND state = 'open' ORDER BY detected_at LIMIT 1;

-- name: ListOpenHumanGates :many
SELECT * FROM human_gates WHERE state = 'open' ORDER BY detected_at, gate_id;
