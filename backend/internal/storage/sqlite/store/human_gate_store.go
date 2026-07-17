package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// SaveHumanGate atomically inserts or updates one durable human gate.
func (s *Store) SaveHumanGate(ctx context.Context, gate domain.HumanGate) error {
	evidence, err := json.Marshal(gate.Evidence)
	if err != nil {
		return err
	}
	edges, err := json.Marshal(gate.DependencyEdges)
	if err != nil {
		return err
	}
	actions, err := json.Marshal(gate.AllowedActions)
	if err != nil {
		return err
	}
	var actor, actorClass, provenance, action, decision, resulting, failure string
	var resolvedAt time.Time
	if gate.Resolution != nil {
		actor, actorClass, provenance = gate.Resolution.Actor, string(gate.Resolution.ActorType), gate.Resolution.AuthorizationProvenance
		action, decision, resolvedAt, resulting = gate.Resolution.Action, gate.Resolution.Decision, gate.Resolution.AuthorizedAt, string(gate.Resolution.ResultingState)
		failure = gate.Resolution.FailureReason
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.qw.UpsertHumanGate(ctx, gen.UpsertHumanGateParams{
		GateID: gate.ID, DedupeKey: gate.DedupeKey, ProjectID: string(gate.ProjectID), SessionID: string(gate.SessionID), SourceGeneration: gate.SourceGeneration,
		ProfileHash: gate.ProfileHash, Reason: string(gate.Reason), RequiredDecision: gate.RequiredDecision, EvidenceJson: string(evidence), AffectedTaskID: gate.AffectedTaskID,
		DependencyEdgesJson: string(edges), AllowedActionsJson: string(actions), State: string(gate.State), DetectedAt: gate.DetectedAt, UpdatedAt: gate.UpdatedAt,
		NotifiedAt: nullableTime(gate.NotifiedAt), ReminderIntervalSeconds: int64(gate.ReminderInterval / time.Second), NextReminderAt: nullableTime(gate.NextReminderAt),
		EscalationAt: nullableTime(gate.EscalationAt), ReminderCount: int64(gate.ReminderCount), LastRemindedAt: nullableTime(gate.LastRemindedAt),
		ResolutionActor: actor, ResolutionActorClass: actorClass, ResolutionProvenance: provenance, ResolutionAction: action, ResolutionDecision: decision,
		ResolvedAt: nullableTime(resolvedAt), ResultingState: resulting, ResolutionFailure: failure,
	})
}

// GetHumanGate retrieves one gate by its stable identity.
func (s *Store) GetHumanGate(ctx context.Context, id string) (domain.HumanGate, bool, error) {
	row, err := s.qr.GetHumanGate(ctx, id)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.HumanGate{}, false, nil
	}
	if err != nil {
		return domain.HumanGate{}, false, err
	}
	gate, err := humanGateFromRow(row)
	return gate, err == nil, err
}

// GetOpenHumanGateForSession retrieves the protected gate for one session, if any.
func (s *Store) GetOpenHumanGateForSession(ctx context.Context, id domain.SessionID) (domain.HumanGate, bool, error) {
	row, err := s.qr.GetOpenHumanGateForSession(ctx, string(id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.HumanGate{}, false, nil
	}
	if err != nil {
		return domain.HumanGate{}, false, err
	}
	gate, err := humanGateFromRow(row)
	return gate, err == nil, err
}

// ListOpenHumanGates returns all durable gates that still protect their lanes.
func (s *Store) ListOpenHumanGates(ctx context.Context) ([]domain.HumanGate, error) {
	rows, err := s.qr.ListOpenHumanGates(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domain.HumanGate, 0, len(rows))
	for _, row := range rows {
		gate, convErr := humanGateFromRow(row)
		if convErr != nil {
			return nil, convErr
		}
		out = append(out, gate)
	}
	return out, nil
}

func humanGateFromRow(row gen.HumanGate) (domain.HumanGate, error) {
	gate := domain.HumanGate{ID: row.GateID, DedupeKey: row.DedupeKey, ProjectID: domain.ProjectID(row.ProjectID), SessionID: domain.SessionID(row.SessionID), SourceGeneration: row.SourceGeneration,
		ProfileHash: row.ProfileHash, Reason: domain.BlockReason(row.Reason), RequiredDecision: row.RequiredDecision, AffectedTaskID: row.AffectedTaskID, State: domain.GateState(row.State),
		DetectedAt: row.DetectedAt, UpdatedAt: row.UpdatedAt, NotifiedAt: nullTimeToTime(row.NotifiedAt), ReminderInterval: time.Duration(row.ReminderIntervalSeconds) * time.Second,
		NextReminderAt: nullTimeToTime(row.NextReminderAt), EscalationAt: nullTimeToTime(row.EscalationAt), ReminderCount: int(row.ReminderCount), LastRemindedAt: nullTimeToTime(row.LastRemindedAt)}
	if err := json.Unmarshal([]byte(row.EvidenceJson), &gate.Evidence); err != nil {
		return domain.HumanGate{}, fmt.Errorf("decode gate evidence: %w", err)
	}
	if err := json.Unmarshal([]byte(row.DependencyEdgesJson), &gate.DependencyEdges); err != nil {
		return domain.HumanGate{}, fmt.Errorf("decode gate dependencies: %w", err)
	}
	if err := json.Unmarshal([]byte(row.AllowedActionsJson), &gate.AllowedActions); err != nil {
		return domain.HumanGate{}, fmt.Errorf("decode gate actions: %w", err)
	}
	if row.ResolutionActor != "" {
		gate.Resolution = &domain.HumanGateResolution{Actor: row.ResolutionActor, ActorType: domain.GateActorType(row.ResolutionActorClass), AuthorizationProvenance: row.ResolutionProvenance,
			Action: row.ResolutionAction, Decision: row.ResolutionDecision, AuthorizedAt: nullTimeToTime(row.ResolvedAt), ResultingState: domain.GateState(row.ResultingState), FailureReason: row.ResolutionFailure}
	}
	return gate, nil
}
