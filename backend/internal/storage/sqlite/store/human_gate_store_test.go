package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func TestHumanGatePersistsAcrossRestartWithEvidenceAndEdges(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	s, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	if err := s.UpsertProject(ctx, domain.ProjectRecord{ID: "project", Path: "/tmp/project", RegisteredAt: now}); err != nil {
		t.Fatal(err)
	}
	rec, err := s.CreateSession(ctx, domain.SessionRecord{ProjectID: "project", Kind: domain.KindWorker, Metadata: domain.SessionMetadata{Generation: "g1", ExecutionProfile: domain.ExecutionProfile{Hash: "profile-1"}}, CreatedAt: now, UpdatedAt: now})
	if err != nil {
		t.Fatal(err)
	}
	gate := domain.HumanGate{ID: "gate-1", DedupeKey: "key-1", ProjectID: "project", SessionID: rec.ID, SourceGeneration: "g1", ProfileHash: "profile-1", Reason: domain.BlockReasonMissingCredentials,
		RequiredDecision: "provide token", Evidence: []domain.GateEvidence{{Kind: "error", Source: "github", Detail: "401"}}, AffectedTaskID: "task-a",
		DependencyEdges: []domain.GateDependencyEdge{{FromTaskID: "task-a", ToTaskID: "task-b"}}, AllowedActions: []string{"credentials-provided"}, State: domain.GateOpen,
		DetectedAt: now, UpdatedAt: now, ReminderInterval: time.Hour, NextReminderAt: now.Add(time.Hour), EscalationAt: now.Add(24 * time.Hour)}
	if err := s.SaveHumanGate(ctx, gate); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sqlite.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	got, ok, err := reopened.GetHumanGate(ctx, gate.ID)
	if err != nil || !ok || got.RequiredDecision != gate.RequiredDecision || len(got.Evidence) != 1 || len(got.DependencyEdges) != 1 || got.NextReminderAt != gate.NextReminderAt || got.EscalationAt != gate.EscalationAt {
		t.Fatalf("rehydrated gate=%+v ok=%v err=%v", got, ok, err)
	}
}

func TestHumanGateRebindUpdatesIdentityWithoutOpenGateConflict(t *testing.T) {
	ctx := context.Background()
	s := newTestStore(t)
	seedProject(t, s, "project")
	rec, err := s.CreateSession(ctx, domain.SessionRecord{ProjectID: "project", Kind: domain.KindWorker, Metadata: domain.SessionMetadata{Generation: "g1", ExecutionProfile: domain.ExecutionProfile{Hash: "profile-1"}}, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	gate := domain.HumanGate{ID: "gate-rebind", DedupeKey: "gate-rebind", ProjectID: "project", SessionID: rec.ID, SourceGeneration: "g1", ProfileHash: "profile-1", Reason: domain.BlockReasonProductDecision,
		RequiredDecision: "choose", Evidence: []domain.GateEvidence{{Kind: "request", Source: "worker", Detail: "choose"}}, AffectedTaskID: "task-a", AllowedActions: []string{"choose-a"}, State: domain.GateOpen, DetectedAt: now, UpdatedAt: now}
	if err := s.SaveHumanGate(ctx, gate); err != nil {
		t.Fatal(err)
	}
	gate.SourceGeneration, gate.ProfileHash, gate.UpdatedAt = "g2", "profile-2", now.Add(time.Minute)
	gate.Evidence = append(gate.Evidence, domain.GateEvidence{Kind: "identity_rebound", Source: "gate_manager", Detail: "g1 to g2"})
	if err := s.SaveHumanGate(ctx, gate); err != nil {
		t.Fatalf("rebind save: %v", err)
	}
	got, ok, err := s.GetOpenHumanGateForSession(ctx, rec.ID)
	if err != nil || !ok || got.ID != gate.ID || got.SourceGeneration != "g2" || got.ProfileHash != "profile-2" || len(got.Evidence) != 2 {
		t.Fatalf("rebound gate=%+v ok=%v err=%v", got, ok, err)
	}
}
