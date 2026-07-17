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
