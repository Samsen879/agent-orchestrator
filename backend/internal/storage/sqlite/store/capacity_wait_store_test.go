package store_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

func TestCapacityWaitSurvivesStoreRestart(t *testing.T) {
	ctx := context.Background()
	dataDir := filepath.Join(t.TempDir(), "ao")
	store, err := sqlite.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	if err := store.UpsertProject(ctx, domain.ProjectRecord{ID: "project", Path: "/tmp/project", RegisteredAt: now}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateSession(ctx, domain.SessionRecord{ProjectID: "project", Kind: domain.KindWorker, CreatedAt: now, UpdatedAt: now}); err != nil {
		t.Fatal(err)
	}
	wait := domain.CapacityWait{
		EpisodeID: "episode-1", ProjectID: "project", SessionID: "project-1", SourceGeneration: "generation-1",
		AgentSessionID: "thread-1", WorkspacePath: "/worktrees/project-1", Branch: "task/1602", ProfileHash: "profile-hash",
		State: domain.CapacityWaitScheduled, ErrorClass: domain.ProviderErrorCapacity, SourceError: "429 Too Many Requests",
		RuntimeHandleID: "runtime-1", OutputFingerprint: "fingerprint", NextProbeAt: now.Add(time.Minute), AttemptCount: 2,
		NotifiedAt: now, CreatedAt: now, UpdatedAt: now,
	}
	if err := store.SaveCapacityWait(ctx, wait); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := sqlite.Open(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	got, ok, err := reopened.GetCapacityWait(ctx, wait.SessionID)
	if err != nil || !ok {
		t.Fatalf("GetCapacityWait: ok=%v err=%v", ok, err)
	}
	if got.EpisodeID != wait.EpisodeID || got.SourceGeneration != wait.SourceGeneration || got.AgentSessionID != wait.AgentSessionID || got.ProfileHash != wait.ProfileHash || got.NextProbeAt != wait.NextProbeAt || got.AttemptCount != wait.AttemptCount || got.State != wait.State {
		t.Fatalf("reopened wait = %+v, want %+v", got, wait)
	}
	active, err := reopened.ListActiveCapacityWaits(ctx)
	if err != nil || len(active) != 1 {
		t.Fatalf("ListActiveCapacityWaits: len=%d err=%v", len(active), err)
	}
}
