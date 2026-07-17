package integration

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type fixedReactionPRResolver struct {
	target domain.LifecycleReactionPRTarget
}

func (r fixedReactionPRResolver) ResolveReactionPR(context.Context, domain.LifecycleReaction) (domain.LifecycleReactionPRTarget, error) {
	return r.target, nil
}

func TestLifecycleReactionRejectsStaleTargetsBeforeDelivery(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	t.Run("deleted source session is target missing", func(t *testing.T) {
		store := openReactionStore(t, t.TempDir(), now)
		manager := lifecycle.New(store, nil)
		reaction := testReaction("missing", domain.SessionRecord{
			ID: "project-404", ProjectID: "project", Metadata: domain.SessionMetadata{Generation: "generation-1", RuntimeHandleID: "runtime-1"},
		}, now)

		deliveries := 0
		outcome, err := manager.RouteReaction(ctx, reaction, func() error { deliveries++; return nil })
		if err != nil {
			t.Fatalf("RouteReaction: %v", err)
		}
		if outcome.State != domain.LifecycleReactionTargetMissing || deliveries != 0 {
			t.Fatalf("outcome = %+v, deliveries = %d", outcome, deliveries)
		}
		assertStoredReaction(t, store, reaction.EventID, domain.LifecycleReactionTargetMissing, reaction)
	})

	t.Run("generation N is superseded after generation N plus one", func(t *testing.T) {
		store := openReactionStore(t, t.TempDir(), now)
		source := createReactionSession(t, store, "", "generation-2", now)
		staleSource := source
		staleSource.Metadata.Generation = "generation-1"
		reaction := testReaction("old-generation", staleSource, now)

		deliveries := 0
		outcome, err := lifecycle.New(store, nil).RouteReaction(ctx, reaction, func() error { deliveries++; return nil })
		if err != nil {
			t.Fatalf("RouteReaction: %v", err)
		}
		if outcome.State != domain.LifecycleReactionSuperseded || deliveries != 0 {
			t.Fatalf("outcome = %+v, deliveries = %d", outcome, deliveries)
		}
	})

	t.Run("expired reaction is tombstoned", func(t *testing.T) {
		store := openReactionStore(t, t.TempDir(), now)
		source := createReactionSession(t, store, "", "generation-1", now)
		reaction := testReaction("expired", source, now.Add(-2*time.Hour))
		reaction.ExpiresAt = now.Add(-time.Hour)

		deliveries := 0
		outcome, err := lifecycle.New(store, nil).RouteReaction(ctx, reaction, func() error { deliveries++; return nil })
		if err != nil {
			t.Fatalf("RouteReaction: %v", err)
		}
		if outcome.State != domain.LifecycleReactionExpired || deliveries != 0 {
			t.Fatalf("outcome = %+v, deliveries = %d", outcome, deliveries)
		}
	})

	t.Run("stale worker handoff names the current successor", func(t *testing.T) {
		store := openReactionStore(t, t.TempDir(), now)
		oldWorker := createReactionSession(t, store, "1603", "generation-1", now)
		newWorker := createReactionSession(t, store, "1603", "generation-2", now.Add(time.Second))
		reaction := testReaction("stale-handoff", oldWorker, now)

		deliveries := 0
		outcome, err := lifecycle.New(store, nil).RouteReaction(ctx, reaction, func() error { deliveries++; return nil })
		if err != nil {
			t.Fatalf("RouteReaction: %v", err)
		}
		if outcome.State != domain.LifecycleReactionSuperseded || deliveries != 0 {
			t.Fatalf("outcome = %+v, deliveries = %d", outcome, deliveries)
		}
		stored, ok, err := store.GetLifecycleReaction(ctx, reaction.EventID)
		if err != nil || !ok {
			t.Fatalf("GetLifecycleReaction: ok=%v err=%v", ok, err)
		}
		if stored.SuccessorSessionID != newWorker.ID {
			t.Fatalf("successor = %q, want %q", stored.SuccessorSessionID, newWorker.ID)
		}

		current := testReaction("current-successor", newWorker, now.Add(time.Second))
		currentDeliveries := 0
		currentOutcome, err := lifecycle.New(store, nil).RouteReaction(ctx, current, func() error { currentDeliveries++; return nil })
		if err != nil {
			t.Fatalf("current RouteReaction: %v", err)
		}
		if currentOutcome.State != domain.LifecycleReactionDelivered || currentDeliveries != 1 {
			t.Fatalf("current outcome = %+v, deliveries = %d", currentOutcome, currentDeliveries)
		}
	})

	t.Run("stale live PR head fails closed", func(t *testing.T) {
		store := openReactionStore(t, t.TempDir(), now)
		source := createReactionSession(t, store, "1603", "generation-1", now)
		prURL := "https://github.com/acme/project/pull/1603"
		if err := store.WriteSCMObservation(ctx, domain.PullRequest{
			URL: prURL, SessionID: source.ID, Number: 1603, Repo: "acme/project", SourceBranch: source.Metadata.Branch, HeadSHA: "head-old", ObservedAt: now,
		}, nil, nil, nil, nil, ports.ReviewWritePreserve); err != nil {
			t.Fatalf("WriteSCMObservation: %v", err)
		}
		reaction := testReaction("stale-head", source, now)
		reaction.PRURL, reaction.PRNumber, reaction.Repo, reaction.HeadSHA = prURL, 1603, "acme/project", "head-old"
		manager := lifecycle.New(store, nil)
		manager.SetReactionPRResolver(fixedReactionPRResolver{target: domain.LifecycleReactionPRTarget{
			Found: true, URL: prURL, Number: 1603, Repo: "acme/project", SourceBranch: source.Metadata.Branch, HeadSHA: "head-new",
		}})

		deliveries := 0
		outcome, err := manager.RouteReaction(ctx, reaction, func() error { deliveries++; return nil })
		if err != nil {
			t.Fatalf("RouteReaction: %v", err)
		}
		if outcome.State != domain.LifecycleReactionSuperseded || deliveries != 0 {
			t.Fatalf("outcome = %+v, deliveries = %d", outcome, deliveries)
		}
	})
}

func TestLifecycleReactionExactlyOnceAndRestartTombstones(t *testing.T) {
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	t.Run("current reaction is delivered exactly once", func(t *testing.T) {
		store := openReactionStore(t, t.TempDir(), now)
		source := createReactionSession(t, store, "1603", "generation-1", now)
		reaction := testReaction("current", source, now)
		reaction.IssueURL = "https://github.com/acme/project/issues/1603"
		manager := lifecycle.New(store, nil)

		deliveries := 0
		for i := 0; i < 2; i++ {
			outcome, err := manager.RouteReaction(ctx, reaction, func() error { deliveries++; return nil })
			if err != nil {
				t.Fatalf("RouteReaction %d: %v", i, err)
			}
			if outcome.State != domain.LifecycleReactionDelivered {
				t.Fatalf("outcome %d = %+v", i, outcome)
			}
		}
		if deliveries != 1 {
			t.Fatalf("deliveries = %d, want 1", deliveries)
		}
		assertStoredReaction(t, store, reaction.EventID, domain.LifecycleReactionDelivered, reaction)
	})

	t.Run("restart preserves a target missing tombstone", func(t *testing.T) {
		dataDir := filepath.Join(t.TempDir(), "ao")
		store := openReactionStore(t, dataDir, now)
		reaction := testReaction("restart-missing", domain.SessionRecord{
			ID: "project-404", ProjectID: "project", Metadata: domain.SessionMetadata{Generation: "generation-1", RuntimeHandleID: "runtime-1"},
		}, now)
		firstDeliveries := 0
		if _, err := lifecycle.New(store, nil).RouteReaction(ctx, reaction, func() error { firstDeliveries++; return nil }); err != nil {
			t.Fatalf("first RouteReaction: %v", err)
		}
		if err := store.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}

		reopened, err := sqlite.Open(dataDir)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		t.Cleanup(func() { _ = reopened.Close() })
		secondDeliveries := 0
		outcome, err := lifecycle.New(reopened, nil).RouteReaction(ctx, reaction, func() error { secondDeliveries++; return nil })
		if err != nil {
			t.Fatalf("second RouteReaction: %v", err)
		}
		if outcome.State != domain.LifecycleReactionTargetMissing || firstDeliveries != 0 || secondDeliveries != 0 {
			t.Fatalf("outcome=%+v deliveries=%d/%d", outcome, firstDeliveries, secondDeliveries)
		}
	})
}

func openReactionStore(t *testing.T, dataDir string, now time.Time) *sqlite.Store {
	t.Helper()
	store, err := sqlite.Open(dataDir)
	if err != nil {
		t.Fatalf("sqlite.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	if err := store.UpsertProject(context.Background(), domain.ProjectRecord{ID: "project", Path: "/tmp/project", RegisteredAt: now}); err != nil {
		t.Fatalf("UpsertProject: %v", err)
	}
	return store
}

func createReactionSession(t *testing.T, store *sqlite.Store, issue domain.IssueID, generation string, now time.Time) domain.SessionRecord {
	t.Helper()
	rec, err := store.CreateSession(context.Background(), domain.SessionRecord{
		ProjectID: "project", IssueID: issue, Kind: domain.KindWorker,
		Metadata:  domain.SessionMetadata{Generation: generation, Branch: "task/1603", WorkspacePath: "/tmp/worktree", RuntimeHandleID: "runtime-" + generation},
		CreatedAt: now, UpdatedAt: now,
	})
	if err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	return rec
}

func testReaction(suffix string, source domain.SessionRecord, now time.Time) domain.LifecycleReaction {
	return domain.LifecycleReaction{
		Version: domain.LifecycleReactionVersion, EventID: "test:" + suffix, ProjectID: source.ProjectID,
		SourceSessionID: source.ID, SourceGeneration: source.Metadata.Generation, IssueID: source.IssueID,
		Branch: source.Metadata.Branch, CreatedAt: now, ExpiresAt: now.Add(time.Hour), IdempotencyKey: "idempotency:" + suffix,
	}
}

func assertStoredReaction(t *testing.T, store *sqlite.Store, eventID string, state domain.LifecycleReactionState, want domain.LifecycleReaction) {
	t.Helper()
	got, ok, err := store.GetLifecycleReaction(context.Background(), eventID)
	if err != nil || !ok {
		t.Fatalf("GetLifecycleReaction: ok=%v err=%v", ok, err)
	}
	if got.State != state || got.ProjectID != want.ProjectID || got.SourceSessionID != want.SourceSessionID || got.SourceGeneration != want.SourceGeneration ||
		got.IssueID != want.IssueID || got.IssueURL != want.IssueURL || got.Branch != want.Branch || got.HeadSHA != want.HeadSHA ||
		got.CreatedAt != want.CreatedAt || got.ExpiresAt != want.ExpiresAt || got.IdempotencyKey != want.IdempotencyKey {
		t.Fatalf("stored reaction = %+v, want state=%s identity=%+v", got, state, want)
	}
	if state != domain.LifecycleReactionDelivered && got.Reason == "" {
		t.Fatalf("tombstone reason is empty: %+v", got)
	}
}
