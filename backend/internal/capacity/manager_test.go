package capacity

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite"
)

type fakeStore struct {
	mu       sync.Mutex
	waits    map[domain.SessionID]domain.CapacityWait
	sessions map[domain.SessionID]domain.SessionRecord
}

func (s *fakeStore) SaveCapacityWait(_ context.Context, wait domain.CapacityWait) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waits == nil {
		s.waits = map[domain.SessionID]domain.CapacityWait{}
	}
	s.waits[wait.SessionID] = wait
	return nil
}

func (s *fakeStore) GetCapacityWait(_ context.Context, id domain.SessionID) (domain.CapacityWait, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	wait, ok := s.waits[id]
	return wait, ok, nil
}

func (s *fakeStore) ListActiveCapacityWaits(context.Context) ([]domain.CapacityWait, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var waits []domain.CapacityWait
	for _, wait := range s.waits {
		if wait.State.Active() {
			waits = append(waits, wait)
		}
	}
	return waits, nil
}

func (s *fakeStore) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.sessions[id]
	return rec, ok, nil
}

func (s *fakeStore) setSession(rec domain.SessionRecord) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions[rec.ID] = rec
}

type fakeRouter struct {
	reactions []domain.LifecycleReaction
	state     domain.LifecycleReactionState
}

func (r *fakeRouter) RouteReaction(_ context.Context, reaction domain.LifecycleReaction, deliver func() error) (lifecycle.ReactionRouteOutcome, error) {
	r.reactions = append(r.reactions, reaction)
	if r.state != "" && r.state != domain.LifecycleReactionDelivered {
		return lifecycle.ReactionRouteOutcome{State: r.state}, nil
	}
	if err := deliver(); err != nil {
		return lifecycle.ReactionRouteOutcome{}, err
	}
	return lifecycle.ReactionRouteOutcome{State: domain.LifecycleReactionDelivered, Delivered: true}, nil
}

type fakeResumer struct {
	store  *fakeStore
	errs   []error
	calls  []domain.CapacityWait
	called chan struct{}
}

func (r *fakeResumer) ResumeCapacity(_ context.Context, wait domain.CapacityWait) error {
	r.calls = append(r.calls, wait)
	if r.called != nil {
		select {
		case r.called <- struct{}{}:
		default:
		}
	}
	if len(r.errs) > 0 {
		err := r.errs[0]
		r.errs = r.errs[1:]
		if err != nil {
			return err
		}
	}
	rec, ok, _ := r.store.GetSession(context.Background(), wait.SessionID)
	if ok {
		rec.Metadata.RuntimeHandleID = "runtime-resumed"
		r.store.setSession(rec)
	}
	return nil
}

type fakeNotifier struct {
	intents []ports.NotificationIntent
}

func (n *fakeNotifier) Notify(_ context.Context, intent ports.NotificationIntent) error {
	n.intents = append(n.intents, intent)
	return nil
}

func TestClassifyProviderErrorSeparatesFailClosedFailures(t *testing.T) {
	tests := []struct {
		input string
		want  domain.ProviderErrorClass
	}{
		{"Error: 429 Too Many Requests", domain.ProviderErrorCapacity},
		{"provider capacity is temporarily unavailable", domain.ProviderErrorCapacity},
		{"401 Unauthorized: invalid API key", domain.ProviderErrorAuthentication},
		{"request rejected by content policy", domain.ProviderErrorPolicy},
		{"model not found in configuration", domain.ProviderErrorConfiguration},
		{"execution profile drift: profile hash mismatch", domain.ProviderErrorMalformedProfile},
		{"ordinary tool failure", domain.ProviderErrorUnknown},
	}
	for _, tt := range tests {
		if got, _ := ClassifyProviderError(tt.input); got != tt.want {
			t.Fatalf("ClassifyProviderError(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestBackoffAddsJitterWithoutExceedingCeiling(t *testing.T) {
	manager := New(nil, nil, nil, nil, Config{
		BaseDelay: time.Minute,
		Ceiling:   10 * time.Minute,
		Jitter:    func(maxDelay time.Duration) time.Duration { return maxDelay },
	})
	if got := manager.backoff(0); got != 75*time.Second {
		t.Fatalf("first delay = %s, want 1m15s", got)
	}
	if got := manager.backoff(3); got != 10*time.Minute {
		t.Fatalf("bounded jitter delay = %s, want 10m", got)
	}
	if got := manager.backoff(20); got != 10*time.Minute {
		t.Fatalf("ceiling delay = %s, want 10m", got)
	}
}

func TestObserveOutputPersistsIdentityAndDeduplicatesEpisode(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	store, source := capacityFixture(t, now)
	router := &fakeRouter{}
	notifier := &fakeNotifier{}
	manager := New(store, router, &fakeResumer{store: store}, notifier, Config{Clock: func() time.Time { return now }, BaseDelay: time.Minute, Ceiling: 10 * time.Minute, Jitter: func(time.Duration) time.Duration { return 0 }})

	output := "Error: provider capacity is temporarily unavailable"
	if err := manager.ObserveOutput(ctx, source, output, now); err != nil {
		t.Fatalf("ObserveOutput: %v", err)
	}
	if err := manager.ObserveOutput(ctx, source, output, now.Add(time.Second)); err != nil {
		t.Fatalf("repeat ObserveOutput: %v", err)
	}
	wait, ok, _ := store.GetCapacityWait(ctx, source.ID)
	if !ok {
		t.Fatal("capacity wait was not persisted")
	}
	if wait.State != domain.CapacityWaitWaiting || wait.ProjectID != source.ProjectID || wait.SessionID != source.ID || wait.SourceGeneration != source.Metadata.Generation ||
		wait.AgentSessionID != source.Metadata.AgentSessionID || wait.WorkspacePath != source.Metadata.WorkspacePath || wait.Branch != source.Metadata.Branch ||
		wait.ProfileHash != source.Metadata.ExecutionProfile.Hash || wait.NextProbeAt != now.Add(time.Minute) || wait.AttemptCount != 0 {
		t.Fatalf("persisted wait = %+v", wait)
	}
	if len(notifier.intents) != 1 || notifier.intents[0].Type != domain.NotificationCapacityWait {
		t.Fatalf("notifications = %+v, want one capacity notification", notifier.intents)
	}
	if len(router.reactions) != 2 || router.reactions[0].SourceGeneration != source.Metadata.Generation {
		t.Fatalf("reaction identities = %+v", router.reactions)
	}
}

func TestCapacityRecoveryReschedulesThenResumesSameIdentity(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	store, source := capacityFixture(t, now)
	clock := now
	resumer := &fakeResumer{store: store, errs: []error{errors.New("rate_limit_exceeded"), nil}}
	manager := New(store, &fakeRouter{}, resumer, &fakeNotifier{}, Config{Clock: func() time.Time { return clock }, BaseDelay: time.Minute, Ceiling: 2 * time.Minute, Jitter: func(time.Duration) time.Duration { return 0 }})
	if err := manager.ObserveOutput(ctx, source, "429 Too Many Requests", now); err != nil {
		t.Fatal(err)
	}

	clock = now.Add(time.Minute)
	if err := manager.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	wait, _, _ := store.GetCapacityWait(ctx, source.ID)
	if wait.State != domain.CapacityWaitScheduled || wait.AttemptCount != 1 || wait.NextProbeAt != clock.Add(2*time.Minute) {
		t.Fatalf("continued-capacity wait = %+v", wait)
	}

	clock = wait.NextProbeAt
	if err := manager.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	wait, _, _ = store.GetCapacityWait(ctx, source.ID)
	if wait.State != domain.CapacityWaitResuming || len(resumer.calls) != 2 {
		t.Fatalf("resume state=%+v calls=%d", wait, len(resumer.calls))
	}
	for _, call := range resumer.calls {
		if call.SessionID != source.ID || call.AgentSessionID != source.Metadata.AgentSessionID || call.WorkspacePath != source.Metadata.WorkspacePath || call.Branch != source.Metadata.Branch || call.ProfileHash != source.Metadata.ExecutionProfile.Hash {
			t.Fatalf("resume changed identity: %+v", call)
		}
	}
	current, _, _ := store.GetSession(ctx, source.ID)
	if err := manager.ObserveActivity(ctx, current, ports.ActivitySignal{Valid: true, State: domain.ActivityActive}); err != nil {
		t.Fatal(err)
	}
	wait, _, _ = store.GetCapacityWait(ctx, source.ID)
	if wait.State != domain.CapacityWaitRecovered {
		t.Fatalf("recovered state = %q", wait.State)
	}
}

func TestRestartRehydratesDueWaitWithoutChangingReviewProfile(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	store, source := capacityFixture(t, now)
	first := New(store, &fakeRouter{}, &fakeResumer{store: store}, &fakeNotifier{}, Config{Clock: func() time.Time { return now }, BaseDelay: time.Minute, Jitter: func(time.Duration) time.Duration { return 0 }})
	if err := first.ObserveOutput(ctx, source, "model is temporarily unavailable", now); err != nil {
		t.Fatal(err)
	}

	resumer := &fakeResumer{store: store}
	restarted := New(store, &fakeRouter{}, resumer, &fakeNotifier{}, Config{Clock: func() time.Time { return now.Add(time.Minute) }, BaseDelay: time.Minute, Jitter: func(time.Duration) time.Duration { return 0 }})
	if err := restarted.RunDue(ctx); err != nil {
		t.Fatal(err)
	}
	if len(resumer.calls) != 1 {
		t.Fatalf("restart resume calls = %d, want 1", len(resumer.calls))
	}
	profile := source.Metadata.ExecutionProfile
	if profile.Model != "gpt-5.4" || profile.ReasoningEffort != "high" || !profile.FastMode || profile.ReviewModel != "gpt-5.4-review" || resumer.calls[0].ProfileHash != profile.Hash {
		t.Fatalf("immutable worker/review profile changed: profile=%+v call=%+v", profile, resumer.calls[0])
	}
}

func TestRestartArmsOneTimerAndRunsPersistedDeadline(t *testing.T) {
	ctx := context.Background()
	store, source := capacityFixture(t, time.Now().UTC())
	first := New(store, &fakeRouter{}, &fakeResumer{store: store}, &fakeNotifier{}, Config{BaseDelay: 25 * time.Millisecond, Ceiling: time.Second, Jitter: func(time.Duration) time.Duration { return 0 }})
	if err := first.ObserveOutput(ctx, source, "429 Too Many Requests", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}

	called := make(chan struct{}, 1)
	resumer := &fakeResumer{store: store, called: called}
	restarted := New(store, &fakeRouter{}, resumer, &fakeNotifier{}, Config{BaseDelay: 25 * time.Millisecond, Ceiling: time.Second, Jitter: func(time.Duration) time.Duration { return 0 }})
	runCtx, cancel := context.WithCancel(context.Background())
	done := restarted.Start(runCtx)
	if duplicate := restarted.Start(runCtx); duplicate != done {
		t.Fatal("Start created more than one scheduler/timer loop")
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("rehydrated deadline did not run")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("capacity scheduler did not stop")
	}
}

func TestAuthenticationDoesNotEnterRetryLoop(t *testing.T) {
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	store, source := capacityFixture(t, now)
	manager := New(store, &fakeRouter{}, &fakeResumer{store: store}, &fakeNotifier{}, Config{Clock: func() time.Time { return now }})
	if err := manager.ObserveOutput(context.Background(), source, "401 Unauthorized: invalid API key", now); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.GetCapacityWait(context.Background(), source.ID); found {
		t.Fatal("authentication failure entered the capacity retry loop")
	}
}

func TestMalformedProfileDoesNotEnterRetryLoop(t *testing.T) {
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	store, source := capacityFixture(t, now)
	source.Metadata.ObservedExecutionProfileHash = "drifted"
	store.setSession(source)
	manager := New(store, &fakeRouter{}, &fakeResumer{store: store}, &fakeNotifier{}, Config{Clock: func() time.Time { return now }})
	if err := manager.ObserveOutput(context.Background(), source, "429 Too Many Requests", now); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := store.GetCapacityWait(context.Background(), source.ID); found {
		t.Fatal("malformed profile entered the capacity retry loop")
	}
}

func TestCapacityObservationUsesGenerationAwareReactionBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	store, err := sqlite.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := store.UpsertProject(ctx, domain.ProjectRecord{ID: "project", Path: "/tmp/project", RegisteredAt: now}); err != nil {
		t.Fatal(err)
	}
	_, source := capacityFixture(t, now)
	source.Metadata.Generation = "generation-2"
	current, err := store.CreateSession(ctx, source)
	if err != nil {
		t.Fatal(err)
	}
	stale := current
	stale.Metadata.Generation = "generation-1"
	manager := New(store, lifecycle.New(store, nil), nil, nil, Config{Clock: func() time.Time { return now }, Jitter: func(time.Duration) time.Duration { return 0 }})
	if err := manager.ObserveOutput(ctx, stale, "429 Too Many Requests", now); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetCapacityWait(ctx, current.ID); err != nil || found {
		t.Fatalf("stale generation created wait: found=%v err=%v", found, err)
	}
}

func capacityFixture(t *testing.T, now time.Time) (*fakeStore, domain.SessionRecord) {
	t.Helper()
	profile, err := domain.NewExecutionProfile(domain.AgentConfig{Model: "gpt-5.4", ReasoningEffort: "high", FastMode: true, ReviewModel: "gpt-5.4-review"}, "test")
	if err != nil {
		t.Fatal(err)
	}
	source := domain.SessionRecord{
		ID: "project-1", ProjectID: "project", IssueID: "1602", Kind: domain.KindWorker, Harness: domain.HarnessCodex, DisplayName: "capacity-worker",
		Metadata: domain.SessionMetadata{
			Generation: "generation-1", SpawnState: domain.SpawnStateSpawned, Branch: "task/1602", WorkspacePath: "/worktrees/project-1",
			RuntimeHandleID: "runtime-1", AgentSessionID: "thread-1", ExecutionProfile: profile, ObservedExecutionProfileHash: profile.Hash,
		},
		CreatedAt: now, UpdatedAt: now,
	}
	store := &fakeStore{waits: map[domain.SessionID]domain.CapacityWait{}, sessions: map[domain.SessionID]domain.SessionRecord{source.ID: source}}
	return store, source
}
