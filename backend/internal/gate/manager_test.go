package gate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type fakeStore struct {
	gates    map[string]domain.HumanGate
	sessions map[domain.SessionID]domain.SessionRecord
	getHook  func(domain.SessionID)
	mu       sync.Mutex
}

func (s *fakeStore) SaveHumanGate(_ context.Context, gate domain.HumanGate) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gates[gate.ID] = gate
	return nil
}
func (s *fakeStore) GetHumanGate(_ context.Context, id string) (domain.HumanGate, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	gate, ok := s.gates[id]
	return gate, ok, nil
}
func (s *fakeStore) GetOpenHumanGateForSession(_ context.Context, id domain.SessionID) (domain.HumanGate, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, gate := range s.gates {
		if gate.SessionID == id && gate.Open() {
			return gate, true, nil
		}
	}
	return domain.HumanGate{}, false, nil
}
func (s *fakeStore) ListOpenHumanGates(context.Context) ([]domain.HumanGate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []domain.HumanGate
	for _, gate := range s.gates {
		if gate.Open() {
			out = append(out, gate)
		}
	}
	return out, nil
}
func (s *fakeStore) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
	if s.getHook != nil {
		s.getHook(id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.sessions[id]
	return rec, ok, nil
}

type fakeResumer struct {
	calls []domain.HumanGate
	errs  []error
}

func (r *fakeResumer) ResumeHumanGate(_ context.Context, gate domain.HumanGate, _ string) error {
	r.calls = append(r.calls, gate)
	if len(r.errs) == 0 {
		return nil
	}
	err := r.errs[0]
	r.errs = r.errs[1:]
	return err
}

type fakeNotifier struct {
	intents []ports.NotificationIntent
	err     error
}

func (n *fakeNotifier) Notify(_ context.Context, intent ports.NotificationIntent) error {
	n.intents = append(n.intents, intent)
	return n.err
}

func TestClassifyBlockPolicyTable(t *testing.T) {
	tests := []struct {
		reason domain.BlockReason
		want   domain.BlockCategory
	}{
		{domain.BlockReasonCIFailure, domain.BlockOperationalAuto}, {domain.BlockReasonReviewCoordination, domain.BlockOperationalAuto},
		{domain.BlockReasonStaleWorker, domain.BlockOperationalAuto},
		{domain.BlockReasonProviderCapacity, domain.BlockTemporalWait}, {domain.BlockReasonMissingCredentials, domain.BlockHumanGate},
		{domain.BlockReasonInfrastructureUnavailable, domain.BlockTemporalWait},
		{domain.BlockReasonPrivilegedAction, domain.BlockHumanGate}, {domain.BlockReasonPaidAction, domain.BlockHumanGate},
		{domain.BlockReasonDestructiveAction, domain.BlockHumanGate}, {domain.BlockReasonPolicyException, domain.BlockHumanGate},
		{domain.BlockReasonProductDecision, domain.BlockHumanGate}, {domain.BlockReasonExplicitApproval, domain.BlockHumanGate},
		{domain.BlockReasonInvalidConfiguration, domain.BlockTerminalFailure}, {domain.BlockReasonControlPlaneInstall, domain.BlockTerminalFailure},
	}
	for _, tt := range tests {
		got, err := domain.ClassifyBlock(tt.reason)
		if err != nil || got != tt.want {
			t.Fatalf("ClassifyBlock(%q)=(%q,%v), want %q", tt.reason, got, err, tt.want)
		}
	}
}

func TestOperationalAndCapacityDoNotCreateGateOrDriftProfile(t *testing.T) {
	store, _, manager := fixture()
	for _, tt := range []struct {
		reason   domain.BlockReason
		category domain.BlockCategory
	}{
		{domain.BlockReasonCIFailure, domain.BlockOperationalAuto},
		{domain.BlockReasonReviewCoordination, domain.BlockOperationalAuto},
		{domain.BlockReasonProviderCapacity, domain.BlockTemporalWait},
		{domain.BlockReasonControlPlaneInstall, domain.BlockTerminalFailure},
	} {
		result, err := manager.Detect(context.Background(), domain.BlockSignal{Reason: tt.reason, ProfileHash: "profile-1"})
		if err != nil || result.Category != tt.category || result.ProfileHash != "profile-1" || result.Gate != nil {
			t.Fatalf("result=%+v err=%v", result, err)
		}
	}
	if len(store.gates) != 0 {
		t.Fatalf("non-human categories created gates: %+v", store.gates)
	}
}

func TestInvalidSignalsReturnTypedValidationError(t *testing.T) {
	_, _, manager := fixture()
	if _, err := manager.Detect(context.Background(), domain.BlockSignal{Reason: "unknown"}); !errors.Is(err, ErrInvalidSignal) {
		t.Fatalf("unknown reason err=%v", err)
	}
	signal := humanSignal(domain.BlockReasonProductDecision)
	signal.ProfileHash = ""
	if _, err := manager.Detect(context.Background(), signal); !errors.Is(err, ErrInvalidSignal) {
		t.Fatalf("missing profile err=%v", err)
	}
}

func TestHumanGateFixturesPersistOnceAndDeduplicateNotification(t *testing.T) {
	for _, reason := range []domain.BlockReason{domain.BlockReasonMissingCredentials, domain.BlockReasonPrivilegedAction, domain.BlockReasonPaidAction, domain.BlockReasonDestructiveAction, domain.BlockReasonPolicyException, domain.BlockReasonProductDecision} {
		store, notifier, manager := fixture()
		signal := humanSignal(reason)
		first, err := manager.Detect(context.Background(), signal)
		if err != nil {
			t.Fatal(err)
		}
		signal.Evidence = append(signal.Evidence, domain.GateEvidence{Kind: "log", Source: "retry", Detail: "same gate"})
		second, err := manager.Detect(context.Background(), signal)
		if err != nil {
			t.Fatal(err)
		}
		if first.Gate.ID != second.Gate.ID || len(store.gates) != 1 || len(notifier.intents) != 1 || len(second.Gate.Evidence) != 2 {
			t.Fatalf("reason=%q first=%+v second=%+v gates=%d notifications=%d", reason, first, second, len(store.gates), len(notifier.intents))
		}
		if notifier.intents[0].DedupeKey != first.Gate.ID {
			t.Fatalf("notification dedupe key=%q gate=%q", notifier.intents[0].DedupeKey, first.Gate.ID)
		}
	}
}

func TestStaleOpenGateRebindsBeforeNewGenerationDetection(t *testing.T) {
	store, _, manager := fixture()
	created, err := manager.Detect(context.Background(), humanSignal(domain.BlockReasonProductDecision))
	if err != nil {
		t.Fatal(err)
	}
	rec := store.sessions["project-1"]
	rec.Metadata.Generation = "generation-2"
	rec.Metadata.ExecutionProfile.Hash = "profile-2"
	store.sessions[rec.ID] = rec
	signal := humanSignal(domain.BlockReasonPolicyException)
	signal.DedupeKey = "new-generation-gate"
	signal.SourceGeneration = "generation-2"
	signal.ProfileHash = "profile-2"
	result, err := manager.Detect(context.Background(), signal)
	if !errors.Is(err, ErrLaneAlreadyGated) || result.Gate == nil || result.Gate.ID != created.Gate.ID {
		t.Fatalf("stale rebind result=%+v err=%v", result, err)
	}
	if result.Gate.SourceGeneration != "generation-2" || result.Gate.ProfileHash != "profile-2" || len(store.gates) != 1 {
		t.Fatalf("rebound gate=%+v gates=%d", result.Gate, len(store.gates))
	}
	if got := result.Gate.Evidence[len(result.Gate.Evidence)-1]; got.Kind != "identity_rebound" {
		t.Fatalf("rebind evidence=%+v", result.Gate.Evidence)
	}
}

func TestIndependentSessionDetectionDoesNotWaitOnAnotherSessionLock(t *testing.T) {
	store, _, manager := fixture()
	store.sessions["project-2"] = domain.SessionRecord{ID: "project-2", ProjectID: "project", Kind: domain.KindWorker, Metadata: domain.SessionMetadata{Generation: "generation-1", ExecutionProfile: domain.ExecutionProfile{Hash: "profile-1"}}}
	entered, release := make(chan struct{}), make(chan struct{})
	store.getHook = func(id domain.SessionID) {
		if id == "project-1" {
			close(entered)
			<-release
		}
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := manager.Detect(context.Background(), humanSignal(domain.BlockReasonProductDecision))
		firstDone <- err
	}()
	<-entered
	secondSignal := humanSignal(domain.BlockReasonProductDecision)
	secondSignal.SessionID = "project-2"
	secondDone := make(chan error, 1)
	go func() {
		_, err := manager.Detect(context.Background(), secondSignal)
		secondDone <- err
	}()
	select {
	case err := <-secondDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("independent session detection was blocked by another session lock")
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	manager.locksMu.Lock()
	defer manager.locksMu.Unlock()
	if len(manager.locks) != 0 {
		t.Fatalf("session locks retained after completion: %d", len(manager.locks))
	}
}

func TestNotificationFailureLeavesDurableGateRetryable(t *testing.T) {
	store, notifier, manager := fixture()
	notifier.err = errors.New("notification unavailable")
	if _, err := manager.Detect(context.Background(), humanSignal(domain.BlockReasonProductDecision)); err == nil {
		t.Fatal("expected notification failure")
	}
	gate, ok, err := store.GetOpenHumanGateForSession(context.Background(), "project-1")
	if err != nil || !ok || !gate.NotifiedAt.IsZero() {
		t.Fatalf("persisted gate=%+v ok=%v err=%v", gate, ok, err)
	}
	notifier.err = nil
	result, err := manager.Detect(context.Background(), humanSignal(domain.BlockReasonProductDecision))
	if err != nil || result.Gate == nil || result.Gate.NotifiedAt.IsZero() || len(notifier.intents) != 2 {
		t.Fatalf("retry result=%+v intents=%d err=%v", result, len(notifier.intents), err)
	}
}

func TestHumanGateDedupeIdentityIsScopedToExactLane(t *testing.T) {
	store, _, manager := fixture()
	store.sessions["project-2"] = domain.SessionRecord{ID: "project-2", ProjectID: "project", Kind: domain.KindWorker, Metadata: domain.SessionMetadata{Generation: "generation-1", ExecutionProfile: domain.ExecutionProfile{Hash: "profile-1"}}}
	firstSignal := humanSignal(domain.BlockReasonProductDecision)
	first, err := manager.Detect(context.Background(), firstSignal)
	if err != nil {
		t.Fatal(err)
	}
	secondSignal := firstSignal
	secondSignal.SessionID = "project-2"
	second, err := manager.Detect(context.Background(), secondSignal)
	if err != nil {
		t.Fatal(err)
	}
	if first.Gate.ID == second.Gate.ID || len(store.gates) != 2 || first.Gate.SessionID != "project-1" || second.Gate.SessionID != "project-2" {
		t.Fatalf("lane-scoped gates first=%+v second=%+v gates=%d", first.Gate, second.Gate, len(store.gates))
	}
}

func TestDifferentGateCannotStackOnAlreadyProtectedLane(t *testing.T) {
	store, _, manager := fixture()
	first, err := manager.Detect(context.Background(), humanSignal(domain.BlockReasonProductDecision))
	if err != nil {
		t.Fatal(err)
	}
	secondSignal := humanSignal(domain.BlockReasonPolicyException)
	secondSignal.DedupeKey = "different-gate"
	second, err := manager.Detect(context.Background(), secondSignal)
	if !errors.Is(err, ErrLaneAlreadyGated) || second.Gate == nil || second.Gate.ID != first.Gate.ID || len(store.gates) != 1 {
		t.Fatalf("stacked gate result=%+v err=%v gates=%d", second, err, len(store.gates))
	}
}

func TestResolutionFailureRemainsProtectedAndMatchingRetryResumesLane(t *testing.T) {
	_, _, manager := fixture()
	resumer := &fakeResumer{errs: []error{errors.New("pane unavailable"), nil}}
	manager.resumer = resumer
	created, err := manager.Detect(context.Background(), humanSignal(domain.BlockReasonProductDecision))
	if err != nil {
		t.Fatal(err)
	}
	bad := domain.HumanGateResolution{Actor: "orchestrator", ActorType: domain.GateActorOrchestrator, AuthorizationProvenance: "prompt", Action: "choose-a", Decision: "A"}
	if _, err := manager.Resolve(context.Background(), created.Gate.ID, bad); !errors.Is(err, ErrHumanAuthorityRequired) {
		t.Fatalf("orchestrator resolution err=%v", err)
	}
	resolution := domain.HumanGateResolution{Actor: "alice", ActorType: domain.GateActorHuman, AuthorizationProvenance: "desktop-confirmation:42", Action: "choose-a", Decision: "choose option A"}
	failed, err := manager.Resolve(context.Background(), created.Gate.ID, resolution)
	if err == nil || failed.State != domain.GateOpen || failed.Resolution == nil || failed.Resolution.FailureReason != "pane unavailable" || failed.Resolution.ResultingState != domain.GateOpen {
		t.Fatalf("failed attempt gate=%+v err=%v", failed, err)
	}
	resumed, err := manager.Resolve(context.Background(), created.Gate.ID, resolution)
	if err != nil || resumed.State != domain.GateResumed || resumed.Resolution.ResultingState != domain.GateResumed || resumed.Resolution.FailureReason != "" || len(resumer.calls) != 2 {
		t.Fatalf("resumed=%+v err=%v calls=%d", resumed, err, len(resumer.calls))
	}
	if resumer.calls[0].SessionID != "project-1" || resumer.calls[1].SessionID != "project-1" {
		t.Fatalf("wrong lane resumed: %+v", resumer.calls)
	}
}

func TestAuthorizedResolutionRebindsStaleProfileBeforeResume(t *testing.T) {
	store, _, manager := fixture()
	resumer := &fakeResumer{}
	manager.resumer = resumer
	created, err := manager.Detect(context.Background(), humanSignal(domain.BlockReasonProductDecision))
	if err != nil {
		t.Fatal(err)
	}
	rec := store.sessions["project-1"]
	rec.Metadata.ExecutionProfile.Hash = "profile-2"
	store.sessions[rec.ID] = rec
	resolution := domain.HumanGateResolution{Actor: "alice", ActorType: domain.GateActorHuman, AuthorizationProvenance: "desktop-confirmation:43", Action: "choose-a", Decision: "choose option A"}
	resolved, err := manager.Resolve(context.Background(), created.Gate.ID, resolution)
	if err != nil || resolved.State != domain.GateResumed || resolved.ProfileHash != "profile-2" || len(resumer.calls) != 1 || resumer.calls[0].ProfileHash != "profile-2" {
		t.Fatalf("resolved=%+v calls=%+v err=%v", resolved, resumer.calls, err)
	}
}

func fixture() (*fakeStore, *fakeNotifier, *Manager) {
	now := time.Date(2026, 7, 17, 8, 0, 0, 0, time.UTC)
	rec := domain.SessionRecord{ID: "project-1", ProjectID: "project", Kind: domain.KindWorker, Metadata: domain.SessionMetadata{Generation: "generation-1", ExecutionProfile: domain.ExecutionProfile{Hash: "profile-1"}}}
	store := &fakeStore{gates: map[string]domain.HumanGate{}, sessions: map[domain.SessionID]domain.SessionRecord{rec.ID: rec}}
	notifier := &fakeNotifier{}
	return store, notifier, New(store, &fakeResumer{}, notifier, func() time.Time { return now })
}

func humanSignal(reason domain.BlockReason) domain.BlockSignal {
	return domain.BlockSignal{DedupeKey: "gate-key", ProjectID: "project", SessionID: "project-1", SourceGeneration: "generation-1", ProfileHash: "profile-1", Reason: reason,
		RequiredDecision: "Choose A or B", Evidence: []domain.GateEvidence{{Kind: "request", Source: "worker", Detail: "decision required"}}, AffectedTaskID: "task-a",
		DependencyEdges: []domain.GateDependencyEdge{{FromTaskID: "task-a", ToTaskID: "task-dependent"}}, AllowedActions: []string{"choose-a", "choose-b"}, ReminderInterval: time.Hour}
}
