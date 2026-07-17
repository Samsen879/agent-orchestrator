package gate

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

type fakeStore struct {
	gates    map[string]domain.HumanGate
	sessions map[domain.SessionID]domain.SessionRecord
}

func (s *fakeStore) SaveHumanGate(_ context.Context, gate domain.HumanGate) error {
	s.gates[gate.ID] = gate
	return nil
}
func (s *fakeStore) GetHumanGate(_ context.Context, id string) (domain.HumanGate, bool, error) {
	gate, ok := s.gates[id]
	return gate, ok, nil
}
func (s *fakeStore) GetOpenHumanGateForSession(_ context.Context, id domain.SessionID) (domain.HumanGate, bool, error) {
	for _, gate := range s.gates {
		if gate.SessionID == id && gate.Open() {
			return gate, true, nil
		}
	}
	return domain.HumanGate{}, false, nil
}
func (s *fakeStore) ListOpenHumanGates(context.Context) ([]domain.HumanGate, error) {
	var out []domain.HumanGate
	for _, gate := range s.gates {
		if gate.Open() {
			out = append(out, gate)
		}
	}
	return out, nil
}
func (s *fakeStore) GetSession(_ context.Context, id domain.SessionID) (domain.SessionRecord, bool, error) {
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

type fakeNotifier struct{ intents []ports.NotificationIntent }

func (n *fakeNotifier) Notify(_ context.Context, intent ports.NotificationIntent) error {
	n.intents = append(n.intents, intent)
	return nil
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
