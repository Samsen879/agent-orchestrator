// Package gate implements deterministic blocked-state classification and
// durable, lane-scoped human-gate resolution.
package gate

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

// Gate manager errors distinguish lookup, state, authority, and lane-matching failures.
var (
	// ErrGateNotFound reports that a requested durable gate does not exist.
	ErrGateNotFound           = errors.New("human gate not found")
	ErrGateNotOpen            = errors.New("human gate is not open")
	ErrLaneAlreadyGated       = errors.New("session already has an open human gate")
	ErrHumanAuthorityRequired = errors.New("human gate resolution requires human authority")
	ErrResolutionMismatch     = errors.New("human gate resolution does not match the gate")
)

// Store persists gates and reads the session facts used to bind them to an exact lane.
type Store interface {
	SaveHumanGate(context.Context, domain.HumanGate) error
	GetHumanGate(context.Context, string) (domain.HumanGate, bool, error)
	GetOpenHumanGateForSession(context.Context, domain.SessionID) (domain.HumanGate, bool, error)
	ListOpenHumanGates(context.Context) ([]domain.HumanGate, error)
	GetSession(context.Context, domain.SessionID) (domain.SessionRecord, bool, error)
}

// Resumer sends an authorized decision only to the gate's exact protected lane.
type Resumer interface {
	ResumeHumanGate(context.Context, domain.HumanGate, string) error
}

// Notifier emits the deduplicated initial human-gate notification.
type Notifier interface {
	Notify(context.Context, ports.NotificationIntent) error
}

// Result is the deterministic outcome of classifying a blocked-state signal.
type Result struct {
	Category    domain.BlockCategory
	ProfileHash string
	Gate        *domain.HumanGate
}

// Manager classifies blocked states and owns durable human-gate transitions.
type Manager struct {
	store    Store
	resumer  Resumer
	notifier Notifier
	clock    func() time.Time
	mu       sync.Mutex
}

// New constructs a deterministic human-gate manager.
func New(store Store, resumer Resumer, notifier Notifier, clock func() time.Time) *Manager {
	if clock == nil {
		clock = func() time.Time { return time.Now().UTC() }
	}
	return &Manager{store: store, resumer: resumer, notifier: notifier, clock: clock}
}

// Detect classifies first and persists only protected human gates.
func (m *Manager) Detect(ctx context.Context, signal domain.BlockSignal) (Result, error) {
	category, err := domain.ClassifyBlock(signal.Reason)
	if err != nil {
		return Result{}, err
	}
	result := Result{Category: category, ProfileHash: signal.ProfileHash}
	if category != domain.BlockHumanGate {
		return result, nil
	}
	if err := validateHumanSignal(signal); err != nil {
		return Result{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, found, err := m.store.GetSession(ctx, signal.SessionID)
	if err != nil {
		return Result{}, err
	}
	if !found || rec.IsTerminated || rec.ProjectID != signal.ProjectID || rec.Metadata.Generation != signal.SourceGeneration ||
		(signal.ProfileHash != "" && rec.Metadata.ExecutionProfile.Hash != signal.ProfileHash) {
		return Result{}, ErrResolutionMismatch
	}
	now := signal.DetectedAt.UTC()
	if now.IsZero() {
		now = m.clock()
	}
	id := signal.StableIdentity()
	gate, exists, err := m.store.GetHumanGate(ctx, id)
	if err != nil {
		return Result{}, err
	}
	if exists && !gate.Open() {
		result.Gate = &gate
		return result, nil
	}
	if !exists {
		openGate, open, openErr := m.store.GetOpenHumanGateForSession(ctx, signal.SessionID)
		if openErr != nil {
			return Result{}, openErr
		}
		if open && openGate.MatchesSession(rec) {
			result.Gate = &openGate
			return result, ErrLaneAlreadyGated
		}
		gate = domain.HumanGate{
			ID: id, DedupeKey: id, ProjectID: signal.ProjectID, SessionID: signal.SessionID, SourceGeneration: signal.SourceGeneration,
			ProfileHash: signal.ProfileHash, Reason: signal.Reason, RequiredDecision: strings.TrimSpace(signal.RequiredDecision),
			Evidence: signal.Evidence, AffectedTaskID: strings.TrimSpace(signal.AffectedTaskID), DependencyEdges: signal.DependencyEdges,
			AllowedActions: normalized(signal.AllowedActions), State: domain.GateOpen, DetectedAt: now, UpdatedAt: now,
			ReminderInterval: signal.ReminderInterval, EscalationAt: signal.EscalationAt.UTC(),
		}
		if gate.ReminderInterval > 0 {
			gate.NextReminderAt = now.Add(gate.ReminderInterval)
		}
	} else {
		gate.Evidence = mergeEvidence(gate.Evidence, signal.Evidence)
		gate.DependencyEdges = mergeEdges(gate.DependencyEdges, signal.DependencyEdges)
		gate.AllowedActions = normalized(append(gate.AllowedActions, signal.AllowedActions...))
		gate.UpdatedAt = now
	}
	if err := m.store.SaveHumanGate(ctx, gate); err != nil {
		return Result{}, err
	}
	if gate.NotifiedAt.IsZero() && m.notifier != nil {
		intent := ports.NotificationIntent{Type: domain.NotificationHumanGate, SessionID: gate.SessionID, ProjectID: gate.ProjectID, CreatedAt: now,
			SessionDisplayName: rec.DisplayName, RequiredDecision: gate.RequiredDecision, AffectedTaskID: gate.AffectedTaskID}
		if err := m.notifier.Notify(ctx, intent); err != nil {
			return Result{}, err
		}
		gate.NotifiedAt = now
		gate.UpdatedAt = now
		if err := m.store.SaveHumanGate(ctx, gate); err != nil {
			return Result{}, err
		}
	}
	result.Gate = &gate
	return result, nil
}

// Resolve records explicit human authorization before resuming exactly one lane.
func (m *Manager) Resolve(ctx context.Context, gateID string, resolution domain.HumanGateResolution) (domain.HumanGate, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if resolution.ActorType != domain.GateActorHuman || strings.TrimSpace(resolution.Actor) == "" || strings.TrimSpace(resolution.AuthorizationProvenance) == "" {
		return domain.HumanGate{}, ErrHumanAuthorityRequired
	}
	gate, found, err := m.store.GetHumanGate(ctx, gateID)
	if err != nil {
		return domain.HumanGate{}, err
	}
	if !found {
		return domain.HumanGate{}, ErrGateNotFound
	}
	if !gate.Open() {
		return domain.HumanGate{}, ErrGateNotOpen
	}
	if !slices.Contains(gate.AllowedActions, strings.TrimSpace(resolution.Action)) || strings.TrimSpace(resolution.Decision) == "" {
		return domain.HumanGate{}, ErrResolutionMismatch
	}
	rec, current, err := m.store.GetSession(ctx, gate.SessionID)
	if err != nil {
		return domain.HumanGate{}, err
	}
	if !current || rec.IsTerminated || !gate.MatchesSession(rec) {
		return domain.HumanGate{}, ErrResolutionMismatch
	}
	now := resolution.AuthorizedAt.UTC()
	if now.IsZero() {
		now = m.clock()
	}
	resolution.Actor = strings.TrimSpace(resolution.Actor)
	resolution.AuthorizationProvenance = strings.TrimSpace(resolution.AuthorizationProvenance)
	resolution.Action = strings.TrimSpace(resolution.Action)
	resolution.Decision = strings.TrimSpace(resolution.Decision)
	resolution.AuthorizedAt = now
	resolution.ResultingState = domain.GateOpen
	gate.State = domain.GateOpen
	gate.Resolution = &resolution
	gate.UpdatedAt = now
	if err := m.store.SaveHumanGate(ctx, gate); err != nil {
		return domain.HumanGate{}, err
	}
	if m.resumer == nil {
		return gate, errors.New("human gate resumer unavailable")
	}
	message := fmt.Sprintf("Human gate %s resolved by %s via %s. Approved action: %s. Decision: %s", gate.ID, resolution.Actor, resolution.AuthorizationProvenance, resolution.Action, resolution.Decision)
	if err := m.resumer.ResumeHumanGate(ctx, gate, message); err != nil {
		gate.Resolution.FailureReason = err.Error()
		gate.UpdatedAt = m.clock()
		if saveErr := m.store.SaveHumanGate(ctx, gate); saveErr != nil {
			return domain.HumanGate{}, saveErr
		}
		return gate, err
	}
	gate.State = domain.GateResumed
	gate.Resolution.ResultingState = domain.GateResumed
	gate.UpdatedAt = m.clock()
	if err := m.store.SaveHumanGate(ctx, gate); err != nil {
		return domain.HumanGate{}, err
	}
	return gate, nil
}

// ListOpen returns all currently protected human gates.
func (m *Manager) ListOpen(ctx context.Context) ([]domain.HumanGate, error) {
	return m.store.ListOpenHumanGates(ctx)
}

func validateHumanSignal(signal domain.BlockSignal) error {
	if signal.ProjectID == "" || signal.SessionID == "" || signal.SourceGeneration == "" || strings.TrimSpace(signal.ProfileHash) == "" || strings.TrimSpace(signal.RequiredDecision) == "" ||
		strings.TrimSpace(signal.AffectedTaskID) == "" || len(normalized(signal.AllowedActions)) == 0 || len(signal.Evidence) == 0 {
		return errors.New("human gate requires project, session, generation, profile, decision, evidence, affected task, and allowed actions")
	}
	return nil
}

func normalized(values []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	slices.Sort(out)
	return out
}

func mergeEvidence(a, b []domain.GateEvidence) []domain.GateEvidence {
	seen := map[domain.GateEvidence]bool{}
	out := make([]domain.GateEvidence, 0, len(a)+len(b))
	for _, item := range append(a, b...) {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}

func mergeEdges(a, b []domain.GateDependencyEdge) []domain.GateDependencyEdge {
	seen := map[domain.GateDependencyEdge]bool{}
	out := make([]domain.GateDependencyEdge, 0, len(a)+len(b))
	for _, item := range append(a, b...) {
		if !seen[item] {
			seen[item] = true
			out = append(out, item)
		}
	}
	return out
}
