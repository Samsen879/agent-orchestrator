// Package capacity implements durable provider-capacity recovery for AO sessions.
package capacity

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/lifecycle"
	"github.com/aoagents/agent-orchestrator/backend/internal/ports"
)

const (
	defaultBaseDelay = 30 * time.Second
	defaultCeiling   = 15 * time.Minute
	reactionTTL      = 30 * time.Minute
	maxSourceError   = 2048
)

type store interface {
	SaveCapacityWait(context.Context, domain.CapacityWait) error
	GetCapacityWait(context.Context, domain.SessionID) (domain.CapacityWait, bool, error)
	ListActiveCapacityWaits(context.Context) ([]domain.CapacityWait, error)
	GetSession(context.Context, domain.SessionID) (domain.SessionRecord, bool, error)
}

// ReactionRouter is the generation-aware lifecycle admission boundary.
type ReactionRouter interface {
	RouteReaction(context.Context, domain.LifecycleReaction, func() error) (lifecycle.ReactionRouteOutcome, error)
}

// Resumer relaunches an episode on its original native thread and workspace.
type Resumer interface {
	ResumeCapacity(context.Context, domain.CapacityWait) error
}

type notifier interface {
	Notify(context.Context, ports.NotificationIntent) error
}

// Config controls deterministic scheduling behavior.
type Config struct {
	Clock     func() time.Time
	BaseDelay time.Duration
	Ceiling   time.Duration
	Jitter    func(time.Duration) time.Duration
	Logger    *slog.Logger
}

// Manager owns one restart-safe timer and serializes capacity episode changes.
type Manager struct {
	store     store
	router    ReactionRouter
	resumer   Resumer
	notifier  notifier
	clock     func() time.Time
	baseDelay time.Duration
	ceiling   time.Duration
	jitter    func(time.Duration) time.Duration
	logger    *slog.Logger
	wake      chan struct{}
	mu        sync.Mutex
	startOnce sync.Once
	done      chan struct{}
}

// New constructs a capacity recovery manager.
func New(store store, router ReactionRouter, resumer Resumer, notifier notifier, cfg Config) *Manager {
	m := &Manager{store: store, router: router, resumer: resumer, notifier: notifier, clock: cfg.Clock, baseDelay: cfg.BaseDelay, ceiling: cfg.Ceiling, jitter: cfg.Jitter, logger: cfg.Logger, wake: make(chan struct{}, 1)}
	if m.clock == nil {
		m.clock = func() time.Time { return time.Now().UTC() }
	}
	if m.baseDelay <= 0 {
		m.baseDelay = defaultBaseDelay
	}
	if m.ceiling <= 0 {
		m.ceiling = defaultCeiling
	}
	if m.jitter == nil {
		m.jitter = func(maxDelay time.Duration) time.Duration {
			if maxDelay <= 0 {
				return 0
			}
			n, err := rand.Int(rand.Reader, big.NewInt(int64(maxDelay)+1))
			if err != nil {
				return 0
			}
			return time.Duration(n.Int64())
		}
	}
	if m.logger == nil {
		m.logger = slog.Default()
	}
	return m
}

// Start rehydrates durable deadlines and runs one timer until ctx is canceled.
func (m *Manager) Start(ctx context.Context) <-chan struct{} {
	m.startOnce.Do(func() {
		m.done = make(chan struct{})
		go func() {
			defer close(m.done)
			m.loop(ctx)
		}()
	})
	return m.done
}

func (m *Manager) loop(ctx context.Context) {
	for {
		delay, err := m.nextDelay(ctx)
		if err != nil {
			m.logger.Error("capacity: load schedule", "err", err)
			delay = m.baseDelay
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return
		case <-m.wake:
			if !timer.Stop() {
				<-timer.C
			}
		case <-timer.C:
			if err := m.RunDue(ctx); err != nil && !errors.Is(err, context.Canceled) {
				m.logger.Error("capacity: scheduled probes", "err", err)
			}
		}
	}
}

func (m *Manager) nextDelay(ctx context.Context) (time.Duration, error) {
	waits, err := m.store.ListActiveCapacityWaits(ctx)
	if err != nil {
		return 0, err
	}
	now := m.clock()
	var earliest time.Time
	for _, wait := range waits {
		if wait.State == domain.CapacityWaitResuming {
			continue
		}
		if earliest.IsZero() || wait.NextProbeAt.Before(earliest) {
			earliest = wait.NextProbeAt
		}
	}
	if earliest.IsZero() {
		return m.ceiling, nil
	}
	if !earliest.After(now) {
		return 0, nil
	}
	return earliest.Sub(now), nil
}

// ObserveOutput classifies a bounded runtime tail and records capacity only.
// Authentication, policy, configuration, and malformed-profile errors never
// enter the retry loop; if they interrupt an active episode it fails closed.
func (m *Manager) ObserveOutput(ctx context.Context, source domain.SessionRecord, output string, observedAt time.Time) error {
	class, sourceError := ClassifyProviderError(output)
	if class == domain.ProviderErrorUnknown {
		return nil
	}
	if observedAt.IsZero() {
		observedAt = m.clock()
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if class != domain.ProviderErrorCapacity {
		return m.failClosedIfActive(ctx, source, class, sourceError, observedAt)
	}
	profile := source.Metadata.ExecutionProfile
	if profile.Validate() != nil || source.Metadata.ObservedExecutionProfileHash != profile.Hash {
		return m.failClosedIfActive(ctx, source, domain.ProviderErrorMalformedProfile, "execution profile is invalid or configured/observed hashes differ", observedAt)
	}
	if source.Metadata.Generation == "" || source.Metadata.AgentSessionID == "" || source.Metadata.WorkspacePath == "" || source.Metadata.Branch == "" || profile.Hash == "" {
		return nil
	}
	fingerprint := fingerprint(sourceError)
	event := capacityReaction(source, "capacity-observed", source.Metadata.RuntimeHandleID+"\x00"+fingerprint, observedAt)
	_, err := m.router.RouteReaction(ctx, event, func() error {
		return m.recordCapacity(ctx, source, sourceError, fingerprint, observedAt)
	})
	return err
}

func (m *Manager) recordCapacity(ctx context.Context, source domain.SessionRecord, sourceError, outputFingerprint string, now time.Time) error {
	wait, found, err := m.store.GetCapacityWait(ctx, source.ID)
	if err != nil {
		return err
	}
	sameEpisode := found && wait.State.Active() && wait.SourceGeneration == source.Metadata.Generation && wait.ProfileHash == source.Metadata.ExecutionProfile.Hash
	if sameEpisode && wait.State != domain.CapacityWaitResuming && wait.RuntimeHandleID == source.Metadata.RuntimeHandleID && wait.OutputFingerprint == outputFingerprint {
		return nil
	}
	if !sameEpisode {
		wait = domain.CapacityWait{
			EpisodeID: uuid.NewString(), ProjectID: source.ProjectID, SessionID: source.ID, SourceGeneration: source.Metadata.Generation,
			AgentSessionID: source.Metadata.AgentSessionID, WorkspacePath: source.Metadata.WorkspacePath, Branch: source.Metadata.Branch,
			ProfileHash: source.Metadata.ExecutionProfile.Hash, State: domain.CapacityWaitWaiting, CreatedAt: now,
		}
	} else {
		wait.State = domain.CapacityWaitScheduled
		wait.AttemptCount++
	}
	wait.ErrorClass = domain.ProviderErrorCapacity
	wait.SourceError = bounded(sourceError)
	wait.RuntimeHandleID = source.Metadata.RuntimeHandleID
	wait.OutputFingerprint = outputFingerprint
	wait.NextProbeAt = now.Add(m.backoff(wait.AttemptCount))
	wait.UpdatedAt = now
	if err := m.store.SaveCapacityWait(ctx, wait); err != nil {
		return err
	}
	if wait.NotifiedAt.IsZero() && m.notifier != nil {
		intent := ports.NotificationIntent{Type: domain.NotificationCapacityWait, SessionID: source.ID, ProjectID: source.ProjectID, CreatedAt: now, SessionDisplayName: source.DisplayName, WaitReason: wait.SourceError, NextProbeAt: wait.NextProbeAt}
		if err := m.notifier.Notify(ctx, intent); err != nil {
			return err
		}
		wait.NotifiedAt = now
		wait.UpdatedAt = now
		if err := m.store.SaveCapacityWait(ctx, wait); err != nil {
			return err
		}
	}
	m.signalWake()
	return nil
}

func (m *Manager) failClosedIfActive(ctx context.Context, source domain.SessionRecord, class domain.ProviderErrorClass, sourceError string, now time.Time) error {
	wait, found, err := m.store.GetCapacityWait(ctx, source.ID)
	if err != nil || !found || !wait.State.Active() || wait.SourceGeneration != source.Metadata.Generation {
		return err
	}
	wait.State = domain.CapacityWaitFailedClosed
	wait.ErrorClass = class
	wait.SourceError = bounded(sourceError)
	wait.UpdatedAt = now
	return m.store.SaveCapacityWait(ctx, wait)
}

// ObserveActivity completes an episode only after the resumed generation emits
// authoritative activity from the same native thread.
func (m *Manager) ObserveActivity(ctx context.Context, source domain.SessionRecord, signal ports.ActivitySignal) error {
	if !signal.Valid {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	wait, found, err := m.store.GetCapacityWait(ctx, source.ID)
	if err != nil || !found || wait.State != domain.CapacityWaitResuming {
		return err
	}
	if wait.SourceGeneration != source.Metadata.Generation || wait.AgentSessionID != source.Metadata.AgentSessionID || wait.ProfileHash != source.Metadata.ExecutionProfile.Hash {
		return nil
	}
	wait.State = domain.CapacityWaitRecovered
	wait.UpdatedAt = m.clock()
	return m.store.SaveCapacityWait(ctx, wait)
}

// RunDue executes every durable probe whose deadline has elapsed.
func (m *Manager) RunDue(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	waits, err := m.store.ListActiveCapacityWaits(ctx)
	if err != nil {
		return err
	}
	now := m.clock()
	for _, wait := range waits {
		if wait.State == domain.CapacityWaitResuming || wait.NextProbeAt.After(now) {
			continue
		}
		if err := m.resumeOne(ctx, wait, now); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) resumeOne(ctx context.Context, wait domain.CapacityWait, now time.Time) error {
	if wait.State == domain.CapacityWaitWaiting {
		wait.State = domain.CapacityWaitScheduled
		wait.UpdatedAt = now
		if err := m.store.SaveCapacityWait(ctx, wait); err != nil {
			return err
		}
	}
	source, found, err := m.store.GetSession(ctx, wait.SessionID)
	if err != nil {
		return err
	}
	if !found {
		wait.State = domain.CapacityWaitFailedClosed
		wait.SourceError = "source session missing"
		wait.UpdatedAt = now
		return m.store.SaveCapacityWait(ctx, wait)
	}
	event := capacityReaction(source, "capacity-resume", fmt.Sprintf("%s\x00%d", wait.EpisodeID, wait.AttemptCount), now)
	event.SourceGeneration = wait.SourceGeneration
	event.Branch = wait.Branch
	var resumeErr error
	outcome, err := m.router.RouteReaction(ctx, event, func() error {
		resumeErr = m.resumer.ResumeCapacity(ctx, wait)
		return nil
	})
	if err != nil {
		return err
	}
	if !outcome.Delivered {
		wait.State = domain.CapacityWaitFailedClosed
		wait.SourceError = "capacity resume superseded by current lifecycle identity"
		wait.UpdatedAt = now
		return m.store.SaveCapacityWait(ctx, wait)
	}
	if resumeErr != nil {
		class, message := ClassifyProviderError(resumeErr.Error())
		wait.ErrorClass = class
		wait.SourceError = bounded(message)
		wait.UpdatedAt = now
		if class == domain.ProviderErrorCapacity {
			wait.State = domain.CapacityWaitScheduled
			wait.AttemptCount++
			wait.NextProbeAt = now.Add(m.backoff(wait.AttemptCount))
			if err := m.store.SaveCapacityWait(ctx, wait); err != nil {
				return err
			}
			m.signalWake()
			return nil
		}
		wait.State = domain.CapacityWaitFailedClosed
		return m.store.SaveCapacityWait(ctx, wait)
	}
	current, ok, err := m.store.GetSession(ctx, wait.SessionID)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	wait.State = domain.CapacityWaitResuming
	wait.RuntimeHandleID = current.Metadata.RuntimeHandleID
	wait.OutputFingerprint = ""
	wait.UpdatedAt = now
	return m.store.SaveCapacityWait(ctx, wait)
}

func (m *Manager) backoff(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := m.baseDelay
	for i := 0; i < attempt && delay < m.ceiling; i++ {
		if delay > m.ceiling/2 {
			delay = m.ceiling
			break
		}
		delay *= 2
	}
	if delay > m.ceiling {
		delay = m.ceiling
	}
	room := m.ceiling - delay
	jitterMax := delay / 4
	if jitterMax > room {
		jitterMax = room
	}
	return delay + m.jitter(jitterMax)
}

func (m *Manager) signalWake() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

var ansiPattern = regexp.MustCompile(`\x1b\[[0-?]*[\x20-\x2f]*[\x40-\x7e]`)

// ClassifyProviderError returns the highest-safety matching provider error.
func ClassifyProviderError(raw string) (domain.ProviderErrorClass, string) {
	clean := strings.TrimSpace(ansiPattern.ReplaceAllString(raw, ""))
	lower := strings.ToLower(clean)
	patterns := []struct {
		class domain.ProviderErrorClass
		terms []string
	}{
		{domain.ProviderErrorMalformedProfile, []string{"execution profile drift", "malformed execution profile", "invalid execution profile", "profile hash mismatch"}},
		{domain.ProviderErrorAuthentication, []string{"invalid api key", "authentication failed", "not logged in", "unauthorized", "401 unauthorized"}},
		{domain.ProviderErrorPolicy, []string{"content policy", "policy violation", "safety policy", "permission denied by policy"}},
		{domain.ProviderErrorConfiguration, []string{"model not found", "unsupported model", "invalid model", "invalid configuration", "unknown reasoning effort"}},
		{domain.ProviderErrorCapacity, []string{"429 too many requests", "rate_limit_exceeded", "rate limit exceeded", "server is overloaded", "provider capacity", "model is temporarily unavailable", "you've hit your usage limit"}},
	}
	for _, candidate := range patterns {
		for _, term := range candidate.terms {
			if strings.Contains(lower, term) {
				return candidate.class, bounded(clean)
			}
		}
	}
	return domain.ProviderErrorUnknown, bounded(clean)
}

func capacityReaction(source domain.SessionRecord, kind, identity string, now time.Time) domain.LifecycleReaction {
	sum := sha256.Sum256([]byte(strings.Join([]string{kind, string(source.ProjectID), string(source.ID), source.Metadata.Generation, identity}, "\x00")))
	key := hex.EncodeToString(sum[:])
	return domain.LifecycleReaction{
		Version: domain.LifecycleReactionVersion, EventID: kind + ":" + key, ProjectID: source.ProjectID, SourceSessionID: source.ID,
		SourceGeneration: source.Metadata.Generation, IssueID: source.IssueID, Branch: source.Metadata.Branch,
		CreatedAt: now, ExpiresAt: now.Add(reactionTTL), IdempotencyKey: key,
	}
}

func fingerprint(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func bounded(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > maxSourceError {
		return value[:maxSourceError]
	}
	return value
}
