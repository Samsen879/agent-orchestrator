package lifecycle

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
)

const defaultReactionTTL = 30 * time.Minute

type reactionStore interface {
	ReserveLifecycleReaction(context.Context, domain.LifecycleReaction) (domain.LifecycleReaction, bool, error)
	SetLifecycleReactionState(context.Context, string, domain.LifecycleReactionState, string, domain.SessionID, time.Time) error
	SupersedeLifecycleReaction(context.Context, string, string, domain.SessionID) error
	GetLifecycleReaction(context.Context, string) (domain.LifecycleReaction, bool, error)
	ListAllSessions(context.Context) ([]domain.SessionRecord, error)
	GetPR(context.Context, string) (domain.PullRequest, bool, error)
}

// ReactionPRResolver refreshes canonical provider truth for a PR-bound event.
type ReactionPRResolver interface {
	ResolveReactionPR(context.Context, domain.LifecycleReaction) (domain.LifecycleReactionPRTarget, error)
}

// ReactionRouteOutcome reports the durable terminal state reached by routing.
type ReactionRouteOutcome struct {
	State     domain.LifecycleReactionState
	Delivered bool
	Duplicate bool
}

// SetReactionPRResolver wires the live provider used immediately before a
// PR-bound reaction is routed.
func (m *Manager) SetReactionPRResolver(resolver ReactionPRResolver) {
	m.reactionMu.Lock()
	defer m.reactionMu.Unlock()
	m.reactionPR = resolver
}

// RouteReaction durably admits, reconciles, and delivers one lifecycle action.
// Terminal rows are immutable and replayed event/idempotency keys are no-ops.
func (m *Manager) RouteReaction(ctx context.Context, reaction domain.LifecycleReaction, deliver func() error) (ReactionRouteOutcome, error) {
	if m.reactionStore == nil {
		return ReactionRouteOutcome{}, errors.New("lifecycle reaction store is unavailable")
	}
	if err := reaction.Validate(); err != nil {
		return ReactionRouteOutcome{}, err
	}
	m.reactionMu.Lock()
	defer m.reactionMu.Unlock()

	stored, existing, err := m.reactionStore.ReserveLifecycleReaction(ctx, reaction)
	if err != nil {
		return ReactionRouteOutcome{}, err
	}
	if stored.State.Terminal() {
		return ReactionRouteOutcome{State: stored.State, Delivered: stored.State == domain.LifecycleReactionDelivered, Duplicate: existing}, nil
	}
	if reaction.SupersedesEventID != "" {
		if err := m.reactionStore.SupersedeLifecycleReaction(ctx, reaction.SupersedesEventID, "superseded by "+reaction.EventID, reaction.SourceSessionID); err != nil {
			return ReactionRouteOutcome{}, err
		}
	}
	state, reason, successor, err := m.reconcileReaction(ctx, stored)
	if err != nil {
		return ReactionRouteOutcome{}, err
	}
	if state != domain.LifecycleReactionPending {
		if err := m.reactionStore.SetLifecycleReactionState(ctx, stored.EventID, state, reason, successor, time.Time{}); err != nil {
			return ReactionRouteOutcome{}, err
		}
		return ReactionRouteOutcome{State: state, Duplicate: existing}, nil
	}
	if deliver != nil {
		if err := deliver(); err != nil {
			return ReactionRouteOutcome{}, err
		}
	}
	deliveredAt := m.clock()
	if err := m.reactionStore.SetLifecycleReactionState(ctx, stored.EventID, domain.LifecycleReactionDelivered, "", "", deliveredAt); err != nil {
		return ReactionRouteOutcome{}, err
	}
	return ReactionRouteOutcome{State: domain.LifecycleReactionDelivered, Delivered: true, Duplicate: existing}, nil
}

func (m *Manager) reconcileReaction(ctx context.Context, reaction domain.LifecycleReaction) (domain.LifecycleReactionState, string, domain.SessionID, error) {
	now := m.clock()
	if !now.Before(reaction.ExpiresAt) {
		return domain.LifecycleReactionExpired, "reaction expiry elapsed", "", nil
	}
	rec, ok, err := m.store.GetSession(ctx, reaction.SourceSessionID)
	if err != nil {
		return "", "", "", err
	}
	if !ok || rec.IsTerminated || rec.Metadata.RuntimeHandleID == "" {
		return domain.LifecycleReactionTargetMissing, "source session or runtime is missing", "", nil
	}
	if rec.ProjectID != reaction.ProjectID || rec.Metadata.Generation != reaction.SourceGeneration {
		return domain.LifecycleReactionSuperseded, "source session generation is no longer current", rec.ID, nil
	}
	if reaction.Branch != "" && rec.Metadata.Branch != reaction.Branch && !strings.HasPrefix(reaction.Branch, rec.Metadata.Branch+"/") {
		return domain.LifecycleReactionSuperseded, "source branch is no longer current", rec.ID, nil
	}
	if reaction.IssueID != "" {
		if rec.IssueID != reaction.IssueID {
			return domain.LifecycleReactionSuperseded, "source session no longer owns the issue", rec.ID, nil
		}
		sessions, listErr := m.reactionStore.ListAllSessions(ctx)
		if listErr != nil {
			return "", "", "", listErr
		}
		for _, candidate := range sessions {
			isNewer := candidate.CreatedAt.After(rec.CreatedAt) || (candidate.CreatedAt.Equal(rec.CreatedAt) && candidate.ID > rec.ID)
			if candidate.ID != rec.ID && candidate.ProjectID == reaction.ProjectID && candidate.IssueID == reaction.IssueID && !candidate.IsTerminated && candidate.Metadata.RuntimeHandleID != "" && isNewer {
				return domain.LifecycleReactionSuperseded, "a successor session owns the issue", candidate.ID, nil
			}
		}
	}
	if reaction.PRURL != "" {
		pr, found, prErr := m.reactionStore.GetPR(ctx, reaction.PRURL)
		if prErr != nil {
			return "", "", "", prErr
		}
		if !found {
			return domain.LifecycleReactionTargetMissing, "PR target is not tracked", "", nil
		}
		if pr.SessionID != rec.ID {
			return domain.LifecycleReactionSuperseded, "a successor session owns the PR", pr.SessionID, nil
		}
		if reaction.PRNumber > 0 && pr.Number != reaction.PRNumber {
			return domain.LifecycleReactionSuperseded, "stored PR number changed", pr.SessionID, nil
		}
		if reaction.Repo != "" && pr.Repo != reaction.Repo {
			return domain.LifecycleReactionSuperseded, "stored PR repository changed", pr.SessionID, nil
		}
		if reaction.Branch != "" && pr.SourceBranch != reaction.Branch {
			return domain.LifecycleReactionSuperseded, "stored PR branch changed", pr.SessionID, nil
		}
		if reaction.HeadSHA != "" && pr.HeadSHA != reaction.HeadSHA {
			return domain.LifecycleReactionSuperseded, "stored PR head changed", pr.SessionID, nil
		}
		if m.reactionPR == nil {
			return "", "", "", errors.New("live PR reaction resolver is unavailable")
		}
		live, resolveErr := m.reactionPR.ResolveReactionPR(ctx, reaction)
		if resolveErr != nil {
			return "", "", "", resolveErr
		}
		if !live.Found {
			return domain.LifecycleReactionTargetMissing, "live PR target is missing", "", nil
		}
		if reaction.PRURL != "" && live.URL != "" && live.URL != reaction.PRURL {
			return domain.LifecycleReactionSuperseded, "live PR URL changed", rec.ID, nil
		}
		if reaction.PRNumber > 0 && live.Number != reaction.PRNumber {
			return domain.LifecycleReactionSuperseded, "live PR number changed", rec.ID, nil
		}
		if reaction.Repo != "" && live.Repo != reaction.Repo {
			return domain.LifecycleReactionSuperseded, "live PR repository changed", rec.ID, nil
		}
		if reaction.HeadSHA != "" && live.HeadSHA != reaction.HeadSHA {
			return domain.LifecycleReactionSuperseded, "live PR head changed", rec.ID, nil
		}
		if reaction.Branch != "" && live.SourceBranch != reaction.Branch {
			return domain.LifecycleReactionSuperseded, "live PR branch changed", rec.ID, nil
		}
	}
	return domain.LifecycleReactionPending, "", "", nil
}

func reactionEnvelope(source domain.SessionRecord, kind string, createdAt time.Time, payload any) domain.LifecycleReaction {
	if createdAt.IsZero() {
		createdAt = time.Now().UTC()
	}
	raw, _ := json.Marshal(payload)
	sum := sha256.Sum256(append([]byte(strings.Join([]string{kind, string(source.ProjectID), string(source.ID), source.Metadata.Generation}, "\x00")+"\x00"), raw...))
	key := hex.EncodeToString(sum[:])
	return domain.LifecycleReaction{
		Version: domain.LifecycleReactionVersion, EventID: kind + ":" + key, ProjectID: source.ProjectID,
		SourceSessionID: source.ID, SourceGeneration: source.Metadata.Generation, IssueID: source.IssueID,
		Branch: source.Metadata.Branch, CreatedAt: createdAt, ExpiresAt: createdAt.Add(defaultReactionTTL), IdempotencyKey: key,
	}
}

func requireReactionSource(source domain.SessionRecord) error {
	if source.ID == "" || source.ProjectID == "" || source.Metadata.Generation == "" {
		return fmt.Errorf("lifecycle reaction source identity is incomplete")
	}
	return nil
}
