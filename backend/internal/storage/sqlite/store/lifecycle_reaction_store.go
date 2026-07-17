package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// ReserveLifecycleReaction persists a pending envelope or returns its idempotent predecessor.
func (s *Store) ReserveLifecycleReaction(ctx context.Context, reaction domain.LifecycleReaction) (domain.LifecycleReaction, bool, error) {
	if err := reaction.Validate(); err != nil {
		return domain.LifecycleReaction{}, false, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	inserted := false
	err := s.inTx(ctx, "reserve lifecycle reaction", func(q *gen.Queries) error {
		rows, err := q.InsertLifecycleReaction(ctx, reactionToInsert(reaction))
		if err != nil {
			return err
		}
		inserted = rows == 1
		return nil
	})
	if err != nil {
		return domain.LifecycleReaction{}, false, err
	}
	row, err := s.qr.GetLifecycleReactionByEventID(ctx, reaction.EventID)
	if errors.Is(err, sql.ErrNoRows) {
		row, err = s.qr.GetLifecycleReactionByIdempotencyKey(ctx, reaction.IdempotencyKey)
	}
	if err != nil {
		return domain.LifecycleReaction{}, false, fmt.Errorf("read reserved lifecycle reaction: %w", err)
	}
	return reactionFromRow(row), !inserted, nil
}

// SetLifecycleReactionState makes a pending reaction terminal without rewriting terminal rows.
func (s *Store) SetLifecycleReactionState(ctx context.Context, eventID string, state domain.LifecycleReactionState, reason string, successor domain.SessionID, deliveredAt time.Time) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	rows, err := s.qw.SetLifecycleReactionState(ctx, gen.SetLifecycleReactionStateParams{
		State: string(state), Reason: reason, SuccessorSessionID: string(successor),
		DeliveredAt: nullableTime(deliveredAt), EventID: eventID,
	})
	if err != nil {
		return err
	}
	if rows == 0 {
		row, getErr := s.qw.GetLifecycleReactionByEventID(ctx, eventID)
		if getErr != nil {
			return getErr
		}
		if domain.LifecycleReactionState(row.State) != state {
			return fmt.Errorf("lifecycle reaction %s already terminal as %s", eventID, row.State)
		}
	}
	return nil
}

// SupersedeLifecycleReaction tombstones a pending predecessor event.
func (s *Store) SupersedeLifecycleReaction(ctx context.Context, eventID, reason string, successor domain.SessionID) error {
	if eventID == "" {
		return nil
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	_, err := s.qw.SupersedeLifecycleReaction(ctx, gen.SupersedeLifecycleReactionParams{Reason: reason, SuccessorSessionID: string(successor), EventID: eventID})
	return err
}

// GetLifecycleReaction returns a durable reaction by event identity.
func (s *Store) GetLifecycleReaction(ctx context.Context, eventID string) (domain.LifecycleReaction, bool, error) {
	row, err := s.qr.GetLifecycleReactionByEventID(ctx, eventID)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.LifecycleReaction{}, false, nil
	}
	if err != nil {
		return domain.LifecycleReaction{}, false, err
	}
	return reactionFromRow(row), true, nil
}

func reactionToInsert(r domain.LifecycleReaction) gen.InsertLifecycleReactionParams {
	return gen.InsertLifecycleReactionParams{
		EventID: r.EventID, Version: int64(r.Version), ProjectID: string(r.ProjectID), SourceSessionID: string(r.SourceSessionID), SourceGeneration: r.SourceGeneration,
		IssueID: string(r.IssueID), IssueURL: r.IssueURL, PRURL: r.PRURL, PRNumber: int64(r.PRNumber), Repo: r.Repo, Branch: r.Branch, HeadSha: r.HeadSHA,
		CreatedAt: r.CreatedAt, ExpiresAt: r.ExpiresAt, SuccessorSessionID: string(r.SuccessorSessionID), SupersedesEventID: r.SupersedesEventID, IdempotencyKey: r.IdempotencyKey,
	}
}

func reactionFromRow(r gen.LifecycleReaction) domain.LifecycleReaction {
	return domain.LifecycleReaction{
		Version: int(r.Version), EventID: r.EventID, ProjectID: domain.ProjectID(r.ProjectID), SourceSessionID: domain.SessionID(r.SourceSessionID), SourceGeneration: r.SourceGeneration,
		IssueID: domain.IssueID(r.IssueID), IssueURL: r.IssueURL, PRURL: r.PRURL, PRNumber: int(r.PRNumber), Repo: r.Repo, Branch: r.Branch, HeadSHA: r.HeadSha,
		CreatedAt: r.CreatedAt, ExpiresAt: r.ExpiresAt, SuccessorSessionID: domain.SessionID(r.SuccessorSessionID), SupersedesEventID: r.SupersedesEventID, IdempotencyKey: r.IdempotencyKey,
		State: domain.LifecycleReactionState(r.State), Reason: r.Reason, DeliveredAt: nullTimeToTime(r.DeliveredAt),
	}
}

func nullableTime(t time.Time) sql.NullTime {
	return sql.NullTime{Time: t, Valid: !t.IsZero()}
}
