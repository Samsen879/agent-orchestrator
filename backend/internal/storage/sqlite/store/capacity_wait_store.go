package store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/aoagents/agent-orchestrator/backend/internal/domain"
	"github.com/aoagents/agent-orchestrator/backend/internal/storage/sqlite/gen"
)

// SaveCapacityWait writes the complete durable snapshot for a capacity episode.
func (s *Store) SaveCapacityWait(ctx context.Context, wait domain.CapacityWait) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.qw.UpsertCapacityWait(ctx, gen.UpsertCapacityWaitParams{
		SessionID: string(wait.SessionID), EpisodeID: wait.EpisodeID, ProjectID: string(wait.ProjectID), SourceGeneration: wait.SourceGeneration,
		AgentSessionID: wait.AgentSessionID, WorkspacePath: wait.WorkspacePath, Branch: wait.Branch, ProfileHash: wait.ProfileHash,
		State: string(wait.State), ErrorClass: string(wait.ErrorClass), SourceError: wait.SourceError, RuntimeHandleID: wait.RuntimeHandleID,
		OutputFingerprint: wait.OutputFingerprint, NextProbeAt: wait.NextProbeAt, AttemptCount: int64(wait.AttemptCount),
		NotifiedAt: nullableTime(wait.NotifiedAt), CreatedAt: wait.CreatedAt, UpdatedAt: wait.UpdatedAt,
	})
}

// GetCapacityWait returns the latest episode for a session.
func (s *Store) GetCapacityWait(ctx context.Context, id domain.SessionID) (domain.CapacityWait, bool, error) {
	row, err := s.qr.GetCapacityWait(ctx, string(id))
	if errors.Is(err, sql.ErrNoRows) {
		return domain.CapacityWait{}, false, nil
	}
	if err != nil {
		return domain.CapacityWait{}, false, err
	}
	return capacityWaitFromRow(row), true, nil
}

// ListActiveCapacityWaits returns all episodes that still affect scheduling or status.
func (s *Store) ListActiveCapacityWaits(ctx context.Context) ([]domain.CapacityWait, error) {
	rows, err := s.qr.ListActiveCapacityWaits(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]domain.CapacityWait, 0, len(rows))
	for _, row := range rows {
		out = append(out, capacityWaitFromRow(row))
	}
	return out, nil
}

func capacityWaitFromRow(row gen.CapacityWait) domain.CapacityWait {
	return domain.CapacityWait{
		EpisodeID: row.EpisodeID, ProjectID: domain.ProjectID(row.ProjectID), SessionID: domain.SessionID(row.SessionID), SourceGeneration: row.SourceGeneration,
		AgentSessionID: row.AgentSessionID, WorkspacePath: row.WorkspacePath, Branch: row.Branch, ProfileHash: row.ProfileHash,
		State: domain.CapacityWaitState(row.State), ErrorClass: domain.ProviderErrorClass(row.ErrorClass), SourceError: row.SourceError,
		RuntimeHandleID: row.RuntimeHandleID, OutputFingerprint: row.OutputFingerprint, NextProbeAt: row.NextProbeAt,
		AttemptCount: int(row.AttemptCount), NotifiedAt: nullTimeToTime(row.NotifiedAt), CreatedAt: row.CreatedAt, UpdatedAt: row.UpdatedAt,
	}
}
