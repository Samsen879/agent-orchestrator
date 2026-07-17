package domain

import (
	"errors"
	"time"
)

// LifecycleReactionVersion is the current durable reaction envelope schema.
const LifecycleReactionVersion = 1

// LifecycleReactionState is the durable admission/delivery state of a reaction.
type LifecycleReactionState string

const (
	// LifecycleReactionPending means the reaction is reserved but not terminal.
	LifecycleReactionPending LifecycleReactionState = "pending"
	// LifecycleReactionDelivered means the reaction side effect completed.
	LifecycleReactionDelivered LifecycleReactionState = "delivered"
	// LifecycleReactionSuperseded means newer durable ownership made the reaction stale.
	LifecycleReactionSuperseded LifecycleReactionState = "superseded"
	// LifecycleReactionExpired means the reaction exceeded its delivery window.
	LifecycleReactionExpired LifecycleReactionState = "expired"
	// LifecycleReactionTargetMissing means its source session, runtime, or external target disappeared.
	LifecycleReactionTargetMissing LifecycleReactionState = "target_missing"
)

// Terminal reports whether no further delivery attempt is permitted.
func (s LifecycleReactionState) Terminal() bool {
	return s != "" && s != LifecycleReactionPending
}

// LifecycleReaction binds one delayed lifecycle action to its source generation
// and external target snapshot.
type LifecycleReaction struct {
	Version            int
	EventID            string
	ProjectID          ProjectID
	SourceSessionID    SessionID
	SourceGeneration   string
	IssueID            IssueID
	IssueURL           string
	PRURL              string
	PRNumber           int
	Repo               string
	Branch             string
	HeadSHA            string
	CreatedAt          time.Time
	ExpiresAt          time.Time
	SuccessorSessionID SessionID
	SupersedesEventID  string
	IdempotencyKey     string
	State              LifecycleReactionState
	Reason             string
	DeliveredAt        time.Time
}

// Validate checks the versioned identity and delivery-window invariants.
func (r LifecycleReaction) Validate() error {
	if r.Version != LifecycleReactionVersion {
		return errors.New("lifecycle reaction: unsupported version")
	}
	if r.EventID == "" || r.ProjectID == "" || r.SourceSessionID == "" || r.SourceGeneration == "" || r.IdempotencyKey == "" {
		return errors.New("lifecycle reaction: event, project, source session, generation, and idempotency key are required")
	}
	if r.CreatedAt.IsZero() || r.ExpiresAt.IsZero() || !r.ExpiresAt.After(r.CreatedAt) {
		return errors.New("lifecycle reaction: valid creation and expiry times are required")
	}
	return nil
}

// LifecycleReactionPRTarget is the canonical live provider snapshot used at admission.
type LifecycleReactionPRTarget struct {
	Found        bool
	URL          string
	Number       int
	Repo         string
	SourceBranch string
	HeadSHA      string
}
