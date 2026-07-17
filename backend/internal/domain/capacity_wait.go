package domain

import "time"

// CapacityWaitState is the durable phase of one provider-capacity episode.
type CapacityWaitState string

const (
	// CapacityWaitWaiting is the initial wait before the first scheduled probe.
	CapacityWaitWaiting CapacityWaitState = "capacity_wait"
	// CapacityWaitScheduled means a probe is scheduled after a capacity response.
	CapacityWaitScheduled CapacityWaitState = "scheduled_probe"
	// CapacityWaitResuming means the same native thread has been relaunched.
	CapacityWaitResuming CapacityWaitState = "same_session_resume"
	// CapacityWaitRecovered means activity proved the resumed thread is running.
	CapacityWaitRecovered CapacityWaitState = "recovered"
	// CapacityWaitFailedClosed means a non-capacity error stopped automatic recovery.
	CapacityWaitFailedClosed CapacityWaitState = "failed_closed"
)

// Active reports whether the episode still participates in scheduling/status.
func (s CapacityWaitState) Active() bool {
	return s == CapacityWaitWaiting || s == CapacityWaitScheduled || s == CapacityWaitResuming
}

// ProviderErrorClass separates retryable capacity from fail-closed errors.
type ProviderErrorClass string

const (
	// ProviderErrorCapacity is a retryable provider-side capacity exhaustion.
	ProviderErrorCapacity ProviderErrorClass = "capacity"
	// ProviderErrorAuthentication requires operator authentication repair.
	ProviderErrorAuthentication ProviderErrorClass = "authentication"
	// ProviderErrorPolicy requires a policy or permission decision.
	ProviderErrorPolicy ProviderErrorClass = "policy"
	// ProviderErrorConfiguration requires configuration repair.
	ProviderErrorConfiguration ProviderErrorClass = "configuration"
	// ProviderErrorMalformedProfile means the immutable profile cannot be trusted.
	ProviderErrorMalformedProfile ProviderErrorClass = "malformed_profile"
	// ProviderErrorUnknown is not eligible for automatic capacity recovery.
	ProviderErrorUnknown ProviderErrorClass = "unknown"
)

// CapacityWait is the persisted identity and schedule for one recovery episode.
type CapacityWait struct {
	EpisodeID         string
	ProjectID         ProjectID
	SessionID         SessionID
	SourceGeneration  string
	AgentSessionID    string
	WorkspacePath     string
	Branch            string
	ProfileHash       string
	State             CapacityWaitState
	ErrorClass        ProviderErrorClass
	SourceError       string
	RuntimeHandleID   string
	OutputFingerprint string
	NextProbeAt       time.Time
	AttemptCount      int
	NotifiedAt        time.Time
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// MatchesSession reports whether the durable episode still names the exact
// launch generation, native thread, implementation workspace, branch, and
// immutable execution profile represented by rec.
func (w CapacityWait) MatchesSession(rec SessionRecord) bool {
	return w.SessionID == rec.ID &&
		w.SourceGeneration != "" && w.SourceGeneration == rec.Metadata.Generation &&
		w.AgentSessionID != "" && w.AgentSessionID == rec.Metadata.AgentSessionID &&
		w.WorkspacePath != "" && w.WorkspacePath == rec.Metadata.WorkspacePath &&
		w.Branch != "" && w.Branch == rec.Metadata.Branch &&
		w.ProfileHash != "" && w.ProfileHash == rec.Metadata.ExecutionProfile.Hash &&
		w.ProfileHash == rec.Metadata.ObservedExecutionProfileHash
}
