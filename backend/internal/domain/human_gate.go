package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
)

// BlockCategory is the deterministic policy class for an interrupted lane.
type BlockCategory string

// Block categories define whether AO may recover, must wait, must stop for a human, or has terminally failed.
const (
	// BlockOperationalAuto permits AO to recover routine coordination failures.
	BlockOperationalAuto BlockCategory = "operational_auto"
	BlockTemporalWait    BlockCategory = "temporal_wait"
	BlockHumanGate       BlockCategory = "human_gate"
	BlockTerminalFailure BlockCategory = "terminal_failure"
)

// BlockReason is an explicit machine-produced reason code. Free-form prompt
// text is evidence only and never determines policy.
type BlockReason string

// Block reason codes are machine-produced inputs to the fixed classification table.
const (
	// BlockReasonCIFailure identifies routine CI coordination work.
	BlockReasonCIFailure                 BlockReason = "ci_failure"
	BlockReasonReviewCoordination        BlockReason = "review_coordination"
	BlockReasonStaleWorker               BlockReason = "stale_worker"
	BlockReasonProviderCapacity          BlockReason = "provider_capacity"
	BlockReasonInfrastructureUnavailable BlockReason = "infrastructure_temporarily_unavailable"
	BlockReasonMissingCredentials        BlockReason = "missing_credentials"
	BlockReasonPrivilegedAction          BlockReason = "privileged_action"
	BlockReasonPaidAction                BlockReason = "paid_action"
	BlockReasonDestructiveAction         BlockReason = "destructive_action"
	BlockReasonPolicyException           BlockReason = "policy_exception"
	BlockReasonProductDecision           BlockReason = "product_decision"
	BlockReasonExplicitApproval          BlockReason = "explicit_human_approval"
	BlockReasonInvalidConfiguration      BlockReason = "invalid_configuration"
	BlockReasonControlPlaneInstall       BlockReason = "control_plane_installation_failure"
)

// ClassifyBlock applies the fixed policy table before any LLM coordination.
func ClassifyBlock(reason BlockReason) (BlockCategory, error) {
	switch reason {
	case BlockReasonCIFailure, BlockReasonReviewCoordination, BlockReasonStaleWorker:
		return BlockOperationalAuto, nil
	case BlockReasonProviderCapacity, BlockReasonInfrastructureUnavailable:
		return BlockTemporalWait, nil
	case BlockReasonMissingCredentials, BlockReasonPrivilegedAction, BlockReasonPaidAction,
		BlockReasonDestructiveAction, BlockReasonPolicyException, BlockReasonProductDecision, BlockReasonExplicitApproval:
		return BlockHumanGate, nil
	case BlockReasonInvalidConfiguration, BlockReasonControlPlaneInstall:
		return BlockTerminalFailure, nil
	default:
		return "", errors.New("unknown block reason")
	}
}

// GateState records the durable lifecycle of a protected human decision.
type GateState string

// Gate states distinguish protected, decided, and successfully resumed lanes.
const (
	// GateOpen keeps the affected lane protected until an authorized resolution resumes it.
	GateOpen     GateState = "open"
	GateResolved GateState = "resolved"
	GateResumed  GateState = "resumed"
)

// GateActorType identifies the capability class that supplied a resolution event.
type GateActorType string

// Gate actor classes identify trusted human events and non-human actors that must be rejected.
const (
	// GateActorHuman is the only actor class authorized to resolve a protected gate.
	GateActorHuman          GateActorType = "human"
	GateActorOrchestrator   GateActorType = "orchestrator"
	GateActorAOWorker       GateActorType = "ao_worker"
	GateActorNativeSubagent GateActorType = "native_subagent"
)

// GateEvidence is one typed fact supporting why a protected decision is required.
type GateEvidence struct {
	Kind   string `json:"kind"`
	Source string `json:"source"`
	Detail string `json:"detail"`
}

// GateDependencyEdge describes a task dependency affected by a protected gate.
type GateDependencyEdge struct {
	FromTaskID string `json:"fromTaskId"`
	ToTaskID   string `json:"toTaskId"`
}

// BlockSignal is the typed input to classification and optional gate creation.
type BlockSignal struct {
	DedupeKey        string
	ProjectID        ProjectID
	SessionID        SessionID
	SourceGeneration string
	ProfileHash      string
	Reason           BlockReason
	RequiredDecision string
	Evidence         []GateEvidence
	AffectedTaskID   string
	DependencyEdges  []GateDependencyEdge
	AllowedActions   []string
	DetectedAt       time.Time
	ReminderInterval time.Duration
	EscalationAt     time.Time
}

// HumanGateResolution is the durable audit record for a protected decision.
type HumanGateResolution struct {
	Actor                   string
	ActorType               GateActorType
	AuthorizationProvenance string
	Action                  string
	Decision                string
	AuthorizedAt            time.Time
	ResultingState          GateState
	FailureReason           string
}

// HumanGate is the durable lane-scoped protected state.
type HumanGate struct {
	ID               string
	DedupeKey        string
	ProjectID        ProjectID
	SessionID        SessionID
	SourceGeneration string
	ProfileHash      string
	Reason           BlockReason
	RequiredDecision string
	Evidence         []GateEvidence
	AffectedTaskID   string
	DependencyEdges  []GateDependencyEdge
	AllowedActions   []string
	State            GateState
	DetectedAt       time.Time
	UpdatedAt        time.Time
	NotifiedAt       time.Time
	ReminderInterval time.Duration
	NextReminderAt   time.Time
	EscalationAt     time.Time
	ReminderCount    int
	LastRemindedAt   time.Time
	Resolution       *HumanGateResolution
}

// Open reports whether the gate still protects its affected lane.
func (g HumanGate) Open() bool { return g.State == GateOpen }

// MatchesSession verifies that a gate still belongs to the exact durable session generation and profile.
func (g HumanGate) MatchesSession(rec SessionRecord) bool {
	return g.SessionID == rec.ID && g.SourceGeneration != "" && g.SourceGeneration == rec.Metadata.Generation &&
		g.ProfileHash != "" && g.ProfileHash == rec.Metadata.ExecutionProfile.Hash
}

// StableIdentity returns the deterministic deduplication identity for a blocked-state signal.
func (s BlockSignal) StableIdentity() string {
	producerKey := strings.TrimSpace(s.DedupeKey)
	if producerKey == "" {
		producerKey = strings.TrimSpace(s.RequiredDecision)
	}
	key := strings.Join([]string{string(s.ProjectID), string(s.SessionID), s.SourceGeneration, s.ProfileHash, string(s.Reason), s.AffectedTaskID, producerKey}, "\x00")
	sum := sha256.Sum256([]byte(key))
	return "gate_" + hex.EncodeToString(sum[:])
}
