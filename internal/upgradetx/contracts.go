// Package upgradetx owns the neutral upgrade transaction vocabulary.
// Values describe evidence; constructing one never authorizes execution. Wire,
// disk and adjunct codecs must validate their own boundaries rather than encode
// an engine's mutable memory. This package depends only on the standard library.
// origin: hot-update S8 plan §2 / P00
package upgradetx

// RecordVersion and DiskSchemaVersion describe the v2 transaction journal, not
// the public protocol or image ABI. Neither existing version is bumped by S8.
const (
	RecordVersion     uint32 = 2
	DiskSchemaVersion uint32 = 2
	CodeNotWired             = "upgrade_not_wired"
)

// S8-owned codes stay below proto. Wire/admin adapters explicitly project them
// into the public registry and CLI exit classes; an accepted unknown result
// must never be projected as a retryable refusal that invites a second Start.
const (
	CodeUnsupportedComponent       = "unsupported_component"
	CodeStateCorrupt               = "upgrade_state_corrupt"
	CodeGrantUnknown               = "upgrade_grant_unknown"
	CodeGrantExpired               = "upgrade_grant_expired"
	CodeRejected                   = "upgrade_rejected"
	CodeDeferred                   = "upgrade_deferred"
	CodeUnsettled                  = "upgrade_unsettled"
	CodeWindowClosed               = "upgrade_window_closed"
	CodeSessionIncarnationMismatch = "session_incarnation_mismatch"
)

// These closed string vocabularies retain an invalid empty value. Their member
// prefixes belong in the existing enum gate; explicit strings are not a reason
// to bypass its exhaustive-switch checks. No integer/iota family is introduced.
type Component string

const (
	ComponentAgent  Component = "agent"
	ComponentBroker Component = "broker"
	ComponentNATS   Component = "nats"
	ComponentCaddy  Component = "caddy"
)

// NATS and Caddy name reserved components, not executable S8 capabilities.
// A missing domain cannot stand in for explicit not_applicable scope evidence.
type Domain string

const (
	DomainControl         Domain = "control"
	DomainExec            Domain = "exec"
	DomainPTY             Domain = "pty"
	DomainFlow            Domain = "flow"
	DomainSS              Domain = "ss"
	DomainTransferWorker  Domain = "transfer_worker"
	DomainTransferTracker Domain = "transfer_tracker"
	DomainBrokerStore     Domain = "broker_store"
	DomainBrokerRuntime   Domain = "broker_runtime"
	DomainAuthority       Domain = "authority"
	DomainUpgrade         Domain = "upgrade"
)

type ReceiptKind string

const (
	ReceiptControlReady          ReceiptKind = "control_ready"
	ReceiptResourcesMigrated     ReceiptKind = "resources_migrated"
	ReceiptOldGenerationReleased ReceiptKind = "old_generation_released"
)

type Direction string

const (
	DirectionForward Direction = "forward"
	DirectionReverse Direction = "reverse"
)

type CutStage string

const (
	CutNone          CutStage = "none"
	CutPristine      CutStage = "pristine"
	CutPartialActive CutStage = "partial_active"
	CutActive        CutStage = "active"
	CutRetiring      CutStage = "retiring"
	CutReleased      CutStage = "released"
)

type Phase string

const (
	PhasePrepared         Phase = "prepared"
	PhaseStaged           Phase = "staged"
	PhaseReady            Phase = "ready"
	PhaseFreezing         Phase = "freezing"
	PhaseActivating       Phase = "activating"
	PhaseServingNew       Phase = "serving_new"
	PhaseMigrating        Phase = "migrating"
	PhaseCompleted        Phase = "completed"
	PhaseDeferred         Phase = "deferred"
	PhaseRejected         Phase = "rejected"
	PhaseRollbackPending  Phase = "rollback_pending"
	PhaseRollbackBlocked  Phase = "rollback_blocked"
	PhaseRolledBack       Phase = "rolled_back"
	PhaseRecoveryRequired Phase = "recovery_required"
	PhaseCancelled        Phase = "cancelled"
	PhaseAbandoned        Phase = "abandoned"
)

type Outcome string

const (
	OutcomePending            Outcome = "pending"
	OutcomeSuccess            Outcome = "success"
	OutcomeDeferred           Outcome = "deferred"
	OutcomeRejected           Outcome = "rejected"
	OutcomeRolledBack         Outcome = "rolled_back"
	OutcomeLegacyInterrupting Outcome = "legacy_interrupting"
	OutcomeUnknown            Outcome = "unknown"
	OutcomeCancelled          Outcome = "cancelled"
)

// Interrupting execution requires an explicit grant; an omitted mode never
// authorizes a legacy operation, even when decoding an older request.
type Mode string

const (
	ModeHot                Mode = "hot"
	ModeLegacyInterrupting Mode = "legacy_interrupting"
)

type ActionState string

const (
	ActionIntent   ActionState = "intent"
	ActionObserved ActionState = "observed"
	ActionDurable  ActionState = "durable"
)

type WindowState string

const (
	WindowOpen    WindowState = "open"
	WindowClosing WindowState = "closing"
	WindowClosed  WindowState = "closed"
)

type StepKind string

const (
	StepBrokerCut    StepKind = "broker_cut"
	StepAgentCut     StepKind = "agent_cut"
	StepHomeConverge StepKind = "home_converge"
	StepRelease      StepKind = "release"
)

// Each action needs a durable intent and independent effect evidence before a
// result becomes durable. A lost reply cannot authorize a second dispatch.
type ActionKind string

const (
	ActionPublish  ActionKind = "publish"
	ActionSelect   ActionKind = "select"
	ActionFreeze   ActionKind = "freeze"
	ActionDispatch ActionKind = "dispatch"
	ActionActivate ActionKind = "activate"
	ActionControl  ActionKind = "control"
	ActionMigrate  ActionKind = "migrate"
	ActionRetire   ActionKind = "retire"
	ActionReverse  ActionKind = "reverse"
	ActionRelease  ActionKind = "release"
)

// Error separates a refusal from failure after durable acceptance. Detail must
// contain public diagnostics only: never PIN hashes, salts or sealed state.
type Error struct {
	Code     string `json:"code"`
	Detail   string `json:"detail,omitempty"`
	Accepted bool   `json:"accepted"`
}

func (e *Error) Error() string {
	if e.Detail == "" {
		return e.Code
	}
	return e.Code + ": " + e.Detail
}

// Key survives cold starts. Owner instance identity must not change its store
// namespace. The role decides which optional identity fields are mandatory.
type Key struct {
	DomainID      string    `json:"domain_id"`
	Component     Component `json:"component"`
	SID           string    `json:"sid,omitempty"`
	ConfiguredNID string    `json:"configured_nid,omitempty"`
	ConfigPath    string    `json:"config_path,omitempty"`
	UID           uint32    `json:"uid,omitempty"`
}

// IDs retain the exact string field types in the frozen table. Boundary owners
// validate bounded 128-bit random hex IDs and lowercase 64-hex SHAs; using a
// named replacement for these fields would break the frozen method signatures.
type Handle struct {
	Key        Key    `json:"key"`
	TxID       string `json:"tx_id"`
	CutID      string `json:"cut_id"`
	Generation uint64 `json:"generation"`
	Revision   uint64 `json:"revision"`
}

// PID alone is reusable. All four fields are needed to establish ownership.
type Owner struct {
	BootID     string `json:"boot_id"`
	PID        int    `json:"pid"`
	StartTicks uint64 `json:"start_ticks"`
	InstanceID string `json:"instance_id"`
}

type Fence struct {
	Current     Handle `json:"current"`
	Owner       Owner  `json:"owner"`
	RunningSHA  string `json:"running_sha"`
	PreviousSHA string `json:"previous_sha"`
	Incarnation string `json:"incarnation"`
}

// A grant belongs to its issuer, actor and parent slot, not just its two SHAs.
// ExpiresAt and all other timestamps use Unix nanoseconds, never a renewed TTL.
type Grant struct {
	Handle           Handle `json:"handle"`
	ParentTxID       string `json:"parent_tx_id,omitempty"`
	ParentSlot       string `json:"parent_slot,omitempty"`
	ClientNonce      string `json:"client_nonce"`
	Issuer           Owner  `json:"issuer"`
	ActorFP          string `json:"actor_fp"`
	Mode             Mode   `json:"mode"`
	SourceRelease    string `json:"source_release"`
	TargetRelease    string `json:"target_release"`
	SourceSHA        string `json:"source_sha"`
	TargetSHA        string `json:"target_sha"`
	ScopeDigest      string `json:"scope_digest"`
	CapabilityDigest string `json:"capability_digest"`
	ExpiresAt        int64  `json:"expires_at"`
}

// Scope is sorted and deduplicated before sealing. Its expected identities must
// not be replaced by counters or recomputed from a later, smaller roster.
type Scope struct {
	ID           string     `json:"id"`
	Participants []Key      `json:"participants"`
	Domains      []Domain   `json:"domains"`
	Expected     []Resource `json:"expected"`
}

// Resource carries an original object/worker/permit identity. Custody records
// who can still execute or retain it, including responsibility after termination.
type Resource struct {
	Domain     Domain `json:"domain"`
	ID         string `json:"id"`
	Origin     string `json:"origin"`
	Owner      Owner  `json:"owner"`
	Generation uint64 `json:"generation"`
	State      string `json:"state"`
	Custody    string `json:"custody"`
}

type Stamp struct {
	Attempt     Handle `json:"attempt"`
	Producer    Owner  `json:"producer"`
	Nonce       string `json:"nonce"`
	Sequence    uint64 `json:"sequence"`
	At          int64  `json:"at"`
	ScopeDigest string `json:"scope_digest"`
}

// Known and Complete remain explicit even for an empty resource set. Neither
// a missing producer nor an unobserved domain can sign a zero-resource proof.
type Census struct {
	Stamp          Stamp      `json:"stamp"`
	Known          bool       `json:"known"`
	Complete       bool       `json:"complete"`
	Resources      []Resource `json:"resources"`
	ExpectedDigest string     `json:"expected_digest"`
	Admitting      uint64     `json:"admitting"`
	OldOwners      []Owner    `json:"old_owners"`
}

type Receipt struct {
	Kind           ReceiptKind `json:"kind"`
	Stamp          Stamp       `json:"stamp"`
	Known          bool        `json:"known"`
	Complete       bool        `json:"complete"`
	RunningSHA     string      `json:"running_sha"`
	Census         Census      `json:"census"`
	EvidenceDigest string      `json:"evidence_digest"`
}

// Forward and reverse have separate cuts, generations and receipts. Historical
// control readiness never substitutes for a fresh observation of the live image.
type Attempt struct {
	Handle           Handle    `json:"handle"`
	Direction        Direction `json:"direction"`
	SourceSHA        string    `json:"source_sha"`
	TargetSHA        string    `json:"target_sha"`
	SourceGeneration uint64    `json:"source_generation"`
	Phase            Phase     `json:"phase"`
	CutStage         CutStage  `json:"cut_stage"`
	Outcome          Outcome   `json:"outcome"`
	Scope            Scope     `json:"scope"`
	Control          Receipt   `json:"control"`
	Resources        Receipt   `json:"resources"`
	Released         Receipt   `json:"released"`
}

type Action struct {
	ID           string      `json:"id"`
	Handle       Handle      `json:"handle"`
	Kind         ActionKind  `json:"kind"`
	IntentDigest string      `json:"intent_digest"`
	ResultDigest string      `json:"result_digest"`
	State        ActionState `json:"state"`
	Nonce        string      `json:"nonce"`
}

// Archived execution records are immutable. Reporting and rollback-window
// bookkeeping live in Metadata so they cannot rewrite an accepted outcome.
type Record struct {
	Cancellation        *CancellationProof `json:"cancellation,omitempty"`
	Version             uint32             `json:"version"`
	Key                 Key                `json:"key"`
	Revision            uint64             `json:"revision"`
	Grant               Grant              `json:"grant"`
	Forward             Attempt            `json:"forward"`
	Reverse             Attempt            `json:"reverse"`
	Actions             []Action           `json:"actions"`
	Reason              string             `json:"reason"`
	EffectiveSHA        string             `json:"effective_sha"`
	RecordedPreviousSHA string             `json:"recorded_previous_sha"`
}

type Metadata struct {
	TxID             string      `json:"tx_id"`
	ReportedRevision uint64      `json:"reported_revision"`
	Window           WindowState `json:"window"`
	Pins             []string    `json:"pins"`
	ArchivedAt       int64       `json:"archived_at"`
}

type Observation struct {
	Nonce        string  `json:"nonce"`
	At           int64   `json:"at"`
	Current      Attempt `json:"current"`
	Census       Census  `json:"census"`
	OwnerKnown   bool    `json:"owner_known"`
	RunningSHA   string  `json:"running_sha"`
	EffectiveSHA string  `json:"effective_sha"`
}

// Full views are private to the original actor or an authorized administrator.
// Public health uses P13's separate, deliberately smaller proto projection.
type View struct {
	Historical  Record      `json:"historical"`
	Metadata    Metadata    `json:"metadata"`
	Observation Observation `json:"observation"`
	Code        string      `json:"code,omitempty"`
	Detail      string      `json:"detail,omitempty"`
}

type Step struct {
	Ordinal     uint32   `json:"ordinal"`
	Participant Key      `json:"participant"`
	Grant       Grant    `json:"grant"`
	Kind        StepKind `json:"kind"`
	DependsOn   []uint32 `json:"depends_on"`
}

type Plan struct {
	Parent               Handle `json:"parent"`
	Scope                Scope  `json:"scope"`
	Steps                []Step `json:"steps"`
	SourceManifestDigest string `json:"source_manifest_digest"`
	TargetManifestDigest string `json:"target_manifest_digest"`
	Deadline             int64  `json:"deadline"`
}

// Missing old-protocol identities cannot be inferred from time proximity or
// matching SHAs. Causal must come from evidence tied to the original dispatch.
type LegacyUpgradeProof struct {
	ParentTxID       string `json:"parent_tx_id"`
	Issuer           Owner  `json:"issuer"`
	GrantNonce       string `json:"grant_nonce"`
	TargetInstance   string `json:"target_instance,omitempty"`
	TargetKey        Key    `json:"target_key"`
	SourceSHA        string `json:"source_sha"`
	CandidateSHA     string `json:"candidate_sha"`
	DispatchActionID string `json:"dispatch_action_id"`
	LegacyUpgradeID  string `json:"legacy_upgrade_id,omitempty"`
	ObservedSHA      string `json:"observed_sha,omitempty"`
	ObservedNonce    string `json:"observed_nonce,omitempty"`
	State            string `json:"state"`
	Causal           bool   `json:"causal"`
}

type Event struct {
	// origin: S8 internal review rv3-leaves F9
	// Recovery authorizes a narrow expected-revision edge, not a reusable
	// journal receipt. The writer validates it without replacing historical proof.
	Recovery     *Observation       `json:"recovery,omitempty"`
	Cancellation *CancellationProof `json:"cancellation,omitempty"`
	Action       Action             `json:"action"`
	Receipt      Receipt            `json:"receipt"`
	Phase        Phase              `json:"phase"`
	CutStage     CutStage           `json:"cut_stage"`
	Outcome      Outcome            `json:"outcome"`
	Reason       string             `json:"reason"`
}

type PrepareRequest struct {
	ParentTxID       string `json:"parent_tx_id,omitempty"`
	ParentSlot       string `json:"parent_slot,omitempty"`
	ClientNonce      string `json:"client_nonce"`
	ActorFP          string `json:"actor_fp"`
	SourceRelease    string `json:"source_release"`
	TargetRelease    string `json:"target_release"`
	SourceSHA        string `json:"source_sha"`
	TargetSHA        string `json:"target_sha"`
	Mode             Mode   `json:"mode"`
	Scope            Scope  `json:"scope"`
	CapabilityDigest string `json:"capability_digest"`
	ExpiresAt        int64  `json:"expires_at"`
}

type ReverseRequest struct {
	ClientNonce    string `json:"client_nonce"`
	ActorFP        string `json:"actor_fp"`
	HistoricalTxID string `json:"historical_tx_id"`
	TargetSHA      string `json:"target_sha"`
	Scope          Scope  `json:"scope"`
	ExpiresAt      int64  `json:"expires_at"`
}

// Reaping alone is insufficient: descendants and transferred FD custody must
// also be settled before the unresolved current transaction can be archived.
type DeathProof struct {
	Owner      Owner  `json:"owner"`
	Stamp      Stamp  `json:"stamp"`
	Custody    Census `json:"custody"`
	Reaped     bool   `json:"reaped"`
	NoOwnedFDs bool   `json:"no_owned_fds"`
}

// Store is the sole durable writer. Expected handles fence every late callback;
// Load cannot create a grant, and Start cannot resurrect an unknown grant.
type Store interface {
	Prepare(key Key, req PrepareRequest) (Grant, error)
	Start(grant Grant, fence Fence) (Record, error)
	Load(key Key, txID string) (View, error)
	Apply(expected Handle, event Event) (Record, error)
	PrepareReverse(expected Fence, req ReverseRequest) (Grant, error)
	Archive(expected Handle, death DeathProof) (View, error)
	CloseWindow(expected Handle) (Metadata, error)
}

// P01 implements these functions in its final leaf files. Aliases freeze the
// signatures without introducing replaceable global callbacks or fake bodies.
type ValidatePlanFunc = func(plan Plan) error
type TransitionFunc = func(record Record, event Event) (Record, error)
type CompletedFunc = func(attempt Attempt, observation Observation) error
