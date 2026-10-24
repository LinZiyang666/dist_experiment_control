// Package xfervalue owns the neutral transfer-continuation value vocabulary:
// the declarations of xfercontract that do not name a transport. xfercontract
// re-exports every one of them by alias, so identities are unchanged.
//
// The split exists because xfercontract's ports legitimately name
// jetstream.JetStream, and Go imports whole packages: every importer of
// xfercontract reached the NATS client. proto aliases this vocabulary, and
// cluster/state, tunnel/transport, agentstate and every other proto importer
// must stay NATS-free (layering L-2); transferpath must stay transport-free.
// This file is P00's contracts.go moved verbatim; only this comment and the
// package clause differ (contractstub proves the moved digest).
// It carries values only: a declaration cannot authorize IO or ACK.
// origin: hot-update S7 plan §6.2 / §8.1 / §11.1 / P00 — NATS-free value split
package xfervalue

import (
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/xfercontract/xferport"
)

// ErrContractStub is wrapped only by contractstub. Production must never import
// that package; errors.Is remains usable without coupling callers to stub text.
var ErrContractStub = errors.New("not implemented")

type Deadline = controlbudget.Deadline
type Attempt = controlbudget.Attempt

// Clock binds local BOOTTIME to its boot. Cross-host messages carry remaining
// time instead; restoring a deadline from another boot cannot renew authority.
type Clock func() (bootID string, nowNS int64, err error)

const (
	Version        = "transfer-v1"
	StateSchema    = "xfer-state-v1"
	ResourceState  = "xfer-state-v1"
	ResourceSource = "xfer-source-v1"
	ResourceTmp    = "xfer-tmp-v1"
	ResourceParent = "xfer-parent-v1"
	StateFileName  = "state"
	SourceFileName = "source"
	TmpFileName    = "tmp"
	ParentFileName = "parent"
)

// These are the single stable code spellings. P01 maps wire fields to these
// values; P25/P27 classify them without inventing another error namespace.
const (
	CodeIntentConflict        = "transfer_intent_conflict"
	CodePermitExpired         = "transfer_permit_expired"
	CodeOwnerRecovering       = "transfer_owner_recovering"
	CodeResultUnknown         = "transfer_result_unknown"
	CodeObjectConflict        = "transfer_object_conflict"
	CodeObjectVersionMismatch = "transfer_object_version_mismatch"
	CodeSourceChanged         = "transfer_source_changed"
	CodeStorageHorizon        = "transfer_storage_horizon"
	CodePayloadUnsupported    = "transfer_payload_unsupported"
	CodeCapacityExceeded      = "transfer_capacity_exceeded"
	CodeTerminalConflict      = "transfer_terminal_conflict"
	CodeCommitInflight        = "transfer_commit_inflight"
	CodeDurabilityUnknown     = "transfer_durability_unknown"
	CodeReconcileRequired     = "transfer_reconcile_required"
	// Existing codes retain their meaning and spelling.
	CodeBudgetExceeded        = "transfer_budget_exceeded"
	CodeCanceled              = "transfer_canceled"
	CodeSessionDeleting       = "session_not_found_or_deleting"
	CodeNotMember             = "not_a_member"
	CodeNotOwner              = "not_owner_or_creator"
	CodeUnknown               = "transfer_unknown"
	ReasonSettlementAbandoned = "settlement_abandoned"
)

// All string enums have an invalid zero value. Unknown is an affirmative census
// result, never the default interpretation of a missing response.
type StatusState string

const (
	StatusUnknown  StatusState = "unknown"
	StatusLive     StatusState = "live"
	StatusTerminal StatusState = "terminal"
)

type Phase = xferport.Phase
type CommitPrimitive = xferport.CommitPrimitive
type PrimitiveOutcome = xferport.PrimitiveOutcome

const (
	PhaseOpen             = xferport.PhaseOpen
	PhasePrepared         = xferport.PhasePrepared
	PhaseSending          = xferport.PhaseSending
	PhaseReceiving        = xferport.PhaseReceiving
	PhaseCommitReserved   = xferport.PhaseCommitReserved
	PhaseDecided          = xferport.PhaseDecided
	PhaseTerminalPending  = xferport.PhaseTerminalPending
	PhaseHistoryConfirmed = xferport.PhaseHistoryConfirmed
	PhaseCleanupPending   = xferport.PhaseCleanupPending
	PhaseSettled          = xferport.PhaseSettled
	PrimitiveRename       = xferport.PrimitiveRename
	PrimitiveLink         = xferport.PrimitiveLink
	OutcomeNotEntered     = xferport.OutcomeNotEntered
	OutcomeInflight       = xferport.OutcomeInflight
	OutcomeLanded         = xferport.OutcomeLanded
)

// Cancellation expresses intent to close admission. Only the receiver outcome
// determines whether that request won the race with the commit primitive.
type CancelReason string

const (
	CancelUser     CancelReason = "user"
	CancelDeadline CancelReason = "deadline"
	CancelRevoked  CancelReason = "revoked"
	CancelStopped  CancelReason = "stopped"
)

// Raft commitment is deliberately distinct from exact physical history proof.
// Only HistoryConfirmed can discharge a canonical publication obligation.
type AuditDelivery string

const (
	DeliveryPending          AuditDelivery = "pending"
	DeliveryRaftCommitted    AuditDelivery = "raft_committed"
	DeliveryHistoryConfirmed AuditDelivery = "history_confirmed"
)

// Disposition belongs to the terminal decision. Pending and conflicting
// evidence retain the original slot rather than turning into a new transfer.
type TerminalDisposition string

const (
	DispositionPending  TerminalDisposition = "pending"
	DispositionComplete TerminalDisposition = "complete"
	DispositionFailed   TerminalDisposition = "failed"
	DispositionConflict TerminalDisposition = "conflict"
)

// Metadata and retirement each need their own ACK. A completed byte prefix
// cannot expose metadata early or authorize purging a late pending publish.
type ObjectWritePhase string

const (
	WriteOpen            ObjectWritePhase = "open"
	WritePending         ObjectWritePhase = "pending"
	WriteMetadataPending ObjectWritePhase = "metadata_pending"
	WriteFinalized       ObjectWritePhase = "finalized"
	WriteRetiring        ObjectWritePhase = "retiring"
	WriteRetired         ObjectWritePhase = "retired"
)

// Dirty connection barriers outlive the last reader reference. Stop must
// retain consumer deletion work until the server confirms its retirement.
type ReaderPhase string

const (
	ReaderOpen          ReaderPhase = "open"
	ReaderFetching      ReaderPhase = "fetching"
	ReaderDirty         ReaderPhase = "dirty"
	ReaderDeletePending ReaderPhase = "delete_pending"
	ReaderStopped       ReaderPhase = "stopped"
)

// Each mutation is serialized against a revision and the original entry
// nonce. Abandon is the explicit operator decision required by lead LQ1.
type LedgerMutationKind string

const (
	MutationAdmit      LedgerMutationKind = "admit"
	MutationSourceSeal LedgerMutationKind = "source_seal"
	MutationReserve    LedgerMutationKind = "reserve"
	MutationDecide     LedgerMutationKind = "decide"
	MutationHistory    LedgerMutationKind = "history"
	MutationCleanup    LedgerMutationKind = "cleanup"
	MutationAbandon    LedgerMutationKind = "abandon"
)

// New creation and retained continuation have different admission rules. A
// settlement grant never reauthorizes new byte work or ordinary commands.
type AccessKind string

const (
	AccessCreate   AccessKind = "create"
	AccessContinue AccessKind = "continue"
	AccessStatus   AccessKind = "status"
	AccessSettle   AccessKind = "settle"
)

// Operation is closed even though the transport is generic. The controller
// rejects unknown operations and payload fields belonging to another branch.
type Operation string

const (
	OpProbe        Operation = "probe"
	OpPermit       Operation = "permit"
	OpPrepare      Operation = "prepare"
	OpStatus       Operation = "status"
	OpSource       Operation = "source"
	OpReserve      Operation = "reserve"
	OpCommit       Operation = "commit"
	OpTerminal     Operation = "terminal"
	OpCancel       Operation = "cancel"
	OpFinalize     Operation = "finalize"
	OpCensus       Operation = "census"
	OpBucketConfig Operation = "bucket-config"
)

// Controls are generation-bound messages to an existing worker, not another
// creation path. Receipt delivery must match its exact canonical terminal.
type ControlKind string

const (
	ControlStart   ControlKind = "start"
	ControlGrant   ControlKind = "grant"
	ControlCancel  ControlKind = "cancel"
	ControlRevoke  ControlKind = "revoke"
	ControlReceipt ControlKind = "receipt"
)

// Settlement is a narrowly scoped authenticated role; possessing a capsule
// without the receiver NKey proof does not establish this principal.
type PrincipalRole string

const (
	RoleCTL        PrincipalRole = "ctl"
	RoleAgent      PrincipalRole = "agent"
	RoleBroker     PrincipalRole = "broker"
	RoleSettlement PrincipalRole = "transfer-settlement"
)

type Identity = xferport.Identity

type Key struct{ SID, TransferID string }

// A retry stays bound to the original process and boot, even after rehoming.
// A missing or unready origin is unavailable, never an authoritative absence.
type Origin struct {
	BrokerID   string `json:"broker_id"`
	InstanceID string `json:"instance_id"`
	BootID     string `json:"boot_id"`
	Generation uint64 `json:"generation"`
}

// Intent stays immutable after its digest is sealed. A later pull source seal
// supplies unknown source facts without rewriting the original request.
type Intent struct {
	Verb         string `json:"verb"`
	Tier         string `json:"tier"`
	Path         string `json:"path"`
	SHA256       string `json:"sha256"`
	Size         int64  `json:"size"`
	Force        bool   `json:"force"`
	RateLimitBPS int64  `json:"rate_limit_bps"`
	ChunkSize    uint32 `json:"chunk_size"`
}

// Expiry limits first creation only. An existing entry must be reconciled
// before checking the permit, so an expired permit cannot erase live work.
type CreationPermit struct {
	Identity    Identity  `json:"identity"`
	Origin      Origin    `json:"origin"`
	IssuedAt    time.Time `json:"issued_at"`
	CreateUntil time.Time `json:"create_until"`
	PermitNonce string    `json:"permit_nonce"`
	Signature   string    `json:"signature"`
}

// StartedAt never moves on reconnect or source-size discovery. RemainingNS
// crosses hosts; receivers deduct elapsed request time instead of resetting it.
type BudgetSeal struct {
	StartedAt         time.Time `json:"started_at"`
	Size              int64     `json:"size"`
	RateLimitBPS      int64     `json:"rate_limit_bps"`
	EffectiveBudgetNS int64     `json:"effective_budget_ns"`
	RemainingNS       int64     `json:"remaining_ns"`
	SourceSealed      bool      `json:"source_sealed"`
}

// The fingerprint binds the pinned source stat identity without publishing a
// path. Size, digest and geometry must agree before the first object chunk.
type SourceSeal struct {
	Size              int64  `json:"size"`
	SHA256            string `json:"sha256"`
	ChunkSize         uint32 `json:"chunk_size"`
	Chunks            uint32 `json:"chunks"`
	RateLimitBPS      int64  `json:"rate_limit_bps"`
	SourceFingerprint string `json:"source_fingerprint"`
}

type CommitGrant = xferport.CommitGrant
type ReceiverOutcome = xferport.ReceiverOutcome

// A terminal receipt proves exact canonical bytes in history. A raft index,
// Flush or an arbitrary terminal with the same transfer ID is insufficient.
type TerminalReceipt struct {
	TerminalID      string `json:"terminal_id"`
	Kind            string `json:"kind"`
	CanonicalDigest string `json:"canonical_digest"`
	ReqID           string `json:"req_id"`
	HistoryStream   string `json:"history_stream"`
	HistorySeq      uint64 `json:"history_seq"`
	Bytes           int64  `json:"bytes"`
	SHA256          string `json:"sha256"`
	Code            string `json:"code"`
	// Operator abandonment is a failed record, never proof of no effect.
	// origin: hot-update S7 plan lead LQ1
	OutcomeUnknown bool `json:"outcome_unknown,omitempty"`
}

// Capsules stay in private state and authenticated settlement messages. Status
// deliberately has no capsule field, so a read-only query cannot disclose it.
type SettlementCapsule struct {
	Identity     Identity `json:"identity"`
	Origin       Origin   `json:"origin"`
	ReceiverNkey string   `json:"receiver_nkey"`
	Issuer       string   `json:"issuer"`
	Nonce        string   `json:"nonce"`
	Signature    string   `json:"signature"`
}

// Public bundles contain only this bounded reference. Detailed actor state,
// canonical bytes and settlement authentication stay in the sealed codec.
type ImageRef struct {
	Schema       string `json:"schema"`
	ResourceID   string `json:"resource_id"`
	Digest       string `json:"digest"`
	Count        uint32 `json:"count"`
	EncodedBytes uint64 `json:"encoded_bytes"`
}

// StreamCreated fixes the storage incarnation as well as the stream name.
// Recreation/reset is not a continuation of the original object version.
type ObjectSpec struct {
	Identity                              Identity
	Bucket, Stream, StreamCreated, SHA256 string
	Size                                  int64
	ChunkSize, Chunks                     uint32
}

// Index is zero-based and StreamSeq is the raw readback sequence. Global
// stream gaps are legal; they are not missing logical transfer chunks.
type ChunkReceipt struct {
	Index     uint32
	StreamSeq uint64
	Offset    int64
	Length    uint32
	SHA256    [32]byte
}
type Chunk struct {
	Receipt ChunkReceipt
	Data    []byte
}

// ChunkIndex counts complete synced chunks; zero is the empty prefix. Neither
// fetched bytes nor a consumer delivery ordinal can advance this checkpoint.
type SinkCheckpoint struct {
	ChunkIndex            uint32
	DurableOffset         int64
	LastConsumedStreamSeq uint64
	SHA256State           []byte
}

// Bytes are already normalized by the audit owner. Retries publish these
// exact bytes rather than recomputing timestamps, request IDs or digests.
type CanonicalRecord struct {
	Identity            Identity
	Kind, ReqID, Digest string
	Bytes               []byte
}
type HistoryReceipt struct {
	Stream, RecordDigest string
	Sequence             uint64
}

// Complete covers the full matching subject scan through ScannedThrough.
// Finding the first terminal is insufficient to exclude contradictory rows.
type HistorySet struct {
	Start, Terminal []CanonicalRecord
	Receipts        []HistoryReceipt
	ScannedThrough  uint64
	Complete        bool
}

// Terminal requires exact history proof; live includes pending settlement.
// Unknown is allowed only after a complete ready census at the original owner.
type Status struct {
	State                 StatusState
	Phase                 Phase
	Identity              Identity
	Origin                Origin
	Budget                BudgetSeal
	DurableOffset         int64
	ChunkIndex            uint32
	LastConsumedStreamSeq uint64
	Terminal              *TerminalReceipt
	CleanupPending        bool
	Code, Error           string
}
type StatusRequest struct {
	CallID   string
	Identity Identity
	Origin   Origin
}
type ReserveRequest struct {
	CallID           string
	Origin           Origin
	ReceiverInstance string
}
type CancelRequest struct {
	CallID string
	Origin Origin
	Reason CancelReason
}

// Inline bytes are copied into reserved worker state before prepare succeeds.
// Replaying a capable prepare returns that state without writing them again.
type Prepare struct {
	CallID     string
	Identity   Identity
	Origin     Origin
	Intent     Intent
	Permit     CreationPermit
	Budget     BudgetSeal
	Source     *SourceSeal
	Settlement *SettlementCapsule
	Inline     []byte
}

// Request is a tagged union. Each operation validates its exact field set;
// bucket configuration uses ConfigPatch rather than a public arbitrary map.
type Request struct {
	Op       Operation
	CallID   string
	Identity Identity
	Origin   Origin
	Intent   *Intent
	Permit   *CreationPermit
	Source   *SourceSeal
	Reserve  *ReserveRequest
	Outcome  *ReceiverOutcome
	Cancel   *CancelRequest
	// Authenticated prepare-only payload; retained before the first send.
	// origin: hot-update S7 plan §8.1 / INTEG-D carrier amendment
	Inline []byte
}
type Response struct {
	Status  *Status
	Permit  *CreationPermit
	Grant   *CommitGrant
	Receipt *TerminalReceipt
	Probe   *ProbeResult
	// Prepare-only carriers must never be returned by a status query.
	// origin: hot-update S7 plan §6.4 / INTEG-D carrier amendment
	Inline      []byte
	Source      *SourceSeal
	Settlement  *SettlementCapsule
	Code, Error string
}
type ProbeResult struct {
	Version, HistoryGuardVersion, BucketConfigVersion, InstanceID string
	MaxPayload, MaxMsgSize                                        int64
	Code, Error                                                   string
}
type Principal struct {
	NKey, FP, SID, NID, InstanceID string
	Role                           PrincipalRole
}
type Control struct {
	Kind     ControlKind
	Identity Identity
	Grant    *CommitGrant
	Outcome  *ReceiverOutcome
	Receipt  *TerminalReceipt
	Reason   CancelReason
}

// Reservations include simultaneous codec copies, original descriptors,
// Prepared duplicates and custody witnesses, not just the steady-state bytes.
type Reservation struct {
	ReplayBytes, NonReplayBytes, SealedBytes, PublicBytes, HandoffBytes int64
	FDs, Resources                                                      uint32
}
type ReservationID string
type Salvage = xferport.Salvage[handoff.Resource]

// Legacy has no history receipt and cannot satisfy the capable invocation
// interface. BestEffort must remain explicit through the caller presentation.
type LegacyResult struct {
	Kind                string
	Bytes               int64
	SHA256, Code, Error string
	BestEffort          bool
}
