package proto

import (
	"time"

	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// proto imports the NATS-free half of the transfer contract. xfercontract
// itself names jetstream.JetStream in its ports, and every package that imports
// proto (cluster/state, tunnel/transport, agentstate, ...) must stay NATS-free.
// origin: hot-update S7 plan §6.2 / §11.1 / P00 — NATS-free value split
// Wire and leaf owners use one vocabulary. Aliases preserve enum identity while
// the wire structs stay separate from private runtime state and its codecs.
// origin: hot-update S7 plan §6.2 / P01
const TransferVersion = xfervalue.Version

// Keep the complete reader advertisement here with the additive domain so the
// legacy version emitter stays within its frozen per-file code ceiling.
// origin: hot-update S7 plan §6.2 / §8.4 / P01
const transferImageDomainsLine = "image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1"

type StatusState = xfervalue.StatusState
type Phase = xfervalue.Phase
type CommitPrimitive = xfervalue.CommitPrimitive
type PrimitiveOutcome = xfervalue.PrimitiveOutcome
type CancelReason = xfervalue.CancelReason

// The public reference is already a wire-tagged neutral value, unlike private
// worker/tracker state. Keep one shape for both bundle producers and consumers.
// origin: hot-update S7 plan §6.2 / §8.2 / P01
type ImageRef = xfervalue.ImageRef

const (
	StatusUnknown  = xfervalue.StatusUnknown
	StatusLive     = xfervalue.StatusLive
	StatusTerminal = xfervalue.StatusTerminal

	PhaseOpen             = xfervalue.PhaseOpen
	PhasePrepared         = xfervalue.PhasePrepared
	PhaseSending          = xfervalue.PhaseSending
	PhaseReceiving        = xfervalue.PhaseReceiving
	PhaseCommitReserved   = xfervalue.PhaseCommitReserved
	PhaseDecided          = xfervalue.PhaseDecided
	PhaseTerminalPending  = xfervalue.PhaseTerminalPending
	PhaseHistoryConfirmed = xfervalue.PhaseHistoryConfirmed
	PhaseCleanupPending   = xfervalue.PhaseCleanupPending
	PhaseSettled          = xfervalue.PhaseSettled

	PrimitiveRename   = xfervalue.PrimitiveRename
	PrimitiveLink     = xfervalue.PrimitiveLink
	OutcomeNotEntered = xfervalue.OutcomeNotEntered
	OutcomeInflight   = xfervalue.OutcomeInflight
	OutcomeLanded     = xfervalue.OutcomeLanded
	CancelUser        = xfervalue.CancelUser
	CancelDeadline    = xfervalue.CancelDeadline
	CancelRevoked     = xfervalue.CancelRevoked
	CancelStopped     = xfervalue.CancelStopped
)

// Unknown result and recovering owner retain responsibility; they never assert
// an authoritative absence. Storage and payload refusals precede the first byte.
// CodeTransferBudgetExceeded already exists in codes.go with its legacy spelling.
// origin: hot-update S7 plan §1.2 / §6.1 / lead LQ1–LQ3 / P01
const (
	CodeTransferIntentConflict        = xfervalue.CodeIntentConflict
	CodeTransferPermitExpired         = xfervalue.CodePermitExpired
	CodeTransferOwnerRecovering       = xfervalue.CodeOwnerRecovering
	CodeTransferResultUnknown         = xfervalue.CodeResultUnknown
	CodeTransferObjectConflict        = xfervalue.CodeObjectConflict
	CodeTransferObjectVersionMismatch = xfervalue.CodeObjectVersionMismatch
	CodeTransferSourceChanged         = xfervalue.CodeSourceChanged
	CodeTransferStorageHorizon        = xfervalue.CodeStorageHorizon
	CodeTransferPayloadUnsupported    = xfervalue.CodePayloadUnsupported
	CodeTransferCapacityExceeded      = xfervalue.CodeCapacityExceeded
	CodeTransferTerminalConflict      = xfervalue.CodeTerminalConflict
	CodeTransferCommitInflight        = xfervalue.CodeCommitInflight
	CodeTransferDurabilityUnknown     = xfervalue.CodeDurabilityUnknown
	CodeTransferReconcileRequired     = xfervalue.CodeReconcileRequired
	CodeTransferCanceled              = xfervalue.CodeCanceled
	CodeTransferUnknown               = xfervalue.CodeUnknown
	ReasonSettlementAbandoned         = xfervalue.ReasonSettlementAbandoned
)

// These are public wire values, not aliases to a worker snapshot. Required
// fields deliberately lack omitempty: missing authority cannot become success.
// The roundtrip manifest pins their mapping to the frozen neutral contracts.
// origin: hot-update S7 plan §6.2 / P01
type Identity struct {
	SID           string `json:"sid"`
	NID           string `json:"nid"`
	ActorFP       string `json:"actor_fp"`
	AgentInstance string `json:"agent_instance"`
	TransferID    string `json:"transfer_id"`
	EntryNonce    string `json:"entry_nonce"`
	IntentDigest  string `json:"intent_digest"`
	ObjectVersion string `json:"object_version"`
}
type Origin struct {
	BrokerID   string `json:"broker_id"`
	InstanceID string `json:"instance_id"`
	BootID     string `json:"boot_id"`
	Generation uint64 `json:"generation"`
}
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
type CreationPermit struct {
	Identity    Identity  `json:"identity"`
	Origin      Origin    `json:"origin"`
	IssuedAt    time.Time `json:"issued_at"`
	CreateUntil time.Time `json:"create_until"`
	PermitNonce string    `json:"permit_nonce"`
	Signature   string    `json:"signature"`
}
type BudgetSeal struct {
	StartedAt         time.Time `json:"started_at"`
	Size              int64     `json:"size"`
	RateLimitBPS      int64     `json:"rate_limit_bps"`
	EffectiveBudgetNS int64     `json:"effective_budget_ns"`
	RemainingNS       int64     `json:"remaining_ns"`
	SourceSealed      bool      `json:"source_sealed"`
}
type SourceSeal struct {
	Size              int64  `json:"size"`
	SHA256            string `json:"sha256"`
	ChunkSize         uint32 `json:"chunk_size"`
	Chunks            uint32 `json:"chunks"`
	RateLimitBPS      int64  `json:"rate_limit_bps"`
	SourceFingerprint string `json:"source_fingerprint"`
}
type TransferPermitReq struct {
	CallID        string `json:"call_id"`
	Intent        Intent `json:"intent"`
	TransferID    string `json:"transfer_id"`
	AgentInstance string `json:"agent_instance"`
}
type TransferPermitResp struct {
	Permit *CreationPermit `json:"permit"`
	Code   string          `json:"code"`
	Error  string          `json:"error"`
}
type TransferProbeReq struct {
	CallID string `json:"call_id"`
	SID    string `json:"sid"`
	NID    string `json:"nid"`
}
type TransferProbeResp struct {
	Version             string `json:"version"`
	HistoryGuardVersion string `json:"history_guard_version"`
	BucketConfigVersion string `json:"bucket_config_version"`
	InstanceID          string `json:"instance_id"`
	MaxPayload          int64  `json:"max_payload"`
	MaxMsgSize          int64  `json:"max_msg_size"`
	Code                string `json:"code"`
	Error               string `json:"error"`
}
type TransferStatusReq struct {
	CallID   string   `json:"call_id"`
	Identity Identity `json:"identity"`
	Origin   Origin   `json:"origin"`
}
type TransferStatusResp struct {
	State                 StatusState      `json:"state"`
	Phase                 Phase            `json:"phase"`
	Identity              Identity         `json:"identity"`
	Origin                Origin           `json:"origin"`
	Budget                BudgetSeal       `json:"budget"`
	DurableOffset         int64            `json:"durable_offset"`
	ChunkIndex            uint32           `json:"chunk_index"`
	LastConsumedStreamSeq uint64           `json:"last_consumed_stream_seq"`
	Terminal              *TerminalReceipt `json:"terminal"`
	CleanupPending        bool             `json:"cleanup_pending"`
	Code                  string           `json:"code"`
	Error                 string           `json:"error"`
}
type TransferSourceReq struct {
	CallID   string     `json:"call_id"`
	Identity Identity   `json:"identity"`
	Origin   Origin     `json:"origin"`
	Source   SourceSeal `json:"source"`
}
type TransferReserveReq struct {
	CallID           string   `json:"call_id"`
	Identity         Identity `json:"identity"`
	Origin           Origin   `json:"origin"`
	ReceiverInstance string   `json:"receiver_instance"`
}
type TransferReserveResp struct {
	Grant *CommitGrant `json:"grant"`
	Code  string       `json:"code"`
	Error string       `json:"error"`
	// The source the sender sealed into the original row. A pull receiver
	// lands only bytes matching it, so a self-consistent forged object at the
	// fixed version can never be committed even when prepare carried no seal.
	// origin: hot-update S7 plan §2.2 / §4 / INTEG-D pull source seal
	Source *SourceSeal `json:"source,omitempty"`
}
type CommitGrant struct {
	Identity         Identity `json:"identity"`
	GrantNonce       string   `json:"grant_nonce"`
	ReceiverInstance string   `json:"receiver_instance"`
	TerminalID       string   `json:"terminal_id"`
}
type ReceiverOutcome struct {
	Grant             CommitGrant      `json:"grant"`
	Primitive         CommitPrimitive  `json:"primitive"`
	Outcome           PrimitiveOutcome `json:"outcome"`
	Bytes             int64            `json:"bytes"`
	SHA256            string           `json:"sha256"`
	Code              string           `json:"code"`
	Error             string           `json:"error"`
	ParentSynced      bool             `json:"parent_synced"`
	TmpCleanupPending bool             `json:"tmp_cleanup_pending"`
}
type TransferTerminalReq struct {
	CallID   string          `json:"call_id"`
	Identity Identity        `json:"identity"`
	Origin   Origin          `json:"origin"`
	Outcome  ReceiverOutcome `json:"outcome"`
}
type TransferTerminalResp struct {
	Receipt *TerminalReceipt `json:"receipt"`
	Code    string           `json:"code"`
	Error   string           `json:"error"`
	// The exact decision bytes accompany only an exact receipt.
	// origin: hot-update S7 plan §4 / §5.1 / INTEG-D courier canonical carrier
	Canonical *TransferCanonical `json:"canonical,omitempty"`
}
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
	// An abandoned settlement proves failure to settle, never absence of a filesystem effect.
	// origin: hot-update S7 plan lead LQ1
	OutcomeUnknown bool `json:"outcome_unknown,omitempty"`
}
type TransferCancelReq struct {
	CallID   string       `json:"call_id"`
	Identity Identity     `json:"identity"`
	Origin   Origin       `json:"origin"`
	Reason   CancelReason `json:"reason"`
}
type TransferCensusReq struct {
	CallID string `json:"call_id"`
	SID    string `json:"sid"`
	Origin Origin `json:"origin"`
}
type TransferCensusResp struct {
	Origin  Origin       `json:"origin"`
	Ready   bool         `json:"ready"`
	Entries []Protection `json:"entries"`
	Digest  string       `json:"digest"`
	Code    string       `json:"code"`
}
type Protection struct {
	Identity        Identity  `json:"identity"`
	Phase           Phase     `json:"phase"`
	ObjectVersion   string    `json:"object_version"`
	CreationUntil   time.Time `json:"creation_until"`
	WriterRetired   bool      `json:"writer_retired"`
	OwnerGeneration uint64    `json:"owner_generation"`
}
type TransferContinuation struct {
	// The complete original intent keeps rate, tier and geometry authenticated
	// across prepare forwarding; source discovery cannot rewrite its digest.
	// origin: hot-update S7 plan §6.1 / INTEG-D carrier amendment
	Intent     *Intent            `json:"intent,omitempty"`
	Version    string             `json:"version"`
	Permit     *CreationPermit    `json:"permit"`
	Identity   Identity           `json:"identity"`
	Origin     Origin             `json:"origin"`
	Budget     BudgetSeal         `json:"budget"`
	Source     *SourceSeal        `json:"source"`
	Settlement *SettlementCapsule `json:"settlement"`
	// Only the deciding broker attaches a durable Grant, and only on its
	// directed commit delivery to the original receiver. A ctl request or any
	// response carrying one is refused, so no client can mint receiver authority.
	// origin: hot-update S7 plan §4 / §6.3 / INTEG-D outer commit adapter
	Grant *CommitGrant `json:"grant,omitempty"`
}

// TransferCanonical carries the deciding broker's exact canonical audit bytes
// beside a terminal receipt. A receiver courier binds them before it accepts the
// ACK; it never reconstructs a broker timestamp or ReqID itself.
// origin: hot-update S7 plan §4 / §5.1 / INTEG-D courier canonical carrier
type TransferCanonical struct {
	Kind   string `json:"kind"`
	ReqID  string `json:"req_id"`
	Digest string `json:"digest"`
	Bytes  []byte `json:"bytes"`
}

// Settlement capsules are only creation/settlement carriers. Status has no
// capsule field, so a public query cannot accidentally expose its authority.
type SettlementCapsule struct {
	Identity     Identity `json:"identity"`
	Origin       Origin   `json:"origin"`
	ReceiverNkey string   `json:"receiver_nkey"`
	Issuer       string   `json:"issuer"`
	Nonce        string   `json:"nonce"`
	Signature    string   `json:"signature"`
}
