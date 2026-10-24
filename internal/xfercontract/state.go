package xfercontract

import (
	"time"

	"github.com/LinZiyang666/tether/internal/xfercontract/xferport"
	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// Private state belongs in xfer-state-v1 sealed resources. These records are
// codec inputs, not public image JSON; ImageRef is the only public image root.
// Presence pointers distinguish absent evidence from a zero-valued receipt.
// origin: hot-update S7 plan §2–§5 / §8.1–§8.3

type ResourceRef = xferport.ResourceRef
type FileIdentity = xferport.FileIdentity

type Lineage struct {
	InstanceID, BootID string
	Generation         uint64
}
type BootDeadline = xfervalue.BootDeadline

// An attempt retains the original bytes AND both fences. A retry cannot mint a
// new attachment or change the original headers merely because its ACK was lost.
type PendingAttempt struct {
	Index                uint32
	Offset               int64
	Length               uint32
	SHA256               [32]byte
	Bytes                []byte
	PreviousSubjectSeq   uint64
	ExpectedStreamSeq    uint64
	AttachmentGeneration uint64
}

// Keep the original SDK metadata bytes and sequence for exact lost-ACK
// readback. Foreign metadata at the same name must never be adopted.
type ObjectReceipt struct {
	Spec           ObjectSpec
	MetadataSeq    uint64
	MetadataDigest string
	Metadata       []byte
}

// The fence ACK must be later than every old attempt stream expectation.
// Stopping a goroutine does not prove a sent HPUB cannot arrive after purge.
type RetirementFence struct {
	ExpectedStreamSeq   uint64
	PreviousMetadataSeq uint64
	Receipt             *HistoryReceipt
	WriterRetired       bool
	ConsumersRetired    bool
	Purged              bool
}

// Freeze retains the single pending attempt as well as committed receipts.
// Omitting either side permits a duplicate physical chunk after an ACK loss.
type WriterState struct {
	ObjectSpec           ObjectSpec
	Phase                ObjectWritePhase
	Receipts             []ChunkReceipt
	PendingAttempt       *PendingAttempt
	MetaReceipt          *ObjectReceipt
	AttachmentGeneration uint64
	RetirementFence      RetirementFence
}

// Only the synced checkpoint crosses an image. Fetched but unsynced data is
// discarded by the old owner before export, never repaired during Import.
type ReaderState struct {
	ObjectSpec         ObjectSpec
	Phase              ReaderPhase
	SinkCheckpoint     SinkCheckpoint
	ConsumerIdentity   string
	ConnectionIdentity string
	DirtyBarrier       bool
	DeletePending      bool
}

type AtomicState = xferport.AtomicState

// Source remains the original pinned file through cuts. Offset follows
// confirmed object receipts; it cannot advance merely because a read finished.
type SenderState struct {
	Identity       Identity
	Intent         Intent
	Budget         BudgetSeal
	ByteDeadline   BootDeadline
	Source         ResourceRef
	SourceIdentity FileIdentity
	SourceSeal     SourceSeal
	Writer         WriterState
	Offset         int64
	SHA256State    []byte
	Stopped        bool
	// Inline is a Tier A pull's exact copy of the sealed source: the only byte
	// carrier of a transfer without an object version. nil means not yet
	// copied (an empty source copies to a non-nil empty slice); a prepare
	// replay and every image cut reuse these bytes, never a re-read.
	// origin: hot-update S7 plan §8.1 / FIX-INLINE
	Inline []byte
}

// GateClosed survives late grants and control replays. InlineOffset retains
// the tier-A prefix even though that transfer has no object-store reader.
type ReceiverState struct {
	Identity     Identity
	Budget       BudgetSeal
	ByteDeadline BootDeadline
	Reader       ReaderState
	Atomic       AtomicState
	Checkpoint   SinkCheckpoint
	InlineOffset int64
	GateClosed   bool
	Grant        CommitGrant
	Outcome      ReceiverOutcome
	Stopped      bool
}

// Byte-deadline expiry does not retire a courier. It retains authentication,
// exact outcome and retry/ACK responsibility until history is confirmed.
type CourierState struct {
	Identity    Identity
	Outcome     ReceiverOutcome
	Canonical   CanonicalRecord
	Capsule     SettlementCapsule
	Reply       string
	Attempt     Attempt
	Receipt     *TerminalReceipt
	NextAttempt BootDeadline
	Cleanup     CleanupState
	Stopped     bool
}

// Frozen admission and revocation survive before any imported actor runs.
// Generation/nonce checks reject callbacks belonging to an earlier owner.
type AdmissionState struct {
	Closed     bool
	Generation uint64
	Inflight   uint32
}
type Revocation struct {
	Identity Identity
	Reason   CancelReason
	At       BootDeadline
}

// Prepared-only entries still retain the transfer image domain. An empty
// byte cursor is not permission to drop admission or the creation fence.
type WorkerEntry struct {
	Prepare         Prepare
	Status          Status
	OwnerGeneration uint64
	Started         bool
	GateClosed      bool
	Reservation     ReservationID
}

// Outcome and canonical bytes are one decision. Import must reject a digest
// or receipt that describes different bytes instead of silently normalizing it.
type CanonicalOutcome struct {
	Identity  Identity
	Outcome   ReceiverOutcome
	Canonical CanonicalRecord
	Receipt   *TerminalReceipt
}

// Both creation expiry and result retention must pass before forgetting a
// settled entry. Unsettled grants/publications are not governed by this TTL.
type RetentionState struct {
	Identity    Identity
	CreateUntil time.Time
	ResultUntil time.Time
	Deadline    BootDeadline
}
type CapacityReservation struct {
	ID          ReservationID
	Reservation Reservation
}

// ConsumerDelete is a server responsibility, never a socket or SDK handle.
// Keep it in the sealed transfer image, including duties with no live reader.
// origin: S7 internal review rv-jsnats F3
type ConsumerDelete struct {
	Stream        string
	StreamCreated string
	Consumer      string
}

// ConsumerCleanupState carries one independent cleanup owner across an exec.
// Socket barriers cannot cross exec: Freeze requires those to be proven first.
type ConsumerCleanupState struct {
	OwnerID string
	Deletes []ConsumerDelete
}

// Actor collections are keyed by full Identity, not their slice positions.
// Codec validation closes references, revocations and capacity as one union.
type WorkerState struct {
	Schema               string
	Lineage              Lineage
	Admission            AdmissionState
	Revocations          []Revocation
	Entries              []WorkerEntry
	Senders              []SenderState
	Receivers            []ReceiverState
	CanonicalOutcomes    []CanonicalOutcome
	Couriers             []CourierState
	ResultRetention      []RetentionState
	CapacityReservations []CapacityReservation
	ConsumerCleanup      []ConsumerCleanupState
}

// A ledger row is a whole immutable decision at one revision. Mutations name
// their expected revision at the port; START cannot replace a staged TERMINAL.
// LQ1's OutcomeUnknown accompanies only canonical failed abandonment evidence.
type LedgerRow struct {
	Identity          Identity
	Origin            Origin
	Permit            CreationPermit
	Intent            Intent
	EntryNonce        string
	Revision          uint64
	SourceSeal        *SourceSeal
	StartedAt         time.Time
	BudgetSeal        BudgetSeal
	Phase             Phase
	Grant             *CommitGrant
	ReceiverOutcome   *ReceiverOutcome
	CanonicalStart    *CanonicalRecord
	CanonicalTerminal *CanonicalRecord
	StartDelivery     AuditDelivery
	TerminalDelivery  AuditDelivery
	HistoryReceipts   []HistoryReceipt
	Cleanup           CleanupState
	Retention         RetentionState
	Settlement        *SettlementCapsule
	OutcomeUnknown    bool
}

// Row carries the complete proposed revision. The store checks monotonic
// additions and nonce identity instead of overwriting from an old START copy.
type LedgerMutation struct {
	Kind       LedgerMutationKind
	EntryNonce string
	Row        LedgerRow
}

// Ready+Complete are affirmative evidence. Empty Entries with either false
// cannot authorize unknown status, object GC or deletion of session history.
type LedgerCensus struct {
	Origin   Origin
	Ready    bool
	Complete bool
	Entries  []Protection
	Rows     []LedgerRow
	Digest   string
	Code     string
}

// GC needs the original version and owner generation as positive evidence.
// Object age or absence from the current home tracker is not a retirement proof.
type Protection struct {
	Identity        Identity
	Phase           Phase
	ObjectVersion   string
	CreationUntil   time.Time
	WriterRetired   bool
	OwnerGeneration uint64
}

// Pending cleanup continues through the finalizer even while live protection
// shields the entry from reaping. Protection cannot disable its own progress.
type CleanupState struct {
	Identity             Identity
	WriterFence          RetirementFence
	Consumers            []ReaderState
	TmpUnlinkPending     bool
	DirectorySyncPending bool
	ObjectPurgePending   bool
	PrimaryRetirePending bool
	OutboxRetirePending  bool
}

// Cleanup facts are independently acknowledged. Closing a data attachment
// cannot stand in for consumer deletion, directory Sync or an object fence.
type CleanupReceipt struct {
	Identity         Identity
	OwnerGeneration  uint64
	WriterRetired    bool
	ConsumersRetired bool
	FenceSequence    uint64
	ObjectPurged     bool
	TmpUnlinked      bool
	DirectorySynced  bool
	Digest           string
}

// Retire is bound to key, nonce and revision. This prevents delayed cleanup
// from consuming a newer row while preserving the primary-before-outbox order.
type RetirementProof struct {
	Identity         Identity
	Origin           Origin
	OwnerGeneration  uint64
	EntryNonce       string
	Revision         uint64
	CreationUntil    time.Time
	WriterRetired    bool
	ReceiverRetired  bool
	ConsumersRetired bool
	StartReceipt     HistoryReceipt
	TerminalReceipt  HistoryReceipt
	Cleanup          CleanupReceipt
}
type ReapedEntry struct {
	Identity  Identity
	At        time.Time
	Retention RetentionState
}
type WatchdogDeadline struct {
	Identity     Identity
	ByteDeadline BootDeadline
	Fired        bool
}
type GrantSlot struct {
	Identity Identity
	Grant    CommitGrant
	Revision uint64
}

// Sleeping imported entries protect both primary and outbox before any
// reaper, watchdog or eager cold scan runs against the new backend.
type TrackerState struct {
	Schema                        string
	Origin                        Origin
	Generation                    uint64
	Admission                     AdmissionState
	ProvisioningFence             AdmissionState
	Entries                       []LedgerRow
	Reaped                        []ReapedEntry
	WatchdogDeadlines             []WatchdogDeadline
	PendingStart, PendingTerminal []CanonicalRecord
	GrantSlots                    []GrantSlot
	LedgerRevision                uint64
	Census                        LedgerCensus
	Cleanup                       []CleanupState
	ConfigAttempts                []ConfigAttempt
}

// Patches contain intent, not a stale complete StreamConfig. The coordinator
// reads/copies the entire server config before applying only its owned fields.
type ConfigPatch struct {
	Bucket          string
	Replicas        int
	MaxBytes        int64
	Duplicates      time.Duration
	RecoveryHorizon time.Duration
}

// Limits are observed server/account facts, not permission to silently lower
// geometry, shorten retention or change an operator-owned stream setting.
type StorageLimits struct {
	DiskBytes, AccountMaxBytes, ServerMaxBytes int64
	MaxPayload, MaxMsgSize                     int64
	MaxDuplicates                              time.Duration
}

// This is a readback of the full configured stream before first-byte access.
// A successful UPDATE send without its definite response is not a seal.
type BucketSeal struct {
	Bucket, Stream, StreamCreated, ConfigDigest string
	Replicas                                    int
	MaxBytes                                    int64
	Duplicates, MaxAge                          time.Duration
	MaxPayload, MaxMsgSize                      int64
	Proof                                       ConfigWriterProof
}

// Leader identity alone cannot retire an old sent-unknown config write. Both
// the future gate and all earlier requests must be conclusively closed.
type ConfigWriterProof struct {
	BrokerID, InstanceID, BootID, Bucket, Nonce string
	Generation                                  uint64
	GateClosed                                  bool
	RequestsJoined                              bool
}

// Unknown attempts survive handoff. INFO equality does not establish that a
// delayed full-config update has left the network and cannot overwrite it later.
type ConfigAttempt struct {
	Patch       ConfigPatch
	Proof       ConfigWriterProof
	Attempt     Attempt
	Seal        *BucketSeal
	SentUnknown bool
}
type ConfigWriterState struct {
	Admission AdmissionState
	Proofs    []ConfigWriterProof
	Attempts  []ConfigAttempt
}

// A settlement capsule is not a bearer token. Authentication still proves
// possession of the original receiver key over the CONNECT challenge.
type NKeyProof struct {
	NKey             string
	Nonce, Signature []byte
}

// Verification grants only this retained entry at its original owner. The
// caller must not interpret it as renewed membership or ordinary OBJ access.
type SettlementGrant struct {
	Identity        Identity
	Origin          Origin
	ReceiverNkey    string
	EntryNonce      string
	OwnerGeneration uint64
}

// Observation and its kinds live in the NATS-free value package so the
// filesystem leaf can emit them; the declarations and their rationale moved
// verbatim to xfervalue/state.go. These aliases keep every identity.
// origin: hot-update S7 plan §10.4 / §11.1 / P00 — NATS-free value split
type ObservationKind = xfervalue.ObservationKind

const (
	ObserveAppend         ObservationKind = xfervalue.ObserveAppend
	ObserveResolve        ObservationKind = xfervalue.ObserveResolve
	ObserveFinalize       ObservationKind = xfervalue.ObserveFinalize
	ObserveFetched        ObservationKind = xfervalue.ObserveFetched
	ObserveSynced         ObservationKind = xfervalue.ObserveSynced
	ObserveReaderStop     ObservationKind = xfervalue.ObserveReaderStop
	ObservePrimitiveEnter ObservationKind = xfervalue.ObservePrimitiveEnter
	ObservePrimitiveExit  ObservationKind = xfervalue.ObservePrimitiveExit
	ObserveDirectorySync  ObservationKind = xfervalue.ObserveDirectorySync
	ObserveFreeze         ObservationKind = xfervalue.ObserveFreeze
	ObserveImport         ObservationKind = xfervalue.ObserveImport
	ObserveControl        ObservationKind = xfervalue.ObserveControl
	ObserveAcknowledge    ObservationKind = xfervalue.ObserveAcknowledge
	ObserveReserve        ObservationKind = xfervalue.ObserveReserve
	ObserveFinish         ObservationKind = xfervalue.ObserveFinish
	ObserveCallbackEnter  ObservationKind = xfervalue.ObserveCallbackEnter
	ObserveCallbackJoin   ObservationKind = xfervalue.ObserveCallbackJoin
	ObserveLedgerSync     ObservationKind = xfervalue.ObserveLedgerSync
	ObserveHistory        ObservationKind = xfervalue.ObserveHistory
	ObserveRetirement     ObservationKind = xfervalue.ObserveRetirement
	ObserveInstall        ObservationKind = xfervalue.ObserveInstall
	ObserveRollback       ObservationKind = xfervalue.ObserveRollback
	ObserveAttempt        ObservationKind = xfervalue.ObserveAttempt
	ObserveDrain          ObservationKind = xfervalue.ObserveDrain
)

type Observation = xfervalue.Observation
