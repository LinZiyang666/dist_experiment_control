package xfercontract

import (
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/xfercontract/xferport"
	"github.com/nats-io/nats.go/jetstream"
)

// Ports are neutral so implementations never import an outer manager merely to
// name a callback. Deadlines bound operations; explicit Stop owns lifetimes.
// origin: hot-update S7 plan §8.1 / §11.1 / P00

// Reserve runs before FD allocation or first-byte/grant side effects. Release
// names the original nonreusable reservation, never an amount guessed later.
type CapacityAccount interface {
	Reserve(Reservation) (ReservationID, error)
	Release(ReservationID) error
}

// Request reports whether the attempt escaped. Sent-unknown requires engine
// reconciliation before replay; OpenData owns a dedicated data attachment.
type Transport interface {
	Request(Request, Deadline) (Response, Attempt, error)
	OpenData(Identity, Deadline) (jetstream.JetStream, error)
	CloseData(Identity, Deadline) error
}

// Check distinguishes new admission, continued ownership and settlement so
// replacing a control session cannot become a data-plane cancellation source.
type Authority interface {
	Check(Identity, AccessKind, Deadline) error
}

// Resolve asks the original receiver for gate/primitive evidence. A deadline
// or lost origin never supplies a synthetic not-entered outcome.
type ReceiverResolver interface {
	Resolve(Identity, Deadline) (ReceiverOutcome, error)
}

// Census returns ready evidence at the requested original owner. Incomplete
// or unreachable owners protect resources instead of returning an empty set.
type OwnerResolver interface {
	Census(Origin, Deadline) (LedgerCensus, error)
}

// Only the finalizer can consume this proof and purge the exact original
// NUID. GC selects candidates; it cannot bypass writer/consumer retirement.
type ObjectRetirer interface {
	Retire(Identity, RetirementProof, Deadline) (CleanupReceipt, error)
}

// The caller supplies the already minted business identity. Issue is a
// mutation and must not be smuggled into a read-only probe or status request.
type PermitSigner interface {
	Issue(Intent, string, string, Deadline) (CreationPermit, error)
	Verify(CreationPermit) error
}

// Issuer verification binds the capsule and receiver challenge together.
// Scope/nonce authority is then checked against retained owner responsibility.
type IssuerVerifier interface {
	Verify(SettlementCapsule, NKeyProof) error
}

// Acquire serializes config ownership per bucket. Retire must join pending
// requests, including sent-unknown updates, before handing authority onward.
type ConfigAuthority interface {
	Acquire(string, Deadline) (ConfigWriterProof, error)
	Retire(ConfigWriterProof, Deadline) error
}

// History and cleanup arrive as revision messages, not callbacks that mutate
// tracker internals. Late acknowledgements cannot retire a replacement entry.
type DecisionPort interface {
	Read(Key, Deadline) (LedgerRow, error)
	Pending(Deadline) ([]LedgerRow, error)
	AcceptHistory(Key, string, uint64, HistoryReceipt, Deadline) error
	AcceptCleanup(Key, string, uint64, CleanupReceipt, Deadline) error
}

// Update is revision CAS; Retire additionally checks nonce and proof. Scan
// preserves unreadable/conflicting rows as incomplete evidence, not absence.
type LedgerPort interface {
	Update(Key, uint64, LedgerMutation, Deadline) (LedgerRow, error)
	Scan(Deadline) (LedgerCensus, error)
	Retire(Key, string, RetirementProof, Deadline) error
}

// Confirm proves the exact canonical bytes and physical sequence. Lookup
// reports the complete matching set, including duplicate/opposite terminals.
type HistoryPort interface {
	Confirm(CanonicalRecord, Deadline) (HistoryReceipt, error)
	Lookup(Key, Deadline) (HistorySet, error)
}

// Append admits exactly one logical pending chunk. ResolvePending reconciles
// its original subject/header/body before a later append may advance.
type ObjectWriter interface {
	Append(uint32, []byte, Deadline) (ChunkReceipt, error)
	ResolvePending(Deadline) (ChunkReceipt, bool, error)
	Finalize(Deadline) (ObjectReceipt, error)
	Freeze(Deadline) (WriterState, error)
	Stop(Deadline) error
}

// Fetch is separate from sink progress. ConfirmSynced is called only after a
// complete chunk has been written and its real Sync succeeded.
type ObjectReader interface {
	ReadChunk(Deadline) (Chunk, error)
	ConfirmSynced(SinkCheckpoint) error
	Freeze(Deadline) (ReaderState, error)
	Stop(Deadline) error
}
type FileSource = xferport.FileSource
type AtomicPort = xferport.AtomicPort[Deadline, handoff.Resource]

// TrackerPort deliberately omits image ownership; outer image wiring uses the
// larger TrackerManager. A finalizer can only submit revision-bound decisions.
type TrackerPort interface {
	Admit(CreationPermit, Intent, Deadline) (Status, error)
	SourceSeal(Identity, SourceSeal, Deadline) (Status, error)
	Reserve(Identity, ReserveRequest, Deadline) (CommitGrant, error)
	Finish(Identity, ReceiverOutcome, Deadline) (TerminalReceipt, error)
	Cancel(Identity, CancelRequest, Deadline) (Status, error)
	Status(StatusRequest, Deadline) (Status, error)
}
type FinalizerPort interface {
	Advance(Identity, Deadline) (Status, error)
	Stop(Deadline) error
}

// LegacyOrphanFinalizer is an additive P00 port: an unmapped cap0 object has
// no ledger Identity to pass to Advance. Only the finalizer may consume this
// immutable storage candidate, after fresh census and raw legacy validation.
// It never grants v1 writer/receiver retirement or manufactures an audit row.
// origin: hot-update S7 plan §5.3 / T29 / INTEG-R additive P00 amendment
type LegacyOrphanFinalizer interface {
	RetireLegacyOrphan(LegacyOrphanCandidate, Deadline) (LegacyOrphanReceipt, error)
}

type LegacyOrphanCandidate struct {
	Bucket, Stream, StreamCreated string
	Name, NUID                    string
	MetadataSequence              uint64
	MetadataDigest                string
}

type LegacyOrphanReceipt struct {
	Candidate     LegacyOrphanCandidate
	FenceSequence uint64
}

// Owners uses the same complete zero-origin aggregate as GC, including both
// durable directories and every possible original owner. Missing bindings
// refuse retirement; they do not enable an age-only fallback.
type LegacyOrphanConfig struct {
	JS     jetstream.JetStream
	Owners OwnerResolver
}

type BucketPort interface {
	Apply(ConfigPatch, Deadline) (BucketSeal, error)
	Freeze(Deadline) (ConfigWriterState, error)
	Stop(Deadline) error
}

// Seal binds size/hash/geometry to the original source before Step sends
// bytes. A changed source cannot publish successful object metadata.
type Sender interface {
	Seal(Deadline) (SourceSeal, error)
	Step(Deadline) error
	Freeze(Deadline) (SenderState, error)
	Stop(Deadline) error
}

// Reserve installs the original grant without entering the primitive. Step
// owns actual IO; Freeze exports only the fully synced sink prefix.
type Receiver interface {
	Reserve(CommitGrant) error
	Step(Deadline) error
	Freeze(Deadline) (ReceiverState, error)
	Stop(Deadline) error
}

// Acknowledge validates exact identity/kind/digest/history evidence. Stop is
// explicit lifecycle control and is never inferred from a byte timeout.
type Courier interface {
	Acknowledge(TerminalReceipt) error
	Step(Deadline) error
	Freeze(Deadline) (CourierState, error)
	Stop(Deadline) error
}

// Prepare/Apply live under one registry fence. Thaw replaces bindings only
// while frozen, preserving existing actors through ordinary NATS replacement.
type WorkerManager interface {
	Prepare(Prepare, Deadline) (Status, error)
	Start(Identity) error
	Apply(Control, Deadline) (Status, error)
	Status(Identity) (Status, error)
	Freeze(Deadline) (WorkerFrozen, error)
	Thaw(Bindings, Deadline) error
	Stop(Deadline) error
}

// Export borrows original business resources; pristine detach transfers the
// whole taken closure. Neither operation may silently close an original OFD.
type WorkerFrozen interface {
	Export() (WorkerState, []handoff.Resource, error)
	DetachPristine() (Salvage, error)
}

// Bind installs the current backend before imported callbacks can run.
// Freeze joins callbacks as well as timer and provisioning admission.
type TrackerManager interface {
	TrackerPort
	Freeze(Deadline) (TrackerFrozen, error)
	Bind(Bindings, Deadline) error
	Stop(Deadline) error
}

// Retained grants, canonical publications and cleanup travel together.
// Pristine salvage is unavailable once the imported owner becomes active.
type TrackerFrozen interface {
	Export() (TrackerState, []handoff.Resource, error)
	DetachPristine() (Salvage, error)
}

// Pass has one bounded deadline for both primary/outbox enumeration. Cancel
// must join those lists so a stalled pass cannot freeze future reconciliation.
type Collector interface {
	Pass(Deadline) (LedgerCensus, error)
	Stop(Deadline) error
}

// Handle validates a closed operation/payload union and authenticated actor.
// Original-owner routing and new-home creation are distinct authority paths.
type Controller interface {
	Handle(Request, Principal, Deadline) (Response, error)
	Stop(Deadline) error
}

// One invocation owns one business ID and object version. Its result is an
// exact terminal receipt; legacy best-effort results use another interface.
type Invocation interface {
	Run(Deadline) (TerminalReceipt, error)
	Cancel(CancelReason, Deadline) (Status, error)
	Stop(Deadline) error
}
type SettlementVerifier interface {
	Verify(SettlementCapsule, NKeyProof, Deadline) (SettlementGrant, error)
	Stop(Deadline) error
}

// A legacy invocation cannot be assigned to Invocation: its best-effort result
// has no history receipt, checkpoint or resume authority.
type LegacyInvocation interface {
	Run(Deadline) (LegacyResult, error)
	Stop(Deadline) error
}

type WriterFactory func(jetstream.JetStream, ObjectSpec) (ObjectWriter, error)
type ReaderFactory func(jetstream.JetStream, ObjectSpec, SinkCheckpoint) (ObjectReader, error)
type AtomicImportFactory = xferport.AtomicImportFactory[Deadline, handoff.Resource]
type SenderFactory func(SendConfig) (Sender, error)
type ReceiverFactory func(ReceiveConfig) (Receiver, error)
type CourierFactory func(CourierConfig) (Courier, error)
type WorkerFactory func(WorkerConfig) (WorkerManager, error)
type WorkerImportFactory func(WorkerConfig, WorkerState, FileSource) (WorkerManager, Salvage, error)
type TrackerFactory func(TrackerConfig) (TrackerManager, error)
type TrackerImportFactory func(TrackerConfig, TrackerState) (TrackerManager, error)
type FinalizerFactory func(FinalConfig) (FinalizerPort, error)
type LedgerFactory func(LedgerConfig) (LedgerPort, error)
type HistoryFactory func(HistoryConfig) (HistoryPort, error)
type CollectorFactory func(GCConfig) (Collector, error)
type BucketFactory func(BucketConfig) (BucketPort, error)
type ControlFactory func(ControlConfig) (Controller, error)
type ClientFactory func(ClientConfig) (Invocation, error)
type AuthFactory func(AuthConfig) (SettlementVerifier, error)
type LegacyFactory func(LegacyConfig) (LegacyInvocation, error)
type WorkerEncoder func(WorkerState) ([]byte, error)
type WorkerDecoder func([]byte) (WorkerState, error)
type TrackerEncoder func(TrackerState) ([]byte, error)
type TrackerDecoder func([]byte) (TrackerState, error)

// Base has no implicit clock, budget, logger or lifetime defaults. Embedding it
// supplies the exact same accounting and observation ports to every leaf.
type Base struct {
	Clock    Clock
	UTC      func() time.Time
	Capacity CapacityAccount
	Observe  func(Observation)
}
type WorkerConfig struct {
	Base
	Identity     Identity
	Authority    Authority
	Transport    Transport
	MakeSender   SenderFactory
	MakeReceiver ReceiverFactory
	MakeCourier  CourierFactory
}
type SendConfig struct {
	Base
	Identity  Identity
	Intent    Intent
	Budget    BudgetSeal
	Source    *os.File
	Writer    ObjectWriter
	Transport Transport
}
type ReceiveConfig struct {
	Base
	Identity  Identity
	Budget    BudgetSeal
	Reader    ObjectReader
	Atomic    AtomicPort
	Transport Transport
}
type CourierConfig struct {
	Base
	Identity  Identity
	Outcome   ReceiverOutcome
	Capsule   SettlementCapsule
	Transport Transport
}
type TrackerConfig struct {
	Base
	Origin          Origin
	Ledger          LedgerPort
	History         HistoryPort
	ResolveReceiver ReceiverResolver
	Finalizer       FinalizerPort
}
type FinalConfig struct {
	Base
	LegacyOrphans   *LegacyOrphanConfig
	Decisions       DecisionPort
	Ledger          LedgerPort
	History         HistoryPort
	Objects         ObjectRetirer
	ResolveReceiver ReceiverResolver
}
type LedgerConfig struct {
	Base
	Root         string
	FallbackRoot string
}
type HistoryConfig struct {
	Base
	JS  jetstream.JetStream
	SID string
}
type GCConfig struct {
	Base
	JS        jetstream.JetStream
	Owners    OwnerResolver
	Finalizer FinalizerPort
}
type BucketConfig struct {
	Base
	JS        jetstream.JetStream
	Authority ConfigAuthority
	Limits    StorageLimits
}
type ControlConfig struct {
	Base
	Tracker   TrackerPort
	Authority Authority
	Permits   PermitSigner
	Buckets   BucketPort
	Transport Transport
}
type ClientConfig struct {
	Base
	Identity    Identity
	Intent      Intent
	Source      *os.File
	Destination AtomicPort
	Transport   Transport
	MakeWriter  WriterFactory
	MakeReader  ReaderFactory
	MakeCourier CourierFactory
}
type AuthConfig struct {
	Base
	Issuers IssuerVerifier
	Owners  OwnerResolver
}
type LegacyConfig struct {
	Base
	Intent     Intent
	TransferID string
	PutOnce    func(Deadline) (LegacyResult, error)
}

// Bindings may change only while frozen. Import returns sleeping owners; a
// current backend must be installed before callbacks or byte work resume.
type Bindings struct {
	JS              jetstream.JetStream
	Transport       Transport
	Authority       Authority
	Ledger          LedgerPort
	OwnerGeneration uint64
}
