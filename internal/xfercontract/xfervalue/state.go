package xfervalue

import "github.com/LinZiyang666/tether/internal/xfercontract/xferport"

// These declarations moved verbatim from xfercontract/state.go: the filesystem
// leaf (transferpath) records custody observations and must not reach NATS.
// xfercontract/state.go aliases each one in place, and contractstub rebuilds
// the frozen P00 state.go digest from that file plus these declarations.
// origin: hot-update S7 plan §8.1 / §10.4 / §11.1 / P00 — NATS-free value split

type ResourceRef = xferport.ResourceRef
type FileIdentity = xferport.FileIdentity

type BootDeadline struct {
	BootID   string
	Deadline Deadline
}

type AtomicState = xferport.AtomicState

// Observation has typed, bounded fields instead of an arbitrary payload map.
// It carries digests and resource references; no path, capsule or file bytes.
// P28 owns the custody encoder/poison/drain protocol, not these product values.
// origin: hot-update S7 plan §10.4 / P28
// RetryFamily intentionally remains cli.RetryFamily (P26); a second enum here
// would split the shared helper's sent-unknown classification.
type ObservationKind string

const (
	ObserveAppend         ObservationKind = "append"
	ObserveResolve        ObservationKind = "resolve"
	ObserveFinalize       ObservationKind = "finalize"
	ObserveFetched        ObservationKind = "fetched"
	ObserveSynced         ObservationKind = "synced"
	ObserveReaderStop     ObservationKind = "reader_stop"
	ObservePrimitiveEnter ObservationKind = "primitive_enter"
	ObservePrimitiveExit  ObservationKind = "primitive_exit"
	ObserveDirectorySync  ObservationKind = "directory_sync"
	ObserveFreeze         ObservationKind = "freeze"
	ObserveImport         ObservationKind = "import"
	ObserveControl        ObservationKind = "control"
	ObserveAcknowledge    ObservationKind = "acknowledge"
	ObserveReserve        ObservationKind = "reserve"
	ObserveFinish         ObservationKind = "finish"
	ObserveCallbackEnter  ObservationKind = "callback_enter"
	ObserveCallbackJoin   ObservationKind = "callback_join"
	ObserveLedgerSync     ObservationKind = "ledger_sync"
	ObserveHistory        ObservationKind = "history"
	ObserveRetirement     ObservationKind = "retirement"
	ObserveInstall        ObservationKind = "install"
	ObserveRollback       ObservationKind = "rollback"
	ObserveAttempt        ObservationKind = "attempt"
	ObserveDrain          ObservationKind = "drain"
)

type Observation struct {
	Kind                                                                 ObservationKind
	ProcessStart                                                         uint64
	BootID                                                               string
	PID                                                                  int
	Generation                                                           uint64
	CutID                                                                string
	TransferID, EntryNonce, ObjectVersion, Leg                           string
	Phase                                                                Phase
	EventSeq                                                             uint64
	BoottimeNS                                                           int64
	Index                                                                uint32
	Offset                                                               int64
	Length                                                               uint32
	SHA256                                                               string
	ExpectedSubjectSeq, ExpectedStreamSeq, ActualACKSeq                  uint64
	AttachmentGeneration                                                 uint64
	FetchedOffset, DurableOffset                                         int64
	LastConsumedStreamSeq                                                uint64
	HashStateDigest                                                      string
	ConsumerIdentity, ConnectionIdentity, ServerIdentity, StreamIdentity string
	Dropped                                                              uint64
	PONGConfirmed, ConsumerDeleted                                       bool
	Primitive                                                            CommitPrimitive
	Outcome                                                              PrimitiveOutcome
	Errno                                                                int32
	ParentSynced                                                         bool
	Tmp, Parent, Source                                                  ResourceRef
	OFDWitnessDigest                                                     string
	AdmissionClosed                                                      bool
	GrantNonce, CanonicalDigest, StopNonce                               string
	OriginalDeadline                                                     BootDeadline
	Revision, PrimaryRevision, OutboxRevision                            uint64
	FileSynced, DirectorySynced                                          bool
	RaftIndex, HistorySeq, PublisherCheckpoint                           uint64
	RetirementDigest, ReaderSHA                                          string
	Active, InstalledBeforeReaper                                        bool
	Attempt                                                              Attempt
	NativeOutcome                                                        int
}
