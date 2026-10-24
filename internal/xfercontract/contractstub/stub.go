// Package contractstub supplies inert compile witnesses for independent S7 leaves.
// Never import it from production: neither construction nor any method proves
// admission, persistence, cleanup or successful retirement. Stubs take no files,
// invoke no callbacks, open no descriptors and start no goroutines.
// origin: hot-update S7 plan §8.1 / P00
package contractstub

import (
	"fmt"
	"io"
	"os"

	"github.com/LinZiyang666/tether/internal/handoff"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

var (
	errJsstreamP02     = fmt.Errorf("jsstream: %w (S7 P02)", x.ErrContractStub)
	errJsstreamP03     = fmt.Errorf("jsstream: %w (S7 P03)", x.ErrContractStub)
	errTransferpathP05 = fmt.Errorf("transferpath: %w (S7 P05)", x.ErrContractStub)
	errWorkerP06       = fmt.Errorf("xferworker: %w (S7 P06)", x.ErrContractStub)
	errSendP08         = fmt.Errorf("xfersend: %w (S7 P08)", x.ErrContractStub)
	errReceiveP07      = fmt.Errorf("xferreceive: %w (S7 P07)", x.ErrContractStub)
	errCourierP09      = fmt.Errorf("xfercourier: %w (S7 P09)", x.ErrContractStub)
	errTrackP11        = fmt.Errorf("xfertrack: %w (S7 P11)", x.ErrContractStub)
	errLedgerP13       = fmt.Errorf("xferledger: %w (S7 P13)", x.ErrContractStub)
	errHistoryP14      = fmt.Errorf("xferhistory: %w (S7 P14)", x.ErrContractStub)
	errFinalP12        = fmt.Errorf("xferfinal: %w (S7 P12)", x.ErrContractStub)
	errGcP17           = fmt.Errorf("xfergc: %w (S7 P17)", x.ErrContractStub)
	errBucketP04       = fmt.Errorf("xferbucket: %w (S7 P04)", x.ErrContractStub)
	errControlP16      = fmt.Errorf("xfercontrol: %w (S7 P16)", x.ErrContractStub)
	errClientP25       = fmt.Errorf("xferclient: %w (S7 P25)", x.ErrContractStub)
	errAuthP19         = fmt.Errorf("xferauth: %w (S7 P19)", x.ErrContractStub)
	errCliP26          = fmt.Errorf("cli: %w (S7 P26)", x.ErrContractStub)
	errWorkerCodec     = fmt.Errorf("xferimage: %w (S7 P10)", x.ErrContractStub)
	errTrackerCodec    = fmt.Errorf("xferimage: %w (S7 P18)", x.ErrContractStub)
)

// Writer is the inert P02 (jsstream) interface witness.
type Writer struct{}

var _ x.ObjectWriter = Writer{}

func (Writer) Append(uint32, []byte, x.Deadline) (x.ChunkReceipt, error) {
	return x.ChunkReceipt{}, errJsstreamP02
}
func (Writer) ResolvePending(x.Deadline) (x.ChunkReceipt, bool, error) {
	return x.ChunkReceipt{}, false, errJsstreamP02
}
func (Writer) Finalize(x.Deadline) (x.ObjectReceipt, error) {
	return x.ObjectReceipt{}, errJsstreamP02
}
func (Writer) Freeze(x.Deadline) (x.WriterState, error) {
	return x.WriterState{}, errJsstreamP02
}
func (Writer) Stop(x.Deadline) error {
	return errJsstreamP02
}

// Reader is the inert P03 (jsstream) interface witness.
type Reader struct{}

var _ x.ObjectReader = Reader{}

func (Reader) ReadChunk(x.Deadline) (x.Chunk, error) {
	return x.Chunk{}, errJsstreamP03
}
func (Reader) ConfirmSynced(x.SinkCheckpoint) error {
	return errJsstreamP03
}
func (Reader) Freeze(x.Deadline) (x.ReaderState, error) {
	return x.ReaderState{}, errJsstreamP03
}
func (Reader) Stop(x.Deadline) error {
	return errJsstreamP03
}

// Atomic is the inert P05 (transferpath) interface witness.
type Atomic struct{}

var _ x.AtomicPort = Atomic{}

func (Atomic) WriteAt([]byte, int64) (int, error) {
	return 0, errTransferpathP05
}
func (Atomic) Sync() error {
	return errTransferpathP05
}
func (Atomic) Freeze(x.Deadline) (x.AtomicState, []handoff.Resource, error) {
	return x.AtomicState{}, nil, errTransferpathP05
}
func (Atomic) Stop(x.Deadline) error {
	return errTransferpathP05
}

// Commit has no error result in the frozen API. Leave primitive/outcome
// unresolved and carry the stub error; never fabricate a no-syscall receipt.
func (Atomic) Commit(x.CommitGrant, x.Deadline) x.ReceiverOutcome {
	return x.ReceiverOutcome{Code: x.CodeResultUnknown, Error: errTransferpathP05.Error()}
}

// Source is the inert P05 (transferpath) interface witness.
type Source struct{}

var _ x.FileSource = Source{}

func (Source) TakeFile(string, string) (*os.File, error) {
	return nil, errTransferpathP05
}

// Worker is the inert P06 (xferworker) interface witness.
type Worker struct{}

var _ x.WorkerManager = Worker{}

func (Worker) Prepare(x.Prepare, x.Deadline) (x.Status, error) {
	return x.Status{}, errWorkerP06
}
func (Worker) Start(x.Identity) error {
	return errWorkerP06
}
func (Worker) Apply(x.Control, x.Deadline) (x.Status, error) {
	return x.Status{}, errWorkerP06
}
func (Worker) Status(x.Identity) (x.Status, error) {
	return x.Status{}, errWorkerP06
}
func (Worker) Freeze(x.Deadline) (x.WorkerFrozen, error) {
	return nil, errWorkerP06
}
func (Worker) Thaw(x.Bindings, x.Deadline) error {
	return errWorkerP06
}
func (Worker) Stop(x.Deadline) error {
	return errWorkerP06
}

// FrozenWorker is the inert P06 (xferworker) interface witness.
type FrozenWorker struct{}

var _ x.WorkerFrozen = FrozenWorker{}

func (FrozenWorker) Export() (x.WorkerState, []handoff.Resource, error) {
	return x.WorkerState{}, nil, errWorkerP06
}
func (FrozenWorker) DetachPristine() (x.Salvage, error) {
	return x.Salvage{}, errWorkerP06
}

// Sender is the inert P08 (xfersend) interface witness.
type Sender struct{}

var _ x.Sender = Sender{}

func (Sender) Seal(x.Deadline) (x.SourceSeal, error) {
	return x.SourceSeal{}, errSendP08
}
func (Sender) Step(x.Deadline) error {
	return errSendP08
}
func (Sender) Freeze(x.Deadline) (x.SenderState, error) {
	return x.SenderState{}, errSendP08
}
func (Sender) Stop(x.Deadline) error {
	return errSendP08
}

// Receiver is the inert P07 (xferreceive) interface witness.
type Receiver struct{}

var _ x.Receiver = Receiver{}

func (Receiver) Reserve(x.CommitGrant) error {
	return errReceiveP07
}
func (Receiver) Step(x.Deadline) error {
	return errReceiveP07
}
func (Receiver) Freeze(x.Deadline) (x.ReceiverState, error) {
	return x.ReceiverState{}, errReceiveP07
}
func (Receiver) Stop(x.Deadline) error {
	return errReceiveP07
}

// Courier is the inert P09 (xfercourier) interface witness.
type Courier struct{}

var _ x.Courier = Courier{}

func (Courier) Acknowledge(x.TerminalReceipt) error {
	return errCourierP09
}
func (Courier) Step(x.Deadline) error {
	return errCourierP09
}
func (Courier) Freeze(x.Deadline) (x.CourierState, error) {
	return x.CourierState{}, errCourierP09
}
func (Courier) Stop(x.Deadline) error {
	return errCourierP09
}

// Tracker is the inert P11 (xfertrack) interface witness.
type Tracker struct{}

var _ x.TrackerManager = Tracker{}

func (Tracker) Admit(x.CreationPermit, x.Intent, x.Deadline) (x.Status, error) {
	return x.Status{}, errTrackP11
}
func (Tracker) SourceSeal(x.Identity, x.SourceSeal, x.Deadline) (x.Status, error) {
	return x.Status{}, errTrackP11
}
func (Tracker) Reserve(x.Identity, x.ReserveRequest, x.Deadline) (x.CommitGrant, error) {
	return x.CommitGrant{}, errTrackP11
}
func (Tracker) Finish(x.Identity, x.ReceiverOutcome, x.Deadline) (x.TerminalReceipt, error) {
	return x.TerminalReceipt{}, errTrackP11
}
func (Tracker) Cancel(x.Identity, x.CancelRequest, x.Deadline) (x.Status, error) {
	return x.Status{}, errTrackP11
}
func (Tracker) Status(x.StatusRequest, x.Deadline) (x.Status, error) {
	return x.Status{}, errTrackP11
}
func (Tracker) Freeze(x.Deadline) (x.TrackerFrozen, error) {
	return nil, errTrackP11
}
func (Tracker) Bind(x.Bindings, x.Deadline) error {
	return errTrackP11
}
func (Tracker) Stop(x.Deadline) error {
	return errTrackP11
}

// FrozenTracker is the inert P11 (xfertrack) interface witness.
type FrozenTracker struct{}

var _ x.TrackerFrozen = FrozenTracker{}

func (FrozenTracker) Export() (x.TrackerState, []handoff.Resource, error) {
	return x.TrackerState{}, nil, errTrackP11
}
func (FrozenTracker) DetachPristine() (x.Salvage, error) {
	return x.Salvage{}, errTrackP11
}

// Ledger is the inert P13 (xferledger) interface witness.
type Ledger struct{}

var _ x.LedgerPort = Ledger{}

func (Ledger) Update(x.Key, uint64, x.LedgerMutation, x.Deadline) (x.LedgerRow, error) {
	return x.LedgerRow{}, errLedgerP13
}
func (Ledger) Scan(x.Deadline) (x.LedgerCensus, error) {
	return x.LedgerCensus{}, errLedgerP13
}
func (Ledger) Retire(x.Key, string, x.RetirementProof, x.Deadline) error {
	return errLedgerP13
}

// History is the inert P14 (xferhistory) interface witness.
type History struct{}

var _ x.HistoryPort = History{}

func (History) Confirm(x.CanonicalRecord, x.Deadline) (x.HistoryReceipt, error) {
	return x.HistoryReceipt{}, errHistoryP14
}
func (History) Lookup(x.Key, x.Deadline) (x.HistorySet, error) {
	return x.HistorySet{}, errHistoryP14
}

// Finalizer is the inert P12 (xferfinal) interface witness.
type Finalizer struct{}

var _ x.FinalizerPort = Finalizer{}

func (Finalizer) Advance(x.Identity, x.Deadline) (x.Status, error) {
	return x.Status{}, errFinalP12
}
func (Finalizer) Stop(x.Deadline) error {
	return errFinalP12
}

// Collector is the inert P17 (xfergc) interface witness.
type Collector struct{}

var _ x.Collector = Collector{}

func (Collector) Pass(x.Deadline) (x.LedgerCensus, error) {
	return x.LedgerCensus{}, errGcP17
}
func (Collector) Stop(x.Deadline) error {
	return errGcP17
}

// Bucket is the inert P04 (xferbucket) interface witness.
type Bucket struct{}

var _ x.BucketPort = Bucket{}

func (Bucket) Apply(x.ConfigPatch, x.Deadline) (x.BucketSeal, error) {
	return x.BucketSeal{}, errBucketP04
}
func (Bucket) Freeze(x.Deadline) (x.ConfigWriterState, error) {
	return x.ConfigWriterState{}, errBucketP04
}
func (Bucket) Stop(x.Deadline) error {
	return errBucketP04
}

// Controller is the inert P16 (xfercontrol) interface witness.
type Controller struct{}

var _ x.Controller = Controller{}

func (Controller) Handle(x.Request, x.Principal, x.Deadline) (x.Response, error) {
	return x.Response{}, errControlP16
}
func (Controller) Stop(x.Deadline) error {
	return errControlP16
}

// Client is the inert P25 (xferclient) interface witness.
type Client struct{}

var _ x.Invocation = Client{}

func (Client) Run(x.Deadline) (x.TerminalReceipt, error) {
	return x.TerminalReceipt{}, errClientP25
}
func (Client) Cancel(x.CancelReason, x.Deadline) (x.Status, error) {
	return x.Status{}, errClientP25
}
func (Client) Stop(x.Deadline) error {
	return errClientP25
}

// Legacy is the inert P25 (xferclient) interface witness.
type Legacy struct{}

var _ x.LegacyInvocation = Legacy{}

func (Legacy) Run(x.Deadline) (x.LegacyResult, error) {
	return x.LegacyResult{}, errClientP25
}
func (Legacy) Stop(x.Deadline) error {
	return errClientP25
}

// Auth is the inert P19 (xferauth) interface witness.
type Auth struct{}

var _ x.SettlementVerifier = Auth{}

func (Auth) Verify(x.SettlementCapsule, x.NKeyProof, x.Deadline) (x.SettlementGrant, error) {
	return x.SettlementGrant{}, errAuthP19
}
func (Auth) Stop(x.Deadline) error {
	return errAuthP19
}

// Capacity is the inert P06 (xferworker) interface witness.
type Capacity struct{}

var _ x.CapacityAccount = Capacity{}

func (Capacity) Reserve(x.Reservation) (x.ReservationID, error) {
	return "", errWorkerP06
}
func (Capacity) Release(x.ReservationID) error {
	return errWorkerP06
}

// Transport is the inert P26 (cli) interface witness.
type Transport struct{}

var _ x.Transport = Transport{}

func (Transport) Request(x.Request, x.Deadline) (x.Response, x.Attempt, error) {
	return x.Response{}, x.Attempt{}, errCliP26
}
func (Transport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errCliP26
}
func (Transport) CloseData(x.Identity, x.Deadline) error {
	return errCliP26
}

// Authority is the inert P16 (xfercontrol) interface witness.
type Authority struct{}

var _ x.Authority = Authority{}

func (Authority) Check(x.Identity, x.AccessKind, x.Deadline) error {
	return errControlP16
}

// ReceiverResolver is the inert P16 (xfercontrol) interface witness.
type ReceiverResolver struct{}

var _ x.ReceiverResolver = ReceiverResolver{}

func (ReceiverResolver) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	return x.ReceiverOutcome{}, errControlP16
}

// OwnerResolver is the inert P17 (xfergc) interface witness.
type OwnerResolver struct{}

var _ x.OwnerResolver = OwnerResolver{}

func (OwnerResolver) Census(x.Origin, x.Deadline) (x.LedgerCensus, error) {
	return x.LedgerCensus{}, errGcP17
}

// ObjectRetirer is the inert P12 (xferfinal) interface witness.
type ObjectRetirer struct{}

var _ x.ObjectRetirer = ObjectRetirer{}

func (ObjectRetirer) Retire(x.Identity, x.RetirementProof, x.Deadline) (x.CleanupReceipt, error) {
	return x.CleanupReceipt{}, errFinalP12
}

// PermitSigner is the inert P16 (xfercontrol) interface witness.
type PermitSigner struct{}

var _ x.PermitSigner = PermitSigner{}

func (PermitSigner) Issue(x.Intent, string, string, x.Deadline) (x.CreationPermit, error) {
	return x.CreationPermit{}, errControlP16
}
func (PermitSigner) Verify(x.CreationPermit) error {
	return errControlP16
}

// IssuerVerifier is the inert P19 (xferauth) interface witness.
type IssuerVerifier struct{}

var _ x.IssuerVerifier = IssuerVerifier{}

func (IssuerVerifier) Verify(x.SettlementCapsule, x.NKeyProof) error {
	return errAuthP19
}

// ConfigAuthority is the inert P04 (xferbucket) interface witness.
type ConfigAuthority struct{}

var _ x.ConfigAuthority = ConfigAuthority{}

func (ConfigAuthority) Acquire(string, x.Deadline) (x.ConfigWriterProof, error) {
	return x.ConfigWriterProof{}, errBucketP04
}
func (ConfigAuthority) Retire(x.ConfigWriterProof, x.Deadline) error {
	return errBucketP04
}

// Decisions is the inert P11 (xfertrack) interface witness.
type Decisions struct{}

var _ x.DecisionPort = Decisions{}

func (Decisions) Read(x.Key, x.Deadline) (x.LedgerRow, error) {
	return x.LedgerRow{}, errTrackP11
}
func (Decisions) Pending(x.Deadline) ([]x.LedgerRow, error) {
	return nil, errTrackP11
}
func (Decisions) AcceptHistory(x.Key, string, uint64, x.HistoryReceipt, x.Deadline) error {
	return errTrackP11
}
func (Decisions) AcceptCleanup(x.Key, string, uint64, x.CleanupReceipt, x.Deadline) error {
	return errTrackP11
}

// Constructors return no owner on failure. Tests can use the zero-size witness
// types above to compile consumers without pretending construction succeeded.
func NewTransferWriter(jetstream.JetStream, x.ObjectSpec) (x.ObjectWriter, error) {
	return nil, errJsstreamP02
}
func NewTransferReader(jetstream.JetStream, x.ObjectSpec, x.SinkCheckpoint) (x.ObjectReader, error) {
	return nil, errJsstreamP03
}
func ImportAtomic(x.AtomicState, x.FileSource) (x.AtomicPort, x.Salvage, error) {
	return nil, x.Salvage{}, errTransferpathP05
}
func NewWorker(x.WorkerConfig) (x.WorkerManager, error) {
	return nil, errWorkerP06
}
func ImportWorker(x.WorkerConfig, x.WorkerState, x.FileSource) (x.WorkerManager, x.Salvage, error) {
	return nil, x.Salvage{}, errWorkerP06
}
func NewSender(x.SendConfig) (x.Sender, error) {
	return nil, errSendP08
}
func NewReceiver(x.ReceiveConfig) (x.Receiver, error) {
	return nil, errReceiveP07
}
func NewCourier(x.CourierConfig) (x.Courier, error) {
	return nil, errCourierP09
}
func NewTracker(x.TrackerConfig) (x.TrackerManager, error) {
	return nil, errTrackP11
}
func ImportTracker(x.TrackerConfig, x.TrackerState) (x.TrackerManager, error) {
	return nil, errTrackP11
}
func NewLedger(x.LedgerConfig) (x.LedgerPort, error) {
	return nil, errLedgerP13
}
func NewHistory(x.HistoryConfig) (x.HistoryPort, error) {
	return nil, errHistoryP14
}
func NewFinalizer(x.FinalConfig) (x.FinalizerPort, error) {
	return nil, errFinalP12
}
func NewCollector(x.GCConfig) (x.Collector, error) {
	return nil, errGcP17
}
func NewBucket(x.BucketConfig) (x.BucketPort, error) {
	return nil, errBucketP04
}
func NewController(x.ControlConfig) (x.Controller, error) {
	return nil, errControlP16
}
func NewClient(x.ClientConfig) (x.Invocation, error) {
	return nil, errClientP25
}
func NewLegacy(x.LegacyConfig) (x.LegacyInvocation, error) {
	return nil, errClientP25
}
func NewAuth(x.AuthConfig) (x.SettlementVerifier, error) {
	return nil, errAuthP19
}
func PutLegacyOnce(jetstream.JetStream, string, string, io.Reader, x.Deadline) (*jetstream.ObjectInfo, error) {
	return nil, errJsstreamP02
}
func EncodeWorker(x.WorkerState) ([]byte, error) {
	return nil, errWorkerCodec
}
func DecodeWorker([]byte) (x.WorkerState, error) {
	return x.WorkerState{}, errWorkerCodec
}
func EncodeTracker(x.TrackerState) ([]byte, error) {
	return nil, errTrackerCodec
}
func DecodeTracker([]byte) (x.TrackerState, error) {
	return x.TrackerState{}, errTrackerCodec
}
