// Package xfercontract owns the neutral transfer-continuation vocabulary.
// It carries values and ports only: a declaration cannot authorize IO or ACK.
// origin: hot-update S7 plan §6.2 / §8.1 / P00
//
// The values are declared in the NATS-free xfervalue package and re-exported
// here by alias, one for one, so every identity, field, tag and enum value is
// unchanged. ports.go legitimately names jetstream.JetStream, and Go imports a
// whole package: before the split every importer of this package — proto, and
// through proto cluster/state, tunnel/transport, agentstate, ptysession — reached
// the NATS client, breaking layering L-2. Transport-free code imports xfervalue;
// port owners keep importing this package. contractstub pins the alias census,
// and proves xfervalue/contracts.go is this file's frozen P00 body verbatim.
// origin: hot-update S7 plan §6.2 / §11.1 / P00 — NATS-free value split
package xfercontract

import "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"

// ErrContractStub is wrapped only by contractstub. Production must never import
// that package; errors.Is remains usable without coupling callers to stub text.
var ErrContractStub = xfervalue.ErrContractStub

type Deadline = xfervalue.Deadline
type Attempt = xfervalue.Attempt
type Clock = xfervalue.Clock

const (
	Version        = xfervalue.Version
	StateSchema    = xfervalue.StateSchema
	ResourceState  = xfervalue.ResourceState
	ResourceSource = xfervalue.ResourceSource
	ResourceTmp    = xfervalue.ResourceTmp
	ResourceParent = xfervalue.ResourceParent
	StateFileName  = xfervalue.StateFileName
	SourceFileName = xfervalue.SourceFileName
	TmpFileName    = xfervalue.TmpFileName
	ParentFileName = xfervalue.ParentFileName
)

const (
	CodeIntentConflict        = xfervalue.CodeIntentConflict
	CodePermitExpired         = xfervalue.CodePermitExpired
	CodeOwnerRecovering       = xfervalue.CodeOwnerRecovering
	CodeResultUnknown         = xfervalue.CodeResultUnknown
	CodeObjectConflict        = xfervalue.CodeObjectConflict
	CodeObjectVersionMismatch = xfervalue.CodeObjectVersionMismatch
	CodeSourceChanged         = xfervalue.CodeSourceChanged
	CodeStorageHorizon        = xfervalue.CodeStorageHorizon
	CodePayloadUnsupported    = xfervalue.CodePayloadUnsupported
	CodeCapacityExceeded      = xfervalue.CodeCapacityExceeded
	CodeTerminalConflict      = xfervalue.CodeTerminalConflict
	CodeCommitInflight        = xfervalue.CodeCommitInflight
	CodeDurabilityUnknown     = xfervalue.CodeDurabilityUnknown
	CodeReconcileRequired     = xfervalue.CodeReconcileRequired
	CodeBudgetExceeded        = xfervalue.CodeBudgetExceeded
	CodeCanceled              = xfervalue.CodeCanceled
	CodeSessionDeleting       = xfervalue.CodeSessionDeleting
	CodeNotMember             = xfervalue.CodeNotMember
	CodeNotOwner              = xfervalue.CodeNotOwner
	CodeUnknown               = xfervalue.CodeUnknown
	ReasonSettlementAbandoned = xfervalue.ReasonSettlementAbandoned
)

// Enum re-exports name their (alias) type: the compiler then refuses a member
// re-exported under the wrong family, and source scanners that key closed
// families as xfercontract.<Type> still find every member here.
// origin: hot-update S7 plan §11.2 / T52 / P00 — NATS-free value split
type StatusState = xfervalue.StatusState

const (
	StatusUnknown  StatusState = xfervalue.StatusUnknown
	StatusLive     StatusState = xfervalue.StatusLive
	StatusTerminal StatusState = xfervalue.StatusTerminal
)

type Phase = xfervalue.Phase
type CommitPrimitive = xfervalue.CommitPrimitive
type PrimitiveOutcome = xfervalue.PrimitiveOutcome

const (
	PhaseOpen             Phase            = xfervalue.PhaseOpen
	PhasePrepared         Phase            = xfervalue.PhasePrepared
	PhaseSending          Phase            = xfervalue.PhaseSending
	PhaseReceiving        Phase            = xfervalue.PhaseReceiving
	PhaseCommitReserved   Phase            = xfervalue.PhaseCommitReserved
	PhaseDecided          Phase            = xfervalue.PhaseDecided
	PhaseTerminalPending  Phase            = xfervalue.PhaseTerminalPending
	PhaseHistoryConfirmed Phase            = xfervalue.PhaseHistoryConfirmed
	PhaseCleanupPending   Phase            = xfervalue.PhaseCleanupPending
	PhaseSettled          Phase            = xfervalue.PhaseSettled
	PrimitiveRename       CommitPrimitive  = xfervalue.PrimitiveRename
	PrimitiveLink         CommitPrimitive  = xfervalue.PrimitiveLink
	OutcomeNotEntered     PrimitiveOutcome = xfervalue.OutcomeNotEntered
	OutcomeInflight       PrimitiveOutcome = xfervalue.OutcomeInflight
	OutcomeLanded         PrimitiveOutcome = xfervalue.OutcomeLanded
)

type CancelReason = xfervalue.CancelReason

const (
	CancelUser     CancelReason = xfervalue.CancelUser
	CancelDeadline CancelReason = xfervalue.CancelDeadline
	CancelRevoked  CancelReason = xfervalue.CancelRevoked
	CancelStopped  CancelReason = xfervalue.CancelStopped
)

type AuditDelivery = xfervalue.AuditDelivery

const (
	DeliveryPending          AuditDelivery = xfervalue.DeliveryPending
	DeliveryRaftCommitted    AuditDelivery = xfervalue.DeliveryRaftCommitted
	DeliveryHistoryConfirmed AuditDelivery = xfervalue.DeliveryHistoryConfirmed
)

type TerminalDisposition = xfervalue.TerminalDisposition

const (
	DispositionPending  TerminalDisposition = xfervalue.DispositionPending
	DispositionComplete TerminalDisposition = xfervalue.DispositionComplete
	DispositionFailed   TerminalDisposition = xfervalue.DispositionFailed
	DispositionConflict TerminalDisposition = xfervalue.DispositionConflict
)

type ObjectWritePhase = xfervalue.ObjectWritePhase

const (
	WriteOpen            ObjectWritePhase = xfervalue.WriteOpen
	WritePending         ObjectWritePhase = xfervalue.WritePending
	WriteMetadataPending ObjectWritePhase = xfervalue.WriteMetadataPending
	WriteFinalized       ObjectWritePhase = xfervalue.WriteFinalized
	WriteRetiring        ObjectWritePhase = xfervalue.WriteRetiring
	WriteRetired         ObjectWritePhase = xfervalue.WriteRetired
)

type ReaderPhase = xfervalue.ReaderPhase

const (
	ReaderOpen          ReaderPhase = xfervalue.ReaderOpen
	ReaderFetching      ReaderPhase = xfervalue.ReaderFetching
	ReaderDirty         ReaderPhase = xfervalue.ReaderDirty
	ReaderDeletePending ReaderPhase = xfervalue.ReaderDeletePending
	ReaderStopped       ReaderPhase = xfervalue.ReaderStopped
)

type LedgerMutationKind = xfervalue.LedgerMutationKind

const (
	MutationAdmit      LedgerMutationKind = xfervalue.MutationAdmit
	MutationSourceSeal LedgerMutationKind = xfervalue.MutationSourceSeal
	MutationReserve    LedgerMutationKind = xfervalue.MutationReserve
	MutationDecide     LedgerMutationKind = xfervalue.MutationDecide
	MutationHistory    LedgerMutationKind = xfervalue.MutationHistory
	MutationCleanup    LedgerMutationKind = xfervalue.MutationCleanup
	MutationAbandon    LedgerMutationKind = xfervalue.MutationAbandon
)

type AccessKind = xfervalue.AccessKind

const (
	AccessCreate   AccessKind = xfervalue.AccessCreate
	AccessContinue AccessKind = xfervalue.AccessContinue
	AccessStatus   AccessKind = xfervalue.AccessStatus
	AccessSettle   AccessKind = xfervalue.AccessSettle
)

type Operation = xfervalue.Operation

const (
	OpProbe        Operation = xfervalue.OpProbe
	OpPermit       Operation = xfervalue.OpPermit
	OpPrepare      Operation = xfervalue.OpPrepare
	OpStatus       Operation = xfervalue.OpStatus
	OpSource       Operation = xfervalue.OpSource
	OpReserve      Operation = xfervalue.OpReserve
	OpCommit       Operation = xfervalue.OpCommit
	OpTerminal     Operation = xfervalue.OpTerminal
	OpCancel       Operation = xfervalue.OpCancel
	OpFinalize     Operation = xfervalue.OpFinalize
	OpCensus       Operation = xfervalue.OpCensus
	OpBucketConfig Operation = xfervalue.OpBucketConfig
)

type ControlKind = xfervalue.ControlKind

const (
	ControlStart   ControlKind = xfervalue.ControlStart
	ControlGrant   ControlKind = xfervalue.ControlGrant
	ControlCancel  ControlKind = xfervalue.ControlCancel
	ControlRevoke  ControlKind = xfervalue.ControlRevoke
	ControlReceipt ControlKind = xfervalue.ControlReceipt
)

type PrincipalRole = xfervalue.PrincipalRole

const (
	RoleCTL        PrincipalRole = xfervalue.RoleCTL
	RoleAgent      PrincipalRole = xfervalue.RoleAgent
	RoleBroker     PrincipalRole = xfervalue.RoleBroker
	RoleSettlement PrincipalRole = xfervalue.RoleSettlement
)

type Identity = xfervalue.Identity
type Key = xfervalue.Key
type Origin = xfervalue.Origin
type Intent = xfervalue.Intent
type CreationPermit = xfervalue.CreationPermit
type BudgetSeal = xfervalue.BudgetSeal
type SourceSeal = xfervalue.SourceSeal
type CommitGrant = xfervalue.CommitGrant
type ReceiverOutcome = xfervalue.ReceiverOutcome
type TerminalReceipt = xfervalue.TerminalReceipt
type SettlementCapsule = xfervalue.SettlementCapsule
type ImageRef = xfervalue.ImageRef
type ObjectSpec = xfervalue.ObjectSpec
type ChunkReceipt = xfervalue.ChunkReceipt
type Chunk = xfervalue.Chunk
type SinkCheckpoint = xfervalue.SinkCheckpoint
type CanonicalRecord = xfervalue.CanonicalRecord
type HistoryReceipt = xfervalue.HistoryReceipt
type HistorySet = xfervalue.HistorySet
type Status = xfervalue.Status
type StatusRequest = xfervalue.StatusRequest
type ReserveRequest = xfervalue.ReserveRequest
type CancelRequest = xfervalue.CancelRequest
type Prepare = xfervalue.Prepare
type Request = xfervalue.Request
type Response = xfervalue.Response
type ProbeResult = xfervalue.ProbeResult
type Principal = xfervalue.Principal
type Control = xfervalue.Control
type Reservation = xfervalue.Reservation
type ReservationID = xfervalue.ReservationID
type Salvage = xfervalue.Salvage
type LegacyResult = xfervalue.LegacyResult
