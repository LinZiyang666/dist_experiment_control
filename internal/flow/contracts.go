// Package flow owns logical bytes and OPEN retirement independently of a carrier.
// ACK describes a successful write to the original business socket, never carrier delivery.
// origin: hot-update S5 plan §2.1 / §2.2 / §3
package flow

import (
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/workload"
)

const (
	Version             = 1
	HeaderBytes         = 40
	MaxDataBytes        = 32 << 10
	MaxControlBytes     = 32 << 10
	MaxFlows            = 32
	MaxPendingFDs       = 32
	MaxRetirementHoles  = 1024
	MaxTerminalMetadata = 4096
)

var (
	ErrNotImplemented      = errors.New("flow: not implemented (S5 P1)")
	ErrCodecNotImplemented = errors.New("flow: not implemented (S5 P2)")
	ErrImageNotImplemented = errors.New("flow: not implemented (S5 P6)")
)

type ID [16]byte
type EndpointID [16]byte
type Kind uint8

const (
	KindOpen     Kind = 1
	KindOpenACK  Kind = 2
	KindResume   Kind = 3
	KindResumeOK Kind = 4
	KindReject   Kind = 5
	KindData     Kind = 6
	KindACK      Kind = 7
	KindFIN      Kind = 8
	KindClose    Kind = 9
)

type Phase uint8

const (
	FlowPhaseOpening    Phase = 1
	FlowPhaseConnecting Phase = 2
	FlowPhaseActive     Phase = 3
	FlowPhaseDraining   Phase = 4
	FlowPhaseTerminal   Phase = 5
)

type Reason uint16

const (
	ReasonMalformed      Reason = 1
	ReasonOffsetGap      Reason = 2
	ReasonOffsetExpired  Reason = 3
	ReasonIntentConflict Reason = 4
	ReasonUnknownFlow    Reason = 5
	ReasonRetired        Reason = 6
	ReasonRevoked        Reason = 7
	ReasonStaleEpoch     Reason = 8
	ReasonOwnerConflict  Reason = 9
	ReasonCapacity       Reason = 10
	ReasonUnauthorized   Reason = 11
	ReasonDeadline       Reason = 12
	ReasonUnsupported    Reason = 13
	ReasonIOFailure      Reason = 14
	ReasonNonceExhausted Reason = 15
	ReasonStateInvalid   Reason = 16
)

// Only ACK may set FINReceipt; only CLOSE may set CloseReceipt. Unknown bits fail closed.
const (
	FlagDirection    uint16 = 1 << 0
	FlagFINReceipt   uint16 = 1 << 1
	FlagCloseReceipt uint16 = 1 << 2
)

type ResumeScope uint8

const (
	ResumeScopeEndpoint ResumeScope = 0
	ResumeScopeFlow     ResumeScope = 1
)

type ResumeMode uint8

const (
	ResumeModeInitial   ResumeMode = 1
	ResumeModeReconnect ResumeMode = 2
	ResumeModePrepare   ResumeMode = 3
	ResumeModeActivate  ResumeMode = 4
	ResumeModeAbort     ResumeMode = 5
	ResumeModeBind      ResumeMode = 6
)

type ResumeVerdict uint8

const (
	ResumeVerdictAccepted ResumeVerdict = 1
	ResumeVerdictPending  ResumeVerdict = 2
	ResumeVerdictRejected ResumeVerdict = 3
)

type CloseMode uint8

const (
	CloseModeNormal     CloseMode = 0
	CloseModeReadAbort  CloseMode = 1
	CloseModeWriteAbort CloseMode = 2
	CloseModeRevoke     CloseMode = 3
)
const (
	MaxTicketBytes    = 512
	MaxResumePages    = 64
	MaxResumeBytes    = 2 << 20
	MaxKeyIDBytes     = 128
	MaxKeySecretBytes = 512
)

// Frame is a binary header followed by exactly BodyLen bytes; it is not a NATS message.
type Frame struct {
	Version    uint8
	Kind       Kind
	Flags      uint16
	BodyLen    uint32
	FlowID     ID
	CarrierGen uint64
	Offset     uint64
	Body       []byte
}
type Config struct {
	Budget        *workload.ReplayBudget
	MaxFlows      int
	MaxPendingFDs int
	Dial          func(ID, uint16, time.Time, <-chan struct{}) (*rawsocket.Socket, error)
	OnTerminal    func(ID, Reason)
}
type Manager struct{ state *managerState }
type Frozen struct{ state *frozenState }

// Public images hold opaque resource references and byte accounting only.
// Pending bytes and connect/authority decisions belong in sealed CutState records.
type RecordImage struct {
	ID         ID       `json:"id"`
	OpenSeq    uint64   `json:"open_seq"`
	Intent     [32]byte `json:"intent"`
	Phase      Phase    `json:"phase"`
	TxBase     uint64   `json:"tx_base"`
	TxEnd      uint64   `json:"tx_end"`
	RxApplied  uint64   `json:"rx_applied"`
	RxReceived uint64   `json:"rx_received"`
	TxFIN      uint64   `json:"tx_fin"`
	RxFIN      uint64   `json:"rx_fin"`
	FINFlags   uint8    `json:"fin_flags"`
	RawRef     string   `json:"raw_ref"`
	ReplayRef  string   `json:"replay_ref"`
}
type RetirementImage struct {
	EndpointID     EndpointID `json:"endpoint_id"`
	NextOpenSeq    uint64     `json:"next_open_seq"`
	RetiredThrough uint64     `json:"retired_through"`
	Holes          []uint64   `json:"holes"`
}
type Image struct {
	Version       int               `json:"version"`
	Records       []RecordImage     `json:"records"`
	Retirement    []RetirementImage `json:"retirement"`
	StateRef      string            `json:"state_ref"`
	ReservedBytes int64             `json:"reserved_bytes"`
}
