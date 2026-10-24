package tunnel

import (
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/LinZiyang666/tether/internal/workload"
)

var (
	ErrFlowNotImplemented      = errors.New("tunnel: not implemented (S5 P1)")
	ErrFlowImageNotImplemented = errors.New("tunnel: not implemented (S5 P7)")
)

type AuthRequest struct {
	Allocation Allocation
	Deadline   time.Time
	Stop       <-chan struct{}
}

type TokenLookupOperation func(AuthRequest) error
type ResumeAuthorizer func(AuthRequest) (Authority, error)
type FlowConfig struct {
	Budget        *workload.ReplayBudget
	InstanceID    string
	OwnerGen      uint64
	BootID        string
	MaxEndpoints  int
	MaxPendingFDs int
	TokenLookup   TokenLookupOperation
	Authorize     ResumeAuthorizer
	// Called before the first SYN; a zero ID denotes a legacy bridge whose
	// retained session remains non-exportable, not an anonymous v1 flow.
	BeforeLocalConnect func(flow.ID, rawsocket.Tuple) error
}

type ClientFrozen struct{ state *clientFrozenState }
type Census struct {
	Owned       int
	Legacy      int
	Admission   int
	Rehome      int
	Retained    int
	Unsupported bool
}

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
const FlowALPN = flowstate.FlowALPN
const DetachedGrace = flowstate.DetachedGrace
const ResumeAttempt = flowstate.ResumeAttempt
const LookupBudget = flowstate.LookupBudget
const PermitTTL = flowstate.PermitTTL
const CarrierWriteBudget = flowstate.CarrierWriteBudget
const MaxEndpoints = flowstate.MaxEndpoints

type EndpointPhase = flowstate.EndpointPhase

const EndpointPhaseBinding = flowstate.EndpointPhaseBinding
const EndpointPhaseAttached = flowstate.EndpointPhaseAttached
const EndpointPhaseDetached = flowstate.EndpointPhaseDetached
const EndpointPhasePrepared = flowstate.EndpointPhasePrepared
const EndpointPhaseClosing = flowstate.EndpointPhaseClosing
const EndpointPhaseClosed = flowstate.EndpointPhaseClosed

type TicketPurpose = flowstate.TicketPurpose

const TicketPurposeCurrentOwner = flowstate.TicketPurposeCurrentOwner
const TicketPurposeHandoff = flowstate.TicketPurposeHandoff

type Allocation = flowstate.Allocation
type AuthorizedKey = flowstate.AuthorizedKey
type Authority = flowstate.Authority

// OwnerGen starts nonzero and increases strictly by one on a handoff; overflow
// is a refusal. CarrierGen identifies an attachment, not a business socket.
type EndpointImage struct {
	ID         flow.EndpointID `json:"id"`
	OwnerGen   uint64          `json:"owner_gen"`
	CarrierGen uint64          `json:"carrier_gen"`
	FlowIDs    []flow.ID       `json:"flow_ids"`
	StateRef   string          `json:"state_ref"`
}
type ClientImage struct {
	Version   int             `json:"version"`
	Endpoints []EndpointImage `json:"endpoints"`
	Flows     flow.Image      `json:"flows"`
	StateRef  string          `json:"state_ref"`
}
