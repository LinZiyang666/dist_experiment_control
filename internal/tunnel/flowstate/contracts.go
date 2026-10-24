package flowstate

import (
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
)

// These deadlines bound individual operations, never extend the first detach or cut.
// origin: hot-update S5 plan §2.1 / §4.5
const (
	FlowALPN           = "tether-flow/1"
	DetachedGrace      = 8 * time.Second
	ResumeAttempt      = 2 * time.Second
	LookupBudget       = time.Second
	PermitTTL          = 120 * time.Second
	CarrierWriteBudget = time.Second
	MaxEndpoints       = 128
)

type EndpointPhase uint8

const (
	EndpointPhaseBinding  EndpointPhase = 1
	EndpointPhaseAttached EndpointPhase = 2
	EndpointPhaseDetached EndpointPhase = 3
	EndpointPhasePrepared EndpointPhase = 4
	EndpointPhaseClosing  EndpointPhase = 5
	EndpointPhaseClosed   EndpointPhase = 6
)

type TicketPurpose uint8

const (
	TicketPurposeCurrentOwner TicketPurpose = 1
	TicketPurposeHandoff      TicketPurpose = 2
)

// Allocation and Authority are private TLS/sealed-state values. AppliedRevision
// is a SQLite command revision; it must not be relabeled as a Raft log index.
// origin: hot-update S5 plan §2.1 / §4.3 / lead Q1
type Allocation struct {
	SID       string
	NID       string
	Port      int
	TokenHash string
	Epoch     int64
}

type AuthorizedKey struct {
	ID     string
	Secret []byte
}

type Authority struct {
	Enabled         bool
	Allocation      Allocation
	Generation      int64
	ProxyEpoch      int64
	AppliedRevision uint64
	Keys            []AuthorizedKey
}

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
