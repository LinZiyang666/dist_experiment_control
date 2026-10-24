// Package infracontract defines the neutral S9 infrastructure boundary.
// Only standard-library and controlbudget dependencies belong here.
// origin: hot-update S9 plan §2.1 / T01
package infracontract

type HostOp uint8

const (
	Inspect         HostOp = 1
	Stage           HostOp = 2
	FenceStart      HostOp = 3
	Promote         HostOp = 4
	SignalLDM       HostOp = 5
	SignalStop      HostOp = 6
	SignalGraceful  HostOp = 7
	StartWarm       HostOp = 8
	SwitchIngress   HostOp = 9
	FinalizeStorage HostOp = 10
	ReleaseStart    HostOp = 11
	RetireWarm      HostOp = 12
	RecoverHost     HostOp = 13
	RotatePolicy    HostOp = 14
	UpdateSupport   HostOp = 15
	LaunchClaim     HostOp = 16
	LaunchAck       HostOp = 17
	VerifyIngress   HostOp = 18
)

type HostRequest struct {
	Version         uint16
	Op              HostOp
	Component       Component
	Tx              ID
	Step            uint32
	Domain          ID
	Sequence        uint64
	PolicyRevision  uint64
	KeyID           string
	HostBootID      string
	Nonce           ID
	Expected        Process
	Candidate       Artifact
	Store           Store
	ExpectedJournal Digest
	Config          Digest
	BeforeCensus    Digest
	WarmCensus      Digest
	ReturnProof     Digest
	Payload         Digest
	RemainingNS     uint64
	Signature       []byte
}
type HostReceipt struct {
	Tx            ID
	Sequence      uint64
	Revision      uint64
	Op            HostOp
	Applied       bool
	RequestDigest Digest
	Journal       Digest
	Current       Process
	Census        Census
	Code          string
}
type Peer struct {
	PID        int32
	UID        uint32
	GID        uint32
	BootID     string
	StartTicks uint64
}

// These are protocol ceilings, not permission to truncate an incomplete census.
// The receiver deducts queue and transit time from RemainingNS before creating a
// local Deadline; replay must never renew that absolute deadline.
// origin: hot-update S9 plan §2.2 / T02
const (
	RequestVersion       uint16 = 1
	MaxRequestBytes             = 64 << 10
	MaxCensusPages              = 64
	MaxCensusPageBytes          = 64 << 10
	MaxCensusBytes              = 4 << 20
	RequestSigningDomain        = "tether-infra-host-v1\n"
)

// HostOpDescriptor freezes operand admission and the real backend owner. Fields
// absent from Required and Optional are forbidden. The common signed envelope
// (version, operation, component, transaction, step, domain, sequence, policy,
// key, boot, nonce and remaining budget) is required for every operation.
// Signature may be absent while assembling signing bytes; verification requires
// a complete Ed25519 signature. Inspect is still authenticated.
type HostOpDescriptor struct {
	Op         HostOp
	Owner      string
	Components []Component
	Required   []string
	Optional   []string
}

// HostOpDescriptors returns independent storage so callers cannot widen global
// admission by editing a returned descriptor. Payload only references immutable,
// root-verified records. Census and return digests must be resolved and rechecked
// under CAS by the backend; they are not caller assertions of zero responsibility.
// RotatePolicy has no process operand; it changes authority, not a process.
// origin: hot-update S9 plan §2.1 / §6.2 / §10.3 / T02
func HostOpDescriptors() []HostOpDescriptor {
	return []HostOpDescriptor{
		{Inspect, "P02", []Component{NATS, Caddy, HostSupport}, nil, []string{"Expected", "ExpectedJournal"}},
		{Stage, "P04", []Component{NATS, Caddy, HostSupport}, []string{"Candidate", "ExpectedJournal", "Payload"}, []string{"Expected", "Store", "Config"}},
		{FenceStart, "P05", []Component{NATS, Caddy}, []string{"Expected", "ExpectedJournal", "BeforeCensus"}, []string{"Candidate", "Store", "Config", "Payload"}},
		{Promote, "P04", []Component{NATS, Caddy}, []string{"Expected", "Candidate", "ExpectedJournal", "Config", "Payload"}, []string{"Store", "BeforeCensus"}},
		{SignalLDM, "P02", []Component{NATS}, []string{"Expected", "ExpectedJournal", "BeforeCensus", "Payload"}, nil},
		{SignalStop, "P02", []Component{NATS}, []string{"Expected", "ExpectedJournal", "BeforeCensus", "Payload"}, nil},
		{SignalGraceful, "P02", []Component{Caddy}, []string{"Expected", "ExpectedJournal", "BeforeCensus", "Payload"}, nil},
		{StartWarm, "P05", []Component{Caddy}, []string{"Expected", "Candidate", "ExpectedJournal", "Config", "Payload"}, []string{"Store", "BeforeCensus"}},
		{SwitchIngress, "P07", []Component{Caddy}, []string{"Expected", "ExpectedJournal", "BeforeCensus", "WarmCensus", "Payload"}, []string{"ReturnProof"}},
		{FinalizeStorage, "P09", []Component{Caddy}, []string{"Expected", "ExpectedJournal", "BeforeCensus", "Payload"}, []string{"Store", "Config"}},
		{ReleaseStart, "P05", []Component{NATS, Caddy}, []string{"Expected", "Candidate", "ExpectedJournal", "Config", "Payload"}, []string{"Store", "BeforeCensus"}},
		{RetireWarm, "P05", []Component{Caddy}, []string{"Expected", "ExpectedJournal", "WarmCensus", "ReturnProof"}, []string{"BeforeCensus", "Payload"}},
		{RecoverHost, "P04", []Component{NATS, Caddy, HostSupport}, []string{"Expected", "ExpectedJournal", "Payload"}, []string{"Candidate", "Store", "Config", "BeforeCensus", "WarmCensus", "ReturnProof"}},
		{RotatePolicy, "P03", []Component{HostSupport}, []string{"ExpectedJournal", "Payload"}, nil},
		{UpdateSupport, "P04", []Component{HostSupport}, []string{"Expected", "Candidate", "ExpectedJournal", "Payload"}, []string{"BeforeCensus", "ReturnProof"}},
		{LaunchClaim, "P06", []Component{NATS, Caddy}, []string{"Expected", "Candidate", "ExpectedJournal", "Config", "Payload"}, []string{"Store"}},
		{LaunchAck, "P06", []Component{NATS, Caddy}, []string{"Expected", "Candidate", "ExpectedJournal", "BeforeCensus", "Payload"}, []string{"Store", "Config"}},
		{VerifyIngress, "P40", []Component{Caddy}, []string{"Expected", "BeforeCensus", "Payload"}, nil},
	}
}
