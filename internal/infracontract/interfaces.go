// Package infracontract defines the neutral S9 infrastructure boundary.
// Only standard-library and controlbudget dependencies belong here.
// origin: hot-update S9 plan §2.1 / T01
package infracontract

import "errors"

var (
	ErrContractStub = errors.New("infra_contract_stub")
	ErrUnsupported  = errors.New("infra_unsupported")
	ErrIncomplete   = errors.New("infra_inventory_incomplete")
)

type Host interface {
	Do(HostRequest, Deadline, Stop) (HostReceipt, error)
	Inspect(ID, Deadline) (Census, error)
}

// Index zero is forbidden; all eighteen backends must be present exactly once.
// P04 validates the table, and P40 composes the real root executable. Missing
// dependencies must never advertise a capability or produce a completed status.
type HostBindings [19]Host
type HostFactory func(HostBindings) (Host, error)
type Artifacts interface {
	Resolve(ArtifactKind, string, string, string) (Artifact, error)
	Verify(Artifact, Deadline, Stop) error
	Compatible(Artifact, Artifact, Digest, Store, Deadline) error
}
type ReadyProbe interface {
	Read(Plan, Deadline, Stop) (Readiness, error)
}
type Cohort interface {
	Seal(Plan, Deadline) (Checkpoint, error)
	Install(Checkpoint, RouteProof, Deadline) error
	Move(Checkpoint, Deadline, Stop) (MoveResult, error)
	Return(Checkpoint, Deadline, Stop) (MoveResult, error)
	Bind(Checkpoint, []RouteProof, Deadline) (Census, error)
	Release(Checkpoint, Deadline) error
}
type Routes interface {
	Prepare(Plan, Checkpoint, RouteDirection, Deadline) ([]RouteProof, error)
	Bind(Plan, MoveResult, RouteDirection, Deadline) ([]RouteProof, error)
}
type Transfers interface {
	Fence(ID, Deadline) (TransferSeal, error)
	Rebind(TransferSeal, Deadline) error
	Release(TransferSeal, Deadline) error
}
type Recovery interface {
	Seal(Plan, Deadline) (Checkpoint, error)
	VerifyBeforeReplay(Checkpoint, Deadline, Stop) error
	Release(Checkpoint, Deadline) error
}
type Transaction interface {
	Acquire(Plan, Deadline) (Status, error)
	CompareAndSwap(Status, Phase, Digest, Deadline) (Status, error)
	Load(ID, Deadline) (Status, error)
}
type Observer interface{ Record(Observation) error }

// Dependencies are memory-only bindings. Do not serialize interfaces or place
// live S9 owners in the S5/S6 image: the upgrade domain excludes concurrent cuts.
type Dependencies struct {
	Host      Host
	Artifacts Artifacts
	Ready     ReadyProbe
	Cohort    Cohort
	Routes    Routes
	Transfers Transfers
	Recovery  Recovery
	Tx        Transaction
	Observe   Observer
}
type Driver interface {
	Advance(Plan, Deadline, Stop) (Status, error)
	Recover(Plan, Deadline, Stop) (Status, error)
}
type DriverFactory func(Dependencies) (Driver, error)
type Service interface {
	Start(Plan, Deadline, Stop) (Status, error)
	Resume(ID, Deadline, Stop) (Status, error)
	Status(ID, Deadline) (Status, error)
}
type ServiceFactory func(Dependencies, Driver, Driver) (Service, error)
