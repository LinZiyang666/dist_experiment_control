// Package contractstub contains permanent, inert compile adapters. Production
// must not import it, directly or transitively. Real owners implement the frozen
// interfaces in their own packages; these adapters never grow an implementation.
// origin: hot-update S9 plan §2.1 / T01
package contractstub

import (
	"errors"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Each error names the implementation owner and remains errors.Is compatible
// with ErrContractStub. Error text contains no caller-supplied fields.
type failure string

func (f failure) Error() string { return "not implemented (S9 " + string(f) + ")" }
func (failure) Unwrap() error   { return c.ErrContractStub }

// NewDependencies allocates only inert values, never files, workers or sockets.
func NewDependencies() c.Dependencies {
	return c.Dependencies{Host: host{}, Artifacts: artifacts{}, Ready: ready{}, Cohort: cohort{}, Routes: routes{}, Transfers: transfers{}, Recovery: recovery{}, Tx: transaction{}, Observe: observer{}}
}

// NewDriver has no component argument, so its factory failure names both actual
// driver owners. Advance/Recover can identify the single owner from the Plan.
func NewDriver(c.Dependencies) (c.Driver, error) {
	return nil, errors.Join(failure("P10"), failure("P13"))
}
func NewService(c.Dependencies, c.Driver, c.Driver) (c.Service, error) {
	return nil, failure("P20")
}

var (
	_ c.DriverFactory  = NewDriver
	_ c.ServiceFactory = NewService
	_ c.Host           = host{}
	_ c.Artifacts      = artifacts{}
	_ c.ReadyProbe     = ready{}
	_ c.Cohort         = cohort{}
	_ c.Routes         = routes{}
	_ c.Transfers      = transfers{}
	_ c.Recovery       = recovery{}
	_ c.Transaction    = transaction{}
	_ c.Observer       = observer{}
	_ c.Driver         = driver{}
	_ c.Service        = service{}
)

type host struct{}

func (host) Do(r c.HostRequest, _ c.Deadline, _ c.Stop) (c.HostReceipt, error) {
	for _, row := range c.HostOpDescriptors() {
		if row.Op == r.Op {
			return c.HostReceipt{}, failure(row.Owner)
		}
	}
	return c.HostReceipt{}, failure("P04")
}
func (host) Inspect(c.ID, c.Deadline) (c.Census, error) { return c.Census{}, failure("P02") }

type artifacts struct{}

func (artifacts) Resolve(c.ArtifactKind, string, string, string) (c.Artifact, error) {
	return c.Artifact{}, failure("P01")
}
func (artifacts) Verify(c.Artifact, c.Deadline, c.Stop) error { return failure("P01") }
func (artifacts) Compatible(c.Artifact, c.Artifact, c.Digest, c.Store, c.Deadline) error {
	return failure("P01")
}

type ready struct{}

func (ready) Read(c.Plan, c.Deadline, c.Stop) (c.Readiness, error) {
	return c.Readiness{}, failure("P11")
}

type cohort struct{}

func (cohort) Seal(c.Plan, c.Deadline) (c.Checkpoint, error)        { return c.Checkpoint{}, failure("P15") }
func (cohort) Install(c.Checkpoint, c.RouteProof, c.Deadline) error { return failure("P15") }
func (cohort) Move(c.Checkpoint, c.Deadline, c.Stop) (c.MoveResult, error) {
	return c.MoveResult{}, failure("P15")
}
func (cohort) Return(c.Checkpoint, c.Deadline, c.Stop) (c.MoveResult, error) {
	return c.MoveResult{}, failure("P15")
}
func (cohort) Bind(c.Checkpoint, []c.RouteProof, c.Deadline) (c.Census, error) {
	return c.Census{}, failure("P15")
}
func (cohort) Release(c.Checkpoint, c.Deadline) error { return failure("P15") }

type routes struct{}

func (routes) Prepare(c.Plan, c.Checkpoint, c.RouteDirection, c.Deadline) ([]c.RouteProof, error) {
	return nil, failure("P16")
}
func (routes) Bind(c.Plan, c.MoveResult, c.RouteDirection, c.Deadline) ([]c.RouteProof, error) {
	return nil, failure("P16")
}

type transfers struct{}

func (transfers) Fence(c.ID, c.Deadline) (c.TransferSeal, error) {
	return c.TransferSeal{}, failure("P19")
}
func (transfers) Rebind(c.TransferSeal, c.Deadline) error  { return failure("P19") }
func (transfers) Release(c.TransferSeal, c.Deadline) error { return failure("P19") }

type recovery struct{}

func (recovery) Seal(c.Plan, c.Deadline) (c.Checkpoint, error)             { return c.Checkpoint{}, failure("P18") }
func (recovery) VerifyBeforeReplay(c.Checkpoint, c.Deadline, c.Stop) error { return failure("P18") }
func (recovery) Release(c.Checkpoint, c.Deadline) error                    { return failure("P18") }

type transaction struct{}

func (transaction) Acquire(c.Plan, c.Deadline) (c.Status, error) { return c.Status{}, failure("P20") }
func (transaction) CompareAndSwap(c.Status, c.Phase, c.Digest, c.Deadline) (c.Status, error) {
	return c.Status{}, failure("P20")
}
func (transaction) Load(c.ID, c.Deadline) (c.Status, error) { return c.Status{}, failure("P20") }

type observer struct{}

func (observer) Record(c.Observation) error { return failure("P02") }

type driver struct{}

func (driver) Advance(p c.Plan, _ c.Deadline, _ c.Stop) (c.Status, error) {
	return c.Status{}, driverFailure(p)
}
func (driver) Recover(p c.Plan, _ c.Deadline, _ c.Stop) (c.Status, error) {
	return c.Status{}, driverFailure(p)
}
func driverFailure(p c.Plan) error {
	if p.Component == c.NATS {
		return failure("P13")
	}
	if p.Component == c.Caddy {
		return failure("P10")
	}
	return errors.Join(failure("P10"), failure("P13"))
}

type service struct{}

func (service) Start(c.Plan, c.Deadline, c.Stop) (c.Status, error) { return c.Status{}, failure("P20") }
func (service) Resume(c.ID, c.Deadline, c.Stop) (c.Status, error)  { return c.Status{}, failure("P20") }
func (service) Status(c.ID, c.Deadline) (c.Status, error)          { return c.Status{}, failure("P20") }
