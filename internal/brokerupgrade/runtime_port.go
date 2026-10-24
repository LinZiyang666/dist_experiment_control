// runtime_port.go binds the broker runtime adapter (internal/brokerupgradeport,
// the broker's counterpart of agentupgradeport) to this engine: the boot
// construction names it keeps (RuntimeHost, NewRuntime), the Broker's port
// attachment, and the four S6 store-cut hooks keyed by the Broker's
// Dependencies. A reload that returns after its Start already replied is the
// one hook that needs the engine: it journals the cut's current-state
// recovery through this endpoint's sequencer (settleReturned).
// origin: hot-update S8 plan §2.2 / §4.1 / §6.1 / §11.2 P15 P17;
// S8 FIX-DESCRIBE (P19 request R4); S8 BROKER-EXPORTER R-11
package brokerupgrade

import (
	"database/sql"
	"errors"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Public refusal reasons of the broker runtime (brokerupgradeport).
const (
	ReasonBrokerCutUnavailable = brokerupgradeport.ReasonBrokerCutUnavailable
	ReasonDomainCensus         = brokerupgradeport.ReasonDomainCensus
	ReasonUpgradeInProgress    = brokerupgradeport.ReasonUpgradeInProgress
	ReasonActivationDispatched = brokerupgradeport.ReasonActivationDispatched
)

// ArmNotQueued preserves the admission owner's positive proof that it refused
// before requesting any reload. A timeout after queueing must never use it.
// origin: S8 internal review roll-recovery-stall F5
func ArmNotQueued(cause error) error { return unqueuedActivation{cause} }

// Maintenance admission precedes queueing in every topology. Its five-second
// window is independent of Activate's wait for Run; only a pre-queue refusal
// proves ActivationUnstarted. An arm timeout retains its unknown outcome.
// origin: S6 internal review SINGLEACK F1 / lead decision D-C / plan §7.1
func ArmPlannedCut(deadline controlbudget.Deadline, admit func(controlbudget.Deadline) error, arm func(controlbudget.Deadline) error) error {
	admission, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		return ArmNotQueued(err)
	}
	if err := admit(min(deadline, admission)); err != nil {
		return ArmNotQueued(err)
	}
	return arm(deadline)
}

type unqueuedActivation struct{ error }

func (unqueuedActivation) ActivationUnstarted() bool { return true }
func (e unqueuedActivation) Unwrap() error           { return e.error }

func activationUnstarted(err error) bool {
	var proof interface{ ActivationUnstarted() bool }
	return errors.As(err, &proof) && proof.ActivationUnstarted()
}

// RuntimeHost is the running broker's half known at boot
// (brokerupgradeport.RuntimeHost); BrokerPort is the Broker's half, and
// BrokerFacts the bounded sample its Facts returns (TunnelSession: one of
// its installed tunnel allocations).
type (
	RuntimeHost   = brokerupgradeport.RuntimeHost
	BrokerPort    = brokerupgradeport.BrokerPort
	BrokerFacts   = brokerupgradeport.BrokerFacts
	TunnelSession = brokerupgradeport.TunnelSession
)

// NewRuntime returns the broker runtime adapter. Missing callbacks are
// refusals at the operation that needs them, never an empty observation.
func NewRuntime(host RuntimeHost) upgradeport.RuntimePort { return brokerupgradeport.NewRuntime(host) }

// Attach installs the running Broker's port into its registered runtime and
// returns deps, so the Broker can bind in the same expression.
func Attach(deps *Dependencies, port BrokerPort) *Dependencies {
	if deps != nil {
		brokerupgradeport.Attach(deps.Runtime, port)
	}
	return deps
}

func runtimeOf(deps *Dependencies) upgradeport.RuntimePort {
	if deps == nil {
		return nil
	}
	return deps.Runtime
}

// CutReaders, CutPath, SealCut and CutResources are the S6 store cut's four
// inputs an armed S8 cut changes (brokerupgradeport/cut.go).
func CutReaders(deps *Dependencies, candidate, previous string) (string, string, string) {
	return brokerupgradeport.CutReaders(runtimeOf(deps), candidate, previous)
}
func CutPath(deps *Dependencies, path string) string {
	return brokerupgradeport.CutPath(runtimeOf(deps), path)
}
func SealCut(deps *Dependencies, db *sql.DB, prior error) error {
	return brokerupgradeport.SealCut(runtimeOf(deps), db, prior)
}
func CutResources(deps *Dependencies) []handoff.Resource {
	return brokerupgradeport.CutResources(runtimeOf(deps))
}

// CutReturned is called by the Run loop when an S6 reload returned: a
// successful exec never returns, so cause is the reason the source still
// serves. The armed plan is disarmed and its reservation released. If its
// Start already returned (dispatched), the accepted cut's current-state
// recovery is journaled here; otherwise Activate is still waiting and the
// engine journals it.
func CutReturned(deps *Dependencies, cause error) error {
	attempt, dispatched := brokerupgradeport.Returned(runtimeOf(deps))
	if dispatched && deps.endpoint != nil {
		settleReturned(deps.endpoint, attempt, cause)
	}
	return cause
}

// activationDispatched recognizes the runtime's armed Activate reply.
func activationDispatched(err error) bool {
	var typed *upgradetx.Error
	return errors.As(err, &typed) && typed.Code == upgradetx.CodeUnsettled && typed.Detail == ReasonActivationDispatched && typed.Accepted
}

// settleReturned journals that an armed activation returned without exec: the
// source still serves, so the accepted cut requires current-state recovery.
// The engine is idle (Start returned); a busy engine is retried briefly, and
// a record that moved on is left to its new owner.
func settleReturned(endpoint upgradeport.Endpoint, expected upgradetx.Handle, cause error) {
	e, ok := endpoint.(*engine)
	if !ok {
		return
	}
	until := time.Now().Add(upgradebudget.RequestLatency)
	for !e.mu.TryLock() {
		if time.Now().After(until) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	defer e.mu.Unlock()
	view, err := e.loadExpected(expected)
	if err != nil {
		return
	}
	if cause == nil {
		cause = refusal(upgradetx.CodeUnsettled, "broker activation returned without exec")
	}
	_, _ = e.failed(view.Historical, cause)
}

// unwiredRegistry is the completion registry of a broker that never imported
// a cut: no admission, snapshot or seal can be signed here.
type unwiredRegistry struct{}

// UnwiredRegistry refuses every completion operation with upgrade_not_wired.
func UnwiredRegistry() upgradeobserve.Registry { return unwiredRegistry{} }

func registryUnwired() error {
	return refusal(upgradetx.CodeNotWired, "broker completion registry belongs to an importing process")
}
func (unwiredRegistry) Begin(upgradetx.Resource) (uint64, error) { return 0, registryUnwired() }
func (unwiredRegistry) Finish(uint64, upgradetx.Resource) error  { return registryUnwired() }
func (unwiredRegistry) Snapshot(upgradetx.Stamp) (upgradetx.Census, error) {
	return upgradetx.Census{}, registryUnwired()
}
func (unwiredRegistry) Seal(upgradetx.Attempt, string) (upgradetx.Receipt, error) {
	return upgradetx.Receipt{}, registryUnwired()
}
