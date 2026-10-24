// Production wiring keeps the Broker owner thin. The broker binds exactly one
// endpoint for its canonical key; missing ports leave every entry refusing
// before it accepts work, and no hot capability is advertised by this package.
// origin: hot-update S8 plan §2.2 / §6.3 / §11.2 P15
package brokerupgrade

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Dependencies is supplied by the canonical broker bootstrap. The store is the
// sole journal writer for Key; the runtime owns the current database, image,
// admission gate and inherited lease. Imported is the exact accepted handle a
// restored image carried across exec, or zero when no cut was inherited.
// origin: hot-update S8 plan §2.2 / §3.3 / §11.2 P15
type Dependencies struct {
	Stage    func(context.Context, upgradetx.Grant) (upgradeartifact.Receipt, error)
	Key      upgradetx.Key
	Store    upgradetx.Store
	Runtime  upgradeport.RuntimePort
	Registry upgradeobserve.Registry
	Imported upgradetx.Handle
	// Lease adopts the inherited domain reservation once the restored image
	// holds ACTIVE responsibility (brokerimage.BootImage.UpgradeCut); nil when
	// no cut was inherited. The commit owner keeps it until completion.
	// origin: hot-update S8 plan §5.1 / M08 / §11.2 P17; S8 BROKER-EXPORTER R-11
	Lease  func() upgradelock.Lease
	heldMu sync.Mutex
	held   upgradelock.Lease
	// endpoint is the bound engine, for the Run loop's CutReturned.
	endpoint upgradeport.Endpoint
}

// Bind validates the canonical identity and returns the broker's only endpoint.
// role is the broker's live cluster-role sampler. It is consulted on every
// gated runtime step and never cached here; see GateRuntime.
func Bind(deps *Dependencies, role func() error) (upgradeport.Endpoint, upgradetx.Key, error) {
	if deps == nil {
		return nil, upgradetx.Key{}, nil
	}
	if deps.Key.Component != upgradetx.ComponentBroker || upgradetx.ValidateKey(deps.Key) != nil {
		return nil, upgradetx.Key{}, refusal(upgradetx.CodeRejected, "broker upgrade key does not name this broker component")
	}
	if deps.Store == nil || deps.Runtime == nil || deps.Registry == nil || role == nil {
		return nil, upgradetx.Key{}, refusal(upgradetx.CodeNotWired, "broker transaction adapters are unavailable")
	}
	deps.endpoint = New(deps.Store, GateRuntime(deps.Runtime, role), deps.Registry)
	return deps.endpoint, deps.Key, nil
}

// ResumeImported settles only the handle a restored image carried. It never
// loads whichever transaction is Current: a delayed boot callback must not
// acquire a newer transaction's fence (M11). A failure leaves the accepted
// record for query; this function cannot invent a terminal outcome.
// The inherited reservation is adopted first and released only once the
// completion is durable; an unresolved cut keeps it for recovery. A carried
// authority map that differs from the current store's (a session recreated
// or removed across the cut) is never completed: it is left for recovery.
// origin: hot-update S8 plan §3.3 / §5.1 / §6.1 / §8.1 / §11.2 P10 request to P15; S8 BROKER-EXPORTER R-11
func ResumeImported(ctx context.Context, endpoint upgradeport.Endpoint, deps *Dependencies, logger *slog.Logger) {
	if endpoint == nil || deps == nil || deps.Imported.TxID == "" {
		return
	}
	deps.heldMu.Lock()
	defer deps.heldMu.Unlock()
	if deps.held == nil && deps.Lease != nil {
		deps.held = deps.Lease()
	}
	if err := brokerupgradeport.MatchInherited(deps.Runtime); err != nil {
		if logger != nil {
			logger.Error("broker: inherited upgrade cut authority differs from the current store; leaving it for recovery", "tx_id", deps.Imported.TxID)
		}
		return
	}
	view, err := Resume(ctx, endpoint, importedRecoveryHandle(endpoint, deps.Imported))
	if err == nil && active(view.Historical).Phase == upgradetx.PhaseCompleted && deps.held != nil && view.Historical.Grant.ParentTxID == "" {
		_ = deps.held.Close()
		deps.held = nil
	}
	if err != nil && logger != nil {
		logger.Error("broker: inherited upgrade transaction retained unresolved evidence", "tx_id", deps.Imported.TxID)
	}
}

// importedRecoveryHandle reconciles only the verified adjunct's activation
// boundary. The source may journal an uncertain Arm reply after SealCut, then
// exec the target with that already sealed handle. Ordinary Resume retains its
// exact revision CAS: only this boot path can prove the carried activation's
// fresh target control and custody before submitting a new, explicit fence.
// origin: S8 internal review roll-recovery-stall F3
func importedRecoveryHandle(endpoint upgradeport.Endpoint, carried upgradetx.Handle) upgradetx.Handle {
	e, ok := endpoint.(*engine)
	if !ok || e.store == nil || e.runtime == nil || carried.Revision == 0 {
		return carried
	}
	view, err := e.store.Load(carried.Key, carried.TxID)
	if err != nil {
		return carried
	}
	a := active(view.Historical)
	if !sameAttempt(a.Handle, carried) || a.Phase != upgradetx.PhaseRecoveryRequired || view.Historical.Revision <= carried.Revision {
		return carried
	}
	activation := false
	for _, action := range view.Historical.Actions {
		if sameAttempt(action.Handle, carried) && action.Kind == upgradetx.ActionActivate && action.Handle.Revision == carried.Revision-1 {
			activation = true
		}
	}
	if !activation {
		return carried
	}
	expected := handle(view.Historical)
	stamp := makeStamp(expected, view.Historical.Grant.Issuer, view.Historical.Grant.ScopeDigest, nonce())
	o, err := e.runtime.Observe(stamp)
	if err != nil || !runningProof(o, stamp, a) || o.Current.Handle != expected ||
		!validReceipt(o.Current.Control, upgradetx.ReceiptControlReady, a, stamp, o.Census.Stamp.Producer) || o.Current.Control.Stamp.Nonce != stamp.Nonce {
		return carried
	}
	return expected
}

// errMembershipInFlight is the fresh-role refusal for a concurrent join/retire.
var errMembershipInFlight = errors.New("broker: a cluster membership operation is in flight")

// FreshRole is the broker cut role, read from the live Node at call time. It is
// the S6 topology rule plus the membership-operation census; both are sampled
// again by every gated step instead of trusting an admission snapshot.
// origin: hot-update S8 plan §6.3 / §8.2 F8 / §11.2 P15
func FreshRole(n *cluster.Node) error {
	if n == nil {
		return nil
	}
	if err := CheckLegacyReloadRole(n); err != nil {
		return err
	}
	ops, err := cluster.NonTerminalOperations(n.RODB())
	if err != nil {
		return err
	}
	if len(ops) != 0 {
		return errMembershipInFlight
	}
	return nil
}

// GateRuntime re-samples the broker's live cluster role before every step that
// can admit, freeze or activate a cut. Prepare, the first preflight and an S6
// admission sample are not authority for a later step: an election, a second
// voter or a membership operation can arrive between any two of them. Pre-
// activation refusals stay deferrable; the engine decides any accepted outcome.
// origin: hot-update S8 plan §6.3 / §8.2 F8 / §11.2 P15 (cached-role mutant)
func GateRuntime(inner upgradeport.RuntimePort, role func() error) upgradeport.RuntimePort {
	gated := gatedRuntime{RuntimePort: inner, role: role}
	// Archive's optional independent death observer must stay visible through
	// the decorator; hiding it would turn every archive into a missing port.
	if death, ok := inner.(deathObserver); ok {
		return gatedDeathRuntime{gatedRuntime: gated, death: death}
	}
	return gated
}

type deathObserver interface {
	ObserveDeath(upgradetx.Stamp) (upgradetx.DeathProof, error)
}

type gatedRuntime struct {
	upgradeport.RuntimePort
	role func() error
}

type gatedDeathRuntime struct {
	gatedRuntime
	death deathObserver
}

func (g gatedDeathRuntime) ObserveDeath(stamp upgradetx.Stamp) (upgradetx.DeathProof, error) {
	return g.death.ObserveDeath(stamp)
}

func (g gatedRuntime) admit() error {
	if err := g.role(); err != nil {
		if errors.Is(err, ErrLegacyReloadLeader) {
			return refusal(upgradetx.CodeDeferred, "transfer leadership before cutting a broker with multiple voters")
		}
		return refusal(upgradetx.CodeDeferred, "broker cluster role is not admissible for an upgrade cut")
	}
	return nil
}

func (g gatedRuntime) Preflight(ctx context.Context, grant upgradetx.Grant, artifact upgradeartifact.Receipt) (upgradeport.PreflightProof, error) {
	if err := g.admit(); err != nil {
		return upgradeport.PreflightProof{}, err
	}
	return g.RuntimePort.Preflight(ctx, grant, artifact)
}

func (g gatedRuntime) Freeze(h upgradetx.Handle) (upgradeport.Cut, error) {
	if err := g.admit(); err != nil {
		return upgradeport.Cut{}, err
	}
	return g.RuntimePort.Freeze(h)
}

func (g gatedRuntime) Activate(cut upgradeport.Cut) (upgradeport.Activation, error) {
	if err := g.admit(); err != nil {
		return upgradeport.Activation{}, ArmNotQueued(err)
	}
	return g.RuntimePort.Activate(cut)
}

// DescribeRuntime stays visible through the decorator (like ObserveDeath): an
// embedded interface would hide it and turn every describe into not wired.
// A description has no effect, so it is not role-gated; Start still is.
// origin: S8 FIX-DESCRIBE (P19 request R1)
func (g gatedRuntime) DescribeRuntime(nonce string) (upgradeport.Description, error) {
	describer, ok := g.RuntimePort.(upgradeport.RuntimeDescriber)
	if !ok {
		return upgradeport.Description{}, refusal(upgradetx.CodeNotWired, "broker runtime cannot describe itself")
	}
	return describer.DescribeRuntime(nonce)
}
