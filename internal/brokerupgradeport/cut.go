// cut.go arms the S6 broker store cut with one accepted S8 cut.
// The S6 pipeline (brokerimage.ReloadCurrentStore, run by the Broker's Run
// loop) stays the only freeze/close/export/exec path; an armed plan changes
// exactly four of its inputs, through hooks the Broker calls where those
// inputs are used (bound to its Dependencies by internal/brokerupgrade):
//
//   - CutReaders: the readers its preflight probes — the immutable target slot
//     and the published source slot (the target image's rollback reader).
//   - SealCut: after its freeze (business fenced, store still open) the
//     complete session authority map is read and the upgrade-cut-v1 adjunct is
//     packed with a duplicate of the domain lease.
//   - CutResources: the adjunct travels in every final Prepare.
//   - CutPath: the exec runs the target slot.
//
// Activate returns once the S6 cut is ARMED — its final image exists over the
// closed store — so the Start reply can be written before the cut drains the
// initiating admin reply (adminsock.JoinImageReplies) and execs. A reload that
// returns without exec (Returned) releases the reservation and tells the
// engine whether the Start already returned, so it can journal the
// current-state recovery itself.
// origin: hot-update S8 plan §4.1 / §5.1 / §6.1 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
package brokerupgradeport

import (
	"database/sql"
	"fmt"
	"os"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// ReasonActivationDispatched is the Start reply of an armed broker cut: the
// final image exists and the exec follows; the transaction is accepted and
// its outcome belongs to the imported target (query/wait).
const ReasonActivationDispatched = "broker_activation_dispatched"

// armBudget bounds Activate's wait for the S6 cut to arm: the Run loop picks
// the request up, probes both readers and closes the store within its own
// five-second cut budget.
const armBudget = 3 * upgradebudget.RequestLatency

// armedCut is the S8 plan of one S6 store cut. mu orders Activate's view of
// the arm result against the Run loop's Returned: a plan is pending until
// Activate saw it armed (dispatched) or its reload returned (returned).
type armedCut struct {
	mu                   sync.Mutex
	state                upgradecut.State
	target, previous     string
	held                 *heldCut
	dispatched, returned bool
	resource             handoff.Resource
	duplicate            *os.File
}

func errDispatched() error {
	return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: ReasonActivationDispatched, Accepted: true}
}

// Activate arms the S6 store cut with this cut's plan and waits until its
// final image exists (dispatched: the exec follows and never returns here) or
// the cut refused before arming (an accepted unknown: the engine journals the
// current-state recovery). The durable partial_active intent precedes it.
// origin: hot-update S8 plan §4.1 / §5.1 / §6.1 / M08
func (p *brokerRuntime) Activate(cut upgradeport.Cut) (upgradeport.Activation, error) {
	unavailable := &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: ReasonActivationUnavailable, Accepted: true}
	port := p.port.Load()
	if port == nil {
		return upgradeport.Activation{}, cutUnavailable() // no Broker: nothing can be armed
	}
	p.mu.Lock()
	held, ready := p.held, p.ready[cut.Attempt.TxID]
	if port == nil || port.Arm == nil || held == nil || !upgradetx.SameCut(held.handle, cut.Attempt) || held.sequence != cut.Sequence || ready.target == "" {
		p.mu.Unlock()
		return upgradeport.Activation{}, unavailable
	}
	// From here Activate (or the Run loop's Returned) owns the reservation.
	held.activating = true
	held.timer.Stop()
	p.mu.Unlock()
	view, started := p.record(cut.Attempt)
	if !started {
		p.release(held)
		return upgradeport.Activation{}, unavailable
	}
	plan := &armedCut{target: ready.target, previous: ready.previous, held: held,
		state: upgradecut.State{ParentToken: view.Historical.Grant.ParentTxID, Attempt: cut.Attempt, SourceSHA: ready.sourceSHA, TargetSHA: active(view.Historical).TargetSHA, Owner: cut.Owner, Scope: cut.Scope}}
	if !p.armed.CompareAndSwap(nil, plan) {
		p.release(held)
		return upgradeport.Activation{}, unavailable
	}
	deadline, err := controlbudget.After(armBudget)
	if err == nil {
		err = port.Arm(deadline)
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	// origin: S8 internal review roll-recovery-stall F5
	// Admission refused before any S6 request existed. There can be no Run-side
	// Returned callback; retain no armed plan or stopped reservation timer here.
	if activationUnstarted(err) {
		p.armed.CompareAndSwap(plan, nil)
		plan.returned = true
		plan.closeAdjunct()
		p.release(held)
	}
	if err == nil && !plan.dispatched && !plan.returned {
		plan.dispatched = true
		return upgradeport.Activation{}, errDispatched()
	}
	// Refused before its final image, or already returned: Returned owns the
	// cleanup once the reload ends; nothing here may exec again.
	unarmed := &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: "broker store cut was not armed", Accepted: true}
	// origin: S8 internal review roll-recovery-stall F2
	// Keep the stable public refusal and the local cause independently. Wire
	// adapters extract the typed refusal; diagnostics must retain the actual
	// maintenance, reader or store failure rather than erase it at this seam.
	if err != nil {
		return upgradeport.Activation{}, fmt.Errorf("%w: %w", unarmed, err)
	}
	return upgradeport.Activation{}, unarmed
}

func armedOf(runtime upgradeport.RuntimePort) *armedCut {
	if rt := runtimeOf(runtime); rt != nil {
		return rt.armed.Load()
	}
	return nil
}

// CutReaders returns the readers the next S6 preflight probes: an armed cut's
// target slot and published source slot (no expected-previous pin: the source
// is the running bytes), otherwise the S6 defaults unchanged.
func CutReaders(runtime upgradeport.RuntimePort, candidate, previous string) (string, string, string) {
	if plan := armedOf(runtime); plan != nil {
		return plan.target, plan.previous, ""
	}
	return candidate, previous, ""
}

// CutPath is the executable the S6 exec runs: an armed cut's target slot.
func CutPath(runtime upgradeport.RuntimePort, path string) string {
	if plan := armedOf(runtime); plan != nil {
		return plan.target
	}
	return path
}

// SealCut runs after the S6 freeze, before the store closes (prior is the S6
// check that runs there). The authority map is read now, with session create
// and remove fenced, so it is exactly the store the image carries.
// origin: hot-update S8 plan §6.1 / §6.2 / §11.2 P17
func SealCut(runtime upgradeport.RuntimePort, db *sql.DB, prior error) error {
	plan := armedOf(runtime)
	if prior != nil || plan == nil {
		return prior
	}
	authorities, err := SessionAuthorities(db)
	if err != nil {
		return err
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	if plan.dispatched || plan.returned {
		return refusal(upgradetx.CodeUnsettled, "broker store cut is no longer armed")
	}
	state := plan.state
	state.Authorities = authorities
	duplicate, err := dupLease(plan.held.lease)
	if err != nil {
		return refusal(upgradetx.CodeUnsettled, "upgrade lease custody requires recovery")
	}
	resource, err := upgradecut.Pack(state, map[uint32]*os.File{state.LockFD: duplicate})
	if err != nil {
		_ = duplicate.Close()
		return err
	}
	plan.closeAdjunct()
	plan.state, plan.resource, plan.duplicate = state, resource, duplicate
	return nil
}

// CutResources are the extra resources every final S6 Prepare carries: the
// sealed adjunct of an armed cut, none otherwise. Prepare duplicates its
// file, so the plan keeps its own until the reload ends.
func CutResources(runtime upgradeport.RuntimePort) []handoff.Resource {
	plan := armedOf(runtime)
	if plan == nil {
		return nil
	}
	plan.mu.Lock()
	defer plan.mu.Unlock()
	if plan.resource.ID == "" {
		return nil
	}
	return []handoff.Resource{plan.resource}
}

// Returned is called when an S6 reload returned: a successful exec never
// returns, so the source still serves. The armed plan is disarmed and its
// reservation released. It reports the plan's handle and whether its Start
// already returned (dispatched): only then does the caller journal the
// accepted cut's current-state recovery; otherwise Activate is still waiting
// and the engine journals it.
func Returned(runtime upgradeport.RuntimePort) (upgradetx.Handle, bool) {
	rt := runtimeOf(runtime)
	if rt == nil {
		return upgradetx.Handle{}, false
	}
	plan := rt.armed.Swap(nil)
	if plan == nil {
		return upgradetx.Handle{}, false
	}
	plan.mu.Lock()
	dispatched := plan.dispatched
	plan.returned = true
	plan.closeAdjunct()
	plan.mu.Unlock()
	rt.release(plan.held)
	return plan.state.Attempt, dispatched
}

func (c *armedCut) closeAdjunct() {
	if c.duplicate != nil {
		_ = c.duplicate.Close()
	}
	c.duplicate, c.resource = nil, handoff.Resource{}
}

// dupLease lends Pack a duplicate of the reservation's OFD. Closing the
// duplicate never issues LOCK_UN on the exporter's lease.
func dupLease(lease upgradelock.Lease) (*os.File, error) {
	fd, err := unix.FcntlInt(lease.FD(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "upgrade-lease-export"), nil
}

// VerifyInherited admits a carried adjunct against this broker before its
// import takes any descriptor: the configured Key, the running bytes (the
// target) and the accepted journal attempt that dispatched exactly this cut —
// unsettled, with a durable activation intent and matching image SHAs.
// origin: hot-update S8 plan §3.3 / §5.1 / §6.1 / §11.2 P17; S8 BROKER-EXPORTER R-11
func VerifyInherited(journal upgradetx.Store, key upgradetx.Key, state upgradecut.State, running string) error {
	refused := refusal(upgradetx.CodeRejected, "upgrade cut is not the accepted activation of this broker")
	if journal == nil || state.Attempt.Key != key || running != state.TargetSHA {
		return refused
	}
	view, err := journal.Load(key, state.Attempt.TxID)
	if err != nil {
		return err
	}
	a := active(view.Historical)
	if state.ParentToken != view.Historical.Grant.ParentTxID || !upgradetx.SameCut(a.Handle, state.Attempt) || state.Attempt.Revision > view.Historical.Revision ||
		a.SourceSHA != state.SourceSHA || a.TargetSHA != state.TargetSHA ||
		a.CutStage != upgradetx.CutPartialActive && a.CutStage != upgradetx.CutActive ||
		a.Phase == upgradetx.PhaseCompleted || a.Phase == upgradetx.PhaseRolledBack || a.Phase == upgradetx.PhaseCancelled || a.Phase == upgradetx.PhaseAbandoned {
		return refused
	}
	return nil
}

// MatchInherited compares the carried adjunct's complete authority map with
// the authority rows of the store this image opened
// (upgradecut.MatchAuthorities). A runtime that imported no cut matches.
// origin: hot-update S8 plan §6.1 / §6.2 / §11.2 P17; S8 BROKER-EXPORTER R-11
func MatchInherited(runtime upgradeport.RuntimePort) error {
	rt := runtimeOf(runtime)
	if rt == nil || rt.host.Inherited == nil {
		return nil
	}
	port := rt.port.Load()
	if port == nil || port.DB == nil {
		return refusal(upgradetx.CodeDeferred, ReasonIncarnationUnknown)
	}
	actual, err := SessionAuthorities(port.DB())
	if err != nil {
		return err
	}
	return upgradecut.MatchAuthorities(rt.host.Inherited.Authorities, actual)
}

// ArmedGrant is available only after the runtime's Activate established the
// exact cut. Maintenance admission must use this journal identity, never an
// independently supplied holder or CutID.
func ArmedGrant(runtime upgradeport.RuntimePort, store upgradetx.Store) upgradetx.Grant {
	plan := armedOf(runtime)
	if plan == nil || store == nil {
		return upgradetx.Grant{}
	}
	v, err := store.Load(plan.state.Attempt.Key, plan.state.Attempt.TxID)
	if err != nil || !upgradetx.SameCut(v.Historical.Forward.Handle, plan.state.Attempt) {
		return upgradetx.Grant{}
	}
	return v.Historical.Grant
}
