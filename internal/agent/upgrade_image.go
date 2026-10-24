// upgrade_image.go — the running agent's half of a same-PID image switch
// (hot-update S3, architecture §22.6): the admission gate a cut closes, the
// preflight deferred set, the cut itself (agentimage.Freeze over this
// agent's owners), and the re-dispatch exec shared by every path that hands
// a bundle to another binary. Package-level functions and small types only:
// Agent's method budget is a ratchet (structural_budget_golden: 125).
// origin: docs/reviews/hot-update-agent-bootstrap-plan.md §2 / §4.
package agent

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/LinZiyang666/tether/internal/xferagentimage"
	"github.com/LinZiyang666/tether/internal/xfercontract"

	"github.com/nats-io/nats.go"
)

// errSessionGone wraps a register refusal that says the session itself is
// gone (session_not_found / session_not_found_or_deleting): the session
// authority is revoked, so the carried workloads were revoked with it and a
// pending upgrade must NOT re-dispatch them to the previous binary (plan
// §3.3 / §3.5).
var errSessionGone = errors.New("agent: session is gone")

// gateKind names an admission site the cut must close before it freezes:
// the legacy (non-reliable) paths that start work the image cannot carry.
type gateKind int

const (
	gateExec gateKind = iota
	gateRun
	gateTransfer
	gateExpose
	gateSS
	gateTransferV1 // capable callbacks join through the persistent worker fence
	gateKinds
)

// upgradeGate admits legacy work while open and refuses it while a cut is in
// progress — with the `try_again` code an N-1 client already classifies as
// transient, carrying `agent_upgrading` in the detail (S4 drill 36.L: a bare
// agent_upgrading made v0.6.0 clients exit 70). Its in-flight counts are also the data source
// of the legacy_session_busy and transfers_in_flight deferrals: a handler
// holds its admission for its whole life, so "checked busy-free, then
// someone started work" cannot happen once the gate is closed (§4.0).
type upgradeGate struct {
	mu              sync.Mutex
	closed          bool
	ptyBlocked      bool
	ptyUpgradeID    string
	flowBlocked     bool
	ssBlocked       bool
	domainUpgradeID string
	transferBlocked bool
	inflight        [gateKinds]int
	// Serialized by upgradeMu with watchdog marker transitions.
	rollbackLoggedAt time.Time
}

// admit reserves one in-flight slot of kind; ok=false means a cut is in
// progress and the caller must refuse with `try_again` + agent_upgrading detail. release must be
// called exactly once when the admitted work ends.
func (g *upgradeGate) admit(kind gateKind) (release func(), ok bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	// origin: hot-update S5 plan §5.2 / T36. Pending restrictions outlive
	// the transient cut and are checked before reserving any side effect.
	if g.closed || (kind == gateRun && g.ptyBlocked) ||
		(kind == gateExpose && g.flowBlocked) || (kind == gateSS && g.ssBlocked) ||
		(kind == gateTransferV1 && g.transferBlocked) {
		return nil, false
	}
	g.inflight[kind]++
	var once sync.Once
	return func() {
		once.Do(func() {
			g.mu.Lock()
			g.inflight[kind]--
			g.mu.Unlock()
		})
	}, true
}

// close stops admission; open resumes it. Both are idempotent.
func (g *upgradeGate) close() {
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
}

func (g *upgradeGate) open() {
	g.mu.Lock()
	g.closed = false
	g.mu.Unlock()
}

// count reports the admitted work of kind still in flight.
func (g *upgradeGate) count(kind gateKind) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inflight[kind]
}

func redispatchExec(prepared *handoff.Prepared, env []string, fallback func(string) error) func(string) error {
	return agentupgradeport.RedispatchExec(prepared, env, fallback)
}

// runFailedBeforeControlReady is Run's exit filter for a session error that
// ends the agent before its first register (W3, plan §3.5). It is the cut
// path's hook: a thawed imported image whose upgrade is still pending is
// re-dispatched to the previous binary with its CURRENT state instead of
// letting the deferred runtime Close kill the carried tasks.
func runFailedBeforeControlReady(ctx context.Context, a *Agent, err error) error {
	// origin: hot-update S3 plan §3.5. A stop signal must not boot prev
	// underneath systemd's pending stop/SIGKILL, nor undo session revocation.
	// session has already canceled its child context on return. Only Run's
	// parent can distinguish an operator stop from that routine teardown.
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, errSessionGone) ||
		(ctx != nil && ctx.Err() != nil) || !activeUpgradeImage(a) {
		return err
	}
	if a.cfg.Upgrade != nil {
		recoverAgentTransaction(ctx, a)
		return err
	}
	exePath, pathErr := a.upgradeExePath()
	if pathErr != nil {
		return err
	}
	a.upgradeMu.Lock()
	defer a.upgradeMu.Unlock()
	if lockErr := withUpgradeFileLock(exePath, true, func() error {
		rollbackBeforeControlReadyLocked(ctx, a, exePath)
		return nil
	}); lockErr != nil {
		a.cfg.Logger.Error("agent: run failure rollback: host-wide lock unavailable", "err", lockErr)
	}
	return err
}

func rollbackBeforeControlReadyLocked(ctx context.Context, a *Agent, exePath string) {
	agentupgrade.RecoverLegacyBeforeControlLocked(ctx, legacyAgentPort(a), exePath)
}

// busyChecks is shared by the read-only preflight and the cut. Admission
// stays closed across the latter, so an accepted legacy handler is either
// counted here or has already finished before Freeze starts.
// origin: hot-update S3 plan §4.0 / §4.1.
func busyChecks(a *Agent, deadlines ...time.Time) []agentimage.BusyCheck {
	// origin: hot-update S4 internal review rv-fd rv-fd-3
	// The caller's ownership precheck budget also covers the legacy predicates.
	ownershipDeadline := time.Now().Add(upgradebudget.SafePointWait)
	if len(deadlines) != 0 {
		ownershipDeadline = deadlines[0]
	}
	return []agentimage.BusyCheck{
		// Live S9 responsibilities have no image codec; every image entry,
		// including rollback watchdogs, uses this shared preflight predicate.
		// origin: hot-update S9 plan §7.3 / T50 / M25
		func() (string, string, bool) {
			return "infra_busy", "infrastructure movement pending", agentInfraPending(a)
		},
		func() (string, string, bool) {
			// Read admission before owners: a handler transferring responsibility
			// is then visible on at least one side of that handover. Reliable PTYs
			// belong exclusively to Runtime and never reserve legacy admission.
			// origin: hot-update S4 plan §7.1 / T29.
			n := a.gate.count(gateRun) + a.gate.count(gateExec)
			a.procsMu.Lock()
			n += len(a.procs)
			a.procsMu.Unlock()
			owned := map[string]bool{}
			if a.processRuntime != nil {
				pids, err := execsession.TaskPIDsUntil(a.processRuntime, ownershipDeadline)
				if err != nil {
					return agentimage.ReasonLegacySessionBusy, "legacy runtime ownership guard busy", true
				}
				for _, pid := range pids {
					owned[pid] = true
				}
			}
			rows, err := workload.SnapshotUntil(&a.workloads, ownershipDeadline)
			if err != nil {
				return agentimage.ReasonLegacySessionBusy, "legacy process ownership guard busy", true
			}
			for _, row := range rows {
				if !owned[row.ID] {
					n++
				}
			}
			return agentimage.ReasonLegacySessionBusy, fmt.Sprintf("%d legacy sessions or operations in flight", n), n > 0
		},
		func() (string, string, bool) {
			// origin: hot-update S5 plan §7.3 / T37. Unknown adapters and
			// retained legacy slots cannot become representable by deleting
			// the old flow_busy predicate. Read admission before the owner.
			// origin: hot-update S3 plan §4.0 / §4.1 (CC-4). Read admission
			// FIRST: with the gate closed, an opener is either still admitted
			// or has installed its session before releasing. Reversing these
			// reads can miss both sides of that handover.
			opening := a.gate.count(gateExpose)
			c := flowUpgradeCensus(a)
			busy := c.Unsupported || c.Legacy > 0 ||
				(c.Owned+c.Retained+c.Admission+c.Rehome > 0 && !flowImageOwnerReady(a)) ||
				(opening > c.Admission && a.cfg.ExposeAdapter != nil)
			return agentimage.ReasonLegacySessionBusy, "flow ownership is legacy, unknown, or not registered for the image", busy
		},
		func() (string, string, bool) {
			// origin: hot-update S5 plan §6.1 / §7.3 / T37. A listener or
			// salt history is retained responsibility even without an actor.
			err := checkSSImageState(a)
			if errors.Is(err, ssproxy.ErrCapacity) {
				return agentimage.ReasonImageTooLarge, "SS image capacity exceeded", true
			}
			return ssStateUnrepresentable, "SS state is not representable by the image owner", err != nil
		},
		func() (string, string, bool) {
			// origin: hot-update S5 plan §7.3 / T37. Typed v1 admission
			// belongs to its image owner; control-only expose still uses the
			// transfer gate, and unknown adapters remain legacy responsibility.
			n := a.gate.count(gateTransfer)
			if a.cfg.ExposeAdapter == nil {
				// An adapter-free expose only persists control-plane state; its
				// admission remains a transfers_in_flight deferral below.
				n += a.gate.count(gateExpose)
			}
			a.pushCacheMu.Lock()
			for _, entry := range a.pushCommitCache {
				if !pushCommitEntryExpired(entry, a.upgradeNow()) {
					n++
				}
			}
			a.pushCacheMu.Unlock()
			return agentimage.ReasonTransfersInFlight, fmt.Sprintf("%d transfers or expose operations in flight", n), n > 0
		},
		func() (string, string, bool) {
			a.homeReadMu.Lock()
			home := a.homeReadInFlight
			a.homeReadMu.Unlock()
			if home {
				return agentimage.ReasonRemoteFSProbeInFlight, "home state.json read in flight", true
			}
			if a.spawnPolicy != nil {
				c := a.spawnPolicy.Census()
				return agentimage.ReasonRemoteFSProbeInFlight, fmt.Sprintf("spawnsafe: probes in flight=%d, retained=%d", c.ProbesInFlight, c.ProbesRetained), c.ProbesInFlight > 0
			}
			return agentimage.ReasonRemoteFSProbeInFlight, "", false
		},
		func() (string, string, bool) {
			// An in-time spawn belongs to an admitted Runtime startup. The cut
			// quiesces and joins it; rejecting it here would starve upgrades on
			// busy nodes before that fence ever runs. Only the reaper's abandoned
			// ownership is an immediate imaging hazard.
			// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT (drill 36.A).
			n := 0
			if a.spawnPolicy != nil {
				n = a.spawnPolicy.Census().Wedged
			}
			return agentimage.ReasonSpawnInFlight, fmt.Sprintf("spawnsafe: abandoned=%d", n), n > 0
		},
		func() (string, string, bool) {
			closed := a.processRuntime == nil
			if !closed {
				err := execsession.CheckLiveUntil(a.processRuntime, ownershipDeadline)
				if err != nil && !errors.Is(err, execsession.ErrClosed) {
					return agentimage.ReasonSpawnInFlight, "runtime state guard busy", true
				}
				closed = errors.Is(err, execsession.ErrClosed)
			}
			return agentimage.ReasonRuntimeClosed, "runtime closed", closed
		},
		func() (string, string, bool) {
			return agentimage.ReasonPlatformUnsupported, "process image handoff requires Linux", runtime.GOOS != "linux"
		},
		func() (string, string, bool) {
			n := 0
			if a.processRuntime != nil {
				pids, err := execsession.TaskPIDsUntil(a.processRuntime, ownershipDeadline)
				if err != nil {
					return agentimage.ReasonSpawnInFlight, "runtime size guard busy", true
				}
				n = len(pids)
			}
			return agentimage.ReasonImageTooLarge, fmt.Sprintf("%d tasks exceed the descriptor bound", n), n*6 > 16384
		},
	}
}

func preflightDeferrals(a *Agent) []agentimage.DeferredError {
	return agentupgrade.PreflightDeferrals(busyChecks(a))
}

func transactionDeferrals(a *Agent) []agentimage.DeferredError {
	return agentupgradeport.RuntimeDeferrals(&a.cutMu, &a.workloads, func() []agentimage.DeferredError { return preflightDeferrals(a) })
}

func transactionSnapshot(a *Agent) (execsession.Image, agentimage.CourierState, error) {
	c := flowUpgradeCensus(a)
	return agentupgradeport.SnapshotRuntime(a.processRuntime, &a.cutMu, &a.workloads, c.Unsupported || c.Legacy+c.Admission+c.Rehome != 0 || checkSSImageState(a) != nil, func() agentimage.CourierState {
		if a.courier != nil {
			return exportCourierState(a.courier)
		}
		return agentimage.CourierState{}
	})
}

// The home probe is separate from the pure preflight, single-flight with
// reconnect reads, and bounded even if the spawn policy has a longer timeout.
func probeUpgradeHome(a *Agent) {
	if !a.homeHangable || a.stateStore == nil {
		return
	}
	a.homeReadMu.Lock()
	if a.homeReadInFlight {
		a.homeReadMu.Unlock()
		return
	}
	a.homeReadInFlight = true
	a.homeReadMu.Unlock()
	done := make(chan struct{})
	go func() {
		_, _ = a.stateStore.loadNoLock()
		a.homeReadMu.Lock()
		a.homeReadInFlight = false
		a.homeReadMu.Unlock()
		close(done)
	}()
	timer := time.NewTimer(upgradebudget.RequestLatency)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		if a.spawnPolicy != nil {
			a.spawnPolicy.InvalidateHealthy()
		}
	}
}

func imageOwners(a *Agent, deadline ...time.Time) agentimage.Owners {
	o := agentimage.Owners{Runtime: a.processRuntime, Manager: &a.workloads, Spawn: a.spawnPolicy,
		Busy: busyChecks(a, deadline...), Lineage: agentimage.Lineage{Generation: a.imageGeneration + 1, InstanceID: a.instanceID, RoutingNID: nidOf(a)}}
	if a.courier != nil {
		o.Courier = NewCourierImage(a)
	}
	// origin: hot-update S5 plan §5.1 / §7.3. The typed production owners
	// participate in the same cut; an absent optional interface is no proof.
	if adapter, ok := a.cfg.ExposeAdapter.(*TunnelExposeAdapter); ok && adapter.client != nil && !adapter.client.FlowCensus().Unsupported {
		o.Flow = adapter.client
	}
	if a.proxy != nil {
		a.proxy.mu.Lock()
		o.SS = a.proxy.srv
		a.proxy.mu.Unlock()
	}
	// Capable work never enters the legacy transfer count/cache. Its owner
	// retains prepared, terminal and courier state even with no active bytes.
	// origin: hot-update S7 plan §8.2–§8.4 / P21
	o.Transfers = transferImageWorker(a)
	return o
}

// transferImageWorker is the handler's bound continuation worker as the image
// owner, or nil when no production worker was ever built (the cut then
// carries no transfer domain). Its runtime resolves the current session's
// bindings at thaw, so no second owner reference or stale backend lives here.
// origin: hot-update S7 plan §8.1–§8.4 / P21 — agent image cut continuity
func transferImageWorker(a *Agent) agentimage.TransferWorker {
	if h := a.transfers.Loaded(); h != nil {
		return xferagentimage.Worker(h.Runtime)
	}
	return nil
}

// transactionTransfers is the S8 identity census of the manager
// transferImageWorker wraps (agentupgradeport.TransferCensus); a handler with
// no bound worker holds no capable transfer.
// origin: S8 FIX-DESCRIBE (transfer census)
func transactionTransfers(a *Agent) ([]agentupgradeport.TransferEntry, error) {
	var worker xfercontract.WorkerManager
	if h := a.transfers.Loaded(); h != nil && h.Runtime != nil {
		worker = h.Runtime.BoundWorker()
	}
	return agentupgradeport.TransferCensus(worker)
}

// withTransferCensus gives the runtime host (agentRuntimeHost) its capable
// transfer enumerator, beside the image owner it reads; without one the host
// refuses every census rather than reading the domain as empty.
// origin: S8 FIX-DESCRIBE (transfer census)
func withTransferCensus(a *Agent, host agentupgradeport.Host) agentupgradeport.Host {
	host.Transfers = func() ([]agentupgradeport.TransferEntry, error) { return transactionTransfers(a) }
	// origin: S8 internal review TAGGED-SUITE-SWEEP-A F2. Read the same typed
	// owners the cut freezes; an arbitrary adapter cannot supply an empty proof.
	host.Domains = func() (agentupgradeport.FlowSSState, error) {
		return agentupgradeport.CensusOwners(imageOwners(a), flowUpgradeCensus(a), hasSSUpgradeState(a))
	}
	return host
}

// Only new creation consults the pending-prev gate. P20 routes status,
// settlement and continuations straight to the retained manager; capability
// stays advertised so refusal cannot trigger a legacy fallback.
// origin: hot-update S7 plan §8.4 / P21
func admitTransferCreation(a *Agent) (func(), bool) { return a.gate.admit(gateTransferV1) }

func checkTransferUpgradeAdmission(a *Agent) error {
	a.gate.mu.Lock()
	defer a.gate.mu.Unlock()
	if a.gate.closed || a.gate.transferBlocked {
		return procrequest.ErrBusy
	}
	return nil
}

type imageCut struct {
	private    *sealedstate.Snapshot // cut-owned metadata, never a business descriptor
	frozen     *agentimage.Frozen
	tasks      int
	generation uint64
	// Admission follows the asynchronous exec through disk recovery; thawing
	// alone must not let another installer replace its selected executable.
	releaseAdmission func()
	// A legacy target gets no prepared descriptors, but keeping its empty
	// cut frozen prevents a reliable create between the final check and exec.
	legacy bool
	// Both actual readers must accept every re-export, including retained
	// metadata created after Freeze. The target hash is checked again at exec.
	readers []agentimage.Capabilities
	// The S8 upgrade-cut-v1 adjunct rides beside, never inside, the closed
	// image and the SS private metadata. origin: hot-update S8 plan §6.1
	adjunct []handoff.Resource
}

// On success ownership of cutMu transfers to cut.abandon or the exec.
// Locking in the exec goroutine would leave Freeze→reply unprotected.
// origin: hot-update S3 plan §2.5.
func freezeForUpgrade(a *Agent, deadline time.Time) (*imageCut, error) {
	// The direct cut also guards callers that bypass the read-only preflight.
	// origin: hot-update S9 plan §7.3 / T50
	if agentInfraPending(a) {
		return nil, errors.New("infra_busy")
	}
	// A direct cut targets the running reader; install and ReExecOnly replace
	// this with the capability tuple from their isolated candidate probe.
	runningSHA, err := a.runningImageSHA()
	if err != nil {
		return nil, fmt.Errorf("cannot hash running image for cut: %w", err)
	}
	// origin: hot-update S4 internal review rv-fd rv-fd-3
	// Activation recovery's outer cut guard shares the same absolute budget.
	for !a.cutMu.TryLock() {
		if !time.Now().Before(deadline) {
			return nil, os.ErrDeadlineExceeded
		}
		time.Sleep(min(time.Millisecond, time.Until(deadline)))
	}
	if !time.Now().Before(deadline) {
		a.cutMu.Unlock()
		return nil, os.ErrDeadlineExceeded
	}
	// Recheck under the cut barrier after its bounded lock wait.
	// origin: hot-update S9 plan §7.3 / T50
	if agentInfraPending(a) {
		a.cutMu.Unlock()
		return nil, errors.New("infra_busy")
	}
	if a.processRuntime == nil {
		a.cutMu.Unlock()
		return nil, &agentimage.DeferredError{Reason: agentimage.ReasonRuntimeClosed, Detail: "runtime closed"}
	}
	tableFrozen, guardErr := workload.FrozenUntil(&a.workloads, deadline)
	if guardErr != nil {
		a.cutMu.Unlock()
		return nil, guardErr
	}
	if tableFrozen {
		a.cutMu.Unlock()
		return nil, fmt.Errorf("agent: process table already frozen: %w", workload.ErrFrozen)
	}
	// origin: hot-update S5 internal review rv-authority-6. Capture readiness
	// before a planned carrier down edge can race the final private export.
	beginPlannedProxyReadiness(a)
	frozen, err := agentimage.Freeze(imageOwners(a, deadline), deadline)
	if err != nil {
		cancelAttachedProxyReadiness(a)
		// origin: hot-update S5 plan §5.4. Preserve capacity versus unknown
		// semantics without exposing private actor errors in a receipt.
		if errors.Is(err, ssproxy.ErrStateUnrepresentable) {
			err = &agentimage.DeferredError{Reason: ssStateUnrepresentable, Detail: "SS state is not representable"}
		} else if errors.Is(err, ssproxy.ErrCapacity) {
			err = &agentimage.DeferredError{Reason: agentimage.ReasonImageTooLarge, Detail: "SS image capacity exceeded"}
		}
		a.cutMu.Unlock()
		return nil, err
	}
	b := frozen.Bundle()
	return &imageCut{frozen: frozen, tasks: len(b.Runtime.Tasks), generation: b.Lineage.Generation, readers: []agentimage.Capabilities{agentimage.CurrentCapabilities(runningSHA)}}, nil
}

func (cut *imageCut) abandon(a *Agent) error {
	// The current connection may have been replaced while exec was attempted:
	// the transfer owner's thaw binds that current backend while still fenced,
	// before old actors can run again (xferagent resolves it at the call).
	// origin: hot-update S7 plan §8.2 / P21
	err := cut.frozen.Thaw()
	if cut.private != nil {
		err = errors.Join(err, cut.private.Close())
		cut.private = nil
	}
	if err == nil {
		// origin: hot-update S5 plan §5.4 / TestFlowSSExecFailureThawsBothDirections.
		// Thaw restores local pumps with SS authority pending. The current
		// owner must resume and obtain a fresh grant before admission reopens.
		err = resumeUpgradeImage(a, time.Now().Add(tunnel.ResumeAttempt))
	}
	if err != nil {
		a.cfg.Logger.Error("agent: image thaw failed", "err", err)
	}
	// Admission must not resume while either direction still owns a fence.
	// origin: hot-update S4 plan §4.2 / T33.
	if err == nil {
		a.gate.open()
	}
	a.cutMu.Unlock()
	return err
}

func publishHeartbeatOnce(a *Agent, nc *nats.Conn) {
	if nc == nil {
		return
	}
	_ = nc.Publish(proto.SubjNodeHeartbeat(a.cfg.SID, nidOf(a)), heartbeatPayload(a, time.Now()))
	_ = nc.FlushTimeout(upgradebudget.ReplyDrain)
}

// Runtime currently exposes tombstones/revocations only through Export.
// For a legacy candidate take a bounded temporary fence to prove emptiness;
// task counts alone would leak the revocation history to a bare exec.
func imageIsEmpty(a *Agent) bool {
	// origin: hot-update S5 plan §5.2 / T35. Idle/retired metadata and
	// salts forbid a bare exec just as live sockets do.
	if len(requiredUpgradeDomains(a)) > 1 || hasSSUpgradeState(a) || hasFlowUpgradeState(a) {
		return false
	}
	if a.courier != nil {
		a.courier.mu.Lock()
		n := len(a.courier.pending)
		a.courier.mu.Unlock()
		if n > 0 {
			return false
		}
	}
	if a.processRuntime == nil {
		return true
	}
	a.cutMu.Lock()
	defer a.cutMu.Unlock()
	if a.workloads.Frozen() {
		return false
	}
	if err := a.processRuntime.Freeze(time.Now().Add(upgradebudget.SafePointWait)); err != nil {
		return false
	}
	img, _, err := a.processRuntime.Export()
	thawErr := a.processRuntime.Thaw()
	return err == nil && thawErr == nil && agentimage.EmptyRuntimeImage(img)
}

var errImageABIUnsupported = agentimage.ErrABIUnsupported

func checkCandidateABI(a *Agent, candidate agentimage.Capabilities) error {
	return agentupgrade.CheckCandidateABI(a.cfg.Logger, func() bool { return imageIsEmpty(a) }, func() []string { return requiredUpgradeDomains(a) }, candidate)
}

func activeUpgradeImage(a *Agent) bool {
	return a.image != nil && a.image.Phase() == agentboot.PhaseActive
}

func ownsUpgradeImage(a *Agent) bool { return agentupgradeport.OwnsImage(a.image) }

// Legacy pending-marker rationale, now owned by agentupgrade/agentupgradeport.
// origin: hot-update S3 plan §2.1 / §2.6; concurrency verifier CC-V1.
// Expiry schedules recovery; it does not surrender a live owner's prev slot.
// A failed commit can leave ready + pending before the watchdog's first cut.

func cutFailureClass(err error) string { return agentupgradeport.CutFailureClass(err) }

type rollbackResult bool

const rollbackDone rollbackResult = false
const rollbackRetry rollbackResult = true

// Caller owns upgradeMu and the host flock. Every failure before exec
// preserves the current business image; a retry never falls back to bare exec.
func rollbackCurrentImage(a *Agent, exePath string, m *upgradeMarker, reason string, retry bool) rollbackResult {
	// origin: hot-update S4 plan §4.2 / §5.2.
	// A known busy owner cannot be cut, irrespective of the reader. Refuse it
	// before executing a legacy prev: its five-second probe would otherwise
	// hold upgradeMu/flock on every retry and delay the blocked-cut receipt.
	// Freeze still rechecks all owners after the probe to cover late admission.
	if reasons := preflightDeferrals(a); len(reasons) != 0 {
		return deferRollbackCut(a, exePath, m, &reasons[0], retry)
	}
	// Probe before freezing: a version subprocess must not spend the cut's
	// safe-point budget, and stale marker metadata must never authorize prev.
	previous, probeErr := previousImageCapabilities(exePath, m)
	if probeErr != nil {
		a.cfg.Logger.Error("agent: upgrade rollback reader unavailable", "err", probeErr)
		if retry {
			a.rollbackInFlight.Store(true)
			m.ResourcePhase, m.Detail = markerPhaseRollbackBlocked, "previous image unavailable"
			if err := writeUpgradeMarker(upgradeMarkerPath(exePath), m); err != nil {
				a.cfg.Logger.Error("agent: upgrade rollback blocked marker write failed", "err", err)
			}
			return rollbackRetry
		}
		return rollbackDone
	}
	a.gate.close()
	cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
	if err != nil {
		a.gate.open()
		return deferRollbackCut(a, exePath, m, err, retry)
	}
	rolledBack := false
	defer func() {
		// origin: hot-update S3 plan §2.6. A returning exec leaves ownership
		// here. Persist it after thaw under the caller's upgradeMu/flock,
		// preserving the rollback reason consumed by the deadline drills.
		if err := cut.abandon(a); err == nil && rolledBack {
			m.ResourcePhase = markerPhaseThawed
			if err := writeUpgradeMarker(upgradeMarkerPath(exePath), m); err != nil {
				a.cfg.Logger.Error("agent: upgrade rollback thaw marker write failed", "err", err)
			}
		}
	}()
	if err := agentimage.CheckBundleCapabilities(cut.frozen.Bundle(), previous); err != nil {
		a.cfg.Logger.Error("agent: upgrade rollback image domain refused", "err", err)
		if retry {
			a.rollbackInFlight.Store(true)
			m.ResourcePhase, m.Detail = markerPhaseRollbackBlocked, "freeze failed: "+cutFailureClass(err)
			if werr := writeUpgradeMarker(upgradeMarkerPath(exePath), m); werr != nil {
				a.cfg.Logger.Error("agent: upgrade rollback blocked marker write failed", "err", werr)
			}
			return rollbackRetry
		}
		return rollbackDone
	}
	if a.cfg.UpgradeCutObserver != nil {
		a.cfg.UpgradeCutObserver(cut.frozen.Bundle())
	}
	// origin: hot-update S4 plan §5.2 / T32. Rollback uses the current
	// owner's final re-export, not the image originally received at boot.
	prepared, err := prepareImageCut(a, cut, func(b agentimage.Bundle) error {
		if err := agentimage.VerifyCapabilitySHA(upgradePrevPath(exePath), previous); err != nil {
			return err
		}
		return agentimage.CheckBundleCapabilities(b, previous)
	})
	if err != nil {
		a.cfg.Logger.Error("agent: upgrade rollback prepare failed", "err", err)
		if retry {
			a.rollbackInFlight.Store(true)
			m.ResourcePhase, m.Detail = markerPhaseRollbackBlocked, "prepare failed: "+cutFailureClass(err)
			if werr := writeUpgradeMarker(upgradeMarkerPath(exePath), m); werr != nil {
				a.cfg.Logger.Error("agent: upgrade rollback blocked marker write failed", "err", werr)
			}
			return rollbackRetry
		}
		return rollbackDone
	}
	execsession.LogPTYImage(a.cfg.Logger, "cut", cut.generation, cut.frozen.Bundle().Runtime)
	defer func() { _ = prepared.Close() }()
	execFn := redispatchExec(prepared, execEnv(), a.upgradeExec)
	// The test hook substitutes only the final exec, never the cut/prepare.
	if a.cfg.UpgradeExecFn != nil {
		execFn = a.cfg.UpgradeExecFn
	} else if a.cfg.UpgradeNoExit {
		execFn = nil
	}
	// origin: hot-update S3 plan §5.4 / §0 exit 3. These receipts describe
	// the current-state cut being sent, not the generation we imported.
	m.Generation, m.ResourcePhase = cut.generation, markerPhaseRedispatched
	a.cfg.Logger.Info("agent: image cut", "generation", cut.generation, "tasks", cut.tasks,
		"courier_events", len(cut.frozen.Bundle().Courier.Events))
	if execFn != nil {
		dispatch := execFn
		execFn = func(path string) error {
			a.cfg.Logger.Info("agent: image re-dispatched", "image", fmt.Sprintf("instance=%s generation=%d tasks=%d", cut.frozen.Bundle().Instance, cut.generation, cut.tasks), "generation", cut.generation)
			return dispatch(path)
		}
	}
	executeRollback(exePath, m, reason, a.cfg.Logger, true, execFn)
	rolledBack = true
	a.rollbackInFlight.Store(false)
	return rollbackDone
}

// Both the read-only preflight and the final Freeze rejection retain the same
// pending transaction, retry admission and rate-limited blocked-cut diagnosis.
func deferRollbackCut(a *Agent, exePath string, m *upgradeMarker, err error, retry bool) rollbackResult {
	if !retry {
		return rollbackDone
	}
	detail := "freeze failed"
	var deferred *agentimage.DeferredError
	if errors.As(err, &deferred) {
		detail = deferred.Reason
	}
	m.ResourcePhase, m.Detail = markerPhaseRollbackBlocked, detail
	a.rollbackInFlight.Store(true)
	if werr := writeUpgradeMarker(upgradeMarkerPath(exePath), m); werr != nil {
		a.cfg.Logger.Error("agent: upgrade rollback blocked marker write failed", "err", werr)
	}
	now := a.upgradeNow()
	if a.gate.rollbackLoggedAt.IsZero() || now.Sub(a.gate.rollbackLoggedAt) >= time.Minute {
		a.gate.rollbackLoggedAt = now
		a.cfg.Logger.Error("agent: upgrade rollback blocked", "reason", detail)
	}
	return rollbackRetry
}

// Composition Thaw does not authorize SS. Each installed contributor must
// provide the corresponding current-owner recovery operation; no fallback
// to old keys is permitted on an exec failure or a failed prepare.
// origin: hot-update S5 plan §5.4 / §6.5.
var errUpgradeImageRecovery = errors.New("agent: current-owner image recovery failed")

func resumeUpgradeImage(a *Agent, deadline time.Time) error {
	if err := activateProxyTimer(a); err != nil {
		return errUpgradeImageRecovery
	}
	adapter, ok := a.cfg.ExposeAdapter.(*TunnelExposeAdapter)
	if !ok || adapter.client == nil || adapter.client.FlowCensus().Unsupported {
		return nil
	}
	if err := adapter.client.ResumeCarriers(deadline); err != nil {
		return errUpgradeImageRecovery
	}
	a.proxy.mu.Lock()
	server := a.proxy.srv
	a.proxy.mu.Unlock()
	if server == nil {
		return nil
	}
	state := &flowRestoreState{client: adapter.client, server: server}
	for _, authority := range tunnel.FlowAuthorities(adapter.client) {
		if err := authorizeRestoredProxy(a, state, authority); err != nil {
			return errUpgradeImageRecovery
		}
	}
	if !state.authorized.Load() {
		return errUpgradeImageRecovery
	}
	return nil
}

// Rehome control intent belongs to the image only when the typed owner has
// journaled it. The agent's independently admitted operations remain visible
// until that handover; losing the carrier does not discard their responsibility.
// origin: hot-update S5 plan §7.3 / T37.
func includeUpgradeRehomes(a *Agent, c tunnel.Census) tunnel.Census {
	a.rehomeMu.Lock()
	n := len(a.rehomeRunning)
	a.rehomeMu.Unlock()
	c.Legacy += max(0, n-c.Rehome)
	return c
}

// Re-export ordering and fired cleanup together with the domain cut. Replacing
// a Prepared or abandoning an exec closes only the cut-owned sealed metadata.
// origin: hot-update S5 plan §5.1 / §7.2 / T33
func prepareImageCut(a *Agent, cut *imageCut, check func(agentimage.Bundle) error) (*handoff.Prepared, error) {
	var extra []handoff.Resource
	if hasSSUpgradeState(a) {
		state, err := snapshotProxyCutUntil(a, time.Now().Add(upgradebudget.SafePointWait))
		if err != nil {
			return nil, err
		}
		data, err := encodeProxyCut(state)
		if err != nil {
			return nil, err
		}
		defer clear(data)
		next, err := sealedstate.New(proxyStateRef, data)
		if err != nil {
			return nil, err
		}
		if cut.private != nil {
			_ = cut.private.Close()
		}
		cut.private = next
		extra = next.Resources
	}
	return cut.frozen.PrepareChecked(func(bundle agentimage.Bundle) error {
		// The closed cut defines required domains even when no extra proxy state or
		// optional caller check exists. Both actual readers must accept this cut.
		// origin: hot-update S5 internal review rv-image-3 / rv-authority-5
		if len(cut.readers) == 0 {
			return errImageABIUnsupported
		}
		if err := agentimage.CheckBundleCapabilities(bundle, cut.readers...); err != nil {
			return err
		}
		// Retained apply/timer state can outlive Server. Its auxiliary sealed
		// descriptor still requires ss-v1 at the final reader gate.
		if len(extra) != 0 {
			for _, reader := range cut.readers {
				if err := agentimage.CheckDomains(reader, []string{"exec-v1", "ss-v1"}); err != nil {
					return err
				}
			}
		}
		if check != nil {
			if err := check(bundle); err != nil {
				return err
			}
		}
		// origin: hot-update S5 plan §5.2 / §9.1. The last re-export and
		// reader checks have completed; late revoke now targets this cut.
		if drillobserve.Hook != nil {
			observeImageDomains(bundle)
			drillobserve.Hook(drillobserve.Event{Kind: "final-cut", Domain: "agent", Values: [16]uint64{cut.generation}})
		}
		a.cfg.Logger.Info("agent: image final cut", "generation", cut.generation)
		return nil
	}, append(append([]handoff.Resource(nil), extra...), cut.adjunct...)...)
}

// transactionCut is one S8 cut of the complete current owner set. Its final
// re-export goes through the same checked funnel as every other dispatch.
// origin: hot-update S8 plan §5.1 / §6.1 / §11.2 P17; S8 FIX-AGENTWIRE
type transactionCut struct {
	a   *Agent
	cut *imageCut
}

func (c transactionCut) Bundle() agentimage.Bundle { return c.cut.frozen.Bundle() }
func (c transactionCut) Thaw() error               { return c.cut.abandon(c.a) }

// Prepare carries the adjunct beside the bundle. Late work admitted before the
// fence (revocation, retained terminal) is judged on this final re-export.
func (c transactionCut) Prepare(target string, adjunct handoff.Resource) (*handoff.Prepared, error) {
	if c.a.cfg.UpgradeCutObserver != nil {
		c.a.cfg.UpgradeCutObserver(c.cut.frozen.Bundle())
	}
	c.cut.adjunct = []handoff.Resource{adjunct}
	defer func() { c.cut.adjunct = nil }()
	readers := c.cut.readers
	prepared, err := prepareImageCut(c.a, c.cut, func(b agentimage.Bundle) error {
		if len(readers) != 2 {
			return errImageABIUnsupported
		}
		if err := agentimage.VerifyCapabilitySHA(target, readers[0]); err != nil {
			return err
		}
		return agentimage.CheckBundleCapabilities(b, readers...)
	})
	if err == nil {
		execsession.LogPTYImage(c.a.cfg.Logger, "cut", c.cut.generation, c.cut.frozen.Bundle().Runtime)
	}
	return prepared, err
}

// freezeTransaction closes admission and freezes; a refused freeze reopens the
// gate only when no other cut owns it (that cut keeps its own fence).
func freezeTransaction(a *Agent, readers []agentimage.Capabilities) (agentupgradeport.Image, error) {
	publishHeartbeatOnce(a, a.ncBox.Load())
	a.gate.close()
	cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
	if err != nil {
		if a.cutMu.TryLock() {
			a.gate.open()
			a.cutMu.Unlock()
		}
		return nil, err
	}
	cut.readers = readers
	return transactionCut{a: a, cut: cut}, nil
}

// transactionExec is replaceable only by tests that inspect the live Prepared.
var transactionExec = func(prepared *handoff.Prepared, path string, argv, env []string) error {
	return prepared.Exec(path, argv, env)
}

func execTransaction(a *Agent, path string, prepared *handoff.Prepared) error {
	if a.cfg.UpgradeExecFn != nil {
		return a.cfg.UpgradeExecFn(path)
	}
	if a.cfg.UpgradeNoExit {
		return errors.New("agent: upgrade exec disabled")
	}
	argv := append([]string(nil), os.Args...)
	argv[0] = path
	return transactionExec(prepared, path, argv, execEnv())
}

// Domain counts come from the final actual owner cut, not shared budget Used.
// FD fingerprints let the external keeper count remaining OFD duplicates live.
// origin: hot-update S5 plan §3.4 / §9.1 / DRILL-OBSERVER
func observeImageDomains(b agentimage.Bundle) {
	for _, event := range agentimage.DomainObservations(b) {
		drillobserve.Hook(event)
	}
}

func recoverAgentTransaction(ctx context.Context, a *Agent) {
	agentupgrade.RecoverCurrent(ctx, a.cfg.Upgrade, a.upgradeEndpoint, a.cfg.Logger)
}

func finishAgentControlReady(ctx context.Context, a *Agent, nc *nats.Conn, delivery *upgradeMarker) error {
	return agentupgrade.FinishControlReady(ctx, agentupgrade.ReadyPort{Control: agentControlPort(a), Endpoint: a.upgradeEndpoint,
		Captured: delivery, Legacy: legacyAgentPort(a), Ready: &a.controlReady, Image: a.image, RefreshAdmission: func() { refreshPTYAdmission(a) }}, nc)
}
