// bootstrap.go owns the interval between dispatch and control-ready. Until
// activation the image must either reach the previous binary or be explicitly
// abandoned; returning from a constructor must never silently close its pipes.
// origin: hot-update S3 plan §1 / §3.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/filetransfer"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/xferagent"
	"github.com/LinZiyang666/tether/internal/xferagentimage"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Session installation publishes only the interface under the same short lock
// as runCtx. The imported worker has its own lifetime, so both byte attachment
// lookup and terminal cleanup must take a snapshot before making NATS calls.
// origin: S7 internal review CUT-CAPACITY F6
func bindAgentTransferSession(a *Agent, nc *nats.Conn) error {
	// JetStream client for file-transfer Tier-B. `jetstream.New` is
	// pure client-side scaffolding (never errors), so we can stash
	// the handle eagerly. We DON'T probe with AccountInfo here —
	// the agent's perm template intentionally excludes
	// `$JS.API.INFO`, so a probe would log a spurious perm
	// violation on every agent start. Tier-B handlers surface a
	// real JS-absent error via the Put/Get path itself.
	js, _ := jetstream.New(nc)
	a.runCtxMu.Lock()
	a.js = js
	a.runCtxMu.Unlock()
	// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 Requests 1
	return xferagent.RebindConsumerCleanup(transferHandler(a).Runtime, js)
}

func snapshotAgentJetStream(a *Agent) jetstream.JetStream {
	a.runCtxMu.Lock()
	defer a.runCtxMu.Unlock()
	return a.js
}

// ImageRestoreError retains the cause for classification without rendering
// it: a rejected decoder input can contain the registry's authentication key.
type ImageRestoreError struct {
	Summary string
	Err     error
}

func (e *ImageRestoreError) Error() string { return "agent: image restore refused (" + e.Summary + ")" }
func (e *ImageRestoreError) Unwrap() error { return e.Err }

type imageActivationError struct{ err error }

func (e *imageActivationError) Error() string { return "agent: image thaw failed" }
func (e *imageActivationError) Unwrap() error { return e.err }

// openAndImportUpgrade opens the registered journal under this process's
// owner, then verifies a carried cut against it. cmd names the registered
// Key and journal (UpgradeDependencies.Open); only agent.New knows the
// lineage instance whose ProcessOwner issues grants, so the store opens here,
// exactly once. opened is what a constructor failure leaves for FinishBoot's
// redispatch. A cold start whose journal cannot open keeps business first:
// it runs without transaction capability (no endpoint, so register never
// advertises upgrade-tx-v1). An inherited image refuses instead, because its
// carried cut can only be verified against the journal that accepted it.
// origin: hot-update S8 plan §3.1 / §5.1 / §5.2; S8 FIX-DESCRIBE (P19 request
// R3; the cold/inherited split is taken from P19's OpenDeferredJournal)
func openAndImportUpgrade(a *Agent, img *agentboot.Image) (opened, imported *UpgradeDependencies, err error) {
	opened = a.cfg.Upgrade
	if opened != nil && opened.Store == nil && opened.Open != nil {
		var owner upgradetx.Owner
		var store upgradetx.Store
		if owner, err = agentupgrade.ProcessOwner(a.instanceID); err == nil {
			store, err = opened.Open(owner)
		}
		if err == nil && store == nil {
			err = errors.New("agent: upgrade journal opener returned no journal")
		}
		if err != nil && img != nil {
			return opened, nil, err
		}
		if err != nil {
			a.cfg.Logger.Error("agent: upgrade journal unavailable; transactions disabled for this run", "err", err)
			return nil, nil, nil
		}
		deps := *opened
		deps.Store, deps.Open = store, nil
		opened = &deps
	}
	imported, err = agentupgrade.ImportCut(img, opened, a.runningImageSHA)
	return opened, imported, err
}

func restoreImage(a *Agent, img *agentboot.Image) error {
	// origin: hot-update S5 plan §7.2. Restore the complete apply/timer pair
	// before any domain binds its server or any subscription can run.
	data, err := img.ReadSealed(proxyStateRef, proxyStateLimit)
	if err != nil {
		return &ImageRestoreError{Summary: img.Summary(), Err: err}
	}
	if data != nil {
		state, err := decodeProxyCut(data)
		clear(data)
		if err == nil {
			err = restoreProxyCut(a, state)
		}
		if err != nil {
			return &ImageRestoreError{Summary: img.Summary(), Err: err}
		}
	}
	target := agentimage.Target{
		Runtime: processRuntimeConfig(a), Spawn: a.spawnPolicy, Courier: NewCourierImage(a),
	}
	// origin: hot-update S5 plan §7.1 / T38. Restore needs the same node
	// budget and local-connect reservation as the initial typed owner.
	target.Flow = agentFlowConfig(a)
	// A sealed continuation worker becomes the handler's only worker before
	// activation; its thaw (within the component handover) binds the current
	// session. origin: hot-update S7 plan §8.1–§8.2 / P21
	target.Transfers = xferagentimage.Importer(&a.transfers, a.cfg.TransferWorker, func() *filetransfer.Handler { return transferShell(a) })
	if target.TransferDeadline, err = controlbudget.After(upgradebudget.ComponentHandover); err != nil {
		return &ImageRestoreError{Summary: img.Summary(), Err: err}
	}
	imp, err := img.Restore(target)
	if err != nil {
		return &ImageRestoreError{Summary: img.Summary(), Err: err}
	}
	adoptImageLineage(a, imp.Bundle.Lineage)
	a.processRuntime, a.image, a.imageGeneration = imp.Runtime, img, img.Generation()
	if imp.Flow == nil {
		if err := configureAgentFlows(a); err != nil {
			return &ImageRestoreError{Summary: img.Summary(), Err: err}
		}
	}
	if imp.Flow != nil || imp.SS != nil {
		if err := bindRestoredFlows(a, imp.Flow, imp.SS); err != nil {
			return &ImageRestoreError{Summary: img.Summary(), Err: err}
		}
	}
	a.cfg.Logger.Info("agent: image restored", "generation", img.Generation(), "tasks", img.Tasks(),
		"courier_events", len(imp.Bundle.Courier.Events), "spawn_verdicts", imp.SpawnVerdicts)
	return nil
}

// The sealed bundle wins a disagreement with the environment, but malformed
// lineage never changes routing. Resuming a lease must not claim the basename's
// process rows through previousNID (those may belong to a sibling clone).
// origin: hot-update S3 plan §1.7.
func adoptImageLineage(a *Agent, lineage agentimage.Lineage) {
	if lineage.InstanceID != "" {
		if len(lineage.InstanceID) != instanceIDLen || !isLowerBase32(lineage.InstanceID) {
			a.cfg.Logger.Warn("agent: malformed image instance lineage; keeping environment lineage")
		} else {
			if a.instanceID != "" && a.instanceID != lineage.InstanceID {
				a.cfg.Logger.Warn("agent: image instance lineage disagrees with environment; using image lineage")
			}
			a.instanceID = lineage.InstanceID
		}
	}
	if route := lineage.RoutingNID; route != "" {
		if route != a.cfg.NID && route != nidOf(a) && !acceptableLeaseName(a, route) {
			a.cfg.Logger.Warn("agent: malformed image routing lineage; keeping environment lineage")
		} else {
			if nidOf(a) != route {
				a.cfg.Logger.Warn("agent: image routing lineage disagrees with environment; using image lineage")
			}
			if route == a.cfg.NID {
				dropLease(a)
			} else {
				resumeRoutingNID(a, route)
			}
		}
	}
	setExecLineage(a.instanceID, nidOf(a))
}

// origin: hot-update S3 plan §3.2; S4 internal review rv-fd rv-fd-4.
// Run installs recovery before activation and still activates before network
// operations. A pristine failure leaves salvage for FinishBoot; partial active
// responsibility belongs to Run's current-state recovery and exit funnel.
func activateImage(a *Agent) error {
	if a.image == nil || a.image.Phase() == agentboot.PhaseReady {
		return nil
	}
	if a.image.Activated() && (a.flowState == nil || a.flowState.activationLogged) {
		return nil
	}
	var window, shim, imported time.Duration
	if !a.image.Activated() {
		var err error
		window, shim, imported, err = a.image.Activate()
		// origin: hot-update S8 plan §5.1 / M08; S8 FIX-AGENTWIRE. Once
		// activation committed, even partially, the commit owner holds the
		// inherited reservation; a pristine refusal leaves it with the image.
		agentupgrade.AdoptLease(a.image, a.cfg.Upgrade)
		if err != nil {
			return &imageActivationError{err: err}
		}
		if a.flowState != nil {
			a.flowState.shim, a.flowState.imported = shim, imported
		}
	}
	// origin: hot-update S5 plan §6.5 / §7.2. Expiry runs before RESUME
	// authority; a nil scheduler handle cannot erase fired cleanup.
	if err := activateProxyTimer(a); err != nil {
		return &imageActivationError{err: err}
	}
	// origin: hot-update S5 plan §7.1 / T38 — raw-active is already an
	// ownership boundary. A failed RESUME retains this owner for current-cut
	// recovery; it cannot return to the constructor's pristine bundle.
	if err := activateRestoredFlows(a, time.Now().Add(tunnel.DetachedGrace)); err != nil {
		return &imageActivationError{err: err}
	}
	if a.flowState != nil {
		shim, imported = a.flowState.shim, a.flowState.imported
		window = a.image.Imported().FreezeWindow()
		a.flowState.activationLogged = true
	}
	a.processRuntime.LogPTYActivation(a.imageGeneration)
	stampMarkerPhase(a, markerPhaseActive, a.image.Tasks(), window)
	// One line per generation, with a stable shape for drills and calibration.
	a.cfg.Logger.Info(fmt.Sprintf("agent: image activated generation=%d tasks=%d freeze_window_ms=%d shim_ms=%d import_ms=%d",
		a.imageGeneration, a.image.Tasks(), window.Milliseconds(), shim.Milliseconds(), imported.Milliseconds()))
	return nil
}

// activateRunImage keeps exit responsibility throughout activation. A transient
// error gets three bounded attempts; an active failure retains its owners and
// enters the watchdog's current-cut/rollback_blocked transaction. Only explicit
// cancellation can end that responsibility while directions remain unfinished.
// origin: hot-update S4 internal review rv-fd rv-fd-4
func activateRunImage(ctx context.Context, a *Agent) error {
	for {
		var activationErr error
		for attempt := 0; attempt < 3; attempt++ {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// The watchdog's cut cannot overlap an activation attempt.
			a.cutMu.Lock()
			activationErr = activateImage(a)
			a.cutMu.Unlock()
			if activationErr == nil {
				return nil
			}
			if !activeUpgradeImage(a) {
				return activationErr // pristine ownership belongs to FinishBoot
			}
		}
		a.cfg.Logger.Error("agent: partial image activation retained for current-state recovery", "reason", errClass(activationErr))
		exe, err := a.upgradeExePath()
		if err == nil {
			// This shares upgradeMu/flock and the exact current-state transaction
			// with the deadline watchdog, including deferred-cut retry receipts.
			a.watchdogRollback(exe)
		}
		timer := time.NewTimer(upgradebudget.RollbackCutRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// A pristine import is detached by FinishBoot; active ownership is cleaned up
// only after Run has discharged recovery or its operator cancelled the loop.
func stopBootProcessRuntime(ctx context.Context, a *Agent) {
	if a.image != nil && !ownsUpgradeImage(a) {
		return
	}
	stopProcessRuntime(ctx, a)
}

// Marker I/O happens after thaw, outside the measured freeze window, and under
// the same host lock as install/rollback so a sibling cannot be overwritten.
func stampMarkerPhase(a *Agent, phase string, tasks int, window time.Duration) {
	exe, err := a.upgradeExePath()
	if err != nil {
		return
	}
	if err := withUpgradeFileLock(exe, true, func() error {
		m, err := readUpgradeMarker(upgradeMarkerPath(exe))
		if err != nil {
			return err
		}
		if m == nil || m.State != upgradeStatePending || !a.markerTargetsThisAgent(m) {
			return nil
		}
		m.ResourcePhase, m.ImportedResources, m.FreezeWindowMS = phase, tasks, window.Milliseconds()
		if a.image != nil {
			m.Generation = a.imageGeneration
		}
		return writeUpgradeMarker(upgradeMarkerPath(exe), m)
	}); err != nil {
		a.cfg.Logger.Warn("agent: image phase marker unavailable", "phase", phase, "err", err)
	}
}

func redispatchEnv(img *agentboot.Image) []string {
	env := img.Env()
	return execEnvFor(env[instanceIDEnv], env[routingNIDEnv])
}

// bootRedispatchExec is injectable separately from bare exec: tests inspect the
// Prepared while it is live and force exec failure without replacing themselves.
var bootRedispatchExec = redispatchExec

// FinishBoot is main's exit funnel, including Cobra and constructor failures.
// Active images are handled by Run's current-state cut; ready/lost images have
// already discharged bootstrap ownership.
func FinishBoot(runErr error) {
	img := agentboot.Current()
	if img == nil {
		return
	}
	switch img.Phase() {
	case agentboot.PhaseNone, agentboot.PhaseReady, agentboot.PhaseLost, agentboot.PhaseActive:
		return
	case agentboot.PhaseHeld, agentboot.PhaseRefused, agentboot.PhaseImported:
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// origin: hot-update S8 plan §3.3 / §8.1; S8 FIX-AGENTWIRE. The v1 boot
	// recovery refuses a v2 journal and would abandon the carried tasks; an S8
	// dispatch, imported or not, takes the transaction's own pristine return.
	if state, carried := img.CarriedUpgradeCut(); carried {
		running, _ := agentupgrade.RunningImageSHA("", os.Executable)
		finishTransactionBoot(img, state, logger, runErr, running)
		return
	}
	exe, err := os.Executable()
	if err != nil {
		img.Abandon(logger, agentboot.CrashWindowNoRollbackTarget, errClass(runErr))
		return
	}
	finishBoot(img, strings.TrimSuffix(exe, " (deleted)"), logger, runErr, realExec)
}

func finishBoot(img *agentboot.Image, exe string, logger *slog.Logger, runErr error, execFn func(string) error) {
	upgradeStore().FinishBoot(img, exe, logger, func() string { return errClass(runErr) }, ownsPendingBootMarker, bootImageRedispatch, execFn)
}

// bootTransactionPorts is the configured journal FinishBoot needs after a
// constructor failure, when main holds no Agent. New records it first.
var bootTransactionPorts atomic.Pointer[UpgradeDependencies]

func finishTransactionBoot(img *agentboot.Image, state *upgradecut.State, logger *slog.Logger, runErr error, running string) {
	agentupgrade.FinishTransactionBoot(img, state, bootTransactionPorts.Load(), running, logger, errClass(runErr), func(p *handoff.Prepared) func(string) error {
		return bootImageRedispatch(p, img, func(string) error { return errors.New("agent: transaction redispatch has no bundle") })
	})
}

func bootImageRedispatch(prepared *handoff.Prepared, img *agentboot.Image, fallback func(string) error) func(string) error {
	return bootRedispatchExec(prepared, redispatchEnv(img), fallback)
}

// origin: hot-update S3 plan §1.7. A host-scoped marker alone is insufficient:
// a sibling may share the binary but must never roll back another transaction.
func ownsPendingBootMarker(m *upgradeMarker, img *agentboot.Image) bool {
	return m != nil && m.State == upgradeStatePending &&
		((m.UpgradeID != "" && m.UpgradeID == currentUpgradeBootProof()) ||
			(m.TargetInstance != "" && m.TargetInstance == img.Env()[instanceIDEnv]))
}

// Only a vocabulary word crosses into the marker; error text can contain YAML,
// command arguments or untrusted bundle data. Unrecognized errors are "new".
func errClass(err error) string {
	var restore *ImageRestoreError
	var activate *imageActivationError
	switch {
	case errors.As(err, &restore):
		return "bundle_rejected"
	case errors.As(err, &activate):
		return "thaw_failed"
	case err == nil:
		return "new"
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "unknown flag"), strings.Contains(msg, "unknown command"),
		strings.Contains(msg, "required flag"), strings.Contains(msg, "invalid argument"), strings.Contains(msg, "accepts "):
		return "cobra"
	case strings.Contains(msg, "identity"), strings.Contains(msg, "nkey"):
		return "identity"
	case strings.Contains(msg, "lineage"):
		return "lineage"
	case strings.Contains(msg, "config"), strings.Contains(msg, "yaml"), strings.Contains(msg, "log-level"), strings.Contains(msg, "remote_fs"):
		return "config"
	default:
		return "new"
	}
}

// refreshPTYAdmission runs before subscriptions and after the durable commit
// attempt. A successful register alone is insufficient: a failed marker write
// leaves rollback armed and must leave terminal creation closed.
// origin: hot-update S4 plan §2.2 / §7.1
func refreshPTYAdmission(a *Agent) {
	// One SHA-bound gate serves reliable create and legacy run. Re-reading a
	// visible committed rename must not release it after directory fsync failed;
	// only commitUpgradeAfterRegister's durable write owns that release.
	// origin: hot-update S4 plan §5.2 / T31.
	if err := initPTYUpgradeAdmission(a); err != nil {
		a.cfg.Logger.Error("agent: pending PTY admission initialization failed", "err", err)
	}
}

func checkPTYAdmission(a *Agent) error {
	return checkPTYUpgradeAdmission(a)
}

type flowRestoreState struct {
	client           *tunnel.Client
	server           *ssproxy.Server
	runCtx           context.Context // installed by Run, never inherited from a NATS session
	rawActive        bool
	resumed          bool
	activationLogged bool
	shim, imported   time.Duration
	resumeDeadline   time.Time
	closeFlows       func() error
	ssRawActive      bool
	authorized       atomic.Bool
}

// Binding swaps a dormant wrapper, never re-opens the original listener or
// reconstructs a business socket. A custom adapter cannot claim these owners.
// origin: hot-update S5 plan §7.1 / T38
func bindRestoredFlows(a *Agent, client *tunnel.Client, server *ssproxy.Server) error {
	if client == nil {
		return errors.New("agent: restored flow client missing")
	}
	if a.flowState != nil {
		return errors.New("agent: duplicate restored flow binding")
	}
	adapter, ok := a.cfg.ExposeAdapter.(*TunnelExposeAdapter)
	if !ok {
		return errors.New("agent: restored flow adapter unavailable")
	}
	state := &flowRestoreState{client: client, server: server}
	// Client.Start owns the whole endpoint/manager shutdown on Run cancellation.
	// Reuse that lifetime owner; a nonexistent optional CloseFlows interface
	// previously refused every real import before it could activate.
	state.closeFlows = func() error {
		// A Runtime failure may precede carrier activation. The same lifetime
		// closer must retire even the dormant imported flow manager on exit.
		if state.runCtx != nil {
			startExposeAdapter(state.runCtx, a)
		}
		stopExposeAdapter(a)
		return nil
	}
	if server != nil {
		if err := wireProxyFlowRevoke(a, server); err != nil {
			return err
		}
	}
	if err := adapter.adoptRestoredClient(client); err != nil {
		return err
	}
	client.SetSessionStateHook(a.onTunnelSessionState)
	a.flowState = state
	if server != nil {
		a.proxy.srv = server // constructor: no callbacks or workers exist yet
	}
	return nil
}

// Only a complete fresh grant for this restored allocation opens the actor
// gate. Expose allocations on the same Client cannot authorize the SS owner.
// origin: hot-update S5 plan §4.3 / §6.5 / T38
func authorizeRestoredProxy(a *Agent, state *flowRestoreState, authority tunnel.Authority) error {
	// RESUME and NATS apply reserve the same ownership receipt, then do SS
	// cleanup without holding the proxy table lock. Only success publishes the
	// new pair/digest; a rejected stale grant leaves current traffic untouched.
	// origin: hot-update S5 internal review rv-authority-4 / rv-ss-5
	release := beginProxyApply(a)
	defer release()
	a.proxy.mu.Lock()
	port := a.proxy.publicPort
	a.proxy.mu.Unlock()
	if authority.Allocation.Port != port {
		return nil
	}
	if port == 0 || authority.Allocation.SID != a.cfg.SID || authority.Allocation.NID != nidOf(a) {
		return ssproxy.ErrAuthorityPending
	}
	keys := make([]ssproxy.Key, len(authority.Keys))
	for i, key := range authority.Keys {
		keys[i] = ssproxy.Key{KeyID: key.ID, Secret: string(key.Secret)}
	}
	grant := ssproxy.Grant{Enabled: authority.Enabled, Generation: authority.Generation,
		Epoch: authority.ProxyEpoch, AppliedRevision: authority.AppliedRevision, Keys: keys}
	var err error
	if a.image != nil && a.image.Imported() != nil && a.image.Imported().SS == state.server {
		err = a.image.Imported().AuthorizeSS(grant)
	} else {
		err = state.server.Authorize(grant)
	}
	if err == nil {
		directive := &proto.ProxyDirective{Enabled: authority.Enabled, Generation: authority.Generation, Epoch: authority.ProxyEpoch}
		for _, key := range keys {
			directive.Keys = append(directive.Keys, proto.ProxyKey{SubID: key.KeyID, Secret: key.Secret})
		}
		digest, digestErr := proxyDirectiveDigest(directive)
		if digestErr != nil {
			return digestErr
		}
		a.proxy.mu.Lock()
		a.proxy.gen, a.proxy.epoch = authority.Generation, authority.ProxyEpoch
		a.proxy.keysetDigest, a.proxy.digestApplied = digest, true
		a.proxy.authoritativelyOff = !authority.Enabled
		a.proxy.mu.Unlock()
		if !authority.Enabled {
			// Fresh RESUME revocation ends planned readiness immediately, just
			// like the versioned push entry point; grace cannot conceal it.
			// origin: hot-update S5 internal review rv-authority-6
			clearPlannedProxyReadiness(a)
			a.proxyTunnelUp.Store(false)
			if nc := a.ncBox.Load(); nc != nil {
				a.pubProxyReady(nc, false)
			}
		}
		state.authorized.Store(authority.Enabled)
	}
	return err
}

// Activation is retriable, but never creates a second raw pump or resets the
// detach deadline. SS remains AuthorityPending while TLS/REGISTER/RESUME runs.
// origin: hot-update S5 plan §7.1 / T38
func activateRestoredFlows(a *Agent, deadline time.Time) error {
	s := a.flowState
	if s == nil || s.resumed {
		return nil
	}
	if s.runCtx == nil || s.runCtx.Err() != nil {
		return errors.New("agent: flow activation outside Run")
	}
	// The retry path must preserve its first absolute deadline as well as the
	// Endpoint's imported detach deadline; a new activation call buys no time.
	if s.resumeDeadline.IsZero() || deadline.Before(s.resumeDeadline) {
		s.resumeDeadline = deadline
	}
	deadline = s.resumeDeadline
	if !s.rawActive {
		if s.server != nil && !s.ssRawActive {
			if err := s.server.ActivateRaw(); err != nil {
				return err
			}
			s.ssRawActive = true
		}
		if err := s.client.ActivateRaw(); err != nil {
			return err
		}
		s.rawActive = true
	}
	startExposeAdapter(s.runCtx, a)
	// Composition records carrier/authority completion on the same BOOTTIME
	// used by its freeze window. Fresh grants are copied only after verified
	// RESUME; sealed old keys cannot open the actor gate.
	if err := a.image.Imported().ResumeCarriers(deadline); err != nil {
		return err
	}
	if s.server != nil {
		for _, authority := range tunnel.FlowAuthorities(s.client) {
			if err := authorizeRestoredProxy(a, s, authority); err != nil {
				return err
			}
		}
		if !s.authorized.Load() {
			return ssproxy.ErrAuthorityPending
		}
	}
	s.resumed = true
	return nil
}

const flowResumeWorkers = 32

// Each operation owns one Endpoint and includes TLS/REGISTER/RESUME. Workers
// join even after a failure; a failed authorization must not orphan a dial or
// keep a global lock held over healthy endpoints. The absolute cut deadline is
// never renewed by retry or by moving from the active cohort to idle owners.
// origin: hot-update S5 plan §7.1 / T38
func resumeFlowOperations(ctx context.Context, active, idle []func(time.Time) error, deadline time.Time) error {
	var allErrors []error
	for _, cohort := range [][]func(time.Time) error{active, idle} {
		var wg sync.WaitGroup
		var mu sync.Mutex
		jobs := make(chan func(time.Time) error)
		for worker := 0; worker < flowResumeWorkers; worker++ {
			wg.Go(func() {
				for operation := range jobs {
					err := resumeFlowOperation(ctx, operation, deadline)
					if err != nil {
						mu.Lock()
						allErrors = append(allErrors, err)
						mu.Unlock()
					}
				}
			})
		}
		for _, operation := range cohort {
			jobs <- operation
		}
		close(jobs)
		wg.Wait()
	}
	return errors.Join(allErrors...)
}

func resumeFlowOperation(ctx context.Context, operation func(time.Time) error, deadline time.Time) error {
	if operation == nil {
		return errors.New("agent: missing carrier resume operation")
	}
	backoff := 250 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		attempt := time.Now().Add(tunnel.ResumeAttempt)
		if deadline.Before(attempt) {
			attempt = deadline
		}
		if err := operation(attempt); err == nil {
			return nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		wait := backoff
		if remaining < wait {
			wait = remaining
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
		if backoff > time.Second {
			backoff = time.Second
		}
	}
}

func stopRestoredFlows(a *Agent) {
	if s := a.flowState; s != nil {
		if s.server != nil {
			_ = s.server.RevokeAll("agent_exit")
		}
		if s.closeFlows != nil {
			_ = s.closeFlows()
		}
	}
}

func startExposeAdapter(ctx context.Context, a *Agent) {
	if adapter, ok := a.cfg.ExposeAdapter.(interface{ Start(context.Context) }); ok {
		adapter.Start(ctx)
	}
}

// origin: hot-update S5 plan §3.4 / §7.1 / §7.4
// Linux advertises v1 only after constructing a complete owner. Darwin keeps
// the existing legacy transport and never enters the Linux image API.
func configureAgentFlows(a *Agent) error {
	adapter, ok := a.cfg.ExposeAdapter.(*TunnelExposeAdapter)
	if !ok || runtime.GOOS != "linux" {
		return nil
	}
	// A fresh v1 owner starts at generation one; its first cut hands off to two.
	a.imageGeneration = max(a.imageGeneration, 1)
	return adapter.client.SetFlowConfig(agentFlowConfig(a))
}

func agentFlowConfig(a *Agent) tunnel.FlowConfig {
	cfg := processRuntimeConfig(a)
	boot, _ := procrequest.CurrentBootID()
	return tunnel.FlowConfig{Budget: cfg.SharedReplayBudget, InstanceID: a.instanceID,
		OwnerGen: max(a.imageGeneration, 1), BootID: boot, MaxEndpoints: tunnel.MaxEndpoints,
		MaxPendingFDs: flow.MaxPendingFDs, BeforeLocalConnect: func(id flow.ID, tuple rawsocket.Tuple) error {
			return expectProxyFlow(a, id, tuple)
		}}
}
