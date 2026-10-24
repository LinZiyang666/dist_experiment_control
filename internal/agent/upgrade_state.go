// Upgrade-safety state machine — the agent side of `tether node upgrade`'s
// commit/rollback protocol (upgrade-safety plan §3, architecture §21.3).
//
// The old pipeline was a one-way door: syscall.Exec replaces the process image,
// and if the new binary is broken the supervisor loops the SAME broken binary
// forever — behind NAT that means the agent is gone until someone gets
// physical access. This file adds the pieces that make the door two-way:
//
//   - a marker file (.tether-upgrade.json, next to the binary) recording the
//     in-flight upgrade: prev/new sha, versions, an absolute register
//     deadline, and a boot budget;
//   - a prev slot (<binary>.prev, hardlinked before the flip) holding the
//     last known-good binary;
//   - decideBoot, a pure function run at agent-daemon boot ONLY (`tether
//     agent` — never `version` or any other subcommand, or the smoke gate's
//     own `version` probe would burn boot budget);
//   - a register-deadline watchdog armed while the marker is pending;
//   - control-ready commit: the first successful register after the re-exec
//     captures its report; subscriptions, reconciliation, Flush and a fresh
//     nonce request must complete before pending can become committed.
//
// Commit and rollback are mutually exclusive: every marker transition after
// boot goes through Agent.upgradeMu, so a register racing the watchdog cannot
// commit after the rollback started, and a fired watchdog is a no-op once the
// marker is committed.
package agent

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/binaryupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeport"
)

const (
	// upgradeBootBudget is how many times the NEW binary may be (re)started
	// while pending before boot rolls back. It — not the register deadline —
	// is what closes the crash-loop failure mode: this repo's units restart
	// every 2s/5s, which never trips systemd's default start limit
	// (10s window / 5 starts), so an instant-crash binary would otherwise
	// loop right through the deadline. Budget 3 converges in ~15s. The
	// number lives in upgradebudget (hot-update S3): one source for the
	// agent, the ctl's wait budget and the deploy-tier drills.
	upgradeBootBudget = upgradebudget.BootBudget

	// upgradeRegisterDeadline bounds pending: the new binary must complete a
	// register within this window or the watchdog restores the prev slot. A
	// constant, not a flag (plan §0: no tunables without a use case). On a
	// weak NAT link an honest binary can lose the race — the rollback is
	// harmless (node stays online on the old version; re-run the upgrade).
	// It is the agent's ComponentHandover budget (upgradebudget, S3).
	upgradeRegisterDeadline = upgradebudget.ComponentHandover
)

// The agent retains its marker vocabulary while the disk transaction lives below it.
// origin: hot-update S4 plan §10.
type upgradeMarker = binaryupgrade.Marker

const (
	upgradeStatePending        = binaryupgrade.StatePending
	upgradeStateCommitted      = binaryupgrade.StateCommitted
	upgradeStateRolledBack     = binaryupgrade.StateRolledBack
	upgradeStateRollbackFailed = binaryupgrade.StateRollbackFailed
	markerPhaseStaged          = binaryupgrade.PhaseStaged
	markerPhaseActive          = binaryupgrade.PhaseActive
	markerPhaseRedispatched    = binaryupgrade.PhaseRedispatched
	markerPhaseThawed          = binaryupgrade.PhaseThawed
	markerPhaseDeferred        = binaryupgrade.PhaseDeferred
	markerPhaseLost            = binaryupgrade.PhaseLost
	markerPhaseRollbackBlocked = binaryupgrade.PhaseRollbackBlocked
)

func upgradeMarkerPath(exePath string) string { return binaryupgrade.MarkerPath(exePath) }
func upgradePrevPath(exePath string) string   { return binaryupgrade.PrevPath(exePath) }
func upgradeLockPath(exePath string) string   { return binaryupgrade.LockPath(exePath) }
func withUpgradeFileLock(exePath string, blocking bool, fn func() error) error {
	return binaryupgrade.WithFileLock(exePath, blocking, fn)
}

var errUpgradeLockBusy = binaryupgrade.ErrLockBusy

func upgradeStore() binaryupgrade.Store { return binaryupgrade.Store{BeforeSync: upgradeSyncObserver} }
func syncFile(f *os.File) error         { return upgradeStore().SyncFile(f) }

func readUpgradeMarker(path string) (*upgradeMarker, error) {
	return agentupgrade.ReadLegacyMarker(path)
}
func writeUpgradeMarker(path string, m *upgradeMarker) error {
	return agentupgrade.WriteLegacyMarker(upgradeStore(), path, m)
}
func executeRollback(exePath string, m *upgradeMarker, reason string, logger *slog.Logger, reexec bool, execFn func(string) error) {
	agentupgrade.ExecuteLegacyRollback(upgradeStore(), exePath, m, reason, logger, reexec, execFn)
}

func rememberUpgradeBootProof(id string) { agentupgrade.RememberBootProof(id) }
func currentUpgradeBootProof() string    { return agentupgrade.CurrentBootProof() }

// upgradeSyncObserver (external review F6) is the injection point for the
// ordered-durability tests: nil in production. When set, it is called at
// every fsync point BEFORE the real sync; a non-nil return simulates that
// sync failing (the power-cut/error matrix), and the real sync is skipped.
var upgradeSyncObserver func(kind, path string) error

// realExec is the production execFn: replace this process image in place
// (same PID — supervisors never see a death).
func realExec(exePath string) error {
	argv := append([]string(nil), os.Args...)
	argv[0] = exePath
	// execEnv, not os.Environ — see the upgrade.go re-exec site. Both rollback
	// paths run through here, and a rollback that renamed the node would be
	// even harder to reason about than an upgrade that did.
	return syscall.Exec(exePath, argv, execEnv())
}

// BootUpgradeCheck runs the boot-time half of the upgrade state machine. It
// MUST be called from the `tether agent` daemon path only — before Agent.Run,
// and never from any other subcommand (the smoke gate probes the staged
// binary with `version`; if that probe consumed boot budget the gate would
// sabotage itself). origin: upgrade-safety plan §3.1 (定稿修订 R1).
func BootUpgradeCheck(logger *slog.Logger) {
	exePath, err := os.Executable()
	if err != nil {
		logger.Warn("agent: boot upgrade check skipped: cannot resolve own executable", "err", err)
		return
	}
	// After a rename-replace the running inode is gone and os.Executable can
	// report a " (deleted)" suffix (same trim as handleReExecOnly).
	exePath = strings.TrimSuffix(exePath, " (deleted)")
	rememberUpgradeBootProof(bootUpgradeCheck(exePath, logger, time.Now(), realExec))
}

// bootUpgradeCheck is the hook-injectable body of BootUpgradeCheck.
//
// Identity-free BY NECESSITY (external review F2 moved this before Cobra
// parsing, so sid/nid are not yet known) — and identity-free is SOUND here:
// on a shared-binary host, every process launched after the flip runs the
// staged bytes, so any boot of the staged image is evidence about that
// image, and the boot budget deliberately counts HOST boots of it
// (documented in architecture §21.3). The self==prev converge and the
// budget/deadline rollback both act on host-level facts (which bytes are at
// dst / how often they crashed), never on instance identity.
func bootUpgradeCheck(exePath string, logger *slog.Logger, now time.Time, execFn func(string) error) string {
	var proof string
	if err := withUpgradeFileLock(exePath, true, func() error {
		proof = bootUpgradeCheckLocked(exePath, logger, now, agentboot.Current(), execFn)
		return nil
	}); err != nil {
		logger.Warn("agent: boot upgrade check skipped: host-wide upgrade lock unavailable", "err", err)
		return ""
	}
	return proof
}

func bootUpgradeCheckLocked(exePath string, logger *slog.Logger, now time.Time, img *agentboot.Image, execFn func(string) error) string {
	return agentupgrade.CheckLegacyBoot(upgradeStore(), exePath, logger, now, img, bootImageRedispatch, execFn)
}

// upgradeExePath resolves the binary path the upgrade machinery operates on
// (Config override for tests, os.Executable in production).
func (a *Agent) upgradeExePath() (string, error) {
	if a.cfg.UpgradeExecutablePath != "" {
		return a.cfg.UpgradeExecutablePath, nil
	}
	p, err := os.Executable()
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(p, " (deleted)"), nil
}

func (a *Agent) runningImageSHA() (string, error) {
	return agentupgrade.RunningImageSHA(a.cfg.UpgradeRunningImagePath, a.upgradeExePath)
}

func (a *Agent) markerTargetsThisAgent(m *upgradeMarker) bool {
	return agentupgrade.TargetsLegacy(legacyAgentPort(a), m)
}

func (a *Agent) upgradeNow() time.Time {
	if a.cfg.UpgradeNowFn != nil {
		return a.cfg.UpgradeNowFn()
	}
	return time.Now()
}

func (a *Agent) upgradeExec(exePath string) error {
	if a.cfg.UpgradeExecFn != nil {
		return a.cfg.UpgradeExecFn(exePath)
	}
	return realExec(exePath)
}

// armUpgradeWatchdog starts the register-deadline watchdog when a pending
// marker exists. Called once per Run; the goroutine exits on ctx cancel (leak
// gate) or on firing. commitUpgradeAfterRegister cancels it via upgradeWatch.
func (a *Agent) armUpgradeWatchdog(ctx context.Context) {
	if armTransactionWatchdog(ctx, a) {
		return
	}
	// Close before Run opens any subscription, even on a cold pending boot.
	// P8 also wires this initializer into New before callers can admit work.
	// origin: hot-update S4 plan §5.2 / T31.
	if err := initPTYUpgradeAdmission(a); err != nil {
		a.cfg.Logger.Error("agent: pending PTY admission remains closed", "err", err)
	}

	exePath, err := a.upgradeExePath()
	if err != nil {
		return
	}
	m, merr := readUpgradeMarker(upgradeMarkerPath(exePath))
	if merr != nil || m == nil || m.State != upgradeStatePending {
		return
	}
	// external review F1: only the TARGETED instance drives its upgrade's
	// runtime lifecycle. A co-hosted sibling restarting mid-window must not
	// arm a watchdog for someone else's upgrade — if the target dies, the
	// boot-time deadline/budget check on its supervisor's next relaunch is
	// the convergence path.
	if !a.markerTargetsThisAgent(m) {
		return
	}
	wait := m.Deadline.Sub(a.upgradeNow())
	if wait < 0 {
		wait = 0
	}
	wctx, cancel := context.WithCancel(ctx)
	a.upgradeMu.Lock()
	a.upgradeWatchdogStop = cancel
	a.upgradeMu.Unlock()
	a.cfg.Logger.Info("agent: upgrade watchdog armed (pending register deadline)",
		"deadline_in", wait.Round(time.Second).String(), "new_version", m.NewVersion)
	agentupgrade.ArmLegacyWatchdog(wctx, wait, &a.upgradeWatchdogWG, func() bool { return bool(watchdogCapturedUpgrade(a, exePath, m)) })
}

// watchdogRollback fires at the register deadline: if the marker is STILL
// pending (mutex — a commit that won the race makes this a no-op), restore
// prev and exec into it.
func (a *Agent) watchdogRollback(exePath string) rollbackResult {
	return watchdogCapturedUpgrade(a, exePath, captureUpgradeReport(a))
}

// upgradeRegisterReport returns the UpgradeState/UpgradeDetail pair to attach
// to a register request (empty on an ordinary register). Wire semantics in
// proto.NodeRegisterReq.
func (a *Agent) upgradeRegisterReport() (state, detail string) {
	return upgradeReportForMarker(a, captureUpgradeReport(a))
}

// commitUpgradeAfterRegister retains the legacy unit adapter. Production uses
// commitCapturedUpgradeReport after control readiness with the identity captured
// before the network wait. The UpgradeState string is what register CARRIED
// (from upgradeRegisterReport at request-build time; "" on an ordinary register).
// A proved report promotes pending to committed, stops the watchdog, and clears
// a terminal marker the register
// just delivered. Mutex-serialized against watchdogRollback — whoever takes
// the lock first wins, the loser no-ops on the state check. The reported
// parameter is what makes the no-op precise: a register that attached
// nothing must not clear a terminal record some LATER register still needs
// to deliver, and a register that attached "committed" must not commit a
// marker the watchdog rolled back while the request was in flight.
func (a *Agent) commitUpgradeAfterRegister(reported string) {
	commitCapturedUpgradeReport(a, reported, captureUpgradeReport(a))
}

// Reports and timers capture immutable transaction identity before waiting.
// origin: hot-update S8 plan §3.3 / M11.

func agentIncarnation(a *Agent) string {
	if value := a.incarnation.Load(); value != nil {
		return *value
	}
	return ""
}

func armTransactionWatchdog(ctx context.Context, a *Agent) bool {
	return agentupgrade.ArmWatchdog(ctx, a.cfg.Upgrade, a.upgradeEndpoint, agentIncarnation(a), &a.upgradeWatchdogWG, a.cfg.Logger)
}

func upgradeNonce() string { return agentupgrade.Nonce() }

// execLineage carries what the two syscall.Exec sites must hand forward.
//
// It is PROCESS-level state rather than a field on Agent because one of the two
// sites (realExec in upgrade_state.go) is a package-level function with no
// receiver, and because the thing being modelled genuinely is per-process: a
// process has exactly one instance lineage. Tests never traverse the exec path,
// so a shared value cannot leak between concurrently-constructed Agents there.
var execLineage struct {
	instanceID atomic.Pointer[string]
	routingNID atomic.Pointer[string]
}

// setExecLineage records what a subsequent re-exec must carry forward.
func setExecLineage(instanceID, routingNID string) {
	if instanceID != "" {
		execLineage.instanceID.Store(&instanceID)
	}
	if routingNID != "" {
		execLineage.routingNID.Store(&routingNID)
	}
}

// execEnv returns the environment for a syscall.Exec of this binary: the
// current environment with the instance lineage attached, so the re-executed
// image continues the SAME instance instead of arriving as a stranger and
// contesting the name its own predecessor still holds.
//
// Without this, `node upgrade` would rename every node it upgraded.
func execEnv() []string {
	// Fall back to the raw environment when no Agent has recorded a lineage yet.
	//
	// The boot-time upgrade check runs BEFORE Agent.New — a rollback there
	// re-execs from a process that never called mintInstanceID, so execLineage
	// is empty while the inherited variables are still in os.Environ(). Without
	// this fallback the rollback would strip the lineage and never re-add it,
	// and the recovered agent would arrive as a stranger contesting its own
	// name.
	return execEnvFor(loadedLineage(execLineage.instanceID.Load(), instanceIDEnv),
		loadedLineage(execLineage.routingNID.Load(), routingNIDEnv))
}

type UpgradeDependencies = agentupgrade.Dependencies

// UpgradeEndpoint exposes only the constructed engine; production adapters
// never install a compile stub or use the legacy install/re-exec path.
// origin: hot-update S8 plan §2.2 / §11.2 P14.
func UpgradeEndpoint(a *Agent) upgradeport.Endpoint { return a.upgradeEndpoint }

func checkRegisterAuthority(a *Agent, expected string, body []byte) error {
	var resp proto.NodeRegisterResp
	if err := json.Unmarshal(body, &resp); err != nil {
		return err
	}
	return upgradeauthority.CheckRegister(&a.incarnation, expected, resp)
}
