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
//   - commit-on-register: the first successful register after the re-exec is
//     the health check-in that promotes pending → committed.
//
// Commit and rollback are mutually exclusive: every marker transition after
// boot goes through Agent.upgradeMu, so a register racing the watchdog cannot
// commit after the rollback started, and a fired watchdog is a no-op once the
// marker is committed.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

const (
	upgradeStatePending        = "pending"
	upgradeStateCommitted      = "committed"
	upgradeStateRolledBack     = "rolled_back"
	upgradeStateRollbackFailed = "rollback_failed"

	// upgradeMarkerName lives NEXT TO the binary (same writable dir the
	// installer already requires for its tmp file and atomic rename).
	upgradeMarkerName = ".tether-upgrade.json"

	// upgradeBootBudget is how many times the NEW binary may be (re)started
	// while pending before boot rolls back. It — not the register deadline —
	// is what closes the crash-loop failure mode: this repo's units restart
	// every 2s/5s, which never trips systemd's default start limit
	// (10s window / 5 starts), so an instant-crash binary would otherwise
	// loop right through the deadline. Budget 3 converges in ~15s.
	upgradeBootBudget = 3

	// upgradeRegisterDeadline bounds pending: the new binary must complete a
	// register within this window or the watchdog restores the prev slot. A
	// constant, not a flag (plan §0: no tunables without a use case). On a
	// weak NAT link an honest binary can lose the race — the rollback is
	// harmless (node stays online on the old version; re-run the upgrade).
	upgradeRegisterDeadline = 120 * time.Second
)

// upgradeMarker is the on-disk record of an in-flight (or just-finished)
// upgrade. Written atomically (tmp + rename) next to the binary.
//
// TargetSID/TargetNID/UpgradeID (external review F1): the marker lives next
// to a binary that MAY be shared by several agent processes on one host
// (requirements: one machine can run multiple agents). These fields pin the
// upgrade to the single instance `node upgrade <nid>` targeted — commit and
// register-report refuse a marker whose target is not this agent, so a
// co-hosted sibling can neither claim nor clear another instance's upgrade.
// A shared binary is ONE UPGRADE DOMAIN: while a marker is pending, sibling
// installs are refused (upgrade_in_progress) until it commits or rolls back.
type upgradeMarker struct {
	State       string    `json:"state"`
	PrevSHA     string    `json:"prev_sha"`
	NewSHA      string    `json:"new_sha"`
	PrevVersion string    `json:"prev_version"`
	NewVersion  string    `json:"new_version"`
	Deadline    time.Time `json:"deadline"`
	BootCount   int       `json:"boot_count"`
	BootBudget  int       `json:"boot_budget"`
	Detail      string    `json:"detail,omitempty"`
	TargetSID   string    `json:"target_sid,omitempty"`
	TargetNID   string    `json:"target_nid,omitempty"`
	UpgradeID   string    `json:"upgrade_id,omitempty"`
}

func upgradeMarkerPath(exePath string) string {
	return filepath.Join(filepath.Dir(exePath), upgradeMarkerName)
}

func upgradePrevPath(exePath string) string { return exePath + ".prev" }

// upgradeLockPath is the CROSS-PROCESS mutual exclusion for every
// read-modify-write of the marker/prev/dst triple (external review F1: the
// in-process mutexes cannot serialize two agent processes sharing one
// binary). flock, not O_EXCL: released automatically when a process dies.
func upgradeLockPath(exePath string) string {
	return filepath.Join(filepath.Dir(exePath), ".tether-upgrade.lock")
}

// withUpgradeFileLock runs fn holding the host-wide upgrade flock. blocking
// decides contention behavior: the install pipeline uses non-blocking (a
// loser replies upgrade_in_progress instead of queueing behind a ~35s
// download), the short marker transitions (boot / commit / watchdog /
// exec-failure recovery) block — their critical sections are milliseconds.
// Returns errUpgradeLockBusy when non-blocking and the lock is held.
func withUpgradeFileLock(exePath string, blocking bool, fn func() error) error {
	f, err := os.OpenFile(upgradeLockPath(exePath), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("open upgrade lock: %w", err)
	}
	defer func() { _ = f.Close() }()
	how := syscall.LOCK_EX
	if !blocking {
		how |= syscall.LOCK_NB
	}
	var lockErr error
	for {
		lockErr = syscall.Flock(int(f.Fd()), how)
		if !errors.Is(lockErr, syscall.EINTR) {
			break
		}
	}
	if lockErr != nil {
		if !blocking && (errors.Is(lockErr, syscall.EWOULDBLOCK) || errors.Is(lockErr, syscall.EAGAIN)) {
			return errUpgradeLockBusy
		}
		return fmt.Errorf("flock upgrade lock: %w", lockErr)
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	return fn()
}

// errUpgradeLockBusy: another PROCESS holds the host-wide upgrade lock.
var errUpgradeLockBusy = errors.New("upgrade: host-wide upgrade lock is held by another process")

// upgradeBootProof is populated only by the pre-Cobra boot shim in this
// process. Unlike BootCount (shared on disk) or a path-based SHA (borrowable
// after rename on non-Linux systems), the marker's unique UpgradeID can be
// carried into Agent.New only by a process that actually traversed the
// pending boot decision.
var upgradeBootProof struct {
	sync.RWMutex
	id string
}

func rememberUpgradeBootProof(id string) {
	upgradeBootProof.Lock()
	upgradeBootProof.id = id
	upgradeBootProof.Unlock()
}

func currentUpgradeBootProof() string {
	upgradeBootProof.RLock()
	defer upgradeBootProof.RUnlock()
	return upgradeBootProof.id
}

// upgradeSyncObserver (external review F6) is the injection point for the
// ordered-durability tests: nil in production. When set, it is called at
// every fsync point BEFORE the real sync; a non-nil return simulates that
// sync failing (the power-cut/error matrix), and the real sync is skipped.
var upgradeSyncObserver func(kind, path string) error

// syncFile fsyncs an open file (data + metadata, including a chmod done on
// the same fd). Part of the F6 ordered-durability protocol: rename gives
// crash ATOMICITY, not persistence — without fsync of file and directory a
// power cut can surface a truncated dst or a lost marker after reboot.
func syncFile(f *os.File) error {
	if upgradeSyncObserver != nil {
		return upgradeSyncObserver("file", f.Name())
	}
	return f.Sync()
}

// syncDirE fsyncs a directory, making the rename/link/remove entries inside
// it durable. Error-returning: the install pipeline treats a failed sync as
// install_failed (nothing irreversible has happened yet).
func syncDirE(dir string) error {
	if upgradeSyncObserver != nil {
		return upgradeSyncObserver("dir", dir)
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	serr := d.Sync()
	cerr := d.Close()
	if serr != nil {
		return serr
	}
	return cerr
}

// syncDir is the best-effort variant for the rollback/terminal paths, where
// refusing to proceed on a sync error would recreate the stuck states this
// machinery exists to end. Loud, not fatal.
func syncDir(dir string) {
	if err := syncDirE(dir); err != nil {
		slog.Default().Warn("upgrade: directory fsync failed (durability of the last transition is not guaranteed)",
			"dir", dir, "err", err)
	}
}

// readUpgradeMarker returns (nil, nil) when no marker exists, and (nil, err)
// when the file exists but does not decode — corrupt markers are treated as
// idle by every caller (loudly, never a panic and never a block: a corrupt
// marker must not brick the upgrade path).
func readUpgradeMarker(path string) (*upgradeMarker, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var m upgradeMarker
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("upgrade marker %s is corrupt: %w", path, err)
	}
	return &m, nil
}

func writeUpgradeMarker(path string, m *upgradeMarker) error {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tether-upgrade-marker-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	_, werr := tmp.Write(raw)
	// fsync before the rename (internal review S13): without it, a power cut
	// after the rename can leave a zero-length marker on filesystems without
	// auto_da_alloc-style heuristics — which reads as corrupt→idle and
	// silently disarms the rollback machinery.
	serr := syncFile(tmp)
	cerr := tmp.Close()
	if werr != nil || serr != nil || cerr != nil {
		_ = os.Remove(name)
		return errors.Join(werr, serr, cerr)
	}
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return err
	}
	// Directory fsync (external review F6): the rename above is atomic but
	// not durable until the directory entry itself is on stable storage.
	return syncDirE(filepath.Dir(path))
}

// bootAction is decideBoot's verdict. Deliberately a string type, not an iota
// enum (keeps it out of the enum-switch-default gate's family table and makes
// test failures self-describing).
type bootAction string

const (
	// bootProceed: no marker / terminal marker — a perfectly ordinary boot.
	bootProceed bootAction = "proceed"
	// bootContinuePending: we ARE the staged new binary, inside budget and
	// deadline. Caller persists boot_count+1 and lets Run arm the watchdog.
	bootContinuePending bootAction = "continue_pending"
	// bootRollback: pending but the boot budget or deadline is exhausted —
	// restore the prev slot and exec into it.
	bootRollback bootAction = "rollback"
	// bootConvergeRolledBack: we are the PREV binary while the marker still
	// says pending — a rollback (or a failed install before the flip) was
	// interrupted before the marker caught up. Converge the record.
	bootConvergeRolledBack bootAction = "converge_rolled_back"
	// bootMarkOrphan: the running binary matches NEITHER sha (someone swapped
	// the binary by hand mid-upgrade). Record rollback_failed and run with
	// what's on disk — anything else would fight the operator.
	bootMarkOrphan bootAction = "mark_orphan"
)

// decideBoot is the pure boot-time decision (plan §3.1, table-driven-tested in
// upgrade_state_test.go). It does no IO: the caller supplies the marker, the
// sha of the binary it is running from, and the clock.
func decideBoot(m *upgradeMarker, selfSHA string, now time.Time) bootAction {
	if m == nil || m.State != upgradeStatePending {
		return bootProceed
	}
	if selfSHA == m.NewSHA {
		if m.BootCount >= m.BootBudget || !now.Before(m.Deadline) {
			return bootRollback
		}
		return bootContinuePending
	}
	if selfSHA == m.PrevSHA {
		return bootConvergeRolledBack
	}
	return bootMarkOrphan
}

// executeRollback restores the prev slot over the live binary path and records
// the outcome. When reexec is true (boot rollback, watchdog rollback) it then
// execs the restored binary via execFn; when false (syscall.Exec failure — the
// still-running OLD process image is itself the restored code) it just
// returns. Rollback failure (prev slot missing or not the recorded sha) is a
// TERMINAL record: log loudly, keep running whatever we are — a retry loop
// here would recreate the crash-loop this machinery exists to end.
func executeRollback(exePath string, m *upgradeMarker, reason string, logger *slog.Logger, reexec bool, execFn func(string) error) {
	markerPath := upgradeMarkerPath(exePath)
	prev := upgradePrevPath(exePath)

	prevSHA, err := sha256OfFile(prev)
	if err != nil || prevSHA != m.PrevSHA {
		m.State = upgradeStateRollbackFailed
		m.Detail = fmt.Sprintf("prev slot unusable (err=%v, sha=%s, want=%s) — running what is on disk", err, prevSHA, m.PrevSHA)
		if werr := writeUpgradeMarker(markerPath, m); werr != nil {
			logger.Error("agent: upgrade rollback: marker write failed on top of an unusable prev slot", "err", werr)
		}
		logger.Error("agent: UPGRADE ROLLBACK FAILED — prev slot is missing or corrupt; continuing on the current binary",
			"reason", reason, "detail", m.Detail)
		return
	}
	if err := os.Rename(prev, exePath); err != nil {
		m.State = upgradeStateRollbackFailed
		m.Detail = fmt.Sprintf("restore rename failed: %v", err)
		if werr := writeUpgradeMarker(markerPath, m); werr != nil {
			logger.Error("agent: upgrade rollback: marker write failed after a failed restore", "err", werr)
		}
		logger.Error("agent: UPGRADE ROLLBACK FAILED — could not restore prev slot; continuing on the current binary",
			"reason", reason, "err", err)
		return
	}
	syncDir(filepath.Dir(exePath)) // F6: make the restore rename durable before recording it
	m.State = upgradeStateRolledBack
	m.Detail = reason
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		// The restore itself landed; a stale pending marker converges at the
		// next boot via decideBoot's prev-sha branch.
		logger.Error("agent: upgrade rollback: restore landed but marker write failed", "err", err)
	}
	logger.Warn("agent: upgrade ROLLED BACK to previous binary",
		"reason", reason, "prev_version", m.PrevVersion, "new_version", m.NewVersion)
	if reexec && execFn != nil {
		if err := execFn(exePath); err != nil {
			// origin: internal review S12. Do NOT exit here: on setsid-nohup
			// there is no supervisor to relaunch anything, and this running
			// image — whatever its flaws — is the only live process the
			// NAT'd host has. The restore has landed on disk (dst = old
			// binary, marker = rolled_back), so any later restart, manual or
			// supervised, comes back as the known-good binary. Keep serving.
			logger.Error("agent: upgrade rollback: exec of restored binary failed; "+
				"continuing on the current process image (disk already restored — the next restart boots the old binary)",
				"err", err)
		}
	}
}

// realExec is the production execFn: replace this process image in place
// (same PID — supervisors never see a death).
func realExec(exePath string) error {
	argv := append([]string(nil), os.Args...)
	argv[0] = exePath
	return syscall.Exec(exePath, argv, os.Environ())
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
		proof = bootUpgradeCheckLocked(exePath, logger, now, execFn)
		return nil
	}); err != nil {
		logger.Warn("agent: boot upgrade check skipped: host-wide upgrade lock unavailable", "err", err)
		return ""
	}
	return proof
}

func bootUpgradeCheckLocked(exePath string, logger *slog.Logger, now time.Time, execFn func(string) error) string {
	markerPath := upgradeMarkerPath(exePath)
	m, err := readUpgradeMarker(markerPath)
	if err != nil {
		logger.Warn("agent: upgrade marker unreadable — treating as idle", "err", err)
		return ""
	}
	if m == nil || m.State != upgradeStatePending {
		return ""
	}
	selfSHA, err := sha256OfFile(exePath)
	if err != nil {
		logger.Warn("agent: boot upgrade check: cannot hash own binary — leaving marker untouched", "err", err)
		return ""
	}
	switch decideBoot(m, selfSHA, now) {
	case bootProceed:
		return ""
	case bootContinuePending:
		m.BootCount++
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			logger.Warn("agent: boot upgrade check: boot_count persist failed", "err", err)
			return ""
		}
		logger.Info("agent: booting a staged upgrade (pending commit)",
			"new_version", m.NewVersion, "boot_count", m.BootCount, "boot_budget", m.BootBudget,
			"deadline", m.Deadline.UTC().Format(time.RFC3339))
		return m.UpgradeID
	case bootRollback:
		executeRollback(exePath, m, fmt.Sprintf("boot: budget/deadline exhausted (boot_count=%d/%d, deadline=%s)",
			m.BootCount, m.BootBudget, m.Deadline.UTC().Format(time.RFC3339)), logger, true, execFn)
	case bootConvergeRolledBack:
		m.State = upgradeStateRolledBack
		if m.Detail == "" {
			m.Detail = "rollback interrupted; converged at boot (running binary matches prev sha)"
		}
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			logger.Warn("agent: boot upgrade check: converge write failed", "err", err)
		}
		logger.Warn("agent: converged an interrupted upgrade rollback at boot",
			"prev_version", m.PrevVersion, "new_version", m.NewVersion)
	case bootMarkOrphan:
		m.State = upgradeStateRollbackFailed
		m.Detail = "running binary matches neither prev nor new sha (replaced by hand mid-upgrade?)"
		if err := writeUpgradeMarker(markerPath, m); err != nil {
			logger.Warn("agent: boot upgrade check: orphan write failed", "err", err)
		}
		logger.Error("agent: upgrade marker orphaned — running binary matches neither recorded sha; continuing")
	}
	return ""
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

// runningImageSHA hashes THIS PROCESS'S executing image — not the on-disk
// path (external review F1): after the install flip, the path holds the new
// binary for EVERY process on the host, but only the process that re-exec'd
// is actually running it. /proc/self/exe reads the running image's bytes even
// when the path it once came from has been renamed over; that makes this the
// one commit proof a co-hosted sibling cannot borrow.
func (a *Agent) runningImageSHA() (string, error) {
	p := a.cfg.UpgradeRunningImagePath
	if p == "" {
		if runtime.GOOS == "linux" {
			p = "/proc/self/exe"
		} else {
			exe, err := os.Executable()
			if err != nil {
				return "", err
			}
			p = strings.TrimSuffix(exe, " (deleted)")
		}
	}
	return sha256OfFile(p)
}

// markerTargetsThisAgent is the instance-identity half of the commit proof
// (external review F1): a shared-binary sibling with a different sid/nid must
// neither report nor transition an upgrade that targeted someone else.
func (a *Agent) markerTargetsThisAgent(m *upgradeMarker) bool {
	return m.TargetSID == a.cfg.SID && m.TargetNID == a.cfg.NID
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
	timer := time.NewTimer(wait)
	go func() {
		defer timer.Stop()
		select {
		case <-wctx.Done():
			return
		case <-timer.C:
			a.watchdogRollback(exePath)
		}
	}()
}

// watchdogRollback fires at the register deadline: if the marker is STILL
// pending (mutex — a commit that won the race makes this a no-op), restore
// prev and exec into it.
func (a *Agent) watchdogRollback(exePath string) {
	a.upgradeMu.Lock()
	defer a.upgradeMu.Unlock()
	// Host-wide flock (external review F1): the rollback's read-decide-write
	// must exclude sibling PROCESSES, not just our own goroutines.
	lerr := withUpgradeFileLock(exePath, true, func() error {
		m, err := readUpgradeMarker(upgradeMarkerPath(exePath))
		if err != nil || m == nil || m.State != upgradeStatePending || !a.markerTargetsThisAgent(m) {
			//nolint:nilerr // The closure's error return propagates LOCK failures only; a corrupt
			// marker is by contract "idle" — no pending upgrade of ours means the watchdog has no work.
			return nil
		}
		execFn := func(p string) error { return a.upgradeExec(p) }
		if a.cfg.UpgradeNoExit && a.cfg.UpgradeExecFn == nil {
			execFn = nil // in-process test harness: record the rollback, don't exec
		}
		executeRollback(exePath, m,
			fmt.Sprintf("register deadline exceeded (%s) without a successful register", upgradeRegisterDeadline),
			a.cfg.Logger, true, execFn)
		return nil
	})
	if lerr != nil {
		a.cfg.Logger.Error("agent: upgrade watchdog: host-wide lock unavailable; rollback was not performed",
			"err", lerr)
	}
}

// upgradeRegisterReport returns the UpgradeState/UpgradeDetail pair to attach
// to a register request (empty on an ordinary register). Wire semantics in
// proto.NodeRegisterReq.
func (a *Agent) upgradeRegisterReport() (state, detail string) {
	exePath, err := a.upgradeExePath()
	if err != nil {
		return "", ""
	}
	m, merr := readUpgradeMarker(upgradeMarkerPath(exePath))
	if merr != nil || m == nil {
		return "", ""
	}
	switch m.State {
	case upgradeStatePending:
		// Four-part commit proof (internal review S1 + external reviews F1/F9),
		// each closing a distinct impersonation:
		//   1. target identity — a co-hosted sibling with another sid/nid
		//      must not claim an upgrade that targeted someone else;
		//   2. BootCount > 0 — the staged image went through decideBoot at
		//      least once (the install-time marker is always written with 0,
		//      so a register racing the flip→exec window proves nothing);
		//   3. process-local boot proof == UpgradeID — this process traversed
		//      the pending boot shim for this exact transaction;
		//   4. RUNNING-IMAGE sha == NewSHA — this very process executes the
		//      staged bytes. Hashing the on-disk path here would be
		//      borrowable: after the flip EVERY process on the host sees
		//      NewSHA at the path while still running the old image.
		if !a.markerTargetsThisAgent(m) {
			return "", ""
		}
		if m.BootCount == 0 || m.UpgradeID == "" || a.upgradeBootProofID != m.UpgradeID {
			return "", ""
		}
		selfSHA, serr := a.runningImageSHA()
		if serr != nil || selfSHA != m.NewSHA {
			return "", ""
		}
		return upgradeStateCommitted, fmt.Sprintf("upgraded %s -> %s", m.PrevVersion, m.NewVersion)
	case upgradeStateRolledBack, upgradeStateRollbackFailed:
		if !a.markerTargetsThisAgent(m) {
			return "", ""
		}
		return m.State, m.Detail
	default:
		return "", ""
	}
}

// commitUpgradeAfterRegister runs on every SUCCESSFUL register, with the
// UpgradeState string that register actually CARRIED (from
// upgradeRegisterReport at request-build time; "" on an ordinary register).
// It promotes a pending marker to committed (that register was the health
// check-in), stops the watchdog, and clears a terminal marker the register
// just delivered. Mutex-serialized against watchdogRollback — whoever takes
// the lock first wins, the loser no-ops on the state check. The reported
// parameter is what makes the no-op precise: a register that attached
// nothing must not clear a terminal record some LATER register still needs
// to deliver, and a register that attached "committed" must not commit a
// marker the watchdog rolled back while the request was in flight.
func (a *Agent) commitUpgradeAfterRegister(reported string) {
	if reported == "" {
		return
	}
	exePath, err := a.upgradeExePath()
	if err != nil {
		return
	}
	a.upgradeMu.Lock()
	defer a.upgradeMu.Unlock()
	// The host-wide flock (external review F1) serializes this
	// read-decide-write against SIBLING PROCESSES sharing the binary; the
	// in-process upgradeMu above serializes it against our own watchdog.
	lerr := withUpgradeFileLock(exePath, true, func() error {
		markerPath := upgradeMarkerPath(exePath)
		m, merr := readUpgradeMarker(markerPath)
		if merr != nil || m == nil {
			//nolint:nilerr // The closure's error return propagates LOCK failures only; a corrupt
			// marker is by contract "idle" — there is nothing for this register to commit or clear.
			return nil
		}
		switch {
		case m.State == upgradeStatePending && reported == upgradeStateCommitted:
			// Same four-part proof as upgradeRegisterReport (S1 + F1/F9):
			// target identity, boot count, process-local upgrade ID, running-image sha.
			if !a.markerTargetsThisAgent(m) || m.BootCount == 0 ||
				m.UpgradeID == "" || a.upgradeBootProofID != m.UpgradeID {
				return nil
			}
			selfSHA, serr := a.runningImageSHA()
			if serr != nil || selfSHA != m.NewSHA {
				//nolint:nilerr // The closure's error return propagates LOCK failures only; an
				// unreadable running image simply fails the commit PROOF — refusing to commit is
				// the entire point of the check, not an error to surface.
				return nil // not the staged image (or unreadable) — not our commit to make
			}
			m.State = upgradeStateCommitted
			if err := writeUpgradeMarker(markerPath, m); err != nil {
				// origin: internal review S14 — honest failure note: the marker
				// stays pending and the watchdog stays armed, so a healthy,
				// registered upgrade may still be rolled back at the deadline.
				// That is the SAFE direction (old binary, node stays reachable);
				// deliberately not stopping the watchdog here, because a pending
				// marker with no watchdog has no owner left to converge it.
				a.cfg.Logger.Error("agent: upgrade commit: marker write failed — watchdog stays armed and may roll back at the deadline", "err", err)
				return nil
			}
			if a.upgradeWatchdogStop != nil {
				a.upgradeWatchdogStop()
				a.upgradeWatchdogStop = nil
			}
			a.cfg.Logger.Info("agent: upgrade COMMITTED (registered as the new binary)",
				"prev_version", m.PrevVersion, "new_version", m.NewVersion, "release", proto.ReleaseVersion)
		case m.State == reported && (m.State == upgradeStateRolledBack || m.State == upgradeStateRollbackFailed):
			// The register that just succeeded carried this terminal outcome —
			// delivered; clear it so later registers stop re-reporting.
			// (A COMMITTED marker is deliberately NOT cleared, by contrast: it is
			// the operator's on-host breadcrumb of the last successful upgrade
			// and is replaced wholesale by the next install — internal review
			// S24. The asymmetry is intentional: terminal failure states are
			// one-shot reports, success is a durable record.)
			if !a.markerTargetsThisAgent(m) {
				return nil
			}
			if err := os.Remove(markerPath); err != nil {
				a.cfg.Logger.Warn("agent: upgrade: could not clear delivered terminal marker", "err", err)
			}
			syncDir(filepath.Dir(markerPath))
		}
		return nil
	})
	if lerr != nil {
		a.cfg.Logger.Warn("agent: upgrade commit: host-wide lock unavailable", "err", lerr)
	}
}
