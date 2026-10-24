//go:build linux

// These tests use real process images, children, and embedded NATS. The final
// exec is intercepted so rollback intent and the live Prepared can be examined.
// They do not exercise a supervisor or replace the test process.
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradeobserve"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"

	"github.com/nats-io/nats.go"
)

// origin: hot-update S4 plan §4.2 / §5.2. A blocked cut has no use for a
// reader probe: a slow legacy prev must not delay the rollback_blocked receipt
// or hold upgradeMu/flock while business admission is already known to be busy.
func TestWatchdogDefersBusyCutBeforePreviousImageProbe(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			task := cutStartTask(t, a)
			cutImportActive(t, a)
			m := cutPending(t, a)
			dir := t.TempDir()
			probed, release := filepath.Join(dir, "probed"), filepath.Join(dir, "release")
			script := domainScript("exec-v1 pty-v1", 1) +
				": > '" + probed + "'\nwhile [ ! -e '" + release + "' ]; do sleep 0.01; done\n"
			if err := os.WriteFile(upgradePrevPath(a.cfg.UpgradeExecutablePath), []byte(script), 0755); err != nil {
				t.Fatal(err)
			}
			m.PrevSHA = mustSHA(t, upgradePrevPath(a.cfg.UpgradeExecutablePath))
			m.PrevImageSHA256, m.PrevImageABI, m.PrevImageDomains = "", 0, nil
			if err := writeUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath), m); err != nil {
				t.Fatal(err)
			}
			a.procs["busy"] = &procRec{}
			result := make(chan rollbackResult, 1)
			done := make(chan struct{})
			go func() {
				defer close(done)
				result <- a.watchdogRollback(a.cfg.UpgradeExecutablePath)
			}()
			defer func() {
				_ = os.WriteFile(release, nil, 0600)
				<-done
			}()
			select {
			case got := <-result:
				if got != rollbackRetry {
					t.Fatalf("busy cut result=%v, want retry", got)
				}
			case <-time.After(upgradebudget.RequestLatency / 10):
				t.Fatal("busy rollback waited for the previous-image version probe")
			}
			<-done
			if _, err := os.Stat(probed); !os.IsNotExist(err) {
				t.Fatal("busy rollback executed the previous image")
			}
			blocked := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
			if !a.rollbackInFlight.Load() || blocked.State != upgradeStatePending ||
				blocked.ResourcePhase != markerPhaseRollbackBlocked || blocked.Detail != agentimage.ReasonLegacySessionBusy {
				t.Fatalf("busy rollback lost its pending responsibility: %+v", blocked)
			}
			cutRequireOpen(t, a)
			delete(a.procs, "busy")
			if err := os.WriteFile(release, nil, 0600); err != nil {
				t.Fatal(err)
			}
			execs := 0
			a.cfg.UpgradeExecFn = func(string) error { execs++; return syscall.ENOEXEC }
			if a.watchdogRollback(a.cfg.UpgradeExecutablePath) != rollbackDone || execs != 1 {
				t.Fatal("cleared cut did not validate and dispatch the previous image")
			}
			if _, err := os.Stat(probed); err != nil {
				t.Fatal("cleared cut skipped the required reader probe")
			}
			cutRequireOpen(t, a)
			cutFinishTask(t, a, task)
		})
	}
	testharness.AssertNoFDLeak(t, "busy rollback before reader probe x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "busy rollback before reader probe x5", beforeG)
}

// origin: hot-update S3 plan §2.6 / §3.5; rollback review RB-1
func TestRunAuthoritativeRefusalRedispatchesImportedTasks(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			nc := cutNATS(t)
			a := cutTestAgent(t)
			a.cfg.NATSURL = nc.ConnectedUrl()
			a.cfg.RegisterTimeout = time.Second
			startAckResponder(t, nc.ConnectedUrl())
			task := cutStartTask(t, a)
			childPID := 0
			for _, row := range a.workloads.Snapshot() {
				if row.ID == task.pid {
					childPID = row.OSPID
				}
			}
			if childPID == 0 {
				t.Fatal("missing real child")
			}
			cutImportActive(t, a)
			m := cutPending(t, a)
			m.Deadline = time.Now().Add(time.Hour) // only Run's exit path can trigger rollback
			if err := writeUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath), m); err != nil {
				t.Fatal(err)
			}
			if _, err := nc.Subscribe(proto.SubjNodeRegister(a.cfg.SID, a.cfg.NID), func(msg *nats.Msg) {
				body, _ := json.Marshal(proto.NodeRegisterResp{Code: "proto_mismatch", Error: "refusal fixture"})
				_ = msg.Respond(body)
			}); err != nil {
				t.Fatal(err)
			}
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			logs := &rollbackReceipts{}
			a.cfg.Logger = slog.New(logs)
			execs, prepared := 0, false
			a.cfg.UpgradeExecFn = func(string) error {
				execs++
				prepared = cutHandoffFDs() == 1
				requireRollbackReceipt(t, a, logs, task.pid)
				return syscall.ENOEXEC
			}
			ctx, cancel := context.WithTimeout(t.Context(), cutTestWait)
			defer cancel()
			err := a.Run(ctx)
			if err == nil || !strings.Contains(err.Error(), "proto_mismatch") || ctx.Err() != nil {
				t.Fatalf("did not reach authoritative refusal with live parent: err=%v parent=%v", err, ctx.Err())
			}
			got := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
			// origin: hot-update S8 plan §1 / §11.2 P17; S8 review rv-fault F7.
			// The redispatch exec failed: the attempt is recorded, never rolled_back.
			if execs != 1 || !prepared || got.State != upgradeStateRollbackFailed {
				t.Fatalf("authoritative refusal skipped rollback: execs=%d prepared=%t state=%s parent=%v session=%v child_status=%v", execs, prepared, got.State, ctx.Err(), a.loadRunCtx().Err(), syscall.Kill(childPID, 0))
			}
		})
	}
	testharness.AssertNoFDLeak(t, "rollback ownership x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "rollback ownership x5", beforeG)
}

// origin: hot-update S3 plan §2.6 / §3.5; rollback review RB-2
func TestWatchdogReadyImageRetainsHandoffAfterCommitWriteFailure(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			nc := cutNATS(t)
			a := cutTestAgent(t)
			a.cfg.NATSURL = nc.ConnectedUrl()
			task := cutStartTask(t, a)
			cutImportActive(t, a)
			m := cutPending(t, a)
			a.upgradeBootProofID = m.UpgradeID
			if _, err := nc.Subscribe(proto.SubjNodeRegister(a.cfg.SID, a.cfg.NID), func(msg *nats.Msg) {
				body, _ := json.Marshal(proto.NodeRegisterResp{OK: true, AcceptedProcesses: []string{task.pid}})
				_ = msg.Respond(body)
			}); err != nil {
				t.Fatal(err)
			}
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			oldObserver := upgradeSyncObserver
			t.Cleanup(func() { upgradeSyncObserver = oldObserver })
			var watchdogStopped atomic.Bool
			a.upgradeWatchdogStop = func() { watchdogStopped.Store(true) }
			var writesFailed atomic.Int32
			upgradeSyncObserver = func(kind, _ string) error {
				if kind == "file" {
					writesFailed.Add(1)
					return syscall.ENOSPC
				}
				return nil
			}
			// origin: hot-update S8 plan §4.2 / P14. Establish the real session's
			// forwarded/intake/sys-events subscribers, reconciliation and Flush;
			// register alone cannot attempt commit or mark the imported image ready.
			sessionCtx, stopSession := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { _, err := a.session(sessionCtx); done <- err }()
			defer func() { stopSession(); <-done }()
			cutWait(t, "control-ready after failed commit", func() bool {
				return writesFailed.Load() == 1 && a.image.Phase() == agentboot.PhaseReady
			})
			if writesFailed.Load() != 1 || watchdogStopped.Load() || a.image.Phase() != agentboot.PhaseReady || a.controlReady.Load() == nil {
				t.Fatalf("failed commit fixture: failed_writes=%d phase=%s", writesFailed.Load(), a.image.Phase())
			}
			if got := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath)); got.State != upgradeStatePending {
				t.Fatalf("commit failure did not retain pending marker: %s", got.State)
			}
			execs, prepared, cuts := 0, false, 0
			a.cfg.UpgradeCutObserver = func(b agentimage.Bundle) {
				cuts++
				if len(b.Runtime.Tasks) != 1 || b.Runtime.Tasks[0].PID != task.pid {
					t.Error("watchdog did not carry the imported child")
				}
			}
			a.cfg.UpgradeExecFn = func(string) error {
				execs++
				prepared = cutHandoffFDs() == 1 && a.workloads.Frozen() && a.processRuntime.Frozen()
				return syscall.ENOEXEC
			}
			upgradeSyncObserver = oldObserver
			a.watchdogRollback(a.cfg.UpgradeExecutablePath)
			cutRequireOpen(t, a)
			cutFinishTask(t, a, task)
			if execs != 1 || !prepared || cuts != 1 {
				t.Fatalf("ready image reached bare rollback: execs=%d prepared=%t cuts=%d", execs, prepared, cuts)
			}
		})
	}
	testharness.AssertNoFDLeak(t, "rollback ownership x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "rollback ownership x5", beforeG)
}

// origin: hot-update S3 plan §2.6 / §3.5; rollback review RB-3
func TestWatchdogMarkerGenerationMatchesRedispatchedBundle(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			task := cutStartTask(t, a)
			cutImportActive(t, a)
			m := cutPending(t, a)
			m.Generation = a.imageGeneration
			if err := writeUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath), m); err != nil {
				t.Fatal(err)
			}
			logs := &rollbackReceipts{}
			a.cfg.Logger = slog.New(logs)
			var sent uint64
			a.cfg.UpgradeExecFn = func(string) error {
				sent = requireRollbackReceipt(t, a, logs, task.pid)
				return syscall.ENOEXEC
			}
			a.watchdogRollback(a.cfg.UpgradeExecutablePath)
			if sent != a.imageGeneration+1 {
				t.Fatal("fixture did not re-export the next generation")
			}
			cutFinishTask(t, a, task)
		})
	}
	testharness.AssertNoFDLeak(t, "rollback ownership x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "rollback ownership x5", beforeG)
}

// origin: hot-update S3 plan §2.6 / §3.5; rollback review RB-4
func TestWatchdogExecFailureRecordsThawedResources(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			task := cutStartTask(t, a)
			cutImportActive(t, a)
			cutPending(t, a)
			a.cfg.UpgradeExecFn = func(string) error { return syscall.ENOEXEC }
			oldObserver := upgradeSyncObserver
			defer func() { upgradeSyncObserver = oldObserver }()
			observedThawedWrite := false
			upgradeSyncObserver = func(kind, _ string) error {
				if kind == "file" && !a.processRuntime.Frozen() && !a.workloads.Frozen() {
					observedThawedWrite = true
					if a.upgradeMu.TryLock() {
						a.upgradeMu.Unlock()
						t.Error("thaw receipt escaped upgradeMu")
					}
					if err := withUpgradeFileLock(a.cfg.UpgradeExecutablePath, false, func() error { return nil }); !errors.Is(err, errUpgradeLockBusy) {
						t.Errorf("thaw receipt escaped host flock: %v", err)
					}
				}
				return nil
			}
			a.watchdogRollback(a.cfg.UpgradeExecutablePath)
			cutRequireOpen(t, a)
			cutFinishTask(t, a, task)
			m := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
			if !observedThawedWrite || m.ResourcePhase != markerPhaseThawed || m.Detail != fmt.Sprintf("register deadline exceeded (%s) without a successful register", upgradeRegisterDeadline) {
				t.Fatalf("failed exec left resource_phase=%s although runtime thawed and original child completed", m.ResourcePhase)
			}
		})
	}
	testharness.AssertNoFDLeak(t, "rollback ownership x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "rollback ownership x5", beforeG)
}

// Record structured levels and attributes at the exec boundary, before a
// returning test exec thaws the runtime and changes its marker again.
type rollbackReceipts struct {
	mu      sync.Mutex
	records []slog.Record
}

func (l *rollbackReceipts) Enabled(context.Context, slog.Level) bool { return true }
func (l *rollbackReceipts) Handle(_ context.Context, r slog.Record) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, r.Clone())
	return nil
}
func (l *rollbackReceipts) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *rollbackReceipts) WithGroup(string) slog.Handler      { return l }
func requireRollbackReceipt(t *testing.T, a *Agent, logs *rollbackReceipts, pid string) uint64 {
	t.Helper()
	state, _ := bootPreparedState(t)
	bundle, err := agentimage.Decode(state)
	if err != nil {
		t.Error("rollback manifest did not decode")
		return 0
	}
	if len(bundle.Runtime.Tasks) != 1 || bundle.Runtime.Tasks[0].PID != pid {
		t.Error("rollback manifest lost the original child")
	}
	marker := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
	gen := bundle.Lineage.Generation
	// origin: hot-update S8 plan §1 / §11.2 P17 mutant "preexec rolled_back";
	// S8 review rv-fault F7. At the exec boundary the candidate still runs, so
	// the marker carries recovery intent; the restored image records rolled_back.
	if marker.State != upgradeStatePending || marker.ResourcePhase != markerPhaseRedispatched || marker.Generation != gen {
		t.Errorf("at exec marker=%s/%s/generation=%d; manifest generation=%d", marker.State, marker.ResourcePhase, marker.Generation, gen)
	}
	logs.mu.Lock()
	defer logs.mu.Unlock()
	for _, msg := range []string{"agent: image cut", "agent: image re-dispatched"} {
		found := 0
		for _, record := range logs.records {
			if record.Message != msg {
				continue
			}
			found++
			attrs := map[string]slog.Value{}
			record.Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value; return true })
			if record.Level != slog.LevelInfo || attrs["generation"].String() != strconv.FormatUint(gen, 10) {
				t.Errorf("%s missing Info/generation receipt", msg)
			}
			if msg == "agent: image cut" {
				if attrs["tasks"].String() != strconv.Itoa(len(bundle.Runtime.Tasks)) || attrs["courier_events"].String() != strconv.Itoa(len(bundle.Courier.Events)) {
					t.Error("cut receipt differs from manifest")
				}
			} else if !strings.Contains(attrs["image"].String(), fmt.Sprintf("generation=%d", gen)) {
				t.Error("redispatch summary does not describe the sent generation")
			}
		}
		if found != 1 {
			t.Errorf("at exec: %s receipts=%d, want 1", msg, found)
		}
	}
	return gen
}

// origin: hot-update S3 plan §3.5. A stop arriving while recovery waits for
// the transaction lock must win even after the initial parent check passed.
func TestRunFailureRechecksParentAfterLock(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			task := cutStartTask(t, a)
			cutImportActive(t, a)
			cutPending(t, a)
			parent, cancel := context.WithCancel(t.Context())
			defer cancel()
			ctx := &rollbackParentCheck{Context: parent, checked: make(chan struct{})}
			a.setRunCtx(t.Context())
			execs := 0
			a.cfg.UpgradeExecFn = func(string) error { execs++; return syscall.ENOEXEC }
			cause := errors.New("authoritative refusal")
			done := make(chan error, 1)
			a.upgradeMu.Lock()
			go func() { done <- runFailedBeforeControlReady(ctx, a, cause) }()
			select {
			case <-ctx.checked:
			case <-time.After(cutTestWait):
				a.upgradeMu.Unlock()
				t.Fatal("recovery did not check its parent")
			}
			cancel()
			a.upgradeMu.Unlock()
			select {
			case err := <-done:
				if err != cause {
					t.Errorf("recovery changed the refusal: %v", err)
				}
			case <-time.After(cutTestWait):
				t.Fatal("recovery did not finish after cancellation")
			}
			if execs != 0 {
				t.Error("recovery dispatched after parent cancellation at the lock")
			}
			if m := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath)); m.State != upgradeStatePending {
				t.Error("canceled recovery changed the marker")
			}
			cutFinishTask(t, a, task)
		})
	}
	testharness.AssertNoFDLeak(t, "parent recheck x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "parent recheck x5", beforeG)
}

type rollbackParentCheck struct {
	context.Context
	once    sync.Once
	checked chan struct{}
}

func (ctx *rollbackParentCheck) Err() error {
	err := ctx.Context.Err()
	ctx.once.Do(func() { close(ctx.checked) })
	return err
}

// origin: hot-update S3 plan §0 exit 3 / §1.6. Early bootstrap refusal
// sends the same bundle, and must emit the redispatch receipt before exec.
func TestBootstrapFailureEmitsRedispatchReceipt(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := newBootAgent(t, "nats://127.0.0.1:1", "node")
			task := startBootTask(t, a)
			frozen := freezeBootAgent(t, a)
			img, _ := cutBootImage(t, a, frozen)
			exe, _, marker := pendingBootMarker(t, a.instanceID, 1)
			oldProof := currentUpgradeBootProof()
			rememberUpgradeBootProof(marker.UpgradeID)
			defer rememberUpgradeBootProof(oldProof)
			logs := &rollbackReceipts{}
			old := bootRedispatchExec
			defer func() { bootRedispatchExec = old }()
			calls := 0
			bootRedispatchExec = func(_ *handoff.Prepared, _ []string, _ func(string) error) func(string) error {
				return func(string) error {
					calls++
					state, _ := bootPreparedState(t)
					bundle, err := agentimage.Decode(state)
					if err != nil || len(bundle.Runtime.Tasks) != 1 || bundle.Runtime.Tasks[0].PID != task.pid {
						t.Error("bootstrap redispatch lost its carried child")
					}
					logs.mu.Lock()
					defer logs.mu.Unlock()
					found := 0
					for _, r := range logs.records {
						if r.Message != "agent: image re-dispatched" {
							continue
						}
						found++
						attrs := map[string]slog.Value{}
						r.Attrs(func(a slog.Attr) bool { attrs[a.Key] = a.Value; return true })
						if r.Level != slog.LevelInfo || attrs["generation"].String() != strconv.FormatUint(bundle.Lineage.Generation, 10) || attrs["image"].String() != img.Summary() {
							t.Error("bootstrap redispatch receipt differs from its manifest")
						}
					}
					if found != 1 {
						t.Errorf("bootstrap redispatch receipts=%d, want 1", found)
					}
					return syscall.ENOEXEC
				}
			}
			finishBoot(img, exe, slog.New(logs), errors.New("unknown flag: fixture"), func(string) error { t.Error("bootstrap used bare exec"); return syscall.ENOEXEC })
			if calls != 1 {
				t.Error("bootstrap did not dispatch")
			}
		})
	}
	testharness.AssertNoFDLeak(t, "bootstrap redispatch receipt x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "bootstrap redispatch receipt x5", beforeG)
}

// cutTransactionAdmission binds the real P02 store and P13 route before the
// legacy pending fixture is written. Runtime/registry ports are fail-on-use:
// Prepare must refuse the unresolved legacy journal before any cut or receipt.
// This certifies the admission boundary, not the pending P17 runtime adapter.
// origin: hot-update S8 plan §3.3 / §7.2 / P14 fixture migration.
func cutTransactionAdmission(t *testing.T, a *Agent) func(*nats.Conn, proto.UpgradeForwardedReq) upgradewire.Response {
	t.Helper()
	route := openCutTransactionRoute(t, a, false)
	return func(nc *nats.Conn, candidate proto.UpgradeForwardedReq) upgradewire.Response {
		t.Helper()
		return route.refuse(t, a, nc, candidate)
	}
}

// cutTransactionRoute is one agent's real P02 journal plus the strict P13
// forwarded route into the real agentupgrade endpoint. Its runtime/registry
// ports are fail-on-use, so a request can only be refused or receive an inert
// grant: neither may reach a cut, publication or receipt.
// origin: hot-update S8 plan §3.1 / §3.3 / §7.2 / P14 fixture migration.
type cutTransactionRoute struct {
	key      upgradetx.Key
	store    *upgradestore.Store
	endpoint upgradeport.Endpoint
}

// migrate opens the journal with the legacy-projection reader P17 enables:
// a terminal v1 marker is then admissible while pending/blocked v1 still is
// not. Open itself refuses an unresolved v1 journal, so bind the route before
// the legacy pending fixture is written.
func openCutTransactionRoute(t *testing.T, a *Agent, migrate bool) cutTransactionRoute {
	t.Helper()
	key := upgradetx.Key{DomainID: "watchdog-admission", Component: upgradetx.ComponentAgent, SID: a.cfg.SID, ConfiguredNID: a.cfg.NID, UID: uint32(os.Geteuid())}
	ticks, err := readStartTimeTicks(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	store, err := upgradestore.Open(upgradestore.Options{Path: upgradeMarkerPath(a.cfg.UpgradeExecutablePath), GuardPath: filepath.Join(filepath.Dir(a.cfg.UpgradeExecutablePath), key.DomainID+".guard"), Key: key,
		Owner: upgradetx.Owner{BootID: readBootID(), PID: os.Getpid(), StartTicks: uint64(ticks), InstanceID: a.instanceID}, SourceGeneration: a.imageGeneration, Transition: upgradetx.Transition, AllowLegacyMigration: migrate})
	if err != nil {
		t.Fatal(err)
	}
	return cutTransactionRoute{key: key, store: store, endpoint: agentupgrade.New(store, &struct{ upgradeport.RuntimePort }{}, &struct{ upgradeobserve.Registry }{})}
}

// The scope lists every frozen domain (S8 plan §2.1 / M06), so a refusal can
// only come from the journal, never from an invalid request.
func (r cutTransactionRoute) prepare(t *testing.T, a *Agent, candidate proto.UpgradeForwardedReq) upgradewire.Request {
	t.Helper()
	domains := []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec,
		upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}
	return upgradewire.Request{Op: upgradewire.OpPrepare, Key: r.key, Prepare: upgradetx.PrepareRequest{ClientNonce: upgradeNonce(), ActorFP: candidate.ActorFP, Mode: upgradetx.ModeHot, SourceRelease: "source", TargetRelease: "candidate", SourceSHA: mustSHA(t, a.cfg.UpgradeExecutablePath), TargetSHA: candidate.SHA256, CapabilityDigest: strings.Repeat("c", 64), ExpiresAt: time.Now().Add(upgradebudget.ComponentHandover).UnixNano(), Scope: upgradetx.Scope{ID: upgradeNonce(), Participants: []upgradetx.Key{r.key}, Domains: domains}}}
}

// refuse asserts the actual journal refusal and its safe wire projection, and
// that the unresolved legacy journal is byte-identical afterwards.
func (r cutTransactionRoute) refuse(t *testing.T, a *Agent, nc *nats.Conn, candidate proto.UpgradeForwardedReq) upgradewire.Response {
	t.Helper()
	candidate.ActorFP = "rollback-fixture-actor"
	before, err := os.ReadFile(upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
	if err != nil {
		t.Fatal(err)
	}
	req := r.prepare(t, a, candidate)
	// Assert the actual journal refusal before checking its safe wire projection.
	// The runtime ports above must remain unreachable on this path.
	_, err = r.store.Prepare(r.key, req.Prepare)
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Code != upgradetx.CodeDeferred || refusal.Detail != "legacy journal requires migration" {
		t.Fatal("pending legacy journal admitted a competing transaction")
	}
	resp := r.send(t, a, nc, req, candidate.ActorFP)
	after, err := os.ReadFile(upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
	if err != nil || string(before) != string(after) {
		t.Fatal("transaction admission changed the blocked legacy journal")
	}
	return resp
}

// admit asserts that the strict route issues an inert grant bound to this key,
// actor, nonce, current on-disk source and candidate. It never Starts it.
func (r cutTransactionRoute) admit(t *testing.T, a *Agent, nc *nats.Conn, candidate proto.UpgradeForwardedReq) upgradetx.Grant {
	t.Helper()
	candidate.ActorFP = "rollback-fixture-actor"
	req := r.prepare(t, a, candidate)
	resp := r.send(t, a, nc, req, candidate.ActorFP)
	g := resp.View.Historical.Grant
	if resp.Code != "" || g.Handle.Key != r.key || len(g.Handle.TxID) != 32 || g.ClientNonce != req.Prepare.ClientNonce ||
		g.ActorFP != candidate.ActorFP || g.SourceSHA != req.Prepare.SourceSHA || g.TargetSHA != candidate.SHA256 {
		t.Fatalf("resolved journal did not admit the next transaction: code=%s error=%s", resp.Code, resp.Error)
	}
	return g
}

// send drives one request through the agent's real forwarded dispatcher.
func (r cutTransactionRoute) send(t *testing.T, a *Agent, nc *nats.Conn, req upgradewire.Request, actor string) upgradewire.Response {
	t.Helper()
	oldPorts, oldEndpoint := a.cfg.Upgrade, a.upgradeEndpoint
	a.cfg.Upgrade, a.upgradeEndpoint = &UpgradeDependencies{Key: r.key, HandleWire: agentupgrade.HandleWire}, r.endpoint
	a.setRunCtx(t.Context())
	defer func() { a.cfg.Upgrade, a.upgradeEndpoint = oldPorts, oldEndpoint }()
	// This subscriber stands in for a session's forwarded wildcard. Like
	// session()'s per-session reset, it lifts an ended session's drain barrier
	// only while installed; otherwise dispatch would silently drop the request.
	a.proxyDrainMu.Lock()
	draining := a.proxyDraining
	a.proxyDraining = false
	a.proxyDrainMu.Unlock()
	defer func() {
		a.proxyDrainMu.Lock()
		a.proxyDraining = draining
		a.proxyDrainMu.Unlock()
	}()
	subject := proto.SubjCmdForwarded(a.cfg.SID, nidOf(a), "upgrade-tx")
	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		if !dispatchAgentUpgrade(a, nc, msg) {
			t.Error("transaction was not dispatched")
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	body, err := upgradewire.EncodeForwarded(req, actor)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), upgradebudget.RequestLatency)
	defer cancel()
	msg, err := nc.RequestWithContext(ctx, subject, body)
	if err != nil {
		t.Fatal(err)
	}
	a.upgradeRequestWG.Wait()
	resp, err := upgradewire.DecodeResponse(msg.Data)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}
