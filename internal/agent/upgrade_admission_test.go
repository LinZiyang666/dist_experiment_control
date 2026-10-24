//go:build linux

package agent

import (
	"context"
	"encoding/json"
	"fmt"
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
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go"
)

// Wait for the recovery owner to release its mutex before the test tears down
// shared fixtures. Acquiring the lock observes completion of its critical section.
func waitUpgradeLock(mu *sync.Mutex) {
	mu.Lock()
	defer mu.Unlock()
}

// cutControlSession runs the agent's real session against a.cfg.NATSURL:
// register, forwarded/intake/sys-events subscriptions, reconciliation, Flush
// and the nonce control probe. The returned stop cancels and joins it once.
// origin: hot-update S8 plan §4.2 / M05; S8 FIX-FIXTURES.
func cutControlSession(t *testing.T, a *Agent) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = a.session(ctx)
	}()
	stop := sync.OnceFunc(func() { cancel(); <-done })
	t.Cleanup(stop)
	return stop
}

// origin: hot-update S3 plan §2.3 / §2.5 / §2.6; concurrency review CC-2
//
// origin: hot-update S8 plan §3.1 / §3.3 / §5.2; S8 FIX-FIXTURES. The staged
// ReExecOnly envelope carries no grant and stays refused after resolution;
// what the resolution must release is the journal, so the next strict
// transaction Prepare is admitted.
func TestSuccessfulCommitClearsDeferredRollbackAdmission(t *testing.T) {
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, resolution := range []string{"commit", "watchdog_terminal", "watchdog_missing", "terminal_report", "rollback"} {
			t.Run(fmt.Sprintf("%d/%s", round, resolution), func(t *testing.T) {
				a := cutTestAgent(t)
				cutImportActive(t, a)
				script := []byte("#!/bin/sh\necho '" + proto.VersionLine() + "'\necho '" + proto.ImageABILine() + "'\n")
				if err := os.WriteFile(a.cfg.UpgradeExecutablePath, script, 0755); err != nil {
					t.Fatal(err)
				}
				route := openCutTransactionRoute(t, a, true)
				m := cutPending(t, a)
				a.upgradeBootProofID = m.UpgradeID
				a.procs["busy"] = &procRec{}
				if a.watchdogRollback(a.cfg.UpgradeExecutablePath) != rollbackRetry || !a.rollbackInFlight.Load() {
					t.Fatal("fixture did not reach deferred rollback")
				}
				delete(a.procs, "busy")
				switch resolution {
				case "commit":
					a.commitUpgradeAfterRegister(upgradeStateCommitted)
					if got := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath)); got.State != upgradeStateCommitted {
						t.Fatal("commit did not land")
					}
				case "watchdog_terminal":
					m.State = upgradeStateCommitted
					if err := writeUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath), m); err != nil {
						t.Fatal(err)
					}
					a.watchdogRollback(a.cfg.UpgradeExecutablePath)
				case "watchdog_missing":
					if err := os.Remove(upgradeMarkerPath(a.cfg.UpgradeExecutablePath)); err != nil {
						t.Fatal(err)
					}
					a.watchdogRollback(a.cfg.UpgradeExecutablePath)
				case "terminal_report":
					m.State = upgradeStateRolledBack
					if err := writeUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath), m); err != nil {
						t.Fatal(err)
					}
					a.commitUpgradeAfterRegister(upgradeStateRolledBack)
					if got, err := readUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath)); err != nil || got != nil {
						t.Fatal("terminal report was not cleared")
					}
				case "rollback":
					// Preserve an executable prev so the subsequent cluster leg is valid.
					if err := os.WriteFile(upgradePrevPath(a.cfg.UpgradeExecutablePath), script, 0755); err != nil {
						t.Fatal(err)
					}
					m.PrevSHA = sha256OfBytes(script)
					if err := writeUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath), m); err != nil {
						t.Fatal(err)
					}
					a.watchdogRollback(a.cfg.UpgradeExecutablePath)
				}
				if a.rollbackInFlight.Load() {
					t.Error("resolved upgrade retains rollback admission")
				}
				nc := cutNATS(t)
				response := cutForward(t, a, nc, proto.UpgradeForwardedReq{ReExecOnly: true, SHA256: mustSHA(t, a.cfg.UpgradeExecutablePath)})
				if response.OK || response.Code != "upgrade_deferred" {
					t.Errorf("grantless ReExecOnly was admitted: %+v", response)
				}
				route.admit(t, a, nc, proto.UpgradeForwardedReq{SHA256: sha256OfBytes(script)})
				cutRequireOpen(t, a)
			})
		}
	}
	testharness.AssertNoFDLeak(t, "resolved rollback admission x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "resolved rollback admission x5", beforeG)
}

// origin: hot-update S3 plan §2.3 / §2.5 / §2.6; concurrency review CC-5
//
// origin: hot-update S8 plan §3.3 / §5.2; S8 FIX-FIXTURES. A grantless
// ReExecOnly no longer cuts, so the frozen image is owned by the same
// untracked re-exec owner through a direct cut that holds the admission the
// retired handler handed it. A grantless install arriving meanwhile must still
// neither download nor touch the exec target, and a granted Start through the
// production RuntimePort defers upgrade_in_progress before acceptance.
func TestInstallRefusesWhileReExecOnlyOwnsFrozenImage(t *testing.T) {
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			a := cutTestAgent(t)
			nc := cutNATS(t)
			script := []byte("#!/bin/sh\n# original executable, distinct from the install candidate\necho '" + proto.VersionLine() + "'\necho '" + proto.ImageABILine() + "'\n")
			if err := os.WriteFile(a.cfg.UpgradeExecutablePath, script, 0755); err != nil {
				t.Fatal(err)
			}
			a.cfg.UpgradeRunningImagePath = filepath.Join(t.TempDir(), "running-image")
			if err := os.WriteFile(a.cfg.UpgradeRunningImagePath, script, 0755); err != nil {
				t.Fatal(err)
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			var execs atomic.Int32
			a.cfg.UpgradeExecFn = func(string) error {
				if execs.Add(1) == 1 {
					close(entered)
					<-release
				}
				return syscall.ENOEXEC
			}
			if first := cutForward(t, a, nc, proto.UpgradeForwardedReq{ReExecOnly: true, SHA256: mustSHA(t, a.cfg.UpgradeExecutablePath)}); first.OK || first.Code != "upgrade_deferred" || execs.Load() != 0 {
				t.Fatalf("grantless ReExecOnly cut the image: %+v", first)
			}
			a.upgradeInstallMu.Lock()
			a.gate.close()
			cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
			if err != nil {
				a.gate.open()
				a.upgradeInstallMu.Unlock()
				t.Fatal(err)
			}
			cut.releaseAdmission = a.upgradeInstallMu.Unlock
			a.reExecInPlace(a.cfg.UpgradeExecutablePath, cut, false)
			select {
			case <-entered:
			case <-time.After(cutTestWait):
				t.Fatal("first exec did not reach its barrier")
			}
			if a.upgradeInstallMu.TryLock() {
				a.upgradeInstallMu.Unlock()
				t.Error("ReExecOnly released upgrade admission before exec returned")
			}
			// origin: hot-update S8 plan §3.1 / §5.1 / §7.1; S8 FIX-AGENTWIRE. A
			// granted Start on the production RuntimePort meets the same live cut:
			// it defers with the exact reason before acceptance, any publication
			// action, selector change or exec, and the frozen owners stay its own.
			route := openGrantedRoute(t, a, false)
			grant, receipt := route.publish(t, a, nc, s8Script("granted candidate "+a.instanceID, s8Domains))
			if refused := route.start(t, a, nc, grant, receipt); refused.Code != upgradetx.CodeDeferred || refused.Error != agentupgradeport.ReasonUpgradeInProgress || execs.Load() != 1 {
				t.Fatalf("granted Start overlapped the live cut: code=%s error=%s execs=%d", refused.Code, refused.Error, execs.Load())
			}
			route.requireUnaccepted(t, grant)
			if !a.processRuntime.Frozen() || !a.workloads.Frozen() {
				t.Error("refused granted Start disturbed the live cut")
			}
			route.detach(a)
			hit := make(chan struct{}, 1)
			req, hits := cutServer(t, a, cutCandidate(t, proto.ImageABIVersion), func() { hit <- struct{}{} })
			response := make(chan proto.UpgradeForwardedResp, 1)
			go func() { response <- cutForward(t, a, nc, req) }()
			var second proto.UpgradeForwardedResp
			select {
			case second = <-response:
				if hits.Load() != 0 || mustSHA(t, a.cfg.UpgradeExecutablePath) != sha256OfBytes(script) {
					t.Error("refused install downloaded or mutated the exec target")
				}
				if marker, err := readUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath)); err != nil || marker != nil {
					t.Error("refused install wrote a marker")
				}
				once.Do(func() { close(release) })
			case <-hit:
				cutWait(t, "second install marker while first cut is held", func() bool {
					m, _ := readUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
					return m != nil && m.State == upgradeStatePending
				})
				cutWait(t, "second install replaces first exec target", func() bool {
					sha, _ := sha256OfFile(a.cfg.UpgradeExecutablePath)
					return sha != sha256OfBytes(script)
				})
				t.Error("second install replaced the executable while ReExecOnly still owned the first frozen image")
				once.Do(func() { close(release) })
				select {
				case second = <-response:
				case <-time.After(cutTestWait):
					t.Fatal("second install did not return after releasing the first cut")
				}
			case <-time.After(cutTestWait):
				t.Fatal("second install neither refused nor downloaded")
			}
			if second.OK || second.Code != "upgrade_deferred" {
				t.Errorf("concurrent install response: OK=%v Code=%q, want upgrade_deferred", second.OK, second.Code)
			}
			if second.OK {
				cutWait(t, "second exec failure recovery", func() bool {
					m, _ := readUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
					return m != nil && m.State == upgradeStateRolledBack
				})
				waitUpgradeLock(&a.upgradeMu)
			}
			waitUpgradeLock(&a.upgradeInstallMu)
			waitUpgradeLock(&a.cutMu)
			cutRequireOpen(t, a)
		})
	}
	testharness.AssertNoFDLeak(t, "upgrade admission x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "upgrade admission x5", beforeG)
}

// origin: hot-update S3 plan §2.2 / §2.5. Returning from exec opens the
// workload gate before disk recovery; upgrade admission must span that gap.
//
// origin: hot-update S8 plan §3.1 / §3.3 / §5.2 / §7.2; S8 FIX-FIXTURES. A
// grantless install or ReExecOnly envelope no longer downloads, flips or cuts,
// so the legacy pending journal is the fixture and its own exec/recovery owner
// is driven directly. Across the thawed gap the durable journal must refuse a
// strict transaction Prepare; only the durable rolled_back record admits the
// next one. The granted install leg runs on the production RuntimePort
// (exerciseGrantedInstallLeg; S8 FIX-AGENTWIRE).
func TestUpgradeAdmissionSpansInstallRecovery(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			task := cutStartTask(t, a)
			nc := cutNATS(t)
			route := openCutTransactionRoute(t, a, true)
			exe := a.cfg.UpgradeExecutablePath
			req, hits := cutServer(t, a, cutCandidate(t, proto.ImageABIVersion), nil)
			original := mustSHA(t, exe)
			for _, envelope := range []proto.UpgradeForwardedReq{req, {ReExecOnly: true, SHA256: original}} {
				resp := cutForward(t, a, nc, envelope)
				if _, err := os.Stat(upgradeMarkerPath(exe)); resp.OK || resp.Code != "upgrade_deferred" || hits.Load() != 0 || mustSHA(t, exe) != original || !os.IsNotExist(err) {
					t.Fatalf("grantless envelope reached the install pipeline: %+v", resp)
				}
			}
			m := cutPending(t, a)
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			var execs atomic.Int32
			a.cfg.UpgradeExecFn = func(string) error {
				if execs.Add(1) == 1 {
					close(entered)
					<-release
				}
				return syscall.ENOEXEC
			}
			// The tracked exec owner keeps this admission until disk recovery
			// returns, exactly as the retired forwarded install handed it over.
			a.upgradeInstallMu.Lock()
			a.gate.close()
			cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
			if err != nil {
				a.gate.open()
				a.upgradeInstallMu.Unlock()
				t.Fatal(err)
			}
			cut.releaseAdmission = a.upgradeInstallMu.Unlock
			a.reExecInPlace(exe, cut, true)
			select {
			case <-entered:
			case <-time.After(cutTestWait):
				t.Fatal("install exec did not enter")
			}
			if a.upgradeInstallMu.TryLock() {
				a.upgradeInstallMu.Unlock()
				t.Error("install released admission before exec returned")
			}
			a.upgradeMu.Lock()
			var unlockOnce sync.Once
			defer unlockOnce.Do(a.upgradeMu.Unlock)
			releaseOnce.Do(func() { close(release) })
			cutWait(t, "runtime thaw before blocked disk recovery", func() bool {
				a.gate.mu.Lock()
				defer a.gate.mu.Unlock()
				return !a.gate.closed && !a.processRuntime.Frozen() && !a.workloads.Frozen()
			})
			if a.upgradeInstallMu.TryLock() {
				a.upgradeInstallMu.Unlock()
				t.Error("install released admission before disk recovery")
			}
			second := route.refuse(t, a, nc, req)
			if second.Code != upgradetx.CodeDeferred || second.View.Historical.Grant.Handle.TxID != "" || hits.Load() != 0 ||
				mustSHA(t, exe) != m.NewSHA || mustSHA(t, upgradePrevPath(exe)) != m.PrevSHA {
				t.Errorf("transaction overlapped disk recovery: code=%s error=%s", second.Code, second.Error)
			}
			if legacy := cutForward(t, a, nc, proto.UpgradeForwardedReq{ReExecOnly: true, SHA256: m.NewSHA}); legacy.OK || legacy.Code != "upgrade_deferred" {
				t.Errorf("ReExecOnly overlapped disk recovery: %+v", legacy)
			}
			unlockOnce.Do(a.upgradeMu.Unlock)
			cutWait(t, "install disk recovery", func() bool {
				m, _ := readUpgradeMarker(upgradeMarkerPath(exe))
				return m != nil && m.State == upgradeStateRolledBack
			})
			waitUpgradeLock(&a.upgradeMu)
			waitUpgradeLock(&a.upgradeInstallMu)
			cutRequireOpen(t, a)
			if mustSHA(t, exe) != m.PrevSHA {
				t.Error("disk recovery did not restore the known-good previous image")
			}
			// The refusal above was the unresolved journal, not a closed route.
			route.admit(t, a, nc, req)
			if hits.Load() != 0 || mustSHA(t, exe) != m.PrevSHA {
				t.Error("an inert grant downloaded or changed the recovered image")
			}
			cutFinishTask(t, a, task)
		})
	}
	testharness.AssertNoFDLeak(t, "install recovery admission x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "install recovery admission x5", beforeG)
	// origin: hot-update S8 plan §3.1 / §3.2 / §4.1 / §8.1; S8 FIX-AGENTWIRE.
	// The granted install leg on the production RuntimePort: the accepted
	// Start owns the sequencer, cut and reservation through its returning exec,
	// and the returned exec keeps Current in recovery instead of releasing it.
	beforeFD, beforeG = testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run("granted_install_leg/"+strconv.Itoa(round), func(t *testing.T) { exerciseGrantedInstallLeg(t) })
	}
	testharness.AssertNoFDLeak(t, "granted install leg x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "granted install leg x5", beforeG)
}

// origin: hot-update S3 plan §2.5. Cuts owned outside a forwarded handler
// have no install mutex or pending marker; the closed gate still excludes disk mutation.
//
// origin: hot-update S8 plan §3.3 / §7.2; S8 FIX-FIXTURES. The grantless
// envelope is now refused upgrade_deferred before any gate or cut check; the
// no-download/no-mutation half still holds while the direct cut is live, and
// the production RuntimePort excludes a granted Start the same way.
func TestInstallRefusesFrozenCutBeforeDownload(t *testing.T) {
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			a := cutTestAgent(t)
			nc := cutNATS(t)
			a.gate.close()
			cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
			if err != nil {
				t.Fatal(err)
			}
			var abandonOnce sync.Once
			abandon := func() { abandonOnce.Do(func() { _ = cut.abandon(a) }) }
			defer abandon()
			fetched := make(chan struct{}, 1)
			req, hits := cutServer(t, a, cutCandidate(t, proto.ImageABIVersion), func() { fetched <- struct{}{} })
			original := mustSHA(t, a.cfg.UpgradeExecutablePath)
			replied := make(chan proto.UpgradeForwardedResp, 1)
			go func() { replied <- cutForward(t, a, nc, req) }()
			var response proto.UpgradeForwardedResp
			select {
			case response = <-replied:
			case <-fetched:
				t.Error("install downloaded while another cut owned the runtime")
				abandon()
				select {
				case response = <-replied:
				case <-time.After(cutTestWait):
					t.Fatal("install did not finish after abandoning the cut")
				}
			case <-time.After(cutTestWait):
				t.Fatal("install did not refuse the cut")
			}
			if _, err := os.Stat(upgradeMarkerPath(a.cfg.UpgradeExecutablePath)); response.OK || response.Code != "upgrade_deferred" || hits.Load() != 0 || mustSHA(t, a.cfg.UpgradeExecutablePath) != original || !os.IsNotExist(err) {
				t.Errorf("outstanding cut did not exclude install: %+v", response)
			}
			// origin: hot-update S8 plan §3.1 / §5.1 / §7.1; S8 FIX-AGENTWIRE. A
			// granted Start on the production RuntimePort is excluded by the same
			// direct cut: upgrade_in_progress before acceptance, selector or exec.
			route := openGrantedRoute(t, a, false)
			grant, receipt := route.publish(t, a, nc, s8Script("granted candidate "+a.instanceID, s8Domains))
			a.cfg.UpgradeExecFn = func(string) error { t.Error("granted Start reached exec during a direct cut"); return syscall.ENOEXEC }
			if refused := route.start(t, a, nc, grant, receipt); refused.Code != upgradetx.CodeDeferred || refused.Error != agentupgradeport.ReasonUpgradeInProgress {
				t.Fatalf("granted Start overlapped the direct cut: code=%s error=%s", refused.Code, refused.Error)
			}
			route.requireUnaccepted(t, grant)
			if !a.processRuntime.Frozen() || mustSHA(t, a.cfg.UpgradeExecutablePath) != original {
				t.Error("refused granted Start disturbed the direct cut or the executable")
			}
			route.detach(a)
			abandon()
			cutRequireOpen(t, a)
		})
	}
	testharness.AssertNoFDLeak(t, "direct cut admission x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "direct cut admission x5", beforeG)
}

// origin: hot-update S3 plan §2.1 / §2.6; concurrency verifier CC-V1.
// Real register accepts the carried child, but a failed fsync leaves the
// watchdog responsible for the original transaction and its known-good prev.
//
// origin: hot-update S8 plan §4.2 / M05 / §3.1 / §3.3 / §7.2; S8 FIX-FIXTURES.
// Register alone no longer commits: the real session's subscriptions, Flush
// and nonce probe precede the failing commit write. The competing install is
// a strict transaction Prepare against the real journal (the grantless
// envelope is refused before download), and the production commit point is
// the next control readiness. Only that durable commit admits a new Prepare.
func TestExpiredLiveUpgradeKeepsItsPreviousImageUntilResolution(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, resolution := range []string{"commit", "rollback"} {
			t.Run(fmt.Sprintf("%d/%s", round, resolution), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				nc := cutNATS(t)
				a := cutTestAgent(t)
				a.cfg.NATSURL = nc.ConnectedUrl()
				task := cutStartTask(t, a)
				cutImportActive(t, a)
				route := openCutTransactionRoute(t, a, true)
				first := cutPending(t, a)
				a.upgradeBootProofID = first.UpgradeID
				if _, err := nc.Subscribe(proto.SubjNodeRegister(a.cfg.SID, nidOf(a)), func(msg *nats.Msg) {
					body, _ := json.Marshal(proto.NodeRegisterResp{OK: true, AcceptedProcesses: []string{task.pid}})
					_ = msg.Respond(body)
				}); err != nil {
					t.Fatal(err)
				}
				if err := nc.FlushTimeout(time.Second); err != nil {
					t.Fatal(err)
				}
				previousObserver := upgradeSyncObserver
				defer func() { upgradeSyncObserver = previousObserver }()
				var writesFailed atomic.Int32
				upgradeSyncObserver = func(kind, path string) error {
					if kind == "file" && strings.HasPrefix(filepath.Base(path), ".tether-upgrade-marker-") {
						writesFailed.Add(1)
						return syscall.EIO
					}
					return nil
				}
				stopSession := cutControlSession(t, a)
				cutWait(t, "control-ready after the failed commit write", func() bool {
					return writesFailed.Load() == 1 && a.image.Phase() == agentboot.PhaseReady && a.controlReady.Load() != nil
				})
				stopSession()
				upgradeSyncObserver = previousObserver
				if writesFailed.Load() != 1 {
					t.Fatalf("control-ready fault premise: failed_writes=%d", writesFailed.Load())
				}
				if a.upgradeNow().Before(first.Deadline) || a.rollbackInFlight.Load() || a.image.Phase() != agentboot.PhaseReady {
					t.Fatal("expired pending + ready premise missing")
				}
				markerPath := upgradeMarkerPath(a.cfg.UpgradeExecutablePath)
				before, err := os.ReadFile(markerPath)
				if err != nil {
					t.Fatal(err)
				}
				current := readMarkerState(t, markerPath)
				if current.State != upgradeStatePending || current.UpgradeID != first.UpgradeID {
					t.Fatal("register did not leave the first transaction pending")
				}
				req, hits := cutServer(t, a, cutCandidate(t, proto.ImageABIVersion), nil)
				legacy := cutForward(t, a, nc, req)
				resp := route.refuse(t, a, nc, req)
				after, err := os.ReadFile(markerPath)
				if err != nil {
					t.Fatal(err)
				}
				if legacy.OK || legacy.Code != "upgrade_deferred" || resp.Code != upgradetx.CodeDeferred || resp.View.Historical.Grant.Handle.TxID != "" ||
					hits.Load() != 0 || string(before) != string(after) || mustSHA(t, a.cfg.UpgradeExecutablePath) != first.NewSHA || mustSHA(t, upgradePrevPath(a.cfg.UpgradeExecutablePath)) != first.PrevSHA {
					t.Errorf("expired live transaction lost its admission/prev: legacy=%+v code=%s downloads=%d marker_changed=%v", legacy, resp.Code, hits.Load(), string(before) != string(after))
				}
				if resolution == "commit" {
					stopSession = cutControlSession(t, a)
					cutWait(t, "durable commit at the next control readiness", func() bool {
						m, _ := readUpgradeMarker(markerPath)
						return m != nil && m.State == upgradeStateCommitted && a.controlReady.Load() != nil
					})
					stopSession()
					if m := readMarkerState(t, markerPath); m.State != upgradeStateCommitted {
						t.Fatal("commit did not resolve the pending transaction")
					}
					if grant := route.admit(t, a, nc, req); grant.SourceSHA != first.NewSHA || hits.Load() != 0 {
						t.Errorf("durable commit did not release transaction admission: source=%s downloads=%d", grant.SourceSHA, hits.Load())
					}
				} else {
					if a.watchdogRollback(a.cfg.UpgradeExecutablePath) != rollbackDone {
						t.Fatal("watchdog did not resolve the pending transaction")
					}
					if mustSHA(t, a.cfg.UpgradeExecutablePath) != first.PrevSHA {
						t.Error("watchdog no longer restored the original known-good image")
					}
				}
				cutRequireOpen(t, a)
				cutFinishTask(t, a, task)
			})
		}
	}
	testharness.AssertNoFDLeak(t, "expired live pending transaction x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "expired live pending transaction x5", beforeG)
}

// origin: hot-update S3 plan §2.1; concurrency verifier CC-V1.
// A stale marker remains replaceable without matching live ownership evidence.
//
// origin: hot-update S8 plan §3.1 / §3.3 / §7.2; S8 FIX-FIXTURES. Intentional
// change: every unresolved v1 pending journal occupies Current whether or not
// its owner is known, and pending is never GC'd. A new strict Prepare is
// refused for each ownership shape, so the old recorded prev cannot be
// overwritten; an orphan is resolved by its own boot/watchdog recovery or the
// P17 migration, never by the next install. The grantless envelope stays
// refused before download in every shape.
func TestExpiredPendingAdmissionRequiresLiveOwnership(t *testing.T) {
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, tc := range []struct {
			name                        string
			carry, proof, foreign, live bool
		}{
			{"boot_proof", false, true, false, true},
			{"carried_runtime", true, false, false, true},
			{"orphan", false, false, false, false},
			{"empty_proof", false, false, false, false},
			{"foreign_target", true, true, true, false},
		} {
			t.Run(fmt.Sprintf("%d/%s", round, tc.name), func(t *testing.T) {
				a := cutTestAgent(t)
				if tc.carry {
					cutImportActive(t, a)
					a.image.MarkReady()
				}
				route := openCutTransactionRoute(t, a, true)
				m := cutPending(t, a)
				if tc.proof {
					a.upgradeBootProofID = m.UpgradeID
				} else {
					a.upgradeBootProofID = "another-transaction"
				}
				if tc.name == "empty_proof" {
					m.UpgradeID = ""
					a.upgradeBootProofID = ""
				}
				if tc.foreign {
					m.TargetNID = "another-node"
				}
				markerPath := upgradeMarkerPath(a.cfg.UpgradeExecutablePath)
				if err := writeUpgradeMarker(markerPath, m); err != nil {
					t.Fatal(err)
				}
				req, hits := cutServer(t, a, cutCandidate(t, proto.ImageABIVersion), nil)
				nc := cutNATS(t)
				legacy := cutForward(t, a, nc, req)
				resp := route.refuse(t, a, nc, req)
				after := readMarkerState(t, markerPath)
				if legacy.OK || legacy.Code != "upgrade_deferred" || resp.Code != upgradetx.CodeDeferred || resp.View.Historical.Grant.Handle.TxID != "" ||
					hits.Load() != 0 || after.UpgradeID != m.UpgradeID || mustSHA(t, upgradePrevPath(a.cfg.UpgradeExecutablePath)) != m.PrevSHA {
					t.Errorf("pending journal (live=%t) lost Current: legacy=%+v code=%s", tc.live, legacy, resp.Code)
				}
				cutRequireOpen(t, a)
			})
		}
	}
	testharness.AssertNoFDLeak(t, "pending admission ownership x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "pending admission ownership x5", beforeG)
}
