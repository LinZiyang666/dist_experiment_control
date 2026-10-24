//go:build linux

// upgrade_image_test.go exercises real children, process tables, handoff
// descriptors and forwarded requests over embedded NATS. Only the final exec
// and artifact server are controlled; same-PID bootstrap is covered separately.
// origin: hot-update S3 plan §2 / §4 / §5.2 / §7.
package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/workload"

	"github.com/nats-io/nats.go"
)

const cutTestWait = 10 * time.Second

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT test 1 (drill 36.A).
func TestPreflightAllowsInTimeSpawnBeforeQuiescingCut(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) { exercisePreflightInTimeSpawn(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestPreflightAllowsInTimeSpawnBeforeQuiescingCut", beforeG)
	testharness.AssertNoFDLeak(t, "TestPreflightAllowsInTimeSpawnBeforeQuiescingCut", beforeFD, 0)
}

func exercisePreflightInTimeSpawn(t *testing.T) {
	a := cutTestAgent(t)
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	resume := sync.OnceFunc(func() { close(release) })
	t.Cleanup(func() { resume(); <-done })
	go func() {
		done <- a.spawnPolicy.RunStart(func() error {
			close(entered)
			<-release
			return nil
		}, cutTestWait)
	}()
	select {
	case <-entered:
	case <-time.After(cutTestWait):
		t.Fatal("spawn did not acquire its slot")
	}
	if reasons := preflightDeferrals(a); len(reasons) != 0 {
		t.Fatalf("preflight rejected an in-time operation: %v", reasons)
	}
	a.gate.close()
	type result struct {
		cut *imageCut
		err error
	}
	frozen := make(chan result, 1)
	go func() {
		cut, err := freezeForUpgrade(a, time.Now().Add(cutTestWait))
		frozen <- result{cut, err}
	}()
	select {
	case got := <-frozen:
		t.Fatalf("cut returned before the admitted slot drained: %v", got.err)
	case <-time.After(30 * time.Millisecond):
	}
	resume()
	select {
	case got := <-frozen:
		if got.err != nil {
			t.Fatal(got.err)
		}
		if err := got.cut.abandon(a); err != nil {
			t.Fatal(err)
		}
		if releaseAdmission, ok := a.gate.admit(gateExec); !ok {
			t.Fatal("rollback retained legacy admission")
		} else {
			releaseAdmission()
		}
	case <-time.After(cutTestWait):
		t.Fatal("cut did not resume after the spawn finished")
	}
}

func cutTestAgent(t *testing.T) *Agent {
	t.Helper()
	a, err := New(Config{SID: "lab", NID: "cut-agent", Home: t.TempDir(), NATSURL: "nats://127.0.0.1:1",
		Logger: testharness.SilentLog(), RemoteFSMode: "off", UpgradeNoExit: true})
	if err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(t.TempDir(), "tether")
	if err := os.WriteFile(dst, []byte("original image"), 0755); err != nil {
		t.Fatal(err)
	}
	a.cfg.UpgradeExecutablePath, a.cfg.UpgradeRunningImagePath = dst, dst
	a.pushCommitCache = map[string]pushCommitEntry{}
	t.Cleanup(func() {
		// origin: hot-update S5 PROC-LEAK. A discarded exporter may keep
		// only table ownership; fixture teardown must kill those groups too.
		for _, row := range a.workloads.Snapshot() {
			_ = syscall.Kill(-row.OSPID, syscall.SIGKILL)
		}
		a.workloads.Thaw()
		if a.processRuntime != nil {
			if a.processRuntime.Frozen() {
				_ = a.processRuntime.Thaw()
			}
			ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), cutTestWait)
			defer cancel()
			if err := a.processRuntime.Close(ctx); err != nil {
				t.Error(err)
			}
		}
	})
	return a
}

func cutWait(t *testing.T, what string, pred func() bool) {
	t.Helper()
	if !testharness.WaitFor(t, cutTestWait, time.Millisecond, pred) {
		t.Fatalf("timed out: %s", what)
	}
}

func cutNATS(t *testing.T) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(startNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func cutForward(t *testing.T, a *Agent, nc *nats.Conn, req proto.UpgradeForwardedReq) proto.UpgradeForwardedResp {
	t.Helper()
	subject := nats.NewInbox()
	done := make(chan struct{})
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) { defer close(done); a.handleUpgradeForwarded(nc, m) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	data, _ := json.Marshal(req)
	msg, err := nc.Request(subject, data, cutTestWait+upgradebudget.SafePointWait)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(cutTestWait):
		t.Fatal("forwarded handler did not return")
	}
	var resp proto.UpgradeForwardedResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func cutCandidate(t *testing.T, abi int) []byte {
	t.Helper()
	return testTarball(t, []byte(cutCandidateScript(abi)))
}

func cutCandidateScript(abi int) string {
	script := "#!/bin/sh\necho '" + proto.VersionLine() + "'\n"
	if abi > 0 {
		script += fmt.Sprintf("echo 'image-abi v%d'\n", abi)
	}
	return script
}

// cutCandidateReader is the isolated version probe of the exact candidate
// bytes, the reader a granted Preflight validates (S8 plan §3.3 / §6.1).
func cutCandidateReader(t *testing.T, abi int) agentimage.Capabilities {
	t.Helper()
	script := cutCandidateScript(abi)
	_, reader, err := smokeVersionCapabilities(writeScript(t, script))
	if err != nil || reader.ABI != abi || reader.SHA256 != sha256OfBytes([]byte(script)) {
		t.Fatalf("candidate version probe: abi=%d sha=%s err=%v", reader.ABI, reader.SHA256, err)
	}
	return reader
}

func cutServer(t *testing.T, a *Agent, body []byte, during func()) (proto.UpgradeForwardedReq, *atomic.Int32) {
	t.Helper()
	hits := &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		if during != nil {
			during()
		}
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	a.cfg.UpgradeURLAllowlist = []string{srv.URL}
	return proto.UpgradeForwardedReq{URL: srv.URL, SHA256: sha256OfBytes(body)}, hits
}

func cutRequireOpen(t *testing.T, a *Agent) {
	t.Helper()
	release, ok := a.gate.admit(gateExec)
	if !ok {
		t.Fatal("upgrade gate still closed")
	}
	release()
	if a.processRuntime != nil && a.processRuntime.Frozen() || a.workloads.Frozen() {
		t.Fatal("business owners still frozen")
	}
}

type cutExposeCount struct {
	noopExpose
	sessions int
}

func (c cutExposeCount) SessionCount() int { return c.sessions }

// origin: hot-update S5 plan §7.3 / T37; hot-update S3 plan §4.1 / T6. Every agent-owned busy source is
// manufactured through its owning API or locked state, then refused through
// both Freeze and a real forwarded request before the artifact is fetched.
func TestPreflightDefersOnEachReasonCode(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	nc := cutNATS(t)
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("owned_v1/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseOwnedFlowUpgradeAdmission(t)
		})
	}
	t.Run("active_reason_set", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		// origin: hot-update S4 plan §7.1 / T29. The code is retired from the
		// public deferred set as well as from the live PTY busy predicate.
		want := []string{agentimage.ReasonSpawnInFlight, agentimage.ReasonRemoteFSProbeInFlight, agentimage.ReasonForeignProcessRows, agentimage.ReasonImageTooLarge, agentimage.ReasonRuntimeClosed, agentimage.ReasonPlatformUnsupported, agentimage.ReasonTransfersInFlight, agentimage.ReasonLegacySessionBusy, ssStateUnrepresentable}
		if strings.Join(agentimage.DeferredReasons[:], ",") != strings.Join(want, ",") {
			t.Fatal("active deferred reasons must contain exactly the nine S5 codes (pending P17)")
		}
	})

	cases := []struct {
		name, reason string
		busy         func(*testing.T, *Agent)
	}{
		{"pty", agentimage.ReasonLegacySessionBusy, func(_ *testing.T, a *Agent) { a.procs["pty"] = &procRec{} }},
		{"run_admission", agentimage.ReasonLegacySessionBusy, func(t *testing.T, a *Agent) {
			release, _ := a.gate.admit(gateRun)
			t.Cleanup(release)
		}},
		{"legacy_admission", agentimage.ReasonLegacySessionBusy, func(t *testing.T, a *Agent) {
			release, _ := a.gate.admit(gateExec)
			t.Cleanup(release)
		}},
		{"legacy_child", agentimage.ReasonLegacySessionBusy, func(t *testing.T, a *Agent) {

			cmd := exec.Command("/bin/sleep", "30")
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			if err := a.workloads.AdoptDraining(workload.Identity{ID: "foreign-child", OSPID: cmd.Process.Pid, StartedAt: time.Now()}, cmd.Process); err != nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
				t.Fatal(err)
			}
			t.Cleanup(func() {
				_ = a.workloads.Signal("foreign-child", syscall.SIGKILL)
				_ = a.workloads.AllowReap("foreign-child")
				_, _ = a.workloads.Wait("foreign-child")
				_ = a.workloads.Forget("foreign-child")
			})
		}},
		{"expose", agentimage.ReasonLegacySessionBusy, func(_ *testing.T, a *Agent) { a.cfg.ExposeAdapter = cutExposeCount{sessions: 1} }},
		{"uninitialized_adapter", agentimage.ReasonLegacySessionBusy, func(_ *testing.T, a *Agent) { a.cfg.ExposeAdapter = &TunnelExposeAdapter{} }},
		{"unknown_adapter", agentimage.ReasonLegacySessionBusy, func(_ *testing.T, a *Agent) { a.cfg.ExposeAdapter = cutExposeCount{} }},
		{"unknown_expose_admission", agentimage.ReasonLegacySessionBusy, func(t *testing.T, a *Agent) {

			a.cfg.ExposeAdapter = cutExposeCount{}
			release, ok := a.gate.admit(gateExpose)
			if !ok {
				t.Fatal("fixture admission refused")
			}
			t.Cleanup(release)
		}},
		{"rehome", agentimage.ReasonLegacySessionBusy, func(_ *testing.T, a *Agent) { a.rehomeRunning[1234] = true }},
		{"proxy", ssStateUnrepresentable, func(t *testing.T, a *Agent) {

			srv := ssproxy.New(a.cfg.Logger)
			if _, err := srv.Start(0, nil); err != nil {
				t.Fatal(err)
			}
			a.proxy.srv = srv
			// origin: hot-update S5 plan §7.2 / T37. An idle listener is
			// representable. A real legacy timer without an absolute BOOTTIME
			// deadline cannot be reconstructed without renewing its grace.
			a.flcTimer = time.AfterFunc(time.Hour, func() {})
			t.Cleanup(func() { a.flcTimer.Stop(); a.flcTimer = nil })
			t.Cleanup(srv.Stop)
		}},
		{"transfer_admission", agentimage.ReasonTransfersInFlight, func(t *testing.T, a *Agent) {
			release, _ := a.gate.admit(gateTransfer)
			t.Cleanup(release)
		}},
		{"expose_admission", agentimage.ReasonTransfersInFlight, func(t *testing.T, a *Agent) {
			release, _ := a.gate.admit(gateExpose)
			t.Cleanup(release)
		}},
		{"push_commit", agentimage.ReasonTransfersInFlight, func(_ *testing.T, a *Agent) { a.pushCommitCache["pending"] = pushCommitEntry{added: time.Now()} }},
		{"home_read", agentimage.ReasonRemoteFSProbeInFlight, func(_ *testing.T, a *Agent) { a.homeReadInFlight = true }},
		{"remote_probe", agentimage.ReasonRemoteFSProbeInFlight, func(t *testing.T, a *Agent) {

			release, done := make(chan struct{}), make(chan struct{})
			policy, err := spawnsafe.New(spawnsafe.Config{MountSource: func() ([]byte, error) {
				return fakeMountinfo([2]string{"/", "ext4"}, [2]string{"/cut-probe", "nfs4"}), nil
			}, Probe: func(string) bool { <-release; return false }, ProbeTimeout: upgradebudget.RequestLatency})
			if err != nil {
				t.Fatal(err)
			}
			a.spawnPolicy = policy
			go func() {
				defer close(done)
				_, _ = policy.Prepare([]string{"/bin/true"}, "/cut-probe", "/usr/bin", nil, false)
			}()
			t.Cleanup(func() {
				close(release)
				<-done
				cutWait(t, "probe released", func() bool { return policy.Census().ProbesInFlight == 0 })
			})
			cutWait(t, "probe in flight", func() bool { return policy.Census().ProbesInFlight > 0 })
		}},
		{"image_too_large", agentimage.ReasonImageTooLarge, func(t *testing.T, a *Agent) {

			stopProcessRuntime(t.Context(), a)
			cfg := processRuntimeConfig(a)
			cfg.ReplayCapacity = 16
			cfg.Instance = "large-runtime"
			var clockShift atomic.Int64
			cfg.Clock = func() (int64, error) {
				now, err := procrequest.BootNow()
				return now + clockShift.Load(), err
			}
			rt, err := execsession.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			a.processRuntime = rt
			digest := sha256.Sum256([]byte("large image actor"))
			scope := procrequest.Scope{SID: a.cfg.SID, NID: a.cfg.NID, ActorFP: "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:]), ClientLineage: "large-client"}
			probe, err := cutHandle(a, scope, proto.ProcessResumeReq{Op: "probe"})
			if err != nil {
				t.Fatal(err)
			}
			// The OS rejects each missing executable. Terminal owners retain
			// tiny real replay memfds; no live process farm is manufactured.
			missing := filepath.Join(t.TempDir(), "missing-executable")
			for i := 0; i < 2731; i++ {
				// Exercise lease rollover independently of machine load; no
				// elapsed wall time is required to prove the refreshed admission.
				// origin: hot-update S5 plan §7.3 / T37
				if i == 128 {
					clockShift.Store(int64(procrequest.DefaultTTL + time.Second))
				}
				// The descriptor-bound fixture can outlast a create lease under
				// concurrent race builds. Refresh only setup admission; every
				// completed terminal remains retained for the original size check.
				// origin: hot-update S5 plan §7.3 / T37
				if i%128 == 0 {
					probe, err = cutHandle(a, scope, proto.ProcessResumeReq{Op: "probe"})
					if err != nil || probe.Lease == nil {
						t.Fatal("size fixture could not refresh create admission")
					}
				}
				h := proto.ProcessHandle{Lease: *probe.Lease, RequestID: fmt.Sprintf("missing-%d", i)}
				if _, err := cutHandle(a, scope, proto.ProcessResumeReq{Op: "create", Handle: &h, AttachEpoch: 1, Exec: &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{missing}}}); err != nil {
					t.Fatal(err)
				}
				deadline := time.Now().Add(cutTestWait)
				for {
					reply, err := cutHandle(a, scope, proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1})
					if err != nil {
						t.Fatal(err)
					}
					if reply.Phase == "terminal" {
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("missing executable did not terminate")
					}
					runtime.Gosched()
				}
			}
		}},
		{"wedged_spawn", agentimage.ReasonSpawnInFlight, func(t *testing.T, a *Agent) {

			// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT test 2.
			// Preflight vetoes reaper ownership, not a healthy raw acquired slot.
			release := make(chan struct{})
			if err := a.spawnPolicy.RunStart(func() error { <-release; return nil }, time.Millisecond); !errors.Is(err, spawnsafe.ErrSpawnTimeout) {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				close(release)
				cutWait(t, "abandoned spawn reaped", func() bool { return a.spawnPolicy.WedgedCount() == 0 })
			})
		}},
		{"runtime_closed", agentimage.ReasonRuntimeClosed, func(t *testing.T, a *Agent) {
			stopProcessRuntime(t.Context(), a)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			tc.busy(t, a)
			req, hits := cutServer(t, a, cutCandidate(t, proto.ImageABIVersion), nil)
			before := mustSHA(t, a.cfg.UpgradeExecutablePath)
			reasons := preflightDeferrals(a)
			if len(reasons) != 1 || reasons[0].Reason != tc.reason || reasons[0].Detail == "" {
				t.Fatalf("preflight=%v, want only %s with detail", reasons, tc.reason)
			}
			frozen, err := agentimage.Freeze(imageOwners(a), time.Now().Add(upgradebudget.SafePointWait))
			var deferred *agentimage.DeferredError
			if frozen != nil || !errors.As(err, &deferred) || deferred.Reason != tc.reason {
				t.Fatalf("Freeze returned cut=%t error=%v, want %s", frozen != nil, err, tc.reason)
			}
			if a.processRuntime.Frozen() || a.workloads.Frozen() {
				t.Fatal("deferral froze an owner")
			}
			// origin: hot-update S8 plan §3.3 / §7.1 / §7.2; S8 FIX-FIXTURES. The
			// grantless envelope is refused before preflight runs, so its reply
			// cannot name a reason; the exact code is asserted on preflight and
			// Freeze above, and on the wire by the granted path below.
			resp := cutForward(t, a, nc, req)
			if resp.OK || resp.Code != "upgrade_deferred" {
				t.Fatalf("reply=%+v", resp)
			}
			if hits.Load() != 0 || mustSHA(t, a.cfg.UpgradeExecutablePath) != before {
				t.Fatal("preflight downloaded or changed dst")
			}
			for _, p := range []string{upgradeMarkerPath(a.cfg.UpgradeExecutablePath), upgradePrevPath(a.cfg.UpgradeExecutablePath)} {
				if _, err := os.Stat(p); !os.IsNotExist(err) {
					t.Fatalf("preflight changed %s: %v", p, err)
				}
			}
			// origin: hot-update S8 plan §7.1; S8 FIX-FIXTURES R2; S8 FIX-AGENTWIRE.
			// A granted Start on the production RuntimePort carries the exact
			// reason on the wire (upgrade_deferred, 75) before acceptance.
			route := openGrantedRoute(t, a, false)
			grant, receipt := route.publish(t, a, nc, s8Script("deferred "+tc.name+" "+a.instanceID, s8Domains))
			if granted := route.start(t, a, nc, grant, receipt); granted.Code != upgradetx.CodeDeferred || granted.Error != tc.reason {
				t.Fatalf("granted deferral on the wire: code=%s error=%q, want %s", granted.Code, granted.Error, tc.reason)
			}
			route.requireUnaccepted(t, grant)
		})
	}
	t.Run("all_hits", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		a := cutTestAgent(t)
		a.procs["pty"] = &procRec{}
		a.homeReadInFlight = true
		a.cfg.ExposeAdapter = cutExposeCount{sessions: 1}
		got := preflightDeferrals(a)
		// Both process and unknown-adapter responsibility must survive under
		// the shared legacy reason, alongside the independent home probe.
		if len(got) != 2 || got[0].Reason != agentimage.ReasonRemoteFSProbeInFlight || got[1].Reason != agentimage.ReasonLegacySessionBusy || !strings.Contains(got[1].Detail, "legacy sessions") || !strings.Contains(got[1].Detail, "flow ownership") {
			t.Fatalf("preflight dropped owner details: %v", got)
		}
	})
	t.Run("expired_prep", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		a := cutTestAgent(t)
		a.pushCommitCache["expired"] = pushCommitEntry{added: time.Now().Add(-pushCommitEntryTTL(0))}
		if got := preflightDeferrals(a); len(got) != 0 {
			t.Fatalf("expired prep defers: %v", got)
		}
	})
	// origin: hot-update S4 plan §7.1 / T29. Exercise real un-attached and
	// spawned legacy owners; the table above also isolates the Manager-only
	// adoption/draining handover after an admission slot is released.
	for round := 0; round < 5; round++ {
		for _, kind := range []string{"exec", "run"} {
			t.Run(fmt.Sprintf("legacy_lifetime/%d/%s", round, kind), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				cutExerciseLegacyAdmission(t, nc, kind)
			})
		}
		t.Run(fmt.Sprintf("legacy_four_stages/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			domainExerciseLegacyStages(t, nc)
		})
		t.Run(fmt.Sprintf("legacy_during_download/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			domainExerciseLateLegacyRun(t, nc)
		})
		t.Run(fmt.Sprintf("reliable_pty_cut/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			task := domainStartPTY(t, a, nil)
			if reasons := preflightDeferrals(a); len(reasons) != 0 {
				t.Fatalf("reliable PTY is counted as legacy: %v", reasons)
			}
			a.gate.close()
			cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
			if err != nil {
				t.Fatal(err)
			}
			if err := cut.abandon(a); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(task.release, nil, 0600); err != nil {
				t.Fatal(err)
			}
			domainWaitTerminal(t, a, task)
		})
	}
	testharness.AssertNoFDLeak(t, "legacy and reliable preflight x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "legacy and reliable preflight x5", beforeG)

}

// origin: hot-update S3 plan §2.1 / T7. The real download window admits
// late work; the subsequent cut must undo the flip before replying.
//
// origin: hot-update S8 plan §3.3 / §4.1 / §7.2; S8 FIX-FIXTURES. The grantless
// envelope no longer downloads, so it opens no window and flips nothing. The
// late-admission window is now between the read-only preflight/candidate
// policy and Freeze: work admitted there must still fail the real final cut
// with the same private-text-free class, before any disk effect, and leave the
// runtime thawed. A post-select failure of a granted Start runs on the
// production RuntimePort (exerciseGrantedPostSelectFailure; S8 FIX-AGENTWIRE).
func TestUpgradeCutFailureAfterFlipRollsDiskBack(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	nc := cutNATS(t)
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, kind := range []string{"pty", "table_frozen", "abi_late"} {
			t.Run(fmt.Sprintf("%d/%s", round, kind), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				a := cutTestAgent(t)
				if kind == "table_frozen" {
					cutStartTask(t, a)
				}
				dst := a.cfg.UpgradeExecutablePath
				old := mustSHA(t, dst)
				abi := proto.ImageABIVersion
				if kind == "abi_late" {
					abi = 0
				}
				req, hits := cutServer(t, a, cutCandidate(t, abi), nil)
				if resp := cutForward(t, a, nc, req); resp.OK || resp.Code != "upgrade_deferred" || hits.Load() != 0 {
					t.Fatalf("grantless envelope opened a download window: %+v", resp)
				}
				reader := cutCandidateReader(t, abi)
				if reasons := preflightDeferrals(a); len(reasons) != 0 {
					t.Fatalf("fixture busy before the late admission: %v", reasons)
				}
				// The first empty-image ABI check admits a bare-exec candidate.
				if err := checkCandidateABI(a, reader); err != nil {
					t.Fatalf("candidate policy refused before the late admission: %v", err)
				}
				switch kind {
				case "pty":
					a.procsMu.Lock()
					a.procs["late"] = &procRec{}
					a.procsMu.Unlock()
				case "table_frozen":
					_, _ = a.workloads.Freeze()
					// A revocation under a foreign manager freeze cannot
					// deliver its kill until that table's owner thaws it.
					_, _ = a.processRuntime.Revoke(a.cfg.SID, "external table fence")
				case "abi_late":
					// A revocation arriving after the first ABI check must
					// survive to the final cut's reader gate.
					if _, err := a.processRuntime.Revoke(a.cfg.SID, "private late revocation detail"); err != nil {
						t.Fatal(err)
					}
				}
				a.gate.close()
				cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
				if err == nil {
					err = agentimage.CheckBundleCapabilities(cut.frozen.Bundle(), reader)
					if thawErr := cut.abandon(a); thawErr != nil {
						t.Fatal(thawErr)
					}
				} else if !a.processRuntime.Frozen() {
					a.gate.open()
				}
				// origin: hot-update S3 plan §2.1 step 9 / TD-7(b). Report only
				// the classified failure, never the underlying private error text.
				var deferred *agentimage.DeferredError
				switch kind {
				case "pty":
					if !errors.As(err, &deferred) || deferred.Reason != agentimage.ReasonLegacySessionBusy {
						t.Fatalf("late legacy session reached the cut: %v", err)
					}
				case "table_frozen":
					if err == nil || cutFailureClass(err) != "frozen_table" {
						t.Fatalf("foreign table freeze class=%v", err)
					}
				case "abi_late":
					if !errors.Is(err, errImageABIUnsupported) || cutFailureClass(err) != "abi_late" {
						t.Fatalf("late revocation reached a bare-exec candidate: %v", err)
					}
				}
				if mustSHA(t, dst) != old {
					t.Fatal("cut failure left new bytes on disk")
				}
				for _, path := range []string{upgradePrevPath(dst), upgradeMarkerPath(dst)} {
					if _, err := os.Stat(path); !os.IsNotExist(err) {
						t.Fatalf("cut failure left %s behind: %v", filepath.Base(path), err)
					}
				}
				if a.processRuntime.Frozen() {
					t.Fatal("failed cut stranded runtime frozen")
				}
				a.workloads.Thaw()
				cutRequireOpen(t, a)
			})
		}
	}
	testharness.AssertNoFDLeak(t, "post-flip cut failures x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "post-flip cut failures x5", beforeG)
	// origin: hot-update S8 plan §4.1 / §5.2 / §8.1; S8 FIX-AGENTWIRE. A granted
	// Start whose final re-export fails after the select/activation intents
	// keeps the source selector, both pins and Current, and thaws the owners.
	beforeFD, beforeG = testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, kind := range []string{"abi_late", "target_replaced"} {
			t.Run(fmt.Sprintf("granted_post_select_failure/%d/%s", round, kind), func(t *testing.T) { exerciseGrantedPostSelectFailure(t, kind) })
		}
	}
	testharness.AssertNoFDLeak(t, "granted post-select failures x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "granted post-select failures x5", beforeG)
}

// origin: hot-update S3 plan §2.1 step 9 / TD-7(b). Errors may carry
// arbitrary paths, identities or image data; marker classes never do.
func TestCutFailureClassesExcludePrivateErrorText(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"frozen_table", fmt.Errorf("private table detail: %w", workload.ErrFrozen), "frozen_table"},
		{"abi_late", fmt.Errorf("private candidate detail: %w", errImageABIUnsupported), "abi_late"},
		{"bundle", fmt.Errorf("private encode detail: %w", agentimage.ErrBundle), "bundle"},
		{"unknown", errors.New("private untyped cut failure"), "image_cut"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := cutFailureClass(tc.err); got != tc.want {
				t.Errorf("failure class=%q, want %q", got, tc.want)
			}
		})
	}
}

type cutTask struct {
	scope        procrequest.Scope
	handle       proto.ProcessHandle
	pid, release string
	stdoutACK    uint64
}

func cutHandle(a *Agent, scope procrequest.Scope, req proto.ProcessResumeReq) (proto.ProcessResumeReply, error) {
	req.Version, req.CallID, req.ActorFP = proto.ProcessResumeVersion, 1, scope.ActorFP
	if req.Op == "probe" {
		req.ClientLineage = scope.ClientLineage
	}
	return a.processRuntime.Handle(scope, req, "_TINBOX.cut")
}

func cutStartTask(t *testing.T, a *Agent) cutTask {
	t.Helper()
	digest := sha256.Sum256([]byte("cut actor"))
	scope := procrequest.Scope{SID: a.cfg.SID, NID: a.cfg.NID, ActorFP: "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:]), ClientLineage: "cut-client"}
	probe, err := cutHandle(a, scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, Op: "probe"})
	if err != nil || probe.Lease == nil {
		t.Fatalf("probe: %+v %v", probe, err)
	}
	h := proto.ProcessHandle{Lease: *probe.Lease, RequestID: "cut-task"}
	release := filepath.Join(t.TempDir(), "release")
	req := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, Op: "create", Handle: &h, AttachEpoch: 1, Exec: &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "printf 'before\\n'; while [ ! -e \"$1\" ]; do sleep 0.01; done; printf 'after\\n'; exit 23", "sh", release}}}
	reply, err := cutHandle(a, scope, req)
	if err != nil {
		t.Fatal(err)
	}
	task := cutTask{scope: scope, handle: h, pid: reply.PID, release: release, stdoutACK: 3}
	cutWait(t, "child output before cut", func() bool {
		r, e := cutHandle(a, scope, proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1})
		return e == nil && r.Stdout != nil && string(r.Stdout.Data) == "before\n"
	})
	if _, err := cutHandle(a, scope, proto.ProcessResumeReq{Op: "ack", Handle: &h, AttachEpoch: 1, StdoutACK: 3}); err != nil {
		t.Fatal(err)
	}
	return task
}

func cutFinishTask(t *testing.T, a *Agent, task cutTask) {
	t.Helper()
	if err := os.WriteFile(task.release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	cutWait(t, "exact output and terminal after thaw", func() bool {
		r, e := cutHandle(a, task.scope, proto.ProcessResumeReq{Op: "resume", Handle: &task.handle, AttachEpoch: 1, StdoutACK: task.stdoutACK})
		return e == nil && r.Stdout != nil && string(r.Stdout.Data) == "before\n"[task.stdoutACK:]+"after\n" && r.Phase == "terminal" && r.ExitCode == 23
	})
	cutWait(t, "courier exit 23", func() bool {
		a.courier.mu.Lock()
		defer a.courier.mu.Unlock()
		ev := a.courier.pending[courierKey(task.pid, procEventExit)]
		return ev != nil && ev.rc == 23
	})
}

func cutPending(t *testing.T, a *Agent) *upgradeMarker {
	t.Helper()
	dst := a.cfg.UpgradeExecutablePath
	if err := os.WriteFile(upgradePrevPath(dst), []byte(domainScript("exec-v1 pty-v1", 1)), 0755); err != nil {
		t.Fatal(err)
	}
	m := testMarker(upgradeStatePending, 1, time.Now().Add(-time.Second))
	m.NewSHA, m.PrevSHA = mustSHA(t, dst), mustSHA(t, upgradePrevPath(dst))
	m.TargetSID, m.TargetNID, m.TargetInstance = a.cfg.SID, a.cfg.NID, a.instanceID
	if err := writeUpgradeMarker(upgradeMarkerPath(dst), m); err != nil {
		t.Fatal(err)
	}
	return m
}

func cutHandoffFDs() int {
	entries, _ := os.ReadDir("/proc/self/fd")
	n := 0
	for _, entry := range entries {
		target, _ := os.Readlink("/proc/self/fd/" + entry.Name())
		if strings.Contains(target, "memfd:tether-handoff") {
			n++
		}
	}
	return n
}

// origin: hot-update S3 plan §2.2 / T8. Each round proves both ordering and
// resumed byte/exit accounting after an exec syscall failure.
func TestUpgradeExecFailureThawsOriginalBusiness(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseUpgradeExecFailure(t)
		})
	}
	testharness.AssertNoFDLeak(t, "upgrade exec failure x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "upgrade exec failure x5", beforeG)
}

func exerciseUpgradeExecFailure(t *testing.T) {
	a := cutTestAgent(t)
	task := cutStartTask(t, a)
	m := cutPending(t, a)
	invoked := make(chan struct{})
	a.cfg.UpgradeExecFn = func(string) error {
		if !a.processRuntime.Frozen() || !a.workloads.Frozen() || cutHandoffFDs() != 1 {
			t.Error("exec did not receive a prepared, frozen image")
		}
		close(invoked)
		return syscall.ENOEXEC
	}
	ordering := make(chan bool, 1)
	finished := make(chan struct{})
	started := false
	upgradeSyncObserver = func(kind, path string) error {
		if kind == "file" && strings.Contains(path, ".tether-upgrade-marker-") {
			a.gate.mu.Lock()
			open := !a.gate.closed
			a.gate.mu.Unlock()
			ordering <- (!a.processRuntime.Frozen() && !a.workloads.Frozen() && open)
		}
		return nil
	}
	defer func() {
		// A failed prepare may return before the exec hook. Join that recovery
		// even on the assertion's error path before changing its shared seam.
		// origin: hot-update S4 plan §4.2 / T33.
		if started {
			<-finished
		}
		upgradeSyncObserver = nil
	}()
	a.gate.close()
	cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
	if err != nil {
		t.Fatal(err)
	}
	cut.releaseAdmission = func() { close(finished) }
	started = true
	a.reExecInPlace(a.cfg.UpgradeExecutablePath, cut, true)
	select {
	case <-invoked:
	case <-time.After(cutTestWait):
		t.Fatal("exec hook not invoked")
	}
	select {
	case ok := <-ordering:
		if !ok {
			t.Fatal("disk recovery ran before thaw/open")
		}
	case <-time.After(cutTestWait):
		t.Fatal("exec failure did not recover disk")
	}
	cutWait(t, "recovery completes", func() bool {
		got, _ := readUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
		return got != nil && got.State == upgradeStateRolledBack
	})
	// Synchronize with the recovery goroutine before removing the sync observer.
	waitUpgradeLock(&a.upgradeMu)
	cutRequireOpen(t, a)
	got := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
	if got.ResourcePhase != markerPhaseThawed || mustSHA(t, a.cfg.UpgradeExecutablePath) != m.PrevSHA || cutHandoffFDs() != 0 {
		t.Fatalf("recovery lost phase, disk or descriptors: %+v", got)
	}
	cutFinishTask(t, a, task)
}

// origin: hot-update S3 plan §2.5 / T8'. Run's deferred Close must wait
// while exec owns the cut, then observe a thawed runtime on failure.
func TestRunExitDuringCutWaitsForAbandon(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			task := cutStartTask(t, a)
			entered, release, stopped := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			a.cfg.UpgradeExecFn = func(string) error { close(entered); <-release; return syscall.ENOEXEC }
			a.gate.close()
			cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			a.setRunCtx(ctx)
			a.reExecInPlace(a.cfg.UpgradeExecutablePath, cut, false)
			<-entered
			cancel()
			go func() { stopProcessRuntime(ctx, a); close(stopped) }()
			select {
			case <-stopped:
				t.Fatal("Run cleanup crossed the cut latch")
			case <-time.After(upgradebudget.ReplyDrain):
			}
			if _, _, err := a.processRuntime.Export(); err != nil {
				t.Fatalf("runtime discarded during cut: %v", err)
			}
			once.Do(func() { close(release) })
			select {
			case <-stopped:
			case <-time.After(cutTestWait):
				t.Fatal("Run cleanup did not finish after abandon")
			}
			cutWait(t, "cleanup exit retained", func() bool {
				a.courier.mu.Lock()
				defer a.courier.mu.Unlock()
				return a.courier.pending[courierKey(task.pid, procEventExit)] != nil
			})
		})
	}
	testharness.AssertNoFDLeak(t, "Run exit during cut x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "Run exit during cut x5", beforeG)
}

func cutImportActive(t *testing.T, a *Agent) {
	t.Helper()
	frozen, err := agentimage.Freeze(imageOwners(a), time.Now().Add(upgradebudget.SafePointWait))
	if err != nil {
		t.Fatal(err)
	}
	_, resources, err := a.processRuntime.Export()
	if err != nil {
		t.Fatal(err)
	}
	source := handofftest.DupResources(t, resources)
	img := agentboot.FromCut(frozen.Bundle(), a.workloads.Snapshot(), source, source.Borrow())
	imp, err := img.Restore(agentimage.Target{Runtime: processRuntimeConfig(a)})
	if err != nil {
		t.Fatal(err)
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	a.processRuntime = imp.Runtime
	if _, _, _, err := img.Activate(); err != nil {
		t.Fatal(err)
	}
	a.image, a.imageGeneration = img, img.Generation()
}

type cutRollbackLogs struct {
	blocked    atomic.Int32
	wrongLevel atomic.Bool
}

func (l *cutRollbackLogs) Enabled(context.Context, slog.Level) bool { return true }
func (l *cutRollbackLogs) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "agent: upgrade rollback blocked" {
		l.blocked.Add(1)
		if r.Level != slog.LevelError {
			l.wrongLevel.Store(true)
		}
	}
	return nil
}
func (l *cutRollbackLogs) WithAttrs([]slog.Attr) slog.Handler { return l }
func (l *cutRollbackLogs) WithGroup(string) slog.Handler      { return l }

// origin: hot-update S3 plan §2.6 / T11. A deferred rollback must keep
// prev intact and retry CURRENT state, including ACKs accepted after import.
func TestWatchdogRollbackReexportsCurrentStateOrDefers(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	nc := cutNATS(t)
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			logs := &cutRollbackLogs{}
			a.cfg.Logger = slog.New(logs)
			now := time.Now()
			a.cfg.UpgradeNowFn = func() time.Time { return now }

			task := cutStartTask(t, a)
			cutImportActive(t, a)
			task.stdoutACK = 4
			if _, err := cutHandle(a, task.scope, proto.ProcessResumeReq{Op: "ack", Handle: &task.handle, AttachEpoch: 1, StdoutACK: task.stdoutACK}); err != nil {
				t.Fatal(err)
			}
			prepare := cutTransactionAdmission(t, a)
			m := cutPending(t, a)
			a.procs["busy"] = &procRec{}
			var execs int
			a.cfg.UpgradeExecFn = func(string) error {
				execs++
				if cutHandoffFDs() != 1 {
					t.Error("rollback has no Prepared")
				}
				return syscall.ENOEXEC
			}
			var seen agentimage.Bundle
			a.cfg.UpgradeCutObserver = func(b agentimage.Bundle) { seen = b }
			if a.watchdogRollback(a.cfg.UpgradeExecutablePath) != rollbackRetry || execs != 0 {
				t.Fatal("busy rollback did not defer")
			}
			blocked := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
			if blocked.State != upgradeStatePending || blocked.ResourcePhase != markerPhaseRollbackBlocked || blocked.Detail != agentimage.ReasonLegacySessionBusy {
				t.Fatalf("blocked marker=%+v", blocked)
			}
			req, hits := cutServer(t, a, cutCandidate(t, proto.ImageABIVersion), nil)
			resp := prepare(nc, req)
			if resp.Code != upgradetx.CodeDeferred || resp.Error != "upgrade port operation failed" || resp.View.Historical.Grant.Handle.TxID != "" || hits.Load() != 0 || mustSHA(t, upgradePrevPath(a.cfg.UpgradeExecutablePath)) != m.PrevSHA {
				t.Fatalf("second transaction damaged blocked rollback: code=%s error=%s", resp.Code, resp.Error)
			}

			if a.watchdogRollback(a.cfg.UpgradeExecutablePath) != rollbackRetry || logs.blocked.Load() != 1 {
				t.Fatal("blocked rollback log was not limited to one per minute")
			}
			now = now.Add(time.Minute)
			if a.watchdogRollback(a.cfg.UpgradeExecutablePath) != rollbackRetry || logs.blocked.Load() != 2 {
				t.Fatal("blocked rollback stopped reporting after one minute")
			}
			// origin: hot-update S3 plan §2.6 / TD-4. Error-only sinks must
			// retain the rate-limited alarm as well as its reason text.
			if logs.wrongLevel.Load() {
				t.Fatal("rollback blocked alarm is not Error level")
			}
			now = now.Add(upgradebudget.RollbackCutRetry)
			delete(a.procs, "busy")
			if a.watchdogRollback(a.cfg.UpgradeExecutablePath) != rollbackDone || execs != 1 {
				t.Fatal("clear rollback did not dispatch")
			}
			if a.rollbackInFlight.Load() {
				t.Fatal("completed rollback still blocks another cut")
			}
			if len(seen.Runtime.Tasks) != 1 || seen.Lineage.Generation != a.imageGeneration+1 ||
				seen.Lineage.InstanceID != a.instanceID || seen.Lineage.RoutingNID != nidOf(a) || len(seen.Courier.Events) == 0 ||
				seen.Runtime.Tasks[0].Stdout.Replay.Base != task.stdoutACK {
				t.Fatal("watchdog did not re-export current state")
			}
			got := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
			// origin: hot-update S8 plan §1 / §11.2 P17; S8 review rv-fault F7.
			// The injected exec failed, so the candidate still runs: the reverse
			// did not happen and the marker must not claim rolled_back.
			if got.State != upgradeStateRollbackFailed || got.ResourcePhase != markerPhaseThawed || got.Detail != fmt.Sprintf("register deadline exceeded (%s) without a successful register", upgradeRegisterDeadline) {
				t.Fatalf("rollback marker=%+v", got)
			}
			cutRequireOpen(t, a)
			cutFinishTask(t, a, task)
		})
	}

	t.Run("timer_retry", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		a := cutTestAgent(t)
		cutImportActive(t, a)
		cutPending(t, a)
		a.procs["busy"] = &procRec{}
		dispatched := make(chan struct{})
		a.cfg.UpgradeExecFn = func(string) error { close(dispatched); return syscall.ENOEXEC }
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		a.armUpgradeWatchdog(ctx)
		cutWait(t, "watchdog blocked", func() bool {
			m, _ := readUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
			return m != nil && m.ResourcePhase == markerPhaseRollbackBlocked
		})
		a.procsMu.Lock()
		delete(a.procs, "busy")
		a.procsMu.Unlock()
		select {
		case <-dispatched:
		case <-time.After(2*upgradebudget.RollbackCutRetry + time.Second):
			t.Fatal("watchdog did not retry its blocked cut")
		}
		waitUpgradeLock(&a.upgradeMu)
		cutRequireOpen(t, a)
	})
	nc.Close()
	testharness.AssertNoFDLeak(t, "watchdog current image x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "watchdog current image x5", beforeG)
}

// origin: hot-update S3 plan §3.5 / T12. Stop and authority loss never
// re-dispatch; an authoritative register refusal cuts the active image.
func TestRunFailedBeforeControlReadyTable(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, kind := range []string{"ctx_canceled", "error_canceled", "session_gone", "refused", "deferred", "not_pending", "foreign_target", "no_image", "no_error"} {
			t.Run(fmt.Sprintf("%d/%s", round, kind), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				a := cutTestAgent(t)
				task := cutStartTask(t, a)
				cutImportActive(t, a)
				m := cutPending(t, a)
				cause := errors.New("proto_mismatch")
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				a.setRunCtx(ctx)
				switch kind {
				case "ctx_canceled":
					cancel()
				case "error_canceled":
					cause = fmt.Errorf("register: %w", context.Canceled)
				case "session_gone":
					cause = fmt.Errorf("register: %w", errSessionGone)
				case "deferred":
					a.procs["busy"] = &procRec{}
				case "not_pending":
					m.State = upgradeStateCommitted
				case "foreign_target":
					m.TargetInstance = "another-instance"
				case "no_image":
					a.image = nil
				case "no_error":
					cause = nil
				}
				if err := writeUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath), m); err != nil {
					t.Fatal(err)
				}
				execs, observed := 0, false
				a.cfg.UpgradeCutObserver = func(b agentimage.Bundle) {
					observed = len(b.Runtime.Tasks) == 1 && b.Lineage.Generation == a.imageGeneration+1
				}
				a.cfg.UpgradeExecFn = func(string) error {
					execs++
					if cutHandoffFDs() != 1 {
						t.Error("missing Prepared on Run failure")
					}
					return syscall.ENOEXEC
				}
				if got := runFailedBeforeControlReady(ctx, a, cause); got != cause {
					t.Fatalf("Run error changed: %v", got)
				}
				if kind == "refused" {
					if execs != 1 || !observed {
						t.Fatal("register refusal did not re-export current image")
					}
				} else {
					if execs != 0 {
						t.Fatal("stop/authority/busy/non-owner path re-dispatched")
					}
					got := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
					if got.State != m.State {
						t.Fatalf("marker changed: %+v", got)
					}
				}
				cutRequireOpen(t, a)
				cutFinishTask(t, a, task)
			})
		}
	}
	testharness.AssertNoFDLeak(t, "Run failure current image x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "Run failure current image x5", beforeG)
}

func cutUnexpectedDescriptors() ([]string, error) {
	entries, err := os.ReadDir("/proc/self/fdinfo")
	if err != nil {
		return nil, err
	}
	var leaks []string
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil || fd < 3 {
			continue
		}
		data, err := os.ReadFile("/proc/self/fdinfo/" + entry.Name())
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) != 2 || fields[0] != "flags:" {
				continue
			}
			flags, err := strconv.ParseUint(fields[1], 8, 64)
			if err != nil {
				return nil, err
			}
			if flags&syscall.O_CLOEXEC == 0 {
				target, _ := os.Readlink("/proc/self/fd/" + entry.Name())
				leaks = append(leaks, entry.Name()+":"+target)
			}
		}
	}
	return leaks, nil
}

// origin: hot-update S3 plan §5.2. At the exec hook, Prepared still owns
// CLOEXEC duplicates; every fd >=3 must therefore be protected, without an
// inherited-fd baseline that could accidentally bless an existing leak.
func TestUpgradeCutLeavesNoInheritableDescriptors(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			task := cutStartTask(t, a)
			cutOpenDescriptorOwners(t, a)
			inspected := make(chan error, 1)
			a.cfg.UpgradeExecFn = func(string) error {
				// Include the real upgrade lock while its owner holds it.
				err := withUpgradeFileLock(a.cfg.UpgradeExecutablePath, true, func() error {
					leaks, err := cutUnexpectedDescriptors()
					if err == nil && len(leaks) > 0 {
						err = fmt.Errorf("unexpected inheritable descriptors: %v", leaks)
					}
					return err
				})
				inspected <- err
				return syscall.ENOEXEC
			}
			a.gate.close()
			cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
			if err != nil {
				t.Fatal(err)
			}
			a.reExecInPlace(a.cfg.UpgradeExecutablePath, cut, false)
			select {
			case err := <-inspected:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(cutTestWait):
				t.Fatal("exec hook not invoked")
			}
			cutWait(t, "cut abandoned", func() bool {
				if !a.cutMu.TryLock() {
					return false
				}
				a.cutMu.Unlock()
				return true
			})
			cutRequireOpen(t, a)
			cutFinishTask(t, a, task)
		})
	}
	testharness.AssertNoFDLeak(t, "cut descriptor audit x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "cut descriptor audit x5", beforeG)
}

// origin: hot-update S3 plan §2.1 / Q1. Refusing a legacy candidate must
// include a pure revocation history even when the runtime has no tasks.
//
// origin: hot-update S8 plan §3.3 / §6.1 / §7.2; S8 FIX-FIXTURES. The grantless
// envelope is refused before download, so it validates and stages nothing.
// Validation runs on the agent's candidate policy over the isolated version
// probe of the exact bytes, and a compatible candidate must also pass the
// final reader gate of a real cut: the checks a granted Preflight/Freeze use.
func TestUpgradeCandidateABIProtectsEveryRetainedDomain(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	nc := cutNATS(t)
	for _, kind := range []string{"task", "revocation", "courier", "tombstone", "future", "empty", "current", "current_with_task", "foreign_freeze"} {
		t.Run(kind, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			abi, task := retainCandidateDomain(t, a, kind)
			req, hits := cutServer(t, a, cutCandidate(t, abi), nil)
			dst := a.cfg.UpgradeExecutablePath
			old := mustSHA(t, dst)
			if resp := cutForward(t, a, nc, req); resp.OK || resp.Code != "upgrade_deferred" || hits.Load() != 0 {
				t.Fatalf("grantless envelope reached candidate validation: %+v", resp)
			}
			reader := cutCandidateReader(t, abi)
			err := checkCandidateABI(a, reader)
			if kind == "empty" || kind == "current" || kind == "current_with_task" {
				if err != nil {
					t.Fatalf("compatible candidate refused: %v", err)
				}
				// The final reader gate re-reads the actual cut, carried task included.
				a.gate.close()
				cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
				if err != nil {
					a.gate.open()
					t.Fatal(err)
				}
				gateErr := agentimage.CheckBundleCapabilities(cut.frozen.Bundle(), reader)
				if thawErr := cut.abandon(a); gateErr != nil || thawErr != nil {
					t.Fatalf("compatible candidate refused at the final cut: gate=%v thaw=%v", gateErr, thawErr)
				}
				cutRequireOpen(t, a)
				if kind == "current_with_task" {
					cutFinishTask(t, a, task)
				}
			} else if !errors.Is(err, errImageABIUnsupported) {
				t.Fatalf("unsafe candidate accepted: %v", err)
			}
			if a.processRuntime.Frozen() {
				t.Fatal("candidate validation stranded the runtime frozen")
			}
			if mustSHA(t, dst) != old {
				t.Fatal("candidate validation modified dst")
			}
			if _, err := os.Stat(upgradePrevPath(dst)); !os.IsNotExist(err) {
				t.Fatal("candidate validation wrote prev")
			}
			if _, err := os.Stat(upgradeMarkerPath(dst)); !os.IsNotExist(err) {
				t.Fatal("candidate validation wrote marker")
			}
		})
	}
	// origin: hot-update S8 plan §6.1 / §7.1; S8 FIX-FIXTURES R2; S8 FIX-AGENTWIRE.
	// The same retained-domain table through a granted Start on the production
	// RuntimePort, plus a live PTY offered to a reader without pty-v1, SS
	// metadata retained by the real census, and an agent with
	// no creation identity (incarnation) for the adjunct to carry.
	for _, kind := range []string{"task", "revocation", "courier", "tombstone", "future", "empty", "current", "current_with_task", "foreign_freeze", "pty_without_domain", "ss_metadata", "no_incarnation"} {
		t.Run("granted_preflight/"+kind, func(t *testing.T) { exerciseGrantedCandidateDomains(t, nc, kind) })
	}
}

// retainCandidateDomain gives the agent one kind of retained responsibility
// through its real owners and returns the candidate ABI the S3 table offers.
// Clock injection changes the runtime's responsibility; no image is rewritten.
func retainCandidateDomain(t *testing.T, a *Agent, kind string) (int, cutTask) {
	t.Helper()
	abi := 0
	var task cutTask
	switch kind {
	case "task":
		cutStartTask(t, a)
	case "revocation":
		if _, err := a.processRuntime.Revoke(a.cfg.SID, "removed"); err != nil {
			t.Fatal(err)
		}
	case "courier":
		a.courier.enqueueExit("retained", 23)
	case "future":
		abi = proto.ImageABIVersion + 1
	case "current":
		abi = proto.ImageABIVersion
	case "current_with_task":
		abi = proto.ImageABIVersion
		task = cutStartTask(t, a)
	case "pty_without_domain":
		abi = proto.ImageABIVersion
		domainRetainPTY(t, a, "live")
	case "tombstone":
		stopProcessRuntime(t.Context(), a)
		now, err := procrequest.BootNow()
		if err != nil {
			t.Fatal(err)
		}
		var clock atomic.Int64
		clock.Store(now)
		cfg := processRuntimeConfig(a)
		cfg.Instance = "tombstone-runtime"
		cfg.Clock = func() (int64, error) { return clock.Load(), nil }
		rt, err := execsession.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		a.processRuntime = rt
		finished := cutStartTask(t, a)
		cutFinishTask(t, a, finished)
		clock.Add((execsession.AbandonedTerminalGrace + time.Minute).Nanoseconds())
		cutWait(t, "terminal swept to tombstone", func() bool {
			_, err := a.processRuntime.Sweep()
			return err == nil && len(a.processRuntime.TaskPIDs()) == 0
		})
		a.courier.mu.Lock()
		a.courier.pending = map[string]*pendingProcEvent{}
		a.courier.mu.Unlock()

	case "foreign_freeze":
		if _, err := a.workloads.Freeze(); err != nil {
			t.Fatal(err)
		}
	}
	return abi, task
}

// origin: hot-update S3 plan §2.1; drill 33 C2 first real run 2026-10-02 — the
// plan's running == dst form refused the shared-binary sibling whose dst a
// co-hosted agent had already replaced with this very candidate.
//
// origin: hot-update S8 plan §3.1 / §5.2; S8 FIX-FIXTURES R2; S8 FIX-AGENTWIRE.
// The v1 installer and its disk prev slot have no production caller. The
// recorded previous image is now the running source itself, published as an
// immutable SHA slot bound to the grant; the canonical pathname is never read
// as prev. The four cases keep their meaning on the production RuntimePort.
func TestInstallPrevSlotAcceptsOnlyRollbackCapableImages(t *testing.T) {
	nc := cutNATS(t)
	for _, tc := range []struct {
		name       string
		dst        string // canonical bytes: "" running == dst, "candidate" a sibling's flip, "unrelated"
		diskSource bool   // the grant names the canonical bytes, not the running image
		missing    bool   // the running image cannot be hashed
		wantOK     bool   // admitted: recorded previous == the running source bytes
	}{
		{name: "exporter", wantOK: true},
		{name: "shared_sibling_candidate", dst: "candidate", wantOK: true},
		{name: "unrelated_disk", dst: "unrelated", diskSource: true},
		{name: "unhashable_running", missing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := cutTestAgent(t)
			r := openGrantedRoute(t, a, false)
			dst := a.cfg.UpgradeExecutablePath
			candidate := s8Script("prev-slot candidate "+a.instanceID, s8Domains)
			switch tc.dst {
			case "":
				if err := os.WriteFile(dst, r.source, 0o755); err != nil {
					t.Fatal(err)
				}
			case "candidate":
				if err := os.WriteFile(dst, candidate, 0o755); err != nil {
					t.Fatal(err)
				}
			default:
				if err := os.WriteFile(dst, []byte("#!/bin/sh\n# unrelated on-disk image\n"), 0o755); err != nil {
					t.Fatal(err)
				}
			}
			running := r.source
			if tc.diskSource {
				disk, err := os.ReadFile(dst)
				if err != nil {
					t.Fatal(err)
				}
				r.source = disk
			}
			before := mustSHA(t, dst)
			grant, receipt := r.publish(t, a, nc, candidate)
			if tc.missing {
				if err := os.Remove(a.cfg.UpgradeRunningImagePath); err != nil {
					t.Fatal(err)
				}
			}
			execs := 0
			a.cfg.UpgradeExecFn = func(string) error { execs++; return syscall.ENOEXEC }
			// The grantless envelope cannot stage a prev slot of any kind.
			if legacy := cutForward(t, a, nc, proto.UpgradeForwardedReq{URL: "http://127.0.0.1:1/never", SHA256: grant.TargetSHA}); legacy.OK || legacy.Code != "upgrade_deferred" {
				t.Fatalf("grantless envelope reached the installer: %+v", legacy)
			}
			resp := r.start(t, a, nc, grant, receipt)
			cutRequireOpen(t, a)
			if mustSHA(t, dst) != before {
				t.Fatal("an S8 Start modified the canonical executable")
			}
			if _, err := os.Stat(upgradePrevPath(dst)); !os.IsNotExist(err) {
				t.Fatal("an S8 Start wrote a v1 prev slot")
			}
			if !tc.wantOK {
				if execs != 0 || resp.Code != upgradetx.CodeDeferred || resp.Error != agentupgradeport.ReasonRunningImageChanged {
					t.Fatalf("inadmissible previous image not refused: code=%s error=%q execs=%d", resp.Code, resp.Error, execs)
				}
				r.requireUnaccepted(t, grant)
				return
			}
			if execs != 1 || resp.Code != upgradetx.CodeUnsettled {
				t.Fatalf("rollback-capable running source refused: code=%s error=%q", resp.Code, resp.Error)
			}
			view, err := r.current()
			if err != nil {
				t.Fatal(err)
			}
			source, err := os.ReadFile(receipt.SourcePath)
			if err != nil || view.Historical.RecordedPreviousSHA != shaOf(running) || !bytes.Equal(source, running) {
				t.Fatal("recorded previous image is not the running source bytes")
			}
		})
	}
}

// origin: hot-update S3 plan §4.0. Every legacy entry point must refuse
// before parsing or side effects while the cut owns admission.
func TestUpgradeGateRefusesEveryLegacyAdmission(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	nc := cutNATS(t)
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	cases := []struct {
		name         string
		call         func(*Agent, *nats.Conn, *nats.Msg)
		field, value string
	}{
		{"exec", (*Agent).handleExecForwarded, "error", "try_again: agent_upgrading: image switch in progress; retry"},
		{"run", (*Agent).handleRunForwarded, "reason", "try_again: agent_upgrading: image switch in progress; retry"},
		{"push", (*Agent).handlePushForwarded, "code", "try_again"},
		{"push_commit", (*Agent).handlePushCommitForwarded, "code", "try_again"},
		{"pull", (*Agent).handlePullForwarded, "code", "try_again"},
		{"expose", (*Agent).handleExposeForwarded, "code", "try_again"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			a.gate.close()
			subject := nats.NewInbox()
			sub, err := nc.Subscribe(subject, func(m *nats.Msg) { tc.call(a, nc, m) })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			reply, err := nc.Request(subject, []byte("{}"), cutTestWait)
			if err != nil {
				t.Fatal(err)
			}
			var body map[string]any
			if err := json.Unmarshal(reply.Data, &body); err != nil {
				t.Fatal(err)
			}
			if body[tc.field] != tc.value {
				t.Fatalf("closed gate admitted %s: %s", tc.name, reply.Data)
			}
			a.gate.open()
			for k := gateExec; k < gateKinds; k++ {
				if a.gate.count(k) != 0 {
					t.Fatalf("refusal leaked %v admission", k)
				}
			}
		})
	}
	t.Run("proxy", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		a := cutTestAgent(t)
		a.cfg.ExposeAdapter = noopExpose{}
		a.gate.close()
		a.proxy.mu.Lock()
		a.proxyStartLocked(nc, a.proxy, 12345, 0, "token", 1, 1, nil, nil)
		started := a.proxy.srv
		a.proxy.mu.Unlock()
		if started != nil {
			started.Stop()
			t.Fatal("proxy started through the closed gate")
		}
	})
	for round := 0; round < 5; round++ {
		for _, kind := range []string{"exec", "run"} {
			t.Run(fmt.Sprintf("lifetime/%d/%s", round, kind), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				cutExerciseLegacyAdmission(t, nc, kind)
			})
		}
	}
	testharness.AssertNoFDLeak(t, "legacy admission lifetime x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "legacy admission lifetime x5", beforeG)

}

func cutExerciseLegacyAdmission(t *testing.T, nc *nats.Conn, kind string) {
	t.Helper()
	a := cutTestAgent(t)
	a.setRunCtx(t.Context())
	releasePath := filepath.Join(t.TempDir(), "release-child")
	argv := []string{"/bin/sh", "-c", "while [ ! -e \"$1\" ]; do sleep 0.01; done; exit 23", "sh", releasePath}
	subject, inbox := nats.NewInbox(), nats.NewInbox()
	replies, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replies.Unsubscribe() }()
	done := make(chan struct{})
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		defer close(done)
		if kind == "exec" {
			a.handleExecForwarded(nc, m)
		} else {
			a.handleRunForwarded(nc, m)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	defer func() {
		_ = os.WriteFile(releasePath, nil, 0600)
		select {
		case <-done:
		case <-time.After(cutTestWait):
			t.Error("legacy handler did not finish")
		}
	}()
	data, _ := json.Marshal(proto.ExecReq{Cwd: t.TempDir(), Argv: argv})
	if err := nc.PublishRequest(subject, inbox, data); err != nil {
		t.Fatal(err)
	}
	reply, err := replies.NextMsg(cutTestWait)
	if err != nil {
		t.Fatal(err)
	}
	var first proto.RunChunk
	if err := json.Unmarshal(reply.Data, &first); err != nil {
		t.Fatal(err)
	}
	gate, reason := gateExec, agentimage.ReasonLegacySessionBusy
	if kind == "run" {
		gate, reason = gateRun, agentimage.ReasonLegacySessionBusy
		if first.Kind != "ready" {
			t.Fatalf("PTY not ready: %s", reply.Data)
		}
		// No child exists yet, so only admission protects the allocated PTY.
		// origin: hot-update S4 plan §7.1 / T29.
		if a.gate.count(gateRun) != 1 {
			t.Fatal("legacy allocation/attach dropped run admission")
		}
		a.gate.close()
		_, cutErr := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
		var deferred *agentimage.DeferredError
		if !errors.As(cutErr, &deferred) || deferred.Reason != agentimage.ReasonLegacySessionBusy {
			t.Fatalf("un-attached legacy PTY cut err=%v", cutErr)
		}
		a.gate.open()
		attach, _ := json.Marshal(proto.PtyAttachEvent{Cols: 80, Rows: 24})
		if err := nc.Publish(proto.SubjPtyAttach(a.cfg.SID, first.PID), attach); err != nil {
			t.Fatal(err)
		}
	}
	cutWait(t, "legacy child adopted", func() bool { return len(a.workloads.Snapshot()) == 1 })
	if a.gate.count(gate) != 1 {
		t.Fatal("legacy admission released before the child exited")
	}
	a.gate.close()
	_, err = freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
	var deferred *agentimage.DeferredError
	if !errors.As(err, &deferred) || deferred.Reason != reason {
		t.Fatalf("legacy child cut was not deferred: %v", err)
	}
	a.gate.open()
	if err := os.WriteFile(releasePath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(cutTestWait):
		t.Fatal("legacy child did not exit")
	}
	if a.gate.count(gate) != 0 {
		t.Fatal("legacy exit leaked admission")
	}
}

// origin: hot-update S3 plan §2.3 / Q3. A real forwarded cluster leg
// freezes before replying, rejects a concurrent request, and survives exec
// failure with the same live child and no fabricated rollback marker.
//
// origin: hot-update S8 plan §3.3 / §5.2; S8 FIX-FIXTURES R2; S8 FIX-AGENTWIRE.
// A ReExecOnly leg now needs the v2 lock, journal, recorded prev and grant:
// the cluster driver's child is a granted Start whose target is the staged
// bytes. It freezes (after a leading heartbeat) before exec, a concurrent
// request does not queue behind the cut, and a returning exec keeps the same
// live child while the journal reports recovery, never a fabricated rollback.
func TestReExecOnlyFailureKeepsTheCutAlive(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	nc := cutNATS(t)
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			task := cutStartTask(t, a)
			route := openGrantedRoute(t, a, false)
			candidate := s8Script("staged re-exec "+a.instanceID, s8Domains)
			if err := os.WriteFile(a.cfg.UpgradeExecutablePath, candidate, 0755); err != nil {
				t.Fatal(err)
			}
			req := proto.UpgradeForwardedReq{ReExecOnly: true, SHA256: sha256OfBytes(candidate)}
			if refused := cutForward(t, a, nc, req); refused.OK || refused.Code != "upgrade_deferred" {
				t.Fatalf("grantless cluster leg not refused: %+v", refused)
			}
			grant, receipt := route.publish(t, a, nc, candidate)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			a.cfg.UpgradeExecFn = func(string) error { close(entered); <-release; return syscall.ENOEXEC }
			a.ncBox.Store(nc)
			hb, err := nc.SubscribeSync(proto.SubjNodeHeartbeat(a.cfg.SID, nidOf(a)))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = hb.Unsubscribe() }()
			endpoint := a.upgradeEndpoint
			done := make(chan startResult, 1)
			go func() {
				view, err := endpoint.Start(t.Context(), grant, receipt)
				done <- startResult{view, err}
			}()
			select {
			case <-entered:
			case <-time.After(cutTestWait):
				t.Fatal("cluster leg did not exec")
			}
			if _, err := hb.NextMsg(cutTestWait); err != nil {
				t.Fatal("cut omitted the leading heartbeat")
			}
			if !a.processRuntime.Frozen() || !a.workloads.Frozen() {
				t.Fatal("re-exec reached exec before freezing")
			}
			if refused := cutForward(t, a, nc, req); refused.OK || refused.Code != "upgrade_deferred" {
				t.Fatalf("concurrent grantless leg not refused: %+v", refused)
			}
			// The sequencer is cancellable: a bounded concurrent Start returns at
			// its own deadline rather than queueing behind the exec.
			bounded, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			_, queued := endpoint.Start(bounded, grant, receipt)
			cancel()
			if !errors.Is(queued, context.DeadlineExceeded) {
				t.Fatalf("second cut was queued: %v", queued)
			}
			once.Do(func() { close(release) })
			select {
			case got := <-done:
				var refusal *upgradetx.Error
				if !errors.As(got.err, &refusal) || refusal.Code != upgradetx.CodeUnsettled {
					t.Fatalf("returned exec reported %v", got.err)
				}
			case <-time.After(cutTestWait):
				t.Fatal("cluster leg did not return")
			}
			cutWait(t, "ReExecOnly abandon", func() bool {
				if !a.cutMu.TryLock() {
					return false
				}
				a.cutMu.Unlock()
				return true
			})
			cutRequireOpen(t, a)
			cutFinishTask(t, a, task)
			route.requireRecovery(t, grant, receipt)
			if _, err := os.Stat(upgradeMarkerPath(a.cfg.UpgradeExecutablePath)); !os.IsNotExist(err) {
				t.Fatal("cluster leg invented a marker")
			}
		})
	}
	testharness.AssertNoFDLeak(t, "cluster leg failure x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "cluster leg failure x5", beforeG)
}

// origin: hot-update S3 plan §4.1. A real FIFO stands in for a wedged home
// read: preflight has a bounded wait and later callers cannot multiply the
// outstanding read. Healing releases the descriptor and permits another cut.
func TestUpgradeHomeProbeIsBoundedAndSingleFlight(t *testing.T) {
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	// Each round owns a separate Agent and FIFO. Construct Agents before
	// parallel execution because New records process-wide exec lineage; join
	// the group before either process-wide leak assertion.
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(strconv.Itoa(round), func(t *testing.T) {
				a := cutTestAgent(t)
				t.Parallel()
				a.homeHangable = true
				path := filepath.Join(t.TempDir(), "home-fifo")
				if err := syscall.Mkfifo(path, 0600); err != nil {
					t.Fatal(err)
				}
				a.stateStore.path = path
				done := make(chan struct{})
				go func() { probeUpgradeHome(a); close(done) }()
				var healed bool
				heal := func() {
					if healed {
						return
					}
					healed = true
					writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0600)
					if err != nil {
						t.Error(err)
						return
					}
					_, _ = writer.WriteString("{}")
					_ = writer.Close()
				}
				defer heal()
				cutWait(t, "home reader started", func() bool { a.homeReadMu.Lock(); defer a.homeReadMu.Unlock(); return a.homeReadInFlight })
				before := time.Now()
				probeUpgradeHome(a)
				if time.Since(before) > upgradebudget.RequestLatency/10 {
					t.Fatal("second home probe waited instead of preserving single-flight")
				}
				select {
				case <-done:
				case <-time.After(upgradebudget.RequestLatency + time.Second):
					t.Fatal("home probe exceeded its bounded wait")
				}
				reasons := preflightDeferrals(a)
				if len(reasons) != 1 || reasons[0].Reason != agentimage.ReasonRemoteFSProbeInFlight {
					t.Fatalf("abandoned home read missing from preflight: %v", reasons)
				}
				heal()
				cutWait(t, "home read healed", func() bool { a.homeReadMu.Lock(); defer a.homeReadMu.Unlock(); return !a.homeReadInFlight })
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
				probeUpgradeHome(a)
				if got := preflightDeferrals(a); len(got) > 0 {
					t.Fatalf("healthy home stayed deferred: %v", got)
				}
			})
		}
	})
	testharness.AssertNoFDLeak(t, "bounded home probe x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "bounded home probe x5", beforeG)
}

// origin: hot-update S3 plan §4.1. Count the production adapter's actual
// session slots, including a disconnected slot waiting to redial; an idle
// public listener is already enough to defer this stage's cut.
func TestUpgradeDefersForDisconnectedTunnelSessions(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			control, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			public, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				_ = control.Close()
				t.Fatal(err)
			}
			addr, port := control.Addr().String(), public.Addr().(*net.TCPAddr).Port
			_ = control.Close()
			_ = public.Close()
			server := tunnel.NewServer(addr, "127.0.0.1", func(sid, nid string, got int, _ string, _ int64) error {
				if sid != a.cfg.SID || nid != a.cfg.NID || got != port {
					return errors.New("unexpected tunnel identity")
				}
				return nil
			}, a.cfg.Logger)
			serverCtx, serverCancel := context.WithCancel(t.Context())
			defer serverCancel()
			if err := server.Start(serverCtx); err != nil {
				t.Fatal(err)
			}
			defer server.Close()
			clientCtx, clientCancel := context.WithCancel(t.Context())
			defer clientCancel()
			adapter := NewTunnelExposeAdapter(addr, a.cfg.SID, a.cfg.NID, a.cfg.Logger)
			disconnected := make(chan struct{})
			var down sync.Once
			adapter.SetSessionStateHook(func(_ int, up bool) {
				if !up {
					down.Do(func() { close(disconnected) })
				}
			})
			adapter.Start(clientCtx)
			a.cfg.ExposeAdapter = adapter
			if adapter.SessionCount() != 0 {
				t.Fatal("fresh adapter has sessions")
			}
			if err := adapter.AddProxy(t.Context(), PortToken{Name: "cut", Port: port, LocalPort: 1, Token: "cut-token"}); err != nil {
				t.Fatal(err)
			}
			defer func() { _ = adapter.RemoveProxy("cut", port) }()
			if adapter.SessionCount() != 1 {
				t.Fatal("connected session absent from census")
			}
			serverCancel()
			server.Close()
			select {
			case <-disconnected:
			case <-time.After(cutTestWait):
				t.Fatal("tunnel transport did not disconnect")
			}
			if adapter.SessionCount() != 1 {
				t.Fatal("disconnected slot vanished from census")
			}
			reasons := preflightDeferrals(a)
			if len(reasons) != 1 || reasons[0].Reason != agentimage.ReasonLegacySessionBusy {
				t.Fatalf("disconnected session allowed cut: %v", reasons)
			}
			if err := adapter.RemoveProxy("cut", port); err != nil {
				t.Fatal(err)
			}
			if adapter.SessionCount() != 0 {
				t.Fatal("closed slot remains in census")
			}
		})
	}
	testharness.AssertNoFDLeak(t, "tunnel session census x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "tunnel session census x5", beforeG)
}

// origin: hot-update S4 plan §4.2 / T33. Backpressure makes A<Q at the
// cut; the same master must apply that queued suffix exactly once after a
// failed exec and still accept an ordered resize before the child drains.
func TestPTYUpgradeExecFailureRestoresBothDirections(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	t.Run("owner_order", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		// These structural checks supplement the real PTY receipt below. They
		// pin the only exec-failure funnel without scheduler timing assumptions.
		// Source-gate mutations must be applied on disk, not through an overlay.
		// origin: hot-update S4 plan §4.2 / T33.
		files := token.NewFileSet()
		imageSource, err := parser.ParseFile(files, "upgrade_image.go", nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var thaw, open token.Pos
		guarded := false
		for _, decl := range imageSource.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "abandon" {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
						switch selector.Sel.Name {
						case "Thaw":
							thaw = call.Pos()
						case "open":
							if open == token.NoPos || call.Pos() < open {
								open = call.Pos()
							}
						}
					}
				}
				if guard, ok := node.(*ast.IfStmt); ok {
					if condition, ok := guard.Cond.(*ast.BinaryExpr); ok && condition.Op == token.EQL {
						left, lok := condition.X.(*ast.Ident)
						right, rok := condition.Y.(*ast.Ident)
						if lok && rok && left.Name == "err" && right.Name == "nil" {
							ast.Inspect(guard.Body, func(child ast.Node) bool {
								if selector, ok := child.(*ast.SelectorExpr); ok && selector.Sel.Name == "open" {
									guarded = true
								}
								return true
							})
						}
					}
				}
				return true
			})
		}
		if thaw == token.NoPos || open == token.NoPos || open < thaw || !guarded {
			t.Fatal("admission must open only after successful owner thaw")
		}
		source, err := parser.ParseFile(files, "upgrade.go", nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var preparedClose, abandon token.Pos
		for _, decl := range source.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "reExecInPlace" {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				receiver, ok := selector.X.(*ast.Ident)
				if ok && receiver.Name == "prepared" && selector.Sel.Name == "Close" {
					preparedClose = call.Pos()
				}
				if ok && receiver.Name == "cut" && selector.Sel.Name == "abandon" {
					abandon = call.Pos()
				}
				if selector.Sel.Name == "Discard" || selector.Sel.Name == "TakeMaster" || selector.Sel.Name == "Close" && !ok {
					t.Fatal("returning exec cannot destroy original owners")
				}
				return true
			})
		}
		if preparedClose == token.NoPos || abandon == token.NoPos || abandon < preparedClose {
			t.Fatal("returning exec must close Prepared duplicates then abandon the cut")
		}
	})

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, failure := range []string{"exec", "prepare"} {
			t.Run(fmt.Sprintf("%d/%s", round, failure), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				a := cutTestAgent(t)
				release := filepath.Join(t.TempDir(), "drain-input")
				const inputBytes = 256 << 10
				argv := []string{"/bin/sh", "-c", fmt.Sprintf("stty raw -echo; printf 'before\\n'; while [ ! -e \"$1\" ]; do sleep 0.01; done; head -c %d | sha256sum; stty size; printf 'after\\n'; exit 23", inputBytes), "sh", release}
				task := domainStartPTY(t, a, argv)
				task.release = release
				data := bytes.Repeat([]byte("x"), inputBytes)
				var applied, accepted uint64
				for offset := 0; offset < len(data); offset += 32 << 10 {
					reply, err := cutHandle(a, task.scope, proto.ProcessResumeReq{Kind: "pty", Op: "input", Handle: &task.handle, AttachEpoch: 1, InputSeq: uint64(offset), Input: data[offset : offset+(32<<10)]})
					if err != nil {
						t.Fatal(err)
					}
					applied, accepted = reply.InputACK, reply.InputAccepted
				}
				if accepted != inputBytes || applied >= accepted {
					t.Fatalf("real master did not retain queued input: A=%d Q=%d", applied, accepted)
				}
				// Source end aborts an input direction that was never finished, even
				// when A==Q. Finish the real queued stream before the cut; successful
				// recovery must apply it and its FIN without inventing an abort.
				// origin: hot-update S4 plan §4.2 / T33.
				fin, err := cutHandle(a, task.scope, proto.ProcessResumeReq{Kind: "pty", Op: "input", Handle: &task.handle, AttachEpoch: 1, InputSeq: accepted, InputFIN: true})
				if err != nil || !fin.InputFIN || fin.InputAccepted != accepted {
					t.Fatalf("queued FIN receipt=%+v err=%v", fin, err)
				}
				beforeRows := a.workloads.Snapshot()
				if len(beforeRows) != 1 {
					t.Fatal("original PTY identity missing")
				}
				m := cutPending(t, a)
				invoked := make(chan struct{}, 1)
				a.cfg.UpgradeExecFn = func(string) error {
					if !a.processRuntime.Frozen() || !a.workloads.Frozen() || cutHandoffFDs() != 1 {
						t.Error("exec did not own prepared frozen image")
					}
					invoked <- struct{}{}
					return syscall.ENOEXEC
				}
				originalObserver := upgradeSyncObserver
				defer func() { upgradeSyncObserver = originalObserver }()
				restoredBeforeRecovery := false
				upgradeSyncObserver = func(kind, path string) error {
					if kind == "file" && strings.HasPrefix(filepath.Base(path), ".tether-upgrade-marker-") {
						a.gate.mu.Lock()
						open := !a.gate.closed
						a.gate.mu.Unlock()
						restoredBeforeRecovery = open && !a.workloads.Frozen() && !a.processRuntime.Frozen()
					}
					return nil
				}
				a.gate.close()
				cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
				if err != nil {
					t.Fatal(err)
				}
				if failure == "prepare" {
					// A changed sandbox target rejects preparation before exec. Neither
					// this refusal nor Prepared.Close may close the original terminal.
					if err := os.WriteFile(a.cfg.UpgradeExecutablePath, []byte("different image"), 0755); err != nil {
						t.Fatal(err)
					}
				}
				a.reExecInPlace(a.cfg.UpgradeExecutablePath, cut, true)
				cutWait(t, "PTY exec/prepare failure marker", func() bool {
					marker, _ := readUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
					return marker != nil && marker.State == upgradeStateRolledBack
				})
				waitUpgradeLock(&a.upgradeMu)
				cutWait(t, "failed exec releases the cut owner", func() bool {
					if !a.cutMu.TryLock() {
						return false
					}
					a.cutMu.Unlock()
					return true
				})
				if !restoredBeforeRecovery {
					t.Fatal("business admission opened before both directions recovered")
				}
				if failure == "exec" {
					select {
					case <-invoked:
					default:
						t.Fatal("exec syscall failure was not exercised")
					}
				}
				if failure == "prepare" {
					select {
					case <-invoked:
						t.Fatal("changed target reached exec")
					default:
					}
				}
				marker := readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath))
				if marker.ResourcePhase != markerPhaseThawed || mustSHA(t, a.cfg.UpgradeExecutablePath) != m.PrevSHA || cutHandoffFDs() != 0 {
					t.Fatalf("PTY failure recovery lost marker/disk/descriptors: %+v", marker)
				}
				cutRequireOpen(t, a)
				reply, err := cutHandle(a, task.scope, proto.ProcessResumeReq{Kind: "pty", Op: "resize", Handle: &task.handle, AttachEpoch: 1, ControlSeq: 1, Resize: &proto.PtyResizeEvent{Cols: 100, Rows: 40}})
				if err != nil || reply.ControlACK != 1 {
					t.Fatalf("original control direction did not recover: ACK=%d err=%v", reply.ControlACK, err)
				}
				afterRows := a.workloads.Snapshot()
				if len(afterRows) != 1 || afterRows[0].OSPID != beforeRows[0].OSPID || afterRows[0].StartTimeTicks != beforeRows[0].StartTimeTicks {
					t.Fatal("failure replaced or closed original PTY child")
				}
				if err := os.WriteFile(release, nil, 0600); err != nil {
					t.Fatal(err)
				}
				final := domainWaitTerminal(t, a, task)
				digest := sha256.Sum256(data)
				expected := fmt.Sprintf("before\n%x  -\n40 100\nafter\n", digest)
				if string(final.Stdout.Data) != expected || final.InputACK != inputBytes || final.InputAccepted != inputBytes || !final.InputFIN || !final.InputEnded || final.InputError != "" {
					t.Fatalf("queued input/output did not recover exactly once: A=%d Q=%d output=%q error=%q", final.InputACK, final.InputAccepted, final.Stdout.Data, final.InputError)
				}
			})
		}
	}
	testharness.AssertNoFDLeak(t, "PTY failed exec both directions x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "PTY failed exec both directions x5", beforeG)
}

// The same legacy request moves through allocation, spawn preparation, a
// live child and descendant tail drain. An upgrade must refuse in every
// interval, including before a Manager row exists and after the leader exits.
// origin: hot-update S4 plan §7.1 / T29.
func domainExerciseLegacyStages(t *testing.T, nc *nats.Conn) {
	t.Helper()
	a := cutTestAgent(t)
	a.setRunCtx(t.Context())
	dir := t.TempDir()
	leaderRelease, tailRelease, tailReady := filepath.Join(dir, "leader"), filepath.Join(dir, "tail"), filepath.Join(dir, "tail-ready")
	probeEntered, probeRelease := make(chan struct{}), make(chan struct{})
	var probeOnce, releaseOnce sync.Once
	policy, err := spawnsafe.New(spawnsafe.Config{MountSource: func() ([]byte, error) { return fakeMountinfo([2]string{"/", "ext4"}, [2]string{dir, "nfs4"}), nil }, Probe: func(string) bool { probeOnce.Do(func() { close(probeEntered) }); <-probeRelease; return true }, ProbeTimeout: cutTestWait})
	if err != nil {
		t.Fatal(err)
	}
	a.spawnPolicy = policy
	argv := []string{"/bin/sh", "-c", "printf 'started\\n'; (trap '' HUP; : > \"$3\"; while [ ! -e \"$2\" ]; do sleep 0.01; done; printf 'tail\\n') & while [ ! -e \"$1\" ]; do sleep 0.01; done; exit 23", "sh", leaderRelease, tailRelease, tailReady}
	subject, inbox := nats.NewInbox(), nats.NewInbox()
	replies, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replies.Unsubscribe() }()
	done := make(chan struct{})
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) { defer close(done); a.handleRunForwarded(nc, m) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	defer func() {
		releaseOnce.Do(func() { close(probeRelease) })
		_ = os.WriteFile(leaderRelease, nil, 0600)
		_ = os.WriteFile(tailRelease, nil, 0600)
		select {
		case <-done:
		case <-time.After(cutTestWait):
			t.Error("legacy stage fixture did not release")
		}
	}()
	data, _ := json.Marshal(proto.RunReq{Argv: argv, Cwd: dir, Safe: true})
	if err := nc.PublishRequest(subject, inbox, data); err != nil {
		t.Fatal(err)
	}
	msg, err := replies.NextMsg(cutTestWait)
	if err != nil {
		t.Fatal(err)
	}
	var ready proto.RunChunk
	if err := json.Unmarshal(msg.Data, &ready); err != nil {
		t.Fatal(err)
	}
	if ready.Kind != "ready" {
		t.Fatalf("legacy allocation receipt=%s", msg.Data)
	}
	outputSub, err := nc.SubscribeSync(proto.SubjPtyOut(a.cfg.SID, ready.PID))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outputSub.Unsubscribe() }()
	request, hits := cutServer(t, a, cutCandidate(t, 1), nil)
	assertBusy := func(stage string) {
		t.Helper()
		found := false
		for _, reason := range preflightDeferrals(a) {
			found = found || reason.Reason == agentimage.ReasonLegacySessionBusy
		}
		if !found {
			t.Fatalf("legacy %s lost admission/owner visibility", stage)
		}
		response := cutForward(t, a, nc, request)
		if response.OK || response.Code != "upgrade_deferred" || hits.Load() != 0 {
			t.Fatalf("legacy %s admitted upgrade: %+v", stage, response)
		}
	}
	assertBusy("allocated_unattached")
	attach, _ := json.Marshal(proto.PtyAttachEvent{Cols: 80, Rows: 24})
	if err := nc.Publish(proto.SubjPtyAttach(a.cfg.SID, ready.PID), attach); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probeEntered:
	case <-time.After(cutTestWait):
		t.Fatal("legacy spawn preparation did not reach probe")
	}
	if len(a.workloads.Snapshot()) != 0 {
		t.Fatal("pre-spawn fixture already adopted a child")
	}
	assertBusy("spawn_preparation")
	releaseOnce.Do(func() { close(probeRelease) })
	cutWait(t, "legacy live owner", func() bool { return len(a.workloads.Snapshot()) == 1 })
	cutWait(t, "original slave writer ready", func() bool { _, err := os.Stat(tailReady); return err == nil })
	assertBusy("live")
	if err := os.WriteFile(leaderRelease, nil, 0600); err != nil {
		t.Fatal(err)
	}
	cutWait(t, "legacy leader exited with tail held", func() bool {
		rows := a.workloads.Snapshot()
		if len(rows) != 1 || !rows[0].ReapHeld || rows[0].Exit != nil {
			return false
		}
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", rows[0].OSPID))
		if err != nil {
			return false
		}
		// Manager must retain the kernel exit receipt until the original slave
		// writer ends; waiting for Manager.Exit here would require early reap.
		fields := strings.Fields(string(stat)[strings.LastIndexByte(string(stat), ')')+1:])
		return len(fields) > 0 && fields[0] == "Z"
	})
	// The old 200ms close used to drop this admission while the original
	// slave still had a writer. The S4 owner must wait for actual source end.
	timer := time.NewTimer(350 * time.Millisecond)
	defer timer.Stop()
	for {
		assertBusy("draining")
		select {
		case <-timer.C:
			goto releaseTail
		default:
			runtime.Gosched()
		}
	}
releaseTail:
	if err := os.WriteFile(tailRelease, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(cutTestWait):
		t.Fatal("legacy tail never drained")
	}
	// origin: hot-update S4 plan §7.1 / T29. The handler has joined after
	// publishing its exit and tail. Flush orders their delivery to this same
	// connection; Pending then drains every queued message without two fixed
	// one-second waits for streams that have already ended.
	if err := nc.FlushTimeout(cutTestWait); err != nil {
		t.Fatal(err)
	}
	var output []byte
	exited := false
	replyCount, _, err := replies.Pending()
	if err != nil {
		t.Fatal(err)
	}
	for range replyCount {
		msg, err := replies.NextMsg(cutTestWait)
		if err != nil {
			t.Fatal(err)
		}
		var chunk proto.RunChunk
		if err := json.Unmarshal(msg.Data, &chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Kind == "exit" {
			exited = chunk.ExitCode == 23
		}
	}
	outputCount, _, err := outputSub.Pending()
	if err != nil {
		t.Fatal(err)
	}
	for range outputCount {
		msg, err := outputSub.NextMsg(cutTestWait)
		if err != nil {
			t.Fatal(err)
		}
		output = append(output, msg.Data...)
	}
	if !exited || !bytes.Contains(output, []byte("tail")) || a.gate.count(gateRun) != 0 {
		t.Fatalf("legacy source-end receipt: exited=%v output=%q admission=%d", exited, output, a.gate.count(gateRun))
	}
}

// origin: hot-update S4 plan §7.1 / T29. A real allocation after preflight
// must be visible to the closed-gate cut, including before attach or spawn.
//
// origin: hot-update S8 plan §3.3 / §4.1 / §7.2; S8 FIX-FIXTURES. The grantless
// envelope no longer downloads, so the real allocation is made in the window
// a granted Start leaves between the read-only preflight and Freeze. The cut
// must defer on it before any disk effect; no marker or prev is written.
func domainExerciseLateLegacyRun(t *testing.T, nc *nats.Conn) {
	t.Helper()
	a := cutTestAgent(t)
	a.setRunCtx(t.Context())
	dst := a.cfg.UpgradeExecutablePath
	before := mustSHA(t, dst)
	subject, inbox := nats.NewInbox(), nats.NewInbox()
	replies, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replies.Unsubscribe() }()
	done := make(chan struct{})
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) { defer close(done); a.handleRunForwarded(nc, m) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	request, hits := cutServer(t, a, cutCandidate(t, 1), nil)
	if response := cutForward(t, a, nc, request); response.OK || response.Code != "upgrade_deferred" || hits.Load() != 0 {
		t.Fatalf("grantless envelope opened a download window: %+v downloads=%d", response, hits.Load())
	}
	if reasons := preflightDeferrals(a); len(reasons) != 0 {
		t.Fatalf("fixture busy before the late allocation: %v", reasons)
	}
	var ready proto.RunChunk
	defer func() {
		// Release the original real allocation on every exit path instead of
		// leaking it, including an assertion failure after the reply.
		if ready.PID == "" {
			return
		}
		attach, _ := json.Marshal(proto.PtyAttachEvent{Cols: 80, Rows: 24})
		_ = nc.Publish(proto.SubjPtyAttach(a.cfg.SID, ready.PID), attach)
		select {
		case <-done:
		case <-time.After(cutTestWait):
			t.Error("late legacy allocation did not finish")
		}
	}()
	data, err := json.Marshal(proto.RunReq{Cwd: t.TempDir(), Argv: []string{"/bin/true"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.PublishRequest(subject, inbox, data); err != nil {
		t.Fatal(err)
	}
	msg, err := replies.NextMsg(cutTestWait)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(msg.Data, &ready); err != nil || ready.Kind != "ready" || ready.PID == "" {
		t.Fatalf("late allocation receipt=%+v err=%v", ready, err)
	}
	a.gate.close()
	cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
	if err == nil {
		_ = cut.abandon(a)
		t.Fatal("late legacy allocation reached the final cut")
	}
	if !a.processRuntime.Frozen() {
		a.gate.open()
	}
	var deferred *agentimage.DeferredError
	if !errors.As(err, &deferred) || deferred.Reason != agentimage.ReasonLegacySessionBusy {
		t.Fatalf("late legacy cut err=%v", err)
	}
	if _, err := os.Stat(upgradeMarkerPath(dst)); !os.IsNotExist(err) || mustSHA(t, dst) != before {
		t.Fatalf("late legacy cut touched disk: marker_err=%v", err)
	}
	cutRequireOpen(t, a)
}

func exerciseOwnedFlowUpgradeAdmission(t *testing.T) {
	t.Helper()
	a := cutTestAgent(t)
	var paused atomic.Bool
	entered, proceed := make(chan struct{}, 1), make(chan struct{})
	adapter, _, target := flowDomainAdapter(t, a, func(req tunnel.AuthRequest) error {
		if !paused.Load() {
			return nil
		}
		select {
		case entered <- struct{}{}:
		default:
		}
		timer := time.NewTimer(max(0, time.Until(req.Deadline)))
		defer timer.Stop()
		select {
		case <-proceed:
			return nil
		case <-req.Stop:
			return context.Canceled
		case <-timer.C:
			return context.DeadlineExceeded
		}
	})
	if reasons := preflightDeferrals(a); len(reasons) != 0 {
		t.Fatalf("typed v1 idle owner was classified as busy (pending P14/P17): %v", reasons)
	}
	// A real open remains inside both the agent gate and the typed client's
	// census while authorization is blocked. Counting it again as a transfer
	// would restore the removed flow_busy veto under a different reason.
	// origin: hot-update S5 plan §7.3 / T37
	public := flowFailureListener(t)
	port := public.Addr().(*net.TCPAddr).Port
	_ = public.Close()
	release, admitted := a.gate.admit(gateExpose)
	if !admitted {
		t.Fatal("typed expose fixture could not enter admission")
	}
	paused.Store(true)
	done := make(chan error, 1)
	go func() {
		err := adapter.AddProxy(t.Context(), PortToken{Port: port, LocalPort: target.Addr().(*net.TCPAddr).Port, Token: "fixture"})
		release()
		done <- err
	}()
	defer func() {
		close(proceed)
		if err := <-done; err != nil {
			t.Error(err)
		}
		adapter.client.Close(port)
	}()
	select {
	case <-entered:
	case <-time.After(cutTestWait):
		t.Fatal("typed open never reached authorization")
	}
	// OpenHome and its nested dial each register responsibility; their census
	// must cover the agent's admission, without requiring a one-to-one count.
	if a.gate.count(gateExpose) != 1 || adapter.client.FlowCensus().Admission < 1 {
		t.Fatal("typed open admission premise is empty")
	}
	if reasons := preflightDeferrals(a); len(reasons) != 0 {
		t.Fatalf("typed v1 in-flight open was classified as busy: %v", reasons)
	}
}
