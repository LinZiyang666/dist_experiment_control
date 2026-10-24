//go:build linux

package agent

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/nats-io/nats.go"
)

func domainScript(domains string, abi int) string {
	s := "#!/bin/sh\nprintf '%s\\n' '" + proto.VersionLine() + "'\n"
	if domains != "" {
		s += "printf '%s\\n' 'image-domains " + domains + "'\n"
	}
	if abi != 0 {
		s += fmt.Sprintf("printf '%%s\\n' 'image-abi v%d'\n", abi)
	}
	return s
}

// origin: hot-update S4 plan §5.2 / T30.
func TestPTYCandidateDomainsAndSiblingPrev(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	t.Run("reader_policy", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		for _, tc := range []struct {
			name     string
			reader   agentimage.Capabilities
			required []string
			refuse   bool
		}{
			{"exec_only_s3", agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1"}}, []string{"exec-v1"}, false},
			{"pty_without_domain", agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1"}}, []string{"exec-v1", "pty-v1"}, true},
			{"unknown_domain_is_not_pty", agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1", "future-v1"}}, []string{"exec-v1", "pty-v1"}, true},
			{"pty_reader", agentimage.CurrentCapabilities("sha"), []string{"exec-v1", "pty-v1"}, false},
			{"future_abi_empty", agentimage.Capabilities{ABI: 2, Domains: []string{"exec-v1", "pty-v1"}}, nil, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				err := agentimage.CheckDomains(tc.reader, tc.required)
				if (err != nil) != tc.refuse || err != nil && !errors.Is(err, errImageABIUnsupported) {
					t.Fatalf("domain policy refusal=%v, want %v", err, tc.refuse)
				}
				if tc.refuse && tc.reader.ABI == 1 && err.Error() != "upgrade: candidate image domain is unsupported: requires pty-v1" {
					t.Fatalf("domain diagnosis=%q", err)
				}
			})
		}
	})
	t.Run("prev_selection", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		candidate := agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1"}, SHA256: "candidate"}
		for _, tc := range []struct {
			name, running, disk string
			refuse              bool
		}{
			{"running", "running", "running", false},
			{"shared_sibling", "running", "candidate", false},
			{"third_sha", "running", "unrelated", true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				previous, err := agentimage.SelectPreviousCapabilities(tc.running, tc.disk, candidate)
				if errors.Is(err, errRunningImageDiffers) != tc.refuse {
					t.Fatalf("prev selection err=%v want refused=%v", err, tc.refuse)
				}
				if tc.refuse {
					return
				}
				if previous.SHA256 != tc.disk {
					t.Fatal("selected reader is not bound to actual prev bytes")
				}
				domainErr := agentimage.CheckDomains(previous, []string{"exec-v1", "pty-v1"})
				if tc.disk == "candidate" {
					if !reflect.DeepEqual(previous, candidate) || !errors.Is(domainErr, errImageABIUnsupported) {
						t.Fatal("actual candidate prev capabilities were replaced by exporter capabilities")
					}
				} else if domainErr != nil {
					t.Fatal(domainErr)
				}
			})
		}
	})
	t.Run("retained_summary", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		// These metadata shapes deliberately have no OS resources. Settled rows
		// and clock uncertainty must require the domain after their task is gone.
		for _, state := range []string{"running", "terminal", "tombstone", "settled", "clock_unknown"} {
			t.Run(state, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				var raw string
				switch state {
				case "running", "terminal":
					raw = `{"tasks":[{"kind":"pty","pid":"retained"}]}`
				case "tombstone":
					raw = `{"tombstones":[{"kind":"pty","pid":"retained"}]}`
				case "settled":
					raw = `{"registry":{"records":[{"kind":"pty","pid":"retained","phase":"settled"}]}}`
				case "clock_unknown":
					raw = `{"revocations":[{"sid":"lab","clock_unknown":true}]}`
				}
				var image execsession.Image
				if err := json.Unmarshal([]byte(raw), &image); err != nil {
					t.Fatal(err)
				}
				// A P0 type lacking these fields is a pending dependency, not a fixture
				// that may quietly discard the extension and claim backward readability.
				encoded, err := json.Marshal(image)
				if err != nil {
					t.Fatal(err)
				}
				field := `"kind":"pty"`
				if state == "clock_unknown" {
					field = `"clock_unknown":true`
				}
				if !bytes.Contains(encoded, []byte(field)) {
					t.Fatal("S4 retained-state fields are not implemented (pending P4/P6)")
				}
				required := execsession.RequiredDomains(image)
				if err := agentimage.CheckDomains(agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1"}}, required); !errors.Is(err, errImageABIUnsupported) {
					t.Fatal("retained PTY extension was offered to an S3 reader")
				}
				if err := agentimage.CheckDomains(agentimage.CurrentCapabilities("sha"), required); err != nil {
					t.Fatal(err)
				}
			})
		}
	})
	t.Run("actual_prev_declaration", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		dst := filepath.Join(t.TempDir(), "tether")
		script := domainScript("", 1)
		if err := os.WriteFile(upgradePrevPath(dst), []byte(script), 0755); err != nil {
			t.Fatal(err)
		}
		sha := mustSHA(t, upgradePrevPath(dst))
		marker := &upgradeMarker{PrevSHA: sha}
		caps, err := previousImageCapabilities(dst, marker)
		if err != nil || caps.SHA256 != sha || !reflect.DeepEqual(caps.Domains, []string{"exec-v1"}) {
			t.Fatalf("old marker probe: caps=%+v err=%v", caps, err)
		}
		if err := agentimage.CheckDomains(caps, []string{"exec-v1", "pty-v1"}); !errors.Is(err, errImageABIUnsupported) {
			t.Fatal("actual prev capability was ignored")
		}
		marker.PrevImageSHA256, marker.PrevImageABI, marker.PrevImageDomains = "wrong-sha", 1, []string{"exec-v1", "pty-v1"}
		if _, err := previousImageCapabilities(dst, marker); err == nil {
			t.Fatal("unbound prev declaration accepted")
		}
		marker.PrevImageSHA256 = sha
		if err := os.WriteFile(upgradePrevPath(dst), []byte(domainScript("exec-v1 pty-v1", 1)), 0755); err != nil {
			t.Fatal(err)
		}
		if _, err := previousImageCapabilities(dst, marker); err == nil {
			t.Fatal("changed prev bytes accepted from cached declaration")
		}
	})
	t.Run("sibling", TestInstallPrevSlotAcceptsOnlyRollbackCapableImages)
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, state := range []string{"live", "terminal", "tombstone", "settled", "clock_unknown"} {
			t.Run(fmt.Sprintf("retained_install/%d/%s", round, state), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				a := cutTestAgent(t)
				domainRetainPTY(t, a, state)
				dst := a.cfg.UpgradeExecutablePath
				before := mustSHA(t, dst)
				script := domainScript("", 1)
				req, hits := cutServer(t, a, testTarball(t, []byte(script)), nil)
				// origin: hot-update S8 plan §3.3 / §6.1 / §7.2; S8 FIX-FIXTURES.
				// The grantless envelope is refused before download; the retained
				// domain is refused by the candidate policy over the actual probe
				// of the exact bytes, the check a granted Preflight composes.
				response := cutForward(t, a, cutNATS(t), req)
				if response.OK || response.Code != "upgrade_deferred" || hits.Load() != 0 {
					t.Fatalf("grantless envelope reached candidate validation: %+v", response)
				}
				_, reader, err := smokeVersionCapabilities(writeScript(t, script))
				if err != nil || reader.SHA256 != sha256OfBytes([]byte(script)) {
					t.Fatalf("candidate probe: %+v %v", reader, err)
				}
				if err := checkCandidateABI(a, reader); !errors.Is(err, errImageABIUnsupported) || !strings.Contains(err.Error(), "requires pty-v1") {
					t.Fatalf("retained %s admitted: %v", state, err)
				}
				domainRequireUnchanged(t, dst, before)
				cutRequireOpen(t, a)
			})
		}
	}
	testharness.AssertNoFDLeak(t, "retained PTY domain installs x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "retained PTY domain installs x5", beforeG)
}

func domainRequireUnchanged(t *testing.T, dst, before string) {
	t.Helper()
	if mustSHA(t, dst) != before {
		t.Fatal("domain refusal modified executable")
	}
	for _, path := range []string{upgradePrevPath(dst), upgradeMarkerPath(dst)} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("domain refusal mutated %s: %v", path, err)
		}
	}
}

// Clock injection changes the real runtime's responsibility; no image is
// rewritten to erase or synthesize its PTY records.
func domainRetainPTY(t *testing.T, a *Agent, state string) {
	t.Helper()
	stopProcessRuntime(t.Context(), a)
	cfg := processRuntimeConfig(a)
	// Replacement runtimes need a fresh registry incarnation, as the real
	// initProcessRuntime path supplies before issuing any lease.
	// origin: hot-update S4 plan §5.2 / T30.
	cfg.Instance = "retained-" + a.instanceID
	now, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	var clock atomic.Int64
	clock.Store(now)
	var broken atomic.Bool
	cfg.Clock = func() (int64, error) {
		if broken.Load() {
			return 0, syscall.EIO
		}
		// A live owner shares the real BOOTTIME axis with outer admission.
		// Only terminal-expiry cases need a manually advanced clock.
		// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
		if state == "live" {
			return procrequest.BootNow()
		}
		return clock.Load(), nil
	}
	rt, err := execsession.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	a.processRuntime = rt
	if state == "clock_unknown" {
		broken.Store(true)
		_, err := rt.Revoke(a.cfg.SID, "removed")
		broken.Store(false)
		if err == nil {
			t.Fatal("clock failure was not observed")
		}
		return
	}
	task := domainStartPTY(t, a, nil)
	if state == "live" {
		return
	}
	if err := os.WriteFile(task.release, nil, 0600); err != nil {
		t.Fatal(err)
	}
	reply := domainWaitTerminal(t, a, task)
	if state == "terminal" {
		return
	}
	if state == "tombstone" {
		clock.Add((execsession.AbandonedTerminalGrace + time.Minute).Nanoseconds())
		cutWait(t, "PTY task discarded to tombstone", func() bool { _, err := rt.Sweep(); return err == nil && len(rt.TaskPIDs()) == 0 })
		return
	}
	if reply.TerminalID == "" {
		t.Fatal("PTY terminal has no settlement identity")
	}
	_, err = cutHandle(a, task.scope, proto.ProcessResumeReq{Kind: "pty", Op: "terminal-ack", Handle: &task.handle, AttachEpoch: 1, StdoutACK: reply.Stdout.Offset + uint64(len(reply.Stdout.Data)), StdoutFIN: true, StderrFIN: true, TerminalID: reply.TerminalID})
	if err != nil {
		t.Fatal(err)
	}
	if len(rt.TaskPIDs()) != 0 {
		t.Fatal("settled PTY still has a runtime task")
	}
}

func domainStartPTY(t *testing.T, a *Agent, argv []string) cutTask {
	t.Helper()
	scope := cutTaskScope(a)
	probe, err := cutHandle(a, scope, proto.ProcessResumeReq{Kind: "pty", Op: "probe"})
	if err != nil || probe.Lease == nil || probe.PTYVersion != 1 {
		t.Fatalf("real PTY capability unavailable (pending P6/P8): version=%d err=%v", probe.PTYVersion, err)
	}
	h := proto.ProcessHandle{Lease: *probe.Lease, RequestID: "domain-pty"}
	release := filepath.Join(t.TempDir(), "release-pty")
	if argv == nil {
		argv = []string{"/bin/sh", "-c", "stty raw -echo; printf 'before\\n'; while [ ! -e \"$1\" ]; do sleep 0.01; done; printf 'after\\n'; exit 23", "sh", release}
	}
	reply, err := cutHandle(a, scope, proto.ProcessResumeReq{Kind: "pty", Op: "create", Handle: &h, AttachEpoch: 1, Run: &proto.RunReq{Cwd: t.TempDir(), Argv: argv, Cols: 80, Rows: 24}})
	if err != nil || reply.PID == "" {
		t.Fatalf("real PTY create: pid=%q err=%v", reply.PID, err)
	}
	task := cutTask{scope: scope, handle: h, pid: reply.PID, release: release}
	cutWait(t, "original PTY output", func() bool {
		r, e := cutHandle(a, scope, proto.ProcessResumeReq{Kind: "pty", Op: "resume", Handle: &h, AttachEpoch: 1})
		return e == nil && r.Stdout != nil && string(r.Stdout.Data) == "before\n"
	})
	if a.gate.count(gateRun) != 0 {
		t.Fatal("reliable PTY incorrectly reserves gateRun")
	}
	a.procsMu.Lock()
	legacy := len(a.procs)
	a.procsMu.Unlock()
	if legacy != 0 {
		t.Fatal("reliable PTY incorrectly registered in legacy procs")
	}
	return task
}

func cutTaskScope(a *Agent) procrequest.Scope {
	// A fixed valid OpenSSH fingerprint; no private key enters these fixtures.
	return procrequest.Scope{SID: a.cfg.SID, NID: a.cfg.NID, ActorFP: "SHA256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", ClientLineage: "domain-client"}
}

func domainWaitTerminal(t *testing.T, a *Agent, task cutTask) proto.ProcessResumeReply {
	t.Helper()
	var terminal proto.ProcessResumeReply
	cutWait(t, "original PTY terminal", func() bool {
		r, e := cutHandle(a, task.scope, proto.ProcessResumeReq{Kind: "pty", Op: "resume", Handle: &task.handle, AttachEpoch: 1})
		terminal = r
		return e == nil && r.Phase == "terminal" && r.Stdout != nil && r.Stdout.FIN && r.Stderr != nil && r.Stderr.FIN && r.ExitCode == 23
	})
	return terminal
}

// origin: hot-update S4 plan §5.2 / T31.
func TestPTYPendingAdmissionCannotFallBack(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	t.Run("durable_policy", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		for _, arm := range []string{"pending", "failed_commit", "failed_commit_dir", "sibling", "pty_prev", "old_marker", "bad_prev_sha"} {
			t.Run(arm, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				dst := filepath.Join(t.TempDir(), "tether")
				if err := os.WriteFile(dst, []byte(domainScript("exec-v1 pty-v1", 1)), 0755); err != nil {
					t.Fatal(err)
				}
				domains := ""
				if arm == "pty_prev" {
					domains = "exec-v1 pty-v1"
				}
				if err := os.WriteFile(upgradePrevPath(dst), []byte(domainScript(domains, 1)), 0755); err != nil {
					t.Fatal(err)
				}
				m := &upgradeMarker{State: upgradeStatePending, PrevSHA: mustSHA(t, upgradePrevPath(dst)), NewSHA: mustSHA(t, dst), TargetSID: "lab", TargetNID: "domain-agent", TargetInstance: "owner", UpgradeID: "pending-id", BootCount: 1, BootBudget: 3, Deadline: time.Now().Add(time.Minute)}
				if arm != "old_marker" {
					m.PrevImageSHA256, m.PrevImageABI, m.PrevImageDomains = m.PrevSHA, 1, []string{"exec-v1"}
					if arm == "pty_prev" {
						m.PrevImageDomains = append(m.PrevImageDomains, "pty-v1")
					}
				}
				if arm == "bad_prev_sha" {
					m.PrevImageSHA256 = "bad-sha"
				}
				if err := writeUpgradeMarker(upgradeMarkerPath(dst), m); err != nil {
					t.Fatal(err)
				}
				a := &Agent{cfg: Config{SID: "lab", NID: "domain-agent", UpgradeExecutablePath: dst, UpgradeRunningImagePath: dst, Logger: testharness.SilentLog()}, instanceID: "owner", upgradeBootProofID: m.UpgradeID}
				if arm == "sibling" {
					a.instanceID = "sibling"
					a.upgradeBootProofID = ""
				}
				err := initPTYUpgradeAdmission(a)
				if (err != nil) != (arm == "bad_prev_sha") {
					t.Fatalf("initializer err=%v", err)
				}
				blocked := arm != "sibling" && arm != "pty_prev"
				domainRequirePTYAdmission(t, a, blocked)
				// Opening the transient cut gate must preserve the pending restriction.
				a.gate.close()
				a.gate.open()
				domainRequirePTYAdmission(t, a, blocked)
				if !blocked {
					return
				}
				releasePTYUpgradeAdmission(a, "foreign-id")
				domainRequirePTYAdmission(t, a, true)
				original := upgradeSyncObserver
				defer func() { upgradeSyncObserver = original }()
				if strings.HasPrefix(arm, "failed_commit") {
					upgradeSyncObserver = func(kind, path string) error {
						if arm == "failed_commit" && kind == "file" && strings.HasPrefix(filepath.Base(path), ".tether-upgrade-marker-") || arm == "failed_commit_dir" && kind == "dir" && path == filepath.Dir(dst) {
							return syscall.EIO
						}
						return nil
					}
				}
				a.commitUpgradeAfterRegister(upgradeStateCommitted)
				domainRequirePTYAdmission(t, a, strings.HasPrefix(arm, "failed_commit"))
				marker := readMarkerState(t, upgradeMarkerPath(dst))
				want := upgradeStateCommitted
				if arm == "failed_commit" {
					want = upgradeStatePending
				}
				if marker.State != want {
					t.Fatalf("commit state=%s want=%s", marker.State, want)
				}
				if strings.HasPrefix(arm, "failed_commit") {
					upgradeSyncObserver = original
					a.commitUpgradeAfterRegister(upgradeStateCommitted)
					domainRequirePTYAdmission(t, a, false)
					if readMarkerState(t, upgradeMarkerPath(dst)).State != upgradeStateCommitted {
						t.Fatal("durable commit retry did not land")
					}
				}
			})
		}
	})
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("mark_ready/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			cutImportActive(t, a)
			m := cutPending(t, a)
			if err := os.WriteFile(upgradePrevPath(a.cfg.UpgradeExecutablePath), []byte(domainScript("", 1)), 0755); err != nil {
				t.Fatal(err)
			}
			m.PrevSHA = mustSHA(t, upgradePrevPath(a.cfg.UpgradeExecutablePath))
			if err := writeUpgradeMarker(upgradeMarkerPath(a.cfg.UpgradeExecutablePath), m); err != nil {
				t.Fatal(err)
			}
			a.upgradeBootProofID = m.UpgradeID
			if err := initPTYUpgradeAdmission(a); err != nil {
				t.Fatal(err)
			}
			a.image.MarkReady()
			domainRequirePTYAdmission(t, a, true)
			if readMarkerState(t, upgradeMarkerPath(a.cfg.UpgradeExecutablePath)).State != upgradeStatePending {
				t.Fatal("MarkReady committed the marker")
			}
			a.commitUpgradeAfterRegister(upgradeStateCommitted)
			domainRequirePTYAdmission(t, a, false)
		})
		t.Run(fmt.Sprintf("before_subscriptions/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := domainNewPendingAgent(t)
			// New must close both forms of PTY admission before Run can subscribe.
			domainRequirePTYAdmission(t, a, true)
			cfg := processRuntimeConfig(a)
			cfg.Instance = "pending-" + a.instanceID
			if cfg.StartPTY == nil || cfg.PTYAdmission == nil {
				t.Fatal("production PTY admission wiring is not implemented (pending P8)")
			}
			var starts atomic.Int32
			original := cfg.StartPTY
			cfg.StartPTY = func(pid string, req *proto.RunReq) (*pty.Session, string, error) {
				starts.Add(1)
				return original(pid, req)
			}
			stopProcessRuntime(t.Context(), a)
			rt, err := execsession.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			a.processRuntime = rt
			scope := cutTaskScope(a)
			for _, op := range []string{"probe", "pty-probe"} {
				r, e := cutHandle(a, scope, proto.ProcessResumeReq{Kind: "pty", Op: op, ClientLineage: scope.ClientLineage})
				if e != nil || r.Kind != "capability" || r.PTYVersion != 1 || r.Lease == nil {
					t.Fatalf("pending hides real capability: version=%d err=%v", r.PTYVersion, e)
				}
				h := proto.ProcessHandle{Lease: *r.Lease, RequestID: "must-not-start-" + op}
				_, e = cutHandle(a, scope, proto.ProcessResumeReq{Kind: "pty", Op: "create", Handle: &h, AttachEpoch: 1, Run: &proto.RunReq{Cwd: t.TempDir(), Argv: []string{"/bin/true"}}})
				if !errors.Is(e, procrequest.ErrBusy) {
					t.Fatalf("pending create err=%v want busy", e)
				}
			}
			nc := cutNATS(t)
			a.setRunCtx(t.Context())
			subject := nats.NewInbox()
			sub, err := nc.Subscribe(subject, func(m *nats.Msg) { a.handleRunForwarded(nc, m) })
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			body, _ := json.Marshal(proto.RunReq{Cwd: t.TempDir(), Argv: []string{"/bin/true"}})
			msg, err := nc.Request(subject, body, cutTestWait)
			if err != nil {
				t.Fatal(err)
			}
			var chunk proto.RunChunk
			if err := json.Unmarshal(msg.Data, &chunk); err != nil {
				t.Fatal(err)
			}
			if chunk.Reason != "try_again: agent_upgrading: image switch in progress; retry" || starts.Load() != 0 || len(a.workloads.Snapshot()) != 0 {
				t.Fatalf("pending fallback started work: reason=%s starts=%d", chunk.Reason, starts.Load())
			}
		})
	}
	testharness.AssertNoFDLeak(t, "pending PTY admission x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "pending PTY admission x5", beforeG)
}

func domainRequirePTYAdmission(t *testing.T, a *Agent, blocked bool) {
	t.Helper()
	err := checkPTYUpgradeAdmission(a)
	if errors.Is(err, procrequest.ErrBusy) != blocked {
		t.Fatalf("reliable admission err=%v want blocked=%v", err, blocked)
	}
	release, ok := a.gate.admit(gateRun)
	if ok {
		release()
	}
	if ok == blocked {
		t.Fatalf("legacy admission open=%v want blocked=%v", ok, blocked)
	}
	release, ok = a.gate.admit(gateExec)
	if !ok {
		t.Fatal("pending PTY restriction blocks exec")
	}
	release()
}

func domainNewPendingAgent(t *testing.T) *Agent {
	t.Helper()
	dst := filepath.Join(t.TempDir(), "tether")
	if err := os.WriteFile(dst, []byte(domainScript("exec-v1 pty-v1", 1)), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(upgradePrevPath(dst), []byte(domainScript("", 1)), 0755); err != nil {
		t.Fatal(err)
	}
	m := &upgradeMarker{State: upgradeStatePending, PrevSHA: mustSHA(t, upgradePrevPath(dst)), NewSHA: mustSHA(t, dst), TargetSID: "lab", TargetNID: "domain-agent", UpgradeID: "boot-pending", BootCount: 1, BootBudget: 3, Deadline: time.Now().Add(time.Minute)}
	if err := writeUpgradeMarker(upgradeMarkerPath(dst), m); err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{SID: "lab", NID: "domain-agent", Home: t.TempDir(), NATSURL: "nats://127.0.0.1:1", RemoteFSMode: "off", Logger: testharness.SilentLog(), UpgradeExecutablePath: dst, UpgradeRunningImagePath: dst, UpgradeBootProofID: m.UpgradeID, UpgradeNoExit: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
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

// origin: hot-update S4 plan §5.2 / T46.
func TestPTYDomainVersionProbeAndPureExecReadability(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	const modeKey = "_TETHER_P9_EXEC_READER"
	if reader := os.Getenv(modeKey); reader != "" {
		receipt, release := os.Getenv(modeKey+"_RECEIPT"), os.Getenv(modeKey+"_RELEASE")
		// Business children must not inherit harness dispatch or handoff keys.
		for _, key := range []string{modeKey, modeKey + "_RECEIPT", modeKey + "_RELEASE"} {
			_ = os.Unsetenv(key)
		}
		// The outer fence starts before a still-admitted task advances.
		// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
		admission, err := procrequest.BootNow()
		if err != nil {
			t.Fatal(err)
		}
		a := cutTestAgent(t)
		task := cutStartTask(t, a)
		rows := a.workloads.Snapshot()
		if len(rows) != 1 || rows[0].OSPID <= 1 || rows[0].StartTimeTicks <= 0 {
			t.Fatal("nonempty child identity missing")
		}
		fixture := struct {
			PID, ChildPID    int
			Ticks, Admission int64
			Release          string
		}{os.Getpid(), rows[0].OSPID, rows[0].StartTimeTicks, admission, task.release}
		data, err := json.Marshal(fixture)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(release, data, 0600); err != nil {
			t.Fatal(err)
		}
		frozen, err := agentimage.Freeze(agentimage.Owners{Runtime: a.processRuntime, Manager: &a.workloads, Lineage: agentimage.Lineage{Generation: 1}, FrozenAtBootNS: admission}, time.Now().Add(upgradebudget.SafePointWait))
		if err != nil {
			t.Fatal(err)
		}
		// This receipt tests the encoded exec schema against the real S3 reader;
		// the upgrade's checked prepare paths are independently guarded by T32.
		prepared, err := frozen.Prepare()
		if err != nil {
			t.Fatal(err)
		}
		if err := prepared.Exec(reader, []string{reader, receipt, release}, handoff.CleanEnvironment(os.Environ())); err != nil {
			t.Fatal(err)
		}
		t.Fatal("exec returned success")
	}
	t.Run("parsing", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		first := proto.VersionLine() + "\n"
		abi := proto.ImageABILine() + "\n"
		domains := proto.ImageDomainsLine() + "\n"
		for _, tc := range []struct {
			name, output string
			wantABI      int
			wantDomains  []string
			refuse       bool
		}{
			// origin: hot-update S7 plan §8.4 / P21. Reader declarations are
			// additive; accepting the token does not invent a capable owner.
			{"s7", first + "image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1\n" + abi, 1, []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1", "broker-image-v1", "transfer-v1"}, false},
			{"s6", first + "linux/amd64\ngo1.26.8\nimage-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1\n" + abi, 1, []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1", "broker-image-v1"}, false},
			// An S5 candidate advertises exactly the four agent domains.
			{"s5", first + "linux/amd64\ngo1.26.8\nimage-domains exec-v1 pty-v1 flow-v1 ss-v1\n" + abi, 1, []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1"}, false},
			{"s4", first + "image-domains exec-v1 pty-v1\n" + abi, 1, []string{"exec-v1", "pty-v1"}, false},
			{"s3", first + abi, 1, []string{"exec-v1"}, false},
			{"bare", first, 0, nil, false},
			{"unknown_token", first + "image-domains exec-v1 pty-v1 future-v2\n" + abi, 1, []string{"exec-v1", "pty-v1", "future-v2"}, false},
			{"bad_first", domains + first + abi, 0, nil, true},
			{"abi_not_last", first + abi + domains, 0, nil, true},
			{"abi_duplicate", first + abi + abi, 0, nil, true},
			{"invalid_abi_then_valid", first + "image-abi vx\n" + abi, 0, nil, true},
			{"malformed_abi_prefix", first + "image-abi 1\n", 0, nil, true},
			{"abi_zero", first + "image-abi v0\n", 0, nil, true},
			{"domains_duplicate", first + domains + domains + abi, 0, nil, true},
			{"malformed_domains_then_valid", first + "image-domains\n" + domains + abi, 0, nil, true},
			{"duplicate_token", first + "image-domains exec-v1 exec-v1\n" + abi, 0, nil, true},
			{"malformed_token", first + "image-domains exec-v1 pty/v1\n" + abi, 0, nil, true},
			{"domains_without_abi", first + domains, 0, nil, true},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				release, caps, err := smokeVersionCapabilities(writeScript(t, "#!/bin/sh\ncat <<'VERSION'\n"+tc.output+"VERSION\n"))
				if (err != nil) != tc.refuse {
					t.Fatalf("refusal=%v want=%v", err, tc.refuse)
				}
				if !tc.refuse && (!proto.SameRelease(release, proto.ReleaseVersion) || caps.ABI != tc.wantABI || !reflect.DeepEqual(caps.Domains, tc.wantDomains) || caps.SHA256 == "") {
					t.Fatalf("capabilities=%+v release=%q", caps, release)
				}
			})
		}
	})
	t.Run("sha_binding", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		path := writeScript(t, domainScript("exec-v1 pty-v1", 1))
		_, caps, err := smokeVersionCapabilities(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(domainScript("", 1)), 0755); err != nil {
			t.Fatal(err)
		}
		if err := agentimage.VerifyCapabilitySHA(path, caps); err == nil {
			t.Fatal("old capability authorizes replaced pathname")
		}
		// A rename during the isolated probe must be caught, even if both files
		// print perfectly valid output. The candidate mutates only its sandbox.
		replacement := writeScript(t, domainScript("", 1))
		body := domainScript("exec-v1 pty-v1", 1) + "/bin/mv '" + replacement + "' \"$0\"\n"
		if _, _, err := smokeVersionCapabilities(writeScript(t, body)); err == nil {
			t.Fatal("candidate replaced during probe was accepted")
		}
	})
	t.Run("bounded_output", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		writer := agentimage.NewVersionOutput(smokeOutputCap)
		data := bytes.Repeat([]byte("x"), 2*smokeOutputCap)
		n, err := writer.Write(data)
		if err != nil || n != len(data) || len(writer.String()) != smokeOutputCap || !writer.Overflowed() {
			t.Fatal("version output capture is not bounded and fail-closed")
		}
		body := domainScript("exec-v1 pty-v1", 1) + "/usr/bin/head -c 100000 /dev/zero\n"
		if _, _, err := smokeVersionCapabilities(writeScript(t, body)); err == nil {
			t.Fatal("oversized version output accepted")
		}
	})
	t.Run("real_emitter", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		binary := domainBuildBinary(t, "", "")
		cmd := exec.Command(binary, "version")
		cmd.Env = handoff.CleanEnvironment(os.Environ())
		out, err := cmd.Output()
		if err != nil {
			t.Fatal(err)
		}
		// origin: hot-update S8 plan §6.1 / P13; S8 FIX-FIXTURES. The image
		// lines are the SSOT advertisement (domains, read epochs, ABI last);
		// the first line and the final ABI line keep their N-1 positions.
		lines := strings.Split(strings.TrimSpace(string(out)), "\n")
		image := proto.ImageVersionLines()
		// origin: S8 BROKER-EXPORTER R-11 (hot-update S8 plan §6.1 / §11.2 P17) —
		// the emitted domain line is the S6 domain line plus the S8 adjunct
		// domain, and the three callable S8 adapters are advertised.
		if len(lines) != 3+len(image) || lines[0] != proto.VersionLine() || !reflect.DeepEqual(lines[3:], image) ||
			image[0] != proto.ImageDomainsLine()+" upgrade-cut-v1" || lines[len(lines)-1] != proto.ImageABILine() {
			t.Fatalf("real version seam=%q", out)
		}
		release, caps, err := smokeVersionCapabilities(binary)
		// The S7 emitter belongs to P01. Keep the complete assertion here so
		// accepting a synthetic reader line cannot sign the actual binary.
		// origin: hot-update S7 plan §8.4 / P21; real emitter pending P01
		if err != nil || !proto.SameRelease(release, proto.ReleaseVersion) || !reflect.DeepEqual(caps.Domains, []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1", "broker-image-v1", "transfer-v1", "upgrade-cut-v1"}) ||
			!reflect.DeepEqual(caps.Features, []string{"upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1"}) || caps.SHA256 != mustSHA(t, binary) {
			t.Fatalf("real probe caps=%+v err=%v", caps, err)
		}
	})
	t.Run("pure_exec_s3_reader", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		reader := domainBuildS3Reader(t)
		beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
		for round := 0; round < 5; round++ {
			t.Run(strconv.Itoa(round), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				dir := t.TempDir()
				receipt, fixture := filepath.Join(dir, "receipt.json"), filepath.Join(dir, "fixture.json")
				// origin: hot-update S5 PROC-LEAK. The old reader can reject
				// after exec has erased the exporting fixture's Cleanup callbacks.
				t.Cleanup(func() {
					data, err := os.ReadFile(fixture)
					var child struct{ ChildPID int }
					if err == nil && json.Unmarshal(data, &child) == nil && child.ChildPID > 0 {
						_ = syscall.Kill(-child.ChildPID, syscall.SIGKILL)
					}
				})
				ctx, cancel := context.WithTimeout(t.Context(), 2*cutTestWait)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestPTYDomainVersionProbeAndPureExecReadability$", "-test.count=1")
				cmd.Dir = dir
				cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "TMPDIR="+dir, modeKey+"="+reader, modeKey+"_RECEIPT="+receipt, modeKey+"_RELEASE="+fixture)
				cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
				cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
				cmd.WaitDelay = time.Second
				defer func() {
					if cmd.Process != nil {
						_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
					}
				}()
				out, err := cmd.CombinedOutput()
				if err != nil {
					t.Fatalf("same-PID S3 reader: %v\n%s", err, out)
				}
				if bytes.Contains(out, []byte("secret")) {
					t.Fatal("reader printed secret-bearing metadata")
				}
				var got struct {
					PID, ChildPID, Exit                            int
					Ticks                                          int64
					Output                                         string
					Resources, ExecResources, CalibrationResources int
					CalibrationAt                                  int64
				}
				data, err := os.ReadFile(receipt)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &got); err != nil {
					t.Fatal(err)
				}
				var before struct {
					PID, ChildPID    int
					Ticks, Admission int64
					Release          string
				}
				data, err = os.ReadFile(fixture)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(data, &before); err != nil {
					t.Fatal(err)
				}
				if got.PID != before.PID || got.PID != cmd.Process.Pid || got.ChildPID != before.ChildPID || got.Ticks != before.Ticks || got.Exit != 23 || got.Output != "ore\nafter\n" || got.Resources != 2 || got.ExecResources != 1 || got.CalibrationResources != 1 || got.CalibrationAt != before.Admission {
					t.Fatalf("S3 continuity receipt=%+v before=%+v", got, before)
				}
			})
		}
		testharness.AssertNoFDLeak(t, "pure exec S3 reader x5", beforeFD, 0)
		testharness.AssertNoGoroutineLeak(t, "pure exec S3 reader x5", beforeG)
	})
}

// Nested builds inherit GOFLAGS so overlays reach the real product emitter.
// The preceding release reader is intentionally built from its frozen source.
func domainBuildBinary(t *testing.T, dir, target string) string {
	t.Helper()
	if dir == "" {
		var err error
		dir, err = filepath.Abs("../..")
		if err != nil {
			t.Fatal(err)
		}
	}
	if target == "" {
		target = "./cmd/tether"
	}
	output := filepath.Join(t.TempDir(), "reader")
	cmd := exec.Command("go", "build", "-trimpath", "-buildvcs=false", "-o", output, target)
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "-race" && setting.Value == "true" {
				cmd.Args = append(cmd.Args[:2:2], append([]string{"-race"}, cmd.Args[2:]...)...)
			}
		}
	}
	cmd.Dir = dir
	t.Logf("real binary build: %s", strings.Join(cmd.Args, " "))
	defer func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}()
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build real reader: %v\n%s", err, out)
	}
	return output
}

func domainBuildS3Reader(t *testing.T) string {
	t.Helper()
	const source = "0ec45bfeab0e614c1f900e8b29cb87e0091de981"
	dir := t.TempDir()
	archiveCmd := exec.Command("git", "archive", source)
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	archiveCmd.Dir = root
	archive, err := archiveCmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	tr := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(dir, header.Name)
		if header.Typeflag == tar.TypeDir {
			if err := os.MkdirAll(path, 0755); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if header.Typeflag != tar.TypeReg {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, os.FileMode(header.Mode)); err != nil {
			t.Fatal(err)
		}
	}
	// The adapter lives beside the frozen agent source so it can use the
	// real processRuntimeConfig, including its spawn/exit callbacks.
	adapter := filepath.Join(dir, "internal", "agent", "domain_reader_adapter.go")
	if err := os.WriteFile(adapter, []byte(domainS3ReaderSource), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "cmd", "domain-reader", "main.go")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	dispatcher := `package main
 import "github.com/LinZiyang666/tether/internal/agent"
 func main(){agent.ReadP9Exec()}
 `
	if err := os.WriteFile(path, []byte(dispatcher), 0600); err != nil {
		t.Fatal(err)
	}
	return domainBuildBinary(t, dir, "./cmd/domain-reader")
}

// This adapter uses the actual S3 Decode/Restore/Thaw implementation. Only
// its oracle is new; no S4 source, schema copy or permissive decoder is used.
const domainS3ReaderSource = `package agent
import (
 "context"
 "encoding/json"
 "fmt"
 "os"
 "path/filepath"
 "time"
 "github.com/LinZiyang666/tether/internal/agentimage"
 "github.com/LinZiyang666/tether/internal/handoff"
 "github.com/LinZiyang666/tether/internal/procrequest"
 "github.com/LinZiyang666/tether/internal/proto"
)
var step="fixture"
func fail(err error){if err!=nil {fmt.Fprintln(os.Stderr,"S3 reader failed at",step);os.Exit(1)}}
func ReadP9Exec(){
 var before struct{PID,ChildPID int;Ticks,Admission int64;Release string}
 data,err:=os.ReadFile(os.Args[2]);fail(err);fail(json.Unmarshal(data,&before))
 step="handoff restore"
 restored,err:=handoff.Restore();fail(err);if restored==nil {os.Exit(2)}
 step="bundle decode"
 bundle,err:=agentimage.Decode(restored.State);fail(err)
 if len(bundle.Runtime.Tasks)!=1||len(bundle.Processes.Records)!=1||bundle.Runtime.FrozenAtBootNS<=before.Admission {os.Exit(3)}
 // The real S3 redispatch must retain the optional resource without schema changes.
 resources,execResources,calibrationResources,calibrationAt:=len(restored.Resources),0,0,int64(0)
 for _,resource:=range restored.Borrow(){
  switch resource.Kind {
  case "exec-task":execResources++
  case "image-freeze-calibration-v1":
   if resource.ID!="image-freeze-calibration"||len(resource.Files)!=1||resource.Files["state"]==nil {os.Exit(7)}
   calibrationResources++;fail(json.Unmarshal(resource.Data,&calibrationAt))
  default:os.Exit(8)
  }
 }
 repacked,err:=restored.Repack();fail(err);fail(repacked.Close())
 a,err:=New(Config{SID:"lab",NID:"cut-agent",Home:filepath.Join(filepath.Dir(os.Args[1]),"home"),NATSURL:"nats://127.0.0.1:1",RemoteFSMode:"off"});fail(err)
 // ctx-root: the standalone receiver owns its unused adapter cleanup.
 setupCtx,setupCancel:=context.WithTimeout(context.Background(),10*time.Second)
 fail(a.processRuntime.Close(setupCtx));setupCancel()
 step="domain restore"
 imported,_,err:=agentimage.Restore(restored,agentimage.Target{Runtime:processRuntimeConfig(a)});fail(err)
 step="domain thaw"
  fail(imported.Thaw());fail(restored.Close())
 row:=a.workloads.Snapshot()[0]
 if row.OSPID!=before.ChildPID||row.StartTimeTicks!=before.Ticks||os.Getpid()!=before.PID {os.Exit(4)}
 again,err:=handoff.Restore();fail(err);if again!=nil {os.Exit(5)}
 step="business release"
 fail(os.WriteFile(before.Release,nil,0600))
 h:=bundle.Runtime.Tasks[0].Handle;l:=h.Lease
 scope:=procrequest.Scope{SID:l.SID,NID:l.NID,ActorFP:l.ActorFP,ClientLineage:l.ClientLineage}
 step="business result"
 deadline:=time.Now().Add(10*time.Second)
 var final proto.ProcessResumeReply
 for time.Now().Before(deadline){
  final,err=imported.Runtime.Handle(scope,proto.ProcessResumeReq{Version:proto.ProcessResumeVersion,CallID:1,Op:"resume",ActorFP:l.ActorFP,Handle:&h,AttachEpoch:1,StdoutACK:3},"_TINBOX.cut")
  fail(err)
  if final.Phase=="terminal"&&final.Stdout!=nil&&final.Stdout.FIN&&final.Stderr!=nil&&final.Stderr.FIN {break}
  time.Sleep(time.Millisecond)
 }
 if final.Phase!="terminal"||final.ExitCode!=23||final.Stdout==nil {os.Exit(6)}
 receipt:=struct{PID,ChildPID,Exit int;Ticks int64;Output string;Resources,ExecResources,CalibrationResources int;CalibrationAt int64}{os.Getpid(),row.OSPID,final.ExitCode,row.StartTimeTicks,string(final.Stdout.Data),resources,execResources,calibrationResources,calibrationAt}
 // ctx-root: this standalone reader owns the bounded cleanup lifetime.
 ctx,cancel:=context.WithTimeout(context.Background(),10*time.Second);defer cancel()
 fail(imported.Runtime.Close(ctx))
 data,err=json.Marshal(receipt);fail(err);fail(os.WriteFile(os.Args[1],data,0600))
}`

// origin: hot-update S4 plan §5.2 / T32.
func TestPTYFinalPrepareRechecksDomains(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	t.Run("checked_prepare_sites", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		// The install and ReExecOnly paths share one returning-exec funnel;
		// watchdog/early rollback has the other. Neither may bypass the callback
		// applied to the final re-export. Mutate these source guards on disk.
		// origin: hot-update S4 plan §5.2 / T32.
		checked := 0
		for _, path := range []string{"upgrade.go", "upgrade_image.go"} {
			source, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(source, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				// origin: hot-update S5 plan §7.2 / T33. The sealed proxy
				// wrapper preserves the caller's fresh bundle/SHA callback.
				if selector, ok := call.Fun.(*ast.SelectorExpr); ok {
					if frozen, ok := selector.X.(*ast.SelectorExpr); ok && frozen.Sel.Name == "frozen" && selector.Sel.Name == "Prepare" {
						t.Fatal("upgrade dispatch bypasses final domain callback")
					}
				}
				function, ok := call.Fun.(*ast.Ident)
				if !ok || function.Name != "prepareImageCut" {
					return true
				}
				checked++
				if len(call.Args) != 3 {
					t.Fatal("checked prepare has no validator")
				}
				validator, ok := call.Args[2].(*ast.FuncLit)
				if !ok {
					t.Fatal("checked prepare validator is not explicit")
				}
				validatesBundle, pinsSHA := false, false
				ast.Inspect(validator.Body, func(child ast.Node) bool {
					invocation, ok := child.(*ast.CallExpr)
					if !ok {
						return true
					}
					function, ok := invocation.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					owner, ok := function.X.(*ast.Ident)
					if !ok || owner.Name != "agentimage" {
						return true
					}
					if function.Sel.Name == "CheckBundleCapabilities" && len(invocation.Args) > 0 {
						argument, ok := invocation.Args[0].(*ast.Ident)
						validatesBundle = ok && argument.Name == "b"
					}
					pinsSHA = pinsSHA || function.Sel.Name == "VerifyCapabilitySHA"
					return true
				})
				if !validatesBundle || !pinsSHA {
					t.Fatal("final prepare does not validate the fresh bundle and pinned reader")
				}
				return true
			})
		}
		// origin: S8 FIX-AGENTWIRE. The S8 transaction cut (transactionCut.Prepare)
		// is the third funnel; it carries the adjunct beside the same check.
		if checked != 3 {
			t.Fatalf("checked dispatch funnels=%d want=3", checked)
		}
		// The wrapper must invoke the caller's validator from inside the
		// actual final re-export callback. Checking a cached bundle before it
		// enters PrepareChecked would miss a late PTY/flow/SS decision.
		file, err := parser.ParseFile(token.NewFileSet(), "upgrade_image.go", nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		forwarded := false
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != "prepareImageCut" {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != "PrepareChecked" || len(call.Args) == 0 {
					return true
				}
				callback, ok := call.Args[0].(*ast.FuncLit)
				if !ok {
					return true
				}
				ast.Inspect(callback.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok || len(call.Args) != 1 {
						return true
					}
					fn, ok := call.Fun.(*ast.Ident)
					if !ok || fn.Name != "check" {
						return true
					}
					arg, ok := call.Args[0].(*ast.Ident)
					forwarded = forwarded || ok && arg.Name == callback.Type.Params.List[0].Names[0].Name
					return true
				})
				return true
			})
		}
		if !forwarded {
			t.Fatal("sealed metadata wrapper bypasses the fresh final reader callback")
		}

	})

	t.Run("bare_retained_check", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		for _, image := range []execsession.Image{
			{Registry: procrequest.State{Records: []procrequest.Record{{PID: "retained", Phase: "settled"}}}},
			{Revocations: []execsession.RevocationImage{{SID: "lab", Reason: "removed"}}},
			{Tasks: []execsession.TaskImage{{PID: "retained"}}},
			{Tombstones: []execsession.TombstoneImage{{PID: "retained"}}},
		} {
			before, err := json.Marshal(image)
			if err != nil {
				t.Fatal(err)
			}
			if err := agentimage.CheckBundleCapabilities(agentimage.Bundle{Runtime: image}, agentimage.Capabilities{}); !errors.Is(err, errImageABIUnsupported) {
				t.Fatal("late retained state was offered to a bare-exec reader")
			}
			after, err := json.Marshal(image)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(before, after) {
				t.Fatal("late domain check erased retained metadata")
			}
		}
	})

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, window := range []string{"during_download", "after_probe", "before_prepare", "reexec_prepare", "watchdog_prepare"} {
			t.Run(fmt.Sprintf("%d/%s", round, window), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				a := cutTestAgent(t)
				dst := a.cfg.UpgradeExecutablePath
				before := mustSHA(t, dst)
				stopProcessRuntime(t.Context(), a)
				cfg := processRuntimeConfig(a)
				cfg.Instance = "late-" + a.instanceID
				var broken atomic.Bool
				cfg.Clock = func() (int64, error) {
					if broken.Load() {
						return 0, syscall.EIO
					}
					return procrequest.BootNow()
				}
				rt, err := execsession.New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				a.processRuntime = rt
				inject := func() {
					broken.Store(true)
					_, err := a.processRuntime.Revoke(a.cfg.SID, "late authority loss")
					broken.Store(false)
					if err == nil {
						t.Error("late clock failure was not observed")
					}
				}
				var execs atomic.Int32
				a.cfg.UpgradeExecFn = func(string) error { execs.Add(1); return syscall.ENOEXEC }
				if strings.Contains(window, "prepare") {
					a.cfg.UpgradeCutObserver = func(agentimage.Bundle) { inject() }
				}
				candidate := testTarball(t, []byte(domainScript("", 1)))
				req, hits := cutServer(t, a, candidate, nil)
				if window == "reexec_prepare" {
					if err := os.WriteFile(dst, []byte(domainScript("", 1)), 0755); err != nil {
						t.Fatal(err)
					}
					before = mustSHA(t, dst)
					req = proto.UpgradeForwardedReq{ReExecOnly: true, SHA256: before}
				}
				if window == "watchdog_prepare" {
					// Restore creates a new runtime. Its captured clock must share
					// this failure seam, and revocation must target that current owner.
					// origin: hot-update S4 plan §5.2 / T32.
					originalClock := processBootClock
					processBootClock = cfg.Clock
					defer func() { processBootClock = originalClock }()
					cutImportActive(t, a)
					m := cutPending(t, a)
					if err := os.WriteFile(upgradePrevPath(dst), []byte(domainScript("", 1)), 0755); err != nil {
						t.Fatal(err)
					}
					m.PrevSHA = mustSHA(t, upgradePrevPath(dst))
					if err := writeUpgradeMarker(upgradeMarkerPath(dst), m); err != nil {
						t.Fatal(err)
					}
					result := a.watchdogRollback(dst)
					if result != rollbackRetry || execs.Load() != 0 {
						t.Fatalf("late rollback domain accepted: retry=%v execs=%d", result, execs.Load())
					}
					marker := readMarkerState(t, upgradeMarkerPath(dst))
					if marker.State != upgradeStatePending || marker.ResourcePhase != markerPhaseRollbackBlocked || marker.Detail != "prepare failed: abi_late" || mustSHA(t, dst) != before {
						t.Fatalf("late rollback lost current image: %+v", marker)
					}
					cutRequireOpen(t, a)
					return
				}
				// origin: hot-update S8 plan §3.3 / §4.1 / §6.1 / §7.2; S8 FIX-FIXTURES
				// R2; S8 FIX-AGENTWIRE. The grantless envelope neither downloads nor
				// re-execs. The same windows run through a granted Start on the
				// production RuntimePort; its candidate reads every domain but PTY:
				// before Preflight (publication) the late PTY is a precise rejection,
				// after the first Preflight an R0 cancellation, and at the final
				// re-export (install or staged re-exec) a retained Current.
				nc := cutNATS(t)
				if response := cutForward(t, a, nc, req); response.OK || response.Code != "upgrade_deferred" || hits.Load() != 0 {
					t.Fatalf("grantless envelope reached the cut: %+v", response)
				}
				route := openGrantedRoute(t, a, false)
				target := s8Script("final-prepare "+window+" "+a.instanceID, "exec-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1")
				if window == "reexec_prepare" {
					// A staged re-exec of on-disk bytes is an ordinary granted
					// transaction whose target is those bytes, published.
					if err := os.WriteFile(dst, target, 0o755); err != nil {
						t.Fatal(err)
					}
					before = mustSHA(t, dst)
				}
				if window == "during_download" {
					domainRetainPTY(t, a, "live")
				}
				grant, receipt := route.publish(t, a, nc, target)
				var injected atomic.Bool
				if window == "after_probe" {
					route.interpose(a, func() { domainRetainPTY(t, a, "live"); injected.Store(true) })
				}
				resp := route.start(t, a, nc, grant, receipt)
				switch window {
				case "during_download":
					if resp.Code != upgradetx.CodeRejected || resp.Error != agentupgradeport.ReasonImageABIUnsupported {
						t.Fatalf("publication-window extension accepted: code=%s error=%q", resp.Code, resp.Error)
					}
					route.requireUnaccepted(t, grant)
				case "after_probe":
					if !injected.Load() || resp.Code != upgradetx.CodeUnsettled || resp.Error != agentupgradeport.ReasonImageABIUnsupported {
						t.Fatalf("post-preflight extension accepted: injected=%v code=%s error=%q", injected.Load(), resp.Code, resp.Error)
					}
					route.requireCancelled(t, grant)
				default:
					if resp.Code != upgradetx.CodeUnsettled {
						t.Fatalf("late final re-export accepted: code=%s error=%q", resp.Code, resp.Error)
					}
					route.requireRecovery(t, grant, receipt)
				}
				domainRequireUnchanged(t, dst, before)
				if execs.Load() != 0 {
					t.Fatal("domain-incompatible final bundle reached exec")
				}
				if !strings.Contains(window, "probe") && window != "during_download" {
					if err := checkCandidateABI(a, agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1"}}); !errors.Is(err, errImageABIUnsupported) {
						t.Fatal("late retained extension was erased to make prepare readable")
					}
				}
				cutRequireOpen(t, a)
			})
		}
	}
	testharness.AssertNoFDLeak(t, "final domain checks x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "final domain checks x5", beforeG)
}
