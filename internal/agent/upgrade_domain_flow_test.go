//go:build linux

package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S5 plan §5.2 / T35. Reader checks cover retained
// metadata with zero active sockets, the actual sibling prev, and final SHA.
func TestFlowSSCandidateAndActualPrevDomains(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("readers/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFlowSSReaderDomains(t)
		})
		t.Run(fmt.Sprintf("real_flow/%d", round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseRetainedFlowDomains(t)
		})
	}
	testharness.AssertNoFDLeak(t, "flow/SS reader policy x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "flow/SS reader policy x5", beforeG)

	t.Run("retained_bundle", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		// These are public image summaries, not replacements for runtime
		// implementations. Decode must preserve nonnil domain responsibility
		// even when the sealed state's only responsibility is retirement/salts.
		for _, tc := range []struct{ name, raw, domain string }{
			{"idle_endpoint", `{"flow":{"version":1,"endpoints":[{"state_ref":"flow-state.fixture"}]}}`, "flow-v1"},
			{"retirement", `{"flow":{"version":1,"state_ref":"flow-state.retirement","flows":{"version":1,"state_ref":"flow-state.retirement"}}}`, "flow-v1"},
			{"salt_history", `{"ss":{"version":1,"state_ref":"ss-state.salts","connection_count":0}}`, "ss-v1"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				var b agentimage.Bundle
				if err := json.Unmarshal([]byte(tc.raw), &b); err != nil {
					t.Fatal(err)
				}
				reader := agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1", "pty-v1"}}
				if err := agentimage.CheckBundleCapabilities(b, reader); !errors.Is(err, errImageABIUnsupported) || !strings.Contains(err.Error(), tc.domain) {
					t.Fatalf("retained %s domain ignored (pending P17): %v", tc.name, err)
				}
				if err := agentimage.CheckBundleCapabilities(b, agentimage.CurrentCapabilities("fixture")); err != nil {
					t.Fatal(err)
				}
				if err := agentimage.CheckBundleCapabilities(b, agentimage.Capabilities{}); !errors.Is(err, errImageABIUnsupported) {
					t.Fatal("legacy reader accepted retained new domain")
				}
			})
		}
	})
}

func exerciseFlowSSReaderDomains(t *testing.T) {
	t.Helper()
	for _, state := range []string{"listener", "keys", "stopped_retained", "ordering_only"} {
		t.Run(state, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final subtest process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			srv := ssproxy.New(a.cfg.Logger)
			a.proxy.srv = srv
			keys := []ssproxy.Key(nil)
			if state == "keys" {
				keys = []ssproxy.Key{{KeyID: "fixture", Secret: "reader-policy-fixture"}}
			}
			if _, err := srv.Start(0, keys); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(srv.Stop)
			if state == "stopped_retained" || state == "ordering_only" {
				srv.Stop()
			}
			if state == "ordering_only" {
				// The serialized apply pair is image responsibility after the
				// server has been fully removed, independent of active actors.
				a.proxy.mu.Lock()
				a.proxy.srv, a.proxy.gen, a.proxy.epoch = nil, 1, 1
				a.proxy.mu.Unlock()
			}
			for _, domains := range []string{"exec-v1", "exec-v1 pty-v1 flow-v1"} {
				caps := agentimage.Capabilities{ABI: 1, Domains: strings.Fields(domains)}
				if err := checkCandidateABI(a, caps); !errors.Is(err, errImageABIUnsupported) || !strings.Contains(err.Error(), "ss-v1") {
					t.Fatalf("retained SS accepted ABI alone: %v", err)
				}
			}
			if err := checkCandidateABI(a, agentimage.CurrentCapabilities("fixture")); err != nil {
				t.Fatal(err)
			}
			if err := checkCandidateABI(a, agentimage.Capabilities{}); !errors.Is(err, errImageABIUnsupported) {
				t.Fatal("bare exec accepted retained SS")
			}

			candidate := []byte(domainScript("exec-v1 pty-v1 flow-v1", 1))
			dst := a.cfg.UpgradeExecutablePath
			before := mustSHA(t, dst)
			_, err := a.installNewBinary(testTarball(t, candidate), dst, installPolicy{runningSHA: before})
			if !errors.Is(err, errImageABIUnsupported) {
				t.Fatalf("first reader gate admitted SS-incompatible candidate: %v", err)
			}
			domainRequireUnchanged(t, dst, before)
		})
	}
	if err := checkUpgradeCutReaders("", agentimage.Bundle{}, nil); !errors.Is(err, errImageABIUnsupported) {
		t.Fatal("empty reader tuple must refuse before file or exec side effects")
	}
	for _, arm := range []string{"running", "sibling_candidate", "third_sha"} {
		t.Run(arm, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final subtest process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			dst := a.cfg.UpgradeExecutablePath
			candidate := []byte(domainScript("exec-v1 pty-v1 flow-v1 ss-v1", 1))
			running := mustSHA(t, dst)
			if arm != "running" {
				a.cfg.UpgradeRunningImagePath = writeScript(t, "original image")
				content := candidate
				if arm == "third_sha" {
					content = []byte("unrelated image")
				}
				if err := os.WriteFile(dst, content, 0755); err != nil {
					t.Fatal(err)
				}
			}
			before := mustSHA(t, dst)
			var readers []agentimage.Capabilities
			_, err := a.installNewBinary(testTarball(t, candidate), dst, installPolicy{runningSHA: running, readers: &readers})
			if arm == "third_sha" {
				if !errors.Is(err, errRunningImageDiffers) {
					t.Fatalf("third SHA admitted: %v", err)
				}
				domainRequireUnchanged(t, dst, before)
				return
			}
			if err != nil {
				t.Fatalf("actual rollback-capable prev rejected: %v", err)
			}
			if len(readers) != 2 || readers[1].SHA256 != before {
				t.Fatal("actual prev was replaced by running capabilities")
			}
			if err := checkUpgradeCutReaders(dst, agentimage.Bundle{}, readers); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(upgradePrevPath(dst), []byte("changed previous image"), 0755); err != nil {
				t.Fatal(err)
			}
			if err := checkUpgradeCutReaders(dst, agentimage.Bundle{}, readers); err == nil {
				t.Fatal("final gate accepted changed actual prev SHA")
			}
		})
	}
}

// origin: hot-update S5 plan §5.2 / T36. MarkReady and even a visible
// committed rename with a failed directory sync cannot authorize new state.
func TestPendingOldPrevRejectsNewFlowSSAdmission(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("policy/%d", round), func(t *testing.T) { exercisePendingFlowSSAdmission(t) })
		t.Run(fmt.Sprintf("expose_entrypoint/%d", round), func(t *testing.T) { exercisePendingExposeEntry(t) })
		t.Run(fmt.Sprintf("proxy_entrypoint/%d", round), func(t *testing.T) { exercisePendingProxyEntry(t) })
	}
	testharness.AssertNoFDLeak(t, "pending flow/SS admission x5", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "pending flow/SS admission x5", beforeG)

}

func exercisePendingFlowSSAdmission(t *testing.T) {
	t.Helper()
	for _, arm := range []string{"pending", "failed_commit_file", "failed_commit_dir", "sibling", "flow_prev", "ss_prev", "all_prev", "bad_prev_sha"} {
		t.Run(arm, func(t *testing.T) {
			a := cutTestAgent(t)
			cutImportActive(t, a)
			m := cutPending(t, a)
			domains := []string{"exec-v1", "pty-v1"}
			if arm == "flow_prev" || arm == "all_prev" {
				domains = append(domains, "flow-v1")
			}
			if arm == "ss_prev" || arm == "all_prev" {
				domains = append(domains, "ss-v1")
			}
			dst := a.cfg.UpgradeExecutablePath
			if err := os.WriteFile(upgradePrevPath(dst), []byte(domainScript(strings.Join(domains, " "), 1)), 0755); err != nil {
				t.Fatal(err)
			}
			m.PrevSHA = mustSHA(t, upgradePrevPath(dst))
			m.PrevImageSHA256, m.PrevImageABI, m.PrevImageDomains = m.PrevSHA, 1, domains
			if arm == "bad_prev_sha" {
				m.PrevImageSHA256 = "unbound"
			}
			if err := writeUpgradeMarker(upgradeMarkerPath(dst), m); err != nil {
				t.Fatal(err)
			}
			a.upgradeBootProofID = m.UpgradeID
			if arm == "sibling" {
				a.instanceID = "sibling"
				a.upgradeBootProofID = ""
			}
			err := initPTYUpgradeAdmission(a)
			if (err != nil) != (arm == "bad_prev_sha") {
				t.Fatalf("pending initialization: %v", err)
			}
			flowBlocked := arm != "sibling" && arm != "flow_prev" && arm != "all_prev"
			ssBlocked := arm != "sibling" && arm != "ss_prev" && arm != "all_prev"
			flowSSRequireAdmission(t, a, flowBlocked, ssBlocked)
			a.image.MarkReady()
			a.gate.close()
			a.gate.open()
			releaseFlowSSUpgradeAdmission(a, "foreign-transaction")
			flowSSRequireAdmission(t, a, flowBlocked, ssBlocked)
			original := upgradeSyncObserver
			defer func() { upgradeSyncObserver = original }()
			if strings.HasPrefix(arm, "failed_commit") {
				upgradeSyncObserver = func(kind, path string) error {
					if arm == "failed_commit_file" && kind == "file" && strings.HasPrefix(filepath.Base(path), ".tether-upgrade-marker-") || arm == "failed_commit_dir" && kind == "dir" && path == filepath.Dir(dst) {
						return syscall.EIO
					}
					return nil
				}
			}
			a.commitUpgradeAfterRegister(upgradeStateCommitted)
			if strings.HasPrefix(arm, "failed_commit") {
				flowSSRequireAdmission(t, a, true, true)
				upgradeSyncObserver = original
				a.commitUpgradeAfterRegister(upgradeStateCommitted)
			}
			if arm != "sibling" {
				flowSSRequireAdmission(t, a, false, false)
			}
		})
	}
}

func flowSSRequireAdmission(t *testing.T, a *Agent, flowBlocked, ssBlocked bool) {
	t.Helper()
	for _, tc := range []struct {
		kind    gateKind
		guard   func(*Agent) error
		blocked bool
	}{
		{gateExpose, checkFlowUpgradeAdmission, flowBlocked}, {gateSS, checkSSUpgradeAdmission, ssBlocked},
	} {
		if errors.Is(tc.guard(a), procrequest.ErrBusy) != tc.blocked {
			t.Fatalf("domain %d guard blocked=%v want=%v", tc.kind, tc.guard(a), tc.blocked)
		}
		release, admitted := a.gate.admit(tc.kind)
		if admitted {
			release()
		}
		if admitted == tc.blocked {
			t.Fatalf("domain %d admission=%v want blocked=%v", tc.kind, admitted, tc.blocked)
		}
	}
}

// Uses the real configured flow owner. Idle listeners and retired journals
// must still veto an old reader after their last business connection closes.
func exerciseRetainedFlowDomains(t *testing.T) {
	t.Helper()
	a := cutTestAgent(t)
	adapter, port, target := flowDomainAdapter(t, a)
	for _, phase := range []string{"idle", "live", "retired"} {
		var peer, client net.Conn
		if phase != "idle" {
			client = flowFailureDial(t, port)
			var err error
			peer, err = target.Accept()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = peer.Close() })
			flowFailureExchange(t, client, peer, []byte("domain-responsibility"))
			if phase == "retired" {
				_ = peer.Close()
				_ = client.Close()
			}
		}
		candidate := agentimage.Capabilities{ABI: 1, Domains: []string{"exec-v1", "pty-v1", "ss-v1"}}
		if err := checkCandidateABI(a, candidate); !errors.Is(err, errImageABIUnsupported) || !strings.Contains(err.Error(), "flow-v1") {
			t.Fatalf("%s flow responsibility ignored: %v", phase, err)
		}
		if err := checkCandidateABI(a, agentimage.CurrentCapabilities("fixture")); err != nil {
			t.Fatal(err)
		}
		if peer != nil {
			_ = peer.Close()
			_ = client.Close()
		}
	}
	if c := adapter.client.FlowCensus(); c.Unsupported || c.Owned+c.Retained == 0 {
		t.Fatal("retained typed flow owner absent")
	}
}

func flowDomainAdapter(t *testing.T, a *Agent, lookup ...func(tunnel.AuthRequest) error) (*TunnelExposeAdapter, int, *net.TCPListener) {
	t.Helper()
	target := flowFailureListener(t)
	control, public := flowFailureListener(t), flowFailureListener(t)
	addr, port := control.Addr().String(), public.Addr().(*net.TCPAddr).Port
	_ = control.Close()
	_ = public.Close()
	budget := &workload.ReplayBudget{}
	server := tunnel.NewServer(addr, "127.0.0.1", func(string, string, int, string, int64) error { return nil }, a.cfg.Logger)
	tokenLookup := func(tunnel.AuthRequest) error { return nil }
	if len(lookup) != 0 {
		tokenLookup = lookup[0]
	}
	if err := server.SetFlowConfig(tunnel.FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "fixture-broker", BootID: "fixture-boot", OwnerGen: 1, TokenLookup: tokenLookup, Authorize: func(req tunnel.AuthRequest) (tunnel.Authority, error) {
		return tunnel.Authority{Enabled: true, Allocation: req.Allocation, AppliedRevision: 1}, nil
	}}); err != nil {
		t.Fatalf("real flow broker (pending P1): %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(func() { cancel(); server.Close() })
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	adapter := NewTunnelExposeAdapter(addr, a.cfg.SID, a.cfg.NID, a.cfg.Logger)
	if err := adapter.client.SetFlowConfig(tunnel.FlowConfig{Budget: budget, InstanceID: a.instanceID, OwnerGen: 1, BootID: "fixture"}); err != nil {
		t.Fatalf("real flow client (pending P1): %v", err)
	}
	adapter.Start(ctx)
	a.cfg.ExposeAdapter = adapter
	a.imageGeneration = 1
	t.Cleanup(func() { adapter.client.Close(port) })
	if err := adapter.AddProxy(t.Context(), PortToken{Name: "fixture", Port: port, LocalPort: target.Addr().(*net.TCPAddr).Port, Token: "fixture"}); err != nil {
		t.Fatal(err)
	}
	return adapter, port, target
}

func exercisePendingExposeEntry(t *testing.T) {
	t.Helper()
	a := domainNewPendingAgent(t)
	nc := cutNATS(t)
	a.setRunCtx(t.Context())
	inbox := nats.NewInbox()
	sub, err := nc.Subscribe(inbox, func(m *nats.Msg) { a.handleExposeForwarded(nc, m) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	body, err := json.Marshal(proto.ExposeForwardedReq{Name: "blocked", Port: 12345, LocalPort: 23456, Token: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := nc.Request(inbox, body, cutTestWait)
	if err != nil {
		t.Fatal(err)
	}
	var reply proto.ExposeForwardedResp
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		t.Fatal(err)
	}
	if reply.OK || reply.Code != "try_again" || !strings.Contains(reply.Error, "agent_upgrading") {
		t.Fatalf("pending expose refused too late: code=%s ok=%v", reply.Code, reply.OK)
	}
	state, err := a.stateStore.load()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.PortTokens) != 0 {
		t.Fatal("pending expose persisted an allocation before refusal")
	}
}

func exercisePendingProxyEntry(t *testing.T) {
	t.Helper()
	a := domainNewPendingAgent(t)
	a.setRunCtx(t.Context())
	// Isolate the SS restriction: a flow-capable prev must not let the
	// proxy's existing gateExpose reservation authorize new SS state.
	dst := a.cfg.UpgradeExecutablePath
	m := readMarkerState(t, upgradeMarkerPath(dst))
	if err := os.WriteFile(upgradePrevPath(dst), []byte(domainScript("exec-v1 pty-v1 flow-v1", 1)), 0755); err != nil {
		t.Fatal(err)
	}
	m.PrevSHA = mustSHA(t, upgradePrevPath(dst))
	if err := writeUpgradeMarker(upgradeMarkerPath(dst), m); err != nil {
		t.Fatal(err)
	}
	if err := initPTYUpgradeAdmission(a); err != nil {
		t.Fatal(err)
	}
	flowSSRequireAdmission(t, a, false, true)
	t.Cleanup(func() {
		if a.proxy.srv != nil {
			a.proxy.srv.Stop()
		}
	})
	// Production apply must check before creating a listener or applying
	// keys; this assertion stays red until P15 wires the guard.
	a.cfg.ExposeAdapter = noopProxyAdapter{}
	a.applyProxyDirective(nil, &proto.ProxyDirective{Enabled: true, Generation: 1, Epoch: 1, PublicPort: 12345, Token: "fixture", Keys: []proto.ProxyKey{{SubID: "fixture", Secret: "fixture"}}})
	if a.proxy.srv != nil || a.proxy.gen != 0 || a.proxy.epoch != 0 {
		t.Fatal("pending old prev admitted listener/keyset side effects (pending P15)")
	}
}
