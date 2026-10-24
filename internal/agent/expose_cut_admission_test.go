//go:build linux

// Real TLS/yamux sessions exercise replay and pushed home admission. Channel
// barriers control authorization; only the retry/successor test scripts the
// adapter. These tests do not replace a running executable.
package agent

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
)

func exposeAdmissionFixture(t *testing.T, a *Agent, authorize func() error) (*TunnelExposeAdapter, PortToken) {
	t.Helper()
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
		if authorize != nil {
			return authorize()
		}
		return nil
	}, a.cfg.Logger)
	ctx, cancel := context.WithCancel(t.Context())
	if err := server.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	t.Cleanup(func() { cancel(); server.Close() })
	adapter := NewTunnelExposeAdapter(addr, a.cfg.SID, a.cfg.NID, a.cfg.Logger)
	adapter.Start(ctx)
	a.cfg.ExposeAdapter = adapter
	tok := PortToken{Name: "gate", Port: port, LocalPort: 1, Token: "gate-token"}
	t.Cleanup(func() { _ = adapter.RemoveProxy(tok.Name, tok.Port) })
	if err := a.stateStore.AddPort(tok); err != nil {
		t.Fatal(err)
	}
	return adapter, tok
}

func waitExposeAdmission(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(cutTestWait):
		t.Fatalf("timed out: %s", what)
	}
}

func waitHomeAdmissionReleased(t *testing.T, a *Agent, port int) {
	t.Helper()
	cutWait(t, "home worker and admission release", func() bool {
		a.rehomeMu.Lock()
		running := a.rehomeRunning[port]
		a.rehomeMu.Unlock()
		return !running && a.gate.count(gateExpose) == 0
	})
}

// origin: hot-update S3 plan §2.2 / §4.0. Reopening admission is only useful
// after the fixture's runtime and process table have actually thawed.
func abandonExposeCut(t *testing.T, a *Agent, cut *imageCut) {
	t.Helper()
	if err := cut.abandon(a); err != nil {
		t.Errorf("abandon expose cut: %v", err)
	}
}

// origin: hot-update S5 plan §7.3 / T37: unfinished legacy admission retains ownership.
func requireFlowDeferred(t *testing.T, a *Agent) {
	t.Helper()
	cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
	if cut != nil {
		defer abandonExposeCut(t, a, cut)
	}
	var deferred *agentimage.DeferredError
	if !errors.As(err, &deferred) || deferred.Reason != agentimage.ReasonLegacySessionBusy {
		t.Fatalf("cut did not report legacy_session_busy: err=%v, runtime frozen=%v", err, a.processRuntime.Frozen())
	}
	if a.processRuntime.Frozen() {
		t.Fatal("deferred cut froze the runtime")
	}
}

// origin: hot-update S3 plan §4.0 / §4.1; concurrency review CC-4 reproducer.
func TestImageCutDefersWhileExposeReplayHandshakeIsInFlight(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, denied := range []bool{false, true} {
			t.Run(strconv.Itoa(round)+"/denied="+strconv.FormatBool(denied), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				a := cutTestAgent(t)
				entered, release := make(chan struct{}), make(chan struct{})
				var once sync.Once
				adapter, tok := exposeAdmissionFixture(t, a, func() error {
					close(entered)
					<-release
					if denied {
						return errors.New("token_unknown_or_revoked")
					}
					return nil
				})
				done := make(chan struct{})
				go func() { a.replayPortsFromState(t.Context()); close(done) }()
				t.Cleanup(func() {
					once.Do(func() { close(release) })
					waitExposeAdmission(t, done, "replay cleanup")
				})
				waitExposeAdmission(t, entered, "real replay authorization")
				if adapter.SessionCount() != 0 {
					t.Fatal("fixture installed a session before authorization completed")
				}
				a.gate.close()
				requireFlowDeferred(t, a)
				once.Do(func() { close(release) })
				waitExposeAdmission(t, done, "replay completion")
				want := 1
				if denied {
					want = 0
				}
				if n := adapter.SessionCount(); n != want {
					t.Fatalf("replay sessions=%d, want %d", n, want)
				}
				if n := a.gate.count(gateExpose); n != 0 {
					t.Fatalf("completed replay retained %d admissions", n)
				}
				if err := adapter.RemoveProxy(tok.Name, tok.Port); err != nil {
					t.Fatal(err)
				}
				cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
				if err != nil {
					t.Fatalf("finished replay still blocks cut: %v", err)
				}
				abandonExposeCut(t, a, cut)
			})
		}
	}
	testharness.AssertNoFDLeak(t, "replay handshake admission", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "replay handshake admission", beforeG)
}

// origin: hot-update S3 plan §4.0 / §4.1; concurrency review CC-4.
func TestImageCutBlocksExposeReplay(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			adapter, _ := exposeAdmissionFixture(t, a, nil)
			a.gate.close()
			cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if cut != nil {
					abandonExposeCut(t, a, cut)
				}
			}()
			a.replayPortsFromState(t.Context())
			if n := adapter.SessionCount(); n != 0 {
				t.Fatalf("replay installed %d sessions while the image was frozen", n)
			}
			abandonExposeCut(t, a, cut)
			cut = nil
			a.replayPortsFromState(t.Context())
			if n := adapter.SessionCount(); n != 1 {
				t.Fatalf("replay after abandon installed %d sessions, want 1", n)
			}
		})
	}
	testharness.AssertNoFDLeak(t, "closed replay admission", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "closed replay admission", beforeG)
}

// origin: hot-update S3 plan §4.0 / §4.1; concurrency review CC-4 reproducer.
func TestImageCutBlocksHomePushOpeningNewExpose(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		for _, deferred := range []bool{false, true} {
			t.Run(strconv.Itoa(round)+"/deferred="+strconv.FormatBool(deferred), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				a := cutTestAgent(t)
				adapter, tok := exposeAdmissionFixture(t, a, nil)
				if deferred {
					tok.HomeBrokerAddr = "127.0.0.1:1"
					if err := a.stateStore.AddPort(tok); err != nil {
						t.Fatal(err)
					}
					a.replayPortsFromState(t.Context())
					a.rehomeMu.Lock()
					marked := a.deferredReplay[tok.Port]
					a.rehomeMu.Unlock()
					if !marked {
						t.Fatal("missing pins did not defer replay")
					}
				}
				a.setRunCtx(t.Context())
				a.gate.close()
				cut, err := freezeForUpgrade(a, time.Now().Add(upgradebudget.SafePointWait))
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if cut != nil {
						abandonExposeCut(t, a, cut)
					}
				}()
				data, err := json.Marshal(proto.HomeAssignment{Directives: []proto.HomeDirective{{Name: tok.Name, PublicPort: tok.Port, Epoch: 1}}})
				if err != nil {
					t.Fatal(err)
				}
				a.handleHomeForwarded(nil, &nats.Msg{Data: data})
				waitHomeAdmissionReleased(t, a, tok.Port)
				if n := adapter.SessionCount(); n != 0 {
					t.Fatalf("home push installed %d sessions while the image was frozen", n)
				}
				sf, err := a.stateStore.load()
				if err != nil || len(sf.PortTokens) != 1 || sf.PortTokens[0].Epoch != 0 {
					t.Fatalf("skipped home changed persisted allocation: state=%v err=%v", sf, err)
				}
				abandonExposeCut(t, a, cut)
				cut = nil
				a.handleHomeForwarded(nil, &nats.Msg{Data: data})
				waitHomeAdmissionReleased(t, a, tok.Port)
				if n := adapter.SessionCount(); n != 1 {
					t.Fatalf("broker replay after abandon installed %d sessions, want 1", n)
				}
			})
		}
	}
	testharness.AssertNoFDLeak(t, "home push admission", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "home push admission", beforeG)
}

// origin: hot-update S3 plan §4.0 / §4.1; concurrency review CC-4.
func TestHomeAdmissionCoversRetriesAndSuccessorWorkers(t *testing.T) {
	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			a := cutTestAgent(t)
			const port = 14000
			retried, successor := make(chan struct{}), make(chan struct{})
			releaseRetry, releaseSuccessor := make(chan struct{}), make(chan struct{})
			var retryOnce, successorOnce sync.Once
			fake := newFakeHomeAdapter(func(_ int, epoch int64, call int) error {
				if call == 1 {
					return transientDeny()
				}
				if epoch == 1 {
					close(retried)
					<-releaseRetry
				} else {
					close(successor)
					<-releaseSuccessor
				}
				return nil
			})
			a.cfg.ExposeAdapter = fake
			var hookOnce sync.Once
			hook := func(got *Agent, gotPort int) {
				if got != a || gotPort != port {
					return
				}
				hookOnce.Do(func() {
					a.applyHomeDirectives(t.Context(), homeAssign(port, 2))
					a.gate.close()
				})
			}
			afterRehomeWantSettledHook.Store(&hook)
			t.Cleanup(func() {
				retryOnce.Do(func() { close(releaseRetry) })
				successorOnce.Do(func() { close(releaseSuccessor) })
				waitHomeAdmissionReleased(t, a, port)
				afterRehomeWantSettledHook.Store(nil)
			})
			a.applyHomeDirectives(t.Context(), homeAssign(port, 1))
			waitExposeAdmission(t, retried, "home retry")
			if n := a.gate.count(gateExpose); n != 1 {
				t.Fatalf("retry owns %d admissions, want 1", n)
			}
			a.gate.close()
			requireFlowDeferred(t, a)
			a.gate.open()
			retryOnce.Do(func() { close(releaseRetry) })
			waitExposeAdmission(t, successor, "successor home worker")
			if n := a.gate.count(gateExpose); n != 1 {
				t.Fatalf("successor owns %d admissions, want 1", n)
			}
			requireFlowDeferred(t, a)
			successorOnce.Do(func() { close(releaseSuccessor) })
			waitHomeAdmissionReleased(t, a, port)
		})
	}
	testharness.AssertNoFDLeak(t, "home retry and successor admission", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "home retry and successor admission", beforeG)
}

type exposeCensusAdapter struct {
	ExposeAdapter
	count func() int
}

func (a *exposeCensusAdapter) SessionCount() int { return a.count() }

// origin: hot-update S3 plan §4.1; concurrency review CC-4. Force the dial to
// install just after the session census takes its snapshot. Reading admission
// afterwards would see zero too, even though a real session now exists.
func TestFlowBusyCountsAdmissionBeforeInstalledSessions(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeFD, beforeG := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := cutTestAgent(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			adapter, _ := exposeAdmissionFixture(t, a, func() error {
				close(entered)
				<-release
				return nil
			})
			done := make(chan struct{})
			a.cfg.ExposeAdapter = &exposeCensusAdapter{ExposeAdapter: adapter, count: func() int {
				n := adapter.SessionCount()
				once.Do(func() { close(release) })
				waitExposeAdmission(t, done, "install during census")
				return n
			}}
			go func() { a.replayPortsFromState(t.Context()); close(done) }()
			t.Cleanup(func() {
				once.Do(func() { close(release) })
				waitExposeAdmission(t, done, "replay cleanup")
			})
			waitExposeAdmission(t, entered, "real replay authorization")
			a.gate.close()
			requireFlowDeferred(t, a)
			if n := adapter.SessionCount(); n != 1 {
				t.Fatalf("census barrier did not install a real session: %d", n)
			}
			if n := a.gate.count(gateExpose); n != 0 {
				t.Fatalf("census barrier left %d admissions", n)
			}
		})
	}
	testharness.AssertNoFDLeak(t, "admission to session census handover", beforeFD, 0)
	testharness.AssertNoGoroutineLeak(t, "admission to session census handover", beforeG)
}
