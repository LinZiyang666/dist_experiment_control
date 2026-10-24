package broker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 plan §4.5 / T14
// The broker contract arm uses injected observation time, real SQL and real
// events. The carrier arm also cuts a real TLS/yamux owner and keeps the original
// public/target TCP connection. This is not a same-PID agent exec receipt;
// P14/P15 and the real-binary tests own the agent's sealed readiness latch.
// origin: S10 internal review MAKETEST-BROKER-IMAGE F3
func TestReaperDoesNotRehomeDuringPlannedCarrierRebuild(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	t.Run("grace-before-third-bad-tick", func(t *testing.T) {
		// Three observations at t=0,5,10 span ten seconds, not fifteen.
		// Equality is unsafe: expiration and the third tick can race.
		shortestBadSpan := time.Duration(proxyRehomeDwell-1) * observeTickInterval
		if tunnel.DetachedGrace >= shortestBadSpan {
			t.Fatalf("detached grace %v must be strictly below shortest rehome dwell %v", tunnel.DetachedGrace, shortestBadSpan)
		}
	})
	before, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			exercisePlannedReaperRebuild(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "planned reaper rebuild", before)
	// The process-wide network poller may be initialized by the first round.
	testharness.AssertNoFDLeak(t, "planned reaper rebuild", beforeFD, 2)
}

func exercisePlannedReaperRebuild(t *testing.T) {
	t.Helper()
	t.Run("broker-observations", func(t *testing.T) {
		exercisePlannedReaperObservations(t)
	})
	t.Run("real-carrier", func(t *testing.T) {
		if runtime.GOOS != "linux" {
			t.Skip("flow image carrier is Linux-only; Darwin legacy lifecycle is owned by P23")
		}
		exercisePlannedReaperCarrier(t)
	})
}

func exercisePlannedReaperObservations(t *testing.T) {
	t.Helper()
	for _, history := range []int{0, proxyRehomeDwell - 1} {
		for _, phase := range []time.Duration{time.Nanosecond, observeTickInterval - time.Nanosecond} {
			t.Run(fmt.Sprintf("history-%d-phase-%s", history, phase), func(t *testing.T) {
				// Independent Raft owners overlap production election waits; the
				// enclosing round joins every cleanup before leak measurement.
				// origin: hot-update S6 plan §9.3 / T56 / TESTFIX
				t.Parallel()
				f := newFlowReaperFixture(t, 23000, 23010)
				before := flowReaperSnapshot(t, f.b)
				events := len(f.tap.sysEvents(t))
				// Historical bad observations are leader-local. The recovered
				// SQL readiness observation is already true, but no healthy tick
				// has yet had a chance to erase the old dwell.
				if history != 0 {
					f.b.proxyDwell.Store(f.alloc.Port, history)
				}
				setFlowReaperReady(t, f.b, true)
				cutAt := f.clock.Now()
				f.clock.advance(phase)
				for tick := 0; tick <= proxyRehomeDwell; tick++ {
					f.b.driveProxyReconcile()
					assertFlowReaperUnchanged(t, f, before, events)
					if _, exists := f.b.proxyDwell.Load(f.alloc.Port); exists {
						t.Fatal("planned rebuild retained old bad dwell after a healthy ready observation")
					}
					f.clock.advance(observeTickInterval)
				}
				if elapsed := f.clock.Now().Sub(cutAt); elapsed < time.Duration(proxyRehomeDwell-1)*observeTickInterval {
					t.Fatal("planned rebuild observation window did not cover shortest bad dwell")
				}
			})
		}
	}
}

func exercisePlannedReaperCarrier(t *testing.T) {
	t.Helper()
	publicPort := flowReaperFreePort(t)
	f := newFlowReaperFixture(t, publicPort, publicPort+1)
	// Configure both owners before transport admission; selected v1 requires
	// complete ownership and the production bounded authority reader.
	client := tunnel.NewClient("", "lab", "node", nil, silentLogger())
	clientCfg := tunnel.FlowConfig{
		Budget: &workload.ReplayBudget{}, InstanceID: "aaaaaaaaaaaaaaaaaaaaaaaaaa", OwnerGen: 1, BootID: "test-boot",
	}
	if err := client.SetFlowConfig(clientCfg); err != nil {
		t.Fatalf("planned carrier requires the P1 flow owner: %v", err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	var authorizations atomic.Int64
	// origin: hot-update S5 plan §4.3 / §4.5 / T14
	serverCfg := tunnel.FlowConfig{
		Budget: &workload.ReplayBudget{}, InstanceID: "reaper-broker", OwnerGen: 1, BootID: "test-broker-boot",
		TokenLookup: func(req tunnel.AuthRequest) error {
			_, err := tunnelAuthorityLookup(f.b, req)
			return err
		},
		Authorize: func(req tunnel.AuthRequest) (tunnel.Authority, error) {
			grant, err := tunnelAuthorityLookup(f.b, req)
			if err == nil {
				authorizations.Add(1)
			}
			return grant, err
		},
	}
	var server *tunnel.Server
	var controlAddr string
	for attempt := 0; attempt < 10; attempt++ {
		controlAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(flowReaperFreePort(t)))
		server = tunnel.NewServer(controlAddr, "127.0.0.1", f.b.tunnelTokenLookup, silentLogger())
		if err := server.SetFlowConfig(serverCfg); err != nil {
			t.Fatalf("planned carrier requires the P1 server owner: %v", err)
		}
		if err := server.Start(ctx); err == nil {
			break
		} else if attempt == 9 {
			t.Fatalf("start control listener after port retries: %v", err)
		}
	}
	t.Cleanup(server.Close)
	target, accepts := flowReaperEchoTarget(t)
	_, targetPortString, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatal(err)
	}
	targetPort, err := strconv.Atoi(targetPortString)
	if err != nil {
		t.Fatal(err)
	}
	// A new client is needed now that the server's ephemeral address and the
	// target are known; both configurations still precede Start/admission.
	client = tunnel.NewClient(controlAddr, "lab", "node", func(int) (int, error) { return targetPort, nil }, silentLogger())
	if err := client.SetFlowConfig(clientCfg); err != nil {
		t.Fatal(err)
	}
	client.Start(ctx)
	// Port removal is not a root join. Wait for retained raw/replay cleanup
	// before the test's FD baseline is read, including failed freeze paths.
	// origin: hot-update S6 plan §9.3 / T56 / TESTFIX
	t.Cleanup(func() {
		if err := tunnel.StopClient(client, tunnel.ResumeAttempt); err != nil {
			t.Error(err)
		}
	})
	if err := client.Open(publicPort, targetPort, f.alloc.Token); err != nil {
		t.Fatalf("open original carrier: %v", err)
	}
	public, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), tunnel.ResumeAttempt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = public.Close() })
	flowReaperExchange(t, public, []byte("original-business-before-cut"))
	if accepts.Load() != 1 || authorizations.Load() == 0 {
		t.Fatal("original business socket or fresh RESUME authority premise is empty")
	}
	before := flowReaperSnapshot(t, f.b)
	events := len(f.tap.sysEvents(t))
	f.b.proxyDwell.Store(publicPort, proxyRehomeDwell-1)
	cutID := [16]byte{1}
	if err := client.PrepareHandoff(2, cutID, time.Now().Add(tunnel.ResumeAttempt)); err != nil {
		t.Fatalf("prepare original owner while attached: %v", err)
	}
	cutAt := time.Now()
	frozen, err := client.FreezeFlows(cutAt.Add(tunnel.DetachedGrace))
	if err != nil {
		t.Fatalf("freeze original carrier: %v", err)
	}
	// The broker sees the last real ready observation during a planned cut.
	// No test shim republishes readiness or pauses the reaper while detached.
	f.clock.advance(time.Nanosecond)
	f.b.driveProxyReconcile()
	assertFlowReaperUnchanged(t, f, before, events)
	grantsBeforeResume := authorizations.Load()
	if err := frozen.Thaw(); err != nil {
		t.Fatalf("thaw original owner after local cut: %v", err)
	}
	if err := client.ResumeCarriers(time.Now().Add(tunnel.ResumeAttempt)); err != nil {
		t.Fatalf("rebuild original owner's carrier: %v", err)
	}
	if authorizations.Load() <= grantsBeforeResume {
		t.Fatal("carrier rebuild did not perform fresh RESUME authorization")
	}
	if time.Since(cutAt) >= tunnel.DetachedGrace {
		t.Fatal("planned rebuild exceeded first-detach grace")
	}
	flowReaperExchange(t, public, []byte("original-business-after-cut"))
	if accepts.Load() != 1 {
		t.Fatal("carrier rebuild redialed the original target TCP socket")
	}
	for tick := 0; tick < proxyRehomeDwell; tick++ {
		f.clock.advance(observeTickInterval)
		f.b.driveProxyReconcile()
		assertFlowReaperUnchanged(t, f, before, events)
	}
}

func flowReaperFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return port
}

func flowReaperExchange(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()
	if err := conn.SetDeadline(time.Now().Add(tunnel.ResumeAttempt)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(conn, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	received := make([]byte, len(payload))
	if _, err := io.ReadFull(conn, received); err != nil {
		t.Fatalf("original business socket lost during carrier cut: %v", err)
	}
	if !bytes.Equal(payload, received) {
		t.Fatal("original business socket changed byte content or order")
	}
}

func flowReaperEchoTarget(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepts := &atomic.Int64{}
	var mu sync.Mutex
	var connections []net.Conn
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			mu.Lock()
			connections = append(connections, conn)
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				// TCP's optional copy methods retain splice pipes in a GC-owned
				// pool. The echo witness must release all of its FDs at join so
				// the leak gate measures the carrier owners independently of GC.
				// origin: S10 internal review MAKETEST-BROKER-IMAGE F3
				_, _ = io.Copy(struct{ io.Writer }{conn}, struct{ io.Reader }{conn})
				_ = conn.Close()
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-done // No worker can be added after the accept loop exits.
		mu.Lock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return listener.Addr().String(), accepts
}
