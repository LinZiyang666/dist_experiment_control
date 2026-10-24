//go:build linux

package agent

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/infraattach"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	ns "github.com/nats-io/nats-server/v2/server"
)

// Supervise the test process before Start, so the runtime's real PTY child
// and any setsid descendants remain under the shared subreaper/lease owner.
// A final directory census supplements supervision; it cannot sign continuity.
func superviseAgentInfra(t *testing.T) bool {
	t.Helper()
	const key = "_TETHER_AGENT_INFRA_TEST"
	if os.Getenv(key) == t.Name() {
		return false
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1", "-test.timeout=60s")
	for _, env := range os.Environ() {
		if !strings.HasPrefix(env, key+"=") {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env, key+"="+t.Name())
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	join, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	err = join()
	if err != nil {
		t.Fatalf("supervised agent fixture: %v\n%s", err, out.String())
	}
	return true
}

// The same PTY child/master, SS listener, raw flow and transfer FD outlive two
// actual control NC teardowns. Original PTY input/output and flow bytes continue
// after each carrier cancellation; a live-looking pointer is not the oracle.
// Full signed cohort movement and real accepted-ingress proof belong to P30.
// origin: hot-update S9 plan §4.5 / T50 / M25
func TestAgentInfraMovePreservesDataOwners(t *testing.T) {
	if superviseAgentInfra(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAgentInfra(t)
	}
	testharness.AssertNoGoroutineLeak(t, "agent original infra data owners x5", beforeG)
	testharness.AssertNoFDLeak(t, "agent original infra data owners x5", beforeFD, 0)
}

// origin: hot-update S9 plan §9.5 / P23
func TestAgentInfraLifecycle(t *testing.T) {
	if superviseAgentInfra(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAgentInfra(t)
	}
	testharness.AssertNoGoroutineLeak(t, "agent infra owners x5", beforeG)
	testharness.AssertNoFDLeak(t, "agent infra owners x5", beforeFD, 0)
}

// A retired carrier cannot conceal a pending domain from direct image cuts or
// the rollback watchdog's shared preflight. The domain survives ncBox changes.
// origin: hot-update S9 plan §7.3 / T50 / M25
func TestAgentInfraPendingDomainBlocksEveryImageEntry(t *testing.T) {
	a, err := New(Config{NATSURL: "nats://127.0.0.1:1", SID: "lab", NID: "node", Home: t.TempDir(), Logger: testharness.SilentLog(), Infra: &infraattach.Config{Busy: func() bool { return true }}})
	if err != nil {
		t.Fatal(err)
	}
	defer stopProcessRuntime(t.Context(), a)
	if !agentInfraPending(a) {
		t.Fatal("missing current NC hid a pending infrastructure domain")
	}
	checks := busyChecks(a)
	reason, _, busy := checks[0]()
	if !busy || reason != "infra_busy" {
		t.Fatal("image preflight or watchdog admitted a pending infrastructure domain")
	}
	cut, cutErr := freezeForUpgrade(a, time.Now().Add(time.Second))
	if cut != nil {
		_ = cut.abandon(a)
	}
	if cutErr == nil || cutErr.Error() != "infra_busy" || cut != nil {
		t.Fatalf("direct image cut bypassed infra exclusion: %v", cutErr)
	}
}

func exerciseAgentInfra(t *testing.T) {
	t.Helper()
	root := testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))
	s, err := ns.NewServer(&ns.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(time.Second) {
		t.Fatal("agent server not ready")
	}
	defer func() { s.Shutdown(); s.WaitForShutdown() }()
	a, worker, factories := transferPortAgent(t, s.ClientURL())
	defer func() {
		stopProxyOnRunExit(a)
		stopRestoredFlows(a)
		_ = worker.Stop(transferTestDeadline(t))
		stopProcessRuntime(t.Context(), a)
	}()
	transfers, processRuntime := transferHandler(a), a.processRuntime
	a.setRunCtx(t.Context())
	a.cfg.ExposeAdapter = noopProxyAdapter{}
	enableProxyForTest(t, a, 1)
	originalSS := a.proxy.srv
	manager, flowReq, flowPeer := agentInfraFlow(t, a)
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := flowPeer.Close(); err != nil {
			t.Error(err)
		}
	}()
	a.flowState = &flowRestoreState{closeFlows: manager.Close}
	scope := ptyTestScope(a)
	handle := ptyProbe(t, a, scope)
	create := ptyTaskRequest(scope, handle, "create")
	create.Run = &proto.RunReq{Cwd: root, KAIntervalMS: 1000, Argv: []string{"/bin/sh", "-c", "stty raw -echo; printf ready; while IFS= read -r line; do printf 'seen:%s\\n' \"$line\"; done"}}
	created, err := a.processRuntime.Handle(scope, ownProcessRun(scope, create), "_TINBOX.pty")
	if err != nil || created.PID == "" {
		t.Fatalf("original PTY create failed: %v", err)
	}
	request := ptyTaskRequest(scope, handle, "resume")
	waitFor(t, 3*time.Second, func() bool {
		status, err := a.processRuntime.Handle(scope, request, "_TINBOX.pty")
		return err == nil && status.Phase == "running" && status.Stdout != nil && string(status.Stdout.Data) == "ready"
	})
	identity, ok := a.workloads.Lookup(created.PID)
	if !ok {
		t.Fatal("PTY child is not owned by the original workload table")
	}
	before := ptyState(t, a, created.PID)
	for round := 0; round < 2; round++ {
		oldCtx, cancel := context.WithCancel(t.Context())
		a.setRunCtx(oldCtx)
		options := a.buildConnOptions()
		client, options, err := infraattach.Prepare(options, transferTestDeadline(t), infraattach.Config{})
		if err != nil {
			t.Fatal(err)
		}
		nc, err := dialAgentNATS(a, s.ClientURL(), options)
		if err != nil {
			t.Fatal(err)
		}
		a.ncBox.Store(nc)
		if infraattach.For(nc) != client || client.Installed() {
			t.Fatal("agent bypassed raw owner or advertised uninstalled capability")
		}
		fin := &sessionFinalizer{agent: a, nc: nc, cancel: cancel, parent: t.Context()}
		cleaned := false
		fin.addBoundedCleanup(func() { cleaned = true })
		fin.Do(teardownRebuild)
		if oldCtx.Err() == nil || !nc.IsClosed() || !cleaned {
			t.Fatal("transport retirement did not cancel and join the old NC before cleanup")
		}
		if a.processRuntime != processRuntime || a.proxy.srv != originalSS || !servingNow(a) || transferHandler(a) != transfers || worker.calls.Load() != 0 || factories.Load() != 1 {
			t.Fatal("NC rebuild stopped or replaced an original data owner")
		}
		if _, err := worker.file.Stat(); err != nil {
			t.Fatal("NC rebuild closed the retained transfer FD")
		}
		next, ok := a.workloads.Lookup(created.PID)
		if !ok || next.Identity != identity.Identity {
			t.Fatal("NC rebuild replaced original PTY child")
		}
		input := request
		input.Op, input.InputSeq = "input", uint64(round*7)
		input.Input = []byte(fmt.Sprintf("line-%d\n", round))
		if _, err := a.processRuntime.Handle(scope, input, "_TINBOX.pty"); err != nil {
			t.Fatal(err)
		}
		want := "ready"
		for i := 0; i <= round; i++ {
			want += fmt.Sprintf("seen:line-%d\n", i)
		}
		waitFor(t, time.Second, func() bool {
			status, err := a.processRuntime.Handle(scope, request, "_TINBOX.pty")
			return err == nil && status.Stdout != nil && string(status.Stdout.Data) == want
		})
		live := ptyState(t, a, created.PID)
		if live.KADeadlineBootNS != before.KADeadlineBootNS || live.LastKASeq != before.LastKASeq || live.Identity != before.Identity {
			t.Fatal("NC rebuild replaced the PTY master or renewed its original keepalive")
		}
		body := []byte(fmt.Sprintf("flow-%d", round))
		frame := flow.Frame{Version: flow.Version, Kind: flow.KindData, FlowID: flowReq.ID, CarrierGen: flowReq.CarrierGen, Flags: flow.FlagDirection, Offset: uint64(round * len(body)), BodyLen: uint32(len(body)), Body: body}
		if err := flow.Receive(manager, frame); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(body))
		if _, err := io.ReadFull(flowPeer, got); err != nil || !bytes.Equal(got, body) {
			t.Fatalf("original raw flow stopped after NC cancellation: %v", err)
		}
	}
}

func agentInfraFlow(t *testing.T, a *Agent) (*flow.Manager, flow.OpenRequest, *net.TCPConn) {
	t.Helper()
	ln, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S10 lint L3a / errcheck
	defer func() {
		if err := ln.Close(); err != nil {
			t.Error(err)
		}
	}()
	conn, err := net.DialTCP("tcp4", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	peer, err := ln.AcceptTCP()
	if err != nil {
		_ = conn.Close()
		t.Fatal(err)
	}
	_ = peer.SetDeadline(time.Now().Add(5 * time.Second))
	raw, err := rawsocket.TakeTCP(conn)
	if err != nil {
		_ = peer.Close()
		t.Fatal(err)
	}
	manager, err := flow.New(flow.Config{Budget: a.replayBudget})
	if err != nil {
		_ = raw.Close()
		_ = peer.Close()
		t.Fatal(err)
	}
	req := flow.OpenRequest{ID: flow.ID{1}, EndpointID: flow.EndpointID{2}, OpenSeq: 1, TargetPort: 22, OwnerGen: 1, CarrierGen: 1}
	req.Intent = flow.OpenIntent(req.EndpointID, req.ID, req.OpenSeq, req.TargetPort)
	if _, err = flow.Open(manager, req, raw); err != nil {
		_ = manager.Close()
		_ = peer.Close()
		t.Fatal(err)
	}
	waitFor(t, time.Second, func() bool {
		state, _, err := flow.Observe(manager, req.ID)
		return err == nil && state.OpenStatus == 2
	})
	return manager, req, peer
}
