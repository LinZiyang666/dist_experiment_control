//go:build linux

package agent

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/sha1"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/nats-io/nats.go"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
)

// origin: hot-update S5 plan §7.1 / T38
// The scheduler's operations include the endpoint's complete bounded attempt.
// A barrier, rather than scheduling luck, proves the last active endpoint can
// run while the first eight authorizations are stalled.
func TestFlowResumeSchedulerKeepsActiveEndpointsAheadOfIdle(t *testing.T) {
	before, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowResumeScheduler(t)
	}
	testharness.AssertNoGoroutineLeak(t, "flow resume scheduler", before)
	testharness.AssertNoFDLeak(t, "flow resume scheduler", fds, 0)
}

func exerciseFlowResumeScheduler(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: bounded scheduler exercise
	defer cancel()
	entered := make(chan struct{}, 32)
	release := make(chan struct{})
	var activeDone atomic.Int32
	active := make([]func(time.Time) error, 32)
	for i := range active {
		active[i] = func(deadline time.Time) error {
			if until := time.Until(deadline); until <= 0 || until > tunnel.ResumeAttempt {
				return errors.New("unbounded carrier attempt")
			}
			entered <- struct{}{}
			select {
			case <-release:
				activeDone.Add(1)
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	var idleEarly, idleDone atomic.Int32
	idle := make([]func(time.Time) error, 96)
	for i := range idle {
		idle[i] = func(time.Time) error {
			if activeDone.Load() != 32 {
				idleEarly.Add(1)
			}
			idleDone.Add(1)
			return nil
		}
	}
	done := make(chan error, 1)
	go func() { done <- resumeFlowOperations(ctx, active, idle, time.Now().Add(4*time.Second)) }()
	timeout := time.NewTimer(2 * time.Second)
	defer timeout.Stop()
	for count := 0; count < 32; count++ {
		select {
		case <-entered:
		case <-timeout.C:
			cancel()
			<-done
			t.Fatalf("only %d of 32 active endpoints started while authorization was blocked", count)
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if idleEarly.Load() != 0 || idleDone.Load() != 96 {
		t.Fatal("idle endpoints ran before the active cohort completed, or were dropped")
	}
}

// origin: hot-update S5 plan §7.1 / T38
func TestFlowResumeRetriesKeepTheAbsoluteCutDeadline(t *testing.T) {
	before, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseFlowResumeRetry(t)
	}
	testharness.AssertNoGoroutineLeak(t, "flow resume retry", before)
	testharness.AssertNoFDLeak(t, "flow resume retry", fds, 0)
}

func exerciseFlowResumeRetry(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: retry deadline exercise
	defer cancel()
	deadline := time.Now().Add(300 * time.Millisecond)
	var attempts int
	err := resumeFlowOperation(ctx, func(attempt time.Time) error {
		attempts++
		if attempt.After(deadline) {
			t.Fatal("retry extended the original deadline")
		}
		return errors.New("authorization refused")
	}, deadline)
	if !errors.Is(err, context.DeadlineExceeded) || attempts < 2 || attempts > 3 {
		t.Fatalf("deadline/retry receipt: attempts=%d, err=%v", attempts, err)
	}
}

// origin: hot-update S5 plan §7.1 / T38
// These exercises use the real frozen contracts: no stub is replaced with a
// successful test double. P1/P7/P8/P10 must land before this oracle can pass.
func TestAgentRestoresRawThenResumeThenRegister(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	before, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("carriers", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseAgentFlowRestore(t)
		})
		t.Run("ss_authority", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseRestoredSSAuthority(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "agent flow restore", before)
	testharness.AssertNoFDLeak(t, "agent flow restore", fds, 0)
}

func reserveFlowTestAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func exerciseAgentFlowRestore(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: full restore exercise
	defer cancel()
	url := startNATS(t)
	exporter := newBootAgent(t, url, "node")
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		t.Fatal(err)
	}
	serverAddress := reserveFlowTestAddress(t)
	var rebuilding atomic.Bool
	slowRelease := make(chan struct{})
	var slowPort int
	server := tunnel.NewServer(serverAddress, "127.0.0.1", func(string, string, int, string, int64) error { return nil }, testharness.SilentLog())
	cfg := tunnel.FlowConfig{Budget: &workload.ReplayBudget{Limit: workload.DefaultReplayBudget},
		InstanceID: exporter.instanceID, OwnerGen: 1, BootID: boot, MaxEndpoints: 128, MaxPendingFDs: 32,
		TokenLookup: func(tunnel.AuthRequest) error { return nil },
		Authorize: func(req tunnel.AuthRequest) (tunnel.Authority, error) {
			if rebuilding.Load() && req.Allocation.Port == slowPort {
				timer := time.NewTimer(time.Until(req.Deadline))
				defer timer.Stop()
				select {
				case <-slowRelease:
				case <-req.Stop:
					return tunnel.Authority{}, context.Canceled
				case <-timer.C:
					return tunnel.Authority{}, context.DeadlineExceeded
				}
			}
			return tunnel.Authority{Enabled: true, Allocation: req.Allocation, Generation: 1, ProxyEpoch: 1, AppliedRevision: 1}, nil
		}}
	if err := server.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(target.Close)
	targetPort := target.Addr().(*net.TCPAddr).Port
	var accepts atomic.Int32
	var targetWG sync.WaitGroup
	var targetConns sync.Map
	targetWG.Go(func() {
		for {
			conn, err := target.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			targetConns.Store(conn, true)
			targetWG.Go(func() {
				defer func(closeFunc func() error) { _ = closeFunc() }(conn.Close)
				defer targetConns.Delete(conn)
				// Hide TCP splice: its cached pipe pairs are Go runtime resources,
				// and would contaminate this fixture's strict business-FD baseline.
				// origin: hot-update S5 plan §7.1 / T38
				_, _ = io.Copy(conn, struct{ io.Reader }{conn})
			})
		}
	})
	defer func() {
		_ = target.Close()
		targetConns.Range(func(key, _ any) bool { _ = key.(net.Conn).Close(); return true })
		targetWG.Wait()
	}()
	initial := NewTunnelExposeAdapter(serverAddress, "lab", "node", testharness.SilentLog())
	var publicPorts []int
	// Adapter cancellation starts the lifetime closer asynchronously. The
	// fixture owns every endpoint and must join their closers before its leak
	// baseline, just as the single-endpoint tunnel fixtures call Client.Close.
	// origin: hot-update S5 plan §5.3 / §7.1 / T38
	defer func() {
		stopExposeAdapter(&Agent{cfg: Config{ExposeAdapter: initial}})
		for _, port := range publicPorts {
			initial.client.Close(port)
		}
	}()
	cfg.Budget = &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	if err := initial.client.SetFlowConfig(cfg); err != nil {
		t.Fatal(err)
	}
	initial.Start(ctx)
	var cohort []net.Conn
	defer func() {
		for _, conn := range cohort {
			_ = conn.Close()
		}
	}()
	for endpoint := 0; endpoint < 40; endpoint++ {
		publicAddr := reserveFlowTestAddress(t)
		_, portText, _ := net.SplitHostPort(publicAddr)
		publicPort, _ := strconv.Atoi(portText)
		publicPorts = append(publicPorts, publicPort)
		if endpoint == 0 {
			slowPort = publicPort
		}
		if err := initial.AddProxy(ctx, PortToken{Port: publicPort, LocalPort: targetPort, Token: "fixture-token"}); err != nil {
			t.Fatal(err)
		}
		if endpoint < 32 {
			conn, err := net.DialTimeout("tcp", publicAddr, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			cohort = append(cohort, conn)
			if err := exchangeFlowRecord(conn, []byte(fmt.Sprintf("before-%02d", endpoint)), time.Now().Add(5*time.Second)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if accepts.Load() != 32 {
		t.Fatalf("active cohort nonempty receipt: accepts=%d", accepts.Load())
	}
	deadline := time.Now().Add(5 * time.Second)
	if err := initial.client.PrepareHandoff(2, [16]byte{1}, deadline); err != nil {
		t.Fatal(err)
	}
	cut, err := initial.client.FreezeFlows(deadline)
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S5 plan §5.3 / T38. A refused dependency export
	// still owns its staged sealed routes; abandon that cut before adapter exit.
	defer func() { _ = cut.Thaw() }()
	im, snapshot, err := cut.Export()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(snapshot.Close)
	if len(im.Flows.Records) != 32 {
		t.Fatal("export dropped the active cohort")
	}
	runtimeCut := freezeBootAgent(t, exporter)
	bundle := runtimeCut.Bundle()
	bundle.Lineage.Generation = 2
	// The frozen S5 public schema names this field "flow". Encoding the fixture
	// through that contract keeps this test compilable on the P0 tree, while the
	// required-domain assertion refuses a reader that still drops the new field.
	data, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	fields["flow"], err = json.Marshal(im)
	if err != nil {
		t.Fatal(err)
	}
	data, err = json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &bundle); err != nil {
		t.Fatal(err)
	}
	hasFlow := false
	for _, domain := range bundle.RequiredDomains() {
		if domain == "flow-v1" {
			hasFlow = true
		}
	}
	if !hasFlow {
		t.Fatal("P17 flow composition unavailable")
	}
	_, runtimeResources, err := exporter.processRuntime.Export()
	if err != nil {
		t.Fatal(err)
	}
	resources, err := handoff.MergeResources(runtimeResources, snapshot.Resources)
	if err != nil {
		t.Fatal(err)
	}
	src := handofftest.DupResources(t, resources)
	defer src.Close()
	image := agentboot.FromCut(bundle, bundle.Processes.Records, src, src.Borrow())
	t.Cleanup(func() {
		if !image.Activated() {
			image.Abandon(nil, agentboot.CrashWindowNoRollbackTarget, "test")
		}
	})
	adapter := NewTunnelExposeAdapter(serverAddress, "lab", "node", testharness.SilentLog())
	receiver, err := New(Config{NATSURL: url, SID: "lab", NID: "node", Home: t.TempDir(), BootImage: image,
		ExposeAdapter: adapter, RemoteFSMode: "off", Logger: testharness.SilentLog(), RosterRefreshInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	cleanupBootAgent(t, receiver)
	if adapter.started || receiver.flowState == nil || receiver.flowState.rawActive || src.Remaining() != 0 {
		t.Fatal("New started workers, dropped flow bindings, or left descriptor ownership incomplete")
	}
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := runtimeCut.Discard(); err != nil {
		t.Fatal(err)
	}
	var earlyNATS atomic.Bool
	receiver.cfg.TeardownDialFn = func(ctx context.Context, network, address string) (net.Conn, error) {
		if !receiver.flowState.rawActive || !receiver.flowState.resumed {
			earlyNATS.Store(true)
		}
		return new(net.Dialer).DialContext(ctx, network, address)
	}
	control, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	registered := make(chan struct{}, 1)
	_, err = control.Subscribe(proto.SubjNodeRegister("lab", "node"), func(msg *nats.Msg) {
		if !receiver.flowState.resumed {
			earlyNATS.Store(true)
		}
		data, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
		_ = msg.Respond(data)
		select {
		case registered <- struct{}{}:
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := control.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	rebuilding.Store(true)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- receiver.Run(runCtx) }()
	defer func() {
		stop()
		<-done
		for _, port := range publicPorts {
			adapter.client.Close(port)
		}
	}()
	healthyDeadline := time.Now().Add(5 * time.Second)
	var trafficWG sync.WaitGroup
	trafficErrors := make(chan error, 31)
	for i := 1; i < 32; i++ {
		trafficWG.Go(func() {
			trafficErrors <- exchangeFlowRecord(cohort[i], []byte(fmt.Sprintf("after-%02d", i)), healthyDeadline)
		})
	}
	trafficWG.Wait()
	close(trafficErrors)
	for err := range trafficErrors {
		if err != nil {
			t.Fatal("healthy active flow exceeded 5s", err)
		}
	}
	close(slowRelease)
	if err := exchangeFlowRecord(cohort[0], []byte("after-00"), healthyDeadline); err != nil {
		t.Fatal(err)
	}
	select {
	case <-registered:
	case <-time.After(5 * time.Second):
		t.Fatal("no post-RESUME register")
	}
	if earlyNATS.Load() || accepts.Load() != 32 {
		t.Fatal("NATS preceded RESUME or a business socket was re-created")
	}
}

func exchangeFlowRecord(conn net.Conn, record []byte, deadline time.Time) error {
	if err := conn.SetDeadline(deadline); err != nil {
		return err
	}
	if _, err := conn.Write(record); err != nil {
		return err
	}
	received := make([]byte, len(record))
	if _, err := io.ReadFull(conn, received); err != nil {
		return err
	}
	if !bytes.Equal(record, received) {
		return errors.New("original flow byte ledger mismatch")
	}
	return nil
}

func exerciseRestoredSSAuthority(t *testing.T) {
	t.Helper()
	target, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(target.Close)
	observed := make(chan string, 2)
	targetConn := make(chan net.Conn, 1)
	var targetWG sync.WaitGroup
	targetWG.Go(func() {
		conn, err := target.Accept()
		if err != nil {
			return
		}
		targetConn <- conn
		defer func(closeFunc func() error) { _ = closeFunc() }(conn.Close)
		for _, word := range []string{"before", "after!"} {
			data := make([]byte, len(word))
			if _, err := io.ReadFull(conn, data); err != nil {
				return
			}
			observed <- string(data)
		}
	})
	defer func() {
		_ = target.Close()
		select {
		case conn := <-targetConn:
			_ = conn.Close()
		default:
		}
		targetWG.Wait()
	}()
	original := ssproxy.New(testharness.SilentLog())
	local, err := original.Start(0, []ssproxy.Key{{KeyID: "fixture", Secret: "fixture-password"}})
	if err != nil {
		t.Fatal(err)
	}
	defer original.Stop()
	// origin: hot-update S5 plan §6.5 / T38. ExpectFlow requires the real
	// flow owner to be registered before the first local connect side effect.
	var revokeSocket atomic.Pointer[rawsocket.Socket]
	if err := original.SetFlowRevoke(func([16]byte, string) {
		if socket := revokeSocket.Load(); socket != nil {
			_ = socket.Shutdown()
		}
	}); err != nil {
		t.Fatal(err)
	}
	socket, err := rawsocket.Connect(netip.MustParseAddrPort(fmt.Sprintf("127.0.0.1:%d", local)), time.Now().Add(time.Second), nil,
		func(tuple rawsocket.Tuple) error { return original.ExpectFlow([16]byte{1}, tuple) })
	if err != nil {
		t.Fatal(err)
	}
	revokeSocket.Store(socket)
	defer func(closeFunc func() error) { _ = closeFunc() }(socket.Close)
	if err := socket.CompleteConnect(time.Now().Add(time.Second), nil); err != nil {
		t.Fatal(err)
	}
	// The sender is an independent SS encoder. Only protocol primitives are
	// shared; no server reader/writer/codec is used to establish the oracle.
	master, previous := []byte{}, []byte{}
	for len(master) < 32 {
		h := md5.New()
		_, _ = h.Write(previous)
		_, _ = h.Write([]byte("fixture-password"))
		previous = h.Sum(nil)
		master = append(master, previous...)
	}
	salt := bytes.Repeat([]byte{0x42}, 32)
	subkey := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha1.New, master[:32], salt, []byte("ss-subkey")), subkey); err != nil {
		t.Fatal(err)
	}
	cipher, err := chacha20poly1305.New(subkey)
	if err != nil {
		t.Fatal(err)
	}
	nonce := make([]byte, 12)
	encode := func(payload []byte) []byte {
		length := make([]byte, 2)
		binary.BigEndian.PutUint16(length, uint16(len(payload)))
		encrypted := cipher.Seal(nil, nonce, length, nil)
		incrementReferenceNonce(nonce)
		encrypted = append(encrypted, cipher.Seal(nil, nonce, payload, nil)...)
		incrementReferenceNonce(nonce)
		return encrypted
	}
	targetPort := target.Addr().(*net.TCPAddr).Port
	header := []byte{1, 127, 0, 0, 1, byte(targetPort >> 8), byte(targetPort)}
	if _, err := socket.Write(append(salt, encode(append(header, []byte("before")...))...)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-observed:
		if got != "before" {
			t.Fatal("reference SS precondition failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SS actor never reached original target")
	}
	frozen, err := original.Freeze(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = frozen.Thaw() }()
	im, snapshot, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(snapshot.Close)
	source := handofftest.DupResources(t, snapshot.Resources)
	restored, salvage, err := ssproxy.Restore(im, source)
	if err != nil {
		closeRefusedFlowResources(salvage)
		t.Fatal(err)
	}
	defer restored.Stop()
	if err := restored.SetFlowRevoke(func([16]byte, string) { _ = socket.Shutdown() }); err != nil {
		t.Fatal(err)
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := restored.ActivateRaw(); err != nil {
		t.Fatal(err)
	}
	if _, err := socket.Write(encode([]byte("after!"))); err != nil {
		t.Fatal(err)
	}
	select {
	case <-observed:
		t.Fatal("SS target Write preceded fresh RESUME grant")
	case <-time.After(100 * time.Millisecond):
	}
	if err := restored.Authorize(ssproxy.Grant{Enabled: true, Generation: 1, Epoch: 1, AppliedRevision: 2,
		Keys: []ssproxy.Key{{KeyID: "fixture", Secret: "fixture-password"}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-observed:
		if got != "after!" {
			t.Fatal("SS suffix changed after authorization")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fresh grant did not release the preserved SS actor")
	}
}

// Refusal transfers taken duplicates back to the caller, rather than Source.
// Keep the original owner alive while closing only that explicit salvage set.
// origin: hot-update S5 plan §5.3 / T38
func closeRefusedFlowResources(resources []handoff.Resource) {
	for _, resource := range resources {
		for _, file := range resource.Files {
			_ = file.Close()
		}
	}
}

func incrementReferenceNonce(nonce []byte) {
	for i := range nonce {
		nonce[i]++
		if nonce[i] != 0 {
			return
		}
	}
}
