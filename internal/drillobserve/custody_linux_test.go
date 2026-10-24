//go:build linux

package drillobserve

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

func custodyPair(t *testing.T) (*os.File, *os.File, int) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	sock := os.NewFile(uintptr(pair[0]), "custody-channel")
	configFD, err := unix.MemfdCreate("custody-config", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatal(err)
	}
	config := os.NewFile(uintptr(configFD), "custody-config")
	data := []byte("TDCFG1\x00\x00")
	data = binary.LittleEndian.AppendUint64(data, 4)
	data = append(data, bytes.Repeat([]byte{7}, 16)...)
	if _, err := config.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(configFD), unix.F_ADD_SEALS, custodySeals); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = config.Close(); _ = sock.Close(); _ = unix.Close(pair[1]) })
	return config, sock, pair[1]
}

// origin: hot-update S5 plan §6.6 / T26 / DRILL-OBSERVER
func TestCustodyRequiresBothStartupEnablers(t *testing.T) {
	if mode := os.Getenv("TETHER_TEST_OBSERVER_HELPER"); mode != "" {
		want := mode == "active"
		if (Hook != nil) != want {
			t.Fatalf("observer active=%t, want %t", Hook != nil, want)
		}
		if os.Getenv(EnableEnv) != "" || os.Getenv(CustodyEnv) != "" {
			t.Fatal("private startup environment retained")
		}
		if want {
			Hook(Event{Kind: "ss-seal", Domain: "ss", Data: []byte("private-cipher-suffix")})
			Flush()
		}
		return
	}
	config, sock, receiver := custodyPair(t)
	ordinary, err := os.CreateTemp(t.TempDir(), "not-custody")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := ordinary.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	unsealedFD, err := unix.MemfdCreate("unsealed-config", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		t.Fatal(err)
	}
	unsealed := os.NewFile(uintptr(unsealedFD), "unsealed-config")
	defer func() {
		if err := unsealed.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	content := make([]byte, 32)
	if _, err := config.ReadAt(content, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := unsealed.Write(content); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name, enabled, descriptor string
		config                    *os.File
		active                    bool
	}{
		{"neither", "", "", config, false}, {"env-only", "v1", "", config, false},
		{"fd-only", "", "3", config, false}, {"ordinary", "v1", "3", ordinary, false},
		{"unsealed-memfd", "v1", "3", unsealed, false}, {"invalid-version", "v2", "3", config, false}, {"both", "v1", "3", config, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mode := "inert"
			if c.active {
				mode = "active"
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestCustodyRequiresBothStartupEnablers$", "-test.count=1")
			cmd.Env = append(StripEnvironment(os.Environ()), "TETHER_TEST_OBSERVER_HELPER="+mode, EnableEnv+"="+c.enabled, CustodyEnv+"="+c.descriptor)
			cmd.ExtraFiles = []*os.File{c.config, sock}
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("helper failed: %v: %s", err, out)
			}
		})
	}
	var timeout unix.Timeval
	timeout.Sec = 2
	if err := unix.SetsockoptTimeval(receiver, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &timeout); err != nil {
		t.Fatal(err)
	}
	b, control := make([]byte, 4), make([]byte, unix.CmsgSpace(4))
	n, oob, flags, _, err := unix.Recvmsg(receiver, b, control, 0)
	if err != nil || n != 4 || flags&(unix.MSG_CTRUNC|unix.MSG_TRUNC) != 0 {
		t.Fatal("active custody did not deliver sealed descriptor")
	}
	messages, err := unix.ParseSocketControlMessage(control[:oob])
	if err != nil {
		t.Fatal(err)
	}
	rights, err := unix.ParseUnixRights(&messages[0])
	if err != nil || len(rights) != 1 {
		t.Fatal("bad custody rights")
	}
	defer func() {
		if err := unix.Close(rights[0]); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	seals, err := unix.FcntlInt(uintptr(rights[0]), unix.F_GET_SEALS, 0)
	if err != nil || seals&custodySeals != custodySeals {
		t.Fatal("observation not immutable")
	}
	raw := make([]byte, 2048)
	read, err := unix.Pread(rights[0], raw, 0)
	if err != nil || !bytes.Contains(raw[:read], []byte("private-cipher-suffix")) {
		t.Fatal("private observation missing from custody")
	}
}

// origin: hot-update S5 plan §6.2 / §9.1 / DRILL-OBSERVER
func TestCustodyOverflowAndWriteFailurePoisonTheStream(t *testing.T) {
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseCustodyBackpressure(t) })
	}
	// Large private suffixes must not repeatedly grow the batch buffer while
	// the bounded queue continues accepting independent product observations.
	// origin: hot-update S5 plan §6.6 / §9.1 / DRILL-RUN 75.C
	batch := make([]Event, maxBatch)
	for i := range batch {
		batch[i] = Event{Kind: "ss-write", Domain: "ss", Data: bytes.Repeat([]byte{byte(i)}, MaxData)}
	}
	var encoded []byte
	allocations := testing.AllocsPerRun(5, func() { encoded = encodeEvents(batch) })
	if allocations != 1 {
		t.Fatalf("custody batch repeatedly reallocates private suffixes: allocations=%v", allocations)
	}
	want := 4
	for _, event := range batch {
		want += 234 + len(event.Kind) + len(event.Domain) + len(event.Data)
	}
	if len(encoded) != want || binary.LittleEndian.Uint32(encoded) != maxBatch {
		t.Fatal("bounded encoding changed the private event layout")
	}
	c := &custody{queue: make(chan pending, 1), channel: -1}
	c.record(Event{Kind: "ss-seal", Domain: "ss"})
	c.record(Event{Kind: "ss-seal", Domain: "ss"})
	if !c.broken.Load() || c.failure.Load() != 2 {
		t.Fatal("overflow did not poison custody")
	}
	c = &custody{queue: make(chan pending, 1), channel: -1}
	c.send([]Event{{Kind: "ss-seal", Domain: "ss"}})
	if !c.broken.Load() || c.failure.Load() != 3 {
		t.Fatal("write failure did not poison custody")
	}
	c = &custody{queue: make(chan pending, 1)}
	c.record(Event{Kind: "ss-write", Domain: "ss", Data: make([]byte, MaxData+1)})
	if !c.broken.Load() || c.failure.Load() != 4 || len(c.queue) != 0 {
		t.Fatal("oversize observation was truncated or retained")
	}
	poisonCustody(c, 3)
	if c.failure.Load() != 4 {
		t.Fatal("later transport failure replaced the original poison cause")
	}
	previous := active
	active = c
	c.fd = -1
	c.channel = -1
	t.Cleanup(func() { active = previous })
	env := ExecEnvironment([]string{"TERM=xterm"})
	if len(env) != 1 || env[0] != "TERM=xterm" {
		t.Fatal("broken custody was re-enabled by exec")
	}

}

// A full descriptor channel is backpressure, not a lost batch. The private
// writer may wait for capacity; the product hook and its unchanged queue may
// never wait. Overflow, flush expiry and a dead peer still revoke custody.
// origin: hot-update S5 plan §6.6 / §9.1 / DRILL-RUN 75.C
func exerciseCustodyBackpressure(t *testing.T) {
	t.Helper()
	for _, scenario := range []string{"drain", "queue-overflow", "flush-timeout", "peer-closed"} {
		t.Run(scenario, func(t *testing.T) {
			pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
			if err := unix.SetsockoptInt(pair[0], unix.SOL_SOCKET, unix.SO_SNDBUF, 4096); err != nil {
				t.Fatal(err)
			}
			filled := 0
			for {
				err := unix.Sendmsg(pair[0], []byte("FULL"), nil, nil, unix.MSG_DONTWAIT|unix.MSG_NOSIGNAL)
				if err == unix.EAGAIN {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				filled++
				if filled > 1024 {
					t.Fatal("custody channel never reached real kernel backpressure")
				}
			}
			c := &custody{channel: pair[0], queue: make(chan pending, queueDepth)}
			done := make(chan struct{})
			go func() {
				c.send([]Event{{Kind: "ss-write", Domain: "ss", Data: []byte("held-private-suffix")}})
				close(done)
			}()
			defer func() { poisonCustody(c, 1); _ = unix.Shutdown(pair[1], unix.SHUT_RDWR); <-done }()
			select {
			case <-done:
				t.Fatal("temporary descriptor backpressure discarded a custody batch")
			case <-time.After(20 * time.Millisecond):
			}
			var held unix.Stat_t
			heldFD := -1
			entries, err := os.ReadDir("/proc/self/fd")
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				link, _ := os.Readlink("/proc/self/fd/" + entry.Name())
				if link == "/memfd:tether-drill-custody (deleted)" {
					fd, err := strconv.Atoi(entry.Name())
					if err != nil {
						t.Fatal(err)
					}
					if heldFD != -1 {
						t.Fatal("backpressure retained more than one sealed batch descriptor")
					}
					heldFD = fd
					if err := unix.Fstat(fd, &held); err != nil {
						t.Fatal(err)
					}
				}
			}
			if heldFD == -1 {
				t.Fatal("backpressure lost sealed descriptor custody")
			}
			var receiverFD = -1
			switch scenario {
			case "drain":
				receiveDeadline := time.Now().Add(time.Second)
				for index := 0; index <= filled; index++ {
					data, control := make([]byte, 4), make([]byte, unix.CmsgSpace(4))
					n, oob, flags, _, err := unix.Recvmsg(pair[1], data, control, unix.MSG_DONTWAIT)
					if err == unix.EAGAIN {
						if !time.Now().Before(receiveDeadline) {
							t.Fatal("retained custody packet never reached the peer")
						}
						index--
						time.Sleep(time.Millisecond)
						continue
					}
					if err != nil || n != 4 || flags != 0 {
						t.Fatal("backpressure altered the custody packet")
					}
					if index < filled {
						if string(data) != "FULL" || oob != 0 {
							t.Fatal("custody packet overtook prior channel messages")
						}
						continue
					}
					messages, err := unix.ParseSocketControlMessage(control[:oob])
					if err != nil || len(messages) != 1 {
						t.Fatal("missing custody descriptor")
					}
					rights, err := unix.ParseUnixRights(&messages[0])
					if err != nil || len(rights) != 1 {
						t.Fatal("invalid custody descriptor")
					}
					receiverFD = rights[0]
					defer func() { _ = unix.Close(receiverFD) }()
					var received unix.Stat_t
					if err := unix.Fstat(receiverFD, &received); err != nil || received.Ino != held.Ino || received.Dev != held.Dev {
						t.Fatal("backpressure replaced the original sealed batch")
					}
					seals, err := unix.FcntlInt(uintptr(receiverFD), unix.F_GET_SEALS, 0)
					if err != nil || seals&custodySeals != custodySeals {
						t.Fatal("backpressure delivered mutable custody")
					}
					raw := make([]byte, 2048)
					read, err := unix.Pread(receiverFD, raw, 0)
					if err != nil || read < 76 || binary.LittleEndian.Uint64(raw[56:64]) != 1 || binary.LittleEndian.Uint64(raw[64:72]) != 0 || !bytes.Contains(raw[:read], []byte("held-private-suffix")) {
						t.Fatal("backpressure dropped, resequenced or poisoned the original observation")
					}
				}
			case "queue-overflow":
				for index := 0; index <= queueDepth; index++ {
					c.record(Event{Kind: "ss-write", Domain: "ss"})
				}
			case "flush-timeout":
				previous := active
				active = c
				Flush()
				active = previous
			case "peer-closed":
				if err := unix.Shutdown(pair[1], unix.SHUT_RDWR); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("custody writer did not release its descriptor")
			}
			wantCause := map[string]uint64{"drain": 0, "queue-overflow": 2, "flush-timeout": 11, "peer-closed": 3}[scenario]
			if c.failure.Load() != wantCause || c.broken.Load() != (wantCause != 0) {
				t.Fatal("backpressure changed custody poison semantics")
			}
			if len(c.queue) > queueDepth || c.sequence != 1 {
				t.Fatal("backpressure renewed the queue or batch sequence")
			}
			var after unix.Stat_t
			if err := unix.Fstat(heldFD, &after); err == nil && after.Ino == held.Ino && heldFD != receiverFD {
				t.Fatal("sender leaked its sealed batch descriptor")
			}
		})
	}
}

// The Python collector reads this real sealed SCM_RIGHTS stream, never a JSON
// event fixture. These declared synthetic values are oracle controls only.
// origin: hot-update S5 plan §9.1 / DRILL-OBSERVER
func TestCustodyRecordedStream(t *testing.T) {
	if active == nil {
		// Missing inherited custody is NOT_RUN pending S10 P06; no stream was exercised.
		// origin: S10 internal review NO-SKIP-AS-PASS F3
		if os.Getenv("TETHER_TEST_OBSERVER_RECORDING") != "" {
			t.Fatal("SETUP-RED: NOT_RUN pending S10 P06: recording requires inherited custody")
		}
		// The ordinary package run supplies the same real startup custody as
		// the Python collector; explicit recorder modes still require it.
		// origin: S10 internal review POLISH-A F2
		g, fd := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseCustodyRecording(t)
		}
		testharness.AssertNoGoroutineLeak(t, "recorded custody x5", g)
		testharness.AssertNoFDLeak(t, "recorded custody x5", fd, 0)
		return
	}
	endpoint, flow, actor := [16]byte{1}, [16]byte{2}, [16]byte{3}
	digest := sha256.Sum256(bytes.Repeat([]byte{6}, 32))
	emit := func(kind, domain string, values [16]uint64, data []byte) {
		Hook(Event{Kind: kind, Domain: domain, Endpoint: endpoint, Flow: flow, Actor: actor, Digest: digest, Values: values, Data: data})
	}
	emit("ss-seal", "ss", [16]uint64{2}, nil)
	emit("ss-write", "ss", [16]uint64{20, 7, 13, 1, 1}, bytes.Repeat([]byte{9}, 13))
	flowData := make([]byte, 32)
	binary.LittleEndian.PutUint64(flowData[16:24], 1)
	binary.LittleEndian.PutUint64(flowData[24:32], 99)
	emit("flow-state", "flow", [16]uint64{1, 1, 1, 2, 1, 1, 77, 0140000, 10, 20, 10, 20, 8 << 20, 10, 1, 1}, flowData)
	emit("resume", "tunnel", [16]uint64{1, 1, 1, 14001}, nil)
	for _, domain := range []string{"exec", "pty", "flow", "ss"} {
		emit("domain-cut", domain, [16]uint64{1}, nil)
	}
	emit("domain-live", "exec", [16]uint64{}, nil)
	emit("domain-live", "pty", [16]uint64{}, nil)
	stateData := make([]byte, 56)
	binary.LittleEndian.PutUint64(stateData[24:32], 1)
	binary.LittleEndian.PutUint64(stateData[32:40], 88)
	emit("ss-state", "ss", [16]uint64{}, stateData)
	emit("flow-retirement", "flow", [16]uint64{1, 0, 0, 1}, nil)
	mode := os.Getenv("TETHER_TEST_OBSERVER_RECORDING")
	if mode == "revoked" {
		emit("ss-revoke-mark", "ss", [16]uint64{8, 0, 0, 0, 0, 1, 1}, stateData)
		emit("flow-state", "flow", [16]uint64{1, 1, 1, 5, 3, 1, 77, 0140000, 10, 20, 10, 20}, nil)
		emit("ss-closed", "ss", [16]uint64{8, 0, 0, 0, 0, 1, 0, 1}, stateData)
	}
	if mode == "progress" {
		emit("flow-state", "flow", [16]uint64{1, 1, 1, 3, 2, 1, 77, 0140000, 11, 21, 11, 21, 8 << 20, 10, 1, 1}, flowData)
		stateData[0] = 1
		emit("ss-state", "ss", [16]uint64{}, stateData)
		emit("flow-retirement", "flow", [16]uint64{2, 0, 1, 1}, nil)
	}
	if mode == "watchdog" {
		emit("final-cut", "agent", [16]uint64{1}, nil)
		emit("register-applied", "agent", [16]uint64{}, nil)
		emit("commit-write-failed", "agent", [16]uint64{1, 1, 1}, nil)
		emit("mark-ready", "agent", [16]uint64{}, nil)
		emit("watchdog-current", "agent", [16]uint64{1, 120_000_000_001, 1}, nil)
		emit("final-cut", "agent", [16]uint64{2}, nil)
	}
	emit("reaper", "proxy", [16]uint64{14001, 0, 3, 1, 1}, []byte("brk1"))
	emit("ss-grant", "ss", [16]uint64{2, 3, 4, 1, 2}, nil)
	emit("carrier-negotiated", "tunnel", [16]uint64{}, nil)
	emit("carrier-prefix", "tunnel", [16]uint64{}, make([]byte, 12))
	Flush()
	if mode == "unit" {
		// Keep the original producer alive through its collector's fresh drain.
		if _, err := io.ReadFull(os.Stdin, make([]byte, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "keeper" {
		_, _ = io.ReadFull(os.Stdin, make([]byte, 1))
		active.broken.Store(true)
		if _, err := fmt.Fprintln(os.Stdout, "custody-poisoned"); err != nil {
			t.Fatal(err)
		}
		_, _ = io.ReadFull(os.Stdin, make([]byte, 1))
	}
}

// origin: hot-update S5 plan §6.6 / T26 / DRILL-OBSERVER
func TestObservationHooksKeepExactProductBarriers(t *testing.T) {
	root := filepath.Join("..", "..")
	cases := []struct{ path, start, before, hook, after string }{
		{"internal/broker/proxy_reconcile.go", "if !b.nodeProxyReady(sid, nid)", "proxyRotateUnready(b, sid, nid)", "Kind: \"reaper\"", "b.clearRehomeMark(existing.Port) // healthy"},
		{"internal/agent/ssproxy/fence.go", "func closeActor", "_ = f.Close()", "observedActor(a, \"ss-closed\")", "func retireActorLocked"},
		{"internal/agent/ssproxy/crypto.go", "func sealChunk", "if *exhausted", "observeSeal(a, nonce, len(plain))", "a.Seal(nil, nonce, plain, nil)"},
		{"internal/agent/ssproxy/crypto.go", "func (aw *aeadWriter) flush", "n, err := aw.w.Write(attempt)", "drillobserve.Hook(e)", "aw.cursor += n"},
		{"internal/agent/ssproxy/actor.go", "func actorIOLocked", "n, err = sock.Write(p)", "drillobserve.Hook(observation)", "if timeoutError(err)"},
		{"internal/agent/ssproxy/fence.go", "func markRevokedLocked", "a.state.put(\"authority_phase\"", "drillobserve.Hook(observedActor(a, \"ss-revoke-mark\"))", "return true"},
		{"internal/tunnel/flow_client.go", "func attachClientFlow", "abortClientPermit(st, e, a)", "observeAttachment(conn, e.id", "return nil"},
		{"internal/tunnel/flow_client.go", "func (c *Client) PrepareHandoff", "err := prepareClientEndpoint(c, e, &a, req, deadline)", "observeAttachment(a.Conn, e.id", "c.logger.Info(\"tunnel: flow handoff prepared\""},
		{"internal/agent/upgrade_image.go", "func prepareImageCut", "if err := check(bundle)", "Kind: \"final-cut\"", "a.cfg.Logger.Info(\"agent: image final cut\""},
		{"internal/agent/ssproxy/keys.go", "func (s *Server) Authorize", "authorizeSurvivorsLocked(st)", "if err == nil {\n\t\tobserveGrantLocked(s, g)", "cleanupActors(s, victims)\n\treturn err"},
		{"internal/flow/manager.go", "func changed", "if drillobserve.Hook != nil", "observeRecordLocked(r)", "close(r.change)"},
		{"internal/flow/manager.go", "func managerChanged", "if drillobserve.Hook != nil", "observeRetirement(id, r)", "close(st.change)"},
		{"internal/tunnel/flow_client.go", "func (c *Client) SetFlowConfig", "func(tuple rawsocket.Tuple) error", "observeFlowSocket(id, flow.EndpointID{}, tuple)", "if cfg.BeforeLocalConnect != nil"},
		{"internal/tunnel/flow_endpoint.go", "func endpointAccept", "req := flow.OpenRequest", "observeFlowSocket(id, e.id, tuple)", "flow.Open(st.manager, req, raw)"},
		{"internal/agentupgrade/legacy_report.go", "func WatchdogLegacy", "if p.ImagePhase()", "Kind: \"watchdog-current\"", "result = p.RollbackImage"},
		{"internal/agentupgrade/legacy_report.go", "func CommitLegacyReport", "if drillobserve.Hook != nil", "Kind: \"register-applied\"", "if reported == \"\""},
		{"internal/agentupgrade/legacy_report.go", "func CommitLegacyReport", "if err := WriteLegacyMarker", "Kind: \"commit-write-failed\"", "p.Logger.Error"},
		{"internal/agentupgrade/control.go", "p.RefreshAdmission()", "p.Image.MarkReady()", "Kind: \"mark-ready\"", "return nil"},
		{"internal/execsession/runtime.go", "func ObserveRuntime", "r.mu.Unlock()", "drillobserve.Hook(*counts[name])", "func observeRuntimeStdio"},
		{"internal/tunnel/client_transport.go", "func (c *Client) bridgeStreamToLocal", "local, err := dialLegacyLocal(c, localPort)", "observeLegacyBridge(observation, stream, local)", "bridge(local, stream)"},
		{"internal/execsession/pty.go", "master, identity, err := session.TakeMaster()", "finishPTYSetupFailure(r, t, err)", "Domain: \"pty\", Values: [16]uint64{identity.Dev, identity.Ino}", "now, err := r.now()"},
		{"internal/agent/upgrade_image.go", "func prepareImageCut", "if err := check(bundle)", "observeImageDomains(bundle)", "Kind: \"final-cut\""},
		// origin: hot-update S10 plan §5.3 / P06 / P07 source handoff
		{"internal/agent/agent.go", "startExposeAdapter(ctx, a)", "if drillobserve.Hook != nil", "go drillobserve.RunRuntimeSamples(watchCtx.Done(),", "a.loadRosterCacheAtBoot()"},
		{"internal/drillobserve/runtime_sample.go", "func RunRuntimeSamples", "if Hook == nil", "time.NewTicker(250 * time.Millisecond)", "defer ticker.Stop()"},
		{"internal/execsession/runtime.go", "func (r *Runtime) start(", "if drillobserve.Hook != nil", "observeRuntimeStdio(stdio)", "workload.NewOutput(stdio.Stdout"},
		{"internal/broker/proxy_reconcile.go", "func (b *Broker) reconcileProxySession", "Kind: \"reaper\"", "uint64(proxyRehomeDwell)", "Data: []byte(existing.HomeBroker)"},
	}
	for _, c := range cases {
		t.Run(c.hook, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(root, c.path))
			if err != nil {
				t.Fatal(err)
			}
			if !observationBarrierOrder(string(raw), c.start, c.before, c.hook, c.after) {
				t.Fatal("observation moved or dropped at its product barrier")
			}
		})
	}
	// The moved observer still belongs to Run cancellation and never acquires
	// business locks. Pin both halves of the source handoff while P07 wires it.
	// origin: hot-update S10 plan §5.3 / P06
	agent, err := os.ReadFile(filepath.Join(root, "internal/agent/agent.go"))
	if err != nil || !strings.Contains(string(agent), "context.WithCancel(ctx)") || !strings.Contains(string(agent), "stopWatch()") {
		t.Fatal("runtime observer lost Run cancellation")
	}
	sampler, err := os.ReadFile("runtime_sample.go")
	if err != nil || !strings.Contains(string(sampler), "case <-done:") {
		t.Fatal("runtime sampler lost exit cancellation")
	}
	_, samplerBody, found := strings.Cut(string(sampler), "func RunRuntimeSamples")
	// The done channel contains struct{} in the frozen signature. Its brace is
	// a type delimiter, so select the body after the callback parameter.
	_, samplerBody, hasBody := strings.Cut(samplerBody, "observeDomains func()) {")
	if !found || !hasBody || !strings.HasPrefix(strings.TrimSpace(samplerBody), "if Hook == nil {\n\t\treturn\n\t}") {
		t.Fatal("inert runtime sampler performed work before its nil guard")
	}
	for _, forbidden := range []string{".Lock()", ".RLock()", "internal/agent", "internal/execsession"} {
		if strings.Contains(string(sampler), forbidden) {
			t.Fatal("runtime sampler acquired a business dependency")
		}
	}
	writer, err := os.ReadFile("custody_linux.go")
	if err != nil || strings.Count(string(writer), "unix.Recvmsg(c.channel,") != 1 ||
		strings.Count(string(writer), "writeCustodyPending(c, p, &batch, requests)") != 2 {
		t.Fatal("runtime challenge raced a second socket reader or disappeared in a legacy drain")
	}
	reaper, err := os.ReadFile(filepath.Join(root, "internal/broker/proxy_reconcile.go"))
	if err != nil || !strings.Contains(string(reaper), "const proxyRehomeDwell = 3") {
		t.Fatal("observer cannot sign a shortened default reaper dwell")
	}
	// Extraction moved the inert guards into the transport leaf. The runtime
	// adapters must remain pure forwarding calls so no work precedes those guards.
	// origin: hot-update S6 plan §9.3 / T54 / TESTFIX
	adapters, err := os.ReadFile(filepath.Join(root, "internal/tunnel/observation.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct{ fn, call string }{
		{"func observeAttachment", "transport.ObserveAttachment(conn, endpoint, owner, carrier, mode, port, kind)"},
		{"func observeFlowSocket", "transport.ObserveFlowSocket(id, endpoint, tuple)"},
	} {
		_, body, found := strings.Cut(string(adapters), c.fn)
		if !found {
			t.Fatal("observation adapter missing")
		}
		_, body, found = strings.Cut(body, "{")
		body, _, closed := strings.Cut(body, "}")
		if !found || !closed || strings.TrimSpace(body) != c.call {
			t.Fatal("observation adapter performs work outside its leaf guard")
		}
	}

	// A benchmark cannot prove absence of syscalls. Pin the literal fast path
	// before any address conversion, hashing, snapshot or observer allocation.
	for _, c := range []struct{ path, fn string }{
		{"internal/agent/ssproxy/observation.go", "func observeCipher"},
		{"internal/agent/ssproxy/observation.go", "func observeSeal"},
		{"internal/tunnel/transport/observation.go", "func ObserveAttachment"},
		{"internal/tunnel/transport/observation.go", "func ObserveFlowSocket"},
		{"internal/tunnel/transport/observation.go", "func ObserveAcceptedFlowSocket"},
		{"internal/execsession/runtime.go", "func ObserveRuntime"},
	} {
		raw, err := os.ReadFile(filepath.Join(root, c.path))
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		at := strings.Index(text, c.fn)
		if at < 0 {
			t.Fatal("inert owner missing")
		}
		body := text[at:]
		body = body[strings.Index(body, "{")+1:]
		guard := "if drillobserve.Hook == nil {\n\t\treturn\n\t}"
		if c.fn == "func observeCipher" {
			guard = "if drillobserve.Hook == nil {\n\t\treturn a\n\t}"
		}
		if c.fn == "func ObserveRuntime" {
			guard = "if drillobserve.Hook == nil || r == nil {\n\t\treturn\n\t}"
		}
		if !strings.HasPrefix(strings.TrimSpace(body), guard) {
			t.Fatal("inert hook no longer consists of a nil check")
		}
	}

	raw, err := os.ReadFile("custody_linux.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	begin := strings.Index(body, "func (c *custody) record")
	end := strings.Index(body[begin:], "func (c *custody) writeLoop")
	for _, forbidden := range []string{"unix.Write(", "unix.Sendmsg(", "os.Open(", "sync.Mutex"} {
		if strings.Contains(body[begin:begin+end], forbidden) {
			t.Fatal("data-path hook performs observer IO or locks")
		}
	}
	e := Event{Data: []byte("private-subkey-target-cipher-nonce")}
	for _, verb := range []string{"%v", "%+v", "%#v", "%s", "%q", "%x", "%X", "%d", "%c"} {
		if strings.Contains(fmt.Sprintf(verb, e), "private-") {
			t.Fatal("event formatting leaked secret")
		}
	}
	public, err := json.Marshal(e)
	if err != nil || strings.Contains(string(public), "Data") || strings.Contains(string(public), "Nonce") || strings.Contains(string(public), "private-") {
		t.Fatal("observation entered public JSON carrier")
	}
	env := []string{"PATH=/safe", EnableEnv + "=v1", CustodyEnv + "=19"}
	if got := StripEnvironment(env); len(got) != 1 || got[0] != env[0] {
		t.Fatal("workload environment retained custody")
	}
}

// The same compiled observer survives real same-PID exec, rather than a modeled
// import. Empty exec-barrier batches also remain in the keeper's sequence.
// origin: hot-update S5 plan §9.1 / DRILL-OBSERVER
func TestCustodySurvivesSamePIDExec(t *testing.T) {
	if os.Getenv("TETHER_TEST_OBSERVER_EXEC_HELPER") == "1" {
		if Hook == nil {
			t.Fatal("successor lost custody")
		}
		var actor [16]byte
		binary.LittleEndian.PutUint64(actor[:8], uint64(os.Getpid()))
		Hook(Event{Kind: "ss-seal", Domain: "ss", Actor: actor})
		Flush()
		if os.Getenv("TETHER_TEST_OBSERVER_EXEC_STAGE") == "" {
			_ = os.Setenv("TETHER_TEST_OBSERVER_EXEC_STAGE", "resumed")
			if err := unix.Exec(os.Args[0], os.Args, ExecEnvironment(os.Environ())); err != nil {
				t.Fatal(err)
			}
		}
		return
	}
	config, sock, receiver := custodyPair(t)
	cmd := exec.Command(os.Args[0], "-test.run=^TestCustodySurvivesSamePIDExec$", "-test.count=1")
	cmd.Dir = t.TempDir()
	cmd.Env = append(StripEnvironment(os.Environ()), "TETHER_TEST_OBSERVER_EXEC_HELPER=1", EnableEnv+"=v1", CustodyEnv+"=3")
	cmd.ExtraFiles = []*os.File{config, sock}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("same-PID exec failed: %v: %s", err, out)
	}
	timeout := unix.Timeval{Sec: 2}
	_ = unix.SetsockoptTimeval(receiver, unix.SOL_SOCKET, unix.SO_RCVTIMEO, &timeout)
	incarnations := map[[16]byte]bool{}
	attempts := 0
	for round := 0; round < 3; round++ {
		raw, control := make([]byte, 4), make([]byte, unix.CmsgSpace(4))
		n, oob, flags, _, err := unix.Recvmsg(receiver, raw, control, 0)
		if err != nil || n != 4 || flags&(unix.MSG_CTRUNC|unix.MSG_TRUNC) != 0 {
			t.Fatal("exec broke keeper sequence")
		}
		messages, err := unix.ParseSocketControlMessage(control[:oob])
		if err != nil || len(messages) != 1 {
			t.Fatal("invalid custody rights")
		}
		rights, err := unix.ParseUnixRights(&messages[0])
		if err != nil || len(rights) != 1 {
			t.Fatal("invalid custody descriptor")
		}
		data := make([]byte, 1024)
		read, err := unix.Pread(rights[0], data, 0)
		_ = unix.Close(rights[0])
		if err != nil || read < 76 {
			t.Fatal("exec custody truncated")
		}
		if binary.LittleEndian.Uint32(data[72:76]) == 0 {
			continue
		}
		// Fixed declared kind/domain put Actor at byte119 in this private frame.
		if binary.LittleEndian.Uint64(data[119:127]) != uint64(cmd.Process.Pid) {
			t.Fatal("exec replaced the process instead of its image")
		}
		var incarnation [16]byte
		copy(incarnation[:], data[40:56])
		incarnations[incarnation] = true
		attempts++
	}
	if attempts != 2 || len(incarnations) != 2 {
		t.Fatal("original and successor attempts did not reach the same keeper")
	}
}

// The scanner follows responsibility moves while still enforcing predecessor,
// exact hook and successor order. Empty or stale roots cannot prove a barrier.
// origin: hot-update S7 plan §11.4 / T50
func observationBarrierOrder(text, start, before, hook, after string) bool {
	offset := strings.Index(text, start)
	if offset < 0 {
		return false
	}
	text = text[offset:]
	predecessor, event, successor := strings.Index(text, before), strings.Index(text, hook), strings.Index(text, after)
	return predecessor >= 0 && event > predecessor && successor > event
}

// origin: hot-update S7 plan §11.4 / T50
func TestObservationBarrierScannerFollowsCurrentOwners(t *testing.T) {
	raw, err := os.ReadFile("../agentupgrade/legacy_report.go")
	if err != nil {
		t.Fatal(err)
	}
	start, before, hook, after := "func WatchdogLegacy", "if p.ImagePhase()", "Kind: \"watchdog-current\"", "result = p.RollbackImage"
	source := string(raw)
	if !observationBarrierOrder(source, start, before, hook, after) {
		t.Fatal("current barrier was not scanned")
	}
	for _, mutated := range []string{"", strings.Replace(source, hook, "Kind: \"omitted\"", 1), strings.Replace(source, before, after, 1)} {
		if observationBarrierOrder(mutated, start, before, hook, after) {
			t.Fatal("missing or misplaced observation escaped")
		}
	}
}
