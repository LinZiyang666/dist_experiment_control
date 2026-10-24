package agent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/filetransfer"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// The recording port owns a real FD and a joined root. It proves the Agent's
// ownership decisions, not sender/receiver/courier semantics from missing leaves.
type agentTransferPort struct {
	file  *os.File
	stop  chan struct{}
	done  chan struct{}
	once  sync.Once
	calls atomic.Int32
}

func newAgentTransferPort(t *testing.T) *agentTransferPort {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "agent-worker-")
	if err != nil {
		t.Fatal(err)
	}
	w := &agentTransferPort{file: f, stop: make(chan struct{}), done: make(chan struct{})}
	go func() { defer close(w.done); <-w.stop }()
	return w
}
func (w *agentTransferPort) Prepare(xfercontract.Prepare, controlbudget.Deadline) (xfercontract.Status, error) {
	return xfercontract.Status{}, errors.New("recording port cannot supply a prepare receipt")
}
func (w *agentTransferPort) Start(xfercontract.Identity) error {
	return errors.New("recording port cannot start byte work")
}
func (w *agentTransferPort) Apply(xfercontract.Control, controlbudget.Deadline) (xfercontract.Status, error) {
	return xfercontract.Status{}, errors.New("recording port cannot supply a control receipt")
}
func (w *agentTransferPort) Status(xfercontract.Identity) (xfercontract.Status, error) {
	return xfercontract.Status{}, errors.New("recording port cannot supply a status receipt")
}
func (w *agentTransferPort) Freeze(controlbudget.Deadline) (xfercontract.WorkerFrozen, error) {
	return nil, errors.New("recording port cannot supply an image receipt")
}
func (w *agentTransferPort) Thaw(xfercontract.Bindings, controlbudget.Deadline) error {
	return errors.New("recording port cannot supply a thaw receipt")
}
func (w *agentTransferPort) Stop(d controlbudget.Deadline) error {
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	w.calls.Add(1)
	var err error
	w.once.Do(func() { close(w.stop); <-w.done; err = w.file.Close() })
	return err
}

func transferPortAgent(t *testing.T, url string) (*Agent, *agentTransferPort, *atomic.Int32) {
	t.Helper()
	w := newAgentTransferPort(t)
	var factories atomic.Int32
	a, err := New(Config{NATSURL: url, SID: "lab", NID: "node", Home: t.TempDir(), RemoteFSMode: "off", Logger: testharness.SilentLog(),
		RegisterTimeout: time.Second, RegisterRetryInitial: time.Millisecond, RegisterRetryMax: time.Millisecond,
		TransferWorker: filetransfer.WorkerDependencies{New: func(cfg xfercontract.WorkerConfig) (xfercontract.WorkerManager, error) {
			if cfg.Identity.SID != "lab" || cfg.Identity.NID != "node" || cfg.Identity.AgentInstance == "" {
				t.Error("worker constructor did not receive actual Agent lineage")
			}
			factories.Add(1)
			return w, nil
		}}})
	if err != nil {
		_ = w.Stop(transferTestDeadline(t))
		t.Fatal(err)
	}
	return a, w, &factories
}
func transferTestDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func assertTransferPortStopped(t *testing.T, w *agentTransferPort) {
	t.Helper()
	if w.calls.Load() != 1 {
		t.Fatalf("original worker Stop calls=%d, want 1", w.calls.Load())
	}
	select {
	case <-w.done:
	default:
		t.Fatal("worker root was not joined")
	}
	if _, err := w.file.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("original worker FD remains open")
	}
}

// origin: hot-update S7 plan §11.3 / T52 / P20
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "Agent transfer lifecycle x5", beforeG)
	testharness.AssertNoFDLeak(t, "Agent transfer lifecycle x5", beforeFD, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	a, w, factories := transferPortAgent(t, "nats://127.0.0.1:1")
	defer func() { _ = w.Stop(transferTestDeadline(t)); stopProcessRuntime(t.Context(), a) }()
	h := transferHandler(a)
	// These are actual session contexts/finalizers. The worker was installed
	// before cancellation so anchoring its Stop to that context turns this red.
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithCancel(t.Context())
		a.setRunCtx(ctx)
		fin := &sessionFinalizer{agent: a, parent: t.Context(), cancel: cancel}
		fin.Do(teardownRebuild)
		if ctx.Err() == nil {
			t.Fatal("fixture did not cancel the old NATS session")
		}
		if transferHandler(a) != h || h.Worker != w || factories.Load() != 1 || w.calls.Load() != 0 {
			t.Fatal("NATS rebuild stopped or replaced the retained worker")
		}
		if _, err := w.file.Stat(); err != nil {
			t.Fatal("NATS rebuild closed an original worker resource")
		}
	}
	var daemonCanceled bool
	handleSysEvent(a, &nats.Msg{Data: []byte(`{"type":"session_destroyed","sid":"other"}`)}, func() { daemonCanceled = true })
	if w.calls.Load() != 0 {
		t.Fatal("foreign session destruction stopped this worker")
	}
	handleSysEvent(a, &nats.Msg{Data: []byte(`{"type":"session_destroyed","sid":"lab"}`)}, func() { daemonCanceled = true })
	assertTransferPortStopped(t, w)
	if daemonCanceled || h.Worker != w {
		t.Fatal("revocation exited the daemon or discarded settlement ownership")
	}
	handleSysEvent(a, &nats.Msg{Data: []byte(`{"type":"session_destroyed","sid":"lab"}`)}, func() { daemonCanceled = true })
	assertTransferPortStopped(t, w)
	if filetransfer.WorkerVersion(h) != "" {
		t.Fatal("revoked worker still advertises new byte admission")
	}
	b, other, _ := transferPortAgent(t, "nats://127.0.0.1:1")
	defer func() { _ = other.Stop(transferTestDeadline(t)); stopProcessRuntime(t.Context(), b) }()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := b.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run cancellation changed: %v", err)
	}
	assertTransferPortStopped(t, other)
}

// origin: hot-update S7 plan §6.2 / §8.4 / P20
func TestTransferCapabilityInstalledByNormalRegister(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferRegister(t)
	}
	testharness.AssertNoGoroutineLeak(t, "normal transfer REGISTER x5", beforeG)
	testharness.AssertNoFDLeak(t, "normal transfer REGISTER x5", beforeFD, 0)
}

func exerciseTransferRegister(t *testing.T) {
	t.Helper()
	s := natstest.RunServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	defer s.WaitForShutdown()
	defer s.Shutdown()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	a, w, factories := transferPortAgent(t, s.ClientURL())
	defer stopProcessRuntime(t.Context(), a)
	defer stopAgentTransfers(a)
	defer func() { _ = w.Stop(transferTestDeadline(t)) }()
	type registration struct {
		proto.NodeRegisterReq
		TransferVersion string `json:"transfer_version"`
	}
	seen := make(chan registration, 8)
	_, err = nc.Subscribe(proto.SubjNodeRegister("lab", "node"), func(msg *nats.Msg) {
		var r registration
		if err := json.Unmarshal(msg.Data, &r); err != nil {
			t.Error(err)
			return
		}
		seen <- r
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true})
		if msg.Reply != "" {
			_ = msg.Respond(body)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := a.register(t.Context(), nc); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-seen:
		if r.TransferVersion != xfercontract.Version || r.ReleasingName || r.RosterRefreshOnly || r.InstanceID != a.instanceID {
			t.Fatalf("normal REGISTER did not install transfer capability: %+v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("normal REGISTER was not observed")
	}
	releaseLeaseName(a, nc)
	select {
	case r := <-seen:
		if r.TransferVersion != "" || !r.ReleasingName || !r.RosterRefreshOnly {
			t.Fatalf("farewell unexpectedly installed capability: %+v", r)
		}
	case <-time.After(time.Second):
		t.Fatal("farewell was not observed")
	}
	if factories.Load() != 1 || w.calls.Load() != 0 {
		t.Fatal("REGISTER replaced or stopped the worker")
	}
	stopAgentTransfers(a)
	if _, err := a.register(t.Context(), nc); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-seen:
		if r.TransferVersion != "" {
			t.Fatal("closed worker advertised transfer capability")
		}
	case <-time.After(time.Second):
		t.Fatal("closed-owner REGISTER was not observed")
	}
}

// origin: hot-update S7 plan §6.4 / P20
func TestTransferRegisterGoneStopsOriginalWorker(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferRegisterGone(t)
	}
	testharness.AssertNoGoroutineLeak(t, "register-gone transfer stop x5", beforeG)
	testharness.AssertNoFDLeak(t, "register-gone transfer stop x5", beforeFD, 0)
}

func exerciseTransferRegisterGone(t *testing.T) {
	t.Helper()
	s := natstest.RunServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	defer s.WaitForShutdown()
	defer s.Shutdown()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	for _, code := range []string{proto.CodeSessionNotFound, proto.CodeSessionNotFoundOrDeleting} {
		a, w, _ := transferPortAgent(t, s.ClientURL())
		func() {
			defer stopProcessRuntime(t.Context(), a)
			defer func() { _ = w.Stop(transferTestDeadline(t)) }()
			h := transferHandler(a)
			sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", "node"), func(msg *nats.Msg) {
				body, _ := json.Marshal(proto.NodeRegisterResp{Code: code})
				_ = msg.Respond(body)
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			if _, err := a.register(t.Context(), nc); !errors.Is(err, errSessionGone) {
				t.Fatalf("register-gone lost authority refusal: %v", err)
			}
			assertTransferPortStopped(t, w)
			if h.Worker != w || filetransfer.WorkerVersion(h) != "" {
				t.Fatal("register-gone lost original ownership or reopened admission")
			}
		}()
	}
}
