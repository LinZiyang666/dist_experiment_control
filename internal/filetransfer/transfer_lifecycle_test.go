package filetransfer

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

// This recording port tests the host's calls and ownership, not the leaf's IO
// state machine. Real reader/writer/worker receipts remain P02/P03/P06–P09 work.
type lifecycleWorker struct {
	mu       sync.Mutex
	prepares []xfercontract.Prepare
	starts   []xfercontract.Identity
	controls []xfercontract.Control
	stops    int
	failStop bool
	file     *os.File
}

func (w *lifecycleWorker) Prepare(p xfercontract.Prepare, _ controlbudget.Deadline) (xfercontract.Status, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.prepares = append(w.prepares, p)
	return xfercontract.Status{State: xfercontract.StatusLive, Phase: xfercontract.PhasePrepared, Identity: p.Identity, Origin: p.Origin, Budget: p.Budget}, nil
}
func (w *lifecycleWorker) Start(id xfercontract.Identity) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.starts = append(w.starts, id)
	return nil
}

// Apply records the directed control; it performs no actor IO. A Grant for an
// entry the host never prepared is refused like the real registry would.
func (w *lifecycleWorker) Apply(c xfercontract.Control, _ controlbudget.Deadline) (xfercontract.Status, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range w.prepares {
		if p.Identity == c.Identity {
			w.controls = append(w.controls, c)
			return xfercontract.Status{State: xfercontract.StatusLive, Phase: xfercontract.PhaseCommitReserved, Identity: p.Identity, Origin: p.Origin, Budget: p.Budget}, nil
		}
	}
	return xfercontract.Status{}, errors.New("xferworker: transfer_intent_conflict")
}
func (w *lifecycleWorker) Status(id xfercontract.Identity) (xfercontract.Status, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, p := range w.prepares {
		if p.Identity == id {
			return xfercontract.Status{State: xfercontract.StatusLive, Phase: xfercontract.PhasePrepared, Identity: id, Origin: p.Origin, Budget: p.Budget}, nil
		}
	}
	return xfercontract.Status{State: xfercontract.StatusUnknown, Identity: id}, nil
}
func (w *lifecycleWorker) Freeze(controlbudget.Deadline) (xfercontract.WorkerFrozen, error) {
	return nil, errors.New("recording port cannot supply an image receipt")
}
func (w *lifecycleWorker) Thaw(xfercontract.Bindings, controlbudget.Deadline) error {
	return errors.New("recording port cannot supply a thaw receipt")
}
func (w *lifecycleWorker) Stop(d controlbudget.Deadline) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, err := controlbudget.Remaining(d); err != nil {
		return err
	}
	w.stops++
	if w.failStop {
		w.failStop = false
		return controlbudget.ErrExpired
	}
	if w.file != nil {
		return w.file.Close()
	}
	return nil
}

// origin: hot-update S7 plan §11.3 / T52 / P20
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "filetransfer worker ownership x5", beforeG)
	testharness.AssertNoFDLeak(t, "filetransfer worker ownership x5", beforeFD, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "worker-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	w := &lifecycleWorker{file: f, failStop: true}
	var owner WorkerOwner
	var constructed atomic.Int32
	deps := WorkerDependencies{New: func(xfercontract.WorkerConfig) (xfercontract.WorkerManager, error) {
		constructed.Add(1)
		return w, nil
	}}
	build := func() *Handler { return &Handler{} }
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			if LoadWorkerHandler(&owner, deps, build).Worker != w {
				t.Error("concurrent load replaced the original owner")
			}
		})
	}
	wg.Wait()
	// Initialization changes the map variable itself. Passing a map value to
	// Purge before taking its mutex races that first assignment, even though
	// the eventual delete is locked. Keep the lookup under the same lock.
	var cache map[string]int
	var cacheMu sync.Mutex
	expired := func(int, time.Time) bool { return false }
	wg.Go(func() {
		for i := 0; i < 500; i++ {
			cacheMu.Lock()
			cache = nil
			cacheMu.Unlock()
			RememberPrepared(&cacheMu, &cache, "original", i, time.Now(), expired)
		}
	})
	wg.Go(func() {
		for i := 0; i < 500; i++ {
			PurgePrepared(&cacheMu, &cache, "original")
		}
	})
	wg.Wait()
	h := LoadWorkerHandler(&owner, deps, build)
	if constructed.Load() != 1 || WorkerVersion(h) != xfercontract.Version {
		t.Fatal("persistent owner was not installed exactly once")
	}
	if !errors.Is(StopWorker(h), controlbudget.ErrExpired) {
		t.Fatal("stop timeout was hidden")
	}
	if h.Worker != w || WorkerVersion(h) != "" {
		t.Fatal("failed stop lost its owner or reopened new admission")
	}
	if _, err := f.Stat(); err != nil {
		t.Fatal("failed stop closed the retained resource")
	}
	if err := StopWorker(h); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("successful stop did not close the original resource")
	}
	if err := StopWorker(h); err != nil || w.stops != 2 {
		t.Fatalf("terminal stop was repeated: calls=%d err=%v", w.stops, err)
	}
	var partial WorkerOwner
	h = LoadWorkerHandler(&partial, WorkerDependencies{New: func(xfercontract.WorkerConfig) (xfercontract.WorkerManager, error) {
		return &lifecycleWorker{}, errors.New("constructor failed after taking ownership")
	}}, build)
	if WorkerVersion(h) != "" || h.Worker == nil || h.WorkerError == nil {
		t.Fatal("partial constructor advertised success or discarded ownership")
	}
	if err := StopWorker(h); err != nil {
		t.Fatal(err)
	}
	var empty WorkerOwner
	h = LoadWorkerHandler(&empty, WorkerDependencies{New: func(xfercontract.WorkerConfig) (xfercontract.WorkerManager, error) {
		return nil, nil
	}}, build)
	if WorkerVersion(h) != "" || h.WorkerError == nil {
		t.Fatal("nil owner with nil error was accepted")
	}
}

// origin: hot-update S7 plan §6.2 / §7 / P20
func TestTransferContinuationDelegatesWithoutLegacyEffects(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferContinuation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "continued transfer adapter x5", beforeG)
	testharness.AssertNoFDLeak(t, "continued transfer adapter x5", beforeFD, 0)
}

func exerciseTransferContinuation(t *testing.T) {
	t.Helper()
	s := natstest.RunServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	defer s.WaitForShutdown()
	defer s.Shutdown()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	h := &Handler{Worker: &lifecycleWorker{}, Logger: testharness.SilentLog(), SID: "lab", NID: func() string { return "node" }, Instance: func() string { return "agent-original" }}
	h.ports = newWorkerPorts(h)
	// Any reachability to a legacy side effect is an immediate failure. This
	// includes a legacy admission refusal, not merely a successful SDK Put.
	h.Admit = func() (func(), bool) { t.Error("v1 reached legacy admission"); return func() {}, false }
	origin := xfercontract.Origin{BrokerID: "origin", InstanceID: "broker-original", BootID: "boot", Generation: 1}
	budget := xfercontract.BudgetSeal{StartedAt: time.Now().UTC().Add(-time.Minute), Size: 3, RateLimitBPS: 128 << 10, EffectiveBudgetNS: int64(5 * time.Minute), RemainingNS: int64(4 * time.Minute), SourceSealed: true}
	path := filepath.Join(t.TempDir(), "destination")
	if err := os.WriteFile(path, []byte("old complete destination"), 0600); err != nil {
		t.Fatal(err)
	}
	// The broker-directed carrier is the real P01 codec; this test builds it
	// with the same encoder the deciding broker uses.
	// origin: hot-update S7 plan §6.3 / INTEG-D agent wiring
	prepare := func(transferID, version string, intent xfercontract.Intent, inline []byte) (xfercontract.Identity, []byte) {
		t.Helper()
		digest, err := proto.TransferIntentDigest(proto.Intent(intent))
		if err != nil {
			t.Fatal(err)
		}
		id := xfercontract.Identity{SID: "lab", NID: "node", AgentInstance: "agent-original", ActorFP: "actor", TransferID: transferID, EntryNonce: "entry", IntentDigest: digest, ObjectVersion: version}
		now := time.Now().UTC()
		permit := xfercontract.CreationPermit{Identity: id, Origin: origin, IssuedAt: now, CreateUntil: now.Add(time.Minute), PermitNonce: "permit", Signature: "signed"}
		_, body, err := proto.EncodeAgentTransferCarrier(xfercontract.Request{Op: xfercontract.OpPrepare, CallID: "prepare-" + transferID,
			Identity: id, Origin: origin, Intent: &intent, Permit: &permit, Inline: inline}, budget, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		return id, body
	}
	call := func(verb string, op xfercontract.Operation, body []byte, want xfercontract.Identity) {
		t.Helper()
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
		msg := &nats.Msg{Reply: inbox, Data: body}
		switch verb {
		case "push":
			h.HandlePushForwarded(nc, msg)
		case "push-commit":
			h.HandlePushCommitForwarded(nc, msg)
		case "pull":
			h.HandlePullForwarded(nc, msg)
		default:
			t.Fatal("bad test verb")
		}
		got, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		reply, err := proto.DecodeTransferResponse(op, verb, got.Data)
		if err != nil || reply.Code != "" || reply.Status == nil {
			t.Fatalf("capable adapter refused: %s %v", got.Data, err)
		}
		// The prepare reply's top-level object version was checked by the codec.
		if reply.Status.Identity != want || reply.Status.Origin != origin {
			t.Fatalf("reply lost the retained identity: %s", got.Data)
		}
		if reply.Settlement != nil || reply.Source != nil {
			t.Fatal("agent reply minted prepare-only authority")
		}
	}
	intentB := xfercontract.Intent{Verb: "push", Tier: "b", Path: path, SHA256: hexSHA256([]byte("new")), Size: 3, Force: true, RateLimitBPS: 128 << 10, ChunkSize: 512 << 10}
	idB, bodyB := prepare("transfer", "fixed-version", intentB, nil)
	h.HandlePushForwarded(nc, &nats.Msg{Data: bodyB})
	if w := h.Worker.(*lifecycleWorker); len(w.prepares) != 0 || len(w.starts) != 0 {
		t.Fatal("request without a reply entered worker side effects")
	}
	call("push", xfercontract.OpPrepare, bodyB, idB)
	// The original path can disappear after prepare; replay must query the
	// retained entry before path validation and never reopen a new destination.
	if err := os.RemoveAll(filepath.Dir(path)); err != nil {
		t.Fatal(err)
	}
	call("push", xfercontract.OpPrepare, bodyB, idB)
	grant := xfercontract.CommitGrant{Identity: idB, GrantNonce: "grant", ReceiverInstance: "agent-original", TerminalID: "terminal"}
	source := xfercontract.SourceSeal{Size: 3, SHA256: intentB.SHA256, ChunkSize: 512 << 10, Chunks: 1, RateLimitBPS: 128 << 10, SourceFingerprint: "pinned"}
	_, commit, err := proto.EncodeAgentTransferCarrier(xfercontract.Request{Op: xfercontract.OpCommit, CallID: "commit-original", Identity: idB, Origin: origin}, budget, &source, &grant)
	if err != nil {
		t.Fatal(err)
	}
	call("push-commit", xfercontract.OpCommit, commit, idB)
	inlinePath := filepath.Join(t.TempDir(), "destination")
	if err := os.WriteFile(inlinePath, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	intentA := xfercontract.Intent{Verb: "push", Tier: "a", Path: inlinePath, SHA256: hexSHA256([]byte("new")), Size: 3, Force: true, RateLimitBPS: 128 << 10}
	idA, bodyA := prepare("inline-transfer", "", intentA, []byte("new"))
	call("push", xfercontract.OpPrepare, bodyA, idA)
	intentPull := xfercontract.Intent{Verb: "pull", Tier: "b", Path: inlinePath, Force: true, RateLimitBPS: 128 << 10, ChunkSize: 512 << 10}
	idP, bodyP := prepare("pull-transfer", "fixed-pull-version", intentPull, nil)
	call("pull", xfercontract.OpPrepare, bodyP, idP)
	w := h.Worker.(*lifecycleWorker)
	// Receivers start only with the commit carrier's sealed geometry and Grant;
	// a pull's sender starts at prepare.
	if len(w.prepares) != 4 || len(w.starts) != 2 || len(w.controls) != 1 || w.stops != 0 {
		t.Fatalf("adapter calls changed: prepares=%d starts=%d controls=%d stops=%d", len(w.prepares), len(w.starts), len(w.controls), w.stops)
	}
	if c := w.controls[0]; c.Kind != xfercontract.ControlGrant || c.Grant == nil || *c.Grant != grant || w.starts[0] != idB || w.starts[1] != idP {
		t.Fatalf("commit did not deliver exactly the durable grant to the original receiver: %+v", w.controls)
	}
	for i, p := range w.prepares {
		expectedID := []string{"transfer", "transfer", "inline-transfer", "pull-transfer"}[i]
		if p.Identity.TransferID != expectedID || p.Budget != budget || p.Origin != origin || p.Intent.RateLimitBPS != 128<<10 {
			t.Fatal("prepare changed the original identity, origin, rate or deadline")
		}
		if (i < 2 || i == 3) && p.Intent.ChunkSize != 512<<10 {
			t.Fatal("prepare dropped the frozen chunk geometry")
		}
	}
	if string(w.prepares[2].Inline) != "new" {
		t.Fatal("inline prepare lost its authenticated bytes")
	}
	if plan, ok := h.ports.plan(idB); !ok || plan.source == nil || *plan.source != source {
		t.Fatal("commit carrier lost the sealed receiver geometry")
	}
	if body, err := os.ReadFile(inlinePath); err != nil || string(body) != "old" {
		t.Fatal("capable adapter changed the inline destination before receiver work")
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("capable adapter performed a legacy filesystem effect")
	}
	if err := StopWorker(h); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §6.1 / §6.2 / P20
func TestTransferContinuationRejectsUnavailableOrMismatchedOwner(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferRefusals(t)
	}
	testharness.AssertNoGoroutineLeak(t, "continued transfer refusals x5", beforeG)
	testharness.AssertNoFDLeak(t, "continued transfer refusals x5", beforeFD, 0)
}

func exerciseTransferRefusals(t *testing.T) {
	t.Helper()
	s := natstest.RunServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	defer s.WaitForShutdown()
	defer s.Shutdown()
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	for _, arm := range []string{"missing-worker", "malformed", "wrong-sid", "wrong-instance", "wrong-actor", "wrong-permit", "wrong-origin", "wrong-version", "no-call-id", "closed-owner", "changed-intent", "changed-rate", "changed-geometry", "disabled-mode"} {
		w := &lifecycleWorker{}
		h := &Handler{Worker: w, Logger: testharness.SilentLog(), SID: "lab", NID: func() string { return "node" }, Instance: func() string { return "instance" }}
		h.ports = newWorkerPorts(h)
		h.Admit = func() (func(), bool) { t.Error("refused v1 reached legacy admission"); return func() {}, false }
		// Start from a valid directed carrier and corrupt exactly one fact, so
		// every refusal is attributable to that fact rather than a bad fixture.
		// origin: hot-update S7 plan §6.1 / §6.3 / INTEG-D agent wiring
		intent := xfercontract.Intent{Verb: "push", Tier: "b", Path: filepath.Join(t.TempDir(), "destination"), SHA256: hexSHA256([]byte("new")), Size: 3, ChunkSize: 512 << 10}
		digest, err := proto.TransferIntentDigest(proto.Intent(intent))
		if err != nil {
			t.Fatal(err)
		}
		id := xfercontract.Identity{SID: "lab", NID: "node", AgentInstance: "instance", ActorFP: "actor", TransferID: "id", EntryNonce: "entry", IntentDigest: digest, ObjectVersion: "version"}
		origin := xfercontract.Origin{BrokerID: "origin", InstanceID: "broker", BootID: "boot", Generation: 1}
		now := time.Now().UTC()
		permit := xfercontract.CreationPermit{Identity: id, Origin: origin, IssuedAt: now, CreateUntil: now.Add(time.Minute), PermitNonce: "permit", Signature: "signed"}
		budget := xfercontract.BudgetSeal{StartedAt: now, Size: 3, EffectiveBudgetNS: int64(5 * time.Minute), RemainingNS: int64(4 * time.Minute)}
		_, valid, err := proto.EncodeAgentTransferCarrier(xfercontract.Request{Op: xfercontract.OpPrepare, CallID: "call", Identity: id, Origin: origin, Intent: &intent, Permit: &permit}, budget, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		var req map[string]any
		if err := json.Unmarshal(valid, &req); err != nil {
			t.Fatal(err)
		}
		c := req["continuation"].(map[string]any)
		identity := c["identity"].(map[string]any)
		permitIdentity := c["permit"].(map[string]any)["identity"].(map[string]any)
		switch arm {
		case "missing-worker":
			h.Worker = nil
		case "malformed":
			req["continuation"] = "invalid"
		case "wrong-sid":
			identity["sid"], permitIdentity["sid"] = "other", "other"
		case "wrong-instance":
			identity["agent_instance"], permitIdentity["agent_instance"] = "other", "other"
		case "wrong-actor":
			req["actor_fp"] = "other"
		case "wrong-permit":
			permitIdentity["transfer_id"] = "other"
		case "wrong-origin":
			c["permit"].(map[string]any)["origin"].(map[string]any)["broker_id"] = "other"
		case "wrong-version":
			c["version"] = "future"
		case "no-call-id":
			delete(req, "call_id")
		case "closed-owner":
			if err := StopWorker(h); err != nil {
				t.Fatal(err)
			}
		case "disabled-mode":
			h.Mode = 2
		case "changed-rate":
			c["intent"].(map[string]any)["rate_limit_bps"] = 1
		case "changed-geometry":
			req["chunk_size"] = 1
		case "changed-intent":
			req["path"] = filepath.Join(t.TempDir(), "destination")
		}
		body, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatal(err)
		}
		h.HandlePushForwarded(nc, &nats.Msg{Reply: inbox, Data: body})
		got, err := sub.NextMsg(time.Second)
		_ = sub.Unsubscribe()
		if err != nil {
			t.Fatal(err)
		}
		var resp struct {
			OK   bool
			Code string
		}
		if err := json.Unmarshal(got.Data, &resp); err != nil {
			t.Fatal(err)
		}
		if arm == "disabled-mode" && resp.Code != "transfer_disabled" {
			t.Fatalf("path policy refusal lost its code: %s", got.Data)
		}
		if resp.OK || resp.Code == "" || len(w.prepares) != 0 || len(w.starts) != 0 {
			t.Fatalf("%s accepted or entered a worker side effect: %s", arm, got.Data)
		}
	}
}
