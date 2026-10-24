//go:build linux

// Real replay files and runtime admission/freeze/import. Spawn is deliberately
// refused by the adapter: failed handles still own empty replay reservations.
// Flow lifecycle uses real sockets, replay files, and frozen image APIs.
package execsession

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S5 plan §3.4 / T30
func TestSharedReplayBudgetCountsOwnedDomains(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	t.Run("runtime", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseSharedRuntimeBudget(t)
		}
		testharness.AssertNoGoroutineLeak(t, "shared runtime budget", goroutines)
		testharness.AssertNoFDLeak(t, "shared runtime budget", fds, 0)
	})
	t.Run("flow", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseSharedFlowBudget(t)
		}
		testharness.AssertNoGoroutineLeak(t, "shared flow budget", goroutines)
		testharness.AssertNoFDLeak(t, "shared flow budget", fds, 0)
	})
	t.Run("active-flow", func(t *testing.T) {
		// origin: hot-update S5 PROC-LEAK / final process census.
		testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

		goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
		for round := 0; round < 5; round++ {
			exerciseSharedActiveFlowBudget(t)
		}
		testharness.AssertNoGoroutineLeak(t, "exec PTY active flow budget", goroutines)
		testharness.AssertNoFDLeak(t, "exec PTY active flow budget", fds, 0)
	})
}

// All three domains are nonempty and share one real reservation owner. The
// original target remains alive while ACK, image import, and domain cleanup
// independently account for capacity rather than retained byte counts.
// origin: hot-update S5 plan §3.4 / T30
func exerciseSharedActiveFlowBudget(t *testing.T) {
	const capacity = workload.DefaultReplayCapacity
	const runtimeOwned = 3*capacity + upgradebudget.PTYInputBytes
	budget := &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	f, runtimeCfg := sharedBudgetRuntime(t, budget)
	defer f.close(t)
	sharedBudgetFailedTasks(t, f)
	if SharedReplayBudget(f.r) != budget {
		t.Fatal("runtime did not expose the node reservation owner")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	deadline := time.Now().Add(runtimeTestBudget)
	if err := listener.(*net.TCPListener).SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().(*net.TCPAddr).AddrPort()
	socket, err := rawsocket.Connect(address, deadline, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = socket.Close() }()
	if err := socket.CompleteConnect(deadline, nil); err != nil {
		t.Fatal(err)
	}
	peer, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = peer.Close() }()
	if err := peer.SetDeadline(deadline); err != nil {
		t.Fatal(err)
	}
	flowCfg := flow.Config{Budget: SharedReplayBudget(f.r), MaxFlows: flow.MaxFlows, MaxPendingFDs: flow.MaxPendingFDs}
	manager, err := flow.New(flowCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Close() }()
	request := flow.OpenRequest{ID: flow.ID{1}, EndpointID: flow.EndpointID{2}, OpenSeq: 1, TargetPort: address.Port(), OwnerGen: 1, CarrierGen: 1, Deadline: deadline}
	request.Intent = flow.OpenIntent(request.EndpointID, request.ID, request.OpenSeq, request.TargetPort)
	if _, err := flow.Open(manager, request, socket); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != runtimeOwned+capacity || f.r.ReplayBudgetUsed() != runtimeOwned {
		t.Fatal("empty active flow failed to reserve against exec and PTY")
	}
	payload := []byte("original-flow-source")
	if _, err := peer.Write(payload); err != nil {
		t.Fatal(err)
	}
	f.waitFor(t, "flow source bytes retained", func() bool {
		state, _, err := flow.Observe(manager, request.ID)
		return err == nil && state.TxEnd == uint64(len(payload))
	})
	frame, _, err := flow.NextData(manager, request.ID, request.CarrierGen, 0)
	if err != nil || !bytes.Equal(frame.Body, payload) {
		t.Fatal("nonempty flow replay precondition failed")
	}
	if err := flow.Receive(manager, flow.Frame{Version: flow.Version, Kind: flow.KindACK, Flags: flow.FlagDirection, FlowID: request.ID, CarrierGen: request.CarrierGen, Offset: uint64(len(payload))}); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != runtimeOwned+capacity {
		t.Fatal("flow ACK released capacity still owned by the domain")
	}
	runtimeImage, runtimeResources, rows := imageCut(t, f)
	frozen, err := manager.Freeze(deadline)
	if err != nil {
		t.Fatal(err)
	}
	flowImage, snapshot, err := frozen.Export()
	if err != nil {
		t.Fatalf("real active flow image export: %v", err)
	}
	defer func() { _ = snapshot.Close() }()
	if len(flowImage.Records) != 1 || flowImage.Records[0].Phase != flow.FlowPhaseActive || flowImage.ReservedBytes != capacity || runtimeImage.BudgetUsed != runtimeOwned || flowImage.Records[0].TxBase != uint64(len(payload)) || flowImage.Records[0].TxBase != flowImage.Records[0].TxEnd {
		t.Fatal("ACKed replay capacity was lost or domain image counted node Used")
	}
	node := &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	foreign, err := workload.NewReplay(node, capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = foreign.Close() }()
	runtimeCfg.SharedReplayBudget = node
	importedRuntime, salvage, err := Import(runtimeCfg, runtimeImage, rows, newImageDupSource(t, runtimeResources))
	if err != nil || importedRuntime == nil || len(salvage) != 0 {
		t.Fatalf("all-domain runtime import: %v", err)
	}
	defer func() {
		resources, _ := importedRuntime.Detach()
		for _, resource := range resources {
			for _, file := range resource.Files {
				_ = file.Close()
			}
		}
	}()
	flowCfg.Budget = SharedReplayBudget(importedRuntime)
	importedFlow, flowSalvage, err := flow.Restore(flowCfg, flowImage, newImageDupSource(t, snapshot.Resources))
	if err != nil || importedFlow == nil || len(flowSalvage) != 0 {
		t.Fatalf("all-domain flow import: %v", err)
	}
	defer func() { _ = importedFlow.Close() }()
	if node.Used() != runtimeOwned+2*capacity || importedRuntime.ReplayBudgetUsed() != runtimeOwned {
		t.Fatal("imported exec PTY flow reservations did not share node accounting")
	}
	if err := importedFlow.Close(); err != nil || node.Used() != runtimeOwned+capacity {
		t.Fatal("flow cleanup released another domain or retained capacity")
	}
	if err := frozen.Discard(); err != nil || budget.Used() != runtimeOwned {
		t.Fatal("old flow discard released exec or PTY capacity")
	}
	if err := f.r.Discard(); err != nil || budget.Used() != 0 {
		t.Fatal("old runtime discard retained capacity")
	}
	f.manager.Thaw()
}

func sharedBudgetRuntime(t *testing.T, budget *workload.ReplayBudget) (*runtimeFixture, Config) {
	t.Helper()
	f, cfg := runtimeFixtureConfig(t, workload.DefaultReplayCapacity)
	cfg.SharedReplayBudget = budget
	// The supplied pointer is authoritative even if a standalone legacy limit
	// disagrees. Neither New nor Import may overwrite its node-wide Limit.
	cfg.ReplayBudget = 1
	cfg.Start = func(string, *proto.ExecReq) (*workload.Stdio, string, error) {
		return nil, "", errors.New("injected spawn refusal")
	}
	cfg.StartPTY = func(string, *proto.RunReq) (*pty.Session, string, error) {
		return nil, "", errors.New("injected PTY spawn refusal")
	}
	cfg.PTYStarted = func(string, proto.RunReq, workload.Identity) {}
	var err error
	f.r, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return f, cfg
}

func sharedBudgetFailedTasks(t *testing.T, f *runtimeFixture) {
	t.Helper()
	handle := f.lease(t, "exec-reservation")
	execReply := f.create(t, handle, proto.ExecReq{Argv: []string{"/bin/false"}})
	ptyHandle := f.lease(t, "pty-reservation")
	ptyReply, err := f.request(proto.ProcessResumeReq{Op: "create", Kind: proto.ProcessKindPTY,
		Handle: &ptyHandle, AttachEpoch: 1, Run: &proto.RunReq{Argv: []string{"/bin/false"}}}, "_TINBOX.actor.pty")
	if err != nil {
		t.Fatal(err)
	}
	for _, pid := range []string{execReply.PID, ptyReply.PID} {
		f.r.mu.Lock()
		owner := f.r.tasks[pid]
		f.r.mu.Unlock()
		if owner == nil {
			t.Fatal("failed spawn lost its handle owner")
		}
		select {
		case <-owner.done:
		case <-time.After(runtimeTestBudget):
			t.Fatal("failed spawn did not publish its terminal")
		}
	}
}

func exerciseSharedRuntimeBudget(t *testing.T) {
	t.Helper()
	const capacity = workload.DefaultReplayCapacity
	const owned = 3*capacity + upgradebudget.PTYInputBytes // exec outputs plus PTY input/output
	budget := &workload.ReplayBudget{Limit: 8 * capacity}
	foreign, err := workload.NewReplay(budget, capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = foreign.Close() }()
	f, cfg := sharedBudgetRuntime(t, budget)
	defer f.close(t)
	// Observe all three allocation sites before failure retires stdin. An
	// unshared stdin allocation can disappear before final ownership census.
	probe := &task{}
	defer closeTask(probe)
	if err := f.r.allocate(probe, []byte("reserved input")); err != nil {
		t.Fatal(err)
	}
	if budget.Used() != 3*capacity+64<<10 {
		t.Fatalf("exec allocation did not share stdin/output budget: node=%d", budget.Used())
	}
	closeTask(probe)
	if budget.Used() != capacity {
		t.Fatal("allocation cleanup released another domain")
	}
	sharedBudgetFailedTasks(t, f)
	if f.r.sharedReplayBudget != budget || budget.Limit != 8*capacity {
		t.Fatal("runtime copied the budget pointer or overwrote its Limit")
	}
	if got := f.r.ReplayBudgetUsed(); got != owned || budget.Used() != owned+capacity {
		t.Fatalf("runtime counted global Used instead of owned domains: runtime=%d node=%d", got, budget.Used())
	}
	img, resources, rows := imageCut(t, f)
	if len(img.Tasks) != 2 || img.BudgetUsed != owned || len(rows) != 0 || len(resources) != 2 {
		t.Fatalf("image counted global Used or lost empty logs: tasks=%d budget=%d rows=%d resources=%d", len(img.Tasks), img.BudgetUsed, len(rows), len(resources))
	}
	for _, ti := range img.Tasks {
		for _, stream := range []*StreamImage{ti.Stdin, ti.Stdout, ti.Stderr} {
			if stream != nil && stream.Replay.End != stream.Replay.Base {
				t.Fatal("fixture expected empty but fully reserved logs")
			}
		}
	}
	// Model the destination node: another domain has already charged it.
	// The exporter keeps a different process-local budget until Discard.
	node := &workload.ReplayBudget{Limit: 6 * capacity}
	other, err := workload.NewReplay(node, capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	cfg.SharedReplayBudget = node
	img.BudgetLimit = 1 << 40 // exporter metadata cannot increase node capacity
	imported, salvage, err := Import(cfg, img, rows, newImageDupSource(t, resources))
	if err != nil || imported == nil || len(salvage) != 0 {
		t.Fatalf("import with existing foreign reservation: err=%v salvage=%d", err, len(salvage))
	}
	if imported.sharedReplayBudget != node || imported.ReplayBudgetUsed() != owned || node.Used() != owned+capacity || node.Limit != 6*capacity {
		t.Fatal("import copied budget, counted global Used, or adopted exporter Limit")
	}
	// A second import must fail before taking descriptors when foreign+local
	// ownership leaves insufficient capacity; no failed import may charge it.
	blockedSource := newImageDupSource(t, resources)
	before := node.Used()
	bad, salvage, err := Import(cfg, img, rows, blockedSource)
	if bad != nil || !errors.Is(err, ErrImage) || len(salvage) != 0 || node.Used() != before {
		t.Fatalf("saturated import changed ownership: err=%v salvage=%d node=%d", err, len(salvage), node.Used())
	}
	for _, files := range blockedSource.files {
		for _, file := range files {
			imageFileOpen(t, "untaken saturated import", file)
		}
	}
	blockedSource.Close()
	back, err := imported.Detach()
	if err != nil || len(back) != 2 || node.Used() != capacity || imported.ReplayBudgetUsed() != 0 {
		t.Fatalf("detach released another domain or retained capacity: err=%v node=%d", err, node.Used())
	}
	for _, res := range back {
		for _, file := range res.Files {
			_ = file.Close()
		}
	}
	if err := f.r.Discard(); err != nil || budget.Used() != capacity || f.r.ReplayBudgetUsed() != 0 {
		t.Fatalf("discard failed to release owned capacity: err=%v node=%d", err, budget.Used())
	}
	f.manager.Thaw()
}

// An idle manager owns zero replay capacity even when other domains already
// reserve against its node. Active admission is exercised separately above.
func exerciseSharedFlowBudget(t *testing.T) {
	t.Helper()
	budget := &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	foreign, err := workload.NewReplay(budget, workload.DefaultReplayCapacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = foreign.Close() }()
	cfg := flow.Config{Budget: budget, MaxFlows: flow.MaxFlows, MaxPendingFDs: flow.MaxPendingFDs}
	manager, err := flow.New(cfg)
	if err != nil {
		t.Fatalf("construct real flow owner: %v", err)
	}
	defer func() { _ = manager.Close() }()
	frozen, err := manager.Freeze(time.Now().Add(runtimeTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	img, snapshot, err := frozen.Export()
	if err != nil {
		t.Fatalf("real idle flow image export: %v", err)
	}
	if snapshot == nil || img.ReservedBytes != 0 || len(img.Records) != 0 || budget.Used() != workload.DefaultReplayCapacity {
		t.Fatal("idle flow charged foreign or empty domain capacity")
	}
	defer func() { _ = snapshot.Close() }()
	src := newImageDupSource(t, snapshot.Resources)
	restored, salvage, err := flow.Restore(cfg, img, src)
	if err != nil || restored == nil || len(salvage) != 0 {
		t.Fatalf("real idle flow image restore: err=%v salvage=%d", err, len(salvage))
	}
	defer func() { _ = restored.Close() }()
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := restored.ActivateRaw(); err != nil || budget.Used() != workload.DefaultReplayCapacity {
		t.Fatalf("idle flow import counted foreign capacity: err=%v node=%d", err, budget.Used())
	}
}

// sharedBudgetTakeFault forces capacity to change after pure validation but
// before construction, exercising real partial-reservation salvage.
type sharedBudgetTakeFault struct {
	*imageDupSource
	budget *workload.ReplayBudget
	block  *workload.Replay
}

func (s *sharedBudgetTakeFault) TakeFile(id, name string) (*os.File, error) {
	if s.block == nil {
		var err error
		s.block, err = workload.NewReplay(s.budget, workload.DefaultReplayCapacity)
		if err != nil {
			return nil, fmt.Errorf("inject competing domain reservation: %w", err)
		}
	}
	return s.imageDupSource.TakeFile(id, name)
}

// origin: hot-update S5 plan §3.4 / T30
func TestSharedReplayImportFailureReturnsOwnedReservations(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSharedImportFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "shared budget import salvage", goroutines)
	testharness.AssertNoFDLeak(t, "shared budget import salvage", fds, 0)
}

func exerciseSharedImportFailure(t *testing.T) {
	t.Helper()
	const capacity = workload.DefaultReplayCapacity
	f, cfg := sharedBudgetRuntime(t, &workload.ReplayBudget{Limit: 6 * capacity})
	defer f.close(t)
	sharedBudgetFailedTasks(t, f)
	img, resources, rows := imageCut(t, f)
	node := &workload.ReplayBudget{Limit: img.BudgetUsed}
	cfg.SharedReplayBudget = node
	src := &sharedBudgetTakeFault{imageDupSource: newImageDupSource(t, resources), budget: node}
	defer func() {
		if src.block != nil {
			_ = src.block.Close()
		}
	}()
	bad, salvage, err := Import(cfg, img, rows, src)
	if bad != nil || err == nil || len(salvage) != 2 || src.block == nil || node.Used() != capacity {
		t.Fatalf("partial import leaked reservation: err=%v salvage=%d node=%d", err, len(salvage), node.Used())
	}
	count := 0
	for _, res := range salvage {
		if res.Kind != resourceKind && res.Kind != ptyResourceKind {
			t.Fatalf("salvage changed resource kind: %s", res.Kind)
		}
		for _, file := range res.Files {
			imageFileOpen(t, "partial import salvage", file)
			_ = file.Close()
			count++
		}
	}
	if count != 4 {
		t.Fatalf("partial import lost descriptor: got=%d want=4", count)
	}
	if err := f.r.Discard(); err != nil {
		t.Fatal(err)
	}
	f.manager.Thaw()
}

// origin: hot-update S5 plan §3.4 / T30
func TestSharedReplayBudgetRejectsInvalidLimit(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	_, cfg := runtimeFixtureConfig(t, workload.DefaultReplayCapacity)
	cfg.SharedReplayBudget = &workload.ReplayBudget{Limit: -1}
	if r, err := New(cfg); r != nil || err == nil || !strings.Contains(err.Error(), "invalid replay capacity or budget") {
		t.Fatal("New accepted a negative shared Limit")
	}
	if r, salvage, err := Import(cfg, Image{}, nil, nil); r != nil || len(salvage) != 0 || err == nil || !strings.Contains(err.Error(), "invalid replay capacity or budget") {
		t.Fatal("Import accepted a negative shared Limit")
	}
}

var _ FileSource = (*sharedBudgetTakeFault)(nil)

// origin: hot-update S5 plan §3.4 / T30
func TestSharedReplayLiveExecKeepsNodeBudget(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSharedLiveExec(t)
	}
	testharness.AssertNoGoroutineLeak(t, "live exec shared replay", goroutines)
	testharness.AssertNoFDLeak(t, "live exec shared replay", fds, 0)
}

func exerciseSharedLiveExec(t *testing.T) {
	t.Helper()
	const capacity = workload.DefaultReplayCapacity
	const owned = 2*capacity + 64<<10
	oldNode := &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	f, cfg := runtimeFixtureConfig(t, capacity)
	cfg.SharedReplayBudget = oldNode
	var err error
	f.r, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer f.close(t)
	gate := imageControlPipe(t, f)
	defer func() { _ = f.control.Close(); _ = gate.Close() }()
	h := f.lease(t, "live-budget")
	created := f.create(t, h, proto.ExecReq{Argv: []string{"/bin/sh", "-c", "read -r line <&3; printf '%s' \"$line\""}, Stdin: []byte("input")})
	f.waitFor(t, "real child running before cut", func() bool {
		f.r.mu.Lock()
		defer f.r.mu.Unlock()
		rec, lookupErr := f.r.registry.Lookup(f.scope, h)
		return lookupErr == nil && rec.Phase == "running"
	})
	img, resources, rows := imageCut(t, f)
	if len(img.Tasks) != 1 || img.Tasks[0].Stdin == nil || img.BudgetUsed != owned || oldNode.Used() != owned || len(rows) != 1 {
		t.Fatal("live exec cut did not retain all three stream reservations")
	}
	node := &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	foreign, err := workload.NewReplay(node, capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = foreign.Close() }()
	cfg.SharedReplayBudget = node
	imported, salvage, err := Import(cfg, img, rows, newImageDupSource(t, resources))
	if err != nil || imported == nil || len(salvage) != 0 {
		t.Fatalf("live exec import: err=%v salvage=%d", err, len(salvage))
	}
	if imported.ReplayBudgetUsed() != owned || node.Used() != owned+capacity {
		t.Fatal("live exec import did not share its stdin reservation")
	}
	if err := f.r.Discard(); err != nil {
		t.Fatal(err)
	}
	f.r = imported
	f.manager.Thaw()
	if err := imported.Thaw(); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.WriteString("same-child\n"); err != nil {
		t.Fatal(err)
	}
	client := &runtimeClient{handle: h, epoch: 1, inbox: "_TINBOX.actor.original"}
	f.drain(t, client)
	if string(client.stdout) != "same-child" || client.terminal.ExitCode != 0 {
		t.Fatal("original child did not continue after import")
	}
	f.requireLifecycle(t, created.PID, 0)
	f.settle(t, client)
	if node.Used() != capacity || oldNode.Used() != 0 || imported.ReplayBudgetUsed() != 0 {
		t.Fatal("settlement released foreign capacity or leaked live exec capacity")
	}
}

// origin: hot-update S5 plan §3.4 / §9.1 / DRILL-OBSERVER
func TestRuntimeObservationCountsLiveOwnedReservations(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRuntimeObservation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "runtime custody census", g)
	testharness.AssertNoFDLeak(t, "runtime custody census", fd, 0)
}
func exerciseRuntimeObservation(t *testing.T) {
	t.Helper()
	log, err := workload.NewReplay(&workload.ReplayBudget{}, 256)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := log.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if _, err := log.Append([]byte("actual replay bytes")); err != nil {
		t.Fatal(err)
	}
	prior := drillobserve.Hook
	var events []drillobserve.Event
	drillobserve.Hook = func(e drillobserve.Event) { events = append(events, e) }
	defer func() { drillobserve.Hook = prior }()
	r := &Runtime{tasks: map[string]*task{"synthetic-owner": {logs: [3]*workload.Replay{log}}}}
	ObserveRuntime(r)
	if len(events) != 3 || events[0].Kind != "domain-file" || events[0].Values[1] == 0 || events[1].Domain != "exec" || events[1].Values[2] != 256 || events[1].Values[3] != 19 {
		t.Fatal("live descriptor/reservation census missing")
	}
	if err := log.Ack(19); err != nil {
		t.Fatal(err)
	}
	events = nil
	ObserveRuntime(r)
	if events[1].Values[2] != 256 || events[1].Values[3] != 0 {
		t.Fatal("ACK was mistaken for reservation release")
	}
	if err := log.Close(); err != nil {
		t.Fatal(err)
	}
	events = nil
	ObserveRuntime(r)
	if len(events) != 2 || events[0].Values[2] != 0 {
		t.Fatal("closed log remained in live reservation census")
	}
}
