//go:build linux

// bundle_test.go — the composed cut in one process: four domains frozen in
// the mandated order, encoded, decoded strictly, imported over the shared
// process table, thawed, and every refusal leaving nothing installed.
// Real: exec.Cmd children, pipes, memfd replays, a spawnsafe policy with a
// fake mount table. Stubbed: the courier (an in-memory exporter/importer
// that records WHEN it was asked) and the exec (bundle_exec_test.go).
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §5.1.
package agentimage

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

const bundleTestBudget = 10 * time.Second

// bundleFixture owns a runtime over real children and the shared table.
type bundleFixture struct {
	manager *workload.Manager
	cfg     execsession.Config
	rt      *execsession.Runtime
	scope   procrequest.Scope
	calls   uint64
	mu      sync.Mutex
	exited  map[string]int
	spawned int
}

func newBundleFixture(t *testing.T, instance string) *bundleFixture {
	t.Helper()
	f := newBundleConfig(t, instance)
	rt, err := execsession.New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.rt = rt
	return f
}

// newBundleConfig builds the adapter without a runtime (an importing image
// gets its runtime from the bundle).
func newBundleConfig(t *testing.T, instance string) *bundleFixture {
	t.Helper()
	dir := t.TempDir()

	f := &bundleFixture{manager: &workload.Manager{}, exited: map[string]int{}}
	digest := sha256.Sum256([]byte("bundle actor"))
	f.scope = procrequest.Scope{SID: "lab", NID: "node", ActorFP: "SHA256:" + base64.RawStdEncoding.EncodeToString(digest[:]), ClientLineage: "bundle-client"}
	f.cfg = execsession.Config{Instance: instance, Manager: f.manager, ReplayCapacity: 256,
		Start: func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
			stdio, err := workload.NewStdio()
			if err != nil {
				return nil, "", err
			}
			cmd := exec.Command(req.Argv[0], req.Argv[1:]...)
			cmd.Dir = dir
			cmd.Env = handoff.CleanEnvironment(os.Environ())
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			stdio.Bind(cmd)
			if err := cmd.Start(); err != nil {
				stdio.Close()
				return nil, "", err
			}
			group := cmd.Process.Pid
			t.Cleanup(func() { _ = syscall.Kill(-group, syscall.SIGKILL) })
			stdio.CloseChild()
			if err := f.manager.AdoptDraining(workload.Identity{ID: pid, OSPID: cmd.Process.Pid, StartedAt: time.Now().UTC()}, cmd.Process); err != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				_ = cmd.Wait()
				stdio.Close()
				return nil, "", err
			}
			f.mu.Lock()
			f.spawned++
			f.mu.Unlock()
			return stdio, "", nil
		},
		Started: func(string, proto.ExecReq, workload.Identity) {},
		Exited:  func(pid string, _ int) { f.mu.Lock(); f.exited[pid]++; f.mu.Unlock() },
	}
	t.Cleanup(func() {
		f.manager.Thaw()
		_ = f.manager.SignalAll(syscall.SIGKILL)
		if f.rt != nil {
			ctx, cancel := context.WithTimeout(context.Background(), bundleTestBudget)
			defer cancel()
			_ = f.rt.Close(ctx)
		}
	})
	return f
}

func (f *bundleFixture) handle(req proto.ProcessResumeReq) (proto.ProcessResumeReply, error) {
	f.calls++
	req.Version, req.CallID, req.ActorFP = proto.ProcessResumeVersion, f.calls, f.scope.ActorFP
	return f.rt.Handle(f.scope, req, "_TINBOX.bundle")
}

func (f *bundleFixture) create(t *testing.T, requestID string, argv []string, stdin []byte) (proto.ProcessHandle, string) {
	t.Helper()
	probe, err := f.handle(proto.ProcessResumeReq{Op: "probe", ClientLineage: f.scope.ClientLineage})
	if err != nil || probe.Lease == nil {
		t.Fatalf("probe: %+v %v", probe, err)
	}
	h := proto.ProcessHandle{Lease: *probe.Lease, RequestID: requestID}
	spec := proto.ExecReq{Argv: argv, Stdin: stdin}
	reply, err := f.handle(proto.ProcessResumeReq{Op: "create", Handle: &h, AttachEpoch: 1, Exec: &spec})
	if err != nil || reply.PID == "" {
		t.Fatalf("create %s: %+v %v", requestID, reply, err)
	}
	return h, reply.PID
}

func (f *bundleFixture) awaitRunning(t *testing.T, h proto.ProcessHandle) {
	t.Helper()
	deadline := time.Now().Add(bundleTestBudget)
	for {
		reply, err := f.handle(proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1})
		if err != nil {
			t.Fatalf("resume: %v", err)
		}
		if reply.Phase == "running" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("never running: %+v", reply)
		}
		time.Sleep(time.Millisecond)
	}
}

// drain collects both streams to their end, then settles.
func (f *bundleFixture) drain(t *testing.T, h proto.ProcessHandle) (string, proto.ProcessResumeReply) {
	t.Helper()
	var out, errOut []byte
	var outFIN, errFIN bool
	var outAbort, errAbort string
	deadline := time.Now().Add(bundleTestBudget)
	for {
		reply, err := f.handle(proto.ProcessResumeReq{Op: "resume", Handle: &h, AttachEpoch: 1,
			StdoutACK: uint64(len(out)), StdoutFIN: outFIN, StderrACK: uint64(len(errOut)), StderrFIN: errFIN})
		if err != nil {
			t.Fatalf("resume %s: %v", h.RequestID, err)
		}
		if reply.Stdout != nil {
			out = append(out, reply.Stdout.Data...)
			outFIN, outAbort = reply.Stdout.FIN, reply.Stdout.Error
		}
		if reply.Stderr != nil {
			errOut = append(errOut, reply.Stderr.Data...)
			errFIN, errAbort = reply.Stderr.FIN, reply.Stderr.Error
		}
		if reply.Phase == "terminal" && (outFIN || outAbort != "") && (errFIN || errAbort != "") {
			ack := proto.ProcessResumeReq{Op: "terminal-ack", Handle: &h, AttachEpoch: 1, TerminalID: reply.TerminalID,
				StdoutACK: uint64(len(out)), StdoutFIN: outFIN, StderrACK: uint64(len(errOut)), StderrFIN: errFIN}
			if settled, err := f.handle(ack); err != nil || settled.Phase != "settled" {
				t.Fatalf("settle %s: %+v %v", h.RequestID, settled, err)
			}
			return string(out), reply
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never ended: %+v", h.RequestID, reply)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// courierStub records the fence state at export time (D8: the outbox is
// read only after both the runtime and the table are frozen) and what it
// was asked to import.
type courierStub struct {
	state                 CourierState
	exportErr             error
	checkErr              error
	importErr             error
	checks                int
	exportedRuntimeFrozen bool
	exportedManagerFrozen bool
	exports               int
	imported              *CourierState
	runtime               *execsession.Runtime
	manager               *workload.Manager
}

func (c *courierStub) ExportOutbox() (CourierState, error) {
	c.exports++
	if c.runtime != nil {
		c.exportedRuntimeFrozen = c.runtime.Frozen()
	}
	if c.manager != nil {
		c.exportedManagerFrozen = c.manager.Frozen()
	}
	return c.state, c.exportErr
}

func (c *courierStub) Check(st CourierState) error {
	c.checks++
	if c.checkErr != nil {
		return c.checkErr
	}
	return st.Validate()
}

func (c *courierStub) ImportOutbox(st CourierState) error {
	if c.importErr != nil {
		return c.importErr
	}
	c.imported = &st
	return nil
}

func courierEvents() CourierState {
	return CourierState{Version: CourierStateVersion, Events: []CourierEvent{
		{PID: "01TASKSTARTED0000000000000", Kind: CourierEventStarted, Payload: json.RawMessage(`{"pid":"01TASKSTARTED0000000000000"}`), EnqueuedAtUnixNano: 1, StartedAtUnixNano: 1, StartTimeTicks: 7},
		{PID: "01TASKEXITED00000000000000", Kind: CourierEventExit, Payload: json.RawMessage(`{"pid":"01TASKEXITED00000000000000","exit_code":3}`), EnqueuedAtUnixNano: 2, Parked: true, RC: 3, EndedAtUnixNano: 2},
	}}
}

// bundleMountSrc renders a one-line fake mount table with an NFS mount at mp.
func bundleMountSrc(mp string) spawnsafe.MountSource {
	raw := []byte(fmt.Sprintf("36 35 0:1 / %s rw - nfs src rw\n", mp))
	return func() ([]byte, error) { return raw, nil }
}

type bundleProbe struct {
	mu    sync.Mutex
	calls int
	block chan bool
}

func (p *bundleProbe) fn(string) bool {
	p.mu.Lock()
	p.calls++
	block := p.block
	p.mu.Unlock()
	if block != nil {
		v, ok := <-block
		return ok && v
	}
	return true
}

func (p *bundleProbe) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// bundleDupSource duplicates the cut's originals the way handoff does.
type bundleDupSource struct {
	files map[string]map[string]*os.File
}

func newBundleDupSource(t *testing.T, resources []handoff.Resource) *bundleDupSource {
	t.Helper()
	src := &bundleDupSource{files: make(map[string]map[string]*os.File, len(resources))}
	for _, res := range resources {
		files := make(map[string]*os.File, len(res.Files))
		for name, file := range res.Files {
			raw, err := file.SyscallConn()
			if err != nil {
				t.Fatal(err)
			}
			dup, dupErr := -1, error(nil)
			if err := raw.Control(func(fd uintptr) { dup, dupErr = unix.FcntlInt(fd, unix.F_DUPFD_CLOEXEC, 3) }); err != nil || dupErr != nil {
				t.Fatalf("dup: %v %v", err, dupErr)
			}
			files[name] = os.NewFile(uintptr(dup), "bundle-dup")
		}
		src.files[res.ID] = files
	}
	t.Cleanup(src.Close)
	return src
}

func (s *bundleDupSource) TakeFile(id, name string) (*os.File, error) {
	f := s.files[id][name]
	if f == nil {
		return nil, fmt.Errorf("bundle: %s/%s unavailable", id, name)
	}
	delete(s.files[id], name)
	return f, nil
}

func (s *bundleDupSource) Close() {
	for _, files := range s.files {
		for name, f := range files {
			_ = f.Close()
			delete(files, name)
		}
	}
}

// resourcesFor returns the exported per-task resources (the ones Prepare
// would duplicate); Frozen keeps them private, so the test re-derives them
// through a Prepare-free path: the fixture's runtime Export was already
// consumed by Freeze, and the originals are exactly what the bundle names.
func (f *Frozen) resourcesForTest() []handoff.Resource { return f.resources }

// bundleSpawnPolicy builds a policy with one hangable mount and a decided
// healthy verdict for it (obtained through the exported Prepare path).
func bundleSpawnPolicy(t *testing.T, mp string, probe *bundleProbe, clock func() time.Time) *spawnsafe.Policy {
	t.Helper()
	p, err := spawnsafe.New(spawnsafe.Config{Mode: spawnsafe.ModeAuto, MountSource: bundleMountSrc(mp), Probe: probe.fn, Now: clock, ProbeTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// origin: hot-update S2 plan §5.1 — TestBundleRoundTrip: four domains cut
// in the mandated order, encoded and strictly decoded, imported over the
// shared table, the old cut discarded, the image thawed: the child's bytes
// and exit code arrive, the spawn verdict is honoured without a probe, the
// courier gets the same event set, and the courier was asked only after
// both the runtime and the table were frozen.
func TestBundleRoundTrip(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "bundle-instance")
	hLive, pidLive := f.create(t, "live", []string{"/bin/sh", "-c", "printf partial; exec sleep 30"}, nil)
	f.awaitRunning(t, hLive)
	hDone, pidDone := f.create(t, "done", []string{"/bin/sh", "-c", "exit 5"}, nil)
	f.drain(t, hDone) // settled: no task remains for it
	_ = pidDone
	probe := &bundleProbe{}
	now := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	clock := func() time.Time { return now }
	policy := bundleSpawnPolicy(t, "/bundle-nfs", probe, clock)
	if _, err := policy.Prepare([]string{"/bin/true"}, "/bundle-nfs", "/usr/bin", nil, false); err != nil {
		t.Fatal(err)
	}
	if probe.count() != 1 {
		t.Fatalf("fixture: %d probes, want 1", probe.count())
	}
	courier := &courierStub{state: courierEvents(), runtime: f.rt, manager: f.manager}
	var busyCalls int
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy, Courier: courier,
		Busy:    []BusyCheck{func() (string, string, bool) { busyCalls++; return "", "", false }},
		Lineage: Lineage{Generation: 3, InstanceID: "inst", RoutingNID: "nid"}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	if busyCalls != 1 || courier.exports != 1 || !courier.exportedRuntimeFrozen || !courier.exportedManagerFrozen {
		t.Fatalf("cut order: busy=%d exports=%d runtimeFrozenAtExport=%t managerFrozenAtExport=%t",
			busyCalls, courier.exports, courier.exportedRuntimeFrozen, courier.exportedManagerFrozen)
	}
	b := frozen.Bundle()
	if b.Version != BundleVersion || b.Instance != "bundle-instance" || b.Lineage.Generation != 3 || b.Lineage.ProtoVersion != proto.ProtoVersion ||
		len(b.Runtime.Tasks) != 1 || b.Runtime.Tasks[0].PID != pidLive || len(b.Processes.Records) != 1 || b.SpawnSafe == nil ||
		len(b.SpawnSafe.Verdicts) != 1 || len(b.Courier.Events) != 2 {
		t.Fatalf("bundle: %s", b)
	}
	if !b.Lineage.Matches("inst", "nid") || b.Lineage.Matches("other", "nid") {
		t.Fatal("lineage match")
	}
	if strings.Contains(b.String(), base64.StdEncoding.EncodeToString(b.Runtime.Registry.Secret)) {
		t.Fatal("Bundle.String prints the registry secret")
	}
	// Envelope round trip, as exec would carry it.
	data, err := encode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"secret"`) {
		t.Fatal("the encoded bundle must carry the registry key for the sealed snapshot")
	}
	decoded, err := Decode(handoff.State{Role: "agent", Instance: b.Instance, Generation: 3, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(handoff.State{Role: "agent", Instance: b.Instance, Generation: 4, Data: data}); err == nil {
		t.Fatal("envelope generation mismatch accepted")
	}
	// Import over the shared (still frozen) table with a fresh policy and courier.
	probe2 := &bundleProbe{}
	policy2 := bundleSpawnPolicy(t, "/bundle-nfs", probe2, clock)
	courier2 := &courierStub{}
	src := newBundleDupSource(t, frozen.resourcesForTest())
	target := Target{Runtime: f.cfg, Spawn: policy2, Courier: courier2}
	imported, salvage, err := Import(decoded, f.manager.Snapshot(), src, target)
	if err != nil {
		t.Fatalf("import: %v (salvage %v)", err, salvage)
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := frozen.Thaw(); err == nil {
		t.Fatal("a discarded cut could be thawed")
	}
	if imported.FreezeWindow() != 0 {
		t.Fatal("freeze window before thaw")
	}
	if err := imported.Thaw(); err != nil {
		t.Fatal(err)
	}
	if imported.FreezeWindow() <= 0 {
		t.Fatalf("freeze window %v", imported.FreezeWindow())
	}
	f.rt = imported.Runtime
	if courier2.imported == nil || len(courier2.imported.Events) != 2 || !courier2.imported.Events[1].Parked {
		t.Fatalf("courier import: %+v", courier2.imported)
	}
	if _, err := policy2.Prepare([]string{"/bin/true"}, "/bundle-nfs", "/usr/bin", nil, false); err != nil {
		t.Fatal(err)
	}
	if probe2.count() != 0 {
		t.Fatalf("imported verdict was re-probed %d times", probe2.count())
	}
	// The live task ends under the new image with its bytes intact and its
	// real terminal (a SIGKILL delivered after the switch).
	if err := f.manager.Signal(pidLive, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	out, reply := f.drain(t, hLive)
	if out != "partial" || reply.Signal != int(syscall.SIGKILL) || reply.TargetInstance != "bundle-instance" {
		t.Fatalf("live task after the switch: out=%q reply=%+v", out, reply)
	}
	f.mu.Lock()
	exits := f.exited[pidLive]
	f.mu.Unlock()
	if exits != 1 {
		t.Fatalf("live task exited %d times", exits)
	}
}

// origin: hot-update S2 plan §5.1 — every bundle-level refusal returns the
// taken descriptors open, installs nothing in the fresh table or policy,
// leaves the child alone and lets the intact bundle import afterwards.
func TestBundleRestoreRejectsPartialBundleWithoutInstalling(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "reject-instance")
	hLive, _ := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	f.awaitRunning(t, hLive)
	probe := &bundleProbe{}
	policy := bundleSpawnPolicy(t, "/reject-nfs", probe, time.Now)
	if _, err := policy.Prepare([]string{"/bin/true"}, "/reject-nfs", "/usr/bin", nil, false); err != nil {
		t.Fatal(err)
	}
	courier := &courierStub{state: courierEvents()}
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy, Courier: courier, Lineage: Lineage{Generation: 2}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	good := frozen.Bundle()
	osPID := good.Processes.Records[0].OSPID
	data, err := encode(good)
	if err != nil {
		t.Fatal(err)
	}
	clone := func(t *testing.T) Bundle {

		var b Bundle
		if err := json.Unmarshal(data, &b); err != nil {
			t.Fatal(err)
		}
		return b
	}
	mutations := []struct {
		name string
		edit func(*Bundle)
		raw  func(string) string
		want string
		// courierCheck rows are refused by the courier's own pure Check (the
		// real one lives in internal/agent and re-decodes every payload;
		// the stub here refuses when asked to, and the row proves the
		// refusal happens before any descriptor is taken).
		courierCheck bool
		noRedispatch bool
	}{
		{name: "bundle-version", edit: func(b *Bundle) { b.Version = 2 }},
		{name: "instance-mismatch", edit: func(b *Bundle) { b.Instance = "someone-else" }},
		{name: "boot-id", edit: func(b *Bundle) { b.Lineage.BootID = "another-boot" }},
		{name: "proto-version", edit: func(b *Bundle) { b.Lineage.ProtoVersion++ }},
		// generation 0 is refused by handoff.Prepare itself, so this is the
		// one bundle a salvage cannot re-dispatch: it could never have been
		// dispatched to begin with.
		{name: "generation-zero", edit: func(b *Bundle) { b.Lineage.Generation = 0 }, noRedispatch: true},
		{name: "courier-kind", edit: func(b *Bundle) { b.Courier.Events[0].Kind = "bogus" }},
		{name: "courier-parked-started", edit: func(b *Bundle) { b.Courier.Events[0].Parked = true }},
		{name: "spawn-safe-version", edit: func(b *Bundle) { b.SpawnSafe.Version = 9 }},
		{name: "foreign-row", edit: func(b *Bundle) {
			b.Processes.Records = append(b.Processes.Records, workload.Record{Identity: workload.Identity{ID: "legacy", OSPID: osPID, StartedAt: time.Now()}})
		}, want: ReasonForeignProcessRows},
		{name: "runtime-version", edit: func(b *Bundle) { b.Runtime.Version = 2 }},
		// origin: hot-update S2 internal review, import lane missed-coverage 0
		// — the four plan §5.1 rows the first table lacked.
		{name: "processes-owner-pid", edit: func(b *Bundle) { b.Processes.OwnerPID++ }},
		{name: "spawn-safe-unknown-state", raw: func(s string) string {
			return strings.Replace(s, `"healthy":`, `"state":"bogus","healthy":`, 1)
		}},
		{name: "courier-payload-disagrees-with-fields", edit: func(b *Bundle) { b.Courier.Events[1].RC++ }, courierCheck: true},
		{name: "image-too-large-descriptors", edit: func(b *Bundle) {
			// origin: hot-update S4 plan §5.1 — exceed the actual file bound;
			// empty tasks no longer stand for six hypothetical descriptors.
			for i := 0; i <= maxHandoffFiles; i++ {
				b.Runtime.Tasks = append(b.Runtime.Tasks, execsession.TaskImage{PID: fmt.Sprintf("p%05d", i),
					Files: map[string]execsession.FileIdentity{"stdout-replay": {Dev: 1, Ino: uint64(i + 1)}}})
			}
		}, want: ReasonImageTooLarge},
		{name: "unknown-top-level-key", raw: func(s string) string { return strings.Replace(s, `"version":1`, `"version":1,"future_field":1`, 1) }},
		{name: "unknown-runtime-key", raw: func(s string) string {
			return strings.Replace(s, `"frozen_at_boot_ns"`, `"newer_runtime_field":1,"frozen_at_boot_ns"`, 1)
		}},
		{name: "trailing-data", raw: func(s string) string { return s + "{}" }},
	}
	tableBefore := fmt.Sprintf("%+v", f.manager.Snapshot())
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			var b Bundle
			var decodeErr error
			if tc.raw != nil {
				b, decodeErr = Decode(handoff.State{Role: "agent", Instance: good.Instance, Generation: 2, Data: []byte(tc.raw(string(data)))})
				if decodeErr == nil {
					t.Fatalf("mutation %s decoded", tc.name)
				}
				if !errors.Is(decodeErr, ErrBundle) {
					t.Fatalf("mutation %s: %v is not ErrBundle", tc.name, decodeErr)
				}
				return
			}
			b = clone(t)
			tc.edit(&b)
			fdBefore := testharness.FDCount()
			src := newBundleDupSource(t, frozen.resourcesForTest())
			fresh := &workload.Manager{}
			cfg := f.cfg
			cfg.Manager = fresh
			probe2 := &bundleProbe{}
			policy2 := bundleSpawnPolicy(t, "/reject-nfs", probe2, time.Now)
			courier2 := &courierStub{}
			if tc.courierCheck {
				courier2.checkErr = errors.New("courier import: exit event disagrees with its payload")
			}
			imported, salvage, err := Import(b, b.Processes.Records, src, Target{Runtime: cfg, Spawn: policy2, Courier: courier2})
			if err == nil || imported != nil {
				t.Fatalf("mutation %s imported", tc.name)
			}
			if tc.courierCheck && len(salvage.Resources) != 0 {
				t.Fatalf("mutation %s: descriptors taken before the courier pre-check", tc.name)
			}
			var deferred *DeferredError
			if tc.want != "" && (!errors.As(err, &deferred) || deferred.Reason != tc.want) {
				t.Fatalf("mutation %s: want deferral %s, got %v", tc.name, tc.want, err)
			}
			if tc.want == "" && !errors.Is(err, ErrBundle) {
				t.Fatalf("mutation %s: %v is not ErrBundle", tc.name, err)
			}
			if strings.Contains(err.Error(), base64.StdEncoding.EncodeToString(good.Runtime.Registry.Secret)) {
				t.Fatalf("mutation %s: error carries the registry secret", tc.name)
			}
			for _, res := range salvage.Resources {
				for name, file := range res.Files {
					if _, err := file.Stat(); err != nil {
						t.Fatalf("mutation %s: salvaged %s/%s closed", tc.name, res.ID, name)
					}
					if !fileIsCloexec(t, file) {
						t.Fatalf("mutation %s: salvaged %s/%s lost CLOEXEC", tc.name, res.ID, name)
					}
				}
			}
			// The salvage re-dispatches the SAME bundle (plan §5.1: Prepare on
			// the salvage is executable) and the source keeps whatever the
			// import did not take.
			if salvage.Bundle() == nil {
				t.Fatalf("mutation %s: salvage lost the bundle", tc.name)
			}
			prepared, err := salvage.Prepare()
			switch {
			case tc.noRedispatch && err == nil:
				t.Fatalf("mutation %s: an undispatchable bundle re-dispatched", tc.name)
			case !tc.noRedispatch && err != nil:
				t.Fatalf("mutation %s: salvage.Prepare: %v", tc.name, err)
			case err == nil:
				if err := prepared.Close(); err != nil {
					t.Fatal(err)
				}
			}
			for id, files := range src.files {
				for name, file := range files {
					if _, err := file.Stat(); err != nil {
						t.Fatalf("mutation %s: untaken source file %s/%s closed", tc.name, id, name)
					}
				}
			}
			if err := salvage.Close(); err != nil {
				t.Fatal(err)
			}
			src.Close()
			if got := testharness.FDCount(); got != fdBefore {
				t.Fatalf("mutation %s: descriptors leaked or closed: %d vs %d", tc.name, got, fdBefore)
			}
			if rows := fresh.Snapshot(); len(rows) != 0 {
				t.Fatalf("mutation %s: process table installed: %+v", tc.name, rows)
			}
			if c := policy2.Census(); c.ProbesInFlight != 0 || probe2.count() != 0 {
				t.Fatalf("mutation %s: policy touched", tc.name)
			}
			if courier2.imported != nil {
				t.Fatalf("mutation %s: courier imported", tc.name)
			}
			if err := syscall.Kill(osPID, 0); err != nil {
				t.Fatalf("mutation %s: child gone: %v", tc.name, err)
			}
			if got := fmt.Sprintf("%+v", f.manager.Snapshot()); got != tableBefore {
				t.Fatalf("mutation %s: shared table changed", tc.name)
			}
		})
	}
	// The intact bundle still goes through, and nothing above consumed it.
	if err := frozen.Thaw(); err != nil {
		t.Fatal(err)
	}
	_ = f.manager.Signal(good.Runtime.Tasks[0].PID, syscall.SIGKILL)
	if _, reply := f.drain(t, hLive); reply.Signal != int(syscall.SIGKILL) {
		t.Fatalf("live task after the refusals: %+v", reply)
	}
}

// origin: hot-update S2 plan exit #4 M3 / §5.1 — the replay budget after an
// import equals the sum the exporter reported; a second reservation of the
// same logs would show as a doubled figure and a refused fresh log.
func TestBudgetConservedAcrossImport(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "budget-instance")
	f.cfg.ReplayCapacity = 1024
	rt, err := execsession.New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), bundleTestBudget)
	defer cancel()
	_ = f.rt.Close(ctx)
	f.rt = rt
	var handles []proto.ProcessHandle
	for i, stdin := range [][]byte{nil, []byte(strings.Repeat("x", 70<<10)), nil} {
		h, _ := f.create(t, fmt.Sprintf("task%d", i), []string{"/bin/sh", "-c", "cat >/dev/null; exec sleep 30"}, stdin)
		f.awaitRunning(t, h)
		handles = append(handles, h)
	}
	hDone, _ := f.create(t, "done", []string{"/bin/sh", "-c", "exit 1"}, nil)
	deadline := time.Now().Add(bundleTestBudget)
	for {
		reply, err := f.handle(proto.ProcessResumeReq{Op: "resume", Handle: &hDone, AttachEpoch: 1})
		if err == nil && reply.Phase == "terminal" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("terminal task never ended")
		}
		time.Sleep(time.Millisecond)
	}
	usedBefore := f.rt.ReplayBudgetUsed()
	want := int64(3*(64<<10) + 70<<10 - 64<<10 + 4*2*1024)
	if usedBefore != want {
		t.Fatalf("fixture budget %d, want %d", usedBefore, want)
	}
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Lineage: Lineage{Generation: 1}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	b := frozen.Bundle()
	if b.Runtime.BudgetUsed != usedBefore {
		t.Fatalf("exported budget %d, live %d", b.Runtime.BudgetUsed, usedBefore)
	}
	src := newBundleDupSource(t, frozen.resourcesForTest())
	imported, _, err := Import(b, f.manager.Snapshot(), src, Target{Runtime: f.cfg})
	if err != nil {
		t.Fatal(err)
	}
	if got := imported.Runtime.ReplayBudgetUsed(); got != usedBefore {
		t.Fatalf("budget after import %d, before %d", got, usedBefore)
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := imported.Thaw(); err != nil {
		t.Fatal(err)
	}
	f.rt = imported.Runtime
	// Probe the headroom: a fresh task fits exactly once the limit is reached.
	f.cfg.Manager = f.manager
	_ = f.manager.SignalAll(syscall.SIGKILL)
	for _, h := range handles {
		f.drain(t, h)
	}
	f.drain(t, hDone)
	if got := f.rt.ReplayBudgetUsed(); got != 0 {
		t.Fatalf("budget after settling everything: %d", got)
	}
}

// origin: hot-update S2 plan §5.1 — the pure courier validation.
func TestCourierStateRejectsMalformed(t *testing.T) {
	good := courierEvents()
	if err := good.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, edit := range map[string]func(*CourierState){
		"version":        func(s *CourierState) { s.Version = 2 },
		"kind":           func(s *CourierState) { s.Events[0].Kind = "restarted" },
		"pid-empty":      func(s *CourierState) { s.Events[0].PID = "" },
		"payload":        func(s *CourierState) { s.Events[0].Payload = json.RawMessage(`{not json`) },
		"duplicate":      func(s *CourierState) { s.Events = append(s.Events, s.Events[0]) },
		"parked-started": func(s *CourierState) { s.Events[0].Parked = true },
		"negative-fails": func(s *CourierState) { s.Events[0].Fails = -1 },
		"pid-not-token":  func(s *CourierState) { s.Events[0].PID = "not.a/token>" },
		"silent-beyond-park": func(s *CourierState) {
			s.Events[1].Silent = MaxCourierSilent + 1 // the courier parks at MaxCourierSilent; more was never produced
		},
		"no-enqueue": func(s *CourierState) { s.Events[0].EnqueuedAtUnixNano = 0 },
		"too-many": func(s *CourierState) {
			for i := 0; i <= MaxCourierEvents; i++ {
				s.Events = append(s.Events, CourierEvent{PID: fmt.Sprintf("p%d", i), Kind: CourierEventExit, Payload: json.RawMessage(`{}`), EnqueuedAtUnixNano: 1})
			}
		},
		"too-large": func(s *CourierState) {
			s.Events[0].Payload = json.RawMessage(`"` + strings.Repeat("a", MaxCourierBytes) + `"`)
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := courierEvents()
			edit(&bad)
			if err := bad.Validate(); err == nil {
				t.Fatalf("%s accepted", name)
			}
		})
	}
	if MaxCourierEvents != 4096 {
		t.Fatal("MaxCourierEvents must equal the agent courier's pending bound (pinned by internal/agent)")
	}
	_ = runtime.NumGoroutine
}

// fileIsCloexec reads FD_CLOEXEC off a descriptor the salvage handed back.
func fileIsCloexec(t *testing.T, file *os.File) bool {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	flags, ferr := 0, error(nil)
	if err := raw.Control(func(fd uintptr) { flags, ferr = unix.FcntlInt(fd, unix.F_GETFD, 0) }); err != nil || ferr != nil {
		t.Fatalf("F_GETFD: %v %v", err, ferr)
	}
	return flags&unix.FD_CLOEXEC != 0
}

// origin: hot-update S2 internal review, import lane missed-coverage 2 —
// the encode arm of image_too_large. No exporter produces it through
// Validate (the descriptor bound trips first for tasks), so the one shape
// that reaches it is metadata volume: revocations with maximal reasons.
func TestEncodeDefersAnOversizedBundle(t *testing.T) {
	b := Bundle{Version: BundleVersion, Instance: "big", Lineage: Lineage{Generation: 1}}
	reason := strings.Repeat("r", 4096)
	for len(b.Runtime.Revocations)*len(reason) <= MaxBundleBytes {
		b.Runtime.Revocations = append(b.Runtime.Revocations, execsession.RevocationImage{SID: "s", Reason: reason})
	}
	_, err := encode(b)
	var deferred *DeferredError
	if !errors.As(err, &deferred) || deferred.Reason != ReasonImageTooLarge {
		t.Fatalf("oversized bundle encoded: %v", err)
	}
	b.Runtime.Revocations = b.Runtime.Revocations[:1]
	if _, err := encode(b); err != nil {
		t.Fatalf("small bundle refused: %v", err)
	}
}

// Real: composed JSON and capability checks. Private owner records and carrier
// traffic belong to TestBundleFinalReexportIncludesFlowSSRevoke.
// origin: hot-update S5 plan §5.1 / §5.2
func TestBundleFlowSSMetadataKeepsInactiveResponsibility(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseBundleFlowSSMetadata(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "bundle-retained-domains", beforeG)
	testharness.AssertNoFDLeak(t, "bundle-retained-domains", beforeFD, 0)
}

func exerciseBundleFlowSSMetadata(t *testing.T) {
	f := newBundleFixture(t, "metadata-domains")
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !cut.done {
			_ = cut.Thaw()
		}
	}()
	base := cut.Bundle()
	raw, err := Encode(base)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"flow"`) || strings.Contains(string(raw), `"ss"`) {
		t.Fatal("empty optional domains were serialized")
	}
	b := base
	b.Flow, b.SS = flowSSMetadata()
	raw, err = Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(handoff.State{Role: Role, Instance: b.Instance, Generation: b.Lineage.Generation, Data: raw})
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Flow == nil || decoded.SS == nil || decoded.SS.ConnectionCount != 0 || len(decoded.Flow.Flows.Records) != 0 {
		t.Fatal("inactive retained responsibility disappeared")
	}
	if got := strings.Join(decoded.RequiredDomains(), ","); got != "exec-v1,flow-v1,ss-v1" {
		t.Fatalf("retained domains=%s", got)
	}
	for _, domains := range [][]string{{"exec-v1"}, {"exec-v1", "flow-v1"}, {"exec-v1", "ss-v1"}} {
		if err := CheckDomains(Capabilities{ABI: 1, Domains: domains}, decoded.RequiredDomains()); !errors.Is(err, ErrABIUnsupported) {
			t.Fatal("reader missing a retained domain was accepted")
		}
	}
	if err := CheckDomains(CurrentCapabilities("test-sha"), decoded.RequiredDomains()); err != nil {
		t.Fatal(err)
	}
	// The endpoint route state lives in the same declared sealed cut as the
	// client journal; a shared reference must retain its required domain.
	// origin: hot-update S5 plan §5.1 / §12.1 / T33
	b.Flow.Endpoints = metadataEndpoints(1)
	b.Flow.Endpoints[0].StateRef = b.Flow.StateRef
	if err := b.Validate(); err != nil {
		t.Fatalf("aggregate endpoint state rejected: %v", err)
	}
	for number, bad := range []func(*Bundle){
		func(b *Bundle) { b.Flow.Version++ },
		func(b *Bundle) { b.Flow.Flows.Version++ },
		func(b *Bundle) { b.Flow.Flows.StateRef = "flow-state.private-target" },
		func(b *Bundle) { b.Flow.Flows.Records = make([]flow.RecordImage, flow.MaxFlows+1) },
		func(b *Bundle) { b.Flow.Endpoints = metadataEndpoints(tunnel.MaxEndpoints + 1) },
		func(b *Bundle) { b.Flow.Endpoints = metadataEndpoints(1); b.Flow.Endpoints[0].ID = flow.EndpointID{} },
		func(b *Bundle) {
			b.Flow.Endpoints = metadataEndpoints(2)
			b.Flow.Endpoints[1].ID = b.Flow.Endpoints[0].ID
		},
		func(b *Bundle) { b.Flow.Endpoints = metadataEndpoints(1); b.Flow.Endpoints[0].OwnerGen = 0 },
		func(b *Bundle) {
			b.Flow.Endpoints = metadataEndpoints(1)
			b.Flow.Endpoints[0].FlowIDs = make([]flow.ID, flow.MaxFlows+1)
		},
		func(b *Bundle) {
			b.Flow.Endpoints = metadataEndpoints(1)
			b.Flow.Endpoints[0].StateRef = "flow-endpoint.private-target"
		},
		func(b *Bundle) {
			b.Flow.Endpoints = metadataEndpoints(1)
			b.Flow.Endpoints[0].StateRef = "flow-state." + strings.Repeat("2", 32)
		},
		func(b *Bundle) { b.Runtime.BudgetUsed = -1 },
		func(b *Bundle) { b.Runtime.BudgetUsed = (1 << 63) - 1; b.Flow.Flows.ReservedBytes = 1 << 20 },
		func(b *Bundle) { b.SS.Version++ },
		func(b *Bundle) { b.Flow.StateRef = "flow-state.secret-target" },
		func(b *Bundle) { b.SS.ConnectionCount = -1 },
		func(b *Bundle) { b.SS.ConnectionCount = ssproxy.MaxActors + 1 },
		func(b *Bundle) { b.SS.ListenerRef = "ss-listener.private-target" },
		func(b *Bundle) { b.SS.StateRef = "ss-state.private-target" },
		func(b *Bundle) { b.SS.SecretBytes = -1 },
		func(b *Bundle) { b.Flow.Flows.ReservedBytes = -1 },
		func(b *Bundle) { b.Flow.Flows.ReservedBytes = workload.DefaultReplayBudget + 1 },
		func(b *Bundle) { b.SS.SecretBytes = sealedstate.MaxStateBytes + 1 },
		func(b *Bundle) { b.Runtime.BudgetUsed = workload.DefaultReplayBudget; b.Flow.Flows.ReservedBytes = 1 },
	} {
		candidate := base
		candidate.Flow, candidate.SS = flowSSMetadata()
		bad(&candidate)
		if err := candidate.Validate(); err == nil {
			t.Fatalf("invalid flow/SS bundle accepted: case=%d", number)
		}
	}
}

// The caller's admission-close timestamp includes work before the local cut.
// Re-export must preserve that instant rather than restarting calibration.
// origin: hot-update S5 plan §5.3 / §7.1
func TestBundleFreezeWindowStartsAtAdmission(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseBundleAdmissionTimestamp(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "admission-window", beforeG)
	testharness.AssertNoFDLeak(t, "admission-window", beforeFD, 0)
}

func exerciseBundleAdmissionTimestamp(t *testing.T) {
	admission, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	f := newBundleFixture(t, "admission-window")
	// Runtime requests admitted before its own fence may advance while the
	// outer admission/PREPARE clock is already running. Both clocks must
	// survive import without rejecting the original task or hiding the stall.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN
	h, _ := f.create(t, "after-admission", []string{"/bin/sh", "-c", "printf ready; exec sleep 30"}, nil)
	f.awaitRunning(t, h)
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, FrozenAtBootNS: admission}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if !cut.done {
			_ = cut.Thaw()
		}
	}()
	if cut.Bundle().admissionAt != admission || cut.Bundle().Runtime.FrozenAtBootNS <= admission {
		t.Fatal("cut restarted the clock after admission closed")
	}
	// Calibration cannot claim an instant outside the actual accounting cut.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
	for _, at := range []int64{-1, 0, cut.Bundle().Runtime.FrozenAtBootNS + 1} {
		bad := cut.Bundle()
		bad.admissionAt = at
		if at != 0 && bad.Validate() == nil {
			t.Fatal("invalid in-process calibration was accepted")
		}
		snapshot, err := freezeCalibration(bad)
		if snapshot != nil {
			_ = snapshot.Close()
		}
		if err == nil {
			t.Fatal("invalid calibration was exported")
		}
	}
	packed, err := cut.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	if err := packed.Close(); err != nil {
		t.Fatal(err)
	}
	if cut.Bundle().admissionAt != admission || cut.Bundle().Runtime.FrozenAtBootNS <= admission {
		t.Fatal("final re-export restarted the admission clock")
	}
	src := newBundleDupSource(t, cut.resourcesForTest())
	imported, salvage, err := Import(cut.Bundle(), f.manager.Snapshot(), src, Target{Runtime: f.cfg})
	if err != nil || salvage != nil {
		t.Fatalf("outer admission clock invalidated a later runtime task: %v", err)
	}
	if imported.FrozenAt != admission {
		t.Fatal("import lost the full admission-to-activation interval")
	}
	if err := cut.Discard(); err != nil {
		t.Fatal(err)
	}
	f.rt = imported.Runtime
	if err := imported.Thaw(); err != nil {
		t.Fatal(err)
	}
}
