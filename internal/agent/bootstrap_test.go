//go:build linux

// bootstrap_test.go exercises real runtime children and an embedded broker.
// Only exec failure and authoritative register refusals are injected; the real
// binary/exec order is covered by agentboot's boot_exec_test.go.
package agent

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentboot"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

const (
	bootTestBudget = 10 * time.Second
	// The leak gate recognizes this alias; the shared helper remains the source.
	leakExerciseRounds = testharness.LeakExerciseRounds
)

func newBootAgent(t *testing.T, url, nid string) *Agent {
	t.Helper()
	a, err := New(Config{NATSURL: url, SID: "lab", NID: nid, Home: t.TempDir(), Logger: testharness.SilentLog(),
		RemoteFSMode: "off", RosterRefreshInterval: -1, RegisterTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	cleanupBootAgent(t, a)
	return a
}

func cleanupBootAgent(t *testing.T, a *Agent) {
	t.Helper()
	t.Cleanup(func() {
		a.workloads.Thaw()
		if a.processRuntime != nil {
			_ = a.processRuntime.Thaw()
			ctx, cancel := context.WithTimeout(context.Background(), bootTestBudget) // ctx-root: test teardown after Run's context ended
			defer cancel()
			if err := a.processRuntime.Close(ctx); err != nil {
				t.Error(err)
			}
		}
		// An in-process receiver owns a different table over the same child.
		// Runtime.Discard leaves the exporter table intact; release its reap
		// holds only after the receiver cleanup, then retire its stale handles.
		_ = a.workloads.SignalAll(syscall.SIGKILL)
		for _, row := range a.workloads.Snapshot() {
			_ = a.workloads.AllowReap(row.ID)
			_, _ = a.workloads.Wait(row.ID)
			_ = a.workloads.Forget(row.ID)
		}
	})
}

type bootTask struct {
	scope  procrequest.Scope
	handle proto.ProcessHandle
	pid    string
	row    workload.Record
}

func bootCall(rt *execsession.Runtime, task bootTask, op string) (proto.ProcessResumeReply, error) {
	req := proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 7,
		Op: op, ActorFP: task.scope.ActorFP, Handle: &task.handle, AttachEpoch: 1}
	if op == "create" {
		req.Exec = &proto.ExecReq{Argv: []string{"/bin/sh", "-c", "printf carried; exec sleep 60"}}
	}
	return rt.Handle(task.scope, req, "_TINBOX.boot")
}

func startBootTask(t *testing.T, a *Agent) bootTask {
	t.Helper()
	task := bootTask{scope: procrequest.Scope{SID: a.cfg.SID, NID: nidOf(a), ActorFP: "SHA256:" + strings.Repeat("A", 43), ClientLineage: "bootclient"}}
	probe, err := a.processRuntime.Handle(task.scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 1,
		Op: "probe", ActorFP: task.scope.ActorFP, ClientLineage: task.scope.ClientLineage}, "_TINBOX.boot")
	if err != nil || probe.Lease == nil {
		t.Fatalf("probe failed: %v", err)
	}
	task.handle = proto.ProcessHandle{Lease: *probe.Lease, RequestID: "boottask"}
	reply, err := a.processRuntime.Handle(task.scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 2,
		Op: "create", ActorFP: task.scope.ActorFP, Handle: &task.handle, AttachEpoch: 1,
		Exec: &proto.ExecReq{Cwd: t.TempDir(), Argv: []string{"/bin/sh", "-c", "printf carried; exec sleep 60"}}}, "_TINBOX.boot")
	if err != nil || reply.PID == "" {
		t.Fatalf("create failed: %v", err)
	}
	task.pid = reply.PID
	waitFor(t, bootTestBudget, func() bool {
		r, err := bootCall(a.processRuntime, task, "resume")
		return err == nil && r.Phase == "running" && r.Stdout != nil && string(r.Stdout.Data) == "carried"
	})
	var ok bool
	task.row, ok = a.workloads.Lookup(task.pid)
	if !ok || task.row.StartTimeTicks == 0 {
		t.Fatal("task has no kernel identity")
	}
	return task
}

func freezeBootAgent(t *testing.T, a *Agent) *agentimage.Frozen {
	t.Helper()
	// origin: hot-update S5 plan §7.1 / T33. Composition and its Runtime
	// must stamp the same BOOTTIME, including adversarial clock advancement.
	frozenAt, err := processBootClock()
	if err != nil {
		t.Fatal(err)
	}
	f, err := agentimage.Freeze(agentimage.Owners{FrozenAtBootNS: frozenAt, Runtime: a.processRuntime, Manager: &a.workloads,
		Spawn: a.spawnPolicy, Courier: NewCourierImage(a), Lineage: agentimage.Lineage{Generation: 7, InstanceID: a.instanceID, RoutingNID: nidOf(a)}}, time.Now().Add(bootTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func cutBootImage(t *testing.T, a *Agent, f *agentimage.Frozen) (*agentboot.Image, *handofftest.DupSource) {
	t.Helper()
	_, resources, err := a.processRuntime.Export()
	if err != nil {
		t.Fatal(err)
	}
	src := handofftest.DupResources(t, resources)
	b := f.Bundle()
	img := agentboot.FromCut(b, b.Processes.Records, src, src.Borrow())
	t.Cleanup(func() {
		if img.Phase() != agentboot.PhaseActive && img.Phase() != agentboot.PhaseReady {
			img.Abandon(nil, agentboot.CrashWindowNoRollbackTarget, "test")
		}
	})
	return img, src
}

// Read only the sealed manifest, never print it. This is also the receipt of
// what the injectable exec would actually carry, not a copy of the input bundle.
func bootPreparedState(t *testing.T) (handoff.State, string) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		path := "/proc/self/fd/" + entry.Name()
		name, _ := os.Readlink(path)
		if !strings.HasPrefix(name, "/memfd:tether-handoff") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			State handoff.State `json:"state"`
			Nonce string        `json:"nonce"`
		}
		if err := json.Unmarshal(data, &envelope); err != nil {
			t.Fatal("invalid prepared manifest")
		}
		return envelope.State, entry.Name() + ":" + envelope.Nonce
	}
	t.Fatal("no prepared handoff manifest")
	return handoff.State{}, ""
}

// Model only the exec ownership transfer in-process: Prepare's duplicates are
// transferred to Begin, so the old Prepared MUST NOT be closed afterwards (just
// as its owner disappears on exec). Restore's PID/UID/seal checks still run.
func holdBootImage(t *testing.T, b agentimage.Bundle, resources []handoff.Resource) *agentboot.Image {
	t.Helper()
	data, err := agentimage.Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handoff.Prepare(handoff.State{Role: agentimage.Role, Instance: b.Instance, Generation: b.Lineage.Generation, Data: data}, resources); err != nil {
		t.Fatal(err)
	}
	_, dispatch := bootPreparedState(t)
	t.Setenv("_TETHER_HANDOFF", dispatch)
	t.Setenv(instanceIDEnv, b.Lineage.InstanceID)
	t.Setenv(routingNIDEnv, b.Lineage.RoutingNID)
	agentboot.Reset()
	img := agentboot.Begin(testharness.SilentLog(), LineageEnvKeys()...)
	if img == nil || img.Phase() != agentboot.PhaseHeld {
		t.Fatal("Begin did not hold the prepared image")
	}
	t.Cleanup(func() { img.Abandon(nil, agentboot.CrashWindowNoRollbackTarget, "test"); agentboot.Reset() })
	return img
}

func pendingBootMarker(t *testing.T, instance string, count int) (string, string, *upgradeMarker) {
	t.Helper()
	m := testMarker(upgradeStatePending, count, time.Now().Add(time.Hour))
	m.TargetSID, m.TargetNID, m.TargetInstance = "lab", "node", instance
	exe, path := bootFixture(t, "new-image", "old-image", nil)
	m.NewSHA, m.PrevSHA = mustSHA(t, exe), mustSHA(t, upgradePrevPath(exe))
	if err := writeUpgradeMarker(path, m); err != nil {
		t.Fatal(err)
	}
	return exe, path, m
}

// origin: hot-update S3 plan §1.6 T2'. A failed rollback exec must leave the
// held descriptor owner usable, including after closing the attempted dispatch.
func TestBootRollbackExecFailureKeepsImageHeld(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			a := newBootAgent(t, "nats://127.0.0.1:1", "node")
			task := startBootTask(t, a)
			f := freezeBootAgent(t, a)
			_, resources, err := a.processRuntime.Export()
			if err != nil {
				t.Fatal(err)
			}
			img := holdBootImage(t, f.Bundle(), resources)
			exe, marker, _ := pendingBootMarker(t, a.instanceID, upgradeBootBudget)
			old := bootRedispatchExec
			t.Cleanup(func() { bootRedispatchExec = old })
			var prepared *handoff.Prepared
			bootRedispatchExec = func(p *handoff.Prepared, env []string, fallback func(string) error) func(string) error {
				prepared = p
				return func(path string) error {
					st, _ := bootPreparedState(t)
					b, err := agentimage.Decode(st)
					if err != nil || len(b.Runtime.Tasks) != 1 {
						t.Fatal("rollback lost the task")
					}
					return fallback(path)
				}
			}
			fds := testharness.FDCount()
			called := false
			bootUpgradeCheckLocked(exe, testharness.SilentLog(), time.Now(), img, func(string) error { called = true; return syscall.ENOEXEC })
			if !called || prepared == nil || img.Phase() != agentboot.PhaseHeld {
				t.Fatal("rollback did not preserve held ownership")
			}
			if err := prepared.Exec("/nonexistent", []string{"bad"}, nil); err == nil || !strings.Contains(err.Error(), "closed bundle") {
				t.Fatal("rollback Prepared was not closed")
			}
			if got := testharness.FDCount(); got != fds {
				t.Fatalf("failed exec leaked descriptors: %d -> %d", fds, got)
			}
			m := readMarkerState(t, marker)
			if m.ResourcePhase != markerPhaseRedispatched || !strings.HasPrefix(m.Detail, "boot: budget/deadline exhausted") {
				t.Fatalf("rollback marker: %+v", m)
			}
			b, err := New(Config{NATSURL: a.cfg.NATSURL, SID: "lab", NID: "node", BootImage: img, RemoteFSMode: "off"})
			if err != nil {
				t.Fatalf("held image could not restore after failed exec: %v", err)
			}
			cleanupBootAgent(t, b)
			if err := f.Discard(); err != nil {
				t.Fatal(err)
			}
			if err := activateImage(b); err != nil {
				t.Fatal(err)
			}
			if r, err := bootCall(b.processRuntime, task, "resume"); err != nil || r.Stdout == nil || string(r.Stdout.Data) != "carried" {
				t.Fatal("restored child is not readable")
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "boot rollback x5", gBefore)
	testharness.AssertNoFDLeak(t, "boot rollback x5", fdBefore, 0)
}

func bootBroker(t *testing.T) (string, *sql.DB, *nats.Conn) {
	t.Helper()
	// Retain the embedded server so teardown can observe the broker's async
	// Drain completing before shutdown removes its peer. Otherwise Drain's
	// FlushTimeout can outlive the whole five-round leak exercise.
	// origin: hot-update S4 plan §8.4 / T26
	srv := natsservertest.RunServer(&server.Options{Host: "127.0.0.1", Port: -1})
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	url := srv.ClientURL()
	db := testharness.OpenDB(t)
	stackharness.SeedSession(t, db, "lab", "SHA256:"+strings.Repeat("A", 43))
	b, err := broker.New(broker.Config{NATSURL: url, DB: db, Logger: testharness.SilentLog()})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(bootTestBudget):
			t.Error("broker did not stop")
		}
		waitFor(t, bootTestBudget, func() bool { return srv.NumClients() == 0 })
	})
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	// Subscription readiness is observable with a harmless invalid register.
	waitFor(t, bootTestBudget, func() bool {
		_, err := nc.Request(proto.SubjNodeRegister("lab", "readiness"), []byte(`{}`), 50*time.Millisecond)
		return err == nil
	})
	return url, db, nc
}

func runBootAgent(t *testing.T, a *Agent) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx); close(done) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(bootTestBudget):
			t.Error("agent did not stop")
		}
	})
	return done
}

// origin: hot-update S3 plan §3.1 T3. Refusal must not turn into an empty
// registration, and its salvage must still be a complete one-task dispatch.
// origin: S8 internal review FIX-AGENTRACE-A F3
func TestImportFailureNeverRegistersEmptyTable(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedAgentTest(t, "TestImportFailureNeverRegistersEmptyTable") {
		return
	}
	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			url, db, _ := bootBroker(t)
			a := newBootAgent(t, url, "node")
			task := startBootTask(t, a)
			f := freezeBootAgent(t, a)
			_, resources, err := a.processRuntime.Export()
			if err != nil {
				t.Fatal(err)
			}
			src := handofftest.DupResources(t, resources)
			bundle := f.Bundle()
			bundle.Courier.Events[0].StartTimeTicks++ // valid ABI, inconsistent courier payload
			img := agentboot.FromCut(bundle, bundle.Processes.Records, src, src.Borrow())
			exe, path, m := pendingBootMarker(t, a.instanceID, 1)
			rememberUpgradeBootProof(m.UpgradeID)
			t.Cleanup(func() { rememberUpgradeBootProof("") })
			b, restoreErr := New(Config{NATSURL: url, SID: "lab", NID: "node", BootImage: img, RemoteFSMode: "off"})
			if b != nil {
				cleanupBootAgent(t, b)
				runBootAgent(t, b)
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				var n int
				if err := db.QueryRow(`SELECT count(*) FROM nodes WHERE sid='lab' AND nid='node'`).Scan(&n); err != nil {
					t.Fatal(err)
				}
				if n != 0 {
					t.Fatal("import refusal registered an empty node table")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if strings.Contains((&ImageRestoreError{Summary: img.Summary(), Err: errors.New("secret")}).Error(), "secret") {
				t.Fatal("restore error exposed its untrusted cause")
			}
			var rejected *ImageRestoreError
			if !errors.As(restoreErr, &rejected) || img.Phase() != agentboot.PhaseRefused {
				t.Fatalf("New did not return restore refusal: %v", restoreErr)
			}
			if err := syscall.Kill(task.row.OSPID, 0); err != nil {
				t.Fatal("import refusal killed the original child")
			}
			old := bootRedispatchExec
			t.Cleanup(func() { bootRedispatchExec = old })
			seen := false
			bootRedispatchExec = func(p *handoff.Prepared, _ []string, _ func(string) error) func(string) error {
				return func(string) error {
					state, _ := bootPreparedState(t)
					decoded, err := agentimage.Decode(state)
					if err != nil || len(decoded.Runtime.Tasks) != 1 {
						t.Fatal("refused image lost its task on re-dispatch")
					}
					seen = true
					return syscall.ENOEXEC
				}
			}
			finishBoot(img, exe, testharness.SilentLog(), restoreErr, func(string) error { t.Fatal("bare exec after import refusal"); return nil })
			if !seen || readMarkerState(t, path).Detail != "bootstrap failed before control-ready: bundle_rejected" {
				t.Fatal("restore failure did not re-dispatch")
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "import refusal x5", gBefore)
	testharness.AssertNoFDLeak(t, "import refusal x5", fdBefore, 0)
}

// origin: hot-update S3 plan §3.2/§3.3 T4. The first real register must carry
// the imported kernel identity while the runtime is already thawed.
func TestReconcileSnapshotIncludesImportedTasks(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if runIsolatedAgentTest(t, "TestReconcileSnapshotIncludesImportedTasks") {
		return
	}
	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			url, db, nc := bootBroker(t)
			a := newBootAgent(t, url, "node")
			a.ncBox.Store(nc)
			if _, err := a.register(t.Context(), nc); err != nil {
				t.Fatal(err)
			}
			task := startBootTask(t, a)
			a.courier.deliverRound(t.Context())
			if receiptPending(t, a.courier, task.pid, procEventStarted) != nil {
				t.Fatal("broker never recorded the initial task")
			}
			f := freezeBootAgent(t, a)
			img, _ := cutBootImage(t, a, f)
			exe, markerPath, marker := pendingBootMarker(t, a.instanceID, 1)
			// The validated image must win over conflicting, already-consumed
			// environment lineage without claiming the basename sibling rows.
			t.Setenv(instanceIDEnv, strings.Repeat("z", instanceIDLen))
			t.Setenv(routingNIDEnv, "node-02")
			b, err := New(Config{NATSURL: url, SID: "lab", NID: "node", Home: t.TempDir(), BootImage: img,
				UpgradeExecutablePath: exe, UpgradeRunningImagePath: exe, UpgradeBootProofID: marker.UpgradeID,
				Logger: testharness.SilentLog(), RemoteFSMode: "off", RosterRefreshInterval: -1})
			if err != nil {
				t.Fatal(err)
			}
			cleanupBootAgent(t, b)
			if b.instanceID != a.instanceID || nidOf(b) != nidOf(a) || b.previousNID.Load() != nil || b.imageGeneration != 7 || !b.processRuntime.Frozen() {
				t.Fatal("New lost lineage or activated prematurely")
			}
			// Even a process with the transaction proof may not stamp a
			// marker for another SID. The host lock is not ownership proof.
			marker.TargetSID = "foreign"
			if err := writeUpgradeMarker(markerPath, marker); err != nil {
				t.Fatal(err)
			}
			foreignBefore, _ := os.ReadFile(markerPath)
			stampMarkerPhase(b, markerPhaseActive, 99, time.Hour)
			foreignAfter, _ := os.ReadFile(markerPath)
			if string(foreignBefore) != string(foreignAfter) {
				t.Fatal("activation stamped a foreign marker")
			}
			marker.TargetSID = "lab"
			if err := writeUpgradeMarker(markerPath, marker); err != nil {
				t.Fatal(err)
			}
			rows := localProcessSnapshot(b)
			if len(rows) != 1 || rows[0].PID != task.pid || rows[0].StartTimeTicks != task.row.StartTimeTicks || !rows[0].StartedAt.Equal(task.row.StartedAt) {
				t.Fatalf("imported snapshot lost identity: %+v", rows)
			}
			if err := f.Discard(); err != nil {
				t.Fatal(err)
			}
			// The dial seam is deterministic even if the broker response races
			// our passive observer: no network operation may precede thaw.
			var frozenAtDial atomic.Bool
			b.cfg.TeardownDialFn = func(ctx context.Context, network, address string) (net.Conn, error) {
				if b.processRuntime.Frozen() || b.workloads.Frozen() {
					frozenAtDial.Store(true)
				}
				return new(net.Dialer).DialContext(ctx, network, address)
			}
			observed := make(chan bool, 1)
			sub, err := nc.Subscribe(proto.SubjNodeRegister("lab", "node"), func(msg *nats.Msg) {
				var req proto.NodeRegisterReq
				if json.Unmarshal(msg.Data, &req) != nil || req.ReleasingName {
					return
				}
				select {
				case observed <- b.processRuntime.Frozen() || b.workloads.Frozen() || !strings.Contains(req.UpgradeDetail, "; image gen 7: 1 tasks carried, freeze window "):
				default:
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			runBootAgent(t, b)
			select {
			case frozen := <-observed:
				if frozen || frozenAtDial.Load() {
					t.Fatal("first register preceded thaw or lost its image receipt")
				}
			case <-time.After(bootTestBudget):
				t.Fatal("no first register")
			}
			waitFor(t, bootTestBudget, func() bool { return img.Phase() == agentboot.PhaseReady })
			committed := readMarkerState(t, markerPath)
			if committed.State != upgradeStateCommitted || committed.ResourcePhase != markerPhaseActive ||
				committed.Generation != 7 || committed.ImportedResources != 1 || committed.FreezeWindowMS <= 0 {
				t.Fatalf("activation/commit lost image receipt: %+v", committed)
			}
			terminalBefore, _ := os.ReadFile(markerPath)
			stampMarkerPhase(b, markerPhaseLost, 99, time.Hour)
			terminalAfter, _ := os.ReadFile(markerPath)
			if string(terminalBefore) != string(terminalAfter) {
				t.Fatal("activation overwrote a terminal marker")
			}
			if err := activateImage(b); err != nil {
				t.Fatal("ready image activated twice", err)
			}
			finishBoot(img, exe, testharness.SilentLog(), nil, func(string) error { t.Fatal("ready image rolled back"); return nil })
			if img.Phase() != agentboot.PhaseReady {
				t.Fatal("FinishBoot abandoned a ready image")
			}
			p, err := proc.Get(db, task.pid)
			if err != nil || p.Status != "RUNNING" {
				t.Fatalf("G.1 lost the carried task: %+v %v", p, err)
			}
			if _, ok := b.workloads.Lookup(task.pid); !ok {
				t.Fatal("broker dropped the imported pid")
			}
			if err := syscall.Kill(task.row.OSPID, 0); err != nil {
				t.Fatal("register killed the original child")
			}
			// A new event after register cannot be settled by that register's
			// receipt: its arrival proves the imported agent's courier runs.
			proofPID := proc.NewPID()
			b.courier.enqueueStarted(proofPID, []string{"courier-proof"}, task.scope.ActorFP, readBootID(), 1, time.Now().UTC())
			waitFor(t, bootTestBudget, func() bool { p, err := proc.Get(db, proofPID); return err == nil && p != nil })
		})
	}
	testharness.AssertNoGoroutineLeak(t, "import register x5", gBefore)
	testharness.AssertNoFDLeak(t, "import register x5", fdBefore, 0)
}

// origin: hot-update S3 plan §3.6 T5. Revocation wins before, during and
// after the image cut, including either authoritative session refusal code.
func TestSessionDestroyedTombstoneSurvivesImport(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		for _, arm := range []string{"before", "after", proto.CodeSessionNotFound, proto.CodeSessionNotFoundOrDeleting} {
			t.Run(fmt.Sprintf("%d/%s", round, arm), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				url := startNATS(t)
				a := newBootAgent(t, url, "node")
				task := startBootTask(t, a)
				if arm == "before" {
					revokeSessionWorkloads(a, "session_destroyed")
					waitBootRevoked(t, a, task)
				}
				f := freezeBootAgent(t, a)
				if arm == "after" {
					revokeSessionWorkloads(a, "session_destroyed")
				}
				p, err := f.Prepare() // refresh late revocations, as the real cut does
				if err != nil {
					t.Fatal(err)
				}
				_ = p.Close()
				img, _ := cutBootImage(t, a, f)
				b, err := New(Config{NATSURL: url, SID: "lab", NID: "node", BootImage: img, RemoteFSMode: "off"})
				if err != nil {
					t.Fatal(err)
				}
				cleanupBootAgent(t, b)
				if err := f.Discard(); err != nil {
					t.Fatal(err)
				}
				if err := activateImage(b); err != nil {
					t.Fatal(err)
				}
				if strings.HasPrefix(arm, "session_") {
					nc, err := nats.Connect(url)
					if err != nil {
						t.Fatal(err)
					}
					defer nc.Close()
					if _, err := nc.Subscribe(proto.SubjNodeRegister("lab", "node"), func(msg *nats.Msg) { body, _ := json.Marshal(proto.NodeRegisterResp{Code: arm}); _ = msg.Respond(body) }); err != nil {
						t.Fatal(err)
					}
					if err := nc.FlushTimeout(time.Second); err != nil {
						t.Fatal(err)
					}
					if _, err := b.register(t.Context(), nc); !errors.Is(err, errSessionGone) {
						t.Fatalf("session refusal lost revocation classification: %v", err)
					}
					waitBootRevoked(t, b, task)
					done := runBootAgent(t, b)
					select {
					case err := <-done:
						if !errors.Is(err, errSessionGone) {
							t.Fatalf("Run: %v", err)
						}
					case <-time.After(bootTestBudget):
						t.Fatal("Run did not return on session deletion")
					}
				} else {
					waitBootRevoked(t, b, task)
					revokeSessionWorkloads(b, "session_destroyed")
					waitBootRevoked(t, b, task)
					if _, err := bootCall(b.processRuntime, task, "create"); !errors.Is(err, execsession.ErrRevoked) {
						t.Fatalf("second revoke revived handle: %v", err)
					}
				}
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "revoked import x5", gBefore)
	testharness.AssertNoFDLeak(t, "revoked import x5", fdBefore, 0)
}

func waitBootRevoked(t *testing.T, a *Agent, task bootTask) {
	t.Helper()
	waitFor(t, time.Second, func() bool {
		r, err := bootCall(a.processRuntime, task, "resume")
		return err == nil && r.Phase == "terminal" && strings.HasPrefix(r.Error, "revoked:") && errors.Is(syscall.Kill(task.row.OSPID, 0), syscall.ESRCH)
	})
}

// origin: hot-update S3 plan §1.7/§3.7 T14. A pending host marker only gives
// rollback authority to its transaction proof or original process lineage.
func TestFinishBootRedispatchTable(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	gBefore, fdBefore := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		for _, arm := range []string{"proof", "instance", "unowned", "refused"} {
			t.Run(fmt.Sprintf("%d/%s", round, arm), func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				a := newBootAgent(t, "nats://127.0.0.1:1", "node")
				startBootTask(t, a)
				f := freezeBootAgent(t, a)
				_, resources, err := a.processRuntime.Export()
				if err != nil {
					t.Fatal(err)
				}
				// A sole read end is carried alongside the task. Abandon must
				// close it, proven by EPIPE rather than by the image's phase.
				r, w, err := os.Pipe()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = w.Close() }()
				resources = append(resources, handoff.Resource{ID: "ownership", Kind: "pipe", Files: map[string]*os.File{"read": r}})
				img := holdBootImage(t, f.Bundle(), resources)
				_ = r.Close()
				exe, path, m := pendingBootMarker(t, a.instanceID, 1)
				rememberUpgradeBootProof("unrelated")
				if arm == "proof" || arm == "refused" {
					rememberUpgradeBootProof(m.UpgradeID)
					m.TargetInstance = "different"
				}
				if arm == "unowned" {
					m.TargetInstance = "different"
				}
				t.Cleanup(func() { rememberUpgradeBootProof("") })
				if err := writeUpgradeMarker(path, m); err != nil {
					t.Fatal(err)
				}
				before, _ := os.ReadFile(path)
				if arm == "refused" {
					// Tamper the declared replay identity to refuse after taking
					// part of a task resource, leaving its other files in Restored.
					// FromCut gives the same salvage/remainder split without
					// rewriting a sealed manifest.
					img.Abandon(nil, agentboot.CrashWindowNoRollbackTarget, "replace-fixture")
					cut, src := cutBootImage(t, a, f)
					src.Swap("exec-task."+f.Bundle().Runtime.Tasks[0].PID, "stdout-replay", "stderr-replay")
					img = cut
					if _, err := img.Restore(agentimage.Target{Runtime: processRuntimeConfig(newBootAgent(t, a.cfg.NATSURL, "target"))}); err == nil {
						t.Fatal("swapped resource imported")
					}
					if src.Remaining() == 0 {
						t.Fatal("refusal did not leave a half-taken resource")
					}
				}
				old := bootRedispatchExec
				t.Cleanup(func() { bootRedispatchExec = old })
				var calls atomic.Int32
				bootRedispatchExec = func(p *handoff.Prepared, env []string, _ func(string) error) func(string) error {
					return func(string) error {
						if p == nil {
							t.Fatal("nil redispatch")
						}
						state, _ := bootPreparedState(t)
						bundle, err := agentimage.Decode(state)
						if err != nil || len(bundle.Runtime.Tasks) != 1 {
							t.Fatal("redispatch lost task")
						}
						if arm != "refused" && !strings.Contains(strings.Join(env, "\n"), instanceIDEnv+"="+a.instanceID) {
							t.Fatal("redispatch lost snapshot lineage")
						}
						calls.Add(1)
						return syscall.ENOEXEC
					}
				}
				finishBoot(img, exe, slog.New(slog.DiscardHandler), errors.New("unknown flag: secret"), func(string) error { t.Fatal("bare rollback"); return nil })
				if img.Phase() != agentboot.PhaseLost {
					t.Fatal("failed exec did not explicitly abandon")
				}
				if arm == "unowned" {
					after, _ := os.ReadFile(path)
					if calls.Load() != 0 || string(before) != string(after) {
						t.Fatal("unowned marker was changed or re-dispatched")
					}
					if _, err := w.Write([]byte("x")); !errors.Is(err, syscall.EPIPE) {
						t.Fatalf("abandon did not close the pipe: %v", err)
					}
				} else if calls.Load() != 1 || readMarkerState(t, path).ResourcePhase != markerPhaseRedispatched || readMarkerState(t, path).Detail != "bootstrap failed before control-ready: cobra" {
					t.Fatal("owned marker did not re-dispatch with a sanitized reason")
				}
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "finish boot x5", gBefore)
	testharness.AssertNoFDLeak(t, "finish boot x5", fdBefore, 0)
}

// origin: hot-update S4 plan §2.2 / §7.1. Admission must already be closed
// when New returns; the first subscriber cannot briefly create an unreadable
// rollback image. Reading a still-pending disk marker keeps the gate closed.
func TestPTYAdmissionEstablishedBeforeSubscription(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < leakExerciseRounds; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exe, markerPath := bootFixture(t, "NEW", domainScript("", 1), nil)
			m := testMarker(upgradeStatePending, 1, time.Now().Add(time.Hour))
			m.TargetSID, m.TargetNID = "lab", "node"
			m.NewSHA = mustSHA(t, exe)
			m.PrevSHA = mustSHA(t, upgradePrevPath(exe))
			if err := writeUpgradeMarker(markerPath, m); err != nil {
				t.Fatal(err)
			}
			url := startNATS(t)
			a, err := New(Config{NATSURL: url, SID: "lab", NID: "node", Home: t.TempDir(), RemoteFSMode: "off",
				Logger: testharness.SilentLog(), UpgradeExecutablePath: exe, UpgradeRunningImagePath: exe})
			if err != nil {
				t.Fatal(err)
			}
			cleanupBootAgent(t, a)
			if !errors.Is(checkPTYAdmission(a), procrequest.ErrBusy) ||
				!errors.Is(processRuntimeConfig(a).PTYAdmission(), procrequest.ErrBusy) {
				t.Fatal("New exposed PTY admission before establishing the pending gate")
			}
			nc, err := nats.Connect(url)
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			inbox := "_TINBOX.pending"
			sub, err := nc.SubscribeSync(inbox)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = sub.Unsubscribe() }()
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			// Invalid JSON would report json_parse if the gate were absent;
			// refusing first proves that no allocation or spawn is reached.
			a.handleRunForwarded(nc, &nats.Msg{Reply: inbox, Data: []byte("{")})
			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			var failed proto.RunChunk
			if err := json.Unmarshal(msg.Data, &failed); err != nil {
				t.Fatal(err)
			}
			if failed.Kind != "failed" || failed.Reason != "try_again: agent_upgrading: image switch in progress; retry" || len(a.workloads.Snapshot()) != 0 {
				t.Fatal("legacy fallback bypassed the pending PTY gate")
			}
			refreshPTYAdmission(a) // register succeeded but commit did not reach disk
			if !errors.Is(checkPTYAdmission(a), procrequest.ErrBusy) {
				t.Fatal("register opened admission while marker remained pending")
			}
			// Reading a visible marker cannot prove its directory fsync succeeded.
			// Exercise the transaction owner's durable commit, including the
			// running-image proof, rather than releasing through a refresh.
			// origin: hot-update S4 plan §5.2 / T31.
			a.upgradeBootProofID = m.UpgradeID
			a.commitUpgradeAfterRegister(upgradeStateCommitted)
			if readMarkerState(t, markerPath).State != upgradeStateCommitted {
				t.Fatal("durable commit did not reach the marker")
			}
			refreshPTYAdmission(a)
			if err := checkPTYAdmission(a); err != nil {
				t.Fatalf("durable commit did not open admission: %v", err)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "PTY pending gate x5", beforeG)
	testharness.AssertNoFDLeak(t, "PTY pending gate x5", beforeFD, 0)
}
