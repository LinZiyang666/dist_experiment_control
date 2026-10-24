//go:build linux

package ptysession

import (
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Real: adopted slave, both backpressured pumps, two cuts, resource salvage,
// activation followed by another current-state cut, and terminal acknowledgement.
// Resource counts are checked while the owner is still alive, before cleanup.
// origin: hot-update S4 plan §4.1 / §5.1 / T18
func TestPtyRepeatedLifecycleReleasesResources(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	requirePTYImplementation(t)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	before := actorFDKinds(t)
	for i := 0; i < 5; i++ {
		t.Run("lifecycle", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseActorLifecycle(t)
		})
		t.Run("close-join", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseActorCloseJoin(t)
		})
	}
	after := actorFDKinds(t)
	for kind, n := range before {
		if after[kind] != n {
			t.Errorf("live owner leaked %s: before=%d after=%d", kind, n, after[kind])
		}
	}
	testharness.AssertNoGoroutineLeak(t, "PTY lifecycle", g0)
	testharness.AssertNoFDLeak(t, "PTY lifecycle", fd0, 0)
}
func exerciseActorLifecycle(t *testing.T) {
	f := startActorFixture(t, `tty.setraw(0)
mark('ready')
wait('drain')
data=b''
while len(data)<65536: data+=os.read(0,65536-len(data))
mark('received',data)
os.write(1,b'LIFECYCLE_END')
wait('exit')
`)
	waitMark(t, f.dir, "ready")
	enqueueActor(t, f.a, inputPayload(65536), true)
	// Each cut exports the current actor. A failed import does not consume any
	// master or replay; salvage then constructs the next frozen generation.
	for generation := 0; generation < 2; generation++ {
		state, _ := freezeActor(t, f.a)
		_, err := f.manager.FreezeUntil(time.Now().Add(actorTestBudget))
		requireDependency(t, err)
		files, err := f.a.Detach()
		if err != nil {
			t.Fatal(err)
		}
		if f.budget.Used() != 0 {
			t.Fatal("detach did not release its reservation")
		}
		row, _ := f.manager.Lookup("terminal")
		master, err := pty.RestoreMaster(files["master"], state.Identity, row)
		requireDependency(t, err)
		// Restore consumes the inherited wrapper on success. Failed actor
		// import must retain the current poller owner, not that stale wrapper.
		// origin: hot-update S4 plan §3.1 / T18
		files["master"] = master.Borrow()
		t.Cleanup(func() { _ = master.Close() })
		in, err := workload.RestoreReplay(f.budget, state.Input, files["input-replay"])
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = in.Close() })
		out, err := workload.RestoreReplay(f.budget, state.Output, files["output-replay"])
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = out.Close() })
		cfg := Config{Manager: f.manager, PID: "terminal", Master: master, Identity: state.Identity, Input: in, Output: out, Clock: func() (int64, error) { return int64(time.Hour), nil }}
		broken := state
		broken.Input.End++
		if _, err := Import(cfg, broken); err == nil {
			t.Fatal("mismatched image imported")
		}
		for name, file := range files {
			if _, err := file.Stat(); err != nil {
				t.Fatalf("failed import consumed %s: %v", name, err)
			}
		}
		next, err := Import(cfg, state)
		if err != nil {
			t.Fatal(err)
		}
		f.a = next
		if f.budget.Used() != state.Input.Capacity+state.Output.Capacity {
			t.Fatal("wrong import budget")
		}
		requireDependency(t, next.PreActivate())
		f.manager.Thaw()
		requireDependency(t, next.Activate())
		if _, err := next.Detach(); err == nil {
			t.Fatal("active actor detached stale image")
		}
	}
	markActor(t, f.dir, "drain")
	waitMark(t, f.dir, "received")
	requireFileBytes(t, f.dir+"/received", inputPayload(65536))
	waitOutput(t, f.a, "LIFECYCLE_END")
	markActor(t, f.dir, "exit")
	awaitActor(t, "terminal source FIN", func() bool { f.a.mu.Lock(); defer f.a.mu.Unlock(); return f.a.source == sourceEnded })
	if _, err := f.manager.Wait("terminal"); err != nil {
		t.Fatal(err)
	}
	batch, err := f.a.OutputBatch(0, false, 32768)
	if err != nil || !batch.FIN {
		t.Fatalf("terminal output %+v %v", batch, err)
	}
	if _, err := f.a.OutputBatch(batch.Offset+uint64(len(batch.Data)), true, 1); err != nil {
		t.Fatal(err)
	}
	if err := f.a.Close(); err != nil {
		t.Fatal(err)
	}
	if f.budget.Used() != 0 {
		t.Fatal("terminal ACK leaked replay budget")
	}
	if counts := actorFDKinds(t); counts["master"] != 0 || counts["replay"] != 0 {
		t.Fatalf("live owner retained terminal descriptors: %+v", counts)
	}
}

// Close must join the last read accounting step before retiring its replays.
// A barrier on a real n>0 return makes a missing join observable deterministically.
// origin: hot-update S4 plan §4.1 / T18
func exerciseActorCloseJoin(t *testing.T) {
	f := startActorFixture(t, "tty.setraw(0)\nmark('ready')\nwait('write')\nos.write(1,b'CLOSE_BARRIER')\nwait('exit')\n")
	waitMark(t, f.dir, "ready")
	m := instrumentActor(t, f.a)
	freezeActor(t, f.a)
	release := make(chan struct{})
	defer func() {
		if !stopped(release) {
			close(release)
		}
	}()
	m.readReturned = make(chan struct{}, 1)
	m.releaseRead = release
	activateActor(t, f.a)
	markActor(t, f.dir, "write")
	select {
	case <-m.readReturned:
	case <-time.After(actorTestBudget):
		t.Fatal("no read before close barrier")
	}
	done := make(chan error, 1)
	go func() { done <- f.a.Close() }()
	select {
	case err := <-done:
		t.Fatalf("Close retired replay before reader joined: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if f.budget.Used() == 0 {
		t.Fatal("replay budget released before read join")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if f.budget.Used() != 0 {
		t.Fatal("joined close retained replay budget")
	}
}
