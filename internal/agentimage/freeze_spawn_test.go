//go:build linux

package agentimage

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/spawnexec"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// The barrier is inside the real helper's spawn window, after slot acquisition.
// Runtime must carry the eventual child, rather than mistake this bounded work
// for an abandoned helper or freeze the process table before adoption.
func spawnBundleFixture(t *testing.T, entered chan<- string, proceed <-chan struct{}) (*bundleFixture, *spawnsafe.Policy) {
	t.Helper()
	return spawnBundleFixtureWithAdoptionBarrier(t, entered, proceed, nil)
}

func spawnBundleFixtureWithAdoptionBarrier(t *testing.T, entered chan<- string, proceed <-chan struct{}, beforeAdopt func()) (*bundleFixture, *spawnsafe.Policy) {
	t.Helper()
	f := newBundleConfig(t, "spawn-cut")
	policy, err := spawnsafe.New(spawnsafe.Config{Mode: spawnsafe.ModeOff, MountSource: func() ([]byte, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	f.cfg.Start = func(pid string, req *proto.ExecReq) (*workload.Stdio, string, error) {
		stdio, err := workload.NewStdio()
		if err != nil {
			return nil, "", err
		}
		target := exec.Command(req.Argv[0], req.Argv[1:]...)
		target.Env = handoff.CleanEnvironment(os.Environ())
		stdio.Bind(target)
		cmd, handshake, err := spawnexec.Prepare(target)
		if err != nil {
			stdio.Close()
			return nil, "", err
		}
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		err = policy.RunStartWithCleanup(func() error {
			entered <- pid
			<-proceed
			return handshake.Start(cmd)
		}, bundleTestBudget, handshake.Cancel, func(error) {
			if cmd.Process != nil {
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				_ = cmd.Wait()
			}
		})
		if err != nil {
			stdio.Close()
			return nil, "", err
		}
		stdio.CloseChild()
		if beforeAdopt != nil {
			beforeAdopt()
		}
		if err := f.manager.AdoptDraining(workload.Identity{ID: pid, OSPID: cmd.Process.Pid, StartedAt: time.Now()}, cmd.Process); err != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			_ = cmd.Wait()
			stdio.Close()
			return nil, "", err
		}
		f.mu.Lock()
		f.spawned++
		f.mu.Unlock()
		return stdio, "", nil
	}
	f.rt, err = execsession.New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	return f, policy
}

type spawnCutResult struct {
	cut *Frozen
	err error
}

func beginSpawnCut(f *bundleFixture, policy *spawnsafe.Policy) <-chan spawnCutResult {
	done := make(chan spawnCutResult, 1)
	go func() {
		cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy}, time.Now().Add(bundleTestBudget))
		done <- spawnCutResult{cut, err}
	}()
	return done
}

func finishSpawnCut(t *testing.T, result <-chan spawnCutResult, pid string) {
	t.Helper()
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatalf("cut rejected an in-time spawn: %v", got.err)
		}
		thaw := sync.OnceFunc(func() {
			if err := got.cut.Thaw(); err != nil {
				t.Error(err)
			}
		})
		defer thaw()
		found := false
		for _, task := range got.cut.Bundle().Runtime.Tasks {
			found = found || task.PID == pid
		}
		if !found {
			t.Fatalf("cut did not image the completed spawn %s", pid)
		}
		thaw()
		if !got.cut.owners.Spawn.TryAcquireSpawnSlot() {
			t.Fatal("successful rollback did not reopen spawn admission")
		}
		got.cut.owners.Spawn.ReleaseSpawnSlot()
	case <-time.After(bundleTestBudget):
		t.Fatal("cut did not finish after releasing Start")
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT test 1 (drill 36.A).
func TestFreezeWaitsForInTimeSpawnAndImagesChild(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			entered, proceed := make(chan string, 1), make(chan struct{})
			release := sync.OnceFunc(func() { close(proceed) })
			f, policy := spawnBundleFixture(t, entered, proceed)
			t.Cleanup(release)
			h, pid := f.create(t, "blocked", []string{"/bin/sh", "-c", "printf carried; exit 23"}, nil)
			select {
			case <-entered:
			case <-time.After(bundleTestBudget):
				t.Fatal("Start did not acquire its slot")
			}
			result := beginSpawnCut(f, policy)
			select {
			case got := <-result:
				t.Fatalf("cut returned before Start completed: %v", got.err)
			case <-time.After(30 * time.Millisecond):
				// Negative assertion: the barrier, not elapsed time, holds Start.
			}
			release()
			finishSpawnCut(t, result, pid)
			out, reply := f.drain(t, h)
			if out != "carried" || reply.ExitCode != 23 || f.spawned != 1 {
				t.Fatalf("spawn result: output=%q rc=%d starts=%d", out, reply.ExitCode, f.spawned)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeWaitsForInTimeSpawnAndImagesChild", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeWaitsForInTimeSpawnAndImagesChild", beforeFD, 0)
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT test 2.
func TestFreezeStillDefersOnAbandonedSpawn(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f := newBundleFixture(t, "abandoned-cut")
			policy, err := spawnsafe.New(spawnsafe.Config{MountSource: func() ([]byte, error) { return nil, nil }})
			if err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			t.Cleanup(func() {
				close(release)
				if !testharness.WaitFor(t, bundleTestBudget, time.Millisecond, func() bool { return policy.WedgedCount() == 0 }) {
					t.Error("abandoned reaper retained its slot")
				}
			})
			if err := policy.RunStart(func() error { <-release; return nil }, time.Millisecond); !errors.Is(err, spawnsafe.ErrSpawnTimeout) {
				t.Fatal(err)
			}
			cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy}, time.Now().Add(bundleTestBudget))
			requireDeferred(t, err, ReasonSpawnInFlight)
			if cut != nil || f.rt.Frozen() || f.manager.Frozen() {
				t.Fatal("abandoned spawn froze an owner")
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeStillDefersOnAbandonedSpawn", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeStillDefersOnAbandonedSpawn", beforeFD, 0)
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT tests 1 / 4.
// A slot outside Runtime has no registry starting record to join. It must be
// drained too, and acquisition must remain closed after export until rollback.
func TestFreezeDrainsNonRuntimeSlotsBeforeExport(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFreezeNonRuntimeDrain(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeDrainsNonRuntimeSlotsBeforeExport", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeDrainsNonRuntimeSlotsBeforeExport", beforeFD, 0)
}

func exerciseFreezeNonRuntimeDrain(t *testing.T) {
	f := newBundleFixture(t, "external-slot")
	policy, err := spawnsafe.New(spawnsafe.Config{MountSource: func() ([]byte, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if !policy.TryAcquireSpawnSlot() {
		t.Fatal("initial slot refused")
	}
	release := sync.OnceFunc(policy.ReleaseSpawnSlot)
	t.Cleanup(release)
	result := beginSpawnCut(f, policy)
	select {
	case got := <-result:
		t.Fatalf("cut did not drain an external slot: %v", got.err)
	case <-time.After(30 * time.Millisecond):
	}
	release()
	select {
	case got := <-result:
		if got.err != nil {
			t.Fatal(got.err)
		}
		t.Cleanup(func() {
			if err := got.cut.Thaw(); err != nil {
				t.Error(err)
			}
		})
		if policy.TryAcquireSpawnSlot() {
			policy.ReleaseSpawnSlot()
			t.Fatal("a slot started after the cut completed its drain")
		}
	case <-time.After(bundleTestBudget):
		t.Fatal("external drain did not finish")
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT absolute cut deadline.
func TestFreezeSpawnDrainDeadlineRollsBackRuntime(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFreezeSpawnDrainRollback(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeSpawnDrainDeadlineRollsBackRuntime", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeSpawnDrainDeadlineRollsBackRuntime", beforeFD, 0)
}

func exerciseFreezeSpawnDrainRollback(t *testing.T) {
	f := newBundleFixture(t, "spawn-deadline")
	policy, err := spawnsafe.New(spawnsafe.Config{MountSource: func() ([]byte, error) { return nil, nil }})
	if err != nil {
		t.Fatal(err)
	}
	if !policy.TryAcquireSpawnSlot() {
		t.Fatal("external slot refused")
	}
	release := sync.OnceFunc(policy.ReleaseSpawnSlot)
	t.Cleanup(release)
	result := make(chan error, 1)
	go func() {
		_, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy}, time.Now().Add(50*time.Millisecond))
		result <- err
	}()
	select {
	case err := <-result:
		requireDeferred(t, err, ReasonSpawnInFlight)
	case <-time.After(300 * time.Millisecond):
		release()
		<-result
		t.Fatal("spawn drain extended the absolute cut deadline")
	}
	// At the expired deadline, ThawUntil may report retained recovery rather
	// than finish synchronously. Its acknowledgement must still reopen Runtime.
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return !f.rt.Frozen() }) {
		t.Fatal("drain timeout stranded the frozen Runtime without rollback")
	}
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		if !policy.TryAcquireSpawnSlot() {
			return false
		}
		policy.ReleaseSpawnSlot()
		return true
	}) {
		t.Fatal("drain timeout stranded policy admission")
	}
	release()
	h, _ := f.create(t, "after-timeout", []string{"/bin/sh", "-c", "printf recovered"}, nil)
	out, reply := f.drain(t, h)
	if out != "recovered" || reply.ExitCode != 0 || f.spawned != 1 {
		t.Fatalf("post-timeout request: output=%q rc=%d starts=%d", out, reply.ExitCode, f.spawned)
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT test 4.
func TestFreezeCreationRacingDrainWaitsUntilThaw(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFreezeCreationRacingDrain(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeCreationRacingDrainWaitsUntilThaw", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeCreationRacingDrainWaitsUntilThaw", beforeFD, 0)
}

func exerciseFreezeCreationRacingDrain(t *testing.T) {
	entered, proceed := make(chan string, 1), make(chan struct{})
	f, policy := spawnBundleFixture(t, entered, proceed)
	t.Cleanup(func() { close(proceed) })
	probe, err := f.handle(proto.ProcessResumeReq{Op: "probe", ClientLineage: f.scope.ClientLineage})
	if err != nil || probe.Lease == nil {
		t.Fatal(err)
	}
	h := proto.ProcessHandle{Lease: *probe.Lease, RequestID: "racing-drain"}
	if !policy.TryAcquireSpawnSlot() {
		t.Fatal("external slot refused")
	}
	release := sync.OnceFunc(policy.ReleaseSpawnSlot)
	t.Cleanup(release)
	result := beginSpawnCut(f, policy)
	if !testharness.WaitFor(t, bundleTestBudget, time.Millisecond, f.rt.Frozen) {
		t.Fatal("Runtime did not close creation admission before the policy drain")
	}
	created := make(chan struct{})
	var createErr error
	go func() {
		_, err := f.handle(proto.ProcessResumeReq{Op: "create", Handle: &h, AttachEpoch: 1,
			Exec: &proto.ExecReq{Argv: []string{"/bin/sh", "-c", "printf racing; exit 23"}}})
		createErr = err
		close(created)
	}()
	// Cleanup must open both gates and join the caller before closing Runtime.
	t.Cleanup(func() { policy.Thaw(); <-created })
	select {
	case <-entered:
		t.Fatal("racing Runtime creation started during the policy drain")
	case <-created:
		t.Fatalf("racing creation returned before thaw: %v", createErr)
	case <-time.After(20 * time.Millisecond):
	}
	release()
	got := <-result
	if got.err != nil {
		t.Fatal(got.err)
	}
	thaw := sync.OnceFunc(func() {
		if err := got.cut.Thaw(); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(thaw)
	if len(got.cut.Bundle().Runtime.Tasks) != 0 {
		t.Fatal("the racing creation crossed the closed Runtime admission")
	}
	select {
	case <-entered:
		t.Fatal("racing creation started after the drain and before thaw")
	case <-created:
		t.Fatalf("racing creation returned after drain and before thaw: %v", createErr)
	case <-time.After(20 * time.Millisecond):
	}
	thaw()
	select {
	case <-entered:
	case <-time.After(bundleTestBudget):
		t.Fatal("queued Runtime creation did not resume after thaw")
	}
	proceed <- struct{}{}
	// Join Handle before using fixture's sequential CallID counter again.
	<-created
	if createErr != nil {
		t.Fatal(createErr)
	}
	out, reply := f.drain(t, h)
	if out != "racing" || reply.ExitCode != 23 || f.spawned != 1 {
		t.Fatalf("queued request: output=%q rc=%d starts=%d", out, reply.ExitCode, f.spawned)
	}
}

// origin: hot-update S4 plan §4.1 / SPAWN-INFLIGHT test 3 (drill 36.A).
func TestFreezeUnderContinuousCreationCompletesEveryRequestOnce(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			entered, proceed := make(chan string, 4), make(chan struct{})
			f, policy := spawnBundleFixture(t, entered, proceed)
			ledger := filepath.Join(t.TempDir(), "effects")
			done, stop := make(chan struct{}), make(chan struct{})
			imaged := make(chan struct{})
			t.Cleanup(func() { close(stop); close(proceed); <-done })
			go func() {
				defer close(done)
				for request := range 4 {
					select {
					case <-stop:
						return
					default:
					}
					token := fmt.Sprintf("request-%d", request)
					h, _ := f.create(t, token, []string{"/bin/sh", "-c", `printf '%s\n' "$1" >> "$2"; printf '%s' "$1"; exit 23`, "sh", token, ledger}, nil)
					// Keep the real child unacknowledged until this cut has proved
					// ownership. Attach operations intentionally run while Runtime
					// quiesces; terminal-ack could otherwise legitimately retire
					// this short-lived task before the next freeze poll.
					// origin: hot-update S5 plan §5.3 / S4 SPAWN-INFLIGHT test 3
					select {
					case <-imaged:
					case <-stop:
						return
					}
					out, reply := f.drain(t, h)
					if out != token || reply.ExitCode != 23 {
						t.Errorf("request %s: output=%q rc=%d", token, out, reply.ExitCode)
						return
					}
				}
			}()
			for cut := range 4 {
				var pid string
				select {
				case pid = <-entered:
				case <-done:
					t.Fatal("producer stopped before all cuts")
				case <-time.After(bundleTestBudget):
					t.Fatal("producer did not enter its next spawn window")
				}
				result := beginSpawnCut(f, policy)
				select {
				case got := <-result:
					t.Fatalf("cut %d deferred or skipped an in-time spawn: %v", cut, got.err)
				case <-time.After(30 * time.Millisecond):
				}
				proceed <- struct{}{}
				finishSpawnCut(t, result, pid)
				imaged <- struct{}{}
			}
			<-done
			data, err := os.ReadFile(ledger)
			if err != nil || strings.TrimSpace(string(data)) != "request-0\nrequest-1\nrequest-2\nrequest-3" || f.spawned != 4 {
				t.Fatalf("exactly-once effects: ledger=%q starts=%d err=%v", data, f.spawned, err)
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeUnderContinuousCreationCompletesEveryRequestOnce", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeUnderContinuousCreationCompletesEveryRequestOnce", beforeFD, 0)
}
