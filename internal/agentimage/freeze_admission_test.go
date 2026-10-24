//go:build linux

package agentimage

import (
	"errors"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: hot-update S4 internal review rv-fd rv-fd-3 / rv-fd-V1
func TestFreezeRollbackIncludesManagerActivationGuard(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			f := newBundleConfig(t, "rollback-guard")
			var armed atomic.Bool
			begin, entered, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			f.cfg.Clock = func() (int64, error) {
				if armed.CompareAndSwap(true, false) {
					close(begin)
					<-entered
				}
				return procrequest.BootNow()
			}
			var err error
			f.rt, err = execsession.New(f.cfg)
			if err != nil {
				t.Fatal(err)
			}
			h, pid := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
			f.awaitRunning(t, h)
			controlDone := make(chan error, 1)
			go func() {
				<-begin // Runtime clock proves the read-only precheck already passed.
				controlDone <- f.manager.WithControl(pid, func(workload.Control) error {
					close(entered)
					<-release
					return nil
				})
			}()
			armed.Store(true)
			done := make(chan error, 1)
			go func() {
				_, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager}, time.Now().Add(50*time.Millisecond))
				done <- err
			}()
			late := false
			select {
			case err = <-done:
			case <-time.After(300 * time.Millisecond):
				late = true
			}
			close(release)
			if cerr := <-controlDone; cerr != nil {
				t.Fatal(cerr)
			}
			if late {
				err = <-done
			}
			if late || !errors.Is(err, execsession.ErrRollbackPending) {
				t.Fatalf("rollback guard exceeded deadline or hid recovery: late=%t err=%v", late, err)
			}
			// Handle waits for the owned recovery acknowledgement, then serves the
			// same task. It must never observe a false successful frozen export.
			f.awaitRunning(t, h)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "composed rollback guard", beforeG)
	testharness.AssertNoFDLeak(t, "composed rollback guard", beforeFD, 0)
}

// origin: hot-update S4 internal review rv-fd rv-fd-3
func TestFreezeDeadlineIncludesProcessOwnershipPrecheck(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFreezeDeadlineIncludesProcessOwnershipPrecheck(t, 30*time.Millisecond)
			exerciseFreezeDeadlineIncludesProcessOwnershipPrecheck(t, 50*time.Millisecond)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeDeadlineIncludesProcessOwnershipPrecheck", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeDeadlineIncludesProcessOwnershipPrecheck", beforeFD, 0)
}

func exerciseFreezeDeadlineIncludesProcessOwnershipPrecheck(t *testing.T, budget time.Duration) {
	f := newBundleFixture(t, "guard-deadline")
	h, pid := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, nil)
	f.awaitRunning(t, h)
	entered, release := make(chan struct{}), make(chan struct{})
	controlDone := make(chan error, 1)
	go func() {
		controlDone <- f.manager.WithControl(pid, func(workload.Control) error {
			close(entered)
			<-release
			return nil
		})
	}()
	select {
	case <-entered:
	case err := <-controlDone:
		t.Fatalf("control was not admitted: %v", err)
	case <-time.After(bundleTestBudget):
		t.Fatal("control fixture never acquired Manager guard")
	}
	result := make(chan error, 1)
	deadline := time.Now().Add(budget)
	go func() {
		_, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager}, deadline)
		result <- err
	}()
	late := false
	select {
	case err := <-result:
		if err == nil {
			t.Error("cut succeeded with busy Manager guard")
		}
	case <-time.After(250 * time.Millisecond):
		late = true
	}
	close(release)
	if err := <-controlDone; err != nil {
		t.Fatal(err)
	}
	if late {
		<-result // join the late call before runtime cleanup
		t.Fatalf("Freeze ignored its %s deadline in Manager.Snapshot ownership precheck; still blocked after 250ms", budget)
	}
}
