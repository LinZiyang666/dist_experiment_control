//go:build linux

package ptysession

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/testharness"
)

type recoveryLockMaster struct {
	pty.Master
	entered, release chan struct{}
	once             sync.Once
}

func (m *recoveryLockMaster) SetReadDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		m.once.Do(func() {
			close(m.entered)
			<-m.release
		})
	}
	return m.Master.SetReadDeadline(deadline)
}

// origin: hot-update S4 internal review rv-fd rv-fd-3
func TestFreezeDeadlineIncludesTimeoutRecoveryLock(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseFreezeDeadlineIncludesTimeoutRecoveryLock(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestFreezeDeadlineIncludesTimeoutRecoveryLock", beforeG)
	testharness.AssertNoFDLeak(t, "TestFreezeDeadlineIncludesTimeoutRecoveryLock", beforeFD, 0)
}

func exerciseFreezeDeadlineIncludesTimeoutRecoveryLock(t *testing.T) {
	f := startActorFixture(t, "tty.setraw(0)\nmark('ready')\ndata=os.read(0,32)\nos.write(1,data)\nwait('exit')\n")
	waitMark(t, f.dir, "ready")
	freezeActor(t, f.a)
	m := &recoveryLockMaster{Master: f.a.cfg.Master, entered: make(chan struct{}), release: make(chan struct{})}
	f.a.cfg.Master = m
	activateActor(t, f.a)
	result := make(chan error, 1)
	go func() {
		_, _, err := f.a.Freeze(time.Now().Add(30 * time.Millisecond))
		result <- err
	}()
	select {
	case <-m.entered:
	case <-time.After(actorTestBudget):
		t.Fatal("freeze never paused its admitted generation")
	}
	// Contention begins AFTER the deadline-aware admission lock succeeded.
	// The real master deadline hook only positions the competing lock holder.
	f.a.mu.Lock()
	close(m.release)
	late := false
	var err error
	select {
	case err = <-result:
	case <-time.After(300 * time.Millisecond):
		late = true
	}
	f.a.mu.Unlock()
	if late {
		err = <-result // join before fixture teardown
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected deadline refusal: %v", err)
	}
	if err := f.a.ConfirmRunning(time.Now().Add(actorTestBudget)); err != nil {
		t.Fatal(err)
	}
	if _, err := f.a.AcceptInput(0, []byte("resumed"), false); err != nil {
		t.Fatal(err)
	}
	awaitActor(t, "both directions resumed after old accounting", func() bool {
		out, err := f.a.OutputBatch(0, false, 32)
		return err == nil && string(out.Data) == "resumed" && f.a.InputStatus().Applied == 7
	})
	if late {
		t.Fatal("Freeze timed out but blocked on resume's unbounded accounting lock for over 300ms")
	}
}
