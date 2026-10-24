package upgradestore

import (
	"errors"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review cluster-roll F3
func TestPreparedQueriesPreserveInertAuthorization(t *testing.T) {
	s, req := newFixture(t)
	g := prepare(t, s, req)
	s = reopen(t, s)
	view, err := s.Prepared(g.Handle.Key, g.Handle.TxID)
	if err != nil || view.Historical.Grant != g || view.Historical.Forward.Phase != upgradetx.PhasePrepared || len(view.Historical.Forward.Scope.Expected) != 1 {
		t.Fatalf("original prepared authorization lost: %v %+v", err, view)
	}
	if _, err = s.Load(g.Handle.Key, g.Handle.TxID); err == nil {
		t.Fatal("query accepted inert authorization")
	}
	wrong := g.Handle.Key
	wrong.UID++
	if _, err = s.Prepared(wrong, g.Handle.TxID); err == nil {
		t.Fatal("query crossed component identity")
	}
	if _, err = s.Prepared(g.Handle.Key, strings.Repeat("0", 32)); err == nil {
		t.Fatal("query invented an unknown grant")
	}
	start(t, s, g)
	if _, err = s.Prepared(g.Handle.Key, g.Handle.TxID); err == nil {
		t.Fatal("accepted execution returned as inert")
	}
}

// origin: S8 internal review cluster-roll F10
func TestParentBindingReadDoesNotRelockTheHeldGuard(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseParentBindingGuard(t)
	}
	testharness.AssertNoGoroutineLeak(t, "parent binding under held guard", g)
	testharness.AssertNoFDLeak(t, "parent binding under held guard", fd, 0)
}
func exerciseParentBindingGuard(t *testing.T) {
	t.Helper()
	s, req := newFixture(t)
	s.now = time.Now
	req.ClientNonce = strings.Repeat("8", 32)
	req.ExpiresAt = time.Now().Add(time.Minute).UnixNano()
	g := prepare(t, s, req)
	start(t, s, g)
	guard, err := os.OpenFile(s.options.GuardPath, os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if err = syscall.Flock(int(guard.Fd()), syscall.LOCK_EX); err != nil {
		_ = guard.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		deadline, err := ReadParentDeadline(s.options.Path, g.Handle.Key, g.ClientNonce, g.CapabilityDigest)
		if err == nil && deadline != g.ExpiresAt {
			err = errors.New("deadline changed")
		}
		done <- err
	}()
	select {
	case err := <-done:
		_ = guard.Close()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		_ = guard.Close()
		<-done
		t.Fatal("parent lookup tried to acquire the guard already held by the child writer")
	}
	for _, mutation := range []struct{ parent, slot string }{{"other", g.CapabilityDigest}, {g.ClientNonce, "other"}} {
		if _, err := ReadParentDeadline(s.options.Path, g.Handle.Key, mutation.parent, mutation.slot); err == nil {
			t.Fatal("foreign parent/slot returned a deadline")
		}
	}
}
