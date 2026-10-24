//go:build linux

package execsession

import (
	"errors"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S4 internal review rv-fd rv-fd-2
func TestPartialActivationRetryKeepsAlreadyActiveActors(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := range 5 {
		t.Run(strconv.Itoa(round), func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exercisePartialActivationRetryKeepsAlreadyActiveActors(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "TestPartialActivationRetryKeepsAlreadyActiveActors", beforeG)
	testharness.AssertNoFDLeak(t, "TestPartialActivationRetryKeepsAlreadyActiveActors", beforeFD, 0)
}

func exercisePartialActivationRetryKeepsAlreadyActiveActors(t *testing.T) {
	f := newPTYRuntime(t, 0)
	for _, request := range []string{"first", "second"} {
		q, _ := ptyCreate(t, f, request, []string{"/bin/sh", "-c", "stty raw -echo; printf ready; read line"})
		ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
			return p.Stdout != nil && strings.Contains(string(p.Stdout.Data), "ready")
		})
	}
	imageFreeze(t, f.r)
	failure := errors.New("transient second activation failure")
	calls := 0
	f.r.activatePTY = func(a *ptysession.Actor) error {
		calls++
		if calls == 2 {
			return failure
		}
		return a.Activate()
	}
	if err := f.r.Thaw(); !errors.Is(err, failure) || calls != 2 {
		t.Fatalf("partial activation fixture: calls=%d err=%v", calls, err)
	}
	f.r.activatePTY = nil
	if err := f.r.Thaw(); err != nil {
		t.Fatalf("retry after transient failure rejected an already-active actor: %v", err)
	}
	f.close(t)
}
