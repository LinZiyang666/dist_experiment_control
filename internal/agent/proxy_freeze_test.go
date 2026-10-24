//go:build linux

package agent

import (
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// origin: hot-update S5 internal review HOTUPDATE-T44/T45
func TestHeartbeatKeepsFrozenProxyOwner(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseFrozenProxyHeartbeat(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "frozen proxy heartbeat x5", beforeG)
	testharness.AssertNoFDLeak(t, "frozen proxy heartbeat x5", beforeFD, 0)
}

func exerciseFrozenProxyHeartbeat(t *testing.T) {
	t.Helper()
	a := newProxyTestAgent(t)
	enableProxyForTest(t, a, 7)
	a.proxy.mu.Lock()
	original := a.proxy.srv
	a.proxy.mu.Unlock()
	a.gate.close()
	defer a.gate.open()
	frozen, err := original.Freeze(time.Now().Add(upgradebudget.SafePointWait))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := frozen.Discard(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if original.Serving() {
		t.Fatal("freeze did not exercise the stopped accept-worker state")
	}
	reapProxyCorpseOnHeartbeat(a, nil)
	a.proxy.mu.Lock()
	same := a.proxy.srv == original
	a.proxy.mu.Unlock()
	if !same {
		t.Fatal("heartbeat reaped the intentionally frozen SS owner")
	}
	if _, _, err := frozen.Export(); err != nil {
		t.Fatalf("heartbeat destroyed the frozen descriptor closure: %v", err)
	}
}
