package agentupgrade

import (
	"io"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review activation-and-route-migration F10
func TestAgentWatchdogNeverReversesAnImportedReverse(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseReverseWatchdog(t)
	}
	testharness.AssertNoGoroutineLeak(t, "imported reverse watchdog x5", g)
	testharness.AssertNoFDLeak(t, "imported reverse watchdog x5", fd, 0)
}

func exerciseReverseWatchdog(t *testing.T) {
	t.Helper()
	f := importedFixture(t, upgradetx.CutPartialActive)
	view, err := f.store.Load(f.grant.Handle.Key, f.grant.Handle.TxID)
	if err != nil {
		t.Fatal(err)
	}
	fence, request := reverseInputs(f, view)
	r := view.Historical
	r.Reverse, r.Forward = r.Forward, upgradetx.Attempt{}
	r.Reverse.Direction = upgradetx.DirectionReverse
	f.store.records[r.Grant.Handle.TxID] = r
	ports := &Dependencies{Key: r.Key, Store: f.store, Cut: &upgradecut.State{Attempt: fence.Current, Owner: fence.Owner, TargetSHA: fence.RunningSHA},
		WatchdogFence: fence, WatchdogReverse: request, WatchdogDeadline: time.Now().Add(-time.Second).UnixNano()}
	var joined sync.WaitGroup
	ArmWatchdog(t.Context(), ports, f.endpoint, fence.Incarnation, &joined, slog.New(slog.NewTextHandler(io.Discard, nil)))
	joined.Wait()
	if f.store.reverses != 0 || f.store.currentID != r.Grant.Handle.TxID {
		t.Fatal("imported reverse's expired forward deadline started another reverse")
	}
}
