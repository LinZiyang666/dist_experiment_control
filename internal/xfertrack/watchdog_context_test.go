package xfertrack

import (
	"context"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
)

// watchdog_context_test.go — the positive control for the legacy watchdog's
// context hygiene, run against the implementation where it now lives.
//
// internal/broker/transfer_watchdog_test.go's TestWatchdogReleasesItsChildContextOnEveryExit
// pins the INVARIANT in the source (a deferred cancel on the watchdog
// goroutine's first line) and follows the implementation into this package.
// Its positive control used to arm watchdogs through the broker method and
// count the parent's children; after the move the broker adapter hands its
// runCtx to this package only as a Stopped channel, so that count is zero by
// construction and can no longer show that the reflection probe and the
// scanned function describe the same mechanism. This test does, on the real
// moved function: arming adds exactly one child per watchdog to the context it
// is parented on, and the returned cancel removes it.

// childCount reports how many children a cancelCtx currently holds, and whether the
// count could be read at all. It reaches into runtime internals deliberately and
// reports failure rather than guessing, so a toolchain change retires the test loudly
// instead of turning it green.
func childCount(ctx context.Context) (int, bool) {
	v := reflect.ValueOf(ctx)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return 0, false
	}
	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return 0, false
	}
	f := v.FieldByName("children")
	if !f.IsValid() || f.Kind() != reflect.Map {
		return 0, false
	}
	return f.Len(), true
}

// origin: prerelease audit broker-transfer/BT-F1; hot-update S7 plan §11.4 / P31 (watchdog moved to xfertrack)
func TestLegacyWatchdogChildContextAccountingIsObservable(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	base, ok := childCount(parent)
	if !ok {
		t.Skip("context.cancelCtx.children is not readable on this toolchain; " +
			"TestWatchdogReleasesItsChildContextOnEveryExit has lost its positive control")
	}
	w := &legacyWatchdog{transfers: newTransferTracker(), cfg: LegacyWatchdogHooks{
		Now: time.Now, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Finalize: func(*LegacyEntry, schema.AuditTransfer, bool) {
			t.Error("a tier-A watchdog fired inside this test; its 30s budget makes that impossible")
		},
	}}
	const n = 8
	cancels := make([]context.CancelFunc, 0, n)
	for i := 0; i < n; i++ {
		cancels = append(cancels, w.startTransferWatchdog(parent, NewLegacyEntry(LegacyRecord{
			TransferID: "tid-" + string(rune('a'+i)), SID: "lab", NID: "lab-1",
			Verb: "push", Tier: "a", StartedAt: time.Now(),
		})))
	}
	if got, _ := childCount(parent); got != base+n {
		t.Fatalf("arming %d watchdogs added %d children, want %d.\n\n"+
			"If arming does not show up here then the accounting the source guard reasons about is not the "+
			"accounting the runtime does, and that guard means nothing.",
			n, got-base, n)
	}
	for _, c := range cancels {
		c()
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		got, _ := childCount(parent)
		if got == base {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after cancelling all %d watchdogs the parent still holds %d extra child(ren).\n\n"+
				"A child context is removed from its parent ONLY by cancel, so anything left is left for "+
				"the life of whatever owns the parent.", n, got-base)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
