package broker

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// transfer_watchdog_test.go — the per-transfer watchdog's context hygiene.
//
// origin: prerelease audit broker-transfer/BT-F1.
//
// startTransferWatchdog parents a child context on b.runCtx, which lives as long as
// the broker process. Every ordinary terminal path cancels it. The path where the
// watchdog itself FIRES did not, and the comment beside that call asserted "calling
// it here is a no-op" — true of closing ctx.Done(), false of the thing that leaks: a
// child stays in its parent's children map until it is cancelled. So every timed-out
// transfer left behind a cancelCtx that nothing could ever collect.
//
// WHAT THE TWO TESTS BELOW PROVE, STATED PLAINLY.
//
// The tier-A watchdog budget is a fixed 30 seconds and there is no clock seam on the
// timer, so a test that waits for a real firing costs 30s of every `make test`. That
// price is not worth paying for ~200 bytes per fired watchdog, and pretending
// otherwise would be the more expensive mistake. Instead:
//
//	TestWatchdogReleasesItsChildContextOnEveryExit pins the INVARIANT in the source —
//	a deferred cancel on the goroutine's first line, which covers the firing path, the
//	two early returns and any exit added later. It cannot prove the leak is gone.
//
//	TestParentChildAccountingIsObservable is its positive control. It proves the
//	reflection probe actually sees the children map and that a cancel actually removes
//	an entry from it, so the paragraph above is describing a real mechanism rather than
//	a story about one. Without this the first test is an assertion about text.

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

func TestParentChildAccountingIsObservable(t *testing.T) {
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	base, ok := childCount(parent)
	if !ok {
		t.Skip("context.cancelCtx.children is not readable on this toolchain; " +
			"TestWatchdogReleasesItsChildContextOnEveryExit has lost its positive control")
	}

	b := &Broker{transfers: newTransferTracker(), cfg: Config{Logger: silentLogger(), Now: time.Now}}
	const n = 8
	cancels := make([]context.CancelFunc, 0, n)
	for i := 0; i < n; i++ {
		cancels = append(cancels, b.startTransferWatchdog(parent, &transferEntry{
			transferID: "tid-" + string(rune('a'+i)), sid: "lab", nid: "lab-1",
			verb: "push", tier: "a", startedAt: time.Now(),
		}))
	}
	if got, _ := childCount(parent); got != base+n {
		t.Fatalf("arming %d watchdogs added %d children, want %d.\n\n"+
			"If arming does not show up here then the accounting this file reasons about is not the "+
			"accounting the runtime does, and the source guard beside it means nothing.",
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
				"A child context is removed from its parent ONLY by cancel. The parent here is the "+
				"broker's runCtx, which lives for the whole process, so anything left is left forever.",
				n, got-base)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWatchdogReleasesItsChildContextOnEveryExit(t *testing.T) {
	src, err := os.ReadFile("transfer.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	fset := token.NewFileSet()
	f, perr := parser.ParseFile(fset, "transfer.go", src, 0)
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}

	var fn *ast.FuncDecl
	ast.Inspect(f, func(n ast.Node) bool {
		d, ok := n.(*ast.FuncDecl)
		if ok && d.Name.Name == "startTransferWatchdog" {
			fn = d
			return false
		}
		return true
	})
	if fn == nil {
		t.Fatal("SELF-CHECK FAILED: startTransferWatchdog not found in transfer.go — this guard is " +
			"scanning for a function that no longer exists, so it can never report anything")
	}

	// Find the `go func() { ... }()` and require its FIRST statement to be a deferred
	// cancel. First, not merely present: a cancel placed after the select would miss
	// the two early returns, and one placed at the end would miss every future one.
	var body *ast.BlockStmt
	ast.Inspect(fn, func(n ast.Node) bool {
		if body != nil {
			return false
		}
		g, ok := n.(*ast.GoStmt)
		if !ok {
			return true
		}
		if lit, ok := g.Call.Fun.(*ast.FuncLit); ok {
			body = lit.Body
		}
		return false
	})
	if body == nil || len(body.List) == 0 {
		t.Fatal("SELF-CHECK FAILED: the watchdog goroutine literal was not found; the scanner no " +
			"longer matches the real shape")
	}

	first, ok := body.List[0].(*ast.DeferStmt)
	if !ok {
		t.Fatalf("the watchdog goroutine's first statement is %T, not a defer.\n\n"+
			"It must be `defer cancel()`. The child context is parented on b.runCtx, which lives as "+
			"long as the broker process, and a child is removed from its parent's children map ONLY "+
			"by cancel — so any exit path that skips it leaks a cancelCtx for the life of the "+
			"process. The firing path is the one that used to.", body.List[0])
	}
	id, ok := first.Call.Fun.(*ast.Ident)
	if !ok || id.Name != "cancel" {
		t.Fatalf("the watchdog goroutine defers something other than cancel: %v", first.Call.Fun)
	}
}

// TestWatchdogBudgetIsUnchangedByThisAudit records the number the two tests above
// decline to wait for, so that a future change making a real firing test affordable
// is visible as a change to THIS line rather than as a silent opportunity nobody
// notices.
func TestWatchdogBudgetIsUnchangedByThisAudit(t *testing.T) {
	if got := watchdogBudget(&transferEntry{tier: "a"}); got != proto.XferTimeoutTierA {
		t.Fatalf("tier-A watchdog budget is %v, not %v.\n\n"+
			"If it has become short enough to wait for, replace the source guard in this file with a "+
			"test that arms a watchdog, lets it fire, and polls the parent's child count back to "+
			"baseline — that would prove what the source guard can only assert.", got, proto.XferTimeoutTierA)
	}
}
