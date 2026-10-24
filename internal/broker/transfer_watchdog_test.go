package broker

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xfertrack"
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
//
// Since hot-update S7 P31 the watchdog goroutine lives in internal/xfertrack and is
// rooted there; the broker hands runCtx over only as a Stopped channel. The broker
// control above now proves arming adds NOTHING to runCtx, and the moved function's
// own +n/-n child accounting is exercised beside it, in
// internal/xfertrack/watchdog_context_test.go.

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

	const n = 8

	// POSITIVE CONTROL for the probe itself: n real children of the parent are
	// seen, and cancel removes them. Without this, the zero below could be a
	// blind probe rather than a fact about the runtime.
	probes := make([]context.CancelFunc, 0, n)
	for i := 0; i < n; i++ {
		_, cancel := context.WithCancel(parent)
		probes = append(probes, cancel)
	}
	if got, _ := childCount(parent); got != base+n {
		t.Fatalf("%d real child contexts added %d to the probe's count, want %d — the probe does not see "+
			"the mechanism this file reasons about", n, got-base, n)
	}
	for _, c := range probes {
		c()
	}
	if got, _ := childCount(parent); got != base {
		t.Fatalf("cancel did not remove %d probe children from the parent", got-base)
	}

	// The watchdog moved to xfertrack (hot-update S7 plan §11.4 / P31). Its child
	// context is rooted in the tracker and the broker adapter hands the broker's
	// runCtx over only as a Stopped channel, so arming must add NOTHING to the
	// runCtx children map: the per-transfer leak this file is about cannot
	// accumulate there at all. The moved goroutine's own child accounting — the
	// mechanism TestWatchdogReleasesItsChildContextOnEveryExit's source guard
	// protects — is exercised where it now lives, by
	// internal/xfertrack/watchdog_context_test.go TestLegacyWatchdogChildContextAccountingIsObservable.
	// origin: S7 lead integration (scanner handoff for the moved watchdog)
	b := &Broker{transfers: xfertrack.NewLegacyTracker(), cfg: Config{Logger: silentLogger(), Now: time.Now}}
	cancels := make([]context.CancelFunc, 0, n)
	for i := 0; i < n; i++ {
		cancels = append(cancels, b.startTransferWatchdog(parent, xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{
			TransferID: "tid-" + string(rune('a'+i)), SID: "lab", NID: "lab-1",
			Verb: "push", Tier: "a", StartedAt: time.Now(),
		})))
	}
	if len(cancels) != n || slices.ContainsFunc(cancels, func(c context.CancelFunc) bool { return c == nil }) {
		t.Fatal("arming did not return a cancel for every watchdog")
	}
	if got, _ := childCount(parent); got != base {
		t.Fatalf("arming %d watchdogs through the broker adapter added %d child(ren) to runCtx.\n\n"+
			"A child context is removed from its parent ONLY by cancel. The parent here is the "+
			"broker's runCtx, which lives for the whole process, so anything left is left forever.",
			n, got-base)
	}
	for _, c := range cancels {
		c()
	}
	if got, _ := childCount(parent); got != base {
		t.Fatalf("after cancelling all %d watchdogs the parent holds %d extra child(ren)", n, got-base)
	}
}

func TestWatchdogReleasesItsChildContextOnEveryExit(t *testing.T) {
	// origin: hot-update S7 plan §11.4 / P31; follow the implementation, not the old filename.
	roots := []string{".", "../xfertrack"}
	fn := requireTransferDeclaration(t, "startTransferWatchdog", roots...)
	if err := watchdogChildCancelInvariant(fn); err != nil {
		t.Fatal(err)
	}
	// Each moved root gets a live defect. A clean old adapter must not hide
	// a defective leaf, and deleting the old root must not empty the scan.
	exerciseTransferScannerMoves(t, "startTransferWatchdog", roots, watchdogChildCancelInvariant,
		"func startTransferWatchdog() { go func() { defer cancel(); work() }() }",
		"func startTransferWatchdog() { go func() { work(); defer cancel() }() }")
}

func watchdogChildCancelInvariant(fn *ast.FuncDecl) error {
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
		return fmt.Errorf("SELF-CHECK FAILED: the watchdog goroutine literal was not found; the scanner no longer matches the real shape")
	}

	first, ok := body.List[0].(*ast.DeferStmt)
	if !ok {
		return fmt.Errorf("the watchdog goroutine's first statement is %T, not a defer.\n\n"+
			"It must be `defer cancel()`. The child context is parented on b.runCtx, which lives as "+
			"long as the broker process, and a child is removed from its parent's children map ONLY "+
			"by cancel — so any exit path that skips it leaks a cancelCtx for the life of the "+
			"process. The firing path is the one that used to.", body.List[0])
	}
	id, ok := first.Call.Fun.(*ast.Ident)
	if !ok || id.Name != "cancel" {
		return fmt.Errorf("the watchdog goroutine defers something other than cancel: %v", first.Call.Fun)
	}
	return nil
}

// TestWatchdogBudgetIsUnchangedByThisAudit records the number the two tests above
// decline to wait for, so that a future change making a real firing test affordable
// is visible as a change to THIS line rather than as a silent opportunity nobody
// notices.
func TestWatchdogBudgetIsUnchangedByThisAudit(t *testing.T) {
	if got := xfertrack.LegacyWatchdogBudget(xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{Tier: "a"})); got != proto.XferTimeoutTierA {
		t.Fatalf("tier-A watchdog budget is %v, not %v.\n\n"+
			"If it has become short enough to wait for, replace the source guard in this file with a "+
			"test that arms a watchdog, lets it fire, and polls the parent's child count back to "+
			"baseline — that would prove what the source guard can only assert.", got, proto.XferTimeoutTierA)
	}
}

// The leaf owners move implementation bodies; thin adapters may keep the old
// method names. Resolve the one load-bearing declaration across both roots and
// fail on absence/duplicates, rather than silently scanning an empty old file.
// origin: hot-update S7 plan §11.4 / P31; P34 R1 scanner handoff.
func transferDeclaration(name string, roots ...string) (*ast.FuncDecl, string, error) {
	var found *ast.FuncDecl
	var source string
	for _, root := range roots {
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, "", fmt.Errorf("read transfer scanner root %s: %w", root, err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, entry.Name())
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				return nil, "", fmt.Errorf("parse transfer scanner root %s: %w", path, err)
			}
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || !declarationNamed(fn, name) || fn.Body == nil {
					continue
				}
				// P22 keeps the broker cancellation adapter with the old name.
				// Its delegated StartWatchdog call is not the goroutine owner.
				if name == "startTransferWatchdog" && (root == "." || filepath.Base(root) == "broker") && delegatesLegacyWatchdog(fn) {
					continue
				}
				if found != nil {
					return nil, "", fmt.Errorf("transfer scanner found duplicate %s in %s and %s; select the real owner", name, source, path)
				}
				found, source = fn, path
			}
		}
	}
	if found == nil {
		return nil, "", fmt.Errorf("SELF-CHECK FAILED: %s has no implementation in transfer scanner roots %v", name, roots)
	}
	return found, source, nil
}

// declarationNamed matches a plain name against any function or method of
// that name, and "Type.Method" against exactly that method of Type (pointer or
// value receiver). A moved implementation can land under a new method name
// (the stranded finalizer became (*xferledger.Legacy).Finalize), and a bare
// "Finalize" would also match every unrelated port method in the roots.
// origin: hot-update S7 plan §11.4 / P31; S7 lead integration (scanner handoff)
func declarationNamed(fn *ast.FuncDecl, name string) bool {
	recv, method, qualified := strings.Cut(name, ".")
	if !qualified {
		return fn.Name.Name == name
	}
	if fn.Name.Name != method || fn.Recv == nil || len(fn.Recv.List) != 1 {
		return false
	}
	typ := fn.Recv.List[0].Type
	if star, ok := typ.(*ast.StarExpr); ok {
		typ = star.X
	}
	id, ok := typ.(*ast.Ident)
	return ok && id.Name == recv
}

func requireTransferDeclaration(t *testing.T, name string, roots ...string) *ast.FuncDecl {
	t.Helper()
	fn, source, err := transferDeclaration(name, roots...)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("transfer scanner: %s -> %s", name, source)
	return fn
}

// These are on-disk source fixtures, so the same parser sees positive,
// defective, missing and duplicate implementations after the old file moves.
// No product runtime or mutable private map is substituted here.
func exerciseTransferScannerMoves(t *testing.T, name string, owners []string,
	check func(*ast.FuncDecl) error, good, bad string) {
	t.Helper()
	for _, owner := range owners {
		t.Run("scanner-root/"+filepath.Base(owner), func(t *testing.T) {
			root := t.TempDir()
			oldRoot, leafRoot := filepath.Join(root, "broker"), filepath.Join(root, "leaf")
			for _, dir := range []string{oldRoot, leafRoot} {
				if err := os.Mkdir(dir, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			roots := []string{oldRoot, leafRoot}
			activeRoot := leafRoot
			if owner == "." {
				activeRoot = oldRoot
			}
			active := filepath.Join(activeRoot, "owner.go")
			write := func(path, body string) {
				t.Helper()
				if err := os.WriteFile(path, []byte("package fixture\n"+body+"\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if activeRoot == leafRoot && name == "startTransferWatchdog" {
				write(filepath.Join(oldRoot, "adapter.go"), "func startTransferWatchdog() { cancel := b.transfers.StartWatchdog(e, hooks); stop := context.AfterFunc(parent, cancel); use(stop) }")
			}
			write(active, good)
			fn, _, err := transferDeclaration(name, roots...)
			if err != nil || check(fn) != nil {
				t.Fatalf("positive moved implementation rejected: %v", err)
			}
			write(active, bad)
			fn, _, err = transferDeclaration(name, roots...)
			if err != nil {
				t.Fatal(err)
			}
			if err := check(fn); err == nil {
				t.Fatal("defective moved implementation passed its original invariant")
			}
			if err := os.Remove(active); err != nil {
				t.Fatal(err)
			}
			if _, _, err := transferDeclaration(name, roots...); err == nil {
				t.Fatal("deleting the implementation left the scanner green")
			}
			write(filepath.Join(oldRoot, "owner.go"), good)
			write(filepath.Join(leafRoot, "owner.go"), bad)
			if _, _, err := transferDeclaration(name, roots...); err == nil {
				t.Fatal("a clean old copy hid the defective leaf implementation")
			}
			if name == "startTransferWatchdog" {
				// Only the broker may delegate ownership. Treating a broken
				// leaf as another adapter would revive the clean stale copy.
				write(filepath.Join(leafRoot, "owner.go"), "func startTransferWatchdog() { return b.transfers.StartWatchdog(e, hooks) }")
				if fn, _, err := transferDeclaration(name, roots...); err == nil && check(fn) == nil {
					t.Fatal("a defective leaf was discarded as an adapter and the stale root stayed green")
				}
			}
		})
	}
}

func delegatesLegacyWatchdog(fn *ast.FuncDecl) bool {
	delegates, launches := false, false
	ast.Inspect(fn.Body, func(node ast.Node) bool {
		if _, ok := node.(*ast.GoStmt); ok {
			launches = true
		}
		if call, ok := node.(*ast.CallExpr); ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "StartWatchdog" {
				delegates = true
			}
		}
		return true
	})
	return delegates && !launches
}
