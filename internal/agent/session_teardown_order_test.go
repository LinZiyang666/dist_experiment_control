package agent

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"testing"
	"time"
)

func parseAgentFunction(t *testing.T, file, name string) (*ast.FuncDecl, *token.FileSet) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, file, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", file, err)
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == name {
			return fn, fset
		}
	}
	t.Fatalf("%s has no %s function", file, name)
	return nil, nil
}

// origin: upgrade follow-ups + gotcha #72 external review F1
// Every NATS subscription cleanup must run inside the bounded closer. A plain defer runs before the
// earlier-registered finalizer (LIFO), so Unsubscribe can wait forever for the same nc.mu held by a
// wedged reconnect and prevent the poison/escalation ladder from ever starting.
func TestSessionTeardownBoundsEveryNATSUnsubscribe(t *testing.T) {
	fn, _ := parseAgentFunction(t, "agent.go", "session")
	var directDefers []string
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		d, ok := n.(*ast.DeferStmt)
		if !ok {
			return true
		}
		ast.Inspect(d.Call, func(inner ast.Node) bool {
			sel, ok := inner.(*ast.SelectorExpr)
			if ok && sel.Sel.Name == "Unsubscribe" {
				directDefers = append(directDefers, "Unsubscribe")
			}
			return true
		})
		return true
	})
	if len(directDefers) != 0 {
		t.Fatalf("session has %d direct deferred NATS Unsubscribe call(s); they execute before the bounded finalizer and can recreate the #72 shutdown hang", len(directDefers))
	}
}

// origin: upgrade follow-ups + gotcha #72 external review F2
// The finalizer cancels runCtx. Passing the parent ctx to register leaves a pre-subscription session
// retrying forever after a rebuild finalizes and closes its connection, so Run never reaches a successor.
func TestSessionRegisterUsesCancelableRunContext(t *testing.T) {
	fn, _ := parseAgentFunction(t, "agent.go", "session")
	found := false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "register" || len(call.Args) == 0 {
			return true
		}
		id, ok := call.Args[0].(*ast.Ident)
		if !ok {
			t.Fatalf("session register context is not an identifier: %T", call.Args[0])
		}
		found = true
		if id.Name != "runCtx" {
			t.Fatalf("session calls register with %s, want runCtx: the bounded finalizer cancels runCtx, so the old register retry loop must observe it", id.Name)
		}
		return true
	})
	if !found {
		t.Fatal("session has no register call")
	}
}

// origin: gotcha #72 external re-review R1. connectNATS may return immediately before its
// DisconnectErr callback starts a reconnect that holds nc.mu. ConnectedUrl then blocks on that
// mutex. The finalizer and parent-cancellation hook must already be published, or session() cannot
// reach its own defer and an operator shutdown can remain hostage forever.
func TestSessionPublishesFinalizerBeforeNATSObserver(t *testing.T) {
	fn, fset := parseAgentFunction(t, "agent.go", "session")
	var finalizerPos, observerPos, parentHookPos token.Pos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.SelectorExpr:
			switch fun.Sel.Name {
			case "setSessionFinalizer":
				if finalizerPos == token.NoPos {
					finalizerPos = call.Pos()
				}
			case "ConnectedUrl":
				if observerPos == token.NoPos {
					observerPos = call.Pos()
				}
			case "AfterFunc":
				if id, ok := fun.X.(*ast.Ident); ok && id.Name == "context" && parentHookPos == token.NoPos {
					parentHookPos = call.Pos()
				}
			}
		}
		return true
	})
	if finalizerPos == token.NoPos || observerPos == token.NoPos || parentHookPos == token.NoPos {
		t.Fatalf("cannot locate finalizer/parent hook/ConnectedUrl in session: finalizer=%s hook=%s observer=%s",
			fset.Position(finalizerPos), fset.Position(parentHookPos), fset.Position(observerPos))
	}
	if finalizerPos > observerPos || parentHookPos > observerPos {
		t.Fatalf("session exposes a blocking NATS observer before teardown is externally reachable: finalizer=%s parent hook=%s observer=%s",
			fset.Position(finalizerPos), fset.Position(parentHookPos), fset.Position(observerPos))
	}
}

// A successful initial connect can disconnect before connectNATS returns. Clearing the watchdog
// after the return can therefore cancel the NEW disconnect's only recovery timer. Clear stale
// state before connectNATS starts publishing callbacks.
func TestSessionClearsStaleRedialBeforeConnecting(t *testing.T) {
	fn, fset := parseAgentFunction(t, "agent.go", "session")
	var stopPos, connectPos token.Pos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "stopRedialWatchdog":
			if stopPos == token.NoPos {
				stopPos = call.Pos()
			}
		case "connectNATS":
			if connectPos == token.NoPos {
				connectPos = call.Pos()
			}
		}
		return true
	})
	if stopPos == token.NoPos || connectPos == token.NoPos || stopPos > connectPos {
		t.Fatalf("stale-watchdog clear must precede connectNATS: stop=%s connect=%s",
			fset.Position(stopPos), fset.Position(connectPos))
	}
}

// origin: upgrade follow-ups + gotcha #72 external review F3
// rebuildOntoVoter must cancel or enter the finalizer before calling any nats.Conn observer. Even
// ConnectedUrl takes nc.mu; doReconnect holds that lock across Dial, which is the incident's wedge.
func TestRebuildTeardownDoesNotInspectNATSBeforeCancellation(t *testing.T) {
	fn, fset := parseAgentFunction(t, "roster.go", "rebuildOntoVoter")
	var inspectPos, finalizerPos token.Pos
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch sel.Sel.Name {
		case "ConnectedUrl":
			if inspectPos == token.NoPos {
				inspectPos = call.Pos()
			}
		case "loadSessionFinalizer":
			if finalizerPos == token.NoPos {
				finalizerPos = call.Pos()
			}
		}
		return true
	})
	if finalizerPos == token.NoPos {
		t.Fatalf("cannot locate loadSessionFinalizer in rebuildOntoVoter")
	}
	// origin: external review F3, tightened by the fix. The STRONGEST outcome is no *nats.Conn
	// observer on this path at ALL — the fix replaced ConnectedUrl with a lock-free snapshot taken
	// while the link was healthy (Agent.connectedURLSnapshot), so there is no call to order. Absence
	// therefore PASSES here; the ordering check below still fires if anyone reintroduces one.
	if inspectPos == token.NoPos {
		return
	}
	if inspectPos < finalizerPos {
		t.Fatalf("rebuildOntoVoter calls ConnectedUrl at %s before entering the finalizer at %s; ConnectedUrl takes nc.mu and can block behind the wedged reconnect before cancellation",
			fset.Position(inspectPos), fset.Position(finalizerPos))
	}
}

// origin: upgrade follow-ups + gotcha #72 external review F4
// recoverFromFailedExec may keep the process alive because the ordinary upgrade caller is still
// running the OLD image. A teardown escalation is different: it can be running the staged NEW
// image, and its closer is already known to be wedged. After restoring prev it must execute those
// restored bytes; returning would continue the wrong in-memory image with the wedged closer alive.
func TestWedgedTeardownExecFailureRunsRestoredImage(t *testing.T) {
	now := time.Now()
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 0, now.Add(time.Minute))
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

	a := testAgentFor(t, exePath)
	execCalls := 0
	a.cfg.UpgradeExecFn = func(string) error {
		execCalls++
		if execCalls == 1 {
			return os.ErrInvalid
		}
		return nil
	}
	a.escalateWedgedTeardown(teardownRebuild)

	if execCalls != 2 {
		t.Fatalf("wedged teardown made %d exec call(s), want 2: after the staged image's exec fails and prev is restored, returning keeps the NEW in-memory image and its wedged closer alive", execCalls)
	}
	if got, err := os.ReadFile(exePath); err != nil || string(got) != "OLD" {
		t.Fatalf("restored executable = %q, err=%v; want OLD", string(got), err)
	}
}

// A shared-binary sibling must not roll back another instance's pending upgrade merely because
// its own teardown self-exec failed. The marker identity invariant applies to this recovery path too.
func TestWedgedTeardownDoesNotRestoreSiblingUpgrade(t *testing.T) {
	now := time.Now()
	exePath, markerPath := bootFixture(t, "NEW", "OLD", nil)
	m := testMarker(upgradeStatePending, 0, now.Add(time.Minute))
	m.TargetSID, m.TargetNID = "lab", "target"
	m.NewSHA = mustSHA(t, exePath)
	m.PrevSHA = mustSHA(t, upgradePrevPath(exePath))
	if err := writeUpgradeMarker(markerPath, m); err != nil {
		t.Fatal(err)
	}

	a := testAgentFor(t, exePath)
	a.cfg.SID, a.cfg.NID = "lab", "sibling"
	execCalls := 0
	a.cfg.UpgradeExecFn = func(string) error {
		execCalls++
		if execCalls == 1 {
			return os.ErrInvalid
		}
		return nil
	}
	a.escalateWedgedTeardown(teardownRebuild)

	if got, err := os.ReadFile(exePath); err != nil || string(got) != "NEW" {
		t.Fatalf("sibling escalation changed shared executable to %q, err=%v; want NEW untouched", string(got), err)
	}
	got, err := readUpgradeMarker(markerPath)
	if err != nil || got == nil || got.State != upgradeStatePending {
		t.Fatalf("sibling escalation transitioned target marker: marker=%+v err=%v", got, err)
	}
}
