package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// dataplane_lifetime_test.go — the embedded SS proxy's lifetime must not be bindable to any
// context.
//
// origin: 2026-08-21 weilandserver incident (docs/reviews/proxy-lifecycle-plan.md). The SS
// server took a ctx and anchored its shutdown to it. The agent handed it the PER-SESSION
// runCtx (internal/agent/agent.go, rewritten in every session()), so every control-plane
// session rebuild silently killed the data plane — while the agent kept pointing at the
// corpse and the broker kept advertising the node as a READY exit. Seven hours and forty
// minutes, 5416 identical WARN lines, recoverable only by a session-wide `proxy off/on`
// that renumbered every node's public port.
//
// The fix was not "pass a longer-lived ctx". It was to remove the parameter, so that the set
// of things which can stop the server is CLOSED and greppable — Stop(), called from exactly
// four places enumerated on stopProxyOnRunExit — rather than "whoever happens to hold the
// ctx that was passed in". A longer-lived ctx would have fixed this incident and left the
// next one available.
//
// The invariant is "NO CALLER-OWNED context may reach this type", NOT "no context exists in
// this package". The mechanical form is therefore the SIGNATURE of the entry points, not an
// import ban.
//
// origin: proxy-lifecycle EXTERNAL review F5 (and F1, which is the same mistake seen from the
// other side). The first cut of this gate banned the "context" import package-wide, reasoning
// that ctx caused the incident. That conflated ownership with capability. A caller-owned ctx
// says "the session that asked for you is over"; a server-owned one says "you are being
// stopped" — opposite meanings, and only the first is a bug. The ban's real-world cost was
// concrete: it forced net.LookupIP (which has NO deadline at all) into the handler, so Stop()
// waited out the system resolver while the agent held proxyRuntime.mu. Banning the mechanism
// that fixes a hang is not a safety gate.
//
// So: ssproxy MAY hold a context it creates itself. It may NOT accept one.
//
// KNOWN BLIND SPOTS (external rereview R1) — stated so nobody mistakes this for a proof:
// it inspects only declarations with a receiver, so an exported CONSTRUCTOR or free function
// taking a ctx slips past; it matches the selector's package name literally, so an import
// alias or a type alias escapes; and the "Start must exist" anti-vacuity check accepts a
// Start on ANY receiver, not specifically (*Server). A faithful version needs go/types or
// go/packages to resolve real type identity. This is a cheap textual tripwire against the
// obvious regression, NOT the load-bearing guard — the behavioural tests in
// internal/agent/ssproxy are.
func TestSSProxyEntryPointsAcceptNoCallerContext(t *testing.T) {
	const pkgRoot = "../../internal/agent/ssproxy"

	// Recursive: a future subpackage must not escape the rule (external review F5).
	var files []string
	err := filepath.WalkDir(pkgRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", pkgRoot, err)
	}
	if len(files) == 0 {
		t.Fatalf("scanned 0 non-test files under %s — the gate is not looking at anything", pkgRoot)
	}

	// Exported methods on Server are the surface a caller can reach. None may take a
	// context: that is the only way a caller's lifetime could become the server's.
	foundStart := false
	for _, path := range files {
		fset := token.NewFileSet()
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", path, perr)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name == nil || !fn.Name.IsExported() || fn.Recv == nil {
				continue
			}
			if fn.Name.Name == "Start" {
				foundStart = true
			}
			if fn.Type.Params == nil {
				continue
			}
			for _, field := range fn.Type.Params.List {
				sel, ok := field.Type.(*ast.SelectorExpr)
				if !ok || sel.Sel == nil || sel.Sel.Name != "Context" {
					continue
				}
				if pkg, ok := sel.X.(*ast.Ident); ok && pkg.Name == "context" {
					t.Errorf("%s: exported method %s accepts a context.Context.\n"+
						"The embedded SS proxy is DATA PLANE: no CALLER may hand it a lifetime. "+
						"That is exactly how the per-session runCtx got in and made a routine NATS "+
						"session rebuild kill a live exit for 7h40m "+
						"(docs/reviews/proxy-lifecycle-plan.md).\n"+
						"If you need cancellation, the server must CREATE it (see Server.stopCtx) — "+
						"then it can only fire when this server is being stopped.", path, fn.Name.Name)
				}
			}
		}
	}
	if !foundStart {
		t.Fatal("no exported Start method found under " + pkgRoot + " — this gate is guarding a name that moved")
	}
}

// The agent must not reintroduce a ctx parameter on the two functions that used to carry the
// session ctx down into the SS server. This is the other half of the same invariant: the
// import ban above stops ssproxy from ACCEPTING a ctx, and this stops the agent from
// threading one toward it.
//
// It matches on the parameter list rather than on a call graph because the failure mode is
// textual and local — someone adds `ctx context.Context` back to the signature "so we can
// cancel the server", which is precisely the bug.
func TestProxyDirectivePathTakesNoContext(t *testing.T) {
	const path = "../../internal/agent/proxy.go"

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	banned := map[string]bool{
		"applyProxyDirective": true,
		"proxyStartLocked":    true,
	}
	seen := map[string]bool{}

	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name == nil || !banned[fn.Name.Name] {
			continue
		}
		seen[fn.Name.Name] = true
		if fn.Type.Params == nil {
			continue
		}
		for _, field := range fn.Type.Params.List {
			sel, ok := field.Type.(*ast.SelectorExpr)
			if !ok || sel.Sel == nil || sel.Sel.Name != "Context" {
				continue
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != "context" {
				continue
			}
			t.Errorf("%s takes a context.Context parameter again.\n"+
				"This is the exact shape of the 2026-08-21 incident: the parameter carried the "+
				"per-session runCtx into ssproxy.Server.Start, so a session rebuild stopped the SS "+
				"server and nothing rebuilt it. The server's lifetime belongs to Stop() and to the "+
				"four callers enumerated on stopProxyOnRunExit.", fn.Name.Name)
		}
	}

	// Both functions must still exist, or the gate is guarding names that moved.
	for name := range banned {
		if !seen[name] {
			t.Errorf("%s not found in %s — this gate is guarding a name that no longer exists; "+
				"point it at the current function or delete it deliberately", name, path)
		}
	}
}

// Run must actually WIRE the agent-exit teardown. Removing the SS server's ctx anchor also
// removed the free "agent exit stops it" behaviour, so that stop is now a single deferred call
// in Run — and a single deleted line silently reopens it.
//
// origin: proxy-lifecycle internal review MAJOR (tests lane): the stopper table's "agent exit"
// row calls stopProxyOnRunExit DIRECTLY, so it proves the function works while saying nothing
// about whether anything calls it. Deleting the defer from Run left the whole suite green.
// This is the wiring half; the table is the behaviour half. Both are needed.
func TestRunWiresTheAgentExitProxyTeardown(t *testing.T) {
	const path = "../../internal/agent/agent.go"

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	var run *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name != nil && fn.Name.Name == "Run" && fn.Recv != nil {
			run = fn
			break
		}
	}
	if run == nil {
		t.Fatalf("no (*Agent).Run found in %s — this gate is guarding a name that moved", path)
	}

	found := false
	ast.Inspect(run, func(n ast.Node) bool {
		def, ok := n.(*ast.DeferStmt)
		if !ok || def.Call == nil {
			return true
		}
		id, ok := def.Call.Fun.(*ast.Ident)
		if ok && id.Name == "stopProxyOnRunExit" {
			found = true
		}
		return true
	})

	if !found {
		t.Errorf("(*Agent).Run has no `defer stopProxyOnRunExit(...)`.\n" +
			"The SS server no longer hangs from any context, so agent exit stops it ONLY via this " +
			"deferred call. Without it the process can exit leaving a bound listener and a live " +
			"tunnel behind (docs/reviews/proxy-lifecycle-plan.md S8).\n" +
			"It must also stay registered AFTER `defer a.cancelFailClosed()` so it runs BEFORE it " +
			"(defers are LIFO) and latches agentExiting ahead of any in-flight fail-closed timer.")
	}
}

// heartbeatLoop must actually WIRE the corpse reap. On a single broker this is the ONLY edge
// that can start recovery for a server that died without producing a directive, so a single
// deleted line silently reopens the "dark node advertising READY" state.
//
// origin: proxy-lifecycle external review F4 — and the wiring half is here because the
// behaviour test calls reapProxyCorpseOnHeartbeat DIRECTLY, so removing the call site left it
// green. That is the same "tests the function, not the wiring" gap the external review already
// flagged once (F15 on the agent-exit teardown); it recurred immediately, which is the argument
// for pinning wiring mechanically rather than by discipline.
//
// KNOWN BLIND SPOT (external rereview R1): this only asserts the call APPEARS in the function
// body — it does not check that it runs BEFORE the payload snapshot and Publish, which is the
// property that actually matters (reaping after the snapshot would publish the stale pair and
// change nothing). The load-bearing guard for ordering is the rereviewer's
// TestHeartbeatPublishesReapedCorpseStateOnItsFirstTick, which drives a real NATS server and
// asserts the FIRST published heartbeat already carries (0,0,false). Keep this one as the fast
// "somebody deleted the line" signal; do not treat it as proof of order.
func TestHeartbeatLoopWiresTheCorpseReap(t *testing.T) {
	const path = "../../internal/agent/agent.go"

	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}

	var loop *ast.FuncDecl
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name != nil && fn.Name.Name == "heartbeatLoop" && fn.Recv != nil {
			loop = fn
			break
		}
	}
	if loop == nil {
		t.Fatalf("no (*Agent).heartbeatLoop found in %s — this gate is guarding a name that moved", path)
	}

	called := false
	ast.Inspect(loop, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		if id, ok := call.Fun.(*ast.Ident); ok && id.Name == "reapProxyCorpseOnHeartbeat" {
			called = true
		}
		return true
	})

	if !called {
		t.Error("(*Agent).heartbeatLoop does not call reapProxyCorpseOnHeartbeat.\n" +
			"On a SINGLE broker this is the only recovery edge for an SS server that stopped without " +
			"producing a directive: repairProxy returns early while the node reports `on && ready && " +
			"the exact applied pair`, which is precisely what an un-reaped corpse reports. Without " +
			"this call the node stays dark and keeps advertising READY " +
			"(docs/reviews/proxy-lifecycle-external-review.md F4).")
	}
}
