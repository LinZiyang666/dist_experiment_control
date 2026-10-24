package architecture

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/packages"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
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

// TestAgentImageEntryPointsAcceptNoContext — the image fence is the next data-plane
// object after the SS proxy, and the same rule applies to it: no caller-owned lifetime
// may reach a runtime that owns live children and their pipes. Freeze is deadline-bound
// and Thaw/Export/Import/Discard/Detach take no lifetime at all, so that a bootstrap can
// run the cut before any control connection exists and an S5 data-plane domain can join
// the bundle without violating the SS proxy's own gate. Unlike the ssproxy gate this one
// covers exported FUNCTIONS as well as methods (agentimage's entry points are functions).
//
// Whitelist: execsession.Runtime.Close(ctx) predates the fence (S0) and is teardown, not a
// data-plane lifetime; workload.Output/Input.Freeze(ctx) are the S0 actor primitives the
// fence derives its own bounded context for (ctx-none site in fence.go). Adding a ctx to
// any other exported symbol in these packages is what this gate is for.
// origin: docs/reviews/hot-update-agent-image-abi-plan.md §6 (Q4).
// origin: hot-update S3 plan §9 (docs/reviews/hot-update-agent-bootstrap-plan.md): bootstrap joins the same ctx-free surface.
// origin: hot-update S4 plan §10 / T47 — terminal owners join the same fence.
var imageContextFreeRoots = []string{
	"../../internal/agentimage", "../../internal/execsession", "../../internal/agentboot",
	"../../internal/ptysession", "../../internal/pty", "../../internal/terminalio",
}

// gate-control: TestAgentImageContextImportControls
func TestAgentImageEntryPointsAcceptNoContext(t *testing.T) {
	allowed := map[string]bool{
		"../../internal/execsession:Runtime.Close": true,
	}
	var exported int
	for _, pkgRoot := range imageContextFreeRoots {
		packageExports := 0
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
		for _, path := range files {
			fset := token.NewFileSet()
			file, perr := parser.ParseFile(fset, path, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			count, contexts := imageEntryPointContexts(file)
			exported += count
			packageExports += count
			for _, name := range contexts {
				if !allowed[pkgRoot+":"+name] {
					t.Errorf("%s: exported %s accepts a context.Context.\n"+
						"The image fence is DATA PLANE: a caller-owned lifetime must never reach a runtime "+
						"that owns live children (gotcha #80). Take a deadline (time.Time) or a "+
						"<-chan struct{} instead, and derive any context you need at a ctx-none site.", path, name)
				}
			}
		}
		if packageExports == 0 {
			t.Errorf("no exported entry points under %s — the context guard is blind", pkgRoot)
		}
	}
	if exported < 20 {
		t.Fatalf("saw only %d exported entry points across image and terminal owners — the gate is guarding a surface that moved", exported)
	}
	for key := range allowed {
		pkgRoot, name, _ := strings.Cut(key, ":")
		if !exportedSymbolTakesContext(t, pkgRoot, name) {
			t.Errorf("whitelisted %s no longer takes a context: delete the whitelist entry (a dead exemption reads as a live one)", key)
		}
	}
}

// exportedSymbolTakesContext reports whether pkgRoot declares an exported Type.Method (or
// function) named name with a context.Context parameter: the whitelist's liveness check.
func exportedSymbolTakesContext(t *testing.T, pkgRoot, name string) bool {
	t.Helper()
	found := false
	err := filepath.WalkDir(pkgRoot, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return err
		}
		fset := token.NewFileSet()
		file, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			return perr
		}
		_, contexts := imageEntryPointContexts(file)
		if slices.Contains(contexts, name) {
			found = true
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return found
}

// origin: hot-update S3 plan §9. Import spelling must not change which lifetime
// crosses the fence. Share resolution with the whitelist check so aliases cannot
// either bypass the gate or turn a dead exemption into an apparently live one.
func imageEntryPointContexts(file *ast.File) (exported int, contexts []string) {
	contextNames := map[string]bool{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil || path != "context" {
			continue
		}
		name := "context"
		if imp.Name != nil {
			name = imp.Name.Name
		}
		if name != "_" {
			contextNames[name] = true
		}
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name == nil || !fn.Name.IsExported() || fn.Type.Params == nil {
			continue
		}
		name := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) == 1 {
			recv := fn.Recv.List[0].Type
			if star, ok := recv.(*ast.StarExpr); ok {
				recv = star.X
			}
			if ident, ok := recv.(*ast.Ident); ok {
				if !ident.IsExported() {
					continue // a method on an unexported type is not an entry point
				}
				name = ident.Name + "." + name
			}
		}
		exported++
		for _, field := range fn.Type.Params.List {
			takesContext := false
			switch typ := ast.Unparen(field.Type).(type) {
			case *ast.SelectorExpr:
				pkg, ok := typ.X.(*ast.Ident)
				takesContext = ok && contextNames[pkg.Name] && typ.Sel.Name == "Context"
			case *ast.Ident:
				takesContext = contextNames["."] && typ.Name == "Context"
			}
			if takesContext {
				contexts = append(contexts, name)
				break
			}
		}
	}
	return exported, contexts
}

// origin: hot-update S3 plan §7 T18 / §9. Both import spellings and exemption
// liveness use source fixtures through the very same predicate as the tree scan.
func TestAgentImageContextImportControls(t *testing.T) {
	// Pin scanner membership as well as its predicate: omitting a new owner
	// must fail even while all of its current exports are context-free.
	// origin: hot-update S4 plan §10 / T47.
	for _, pkg := range []string{"ptysession", "pty", "terminalio"} {
		if !slices.Contains(imageContextFreeRoots, "../../internal/"+pkg) {
			t.Errorf("missing context-free owner %s", pkg)
		}
	}

	for _, tc := range []struct {
		name, source string
		exported     int
		contexts     []string
	}{
		{"direct import", `import "context"; func Accept(c context.Context) {}`, 1, []string{"Accept"}},
		{"aliased import", `import ctx "context"; func Accept(c ctx.Context) {}`, 1, []string{"Accept"}},
		{"dot import", `import . "context"; func Accept(c Context) {}`, 1, []string{"Accept"}},
		{"parenthesized alias", `import lifetime "context"; func Accept(c (lifetime.Context)) {}`, 1, []string{"Accept"}},
		{"pointer method", `import ctx "context"; type Runtime struct{}; func (*Runtime) Close(c ctx.Context) {}`, 1, []string{"Runtime.Close"}},
		{"value method", `import . "context"; type Runtime struct{}; func (Runtime) Close(c Context) {}`, 1, []string{"Runtime.Close"}},
		{"private function", `import ctx "context"; func accept(c ctx.Context) {}`, 0, nil},
		{"private receiver", `import . "context"; type runtime struct{}; func (*runtime) Close(c Context) {}`, 0, nil},
		// origin: hot-update S4 plan §10 / T47 — both constructors and owner methods.
		{"terminal actor lifetime", `import "context"; type Actor struct{}; func (*Actor) Activate(c context.Context) {}`, 1, []string{"Actor.Activate"}},
		{"master owner lifetime", `import ctx "context"; type Master struct{}; func (*Master) Close(c ctx.Context) {}`, 1, []string{"Master.Close"}},
		{"local IO lifetime", `import . "context"; type IO struct{}; func (*IO) EnterRaw(c Context) {}`, 1, []string{"IO.EnterRaw"}},
		{"terminal deadline", `import "time"; type Actor struct{}; func (*Actor) Freeze(deadline time.Time) {}`, 1, nil},
		{"no lifetime", `func Accept(deadline int) {}`, 1, nil},
		{"local context name", `type Context struct{}; func Accept(c Context) {}`, 1, nil},
		{"foreign context package", `import context "example.invalid/types"; func Accept(c context.Context) {}`, 1, nil},
		{"foreign alias", `import ctx "example.invalid/types"; type Runtime struct{}; func (*Runtime) Close(c ctx.Context) {}`, 1, nil},
		{"different context symbol", `import "context"; type Runtime struct{}; func (*Runtime) Close(c context.CancelFunc) {}`, 1, nil},
		{"dead exemption", `type Runtime struct{}; func (*Runtime) Close() {}`, 1, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := "package fixture\n" + tc.source
			file, err := parser.ParseFile(token.NewFileSet(), "entry.go", source, 0)
			if err != nil {
				t.Fatal(err)
			}
			count, contexts := imageEntryPointContexts(file)
			if count != tc.exported || !slices.Equal(contexts, tc.contexts) {
				t.Fatalf("got exported=%d, contexts=%v; want exported=%d, contexts=%v", count, contexts, tc.exported, tc.contexts)
			}
			root := t.TempDir()
			if err := os.WriteFile(filepath.Join(root, "entry.go"), []byte(source), 0o600); err != nil {
				t.Fatal(err)
			}
			if got, want := exportedSymbolTakesContext(t, root, "Runtime.Close"), slices.Contains(tc.contexts, "Runtime.Close"); got != want {
				t.Errorf("whitelist liveness = %v, want %v", got, want)
			}
		})
	}
}

// Type identity closes import-alias, cross-file alias, exported constructor,
// callback and options-struct holes in the older textual tripwires above.
// Server-owned stopCtx fields are private and do not cross an entry point.
// origin: hot-update S5 plan §10.1 / T49.
// gate-control: TestFlowSSDataplaneLifetimeControls
var flowSSContextFreeRoots = []string{"flow", "rawsocket", "sealedstate", "ssstate", "agent/ssproxy", "agentimage", "agentboot", "execsession", "ptysession", "pty", "terminalio", "tunnel"}

func TestFlowSSDataplaneEntryPointsAcceptNoCallerContext(t *testing.T) {
	roots := flowSSContextFreeRoots
	patterns := make([]string, len(roots))
	for i, root := range roots {
		patterns[i] = "./internal/" + root
	}
	loaded, err := packages.Load(&packages.Config{Dir: repoRoot(t), Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports}, patterns...)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != len(roots) {
		t.Fatal("context owner discovery incomplete")
	}
	for _, pkg := range loaded {
		if len(pkg.Errors) > 0 {
			t.Fatalf("typecheck %s: %v", pkg.PkgPath, pkg.Errors)
		}
		exports := 0
		for i, file := range pkg.Syntax {
			// Legacy tunnel APIs precede S5. The frozen image/carrier surface lives
			// in flow_* files; importing all tunnel exports would ban legacy Bind.
			if pkg.PkgPath == modPrefix+"internal/tunnel" && !strings.HasPrefix(filepath.Base(pkg.CompiledGoFiles[i]), "flow_") {
				continue
			}

			count, bad, err := resolvedFlowSSCallerContexts(file, pkg.TypesInfo, pkg.PkgPath)
			if err != nil {
				t.Fatal(err)
			}
			exports += count
			for _, name := range bad {
				t.Errorf("%s: exported %s accepts caller-owned context", pkg.PkgPath, name)
			}
		}

		if exports == 0 {
			t.Errorf("no exported entry points scanned in %s", pkg.PkgPath)
		}
	}
}

func signatureAcceptsCallerContext(signature *types.Signature) bool {
	seen := map[types.Type]bool{}
	var contains func(types.Type) bool
	contains = func(typ types.Type) bool {
		typ = types.Unalias(typ)
		if seen[typ] {
			return false
		}
		seen[typ] = true
		if named, ok := typ.(*types.Named); ok {
			object := named.Obj()
			if object.Pkg() != nil && object.Pkg().Path() == "context" && object.Name() == "Context" {
				return true
			}
			// P00 explicitly passes the SDK's JetStream operation port to writer
			// and reader factories. Its methods accept bounded per-operation ctx;
			// the interface value is not a caller lifetime. Keep this exact type
			// opaque while continuing to inspect our own port/factory signatures.
			// origin: hot-update S7 plan §8.1 / T50
			if object.Pkg() != nil && object.Pkg().Path() == "github.com/nats-io/nats.go/jetstream" && object.Name() == "JetStream" {
				return false
			}
			return contains(named.Underlying())
		}
		switch x := typ.(type) {
		case *types.TypeParam:
			return contains(x.Constraint())
		case *types.Pointer:
			return contains(x.Elem())
		case *types.Slice:
			return contains(x.Elem())
		case *types.Array:
			return contains(x.Elem())
		case *types.Chan:
			return contains(x.Elem())
		case *types.Map:
			return contains(x.Key()) || contains(x.Elem())
		case *types.Signature:
			for i := 0; i < x.Params().Len(); i++ {
				if contains(x.Params().At(i).Type()) {
					return true
				}
			}
		case *types.Struct:
			for i := 0; i < x.NumFields(); i++ {
				if x.Field(i).Exported() && contains(x.Field(i).Type()) {
					return true
				}
			}
		case *types.Interface:
			for i := 0; i < x.NumEmbeddeds(); i++ {
				if contains(x.EmbeddedType(i)) {
					return true
				}
			}
			x.Complete()
			// A defined interface with Context's method set is also assignable from
			// the caller's context, even when it no longer carries the original name.
			contextMethods := map[string]bool{"Deadline": false, "Done": false, "Err": false, "Value": false}
			for i := 0; i < x.NumMethods(); i++ {
				if _, ok := contextMethods[x.Method(i).Name()]; ok {
					contextMethods[x.Method(i).Name()] = true
				}
			}
			if contextMethods["Deadline"] && contextMethods["Done"] && contextMethods["Err"] && contextMethods["Value"] {
				return true
			}
			for i := 0; i < x.NumMethods(); i++ {
				if contains(x.Method(i).Type()) {
					return true
				}
			}
		}
		return false
	}
	for i := 0; i < signature.Params().Len(); i++ {
		if contains(signature.Params().At(i).Type()) {
			return true
		}
	}
	return false
}

func contextFixtureSignatures(source string) ([]string, error) {
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "fixture.go", source, 0)
	if err != nil {
		return nil, err
	}

	info := &types.Info{Defs: map[*ast.Ident]types.Object{}}
	_, err = (&types.Config{Importer: importer.Default()}).Check("fixture", set, []*ast.File{file}, info)
	if err != nil {
		return nil, err
	}
	_, bad, err := resolvedFlowSSCallerContexts(file, info, "fixture")
	return bad, err
}

// The SDK JetStream interface is opaque to this gate (P34: frozen P00 factories
// accept it; its per-operation Context parameters do not bind a caller lifetime).
// An UNEXPORTED adapter that implements that exact interface — xferbucket's
// configJS, which routes legacy config writes to the single coordinator — has
// its signatures dictated by the SDK, so only those SDK methods inherit the
// opacity. An exported type is public API and stays checked; any extra exported
// method on the adapter stays checked; a type that does not implement the whole
// interface gets nothing.
// origin: hot-update S7 plan §2.1 / §11.2 / T05 / T50
// Stream adapters have the same SDK-imposed operation signatures (notably
// recoveryStream.GetMsg). Keep the exact package and full interface checks:
// neither a private partial implementation nor a new context method qualifies.
// origin: S10 internal review GATE-FINAL F1 / lead Stream adapter decision
// gate-control: TestSDKAdapterContextExceptionIsExact
func sdkOperationInterface(pkg *types.Package, name string) *types.Interface {
	if pkg == nil {
		return nil
	}
	for _, imported := range pkg.Imports() {
		if imported.Path() != "github.com/nats-io/nats.go/jetstream" {
			continue
		}
		if object := imported.Scope().Lookup(name); object != nil {
			if sdk, ok := object.Type().Underlying().(*types.Interface); ok {
				return sdk
			}
		}
	}
	return nil
}

func sdkAdapterMethod(method *types.Func, sdk *types.Interface) bool {
	signature, ok := method.Type().(*types.Signature)
	if sdk == nil || !ok || signature.Recv() == nil {
		return false
	}
	receiver := signature.Recv().Type()
	base := receiver
	if pointer, ok := base.(*types.Pointer); ok {
		base = pointer.Elem()
	}
	named, ok := types.Unalias(base).(*types.Named)
	if !ok || named.Obj().Exported() || !types.Implements(receiver, sdk) {
		return false
	}
	for i := range sdk.NumMethods() {
		candidate := sdk.Method(i)
		if candidate.Name() == method.Name() && types.Identical(candidate.Type(), method.Type()) {
			return true
		}
	}
	return false
}

// The live file walk and fixtures share discovery, not just the recursive type
// predicate. Dropping constructors, methods or function variables must turn a
// control red even when today's production surface is already context-free.
func resolvedFlowSSCallerContexts(file *ast.File, info *types.Info, pkg string) (int, []string, error) {
	exported := 0
	var bad []string
	for _, decl := range file.Decls {
		switch node := decl.(type) {
		case *ast.FuncDecl:
			if !node.Name.IsExported() {
				continue
			}
			object, ok := info.Defs[node.Name].(*types.Func)
			if !ok {
				return 0, nil, fmt.Errorf("missing resolved exported signature")
			}
			exported++
			if pkg == modPrefix+"internal/execsession" && node.Name.Name == "Close" && node.Recv != nil && receiverTypeName(node.Recv.List[0].Type) == "Runtime" {
				continue
			}
			if sdkAdapterMethod(object, sdkOperationInterface(object.Pkg(), "JetStream")) ||
				sdkAdapterMethod(object, sdkOperationInterface(object.Pkg(), "Stream")) {
				continue
			}
			if signatureAcceptsCallerContext(object.Type().(*types.Signature)) {
				bad = append(bad, object.FullName())
			}
		case *ast.GenDecl:
			// Function aliases are the frozen P00 callback contracts. Interface
			// methods also accept caller lifetimes even without a concrete body.
			// origin: hot-update S6 plan §2.1 / §9.3 / T53.
			if node.Tok == token.TYPE {
				for _, spec := range node.Specs {
					named := spec.(*ast.TypeSpec)
					if !named.Name.IsExported() {
						continue
					}
					object, ok := info.Defs[named.Name].(*types.TypeName)
					if !ok {
						return 0, nil, fmt.Errorf("missing resolved exported type")
					}
					underlying := types.Unalias(object.Type()).Underlying()
					switch shape := underlying.(type) {
					case *types.Signature:
						exported++
						if signatureAcceptsCallerContext(shape) {
							bad = append(bad, named.Name.Name)
						}
					case *types.Interface:
						shape.Complete()
						for i := 0; i < shape.NumMethods(); i++ {
							method := shape.Method(i)
							if !method.Exported() {
								continue
							}
							exported++
							if signatureAcceptsCallerContext(method.Type().(*types.Signature)) {
								bad = append(bad, named.Name.Name+"."+method.Name())
							}
						}
					}
				}
				continue
			}
			if node.Tok != token.VAR {
				continue
			}
			for _, spec := range node.Specs {
				value := spec.(*ast.ValueSpec)
				for _, name := range value.Names {
					if !name.IsExported() {
						continue
					}
					object, ok := info.Defs[name].(*types.Var)
					if !ok {
						return 0, nil, fmt.Errorf("missing resolved exported variable")
					}
					if signature, ok := types.Unalias(object.Type()).Underlying().(*types.Signature); ok {
						exported++
						if signatureAcceptsCallerContext(signature) {
							bad = append(bad, name.Name)
						}
					}
				}
			}
		}
	}
	return exported, bad, nil
}

// A local root context is legal; only caller-owned contexts crossing a public
// parameter (including aliases and callback containers) are forbidden.
// origin: hot-update S5 plan §10.1 / T49.
func TestFlowSSDataplaneLifetimeControls(t *testing.T) {
	if len(flowSSContextFreeRoots) != 12 {
		t.Fatal("context-free owner registry must contain all twelve owners")
	}
	for _, root := range []string{"flow", "rawsocket", "sealedstate", "ssstate", "agent/ssproxy", "agentimage", "agentboot", "execsession", "ptysession", "pty", "terminalio", "tunnel"} {
		if !slices.Contains(flowSSContextFreeRoots, root) {
			t.Fatalf("missing context-free owner %s", root)
		}
	}
	for _, tc := range []struct {
		name, source string
		reject       bool
	}{
		{"direct", `import "context"; func New(c context.Context){}`, true},
		{"method", `import "context";type Server struct{};func(s *Server) Start(c context.Context){}`, true},
		{"private-receiver", `import "context";type server struct{};func(s *server) Start(c context.Context){}`, true},
		{"import-alias", `import lifetime "context"; func New(c lifetime.Context){}`, true},
		{"type-alias", `import "context"; type lifetime = context.Context; func New(c lifetime){}`, true},
		{"generic-constraint", `import "context";func New[T context.Context](c T){}`, true},
		{"alias-chain", `import "context"; type first = context.Context; type second = first; func New(c second){}`, true},
		{"defined-interface", `import "context"; type lifetime context.Context; func New(c lifetime){}`, true},
		{"embedded-interface", `import "context"; type lifetime interface{context.Context}; func New(c lifetime){}`, true},
		{"constructor-options", `import "context"; type Options struct { Lifetime context.Context }; func New(o Options){}`, true},
		{"callback", `import "context"; func New(fn func(context.Context)){}`, true},
		{"function-variable", `import "context"; var New=func(c context.Context){}`, true},
		{"pointer", `import "context";func New(c *context.Context){}`, true},
		{"array", `import "context";func New(c [1]context.Context){}`, true},
		{"channel", `import "context";func New(c <-chan context.Context){}`, true},
		{"container", `import "context"; type lifetime = context.Context; func New(c map[string][]lifetime){}`, true},
		{"own-root", `import "context"; type Server struct { stopCtx context.Context }; func New() *Server { return &Server{stopCtx:context.Background()} }`, false},
		{"own-private-field", `import "context"; type Server struct { stopCtx context.Context }; func Adopt(s *Server){}`, false},
		{"stop-channel", `func New(stop <-chan struct{}){}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := contextFixtureSignatures("package fixture; " + tc.source)
			if err != nil {
				t.Fatal(err)
			}
			if (len(got) > 0) != tc.reject {
				t.Fatalf("caller context predicate got %v, reject=%t", got, tc.reject)
			}
		})
	}
}

// Image owners' new APIs are context-free on both supported build graphs. The
// legacy HTTP/admin/tunnel surface is preserved; its image seams are explicit
// filename responsibilities, never a host-dependent list of compiled symbols.
// origin: hot-update S6 plan §9.3 / §12.1 P31 / T53
// gate-control: TestBrokerDataplaneLifetimeControls
var brokerContextFreeRoots = []string{"brokerimage", "listenerimage", "brokermaint", "controlbudget", "clusterio", "httplisten", "adminsock"}

func brokerContextFile(pkg, path string) bool {
	base := filepath.Base(path)
	switch strings.TrimPrefix(pkg, modPrefix+"internal/") {
	case "adminsock":
		// The upgrade envelope is a ctx-free transport port too; moving its
		// declarations out of the frozen image contract must not hide aliases.
		// origin: S7 internal review GATE-CONVERGE-layering F2
		return strings.HasPrefix(base, "image_") || strings.HasPrefix(base, "upgrade_")
	case "httplisten":
		return strings.HasPrefix(base, "image_")
	case "tunnel":
		return strings.HasPrefix(base, "flow_") || strings.HasPrefix(base, "server_image_")
	default:
		return true
	}
}

// origin: hot-update S6 plan §9.3 / T53
func TestBrokerDataplaneEntryPointsAllOS(t *testing.T) {
	roots := append(append([]string{}, flowSSContextFreeRoots...), brokerContextFreeRoots...)
	// S7 factories and port aliases are caller-visible lifetime boundaries too.
	// Keep them on both build graphs; a Darwin-only alias cannot evade the gate.
	// origin: hot-update S7 plan §8.1 / §11.2 / T50
	roots = append(roots, transferLeafRoots...)
	// origin: hot-update S9 plan §10.2 / T83 — NC owners are ctx-free too.
	roots = append(roots, infraContextFreeRoots...)
	patterns := make([]string, len(roots))
	for i, root := range roots {
		patterns[i] = "./internal/" + root
	}
	for _, platform := range []struct{ os, arch string }{{"linux", "amd64"}, {"darwin", "arm64"}} {
		t.Run(platform.os, func(t *testing.T) {
			env := []string{}
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "GOOS=") && !strings.HasPrefix(value, "GOARCH=") && !strings.HasPrefix(value, "CGO_ENABLED=") {
					env = append(env, value)
				}
			}
			env = append(env, "GOOS="+platform.os, "GOARCH="+platform.arch, "CGO_ENABLED=0")
			loaded, err := packages.Load(&packages.Config{Dir: repoRoot(t), Env: env, Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports}, patterns...)
			if err != nil {
				t.Fatal(err)
			}
			if len(loaded) != len(roots) {
				t.Fatal("image context owner discovery incomplete")
			}
			for _, pkg := range loaded {
				if len(pkg.Errors) > 0 {
					t.Fatalf("typecheck %s: %v", pkg.PkgPath, pkg.Errors)
				}
				declarations := 0
				for i, file := range pkg.Syntax {
					if !brokerContextFile(pkg.PkgPath, pkg.CompiledGoFiles[i]) {
						continue
					}
					count, bad, err := resolvedFlowSSCallerContexts(file, pkg.TypesInfo, pkg.PkgPath)
					if err != nil {
						t.Fatal(err)
					}
					declarations += count
					// Pure value leaves currently have no functions. Their exported types
					// are a legitimate non-vacuity floor, not a reason to exempt later APIs.
					for _, decl := range file.Decls {
						group, ok := decl.(*ast.GenDecl)
						if !ok || group.Tok != token.TYPE {
							continue
						}
						for _, spec := range group.Specs {
							if spec.(*ast.TypeSpec).Name.IsExported() {
								declarations++
							}
						}
					}
					for _, name := range bad {
						t.Errorf("%s: exported %s accepts caller-owned context", pkg.PkgPath, name)
					}
				}
				if declarations == 0 {
					t.Errorf("no image declarations scanned in %s", pkg.PkgPath)
				}
			}
		})
	}
}

// Keep the admin seam independently runnable while transfer owners repair their
// own lifetime violations. This checks the same production roots and resolved
// signatures as the combined gate; an exported context alias must stay red.
// origin: S7 internal review GATE-CONVERGE-layering F2
// gate-control: TestBrokerDataplaneLifetimeControls
func TestBrokerAdminUpgradeLifetimeAllOS(t *testing.T) {
	for _, platform := range []struct{ os, arch string }{{"linux", "amd64"}, {"darwin", "arm64"}} {
		t.Run(platform.os, func(t *testing.T) {
			env := []string{}
			for _, value := range os.Environ() {
				if !strings.HasPrefix(value, "GOOS=") && !strings.HasPrefix(value, "GOARCH=") && !strings.HasPrefix(value, "CGO_ENABLED=") {
					env = append(env, value)
				}
			}
			env = append(env, "GOOS="+platform.os, "GOARCH="+platform.arch, "CGO_ENABLED=0")
			loaded, err := packages.Load(&packages.Config{Dir: repoRoot(t), Env: env, Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo}, "./internal/adminsock")
			if err != nil || len(loaded) != 1 {
				t.Fatalf("admin lifetime discovery: %v packages=%d", err, len(loaded))
			}
			pkg := loaded[0]
			if len(pkg.Errors) != 0 {
				t.Fatal(pkg.Errors)
			}
			foundUpgrade := false
			for i, file := range pkg.Syntax {
				path := pkg.CompiledGoFiles[i]
				if !brokerContextFile(pkg.PkgPath, path) {
					continue
				}
				foundUpgrade = foundUpgrade || filepath.Base(path) == "upgrade_protocol.go"
				_, bad, err := resolvedFlowSSCallerContexts(file, pkg.TypesInfo, pkg.PkgPath)
				if err != nil || len(bad) != 0 {
					t.Errorf("admin image/upgrade lifetimes: %v %v", bad, err)
				}
			}
			if !foundUpgrade {
				t.Fatal("admin lifetime scanner lost the actual upgrade protocol root")
			}
		})
	}
}

// origin: hot-update S7 plan §8.1 / §11.2 / T50
// gate-control: TestTransferDataplaneLifetimeControls
func TestTransferDataplaneLifetimeControls(t *testing.T) {
	for _, tc := range []struct {
		source string
		reject bool
	}{
		{`import lifetime "context";type first=lifetime.Context;type second=first;type Factory func(second)`, true},
		{`import "context";type Manager interface{Stop(context.Context)error}`, true},
		{`import "context";type Config struct{Stop context.Context};type Factory func(Config)`, true},
		{`import "context";type Factory func(func(context.Context))`, true},
		{`type Deadline int64;type Factory func(Deadline)error;type Manager interface{Stop(Deadline)error}`, false},
	} {
		root := transferFixtureTree(t, map[string]string{"internal/xferworker/contract_other.go": "package fixture;" + tc.source})
		raw, err := os.ReadFile(filepath.Join(root, "internal/xferworker/contract_other.go"))
		if err != nil {
			t.Fatal(err)
		}
		bad, err := contextFixtureSignatures(string(raw))
		if err != nil || (len(bad) > 0) != tc.reject {
			t.Fatalf("on-disk AllOS factory/alias lifetime: bad=%v err=%v reject=%t", bad, err, tc.reject)
		}
	}
	// Only an unexported adapter's methods that ARE the opaque SDK interface's
	// methods inherit its opacity.
	// origin: hot-update S7 plan §2.1 / §11.2 / T05 / T50
	const sdkFixture = `package fixture
import "context"
type SDK interface{CreateStream(context.Context, int) error; Stream(context.Context) error}
type adapter struct{ SDK }
func (a *adapter) CreateStream(ctx context.Context, n int) error { return nil }
func (a *adapter) Extra(ctx context.Context) error { return nil }
type Adapter struct{ SDK }
func (a *Adapter) CreateStream(ctx context.Context, n int) error { return nil }
type partial struct{}
func (p *partial) CreateStream(ctx context.Context, n int) error { return nil }`
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, "fixture.go", sdkFixture, 0)
	if err != nil {
		t.Fatal(err)
	}
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}}
	pkg, err := (&types.Config{Importer: importer.Default()}).Check("fixture", set, []*ast.File{file}, info)
	if err != nil {
		t.Fatal(err)
	}
	sdk := pkg.Scope().Lookup("SDK").Type().Underlying().(*types.Interface)
	method := func(receiver, name string) *types.Func {
		object, _, _ := types.LookupFieldOrMethod(types.NewPointer(pkg.Scope().Lookup(receiver).Type()), true, pkg, name)
		return object.(*types.Func)
	}
	for _, tc := range []struct {
		receiver, name string
		allowed        bool
	}{
		{"adapter", "CreateStream", true},
		{"adapter", "Extra", false},
		{"Adapter", "CreateStream", false},
		{"partial", "CreateStream", false},
	} {
		if sdkAdapterMethod(method(tc.receiver, tc.name), sdk) != tc.allowed {
			t.Fatalf("SDK adapter opacity drifted for %s.%s, want allowed=%t", tc.receiver, tc.name, tc.allowed)
		}
	}
	if sdkAdapterMethod(method("adapter", "CreateStream"), nil) {
		t.Fatal("adapter opacity granted without the SDK interface")
	}
}

// origin: hot-update S6 plan §11 / T53
func TestBrokerDataplaneLifetimeControls(t *testing.T) {
	// Moving transaction declarations to their protocol file must keep the
	// resolved lifetime scanner rooted there on every OS, including aliases.
	// origin: S7 internal review GATE-CONVERGE-layering F2
	for _, path := range []string{"internal/adminsock/image_contracts.go", "internal/adminsock/upgrade_protocol.go", "internal/adminsock/upgrade_other.go"} {
		if !brokerContextFile(modPrefix+"internal/adminsock", path) {
			t.Fatalf("admin lifetime scanner lost root %s", path)
		}
		root := transferFixtureTree(t, map[string]string{path: `package adminsock;import "context";type Endpoint interface { Query(context.Context) error };type UpgradeEndpoint = Endpoint`})
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		bad, err := contextFixtureSignatures(string(raw))
		if err != nil || len(bad) != 2 {
			t.Fatalf("on-disk admin context alias escaped: %v %v", bad, err)
		}
	}
	want := []string{"brokerimage", "listenerimage", "brokermaint", "controlbudget", "clusterio", "httplisten", "adminsock"}
	if !slices.Equal(brokerContextFreeRoots, want) {
		t.Fatal("image context owner registry drifted")
	}
	for _, tc := range []struct {
		source string
		bad    bool
	}{
		{`import c "context";type first=c.Context;type next=first;type FreezeFunc=func(next) error`, true},
		{`import "context";type Keeper interface{Freeze(context.Context)error}`, true},
		{`import "context";type keeper interface{Freeze(context.Context)error};type Keeper interface{keeper}`, true},
		{`import "context";type Args struct{Lifetime context.Context};type FreezeFunc=func(Args)`, true},
		{`import "context";type FreezeFunc func(fn func(context.Context))`, true},
		{`type FreezeFunc=func(int64)error;type Keeper interface{Freeze(int64)error}`, false},
		{`import "context";type owner struct{lifetime context.Context};type FreezeFunc=func(*owner)`, false},
	} {
		bad, err := contextFixtureSignatures("package fixture;" + tc.source)
		if err != nil {
			t.Fatal(err)
		}
		if (len(bad) > 0) != tc.bad {
			t.Fatalf("callback/interface context discovery: %v want bad=%t", bad, tc.bad)
		}
	}
	for _, tc := range []struct {
		pkg, file string
		want      bool
	}{
		{"tunnel", "server_image_other.go", true}, {"tunnel", "server_image_contracts.go", true}, {"tunnel", "flow_image.go", true}, {"tunnel", "tunnel.go", false},
		{"adminsock", "image_contracts.go", true}, {"adminsock", "server.go", false}, {"httplisten", "image_listener.go", true}, {"brokerimage", "import_other.go", true},
	} {
		if brokerContextFile(modPrefix+"internal/"+tc.pkg, tc.file) != tc.want {
			t.Fatalf("context responsibility selection drifted: %v", tc)
		}
	}
	// A real P00 function alias with an added caller lifetime must be rejected by
	// exactly the type-resolution predicate used for production declarations.
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "internal/clusterupgrade/broker_contracts.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := strings.Replace(string(raw), "package clusterupgrade", "package fixture\nimport lifetime \"context\"", 1) + "\ntype FreezeFunc=func(lifetime.Context)\n"
	bad, err := contextFixtureSignatures(source)
	if err != nil || len(bad) != 1 || bad[0] != "FreezeFunc" {
		t.Fatalf("real-family alias mutant: %v %v", bad, err)
	}
}

var infraContextFreeRoots = []string{"infracontract", "infratransport", "ingressmove", "inframaint", "infraauth", "infratransfer"}

// The production boundary uses the same type-resolved predicate in
// TestBrokerDataplaneEntryPointsAllOS, including aliases and callback fields.
// origin: hot-update S9 plan §11 / T83 / M31
// gate-control: TestInfraDataplaneLifetimeControls
func TestInfraDataplaneLifetimeControls(t *testing.T) {
	if !slices.Equal(infraContextFreeRoots, []string{"infracontract", "infratransport", "ingressmove", "inframaint", "infraauth", "infratransfer"}) {
		t.Fatal("S9 data-plane owner set drifted")
	}
	root := transferFixtureTree(t, map[string]string{})
	path := "internal/infratransport/owner_other.go"
	for _, tc := range []struct {
		source string
		bad    bool
	}{
		{`package owner;import c "context";type Alias=c.Context;type Factory func(Alias) error`, true},
		{`package owner;import c "context";type Owner interface{Move(c.Context)error}`, true},
		{`package owner;import c "context";type Options struct{Stop func(c.Context)};func New(Options){}`, true},
		{`package owner;type Deadline int64;type Stop <-chan struct{};func Move(Deadline,Stop){}`, false},
	} {
		transferWriteFixture(t, root, path, tc.source)
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		bad, err := contextFixtureSignatures(string(raw))
		if err != nil {
			t.Fatal(err)
		}
		if (len(bad) > 0) != tc.bad {
			t.Fatalf("on-disk S9 caller context: %v want bad=%t", bad, tc.bad)
		}
	}
}

// The exception uses the pinned SDK types and the same declaration scanner as
// the AllOS gate. A matching method name alone cannot grant a caller lifetime.
// origin: S10 internal review GATE-FINAL F1 / lead Stream adapter decision
func TestSDKAdapterContextExceptionIsExact(t *testing.T) {
	loaded, err := packages.Load(&packages.Config{Mode: packages.NeedName | packages.NeedTypes | packages.NeedImports | packages.NeedDeps}, "github.com/nats-io/nats.go/jetstream")
	if err != nil || len(loaded) != 1 || len(loaded[0].Errors) != 0 {
		t.Fatalf("load pinned SDK: packages=%v err=%v", loaded, err)
	}
	sdk := loaded[0]
	imports := sdkContextFixtureImporter{sdk.PkgPath: sdk.Types, "context": sdk.Imports["context"].Types}
	for _, tc := range []struct {
		name, source string
		bad          []string
	}{
		{"jetstream", `type adapter struct{j.JetStream}; func(a *adapter) Stream(context.Context, string)(j.Stream,error){return nil,nil}`, nil},
		{"stream", `type adapter struct{j.Stream}; func(a *adapter) GetMsg(context.Context,uint64,...j.GetMsgOpt)(*j.RawStreamMsg,error){return nil,nil}`, nil},
		{"exported-receiver", `type Adapter struct{j.Stream}; func(a *Adapter) GetMsg(context.Context,uint64,...j.GetMsgOpt)(*j.RawStreamMsg,error){return nil,nil}`, []string{"(*fixture.Adapter).GetMsg"}},
		{"partial-implementation", `type adapter struct{}; func(a *adapter) GetMsg(context.Context,uint64,...j.GetMsgOpt)(*j.RawStreamMsg,error){return nil,nil}`, []string{"(*fixture.adapter).GetMsg"}},
		{"extra-method", `type adapter struct{j.Stream}; func(a *adapter) Extra(context.Context) error{return nil}`, []string{"(*fixture.adapter).Extra"}},
		{"non-sdk-interface", `type Stream interface{GetMsg(context.Context,uint64,...j.GetMsgOpt)(*j.RawStreamMsg,error)}; type adapter struct{Stream}; func(a *adapter) GetMsg(context.Context,uint64,...j.GetMsgOpt)(*j.RawStreamMsg,error){return nil,nil}`, []string{"Stream.GetMsg", "(*fixture.adapter).GetMsg"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := token.NewFileSet()
			file, err := parser.ParseFile(set, "fixture.go", `package fixture; import "context"; import j "github.com/nats-io/nats.go/jetstream";`+tc.source, 0)
			if err != nil {
				t.Fatal(err)
			}
			info := &types.Info{Defs: map[*ast.Ident]types.Object{}}
			if _, err := (&types.Config{Importer: imports}).Check("fixture", set, []*ast.File{file}, info); err != nil {
				t.Fatal(err)
			}
			_, bad, err := resolvedFlowSSCallerContexts(file, info, "fixture")
			if err != nil || !slices.Equal(bad, tc.bad) {
				t.Fatalf("SDK context exception: bad=%v want=%v err=%v", bad, tc.bad, err)
			}
		})
	}
}

// Reuse the SDK's context identity: independently importing its dependency
// would produce distinct named types and falsely report incomplete adapters.
type sdkContextFixtureImporter map[string]*types.Package

func (i sdkContextFixtureImporter) Import(path string) (*types.Package, error) {
	if pkg := i[path]; pkg != nil {
		return pkg, nil
	}
	return nil, fmt.Errorf("unexpected SDK fixture import %s", path)
}

// S10's new data-plane exports stay ctx-free: the runtime-sample loop stops on a
// done channel owned by Run, and the store witness consumes sealed descriptors.
// A collection context must never become the lifetime of an observed product
// object. Every S10 production root — internal/ leaves and the test-tree
// harness, judges and launcher alike — creates a fresh context only at an
// annotated root whose reason names its stop owner; S10 has no legacy ledger.
// origin: hot-update S10 plan §5.3 / §12.2 ctx / P28
// gate-control: TestFullstackDataplaneLifetimeControls
var fullstackContextFreeRoots = []string{"drillobserve", "drillobserve/receipt", "drillobserve/infrastructure", "storewitness"}

func checkFullstackContextRoots(tree fullstackTree) (int, error) {
	manifest, err := parseFullstackManifest(tree.docs[fullstackPlanPath])
	if err != nil {
		return 0, err
	}
	owned, err := fullstackOwnedRows(tree.docs[fullstackOwnershipPath])
	if err != nil {
		return 0, err
	}
	scope := fullstackNewCode(tree, manifest, owned)
	for path := range tree.files {
		if fullstackUnder(path, "internal/drillobserve") {
			scope[path] = "P06"
		}
	}
	roots, scanned := 0, map[string]bool{}
	for _, path := range brokerSortedSourcePaths(scope) {
		if !isProdGoFile(path) {
			continue
		}
		for _, dir := range append(slices.Clone(fullstackRoots), "internal/drillobserve") {
			scanned[dir] = scanned[dir] || fullstackUnder(path, dir)
		}
		source := tree.files[path]
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, path, source, parser.ParseComments)
		if err != nil {
			return 0, err
		}
		if bad := ctxBackgroundSites(set, file, source, path); len(bad) > 0 {
			return 0, fmt.Errorf("unowned S10 context roots: %v", bad)
		}
		for _, group := range file.Comments {
			for _, comment := range group.List {
				for _, marker := range ctxAnnotationMarkers {
					if i := strings.Index(comment.Text, marker); i >= 0 {
						if strings.TrimSpace(comment.Text[i+len(marker):]) == "" {
							return 0, fmt.Errorf("%s: S10 context annotation names no stop owner", path)
						}
						roots++
					}
				}
			}
		}
	}
	for _, dir := range append(slices.Clone(fullstackRoots), "internal/drillobserve") {
		if !scanned[dir] {
			return 0, fmt.Errorf("empty S10 context scope %s", dir)
		}
	}
	return roots, nil
}

// fullstackCallerContexts type-checks the ctx-free S10 owners for one platform.
func fullstackCallerContexts(t *testing.T, platform string) []string {
	t.Helper()
	env := []string{}
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "GOOS=") && !strings.HasPrefix(value, "GOARCH=") && !strings.HasPrefix(value, "CGO_ENABLED=") {
			env = append(env, value)
		}
	}
	goos, goarch, _ := strings.Cut(platform, "/")
	env = append(env, "GOOS="+goos, "GOARCH="+goarch, "CGO_ENABLED=0")
	patterns := make([]string, len(fullstackContextFreeRoots))
	for i, root := range fullstackContextFreeRoots {
		patterns[i] = "./internal/" + root
	}
	loaded, err := packages.Load(&packages.Config{Dir: repoRoot(t), Env: env, Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo}, patterns...)
	if err != nil || len(loaded) != len(fullstackContextFreeRoots) {
		t.Fatalf("S10 context owner discovery: %v packages=%d", err, len(loaded))
	}
	var bad []string
	for _, pkg := range loaded {
		if len(pkg.Errors) > 0 {
			t.Fatalf("typecheck %s on %s: %v", pkg.PkgPath, platform, pkg.Errors)
		}
		declarations := 0
		for _, file := range pkg.Syntax {
			count, found, err := resolvedFlowSSCallerContexts(file, pkg.TypesInfo, pkg.PkgPath)
			if err != nil {
				t.Fatal(err)
			}
			declarations += count
			bad = append(bad, found...)
		}
		if declarations == 0 {
			t.Errorf("empty S10 context-free root %s on %s", pkg.PkgPath, platform)
		}
	}
	return bad
}

// origin: hot-update S10 plan §5.3 / §12.2 ctx / P28
func TestFullstackContextRootsAreOwned(t *testing.T) {
	roots, err := checkFullstackContextRoots(fullstackRepoTree(t))
	if err != nil {
		t.Error(err)
	}
	for _, platform := range []string{"linux/amd64", "darwin/arm64"} {
		if bad := fullstackCallerContexts(t, platform); len(bad) > 0 {
			t.Errorf("%s: S10 data-plane exports accept caller contexts: %v", platform, bad)
		}
	}
	t.Logf("S10 annotated context roots: %d", roots)
}

// Root annotations are mutated in copied real sources; the typed predicate reads
// on-disk fixtures through the same declaration walk as the AllOS gate.
// origin: hot-update S10 plan §10.2 P28 / §12.1 wrong-ctx
func TestFullstackDataplaneLifetimeControls(t *testing.T) {
	if !slices.Equal(fullstackContextFreeRoots, []string{"drillobserve", "drillobserve/receipt", "drillobserve/infrastructure", "storewitness"}) {
		t.Fatal("S10 data-plane owner set drifted")
	}
	real := fullstackRepoTree(t)
	root := fullstackFixture(t, real, append(slices.Clone(fullstackRoots), "internal/drillobserve")...)
	check := func() error {
		_, err := checkFullstackContextRoots(readFullstackTree(t, root))
		return err
	}
	proveFullstackMutations(t, root, check,
		fullstackMutation{path: "test/fullstackharness/infra.go", old: "// ctx-root: collector reads end at Close, independently of one query context.\n", new: "", want: "unowned S10 context roots"},
		fullstackMutation{path: "test/fullstackharness/cmd/supervise/main.go", old: "// ctx-root: test launcher signal lifetime.", new: "// ctx-root:", want: "names no stop owner"},
		fullstackMutation{path: "test/hotupdate/acceptance/root.go", new: "package acceptance\n\nimport \"context\"\n\nvar root = context.TODO\n", want: "unowned S10 context roots"},
		fullstackMutation{path: "test/hotupdate/evidencecmd/root.go", new: "package main\n\nimport lifetime \"context\"\n\nfunc root() { _ = lifetime.Background() }\n", want: "unowned S10 context roots"},
		fullstackMutation{path: "internal/storewitness/root_other.go", new: "package storewitness\n\nimport . \"context\"\n\nfunc root() { _ = Background() }\n", want: "unowned S10 context roots"},
		fullstackMutation{path: "internal/drillobserve/root.go", new: "package drillobserve\n\nimport \"context\"\n\nfunc root() { _ = context.Background() }\n", want: "unowned S10 context roots"},
		fullstackMutation{path: "internal/storewitness/witness.go", remove: true, want: "empty S10 context scope internal/storewitness"},
	)
	transferWriteFixture(t, root, "internal/drillobserve/root.go", "package drillobserve\n\nimport \"context\"\n\nfunc root() {\n\t// ctx-root: Flush owns this test-only loop and cancels it on exit.\n\t_ = context.Background()\n}\n")
	if err := check(); err != nil {
		t.Fatalf("reasoned S10 context root refused: %v", err)
	}
	path := "internal/storewitness/capture_other.go"
	for _, tc := range []struct {
		source string
		bad    bool
	}{
		{`package storewitness;import c "context";func Capture(c.Context) error{return nil}`, true},
		{`package storewitness;import c "context";type Alias=c.Context;type Sampler func(Alias)`, true},
		{`package storewitness;import c "context";type Options struct{Stop func(c.Context)};func New(Options){}`, true},
		{`package storewitness;type Done <-chan struct{};func RunRuntimeSamples(Done, func()){}`, false},
	} {
		transferWriteFixture(t, root, path, tc.source)
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		bad, err := contextFixtureSignatures(string(raw))
		if err != nil || (len(bad) > 0) != tc.bad {
			t.Fatalf("on-disk S10 caller context: %v %v want bad=%t", bad, err, tc.bad)
		}
	}
}
