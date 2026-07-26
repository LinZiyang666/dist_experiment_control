package httplisten_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Batch-A A12. The three internal HTTP surfaces do not share a loopback policy,
// and the difference is deliberate:
//
//	subhttp         (/sub)      loopback ONLY — unauthenticated, Caddy-fronted
//	clustermanifest (manifest)  loopback ONLY — unauthenticated, Caddy-fronted
//	brokermetrics   (/metrics)  may bind a private interface — scraper reaches it
//
// Now that all three call one Bind, the difference is a bool argument. A flipped
// bool on either of the first two exposes an unauthenticated surface on a public
// VPS, and nothing fails: the listener comes up, the tests pass, the endpoint
// simply answers to the internet.
//
// So the policy is asserted structurally instead of behaviourally. This is also
// why the argument was NOT made implicit per-package: an implicit default is
// exactly what cannot be checked from the outside.
func TestHTTPSurfaceLoopbackPolicy(t *testing.T) {
	want := map[string]bool{
		"internal/subhttp":         true,
		"internal/clustermanifest": true,
		"internal/brokermetrics":   false,
	}

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root = filepath.Dir(filepath.Dir(root)) // internal/httplisten -> repo root

	found := map[string]int{} // pkg -> number of httplisten.Bind calls seen
	for pkg, requireLoopback := range want {
		dir := filepath.Join(root, pkg)
		fset := token.NewFileSet()
		err := filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, p, nil, 0)
			if perr != nil {
				return nil
			}
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Bind" {
					return true
				}
				pkgIdent, ok := sel.X.(*ast.Ident)
				if !ok || pkgIdent.Name != "httplisten" {
					return true
				}
				found[pkg]++
				if len(call.Args) != 3 {
					t.Errorf("%s: httplisten.Bind called with %d args, want 3", pkg, len(call.Args))
					return true
				}
				lit, ok := call.Args[2].(*ast.Ident)
				if !ok || (lit.Name != "true" && lit.Name != "false") {
					t.Errorf("%s: requireLoopback must be a literal true/false so this policy is "+
						"statically checkable; got %T", pkg, call.Args[2])
					return true
				}
				got := lit.Name == "true"
				if got != requireLoopback {
					t.Errorf("%s binds with requireLoopback=%v, policy requires %v.\n"+
						"For /sub and the cluster manifest this is not a style question: they are "+
						"UNAUTHENTICATED, so a false here publishes them on whatever interface the "+
						"operator configured, silently.", pkg, got, requireLoopback)
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", dir, err)
		}
	}

	for pkg := range want {
		if found[pkg] == 0 {
			t.Errorf("%s no longer calls httplisten.Bind — it either grew its own listener again "+
				"(the drift this consolidation removed) or moved; either way this policy check has "+
				"gone blind for it", pkg)
		}
	}
}

// TestNoDirectHTTPListenOutsideHTTPListen keeps the consolidation from eroding:
// a package that goes back to net.Listen for an HTTP surface escapes the policy
// test above entirely.
func TestNoDirectHTTPListenOutsideHTTPListen(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root = filepath.Dir(filepath.Dir(root))

	// External review (gate quality #3): this used to scan three hardcoded
	// packages, so a FOURTH HTTP surface — the case the rule exists to govern —
	// escaped it entirely. Scanning the whole tree and exempting the known
	// non-HTTP listeners inverts that: a new surface is caught by default.
	var offenders []string
	// Packages with legitimate non-HTTP listeners (raft transport, tunnel data
	// plane, admin unix socket, test harnesses). Each is a deliberate exemption.
	exempt := map[string]string{
		"internal/cluster":     "raft mTLS transport listener, not HTTP",
		"internal/tunnel":      "tunnel control + public data-plane listeners, not HTTP",
		"internal/adminsock":   "unix domain socket",
		"internal/httplisten":  "this package IS the wrapper",
		"internal/testharness": "test scaffolding",
		"internal/agent":       "ssproxy local listeners, not HTTP surfaces",
		"internal/proxydial":   "outbound dialer tests",
		// clusteroffline's portBindCheck (doctor.go:206) opens and immediately
		// closes a listener to answer "is this addr bindable?" — a probe, not a
		// served surface. Verified by reading it, not assumed from the name.
		"internal/clusteroffline": "doctor port-bindability probe: Listen+Close, nothing is served",
	}
	var scanDirs []string
	for _, top := range []string{"internal", "cmd"} {
		ents, _ := os.ReadDir(filepath.Join(root, top))
		for _, e := range ents {
			if !e.IsDir() {
				continue
			}
			rel := top + "/" + e.Name()
			if _, ok := exempt[rel]; ok {
				continue
			}
			scanDirs = append(scanDirs, rel)
		}
	}
	for _, pkg := range scanDirs {
		dir := filepath.Join(root, pkg)
		fset := token.NewFileSet()
		_ = filepath.Walk(dir, func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, p, nil, 0)
			if perr != nil {
				return nil
			}
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Listen" {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "net" {
					rel, _ := filepath.Rel(root, p)
					offenders = append(offenders, filepath.ToSlash(rel)+":"+
						strings.TrimSpace(fset.Position(call.Pos()).String()))
				}
				return true
			})
			return nil
		})
	}
	if len(offenders) > 0 {
		t.Errorf("direct net.Listen in an HTTP-surface package: %v.\n"+
			"Route it through httplisten.Bind so the loopback policy stays checkable.", offenders)
	}
}
