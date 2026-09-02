package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// ctx_background_ledger_test.go — every `context.Background()` in production code carries a
// `ctx-root:` / `ctx-none:` annotation, or sits in the draining ledger.
//
// WHY THIS EXISTS
// ---------------
// CLAUDE.md §5 names the only two places a fresh Background context is legitimate (the root of a
// process or loop; a caller that structurally has no ctx — nats.go MsgHandler, raft Apply) and asks
// every NEW site to say which one it is, on the line. That rule was written as "存量 39 处不追溯标注，
// 改到那一行时顺手补" — a promise with no mechanism. On 2026-09-01 the production tree had 44 sites
// and ONE annotation: the 39 had grown by five and nobody had "顺手补" a single one, because nothing
// ever asked. `contextcheck` was judged REJECT-FOREVER, so this is the mechanism instead: a
// site-keyed ledger that only drains, exactly like the naming freeze.
//
// gate-control: TestCtxBackgroundScannerSeesTheShapes

var ctxAnnotationMarkers = []string{"ctx-root:", "ctx-none:"}

// ctxBackgroundSites returns `rel: func` for every context.Background() call in one file that is NOT
// annotated on its own line or the line above. Pure; shared with the self-check.
func ctxBackgroundSites(fset *token.FileSet, f *ast.File, src string, rel string) []string {
	// A trailing annotation covers its own line; an annotation on a comment-only line covers the
	// next line. (The sleep_barrier gate's self-check found the "line above" rule leaking from a
	// trailing marker onto the following statement; same fix here.)
	sameLine, lineAbove := map[int]bool{}, map[int]bool{}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			for _, m := range ctxAnnotationMarkers {
				if !strings.Contains(c.Text, m) {
					continue
				}
				pos := fset.Position(c.Pos())
				start := strings.LastIndex(src[:pos.Offset], "\n") + 1
				if strings.TrimSpace(src[start:pos.Offset]) == "" {
					lineAbove[pos.Line] = true
				} else {
					sameLine[pos.Line] = true
				}
			}
		}
	}
	var sites []string
	var walk func(fn string, n ast.Node)
	walk = func(fn string, n ast.Node) {
		ast.Inspect(n, func(m ast.Node) bool {
			switch v := m.(type) {
			case *ast.FuncDecl:
				if v.Body != nil {
					walk(ctxFuncName(v), v.Body)
				}
				return false
			case *ast.CallExpr:
				sel, ok := v.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Background" {
					return true
				}
				pkg, ok := sel.X.(*ast.Ident)
				if !ok || pkg.Name != "context" {
					return true
				}
				line := fset.Position(v.Pos()).Line
				if sameLine[line] || lineAbove[line-1] {
					return true
				}
				name := fn
				if name == "" {
					name = "<file-scope>"
				}
				sites = append(sites, rel+": "+name)
			}
			return true
		})
	}
	walk("", f)
	return sites
}

func ctxFuncName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	return "(" + ctxRecvName(fd.Recv.List[0].Type) + ")." + fd.Name.Name
}

func ctxRecvName(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return "*" + ctxRecvName(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		return ctxRecvName(t.X)
	case *ast.IndexListExpr:
		return ctxRecvName(t.X)
	}
	return "<unknown-recv>"
}

func TestCtxBackgroundSitesAreAnnotatedOrLedgered(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	seen := map[string]int{}
	for _, top := range []string{"cmd", "internal"} {
		err := filepath.Walk(filepath.Join(root, top), func(p string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if n := info.Name(); n == "testdata" || n == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			src, rerr := os.ReadFile(p)
			if rerr != nil {
				return rerr
			}
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, p, src, parser.ParseComments)
			if perr != nil {
				return perr
			}
			rel, _ := filepath.Rel(root, p)
			for _, s := range ctxBackgroundSites(fset, f, string(src), filepath.ToSlash(rel)) {
				seen[s]++
				if !legacyCtxBackgroundSites[s] {
					offenders = append(offenders, s)
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", top, err)
		}
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d unannotated context.Background() site(s) outside the ledger:\n  %s\n\n"+
			"Say which of CLAUDE.md §5's two legitimate shapes it is, on that line or the one above:\n"+
			"    ctx := context.Background() // ctx-root: <why this is a root>\n"+
			"    // ctx-none: <why the caller structurally has no ctx>\n"+
			"or, better, thread the caller's ctx through.", len(offenders), strings.Join(offenders, "\n  "))
	}
	var stale []string
	for s := range legacyCtxBackgroundSites {
		if seen[s] == 0 {
			stale = append(stale, s)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d ledger entr(y/ies) in legacyCtxBackgroundSites no longer name an unannotated site:\n  %s\n\n"+
			"Delete them — annotating or removing a site drains this ledger in the same commit.",
			len(stale), strings.Join(stale, "\n  "))
	}
	if n := len(legacyCtxBackgroundSites); n > legacyCtxBackgroundSitesCap {
		t.Errorf("legacyCtxBackgroundSites has %d entries, cap is %d — this ledger only drains", n, legacyCtxBackgroundSitesCap)
	} else if n < legacyCtxBackgroundSitesCap {
		t.Errorf("legacyCtxBackgroundSites is down to %d entries but the cap says %d — lower the cap in the same commit", n, legacyCtxBackgroundSitesCap)
	}
}

// TestCtxBackgroundScannerSeesTheShapes is the G2 self-check: one synthetic file with every shape the
// scanner must report and must not.
func TestCtxBackgroundScannerSeesTheShapes(t *testing.T) {
	src := `package synth

import "context"

func bare() {
	_ = context.Background() // reported
}

func sameLine() {
	_ = context.Background() // ctx-root: main of a synthetic program
}

func lineAbove() {
	// ctx-none: nats.go MsgHandler has no ctx
	_ = context.Background()
}

func twoAbove() {
	// ctx-none: too far away to count
	x := 1
	_ = context.Background() // reported
	_ = x
}

func (s *svc) method() {
	_ = context.Background() // reported, keyed (*svc).method
}

func trailingMarkerDoesNotLeak() {
	_ = context.Background() // ctx-root: covers this line only
	_ = context.Background() // reported: the marker above is trailing, not a comment-only line
}

func notContext() {
	_ = other.Background() // not the context package
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synth.go", src, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	got := ctxBackgroundSites(fset, f, src, "synth.go")
	want := []string{"synth.go: bare", "synth.go: twoAbove", "synth.go: (*svc).method", "synth.go: trailingMarkerDoesNotLeak"}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("ctxBackgroundSites drifted:\n got  %v\n want %v", got, want)
	}
}
