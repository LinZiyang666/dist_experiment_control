// Package determinism hosts the §13.1 determinism-lint.
//
// D0 ships only the SKELETON (runnable, with two real checks that already have
// bite); the full Apply-reachability lint lands in D2 once an FSM/Apply root
// exists. See test/determinism/README.md and docs/reviews/d0-plan.md §6.
package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// repoRoot walks up from the test's CWD to the directory holding go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found walking up from CWD")
		}
		dir = parent
	}
}

func notTest(fi fs.FileInfo) bool { return !strings.HasSuffix(fi.Name(), "_test.go") }

// ---------------------------------------------------------------------------
// L-1 — banned-import baseline (§13.1 / §3.3).
//
// The Plan/Apply determinism rule forbids Apply-reachable mutators from
// importing non-deterministic sources (crypto/rand, math/rand, oklog/ulid/v2).
// D0 has no FSM/Apply graph to compute reachability, so instead of FAILING on
// these imports (which are legal today — there is no Plan/Apply split yet) the
// skeleton pins the CURRENT set as a monotonic baseline: a NEW banned import in
// a target package turns this red, and removing a baseline hit also turns it red
// (so D2's tightening is intentional, not accidental).
// ---------------------------------------------------------------------------

var determinismTargetPkgs = []string{"port", "proc", "node", "session", "agentprov"}

var bannedImports = map[string]bool{
	`"crypto/rand"`:               true,
	`"math/rand"`:                 true,
	`"github.com/oklog/ulid/v2"`: true,
}

// bannedBaseline is the exact, code-verified set of banned imports present in
// the target packages at D0. TODO(D2): once the Plan/Apply split lands, tighten
// this to t.Fatal on ANY banned import reachable from Apply.
var bannedBaseline = map[string]string{
	"port": `"crypto/rand"`,
	"proc": `"github.com/oklog/ulid/v2"`,
}

func TestDeterminismBannedImportBaseline(t *testing.T) {
	root := repoRoot(t)
	got := map[string][]string{}
	for _, pkg := range determinismTargetPkgs {
		dir := filepath.Join(root, "internal", pkg)
		fset := token.NewFileSet()
		pkgs, err := parser.ParseDir(fset, dir, notTest, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", dir, err)
		}
		for _, p := range pkgs {
			for _, f := range p.Files {
				for _, imp := range f.Imports {
					if bannedImports[imp.Path.Value] {
						got[pkg] = append(got[pkg], imp.Path.Value)
					}
				}
			}
		}
	}
	// Compare got against the baseline.
	for pkg, imp := range bannedBaseline {
		if len(got[pkg]) != 1 || got[pkg][0] != imp {
			t.Errorf("baseline drift in %q: want exactly [%s], got %v "+
				"(TODO(D2): banned-import gate tightens to t.Fatal once Plan/Apply split exists)", pkg, imp, got[pkg])
		}
	}
	for pkg, imps := range got {
		if _, ok := bannedBaseline[pkg]; !ok {
			t.Errorf("NEW banned import(s) in %q: %v — a target package gained a non-deterministic "+
				"import; if this is intentional, update bannedBaseline and re-check the Plan/Apply contract", pkg, imps)
		}
	}
}

// ---------------------------------------------------------------------------
// L-2 — raft-confinement guard.
//
// Machine-enforces the §19 dependency warning "❌ D1 之前碰任何 apply.*": no
// PRODUCT (non-test) package may import hashicorp/raft or raft-boltdb. In D0 the
// only raft importer is internal/cluster's dependency-verification _test.go
// (excluded from this non-test scan), so the guard passes; internal/cluster is
// whitelisted as forward-compat for D1's product raft node.
// ---------------------------------------------------------------------------

var raftImportPaths = map[string]bool{
	`"github.com/hashicorp/raft"`:             true,
	`"github.com/hashicorp/raft-boltdb/v2"`:   true,
}

func TestRaftConfinedToClusterPackage(t *testing.T) {
	root := repoRoot(t)
	clusterPkg := filepath.Join("internal", "cluster")
	var offenders []string
	for _, base := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, base), func(rel string) {
			// Whitelist is D1-FORWARD: it has no effect in D0 because the only
			// raft importers are internal/cluster's *_test.go files, which
			// walkGoFiles already excludes — so this branch is never hit today.
			// It exists so D1's PRODUCT raft node under internal/cluster won't
			// trip the guard. Do not remove it as "dead code".
			if strings.HasPrefix(rel, clusterPkg+string(os.PathSeparator)) || rel == clusterPkg {
				return // whitelist (forward-compat for D1 product code)
			}
			for _, imp := range fileImports(t, filepath.Join(root, rel)) {
				if raftImportPaths[imp] {
					offenders = append(offenders, rel+" imports "+imp)
				}
			}
		})
	}
	if len(offenders) > 0 {
		sort.Strings(offenders)
		t.Errorf("raft must stay confined to internal/cluster in D0 (§19): \n%s", strings.Join(offenders, "\n"))
	}
}

// ---------------------------------------------------------------------------
// Apply-reachability — D2 stub. Registered so D2 has a named接续点.
// ---------------------------------------------------------------------------

func TestApplyReachabilityDeterminismLint(t *testing.T) {
	t.Skip("D2: Apply-reachability lint (forbid FSM-external INSERT to Apply-owned tables, " +
		"forbid Apply→*sql.DB mutators, column-level activity assertions) — no FSM/Apply root " +
		"exists in D0, so the reachability graph cannot be built yet (§13.1, d0-plan §6). The " +
		"runnable D0 skeleton is the banned-import baseline + raft-confinement guard above.")
}

// ---------------------------------------------------------------------------
// TestNoStrayVersionLiteral — version-prefix SSOT tripwire.
//
// After the D0 v1→v2 flip the subject version lives in exactly one place
// (proto.SubjectVersionToken / SubjectPrefix). No PRODUCT (non-test) file may
// carry a hardcoded "tether.v<N>" string literal except the one legitimate
// import-cycle copy in internal/auth/permissions.go. Scans STRING LITERAL AST
// nodes only (comments and the version.go SSOT — whose literals are "tether."
// and "v2" separately — are not flagged).
// ---------------------------------------------------------------------------

var versionLiteralRe = regexp.MustCompile(`tether\.v[0-9]`)

func TestNoStrayVersionLiteral(t *testing.T) {
	root := repoRoot(t)
	whitelist := map[string]bool{
		filepath.Join("internal", "auth", "permissions.go"): true, // import-cycle copy
		filepath.Join("internal", "proto", "version.go"):    true, // SSOT (defensive; no match expected)
	}
	var offenders []string
	for _, base := range []string{"internal", "cmd"} {
		walkGoFiles(t, filepath.Join(root, base), func(rel string) {
			if whitelist[rel] {
				return
			}
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, filepath.Join(root, rel), nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", rel, err)
			}
			ast.Inspect(f, func(n ast.Node) bool {
				lit, ok := n.(*ast.BasicLit)
				if ok && lit.Kind == token.STRING && versionLiteralRe.MatchString(lit.Value) {
					offenders = append(offenders, rel+": "+lit.Value)
				}
				return true
			})
		})
	}
	if len(offenders) > 0 {
		sort.Strings(offenders)
		t.Errorf("stray hardcoded version-prefix literal(s) — derive from proto.SubjectPrefix / "+
			"proto.SubjectVersionToken instead (the only off-SSOT copy allowed is auth/permissions.go):\n%s",
			strings.Join(offenders, "\n"))
	}
}

// TestNoStrayVersionLiteralSelfCheck proves TestNoStrayVersionLiteral's
// detection is NON-VACUOUS: the same AST scan must actually flag a stray
// "tether.v1" string literal while ignoring an identical token in a comment.
// Without this, a green TestNoStrayVersionLiteral could mean either "no
// offenders" or "the scan is broken".
func TestNoStrayVersionLiteralSelfCheck(t *testing.T) {
	const src = "package x\n" +
		"var s = \"tether.v1.s.lab.cmd\"\n" + // <- must be flagged
		"// a comment mentioning tether.v2 must NOT be flagged\n"
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "synthetic.go", src, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	var hits []string
	ast.Inspect(f, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING && versionLiteralRe.MatchString(lit.Value) {
			hits = append(hits, lit.Value)
		}
		return true
	})
	if len(hits) != 1 {
		t.Fatalf("self-check: expected exactly 1 stray STRING-literal flagged (not the comment), got %d: %v", len(hits), hits)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// walkGoFiles invokes fn(relPath) for every non-test .go file under dir.
func walkGoFiles(t *testing.T, dir string, fn func(rel string)) {
	t.Helper()
	root := repoRoot(t)
	_ = filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		fn(rel)
		return nil
	})
}

// fileImports returns the quoted import paths of a single .go file.
func fileImports(t *testing.T, path string) []string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := make([]string, 0, len(f.Imports))
	for _, imp := range f.Imports {
		out = append(out, imp.Path.Value)
	}
	return out
}
