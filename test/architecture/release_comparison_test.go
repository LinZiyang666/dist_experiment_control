package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// release_comparison_test.go — a release string is never compared with == or !=.
//
// origin: prerelease audit round 2, K-F1 / K-F2 / K-F3 / K-F6.
//
// The two ends of a fleet disagree on the leading "v": goreleaser stamps it, a source
// build does not, and `--to-version` is typed by a human who may write either. Every one
// of these comparisons is a CONFIRMATION check — "did the host come back on the version
// we asked for" — so a false negative reports a completed upgrade as unconfirmed, and
// `--all` treats an unconfirmed canary as a failure and leaves the rest of the fleet on
// the old version.
//
// WHY A GATE AND NOT JUST THE FIX. The first sweep converted six call sites and missed
// four, including `AtTarget`'s agent leg one line below its broker leg — and every
// gate stayed green, because `SameRelease` was only ever tested in isolation. That is
// the failure this file exists to make impossible: not "is the helper correct" but "is
// every site using it".
//
// The scan is deliberately narrow. It fires only on an identifier whose NAME says it
// holds a release, so it cannot be satisfied by renaming a variable, and it does not try
// to reason about types.
var releaseIdents = map[string]bool{
	"BrokerVer":      true,
	"AgentVer":       true,
	"ReleaseVersion": true,
	"toVersion":      true,
	"newVersion":     true,
	"startRelease":   true,
}

// releaseComparisonAllowed are the sites where a raw comparison is CORRECT and why.
// Keyed by "<rel path>:<enclosing func>". Each entry must argue for itself.
// Every entry is a TEST asserting a literal it constructed itself. A test that builds
// "v9.9.9" and checks the field came back "v9.9.9" is pinning plumbing, not comparing
// two ends of a fleet — running it through SameRelease would weaken it, because it would
// then also pass for "9.9.9" and stop detecting a v-prefix being dropped in transit.
var releaseComparisonAllowed = map[string]string{
	"cmd/tether/node_versions_test.go:TestCorrelateMissingReportsRenderQuestion":       "asserts the literal it seeded",
	"internal/agent/upgrade_smoke_test.go:TestSmokeVersionParsesTheRealVersionLine":    "asserts the parsed literal",
	"internal/broker/join_version_gate_test.go:TestVersionSkewRefusalDecisions":        "table pins exact refusal inputs",
	"internal/broker/join_version_gate_test.go:TestJoinBundleVersionFieldsAreAdditive": "asserts the literal it seeded",
	"internal/node/node_test.go:TestRegisterIsIdempotentUpsert":                        "asserts the literal it upserted",
}

func TestReleaseStringsAreComparedWithSameRelease(t *testing.T) {
	root := repoRoot(t)
	var offenders []string
	scanned := 0

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "bin":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)

		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, src, 0)
		if perr != nil {
			return nil // not our business; the build catches it
		}
		scanned++
		ast.Inspect(f, func(n ast.Node) bool {
			be, ok := n.(*ast.BinaryExpr)
			if !ok || (be.Op != token.EQL && be.Op != token.NEQ) {
				return true
			}
			if !namesARelease(be.X) && !namesARelease(be.Y) {
				return true
			}
			// A comparison against the empty string is a presence check, not a
			// version comparison, and SameRelease("","") is true — so rewriting
			// those would be wrong.
			if isEmptyString(be.X) || isEmptyString(be.Y) {
				return true
			}
			key := rel + ":" + enclosingFuncKey(f, fset, "", be.Pos())
			key = strings.TrimPrefix(key, ":")
			if _, ok := releaseComparisonAllowed[rel+":"+strings.TrimPrefix(enclosingFuncKey(f, fset, "", be.Pos()), ":")]; ok {
				return true
			}
			offenders = append(offenders, key+"  ("+fset.Position(be.Pos()).String()+")")
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if scanned < 100 {
		t.Fatalf("SELF-CHECK FAILED: only %d Go files parsed — a broken scan reports no offenders, "+
			"which is indistinguishable from success", scanned)
	}
	if len(offenders) > 0 {
		t.Errorf("%d release-string comparison(s) use == / != instead of proto.SameRelease:\n  %s\n\n"+
			"The two ends of a fleet disagree on the leading \"v\", so a raw comparison reports a "+
			"completed upgrade as unconfirmed — and `--all` treats that as a canary failure and "+
			"leaves the rest of the fleet behind. If a site genuinely compares against a literal "+
			"rather than another end of the fleet, add it to releaseComparisonAllowed WITH the reason.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}

func namesARelease(e ast.Expr) bool {
	switch v := e.(type) {
	case *ast.Ident:
		return releaseIdents[v.Name]
	case *ast.SelectorExpr:
		return releaseIdents[v.Sel.Name]
	}
	return false
}

func isEmptyString(e ast.Expr) bool {
	bl, ok := e.(*ast.BasicLit)
	return ok && bl.Kind == token.STRING && (bl.Value == `""` || bl.Value == "``")
}
