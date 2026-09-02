package stackharness_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/stackharness"
)

// seed_test.go — SeedSession behaves like the helpers it absorbed, and the absorbed helpers STAY
// absorbed.
//
// THE RECEIPT (docs/testing-standards.md §六 R2). Absorbing eight copies of a helper into one is a
// refactor of test code, and test code has no tests: nothing would notice if p7 quietly grew its own
// seedSession again next month, or if the absorption had silently dropped a suite. So the table
// below names every file whose seedSession was absorbed, and the test asserts two things about each:
// the file still exists, and its `seedSession` is a FORWARDER (a body that calls
// stackharness.SeedSession and nothing else). Same shape as layering_test.go's originalUnion.
//
// gate-control: TestAbsorbedSeedSessionPredicateSeesTheShapes

var absorbedSeedSession = []string{
	"test/chaos/chaos_harness_test.go",
	"test/cli_e2e/harness_test.go",
	"test/p10/upgrade_e2e_test.go",
	"test/p4/ps_filter_test.go",
	"test/p7/audit_e2e_test.go",
	"test/p8/reconcile_e2e_test.go",
	"test/p9/admin_e2e_test.go",
	"test/security/harness_test.go",
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Dir(filepath.Dir(filepath.Dir(thisFile)))
}

// seedSessionShape classifies a file's `seedSession` declaration: "absent", "forwarder" (the body is
// exactly one statement calling stackharness.SeedSession, optionally after t.Helper()), or "own".
// Pure; shared with the self-check.
func seedSessionShape(src []byte) string {
	f, err := parser.ParseFile(token.NewFileSet(), "", src, 0)
	if err != nil {
		return "unparseable"
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "seedSession" || fd.Body == nil {
			continue
		}
		forwards := false
		for _, st := range fd.Body.List {
			es, ok := st.(*ast.ExprStmt)
			if !ok {
				return "own"
			}
			call, ok := es.X.(*ast.CallExpr)
			if !ok {
				return "own"
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return "own"
			}
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == "t" && sel.Sel.Name == "Helper" {
				continue
			}
			if x, ok := sel.X.(*ast.Ident); ok && x.Name == "stackharness" && sel.Sel.Name == "SeedSession" {
				forwards = true
				continue
			}
			return "own"
		}
		if forwards {
			return "forwarder"
		}
		return "own"
	}
	return "absent"
}

func TestAbsorbedSeedSessionsStayForwarders(t *testing.T) {
	root := repoRoot(t)
	sorted := append([]string(nil), absorbedSeedSession...)
	sort.Strings(sorted)
	if strings.Join(sorted, "\n") != strings.Join(absorbedSeedSession, "\n") {
		t.Fatalf("keep absorbedSeedSession sorted")
	}
	for _, rel := range absorbedSeedSession {
		src, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
		if err != nil {
			t.Errorf("%s: absorbed file no longer exists — remove it from the table WITH the reason in the commit message", rel)
			continue
		}
		switch shape := seedSessionShape(src); shape {
		case "forwarder":
		case "absent":
			t.Errorf("%s: no seedSession at all — the call sites moved to stackharness.SeedSession directly? Then remove the row", rel)
		default:
			t.Errorf("%s: seedSession is %q, not a forwarder to stackharness.SeedSession — the copy grew back", rel, shape)
		}
	}
	if len(absorbedSeedSession) < 8 {
		t.Fatalf("absorbedSeedSession has %d rows; 8 were absorbed on 2026-09-01", len(absorbedSeedSession))
	}
}

func TestAbsorbedSeedSessionPredicateSeesTheShapes(t *testing.T) {
	cases := map[string]string{
		"forwarder": "package x\nfunc seedSession(t *testing.T, db *sql.DB, sid, fp string) {\n\tt.Helper()\n\tstackharness.SeedSession(t, db, sid, fp)\n}\n",
		"own":       "package x\nfunc seedSession(t *testing.T, db *sql.DB, sid, fp string) {\n\tt.Helper()\n\tif _, err := session.Create(db, sid, sid, fp, \"x\", time.Now()); err != nil {\n\t\tt.Fatal(err)\n\t}\n}\n",
		"absent":    "package x\nfunc seedOther(t *testing.T) {}\n",
	}
	for want, src := range cases {
		if got := seedSessionShape([]byte(src)); got != want {
			t.Errorf("shape %q: got %q", want, got)
		}
	}
	// A forwarder that ALSO does something else is "own": the point of the receipt is that the
	// suite carries no session-seeding logic of its own.
	extra := "package x\nfunc seedSession(t *testing.T, db *sql.DB, sid, fp string) {\n\tstackharness.SeedSession(t, db, sid, fp)\n\tdb.Exec(\"UPDATE sessions SET x=1\")\n}\n"
	if got := seedSessionShape([]byte(extra)); got != "own" {
		t.Errorf("forwarder-plus-extra must read as own, got %q", got)
	}
}

// TestSeedSessionCreatesAnActiveOwnedSession is the behavioural half: the absorbed helpers all
// produced a session the broker treats as ACTIVE and owned by fp.
func TestSeedSessionCreatesAnActiveOwnedSession(t *testing.T) {
	db := testharness.OpenDB(t)
	_, fp := testharness.FreshUserPub(t)
	stackharness.SeedSession(t, db, "lab", fp)
	var state, owner, pin string
	if err := db.QueryRow(`SELECT state, owner_pubkey_fp, pin_hash FROM sessions WHERE sid='lab'`).Scan(&state, &owner, &pin); err != nil {
		t.Fatal(err)
	}
	if state != "ACTIVE" || owner != fp || pin != stackharness.PlaceholderPINHash {
		t.Fatalf("seeded session: state=%q owner=%q pin=%q", state, owner, pin)
	}
}
