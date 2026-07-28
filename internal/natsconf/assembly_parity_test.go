package natsconf

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// assembly_parity_test.go — there must be exactly ONE production assembly of a natsconf.Config in the
// whole tree, and it must be RenderDesired (B5, roadmap line 464).
//
// WHAT THIS REPLACED, AND WHY THE REPLACEMENT IS STRONGER
// ------------------------------------------------------
// Config used to be hand-assembled at FIVE places across three packages, all funnelling into
// BuildMergedConf. The first version of this gate compared their field VOCABULARIES and made every
// difference declare itself in a table with a reason. That was the right tool for five assemblies, and
// it did real work — it caught two factual errors in the hand-written table on its very first run.
//
// But a declared-asymmetry table is a way of LIVING with duplication, not of removing it, and its own
// note said so in the present tense: "the assemblies collapse into one RenderDesired call, or they keep
// saying what they mean". They have now collapsed. So the property worth policing is no longer "the five
// vocabularies agree" — it is "there is only one".
//
// This is strictly stronger. The old gate could only see fields a call site MENTIONED; a field it
// mentioned nowhere (natsconf.Config.JSDomain, read by the reconciler's literal and set by nobody — the
// exact vector the roadmap worried about) was invisible to it until someone wired it. With one assembly
// there is no second place for a field to be missing from.
//
// THE FAILURE IT EXISTS FOR
// -------------------------
// Someone needs a conf rendered on a new path, writes `natsconf.Config{…}` because that is what the
// examples in git history show, and now there are two intents again. The consequence is the one this
// project already had: the grow cutover applies a conf the reconciler immediately wants to replace — an
// unplanned nats.conf swap + SIGHUP on a broker that has just restarted.
//
// WHAT TO DO WHEN IT FAILS
// ------------------------
// Do not add your file to an allow-list; there is deliberately no allow-list. Call RenderDesired with the
// intent you mean. If none of the four intents fits, that is a real finding and the fifth intent belongs
// in render_desired.go next to the other four, where the next reader can see all of them at once — which
// is the entire point of naming them.

// theOnlyAssembly is where every rendered nats.conf in this project comes from.
const theOnlyAssembly = "internal/natsconf/render_desired.go"

func TestRenderDesiredIsTheOnlyConfigAssembly(t *testing.T) {
	root := repoRootForAssemblyParity(t)

	found := map[string]map[string]bool{}
	for _, rel := range productionGoFiles(t, root) {
		fields := configLiteralFields(t, filepath.Join(root, rel), rel)
		if len(fields) > 0 {
			found[rel] = fields
		}
	}

	// Non-vacuity FIRST: if the scanner cannot see the one assembly that certainly exists, every other
	// assertion in this file is trivially satisfied and the gate has gone silently blind — which is worse
	// than a divergence, because a divergence at least fails.
	baseline := found[theOnlyAssembly]
	if len(baseline) < 6 {
		t.Fatalf("%s mentions only %d Config field(s) (%v) — the scanner is not reading what it thinks "+
			"it is. Every 'no other assembly exists' assertion below is vacuous until this is fixed.",
			theOnlyAssembly, len(baseline), sortedKeys(baseline))
	}

	var others []string
	for rel := range found {
		if rel != theOnlyAssembly {
			others = append(others, rel+" "+strings.Join(sortedKeys(found[rel]), ","))
		}
	}
	if len(others) > 0 {
		sort.Strings(others)
		t.Errorf("natsconf.Config is assembled outside %s:\n  %s\n\n"+
			"Two assemblies means two INTENTS, and an intent that lives in two places is an intent that "+
			"diverges in one. Call RenderDesired with the intent you mean; if no existing intent fits, add "+
			"the new one to RenderIntent rather than a second assembly here.",
			theOnlyAssembly, strings.Join(others, "\n  "))
	}
}

// TestConfigAssemblyScannerActuallyDetectsASecondAssembly proves the scanner above has the discriminating
// power it claims. It runs over SYNTHESIZED sources rather than the tree, because the tree's success
// condition is "no second assembly exists" — a non-vacuity assertion against real input would fail
// exactly when the codebase is correct (testing-standards G2).
//
// Both spellings are checked, because the assemblies used to live on both sides of the package boundary:
// an external caller writes `natsconf.Config{…}` and a file inside this package writes `Config{…}`. And
// the bare form must be recognised ONLY inside internal/natsconf — three other packages in this repo
// have their own unrelated `Config` type (broker.Config, cluster.Config), and matching a bare `Config{`
// anywhere would report all of them.
func TestConfigAssemblyScannerActuallyDetectsASecondAssembly(t *testing.T) {
	cases := []struct {
		name   string
		rel    string
		src    string
		expect []string
	}{
		{
			name: "qualified literal in another package",
			rel:  "internal/broker/somefile.go",
			src: `package broker

import "github.com/LinZiyang666/tether/internal/natsconf"

func render() (string, error) {
	return natsconf.BuildMergedConf(nil, natsconf.Config{Standalone: true, Local: natsconf.Broker{}})
}
`,
			expect: []string{"Local", "Standalone"},
		},
		{
			name: "bare literal INSIDE internal/natsconf",
			rel:  "internal/natsconf/somefile.go",
			src: `package natsconf

func render(own *Ownership) (string, error) {
	return BuildMergedConf(own, Config{Standalone: false, Peers: nil, ClusterName: "x"})
}
`,
			expect: []string{"ClusterName", "Peers", "Standalone"},
		},
		{
			name: "bare literal OUTSIDE internal/natsconf is a DIFFERENT type and must be ignored",
			rel:  "internal/cluster/production.go",
			src: `package cluster

func build() {
	_ = New(Config{LocalID: "n1", DataDir: "/var/lib"})
}
`,
			expect: nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "synthetic.go")
			if err := os.WriteFile(path, []byte(tc.src), 0o600); err != nil {
				t.Fatal(err)
			}
			got := sortedKeys(configLiteralFields(t, path, tc.rel))
			if strings.Join(got, ",") != strings.Join(tc.expect, ",") {
				t.Errorf("scanner returned %v, want %v.\nA scanner that misses this shape lets a second "+
					"assembly ship; one that over-reports it makes the gate cry wolf on an unrelated "+
					"package's own Config type.", got, tc.expect)
			}
		})
	}
}

// productionGoFiles returns every non-test .go file in the repo, repo-relative. Test files are excluded
// on purpose: a test that builds a Config to exercise Render is doing exactly what it should, and
// forbidding it would make the gate unusable. The uniqueness property is about PRODUCTION intent.
func productionGoFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "dist":
				return fs.SkipDir
			}
			return nil
		}
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		out = append(out, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
	if len(out) < 100 {
		t.Fatalf("only %d production .go files found under %s — the walk is not seeing the tree, so a "+
			"second assembly would go unreported", len(out), root)
	}
	return out
}

// configLiteralFields returns the field names mentioned by every natsconf.Config composite literal in a
// file, unioned. rel is the file's repo-relative path and decides whether the BARE `Config{…}` spelling
// counts: inside internal/natsconf it is natsconf.Config, and everywhere else it is somebody else's type.
func configLiteralFields(t *testing.T, path, rel string) map[string]bool {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	bareCounts := strings.HasPrefix(filepath.ToSlash(rel), "internal/natsconf/")
	out := map[string]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		switch typ := cl.Type.(type) {
		case *ast.SelectorExpr:
			if typ.Sel.Name != "Config" {
				return true
			}
			pkg, ok := typ.X.(*ast.Ident)
			if !ok || pkg.Name != "natsconf" {
				return true
			}
		case *ast.Ident:
			if typ.Name != "Config" || !bareCounts {
				return true
			}
		default:
			return true
		}
		for _, e := range cl.Elts {
			kv, ok := e.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			if id, ok := kv.Key.(*ast.Ident); ok {
				out[id.Name] = true
			}
		}
		return true
	})
	return out
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func repoRootForAssemblyParity(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 8; i++ {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	t.Fatal("could not locate the repo root (no go.mod found walking up) — the gate cannot run")
	return ""
}
