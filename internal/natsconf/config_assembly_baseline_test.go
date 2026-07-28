package natsconf

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

// config_assembly_baseline_test.go — the uniqueness gate again, with the scanner that sees what a
// composite-literal walk cannot.
//
// origin: adversarial review of B5 (lane B5), reviewer-authored; rewritten when the five assemblies
// collapsed into RenderDesired. It complements assembly_parity_test.go and does not replace it.
//
// WHY TWO SCANNERS
// ----------------
// `Config{…}` literal keys are not the only way a Config gets built. This compiles, renders, and is
// completely invisible to an ast.CompositeLit walk that looks at literal fields:
//
//	var cfg natsconf.Config
//	cfg.Standalone = false
//	cfg.Peers = peers
//	return natsconf.BuildMergedConf(own, cfg)
//
// That is not a hypothetical shape either — it is what a mutation on the OLD version of this gate used
// to defeat it: rewriting the reconciler's `cfg := Config{…}` as `var cfg Config` plus assignments left
// the value identical and the gate blind. (Back then the blindness surfaced as a Fatal, because the gate
// expected that file to have a literal. Under the uniqueness rule the same rewrite would surface as
// SILENCE — a second assembly that simply is not seen — which is why this file now walks the whole tree
// rather than a fixed list.)
//
// So: assembly_parity_test.go catches `natsconf.Config{Field: v}`; this catches `cfg.Field = v`. Neither
// substitutes for the other (testing-standards S1), and TestTheTwoScannersHaveDifferentBlindSpots below
// fails if they ever converge, so nobody keeps two scanners that claim the same thing.
//
// THE BLIND SPOT THIS FILE USED TO DECLARE IS NOW CLOSED (external review B2-6)
// -----------------------------------------------------------------------------
// It used to say: "a Config field set inside a METHOD (Config.ApplySecretsDirIdentity sets four) is
// invisible to both. That is not fixable by a better parser." The second sentence was wrong. The
// reviewer supplied the counterexample that mattered — mutation through a typed PARAMETER:
//
//	func applyLocalIntent(desired *natsconf.Config) { desired.Standalone = true }
//	func build(own *natsconf.Ownership) (string, error) {
//	    var cfg natsconf.Config
//	    applyLocalIntent(&cfg)
//	    return natsconf.BuildMergedConf(own, cfg)
//	}
//
// That is an ordinary refactor, not an exotic bypass, and it was a fully invisible second production
// assembly. The scanner now records Config-typed parameters, results and receivers, which closes both
// the parameter case and the method case.
//
// Closing it immediately surfaced two mutators INSIDE this package, and they are the reason the rule
// below is scoped rather than absolute: they are the shared pipeline itself, not competing assemblies.
// See renderPipelineMutators.

// renderPipelineMutators are the files INSIDE internal/natsconf that legitimately mutate a Config,
// each with the reason. They are the render pipeline every caller funnels through, not alternatives to
// it — a caller cannot reach them except by calling RenderDesired or BuildMergedConf.
//
// This list is not a softening of the rule; it is what makes the rule checkable at all. Before the
// scanner saw parameters and receivers these two were invisible, so "exactly one assembly" was true only
// of the shapes the scanner happened to match. Now they are enumerated, and a THIRD mutator appearing
// inside this package has to justify itself here rather than arriving silently.
// The key is "<repo-relative file>:<function>" — a FUNCTION, not a file (external review RB2-3).
// A file-wide entry silently covered any second helper added to the same file, which is the same
// allow-list decay G3 forbids one level up.
var renderPipelineMutators = map[string]string{
	"internal/natsconf/reconcile.go:ApplySecretsDirIdentity": "the ONE derivation of the routes-mTLS " +
		"identity + route listen, shared by RenderDesired's secrets-dir path. It exists precisely so the " +
		"grow cutover and the reconciler cannot derive it differently; the export it replaced had a doc " +
		"comment saying it was exported to be copied byte-for-byte.",
	"internal/natsconf/takeover.go:BuildMergedConf": "the HARVEST FALLBACKS — it fills MonitorListen, the " +
		"three route-mTLS paths, ClusterListen and ClusterName from the LIVE conf when the caller left " +
		"them empty. That is the whole reason callers may say nothing about a field, and it happens after " +
		"every assembly rather than instead of one.",
}

func TestNoConfigIsAssembledByFieldAssignmentOutsideRenderDesired(t *testing.T) {
	root := repoRootForAssemblyParity(t)

	// Keyed by "<file>:<function>" so an exemption covers ONE function (RB2-3).
	found := map[string]map[string]bool{}
	fileFields := map[string]map[string]bool{}
	for _, rel := range productionGoFiles(t, root) {
		for fn, fields := range configFieldsByFunction(t, filepath.Join(root, rel), rel) {
			if len(fields) == 0 {
				continue
			}
			found[rel+":"+fn] = fields
			if fileFields[rel] == nil {
				fileFields[rel] = map[string]bool{}
			}
			for k := range fields {
				fileFields[rel][k] = true
			}
		}
	}

	baseline := fileFields[theOnlyAssembly]
	if len(baseline) < 6 {
		t.Fatalf("%s mentions only %d Config field(s) (%v) — this scanner is not reading what it thinks "+
			"it is, and the uniqueness assertion below is vacuous until that is fixed",
			theOnlyAssembly, len(baseline), sortedFieldNames(baseline))
	}

	var others []string
	for key := range found {
		file := key[:strings.LastIndex(key, ":")]
		if file == theOnlyAssembly || renderPipelineMutators[key] != "" {
			continue
		}
		others = append(others, key+" "+strings.Join(sortedFieldNames(found[key]), ","))
	}
	if len(others) > 0 {
		sort.Strings(others)
		t.Errorf("natsconf.Config is assembled or mutated outside %s (by literal, field assignment, or "+
			"through a Config-typed parameter/receiver):\n  %s\n\n"+
			"Call RenderDesired with the intent you mean. `var cfg natsconf.Config` + assignments, and "+
			"handing a *natsconf.Config to a helper, are the two shapes that hide a second assembly from "+
			"a literal-only scan — both are what this check exists for. If the mutation genuinely belongs "+
			"to the shared render pipeline inside internal/natsconf, add it to renderPipelineMutators WITH "+
			"the reason. The key is file:FUNCTION — adding a second helper to an already-listed file does "+
			"NOT inherit its exemption.", theOnlyAssembly, strings.Join(others, "\n  "))
	}

	// A stale entry is worse than no entry: it silently exempts whatever that function becomes next.
	for key, why := range renderPipelineMutators {
		if len(found[key]) == 0 {
			t.Errorf("renderPipelineMutators lists %s (%q) but that function no longer mutates a Config — "+
				"remove the entry in the same commit, or the exemption outlives the thing it excused",
				key, why)
		}
	}
}

// TestTheTwoScannersHaveDifferentBlindSpots pins that this file is not redundant with
// assembly_parity_test.go. Run over SYNTHESIZED source, because the property is about the SCANNERS, and
// the tree's success state (one assembly, written as a literal) cannot exhibit the difference
// (testing-standards G2).
func TestTheTwoScannersHaveDifferentBlindSpots(t *testing.T) {
	const src = `package broker

import "github.com/LinZiyang666/tether/internal/natsconf"

func build() natsconf.Config {
	cfg := natsconf.Config{Local: natsconf.Broker{}, Peers: nil}
	cfg.MonitorListen = "127.0.0.1:8223"
	cfg.ClusterName = "tether"
	return cfg
}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "synthetic.go")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	const rel = "internal/broker/synthetic.go"

	assigns := configFieldsIncludingAssignments(t, path, rel)
	for _, want := range []string{"Local", "Peers", "MonitorListen", "ClusterName"} {
		if !assigns[want] {
			t.Errorf("this file's scanner missed %q (found %v) — a field set by assignment after the "+
				"literal is exactly what it exists to see", want, sortedFieldNames(assigns))
		}
	}

	litOnly := configLiteralFields(t, path, rel)
	if litOnly["MonitorListen"] || litOnly["ClusterName"] {
		t.Error("assembly_parity_test.go's scanner now sees assignment-set fields too. If that is a " +
			"deliberate improvement, DELETE this file rather than leaving two scanners that claim the " +
			"same thing — two gates asserting one property is how a suite grows without gaining coverage.")
	}
	if !litOnly["Local"] || !litOnly["Peers"] {
		t.Errorf("the literal-only scanner must still see literal keys, got %v", sortedFieldNames(litOnly))
	}
}

// TestConfigAssemblyScannerSeesTypedHelperMutation is an independent external-review counterexample.
// A helper parameter is an ordinary way to move assembly details out of the caller; if the uniqueness
// gate cannot see it, a second production assembly can pass while the gate still reports exactly one.
func TestConfigAssemblyScannerSeesTypedHelperMutation(t *testing.T) {
	const src = `package broker

import "github.com/LinZiyang666/tether/internal/natsconf"

func applyLocalIntent(desired *natsconf.Config) {
	desired.Standalone = true
}

func build(own *natsconf.Ownership) (string, error) {
	var cfg natsconf.Config
	applyLocalIntent(&cfg)
	return natsconf.BuildMergedConf(own, cfg)
}
`
	dir := t.TempDir()
	path := filepath.Join(dir, "synthetic.go")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	const rel = "internal/broker/synthetic.go"
	if fields := configFieldsIncludingAssignments(t, path, rel); !fields["Standalone"] {
		t.Fatalf("assignment-aware scanner missed Config mutation through a typed helper parameter "+
			"(found %v); the claimed whole-tree uniqueness gate can therefore false-green",
			sortedFieldNames(fields))
	}
}

// TestConfigAssemblyScannerSeesOrdinaryTypeSpellings keeps the whole-tree claim honest across normal
// Go refactors. Neither an import alias nor a local type alias changes the type being mutated.
func TestConfigAssemblyScannerSeesOrdinaryTypeSpellings(t *testing.T) {
	cases := []struct {
		name string
		src  string
	}{
		{
			name: "import alias",
			src: `package broker

import nc "github.com/LinZiyang666/tether/internal/natsconf"

func mutate(desired *nc.Config) {
	desired.Standalone = true
}
`,
		},
		{
			name: "type alias",
			src: `package broker

import "github.com/LinZiyang666/tether/internal/natsconf"

type desiredConfig = natsconf.Config

func mutate(desired *desiredConfig) {
	desired.Standalone = true
}
`,
		},
		{
			name: "pointer composite literal",
			src: `package broker

import "github.com/LinZiyang666/tether/internal/natsconf"

func mutate() {
	desired := &natsconf.Config{}
	desired.Standalone = true
}
`,
		},
		{
			name: "new expression",
			src: `package broker

import "github.com/LinZiyang666/tether/internal/natsconf"

func mutate() {
	desired := new(natsconf.Config)
	desired.Standalone = true
}
`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "synthetic.go")
			if err := os.WriteFile(path, []byte(tc.src), 0o600); err != nil {
				t.Fatal(err)
			}
			if fields := configFieldsIncludingAssignments(t, path, "internal/broker/synthetic.go"); !fields["Standalone"] {
				t.Fatalf("assignment-aware scanner missed Config mutation through %s (found %v); "+
					"the source still compiles to the same natsconf.Config mutation, so the uniqueness "+
					"gate can false-green after an ordinary refactor", tc.name, sortedFieldNames(fields))
			}
		})
	}
}

// configFieldsIncludingAssignments returns every Config field a file mentions: composite-literal keys
// UNION fields assigned through a variable that holds a Config — whether that variable was initialised
// from a literal (`cfg := natsconf.Config{…}`) or merely declared (`var cfg natsconf.Config`).
//
// rel decides whether the bare `Config` spelling counts, for the same reason as in
// assembly_parity_test.go: three other packages here have their own `Config` type.
// configFieldsByFunction is configFieldsIncludingAssignments split per enclosing top-level function, so
// an exemption can name ONE function instead of a whole file (RB2-3). A method is keyed by its own
// name; anything at file scope lands under "<file-scope>".
func configFieldsByFunction(t *testing.T, path, rel string) map[string]map[string]bool {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := map[string]map[string]bool{}
	// The whole-file scan is the source of truth for WHICH fields are touched; attributing them to a
	// function is a second pass over the same AST, so the two can never disagree about the field set.
	whole := configFieldsIncludingAssignments(t, path, rel)
	if len(whole) == 0 {
		return out
	}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		sub := configFieldsInNode(fn, rel, f)
		if len(sub) > 0 {
			out[fn.Name.Name] = sub
		}
	}
	// Anything the whole-file scan saw that no function claimed (a package-level var initialised from a
	// Config literal) is still reported, under a name that cannot be mistaken for a function.
	claimed := map[string]bool{}
	for _, sub := range out {
		for k := range sub {
			claimed[k] = true
		}
	}
	leftover := map[string]bool{}
	for k := range whole {
		if !claimed[k] {
			leftover[k] = true
		}
	}
	if len(leftover) > 0 {
		out["<file-scope>"] = leftover
	}
	return out
}

func configFieldsIncludingAssignments(t *testing.T, path, rel string) map[string]bool {
	t.Helper()
	src, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return configFieldsInNode(f, rel, f)
}

// configFieldsInNode is the ONE matcher. `scan` is the subtree to walk (the whole file, or one
// FuncDecl); `f` is always the enclosing file, because imports and type aliases are file-scoped facts
// that a per-function walk still has to consult.
//
// Deliberately not duplicated per caller: the per-function variant exists only to attribute fields to a
// function, and a second copy of the matching logic would let the two disagree — which is the defect
// that made the sibling enablement gate's self-check meaningless until this same round.
func configFieldsInNode(scan ast.Node, rel string, f *ast.File) map[string]bool {
	bareCounts := strings.HasPrefix(filepath.ToSlash(rel), "internal/natsconf/")

	// RB2-3: resolve the package identifier from the IMPORT DECLARATION rather than assuming it spells
	// `natsconf`, and collect local `type X = natsconf.Config` aliases. Both are ordinary,
	// semantics-preserving Go refactors that left the previous matcher — which hard-coded the identifier
	// `natsconf` and the bare name `Config` — returning an empty field set for a real second assembly.
	//
	// This is still AST-only, not go/types. The reviewer recommended a full resolver and that remains the
	// complete answer; what is implemented here closes the two spellings they demonstrated plus the
	// transitive alias chain, and the two counterexample rows stay as permanent negative controls so a
	// third spelling arrives as a red rather than as silence. The residual gap is named in the doc comment
	// on this function.
	pkgIdents := map[string]bool{}
	for _, imp := range f.Imports {
		if strings.Trim(imp.Path.Value, `"`) != "github.com/LinZiyang666/tether/internal/natsconf" {
			continue
		}
		if imp.Name != nil {
			if imp.Name.Name != "_" && imp.Name.Name != "." {
				pkgIdents[imp.Name.Name] = true
			}
			continue
		}
		pkgIdents["natsconf"] = true // default identifier is the package name
	}

	// isConfigType unwraps a pointer, because B2-6's counterexample mutates through a `*natsconf.Config`
	// PARAMETER and a value-only matcher cannot see it.
	var isConfigType func(e ast.Expr) bool
	isConfigType = func(e ast.Expr) bool {
		switch typ := e.(type) {
		case *ast.StarExpr:
			return isConfigType(typ.X)
		case *ast.SelectorExpr:
			pkg, ok := typ.X.(*ast.Ident)
			return ok && pkgIdents[pkg.Name] && typ.Sel.Name == "Config"
		case *ast.Ident:
			return typ.Name == "Config" && bareCounts
		}
		return false
	}

	// Local type aliases. Repeated to a fixed point so `type A = natsconf.Config; type B = A` resolves;
	// the file's declaration count bounds the iterations, so a cycle cannot spin.
	aliasNames := map[string]bool{}
	for pass := 0; pass < len(f.Decls)+1; pass++ {
		grew := false
		ast.Inspect(f, func(n ast.Node) bool {
			ts, ok := n.(*ast.TypeSpec)
			if !ok || !ts.Assign.IsValid() || aliasNames[ts.Name.Name] {
				return true
			}
			// A defined type (`type X natsconf.Config`, no `=`) is a DIFFERENT type with the same fields;
			// mutating it is not mutating a natsconf.Config, so only true aliases count.
			if isConfigType(ts.Type) {
				aliasNames[ts.Name.Name] = true
				grew = true
			}
			return true
		})
		if !grew {
			break
		}
		// Fold the newly-known aliases into the type test for the next pass.
		prev := isConfigType
		isConfigType = func(e ast.Expr) bool {
			if id, ok := e.(*ast.Ident); ok && aliasNames[id.Name] {
				return true
			}
			if st, ok := e.(*ast.StarExpr); ok {
				if id, ok := st.X.(*ast.Ident); ok && aliasNames[id.Name] {
					return true
				}
			}
			return prev(e)
		}
	}

	out := map[string]bool{}
	configVars := map[string]bool{}

	// expressionHoldsConfig recognises values whose static spelling makes them Config-shaped. Pointer
	// construction and new(Config) are ordinary assembly forms; stopping at a bare CompositeLit made
	// `cfg := &natsconf.Config{}; cfg.Standalone = true` invisible. Identifier propagation is evaluated
	// to a fixed point below, so `view := cfg` cannot shed the type merely by adding a local.
	var expressionHoldsConfig func(ast.Expr) bool
	expressionHoldsConfig = func(e ast.Expr) bool {
		switch node := e.(type) {
		case *ast.ParenExpr:
			return expressionHoldsConfig(node.X)
		case *ast.CompositeLit:
			return isConfigType(node.Type)
		case *ast.UnaryExpr:
			return expressionHoldsConfig(node.X)
		case *ast.CallExpr:
			id, ok := node.Fun.(*ast.Ident)
			return ok && id.Name == "new" && len(node.Args) == 1 && isConfigType(node.Args[0])
		case *ast.Ident:
			return configVars[node.Name]
		}
		return false
	}

	// Pass 1: literal keys, plus every variable known to hold a Config — typed parameters/declarations,
	// values, pointers, new(Config), and transitive local aliases. Rewalk until no identifier is added;
	// the set only grows, so this terminates after at most the number of identifiers in the subtree.
	for {
		grew := false
		ast.Inspect(scan, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.CompositeLit:
				if !isConfigType(node.Type) {
					return true
				}
				for _, e := range node.Elts {
					if kv, ok := e.(*ast.KeyValueExpr); ok {
						if id, ok := kv.Key.(*ast.Ident); ok {
							out[id.Name] = true
						}
					}
				}
			case *ast.AssignStmt:
				for i, rhs := range node.Rhs {
					if i >= len(node.Lhs) || !expressionHoldsConfig(rhs) {
						continue
					}
					if id, ok := node.Lhs[i].(*ast.Ident); ok && id.Name != "_" && !configVars[id.Name] {
						configVars[id.Name] = true
						grew = true
					}
				}
			case *ast.ValueSpec:
				for i, name := range node.Names {
					holds := isConfigType(node.Type)
					if !holds && i < len(node.Values) {
						holds = expressionHoldsConfig(node.Values[i])
					}
					if holds && name.Name != "_" && !configVars[name.Name] {
						configVars[name.Name] = true
						grew = true
					}
				}
			// external review B2-6: a PARAMETER typed Config or *Config is a Config-holding variable too.
			case *ast.FuncDecl:
				before := len(configVars)
				collectConfigParams(node.Type, configVars, isConfigType)
				if node.Recv != nil {
					collectConfigFields(node.Recv.List, configVars, isConfigType)
				}
				grew = grew || len(configVars) != before
			case *ast.FuncLit:
				before := len(configVars)
				collectConfigParams(node.Type, configVars, isConfigType)
				grew = grew || len(configVars) != before
			}
			return true
		})
		if !grew {
			break
		}
	}

	// Pass 2: assignments to a field of one of those variables.
	ast.Inspect(scan, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok {
			return true
		}
		for _, lhs := range as.Lhs {
			sel, ok := lhs.(*ast.SelectorExpr)
			if !ok {
				continue
			}
			if base, ok := sel.X.(*ast.Ident); ok && configVars[base.Name] {
				out[sel.Sel.Name] = true
			}
		}
		return true
	})
	return out
}

// collectConfigParams records every parameter (and named result) of a signature whose type is Config or
// *Config, so pass 2 can attribute field assignments made through it.
func collectConfigParams(sig *ast.FuncType, into map[string]bool, isConfigType func(ast.Expr) bool) {
	if sig == nil {
		return
	}
	if sig.Params != nil {
		collectConfigFields(sig.Params.List, into, isConfigType)
	}
	if sig.Results != nil {
		collectConfigFields(sig.Results.List, into, isConfigType)
	}
}

func collectConfigFields(fields []*ast.Field, into map[string]bool, isConfigType func(ast.Expr) bool) {
	for _, fld := range fields {
		if !isConfigType(fld.Type) {
			continue
		}
		for _, name := range fld.Names {
			if name.Name != "_" {
				into[name.Name] = true
			}
		}
	}
}

func sortedFieldNames(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
