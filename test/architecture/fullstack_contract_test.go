package architecture

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// fullstack_contract_test.go — the P00 acceptance contract stays frozen, and its
// permanent failure stubs stay inert and unreachable from every real path.
//
// Plan §5.1 makes P00 the single owner of the DTOs, ports and closed vocabulary;
// implementations live in their final packages and are injected as factories.
// Three source shapes would quietly turn that boundary into a compile-only
// witness signing production behaviour, and each is read from disk here rather
// than trusted from P00's own test:
//   - a contract field, port signature or vocabulary member changes without
//     returning to P00 (an "unknown vocabulary" accepted by both sides);
//   - a stub starts succeeding, loses its owner text or gains a side effect;
//   - a fixture, driver or product imports contractstub in place of a real
//     factory, or a real factory is missing, re-shaped or an empty success.
//
// origin: hot-update S10 plan §5.1 / §10.2 P00 / P28 / §12.1
// gate-control: TestFullstackContractGateControls

const (
	fullstackContractImport = modPrefix + "test/hotupdate/acceptance/contract"
	fullstackStubImport     = modPrefix + "test/hotupdate/acceptance/contractstub"
	fullstackStubPath       = "test/hotupdate/acceptance/contractstub/stub.go"
	fullstackStubText       = "not implemented (S10"
)

// Declarations only: comments and bodies may change, a field, tag, type or
// signature may not without a P00 contract update and a new digest here.
var fullstackFrozenDeclarations = map[string]string{
	"test/hotupdate/acceptance/contract/types.go": "70b79ead56a86029156064364fc44ab26c709af540600ad7b96f3f8c71cfb2f1",
	"test/hotupdate/acceptance/contract/ports.go": "3137877e6bafd9c3a63f25bda301b7672f580a47a0c730222c4f0d38981ec84a",
}

// Plan §5.1's closed vocabularies and the native verdict → rc pairs. The schema
// is the disk boundary every collector shares; a new member is a P00 decision.
var fullstackFrozenVocabulary = map[string][]string{
	"role":     {"agent", "broker", "nats", "caddy", "ctl", "fixture", "support"},
	"topology": {"single", "quorum"},
	"regime":   {"calibration", "quiet", "default", "live-grow", "control"},
	"tier":     {"hermetic", "deploy", "source_gate"},
	"verdict":  {"GREEN", "ASSERT-FAIL", "SETUP-RED", "PRODUCT-RED", "INCOMPLETE"},
}

var fullstackNativeRC = map[string]float64{"GREEN": 0, "ASSERT-FAIL": 1, "SETUP-RED": 2, "PRODUCT-RED": 3, "INCOMPLETE": 4}

// Each failure stub names the package whose real factory replaces it, and the
// directory where that factory must be declared with the identical shape.
var fullstackStubOwners = map[string]struct{ owner, home string }{
	"Plan":           {"P01", "test/hotupdate/acceptance"},
	"Judge":          {"P01", "test/hotupdate/acceptance"},
	"Quantiles":      {"P02", "test/hotupdate/acceptance"},
	"JudgeResources": {"P03", "test/hotupdate/acceptance"},
	"NewSnapshotter": {"P04", "test/fullstackharness"},
	"NewDriver":      {"P18", "test/fullstackharness"},
	"NewFixture":     {"P19", "test/fullstackharness"},
}

func checkFullstackContracts(tree fullstackTree) error {
	for _, path := range brokerSortedSourcePaths(fullstackFrozenDeclarations) {
		if tree.files[path] == "" {
			return fmt.Errorf("empty S10 contract root %s", path)
		}
		digest, err := infraDeclarationDigest(tree.files[path])
		if err != nil || digest != fullstackFrozenDeclarations[path] {
			return fmt.Errorf("%s: frozen S10 contract declaration drift (%s): %v", path, digest, err)
		}
	}
	if err := checkFullstackVocabulary(tree.docs[fullstackSchemaPath]); err != nil {
		return err
	}
	shapes, err := checkFullstackStubs(tree.files[fullstackStubPath])
	if err != nil {
		return err
	}
	if err := checkFullstackFactories(tree, shapes); err != nil {
		return err
	}
	return checkFullstackStubReach(tree)
}

func checkFullstackVocabulary(schema string) error {
	var root struct {
		Defs map[string]struct {
			Enum  []any `json:"enum"`
			AllOf []struct {
				OneOf []struct {
					Properties struct {
						Verdict struct{ Const string }   `json:"verdict"`
						RC      struct{ Const *float64 } `json:"rc"`
					} `json:"properties"`
				} `json:"oneOf"`
			} `json:"allOf"`
		} `json:"$defs"`
	}
	if err := json.Unmarshal([]byte(schema), &root); err != nil || len(root.Defs) == 0 {
		return fmt.Errorf("empty or unreadable S10 schema root: %v", err)
	}
	for name, want := range fullstackFrozenVocabulary {
		// Upstream S9 enums are integers; a frozen S10 vocabulary is strings only.
		var got []string
		for _, member := range root.Defs[name].Enum {
			text, ok := member.(string)
			if !ok {
				return fmt.Errorf("S10 vocabulary %s has non-string member %v", name, member)
			}
			got = append(got, text)
		}
		sort.Strings(got)
		frozen := slices.Clone(want)
		sort.Strings(frozen)
		if !slices.Equal(got, frozen) {
			return fmt.Errorf("S10 vocabulary %s drifted: %v, frozen %v", name, root.Defs[name].Enum, want)
		}
	}
	pairs := map[string]float64{}
	if allOf := root.Defs["result"].AllOf; len(allOf) == 1 {
		for _, branch := range allOf[0].OneOf {
			if rc := branch.Properties.RC.Const; rc != nil {
				pairs[branch.Properties.Verdict.Const] = *rc
			}
		}
	}
	if len(pairs) != len(fullstackNativeRC) {
		return fmt.Errorf("S10 native verdict/rc vocabulary drifted: %v", pairs)
	}
	for verdict, rc := range fullstackNativeRC {
		if got, ok := pairs[verdict]; !ok || got != rc {
			return fmt.Errorf("S10 native verdict %s must keep rc %v", verdict, rc)
		}
	}
	return nil
}

type fullstackShape struct{ params, results []string }

func fullstackFuncShape(fn *ast.FuncDecl, imports map[string]string) fullstackShape {
	var shape fullstackShape
	for _, list := range []struct {
		fields *ast.FieldList
		out    *[]string
	}{{fn.Type.Params, &shape.params}, {fn.Type.Results, &shape.results}} {
		if list.fields == nil {
			continue
		}
		for _, field := range list.fields.List {
			for n := max(1, len(field.Names)); n > 0; n-- {
				*list.out = append(*list.out, transferExpr(field.Type, imports))
			}
		}
	}
	return shape
}

// A stub body is one return whose error is failure("not implemented (S10 <owner>)").
// Every other result is nil or an empty composite literal; nothing else runs.
func checkFullstackStubs(source string) (map[string]fullstackShape, error) {
	if source == "" {
		return nil, fmt.Errorf("empty S10 stub root %s", fullstackStubPath)
	}
	file, err := brokerParse(fullstackStubPath, source)
	if err != nil {
		return nil, err
	}
	imports := brokerImports(file)
	for _, path := range imports {
		if len(imports) != 1 || path != fullstackContractImport {
			return nil, fmt.Errorf("S10 stub imports %v; only the contract is allowed", imports)
		}
	}
	shapes := map[string]fullstackShape{}
	unwrap := false
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Recv != nil {
			if receiverTypeName(fn.Recv.List[0].Type) != "failure" || (fn.Name.Name != "Error" && fn.Name.Name != "Unwrap") {
				return nil, fmt.Errorf("S10 stub declares extra method %s", fn.Name.Name)
			}
			if fn.Name.Name == "Unwrap" && len(fn.Body.List) == 1 {
				if ret, ok := fn.Body.List[0].(*ast.ReturnStmt); ok && len(ret.Results) == 1 {
					sel, ok := ret.Results[0].(*ast.SelectorExpr)
					unwrap = ok && transferExpr(sel, imports) == fullstackContractImport+".ErrContractStub"
				}
			}
			continue
		}
		want, known := fullstackStubOwners[fn.Name.Name]
		if !known {
			return nil, fmt.Errorf("S10 stub declares unknown function %s", fn.Name.Name)
		}
		if fn.Body == nil || len(fn.Body.List) != 1 {
			return nil, fmt.Errorf("S10 stub %s is not a single inert return", fn.Name.Name)
		}
		ret, ok := fn.Body.List[0].(*ast.ReturnStmt)
		if !ok || len(ret.Results) == 0 {
			return nil, fmt.Errorf("S10 stub %s is not a single inert return", fn.Name.Name)
		}
		for i, result := range ret.Results {
			if i == len(ret.Results)-1 {
				call, ok := result.(*ast.CallExpr)
				lit, isLit := (*ast.BasicLit)(nil), false
				if ok && len(call.Args) == 1 {
					lit, isLit = call.Args[0].(*ast.BasicLit)
				}
				if !ok || !isLit || transferExpr(call.Fun, imports) != "failure" || lit.Value != strconv.Quote("not implemented (S10 "+want.owner+")") {
					return nil, fmt.Errorf("S10 stub %s must fail with its owner %s", fn.Name.Name, want.owner)
				}
				continue
			}
			ident, isNil := result.(*ast.Ident)
			composite, isEmpty := result.(*ast.CompositeLit)
			if (!isNil || ident.Name != "nil") && (!isEmpty || len(composite.Elts) != 0) {
				return nil, fmt.Errorf("S10 stub %s returns a fabricated value", fn.Name.Name)
			}
		}
		shapes[fn.Name.Name] = fullstackFuncShape(fn, imports)
	}
	if !unwrap || len(shapes) != len(fullstackStubOwners) {
		return nil, fmt.Errorf("S10 stub set incomplete (%d/%d) or failure no longer unwraps to ErrContractStub", len(shapes), len(fullstackStubOwners))
	}
	return shapes, nil
}

// A Linux-selected real factory has the stub's exact shape, does some work, and
// never routes back to the stub. Non-Linux twins may refuse explicitly.
func checkFullstackFactories(tree fullstackTree, shapes map[string]fullstackShape) error {
	real := map[string]bool{}
	for _, path := range brokerSortedSourcePaths(tree.files) {
		if !isProdGoFile(path) || fullstackDir(path) != "test/hotupdate/acceptance" && fullstackDir(path) != "test/fullstackharness" {
			continue
		}
		file, err := brokerParse(path, tree.files[path])
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fullstackStubOwners[fn.Name.Name].home != fullstackDir(path) {
				continue
			}
			name := fn.Name.Name
			shape := fullstackFuncShape(fn, brokerImports(file))
			if !slices.Equal(shape.params, shapes[name].params) || !slices.Equal(shape.results, shapes[name].results) {
				return fmt.Errorf("%s: real factory %s shape %v differs from the frozen %v", path, name, shape, shapes[name])
			}
			if !fullstackSelectsLinux(path, file) {
				continue
			}
			calls, stub := 0, false
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch value := node.(type) {
				case *ast.CallExpr:
					calls++
				case *ast.BasicLit:
					stub = stub || strings.Contains(value.Value, fullstackStubText)
				case *ast.SelectorExpr:
					stub = stub || value.Sel.Name == "ErrContractStub"
				}
				return true
			})
			if calls == 0 || stub {
				return fmt.Errorf("%s: real factory %s is an empty success or a stub", path, name)
			}
			real[name] = true
		}
	}
	names := make([]string, 0, len(fullstackStubOwners))
	for name := range fullstackStubOwners {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !real[name] {
			want := fullstackStubOwners[name]
			return fmt.Errorf("missing Linux real factory %s in %s (pending %s)", name, want.home, want.owner)
		}
	}
	return nil
}

func fullstackBase(path string) string { return path[strings.LastIndex(path, "/")+1:] }
func fullstackDir(path string) string  { return path[:max(0, strings.LastIndex(path, "/"))] }

// fullstackSelects evaluates the file-name GOOS suffix and the //go:build line
// for one platform. _other.go carries no implicit constraint; only the line does.
func fullstackSelects(path string, file *ast.File, goos, goarch string) bool {
	base := strings.TrimSuffix(strings.TrimSuffix(fullstackBase(path), ".go"), "_test")
	for _, suffix := range []string{"linux", "darwin", "windows", "freebsd", "openbsd", "netbsd"} {
		if strings.HasSuffix(base, "_"+suffix) && suffix != goos {
			return false
		}
	}
	for _, group := range file.Comments {
		for _, comment := range group.List {
			if !constraint.IsGoBuild(comment.Text) {
				continue
			}
			expr, err := constraint.Parse(comment.Text)
			return err == nil && expr.Eval(func(tag string) bool {
				return tag == goos || tag == goarch || tag == "unix" && goos != "windows"
			})
		}
	}
	return true
}

func fullstackSelectsLinux(path string, file *ast.File) bool {
	return fullstackSelects(path, file, "linux", "amd64")
}

// Only P00's own failure test may import the stub. A fixture, driver, judge or
// product that imports it, or fabricates its owner text, signs nothing real.
// Source-gate tests are exempt from the text rule only: their mutation strings
// are data written into temporary trees, never a reachable failure.
func checkFullstackStubReach(tree fullstackTree) error {
	for _, path := range brokerSortedSourcePaths(tree.files) {
		source := tree.files[path]
		if !strings.HasSuffix(path, ".go") || path == fullstackStubPath {
			continue
		}
		p00Test := strings.HasPrefix(path, "test/hotupdate/acceptance/contract/") && strings.HasSuffix(path, "_test.go")
		gateData := fullstackDir(path) == "test/architecture" && strings.HasSuffix(path, "_test.go")
		if strings.Contains(source, "acceptance/contractstub") && !p00Test {
			file, err := parser.ParseFile(token.NewFileSet(), path, source, parser.ImportsOnly)
			if err != nil {
				return err
			}
			for _, spec := range file.Imports {
				if value, _ := strconv.Unquote(spec.Path.Value); value == fullstackStubImport {
					return fmt.Errorf("%s: imports the S10 failure stub instead of a real factory", path)
				}
			}
		}
		if strings.Contains(source, fullstackStubText) && !p00Test && !gateData {
			file, err := parser.ParseFile(token.NewFileSet(), path, source, 0)
			if err != nil {
				return err
			}
			var found error
			ast.Inspect(file, func(node ast.Node) bool {
				if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING && strings.Contains(lit.Value, fullstackStubText) {
					found = fmt.Errorf("%s: fabricates the S10 stub failure text", path)
				}
				return found == nil
			})
			if found != nil {
				return found
			}
		}
	}
	return nil
}

// origin: hot-update S10 plan §5.1 / §10.2 P00 / P28
func TestFullstackContractsStayFrozenAndInert(t *testing.T) {
	tree := fullstackRepoTree(t)
	for _, path := range brokerSortedSourcePaths(fullstackFrozenDeclarations) {
		digest, err := infraDeclarationDigest(tree.files[path])
		t.Logf("frozen S10 declaration %s %s (%v)", path, digest, err)
	}
	if err := checkFullstackContracts(tree); err != nil {
		t.Fatal(err)
	}
}

// Every mutation edits a copied real contract, stub, factory or product file on
// disk and re-reads it with the same reader as the gate.
// origin: hot-update S10 plan §10.2 P28 / §12.1
func TestFullstackContractGateControls(t *testing.T) {
	real := fullstackRepoTree(t)
	root := fullstackFixture(t, real, "test/hotupdate/acceptance", "test/fullstackharness")
	check := func() error { return checkFullstackContracts(readFullstackTree(t, root)) }
	if err := check(); err != nil {
		t.Fatal(err)
	}
	const types, ports = "test/hotupdate/acceptance/contract/types.go", "test/hotupdate/acceptance/contract/ports.go"
	// A comment is not a contract change; this positive control must stay green.
	transferWriteFixture(t, root, types, real.files[types]+"\n// comments may evolve without P00\n")
	if err := check(); err != nil {
		t.Fatalf("comment-only contract edit refused: %v", err)
	}
	transferWriteFixture(t, root, types, real.files[types])
	proveFullstackMutations(t, root, check,
		fullstackMutation{path: types, new: "\ntype UnfrozenEvidence struct{ Extra string }\n", want: "declaration drift"},
		fullstackMutation{path: types, old: "UID                                uint32", new: "UID                                uint64", want: "declaration drift"},
		fullstackMutation{path: ports, old: "Sample(context.Context, Key, string) (Sample, error)", new: "Sample(Key, string) (Sample, error)", want: "declaration drift"},
		fullstackMutation{path: fullstackSchemaPath, old: `"fixture","support"]`, new: `"fixture","support","operator"]`, want: "vocabulary role drifted"},
		fullstackMutation{path: fullstackSchemaPath, old: `"tier": {"enum":["hermetic","deploy","source_gate"]}`, new: `"tier": {"enum":["hermetic","deploy"]}`, want: "vocabulary tier drifted"},
		fullstackMutation{path: fullstackSchemaPath, old: `{"const":"GREEN"},"rc":{"const":0}`, new: `{"const":"GREEN"},"rc":{"const":75}`, want: "native verdict GREEN"},
		fullstackMutation{path: fullstackStubPath, old: `return nil, failure("not implemented (S10 P18)")`, new: `return nil, nil`, want: "must fail with its owner"},
		fullstackMutation{path: fullstackStubPath, old: `"not implemented (S10 P04)"`, new: `"not implemented (S10 P05)"`, want: "owner P04"},
		fullstackMutation{path: fullstackStubPath, new: "\nfunc Warm() { _ = contract.Profile{} }\n", want: "unknown function Warm"},
		fullstackMutation{path: fullstackStubPath, old: "func (failure) Unwrap() error   { return contract.ErrContractStub }", new: "func (failure) Unwrap() error   { return nil }", want: "unwraps to ErrContractStub"},
		fullstackMutation{path: "test/hotupdate/acceptance/plan.go", old: "func Plan(p c.Profile)", new: "func plan(p c.Profile)", want: "missing Linux real factory Plan"},
		fullstackMutation{path: "test/fullstackharness/driver.go", old: "func NewDriver(profile contract.Profile) (contract.Driver, error) {", new: "func NewDriver(profile contract.Profile, attempt int) (contract.Driver, error) {", want: "real factory NewDriver shape"},
		fullstackMutation{path: "test/fullstackharness/fixture_stub.go", new: "package fullstackharness\n\nimport stub \"" + fullstackStubImport + "\"\n\nvar wiredFixture = stub.NewFixture\n", want: "imports the S10 failure stub"},
		fullstackMutation{path: "internal/agent/acceptance_stub.go", new: "package agent\n\nimport _ \"" + fullstackStubImport + "\"\n", want: "imports the S10 failure stub"},
		fullstackMutation{path: "test/fullstackharness/fixture_text.go", new: "package fullstackharness\n\nvar pending = \"not implemented (S10 P19)\"\n", want: "fabricates the S10 stub failure text"},
	)
	// The owner text in prose is documentation, not a fabricated failure.
	transferWriteFixture(t, root, "test/fullstackharness/fixture_text.go", "package fullstackharness\n\n// A stub says not implemented (S10 P19); this harness never does.\n")
	if err := check(); err != nil {
		t.Fatalf("stub text in a comment refused: %v", err)
	}
}
