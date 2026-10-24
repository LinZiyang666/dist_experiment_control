package architecture

import (
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/importer"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

// Each scan owns its syntax and type information. In particular, a source
// mutation must never reuse the preceding scan's method-resolution results.
// Only external compiler exports are cached; repository declarations are read
// and checked afresh, for Linux and Darwin independently.
// origin: hot-update S7 plan §11.2 / T48–T49 / S7-GATES lead decisions 1–2
type transferGraph struct {
	fset    *token.FileSet
	files   map[string]*ast.File
	sources map[string]string
	edges   map[string][]string
	sdkPuts map[string]int
	types   map[ast.Expr]types.Type
	err     error
}

func newTransferGraph(sources map[string]string) *transferGraph {
	return &transferGraph{fset: token.NewFileSet(), files: map[string]*ast.File{}, sources: sources}
}

var transferExports sync.Map // GOOS -> compiler export paths; no repository syntax

func transferExternalImporter(fset *token.FileSet, platform string) (types.Importer, error) {
	value, ok := transferExports.Load(platform)
	if !ok {
		cmd := exec.Command("go", "list", "-deps", "-export", "-f", "{{.ImportPath}}\t{{.Export}}", "./internal/...", "./cmd/...")
		cmd.Dir = "../.."
		cmd.Env = append(os.Environ(), "GOOS="+platform, "GOARCH=arm64")
		raw, err := cmd.Output()
		if err != nil {
			return nil, fmt.Errorf("transfer type exports (%s): %w", platform, err)
		}
		paths := map[string]string{}
		for _, line := range strings.Split(string(raw), "\n") {
			path, export, found := strings.Cut(line, "\t")
			if found && export != "" {
				paths[path] = export
			}
		}
		value, _ = transferExports.LoadOrStore(platform, paths)
	}
	paths := value.(map[string]string)
	return importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
		if paths[path] == "" {
			return nil, fmt.Errorf("missing compiler export for %s", path)
		}
		return os.Open(paths[path])
	}), nil
}

type transferTypeImporter struct {
	graph    *transferGraph
	external types.Importer
	files    map[string][]*ast.File
	packages map[string]*types.Package
	info     *types.Info
	errors   []error
}

func (i *transferTypeImporter) Import(path string) (*types.Package, error) {
	if pkg := i.packages[path]; pkg != nil {
		return pkg, nil
	}
	files, local := i.files[path]
	if !local {
		return i.external.Import(path)
	}
	pkg := types.NewPackage(path, files[0].Name.Name)
	i.packages[path] = pkg
	// The source controls also exercise partial/invalid declarations. go/types
	// still supplies object identity for their valid portion; unresolved names
	// do not acquire an invented same-name edge to another receiver.
	config := types.Config{Importer: i, Error: func(err error) { i.errors = append(i.errors, err) }}
	checker := types.NewChecker(&config, i.graph.fset, pkg, i.info)
	_ = checker.Files(files)
	return pkg, nil
}

func transferPlatformFile(path, source, platform string) bool {
	base := strings.TrimSuffix(filepath.Base(path), ".go")
	parts := strings.Split(base, "_")
	for _, suffix := range parts[len(parts)-1:] {
		if slices.Contains([]string{"linux", "darwin", "windows", "freebsd", "openbsd", "netbsd", "dragonfly", "solaris", "illumos", "aix", "android", "ios", "plan9", "js", "wasip1"}, suffix) && suffix != platform {
			return false
		}
	}
	for _, line := range strings.Split(source, "\n") {
		if constraint.IsGoBuild(line) {
			expr, err := constraint.Parse(line)
			return err == nil && expr.Eval(func(tag string) bool { return tag == platform || tag == "unix" || tag == "arm64" || tag == "gc" })
		}
		if strings.HasPrefix(line, "package ") {
			break
		}
	}
	return true
}

// Object keys distinguish methods by their declared receiver, including
// promoted methods and aliases. Local variables and struct field names never
// resolve to a package-level declaration merely because their spelling agrees.
func transferObjectKey(obj types.Object) string {
	if obj == nil || obj.Pkg() == nil {
		return ""
	}
	name := obj.Name()
	if fn, ok := obj.(*types.Func); ok {
		if recv := fn.Type().(*types.Signature).Recv(); recv != nil {
			typ := recv.Type()
			if ptr, ok := typ.(*types.Pointer); ok {
				typ = ptr.Elem()
			}
			if named, ok := types.Unalias(typ).(*types.Named); ok {
				name = named.Obj().Name() + "." + name
			} else {
				return ""
			}
		}
	} else if obj.Parent() != obj.Pkg().Scope() {
		return ""
	}
	return obj.Pkg().Path() + ":" + name
}

func (g *transferGraph) build(all map[string]transferDeclaration) error {
	if g.edges != nil {
		return g.err
	}
	g.edges = map[string][]string{}
	g.sdkPuts = map[string]int{}
	g.types = map[ast.Expr]types.Type{}
	index := map[string][]string{}
	for site, decl := range all {
		key := modPrefix + filepath.ToSlash(filepath.Dir(decl.path)) + ":" + decl.name
		index[key] = append(index[key], site)
	}
	for _, platform := range []string{"linux", "darwin"} {
		if err := g.buildPlatform(all, index, platform); err != nil {
			g.err = err
			return err
		}
	}
	for site, edges := range g.edges {
		slices.Sort(edges)
		g.edges[site] = slices.Compact(edges)
	}
	return nil
}

func (g *transferGraph) buildPlatform(all map[string]transferDeclaration, index map[string][]string, platform string) error {
	external, err := transferExternalImporter(g.fset, platform)
	if err != nil {
		return err
	}
	info := &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}, Types: map[ast.Expr]types.TypeAndValue{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	i := &transferTypeImporter{graph: g, external: external, files: map[string][]*ast.File{}, packages: map[string]*types.Package{}, info: info}
	active := map[string]bool{}
	for _, path := range brokerSortedSourcePaths(g.sources) {
		file := g.files[path]
		if file != nil && transferPlatformFile(path, g.sources[path], platform) {
			pkg := modPrefix + filepath.ToSlash(filepath.Dir(path))
			i.files[pkg] = append(i.files[pkg], file)
			active[path] = true
		}
	}
	var packages []string
	for pkg := range i.files {
		packages = append(packages, pkg)
	}
	slices.Sort(packages)
	for _, pkg := range packages {
		if _, err := i.Import(pkg); err != nil {
			return err
		}
	}
	// Real repository scans cannot certify a partial graph. Synthetic parser
	// controls intentionally include incomplete declarations; the production
	// source set includes main and must type-check completely on both targets.
	if g.sources["cmd/tether/main.go"] != "" && len(i.errors) != 0 {
		return fmt.Errorf("transfer type graph (%s): %w", platform, i.errors[0])
	}
	for expr, value := range info.Types {
		g.types[expr] = value.Type
	}
	implementations := map[string][]types.Type{}
	indexed := map[types.Type]bool{}
	for _, obj := range info.Defs {
		if name, ok := obj.(*types.TypeName); ok {
			if named, ok := types.Unalias(name.Type()).(*types.Named); ok {
				if _, iface := named.Underlying().(*types.Interface); !iface {
					for _, typ := range []types.Type{named, types.NewPointer(named)} {
						if indexed[typ] {
							continue
						}
						indexed[typ] = true
						methods := types.NewMethodSet(typ)
						for n := 0; n < methods.Len(); n++ {
							name := methods.At(n).Obj().Name()
							implementations[name] = append(implementations[name], typ)
						}
					}
				}
			}
		}
	}
	// Callback parameters acquire edges from the actual values supplied at
	// call sites. This is why NewLegacyObject reaches PutLegacyOnce even though
	// the constructor's body only invokes its 'put' parameter.
	values := map[types.Object][]types.Object{}
	bind := func(target types.Object, expr ast.Expr) {
		if target == nil {
			return
		}
		if _, callable := target.Type().Underlying().(*types.Signature); !callable {
			return
		}
		if obj := transferExpressionObject(info, expr); obj != nil {
			values[target] = append(values[target], obj)
			return
		}
		// Literal bodies are already part of their caller declaration. Binding
		// every anonymous planner to a shared generic callback parameter would
		// join unrelated callers and recreate the old name-only graph's drift.
	}
	for _, file := range g.files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.ValueSpec:
				for n, name := range value.Names {
					if n < len(value.Values) {
						bind(info.Defs[name], value.Values[n])
					}
				}
			case *ast.AssignStmt:
				for n, lhs := range value.Lhs {
					if id, ok := lhs.(*ast.Ident); ok && n < len(value.Rhs) {
						obj := info.Defs[id]
						if obj == nil {
							obj = info.Uses[id]
						}
						bind(obj, value.Rhs[n])
					}
				}
			}
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			fn := transferExpressionObject(info, call.Fun)
			if fn == nil {
				return true
			}
			sig, ok := fn.Type().(*types.Signature)
			if !ok {
				return true
			}
			for n, arg := range call.Args {
				if n >= sig.Params().Len() {
					break
				}
				bind(sig.Params().At(n), arg)
			}
			return true
		})
	}
	for site, decl := range all {
		if !active[decl.path] {
			continue
		}
		seen := map[types.Object]bool{}
		var reference func(types.Object)
		reference = func(obj types.Object) {
			if obj == nil || seen[obj] {
				return
			}
			seen[obj] = true
			for _, target := range index[transferObjectKey(obj)] {
				if active[all[target].path] {
					g.edges[site] = append(g.edges[site], target)
				}
			}
			for _, value := range values[obj] {
				reference(value)
			}
		}
		ast.Inspect(decl.node, func(node ast.Node) bool {
			if ident, ok := node.(*ast.Ident); ok {
				reference(info.Uses[ident])
			}
			if sel, ok := node.(*ast.SelectorExpr); ok {
				if obj, ok := info.Uses[sel.Sel].(*types.Func); ok && obj.Pkg() != nil && obj.Name() == "Put" &&
					(obj.Pkg().Path() == "github.com/nats-io/nats.go/jetstream" || obj.Pkg().Path() == "github.com/nats-io/nats.go") {
					// Count source references once across the two platform checks.
					if platform == "linux" || !transferPlatformFile(decl.path, g.sources[decl.path], "linux") {
						g.sdkPuts[site]++
					}
				}
				if selection := info.Selections[sel]; selection != nil {
					if iface, ok := selection.Recv().Underlying().(*types.Interface); ok {
						for _, typ := range implementations[sel.Sel.Name] {
							if types.Implements(typ, iface) {
								obj, _, _ := types.LookupFieldOrMethod(typ, true, selection.Obj().Pkg(), sel.Sel.Name)
								reference(obj)
							}
						}
					}
				}
			}
			return true
		})
	}
	return nil
}

func transferSortedDeclarationSites(all map[string]transferDeclaration) []string {
	var sites []string
	for site := range all {
		sites = append(sites, site)
	}
	slices.Sort(sites)
	return sites
}

// The positive closure includes promoted methods, interface dispatch, method
// values and a callback passed as a value. Decoys share the method spelling and
// signature but have the wrong receiver. Every mutation is written to disk.
// origin: hot-update S7 plan §11.2 / T48–T49 / S7-GATES lead decisions 1–2
func exerciseTypedTransferReachability(t *testing.T) {
	path := "internal/codec/codec.go"
	source := `package codec
type Receiver struct{}
func (Receiver) Sign() string { return helper() }
func helper() string { return "real-contract" }
type Decoy struct{}
func (Decoy) Sign() string { return "unrelated" }
type Alias = Receiver
type Promoted struct{ Alias }
func Root(r Promoted) string { f := r.Sign; return f() }
func Adapter(put func() string) string { return put() }
func Caller() string { return Adapter(helper) }
type Port interface{ Sign() string }
type NotPort struct{}
func (NotPort) Sign() int { return 1 }
func ViaPort(p Port) string { return p.Sign() }
`
	root := transferFixtureTree(t, map[string]string{path: source})
	closure := func(symbol string) []string {
		all, err := transferDeclarations(brokerGateSources(t, root))
		if err != nil {
			t.Fatal(err)
		}
		nodes, err := reachableTransferDeclarations(all, path+":"+symbol)
		if err != nil {
			t.Fatal(err)
		}
		var sites []string
		for _, node := range nodes {
			sites = append(sites, node.name)
		}
		slices.Sort(sites)
		return sites
	}
	for _, symbol := range []string{"Root", "Adapter", "ViaPort"} {
		names := closure(symbol)
		if !slices.Contains(names, "helper") || slices.Contains(names, "NotPort.Sign") || symbol != "ViaPort" && slices.Contains(names, "Decoy.Sign") {
			t.Fatalf("typed %s closure: %v", symbol, names)
		}
	}
	baseline := closure("Root")
	transferWriteFixture(t, root, path, strings.Replace(source, `"unrelated"`, `"noise-changed"`, 1))
	if err := checkTransferInventory(closure("Root"), baseline); err != nil {
		t.Fatalf("unrelated receiver drifted: %v", err)
	}
	transferWriteFixture(t, root, path, strings.Replace(source, "return helper()", `return "contract-removed"`, 1))
	if checkTransferInventory(closure("Root"), baseline) == nil {
		t.Fatal("on-disk contract edge removal escaped")
	}
	transferWriteFixture(t, root, path, source)
	all, err := transferDeclarations(brokerGateSources(t, root))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := reachableTransferDeclarations(all, path+":Root")
	if err != nil {
		t.Fatal(err)
	}
	tokens := transferRootTokens(nodes)
	transferWriteFixture(t, root, path, strings.Replace(source, `"real-contract"`, `"changed-contract"`, 1))
	all, err = transferDeclarations(brokerGateSources(t, root))
	if err != nil {
		t.Fatal(err)
	}
	nodes, err = reachableTransferDeclarations(all, path+":Root")
	if err != nil {
		t.Fatal(err)
	}
	if checkTransferInventory(transferRootTokens(nodes), tokens) == nil {
		t.Fatal("on-disk contract token drift escaped")
	}
}

func transferExpressionObject(info *types.Info, expr ast.Expr) types.Object {
	switch value := expr.(type) {
	case *ast.Ident:
		return info.Uses[value]
	case *ast.SelectorExpr:
		return info.Uses[value.Sel]
	case *ast.ParenExpr:
		return transferExpressionObject(info, value.X)
	case *ast.IndexExpr:
		return transferExpressionObject(info, value.X)
	case *ast.IndexListExpr:
		return transferExpressionObject(info, value.X)
	}
	return nil
}
