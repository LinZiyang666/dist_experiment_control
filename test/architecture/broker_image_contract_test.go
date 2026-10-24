package architecture

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// These gates inspect every production platform file, including files excluded
// by the host build. Runtime correctness still belongs to the real cut tests;
// the source gate prevents the forbidden assembly shapes from returning.
// origin: hot-update S6 plan §3.2 / §9.2 / §12.1 P31 / T53
// gate-control: TestBrokerImageContractControls

func brokerGateSources(t *testing.T, root string) map[string]string {
	t.Helper()
	sources := map[string]string{}
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" || entry.Name() == "vendor" {
					return filepath.SkipDir
				}
				return nil
			}
			if !isProdGoFile(path) {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			sources[filepath.ToSlash(rel)] = string(raw)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(sources) == 0 {
		t.Fatal("no production sources found")
	}
	return sources
}

func brokerParse(path, source string) (*ast.File, error) {
	return parser.ParseFile(token.NewFileSet(), path, source, parser.ParseComments)
}

func brokerImports(file *ast.File) map[string]string {
	imports := map[string]string{}
	for _, spec := range file.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		name := filepath.Base(path)
		if spec.Name != nil {
			name = spec.Name.Name
		}
		imports[name] = path
	}
	return imports
}

func brokerCanonicalDigest(source string) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "contract.go", source, 0)
	if err != nil {
		return "", err
	}
	// The S7 Bundle reference is the one authorized additive S6-contract
	// change. Remove only its exact shape before hashing the preserved S6
	// declarations; every pre-existing field and function remains pinned.
	// origin: hot-update S7 plan §8.3 / T48
	if err := stripBrokerTransferReference(file); err != nil {
		return "", err
	}
	var out bytes.Buffer
	if err := printer.Fprint(&out, token.NewFileSet(), file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(out.Bytes())), nil
}

func stripBrokerTransferReference(file *ast.File) error {
	if file.Name.Name != "brokerimage" {
		return nil
	}
	imports := brokerImports(file)
	stripped := false
	for _, declaration := range file.Decls {
		group, ok := declaration.(*ast.GenDecl)
		if !ok || group.Tok != token.TYPE {
			continue
		}
		for _, spec := range group.Specs {
			typ := spec.(*ast.TypeSpec)
			structure, ok := typ.Type.(*ast.StructType)
			if typ.Name.Name != "Bundle" || !ok {
				continue
			}
			var keep []*ast.Field
			for _, field := range structure.Fields.List {
				if len(field.Names) != 1 || field.Names[0].Name != "Transfers" {
					keep = append(keep, field)
					continue
				}
				tag := ""
				if field.Tag != nil {
					tag, _ = strconv.Unquote(field.Tag.Value)
				}
				if stripped || tag != `json:"transfers,omitempty"` || transferExpr(field.Type, imports) != "*"+modPrefix+"internal/xfercontract.ImageRef" {
					return fmt.Errorf("Bundle.Transfers changed the frozen S7 public reference")
				}
				stripped = true
			}
			structure.Fields.List = keep
		}
	}
	if !stripped {
		return nil
	}
	// Keep the import whenever another declaration references it; an added
	// API or private state field must still change the S6 contract digest.
	used := false
	ast.Inspect(file, func(node ast.Node) bool {
		if selector, ok := node.(*ast.SelectorExpr); ok {
			if alias, ok := selector.X.(*ast.Ident); ok && imports[alias.Name] == modPrefix+"internal/xfercontract" {
				used = true
			}
		}
		return true
	})
	if !used {
		for _, declaration := range file.Decls {
			group, ok := declaration.(*ast.GenDecl)
			if !ok || group.Tok != token.IMPORT {
				continue
			}
			var keep []ast.Spec
			for _, spec := range group.Specs {
				imp := spec.(*ast.ImportSpec)
				path, _ := strconv.Unquote(imp.Path.Value)
				if path != modPrefix+"internal/xfercontract" {
					keep = append(keep, spec)
				}
			}
			group.Specs = keep
		}
	}
	return nil
}

// origin: hot-update S7 plan §8.3 / T48
// gate-control: TestTransferBrokerContractControls
func TestTransferBrokerContractControls(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "internal/brokerimage/contracts.go"))
	if err != nil {
		t.Fatal(err)
	}
	base, err := brokerCanonicalDigest(string(raw))
	if err != nil {
		t.Fatal(err)
	}
	file, err := brokerParse("contracts.go", string(raw))
	if err != nil {
		t.Fatal(err)
	}
	alias := ""
	for name, path := range brokerImports(file) {
		if path == modPrefix+"internal/xfercontract" {
			alias = name
		}
	}
	addition := string(raw)
	if alias == "" {
		alias = "x"
		addition = strings.Replace(addition, "import (", "import (\n x \""+modPrefix+"internal/xfercontract\"", 1)
		addition = strings.Replace(addition, "type Bundle struct {", "type Bundle struct {\nTransfers *x.ImageRef `json:\"transfers,omitempty\"`", 1)
	}
	if !strings.Contains(addition, "*"+alias+".ImageRef") {
		t.Fatal("actual Bundle additive control did not apply")
	}
	root := transferFixtureTree(t, map[string]string{"internal/brokerimage/contracts.go": addition})
	check := func(source string) (string, error) {
		transferWriteFixture(t, root, "internal/brokerimage/contracts.go", source)
		raw, err := os.ReadFile(filepath.Join(root, "internal/brokerimage/contracts.go"))
		if err != nil {
			return "", err
		}
		return brokerCanonicalDigest(string(raw))
	}
	if got, err := check(addition); err != nil || got != base {
		t.Fatalf("exact S7 additive reference altered preserved S6 contract: %s %s %v", got, base, err)
	}
	for _, source := range []string{
		strings.Replace(addition, "*"+alias+".ImageRef", "*"+alias+".WorkerState", 1),
		strings.ReplaceAll(addition, "transfers,omitempty", "transfers"),
		strings.Replace(addition, "Generation      uint64", "Generation      uint32", 1),
		addition + "\ntype Drift struct{ Detail " + alias + ".TrackerState }\n",
	} {
		if got, err := check(source); err == nil && got == base {
			t.Fatal("on-disk private-state/tag/pre-existing-field drift escaped")
		}
	}
}

// P00 declarations are frozen. Normalizing through the AST leaves comments and
// gofmt free to change while retaining fields, tags, aliases and method sets.
//
// Two digests moved with S7, each to the plan-owned additive shape and nothing else:
//   - brokerimage/contracts.go (S6 b09d3519…): P24 added type TransferOwner and
//     Config.Transfers (plan §8.2 / P24 file row). Removing exactly those two plus
//     their two imports (and the Bundle.Transfers reference stripped above)
//     reproduces the S6 digest, so every S6 declaration is byte-identical.
//   - cli/retry_contracts.go (S6 4d875d6e…): P26 appended the RetryTransfer member
//     to the existing RetryFamily (plan §7 / T32); no other declaration changed.
//
// origin: hot-update S7 plan §7 / §8.2 / T32 / P24 / P26
var brokerFrozenContracts = map[string]string{
	"internal/brokerimage/contracts.go":         "0411686433fd8c5c99307cc3c35dd8b9ee221058d8418100f9f8dcc9c81d6fb8",
	"internal/tunnel/server_image_contracts.go": "698152a1728e935216b5fd4de48ead9ab3a13dc6f36b8907af6838514026940f",
	"internal/listenerimage/contracts.go":       "a51b3186a0d97440f2d39edc9a92450da201f8f7ccdbd85c52ed825555ffac7a",
	"internal/storage/image_contracts.go":       "8ad27e743b6c73416e09170faf9990b6091576a3dd164944f9ac7c0dc4464e17",
	"internal/brokermaint/contracts.go":         "46105ca22d38326bc4256352329da15378a1c68ef5d509a871e494b6340890cf",
	"internal/controlbudget/contracts.go":       "ceca8c0c890d9ca27f3e4a5bbc912c4ff8d58f9afdee2aa88ba30f6bcf898274",
	"internal/cli/retry_contracts.go":           "cd8ab38cb67c64d2478320bd6206b5edcad2a9298ffeb5218892b22c9d5942ed",
	// S8 adds the authority incarnation fence and its matching guard to the
	// preserved S6 allocation shape. Pin the complete current declarations.
	// origin: hot-update S8 plan §6.2 / P35
	"internal/port/image_contracts.go":              "97f6836559e44921ab1138a832452622df23a029d228822bd5d732391b8b51d7",
	"internal/clusterio/contracts.go":               "6e7b724f2c8982551aaea9bf328325d1f1b31202ef8418b16664a74cb9b44c16",
	"internal/execclient/admission_contracts.go":    "8186f15806e89b1a4cbcfafbae50debcfdc8a1da8f81b6d9ec41d3906bc29caf",
	"internal/proto/broker_image.go":                "0125d183fed50ba0ba8956d3763d54af71a3058817a12ce43189e37c286b998a",
	"internal/clusterupgrade/broker_contracts.go":   "76c400b730cbf0be2d26e8ca1be6b7e8faf23c70cab235deb77b54721a5a1e45",
	"internal/httplisten/image_contracts.go":        "e5b159b41c7267397d907c10fbc07614eb380e6cb678b1467b818b731d46cbaf",
	"internal/adminsock/image_contracts.go":         "f878ae1494fe64e2ec209d8a7edcceddbe86ca8ddf704251e665749d2a8e0beb",
	"test/clusterharness/broker_image_contracts.go": "d91543546059621d770c17bb389c66533a7a244e005e3bf17f0a8ce69e6f39c8",
}

// origin: hot-update S6 plan §2.1 / T53
func checkBrokerProductionStubs(sources map[string]string) error {
	for path, source := range sources {
		if strings.HasSuffix(path, "_test.go") || strings.HasPrefix(path, "internal/brokerimage/contractstub/") {
			continue
		}
		file, err := brokerParse(path, source)
		if err != nil {
			return err
		}
		for _, imp := range brokerImports(file) {
			if imp == modPrefix+"internal/brokerimage/contractstub" || strings.HasPrefix(imp, modPrefix+"internal/brokerimage/contractstub/") {
				return fmt.Errorf("%s: production imports contractstub", path)
			}
		}
		var bad bool
		ast.Inspect(file, func(node ast.Node) bool {
			lit, ok := node.(*ast.BasicLit)
			if ok && lit.Kind == token.STRING {
				value, _ := strconv.Unquote(lit.Value)
				bad = bad || strings.Contains(value, "not implemented (S6 ")
			}
			return true
		})
		if bad {
			return fmt.Errorf("%s: production contains S6 stub error", path)
		}
	}
	return nil
}

// Exact bytes pin the SQL migration set and dependency directives. state/command.go's
// bytes pin both declared and accepted ops and the envelope version. None of
// these files is an S6 implementation seam, so a new persistent format is red.
// CLUSTER-SLIM moved the vocabulary to state: OpType declarations and the
// accepted-op map retain the frozen bytes; only local helper names were exported.
// origin: hot-update S6 plan §7.1 / §9.3 / T53
var brokerFrozenPersistentFiles = map[string]string{
	"go.mod":                            "376b9571142b0e525eb6f06fc127bd33ed6df6a9fbd10337bf9f469a301ed40e",
	"go.sum":                            "92b0f394d3e258eddb9e84cd4e80846fcd71ede2b5a29a04dabca5f098350daa",
	"internal/cluster/state/command.go": "b04d38fff11b48a3649ed71226745ecd4a6d8b1df5c0ce59a923b5ba129c456f",
	"internal/storage/migrations/0001_init.sql":                           "6a70e0f9e8652ef740d322f4ab7c3cca47382bd8cd3fc98455168292ccace095",
	"internal/storage/migrations/0002_agent_provisioning.sql":             "ba3cb7263e869f827550bbee2f2ab4347eb108bd070ad89a793039a60f487f47",
	"internal/storage/migrations/0003_port_alloc_history.sql":             "c37bd58ad2b172403ceb051c366c9dceba68f6d306b9f3846b48a70d35068941",
	"internal/storage/migrations/0004_token_hash_index.sql":               "67e4392d1d0a5c505d607a26204a32b0c87161897221e6dc9834d5aebcb7395e",
	"internal/storage/migrations/0005_processes_gc_indexes.sql":           "5015157d1dd5bc0703c53b690461fddfca23da564af36bf4ee26aaae549105ee",
	"internal/storage/migrations/0006_proxy.sql":                          "e3ba63807de24921aceaba9f08658f9c254c4879ada7405876b556a36144fce2",
	"internal/storage/migrations/0007_proxy_generation.sql":               "d129a6bd2dfbed06203e9b80afdfee7315519b57ca63ad6045b0e992b81ce716",
	"internal/storage/migrations/0008_cluster_nodes.sql":                  "ad491a10810169f94e0e90c6fbecdcebb42d495f05b3bea3c09fa6658c6963da",
	"internal/storage/migrations/0009_cluster_meta_alerts.sql":            "51dda645b54060faf21a91b7600760426450ff8a46e502b6368ff2d971c4d184",
	"internal/storage/migrations/0010_port_cluster_columns.sql":           "732d22a60e478e22aa771b7984873e3c57bfda040de991e42305cd83ed4e2b8d",
	"internal/storage/migrations/0011_cluster_reqid_ledger.sql":           "de534c7cf55ab5f34f137efa5b2c6c6d1d66823b05f9c9ec9f1d988051ecff6f",
	"internal/storage/migrations/0012_nodes_nats_server.sql":              "f5bd99c929a5a864c8303cd44b9d776c577636299e4b2d42994db6a0da815117",
	"internal/storage/migrations/0013_cluster_nodes_join_pop.sql":         "d04372a68ba05d7f35c0154354573f374f535571a13edc321ae43559bb948b3f",
	"internal/storage/migrations/0014_cluster_nodes_bus_nkey.sql":         "c040d225a045906588de206a08334418e1f952ab8792fc8b8716c14a24e8044a",
	"internal/storage/migrations/0015_cluster_operations.sql":             "d9dc95452a9f0b901f8f191a55e94cbb0546a828ddc9ccc8460eaff4fd7cf842",
	"internal/storage/migrations/0016_proxy_cluster.sql":                  "83ac7c5473427ea5dd238d6f94a15be07149fb57c0c2f799883f8c0468496c05",
	"internal/storage/migrations/0017_port_alloc_last_rehome.sql":         "09a9fa2ef39a0dcf181ea31b35536444c1903cc158d52e8692c266ea7470c4d7",
	"internal/storage/migrations/0018_alerts_kind_proxy_bind_stalled.sql": "08f5979c82a234061bb3083f1d8656f9808d72fd14ad28da5eb75cc3bfdd6f95",
	"internal/storage/migrations/0019_session_creators.sql":               "2c37dfc3027532ff03ac842196a361de4184cac1867d5e405bef580368c04bf9",
}

// origin: hot-update S6 plan §9.2 / T53
func checkBrokerPersistentFiles(files map[string]string) error {
	for path, want := range brokerFrozenPersistentFiles {
		got, ok := files[path]
		if !ok || fmt.Sprintf("%x", sha256.Sum256([]byte(got))) != want {
			return fmt.Errorf("NO_NEW_OPTYPE/MIGRATION: changed or missing %s", path)
		}
	}
	for path := range files {
		if strings.HasPrefix(path, "internal/storage/migrations/") {
			if _, ok := brokerFrozenPersistentFiles[path]; !ok {
				return fmt.Errorf("NO_NEW_OPTYPE/MIGRATION: added %s", path)
			}
		}
	}
	return nil
}

// origin: hot-update S6 plan §2 / §9.2 / §13 / T53
func TestBrokerImageContracts(t *testing.T) {
	root := repoRoot(t)
	sources := brokerGateSources(t, root)
	if err := checkBrokerProductionStubs(sources); err != nil {
		t.Error(err)
	}
	for path, want := range brokerFrozenContracts {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		got, err := brokerCanonicalDigest(string(raw))
		if err != nil {
			t.Fatal(err)
		}
		if got != want {
			t.Errorf("frozen contract %s: got %s want %s", path, got, want)
		}
	}
	files := map[string]string{}
	for path := range brokerFrozenPersistentFiles {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		files[path] = string(raw)
	}
	entries, err := os.ReadDir(filepath.Join(root, "internal/storage/migrations"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatal("unexpected nested migration directory")
		}
		path := "internal/storage/migrations/" + entry.Name()
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		files[path] = string(raw)
	}
	if err := checkBrokerPersistentFiles(files); err != nil {
		t.Error(err)
	}
}

// Follow reachable helpers in the AllOS internal graph. A named alias cannot
// hide cold Open/New/Bootstrap or a SQL/Bolt operation behind a pure smoke API.
// The check is deliberately conservative: file-scoped function values and
// dynamic methods are not a proof of purity and require a runtime receipt.
// origin: hot-update S6 plan §3.1 / T53
func checkBrokerPurePreflight(sources map[string]string) error {
	type function struct {
		file *ast.File
		decl *ast.FuncDecl
		path string
	}
	functions := map[string][]function{}
	for path, source := range sources {
		if !strings.HasPrefix(path, "internal/") || strings.Contains(path, "/contractstub/") {
			continue
		}
		file, err := brokerParse(path, source)
		if err != nil {
			return err
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv == nil && fn.Body != nil {
				key := filepath.ToSlash(filepath.Dir(path)) + "." + fn.Name.Name
				functions[key] = append(functions[key], function{file, fn, path})
			}
		}
	}
	root := "internal/brokerimage.Preflight"
	if len(functions[root]) == 0 {
		return fmt.Errorf("NOT_RUN pending P02: missing real Preflight")
	}
	seen := map[string]bool{}
	var visit func(string) error
	visit = func(key string) error {
		if seen[key] {
			return nil
		}
		seen[key] = true
		for _, fn := range functions[key] {
			imports := brokerImports(fn.file)
			for _, imp := range imports {
				if strings.Contains(imp, "sqlite") || strings.Contains(imp, "bbolt") || strings.Contains(imp, "nats-server") {
					return fmt.Errorf("%s: preflight imports store/server implementation %s", fn.path, imp)
				}
			}
			var failure error
			ast.Inspect(fn.decl.Body, func(node ast.Node) bool {
				if failure != nil {
					return false
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				target := ""
				switch callee := call.Fun.(type) {
				case *ast.Ident:
					target = filepath.ToSlash(filepath.Dir(fn.path)) + "." + callee.Name
				case *ast.SelectorExpr:
					name, ok := callee.X.(*ast.Ident)
					if !ok {
						return true
					}
					imp := imports[name.Name]
					if imp == "database/sql" || strings.Contains(imp, "bbolt") || strings.Contains(imp, "sqlite") || imp == modPrefix+"internal/cluster" || imp == modPrefix+"internal/broker" || strings.HasPrefix(imp, "github.com/nats-io/") {
						failure = fmt.Errorf("%s: preflight reaches store/transport call %s.%s", fn.path, imp, callee.Sel.Name)
						return false
					}
					if strings.HasPrefix(imp, modPrefix+"internal/") {
						target = strings.TrimPrefix(imp, modPrefix) + "." + callee.Sel.Name
					}
				}
				if strings.HasPrefix(target, "internal/storage.") && (strings.Contains(target, "Open") || strings.Contains(target, "Migration") && !strings.Contains(target, "Digest") && !strings.Contains(target, "Supported")) {
					failure = fmt.Errorf("%s: preflight reaches cold store call %s", fn.path, target)
					return false
				}
				if len(functions[target]) > 0 {
					failure = visit(target)
				}
				return failure == nil
			})
			if failure != nil {
				return failure
			}
		}
		return nil
	}
	return visit(root)
}

// A structural tripwire, not a substitute for T02/T49: every function that
// assembles the live broker cut must close, export the actual seal, then prepare
// and exec in that order. Calls in deferred closures do not count as closure.
// origin: hot-update S6 plan §3.2 / T53
// The structural cap moved the cut sequence to a generic leaf. Follow only the
// real synchronous Run -> runBrokerImageReload -> ReloadCurrentStore binding,
// and require the callback to execute Prepared.Exec with the inherited argv.
// A dormant leaf or a goroutine/defer containing it cannot sign the old Run.
// origin: hot-update S6 plan §3.2 / §9.3 / T53
func brokerDelegatedCutBound(sources map[string]string) (bool, error) {
	runBound, leafBound, execBound := false, false, false
	// Follow synchronous package helpers from Run. A dormant, deferred or
	// goroutine-only wrapper cannot bind the current cut to the running broker.
	// origin: hot-update S6 plan §3.2 / T53
	functions := map[string]*ast.FuncDecl{}
	for path, source := range sources {
		if !strings.HasPrefix(path, "internal/broker/") {
			continue
		}
		file, err := brokerParse(path, source)
		if err != nil {
			return false, err
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				functions[fn.Name.Name] = fn
			}
		}
	}
	reachable := map[string]bool{}
	var visit func(string)
	visit = func(name string) {
		if reachable[name] || functions[name] == nil {
			return
		}
		reachable[name] = true
		ast.Inspect(functions[name].Body, func(node ast.Node) bool {
			switch node.(type) {
			case *ast.GoStmt, *ast.DeferStmt, *ast.FuncLit:
				return false
			}
			if call, ok := node.(*ast.CallExpr); ok {
				if target, ok := call.Fun.(*ast.Ident); ok {
					visit(target.Name)
				}
			}
			return true
		})
	}
	visit("Run")
	runBound = reachable["runBrokerImageReload"]
	for path, source := range sources {
		if !strings.HasPrefix(path, "internal/broker/") {
			continue
		}
		file, err := brokerParse(path, source)
		if err != nil {
			return false, err
		}
		imports := brokerImports(file)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if fn.Name.Name == "Run" {
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					switch node.(type) {
					case *ast.GoStmt, *ast.DeferStmt, *ast.FuncLit:
						return false
					}
					call, ok := node.(*ast.CallExpr)
					if ok {
						id, ok := call.Fun.(*ast.Ident)
						runBound = runBound || ok && id.Name == "runBrokerImageReload"
					}
					return true
				})
			}
			if fn.Name.Name != "runBrokerImageReloadUsing" || !reachable[fn.Name.Name] {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				switch node.(type) {
				case *ast.GoStmt, *ast.DeferStmt:
					return false
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				id, ok := sel.X.(*ast.Ident)
				if ok && imports[id.Name] == modPrefix+"internal/brokerimage" && sel.Sel.Name == "ReloadCurrentStore" {
					leafBound = true
					if len(call.Args) != 4 {
						return false
					}
					callbacks, ok := call.Args[3].(*ast.CompositeLit)
					if !ok {
						return false
					}
					for _, element := range callbacks.Elts {
						pair, ok := element.(*ast.KeyValueExpr)
						if !ok {
							continue
						}
						key, ok := pair.Key.(*ast.Ident)
						if !ok || key.Name != "Exec" {
							continue
						}
						callback, ok := pair.Value.(*ast.FuncLit)
						if !ok {
							continue
						}
						// ExecCommit validates the decorated cut; its fallback must still
						// execute the same prepared owner with sealed argv. Both returns
						// are synchronous; a nested/deferred call is no evidence.
						ast.Inspect(callback.Body, func(node ast.Node) bool {
							switch node.(type) {
							case *ast.GoStmt, *ast.DeferStmt, *ast.FuncLit:
								return false
							}
							ret, ok := node.(*ast.ReturnStmt)
							if !ok || len(ret.Results) != 1 {
								return true
							}
							execution, ok := ret.Results[0].(*ast.CallExpr)
							if !ok || len(execution.Args) != 3 {
								return true
							}
							target, ok := execution.Fun.(*ast.SelectorExpr)
							if !ok || target.Sel.Name != "Exec" {
								return true
							}
							owner, ok := target.X.(*ast.Ident)
							if !ok || owner.Name != "prepared" {
								return true
							}
							argv, ok := execution.Args[1].(*ast.SelectorExpr)
							execBound = execBound || ok && argv.Sel.Name == "Args"
							return true
						})
					}
				}
				return true
			})
		}
	}
	if leafBound && (!runBound || !execBound) {
		return false, fmt.Errorf("delegated broker cut is not synchronously bound to Run/Prepared.Exec")
	}
	return leafBound, nil
}

func checkBrokerSealOrder(sources map[string]string) error {
	delegated, err := brokerDelegatedCutBound(sources)
	if err != nil {
		return err
	}
	found := 0
	for path, source := range sources {
		leaf := delegated && path == "internal/brokerimage/store_reload.go"
		if !strings.HasPrefix(path, "internal/broker/") && !leaf {
			continue
		}
		file, err := brokerParse(path, source)
		if err != nil {
			return err
		}
		imports := brokerImports(file)
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			if leaf && fn.Name.Name != "ReloadCurrentStore" {
				continue
			}
			// The synchronous exec latch owns its callback. Other function literals,
			// including deferred cleanup and dead helpers, cannot certify the cut.
			// origin: hot-update S6 plan §3.2 / T53
			latchBodies := map[*ast.FuncLit]bool{}
			if leaf {
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					sel, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || sel.Sel.Name != "WithExecLatch" || len(call.Args) != 1 {
						return true
					}
					body, ok := call.Args[0].(*ast.FuncLit)
					if ok {
						latchBodies[body] = true
					}
					return true
				})
			}
			inspectCut := func(node ast.Node) bool {
				if _, ok := node.(*ast.DeferStmt); ok {
					return false
				}
				if body, ok := node.(*ast.FuncLit); ok {
					return latchBodies[body]
				}
				return true
			}
			var calls []string
			receipts := map[*ast.CallExpr][]ast.Expr{}
			var guards []*ast.IfStmt
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if !inspectCut(node) {
					return false
				}
				if assign, ok := node.(*ast.AssignStmt); ok && len(assign.Rhs) == 1 {
					if call, ok := assign.Rhs[0].(*ast.CallExpr); ok {
						receipts[call] = assign.Lhs
					}
				}
				if guard, ok := node.(*ast.IfStmt); ok {
					guards = append(guards, guard)
				}
				return true
			})
			var closeCall, exportCall, prepareCall, execCall *ast.CallExpr
			var exports, prepares []*ast.CallExpr
			hasFreeze := false
			// Shutdown's reversible Prepare is outside image assembly. Resolve
			// the actual Freeze result so only that cut's Export/Prepare count;
			// an early cut.Prepare must still fail the closed-store ordering.
			// origin: S7 internal review GATE-CONVERGE-layering F1
			cutOwner := ""
			if leaf {
				for call, results := range receipts {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Freeze" && len(results) > 0 {
						if owner, ok := sel.X.(*ast.Ident); ok && owner.Name == "r" {
							if result, ok := results[0].(*ast.Ident); ok {
								cutOwner = result.Name
							}
						}
					}
				}
				if cutOwner == "" || cutOwner == "_" {
					return fmt.Errorf("%s: missing live Freeze result", path)
				}
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if !inspectCut(node) {
					return false
				}
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if id, ok := sel.X.(*ast.Ident); ok && sel.Sel.Name == "Freeze" && (imports[id.Name] == modPrefix+"internal/brokerimage" || leaf && id.Name == "r") {
					hasFreeze = true
				}
				if leaf && (sel.Sel.Name == "Export" || sel.Sel.Name == "Prepare") {
					if owner, ok := sel.X.(*ast.Ident); !ok || owner.Name != cutOwner {
						return true
					}
				}
				switch sel.Sel.Name {
				case "CloseForImage":
					closeCall = call
					calls = append(calls, sel.Sel.Name)
				case "Export":
					exportCall = call
					exports = append(exports, call)
					calls = append(calls, sel.Sel.Name)
				case "Prepare":
					prepareCall = call
					prepares = append(prepares, call)
					calls = append(calls, sel.Sel.Name)
				case "Exec":
					execCall = call
					calls = append(calls, sel.Sel.Name)
				}
				return true
			})
			if !hasFreeze {
				continue
			}
			found++
			want := []string{"CloseForImage", "Export", "Prepare", "Exec"}
			if len(calls) < len(want) || !slices.Equal(calls[:len(want)], want) {
				return fmt.Errorf("%s:%s: incomplete or premature live cut assembly: %v", path, fn.Name.Name, calls)
			}
			exportCall, prepareCall = exports[0], prepares[0]
			var retryLoop *ast.ForStmt
			if len(calls) > len(want) {
				// A late ACKed revoke may invalidate Prepared after the store is
				// closed. Plan §6.2 requires regeneration in the same exec loop;
				// arbitrary additional exports must still fail closed.
				// origin: hot-update S6 plan §6.2 / T25 / T53
				if !leaf || !slices.Equal(calls[4:], []string{"Export", "Prepare"}) {
					return fmt.Errorf("%s: unexpected repeated cut: %v", path, calls)
				}
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					loop, ok := node.(*ast.ForStmt)
					if !ok || loop.Pos() > execCall.Pos() || loop.End() < prepares[1].End() {
						return true
					}
					refusal, closed := false, false
					for _, stmt := range loop.Body.List {
						guard, ok := stmt.(*ast.IfStmt)
						if !ok || guard.End() > exports[1].Pos() {
							continue
						}
						neg, ok := guard.Cond.(*ast.UnaryExpr)
						if ok && neg.Op == token.NOT {
							call, ok := neg.X.(*ast.CallExpr)
							if ok && len(call.Args) == 2 {
								target, tok := call.Fun.(*ast.SelectorExpr)
								owner, ook := func() (*ast.Ident, bool) {
									if !tok {
										return nil, false
									}
									id, ok := target.X.(*ast.Ident)
									return id, ok
								}()
								reason, rok := call.Args[1].(*ast.Ident)
								if tok && ook && imports[owner.Name] == "errors" && target.Sel.Name == "Is" && rok && reason.Name == "ErrCutChanged" && len(guard.Body.List) == 1 {
									branch, ok := guard.Body.List[0].(*ast.BranchStmt)
									refusal = ok && branch.Tok == token.BREAK
								}
							}
						}
						if guard.Init != nil {
							assign, ok := guard.Init.(*ast.AssignStmt)
							if ok && len(assign.Rhs) == 1 {
								call, ok := assign.Rhs[0].(*ast.CallExpr)
								if ok {
									target, ok := call.Fun.(*ast.SelectorExpr)
									if ok {
										id, ok := target.X.(*ast.Ident)
										if ok && id.Name == "prepared" && target.Sel.Name == "Close" && len(guard.Body.List) == 1 && len(assign.Lhs) == 1 {
											name, named := assign.Lhs[0].(*ast.Ident)
											condition, guarded := guard.Cond.(*ast.BinaryExpr)
											if named && guarded && condition.Op == token.NEQ {
												left, lok := condition.X.(*ast.Ident)
												right, rok := condition.Y.(*ast.Ident)
												if lok && rok && left.Name == name.Name && right.Name == "nil" {
													_, closed = guard.Body.List[0].(*ast.ReturnStmt)
												}
											}
										}
									}
								}
							}
						}
					}
					if refusal && closed {
						retryLoop = loop
					}
					return true
				})
				if retryLoop == nil {
					return fmt.Errorf("%s: regeneration lacks cut-changed refusal or prepared close", path)
				}
			}

			// Export must consume the actual close receipt. A pre-seeded seal
			// with all phases marked closed is not evidence of a closed writer.
			// origin: hot-update S6 plan §3.2 / T53.
			if len(receipts[closeCall]) < 2 || len(exportCall.Args) != 1 {
				return fmt.Errorf("%s: missing actual close receipt", path)
			}
			seal, ok := receipts[closeCall][0].(*ast.Ident)
			for _, call := range exports {
				if len(call.Args) != 1 {
					return fmt.Errorf("%s: export lacks actual seal", path)
				}
				arg, argOK := call.Args[0].(*ast.Ident)
				if !ok || !argOK || seal.Name == "_" || seal.Name != arg.Name {
					return fmt.Errorf("%s: export uses preliminary seal", path)
				}
			}
			pairs := [][2]*ast.CallExpr{{closeCall, exportCall}, {exportCall, prepareCall}, {prepareCall, execCall}}
			if retryLoop != nil {
				pairs = append(pairs, [2]*ast.CallExpr{exports[1], prepares[1]}, [2]*ast.CallExpr{prepares[1], nil})
			}
			for _, pair := range pairs {
				next := fn.End()
				if pair[1] != nil {
					next = pair[1].Pos()
				} else if retryLoop != nil {
					next = retryLoop.End()
				}
				results := receipts[pair[0]]
				if len(results) < 2 {
					return fmt.Errorf("%s: cut error result ignored", path)
				}
				errName, ok := results[len(results)-1].(*ast.Ident)
				checked := false
				if ok && errName.Name != "_" {
					for _, guard := range guards {
						if guard.Pos() <= pair[0].End() || guard.End() >= next {
							continue
						}
						condition, ok := guard.Cond.(*ast.BinaryExpr)
						if !ok || condition.Op != token.NEQ {
							continue
						}
						left, leftOK := condition.X.(*ast.Ident)
						right, rightOK := condition.Y.(*ast.Ident)
						if !leftOK || !rightOK || left.Name != errName.Name || right.Name != "nil" {
							continue
						}
						// Require an actual exit. Logging an error and falling
						// through still permits preparing an unclosed store.
						for _, statement := range guard.Body.List {
							switch statement.(type) {
							case *ast.ReturnStmt, *ast.BranchStmt:
								checked = true
							}
						}
					}
				}
				if !checked {
					return fmt.Errorf("%s: cut error unchecked before next side effect", path)
				}
			}
		}
	}
	if found == 0 {
		return fmt.Errorf("NOT_RUN pending P11: missing real broker cut assembly")
	}
	return nil
}

// Keep cut ordering independently runnable: an unrelated missing home planner
// must not make a broken seal-order mutation indistinguishable from baseline.
// origin: hot-update S6 plan §3.2 / T53
// gate-control: TestBrokerImageContractControls
func TestBrokerCutUsesActualClosedStoreSeal(t *testing.T) {
	if err := checkBrokerSealOrder(brokerGateSources(t, repoRoot(t))); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §3.1–§3.2 / T53
func TestBrokerImageRuntimeContractShapes(t *testing.T) {
	sources := brokerGateSources(t, repoRoot(t))
	for _, check := range []func(map[string]string) error{checkBrokerPurePreflight, checkBrokerSealOrder, checkBrokerGuardedHomeMutators} {
		if err := check(sources); err != nil {
			t.Error(err)
		}
	}
}

// Guarded planners re-read full allocation identity and maintenance in the
// writer critical section. Production may not call the old cold/test planners.
// origin: hot-update S6 plan §2.1 / §7.2 / T53
func checkBrokerGuardedHomeMutators(sources map[string]string) error {
	for path, source := range sources {
		if strings.HasSuffix(path, "_test.go") || strings.HasPrefix(path, "internal/port/") {
			continue
		}
		file, err := brokerParse(path, source)
		if err != nil {
			return err
		}
		portAlias := ""
		for alias, imp := range brokerImports(file) {
			if imp == modPrefix+"internal/port" {
				portAlias = alias
			}
		}
		permissionFrees := brokerPermissionFreeCalls(path, file, portAlias)
		var failure error
		ast.Inspect(file, func(node ast.Node) bool {
			sel, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			id, ok := sel.X.(*ast.Ident)
			if !ok || id.Name != portAlias {
				return true
			}
			if sel.Sel.Name == "PlanFreeAllocation" && permissionFrees[sel] {
				return true
			}
			switch sel.Sel.Name {
			case "PlanReassignHome", "PlanAllocate", "PlanRotateAllocation", "PlanFreeAllocation":
				failure = fmt.Errorf("%s: production bypasses guarded home planner %s", path, sel.Sel.Name)
			}
			return true
		})
		if failure != nil {
			return failure
		}
	}
	return nil
}

// Permission removal is independent of maintenance. Only the existing local
// expose-rm owner and its matching forwarded verb may use the legacy payload;
// cleanup and rotation still require the guarded full home identity.
// origin: hot-update S6 plan §7.2 / internal/port/plan.go PlanFreeAllocationGuarded
func brokerPermissionFreeCalls(path string, file *ast.File, portAlias string) map[*ast.SelectorExpr]bool {
	allowed := map[*ast.SelectorExpr]bool{}
	mark := func(body *ast.BlockStmt, allocation string) {
		if body == nil {
			return
		}
		ast.Inspect(body, func(node ast.Node) bool {
			ret, ok := node.(*ast.ReturnStmt)
			if !ok || len(ret.Results) != 1 {
				return true
			}
			call, ok := ret.Results[0].(*ast.CallExpr)
			if !ok || len(call.Args) != 3 {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "PlanFreeAllocation" {
				return true
			}
			pkg, ok := sel.X.(*ast.Ident)
			if !ok || pkg.Name != portAlias {
				return true
			}
			if id, ok := call.Args[1].(*ast.Ident); ok && allocation == "narrowed" && id.Name == allocation {
				allowed[sel] = true
			}
			if value, ok := call.Args[1].(*ast.CallExpr); ok && allocation == "p" && len(value.Args) == 0 {
				if target, ok := value.Fun.(*ast.SelectorExpr); ok && target.Sel.Name == "allocation" {
					if id, ok := target.X.(*ast.Ident); ok && id.Name == allocation {
						allowed[sel] = true
					}
				}
			}
			return true
		})
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && path == "internal/broker/clusterwrite.go" && fn.Name.Name == "freePortAllocation" {
			identity, forwarded := false, false
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				if call, ok := node.(*ast.CallExpr); ok {
					if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "allocIdentity" {
						identity = true
					}
					// The authority writer serializes this exact identity once before
					// invoking either the local or forwarded permission planner.
					// origin: S7 internal review GATES-PROD F4
					if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "proposeValue" && len(call.Args) > 1 {
						if verb, ok := call.Args[1].(*ast.Ident); ok && verb.Name == "VerbPortFreeAllocation" {
							forwarded = true
						}
					}
					if target, ok := call.Fun.(*ast.SelectorExpr); ok && target.Sel.Name == "proposeOrForward" && len(call.Args) > 0 {
						if verb, ok := call.Args[0].(*ast.Ident); ok && verb.Name == "VerbPortFreeAllocation" {
							forwarded = true
						}
					}
				}
				return true
			})
			if identity && forwarded {
				mark(fn.Body, "narrowed")
			}
		}
		if path == "internal/broker/cluster_forward.go" {
			ast.Inspect(decl, func(node ast.Node) bool {
				pair, ok := node.(*ast.KeyValueExpr)
				if !ok {
					return true
				}
				key, ok := pair.Key.(*ast.Ident)
				if !ok || key.Name != "VerbPortFreeAllocation" {
					return true
				}
				call, ok := pair.Value.(*ast.CallExpr)
				if !ok || len(call.Args) != 1 {
					return true
				}
				name, ok := call.Fun.(*ast.Ident)
				if !ok || name.Name != "propose" {
					return true
				}
				fn, ok := call.Args[0].(*ast.FuncLit)
				if ok {
					mark(fn.Body, "p")
				}
				return true
			})
		}
	}
	return allowed
}

// Controls also use copied real files. Writing the mutant to a temporary tree
// keeps source scans honest without editing another parallel owner's file.
// origin: hot-update S6 plan §11 / §12.1 P31 / T53
func TestBrokerImageContractControls(t *testing.T) {
	// Bind the extracted real cut and mutate its exact consumer seams. This is
	// separate from the inline fixture: merely adding a dormant leaf cannot pass.
	// origin: hot-update S6 plan §3.2 / T53
	delegated := map[string]string{}
	delegatedRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(delegatedRoot, "cmd"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"internal/broker/broker.go", "internal/broker/reexec.go", "internal/brokerimage/store_reload.go"} {
		raw, err := os.ReadFile(filepath.Join(repoRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		delegated[path] = string(raw)
		destination := filepath.Join(delegatedRoot, path)
		if err := os.MkdirAll(filepath.Dir(destination), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(destination, raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := checkBrokerSealOrder(brokerGateSources(t, delegatedRoot)); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct{ path, old, new string }{
		{"internal/broker/broker.go", "err := runBrokerImageReload(b,", "err := dormantReload(b,"},
		{"internal/broker/reexec.go", "return runBrokerImageReloadUsing(b,", "return dormantReloadUsing(b,"},
		{"internal/brokerimage/store_reload.go", "if !errors.Is(err, ErrCutChanged)", "if !errors.Is(err, ErrImage)"},
		{"internal/brokerimage/store_reload.go", "if closeErr := prepared.Close();", "if closeErr := prepared.Unused();"},
		{"internal/brokerimage/store_reload.go", "bundle, nextResources, err = cut.Export(seal)", "bundle, nextResources, err = cut.Export(storage.StoreSeal{})"},
		{"internal/brokerimage/store_reload.go", "bundle, nextResources, err = cut.Export(seal)\n\t\tif err != nil {", "bundle, nextResources, err = cut.Export(seal)\n\t\tif false {"},
		{"internal/brokerimage/store_reload.go", "prepared, err = cut.Prepare(bundle, nextResources)\n\t\tif err != nil {", "prepared, err = cut.Prepare(bundle, nextResources)\n\t\tif false {"},
		{"internal/broker/reexec.go", "return prepared.Exec(path, bundle.Args, env)", "return nil"},
		{"internal/brokerimage/store_reload.go", "seal, err = cfg.Store.CloseForImage(deadline)", "seal, err = storage.StoreSeal{}, nil"},
		{"internal/brokerimage/store_reload.go", "seal, err = cfg.Store.CloseForImage(deadline)", "prepared, err = cut.Prepare(bundle, resources)\n\tseal, err = cfg.Store.CloseForImage(deadline)"},
		{"internal/brokerimage/store_reload.go", "prepared, err = cut.Prepare(bundle, resources)", "prepared, err = shutdown.Prepare(bundle, resources)"},
		{"internal/brokerimage/store_reload.go", "cut.Export(seal)", "cut.Export(storage.StoreSeal{})"},
		{"internal/brokerimage/store_reload.go", "cut.Prepare(bundle, resources)", "cut.Unused(bundle, resources)"},
		{"internal/brokerimage/store_reload.go", "executionErr = callbacks.Exec(", "executionErr = callbacks.Unused("},
		{"internal/brokerimage/store_reload.go", "seal, err = cfg.Store.CloseForImage(deadline)\n\tif err != nil {", "seal, err = cfg.Store.CloseForImage(deadline)\n\tif false {"},
	} {
		original := delegated[mutation.path]
		changed := strings.Replace(original, mutation.old, mutation.new, 1)
		if changed == original {
			t.Fatalf("delegated mutation did not apply: %s", mutation.old)
		}
		if err := os.WriteFile(filepath.Join(delegatedRoot, mutation.path), []byte(changed), 0600); err != nil {
			t.Fatal(err)
		}
		if checkBrokerSealOrder(brokerGateSources(t, delegatedRoot)) == nil {
			t.Fatalf("delegated cut defect escaped: %s", mutation.old)
		}
		if err := os.WriteFile(filepath.Join(delegatedRoot, mutation.path), []byte(original), 0600); err != nil {
			t.Fatal(err)
		}
	}

	// The permission path is the sole legacy free consumer allowed by §7.2.
	// Changing its owner, verb or identity must expose the old planner again.
	// origin: hot-update S6 plan §7.2 / T53
	permissionSources := map[string]string{}
	for _, path := range []string{"internal/broker/clusterwrite.go", "internal/broker/cluster_forward.go"} {
		raw, err := os.ReadFile(filepath.Join(repoRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		permissionSources[path] = string(raw)
	}
	if err := checkBrokerGuardedHomeMutators(permissionSources); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []struct{ path, old, new string }{
		{"internal/broker/clusterwrite.go", "func (b *Broker) freePortAllocation(", "func (b *Broker) cleanupAllocation("},
		{"internal/broker/clusterwrite.go", "return port.PlanFreeAllocation(db, narrowed,", "return port.PlanFreeAllocation(db, a,"},
		{"internal/broker/cluster_forward.go", "VerbPortFreeAllocation: propose(", "VerbProxyCleanup: propose("},
		{"internal/broker/cluster_forward.go", "return port.PlanFreeAllocation(db, p.allocation(),", "return port.PlanFreeAllocation(db, allocation,"},
	} {
		original := permissionSources[mutation.path]
		changed := strings.Replace(original, mutation.old, mutation.new, 1)
		if changed == original {
			t.Fatalf("permission mutation did not apply: %s", mutation.old)
		}
		permissionSources[mutation.path] = changed
		if checkBrokerGuardedHomeMutators(permissionSources) == nil {
			t.Fatalf("maintenance bypass borrowed permission consumer: %s", mutation.old)
		}
		permissionSources[mutation.path] = original
	}

	for _, name := range []string{"PlanReassignHome", "PlanAllocate", "PlanRotateAllocation", "PlanFreeAllocation"} {
		source := "package broker;import home \"github.com/LinZiyang666/tether/internal/port\";var plan = home." + name
		if err := checkBrokerGuardedHomeMutators(map[string]string{"internal/broker/reexec.go": source + "Guarded"}); err != nil {
			t.Fatal(err)
		}
		if checkBrokerGuardedHomeMutators(map[string]string{"internal/broker/reexec.go": source}) == nil {
			t.Fatalf("unguarded planner escaped %s", name)
		}
	}
	good := map[string]string{"internal/brokerimage/preflight.go": `package brokerimage;func Preflight(){ smoke() };func smoke(){}`}
	if err := checkBrokerPurePreflight(good); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`import s "database/sql";func Preflight(){s.Open("sqlite", "db")}`, `import s "github.com/LinZiyang666/tether/internal/storage";func Preflight(){helper()};func helper(){s.Open("db")}`, `import _ "modernc.org/sqlite";func Preflight(){}`} {
		if checkBrokerPurePreflight(map[string]string{"internal/brokerimage/preflight.go": "package brokerimage;" + body}) == nil {
			t.Fatal("impure preflight accepted")
		}
	}
	cut := `package broker;import image "github.com/LinZiyang666/tether/internal/brokerimage";func cut(){ image.Freeze(cfg); seal,err:=store.CloseForImage(d); if err!=nil{return}; bundle,resources,err:=cut.Export(seal); if err!=nil{return}; prepared,err:=cut.Prepare(bundle, resources); if err!=nil{return}; prepared.Exec(path, args, env) }`
	if err := checkBrokerSealOrder(map[string]string{"internal/broker/reexec.go": cut}); err != nil {
		t.Fatal(err)
	}
	for _, mutant := range []string{strings.Replace(cut, "seal,err:=store.CloseForImage(d);", "", 1), strings.Replace(cut, "seal,err:=store.CloseForImage(d);", "defer store.CloseForImage(d);", 1), strings.Replace(cut, "seal,err:=store.CloseForImage(d);", "prepared,err:=cut.Prepare(bundle,resources); seal,err:=store.CloseForImage(d);", 1), strings.Replace(cut, "image.Freeze", "image.Thaw", 1), strings.Replace(cut, "cut.Export(seal)", "cut.Export(preliminary)", 1), strings.Replace(cut, "if err!=nil{return}", "if err!=nil{logError(err)}", 1)} {
		if checkBrokerSealOrder(map[string]string{"internal/broker/reexec.go": mutant}) == nil {
			t.Fatal("premature or missing live cut accepted")
		}
	}
	for _, source := range []string{`package x;import s "github.com/LinZiyang666/tether/internal/brokerimage/contractstub";var _=s.Freeze`, `package x;var err="not implemented (S6 P01)"`} {
		if checkBrokerProductionStubs(map[string]string{"internal/x/x_other.go": source}) == nil {
			t.Fatal("production stub accepted")
		}
	}
	if err := checkBrokerProductionStubs(map[string]string{"internal/x/x.go": `package x // contractstub is prose`}); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	root := repoRoot(t)
	for path := range brokerFrozenPersistentFiles {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		files[path] = string(raw)
	}
	if err := checkBrokerPersistentFiles(files); err != nil {
		t.Fatal(err)
	}
	for path, source := range files {
		files[path] = source + "mutant"
		if checkBrokerPersistentFiles(files) == nil {
			t.Fatalf("persistent drift escaped: %s", path)
		}
		files[path] = source
	}
	files["internal/storage/migrations/9999_broker_image.sql"] = "CREATE TABLE forbidden(id INTEGER);"
	if checkBrokerPersistentFiles(files) == nil {
		t.Fatal("new migration escaped")
	}
	for path, want := range brokerFrozenContracts {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		got, err := brokerCanonicalDigest(string(raw) + "\n// harmless comment\n")
		if err != nil || got != want {
			t.Fatalf("comment normalization %s: %s %v", path, got, err)
		}
		got, err = brokerCanonicalDigest(string(raw) + "\ntype ContractDrift int\n")
		if err != nil || got == want {
			t.Fatalf("contract addition escaped %s", path)
		}
	}
	// The S8 authority addition is pinned with every existing S6 field, rather
	// than stripping a broad declaration family or allowing arbitrary drift.
	// Exercise the actual file reader on disk for both sides of this seam.
	// origin: S7 internal review GATE-CONVERGE-layering F5
	portPath := "internal/port/image_contracts.go"
	portRaw, err := os.ReadFile(filepath.Join(root, portPath))
	if err != nil {
		t.Fatal(err)
	}
	portRoot := transferFixtureTree(t, map[string]string{portPath: string(portRaw)})
	for _, pair := range [][2]string{{"Authority upgradeauthority.Guard", "Authority bool"}, {"Expected            *Allocation", "Expected            Allocation"}, {"NowBootNS           int64", "NowBootNS           uint64"}} {
		changed := strings.Replace(string(portRaw), pair[0], pair[1], 1)
		if changed == string(portRaw) {
			t.Fatalf("port contract mutation did not apply: %s", pair[0])
		}
		transferWriteFixture(t, portRoot, portPath, changed)
		raw, err := os.ReadFile(filepath.Join(portRoot, portPath))
		if err != nil {
			t.Fatal(err)
		}
		got, err := brokerCanonicalDigest(string(raw))
		if err != nil || got == brokerFrozenContracts[portPath] {
			t.Fatalf("on-disk allocation/authority contract drift escaped: %s", pair[0])
		}
	}
	rawContract, err := os.ReadFile(filepath.Join(root, "internal/brokerimage/contracts.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, pair := range [][2]string{{"Generation      uint64", "Generation      uint32"}, {"json:\"generation\"", "json:\"generation_drift\""}, {"type FreezeFunc = func(Config)", "type FreezeFunc = func(*Config)"}} {
		changed := strings.Replace(string(rawContract), pair[0], pair[1], 1)
		if changed == string(rawContract) {
			t.Fatalf("real contract mutation did not apply: %s", pair[0])
		}
		digest, err := brokerCanonicalDigest(changed)
		if err != nil || digest == brokerFrozenContracts["internal/brokerimage/contracts.go"] {
			t.Fatalf("real contract drift escaped: %s (%v)", pair[0], err)
		}
	}
	// A copied production file, mutated on disk, exercises the live source reader.
	temp := t.TempDir()
	for _, dir := range []string{"internal/brokerimage", "cmd"} {
		if err := os.MkdirAll(filepath.Join(temp, dir), 0700); err != nil {
			t.Fatal(err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal/brokerimage/contracts.go"))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(temp, "internal/brokerimage/contracts.go")
	if err := os.WriteFile(path, append(raw, []byte("\nvar forbiddenStub = \"not implemented (S6 P01)\"\n")...), 0600); err != nil {
		t.Fatal(err)
	}
	if checkBrokerProductionStubs(brokerGateSources(t, temp)) == nil {
		t.Fatal("on-disk real-family stub escaped")
	}
}

// Own the handwritten call-graph/source parser: malformed inputs may fail but
// must not panic; purity and ownership are still asserted by deterministic controls.
// origin: hot-update S6 plan §12.1 P31 / T53
func FuzzBrokerContractSourceParser(f *testing.F) {
	for _, seed := range []string{"package x", "package brokerimage;func Preflight(){ Preflight() }", "package x;import c \"context\";func F(){c.Background()}", "package"} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, source string) {
		if len(source) > 65536 {
			t.Skip()
		}
		sources := map[string]string{"internal/brokerimage/preflight.go": source}
		_ = checkBrokerProductionStubs(sources)
		_ = checkBrokerPurePreflight(sources)
		_ = checkBrokerSealOrder(sources)
		_ = checkBrokerGuardedHomeMutators(sources)
	})
}

// T53 is the exact plan entry; individual gates remain independently runnable.
// origin: hot-update S6 plan §11 / T53
func TestBrokerStructuralAndLayeringControls(t *testing.T) {
	t.Run("contracts", TestBrokerImageContractControls)
	t.Run("structural", TestBrokerStructuralBudgetControls)
	t.Run("layering", TestBrokerLayeringControls)
	t.Run("dataplane", TestBrokerDataplaneLifetimeControls)
	t.Run("platform", TestBrokerBuildTagControls)
	t.Run("contexts", TestBrokerContextRootControls)
	t.Run("budgets", TestBrokerBudgetControls)
}

// Keep diagnostics stable across map iteration, including the smallest mutant.
func brokerSortedSourcePaths(sources map[string]string) []string {
	paths := make([]string, 0, len(sources))
	for path := range sources {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

// Recursively follow the actual image field/alias definitions. S9's upgrade
// domain excludes live infra owners from image state; a nested wrapper or a
// Darwin-only alias must not turn the asserted zero delta into an exemption.
// Existing S6 hashes and private inventory assertions remain independent.
// origin: hot-update S9 plan §7.3 / §10.2 / T82
func checkInfraImageState(sources map[string]string, roots []string) error {
	if len(roots) == 0 {
		return fmt.Errorf("empty S9 image-state roots")
	}
	all, err := transferDeclarations(sources)
	if err != nil {
		return err
	}
	index := map[string][]transferDeclaration{}
	for _, decl := range all {
		if _, ok := decl.node.(*ast.TypeSpec); ok {
			key := filepath.ToSlash(filepath.Dir(decl.path)) + ":" + decl.name
			index[key] = append(index[key], decl)
		}
	}
	seen := map[string]bool{}
	var walk func(transferDeclaration, ast.Expr) error
	visit := func(key string) error {
		if seen[key] {
			return nil
		}
		seen[key] = true
		for _, decl := range index[key] {
			if err := walk(decl, decl.node.(*ast.TypeSpec).Type); err != nil {
				return err
			}
		}
		return nil
	}
	walk = func(decl transferDeclaration, expr ast.Expr) error {
		pkg := filepath.ToSlash(filepath.Dir(decl.path))
		key := ""
		switch value := expr.(type) {
		case *ast.Ident:
			key = pkg + ":" + value.Name
		case *ast.SelectorExpr:
			if id, ok := value.X.(*ast.Ident); ok {
				pkg = strings.TrimPrefix(decl.imports[id.Name], modPrefix)
				key = pkg + ":" + value.Sel.Name
			}
		case *ast.StructType:
			for _, field := range value.Fields.List {
				if err := walk(decl, field.Type); err != nil {
					return err
				}
			}
		case *ast.StarExpr:
			return walk(decl, value.X)
		case *ast.ArrayType:
			return walk(decl, value.Elt)
		case *ast.MapType:
			if err := walk(decl, value.Key); err != nil {
				return err
			}
			return walk(decl, value.Value)
		case *ast.ChanType:
			return walk(decl, value.Value)
		}
		if key != "" {
			for _, leaf := range infraLeafRoots {
				if pkg == "internal/"+leaf || strings.HasPrefix(pkg, "internal/"+leaf+"/") {
					return fmt.Errorf("%s: image state reaches live S9 owner %s", decl.path, key)
				}
			}
			return visit(key)
		}
		return nil
	}
	for _, root := range roots {
		if len(index[root]) == 0 {
			return fmt.Errorf("missing actual image-state root %s", root)
		}
		if err := visit(root); err != nil {
			return err
		}
	}
	return nil
}

// origin: hot-update S9 plan §10.2 / T82
// gate-control: TestInfraImageStateControls
func TestInfraImageSchemasExcludeLiveOwners(t *testing.T) {
	if err := checkInfraImageState(brokerGateSources(t, repoRoot(t)), []string{"internal/agentimage:Bundle", "internal/brokerimage:Bundle", "internal/brokerimage:PrivateState"}); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §11 / T82 / M31
func TestInfraImageStateControls(t *testing.T) {
	path := "internal/brokerimage/contracts.go"
	good := `package image;type Nested struct{Count uint64};type Bundle struct{State Nested}`
	root := transferFixtureTree(t, map[string]string{path: good})
	roots := []string{"internal/brokerimage:Bundle"}
	read := func() map[string]string { return brokerGateSources(t, root) }
	if err := checkInfraImageState(read(), roots); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		`package image;import c "github.com/LinZiyang666/tether/internal/infracontract";type Bundle struct{State c.Checkpoint}`,
		`package image;import c "github.com/LinZiyang666/tether/internal/infracontract";type Alias=c.Checkpoint;type Nested struct{State Alias};type Bundle struct{State *Nested}`,
		`package image;import c "github.com/LinZiyang666/tether/internal/infraupgrade";type Bundle struct{State map[string][]c.SupportState}`,
	} {
		transferWriteFixture(t, root, path, source)
		if checkInfraImageState(read(), roots) == nil {
			t.Fatal("on-disk hidden live infra image owner escaped")
		}
	}
	transferWriteFixture(t, root, path, good)
	transferWriteFixture(t, root, "internal/brokerimage/hidden_other.go", `package image;import c "github.com/LinZiyang666/tether/internal/infracontract";type Nested struct{State c.Checkpoint}`)
	if checkInfraImageState(read(), roots) == nil {
		t.Fatal("excluded-platform image owner escaped")
	}
	if checkInfraImageState(read(), []string{"internal/brokerimage:Missing"}) == nil {
		t.Fatal("missing image root escaped")
	}
}
