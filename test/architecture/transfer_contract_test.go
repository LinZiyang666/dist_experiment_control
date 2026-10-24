package architecture

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// These gates read source and manifests; no product runtime or child process is
// started. Missing implementations and inventories are failures, never stubs
// that certify continuity. Sections share P35's frozen source-moves artifact;
// private fields use its separate state inventory, not the S5/S6 namespaces.
// origin: hot-update S7 plan §11.2 / T48–T50
const transferContractInventory = "test/architecture/testdata/transfer_source_moves.tsv#contract-union"
const transferScannerInventory = "test/architecture/testdata/transfer_source_moves.tsv#scanner-roots"
const transferLegacyInventory = "test/architecture/testdata/transfer_source_moves.tsv#legacy-put"

var transferLeafRoots = []string{
	"xfercontract", "xferworker", "xfersend", "xferreceive", "xfercourier",
	"xfertrack", "xferfinal", "xferledger", "xferhistory", "xferbucket",
	"xfergc", "xfercontrol", "xferclient", "xferimage", "xferauth", "xferbridge", "xferaudit",
	// origin: hot-update S7 plan §11.4 / T50; INTEG-M live assembly owners.
	"xferagent", "xferagentimage", "xferserve", "xfercapacity", "xferid", "xferproof",
}

type transferDeclaration struct {
	path, name string
	node       ast.Node
	imports    map[string]string
	graph      *transferGraph
}

func transferDeclarations(sources map[string]string) (map[string]transferDeclaration, error) {
	result := map[string]transferDeclaration{}
	graph := newTransferGraph(sources)
	for _, path := range brokerSortedSourcePaths(sources) {
		if !isProdGoFile(path) {
			continue
		}
		file, err := parser.ParseFile(graph.fset, path, sources[path], parser.ParseComments)
		if err != nil {
			return nil, err
		}
		graph.files[path] = file
		add := func(name string, node ast.Node) error {
			key := path + ":" + name
			if _, duplicate := result[key]; duplicate {
				return fmt.Errorf("duplicate declaration %s", key)
			}
			result[key] = transferDeclaration{path: path, name: name, node: node, imports: brokerImports(file), graph: graph}
			return nil
		}
		for _, declaration := range file.Decls {
			switch node := declaration.(type) {
			case *ast.FuncDecl:
				name := node.Name.Name
				if node.Recv != nil {
					name = receiverTypeName(node.Recv.List[0].Type) + "." + name
				}
				if err := add(name, node); err != nil {
					return nil, err
				}
			case *ast.GenDecl:
				for _, spec := range node.Specs {
					switch value := spec.(type) {
					case *ast.TypeSpec:
						if err := add(value.Name.Name, value); err != nil {
							return nil, err
						}
					case *ast.ValueSpec:
						for _, name := range value.Names {
							if name.Name != "_" {
								if err := add(name.Name, value); err != nil {
									return nil, err
								}
							}
						}
					}
				}
			}
		}
	}
	return result, nil
}

// References include method values, aliases and callback arguments. The typed
// graph keeps an unrelated receiver's identically named method out of the
// contract closure while preserving the function value supplied to a port.
// origin: hot-update S7 plan §11.2 / T48–T49 / S7-GATES lead decisions 1–2
func reachableTransferDeclarations(all map[string]transferDeclaration, root string) ([]transferDeclaration, error) {
	decl, ok := all[root]
	if !ok {
		return nil, fmt.Errorf("missing actual scanner root %s", root)
	}
	if err := decl.graph.build(all); err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	var result []transferDeclaration
	var visit func(string)
	visit = func(key string) {
		if seen[key] {
			return
		}
		seen[key] = true
		result = append(result, all[key])
		for _, target := range decl.graph.edges[key] {
			visit(target)
		}
	}
	visit(root)
	return result, nil
}

func readTransferTSV(root, path string, columns int) ([][]string, error) {
	file, section, _ := strings.Cut(path, "#")
	raw, err := os.ReadFile(filepath.Join(root, file))
	if err != nil {
		return nil, fmt.Errorf("P35 inventory %s: %w", path, err)
	}
	var rows [][]string
	seen := map[string]bool{}
	active := section == ""
	for i, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "# section: ") {
			active = strings.TrimPrefix(line, "# section: ") == section
			continue
		}
		if !active {
			continue
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != columns || seen[line] || slices.Contains(fields, "") {
			return nil, fmt.Errorf("%s:%d: malformed or duplicate inventory row", path, i+1)
		}
		seen[line] = true
		rows = append(rows, fields)
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("%s: empty scanner/inventory root", path)
	}
	return rows, nil
}

// Exact equality catches extra fields as well as omissions. Neither a stale
// golden nor an empty scan can substitute for the actual union.
func checkTransferInventory(actual, frozen []string) error {
	if len(actual) == 0 || len(frozen) == 0 {
		return fmt.Errorf("empty transfer inventory")
	}
	for _, rows := range [][]string{actual, frozen} {
		seen := map[string]bool{}
		for _, row := range rows {
			if seen[row] {
				return fmt.Errorf("duplicate inventory row %s", row)
			}
			seen[row] = true
		}
	}
	a, b := slices.Clone(actual), slices.Clone(frozen)
	sort.Strings(a)
	sort.Strings(b)
	if !slices.Equal(a, b) {
		removed, added := transferDifference(b, a), transferDifference(a, b)
		return fmt.Errorf("transfer inventory drift: removed=%d %v added=%d %v", len(removed), removed[:min(20, len(removed))], len(added), added[:min(20, len(added))])
	}
	return nil
}

func transferDifference(a, b []string) []string {
	var out []string
	known := map[string]bool{}
	for _, row := range b {
		known[row] = true
	}
	for _, row := range a {
		if !known[row] {
			out = append(out, row)
		}
	}
	return out
}

// Resolve aliases against the actual declaration graph rather than expecting a
// DTO to be written in a particular file or with a particular import spelling.
func transferStructRows(all map[string]transferDeclaration, site string, seen map[string]bool) ([]string, error) {
	if seen[site] {
		return nil, fmt.Errorf("cyclic struct alias %s", site)
	}
	seen[site] = true
	decl, ok := all[site]
	if !ok {
		return nil, fmt.Errorf("missing wire type %s", site)
	}
	spec, ok := decl.node.(*ast.TypeSpec)
	if !ok {
		return nil, fmt.Errorf("%s is not a type", site)
	}
	structure, ok := spec.Type.(*ast.StructType)
	if !ok {
		pkg, name := filepath.ToSlash(filepath.Dir(decl.path)), ""
		switch value := spec.Type.(type) {
		case *ast.Ident:
			name = value.Name
		case *ast.SelectorExpr:
			alias, ok := value.X.(*ast.Ident)
			if ok {
				pkg, name = strings.TrimPrefix(decl.imports[alias.Name], modPrefix), value.Sel.Name
			}
		}
		for key, candidate := range all {
			if filepath.ToSlash(filepath.Dir(candidate.path)) == pkg && candidate.name == name {
				return transferStructRows(all, key, seen)
			}
		}
		return nil, fmt.Errorf("unsupported wire alias %s", site)
	}
	var rows []string
	for _, field := range structure.Fields.List {
		if len(field.Names) == 0 {
			return nil, fmt.Errorf("%s: anonymous wire field must have an explicit canonical mapping", site)
		}
		tag := "-"
		if field.Tag != nil {
			tag, _ = strconv.Unquote(field.Tag.Value)
		}
		for _, name := range field.Names {
			if name.IsExported() {
				rows = append(rows, name.Name+"\t"+tag+"\t"+transferExpr(field.Type, decl.imports))
			}
		}
	}
	return rows, nil
}

func transferExpr(expr ast.Expr, imports map[string]string) string {
	switch node := expr.(type) {
	case *ast.Ident:
		return node.Name
	case *ast.SelectorExpr:
		if alias, ok := node.X.(*ast.Ident); ok {
			return imports[alias.Name] + "." + node.Sel.Name
		}
	case *ast.StarExpr:
		return "*" + transferExpr(node.X, imports)
	case *ast.ArrayType:
		if node.Len == nil {
			return "[]" + transferExpr(node.Elt, imports)
		}
	}
	return fmt.Sprintf("%T", expr)
}

// origin: hot-update S7 plan §6.2 / §8.3 / T48
// gate-control: TestTransferContractInventoryControls
func TestTransferContractsAndInventoriesAgree(t *testing.T) {
	root := repoRoot(t)
	actual := actualTransferContractRows(t, root)
	frozenRows, err := readTransferTSV(root, transferContractInventory, 4)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []string
	for _, row := range frozenRows {
		frozen = append(frozen, strings.Join(row, "\t"))
	}
	if err := checkTransferInventory(actual, frozen); err != nil {
		t.Fatal(err)
	}
}

func actualTransferContractRows(t *testing.T, root string) []string {
	t.Helper()
	all, err := transferDeclarations(brokerGateSources(t, root))
	if err != nil {
		t.Fatal(err)
	}
	var actual []string
	for _, name := range []string{
		"Identity", "Origin", "Intent", "CreationPermit", "BudgetSeal", "SourceSeal",
		"CommitGrant", "ReceiverOutcome", "TerminalReceipt", "SettlementCapsule", "ImageRef",
		"TransferPermitReq", "TransferPermitResp", "TransferProbeReq", "TransferProbeResp",
		"TransferStatusReq", "TransferStatusResp", "TransferSourceReq", "TransferReserveReq",
		"TransferReserveResp", "TransferTerminalReq", "TransferTerminalResp", "TransferCancelReq",
		"TransferCensusReq", "TransferCensusResp", "TransferContinuation",
	} {
		matches := 0
		for site, decl := range all {
			if filepath.Dir(decl.path) != "internal/proto" || decl.name != name {
				continue
			}
			matches++
			rows, err := transferStructRows(all, site, map[string]bool{})
			if err != nil || len(rows) == 0 {
				t.Fatalf("wire %s: %v (fields=%d)", name, err, len(rows))
			}
			for _, row := range rows {
				actual = append(actual, "wire\tproto."+name+"."+row)
			}
		}
		if matches != 1 {
			t.Errorf("P01: wire root %s has %d declarations, want one", name, matches)
		}
	}
	for _, name := range []string{"PushPrepareResp", "PullPrepareResp", "NodeRegisterReq", "NodeRegisterResp"} {
		found := false
		for site, decl := range all {
			if filepath.Dir(decl.path) != "internal/proto" || decl.name != name {
				continue
			}
			rows, err := transferStructRows(all, site, map[string]bool{})
			if err != nil {
				t.Fatal(err)
			}
			field := "TransferVersion\tjson:\"transfer_version,omitempty\"\tstring"
			if strings.Contains(name, "Prepare") {
				field = "ObjectVersion\tjson:\"object_version,omitempty\"\tstring"
			}
			found = slices.Contains(rows, field)
			for _, row := range rows {
				actual = append(actual, "wire\tproto."+name+"."+row)
			}
		}
		if !found {
			t.Errorf("P01: %s missing additive top-level transfer field", name)
		}
	}
	for _, row := range transferPublicImageRows(t) {
		actual = append(actual, "public\t"+row)
	}
	roots, err := readTransferTSV(root, transferScannerInventory, 4)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range roots {
		if !strings.HasPrefix(row[0], "contract/") {
			continue
		}
		nodes, err := reachableTransferDeclarations(all, row[2])
		if err != nil {
			t.Fatal(err)
		}
		for _, token := range transferRootTokens(nodes) {
			actual = append(actual, row[0]+"\t"+row[2]+"\t"+token+"\t-")
		}
	}
	return actual
}

// Tokens are harvested only from reachable live declarations, never from an
// unattached descriptor table. Function names and imports are kept so moved
// SUB/ACL/header/signing helpers cannot be silently omitted by a stale scan.
func transferRootTokens(nodes []transferDeclaration) []string {
	set := map[string]bool{}
	for _, decl := range nodes {
		ast.Inspect(decl.node, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.BasicLit:
				if value.Kind == token.STRING {
					text, _ := strconv.Unquote(value.Value)
					set["literal="+strconv.Quote(text)] = true
				}
			case *ast.SelectorExpr:
				set["selector="+transferExpr(value, decl.imports)] = true
			}
			return true
		})
	}
	var out []string
	for key := range set {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

// origin: hot-update S7 plan §10.1 / T48
func TestTransferContractInventoryControls(t *testing.T) {
	exerciseTypedTransferReachability(t)
	good := []string{"wire\tproto.Identity.ObjectVersion\tjson:\"object_version\"\tstring", "public\txfercontract.ImageRef.Digest\tjson:\"digest\"\tstring"}
	if err := checkTransferInventory(good, slices.Clone(good)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range [][]string{nil, good[:1], append(slices.Clone(good), "extra"), {good[0], good[0]}} {
		if checkTransferInventory(bad, good) == nil || checkTransferInventory(good, bad) == nil {
			t.Fatalf("missing/extra/duplicate/empty inventory escaped: %v", bad)
		}
	}
	root := transferFixtureTree(t, map[string]string{
		"internal/proto/value.go": "package proto;type Identity struct{ObjectVersion string `json:\"object_version\"`}",
		"internal/proto/alias.go": "package proto;type Alias=Identity",
	})
	all, err := transferDeclarations(brokerGateSources(t, root))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := transferStructRows(all, "internal/proto/alias.go:Alias", map[string]bool{})
	if err != nil || !slices.Equal(rows, []string{"ObjectVersion\tjson:\"object_version\"\tstring"}) {
		t.Fatalf("actual alias fields: %v %v", rows, err)
	}
	for _, source := range []string{
		"package proto;type Identity struct{}",
		"package proto;type Identity struct{ObjectVersion string `json:\"wrong\"`}",
		"package proto;type Identity struct{ObjectVersion string `json:\"object_version\"`; Extra bool}",
	} {
		transferWriteFixture(t, root, "internal/proto/value.go", source)
		mutant, err := transferDeclarations(brokerGateSources(t, root))
		if err != nil {
			t.Fatal(err)
		}
		got, err := transferStructRows(mutant, "internal/proto/alias.go:Alias", map[string]bool{})
		if err == nil && checkTransferInventory(got, rows) == nil {
			t.Fatal("on-disk wire field mutation escaped")
		}
	}
	if len(collectImageFields(reflect.TypeFor[xfercontract.ImageRef]())) != 5 {
		t.Fatal("frozen public reference shape drifted")
	}
}

// The legacy allowance is an exact function ledger, not a file exemption.
// Only the SDK adapter may reference Put; capable references to any of the
// three allowed functions remain forbidden. Empty/dead allowances are red.
func checkTransferSDKPut(sources map[string]string, allowed map[string]string) error {
	all, err := transferDeclarations(sources)
	if err != nil {
		return err
	}
	roles := map[string]bool{}
	for site, role := range allowed {
		decl, live := all[site]
		if !live || roles[role] {
			return fmt.Errorf("dead or duplicate legacy allowance %s", site)
		}
		roles[role] = true
		switch role {
		case "sdk":
			if site != "internal/jsstream/object_legacy.go:PutLegacyOnce" {
				return fmt.Errorf("SDK Put allowance moved outside the one-shot adapter")
			}
		case "client":
			if decl.path != "internal/xferclient/legacy.go" {
				return fmt.Errorf("legacy client allowance escaped legacy.go")
			}
		case "agent":
			if filepath.Dir(decl.path) != "internal/filetransfer" {
				return fmt.Errorf("legacy agent allowance escaped filetransfer")
			}
		default:
			return fmt.Errorf("unknown legacy role %s", role)
		}
	}
	if len(roles) != 3 {
		return fmt.Errorf("legacy Put allowlist must contain sdk/client/agent")
	}
	if err := all["internal/jsstream/object_legacy.go:PutLegacyOnce"].graph.build(all); err != nil {
		return err
	}
	var putErrors []error
	for _, site := range transferSortedDeclarationSites(all) {
		puts := all[site].graph.sdkPuts[site]
		if puts > 0 && allowed[site] != "sdk" {
			putErrors = append(putErrors, fmt.Errorf("%s: SDK Put reference outside legacy adapter", site))
		}
		if allowed[site] == "sdk" && puts != 1 {
			putErrors = append(putErrors, fmt.Errorf("%s: one-shot adapter must reference SDK Put exactly once", site))
		}
	}
	if err := errors.Join(putErrors...); err != nil {
		return err
	}
	for site, role := range allowed {
		if role == "sdk" {
			loop := false
			ast.Inspect(all[site].node, func(node ast.Node) bool {
				if _, ok := node.(*ast.ForStmt); ok {
					loop = true
				}
				if _, ok := node.(*ast.RangeStmt); ok {
					loop = true
				}
				return true
			})
			if loop {
				return fmt.Errorf("legacy SDK adapter contains a retry loop")
			}
			continue
		}
		nodes, err := reachableTransferDeclarations(all, site)
		if err != nil {
			return err
		}
		callsSDK := false
		for _, node := range nodes {
			callsSDK = callsSDK || allowed[node.path+":"+node.name] == "sdk"
		}
		if !callsSDK {
			return fmt.Errorf("dead legacy Put caller %s", site)
		}
	}
	capable := 0
	for site, decl := range all {
		fn, function := decl.node.(*ast.FuncDecl)
		if !function || fn.Body == nil {
			continue
		}
		if filepath.Dir(decl.path) != "internal/xferclient" || allowed[site] != "" || decl.path == "internal/xferclient/legacy.go" {
			continue
		}
		capable++
		nodes, err := reachableTransferDeclarations(all, site)
		if err != nil {
			return err
		}
		for _, target := range nodes {
			if allowed[target.path+":"+target.name] != "" || target.path == "internal/xferclient/legacy.go" {
				return fmt.Errorf("%s: capable call graph reaches legacy %s", site, target.name)
			}
			if transferMintsOrRewinds(target) {
				return fmt.Errorf("%s: capable invocation rewinds or mints a new identity at %s:%s", site, target.path, target.name)
			}
		}
	}
	if capable == 0 {
		return fmt.Errorf("empty capable invocation root (pending P25)")
	}
	return nil
}

// origin: hot-update S7 plan §7 / T49 / M01
// origin: S7 internal review INTEG-G F5
// gate-control: TestTransferSDKPutCallGraphControls
// origin: S7 internal review GATE-CONVERGE-contracts F5
func TestTransferContinuationCannotReachSDKPut(t *testing.T) {
	root := repoRoot(t)
	rows, err := readTransferTSV(root, transferLegacyInventory, 2)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]string{}
	for _, row := range rows {
		if _, duplicate := allowed[row[0]]; duplicate {
			t.Fatal("duplicate legacy function allowance")
		}
		allowed[row[0]] = row[1]
	}
	sources := brokerGateSources(t, root)
	if err := checkTransferSDKPut(sources, allowed); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §7 / T49 / M01
// origin: S7 internal review INTEG-G F5
func TestTransferSDKPutCallGraphControls(t *testing.T) {
	allowed := map[string]string{"internal/jsstream/object_legacy.go:PutLegacyOnce": "sdk", "internal/xferclient/legacy.go:NewLegacyObject": "client", "internal/filetransfer/legacy.go:legacyOnce": "agent"}
	sources := map[string]string{
		"internal/jsstream/object_legacy.go": `package jsstream;import j "github.com/nats-io/nats.go/jetstream";var store j.ObjectStore;func PutLegacyOnce(){store.Put(nil,j.ObjectMeta{},nil)}`,
		"internal/xferclient/legacy.go":      "package xferclient;func NewLegacyObject(put func()){put()}",
		"internal/filetransfer/legacy.go":    "package filetransfer;import j \"github.com/LinZiyang666/tether/internal/jsstream\";func legacyOnce(){j.PutLegacyOnce()}",
		"cmd/tether/transfer.go":             "package main;import j \"github.com/LinZiyang666/tether/internal/jsstream\";import c \"github.com/LinZiyang666/tether/internal/xferclient\";func legacy(){c.NewLegacyObject(j.PutLegacyOnce)}",
		"internal/xferclient/invocation.go":  "package xferclient;type Writer struct{};func(Writer)Append(){};func(Writer)Put(){};var writer Writer;func New(){writer.Append();writer.Put()}",
	}
	root := transferFixtureTree(t, sources)
	transferWriteFixture(t, root, "internal/cli/transfer.go", "package cli;import j \"github.com/LinZiyang666/tether/internal/jsstream\";func Retry(){j.PutLegacyOnce()}")
	check := func() error { return checkTransferSDKPut(brokerGateSources(t, root), allowed) }
	if err := check(); err != nil {
		t.Fatal(err)
	}
	// Consumer names retire subscriptions; they do not replace the invocation
	// or the object version whose synced prefix the new consumer reads.
	// origin: S7 internal review INTEG-G F5
	transferWriteFixture(t, root, "internal/jsstream/object_resume.go", `package jsstream;import "github.com/nats-io/nuid";import j "github.com/nats-io/nats.go/jetstream";type transferResumeReader struct{consumer string};func(r *transferResumeReader)openConsumer(){r.consumer=nuid.Next();_ = j.ConsumerConfig{Name:r.consumer}};func Read(){r:=&transferResumeReader{};r.openConsumer()}`)
	transferWriteFixture(t, root, "internal/xferclient/invocation.go", `package xferclient;import j "github.com/LinZiyang666/tether/internal/jsstream";func New(){j.Read()}`)
	if err := check(); err != nil {
		t.Fatalf("original-object consumer renewal: %v", err)
	}
	for _, source := range []string{"package xferclient;func New(){source.Seek(1,0)}", "package xferclient;func New(){const offset=1;source.Seek(offset,0)}"} {
		transferWriteFixture(t, root, "internal/xferclient/invocation.go", source)
		if err := check(); err != nil {
			t.Fatalf("nonzero offset precision: %v", err)
		}
	}
	transferWriteFixture(t, root, "internal/xferclient/invocation.go", `package xferclient;import j "github.com/nats-io/nats.go/jetstream";func New(js j.JetStream){_,_=js.ObjectStore(nil,"original-bucket")}`)
	if err := check(); err != nil {
		t.Fatalf("capable original-object metadata lookup: %v", err)
	}
	transferWriteFixture(t, root, "internal/xferclient/invocation.go", sources["internal/xferclient/invocation.go"])
	legacyCLI, err := os.ReadFile(filepath.Join(root, "cmd/tether/transfer.go"))
	if err != nil {
		t.Fatal(err)
	}
	transferWriteFixture(t, root, "cmd/tether/transfer.go", strings.Replace(string(legacyCLI), "package main;", "package main;import sdk \"github.com/nats-io/nats.go/jetstream\";", 1)+";var sdkStore sdk.ObjectStore;func pushTierB(){sdkStore.Put(nil,sdk.ObjectMeta{},nil)}")
	if check() == nil {
		t.Fatal("on-disk outer CLI SDK Put escaped")
	}
	transferWriteFixture(t, root, "cmd/tether/transfer.go", string(legacyCLI))
	for _, source := range []string{
		`package xferclient;import j "github.com/nats-io/nats.go/jetstream";var store j.ObjectStore;func New(){store.Put(nil,j.ObjectMeta{},nil)}`,
		`package xferclient;import j "github.com/nats-io/nats.go/jetstream";var store j.ObjectStore;func New(){put:=store.Put;put(nil,j.ObjectMeta{},nil)}`,
		`package xferclient;import j "github.com/nats-io/nats.go/jetstream";var store j.ObjectStore;var put=store.Put;func New(){put(nil,j.ObjectMeta{},nil)}`,
		"package xferclient;import j \"github.com/LinZiyang666/tether/internal/jsstream\";func New(){f:=func(){NewLegacyObject(j.PutLegacyOnce)};f()}",
		"package xferclient;import j \"github.com/LinZiyang666/tether/internal/jsstream\";func New(){alias(j.PutLegacyOnce)};var alias=NewLegacyObject",
		"package xferclient;import j \"github.com/LinZiyang666/tether/internal/jsstream\";func New(){wrapper()};func wrapper(){NewLegacyObject(j.PutLegacyOnce)}",
		"package xferclient;import alias \"github.com/LinZiyang666/tether/internal/jsstream\";func New(){alias.PutLegacyOnce()}",
		"package xferclient;import alias \"github.com/LinZiyang666/tether/internal/cli\";func New(){alias.Retry()}",
		"package xferclient;func New(){source.Seek(0,0)}",
		"package xferclient;func New(){wrapper()};func wrapper(){source.Seek(0,0)}",
		// origin: S7 internal review INTEG-G F5 — method values must not hide rewinds.
		"package xferclient;func New(){rewind:=source.Seek;rewind(0,0)}",
		"package xferclient;func New(){const start=0;source.Seek(start,0)}",
		"package xferclient;func New(){source.Seek(0x0,0)}",
		"package xferclient;func New(){NewTransferID()}",
		"package xferclient;import id \"github.com/nats-io/nuid\";func New(){id.Next()}",
		"package xferclient;type Factory func()", // no capable implementation
	} {
		transferWriteFixture(t, root, "internal/xferclient/invocation.go", source)
		if check() == nil {
			t.Fatalf("on-disk capable call graph mutation escaped: %s", source)
		}
	}
	transferWriteFixture(t, root, "internal/xferclient/invocation.go", sources["internal/xferclient/invocation.go"])
	delete(allowed, "internal/filetransfer/legacy.go:legacyOnce")
	if check() == nil {
		t.Fatal("empty legacy role escaped")
	}
}

func transferFixtureTree(t *testing.T, sources map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for _, top := range []string{"internal", "cmd"} {
		if err := os.MkdirAll(filepath.Join(root, top), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for path, source := range sources {
		transferWriteFixture(t, root, path, source)
	}
	return root
}

func transferWriteFixture(t *testing.T, root, path, source string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(root, filepath.Dir(path)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, path), []byte(source), 0o600); err != nil {
		t.Fatal(err)
	}
}
