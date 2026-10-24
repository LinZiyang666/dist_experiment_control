package architecture

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/xfercontract"
)

const transferStateInventory = "test/architecture/testdata/transfer_state_inventory.txt"
const transferDocumentationClaims = "test/architecture/testdata/transfer_source_moves.tsv#documentation-claims"

// Public and private schemas deliberately have different roots. Adding worker
// detail to Bundle must not be blessed by adding it to the private inventory.
// origin: hot-update S7 plan §8.3 / T48
func transferPrivateRows() []string {
	set := map[string]bool{}
	for _, typ := range []reflect.Type{reflect.TypeFor[xfercontract.WorkerState](), reflect.TypeFor[xfercontract.TrackerState](), reflect.TypeFor[xfercontract.LedgerRow]()} {
		for _, row := range collectImageFields(typ) {
			set[row] = true
		}
	}
	var rows []string
	for row := range set {
		rows = append(rows, row)
	}
	sort.Strings(rows)
	return rows
}

// JSON codecs are valid only when the actual entry graph passes the complete
// root value to the standard codec. Otherwise every nested field must have a
// reachable read/write in each direction and in the runtime state builder.
// Descriptor tables alone never supply field coverage.
func checkTransferStateUse(nodes []transferDeclaration, rootType string, fields []string, direction string) error {
	if len(nodes) == 0 || len(fields) == 0 {
		return fmt.Errorf("empty private state root %s/%s", rootType, direction)
	}
	seen := map[string]bool{}
	wholeValue := false
	for _, decl := range nodes {
		typed := map[string]bool{}
		if fn, ok := decl.node.(*ast.FuncDecl); ok && fn.Type.Params != nil {
			for _, param := range fn.Type.Params.List {
				typ := transferExpr(param.Type, decl.imports)
				if typ == rootType || typ == "*"+rootType || strings.HasSuffix(typ, "."+rootType) {
					for _, name := range param.Names {
						typed[name.Name] = true
					}
				}
			}
		}
		ast.Inspect(decl.node, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.SelectorExpr:
				seen[value.Sel.Name] = true
				// Copying a typed nested struct carries every member, including
				// newly added retirement receipts that need no spelling at this
				// builder. A field declaration alone supplies no such evidence.
				// origin: hot-update S7 plan §8.3 / T48 / S7-GATES typed closure
				transferCopiedFields(decl.graph.types[value], seen, map[types.Type]bool{})
			case *ast.KeyValueExpr:
				if key, ok := value.Key.(*ast.Ident); ok {
					seen[key.Name] = true
				}
			case *ast.CallExpr:
				selector, ok := value.Fun.(*ast.SelectorExpr)
				if !ok {
					break
				}
				alias, ok := selector.X.(*ast.Ident)
				codec := ok && decl.imports[alias.Name] == "encoding/json" && (direction == "encode" && selector.Sel.Name == "Marshal" || direction == "decode" && selector.Sel.Name == "Unmarshal")
				// Decoder.Decode and envelope fields use the same standard
				// whole-value contract; the object type proves the actual codec.
				if obj := transferExpressionObjectType(decl.graph, selector); obj != nil && direction == "decode" && selector.Sel.Name == "Decode" {
					if named, ok := types.Unalias(obj).(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "encoding/json" && named.Obj().Name() == "Decoder" {
						codec = true
					}
				}
				for _, arg := range value.Args {
					if codec && transferContainsType(decl.graph.types[arg], rootType, map[types.Type]bool{}) {
						wholeValue = true
					}
					if unary, ok := arg.(*ast.UnaryExpr); ok && unary.Op == token.AND {
						arg = unary.X
					}
					if ident, ok := arg.(*ast.Ident); ok && typed[ident.Name] && codec {
						wholeValue = true
					}
				}
			}
			return true
		})
	}
	if wholeValue {
		return nil
	}
	for _, row := range fields {
		key := strings.Split(row, "\t")[0]
		field := key[strings.LastIndex(key, ".")+1:]
		if !seen[field] {
			return fmt.Errorf("%s/%s: private field %s is absent from actual codec/build graph", rootType, direction, key)
		}
	}
	return nil
}

func transferExpressionObjectType(graph *transferGraph, selector *ast.SelectorExpr) types.Type {
	typ := graph.types[selector.X]
	if ptr, ok := typ.(*types.Pointer); ok {
		typ = ptr.Elem()
	}
	return typ
}

func transferContainsType(typ types.Type, name string, visited map[types.Type]bool) bool {
	if typ == nil || visited[typ] {
		return false
	}
	visited[typ] = true
	typ = types.Unalias(typ)
	if named, ok := typ.(*types.Named); ok && named.Obj().Name() == name {
		return true
	}
	switch value := typ.Underlying().(type) {
	case *types.Pointer:
		return transferContainsType(value.Elem(), name, visited)
	case *types.Slice:
		return transferContainsType(value.Elem(), name, visited)
	case *types.Struct:
		for n := 0; n < value.NumFields(); n++ {
			if transferContainsType(value.Field(n).Type(), name, visited) {
				return true
			}
		}
	}
	return false
}

func transferCopiedFields(typ types.Type, fields map[string]bool, visited map[types.Type]bool) {
	if typ == nil || visited[typ] {
		return
	}
	visited[typ] = true
	switch value := typ.Underlying().(type) {
	case *types.Pointer:
		transferCopiedFields(value.Elem(), fields, visited)
	case *types.Slice:
		transferCopiedFields(value.Elem(), fields, visited)
	case *types.Struct:
		for n := 0; n < value.NumFields(); n++ {
			field := value.Field(n)
			if field.Exported() {
				fields[field.Name()] = true
				transferCopiedFields(field.Type(), fields, visited)
			}
		}
	}
}

// origin: hot-update S7 plan §8.3 / §11.2 / T48
// gate-control: TestTransferPrivateStateInventoryControls
func TestTransferPrivateStateInventoryMatchesActualRoots(t *testing.T) {
	root := repoRoot(t)
	actual := transferPrivateRows()
	for _, row := range actual {
		t.Logf("private-field\t%s", row)
	}
	rows, err := readTransferTSV(root, transferStateInventory, 3)
	if err != nil {
		t.Fatal(err)
	}
	var frozen []string
	for _, row := range rows {
		frozen = append(frozen, strings.Join(row, "\t"))
	}
	if err := checkTransferInventory(actual, frozen); err != nil {
		t.Fatal(err)
	}
	roots, err := readTransferTSV(root, transferScannerInventory, 4)
	if err != nil {
		t.Fatal(err)
	}
	all, err := transferDeclarations(brokerGateSources(t, root))
	if err != nil {
		t.Fatal(err)
	}
	for _, owner := range []struct{ role, typ string }{{"worker", "WorkerState"}, {"tracker", "TrackerState"}, {"ledger", "LedgerRow"}} {
		fields := collectImageFields(reflect.TypeOf(map[string]any{"WorkerState": xfercontract.WorkerState{}, "TrackerState": xfercontract.TrackerState{}, "LedgerRow": xfercontract.LedgerRow{}}[owner.typ]))
		for _, direction := range []string{"build", "encode", "decode"} {
			role := "private/" + owner.role + "-" + direction
			matched := 0
			for _, row := range roots {
				if row[0] != role {
					continue
				}
				matched++
				nodes, err := reachableTransferDeclarations(all, row[2])
				if err != nil {
					t.Fatal(err)
				}
				if err := checkTransferStateUse(nodes, owner.typ, fields, direction); err != nil {
					t.Error(err)
				}
			}
			if matched != 1 {
				t.Errorf("%s: expected one actual private root, got %d", role, matched)
			}
		}
	}
}

// origin: hot-update S7 plan §8.3 / T48
func TestTransferPrivateStateInventoryControls(t *testing.T) {
	fields := []string{"xfercontract.WorkerState.Schema\t-\tstring", "xfercontract.WorkerState.Entries\t-\t[]xfercontract.WorkerEntry"}
	for _, tc := range []struct {
		source, direction string
		good              bool
	}{
		{"package codec;func encode(s WorkerState){write(s.Schema);write(s.Entries)}", "encode", true},
		{"package codec;func encode(s WorkerState){write(s.Schema)}", "encode", false},
		{"package codec;func encode(s WorkerState){write(s.Entries)}", "encode", false},
		{"package codec;func encode(s WorkerState){}", "encode", false},
		{"package codec;import j \"encoding/json\";func encode(s WorkerState){j.Marshal(s)}", "encode", true},
		{"package codec;import j \"encoding/json\";func encode(s WorkerState){j.Marshal(s.Schema)}", "encode", false},
		{"package codec;import j \"encoding/json\";func encode(s *WorkerState,b []byte){j.Unmarshal(b,s)}", "decode", true},
		{"package codec;import j \"encoding/json\";func encode(s WorkerState){j.Unmarshal(s)}", "encode", false},
		{`package codec;type Child struct{Schema string;Entries []int};type WorkerState struct{Child Child};func write(Child){};func encode(s WorkerState){write(s.Child)}`, "build", true},
		{`package codec;type Child struct{Schema string;Entries []int};type WorkerState struct{Child Child};func encode(s WorkerState){}`, "build", false},
		{`package codec;import j "encoding/json";type WorkerState struct{Schema string;Entries []int};func encode(b []byte){var state WorkerState;j.Unmarshal(b,&state)}`, "decode", true},
		{`package codec;import j "encoding/json";type WorkerState struct{Schema string;Entries []int};func encode(b []byte){var state WorkerState;j.Unmarshal(b,&state.Schema)}`, "decode", false},
		{`package codec;import j "encoding/json";type WorkerState struct{Schema string;Entries []int};type Envelope struct{State WorkerState};func encode(s Envelope){j.Marshal(s)}`, "encode", true},
		{`package codec;import j "encoding/json";type WorkerState struct{Schema string;Entries []int};func encode(d *j.Decoder){var state WorkerState;d.Decode(&state)}`, "decode", true},
		{`package codec;type WorkerState struct{Schema string;Entries []int};type Decoder struct{};func(*Decoder)Decode(any){};func encode(d *Decoder){var state WorkerState;d.Decode(&state)}`, "decode", false},
	} {
		root := transferFixtureTree(t, map[string]string{"internal/codec/state.go": tc.source})
		all, err := transferDeclarations(brokerGateSources(t, root))
		if err != nil {
			t.Fatal(err)
		}
		nodes, err := reachableTransferDeclarations(all, "internal/codec/state.go:encode")
		if err != nil {
			t.Fatal(err)
		}
		if err := checkTransferStateUse(nodes, "WorkerState", fields, tc.direction); (err == nil) != tc.good {
			t.Fatalf("private root control (%s): %v", tc.source, err)
		}
	}
	if len(transferPrivateRows()) < 150 {
		t.Fatal("private reachable closure lost nested fields")
	}
}

var transferScannerRoles = []string{
	"contract/headers", "contract/register-sign", "contract/sub-cold", "contract/sub-hot",
	"contract/acl-broker", "contract/acl-ctl", "contract/acl-agent", "contract/subjects",
	"private/worker-build", "private/worker-encode", "private/worker-decode",
	"private/tracker-build", "private/tracker-encode", "private/tracker-decode",
	"private/ledger-build", "private/ledger-encode", "private/ledger-decode",
	"migration/watchdog", "migration/ledger-disposition", "migration/budget",
	"migration/errors", "migration/ctx", "migration/transfer-busy", "migration/publisher",
	// origin: hot-update S7 plan §11.4 / T50; verified INTEG-M and FIX-REAP moves.
	"migration/object-writer-validateobjectspec",
	"migration/object-writer-checkwriterstream",
	"migration/object-writer-checktransferretention",
	"migration/object-writer-chunksubject",
	"migration/object-writer-chunklength",
	"migration/object-writer-pendingreceipt",
	"migration/object-writer-chunkmessage",
	"migration/object-writer-checkchunk",
	"migration/object-reader-objectchunktoken",
	"migration/object-receipt-transferpayloadlimit",
	"migration/object-receipt-transferchunksize",
	"migration/object-receipt-checkwriterpayload",
	"migration/object-receipt-metadatasubject",
	"migration/object-receipt-objectmetadata",
	"migration/object-receipt-metadatareceipt",
	"migration/object-receipt-metadatamessage",
	"migration/object-receipt-cloneobjectreceipt",
	"migration/object-receipt-clonewriterstate",
	"migration/object-receipt-validatewriterstate",
	"migration/object-receipt-finalize",
	"migration/object-receipt-exactmetadata",
	"migration/path-validatomicgrant",
	"migration/path-validateatomicdecision",
	"migration/path-atomicversiontoken",
	"migration/transfer-atomic-import-linux-importatomic",
	"migration/transfer-reconcile-broker-reapbucketobjects",
	"migration/transfer-reconcile-reaporphanchunks",
	"migration/transfer-reconcile-broker-xferbuckethasagedobjects",
	"migration/xfer-inflight-broker-commitsynthetictransferterminal",
	"migration/transfer-xferreservefor",
	"migration/transfer-xfermaxbytesforceiling",
	"migration/transfer-xferpayloadlimit",
	"migration/transfer-broker-xferbucketmaxbytes",
	"migration/transfer-objectstoreconfigfromstream",
	"migration/transfer-xfercrosshomeextrafor",
	"migration/handler-loadworkerhandler",
	"migration/registry-controlvalid",
	"migration/owner-staterefs",
	"migration/registry-clonestate",
	"migration/legacy-uptime",
	"migration/legacy-reap-floor",
	"migration/legacy-aged-gauge",
	"migration/source-unix",
	"migration/source-other",
}

// A relocation row records the original site, actual destination, and the
// test/scanner that still guards it. Exact source resolution rejects dead paths
// even if the file itself still exists after a slim move.
func checkTransferScannerRoots(sources map[string]string, tests map[string]string, rows [][]string) error {
	all, err := transferDeclarations(sources)
	if err != nil {
		return err
	}
	roles := map[string]bool{}
	for _, row := range rows {
		if len(row) != 4 || roles[row[0]] || !slices.Contains(transferScannerRoles, row[0]) {
			return fmt.Errorf("duplicate/unknown scanner role %v", row)
		}
		roles[row[0]] = true
		if _, symbol, ok := strings.Cut(row[1], ":"); !ok || symbol == "" {
			return fmt.Errorf("missing old-file:symbol migration evidence %v", row)
		}
		nodes, err := reachableTransferDeclarations(all, row[2])
		if err != nil {
			return err
		}
		if len(transferRootTokens(nodes)) == 0 {
			return fmt.Errorf("%s: empty actual scanner root", row[0])
		}
		path, name, ok := strings.Cut(row[3], ":")
		if !ok || !strings.HasSuffix(path, "_test.go") || !strings.HasPrefix(name, "Test") {
			return fmt.Errorf("invalid test/scanner anchor %s", row[3])
		}
		file, err := brokerParse(path, tests[path])
		if err != nil {
			return fmt.Errorf("dead scanner path %s: %w", path, err)
		}
		found := false
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name && fn.Body != nil && len(fn.Body.List) > 0 {
				found = true
			}
		}
		if !found {
			return fmt.Errorf("dead scanner/test symbol %s", row[3])
		}
	}
	for _, role := range transferScannerRoles {
		if !roles[role] {
			return fmt.Errorf("missing scanner role %s", role)
		}
	}
	return nil
}

// origin: hot-update S7 plan §11.4 / T50
// gate-control: TestTransferScannerRootControls
func TestTransferErrorAndScannerRootsAreComplete(t *testing.T) {
	root := repoRoot(t)
	rows, err := readTransferTSV(root, transferScannerInventory, 4)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]string{}
	for _, row := range rows {
		path, _, _ := strings.Cut(row[3], ":")
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		tests[path] = string(raw)
	}
	sources := brokerGateSources(t, root)
	for _, pkg := range append(slices.Clone(transferLeafRoots), "jsstream", "transferpath", "filetransfer") {
		n := 0
		for path := range sources {
			if filepath.ToSlash(filepath.Dir(path)) == "internal/"+pkg {
				n++
			}
		}
		if n == 0 {
			t.Errorf("empty transfer scanner package internal/%s", pkg)
		}
	}
	if err := checkTransferScannerRoots(sources, tests, rows); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §11.4 / T50
func TestTransferScannerRootControls(t *testing.T) {
	root := transferFixtureTree(t, map[string]string{
		"internal/new/owner.go": "package owner;func Snapshot(){codec.Field(\"actual\")}",
		"internal/old/owner.go": "package owner;func Other(){}",
	})
	tests := map[string]string{"test/architecture/guard_test.go": "package architecture;func TestGuard(){assert()}"}
	var rows [][]string
	for _, role := range transferScannerRoles {
		rows = append(rows, []string{role, "internal/old/owner.go:Snapshot", "internal/new/owner.go:Snapshot", "test/architecture/guard_test.go:TestGuard"})
	}
	check := func(rows [][]string) error { return checkTransferScannerRoots(brokerGateSources(t, root), tests, rows) }
	if err := check(rows); err != nil {
		t.Fatal(err)
	}
	for i := range rows {
		mutant := make([][]string, len(rows))
		for j := range rows {
			mutant[j] = slices.Clone(rows[j])
		}
		mutant[i][2] = "internal/old/owner.go:Snapshot"
		if check(mutant) == nil || check(append(slices.Clone(rows[:i]), rows[i+1:]...)) == nil {
			t.Fatalf("stale/missing scanner role escaped %s", rows[i][0])
		}
	}
	transferWriteFixture(t, root, "internal/new/owner.go", "package owner;func Snapshot(){}")
	if check(rows) == nil {
		t.Fatal("on-disk empty root escaped")
	}
	transferWriteFixture(t, root, "internal/new/owner.go", "package owner;func Snapshot(){codec.Field(\"actual\")}")
	tests["test/architecture/guard_test.go"] = "package architecture;func TestMoved(){assert()}"
	if check(rows) == nil {
		t.Fatal("dead test/scanner symbol escaped")
	}
}

// The native receipt shape is shared by tests and deploy arms. Status text is
// insufficient: every claimed completion needs candidate identities and a raw
// artifact whose digest matches the recorded oracle input.
type transferNativeReceipt struct {
	SourceSHA      string  `json:"source_sha"`
	DirtyDigest    string  `json:"dirty_digest"`
	ArtifactSHA    string  `json:"artifact_sha"`
	RunningSHA     string  `json:"running_sha"`
	TestOrArm      string  `json:"test_or_arm"`
	Command        string  `json:"command"`
	NativeRC       *int    `json:"native_rc"`
	MutationID     string  `json:"mutation_id"`
	RawPath        string  `json:"raw_path"`
	OracleInputSHA string  `json:"oracle_input_sha"`
	ReaderDigest   string  `json:"reader_digest"`
	WallSeconds    float64 `json:"wall_s"`
}

func transferHex(value string, lengths ...int) bool {
	if !slices.Contains(lengths, len(value)) {
		return false
	}
	for _, c := range value {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func checkTransferNativeReceipt(root string, raw []byte, claim string) error {
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return fmt.Errorf("%s: native receipt must be an object", claim)
	}
	keys := map[string]bool{}
	for decoder.More() {
		key, err := decoder.Token()
		name, ok := key.(string)
		if err != nil || !ok || keys[name] {
			return fmt.Errorf("%s: duplicate/invalid native receipt field", claim)
		}
		keys[name] = true
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return err
		}
	}
	for _, key := range []string{"source_sha", "dirty_digest", "artifact_sha", "running_sha", "test_or_arm", "command", "native_rc", "mutation_id", "raw_path", "oracle_input_sha", "reader_digest", "wall_s"} {
		if !keys[key] {
			return fmt.Errorf("%s: missing exact native receipt field %s", claim, key)
		}
	}
	var receipt transferNativeReceipt
	if err := json.Unmarshal(raw, &receipt); err != nil {
		return err
	}
	if !transferHex(receipt.SourceSHA, 40) || !transferHex(receipt.DirtyDigest, 64) || !transferHex(receipt.ArtifactSHA, 64) || receipt.RunningSHA != receipt.ArtifactSHA || !transferHex(receipt.ReaderDigest, 64) || !transferHex(receipt.OracleInputSHA, 64) {
		return fmt.Errorf("%s: missing/mismatched candidate receipt identities", claim)
	}
	if receipt.TestOrArm != claim || receipt.Command == "" || receipt.MutationID == "" || receipt.NativeRC == nil || *receipt.NativeRC != 0 || receipt.WallSeconds <= 0 {
		return fmt.Errorf("%s: invalid native receipt outcome/shape", claim)
	}
	path := filepath.Clean(receipt.RawPath)
	if filepath.IsAbs(path) || path == "." || strings.HasPrefix(path, "../") {
		return fmt.Errorf("%s: receipt raw path leaves candidate", claim)
	}
	rawArtifact, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || len(rawArtifact) == 0 || transferDigest(rawArtifact) != receipt.OracleInputSHA {
		return fmt.Errorf("%s: raw oracle artifact missing or digest mismatch", claim)
	}
	return nil
}

func transferDigest(raw []byte) string { return fmt.Sprintf("%x", sha256.Sum256(raw)) }

// Claim rows: document, literal claim marker, receipt JSON path (or NOT_RUN).
// Every checked S7 tasklist item must be listed; a broad "S7 done" checkbox
// cannot inherit a receipt for one child arm.
func checkTransferDocumentation(root string, rows [][]string) error {
	if len(rows) == 0 {
		return fmt.Errorf("empty S7 documentation claims")
	}
	documents := map[string]string{}
	markers := map[string]bool{}
	for _, row := range rows {
		if len(row) != 3 || markers[row[0]+":"+row[1]] {
			return fmt.Errorf("malformed/duplicate documentation claim")
		}
		markers[row[0]+":"+row[1]] = true
		raw, err := os.ReadFile(filepath.Join(root, row[0]))
		if err != nil {
			return fmt.Errorf("dead documentation path %s", row[0])
		}
		documents[row[0]] = string(raw)
		if strings.Count(string(raw), row[1]) != 1 {
			return fmt.Errorf("missing/ambiguous claim %s", row[1])
		}
		checked := strings.Contains(row[1], "[x]") || strings.Contains(row[1], "[X]") || strings.Contains(row[1], "COMPLETE")
		if row[2] == "NOT_RUN" {
			if checked {
				return fmt.Errorf("checked S7 claim lacks receipt %s", row[1])
			}
			continue
		}
		receipt, err := os.ReadFile(filepath.Join(root, row[2]))
		if err != nil {
			return fmt.Errorf("missing claim receipt %s", row[2])
		}
		if err := checkTransferNativeReceipt(root, receipt, row[1]); err != nil {
			return err
		}
	}
	for path, document := range documents {
		claims := document
		scoped := strings.Contains(path, "transfer")
		if strings.Contains(document, "<!-- s7-claims:") {
			start, end := "<!-- s7-claims:start -->", "<!-- s7-claims:end -->"
			if strings.Count(document, start) != 1 || strings.Count(document, end) != 1 || strings.Index(document, start) >= strings.Index(document, end) {
				return fmt.Errorf("%s: missing/duplicate S7 claim boundaries", path)
			}
			claims = strings.Split(strings.Split(document, start)[1], end)[0]
			scoped = true
		}
		for _, line := range strings.Split(claims, "\n") {
			transferClaim := scoped || strings.Contains(strings.ToLower(line), "transfer") || strings.Contains(line, "S7")
			if transferClaim && (strings.Contains(line, "[x]") || strings.Contains(line, "[X]")) && !markers[path+":"+strings.TrimSpace(line)] {
				return fmt.Errorf("unregistered checked S7 claim in %s", path)
			}
		}
		// Relative Markdown links are executable source/document promises.
		// Validate them as data; this gate starts no shell, binary or service.
		for _, link := range regexp.MustCompile(`\]\(([^)#]+)(?:#[^)]*)?\)`).FindAllStringSubmatch(document, -1) {
			if strings.Contains(link[1], "://") || strings.HasPrefix(link[1], "#") {
				continue
			}
			target := filepath.Join(root, filepath.Dir(path), link[1])
			if _, err := os.Stat(target); err != nil {
				return fmt.Errorf("%s: dead documentation/source link %s", path, link[1])
			}
		}
		for _, obsolete := range []string{"transfer --resume", "SDK Put retry resumes", "transfer does not survive hot update"} {
			if strings.Contains(document, obsolete) {
				return fmt.Errorf("obsolete S7 command/contract in %s: %s", path, obsolete)
			}
		}
	}
	return nil
}

// origin: hot-update S7 plan §10.1 / §12.2 / T54
// gate-control: TestTransferDocumentationReceiptControls
func TestTransferDocumentationClaimsHaveReceipts(t *testing.T) {
	root := repoRoot(t)
	rows, err := readTransferTSV(root, transferDocumentationClaims, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := checkTransferDocumentation(root, rows); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §10.1 / T54
func TestTransferDocumentationReceiptControls(t *testing.T) {
	root := transferFixtureTree(t, map[string]string{})
	path := "docs/transfer-tasklist.md"
	transferWriteFixture(t, root, path, "- [ ] transfer continuity\n")
	rows := [][]string{{path, "- [ ] transfer continuity", "NOT_RUN"}}
	if err := checkTransferDocumentation(root, rows); err != nil {
		t.Fatal(err)
	}
	// Older signed stages stay outside S7's receipt scope. Missing, duplicate
	// or reversed claim boundaries must still be red, as must a checked child
	// introduced inside the scoped region without its own exact receipt.
	// origin: hot-update S7 plan §10.1 / T54 / S7-GATES lead decision 3
	scoped := "- [x] S6 transfer subitem\n<!-- s7-claims:start -->\n- [ ] transfer continuity\n<!-- s7-claims:end -->\n"
	transferWriteFixture(t, root, path, scoped)
	if err := checkTransferDocumentation(root, rows); err != nil {
		t.Fatal(err)
	}
	for _, document := range []string{
		strings.Replace(scoped, "<!-- s7-claims:end -->", "", 1),
		strings.Replace(scoped, "<!-- s7-claims:start -->", "<!-- s7-claims:end -->", 1),
		scoped + "<!-- s7-claims:start -->\n",
		strings.Replace(scoped, "<!-- s7-claims:end -->", "- [x] native Darwin\n<!-- s7-claims:end -->", 1),
	} {
		transferWriteFixture(t, root, path, document)
		if checkTransferDocumentation(root, rows) == nil {
			t.Fatal("on-disk S7 claim boundary/child mutation escaped")
		}
	}
	for _, mutation := range []struct{ document, marker string }{
		{"- [x] transfer continuity\n", "- [x] transfer continuity"},
		{"- [ ] transfer continuity\n- [x] native Darwin\n", "- [ ] transfer continuity"},
		{"- [ ] transfer continuity\ntransfer --resume\n", "- [ ] transfer continuity"},
	} {
		transferWriteFixture(t, root, path, mutation.document)
		if checkTransferDocumentation(root, [][]string{{path, mutation.marker, "NOT_RUN"}}) == nil {
			t.Fatal("on-disk unchecked-receipt/obsolete-command mutation escaped")
		}
	}
	transferWriteFixture(t, root, "raw/oracle.txt", "actual raw oracle\n")
	rc := 0
	receipt := transferNativeReceipt{strings.Repeat("a", 40), strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("c", 64), "- [x] transfer continuity", "go test -race ./test/hotupdate", &rc, "none", "raw/oracle.txt", transferDigest([]byte("actual raw oracle\n")), strings.Repeat("d", 64), 1}
	transferWriteFixture(t, root, path, receipt.TestOrArm+"\n")
	writeReceipt := func(value transferNativeReceipt) {
		bytes, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		transferWriteFixture(t, root, "raw/receipt.json", string(bytes))
	}
	rows = [][]string{{path, receipt.TestOrArm, "raw/receipt.json"}}
	writeReceipt(receipt)
	if err := checkTransferDocumentation(root, rows); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*transferNativeReceipt){
		func(r *transferNativeReceipt) { r.NativeRC = nil },
		func(r *transferNativeReceipt) { bad := 4; r.NativeRC = &bad },
		func(r *transferNativeReceipt) { r.RunningSHA = strings.Repeat("e", 64) },
		func(r *transferNativeReceipt) { r.RawPath = "raw/missing" },
		func(r *transferNativeReceipt) { r.RawPath = "../outside" },
		func(r *transferNativeReceipt) { r.OracleInputSHA = strings.Repeat("f", 64) },
		func(r *transferNativeReceipt) { r.ReaderDigest = "" },
		func(r *transferNativeReceipt) { r.WallSeconds = 0 },
	} {
		bad := receipt
		mutate(&bad)
		writeReceipt(bad)
		if checkTransferDocumentation(root, rows) == nil {
			t.Fatal("on-disk native receipt mutation escaped")
		}
	}
	writeReceipt(receipt)
	raw, err := os.ReadFile(filepath.Join(root, "raw/receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	transferWriteFixture(t, root, "raw/receipt.json", strings.Replace(string(raw), `"native_rc":0`, `"native_rc":4,"native_rc":0`, 1))
	if checkTransferDocumentation(root, rows) == nil {
		t.Fatal("duplicate native_rc hid an incomplete result")
	}
	transferWriteFixture(t, root, path, receipt.TestOrArm+"\n[source](missing.go)\n")
	writeReceipt(receipt)
	if checkTransferDocumentation(root, rows) == nil {
		t.Fatal("dead source link escaped documentation audit")
	}
	if checkTransferDocumentation(root, nil) == nil || checkTransferDocumentation(root, [][]string{{"docs/dead.md", "claim", "NOT_RUN"}}) == nil {
		t.Fatal("empty/dead documentation root escaped")
	}
}
