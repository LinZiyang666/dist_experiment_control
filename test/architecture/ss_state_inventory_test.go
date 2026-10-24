package architecture

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	_ "unsafe"

	"github.com/LinZiyang666/tether/internal/sealedstate"
)

// A schema table unused by either codec direction proves nothing. Follow local
// calls from the actual entry points and inventory the reachable descriptor
// accesses, snapshot field IDs, and restore field IDs separately.
// The source gate and real binary round trip are complementary: a correct table
// alone cannot prove byte continuity, and a symmetric codec alone cannot prove
// that a private runtime snapshot included every required decision.
// origin: hot-update S5 plan §10.1 / §12.1 / T49.
// gate-control: TestSSStateInventoryControls
func TestSSStateInventoryMatchesPrivateRoots(t *testing.T) {
	t.Run("descriptor-ledger", func(t *testing.T) { assertBinaryInventory(t, "testdata/ss_state_inventory.txt", ssStateInventory()) })
	roots := repoRoot(t)
	codec := readFlowSSSources(t, roots, "internal/ssstate", false)
	for _, name := range []string{"encodeState", "decodeState"} {
		t.Run(name, func(t *testing.T) {
			nodes, err := reachableFlowSSFunctions(codec, name)
			if err != nil {
				t.Fatal(err)
			}
			if err := checkPrivateCodecDescriptorUse(nodes); err != nil {
				t.Fatalf("pending P9: %s: %v", name, err)
			}
		})
	}
	for _, owner := range []struct {
		pkg   string
		kinds []string
	}{
		{"internal/flow", []string{"flow", "retirement"}},
		{"internal/tunnel", []string{"endpoint", "permit", "receipt"}},
		{"internal/agent/ssproxy", []string{"ss-server", "ss-actor", "material", "salt-ring", "authority", "binding", "timer"}},
	} {
		sources := readFlowSSSources(t, roots, owner.pkg, false)
		// Follow the moved shared control builder while retaining the runtime's
		// own record-copy roots. Merging the codec's separate clone/wrap helpers
		// would let their same-named functions mask a broken actor record copy.
		// origin: hot-update S5 plan §12.1 / T49
		if owner.pkg == "internal/agent/ssproxy" {
			sources["ssstate/proxy_control.go"] = codec["proxy_control.go"]
		}
		for _, root := range []string{"SnapshotState", "restoreCutState"} {
			t.Run(owner.pkg+"/"+root, func(t *testing.T) {
				nodes, err := reachableFlowSSFunctions(sources, root)
				if err != nil {
					t.Fatal(err)
				}
				if err := checkPrivateRootFields(nodes, owner.kinds, sealedstate.RecordDescriptors); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func ssStateInventory() []string {
	var rows []string
	for _, record := range sealedstate.RecordDescriptors {
		for _, field := range record.Fields {
			rows = append(rows, fmt.Sprintf("%s.%s\t%d\t%d\t%d\t%t", record.Kind, field.Name, field.Type, field.Width, field.MaxBytes, field.Required))
		}
	}
	sort.Strings(rows)
	return rows
}

// Read every production platform twin. Ignoring the inactive OS would let an
// unsupported implementation acquire a second unsafe schema unnoticed.
func readFlowSSSources(t *testing.T, root, pkg string, tests bool) map[string]*ast.File {
	t.Helper()
	result := map[string]*ast.File{}
	entries, err := os.ReadDir(filepath.Join(root, pkg))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") != tests {
			continue
		}
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, pkg, entry.Name()), nil, parser.ParseComments)
		if err != nil {
			t.Fatal(err)
		}
		result[entry.Name()] = file
	}
	if len(result) == 0 {
		t.Fatalf("no source files in %s", pkg)
	}
	return result
}

// A name maps to every platform/receiver implementation with that name. This
// conservatively follows local calls and selectors, so split helpers cannot
// evade the entry-point scan by moving descriptor access into another file.
func reachableFlowSSFunctions(files map[string]*ast.File, root string) ([]ast.Node, error) {
	functions := map[string][]*ast.FuncDecl{}
	for _, file := range files {
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				functions[fn.Name.Name] = append(functions[fn.Name.Name], fn)
			}
		}
	}
	if len(functions[root]) == 0 {
		return nil, fmt.Errorf("missing actual private root %s", root)
	}
	var nodes []ast.Node
	seen := map[string]bool{}
	var visit func(string)
	visit = func(name string) {
		if seen[name] {
			return
		}
		seen[name] = true
		for _, fn := range functions[name] {
			nodes = append(nodes, fn)
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch target := call.Fun.(type) {
				case *ast.Ident:
					visit(target.Name)
				case *ast.SelectorExpr:
					visit(target.Sel.Name)
				}
				return true
			})
		}
	}
	visit(root)
	return nodes, nil
}

func privateRootTokens(nodes []ast.Node) (map[string]bool, map[string]bool) {
	names, selectors := map[string]bool{}, map[string]bool{}
	functions := map[string]*ast.FuncDecl{}
	for _, node := range nodes {
		if fn, ok := node.(*ast.FuncDecl); ok {
			functions[fn.Name.Name] = fn
		}
	}
	for _, node := range nodes {
		ast.Inspect(node, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.BasicLit:
				if x.Kind == token.STRING {
					if value, err := strconv.Unquote(x.Value); err == nil {
						names[value] = true
					}
				}
			case *ast.SelectorExpr:
				selectors[x.Sel.Name] = true
			case *ast.CallExpr:
				// Fingerprint helpers form field IDs from a literal caller prefix.
				// Resolve only arguments of a reachable local function: combining
				// unrelated string tokens would certify IDs no runtime ever uses.
				// origin: hot-update S5 plan §5.1 / §12.1 / T49.
				target, ok := x.Fun.(*ast.Ident)
				if !ok || functions[target.Name] == nil {
					break
				}
				fn := functions[target.Name]
				arguments := map[string]string{}
				index := 0
				for _, parameter := range fn.Type.Params.List {
					for _, name := range parameter.Names {
						if index < len(x.Args) {
							if value, ok := privateRootString(x.Args[index], nil); ok {
								arguments[name.Name] = value
							}
						}
						index++
					}
				}
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					if expression, ok := node.(*ast.BinaryExpr); ok {
						if value, ok := privateRootString(expression, arguments); ok {
							names[value] = true
						}
					}
					return true
				})
			}
			return true
		})
	}
	return names, selectors
}

func privateRootString(expression ast.Expr, arguments map[string]string) (string, bool) {
	switch value := expression.(type) {
	case *ast.BasicLit:
		if value.Kind == token.STRING {
			text, err := strconv.Unquote(value.Value)
			return text, err == nil
		}
	case *ast.Ident:
		text, ok := arguments[value.Name]
		return text, ok
	case *ast.BinaryExpr:
		if value.Op == token.ADD {
			left, leftOK := privateRootString(value.X, arguments)
			right, rightOK := privateRootString(value.Y, arguments)
			return left + right, leftOK && rightOK
		}
	}
	return "", false
}

func checkPrivateCodecDescriptorUse(nodes []ast.Node) error {
	_, selectors := privateRootTokens(nodes)
	// Type and width must constrain the actual byte operations, not just a
	// descriptor mention in a comment or a private test-only schema table.
	for _, member := range []string{"RecordDescriptors", "Name", "Type", "Width", "MaxBytes", "Required"} {
		if !selectors[member] {
			return fmt.Errorf("actual codec does not consume descriptor %s", member)
		}
	}
	return nil
}

func checkPrivateRootFields(nodes []ast.Node, kinds []string, descriptors []sealedstate.RecordDescriptor) error {
	names, _ := privateRootTokens(nodes)
	actorRecord := privateRootCopiesActorRecord(nodes)
	var missing []string
	for _, record := range descriptors {
		if !slices.Contains(kinds, record.Kind) {
			continue
		}
		for _, field := range record.Fields {
			if names[field.Name] || (record.Kind == sealedstate.KindSSActor && actorRecord) {
				continue
			}
			missing = append(missing, record.Kind+"."+field.Name)
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		return fmt.Errorf("private snapshot/restore omits required field IDs: %s", strings.Join(missing, ", "))
	}
	return nil
}

// Actor cursors live in the descriptor-owned record itself, rather than an
// export struct rebuilt with literal field names. Prove the complete copy and
// the attachment to the live actor; seeing RecordDescriptors alone is not
// evidence that a nonce or buffer survives. Keep this deliberately strict:
// a filter/branch in either loop requires a fresh review of the transport.
// origin: hot-update S5 plan §6.1 / §12.1 / T49.
func privateRootCopiesActorRecord(nodes []ast.Node) bool {
	text := func(node ast.Node) string {
		var out bytes.Buffer
		if err := printer.Fprint(&out, token.NewFileSet(), node); err != nil {
			return ""
		}
		return strings.Join(strings.Fields(out.String()), " ")
	}
	clone, wrap, snapshot, restore := false, false, false, false
	for _, node := range nodes {
		fn, ok := node.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Name.Name == "clone" {
			clone = text(fn.Body) == "{ r := sealedstate.Record{Kind: p.record.Kind, Fields: make([]sealedstate.Field, len(p.record.Fields))} for i, f := range p.record.Fields { r.Fields[i] = f r.Fields[i].Bytes = append([]byte(nil), f.Bytes...) } return r }"
		}
		if fn.Name.Name == "wrapPrivateRecord" {
			wrap = text(fn.Body) == "{ p := privateRecord{record: r, fields: make(map[string]*sealedstate.Field)} for i := range p.record.Fields { f := &p.record.Fields[i] p.fields[f.Name] = f } return p }"
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			switch fn.Name.Name {
			case "SnapshotState":
				if loop, ok := node.(*ast.RangeStmt); ok {
					actor, copied := false, false
					for _, stmt := range loop.Body.List {
						actor = actor || text(stmt) == "p := a.state"
						copied = copied || text(stmt) == "out.Records = append(out.Records, p.clone())"
					}
					snapshot = snapshot || actor && copied
				}
			case "restoreActorRecord":
				if literal, ok := node.(*ast.CompositeLit); ok && text(literal.Type) == "ssActor" {
					for _, element := range literal.Elts {
						if field, ok := element.(*ast.KeyValueExpr); ok && text(field) == "state: p" {
							restore = true
						}
					}
				}
			}
			return true
		})
	}
	return clone && wrap && (snapshot || restore)
}

// Each vulnerable field is independently removed from a reachable runtime
// fixture. Dropping a descriptor itself is independently caught by the ledger.
// origin: hot-update S5 plan §10.1 / T49.
func TestSSStateInventoryControls(t *testing.T) {
	critical := map[string]struct {
		typ            uint8
		width, maximum int
	}{
		"ss-actor.rx_nonce": {7, 12, 0}, "ss-actor.tx_nonce": {7, 12, 0},
		"flow.tx_pending": {4, 0, 32768}, "flow.rx_pending": {4, 0, 32768},
		"salt-ring.salts": {4, 0, 8192 * 32}, "salt-ring.head": {1, 8, 0}, "salt-ring.count": {1, 8, 0},
		"timer.fired": {3, 1, 0}, "timer.cleanup_pending": {3, 1, 0}, "ss-actor.connect_so_error_consumed": {3, 1, 0},
		// I1 reconnect state must stay in the descriptor-driven sealed codec;
		// otherwise an imported endpoint loses its original route or retry.
		// origin: hot-update S5 plan §4.4 / §5.1 / §12.1 / T49.
		"endpoint.register_token":          {5, 0, 4096},
		"endpoint.default_broker_addr":     {5, 0, 4096},
		"endpoint.broker_addr":             {5, 0, 4096},
		"endpoint.pin_current":             {5, 0, 4096},
		"endpoint.pin_previous":            {5, 0, 4096},
		"endpoint.pin_valid_until_unix_ns": {1, 8, 0},
		"endpoint.pending_resume":          {4, 0, 32768},
	}
	for _, record := range sealedstate.RecordDescriptors {
		for _, field := range record.Fields {
			if want, ok := critical[record.Kind+"."+field.Name]; ok {
				if field.Type != want.typ || field.Width != want.width || field.MaxBytes != want.maximum || !field.Required {
					t.Fatalf("critical descriptor %s.%s type/width/bound/required drifted", record.Kind, field.Name)
				}
			}
		}
	}

	rows := ssStateInventory()
	// The original 257 fields plus I1's seven declared endpoint fields. The
	// append-only ledger and actual codec round trip still check every row.
	if len(rows) != 264 {
		t.Fatalf("frozen private fields=%d, want 264", len(rows))
	}
	for _, field := range []string{"rx_nonce", "tx_nonce", "tx_pending", "rx_pending", "salts", "head", "count", "fired", "cleanup_pending", "connect_so_error_consumed", "register_token", "default_broker_addr", "broker_addr", "pin_current", "pin_previous", "pin_valid_until_unix_ns", "pending_resume"} {
		found := false
		for i, row := range rows {
			if strings.Contains(row, "."+field+"\t") {
				found = true
				t.Run(row, func(t *testing.T) {
					if compareInventoryRows(rows, append(slices.Clone(rows[:i]), rows[i+1:]...)) == nil {
						t.Fatal("private field omission accepted")
					}
				})
			}
		}
		if !found {
			t.Fatalf("missing critical frozen field %s", field)
		}
	}
	descriptor := []sealedstate.RecordDescriptor{{Kind: "ss-actor", Fields: []sealedstate.FieldDescriptor{{Name: "rx_nonce"}, {Name: "tx_pending"}, {Name: "salts"}, {Name: "fired"}}}}
	const source = `package fixture
 func SnapshotState(){build()}
 func build(){ _="rx_nonce"; _="tx_pending"; _="salts"; _="fired" }
 func encodeState(){ codec() }
 func codec(){ _=sealedstate.RecordDescriptors; _=d.Name; _=d.Type; _=d.Width; _=d.MaxBytes; _=d.Required }
 `
	check := func(src string, codec bool) error {
		file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", src, 0)
		if err != nil {
			return err
		}
		root := "SnapshotState"
		if codec {
			root = "encodeState"
		}
		nodes, err := reachableFlowSSFunctions(map[string]*ast.File{"fixture.go": file}, root)
		if err != nil {
			return err
		}
		if codec {
			return checkPrivateCodecDescriptorUse(nodes)
		}
		return checkPrivateRootFields(nodes, []string{"ss-actor"}, descriptor)
	}
	for _, codec := range []bool{false, true} {
		if err := check(source, codec); err != nil {
			t.Fatal(err)
		}
	}
	for _, field := range descriptor[0].Fields {
		t.Run("runtime-"+field.Name, func(t *testing.T) {
			if check(strings.Replace(source, `"`+field.Name+`"`, `"omitted"`, 1), false) == nil {
				t.Fatal("runtime omitted private field accepted")
			}
		})
	}
	for _, member := range []string{"RecordDescriptors", "Name", "Type", "Width", "MaxBytes", "Required"} {
		t.Run("codec-"+member, func(t *testing.T) {
			if check(strings.ReplaceAll(source, "."+member, ".Unused"), true) == nil {
				t.Fatal("unused descriptor accepted")
			}
		})
	}
	if err := check(strings.Replace(source, "build()", "unrelated()", 1), false); err == nil {
		t.Fatal("unreachable snapshot helper accepted")
	}
	const fingerprintSource = `package fixture
func SnapshotState() { fingerprint("listener") }
func fingerprint(prefix string) { p.put(prefix+"_dev", 1); p.put(prefix+"_ino", 2) }
`
	fingerprintCheck := func(source string) error {
		file, err := parser.ParseFile(token.NewFileSet(), "fingerprint.go", source, 0)
		if err != nil {
			return err
		}
		nodes, err := reachableFlowSSFunctions(map[string]*ast.File{"fingerprint.go": file}, "SnapshotState")
		if err != nil {
			return err
		}
		return checkPrivateRootFields(nodes, []string{"ss-server"}, []sealedstate.RecordDescriptor{{Kind: "ss-server", Fields: []sealedstate.FieldDescriptor{{Name: "listener_dev"}, {Name: "listener_ino"}}}})
	}
	if err := fingerprintCheck(fingerprintSource); err != nil {
		t.Fatal(err)
	}
	for _, omission := range []string{`"listener"`, `"_dev"`, `"_ino"`} {
		if fingerprintCheck(strings.Replace(fingerprintSource, omission, `"omitted"`, 1)) == nil {
			t.Fatalf("dynamic fingerprint field omission accepted: %s", omission)
		}
	}

	const actorSource = `package fixture
func SnapshotState() { wrapPrivateRecord(record); for _, id := range ids { a := actors[id]; p := a.state; out.Records = append(out.Records, p.clone()) }; binding.clone() }
func restoreCutState() { wrapPrivateRecord(record); record.clone(); restoreActorRecord(record) }
func restoreActorRecord(p privateRecord) { a := &ssActor{state: p}; _ = a }
func (p privateRecord) clone() sealedstate.Record { r := sealedstate.Record{Kind: p.record.Kind, Fields: make([]sealedstate.Field, len(p.record.Fields))}; for i, f := range p.record.Fields { r.Fields[i] = f; r.Fields[i].Bytes = append([]byte(nil), f.Bytes...) }; return r }
func wrapPrivateRecord(r sealedstate.Record) privateRecord { p := privateRecord{record: r, fields: make(map[string]*sealedstate.Field)}; for i := range p.record.Fields { f := &p.record.Fields[i]; p.fields[f.Name] = f }; return p }
`
	actorCheck := func(source, root string) bool {
		file, err := parser.ParseFile(token.NewFileSet(), "actor.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		nodes, err := reachableFlowSSFunctions(map[string]*ast.File{"actor.go": file}, root)
		if err != nil {
			t.Fatal(err)
		}
		return privateRootCopiesActorRecord(nodes)
	}
	for _, root := range []string{"SnapshotState", "restoreCutState"} {
		if !actorCheck(actorSource, root) {
			t.Fatalf("complete actor record fixture rejected at %s", root)
		}
		// Check the real owner as well: synthetic shape controls cannot certify
		// that the runtime still uses the complete-copy path they describe.
		nodes, err := reachableFlowSSFunctions(readFlowSSSources(t, repoRoot(t), "internal/agent/ssproxy", false), root)
		if err != nil || !privateRootCopiesActorRecord(nodes) {
			t.Fatalf("actual actor record is not completely transported at %s: %v", root, err)
		}
		fingerprint := []sealedstate.RecordDescriptor{{Kind: "ss-server", Fields: []sealedstate.FieldDescriptor{{Name: "listener_dev"}, {Name: "listener_ino"}, {Name: "listener_type"}}}}
		if err := checkPrivateRootFields(nodes, []string{"ss-server"}, fingerprint); err != nil {
			t.Fatalf("actual fingerprint helper lost fields at %s: %v", root, err)
		}
	}
	for _, mutation := range []struct{ old, replacement, root string }{
		{"r.Fields[i] = f", "r.Fields[i].Name = f.Name", "SnapshotState"},
		{"r.Fields[i].Bytes = append([]byte(nil), f.Bytes...)", "r.Fields[i].Bytes = nil", "SnapshotState"},
		{"p.fields[f.Name] = f", "p.fields[f.Name] = nil", "restoreCutState"},
		{"p := a.state", "p := unrelated.state", "SnapshotState"},
		{"out.Records = append(out.Records, p.clone())", "out.Records = append(out.Records, binding.clone())", "SnapshotState"},
		{"state: p", "state: empty", "restoreCutState"},
	} {
		if actorCheck(strings.Replace(actorSource, mutation.old, mutation.replacement, 1), mutation.root) {
			t.Fatalf("actor record omission accepted: %s", mutation.old)
		}
	}
}

// The frozen codec hooks are intentionally unexported. These test-only bridges
// call those exact pure functions without adding a production API, changing a
// frozen signature, or spawning another go test from a gate. No IO/FD owner is
// called through a bridge; all resources stay out of this binary fixture.
// origin: hot-update S5 plan §12.1 / §10.1 / T49.
//
//go:linkname inventoryEncodePrivateState github.com/LinZiyang666/tether/internal/agent/ssproxy.encodeState
func inventoryEncodePrivateState(records []sealedstate.Record) ([]byte, error)

//go:linkname inventoryDecodePrivateState github.com/LinZiyang666/tether/internal/agent/ssproxy.decodeState
func inventoryDecodePrivateState(data []byte) ([]sealedstate.Record, error)

// Nonzero values prevent an omitted nonce/cursor/decision from passing via
// Go's zero initialization. Every field is then independently removed and
// independently corrupted in the actual binary decoder input.
// origin: hot-update S5 plan §10.1 / §12.1 / T49.
func TestSSStateInventoryPrivateCodecKeepsEveryField(t *testing.T) {
	records := make([]sealedstate.Record, 0, len(sealedstate.RecordDescriptors))
	for i, descriptor := range sealedstate.RecordDescriptors {
		record := sealedstate.Record{Kind: descriptor.Kind}
		for _, descriptor := range descriptor.Fields {
			field := sealedstate.Field{Name: descriptor.Name, Type: descriptor.Type}
			switch field.Type {
			case sealedstate.FieldU64, sealedstate.FieldNonnegativeI64, sealedstate.FieldBool:
				field.U64 = 1
			default:
				width := descriptor.Width
				if width == 0 {
					width = 1
				}
				field.Bytes = bytes.Repeat([]byte{byte(i + 1)}, width)
				if field.Type == sealedstate.FieldUTF8 {
					field.Bytes = []byte("fixture")
				}
			}
			record.Fields = append(record.Fields, field)
		}
		records = append(records, record)
	}
	encoded, err := inventoryEncodePrivateState(records)
	if err != nil {
		t.Fatalf("real binary codec required (pending P9): %v", err)
	}
	if len(encoded) == 0 || len(encoded) > sealedstate.MaxStateBytes {
		t.Fatal("empty or unbounded private binary state")
	}
	decoded, err := inventoryDecodePrivateState(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(records, decoded) {
		t.Fatal("actual binary codec lost a private field/value")
	}
	for i, record := range records {
		for j, field := range record.Fields {
			t.Run(record.Kind+"/"+field.Name, func(t *testing.T) {
				broken := make([]sealedstate.Record, len(records))
				copy(broken, records)
				broken[i].Fields = append(append([]sealedstate.Field{}, record.Fields[:j]...), record.Fields[j+1:]...)
				if _, err := inventoryEncodePrivateState(broken); err == nil {
					t.Fatal("actual encoder accepted missing required field")
				}
				// Field IDs are literal ASCII names on wire. Corrupt only this record's
				// spelling in a single-record encoding, independent of envelope format.
				single, err := inventoryEncodePrivateState([]sealedstate.Record{record})
				if err != nil {
					t.Fatal(err)
				}
				offset := bytes.Index(single, []byte(field.Name))
				if offset < 0 {
					t.Fatal("actual encoder did not use descriptor field ID")
				}
				single[offset] = '!'
				if _, err := inventoryDecodePrivateState(single); err == nil {
					t.Fatal("actual decoder accepted unknown private field ID")
				}
			})
		}
	}
	for _, bad := range [][]byte{nil, encoded[:len(encoded)-1], append(append([]byte{}, encoded...), 0)} {
		if _, err := inventoryDecodePrivateState(bad); err == nil {
			t.Fatal("actual decoder accepted truncated/trailing private state")
		}
	}
}
