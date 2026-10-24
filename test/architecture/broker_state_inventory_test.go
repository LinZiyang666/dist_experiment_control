package architecture

import (
	"flag"
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

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/storage"
)

const brokerStateInventoryPath = "testdata/broker_state_inventory.txt"

var updateBrokerStateInventory = flag.Bool("update-broker-state-inventory", false,
	"append broker-only private descriptors (refuses deletion or metadata changes)")

// Namespace membership is a protocol boundary, not a global sealedstate extension.
// These rows also pin codec ownership: two encoders cannot claim the same kind.
var brokerPrivateRecords = []brokerimage.PrivateRecordDescriptor{
	{Kind: "broker-runtime", Version: 1, StateRecord: "broker-state-v1", Owner: "P02", CompatibleReader: "broker-image-v1"},
	{Kind: "store", Version: 1, StateRecord: "broker-state-v1", Owner: "P03", CompatibleReader: "broker-image-v1"},
	{Kind: "control", Version: 1, StateRecord: "broker-state-v1", Owner: "P02", CompatibleReader: "broker-image-v1"},
	{Kind: "server", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1"},
	{Kind: "endpoint", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1"},
	{Kind: "permit", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1"},
	{Kind: "receipt", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1"},
	{Kind: "fences", Version: 1, StateRecord: "broker-tunnel-state-v1", Owner: "P07", CompatibleReader: "broker-image-v1"},
	{Kind: "resources", Version: 1, StateRecord: "broker-state-v1", Owner: "P02", CompatibleReader: "broker-image-v1"},
}

// origin: hot-update S6 plan §2.2 / §9.1 / T47
// gate-control: TestBrokerImageInventoryControls
func TestBrokerPrivateStatePartition(t *testing.T) {
	if err := checkBrokerPrivatePartition(brokerimage.PrivateRecordDescriptors, sealedstate.RecordDescriptors); err != nil {
		t.Fatal(err)
	}
}

func checkBrokerPrivatePartition(records []brokerimage.PrivateRecordDescriptor, global []sealedstate.RecordDescriptor) error {
	metadata := slices.Clone(records)
	for i := range metadata {
		metadata[i].Fields = nil
	}
	byKind := func(a, b brokerimage.PrivateRecordDescriptor) int { return strings.Compare(a.Kind, b.Kind) }
	slices.SortFunc(metadata, byKind)
	want := slices.Clone(brokerPrivateRecords)
	slices.SortFunc(want, byKind)
	if !reflect.DeepEqual(metadata, want) {
		return fmt.Errorf("broker private namespace/version/owner/reader membership drift")
	}
	var kinds []string
	for _, descriptor := range global {
		kinds = append(kinds, descriptor.Kind)
	}
	sort.Strings(kinds)
	if !slices.Equal(kinds, []string{"authority", "binding", "endpoint", "flow", "material", "permit", "receipt", "retirement", "salt-ring", "ss-actor", "ss-server", "timer"}) {
		return fmt.Errorf("broker-only kind polluted the frozen S2-S5 reader partition: %v", kinds)
	}
	return nil
}

// Collect producer tables next to their codecs, including inactive OS
// twins. Reflected canonical JSON projections are checked against their Go types
// before their compiled field manifests enter the same append-only inventory. P00's empty Fields are only an ownership skeleton. They must not certify
// a completed runtime or allow an updater to invent missing private state.
// origin: hot-update S6 plan §2.2 / §12.1 P32 / T47
// gate-control: TestBrokerImageInventoryControls
func TestBrokerPrivateStateInventoryAppendOnly(t *testing.T) {
	records := collectBrokerPrivateDescriptors(t)
	rows, err := brokerPrivateInventoryRows(records, false)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(brokerStateInventoryPath)
	if err != nil && (!os.IsNotExist(err) || !*updateBrokerStateInventory) {
		t.Fatal(err)
	}
	updated, err := appendBrokerPrivateInventory(raw, rows)
	if err != nil {
		t.Fatal(err)
	}
	if string(updated) != string(raw) {
		if !*updateBrokerStateInventory {
			t.Fatalf("new broker private descriptor rows require -update-broker-state-inventory:\n%s", strings.TrimPrefix(string(updated), string(raw)))
		}
		if err := os.WriteFile(brokerStateInventoryPath, updated, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func collectBrokerPrivateDescriptors(t *testing.T) []brokerimage.PrivateRecordDescriptor {
	t.Helper()
	records := slices.Clone(brokerimage.PrivateRecordDescriptors)
	index := map[string]int{}
	for i := range records {
		records[i].Fields = slices.Clone(records[i].Fields)
		index[records[i].Kind] = i
	}
	// P07's literal producer now lives beside the codec in the flowstate leaf.
	// The historical owner and all field metadata remain append-only.
	// origin: hot-update S6 plan §2.2 / §9.3 / T47
	for _, owner := range []string{"internal/brokerimage", "internal/storage", "internal/tunnel/flowstate", "internal/brokermaint"} {
		files := readFlowSSSources(t, repoRoot(t), owner, false)
		for filename, file := range files {
			if owner == "internal/brokerimage" && filename == "private_contracts.go" {
				continue
			}
			if owner == "internal/brokerimage" && filename == "codec.go" {
				if err := expandBrokerCodecFields(file, brokerimage.CodecRecordDescriptors); err != nil {
					t.Fatal(err)
				}
			}
			fields, err := brokerDescriptorFields(file)
			if err != nil {
				t.Fatalf("%s/%s: %v", owner, filename, err)
			}
			for kind, additions := range fields {
				i, ok := index[kind]
				if !ok {
					t.Fatalf("%s/%s: unknown private record kind %s", owner, filename, kind)
				}
				wantOwner := "internal/brokerimage"
				switch records[i].Owner {
				case "P03":
					wantOwner = "internal/storage"
				case "P07":
					wantOwner = "internal/tunnel/flowstate"
				}
				// P03 owns StoreSeal; P02 owns its codec, as frozen in §2.2.
				if kind == "store" && owner == "internal/brokerimage" && filename == "codec.go" {
					wantOwner = owner
				}
				if owner != wantOwner {
					t.Fatalf("%s/%s: descriptor %s belongs to %s", owner, filename, kind, wantOwner)
				}
				records[i].Fields = append(records[i].Fields, additions...)
			}
		}
	}
	return records
}

// A producer table uses the frozen PrivateRecordDescriptor/PrivateFieldDescriptor
// types. Inspect package declarations only: a dead function or test fixture must
// not supply inventory rows for a codec that never uses them. Unsupported dynamic
// expressions fail closed and ask for the producer's literal field manifest.

// The canonical JSON codec derives fields from concrete Go projections. Check
// every compiled field against an independent reflection walk, then materialize
// the manifest for the literal parser. No arbitrary dynamic descriptor gets this
// path, and deleting a field or changing a tag still changes the inventory.
// origin: hot-update S6 plan §2.2 / T47
func expandBrokerCodecFields(file *ast.File, records []brokerimage.PrivateRecordDescriptor) error {
	projections := map[string]reflect.Type{
		"broker-runtime": reflect.TypeFor[brokerimage.RuntimeState](),
		"control":        reflect.TypeFor[brokerimage.ControlState](),
		"store":          reflect.TypeFor[storage.StoreSeal](),
		"resources": reflect.TypeFor[struct {
			Owners []brokerimage.ResourceOwnership `json:"owners"`
		}](),
	}
	byKind := map[string][]brokerimage.PrivateFieldDescriptor{}
	for _, record := range records {
		typ := projections[record.Kind]
		if typ == nil {
			return fmt.Errorf("unknown reflected codec record %s", record.Kind)
		}
		expected := map[string]brokerimage.PrivateFieldDescriptor{}
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			expected[f.Tag.Get("json")] = brokerimage.PrivateFieldDescriptor{Name: f.Tag.Get("json"), Type: f.Type.String(), Encoding: "canonical-json", MaxBytes: brokerimage.MaxBundleBytes, CompatibleReader: brokerimage.Domain}
		}
		if len(record.Fields) != len(expected) {
			return fmt.Errorf("%s: reflected codec field count drift", record.Kind)
		}
		for _, field := range record.Fields {
			want, ok := expected[field.Name]
			if !ok || field != want {
				return fmt.Errorf("%s.%s: reflected codec field metadata drift", record.Kind, field.Name)
			}
			delete(expected, field.Name)
		}
		byKind[record.Kind] = record.Fields
	}
	found := 0
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			if len(value.Names) != 1 || value.Names[0].Name != "CodecRecordDescriptors" || len(value.Values) != 1 {
				continue
			}
			list, ok := value.Values[0].(*ast.CompositeLit)
			if !ok {
				return fmt.Errorf("codec descriptor table must be a literal")
			}
			for _, element := range list.Elts {
				record, ok := element.(*ast.CompositeLit)
				if !ok {
					return fmt.Errorf("codec record must be a literal")
				}
				members, err := brokerLiteralMembers(record)
				if err != nil {
					return err
				}
				kind, err := brokerLiteralString(members["Kind"])
				if err != nil {
					return err
				}
				fields, ok := byKind[kind]
				if !ok {
					return fmt.Errorf("unknown codec projection %s", kind)
				}
				var literals []string
				for _, field := range fields {
					literals = append(literals, fmt.Sprintf("{Name:%q,Type:%q,Encoding:%q,MaxBytes:%d,Secret:%t,CompatibleReader:%q}", field.Name, field.Type, field.Encoding, field.MaxBytes, field.Secret, field.CompatibleReader))
				}
				expression, err := parser.ParseExpr("[]PrivateFieldDescriptor{" + strings.Join(literals, ",") + "}")
				if err != nil {
					return err
				}
				for _, item := range record.Elts {
					member := item.(*ast.KeyValueExpr)
					if member.Key.(*ast.Ident).Name == "Fields" {
						member.Value = expression
						found++
					}
				}
			}
		}
	}
	if found != len(projections) {
		return fmt.Errorf("missing canonical codec projection")
	}
	return nil
}

func brokerDescriptorFields(file *ast.File) (map[string][]brokerimage.PrivateFieldDescriptor, error) {
	out := map[string][]brokerimage.PrivateFieldDescriptor{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for _, expression := range value.Values {
				list, ok := expression.(*ast.CompositeLit)
				if !ok {
					continue
				}
				array, ok := list.Type.(*ast.ArrayType)
				if !ok || brokerTypeName(array.Elt) != "PrivateRecordDescriptor" {
					continue
				}
				for _, element := range list.Elts {
					record, ok := element.(*ast.CompositeLit)
					if !ok {
						return nil, fmt.Errorf("private descriptor record must be a literal")
					}
					members, err := brokerLiteralMembers(record)
					if err != nil {
						return nil, err
					}
					kind, err := brokerLiteralString(members["Kind"])
					if err != nil || kind == "" {
						return nil, fmt.Errorf("private descriptor needs literal Kind")
					}
					var expected *brokerimage.PrivateRecordDescriptor
					for i := range brokerPrivateRecords {
						if brokerPrivateRecords[i].Kind == kind {
							expected = &brokerPrivateRecords[i]
						}
					}
					if expected == nil || len(members) != 6 {
						return nil, fmt.Errorf("%s: unknown/incomplete record descriptor", kind)
					}
					for name, want := range map[string]string{"StateRecord": expected.StateRecord, "Owner": expected.Owner, "CompatibleReader": expected.CompatibleReader} {
						actual, err := brokerLiteralString(members[name])
						if err != nil || actual != want {
							return nil, fmt.Errorf("%s: record descriptor %s drift", kind, name)
						}
					}
					version := members["Version"]
					literal, literalOK := version.(*ast.BasicLit)
					if (!literalOK || literal.Kind != token.INT || literal.Value != "1") && brokerTypeName(version) != "PrivateDescriptorVersion" {
						return nil, fmt.Errorf("%s: record descriptor Version drift", kind)
					}
					fields, ok := members["Fields"].(*ast.CompositeLit)
					if !ok {
						return nil, fmt.Errorf("%s: private descriptor needs literal Fields", kind)
					}
					for _, element := range fields.Elts {
						field, ok := element.(*ast.CompositeLit)
						if !ok {
							return nil, fmt.Errorf("%s: private field must be a literal", kind)
						}
						members, err := brokerLiteralMembers(field)
						if err != nil {
							return nil, err
						}
						if len(members) != 6 {
							return nil, fmt.Errorf("%s: incomplete/unknown private field metadata", kind)
						}
						var d brokerimage.PrivateFieldDescriptor
						for name, target := range map[string]*string{"Name": &d.Name, "Type": &d.Type, "Encoding": &d.Encoding, "CompatibleReader": &d.CompatibleReader} {
							*target, err = brokerLiteralString(members[name])
							if err != nil {
								return nil, fmt.Errorf("%s: descriptor %s: %w", kind, name, err)
							}
						}
						maximum, ok := members["MaxBytes"].(*ast.BasicLit)
						if !ok || maximum.Kind != token.INT {
							return nil, fmt.Errorf("%s.%s: MaxBytes must be an integer literal", kind, d.Name)
						}
						d.MaxBytes, err = strconv.Atoi(maximum.Value)
						if err != nil {
							return nil, err
						}
						secret, ok := members["Secret"].(*ast.Ident)
						if !ok || secret.Name != "true" && secret.Name != "false" {
							return nil, fmt.Errorf("%s.%s: Secret must be explicit", kind, d.Name)
						}
						d.Secret = secret.Name == "true"
						out[kind] = append(out[kind], d)
					}
				}
			}
		}
	}
	return out, nil
}

func brokerDescriptorTableNames(file *ast.File) []string {
	var names []string
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.VAR {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, expression := range value.Values {
				literal, ok := expression.(*ast.CompositeLit)
				if !ok {
					continue
				}
				array, ok := literal.Type.(*ast.ArrayType)
				if ok && brokerTypeName(array.Elt) == "PrivateRecordDescriptor" && i < len(value.Names) {
					names = append(names, value.Names[i].Name)
				}
			}
		}
	}
	return names
}

func brokerTypeName(expression ast.Expr) string {
	switch typ := expression.(type) {
	case *ast.Ident:
		return typ.Name
	case *ast.SelectorExpr:
		return typ.Sel.Name
	default:
		return ""
	}
}

func brokerLiteralMembers(literal *ast.CompositeLit) (map[string]ast.Expr, error) {
	out := map[string]ast.Expr{}
	for _, element := range literal.Elts {
		member, ok := element.(*ast.KeyValueExpr)
		if !ok {
			return nil, fmt.Errorf("private descriptors must use keyed fields")
		}
		name, ok := member.Key.(*ast.Ident)
		if !ok || out[name.Name] != nil {
			return nil, fmt.Errorf("invalid or duplicate private descriptor member")
		}
		out[name.Name] = member.Value
	}
	return out, nil
}

func brokerLiteralString(expression ast.Expr) (string, error) {
	if literal, ok := expression.(*ast.BasicLit); ok && literal.Kind == token.STRING {
		return strconv.Unquote(literal.Value)
	}
	switch brokerTypeName(expression) {
	case "Domain":
		return brokerimage.Domain, nil
	case "PrivateStateRecord":
		return brokerimage.PrivateStateRecord, nil
	case "TunnelStateRecord":
		return brokerimage.TunnelStateRecord, nil
	}
	return "", fmt.Errorf("expected string literal or frozen namespace constant")
}

func brokerPrivateInventoryRows(records []brokerimage.PrivateRecordDescriptor, complete bool) ([]string, error) {
	if err := checkBrokerPrivatePartition(records, sealedstate.RecordDescriptors); err != nil {
		return nil, err
	}
	var rows []string
	for _, record := range records {
		rows = append(rows, fmt.Sprintf("record\t%s\t%d\t%s\t%s\t%s", record.Kind, record.Version, record.StateRecord, record.Owner, record.CompatibleReader))
		if complete && len(record.Fields) == 0 {
			return nil, fmt.Errorf("NOT_RUN pending P02/P03/P07/P15: private descriptor %s has no producer fields", record.Kind)
		}
		seen := map[string]bool{}
		for _, field := range record.Fields {
			if field.Name == "" || strings.ContainsAny(field.Name+field.Type+field.Encoding, "\t\r\n") || seen[field.Name] || field.Type == "" || field.Encoding == "" || field.MaxBytes < 0 || field.MaxBytes > sealedstate.MaxStateBytes || field.CompatibleReader != record.CompatibleReader {
				return nil, fmt.Errorf("invalid/duplicate private descriptor %s.%s", record.Kind, field.Name)
			}
			seen[field.Name] = true
			rows = append(rows, fmt.Sprintf("field\t%s.%s\t%s\t%s\t%d\t%t\t%s", record.Kind, field.Name, field.Type, field.Encoding, field.MaxBytes, field.Secret, field.CompatibleReader))
		}
	}
	sort.Strings(rows)
	return rows, nil
}

func appendBrokerPrivateInventory(raw []byte, rows []string) ([]byte, error) {
	live, old := map[string]string{}, map[string]bool{}
	for _, row := range rows {
		columns := strings.Split(row, "\t")
		if len(columns) < 2 || live[columns[0]+"\t"+columns[1]] != "" {
			return nil, fmt.Errorf("invalid/duplicate private inventory row")
		}
		live[columns[0]+"\t"+columns[1]] = row
	}
	if len(live) == 0 {
		return nil, fmt.Errorf("empty private inventory")
	}
	for _, row := range strings.Split(string(raw), "\n") {
		if row == "" || strings.HasPrefix(row, "#") {
			continue
		}
		columns := strings.Split(row, "\t")
		if len(columns) < 2 {
			return nil, fmt.Errorf("malformed private inventory row")
		}
		identity := columns[0] + "\t" + columns[1]
		if old[identity] || live[identity] != row {
			return nil, fmt.Errorf("refusing private inventory deletion/metadata drift/duplicate: %s", row)
		}
		old[identity] = true
	}
	out := append([]byte(nil), raw...)
	if len(out) == 0 {
		out = []byte("# Append-only broker-only private descriptor inventory.\n# record<TAB>kind<TAB>version<TAB>state record<TAB>owner<TAB>reader\n# field<TAB>kind.name<TAB>type<TAB>encoding<TAB>max bytes<TAB>secret<TAB>reader\n# P00 record rows pin ownership; closure requires actual producer field rows.\n")
	}
	if out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}
	var added []string
	for identity, row := range live {
		if !old[identity] {
			added = append(added, row)
		}
	}
	sort.Strings(added)
	for _, row := range added {
		out = append(out, []byte(row+"\n")...)
	}
	return out, nil
}

// No stub or empty descriptor can pass this closure gate. Each direction is
// checked separately from its production entry point; an unrelated helper or a
// package-level table cannot stand in for a field actually exported/imported.
// origin: hot-update S6 plan §2.2 / §11 T47
// gate-control: TestBrokerImageInventoryControls
func TestBrokerPrivateStateInventoryMatchesCodecs(t *testing.T) {
	records := collectBrokerPrivateDescriptors(t)
	if _, err := brokerPrivateInventoryRows(records, true); err != nil {
		t.Fatal(err)
	}
	for _, owner := range []struct {
		pkg, namespace, export, restore string
	}{
		{"internal/brokerimage", "broker-state-v1", "Export", "Import"},
		{"internal/tunnel", "broker-tunnel-state-v1", "snapshot", "ImportServer"},
	} {
		files := readFlowSSSources(t, repoRoot(t), owner.pkg, false)
		// Runtime builders call the moved leaf codec. Include that owner so both
		// directions still prove reachable descriptor consumption and field mapping.
		// origin: hot-update S6 plan §2.2 / §9.3 / T47
		if owner.pkg == "internal/tunnel" {
			for name, file := range readFlowSSSources(t, repoRoot(t), "internal/tunnel/flowstate", false) {
				files["flowstate/"+name] = file
			}
		}
		var tables []string
		for filename, file := range files {
			if filename != "private_contracts.go" {
				tables = append(tables, brokerDescriptorTableNames(file)...)
			}
		}
		// The production exporter decorates the leaf cut with its actual runtime
		// projection. Follow that owner rather than treating the undecorated Cut
		// as the broker-state assembler.
		// origin: hot-update S6 plan §2.2 / §3.2 / T47
		if owner.pkg == "internal/brokerimage" {
			for name, file := range readFlowSSSources(t, repoRoot(t), "internal/broker", false) {
				files["broker/"+name] = file
			}
			// S7 P24 moved that decorator verbatim from broker/reexec.go brokerRunCut
			// to xferbridge.Cut (Export -> project). Follow the moved owner too; the
			// broker root stays scanned so a decorator moving back is still followed.
			// origin: hot-update S7 plan §8.2 / P24
			for name, file := range readFlowSSSources(t, repoRoot(t), "internal/xferbridge", false) {
				files["xferbridge/"+name] = file
			}
		}
		for _, root := range []string{owner.export, owner.restore} {
			nodes, err := reachableFlowSSFunctions(files, root)
			if owner.pkg == "internal/tunnel" {
				nodes, err = reachableBrokerTunnelPrivateFunctions(files, root)
			}
			if err != nil {
				t.Fatalf("NOT_RUN pending P02/P07: %s.%s: %v", owner.pkg, root, err)
			}
			if owner.pkg == "internal/brokerimage" {
				// Reflection maps every public projection field to one framed ID.
				// Include the independently checked expanded table only when this
				// direction actually consumes it; metadata consumption still has
				// to be proved by reachable codec guards below.
				used := false
				for _, node := range nodes {
					ast.Inspect(node, func(n ast.Node) bool {
						if id, ok := n.(*ast.Ident); ok && id.Name == "CodecRecordDescriptors" {
							used = true
						}
						return true
					})
				}
				if used {
					if err := expandBrokerCodecFields(files["codec.go"], brokerimage.CodecRecordDescriptors); err != nil {
						t.Fatal(err)
					}
					for _, decl := range files["codec.go"].Decls {
						if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.VAR {
							for _, spec := range gen.Specs {
								if v, ok := spec.(*ast.ValueSpec); ok && len(v.Names) == 1 && v.Names[0].Name == "CodecRecordDescriptors" {
									nodes = append(nodes, v)
								}
							}
						}
					}
				}
			}
			if err := checkBrokerPrivateDirection(nodes, records, owner.namespace, tables); err != nil {
				t.Fatalf("%s.%s: %v", owner.pkg, root, err)
			}
		}
	}
}

// Imported socket/keeper methods named Export are not tunnel exporters.
// Following them by name alone reaches the restore validator and lets importer
// fields certify an incomplete exporter. Only calls within these codec packages
// can supply private-field evidence; the runtime snapshot roots are explicit.
// origin: hot-update S6 plan §2.2 / §11 / T47
func reachableBrokerTunnelPrivateFunctions(files map[string]*ast.File, root string) ([]ast.Node, error) {
	type function struct {
		decl    *ast.FuncDecl
		imports map[string]string
	}
	functions := map[string][]function{}
	for _, file := range files {
		imports := map[string]string{}
		for _, imp := range file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return nil, err
			}
			if path != modPrefix+"internal/tunnel/flowstate" {
				continue
			}
			name := "flowstate"
			if imp.Name != nil {
				name = imp.Name.Name
			}
			imports[name] = "flowstate"
		}
		for _, decl := range file.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok {
				key := file.Name.Name + "." + fn.Name.Name
				functions[key] = append(functions[key], function{fn, imports})
			}
		}
	}
	if len(functions["tunnel."+root]) == 0 {
		return nil, fmt.Errorf("missing actual private root %s", root)
	}
	var nodes []ast.Node
	seen := map[string]bool{}
	var visit func(string)
	visit = func(key string) {
		if seen[key] {
			return
		}
		seen[key] = true
		pkg, _, _ := strings.Cut(key, ".")
		for _, fn := range functions[key] {
			nodes = append(nodes, fn.decl)
			ast.Inspect(fn.decl.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch target := call.Fun.(type) {
				case *ast.Ident:
					visit(pkg + "." + target.Name)
				case *ast.SelectorExpr:
					if alias, ok := target.X.(*ast.Ident); ok && fn.imports[alias.Name] != "" {
						visit(fn.imports[alias.Name] + "." + target.Sel.Name)
					}
				}
				return true
			})
		}
	}
	visit("tunnel." + root)
	return nodes, nil
}

// brokerDescriptorEqualityMembers reports descriptor members a reachable codec
// consumes by exact equality instead of one selector per member: S7 P24 made
// validatePrivateCodec compare each record's Fields with a per-field projection
// built from reflection (reflect.DeepEqual(record.Fields, privateFields(T))).
// DeepEqual reads every member of every descriptor, so all members count — but
// only when the comparison really is against the record's Fields AND the other
// side is a reachable function constructing PrivateFieldDescriptor values whose
// Name and Type are set (a projection, not a constant or an unrelated value).
// origin: hot-update S7 plan §8.3 / P24
func brokerDescriptorEqualityMembers(nodes []ast.Node) map[string]bool {
	functions := map[string]*ast.FuncDecl{}
	for _, node := range nodes {
		if fn, ok := node.(*ast.FuncDecl); ok && fn.Body != nil {
			functions[fn.Name.Name] = fn
		}
	}
	projects := func(name string) bool {
		fn := functions[name]
		if fn == nil {
			return false
		}
		descriptorType := func(expr ast.Expr) bool {
			switch typ := expr.(type) {
			case *ast.Ident:
				return typ.Name == "PrivateFieldDescriptor"
			case *ast.SelectorExpr:
				return typ.Sel.Name == "PrivateFieldDescriptor"
			}
			return false
		}
		projection := func(lit *ast.CompositeLit) bool {
			keys := map[string]bool{}
			for _, element := range lit.Elts {
				if kv, ok := element.(*ast.KeyValueExpr); ok {
					if key, ok := kv.Key.(*ast.Ident); ok {
						keys[key.Name] = true
					}
				}
			}
			return keys["Name"] && keys["Type"]
		}
		found := false
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			if descriptorType(lit.Type) {
				found = found || projection(lit)
			}
			// Elements of a []PrivateFieldDescriptor literal elide their type.
			if array, ok := lit.Type.(*ast.ArrayType); ok && descriptorType(array.Elt) {
				for _, element := range lit.Elts {
					if inner, ok := element.(*ast.CompositeLit); ok && inner.Type == nil {
						found = found || projection(inner)
					}
				}
			}
			return true
		})
		return found
	}
	consumed := false
	for _, node := range nodes {
		ast.Inspect(node, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 2 {
				return true
			}
			fun, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || fun.Sel.Name != "DeepEqual" {
				return true
			}
			if pkg, ok := fun.X.(*ast.Ident); !ok || pkg.Name != "reflect" {
				return true
			}
			for i := range 2 {
				fields, ok := call.Args[i].(*ast.SelectorExpr)
				if !ok || fields.Sel.Name != "Fields" {
					continue
				}
				other, ok := call.Args[1-i].(*ast.CallExpr)
				if !ok {
					continue
				}
				if target, ok := other.Fun.(*ast.Ident); ok && projects(target.Name) {
					consumed = true
				}
			}
			return true
		})
	}
	members := map[string]bool{}
	if consumed {
		descriptor := reflect.TypeFor[brokerimage.PrivateFieldDescriptor]()
		for i := range descriptor.NumField() {
			members[descriptor.Field(i).Name] = true
		}
	}
	return members
}

func checkBrokerPrivateDirection(nodes []ast.Node, records []brokerimage.PrivateRecordDescriptor, namespace string, tables []string) error {
	_, selectors := privateRootTokens(nodes)
	used := false
	for _, node := range nodes {
		ast.Inspect(node, func(node ast.Node) bool {
			// A mention or index in an unentered loop cannot consume a table.
			// Both real codecs iterate the descriptor manifest before framing fields.
			// origin: hot-update S6 plan §2.2 / T47
			loop, ok := node.(*ast.RangeStmt)
			if !ok {
				return true
			}
			name := ""
			switch table := loop.X.(type) {
			case *ast.Ident:
				name = table.Name
			case *ast.SelectorExpr:
				name = table.Sel.Name
			}
			used = used || slices.Contains(tables, name)
			return true
		})
	}
	if !used {
		return fmt.Errorf("actual codec does not consume its producer descriptor table")
	}
	equality := brokerDescriptorEqualityMembers(nodes)
	for _, member := range []string{"Name", "Type", "Encoding", "MaxBytes", "Secret", "CompatibleReader"} {
		if !selectors[member] && !equality[member] {
			return fmt.Errorf("actual codec does not consume descriptor %s", member)
		}
	}
	// Field IDs are scoped by kind. A same-named cursor in a different record
	// cannot stand in for the omitted cursor. Require the kind and field in the
	// same reachable builder/restore function, or a qualified literal field ID.
	var localNames []map[string]bool
	for _, node := range nodes {
		names, _ := privateRootTokens([]ast.Node{node})
		localNames = append(localNames, names)
	}
	// A closed kind switch can dispatch restoration to a dedicated helper.
	// Preserve that literal kind provenance across only its reachable callees;
	// fields from a different case or an unused helper cannot satisfy this one.
	// origin: hot-update S6 plan §2.2 / T47
	functions := map[string][]*ast.FuncDecl{}
	for _, node := range nodes {
		if fn, ok := node.(*ast.FuncDecl); ok {
			functions[fn.Name.Name] = append(functions[fn.Name.Name], fn)
		}
	}
	for _, node := range nodes {
		ast.Inspect(node, func(n ast.Node) bool {
			branch, ok := n.(*ast.CaseClause)
			if !ok || len(branch.List) != 1 {
				return true
			}
			lit, ok := branch.List[0].(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				return true
			}
			kind, err := strconv.Unquote(lit.Value)
			if err != nil {
				return true
			}
			names, _ := privateRootTokens([]ast.Node{branch})
			names[kind] = true
			seen := map[string]bool{}
			var follow func(ast.Node)
			follow = func(body ast.Node) {
				ast.Inspect(body, func(n ast.Node) bool {
					call, ok := n.(*ast.CallExpr)
					if !ok {
						return true
					}
					name := ""
					switch target := call.Fun.(type) {
					case *ast.Ident:
						name = target.Name
					case *ast.SelectorExpr:
						name = target.Sel.Name
					}
					if seen[name] {
						return true
					}
					seen[name] = true
					for _, fn := range functions[name] {
						tokens, _ := privateRootTokens([]ast.Node{fn})
						for token := range tokens {
							names[token] = true
						}
						follow(fn.Body)
					}
					return true
				})
			}
			follow(branch)
			localNames = append(localNames, names)
			return true
		})
	}
	fields := 0
	for _, record := range records {
		if record.StateRecord != namespace {
			continue
		}
		for _, field := range record.Fields {
			fields++
			found := false
			for _, names := range localNames {
				found = found || names[record.Kind+"."+field.Name] || names[record.Kind] && names[field.Name]
			}
			if !found {
				return fmt.Errorf("private runtime/codec omits field ID %s.%s", record.Kind, field.Name)
			}
		}
	}
	if fields == 0 {
		return fmt.Errorf("no actual private fields in %s", namespace)
	}
	return nil
}

// Synthetic samples exercise the same predicates as the live gates. They test
// scanner sensitivity; they do not claim a missing owner's codec is implemented.
// origin: hot-update S6 plan §11 / T47
func TestBrokerImageInventoryControls(t *testing.T) {
	records := slices.Clone(brokerPrivateRecords)
	if err := checkBrokerPrivatePartition(records, sealedstate.RecordDescriptors); err != nil {
		t.Fatal(err)
	}
	for i := range records {
		broken := slices.Clone(records)
		broken[i].Owner = "other"
		if checkBrokerPrivatePartition(broken, sealedstate.RecordDescriptors) == nil {
			t.Fatal("private owner drift accepted")
		}
		if checkBrokerPrivatePartition(append(slices.Clone(records[:i]), records[i+1:]...), sealedstate.RecordDescriptors) == nil {
			t.Fatal("private kind deletion accepted")
		}
	}
	global := append(slices.Clone(sealedstate.RecordDescriptors), sealedstate.RecordDescriptor{Kind: "broker-runtime"})
	if checkBrokerPrivatePartition(records, global) == nil {
		t.Fatal("broker-only global kind accepted by old readers")
	}
	if _, err := brokerPrivateInventoryRows(records, true); err == nil {
		t.Fatal("empty P00 skeleton certified codec closure")
	}
	for i := range records {
		records[i].Fields = []brokerimage.PrivateFieldDescriptor{{Name: "cursor", Type: "uint64", Encoding: "u64be", MaxBytes: 8, Secret: false, CompatibleReader: "broker-image-v1"}}
	}
	rows, err := brokerPrivateInventoryRows(records, true)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := appendBrokerPrivateInventory(nil, rows)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		if _, err := appendBrokerPrivateInventory(ledger, append(slices.Clone(rows[:i]), rows[i+1:]...)); err == nil {
			t.Fatal("private record/field deletion accepted")
		}
		columns := strings.Split(row, "\t")
		for column := 2; column < len(columns); column++ {
			broken := slices.Clone(rows)
			changed := slices.Clone(columns)
			changed[column] += "drift"
			broken[i] = strings.Join(changed, "\t")
			if _, err := appendBrokerPrivateInventory(ledger, broken); err == nil {
				t.Fatalf("private metadata drift accepted: %s column %d", columns[1], column)
			}
		}
	}
	const fixture = `package fixture
var descriptors = []brokerimage.PrivateRecordDescriptor{{Kind:"broker-runtime", Version:1, StateRecord:"broker-state-v1", Owner:"P02", CompatibleReader:"broker-image-v1", Fields:[]brokerimage.PrivateFieldDescriptor{{Name:"cursor", Type:"uint64", Encoding:"u64be", MaxBytes:8, Secret:false, CompatibleReader:"broker-image-v1"}}}}
func Export(){encode()}
func Import(){decode()}
func encode(){for range descriptors {};_=d.Name;_=d.Type;_=d.Encoding;_=d.MaxBytes;_=d.Secret;_=d.CompatibleReader;_= "cursor";_= "broker-runtime";_= "store";_= "control";_= "resources"}
func decode(){for range descriptors {};_=d.Name;_=d.Type;_=d.Encoding;_=d.MaxBytes;_=d.Secret;_=d.CompatibleReader;_= "cursor";_= "broker-runtime";_= "store";_= "control";_= "resources"}
`
	parse := func(source string) *ast.File {
		file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		return file
	}
	// Reflected manifests receive the same drift controls as literal tables.
	// Each mutation changes the compiled producer table, independently of the
	// source expansion; the gate must reject it before appending any rows.
	// origin: hot-update S6 plan §2.2 / T47
	codecSource, err := os.ReadFile(filepath.Join(repoRoot(t), "internal/brokerimage/codec.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := expandBrokerCodecFields(parse(string(codecSource)), brokerimage.CodecRecordDescriptors); err != nil {
		t.Fatal(err)
	}
	for i, record := range brokerimage.CodecRecordDescriptors {
		for _, defect := range []string{"delete", "name", "type", "encoding", "max", "secret", "reader"} {
			broken := slices.Clone(brokerimage.CodecRecordDescriptors)
			broken[i].Fields = slices.Clone(record.Fields)
			switch defect {
			case "delete":
				broken[i].Fields = broken[i].Fields[1:]
			case "name":
				broken[i].Fields[0].Name += "_lost"
			case "type":
				broken[i].Fields[0].Type = "unknown"
			case "encoding":
				broken[i].Fields[0].Encoding = "unknown"
			case "max":
				broken[i].Fields[0].MaxBytes++
			case "secret":
				broken[i].Fields[0].Secret = true
			case "reader":
				broken[i].Fields[0].CompatibleReader = "unknown"
			}
			if expandBrokerCodecFields(parse(string(codecSource)), broken) == nil {
				t.Fatalf("reflected descriptor drift accepted: %s/%s", record.Kind, defect)
			}
		}
	}
	if expandBrokerCodecFields(parse("package x"), brokerimage.CodecRecordDescriptors) == nil {
		t.Fatal("missing reflected descriptor table accepted")
	}
	// A socket Export selector cannot borrow a same-named runtime method.
	// The codec leaf remains reachable through its actual import qualifier.
	// origin: hot-update S6 plan §2.2 / T47
	graph := map[string]*ast.File{
		"runtime.go":         parse(`package tunnel; import raw "os"; import codec "github.com/LinZiyang666/tether/internal/tunnel/flowstate"; func snapshot(){raw.Export();codec.ServerRecord()}; func Export(){_= "borrowed"}`),
		"flowstate/codec.go": parse(`package flowstate;func ServerRecord(){_= "actual"}`),
	}
	nodes, err := reachableBrokerTunnelPrivateFunctions(graph, "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	tokens, _ := privateRootTokens(nodes)
	if tokens["borrowed"] || !tokens["actual"] {
		t.Fatal("private graph borrowed socket Export or lost the codec leaf")
	}
	if _, err := reachableBrokerTunnelPrivateFunctions(graph, "missing"); err == nil {
		t.Fatal("missing private root accepted")
	}
	fields, err := brokerDescriptorFields(parse(fixture))
	if err != nil || !reflect.DeepEqual(fields["broker-runtime"], records[0].Fields) {
		t.Fatalf("producer table collector failed: %v, %v", fields, err)
	}
	// Both the literal and shared-constant version forms remain valid; missing,
	// non-integer or changed literals cannot certify a private state reader.
	// origin: hot-update S6 plan §2.2 / T47
	if _, err := brokerDescriptorFields(parse(strings.Replace(fixture, "Version:1", "Version:brokerimage.PrivateDescriptorVersion", 1))); err != nil {
		t.Fatal(err)
	}
	for _, version := range []string{"2", "1.0", `"1"`, "nil"} {
		if _, err := brokerDescriptorFields(parse(strings.Replace(fixture, "Version:1", "Version:"+version, 1))); err == nil {
			t.Fatalf("invalid private descriptor version accepted: %s", version)
		}
	}
	// Closed dispatch provenance survives a dedicated restoration helper.
	// A field from another kind must not be borrowed to satisfy this one.
	// origin: hot-update S6 plan §2.2 / T47
	dispatched := strings.Replace(fixture, "func Import(){decode()}", `func Import(){switch r.Kind {case "broker-runtime":decode()}}`, 1)
	start := strings.Index(dispatched, "func decode()")
	dispatched = dispatched[:start] + strings.Replace(dispatched[start:], `_= "broker-runtime";`, "", 1)
	checkDispatch := func(source string) error {
		file := parse(source)
		nodes, err := reachableFlowSSFunctions(map[string]*ast.File{"fixture.go": file}, "Import")
		if err != nil {
			return err
		}
		return checkBrokerPrivateDirection(nodes, records, "broker-state-v1", brokerDescriptorTableNames(file))
	}
	if err := checkDispatch(dispatched); err != nil {
		t.Fatal(err)
	}
	if checkDispatch(strings.Replace(dispatched, `case "broker-runtime"`, `case "wrong-kind"`, 1)) == nil {
		t.Fatal("restoration borrowed field IDs from another dispatch kind")
	}
	if checkDispatch(strings.Replace(dispatched, `func decode(){`, `func dormantDecode(){`, 1)) == nil {
		t.Fatal("dormant restoration helper satisfied field IDs")
	}
	for _, root := range []string{"Export", "Import"} {
		check := func(source string) error {
			nodes, err := reachableFlowSSFunctions(map[string]*ast.File{"fixture.go": parse(source)}, root)
			if err != nil {
				return err
			}
			return checkBrokerPrivateDirection(nodes, records, "broker-state-v1", brokerDescriptorTableNames(parse(source)))
		}
		if err := check(fixture); err != nil {
			t.Fatal(err)
		}
		if check(strings.ReplaceAll(fixture, "range descriptors", "range unusedTable")) == nil {
			t.Fatalf("unused producer descriptor table accepted at %s", root)
		}
		for _, member := range []string{"Name", "Type", "Encoding", "MaxBytes", "Secret", "CompatibleReader"} {
			if check(strings.ReplaceAll(fixture, "d."+member, "d.Unused")) == nil {
				t.Fatalf("unconsumed codec descriptor %s accepted at %s", member, root)
			}
		}
		// Exact equality against a reachable per-field projection consumes every
		// member; equality with anything else consumes none.
		// origin: hot-update S7 plan §8.3 / P24
		equal := strings.ReplaceAll(fixture, "_=d.Type;_=d.Encoding;", "") + `
func validate(){_=reflect.DeepEqual(record.Fields, project(typ))}
func project(typ reflect.Type) []brokerimage.PrivateFieldDescriptor {return []brokerimage.PrivateFieldDescriptor{{Name: typ.Name(), Type: typ.String(), Encoding: "canonical-json"}}}
`
		equal = strings.ReplaceAll(equal, "func encode(){", "func encode(){validate();")
		equal = strings.ReplaceAll(equal, "func decode(){", "func decode(){validate();")
		if err := check(equal); err != nil {
			t.Fatalf("exact projection equality not recognised at %s: %v", root, err)
		}
		for _, defect := range []string{
			strings.Replace(equal, "record.Fields", "record.Kind", 1),
			strings.Replace(equal, "project(typ))}", "constant())}", 1),
			strings.Replace(equal, "Type: typ.String(), ", "", 1),
			strings.Replace(equal, "func validate(){", "func dormant(){", 1),
			strings.Replace(equal, "reflect.DeepEqual", "other.DeepEqual", 1),
		} {
			if check(defect) == nil {
				t.Fatalf("non-projection equality certified descriptor members at %s", root)
			}
		}
		if check(strings.ReplaceAll(fixture, `_= "broker-runtime"`, `_= "unrelated-kind"`)) == nil {
			t.Fatalf("same-named field in another kind masked omission at %s", root)
		}
		if check(strings.ReplaceAll(fixture, `_= "cursor"`, `_= "omitted"`)) == nil {
			t.Fatalf("private codec field omission accepted at %s", root)
		}
		call := "encode()"
		if root == "Import" {
			call = "decode()"
		}
		if check(strings.Replace(fixture, call, "unrelated()", 1)) == nil {
			t.Fatalf("unreachable private codec accepted at %s", root)
		}
	}
	for _, defect := range []string{`Secret:false`, `MaxBytes:8`, `Name:"cursor"`} {
		if _, err := brokerDescriptorFields(parse(strings.Replace(fixture, defect, strings.Split(defect, ":")[0]+":dynamic", 1))); err == nil {
			t.Fatalf("incomplete producer field descriptor accepted: %s", defect)
		}
	}
}
