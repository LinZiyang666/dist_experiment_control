package proto

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

var updateWireInventory = flag.Bool("update-wire-inventory", false,
	"append newly added wire fields to testdata/wire_field_inventory.txt (refuses to drop entries)")

// wireInventoryPath is the append-only ledger of every exported field of every
// exported struct in proto, plus the reachable local admin roots: one line
// per field, `Type.Field<TAB>tag<TAB>type`. Existing proto identities retain
// their spelling; admin identities are package-qualified to avoid collisions.
const wireInventoryPath = "testdata/wire_field_inventory.txt"

// origin: upgrade-safety plan §2 — the mechanical half of the N-1 window
// (requirements §6.7, architecture §21.2).
//
// TestWireFieldInventoryAppendOnly pins the SET of wire fields, append-only:
// deleting a field, renaming it, or changing its json tag or type makes the
// ledger entry dangle and the test red. Adding a field only requires re-running
// with -update-wire-inventory — the updater appends but REFUSES to drop
// entries, the same friction philosophy as the structural-budget ratchet: the
// only way to shrink the wire is to hand-edit the ledger and justify it in the
// commit message (that edit is an epoch decision, see architecture §21.4).
//
// Deliberately NOT checked here: omitempty, and "the zero value is legal
// semantics". Old peers ignore unknown JSON fields either way; the real
// additive constraint is behavioral and lives in §21.2 as prose + review
// discipline. A gate that pretended to verify it mechanically would be a
// false badge.
func TestWireFieldInventoryAppendOnly(t *testing.T) {
	current := collectWireFields(t)
	if len(current) < 100 {
		t.Fatalf("parsed only %d exported wire fields from package proto — the collector is broken "+
			"(this package declares far more; a silent short list would let deletions through)", len(current))
	}

	currentSet := make(map[string]bool, len(current))
	for _, l := range current {
		currentSet[l] = true
	}

	raw, err := os.ReadFile(wireInventoryPath)
	if err != nil {
		// Bootstrap path (ledger file absent). Deleting the ledger and
		// re-bootstrapping WOULD shrink it in one step — that residual
		// bypass is accepted (internal review S25): the deletion itself is
		// a tracked-file removal that cannot land without showing up in
		// `git status`/diff and surviving commit review, which is the same
		// human gate a hand-edit goes through.
		if os.IsNotExist(err) && *updateWireInventory {
			writeWireInventory(t, current)
			return
		}
		t.Fatalf("read %s: %v (bootstrap with -update-wire-inventory)", wireInventoryPath, err)
	}

	var ledger []string
	for i, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if len(strings.Split(line, "\t")) != 3 {
			t.Fatalf("%s:%d: malformed ledger line %q (want Type.Field<TAB>tag<TAB>type)", wireInventoryPath, i+1, line)
		}
		ledger = append(ledger, line)
	}

	// Removed / renamed / retyped / retagged: the ledger entry no longer matches
	// any live field. ALWAYS fatal — the updater refuses to absorb this. A wire
	// field that vanishes within an epoch strands the N-1 peer that still sends
	// or expects it (requirements §6.7: additive only inside the window).
	var dangling []string
	for _, l := range ledger {
		if !currentSet[l] {
			dangling = append(dangling, l)
		}
	}
	if len(dangling) > 0 {
		t.Fatalf("wire field inventory: %d ledger entr(ies) no longer match any live field:\n  %s\n"+
			"A wire field was deleted, renamed, re-typed, or its json tag changed. Inside a release window "+
			"that is a wire break (requirements §6.7 N-1). If this is a deliberate EPOCH change "+
			"(ProtoVersion bump, architecture §21.4), hand-edit %s and justify the shrink in the commit "+
			"message — -update-wire-inventory will not do it for you.",
			len(dangling), strings.Join(dangling, "\n  "), wireInventoryPath)
	}

	ledgerSet := make(map[string]bool, len(ledger))
	for _, l := range ledger {
		ledgerSet[l] = true
	}
	var added []string
	for _, l := range current {
		if !ledgerSet[l] {
			added = append(added, l)
		}
	}
	if len(added) > 0 {
		if *updateWireInventory {
			writeWireInventory(t, current)
			return
		}
		t.Fatalf("wire field inventory: %d new field(s) not yet in the ledger:\n  %s\n"+
			"New fields must be additive (omitempty, legal zero value — architecture §21.2), then recorded:\n"+
			"  go test ./internal/proto -run TestWireFieldInventoryAppendOnly -update-wire-inventory",
			len(added), strings.Join(added, "\n  "))
	}
}

// collectWireFields uses the AllOS AST union rather than the host's build
// selection. Local admin replies are real wire too; omitting their roots would
// let a broker-only status field disappear while the proto ledger stayed green.
// origin: hot-update S6 plan §9.1 / T46
func collectWireFields(t *testing.T) []string {
	t.Helper()
	var lines []string
	for _, source := range []struct {
		dir, namespace string
		roots          []string
	}{
		{".", "", nil},
		{"../adminsock", "adminsock.", []string{"Request", "Response", "RuntimeReport"}},
	} {
		entries, err := os.ReadDir(source.dir)
		if err != nil {
			t.Fatalf("parse wire source %s: %v", source.dir, err)
		}
		var files []*ast.File
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), source.dir+"/"+entry.Name(), nil, parser.SkipObjectResolution)
			if err != nil {
				t.Fatalf("parse wire source %s: %v", entry.Name(), err)
			}
			files = append(files, file)
		}
		fields, err := wireFieldsFromFiles(files, source.namespace, source.roots)
		if err != nil {
			t.Fatalf("collect wire source %s: %v", source.dir, err)
		}
		lines = append(lines, fields...)
	}
	sort.Strings(lines)
	return lines
}

// Only named structs reachable from actual admin requests/replies are wire
// roots. Walking field types includes slice/pointer/map elements without
// registering unrelated internal server implementation state as public wire.
func wireFieldsFromFiles(files []*ast.File, namespace string, roots []string) ([]string, error) {
	structs := map[string]*ast.StructType{}
	for _, file := range files {
		for _, decl := range file.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.TYPE {
				continue
			}
			for _, spec := range gen.Specs {
				ts, ok := spec.(*ast.TypeSpec)
				if !ok || !ts.Name.IsExported() {
					continue
				}
				if st, ok := ts.Type.(*ast.StructType); ok {
					if _, duplicate := structs[ts.Name.Name]; duplicate {
						return nil, fmt.Errorf("duplicate wire struct %s%s", namespace, ts.Name.Name)
					}
					structs[ts.Name.Name] = st
				}
			}
		}
	}
	selected := map[string]bool{}
	var visit func(string)
	visit = func(name string) {
		st, found := structs[name]
		if !found || selected[name] {
			return
		}
		selected[name] = true
		for _, field := range st.Fields.List {
			// Unexported implementation fields do not extend the wire graph.
			exported := len(field.Names) == 0
			for _, name := range field.Names {
				exported = exported || name.IsExported()
			}
			if exported {
				ast.Inspect(field.Type, func(n ast.Node) bool {
					// Imported proto types are already covered by the proto
					// source. A selector's local identifier is not a type.
					if _, selector := n.(*ast.SelectorExpr); selector {
						return false
					}
					if id, ok := n.(*ast.Ident); ok {
						visit(id.Name)
					}
					return true
				})
			}
		}
	}
	if roots == nil {
		for name := range structs {
			selected[name] = true
		}
	} else {
		for _, name := range roots {
			if _, found := structs[name]; !found {
				return nil, fmt.Errorf("missing wire root %s%s", namespace, name)
			}
			visit(name)
		}
	}
	var lines []string
	for name := range selected {
		for _, field := range structs[name].Fields.List {
			tag := "-"
			if field.Tag != nil {
				tag = strings.Trim(field.Tag.Value, "`")
			}
			typ := types.ExprString(field.Type)
			if len(field.Names) == 0 {
				// Embedded field: its wire identity is the embedded type itself.
				lines = append(lines, fmt.Sprintf("%s%s.%s\t%s\t%s", namespace, name, typ, tag, typ))
				continue
			}
			for _, fn := range field.Names {
				if fn.IsExported() {
					lines = append(lines, fmt.Sprintf("%s%s.%s\t%s\t%s", namespace, name, fn.Name, tag, typ))
				}
			}
		}
	}
	sort.Strings(lines)
	return lines, nil
}

func writeWireInventory(t *testing.T, lines []string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(wireInventoryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	header := "# Append-only wire field inventory (TestWireFieldInventoryAppendOnly).\n" +
		"# One line per exported field of an exported struct in internal/proto and reachable internal/adminsock roots:\n" +
		"#   Type.Field<TAB>struct tag<TAB>Go type\n" +
		"# Entries may be ADDED by -update-wire-inventory; removing or editing one is an\n" +
		"# epoch decision (architecture §21.4) and must be done by hand with a commit-message\n" +
		"# justification. requirements §6.7: wire changes inside a release window are additive only.\n"
	body := header + strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(wireInventoryPath, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s (%d entries)", wireInventoryPath, len(lines))
}
