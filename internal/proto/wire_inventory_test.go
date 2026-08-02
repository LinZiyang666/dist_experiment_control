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
// exported struct in this package: one line per field, `Type.Field<TAB>tag<TAB>type`.
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

// collectWireFields parses every non-test .go file of this package and returns
// one sorted line per exported field of an exported struct type:
// `Type.Field<TAB>tag<TAB>type` (embedded fields use the embedded type name).
func collectWireFields(t *testing.T) []string {
	t.Helper()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, parser.SkipObjectResolution)
	if err != nil {
		t.Fatalf("parse package proto: %v", err)
	}

	var lines []string
	for _, pkg := range pkgs {
		fileNames := make([]string, 0, len(pkg.Files))
		for name := range pkg.Files {
			fileNames = append(fileNames, name)
		}
		sort.Strings(fileNames)
		for _, name := range fileNames {
			ast.Inspect(pkg.Files[name], func(n ast.Node) bool {
				ts, ok := n.(*ast.TypeSpec)
				if !ok || !ts.Name.IsExported() {
					return true
				}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					return true
				}
				for _, f := range st.Fields.List {
					tag := "-"
					if f.Tag != nil {
						tag = strings.Trim(f.Tag.Value, "`")
					}
					typeStr := types.ExprString(f.Type)
					if len(f.Names) == 0 {
						// Embedded field: its wire identity is the embedded type itself.
						lines = append(lines, fmt.Sprintf("%s.%s\t%s\t%s", ts.Name.Name, typeStr, tag, typeStr))
						continue
					}
					for _, fn := range f.Names {
						if !fn.IsExported() {
							continue
						}
						lines = append(lines, fmt.Sprintf("%s.%s\t%s\t%s", ts.Name.Name, fn.Name, tag, typeStr))
					}
				}
				return true
			})
		}
	}
	sort.Strings(lines)
	return lines
}

func writeWireInventory(t *testing.T, lines []string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(wireInventoryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	header := "# Append-only wire field inventory (TestWireFieldInventoryAppendOnly).\n" +
		"# One line per exported field of an exported struct in internal/proto:\n" +
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
