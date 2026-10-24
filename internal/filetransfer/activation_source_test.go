package filetransfer

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Scan the landed composition roots, including files introduced after P20's
// original inventory. The capable agent must never regain SDK Put through
// a slim move, alias, or the legacy carrier adapter.
// origin: hot-update S7 plan §7 / T49 / M01
func TestTransferAgentCompositionUsesOnlyFixedVersionWriters(t *testing.T) {
	writers, imports := 0, 0
	for _, root := range []string{".", "../xferagent"} {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
				continue
			}
			path := filepath.Join(root, entry.Name())
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			ast.Inspect(file, func(node ast.Node) bool {
				if selector, ok := node.(*ast.SelectorExpr); ok {
					if selector.Sel.Name == "Put" || selector.Sel.Name == "PutLegacyOnce" && (root != "." || entry.Name() != "legacy.go") {
						t.Errorf("%s: SDK Put outside the explicit one-shot cap0 adapter", path)
					}
					if selector.Sel.Name == "NewTransferWriter" {
						writers++
					}
					if selector.Sel.Name == "ImportTransferWriter" {
						imports++
					}
				}
				return true
			})
		}
	}
	// The explicit INTEG-G factory and the production runtime each construct
	// a fixed-version writer; only the production image runtime imports one.
	if writers != 2 || imports != 1 {
		t.Fatalf("fixed-version writer construction/import lost: %d/%d", writers, imports)
	}
}
