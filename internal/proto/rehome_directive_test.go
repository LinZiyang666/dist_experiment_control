package proto

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRehomeDirectiveHasNoLivePublisher backs the claim made in
// RehomeDirective's doc comment (messages.go).
//
// Batch-A A4. That doc has asserted since review A5 M5 that "a guard test
// asserts it has no live publisher so a half-wiring is caught" — but no such
// test existed. The type is defined ahead of D7 purely to keep the v2 wire
// stable; if someone half-wires the backup path (constructs and publishes a
// RehomeDirective without the rest of D7 landing), the doc promised that would
// be caught, and it would not have been.
//
// A promised guard is worse than an acknowledged gap: a reviewer who reads the
// sentence stops looking for the hole themselves.
//
// When D7 does wire the backup rehome path, this test SHOULD start failing.
// That is the signal to delete it together with the sentence in messages.go —
// not to add an exemption.
func TestRehomeDirectiveHasNoLivePublisher(t *testing.T) {
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root = filepath.Dir(filepath.Dir(root)) // internal/proto -> repo root
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repo root %q has no go.mod: %v", root, err)
	}

	var sites []string
	fset := token.NewFileSet()
	for _, dir := range []string{"internal", "cmd"} {
		walkErr := filepath.Walk(filepath.Join(root, dir), func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return nil
			}
			f, perr := parser.ParseFile(fset, p, nil, 0)
			if perr != nil {
				return nil
			}
			ast.Inspect(f, func(n ast.Node) bool {
				cl, ok := n.(*ast.CompositeLit)
				if !ok {
					return true
				}
				var name string
				switch tv := cl.Type.(type) {
				case *ast.Ident:
					name = tv.Name
				case *ast.SelectorExpr:
					name = tv.Sel.Name
				default:
					return true
				}
				if name != "RehomeDirective" {
					return true
				}
				rel, _ := filepath.Rel(root, p)
				sites = append(sites, filepath.ToSlash(rel)+":"+itoaLocal(fset.Position(cl.Pos()).Line))
				return true
			})
			return nil
		})
		if walkErr != nil {
			t.Fatalf("walk %s: %v", dir, walkErr)
		}
	}

	if len(sites) > 0 {
		t.Errorf("RehomeDirective is constructed in production code at %v.\n"+
			"Either D7 has landed — in which case delete this test AND the sentence in messages.go that "+
			"points at it — or the backup rehome path has been half-wired, which is exactly what this "+
			"guard exists to catch.", sites)
	}

	// Non-vacuity: a scanner that matched nothing would pass silently forever.
	// Prove it can actually see a composite literal of this type.
	const sample = `package x
type RehomeDirective struct{}
func f() RehomeDirective { return RehomeDirective{} }`
	sfset := token.NewFileSet()
	sf, err := parser.ParseFile(sfset, "sample.go", sample, 0)
	if err != nil {
		t.Fatalf("parse self-check sample: %v", err)
	}
	found := false
	ast.Inspect(sf, func(n ast.Node) bool {
		if cl, ok := n.(*ast.CompositeLit); ok {
			if id, ok := cl.Type.(*ast.Ident); ok && id.Name == "RehomeDirective" {
				found = true
			}
		}
		return true
	})
	if !found {
		t.Error("SELF-CHECK FAILED: the scanner cannot detect a RehomeDirective composite literal even in a " +
			"synthetic sample, so its clean result above means nothing")
	}
}

func itoaLocal(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
