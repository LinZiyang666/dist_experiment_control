package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

// Keep the converted writers independently runnable while the lead retires
// stale ratchet rows. Every named authority writer must select its DB role;
// moving the implementation must not reintroduce the dual-purpose raw handle.
// origin: S7 internal review GATES-PROD F5
func TestAuthorityMutationsUseNamedDBRoles(t *testing.T) {
	owners := map[string][]string{
		"clusterwrite.go":    {"freePortAllocation", "revokePortAllocation", "tombstoneMutationAuthority"},
		"cluster_forward.go": {"markProcExitAuthority"},
		"reconcile.go":       {"refileReconcileAuthority", "cascadeSessionSingleAuthority"},
	}
	for path, names := range owners {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(repoRoot(t), "internal/broker", path), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range names {
			found := false
			for _, decl := range file.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Name.Name != name {
					continue
				}
				found = true
				if n := countCfgDB(fn); n != 0 {
					t.Errorf("%s:%s bypasses named DB role with %d raw handle sites", path, name, n)
				}
				guarded := false
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					if call, ok := node.(*ast.CallExpr); ok {
						if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "singleWriter" {
							guarded = true
						}
					}
					return true
				})
				if !guarded {
					t.Errorf("%s:%s lost its single-mode writer selection", path, name)
				}
			}
			if !found {
				t.Errorf("authority writer disappeared: %s:%s", path, name)
			}
		}
	}
}
