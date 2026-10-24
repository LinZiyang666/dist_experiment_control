package broker

import (
	"bytes"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"testing"
)

// A leaf projection test cannot certify the live cold/hot call site. Pin the
// real runtime owner argument as well as the called adapter, so a nil binding
// cannot make a disconnected Service appear installed.
// origin: hot-update S9 plan §7.1 / T48 / WIRE-2 R10
func TestBrokerInfraServiceRootsUseOwnedRuntime(t *testing.T) {
	for _, row := range []struct{ file, function, adapter, owner string }{
		{"authcallout.go", "brokerInfraConnect", "NewBrokerFromStore", "b.read().SQL()"},
		{"broker.go", "Run", "BindAdmin", "b.infra"},
		{"reexec.go", "brokerImageAdminBackend", "BindAdmin", "b.infra"},
		{"cluster_health.go", "clusterHealthResponder", "SetClusterHealth", "owner.infra"},
		{"runtime_introspect.go", "runtimeSnapshot", "SetRuntimeReport", "b.infra"},
	} {
		set := token.NewFileSet()
		file, err := parser.ParseFile(set, row.file, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != row.function {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok || len(call.Args) == 0 {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || selector.Sel.Name != row.adapter {
					return true
				}
				pkg, ok := selector.X.(*ast.Ident)
				if !ok || pkg.Name != "infraattach" {
					return true
				}
				var argument bytes.Buffer
				if err := format.Node(&argument, set, call.Args[0]); err != nil {
					t.Fatal(err)
				}
				found = argument.String() == row.owner
				return true
			})
		}
		if !found {
			t.Errorf("%s:%s has no %s binding to %s", row.file, row.function, row.adapter, row.owner)
		}
	}
}
