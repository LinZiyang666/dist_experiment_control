package auth

import (
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"testing"
)

func infraBindingFunction(t *testing.T, path, name string) *ast.FuncDecl {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Name.Name == name {
			return fn
		}
	}
	t.Fatalf("missing production binding %s:%s", path, name)
	return nil
}

func bindingCall(expr ast.Expr, owner, name string) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	if owner == "" {
		id, ok := call.Fun.(*ast.Ident)
		return ok && id.Name == name
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != name {
		return false
	}
	id, ok := selector.X.(*ast.Ident)
	return ok && id.Name == owner
}

// The protocol descriptor alone cannot sign an ACL subscriber. Require the
// actual broker table and BOTH consuming loops, including the shared queue
// decision. Otherwise a dead future descriptor passes both ACL directions.
// origin: hot-update S9 plan §7.1 / T47 / WIRE-2 R14
func TestInfraManagementGrantHasProductionSubscriber(t *testing.T) {
	root := filepath.Join("..", "..")
	reexec := filepath.Join(root, "internal", "broker", "reexec.go")
	table := infraBindingFunction(t, reexec, "brokerBusinessSubscriptions")
	bound := false
	ast.Inspect(table.Body, func(node ast.Node) bool {
		literal, ok := node.(*ast.CompositeLit)
		if !ok || len(literal.Elts) != 2 {
			return true
		}
		if bindingCall(literal.Elts[0], "proto", "SubjClusterInfra") && bindingCall(literal.Elts[1], "infraattach", "InfraHandler") {
			bound = true
		}
		return true
	})
	if !bound {
		t.Fatal("management grant has no real exact-host broker handler")
	}
	for _, entry := range []struct{ path, name string }{{filepath.Join(root, "internal", "broker", "broker.go"), "Run"}, {reexec, "subscribeBrokerImageBusiness"}} {
		fn := infraBindingFunction(t, entry.path, entry.name)
		table, direct, queue := false, false, false
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if expr, ok := node.(ast.Expr); ok {
				table = table || bindingCall(expr, "", "brokerBusinessSubscriptions")
				direct = direct || bindingCall(expr, "nc", "Subscribe")
				queue = queue || bindingCall(expr, "infraattach", "QueueBusiness")
			}
			return true
		})
		if !table || !direct || !queue {
			t.Fatalf("%s lost the live direct infra table binding", entry.name)
		}
	}
}
