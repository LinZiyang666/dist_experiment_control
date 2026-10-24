//go:build linux

package hotupdate_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

// A per-task Pdeathsig cannot supervise a worker-thread exec. Keep every build,
// probe and long-lived exec.Cmd in these fixtures on the independently tested
// lease path, including commands that lack a death signal altogether.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN fixture launch regression.
func TestHotUpdateFixturesSuperviseEveryProcess(t *testing.T) {
	for _, dir := range []string{".", "../clusterharness"} {
		paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range paths {
			body, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, path, body, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, operation := range unsupervisedProcessOperations(fset, file) {
				t.Error(operation)
			}
		}
	}
}

func unsupervisedProcessOperations(fset *token.FileSet, file *ast.File) []string {
	var operations []string
	commands := make(map[string]bool)
	ast.Inspect(file, func(node ast.Node) bool {
		switch node := node.(type) {
		case *ast.AssignStmt:
			for i, rhs := range node.Rhs {
				if processCommandExpression(rhs) && i < len(node.Lhs) {
					commands[types.ExprString(node.Lhs[i])] = true
				}
			}
		case *ast.ValueSpec:
			for i, rhs := range node.Values {
				if processCommandExpression(rhs) && i < len(node.Names) {
					commands[node.Names[i].Name] = true
				}
			}
		case *ast.CompositeLit:
			for _, element := range node.Elts {
				if field, ok := element.(*ast.KeyValueExpr); ok && processCommandExpression(field.Value) {
					commands["."+types.ExprString(field.Key)] = true
				}
			}
		}
		return true
	})
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		switch method.Sel.Name {
		case "Start", "Run", "Wait", "Output", "CombinedOutput":
		default:
			return true
		}
		owned := processCommandExpression(method.X) || commands[types.ExprString(method.X)]
		if field, ok := method.X.(*ast.SelectorExpr); ok {
			owned = owned || commands["."+field.Sel.Name]
		}
		if owned {
			operations = append(operations, fmt.Sprintf("%s: unsupervised process operation %s", fset.Position(call.Pos()), types.ExprString(call)))
		}
		return true
	})
	return operations
}

func processCommandExpression(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	owner, ok := selector.X.(*ast.Ident)
	return ok && owner.Name == "exec" && (selector.Sel.Name == "Command" || selector.Sel.Name == "CommandContext")
}

// The scanner's input controls cover ordinary assignment, fixture struct fields
// and inline commands, so changing launch syntax cannot make the gate vacuous.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN scanner controls.
func TestProcessCommandDiscoveryRecognizesLaunchExpressions(t *testing.T) {
	for i, sample := range []struct {
		source string
		want   int
	}{
		{`cmd := exec.Command("tool"); cmd.Run()`, 1},
		{`var cmd = exec.CommandContext(ctx, path); cmd.Wait()`, 1},
		{`p := &fixture{Cmd: exec.Command("tool")}; p.Cmd.Start()`, 1},
		{`exec.Command("tool").CombinedOutput()`, 1},
		{`cmd := exec.Command("tool"); testharness.StartProcess(cmd)`, 0},
		{`p := &fixture{Cmd: exec.Command("tool")}; testharness.StartProcess(p.Cmd)`, 0},
		{`testharness.Output(exec.Command("tool"))`, 0},
		{`server.Start(ctx)`, 0},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			fset := token.NewFileSet()
			file, err := parser.ParseFile(fset, "fixture.go", "package fixture; func f() {"+sample.source+"}", 0)
			if err != nil {
				t.Fatal(err)
			}
			if got := unsupervisedProcessOperations(fset, file); len(got) != sample.want {
				t.Fatalf("process operation discovery: got %v, want %d violations", got, sample.want)
			}
		})
	}
}
