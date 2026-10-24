//go:build linux || darwin

package cli_e2e_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// A worker-thread exec drops the leader's Pdeathsig. Both long-lived product
// fixtures and short build/probe/helper commands need the independent lease
// owner; Cmd.Wait also bypasses its descendant join and pidfd release.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN CLI fixture launch regression.
// gate-control: TestCLIProcessCommandDiscoveryRecognizesLaunchExpressions
func TestCLIFixturesSuperviseEveryProcess(t *testing.T) {
	paths, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("CLI fixture source discovery is empty")
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := parser.ParseFile(fset, path, body, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	operations := cliUnsupervisedProcessOperations(fset, files)
	// File subtests let disjoint owners verify their own migrations while the
	// whole gate keeps detecting launches in concurrently owned files too.
	for _, path := range paths {
		t.Run(filepath.Base(path), func(t *testing.T) {
			for _, operation := range operations {
				if strings.HasPrefix(operation, path+":") {
					t.Error(operation)
				}
			}
		})
	}
}

// Discover command factories across files as well as direct constructors. The
// real harness returns *exec.Cmd from tetherBuildCmd; scanning only assignments
// of exec.Command would silently exempt every variant/historical build.
func cliUnsupervisedProcessOperations(fset *token.FileSet, files []*ast.File) []string {
	factories := make(map[string]bool)
	aliases := make(map[*ast.File]map[string]string)
	for _, file := range files {
		imports := make(map[string]string)
		for _, imp := range file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil || (path != "os" && path != "os/exec") {
				continue
			}
			name := filepath.Base(path)
			if imp.Name != nil {
				name = imp.Name.Name
			}
			imports[name] = path
		}
		aliases[file] = imports
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Type.Results == nil {
				continue
			}
			for _, result := range fn.Type.Results.List {
				if cliCommandType(result.Type, imports) {
					factories[fn.Name.Name] = true
				}
			}
		}
	}
	var operations []string
	for _, file := range files {
		imports := aliases[file]
		commands := make(map[string]bool)
		key := func(expr ast.Expr) string {
			if field, ok := expr.(*ast.SelectorExpr); ok {
				return "." + field.Sel.Name
			}
			return types.ExprString(expr)
		}
		var commandExpr func(ast.Expr) bool
		commandExpr = func(expr ast.Expr) bool {
			if commands[key(expr)] {
				return true
			}
			switch expr := expr.(type) {
			case *ast.ParenExpr:
				return commandExpr(expr.X)
			case *ast.CallExpr:
				if fn, ok := expr.Fun.(*ast.Ident); ok {
					return factories[fn.Name]
				}
				if fn, ok := expr.Fun.(*ast.SelectorExpr); ok {
					owner, ok := fn.X.(*ast.Ident)
					return ok && imports[owner.Name] == "os/exec" && (fn.Sel.Name == "Command" || fn.Sel.Name == "CommandContext")
				}
			case *ast.UnaryExpr:
				return commandExpr(expr.X)
			case *ast.CompositeLit:
				return cliCommandType(expr.Type, imports)
			}
			return false
		}
		// Reach a fixed point so aliases and declarations remain discoverable
		// even when the source order differs from execution order.
		for changed := true; changed; {
			changed = false
			mark := func(name string) {
				if !commands[name] {
					commands[name], changed = true, true
				}
			}
			ast.Inspect(file, func(node ast.Node) bool {
				switch node := node.(type) {
				case *ast.AssignStmt:
					for i, rhs := range node.Rhs {
						if i < len(node.Lhs) && commandExpr(rhs) {
							mark(key(node.Lhs[i]))
						}
					}
				case *ast.ValueSpec:
					for i, name := range node.Names {
						if cliCommandType(node.Type, imports) || (i < len(node.Values) && commandExpr(node.Values[i])) {
							mark(name.Name)
						}
					}
				case *ast.Field:
					if cliCommandType(node.Type, imports) {
						for _, name := range node.Names {
							mark(name.Name)
							mark("." + name.Name)
						}
					}
				case *ast.KeyValueExpr:
					if commandExpr(node.Value) {
						mark("." + types.ExprString(node.Key))
					}
				}
				return true
			})
		}
		ast.Inspect(file, func(node ast.Node) bool {
			selector, ok := node.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			owner, named := selector.X.(*ast.Ident)
			direct := named && imports[owner.Name] == "os" && selector.Sel.Name == "StartProcess"
			operation := false
			switch selector.Sel.Name {
			case "Start", "Run", "Wait", "Output", "CombinedOutput":
				operation = commandExpr(selector.X)
			}
			if direct || operation {
				operations = append(operations, fmt.Sprintf("%s: unsupervised process operation %s", fset.Position(selector.Pos()), types.ExprString(selector)))
			}
			return true
		})
	}
	return operations
}

func cliCommandType(expr ast.Expr, imports map[string]string) bool {
	if pointer, ok := expr.(*ast.StarExpr); ok {
		expr = pointer.X
	}
	selector, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	owner, ok := selector.X.(*ast.Ident)
	return ok && imports[owner.Name] == "os/exec" && selector.Sel.Name == "Cmd"
}

// Syntax controls cover factories, parameters, struct fields, renamed imports,
// aliases and method values as well as os.StartProcess. Positive samples retain
// ordinary non-process Start/Wait methods and the shared supervisor entrypoints.
// origin: hot-update S6 plan §9.3 / FIX-ORPHAN CLI scanner controls.
func TestCLIProcessCommandDiscoveryRecognizesLaunchExpressions(t *testing.T) {
	for i, sample := range []struct {
		source string
		want   int
	}{
		{`func f() { cmd := exec.Command("tool"); cmd.Run() }`, 1},
		{`func f() { var cmd = exec.CommandContext(ctx, path); cmd.Wait() }`, 1},
		{`func f() { p := &fixture{cmd: exec.Command("tool")}; p.cmd.Start() }`, 1},
		{`func f() { exec.Command("tool").CombinedOutput() }`, 1},
		{`func build() *exec.Cmd { return exec.Command("go", "build") }; func f() { cmd := build(); cmd.Output() }`, 1},
		{`func f(cmd *exec.Cmd) { cmd.Start() }`, 1},
		{`type fixture struct { cmd *exec.Cmd }; func f(p *fixture) { p.cmd.Wait() }`, 1},
		{`func f() { cmd := exec.Command("tool"); alias := cmd; alias.Start() }`, 1},
		{`func f() { cmd := exec.Command("tool"); launch := cmd.Start; launch() }`, 1},
		{`func f() { cmd := (&exec.Cmd{Path: "tool"}); cmd.Start() }`, 1},
		{`func f() { os.StartProcess(path, args, attr) }`, 1},
		{`func f() { cmd := exec.Command("tool"); testharness.StartProcess(cmd) }`, 0},
		{`func f() { p := &fixture{cmd: exec.Command("tool")}; testharness.StartProcess(p.cmd) }`, 0},
		{`func f() { testharness.Output(exec.Command("tool")) }`, 0},
		{`func f() { server.Start(ctx); workers.Wait() }`, 0},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			for _, renamed := range []bool{false, true} {
				source := "package fixture; import (\"os\"; \"os/exec\"); " + sample.source
				if renamed {
					source = "package fixture; import (operating \"os\"; process \"os/exec\"); " + sample.source
					// Renaming import uses must not create a scanner escape hatch.
					source = strings.ReplaceAll(strings.ReplaceAll(source, "exec.", "process."), "os.", "operating.")
				}
				fset := token.NewFileSet()
				file, err := parser.ParseFile(fset, "fixture.go", source, 0)
				if err != nil {
					t.Fatal(err)
				}
				if got := cliUnsupervisedProcessOperations(fset, []*ast.File{file}); len(got) != sample.want {
					t.Fatalf("process operation discovery (renamed=%v): got %v, want %d violations", renamed, got, sample.want)
				}
			}
		})
	}
}
