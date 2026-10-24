package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/spf13/cobra"
)

// origin: hot-update S3 plan §6 / §7 T18. The ctl must outwait the agent's
// deadline, and the drill's operator-facing deadline must come from that same owner.
// gate-control: TestUpgradeWaitBudgetReferenceControls
func TestBudgetConstantsAgreeWithTheirOwners(t *testing.T) {
	if want := upgradebudget.ComponentHandover + 30*time.Second; upgradeWaitBudget != want {
		t.Errorf("upgradeWaitBudget = %s, want ComponentHandover plus reconnect slack = %s", upgradeWaitBudget, want)
	}
	src, err := os.ReadFile("node.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkUpgradeWaitBudgetReferences(string(src)); err != nil {
		t.Error(err)
	}

	// Cancellation stops before the first node lookup, so this exercises the real
	// rendering without a broker, a shortened budget, or a wall-clock wait.
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	var out bytes.Buffer
	cmd := &cobra.Command{}
	cmd.SetContext(ctx)
	cmd.SetOut(&out)
	if err := waitForUpgradeCommit(cmd, nil, "lab", "actor", "n1", "v0.0.2", "v0.0.1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait must stop before node lookup: %v", err)
	}
	want := fmt.Sprintf("  waiting for re-register (deadline %ds) …\n", int(upgradebudget.ComponentHandover/time.Second))
	if out.String() != want {
		t.Errorf("deadline announcement = %q, want %q", out.String(), want)
	}
}

func checkUpgradeWaitBudgetReferences(src string) error {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "node.go", src, 0)
	if err != nil {
		return err
	}
	imports := map[string]string{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return err
		}
		name := path[strings.LastIndex(path, "/")+1:]
		if imp.Name != nil {
			name = imp.Name.Name
		}
		imports[path] = name
	}
	for _, path := range []string{"github.com/LinZiyang666/tether/internal/upgradebudget", "time", "fmt"} {
		if name := imports[path]; name == "" || name == "_" || name == "." {
			return fmt.Errorf("upgrade wait must reference a named %s import", path)
		}
	}
	budgetPackage := imports["github.com/LinZiyang666/tether/internal/upgradebudget"]
	timePackage := imports["time"]
	matches := func(expr ast.Expr, want string) bool {
		expected, err := parser.ParseExpr(want)
		if err != nil {
			return false
		}
		var gotText, wantText bytes.Buffer
		if format.Node(&gotText, fset, expr) != nil || format.Node(&wantText, token.NewFileSet(), expected) != nil {
			return false
		}
		return gotText.String() == wantText.String()
	}
	budgets, announcements := 0, 0
	for _, decl := range file.Decls {
		if gen, ok := decl.(*ast.GenDecl); ok && gen.Tok == token.VAR {
			for _, spec := range gen.Specs {
				value := spec.(*ast.ValueSpec)
				for i, name := range value.Names {
					if name.Name != "upgradeWaitBudget" {
						continue
					}
					budgets++
					if i >= len(value.Values) || !matches(value.Values[i], budgetPackage+".ComponentHandover + 30*"+timePackage+".Second") {
						return fmt.Errorf("upgradeWaitBudget must reference upgradebudget.ComponentHandover + 30*time.Second directly")
					}
				}
			}
		}
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "waitForUpgradeCommit" || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) != 3 || !matches(call.Fun, imports["fmt"]+".Fprintf") {
				return true
			}
			literal, ok := call.Args[1].(*ast.BasicLit)
			if !ok || literal.Kind != token.STRING {
				return true
			}
			text, err := strconv.Unquote(literal.Value)
			if err == nil && text == "  waiting for re-register (deadline %ds) …\n" &&
				matches(call.Args[2], "int("+budgetPackage+".ComponentHandover/"+timePackage+".Second)") {
				announcements++
			}
			return true
		})
	}
	if budgets != 1 {
		return fmt.Errorf("found %d upgradeWaitBudget declarations, want 1", budgets)
	}
	if announcements != 1 {
		return fmt.Errorf("found %d deadline announcements referencing ComponentHandover directly, want 1", announcements)
	}
	return nil
}

// origin: hot-update S3 plan §7 T18. Equal literals must fail before a later budget tune.
func TestUpgradeWaitBudgetReferenceControls(t *testing.T) {
	const source = "package main\nimport (\"fmt\"; \"time\"; \"github.com/LinZiyang666/tether/internal/upgradebudget\")\n" +
		"var upgradeWaitBudget = upgradebudget.ComponentHandover + 30*time.Second\n" +
		"func waitForUpgradeCommit() { fmt.Fprintf(out, \"  waiting for re-register (deadline %ds) …\\n\", int(upgradebudget.ComponentHandover/time.Second)) }\n"
	for _, tc := range []struct{ name, src, reason string }{
		{"direct references", source, ""},
		{"aliased budget", strings.ReplaceAll(strings.Replace(source, "\"github.com/", "budget \"github.com/", 1), "upgradebudget.", "budget."), ""},
		{"aliased time", strings.ReplaceAll(strings.Replace(source, "\"time\"", "clock \"time\"", 1), "time.", "clock."), ""},
		{"equal wait literal", strings.Replace(source, "upgradebudget.ComponentHandover + 30*time.Second", "150*time.Second", 1), "upgradeWaitBudget must reference"},
		{"equal handover literal", strings.Replace(source, "upgradebudget.ComponentHandover", "120*time.Second", 1), "upgradeWaitBudget must reference"},
		{"wrong slack", strings.Replace(source, "30*time.Second", "29*time.Second", 1), "upgradeWaitBudget must reference"},
		{"equal announcement literal", strings.Replace(source, "int(upgradebudget.ComponentHandover/time.Second)", "120", 1), "deadline announcements referencing"},
		{"wrong announcement owner", strings.Replace(source, "int(upgradebudget.ComponentHandover/time.Second)", "int(upgradeWaitBudget/time.Second)", 1), "deadline announcements referencing"},
		{"missing announcement", strings.Replace(source, "waiting for re-register", "waiting", 1), "deadline announcements referencing"},
		{"missing wait budget", strings.Replace(source, "upgradeWaitBudget", "otherBudget", 1), "upgradeWaitBudget declarations"},
		{"wrong import", strings.Replace(source, "internal/upgradebudget", "internal/otherbudget", 1), "named github.com/LinZiyang666/tether/internal/upgradebudget import"},
		{"malformed source", "package", "expected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkUpgradeWaitBudgetReferences(tc.src)
			if tc.reason == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("got %v, want %q", err, tc.reason)
			}
		})
	}
}
