package agent

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// origin: hot-update S3 plan §6 / §7 T18. Value equality alone lets an owner
// silently fork today's budget and drift the next time the shared budget changes.
// gate-control: TestAgentBudgetReferenceControls
func TestBudgetConstantsAgreeWithTheirOwners(t *testing.T) {
	if upgradeRegisterDeadline != upgradebudget.ComponentHandover {
		t.Errorf("upgradeRegisterDeadline = %s, want upgradebudget.ComponentHandover = %s", upgradeRegisterDeadline, upgradebudget.ComponentHandover)
	}
	if upgradeBootBudget != upgradebudget.BootBudget {
		t.Errorf("upgradeBootBudget = %d, want upgradebudget.BootBudget = %d", upgradeBootBudget, upgradebudget.BootBudget)
	}
	src, err := os.ReadFile("upgrade_state.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := checkAgentBudgetReferences(string(src)); err != nil {
		t.Fatal(err)
	}
}

func checkAgentBudgetReferences(src string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "upgrade_state.go", src, 0)
	if err != nil {
		return err
	}
	budgetPackage := ""
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return err
		}
		if path == "github.com/LinZiyang666/tether/internal/upgradebudget" {
			budgetPackage = "upgradebudget"
			if imp.Name != nil {
				budgetPackage = imp.Name.Name
			}
		}
	}
	if budgetPackage == "" || budgetPackage == "_" || budgetPackage == "." {
		return fmt.Errorf("agent budgets must reference a named internal/upgradebudget import")
	}
	want := map[string]string{
		"upgradeRegisterDeadline": "ComponentHandover",
		"upgradeBootBudget":       "BootBudget",
	}
	seen := map[string]bool{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				constant, ok := want[name.Name]
				if !ok {
					continue
				}
				if seen[name.Name] || i >= len(value.Values) {
					return fmt.Errorf("%s must have one explicit upgradebudget reference", name.Name)
				}
				seen[name.Name] = true
				sel, ok := value.Values[i].(*ast.SelectorExpr)
				if ok {
					pkg, named := sel.X.(*ast.Ident)
					if named && pkg.Name == budgetPackage && sel.Sel.Name == constant {
						continue
					}
				}
				return fmt.Errorf("%s must reference upgradebudget.%s directly", name.Name, constant)
			}
		}
	}
	for _, name := range []string{"upgradeRegisterDeadline", "upgradeBootBudget"} {
		if !seen[name] {
			return fmt.Errorf("missing agent budget constant %s", name)
		}
	}
	return nil
}

// origin: hot-update S3 plan §7 T18. Exercise the same source predicate as the owner check.
func TestAgentBudgetReferenceControls(t *testing.T) {
	const source = "package agent\nimport \"github.com/LinZiyang666/tether/internal/upgradebudget\"\n" +
		"const (upgradeRegisterDeadline = upgradebudget.ComponentHandover\nupgradeBootBudget = upgradebudget.BootBudget)\n"
	for _, tc := range []struct{ name, src, reason string }{
		{"direct references", source, ""},
		{"aliased import", strings.ReplaceAll(strings.Replace(source, "import ", "import budget ", 1), "upgradebudget.", "budget."), ""},
		{"deadline drift", strings.Replace(source, "upgradebudget.ComponentHandover", "90*time.Second", 1), "upgradeRegisterDeadline must reference"},
		{"equal deadline literal", strings.Replace(source, "upgradebudget.ComponentHandover", "120*time.Second", 1), "upgradeRegisterDeadline must reference"},
		{"equal boot literal", strings.Replace(source, "upgradebudget.BootBudget", "3", 1), "upgradeBootBudget must reference"},
		{"wrong owner", strings.Replace(source, "upgradebudget.ComponentHandover", "upgradebudget.SafePointWait", 1), "upgradeRegisterDeadline must reference"},
		{"missing owner", strings.Replace(source, "upgradeBootBudget", "otherBudget", 1), "missing agent budget constant"},
		{"implicit constant", strings.Replace(source, " = upgradebudget.BootBudget", "", 1), "explicit upgradebudget reference"},
		{"wrong import", strings.Replace(source, "internal/upgradebudget", "internal/otherbudget", 1), "named internal/upgradebudget import"},
		{"variables", strings.Replace(source, "const (", "var (", 1), "missing agent budget constant"},
		{"malformed source", "package", "expected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkAgentBudgetReferences(tc.src)
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
