package architecture

import (
	"fmt"
	"go/ast"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The entry measurement is frozen independently of the per-file debt ledger.
// Tightening a legacy ceiling cannot rewrite the package's original code size.
// origin: S10 internal review ARCH-REDS R5
const brokerEntryCodeLines = 15759

func checkBrokerCodeDecrease(baseline, after int) error {
	if baseline <= 0 || after <= 0 {
		return fmt.Errorf("broker code measurement must be nonempty")
	}
	if after >= baseline || after > 15700 {
		return fmt.Errorf("broker stripped code must decrease with margin: baseline=%d after=%d target<=15700", baseline, after)
	}
	return nil
}

// Keep this broker-only oracle independent of other packages' legacy ceilings:
// an unrelated red aggregate must not hide a regression in the raw decrease.
// origin: S7 internal review BROKER-SLIM-2 F1
// gate-control: TestBrokerCodeBudgetControls
func TestBrokerCodeDecreasesBelowEntryBaseline(t *testing.T) {
	root := repoRoot(t)
	result := measureBrokerStructure(t, brokerGateSources(t, root))
	after := result.code["internal/broker"]
	t.Logf("broker stripped code: baseline=%d after=%d decrease=%d", brokerEntryCodeLines, after, brokerEntryCodeLines-after)
	if err := checkBrokerCodeDecrease(brokerEntryCodeLines, after); err != nil {
		t.Fatal(err)
	}
}

// origin: S7 internal review BROKER-SLIM-2 F1
func TestBrokerCodeBudgetControls(t *testing.T) {
	for _, tc := range []struct {
		name            string
		baseline, after int
		valid           bool
	}{
		{"reported_growth", 15759, 16169, false},
		{"unchanged", 15759, 15759, false},
		{"insufficient_margin", 15759, 15701, false},
		{"target", 15759, 15700, true},
		{"lower_entry_unchanged", 15600, 15600, false},
		{"lower_entry_decreased", 15600, 15599, true},
		{"missing_baseline", 0, 15684, false},
		{"missing_package", 15759, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkBrokerCodeDecrease(tc.baseline, tc.after); (err == nil) != tc.valid {
				t.Fatalf("checkBrokerCodeDecrease(%d, %d) = %v, valid=%v", tc.baseline, tc.after, err, tc.valid)
			}
		})
	}
}

// The entry measurement is history, while per-file ceilings are mutable debt.
// Both callers must name the independent constant so tightening that debt cannot
// rewrite the before side of the decrease proof.
// origin: S10 internal review ARCH-REDS R5
func TestBrokerCodeBaselineIgnoresMutableLedger(t *testing.T) {
	root := repoRoot(t)
	sources := map[string]string{}
	for _, name := range []string{"broker_code_budget_test.go", "structural_budget_test.go"} {
		data, err := os.ReadFile(filepath.Join(root, "test/architecture", name))
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = string(data)
	}
	if err := checkBrokerEntryBaselineSources(sources); err != nil {
		t.Fatal(err)
	}
	// This is an on-disk ledger edit in an isolated fixture, not an edit to the
	// repository's mechanical ledger. Its values deliberately cross the actual
	// measured package size, reproducing the proposed debt tightening.
	ledger := filepath.Join(t.TempDir(), "ceilings.tsv")
	for _, value := range []string{"15759", "15252"} {
		if err := os.WriteFile(ledger, []byte("internal/broker/broker.go\t"+value+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := readLegacyFileCodeCeilings(ledger); err != nil {
			t.Fatal(err)
		}
		if err := checkBrokerEntryBaselineSources(sources); err != nil {
			t.Fatalf("mutable ledger %s moved the entry baseline: %v", value, err)
		}
	}
	for _, name := range []string{"broker_code_budget_test.go", "structural_budget_test.go"} {
		mutated := make(map[string]string, len(sources))
		for path, source := range sources {
			mutated[path] = source
		}
		mutated[name] = strings.ReplaceAll(sources[name], "checkBrokerCodeDecrease(brokerEntryCodeLines,", "checkBrokerCodeDecrease(mutableLedgerTotal,")
		if checkBrokerEntryBaselineSources(mutated) == nil {
			t.Fatalf("%s accepted a mutable baseline", name)
		}
		function := "TestBrokerCodeDecreasesBelowEntryBaseline"
		if name == "structural_budget_test.go" {
			function = "TestTransferOwnershipAndBudgetsAreClosed"
		}
		header := "func " + function + "(t *testing.T) {"
		mutated[name] = strings.Replace(sources[name], header, header+"\nconst brokerEntryCodeLines = 15252", 1)
		if checkBrokerEntryBaselineSources(mutated) == nil {
			t.Fatalf("%s accepted a shadowed entry measurement", name)
		}
	}
	mutated := map[string]string{
		"broker_code_budget_test.go": strings.ReplaceAll(sources["broker_code_budget_test.go"], "brokerEntryCodeLines = 15759", "brokerEntryCodeLines = 15252"),
		"structural_budget_test.go":  sources["structural_budget_test.go"],
	}
	if checkBrokerEntryBaselineSources(mutated) == nil {
		t.Fatal("entry measurement drift escaped")
	}
}

func checkBrokerEntryBaselineSources(sources map[string]string) error {
	frozen := false
	for name, function := range map[string]string{
		"broker_code_budget_test.go": "TestBrokerCodeDecreasesBelowEntryBaseline",
		"structural_budget_test.go":  "TestTransferOwnershipAndBudgetsAreClosed",
	} {
		file, err := brokerParse(name, sources[name])
		if err != nil {
			return err
		}
		calls := 0
		var entrySpec *ast.ValueSpec
		for _, decl := range file.Decls {
			if gen, ok := decl.(*ast.GenDecl); ok && name == "broker_code_budget_test.go" && gen.Tok == token.CONST {
				for _, spec := range gen.Specs {
					value := spec.(*ast.ValueSpec)
					if len(value.Names) == 1 && value.Names[0].Name == "brokerEntryCodeLines" && len(value.Values) == 1 {
						entrySpec = value
						literal, ok := value.Values[0].(*ast.BasicLit)
						frozen = ok && literal.Kind == token.INT && literal.Value == "15759"
					}
				}
			}
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Name.Name != function {
				continue
			}
			ast.Inspect(fn.Body, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				callee, ok := call.Fun.(*ast.Ident)
				if !ok || callee.Name != "checkBrokerCodeDecrease" || len(call.Args) != 2 {
					return true
				}
				if baseline, ok := call.Args[0].(*ast.Ident); ok && baseline.Name == "brokerEntryCodeLines" &&
					(baseline.Obj == nil || baseline.Obj.Kind == ast.Con && baseline.Obj.Decl == entrySpec) {
					calls++
				}
				return true
			})
		}
		if calls != 1 {
			return fmt.Errorf("%s must check the independent broker entry baseline exactly once", function)
		}
	}
	if !frozen {
		return fmt.Errorf("broker entry measurement must be an independent constant equal to 15759")
	}
	return nil
}
