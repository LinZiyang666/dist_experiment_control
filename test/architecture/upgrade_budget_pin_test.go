package architecture

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

// upgrade_budget_pin_test.go — the drills and replay owners must read the same budgets.
// A shell assignment cannot import a Go constant, so reconcile every definition; a missing
// or unparseable value is a blind gate, never agreement. Workload can import the constants:
// check that reference as well as its value, or re-literalizing today's number passes until
// the next budget change. The file is read as data, never sourced or executed.
// origin: hot-update S3 plan §6 / §9 (docs/reviews/hot-update-agent-bootstrap-plan.md).
// gate-control: TestUpgradeBudgetPinControls

const upgradeBudgetShellPath = "test/simcluster/drills/lib/hot_update_budget.sh"

var upgradeBudgetAssignmentRe = regexp.MustCompile(`^(HOT_UPDATE_[A-Z0-9_]+)=([0-9]+)$`)

var upgradeShellBudgets = map[string]int64{
	// The Go definition is owned by P33; TestBrokerBudgetsKeepFrozenValuesAndDerivation
	// resolves its expression rather than importing a compile-stub constant.
	// origin: hot-update S6 plan §13 / T53.
	"HOT_UPDATE_BROKER_MAINTENANCE_TTL_S": 8,
	// origin: hot-update S4 plan §2.3 / §10 / T47 — recovery never renews authority.
	"HOT_UPDATE_PTY_INPUT_BYTES":            upgradebudget.PTYInputBytes,
	"HOT_UPDATE_PTY_BATCH_BYTES":            upgradebudget.PTYBatchBytes,
	"HOT_UPDATE_PTY_RECOVERY_S":             int64(upgradebudget.PTYRecovery / time.Second),
	"HOT_UPDATE_PTY_RECOVERY_OPPORTUNITY_S": int64(upgradebudget.PTYRecoveryOpportunity / time.Second),
	"HOT_UPDATE_LIVE_FLOW_STALL_S":          int64(upgradebudget.LiveFlowStall / time.Second),
	"HOT_UPDATE_FREEZE_MS":                  int64(upgradebudget.LocalFreezeWindow / time.Millisecond),
	"HOT_UPDATE_REQUEST_S":                  int64(upgradebudget.RequestLatency / time.Second),
	"HOT_UPDATE_PROBE_RATE":                 upgradebudget.ProbeRatePerSecond,
	"HOT_UPDATE_REGISTER_DEADLINE_S":        int64(upgradebudget.ComponentHandover / time.Second),
	// origin: hot-update S7 plan §1.2 / §11.2 / T51
	"HOT_UPDATE_TRANSFER_PROGRESS_S": int64(upgradebudget.TransferProgress / time.Second),
	"HOT_UPDATE_SAFE_POINT_S":        int64(upgradebudget.SafePointWait / time.Second),
	// Reconcile the privileged IPC mirrors in the complete shell inventory too.
	// origin: S7 internal review GATE-SYNC F2 / hot-update S9 plan §10.2 / T83
	"HOT_UPDATE_INFRA_READ_S":      int64(upgradebudget.InfraReadBudget / time.Second),
	"HOT_UPDATE_INFRA_ACTION_S":    int64(upgradebudget.InfraActionBudget / time.Second),
	"HOT_UPDATE_INFRA_WORKER_S":    int64(upgradebudget.InfraWorkerBudget / time.Second),
	"HOT_UPDATE_INFRA_MAX_WORKERS": upgradebudget.InfraMaxWorkers,
}

func checkUpgradeShellBudgets(src string) error {
	seen := map[string]bool{}
	for i, line := range strings.Split(src, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m := upgradeBudgetAssignmentRe.FindStringSubmatch(line)
		if m == nil {
			return fmt.Errorf("line %d: expected NAME=<int> assignment, got %q", i+1, line)
		}
		name := m[1]
		want, ok := upgradeShellBudgets[name]
		if !ok {
			return fmt.Errorf("line %d: unreconciled budget %s; add its upgradebudget mapping", i+1, name)
		}
		if seen[name] {
			return fmt.Errorf("line %d: duplicate budget %s", i+1, name)
		}
		seen[name] = true
		got, err := strconv.ParseInt(m[2], 10, 64)
		if err != nil {
			return fmt.Errorf("line %d: invalid integer for %s: %w", i+1, name, err)
		}
		if got != want {
			return fmt.Errorf("%s = %d, upgradebudget requires %d", name, got, want)
		}
	}
	var missing []string
	for name := range upgradeShellBudgets {
		if !seen[name] {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("missing budget assignments: %s", strings.Join(missing, ", "))
	}
	return nil
}

// origin: hot-update S3 plan §6 / §9 (docs/reviews/hot-update-agent-bootstrap-plan.md).
func TestUpgradeBudgetsAgreeAcrossGoAndDrill(t *testing.T) {
	src, err := os.ReadFile(filepath.Join(repoRoot(t), upgradeBudgetShellPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkUpgradeShellBudgets(string(src)); err != nil {
		t.Fatalf("%s: %v", upgradeBudgetShellPath, err)
	}
}

func checkReplayBudgetReferences(src string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "replay.go", src, 0)
	if err != nil {
		return err
	}
	budgetPackage := ""
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return err
		}
		if path == modPrefix+"internal/upgradebudget" {
			budgetPackage = "upgradebudget"
			if imp.Name != nil {
				budgetPackage = imp.Name.Name
			}
		}
	}
	if budgetPackage == "" || budgetPackage == "_" || budgetPackage == "." {
		return fmt.Errorf("replay defaults must reference a named internal/upgradebudget import")
	}
	want := map[string]string{
		"DefaultReplayCapacity": "StreamReplayBytes",
		"DefaultReplayBudget":   "NodeReplayBytes",
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
	for _, name := range []string{"DefaultReplayCapacity", "DefaultReplayBudget"} {
		if !seen[name] {
			return fmt.Errorf("missing replay constant %s", name)
		}
	}
	return nil
}

// origin: hot-update S3 plan §6, revision 5 (docs/reviews/hot-update-agent-bootstrap-plan.md).
func TestReplayDefaultsReferenceUpgradeBudgets(t *testing.T) {
	if workload.DefaultReplayCapacity != upgradebudget.StreamReplayBytes {
		t.Errorf("workload.DefaultReplayCapacity = %d, upgradebudget.StreamReplayBytes = %d",
			workload.DefaultReplayCapacity, upgradebudget.StreamReplayBytes)
	}
	if workload.DefaultReplayBudget != upgradebudget.NodeReplayBytes {
		t.Errorf("workload.DefaultReplayBudget = %d, upgradebudget.NodeReplayBytes = %d",
			workload.DefaultReplayBudget, upgradebudget.NodeReplayBytes)
	}
	const rel = "internal/workload/replay.go"
	src, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkReplayBudgetReferences(string(src)); err != nil {
		t.Fatalf("%s: %v", rel, err)
	}
}

// Both controls call the same predicates as the repository gates. Build the agreeing
// shell from the Go budgets so tuning a budget cannot accidentally turn a negative
// fixture into agreement; drift and deletion exercise each mapping independently.
// origin: hot-update S3 plan §6 / §9 (docs/reviews/hot-update-agent-bootstrap-plan.md).
func TestUpgradeBudgetPinControls(t *testing.T) {
	var assignments []string
	for name, value := range upgradeShellBudgets {
		assignments = append(assignments, fmt.Sprintf("%s=%d\n", name, value))
	}
	sort.Strings(assignments)
	agreeing := "#!/bin/sh\n# Budget definitions only.\n\n" + strings.Join(assignments, "")
	t.Run("agreeing shell", func(t *testing.T) {
		if err := checkUpgradeShellBudgets(agreeing); err != nil {
			t.Fatal(err)
		}
	})
	for _, assignment := range assignments {
		name, _, _ := strings.Cut(assignment, "=")
		for _, tc := range []struct{ name, replacement, reason string }{
			{"drift", fmt.Sprintf("%s=%d\n", name, upgradeShellBudgets[name]+1), "upgradebudget requires"},
			{"missing", "", "missing budget assignments"},
			{"non-integer", name + "=oops\n", "expected NAME=<int>"},
			{"quoted", name + "=\"5\"\n", "expected NAME=<int>"},
			{"expression", name + "=$((2+3))\n", "expected NAME=<int>"},
			{"trailing command", strings.TrimSpace(assignment) + "; true\n", "expected NAME=<int>"},
			{"overflow", name + "=999999999999999999999999\n", "invalid integer"},
			{"duplicate", assignment + assignment, "duplicate budget"},
		} {
			t.Run(name+"/"+tc.name, func(t *testing.T) {
				src := strings.Replace(agreeing, assignment, tc.replacement, 1)
				if err := checkUpgradeShellBudgets(src); err == nil || !strings.Contains(err.Error(), tc.reason) {
					t.Fatalf("got %v, want %q", err, tc.reason)
				}
			})
		}
	}
	for _, tc := range []struct{ name, src, reason string }{
		{"empty", "", "missing budget assignments"},
		{"unreconciled", agreeing + "HOT_UPDATE_NEW_BUDGET=1\n", "unreconciled budget"},
		{"unreconciled malformed", agreeing + "HOT_UPDATE_NEW_BUDGET=oops\n", "expected NAME=<int>"},
		{"exported assignment", agreeing + "export HOT_UPDATE_NEW_BUDGET=1\n", "expected NAME=<int>"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := checkUpgradeShellBudgets(tc.src); err == nil || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("got %v, want %q", err, tc.reason)
			}
		})
	}

	const replay = "package workload\nimport \"github.com/LinZiyang666/tether/internal/upgradebudget\"\n" +
		"const (\nDefaultReplayCapacity int64 = upgradebudget.StreamReplayBytes\n" +
		"DefaultReplayBudget int64 = upgradebudget.NodeReplayBytes\n)\n"
	for _, tc := range []struct{ name, src, reason string }{
		{"agreeing replay", replay, ""},
		{"aliased import", strings.ReplaceAll(strings.Replace(replay, "import ", "import budget ", 1), "upgradebudget.", "budget."), ""},
		{"literal capacity", strings.Replace(replay, "upgradebudget.StreamReplayBytes", "8 << 20", 1), "DefaultReplayCapacity must reference"},
		{"literal budget", strings.Replace(replay, "upgradebudget.NodeReplayBytes", "256 << 20", 1), "DefaultReplayBudget must reference"},
		{"wrong constant", strings.Replace(replay, "upgradebudget.NodeReplayBytes", "upgradebudget.StreamReplayBytes", 1), "DefaultReplayBudget must reference"},
		{"missing constant", strings.Replace(replay, "DefaultReplayBudget", "OtherBudget", 1), "missing replay constant"},
		{"implicit constant", strings.Replace(replay, " int64 = upgradebudget.NodeReplayBytes", "", 1), "explicit upgradebudget reference"},
		{"wrong import", strings.Replace(replay, "internal/upgradebudget", "internal/otherbudget", 1), "named internal/upgradebudget import"},
		{"variables", strings.Replace(replay, "const (", "var (", 1), "missing replay constant"},
		{"malformed source", "package", "expected"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := checkReplayBudgetReferences(tc.src)
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

// First detach is absolute. At the worst tick phase the first failure counts
// immediately, leaving (dwell-1)*tick before rotation. Comparing with dwell*tick
// would wrongly admit a ten-second grace at today's three five-second ticks.
// origin: hot-update S5 plan §4.5 / §10.1 / T49.
// gate-control: TestFlowSSBudgetControls
func TestFlowSSBudgetsKeepDetachedGraceAndReplaySource(t *testing.T) {
	root := repoRoot(t)
	constants := map[string]ast.Expr{}
	for _, rel := range []string{"internal/broker/proxy_reconcile.go", "internal/broker/observability.go", "internal/brokercontrol/clusterstatus.go"} {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, rel), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			group, ok := decl.(*ast.GenDecl)
			if !ok || group.Tok != token.CONST {
				continue
			}
			for _, spec := range group.Specs {
				value := spec.(*ast.ValueSpec)
				for i, name := range value.Names {
					if i < len(value.Values) {
						constants[name.Name] = value.Values[i]
					}
				}
			}
		}
	}
	dwell, err := flowSSIntegerConstant(constants["proxyRehomeDwell"])
	if err != nil {
		t.Fatal(err)
	}
	// origin: hot-update S6 plan §9.3; the alias must still bind the measured control tick.
	alias, ok := constants["observeTickInterval"].(*ast.SelectorExpr)
	if !ok || alias.Sel.Name != "ObserveTickInterval" {
		t.Fatal("broker observation tick no longer follows the control owner")
	}
	tick, err := flowSSDurationConstant(constants["ObserveTickInterval"])
	if err != nil {
		t.Fatal(err)
	}
	if err := checkFlowSSGrace(tunnel.DetachedGrace, dwell, tick); err != nil {
		t.Fatal(err)
	}
	if upgradebudget.StreamReplayBytes != 8<<20 || upgradebudget.NodeReplayBytes != 256<<20 {
		t.Fatal("frozen replay capacities drifted")
	}
	t.Run("flow-replay-reference", func(t *testing.T) {
		sites := 0
		for name, file := range readFlowSSSources(t, root, "internal/flow", false) {
			count, err := checkFlowSSReplayReferences(file)
			sites += count
			if err != nil {
				t.Errorf("internal/flow/%s: %v", name, err)
			}
		}
		if sites == 0 {
			t.Fatal("pending P1/P4: no actual flow replay capacity reference found")
		}
	})
}
func checkFlowSSGrace(grace time.Duration, dwell int64, tick time.Duration) error {
	if dwell < 2 || tick <= 0 || grace <= 0 || grace >= time.Duration(dwell-1)*tick {
		return fmt.Errorf("detached grace %s must be positive and below earliest rehome (%d-1)*%s", grace, dwell, tick)
	}
	return nil
}
func flowSSIntegerConstant(expr ast.Expr) (int64, error) {
	value, ok := expr.(*ast.BasicLit)
	if !ok || value.Kind != token.INT {
		return 0, fmt.Errorf("expected explicit integer constant")
	}
	return strconv.ParseInt(value.Value, 0, 64)
}
func flowSSDurationConstant(expr ast.Expr) (time.Duration, error) {
	product, ok := expr.(*ast.BinaryExpr)
	if !ok || product.Op != token.MUL {
		return 0, fmt.Errorf("expected duration constant product")
	}
	n, err := flowSSIntegerConstant(product.X)
	if err != nil {
		return 0, err
	}
	unit, ok := product.Y.(*ast.SelectorExpr)
	if !ok {
		return 0, fmt.Errorf("expected time unit")
	}
	pkg, ok := unit.X.(*ast.Ident)
	if !ok || pkg.Name != "time" {
		return 0, fmt.Errorf("expected time package")
	}
	switch unit.Sel.Name {
	case "Second":
		return time.Duration(n) * time.Second, nil
	case "Millisecond":
		return time.Duration(n) * time.Millisecond, nil
	}
	return 0, fmt.Errorf("unsupported duration unit")
}

// Follow package-local const aliases, but never accept a copied arithmetic
// literal. Equal values today are not proof of a shared source tomorrow.
func checkFlowSSReplayReferences(file *ast.File) (int, error) {
	imports := map[string]string{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return 0, err
		}
		name := filepath.Base(path)
		if imp.Name != nil {
			name = imp.Name.Name
		}
		imports[name] = path
	}
	constants := map[string]ast.Expr{}
	for _, decl := range file.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok || group.Tok != token.CONST {
			continue
		}
		for _, spec := range group.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if i < len(value.Values) {
					constants[name.Name] = value.Values[i]
				}
			}
		}
	}
	var reference func(ast.Expr, map[string]bool) bool
	reference = func(expr ast.Expr, seen map[string]bool) bool {
		if id, ok := expr.(*ast.Ident); ok {
			if seen[id.Name] {
				return false
			}
			seen[id.Name] = true
			return reference(constants[id.Name], seen)
		}
		selector, ok := expr.(*ast.SelectorExpr)
		if !ok {
			return false
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok {
			return false
		}
		return imports[pkg.Name] == modPrefix+"internal/upgradebudget" && selector.Sel.Name == "StreamReplayBytes" || imports[pkg.Name] == modPrefix+"internal/workload" && selector.Sel.Name == "DefaultReplayCapacity"
	}
	count := 0
	var failure error
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || selector.Sel.Name != "NewReplay" {
			return true
		}
		pkg, ok := selector.X.(*ast.Ident)
		if !ok || imports[pkg.Name] != modPrefix+"internal/workload" {
			return true
		}
		count++
		if len(call.Args) != 2 || !reference(call.Args[1], map[string]bool{}) {
			failure = fmt.Errorf("flow replay capacity must reference upgradebudget.StreamReplayBytes or workload.DefaultReplayCapacity")
		}
		return true
	})
	return count, failure
}

// origin: hot-update S5 plan §10.1 / T49.
func TestFlowSSBudgetControls(t *testing.T) {
	if checkFlowSSGrace(8*time.Second, 3, 5*time.Second) != nil {
		t.Fatal("valid grace rejected")
	}
	for _, grace := range []time.Duration{0, 10 * time.Second, 15 * time.Second} {
		if checkFlowSSGrace(grace, 3, 5*time.Second) == nil {
			t.Fatal("unsafe detached grace accepted")
		}
	}
	const prefix = `package flow;import ("github.com/LinZiyang666/tether/internal/workload";budget "github.com/LinZiyang666/tether/internal/upgradebudget");`
	for _, tc := range []struct {
		capacity string
		valid    bool
	}{{"budget.StreamReplayBytes", true}, {"workload.DefaultReplayCapacity", true}, {"8 << 20", false}, {"budget.NodeReplayBytes", false}, {"capacity", true}} {
		file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", prefix+`const capacity=budget.StreamReplayBytes;func newLog(){workload.NewReplay(shared,`+tc.capacity+`)}`, 0)
		if err != nil {
			t.Fatal(err)
		}
		count, err := checkFlowSSReplayReferences(file)
		if count != 1 || (err == nil) != tc.valid {
			t.Fatalf("capacity %s: sites=%d err=%v", tc.capacity, count, err)
		}
	}
}

// The maintenance lease is derived from the request budget, never a watchdog
// window. Pin values as well as references: changing Go and shell together
// cannot widen an acceptance window without changing the frozen contract.
// origin: hot-update S6 plan §7 / §13 / T53
// gate-control: TestBrokerBudgetControls
const brokerMaintenanceShellBudget = "HOT_UPDATE_BROKER_MAINTENANCE_TTL_S"

func checkBrokerBudgetSource(source string) error {
	file, err := brokerParse("budget.go", source)
	if err != nil {
		return err
	}
	imports := brokerImports(file)
	expressions := map[string]ast.Expr{}
	for _, decl := range file.Decls {
		group, ok := decl.(*ast.GenDecl)
		if !ok || group.Tok != token.CONST {
			continue
		}
		for _, spec := range group.Specs {
			value := spec.(*ast.ValueSpec)
			for i, name := range value.Names {
				if i < len(value.Values) {
					expressions[name.Name] = value.Values[i]
				}
			}
		}
	}
	var evaluate func(ast.Expr, map[string]bool) (int64, error)
	evaluate = func(expr ast.Expr, seen map[string]bool) (int64, error) {
		switch value := expr.(type) {
		case *ast.BasicLit:
			if value.Kind == token.INT {
				return strconv.ParseInt(value.Value, 0, 64)
			}
		case *ast.ParenExpr:
			return evaluate(value.X, seen)
		case *ast.Ident:
			if seen[value.Name] {
				return 0, fmt.Errorf("cyclic budget constant")
			}
			seen[value.Name] = true
			return evaluate(expressions[value.Name], seen)
		case *ast.SelectorExpr:
			pkg, ok := value.X.(*ast.Ident)
			if !ok || imports[pkg.Name] != "time" {
				break
			}
			switch value.Sel.Name {
			case "Second":
				return int64(time.Second), nil
			case "Millisecond":
				return int64(time.Millisecond), nil
			}
		case *ast.BinaryExpr:
			// Each branch gets its own ancestry: RequestLatency+RequestLatency is
			// legal arithmetic, while an actual self-reference must fail closed.
			leftSeen, rightSeen := map[string]bool{}, map[string]bool{}
			for name, present := range seen {
				leftSeen[name] = present
				rightSeen[name] = present
			}
			left, err := evaluate(value.X, leftSeen)
			if err != nil {
				return 0, err
			}
			right, err := evaluate(value.Y, rightSeen)
			if err != nil {
				return 0, err
			}
			switch value.Op {
			case token.ADD:
				return left + right, nil
			case token.MUL:
				return left * right, nil
			}
		}
		return 0, fmt.Errorf("unresolved budget expression")
	}
	for name, want := range map[string]int64{"RequestLatency": int64(5 * time.Second), "LiveFlowStall": int64(5 * time.Second), "LocalFreezeWindow": int64(500 * time.Millisecond), "ProbeRatePerSecond": 10, "BrokerMaintenanceTTL": int64(8 * time.Second)} {
		got, err := evaluate(expressions[name], map[string]bool{name: true})
		if err != nil || got != want {
			return fmt.Errorf("%s: got %d (%v), frozen budget %d", name, got, err, want)
		}
	}
	// Preserve the dependency, not merely today's equal eight-second value.
	ttl, ok := expressions["BrokerMaintenanceTTL"].(*ast.BinaryExpr)
	if !ok || ttl.Op != token.ADD {
		return fmt.Errorf("BrokerMaintenanceTTL must derive from RequestLatency + 3s")
	}
	request, ok := ttl.X.(*ast.Ident)
	margin, err := evaluate(ttl.Y, map[string]bool{})
	if !ok || request.Name != "RequestLatency" || err != nil || margin != int64(3*time.Second) {
		return fmt.Errorf("BrokerMaintenanceTTL must derive from RequestLatency + 3s")
	}
	return nil
}

func checkBrokerBudgetShell(source string) error {
	count := 0
	for _, line := range strings.Split(source, "\n") {
		name, _, found := strings.Cut(strings.TrimSpace(line), "=")
		if !found || name != brokerMaintenanceShellBudget {
			continue
		}
		match := upgradeBudgetAssignmentRe.FindStringSubmatch(strings.TrimSpace(line))
		if match == nil || match[2] != "8" {
			return fmt.Errorf("%s must be explicit 8 seconds", brokerMaintenanceShellBudget)
		}
		count++
	}
	if count != 1 {
		return fmt.Errorf("%s: got %d assignments, want one", brokerMaintenanceShellBudget, count)
	}
	return checkUpgradeShellBudgets(source)
}

// origin: hot-update S6 plan §13 / T53
func TestBrokerBudgetsKeepFrozenValuesAndDerivation(t *testing.T) {
	root := repoRoot(t)
	source, err := os.ReadFile(filepath.Join(root, "internal/upgradebudget/budget.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerBudgetSource(string(source)); err != nil {
		t.Errorf("NOT_RUN pending P33 budget constant: %v", err)
	}
	shell, err := os.ReadFile(filepath.Join(root, upgradeBudgetShellPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkBrokerBudgetShell(string(shell)); err != nil {
		t.Errorf("NOT_RUN pending P33 shell budget: %v", err)
	}
}

// origin: hot-update S6 plan §11 / T53
func TestBrokerBudgetControls(t *testing.T) {
	good := `package upgradebudget;import clock "time";const(RequestLatency=5*clock.Second;LiveFlowStall=5*clock.Second;LocalFreezeWindow=500*clock.Millisecond;ProbeRatePerSecond=10;BrokerMaintenanceTTL=RequestLatency+3*clock.Second)`
	if err := checkBrokerBudgetSource(good); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ old, replacement string }{
		{"5*clock.Second", "6*clock.Second"}, {"LiveFlowStall=5", "LiveFlowStall=6"}, {"500*clock.Millisecond", "600*clock.Millisecond"}, {"ProbeRatePerSecond=10", "ProbeRatePerSecond=9"},
		{"RequestLatency+3*clock.Second", "8*clock.Second"}, {"RequestLatency+3*clock.Second", "RequestLatency+4*clock.Second"}, {"BrokerMaintenanceTTL=", "OtherTTL="},
	} {
		if checkBrokerBudgetSource(strings.Replace(good, tc.old, tc.replacement, 1)) == nil {
			t.Fatalf("budget mutation escaped %v", tc)
		}
	}
	shell := ""
	for name, value := range upgradeShellBudgets {
		shell += fmt.Sprintf("%s=%d\n", name, value)
	}

	if err := checkBrokerBudgetShell(shell); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(shell, "_TTL_S=8", "_TTL_S=9", 1), strings.Replace(shell, brokerMaintenanceShellBudget+"=8\n", "", 1), shell + brokerMaintenanceShellBudget + "=8\n", strings.Replace(shell, "_TTL_S=8", "_TTL_S=$((5+3))", 1)} {
		if checkBrokerBudgetShell(bad) == nil {
			t.Fatal("shell maintenance budget defect escaped")
		}
	}
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "internal/upgradebudget/budget.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	if !strings.Contains(source, "BrokerMaintenanceTTL") {
		source += "\nconst BrokerMaintenanceTTL = RequestLatency + 3*time.Second\n"
	}
	if err := checkBrokerBudgetSource(source); err != nil {
		t.Fatal(err)
	}
	if checkBrokerBudgetSource(strings.Replace(source, "RequestLatency + 3*time.Second", "8*time.Second", 1)) == nil {
		t.Fatal("real-family literal TTL mutation escaped")
	}
}

// origin: hot-update S7 plan §1.2 / T51
// gate-control: TestTransferProgressBudgetControls
func TestTransferProgressBudgetControls(t *testing.T) {
	if upgradebudget.TransferProgress != 10*time.Second || upgradeShellBudgets["HOT_UPDATE_TRANSFER_PROGRESS_S"] != 10 {
		t.Fatal("transfer durable progress budget must remain 10 seconds")
	}
	var source strings.Builder
	for name, value := range upgradeShellBudgets {
		fmt.Fprintf(&source, "%s=%d\n", name, value)
	}
	root := transferFixtureTree(t, map[string]string{})
	path := "budget.sh"
	for _, tc := range []struct {
		text string
		good bool
	}{
		{source.String(), true},
		{strings.Replace(source.String(), "HOT_UPDATE_TRANSFER_PROGRESS_S=10\n", "", 1), false},
		{strings.Replace(source.String(), "HOT_UPDATE_TRANSFER_PROGRESS_S=10", "HOT_UPDATE_TRANSFER_PROGRESS_S=5", 1), false},
		{source.String() + "HOT_UPDATE_TRANSFER_PROGRESS_S=10\n", false},
	} {
		transferWriteFixture(t, root, path, tc.text)
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		if err := checkUpgradeShellBudgets(string(raw)); (err == nil) != tc.good {
			t.Fatalf("on-disk transfer budget pin: %v good=%t", err, tc.good)
		}
	}
}

// The helper budgets have one definition alongside the hot-update SSOT. IPC
// exports keep their existing names but must alias these definitions, rather
// than quietly creating a second, drifting set of literals.
// origin: hot-update S9 plan §10.2 / T83
var infraBudgetNames = map[string]int64{"InfraReadBudget": 2, "InfraActionBudget": 10, "InfraWorkerBudget": 15, "InfraMaxWorkers": 4}
var infraBudgetShellNames = map[string]int64{"HOT_UPDATE_INFRA_READ_S": 2, "HOT_UPDATE_INFRA_ACTION_S": 10, "HOT_UPDATE_INFRA_WORKER_S": 15, "HOT_UPDATE_INFRA_MAX_WORKERS": 4}

func checkInfraBudgetDefinitions(source, shell string) error {
	file, err := brokerParse("internal/upgradebudget/infra.go", source)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, decl := range file.Decls {
		if group, ok := decl.(*ast.GenDecl); ok && group.Tok == token.CONST {
			for _, spec := range group.Specs {
				value := spec.(*ast.ValueSpec)
				for i, name := range value.Names {
					want, required := infraBudgetNames[name.Name]
					if !required {
						continue
					}
					if seen[name.Name] || i >= len(value.Values) {
						return fmt.Errorf("duplicate/nonexplicit S9 budget %s", name.Name)
					}
					seen[name.Name] = true
					expr := value.Values[i]
					if name.Name == "InfraMaxWorkers" {
						got, err := flowSSIntegerConstant(expr)
						if err != nil || got != want {
							return fmt.Errorf("bad S9 concurrency %s", name.Name)
						}
					} else {
						got, err := flowSSDurationConstant(expr)
						if err != nil || got != time.Duration(want)*time.Second {
							return fmt.Errorf("bad S9 duration %s", name.Name)
						}
					}
				}
			}
		}
	}
	for name := range infraBudgetNames {
		if !seen[name] {
			return fmt.Errorf("missing S9 SSOT budget %s", name)
		}
	}
	seen = map[string]bool{}
	for _, line := range strings.Split(shell, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "HOT_UPDATE_INFRA_") {
			continue
		}
		match := upgradeBudgetAssignmentRe.FindStringSubmatch(line)
		if len(match) != 3 {
			return fmt.Errorf("noninteger S9 shell budget %s", line)
		}
		n, err := strconv.ParseInt(match[2], 10, 64)
		want, ok := infraBudgetShellNames[match[1]]
		if err != nil || !ok || seen[match[1]] || n != want {
			return fmt.Errorf("duplicate/drifted S9 shell budget %s", match[1])
		}
		seen[match[1]] = true
	}
	for name := range infraBudgetShellNames {
		if !seen[name] {
			return fmt.Errorf("missing S9 shell budget %s", name)
		}
	}
	return nil
}

// origin: hot-update S9 plan §10.2 / T83
// gate-control: TestInfraBudgetPinControls
func TestInfraBudgetsUseOneSource(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "internal/upgradebudget/infra.go"))
	if err != nil {
		t.Fatalf("NOT_RUN pending P36: %v", err)
	}
	shell, err := os.ReadFile(filepath.Join(root, upgradeBudgetShellPath))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkInfraBudgetDefinitions(string(raw), string(shell)); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(filepath.Join(root, "internal/infrapriv/ipc.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := brokerParse("ipc.go", string(raw))
	if err != nil {
		t.Fatal(err)
	}
	selectors := infraSelectors(file)
	for name := range infraBudgetNames {
		if !selectors["internal/upgradebudget:"+name] {
			t.Errorf("IPC budget copies/omits upgradebudget.%s", name)
		}
	}
}

// origin: hot-update S9 plan §11 / T83 / M31
func TestInfraBudgetPinControls(t *testing.T) {
	root := transferFixtureTree(t, map[string]string{})
	good := `package upgradebudget;import "time";const(InfraReadBudget=2*time.Second;InfraActionBudget=10*time.Second;InfraWorkerBudget=15*time.Second;InfraMaxWorkers=4)`
	shell := "HOT_UPDATE_INFRA_READ_S=2\nHOT_UPDATE_INFRA_ACTION_S=10\nHOT_UPDATE_INFRA_WORKER_S=15\nHOT_UPDATE_INFRA_MAX_WORKERS=4\n"
	check := func(source, sh string) error {
		transferWriteFixture(t, root, "internal/upgradebudget/infra.go", source)
		transferWriteFixture(t, root, "budget.sh", sh)
		raw, e := os.ReadFile(filepath.Join(root, "internal/upgradebudget/infra.go"))
		if e != nil {
			t.Fatal(e)
		}
		rawShell, e := os.ReadFile(filepath.Join(root, "budget.sh"))
		if e != nil {
			t.Fatal(e)
		}
		return checkInfraBudgetDefinitions(string(raw), string(rawShell))
	}
	if err := check(good, shell); err != nil {
		t.Fatal(err)
	}
	for name := range infraBudgetNames {
		if check(strings.Replace(good, name, "DormantBudget", 1), shell) == nil {
			t.Fatalf("missing S9 Go budget escaped %s", name)
		}
	}
	for _, bad := range []string{strings.Replace(shell, "READ_S=2", "READ_S=3", 1), strings.Replace(shell, "READ_S=2", "READ_S=two", 1), shell + "HOT_UPDATE_INFRA_READ_S=2\n", strings.Replace(shell, "HOT_UPDATE_INFRA_READ_S=2\n", "", 1)} {
		if check(good, bad) == nil {
			t.Fatal("on-disk S9 shell budget mutation escaped")
		}
	}
	if check(strings.Replace(good, "15*time.Second", "120*time.Second", 1), shell) == nil {
		t.Fatal("worker lifetime widened to component deadline")
	}
}
