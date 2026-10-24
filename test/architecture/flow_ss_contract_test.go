package architecture

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
)

// These are exact contract sets, not minimum counts: accepting 124 methods or
// one fuzz target would silently lose an entry point just as accepting 126
// would silently grow Agent's responsibility.
// origin: hot-update S5 plan §7.3 / §10.1 / T49.
// gate-control: TestFlowSSContractControls
func TestFlowSSContractsKeepClosedSets(t *testing.T) {
	root := repoRoot(t)
	t.Run("Agent", func(t *testing.T) {
		files := readFlowSSSources(t, root, "internal/agent", false)
		methods := countFlowSSReceiverMethods(files, "Agent")
		if err := checkFlowSSAgentBudget(methods, len(files)); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("deferred", func(t *testing.T) {
		if err := checkFlowSSDeferred(agentimage.DeferredReasons[:]); err != nil {
			t.Fatalf("pending P17: %v", err)
		}
	})
	t.Run("fuzz", func(t *testing.T) {
		discovered := map[string]bool{}
		for _, pkg := range []string{"internal/flow", "internal/agent/ssproxy"} {
			files := readFlowSSSources(t, root, pkg, true)
			for _, file := range files {
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok || fn.Recv != nil || !strings.HasPrefix(fn.Name.Name, "Fuzz") {
						continue
					}
					if fn.Name.Name == "FuzzFlowFrame" || fn.Name.Name == "FuzzSSFrame" {
						key := pkg + ":" + fn.Name.Name
						if discovered[key] {
							t.Errorf("duplicate fuzzer %s", key)
						}
						discovered[key] = true
						if err := checkFlowSSFuzzer(fn); err != nil {
							t.Errorf("%s: %v", key, err)
						}
					}
				}
			}
		}
		if err := checkFlowSSFuzzSet(discovered); err != nil {
			t.Fatalf("pending P2/P9: %v", err)
		}
	})
	t.Run("public-secrets", func(t *testing.T) {
		if err := compareInventoryRows(flowSSPublicImageSchema, flowSSPublicImageRows()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("config", func(t *testing.T) {
		pointer := reflect.TypeOf((*workload.ReplayBudget)(nil))
		for _, typ := range []reflect.Type{reflect.TypeOf(flow.Config{}), reflect.TypeOf(tunnel.FlowConfig{})} {
			field, ok := typ.FieldByName("Budget")
			if !ok || field.Type != pointer {
				t.Errorf("%s must borrow shared *workload.ReplayBudget", typ)
			}
		}
		if flow.MaxFlows != 32 || flow.MaxPendingFDs != 32 || flow.MaxRetirementHoles != 1024 || flow.MaxTerminalMetadata != 4096 || tunnel.MaxEndpoints != 128 || ssproxy.MaxActors != 128 || ssproxy.MaxMaterialHistory != 64 || ssproxy.MaxKeys != 256 {
			t.Fatal("bounded owner contract drifted")
		}
		if problems := flowSSReplayBudgetProblems(t, root); len(problems) != 0 {
			t.Fatal(strings.Join(problems, "\n"))
		}
	})
}

func countFlowSSReceiverMethods(files map[string]*ast.File, receiver string) int {
	count := 0
	for _, file := range files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if ok && fn.Recv != nil && len(fn.Recv.List) == 1 && receiverTypeName(fn.Recv.List[0].Type) == receiver {
				count++
			}
		}
	}
	return count
}
func checkFlowSSAgentBudget(methods, files int) error {
	if methods != 125 {
		return fmt.Errorf("Agent has %d methods; exact frozen contract is 125", methods)
	}
	if files > 19 {
		return fmt.Errorf("Agent has %d production files; ceiling is 19", files)
	}
	return nil
}

var flowSSDeferredReasons = []string{"spawn_in_flight", "remote_fs_probe_in_flight", "foreign_process_rows", "image_too_large", "runtime_closed", "platform_unsupported", "transfers_in_flight", "legacy_session_busy", "ss_state_unrepresentable"}

func checkFlowSSDeferred(reasons []string) error {
	if !slices.Equal(reasons, flowSSDeferredReasons) {
		return fmt.Errorf("deferred reasons must be the ordered nine-code S5 set; got %v", reasons)
	}
	return nil
}
func checkFlowSSFuzzSet(discovered map[string]bool) error {
	want := []string{"internal/flow:FuzzFlowFrame", "internal/agent/ssproxy:FuzzSSFrame"}
	if len(discovered) != 2 {
		return fmt.Errorf("exact FlowFrame/SSFrame fuzz discovery requires two targets: %v", discovered)
	}
	for _, key := range want {
		if !discovered[key] {
			return fmt.Errorf("missing exact fuzz target %s", key)
		}
	}
	return nil
}
func checkFlowSSFuzzer(fn *ast.FuncDecl) error {
	registered, decoder := false, false
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		target, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || target.Sel.Name != "Fuzz" {
			return true
		}
		registered = true
		for _, arg := range call.Args {
			callback, ok := arg.(*ast.FuncLit)
			if !ok {
				continue
			}
			ast.Inspect(callback.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				switch callee := call.Fun.(type) {
				case *ast.SelectorExpr:
					decoder = decoder || callee.Sel.Name == "DecodeFrame"
				case *ast.Ident:
					decoder = decoder || callee.Name == "decodeState" || callee.Name == "DecodeFrame"
				}
				return true
			})
		}
		return true
	})
	if !registered || !decoder {
		return fmt.Errorf("fuzzer must register a callback and reach the real decoder inside it")
	}
	return nil
}

// A naked default in a closed state switch quietly converts unknown values to
// success. This source tripwire supplements P27's full enum-family/type gate.
// It deliberately scopes only the new S5 owner files and typed enum params.
// origin: hot-update S5 plan §10.1 / T49.
func TestFlowSSStateSwitchDefaultsRejectUnknown(t *testing.T) {
	if problems := flowSSOwnerStateProblems(t, repoRoot(t)); len(problems) != 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
}

// The live checks and controls share the complete owner walk so a moved owner
// cannot disappear while the isolated AST predicate still passes its controls.
// origin: hot-update S6 plan §9.3 / HARDGATE-A
func flowSSReplayBudgetProblems(t *testing.T, root string) []string {
	t.Helper()
	var problems []string
	for _, pkg := range []string{"internal/flow", "internal/tunnel", "internal/tunnel/flowcarrier", "internal/tunnel/flowstate", "internal/tunnel/transport", "internal/agent/ssproxy"} {
		for name, file := range readFlowSSSources(t, root, pkg, false) {
			if pkg == "internal/tunnel" && !strings.HasPrefix(name, "flow_") {
				continue
			}
			ast.Inspect(file, func(node ast.Node) bool {
				call, ok := node.(*ast.CallExpr)
				if !ok {
					return true
				}
				selector, ok := call.Fun.(*ast.SelectorExpr)
				if ok && selector.Sel.Name == "NewReplayBudget" {
					problems = append(problems, fmt.Sprintf("%s/%s creates a second node replay budget", pkg, name))
				}
				return true
			})
		}
	}
	return problems
}

func flowSSOwnerStateProblems(t *testing.T, root string) []string {
	t.Helper()
	var problems []string
	for _, pkg := range []string{"internal/flow", "internal/agent/ssproxy", "internal/rawsocket", "internal/tunnel", "internal/tunnel/flowcarrier", "internal/tunnel/flowstate", "internal/tunnel/transport"} {
		for name, file := range readFlowSSSources(t, root, pkg, false) {
			if pkg == "internal/tunnel" && !strings.HasPrefix(name, "flow_") {
				continue
			}
			if err := checkFlowSSStateDefaults(file); err != nil {
				problems = append(problems, fmt.Sprintf("%s/%s: %v", pkg, name, err))
			}
		}
	}
	return problems
}
func checkFlowSSStateDefaults(file *ast.File) error {
	families := map[string]bool{"Kind": true, "Phase": true, "Reason": true, "ResumeScope": true, "ResumeMode": true, "ResumeVerdict": true, "CloseMode": true, "SSPhase": true, "AuthorityPhase": true, "EndpointPhase": true, "TicketPurpose": true, "RawConnectState": true, "ConnectState": true}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		typed := map[string]bool{}
		if fn.Type.Params != nil {
			for _, field := range fn.Type.Params.List {
				id, ok := field.Type.(*ast.Ident)
				if ok && families[id.Name] {
					for _, name := range field.Names {
						typed[name.Name] = true
					}
				}
			}
		}
		var failure error
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			sw, ok := n.(*ast.SwitchStmt)
			if !ok {
				return true
			}
			id, ok := sw.Tag.(*ast.Ident)
			if !ok || !typed[id.Name] {
				return true
			}
			found := false
			for _, statement := range sw.Body.List {
				clause := statement.(*ast.CaseClause)
				if clause.List != nil {
					continue
				}
				found = true
				if !flowSSDefaultRejects(clause.Body) {
					failure = fmt.Errorf("%s has a naked default for closed state %s", fn.Name.Name, id.Name)
				}
			}
			if !found {
				failure = fmt.Errorf("%s has no fail-closed default for %s", fn.Name.Name, id.Name)
			}
			return true
		})
		if failure != nil {
			return failure
		}
	}
	return nil
}
func flowSSDefaultRejects(body []ast.Stmt) bool {
	if len(body) == 0 {
		return false
	}
	statement, ok := body[len(body)-1].(*ast.ReturnStmt)
	if !ok {
		return false
	}
	for _, result := range statement.Results {
		switch x := result.(type) {
		case *ast.Ident:
			if x.Name == "false" || strings.HasPrefix(x.Name, "Err") || strings.EqualFold(x.Name, "err") {
				return true
			}
		case *ast.SelectorExpr:
			if strings.HasPrefix(x.Sel.Name, "Err") {
				return true
			}
		case *ast.CallExpr:
			if selector, ok := x.Fun.(*ast.SelectorExpr); ok && (selector.Sel.Name == "Errorf" || selector.Sel.Name == "New") {
				return true
			}
		}
	}
	return false
}

// origin: hot-update S5 plan §10.1 / T49.
func TestFlowSSContractControls(t *testing.T) {
	// Each retained and extracted owner must be read through the live walk.
	// The fixtures stay on disk because these guards inspect source paths.
	// origin: hot-update S6 plan §9.3 / HARDGATE-A
	owners := []string{"internal/flow", "internal/agent/ssproxy", "internal/rawsocket", "internal/tunnel", "internal/tunnel/flowcarrier", "internal/tunnel/flowstate", "internal/tunnel/transport"}
	root := t.TempDir()
	write := func(pkg, body string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Join(root, pkg), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, pkg, "flow_fixture.go"), []byte("package fixture;"+body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, pkg := range owners {
		write(pkg, "")
	}
	if len(flowSSReplayBudgetProblems(t, root)) != 0 || len(flowSSOwnerStateProblems(t, root)) != 0 {
		t.Fatal("empty owner fixture rejected")
	}
	for _, pkg := range owners {
		write(pkg, "func apply(phase Phase)error{switch phase{case 1:return nil;default:return nil}}")
		if problems := flowSSOwnerStateProblems(t, root); len(problems) != 1 || !strings.HasPrefix(problems[0], pkg+"/") {
			t.Fatalf("state defect in %s escaped the live owner walk: %v", pkg, problems)
		}
		if pkg != "internal/rawsocket" { // rawsocket has no replay budget responsibility.
			write(pkg, "func allocate(){workload.NewReplayBudget()}")
			if problems := flowSSReplayBudgetProblems(t, root); len(problems) != 1 || !strings.HasPrefix(problems[0], pkg+"/") {
				t.Fatalf("second replay budget in %s escaped the live owner walk: %v", pkg, problems)
			}
		}
		write(pkg, "")
	}
	if len(flowSSDeferredReasons) != 9 || flowSSDeferredReasons[8] != "ss_state_unrepresentable" {
		t.Fatal("frozen deferred set must contain exactly nine reasons including SS representation")
	}
	// Exercise the repository receiver counter, including value/pointer methods
	// and decoys. Literal 124/126 fixtures alone would not protect discovery.
	var source strings.Builder
	source.WriteString("package fixture;type Agent struct{};type Other struct{};")
	for i := 0; i < 125; i++ {
		receiver := "Agent"
		if i%2 == 0 {
			receiver = "*Agent"
		}
		fmt.Fprintf(&source, "func(a %s) Method%d(){};", receiver, i)
	}
	source.WriteString("func(a Other) Decoy(){};func Free(){};")
	file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", source.String(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got := countFlowSSReceiverMethods(map[string]*ast.File{"fixture.go": file}, "Agent"); got != 125 {
		t.Fatalf("actual receiver discovery counted %d Agent methods", got)
	}

	if checkFlowSSAgentBudget(125, 19) != nil {
		t.Fatal("valid Agent budget rejected")
	}
	for _, tc := range []struct{ methods, files int }{{124, 19}, {126, 19}, {125, 20}} {
		if checkFlowSSAgentBudget(tc.methods, tc.files) == nil {
			t.Fatalf("Agent budget defect accepted: %+v", tc)
		}
	}
	if checkFlowSSDeferred(flowSSDeferredReasons) != nil {
		t.Fatal("valid deferred set rejected")
	}
	for i := range flowSSDeferredReasons {
		broken := append(slices.Clone(flowSSDeferredReasons[:i]), flowSSDeferredReasons[i+1:]...)
		if checkFlowSSDeferred(broken) == nil {
			t.Fatal("missing deferred reason accepted")
		}
	}
	if checkFlowSSDeferred(append(slices.Clone(flowSSDeferredReasons), "flow_busy")) == nil {
		t.Fatal("obsolete busy code accepted")
	}
	for _, keys := range [][]string{{"internal/flow:FuzzFlowFrame", "internal/agent/ssproxy:FuzzSSFrame"}, {"internal/flow:FuzzFlowFrame"}, {"internal/flow:FuzzFlowFrame", "internal/agent/ssproxy:FuzzWrong"}, {"internal/flow:FuzzFlowFrame", "internal/agent/ssproxy:FuzzSSFrame", "internal/other:FuzzFlowFrame"}} {
		got := map[string]bool{}
		for _, key := range keys {
			got[key] = true
		}
		if (checkFlowSSFuzzSet(got) == nil) != (len(keys) == 2 && keys[1] == "internal/agent/ssproxy:FuzzSSFrame") {
			t.Fatal("exact fuzz discovery accepted missing/wrong target")
		}
	}
	for _, tc := range []struct {
		body  string
		valid bool
	}{{"return ErrInvalid", true}, {"return false", true}, {"return nil", false}, {"", false}} {
		file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", "package fixture; func apply(phase SSPhase)error{switch phase {case 1:return nil;default:"+tc.body+"}}", 0)
		if err != nil {
			t.Fatal(err)
		}
		if (checkFlowSSStateDefaults(file) == nil) != tc.valid {
			t.Fatal("naked closed-state default escaped")
		}
	}
	// Read the source through the same parser used for actual fuzz discovery.
	for _, body := range []string{`f.Fuzz(func(t *testing.T,b []byte){flow.DecodeFrame(bytes.NewReader(b))})`, `f.Fuzz(func(t *testing.T,b []byte){})`, `flow.DecodeFrame(bytes.NewReader(nil));f.Fuzz(func(t *testing.T,b []byte){})`} {
		file, err := parser.ParseFile(token.NewFileSet(), "fixture.go", "package fixture;func FuzzFlowFrame(f *testing.F){"+body+"}", 0)
		if err != nil {
			t.Fatal(err)
		}
		fn := file.Decls[0].(*ast.FuncDecl)
		if (checkFlowSSFuzzer(fn) == nil) != strings.Contains(body, "NewReader(b)") {
			t.Fatal("empty fuzz entry accepted")
		}
	}
	// Test-only source strings are not production public carriers.
	if _, err := os.Stat(filepath.Join(repoRoot(t), "internal/agentimage/bundle.go")); err != nil {
		t.Fatal(err)
	}
}
