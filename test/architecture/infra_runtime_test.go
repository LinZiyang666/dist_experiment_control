package architecture

import (
	"fmt"
	"go/ast"
	"go/token"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

type infraRootRule struct {
	root, target, origin string
}

// The source row artifact mirrors this independent set. Renames must update
// both the actual root and its test mapping; a nonempty unrelated declaration
// cannot make a moved scanner green.
// origin: hot-update S9 plan §10.2 / T83
var infraRuntimeRoots = []infraRootRule{
	{"cmd/tether-infra-helper/main.go:main", "cmd/tether-infra-helper:sanitizeInherited", "T92"},
	{"cmd/tether-infra-helper/main.go:main", "internal/infraroot:OpenInstalled", "T91"},
	{"cmd/tether-infra-launch/main.go:main", "internal/infralaunch:Run", "T14"},
	{"internal/infraroot/bind.go:New", "internal/infrahost:NewWorker", "T91"},
	{"internal/infracontract/codec.go:SigningBytes", "internal/infracontract:validateRequest", "T02"},
	{"internal/infracontract/codec.go:VerifyRequest", "crypto/ed25519:Verify", "T02"},
	{"internal/infraupgrade/nats.go:natsDriver.Advance", "internal/infraupgrade:natsDriver.cutover", "T29"},
	{"internal/infraupgrade/caddy.go:caddyDriver.Advance", "internal/infraupgrade:caddyDriver.run", "T22"},
	{"internal/infratransport/move.go:Move", "internal/infratransport:Transport.Interrupt", "T32"},
	{"internal/infratransport/move.go:Transport.Interrupt", "internal/infratransport:Transport.interrupt", "T33"},
	{"internal/infratransfer/adapter.go:Adapter.Fence", "internal/infratransfer:checkpoint", "T42"},
}

type infraHostRule struct {
	owner  string
	roots  []string
	prefix string
}

// Constructors and loops are one caller family: a constructor may install a
// callback that its loop uses later. Require an actual binding in that family,
// rather than freezing an adapter's private function name before P22/P23 land.
// origin: hot-update S9 plan §8 / §10.2 / T83
var infraHostRoots = []infraHostRule{
	{"P22", []string{"internal/broker/broker.go:New", "internal/broker/broker.go:Broker.Run"}, "internal/infraattach:"},
	{"P23", []string{"internal/agent/agent.go:New", "internal/agent/agent.go:Agent.connectNATS"}, "internal/infraattach:"},
	{"P23", []string{"cmd/tether/ctl_connect.go:connectCtlOpts"}, "internal/infraattach:"},
	// origin: hot-update S9 plan §7 / P24 / T52; include the signed component command family.
	{"P24", []string{"cmd/tether/cluster_upgrade.go:newBrokerCmd", "cmd/tether/cluster_upgrade.go:newClusterUpgradeCmd", "cmd/tether/admin.go:newAdminCmd"}, "internal/infracli:"},
}

func checkInfraHostBindings(sources map[string]string, rules []infraHostRule) error {
	if len(rules) == 0 {
		return fmt.Errorf("empty S9 host caller families")
	}
	all, err := transferDeclarations(sources)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		if len(rule.roots) == 0 || rule.prefix == "" {
			return fmt.Errorf("empty S9 host root/target")
		}
		bound := false
		for _, root := range rule.roots {
			calls, err := infraCallsFromDeclarations(all, root)
			if err != nil {
				return err
			}
			for target := range calls {
				if !strings.HasPrefix(target, rule.prefix) {
					continue
				}
				// A reference to a missing adapter cannot certify wiring either.
				for _, decl := range all {
					key := filepath.ToSlash(filepath.Dir(decl.path)) + ":" + decl.name
					if fn, ok := decl.node.(*ast.FuncDecl); key == target && ok && fn.Body != nil && len(fn.Body.List) > 0 {
						bound = true
					}
				}
			}
		}
		if !bound {
			return fmt.Errorf("NOT_RUN pending %s: actual caller family %v has no live %s binding", rule.owner, rule.roots, rule.prefix)
		}
	}
	return nil
}

// origin: hot-update S9 plan §8 / §10.2 / T83
// gate-control: TestInfraRuntimeGateControls
func TestInfraHostCallersBindOwnedAdapters(t *testing.T) {
	if err := checkInfraHostBindings(brokerGateSources(t, repoRoot(t)), infraHostRoots); err != nil {
		t.Fatal(err)
	}
}

// Reverse-discover configuration emitters rather than trusting a producer list
// to enumerate itself. Method values count too: moving a write behind an alias
// must not remove its registration obligation. This is a source inventory;
// P18/P22's behavioral tests prove admission/join at those registered producers.
// origin: hot-update S9 plan §3.2 / §10.2 / T83 / M31
func infraAssetProducerRows(sources map[string]string) ([]string, error) {
	all, err := transferDeclarations(sources)
	if err != nil {
		return nil, err
	}
	mutators := []string{"CreateStream", "UpdateStream", "CreateOrUpdateStream", "DeleteStream", "PurgeStream", "Purge", "EnsureStream", "CreateConsumer", "UpdateConsumer", "CreateOrUpdateConsumer", "OrderedConsumer", "DeleteConsumer", "EnsureConsumer", "CreateObjectStore", "UpdateObjectStore", "CreateOrUpdateObjectStore", "DeleteObjectStore", "CreateKeyValue", "UpdateKeyValue", "CreateOrUpdateKeyValue", "DeleteKeyValue"}
	rows := map[string]bool{}
	for root, decl := range all {
		if !strings.HasPrefix(decl.path, "internal/") {
			continue
		}
		fn, ok := decl.node.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			if selector, ok := node.(*ast.SelectorExpr); ok && infraContains(mutators, selector.Sel.Name) {
				rows[root+"\t"+selector.Sel.Name+"\tT41/T49"] = true
			}
			return true
		})
	}
	var result []string
	for row := range rows {
		result = append(result, row)
	}
	slices.Sort(result)
	return result, nil
}

func checkInfraAssetProducerRows(sources map[string]string, registered []string) error {
	actual, err := infraAssetProducerRows(sources)
	if err != nil {
		return err
	}
	want := slices.Clone(registered)
	slices.Sort(want)
	if len(actual) == 0 || len(want) == 0 || !slices.Equal(actual, want) {
		return fmt.Errorf("S9 asset producer inventory drift: actual %v; registered %v", actual, want)
	}
	return nil
}

// origin: hot-update S9 plan §3.2 / §10.2 / T83
// gate-control: TestInfraRuntimeGateControls
func TestInfraAssetProducersHaveExactRoots(t *testing.T) {
	sources := brokerGateSources(t, repoRoot(t))
	actual, err := infraAssetProducerRows(sources)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range actual {
		t.Log(row)
	}
	rows, err := readTransferTSV(repoRoot(t), "test/architecture/testdata/broker_image_gate_rows.tsv#infra-js-producers", 3)
	if err != nil {
		t.Fatal(err)
	}
	var registered []string
	for _, row := range rows {
		registered = append(registered, strings.Join(row, "\t"))
	}
	if err := checkInfraAssetProducerRows(sources, registered); err != nil {
		t.Fatal(err)
	}
}

// Resolve declarations by package and receiver, rather than globally matching
// method spelling. Free-function aliases and cross-file helpers stay visible.
// Dead go/defer bodies cannot substitute for a synchronous production binding.
// This is a source tripwire; the independent real-process tests sign effects.

func infraCallsFromDeclarations(all map[string]transferDeclaration, root string) (map[string]bool, error) {
	entry, ok := all[root]
	if !ok {
		return nil, fmt.Errorf("missing actual S9 scanner root %s", root)
	}
	fn, ok := entry.node.(*ast.FuncDecl)
	if !ok || fn.Body == nil || len(fn.Body.List) == 0 {
		return nil, fmt.Errorf("empty S9 scanner root %s", root)
	}
	index := map[string][]transferDeclaration{}
	for _, decl := range all {
		key := filepath.ToSlash(filepath.Dir(decl.path)) + ":" + decl.name
		index[key] = append(index[key], decl)
	}
	seen := map[string]bool{}
	calls := map[string]bool{}
	var visit func(transferDeclaration)
	visit = func(decl transferDeclaration) {
		site := decl.path + ":" + decl.name
		if seen[site] {
			return
		}
		seen[site] = true
		pkg := filepath.ToSlash(filepath.Dir(decl.path))
		locals := map[string]string{}
		aliases := map[string]ast.Expr{}
		qualify := func(expr ast.Expr) string {
			for {
				p, ok := expr.(*ast.StarExpr)
				if !ok {
					break
				}
				expr = p.X
			}
			switch typ := expr.(type) {
			case *ast.Ident:
				return pkg + ":" + typ.Name
			case *ast.SelectorExpr:
				if id, ok := typ.X.(*ast.Ident); ok && decl.imports[id.Name] != "" {
					return strings.TrimPrefix(decl.imports[id.Name], modPrefix) + ":" + typ.Sel.Name
				}
			}
			return ""
		}
		if fn, ok := decl.node.(*ast.FuncDecl); ok {
			for _, fields := range []*ast.FieldList{fn.Recv, fn.Type.Params} {
				if fields == nil {
					continue
				}
				for _, field := range fields.List {
					for _, name := range field.Names {
						locals[name.Name] = qualify(field.Type)
					}
				}
			}
		}
		var target func(ast.Expr, map[string]bool) string
		target = func(expr ast.Expr, resolving map[string]bool) string {
			switch value := expr.(type) {
			case *ast.IndexExpr:
				return target(value.X, resolving)
			case *ast.IndexListExpr:
				return target(value.X, resolving)
			case *ast.ParenExpr:
				return target(value.X, resolving)
			case *ast.Ident:
				if alias := aliases[value.Name]; alias != nil && !resolving[value.Name] {
					resolving[value.Name] = true
					return target(alias, resolving)
				}
				return pkg + ":" + value.Name
			case *ast.FuncLit:
				key := pkg + ":" + decl.name + "@" + fmt.Sprint(value.Pos())
				if len(index[key]) == 0 {
					index[key] = []transferDeclaration{{path: decl.path, name: decl.name + "@" + fmt.Sprint(value.Pos()), node: value, imports: decl.imports}}
				}
				return key
			case *ast.SelectorExpr:
				if id, ok := value.X.(*ast.Ident); ok {
					if imp := decl.imports[id.Name]; imp != "" {
						return strings.TrimPrefix(imp, modPrefix) + ":" + value.Sel.Name
					}
					if typ := locals[id.Name]; typ != "" {
						return typ + "." + value.Sel.Name
					}
				}
			}
			return ""
		}
		bind := func(key string) {
			if key == "" {
				return
			}
			calls[key] = true
			for _, next := range index[key] {
				visit(next)
			}
		}
		ast.Inspect(decl.node, func(node ast.Node) bool {
			switch value := node.(type) {
			case *ast.GoStmt, *ast.DeferStmt:
				return false
			case *ast.FuncLit:
				// Only a called/passed literal is visited as its own declaration.
				return node == decl.node
			case *ast.IfStmt:
				if id, ok := value.Cond.(*ast.Ident); ok && id.Name == "false" {
					if value.Else != nil {
						ast.Inspect(value.Else, func(n ast.Node) bool {
							if call, ok := n.(*ast.CallExpr); ok {
								bind(target(call.Fun, map[string]bool{}))
							}
							return true
						})
					}
					return false
				}
			case *ast.AssignStmt:
				for i, lhs := range value.Lhs {
					if id, ok := lhs.(*ast.Ident); ok && i < len(value.Rhs) {
						aliases[id.Name] = value.Rhs[i]
					}
				}
			case *ast.ValueSpec:
				if node == decl.node {
					for _, expr := range value.Values {
						bind(target(expr, map[string]bool{}))
					}
				}
				for i, name := range value.Names {
					locals[name.Name] = qualify(value.Type)
					if i < len(value.Values) {
						aliases[name.Name] = value.Values[i]
					}
				}
			case *ast.CallExpr:
				bind(target(value.Fun, map[string]bool{}))
				// Factories passed as actual call arguments are production bindings,
				// unlike a dormant same-file function declaration or comment.
				for _, arg := range value.Args {
					key := target(arg, map[string]bool{})
					if len(index[key]) != 0 {
						bind(key)
					}
				}
			}
			return true
		})
	}
	visit(entry)
	return calls, nil
}

func checkInfraRuntimeRoots(sources map[string]string, rules []infraRootRule) error {
	if len(rules) == 0 {
		return fmt.Errorf("empty S9 scanner root set")
	}
	all, err := transferDeclarations(sources)
	if err != nil {
		return err
	}
	for _, rule := range rules {
		calls, err := infraCallsFromDeclarations(all, rule.root)
		if err != nil {
			return err
		}
		if !calls[rule.target] {
			return fmt.Errorf("%s / %s: required production call %s is unreachable", rule.origin, rule.root, rule.target)
		}
	}
	return nil
}

// Signing a malformed request must stop before the signature operation. A
// retained call with its error branch deleted is as unsafe as a deleted call.
// Match the actual assigned error and immediate propagation, not a grep for
// "err" somewhere else in the same function.
// origin: hot-update S9 plan §2.2 / §10.2 / T83 / M31
func checkInfraRequestErrorPaths(sources map[string]string) error {
	all, err := transferDeclarations(sources)
	if err != nil {
		return err
	}
	for root, callee := range map[string]string{
		"internal/infracontract/codec.go:SigningBytes":  "EncodeRequest",
		"internal/infracontract/codec.go:VerifyRequest": "SigningBytes",
	} {
		decl, exists := all[root]
		if !exists {
			return fmt.Errorf("missing error-path scanner root %s", root)
		}
		fn, ok := decl.node.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			return fmt.Errorf("empty error-path scanner root %s", root)
		}
		guarded := false
		for i, statement := range fn.Body.List {
			assignment, ok := statement.(*ast.AssignStmt)
			if !ok || len(assignment.Rhs) != 1 || len(assignment.Lhs) != 2 || i+1 >= len(fn.Body.List) {
				continue
			}
			call, ok := assignment.Rhs[0].(*ast.CallExpr)
			if !ok {
				continue
			}
			name, ok := call.Fun.(*ast.Ident)
			if !ok || name.Name != callee {
				continue
			}
			errorName, ok := assignment.Lhs[1].(*ast.Ident)
			guard, conditional := fn.Body.List[i+1].(*ast.IfStmt)
			if !ok || errorName.Name == "_" || !conditional || len(guard.Body.List) != 1 {
				continue
			}
			comparison, ok := guard.Cond.(*ast.BinaryExpr)
			if !ok || comparison.Op != token.NEQ {
				continue
			}
			left, lOK := comparison.X.(*ast.Ident)
			right, rOK := comparison.Y.(*ast.Ident)
			ret, returns := guard.Body.List[0].(*ast.ReturnStmt)
			if !lOK || !rOK || left.Name != errorName.Name || right.Name != "nil" || !returns || len(ret.Results) == 0 {
				continue
			}
			last, ok := ret.Results[len(ret.Results)-1].(*ast.Ident)
			guarded = ok && last.Name == errorName.Name
		}
		if !guarded {
			return fmt.Errorf("%s: request/signing error is not propagated before effect", root)
		}
	}
	return nil
}

// origin: hot-update S9 plan §10.2 / T83
// gate-control: TestInfraRuntimeGateControls
func TestInfraRuntimeScannerRootsAreLive(t *testing.T) {
	rows, err := readTransferTSV(repoRoot(t), "test/architecture/testdata/broker_image_gate_rows.tsv#infra-scanner-roots", 3)
	if err != nil {
		t.Fatal(err)
	}
	var got, want []string
	for _, row := range rows {
		got = append(got, strings.Join(row, "\t"))
	}
	for _, rule := range infraRuntimeRoots {
		want = append(want, strings.Join([]string{rule.root, rule.target, rule.origin}, "\t"))
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Fatalf("S9 scanner root/origin mapping drift: got %v want %v", got, want)
	}
	if err := checkInfraRuntimeRoots(brokerGateSources(t, repoRoot(t)), infraRuntimeRoots); err != nil {
		t.Fatal(err)
	}
	if err := checkInfraRequestErrorPaths(brokerGateSources(t, repoRoot(t))); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §11 / T83 / M31
func TestInfraRuntimeGateControls(t *testing.T) {
	good := map[string]string{
		"internal/infraattach/root.go":            `package owner; import leaf "github.com/LinZiyang666/tether/internal/infratransport"; func Run(){forward()};func forward(){f:=leaf.Move;f()}`,
		"internal/infratransport/move.go":         `package transport;func Move(){helper()}`,
		"internal/infratransport/helper_other.go": `package transport;func helper(){}`,
	}
	root := transferFixtureTree(t, good)
	producerPath := "internal/jsstream/config.go"
	producer := `package jsstream;func Ensure(){create:=js.CreateStream;create();js.DeleteConsumer()}`
	transferWriteFixture(t, root, producerPath, producer)
	producerRows := []string{producerPath + ":Ensure\tCreateStream\tT41/T49", producerPath + ":Ensure\tDeleteConsumer\tT41/T49"}
	if err := checkInfraAssetProducerRows(brokerGateSources(t, root), producerRows); err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]string{nil, producerRows[:1], append(slices.Clone(producerRows), producerRows[0])} {
		if checkInfraAssetProducerRows(brokerGateSources(t, root), rows) == nil {
			t.Fatal("empty/missing/duplicate asset producer registration escaped")
		}
	}
	for _, source := range []string{"package jsstream;func Ensure(){ /* js.CreateStream();js.DeleteConsumer() */ }", producer + ";func LateJoin(){js.Purge()}"} {
		transferWriteFixture(t, root, producerPath, source)
		if checkInfraAssetProducerRows(brokerGateSources(t, root), producerRows) == nil {
			t.Fatal("on-disk dead/new asset producer escaped registration")
		}
	}
	transferWriteFixture(t, root, producerPath, producer)
	rules := []infraRootRule{{"internal/infraattach/root.go:Run", "internal/infratransport:helper", "T32"}}
	read := func() map[string]string { return brokerGateSources(t, root) }
	if err := checkInfraRuntimeRoots(read(), rules); err != nil {
		t.Fatal(err)
	}
	codec := `package contract;func EncodeRequest(int)([]byte,error){return nil,nil};func SigningBytes(r int)([]byte,error){data,err:=EncodeRequest(r);if err!=nil{return nil,err};return data,nil};func VerifyRequest(r int)error{_,err:=SigningBytes(r);if err!=nil{return err};return nil}`
	codecPath := "internal/infracontract/codec.go"
	transferWriteFixture(t, root, codecPath, codec)
	bindings := []infraHostRule{{"P22", []string{"internal/infraattach/root.go:Run"}, "internal/infratransport:"}}
	if err := checkInfraHostBindings(read(), bindings); err != nil {
		t.Fatal(err)
	}
	transferWriteFixture(t, root, "internal/infraattach/root.go", "package owner;func Run(){ _ = 1 /* leaf.Move() */ };func unused(){forward()};func forward(){}")
	if checkInfraHostBindings(read(), bindings) == nil {
		t.Fatal("dormant adapter satisfied actual host caller")
	}
	transferWriteFixture(t, root, "internal/infraattach/root.go", good["internal/infraattach/root.go"])
	if err := checkInfraRequestErrorPaths(read()); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Replace(codec, "if err!=nil{return nil,err}", "", 1),
		strings.Replace(codec, "if err!=nil{return nil,err}", "if false{return nil,err}", 1),
		strings.Replace(codec, "if err!=nil{return err}", "if err!=nil{return nil}", 1),
	} {
		transferWriteFixture(t, root, codecPath, bad)
		if checkInfraRequestErrorPaths(read()) == nil {
			t.Fatal("on-disk request error-path deletion escaped")
		}
	}
	transferWriteFixture(t, root, codecPath, codec)
	for _, body := range []string{"", "return", "/* forward() */", "go forward()", "defer forward()", "if false {forward()}", "f:=func(){forward()};_ = f"} {
		transferWriteFixture(t, root, "internal/infraattach/root.go", "package owner;func Run(){"+body+"};func forward(){}")
		if checkInfraRuntimeRoots(read(), rules) == nil {
			t.Fatalf("on-disk dormant/missing call escaped: %s", body)
		}
	}
	transferWriteFixture(t, root, "internal/infraattach/root.go", good["internal/infraattach/root.go"])
	transferWriteFixture(t, root, "internal/infratransport/move.go", "package transport;func Move(){};func unused(){helper()}")
	if checkInfraRuntimeRoots(read(), rules) == nil {
		t.Fatal("wrong function's call satisfied actual root")
	}
	if checkInfraRuntimeRoots(read(), nil) == nil {
		t.Fatal("empty root inventory escaped")
	}
	transferWriteFixture(t, root, "internal/infratransport/move.go", good["internal/infratransport/move.go"])
	if err := checkInfraRuntimeRoots(read(), rules); err != nil {
		t.Fatal(err)
	}

	methodPath := "internal/infratransport/method.go"
	methodRule := []infraRootRule{{methodPath + ":Run", "internal/infratransport:Transport.Interrupt", "T32"}}
	for _, tc := range []struct {
		source string
		bad    bool
	}{
		{`package owner;type Transport struct{};func(t *Transport)Interrupt(){};func Run(t *Transport){f:=t.Interrupt;f()}`, false},
		{`package owner;type Transport struct{};type Other struct{};func(t *Transport)Interrupt(){};func(t *Other)Interrupt(){};func Run(t *Other){t.Interrupt()}`, true},
	} {
		transferWriteFixture(t, root, methodPath, tc.source)
		if err := checkInfraRuntimeRoots(read(), methodRule); (err != nil) != tc.bad {
			t.Fatalf("receiver/method-value root: %v want bad=%t", err, tc.bad)
		}
	}
	aliasPath := "internal/infraattach/alias.go"
	transferWriteFixture(t, root, aliasPath, `package owner;import leaf "github.com/LinZiyang666/tether/internal/infratransport";var packageAlias=leaf.Move;func AliasRoot(){packageAlias()}`)
	if err := checkInfraRuntimeRoots(read(), []infraRootRule{{aliasPath + ":AliasRoot", "internal/infratransport:helper", "T32"}}); err != nil {
		t.Fatal(err)
	}
	for _, control := range []struct {
		name string
		run  func(*testing.T)
	}{
		{"image", TestInfraImageStateControls},
		{"structure", TestInfraStructuralBudgetControls},
		{"layering", TestInfraLayeringControls},
		{"contexts", TestInfraContextRootControls},
		{"lifetime", TestInfraDataplaneLifetimeControls},
		{"tags", TestInfraBuildTagControls},
		{"budgets", TestInfraBudgetPinControls},
		{"pins", TestInfraNATSCatalogPinControls},
		{"ping", TestInfraPingProfileControls},
		{"supply", TestInfraSupplyControls},
		{"registry", TestInfraGateRegistryControls},
		{"release", TestInfraReleaseArtifactControls},
	} {
		t.Run(control.name, control.run)
	}
}

// AST field access matters to this gate, so a comment/string containing a
// selector never counts. Used by the budget, ping and lifetime controls.
func infraSelectors(file *ast.File) map[string]bool {
	result := map[string]bool{}
	imports := brokerImports(file)
	ast.Inspect(file, func(node ast.Node) bool {
		if value, ok := node.(*ast.SelectorExpr); ok {
			if id, ok := value.X.(*ast.Ident); ok && imports[id.Name] != "" {
				result[strings.TrimPrefix(imports[id.Name], modPrefix)+":"+value.Sel.Name] = true
			}
		}
		return true
	})
	return result
}
