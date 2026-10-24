//go:build linux

package agent

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 plan §7.1 / TestBootstrapFlowRestoreStartsNoWorkers
// The constructor runs through the real dispatched-image path. Flow owner
// import itself is additionally exercised by T38 once P1/P7/P17 are integrated.
func TestBootstrapFlowRestoreStartsNoWorkers(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	before, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("restore", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			exerciseBootstrapFlowRestore(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "dormant flow bootstrap", before)
	testharness.AssertNoFDLeak(t, "dormant flow bootstrap", fds, 0)
}

func exerciseBootstrapFlowRestore(t *testing.T) {
	a := newBootAgent(t, "nats://127.0.0.1:1", "node")
	startBootTask(t, a)
	frozen := freezeBootAgent(t, a)
	image, _ := cutBootImage(t, a, frozen)
	adapter := NewTunnelExposeAdapter("127.0.0.1:1", "lab", "node", testharness.SilentLog())
	b, err := New(Config{NATSURL: "nats://127.0.0.1:1", SID: "lab", NID: "node",
		Home: t.TempDir(), BootImage: image, ExposeAdapter: adapter,
		RemoteFSMode: "off", Logger: testharness.SilentLog()})
	if err != nil {
		t.Fatal(err)
	}
	cleanupBootAgent(t, b)
	t.Cleanup(func() { stopExposeAdapter(b) })
	if adapter.started || !b.processRuntime.Frozen() || !b.workloads.Frozen() {
		t.Fatal("constructor started an adapter or activated a restored owner")
	}
	if adapter.client.SessionCount() != 0 {
		t.Fatal("constructor registered a carrier")
	}
	// origin: hot-update S5 plan §3.4 / §7.1. An exec-only import still
	// needs a dormant v1 owner for the next OPEN and the next complete cut.
	if adapter.client.FlowCensus().Unsupported || imageOwners(b).Flow != adapter.client || b.replayBudget == nil {
		t.Fatal("constructor omitted the dormant typed flow owner or shared node budget")
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S5 plan §7.1 / T38
// This source guard complements T38's actual network oracle. In particular it
// remains runnable before the flow image dependencies have implementations.
func TestFlowBootstrapOrderKeepsNetworkBehindActivation(t *testing.T) {
	body, err := os.ReadFile("agent.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "agent.go", body, 0)
	if err != nil {
		t.Fatal(err)
	}
	var run *ast.FuncDecl
	for _, decl := range file.Decls {
		if f, ok := decl.(*ast.FuncDecl); ok && f.Name.Name == "Run" {
			run = f
		}
	}
	if run == nil {
		t.Fatal("Run missing")
	}
	calls := make(map[string]token.Pos)
	ast.Inspect(run.Body, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		name := ""
		switch f := call.Fun.(type) {
		case *ast.Ident:
			name = f.Name
		case *ast.SelectorExpr:
			name = f.Sel.Name
		}
		if calls[name] == 0 {
			calls[name] = call.Pos()
		}
		return true
	})
	for _, pair := range [][2]string{
		{"armUpgradeWatchdog", "activateRunImage"},
		{"stopProxyOnRunExit", "activateRunImage"},
		{"stopRestoredFlows", "activateRunImage"},
		{"activateRunImage", "startExposeAdapter"},
		{"startExposeAdapter", "loadRosterCacheAtBoot"},
		{"activateRunImage", "session"},
	} {
		if calls[pair[0]] == 0 || calls[pair[1]] == 0 || calls[pair[0]] >= calls[pair[1]] {
			t.Fatalf("bootstrap order violated: %s must precede %s", pair[0], pair[1])
		}
	}
	// CLI construction must never pre-start an adapter whose imported image has
	// not yet validated. This is a distinct entry point from Agent.New.
	cliBody, err := os.ReadFile("../../cmd/tether/agent.go")
	if err != nil {
		t.Fatal(err)
	}
	cliFile, err := parser.ParseFile(token.NewFileSet(), "cli-agent.go", cliBody, 0)
	if err != nil {
		t.Fatal(err)
	}
	ast.Inspect(cliFile, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if ok {
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel.Name == "Start" {
				t.Error("CLI starts a worker before Agent.New/Run")
			}
		}
		return true
	})
}

// origin: hot-update S5 plan §6.5 / §7.1 / T38
// Imported keys are actor state, never proof of present permission. Before the
// independent SS trace can run, this guard also rejects an activation/constructor
// shortcut that calls Authorize or SetKeys using the imported keyset.
func TestFlowRestoreAuthorizesOnlyFromResumeReply(t *testing.T) {
	body, err := os.ReadFile("bootstrap.go")
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "bootstrap.go", body, 0)
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if selector.Sel.Name == "SetKeys" || (selector.Sel.Name == "Authorize" && fn.Name.Name != "authorizeRestoredProxy") {
				t.Errorf("%s releases imported SS authority without a fresh RESUME reply", fn.Name.Name)
			}
			if selector.Sel.Name == "Authorize" {
				found++
			}
			return true
		})
	}
	if found != 1 {
		t.Fatalf("fresh RESUME authorization sites=%d, want 1", found)
	}
}
