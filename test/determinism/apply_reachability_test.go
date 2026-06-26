package determinism

import (
	"strings"
	"testing"

	"golang.org/x/tools/go/callgraph"
	"golang.org/x/tools/go/callgraph/cha"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/go/ssa"
	"golang.org/x/tools/go/ssa/ssautil"
)

// §13.1 Apply-reachability determinism lint (D2). The Plan/Apply contract says the
// replicated write path (FSM Apply) execs ONLY leader-baked SQL via the shared
// genericExecApplier — it must NEVER re-enter a self-generated mutator package
// (internal/{port,proc,node,session,agentprov}), because that is where the
// nondeterministic sources live (port's crypto/rand genToken, proc's oklog/ulid
// NewPID). Those imports are LEGAL at the package level — they back leader-only
// Plan* / agent-side code — so a package-level import ban would be wrong; the real
// guarantee is that NO mutator code is reachable from Apply.
//
// This MUST use a sound call graph (CHA), NOT a hand-rolled AST BFS: the FSM
// dispatches ops through `defaultAppliers() map[OpType]Applier` — an interface
// value in a map. A static BFS over resolvable call expressions cannot follow that
// dynamic dispatch and would declare the whole Apply subtree unreachable →
// vacuously green. CHA over-approximates interface dispatch (every concrete
// Applier.ApplyTx is an edge), so a poison applier that called back into a mutator
// WOULD be caught.
//
// Scope note: we check reachability into OUR mutator packages, NOT raw
// "reaches crypto/rand anywhere" — CHA's over-approximation routes Apply ->
// tx.Exec -> database/sql driver interfaces -> the modernc driver's internal RNG,
// which is irrelevant to op-level determinism. internal/cluster's own freedom from
// rand/ulid is covered separately by TestClusterApplyNoNondeterministicImports.
//
// Non-vacuity is proven by a POSITIVE control: the SAME walk from PlanAllocate DOES
// reach internal/port AND crypto/rand — so "Apply reaches no mutator" is a real
// result, not a broken walk that finds nothing anywhere.

const (
	pkgCluster   = "github.com/LinZiyang666/tether/internal/cluster"
	pkgPort      = "github.com/LinZiyang666/tether/internal/port"
	pkgProc      = "github.com/LinZiyang666/tether/internal/proc"
	pkgNode      = "github.com/LinZiyang666/tether/internal/node"
	pkgSession   = "github.com/LinZiyang666/tether/internal/session"
	pkgAgentprov = "github.com/LinZiyang666/tether/internal/agentprov"
)

// nondeterministicPkgs are the §3.4 nondeterminism sources forbidden on the
// replicated Apply path. They are LEGAL at package level (leader-only Plan* /
// agent-side code) — the lint forbids only a CALL into them reachable from Apply.
var nondeterministicPkgs = map[string]bool{
	"crypto/rand":              true,
	"math/rand":                true,
	"math/rand/v2":             true,
	"github.com/oklog/ulid/v2": true,
}

func loadCHA(t *testing.T) *callgraph.Graph {
	t.Helper()
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles |
			packages.NeedImports | packages.NeedDeps | packages.NeedTypes |
			packages.NeedSyntax | packages.NeedTypesInfo,
		Dir: repoRoot(t),
	}
	pkgs, err := packages.Load(cfg, pkgCluster, pkgPort, pkgProc, pkgNode, pkgSession, pkgAgentprov)
	if err != nil {
		t.Fatalf("packages.Load: %v", err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		t.Fatal("package load errors (see above)")
	}
	prog, _ := ssautil.AllPackages(pkgs, ssa.InstantiateGenerics)
	prog.Build()
	return cha.CallGraph(prog)
}

// findFunc returns the call-graph node for the function whose ssa string contains
// each of the given fragments (e.g. pkg path + receiver + ".Method").
func findFunc(t *testing.T, cg *callgraph.Graph, frags ...string) *callgraph.Node {
	t.Helper()
	var matches []*callgraph.Node
	for fn, node := range cg.Nodes {
		if fn == nil {
			continue
		}
		s := fn.String()
		ok := true
		for _, fr := range frags {
			if !strings.Contains(s, fr) {
				ok = false
				break
			}
		}
		if ok {
			matches = append(matches, node)
		}
	}
	// Disambiguate a substring collision (e.g. "internal/port.PlanAllocate" is also a substring of
	// "internal/port.PlanAllocateProxy"): when more than one matches, prefer the node whose qualified
	// name ENDS EXACTLY with the last fragment — an exact-suffix match excludes the longer sibling.
	if len(matches) > 1 {
		last := frags[len(frags)-1]
		var exact []*callgraph.Node
		for _, m := range matches {
			if strings.HasSuffix(m.Func.String(), last) {
				exact = append(exact, m)
			}
		}
		if len(exact) >= 1 {
			matches = exact
		}
	}
	if len(matches) > 1 {
		t.Fatalf("ambiguous function match for %v: %s AND %s", frags, matches[0].Func.String(), matches[1].Func.String())
	}
	if len(matches) == 0 {
		t.Fatalf("no function matched %v in the CHA graph", frags)
	}
	return matches[0]
}

const modulePrefix = "github.com/LinZiyang666/tether/"

func inModule(fn *ssa.Function) bool {
	return fn != nil && fn.Pkg != nil && strings.HasPrefix(fn.Pkg.Pkg.Path(), modulePrefix)
}

// reachableNodes returns every IN-MODULE call-graph node reachable from root,
// WITHOUT descending past our module boundary. Pruning at third-party packages
// (raft, stdlib, the modernc driver) is the load-bearing precision step: CHA
// over-approximates interface dispatch so a raw whole-program walk from fsm.Apply
// "reaches" raft's UUID rand, net/http, testing, etc. — none of which is OUR op
// code. The §3.4 rule is about OUR self-generated mutators' keys/fences; a third
// party's internal RNG does not affect replicated logical content.
func reachableNodes(root *callgraph.Node) []*callgraph.Node {
	seen := map[*callgraph.Node]bool{root: true}
	var nodes []*callgraph.Node
	queue := []*callgraph.Node{root}
	for len(queue) > 0 {
		n := queue[0]
		queue = queue[1:]
		if inModule(n.Func) {
			nodes = append(nodes, n)
		}
		for _, e := range n.Out {
			if seen[e.Callee] {
				continue
			}
			seen[e.Callee] = true
			if inModule(e.Callee.Func) {
				queue = append(queue, e.Callee)
			}
		}
	}
	return nodes
}

// bannedCallsFromNode returns the nondeterministic packages this node CALLS via
// ANY call-graph edge — static OR dynamic. Checking call-graph edges (not just
// instr.StaticCallee) is the Stage C T4 fix: a banned call routed through a
// function value / method value / interface (e.g. `var r = rand.Read; r(buf)`) has
// StaticCallee()==nil but a real CHA edge to crypto/rand.Read, which the old
// StaticCallee-only scan missed. The edge's callee package is checked even when
// third-party, so `crypto/rand.Read` is caught while `(*sql.Tx).Exec` (database/sql,
// not banned) is not — no over-approximation, because we only inspect the
// out-edges of IN-MODULE nodes.
func bannedCallsFromNode(n *callgraph.Node) []string {
	var hits []string
	seen := map[string]bool{}
	for _, e := range n.Out {
		if e.Callee == nil || e.Callee.Func == nil || e.Callee.Func.Pkg == nil {
			continue
		}
		p := e.Callee.Func.Pkg.Pkg.Path()
		if nondeterministicPkgs[p] && !seen[p] {
			seen[p] = true
			hits = append(hits, p)
		}
	}
	return hits
}

func TestApplyReachability_NoNondeterministicImports(t *testing.T) {
	cg := loadCHA(t)

	// Root: the FSM Apply method. (*cluster.fsm).Apply is the raft entry point;
	// applyCommand + the applier dispatch hang off it.
	applyRoot := findFunc(t, cg, "internal/cluster.fsm).Apply")
	applyNodes := reachableNodes(applyRoot)
	// floor guard: a wrong/empty root would make the lint vacuously green. Apply
	// genuinely reaches applyCommand + the cursor read/writes + the applier.
	if len(applyNodes) < 4 {
		t.Fatalf("FSM Apply reaches only %d in-module nodes — root match likely wrong (lint would be vacuous)", len(applyNodes))
	}
	// NON-VACUITY (the negative-control equivalent the plan §6 asks for): prove CHA
	// actually traverses the map[OpType]Applier interface dispatch into the concrete
	// applier — i.e. the Apply subtree is NOT severed. If genericExecApplier.ApplyTx
	// is reachable from fsm.Apply, then a POISON applier registered in the same map
	// would ALSO be reachable, and its banned out-edge caught below. (A hand-rolled
	// BFS over static call exprs would FAIL to find ApplyTx — the whole point of CHA.)
	reachesApplier := false
	for _, n := range applyNodes {
		if strings.Contains(n.Func.String(), "genericExecApplier") && strings.Contains(n.Func.String(), "ApplyTx") {
			reachesApplier = true
		}
	}
	if !reachesApplier {
		t.Fatal("non-vacuity FAILED: fsm.Apply does not reach genericExecApplier.ApplyTx through the " +
			"map/interface dispatch — CHA isn't following the applier dispatch, so a poison applier " +
			"would escape the lint (the lint would be vacuously green).")
	}

	for _, n := range applyNodes {
		if hits := bannedCallsFromNode(n); len(hits) > 0 {
			t.Errorf("FSM Apply REACHES %s which calls nondeterministic %v — a replicated write "+
				"path must not (architecture §3.3/§13.1). Move the nondeterminism into a leader-only Plan*.",
				n.Func.String(), hits)
		}
	}

	// POSITIVE control (non-vacuity): the SAME walk + check from PlanAllocate DOES
	// find a call into crypto/rand (genToken). If it doesn't, the walk/scan is
	// broken and the Apply-clean result above is meaningless.
	planRoot := findFunc(t, cg, "internal/port.PlanAllocate")
	foundRand := false
	for _, n := range reachableNodes(planRoot) {
		for _, h := range bannedCallsFromNode(n) {
			if h == "crypto/rand" {
				foundRand = true
			}
		}
	}
	if !foundRand {
		t.Fatal("positive control FAILED: PlanAllocate should reach a crypto/rand call (genToken) — " +
			"the reachability/scan finds nothing, so the Apply lint is vacuous")
	}
}
