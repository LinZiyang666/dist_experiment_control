package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
)

// admit_ordering_test.go (batch B, B1) — the guard for a defect the behavioural net cannot see.
//
// THE DEFECT THIS EXISTS BECAUSE OF
//
// The first version of the admit() conversion called ONE admit() that did both the subject
// parse and the storage-backed ACL checks, and only then consulted isClusterFollower(). That
// moved three DB reads (IsActive, IsMember/IsOwner, LookupStatus) to BEFORE the follower short
// circuit.
//
// The cost is not incidental. exec, run, kill and expose-rm are all listed in
// isBroadcastClusterSubject (clusterwrite.go:59-80), so every broker in the cluster receives
// every one of these requests and each follower returns silently. Reading storage first turns
// one ctl command into 3*(N-1) extra RODB queries whose results cannot affect any reply.
//
// It passed every test in the repo. ingress_characterization_test.go — the net written
// specifically to prove this conversion behaviour-preserving — constructs brokers with
// `clusterMode` false, so isClusterFollower() is ALWAYS false there and the reordering is
// invisible by construction. A behavioural test for it needs a real raft node in follower
// state, which is exactly the fixture cost that made the original hand-copied prologues never
// get a negative-case net in the first place.
//
// So this guard is STRUCTURAL: it asserts, from the AST, that each handler consults its
// cluster-role short circuit BEFORE the storage half of the gate. A structural check cannot
// prove the handler is correct — but it can prove this specific ordering did not silently
// invert, which is the failure that actually happened.
//
// (docs/testing-standards.md §S1's own lesson applies in reverse here: "结构检查与行为检查互相
// 替代不了" — the behavioural net covers what the codes are, this covers when storage is touched.)

// shortCircuitGuarded names each converted handler that MUST consult a cluster-role predicate
// between admitSubject and admitACL, and the predicate it uses. upgrade is deliberately absent:
// it has no short circuit at all (its subject is queue-grouped and broker.go:1005-1013 records
// the incident behind that), so it calls the combined admit().
var shortCircuitGuarded = map[string]string{
	"handleExecReq":     "isClusterFollower",
	"handleRunReq":      "isClusterFollower",
	"handleKillReq":     "isClusterFollower",
	"handleExposeRmReq": "isClusterFollower",
}

// combinedAdmitAllowed names the handlers permitted to call the one-shot admit(). Any OTHER
// handler calling it is either missing a short circuit or has just had one deleted.
var combinedAdmitAllowed = map[string]bool{
	"handleUpgradeReq": true,
	// handleExposeReq runs its leader check BEFORE parsing (an ordering difference from every
	// other converted verb, preserved deliberately), so by the time it reaches the gate there
	// is nothing left to interleave.
	"handleExposeReq": true,
	"admit":           true, // the wrapper itself
}

func TestClusterShortCircuitPrecedesStorageChecks(t *testing.T) {
	files := []string{"exec.go", "run.go", "expose.go", "upgrade.go"}
	seen := map[string]bool{}
	visited := 0

	for _, file := range files {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, file, nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", file, err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			visited++
			name := fd.Name.Name
			calls := callSequence(fd)

			if predicate, guarded := shortCircuitGuarded[name]; guarded {
				seen[name] = true
				iSubject := indexOf(calls, "admitSubject")
				iGuard := indexOf(calls, predicate)
				iACL := indexOf(calls, "admitACL")
				switch {
				case iSubject < 0:
					t.Errorf("%s no longer calls admitSubject — if it went back to a hand-written "+
						"prologue, this guard is now watching nothing", name)
				case iGuard < 0:
					t.Errorf("%s no longer calls %s. Losing the short circuit means every broker in "+
						"the cluster acts on this verb, not just the one that should", name, predicate)
				case iACL < 0:
					t.Errorf("%s calls admitSubject but never admitACL — the session, membership and "+
						"node checks are GONE. This is the authorization boundary", name)
				case iSubject >= iGuard || iGuard >= iACL:
					t.Errorf("%s calls in the order %v. Required: admitSubject -> %s -> admitACL.\n"+
						"If %s moved after admitACL, a follower now performs three storage reads for "+
						"every request it is about to drop. No behavioural test in this repo sees that: "+
						"ingress_characterization_test.go builds single-mode brokers, where %s is "+
						"always false.", name, calls, predicate, predicate, predicate)
				}
			}

			if i := indexOf(calls, "admit"); i >= 0 && !combinedAdmitAllowed[name] {
				t.Errorf("%s calls the combined admit(), which does the storage checks with nothing "+
					"able to run between the parse and them. Either it needs a cluster-role short "+
					"circuit (use admitSubject/admitACL) or it must be added to combinedAdmitAllowed "+
					"with a reason", name)
			}
		}
	}

	if visited < 20 {
		t.Fatalf("only %d function declarations visited across %v — the AST walk is broken, and a "+
			"broken walk reports every ordering correct", visited, files)
	}
	for name := range shortCircuitGuarded {
		if !seen[name] {
			t.Errorf("handler %s was not found in %v — it was renamed or moved, and this guard "+
				"silently stopped covering it", name, files)
		}
	}
}

// callSequence returns the callee names in source order, including selector calls (b.foo() ->
// "foo"). Nested function literals are included, which is what we want: a short circuit hidden
// inside a closure still runs where it appears.
func callSequence(fd *ast.FuncDecl) []string {
	var out []string
	ast.Inspect(fd, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fn := call.Fun.(type) {
		case *ast.Ident:
			out = append(out, fn.Name)
		case *ast.SelectorExpr:
			out = append(out, fn.Sel.Name)
		}
		return true
	})
	return out
}

func indexOf(xs []string, want string) int {
	for i, x := range xs {
		if x == want {
			return i
		}
	}
	return -1
}

// TestCallSequenceSelfCheck proves callSequence preserves SOURCE ORDER and sees through method
// selectors. Without it, a walker that returned calls in map order — or that missed b.foo()
// entirely — would make every ordering assertion above vacuously true.
func TestCallSequenceSelfCheck(t *testing.T) {
	const src = `package broker
func sample() {
	first()
	b.second()
	if b.third() {
		return
	}
	b.fourth()
}`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "sample.go", src, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var fd *ast.FuncDecl
	for _, d := range f.Decls {
		if x, ok := d.(*ast.FuncDecl); ok && x.Name.Name == "sample" {
			fd = x
		}
	}
	if fd == nil {
		t.Fatal("sample not found")
	}
	got := callSequence(fd)
	want := []string{"first", "second", "third", "fourth"}
	if len(got) != len(want) {
		t.Fatalf("callSequence returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("callSequence is not in source order: got %v, want %v", got, want)
		}
	}
	if indexOf(got, "third") <= indexOf(got, "second") {
		t.Fatal("indexOf does not reflect source order — every ordering assertion is vacuous")
	}
	if indexOf(got, "absent") != -1 {
		t.Fatal("indexOf must report -1 for an absent call, else a DELETED short circuit reads as " +
			"position 0 and passes the ordering check")
	}
}
