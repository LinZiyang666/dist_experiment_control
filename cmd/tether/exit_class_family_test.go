package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/spf13/cobra"
)

// exit_class_family_test.go — a refusal's exit code has to say whether retrying can help.
//
// docs/usage.md §9.13 defines 70 as "back off and retry", so an UNCLASSIFIED refusal is
// not merely uninformative: it actively instructs automation to retry things that can
// never succeed by retrying.

// TestExecRefusalsCarryTheSameClassAsRun is CLI-F1.
//
// execFailureMessage returned a bare error, so classifyExit had nothing to key on and
// every broker or agent refusal of `tether exec` came out as 70 — including
// not_a_member (a permission decision), node_offline (needs a human) and argv_required
// (a malformed request). The IDENTICAL refusal through `tether run` was classified
// correctly. Two readers of one wire shape, disagreeing.
func TestExecRefusalsCarryTheSameClassAsRun(t *testing.T) {
	// Reasons drawn from the shared broker-code table, so this cannot drift into
	// asserting a private list.
	for code := range brokerCodeExitClasses {
		t.Run(code, func(t *testing.T) {
			want := classifyExit(runFailureMessage(code))
			got := classifyExit(execFailureMessage(code))
			if got != want {
				t.Fatalf("`exec` exits %d for %q while `run` exits %d for the SAME refusal.\n\n"+
					"70 means 'retry me' to every caller following docs/usage.md §9.13. A "+
					"permission decision or a malformed request will never succeed on a retry, "+
					"so the two verbs disagreeing is the difference between an automation that "+
					"stops and one that spins.", got, code, want)
			}
		})
	}

	// The detail form must key on the CODE, not the whole string — the split has to come
	// before the trim, or " store_error: detail" keys on nothing.
	//
	// THE PROBE CODE MUST BE ONE THE TABLE ACTUALLY MAPS — origin: prerelease audit
	// round 2, J3. This used `store_error`, which is NOT in brokerCodeExitClasses, so
	// its class is the exitInternal fallback — byte-identical to what a FAILED lookup
	// returns. Both sides of the comparison were the fallback and the assertion held
	// with the split removed entirely, which is the one edit it exists to catch.
	// `not_a_member` maps to exitNoPerm, so a lookup that misses is now visible.
	const mappedCode = "not_a_member"
	if brokerCodeExitClass(mappedCode) == exitInternal {
		t.Fatalf("premise broken: %q now classifies as exitInternal, which is also the "+
			"lookup-failure fallback — this assertion can no longer distinguish them", mappedCode)
	}
	withDetail := classifyExit(execFailureMessage(mappedCode + ": disk on fire"))
	bare := classifyExit(execFailureMessage(mappedCode))
	if withDetail != bare {
		t.Errorf("a reason carrying a detail classified as %d while the bare code classified as %d; "+
			"the colon split is not reaching the table", withDetail, bare)
	}
	if withDetail == exitInternal {
		t.Errorf("both forms fell through to the unclassified %d; the split reached the table "+
			"with nothing in it", exitInternal)
	}

	// And the operator-visible text must be unchanged by the classification.
	if msg := execFailureMessage("not_a_member").Error(); !strings.Contains(msg, "exec:") {
		t.Errorf("the message lost its prefix: %q", msg)
	}
}

// TestEveryAbortRefusalIsAUsageError is CLI-F2.
//
// A confirmation prompt the operator declined is a usage outcome, not a transient one.
// Ten sites returned a bare "aborted (...)" error and exactly ONE carried a class, so
// the exception looked like the rule.
func TestEveryAbortRefusalIsAUsageError(t *testing.T) {
	// Source-level, because each site is only reachable behind an interactive prompt.
	// What has to hold is that no site constructs the refusal WITHOUT a class.
	//
	// AST OVER THE WHOLE PACKAGE, not a line scan of five named files — origin:
	// prerelease audit round 2, J7. The first version could not fail for a SIXTH file
	// (a new command with a new prompt is exactly how this recurs), for a call built
	// across two lines, or for errors.New — three shapes that are not exotic, they are
	// what gofmt produces the moment a message grows.
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse cmd/tether: %v", err)
	}
	checked := 0
	for _, pkg := range pkgs {
		for name, file := range pkg.Files {
			ast.Inspect(file, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok || !isPlainErrorConstructor(call) {
					return true
				}
				checked++
				if !callMentionsAbort(call) {
					return true
				}
				t.Errorf("%s:%d: an abort refusal is constructed without an exit class.\n\n"+
					"It becomes a 70, which docs/usage.md §9.13 tells automation to retry — and a "+
					"prompt the operator declined is not going to answer differently next time.",
					name, fset.Position(call.Pos()).Line)
				return true
			})
		}
	}
	if checked == 0 {
		t.Fatal("the walk found no fmt.Errorf/errors.New calls at all in cmd/tether — it is not " +
			"looking at the source it claims to")
	}
}

// isPlainErrorConstructor reports whether call is fmt.Errorf or errors.New, i.e.
// an error built with NO exit class attached. usageErr / unavailErr and the rest
// of the classed constructors are deliberately not matched.
func isPlainErrorConstructor(call *ast.CallExpr) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return (pkg.Name == "fmt" && sel.Sel.Name == "Errorf") ||
		(pkg.Name == "errors" && sel.Sel.Name == "New")
}

// callMentionsAbort reports whether any string literal ANYWHERE in the call names
// an abort — including a message split across several concatenated lines, which
// the old single-line scan could not see.
func callMentionsAbort(call *ast.CallExpr) bool {
	found := false
	ast.Inspect(call, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		if strings.Contains(lit.Value, "aborted") {
			found = true
		}
		return true
	})
	return found
}

// TestTheDestructiveGateSplitsTransientFromPersistent is the other half of CLI-F2.
func TestTheDestructiveGateSplitsTransientFromPersistent(t *testing.T) {
	cases := []struct {
		name string
		gate proto.DestructiveGate
		want int
	}{
		{"quorum lost is transient", proto.DestructiveGate{QuorumLost: true}, exitTransient},
		{"force-single is persistent", proto.DestructiveGate{ForceSingleActive: true}, exitUsage},
		{"both: the persistent one wins", proto.DestructiveGate{QuorumLost: true, ForceSingleActive: true}, exitUsage},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := gateExitClass(tc.gate); got != tc.want {
				t.Fatalf("exit class %d, want %d.\n\n"+
					"Waiting fixes a lost quorum and does not fix a cluster running on one emergency "+
					"broker. Telling automation to retry the second one is telling it to spin until "+
					"a human happens to notice.", got, tc.want)
			}
		})
	}
}

// TestAlertAckWaitsAtLeastAsLongAsTheBrokerDoes is CLI-F3.
//
// `alert ack` is not a local read: the receiving broker forwards it to the raft leader
// and waits for the commit, with its OWN budget of ExposeForwardTimeout (5s). A 500ms
// ctl deadline meant the ctl gave up first on every ack that had to cross to a leader,
// and reported "no broker forwarded the ack" for one that was being committed.
func TestAlertAckWaitsAtLeastAsLongAsTheBrokerDoes(t *testing.T) {
	// READ THE BROKER'S OWN NUMBER, do not restate it — origin: prerelease audit round
	// 2, J4. This was a hardcoded `5 * 1000`, i.e. the very number the comment above it
	// says the constant must be measured against, copied. Raise the broker's forward
	// budget to 10s and this guard stays green while the ctl once again gives up first
	// and reports "no broker forwarded the ack" for an ack that is committing.
	brokerForwardBudget := (&broker.Config{}).ExposeForwardTimeout()
	if alertAckTimeout < brokerForwardBudget {
		t.Fatalf("alertAckTimeout is %v, shorter than the broker's own %v forward budget.\n\n"+
			"The ctl then times out first and reports a failure for an ack that is committing. A "+
			"deadline measured against a number you do not control is not a deadline.",
			alertAckTimeout, brokerForwardBudget)
	}
	if alertAckTimeout == alertLsTimeout {
		t.Error("the ack and the banner-fetch share one constant again. They are different requests: " +
			"one is a local read, the other crosses to the raft leader and waits for a commit.")
	}
	// AND IT MUST BE WIRED. A constant with the right value that nothing reads is not a
	// deadline either: the first version asserted only the value, so replacing
	// ackAlert's timeout argument with alertLsTimeout left it green.
	src, err := readCLISource("alert_gate.go")
	if err != nil {
		t.Fatalf("read alert_gate.go: %v", err)
	}
	if !strings.Contains(src, "nc.Request(proto.SubjCtrlAlertAck(actor), body, alertAckTimeout)") {
		t.Error("ackAlert no longer passes alertAckTimeout to its Request.\n\n" +
			"Whatever budget it does pass is the one that decides whether the ctl outlives the " +
			"broker's forward, and this test is checking the other one.")
	}
}

// TestAgentSubcommandsDoNotSpendTheDaemonsBootBudget is L4-F3, and it reconciles
// against the REAL command tree so a new subcommand cannot quietly join the daemon.
func TestAgentSubcommandsDoNotSpendTheDaemonsBootBudget(t *testing.T) {
	root := newRootCmd()
	var agentCmd *cobra.Command
	for _, c := range root.Commands() {
		if c.Name() == "agent" {
			agentCmd = c
			break
		}
	}
	if agentCmd == nil {
		t.Fatal("SELF-CHECK FAILED: no `agent` command in the tree; this guard is testing nothing")
	}

	for _, sub := range agentCmd.Commands() {
		name := sub.Name()
		if !agentNonDaemonSubcommands[name] {
			t.Errorf("`tether agent %s` is not listed in agentNonDaemonSubcommands.\n\n"+
				"It therefore reaches isAgentDaemonInvocation's `return true` and consumes a tick of "+
				"the DAEMON's boot budget — the counter that decides whether a freshly staged binary "+
				"is considered to have booted. Add it, or say here why it really is daemon-like.", name)
		}
		if isAgentDaemonInvocation([]string{"tether", "agent", name}) {
			t.Errorf("`tether agent %s` is classified as the daemon", name)
		}
	}

	// The daemon itself, and a flag VALUE that happens to be a subcommand name, must
	// both still classify as the daemon — otherwise the exclusion is too eager and the
	// real daemon stops consuming its own budget.
	if !isAgentDaemonInvocation([]string{"tether", "agent"}) {
		t.Error("the bare `tether agent` daemon was excluded")
	}

	// `agent join --start` runs the daemon IN-PROCESS for the life of the host, so it
	// must consume the boot budget like any other daemon launch (round 2, J1). Without
	// this, agent.BootUpgradeCheck never runs on that shape and every `node upgrade`
	// against such an agent is structurally unable to commit.
	for _, argv := range [][]string{
		{"tether", "agent", "join", "invite://x", "--start"},
		{"tether", "agent", "join", "invite://x", "--start=true"},
		{"tether", "agent", "join", "--start", "invite://x"},
	} {
		if !isAgentDaemonInvocation(argv) {
			t.Errorf("`%v` runs the daemon in-process but was classified as a subcommand — it "+
				"loses the boot budget, so `node upgrade` can never commit against it", argv[1:])
		}
	}
	// ...and join WITHOUT --start really is just a subcommand.
	for _, argv := range [][]string{
		{"tether", "agent", "join", "invite://x"},
		{"tether", "agent", "join", "invite://x", "--start=false"},
	} {
		if isAgentDaemonInvocation(argv) {
			t.Errorf("`%v` does not run the daemon but consumed its boot budget", argv[1:])
		}
	}
	if !isAgentDaemonInvocation([]string{"tether", "agent", "--nid", "doctor"}) {
		t.Error("`tether agent --nid doctor` was excluded: the predicate is matching a flag VALUE " +
			"against the subcommand table")
	}
}

func readCLISource(name string) (string, error) {
	b, err := os.ReadFile(name)
	return string(b), err
}
