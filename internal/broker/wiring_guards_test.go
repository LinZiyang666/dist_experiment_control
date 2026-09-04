package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
)

// wiring_guards_test.go — the helpers this batch added are actually CALLED.
//
// origin: prerelease audit round 2, F-T3 / F-T4 / G6 (and the same shape in G-2, J4,
// K-F2, AC-3).
//
// The pattern the review found in eight of my guards: the test drives the new pure
// function directly and nothing anywhere asserts the production path uses it. Deleting
// the call site leaves the whole suite green, so the fix can be reverted invisibly —
// which is the only failure mode that matters for a fix nobody will look at again.
//
// A source-shape assertion is a weaker instrument than a behavioural test and is chosen
// deliberately here: every one of these call sites sits behind a live NATS connection, a
// JetStream handle or a raft cluster, i.e. a deploy-tier fixture. An unwired guard is
// worse than a source-shape one.

// wiredCall is one production call site that must not silently disappear.
type wiredCall struct {
	file    string
	fn      string
	call    string
	because string
}

var requiredWirings = []wiredCall{
	{
		file: "transfer.go", fn: "handlePushReq", call: "transferRequestBounds(",
		because: "BT-F5's length bounds. Unwired, one push.req can carry max_payload into the " +
			"tracker and the ~200 KiB memory bound its own comment claims becomes 1024 * max_payload.",
	},
	{
		file: "transfer.go", fn: "handlePullReq", call: "transferRequestBounds(",
		because: "the pull sibling — the tracker they feed is one map, so bounding only one of " +
			"them leaves the bound unreached.",
	},
	{
		file: "transfer_reconcile.go", fn: "reapBucketObjects", call: "xferObjectReapFloor(",
		because: "BT-F4's uptime-decaying floor. Unwired, a restarted broker deletes a 2-GiB " +
			"tier-B object three minutes into its 35m08s budget, out from under the agent reading it.",
	},
	{
		file: "clusterwrite.go", fn: "setSessionCreator", call: "assertAllVotersSupportSessionCreatorOps(b.cl.admin,",
		because: "the mixed-version gate on the admission write. Unwired, a `session-allow` is " +
			"proposed while a voter that predates OpSessionCreatorSet poison-skips it: the CLI " +
			"reports success, the leader's session_creators row exists and that voter's does not, " +
			"and which broker a ctl reaches is decided by a NATS queue group. The unit test drives " +
			"checkVotersSupportOps directly, so deleting this call leaves it green — which is the " +
			"failure mode this whole file exists for. origin: increment 2 internal review, six lanes.",
	},
	{
		file: "xfer_inflight.go", fn: "xferInflightDir", call: "xferLedgerSubdir(",
		because: "#57's single-broker ledger fallback. Unwired, install.sh's commented-out " +
			"data_dir means the ledger does not exist and the protected set is always empty — " +
			"which the reaper reads as 'nobody owns any of these objects'.",
	},
}

func TestTheseFixesAreWiredIntoProduction(t *testing.T) {
	for _, w := range requiredWirings {
		t.Run(w.fn+"/"+strings.TrimSuffix(w.call, "("), func(t *testing.T) {
			src, err := os.ReadFile(w.file)
			if err != nil {
				t.Fatalf("read %s: %v", w.file, err)
			}
			body := funcBodyText(t, w.file, string(src), w.fn)
			if !strings.Contains(body, w.call) {
				t.Fatalf("%s does not call %s.\n\nWhy it matters: %s\n\n"+
					"The unit test for this helper passes either way, which is exactly how a fix "+
					"gets reverted without a single gate going red.", w.fn, w.call, w.because)
			}
		})
	}
}

// funcBodyText returns the source text of the named function's body, or fails loudly.
// A scanner that silently finds nothing reports no offenders, which is indistinguishable
// from success — so a missing function is a hard failure, not a skip.
func funcBodyText(t *testing.T, filename, src, name string) string {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, filename, src, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", filename, err)
	}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name.Name != name {
			continue
		}
		return src[fset.Position(fn.Body.Pos()).Offset:fset.Position(fn.Body.End()).Offset]
	}
	t.Fatalf("SELF-CHECK FAILED: %s not found in %s — this guard is scanning for a function that "+
		"no longer exists, so it can never report anything", name, filename)
	return ""
}
