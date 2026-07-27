package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// write_verbs_test.go (batch B, B2) — the dispatch table.
//
// WHAT THE TABLE IS AND IS NOT FOR
//
// It replaced a 17-arm switch in dispatchForward. The roadmap sold that as "collapse a 5-way
// shotgun", but the plan's own audit found the shotgun was smaller than claimed: only 10 of the
// 17 verbs have BOTH a leader-local closure and a forward arm, and the one real defect that shape
// caused (leader and follower planning against different values) was closed separately by
// allocIdentity, in ten lines, before this table existed.
//
// So the table's remaining value is narrow and worth stating plainly: it makes the decode type
// and the Plan call impossible to disagree, because a single generic builder owns both. The old
// arms spelled out `var p T` and then a Plan call, and nothing but reading them side by side kept
// the two in agreement. That is a real property, and it is what these tests pin.
//
// It is NOT a claim that adding a verb is now safe by construction. A new entry still has to
// appear in frozenForwardVerbs (TestForwardVerbConstantsAreAllFrozen), have a frozen payload
// (TestEveryDispatchedPayloadTypeIsFrozen), and agree with verbAllowsReqID (below).

// TestWriteVerbsCoversEveryConstant is the completeness check in the direction the freeze does
// not cover: every Verb* constant declared in cluster_forward.go must have a table entry.
// TestDispatchForwardCoversEveryFrozenVerb checks table-vs-FREEZE; this checks table-vs-SOURCE, so
// a verb added to the const block and forgotten in both places is still caught.
func TestWriteVerbsCoversEveryConstant(t *testing.T) {
	consts := scanVerbConsts(t, "cluster_forward.go")
	if len(consts) < 10 {
		t.Fatalf("verb-const scan found %d consts — broken scan (17 today)", len(consts))
	}
	for name := range consts {
		if _, ok := writeVerbs[verbValueOf(t, name)]; !ok {
			t.Errorf("verb constant %s has no writeVerbs entry — a forward for it falls through to "+
				"the unknown-verb error, which reads to an operator as a version-skew bug rather "+
				"than a missing arm", name)
		}
	}
	if len(writeVerbs) != len(consts) {
		t.Errorf("writeVerbs has %d entries but %d verb constants exist — one side gained something "+
			"the other did not", len(writeVerbs), len(consts))
	}
}

// verbValueOf maps a Verb* constant NAME to its value, so the source scan (which yields names)
// can index the table (which is keyed by value).
func verbValueOf(t *testing.T, name string) string {
	t.Helper()
	v, ok := map[string]string{
		"VerbProvision": VerbProvision, "VerbJoin": VerbJoin, "VerbReconcile": VerbReconcile,
		"VerbTransferAudit": VerbTransferAudit, "VerbAlertSignal": VerbAlertSignal,
		"VerbAlertAck": VerbAlertAck, "VerbSessionCreate": VerbSessionCreate,
		"VerbBusNkeySet": VerbBusNkeySet, "VerbPortFree": VerbPortFree,
		"VerbPortRevoke": VerbPortRevoke, "VerbPortFreeAllocation": VerbPortFreeAllocation,
		"VerbNodeRegister": VerbNodeRegister, "VerbProcInsert": VerbProcInsert,
		"VerbProcMarkExited": VerbProcMarkExited, "VerbSessionTombstone": VerbSessionTombstone,
		"VerbSessionDrop": VerbSessionDrop, "VerbNodeEvict": VerbNodeEvict,
	}[name]
	if !ok {
		t.Fatalf("verb constant %s is not bound in this test — add it (and to frozenForwardVerbs)", name)
	}
	return v
}

// TestReqIDGateRunsBeforeTheTable pins the CC-4 contract's POSITION, which is the one thing the
// conversion could have moved without any test noticing.
//
// The gate rejects a non-empty ReqID for every verb not on the allow-list, AT THE WIRE BOUNDARY.
// cluster_forward.go's own comment records why: a future verb must not be able to reintroduce the
// external-F1 stale-ledger false-success. If the gate moved inside a handler, a verb whose handler
// forgot it would accept a forged ReqID — and the only visible symptom would be a write that
// dedups against a stale ledger entry and reports success without committing.
func TestReqIDGateRunsBeforeTheTable(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "cluster_forward.go", nil, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	var body []string
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "dispatchForward" {
			continue
		}
		ast.Inspect(fd, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.Ident:
				if x.Name == "verbAllowsReqID" || x.Name == "writeVerbs" {
					body = append(body, x.Name)
				}
			}
			return true
		})
	}
	iGate, iTable := indexOf(body, "verbAllowsReqID"), indexOf(body, "writeVerbs")
	switch {
	case iGate < 0:
		t.Fatal("dispatchForward no longer consults verbAllowsReqID — the CC-4 wire-boundary ReqID " +
			"gate is GONE, and a forged ReqID on any verb can dedup against a stale ledger entry")
	case iTable < 0:
		t.Fatal("dispatchForward no longer consults writeVerbs — this test is watching the wrong function")
	case iGate > iTable:
		t.Errorf("the ReqID gate now runs AFTER the table lookup (order: %v). It must stay at the "+
			"wire boundary: a per-handler gate is one a new verb can forget.", body)
	}
}

// TestUnknownVerbErrorIsByteStable pins the operator-facing string. It travels back through the
// forward reply and, for the alert-ack path (cluster_health.go), straight to a terminal.
func TestUnknownVerbErrorIsByteStable(t *testing.T) {
	err := dispatchForward(nil, nil, forwardEnvelope{Verb: "no-such-verb"})
	if err == nil {
		t.Fatal("an unknown verb must be an error")
	}
	const want = `cluster_forward: unknown verb "no-such-verb"`
	if err.Error() != want {
		t.Errorf("unknown-verb error changed:\n got: %q\nwant: %q\n"+
			"This string reaches an operator terminal; a reworded version breaks their grep and any "+
			"runbook that quotes it.", err.Error(), want)
	}
}

// TestReqIDRejectionPrecedesUnknownVerb pins a subtle consequence of the gate's position: a
// forged ReqID on a verb that does not even exist must be rejected as a ReqID violation, not as an
// unknown verb. Otherwise an attacker probing verb names learns which ones exist by the error they
// get back.
func TestReqIDRejectionPrecedesUnknownVerb(t *testing.T) {
	err := dispatchForward(nil, nil, forwardEnvelope{Verb: "no-such-verb", ReqID: "forged"})
	if err == nil {
		t.Fatal("a non-empty ReqID on a non-allow-listed verb must be rejected")
	}
	if !strings.Contains(err.Error(), ErrReqIDNotAllowed.Error()) {
		t.Errorf("got %q, want the ReqID rejection — the gate must run before the table lookup so "+
			"the error does not disclose whether the verb exists", err.Error())
	}
}

// TestVerbAllowsReqIDAgreesWithTheBuilders is the consistency check between the two places that
// encode "does this verb carry a boundary-minted ReqID": the verbAllowsReqID map and the choice of
// propose() vs proposeWithReqID() in the table.
//
// A verb built with proposeWithReqID but absent from verbAllowsReqID can NEVER receive a ReqID —
// the gate rejects it first, so its idempotency key is silently always empty. The reverse (on the
// allow-list, built with plain propose) accepts a ReqID and then discards it, which is worse: the
// caller believes the write is deduplicated and it is not.
func TestVerbAllowsReqIDAgreesWithTheBuilders(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "cluster_forward.go", nil, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	builtWithReqID := map[string]bool{}
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.VAR {
			continue
		}
		for _, spec := range gd.Specs {
			vs, ok := spec.(*ast.ValueSpec)
			if !ok || len(vs.Names) == 0 || vs.Names[0].Name != "writeVerbs" || len(vs.Values) == 0 {
				continue
			}
			lit, _ := vs.Values[0].(*ast.CompositeLit)
			if lit == nil {
				continue
			}
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				call, ok := kv.Value.(*ast.CallExpr)
				if !ok {
					continue
				}
				fn, ok := call.Fun.(*ast.Ident)
				if !ok {
					continue
				}
				if id, ok := kv.Key.(*ast.Ident); ok && fn.Name == "proposeWithReqID" {
					builtWithReqID[id.Name] = true
				}
			}
		}
	}
	if len(builtWithReqID) == 0 {
		t.Fatal("no table entry uses proposeWithReqID — either the scan is broken or reconcile lost " +
			"its idempotency key; both are worth failing on")
	}
	for name := range builtWithReqID {
		if !verbAllowsReqID[verbValueOf(t, name)] {
			t.Errorf("%s is built with proposeWithReqID but is NOT in verbAllowsReqID — the wire "+
				"gate rejects its ReqID before the handler runs, so its idempotency key is always "+
				"empty and a forwarder retry double-commits", name)
		}
	}
	for verb := range verbAllowsReqID {
		found := false
		for name := range builtWithReqID {
			if verbValueOf(t, name) == verb {
				found = true
			}
		}
		if !found {
			t.Errorf("verb %q is on the ReqID allow-list but its table entry uses plain propose() — "+
				"it accepts a ReqID and then discards it, so the caller believes the write is "+
				"deduplicated when it is not", verb)
		}
	}
}

// NOTE: there is deliberately no test that propose() "decodes into the declared type". That
// property is guaranteed by the generic parameter itself — propose[P] unmarshals into a `var p P`
// and P is the only thing it can hand the plan closure — so a test for it would assert what the
// compiler already enforces. Writing one anyway was the first draft of this file; it is recorded
// here rather than deleted silently, because "add a test for everything" produces exactly the
// vacuous assertions this batch spent its review budget removing.
