package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// ps_totals_test.go — handlePsReq must report the section totals
// UNCONDITIONALLY, not only when a cap bit.
//
// origin: docs/reviews/h1-external-review.md, the "疑惑/低风险残留" list (the
// PsResp "byte-identical" correction), plus my own mutation check of the guard
// written for it.
//
// # WHY THIS TEST EXISTS SEPARATELY FROM THE PROTO ONE
//
// internal/proto/ps_truncation_test.go pins the STRUCT's wire shape: with
// `omitempty`, a zero total is invisible and a non-zero one is not. That test
// is real and mutation-verified — but it can only see the struct. Its comment
// also asserted something about the HANDLER ("*_Total is assigned
// unconditionally"), and a mutation proved that half was unbacked: making
// handlePsReq send the totals only when truncated left every proto test green.
//
// An unbacked claim in a comment is worse than no claim, because the next
// person weighing this exact change reads it as already-tested. So the handler
// half gets its own assertion, in the package that owns the handler.
//
// # WHAT WOULD BREAK WITHOUT IT
//
// Sending *_Total only when a cap bit looks like a tidy way to restore the
// "byte-identical to v0.4.7" property the h1 comment used to claim. It is not
// tidy: an untruncated reply would then say `procs_total: 0` while listing N
// rows, and any consumer that reads the total as "how many exist" — a paging
// UI, an operator script, a future `--json` consumer — reads zero for a
// perfectly healthy session. The bit and the count answer different questions
// and both must always be present.
//
// Source-level rather than behavioural: driving handlePsReq needs a broker, a
// NATS conn and a seeded DB, and the property under test is one line of
// construction. The census style is the same one reply_egress_test.go uses.
func TestPsRespTotalsAreReportedUnconditionally(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "exec.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}

	var body *ast.FuncDecl
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Name.Name == "handlePsReq" {
			body = fn
			break
		}
	}
	if body == nil {
		t.Fatal("handlePsReq is gone from exec.go — this guard and the PsResp comment both need re-deriving")
	}

	// Find the PsResp composite literal and check how each total is filled.
	found := map[string]ast.Expr{}
	ast.Inspect(body.Body, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}
		sel, ok := lit.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "PsResp" {
			return true
		}
		for _, elt := range lit.Elts {
			kv, ok := elt.(*ast.KeyValueExpr)
			if !ok {
				continue
			}
			key, ok := kv.Key.(*ast.Ident)
			if !ok {
				continue
			}
			found[key.Name] = kv.Value
		}
		return true
	})

	if len(found) == 0 {
		t.Fatal("no proto.PsResp composite literal in handlePsReq — the reply is built some other way now")
	}

	for _, name := range []string{"ProcsTotal", "PortsTotal"} {
		val, ok := found[name]
		if !ok {
			t.Errorf("handlePsReq's PsResp literal no longer sets %s. A reply that omits the total "+
				"cannot distinguish 'no rows exist' from 'the count was not reported'", name)
			continue
		}
		// The honest form is a plain identifier (the counted value). Anything
		// conditional — a call wrapping an if, a ternary-ish closure, a
		// conditional expression — means the total became a function of the
		// truncation bit, which is exactly the regression this pins.
		if _, isIdent := val.(*ast.Ident); !isIdent {
			t.Errorf("%s is assigned a non-trivial expression (%T), not the counted value directly. "+
				"If that expression makes the total depend on whether a cap bit, an untruncated reply "+
				"will report 0 while listing rows — the count and the bit answer different questions. "+
				"If it is an honest refactor, update this guard deliberately", name, val)
		}
	}

	// The bits, by contrast, MUST be derived — a hard-coded false would make
	// every partial view silently claim to be complete.
	for _, name := range []string{"ProcsTruncated", "PortsTruncated"} {
		val, ok := found[name]
		if !ok {
			t.Errorf("handlePsReq no longer sets %s — a capped reply would render as the whole truth", name)
			continue
		}
		if id, isIdent := val.(*ast.Ident); isIdent && (id.Name == "false" || id.Name == "true") {
			t.Errorf("%s is hard-coded to %s. It must be derived from the counted total vs the "+
				"returned length, or the ctl's 'view is partial' banner becomes decoration", name, id.Name)
		}
	}

	// Belt: the CORRECTION on PsResp must stay put.
	//
	// origin: same review item — and the first cut of this arm was decoration.
	// It searched for the retracted claim while excusing any comment group that
	// also contained "FALSE"; since the correction itself says "That is FALSE",
	// the exclusion matched every time, and re-adding the claim to that same
	// group went undetected. (Found by mutating it, which is the only reason
	// this note can be written honestly.)
	//
	// So the check is inverted: pin the correction's PRESENCE. Anyone restoring
	// the byte-identity claim has to delete the paragraph that refutes it, and
	// deleting it is what this catches. That direction cannot be defeated by
	// adding text.
	msgs, err := parser.ParseFile(token.NewFileSet(), "../proto/messages.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatal(err)
	}
	correction := false
	for _, cg := range msgs.Comments {
		txt := cg.Text()
		if strings.Contains(txt, "byte-identical") && strings.Contains(txt, "That is FALSE") &&
			strings.Contains(txt, "omitempty` drops only the") {
			correction = true
			break
		}
	}
	if !correction {
		t.Error("the PsResp comment no longer carries the external review's correction. The retracted " +
			"claim was that an untruncated reply is byte-identical to v0.4.7; it is not, because *_Total " +
			"is set unconditionally and omitempty drops only the ZERO value, so any reply listing a row " +
			"carries procs_total. N-1 compatibility rests on additivity with a legal zero value. If the " +
			"correction was reworded, update this guard; if it was deleted, put it back — a wrong " +
			"justification for a right property is how the next wire change gets waved through")
	}
}
