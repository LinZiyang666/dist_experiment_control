// Adopted from the Stage-C internal review (CLAUDE.md §3 step 5): the reviewer authored these pins,
// the main process reviewed, renamed and now owns them.
package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
)

// ── G67 internal review, TEST-ADEQUACY lane ──────────────────────────────────────────────────────
//
// TestTransferRefusalCarriesExitClassWithoutChangingText exercises transferRefusalErr as a FUNCTION.
// Nothing exercises the CALL SITES — and that is the exact mistake its own doc comment records
// against the pre-G67 code ("the map entry existed but nothing consulted it, so it exited the
// unclassified 70 — caught on the deploy tier, not by any unit test"). Measured by mutation against
// the working tree:
//
//	revert ALL FIVE `transferRefusalErr(x.Code, ...)` call sites to plain fmt.Errorf
//	  -> `go test ./cmd/tether/` stays GREEN (64s, 0 failures)
//	  -> and drills/67 never asserts an rc either: it greps `code=jetstream_not_ready` out of stderr
//	     and only tests `[ "$_G67_RC" = 0 ]` to detect "no refusal happened".
//	  -> drills/61's refuse_clean helper likewise only checks rc != 0.
//
//	pass a literal 0 instead of nc.MaxPayload() at BOTH tier-A-ceiling call sites
//	  -> `go test ./cmd/tether/` stays GREEN, i.e. the "a missing measurement must never widen the
//	     budget" fix can be disconnected from the program without any test noticing.
//
//	delete BOTH `probe.warning()` stderr notes
//	  -> `go test ./cmd/tether/` stays GREEN, i.e. "proceeding on a guess must be announced" is
//	     asserted about the METHOD and never about the program.
//
// A behavioural pin (drive runPush against a stub broker and assert the *ExitError class) would be
// strictly better; this AST pin is the cheap structural stopgap that at least cannot be silently
// disconnected. Precedent for source-scanning tests in this repo: test/determinism/lint_skeleton_test.go.

func g67ParseTransferGo(t *testing.T) (*token.FileSet, *ast.File) {
	t.Helper()
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "transfer.go", nil, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse transfer.go: %v", err)
	}
	return fset, f
}

// TestG67TLEveryTransferRefusalGoesThroughTransferRefusalErr: a refusal formatted with the literal
// `code=%s` token is a broker-code refusal and MUST carry an exit class. Building one with a bare
// fmt.Errorf silently returns it to the unclassified exit 70, which is what G67 step 6 fixed.
func TestG67EveryTransferRefusalGoesThroughTransferRefusalErr(t *testing.T) {
	fset, f := g67ParseTransferGo(t)
	var bad []string
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || len(call.Args) == 0 {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != "Errorf" {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "fmt" {
			return true
		}
		lit, ok := call.Args[0].(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}
		if strings.Contains(lit.Value, "code=%s") {
			bad = append(bad, fset.Position(call.Pos()).String()+": "+lit.Value)
		}
		return true
	})
	if len(bad) > 0 {
		t.Fatalf("plan G67 §2 IN item 5 says SIX transfer refusal points get an exit class; five were "+
			"wired. These are built with a bare fmt.Errorf, so they carry NO exit class and land on the "+
			"unclassified exit 70. Route them through transferRefusalErr, which keeps the text "+
			"byte-identical (drills/61 greps the literal `code=<X>` token):\n  %s\n\n"+
			"Severity note for the reviewer: the site below is currently INERT — the only publisher of "+
			"ev.transfer.<id>.failed is the agent (internal/agent/transfer.go:468) and none of the codes "+
			"it emits (io_error / sha_mismatch / object_get_failed / the path_* family) is in "+
			"brokerCodeExitClasses, so 70 is what they would get anyway. It matters because it is the ONE "+
			"place a transfer's terminal outcome is reported and it is now the only one that silently "+
			"ignores the map: the day any agent-side code is classified, this path keeps exiting 70.",
			strings.Join(bad, "\n  "))
	}
}

// TestG67TLTierACeilingCallSitesUseTheConnectionMeasurement: nc.MaxPayload() is the ground truth for
// what THIS client may publish and is populated from server INFO on any connected conn. It is the
// ONLY measurement available when the caps probe fails, which is the #67 face-B case; passing a
// literal instead re-opens the silent widening of the tier-A/B boundary that the fix removed.
func TestG67TierACeilingCallSitesUseTheConnectionMeasurement(t *testing.T) {
	fset, f := g67ParseTransferGo(t)
	seen := map[string]int{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		// Only the COMMAND entry points. chooseTier's own body legitimately forwards its parameter.
		if !ok || (fn.Name.Name != "runPush" && fn.Name.Name != "runPull") {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			id, ok := call.Fun.(*ast.Ident)
			if !ok || (id.Name != "chooseTier" && id.Name != "tierAInlineCeiling") {
				return true
			}
			hasConnMeasurement := false
			for _, a := range call.Args {
				inner, ok := a.(*ast.CallExpr)
				if !ok {
					continue
				}
				if s, ok := inner.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "MaxPayload" {
					hasConnMeasurement = true
				}
			}
			if !hasConnMeasurement {
				t.Errorf("%s: %s in %s is called without nc.MaxPayload(); a missing measurement must never "+
					"WIDEN the inline ceiling, and the connection's own max_payload is the one measurement "+
					"that is always available", fset.Position(call.Pos()), id.Name, fn.Name.Name)
			}
			seen[id.Name]++
			return true
		})
	}
	// Anti-vacuity: this test must fail if the call sites are renamed or removed rather than passing
	// because it found nothing to check. push calls chooseTier, pull calls tierAInlineCeiling.
	if seen["chooseTier"] < 1 || seen["tierAInlineCeiling"] < 1 {
		t.Fatalf("expected at least one chooseTier and one tierAInlineCeiling CALL SITE in transfer.go, "+
			"found %v — the pin has nothing to hold down", seen)
	}
}

// TestG67TLProbeWarningIsAnnouncedAtBothCallSites: proceeding on a guess (an undetermined or refused
// caps probe) is only acceptable because the operator is TOLD. capsProbe.warning() returning the
// right string is asserted; that anyone prints it is not.
func TestG67ProbeWarningIsAnnouncedAtBothCallSites(t *testing.T) {
	_, f := g67ParseTransferGo(t)
	uses := 0
	ast.Inspect(f, func(n ast.Node) bool {
		sel, ok := n.(*ast.SelectorExpr)
		if ok && sel.Sel.Name == "warning" {
			uses++
		}
		return true
	})
	// one per verb (push, pull); the method declaration itself is not a SelectorExpr.
	if uses < 2 {
		t.Fatalf("capsProbe.warning() is consulted %d time(s) in transfer.go, want >= 2 (push and pull). "+
			"chooseTier now proceeds OPTIMISTICALLY to tier B whenever the probe produced no authoritative "+
			"answer; that is only honest while the guess is announced on stderr", uses)
	}
}
