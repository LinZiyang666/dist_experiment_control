// Adopted from the Stage-C internal review (CLAUDE.md §3 step 5): the reviewer authored these pins,
// the main process reviewed, renamed and now owns them.
package main

import (
	"go/ast"
	"go/parser"
	"go/printer"
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
//
// S7: the cap0 tier boundary moved with the one-shot legacy owner into internal/cli. Its push tier
// choice and its pull MaxInline are each a min() of the protocol ceiling and the CONTROL connection's
// MaxPayload(); a capable transfer takes the boundary from the authenticated agent probe instead.
// origin: hot-update S7 plan §9 / P31 (re-pointed from runPush/runPull)
func TestG67TierACeilingCallSitesUseTheConnectionMeasurement(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "../../internal/cli/transfer_legacy.go", nil, 0)
	if err != nil {
		t.Fatalf("parse transfer_legacy.go: %v", err)
	}
	seen := map[string]int{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || (fn.Name.Name != "legacyOnce" && fn.Name.Name != "legacyPush") {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "min" || !strings.Contains(exprString(fset, call), "XferTierAMaxBytes") {
				return true
			}
			measured := false
			ast.Inspect(call, func(m ast.Node) bool {
				if inner, ok := m.(*ast.CallExpr); ok {
					if s, ok := inner.Fun.(*ast.SelectorExpr); ok && s.Sel.Name == "MaxPayload" && strings.HasSuffix(exprString(fset, s.X), "control") {
						measured = true
					}
				}
				return true
			})
			if !measured {
				t.Errorf("%s: the tier-A ceiling in %s is %q, not clamped by the control connection's "+
					"MaxPayload(); a missing measurement must never WIDEN the inline ceiling",
					fset.Position(call.Pos()), fn.Name.Name, exprString(fset, call))
			}
			seen[fn.Name.Name]++
			return true
		})
	}
	// Anti-vacuity: push chooses its tier, pull bounds MaxInline — one clamp each.
	if seen["legacyPush"] < 1 || seen["legacyOnce"] < 1 {
		t.Fatalf("expected a tier-A ceiling clamp in both legacyPush and legacyOnce, found %v — the pin has nothing to hold down", seen)
	}
}

// TestTierBEntryPointsDeriveTheirPhaseTimeoutFromTheSize pins the CALL SITE of the cap0 push phase
// bound: TestPhaseTimeoutForDerivesFromSize exercises the function, and mutation PT-3 (internal review
// round 1 R4-F6) showed that dropping the call leaves every test green — the 37-minute flat budget drill
// 67 measured would be back. S7: the only cap0 push entry is RunTransferCommand's legacy branch; it must
// call LegacyPhaseTimeout exactly once with the size of THIS transfer and the --timeout flag value.
// origin: simcluster-speed internal review round 1 R4-F6; hot-update S7 plan §9 / P31 (re-pointed)
func TestTierBEntryPointsDeriveTheirPhaseTimeoutFromTheSize(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "../../internal/cli/transfer_command.go", nil, 0)
	if err != nil {
		t.Fatalf("parse transfer_command.go: %v", err)
	}
	calls := 0
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "RunTransferCommand" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := call.Fun.(*ast.Ident); !ok || id.Name != "LegacyPhaseTimeout" {
				return true
			}
			calls++
			at := fset.Position(call.Pos()).String()
			if len(call.Args) != 3 || exprString(fset, call.Args[1]) != "cfg.Intent.Size" || exprString(fset, call.Args[2]) != "timeout" {
				t.Fatalf("%s: the cap0 push bound is %q, want LegacyPhaseTimeout(<explicit>, cfg.Intent.Size, timeout) — "+
					"the budget must be sized for THIS transfer and an explicit flag must still win", at, exprString(fset, call))
			}
			return true
		})
	}
	if calls != 1 {
		t.Fatalf("expected exactly one LegacyPhaseTimeout call in RunTransferCommand, found %d", calls)
	}
}

// exprString renders an expression as source text for comparison in error messages and equality checks.
func exprString(fset *token.FileSet, e ast.Expr) string {
	var sb strings.Builder
	if err := printer.Fprint(&sb, fset, e); err != nil {
		return "<unprintable>"
	}
	return sb.String()
}
