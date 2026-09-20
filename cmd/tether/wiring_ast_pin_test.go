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

// TestTierBEntryPointsDeriveTheirPhaseTimeoutFromTheSize pins the CALL SITES of phaseTimeoutFor, the
// same shape of hole the G67 pins above close for transferRefusalErr: TestPhaseTimeoutForDerivesFromSize
// exercises the function, and mutation PT-3 (internal review round 1 R4-F6) showed that passing the raw
// `timeout` flag to finishPullTierB instead of phaseTimeoutFor(cmd, pr.Size, timeout) leaves every test
// green — the 37-minute flat budget drill 67 measured would be back on the pull side with nothing
// noticing. The pin: the two tier-B entry points are each called exactly once, and the LAST argument of
// each call is phaseTimeoutFor(cmd, <the size that call itself carries>, timeout) — for push the size is
// the argument at the size position of the same pushTierB call, for pull it is `<prepare-reply>.Size`
// where the prepare reply is the argument passed at its own position of the finishPullTierB call.
// origin: simcluster-speed internal review round 1 R4-F6
func TestTierBEntryPointsDeriveTheirPhaseTimeoutFromTheSize(t *testing.T) {
	fset, f := g67ParseTransferGo(t)
	// The positional contract, read off the two signatures:
	//   pushTierB(cmd, nc, actor, sid, spec, transferID, localAbs, size, force, timeout)
	//   finishPullTierB(cmd, nc, actor, sid, spec, transferID, localAbs, startedAt, pr, force, timeout)
	want := map[string]struct {
		nargs   int
		sizeArg func(call *ast.CallExpr) string // the expression phaseTimeoutFor's size must equal
	}{
		"pushTierB": {nargs: 10, sizeArg: func(c *ast.CallExpr) string { return exprString(fset, c.Args[7]) }},
		"finishPullTierB": {nargs: 11, sizeArg: func(c *ast.CallExpr) string {
			return exprString(fset, c.Args[8]) + ".Size"
		}},
	}
	seen := map[string]int{}
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		spec, ok := want[id.Name]
		if !ok {
			return true
		}
		seen[id.Name]++
		at := fset.Position(call.Pos()).String()
		if len(call.Args) != spec.nargs {
			t.Fatalf("%s: %s takes %d arguments, this pin was written for %d — re-derive the positions", at, id.Name, len(call.Args), spec.nargs)
		}
		last, ok := call.Args[len(call.Args)-1].(*ast.CallExpr)
		if !ok {
			t.Fatalf("%s: %s's timeout argument is %q, not a phaseTimeoutFor(...) call — the tier-B phase budget "+
				"is the flat flag again (37 min for every size)", at, id.Name, exprString(fset, call.Args[len(call.Args)-1]))
		}
		if fn, ok := last.Fun.(*ast.Ident); !ok || fn.Name != "phaseTimeoutFor" || len(last.Args) != 3 {
			t.Fatalf("%s: %s's timeout argument is %q, want phaseTimeoutFor(cmd, <size>, timeout)", at, id.Name, exprString(fset, last))
		}
		if got, wantSize := exprString(fset, last.Args[1]), spec.sizeArg(call); got != wantSize {
			t.Fatalf("%s: %s derives its phase timeout from %q but transfers %q — the budget must be sized for THIS transfer", at, id.Name, got, wantSize)
		}
		if got := exprString(fset, last.Args[2]); got != "timeout" {
			t.Fatalf("%s: phaseTimeoutFor's flag argument is %q, want the --timeout flag value `timeout` (an explicit flag must still win)", at, got)
		}
		return true
	})
	// Anti-vacuity: exactly one call site per verb — a second one would be a second budget to pin.
	if seen["pushTierB"] != 1 || seen["finishPullTierB"] != 1 {
		t.Fatalf("expected exactly one pushTierB and one finishPullTierB call site in transfer.go, found %v", seen)
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

// TestPushTierBWiresTheWatchdogAndTheRetryLadder pins the #84 ladder at its ONLY call site (round-2
// review R4-2-F2, the same hole class as round-1 R4-F6): inside pushTierB there is exactly one
// putWithJSRetry call, its arguments are the production classifier and the production constants,
// and exactly one putWithJSWatchdog call whose clocks are the production constants and whose probe
// closure calls probeJetStreamForBucket. Handing the ladder a never-transient classifier, an
// unreachable strike count, or a probe that is not the ACL-bounded STREAM.INFO one left every
// hermetic test green — only drill 67 (deploy tier, on demand) would have noticed.
// origin: simcluster-speed review round 2 R4-2-F2
func TestPushTierBWiresTheWatchdogAndTheRetryLadder(t *testing.T) {
	fset, f := g67ParseTransferGo(t)
	var push *ast.FuncDecl
	for _, d := range f.Decls {
		if fd, ok := d.(*ast.FuncDecl); ok && fd.Name.Name == "pushTierB" {
			push = fd
		}
	}
	if push == nil {
		t.Fatal("pushTierB not found in transfer.go")
	}
	retries, watchdogs := 0, 0
	ast.Inspect(push.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		id, ok := call.Fun.(*ast.Ident)
		if !ok {
			return true
		}
		at := fset.Position(call.Pos()).String()
		switch id.Name {
		case "putWithJSRetry":
			retries++
			want := []string{"putCtx", "attemptPut", "jetStreamUnavailableFace", "jsPutRetryAttempts", "jsPutRetryBackoff", "sleepCtx"}
			if len(call.Args) != len(want) {
				t.Fatalf("%s: putWithJSRetry takes %d arguments, this pin was written for %d", at, len(call.Args), len(want))
			}
			for i, w := range want {
				if got := exprString(fset, call.Args[i]); got != w {
					t.Fatalf("%s: putWithJSRetry argument %d is %q, want %q — the retry ladder is no longer driven by the production classifier/constants", at, i, got, w)
				}
			}
		case "putWithJSWatchdog":
			watchdogs++
			if len(call.Args) != 6 {
				t.Fatalf("%s: putWithJSWatchdog takes %d arguments, this pin was written for 6", at, len(call.Args))
			}
			for i, w := range map[int]string{3: "jsPutProbeInterval", 4: "jsPutProbeTimeout", 5: "jsPutProbeStrikes"} {
				if got := exprString(fset, call.Args[i]); got != w {
					t.Fatalf("%s: putWithJSWatchdog clock argument %d is %q, want %q", at, i, got, w)
				}
			}
			probes := 0
			ast.Inspect(call.Args[2], func(m ast.Node) bool {
				if c, ok := m.(*ast.CallExpr); ok {
					if fn, ok := c.Fun.(*ast.Ident); ok && fn.Name == "probeJetStreamForBucket" {
						probes++
					}
				}
				return true
			})
			if probes != 1 {
				t.Fatalf("%s: putWithJSWatchdog's probe closure calls probeJetStreamForBucket %d times, want exactly 1 (the ACL-bounded STREAM.INFO probe is the only permitted one)", at, probes)
			}
		}
		return true
	})
	if retries != 1 || watchdogs != 1 {
		t.Fatalf("pushTierB must call putWithJSRetry once and putWithJSWatchdog once; found %d and %d", retries, watchdogs)
	}
}
