package architecture

import (
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// tls_verify_pairing_test.go — InsecureSkipVerify:true must be paired with a peer-verification callback.
//
// origin: line-2 §12, the gosec G402 review. S3 declined to enable gosec wholesale (111 reports, almost
// all against conventions this repo had already reasoned through) but asked for a one-time human review
// of its G402/G202/G404 findings. That review found one G402: internal/cluster/transport.go sets
//
//	InsecureSkipVerify:    true,
//	VerifyPeerCertificate: verifyChainToCA,
//
// which is the CORRECT idiom for "verify the chain, skip the hostname" — the raft peers are IP:port and
// their leaves are CN-only, so hostname verification cannot succeed, while verifyChainToCA still calls
// leaf.Verify against the cluster CA and the server side still uses RequireAndVerifyClientCert.
//
// WHY A GATE AND NOT JUST THE REVIEW
// ----------------------------------
// A one-time review is true on the day it is written. What makes this pairing dangerous is that HALF of
// it is inert-looking: deleting the VerifyPeerCertificate line leaves code that compiles, connects, and
// passes every functional test, while accepting any certificate from anyone. The remaining
// InsecureSkipVerify:true then reads as if someone had thought about it, because the comment next to it
// says they did.
//
// So the review's conclusion is recorded as an assertion instead of a sentence: the two fields live and
// die together. This is the whole thesis of line 2 applied to its own finding — a human judgement that
// is not mechanised is a judgement that has to be made again, by someone who may not know it was ever
// made.

// unverifiedTLSFallbacks are the sites that skip hostname verification with NO peer check at all.
// Each is a documented protocol-level decision, not an oversight, and each has to argue for itself
// here — the map is a ledger, so a new entry is a visible edit in review rather than a line of code
// that quietly stops verifying.
//
// KEYED BY file:ENCLOSING FUNCTION, not file:line.
//
// origin: line-2 external review M15. The first version keyed by file:line and its doc comment argued
// that the drift was a feature — "a moved entry fails the reverse assertion and forces someone to
// re-read the site". Measured against what the failure actually SAYS, that argument does not survive:
// adding one comment line above the site produces a security-level failure reading "no longer name an
// InsecureSkipVerify:true site", which is false. The site is right there. Nothing about the TLS
// decision changed. A gate that cries wolf on a comment edit is a gate whose next real report gets
// waved through, and this is the repo's only TLS gate.
//
// A function name is stable across edits above it and still names ONE site. The hole it opens — a
// second InsecureSkipVerify literal added to an already-ledgered function inheriting the exemption —
// is closed by the exact-count assertion at the end of the test, which reddens on a new site anywhere,
// paired or not.
var unverifiedTLSFallbacks = map[string]string{
	"internal/tunnel/tls.go:clientTLSConfig": "architecture F.5 / §16.7: the N=1 fallback. With no " +
		"cluster CA and no cert pins configured there is nothing to verify AGAINST — the broker's cert " +
		"is self-signed and the operator has not pinned it. Skipping verification is the documented, " +
		"opt-out-able v1 behaviour; the pinned path (clientTLSConfigPinned) verifies via VerifyConnection.",
}

// valueMaySkipHostname reports whether an expression assigned to InsecureSkipVerify might be true.
//
// FAILS TOWARD REPORTING. origin: line-2 closure verification M1. Both scanners used to require the literal
// identifier `true`, so every other spelling escaped the repo's only TLS gate:
//
//	cfg.InsecureSkipVerify = skipVerify        // a variable
//	InsecureSkipVerify: cfg.Insecure,          // a field
//	InsecureSkipVerify: os.Getenv("X") != "",  // an expression
//
// Each of those is ordinary Go and each one turns hostname verification off on some path. Deciding
// statically whether an arbitrary expression can be true is not something a gate should attempt, so the
// rule is inverted: only the literal `false` is treated as definitely-not-skipping. Everything else must be
// paired with a verification callback or ledgered.
//
// The cost is a false positive on `InsecureSkipVerify: someDefinitelyFalseConst`, which is loud, one line
// to resolve, and vastly preferable to the silent alternative — a config that stops verifying peers because
// the value moved into a variable.
func valueMaySkipHostname(v ast.Expr) bool {
	if id, ok := v.(*ast.Ident); ok && id.Name == "false" {
		return false
	}
	return true
}

// enclosingFuncKey returns "<rel>:<funcName>" for the function containing pos, or "<rel>:<file-level>"
// for a package-level declaration. Methods are spelled "Recv.Method" so two same-named methods on
// different types do not collide.
func enclosingFuncKey(f *ast.File, fset *token.FileSet, rel string, pos token.Pos) string {
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Pos() > pos || fn.End() < pos {
			continue
		}
		name := fn.Name.Name
		if fn.Recv != nil && len(fn.Recv.List) > 0 {
			name = receiverName(fn.Recv.List[0].Type) + "." + name
		}
		return rel + ":" + name
	}
	_ = fset
	return rel + ":<file-level>"
}

// receiverName unwraps *T / T to T.
func receiverName(e ast.Expr) string {
	if star, ok := e.(*ast.StarExpr); ok {
		e = star.X
	}
	if id, ok := e.(*ast.Ident); ok {
		return id.Name
	}
	return "?"
}

// TestInsecureSkipVerifyIsAlwaysPairedWithChainVerification asserts the pairing described in this
// file's header: any tls.Config that turns hostname verification off must verify the peer some other
// way, via VerifyPeerCertificate (chain to a CA) or VerifyConnection (pins), or be a named entry in
// unverifiedTLSFallbacks with a written reason.
func TestInsecureSkipVerifyIsAlwaysPairedWithChainVerification(t *testing.T) {
	root := repoRoot(t)

	var offenders []string
	found := 0
	seen := map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata", "bin":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		if !strings.Contains(string(src), "InsecureSkipVerify") {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)

		fset := token.NewFileSet()
		f, perr := parser.ParseFile(fset, p, src, 0)
		if perr != nil {
			t.Fatalf("parse %s: %v", rel, perr)
		}
		// ASSIGNMENT form, checked first because it is the one that used to escape entirely.
		// `cfg.InsecureSkipVerify = true` is ordinary Go — a config built in steps rather than in one
		// literal — and the first version of this gate only walked *ast.CompositeLit, so that spelling
		// had NO coverage from anything: not from here, and not from gosec, which this repo deliberately
		// does not enable (plan §11 X7). One line of normal style disabled the repo's only TLS gate.
		//
		// There is no literal to inspect for a paired callback here, so the rule is stricter: an
		// assignment that turns hostname verification off must be accompanied, somewhere in the SAME
		// function, by an assignment to one of the two verification callbacks. Same-function rather than
		// same-statement because the whole reason to build a config in steps is that the steps are
		// separate.
		ast.Inspect(f, func(n ast.Node) bool {
			fn, ok := n.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				return true
			}
			// PAIRED PER CONFIG OBJECT, NOT PER FUNCTION. origin: line-2 INDEPENDENT EXTERNAL REVIEW M2
			// (MAJOR / SECURITY).
			//
			// The first version kept ONE skipPos and ONE function-level `verifiesInFunc` bool. So any
			// verification callback anywhere in the same function laundered an unsafe config: the reviewer
			// set `client.InsecureSkipVerify = true`, removed client's callback, assigned
			// `unrelated.VerifyPeerCertificate`, and the gate PASSED while that client accepted
			// unverified certificates. It also counted at most one skip site per function, so a second
			// unsafe assignment in the same function was invisible to the exact-count assertion too.
			//
			// Both halves come from the same mistake — treating the function as the unit when the unit is
			// the tls.Config. Keying on the selector BASE (`client` in `client.InsecureSkipVerify`) pairs
			// each skip with the callbacks assigned to that same object.
			skipsByBase := map[string]token.Pos{}
			verifiesByBase := map[string]bool{}
			ast.Inspect(fn.Body, func(inner ast.Node) bool {
				as, ok := inner.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for i, lhs := range as.Lhs {
					sel, ok := lhs.(*ast.SelectorExpr)
					if !ok {
						continue
					}
					base := selectorBaseName(sel.X)
					switch sel.Sel.Name {
					case "InsecureSkipVerify":
						if i < len(as.Rhs) && valueMaySkipHostname(as.Rhs[i]) {
							if _, dup := skipsByBase[base]; !dup {
								skipsByBase[base] = as.Pos()
							}
						}
					case "VerifyPeerCertificate", "VerifyConnection":
						if rhs, ok := assignedRHS(as, i); ok && callbackMayVerify(rhs) {
							verifiesByBase[base] = true
						}
					}
				}
				return true
			})
			var bases []string
			for base := range skipsByBase {
				bases = append(bases, base)
			}
			sort.Strings(bases)
			for _, base := range bases {
				skipPos := skipsByBase[base]
				found++
				key := enclosingFuncKey(f, fset, rel, skipPos)
				seen[key] = true
				if _, excused := unverifiedTLSFallbacks[key]; !verifiesByBase[base] && !excused {
					offenders = append(offenders, key+" (assignment form on `"+base+"`, line "+
						strconv.Itoa(fset.Position(skipPos).Line)+")")
				}
			}
			return true
		})

		ast.Inspect(f, func(n ast.Node) bool {
			lit, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			var skipsHostname, verifiesChain bool
			for _, elt := range lit.Elts {
				kv, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := kv.Key.(*ast.Ident)
				if !ok {
					continue
				}
				switch key.Name {
				case "InsecureSkipVerify":
					if valueMaySkipHostname(kv.Value) {
						skipsHostname = true
					}
				// EITHER callback counts. Go has two, they verify at different points, and both are
				// legitimate ways to say "I am checking the peer myself": VerifyPeerCertificate gets the
				// raw chain (internal/cluster/transport.go chains it to the cluster CA),
				// VerifyConnection gets the completed ConnectionState (internal/tunnel/tls.go checks a
				// self-signed home cert against its pins there, because a pinned self-signed cert has
				// no chain to verify). An earlier draft of this gate only knew the first one and
				// reported the tunnel's pin check as unverified — a false positive that would have
				// pushed someone to "fix" working pin verification.
				case "VerifyPeerCertificate", "VerifyConnection":
					if callbackMayVerify(kv.Value) {
						verifiesChain = true
					}
				}
			}
			if skipsHostname {
				found++
				key := enclosingFuncKey(f, fset, rel, lit.Pos())
				seen[key] = true
				if _, excused := unverifiedTLSFallbacks[key]; !verifiesChain && !excused {
					offenders = append(offenders, key+" (line "+
						strconv.Itoa(fset.Position(lit.Pos()).Line)+")")
				}
			}
			return true
		})
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d tls.Config literal(s) set InsecureSkipVerify:true with NEITHER "+
			"VerifyPeerCertificate NOR VerifyConnection:\n  %s\n\n"+
			"That combination accepts a certificate from anybody — including one signed by a CA this\n"+
			"cluster has never heard of — while looking deliberate to the next reader. If hostname\n"+
			"verification genuinely cannot be used, supply one of the two callbacks: chain the peer leaf\n"+
			"to the cluster CA (internal/cluster/transport.go) or check it against pins\n"+
			"(internal/tunnel/tls.go). If there is genuinely nothing to verify against, the site belongs\n"+
			"in unverifiedTLSFallbacks WITH a written reason.",
			len(offenders), strings.Join(offenders, "\n  "))
	}

	// Reverse assertion: the ledger drains. An entry whose site has moved or gained a callback is no
	// longer excusing anything, and leaving it means the next InsecureSkipVerify that lands on that
	// exact line inherits an exemption written for different code.
	var stale []string
	for pos := range unverifiedTLSFallbacks {
		if !seen[pos] {
			stale = append(stale, pos)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d unverifiedTLSFallbacks entr(y/ies) name a function that no longer contains an "+
			"InsecureSkipVerify:true site:\n  %s\n\n"+
			"The function was renamed, or the site was fixed or removed. Re-read it, then update or delete "+
			"the entry. (The key is file:FUNCTION, so this cannot be triggered by editing lines above the "+
			"site — if it fired, something real changed.)",
			len(stale), strings.Join(stale, "\n  "))
	}
	for pos, reason := range unverifiedTLSFallbacks {
		if strings.TrimSpace(reason) == "" {
			t.Errorf("unverifiedTLSFallbacks[%q] has an empty reason — an exemption that does not argue "+
				"for itself is just a slower way of having no gate", pos)
		}
	}

	// Non-vacuity, as an EXACT count in both directions.
	//
	// origin: line-2 external review IDG-9. This was `if found == 0`, which is the weakest lower bound
	// available: a scanner that had rotted down to matching one of four sites would sail through it, and
	// the gate's success state ("no offenders") is exactly what a broken scanner reports. `found == 0` only
	// catches TOTAL failure.
	//
	// The five sites, all deliberate and all read by hand — the first four during the gosec G402 review,
	// the fifth during the 2026-09-02 prerelease audit:
	//
	//	internal/tunnel/tls.go        the N=1 unpinned fallback (ledgered) and the pinned path
	//	internal/cluster/transport.go the raft transport's CN-only leaf, paired with verifyChainToCA
	//	internal/tunnel/register_and_fence_test.go  a test dialling the harness's self-signed listener
	//	internal/tunnel/register_admission_test.go  admissionTLS, the dial config for the unauthenticated-
	//	                             admission harness. PAIRED with a VerifyConnection pinning the leaf
	//	                             fingerprint the harness itself minted, so it took no exemption. Both
	//	                             call sites share this one constructor deliberately: a per-call literal
	//	                             would have added two countable sites and two places to forget the pin.
	//
	// Growth is the interesting direction and it FAILS here rather than being absorbed: a sixth site must
	// be read by a person, and this line is where they are made to do it. Shrinkage fails too, because a
	// count that is allowed to drop silently is a count that stops meaning anything the moment the
	// scanner breaks.
	// PRODUCTION sites named individually, not just counted. origin: line-2 closure verification §6 B8.
	// The exact count alone can be satisfied by the wrong four: one of the four is in
	// internal/tunnel/register_and_fence_test.go, so a scanner that had stopped seeing PRODUCTION literals
	// entirely could still be within one of the target and pass on test files. Naming the production sites
	// makes "the scanner still sees the code that matters" a separate, non-substitutable assertion.
	for _, mustSee := range []string{
		"internal/tunnel/tls.go:clientTLSConfig",
		"internal/tunnel/tls.go:clientTLSConfigPinned",
		"internal/cluster/transport.go:clusterTLSConfigs",
	} {
		if !seen[mustSee] {
			t.Errorf("the scan did not find the known InsecureSkipVerify site %s.\n\n"+
				"That is a PRODUCTION site this gate exists for. Either it was removed (delete it from this "+
				"list and lower expectedSkipVerifySites in the same commit) or the scanner stopped "+
				"recognising its shape — in which case every 'no offenders' verdict above is meaningless "+
				"even if the total still happens to add up on test files.", mustSee)
		}
	}

	const expectedSkipVerifySites = 5
	if found != expectedSkipVerifySites {
		t.Errorf("the scan found %d InsecureSkipVerify:true site(s), expected exactly %d.\n\n"+
			"MORE: a new site appeared. Read it, then either pair it with VerifyPeerCertificate/"+
			"VerifyConnection or add it to unverifiedTLSFallbacks with an argument, and bump this constant "+
			"in the same commit.\n"+
			"FEWER: either a site was correctly removed (bump the constant down and say so) or the scanner "+
			"stopped seeing a shape it used to see — which would make every 'no offenders' verdict above "+
			"meaningless.", found, expectedSkipVerifySites)
	}
}

// selectorBaseName renders the object a field assignment targets: `client` for `client.InsecureSkipVerify`,
// `s.cfg` for `s.cfg.InsecureSkipVerify`. Anything it cannot render becomes "?", which groups conservatively
// (several unknowns share one bucket, so a callback on one cannot excuse a skip on another unless both are
// unrenderable) rather than optimistically.
//
// origin: line-2 INDEPENDENT EXTERNAL REVIEW M2. The pairing has to be per config object; see the call site.
func selectorBaseName(e ast.Expr) string {
	var b strings.Builder
	if err := printer.Fprint(&b, token.NewFileSet(), e); err == nil && b.Len() > 0 {
		return b.String()
	}
	// Never merge expressions the printer cannot render. A shared "?" bucket
	// lets a callback on one unknown expression launder a skip on another.
	return "?@" + strconv.Itoa(int(e.Pos()))
}

// assignedRHS returns the expression assigned to lhs[i] when the mapping is
// syntactically one-to-one. A multi-result call cannot be resolved without
// type information, so it is not accepted as proof of a verification callback.
func assignedRHS(as *ast.AssignStmt, i int) (ast.Expr, bool) {
	if len(as.Lhs) != len(as.Rhs) || i < 0 || i >= len(as.Rhs) {
		return nil, false
	}
	return as.Rhs[i], true
}

// callbackMayVerify rejects the statically-certain no-op spelling. Variables
// and calls can still be nil at runtime, but a literal nil is conclusive proof
// that no callback runs and must never satisfy this security gate.
func callbackMayVerify(e ast.Expr) bool {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			break
		}
		e = p.X
	}
	id, ok := e.(*ast.Ident)
	return !ok || id.Name != "nil"
}

func TestTLSCallbackMayVerifyRejectsLiteralNil(t *testing.T) {
	for _, tc := range []struct {
		expr string
		want bool
	}{
		{"nil", false},
		{"(nil)", false},
		{"verifyPeer", true},
		{"func() {}", true},
	} {
		e, err := parser.ParseExpr(tc.expr)
		if err != nil {
			t.Fatalf("parse %q: %v", tc.expr, err)
		}
		if got := callbackMayVerify(e); got != tc.want {
			t.Errorf("callbackMayVerify(%s) = %v, want %v", tc.expr, got, tc.want)
		}
	}
}

// TestTLSPairingRejectsLaunderedConfig is external review M2's counter-example, frozen as a self-check.
//
// The reviewer's mutation was: set `client.InsecureSkipVerify = true`, remove client's callback, and assign
// `unrelated.VerifyPeerCertificate` in the same function. The function-scoped pairing passed it, and that
// client accepted unverified certificates. Keeping the reviewer's exact shape as a synthetic source sample
// means the fix cannot be undone by a refactor that "simplifies" the pairing back to a per-function bool.
//
// Synthetic source rather than a real file: this asserts a property of the SCANNER, and putting the sample
// in the tree would either be a real unsafe config or a fake one someone would eventually delete.
func TestTLSPairingRejectsLaunderedConfig(t *testing.T) {
	const src = `package probe

import "crypto/tls"

func launderedByAnotherConfig() (*tls.Config, *tls.Config) {
	client := &tls.Config{}
	client.InsecureSkipVerify = true
	unrelated := &tls.Config{}
	unrelated.VerifyPeerCertificate = verifyPeer
	return client, unrelated
}

func properlyPaired() *tls.Config {
	c := &tls.Config{}
	c.InsecureSkipVerify = true
	c.VerifyPeerCertificate = verifyPeer
	return c
}

func twoUnsafeInOneFunc() (*tls.Config, *tls.Config) {
	a := &tls.Config{}
	a.InsecureSkipVerify = true
	b := &tls.Config{}
	b.InsecureSkipVerify = true
	a.VerifyConnection = verifyConnection
	return a, b
}

func nilCallbackIsNotVerification() *tls.Config {
	c := &tls.Config{}
	c.InsecureSkipVerify = true
	c.VerifyPeerCertificate = nil
	return c
}

func indexedConfigsDoNotLaunder(configs []*tls.Config) {
	configs[0].InsecureSkipVerify = true
	configs[1].VerifyPeerCertificate = verifyPeer
}

func callArgumentsDoNotLaunder() {
	getConfig(0).InsecureSkipVerify = true
	getConfig(1).VerifyConnection = verifyConnection
}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "probe.go", src, 0)
	if err != nil {
		t.Fatalf("parse sample: %v", err)
	}

	// Re-run the same per-base pairing the gate uses, over the sample.
	type verdict struct{ skips, unpaired []string }
	got := map[string]*verdict{}
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil {
			continue
		}
		skips := map[string]bool{}
		verifies := map[string]bool{}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			as, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, lhs := range as.Lhs {
				sel, ok := lhs.(*ast.SelectorExpr)
				if !ok {
					continue
				}
				base := selectorBaseName(sel.X)
				switch sel.Sel.Name {
				case "InsecureSkipVerify":
					if i < len(as.Rhs) && valueMaySkipHostname(as.Rhs[i]) {
						skips[base] = true
					}
				case "VerifyPeerCertificate", "VerifyConnection":
					if rhs, ok := assignedRHS(as, i); ok && callbackMayVerify(rhs) {
						verifies[base] = true
					}
				}
			}
			return true
		})
		v := &verdict{}
		for base := range skips {
			v.skips = append(v.skips, base)
			if !verifies[base] {
				v.unpaired = append(v.unpaired, base)
			}
		}
		sort.Strings(v.skips)
		sort.Strings(v.unpaired)
		got[fn.Name.Name] = v
	}

	for _, tc := range []struct {
		fn              string
		skips, unpaired []string
		why             string
	}{
		{"launderedByAnotherConfig", []string{"client"}, []string{"client"},
			"M2's counter-example: a callback on a DIFFERENT config must not excuse this one"},
		{"properlyPaired", []string{"c"}, nil,
			"the callback is on the same object, so this is the correct idiom and must not be reported"},
		{"twoUnsafeInOneFunc", []string{"a", "b"}, []string{"b"},
			"two skips in one function must BOTH be counted (the old code kept a single skipPos), and only " +
				"the one without a callback is an offender"},
		{"nilCallbackIsNotVerification", []string{"c"}, []string{"c"},
			"a literal nil callback is the default no-op and provides no peer verification"},
		{"indexedConfigsDoNotLaunder", []string{"configs[0]"}, []string{"configs[0]"},
			"configs[1]'s callback must not launder configs[0]; the index is part of object identity"},
		{"callArgumentsDoNotLaunder", []string{"getConfig(0)"}, []string{"getConfig(0)"},
			"getConfig(1)'s callback must not launder getConfig(0); call arguments are part of identity"},
	} {
		v := got[tc.fn]
		if v == nil {
			t.Errorf("%s was not analysed at all", tc.fn)
			continue
		}
		if strings.Join(v.skips, ",") != strings.Join(tc.skips, ",") {
			t.Errorf("%s: skip sites %v, want %v — %s", tc.fn, v.skips, tc.skips, tc.why)
		}
		if strings.Join(v.unpaired, ",") != strings.Join(tc.unpaired, ",") {
			t.Errorf("%s: unpaired %v, want %v — %s", tc.fn, v.unpaired, tc.unpaired, tc.why)
		}
	}
}
