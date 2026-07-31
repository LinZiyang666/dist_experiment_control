package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// topo_classification_test.go — the structural gate that keeps "is this broker's NATS topology
// converged?" answered in ONE place.
//
// WHY A GATE AND NOT JUST THE UNIT TESTS
// --------------------------------------
// Before batch C the question had four implementations, and the two that classify a FAILURE did it
// by substring-matching a free-text Reason with DIFFERENT substring sets on the two ends. Nobody
// wrote that divergence on purpose; it appeared because a substring set is a convention maintained
// across a package boundary, and the second end was written by copying the first and then drifting.
// The unit tests pin today's behaviour. Only this gate stops the NEXT copy.
//
// WHAT IT CHECKS
// --------------
// No file outside the classifier may pass a topology-reason marker to strings.Contains/HasPrefix/
// HasSuffix/Index/LastIndex. The check is AST-anchored on the ARGUMENT POSITION, not on a file-level
// co-occurrence of the two identifiers: clusterstatus.go legitimately calls strings.Contains for
// unrelated reasons and still references TopoReconcileReason (it passes it to ClassifyTopo), so a
// co-occurrence predicate would be RED on a correct implementation and get whitelisted into
// uselessness within a release.

// topoReasonMarkers are the substrings that classify a topology reconcile OUTCOME. "render" is in the
// list bare: the pre-batch-C broker matcher used exactly that, which is how a deliberately withheld
// cutover ("cutover rendeRED + validated but WITHHELD") was classified STUCK and routed the operator
// to a command that would apply a clustered conf under a running standalone nats-server.
var topoReasonMarkers = []string{
	"unrecognized directive",
	"nats-server -t",
	"render",
	"rendered",
	"apply: ",
	"cutover rendered + validated but WITHHELD",
}

// topoMatchFuncs are the stdlib substring predicates a drifting copy would reach for.
var topoMatchFuncs = map[string]bool{
	"Contains": true, "ContainsAny": true, "HasPrefix": true, "HasSuffix": true,
	"Index": true, "LastIndex": true, "EqualFold": true,
}

// topoClassifierFile is the ONE place allowed to match on these markers: the shared classifier's
// mixed-version fallback, which exists precisely so both ends stop having their own.
const topoClassifierFile = "internal/natsconf/topostate.go"

func TestTopologyReasonMatchingLivesInOnePlace(t *testing.T) {
	root := repoRoot(t)
	hits := scanTopoReasonMatches(t, root)
	var offenders []string
	for _, h := range hits {
		if h.file == topoClassifierFile {
			continue
		}
		offenders = append(offenders, h.String())
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d site(s) outside %s substring-match a topology reconcile reason:\n  %s\n\n"+
			"Call natsconf.ClassifyTopo instead. Every one of these is a second opinion on the same "+
			"question, and the last time there were two they disagreed: the ctl side was missing the "+
			"render marker, so a render failure rendered as \"still catching up\" while the broker "+
			"correctly ruled the cluster DEGRADED — telling the operator to wait for a self-heal that "+
			"was never coming.",
			len(offenders), topoClassifierFile, strings.Join(offenders, "\n  "))
	}
	// Non-vacuity over the LIVE tree: the classifier itself must still contain the fallback, or this
	// gate is counting an empty set and would stay green if someone deleted the mixed-version path.
	found := false
	for _, h := range hits {
		if h.file == topoClassifierFile {
			found = true
		}
	}
	if !found {
		t.Errorf("SELF-CHECK FAILED: %s no longer matches any topology reason marker. Either the "+
			"mixed-version fallback was deleted (a pre-batch-C broker's report then classifies by "+
			"generation alone, silently losing STUCK/HELD) or the marker list drifted from it — "+
			"either way this gate is now scanning for something that does not exist.", topoClassifierFile)
	}
}

// TestTopologyReasonScannerIsNotVacuous proves the scanner can actually fail, over a synthesized
// sample rather than the live tree.
func TestTopologyReasonScannerIsNotVacuous(t *testing.T) {
	const sample = `package x

import "strings"

func f(reason string) bool {
	if strings.Contains(reason, "unrecognized directive") {
		return true
	}
	return strings.HasPrefix(reason, "apply: ")
}

func g(other string) bool { return strings.Contains(other, "totally unrelated") }
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "sample.go", sample, 0)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	got := topoMatchesInFile(fset, f, "sample.go")
	if len(got) != 2 {
		t.Fatalf("SELF-CHECK FAILED: scanner found %d marker match(es) in a sample that has exactly 2 "+
			"(%v) — its clean result on the real tree is meaningless", len(got), got)
	}
	// And it must NOT flag the unrelated Contains, or the gate would be noise and get whitelisted away.
	for _, h := range got {
		if strings.Contains(h.marker, "unrelated") {
			t.Fatalf("scanner flagged an unrelated strings.Contains (%s)", h)
		}
	}
}

type topoMatchHit struct {
	file   string
	line   int
	fn     string
	marker string
}

func (h topoMatchHit) String() string {
	return h.file + ":" + strconv.Itoa(h.line) + " strings." + h.fn + "(…, \"" + h.marker + "\")"
}

func scanTopoReasonMatches(t *testing.T, root string) []topoMatchHit {
	t.Helper()
	var out []topoMatchHit
	fset := token.NewFileSet()
	for _, dir := range []string{"internal", "cmd"} {
		base := filepath.Join(root, dir)
		err := filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			f, perr := parser.ParseFile(fset, p, nil, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", p, perr)
			}
			rel, rerr := filepath.Rel(root, p)
			if rerr != nil {
				rel = p
			}
			out = append(out, topoMatchesInFile(fset, f, filepath.ToSlash(rel))...)
			return nil
		})
		if err != nil {
			t.Fatalf("walk %s: %v", base, err)
		}
	}
	return out
}

// topoMatchesInFile returns every `strings.<MatchFunc>(…, "<marker>")` call in f whose LITERAL
// argument is (or contains) a topology reason marker. Matching on the argument position is what
// keeps an unrelated strings.Contains in the same file from tripping the gate.
func topoMatchesInFile(fset *token.FileSet, f *ast.File, rel string) []topoMatchHit {
	var out []topoMatchHit
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok || !topoMatchFuncs[sel.Sel.Name] {
			return true
		}
		pkg, ok := sel.X.(*ast.Ident)
		if !ok || pkg.Name != "strings" {
			return true
		}
		for _, arg := range call.Args {
			lit, ok := arg.(*ast.BasicLit)
			if !ok || lit.Kind != token.STRING {
				continue
			}
			val := strings.Trim(lit.Value, "`\"")
			for _, m := range topoReasonMarkers {
				if val == m || strings.Contains(val, m) {
					out = append(out, topoMatchHit{
						file: rel, line: fset.Position(lit.Pos()).Line, fn: sel.Sel.Name, marker: val,
					})
					break
				}
			}
		}
		return true
	})
	return out
}

// ─── batch C / C2: the tier ceilings have ONE source ───────────────────────────
//
// 8 MiB and 2 GiB existed in three hand-kept copies (broker, agent, ctl). They are a wire-adjacent
// contract — the broker REFUSES at them, so an end that offers more produces a user-visible failure —
// and three copies is three chances to drift on one edit. They now live in internal/proto; this gate
// stops a fourth copy from appearing.
//
// It matches on the LITERAL VALUE, so it must not fire on the several unrelated constants that happen
// to share a number: xferEventsHistoryReserve is also 2 GiB and xferBucketCap is 8 GiB, and both are
// JetStream sizing, not transfer tiers. The allowlist is by (file, const-name), not by line, so it
// survives an edit above it.

var xferTierLiterals = map[string]string{
	"8 * 1024 * 1024":        "proto.XferTierAMaxBytes",
	"2 * 1024 * 1024 * 1024": "proto.XferMaxBytes",
}

// xferTierConstAllowlist names constants that legitimately use one of those values for something that
// is NOT a transfer tier ceiling. Keyed by const NAME so it cannot silently widen to a whole file.
var xferTierConstAllowlist = map[string]string{
	"xferEventsHistoryReserve": "JetStream events+history reservation, not a transfer tier",
	"XferMaxBytes":             "the definition itself",
	"XferTierAMaxBytes":        "the definition itself",
}

func TestXferTierCeilingsHaveOneSource(t *testing.T) {
	root := repoRoot(t)
	hits := scanXferTierLiterals(t, root)
	var offenders []string
	for _, h := range hits {
		if _, ok := xferTierConstAllowlist[h.fn]; ok {
			continue
		}
		offenders = append(offenders, h.file+":"+strconv.Itoa(h.line)+" const "+h.fn+" = "+h.marker)
	}
	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d constant(s) re-declare a transfer tier ceiling instead of reading it from "+
			"internal/proto:\n  %s\n\nUse proto.XferTierAMaxBytes / proto.XferMaxBytes. The broker "+
			"REFUSES at these values, so a copy that drifts makes another end offer what will be "+
			"refused — which is what the three former copies were heading for.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
	// Non-vacuity over the LIVE tree: the definitions themselves must still be found, or the scanner
	// is looking for a shape that no longer exists and its clean result means nothing.
	seen := map[string]bool{}
	for _, h := range hits {
		seen[h.fn] = true
	}
	for _, name := range []string{"XferMaxBytes", "XferTierAMaxBytes"} {
		if !seen[name] {
			t.Errorf("SELF-CHECK FAILED: the scanner did not find the definition of %s. Either it moved "+
				"or its literal was rewritten (e.g. to 2147483648), and this gate is now scanning for "+
				"nothing.", name)
		}
	}
}

// scanXferTierLiterals finds `const NAME = <tier literal>` declarations across internal/ and cmd/.
func scanXferTierLiterals(t *testing.T, root string) []topoMatchHit {
	t.Helper()
	var out []topoMatchHit
	fset := token.NewFileSet()
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(p string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") {
				return err
			}
			src, rerr := os.ReadFile(p)
			if rerr != nil {
				t.Fatalf("read %s: %v", p, rerr)
			}
			f, perr := parser.ParseFile(fset, p, src, 0)
			if perr != nil {
				t.Fatalf("parse %s: %v", p, perr)
			}
			rel, _ := filepath.Rel(root, p)
			rel = filepath.ToSlash(rel)
			for _, decl := range f.Decls {
				gd, ok := decl.(*ast.GenDecl)
				if !ok || gd.Tok != token.CONST {
					continue
				}
				for _, s := range gd.Specs {
					vs, ok := s.(*ast.ValueSpec)
					if !ok {
						continue
					}
					for i, id := range vs.Names {
						if i >= len(vs.Values) {
							continue
						}
						start := fset.Position(vs.Values[i].Pos()).Offset
						end := fset.Position(vs.Values[i].End()).Offset
						if start < 0 || end > len(src) || start >= end {
							continue
						}
						expr := strings.Join(strings.Fields(string(src[start:end])), " ")
						for lit := range xferTierLiterals {
							// EXACT match, not Contains. "8 * 1024 * 1024" is a PREFIX of
							// "8 * 1024 * 1024 * 1024" (8 GiB, xferBucketCap — a JetStream bucket
							// ceiling, not a transfer tier), and a substring predicate flagged it on the
							// first run of this gate. That is the same class of defect batch C exists to
							// remove, reproduced inside the gate meant to prevent it.
							if expr == lit {
								out = append(out, topoMatchHit{
									file: rel, line: fset.Position(id.Pos()).Line, fn: id.Name, marker: expr,
								})
							}
						}
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatalf("walk: %v", err)
		}
	}
	return out
}
