package natsconf

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// jetstream_enablement_predicate_test.go — "is JetStream enabled?" must be answered in exactly ONE
// place, and raw `Parsed["jetstream"]` may not be that place outside this package.
//
// origin: batch B2 independent external review B2-1
//
// WHAT WENT WRONG
// ---------------
// B5 made Ownership.HasJetStream value-aware, because nats-server treats `jetstream: false|disabled|
// off|no` as an explicit DISABLE and the old key-presence version made BuildMergedConf refuse to render
// for a broker whose operator had deliberately switched JetStream off. That fix landed — and the manual
// takeover in cmd/tether/cluster_natsconf.go kept its OWN copy of the old predicate, inlined:
//
//	if _, hasJS := own.Parsed["jetstream"]; hasJS && own.JSStoreDir() == "" { return error }
//
// So `tether cluster takeover-natsconf` stayed permanently unavailable for that configuration shape,
// and told the operator to add a store_dir — to re-enable the subsystem they had turned off. One
// predicate was fixed, its duplicate was not, and nothing connected them.
//
// WHY A GATE AND NOT JUST THE FIX
// -------------------------------
// `Parsed` is an exported map, so re-deriving enablement from it is always one line away, reads
// naturally, and compiles. A conf carries TWO independent facts — cluster-block presence (topology) and
// whether JetStream is on (enablement) — and the wrong one looks identical to the right one at the call
// site. This gate keeps the enablement answer from being re-derived; IsClusteredTopology /
// IsStandaloneTopology answer the other question and are not enablement predicates at all.
//
// (An earlier version of this comment argued the topology predicates "must NOT become value-aware
// because merging them would break force-single's de-cluster". That defence was refuted by external
// review RB2-1: the compound predicates it defended were themselves the reason force-single could not
// de-cluster a `jetstream: false` + cluster{} conf. They are deleted; see Ownership.IsClusteredTopology.)

// jetstreamRawMapImplementers are the exact FUNCTIONS allowed to read Parsed["jetstream"] directly,
// WITH the reason. A file-level exemption silently covered a third reader added beside these two.
var jetstreamRawMapImplementers = map[string]string{
	"internal/natsconf/preflight.go:JSStoreDir": "harvests store_dir from the jetstream{} map",
	"internal/natsconf/preflight.go:HasJetStream": "the value-aware enablement implementation every " +
		"other site is required to call",
}

func TestJetStreamEnablementIsDecidedInExactlyOnePlace(t *testing.T) {
	root := repoRootForAssemblyParity(t)

	var offenders []string
	seenImplementers := map[string]bool{}
	scanned := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") || strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		src, rderr := os.ReadFile(p)
		if rderr != nil {
			return rderr
		}
		scanned++
		f, perr := parser.ParseFile(token.NewFileSet(), p, src, 0)
		if perr != nil {
			return perr
		}
		for _, pos := range rawJetStreamKeyReads(f) {
			key := rel + ":" + enclosingFunctionAt(f, pos)
			if jetstreamRawMapImplementers[key] != "" {
				seenImplementers[key] = true
				continue
			}
			line := strings.Count(string(src[:pos-1]), "\n") + 1
			offenders = append(offenders, key+":"+itoa(line))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// NON-VACUITY over the live tree, legitimate per docs/testing-standards.md G2b: the success state of
	// this gate still contains the files it walks (it is not trying to empty the tree of .go files), so a
	// count that collapses means the walk went blind rather than that the codebase got clean.
	if scanned < 100 {
		t.Fatalf("scanned only %d production .go file(s) — the walk is not "+
			"seeing the tree, so a re-derived enablement predicate would go unreported", scanned)
	}
	for key, why := range jetstreamRawMapImplementers {
		if !seenImplementers[key] {
			t.Errorf("jetstreamRawMapImplementers lists %s (%q), but that function no longer reads the raw "+
				"key — remove the stale exemption in the same change", key, why)
		}
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("JetStream enablement is re-derived from the raw Parsed map at:\n  %s\n\n"+
			"Use Ownership.HasJetStream() — it is the ONE value-aware predicate, and it is value-aware "+
			"because nats-server accepts `jetstream: false|disabled|off|no` as an explicit DISABLE. A "+
			"key-presence copy refuses to render for a broker whose operator deliberately switched "+
			"JetStream off, and then advises them to switch it back on.\n"+
			"If what you actually need is \"which topology mode is this conf in\", call "+
			"IsClusteredTopology / IsStandaloneTopology instead — they answer cluster-block presence and "+
			"say nothing about JetStream. If you need BOTH facts, write both: that is the point.",
			strings.Join(offenders, "\n  "))
	}
}

// TestEnablementPredicateGateSeesTheRealShape is the synthesized self-check. It has to be synthesized:
// the tree's success state is "no such expression exists outside this package", so a non-vacuity
// assertion made against real input would fail exactly when the codebase is correct
// (docs/testing-standards.md G2).
//
// The third row is the one that keeps this gate honest in the other direction — it must NOT report a
// topology predicate, or it would push people toward the merge that breaks force-single.
func TestEnablementPredicateGateSeesTheRealShape(t *testing.T) {
	cases := []struct {
		name  string
		src   string
		wantN int
		why   string
	}{
		{
			name: "the exact shape the takeover shipped",
			src: `package main

func f(own *natsconf.Ownership) error {
	if _, hasJS := own.Parsed["jetstream"]; hasJS && own.JSStoreDir() == "" {
		return errors.New("no")
	}
	return nil
}
`,
			wantN: 1,
			why:   "this is verbatim what cmd/tether/cluster_natsconf.go carried past the B5 fix",
		},
		{
			name: "a plain read, no comma-ok",
			src: `package main

func f(own *natsconf.Ownership) bool {
	return own.Parsed["jetstream"] != nil
}
`,
			wantN: 1,
			why:   "the comma-ok form is not the only spelling; a matcher keyed on it would miss this one",
		},
		{
			name: "the exported map is first assigned to a local",
			src: `package main

func f(own *natsconf.Ownership) bool {
	parsed := own.Parsed
	_, ok := parsed["jetstream"]
	return ok
}
`,
			wantN: 1,
			why: "this is the same raw key-presence predicate after an ordinary extract-local refactor. " +
				"A gate that only matches a selector directly under IndexExpr does not enforce the claimed " +
				"single enablement decision",
		},
		{
			name: "the extracted local is copied once",
			src: `package main

func f(own *natsconf.Ownership) bool {
	parsed := own.Parsed
	view := parsed
	_, ok := view["jetstream"]
	return ok
}
`,
			wantN: 1,
			why: "one more local assignment does not change the predicate. Alias tracking that stops after " +
				"the first selector extraction can still false-green an ordinary refactor",
		},
		{
			name: "HasJetStream + a topology predicate: both legitimate, neither reported",
			src: `package main

func f(own *natsconf.Ownership) bool {
	return own.HasJetStream() && own.IsClusteredTopology()
}
`,
			wantN: 0,
			why: "this is the SHAPE THE FIX PRESCRIBES — a caller that needs both facts writes both. " +
				"Reporting it would push people back toward one predicate answering two questions, which " +
				"is what made a de-cluster refuse and an `rm -rf` get advised to a broker with JS off",
		},
		{
			name: "another map key entirely",
			src: `package main

func f(own *natsconf.Ownership) bool {
	_, ok := own.Parsed["cluster"]
	return ok
}
`,
			wantN: 0,
			why:   "cluster-block presence is how the topology predicates work and is not an enablement claim",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := parser.ParseFile(token.NewFileSet(), "synthetic.go", tc.src, 0)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			// THE SHARED matcher, not a copy of it. This table used to inline its own walk, which meant
			// the self-check exercised a duplicate: the extract-local row could be added here and pass
			// against a second implementation while the real scanner stayed blind. A self-check that
			// reimplements the thing it checks proves nothing about the thing it checks.
			got := len(rawJetStreamKeyReads(f))
			if got != tc.wantN {
				t.Errorf("matcher found %d occurrence(s), want %d.\nwhy it matters: %s", got, tc.wantN, tc.why)
			}
		})
	}
}

// rawJetStreamKeyReads returns the position of every expression in f that reads the "jetstream" key out
// of an Ownership.Parsed map — whether the map is reached through the field selector or through a local
// that was assigned from it.
//
// RB2-1: the first version matched only `<expr>.Parsed["jetstream"]`. An ordinary extract-local
// refactor —
//
//	parsed := own.Parsed
//	_, ok := parsed["jetstream"]
//
// — is the same predicate and was invisible, which means the gate's claim of "exactly one enablement
// decision" was true only of one spelling. The alias set is computed first so an assignment anywhere in
// the file is picked up regardless of statement order; that over-approximates (a local named `parsed`
// assigned from something else later would also count) and the bias is deliberate: a false report costs
// a reader one look, a missed one costs a destructive operator instruction.
func rawJetStreamKeyReads(f *ast.File) []token.Pos {
	// Pass 1: locals assigned from a `.Parsed` selector, or transitively from another known alias.
	aliases := map[string]bool{}
	for {
		grew := false
		ast.Inspect(f, func(n ast.Node) bool {
			switch node := n.(type) {
			case *ast.AssignStmt:
				for i, rhs := range node.Rhs {
					if i >= len(node.Lhs) || !isParsedMapAlias(rhs, aliases) {
						continue
					}
					if id, ok := node.Lhs[i].(*ast.Ident); ok && id.Name != "_" && !aliases[id.Name] {
						aliases[id.Name] = true
						grew = true
					}
				}
			case *ast.ValueSpec:
				for i, rhs := range node.Values {
					if i >= len(node.Names) || !isParsedMapAlias(rhs, aliases) {
						continue
					}
					name := node.Names[i].Name
					if name != "_" && !aliases[name] {
						aliases[name] = true
						grew = true
					}
				}
			}
			return true
		})
		if !grew {
			break
		}
	}

	// Pass 2: index expressions with the "jetstream" key over either spelling.
	var out []token.Pos
	ast.Inspect(f, func(n ast.Node) bool {
		idx, ok := n.(*ast.IndexExpr)
		if !ok {
			return true
		}
		lit, ok := idx.Index.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING || strings.Trim(lit.Value, `"`) != "jetstream" {
			return true
		}
		switch base := idx.X.(type) {
		case *ast.SelectorExpr: // own.Parsed["jetstream"]
			if base.Sel.Name == "Parsed" {
				out = append(out, idx.Pos())
			}
		case *ast.Ident: // parsed["jetstream"], where parsed came from .Parsed
			if aliases[base.Name] {
				out = append(out, idx.Pos())
			}
		}
		return true
	})
	return out
}

func isParsedMapAlias(e ast.Expr, aliases map[string]bool) bool {
	switch node := e.(type) {
	case *ast.ParenExpr:
		return isParsedMapAlias(node.X, aliases)
	case *ast.SelectorExpr:
		return node.Sel.Name == "Parsed"
	case *ast.Ident:
		return aliases[node.Name]
	}
	return false
}

func enclosingFunctionAt(f *ast.File, pos token.Pos) string {
	for _, decl := range f.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if ok && fn.Pos() <= pos && pos <= fn.End() {
			return fn.Name.Name
		}
	}
	return "<file-scope>"
}

// itoa avoids pulling strconv in for one call in an error path.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
