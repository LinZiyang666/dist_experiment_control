package architecture

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// structural_budget_test.go — the structural debt ratchet (S3 G3.2, reshaped by the line-2 plan §4).
//
// WHAT IT MEASURES
// ----------------
// FOUR dimensions, all of them COMMENT-IMMUNE:
//
//	type-methods            how many methods hang off one type
//	pkg-files               how many production files are in one package
//	pkg-code-lines          how much CODE (comment-stripped, quantised to 2000) is in one package
//	main-noncli-code-lines  how much non-CLI logic is trapped in package main (comment-stripped, q. 100)
//
// This header said "Three dimensions, and here is why S3's fourth is deliberately absent" for the whole of
// stage C, 290 lines above the fourth dimension it now implements. origin: line-2 closure verification —
// the same "a declaration nothing checks" defect this file is a gate against, in the gate's own doc.
//
// WHAT S3'S REJECTED FOURTH ACTUALLY WAS, AND WHAT REPLACED IT
// -----------------------------------------------------------
// S3 proposed `pkg-lines`: PHYSICAL .go lines per package. That one is still rejected, and the reason is
// measured rather than aesthetic: across batches A/B/C internal/broker grew 3249 lines, of which 2026 --
// 62.4% -- were COMMENT lines. A physical-line ratchet on this repo is, empirically, mostly a comment
// ratchet, and its failure text would have called comment growth "structural debt" and comment deletion
// "good". This repo's comments carry incident history and invariant arguments; a gate that taxes them is
// worse than no gate.
//
// `pkg-code-lines` is NOT that. It strips comments via go/scanner and quantises to 2000, so it measures the
// thing pkg-lines was reaching for without the tax. It exists because plan §11 X2's rejection of pkg-lines
// carried a reopen condition -- "find a comment-immune package-size measure AND show pkg-files missing real
// growth" -- and both halves came true inside the same change that wrote the rejection: countCodeLines is
// that measure, and a review lane appended 1082 lines of real code to internal/broker/loopset.go with every
// gate green, because type-methods and pkg-files are both blind to growth INSIDE existing files.
//
// The same measurement is why main-noncli is counted in NON-COMMENT CODE LINES via go/scanner rather
// than physical lines: the dimension is worth keeping (it fell 3673 -> 1722 when batches A/B moved
// orchestration out of package main, and that win should be locked in), but not at the price of taxing
// the comments in those files.
//
// THE RATCHET IS BIDIRECTIONAL, WHICH IS NOT WHAT S3 SPECIFIED
// -----------------------------------------------------------
// S3 said "current <= golden passes". That form rots: after a refactor drops Broker from 279 methods to
// 200, the golden still says 279 and 79 methods of slack accumulate silently, so the gate stops
// measuring anything long before anyone notices. This file uses the numeric form of B6's DRAINING
// LEDGER instead -- BOTH directions fail:
//
//	current > golden   debt grew. Hand-edit the golden and say why in the commit message.
//	current < golden   debt shrank. Run -update-structural-budget to lock the win in.
//	entity fell below the threshold entirely   delete its line; the ledger shrinks.
//	entity over threshold with no line         a new offender appeared.
//
// -update-structural-budget REFUSES TO WIDEN. That single property is what keeps the routine direction
// safe: because the flag can only tighten, running it can never smuggle unrelated growth past review,
// so "shrank -> red -> run update" being a common path costs nothing. Copying
// cmd/tether/command_tree_inventory_test.go's unconditional rewrite instead would have made the flag a
// one-keystroke way to grant yourself more budget, i.e. a gate that anybody over budget can dissolve.

var updateStructuralBudget = flag.Bool("update-structural-budget", false,
	"tighten the structural budget golden to today's values (never widens; over-budget entities are refused)")

const structuralBudgetGolden = "testdata/structural_budget_golden.txt"

// Thresholds. An entity below its threshold is not registered at all, so the ledger only ever lists
// things that are ALREADY oversized -- it is a list of debts, not an inventory.
const (
	typeMethodThreshold = 40
	pkgFileThreshold    = 20
	// Quantised deliberately coarse. A per-line ratchet on a package would redden on every ordinary
	// commit (see PI-9 for what that felt like on main-noncli), so the ledger records code lines
	// ROUNDED DOWN to this multiple: day-to-day work is invisible, and crossing a 2000-line boundary is
	// the deliberate event worth a conversation.
	pkgCodeLineQuantum   = 2000
	pkgCodeLineThreshold = 2000

	// main-noncli gets the same treatment at a finer grain. origin: line-2 review PI-9. This dimension
	// shipped as an EXACT line count, and the cost showed up immediately: two golden raises inside the
	// increment that introduced it (1102 -> 1106 -> 1116), for +4 and +10 lines of ordinary work. A
	// ratchet that reddens on a four-line edit teaches people to run `-update` reflexively, which is
	// exactly how a ratchet stops being read.
	//
	// 100 rather than 2000 because the quantity is a tenth the size: at 2000 this entity would round to
	// zero and vanish from the ledger entirely. A new non-CLI helper worth ~100 lines in package main is
	// the smallest thing this dimension should still notice, and it does.
	mainNonCLIQuantum = 100
)

type budgetEntry struct {
	kind   string // "type-methods" | "pkg-files" | "pkg-code-lines" | "main-noncli-code-lines"
	entity string
	value  int
}

func (e budgetEntry) key() string { return e.kind + " " + e.entity }

// ---------- measurement ----------

// isProdGoFile reports whether a path is a non-test .go file.
func isProdGoFile(name string) bool {
	return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_test.go")
}

// measureTypeMethods counts methods per receiver type across internal/ and cmd/.
func measureTypeMethods(t *testing.T, root string) []budgetEntry {
	t.Helper()
	counts := map[string]int{}
	for _, base := range []string{"internal", "cmd"} {
		walkPkgDirs(t, root, filepath.Join(root, base), func(dir, rel string) {
			names, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir %s: %v", dir, err)
			}
			for _, de := range names {
				if de.IsDir() || !isProdGoFile(de.Name()) {
					continue
				}
				fset := token.NewFileSet()
				f, perr := parser.ParseFile(fset, filepath.Join(dir, de.Name()), nil, 0)
				if perr != nil {
					t.Fatalf("parse %s/%s: %v", rel, de.Name(), perr)
				}
				for _, d := range f.Decls {
					fn, ok := d.(*ast.FuncDecl)
					if !ok || fn.Recv == nil || len(fn.Recv.List) == 0 {
						continue
					}
					if name := receiverTypeName(fn.Recv.List[0].Type); name != "" {
						counts[rel+"."+name]++
					}
				}
			}
		})
	}
	var out []budgetEntry
	for entity, n := range counts {
		if n > typeMethodThreshold {
			out = append(out, budgetEntry{kind: "type-methods", entity: entity, value: n})
		}
	}
	return out
}

func receiverTypeName(e ast.Expr) string {
	switch v := e.(type) {
	case *ast.StarExpr:
		return receiverTypeName(v.X)
	case *ast.Ident:
		return v.Name
	case *ast.IndexExpr: // generic receiver Foo[T]
		return receiverTypeName(v.X)
	case *ast.IndexListExpr:
		return receiverTypeName(v.X)
	}
	return ""
}

// measurePkgFiles counts production files per package directory.
func measurePkgFiles(t *testing.T, root string) []budgetEntry {
	t.Helper()
	var out []budgetEntry
	for _, base := range []string{"internal", "cmd"} {
		walkPkgDirs(t, root, filepath.Join(root, base), func(dir, rel string) {
			names, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir %s: %v", dir, err)
			}
			n := 0
			for _, de := range names {
				if !de.IsDir() && isProdGoFile(de.Name()) {
					n++
				}
			}
			if n > pkgFileThreshold {
				out = append(out, budgetEntry{kind: "pkg-files", entity: rel, value: n})
			}
		})
	}
	return out
}

// measureMainNonCLICodeLines counts NON-COMMENT, non-blank code lines in the cmd/tether files that do
// not IMPORT cobra -- i.e. the orchestration/protocol logic that is sitting in package main instead of
// in a package that can be imported and tested directly (L03-F6).
//
// go/scanner with mode 0 does not emit COMMENT tokens, so counting the distinct lines that carry at
// least one emitted token yields exactly "lines with code on them" -- comments and blank lines are
// excluded structurally rather than by regex.
//
// CLASSIFY BY IMPORT, NOT BY SUBSTRING. origin: line-2 review PI-2. The first version asked
// `strings.Contains(src, "cobra")`, so a file was exempted from the count by MENTIONING cobra anywhere
// -- including in a comment. That is not hypothetical: cmd/tether/poll.go discusses cobra in prose,
// imports nothing of the sort, and was therefore invisible to a dimension whose entire job is to find
// non-CLI logic hiding in package main. Worse, the exemption ran the wrong way for a ratchet: writing
// the word "cobra" in a comment was a one-line way to remove a file from the budget forever.
//
// Parsing imports answers the question the dimension is actually asking ("is this file wiring up the
// CLI?") and cannot be spelled into existence by a comment.
func measureMainNonCLICodeLines(t *testing.T, root string) []budgetEntry {
	t.Helper()
	dir := filepath.Join(root, "cmd", "tether")
	names, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("readdir cmd/tether: %v", err)
	}
	total := 0
	for _, de := range names {
		if de.IsDir() || !isProdGoFile(de.Name()) {
			continue
		}
		path := filepath.Join(dir, de.Name())
		src, rerr := os.ReadFile(path)
		if rerr != nil {
			t.Fatalf("read %s: %v", path, rerr)
		}
		if importsCobra(t, path, src) {
			continue
		}
		total += countCodeLines(t, path, src)
	}
	q := total - total%mainNonCLIQuantum
	return []budgetEntry{{kind: "main-noncli-code-lines", entity: "cmd/tether", value: q}}
}

// importsCobra reports whether a file has cobra in its import list.
func importsCobra(t *testing.T, path string, src []byte) bool {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.ImportsOnly)
	if err != nil {
		t.Fatalf("parse imports of %s: %v", path, err)
	}
	for _, spec := range f.Imports {
		p, uerr := strconv.Unquote(spec.Path.Value)
		if uerr != nil {
			continue
		}
		if p == "github.com/spf13/cobra" {
			return true
		}
	}
	return false
}

func countCodeLines(t *testing.T, path string, src []byte) int {
	t.Helper()
	fset := token.NewFileSet()
	file := fset.AddFile(path, fset.Base(), len(src))
	var s scanner.Scanner
	s.Init(file, src, func(pos token.Position, msg string) {
		t.Fatalf("scan %s:%d: %s", path, pos.Line, msg)
	}, 0) // mode 0: comments are NOT returned
	lines := map[int]bool{}
	for {
		pos, tok, _ := s.Scan()
		if tok == token.EOF {
			break
		}
		if tok == token.SEMICOLON {
			// Auto-inserted semicolons carry the position of the newline, which would count a line
			// that has no code of its own. Explicit ones sit on a line that has other tokens anyway.
			continue
		}
		lines[file.Position(pos).Line] = true
	}
	return len(lines)
}

// walkPkgDirs invokes fn for every directory under base that holds at least one .go file. rel is
// reported relative to root, so entity names in the ledger are repo-relative import-ish paths
// (`internal/broker`) and do not embed the checkout directory's name.
func walkPkgDirs(t *testing.T, root, base string, fn func(dir, rel string)) {
	t.Helper()
	err := filepath.Walk(base, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return nil
		}
		switch info.Name() {
		case "testdata", ".git", "vendor":
			return filepath.SkipDir
		}
		names, rerr := os.ReadDir(p)
		if rerr != nil {
			return rerr
		}
		has := false
		for _, de := range names {
			if !de.IsDir() && isProdGoFile(de.Name()) {
				has = true
				break
			}
		}
		if !has {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		fn(p, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", base, err)
	}
}

// measurePkgCodeLines counts NON-COMMENT code lines per package, comment-stripped by the same
// go/scanner pass main-noncli uses.
//
// origin: line-2 external review PI-8, and the auditor's re-adjudication of plan §11 X2. X2 rejected
// S3's pkg-lines dimension FOREVER, with the reopen condition "find a comment-immune package-size
// measure AND show pkg-files misses real growth". Both halves were satisfied inside the same change
// that wrote the rejection: countCodeLines (below, comment-immune) is that measure, and a review lane
// appended 1082 lines of REAL CODE to internal/broker/loopset.go (219 -> 1301) with every gate green,
// because type-methods and pkg-files are both blind to growth inside existing files.
//
// That blindness was the worst possible shape: the other two dimensions were already ratcheted, so the
// only unmeasured direction was "pile more into a file that already exists" -- and CLAUDE.md's own
// "prefer merging into an existing file" rule points straight at it. A ratchet whose one gap is also
// the path of least resistance is a ratchet that shapes behaviour toward god files.
//
// Physical lines stay rejected (X2's real argument): 62.4% of internal/broker's growth was comments.
// This measures code.
func measurePkgCodeLines(t *testing.T, root string) []budgetEntry {
	t.Helper()
	var out []budgetEntry
	for _, base := range []string{"internal", "cmd"} {
		walkPkgDirs(t, root, filepath.Join(root, base), func(dir, rel string) {
			names, err := os.ReadDir(dir)
			if err != nil {
				t.Fatalf("readdir %s: %v", dir, err)
			}
			total := 0
			for _, de := range names {
				if de.IsDir() || !isProdGoFile(de.Name()) {
					continue
				}
				path := filepath.Join(dir, de.Name())
				src, rerr := os.ReadFile(path)
				if rerr != nil {
					t.Fatalf("read %s: %v", path, rerr)
				}
				total += countCodeLines(t, path, src)
			}
			if total > pkgCodeLineThreshold {
				quantised := total / pkgCodeLineQuantum * pkgCodeLineQuantum
				out = append(out, budgetEntry{kind: "pkg-code-lines", entity: rel, value: quantised})
			}
		})
	}
	return out
}

func measureAll(t *testing.T, root string) []budgetEntry {
	t.Helper()
	var out []budgetEntry
	out = append(out, measureTypeMethods(t, root)...)
	out = append(out, measurePkgFiles(t, root)...)
	out = append(out, measurePkgCodeLines(t, root)...)
	out = append(out, measureMainNonCLICodeLines(t, root)...)
	sort.Slice(out, func(i, j int) bool { return out[i].key() < out[j].key() })
	return out
}

// ---------- golden ----------

func readGolden(t *testing.T, path string) (map[string]int, error) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	out := map[string]int{}
	for i, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 3 {
			t.Fatalf("%s:%d: want `<kind> <entity> <count>`, got %q", path, i+1, line)
		}
		n, cerr := strconv.Atoi(f[2])
		if cerr != nil {
			t.Fatalf("%s:%d: unparseable count %q", path, i+1, f[2])
		}
		out[f[0]+" "+f[1]] = n
	}
	return out, nil
}

// goldenSentinel is the LAST line of the generated header. Everything after it is hand-written.
//
// The boundary is an exact full-line sentinel rather than a prefix match on some header sentence,
// because the prefix version had a compounding bug: the marker matched only the FIRST line of a
// two-line paragraph, so the second line was classified as hand-written and preserved -- while the
// template also re-emitted it. Every `-update` invocation therefore added one more copy of
// "# one already here, which is what makes running it a safe routine action." The flag CLAUDE.md calls
// "a safe routine action" grew the file it maintained, without bound, one line per run.
const goldenSentinel = "# ==== everything below this line is hand-written and survives regeneration ===="

// preservedComments recovers the hand-written comment blocks from an existing golden, keyed by the entry
// they sit above so that re-sorting entries does not detach a justification from what it justifies.
//
// origin: line-2 external review D3/GI-5/PF-5/IDG-4 — four lanes, same defect. -update-structural-budget
// rewrote the whole file from a fixed template, which silently deleted the 13 hand-written lines
// justifying the two budget raises. That flag is the action the RED path tells you to run, and CLAUDE.md
// §5 calls it "a safe routine action" — so the justification a reviewer is supposed to find had a
// half-life of one routine command. Worse, the same CLAUDE.md paragraph six lines up says "comments are
// an asset; any refactor must carry them across wholesale", and this was the gate's own flag violating it.
//
// The first fix preserved the comments but dumped them all in one blob at the top, which put the
// pkg-code-lines rationale directly above the main-noncli entry -- text that now described the wrong
// number. Attaching each block to its key is what makes the preservation actually readable.
func preservedComments(t *testing.T, path string) (orphans []string, byKey map[string][]string) {
	t.Helper()
	byKey = map[string][]string{}
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, byKey // first generation: nothing to preserve
		}
		t.Fatalf("read %s before preserving ledger justifications: %v\n\n"+
			"Only a genuinely absent file is first generation. Treating permission, I/O, or wrong-shape "+
			"errors as an empty ledger would let the update path overwrite a file whose hand-written "+
			"justifications it never read.", path, err)
	}

	// REFUSE rather than wipe when the boundary is missing. origin: line-2 closure verification M3.
	//
	// Everything below depends on finding goldenSentinel. Without it `past` never becomes true, nothing is
	// collected, and renderGolden then writes a file consisting of the template plus the data lines — i.e.
	// EVERY hand-written justification deleted, at rc=0, by the command the RED path tells people to run.
	// That is the worst available failure shape: silent, total, and triggered by following instructions.
	//
	// A file that exists but has no sentinel is either hand-mangled or written by an older version of this
	// code. Both need a human, and neither should be resolved by throwing the comments away.
	if len(strings.TrimSpace(string(b))) > 0 && !strings.Contains(string(b), goldenSentinel) {
		t.Fatalf("%s exists but contains no boundary line %q.\n\n"+
			"Refusing to regenerate: everything below that line is hand-written justification, and without "+
			"the boundary this function cannot tell it from the generated template — it would preserve "+
			"nothing and the write would delete all of it, silently, at rc=0.\n\n"+
			"Restore the line (it goes immediately after the NOTE paragraph, before the first comment block) "+
			"or, if the file really should be regenerated from scratch, delete it and re-run.",
			path, goldenSentinel)
	}

	past := false
	var pending []string
	for _, line := range strings.Split(string(b), "\n") {
		if line == goldenSentinel {
			past = true
			continue
		}
		if !past {
			continue // generated header; the template re-emits it verbatim
		}
		if strings.HasPrefix(line, "#") {
			pending = append(pending, line)
			continue
		}
		if k := entryKeyOfGoldenLine(line); k != "" {
			if len(pending) > 0 {
				byKey[k] = append(byKey[k], pending...)
				pending = nil
			}
			continue
		}
	}
	// A trailing block belongs to no entry -- most often because the entity it described dropped below
	// threshold and left the ledger. Keep it rather than delete it; losing the reasoning is the failure
	// this function exists to prevent, and an orphaned paragraph is a much cheaper problem than a
	// silently-deleted one.
	return pending, byKey
}

// entryKeyOfGoldenLine returns "<kind> <entity>" for a data line, or "" for anything else.
func entryKeyOfGoldenLine(line string) string {
	f := strings.Fields(line)
	if len(f) != 3 {
		return ""
	}
	if _, err := strconv.Atoi(f[2]); err != nil {
		return ""
	}
	return f[0] + " " + f[1]
}

func renderGolden(entries []budgetEntry, orphans []string, byKey map[string][]string) string {
	var b strings.Builder
	b.WriteString("# structural_budget_golden.txt — the debt ledger for test/architecture/structural_budget_test.go.\n")
	b.WriteString("#\n")
	b.WriteString("# Only entities ALREADY over threshold are listed, so this file is a list of debts, not an\n")
	b.WriteString("# inventory. Both directions of drift fail: growth demands a hand edit plus a justification in\n")
	b.WriteString("# the commit message, shrinkage demands `go test ./test/architecture/ -update-structural-budget`\n")
	b.WriteString("# so the improvement is locked in and cannot silently become slack.\n")
	b.WriteString("#\n")
	b.WriteString(fmt.Sprintf("# thresholds: type-methods > %d methods, pkg-files > %d files, pkg-code-lines > %d\n",
		typeMethodThreshold, pkgFileThreshold, pkgCodeLineThreshold))
	b.WriteString(fmt.Sprintf("# code lines. The two code-line dimensions are quantised DOWN (pkg-code-lines to %d,\n",
		pkgCodeLineQuantum))
	b.WriteString(fmt.Sprintf("# main-noncli-code-lines to %d) so ordinary commits are invisible and only crossing a boundary\n",
		mainNonCLIQuantum))
	b.WriteString("# is an event. main-noncli-code-lines has no lower threshold: it is always tracked.\n")
	b.WriteString("#\n")
	b.WriteString("# NOTE: -update-structural-budget only ever TIGHTENS. It refuses to write a value wider than the\n")
	b.WriteString("# one already here, and it refuses to admit a NEW over-budget entity at all, which is what makes\n")
	b.WriteString("# running it a safe routine action.\n")
	b.WriteString(goldenSentinel + "\n")
	for _, line := range orphans {
		b.WriteString(line + "\n")
	}
	for _, e := range entries {
		for _, line := range byKey[e.key()] {
			b.WriteString(line + "\n")
		}
		b.WriteString(fmt.Sprintf("%s %s %d\n", e.kind, e.entity, e.value))
	}
	return b.String()
}

func TestStructuralBudget(t *testing.T) {
	root := repoRoot(t)
	current := measureAll(t, root)
	goldenPath := filepath.Join(root, "test", "architecture", structuralBudgetGolden)

	if *updateStructuralBudget {
		updateGoldenTightenOnly(t, goldenPath, current)
		return
	}

	golden, err := readGolden(t, goldenPath)
	if err != nil {
		t.Fatalf("read golden %s: %v\nGenerate it with:\n"+
			"  go test ./test/architecture/ -run TestStructuralBudget -update-structural-budget", goldenPath, err)
	}

	seen := map[string]bool{}
	for _, e := range current {
		seen[e.key()] = true
		want, ok := golden[e.key()]
		if !ok {
			t.Errorf("NEW over-budget entity: %s = %d (threshold exceeded, not in the ledger).\n"+
				"  If this growth is intended, add the line and say WHY in the commit message.",
				e.key(), e.value)
			continue
		}
		switch {
		case e.value > want:
			t.Errorf("BUDGET EXCEEDED: %s = %d, ledger says %d (+%d).\n"+
				"  Raising a budget is an edit to an invariant: change the line BY HAND and justify it in the\n"+
				"  commit message. -update-structural-budget will refuse to widen it for you.",
				e.key(), e.value, want, e.value-want)
		case e.value < want:
			t.Errorf("DEBT SHRANK and the ledger is stale: %s = %d, ledger still says %d.\n"+
				"  Lock the improvement in:  go test ./test/architecture/ -run TestStructuralBudget -update-structural-budget\n"+
				"  (leaving the slack in place is how a ratchet quietly stops measuring anything.)",
				e.key(), e.value, want)
		}
	}
	var stale []string
	for key := range golden {
		if !seen[key] {
			stale = append(stale, key)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d ledger entr(ies) name something that is no longer over budget:\n  %s\n\n"+
			"Delete those lines — the ledger must drain. An allow-list that is only ever appended to stops\n"+
			"being a measurement and becomes a permanent exemption.",
			len(stale), strings.Join(stale, "\n  "))
	}
}

// updateGoldenTightenOnly writes the golden, but ONLY where the new value is tighter. An entity that is
// over its recorded budget is refused, loudly: the whole point of the ledger is that widening costs a
// deliberate hand edit, and a flag that widens on request is a gate anybody over budget can dissolve.
func updateGoldenTightenOnly(t *testing.T, goldenPath string, current []budgetEntry) {
	t.Helper()
	prev, err := readGolden(t, goldenPath)
	firstGeneration := false
	if err != nil {
		if !os.IsNotExist(err) {
			t.Fatalf("read golden for update: %v", err)
		}
		prev = map[string]int{}
		firstGeneration = true
	}
	// BOOTSTRAP IS A REAL MODE, NOT A COMMENT. origin: line-2 INDEPENDENT EXTERNAL REVIEW M6.
	//
	// The missing-golden failure message told the reader to run this flag to generate one. It could not
	// work: with no file, `prev` is empty, every measured entity hits the `!known` branch added for GI-4,
	// and the command refuses all twelve. So the documented recovery path was impossible — a instruction
	// that had never been executed by anyone, including the person who wrote it.
	//
	// The two halves are not in conflict once they are separated: `!known` exists to stop a NEW over-budget
	// entity being admitted to an EXISTING ledger without review. When there is no ledger at all, there is
	// no review to smuggle anything past — the whole file is the thing being reviewed. So bootstrap writes,
	// and says loudly that what it produced is a proposal rather than an accepted baseline.
	if firstGeneration {
		t.Logf("BOOTSTRAP: %s did not exist, so this run is generating it from scratch.\n"+
			"  The result is a PROPOSAL, not an accepted baseline: read every line before committing it, and "+
			"write the justification for each entry by hand. The refusal that normally guards new entities "+
			"is deliberately not applied here — there is no prior ledger for a new entity to be smuggled "+
			"past.", structuralBudgetGolden)
	}

	var refused []string
	out := make([]budgetEntry, 0, len(current))
	for _, e := range current {
		was, known := prev[e.key()]
		switch {
		case known && e.value > was:
			refused = append(refused, fmt.Sprintf("%s: ledger %d, measured %d (+%d)", e.key(), was, e.value, e.value-was))
			out = append(out, budgetEntry{kind: e.kind, entity: e.entity, value: was}) // keep the tighter value
		case !known && firstGeneration:
			// Bootstrap: there is no ledger to smuggle anything past. See the note above.
			out = append(out, e)
		case !known:
			// origin: line-2 external review GI-4. This branch used to fall through and WRITE the new
			// entity, rc=0, silently -- so a brand-new over-budget type or package was admitted to the
			// ledger by the routine tightening command, with no edit to review and no justification. The
			// file header claimed the flag "can never smuggle unrelated growth past review"; a new
			// entity is exactly unrelated growth, and it was the one shape the refusal missed.
			refused = append(refused, fmt.Sprintf("%s: NEW over-budget entity at %d (not in the ledger)",
				e.key(), e.value))
		default:
			out = append(out, e)
		}
	}
	if len(refused) > 0 {
		sort.Strings(refused)
		t.Fatalf("-update-structural-budget REFUSES %d change(s) that are not tightenings:\n  %s\n\n"+
			"This flag only ever tightens. To raise a budget or admit a new over-budget entity, edit %s by\n"+
			"hand and justify it in the commit message — that friction IS the gate.",
			len(refused), strings.Join(refused, "\n  "), structuralBudgetGolden)
	}

	if err := os.MkdirAll(filepath.Dir(goldenPath), 0o755); err != nil {
		t.Fatalf("mkdir testdata: %v", err)
	}
	orphans, byKey := preservedComments(t, goldenPath)
	if err := os.WriteFile(goldenPath, []byte(renderGolden(out, orphans, byKey)), 0o644); err != nil {
		t.Fatalf("write golden: %v", err)
	}
	t.Logf("tightened %s to %d entries", structuralBudgetGolden, len(out))
}

// TestStructuralBudgetMeasurementIsNonVacuous proves the three measurements actually measure. A green
// TestStructuralBudget with a broken scanner and an empty ledger is indistinguishable from a green one
// with a working scanner, and the empty-ledger state is reachable by accident.
func TestStructuralBudgetMeasurementIsNonVacuous(t *testing.T) {
	root := repoRoot(t)

	tm := measureTypeMethods(t, root)
	if len(tm) == 0 {
		t.Error("type-methods found nothing over 40 — internal/broker.Broker alone has well over 200")
	}
	var foundBroker bool
	for _, e := range tm {
		if e.entity == "internal/broker.Broker" {
			foundBroker = true
			if e.value < 100 {
				t.Errorf("internal/broker.Broker measured %d methods, which is implausibly low", e.value)
			}
		}
	}
	if !foundBroker {
		t.Error("type-methods did not find internal/broker.Broker at all")
	}

	if pf := measurePkgFiles(t, root); len(pf) == 0 {
		t.Error("pkg-files found no package over 20 production files — internal/broker has ~70")
	}

	mn := measureMainNonCLICodeLines(t, root)
	if len(mn) != 1 || mn[0].value == 0 {
		t.Errorf("main-noncli-code-lines produced %v, want exactly one non-zero entry", mn)
	}
	// The comment-immunity property is the reason this dimension is counted in code lines at all, so
	// assert it directly rather than trusting the scanner mode: a file that is ALL comments must
	// contribute zero.
	if got := countCodeLines(t, "probe.go", []byte("package p\n\n// just\n// comments\n")); got != 1 {
		t.Errorf("countCodeLines counted %d lines for a file whose only code is `package p`, want 1 "+
			"(comments must not count)", got)
	}
}

// TestUpdateFlagPreservesLedgerJustifications is the regression guard review M3 asked for and that the
// closure verification found missing.
//
// WHY IT MATTERS THAT THIS EXISTS. `-update-structural-budget` REWRITES the golden. The golden's value is
// not the twelve numbers — those are re-derivable in a second — it is the forty-odd hand-written lines
// explaining why each budget was raised, which is the only record of what a reviewer accepted and on what
// grounds. M3's finding was that the flag used to delete them silently; the fix (goldenSentinel +
// preservedComments + per-key re-attachment) was written, and then nothing exercised it. The whole writer
// path — updateGoldenTightenOnly, preservedComments, entryKeyOfGoldenLine, renderGolden — was reachable
// only by a human typing the flag, so its failure mode was rc=0 and a quietly emptier file.
//
// WHAT IS AND IS NOT COVERED. This drives the data functions against a temp file: preservation,
// attachment, idempotency, orphan rescue, and the missing-sentinel refusal. It does NOT drive
// updateGoldenTightenOnly's refusal branch, because that branch reports via t.Fatalf and a test cannot
// assert a Fatalf without dying with it. That branch is covered by mutation instead (plan §14 judgement 10
// runs the flag on an over-budget tree and checks it refuses), and saying so here is better than implying
// this test covers more than it does.
func TestUpdateFlagPreservesLedgerJustifications(t *testing.T) {
	entries := []budgetEntry{
		{kind: "pkg-files", entity: "internal/broker", value: 70},
		{kind: "type-methods", entity: "internal/broker.Broker", value: 279},
	}

	const orphanBlock = "# ORPHAN: a block attached to nothing, e.g. an entity that left the ledger."
	const brokerWhy = "# +8 the loopset split; every line buys an operator a stated cause."
	const typeWhy = "# Broker.Broker is the god type four audit lanes agreed not to split."

	build := func(sentinel string) string {
		var b strings.Builder
		b.WriteString("# header line the template owns\n")
		b.WriteString(sentinel + "\n")
		b.WriteString(orphanBlock + "\n")
		b.WriteString(brokerWhy + "\n")
		b.WriteString("pkg-files internal/broker 70\n")
		b.WriteString(typeWhy + "\n")
		b.WriteString("type-methods internal/broker.Broker 279\n")
		return b.String()
	}

	t.Run("preserves each block and keeps it attached to its own entry", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "golden.txt")
		if err := os.WriteFile(path, []byte(build(goldenSentinel)), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		orphans, byKey := preservedComments(t, path)
		out := renderGolden(entries, orphans, byKey)

		for _, want := range []string{orphanBlock, brokerWhy, typeWhy} {
			if !strings.Contains(out, want) {
				t.Errorf("regeneration DROPPED a hand-written line:\n  %s\n\n"+
					"That is M3 exactly: the flag CLAUDE.md calls a safe routine action deleting the only "+
					"record of why a budget was raised.", want)
			}
		}
		// Attachment, not merely survival: the justification must still sit immediately above the entry it
		// justifies, or a reader finds the loopset argument above the type-methods number.
		brokerAt := strings.Index(out, brokerWhy)
		entryAt := strings.Index(out, "pkg-files internal/broker 70")
		typeAt := strings.Index(out, typeWhy)
		typeEntryAt := strings.Index(out, "type-methods internal/broker.Broker 279")
		if brokerAt >= entryAt || entryAt >= typeAt || typeAt >= typeEntryAt {
			t.Errorf("blocks were preserved but DETACHED from their entries.\n%s\n\n"+
				"A justification above the wrong number is worse than a deleted one: it is a false "+
				"explanation a reader has no reason to doubt.", out)
		}
	})

	t.Run("is idempotent — a second regeneration changes nothing", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "golden.txt")
		if err := os.WriteFile(path, []byte(build(goldenSentinel)), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		o1, k1 := preservedComments(t, path)
		first := renderGolden(entries, o1, k1)
		if err := os.WriteFile(path, []byte(first), 0o644); err != nil {
			t.Fatalf("write first: %v", err)
		}
		o2, k2 := preservedComments(t, path)
		second := renderGolden(entries, o2, k2)
		if first != second {
			t.Errorf("regeneration is NOT idempotent — running the flag twice changes the file.\n\n"+
				"The first version of this mechanism grew the file by one duplicated header line per "+
				"invocation, without bound, because the boundary was a prefix match on a two-line "+
				"paragraph. An unbounded-growth bug in the routine maintenance command is invisible until "+
				"somebody reads the diff.\nfirst:\n%s\nsecond:\n%s", first, second)
		}
	})

	// origin: line-2 INDEPENDENT EXTERNAL REVIEW M6, second half. This subtest was named "refuses to
	// regenerate a golden whose sentinel is gone" and never called preservedComments. It asserted the
	// fixture lacked a sentinel and that a render with no preserved blocks carries no comments — both true
	// of any input, neither touching the guard. Deleting the real Fatalf left it green, and its own t.Log
	// handed the proof obligation to a manual mutation nobody was scheduled to run.
	//
	// The guard reports via t.Fatalf, which a test cannot assert without dying with it — so run it in a
	// SUBPROCESS. `go test -run <this> -update-structural-budget` against a mangled copy must exit non-zero
	// AND leave the file untouched, which is the property that matters: a refusal that still writes is not
	// a refusal.
	t.Run("refuses to regenerate a golden whose sentinel is gone", func(t *testing.T) {
		if os.Getenv("TETHER_BUDGET_SENTINEL_CHILD") == "1" {
			// Child: drive the real writer path at the path the parent seeded.
			path := os.Getenv("TETHER_BUDGET_SENTINEL_PATH")
			orphans, byKey := preservedComments(t, path) // t.Fatalf's here when the sentinel is missing
			_ = renderGolden(entries, orphans, byKey)
			return
		}

		dir := t.TempDir()
		path := filepath.Join(dir, "golden.txt")
		mangled := build("# ==== somebody edited the boundary line ====")
		if err := os.WriteFile(path, []byte(mangled), 0o644); err != nil {
			t.Fatalf("seed: %v", err)
		}
		before, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read seed: %v", err)
		}

		cmd := exec.Command(os.Args[0],
			"-test.run", "^TestUpdateFlagPreservesLedgerJustifications$/^refuses_to_regenerate_a_golden_whose_sentinel_is_gone$",
			"-test.v")
		cmd.Env = append(os.Environ(),
			"TETHER_BUDGET_SENTINEL_CHILD=1",
			"TETHER_BUDGET_SENTINEL_PATH="+path)
		out, runErr := cmd.CombinedOutput()

		if runErr == nil {
			t.Errorf("the child process exited 0 on a golden with no sentinel — the guard did not fire.\n\n"+
				"That is the state external review M6 found: this subtest was passing without ever calling "+
				"preservedComments, so deleting the Fatalf cost nothing.\nchild output:\n%s", out)
		}
		if !strings.Contains(string(out), "contains no boundary line") {
			t.Errorf("the child failed, but not with the sentinel refusal — so this test is proving something "+
				"other than what it claims.\nchild output:\n%s", out)
		}
		after, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read after: %v", err)
		}
		if string(after) != string(before) {
			t.Errorf("the guard refused BUT the file was modified. A refusal that still writes is not a "+
				"refusal — the hand-written justifications are exactly what must survive.\nbefore:\n%s\nafter:\n%s",
				before, after)
		}
	})
}
