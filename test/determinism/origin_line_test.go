package determinism

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// origin_line_test.go — an `// origin:` line must point at something that EXISTS (B6-4).
//
// WHY THESE LINES EXIST AT ALL
// ----------------------------
// B6 renamed 158 test files from process names (p13_external_review_round6_test.go,
// g4_external_review_fixes_test.go, codex_allgreen_external_review_test.go) to topic names. That is the
// right trade — a topic name is findable by the person changing the unit under test, a round number is
// not — but it DELETES information. The old filename was the only record of which review round produced
// those assertions, and for 94 of the renamed files nothing in the prose said it either.
//
// The roadmap's answer (S1-refactor-roadmap.md:478-482) is the `// origin:` line: it survives a rename,
// it is greppable, and unlike the filename it can name the review DOCUMENT rather than just a round.
// That last part is what makes it worth the line — `docs/reviews/p13-external-review-round6.md` is
// something a reader can open.
//
// WHY IT NEEDS A GATE
// -------------------
// A pointer that rots is worse than no pointer: it costs the reader a lookup and then tells them
// nothing. Review documents do get renamed and consolidated (this repo has 354 of them), so the day one
// moves, every origin line naming it becomes a dead link with no way to notice. This gate makes that
// day fail loudly instead.
//
// It deliberately does NOT require every test file to HAVE an origin line. Most files never needed one:
// their doc comments already say where they came from, and a blanket requirement would produce 500
// ceremonial lines whose only reader is this gate. The rule is narrow on purpose — if you write the
// line, it must be true.

// originDocRef matches a docs/... path mentioned anywhere on an `// origin:` line. Several lines cite
// two documents (a plan and its review), so all matches on the line are checked.
var originDocRef = regexp.MustCompile(`docs/[A-Za-z0-9._/-]+\.md`)

func TestOriginLinesPointAtDocumentsThatExist(t *testing.T) {
	root := repoRoot(t)

	var broken []string
	lines := 0
	refs := 0
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
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
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		for i, line := range strings.Split(string(src), "\n") {
			trimmed := strings.TrimSpace(line)
			if !strings.HasPrefix(trimmed, "// origin:") {
				continue
			}
			lines++
			for _, ref := range originDocRef.FindAllString(trimmed, -1) {
				refs++
				// Trim a trailing comma/period that belongs to the prose, not the path.
				ref = strings.TrimRight(ref, ".,;")
				if !strings.HasSuffix(ref, ".md") {
					continue
				}
				if _, serr := os.Stat(filepath.Join(root, ref)); serr != nil {
					broken = append(broken, fmt.Sprintf("%s:%d cites %s", rel, i+1, ref))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	// NON-VACUITY, and it is legitimate here (docs/testing-standards.md G2b): the success state of this
	// gate still CONTAINS origin lines and their document references — unlike the naming freeze, whose
	// success state empties the thing it counts. If the scan stops finding them, it has gone blind.
	if lines < 90 {
		t.Fatalf("found only %d `// origin:` line(s); B6 added one to each of the 94 renamed files whose "+
			"prose did not already record where they came from. The scan has gone blind, or the lines "+
			"were removed — either way this gate is now asserting nothing.", lines)
	}
	if refs < 40 {
		t.Fatalf("found %d origin line(s) but only %d document reference(s); about half the B6 lines cite "+
			"a docs/reviews/*.md file. A regex that stopped matching them makes every check below vacuous.",
			lines, refs)
	}

	sort.Strings(broken)
	if len(broken) > 0 {
		t.Errorf("%d `// origin:` line(s) cite a document that does not exist:\n  %s\n\n"+
			"These lines are the ONLY record of which review round produced those assertions — the "+
			"filename that used to carry it was deleted by the B6 rename. A dead pointer costs the "+
			"reader a lookup and returns nothing, which is worse than no pointer at all. Update the "+
			"path if the document moved, or drop the citation and keep the round name.",
			len(broken), strings.Join(broken, "\n  "))
	}
}

// TestOriginDocRefRecognisesTheRealShapes is the self-check, over SYNTHESIZED lines. Synthesized rather
// than counted from the tree because a matcher that silently stopped matching would report zero broken
// references and read as a clean bill of health (docs/testing-standards.md G2).
func TestOriginDocRefRecognisesTheRealShapes(t *testing.T) {
	cases := []struct {
		line string
		want []string
	}{
		{
			line: "// origin: d8_external_review_test.go (renamed in B6) — docs/reviews/d8-external-review.md",
			want: []string{"docs/reviews/d8-external-review.md"},
		},
		{
			line: "// origin: b4_alert_test.go (renamed in B6) — docs/reviews/b4-plan.md, docs/reviews/b4-review.md",
			want: []string{"docs/reviews/b4-plan.md", "docs/reviews/b4-review.md"},
		},
		{
			// The half that cites no document: the old filename alone still names the round.
			line: "// origin: g67_caps_test.go (renamed in B6)",
			want: nil,
		},
		{
			line: "// origin: p13_external_review_round6_test.go — docs/reviews/p13-external-review-round6.md.",
			want: []string{"docs/reviews/p13-external-review-round6.md"},
		},
	}
	for _, tc := range cases {
		got := originDocRef.FindAllString(tc.line, -1)
		for i := range got {
			got[i] = strings.TrimRight(got[i], ".,;")
		}
		if strings.Join(got, "|") != strings.Join(tc.want, "|") {
			t.Errorf("on %q\n  got  %v\n  want %v\nA matcher that misses a citation lets it rot; one that "+
				"over-matches turns prose into a false broken-link report.", tc.line, got, tc.want)
		}
	}
}

// prereleaseAuditAnchor matches `prerelease audit <lane>/<ID>` on an `// origin:` line.
// The lane is documentation for the reader; the ID is what has to resolve.
var prereleaseAuditAnchor = regexp.MustCompile(`prerelease audit [a-z0-9-]+/([A-Za-z0-9-]+)`)

// origin: prerelease audit round 2, C10.
//
// AN ANCHOR THAT RESOLVES TO NOTHING IS WORSE THAN NO ANCHOR.
//
// The gate above only checks `docs/....md` paths, and the 175 anchors this audit
// added carry no path — they name a lane and a finding id. So none of them were
// checked by anything, and round 2 found ids that appeared in no document at all:
// a reader who greps for `cli-serve-agent-cluster/L4-F1` finds the one comment that
// mentions it and learns nothing, having spent the lookup.
//
// The plan is the index (its §8 lists the ids added during implementation), so this
// is the same contract as the doc-path half: if you write the line, it must be true.
func TestPrereleaseAuditAnchorsResolve(t *testing.T) {
	root := repoRoot(t)
	index := ""
	for _, name := range []string{
		"docs/reviews/prerelease-audit-plan.md",
		"docs/reviews/prerelease-audit-review.md",
	} {
		body, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			// The audit documents are archived together with the code they
			// describe. Absent, there is nothing to reconcile against — say so
			// rather than passing silently.
			t.Skipf("%s is not present; the anchor index cannot be checked", name)
		}
		index += string(body)
	}

	seen := map[string]bool{}
	var dead []string
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
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
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		for i, line := range strings.Split(string(src), "\n") {
			if !strings.Contains(line, "origin:") {
				continue
			}
			for _, m := range prereleaseAuditAnchor.FindAllStringSubmatch(line, -1) {
				id := m[1]
				if id == "" || seen[id] {
					continue
				}
				seen[id] = true
				if !strings.Contains(index, id) {
					dead = append(dead, fmt.Sprintf("%s:%d cites %s", rel, i+1, id))
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(seen) == 0 {
		t.Skip("no prerelease-audit anchors in the tree")
	}
	sort.Strings(dead)
	if len(dead) > 0 {
		t.Fatalf("%d prerelease-audit anchor id(s) resolve to nothing in the audit documents:\n  %s\n\n"+
			"Add the id to docs/reviews/prerelease-audit-plan.md §8 with a one-line summary. An "+
			"anchor exists so a reader can find out WHY a line is the way it is; one that leads "+
			"nowhere costs them the lookup and pays back nothing.",
			len(dead), strings.Join(dead, "\n  "))
	}
}
