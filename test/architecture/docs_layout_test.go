package architecture

import (
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// docs_layout_test.go — process artifacts must not sit in docs/ top level.
//
// origin: line-2 external review M25. CLAUDE.md step 7 calls its archiving criterion "机械，不靠临场判断"
// (mechanical, no on-the-spot judgement) and then gives, as the operable form, a counterfactual question to
// ask about a hypothetical future reader: "would the next person changing code need to read this?" That is
// a judgement call, and the whole clause had no gate at all — which is how `batch-a-roadmap.md` came back
// to docs/ top level after commit 03ff578 had already archived it once. The clause itself says the
// sediment's cause is "reports and baselines filed side by side, and nobody dares to bulk-archive".
//
// The judgement half cannot be automated and is not claimed to be. But one half IS mechanical: a filename
// ending in -plan / -review / -rereview / -tasklist / -roadmap names a process artifact by construction,
// and those are exactly the four suffixes the clause enumerates. That half is checked here.
//
// SCOPE: TRACKED files only. docs/cluster-ha-realmachine-test-plan.md matches `*-plan.md` and is
// deliberately exempt — it is in .gitignore (line 67) and untracked, i.e. a local working document for the
// operator's own fleet runs rather than a repo artifact. The archive clause is about repo hygiene, so an
// untracked local file is outside it. (Line-2 review M24 asked for an explicit ruling on this file; this is
// it. Nothing needs to move.)
//
// The list is checked against `git ls-files` rather than a directory read precisely so that ruling is
// mechanical rather than a name-based exception someone has to remember.

var processArtifactRe = regexp.MustCompile(`-(plan|review|rereview|tasklist|roadmap)(-round[0-9]+)?\.md$`)

func TestDocsTopLevelHoldsNoProcessArtifacts(t *testing.T) {
	root := repoRoot(t)

	cmd := exec.Command("git", "ls-files", "docs")
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("git ls-files docs: %v", err)
	}

	var offenders []string
	topLevel := 0
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		rel := strings.TrimSpace(line)
		if rel == "" || !strings.HasSuffix(rel, ".md") {
			continue
		}
		// Top level only: docs/x.md, not docs/reviews/x.md — the archive IS docs/reviews.
		if filepath.Dir(rel) != "docs" {
			continue
		}
		topLevel++
		if processArtifactRe.MatchString(filepath.Base(rel)) {
			offenders = append(offenders, rel)
		}
	}

	if topLevel < 5 {
		t.Fatalf("only %d tracked markdown files found at docs/ top level — `git ls-files` is not returning "+
			"what this gate assumes, so a clean result would mean nothing", topLevel)
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d process artifact(s) are tracked at docs/ top level:\n  %s\n\n"+
			"docs/ top level holds LIVE documents — the baselines someone reads before changing code. A "+
			"plan, review, tasklist or finished roadmap describes work that is over. `git mv` it into "+
			"docs/reviews/ and add a line to docs/reviews/INDEX.md (CLAUDE.md §3 step 7).\n\n"+
			"This gate exists because that clause was carried for an entire increment with nothing "+
			"enforcing it, and the file it was written about came back to this directory.",
			len(offenders), strings.Join(offenders, "\n  "))
	}
}

// TestProcessArtifactPatternRecognisesTheRealShapes is the companion. The pattern above decides what gets
// moved, so its false positives block legitimate baselines and its false negatives are the sediment.
// gate-control: TestProcessArtifactPatternRecognisesTheRealShapes
func TestProcessArtifactPatternRecognisesTheRealShapes(t *testing.T) {
	mustMatch := []string{
		"line2-plan.md", "line2-review.md", "batch-a-roadmap.md", "b6-tasklist.md",
		"p13-external-review.md", "p13-external-review-round2.md", "g4-external-rereview.md",
	}
	for _, n := range mustMatch {
		if !processArtifactRe.MatchString(n) {
			t.Errorf("pattern does NOT match %q — a process artifact this gate is supposed to catch", n)
		}
	}

	// Every live baseline in docs/ today, plus the shapes most likely to collide. A false positive here
	// would demand that a baseline be archived, which is the opposite of the rule.
	mustNotMatch := []string{
		"architecture.md", "requirements.md", "usage.md", "broker-ops.md", "cluster.md",
		"cluster-runbook.md", "deploy-tier-gotchas.md", "devices.md", "devices-ops.local.md",
		"distributed-broker-architecture.md", "testing-standards.md",
		// Tricky: the words appear but not as the trailing artifact suffix.
		"review-process.md", "planning-guide.md", "roadmap-conventions.md", "code-review-checklist.md",
	}
	for _, n := range mustNotMatch {
		if processArtifactRe.MatchString(n) {
			t.Errorf("pattern WRONGLY matches %q — that is a live baseline, and archiving it would hide the "+
				"document people are supposed to read before changing code", n)
		}
	}
}
