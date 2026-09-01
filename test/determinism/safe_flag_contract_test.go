package determinism

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// safe_flag_contract_test.go — the `--safe` contract must be stated in all three places
// a user can read it, not just in the one the author happened to edit.
//
// origin: docs/deploy-tier-gotchas.md #81 (timan107, 2026-08-29)
// origin: docs/reviews/remote-fs-stale-health-review.md F-11
//
// # WHY A GATE
//
// `--safe` gained a second, load-bearing behaviour in the #81 fix: besides re-reading
// the mount table it now DISCARDS the cached mount-health verdicts and re-probes. That
// is the whole reason it works at all on a long-lived agent — on timan107 the mount
// table was already correct and only the cached verdict was wrong, so the old `--safe`
// was indistinguishable from no flag.
//
// When that contract changed, the two CLI help strings were updated and the two flag
// TABLE rows in docs/usage.md were not — so anyone looking the flag up in the manual
// read the old contract. This is the exact shape the repo has been bitten by before
// (a verb's contract changes; the hand-written copy that operators actually grep goes
// stale), which is why it is now mechanical rather than a review habit.
//
// Deliberately token-based rather than exact-text: the three renderings are prose in
// two languages and should not be forced into lockstep wording. What must not drift is
// the CLAIM.
func TestSafeFlagContractStatedInBothHelpAndUsage(t *testing.T) {
	root := repoRoot(t)

	// (1) The two help strings must stay identical to each other. They describe one
	// flag with one behaviour; letting them diverge is how one of them goes stale.
	execHelp := safeFlagHelp(t, filepath.Join(root, "cmd/tether/exec.go"))
	runHelp := safeFlagHelp(t, filepath.Join(root, "cmd/tether/run.go"))
	if execHelp != runHelp {
		t.Errorf("`--safe` help diverged between exec and run:\n  exec: %s\n  run:  %s", execHelp, runHelp)
	}
	for _, want := range []string{"re-probe", "cached"} {
		if !strings.Contains(execHelp, want) {
			t.Errorf("`--safe` help must state that it re-probes and discards cached verdicts "+
				"(missing %q): %s", want, execHelp)
		}
	}

	// (2) Every flag-table row for `--safe` in the manual must state the same thing.
	// Matching per ROW is load-bearing: a check that merely looked for the tokens
	// somewhere in usage.md would pass on the prose in §7.7 while both table rows still
	// carried the old contract — which is precisely what happened.
	usage := readFile(t, filepath.Join(root, "docs/usage.md"))
	rows := 0
	for _, line := range strings.Split(usage, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "| `--safe`") {
			continue
		}
		rows++
		if !strings.Contains(trimmed, "作废") || !strings.Contains(trimmed, "重探") {
			t.Errorf("docs/usage.md `--safe` table row does not state that it discards cached "+
				"mount-health verdicts and re-probes:\n  %s\n"+
				"Changing this flag's contract means changing THREE places: cmd/tether/exec.go, "+
				"cmd/tether/run.go, and every flag row here.", trimmed)
		}
	}
	if rows < 2 {
		t.Errorf("found %d `--safe` flag rows in docs/usage.md, want at least 2 (exec and run) — "+
			"if a row moved, this gate no longer covers it", rows)
	}

	// (3) The §7.7 prose is where the cost and the limits live; keep it in the loop too.
	if !strings.Contains(usage, "强制作废已缓存的健康判定") {
		t.Error("docs/usage.md §7.7 no longer explains that --safe discards cached health verdicts")
	}
}

// safeFlagHelp extracts the one-line help text registered for the --safe flag.
func safeFlagHelp(t *testing.T, path string) string {
	t.Helper()
	src := readFile(t, path)
	const marker = `"safe"`
	i := strings.Index(src, marker)
	if i < 0 {
		t.Fatalf("%s: no --safe flag registration found; this gate is now blind", path)
	}
	// The help string is the last quoted argument of the BoolVar call.
	rest := src[i:]
	end := strings.Index(rest, ")\n")
	if end < 0 {
		t.Fatalf("%s: could not delimit the --safe flag registration", path)
	}
	seg := rest[:end]
	last := strings.LastIndex(seg, `"`)
	if last <= 0 {
		t.Fatalf("%s: no help string in the --safe registration", path)
	}
	prev := strings.LastIndex(seg[:last], `"`)
	if prev < 0 {
		t.Fatalf("%s: malformed --safe help string", path)
	}
	return seg[prev+1 : last]
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}
