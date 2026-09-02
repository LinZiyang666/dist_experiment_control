package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// simcluster_gate_set_test.go — test/simcluster/tests/run-all.sh must run every hermetic script in its
// directory, and every script it names must exist.
//
// WHY THIS EXISTS
// ---------------
// run-all.sh's own header says "a gate nobody runs is a gate that does not exist". On 2026-09-01, when
// it was first wired into `make gates` and CI (docs/reviews/test-system-overhaul-plan.md B1), three
// things were true at once:
//
//   - nothing called run-all.sh — not the Makefile, not ci.yml, not any architecture gate;
//   - three scripts in the directory were not in its loop (r16-g67-g69-external-review.sh,
//     r16-g67-g69-external-rereview.sh, s7-s9-external-review.sh), and one of them is bash-only
//     while the loop invoked everything with `sh`, so it could not have run even if listed;
//   - the set was RED on a clean tree: ledger-crosscheck reported gotcha #80 as unowned because its
//     heading said 已修 and the gate's closed-vocabulary is 已修复.
//
// Wiring fixes the first. This file fixes the second going forward: the loop and the directory are
// reconciled BOTH ways, so a new script cannot be dropped beside the set without joining it, and a
// name in the loop cannot outlive its file. The third was a ledger edit; it is recorded here so the
// next person who finds the set red on a clean tree knows it has happened before.
//
// gate-control: TestSimclusterGateSetReconciliationSeesEveryShape

const (
	simTestsDir = "test/simcluster/tests"
	simRunAll   = "run-all.sh"
)

// simGateSetExclusions lists scripts under tests/ that run-all.sh deliberately does NOT run, each with
// a reason. It is a draining ledger with the usual reverse assertion (an entry naming a file that no
// longer exists reddens) and starts EMPTY: on the day the gate landed every script either sat in the
// loop or was invoked explicitly by run-all.sh (kept-sites.sh, via --check). An allow-list with
// nothing in it is not an allow-list — it is the promise that there is nothing to allow.
var simGateSetExclusions = map[string]string{}

// simGateSetExclusionsCap pins the table's size both ways (internal review L1-F8: an exception table
// without a cap can grow silently; every other ledger in this tree is pinned).
const simGateSetExclusionsCap = 0

var (
	// The for-loop word list: `for t in a b c; do`.
	runAllLoopRe = regexp.MustCompile(`(?m)^for t in (.+); do\s*$`)
	// An explicit invocation of one script by name: sh "$HERE/kept-sites.sh" --check ...
	runAllExplicitRe = regexp.MustCompile(`"\$HERE/([A-Za-z0-9_.-]+)\.sh"`)
)

// runAllNames returns the script base names (without .sh) that run-all.sh runs: the loop list plus
// every script it invokes by literal name. Shared with the self-check.
func runAllNames(text string) (loop, explicit []string) {
	if m := runAllLoopRe.FindStringSubmatch(text); m != nil {
		loop = strings.Fields(m[1])
	}
	for _, m := range runAllExplicitRe.FindAllStringSubmatch(text, -1) {
		explicit = append(explicit, m[1])
	}
	return loop, explicit
}

// reconcileGateSet is the pure comparison: scripts present in the directory vs names run-all.sh runs.
// Returns (not run, named but missing, stale exclusions).
func reconcileGateSet(scripts []string, loop, explicit []string, exclusions map[string]string) (notRun, missing, stale []string) {
	runs := map[string]bool{}
	for _, n := range loop {
		runs[n] = true
	}
	for _, n := range explicit {
		runs[n] = true
	}
	present := map[string]bool{}
	for _, s := range scripts {
		present[s] = true
	}
	for _, s := range scripts {
		if s == strings.TrimSuffix(simRunAll, ".sh") {
			continue
		}
		if runs[s] {
			if _, excluded := exclusions[s+".sh"]; excluded {
				stale = append(stale, s+".sh (excluded but also run)")
			}
			continue
		}
		if _, excluded := exclusions[s+".sh"]; excluded {
			continue
		}
		notRun = append(notRun, s+".sh")
	}
	for n := range runs {
		if !present[n] {
			missing = append(missing, n+".sh")
		}
	}
	for f := range exclusions {
		if !present[strings.TrimSuffix(f, ".sh")] {
			stale = append(stale, f+" (excluded but does not exist)")
		}
	}
	sort.Strings(notRun)
	sort.Strings(missing)
	sort.Strings(stale)
	return notRun, missing, stale
}

func TestSimclusterHermeticGateSetRunsEveryScript(t *testing.T) {
	root := repoRoot(t)
	dir := filepath.Join(root, simTestsDir)
	raw, err := os.ReadFile(filepath.Join(dir, simRunAll))
	if err != nil {
		t.Fatalf("read %s: %v", simRunAll, err)
	}
	loop, explicit := runAllNames(string(raw))
	if len(loop) < 10 {
		t.Fatalf("parsed only %d names from run-all.sh's loop — the `for t in ...; do` line moved or "+
			"the parser is broken (G2)", len(loop))
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	var scripts []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sh") {
			continue
		}
		scripts = append(scripts, strings.TrimSuffix(e.Name(), ".sh"))
	}
	if n := len(simGateSetExclusions); n != simGateSetExclusionsCap {
		t.Errorf("simGateSetExclusions has %d entries, cap says %d — move the cap in the same change, with the reason", n, simGateSetExclusionsCap)
	}
	notRun, missing, stale := reconcileGateSet(scripts, loop, explicit, simGateSetExclusions)
	if len(notRun) > 0 {
		t.Errorf("%d script(s) under %s are not run by %s:\n  %s\n\n"+
			"Add them to the loop (or invoke them explicitly), or register them in simGateSetExclusions "+
			"WITH a reason. A gate script that sits beside the set but not in it is the exact shape this "+
			"file was written for.", len(notRun), simTestsDir, simRunAll, strings.Join(notRun, "\n  "))
	}
	if len(missing) > 0 {
		t.Errorf("%s names %d script(s) that do not exist:\n  %s\n\n"+
			"Its loop reports a missing script as FAIL, so this would already be red at run time — "+
			"but only for whoever runs it. Fix the loop.", simRunAll, len(missing), strings.Join(missing, "\n  "))
	}
	if len(stale) > 0 {
		t.Errorf("%d stale exclusion(s) in simGateSetExclusions:\n  %s", len(stale), strings.Join(stale, "\n  "))
	}
}

// TestSimclusterGateSetReconciliationSeesEveryShape is the G2 self-check: synthetic run-all text and a
// synthetic directory listing, through the same parser and comparison the gate uses. Both directions
// and the stale-exclusion path must each report exactly their case.
func TestSimclusterGateSetReconciliationSeesEveryShape(t *testing.T) {
	text := `#!/bin/sh
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
for t in alpha beta gamma; do
    if out=$(sh "$HERE/$t.sh" 2>&1); then printf 'PASS\n'; fi
done
if out=$(sh "$HERE/tool.sh" --check "$HERE/baseline.tsv" 2>&1); then printf 'PASS\n'; fi
`
	loop, explicit := runAllNames(text)
	if strings.Join(loop, ",") != "alpha,beta,gamma" {
		t.Fatalf("loop parse: got %v", loop)
	}
	if strings.Join(explicit, ",") != "tool" {
		t.Fatalf("explicit parse: got %v (the `$t` invocation must NOT count — it is not a literal name)", explicit)
	}
	// Directory: alpha, beta present; gamma MISSING; delta present but not run; tool present (explicit);
	// run-all itself present and ignored; excluded-one present and in the exclusion table (fine);
	// epsilon present, excluded AND in the loop would be stale — covered by a second table below.
	scripts := []string{"alpha", "beta", "delta", "tool", "run-all", "excluded-one"}
	notRun, missing, stale := reconcileGateSet(scripts, loop, explicit,
		map[string]string{"excluded-one.sh": "reason", "ghost.sh": "reason for a file that does not exist"})
	if strings.Join(notRun, ",") != "delta.sh" {
		t.Errorf("notRun: got %v, want [delta.sh]", notRun)
	}
	if strings.Join(missing, ",") != "gamma.sh" {
		t.Errorf("missing: got %v, want [gamma.sh]", missing)
	}
	if len(stale) != 1 || !strings.HasPrefix(stale[0], "ghost.sh") {
		t.Errorf("stale: got %v, want the ghost.sh entry", stale)
	}
	// An exclusion for a script that IS run is stale in the other direction.
	_, _, stale2 := reconcileGateSet([]string{"alpha"}, []string{"alpha"}, nil, map[string]string{"alpha.sh": "reason"})
	if len(stale2) != 1 || !strings.HasPrefix(stale2[0], "alpha.sh") {
		t.Errorf("excluded-but-run: got %v", stale2)
	}
}
