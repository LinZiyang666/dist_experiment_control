package architecture

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
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

// Bind the three broker-specific selftests explicitly, as well as reconciling
// the directory. Deleting both a script and its run-all word must still fail.
// origin: hot-update S6 plan §13 / T58
func brokerHermeticEntryProblem(runAll string, present map[string]bool) bool {
	loop, explicit := runAllNames(runAll)
	for _, name := range []string{"hot-update-broker-ledger-test", "hot-update-broker-probe-test", "hot-update-broker-drill-test"} {
		count := 0
		for _, entry := range append(loop, explicit...) {
			if entry == name {
				count++
			}
		}
		if count != 1 || !present[name] {
			return true
		}
	}
	return false
}

// Missing P29/P30 sources are a pending dependency, never an exclusion.
// origin: hot-update S6 plan §13 / T58
// gate-control: TestBrokerHermeticGateRegistrationControls
func TestBrokerHermeticGateRegistration(t *testing.T) {
	root := filepath.Join(repoRoot(t), simTestsDir)
	raw, err := os.ReadFile(filepath.Join(root, simRunAll))
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			present[strings.TrimSuffix(entry.Name(), ".sh")] = true
		}
	}
	if brokerHermeticEntryProblem(string(raw), present) {
		t.Fatal("S6 hermetic sources/entries missing or duplicated (pending P29/P30)")
	}
}

// origin: hot-update S6 plan §11 / T58
func TestBrokerHermeticGateRegistrationControls(t *testing.T) {
	const good = "for t in hot-update-broker-ledger-test hot-update-broker-probe-test hot-update-broker-drill-test; do\n"
	present := map[string]bool{"hot-update-broker-ledger-test": true, "hot-update-broker-probe-test": true, "hot-update-broker-drill-test": true}
	if brokerHermeticEntryProblem(good, present) {
		t.Fatal("positive entry rejected")
	}
	for name := range present {
		broken := map[string]bool{}
		for entry, value := range present {
			if entry != name {
				broken[entry] = value
			}
		}
		if !brokerHermeticEntryProblem(good, broken) {
			t.Fatal("removed script accepted")
		}
		missing := strings.Replace(good, name, "", 1)
		if !brokerHermeticEntryProblem(missing, present) || !brokerHermeticEntryProblem(missing, broken) {
			t.Fatal("removed gate/script pair accepted")
		}
		if !brokerHermeticEntryProblem(strings.Replace(good, name, name+" "+name, 1), present) {
			t.Fatal("duplicate entry accepted")
		}
	}
}

// Pin the S9 sources as well as directory reconciliation. Removing a script
// together with its dispatcher word must remain red; pending P32 is no waiver.
// origin: hot-update S9 plan §9.3 / §12 P37 / T87 / M34
var infraHermeticScripts = []string{
	"lint-install-selftest", "hot-update-infra-supply-test", "hot-update-infra-drill-test",
	"hot-update-infra-ledger-test", "hot-update-infra-fixture-test",
}

func infraHermeticEntryProblem(runAll string, present map[string]bool) bool {
	loop, explicit := runAllNames(runAll)
	for _, name := range infraHermeticScripts {
		count := 0
		for _, entry := range append(loop, explicit...) {
			if entry == name {
				count++
			}
		}
		if count != 1 || !present[name] {
			return true
		}
	}
	return false
}

// origin: hot-update S9 plan §12 P37 / T87 / M34
// gate-control: TestInfraHermeticGateRegistrationControls
func TestInfraHermeticGateRegistration(t *testing.T) {
	dir := filepath.Join(repoRoot(t), simTestsDir)
	raw, err := os.ReadFile(filepath.Join(dir, simRunAll))
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	for _, name := range infraHermeticScripts {
		info, err := os.Stat(filepath.Join(dir, name+".sh"))
		present[name] = err == nil && info.Mode().IsRegular()
	}
	if infraHermeticEntryProblem(string(raw), present) {
		t.Fatal("S9 hermetic source/entry missing or duplicated; NOT_RUN pending P32 if hot-update-infra-fixture-test.sh is absent")
	}
}

// The same comparison catches deletion, paired deletion and duplicate entries.
// origin: hot-update S9 plan §12 P37 / T87 / M34
func TestInfraHermeticGateRegistrationControls(t *testing.T) {
	if len(infraHermeticScripts) != 5 {
		t.Fatal("infra control source set must retain all five owned/prerequisite entries")
	}
	good := "for t in " + strings.Join(infraHermeticScripts, " ") + "; do\n"
	present := map[string]bool{}
	for _, name := range infraHermeticScripts {
		present[name] = true
	}
	if infraHermeticEntryProblem(good, present) {
		t.Fatal("positive infra registration rejected")
	}
	// Test the real dispatcher membership even while P32's source is pending.
	// Source existence remains the separate, non-waived registration gate.
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), simTestsDir, simRunAll))
	if err != nil {
		t.Fatal(err)
	}
	if infraHermeticEntryProblem(string(raw), present) {
		t.Fatal("real run-all is missing or duplicating an infra control entry")
	}
	for _, name := range infraHermeticScripts {
		t.Run(name, func(t *testing.T) {
			broken := map[string]bool{}
			for entry, value := range present {
				broken[entry] = value && entry != name
			}
			missing := strings.Replace(good, name, "", 1)
			if !infraHermeticEntryProblem(good, broken) || !infraHermeticEntryProblem(missing, present) ||
				!infraHermeticEntryProblem(missing, broken) {
				t.Fatal("missing gate or source/gate pair accepted")
			}
			if !infraHermeticEntryProblem(strings.Replace(good, name, name+" "+name, 1), present) {
				t.Fatal("duplicate gate accepted")
			}
		})
	}
}

// T87 consumes P36's actual ledger through the registered script. Its control
// archives do not replace the candidate/source/native receipts of check().
// origin: hot-update S9 plan §9.2 / §12 P37 / T87
// gate-control: TestInfraLedgerRequiresNativeEvidenceControls
func TestInfraLedgerRequiresNativeEvidence(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "sh", filepath.Join(repoRoot(t), simTestsDir, "hot-update-infra-ledger-test.sh"), "all")
	output, err := testharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("infra ledger native-evidence check: %v\n%s", err, output)
	}
}

// origin: hot-update S9 plan §12 P37 / T87 / M34
func TestInfraLedgerRequiresNativeEvidenceControls(t *testing.T) {
	cmd := exec.CommandContext(t.Context(), "sh", filepath.Join(repoRoot(t), simTestsDir, "hot-update-infra-ledger-test.sh"), "controls")
	output, err := testharness.CombinedOutput(cmd)
	if err != nil {
		t.Fatalf("infra ledger native-evidence controls: %v\n%s", err, output)
	}
	if !strings.Contains(string(output), "infra ledger controls: PASS") {
		t.Fatalf("infra ledger controls did not exercise archives:\n%s", output)
	}
}

// Extract only the dispatcher function. Executing run-all would also run the
// full gate set; these fixtures prove interpreter and supervision semantics.
// origin: hot-update S9 plan §9.5 / §12 P37 / T88 / M34
func TestSimclusterGateSetHonorsShebangUnderSupervisor(t *testing.T) {
	root := repoRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, simTestsDir, simRunAll))
	if err != nil {
		t.Fatal(err)
	}
	dispatch := regexp.MustCompile(`(?ms)^run_script\(\) \{\n.*?^}`).FindString(string(raw))
	if dispatch == "" {
		t.Fatal("real shebang dispatcher is missing")
	}
	dir := t.TempDir()
	supervisor := filepath.Join(dir, "supervise")
	build := exec.CommandContext(t.Context(), "go", "build", "-o", supervisor, "./test/fullstackharness/cmd/supervise")
	build.Dir = root
	if output, err := testharness.CombinedOutput(build); err != nil {
		t.Fatalf("build shared supervisor: %v\n%s", err, output)
	}
	for _, fixture := range []struct {
		name, source string
		want         string
		refuse       bool
	}{
		{"posix", "#!/bin/sh\nprintf '%s\\n' \"posix:$1\"\n", "posix:argument", false},
		{"bash", "#!/usr/bin/env bash\n[[ $1 == argument ]] || exit 99\nprintf 'bash\\n'\n", "bash", false},
		{"python", "#!/usr/bin/env python3\nimport sys\nassert sys.flags.isolated and sys.flags.dont_write_bytecode\nprint('python:'+sys.argv[1])\n", "python:argument", false},
		{"custody", "#!/usr/bin/env python3\nimport os\nassert os.readlink('/proc/%d/exe' % os.getppid()) == os.environ['P37_EXPECTED_SUPERVISOR']\nprint('custody')\n", "custody", false},
		{"unknown", "#!/usr/bin/env false\n", "", true},
		{"missing", "printf 'must never execute\\n'\n", "", true},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			path := filepath.Join(dir, fixture.name+".sh")
			if err := os.WriteFile(path, []byte(fixture.source), 0o600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(t.Context(), "sh", "-c", dispatch+"\nrun_script \"$1\" argument\n", "dispatch-control", path)
			cmd.Env = append(os.Environ(), "SIM_UPGRADE_SUPERVISOR="+supervisor, "P37_EXPECTED_SUPERVISOR="+supervisor)
			output, err := testharness.CombinedOutput(cmd)
			if fixture.refuse {
				if err == nil || !strings.Contains(string(output), "unsupported or missing shebang") {
					t.Fatalf("unknown interpreter accepted: %v\n%s", err, output)
				}
			} else if err != nil || strings.TrimSpace(string(output)) != fixture.want {
				t.Fatalf("shebang dispatch: %v\n%s", err, output)
			}
		})
	}
	cmd := exec.CommandContext(t.Context(), "sh", "-c", dispatch+"\nrun_script \"$1\" argument\n", "dispatch-control", filepath.Join(dir, "posix.sh"))
	cmd.Env = append(os.Environ(), "SIM_UPGRADE_SUPERVISOR="+filepath.Join(dir, "absent"))
	output, err := testharness.CombinedOutput(cmd)
	if err == nil || !strings.Contains(string(output), "shared supervisor is required") {
		t.Fatalf("missing supervisor accepted: %v\n%s", err, output)
	}
}

// S10 hermetic registration, the static half of plan §12.3. run-all invokes each
// fullstack wrapper exactly once and keeps the installer lint entry; the
// fullstack wrappers on disk equal the registered set both ways; every
// discovered S10 Python selftest is declared by a required-test row of P32's
// runner and no row names a dead file. P32 executes and reconciles receipts; a
// selftest dropped from both the runner and its declaration stays red here.
// origin: hot-update S10 plan §12.3 / §10.2 P32 / P28
// gate-control: TestFullstackHermeticGateRegistrationControls
var fullstackHermeticScripts = []string{"hot-update-fullstack-test", "hot-update-fullstack-logs-test", "lint-install"}

var fullstackRequiredTestRe = regexp.MustCompile(`(?m)^# required-test (\S+) \S+$`)

func checkFullstackHermetic(tree fullstackTree) (int, error) {
	runAll := tree.files[simTestsDir+"/"+simRunAll]
	if runAll == "" {
		return 0, fmt.Errorf("empty hermetic root %s/%s", simTestsDir, simRunAll)
	}
	loop, explicit := runAllNames(runAll)
	for _, name := range fullstackHermeticScripts {
		if n := len(slices.DeleteFunc(append(slices.Clone(loop), explicit...), func(entry string) bool { return entry != name })); n != 1 {
			return 0, fmt.Errorf("run-all must invoke %s exactly once, found %d", name, n)
		}
		if tree.files[simTestsDir+"/"+name+".sh"] == "" {
			return 0, fmt.Errorf("missing S10 hermetic source %s.sh", name)
		}
	}
	declared, discovered := map[string]bool{}, 0
	for path := range tree.files {
		dir, base := fullstackDir(path), fullstackBase(path)
		if dir == simTestsDir && strings.HasSuffix(base, ".sh") && strings.Contains(base, "fullstack") &&
			!slices.Contains(fullstackHermeticScripts, strings.TrimSuffix(base, ".sh")) {
			return 0, fmt.Errorf("unregistered S10 hermetic wrapper %s", path)
		}
	}
	for _, m := range fullstackRequiredTestRe.FindAllStringSubmatch(tree.files[simTestsDir+"/hot-update-fullstack-test.sh"], -1) {
		if tree.files["test/simcluster/"+m[1]] == "" {
			return 0, fmt.Errorf("dead required-test row names %s", m[1])
		}
		declared[m[1]] = true
	}
	// The same discovery rule as P32's runner (discovered_selftests).
	for _, path := range brokerSortedSourcePaths(tree.files) {
		rel := strings.TrimPrefix(path, "test/simcluster/")
		base := fullstackBase(rel)
		if dir := fullstackDir(rel); dir != "drills/lib" && dir != "lib" && dir != "tests" || rel == path {
			continue
		}
		if strings.HasPrefix(base, "fullstack_") && strings.HasSuffix(base, "_test.py") ||
			strings.HasPrefix(base, "fullstack-") && strings.HasSuffix(base, "-test.py") || base == "hot-update-fullstack-test.py" {
			discovered++
			if !declared[rel] {
				return 0, fmt.Errorf("undeclared S10 Python selftest %s", rel)
			}
		}
	}
	if discovered == 0 {
		return 0, fmt.Errorf("empty S10 Python selftest root")
	}
	return discovered, nil
}

// origin: hot-update S10 plan §12.3 / P28
func TestFullstackHermeticGateRegistration(t *testing.T) {
	discovered, err := checkFullstackHermetic(fullstackRepoTree(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("S10 Python selftests declared and discovered: %d", discovered)
}

// Every mutation edits a copied real run-all, wrapper or selftest on disk.
// origin: hot-update S10 plan §10.2 P28 / §12.3
func TestFullstackHermeticGateRegistrationControls(t *testing.T) {
	if !slices.Equal(fullstackHermeticScripts, []string{"hot-update-fullstack-test", "hot-update-fullstack-logs-test", "lint-install"}) {
		t.Fatal("S10 hermetic entry set drifted")
	}
	real := fullstackRepoTree(t)
	root := fullstackFixture(t, real, simTestsDir, "test/simcluster/drills/lib", "test/simcluster/lib")
	check := func() error {
		_, err := checkFullstackHermetic(readFullstackTree(t, root))
		return err
	}
	const runAll, runner = simTestsDir + "/" + simRunAll, simTestsDir + "/hot-update-fullstack-test.sh"
	proveFullstackMutations(t, root, check,
		fullstackMutation{path: runAll, old: " hot-update-fullstack-logs-test ", new: " ", want: "hot-update-fullstack-logs-test exactly once, found 0"},
		fullstackMutation{path: runAll, old: " hot-update-fullstack-test ", new: " hot-update-fullstack-test hot-update-fullstack-test ", want: "hot-update-fullstack-test exactly once, found 2"},
		fullstackMutation{path: runAll, old: " lint-install lint-install-selftest ", new: " lint-install-selftest ", want: "lint-install exactly once, found 0"},
		fullstackMutation{path: simTestsDir + "/hot-update-fullstack-logs-test.sh", remove: true, want: "missing S10 hermetic source"},
		fullstackMutation{path: simTestsDir + "/hot-update-fullstack-extra-test.sh", new: "#!/bin/sh\nexit 0\n", want: "unregistered S10 hermetic wrapper"},
		fullstackMutation{path: "test/simcluster/drills/lib/fullstack_extra_test.py", new: "#!/usr/bin/env python3\n", want: "undeclared S10 Python selftest drills/lib/fullstack_extra_test.py"},
		fullstackMutation{path: "test/simcluster/lib/fullstack-extra-test.py", new: "#!/usr/bin/env python3\n", want: "undeclared S10 Python selftest lib/fullstack-extra-test.py"},
		fullstackMutation{path: runner, new: "# required-test drills/lib/fullstack_missing_test.py MissingTest.test_gone\n", want: "dead required-test row"},
		fullstackMutation{path: "test/simcluster/drills/lib/fullstack_observation_test.py", remove: true, want: "dead required-test row names drills/lib/fullstack_observation_test.py"},
	)
}
