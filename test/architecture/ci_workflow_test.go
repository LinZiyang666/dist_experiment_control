package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// ci_workflow_test.go — assertions about .github/workflows/ci.yml itself.
//
// These live here rather than beside the gates they protect because they are all about the same
// artifact and share one parser. Both failure modes they cover are SILENT: a CI job that cannot
// run, and a CI job that runs but is missing a precondition, look identical to a green board.

func readCIWorkflow(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), ".github", "workflows", "ci.yml"))
	if err != nil {
		t.Fatalf("read CI workflow: %v", err)
	}
	return string(b)
}

// ciJobBlocks splits a GitHub Actions workflow into {job name: body}. Deliberately a line scanner
// rather than a YAML dependency: the shape needed is one level of keys under `jobs:`, and the test
// tree carries no YAML parser (the sibling nolint gate makes the same trade for the same reason).
func ciJobBlocks(t *testing.T, workflow string) map[string]string {
	t.Helper()
	jobs := splitJobBlocks(workflow)
	if len(jobs) == 0 {
		t.Fatalf("parsed 0 jobs from .github/workflows/ci.yml; the scanner is broken")
	}
	return jobs
}

// splitJobBlocks is the pure scanner behind ciJobBlocks, shared with releaseChainProblems so the
// gate-control can feed both synthetic workflows.
func splitJobBlocks(workflow string) map[string]string {
	lines := strings.Split(workflow, "\n")
	inJobs := false
	jobs := map[string]string{}
	cur := ""
	var body []string
	flush := func() {
		if cur != "" {
			jobs[cur] = strings.Join(body, "\n")
		}
		cur, body = "", nil
	}
	jobKeyRe := regexp.MustCompile(`^  ([A-Za-z0-9_.-]+):\s*$`)
	for _, line := range lines {
		switch {
		case line == "jobs:":
			inJobs = true
		case !inJobs:
			// still in the top-level preamble
		case strings.HasPrefix(strings.TrimSpace(line), "#"):
			// Comments are not part of a job. Every assertion in this file is a substring test over a
			// body, and the first version kept comment lines in the body — so a job's explanatory
			// comment (which sits ABOVE its key and therefore lands in the PREVIOUS job's body) could
			// satisfy or trip an assertion about a job it does not belong to: the release job's
			// comment mentioning the tag ref reddened the fuzz job's "never on a tag" check, and a
			// comment saying `# fires on refs/tags/v` would have satisfied the release job's tag test
			// with the real `if:` deleted (internal review, product-ci verifier M1).
		case jobKeyRe.MatchString(line):
			flush()
			cur = jobKeyRe.FindStringSubmatch(line)[1]
		case cur != "":
			body = append(body, line)
		}
	}
	flush()
	return jobs
}

func jobNames(jobs map[string]string) []string {
	out := make([]string, 0, len(jobs))
	for n := range jobs {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// TestCIProvidesHistoryForDeletedRegressionProvenance keeps the frozen-history
// gate runnable in GitHub Actions. actions/checkout defaults to fetch-depth=1;
// the frozen ancestor is then absent even though it is part of the repository's
// history, so the job fails four git-show calls before checking a single assertion.
//
// KEYED ON WHAT THE JOB RUNS, NOT ON ITS NAME. The first version asserted that the job
// literally named `build-test` carried fetch-depth: 0. That is the right requirement
// attached to the wrong subject — moving `make test` into a differently-named job, or
// adding a second job that runs it, leaves this gate green and CI deterministically red.
// The requirement belongs to "runs the suite that contains the provenance gate", so that
// is what is parsed.
func TestCIProvidesHistoryForDeletedRegressionProvenance(t *testing.T) {
	jobs := ciJobBlocks(t, readCIWorkflow(t))

	// Only `make test` / a bare `go test` over the whole tree reaches test/architecture. `make lint`
	// and `make e2e-parallel` do not (the parallel runner enumerates the phase suites explicitly), so
	// requiring deep history of them would be cargo-culting the fix onto jobs that cannot need it.
	needsHistory := regexp.MustCompile(`(?m)^\s*-?\s*(run:\s*)?(make test|make gates|go test \./\.\.\.)\s*$`)

	var checked int
	for name, body := range jobs {
		if !needsHistory.MatchString(body) {
			continue
		}
		checked++
		if !strings.Contains(body, "fetch-depth: 0") {
			t.Errorf("CI job %q runs the full test suite but checks out shallow.\n\n"+
				"TestDeletedRegressionTestNamesAreReal reads frozen ancestor %s via `git show`. "+
				"actions/checkout defaults to depth 1, where that commit is unreachable and the job "+
				"fails deterministically. Add:\n"+
				"    - uses: actions/checkout@v4\n"+
				"      with:\n"+
				"        fetch-depth: 0",
				name, deletedRegressionTestsCommit)
		}
	}
	if checked == 0 {
		t.Fatalf("no CI job was found to run the full test suite (parsed %d job(s): %v).\n\n"+
			"Either the workflow stopped running `make test` — in which case the provenance gate no "+
			"longer runs in CI at all and this check is the only thing that would say so — or the "+
			"parser broke and every future shallow checkout would pass unexamined.",
			len(jobs), jobNames(jobs))
	}
}

// TestCIE2EJobIsStillReachable pins that the e2e matrix can actually fire.
//
// origin: the e2e job's trigger was narrowed from "schedule OR every push to main" to
// "schedule OR manual OR version tag", because the same matrix already runs locally before every
// commit (CLAUDE.md §5) and the CI copy costs 42m22s on a 2-core runner — ~1300 min/month against a
// 2000 min/month allowance.
//
// Narrowing a trigger is one edit away from disabling it. A job whose `if:` names an event the
// workflow does not declare — the shape you get by deleting `schedule:` from `on:` and forgetting
// the `if:`, or by renaming an event — never runs, reports nothing, and leaves a permanently green
// board. This repo has been bitten by exactly this class twice (a splitter that dropped
// TestAllPhases, a runner that reported a shorter round as a pass), and both times the tell was
// that success and absence look the same.
//
// So: every event the e2e job's condition names must be declared in `on:`, and at least one
// UNATTENDED trigger must survive. Manual dispatch alone does not count — a gate that only runs
// when someone remembers is not a gate.
func TestCIE2EJobIsStillReachable(t *testing.T) {
	workflow := readCIWorkflow(t)
	jobs := ciJobBlocks(t, workflow)

	body, ok := jobs["e2e"]
	if !ok {
		t.Fatalf("there is no `e2e` job in ci.yml (jobs: %v).\n\n"+
			"If the full matrix genuinely no longer runs in CI, that is a decision to record in "+
			"CLAUDE.md — but it must not happen by a job quietly disappearing.", jobNames(jobs))
	}

	// The `on:` block is everything before `jobs:`.
	onBlock := workflow
	if i := strings.Index(workflow, "\njobs:"); i > 0 {
		onBlock = workflow[:i]
	}
	if onBlock == workflow {
		t.Fatalf("cannot locate the `jobs:` key in ci.yml; the parser is broken and every assertion " +
			"below would be checking the whole file instead of the trigger block")
	}

	// Events the condition names -> the literal that must appear in `on:` for it to be declared.
	// refs/tags is not an event name: `on: push` fires for tag pushes, so a tag arm needs `push`.
	declaredBy := map[string]string{
		"schedule":          "schedule:",
		"workflow_dispatch": "workflow_dispatch:",
		"push":              "push:",
		"pull_request":      "pull_request:",
	}
	// Unattended = fires without a human. A tag push is a human act, and workflow_dispatch is the
	// definition of one.
	unattended := map[string]bool{"schedule": true, "push": true, "pull_request": true}

	named := map[string]bool{}
	for ev := range declaredBy {
		if strings.Contains(body, "'"+ev+"'") || strings.Contains(body, `"`+ev+`"`) {
			named[ev] = true
		}
	}
	if strings.Contains(body, "refs/tags/") {
		named["push"] = true // the tag arm rides on `on: push`
	}
	if len(named) == 0 {
		t.Fatalf("the e2e job's `if:` names no recognised event:\n%s\n\n"+
			"Either it now runs unconditionally (say so by deleting the `if:`), or the condition was "+
			"written in a shape this check cannot read — in which case extend the check rather than "+
			"leaving the job's reachability unverified.", body)
	}

	for ev := range named {
		if !strings.Contains(onBlock, declaredBy[ev]) {
			t.Errorf("the e2e job's condition names %q, but `on:` does not declare %s.\n\n"+
				"The job can never fire on that event. This is the silent shape: no failure, no run, "+
				"a green board, and the full matrix has not executed in CI since the edit.",
				ev, declaredBy[ev])
		}
	}

	var auto []string
	for ev := range named {
		if unattended[ev] && strings.Contains(onBlock, declaredBy[ev]) {
			auto = append(auto, ev)
		}
	}
	if len(auto) == 0 {
		t.Errorf("the e2e job has no UNATTENDED trigger left (it names %v).\n\n"+
			"workflow_dispatch and tag pushes both require someone to decide to run it. The CI copy "+
			"of the matrix exists to catch environment drift — a runner image update, a Go patch "+
			"release — which nobody is watching for and therefore nobody will trigger.",
			namedKeys(named))
	}

	// Non-vacuity: the schedule must be a real cron, not an empty key. `schedule:` with nothing
	// under it is valid YAML and fires never.
	if strings.Contains(onBlock, "schedule:") && !regexp.MustCompile(`cron:\s*["']`).MatchString(onBlock) {
		t.Errorf("`on: schedule:` is declared with no cron expression under it — it fires never.\n\n%s", onBlock)
	}
	// And the job must actually run the matrix. Absorbed from test/p11/ci_test.go (its single
	// P11-review test) on 2026-09-01: that package held one function asserting the same artifact
	// this file parses, with its own copy of the workflow read — the layering-rules shape (one
	// invariant, two files, two readers). The package was deleted with it; the inventory golden was
	// hand-edited to drop its one line. origin: docs/reviews/test-system-overhaul-plan.md B4 (§-1 F10).
	if !strings.Contains(body, "make e2e-parallel") {
		t.Errorf("the e2e job does not run `make e2e-parallel` — P11 requires the P2–P10 matrix in CI:\n%s", body)
	}
}

// TestCIFuzzJobIsUnattendedButNeverPerCommit pins the fuzz job's trigger shape from both sides.
//
// It must be reachable without a human (schedule, with a real cron — the same reasoning as the e2e
// job), and it must NOT fire on push or pull_request: `make fuzz` is non-deterministic by design,
// so a red it produces cannot be reproduced by re-running the same commit, and a per-commit gate
// that can go red for no reproducible reason is a gate people learn to re-run until green.
// origin: docs/reviews/test-system-overhaul-plan.md B2 (infra I8).
func TestCIFuzzJobIsUnattendedButNeverPerCommit(t *testing.T) {
	workflow := readCIWorkflow(t)
	jobs := ciJobBlocks(t, workflow)
	body, ok := jobs["fuzz"]
	if !ok {
		t.Fatalf("there is no `fuzz` job in ci.yml (jobs: %v); 23+ Fuzz targets went unrun for months "+
			"because nothing scheduled them — see docs/reviews/test-system-overhaul-plan.md B2", jobNames(jobs))
	}
	onBlock := workflow[:strings.Index(workflow, "\njobs:")]
	if !strings.Contains(body, "'schedule'") || !strings.Contains(onBlock, "schedule:") {
		t.Errorf("the fuzz job must name 'schedule' in its `if:` and `on:` must declare schedule:\n%s", body)
	}
	if !regexp.MustCompile(`cron:\s*["']`).MatchString(onBlock) {
		t.Errorf("`on: schedule:` has no cron expression — the fuzz job fires never")
	}
	for _, forbidden := range []string{"'push'", "'pull_request'", "refs/tags/"} {
		if strings.Contains(body, forbidden) {
			t.Errorf("the fuzz job names %s in its condition — fuzzing must never be a per-commit or per-tag gate", forbidden)
		}
	}
	if !strings.Contains(body, "if:") {
		t.Errorf("the fuzz job has no `if:` at all, so it runs on every push and pull_request")
	}
	if !strings.Contains(body, "make fuzz") {
		t.Errorf("the fuzz job does not run `make fuzz`")
	}
}

// TestReleaseJobNeedsEveryGate pins that publishing a release waits for every gate.
//
// origin: docs/reviews/test-system-overhaul-plan.md B1 (infra I4). release.yml used to fire goreleaser
// on the tag push with no `needs` at all, in a workflow that could not see ci.yml's e2e job — while the
// e2e job's own comment called the tag "the single moment where 'it passed on the maintainer's box'
// is not good enough". A sentence in a comment is not a dependency. Now `release` is a job in ci.yml
// and this test reads its `needs:` list: build-test, lint AND e2e, every time, or the artifact must not
// leave the building.
//
// The three gate jobs are looked up by name so that renaming one (or deleting it) reddens here rather
// than leaving a `needs` entry that GitHub treats as "unknown job" — which skips the dependent job
// silently, the exact shape this file exists to stop.
func TestReleaseJobNeedsEveryGate(t *testing.T) {
	jobs := ciJobBlocks(t, readCIWorkflow(t))
	body, ok := jobs["release"]
	if !ok {
		t.Fatalf("ci.yml has no `release` job (jobs: %v).\n\n"+
			"Publishing must be a job in THIS workflow so it can `needs` the gates; a separate "+
			"workflow cannot depend on them. See TestNoWorkflowPublishesOutsideCI.", jobNames(jobs))
	}
	gates := []string{"build-test", "lint", "e2e"}
	for _, g := range gates {
		if _, exists := jobs[g]; !exists {
			t.Fatalf("gate job %q no longer exists in ci.yml; the release job's `needs` would name an "+
				"unknown job, which GitHub resolves by never running the dependent job", g)
		}
	}
	needsRe := regexp.MustCompile(`(?m)^\s*needs:\s*\[([^\]]*)\]\s*$`)
	m := needsRe.FindStringSubmatch(body)
	if m == nil {
		t.Fatalf("the release job has no `needs: [...]` line:\n%s", body)
	}
	have := map[string]bool{}
	for _, n := range strings.Split(m[1], ",") {
		have[strings.TrimSpace(n)] = true
	}
	for _, g := range gates {
		if !have[g] {
			t.Errorf("the release job does not `needs: %s` — a red %s on the tag would still publish", g, g)
		}
	}
	if !strings.Contains(body, "refs/tags/v") {
		t.Errorf("the release job must be conditioned on a version tag (`startsWith(github.ref, 'refs/tags/v')`); found:\n%s", body)
	}
	// The EVENT too, not only the ref: a workflow_dispatch selected against an existing tag satisfies
	// the ref test alone and would run goreleaser against a published release (internal review L3-F6).
	if !strings.Contains(body, "github.event_name == 'push'") {
		t.Errorf("the release job must be conditioned on `github.event_name == 'push'` as well as the tag ref — a manual dispatch on a tag must not publish; found:\n%s", body)
	}
	if !strings.Contains(body, "goreleaser") {
		t.Errorf("the release job does not run goreleaser — what does it publish?")
	}
	if !regexp.MustCompile(`(?m)^\s+contents:\s*write\s*$`).MatchString(body) {
		t.Errorf("the release job needs job-level `permissions: contents: write` (the workflow default is read-only)")
	}
}

// releaseChainProblems reads the WHOLE chain from `on:` to goreleaser and returns every way a tag
// push could fail to publish, or publish without the gates, that TestReleaseJobNeedsEveryGate
// cannot see from the release job's body alone:
//
//   - a job the release job `needs` whose own `if:` has no tag arm: GitHub skips a job whose
//     condition is false, and then skips every job that needs it — the tag never publishes, and
//     every assertion about the release job stays green (internal review L3-F1);
//   - an `on: push:` ref/path filter: with only `branches:` or `paths:` set, GitHub does not start
//     the workflow for a tag push at all (L3-F1);
//   - `always()` / `!cancelled()` in a condition, or `continue-on-error`, on the release job or any
//     gate it needs: a failed gate would still let the tag publish (L1-F4).
//
// Pure, so the gate-control below can feed it synthetic workflows.
func releaseChainProblems(workflow string) []string {
	var problems []string
	jobs := splitJobBlocks(workflow)
	release, ok := jobs["release"]
	if !ok {
		return []string{"no release job"}
	}
	m := regexp.MustCompile(`(?m)^\s*needs:\s*\[([^\]]*)\]\s*$`).FindStringSubmatch(release)
	if m == nil {
		return []string{"release has no inline needs list"}
	}
	ifRe := regexp.MustCompile(`(?m)^\s*if:`)
	chain := []string{"release"}
	for _, n := range strings.Split(m[1], ",") {
		n = strings.TrimSpace(n)
		body, exists := jobs[n]
		if !exists {
			problems = append(problems, "release needs unknown job "+n)
			continue
		}
		chain = append(chain, n)
		if ifRe.MatchString(body) && !strings.Contains(body, "refs/tags/v") {
			problems = append(problems, "prerequisite job "+n+" has an `if:` with no refs/tags/v arm: skipped on a tag push, and GitHub skips every job that needs a skipped job")
		}
	}
	// Every status function that OVERRIDES the default `success()` condition lets a job run after a
	// failed `needs`: always(), failure() and cancelled() (the last covers `!cancelled()` too). The
	// first version listed always() and !cancelled() only, and its own control could not see
	// `failure() && startsWith(…)` (external review F1). success() is the default and is not listed.
	for _, job := range chain {
		for _, tok := range []string{"always()", "failure()", "cancelled()", "continue-on-error"} {
			if strings.Contains(jobs[job], tok) {
				problems = append(problems, "job "+job+" contains "+tok+" — a failed gate would still let the tag publish")
			}
		}
	}
	i := strings.Index(workflow, "\njobs:")
	if i < 0 {
		return append(problems, "no jobs: key")
	}
	onBlock := workflow[:i]
	pm := regexp.MustCompile(`(?m)^  push:\s*$((?:\n    .*)*)`).FindStringSubmatch(onBlock)
	if pm == nil {
		return append(problems, "`on:` has no bare `push:` key; tag pushes only start the workflow through on.push")
	}
	filter := pm[1]
	if regexp.MustCompile(`(?m)^\s*(branches|branches-ignore|paths|paths-ignore|tags-ignore):`).MatchString(filter) {
		problems = append(problems, "`on: push:` carries a ref/path filter; with only branches:/paths: set GitHub does not run the workflow for tag pushes")
	}
	if regexp.MustCompile(`(?m)^\s*tags:`).MatchString(filter) && !strings.Contains(filter, "v*") {
		problems = append(problems, "`on: push: tags:` does not admit v* tags")
	}
	return problems
}

// TestReleaseChainIsReachableOnATagPush closes the silent shapes TestReleaseJobNeedsEveryGate cannot
// see: a gate the release job `needs` that is itself skipped on a tag push, an on.push filter that
// stops tag pushes from starting the workflow at all, and a short-circuit token that lets a red gate
// publish. Any of them leaves every other assertion in this file green while no tag ever publishes —
// or every tag does.
// origin: test-system-overhaul internal review L3-F1 / L1-F4
func TestReleaseChainIsReachableOnATagPush(t *testing.T) {
	for _, p := range releaseChainProblems(readCIWorkflow(t)) {
		t.Error(p)
	}
}

// TestReleaseChainPredicateSeesEveryBreak is the gate-control for releaseChainProblems: one good
// workflow and one mutation per problem class.
func TestReleaseChainPredicateSeesEveryBreak(t *testing.T) {
	good := "name: x\non:\n  push:\n  schedule:\n    - cron: \"0 0 * * 1\"\njobs:\n" +
		"  build-test:\n    steps:\n      - run: make test\n" +
		"  e2e:\n    if: github.event_name == 'schedule' || startsWith(github.ref, 'refs/tags/v')\n    steps:\n      - run: make e2e-parallel\n" +
		"  release:\n    if: github.event_name == 'push' && startsWith(github.ref, 'refs/tags/v')\n    needs: [build-test, e2e]\n    steps:\n      - run: goreleaser\n"
	if p := releaseChainProblems(good); len(p) != 0 {
		t.Fatalf("control workflow reported %v", p)
	}
	mutations := map[string]string{
		"e2e loses its tag arm":           strings.Replace(good, " || startsWith(github.ref, 'refs/tags/v')", "", 1),
		"on.push gains a branches filter": strings.Replace(good, "  push:\n", "  push:\n    branches: [main]\n", 1),
		"on.push tags: without v*":        strings.Replace(good, "  push:\n", "  push:\n    tags: ['release-*']\n", 1),
		"release if: always()":            strings.Replace(good, "if: github.event_name == 'push' &&", "if: always() &&", 1),
		// origin: docs/reviews/test-system-overhaul-external-review.md F1
		"release if: failure()":            strings.Replace(good, "if: github.event_name == 'push' &&", "if: failure() &&", 1),
		"release if: cancelled()":          strings.Replace(good, "if: github.event_name == 'push' &&", "if: cancelled() &&", 1),
		"release if: !cancelled()":         strings.Replace(good, "if: github.event_name == 'push' &&", "if: !cancelled() &&", 1),
		"gate if: always()":                strings.Replace(good, "if: github.event_name == 'schedule' ||", "if: always() || github.event_name == 'schedule' ||", 1),
		"gate step continue-on-error":      strings.Replace(good, "      - run: make e2e-parallel\n", "      - run: make e2e-parallel\n        continue-on-error: true\n", 1),
		"release needs a job that is gone": strings.Replace(good, "needs: [build-test, e2e]", "needs: [build-test, lint, e2e]", 1),
	}
	for name, wf := range mutations {
		if p := releaseChainProblems(wf); len(p) == 0 {
			t.Errorf("mutation %q was not reported", name)
		}
	}
}

// TestLiveDocsReferenceOnlyExistingWorkflowFiles: the usage manual pointed at release.yml for a whole
// batch after that file was deleted (internal review L3-F2 / L5-F4 / L6-F4). A live document that
// names a workflow file names one that exists.
func TestLiveDocsReferenceOnlyExistingWorkflowFiles(t *testing.T) {
	root := repoRoot(t)
	re := regexp.MustCompile(`\.github/workflows/([A-Za-z0-9_.-]+\.ya?ml)`)
	var bad []string
	checked := 0
	for _, doc := range []string{"CLAUDE.md", "README.md", "docs/usage.md", "docs/broker-ops.md", "docs/deploy-tier-gotchas.md",
		"docs/distributed-broker-architecture.md", "docs/testing-standards.md", "test/README.md"} {
		b, err := os.ReadFile(filepath.Join(root, doc))
		if err != nil {
			continue
		}
		for _, m := range re.FindAllStringSubmatch(string(b), -1) {
			checked++
			if _, err := os.Stat(filepath.Join(root, ".github", "workflows", m[1])); err != nil {
				bad = append(bad, doc+" -> "+m[0])
			}
		}
	}
	if checked == 0 {
		t.Fatal("no live document references any workflow file — the scan surface moved")
	}
	if len(bad) > 0 {
		t.Errorf("live docs reference workflow files that do not exist:\n  %s", strings.Join(bad, "\n  "))
	}
}

// TestNoWorkflowPublishesOutsideCI is the other half: a release job that waits for the gates is only
// a guarantee if it is the ONLY way an artifact gets published. Any other workflow file that triggers
// on a tag push, or that runs goreleaser, is a bypass — and release.yml was exactly that until it was
// folded into ci.yml.
func TestNoWorkflowPublishesOutsideCI(t *testing.T) {
	dir := filepath.Join(repoRoot(t), ".github", "workflows")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	tagTrigger := regexp.MustCompile(`(?m)^\s*tags:`)
	var seen []string
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || (!strings.HasSuffix(name, ".yml") && !strings.HasSuffix(name, ".yaml")) {
			continue
		}
		seen = append(seen, name)
		if name == "ci.yml" {
			continue
		}
		b, rerr := os.ReadFile(filepath.Join(dir, name))
		if rerr != nil {
			t.Fatalf("read %s: %v", name, rerr)
		}
		text := string(b)
		// goreleaser is how THIS repo publishes; the other names are how a second workflow could
		// publish without it (a grep gate's word list is an approximation, and says so —
		// internal review L1-F12).
		for _, publisher := range []string{"goreleaser", "gh release", "action-gh-release", "upload-release-asset", "/releases"} {
			if strings.Contains(text, publisher) {
				t.Errorf(".github/workflows/%s contains %q outside ci.yml — it publishes without waiting for the gates", name, publisher)
			}
		}
		onBlock := text
		if i := strings.Index(text, "\njobs:"); i > 0 {
			onBlock = text[:i]
		}
		if tagTrigger.MatchString(onBlock) {
			t.Errorf(".github/workflows/%s triggers on tag pushes outside ci.yml — a tag must reach the "+
				"release job through the gates, not through a second workflow", name)
		}
	}
	// Non-vacuity (G2): the directory must contain at least ci.yml, or the scan looked at nothing.
	found := false
	for _, n := range seen {
		if n == "ci.yml" {
			found = true
		}
	}
	if !found {
		t.Fatalf("scanned %v under .github/workflows but did not see ci.yml — wrong directory?", seen)
	}
}

// TestCIJobBlockParserSeesTheShapes is the G2 control for ciJobBlocks, the one parser every
// assertion in this file rides on: a synthetic workflow with a preamble, three jobs (one with a
// nested `permissions:` block that must NOT be read as a job), and a trailing key.
//
// gate-control: TestCIJobBlockParserSeesTheShapes
func TestCIJobBlockParserSeesTheShapes(t *testing.T) {
	wf := "name: x\non:\n  push:\n  schedule:\n    - cron: \"0 0 * * 1\"\njobs:\n" +
		"  build-test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n" +
		"  # the release job fires on refs/tags/v — a comment ABOVE its key\n" +
		"  release:\n    if: startsWith(github.ref, 'refs/tags/v')\n    needs: [build-test]\n    permissions:\n      contents: write\n    steps:\n      # goreleaser step\n      - run: goreleaser\n" +
		"  e2e:\n    steps:\n      - run: make e2e-parallel\n"
	jobs := ciJobBlocks(t, wf)
	if got := jobNames(jobs); strings.Join(got, ",") != "build-test,e2e,release" {
		t.Fatalf("parsed jobs %v", got)
	}
	if !strings.Contains(jobs["release"], "contents: write") || !strings.Contains(jobs["release"], "needs: [build-test]") {
		t.Fatalf("release body lost its nested keys:\n%s", jobs["release"])
	}
	if strings.Contains(jobs["build-test"], "goreleaser") {
		t.Fatalf("job bodies bled into each other")
	}
	if _, ok := jobs["permissions"]; ok {
		t.Fatalf("a nested `permissions:` block was parsed as a job")
	}
	// Comments are not body: the release comment above its key must not land in build-test, and the
	// step comment must not be in release.
	if strings.Contains(jobs["build-test"], "refs/tags/v") || strings.Contains(jobs["release"], "goreleaser step") {
		t.Fatalf("comment lines leaked into job bodies:\nbuild-test:\n%s\nrelease:\n%s", jobs["build-test"], jobs["release"])
	}
}

func namedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
