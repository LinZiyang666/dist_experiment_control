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
		case jobKeyRe.MatchString(line):
			flush()
			cur = jobKeyRe.FindStringSubmatch(line)[1]
		case cur != "":
			body = append(body, line)
		}
	}
	flush()
	if len(jobs) == 0 {
		t.Fatalf("parsed 0 jobs from .github/workflows/ci.yml; the scanner is broken")
	}
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
}

func namedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
