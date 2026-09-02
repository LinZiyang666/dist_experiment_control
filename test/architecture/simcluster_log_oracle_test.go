package architecture

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// simcluster_log_oracle_test.go — the simcluster drills must read tether's log
// streams through ONE mapping.
//
// origin: docs/reviews/h1-external-review.md F3.
//
// # WHY THIS IS A GATE AND NOT A CONVENTION
//
// h1 moved two of the four streams: the broker's slog left the unit's
// StandardError redirect for a process-owned rotating file, and the agent's
// slog left journald for $HOME/.tether/agent/<sid>/agent.log. Roughly a dozen
// drills had each inlined its own `journalctl -u tether-agent …` or
// `tail /var/log/tether/broker.err`, so the move turned working oracles into
// SILENT FALSE VERDICTS — drill 94 reported two ASSERTION FAILURES against a
// product that was reconciling correctly.
//
// That is the worst outcome this harness can produce. Its mandate
// (test/simcluster/README.md) is to expose real defects faithfully; an oracle
// that manufactures a defect does the exact opposite, and costs an
// investigation into a product that was fine. Nothing in `go test` or CI could
// have caught it, because the drills are the third tier and run by hand.
//
// A convention would not have held: the mapping had already been duplicated
// once per drill BEFORE h1, which is why one move broke a dozen of them at
// once. So the gate enforces the structural property that makes the next move
// survivable — exactly one file knows where the streams live.
//
// WHAT IT CHECKS
//
//  1. Every drill that calls a sim_*log helper actually sources logs.sh. A
//     missing source is a RUNTIME command-not-found, which `bash -n` cannot
//     see — it was the concrete breakage the F3 remediation itself shipped and
//     had to fix by hand, in three files.
//  2. No drill inlines a stream location outside logs.sh, except the entries in
//     the shrink-only ledger below.
//
// Both checks are pure functions over a drill's text (simCallsHelperWithoutSource,
// simInlinedStreamLines) so the gate-control can feed them synthetic drills — the
// positive/negative table plan B4 asked for and the first landing skipped
// (internal review L5-F9 / L6-F8).
//
// gate-control: TestSimclusterLogOraclePredicatesSeeTheShapes
const (
	simDrillsDir = "test/simcluster/drills"
	simLogsLib   = "test/simcluster/drills/lib/logs.sh"
)

// simInlinedStreamRe matches a raw stream location. Deliberately broad: a
// pattern tuned to today's exact spelling would go blind the moment someone
// writes the same read a slightly different way, which is the failure mode this
// file exists to prevent.
var simInlinedStreamRe = regexp.MustCompile(
	`/var/log/tether/broker\.(err|log)|journalctl -u tether-agent|\.tether/agent/[^\s]*agent\.(log|boot\.err)`)

// simHelperCallRe matches a call to any helper logs.sh publishes.
var simHelperCallRe = regexp.MustCompile(`\bsim_(broker|agent)_(slog|panic)[a-z_]*\b`)

// simInlineLedger is a SHRINK-ONLY account of drills allowed to name a stream
// directly, with the reason each one cannot go through the library. Keys are
// file basenames; the count is the number of inlining lines permitted.
//
// The count being exact is the point: a drill already on this list still goes
// red when it adds a NEW inlined read, so the ledger cannot quietly become a
// blanket exemption for the file. Removing an entry that no longer exists also
// goes red — an exemption nobody can retire is how a gate rots into decoration.
var simInlineLedger = map[string]struct {
	lines  int
	reason string
}{
	// The agent's PANIC stream. logs.sh publishes sim_agent_panic_sink for the
	// boot.err half; this drill additionally reads the JOURNAL half (pre-logger
	// boot output), which is a distinct question no helper answers today.
	"94-agent-reconcile.sh": {1, "reads the agent journal for pre-logger boot output; the slog half goes through logs.sh"},
	// The crash/integrity oracle is deliberately stream-agnostic ("is there a
	// panic ANYWHERE") and reads the journal in addition to both slogs.
	"97-soak-cycles.sh": {1, "orthogonal crash oracle spans all streams by design; the two slog halves go through logs.sh"},
	// A container-local `trap` that runs INSIDE the node during a reset, where
	// the harness's shell functions are not in scope at all.
	"42-rejoin-returning.sh": {1, "in-container reset trap; the sim's shell functions do not exist in that context"},
	// Reads BOTH broker files in a single `grep -h` across nodes for a
	// diagnostic dump; the assertions in these drills go through the library.
	"41-shrink-to-standalone.sh":  {1, "multi-node diagnostic dump; assertions use the library"},
	"43-migrate-live-data.sh":     {1, "diagnostic dump; assertions use the library"},
	"93-metrics-observability.sh": {1, "one JSON-shape probe that must tail both files in a single remote pipeline"},
	"96-mid-flight-chaos.sh":      {2, "committer-attribution artifact needs `grep -ahF` semantics across both files"},
	"50-backup-restore.sh":        {1, "agent-journal diagnostic beside the slog tail, both dumped"},
}

// simCallsHelperWithoutSource is check (1) as a pure predicate: the drill calls a sim_*log helper
// and never sources logs.sh.
func simCallsHelperWithoutSource(body string) bool {
	return simHelperCallRe.MatchString(body) && !strings.Contains(body, `drills/lib/logs.sh"`)
}

// simInlinedStreamLines is check (2)'s counter as a pure function: the number of NON-COMMENT lines
// that name a stream location directly. Comments describe the mapping; they do not depend on it.
// The whole point of the h1 headnotes is to explain WHERE each stream lives, and a gate that forbade
// saying so would delete the documentation it depends on.
func simInlinedStreamLines(body string) int {
	got := 0
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		if simInlinedStreamRe.MatchString(line) {
			got++
		}
	}
	return got
}

// TestSimclusterDrillsSourceLogOracle is check (1): a drill calling a helper it
// never sourced fails at RUNTIME with command-not-found, mid-drill, and reads
// as a product failure.
func TestSimclusterDrillsSourceLogOracle(t *testing.T) {
	root := repoRoot(t)
	for _, path := range simDrillFiles(t, root) {
		if simCallsHelperWithoutSource(readFileString(t, path)) {
			t.Errorf("%s calls a sim_*log helper but never sources drills/lib/logs.sh — "+
				"that is a RUNTIME command-not-found (bash -n cannot see it), and a drill that dies "+
				"mid-oracle reads as a product failure",
				filepath.Base(path))
		}
	}
}

// TestSimclusterLogStreamsHaveOneMapping is check (2): the stream locations
// live in logs.sh, and every exception is booked.
func TestSimclusterLogStreamsHaveOneMapping(t *testing.T) {
	root := repoRoot(t)
	seen := map[string]bool{}

	for _, path := range simDrillFiles(t, root) {
		base := filepath.Base(path)
		got := simInlinedStreamLines(readFileString(t, path))
		entry, booked := simInlineLedger[base]
		if booked {
			seen[base] = true
		}
		switch {
		case got == 0 && booked:
			t.Errorf("%s is in simInlineLedger but no longer inlines any stream location — "+
				"delete its entry. A ledger that keeps dead exemptions stops being shrink-only "+
				"and becomes a permanent waiver (reason on file: %q)", base, entry.reason)
		case got > 0 && !booked:
			t.Errorf("%s names a tether log stream directly (%d line(s)). Use "+
				"drills/lib/logs.sh, or add an entry to simInlineLedger saying why this read "+
				"cannot go through it. h1 F3: a dozen drills each held their own copy of the "+
				"mapping, so ONE stream move silently broke all of them at once and reported a "+
				"healthy product as failing", base, got)
		case got > entry.lines:
			t.Errorf("%s inlines %d stream location(s), ledger allows %d. The count is exact on "+
				"purpose: a booked drill must still go red when it adds a NEW inlined read, or its "+
				"entry becomes a blanket exemption for the whole file (reason on file: %q)",
				base, got, entry.lines, entry.reason)
		case got < entry.lines:
			t.Errorf("%s inlines %d stream location(s) but the ledger still allows %d — lower it. "+
				"Ledgers here only ever shrink", base, got, entry.lines)
		}
	}

	var stale []string
	for base := range simInlineLedger {
		if !seen[base] {
			stale = append(stale, base)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("simInlineLedger names drills that no longer exist: %v — a ledger entry pointing at "+
			"nothing is an exemption nobody can retire", stale)
	}
}

// TestSimclusterLogOracleLibraryIsTheMapping pins the library itself: it must
// know all four streams. A helper set that silently lost one would push callers
// straight back to inlining.
func TestSimclusterLogOracleLibraryIsTheMapping(t *testing.T) {
	body := readFileString(t, filepath.Join(repoRoot(t), simLogsLib))
	for _, want := range []struct{ token, stream string }{
		{"/var/log/tether/broker.log", "broker slog"},
		{"journalctl -u tether-broker", "broker panic/journal"},
		{"agent.log", "agent slog"},
		{"agent.boot.err", "agent panic sink"},
	} {
		if !strings.Contains(body, want.token) {
			t.Errorf("logs.sh no longer knows where the %s stream lives (expected %q). "+
				"Every drill delegates to this file; a stream missing here means the next caller "+
				"inlines it again", want.stream, want.token)
		}
	}
}

func simDrillFiles(t *testing.T, root string) []string {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(root, simDrillsDir, "*.sh"))
	if err != nil {
		t.Fatalf("glob drills: %v", err)
	}
	if len(matches) == 0 {
		t.Fatalf("no drills found under %s — the gate would pass vacuously", simDrillsDir)
	}
	sort.Strings(matches)
	return matches
}

func readFileString(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// TestSimclusterLogOraclePredicatesSeeTheShapes is the gate-control: synthetic drill text through
// the two predicates the gate uses — a drill that inlines a stream, one that reads through the
// library, a comment that merely describes the mapping, and the sourced / unsourced helper call.
func TestSimclusterLogOraclePredicatesSeeTheShapes(t *testing.T) {
	inlined := "#!/bin/bash\n# broker slog lives in /var/log/tether/broker.log (a comment: fine)\n" +
		"ssh node1 tail -n 50 /var/log/tether/broker.log | grep -q reconciled\n" +
		"journalctl -u tether-agent --since -5m | grep -c panic\n"
	if got := simInlinedStreamLines(inlined); got != 2 {
		t.Fatalf("inlined stream lines: got %d, want 2 (the comment must not count)", got)
	}
	viaLibrary := "#!/bin/bash\n. \"$HERE/lib/logs.sh\"\nsim_broker_slog node1 | grep -q reconciled\n"
	if got := simInlinedStreamLines(viaLibrary); got != 0 {
		t.Fatalf("a drill reading through logs.sh must inline nothing, got %d", got)
	}
	unsourced := "#!/bin/bash\nsim_agent_slog node1 | grep -q started\n"
	if !simCallsHelperWithoutSource(unsourced) {
		t.Fatal("a helper call with no `source …drills/lib/logs.sh\"` must be reported")
	}
	sourced := "#!/bin/bash\nsource \"$HERE/drills/lib/logs.sh\"\nsim_agent_slog node1 | grep -q started\n"
	if simCallsHelperWithoutSource(sourced) {
		t.Fatal("a sourced helper call must not be reported")
	}
	if simCallsHelperWithoutSource("#!/bin/bash\necho no helper here\n") {
		t.Fatal("a drill with no helper call has nothing to source")
	}
}
