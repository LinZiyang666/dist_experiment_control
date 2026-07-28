package determinism

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// test_naming_test.go — new test files must be named after the UNIT UNDER TEST, not after the
// development-process event that prompted them (B6).
//
// WHAT WENT WRONG
// ---------------
// 158 of the repo's test files were named after review rounds, phase numbers and audit batches:
// p13_external_review_round6_test.go, g4_external_review_fixes_test.go, b6_skew_test.go,
// codex_allgreen_external_review_test.go. The cost is not aesthetic. Someone changing CloseProxy has no
// way to find the four files that test its fence, so the same invariant gets re-discovered and re-tested
// once per review round — internal/tunnel had FOUR files testing one invariant, one per round, each
// finding the same hole in a different verb. Had round 2 been written as a {verb, killFn} table, rounds 5
// and 6 would structurally not have happened.
//
// WHY A GATE AND NOT JUST A CLEANUP
// ---------------------------------
// The 158 accumulated one file at a time, each individually reasonable at the moment it was written. A
// one-off rename fixes today's list and nothing else; the gate is what stops tomorrow's. This is the
// sequencing S3 asked for ("止血 first, the renames are optional cleanup") — the freeze lands first and
// the renames then drain its allow-list, so neither blocks the other.
//
// THE ALLOW-LIST IS A DRAINING LEDGER, NOT AN EXEMPTION MECHANISM
// --------------------------------------------------------------
// Every entry is a file that predates the rule. Entries are removed as files are renamed, and a stale
// entry (naming a file that no longer exists) FAILS — otherwise the list rots into a permanent
// allow-everything, which is the failure mode of every allow-list that is only ever appended to.

// processNamedPattern matches a test filename that is named after a development-process event.
//
// Published deliberately (rather than being an opaque regex) because the roadmap's own count of these
// files disagrees with four other reports' counts — 141, 134, 123, 155 and 165 all appear across the
// audit — and a number nobody can reproduce is a number nobody can act on. This is the regex behind
// "158", the only one of the six that anybody can re-derive. See legacy_process_named_list.go §"ON THE
// NUMBER 158" for the full reconciliation.
var processNamedPattern = regexp.MustCompile(
	`^(` +
		// review-process events
		`.*external_?rereview.*|.*external_review.*|.*_review_.*|.*_review$|` +
		`.*round[0-9]+.*|.*codex.*|.*allgreen.*|.*megaaudit.*|` +
		// bare phase / batch / audit identifiers used as the WHOLE prefix
		`(r|p|d|b|c|g|s)[0-9]+(_[0-9a-z]+)*|` +
		`(r|p|d|b|c|g|s)[0-9]+g?[0-9]*_.*|` +
		`g[0-9]+g[0-9]+_.*|ops[0-9]+.*` +
		`)$`)

// isProcessNamed reports whether a test file BASENAME (without the _test.go suffix) is process-named.
func isProcessNamed(base string) bool {
	return processNamedPattern.MatchString(base)
}

// TestNoNewProcessNamedTestFiles is the freeze. It walks every *_test.go in the repo and fails on any
// process-named file that is not in the draining ledger.
func TestNoNewProcessNamedTestFiles(t *testing.T) {
	root := repoRootForGuards(t)

	var offenders []string
	seen := map[string]bool{}
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules", "testdata":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		base := strings.TrimSuffix(d.Name(), "_test.go")
		if !isProcessNamed(base) {
			return nil
		}
		seen[rel] = true
		if !legacyProcessNamedTestFiles[rel] {
			offenders = append(offenders, rel)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d test file(s) are named after a development-process event and are not in the legacy "+
			"ledger:\n  %s\n\n"+
			"Name a test file after the UNIT UNDER TEST (tunnel_fence_test.go, not "+
			"p13_external_review_round6_test.go). A reviewer's finding belongs in a `// origin:` line above "+
			"the test function, where it survives a rename and does not hide the test from the next person "+
			"who changes that unit. See CLAUDE.md §3 step 5b.",
			len(offenders), strings.Join(offenders, "\n  "))
	}

	// REVERSE ASSERTION: the ledger must not name files that no longer exist. Without this the list only
	// ever grows and eventually permits everything — and it is also how the renames DRAIN it: each rename
	// makes an entry stale, and this failure is the reminder to delete it in the same commit.
	var stale []string
	for rel := range legacyProcessNamedTestFiles {
		if !seen[rel] {
			stale = append(stale, rel)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d entr(y/ies) in legacyProcessNamedTestFiles no longer name a process-named file that "+
			"exists (renamed, deleted, or the pattern changed):\n  %s\n\n"+
			"Delete them in the same commit as the rename. An allow-list that is only ever appended to "+
			"stops meaning anything.", len(stale), strings.Join(stale, "\n  "))
	}
}

// TestProcessNamePatternRecognisesTheRealShapes is the NON-VACUITY companion, and it runs over
// SYNTHESIZED names rather than over the tree.
//
// This distinction is load-bearing and worth stating: the gate's SUCCESS condition is that no
// unlisted process-named file exists, so any non-vacuity assertion evaluated against the live tree
// would fail exactly when the codebase is clean — and would therefore reward leaving files unrenamed.
// docs/testing-standards.md G2 says to synthesize one sample per supported shape; that is the only form
// of non-vacuity that is compatible with a gate whose job is to empty the thing it counts.
//
// (Contrast raft_timing_guard_test.go's countProductionConstantUses, which legitimately counts the live
// tree — because THAT gate's success state still has the thing it counts in it.)
func TestProcessNamePatternRecognisesTheRealShapes(t *testing.T) {
	mustMatch := []string{
		"p13_external_review_round6",
		"p13_external_review_round2",
		"g4_external_review_fixes",
		"g4_external_rereview",
		"b6_skew",
		"g1g7_audit",
		"r16_g67_g69_external_review",
		"codex_allgreen_external_review",
		"d8_external_review",
		"cluster_operation_external_review",
		"p10_round2_review",
		"ops11",
		"force_single_callsite_round6",
	}
	for _, n := range mustMatch {
		if !isProcessNamed(n) {
			t.Errorf("pattern does NOT match %q, but that is exactly the shape being frozen — the gate "+
				"would let a new one through", n)
		}
	}

	// NEGATIVE CONTROLS: legitimate topic names must not match, or the gate would demand that every
	// correctly-named file be added to the legacy ledger and the rule would be unusable.
	mustNotMatch := []string{
		"tunnel", "proxy_generation_fencing", "home_delivery", "xfer_inflight", "reconcile_registry",
		"operation_ops", "clusterstatus", "preflight", "takeover", "jetstream_guard",
		"assembly_parity", "leakgate", "wire_freeze", "admit", "dbrole",
		// Tricky ones: they contain digits or a letter+digit sequence but are NOT process-named.
		"sha256_helpers", "port_range", "http2_listen",
	}
	for _, n := range mustNotMatch {
		if isProcessNamed(n) {
			t.Errorf("pattern WRONGLY matches %q — a legitimate topic name. A false positive here forces "+
				"correctly-named files into the legacy ledger and destroys the rule's meaning", n)
		}
	}
}

// TestLegacyLedgerCountMatchesThePublishedNumber keeps the documented figure honest. The audit reports
// disagree with each other (141 / 155 / 165 all appear, plus 134-vs-123 inside a single lane report), and
// none of them publishes its regex, so the number this repo acts on is the one THIS gate can reproduce
// from the pattern above. See legacy_process_named_list.go for the full reconciliation.
func TestLegacyLedgerCountMatchesThePublishedNumber(t *testing.T) {
	// Zero: the ledger started at 158 and the renames in this same batch drained every entry. It stays at
	// zero — a new process-named file is not an exemption request, it is the thing step 5b forbids.
	//
	// The reader is pointed at legacy_process_named_list.go and NOT at the plan document. An earlier
	// version of this message named `docs/reviews/batch-b2-plan.md`, which published a different figure
	// (165) from the one asserted here — so the gate whose whole job is keeping the published number
	// honest was sending a failing reader to a contradicting source. The source of truth for this number
	// is the regex above plus the ledger next to it, both in code, both re-derivable.
	const published = 0
	if got := len(legacyProcessNamedTestFiles); got != published {
		t.Errorf("legacyProcessNamedTestFiles has %d entries; this gate publishes %d.\n"+
			"The ledger is a DRAINING one: it started at 158 and every entry was removed by a rename. "+
			"Adding an entry back is not an exemption request — see legacy_process_named_list.go, "+
			"section \"KEEPING IT EMPTY\". If you are draining it further, lower the number here in the "+
			"same commit; the point of publishing it is that a reader can reproduce it.", got, published)
	}
}

// legacyProcessNamedTestFiles is generated by walking the tree at the moment the freeze landed. It is
// mechanical, not curated: `go test -run TestNoNewProcessNamedTestFiles` names anything missing.
var legacyProcessNamedTestFiles = func() map[string]bool {
	out := map[string]bool{}
	for _, p := range strings.Fields(legacyProcessNamedList) {
		out[p] = true
	}
	return out
}()

// repoRootForGuards is defined in promised_guard_test.go (this package's shared meta-gate helper).
var _ = os.Stat
