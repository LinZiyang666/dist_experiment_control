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
		// review-process events. `.*rereview.*` (not `.*external_?rereview.*`)
		// because a BARE `rereview` — `config_validation_rereview_test.go`,
		// added by a second external review — walked straight through the old
		// pattern that required an `external` prefix. Same lesson as the A9 note
		// below: a token that only matches its most common spelling is a token
		// somebody has to remember to widen; "rereview" is never a topic name.
		`.*rereview.*|.*external_review.*|.*_review_.*|.*_review$|` +
		`.*round[0-9]+.*|.*codex.*|.*allgreen.*|.*megaaudit.*|` +
		// bare phase / batch / audit identifiers used as the WHOLE prefix.
		//
		// EVERY letter, not a hand-picked subset. origin: line-2 closure verification (M8 residual). The
		// FUNCTION-name pattern below was widened from a curated letter list to [A-Z] during the review, on
		// the argument that a curated list is one somebody has to remember to extend — and the proof was a
		// live `A9` violation the curated version had let through. That argument applies word for word to
		// this pattern, which was left on `(r|p|d|b|c|g|s)`: `a9_probe_test.go`, `x7_*`, `y1_*`, `m2_*` and
		// `f3_*` all walked straight through the FILE freeze while their function-level equivalents were
		// caught. Fixing one half and publishing the reasoning for both is worse than fixing neither,
		// because the reasoning reads as applied.
		`[a-z][0-9]+(_[0-9a-z]+)*|` +
		`[a-z][0-9]+g?[0-9]*_.*|` +
		`g[0-9]+g[0-9]+_.*|ops[0-9]+.*` +
		`)$`)

// fileProductPrefixRe extracts a file basename's leading `<letters><digits>` token, so the same
// productPrefixes list that protects `TestS3…` also protects `x509_parse_test.go`.
//
// origin: line-2 closure verification. Widening this pattern's letter class from `(r|p|d|b|c|g|s)` to
// `[a-z]` (which it had to be — see the note in processNamedPattern) makes it collide with exactly the
// product names the function-name half already had to exempt: x509, s3, h2, sha256. Two patterns, one
// subtraction list.
var fileProductPrefixRe = regexp.MustCompile(`^([a-z]+[0-9]+)`)

// isProcessNamed reports whether a test file BASENAME (without the _test.go suffix) is process-named.
func isProcessNamed(base string) bool {
	if !processNamedPattern.MatchString(base) {
		return false
	}
	// A review-process token in the NAME is decisive; only the bare batch-prefix shape is subtractable.
	// (Same asymmetry, and same reason, as isProcessNamedFunc: subtracting from the review-token half
	// would let `s3_external_review_test.go` through.)
	for _, token := range []string{"external_review", "rereview", "_review", "round", "codex", "allgreen", "megaaudit", "ops"} {
		if strings.Contains(base, token) {
			return true
		}
	}
	if m := fileProductPrefixRe.FindStringSubmatch(base); m != nil && hasProductPrefix(m[1]) {
		return false
	}
	return true
}

// TestNoNewProcessNamedTestFiles is the freeze. It walks every *_test.go in the repo and fails on any
// process-named file that is not in the draining ledger.
func TestNoNewProcessNamedTestFiles(t *testing.T) {
	root := repoRoot(t)

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

// THE FUNCTION-NAME RULE lives in TWO patterns below (reviewTokenFuncRe + batchPrefixFuncRe) rather than
// one, because only ONE of the two halves may be subtracted from by productPrefixes. The single
// combined regex that used to be here is gone: with the subtraction running before it, a name
// carrying BOTH a product prefix and a review token escaped the gate entirely.
// origin: line-2 closure verification.
//
// The rule is separate from processNamedPattern because the two shapes differ: a filename is
// entirely process-named (`p13_external_review_round6`), whereas a function name carries the process
// token as a PREFIX and then says something real (`TestB4ExposeExplainJSONNoDeferredKeys`). Reusing
// the filename regex here would match almost nothing.
//
// WHY FUNCTION NAMES NEEDED THEIR OWN GATE
// ----------------------------------------
// The file freeze above drained to zero: 158 files renamed, ledger empty. That made the debt look
// paid. It was not — the same rot was one level down and completely unguarded: 529 of 2467 test
// functions (21.4%) still carried the batch or review round that produced them.
//
// The argument in CLAUDE.md §3 step 5b for putting provenance in a `// origin:` comment is that the
// comment "survives a rename, and a filename does not". That is true, and it is EQUALLY true of
// function names: someone changing classifyExit cannot grep their way to
// TestBExternalReviewBadRequestIsUsage any more than they could have found
// p13_external_review_round6_test.go. Freezing only filenames closed half the hole and made the other
// half harder to see, because the ledger being empty read as "done".

// productPrefixes are the `<Letter><digits>` tokens that name a PROTOCOL OR PRODUCT rather than a
// development batch. They live in exactly the shape batchPrefixFuncRe matches, so without this list the
// widened regex blocks correctly-named tests.
//
// origin: line-2 external review m16 / PF-11, which asked for synthetic negative controls on this regex.
// Writing them found three live false positives introduced by the PF-7 widening from a curated letter
// list to [A-Z]: TestS3BucketUploadRetries [example], TestH2StreamResetIsNotFatal [example],
// TestX11ForwardingRefused [example]. (All three are synthetic; [example] is what keeps
// promised_guard_test.go from reading a negative-control name as a promise that such a test exists.)
//
// WHY A SUBTRACTION LIST AND NOT A NARROWER REGEX. The two failure directions are not symmetric. A false
// POSITIVE is loud — the author sees the error, reads this list, and adds a line. A false NEGATIVE is
// silent, and a silent hole in exactly this gate is the defect PF-7 found (one live A9-prefixed violation
// shipped by the commit that added the gate). So the shape stays broad and the exceptions are named.
//
// X11 IS GENUINELY AMBIGUOUS and is listed here on purpose: it is both the X Window System and this very
// plan's eleventh permanent-rejection item. No test is named after a plan item today, and X11 the protocol
// is a real thing a test could be about, so the protocol wins. If someone ever does want to name a test
// after plan item X11, the `// origin:` line is where that belongs anyway — which is what step 5b says.
var productPrefixes = map[string]string{
	"S3":     "AWS S3",
	"H2":     "HTTP/2",
	"X11":    "the X Window System (also this plan's item X11 — see above)",
	"SHA256": "the hash",
	"TLS13":  "TLS 1.3",
	"UTF8":   "the encoding",
	"IPv6":   "the protocol",
	"P2p":    "peer-to-peer (lowercase p, so the regex should already miss it — kept as a control)",
	"K8s":    "Kubernetes",
	"H3":     "HTTP/3",
	"X509":   "the certificate standard — a file named x509_parse_test.go is about certificates",
	"B64":    "base64",
	"S390":   "the architecture (a GOARCH build-tag name)",
}

// TestProductPrefixesAreBoundedReasonedAndDoNotUnfreezeTheLedger is the reverse assertion on the
// subtraction list.
//
// origin: line-2 closure verification, which flagged productPrefixes as a category-scoped allow-list with
// no reverse assertion — the shape this repo forbids everywhere else. Two things are checkable:
//
//  1. every entry must carry a reason, so the list cannot grow by accretion;
//  2. the count is capped, so growth is a visible edit;
//  3. NO entry may excuse a name the LEDGER already records as process-named. The ledger is 439 names
//     somebody classified by hand; if the product list excuses one of them, the two mechanisms
//     contradict each other and the product list is silently un-freezing part of the debt.
//
// Clause 3 replaced a first attempt that tried to assert the entries are "shaped like products, not like
// batch labels". That check was wrong and its own failure message said so: `TestS3BucketUpload` [example] and
// `TestB4ExposeReq` are the SAME shape, which is precisely why a subtraction list is needed instead of a
// narrower regex. A guard that contradicts its own error text is worse than no guard.
//
// What remains uncheckable is whether a named product exists at all — `Q7` could be invented. The cap is
// the backstop: an invented entry has to be typed next to a number somebody has to raise.
func TestProductPrefixesAreBoundedReasonedAndDoNotUnfreezeTheLedger(t *testing.T) {
	const productPrefixCap = 13
	if n := len(productPrefixes); n != productPrefixCap {
		t.Errorf("productPrefixes has %d entries, cap is %d. Every entry is a hole in the naming gate, so "+
			"growth must be a visible edit: raise the cap in the same commit and say which product it is.",
			n, productPrefixCap)
	}
	for prefix, why := range productPrefixes {
		if strings.TrimSpace(why) == "" {
			t.Errorf("productPrefixes[%q] has no reason — an exemption that does not argue for itself is "+
				"just a slower way of having no gate", prefix)
		}
	}

	var unfrozen []string
	for key := range legacyProcessNamedTestFuncs {
		name := key
		if i := strings.LastIndex(key, ": "); i >= 0 {
			name = key[i+2:]
		}
		if !isProcessNamedFunc(name) {
			unfrozen = append(unfrozen, key)
		}
	}
	sort.Strings(unfrozen)
	if len(unfrozen) > 0 {
		t.Errorf("%d ledger entr(y/ies) are no longer recognised as process-named, so the product-prefix "+
			"subtraction (or a regex edit) has silently un-frozen part of the debt:\n  %s\n\n"+
			"The ledger is the hand-classified record of what this rule is about. If one of these really is "+
			"a product name rather than a batch label, DELETE it from the ledger in the same commit — "+
			"leaving it in a ledger that no longer applies to it makes both mechanisms lie.",
			len(unfrozen), strings.Join(unfrozen, "\n  "))
	}
}

var productPrefixRe = regexp.MustCompile(`^(?:Test|Benchmark|Fuzz)([A-Za-z]+[0-9]+[A-Za-z]?)`)

// reviewTokenFuncRe is the half of the function-name rule that the product list must NEVER subtract from:
// an explicit review-process token anywhere in the name. `ExternalReview`, `Round3`, `AllGreen`, `Codex`,
// `MegaAudit` are not things a protocol is called.
//
// origin: line-2 closure verification. The first version of the subtraction ran BEFORE the whole regex and
// returned false on any product-prefixed name, so `TestS3ExternalReviewRound6Fixes` [example] escaped the
// gate entirely — the mitigation for a false-positive problem opened a false-NEGATIVE hole, which is the
// strictly worse direction and the one this gate exists to close. Splitting the pattern means the
// subtraction can only ever excuse the shape it was written for (a batch-letter prefix), never a name that
// says "review round" out loud.
var reviewTokenFuncRe = regexp.MustCompile(
	`^(Test|Benchmark|Fuzz)(` +
		`.*ExternalR[ee]?[Rr]eview.*|.*Rereview.*|.*Round[0-9].*|.*AllGreen.*|.*Codex.*|.*MegaAudit.*` +
		`)$`)

// batchPrefixFuncRe is the other half: a `<Letter><digits><Upper>` prefix. This is the shape that collides
// with protocol and product names, so this is the only half productPrefixes subtracts from.
var batchPrefixFuncRe = regexp.MustCompile(`^(Test|Benchmark|Fuzz)[A-Z][0-9]+[A-Z].*$`)

// hasProductPrefix reports whether the leading `<letters><digits>` token of a test name is a known protocol
// or product rather than a development batch. Case-insensitive so the same list serves file names (`s3_`,
// `x509_`) and function names (`TestS3…`).
func hasProductPrefix(token string) bool {
	lower := strings.ToLower(token)
	for prefix := range productPrefixes {
		if strings.HasPrefix(lower, strings.ToLower(prefix)) {
			return true
		}
	}
	return false
}

func isProcessNamedFunc(name string) bool {
	// A review-process token is decisive and is never subtracted.
	if reviewTokenFuncRe.MatchString(name) {
		return true
	}
	if !batchPrefixFuncRe.MatchString(name) {
		return false
	}
	if m := productPrefixRe.FindStringSubmatch(name); m != nil && hasProductPrefix(m[1]) {
		return false
	}
	return true
}

var testFuncDeclRe = regexp.MustCompile(`(?m)^func ((?:Test|Benchmark|Fuzz)[A-Za-z0-9_]*)\s*\(`)

// TestNoNewProcessNamedTestFunctions freezes function names the same way the file gate freezes
// filenames, with the same draining ledger and the same reverse assertion.
func TestNoNewProcessNamedTestFunctions(t *testing.T) {
	root := repoRoot(t)

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
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		rel, rlerr := filepath.Rel(root, p)
		if rlerr != nil {
			return rlerr
		}
		rel = filepath.ToSlash(rel)
		for _, m := range testFuncDeclRe.FindAllStringSubmatch(string(src), -1) {
			name := m[1]
			if !isProcessNamedFunc(name) {
				continue
			}
			// SITE-scoped key, not name-scoped. origin: line-2 external review M7 / PI-7. The first
			// version keyed on the bare function name, which made each of the 436 entries a REUSABLE
			// pass: a lane created test/zzprobe/probe_test.go containing nothing but
			// `func TestB1ClusterVerdict` -- a name already in the ledger -- and the gate stayed green,
			// while the same file with a fresh name reddened. That is a category-scoped exemption, the
			// exact shape this repo's own rule forbids ("cover the sites that were examined, never
			// future ones").
			//
			// It also broke draining: two same-named process-named functions in different packages meant
			// renaming one left `seen[name]` true from the other, so the ledger line survived and went on
			// excusing a third occurrence.
			//
			// The cost is real and is the point: moving a legacy-named test file now makes its entries
			// stale and reddens, and the ledger must be edited in the same commit. That is what a
			// site-scoped exemption feels like.
			key := rel + ": " + name
			seen[key] = true
			if !legacyProcessNamedTestFuncs[key] {
				offenders = append(offenders, key)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d test function(s) are named after a development-process event and are not in the "+
			"legacy ledger:\n  %s\n\n"+
			"Name the function after the BEHAVIOUR it pins (TestExposeExplainOmitsDeferredKeys, not "+
			"TestB4ExposeExplainJSONNoDeferredKeys). The batch or review round that prompted it belongs in "+
			"a `// origin:` line above the function, where it survives a rename instead of hiding the test "+
			"from the next person who changes that behaviour. See CLAUDE.md §3 step 5b.",
			len(offenders), strings.Join(offenders, "\n  "))
	}

	// GROWTH CAP. origin: line-2 external review IDG-6 / PF-13. The ledger's header says "an entry is
	// only ever REMOVED, never added" and that was PROSE: a review lane added a violating function plus a
	// matching ledger line and the gate went green at 437. Its sibling ledger (filenames) has
	// `const published = 0` enforcing the same claim mechanically; this one had nothing.
	//
	// The number is the enforcement. Renaming drains it, so the cap only ever moves DOWN -- and lowering
	// it is a one-token edit in the same commit as the rename, which is the friction that makes the
	// draining visible in review rather than implied by a comment.
	//
	// 439, not 436: the count rose when the key became site-scoped (review M7). Three names each live in
	// two packages and the name-keyed map had collapsed each pair into one entry, so the old number was
	// undercounting present sites, not just permitting future ones. See legacy_process_named_funcs.go.
	const legacyFuncLedgerCap = 439
	if n := len(legacyProcessNamedTestFuncs); n > legacyFuncLedgerCap {
		t.Errorf("legacyProcessNamedTestFuncs has %d entries, cap is %d.\n\n"+
			"This ledger only drains. A new process-named test function must be RENAMED, not added here — "+
			"the header has always said so, and this cap is what makes the sentence true. If you renamed "+
			"something and the count went DOWN, lower the cap in the same commit.",
			n, legacyFuncLedgerCap)
	}
	if n := len(legacyProcessNamedTestFuncs); n < legacyFuncLedgerCap {
		t.Errorf("legacyProcessNamedTestFuncs is down to %d entries but the cap still says %d — lower the "+
			"cap so the win is locked in (an unlowered cap is slack, and slack is how a ratchet stops "+
			"measuring).", n, legacyFuncLedgerCap)
	}

	var stale []string
	for name := range legacyProcessNamedTestFuncs {
		if !seen[name] {
			stale = append(stale, name)
		}
	}
	sort.Strings(stale)
	if len(stale) > 0 {
		t.Errorf("%d entr(y/ies) in legacyProcessNamedTestFuncs no longer name an existing "+
			"process-named test function:\n  %s\n\n"+
			"Delete them in the same commit as the rename — that is how this ledger drains.",
			len(stale), strings.Join(stale, "\n  "))
	}
}

// TestNoProcessNamedProductionFiles is the same rule for production sources, and it carries NO ledger
// on purpose: the tree had exactly one offender when this landed (cmd/tether/d8_alerts.go, renamed to
// alert_gate.go in the same commit), so the rule starts at zero and can stay there. An allow-list with
// nothing in it is not an allow-list.
func TestNoProcessNamedProductionFiles(t *testing.T) {
	root := repoRoot(t)

	var offenders []string
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
		name := d.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			return nil
		}
		if !isProcessNamed(strings.TrimSuffix(name, ".go")) {
			return nil
		}
		rel, rerr := filepath.Rel(root, p)
		if rerr != nil {
			return rerr
		}
		offenders = append(offenders, filepath.ToSlash(rel))
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}

	sort.Strings(offenders)
	if len(offenders) > 0 {
		t.Errorf("%d production file(s) are named after a development-process event:\n  %s\n\n"+
			"Production files are named after the RESPONSIBILITY they hold (alert_gate.go, not "+
			"d8_alerts.go). There is deliberately no legacy ledger for this rule — the tree was at zero "+
			"when it landed, and it is cheap to keep there.",
			len(offenders), strings.Join(offenders, "\n  "))
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
		// bare `rereview` with no `external` prefix — the exact shape that slipped
		// the old `external_?rereview` pattern (a real second-review file did this).
		"config_validation_rereview",
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

// TestProcessNamedFuncPatternRecognisesTheRealShapes is isProcessNamedFunc's companion, with synthetic
// samples on both sides.
//
// origin: line-2 external review m16 / PF-11. The FILENAME regex had this companion from the start; the
// FUNCTION regex — the harder of the two, hand-written, and widened during this same review from a curated
// letter list to [A-Z] — had none. A regex with no negative controls is a regex whose false positives are
// discovered by whoever it blocks next.
//
// The negative half is the load-bearing half here. `[A-Z][0-9]+[A-Z]` is a broad shape and product names
// live in it: S3, H2, X11, K8s. A gate that demands `TestS3BucketUpload [example]` be entered into a legacy
// ledger is a gate people route around.
func TestProcessNamedFuncPatternRecognisesTheRealShapes(t *testing.T) {
	mustMatch := []string{
		"TestB4ExposeReqFlagsSerialize",
		"TestP13ProxyFenceHoldsAcrossReconnect",
		"TestG67FoldProxyHomeCounts",
		"TestD9MatrixCutoverRestart",
		"TestA9RebalanceTargetsExcludeDraining",  // the live violation the widening caught
		"TestY2DownloadCodesSplitByCause",        // this plan's own item labels
		"TestSomethingExternalReviewRound3Fixed", // review-process token anywhere in the name
		// origin: line-2 closure verification. The product-prefix subtraction used to run BEFORE the
		// whole regex, so a product-prefixed name carrying a review token escaped the gate entirely.
		"TestS3ExternalReviewRound6Fixes",
		"TestCodexAllGreenSweep",
		"TestMegaAuditFindings",
		"BenchmarkB4ExposeThroughput", // Benchmark and Fuzz are covered too
		"FuzzP13SubjectParser",
	}
	for _, n := range mustMatch {
		if !isProcessNamedFunc(n) {
			t.Errorf("isProcessNamedFunc does NOT match %q — that is exactly the shape being frozen, so a "+
				"new one would walk through the gate", n)
		}
	}

	mustNotMatch := []string{
		// Ordinary behaviour-named tests.
		"TestExposeExplainOmitsDeferredKeys",
		"TestCloseProxyInvalidatesInFlightRegister",
		"TestPackageLayering",
		"TestNoDefaultOnRepoEnumSwitch",
		// PRODUCT AND PROTOCOL NAMES that live in the [A-Z][0-9]+[A-Z] shape. These are the false
		// positives the widening risks, and each one is a real thing a test here could be named after.
		"TestS3BucketUploadRetries",
		"TestH2StreamResetIsNotFatal",
		"TestX11ForwardingRefused",
		"TestSHA256HelpersRoundTrip",
		"TestTLS13HandshakeRejectsOldCipher",
		"TestUTF8SubjectNamesAreRejected",
		"TestIPv6RouteAddrParses",
		// A digit-then-lowercase sequence must not trip it either.
		"TestP2pDialFallsBack",
	}
	for _, n := range mustNotMatch {
		if isProcessNamedFunc(n) {
			t.Errorf("isProcessNamedFunc WRONGLY matches %q. A false positive forces a correctly-named test "+
				"into the legacy ledger, which is both a lie about the ledger's contents and the friction "+
				"that teaches people to bypass the gate.", n)
		}
	}
}
