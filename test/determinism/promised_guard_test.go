package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// legacyMissingGuards freezes the promises that were already broken when this
// gate was written. There are 33 of them (34 when this comment was first written; line-2 §12 Y3 removed
// the one entry whose only naming comment had been deleted — see the note further down), spread across
// security tests, the
// D5-D9 regression suites and several production files — i.e. the pattern batch
// A hit four times is not a batch-A habit, it is a repo-wide one.
//
// They are frozen rather than fixed here for a plain reason: fixing them means
// deciding, per entry, whether the named test was renamed, deleted, or never
// written — 34 separate archaeology tasks, none of which belong in an increment
// about error codes and loopback guards. The gate is worth having NOW, because
// it stops the count from growing; draining this list is its own piece of work.
//
// An empty struct{} value keeps entries cheap to add and impossible to leave
// half-explained: to remove one, make the test exist.
var legacyMissingGuards = map[string]struct{}{
	"TestAgentProvEvictedBindingCanRebind":     {},
	"TestAgentRunFailsOnBadNATSURL":            {},
	"TestAgentYAMLPathTraversalInSessionField": {},
	"TestClusterDoctorOnlineThresholds":        {},
	"TestD5ProductionWiresNoClusterNode":       {},
	"TestD6ProductionWiresNoClusterNode":       {},
	"TestD7ProductionWiresNoCluster":           {},
	// One entry was removed here by line-2 §12 Y3: TestD8GuardExclusionsJustified [deleted]. The G3.5 merge
	// deleted test/d8/regression_test.go, whose header was the ONLY place naming it, so the promise
	// vanished and the exemption became dead weight. The reverse assertion added in that same change is
	// what reported it BY NAME, which is why it could be deleted rather than guessed at — four other
	// production-wiring entries here are still named by live comments and stay.
	//
	// That name is written out now, which it could not be before. This gate harvests Test-prefixed
	// identifiers from comments, so naming a deleted promise inside the note explaining its deletion
	// re-created it: the entry was gone, the comment still said the name, and the gate reported a
	// brand-new broken promise. The first two attempts at this comment therefore ended in "the identifier
	// is deliberately not written here" — a note about a name, with the name withheld, which is close to
	// useless to the next reader. The [deleted] marker (see notAPromise) is the exit from that, and this
	// is the case it was built for.
	"TestD8ProductionWiresNoCluster":                              {},
	"TestDial_NoProxyBypassesProxy":                               {},
	"TestDxProductionWiresNoCluster":                              {},
	"TestExecArgvNoShellExpansion":                                {},
	"TestExposeNameNULByteCurrentBehavior":                        {},
	"TestG1AgentReportedExitFlowsToSQLite":                        {},
	"TestG1RevokedPortReachesAgentOnReconnect":                    {},
	"TestG67TLBucketNotFoundIsTransient":                          {},
	"TestG67TLEveryTransferRefusalGoesThroughTransferRefusalErr":  {},
	"TestG67TLPermanent10047RuleIsLoadBearing":                    {},
	"TestG67TLProbeWarningIsAnnouncedAtBothCallSites":             {},
	"TestG67TLTierACeilingCallSitesUseTheConnectionMeasurement":   {},
	"TestG67TLTransientCodeRulesSurviveANeutralDescription":       {},
	"TestHomeDeliveryVerbIsWireStable":                            {},
	"TestInstallShBrokerDryRunIsNonRoot":                          {},
	"TestInstallShPrefixRelativePathStaysInsideHome":              {},
	"TestLoopSetCountIsNotHardcoded":                              {},
	"TestMegaAuditMAJ5RaiseFailureNonZeroExitAndGuideRaisedFalse": {},
	"TestPINLargeInputCurrentBehavior":                            {},
	"TestReadyzBands":                                             {},
	"TestRenewLeaseRereolvesTheLeaderEveryCall":                   {},
	"TestSettleReturnsForceSingleImmediately":                     {},
	"TestSettleReturnsQuorumLostImmediately":                      {},
	"TestUpgradeHomesConvergedOpIsWireStable":                     {},
	"TestUpgradeWriteToReadOnlyDirReturnsError":                   {},
}

// TestPromisedGuardTestsExist is the meta-gate this batch earned the hard way.
//
// Batch-A found and removed a false promise: proto.RehomeDirective's doc said
// "a guard test asserts it has no live publisher" and no such test existed. The
// same batch then wrote THREE more of them —
//
//   - internal/proto/codes.go named TestWireCodeNamespacesAgree, twice, in the
//     present tense, before it was written;
//   - internal/auth/acl_reconcile_test.go and internal/auth/permissions.go
//     described a "bidirectional" reconciliation that ran one way;
//   - internal/cluster/node.go said raft logging was "finally wired" while the
//     only production constructor could not pass a logger.
//
// The pattern is not carelessness about tests; it is that a comment asserting a
// guarantee reads to the next reviewer exactly like the guarantee itself, and
// nothing in the toolchain disagrees. A promised guard is worse than an admitted
// gap — the reader stops looking.
//
// notAPromise reports whether a comment line carries an explicit marker saying its Test names are NOT
// claims that such tests exist.
//
// origin: line-2 external review, and specifically the three times this increment tripped over the same
// shape: DESCRIBING a test name re-creates the pattern that detects it. It happened to Y3 (a comment about
// a removed promise became a promise), to loopset.go (prose about a deleted //nolint matched the //nolint
// scanner), and to test/architecture/layering_test.go, whose whole job is to record WHICH deleted test
// each merged rule came from — the map is only useful if it spells the names, and spelling them made this
// gate report ten broken promises.
//
// Before this marker existed the only ways out were to gag the comment ("the identifier is deliberately
// not written here") or to delete the information. Both were taken during this increment and both are
// worse than the problem: the reader of a d5 review report cannot grep for a name nobody wrote down.
//
// The marker is per-comment-line and must be explicit, so it cannot be acquired by accident:
//
//	[deleted]     the named test no longer exists; the name is recorded so it stays greppable
//	[example]     a synthetic name used as a positive/negative control for a naming rule
//
// A marker is a claim in its own right, and a wrong one is visible: writing [deleted] next to a test that
// does exist is a lie a reader can check in one grep.
func notAPromise(text string) bool {
	return strings.Contains(text, "[deleted]") || strings.Contains(text, "[example]")
}

// So: if a comment names a Test function, that function must exist.
func TestPromisedGuardTestsExist(t *testing.T) {
	root := repoRoot(t)

	// Matches a Test identifier mentioned in a comment. Deliberately narrow:
	// only Go-style test names, so prose like "a test asserts" does not trip it.
	named := regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]{3,}\b`)

	declared := map[string]bool{}
	mentioned := map[string][]string{} // test name -> comment sites

	for _, dir := range []string{"internal", "cmd", "test"} {
		walkErr := filepath.Walk(filepath.Join(root, dir), func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") {
				return nil
			}
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, p, nil, parser.ParseComments)
			if perr != nil {
				return nil
			}
			for _, d := range f.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok && strings.HasPrefix(fd.Name.Name, "Test") {
					declared[fd.Name.Name] = true
				}
			}
			rel, _ := filepath.Rel(root, p)
			for _, cg := range f.Comments {
				for _, c := range cg.List {
					if notAPromise(c.Text) {
						continue
					}
					for _, m := range named.FindAllString(c.Text, -1) {
						site := filepath.ToSlash(rel) + ":" + strconv.Itoa(fset.Position(c.Pos()).Line)
						mentioned[m] = append(mentioned[m], site)
					}
				}
			}
			return nil
		})
		if walkErr != nil {
			t.Fatalf("walk %s: %v", dir, walkErr)
		}
	}

	if len(declared) == 0 || len(mentioned) == 0 {
		t.Fatalf("scan degenerated (declared=%d mentioned=%d); a green result would mean nothing",
			len(declared), len(mentioned))
	}
	// NAMED floors on top of the "not zero" one. origin: line-2 closure verification Q3. `== 0` over a
	// population of ~2500 declarations and ~200 mentions only catches a walker that visited nothing; a
	// walker that lost one of the three trees (internal, cmd, test) still clears it by three orders of
	// magnitude, and this gate's whole job is to notice a name that is mentioned but not declared — which
	// is exactly what losing a tree fabricates.
	//
	// One known declaration per scanned tree, so losing any single tree is named rather than counted.
	// Each name must ACTUALLY live in the tree it is paired with, or the failure message names the wrong
	// tree and sends the reader to the wrong place. (First version paired `internal` with a test that lives
	// under test/ — the assertion still fired, but its diagnosis was wrong, which for a message this
	// specific is its own small version of the defect this gate is about.)
	for tree, must := range map[string]string{
		"internal": "TestPTYFailureTransientClassification", // internal/agent
		"cmd":      "TestCauseSplitCodesHaveTriggerTests",   // cmd/tether
		"test":     "TestNoNewProcessNamedTestFunctions",    // test/determinism
	} {
		if !declared[must] {
			t.Fatalf("the scan did not find %s, a declaration that exists today. The %q tree is not being "+
				"walked, and every name declared only there now looks like a broken promise — this gate "+
				"would report FABRICATED failures rather than missing one.", must, tree)
		}
	}

	// Prefix matching: comments routinely name a test family by its stem
	// ("TestD9ClusterMode" for TestD9ClusterModeEnabledDetection). Requiring an
	// exact match turns those into ~40 false positives, and a gate that cries
	// wolf gets muted — which is the failure mode this whole file is about.
	declaredPrefix := func(name string) bool {
		if declared[name] {
			return true
		}
		for d := range declared {
			if strings.HasPrefix(d, name) {
				return true
			}
		}
		return false
	}

	var broken []string
	var legacyStillMissing int
	for name, sites := range mentioned {
		if declaredPrefix(name) {
			if _, frozen := legacyMissingGuards[name]; frozen {
				t.Errorf("%s now exists but is still in legacyMissingGuards; remove the entry", name)
			}
			continue
		}
		if _, frozen := legacyMissingGuards[name]; frozen {
			legacyStillMissing++
			continue
		}
		sort.Strings(sites)
		broken = append(broken, name+"  named at "+strings.Join(sites, ", "))
	}
	sort.Strings(broken)

	// REVERSE ASSERTION (line-2 §12 Y3). The loop above walks `mentioned` — the names comments
	// actually say — so it can only retire a ledger entry when the promised test APPEARS. The other way
	// an entry stops being true is that the COMMENT goes away: delete the sentence and the name is
	// never visited again, and its ledger line sits there forever, still granting an exemption for a
	// promise nobody is making. Every other draining ledger in this repo checks both directions; this
	// one checked one.
	//
	// It was not hypothetical when this landed. The line-2 G3.5 merge deleted the four
	// test/d{5,6,7,8}/regression_test.go files, and their headers were where several of the
	// production-wiring promises below were named — deleting them orphaned those ledger entries, which
	// is what this assertion now reports so they can be removed by name rather than by guess. Both lanes
	// that noticed the missing reverse check wrote "not in this lane's scope" and moved on, which is how
	// it survived to be found here.
	//
	// The promise's identifier is deliberately NOT spelled out above. This scanner harvests
	// Test-prefixed identifiers FROM COMMENTS, so naming it here would re-create the very promise this
	// paragraph says is dead — the orphan check would then never fire for the one entry it was written
	// to catch. (Found by re-reading this file after writing it: the first draft did name it.)
	var orphaned []string
	for name := range legacyMissingGuards {
		if _, stillNamed := mentioned[name]; !stillNamed {
			orphaned = append(orphaned, name)
		}
	}
	sort.Strings(orphaned)
	if len(orphaned) > 0 {
		t.Errorf("%d legacyMissingGuards entr(y/ies) are no longer named by ANY comment:\n  %s\n\n"+
			"The promise they exempt has been deleted, so the exemption is dead weight — remove those "+
			"lines. An exemption that outlives the claim it excuses is how an allow-list turns into a "+
			"permanent allow-everything.",
			len(orphaned), strings.Join(orphaned, "\n  "))
	}

	if legacyStillMissing > 0 {
		t.Logf("%d pre-existing broken promises remain frozen in legacyMissingGuards (see its doc)", legacyStillMissing)
	}
	if len(broken) > 0 {
		t.Errorf("%d NEW comment(s) name a test function that does not exist:\n  %s\n\n"+
			"Write the test, or delete the sentence. A comment claiming a guarantee reads exactly like "+
			"the guarantee to the next reviewer — which is why this repo has shipped four of them.",
			len(broken), strings.Join(broken, "\n  "))
	}
}

// TestPromisedGuardScannerIsNotVacuous proves the scanner can actually fail.
func TestPromisedGuardScannerIsNotVacuous(t *testing.T) {
	const sample = `package x

// TestThisDoesNotExistAnywhere pins something important.
func f() {}
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "sample.go", sample, parser.ParseComments)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	named := regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]{3,}\b`)
	found := false
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if named.MatchString(c.Text) {
				found = true
			}
		}
	}
	if !found {
		t.Error("SELF-CHECK FAILED: the scanner cannot see a test name inside a comment, so its clean " +
			"result above is meaningless")
	}
}

// The repo-root and int-to-string helpers this file used to carry are gone.
//
// origin: line-2 external review GI-8. This package had TWO repo-root helpers and TWO hand-rolled
// int-to-string helpers, in the same package, under different names -- `repoRootForGuards` alongside
// `repoRoot`, `itoa` alongside `itoaDeterminism`. That is not accidental duplication: writing a second
// helper with the same body needs a redeclaration error first, so each pair records someone hitting
// "already declared" and renaming past it instead of calling what was already there.
//
// The two were not equivalent, which is the part that mattered. `repoRootForGuards` located the root as
// filepath.Dir(filepath.Dir(wd)) -- exactly two levels up, correct only for a test whose package sits at
// depth two, silently wrong anywhere else. The survivor (lint_skeleton_test.go) walks up until it finds
// go.mod. Fourteen call sites were using the fragile one.
//
// The int-to-string pair was worse than duplicated: both were hand-rolled digit loops standing in for
// strconv.Itoa. Every call site now uses strconv.Itoa.
//
// dupl is enabled at a threshold of 100 tokens and cannot see any of this -- these helpers are ~30
// tokens each, and lowering the threshold far enough to catch them buries the tree in table-driven-test
// false positives. Small-helper duplication has no linter; it needs a reader.

// TestNotAPromiseMarkersAreBoundedAndHonest keeps notAPromise's escape hatch from becoming the thing it
// was built to avoid.
//
// (The two marker tokens are deliberately not spelled on the same line as this function's name — a line
// carrying both would mark itself, which is a small illustration of why the markers have to be explicit.)
//
// The marker suppresses a real check, so it needs the same friction every other exemption in this repo has:
//
//	CAP        the count is pinned. A marker spreading quietly would hollow out the gate one comment at a
//	           time, which is precisely how an allow-list rots. Raising the number is a visible edit.
//	HONESTY    [deleted] next to a test that DOES exist is a false statement, and unlike the suppression
//	           itself that is mechanically checkable — so it is checked.
//
// What cannot be checked mechanically is someone writing [deleted] on a promise that was never kept. That
// is stated rather than papered over: the marker is a claim by an author, and the cap is what keeps the
// number of such claims small enough to read.
func TestNotAPromiseMarkersAreBoundedAndHonest(t *testing.T) {
	root := repoRoot(t)
	named := regexp.MustCompile(`\bTest[A-Z][A-Za-z0-9_]{3,}\b`)

	declared := map[string]bool{}
	markedDeleted := map[string][]string{}
	markers := 0

	for _, dir := range []string{"internal", "cmd", "test"} {
		walkErr := filepath.Walk(filepath.Join(root, dir), func(p string, info os.FileInfo, err error) error {
			if err != nil || info.IsDir() || !strings.HasSuffix(p, ".go") {
				return nil
			}
			fset := token.NewFileSet()
			f, perr := parser.ParseFile(fset, p, nil, parser.ParseComments)
			if perr != nil {
				return nil
			}
			for _, d := range f.Decls {
				if fd, ok := d.(*ast.FuncDecl); ok && strings.HasPrefix(fd.Name.Name, "Test") {
					declared[fd.Name.Name] = true
				}
			}
			rel, _ := filepath.Rel(root, p)
			for _, cg := range f.Comments {
				for _, c := range cg.List {
					// Count only markers that actually SUPPRESS something: a marked line with no Test name
					// on it silences nothing, and counting the convention's own documentation toward the
					// cap would make the number say something other than "how much checking is off".
					if !notAPromise(c.Text) || len(named.FindAllString(c.Text, -1)) == 0 {
						continue
					}
					markers++
					if !strings.Contains(c.Text, "[deleted]") {
						continue
					}
					site := filepath.ToSlash(rel) + ":" + strconv.Itoa(fset.Position(c.Pos()).Line)
					for _, m := range named.FindAllString(c.Text, -1) {
						markedDeleted[m] = append(markedDeleted[m], site)
					}
				}
			}
			return nil
		})
		if walkErr != nil {
			t.Fatalf("walk %s: %v", dir, walkErr)
		}
	}

	// The cap. 9 today: 3 in layering_test.go's prose, 5 synthetic controls in test_naming_test.go, and 1
	// in legacyMissingGuards above (the Y3 promise's identifier, written out at last — that comment had
	// twice ended in "the identifier is deliberately not written here", a note about a name with the name
	// withheld).
	//
	// It went 15 -> 16 -> 6. The drop is the mechanism working: layering_test.go's TEST-NAME MAP was 10
	// marked names in a comment, and the closure verification found 8 of them FABRICATED — a suppression
	// marker paired with an unverifiable claim, written in the same change by the same author. Moving the
	// map into DATA (`deletedRegressionTests`, reconciled against `git show HEAD:` by
	// TestDeletedRegressionTestNamesAreReal) removed all 10 markers at once, because a string literal is
	// not a comment and this scanner only reads comments.
	//
	// That is the general lesson worth keeping: needing a marker is usually a sign the claim belongs in
	// data where something can check it, not in prose where only a human can.
	const notAPromiseMarkerCap = 9
	if markers > notAPromiseMarkerCap {
		t.Errorf("%d [deleted]/[example] markers, cap is %d.\n\n"+
			"Each one silences the promised-guard check for its line. That is sometimes right — a map of "+
			"deleted test names is only useful if it spells them — but it is never free. If the new marker "+
			"is justified, raise the cap in the same commit and say why; if it is dodging an unkept "+
			"promise, write the test instead.", markers, notAPromiseMarkerCap)
	}
	if markers < notAPromiseMarkerCap {
		t.Errorf("only %d markers but the cap says %d — lower the cap so the reduction is locked in "+
			"(unlowered slack is how a ratchet stops measuring)", markers, notAPromiseMarkerCap)
	}

	var lying []string
	for name, sites := range markedDeleted {
		if declared[name] {
			sort.Strings(sites)
			lying = append(lying, name+"  claimed [deleted] at "+strings.Join(sites, ", "))
		}
	}
	sort.Strings(lying)
	if len(lying) > 0 {
		t.Errorf("%d name(s) are marked [deleted] but the test EXISTS:\n  %s\n\n"+
			"Drop the marker — the promise is kept, so the check should run. A marker that misdescribes "+
			"its subject is worse than no marker: it suppresses a check AND misinforms the reader.",
			len(lying), strings.Join(lying, "\n  "))
	}
}
