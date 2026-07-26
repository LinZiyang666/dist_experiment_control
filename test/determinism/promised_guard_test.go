package determinism

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// legacyMissingGuards freezes the promises that were already broken when this
// gate was written. There are 34 of them, spread across security tests, the
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
	"TestAgentProvEvictedBindingCanRebind":                        {},
	"TestAgentRunFailsOnBadNATSURL":                               {},
	"TestAgentYAMLPathTraversalInSessionField":                    {},
	"TestClusterDoctorOnlineThresholds":                           {},
	"TestD5ProductionWiresNoClusterNode":                          {},
	"TestD6ProductionWiresNoClusterNode":                          {},
	"TestD7ProductionWiresNoCluster":                              {},
	"TestD8GuardExclusionsJustified":                              {},
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
	"TestReviewReadmeIsReleaseCurrent":                            {},
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
// So: if a comment names a Test function, that function must exist.
func TestPromisedGuardTestsExist(t *testing.T) {
	root := repoRootForGuards(t)

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
					for _, m := range named.FindAllString(c.Text, -1) {
						site := filepath.ToSlash(rel) + ":" + itoa(fset.Position(c.Pos()).Line)
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

func repoRootForGuards(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	root := filepath.Dir(filepath.Dir(wd))
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatalf("repo root %q has no go.mod", root)
	}
	return root
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
