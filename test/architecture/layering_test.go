package architecture

import (
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// layering_test.go — every package-layering rule in the repo, in ONE table (S3 G3.5).
//
// WHERE THESE CAME FROM
// ---------------------
// They were four files: test/d{5,6,7,8}/regression_test.go, 380 lines holding 10 test functions and
// FOUR copies of goListDeps + moduleRoot. Each D-phase that touched the layering added its own file
// with its own copy of the helpers, so "how many layering rules does tether have?" had no single
// answer, and the same invariant was asserted up to four times in four wordings:
//
//	internal/cluster must stay NATS-free    asserted in d5, d6, d7 AND d8
//	internal/jsstream must not import cluster   asserted in d5 AND d8
//
// The duplicates were not identical, which is the real cost: d8's version of the cluster rule banned
// only nats.go, while d5's also banned nats.go/jetstream, nats-server, internal/broker and
// internal/jsstream. A reader who found the d8 one first would conclude the boundary was narrower
// than it is. This table carries the UNION of every clause from all four files -- nothing was dropped
// in the merge -- so the narrow restatements cannot mislead anyone again.
//
// Adding a rule is now one row instead of one file.
//
// NOT MOVED HERE (deliberately): the raft-determinism confinement lint in
// test/determinism/lint_skeleton_test.go. It is already in the right place and carries its own
// self-check; S3 §5 explicitly said to leave it and point at it from here, which this comment does.

// TestStackharnessIsImportedOnlyFromTests is the REVERSE direction the table below cannot express:
// test/stackharness may import product packages (that is why it exists — see its header), so
// nothing that ships may import it back. Any non-test .go file under cmd/ or internal/ naming it is
// red; and, G2, at least one _test.go must import it or the rule guards nothing.
// origin: docs/reviews/test-system-overhaul-plan.md B5 (§2.3).
func TestStackharnessIsImportedOnlyFromTests(t *testing.T) {
	root := repoRoot(t)
	const imp = `"github.com/LinZiyang666/tether/test/stackharness"`
	var offenders []string
	importers := 0
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
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
		if !strings.HasSuffix(p, ".go") {
			return nil
		}
		src, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		if !strings.Contains(string(src), imp) {
			return nil
		}
		rel, _ := filepath.Rel(root, p)
		rel = filepath.ToSlash(rel)
		if strings.HasSuffix(p, "_test.go") || strings.HasPrefix(rel, "test/") {
			importers++
			return nil
		}
		offenders = append(offenders, rel)
		return nil
	})
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	if len(offenders) > 0 {
		t.Errorf("%d shipping file(s) import test/stackharness:\n  %s\n\nIt pulls product packages in the "+
			"other direction and is test scaffolding by construction.", len(offenders), strings.Join(offenders, "\n  "))
	}
	if importers < 8 {
		t.Fatalf("only %d test file(s) import test/stackharness — the eight absorbed seedSession forwarders "+
			"should, so either they moved or this scan is blind", importers)
	}
}

// layerRule is one package's boundary.
//
// requiredTransitive is not decoration: it is the per-row self-check. `go list -deps` returning an
// empty or truncated set would make every bannedTransitive clause pass vacuously, so each row names
// something the package provably DOES depend on. A broken query then fails loudly instead of
// certifying the boundary it never examined.
type layerRule struct {
	pkg                string
	why                string
	bannedTransitive   []string
	requiredTransitive []string
	bannedDirect       []string
}

const modPrefix = "github.com/LinZiyang666/tether/"

var layerRules = []layerRule{
	{
		pkg: modPrefix + "internal/testharness",
		why: "harness cycle rule (harness.go header; docs/testing-standards.md §七): fourteen package-broker " +
			"tests, seven package-agent tests and session's own test import testharness, so one import back " +
			"from it is a build-breaking cycle. Product-dependent primitives live in test/stackharness. " +
			"Internal review L5-F5: the rule was prose in the plan and in no header; now it is both.",
		bannedTransitive: []string{
			modPrefix + "internal/broker",
			modPrefix + "internal/agent",
			modPrefix + "internal/session",
		},
		requiredTransitive: []string{
			modPrefix + "internal/storage",
			"github.com/nats-io/nats-server/v2/server",
		},
	},
	{
		pkg: modPrefix + "internal/cluster",
		why: "L-2: raft stays confined and NATS-free. The audit publisher (which needs NATS) lives in " +
			"internal/broker and reads the cluster through the raft-free primitives. internal/cluster " +
			"does import internal/auth for the join-PoP verify, and auth pulls in nats-io/nkeys (crypto) " +
			"-- that is NOT the nats.go client, and the distinction is the whole point of this row.",
		bannedTransitive: []string{
			"github.com/nats-io/nats.go",
			"github.com/nats-io/nats.go/jetstream",
			"github.com/nats-io/nats-server/v2",
			modPrefix + "internal/broker",
			modPrefix + "internal/jsstream",
			modPrefix + "internal/clusternodes",
		},
		requiredTransitive: []string{
			"github.com/hashicorp/raft",
			modPrefix + "internal/auth",
		},
	},
	{
		pkg: modPrefix + "internal/jsstream",
		why: "R-23: ReplicasFor takes nVoters as a plain int, so the stream layer never learns about raft.",
		bannedTransitive: []string{
			modPrefix + "internal/cluster",
		},
		requiredTransitive: []string{
			"github.com/nats-io/nats.go/jetstream",
		},
	},
	{
		pkg: modPrefix + "internal/clusternodes",
		why: "R-3 / L-2: the home seam is a pure-SQL leaf. It survived D6 and D7 adding membership, and " +
			"it must keep surviving.",
		bannedTransitive: []string{
			"github.com/nats-io/nats.go",
			"github.com/nats-io/nats.go/jetstream",
			modPrefix + "internal/cluster",
			modPrefix + "internal/broker",
		},
		requiredTransitive: []string{"database/sql"},
	},
	{
		pkg: modPrefix + "internal/xferaudit",
		why: "A pure render/replay leaf: it returns *cluster.Command and owns the AuditTransfer Aux, " +
			"which is what keeps internal/schema out of internal/cluster. It must never reach NATS.",
		bannedTransitive: []string{"github.com/nats-io/nats.go"},
		requiredTransitive: []string{
			modPrefix + "internal/cluster",
			modPrefix + "internal/schema",
		},
	},
	{
		pkg: modPrefix + "internal/clusteroffline",
		why: "L-2 confines raft to internal/cluster. The offline tool goes through " +
			"cluster.RecoverSingleNode, never through raft itself. This is a DIRECT-import rule: the " +
			"package legitimately reaches raft transitively via internal/cluster.",
		bannedDirect:       []string{"github.com/hashicorp/raft"},
		requiredTransitive: []string{modPrefix + "internal/cluster"},
	},
	{
		pkg: modPrefix + "internal/broker",
		why: "L-2 confines raft to internal/cluster. cutover.go imports internal/cluster, not raft; the " +
			"mTLS transport and the Node both live in cluster.NewProduction. DIRECT-import rule for the " +
			"same reason as internal/clusteroffline.",
		bannedDirect:       []string{"github.com/hashicorp/raft"},
		requiredTransitive: []string{modPrefix + "internal/cluster"},
	},
}

// goListDeps returns the transitive import set of pkg.
func goListDeps(t *testing.T, root, pkg string) map[string]bool {
	t.Helper()
	return runGoList(t, root, pkg, "-deps", "{{.ImportPath}}")
}

// goListDirectImports returns only pkg's own import statements.
func goListDirectImports(t *testing.T, root, pkg string) map[string]bool {
	t.Helper()
	return runGoList(t, root, pkg, "", "{{join .Imports \"\\n\"}}")
}

func runGoList(t *testing.T, root, pkg, mode, format string) map[string]bool {
	t.Helper()
	args := []string{"list"}
	if mode != "" {
		args = append(args, mode)
	}
	args = append(args, "-f", format, pkg)
	cmd := exec.Command("go", args...)
	cmd.Dir = root
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go %s: %v", strings.Join(args, " "), err)
	}
	set := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			set[line] = true
		}
	}
	return set
}

// cacheKeyBallast makes this test's result depend on the CONTENT of the tree, so Go's test cache
// cannot hand back a stale PASS after an import edit.
//
// origin: line-2 external review F4 (deletions lane), independently reproduced by the severity lane.
// Every judgement here comes from `exec.Command("go", "list", ...)`, and a subprocess's output does not
// enter the test binary's testlog — so it is not part of the cache key. Measured: inject a real
// violation, run `go test ./test/architecture/ -run TestPackageLayering`, and Go replies `(cached)`
// GREEN. `-count=1` reddens, and the whole package reddens, but `-run <one gate>` is exactly the
// command a developer types to check the gate they just touched.
//
// It happened to be safe only by accident: a SIBLING test in this package (build_tags_test.go)
// ReadFile()s the whole tree, which put every .go file in the testlog and invalidated the package's
// cache entry for free. That is coupling, not a guarantee — deleting or narrowing that test would have
// silently reintroduced the stale PASS here. Reading the files this gate actually reasons about makes
// the dependency explicit and local.
//
// IT MUST COVER THE TRANSITIVE CLOSURE, NOT THE SIX RULED DIRECTORIES. origin: line-2 closure
// verification M11. The first version read only the packages named in layerRules, while five of the six
// rules judge `go list -deps` — the TRANSITIVE import set. So an import added in a package one hop away
// (internal/auth is the row's own documented near-miss edge: internal/cluster is allowed to reach it, and
// auth pulling in nats.go would violate the cluster row) changed the verdict without changing any file
// this function had read. `-run TestPackageLayering` then returned a cached PASS on a real violation, and
// the comment above claimed the dependency was explicit.
//
// The set comes from `go list -deps` on each ruled package, filtered to this module — i.e. exactly the
// files whose content the gate's judgement depends on. Reading more than the six directories is the
// point; reading the whole tree would work too but would put this gate's cache key at the mercy of every
// unrelated edit, which is the coupling the comment above objects to.
func cacheKeyBallast(t *testing.T, root string) {
	t.Helper()

	// Collect every in-module package the ruled packages transitively depend on, plus the ruled ones.
	dirs := map[string]bool{}
	for _, rule := range layerRules {
		dirs[strings.TrimPrefix(rule.pkg, modPrefix)] = true
		for dep := range goListDeps(t, root, rule.pkg) {
			if strings.HasPrefix(dep, modPrefix) {
				dirs[strings.TrimPrefix(dep, modPrefix)] = true
			}
		}
	}
	if len(dirs) <= len(layerRules) {
		t.Fatalf("cache ballast covers %d package(s) for %d rules — `go list -deps` returned nothing "+
			"in-module, so the ballast is back to the six ruled directories and a violation one hop away "+
			"would again return a cached PASS", len(dirs), len(layerRules))
	}

	var names []string
	for d := range dirs {
		names = append(names, d)
	}
	sort.Strings(names)
	for _, rel := range names {
		dir := filepath.Join(root, rel)
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("cache ballast: readdir %s: %v", dir, err)
		}
		for _, e := range entries {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") {
				continue
			}
			// The READ is the point: os.ReadFile is what testlog records. The bytes are discarded.
			if _, err := os.ReadFile(filepath.Join(dir, e.Name())); err != nil {
				t.Fatalf("cache ballast: read %s/%s: %v", dir, e.Name(), err)
			}
		}
	}
}

func TestPackageLayering(t *testing.T) {
	root := repoRoot(t)
	cacheKeyBallast(t, root)
	for _, rule := range layerRules {
		t.Run(strings.TrimPrefix(rule.pkg, modPrefix), func(t *testing.T) {
			var deps map[string]bool
			if len(rule.bannedTransitive) > 0 || len(rule.requiredTransitive) > 0 {
				deps = goListDeps(t, root, rule.pkg)
			}
			for _, req := range rule.requiredTransitive {
				if !deps[req] {
					t.Fatalf("self-check FAILED: %s does not depend on %q.\n"+
						"Either the go list query broke (in which case every ban below passed vacuously) or "+
						"this edge really went away and the row needs updating.\nRule: %s",
						rule.pkg, req, rule.why)
				}
			}
			// PREFIX match, not an exact map lookup. A banned entry names a module or package ROOT and
			// what shows up in `go list -deps` are its subpackages: nothing in the tree has an import
			// path exactly equal to "github.com/nats-io/nats-server/v2", so an exact lookup on that
			// string could never fire. That is not hypothetical — the clause below sat here as an exact
			// key for the whole life of the four files this table replaced, and a review lane proved it
			// dead by importing the entire embedded NATS server into internal/cluster (16 nats-server
			// packages in deps) with the gate still green. Worse, TestLayeringRulesAreWellFormed was
			// asserting the string set verbatim, so the dead clause had been CERTIFIED as "the union is
			// preserved".
			//
			// Prefix matching makes a root-path ban mean what it reads as. `dep == banned` is kept for
			// the case where the banned path is itself an importable package (internal/broker etc.).
			for _, banned := range rule.bannedTransitive {
				for dep := range deps {
					if dep == banned || strings.HasPrefix(dep, banned+"/") {
						t.Errorf("%s transitively imports %q (via %q) — layering violated.\n%s",
							rule.pkg, banned, dep, rule.why)
						break
					}
				}
			}
			if len(rule.bannedDirect) > 0 {
				direct := goListDirectImports(t, root, rule.pkg)
				for _, banned := range rule.bannedDirect {
					if direct[banned] {
						t.Errorf("%s DIRECTLY imports %q — layering violated.\n%s", rule.pkg, banned, rule.why)
					}
				}
			}
		})
	}
}

// TestLayeringRulesAreWellFormed keeps the table itself honest. A row with no bans asserts nothing; a
// row whose required set is empty has no self-check, so a broken `go list` would let its bans pass
// silently; and a package listed twice means one of the two rows is being maintained by nobody.
// originalUnion is what the four deleted regression files asserted BETWEEN them, per package. It is the
// merge's receipt, kept as data so the claim "no clause was lost" is re-checkable mechanically instead of
// by redoing the hand comparison.
//
// origin: line-2 external review M9 / IDG-7 / PF-12. The first version of this guard had a lower bound of
// `< 5` on a six-row table and full-equality on ONE row (internal/cluster). Both holes were measured: a
// lane deleted the entire internal/xferaudit row (all of the old TestD8XferauditIsLeaf [deleted]) leaving
// five rows, and separately deleted "nats.go/jetstream" from the internal/clusternodes row (a clause copied
// verbatim out of TestD6ClusternodesNoNATSNoCluster [deleted]) — BOTH mutations left every test green.
// The merge itself was
// clean; the guard against the NEXT merge was not, and the entire argument for merging four narrow files
// into one table was that a table cannot quietly lose an assertion.
//
// TEST-NAME MAP — plan §5 asked for this in the file's comments and it was not written. Ten Test functions
// across four files became six rows. It lives in `deletedRegressionTests` below rather than in prose,
// because prose is how the first version of this map came to be FABRICATED.
//
// THE FIRST VERSION OF THIS MAP WAS INVENTED. Eight of its ten names never existed in this repository. It
// was written from memory of what the tests probably were called, it read as authoritative, and its whole
// stated purpose — "so someone reading a d5/d6/d7/d8 review report can find where the assertion went" — was
// therefore inverted: a reader grepping those names finds nothing and concludes the assertion is gone.
//
// Worse, the `[deleted]` marker invented in the same change made promised_guard_test.go structurally unable
// to notice, because that marker's whole job is to say "do not check whether this test exists". A mechanism
// for suppressing a check was paired with an unverifiable claim, in the same commit, by the same author.
// The closure verification caught it with one command: `git log --all -S<name>`.
//
// So the map is now DATA, and TestDeletedRegressionTestNamesAreReal checks every name against
// `git show HEAD:<file>`. The marker still suppresses promised_guard — it has to, the tests really are gone
// — but the claim it suppresses is now asserted somewhere else instead of merely asserted by me.
//
// The assertion below is ⊇, not equality: adding a NEW ban to a row is an improvement and must not fail.
var originalUnion = map[string][]string{
	"internal/cluster": {
		"github.com/nats-io/nats.go",
		"github.com/nats-io/nats.go/jetstream",
		"github.com/nats-io/nats-server/v2",
		modPrefix + "internal/broker",
		modPrefix + "internal/jsstream",
		modPrefix + "internal/clusternodes",
	},
	"internal/jsstream":  {modPrefix + "internal/cluster"},
	"internal/xferaudit": {"github.com/nats-io/nats.go"},
	"internal/clusternodes": {
		"github.com/nats-io/nats.go",
		"github.com/nats-io/nats.go/jetstream",
		modPrefix + "internal/cluster",
		modPrefix + "internal/broker",
	},
	"internal/clusteroffline": {"github.com/hashicorp/raft"},
	"internal/broker":         {"github.com/hashicorp/raft"},
	// Added 2026-09-01 (test-system overhaul, internal review L5-F5), inherited from no deleted file:
	// the rule lived only in prose (plan §2.3) until then. Empty on purpose — the receipt records
	// what the four regression files asserted, and they asserted nothing about testharness.
	"internal/testharness": {},
}

func TestLayeringRulesAreWellFormed(t *testing.T) {
	if len(layerRules) != len(originalUnion) {
		t.Fatalf("%d layering rules but originalUnion records %d packages — a row was added without "+
			"recording what it inherited, or a row was LOST in a merge. The four regression files this "+
			"table replaced asserted boundaries on %d distinct packages.",
			len(layerRules), len(originalUnion), len(originalUnion))
	}
	seen := map[string]bool{}
	for _, rule := range layerRules {
		name := strings.TrimPrefix(rule.pkg, modPrefix)
		if seen[rule.pkg] {
			t.Errorf("%s appears twice — merge the rows, or one of them will drift unmaintained", name)
		}
		seen[rule.pkg] = true
		if len(rule.bannedTransitive) == 0 && len(rule.bannedDirect) == 0 {
			t.Errorf("%s bans nothing — the row asserts no boundary at all", name)
		}
		if len(rule.requiredTransitive) == 0 {
			t.Errorf("%s has no requiredTransitive self-check — a broken go list would make its bans "+
				"pass vacuously", name)
		}
		if strings.TrimSpace(rule.why) == "" {
			t.Errorf("%s has no `why` — the failure message would tell the next person what broke but "+
				"not what the boundary is for", name)
		}
		if !strings.HasPrefix(rule.pkg, modPrefix) {
			t.Errorf("%s is not a package of this module", rule.pkg)
		}
	}
	// UNION PRESERVATION, for EVERY row rather than one.
	byPkg := map[string]*layerRule{}
	for i := range layerRules {
		byPkg[strings.TrimPrefix(layerRules[i].pkg, modPrefix)] = &layerRules[i]
	}
	for name, inherited := range originalUnion {
		rule, ok := byPkg[name]
		if !ok {
			t.Errorf("originalUnion records %d clause(s) inherited for %s, but the table has no row for it. "+
				"See the TEST-NAME MAP above for which deleted test asserted them.", len(inherited), name)
			continue
		}
		bans := map[string]bool{}
		for _, b := range rule.bannedTransitive {
			bans[b] = true
		}
		for _, b := range rule.bannedDirect {
			bans[b] = true
		}
		var missing []string
		for _, want := range inherited {
			if !bans[want] {
				missing = append(missing, want)
			}
		}
		sort.Strings(missing)
		if len(missing) > 0 {
			t.Errorf("the %s row no longer bans %d clause(s) it inherited from the deleted regression "+
				"files:\n  %s\n\nThe merge's whole justification was that a table cannot quietly lose an "+
				"assertion four narrow files were each holding. Restore the clause, or — if the boundary "+
				"genuinely changed — delete it from originalUnion in the same commit and say why.",
				name, len(missing), strings.Join(missing, "\n  "))
		}
	}
}

// deletedRegressionTests is the TEST-NAME MAP: which deleted regression test each merged layerRules row
// came from. Keyed by the file it lived in, so a reader holding a d5/d6/d7/d8 review report can find where
// its assertion went.
//
// The names live in STRING LITERALS, not in a comment, so promised_guard_test.go does not read them as
// promises and no suppression marker is needed — which is the point. The prose version of this map needed
// ten `[deleted]` markers, and those markers are what let eight fabricated names sit unchallenged. Data can
// be reconciled; prose can only be believed. TestDeletedRegressionTestNamesAreReal does the reconciling.
var deletedRegressionTests = map[string][]struct{ name, pkg string }{
	"test/d5/regression_test.go": {
		{"TestD5ClusterNoNATSImport", "internal/cluster"},
		{"TestD5JsstreamNoClusterImport", "internal/jsstream"},
	},
	"test/d6/regression_test.go": {
		{"TestD6ClusternodesNoNATSNoCluster", "internal/clusternodes"},
		{"TestD6ClusterStillNoNATS", "internal/cluster"},
	},
	"test/d7/regression_test.go": {
		{"TestD7ClusternodesStaysLeaf", "internal/clusternodes"},
		{"TestD7ClusterStaysNATSFreeAfterAuthEdge", "internal/cluster"},
		// This one asserted a DIRECT-import rule over two packages at once, which is why the merge produced
		// two rows from it (internal/clusteroffline and internal/broker) rather than one.
		{"TestD7RaftConfinedToCluster", "internal/clusteroffline"},
		{"TestD7RaftConfinedToCluster", "internal/broker"},
	},
	"test/d8/regression_test.go": {
		{"TestD8ClusterStaysNATSFree", "internal/cluster"},
		{"TestD8JsstreamStaysClusterFree", "internal/jsstream"},
		{"TestD8XferauditIsLeaf", "internal/xferaudit"},
	},
}

// TestDeletedRegressionTestNamesAreReal reconciles the TEST-NAME MAP against git history.
//
// origin: line-2 closure verification, M9's second half. This is the check whose absence let the first
// version of the map be fabricated — eight invented names, in a map whose purpose is to be greppable,
// behind a marker that told the promise-checker to look away.
//
// IT READS A FROZEN COMMIT, NOT HEAD. origin: line-2 INDEPENDENT EXTERNAL REVIEW B1 (BLOCKER).
//
// The first version ran `git show HEAD:test/d5/regression_test.go`. In the uncommitted working tree that
// happened to work, because HEAD still contained the four files being deleted. The moment this increment
// is committed, HEAD no longer has them: all four `git show` calls exit 128, `checked` drops from 11 to 0,
// and the gate fails on every subsequent checkout and every CI run.
//
// So `make gates` was green only in the instant before the commit. The comment that used to sit here even
// said the gate would expire — and then shipped it as a release gate anyway, which is worse than not
// noticing: it is a defect that was seen, written down, and delivered.
//
// deletedRegressionTestsCommit is the last commit that contains all four paths. It is an ANCESTOR of every
// future HEAD, so it stays reachable. If a shallow clone cannot reach it the gate says so explicitly rather
// than silently verifying nothing.
const deletedRegressionTestsCommit = "0b1ec070e68e302a24b9b449823953a3c545102a"

func TestDeletedRegressionTestNamesAreReal(t *testing.T) {
	root := repoRoot(t)

	// The defect encoded as an assertion: this gate must not depend on the working tree's commit state.
	// If someone "simplifies" the ref back to HEAD, this fires immediately rather than after the commit.
	if deletedRegressionTestsCommit == "HEAD" {
		t.Fatal("deletedRegressionTestsCommit is HEAD — that is external review B1 exactly: the four paths " +
			"exist at HEAD only while their deletion is uncommitted, so the gate self-destructs on commit")
	}
	head, herr := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if herr == nil && strings.TrimSpace(string(head)) == deletedRegressionTestsCommit {
		t.Errorf("deletedRegressionTestsCommit equals the current HEAD (%s).\n\n"+
			"That is the state B1 described: it works now and breaks the moment anything is committed. "+
			"Freeze the last commit that CONTAINS the four paths instead — `git rev-list -1 HEAD -- "+
			"test/d5/regression_test.go`.", deletedRegressionTestsCommit)
	}
	// And the other half of the premise: the files must be GONE from the working tree. If they came back,
	// this gate is verifying history that is no longer history.
	for rel := range deletedRegressionTests {
		if _, err := os.Stat(filepath.Join(root, rel)); err == nil {
			t.Errorf("%s exists in the working tree, but deletedRegressionTests records it as deleted — "+
				"either the merge was reverted or this map is describing the wrong thing", rel)
		}
	}

	// Map every layerRules row to the packages it covers, so the map's `pkg` column is checked too.
	ruled := map[string]bool{}
	for _, r := range layerRules {
		ruled[strings.TrimPrefix(r.pkg, modPrefix)] = true
	}

	checked := 0
	for file, entries := range deletedRegressionTests {
		cmd := exec.Command("git", "show", deletedRegressionTestsCommit+":"+file)
		cmd.Dir = root
		out, err := cmd.Output()
		if err != nil {
			t.Errorf("git show %s:%s failed (%v).\n\n"+
				"This gate reads the deleted files from a FROZEN ancestor commit. A failure here means that "+
				"commit is unreachable — most likely a shallow clone (`git fetch --unshallow`), or the SHA was "+
				"edited to something that does not contain these paths.", deletedRegressionTestsCommit, file, err)
			continue
		}
		src := string(out)
		for _, e := range entries {
			checked++
			if !strings.Contains(src, "func "+e.name+"(") {
				t.Errorf("deletedRegressionTests names %s as having lived in %s, but that file at commit %s "+
					"contains no such function.\n\n"+
					"An INVENTED provenance entry is worse than no map: its whole purpose is that a reader "+
					"of an old review report can grep the name and find where the assertion went. Get the "+
					"real name from `git show %s:%s | grep '^func Test'`.",
					e.name, file, deletedRegressionTestsCommit, deletedRegressionTestsCommit, file)
			}
			if !ruled[e.pkg] {
				t.Errorf("deletedRegressionTests maps %s to package %q, which has no row in layerRules — "+
					"either the package name is wrong or the row it became was lost", e.name, e.pkg)
			}
		}
	}

	// Non-vacuity, both directions. The map must cover every deleted test, and the count is pinned so a
	// silently shrinking map cannot pass.
	const deletedTestAssertions = 11 // 10 distinct functions; TestD7RaftConfinedToCluster [deleted] covers 2 pkgs
	if checked != deletedTestAssertions {
		t.Errorf("checked %d name→package assertions, expected %d — the map shrank, grew, or a `git show` "+
			"failed silently above", checked, deletedTestAssertions)
	}
}
