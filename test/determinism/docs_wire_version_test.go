package determinism

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// docs_wire_version_test.go — the docs must not state a wire version the code no longer speaks.
//
// This is the documentation half of TestNoStrayVersionLiteral (same file tree, same concern, one
// directory over in this same package): that one scans Go STRING LITERALS under internal/ and cmd/ and
// says the subject prefix has exactly one source of truth. This one scans the operator-facing markdown
// and says the same thing about what we TELL people the subjects are. A doc that says tether.v1 while
// ProtoVersion is 2 is not a typo — it is a yardstick that measures wrong, and CLAUDE.md §1 puts the
// docs above the code in the authority chain, so a wrong doc outranks the truth until someone notices.
//
// WHY THE PATTERN IS NARROW
// -------------------------
// The obvious detector -- grep for `tether.v1` -- reports 74 lines and is unusable, because most of
// them are legitimate:
//
//	docs/usage.md:16       "wire 协议变更走 `tether.v1.*` → `tether.v2.*` 的整次跨版本路径"
//	CLAUDE.md:77           same migration-path idiom
//	batch-a-roadmap.md     was REPORTING this very defect, quoting the literal (it has since been
//	                       archived into docs/reviews/, which this scan does not cover)
//
// Those describe a MIGRATION, in which naming the old version is the entire point. What is actually
// wrong is a doc asserting a stale subject PATH as current. So the pattern requires a letter after the
// version dot -- `tether.v1.session…` matches, `tether.v1.*` does not -- and the three legitimate
// narratives fall out for free. Zero false positives, zero whitelist, zero doc edits: the earlier plan
// of fencing §A–§K behind an HTML comment turned out to be unnecessary once the detector stopped
// conflating "mentions v1" with "claims v1".
//
// WHY ARCHITECTURE.MD IS A COUNT AND NOT AN EXEMPTION
// ---------------------------------------------------
// docs/architecture.md is layer 3 of the authority chain — "只提供当初为何这样取舍；标识符与拓扑细节已过时,
// 不作实现依据". Its 69 v1 subject paths are correct AS HISTORY. The tempting move is to exempt the file.
// That is strictly weaker than what is here: an exemption only ever permits, so nothing would stop the
// file from growing new stale claims — and it demonstrably was growing them (the same measurement was
// 120 four days before this gate landed and 127 after). A COUNT fails in both directions: growth means
// a new stale claim was added, shrinkage means someone modernised a passage and the ledger owes an
// update. The number is the gate; the file is not excused.
var wireSubjectPathRe = regexp.MustCompile(`tether\.v(\d+)\.[a-zA-Z]`)

// historicalWireDocs maps a layer-3 historical document to the exact number of stale subject paths it
// is known to carry. Both directions of drift fail — see the header.
var historicalWireDocs = map[string]int{
	"docs/architecture.md": 69,
}

// docsWireScanRoots are the operator-facing trees. docs/reviews/** is deliberately NOT scanned: those
// are frozen phase records — a d3 plan written against proto v1 is an accurate record of what was true
// then, and rewriting history to satisfy a linter would destroy the only account of why v2 happened.
func docsWireScanFiles(t *testing.T, root string) []string {
	t.Helper()
	var out []string
	// Top-level markdown (CLAUDE.md, README.md, ...).
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("readdir %s: %v", root, err)
	}
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			out = append(out, e.Name())
		}
	}
	// docs/*.md, one level only.
	docEntries, err := os.ReadDir(filepath.Join(root, "docs"))
	if err != nil {
		t.Fatalf("readdir docs: %v", err)
	}
	for _, e := range docEntries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".md") {
			out = append(out, "docs/"+e.Name())
		}
	}
	// Authority-chain documents that live deeper than one level. origin: line-2 closure verification §6 B7.
	// The two loops above cover "repo root, one level" and "docs/, one level", which reads like the whole
	// documentation surface and is not: CLAUDE.md §1's authority table lists test/simcluster/README.md as
	// the authority for the deploy-tier drill Mandate, and it was outside the scan. A wire-version claim in
	// a document CLAUDE.md points people at is exactly as misleading as one in docs/.
	//
	// An explicit list rather than a recursive walk: a walk would pull in docs/reviews/ (368 frozen review
	// reports, all legitimately full of historical v1 paths) and the gate would drown. If a third such
	// document appears, it is one line here — and §1's table is where to look for it.
	for _, rel := range []string{"test/simcluster/README.md"} {
		if _, err := os.Stat(filepath.Join(root, rel)); err == nil {
			out = append(out, rel)
		} else {
			t.Errorf("docsWireScanFiles names %s, which does not exist. Either it moved (update this list) "+
				"or it was deleted (drop the entry) — a scan list naming a missing file quietly shrinks the "+
				"gate's coverage while looking complete.", rel)
		}
	}
	sort.Strings(out)
	return out
}

// scanWireVersions returns, per file, the version numbers found in real subject paths.
func scanWireVersions(t *testing.T, root string, files []string) map[string][]int {
	t.Helper()
	out := map[string][]int{}
	for _, rel := range files {
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatalf("read %s: %v", rel, err)
		}
		for _, m := range wireSubjectPathRe.FindAllSubmatch(b, -1) {
			n, cerr := strconv.Atoi(string(m[1]))
			if cerr != nil {
				t.Fatalf("%s: unparseable version in %q", rel, m[0])
			}
			out[rel] = append(out[rel], n)
		}
	}
	return out
}

func TestDocsUseCurrentWireVersion(t *testing.T) {
	root := repoRoot(t)
	files := docsWireScanFiles(t, root)
	found := scanWireVersions(t, root, files)

	for _, rel := range files {
		versions := found[rel]
		if want, historical := historicalWireDocs[rel]; historical {
			// Count only the STALE ones. origin: line-2 external review m6 / GI-10 / PI-10. The ratchet
			// counted every subject path regardless of version, so adding a CORRECT `tether.v2.*` line to a
			// historical document reported "carries 70 stale wire-subject path(s)" — a message that is
			// false about the thing it just counted, and which asks the author to undo an improvement. The
			// ledger's purpose is to stop v1 claims accumulating; a v2 claim is not one.
			var stale []int
			for _, v := range versions {
				if v != proto.ProtoVersion {
					stale = append(stale, v)
				}
			}
			versions = stale
			if len(versions) != want {
				// Print only the branch that happened. Emitting both and letting the reader pick is how a
				// failure message turns into a wall to skim past; the direction is known here, so say it.
				var guidance string
				if len(versions) > want {
					guidance = fmt.Sprintf("GREW by %d: a passage was added or edited that states a subject "+
						"path this code no longer\n        speaks. Fix the passage — do NOT raise the number "+
						"to match it.", len(versions)-want)
				} else {
					guidance = fmt.Sprintf("SHRANK by %d: a passage was modernised. Lower the number in "+
						"historicalWireDocs so the\n        win is locked in and cannot decay back into slack.",
						want-len(versions))
				}
				t.Errorf("%s carries %d stale wire-subject path(s), the ledger says %d.\n"+
					"  %s\n"+
					"  This file is layer 3 of the authority chain (historical rationale only), which is why\n"+
					"  its v1 paths are tolerated AS A FIXED COUNT rather than exempted outright: an\n"+
					"  exemption would let it keep accumulating stale claims, which is what it was doing.",
					rel, len(versions), want, guidance)
			}
			continue
		}
		var stale []string
		for _, v := range versions {
			if v != proto.ProtoVersion {
				stale = append(stale, fmt.Sprintf("tether.v%d.*", v))
			}
		}
		if len(stale) > 0 {
			t.Errorf("%s states %d subject path(s) at the wrong wire version: %s (proto.ProtoVersion = %d).\n"+
				"  The docs outrank the code in CLAUDE.md §1's authority chain, so a stale path here is not a\n"+
				"  typo — it is a yardstick that measures wrong. Update the passage, or, if it is describing a\n"+
				"  MIGRATION rather than the current wire, write it as `tether.v1.*` → `tether.v2.*` (the\n"+
				"  detector requires a letter after the version dot precisely so migration prose is not\n"+
				"  mistaken for a claim).",
				rel, len(stale), strings.Join(stale, ", "), proto.ProtoVersion)
		}
	}
}

// TestDocsWireVersionScannerIsNonVacuous proves the scan can see, and can discriminate. Without it a
// green TestDocsUseCurrentWireVersion could equally mean "the docs are correct" or "the pattern
// matches nothing", and the latter is the more likely way to end up green.
func TestDocsWireVersionScannerIsNonVacuous(t *testing.T) {
	root := repoRoot(t)
	files := docsWireScanFiles(t, root)
	// NAMED, not counted. origin: line-2 closure verification Q3. The floor was `len(files) < 10` over a
	// scope of 15, i.e. five files of slack in a walker whose failure mode is silently visiting fewer
	// directories. A count cannot say WHICH files it lost, and the ones that matter are few and known: the
	// authority chain's own documents. If the walker stops reaching docs/, `len(files)` still clears 10 on
	// the repo root alone.
	for _, must := range []string{
		"CLAUDE.md",                 // the rules every session loads
		"docs/architecture.md",      // authority layer 3, and the ledgered historical doc
		"docs/usage.md",             // the operator-facing wire surface
		"test/simcluster/README.md", // added to scope by §6 B7; the one entry outside the two loops
	} {
		found := false
		for _, f := range files {
			if f == must {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("%s is not in the scan scope (%d files found). Every wire-version claim in it is "+
				"unchecked, and a count-based floor would not have said which file went missing.",
				must, len(files))
		}
	}
	found := scanWireVersions(t, root, files)
	if len(found) == 0 {
		t.Fatal("the scanner found NO wire subject paths in any doc — it is broken; " +
			"docs/architecture.md alone has 69")
	}

	// It must MATCH a real subject path...
	if m := wireSubjectPathRe.FindStringSubmatch("subscribe to tether.v2.session.foo now"); m == nil {
		t.Error("pattern failed to match a real subject path")
	} else if m[1] != "2" {
		t.Errorf("pattern captured version %q, want \"2\"", m[1])
	}
	// ...and must NOT match the migration idiom, which is the entire reason it is narrow. If this
	// stops holding, three legitimate passages start failing and someone will "fix" the docs by
	// deleting the migration story.
	for _, prose := range []string{
		"wire 协议变更走 `tether.v1.*` → `tether.v2.*` 的整次跨版本路径",
		"任何破坏性 wire 变更走整次跨版本路径（`tether.v1.*` → `v2.*`）",
	} {
		for _, m := range wireSubjectPathRe.FindAllStringSubmatch(prose, -1) {
			if m[1] != strconv.Itoa(proto.ProtoVersion) {
				t.Errorf("pattern matched %q inside migration prose — narrow it back down:\n  %s", m[0], prose)
			}
		}
	}
	// And the ledger must name a file that exists and actually carries paths.
	for rel := range historicalWireDocs {
		if len(found[rel]) == 0 {
			t.Errorf("historicalWireDocs names %s but the scan found no subject paths in it — "+
				"the entry is stale, or the path is wrong", rel)
		}
	}
}
