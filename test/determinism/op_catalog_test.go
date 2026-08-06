package determinism

import (
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// op_catalog_test.go — the §5 raft-op catalog in the LAYER-2 authority doc must
// name every op the FSM actually knows.
//
// origin: docs/reviews/h1-external-review.md F5. h1 added OpProcGC/OpPortGC as
// real replicated ops, and the catalog in
// docs/distributed-broker-architecture.md not only missed them, it stated the
// OPPOSITE ("ProcGC = leader-local, 不进 Raft"). Per CLAUDE.md §1 that document
// outranks the code as the binding contract, so the tree shipped with a
// yardstick that measured backwards: anyone reasoning about rolling upgrades,
// log replay or FSM determinism from the doc would have concluded these
// deletions never enter the log.
//
// A missing catalog line is not cosmetic for exactly that reason, and "someone
// will notice" is what failed here — hence a gate rather than another comment.
//
// DIRECTION AND ITS LIMITS
// ------------------------
// This asserts ONE direction: every cluster.knownOps entry appears in the
// catalog paragraph. The reverse (no catalog entry without a live op) is
// deliberately NOT asserted: the paragraph legitimately names ops that were
// moved out of v1 (AllocateProxy/ProxyGenAdvance with P13) and membership
// paths that are not FSM ops at all (raft.AddVoter). Encoding those exceptions
// would make the gate a second, competing catalog. The direction that bit us
// is the one guarded.
func TestOpCatalogNamesEveryKnownOp(t *testing.T) {
	const docPath = "../../docs/distributed-broker-architecture.md"
	raw, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("read %s: %v", docPath, err)
	}
	doc := string(raw)

	// Scope the search to §5 (the op-set section) so an op named only in an
	// unrelated narrative paragraph does not count as "catalogued".
	start := strings.Index(doc, "## 5. Raft op 集")
	if start < 0 {
		t.Fatal("§5 (Raft op 集) heading not found — the catalog moved or was renamed; " +
			"point this gate at its new home rather than deleting it")
	}
	end := strings.Index(doc[start+1:], "\n## ")
	section := doc[start:]
	if end > 0 {
		section = doc[start : start+1+end]
	}

	// The doc spells ops in backticks, several to a span, and uses a
	// SHARED-PREFIX shorthand a human reads without thinking:
	// `SessionCreate/Tombstone/HardDelete` names three ops, and
	// `ClusterNodeUpsert/Phase/Remove` three more. Expand it the same way:
	// for every later segment, try each prefix of the FIRST segment in front
	// of it (Session + Tombstone → SessionTombstone).
	named := map[string]bool{}
	for _, m := range regexp.MustCompile("`([^`]+)`").FindAllStringSubmatch(section, -1) {
		segs := strings.Split(m[1], "/")
		for i, seg := range segs {
			seg = strings.TrimSpace(seg)
			named[seg] = true
			if i == 0 {
				continue
			}
			head := strings.TrimSpace(segs[0])
			for c := 1; c < len(head); c++ {
				named[head[:c]+seg] = true
			}
		}
	}

	var missing []string
	for op := range cluster.KnownOpsForDocs() {
		name := string(op)
		// The doc drops the "Op" prefix (OpProcGC → ProcGC); accept either.
		if named[name] || named[strings.TrimPrefix(name, "Op")] {
			continue
		}
		missing = append(missing, name)
	}
	sort.Strings(missing)

	// catalogBacklog: ops that were ALREADY undocumented before h1 — added by
	// D5/D7/D8/C5 without a catalog line. h1 did not create this drift and does
	// not close it: documenting 14 ops means describing each one's guard and
	// N-1 constraint correctly, which is real work on code this increment did
	// not touch, and a wrong line in the layer-2 authority is worse than a
	// missing one.
	//
	// This is a SHRINK-ONLY ledger, the repo's standard shape for that
	// situation: an entry that is no longer missing must be DELETED here in the
	// same change, so the exemption cannot rot into permanent cover. What the
	// gate protects TODAY is that no NEW op joins the list — which is exactly
	// how OpProcGC/OpPortGC slipped in with the catalog actively contradicting
	// them (external review F5).
	catalogBacklog := map[string]bool{
		// Keys are OpType VALUES (no "Op" prefix) — the same strings the raft
		// log carries.
		"AuditCheckpointSet": true, "ClusterBusNkeySet": true, "ClusterCertRotate": true,
		"ClusterDrainSet": true, "ClusterMetaClear": true, "ClusterNodeReaddr": true,
		"ClusterNodeRoute": true, "ClusterOpConfirm": true, "ClusterOpStart": true,
		"ClusterOpTransition": true, "ClusterSeedsPublish": true, "ProxyAllocate": true,
		"ProxySetEnabled": true, "ProxySubCreate": true, "ProxySubRevoke": true,
		"TransferAudit": true,
	}
	var undeclared, staleLedger []string
	for _, name := range missing {
		if !catalogBacklog[name] {
			undeclared = append(undeclared, name)
		}
	}
	missingSet := map[string]bool{}
	for _, name := range missing {
		missingSet[name] = true
	}
	for name := range catalogBacklog {
		if !missingSet[name] {
			staleLedger = append(staleLedger, name)
		}
	}
	sort.Strings(staleLedger)

	if len(undeclared) > 0 {
		t.Errorf("%d replicated op(s) are missing from the §5 catalog in %s:\n  %s\n\n"+
			"That document is the layer-2 authority (CLAUDE.md §1): an op the FSM applies but the "+
			"catalog omits — or worse, contradicts — sends the next reader's rolling-upgrade and "+
			"log-replay reasoning the wrong way. Add the op with its guard/keyset semantics and its "+
			"N-1 constraint, do not just append a name.",
			len(undeclared), docPath, strings.Join(undeclared, "\n  "))
	}
	if len(staleLedger) > 0 {
		t.Errorf("%d catalogBacklog entr(ies) are now documented — DELETE them from the ledger in "+
			"this same change:\n  %s\n\nA shrink-only ledger that keeps entries it no longer needs "+
			"is just a permanent exemption wearing a deadline.",
			len(staleLedger), strings.Join(staleLedger, "\n  "))
	}
}
