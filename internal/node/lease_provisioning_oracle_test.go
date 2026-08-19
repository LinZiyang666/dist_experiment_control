package node

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §1.1 I1 (single-instance
// invariance).
//
// ProvisionedNIDs' own docstring promises: "On error it returns nil, so every
// name reads as provisioned and nothing is reported leased. That direction is
// deliberate: reporting a real device as ephemeral would silently drop it from a
// fleet upgrade." internal/broker/exec.go repeats the claim ("A lookup failure
// degrades to 'everything is provisioned'").
//
// A nil map does the OPPOSITE. handleNodeListReq computes
// `leased := looksLeased && !provisioned[nid]`, and a lookup on a nil map yields
// false, so the empty result reads as "NOTHING is provisioned" — every real
// device named `gpu-02` / `worker-07` is reported leased and silently dropped
// from `tether node upgrade --all`. The same empty set is the NORMAL state of a
// single-mode broker started without --auth-callout-seeds-dir (cmd/tether/serve.go:340
// documents "empty = off in single mode"), where agent_provisioning is never
// written at all: agentprov.ProvisionWithPIN in internal/authcallout is its only
// writer.
func TestProvisionedNIDsSignalsUnknownRatherThanEmptyOnFailure(t *testing.T) {
	db, err := storage.Open(t.TempDir() + "/state.db")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	// A store failure. The docstring says this must read as "everything is
	// provisioned".
	if _, err := db.Exec(`DROP TABLE agent_provisioning`); err != nil {
		t.Fatal(err)
	}
	got, known := ProvisionedNIDs(db, "lab")

	// This is the exact expression internal/broker/exec.go evaluates per row.
	// `known` is what makes the failure distinguishable from "nothing is
	// provisioned" — without it a nil map reports every real device as leased.
	// looksLeased is true for "gpu-02"; spelled out rather than inlined as a
	// literal so the expression matches internal/broker/exec.go's shape exactly.
	looksLeased := true
	if _, _, l := proto.SplitLeaseName("gpu-02"); !l {
		looksLeased = false
	}
	leased := known && looksLeased && !got["gpu-02"]
	if leased {
		t.Fatalf("a lookup failure reported the real device \"gpu-02\" as LEASED (map=%v).\n"+
			"ProvisionedNIDs returns a nil map, and `!provisioned[nid]` on a nil map is TRUE, so "+
			"the failure degrades to \"nothing is provisioned\" — the direction the docstring and "+
			"internal/broker/exec.go both say is the unacceptable one, because it silently drops a "+
			"real device from `node upgrade --all`. The same empty set is the steady state of any "+
			"single-mode broker running without auth_callout, where agent_provisioning is never "+
			"written.", got)
	}
}
