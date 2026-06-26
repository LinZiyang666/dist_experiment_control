package d7_test

import (
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
)

// external_review_test.go contains reviewer regressions for D7 contracts that are
// advertised by the architecture/plan but not implemented by the current tree.

func TestD7ReviewTransferLeaderUsesRequestedTarget(t *testing.T) {
	src := readFile(t, filepath.Join(moduleRoot(t), "internal/broker/clusterstatus.go"))
	if strings.Contains(src, "case adminsock.OpClusterTransfer:\n\t\t\tif err := b.admin.node.TransferLeadership();") {
		t.Fatal("cluster transfer-leader <node-id> ignores node_id and calls untargeted TransferLeadership")
	}
	if !strings.Contains(src, "LeadershipTransferToServer") {
		t.Fatal("cluster transfer-leader should route to LeadershipTransferToServer(req.NodeID, addr)")
	}
}

func TestD7ReviewRotateTunnelCertImplemented(t *testing.T) {
	src := readFile(t, filepath.Join(moduleRoot(t), "internal/broker/clusterstatus.go"))
	if strings.Contains(src, "rotate-tunnel-cert: harness-driven") {
		t.Fatal("cluster rotate-tunnel-cert is advertised as D7 scope but the backend is a fixed stub")
	}
}

func TestD7ReviewStatusJSONIsVersioned(t *testing.T) {
	b, err := json.Marshal(d7Fixtures()[0].report)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	if !strings.Contains(string(b), `"schema_version"`) {
		t.Fatalf("cluster status --json lacks the documented schema_version field: %s", string(b))
	}
}

func TestD7ReviewRunbookRecoverMatchesCLI(t *testing.T) {
	src := readFile(t, filepath.Join(moduleRoot(t), "docs/cluster-runbook.md"))
	if strings.Contains(src, "Type WIPE to confirm") {
		t.Fatal("runbook still tells operators to type WIPE, but CLI requires typed --self-id")
	}
	// C8: the recovery verb moved to `cluster recovery rejoin prepare`. The runbook must present the
	// NEW primary spelling WITH --self-id (not the deleted bare `cluster recover`), so this guard tracks
	// the real CLI rather than passing vacuously on an absent old string.
	if strings.Contains(src, "tether cluster recover ") && !strings.Contains(src, "cluster recovery rejoin prepare") {
		t.Fatal("runbook uses the deleted bare `cluster recover`; C8 moved it to `cluster recovery rejoin prepare`")
	}
	if !strings.Contains(src, "cluster recovery rejoin prepare") || !strings.Contains(src, "--self-id") {
		t.Fatal("runbook must present `cluster recovery rejoin prepare … --self-id` (the C8 primary recovery verb)")
	}
	// C8 review M1: `cluster join prepare` takes --node-id (NOT --self-id). The runbook must never
	// instruct the non-existent `join prepare --self-id` (the recovery rejoin prepare --self-id lines
	// are valid + distinct). Scan both operator docs.
	for _, doc := range []string{"docs/cluster-runbook.md", "docs/usage.md"} {
		d := readFile(t, filepath.Join(moduleRoot(t), doc))
		if strings.Contains(d, "cluster join prepare --self-id") {
			t.Fatalf("%s instructs `cluster join prepare --self-id`; the flag is --node-id (M1)", doc)
		}
	}
}

func TestD7ReviewDrainHonorsRebuildOff(t *testing.T) {
	src := readFile(t, filepath.Join(moduleRoot(t), "internal/broker/clusterdrain.go"))
	if !strings.Contains(src, "rebuild_on_failure") {
		t.Fatal("drain migrateExposes never reads rebuild_on_failure; rebuild-OFF exposes are silently rehomed")
	}
}
