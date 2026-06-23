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
	if strings.Contains(src, "tether cluster recover --dump-divergent /root/divergent-$(hostname).json") &&
		!strings.Contains(src, "recover --self-id") {
		t.Fatal("runbook recover command omits the required --self-id flag")
	}
}

func TestD7ReviewDrainHonorsRebuildOff(t *testing.T) {
	src := readFile(t, filepath.Join(moduleRoot(t), "internal/broker/clusterdrain.go"))
	if !strings.Contains(src, "rebuild_on_failure") {
		t.Fatal("drain migrateExposes never reads rebuild_on_failure; rebuild-OFF exposes are silently rehomed")
	}
}
