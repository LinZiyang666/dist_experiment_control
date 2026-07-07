package broker

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// TestG2ExternalReviewDataPlaneBannerNamesRequiredStandaloneFlags pins the operator guidance in the
// persistent force-single DATA-PLANE DEGRADED banner. The online force-single output and runbook both
// require --server-name and --broker-nkey because a still-clustered multi-user nats.conf cannot
// unambiguously auto-pick this broker's bus nkey. The status banner is the long-lived recovery prompt an
// operator sees until the data plane is fixed, so it must not print an incomplete command.
func TestG2ExternalReviewDataPlaneBannerNamesRequiredStandaloneFlags(t *testing.T) {
	n, addr := d7SingleNode(t, "brk-a")
	admin := NewClusterAdmin(n, nil)
	caughtUp := func(barrier uint64) (bool, error) { cur, err := n.AppliedIndex(); return cur >= barrier, err }
	if err := admin.AddNode(d7JoinInput(t, "brk-a", addr), addr, caughtUp, 5*time.Second); err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	setCmd, err := cluster.PlanSetForceSingle(time.Now())
	if err != nil {
		t.Fatalf("PlanSetForceSingle: %v", err)
	}
	proposeCmd(t, n, setCmd)
	admin.SetNatsConfPath(writeConfFile(t, g2ClusteredConf))

	rep, err := admin.StatusReport("ctl-nats")
	if err != nil {
		t.Fatalf("StatusReport: %v", err)
	}
	for _, want := range []string{"tether cluster", "--server-name", "--broker-nkey"} {
		if !strings.Contains(rep.Banner, want) {
			t.Fatalf("DATA-PLANE DEGRADED banner must include %s in the recovery command; got %q", want, rep.Banner)
		}
	}
}

// TestG2ExternalReviewColdStartDiagnosticNamesRequiredStandaloneFlags pins the same guidance in the
// cold-start N=1 JetStream-unavailable diagnostic. This path is otherwise hard to trigger hermetically
// because it lives inside Broker.Run after NATS/JetStream wiring; source-pin tests are already used for
// external review contracts in this package.
func TestG2ExternalReviewColdStartDiagnosticNamesRequiredStandaloneFlags(t *testing.T) {
	srcBytes, err := os.ReadFile("broker.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(srcBytes)
	start := strings.Index(src, "broker: cluster mode requires JetStream")
	if start < 0 {
		t.Fatal("N=1 JetStream-unavailable diagnostic not found")
	}
	endRel := strings.Index(src[start:], "G2 #10")
	if endRel < 0 {
		t.Fatal("N=1 diagnostic boundary not found")
	}
	diag := src[start : start+endRel]
	for _, want := range []string{"tether cluster", "--server-name", "--broker-nkey"} {
		if !strings.Contains(diag, want) {
			t.Fatalf("cold-start N=1 diagnostic must include %s in the recovery command; got %q", want, diag)
		}
	}
}
