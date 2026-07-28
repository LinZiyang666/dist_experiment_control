package broker

import (
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
	// G67 / gotcha #68 (internal review M4: the fix shipped with no anti-regression pin at either tier).
	// R16's A4 added an ACKNOWLEDGEMENT GATE — --to-standalone REFUSES on a data-bearing JetStream
	// store — and updated the grow-cutover remedy while missing this SSOT. This banner fires exactly
	// when JetStream HAS been serving, i.e. exactly when the store IS data-bearing, so without the note
	// it tells the operator to run a command that will refuse. Deliberately NOT asserted on the
	// cold-start sibling below: that path returns from Broker.Run before the admin socket exists, so
	// the online verb is unreachable there and already names the --manual offline escape instead.
	if !strings.Contains(rep.Banner, "--reset-js") {
		t.Fatalf("DATA-PLANE DEGRADED banner must warn that a data-bearing JetStream store makes the "+
			"remedy REFUSE without --reset-js (gotcha #68); got %q", rep.Banner)
	}
}

// TestG2ExternalReviewColdStartDiagnosticNamesRequiredStandaloneFlags pins the same guidance in the
// cold-start N=1 JetStream-unavailable diagnostic. That path lives inside Broker.Run after NATS /
// JetStream wiring and is not hermetically reachable, so the diagnostic is built by a dedicated
// function and asserted here on the COMPOSED string.
//
// It was previously a source-text pin over broker.go. R10 P4 replaced the three hand-copied remedy
// literals (this fatal, the DATA-PLANE-DEGRADED banner, `cluster recovery restore`) with the shared
// natsconf SSOT — after which the source carries a `%s` verb and a scraping pin can no longer see the
// flags it is meant to guarantee. Asserting the real message keeps the contract and survives the
// refactor; a regression that drops a flag from the SSOT still turns this red (verified by mutation).
func TestG2ExternalReviewColdStartDiagnosticNamesRequiredStandaloneFlags(t *testing.T) {
	diag := n1ClusteredJetStreamFatal().Error()
	for _, want := range []string{"tether cluster", "--server-name", "--broker-nkey"} {
		if !strings.Contains(diag, want) {
			t.Fatalf("cold-start N=1 diagnostic must include %s in the recovery command; got %q", want, diag)
		}
	}
	// The N=1 situation is what makes the remedy correct — the message must say so, otherwise an
	// operator on an N>=2 mesh-not-formed box would de-cluster and lose the mesh.
	if !strings.Contains(diag, "N=1") {
		t.Errorf("the diagnostic must scope itself to a lone voter; got %q", diag)
	}
	// R10 P4: it must also name the OFFLINE escape. The online verb needs a live leader to prove
	// N=1, and this fatal fires precisely when the daemon cannot start — so pointing only at the
	// online verb is a dead end (that dead end is the #64 finding).
	if !strings.Contains(diag, "--manual") {
		t.Errorf("the diagnostic must name the offline render, which is the only reachable remedy at boot; got %q", diag)
	}
}
