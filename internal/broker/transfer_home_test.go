package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/jsstream"
)

// TestD8XferTargetReplicasInertDefault pins the build-and-prove invariant: with no cluster
// seam attached (production), xferTargetReplicas returns jsstream.ReplicasSingle so a
// freshly-created OBJ_xfer bucket is byte-identically R=1 (cutover=D9). Attaching the seam
// flips it to the harness-provided value — proving the d8 guard exclusion of transfer_home.go
// is non-vacuous (the field genuinely gates behavior).
func TestD8XferTargetReplicasInertDefault(t *testing.T) {
	b := &Broker{}
	if got := b.xferTargetReplicas(); got != jsstream.ReplicasSingle {
		t.Fatalf("production xferTargetReplicas = %d, want ReplicasSingle (%d)", got, jsstream.ReplicasSingle)
	}
	b.AttachXferReplicas(func() int { return 3 })
	if got := b.xferTargetReplicas(); got != 3 {
		t.Fatalf("after AttachXferReplicas, xferTargetReplicas = %d, want 3", got)
	}
}

// TestD8TransferHomeGateInertInProduction pins the build-and-prove invariant: with no
// cluster identity (selfID==""), the home gate ALWAYS proceeds so the transfer request path
// is byte-identical to today (no home resolution, no silent drops). The clustered home==self /
// home!=self / unresolved routing is exercised in TestD8TransferHomeGateClustered against a
// seeded replicated DB.
func TestD8TransferHomeGateInertInProduction(t *testing.T) {
	b := &Broker{} // selfID == ""
	if !b.transferHomeGate("lab", "lab-1") {
		t.Fatal("production (selfID==\"\") must always proceed — byte-identical transfer path")
	}
	if !b.homeOwnsXferBucket("lab") {
		t.Fatal("production (selfID==\"\") must own every xfer bucket — byte-identical boot reaper")
	}
}
