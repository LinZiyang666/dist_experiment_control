package clusteroffline

import (
	"net"
	"testing"
)

// origin: force_single_round2_external_review_test.go (renamed in B6) — docs/reviews/force-single-online-external-review.md
//
// TestForceSingleRound2ReviewNatsRouteURLIsProbed pins the multi-port hard-refuse
// contract with the canonical cluster_nodes.nats_route shape. Route URLs are
// persisted as nats://host:port, so the probe must strip the scheme before
// dialing; otherwise an alive NATS route peer is missed.
func TestForceSingleRound2ReviewNatsRouteURLIsProbed(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	roster := []Peer{{
		NodeID: "brk-b", RaftAddr: "127.0.0.1:1",
		NatsRoute:  "nats://" + ln.Addr().String(),
		TunnelAddr: "127.0.0.1:1",
	}}
	if err := CheckPeersDead(roster, []string{"brk-b"}); err == nil {
		t.Fatal("CheckPeersDead must refuse when the peer's canonical nats:// route URL is alive")
	}
}
