//go:build d9_integration

package d9_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// testClusterStatusRemoteReachable (B1 item 1) proves the ctl-over-NATS `cluster status --remote`
// transport end-to-end: a ctl identity (a fresh member nkey, no broker admin socket) reaches the
// member-reachable cluster-health responder over NATS and gets a writable-leader signal from a
// healthy single-node leader. The CLI folding of these replies into the user summary
// (summarizeClusterHealth / ctlVerdictLine / ctlExitCode) is unit-tested in cmd/tether; this test
// proves the wire actually reaches in a real cluster-mode broker (the responder is wired only in
// cluster mode, so this also guards that single-mode stays byte-identical: no responder there).
func testClusterStatusRemoteReachable(t *testing.T) {
	h := startD9Broker(t, "d9-node-CH")
	waitLeader(t, h)

	nc, err := nats.Connect(h.url)
	if err != nil {
		t.Fatalf("nats connect: %v", err)
	}
	defer nc.Close()

	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}

	// One request/reply to the member-reachable cluster-health subject (one broker → one reply).
	msg, err := nc.Request(proto.SubjCtrlClusterHealth(actor), nil, 8*time.Second)
	if err != nil {
		t.Fatalf("cluster-health request (ctl over NATS): %v", err)
	}
	var resp proto.ClusterHealthResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		t.Fatalf("decode ClusterHealthResp: %v", err)
	}
	// A healthy, freshly-bootstrapped single-node leader: writable, not force-single, self-named.
	if !resp.WritableLeaderConfirmed {
		t.Errorf("healthy single-node leader must answer WritableLeaderConfirmed=true, got %+v", resp)
	}
	if resp.ForceSingleActive {
		t.Errorf("a freshly-bootstrapped leader must not report force_single_active: %+v", resp)
	}
	if resp.NodeID != "d9-node-CH" {
		t.Errorf("NodeID = %q, want d9-node-CH", resp.NodeID)
	}
}
