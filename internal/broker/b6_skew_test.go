package broker

import (
	"log/slog"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
)

// b6_skew_test.go — B6 A3 version-skew gate (make test): a proto-mismatched joiner is rejected
// with Code=version_skew BEFORE the single-use nonce is claimed, so the operator can retry the
// same token after reinstalling the joiner. The matching/zero-proto ALLOW paths run end-to-end in
// the gated test/d7 AddNode drill (they need a live raft node).

func newSkewTestBackend() (*clusterAdminBackend, *ClusterAdmin) {
	admin := &ClusterAdmin{logger: slog.Default(), issuedNonces: map[string]bool{}}
	return &clusterAdminBackend{admin: admin}, admin
}

func TestVersionSkewRejectBeforeNonceBurn(t *testing.T) {
	be, admin := newSkewTestBackend()
	nonce, err := admin.IssueJoinNonce()
	if err != nil {
		t.Fatalf("issue nonce: %v", err)
	}

	resp := be.handleAdd(adminsock.Request{
		Op:          adminsock.OpClusterAdd,
		NodeID:      "brk-b",
		JoinToken:   nonce + ":deadbeef",
		JoinerProto: proto.ProtoVersion + 99, // a future/foreign proto
	})
	if resp.Code != adminsock.CodeVersionSkew {
		t.Fatalf("want version_skew, got code=%q error=%q", resp.Code, resp.Error)
	}
	// The single-use nonce must NOT have been burned by a rejected joiner — it is still claimable,
	// so the operator can re-run `cluster add` with the SAME token after reinstalling the joiner.
	if !admin.claimJoinNonce(nonce) {
		t.Fatal("a version-skew reject must NOT burn the nonce (gate runs before claimJoinNonce)")
	}
}

// TestVersionSkewAllowPaths — Stage-C MAJOR-6: matching proto, undeclared proto (0), and a release
// skew must all be ALLOWED (NOT version_skew). Tested via the extracted gate (the full allow path
// proceeds to AddNode, which needs a live raft node).
func TestVersionSkewAllowPaths(t *testing.T) {
	be, _ := newSkewTestBackend()
	cases := []struct {
		name string
		req  adminsock.Request
	}{
		{"matching proto", adminsock.Request{Op: adminsock.OpClusterAdd, NodeID: "b", JoinerProto: proto.ProtoVersion}},
		{"undeclared proto (0)", adminsock.Request{Op: adminsock.OpClusterAdd, NodeID: "b", JoinerProto: 0}},
		{"release skew advisory", adminsock.Request{Op: adminsock.OpClusterAdd, NodeID: "b", JoinerProto: proto.ProtoVersion, JoinerRelease: "v9.9.9-other"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, reject := be.versionSkewResponse(c.req); reject {
				t.Fatalf("%s must NOT be version-skew rejected", c.name)
			}
		})
	}
}

func TestVersionSkewRejectsMismatch(t *testing.T) {
	be, _ := newSkewTestBackend()
	resp, reject := be.versionSkewResponse(adminsock.Request{Op: adminsock.OpClusterAdd, NodeID: "b", JoinerProto: proto.ProtoVersion + 1})
	if !reject || resp.Code != adminsock.CodeVersionSkew {
		t.Fatalf("a proto mismatch must reject with version_skew, got reject=%v code=%q", reject, resp.Code)
	}
}
