package broker

import (
	"log/slog"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/proto"
)

// b6_skew_test.go — B6 A3 version-skew gate (make test): a proto-mismatched joiner is rejected
// with Code=version_skew BEFORE the single-use nonce is claimed, so the operator can retry the
// same token after reinstalling the joiner. Only an exact proto+release match is allowed; missing
// declarations fail closed and the matching path runs end-to-end in the gated test/d7 AddNode drill.
//
// WHICH PATH THIS COVERS — read before trusting it (batch B, B4)
//
// Everything here drives `handleAdd`, reachable only through adminsock.OpClusterAdd, which
// internal/adminsock/protocol.go has DELIBERATELY not routed since v0.4.2. So this file has
// always covered a path the CLI cannot take, and it stayed green for a year while the LIVE grow
// path (`cluster join approve` -> StartJoinOperation -> driveJoin) had no version check at all.
// That gap is what batch B's B4 closed, and the live path's coverage now lives in
// internal/broker/join_version_gate_test.go.
//
// This file is KEPT rather than retargeted, deliberately:
//
//   - The property it pins that nothing else does is that a rejected joiner does not BURN its
//     single-use nonce, so the operator can reuse the same token after reinstalling. That is a
//     real property of handleAdd's ordering and it would be lost by deletion.
//   - handleAdd is still live code. It is unreachable only because of a routing-table entry, and
//     internal/adminsock/routing_tripwire_test.go is what keeps it that way. If that entry ever
//     comes back, these tests are the ones that still apply.
//
// What must NOT be inferred from a green run here: that the grow path is version-gated. It is
// not gated by anything in this file.

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

// TestVersionSkewAllowPaths proves the only allow decision: exact proto+release equality.
func TestVersionSkewAllowPaths(t *testing.T) {
	be, _ := newSkewTestBackend()
	req := adminsock.Request{
		Op: adminsock.OpClusterAdd, NodeID: "b",
		JoinerProto: proto.ProtoVersion, JoinerRelease: proto.ReleaseVersion,
	}
	if _, reject := be.versionSkewResponse(req); reject {
		t.Fatal("an exact proto+release match must not be version-skew rejected")
	}
}

func TestVersionSkewRejectsMissingDeclarations(t *testing.T) {
	be, _ := newSkewTestBackend()
	for _, req := range []adminsock.Request{
		{Op: adminsock.OpClusterAdd, NodeID: "b", JoinerRelease: proto.ReleaseVersion},
		{Op: adminsock.OpClusterAdd, NodeID: "b", JoinerProto: proto.ProtoVersion},
	} {
		resp, reject := be.versionSkewResponse(req)
		if !reject || resp.Code != adminsock.CodeVersionSkew {
			t.Errorf("missing version declaration must fail closed; req=%+v reject=%v code=%q",
				req, reject, resp.Code)
		}
	}
}

// TestVersionSkewRejectsReleaseMismatch (external review B3) — the row above used to assert that a
// declared release mismatch was ADVISORY. docs/requirements.md §6.7 mandates identical
// major.minor.patch and says a mismatch must be refused; it is the layer-1 authority, so the
// advisory reading was an implementation choice overruling a requirement.
func TestVersionSkewRejectsReleaseMismatch(t *testing.T) {
	be, _ := newSkewTestBackend()
	resp, reject := be.versionSkewResponse(adminsock.Request{
		Op: adminsock.OpClusterAdd, NodeID: "b",
		JoinerProto: proto.ProtoVersion, JoinerRelease: "v9.9.9-other",
	})
	if !reject {
		t.Fatal("a declared release mismatch must reject (requirements §6.7)")
	}
	if resp.Code != adminsock.CodeVersionSkew {
		t.Fatalf("a release mismatch must carry the SAME version_skew code as a proto mismatch — "+
			"both mean 'reinstall the joiner', and a different code would route automation "+
			"elsewhere; got %q", resp.Code)
	}
}

func TestVersionSkewRejectsMismatch(t *testing.T) {
	be, _ := newSkewTestBackend()
	resp, reject := be.versionSkewResponse(adminsock.Request{Op: adminsock.OpClusterAdd, NodeID: "b", JoinerProto: proto.ProtoVersion + 1})
	if !reject || resp.Code != adminsock.CodeVersionSkew {
		t.Fatalf("a proto mismatch must reject with version_skew, got reject=%v code=%q", reject, resp.Code)
	}
}
