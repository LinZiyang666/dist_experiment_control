package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// origin: force_single_round2_external_review_test.go (renamed in B6) — docs/reviews/force-single-online-external-review.md
//
// TestOnlineForceSingleRound2ReviewEmptySelfIDRefused pins the fixed F2
// contract strictly: broker-side validation must require the operator-confirmed
// node id, not only reject non-empty mismatches. An empty NodeID is what an old
// or hand-written local socket client would send.
func TestOnlineForceSingleRound2ReviewEmptySelfIDRefused(t *testing.T) {
	b, _ := fsTestBackend(t, "brk-a", "brk-b", "127.0.0.1:1")
	markQuorumLostPastDwell(b)
	resp := b.handleForceSingleArm(adminsock.Request{
		Op: adminsock.OpClusterForceSingleArm, ConfirmPeersDead: []string{"brk-b"},
	})
	if resp.OK || resp.Code != adminsock.CodeBadRequest {
		t.Fatalf("empty NodeID must be refused with CodeBadRequest, got OK=%v Code=%q err=%q", resp.OK, resp.Code, resp.Error)
	}
}
