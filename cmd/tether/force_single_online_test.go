package main

import (
	"os"
	"strings"
	"testing"
)

// origin: force_single_online_external_review_test.go (renamed in B6) — docs/reviews/force-single-online-external-review.md
//
// TestOnlineForceSingleExternalReviewCLITransmitsConfirmedSelfID pins the operator
// safety contract: the node id typed by the operator must be checked by the broker,
// not used only as local prompt text. A wrong --self-id on the right socket must not
// be able to force-single the socket owner after confirming the wrong node id.
func TestOnlineForceSingleExternalReviewCLITransmitsConfirmedSelfID(t *testing.T) {
	src, err := os.ReadFile("cluster_offline.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	start := strings.Index(body, "func runForceSingleOnline(")
	end := strings.Index(body, "func printForceSingleReport(")
	if start < 0 || end < 0 || end <= start {
		t.Fatalf("runForceSingleOnline function boundaries not found")
	}
	body = body[start:end]
	if !strings.Contains(body, "Op: adminsock.OpClusterForceSingleArm") ||
		!strings.Contains(body, "Op: adminsock.OpClusterForceSingleCommit") {
		t.Fatal("online force-single arm/commit flow not found")
	}
	if !strings.Contains(body, "NodeID: selfID") && !strings.Contains(body, "SelfID: selfID") {
		t.Fatalf("online force-single does not send the operator-confirmed self id to the broker for validation")
	}
}
