package broker

import (
	"os"
	"strings"
	"testing"
)

// TestOnlineForceSingleExternalReviewMarkerErrorsAreFatal pins the emergency
// visibility contract: after the online RecoverCluster rewrite succeeds, failing to
// persist force_single_active / epoch must not be silently reported as success.
func TestOnlineForceSingleExternalReviewMarkerErrorsAreFatal(t *testing.T) {
	src, err := os.ReadFile("force_single_online.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(src), "_ = b.admin.node.Propose") {
		t.Fatalf("online force-single commit ignores marker/epoch Propose errors; force_single_active can be missing after a reported success")
	}
}

// TestOnlineForceSingleExternalReviewRunbookDocumentsOnlinePath pins the feature's
// stated goal: the runbook should no longer teach that offline disk surgery is the
// only quorum-loss recovery path when the broker is running.
func TestOnlineForceSingleExternalReviewRunbookDocumentsOnlinePath(t *testing.T) {
	b, err := os.ReadFile("../../docs/cluster-runbook.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	if !strings.Contains(doc, "--online") {
		t.Fatalf("cluster runbook does not document `cluster recovery force-single --online`")
	}
	if strings.Contains(doc, "only `cluster recovery force-single` (OFFLINE) can recover") ||
		strings.Contains(doc, "the ONLY operable path") {
		t.Fatalf("cluster runbook still states offline force-single is the only quorum-loss recovery path")
	}
}
