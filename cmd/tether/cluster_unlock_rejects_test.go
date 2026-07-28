package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// origin: codex_allgreen_external_review_test.go (renamed in B6) — docs/reviews/codex-allgreen-external-review.md
//
// A connected NATS server is not evidence that any broker answered the health
// probe.  The unlock command promises rc=0 only after membership is confirmed
// unfenced, so an empty responder set must fail closed.
func TestCodexReviewClusterUnlockRejectsZeroHealthResponders(t *testing.T) {
	url := startCLIExternalReviewNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

	var out bytes.Buffer
	err = runClusterUnlock(context.Background(), nc, "reviewer", nil, &out,
		time.Now(), true, true, false, true)
	if err == nil {
		t.Fatalf("zero broker health responders must not confirm an unlocked cluster; output:\n%s", out.String())
	}
}

// --config "" is an explicit hand-edit path.  It must not print the success
// step that says the seam is already installed and the daemon may be started.
func TestCodexReviewRestoreOptOutDoesNotClaimClusterSeamInstalled(t *testing.T) {
	b := newDRBox(t)
	stdout, stderr, err := b.runRestore(t, "--config", "")
	if err != nil {
		t.Fatalf("restore opt-out unexpectedly failed: %v\nstdout:\n%s\nstderr:\n%s", err, stdout, stderr)
	}
	if strings.Contains(stdout, "✓ broker.cluster seam is in") {
		t.Fatalf("restore opt-out falsely authorizes daemon start without a cluster seam:\n%s", stdout)
	}
	if !strings.Contains(stdout, "FIX the broker.cluster seam") {
		t.Fatalf("restore opt-out must make the manual prerequisite explicit in NEXT steps:\n%s", stdout)
	}
}
