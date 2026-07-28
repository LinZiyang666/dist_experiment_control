package broker

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestExternalReviewTerminalLedgerSurvivesUntilAuditCommit exercises the crash window that
// #57's durable ledger is meant to close. Cluster-mode audit forwarding is asynchronous, so
// returning from emitTransferAudit only means that a goroutine was launched; it does not mean
// the terminal row reached the leader or committed. The ledger must remain recoverable until
// that forward succeeds, otherwise a home-broker crash in the interval leaves the original
// start row dangling forever.
func TestExternalReviewTerminalLedgerSurvivesUntilAuditCommit(t *testing.T) {
	dir := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: dir, Logger: silentLogger(), Now: time.Now}

	forwardEntered := make(chan struct{})
	releaseForward := make(chan struct{})
	b.attachTransferAuditSinkWith(func([]byte) error {
		close(forwardEntered)
		<-releaseForward
		return nil
	})
	defer func() {
		close(releaseForward)
		b.WaitTransferAudit()
	}()

	e := &transferEntry{
		transferID: "external-review-terminal-window",
		sid:        "sess",
		nid:        "node-1",
		verb:       "push",
		tier:       "b",
		startedAt:  time.Now(),
	}
	if code := b.transfers.put(e); code != "" {
		t.Fatalf("put: %s", code)
	}
	b.writeXferInflight(e)
	ledger := filepath.Join(dir, "xfer-inflight", xferInflightFilename(e.transferID))
	if _, err := os.Stat(ledger); err != nil {
		t.Fatalf("ledger must exist before terminal handling: %v", err)
	}

	b.cleanupEntry(e, "forward_failed", "peer went away")
	select {
	case <-forwardEntered:
	case <-time.After(time.Second):
		t.Fatal("terminal audit forward did not start")
	}

	if _, err := os.Stat(ledger); err != nil {
		t.Fatalf("terminal audit has not committed yet, so crash-recovery evidence must remain; stat ledger: %v", err)
	}
}
