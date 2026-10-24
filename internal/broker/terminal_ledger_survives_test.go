package broker

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

// TestExternalReviewTerminalLedgerSurvivesUntilAuditCommit exercises the crash window that
// #57's durable ledger is meant to close. Cluster-mode audit forwarding is asynchronous, so
// returning from emitTransferAudit only means that a goroutine was launched; it does not mean
// the terminal row reached the leader or committed. The ledger must remain recoverable until
// that forward succeeds, otherwise a home-broker crash in the interval leaves the original
// start row dangling forever.
func TestExternalReviewTerminalLedgerSurvivesUntilAuditCommit(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTerminalLedger(t)
	}
	testharness.AssertNoGoroutineLeak(t, "staged terminal before audit commit", g)
	testharness.AssertNoFDLeak(t, "staged terminal before audit commit", fd, 0)
}
func exerciseTerminalLedger(t *testing.T) {
	t.Helper()

	dir := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: dir, Logger: silentLogger(), Now: time.Now}

	forwardEntered := make(chan struct{})
	releaseForward := make(chan struct{})
	var forwarded []byte
	b.attachTransferAuditSinkWith(func(data []byte) error {
		forwarded = append([]byte(nil), data...)
		close(forwardEntered)
		<-releaseForward
		return nil
	})
	defer func() {
		close(releaseForward)
		b.WaitTransferAudit()
	}()

	e := xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{
		TransferID: "external-review-terminal-window",
		SID:        "sess",
		NID:        "node-1",
		Verb:       "push",
		Tier:       "b",
		StartedAt:  time.Now(),
	})
	if code := putLegacyTransfer(b.transfers, e); code != "" {
		t.Fatalf("put: %s", code)
	}
	b.writeXferInflight(e)
	ledger := filepath.Join(dir, "xfer-inflight", xferInflightFilename(b.transfers.Record(e).TransferID))
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
	// Existence alone is insufficient: the start row already existed before
	// staging. Crash recovery must retain the exact bytes currently forwarding.
	// origin: hot-update S7 plan §5.1 / T21
	row, err := readXferInflight(ledger)
	if err != nil || row.Terminal == nil {
		t.Fatalf("exact staged terminal missing: %v", err)
	}
	staged, err := json.Marshal(*row.Terminal)
	if err != nil || !bytes.Equal(staged, forwarded) {
		t.Fatalf("staged terminal differs from in-flight publication: %v", err)
	}

}
