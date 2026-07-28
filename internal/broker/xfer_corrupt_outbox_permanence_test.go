package broker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
)

// xfer_corrupt_outbox_permanence_test.go — what the row-level unreadable-outbox guard COSTS, made
// executable.
//
// origin: batch-B remainder, lane B8 adversarial review, finding B8-1.
//
// The guard itself is right: a start row must not be turned into a synthetic `failed` while a decided
// terminal for the same transfer sits in an outbox row this process could not parse
// (xfer_inflight.go:557-561, and TestUnreadableOutboxRowMustNotLicenseASyntheticTerminal pins it).
//
// WHAT THIS FILE PINS INSTEAD IS THE SHAPE OF THE RESULTING DEAD END
// ------------------------------------------------------------------
// Quarantining renames the bad row to <name>.corrupt (xfer_inflight.go:394-405) and NOTHING in the repo
// ever removes a .corrupt file — consumeXferLedgerRow (xfer_inflight.go:272-286) removes `<hash>.json`
// only, and the sibling housekeeping branch that prunes stale `.xfer-inflight-*.tmp` files
// (xfer_inflight.go:375-381) has no .corrupt counterpart. forEachLedgerRecord folds the quarantined name
// back into the unresolved set on every subsequent pass (xfer_inflight.go:371-374) — deliberately, so the
// answer is durable — so the gate stays armed for that transfer FOREVER.
//
// The consequence is that finalizeStrandedXfers returns (0, nil) for it on every pass, for the life of
// the data directory, and says nothing: reconcile_passes.go:199 logs only when the count is non-zero, the
// ledgerLeave arm of the pass is a bare `return` (xfer_inflight.go:615-616), and the "reason" field added
// by B8 is attached only to the SUCCESS log line (xfer_inflight.go:650-652). The one Warn an operator
// gets is emitted once, by the pass that performed the rename.
//
// That matters because the file's own justification for preferring zero terminals over one wrong one
// (replayStagedTerminal, xfer_inflight.go:426-428) is that "a dangling start is DETECTABLE and
// repairable". This test is the executable statement that, today, it is not.
//
// THIS TEST IS GREEN ON PURPOSE. It documents the gap rather than breaking the tree, and it is where the
// fix lands: when a recurring signal is added (a per-pass Warn naming the transfer, a blocked-count in
// the pass result, or an alert), the "no output ever" half of this test is what has to be edited, so the
// gap cannot be closed by accident and cannot be silently re-opened.
//
// The unrelated second transfer is a POSITIVE CONTROL, and it is the part that keeps the assertion from
// being vacuous: it proves the block is keyed per transfer by xferInflightFilename(transfer_id) rather
// than wedging the whole pass, so "nothing was finalized" cannot pass for the wrong reason.
func TestQuarantinedOutboxRowBlocksFinalizeForeverAndSilently(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}

	const blocked = "tid-outbox-row-quarantined"
	const control = "tid-plain-stranded"
	started := now.Add(-(transferTimeoutTierA + xferStrandedSlack + time.Minute))

	seed := func(tid string) xferInflightRecord {
		return xferInflightRecord{
			TransferID: tid, Session: "sess", Node: "n1", Verb: "push", Tier: "a",
			Bucket: "OBJ_xfer-sess", Path: "/dst", StartedAt: started,
		}
	}
	for _, tid := range []string{blocked, control} {
		if err := b.writeLedgerRecord(b.xferInflightDir(), seed(tid)); err != nil {
			t.Fatalf("seed primary start row for %s: %v", tid, err)
		}
	}

	// Start from the state a PREVIOUS pass left behind: the outbox holds only the quarantined sibling,
	// with no readable `<hash>.json` next to it. This is the durable half of the guard — the half that
	// makes the block permanent rather than one-tick — and it is reachable from every incident in which
	// a staged terminal's bytes were damaged.
	outbox := b.xferTerminalOutboxDir()
	if err := os.MkdirAll(outbox, 0o700); err != nil {
		t.Fatalf("mkdir outbox: %v", err)
	}
	quarantined := filepath.Join(outbox, xferInflightFilename(blocked)+".corrupt")
	if err := os.WriteFile(quarantined, []byte("{ truncated"), 0o600); err != nil {
		t.Fatalf("seed quarantined outbox row: %v", err)
	}

	var committed []schema.AuditTransfer
	b.transferAuditForwardSync = func(payload []byte) error {
		var rec schema.AuditTransfer
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Fatalf("decode forwarded terminal: %v", err)
		}
		committed = append(committed, rec)
		return nil
	}

	const passes = 10
	for pass := 1; pass <= passes; pass++ {
		n, err := b.finalizeStrandedXfers(context.Background())
		if err != nil {
			t.Fatalf("pass %d: a single quarantined row must not fail the whole pass: %v", pass, err)
		}
		want := 0
		if pass == 1 {
			want = 1 // the CONTROL transfer, and only it
		}
		if n != want {
			t.Fatalf("pass %d finalized %d, want %d", pass, n, want)
		}
		for _, rec := range committed {
			if rec.TransferID == blocked {
				t.Fatalf("pass %d: a transfer whose outbox row is merely UNREADABLE must never receive a "+
					"synthetic terminal — the decided outcome is still on disk in %s and would be "+
					"contradicted: %+v", pass, filepath.Base(quarantined), rec)
			}
		}
	}

	// The control proves the pass is alive and that the block is per-transfer, not global.
	if len(committed) != 1 || committed[0].TransferID != control ||
		committed[0].Kind != "failed" || committed[0].Code != "home_broker_restart" {
		t.Fatalf("the positive control did not finalize, so 'nothing was committed for the blocked "+
			"transfer' proves nothing: %+v", committed)
	}
	if _, err := os.Stat(filepath.Join(b.xferInflightDir(), xferInflightFilename(control))); !os.IsNotExist(err) {
		t.Errorf("the control's ledger row survived its committed terminal (stat err: %v)", err)
	}

	// ── THE GAP, STATED AS ASSERTIONS ────────────────────────────────────────────────────────────────
	// After ten passes the blocked transfer is in exactly the state it started in, and the pass has
	// reported success every time. Both facts below are the CURRENT behaviour, not the desired one.
	if _, err := os.Stat(filepath.Join(b.xferInflightDir(), xferInflightFilename(blocked))); err != nil {
		t.Fatalf("the start row must be RETAINED — leaving it is the whole point of the guard: %v", err)
	}
	if _, err := os.Stat(quarantined); err != nil {
		t.Fatalf("the quarantined row is never reaped, so the block never lifts. If this assertion has "+
			"started failing, a .corrupt reaper was added: make sure it emits a signal BEFORE it lifts a "+
			"block, or a transfer will go from silently un-finalizable to silently mis-finalized: %v", err)
	}
	// The audit now holds ZERO terminals for this transfer: the true one is unreadable and quarantined
	// out of the ledger, and the synthetic one is (correctly) refused. Nothing recurring says so.
	for _, rec := range committed {
		if rec.TransferID == blocked {
			t.Fatalf("unreachable given the loop above; kept as a belt-and-braces final check: %+v", rec)
		}
	}
}
