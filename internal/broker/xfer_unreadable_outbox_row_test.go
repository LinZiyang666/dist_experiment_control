package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
)

// xfer_unreadable_outbox_row_test.go — the ROW-level twin of the R6-F1 census guard.
//
// R6-F1 covers the DIRECTORY: when the outbox cannot be scanned at all, an empty census is not proof
// that no exact terminal is staged there, so a start-only primary row must not be synthesized.
// TestExternalRereviewUnreadableOutboxCannotAuthorizeSyntheticTerminal
// (r16_g67_g69_external_rereview_test.go) pins that by replacing the whole outbox DIRECTORY with a
// file, i.e. by making os.ReadDir fail.
//
// This test covers the case one level down, which that guard cannot see: the outbox directory reads
// fine and ONE FILE inside it does not. forEachLedgerRecord quarantines such a row — it renames it to
// <name>.corrupt and continues — and returns nil. So:
//
//	pass 1 never calls fn for that row  ⇒  it is absent from the outbox census
//	the census itself succeeded         ⇒  obErr == nil, so the R6-F1 gate does not fire
//	pass 2 sees a start-only row        ⇒  it synthesizes failed/home_broker_restart
//
// and because the quarantine RENAMED the row out of the ledger, the terminal it held is never
// replayed by any later pass either. The outcome is not a detectable contradictory pair: it is a
// single WRONG terminal, with the real one sitting in a .corrupt file nobody reads. That is strictly
// worse than the dangling start row #57 set out to eliminate — this file's own audit invariant
// (replayStagedTerminal: "EXACTLY ONE TRUE TERMINAL per transfer") is violated silently.
//
// The fix has to identify the transfer WITHOUT reading the row, and it can: the ledger filename is
// xferInflightFilename(transfer_id) = sha256 of the id. The hash is one-way, so a quarantined name
// cannot be turned back into a transfer_id — but pass 2 already HAS the transfer_id and can compute
// the same name forward and ask whether it was quarantined.
//
// Concretely: forEachLedgerRecord returns an `unresolved` set of ledger FILENAMES, and it folds a
// `<name>.json.corrupt` entry back in as `<name>.json` so the census survives the rename that caused the
// problem. The caller then asks `obUnreadable[xferInflightFilename(rec.TransferID)]`. Naming the two
// real identifiers matters here — an earlier version of this comment credited the behaviour to an
// invented `unreadableOutboxNames`, which greps to this comment and nothing else.
func TestUnreadableOutboxRowMustNotLicenseASyntheticTerminal(t *testing.T) {
	n, _ := d7SingleNode(t, "unreadable-outbox-row")
	now := time.Now().UTC()
	root := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	b.cl = &clusterRuntime{node: n}

	const tid = "unreadable-outbox-row"
	start := xferInflightRecord{
		TransferID: tid, Session: "sess", Node: "n1", Verb: "push", Tier: "a",
		Bucket: "OBJ_xfer-sess", Path: "/dst",
		StartedAt: now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
	if err := b.writeLedgerRecord(b.xferInflightDir(), start); err != nil {
		t.Fatalf("seed primary start: %v", err)
	}

	// Stage an exact `complete` terminal in the outbox, then corrupt THAT FILE's bytes while leaving
	// the directory perfectly readable. This is the shape a partial write, a bad sector or a truncated
	// fsync leaves behind — and, unlike the directory-level case, nothing about it looks broken to
	// os.ReadDir.
	staged := start
	staged.Terminal = &schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now, Session: start.Session, Node: start.Node,
		TransferID: tid, Path: start.Path, Tier: start.Tier, Bucket: start.Bucket,
	}
	if err := b.writeLedgerRecord(b.xferTerminalOutboxDir(), staged); err != nil {
		t.Fatalf("seed exact outbox terminal: %v", err)
	}
	outboxRow := filepath.Join(b.xferTerminalOutboxDir(), xferInflightFilename(tid))
	if _, err := os.Stat(outboxRow); err != nil {
		t.Fatalf("the staged outbox row is not where the filename scheme says it is: %v", err)
	}
	if err := os.WriteFile(outboxRow, []byte("{ truncated"), 0o600); err != nil {
		t.Fatalf("corrupt the outbox row: %v", err)
	}

	var committed []schema.AuditTransfer
	b.transferAuditForwardSync = func(payload []byte) error {
		var rec schema.AuditTransfer
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Fatal(err)
		}
		committed = append(committed, rec)
		return nil
	}

	// TWO passes, and the second one is the whole point. The first pass QUARANTINES the unreadable row
	// (renames it to <name>.corrupt), so a guard built only from "this pass failed to parse it" would be
	// armed on pass 1 and disarmed on pass 2 — the synthesizer would be delayed by one tick, not
	// blocked. The census therefore has to recognise the quarantined name too.
	for pass := 1; pass <= 2; pass++ {
		if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
			t.Fatalf("pass %d: one unreadable ROW should not make the whole pass fail: %v", pass, err)
		}
		for _, rec := range committed {
			if rec.Kind == "failed" {
				t.Fatalf("pass %d: an unreadable outbox ROW was treated as an absent terminal, so a synthetic "+
					"%s/%s was committed for a transfer whose real terminal was staged (and has now been "+
					"quarantined out of the ledger, so it will never be replayed).\n"+
					"The directory-level census guard (R6-F1) cannot see this: os.ReadDir succeeded and only "+
					"the row failed to parse.\ncommitted: %+v", pass, rec.Kind, rec.Code, committed)
			}
		}
		if pass == 1 {
			// Confirm the fixture really did reach the quarantine, so pass 2 is exercising the durable
			// half rather than repeating pass 1.
			if _, err := os.Stat(outboxRow); err == nil {
				t.Fatalf("the unreadable row was NOT quarantined on pass 1, so pass 2 does not test the " +
					"durable half of the guard")
			}
			quarantined, gerr := filepath.Glob(outboxRow + ".corrupt*")
			if gerr != nil || len(quarantined) == 0 {
				t.Fatalf("expected a quarantined .corrupt sibling after pass 1: %v %v", quarantined, gerr)
			}
		}
	}

	// The primary start row must still be there: leaving it is the whole point — a dangling start row
	// is detectable and re-triable, a wrong terminal is neither.
	primaryRow := filepath.Join(b.xferInflightDir(), xferInflightFilename(tid))
	if _, err := os.Stat(primaryRow); err != nil {
		t.Errorf("the start row was retired without a terminal being emitted for it: %v", err)
	}
}

// TestUnfinalizableTransferIsReportedOnEveryPass is the half the first version of this fix left out.
//
// "Detectable and repairable" is the argument replayStagedTerminal uses to justify preferring a dangling
// start row over a contradictory terminal pair, and the whole disposition above leans on it. But
// detectability is a property something has to PROVIDE. This state is permanent — the unreadable row is
// quarantined to <name>.corrupt on the pass that finds it, the census folds that name back in forever,
// and nothing removes either file — so a single Warn on the pass that quarantined it, possibly weeks
// earlier and possibly rotated away, is not detection. The internal review (B8-1) caught the plan
// claiming "每轮重试 + 日志暴露": the retry half is true and useless (it re-decides `leave` every tick),
// and the log-exposure half was simply false.
//
// So the assertion is on the LOG, deliberately, because the log is the entire mechanism.
func TestUnfinalizableTransferIsReportedOnEveryPass(t *testing.T) {
	n, _ := d7SingleNode(t, "unfinalizable-reported")
	now := time.Now().UTC()
	root := t.TempDir()

	var logBuf lockedBuffer
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{
		ClusterDataDir: root,
		Logger:         slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelWarn})),
		Now:            func() time.Time { return now },
	}
	b.cl = &clusterRuntime{node: n}

	const tid = "unfinalizable-reported"
	start := xferInflightRecord{
		TransferID: tid, Session: "sess", Node: "n1", Verb: "push", Tier: "a",
		Bucket: "OBJ_xfer-sess", Path: "/dst",
		StartedAt: now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
	if err := b.writeLedgerRecord(b.xferInflightDir(), start); err != nil {
		t.Fatalf("seed primary start: %v", err)
	}
	staged := start
	staged.Terminal = &schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", Ts: now,
		Session: start.Session, Node: start.Node, TransferID: tid,
		Path: start.Path, Tier: start.Tier, Bucket: start.Bucket,
	}
	if err := b.writeLedgerRecord(b.xferTerminalOutboxDir(), staged); err != nil {
		t.Fatalf("seed exact outbox terminal: %v", err)
	}
	if err := os.WriteFile(filepath.Join(b.xferTerminalOutboxDir(), xferInflightFilename(tid)),
		[]byte("{ truncated"), 0o600); err != nil {
		t.Fatalf("corrupt the outbox row: %v", err)
	}
	b.transferAuditForwardSync = func([]byte) error { return nil }

	// THREE passes. Pass 1 quarantines; passes 2 and 3 are the ones an operator would actually be
	// looking at weeks later, and they are the ones the original fix left silent.
	for pass := 1; pass <= 3; pass++ {
		logBuf.Reset()
		if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
			t.Fatalf("pass %d: %v", pass, err)
		}
		out := logBuf.String()
		if !strings.Contains(out, "can never be finalized") {
			t.Errorf("pass %d produced NO report of the unfinalizable transfer.\n"+
				"This state never resolves on its own, so a pass that says nothing means an operator's only "+
				"signal was one line on some earlier pass. Logged this pass:\n%s", pass, out)
		}
		if !strings.Contains(out, tid) {
			t.Errorf("pass %d: the report does not name the transfer, so it cannot be acted on:\n%s", pass, out)
		}
		if !strings.Contains(out, ".corrupt") {
			t.Errorf("pass %d: the report does not tell the operator WHERE to look (the quarantined "+
				".corrupt file) or what to do:\n%s", pass, out)
		}
		// origin: batch B2 independent external review F3
		if strings.Contains(out, "repair it in place") {
			t.Errorf("pass %d: the recovery guidance says to repair the .corrupt file in place, but "+
				"forEachLedgerRecord ignores every .json.corrupt* filename forever; a repaired row must "+
				"also be renamed back to its canonical .json name before the next pass can replay it:\n%s",
				pass, out)
		}
		if strings.Contains(out, "remove BOTH") {
			t.Errorf("pass %d: the recovery guidance says to remove both the corrupt outbox row and the "+
				"matching in-flight row. Removing the primary start row also removes the only input from "+
				"which the finalizer can synthesize a terminal, leaving no row and no terminal forever. "+
				"An unrecoverable outbox row may be removed only while retaining the primary start row:\n%s",
				pass, out)
		}
	}
}

// lockedBuffer is a bytes.Buffer safe for a slog handler writing from the pass goroutine while the test
// reads it. The finalizer is single-goroutine today, but a buffer shared with a logger is exactly the
// kind of thing -race notices later rather than now.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *lockedBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}
