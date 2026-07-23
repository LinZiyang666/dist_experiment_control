package broker

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferaudit"
)

// strandedEntry is a data-bearing tier-B transfer whose home crashed > timeout+slack ago.
func strandedEntry(now time.Time) *transferEntry {
	return &transferEntry{
		transferID: "tid-stranded", sid: "sess", nid: "node1", actor: "AC", actorFP: "fp",
		verb: "pull", tier: "b", bucket: "xfer-sess", path: "/tmp/f",
		startedAt: now.Add(-(transferTimeoutTierB + xferStrandedSlack + time.Minute)),
	}
}

// TestXferInflightFinalizeOnRecovery is the #57 fix pin: a durable in-flight ledger lets a restarted home
// broker synthesize a DETERMINISTIC terminal for a transfer whose start row would otherwise dangle forever,
// while a fresh or still-live transfer is never touched, and the terminal is COMMITTED before the ledger is
// deleted.
func TestXferInflightFinalizeOnRecovery(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	b := &Broker{transfers: newTransferTracker()}
	var committed []schema.AuditTransfer
	b.cfg = Config{ClusterDataDir: dir, Logger: silentLogger(), Now: func() time.Time { return now }}
	// M1: the finalizer commits via the SYNCHRONOUS forward and deletes the ledger only on success.
	b.transferAuditForwardSync = func(payload []byte) error {
		var rec schema.AuditTransfer
		if err := json.Unmarshal(payload, &rec); err != nil {
			return err
		}
		committed = append(committed, rec)
		return nil
	}

	b.writeXferInflight(strandedEntry(now))
	b.writeXferInflight(&transferEntry{transferID: "tid-fresh", sid: "sess", nid: "node1", verb: "push", tier: "a", startedAt: now})
	live := &transferEntry{transferID: "tid-live", sid: "sess", nid: "node1", verb: "pull", tier: "b", startedAt: now.Add(-time.Hour)}
	b.writeXferInflight(live)
	if code := b.transfers.put(live); code != "" {
		t.Fatalf("put live: %s", code)
	}

	n, err := b.finalizeStrandedXfers(context.Background())
	if err != nil {
		t.Fatalf("finalizeStrandedXfers: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected exactly 1 stranded transfer finalized (not the fresh or live one), got %d", n)
	}
	if len(committed) != 1 {
		t.Fatalf("expected 1 committed synthetic terminal, got %d", len(committed))
	}
	rec := committed[0]
	if rec.TransferID != "tid-stranded" || rec.Kind != "failed" || rec.Code != "home_broker_restart" || rec.Verb != "pull" {
		t.Fatalf("wrong synthetic terminal: %+v", rec)
	}
	stranded := strandedEntry(now)
	if wantTs := stranded.startedAt.Add(transferTimeoutTierB); !rec.Ts.Equal(wantTs) {
		t.Fatalf("Ts must be deterministic startedAt+timeout, got %v want %v", rec.Ts, wantTs)
	}
	inflight := filepath.Join(dir, "xfer-inflight")
	if _, se := os.Stat(filepath.Join(inflight, xferInflightFilename("tid-stranded"))); !os.IsNotExist(se) {
		t.Fatal("the finalized (COMMITTED) stranded ledger file must be removed")
	}
	if _, se := os.Stat(filepath.Join(inflight, xferInflightFilename("tid-fresh"))); se != nil {
		t.Fatalf("a fresh transfer's ledger must remain: %v", se)
	}
	if _, se := os.Stat(filepath.Join(inflight, xferInflightFilename("tid-live"))); se != nil {
		t.Fatalf("a live transfer's ledger must remain: %v", se)
	}

	committed = nil
	n2, err := b.finalizeStrandedXfers(context.Background())
	if err != nil {
		t.Fatalf("2nd finalize: %v", err)
	}
	if n2 != 0 {
		t.Fatalf("second pass must finalize 0 (stranded file already removed), got %d", n2)
	}
}

// TestXferInflightFinalizeRetainsLedgerWhenUncommittable is the M1 pin: in the leaderless post-crash window
// the synthetic terminal cannot commit, so the durable ledger MUST be RETAINED (never fire-and-forget the
// terminal then destroy the evidence). A later pass, once a leader exists, finalizes it.
func TestXferInflightFinalizeRetainsLedgerWhenUncommittable(t *testing.T) {
	dir := t.TempDir()
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: dir, Logger: silentLogger(), Now: func() time.Time { return now }}
	b.writeXferInflight(strandedEntry(now))
	ledger := filepath.Join(dir, "xfer-inflight", xferInflightFilename("tid-stranded"))

	// (a) sink NOT attached (pre-wireClusterLate) → cannot confirm → retain, count 0.
	b.transferAuditForwardSync = nil
	if n, err := b.finalizeStrandedXfers(context.Background()); err != nil || n != 0 {
		t.Fatalf("sink-not-attached: want (0,nil), got (%d,%v)", n, err)
	}
	if _, se := os.Stat(ledger); se != nil {
		t.Fatal("ledger MUST be retained when the sink is not attached (never lose the evidence)")
	}

	// (b) leaderless (persistent not_leader) → retain, count 0.
	attempts := 0
	b.transferAuditForwardSync = func([]byte) error { attempts++; return cluster.ErrForwardNotLeader }
	if n, err := b.finalizeStrandedXfers(context.Background()); err != nil || n != 0 {
		t.Fatalf("leaderless: want (0,nil), got (%d,%v)", n, err)
	}
	if attempts < 2 {
		t.Fatalf("a transient not_leader must be RETRIED within the pass, got %d attempts", attempts)
	}
	if _, se := os.Stat(ledger); se != nil {
		t.Fatal("ledger MUST be retained in the leaderless window (the #57 flagship case)")
	}

	// (c) a permanent (non-not_leader) forward error → retain (do NOT destroy evidence on an ambiguous error).
	b.transferAuditForwardSync = func([]byte) error { return errors.New("boom") }
	if n, _ := b.finalizeStrandedXfers(context.Background()); n != 0 {
		t.Fatalf("permanent error: want 0 finalized, got %d", n)
	}
	if _, se := os.Stat(ledger); se != nil {
		t.Fatal("ledger MUST be retained on a permanent forward error")
	}

	// (d) leader returns → committed → finalized + removed.
	var committed int
	b.transferAuditForwardSync = func([]byte) error { committed++; return nil }
	if n, err := b.finalizeStrandedXfers(context.Background()); err != nil || n != 1 {
		t.Fatalf("recovered leader: want (1,nil), got (%d,%v)", n, err)
	}
	if committed != 1 {
		t.Fatalf("want 1 committed terminal, got %d", committed)
	}
	if _, se := os.Stat(ledger); !os.IsNotExist(se) {
		t.Fatal("ledger must be removed once the terminal COMMITTED")
	}
}

// TestXferInflightTerminalDropsLedger pins a TERMINAL path's ledger trailer (internal review M7: no
// transfer-handler test ran with ClusterDataDir set, so every `removeXferInflight` trailer was dead code
// under test). Dropping one is not a cosmetic leak: the ledger would outlive a transfer that ENDED CLEANLY,
// and the finalizer would later publish a contradictory synthetic `failed home_broker_restart` row for it —
// plus the ledger dir would grow without bound. cleanupEntry is the directly-callable terminal; the other
// three (watchdog / handleEvTransfer / handleFinalizeReq) sit on the same `transfers.remove(...)` line and
// remain covered only end-to-end (recorded residual).
func TestXferInflightTerminalDropsLedger(t *testing.T) {
	dir := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: dir, Logger: silentLogger(), Now: time.Now}
	// EXTERNAL REVIEW F1: this pin previously used a no-op sink, which made "emitted" indistinguishable
	// from "committed" and therefore FROZE THE WRONG ORDER as green — the reviewer's counter-example
	// deleted the ledger before the audit was committed and this test still passed. The sink now behaves
	// like the real cluster-mode one: it commits (invokes onCommitted) only when the forward succeeds.
	b.transferAuditSink = func(_ schema.AuditTransfer, onCommitted func()) {
		if onCommitted != nil {
			onCommitted()
		}
	}

	e := &transferEntry{transferID: "tid-term", sid: "sess", nid: "n1", verb: "push", tier: "b", startedAt: time.Now()}
	if code := b.transfers.put(e); code != "" {
		t.Fatalf("put: %s", code)
	}
	b.writeXferInflight(e)
	ledger := filepath.Join(dir, "xfer-inflight", xferInflightFilename("tid-term"))
	if _, err := os.Stat(ledger); err != nil {
		t.Fatalf("ledger must exist while the transfer is in flight: %v", err)
	}

	b.cleanupEntry(e, "forward_failed", "peer went away")

	if _, err := os.Stat(ledger); !os.IsNotExist(err) {
		t.Fatal("a TERMINAL path must drop the in-flight ledger — leaving it makes the #57 finalizer later " +
			"publish a contradictory synthetic `failed` row for a transfer that already ended, and the ledger " +
			"dir grows unbounded")
	}

	// The OTHER half, which this pin used to miss entirely (external review F1): a sink that never
	// commits must leave the ledger ALIVE, because the recovery finalizer is the only thing that can
	// still write a terminal for that transfer.
	e2 := &transferEntry{transferID: "tid-uncommitted", sid: "sess", nid: "n1", verb: "push", tier: "b", startedAt: time.Now()}
	if code := b.transfers.put(e2); code != "" {
		t.Fatalf("put: %s", code)
	}
	b.writeXferInflight(e2)
	b.transferAuditSink = func(schema.AuditTransfer, func()) {} // emitted, never committed
	b.cleanupEntry(e2, "forward_failed", "peer went away")
	if _, err := os.Stat(filepath.Join(dir, "xfer-inflight", xferInflightFilename("tid-uncommitted"))); err != nil {
		t.Fatal("when the audit forward never commits, the ledger MUST survive so the recovery finalizer " +
			"can still synthesize a terminal — deleting it here is the #57 dangling-start shape")
	}
	if b.transfers.get("tid-term") != nil {
		t.Fatal("cleanupEntry must also remove the tracker entry (pre-existing contract)")
	}
}

// TestXferInflightNoLedgerWithoutClusterDataDir pins that single-broker mode (ClusterDataDir unset) writes
// NO ledger — its audit publish is best-effort by design, and #57's durable ledger is cluster-mode only.
func TestXferInflightNoLedgerWithoutClusterDataDir(t *testing.T) {
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{Logger: silentLogger(), Now: time.Now} // no ClusterDataDir
	if d := b.xferInflightDir(); d != "" {
		t.Fatalf("no ClusterDataDir ⇒ no ledger dir, got %q", d)
	}
	b.writeXferInflight(&transferEntry{transferID: "x", sid: "s", tier: "b", startedAt: time.Now()})
	n, err := b.finalizeStrandedXfers(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("single-broker mode: no ledger, no finalize (n=%d err=%v)", n, err)
	}
}

// TestXferInflightDedupReqIDStable pins the #57 dedup keystone: two emissions of the SAME synthetic terminal
// (a crash-window re-emit) derive the SAME content reqID, so the replicated ledger collapses the duplicate.
func TestXferInflightDedupReqIDStable(t *testing.T) {
	now := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)
	e := strandedEntry(now)
	mk := func() schema.AuditTransfer {
		return schema.AuditTransfer{
			V: schema.AuditSchemaVersion, Kind: "failed", Verb: e.verb,
			Ts: e.startedAt.Add(transferTimeoutTierB), Session: e.sid, Node: e.nid,
			ActorNkey: e.actor, ActorFp: e.actorFP,
			TransferID: e.transferID, Path: e.path, Tier: e.tier, Bucket: e.bucket,
			DurationMs: transferTimeoutTierB.Milliseconds(), Code: "home_broker_restart",
			Error: "broker home restarted while the transfer was in flight; the crashed incarnation wrote no terminal audit (#57 finalize-on-recovery)",
		}
	}
	a, err := xferaudit.TransferRecordReqID(mk())
	if err != nil {
		t.Fatalf("reqID a: %v", err)
	}
	b2, err := xferaudit.TransferRecordReqID(mk())
	if err != nil {
		t.Fatalf("reqID b: %v", err)
	}
	if a != b2 || a == "" {
		t.Fatalf("re-emit of the deterministic synthetic terminal must yield an identical non-empty reqID (dedup), got %q vs %q", a, b2)
	}
}

// TestXferTerminalOutboxDoesNotDoubleEmitWithPrimary pins the one hazard the round-4 fallback outbox
// introduces. Staging now falls back to a SIBLING directory when the in-flight directory is unwritable,
// so a transfer can end up with a staged terminal in BOTH places (two terminal paths racing while the
// primary directory breaks in between). Recovery scans both directories, so the naive version replays
// each row independently — two terminals for one transfer, which is exactly the contradiction the outbox
// exists to prevent. The primary row owns the transfer; the outbox counterpart must be dropped, not
// re-emitted.
func TestXferTerminalOutboxDoesNotDoubleEmitWithPrimary(t *testing.T) {
	n, _ := d7SingleNode(t, "xfer-outbox-double-emit")
	now := time.Now().UTC()
	root := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	b.cl = &clusterRuntime{node: n}

	const tid = "xfer-outbox-double-emit"
	terminal := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now, Session: "sess", Node: "n1",
		TransferID: tid, Path: "/dst", Tier: "a", Bucket: "OBJ_xfer-sess",
	}
	rec := xferInflightRecord{
		TransferID: tid, Session: "sess", Node: "n1", Verb: "push", Tier: "a",
		Bucket: "OBJ_xfer-sess", Path: "/dst",
		StartedAt: now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
		Terminal:  &terminal,
	}
	if err := b.writeLedgerRecord(b.xferInflightDir(), rec); err != nil {
		t.Fatalf("seed primary ledger: %v", err)
	}
	if err := b.writeLedgerRecord(b.xferTerminalOutboxDir(), rec); err != nil {
		t.Fatalf("seed fallback outbox: %v", err)
	}

	var forwarded []schema.AuditTransfer
	b.transferAuditForwardSync = func(payload []byte) error {
		var got schema.AuditTransfer
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatalf("unmarshal forwarded terminal: %v", err)
		}
		forwarded = append(forwarded, got)
		return nil
	}
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if len(forwarded) != 1 {
		t.Fatalf("a transfer staged in BOTH ledger directories produced %d terminals, want exactly 1: %+v",
			len(forwarded), forwarded)
	}
	if _, err := os.Stat(filepath.Join(b.xferTerminalOutboxDir(), xferInflightFilename(tid))); !os.IsNotExist(err) {
		t.Fatalf("the redundant outbox row outlived the disposed primary row (stat err: %v) — every later "+
			"pass would re-emit it", err)
	}
}

// TestXferFallbackLedgerStateMatrix walks the fallback outbox through the four states round-5 asked for:
// the normal commit callback while the primary directory is unusable, the first recovery pass after the
// directory comes back, a repeated recovery pass, and the cleanup-failure-then-directory-recovery path
// that ties them together.
//
// The invariant under test is R16's, stated as an outcome rather than a file layout: a transfer ends with
// EXACTLY ONE DISTINCT terminal. A re-emitted identical record is not a violation (same bytes ⇒ same
// reqID ⇒ the replicated dedup ledger collapses it); a `complete` joined by a synthesized
// `failed/home_broker_restart` is, and that is precisely what the round-5 blocker produced.
func TestXferFallbackLedgerStateMatrix(t *testing.T) {
	n, _ := d7SingleNode(t, "xfer-fallback-state-matrix")
	now := time.Now().UTC()
	root := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	b.cl = &clusterRuntime{node: n}

	const tid = "xfer-fallback-state-matrix"
	entry := &transferEntry{
		transferID: tid, sid: "sess", nid: "n1", verb: "push", tier: "a",
		bucket: "OBJ_xfer-sess", path: "/dst",
		startedAt: now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
	b.writeXferInflight(entry) // a healthy start row, exactly as a live transfer leaves behind

	// STATE 1 — the primary directory becomes unusable while the transfer is in flight, and the terminal
	// is decided. Staging falls back to the outbox and the commit callback runs for real.
	ledgerDir := b.xferInflightDir()
	parked := ledgerDir + ".parked"
	if err := os.Rename(ledgerDir, parked); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ledgerDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	var terminals []schema.AuditTransfer
	b.transferAuditSink = func(rec schema.AuditTransfer, onCommitted func()) {
		terminals = append(terminals, rec)
		if onCommitted != nil {
			onCommitted() // the real commit callback, including its ledger cleanup
		}
	}
	b.emitTerminalTransferAudit(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now, Session: "sess", Node: "n1",
		TransferID: tid, Path: "/dst", Tier: "a", Bucket: "OBJ_xfer-sess",
	}, tid)
	if len(terminals) != 1 || terminals[0].Kind != "complete" {
		t.Fatalf("the decided terminal did not reach the wire: %+v", terminals)
	}
	// The cleanup could not touch the primary row, so the exact terminal MUST still be parked in the
	// outbox — it is the only thing that can out-rank the start row that is still hiding in `parked`.
	if _, err := os.Stat(filepath.Join(b.xferTerminalOutboxDir(), xferInflightFilename(tid))); err != nil {
		t.Fatalf("the exact terminal was dropped while the primary start row survived — the next pass would "+
			"synthesize a contradictory terminal: %v", err)
	}

	// STATE 2 — the directory comes back, still holding the old start row. First recovery pass.
	if err := os.Remove(ledgerDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(parked, ledgerDir); err != nil {
		t.Fatal(err)
	}
	b.transferAuditForwardSync = func(payload []byte) error {
		var rec schema.AuditTransfer
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Fatal(err)
		}
		terminals = append(terminals, rec)
		return nil
	}
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("first recovery pass: %v", err)
	}
	// STATE 3 — a repeated pass must find nothing left to do.
	before := len(terminals)
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("repeated recovery pass: %v", err)
	}
	if len(terminals) != before {
		t.Fatalf("a repeated recovery pass re-emitted %d terminal(s); the ledger rows were not consumed: %+v",
			len(terminals)-before, terminals[before:])
	}

	// The outcome invariant: one DISTINCT terminal, and never a synthesized contradiction.
	distinct := map[string]bool{}
	for _, rec := range terminals {
		distinct[rec.Kind+"/"+rec.Code] = true
		if rec.Code == "home_broker_restart" {
			t.Fatalf("recovery synthesized %q beside the real terminal — the exactly-one-terminal invariant "+
				"is broken: %+v", rec.Code, terminals)
		}
	}
	if len(distinct) != 1 {
		t.Fatalf("transfer ended with %d distinct terminals, want exactly 1: %v", len(distinct), distinct)
	}
	// Both credentials must be gone once the directory is usable again, or the landmine is merely delayed.
	if _, err := os.Stat(filepath.Join(ledgerDir, xferInflightFilename(tid))); !os.IsNotExist(err) {
		t.Fatalf("the primary start row outlived the committed terminal (stat err: %v)", err)
	}
	if _, err := os.Stat(filepath.Join(b.xferTerminalOutboxDir(), xferInflightFilename(tid))); !os.IsNotExist(err) {
		t.Fatalf("the outbox row outlived the committed terminal (stat err: %v)", err)
	}
}
