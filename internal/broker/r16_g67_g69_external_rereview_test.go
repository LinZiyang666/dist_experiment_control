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

// TestExternalRereviewCommittedTerminalCrashDoesNotCreateContradiction covers the
// other side of the commit/delete boundary introduced by the F1 response.
//
// A successful forward means the real terminal is durably committed. A process
// can still exit before the in-memory onCommitted callback unlinks the ledger.
// On restart, that surviving start-only ledger must not cause a different
// synthetic failed terminal to be committed beside the real one.
func TestExternalRereviewCommittedTerminalCrashDoesNotCreateContradiction(t *testing.T) {
	now := time.Now().UTC()
	dir := t.TempDir()
	first := &Broker{transfers: newTransferTracker()}
	first.cfg = Config{ClusterDataDir: dir, Logger: silentLogger(), Now: func() time.Time { return now }}

	entry := &transferEntry{
		transferID: "external-rereview-post-commit-window",
		sid:        "sess",
		nid:        "n1",
		verb:       "push",
		tier:       "a",
		bucket:     "OBJ_xfer-sess",
		path:       "/dst",
		startedAt:  now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
	if code := first.transfers.put(entry); code != "" {
		t.Fatalf("put: %s", code)
	}
	first.writeXferInflight(entry)

	var committed []schema.AuditTransfer
	first.transferAuditSink = func(rec schema.AuditTransfer, _ func()) {
		committed = append(committed, rec)
		// Simulate SIGKILL after forward returned success but before the callback.
	}
	first.cleanupEntry(entry, "forward_failed", "peer went away")
	if len(committed) != 1 || committed[0].Code != "forward_failed" {
		t.Fatalf("fixture did not commit the real terminal: %+v", committed)
	}

	restarted := &Broker{transfers: newTransferTracker()}
	restarted.cfg = Config{ClusterDataDir: dir, Logger: silentLogger(), Now: func() time.Time { return now }}
	restarted.transferAuditForwardSync = func(payload []byte) error {
		var rec schema.AuditTransfer
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Fatalf("decode recovery terminal: %v", err)
		}
		committed = append(committed, rec)
		return nil
	}
	if _, err := restarted.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if len(committed) != 1 {
		t.Fatalf("a crash after the real terminal committed produced a contradictory recovery terminal: %+v; "+
			"the ledger must preserve/replay the exact terminal identity or recovery must detect the prior commit",
			committed)
	}
}

// TestExternalRereviewReqIDLookupFailureRetainsTerminalOutbox covers the
// recovery verifier's fail-closed behavior. A local read error is not evidence
// that the terminal committed; deleting the only exact terminal outbox in that
// state can turn a pre-commit crash into a permanent dangling start.
func TestExternalRereviewReqIDLookupFailureRetainsTerminalOutbox(t *testing.T) {
	n, _ := d7SingleNode(t, "external-rereview-closed-db")
	if err := n.Shutdown(); err != nil {
		t.Fatalf("close cluster node: %v", err)
	}

	now := time.Now().UTC()
	dir := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: dir, Logger: silentLogger(), Now: func() time.Time { return now }}
	b.cl = &clusterRuntime{node: n}

	entry := &transferEntry{
		transferID: "external-rereview-lookup-error",
		sid:        "sess",
		nid:        "n1",
		verb:       "push",
		tier:       "a",
		bucket:     "OBJ_xfer-sess",
		path:       "/dst",
		startedAt:  now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
	b.writeXferInflight(entry)
	terminal := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "failed", Verb: "push",
		Ts: now, Session: "sess", Node: "n1",
		TransferID: entry.transferID, Path: entry.path, Tier: entry.tier, Bucket: entry.bucket,
		Code: "forward_failed", Error: "peer went away",
	}
	if staged, _ := b.stageXferInflightTerminal(entry.transferID, terminal); !staged {
		t.Fatal("fixture: stage exact terminal")
	}

	emitted := 0
	b.transferAuditForwardSync = func([]byte) error {
		emitted++
		return nil
	}
	finalized, err := b.finalizeStrandedXfers(context.Background())
	if err != nil {
		t.Fatalf("finalize: %v", err)
	}
	ledger := filepath.Join(dir, "xfer-inflight", xferInflightFilename(entry.transferID))
	if _, statErr := os.Stat(ledger); statErr != nil {
		t.Fatalf("an unknown commit state deleted the only exact terminal outbox: %v", statErr)
	}
	if finalized != 0 || emitted != 0 {
		t.Fatalf("unknown commit state must retain evidence for retry, got finalized=%d emitted=%d", finalized, emitted)
	}
}

// TestExternalRereviewTerminalStageFailureCannotCreateContradiction proves that
// "stage before forward" must be a checked precondition, not best effort. If the
// stage fails but forwarding continues, a post-commit crash leaves the old
// start-only ledger and recovery guesses a different terminal.
func TestExternalRereviewTerminalStageFailureCannotCreateContradiction(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	first := &Broker{transfers: newTransferTracker()}
	first.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	entry := &transferEntry{
		transferID: "external-rereview-stage-failure",
		sid:        "sess",
		nid:        "n1",
		verb:       "push",
		tier:       "a",
		bucket:     "OBJ_xfer-sess",
		path:       "/dst",
		startedAt:  now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
	first.writeXferInflight(entry)

	// Make the ledger directory temporarily unavailable in a deterministic way:
	// park the valid start ledger and put a regular file at the directory path.
	ledgerDir := filepath.Join(root, "xfer-inflight")
	parkedDir := ledgerDir + ".parked"
	if err := os.Rename(ledgerDir, parkedDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ledgerDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}

	var committed []schema.AuditTransfer
	first.transferAuditSink = func(rec schema.AuditTransfer, _ func()) {
		committed = append(committed, rec)
		// Simulate commit followed by SIGKILL before the callback.
	}
	realTerminal := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now, Session: "sess", Node: "n1",
		TransferID: entry.transferID, Path: entry.path, Tier: entry.tier, Bucket: entry.bucket,
	}
	first.emitTerminalTransferAudit(realTerminal, entry.transferID)

	if err := os.Remove(ledgerDir); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(parkedDir, ledgerDir); err != nil {
		t.Fatal(err)
	}

	restarted := &Broker{transfers: newTransferTracker()}
	restarted.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	restarted.transferAuditForwardSync = func(payload []byte) error {
		var rec schema.AuditTransfer
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Fatal(err)
		}
		committed = append(committed, rec)
		return nil
	}
	if _, err := restarted.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("finalize: %v", err)
	}
	if len(committed) != 1 {
		t.Fatalf("terminal staging failed but forwarding continued, so recovery committed a contradictory terminal: %+v", committed)
	}
}

// TestExternalRereviewTerminalStageFailureNeedsExistingRecoveryEvidence
// covers the premise introduced by the round-4 fix: suppressing the real
// terminal is safe only if a start ledger actually survived. The initial ledger
// write is still best-effort, so the same persistent filesystem failure can
// make both writes fail and leave recovery with nothing.
func TestExternalRereviewTerminalStageFailureNeedsExistingRecoveryEvidence(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	ledgerDir := filepath.Join(root, "xfer-inflight")
	if err := os.WriteFile(ledgerDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}

	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	entry := &transferEntry{
		transferID: "external-rereview-no-start-ledger",
		sid:        "sess",
		nid:        "n1",
		verb:       "push",
		tier:       "a",
		bucket:     "OBJ_xfer-sess",
		path:       "/dst",
		startedAt:  now,
	}
	// Production currently logs and continues when this write fails; it then
	// emits a start audit and forwards the transfer.
	b.writeXferInflight(entry)

	forwarded := 0
	b.transferAuditSink = func(schema.AuditTransfer, func()) { forwarded++ }
	b.emitTerminalTransferAudit(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now.Add(time.Second), Session: entry.sid, Node: entry.nid,
		TransferID: entry.transferID, Path: entry.path, Tier: entry.tier, Bucket: entry.bucket,
	}, entry.transferID)

	ledger := filepath.Join(ledgerDir, xferInflightFilename(entry.transferID))
	_, statErr := os.Stat(ledger)
	if forwarded == 0 && statErr != nil {
		t.Fatalf("terminal was suppressed after the initial best-effort ledger write also failed: "+
			"no forward and no recovery evidence (ledger error: %v)", statErr)
	}
}

// TestExternalRereviewTerminalStageFailureNeedsCurrentRecoveryEvidence covers
// the distinction between "this process once wrote a start ledger" and "a
// recovery record still exists now". The data directory can disappear or be
// replaced while the broker keeps running (for example after a mount failure or
// operator recovery). A stale in-memory success bit must not suppress the only
// terminal when staging also fails.
func TestExternalRereviewTerminalStageFailureNeedsCurrentRecoveryEvidence(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	entry := &transferEntry{
		transferID: "external-rereview-lost-start-ledger",
		sid:        "sess",
		nid:        "n1",
		verb:       "push",
		tier:       "a",
		bucket:     "OBJ_xfer-sess",
		path:       "/dst",
		startedAt:  now,
	}
	b.writeXferInflight(entry)

	ledgerDir := filepath.Join(root, "xfer-inflight")
	if err := os.RemoveAll(ledgerDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ledgerDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}

	forwarded := 0
	b.transferAuditSink = func(schema.AuditTransfer, func()) { forwarded++ }
	b.emitTerminalTransferAudit(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now.Add(time.Second), Session: entry.sid, Node: entry.nid,
		TransferID: entry.transferID, Path: entry.path, Tier: entry.tier, Bucket: entry.bucket,
	}, entry.transferID)

	ledger := filepath.Join(ledgerDir, xferInflightFilename(entry.transferID))
	_, statErr := os.Stat(ledger)
	if forwarded == 0 && statErr != nil {
		t.Fatalf("terminal was suppressed using a stale in-memory 'ledger was written' bit: "+
			"the ledger has since disappeared, so recovery has no evidence (ledger error: %v)", statErr)
	}
}

// TestExternalRereviewFallbackReplayDisposesPrimaryStartLedger covers the
// canonical state produced by fallback staging: the primary directory still
// contains the old start-only record while the sibling outbox contains the
// exact terminal. Once recovery commits that terminal, it must dispose of both
// records. Otherwise the next pass sees the surviving start-only record and
// synthesizes a contradictory home_broker_restart terminal.
func TestExternalRereviewFallbackReplayDisposesPrimaryStartLedger(t *testing.T) {
	n, _ := d7SingleNode(t, "external-rereview-outbox-primary-start")
	now := time.Now().UTC()
	root := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	b.cl = &clusterRuntime{node: n}

	const tid = "external-rereview-outbox-primary-start"
	started := now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second)
	start := xferInflightRecord{
		TransferID: tid, Session: "sess", Node: "n1", Verb: "push", Tier: "a",
		Bucket: "OBJ_xfer-sess", Path: "/dst", StartedAt: started,
	}
	terminal := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now, Session: start.Session, Node: start.Node,
		TransferID: tid, Path: start.Path, Tier: start.Tier, Bucket: start.Bucket,
	}
	staged := start
	staged.Terminal = &terminal
	if err := b.writeLedgerRecord(b.xferInflightDir(), start); err != nil {
		t.Fatalf("seed primary start ledger: %v", err)
	}
	if err := b.writeLedgerRecord(b.xferTerminalOutboxDir(), staged); err != nil {
		t.Fatalf("seed fallback terminal outbox: %v", err)
	}

	var forwarded []schema.AuditTransfer
	b.transferAuditForwardSync = func(payload []byte) error {
		var got schema.AuditTransfer
		if err := json.Unmarshal(payload, &got); err != nil {
			t.Fatalf("decode forwarded terminal: %v", err)
		}
		forwarded = append(forwarded, got)
		return nil
	}
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("first finalize: %v", err)
	}
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("second finalize: %v", err)
	}
	if len(forwarded) != 1 || forwarded[0].Kind != "complete" {
		t.Fatalf("fallback replay left the primary start ledger to synthesize a contradictory terminal "+
			"on the next pass: %+v", forwarded)
	}
	for _, dir := range []string{b.xferInflightDir(), b.xferTerminalOutboxDir()} {
		if _, err := os.Stat(filepath.Join(dir, xferInflightFilename(tid))); !os.IsNotExist(err) {
			t.Fatalf("committed fallback terminal left recovery evidence in %s (stat err: %v)", dir, err)
		}
	}
}

// TestExternalRereviewFallbackOutboxWorksWhilePrimaryUnavailable verifies the
// reason a sibling outbox exists at all: recovery must be able to replay it
// while the primary directory is still broken. Treating a primary ReadDir
// error as fatal before the outbox pass makes the fallback unusable in the
// exact failure state it was introduced to survive.
func TestExternalRereviewFallbackOutboxWorksWhilePrimaryUnavailable(t *testing.T) {
	n, _ := d7SingleNode(t, "external-rereview-outbox-primary-broken")
	now := time.Now().UTC()
	root := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	b.cl = &clusterRuntime{node: n}

	if err := os.WriteFile(b.xferInflightDir(), []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("break primary ledger path: %v", err)
	}
	const tid = "external-rereview-outbox-primary-broken"
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
	if err := b.writeLedgerRecord(b.xferTerminalOutboxDir(), rec); err != nil {
		t.Fatalf("seed fallback outbox: %v", err)
	}

	forwarded := 0
	b.transferAuditForwardSync = func([]byte) error {
		forwarded++
		return nil
	}
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("fallback recovery was blocked by the still-broken primary directory: %v", err)
	}
	if forwarded != 1 {
		t.Fatalf("fallback terminal was not replayed while the primary directory remained unavailable: %d",
			forwarded)
	}
}

// TestExternalRereviewFallbackCommitDoesNotExposeOldPrimaryStart covers the
// normal (non-recovery) commit callback. If the primary directory is only
// temporarily unavailable, fallback staging succeeds and forwarding commits,
// but cleanup must not delete the exact outbox while failing to delete the
// hidden primary start. Restoring that directory would otherwise expose a
// start-only record and recovery would synthesize a second terminal.
func TestExternalRereviewFallbackCommitDoesNotExposeOldPrimaryStart(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	first := &Broker{transfers: newTransferTracker()}
	first.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	entry := &transferEntry{
		transferID: "external-rereview-fallback-commit-hidden-start",
		sid:        "sess",
		nid:        "n1",
		verb:       "push",
		tier:       "a",
		bucket:     "OBJ_xfer-sess",
		path:       "/dst",
		startedAt:  now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
	first.writeXferInflight(entry)

	primary := first.xferInflightDir()
	parked := primary + ".parked"
	if err := os.Rename(primary, parked); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(primary, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}

	var committed []schema.AuditTransfer
	first.transferAuditSink = func(rec schema.AuditTransfer, onCommitted func()) {
		committed = append(committed, rec)
		onCommitted()
	}
	first.emitTerminalTransferAudit(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now, Session: entry.sid, Node: entry.nid,
		TransferID: entry.transferID, Path: entry.path, Tier: entry.tier, Bucket: entry.bucket,
	}, entry.transferID)

	if err := os.Remove(primary); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(parked, primary); err != nil {
		t.Fatal(err)
	}

	restarted := &Broker{transfers: newTransferTracker()}
	restarted.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	restarted.transferAuditForwardSync = func(payload []byte) error {
		var rec schema.AuditTransfer
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Fatal(err)
		}
		committed = append(committed, rec)
		return nil
	}
	if _, err := restarted.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("finalize after restoring primary: %v", err)
	}
	if len(committed) != 1 {
		t.Fatalf("commit cleanup deleted the exact fallback outbox but failed to delete the hidden primary "+
			"start, so recovery produced a contradictory terminal: %+v", committed)
	}
}

// TestExternalRereviewUnreadableOutboxCannotAuthorizeSyntheticTerminal checks
// the inverse of the developer's primary-unavailable fix. If the outbox cannot
// be scanned, an empty outboxOwned census is not proof that no exact terminal
// exists there. Synthesizing from a primary start in that state can commit a
// false failure; when the outbox returns, its real terminal becomes a second,
// contradictory outcome.
func TestExternalRereviewUnreadableOutboxCannotAuthorizeSyntheticTerminal(t *testing.T) {
	n, _ := d7SingleNode(t, "external-rereview-outbox-unreadable")
	now := time.Now().UTC()
	root := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	b.cl = &clusterRuntime{node: n}

	const tid = "external-rereview-outbox-unreadable"
	start := xferInflightRecord{
		TransferID: tid, Session: "sess", Node: "n1", Verb: "push", Tier: "a",
		Bucket: "OBJ_xfer-sess", Path: "/dst",
		StartedAt: now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
	terminal := schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now, Session: start.Session, Node: start.Node,
		TransferID: tid, Path: start.Path, Tier: start.Tier, Bucket: start.Bucket,
	}
	staged := start
	staged.Terminal = &terminal
	if err := b.writeLedgerRecord(b.xferInflightDir(), start); err != nil {
		t.Fatalf("seed primary start: %v", err)
	}
	if err := b.writeLedgerRecord(b.xferTerminalOutboxDir(), staged); err != nil {
		t.Fatalf("seed exact outbox terminal: %v", err)
	}

	// Simulate an outbox mount/path that is temporarily unavailable while its
	// durable contents survive and later return.
	outbox := b.xferTerminalOutboxDir()
	parked := outbox + ".parked"
	if err := os.Rename(outbox, parked); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outbox, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
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
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("one unreadable source should not make the whole pass fail: %v", err)
	}

	// Restore the exact-terminal source and let the next pass see what the
	// first pass was not entitled to assume absent.
	if err := os.Remove(outbox); err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err := os.Rename(parked, outbox); err != nil {
		t.Fatal(err)
	}
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("finalize after outbox returns: %v", err)
	}

	distinct := map[string]bool{}
	for _, rec := range committed {
		distinct[rec.Kind+"/"+rec.Code] = true
	}
	if len(distinct) != 1 || !distinct["complete/"] {
		t.Fatalf("an unreadable outbox was treated as an empty outbox, authorizing a contradictory synthetic "+
			"terminal before the exact terminal returned: %+v", committed)
	}
}

// TestExternalRereviewMissingPrimaryPathIsNotConfirmedLedgerAbsence checks
// cleanup's ENOENT interpretation. A missing mount/directory path can hide a
// durable primary start; it does not prove that row was durably removed. The
// exact outbox terminal must survive until the primary source returns.
func TestExternalRereviewMissingPrimaryPathIsNotConfirmedLedgerAbsence(t *testing.T) {
	now := time.Now().UTC()
	root := t.TempDir()
	first := &Broker{transfers: newTransferTracker()}
	first.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	entry := &transferEntry{
		transferID: "external-rereview-primary-path-missing-at-cleanup",
		sid:        "sess",
		nid:        "n1",
		verb:       "push",
		tier:       "a",
		bucket:     "OBJ_xfer-sess",
		path:       "/dst",
		startedAt:  now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
	first.writeXferInflight(entry)

	primary := first.xferInflightDir()
	parked := primary + ".parked"
	if err := os.Rename(primary, parked); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(primary, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}

	var committed []schema.AuditTransfer
	first.transferAuditSink = func(rec schema.AuditTransfer, onCommitted func()) {
		committed = append(committed, rec)
		// The primary mount/path disappears between successful fallback
		// staging and commit cleanup. Its old durable contents still exist.
		if err := os.Remove(primary); err != nil {
			t.Fatal(err)
		}
		onCommitted()
	}
	first.emitTerminalTransferAudit(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now, Session: entry.sid, Node: entry.nid,
		TransferID: entry.transferID, Path: entry.path, Tier: entry.tier, Bucket: entry.bucket,
	}, entry.transferID)

	if err := os.Rename(parked, primary); err != nil {
		t.Fatal(err)
	}
	n, _ := d7SingleNode(t, "external-rereview-primary-path-missing-at-cleanup")
	restarted := &Broker{transfers: newTransferTracker()}
	restarted.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	restarted.cl = &clusterRuntime{node: n}
	restarted.transferAuditForwardSync = func(payload []byte) error {
		var rec schema.AuditTransfer
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Fatal(err)
		}
		committed = append(committed, rec)
		return nil
	}
	if _, err := restarted.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("finalize after primary path returns: %v", err)
	}

	distinct := map[string]bool{}
	for _, rec := range committed {
		distinct[rec.Kind+"/"+rec.Code] = true
	}
	if len(distinct) != 1 || !distinct["complete/"] {
		t.Fatalf("ENOENT below a missing primary directory was treated as confirmed durable row absence, so "+
			"cleanup deleted the exact outbox and recovery synthesized a contradiction: %+v", committed)
	}
}
