package cluster

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	bolt "go.etcd.io/bbolt"
)

// TestUnpublishedAuditOpsInLog pins the v0.4.4-review STEP1 fix: the resnapshot audit-window guard must
// count ONLY genuine audit-bearing ops (OpReconcileBatch / OpTransferAudit) above the cursor — NOT the raw
// LastIndex. A raw delta over-fired on every real migrated broker (config + LogNoop + the self-begetting
// trailing OpAuditCheckpointSet all sit above audit_published_index with zero real audit), forcing
// --accept-audit-loss for a phantom loss and making the documented restart-drain-stop remedy unreachable.
func TestUnpublishedAuditOpsInLog(t *testing.T) {
	dir := t.TempDir()
	_, boltPath := raftPaths(dir)
	if err := os.MkdirAll(filepath.Dir(boltPath), 0o700); err != nil {
		t.Fatal(err)
	}
	store, err := raftboltdb.New(raftboltdb.Options{
		Path:        boltPath,
		BoltOptions: &bolt.Options{Timeout: boltLockProbeTimeout},
	})
	if err != nil {
		t.Fatalf("open boltstore: %v", err)
	}
	mustEncode := func(op OpType) []byte {
		b, e := NewCommand(op, Stmt("SELECT 1")).encode()
		if e != nil {
			t.Fatalf("encode %s: %v", op, e)
		}
		return b
	}
	// A realistic post-migration log: config@1, an election LogNoop, the publisher's self-begetting cursor
	// op, exactly ONE real audit op, and a non-audit command (NodeRegister) — whose state is fully captured
	// by the SQLite snapshot, so truncating it loses nothing.
	logs := []*raft.Log{
		{Index: 1, Term: 1, Type: raft.LogConfiguration, Data: []byte("cfg")},
		{Index: 2, Term: 1, Type: raft.LogNoop},
		{Index: 3, Term: 1, Type: raft.LogCommand, Data: mustEncode(OpAuditCheckpointSet)},
		{Index: 4, Term: 1, Type: raft.LogCommand, Data: mustEncode(OpReconcileBatch)},
		{Index: 5, Term: 1, Type: raft.LogCommand, Data: mustEncode(OpNodeRegister)},
	}
	for _, l := range logs {
		if err := store.StoreLog(l); err != nil {
			t.Fatalf("store log %d: %v", l.Index, err)
		}
	}
	_ = store.Close()

	// From cursor 0: only index 4 (OpReconcileBatch) is audit-bearing — config/noop/checkpoint/register all
	// skipped. The OLD raw `LastIndex(5) > published(0)` guard would have over-counted 5 → spurious refuse.
	count, first, err := UnpublishedAuditOpsInLog(dir, 0)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if count != 1 || first != 4 {
		t.Fatalf("want count=1 first=4 (only OpReconcileBatch counts), got count=%d first=%d", count, first)
	}

	// Cursor at/after the audit op → nothing unpublished, so a clean broker resnapshots without the flag.
	if count, _, err = UnpublishedAuditOpsInLog(dir, 4); err != nil || count != 0 {
		t.Fatalf("want count=0 above the audit op, got count=%d err=%v", count, err)
	}

	// A poison (undecodable) command in the window is FAIL-CLOSED counted — its op is unknown, so we cannot
	// prove it carries no audit, and refusing (operator passes --accept-audit-loss) is the safe default.
	store2, err := raftboltdb.New(raftboltdb.Options{
		Path:        boltPath,
		BoltOptions: &bolt.Options{Timeout: boltLockProbeTimeout},
	})
	if err != nil {
		t.Fatalf("reopen boltstore: %v", err)
	}
	if err := store2.StoreLog(&raft.Log{Index: 6, Term: 1, Type: raft.LogCommand, Data: []byte("{not-json")}); err != nil {
		t.Fatalf("store poison: %v", err)
	}
	_ = store2.Close()
	if count, _, err = UnpublishedAuditOpsInLog(dir, 4); err != nil || count != 1 {
		t.Fatalf("want count=1 (poison fail-closed), got count=%d err=%v", count, err)
	}
}
