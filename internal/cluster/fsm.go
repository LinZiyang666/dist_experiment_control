package cluster

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
)

// cluster_meta KV keys holding the FSM's durable apply cursor (architecture
// §3.7: applied_index authority = SQLite, written in the SAME txn as the op).
const (
	metaKeyAppliedIndex = "applied_index"
	metaKeyAppliedTerm  = "applied_term"
)

// Fail-stop retry policy (§3.7 D1 amendment). raft advances lastApplied after
// FSM.Apply returns regardless of the returned value, so a committed command must
// never be returned un-applied. A transient Begin/Commit failure is retried a few
// times; if it still fails the node FAIL-STOPS (panic) rather than letting raft
// advance past an unapplied entry that a co-occurring snapshot could then drop.
const (
	applyMaxAttempts  = 3
	applyRetryBackoff = 50 * time.Millisecond
)

// reqIDRetentionWindow bounds the cluster_reqid_ledger (§4.1 D4). It is in
// RAFT-INDEX units and must exceed the maximum forwarder-retry horizon — bounded by
// agent NATS reconnect backoff + the not_leader bounce loop (seconds-scale)
// expressed in indices at the control-plane write rate (LOW: session-control
// writes, not data-plane). 1<<20 ≈ 1M indices = months of operation before a key is
// evicted, so a live retry is never aged out prematurely while growth stays bounded.
// Decoupled from D5's raft_index:kind:seq audit-dedup window (which sizes itself).
// A var (not const) only so the in-package GC vacuity test can shrink it; production
// never reassigns it. Mutating it from a test is safe ONLY because no internal/cluster
// test calls t.Parallel() (a parallel test would observe the shrunk window + race it).
var reqIDRetentionWindow uint64 = 1 << 20

// fsm implements raft.FSM over the cluster SQLite write pool.
type fsm struct {
	db     *sql.DB // the SetMaxOpenConns(1) write pool
	ro     *sql.DB // dedicated read-only handle (online-backup source, §3.8)
	tmpDir string  // same-filesystem scratch for snapshot/restore temp files
	dbPath string  // path of the live cluster DB file (for size guards/tests)

	appliers map[OpType]Applier
	logger   *slog.Logger

	// reapplyCount counts entries short-circuited as verified no-ops (§3.7
	// invariant #2), so the kill-9 matrix can assert the idempotent-skip path
	// actually fired (non-vacuity). Atomic; read by tests.
	reapplyCount atomic.Uint64

	// dedupCount counts entries short-circuited by the §4.1 D4 cross-retry ReqID
	// ledger (a committed-but-ack-lost forwarder retry arriving at a NEW index), so
	// the forwarding-idempotency tests can assert the dedup branch actually fired
	// (non-vacuity — the in-scope writes are SQL-idempotent so a row-count assertion
	// alone is vacuous). Atomic; read by tests + Node.DedupCount.
	dedupCount atomic.Uint64

	// rejectCount counts entries a custom applier DETERMINISTICALLY rejected
	// (errAppliedRejected — D7 §8.1 join-PoP verify fail), so the forged-sig test can
	// assert the poison-skip branch actually fired (non-vacuity: a "no row written"
	// assertion alone cannot tell rejection from a never-proposed entry). Atomic.
	rejectCount atomic.Uint64
}

// Apply result sentinels (returned via raft ApplyFuture.Response()).
type appliedOK struct{ index uint64 }       // op executed + applied_index advanced
type appliedNoOp struct{ index uint64 }     // idempotent re-apply skip (§3.7 #2); nothing written
type appliedPoison struct{ index uint64 }   // poison entry skipped, applied_index advanced past it (§2.8)
type appliedDedup struct{ index uint64 }    // §4.1 D4: ReqID already committed; op SKIPPED but applied_index ADVANCED + committed (NOT a rollback — distinct from appliedNoOp)
type appliedRejected struct{ index uint64 } // D7 §8.1: a custom applier DETERMINISTICALLY rejected the op (e.g. join-PoP verify fail); op SQL ran NONE, applied_index ADVANCED + committed, never panicked

// INVARIANT (load-bearing): the committed-OUTCOME FSM result types — appliedOK / appliedNoOp /
// appliedPoison / appliedDedup / appliedRejected — MUST NOT implement the `error` interface. They
// are returned as the raft Apply *Response* (a successful, durably-committed entry whose op may
// have been skipped). Node.Apply turns any error-typed Response into a returned error, so giving
// one of these an Error() method would make a poison-skip (a successful commit) look like an Apply
// FAILURE to the proposer — which silently broke the D7 forged-sig poison-skip path (a `func
// (appliedRejected) Error()` added in "v2 plish" made `Node.Apply(forged) -> error` instead of
// nil). A genuine infra failure rides fut.Error() / a fail-stop panic, NOT a Response. Do NOT add
// an Error() method to any applied* result type.

// errAppliedRejected is the sentinel a custom Applier returns to signal a
// DETERMINISTIC, op-level rejection (D7 §8.1: the join-PoP signature did not
// verify). It is NOT a transient infra failure: applyCommand catches it BEFORE the
// fail-stop retry loop and treats it like a poison entry (advance applied_index,
// run no op SQL, commit, never panic). This is mandatory — returning a plain error
// would feed the §3.7 retry→panic path, so a single forged committed entry from a
// compromised/buggy leader would brick EVERY replica on every boot's log replay
// (a one-shot remote wedge §2.8 forbids; §18.2.4 accepts a compromised leader can
// propose garbage, but it must NOT be able to halt honest replicas). Because the
// verdict is a pure function of committed bytes, every replica rejects identically
// — no DB fork. An Applier wraps it (errors.Is-detectable) only when it has
// executed NO statement; it must never be returned after a partial write.
var errAppliedRejected = errors.New("cluster: applier rejected op deterministically")

// Apply runs one committed log entry (architecture §3.2/§3.7). raft re-applies
// log[lastSnapshot+1 .. commit] UNCONDITIONALLY on restart, so the FSM must be
// self-idempotent by reading its OWN durable applied_index, never by trusting
// raft to skip.
func (f *fsm) Apply(l *raft.Log) any {
	// raft delivers configuration/noop entries here too; only LogCommand carries
	// an op (config entries do NOT advance applied_index — see §5.1 note #2).
	if l.Type != raft.LogCommand {
		return nil
	}
	cmd, derr := decodeCommand(l.Data)
	if derr != nil {
		// Poison entry (§2.8): a committed but structurally-invalid entry. A
		// committed entry is durable; returning an error would wedge the FSM
		// re-applying it forever. Log loudly and ADVANCE applied_index past it as
		// a no-op so a programming/version bug cannot deadlock the cluster.
		f.logger.Error("cluster: poison entry, advancing applied_index past it as a no-op",
			"index", l.Index, "term", l.Term, "err", derr)
		cmd = nil
	}
	// raft advances its lastApplied (and thus a later snapshot.Index) AFTER Apply
	// returns, REGARDLESS of the returned value (raft@v1.7.3 fsm.go runFSM). So a
	// committed command must NEVER be returned un-applied: that would let a
	// co-occurring snapshot pin meta.Index past the durable applied_index, and the
	// entry would be silently DROPPED on a post-snapshot restart (§3.7 D1
	// amendment). On a transient infra failure (Begin/Commit) retry; if it still
	// fails, FAIL-STOP (panic) so the node halts rather than diverging from the log
	// — on restart the (snapshot-free) log replay re-delivers the entry.
	var lastErr error
	for attempt := 1; attempt <= applyMaxAttempts; attempt++ {
		res, err := f.applyCommand(l, cmd)
		if err == nil {
			return res
		}
		lastErr = err
		f.logger.Error("cluster: apply txn failed, retrying", "index", l.Index, "term", l.Term, "attempt", attempt, "err", err)
		time.Sleep(applyRetryBackoff)
	}
	panic(fmt.Sprintf("cluster: FATAL fail-stop: cannot durably apply committed entry %d after %d attempts: %v",
		l.Index, applyMaxAttempts, lastErr))
}

// applyCommand performs the single-txn apply: read cursor, idempotent skip, exec
// op (unless poison), same-txn applied_index UPSERT, commit. A nil cmd is a
// poison entry that advances the cursor without executing any op SQL.
func (f *fsm) applyCommand(l *raft.Log, cmd *Command) (any, error) {
	tx, err := f.db.Begin()
	if err != nil {
		return nil, fmt.Errorf("cluster: begin apply txn: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	applied, err := readAppliedIndexTx(tx)
	if err != nil {
		return nil, err
	}
	// §3.7 invariant #2 — verified idempotent no-op. The txn is rolled back by the
	// defer; NOTHING is written for an already-applied entry.
	if l.Index <= applied {
		f.reapplyCount.Add(1)
		return appliedNoOp{l.Index}, nil
	}

	// §4.1 D4 cross-retry dedup — runs AFTER the index-skip (so raft re-delivery of
	// the SAME index stays on the cheap reapply path) and ONLY for a NEW index
	// (l.Index > applied). A committed-but-ack-lost forwarder retry re-proposes the
	// SAME ReqID at a NEW index; if the ledger already holds it the op ran on a prior
	// committed entry, so SKIP the op SQL but STILL advance applied_index and COMMIT
	// (raft advances lastApplied regardless of return value; a rollback here would
	// wedge re-delivery forever). Distinct from appliedNoOp, which rolls back.
	if cmd != nil && cmd.ReqID != "" {
		seen, err := reqIDSeenTx(tx, cmd.ReqID)
		if err != nil {
			return nil, err
		}
		if seen {
			f.dedupCount.Add(1)
			if err := writeAppliedIndexTx(tx, l.Index, l.Term); err != nil {
				return nil, err
			}
			if applyFailHook != nil {
				if err := applyFailHook(l.Index); err != nil {
					return nil, fmt.Errorf("cluster: injected apply failure @%d: %w", l.Index, err)
				}
			}
			if applyCommitGate != nil {
				applyCommitGate(l.Index)
			}
			if err := tx.Commit(); err != nil {
				return nil, fmt.Errorf("cluster: commit dedup txn @%d: %w", l.Index, err)
			}
			committed = true
			return appliedDedup{l.Index}, nil
		}
	}

	if cmd != nil {
		applier := f.appliers[cmd.Op]
		if applier == nil {
			// Unreachable: decodeCommand validated the op against knownOps.
			return nil, fmt.Errorf("cluster: no applier registered for op %q", cmd.Op)
		}
		if err := applier.ApplyTx(tx, cmd); err != nil {
			// D7 §8.1: a DETERMINISTIC op-level rejection (the applier ran NO op SQL,
			// e.g. join-PoP signature did not verify). Treat exactly like a poison
			// entry — advance applied_index + commit + return appliedRejected — NEVER
			// return the error (that retries→panics, letting a forged committed entry
			// brick every replica on log replay). Every replica reaches the identical
			// deterministic verdict, so the roster row is written nowhere; no fork.
			if errors.Is(err, errAppliedRejected) {
				f.rejectCount.Add(1)
				f.logger.Error("cluster: op rejected by applier (deterministic), advancing applied_index past it as a no-op",
					"op", cmd.Op, "index", l.Index, "term", l.Term, "err", err)
				if werr := writeAppliedIndexTx(tx, l.Index, l.Term); werr != nil {
					return nil, werr
				}
				if applyFailHook != nil {
					if ferr := applyFailHook(l.Index); ferr != nil {
						return nil, fmt.Errorf("cluster: injected apply failure @%d: %w", l.Index, ferr)
					}
				}
				if applyCommitGate != nil {
					applyCommitGate(l.Index)
				}
				if cerr := tx.Commit(); cerr != nil {
					return nil, fmt.Errorf("cluster: commit rejected txn @%d: %w", l.Index, cerr)
				}
				committed = true
				return appliedRejected{l.Index}, nil
			}
			return nil, fmt.Errorf("cluster: apply %s @%d: %w", cmd.Op, l.Index, err)
		}
		// NEW-ReqID path: record it + deterministic same-txn GC so a later retry
		// dedups (above) and the ledger stays bounded. Every replica runs the
		// identical INSERT + range-DELETE at the identical index (no wall-clock).
		if cmd.ReqID != "" {
			if err := insertReqIDTx(tx, cmd.ReqID, l.Index); err != nil {
				return nil, err
			}
			if err := gcReqIDLedgerTx(tx, l.Index); err != nil {
				return nil, err
			}
		}
	}

	// Same-txn cursor write (§3.7 invariant). UPSERT, NEVER bare UPDATE — on the
	// D0-empty cluster_meta a bare UPDATE writes zero rows and applied_index
	// silently never advances.
	if err := writeAppliedIndexTx(tx, l.Index, l.Term); err != nil {
		return nil, err
	}

	// Test-only seam (nil in production): inject a transient commit failure to
	// exercise the fail-stop retry/panic path (§3.7 D1 amendment).
	if applyFailHook != nil {
		if err := applyFailHook(l.Index); err != nil {
			return nil, fmt.Errorf("cluster: injected apply failure @%d: %w", l.Index, err)
		}
	}
	// Test-only seam (nil in production): block here so a kill-9 lands provably
	// inside the "raft committed, SQLite NOT yet committed" window (§13.4 FP1).
	if applyCommitGate != nil {
		applyCommitGate(l.Index)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("cluster: commit apply txn @%d: %w", l.Index, err)
	}
	committed = true
	if cmd == nil {
		return appliedPoison{l.Index}, nil // committed an applied_index advance, ran no op
	}
	return appliedOK{l.Index}, nil
}

// readAppliedIndexTx reads the durable apply cursor inside the txn. A missing row
// (D0 ships cluster_meta empty) reads as 0.
func readAppliedIndexTx(tx *sql.Tx) (uint64, error) {
	var v string
	err := tx.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, metaKeyAppliedIndex).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("cluster: read applied_index: %w", err)
	}
	n, perr := parseUint(v)
	if perr != nil {
		return 0, fmt.Errorf("cluster: corrupt applied_index %q: %w", v, perr)
	}
	return n, nil
}

// writeAppliedIndexTx upserts applied_index + applied_term in the same txn.
func writeAppliedIndexTx(tx *sql.Tx, index, term uint64) error {
	const q = `INSERT INTO cluster_meta(key, value) VALUES(?, ?) ` +
		`ON CONFLICT(key) DO UPDATE SET value = excluded.value`
	if _, err := tx.Exec(q, metaKeyAppliedIndex, formatUint(index)); err != nil {
		return fmt.Errorf("cluster: write applied_index: %w", err)
	}
	if _, err := tx.Exec(q, metaKeyAppliedTerm, formatUint(term)); err != nil {
		return fmt.Errorf("cluster: write applied_term: %w", err)
	}
	return nil
}

// reqIDSeenTx reports whether req_id already stands in the ledger (a prior committed
// entry minted it). §4.1 D4 cross-retry dedup.
func reqIDSeenTx(tx *sql.Tx, reqID string) (bool, error) {
	var one int
	err := tx.QueryRow(`SELECT 1 FROM cluster_reqid_ledger WHERE req_id = ?`, reqID).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("cluster: reqid ledger lookup: %w", err)
	}
	return true, nil
}

// ReqIDCommitted reports whether req_id already stands in the REPLICATED dedup ledger, i.e. whether a
// command carrying it has already been committed. Read-only, safe on any node's RO handle.
//
// External RE-REVIEW F1: recovery uses it to DETECT a prior commit instead of blindly re-emitting a
// staged terminal. The FSM would dedup the replay anyway (Apply skips a seen ReqID), but detecting it
// here means the guarantee does not depend on reaching that layer, costs no raft round trip, and is
// visible to a test that stubs the forward. Retention is ~1M raft indices (months), far longer than the
// 2-minute JetStream Duplicates window, so a broker that was down for hours still detects the commit.
func ReqIDCommitted(db *sql.DB, reqID string) (bool, error) {
	if db == nil || reqID == "" {
		return false, nil
	}
	var one int
	err := db.QueryRow(`SELECT 1 FROM cluster_reqid_ledger WHERE req_id = ?`, reqID).Scan(&one)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("cluster: reqid ledger lookup: %w", err)
	}
	return true, nil
}

// insertReqIDTx records a NEW req_id at raft index. The PRIMARY KEY is the
// dedup-uniqueness backstop: a duplicate INSERT here is a programming bug (the
// dedup branch should have caught it) and surfaces as an error → fail-stop.
func insertReqIDTx(tx *sql.Tx, reqID string, index uint64) error {
	if _, err := tx.Exec(
		`INSERT INTO cluster_reqid_ledger(req_id, raft_index) VALUES(?, ?)`,
		reqID, formatUint(index),
	); err != nil {
		return fmt.Errorf("cluster: reqid ledger insert: %w", err)
	}
	return nil
}

// gcReqIDLedgerTx range-deletes ledger rows older than reqIDRetentionWindow indices
// behind the current index, in the SAME apply txn. Deterministic across replicas
// (keyed on raft index, no wall-clock). Underflow (index <= window) deletes nothing.
func gcReqIDLedgerTx(tx *sql.Tx, index uint64) error {
	if index <= reqIDRetentionWindow {
		return nil
	}
	cutoff := index - reqIDRetentionWindow
	if _, err := tx.Exec(
		`DELETE FROM cluster_reqid_ledger WHERE raft_index < ?`, formatUint(cutoff),
	); err != nil {
		return fmt.Errorf("cluster: reqid ledger gc: %w", err)
	}
	return nil
}

// readAppliedIndexDB reads the cursor outside any txn (for AppliedIndex()).
func readAppliedIndexDB(db *sql.DB) (uint64, error) {
	var v string
	err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, metaKeyAppliedIndex).Scan(&v)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("cluster: read applied_index: %w", err)
	}
	return parseUint(v)
}

// Snapshot captures a point-in-time handle for Persist. It must be FAST — the
// heavy online-backup IO happens in Persist (which raft runs concurrently with
// Apply). The DB file IS the state; the snapshot embeds no separate index
// (raft v1.7.3 sets snapshot.Index itself; see snapshot.go).
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{ro: f.ro, tmpDir: f.tmpDir}, nil
}

// Restore replaces live state from a snapshot stream (architecture §3.8 D1
// amendment): temp source file → integrity_check + foreign_key_check (torn
// reject) → forward migrations → in-place NewRestore → liveness baseline reset.
func (f *fsm) Restore(rc io.ReadCloser) error {
	return f.restoreFrom(rc)
}

var _ raft.FSM = (*fsm)(nil)
