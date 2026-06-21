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
}

// Apply result sentinels (returned via raft ApplyFuture.Response()).
type appliedOK struct{ index uint64 }     // op executed + applied_index advanced
type appliedNoOp struct{ index uint64 }   // idempotent re-apply skip (§3.7 #2); nothing written
type appliedPoison struct{ index uint64 } // poison entry skipped, applied_index advanced past it (§2.8)

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

	if cmd != nil {
		applier := f.appliers[cmd.Op]
		if applier == nil {
			// Unreachable: decodeCommand validated the op against knownOps.
			return nil, fmt.Errorf("cluster: no applier registered for op %q", cmd.Op)
		}
		if err := applier.ApplyTx(tx, cmd); err != nil {
			return nil, fmt.Errorf("cluster: apply %s @%d: %w", cmd.Op, l.Index, err)
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
