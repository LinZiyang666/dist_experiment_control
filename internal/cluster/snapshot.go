package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/LinZiyang666/tether/internal/clusterio"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/hashicorp/raft"
	sqlite "modernc.org/sqlite"
)

// backupStepPages is how many DB pages each online-backup Step copies. Paged
// (not -1 "all at once") so a concurrent Apply can interleave between steps
// (§13.5 WAL concurrency), and so the WAL test can observe interleaving.
const backupStepPages = 64

// modernc's online-backup methods live on the unexported *conn, reached via
// (*sql.Conn).Raw + this interface assertion. *sqlite.Backup is exported, so the
// interface is expressible. A future modernc rename turns the backup_pin_test red
// instead of panicking here.
type backupConn interface {
	NewBackup(dstURI string) (*sqlite.Backup, error)
}
type restoreConn interface {
	NewRestore(srcURI string) (*sqlite.Backup, error)
}

// fsmSnapshot is the FSMSnapshot raft persists. It holds only the read-only
// handle + a scratch dir; all IO is in Persist.
type fsmSnapshot struct {
	ro     *sql.DB
	tmpDir string
	owner  *fsm
}

// Persist writes a consistent online-backup of the live DB into the raft sink.
// Two-stage (modernc NewBackup copies into a FILE, not an io.Writer): backup to a
// temp file, then io.Copy that file into the sink (architecture §3.8 D1
// amendment).
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	// Offline rebuilds own a synchronous reader without a running Node/FSM.
	// Give that operation a local work owner; its completion still precedes the
	// caller's pool Close, while runtime snapshots retain the shared cut census.
	// origin: hot-update S6 plan §3.4; internal review rv-writer-1
	owner := s.owner
	if owner == nil {
		owner = &fsm{}
	}
	ctx, finish, err := beginSnapshotWork(owner, nil)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	defer finish()
	if err := s.persist(ctx, sink); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) persist(ctx context.Context, sink raft.SnapshotSink) error {
	tmp, err := os.CreateTemp(s.tmpDir, "snap-*.db")
	if err != nil {
		return fmt.Errorf("cluster: snapshot temp: %w", err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close() // NewBackup opens its own dst conn on this path
	defer func() { _ = os.Remove(tmpPath) }()

	if err := backupTo(ctx, s.ro, tmpPath); err != nil {
		return err
	}
	f, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("cluster: open snapshot temp: %w", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := copySnapshot(ctx, sink, f, maxSnapshotStageBytes); err != nil {
		return fmt.Errorf("cluster: stream snapshot: %w", err)
	}
	return nil
}

func (s *fsmSnapshot) Release() {}

// backupTo runs modernc's online-backup from ro into a fresh dst file. Step
// returns true=MORE pages / false=DONE; Finish closes the unmanaged dst conn (a
// missing Finish leaks an fd, which the §13.5 fd-baseline gate catches).
// origin: line-2 review M14 (moved here from .golangci.yml, where the exemption was pinned to line
// ranges a comment edit could invalidate). backupTo and restoreInPlace are deliberately symmetric: the
// two halves are read as a PAIR when reasoning about what a snapshot round-trip preserves, and that
// symmetry is the property being maintained. Collapsing them would hide the very correspondence a
// reviewer checks.
//
//nolint:dupl // paired with restoreInPlace; argument above.
func backupTo(ctx context.Context, ro *sql.DB, dstPath string) error {
	conn, err := ro.Conn(ctx)
	if err != nil {
		return fmt.Errorf("cluster: backup conn: %w", err)
	}
	defer func() { _ = conn.Close() }()
	return conn.Raw(func(driverConn any) error {
		bc, ok := driverConn.(backupConn)
		if !ok {
			return fmt.Errorf("cluster: driver conn does not implement NewBackup (modernc API drift)")
		}
		b, err := bc.NewBackup(dstPath)
		if err != nil {
			return fmt.Errorf("cluster: NewBackup: %w", err)
		}
		// backupStepGate (fp3, test-only) fires after the first Step so a SIGKILL
		// lands mid-backup (temp exists, snapshot not finalized).
		stepErr := stepAllContext(ctx, b, backupStepGate)
		finErr := b.Finish() // ALWAYS run (closes dst conn); both errors checked
		if stepErr != nil {
			return stepErr
		}
		if finErr != nil {
			return fmt.Errorf("cluster: backup finish: %w", finErr)
		}
		return nil
	})
}

// BackupDBFile opens srcDBPath READ-ONLY and writes a consistent paged online-backup into
// dstPath. It is the OFFLINE counterpart to Node.BackupDBTo (no running node): the B6
// `cluster backup --offline` path uses it on a daemon-stopped on-disk DB. It never mutates
// the source (read-only handle) and carries no raft state. dstPath must not already exist.
func BackupDBFile(ctx context.Context, srcDBPath, dstPath string) error {
	ro, err := storage.OpenReadOnly("file:" + srcDBPath)
	if err != nil {
		return fmt.Errorf("cluster: open source for backup: %w", err)
	}
	defer func() { _ = ro.Close() }()
	return backupTo(ctx, ro, dstPath)
}

// VerifyIntegrity runs integrity_check + foreign_key_check on the DB at path (the exported
// form of the restore-path guard). B6 restore uses it on a staging copy before and after the
// forward-migration, so a torn/corrupt/FK-violating bundle is refused before it can be
// installed over the live DB.
func VerifyIntegrity(path string) error { return verifyIntegrity(path) }

// restoreInPlace copies srcPath INTO the live write-pool conn (modernc
// NewRestore) — no rename over the open inode (§3.8 D1 amendment).
// origin: line-2 review M14. backupTo above carries the argument for this pair.
//
//nolint:dupl // paired with backupTo; the save/restore symmetry IS the property.
func restoreInPlace(ctx context.Context, db *sql.DB, srcPath string) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("cluster: restore conn: %w", err)
	}
	defer func() { _ = conn.Close() }()
	return conn.Raw(func(driverConn any) error {
		rc, ok := driverConn.(restoreConn)
		if !ok {
			return fmt.Errorf("cluster: driver conn does not implement NewRestore (modernc API drift)")
		}
		b, err := rc.NewRestore(srcPath)
		if err != nil {
			return fmt.Errorf("cluster: NewRestore: %w", err)
		}
		stepErr := stepAllContext(ctx, b, nil) // no fp3 gate on the restore path
		finErr := b.Finish()
		if stepErr != nil {
			return stepErr
		}
		if finErr != nil {
			return fmt.Errorf("cluster: restore finish: %w", finErr)
		}
		return nil
	})
}

// stepAllContext drives a Backup to completion (true=more pages, false=done). If
// afterFirstStep is non-nil it fires once, right after the first Step has copied
// pages (so the destination temp file exists but the backup is not yet finalized).
func stepAllContext(ctx context.Context, b *sqlite.Backup, afterFirstStep func()) error {
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		more, err := b.Step(backupStepPages)
		if err != nil {
			return fmt.Errorf("cluster: backup step: %w", err)
		}
		if first {
			first = false
			if afterFirstStep != nil {
				afterFirstStep()
			}
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !more {
			return nil
		}
	}
}

// restoreFrom implements FSM.Restore (architecture §3.8 D1 amendment).
func (f *fsm) restoreFrom(rc io.ReadCloser) error {
	ctx, finish, err := beginSnapshotWork(f, rc)
	if err != nil {
		_ = rc.Close()
		return err
	}
	defer finish()
	defer func() { _ = rc.Close() }()

	// 1. Stream the snapshot into a temp SOURCE file on the SAME filesystem.
	tmp, err := os.CreateTemp(f.tmpDir, "restore-*.db")
	if err != nil {
		return fmt.Errorf("cluster: restore temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	hash := sha256.New()
	if _, err := copySnapshot(ctx, io.MultiWriter(tmp, hash), rc, maxSnapshotStageBytes); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("cluster: stream restore: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cluster: close restore temp: %w", err)
	}

	var digest [sha256.Size]byte
	copy(digest[:], hash.Sum(nil))
	ticket, bound, err := consumeSnapshotTicket(f, digest)
	if err != nil {
		return err
	}

	// 2. integrity_check + foreign_key_check (torn snapshot rejected — §13.3).
	if err := verifyIntegrity(tmpPath); err != nil {
		return err
	}

	// 3. Forward-migrate the temp file through the SAME storage runner.
	mdb, err := storage.Open("file:" + tmpPath)
	if err != nil {
		return fmt.Errorf("cluster: forward-migrate restore temp: %w", err)
	}
	if err := mdb.Close(); err != nil {
		return fmt.Errorf("cluster: close migrated snapshot: %w", err)
	}

	// audit snapshot F4: re-verify integrity + FK AFTER the forward-migration, before the
	// in-place restore. The pre-migration check (step 2) cannot catch corruption or an FK
	// violation that a migration introduces; installing such a DB into the LIVE write pool would
	// be worse than refusing the restore.
	if err := verifyIntegrity(tmpPath); err != nil {
		return fmt.Errorf("cluster: post-migration integrity: %w", err)
	}

	var stamp snapshotStamp
	if bound {
		stamp, err = readSnapshotStamp(tmpPath)
		if err != nil {
			return err
		}
	}

	// origin: hot-update S6 plan §3.4 / T15
	// This is deliberately AFTER the complete stage, not a sealed/startup floor.
	// An Apply committed during streaming must survive a stale snapshot attempt.
	f.mutationMu.Lock()
	defer f.mutationMu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateSnapshotInstall(f, stamp, ticket, bound); err != nil {
		return err
	}

	// PRESERVE node-local identity across the install (v0.4.4 grow fix). A raft InstallSnapshot copies the
	// LEADER's DB byte-for-byte, and cluster_meta.self_node_id in it is the LEADER's id. A joiner that
	// installs the leader's snapshot to catch up MUST keep ITS OWN id — otherwise the next restart's
	// readSelfNodeID returns the leader's id and the joiner comes up with the leader's identity (two nodes
	// claiming the same raft ServerID → split brain). Read it BEFORE the in-place overwrite, re-write AFTER.
	// For a same-node force-single restore the id is identical, so the re-write is a harmless no-op. This
	// path had NO test coverage because no prior test exercised a real InstallSnapshot (the d9 2-broker
	// join aligned via log replay at a low index instead of installing the leader's snapshot).
	// THE STAGING FILE IS EDITED BEFORE IT IS INSTALLED, so that the install itself is
	// the only step that touches the live database and there is no window between
	// "overwritten" and "fixed up".
	//
	// origin: prerelease audit cluster-fsm/L3-F1. This used to read the id out of the
	// LIVE database (with the error discarded), install the snapshot over it, and then
	// write the id back in a separate transaction, followed by a second one stripping
	// restore_in_progress. Anything that interrupted that sequence — a crash, one failed
	// Exec — left the LEADER's id in the joiner's cluster_meta. The damage was
	// self-confirming rather than transient: the leader's next InstallSnapshot re-read
	// the id from that same live DB, found the leader's, and "preserved" it. The joiner's
	// own identity was gone for good, and the next restart brought it up as a second
	// server claiming the leader's raft ServerID.
	//
	// Prefer f.localID, which is this node's id as raft itself knows it and is not a copy
	// of anything the restore can damage. The read from the live DB stays as a fallback
	// for construction sites that have no id to give, and its error is now checked: an
	// unreadable identity must not be silently downgraded to "don't preserve anything".
	selfID := f.localID
	if selfID == "" {
		switch err := f.db.QueryRow(`SELECT value FROM cluster_meta WHERE key='self_node_id'`).Scan(&selfID); {
		case err == nil, errors.Is(err, sql.ErrNoRows):
		default:
			return fmt.Errorf("cluster: read self_node_id before restore: %w", err)
		}
	}

	// Apply both node-local corrections to the STAGING file.
	//
	// restore_in_progress is a SOURCE-NODE-LOCAL recovery flag (R16 B1): the daemon FATALs
	// on it until `recovery restore` completes, so it must never ride a snapshot to another
	// node. A grow-ready snapshot is taken — correctly — BEFORE the source clears the
	// marker, so the snapshot DB carries it, and a fresh joiner that installed it would
	// FATAL on its next restart over a restore it never ran.
	if err := prepareRestoreStaging(tmpPath, selfID); err != nil {
		return err
	}

	// Cancellation before the mutation point refuses the install. Once admitted,
	// finish it and join it: abandoning a paged NewRestore can damage live state.
	// 4. In-place restore into the live write pool (no rename over open inode). After this
	// line the live DB is already correct — there is nothing left to fix up, and so
	// nothing left to be interrupted between.
	if f.snapshotInstallObserve != nil {
		f.snapshotInstallObserve()
	}
	if err := restoreInPlace(context.WithoutCancel(ctx), f.db, tmpPath); err != nil {
		return err
	}

	// 5. Liveness baseline reset (§3.5 D1 amendment).
	if err := RebuildLiveness(f.db); err != nil {
		return err
	}
	if bound {
		f.snapshotIndex, f.snapshotTerm = ticket.Index, ticket.Term
		f.snapshotConfigIndex, f.snapshotConfigDigest = ticket.ConfigIndex, ticket.ConfigDigest
	}
	return nil
}

// prepareRestoreStaging applies this node's local corrections to the staging file
// BEFORE it is installed: stamp self_node_id (when known) and drop the source's
// restore_in_progress marker.
//
// Doing it here rather than after the install is the whole of the L3-F1 fix. Both
// statements run in ONE transaction on a file nothing else has open, so either the
// staging file is fully corrected or the restore fails and the live DB is untouched.
// There is no state in which the live database holds another node's identity.
func prepareRestoreStaging(path, selfID string) error {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(1)")
	if err != nil {
		return fmt.Errorf("cluster: open restore staging: %w", err)
	}
	defer func() { _ = db.Close() }()
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("cluster: begin restore staging: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if selfID != "" {
		if _, err := tx.Exec(`INSERT INTO cluster_meta(key,value) VALUES('self_node_id',?) `+
			`ON CONFLICT(key) DO UPDATE SET value=excluded.value`, selfID); err != nil {
			return fmt.Errorf("cluster: stage self_node_id: %w", err)
		}
	}
	if _, err := tx.Exec(`DELETE FROM cluster_meta WHERE key='restore_in_progress'`); err != nil {
		return fmt.Errorf("cluster: stage strip restore_in_progress: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("cluster: commit restore staging: %w", err)
	}
	return nil
}

// verifyIntegrity opens a throwaway handle on path and runs integrity_check +
// foreign_key_check. A torn/corrupt DB makes the pragma error or return non-"ok"
// (or surfaces FK violations); any of these REJECTS the restore (§13.3).
func verifyIntegrity(path string) error {
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=foreign_keys(1)")
	if err != nil {
		return fmt.Errorf("cluster: open snapshot for integrity_check: %w", err)
	}
	defer func() { _ = db.Close() }()

	var res string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&res); err != nil {
		return fmt.Errorf("cluster: integrity_check could not run (torn snapshot): %w", err)
	}
	if res != "ok" {
		return fmt.Errorf("cluster: snapshot integrity_check = %q, refusing restore (torn/corrupt)", res)
	}
	rows, err := db.Query(`PRAGMA foreign_key_check`)
	if err != nil {
		return fmt.Errorf("cluster: foreign_key_check could not run: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		return fmt.Errorf("cluster: snapshot has foreign-key violations, refusing restore")
	}
	return rows.Err()
}

// origin: hot-update S6 plan §3.4 / T15
// A database snapshot is disk staging, not a handoff manifest. Bound it separately
// (1 GiB) and copy incrementally; an advertised size never authorizes unbounded IO.
const maxSnapshotStageBytes int64 = 1 << 30

type snapshotStamp struct {
	index, term uint64
	schema      [sha256.Size]byte
}

type snapshotBinding struct {
	ticket   clusterio.SnapshotTicket
	consumed bool
}

// snapshotWork owns cancellation and readers until their real exercise finishes.
// The closed idle channel is a receipt, not a prediction based on cancellation.
// Tickets are ephemeral: no secret or database contents enter image metadata.
type snapshotWork struct {
	mu             sync.Mutex
	ctx            context.Context
	cancel         context.CancelFunc
	stopped        bool
	next           uint64
	sources        map[uint64]io.Closer
	idle           chan struct{}
	requireTickets bool
	bindings       map[[sha256.Size]byte]snapshotBinding
}

func beginSnapshotWork(f *fsm, source io.Closer) (context.Context, func(), error) {
	w := &f.snapshots
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.stopped {
		return nil, nil, fmt.Errorf("cluster: snapshot work fenced")
	}
	if w.ctx == nil {
		// ctx-root: FSM snapshot/backup work has a lifetime independent of callers.
		w.ctx, w.cancel = context.WithCancel(context.Background())
	}
	if len(w.sources) == 0 {
		w.idle = make(chan struct{})
		w.sources = make(map[uint64]io.Closer)
	}
	w.next++
	id := w.next
	w.sources[id] = source
	return w.ctx, func() {
		w.mu.Lock()
		defer w.mu.Unlock()
		delete(w.sources, id)
		if len(w.sources) == 0 {
			close(w.idle)
		}
	}, nil
}

// FenceSnapshotWork prevents new backup/persist/restore work and interrupts real
// readers. P04 joins the returned receipt before closing either SQLite pool.
// Closing a network reader must cancel its socket; a passive NopCloser cannot
// stand in for P04's tracked accepted RPC owner.
func FenceSnapshotWork(n *Node) <-chan struct{} {
	w := &n.fsm.snapshots
	w.mu.Lock()
	w.stopped = true
	if w.cancel != nil {
		w.cancel()
	}
	if w.idle == nil {
		w.idle = make(chan struct{})
		close(w.idle)
	}
	idle := w.idle
	sources := make([]io.Closer, 0, len(w.sources))
	for _, source := range w.sources {
		if source != nil {
			sources = append(sources, source)
		}
	}
	w.mu.Unlock()
	for _, source := range sources {
		_ = source.Close()
	}
	return idle
}

// ResumeSnapshotWork permits a refused cut to resume only after every cancelled
// owner has joined. A timeout cannot create a second work generation.
func ResumeSnapshotWork(n *Node) error {
	w := &n.fsm.snapshots
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(w.sources) != 0 || len(w.bindings) != 0 {
		return fmt.Errorf("cluster: snapshot owners still active")
	}
	if w.cancel != nil {
		w.cancel()
	}
	w.ctx, w.cancel = nil, nil
	w.stopped = false
	return nil
}

// RequireSnapshotTickets is installed by the runtime transport owner before
// exposing Consumer. Direct/offline FSM restore remains available to recovery;
// a network-bound FSM must never accept an unbound reader after Raft reopens it.
func RequireSnapshotTickets(n *Node) {
	f := n.fsm
	f.mutationMu.Lock()
	f.snapshotRaftIndex = func() uint64 {
		if r := n.raft.Load(); r != nil {
			return r.AppliedIndex()
		}
		return 0
	}
	if n.store != nil && n.snaps != nil {
		f.snapshotConfiguration = func() (uint64, string, error) {
			return committedSnapshotConfiguration(n)
		}
	}
	f.mutationMu.Unlock()
	w := &f.snapshots
	w.mu.Lock()
	w.requireTickets = true
	w.mu.Unlock()
}

// BackupSnapshotDB gives Node.BackupDBTo the same reader census/cancellation as
// snapshot Persist. The caller context may cancel a backup, never committed Apply.
func BackupSnapshotDB(ctx context.Context, n *Node, dstPath string) error {
	workCtx, finish, err := beginSnapshotWork(n.fsm, nil)
	if err != nil {
		return err
	}
	defer finish()
	merged, cancel := context.WithCancel(workCtx)
	defer cancel()
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	if err := ctx.Err(); err != nil {
		return err
	}
	return backupTo(merged, n.ro, dstPath)
}

func consumeSnapshotTicket(f *fsm, digest [sha256.Size]byte) (clusterio.SnapshotTicket, bool, error) {
	w := &f.snapshots
	w.mu.Lock()
	defer w.mu.Unlock()
	binding, ok := w.bindings[digest]
	if !ok {
		if w.requireTickets {
			return clusterio.SnapshotTicket{}, false, fmt.Errorf("cluster: unbound snapshot payload")
		}
		return clusterio.SnapshotTicket{}, false, nil
	}
	if binding.consumed {
		return clusterio.SnapshotTicket{}, false, fmt.Errorf("cluster: snapshot ticket already consumed")
	}
	binding.consumed = true
	w.bindings[digest] = binding
	return binding.ticket, true, nil
}

func validateSnapshotTicket(t clusterio.SnapshotTicket) error {
	digest, err := hex.DecodeString(t.ConfigDigest)
	if t.RPCID == 0 || t.Index == 0 || t.Term == 0 || t.ConfigIndex == 0 || t.ConfigIndex > t.Index ||
		t.PayloadBytes <= 0 || t.PayloadBytes > maxSnapshotStageBytes || err != nil || len(digest) != sha256.Size ||
		hex.EncodeToString(digest) != t.ConfigDigest {
		return fmt.Errorf("cluster: invalid snapshot ticket")
	}
	return nil
}

func validateSnapshotInstall(f *fsm, stamp snapshotStamp, ticket clusterio.SnapshotTicket, bound bool) error {
	// RecoverCluster deliberately restores a local snapshot and replays its
	// retained log tail. Only runtime InstallSnapshot is constrained by the live
	// floor; imposing it on recovery disables force-single and grow-ready.
	// origin: hot-update S6 plan §3.4 / T15; internal review rv-writer-1
	if !bound {
		return nil
	}
	current, err := snapshotStampDB(f.db)
	if err != nil {
		return err
	}
	// origin: hot-update S6 plan §3.4 / T15
	// SQL command cursor is not Raft's all-entry snapshot index. Barriers/config
	// can make ticket.Index strictly larger; requiring equality breaks catch-up.
	if stamp.index < current.index || stamp.term < current.term ||
		(stamp.index == current.index && stamp.term != current.term) {
		return fmt.Errorf("cluster: snapshot below current command floor")
	}
	if stamp.schema != current.schema {
		return fmt.Errorf("cluster: snapshot schema mismatch")
	}
	if bound {
		if err := validateSnapshotTicket(ticket); err != nil {
			return err
		}
		if f.snapshotRaftIndex != nil && ticket.Index < f.snapshotRaftIndex() {
			return fmt.Errorf("cluster: snapshot below current Raft apply floor")
		}
		if f.snapshotConfiguration != nil {
			index, digest, err := f.snapshotConfiguration()
			if err != nil {
				return err
			}
			if ticket.ConfigIndex < index || (ticket.ConfigIndex == index && ticket.ConfigDigest != digest) {
				return fmt.Errorf("cluster: snapshot below committed configuration floor")
			}
		}
		if ticket.Index < f.snapshotIndex || ticket.Term < f.snapshotTerm || ticket.ConfigIndex < f.snapshotConfigIndex ||
			(ticket.ConfigIndex == f.snapshotConfigIndex && f.snapshotConfigDigest != "" && ticket.ConfigDigest != f.snapshotConfigDigest) {
			return fmt.Errorf("cluster: snapshot below current Raft metadata floor")
		}
		if stamp.index > ticket.Index || stamp.term > ticket.Term ||
			(stamp.index == ticket.Index && stamp.term != ticket.Term) {
			return fmt.Errorf("cluster: snapshot payload metadata mismatch")
		}
	}
	return nil
}

// Read configuration-only commits without making the FSM a ConfigurationStore.
// Raft may retain an uncommitted configuration in its log: only entries at or
// below CommitIndex participate, and compacted entries come from local snapshots.
// origin: hot-update S6 plan §3.4 / T15; internal review rv-writer-2/11
func committedSnapshotConfiguration(n *Node) (uint64, string, error) {
	r := n.raft.Load()
	if r == nil {
		return 0, "", nil
	}
	committed := r.CommitIndex()
	first, err := n.store.FirstIndex()
	if err != nil {
		return 0, "", err
	}
	last, err := n.store.LastIndex()
	if err != nil {
		return 0, "", err
	}
	var index uint64
	var configuration raft.Configuration
	for i := min(last, committed); i >= first && i > 0; i-- {
		var entry raft.Log
		if err := n.store.GetLog(i, &entry); err != nil {
			return 0, "", err
		}
		if entry.Type == raft.LogConfiguration {
			index, configuration = entry.Index, raft.DecodeConfiguration(entry.Data)
			break
		}
	}
	snapshots, err := n.snaps.List()
	if err != nil {
		return 0, "", err
	}
	for _, snapshot := range snapshots {
		if snapshot.Index <= committed && snapshot.ConfigurationIndex > index {
			index, configuration = snapshot.ConfigurationIndex, snapshot.Configuration
		}
	}
	if index == 0 {
		return 0, "", nil
	}
	digest := sha256.Sum256(raft.EncodeConfiguration(configuration))
	return index, hex.EncodeToString(digest[:]), nil
}

func readSnapshotStamp(path string) (snapshotStamp, error) {
	db, err := storage.OpenReadOnly("file:" + path)
	if err != nil {
		return snapshotStamp{}, err
	}
	stamp, readErr := snapshotStampDB(db)
	return stamp, errors.Join(readErr, db.Close())
}

func snapshotStampDB(db *sql.DB) (snapshotStamp, error) {
	var stamp snapshotStamp
	var index, term string
	present := 0
	for _, entry := range []struct {
		key string
		dst *string
	}{
		{metaKeyAppliedIndex, &index}, {metaKeyAppliedTerm, &term},
	} {
		err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, entry.key).Scan(entry.dst)
		if err == nil {
			present++
		} else if !errors.Is(err, sql.ErrNoRows) {
			return stamp, fmt.Errorf("cluster: read snapshot cursor: %w", err)
		}
	}
	if present != 0 && (present != 2 || index == "" || term == "") {
		return stamp, fmt.Errorf("cluster: incomplete snapshot cursor")
	}
	var err error
	stamp.index, stamp.term, err = parseSnapshotCursor(index, term)
	if err != nil {
		return stamp, err
	}
	// Exact migration names AND schema objects: an unchanged ledger with a
	// dropped table must be refused before the live database is touched.
	hash := sha256.New()
	for _, query := range []string{
		`SELECT version FROM schema_migrations ORDER BY version`,
		`SELECT type || char(0) || name || char(0) || tbl_name || char(0) || coalesce(sql,'') FROM sqlite_master ORDER BY type,name`,
	} {
		rows, err := db.Query(query)
		if err != nil {
			return stamp, fmt.Errorf("cluster: snapshot schema: %w", err)
		}
		for rows.Next() {
			var value string
			if err := rows.Scan(&value); err != nil {
				_ = rows.Close()
				return stamp, err
			}
			_, _ = fmt.Fprintf(hash, "%d:%s", len(value), value)
		}
		err = errors.Join(rows.Err(), rows.Close())
		if err != nil {
			return stamp, err
		}
	}
	copy(stamp.schema[:], hash.Sum(nil))
	return stamp, nil
}

// origin: hot-update S6 plan §3.4 / FuzzBrokerSnapshotStamp
// Missing both cursor keys means a genuinely empty FSM. A partial pair, overflow,
// noncanonical decimal or impossible term cannot be downgraded to a zero floor.
func parseSnapshotCursor(index, term string) (uint64, uint64, error) {
	if index == "" && term == "" {
		return 0, 0, nil
	}
	i, ierr := parseUint(index)
	t, terr := parseUint(term)
	if ierr != nil || terr != nil || formatUint(i) != index || formatUint(t) != term ||
		(i == 0) != (t == 0) {
		return 0, 0, fmt.Errorf("cluster: invalid snapshot cursor")
	}
	return i, t, nil
}

type snapshotContextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r snapshotContextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

func copySnapshot(ctx context.Context, dst io.Writer, src io.Reader, limit int64) (int64, error) {
	if limit <= 0 || limit > maxSnapshotStageBytes {
		return 0, fmt.Errorf("cluster: invalid snapshot staging limit")
	}
	written, err := io.Copy(dst, io.LimitReader(snapshotContextReader{ctx, src}, limit+1))
	if err != nil {
		return written, fmt.Errorf("cluster: stream snapshot stage: %w", err)
	}
	if written > limit {
		return written, fmt.Errorf("cluster: snapshot staging limit exceeded")
	}
	return written, ctx.Err()
}

// ValidateSnapshotRPC compares a previously bound ticket with the actual RPC
// immediately before forwarding it to Raft. Configuration is part of the binding,
// not a free-standing digest supplied by a caller. Call it before StageSnapshotRPC
// and again before forwarding if the request can be mutated during staging.
// origin: hot-update S6 plan §3.4 / T15
func ValidateSnapshotRPC(ticket clusterio.SnapshotTicket, request *raft.InstallSnapshotRequest) (err error) {
	if err := validateSnapshotTicket(ticket); err != nil {
		return err
	}
	if request == nil || len(request.Configuration) > 16<<20 || request.SnapshotVersion != raft.SnapshotVersionMax ||
		request.LastLogIndex != ticket.Index || request.LastLogTerm != ticket.Term ||
		request.ConfigurationIndex != ticket.ConfigIndex || request.Size != ticket.PayloadBytes ||
		request.Term < request.LastLogTerm {
		return fmt.Errorf("cluster: snapshot RPC ticket mismatch")
	}
	digest := sha256.Sum256(request.Configuration)
	if hex.EncodeToString(digest[:]) != ticket.ConfigDigest {
		return fmt.Errorf("cluster: snapshot RPC configuration mismatch")
	}
	// raft.DecodeConfiguration panics on malformed MsgPack. Catch that at the
	// transport seam, before Raft can consume it or alter election metadata.
	defer func() {
		if recover() != nil {
			err = fmt.Errorf("cluster: invalid snapshot RPC configuration")
		}
	}()
	configuration := raft.DecodeConfiguration(request.Configuration)
	if !bytes.Equal(raft.EncodeConfiguration(configuration), request.Configuration) {
		return fmt.Errorf("cluster: noncanonical snapshot RPC configuration")
	}
	ids, addresses := make(map[raft.ServerID]bool), make(map[raft.ServerAddress]bool)
	voters := 0
	for _, server := range configuration.Servers {
		if server.ID == "" || server.Address == "" || ids[server.ID] || addresses[server.Address] {
			return fmt.Errorf("cluster: invalid snapshot RPC configuration")
		}
		ids[server.ID], addresses[server.Address] = true, true
		switch server.Suffrage {
		case raft.Voter:
			voters++
		case raft.Nonvoter, raft.Staging:
		default:
			return fmt.Errorf("cluster: invalid snapshot RPC suffrage")
		}
	}
	if voters == 0 {
		return fmt.Errorf("cluster: snapshot RPC configuration has no voters")
	}
	return nil
}
