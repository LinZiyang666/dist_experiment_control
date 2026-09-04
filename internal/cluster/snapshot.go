package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"

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
}

// Persist writes a consistent online-backup of the live DB into the raft sink.
// Two-stage (modernc NewBackup copies into a FILE, not an io.Writer): backup to a
// temp file, then io.Copy that file into the sink (architecture §3.8 D1
// amendment).
func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.persist(sink); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) persist(sink raft.SnapshotSink) error {
	tmp, err := os.CreateTemp(s.tmpDir, "snap-*.db")
	if err != nil {
		return fmt.Errorf("cluster: snapshot temp: %w", err)
	}
	tmpPath := tmp.Name()
	_ = tmp.Close() // NewBackup opens its own dst conn on this path
	defer func() { _ = os.Remove(tmpPath) }()

	if err := backupTo(context.Background(), s.ro, tmpPath); err != nil {
		return err
	}
	f, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("cluster: open snapshot temp: %w", err)
	}
	defer func() { _ = f.Close() }()
	if _, err := io.Copy(sink, f); err != nil {
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
		stepErr := stepAll(b, backupStepGate)
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
		stepErr := stepAll(b, nil) // no fp3 gate on the restore path
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

// stepAll drives a Backup to completion (true=more pages, false=done). If
// afterFirstStep is non-nil it fires once, right after the first Step has copied
// pages (so the destination temp file exists but the backup is not yet finalized).
func stepAll(b *sqlite.Backup, afterFirstStep func()) error {
	first := true
	for {
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
		if !more {
			return nil
		}
	}
}

// restoreFrom implements FSM.Restore (architecture §3.8 D1 amendment).
func (f *fsm) restoreFrom(rc io.ReadCloser) error {
	defer func() { _ = rc.Close() }()

	// 1. Stream the snapshot into a temp SOURCE file on the SAME filesystem.
	tmp, err := os.CreateTemp(f.tmpDir, "restore-*.db")
	if err != nil {
		return fmt.Errorf("cluster: restore temp: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if _, err := io.Copy(tmp, rc); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("cluster: stream restore: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cluster: close restore temp: %w", err)
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
	_ = mdb.Close()

	// audit snapshot F4: re-verify integrity + FK AFTER the forward-migration, before the
	// in-place restore. The pre-migration check (step 2) cannot catch corruption or an FK
	// violation that a migration introduces; installing such a DB into the LIVE write pool would
	// be worse than refusing the restore.
	if err := verifyIntegrity(tmpPath); err != nil {
		return fmt.Errorf("cluster: post-migration integrity: %w", err)
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

	// 4. In-place restore into the live write pool (no rename over open inode). After this
	// line the live DB is already correct — there is nothing left to fix up, and so
	// nothing left to be interrupted between.
	if err := restoreInPlace(context.Background(), f.db, tmpPath); err != nil {
		return err
	}

	// 5. Liveness baseline reset (§3.5 D1 amendment).
	if err := RebuildLiveness(f.db); err != nil {
		return err
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
