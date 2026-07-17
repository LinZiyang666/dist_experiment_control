package cluster

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// datadirlock.go — the CONTINUOUS daemon↔offline-tool interlock (external review round-5 B3).
//
// Before round-5 the only thing standing between a live daemon and the offline disk surgery was
// RaftStoreLockedByDaemon: a ONE-SHOT open/close probe of raft.db's bolt lock, taken at the start of the
// operation. Everything after it — RecoverCluster (which closes the live bolt store again), the SQLite
// mutations, building the staged snapshot, and finally the directory exchange — ran with NO lock held. A
// daemon revived inside that window (systemd RestartSec, an operator un-mask, a stray `systemctl start`)
// re-opens the OLD raft/raft.db and can ACKNOWLEDGE writes into it; rename/exchange does not invalidate an
// already-open inode, so those acked writes then vanish when the rebuild's cleanup deletes the old tree.
//
// ${DataDir}/tether.lock has existed all along, but ONLY the offline tools took it, so it excluded other
// offline tools and nothing else. The fix is the one the architecture always implied: the DAEMON HOLDS THE
// SAME LOCK FOR ITS ENTIRE LIFETIME. Then the offline tools' existing LOCK_EX|LOCK_NB acquisition is a real
// interlock in both directions — an offline op cannot start under a live daemon, and a daemon cannot start
// (it fail-closes) while an offline op is mid-surgery.
//
// unix.Flock is available on linux and darwin (the two goos targets in build/goreleaser.yaml). The lock is
// advisory and per-open-file-description: it is released automatically if the process dies, which is what we
// want — a crashed daemon must not wedge recovery.

// DataDirLockFile is the single SSOT name of the data-dir interlock file, shared by the daemon and every
// offline tool. Both MUST use this constant.
const DataDirLockFile = "tether.lock"

// DataDirLockPath returns the interlock path for a data dir.
func DataDirLockPath(dataDir string) string { return filepath.Join(dataDir, DataDirLockFile) }

// AcquireDataDirLock takes the exclusive, non-blocking flock on ${dataDir}/tether.lock and returns a
// release func. The caller MUST hold it for as long as it may touch the store:
//   - the daemon: for its whole process lifetime (round-5 B3);
//   - an offline tool: until swap + fsync + cleanup are all finished.
//
// A failure means someone else holds it — a running daemon, or another offline op.
// ErrDataDirLockHeld means ANOTHER LIVE PROCESS holds the lock (a running broker, or an offline recovery).
// It is the only condition that should ever be reported as contention.
var ErrDataDirLockHeld = errors.New("another process holds the data-dir lock (a running tether-broker, or an offline recovery in progress)")

// ErrDataDirLockUnusable means the lock file itself cannot be opened — almost always because a ROOT-run
// offline tool created it and the `User=tether` daemon now gets EACCES. Round-6: this MUST NOT be reported
// as contention. Conflating them printed "stop the previous broker" at an operator whose broker was already
// stopped and whose real problem was a root-owned dotfile — while the last surviving broker crash-looped.
var ErrDataDirLockUnusable = errors.New("the data-dir lock file exists but cannot be opened by this user")

// AcquireDataDirLock takes the exclusive, non-blocking flock. See the file header for the contract.
func AcquireDataDirLock(dataDir string) (release func(), err error) {
	path := DataDirLockPath(dataDir)
	// Round-7 SECURITY (local privilege escalation): O_NOFOLLOW is load-bearing, not hygiene. The data dir
	// is writable by the unprivileged `tether` service account while the runbook invokes recovery through
	// `sudo`. Without O_NOFOLLOW, a tether-planted symlink at tether.lock makes root open the TARGET, and the
	// ownership mirror below then f.Chown's that target to the data-dir owner — i.e. tether can take
	// ownership of ANY file on the box (/etc/shadow, a unit file, another service's DB). O_NOFOLLOW makes the
	// open fail with ELOOP instead; O_CREATE still creates the lock when the path does not exist.
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, fmt.Errorf("cluster: %s is a SYMLINK — refusing to follow it (a symlinked lock would let a sudo-run recovery chown its target); remove it and investigate why it is there", path)
		}
		if errors.Is(err, os.ErrPermission) {
			return nil, fmt.Errorf("%w (%s): %v\n"+
				"  A root-run recovery most likely created it. Fix the OWNER, do not stop anything:\n"+
				"    sudo chown --reference=%s %s\n"+
				"  and prefer `sudo -u tether tether cluster recovery …` so it never happens again",
				ErrDataDirLockUnusable, path, err, dataDir, path)
		}
		return nil, fmt.Errorf("cluster: open data-dir lock %s: %w", path, err)
	}
	// Round-7: O_NOFOLLOW rules out a symlink, but NOT a FIFO/device/directory planted at the same path — and
	// we are about to chown this fd as root. Prove it is a REGULAR file before touching its ownership.
	if fi, serr := f.Stat(); serr != nil || !fi.Mode().IsRegular() {
		_ = f.Close()
		if serr != nil {
			return nil, fmt.Errorf("cluster: stat data-dir lock %s: %w", path, serr)
		}
		return nil, fmt.Errorf("cluster: %s is not a regular file (%s) — refusing to lock or chown it; remove it and investigate why it is there", path, fi.Mode())
	}
	// Round-6: a root-run offline tool must NOT leave a root-owned lock behind — the daemon that has to take
	// it next runs as tether and would EACCES-refuse to start FOREVER (a worse outage than the interlock
	// prevents). Mirror the data dir's ownership onto the lock file. Safe only because the fd above is
	// proven to be a regular, non-symlinked file we opened ourselves (round-7).
	chownLockToDataDirOwner(dataDir, f)
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%w (%s): %v", ErrDataDirLockHeld, path, err)
	}
	return func() {
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		_ = f.Close()
	}, nil
}

// chownLockToDataDirOwner best-effort gives the lock file the data dir's uid/gid. A failure is ignored: the
// common case (already the right owner, or we are not privileged enough to chown) is harmless.
func chownLockToDataDirOwner(dataDir string, f *os.File) {
	fi, err := os.Stat(dataDir)
	if err != nil {
		return
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return
	}
	cur, err := f.Stat()
	if err != nil {
		return
	}
	if cst, ok := cur.Sys().(*syscall.Stat_t); ok && cst.Uid == st.Uid && cst.Gid == st.Gid {
		return // already correct
	}
	_ = f.Chown(int(st.Uid), int(st.Gid))
}
