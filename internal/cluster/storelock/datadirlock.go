package storelock

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/LinZiyang666/tether/internal/handoff"
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
	l, err := acquireDataDirLock(dataDir)
	if err != nil {
		return nil, err
	}
	return func() { _ = l.Release() }, nil
}

func acquireDataDirLock(dataDir string) (*DataDirLock, error) {
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
	l, err := claimDataDirLock(dataDir, f)
	if err != nil {
		_ = f.Close()
	}
	return l, err
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

// DataDirLock owns the original flock OFD. Detach retires the old wrapper without
// unlocking; Release is reserved for the current owner's terminal stop.
// origin: hot-update S6 plan §3.1 / T12
type DataDirLock struct {
	mu       sync.Mutex
	file     *os.File
	path     string
	identity string
}

var dataDirLockClaims = struct {
	sync.Mutex
	owners map[string]*DataDirLock
}{owners: make(map[string]*DataDirLock)}

func claimDataDirLock(dataDir string, f *os.File) (*DataDirLock, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, errors.New("cluster: lock identity unavailable")
	}
	identity := fmt.Sprintf("%d:%d", st.Dev, st.Ino)
	dataDirLockClaims.Lock()
	defer dataDirLockClaims.Unlock()
	if dataDirLockClaims.owners[identity] != nil {
		return nil, errors.New("cluster: lock OFD already owned")
	}
	l := &DataDirLock{file: f, path: DataDirLockPath(dataDir), identity: identity}
	dataDirLockClaims.owners[identity] = l
	return l, nil
}

func AcquireImageDataDirLock(dataDir string) (*DataDirLock, error) {
	if runtime.GOOS != "linux" {
		return nil, handoff.ErrUnsupported
	}
	if err := validateImageDataDir(dataDir); err != nil {
		return nil, err
	}
	return acquireDataDirLock(dataDir)
}

func validateImageDataDir(dataDir string) error {
	if !filepath.IsAbs(dataDir) || filepath.Clean(dataDir) != dataDir {
		return errors.New("cluster: image lock requires canonical absolute data directory")
	}
	resolved, err := filepath.EvalSymlinks(dataDir)
	if err != nil {
		return err
	}
	if resolved != dataDir {
		return errors.New("cluster: image data directory contains a symlink")
	}
	return nil
}

// A hot opener consumes the already-held lock; it cannot create a new lock
// domain after the old writer has closed. Checking the owner registry also
// rules out a caller which merely supplies the expected lock pathname.
// origin: hot-update S6 plan §3.1 / T12
func RequireImageDataDirLock(dataDir string) error {
	if err := validateImageDataDir(dataDir); err != nil {
		return err
	}
	dataDirLockClaims.Lock()
	var owner *DataDirLock
	for _, candidate := range dataDirLockClaims.owners {
		if candidate.path == DataDirLockPath(dataDir) {
			owner = candidate
			break
		}
	}
	dataDirLockClaims.Unlock()
	if owner == nil {
		return errors.New("cluster: image current store has no original lock owner")
	}
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.file == nil {
		return errors.New("cluster: image lock owner detached or released")
	}
	actual, err := owner.file.Stat()
	if err != nil {
		return err
	}
	named, err := os.Lstat(owner.path)
	if err != nil {
		return err
	}
	if !named.Mode().IsRegular() || !os.SameFile(actual, named) {
		return errors.New("cluster: image lock pathname was replaced")
	}
	return nil
}

// AdoptDataDirLock validates the inherited descriptor without open, re-flock or
// LOCK_UN. A fresh file of the same pathname cannot stand in for the held OFD.
// Refusal leaves f with the caller for salvage.
// origin: hot-update S6 plan §3.1 / T12
func AdoptDataDirLock(dataDir string, f *os.File) (*DataDirLock, error) {
	if runtime.GOOS != "linux" {
		return nil, handoff.ErrUnsupported
	}
	if f == nil {
		return nil, errors.New("cluster: missing inherited lock")
	}
	if err := validateImageDataDir(dataDir); err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	named, err := os.Lstat(DataDirLockPath(dataDir))
	if err != nil {
		return nil, err
	}
	parent, err := os.Stat(dataDir)
	if err != nil {
		return nil, err
	}
	fst, ok := fi.Sys().(*syscall.Stat_t)
	pst, parentOK := parent.Sys().(*syscall.Stat_t)
	if !ok || !parentOK || !fi.Mode().IsRegular() || !named.Mode().IsRegular() || !os.SameFile(fi, named) || fst.Nlink != 1 || fst.Uid != pst.Uid || fst.Gid != pst.Gid {
		return nil, errors.New("cluster: inherited lock identity or owner mismatch")
	}
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		return nil, errors.New("cluster: inherited lock must be close-on-exec")
	}
	status, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
	if err != nil || status&unix.O_ACCMODE != unix.O_RDWR {
		return nil, errors.New("cluster: inherited lock must be read-write")
	}
	info, err := os.ReadFile("/proc/self/fdinfo/" + strconv.Itoa(int(f.Fd())))
	if err != nil {
		return nil, err
	}
	held := false
	for _, line := range strings.Split(string(info), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 9 && fields[0] == "lock:" && fields[2] == "FLOCK" && fields[3] == "ADVISORY" && fields[4] == "WRITE" && fields[5] == strconv.Itoa(os.Getpid()) && fields[7] == "0" && fields[8] == "EOF" {
			held = true
		}
	}
	if !held {
		return nil, errors.New("cluster: inherited descriptor does not hold the original flock")
	}
	return claimDataDirLock(dataDir, f)
}

func (l *DataDirLock) Export() (handoff.Resource, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return handoff.Resource{}, errors.New("cluster: lock owner detached or released")
	}
	data, err := json.Marshal(struct {
		Path     string `json:"path"`
		Identity string `json:"identity"`
	}{l.path, l.identity})
	if err != nil {
		return handoff.Resource{}, err
	}
	return handoff.Resource{ID: "broker-store-lock", Kind: "broker-lock", Data: data, Files: map[string]*os.File{"lock": l.file}}, nil
}

func (l *DataDirLock) Detach() (*os.File, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil, errors.New("cluster: lock owner detached or released")
	}
	f := l.file
	l.file = nil
	dataDirLockClaims.Lock()
	delete(dataDirLockClaims.owners, l.identity)
	dataDirLockClaims.Unlock()
	return f, nil
}

func (l *DataDirLock) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	// Only a terminal owner unlocks. An old release closure after Detach has no
	// file and cannot unlock the OFD now held by Prepared or the adopted owner.
	err := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	closeErr := l.file.Close()
	if err != nil || closeErr != nil {
		return errors.Join(err, closeErr)
	}
	l.file = nil
	dataDirLockClaims.Lock()
	delete(dataDirLockClaims.owners, l.identity)
	dataDirLockClaims.Unlock()
	return nil
}
