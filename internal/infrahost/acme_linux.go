//go:build linux

package infrahost

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash"
	"io"
	"os"
	"runtime"
	"sort"
	"time"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// The platform verdict is a before-effect gate, including warm snapshots. A
// caller must not advertise a hot-cut capability after merely compiling this
// package on another architecture. systemd, pidfd census and nft qualification
// belong to their own owners; this gate is only the storage backend's surface.
// origin: hot-update S9 plan lead LQ1 / §5.3
func acmePlatform() error {
	if runtime.GOARCH != "amd64" {
		return infracontract.ErrUnsupported
	}
	return nil
}

// A duplicate gives the worker ownership without closing a policy loader's FD.
// Keeping it CLOEXEC prevents certificates and selectors from reaching unrelated
// tools. Only the launcher may explicitly authorize a directory for stock exec.
// The trusted loader must approve ancestors before supplying this descriptor;
// fstat here cannot turn an arbitrary caller's directory into an approved target.
// Pinning the descriptor then removes later ancestor/path replacement from I/O.
// origin: hot-update S9 plan §5.3 / §6.1 / T20
func acmeDuplicateDirectory(file *os.File, owner int) (*os.File, error) {
	fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, errACMEInternal
	}
	copy := os.NewFile(uintptr(fd), "acme-policy-directory")
	var stat unix.Stat_t
	if unix.Fstat(fd, &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mode&0077 != 0 || (owner >= 0 && stat.Uid != uint32(owner)) {
		_ = copy.Close()
		return nil, infracontract.ErrIncomplete
	}
	return copy, nil
}

// openat2 pins traversal to a policy-approved descriptor, refuses symlinks,
// magic links and mount escapes, and never falls back to pathname traversal.
// O_NONBLOCK prevents opening a hostile FIFO/device from stalling a root worker.
// This is also used for destination entries, so an interrupted tree cannot turn
// a retry into a write through a symlink. Names originate in a bounded directory
// census or a closed private slot, never in the signed request's arbitrary text.
// An unavailable openat2 is unsupported, rather than a less-safe openat fallback.
// origin: hot-update S9 plan §5.3 / T20
func acmeOpen(parent *os.File, name string, flags int, mode uint32) (*os.File, error) {
	fd, err := unix.Openat2(int(parent.Fd()), name, &unix.OpenHow{
		Flags: uint64(flags | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_NONBLOCK), Mode: uint64(mode),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_XDEV,
	})
	if errors.Is(err, unix.ENOSYS) || errors.Is(err, unix.EINVAL) {
		return nil, infracontract.ErrUnsupported
	}
	if err != nil {
		return nil, errACMEInternal
	}
	return os.NewFile(uintptr(fd), "acme-private-entry"), nil
}

func acmeOpenDirectory(parent *os.File, name string) (*os.File, error) {
	return acmeOpen(parent, name, unix.O_RDONLY|unix.O_DIRECTORY, 0)
}

// Lock files are permanent inodes: never unlink one while another invocation can
// hold the previous OFD. Nonblocking flock plus a budget bounds lock contention.
// Each open creates a distinct OFD, so a second worker really contends even in
// the same process. The returned file is the lease: close, inherited dup and exec
// use the kernel's reference lifetime, not a boolean in a dead controller.
// Install/retrofit/uninstall must preserve these inodes while the domain is live.
// A writable alias or unexpected owner would defeat the permanent-lock premise,
// so those shapes are rejected even inside the already private root directory.
// origin: hot-update S9 plan §5.3 / T21
func acmeLock(root *os.File, name string, d infracontract.Deadline, stop infracontract.Stop, wait bool) (*os.File, error) {
	if err := acmeCheck(d, stop); err != nil {
		return nil, err
	}
	file, err := acmeOpen(root, name, unix.O_RDWR|unix.O_CREAT, 0600)
	if err != nil {
		return nil, err
	}
	var stat unix.Stat_t
	if unix.Fstat(int(file.Fd()), &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0777 != 0600 {
		_ = file.Close()
		return nil, infracontract.ErrIncomplete
	}
	for {
		if err := acmeCheck(d, stop); err != nil {
			_ = file.Close()
			return nil, err
		}
		if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err == nil {
			return file, nil
		} else if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EINTR) {
			_ = file.Close()
			return nil, errACMEInternal
		}
		if !wait {
			_ = file.Close()
			return nil, errACMEBusy
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-stop:
			timer.Stop()
			_ = file.Close()
			return nil, errACMEBudget
		case <-timer.C:
		}
	}
}

func (s *ACMEStorage) readState(allowMissing bool) (acmeState, error) {
	var stat unix.Stat_t
	// Missing state permits only an initial warm read or writing the first fence.
	// It never grants startup. Finalization and launch require an existing record;
	// treating a deleted/corrupt selector as an empty default would reopen startup.
	if err := unix.Fstatat(int(s.root.Fd()), "selector.json", &stat, unix.AT_SYMLINK_NOFOLLOW); errors.Is(err, unix.ENOENT) && allowMissing {
		return acmeState{Version: 1, Purpose: "tether-acme-storage-v1", UID: s.config.UID, GID: s.config.GID, Fenced: true}, nil
	} else if err != nil {
		return acmeState{}, errACMENotReady
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Uid != uint32(os.Geteuid()) || stat.Mode&0777 != 0600 || stat.Size > 64<<10 {
		return acmeState{}, infracontract.ErrIncomplete
	}
	// Size is checked both in metadata and while reading: a concurrently enlarged
	// root record must not consume unbounded memory before the codec rejects it.
	// Only this backend's transaction lock protects the local selector; the common
	// root journal still supplies sequence, authority and policy authorization.
	file, err := acmeOpen(s.root, "selector.json", unix.O_RDONLY, 0)
	if err != nil {
		return acmeState{}, err
	}
	defer func() { _ = file.Close() }() // Read-only; the bytes are validated below, not by close.
	b, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil || len(b) > 64<<10 {
		return acmeState{}, infracontract.ErrIncomplete
	}
	var state acmeState
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil {
		return acmeState{}, infracontract.ErrIncomplete
	}
	canonical, _ := json.Marshal(state)
	// Canonical re-encoding also rejects duplicate keys, alternate spellings and
	// trailing bytes; a corrupt barrier can never decode to an implicit permit.
	// origin: hot-update S9 plan §2.2 / §5.3 / T21
	// Version/purpose prevent a different root record from becoming a permit.
	// Ownership is part of the selector commitment, not a best-effort chown after
	// start. Slot admission also applies to pending/garbage cleanup references;
	// accepting only Current would leave a traversal through a recovery field.
	if !bytes.Equal(canonical, b) || state.Version != 1 || state.Purpose != "tether-acme-storage-v1" || state.UID != s.config.UID || state.GID != s.config.GID || state.Revision == 0 ||
		(state.Current != "" && !acmeSlot(state.Current, "tree-")) || (state.Pending != "" && !acmeSlot(state.Pending, "tree-")) || (state.Garbage != "" && !acmeSlot(state.Garbage, "tree-")) ||
		(!state.Fenced && !state.Finalized) || (state.Finalized && (state.Current == "" || state.TreeDigest == (infracontract.Digest{}) || state.RequestDigest == (infracontract.Digest{}))) {
		return acmeState{}, infracontract.ErrIncomplete
	}
	return state, nil
}

func (s *ACMEStorage) writeState(state acmeState) error {
	// Fixed Go field names/order are the private v1 codec. There are no omitted
	// flags or plaintext keys. P35 must track producer, decoder and all these fields
	// together; a new selector field cannot silently disappear from the inventory.
	b, err := json.Marshal(state)
	if err != nil {
		return errACMEInternal
	}
	return s.atomicFile(s.root, "selector.json", b, os.Geteuid(), os.Getegid(), 0600)
}

// A fresh inode is synced before rename, then the containing directory is synced.
// Neither an old snapshot nor an unsynced rename can release startup after crash.
// Every error returned from filesystem I/O is sanitized: PathError may contain
// account names or certificate subjects, which do not belong in public errors.
// Ownership/mode are set on the new inode before it is exposed under its final
// name. A failed chown cannot be downgraded to a warning: the future service UID
// would either lose its keys or gain access to a tree with the wrong protection.
// After-rename errors remain errors even when the effect is visible. P04 must
// reconcile the durable intent/result and actual state before granting a permit.
// origin: hot-update S9 plan §5.3 / T20 / T21
func (s *ACMEStorage) atomicFile(parent *os.File, name string, b []byte, uid, gid int, mode uint32) error {
	temp, err := acmeRandomSlot("tmp-")
	if err != nil {
		return err
	}
	file, err := acmeOpen(parent, temp, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL, 0600)
	if err != nil {
		return err
	}
	// The fsync below is the durability witness: once it succeeds, close has no
	// buffered write left to report, and before it every path already fails.
	defer func() { _ = file.Close() }()
	// After a successful rename the temporary name is gone and this reports
	// ENOENT. On a failure path the returned error already fails the action, and
	// the private tmp- name is never a selector, so cleanup is best effort.
	defer func() { _ = unix.Unlinkat(int(parent.Fd()), temp, 0) }()
	if _, err := file.Write(b); err != nil || unix.Fchown(int(file.Fd()), uid, gid) != nil || unix.Fchmod(int(file.Fd()), mode) != nil {
		return errACMEInternal
	}
	if err := s.syncFile(file); err != nil {
		return errACMEInternal
	}
	if err := unix.Renameat(int(parent.Fd()), temp, int(parent.Fd()), name); err != nil {
		return errACMEInternal
	}
	if err := s.syncFile(parent); err != nil {
		return errACMEInternal
	}
	return nil
}

func (s *ACMEStorage) current(state acmeState) (*os.File, error) {
	// Rollback deliberately reads the current selector, never a warm/previous tree.
	// origin: hot-update S9 plan §5.3 / T20 / M11
	if state.Current == "" {
		// This is the one legacy read before the first successful migration. Later
		// renewals belong to Current, even when the target executable is the old SHA.
		return acmeOpenDirectory(s.initial, ".")
	}
	return acmeOpenDirectory(s.root, state.Current)
}

// Private random names are references, not secret material or caller paths.
// A final tree keeps its name across checkpoint retries; inventing a new slot
// each attempt would lose progress and accumulate unbounded partial copies.
func acmeRandomSlot(prefix string) (string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return "", errACMEInternal
	}
	return prefix + hex.EncodeToString(nonce[:]), nil
}

type acmeCopyBudget struct {
	entries int
	bytes   int64
}

func (s *ACMEStorage) copyTree(source *os.File, slot string, warm bool, d infracontract.Deadline, stop infracontract.Stop) (infracontract.Digest, error) {
	if err := acmeCheck(d, stop); err != nil {
		return infracontract.Digest{}, err
	}
	// Reject oversized inventories before spending a bounded action on thousands
	// of fsyncs. Copy rechecks the limits because a warm writer may still add files.
	// origin: hot-update S9 plan §5.3 / §6.1 / T20
	if err := acmeScanDirectory(source, "", 0, &acmeCopyBudget{}, d, stop); err != nil {
		return infracontract.Digest{}, err
	}
	if err := unix.Mkdirat(int(s.root.Fd()), slot, 0700); err != nil && !errors.Is(err, unix.EEXIST) {
		return infracontract.Digest{}, errACMEInternal
	}
	dest, err := acmeOpenDirectory(s.root, slot)
	if err != nil {
		return infracontract.Digest{}, err
	}
	// Directory effects are made durable by the syncFile calls in copyDirectory
	// and below; closing the descriptor afterwards has nothing left to report.
	defer func() { _ = dest.Close() }()
	// Sorted paths, directory markers and fixed-size file hashes commit deletions,
	// empty directories and contents without placing any raw key in the receipt.
	// Both an A->B upgrade and a B->A rollback use this same current-tree producer.
	h := sha256.New()
	if err := s.copyDirectory(source, dest, "", warm, 0, &acmeCopyBudget{}, h, d, stop); err != nil {
		return infracontract.Digest{}, err
	}
	if err := s.syncFile(s.root); err != nil {
		return infracontract.Digest{}, errACMEInternal
	}
	var digest infracontract.Digest
	copy(digest[:], h.Sum(nil))
	return digest, nil
}

// This metadata pass is deliberately read-only. Its bounds are across the whole
// tree, not just one directory: many individually small pages can still exhaust
// a worker. Warm remains subject to concurrent renewal, so this pass never signs
// completeness on behalf of the later copy or the physical old-writer census.
// The second pass validates actual opened entries and their bytes independently.
func acmeScanDirectory(source *os.File, prefix string, depth int, budget *acmeCopyBudget, d infracontract.Deadline, stop infracontract.Stop) error {
	if depth > 32 {
		return infracontract.ErrIncomplete
	}
	entries, err := acmeEntries(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := acmeCheck(d, stop); err != nil {
			return err
		}
		name := entry.Name()
		if depth == 0 && name == "locks" {
			continue
		}
		budget.entries++
		if budget.entries > 4096 || len(prefix)+len(name) > 4096 {
			return infracontract.ErrIncomplete
		}
		file, err := acmeOpen(source, name, unix.O_RDONLY, 0)
		if err != nil {
			return err
		}
		var stat unix.Stat_t
		if unix.Fstat(int(file.Fd()), &stat) != nil {
			_ = file.Close()
			return infracontract.ErrIncomplete
		}
		if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
			err = acmeScanDirectory(file, prefix+name+"/", depth+1, budget, d, stop)
		} else if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Size < 0 || stat.Size > 16<<20 {
			err = infracontract.ErrIncomplete
		} else {
			budget.bytes += stat.Size
			if budget.bytes > 64<<20 {
				err = infracontract.ErrIncomplete
			}
		}
		_ = file.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// Reopening "." gives every pass a fresh directory cursor. A dup would share
// offsets with another reader, making retries observe an empty suffix as a full
// inventory. The extra entry is a rejection witness, not a truncation allowance.
func acmeEntries(dir *os.File) ([]os.DirEntry, error) {
	reader, err := acmeOpenDirectory(dir, ".")
	if err != nil {
		return nil, err
	}
	defer func() { _ = reader.Close() }() // A private read-only cursor.
	entries, err := reader.ReadDir(4097)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, errACMEInternal
	}
	if len(entries) > 4096 {
		return nil, infracontract.ErrIncomplete
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	return entries, nil
}

func (s *ACMEStorage) copyDirectory(source, dest *os.File, prefix string, warm bool, depth int, budget *acmeCopyBudget, h hash.Hash, d infracontract.Deadline, stop infracontract.Stop) error {
	if depth > 32 {
		return infracontract.ErrIncomplete
	}
	entries, err := acmeEntries(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := acmeCheck(d, stop); err != nil {
			return err
		}
		if s.copyStep != nil {
			s.copyStep()
		}
		name := entry.Name()
		// CertMagic locks are not certificate state. Warm omits them without
		// touching the source; final omits them only after the complete exit proof.
		// origin: hot-update S9 plan §5.3 / T20
		if depth == 0 && name == "locks" {
			continue
		}
		budget.entries++
		if budget.entries > 4096 || len(prefix)+len(name) > 4096 {
			return infracontract.ErrIncomplete
		}
		file, err := acmeOpen(source, name, unix.O_RDONLY, 0)
		if err != nil {
			return err
		}
		err = s.copyEntry(file, dest, name, prefix, warm, depth, budget, h, d, stop)
		_ = file.Close()
		if err != nil {
			return err
		}
	}
	// Destination enumeration removes files deleted since a failed checkpoint.
	// A full delta includes deletions and empty directories, not just new keys.
	destEntries, err := acmeEntries(dest)
	if err != nil {
		return err
	}
	keep := make(map[string]bool, len(entries))
	for _, entry := range entries {
		if depth != 0 || entry.Name() != "locks" {
			keep[entry.Name()] = true
		}
	}
	for _, entry := range destEntries {
		if !keep[entry.Name()] {
			if err := acmeRemoveTree(dest, entry.Name()); err != nil {
				return err
			}
		}
	}
	// Directory ownership is needed even when it is empty. Files alone are not a
	// complete migration: renewal creates children and locks beneath these parents.
	// Warm protection is applied only after filling the directory, so the producer
	// never has to relax a directory already handed to the serving warm invocation.
	mode := uint32(0700)
	if warm {
		mode = 0500
	}
	if unix.Fchown(int(dest.Fd()), s.config.UID, s.config.GID) != nil || unix.Fchmod(int(dest.Fd()), mode) != nil || s.syncFile(dest) != nil {
		return errACMEInternal
	}
	return nil
}

func (s *ACMEStorage) copyEntry(file, dest *os.File, name, prefix string, warm bool, depth int, budget *acmeCopyBudget, h hash.Hash, d infracontract.Deadline, stop infracontract.Stop) error {
	var stat unix.Stat_t
	if unix.Fstat(int(file.Fd()), &stat) != nil || (stat.Uid != 0 && stat.Uid != uint32(s.config.UID) && stat.Uid != uint32(os.Geteuid())) || stat.Mode&0022 != 0 {
		return infracontract.ErrIncomplete
	}
	path := prefix + name
	if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
		if err := unix.Mkdirat(int(dest.Fd()), name, 0700); err != nil && !errors.Is(err, unix.EEXIST) {
			return errACMEInternal
		}
		child, err := acmeOpenDirectory(dest, name)
		if err != nil {
			return err
		}
		// copyDirectory syncs the child before returning; see copyTree's dest.
		defer func() { _ = child.Close() }()
		_, _ = h.Write([]byte("d:" + path + "\x00"))
		return s.copyDirectory(file, child, path+"/", warm, depth+1, budget, h, d, stop)
	}
	// A hardlinked key may alias another writer outside the approved tree. Copying
	// it would defeat the complete-writer proof even though it looks like a regular
	// file. Devices/FIFOs/sockets likewise cannot supply stable filesystem state.
	if stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Size < 0 || stat.Size > 16<<20 {
		return infracontract.ErrIncomplete
	}
	budget.bytes += stat.Size
	if budget.bytes > 64<<20 {
		return infracontract.ErrIncomplete
	}
	b, err := acmeReadBounded(file, stat.Size, d, stop)
	if err != nil {
		return err
	}
	// Warm can race a renewal. A changed opened inode is incomplete, rather than
	// signing a partial account/key pair. P08 additionally validates certificate
	// and key coherence before using the warm configuration to serve requests.
	var after unix.Stat_t
	if unix.Fstat(int(file.Fd()), &after) != nil || after.Size != stat.Size || after.Mtim != stat.Mtim || after.Ctim != stat.Ctim {
		return infracontract.ErrIncomplete
	}
	content := sha256.Sum256(b)
	_, _ = h.Write([]byte("f:" + path + "\x00"))
	_, _ = h.Write(content[:])
	mode := uint32(0600)
	if warm {
		mode = 0400
	}
	// Reuse a completed file after an interrupted bounded action only after
	// rechecking bytes, owner and mode. Size alone can preserve an old renewal.
	if acmeFileMatches(dest, name, b, s.config.UID, s.config.GID, mode, d, stop) {
		return nil
	}
	return s.atomicFile(dest, name, b, s.config.UID, s.config.GID, mode)
}

// Cancellation/budget is checked per chunk, including reads of complete files
// during checkpoint verification. A partial buffer is never published. Root
// worker supervision remains responsible for a kernel filesystem syscall that
// itself stalls; this function introduces no abandoned I/O goroutine to hide it.
func acmeReadBounded(file *os.File, size int64, d infracontract.Deadline, stop infracontract.Stop) ([]byte, error) {
	b := make([]byte, 0, int(size))
	buffer := make([]byte, 32<<10)
	for {
		if err := acmeCheck(d, stop); err != nil {
			return nil, err
		}
		n, err := file.Read(buffer)
		b = append(b, buffer[:n]...)
		if int64(len(b)) > size {
			return nil, infracontract.ErrIncomplete
		}
		if errors.Is(err, io.EOF) {
			if int64(len(b)) != size {
				return nil, infracontract.ErrIncomplete
			}
			return b, nil
		}
		if err != nil {
			return nil, errACMEInternal
		}
	}
}

// A checkpoint reuse is an optimization after proving exact bytes and metadata.
// Same length is particularly unsafe for account keys and renewed metadata;
// stale bytes could make rollback appear healthy until the next renewal fails.
// A mismatch returns to atomic replacement, preserving the partial tree's fence.
func acmeFileMatches(dir *os.File, name string, b []byte, uid, gid int, mode uint32, d infracontract.Deadline, stop infracontract.Stop) bool {
	file, err := acmeOpen(dir, name, unix.O_RDONLY, 0)
	if err != nil {
		return false
	}
	defer func() { _ = file.Close() }() // Read-only verification; close cannot change the verdict.
	var stat unix.Stat_t
	if unix.Fstat(int(file.Fd()), &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Size != int64(len(b)) || stat.Uid != uint32(uid) || stat.Gid != uint32(gid) || stat.Mode&0777 != mode {
		return false
	}
	old, err := acmeReadBounded(file, stat.Size, d, stop)
	return err == nil && bytes.Equal(old, b)
}

// Removal is also descriptor-relative. No recursive pathname helper may follow
// a link inserted in a private tree or unlink the permanent lock inodes.
// State slot validation limits top-level cleanup to tree-/warm- references.
// Recursive entries use lstat and unlink on links; only actual directories are
// opened. Restoring write permission is retirement, after warm responsibility is
// empty, and does not grant a live warm writer access to the selected main tree.
func acmeRemoveTree(parent *os.File, name string) error {
	var stat unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW); errors.Is(err, unix.ENOENT) {
		return nil
	} else if err != nil {
		return errACMEInternal
	}
	flags := 0
	if stat.Mode&unix.S_IFMT == unix.S_IFDIR {
		dir, err := acmeOpenDirectory(parent, name)
		if err != nil {
			return err
		}
		// The unlink and parent sync below decide the result; this descriptor
		// only carried the fchmod and the cursor, so its close has nothing to add.
		defer func() { _ = dir.Close() }()
		// Warm trees are read-only to their consumer until responsibility is zero.
		if unix.Fchmod(int(dir.Fd()), 0700) != nil {
			return errACMEInternal
		}
		entries, err := acmeEntries(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := acmeRemoveTree(dir, entry.Name()); err != nil {
				return err
			}
		}
		flags = unix.AT_REMOVEDIR
	}
	if unix.Unlinkat(int(parent.Fd()), name, flags) != nil || parent.Sync() != nil {
		return errACMEInternal
	}
	return nil
}
