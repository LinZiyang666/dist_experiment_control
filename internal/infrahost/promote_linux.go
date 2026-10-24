//go:build linux

package infrahost

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// Root configuration supplies the directory, never a request. Walk every
// component without following links, then pin the final inode for all IO.
// Production euid is root; sandbox tests use their own private uid directory.
// origin: hot-update S9 plan §6.1 / §6.3 / T10
func openPrivateDirectory(path string) (*os.File, error) {
	if runtime.GOARCH != "amd64" {
		return nil, infracontract.ErrUnsupported
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, ErrPermission
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrPermission
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			_ = unix.Close(fd)
			return nil, ErrPermission
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(fd)
		if err != nil {
			return nil, ErrPermission
		}
		fd = next
	}
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Uid != uint32(os.Geteuid()) || st.Mode&07777 != 0700 {
		_ = unix.Close(fd)
		return nil, ErrPermission
	}
	return os.NewFile(uintptr(fd), "infra-private-directory"), nil
}

func openPrivateFile(dir *os.File, name string, flag int, mode os.FileMode) (*os.File, error) {
	if name == "" || strings.ContainsAny(name, "/\\") || name == "." || name == ".." {
		return nil, ErrPermission
	}
	fd, err := unix.Openat(int(dir.Fd()), name, flag|unix.O_NOFOLLOW|unix.O_CLOEXEC|unix.O_NONBLOCK, uint32(mode.Perm()))
	if err != nil {
		return nil, err
	}
	var st unix.Stat_t
	if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Nlink != 1 || st.Uid != uint32(os.Geteuid()) || st.Mode&07777 != uint32(mode.Perm()) {
		_ = unix.Close(fd)
		return nil, ErrPermission
	}
	return os.NewFile(uintptr(fd), "infra-private-file"), nil
}

// Separate opens are separate flock descriptions. Reusing one Journal FD would
// silently permit simultaneous CAS calls within a process. Never unlink lock.
// origin: hot-update S9 plan §6.1 / T09
func lockJournal(dir *os.File, d infracontract.Deadline, stop infracontract.Stop) (func(), error) {
	return lockJournalAt(dir, d, stop, systemHostClock{})
}

func lockJournalAt(dir *os.File, d infracontract.Deadline, stop infracontract.Stop, clock hostClock) (func(), error) {
	f, err := openPrivateFile(dir, "journal.lock", os.O_RDWR, 0600)
	if err != nil {
		return nil, ErrJournal
	}
	for {
		if err := checkStopAt(clock, d, stop); err != nil {
			_ = f.Close()
			return nil, err
		}
		err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			return func() { _ = f.Close() }, nil
		}
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			_ = f.Close()
			return nil, ErrJournal
		}
		remaining, err := clock.Remaining(d)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		expired, cancelTimer := clock.Timer(min(remaining, time.Millisecond))
		select {
		case <-stop:
			cancelTimer()
			_ = f.Close()
			return nil, ErrStopped
		case <-expired:
			cancelTimer()
		}
	}
}

func temporaryFile(dir *os.File) (*os.File, string, error) {
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, "", ErrJournal
	}
	name := ".pending-" + hex.EncodeToString(nonce[:])
	f, err := openPrivateFile(dir, name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0600)
	return f, name, err
}

func replacePrivateFile(dir *os.File, name string, data []byte) error {
	f, tmp, err := temporaryFile(dir)
	if err != nil {
		return ErrJournal
	}
	// f.Sync below is the durability witness; every earlier path already fails,
	// so the deferred close has no buffered write left to report.
	defer func() { _ = f.Close() }()
	// Best-effort cleanup: a failure path already returns ErrJournal, and the
	// private .pending- name is never read as a selector.
	defer func() { _ = unix.Unlinkat(int(dir.Fd()), tmp, 0) }() // Successful rename has already removed the temporary name.
	if _, err := f.Write(data); err != nil {
		return ErrJournal
	}
	if err := f.Sync(); err != nil {
		return ErrJournal
	}
	if err := unix.Renameat(int(dir.Fd()), tmp, int(dir.Fd()), name); err != nil {
		return ErrJournal
	}
	if err := dir.Sync(); err != nil {
		return ErrJournal
	}
	return nil
}

func artifactName(a infracontract.Artifact) string {
	return "binary-" + hex.EncodeToString(a.BinarySHA256[:])
}

// Copy first, close the writer, then hash the root-owned inode. Hashing the
// caller's FD before copying admits a writable-FD race even after chmod/chown.
// A size-changing source is rejected; a same-size race either copies the exact
// expected bytes or fails the hash. No source inode ever becomes executable.
// origin: hot-update S9 plan §6.2 / T10
func copyArtifact(dir, source *os.File, a infracontract.Artifact, d infracontract.Deadline, stop infracontract.Stop) error {
	if source == nil || a.GOOS != "linux" || a.GOARCH != "amd64" || a.BinarySHA256 == (infracontract.Digest{}) {
		return ErrArtifact
	}
	st, err := source.Stat()
	if err != nil || !st.Mode().IsRegular() || st.Size() <= 0 || st.Size() > 512<<20 {
		return ErrArtifact
	}
	f, tmp, err := temporaryFile(dir)
	if err != nil {
		return ErrArtifact
	}
	// The success path closes f explicitly and checks it below; this deferred
	// close only reclaims the descriptor on an early failure path.
	defer func() { _ = f.Close() }()
	// After a successful rename the temporary name is gone (ENOENT). On a
	// failure path the returned error already fails staging, and the private
	// .pending- name is never selected, so cleanup is best effort.
	defer func() { _ = unix.Unlinkat(int(dir.Fd()), tmp, 0) }()
	buffer := make([]byte, 64<<10)
	for offset := int64(0); offset < st.Size(); {
		if err := checkStop(d, stop); err != nil {
			return err
		}
		n, err := source.ReadAt(buffer[:min(int64(len(buffer)), st.Size()-offset)], offset)
		if err != nil || n == 0 {
			return ErrArtifact
		}
		if _, err := f.Write(buffer[:n]); err != nil {
			return ErrArtifact
		}
		offset += int64(n)
	}
	after, err := source.Stat()
	if err != nil || after.Size() != st.Size() {
		return ErrArtifact
	}
	if err := f.Sync(); err != nil {
		return ErrArtifact
	}
	if err := f.Close(); err != nil {
		return ErrArtifact
	}
	// The temporary name is confined to the root-owned pinned directory.
	owned, err := openPrivateFile(dir, tmp, os.O_RDONLY, 0600)
	if err != nil {
		return ErrArtifact
	}
	// Read-only: the hash, chmod and Sync below decide the result.
	defer func() { _ = owned.Close() }()
	if err := hashArtifact(owned, a.BinarySHA256, d, stop); err != nil {
		return err
	}
	if err := owned.Chmod(0755); err != nil {
		return ErrArtifact
	}
	if err := owned.Sync(); err != nil {
		return ErrArtifact
	}
	// Never overwrite an executable inode in place. Existing identical content
	// is independently verified; hard links and writable files remain invalid.
	err = unix.Renameat2(int(dir.Fd()), tmp, int(dir.Fd()), artifactName(a), unix.RENAME_NOREPLACE)
	if errors.Is(err, unix.EEXIST) {
		return verifyArtifact(dir, a, d, stop)
	}
	if err != nil {
		return ErrArtifact
	}
	if err := dir.Sync(); err != nil {
		return ErrArtifact
	}
	return nil
}

func verifyArtifact(dir *os.File, a infracontract.Artifact, d infracontract.Deadline, stop infracontract.Stop) error {
	if a.GOOS != "linux" || a.GOARCH != "amd64" {
		return infracontract.ErrUnsupported
	}
	f, err := openPrivateFile(dir, artifactName(a), os.O_RDONLY, 0755)
	if err != nil {
		return ErrArtifact
	}
	defer func() { _ = f.Close() }() // Read-only verification; close cannot change the verdict.
	return hashArtifact(f, a.BinarySHA256, d, stop)
}

func hashArtifact(f *os.File, expected infracontract.Digest, d infracontract.Deadline, stop infracontract.Stop) error {
	h := sha256.New()
	buffer := make([]byte, 64<<10)
	var size int64
	for {
		if err := checkStop(d, stop); err != nil {
			return err
		}
		n, err := f.Read(buffer)
		size += int64(n)
		if size > 512<<20 {
			return ErrArtifact
		}
		_, _ = h.Write(buffer[:n])
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ErrArtifact
		}
	}
	if size == 0 || !equalDigest(h.Sum(nil), expected) {
		return ErrArtifact
	}
	return nil
}

func equalDigest(data []byte, expected infracontract.Digest) bool {
	return len(data) == len(expected) && string(data) == string(expected[:])
}
