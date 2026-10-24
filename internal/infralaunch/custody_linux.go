//go:build linux

package infralaunch

import (
	"errors"
	"os"
	"path/filepath"
	"strconv"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// Files lends the immutable five-role set while Prepared retains custody. The
// caller may send it only as the authenticated LaunchClaim's exact reply.
func (p *Prepared) Files() ([]*os.File, error) {
	if err := p.verify(); err != nil {
		return nil, err
	}
	return []*os.File{p.executable, p.config, p.directory, p.lock.directory, p.lock.file}, nil
}

// AdoptPrepared takes every received descriptor on success or failure. No
// caller path is opened, and the permanent lock is never unlocked/reacquired.
func AdoptPrepared(r c.HostRequest, files []*os.File, d c.Deadline) (_ *Prepared, err error) {
	defer func() {
		if err != nil {
			for _, file := range files {
				err = errors.Join(err, file.Close())
			}
		}
	}()
	if len(files) != 5 {
		return nil, ErrLaunchRejected
	}
	p := &Prepared{request: r, executable: files[0], config: files[1], directory: files[2], lock: &Lock{files[4], files[3], r.Store, requestLockName(r)}, deadline: d}
	if err := p.verify(); err != nil {
		return nil, err
	}
	self, err := CurrentProcess(os.Getenv("INVOCATION_ID"))
	if err != nil || self != r.Expected {
		return nil, ErrLaunchRejected
	}
	return p, nil
}

// Successor is the identity to be independently observed, not an exec receipt.
func (p *Prepared) Successor() (c.Process, error) {
	if err := p.verify(); err != nil {
		return c.Process{}, err
	}
	var stat unix.Stat_t
	if unix.Fstat(int(p.executable.Fd()), &stat) != nil {
		return c.Process{}, ErrLaunchRejected
	}
	process := p.request.Expected
	process.ExeDevice, process.ExeInode, process.ExeSHA256 = uint64(stat.Dev), stat.Ino, p.request.Candidate.BinarySHA256
	return process, nil
}

// RecoverStoreLock obtains the actual receiver-held description after worker
// replacement. Opening /proc/PID/fd would create a new description and is NOT a
// replacement for pidfd_getfd. Missing kernel permission remains unsupported.
// The caller pins/revalidates the complete process on either side of this call.
// origin: S9 internal review rv-infra F03 / F04
func RecoverStoreLock(directory, locks *os.File, process c.Process, d c.Deadline) (*Lock, error) {
	return recoverLock(directory, locks, process, c.NATS, d)
}

func RecoverACMEWriter(directory, locks *os.File, process c.Process, d c.Deadline) (*Lock, error) {
	return recoverLock(directory, locks, process, c.Caddy, d)
}

func recoverLock(directory, locks *os.File, process c.Process, component c.Component, d c.Deadline) (*Lock, error) {
	if err := admission(d, nil); err != nil {
		return nil, err
	}
	pidfd, err := unix.PidfdOpen(int(process.PID), 0)
	if err != nil {
		return nil, c.ErrUnsupported
	}
	defer unix.Close(pidfd) //nolint:errcheck // The read-only pidfd is disposable after recovery and cannot affect the inherited lock custody.
	base := "/proc/" + strconv.Itoa(int(process.PID))
	entries, err := os.ReadDir(base + "/fd")
	if err != nil {
		return nil, ErrLaunchRejected
	}
	identity, err := StoreIdentity(directory)
	if err != nil {
		return nil, err
	}
	var named unix.Stat_t
	if unix.Fstatat(int(locks.Fd()), requestLockName(c.HostRequest{Component: component, Store: identity}), &named, unix.AT_SYMLINK_NOFOLLOW) != nil {
		return nil, ErrLaunchRejected
	}
	for _, entry := range entries {
		var st unix.Stat_t
		if unix.Stat(filepath.Join(base, "fd", entry.Name()), &st) != nil || st.Dev != named.Dev || st.Ino != named.Ino {
			continue
		}
		target, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		fd, err := unix.PidfdGetfd(pidfd, target, 0)
		if err != nil {
			return nil, c.ErrUnsupported
		}
		file := os.NewFile(uintptr(fd), "infra-recovered-lock")
		var lock *Lock
		if component == c.Caddy {
			lock, err = AdoptACMEWriter(directory, locks, file)
		} else {
			lock, err = AdoptStoreLock(directory, locks, file)
		}
		if closeErr := file.Close(); closeErr != nil {
			if lock != nil {
				closeErr = errors.Join(closeErr, lock.Close())
			}
			return nil, errors.Join(err, closeErr)
		}
		if err != nil {
			return nil, err
		}
		if err := inheritedLock(lock, process); err != nil {
			return nil, errors.Join(err, lock.Close())
		}
		return lock, nil
	}
	return nil, ErrLaunchRejected
}

func requestLockName(r c.HostRequest) string {
	if r.Component == c.Caddy {
		return "writer.lock"
	}
	return lockName(r.Store)
}

// StoreWithLock observes a pre-existing permanent name without claiming it.
func StoreWithLock(directory, locks *os.File, component c.Component) (c.Store, error) {
	identity, err := StoreIdentity(directory)
	if err != nil {
		return c.Store{}, err
	}
	name := lockName(identity)
	if component == c.Caddy {
		name = "writer.lock"
	}
	var stat unix.Stat_t
	if unix.Fstatat(int(locks.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW) != nil || stat.Mode&unix.S_IFMT != unix.S_IFREG || stat.Nlink != 1 || stat.Mode&0022 != 0 {
		return c.Store{}, ErrLaunchRejected
	}
	identity.LockDevice, identity.LockInode = uint64(stat.Dev), stat.Ino
	return identity, nil
}

// AdoptACMEWriter retains the exact filesystem writer lease in the same five
// descriptor protocol used by NATS. The name is fixed by the component.
func AdoptACMEWriter(directory, root, lease *os.File) (*Lock, error) {
	identity, err := StoreIdentity(directory)
	if err != nil {
		return nil, err
	}
	file, err := duplicateFile(lease)
	if err != nil {
		return nil, err
	}
	locks, err := duplicateFile(root)
	if err != nil {
		return nil, errors.Join(err, file.Close())
	}
	lock := &Lock{file, locks, identity, "writer.lock"}
	if err := lock.verify(); err != nil {
		return nil, errors.Join(err, lock.Close())
	}
	return lock, nil
}
