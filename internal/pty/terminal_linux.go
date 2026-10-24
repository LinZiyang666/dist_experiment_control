//go:build linux

package pty

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

// terminalMaster owns one Go wrapper. Closing it also revokes the read-side
// EIO qualification: an explicit hangup is an abort, not natural source FIN.
type terminalMaster struct {
	file   *os.File
	closed atomic.Bool
}

func (m *terminalMaster) Read(p []byte) (int, error) {
	n, err := m.file.Read(p)
	return terminalReadResult(n, err, !m.closed.Load())
}
func (m *terminalMaster) Write(p []byte) (int, error)        { return m.file.Write(p) }
func (m *terminalMaster) SetReadDeadline(t time.Time) error  { return m.file.SetReadDeadline(t) }
func (m *terminalMaster) SetWriteDeadline(t time.Time) error { return m.file.SetWriteDeadline(t) }
func (m *terminalMaster) Resize(cols, rows uint16) error     { return resizeMaster(m.file, cols, rows) }
func (m *terminalMaster) Borrow() *os.File                   { return m.file }
func (m *terminalMaster) Close() error {
	if m.closed.Swap(true) {
		return nil
	}
	return m.file.Close()
}

// Observe never repairs the terminal. In particular, a zombie leader can have
// lost its controlling tty while descendants are still draining the slave.
// Winsize and termios may also be unavailable after last-slave close; missing
// observations must not invalidate the permanent master identity.
// origin: hot-update S4 plan §3.1 / T10
func (m *terminalMaster) Observe() (TerminalObservation, error) {
	var observation TerminalObservation
	err := controlMaster(m.file, func(fd int) error {
		if size, err := unix.IoctlGetWinsize(fd, unix.TIOCGWINSZ); err == nil {
			observation.Cols, observation.Rows = size.Col, size.Row
		}
		if term, err := unix.IoctlGetTermios(fd, unix.TCGETS); err == nil {
			var encodeErr error
			observation.Termios, encodeErr = binary.Append(nil, binary.NativeEndian, term)
			if encodeErr != nil {
				return encodeErr
			}
		}
		sid, sidErr := unix.IoctlGetInt(fd, unix.TIOCGSID)
		pgid, pgidErr := unix.IoctlGetInt(fd, unix.TIOCGPGRP)
		if sidErr == nil && pgidErr == nil {
			observation.SessionID, observation.ForegroundPGID = sid, pgid
			observation.ControlTTYKnown = true
		}
		return nil
	})
	return observation, err
}

func takeTerminalMaster(file *os.File, row workload.Record) (Master, TerminalIdentity, error) {
	identity, err := masterIdentityFromFile(file)
	if err != nil {
		return nil, TerminalIdentity{}, err
	}
	identity.Version, identity.Started = 1, true
	identity.BootID, identity.LeaderPID, identity.StartTimeTicks = row.BootID, row.OSPID, row.StartTimeTicks
	if err := CheckMaster(file, identity, row); err != nil {
		return nil, TerminalIdentity{}, err
	}
	if err := masterDeadlines(file); err != nil {
		return nil, TerminalIdentity{}, err
	}
	return &terminalMaster{file: file}, identity, nil
}

// RestoreMaster only consumes file after a replacement wrapper is ready. Dup
// gives Go a new descriptor number to register, without modifying the frozen
// source's shared OFD flags. Failure leaves file usable by salvage.
// origin: hot-update S4 plan §3.1 / T10
func RestoreMaster(file *os.File, identity TerminalIdentity, row workload.Record) (Master, error) {
	if err := CheckMaster(file, identity, row); err != nil {
		return nil, err
	}
	duplicate := -1
	err := controlMaster(file, func(fd int) error {
		var err error
		duplicate, err = unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 3)
		return err
	})
	if err != nil {
		return nil, err
	}
	replacement := os.NewFile(uintptr(duplicate), file.Name())
	if err := masterDeadlines(replacement); err != nil {
		_ = replacement.Close()
		return nil, err
	}
	// Frozen caller ownership excludes a concurrent Close. No fallible setup
	// remains after consuming the old wrapper.
	_ = file.Close()
	return &terminalMaster{file: replacement}, nil
}

// CheckMaster is read-only, including on malformed flags. fstat alone cannot
// distinguish two /dev/ptmx opens; mount plus TIOCGPTN binds the original pair.
// The process row reserves the leader, so PID reuse cannot borrow that proof.
// origin: hot-update S4 plan §3.1 / T10
func CheckMaster(file *os.File, identity TerminalIdentity, row workload.Record) error {
	if identity.Version != 1 || !identity.Started || identity.BootID == "" || identity.LeaderPID <= 0 ||
		identity.StartTimeTicks <= 0 || identity.MountID == 0 || row.ID == "" || row.StartedAt.IsZero() ||
		row.Exit != nil || !row.ReapHeld || row.OSPID != identity.LeaderPID ||
		row.BootID != identity.BootID || row.StartTimeTicks != identity.StartTimeTicks {
		return fmt.Errorf("pty: invalid terminal process identity")
	}
	actual, err := masterIdentityFromFile(file)
	if err != nil {
		return err
	}
	if actual.Dev != identity.Dev || actual.Ino != identity.Ino || actual.Rdev != identity.Rdev ||
		actual.MountID != identity.MountID || actual.PTYIndex != identity.PTYIndex {
		return fmt.Errorf("pty: master identity changed")
	}
	return checkTerminalProcess(row.Identity)
}

func masterIdentityFromFile(file *os.File) (TerminalIdentity, error) {
	var identity TerminalIdentity
	err := controlMaster(file, func(fd int) error {
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
		if err != nil {
			return err
		}
		descriptor, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil {
			return err
		}
		// Never repair flags here: F_SETFL would mutate every frozen duplicate.
		if flags&unix.O_NONBLOCK == 0 || flags&unix.O_ACCMODE != unix.O_RDWR || descriptor&unix.FD_CLOEXEC == 0 {
			return fmt.Errorf("pty: master requires nonblocking read-write CLOEXEC descriptor")
		}
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFCHR {
			return fmt.Errorf("pty: master is not a character device")
		}
		index, err := unix.IoctlGetUint32(fd, unix.TIOCGPTN)
		if err != nil {
			return fmt.Errorf("pty: master index: %w", err)
		}
		var fs unix.Statfs_t
		if err := unix.Fstatfs(fd, &fs); err != nil {
			return err
		}
		var mount unix.Statx_t
		if fs.Type == unix.DEVPTS_SUPER_MAGIC {
			if err := unix.Statx(fd, "", unix.AT_EMPTY_PATH, unix.STATX_MNT_ID, &mount); err != nil {
				return err
			}
		} else {
			// /dev/ptmx can be a standalone 5:2 character node on tmpfs,
			// rather than a symlink to /dev/pts/ptmx. Its fstatfs then names
			// /dev, not the slave's devpts. creack's allocator uses this
			// same /dev/pts/<index> path; stat it without opening a slave.
			// origin: hot-update S4 plan §3.1 / T10
			path := "/dev/pts/" + strconv.FormatUint(uint64(index), 10)
			if err := unix.Statfs(path, &fs); err != nil {
				return err
			}
			if fs.Type != unix.DEVPTS_SUPER_MAGIC {
				return fmt.Errorf("pty: slave is not on devpts")
			}
			if err := unix.Statx(unix.AT_FDCWD, path, unix.AT_SYMLINK_NOFOLLOW|unix.AT_NO_AUTOMOUNT,
				unix.STATX_MNT_ID|unix.STATX_TYPE, &mount); err != nil {
				return err
			}
			if mount.Mode&unix.S_IFMT != unix.S_IFCHR {
				return fmt.Errorf("pty: slave identity is not a character device")
			}
		}
		if mount.Mask&unix.STATX_MNT_ID == 0 || mount.Mnt_id == 0 {
			return fmt.Errorf("pty: master mount identity unavailable")
		}
		identity.Dev, identity.Ino, identity.Rdev = uint64(stat.Dev), stat.Ino, uint64(stat.Rdev)
		identity.MountID, identity.PTYIndex = mount.Mnt_id, index
		return nil
	})
	return identity, err
}

// checkTerminalProcess permits zombies but requires the original direct child,
// boot and start ticks. Current tty SID/pgrp are deliberately not qualifications.
func checkTerminalProcess(id workload.Identity) error {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return err
	}
	if strings.TrimSpace(string(boot)) != id.BootID {
		return fmt.Errorf("pty: terminal boot identity changed")
	}
	data, err := os.ReadFile("/proc/" + strconv.Itoa(id.OSPID) + "/stat")
	if err != nil {
		return err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return fmt.Errorf("pty: malformed leader identity")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return fmt.Errorf("pty: incomplete leader identity")
	}
	parent, parentErr := strconv.Atoi(fields[1])
	group, groupErr := strconv.Atoi(fields[2])
	ticks, ticksErr := strconv.ParseInt(fields[19], 10, 64)
	if parentErr != nil || groupErr != nil || ticksErr != nil || parent != os.Getpid() || group != id.OSPID || ticks != id.StartTimeTicks {
		return fmt.Errorf("pty: leader identity changed")
	}
	return nil
}

func masterDeadlines(file *os.File) error {
	if err := file.SetReadDeadline(time.Time{}); err != nil {
		return err
	}
	return file.SetWriteDeadline(time.Time{})
}

func controlMaster(file *os.File, fn func(int) error) error {
	if file == nil {
		return fmt.Errorf("pty: missing master")
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var inner error
	if err := raw.Control(func(fd uintptr) { inner = fn(int(fd)) }); err != nil {
		return err
	}
	return inner
}

// Called with the Session lock, before any legacy pump exists. This proof is
// independent of Session.started, which is also true after a failed Start.
func legacyTerminalVerified(s *Session) bool {
	if !s.ready || !s.draining || s.owner == nil {
		return false
	}
	row, ok := s.owner.Lookup(s.taskID)
	if !ok {
		return false
	}
	identity, err := masterIdentityFromFile(s.Master)
	if err != nil {
		return false
	}
	identity.Version, identity.Started = 1, true
	identity.BootID, identity.LeaderPID, identity.StartTimeTicks = row.BootID, row.OSPID, row.StartTimeTicks
	return CheckMaster(s.Master, identity, row) == nil
}
