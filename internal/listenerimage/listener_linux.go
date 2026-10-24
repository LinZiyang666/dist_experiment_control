//go:build linux

package listenerimage

import (
	"errors"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"golang.org/x/sys/unix"
)

// Adopt validates without changing shared flags or consuming SO_ERROR. It takes
// file only on success and stays paused until Resume creates the new generation.
// origin: hot-update S6 plan §4.2 / §6.1 / T18
func Adopt(desc Descriptor, file *os.File) (Keeper, error) {
	// The importer has already registered each nonnil Take result. Refusing
	// this leaf must leave that exact file available for pristine salvage;
	// neither a rejected descriptor nor an alias error consumes its ownership.
	if err := validateListener(desc, file); err != nil {
		return nil, err
	}
	k, err := claimKeeper(desc, file)
	if err != nil {
		// Returning a nil *keeper through Keeper creates a nonnil interface.
		// Refused alias adoption must return an actual nil owner for salvage.
		return nil, err
	}
	return k, nil
}

func captureListener(ln net.Listener, id string) (*os.File, Descriptor, error) {
	if ln == nil || !validListenerID(id) {
		return nil, Descriptor{}, ErrIdentity
	}
	// Wrappers must expose their native listener explicitly. Capturing a TLS
	// listener would otherwise discard protocol state owned by the control root.
	var source syscall.Conn
	switch native := ln.(type) {
	case *net.TCPListener:
		if native == nil {
			return nil, Descriptor{}, ErrIdentity
		}
		source = native
	case *net.UnixListener:
		if native == nil {
			return nil, Descriptor{}, ErrIdentity
		}
		source = native
	default:
		return nil, Descriptor{}, ErrIdentity
	}
	raw, err := source.SyscallConn()
	if err != nil {
		return nil, Descriptor{}, err
	}
	var fd int
	var op error
	// The new FD has its own CLOEXEC bit, but status flags and backlog belong
	// to the original OFD. Duplicating inside RawConn.Control also excludes
	// native Close reusing the numeric descriptor while we duplicate it.
	err = raw.Control(func(sourceFD uintptr) {
		fd, op = unix.FcntlInt(sourceFD, unix.F_DUPFD_CLOEXEC, 0)
	})
	if err != nil || op != nil {
		return nil, Descriptor{}, errors.Join(err, op)
	}
	// SyscallConn duplication keeps the shared OFD nonblocking. Listener.File
	// and File.Fd are forbidden here because they can change that shared flag.
	file := os.NewFile(uintptr(fd), "listener-keeper")
	desc, err := inspectListener(file, id)
	if err != nil {
		// Only our duplicate is owned on this failure path. Closing the native
		// listener would turn pure capture refusal into a business interruption.
		_ = file.Close()
		return nil, Descriptor{}, err
	}
	return file, desc, nil
}

func validListenerID(id string) bool {
	// Match handoff's resource-name limit and the S6 closed ID alphabet. Paths
	// and addresses have separate fields; an ID cannot smuggle either syntax.
	if len(id) == 0 || len(id) > 256 {
		return false
	}
	for _, c := range id {
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' ||
			c >= '0' && c <= '9' || strings.ContainsRune("._-", c) {
			continue
		}
		return false
	}
	return true
}

// Inspection is read-only and listener-specific. Raw sockets require the
// opposite SO_ACCEPTCONN predicate; sharing their validator would admit the
// wrong responsibility. Never read SO_ERROR or repair status/descriptor flags.
// origin: hot-update S6 plan §6.1 / T18
func inspectListener(file *os.File, id string) (Descriptor, error) {
	if file == nil || !validListenerID(id) {
		return Descriptor{}, ErrIdentity
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return Descriptor{}, ErrIdentity
	}
	desc := Descriptor{ID: id}
	// No numeric FD is stored in the descriptor. FD numbers change when
	// Prepared duplicates the resource, while socket dev/inode/cookie and the
	// local listening address remain the same across same-PID image replacement.
	// Every field is re-derived here rather than copied from untrusted metadata.
	var op error
	err = raw.Control(func(fd uintptr) {
		var stat unix.Stat_t
		// File type comes from Fstat, not the descriptor's SockType claim. A
		// regular file or pipe must never reach net.FileListener's duplication.
		if unix.Fstat(int(fd), &stat) != nil || stat.Mode&unix.S_IFMT != unix.S_IFSOCK {
			op = ErrIdentity
			return
		}
		flags, e := unix.FcntlInt(fd, unix.F_GETFL, 0)
		// A blocking inherited OFD could park Accept beyond the cut deadline.
		// Repairing it would mutate all prepared/salvage aliases, so refuse it.
		if e != nil || flags&unix.O_NONBLOCK == 0 || flags&unix.O_ACCMODE != unix.O_RDWR {
			op = ErrIdentity
			return
		}
		fdFlags, e := unix.FcntlInt(fd, unix.F_GETFD, 0)
		// Restore makes inherited descriptors CLOEXEC before transferring them.
		// Accepting an inheritable original would leak an unlisted FD on exec.
		if e != nil || fdFlags&unix.FD_CLOEXEC == 0 {
			op = ErrIdentity
			return
		}
		typ, e := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		// Unix seqpacket listeners also LISTEN, but their message boundaries
		// do not satisfy the TCP/Unix stream contract used by the upper owners.
		if e != nil || typ != unix.SOCK_STREAM {
			op = ErrIdentity
			return
		}
		listening, e := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
		if e != nil || listening != 1 {
			op = ErrIdentity
			return
		}
		family, e := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_DOMAIN)
		if e != nil {
			op = ErrIdentity
			return
		}
		cookie, e := unix.GetsockoptUint64(int(fd), unix.SOL_SOCKET, unix.SO_COOKIE)
		// Cookie strengthens dev/inode provenance and is shared by all socket
		// aliases. If the kernel cannot supply it, ownership is not provable.
		if e != nil || cookie == 0 {
			op = ErrIdentity
			return
		}
		addr, e := unix.Getsockname(int(fd))
		// Getsockname is non-destructive, unlike SO_ERROR. The kernel's bound
		// local endpoint must exactly match the saved address and family.
		if e != nil {
			op = ErrIdentity
			return
		}
		desc.Device, desc.Inode, desc.Cookie = uint64(stat.Dev), stat.Ino, cookie
		desc.Family, desc.SockType = family, typ
		// These booleans are true only after the actual fcntl checks above. An
		// image claiming false is also rejected by exact descriptor equality;
		// the importer may not waive safety simply by describing unsafe flags.
		desc.Nonblock, desc.CloseOnExec = true, true
		switch address := addr.(type) {
		// Canonical network/address come from the native sockaddr. A metadata
		// string cannot reinterpret a TCP socket as Unix or change its family.
		case *unix.SockaddrInet4:
			if family != unix.AF_INET {
				op = ErrIdentity
				return
			}
			desc.Network = "tcp"
			desc.Address = (&net.TCPAddr{IP: net.IP(address.Addr[:]), Port: address.Port}).String()
		case *unix.SockaddrInet6:
			if family != unix.AF_INET6 {
				op = ErrIdentity
				return
			}
			desc.Network = "tcp"
			zone := ""
			if address.ZoneId != 0 {
				// Preserve the native zone name so scoped IPv6 identity is not
				// collapsed onto the same address bytes on another interface.
				iface, ie := net.InterfaceByIndex(int(address.ZoneId))
				if ie != nil {
					op = ErrIdentity
					return
				}
				zone = iface.Name
			}
			desc.Address = (&net.TCPAddr{IP: net.IP(address.Addr[:]), Port: address.Port, Zone: zone}).String()
		case *unix.SockaddrUnix:
			if family != unix.AF_UNIX || address.Name == "" {
				op = ErrIdentity
				return
			}
			desc.Network, desc.Address = "unix", address.Name
			if !strings.HasPrefix(address.Name, "@") {
				// The filesystem socket inode differs from the socket-FD inode.
				// Lstat also refuses a pathname symlink instead of following it
				// and later accidentally unlinking somebody else's link/socket.
				desc.Path = address.Name
				var pathStat unix.Stat_t
				if unix.Lstat(desc.Path, &pathStat) != nil || pathStat.Mode&unix.S_IFMT != unix.S_IFSOCK {
					op = ErrIdentity
					return
				}
				desc.UnixPathInode = pathStat.Ino
			}
		default:
			// A generic socket is not enough provenance for this leaf. Unknown
			// families have no keeper/view or pathname ownership contract here.
			op = ErrIdentity
		}
	})
	if err != nil || op != nil {
		return Descriptor{}, ErrIdentity
	}
	return desc, nil
}

func validateListener(want Descriptor, file *os.File) error {
	got, err := inspectListener(file, want.ID)
	// Whole-struct equality keeps every frozen identity field binding; adding
	// a field without capturing it cannot silently weaken an older projection.
	if err != nil || got != want {
		return ErrIdentity
	}
	return nil
}

func protectUnixPath(ln net.Listener) {
	// Closing either the cold source wrapper or a later Accept duplicate must
	// not remove the public route while the keeper continues to LISTEN. The
	// terminal owner's explicit inode check below is the only unlink authority.
	if native, ok := ln.(*net.UnixListener); ok {
		native.SetUnlinkOnClose(false)
	}
}

func unlinkListenerPath(desc Descriptor) error {
	// This is normal-stop cleanup only. Adoption and Resume never probe/remove
	// stale paths or bind a replacement: those cold-start operations would break
	// continuity and could steal the pathname from a separate new owner.
	if desc.Path == "" {
		return nil // TCP and abstract Unix listeners own no pathname.
	}
	var stat unix.Stat_t
	if err := unix.Lstat(desc.Path, &stat); err != nil {
		if errors.Is(err, unix.ENOENT) {
			return nil
		}
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFSOCK || stat.Ino != desc.UnixPathInode {
		// A renamed/deleted original can be replaced while a paused keeper
		// remains alive. The replacement's pathname is never ours to remove.
		return nil
	}
	return os.Remove(desc.Path)
}

func listenerAddr(desc Descriptor) net.Addr {
	if desc.Network == "unix" {
		return &net.UnixAddr{Net: "unix", Name: desc.Address}
	}
	// The descriptor was already kernel-validated; this is numeric and does no
	// DNS lookup when returning the address of an inactive imported view.
	addr, _ := net.ResolveTCPAddr("tcp", desc.Address)
	return addr
}

func listenerRemaining(deadline controlbudget.Deadline) (time.Duration, error) {
	// BOOTTIME survives same-PID exec and includes suspend. Wall-clock changes
	// cannot grant a fresh cut window or turn an expired pause into admission.
	var ts unix.Timespec
	if deadline <= 0 || unix.ClockGettime(unix.CLOCK_BOOTTIME, &ts) != nil {
		return 0, ErrDeadline
	}
	if int64(deadline) <= ts.Nano() {
		return 0, ErrDeadline
	}
	return time.Duration(int64(deadline) - ts.Nano()), nil
}
