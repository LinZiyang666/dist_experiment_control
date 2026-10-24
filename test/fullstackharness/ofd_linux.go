package fullstackharness

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"golang.org/x/sys/unix"
)

// OFDProof records a real KCMP_FILE result. Inode and cookie are supplementary
// diagnostics; neither makes two independently opened regular files one OFD.
type OFDProof struct {
	Owner                            contract.Stamp
	OriginalFD, CurrentFD, WitnessFD int
	Flags, SocketCookie              uint64
}

// OFDWitness owns only the explicit CLOEXEC duplicate and pidfd. The original
// descriptor remains borrowed and must still name the same OFD at each check
// within its image. A validated next cut binds a new descriptor to that OFD.
type OFDWitness struct {
	mu          sync.Mutex
	owner       contract.Stamp
	original    int
	bound       int
	file, pidfd *os.File
	cookie      uint64
}

// origin: hot-update S10 plan §4.2 / TestFullstackOFDWitnessRejectsReopen
func compareOFD(pid1, pid2, fd1, fd2 int) error {
	result, _, errno := unix.Syscall6(unix.SYS_KCMP, uintptr(pid1), uintptr(pid2), 0,
		uintptr(fd1), uintptr(fd2), 0) // KCMP_FILE = 0
	if errno != 0 {
		return fmt.Errorf("%w: kcmp: %w", ErrKernelUnsupported, errno)
	}
	if result != 0 {
		return ErrIdentityChanged
	}
	return nil
}

func NewOFDWitness(owner contract.Stamp, fd int) (_ *OFDWitness, err error) {
	if fd < 0 {
		return nil, errors.New("fullstack: negative original descriptor")
	}
	if err := checkKernelProcess(owner.Process); err != nil {
		return nil, err
	}
	pfd, err := unix.PidfdOpen(owner.Process.PID, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: pidfd: %w", ErrKernelUnsupported, err)
	}
	w := &OFDWitness{owner: owner, original: fd, bound: fd, pidfd: os.NewFile(uintptr(pfd), "fullstack-ofd-pidfd")}
	defer func() {
		if err != nil {
			err = errors.Join(err, w.Close())
		}
	}()
	if err := livePIDFD(pfd); err != nil {
		return nil, err
	}
	var dup int
	if owner.Process.PID == os.Getpid() {
		dup, err = unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	} else {
		dup, err = unix.PidfdGetfd(pfd, fd, 0)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: duplicate: %w", ErrKernelUnsupported, err)
	}
	w.file = os.NewFile(uintptr(dup), "fullstack-ofd-witness")
	if err := compareOFD(os.Getpid(), owner.Process.PID, dup, fd); err != nil {
		return nil, err
	}
	if err := checkKernelProcess(owner.Process); err != nil {
		return nil, err
	}
	// Getsockopt does not change blocking flags, unlike net.Conn.File().
	w.cookie, err = unix.GetsockoptUint64(dup, unix.SOL_SOCKET, unix.SO_COOKIE)
	if errors.Is(err, unix.ENOTSOCK) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	return w, nil
}

// ReadDescriptorFlags is read-only, including for a remote process. fdinfo's
// flags include O_CLOEXEC; callers must check that bit after activation rather
// than setting it on a borrowed product descriptor to repair missing evidence.
func ReadDescriptorFlags(pid, fd int) (uint64, error) {
	if pid <= 0 || fd < 0 {
		return 0, errors.New("fullstack: invalid descriptor identity")
	}
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/fdinfo/" + strconv.Itoa(fd))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "flags:") {
			return strconv.ParseUint(strings.TrimSpace(strings.TrimPrefix(line, "flags:")), 8, 64)
		}
	}
	return 0, errors.New("fullstack: missing descriptor flags")
}

func (w *OFDWitness) Check(ctx context.Context, current contract.Stamp, fd int) (OFDProof, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return OFDProof{}, err
	}
	if w.file == nil {
		return OFDProof{}, os.ErrClosed
	}
	if fd < 0 || current.Process != w.owner.Process {
		return OFDProof{}, ErrIdentityChanged
	}
	if err := checkKernelProcess(current.Process); err != nil {
		return OFDProof{}, err
	}
	if err := livePIDFD(int(w.pidfd.Fd())); err != nil {
		return OFDProof{}, err
	}
	dup := int(w.file.Fd())
	// Stamps come from the caller's validated resource census. A generation
	// change alone does not authorize migration: it must be a later cut with
	// a fresh observation. Sequence is local to an image and may reset on exec;
	// the OFD may also survive multiple cuts between these read-only checks.
	// KCMP still binds the supplied FD to the original OFD.
	// This reader proves custody, not the product's completion of that cut.
	// origin: S10 internal review rv-harness F6
	migrating := current.Generation != w.owner.Generation
	if current.Sequence == 0 || current.AtBootNS < w.owner.AtBootNS ||
		(!migrating && (current.CutID != w.owner.CutID || current.Sequence < w.owner.Sequence)) ||
		(migrating && (current.Generation <= w.owner.Generation ||
			current.CutID == "" || current.CutID == w.owner.CutID || current.AtBootNS <= w.owner.AtBootNS)) {
		return OFDProof{}, ErrIdentityChanged
	}
	// Checking both locations catches closing the original and keeping a dup:
	// proving that *some* reference survived is weaker than original custody.
	// origin: hot-update S10 plan §4.2 / wrong-close-leaves-dup
	// Prepared.Exec closes the old CLOEXEC input; only the handoff copy survives.
	// Thereafter the new binding is the one that must not disappear in-image.
	if !migrating {
		if err := compareOFD(os.Getpid(), w.owner.Process.PID, dup, w.bound); err != nil {
			return OFDProof{}, err
		}
	}
	if err := compareOFD(os.Getpid(), current.Process.PID, dup, fd); err != nil {
		return OFDProof{}, err
	}
	flags, err := ReadDescriptorFlags(current.Process.PID, fd)
	if err != nil {
		return OFDProof{}, err
	}
	if w.cookie != 0 {
		cookie, err := unix.GetsockoptUint64(dup, unix.SOL_SOCKET, unix.SO_COOKIE)
		if err != nil || cookie != w.cookie {
			return OFDProof{}, ErrIdentityChanged
		}
	}
	if err := checkKernelProcess(current.Process); err != nil {
		return OFDProof{}, err
	}
	if err := ctx.Err(); err != nil {
		return OFDProof{}, err
	}
	if migrating {
		w.bound = fd
	}
	w.owner = current
	return OFDProof{Owner: current, OriginalFD: w.original, CurrentFD: fd, WitnessFD: dup, Flags: flags, SocketCookie: w.cookie}, nil
}

func (w *OFDWitness) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var err error
	if w.file != nil {
		err = errors.Join(err, w.file.Close())
		w.file = nil
	}
	if w.pidfd != nil {
		err = errors.Join(err, w.pidfd.Close())
		w.pidfd = nil
	}
	return err
}
