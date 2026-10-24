package upgradelock

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/LinZiyang666/tether/internal/upgradetx"
	"golang.org/x/sys/unix"
)

// The declaration in the locked inode binds an inherited descriptor to its
// reservation. It is evidence only: adoption also verifies the kernel's flock
// on that exact OFD. A pathname or identical bytes cannot prove custody.
type descriptor struct {
	Version     uint32        `json:"version"`
	Key         upgradetx.Key `json:"key"`
	ParentToken string        `json:"parent_token"`
}

type lease struct {
	file        *os.File
	domain      *domain
	key         upgradetx.Key
	parentToken string
}

// All wrapper custody changes share one mutex. There are no keeper goroutines;
// an inherited or borrowed duplicate keeps the kernel reservation alive.
var leases = struct {
	sync.Mutex
	owners map[string]*lease
	fds    map[uintptr]*lease
}{owners: make(map[string]*lease), fds: make(map[uintptr]*lease)}

// Acquire refuses an independent contender rather than queueing behind an
// upgrade. A registered child with the live parent's token borrows a dup of
// the original OFD; it never opens and flocks that same inode again.
// origin: hot-update S8 plan §5.1
func Acquire(ctx context.Context, key upgradetx.Key, parentToken string) (Lease, error) {
	if ctx == nil {
		return nil, refusal(upgradetx.CodeRejected, "acquisition requires a context")
	}
	if ctx.Err() != nil {
		return nil, refusal(upgradetx.CodeDeferred, "domain acquisition cancelled")
	}
	d, err := resolveDomain(key)
	if err != nil {
		return nil, err
	}
	if parentToken != "" && !boundedIdentity(parentToken) {
		return nil, refusal(upgradetx.CodeRejected, "invalid parent reservation token")
	}
	leases.Lock()
	defer leases.Unlock()
	if owner := leases.owners[d.id]; owner != nil {
		if parentToken == "" || owner.parentToken != parentToken {
			return nil, refusal(upgradetx.CodeDeferred, "domain reservation is held")
		}
		actual, statErr := owner.file.Stat()
		named, nameErr := os.Lstat(filepath.Join(d.registration.ControlDir, d.id+".lease"))
		if statErr != nil || nameErr != nil || !named.Mode().IsRegular() || !os.SameFile(actual, named) {
			return nil, refusal(upgradetx.CodeRejected, "parent reservation inode changed")
		}
		fd, err := unix.FcntlInt(owner.file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
		if err != nil {
			return nil, refusal(upgradetx.CodeDeferred, "domain duplicate is unavailable")
		}
		l := &lease{file: os.NewFile(uintptr(fd), "upgrade-domain-lease"), domain: d, key: key, parentToken: parentToken}
		leases.fds[l.file.Fd()] = l
		return l, nil
	}
	f, err := openLock(d, ".lease")
	if err != nil {
		return nil, err
	}
	if err := flock(f, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, refusal(upgradetx.CodeDeferred, "domain reservation is held or unavailable")
	}
	encoded, err := json.Marshal(descriptor{Version: 1, Key: key, ParentToken: parentToken})
	if err == nil {
		err = f.Truncate(0)
	}
	if err == nil {
		_, err = f.WriteAt(encoded, 0)
	}
	if err == nil {
		err = f.Sync()
	}
	if err != nil || ctx.Err() != nil {
		_ = f.Close()
		return nil, refusal(upgradetx.CodeDeferred, "reservation declaration is not durable or acquisition cancelled")
	}
	l := &lease{file: f, domain: d, key: key, parentToken: parentToken}
	leases.owners[d.id], leases.fds[f.Fd()] = l, l
	return l, nil
}

// Adopt takes caller custody only after every check succeeds. It neither opens
// the lease again nor re-flocks it, and refusal leaves the FD for salvage.
// The handoff importer must Take this FD before closing auxiliary resources.
// The caller transfers a raw descriptor and must retire any prior os.File
// wrapper; a second live wrapper's finalizer could otherwise close our custody.
// origin: hot-update S8 plan §5.1 / M08
func Adopt(fd uintptr, key upgradetx.Key, parentToken string) (Lease, error) {
	d, err := resolveDomain(key)
	if err != nil {
		return nil, err
	}
	leases.Lock()
	defer leases.Unlock()
	if leases.fds[fd] != nil || fd > uintptr(^uint(0)>>1) {
		return nil, refusal(upgradetx.CodeRejected, "descriptor already owned or invalid")
	}
	var actual unix.Stat_t
	if err := unix.Fstat(int(fd), &actual); err != nil {
		return nil, refusal(upgradetx.CodeRejected, "inherited descriptor is unavailable")
	}
	named, err := os.Lstat(filepath.Join(d.registration.ControlDir, d.id+".lease"))
	if err != nil || !named.Mode().IsRegular() {
		return nil, refusal(upgradetx.CodeRejected, "inherited reservation pathname is invalid")
	}
	st, ok := named.Sys().(*syscall.Stat_t)
	if !ok || uint64(actual.Dev) != uint64(st.Dev) || actual.Ino != st.Ino || actual.Nlink != 1 || actual.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, refusal(upgradetx.CodeRejected, "inherited reservation identity mismatch")
	}
	flags, flagErr := unix.FcntlInt(fd, unix.F_GETFD, 0)
	status, statusErr := unix.FcntlInt(fd, unix.F_GETFL, 0)
	if flagErr != nil || statusErr != nil || flags&unix.FD_CLOEXEC == 0 || status&unix.O_ACCMODE != unix.O_RDWR {
		return nil, refusal(upgradetx.CodeRejected, "inherited reservation flags mismatch")
	}
	if err := requireHeldOFD(fd); err != nil {
		return nil, err
	}
	if actual.Size <= 0 || actual.Size > 8192 {
		return nil, refusal(upgradetx.CodeRejected, "inherited reservation declaration is invalid")
	}
	data := make([]byte, actual.Size)
	n, readErr := unix.Pread(int(fd), data, 0)
	var declaration descriptor
	if readErr != nil || n != len(data) || json.Unmarshal(data, &declaration) != nil || declaration.Version != 1 || declaration.Key.DomainID != key.DomainID || declaration.ParentToken != parentToken {
		return nil, refusal(upgradetx.CodeRejected, "inherited reservation declaration mismatch")
	}
	canonical, marshalErr := json.Marshal(declaration)
	if marshalErr != nil || string(canonical) != string(data) {
		return nil, refusal(upgradetx.CodeRejected, "inherited reservation declaration is not exact")
	}
	// Different registered siblings may borrow only a nonempty parent token.
	// A standalone lease cannot become an unrelated component's reservation.
	if _, err := resolveDomain(declaration.Key); err != nil || (declaration.Key != key && parentToken == "") {
		return nil, refusal(upgradetx.CodeRejected, "inherited reservation component mismatch")
	}
	l := &lease{file: os.NewFile(fd, "upgrade-domain-lease"), domain: d, key: key, parentToken: parentToken}
	leases.fds[fd] = l
	if declaration.Key == key {
		leases.owners[d.id] = l
	}
	return l, nil
}

func (l *lease) FD() uintptr {
	leases.Lock()
	defer leases.Unlock()
	if l.file == nil {
		return ^uintptr(0)
	}
	return l.file.Fd()
}

func (l *lease) DomainID() string { return l.domain.id }

func (l *lease) Close() error {
	leases.Lock()
	defer leases.Unlock()
	if l.file == nil {
		return nil
	}
	// LOCK_UN would also unlock all inherited/borrowed duplicates of this
	// OFD. Closing only our descriptor lets the kernel release the reservation
	// at the last custody holder, including on process death.
	// origin: hot-update S8 plan §5.1 / M08
	fd := l.file.Fd()
	err := l.file.Close()
	l.file = nil
	delete(leases.fds, fd)
	if leases.owners[l.domain.id] == l {
		delete(leases.owners, l.domain.id)
	}
	if err != nil {
		return refusal(upgradetx.CodeUnsettled, "reservation descriptor close failed")
	}
	return nil
}

// WithGuard is deliberately separate from the long reservation. The callback
// is a local RMW operation; callers must not wait for network/exec/children here.
// origin: hot-update S8 plan §5.1
func WithGuard(key upgradetx.Key, fn func() error) error {
	if fn == nil {
		return refusal(upgradetx.CodeRejected, "journal guard requires an operation")
	}
	d, err := resolveDomain(key)
	if err != nil {
		return err
	}
	f, err := openLock(d, ".guard")
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	if err := flock(f, unix.LOCK_EX); err != nil {
		return refusal(upgradetx.CodeDeferred, "journal guard is unavailable")
	}
	return fn()
}

func openLock(d *domain, suffix string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(d.registration.ControlDir, d.id+suffix), os.O_RDWR|os.O_CREATE|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0o600)
	if err != nil {
		return nil, refusal(upgradetx.CodeDeferred, "domain lock is not writable")
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, refusal(upgradetx.CodeRejected, "domain lock is not a regular file")
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	other := ".guard"
	if suffix == ".guard" {
		other = ".lease"
	}
	named, nameErr := os.Lstat(filepath.Join(d.registration.ControlDir, d.id+suffix))
	peer, peerErr := os.Lstat(filepath.Join(d.registration.ControlDir, d.id+other))
	// Link-count and identity checks stop a planted hard link from merging the
	// two lock domains, or a rename from admitting two independent contenders.
	// origin: hot-update S8 plan §5.1
	if !ok || st.Nlink != 1 || nameErr != nil || !os.SameFile(info, named) || (peerErr == nil && os.SameFile(info, peer)) || (peerErr != nil && !os.IsNotExist(peerErr)) {
		_ = f.Close()
		return nil, refusal(upgradetx.CodeRejected, "domain lock inode is aliased or replaced")
	}
	return f, nil
}

func flock(f *os.File, flags int) error {
	for {
		err := unix.Flock(int(f.Fd()), flags)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}
