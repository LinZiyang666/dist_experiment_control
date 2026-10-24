//go:build linux

package handoff

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
)

const requiredSeals = unix.F_SEAL_SEAL | unix.F_SEAL_SHRINK | unix.F_SEAL_GROW | unix.F_SEAL_WRITE

type fileImage struct {
	Name  string `json:"name"`
	FD    int    `json:"fd"`
	Dev   uint64 `json:"dev"`
	Ino   uint64 `json:"ino"`
	Mode  uint32 `json:"mode"`
	Rdev  uint64 `json:"rdev"`
	Flags int    `json:"flags"`
}

type resourceImage struct {
	ID    string          `json:"id"`
	Kind  string          `json:"kind"`
	Data  json.RawMessage `json:"data,omitempty"`
	Files []fileImage     `json:"files,omitempty"`
}

type manifest struct {
	Version   int             `json:"version"`
	PID       int             `json:"pid"`
	UID       int             `json:"uid"`
	Nonce     string          `json:"nonce"`
	State     State           `json:"state"`
	Resources []resourceImage `json:"resources,omitempty"`
}

// Prepared owns an immutable anonymous snapshot and CLOEXEC descriptor copies.
// Exec only returns on failure; Close never closes the caller's originals.
// This bundle is for a same-PID planned exec, not a child process or a crash
// recovery journal. A child must never be given its private environment entry.
type Prepared struct {
	mu         sync.Mutex
	manifestFD int
	fds        []int
	nonce      string
	closed     bool
}

// Prepare validates and duplicates a complete application-provided snapshot.
// It deliberately uses RawConn.Control rather than File.Fd or TCPConn.File:
// Go's File conversions can change O_NONBLOCK on the shared open description,
// which would silently change the original live connection's deadline behavior.
func Prepare(state State, resources []Resource) (*Prepared, error) {
	if err := validateState(state); err != nil {
		return nil, err
	}
	if len(resources) > maxResources {
		return nil, fmt.Errorf("handoff: too many resources")
	}
	// Broker resources have exactly one terminal owner per OFD.
	// origin: hot-update S6 plan §4.1 / T27
	if state.Role == "broker" {
		if err := ValidateResourceOwners(resources); err != nil {
			return nil, err
		}
	}
	var random [32]byte
	if _, err := rand.Read(random[:]); err != nil {
		return nil, fmt.Errorf("handoff: snapshot identity: %w", err)
	}
	p := &Prepared{manifestFD: -1, nonce: hex.EncodeToString(random[:])}
	m := manifest{Version: formatVersion, PID: os.Getpid(), UID: os.Geteuid(), Nonce: p.nonce, State: state}
	ids := make(map[string]bool, len(resources))
	// Count before duplicating, including metadata size, so invalid large
	// inputs cannot consume an unbounded number of descriptors first.
	count, dataBytes := 0, len(state.Data)
	for _, r := range resources {
		count += len(r.Files)
		if !validName(r.ID, 256) || !validName(r.Kind, 64) || ids[r.ID] || count > maxFiles {
			return nil, fmt.Errorf("handoff: invalid or duplicate resource or file limit exceeded")
		}
		ids[r.ID] = true
		if err := validateData(r.Data); err != nil {
			return nil, err
		}
		dataBytes += len(r.Data)
		if dataBytes > maxStateBytes {
			return nil, fmt.Errorf("handoff: snapshot too large")
		}
		for name, f := range r.Files {
			if !validName(name, 64) || f == nil {
				return nil, fmt.Errorf("handoff: invalid file name or nil file")
			}
		}
	}
	for _, r := range resources {
		ri := resourceImage{ID: r.ID, Kind: r.Kind, Data: r.Data}
		names := make([]string, 0, len(r.Files))
		for name := range r.Files {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fi, err := duplicateFile(name, r.Files[name])
			if err != nil {
				_ = p.Close()
				return nil, err
			}
			p.fds = append(p.fds, fi.FD)
			ri.Files = append(ri.Files, fi)
		}
		m.Resources = append(m.Resources, ri)
	}
	blob, err := json.Marshal(m)
	if err != nil {
		_ = p.Close()
		return nil, fmt.Errorf("handoff: encode snapshot: %w", err)
	}
	if len(blob) > maxStateBytes {
		_ = p.Close()
		return nil, fmt.Errorf("handoff: encoded snapshot too large")
	}
	p.manifestFD, err = sealedSnapshot(blob)
	if err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func duplicateFile(name string, file *os.File) (fileImage, error) {
	raw, err := file.SyscallConn()
	if err != nil {
		return fileImage{}, fmt.Errorf("handoff: access file: %w", err)
	}
	fd := -1
	var duplicateErr error
	if err := raw.Control(func(original uintptr) {
		flags, err := unix.FcntlInt(original, unix.F_GETFD, 0)
		if err != nil {
			duplicateErr = err
			return
		}
		if flags&unix.FD_CLOEXEC == 0 {
			duplicateErr = fmt.Errorf("input descriptor must have close-on-exec protection")
			return
		}
		fd, duplicateErr = unix.FcntlInt(original, unix.F_DUPFD_CLOEXEC, 3)
	}); err != nil {
		return fileImage{}, fmt.Errorf("handoff: control file: %w", err)
	}
	if duplicateErr != nil {
		return fileImage{}, fmt.Errorf("handoff: duplicate file: %w", duplicateErr)
	}
	fi, err := describeFile(name, fd)
	if err != nil {
		_ = unix.Close(fd)
		return fileImage{}, err
	}
	return fi, nil
}

func describeFile(name string, fd int) (fileImage, error) {
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil {
		return fileImage{}, fmt.Errorf("handoff: file identity: %w", err)
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return fileImage{}, fmt.Errorf("handoff: file status: %w", err)
	}
	return fileImage{Name: name, FD: fd, Dev: st.Dev, Ino: st.Ino, Mode: st.Mode, Rdev: st.Rdev, Flags: flags}, nil
}

func sealedSnapshot(blob []byte) (int, error) {
	fd, err := unix.MemfdCreate("tether-handoff", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
	if err != nil {
		return -1, fmt.Errorf("handoff: create snapshot: %w", err)
	}
	for offset := 0; offset < len(blob); {
		n, err := unix.Pwrite(fd, blob[offset:], int64(offset))
		if err == unix.EINTR {
			continue
		}
		if err != nil || n == 0 {
			_ = unix.Close(fd)
			return -1, fmt.Errorf("handoff: write snapshot: %w", errors.Join(err, io.ErrShortWrite))
		}
		offset += n
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_ADD_SEALS, requiredSeals); err != nil {
		_ = unix.Close(fd)
		return -1, fmt.Errorf("handoff: seal snapshot: %w", err)
	}
	return fd, nil
}

// Exec replaces this process image, passing only the bundle's explicit copies
// through exec. Every other descriptor retains its existing CLOEXEC policy.
// The application must have reached its I/O and child-reaping safe point first.
// ForkLock closes the Go os/exec fork window; it does not freeze application I/O
// and does not protect raw or foreign-runtime forks. CGO is disabled in tether.
// A nil env inherits the current environment; an empty non-nil env stays empty.
func (p *Prepared) Exec(path string, args, env []string) (result error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || path == "" || len(args) == 0 {
		return fmt.Errorf("handoff: closed bundle or invalid executable")
	}
	if env == nil {
		env = os.Environ()
	}
	env = append(CleanEnvironment(env), environmentKey+"="+strconv.Itoa(p.manifestFD)+":"+p.nonce)
	// Allocate everything before opening the inheritance window. The deferred
	// closure restores flags even if a later F_SETFD or exec itself fails.
	fds := append(append(make([]int, 0, len(p.fds)+1), p.fds...), p.manifestFD)
	// origin: S8 internal review activation-and-route-migration F1
	custody, err := observationExecDescriptors()
	if err != nil {
		return err
	}
	fds = append(fds, custody...)
	env = observationExecEnvironment(env)
	syscall.ForkLock.Lock()
	defer syscall.ForkLock.Unlock()
	// origin: hot-update S6 plan §4.1 / T27. A failed flag repair is a
	// failed handover even when exec already failed for another reason.
	defer func() { result = errors.Join(result, restoreExecFlags(fds)) }()
	for _, fd := range fds {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, 0); err != nil {
			return fmt.Errorf("handoff: allow descriptor inheritance: %w", err)
		}
	}
	// Scan after opening the window, under ForkLock: a manifest-only check
	// cannot see a foreign inheritable FD that would survive the same exec.
	// origin: hot-update S6 plan §4.1 / T27
	if err := validateInheritance(fds, true); err != nil {
		return err
	}
	return fmt.Errorf("handoff: exec: %w", syscall.Exec(path, args, env))
}

// Close releases the prepared descriptor copies and anonymous state.
func (p *Prepared) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	var errs []error
	for _, fd := range p.fds {
		errs = append(errs, unix.Close(fd))
	}
	if p.manifestFD >= 0 {
		errs = append(errs, unix.Close(p.manifestFD))
	}
	p.fds = nil
	p.manifestFD = -1
	return errors.Join(errs...)
}

// Restore consumes a handoff from the environment. No variable means ordinary
// startup and returns (nil, nil); malformed or unverifiable handoffs are errors.
// Call this before starting any application goroutine or subprocess. A restore
// error must abort startup, not fall back to an empty resource table: doing so
// would lose ownership of live business resources and could duplicate work.
func Restore() (*Restored, error) {
	value, present := os.LookupEnv(environmentKey)
	if !present {
		return nil, nil
	}
	if err := os.Unsetenv(environmentKey); err != nil {
		return nil, fmt.Errorf("handoff: consume dispatch: %w", err)
	}
	fd, nonce, err := parseDispatch(value)
	if err != nil {
		return nil, err
	}
	return restoreDescriptor(fd, nonce)
}

// Repack prepares a re-dispatch of this bundle to another image — the
// previous binary when the one that received it cannot activate it — under
// the SAME State and with every file the receiver has not taken. The
// receiver's own taken files travel through the caller's extra resources
// (MergeResources). Ownership is borrowed, never transferred: an exec that
// fails leaves this bundle exactly as usable as before.
func (r *Restored) Repack(extra ...Resource) (*Prepared, error) {
	if r == nil {
		return nil, fmt.Errorf("handoff: no bundle to repack")
	}
	resources, err := MergeResources(r.Borrow(), extra)
	if err != nil {
		return nil, err
	}
	return Prepare(r.State, resources)
}

func restoreDescriptor(fd int, nonce string) (*Restored, error) {
	// Do not take ownership of an arbitrary fd named by a malformed env. The
	// sealed anonymous snapshot must first prove it is the dispatched package.
	seals, err := unix.FcntlInt(uintptr(fd), unix.F_GET_SEALS, 0)
	if err != nil || seals&requiredSeals != requiredSeals {
		return nil, fmt.Errorf("handoff: snapshot is not immutable")
	}
	if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		return nil, fmt.Errorf("handoff: protect snapshot: %w", err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(fd, &st); err != nil || st.Size <= 0 || st.Size > maxStateBytes {
		return nil, fmt.Errorf("handoff: invalid snapshot length")
	}
	blob := make([]byte, int(st.Size))
	for offset := 0; offset < len(blob); {
		n, err := unix.Pread(fd, blob[offset:], int64(offset))
		if err == unix.EINTR {
			continue
		}
		if err != nil || n == 0 {
			return nil, fmt.Errorf("handoff: read snapshot: %w", errors.Join(err, io.ErrUnexpectedEOF))
		}
		offset += n
	}
	var m manifest
	dec := json.NewDecoder(bytes.NewReader(blob))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("handoff: decode snapshot: %w", err)
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("handoff: trailing snapshot data")
	}
	if m.Nonce != nonce || m.PID != os.Getpid() || m.UID != os.Geteuid() {
		return nil, fmt.Errorf("handoff: snapshot owner mismatch")
	}
	// The fd is now established as our package, so it is consumed on all later
	// paths. Resource fds are only owned after the entire manifest is validated.
	defer func() { _ = unix.Close(fd) }()
	if m.Version != formatVersion {
		return nil, fmt.Errorf("handoff: unsupported snapshot version %d", m.Version)
	}
	if err := validateState(m.State); err != nil {
		return nil, err
	}
	if err := validateResources(m.Resources, fd); err != nil {
		return nil, err
	}
	allowed := []int{fd}
	for _, resource := range m.Resources {
		for _, file := range resource.Files {
			allowed = append(allowed, file.FD)
		}
	}
	if err := validateInheritance(allowed, false); err != nil {
		return nil, err
	}
	// No descriptor becomes an os.File (or can be transferred to a caller)
	// before every imported descriptor has its CLOEXEC protection restored.
	for _, ri := range m.Resources {
		for _, fi := range ri.Files {
			if _, err := unix.FcntlInt(uintptr(fi.FD), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
				closeImages(m.Resources)
				return nil, fmt.Errorf("handoff: protect imported file: %w", err)
			}
		}
	}
	r := &Restored{State: m.State, files: make(map[string]map[string]*os.File, len(m.Resources))}
	for _, ri := range m.Resources {
		r.Resources = append(r.Resources, Resource{ID: ri.ID, Kind: ri.Kind, Data: ri.Data})
		files := make(map[string]*os.File, len(ri.Files))
		for _, fi := range ri.Files {
			files[fi.Name] = os.NewFile(uintptr(fi.FD), "handoff-"+ri.Kind+"-"+fi.Name)
		}
		r.files[ri.ID] = files
	}
	if m.State.Role == "broker" {
		if err := ValidateResourceOwners(r.Borrow()); err != nil {
			_ = r.Close()
			return nil, err
		}
	}
	return r, nil
}

func validateResources(resources []resourceImage, snapshotFD int) error {
	if len(resources) > maxResources {
		return fmt.Errorf("handoff: resource limit exceeded")
	}
	ids, fds := make(map[string]bool), map[int]bool{snapshotFD: true}
	for _, ri := range resources {
		if !validName(ri.ID, 256) || !validName(ri.Kind, 64) || ids[ri.ID] {
			return fmt.Errorf("handoff: invalid or duplicate resource")
		}
		ids[ri.ID] = true
		if err := validateData(ri.Data); err != nil {
			return err
		}
		names := make(map[string]bool)
		for _, fi := range ri.Files {
			if !validName(fi.Name, 64) || names[fi.Name] || fi.FD < 3 || fds[fi.FD] || len(fds) > maxFiles {
				return fmt.Errorf("handoff: invalid, duplicate or excessive file descriptors")
			}
			names[fi.Name], fds[fi.FD] = true, true
			actual, err := describeFile(fi.Name, fi.FD)
			if err != nil {
				return err
			}
			if actual != fi {
				return fmt.Errorf("handoff: file identity or status changed")
			}
		}
	}
	return nil
}

func closeImages(resources []resourceImage) {
	for _, ri := range resources {
		for _, fi := range ri.Files {
			_ = unix.Close(fi.FD)
		}
	}
}

// restoreExecFlags reports every repair failure; skipping one permits secrets or
// live sockets to escape through a later unrelated child after a failed exec.
func restoreExecFlags(fds []int) error {
	var result error
	for _, fd := range fds {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
			result = errors.Join(result, fmt.Errorf("handoff: restore close-on-exec: %w", err))
		}
	}
	return result
}

// validateInheritance examines actual descriptors, including ones absent from
// the manifest. Restore also runs in-process in tests, so it permits declared
// descriptors already protected; Exec requires exact equality.
// origin: hot-update S6 plan §4.1 / T27
func validateInheritance(fds []int, exact bool) error {
	allowed := make(map[int]bool, len(fds))
	for _, fd := range fds {
		allowed[fd] = true
	}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return fmt.Errorf("handoff: descriptor census: %w", err)
	}
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil || fd < 3 {
			continue
		}
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if errors.Is(err, unix.EBADF) {
			continue
		} // ReadDir's own closed FD.
		if err != nil {
			return fmt.Errorf("handoff: descriptor flags: %w", err)
		}
		if flags&unix.FD_CLOEXEC != 0 {
			continue
		}
		if !allowed[fd] {
			return errors.New("handoff: unexpected inheritable descriptor")
		}
		delete(allowed, fd)
	}
	if exact && len(allowed) != 0 {
		return errors.New("handoff: incomplete inheritance whitelist")
	}
	return nil
}

// ValidateResourceOwners refuses distinct terminal owners for indistinguishable
// OFDs. Socket cookies and dev/inode/access-mode identify socket/pipe aliases;
// regular-file aliases are refused conservatively even if independently opened.
// It never reads bytes, changes offsets, consumes SO_ERROR, or repairs flags.
// Intentional prepared aliases are private to Prepared, outside this table.
// origin: hot-update S6 plan §4.1 / T27
func ValidateResourceOwners(resources []Resource) error {
	type identity struct {
		dev, ino, rdev, cookie uint64
		mode                   uint32
		access                 int
	}
	seen := make(map[identity]bool)
	ids := make(map[string]bool)
	count := 0
	for _, resource := range resources {
		if !validName(resource.ID, 256) || !validName(resource.Kind, 64) || ids[resource.ID] {
			return errors.New("handoff: invalid resource owner")
		}
		ids[resource.ID] = true
		for name, file := range resource.Files {
			count++
			if count > maxFiles || !validName(name, 64) || file == nil {
				return errors.New("handoff: invalid owned file")
			}
			raw, err := file.SyscallConn()
			if err != nil {
				return errors.New("handoff: owned file unavailable")
			}
			var key identity
			var checkErr error
			err = raw.Control(func(fd uintptr) {
				var st unix.Stat_t
				if unix.Fstat(int(fd), &st) != nil {
					checkErr = errors.New("handoff: owned file identity unavailable")
					return
				}
				flags, e1 := unix.FcntlInt(fd, unix.F_GETFL, 0)
				fdFlags, e2 := unix.FcntlInt(fd, unix.F_GETFD, 0)
				if e1 != nil || e2 != nil || fdFlags&unix.FD_CLOEXEC == 0 {
					checkErr = errors.New("handoff: owned file flags invalid")
					return
				}
				key = identity{dev: st.Dev, ino: st.Ino, rdev: st.Rdev, mode: st.Mode & unix.S_IFMT, access: flags & unix.O_ACCMODE}
				if key.mode == unix.S_IFSOCK {
					key.cookie, checkErr = unix.GetsockoptUint64(int(fd), unix.SOL_SOCKET, unix.SO_COOKIE)
				}
			})
			if err != nil || checkErr != nil {
				return errors.New("handoff: owned file identity invalid")
			}
			if seen[key] {
				return errors.New("handoff: aliased terminal owners")
			}
			seen[key] = true
		}
	}
	if len(resources) > maxResources {
		return errors.New("handoff: resource limit exceeded")
	}
	return nil
}

// ResourceFileIdentity records immutable provenance without serializing a local
// descriptor number, which changes across Prepare. Shared status flags are part
// of the proof; callers cannot silently normalize nonblocking mode on import.
// origin: hot-update S6 plan §4.1 / T27
func ResourceFileIdentity(file *os.File) (string, error) {
	if file == nil {
		return "", errors.New("handoff: missing owned file")
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return "", errors.New("handoff: owned file unavailable")
	}
	var result string
	var checkErr error
	err = raw.Control(func(fd uintptr) {
		info, e := describeFile("file", int(fd))
		if e != nil {
			checkErr = e
			return
		}
		flags, e := unix.FcntlInt(fd, unix.F_GETFD, 0)
		if e != nil || flags&unix.FD_CLOEXEC == 0 {
			checkErr = errors.New("handoff: owned file unprotected")
			return
		}
		var cookie uint64
		if info.Mode&unix.S_IFMT == unix.S_IFSOCK {
			cookie, e = unix.GetsockoptUint64(int(fd), unix.SOL_SOCKET, unix.SO_COOKIE)
			if e != nil {
				checkErr = e
				return
			}
		}
		result = fmt.Sprintf("%d.%d.%d.%d.%d.%d", info.Dev, info.Ino, info.Mode, info.Rdev, info.Flags, cookie)
	})
	if err != nil || checkErr != nil {
		return "", errors.New("handoff: owned file identity unavailable")
	}
	return result, nil
}

// ValidateListenerFile checks the original passive socket without constructing
// an Accept view or consuming socket errors. Adopt must still validate its full
// pathname/address; this guard prevents treating an active socket as a listener.
// origin: hot-update S6 plan §4.1 / T27
func ValidateListenerFile(file *os.File, device, inode, cookie uint64, family, sockType int, nonblock bool) error {
	if file == nil {
		return errors.New("handoff: missing listener")
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return errors.New("handoff: unavailable listener")
	}
	var checkErr error
	err = raw.Control(func(fd uintptr) {
		var st unix.Stat_t
		if unix.Fstat(int(fd), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Dev != device || st.Ino != inode {
			checkErr = errors.New("handoff: listener identity mismatch")
			return
		}
		actualCookie, e1 := unix.GetsockoptUint64(int(fd), unix.SOL_SOCKET, unix.SO_COOKIE)
		actualFamily, e2 := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_DOMAIN)
		actualType, e3 := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_TYPE)
		accepting, e4 := unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_ACCEPTCONN)
		flags, e5 := unix.FcntlInt(fd, unix.F_GETFL, 0)
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || actualCookie != cookie || cookie == 0 || actualFamily != family || actualType != sockType || accepting != 1 || (flags&unix.O_NONBLOCK != 0) != nonblock {
			checkErr = errors.New("handoff: listener socket mismatch")
		}
	})
	if err != nil || checkErr != nil {
		return errors.New("handoff: invalid listener")
	}
	return nil
}
