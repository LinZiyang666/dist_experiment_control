//go:build linux

package infralaunch

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

type Lock struct {
	file, directory *os.File
	store           infracontract.Store
	name            string
}

func supported() error {
	if runtime.GOARCH != "amd64" {
		return infracontract.ErrUnsupported
	}
	return nil
}

// StoreIdentity reads actual kernel identity. MountID is retained as evidence,
// but the permanent filename uses dev/ino so bind aliases share one lock domain.
// origin: hot-update S9 plan §4.1 / T15
func StoreIdentity(directory *os.File) (infracontract.Store, error) {
	if directory == nil {
		return infracontract.Store{}, ErrLaunchRejected
	}
	var stat unix.Statx_t
	if err := unix.Statx(int(directory.Fd()), "", unix.AT_EMPTY_PATH, unix.STATX_INO|unix.STATX_MNT_ID, &stat); err != nil {
		return infracontract.Store{}, infracontract.ErrUnsupported
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Mask&(unix.STATX_INO|unix.STATX_MNT_ID) != unix.STATX_INO|unix.STATX_MNT_ID || stat.Ino == 0 || stat.Mnt_id == 0 {
		return infracontract.Store{}, infracontract.ErrIncomplete
	}
	return infracontract.Store{Device: unix.Mkdev(stat.Dev_major, stat.Dev_minor), Inode: stat.Ino, MountID: stat.Mnt_id}, nil
}

func lockName(s infracontract.Store) string { return fmt.Sprintf("%x-%x.lock", s.Device, s.Inode) }

func privateDirectory(f *os.File) error {
	if f == nil {
		return ErrLaunchRejected
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFDIR || (st.Uid != 0 && st.Uid != uint32(os.Geteuid())) || st.Mode&0022 != 0 {
		return ErrLaunchRejected
	}
	return nil
}

// AcquireStore never removes the inode, including on an unsuccessful lock.
// The root bootstrap may acquire this guard while the legacy owner is alive;
// only the independent census in LaunchClaim permits passing it to a server.
// origin: hot-update S9 plan §4.1 / T14 / T15
func AcquireStore(directory, locks *os.File) (*Lock, error) {
	if err := supported(); err != nil {
		return nil, err
	}
	if err := privateDirectory(locks); err != nil {
		return nil, err
	}
	identity, err := StoreIdentity(directory)
	if err != nil {
		return nil, err
	}
	name := lockName(identity)
	fd, err := unix.Openat(int(locks.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0640)
	if err != nil {
		return nil, ErrLaunchRejected
	}
	f := os.NewFile(uintptr(fd), "infra-lifetime-lock")
	dir, err := duplicateFile(locks)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	l := &Lock{f, dir, identity, name}
	if err := l.checkInode(); err != nil {
		_ = l.Close()
		return nil, err
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = l.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, ErrStoreBusy
		}
		return nil, err
	}
	// Persist the permanent name before publishing it in a journal or receipt.
	if err := f.Sync(); err != nil {
		_ = l.Close()
		return nil, err
	}
	if err := dir.Sync(); err != nil {
		_ = l.Close()
		return nil, err
	}
	return l, nil
}

func (l *Lock) Identity() infracontract.Store { return l.store }

// AdoptStoreLock duplicates a trusted SCM_RIGHTS or pidfd_getfd reference.
// It validates the permanent name and exclusive OFD without an unlock/re-lock
// handoff. The caller retains its own reference until the receiver's Ack.
// origin: hot-update S9 plan §4.1 / T15
func AdoptStoreLock(directory, locks, file *os.File) (*Lock, error) {
	if err := supported(); err != nil {
		return nil, err
	}
	identity, err := StoreIdentity(directory)
	if err != nil {
		return nil, err
	}
	f, err := duplicateFile(file)
	if err != nil {
		return nil, err
	}
	dir, err := duplicateFile(locks)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	l := &Lock{f, dir, identity, lockName(identity)}
	if err := l.verify(); err != nil {
		_ = l.Close()
		return nil, err
	}
	return l, nil
}

func (l *Lock) checkInode() error {
	if l == nil || l.file == nil || l.directory == nil {
		return ErrLaunchRejected
	}
	if err := privateDirectory(l.directory); err != nil {
		return err
	}
	var opened, named unix.Stat_t
	if err := unix.Fstat(int(l.file.Fd()), &opened); err != nil {
		return err
	}
	if err := unix.Fstatat(int(l.directory.Fd()), l.name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return ErrLaunchRejected
	}
	// A replaced name must not create a second lock domain alongside the old OFD.
	// origin: hot-update S9 plan §4.1 / T15
	if opened.Mode&unix.S_IFMT != unix.S_IFREG || (opened.Uid != 0 && opened.Uid != uint32(os.Geteuid())) || opened.Mode&0022 != 0 || opened.Nlink != 1 || named.Mode&unix.S_IFMT != unix.S_IFREG || opened.Dev != named.Dev || opened.Ino != named.Ino {
		return ErrLaunchRejected
	}
	if l.store.LockInode == 0 {
		l.store.LockDevice, l.store.LockInode = uint64(opened.Dev), opened.Ino
	}
	if uint64(opened.Dev) != l.store.LockDevice || opened.Ino != l.store.LockInode {
		return ErrLaunchRejected
	}
	return nil
}

func (l *Lock) verify() error {
	if err := l.checkInode(); err != nil {
		return err
	}
	// On the SAME OFD this is a no-op. A different SCM_RIGHTS description for
	// the locked inode gets EWOULDBLOCK and cannot counterfeit receiver custody.
	if err := unix.Flock(int(l.file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return ErrLaunchRejected
	}
	other, err := unix.Openat(int(l.directory.Fd()), l.name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return ErrLaunchRejected
	}
	defer func() { _ = unix.Close(other) }()
	if err := unix.Flock(other, unix.LOCK_EX|unix.LOCK_NB); !errors.Is(err, unix.EWOULDBLOCK) {
		return ErrLaunchRejected
	}
	return nil
}

func (l *Lock) duplicate() (*Lock, error) {
	if err := l.verify(); err != nil {
		return nil, err
	}
	f, err := duplicateFile(l.file)
	if err != nil {
		return nil, err
	}
	d, err := duplicateFile(l.directory)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &Lock{f, d, l.store, l.name}, nil
}

func (l *Lock) Close() error {
	if l == nil {
		return nil
	}
	var err error
	// Never flock(LOCK_UN): all duplicates share that lock's state.
	if l.file != nil {
		err = l.file.Close()
		l.file = nil
	}
	if l.directory != nil {
		err = errors.Join(err, l.directory.Close())
		l.directory = nil
	}
	return err
}

func duplicateFile(f *os.File) (*os.File, error) {
	if f == nil {
		return nil, ErrLaunchRejected
	}
	fd, err := unix.FcntlInt(f.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), "infra-launch-resource"), nil
}

func prepare(r infracontract.HostRequest, s Selection) (*Prepared, error) {
	p := &Prepared{request: r}
	var err error
	for _, pair := range []struct {
		source *os.File
		target **os.File
	}{{s.Executable, &p.executable}, {s.Config, &p.config}, {s.Directory, &p.directory}} {
		*pair.target, err = duplicateFile(pair.source)
		if err != nil {
			_ = p.Close()
			return nil, err
		}
	}
	if s.Guard != nil {
		p.lock, err = s.Guard.duplicate()
	} else {
		p.lock, err = AcquireStore(s.Directory, s.Locks)
	}
	if err == nil {
		err = p.verify()
	}
	if err != nil {
		_ = p.Close()
		return nil, err
	}
	return p, nil
}

func (p *Prepared) duplicate() (*Prepared, error) {
	copy, err := prepare(p.request, Selection{Executable: p.executable, Config: p.config, Directory: p.directory, Guard: p.lock})
	if err == nil {
		copy.deadline = p.deadline
	}
	return copy, err
}

func immutableHash(f *os.File, want infracontract.Digest, executable bool) error {
	if f == nil || want == (infracontract.Digest{}) {
		return ErrLaunchRejected
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return err
	}
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFL, 0)
	if err != nil {
		return err
	}
	if flags&unix.O_ACCMODE != unix.O_RDONLY || st.Mode&unix.S_IFMT != unix.S_IFREG || (st.Uid != 0 && st.Uid != uint32(os.Geteuid())) || st.Mode&0022 != 0 || st.Nlink != 1 || (executable && st.Mode&0111 == 0) {
		return ErrLaunchRejected
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, st.Size)); err != nil {
		return ErrLaunchRejected
	}
	if string(h.Sum(nil)) != string(want[:]) {
		return ErrLaunchRejected
	}
	return nil
}

func (p *Prepared) verify() error {
	if p == nil {
		return ErrLaunchRejected
	}
	if _, err := infracontract.EncodeRequest(p.request); err != nil {
		return err
	}
	if p.request.Op != infracontract.LaunchClaim {
		return ErrLaunchRejected
	}
	if p.request.Candidate.GOOS != runtime.GOOS || p.request.Candidate.GOARCH != runtime.GOARCH {
		return infracontract.ErrUnsupported
	}
	if err := immutableHash(p.executable, p.request.Candidate.BinarySHA256, true); err != nil {
		return err
	}
	if err := immutableHash(p.config, p.request.Config, false); err != nil {
		return err
	}
	actual, err := StoreIdentity(p.directory)
	if err != nil {
		return err
	}
	// Mount identity belongs to this actual descriptor; callers canonicalize a
	// bind alias before creating the request, while dev/ino determines locking.
	if actual.Device != p.request.Store.Device || actual.Inode != p.request.Store.Inode || actual.MountID != p.request.Store.MountID {
		return ErrLaunchRejected
	}
	if err := p.lock.verify(); err != nil {
		return err
	}
	if p.lock.Identity() != p.request.Store {
		return ErrLaunchRejected
	}
	return nil
}

func executableIdentity(f *os.File, p infracontract.Process) error {
	var st unix.Stat_t
	if f == nil {
		return ErrLaunchRejected
	}
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return err
	}
	if uint64(st.Dev) != p.ExeDevice || st.Ino != p.ExeInode {
		return ErrLaunchRejected
	}
	return nil
}

// CurrentProcess is the launcher's local pre-exec self check. P02 independently
// observes it at the server; environment alone never supplies physical identity.
func CurrentProcess(invocation string) (infracontract.Process, error) {
	if invocation == "" {
		return infracontract.Process{}, ErrLaunchRejected
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return infracontract.Process{}, err
	}
	stat, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return infracontract.Process{}, err
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		return infracontract.Process{}, ErrLaunchRejected
	}
	fields := strings.Fields(string(stat)[end+1:])
	if len(fields) < 20 {
		return infracontract.Process{}, ErrLaunchRejected
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return infracontract.Process{}, err
	}
	cgroup, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return infracontract.Process{}, err
	}
	path := ""
	for _, line := range strings.Split(string(cgroup), "\n") {
		if strings.HasPrefix(line, "0::") {
			path = strings.TrimPrefix(line, "0::")
		}
	}
	if path == "" {
		return infracontract.Process{}, infracontract.ErrUnsupported
	}
	f, err := os.Open("/proc/self/exe")
	if err != nil {
		return infracontract.Process{}, err
	}
	defer func() { _ = f.Close() }()
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		return infracontract.Process{}, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return infracontract.Process{}, err
	}
	var digest infracontract.Digest
	copy(digest[:], h.Sum(nil))
	return infracontract.Process{BootID: strings.TrimSpace(string(boot)), PID: int32(os.Getpid()), StartTicks: ticks, InvocationID: invocation, ExeDevice: uint64(st.Dev), ExeInode: st.Ino, ExeSHA256: digest, Cgroup: path}, nil
}

func execPrepared(p *Prepared, d infracontract.Deadline, stop infracontract.Stop) error {
	if err := supported(); err != nil {
		return err
	}
	self, err := CurrentProcess(os.Getenv("INVOCATION_ID"))
	if err != nil || self != p.request.Expected {
		return ErrLaunchRejected
	}
	config := fmt.Sprintf("/proc/self/fd/%d", p.config.Fd())
	store := fmt.Sprintf("/proc/self/fd/%d", p.directory.Fd())
	var args, env []string
	switch p.request.Component {
	case infracontract.NATS:
		args = []string{"nats-server", "--config", config}
	case infracontract.Caddy:
		args = []string{"caddy", "run", "--config", config}
		env = append(env, "XDG_DATA_HOME="+store, "XDG_CONFIG_HOME="+store)
	case infracontract.HostSupport:
		return ErrLaunchRejected
	default:
		return ErrLaunchRejected
	}
	env = append(env, "PATH=/usr/bin:/bin", "LANG=C", "INVOCATION_ID="+self.InvocationID)
	// Unknown inherited descriptors are closed by exec. Only the explicit OFD,
	// read-only config and selected storage directory survive into stock code.
	// origin: hot-update S9 plan §4.1 / §6.1 / T14
	if err := unix.CloseRange(3, 1<<32-1, unix.CLOSE_RANGE_CLOEXEC); err != nil {
		return infracontract.ErrUnsupported
	}
	files := []*os.File{p.lock.file, p.config, p.directory}
	for _, f := range files {
		if _, err := unix.FcntlInt(f.Fd(), unix.F_SETFD, 0); err != nil {
			return err
		}
	}
	defer func() {
		for _, f := range files {
			unix.CloseOnExec(int(f.Fd()))
		}
	}()
	if err := admission(d, stop); err != nil {
		return err
	}
	return syscall.Exec(fmt.Sprintf("/proc/self/fd/%d", p.executable.Fd()), args, env)
}

// Send emits one versioned canonical LaunchClaim plus exactly five SCM_RIGHTS
// descriptors. The caller keeps its own reference until independent LaunchAck.
// P03 authenticates the peer and P04 calls Do before sending these resources.
func Send(conn *net.UnixConn, p *Prepared, d infracontract.Deadline, stop infracontract.Stop) error {
	if p == nil {
		return ErrLaunchRejected
	}
	if err := p.verify(); err != nil {
		return err
	}
	if err := admission(p.deadline, stop); err != nil {
		return err
	}
	finish, err := transportDeadline(conn, d, stop)
	if err != nil {
		return err
	}
	defer finish()
	data, err := infracontract.EncodeRequest(p.request)
	if err != nil {
		return err
	}
	body := make([]byte, 16+len(data))
	frame := LaunchFrame{Version: 1, RequestBytes: uint32(len(data)), Deadline: p.deadline}
	binary.BigEndian.PutUint32(body, frame.Version)
	binary.BigEndian.PutUint32(body[4:], frame.RequestBytes)
	binary.BigEndian.PutUint64(body[8:], uint64(frame.Deadline))
	copy(body[16:], data)
	rights := unix.UnixRights(int(p.executable.Fd()), int(p.config.Fd()), int(p.directory.Fd()), int(p.lock.directory.Fd()), int(p.lock.file.Fd()))
	n, oob, err := conn.WriteMsgUnix(body, rights, nil)
	if err != nil || oob != len(rights) {
		return ErrLaunchRejected
	}
	if n < len(body) {
		_, err = conn.Write(body[n:])
	}
	return err
}

// Receive only trusts a connected root peer, never a caller-writable selector
// or journal pathname. Its request must identify this actual launcher PID.
func Receive(conn *net.UnixConn, component infracontract.Component, d infracontract.Deadline, stop infracontract.Stop) (*Prepared, error) {
	return receiveFrom(conn, component, d, stop, 0)
}

func receiveFrom(conn *net.UnixConn, component infracontract.Component, d infracontract.Deadline, stop infracontract.Stop, uid uint32) (*Prepared, error) {
	if err := supported(); err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, ErrLaunchRejected
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return nil, err
	}
	var peer *unix.Ucred
	var peerErr error
	if err := raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return nil, err
	}
	if peerErr != nil || peer == nil || peer.Uid != uid {
		return nil, ErrLaunchRejected
	}
	finish, err := transportDeadline(conn, d, stop)
	if err != nil {
		return nil, err
	}
	defer finish()
	header := make([]byte, 16)
	control := make([]byte, unix.CmsgSpace(5*4))
	n, oob, flags, _, err := conn.ReadMsgUnix(header[:1], control)
	var files []*os.File
	defer func() {
		for _, f := range files {
			_ = f.Close()
		}
	}()
	msgs, parseErr := unix.ParseSocketControlMessage(control[:oob])
	for _, msg := range msgs {
		fds, e := unix.ParseUnixRights(&msg)
		if e != nil {
			parseErr = e
			continue
		}
		for _, fd := range fds {
			unix.CloseOnExec(fd)
			files = append(files, os.NewFile(uintptr(fd), "infra-launch-received"))
		}
	}
	if err != nil || parseErr != nil || flags&(unix.MSG_CTRUNC|unix.MSG_TRUNC) != 0 || n != 1 || len(files) != 5 {
		return nil, ErrLaunchRejected
	}
	if _, err := io.ReadFull(conn, header[1:]); err != nil {
		return nil, ErrLaunchRejected
	}
	frame := LaunchFrame{Version: binary.BigEndian.Uint32(header), RequestBytes: binary.BigEndian.Uint32(header[4:]), Deadline: infracontract.Deadline(binary.BigEndian.Uint64(header[8:]))}
	rootDeadline := frame.Deadline
	if frame.Version != 1 || admission(rootDeadline, stop) != nil {
		return nil, ErrLaunchRejected
	}
	size := frame.RequestBytes
	if size == 0 || size > infracontract.MaxRequestBytes {
		return nil, ErrLaunchRejected
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(conn, data); err != nil {
		return nil, ErrLaunchRejected
	}
	r, err := infracontract.DecodeRequest(data)
	if err != nil || r.Op != infracontract.LaunchClaim || r.Component != component {
		return nil, ErrLaunchRejected
	}
	p := &Prepared{request: r, executable: files[0], config: files[1], directory: files[2], lock: &Lock{files[4], files[3], r.Store, requestLockName(r)}, deadline: rootDeadline}
	for _, f := range []*os.File{p.executable, p.config, p.lock.directory, p.lock.file} {
		var st unix.Stat_t
		if unix.Fstat(int(f.Fd()), &st) != nil || st.Uid != uid {
			return nil, ErrLaunchRejected
		}
	}
	if err := p.verify(); err != nil {
		return nil, err
	}
	self, err := CurrentProcess(os.Getenv("INVOCATION_ID"))
	if err != nil || self != r.Expected {
		return nil, ErrLaunchRejected
	}
	files = nil
	return p, nil
}

func transportDeadline(conn *net.UnixConn, d infracontract.Deadline, stop infracontract.Stop) (func(), error) {
	if conn == nil {
		return nil, ErrLaunchRejected
	}
	if err := admission(d, stop); err != nil {
		return nil, err
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(remaining)); err != nil {
		return nil, err
	}
	if stop == nil {
		return func() {}, nil
	}
	done, joined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-done:
		case <-stop:
			_ = conn.SetDeadline(time.Now())
		}
	}()
	return func() { close(done); <-joined }, nil
}
