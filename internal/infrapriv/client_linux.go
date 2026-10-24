//go:build linux

package infrapriv

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// Compilation on another Linux architecture is not an authorization receipt.
// Kernel/systemd/nft capability checks belong to P02/P05/P07 before effects.
// origin: hot-update S9 plan lead LQ1 / §6.1 / T07
func supportedPlatform() error {
	if runtime.GOARCH != "amd64" {
		return c.ErrUnsupported
	}
	return nil
}

// PeerCredentials never trusts a UID/PID/boot supplied in the request. A pidfd
// pins the original process during both stat reads, including PID reuse windows.
// Launch cgroup/MainPID/invocation authorization is additionally P06's guard.
// origin: hot-update S9 plan §2.1 / §6.1 / T07
func PeerCredentials(conn *net.UnixConn) (c.Peer, error) {
	if err := supportedPlatform(); err != nil {
		return c.Peer{}, err
	}
	if conn == nil {
		return c.Peer{}, ErrPermission
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return c.Peer{}, ErrPermission
	}
	var cred *unix.Ucred
	var sockErr error
	if err := raw.Control(func(fd uintptr) { cred, sockErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil || sockErr != nil || cred == nil || cred.Pid <= 0 {
		return c.Peer{}, ErrPermission
	}
	pidfd, err := unix.PidfdOpen(int(cred.Pid), 0)
	if err != nil {
		return c.Peer{}, c.ErrUnsupported
	}
	// The pidfd only pins the peer across the two stat reads and the liveness
	// poll below; once those have decided, a close failure cannot alter them.
	defer func() { _ = unix.Close(pidfd) }()
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return c.Peer{}, ErrPermission
	}
	ticks, err := processStartTicks(cred.Pid)
	if err != nil {
		return c.Peer{}, err
	}
	again, err := processStartTicks(cred.Pid)
	if err != nil || ticks != again {
		return c.Peer{}, ErrPermission
	}
	poll := []unix.PollFd{{Fd: int32(pidfd), Events: unix.POLLIN}}
	if _, err := unix.Poll(poll, 0); err != nil || poll[0].Revents != 0 {
		return c.Peer{}, ErrPermission
	}
	return c.Peer{PID: cred.Pid, UID: cred.Uid, GID: cred.Gid, BootID: strings.TrimSpace(string(boot)), StartTicks: ticks}, nil
}

func processStartTicks(pid int32) (uint64, error) {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(int(pid)) + "/stat")
	if err != nil {
		return 0, ErrPermission
	}
	return parseStartTicks(data)
}

func parseStartTicks(data []byte) (uint64, error) {
	// comm can itself contain spaces and ')' characters; the last ')' is the
	// delimiter before the fixed kernel field layout, not the first whitespace.
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0, ErrPermission
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) <= 19 {
		return 0, ErrPermission
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || ticks == 0 {
		return 0, ErrPermission
	}
	return ticks, nil
}

// receiveFilesLimit bounds ancillary rights before authorization. Only Stage inputs
// and LaunchClaim outputs can subsequently cross the operation whitelist. All
// received references are CLOEXEC and closed on every malformed/truncated path.
// origin: hot-update S9 plan §3.1 / §6.1 / T08
func receiveFilesLimit(conn *net.UnixConn, dst []byte, limit int) (int, []*os.File, error) {
	oob := make([]byte, unix.CmsgSpace(limit*4))
	n, oobn, flags, _, err := conn.ReadMsgUnix(dst, oob)
	var files []*os.File
	bad := false
	if oobn > 0 {
		messages, parseErr := unix.ParseSocketControlMessage(oob[:oobn])
		bad = parseErr != nil
		if parseErr == nil {
			for _, msg := range messages {
				fds, e := unix.ParseUnixRights(&msg)
				if e != nil {
					bad = true
					continue
				}
				for _, fd := range fds {
					unix.CloseOnExec(fd)
					files = append(files, os.NewFile(uintptr(fd), "infra-received-file"))
				}
			}
		}
	}
	if bad || len(files) > limit || flags&(unix.MSG_CTRUNC|unix.MSG_TRUNC) != 0 {
		closeFiles(files)
		return n, nil, ErrIPC
	}
	return n, files, err
}

func sendFiles(conn *net.UnixConn, data []byte, files []*os.File) (int, error) {
	if len(files) > LaunchFileDescriptors {
		return 0, ErrIPC
	}
	fds := make([]int, len(files))
	for i, file := range files {
		fds[i] = int(file.Fd())
	}
	n, _, err := conn.WriteMsgUnix(data, unix.UnixRights(fds...), nil)
	return n, err
}

func validateReceivedFile(file *os.File, readOnly bool) error {
	if file == nil {
		return ErrPermission
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return ErrPermission
	}
	var checkErr error
	if err := raw.Control(func(fd uintptr) {
		var st unix.Stat_t
		if unix.Fstat(int(fd), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG {
			checkErr = ErrPermission
			return
		}
		flags, err := unix.FcntlInt(fd, unix.F_GETFL, 0)
		if err != nil || flags&unix.O_ACCMODE == unix.O_WRONLY || (readOnly && flags&unix.O_ACCMODE != unix.O_RDONLY) {
			checkErr = ErrPermission
		}
	}); err != nil {
		return ErrPermission
	}
	return checkErr
}

// openTrustedDirectory walks each ancestor through a held dirfd. O_NOFOLLOW at
// every step and root ownership without group/other write exclude symlink and
// parent replacement races, not just a check on the final basename.
// origin: hot-update S9 plan §3.1 / §6.3 / T07
func openTrustedDirectory(path string) (*os.File, error) {
	if err := supportedPlatform(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, ErrCredential
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrCredential
	}
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if part == "" {
			continue
		}
		next, err := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(fd)
		if err != nil {
			return nil, ErrCredential
		}
		fd = next
		var st unix.Stat_t
		if unix.Fstat(fd, &st) != nil || st.Uid != 0 || st.Mode&0022 != 0 {
			_ = unix.Close(fd)
			return nil, ErrCredential
		}
	}
	return os.NewFile(uintptr(fd), "infra-trusted-directory"), nil
}

func readOwnedFile(dir *os.File, name string, owner, gid, mode uint32, maxBytes int) ([]byte, error) {
	fd, err := unix.Openat(int(dir.Fd()), name, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, ErrCredential
	}
	file := os.NewFile(uintptr(fd), "infra-credential")
	// Read-only descriptor: the verdict is the before/after metadata pair and
	// the bounded read, all decided before close, so its result carries nothing.
	defer func() { _ = file.Close() }()
	var before unix.Stat_t
	if unix.Fstat(fd, &before) != nil || before.Mode&unix.S_IFMT != unix.S_IFREG || before.Mode&07777 != mode ||
		before.Uid != owner || before.Gid != gid || before.Nlink != 1 || before.Size <= 0 || before.Size > int64(maxBytes) {
		return nil, ErrCredential
	}
	data, err := io.ReadAll(io.LimitReader(file, int64(maxBytes)+1))
	if err != nil || len(data) > maxBytes {
		clear(data)
		return nil, ErrCredential
	}
	var after unix.Stat_t
	// Reading may update atime. Identity, access control, size, mtime and ctime
	// must stay unchanged; treating atime as a mutation rejects our own read.
	if unix.Fstat(fd, &after) != nil || !unchangedMetadata(before, after) {
		clear(data)
		return nil, ErrCredential
	}
	return data, nil
}

func unchangedMetadata(before, after unix.Stat_t) bool {
	return before.Dev == after.Dev && before.Ino == after.Ino && before.Mode == after.Mode && before.Uid == after.Uid &&
		before.Gid == after.Gid && before.Nlink == after.Nlink && before.Size == after.Size && before.Mtim == after.Mtim && before.Ctim == after.Ctim
}

// LoadCredential only selects the closed descriptor's filename, never a caller
// supplied seed path. The root-owned parent permits the intended tether group to
// read 0640, while forbidding that group from replacing any credential inode.
func LoadCredential(directory string, kind CredentialKind, tetherGID uint32) (*Credential, error) {
	row, err := credentialRow(kind)
	if err != nil {
		return nil, err
	}
	dir, err := openTrustedDirectory(directory)
	if err != nil {
		return nil, err
	}
	// openTrustedDirectory hands out an O_RDONLY dirfd that only anchors openat;
	// closing it cannot lose data, so the result is deliberately discarded.
	defer func() { _ = dir.Close() }()
	data, err := readOwnedFile(dir, row.Name, row.OwnerUID, tetherGID, row.Mode, 58)
	if err != nil {
		return nil, err
	}
	return decodeCredential(data)
}

func LoadPolicy(directory string) (Policy, error) {
	dir, err := openTrustedDirectory(directory)
	if err != nil {
		return Policy{}, err
	}
	defer func() { _ = dir.Close() }() // read-only anchor dirfd, as in LoadCredential
	data, err := readOwnedFile(dir, "policy.json", 0, 0, 0600, c.MaxRequestBytes)
	if err != nil {
		return Policy{}, ErrPolicy
	}
	return DecodePolicy(data)
}

// BootstrapCredentials is called only from the explicit root bootstrap under
// P04's exclusive domain lease. Re-running validates and reuses all four seeds;
// any partial existing set refuses rather than silently regenerating authority.
func BootstrapCredentials(directory string, tetherGID uint32) error {
	if err := supportedPlatform(); err != nil {
		return err
	}
	if os.Geteuid() != 0 || tetherGID == 0 {
		return ErrPermission
	}
	dir, err := openTrustedDirectory(directory)
	if err != nil {
		return err
	}
	// Durability is bootstrapCredentialsAt's checked file and directory Sync;
	// closing the O_RDONLY anchor dirfd afterwards cannot undo or lose either.
	defer func() { _ = dir.Close() }()
	return bootstrapCredentialsAt(dir, 0, tetherGID)
}

func bootstrapCredentialsAt(dir *os.File, owner, gid uint32) error {
	existing := 0
	for _, row := range CredentialDescriptors() {
		var st unix.Stat_t
		err := unix.Fstatat(int(dir.Fd()), row.Name, &st, unix.AT_SYMLINK_NOFOLLOW)
		if err == nil {
			existing++
		} else if !errors.Is(err, unix.ENOENT) {
			return ErrCredential
		}
	}
	if existing != 0 && existing != len(CredentialDescriptors()) {
		return ErrCredential
	}
	for _, row := range CredentialDescriptors() {
		if existing != 0 {
			data, err := readOwnedFile(dir, row.Name, owner, gid, row.Mode, 58)
			if err != nil {
				return err
			}
			key, err := decodeCredential(data)
			if err != nil {
				return err
			}
			_ = key.Close()
			continue
		}
		data, err := generateCredential()
		if err != nil {
			return err
		}
		fd, err := unix.Openat(int(dir.Fd()), row.Name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_NOFOLLOW|unix.O_CLOEXEC, row.Mode)
		if err != nil {
			clear(data)
			return ErrCredential
		}
		file := os.NewFile(uintptr(fd), "infra-bootstrap-credential")
		err = file.Chown(int(owner), int(gid))
		if err == nil {
			err = file.Chmod(os.FileMode(row.Mode))
		}
		if err == nil {
			_, err = file.Write(data)
		}
		clear(data)
		if err == nil {
			err = file.Sync()
		}
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return ErrCredential
		}
	}
	if dir.Sync() != nil {
		return ErrCredential
	}
	return nil
}

func NewWorkerLimit(directory string) (*WorkerLimit, error) {
	dir, err := openTrustedDirectory(directory)
	if err != nil {
		return nil, err
	}
	return &WorkerLimit{dir: dir, owner: 0}, nil
}

func acquireWorkerSlot(l *WorkerLimit) (func(), error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil, ErrBusy
	}
	for slot := 0; slot < MaxWorkers; slot++ {
		name := "worker-" + strconv.Itoa(slot) + ".lock"
		fd, err := unix.Openat(int(l.dir.Fd()), name, unix.O_RDWR|unix.O_CREAT|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0600)
		if err != nil {
			return nil, ErrPermission
		}
		var st unix.Stat_t
		if unix.Fstat(fd, &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFREG || st.Mode&07777 != 0600 || st.Uid != l.owner || st.Nlink != 1 {
			_ = unix.Close(fd)
			return nil, ErrPermission
		}
		if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
			_ = unix.Close(fd)
			if errors.Is(err, unix.EWOULDBLOCK) {
				continue
			}
			return nil, ErrPermission
		}
		var once sync.Once
		return func() { once.Do(func() { _ = unix.Close(fd) }) }, nil
	}
	return nil, ErrBusy
}

func NewClient(tetherGID uint32) (*Client, error) {
	if err := supportedPlatform(); err != nil {
		return nil, err
	}
	if tetherGID == 0 {
		return nil, ErrPermission
	}
	return &Client{path: DefaultSocketPath, serverUID: 0, gid: tetherGID, checkPath: true}, nil
}

// Do never retries an unknown result: P04 alone owns durable sequence/digest
// replay. A receipt must match THIS signed request, including its canonical hash.
// origin: hot-update S9 plan §2.2 / §6.1 / T07 / T08
func (client *Client) Do(r c.HostRequest, deadline c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	receipt, files, err := client.Exchange(r, nil, deadline, stop)
	if len(files) != 0 {
		closeFiles(files)
		return c.HostReceipt{}, ErrResultUnknown
	}
	return receipt, err
}

// Exchange transfers only the closed operation's approved FD set. Inputs remain
// caller-owned; returned descriptors become caller-owned. P06 verifies its
// immutable permit and lock identity before clearing the lock FD's CLOEXEC.
func (client *Client) Exchange(r c.HostRequest, inputs []*os.File, deadline c.Deadline, stop c.Stop) (result c.HostReceipt, outputs []*os.File, resultErr error) {
	defer func() {
		if resultErr != nil {
			closeFiles(outputs)
			outputs = nil
		}
	}()
	if err := supportedPlatform(); err != nil {
		return c.HostReceipt{}, nil, err
	}
	if err := checkBudget(deadline, stop); err != nil {
		return c.HostReceipt{}, nil, err
	}
	rule := fileRule(r.Op)
	if len(inputs) > int(rule.MaxInput) {
		return c.HostReceipt{}, nil, ErrPermission
	}
	for _, input := range inputs {
		if err := validateReceivedFile(input, false); err != nil {
			return c.HostReceipt{}, nil, err
		}
	}
	data, err := c.EncodeRequest(r)
	if err != nil || len(r.Signature) != 64 {
		return c.HostReceipt{}, nil, ErrIPC
	}
	if client == nil {
		return c.HostReceipt{}, nil, ErrIPC
	}
	if client.checkPath {
		if err := verifySocketPath(client.path, client.gid); err != nil {
			return c.HostReceipt{}, nil, err
		}
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return c.HostReceipt{}, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), remaining) // ctx-none: Host.Do supplies absolute deadline/stop, not a context.
	dialDone, dialJoined := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(dialJoined)
		select {
		case <-stop:
			cancel()
		case <-dialDone:
		case <-ctx.Done():
		}
	}()
	defer func() { close(dialDone); cancel(); <-dialJoined }()
	connection, err := (&net.Dialer{}).DialContext(ctx, "unix", client.path)
	if err != nil {
		return c.HostReceipt{}, nil, ErrIPC
	}
	conn, ok := connection.(*net.UnixConn)
	if !ok {
		_ = connection.Close()
		return c.HostReceipt{}, nil, ErrIPC
	}
	// The exchange's outcome is fixed by the frames read below, and
	// interruptConn may already have closed this descriptor on stop/deadline;
	// a close error therefore cannot turn a verified receipt into anything else.
	defer func() { _ = conn.Close() }()
	peer, err := PeerCredentials(conn)
	if err != nil || peer.UID != client.serverUID || peer.BootID != r.HostBootID {
		return c.HostReceipt{}, nil, ErrPermission
	}
	workStop, join := interruptConn(conn, deadline, stop)
	defer join()
	if err := setWriteDeadline(conn, deadline); err != nil {
		return c.HostReceipt{}, nil, err
	}
	if err := writeFileFrame(conn, RequestPurpose, data, inputs); err != nil {
		return c.HostReceipt{}, nil, ErrResultUnknown
	}
	if err := conn.CloseWrite(); err != nil {
		return c.HostReceipt{}, nil, ErrResultUnknown
	}
	if err := setReadDeadline(conn, deadline); err != nil {
		return c.HostReceipt{}, nil, ErrResultUnknown
	}
	purpose, encoded, outputs, err := readFileFrameLimit(conn, "", max(MaxFileDescriptors, int(rule.Output)))
	if err != nil {
		return c.HostReceipt{}, outputs, ErrResultUnknown
	}
	if err := checkBudget(deadline, workStop); err != nil {
		return c.HostReceipt{}, outputs, ErrResultUnknown
	}
	if purpose == RefusalPurpose {
		refusal, err := decodeRefusal(encoded)
		if err != nil || len(outputs) != 0 || refusal.Tx != r.Tx || refusal.Sequence != r.Sequence || refusal.RequestDigest != sha256.Sum256(data) {
			return c.HostReceipt{}, outputs, ErrResultUnknown
		}
		return c.HostReceipt{}, outputs, refusalError(refusal.Code)
	}
	receipt, err := c.DecodeReceipt(encoded)
	if err != nil || receipt.Tx != r.Tx || receipt.Sequence != r.Sequence || receipt.Op != r.Op || receipt.RequestDigest != sha256.Sum256(data) {
		return c.HostReceipt{}, outputs, ErrResultUnknown
	}
	if len(outputs) != int(rule.Output) {
		return c.HostReceipt{}, outputs, ErrResultUnknown
	}
	if len(outputs) != 0 {
		if err := validateLaunchFiles(outputs); err != nil {
			return c.HostReceipt{}, outputs, ErrResultUnknown
		}
	}
	return receipt, outputs, nil
}

func verifySocketPath(path string, gid uint32) error {
	dir, err := openTrustedDirectory(filepath.Dir(path))
	if err != nil {
		return ErrNotEnabled
	}
	defer func() { _ = dir.Close() }() // read-only anchor dirfd, as in LoadCredential
	var st unix.Stat_t
	if err := unix.Fstatat(int(dir.Fd()), filepath.Base(path), &st, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return ErrNotEnabled
	}
	if st.Mode&unix.S_IFMT != unix.S_IFSOCK || st.Uid != 0 || st.Gid != gid || st.Mode&07777 != 0660 {
		return ErrPermission
	}
	return nil
}

// Keep formatted filesystem/syscall failures out of protocol replies: their
// text can contain caller paths. Only the closed error family crosses this leaf.
var _ = fmt.Formatter((*Credential)(nil))

// Descriptor roles, not an arbitrary ancillary-count increase, authorize the
// launch reply. P06 additionally binds immutable bytes and the actual lock OFD.
// origin: S9 internal review rv-infra F03
func validateLaunchFiles(files []*os.File) error {
	if len(files) != LaunchFileDescriptors {
		return ErrPermission
	}
	for i, file := range files {
		if file == nil {
			return ErrPermission
		}
		if i == 2 || i == 3 {
			var st unix.Stat_t
			flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFL, 0)
			if err != nil || flags&unix.O_ACCMODE != unix.O_RDONLY || unix.Fstat(int(file.Fd()), &st) != nil || st.Mode&unix.S_IFMT != unix.S_IFDIR {
				return ErrPermission
			}
		} else if err := validateReceivedFile(file, i < 2); err != nil {
			return err
		}
	}
	return nil
}
