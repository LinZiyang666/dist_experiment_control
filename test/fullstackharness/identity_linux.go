// Package fullstackharness collects acceptance evidence without exercising
// upgrade authority. Kernel witnesses cannot supply missing product counters.
// origin: hot-update S10 plan §2.1 / P04
package fullstackharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"golang.org/x/sys/unix"
)

var (
	ErrIdentityChanged   = errors.New("fullstack: process or executing image changed")
	ErrKernelUnsupported = errors.New("fullstack: kernel witness unavailable")
)

// SampleSource must read the actual owner, including its fresh query nonce.
// Installing a source does not certify its observations: every call is checked
// against the kernel, and incomplete counters remain incomplete.
type SampleSource func(context.Context, contract.Key, string) (contract.Sample, error)
type StampReader func(context.Context) (contract.Stamp, error)

type ExecutableIdentity struct {
	Device uint64          `json:"device"`
	Inode  uint64          `json:"inode"`
	SHA    contract.Digest `json:"sha"`
}

// ExecutableRead owns one independent executable FD and one pidfd. The current
// executable is reopened separately; keeping the first FD is what prevents a
// replaced pathname or an unlinked image from changing the bytes being judged.
type ExecutableRead struct {
	Before, After     contract.Stamp
	Original, Current ExecutableIdentity
	mu                sync.Mutex
	exe, pidfd        *os.File
}

// ReadKernelProcess obtains the kernel axes; InstanceID still comes from the
// producer and is deliberately not inferred from a PID or a filesystem path.
func ReadKernelProcess(key contract.Key, pid int, instanceID string) (contract.Process, error) {
	if pid <= 0 || instanceID == "" {
		return contract.Process{}, errors.New("fullstack: missing process identity")
	}
	base := "/proc/" + strconv.Itoa(pid)
	var stat unix.Stat_t
	if err := unix.Stat(base, &stat); err != nil {
		return contract.Process{}, err
	}
	if stat.Uid != key.UID {
		return contract.Process{}, ErrIdentityChanged
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return contract.Process{}, err
	}
	ns, err := os.Readlink(base + "/ns/pid")
	if err != nil {
		return contract.Process{}, err
	}
	data, err := os.ReadFile(base + "/stat")
	if err != nil {
		return contract.Process{}, err
	}
	// comm may contain spaces and parentheses, so splitting the entire stat line
	// can silently turn a different field into start_ticks.
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return contract.Process{}, errors.New("fullstack: malformed process stat")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) <= 19 {
		return contract.Process{}, errors.New("fullstack: short process stat")
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil || ticks == 0 || strings.TrimSpace(string(boot)) == "" || ns == "" {
		return contract.Process{}, errors.New("fullstack: invalid kernel identity")
	}
	return contract.Process{Key: key, BootID: strings.TrimSpace(string(boot)), PIDNS: ns,
		InstanceID: instanceID, PID: pid, StartTicks: ticks}, nil
}

func checkKernelProcess(process contract.Process) error {
	actual, err := ReadKernelProcess(process.Key, process.PID, process.InstanceID)
	if err != nil {
		return err
	}
	if actual != process {
		return ErrIdentityChanged
	}
	return nil
}

// origin: hot-update S10 plan §2.1 / TestFullstackExecutableReadRejectsExecABA
func sameImageStamp(a, b contract.Stamp) bool {
	return a.Process == b.Process && a.Generation == b.Generation && a.CutID == b.CutID &&
		a.TxID == b.TxID && a.Revision == b.Revision && a.Direction == b.Direction &&
		a.ScopeDigest == b.ScopeDigest && a.QueryNonce != "" && a.QueryNonce == b.QueryNonce &&
		b.Sequence >= a.Sequence && b.AtBootNS >= a.AtBootNS
}

func livePIDFD(fd int) error {
	poll := []unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}
	_, err := unix.Poll(poll, 0)
	if err != nil {
		return err
	}
	if poll[0].Revents != 0 {
		return ErrIdentityChanged
	}
	return nil
}

func hashExecutable(ctx context.Context, file *os.File) (ExecutableIdentity, error) {
	var st unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &st); err != nil {
		return ExecutableIdentity{}, err
	}
	if st.Mode&unix.S_IFMT != unix.S_IFREG {
		return ExecutableIdentity{}, errors.New("fullstack: executable is not regular")
	}
	hash := sha256.New()
	buffer := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return ExecutableIdentity{}, err
		}
		n, err := file.Read(buffer)
		if n > 0 {
			_, _ = hash.Write(buffer[:n])
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return ExecutableIdentity{}, err
		}
	}
	var after unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &after); err != nil {
		return ExecutableIdentity{}, err
	}
	if st.Dev != after.Dev || st.Ino != after.Ino || st.Size != after.Size || st.Mtim != after.Mtim || st.Ctim != after.Ctim {
		return ExecutableIdentity{}, ErrIdentityChanged
	}
	var digest contract.Digest
	copy(digest[:], hash.Sum(nil))
	return ExecutableIdentity{Device: uint64(st.Dev), Inode: st.Ino, SHA: digest}, nil
}

// ReadExecutable samples only. A changed cut is returned to the caller for a
// new read; this function never reissues Stage, Apply, or any other effect.
func ReadExecutable(ctx context.Context, before contract.Stamp, current StampReader) (_ *ExecutableRead, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if current == nil || before.QueryNonce == "" || before.CutID == "" || before.Generation == 0 {
		return nil, errors.New("fullstack: missing current image stamp")
	}
	if _, err := contract.EncodeJSON("stamp", before); err != nil {
		return nil, err
	}
	if err := checkKernelProcess(before.Process); err != nil {
		return nil, err
	}
	pfd, err := unix.PidfdOpen(before.Process.PID, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: pidfd: %w", ErrKernelUnsupported, err)
	}
	read := &ExecutableRead{Before: before, pidfd: os.NewFile(uintptr(pfd), "fullstack-pidfd")}
	defer func() {
		if err != nil {
			err = errors.Join(err, read.Close())
		}
	}()
	if err := livePIDFD(pfd); err != nil {
		return nil, err
	}
	if err := checkKernelProcess(before.Process); err != nil {
		return nil, err
	}
	path := "/proc/" + strconv.Itoa(before.Process.PID) + "/exe"
	read.exe, err = os.Open(path)
	if err != nil {
		return nil, err
	}
	read.Original, err = hashExecutable(ctx, read.exe)
	if err != nil {
		return nil, err
	}
	read.After, err = current(ctx)
	if err != nil {
		return nil, err
	}
	if !sameImageStamp(before, read.After) {
		return nil, ErrIdentityChanged
	}
	if err := checkKernelProcess(before.Process); err != nil {
		return nil, err
	}
	reopened, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	read.Current, err = hashExecutable(ctx, reopened)
	err = errors.Join(err, reopened.Close())
	if err != nil {
		return nil, err
	}
	// origin: hot-update S10 plan §2.1 / M07A/M07B/M09A/M09B
	if read.Original != read.Current {
		return nil, ErrIdentityChanged
	}
	final, err := current(ctx)
	if err != nil {
		return nil, err
	}
	if !sameImageStamp(read.After, final) {
		return nil, ErrIdentityChanged
	}
	read.After = final
	if err := checkKernelProcess(before.Process); err != nil {
		return nil, err
	}
	if err := livePIDFD(pfd); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return read, nil
}

// VerifyTarget compares executing bytes, never the installation pathname or
// the producer's claimed SHA. Zero is missing evidence rather than a wildcard.
func (r *ExecutableRead) VerifyTarget(target contract.Digest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.exe == nil {
		return os.ErrClosed
	}
	if target == (contract.Digest{}) || r.Original.SHA != target {
		return ErrIdentityChanged
	}
	return nil
}

func (r *ExecutableRead) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var err error
	if r.exe != nil {
		err = errors.Join(err, r.exe.Close())
		r.exe = nil
	}
	if r.pidfd != nil {
		err = errors.Join(err, r.pidfd.Close())
		r.pidfd = nil
	}
	return err
}

type pendingIdentity struct {
	read *ExecutableRead
	raw  contract.RawRef
}
type kernelSnapshotter struct {
	mu       sync.Mutex
	root     string
	sources  map[contract.Key]SampleSource
	pending  map[contract.Key]pendingIdentity
	busy     map[contract.Key]bool
	ctx      context.Context
	cancel   context.CancelFunc
	joined   chan struct{}
	closed   bool
	active   int
	closeErr error
}

// NewSnapshotter preserves the frozen factory. BindSnapshotSource supplies the
// local, authenticated producer connection after construction; no PID is guessed
// by scanning similarly named processes, units, or executable paths.
func NewSnapshotter(profile contract.Profile) (contract.Snapshotter, error) {
	if !filepath.IsAbs(profile.EvidenceRoot) || len(profile.Required) == 0 {
		return nil, errors.New("fullstack: absolute evidence root and required population needed")
	}
	sources := make(map[contract.Key]SampleSource, len(profile.Required))
	for _, key := range profile.Required {
		if _, exists := sources[key]; exists {
			return nil, errors.New("fullstack: duplicate required runtime")
		}
		if _, err := contract.EncodeJSON("key", key); err != nil {
			return nil, err
		}
		sources[key] = nil
	}
	// ctx-root: snapshotter lifetime ends at Close, independently of one query.
	ctx, cancel := context.WithCancel(context.Background())
	return &kernelSnapshotter{root: profile.EvidenceRoot, sources: sources,
		pending: make(map[contract.Key]pendingIdentity), busy: make(map[contract.Key]bool),
		ctx: ctx, cancel: cancel, joined: make(chan struct{})}, nil
}

func BindSnapshotSource(reader contract.Snapshotter, key contract.Key, source SampleSource) error {
	r, ok := reader.(*kernelSnapshotter)
	if !ok || source == nil {
		return errors.New("fullstack: invalid kernel snapshot binding")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return os.ErrClosed
	}
	previous, required := r.sources[key]
	if !required || previous != nil {
		return errors.New("fullstack: runtime is unrequired or already bound")
	}
	r.sources[key] = source
	return nil
}

func (r *kernelSnapshotter) begin(ctx context.Context, key contract.Key) (context.Context, SampleSource, func(), error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, nil, nil, os.ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	source, exists := r.sources[key]
	if !exists || source == nil {
		return nil, nil, nil, errors.New("fullstack: no bound product source")
	}
	if r.busy[key] {
		return nil, nil, nil, errors.New("fullstack: concurrent runtime sampling")
	}
	r.busy[key], r.active = true, r.active+1
	call, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(r.ctx, cancel)
	finish := func() {
		stop()
		cancel()
		r.mu.Lock()
		defer r.mu.Unlock()
		delete(r.busy, key)
		r.active--
		if r.closed && r.active == 0 {
			close(r.joined)
		}
	}
	return call, source, finish, nil
}

// identityEvidence keeps kernel fields and the original product sample in one
// immutable raw record. The latter's RawRef still points to its own native
// schema; independent evidence must not erase that source or its unknown bits.
type executableEvidence struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	SHA    string `json:"sha"`
}

// Disk evidence uses the same digest representation as the contract codec.
// A Go byte array is an in-memory identity, not a second disk format.
func encodeExecutableIdentity(identity ExecutableIdentity) executableEvidence {
	return executableEvidence{Device: identity.Device, Inode: identity.Inode, SHA: hex.EncodeToString(identity.SHA[:])}
}

type identityEvidence struct {
	Schema   string             `json:"schema"`
	Before   json.RawMessage    `json:"before"`
	After    json.RawMessage    `json:"after"`
	Original executableEvidence `json:"original"`
	Current  executableEvidence `json:"current"`
	Product  json.RawMessage    `json:"product"`
}

func (r *kernelSnapshotter) Sample(ctx context.Context, key contract.Key, nonce string) (_ contract.Sample, err error) {
	if nonce == "" {
		return contract.Sample{}, errors.New("fullstack: missing query nonce")
	}
	call, source, finish, err := r.begin(ctx, key)
	if err != nil {
		return contract.Sample{}, err
	}
	defer finish()
	r.mu.Lock()
	_, held := r.pending[key]
	r.mu.Unlock()
	if held {
		return contract.Sample{}, errors.New("fullstack: drain previous executable witness first")
	}
	first, err := source(call, key, nonce)
	if err != nil {
		return contract.Sample{}, err
	}
	if nonce == "" || first.Stamp.QueryNonce != nonce || first.Stamp.Process.Key != key {
		return contract.Sample{}, ErrIdentityChanged
	}
	product, err := contract.EncodeJSON("sample", first)
	if err != nil {
		return contract.Sample{}, err
	}
	read, err := ReadExecutable(call, first.Stamp, func(ctx context.Context) (contract.Stamp, error) {
		next, err := source(ctx, key, nonce)
		if err == nil && (next.Stamp.Process.Key != key || next.Stamp.QueryNonce != nonce ||
			next.ExeSHA != first.ExeSHA || first.Complete && !next.Complete) {
			err = ErrIdentityChanged
		}
		return next.Stamp, err
	})
	if err != nil {
		return contract.Sample{}, err
	}
	owned := true
	defer func() {
		if owned {
			err = errors.Join(err, read.Close())
		}
	}()
	if err := read.VerifyTarget(first.ExeSHA); err != nil {
		return contract.Sample{}, err
	}
	before, err := contract.EncodeJSON("stamp", read.Before)
	if err != nil {
		return contract.Sample{}, err
	}
	after, err := contract.EncodeJSON("stamp", read.After)
	if err != nil {
		return contract.Sample{}, err
	}
	raw, err := r.archive(identityEvidence{Schema: "fullstack-kernel-identity-v1", Before: before,
		After: after, Original: encodeExecutableIdentity(read.Original), Current: encodeExecutableIdentity(read.Current), Product: product})
	if err != nil {
		return contract.Sample{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed || call.Err() != nil {
		cause := call.Err()
		if r.closed {
			cause = os.ErrClosed
		}
		return contract.Sample{}, errors.Join(cause, os.Remove(raw.Path))
	}
	r.pending[key] = pendingIdentity{read: read, raw: raw}
	owned = false
	first.ExeSHA, first.Raw = read.Original.SHA, raw
	return first, nil
}

func (r *kernelSnapshotter) archive(value identityEvidence) (contract.RawRef, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return contract.RawRef{}, err
	}
	file, err := os.CreateTemp(r.root, "kernel-identity-*.json")
	if err != nil {
		return contract.RawRef{}, err
	}
	_, err = file.Write(data)
	err = errors.Join(err, file.Sync(), file.Close())
	if err != nil {
		_ = os.Remove(file.Name())
		return contract.RawRef{}, err
	}
	return contract.RawRef{Path: file.Name(), SHA: sha256.Sum256(data), Bytes: int64(len(data)), Schema: value.Schema}, nil
}

// Drain releases the executable FD after its associated judgment. At most one
// witness per runtime can remain held; a whole rollout cannot accumulate images.
func (r *kernelSnapshotter) Drain(ctx context.Context, key contract.Key, nonce string) (contract.RawRef, error) {
	call, _, finish, err := r.begin(ctx, key)
	if err != nil {
		return contract.RawRef{}, err
	}
	defer finish()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := call.Err(); err != nil {
		return contract.RawRef{}, err
	}
	entry, exists := r.pending[key]
	if !exists || nonce == "" || entry.read.Before.QueryNonce != nonce {
		return contract.RawRef{}, errors.New("fullstack: no matching executable witness")
	}
	delete(r.pending, key)
	return entry.raw, entry.read.Close()
}

func (r *kernelSnapshotter) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return r.closeErr
	}
	r.closed = true
	r.cancel()
	for key, entry := range r.pending {
		r.closeErr = errors.Join(r.closeErr, entry.read.Close())
		delete(r.pending, key)
	}
	if r.active == 0 {
		close(r.joined)
	}
	return r.closeErr
}

func (r *kernelSnapshotter) Join(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.joined:
		r.mu.Lock()
		defer r.mu.Unlock()
		return r.closeErr
	}
}

var _ contract.ReaderFactory = NewSnapshotter
