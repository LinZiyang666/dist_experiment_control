// Package transferharness shares real-binary launch and raw receipts between
// S7 integration tests. It never imports another package's private test helper.
// Process creation belongs exclusively to the shared FIX-ORPHAN supervisor.
package transferharness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrSupervisionUnavailable prevents a bare cmd.Start/Pdeathsig fallback while
// FIX-ORPHAN is absent from the candidate. A process leak census alone cannot
// clean descendants after SIGKILL of the test process.
// origin: hot-update S7 plan lead L16 / P29.
var ErrSupervisionUnavailable = errors.New("transfer harness: shared process supervisor unavailable")

// Exit is the original native process result. Retryable business rc75 and drill
// INCOMPLETE rc4 stay distinct; callers must never flatten either into success.
type Exit struct {
	NativeRC int
	Err      error
}

// Child is returned by the shared supervisor, which owns Wait and descendant
// collection. Done sends exactly one native Exit and Stop joins all descendants
// using PID plus start ticks. No launch is allowed without both responsibilities.
type Child struct {
	PID  int
	Done <-chan Exit
	Stop func() error
}

// Supervisor is the narrow adapter P32/P33 use for FIX-ORPHAN's shared helper.
// That helper must supervise grandchildren when the enclosing test is killed.
type Supervisor interface {
	Launch(*exec.Cmd) (Child, error)
}

// SharedSupervisor installs the landed FIX-ORPHAN adapter by default. An unset
// factory is an explicit dependency failure, never a launcher.
// Packages may pass a Supervisor directly to New instead of using this factory.
// origin: S7 internal review rv-lifecycle F7
var SharedSupervisor = func(*testing.T) Supervisor { return sharedProcessSupervisor{} }

type sharedProcessSupervisor struct{}

// Launch gives the shared supervisor exclusive wait ownership. In particular,
// Cmd.Wait would wait on the wrong parent after the external subreaper starts.
// The shared lifeline, rather than a test Cleanup or Pdeathsig, handles fatal
// runner exits; Stop still joins the complete tree on ordinary teardown.
// origin: S7 internal review rv-lifecycle F7
func (sharedProcessSupervisor) Launch(cmd *exec.Cmd) (Child, error) {
	wait, err := testsupervisor.StartProcess(cmd)
	if err != nil {
		return Child{}, err
	}
	done, joined := make(chan Exit, 1), make(chan struct{})
	go func() {
		err := wait()
		rc := -1
		var exit interface{ ExitCode() int }
		switch {
		case cmd.ProcessState != nil:
			rc = cmd.ProcessState.ExitCode()
		case errors.As(err, &exit):
			rc = exit.ExitCode()
		case err == nil:
			rc = 0
		}
		done <- Exit{NativeRC: rc, Err: err}
		close(done)
		close(joined)
	}()
	return Child{PID: cmd.Process.Pid, Done: done, Stop: func() error {
		// Process owns the kernel handle, so a concurrent Wait or PID reuse
		// cannot redirect this signal to an unrelated process.
		err := cmd.Process.Kill()
		if errors.Is(err, os.ErrProcessDone) {
			err = nil
		}
		<-joined
		return err
	}}, nil
}

// Spec identifies one immutable artifact and its private test environment.
// ExtraFiles may contain private sealed custody; they are never serialized.
type Spec struct {
	Binary      string
	TransferID  string
	Args        []string
	Env         []string
	ArtifactSHA string
	ExtraFiles  []*os.File
}

// Receipt records the original process identity, bytes and native outcome.
// Output is kept as raw files; the harness does not reconstruct product events.
type Receipt struct {
	TransferID   string `json:"transfer_id"`
	PID          int    `json:"pid"`
	ProcessStart uint64 `json:"process_start"`
	BootID       string `json:"boot_id"`
	ArtifactSHA  string `json:"artifact_sha"`
	RunningSHA   string `json:"running_sha"`
	StdoutPath   string `json:"stdout_path"`
	StderrPath   string `json:"stderr_path"`
	NativeRC     int    `json:"native_rc"`
	WallNS       int64  `json:"wall_ns"`
}

// Harness owns only fixtures beneath Root. Supervisor remains responsible for
// test-death cleanup; GuardProcessDirectory is an independent teardown census.
type Harness struct {
	Root       string
	supervisor Supervisor
	mu         sync.Mutex
	processes  []*Process
	closed     bool
}

// New registers the census before Stop, so LIFO cleanup inspects final teardown.
func New(t *testing.T, supervisor Supervisor) *Harness {
	t.Helper()
	root := testharness.GuardProcessDirectory(t, t.TempDir())
	h := &Harness{Root: root, supervisor: supervisor}
	t.Cleanup(func() {
		if err := h.Close(); err != nil {
			t.Errorf("transfer harness close: %v", err)
		}
	})
	return h
}

// Process has one asynchronous native outcome and idempotent supervised Stop.
type Process struct {
	child    Child
	stdout   *os.File
	stderr   *os.File
	started  time.Time
	done     chan struct{}
	mu       sync.Mutex
	receipt  Receipt
	exitErr  error
	stopOnce sync.Once
	stopErr  error
}

// Start launches only an absolute, hash-checked artifact through Supervisor.
// The caller's ctx owns the test command; it is not a product worker context.
func (h *Harness) Start(ctx context.Context, spec Spec) (*Process, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, errors.New("transfer harness: closed")
	}
	if h.supervisor == nil {
		return nil, ErrSupervisionUnavailable
	}
	if err := validateSpec(h.Root, spec); err != nil {
		return nil, err
	}
	index := len(h.processes)
	out, err := os.CreateTemp(h.Root, fmt.Sprintf("process-%d-stdout-", index))
	if err != nil {
		return nil, err
	}
	errout, err := os.CreateTemp(h.Root, fmt.Sprintf("process-%d-stderr-", index))
	if err != nil {
		_ = out.Close()
		return nil, err
	}
	cmd := exec.CommandContext(ctx, spec.Binary, spec.Args...)
	cmd.Dir, cmd.Stdout, cmd.Stderr, cmd.ExtraFiles = h.Root, out, errout, spec.ExtraFiles
	cmd.Env = privateEnvironment(h.Root, spec.Env)
	// Inherit GOFLAGS verbatim: mutation overlays must also reach nested builders.
	// No helper may replace the supervisor with direct cmd.Start or Pdeathsig.
	child, err := h.supervisor.Launch(cmd)
	if err != nil {
		return nil, errors.Join(err, out.Close(), errout.Close())
	}
	if child.PID <= 0 || child.Done == nil || child.Stop == nil {
		var stopErr error
		if child.Stop != nil {
			stopErr = child.Stop()
		}
		return nil, errors.Join(errors.New("transfer harness: incomplete supervisor custody"), stopErr,
			out.Close(), errout.Close())
	}
	p := &Process{child: child, stdout: out, stderr: errout, started: time.Now(), done: make(chan struct{}),
		receipt: Receipt{TransferID: spec.TransferID, PID: child.PID, ArtifactSHA: spec.ArtifactSHA, StdoutPath: out.Name(),
			StderrPath: errout.Name(), NativeRC: -1}}
	h.processes = append(h.processes, p)
	go p.collect()
	return p, nil
}

func privateEnvironment(root string, extra []string) []string {
	var env []string
	for _, entry := range os.Environ() {
		if strings.HasPrefix(entry, "TETHER_HOME=") || strings.HasPrefix(entry, "TETHER_TEST_DRILL_") {
			continue
		}
		env = append(env, entry)
	}
	env = append(env, "TETHER_HOME="+filepath.Join(root, "home"))
	return append(env, extra...)
}

func validateSpec(root string, spec Spec) error {
	if !filepath.IsAbs(spec.Binary) || spec.ArtifactSHA == "" {
		return errors.New("transfer harness: absolute artifact and SHA required")
	}
	resolved, err := filepath.EvalSymlinks(spec.Binary)
	if err != nil {
		return err
	}
	// Executables come from the private fixture, never the installed binary.
	relative, err := filepath.Rel(root, resolved)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("transfer harness: artifact outside private fixture")
	}
	digest, err := FileSHA(resolved)
	if err != nil {
		return err
	}
	if digest != spec.ArtifactSHA {
		return errors.New("transfer harness: artifact SHA mismatch")
	}
	for _, entry := range spec.Env {
		if strings.HasPrefix(entry, "TETHER_HOME=") {
			home := strings.TrimPrefix(entry, "TETHER_HOME=")
			rel, err := filepath.Rel(root, home)
			if err != nil || !filepath.IsAbs(home) || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
				return errors.New("transfer harness: home outside private fixture")
			}
		}
		if strings.Contains(entry, "/.tether") && !strings.Contains(entry, root+string(filepath.Separator)) {
			return errors.New("transfer harness: production home forbidden")
		}
	}
	for _, arg := range spec.Args {
		if strings.Contains(arg, "/.tether") && !strings.Contains(arg, root+string(filepath.Separator)) {
			return errors.New("transfer harness: production home argument forbidden")
		}
	}
	return nil
}

func (p *Process) collect() {
	exit, ok := <-p.child.Done
	if !ok {
		exit = Exit{NativeRC: -1, Err: errors.New("transfer harness: missing native exit")}
	}
	closeErr := errors.Join(p.stdout.Sync(), p.stderr.Sync(), p.stdout.Close(), p.stderr.Close())
	p.mu.Lock()
	p.receipt.NativeRC, p.receipt.WallNS = exit.NativeRC, time.Since(p.started).Nanoseconds()
	p.exitErr = errors.Join(exit.Err, closeErr)
	p.mu.Unlock()
	close(p.done)
}

// Wait preserves the native outcome even when it is rc75 or ASSERT-FAIL1.
func (p *Process) Wait(ctx context.Context) (Receipt, error) {
	select {
	case <-ctx.Done():
		return p.Receipt(), ctx.Err()
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.receipt, p.exitErr
	}
}

// Receipt returns a race-safe copy without consuming the process outcome.
func (p *Process) Receipt() Receipt {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.receipt
}

// SaveReceipt writes the supervisor's original native outcome to the driver's
// evidence descriptor. Python's live judge independently reads this file;
// canonical history alone cannot certify the original ctl's exit status.
func SaveReceipt(p *Process, out *os.File) error {
	select {
	case <-p.done:
	default:
		return errors.New("transfer harness: native outcome is not settled")
	}
	if err := json.NewEncoder(out).Encode(p.Receipt()); err != nil {
		return err
	}
	return out.Sync()
}

// SampleIdentity pins /proc/<pid>/exe while hashing and rejects PID reuse.
// Integration callers sample at each cut, preserving same PID/start/boot ID.
func (p *Process) SampleIdentity() (Receipt, error) {
	if runtime.GOOS != "linux" {
		return p.Receipt(), errors.New("transfer harness: procfs evidence unavailable")
	}
	base := filepath.Join("/proc", strconv.Itoa(p.child.PID))
	start, err := processStart(base)
	if err != nil {
		return p.Receipt(), err
	}
	digest, err := FileSHA(filepath.Join(base, "exe"))
	if err != nil {
		return p.Receipt(), err
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return p.Receipt(), err
	}
	again, err := processStart(base)
	if err != nil || again != start {
		return p.Receipt(), errors.New("transfer harness: PID recycled during sample")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.receipt.ProcessStart != 0 && p.receipt.ProcessStart != start {
		return p.receipt, errors.New("transfer harness: process identity changed")
	}
	p.receipt.ProcessStart, p.receipt.BootID, p.receipt.RunningSHA = start, strings.TrimSpace(string(boot)), digest
	return p.receipt, nil
}

func processStart(base string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(base, "stat"))
	if err != nil {
		return 0, err
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return 0, errors.New("transfer harness: malformed process stat")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 || fields[0] == "Z" || fields[0] == "X" {
		return 0, errors.New("transfer harness: process is not live")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

// Stop delegates descendant termination to the supervisor and joins receipts.
func (p *Process) Stop() error {
	p.stopOnce.Do(func() {
		p.stopErr = p.child.Stop()
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			p.stopErr = errors.Join(p.stopErr, errors.New("transfer harness: supervisor did not join"))
		}
	})
	return p.stopErr
}

// Close stops all launched processes in reverse order. A failure is retained.
func (h *Harness) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil
	}
	h.closed = true
	var failures []error
	for i := len(h.processes) - 1; i >= 0; i-- {
		failures = append(failures, h.processes[i].Stop())
	}
	return errors.Join(failures...)
}

// FileSHA hashes an open file once rather than trusting an on-disk path label.
func FileSHA(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	digest := sha256.New()
	_, err = io.Copy(digest, f)
	// An oracle that cannot release what it read reports that rather than a digest.
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest.Sum(nil)), nil
}

// RawMessage includes every physical sequence, including deleted holes. History
// consumers compare Data byte for byte; existence of any terminal is not proof.
type RawMessage struct {
	Sequence uint64
	Absent   bool
	Subject  string
	Data     []byte
	Headers  map[string][]string
}

// ScanStream fixes a physical end cursor. Its caller keeps the complete prior
// rows and rejects a changed stream creation timestamp between scans.
func ScanStream(ctx context.Context, stream jetstream.Stream, cursor uint64) ([]RawMessage, *jetstream.StreamInfo, error) {
	info, err := stream.Info(ctx)
	if err != nil {
		return nil, nil, err
	}
	if info.State.LastSeq < cursor {
		return nil, info, errors.New("transfer harness: stream cursor reversed")
	}
	var rows []RawMessage
	for seq := cursor + 1; seq <= info.State.LastSeq; seq++ {
		msg, err := stream.GetMsg(ctx, seq)
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			rows = append(rows, RawMessage{Sequence: seq, Absent: true})
			continue
		}
		if err != nil {
			return nil, info, err
		}
		if msg.Sequence != seq {
			return nil, info, errors.New("transfer harness: physical sequence mismatch")
		}
		rows = append(rows, RawMessage{Sequence: seq, Subject: msg.Subject,
			Data: append([]byte(nil), msg.Data...), Headers: msg.Header})
	}
	return rows, info, nil
}

// ReadObject uses the pinned SDK Get path as a separate object-format oracle.
func ReadObject(ctx context.Context, store jetstream.ObjectStore, name string) (int64, string, error) {
	reader, err := store.Get(ctx, name)
	if err != nil {
		return 0, "", err
	}
	digest := sha256.New()
	count, err := io.Copy(digest, reader)
	// Close also cancels the SDK's chunk subscription; its failure is evidence too.
	if closeErr := reader.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return count, "", err
	}
	return count, hex.EncodeToString(digest.Sum(nil)), nil
}
