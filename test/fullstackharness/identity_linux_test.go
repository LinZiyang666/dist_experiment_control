// These tests run real supervised Linux processes, same-PID exec, pidfds and
// KCMP_FILE. Only the stamp producer is a fixture; no product rollout or
// deployment continuity is certified by this kernel-reader test package.
package fullstackharness

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"golang.org/x/sys/unix"
)

const identityMode = "TETHER_FULLSTACK_IDENTITY_CHILD"

// origin: hot-update S10 plan §2.1 / P04
func TestFullstackExecutableReadRejectsExecABA(t *testing.T) {
	if os.Getenv(identityMode) == "1" {
		runIdentityChild(t)
		return
	}
	g, f := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseExecutableRead(t)
	}
	testharness.AssertNoGoroutineLeak(t, "executable sample lifecycle", g)
	testharness.AssertNoFDLeak(t, "executable sample lifecycle", f, 0)
}

// Deferred cleanup reports failures alongside the original assertion instead
// of hiding ownership errors on an early return.
// origin: hot-update S10 plan §4.2 / LINT-s10
func closeIdentityFixture(t *testing.T, closeResource func() error) {
	t.Helper()
	if err := closeResource(); err != nil {
		t.Errorf("identity fixture close: %v", err)
	}
}

func fixtureKey() contract.Key {
	return contract.Key{Role: "fixture", Host: "localhost", Unit: "kernel-reader", Domain: "identity", UID: uint32(os.Getuid())}
}

func fixtureStamp(t *testing.T) contract.Stamp {
	t.Helper()
	p, err := ReadKernelProcess(fixtureKey(), os.Getpid(), "kernel-fixture")
	if err != nil {
		t.Fatal(err)
	}
	return contract.Stamp{Process: p, TxID: "fixture-tx", CutID: "fixture-cut-1", Direction: "forward",
		QueryNonce: "fixture-query", ScopeDigest: "fixture-scope", Generation: 1, Revision: 1, Sequence: 1, AtBootNS: 1}
}

type identityChild struct {
	t                *testing.T
	input, output    *os.File
	lines            *bufio.Reader
	wait             func() error
	once             sync.Once
	last             contract.Sample
	a, b             string
	digestA, digestB contract.Digest
	root             string
}

func startIdentityChild(ctx context.Context, t *testing.T) *identityChild {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	a, b := filepath.Join(root, "image-a"), filepath.Join(root, "image-b")
	if err := os.WriteFile(a, data, 0700); err != nil {
		t.Fatal(err)
	}
	variant := append(append([]byte(nil), data...), []byte("\nindependent kernel image B\n")...)
	if err := os.WriteFile(b, variant, 0700); err != nil {
		t.Fatal(err)
	}
	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		_ = inR.Close()
		_ = inW.Close()
		t.Fatal(err)
	}
	cmd := exec.CommandContext(ctx, a, "-test.run=^TestFullstackExecutableReadRejectsExecABA$", "-test.count=1")
	cmd.Env = append(identityEnvironment(), identityMode+"=1", "TETHER_FULLSTACK_IDENTITY_A="+a,
		"TETHER_FULLSTACK_IDENTITY_B="+b, "TETHER_FULLSTACK_IDENTITY_GENERATION=1")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = inR, outW, os.Stderr
	wait, err := testharness.StartProcess(cmd)
	_ = inR.Close()
	_ = outW.Close()
	if err != nil {
		_ = inW.Close()
		_ = outR.Close()
		t.Fatal(err)
	}
	c := &identityChild{t: t, input: inW, output: outR, lines: bufio.NewReader(outR), wait: wait,
		a: a, b: b, digestA: sha256.Sum256(data), digestB: sha256.Sum256(variant), root: root}
	t.Cleanup(func() { c.stop() })
	c.receive()
	if c.digestA == c.digestB || c.last.ExeSHA != c.digestA {
		t.Fatal("executing image fixture lacks distinct bytes")
	}
	return c
}

func identityEnvironment() []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "TETHER_FULLSTACK_IDENTITY_") {
			env = append(env, entry)
		}
	}
	return env
}

func (c *identityChild) receive() contract.Sample {
	c.t.Helper()
	if err := c.output.SetReadDeadline(time.Now().Add(15 * time.Second)); err != nil {
		c.t.Fatal(err)
	}
	line, err := c.lines.ReadBytes('\n')
	if err != nil {
		c.t.Fatalf("identity child receipt: %v", err)
	}
	if err := contract.DecodeJSON("sample", bytes.TrimSpace(line), &c.last); err != nil {
		c.t.Fatal(err)
	}
	file, err := os.CreateTemp(c.root, "producer-*.json")
	if err != nil {
		c.t.Fatal(err)
	}
	_, writeErr := file.Write(line)
	if err := errors.Join(writeErr, file.Close()); err != nil {
		c.t.Fatal(err)
	}
	c.last.Raw = contract.RawRef{Path: file.Name(), SHA: sha256.Sum256(line), Bytes: int64(len(line)), Schema: "sample"}
	return c.last
}

func (c *identityChild) sample(ctx context.Context, _ contract.Key, nonce string) (contract.Sample, error) {
	if err := ctx.Err(); err != nil {
		return contract.Sample{}, err
	}
	if _, err := fmt.Fprintln(c.input, "sample "+nonce); err != nil {
		return contract.Sample{}, err
	}
	return c.receive(), nil
}

func (c *identityChild) change(image string) {
	c.t.Helper()
	old := c.last.Stamp
	if _, err := fmt.Fprintln(c.input, "exec "+image); err != nil {
		c.t.Fatal(err)
	}
	c.receive()
	if c.last.Stamp.Process != old.Process || c.last.Stamp.Generation != old.Generation+1 || c.last.Stamp.CutID == old.CutID {
		c.t.Fatalf("fixture did not perform same-PID exec: before=%+v after=%+v", old, c.last.Stamp)
	}
}

func (c *identityChild) stop() {
	c.once.Do(func() {
		_ = c.input.Close()
		if err := c.wait(); err != nil {
			c.t.Errorf("identity child/supervisor wait: %v", err)
		}
		if err := c.output.Close(); err != nil {
			c.t.Errorf("identity output close: %v", err)
		}
	})
}

func exerciseExecutableRead(t *testing.T) {
	t.Helper()
	// ctx-root: supervised executable reader fixture lifetime.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	c := startIdentityChild(ctx, t)
	defer c.stop()
	before := c.last.Stamp
	stable := func(ctx context.Context) (contract.Stamp, error) {
		sample, err := c.sample(ctx, before.Process.Key, before.QueryNonce)
		return sample.Stamp, err
	}
	read, err := ReadExecutable(ctx, before, stable)
	if err != nil {
		t.Fatal(err)
	}
	if read.Original != read.Current || read.Original.Inode == 0 || read.VerifyTarget(c.digestA) != nil {
		t.Fatal("independent executable identity missing")
	}
	remote, err := NewOFDWitness(before, 0)
	if err != nil {
		t.Fatalf("SETUP: remote pidfd_getfd witness required: %v", err)
	}
	defer closeIdentityFixture(t, remote.Close)
	if _, err := remote.Check(ctx, before, 0); err != nil {
		t.Fatal(err)
	}
	if err := read.VerifyTarget(c.digestB); !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("M07/M09 claimed target accepted with old executing bytes: %v", err)
	}
	if err := read.VerifyTarget(contract.Digest{}); err == nil {
		t.Fatal("zero target digest accepted")
	}
	for _, fd := range []*os.File{read.exe, read.pidfd} {
		flags, err := unix.FcntlInt(fd.Fd(), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			t.Fatalf("identity witness lacks CLOEXEC: %v", err)
		}
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	if err := read.VerifyTarget(c.digestA); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed executable witness accepted: %v", err)
	}
	// Same bytes, inode, PID and ticks after A -> B -> A must still be red.
	before = c.last.Stamp
	_, err = ReadExecutable(ctx, before, func(ctx context.Context) (contract.Stamp, error) {
		c.change("b")
		c.change("a")
		return c.last.Stamp, nil
	})
	if !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("same-PID exec ABA accepted: %v", err)
	}
	// Even a lying stable stamp cannot cover up a different current executable.
	before = c.last.Stamp
	_, err = ReadExecutable(ctx, before, func(context.Context) (contract.Stamp, error) { c.change("b"); return before, nil })
	if !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("current exe reopen/hash was not verified: %v", err)
	}
	c.change("a")
	before = c.last.Stamp
	if _, err := remote.Check(ctx, before, 0); err != nil {
		t.Fatalf("original pipe OFD lost across exec: %v", err)
	}
	for _, tc := range []struct {
		name   string
		change func(*contract.Stamp)
	}{
		{"generation", func(s *contract.Stamp) { s.Generation++ }},
		{"cut", func(s *contract.Stamp) { s.CutID += "-changed" }},
		{"namespace", func(s *contract.Stamp) { s.Process.PIDNS += "-changed" }},
		{"ticks", func(s *contract.Stamp) { s.Process.StartTicks++ }},
		{"instance", func(s *contract.Stamp) { s.Process.InstanceID += "-changed" }},
		{"nonce", func(s *contract.Stamp) { s.QueryNonce += "-replayed" }},
		{"revision", func(s *contract.Stamp) { s.Revision++ }},
		{"sequence", func(s *contract.Stamp) { s.Sequence = 0 }},
	} {
		changed := before
		tc.change(&changed)
		if got, err := ReadExecutable(ctx, before, func(context.Context) (contract.Stamp, error) { return changed, nil }); err == nil {
			_ = got.Close()
			t.Fatalf("changed %s accepted", tc.name)
		}
	}
	// A cut beginning during the second hash must be caught by the final read.
	calls := 0
	_, err = ReadExecutable(ctx, before, func(context.Context) (contract.Stamp, error) {
		calls++
		stamp := before
		if calls == 2 {
			stamp.CutID += "-late"
		}
		return stamp, nil
	})
	if !errors.Is(err, ErrIdentityChanged) || calls != 2 {
		t.Fatalf("late cut not rejected: calls=%d err=%v", calls, err)
	}
	bad := before
	bad.Process.StartTicks++
	if _, err := ReadExecutable(ctx, bad, func(context.Context) (contract.Stamp, error) { return bad, nil }); err == nil {
		t.Fatal("kernel start ticks not checked")
	}
	bad = before
	bad.Process.Key.UID++
	if _, err := ReadExecutable(ctx, bad, func(context.Context) (contract.Stamp, error) { return bad, nil }); err == nil {
		t.Fatal("kernel UID not checked")
	}
	exerciseSnapshotter(ctx, t, c)
	// Replace the actual installation path while its old unlinked inode executes.
	newPath := filepath.Join(c.root, "installed-new")
	data, err := os.ReadFile(c.b)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, data, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(newPath, c.a); err != nil {
		t.Fatal(err)
	}
	before = c.last.Stamp
	read, err = ReadExecutable(ctx, before, stable)
	if err != nil {
		t.Fatal(err)
	}
	if read.Original.SHA != c.digestA || read.VerifyTarget(c.digestB) == nil {
		t.Fatal("disk-only replacement became executing proof")
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	c.stop()
	if _, err := ReadExecutable(ctx, before, stable); err == nil {
		t.Fatal("dead owner signed as current")
	}
}

func exerciseSnapshotter(ctx context.Context, t *testing.T, c *identityChild) {
	t.Helper()
	key := c.last.Stamp.Process.Key
	profile := contract.Profile{Required: []contract.Key{key}, EvidenceRoot: c.root}
	reader, err := NewSnapshotter(profile)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIdentityFixture(t, reader.Close)
	if _, err := reader.Sample(ctx, key, "unbound"); err == nil {
		t.Fatal("unbound snapshotter succeeded")
	}
	if err := BindSnapshotSource(reader, key, c.sample); err != nil {
		t.Fatal(err)
	}
	if err := BindSnapshotSource(reader, key, c.sample); err == nil {
		t.Fatal("runtime source silently replaced")
	}
	if _, err := reader.Sample(ctx, fixtureKey(), ""); err == nil {
		t.Fatal("empty query nonce accepted")
	}
	sample, err := reader.Sample(ctx, key, "fresh-query")
	if err != nil {
		t.Fatal(err)
	}
	if sample.ExeSHA != c.digestA || !sample.Complete || sample.Goroutines <= 0 {
		t.Fatal("lost native counters or independent hash")
	}
	kernel, ok := reader.(*kernelSnapshotter)
	if !ok {
		t.Fatal("snapshotter does not provide kernel identity witnesses")
	}
	entry := kernel.pending[key]
	exeFD, pidFD := int(entry.read.exe.Fd()), int(entry.read.pidfd.Fd())
	if err := entry.read.VerifyTarget(c.digestA); err != nil {
		t.Fatalf("witness released before judgment: %v", err)
	}
	if _, err := reader.Sample(ctx, key, "new-query"); err == nil {
		t.Fatal("old image FD accumulated across sampling")
	}
	if _, err := reader.Drain(ctx, key, "wrong-query"); err == nil {
		t.Fatal("wrong nonce released witness")
	}
	raw, err := reader.Drain(ctx, key, "fresh-query")
	if err != nil || raw != sample.Raw {
		t.Fatalf("drain receipt: %+v %v", raw, err)
	}
	for _, fd := range []int{exeFD, pidFD} {
		if _, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0); !errors.Is(err, unix.EBADF) {
			t.Fatalf("drained identity FD remained open: %v", err)
		}
	}
	data, err := os.ReadFile(raw.Path)
	if err != nil || sha256.Sum256(data) != raw.SHA || int64(len(data)) != raw.Bytes {
		t.Fatalf("raw digest/length: %v", err)
	}
	var evidence identityEvidence
	if err := json.Unmarshal(data, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.Original.SHA != hex.EncodeToString(c.digestA[:]) || evidence.Current != evidence.Original || len(evidence.Product) == 0 {
		t.Fatal("independent raw fields lost")
	}
	t.Logf("kernel witness: pid=%d ticks=%d generation=%d cut=%s dev=%d ino=%d sha=%s raw=%x", sample.Stamp.Process.PID,
		sample.Stamp.Process.StartTicks, sample.Stamp.Generation, sample.Stamp.CutID, evidence.Original.Device, evidence.Original.Inode, evidence.Original.SHA, raw.SHA)
	t.Logf("kernel raw: %s", data)
	if _, err := reader.Sample(ctx, key, "after-drain"); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Join(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Sample(ctx, key, "after-close"); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("closed reader sampled: %v", err)
	}
	// A reported target SHA must not overwrite the independently read bytes.
	liar, err := NewSnapshotter(profile)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIdentityFixture(t, liar.Close)
	err = BindSnapshotSource(liar, key, func(ctx context.Context, key contract.Key, nonce string) (contract.Sample, error) {
		s, err := c.sample(ctx, key, nonce)
		s.ExeSHA = c.digestB
		return s, err
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := liar.Sample(ctx, key, "forged-sha"); !errors.Is(err, ErrIdentityChanged) {
		t.Fatalf("M09 forged product SHA signed: %v", err)
	}
	if err := liar.Close(); err != nil {
		t.Fatal(err)
	}
	if err := liar.Join(ctx); err != nil {
		t.Fatal(err)
	}
	profile.EvidenceRoot = filepath.Join(c.root, "missing-root")
	partial, err := NewSnapshotter(profile)
	if err != nil {
		t.Fatal(err)
	}
	if err := BindSnapshotSource(partial, key, c.sample); err != nil {
		t.Fatal(err)
	}
	if _, err := partial.Sample(ctx, key, "archive-failure"); err == nil {
		t.Fatal("missing raw archive signed")
	}
	if err := partial.Close(); err != nil {
		t.Fatal(err)
	}
	if err := partial.Join(ctx); err != nil {
		t.Fatal(err)
	}
	// Close cancels a query already in progress, and Join observes its exit.
	blocked, err := NewSnapshotter(contract.Profile{Required: []contract.Key{key}, EvidenceRoot: c.root})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	if err := BindSnapshotSource(blocked, key, func(ctx context.Context, _ contract.Key, _ string) (contract.Sample, error) {
		close(entered)
		<-ctx.Done()
		return contract.Sample{}, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := blocked.Sample(ctx, key, "cancel-inflight"); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := blocked.Close(); err != nil {
		t.Fatal(err)
	}
	if err := blocked.Join(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("inflight query cancellation: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func runIdentityChild(t *testing.T) {
	generation, err := strconv.ParseUint(os.Getenv("TETHER_FULLSTACK_IDENTITY_GENERATION"), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	// A deliberately awkward comm tests the /proc/stat start_ticks parser.
	if err := os.WriteFile("/proc/thread-self/comm", []byte("name ) with ("), 0600); err != nil {
		t.Fatal(err)
	}
	stamp := fixtureStamp(t)
	stamp.Generation, stamp.CutID = generation, fmt.Sprintf("fixture-cut-%d", generation)
	exe, err := os.Open("/proc/self/exe")
	if err != nil {
		t.Fatal(err)
	}
	// ctx-root: child fixture's read-only startup hash.
	identity, err := hashExecutable(context.Background(), exe)
	if err := errors.Join(err, exe.Close()); err != nil {
		t.Fatal(err)
	}
	emit := func(nonce string) {
		stamp.QueryNonce = nonce
		stamp.Sequence++
		var now unix.Timespec
		if err := unix.ClockGettime(unix.CLOCK_BOOTTIME, &now); err != nil {
			t.Fatal(err)
		}
		stamp.AtBootNS = now.Nano()
		sample := contract.Sample{Stamp: stamp, ExeSHA: identity.SHA, Goroutines: int64(runtime.NumGoroutine()),
			FDs: int64(testharness.FDCount()), Complete: true, Raw: contract.RawRef{Path: "fixture-wire", Schema: "sample"},
			Resources: []contract.Resource{}, Consumers: []string{}, Temps: []string{}, Locks: []string{}, OldImages: []string{}}
		data, err := contract.EncodeJSON("sample", sample)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fmt.Fprintln(os.Stdout, string(data)); err != nil {
			t.Fatal(err)
		}
	}
	emit("fixture-query")
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		words := strings.Fields(scanner.Text())
		if len(words) != 2 {
			t.Fatal("invalid child command")
		}
		switch words[0] {
		case "sample":
			emit(words[1])
		case "exec":
			var path string
			switch words[1] {
			case "a":
				path = os.Getenv("TETHER_FULLSTACK_IDENTITY_A")
			case "b":
				path = os.Getenv("TETHER_FULLSTACK_IDENTITY_B")
			default:
				t.Fatal("invalid child image")
			}
			env := append(identityEnvironment(), identityMode+"=1", "TETHER_FULLSTACK_IDENTITY_A="+os.Getenv("TETHER_FULLSTACK_IDENTITY_A"),
				"TETHER_FULLSTACK_IDENTITY_B="+os.Getenv("TETHER_FULLSTACK_IDENTITY_B"), "TETHER_FULLSTACK_IDENTITY_GENERATION="+strconv.FormatUint(generation+1, 10))
			if err := syscall.Exec(path, []string{path, "-test.run=^TestFullstackExecutableReadRejectsExecABA$", "-test.count=1"}, env); err != nil {
				t.Fatal(err)
			}
		default:
			t.Fatal("unknown child command")
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
}
