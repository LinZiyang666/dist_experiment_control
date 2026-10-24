//go:build linux

// image_exec_test.go exercises actual Linux exec and kernel-owned resources.
// origin: docs/reviews/hot-update-plan.md §3.2 and §5.
// Real: subprocesses, child wait status, pipes, loopback TCP, descriptor flags.
// Stubbed: no broker, NATS, TLS, PTY, workload journal, or version compatibility.
// These tests prove the handoff primitive, not complete hot-update continuity.
// The external test package keeps the shared harness's auth/proto/xfervalue
// dependency from forming a test import cycle back into handoff.
// origin: S8 internal review FIX-VETTAGS F1
package handoff_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
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

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const (
	handoffTestModeEnv = "TETHER_HANDOFF_TEST_MODE"
	handoffTestAddrEnv = "TETHER_HANDOFF_TEST_ADDR"
	handoffTestDirEnv  = "TETHER_HANDOFF_TEST_DIR"
	handoffTestIDEnv   = "TETHER_HANDOFF_TEST_ID"
	handoffTestFDEnv   = "TETHER_HANDOFF_TEST_FD_IDENTITIES"
	handoffTestBudget  = 30 * time.Second
)

type handoffFDIdentity struct {
	Dev uint64 `json:"dev"`
	Ino uint64 `json:"ino"`
}

type handoffExecFixtureState struct {
	ParentPID int               `json:"parent_pid"`
	ChildPID  int               `json:"child_pid"`
	Nonce     string            `json:"nonce"`
	Secret    handoffFDIdentity `json:"secret"`
	Unclaimed handoffFDIdentity `json:"unclaimed"`
}

func TestExecPreservesChildAndOriginalTCPStream(t *testing.T) {
	exerciseHandoffExec(t)
}

func TestExecFailureKeepsOriginalFilesAndFlags(t *testing.T) {
	exerciseHandoffExecFailure(t)
}

func TestHandoffRepeatedExecReleasesResources(t *testing.T) {
	// Warm the netpoller and os/exec machinery before the strict FD baseline.
	exerciseHandoffExec(t)
	goroutines := runtime.NumGoroutine()
	fds := testharness.FDCount()
	for i := 0; i < 5; i++ {
		exerciseHandoffExec(t)
		exerciseHandoffExecFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "handoff exec lifecycle", goroutines)
	testharness.AssertNoFDLeak(t, "handoff exec lifecycle", fds, 0)
}

// TestHandoffExecSubprocess is invoked only in isolated helper processes.
// The before/after branch switch requires a real exec: Exec returning is fatal.
func TestHandoffExecSubprocess(t *testing.T) {
	switch os.Getenv(handoffTestModeEnv) {
	case "":
		return
	case "before":
		handoffBeforeExec(t)
	case "after":
		handoffAfterExec(t)
	case "audit":
		handoffAuditBusinessChild(t)
	default:
		t.Fatalf("unknown subprocess mode %q", os.Getenv(handoffTestModeEnv))
	}
}

func exerciseHandoffExec(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	nonce := filepath.Base(dir)
	listener, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	if err := listener.SetDeadline(time.Now().Add(handoffTestBudget)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), handoffTestBudget)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHandoffExecSubprocess$")
	cmd.Env = handoffTestEnv("before", map[string]string{
		handoffTestAddrEnv: listener.Addr().String(),
		handoffTestDirEnv:  dir,
		handoffTestIDEnv:   nonce,
	})
	// A timeout must also reap/kill the shell fixture, not leave a blocked child.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = time.Second
	var output bytes.Buffer
	cmd.Stdout, cmd.Stderr = &output, &output
	// The shared supervisor owns the whole fixture tree across same-PID exec;
	// the business child remains directly parented to the execing runtime.
	// origin: hot-update S8 plan §9.1 / L21
	processWait, err := testharness.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	var waitOnce sync.Once
	var waitErr error
	wait := func() error {
		waitOnce.Do(func() { waitErr = processWait() })
		return waitErr
	}
	defer func() {
		if cmd.ProcessState == nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
		_ = wait()
		if t.Failed() && output.Len() != 0 {
			t.Logf("handoff subprocess output:\n%s", output.String())
		}
	}()

	peer, err := listener.AcceptTCP()
	if err != nil {
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		_ = wait()
		t.Fatalf("helper did not connect: %v\n%s", err, output.String())
	}
	defer func() { _ = peer.Close() }()
	if err := peer.SetDeadline(time.Now().Add(handoffTestBudget)); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(peer)
	before := handoffReadLine(t, reader)
	fields := strings.Split(before, ":")
	if len(fields) != 4 || fields[0] != "before" || fields[1] != strconv.Itoa(cmd.Process.Pid) || fields[3] != nonce {
		t.Fatalf("invalid pre-exec identity %q, helper pid=%d", before, cmd.Process.Pid)
	}
	childPID, err := strconv.Atoi(fields[2])
	if err != nil || childPID <= 0 {
		t.Fatalf("invalid child pid in %q", before)
	}
	handoffWriteLine(t, peer, "before-ack:"+nonce)
	// The SAME accepted Conn is kept through exec; no reconnect or second accept.
	after := handoffReadLine(t, reader)
	wantAfter := fmt.Sprintf("after:%d:%d:23:%s", cmd.Process.Pid, childPID, nonce)
	if after != wantAfter {
		t.Fatalf("post-exec identity/wait status %q, want %q", after, wantAfter)
	}
	handoffWriteLine(t, peer, "after-ack:"+nonce)
	if done := handoffReadLine(t, reader); done != "done:"+nonce {
		t.Fatalf("last bytes on original connection = %q", done)
	}
	if err := wait(); err != nil {
		t.Fatalf("exec subprocess: %v\n%s", err, output.String())
	}
}

func handoffBeforeExec(t *testing.T) {
	t.Helper()
	if restored, err := handoff.Restore(); err != nil || restored != nil {
		t.Fatalf("ordinary process Restore = (%v, %v), want (nil, nil)", restored, err)
	}
	inputR, inputW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = inputR.Close(); _ = inputW.Close() }()
	outputR, outputW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outputR.Close(); _ = outputW.Close() }()
	child := exec.Command("/bin/sh", "-c", `printf 'ready:%s\n' "$$"; IFS= read -r token; printf 'continued:%s:%s\n' "$$" "$token"; exit 23`)
	child.Stdin, child.Stdout, child.Stderr = inputR, outputW, os.Stderr
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
	_ = inputR.Close()
	_ = outputW.Close()
	if got := handoffReadLine(t, bufio.NewReader(outputR)); got != fmt.Sprintf("ready:%d", child.Process.Pid) {
		t.Fatalf("child did not start under original parent: %q", got)
	}

	conn, err := net.DialTimeout("tcp4", os.Getenv(handoffTestAddrEnv), handoffTestBudget)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if err := conn.SetDeadline(time.Now().Add(handoffTestBudget)); err != nil {
		t.Fatal(err)
	}
	nonce := os.Getenv(handoffTestIDEnv)
	handoffWriteLine(t, conn, fmt.Sprintf("before:%d:%d:%s", os.Getpid(), child.Process.Pid, nonce))
	if ack := handoffReadLine(t, bufio.NewReader(conn)); ack != "before-ack:"+nonce {
		t.Fatalf("pre-exec peer ack = %q", ack)
	}
	// File duplicates the socket for the fixture; the library must not call Fd
	// on caller-owned files or silently change their shared O_NONBLOCK status.
	tcpFile, err := conn.(*net.TCPConn).File()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tcpFile.Close() }()
	_ = conn.Close()
	secret, err := os.CreateTemp(os.Getenv(handoffTestDirEnv), "not-in-manifest-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = secret.Close() }()
	handoffRequireCloexec(t, secret)
	unclaimed, err := os.CreateTemp(os.Getenv(handoffTestDirEnv), "unclaimed-resource-")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unclaimed.Close() }()
	handoffRequireCloexec(t, unclaimed)
	fixture := handoffExecFixtureState{
		ParentPID: os.Getpid(), ChildPID: child.Process.Pid, Nonce: nonce,
		Secret: handoffIdentity(t, secret), Unclaimed: handoffIdentity(t, unclaimed),
	}
	state := handoff.State{Role: "test-runtime", Instance: nonce, Generation: 7, Data: handoffJSON(t, fixture)}
	prepared, err := handoff.Prepare(state, []handoff.Resource{
		{ID: "child", Kind: "process", Data: json.RawMessage(`{"stdio":"pipe"}`), Files: map[string]*os.File{"stdin": inputW, "stdout": outputR}},
		{ID: "flow", Kind: "tcp", Data: json.RawMessage(`{"duplex":true}`), Files: map[string]*os.File{"socket": tcpFile}},
		{ID: "unclaimed", Kind: "file", Files: map[string]*os.File{"file": unclaimed}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	// Originals are deliberately kept open and CLOEXEC. Only Prepare's copies
	// may cross exec; the unrelated secret is not given to Prepare at all.
	handoffRequireCloexec(t, inputW)
	handoffRequireCloexec(t, outputR)
	handoffRequireCloexec(t, tcpFile)
	handoffRequireCloexec(t, secret)
	args := []string{os.Args[0], "-test.run=^TestHandoffExecSubprocess$"}
	if err := prepared.Exec(os.Args[0], args, handoffTestEnv("after", nil)); err != nil {
		t.Fatalf("real Exec failed: %v", err)
	}
	t.Fatal("successful Exec returned to the original runtime")
}

func handoffAfterExec(t *testing.T) {
	t.Helper()
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatalf("exec import = (%v, %v), want a state", restored, err)
	}
	defer func() { _ = restored.Close() }()
	var fixture handoffExecFixtureState
	if err := json.Unmarshal(restored.State.Data, &fixture); err != nil {
		t.Fatal(err)
	}
	if restored.State.Role != "test-runtime" || restored.State.Instance != fixture.Nonce || restored.State.Generation != 7 {
		t.Fatalf("state identity changed: %+v", restored.State)
	}
	if os.Getpid() != fixture.ParentPID {
		t.Fatalf("parent changed across exec: got %d want %d", os.Getpid(), fixture.ParentPID)
	}
	if again, err := handoff.Restore(); err != nil || again != nil {
		t.Fatalf("private handoff environment not consumed: second Restore=(%v, %v)", again, err)
	}
	handoffAssertIdentitiesAbsent(t, []handoffFDIdentity{fixture.Secret})
	if len(restored.Resources) != 3 {
		t.Fatalf("resource metadata count=%d, want 3", len(restored.Resources))
	}
	for _, resource := range restored.Resources {
		if len(resource.Files) != 0 {
			t.Fatalf("metadata exposes a second file owner for %q", resource.ID)
		}
	}
	if got := handoffIdentityCount(t, fixture.Unclaimed); got != 1 {
		t.Fatalf("unclaimed resource not imported exactly once: %d", got)
	}
	input := handoffTake(t, restored, "child", "stdin")
	defer func() { _ = input.Close() }()
	output := handoffTake(t, restored, "child", "stdout")
	defer func() { _ = output.Close() }()
	socket := handoffTake(t, restored, "flow", "socket")
	defer func() { _ = socket.Close() }()
	for _, file := range []*os.File{input, output, socket} {
		handoffRequireCloexec(t, file)
	}
	// Close must release only files still owned by Restored, not taken files.
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	handoffAssertIdentitiesAbsent(t, []handoffFDIdentity{fixture.Unclaimed})
	identities := []handoffFDIdentity{handoffIdentity(t, input), handoffIdentity(t, output), handoffIdentity(t, socket)}
	handoffAuditChild(t, identities)
	// Same child, same pipes, and a real wait4 by its original parent after exec.
	handoffWriteLine(t, input, "resume-"+fixture.Nonce)
	wantChild := fmt.Sprintf("continued:%d:resume-%s", fixture.ChildPID, fixture.Nonce)
	if got := handoffReadLine(t, bufio.NewReader(output)); got != wantChild {
		t.Fatalf("child/pipe continuity = %q, want %q", got, wantChild)
	}
	var status syscall.WaitStatus
	for {
		pid, err := syscall.Wait4(fixture.ChildPID, &status, 0, nil)
		if err == syscall.EINTR {
			continue
		}
		if err != nil || pid != fixture.ChildPID || !status.Exited() || status.ExitStatus() != 23 {
			t.Fatalf("original parent cannot reap exact child: pid=%d status=%v err=%v", pid, status, err)
		}
		break
	}
	if pid, err := syscall.Wait4(fixture.ChildPID, &status, syscall.WNOHANG, nil); err != syscall.ECHILD {
		t.Fatalf("child was not reaped exactly once: second wait pid=%d err=%v", pid, err)
	}
	conn, err := net.FileConn(socket)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	_ = socket.Close()
	if err := conn.SetDeadline(time.Now().Add(handoffTestBudget)); err != nil {
		t.Fatal(err)
	}
	handoffWriteLine(t, conn, fmt.Sprintf("after:%d:%d:23:%s", os.Getpid(), fixture.ChildPID, fixture.Nonce))
	if ack := handoffReadLine(t, bufio.NewReader(conn)); ack != "after-ack:"+fixture.Nonce {
		t.Fatalf("post-exec peer ack = %q", ack)
	}
	handoffWriteLine(t, conn, "done:"+fixture.Nonce)
}

func handoffAuditBusinessChild(t *testing.T) {
	t.Helper()
	if restored, err := handoff.Restore(); err != nil || restored != nil {
		t.Fatalf("business child inherited private restore state: (%v, %v)", restored, err)
	}
	var identities []handoffFDIdentity
	if err := json.Unmarshal([]byte(os.Getenv(handoffTestFDEnv)), &identities); err != nil || len(identities) == 0 {
		t.Fatalf("invalid non-vacuous fd audit input: %v (%d identities)", err, len(identities))
	}
	handoffAssertIdentitiesAbsent(t, identities)
}

func exerciseHandoffExecFailure(t *testing.T) {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close(); _ = writer.Close() }()
	wantReaderFD, wantReaderStatus := handoffFileFlags(t, reader)
	wantWriterFD, wantWriterStatus := handoffFileFlags(t, writer)
	if wantReaderStatus&unix.O_NONBLOCK == 0 || wantWriterStatus&unix.O_NONBLOCK == 0 {
		t.Fatal("pipe fixture must start nonblocking to detect accidental File.Fd conversion")
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "test-runtime", Instance: "failed-exec", Generation: 1}, []handoff.Resource{
		{ID: "pipe", Kind: "pipe", Files: map[string]*os.File{"read": reader, "write": writer}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	for attempt := 0; attempt < 2; attempt++ {
		missing := filepath.Join(t.TempDir(), "missing-runtime")
		if err := prepared.Exec(missing, []string{missing}, os.Environ()); err == nil {
			t.Fatal("exec of missing runtime unexpectedly succeeded")
		}
		gotReaderFD, gotReaderStatus := handoffFileFlags(t, reader)
		gotWriterFD, gotWriterStatus := handoffFileFlags(t, writer)
		if gotReaderFD != wantReaderFD || gotReaderStatus != wantReaderStatus || gotWriterFD != wantWriterFD || gotWriterStatus != wantWriterStatus {
			t.Fatalf("caller flags changed after Prepare/failed Exec: read=%x/%x want=%x/%x write=%x/%x want=%x/%x",
				gotReaderFD, gotReaderStatus, wantReaderFD, wantReaderStatus, gotWriterFD, gotWriterStatus, wantWriterFD, wantWriterStatus)
		}
		// While Prepared is still live, its dup files must again be CLOEXEC.
		// Checking only the caller's original FD would miss leaked owned copies.
		handoffAuditChild(t, []handoffFDIdentity{handoffIdentity(t, reader)})
		payload := []byte(fmt.Sprintf("still-live-%d", attempt))
		if _, err := writer.Write(payload); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, len(payload))
		if _, err := io.ReadFull(reader, got); err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("original pipe failed after Exec error: got %q err=%v", got, err)
		}
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	// Caller ownership survives Prepare.Close as well as Exec failure.
	if _, err := writer.Write([]byte("z")); err != nil {
		t.Fatal(err)
	}
	var final [1]byte
	if _, err := io.ReadFull(reader, final[:]); err != nil || final[0] != 'z' {
		t.Fatalf("Prepare.Close consumed caller file: %q %v", final, err)
	}
}

func handoffAuditChild(t *testing.T, identities []handoffFDIdentity) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), handoffTestBudget)
	defer cancel()
	audit := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHandoffExecSubprocess$")
	audit.Env = handoffTestEnv("audit", map[string]string{handoffTestFDEnv: string(handoffJSON(t, identities))})
	if out, err := testharness.CombinedOutput(audit); err != nil {
		t.Fatalf("owned resources leaked into business child: %v\n%s", err, out)
	}
}

func handoffIdentityCount(t *testing.T, identity handoffFDIdentity) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		var stat syscall.Stat_t
		err := syscall.Stat(filepath.Join("/proc/self/fd", entry.Name()), &stat)
		if err == syscall.ENOENT {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if uint64(stat.Dev) == identity.Dev && stat.Ino == identity.Ino {
			count++
		}
	}
	return count
}

func handoffTake(t *testing.T, restored *handoff.Restored, id, name string) *os.File {
	t.Helper()
	file, err := restored.TakeFile(id, name)
	if err != nil || file == nil {
		t.Fatalf("TakeFile(%q,%q)=(%v,%v)", id, name, file, err)
	}
	return file
}

func handoffFileFlags(t *testing.T, file *os.File) (descriptor, status int) {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var descriptorErr, statusErr error
	if err := raw.Control(func(fd uintptr) {
		descriptor, descriptorErr = unix.FcntlInt(fd, unix.F_GETFD, 0)
		status, statusErr = unix.FcntlInt(fd, unix.F_GETFL, 0)
	}); err != nil {
		t.Fatal(err)
	}
	if descriptorErr != nil || statusErr != nil {
		t.Fatalf("fcntl flags: descriptor=%v status=%v", descriptorErr, statusErr)
	}
	return descriptor, status
}

func handoffRequireCloexec(t *testing.T, file *os.File) {
	t.Helper()
	flags, _ := handoffFileFlags(t, file)
	if flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("file %q is missing FD_CLOEXEC", file.Name())
	}
}

func handoffIdentity(t *testing.T, file *os.File) handoffFDIdentity {
	t.Helper()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatalf("file %q has no Linux stat identity", file.Name())
	}
	return handoffFDIdentity{Dev: uint64(stat.Dev), Ino: stat.Ino}
}

func handoffAssertIdentitiesAbsent(t *testing.T, identities []handoffFDIdentity) {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		var stat syscall.Stat_t
		err := syscall.Stat(filepath.Join("/proc/self/fd", entry.Name()), &stat)
		if err == syscall.ENOENT {
			continue // The ReadDir descriptor itself is already closed.
		}
		if err != nil {
			t.Fatal(err)
		}
		for _, identity := range identities {
			if uint64(stat.Dev) == identity.Dev && stat.Ino == identity.Ino {
				t.Fatalf("inherited forbidden open-file identity %+v at fd %s", identity, entry.Name())
			}
		}
	}
}

func handoffJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func handoffTestEnv(mode string, values map[string]string) []string {
	// Preserve the production handoff marker, if any, so the audit's Restore
	// assertion cannot be made green by the fixture deleting that private key.
	keys := []string{handoffTestModeEnv, handoffTestAddrEnv, handoffTestDirEnv, handoffTestIDEnv, handoffTestFDEnv}
	env := make([]string, 0, len(os.Environ())+len(values)+1)
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		isTestKey := false
		for _, key := range keys {
			if name == key {
				isTestKey = true
				break
			}
		}
		if !isTestKey {
			env = append(env, item)
		}
	}
	env = append(env, handoffTestModeEnv+"="+mode)
	for name, value := range values {
		env = append(env, name+"="+value)
	}
	return env
}

func handoffWriteLine(t *testing.T, writer io.Writer, line string) {
	t.Helper()
	if _, err := io.WriteString(writer, line+"\n"); err != nil {
		t.Fatal(err)
	}
}

func handoffReadLine(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	line, err := reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read continuity record: %v (partial %q)", err, line)
	}
	return strings.TrimSuffix(line, "\n")
}
