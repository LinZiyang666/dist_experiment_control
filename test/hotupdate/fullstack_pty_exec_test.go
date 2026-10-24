//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	cpty "github.com/creack/pty"
	"golang.org/x/sys/unix"
)

// This program is the actual business child. File gates govern its own reads
// and exits; they never stop a product pump, reaper, deadline or control owner.
// The remote slave is held only by the declared business session members.
const fullstackTaskProgram = `import os,sys,time,json,signal,tty,termios,fcntl,struct
from pathlib import Path
root,mode=Path(sys.argv[1]),sys.argv[2]
input_size=int(sys.argv[3]) if len(sys.argv)>3 else 65536
def mark(name,data=b'1'):
 p=root/(name+'.tmp'); p.write_bytes(data); p.replace(root/name)
def wait(name):
 while not (root/name).exists(): time.sleep(.005)
def ident():
 s=Path('/proc/self/stat').read_text().rsplit(') ',1)[1].split()
 return dict(PID=os.getpid(),Session=os.getsid(0),PGID=os.getpgrp(),TTY=int(s[4]),Ticks=int(s[19]))
fd=os.open(root/'starts',os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
os.write(fd,b'start\n'); os.close(fd)
if mode!='exec': tty.setraw(0)
mark('identity.json',json.dumps(ident()).encode())
count=0
def caught(sig,frame):
 global count
 count+=1; mark('signals',str(count).encode())
signal.signal(signal.SIGINT,caught)
signal.signal(signal.SIGHUP,signal.SIG_IGN)
if mode=='tail':
 pid=os.fork()
 if pid:
  wait('descendant.json'); os.write(1,b'PREFIX'); os._exit(23)
 os.setpgid(0,0)
 mark('descendant.json',json.dumps(ident()).encode())
 wait('tail'); os.write(1,b'TAIL'); os._exit(0)
if mode=='victim':
 for i in range(2):
  pid=os.fork()
  if pid==0:
   os.setpgid(0,0); mark('member'+str(i)+'.json',json.dumps(ident()).encode())
   if i==1: os.kill(os.getpid(),signal.SIGSTOP)
   while True: time.sleep(.01)
 wait('member0.json'); wait('member1.json')
os.write(1,b'PREFIX')
if mode=='exec': os.write(2,b'STDERR_PREFIX')
drained=False
while not (root/'finish').exists():
 if (root/'resize-observe').exists() and not (root/'resize-first').exists():
  mark('resize-first',fcntl.ioctl(0,termios.TIOCGWINSZ,bytes(8)))
 if (root/'resize-change').exists() and not (root/'resize-changed').exists():
  fcntl.ioctl(0,termios.TIOCSWINSZ,struct.pack('HHHH',37,113,0,0)); mark('resize-changed')
 if (root/'resize-after').exists() and not (root/'resize-final').exists():
  mark('resize-final',fcntl.ioctl(0,termios.TIOCGWINSZ,bytes(8)))
 if (root/'signal-observe').exists() and not (root/'signals-after').exists():
  mark('signals-after',str(count).encode())
 if (root/'drain').exists() and not drained:
  data=b''
  while len(data)<input_size: data+=os.read(0,input_size-len(data))
  mark('input',data); drained=True
 time.sleep(.005)
os.write(1,b'TAIL')
if mode=='exec': os.write(2,b'STDERR_TAIL')
sys.exit(23)
`

func fullstackBusiness(t *testing.T, mode string) (string, []string) {
	t.Helper()
	root := t.TempDir()
	script := filepath.Join(root, "business.py")
	transferMust(t, os.WriteFile(script, []byte(fullstackTaskProgram), 0600))
	return root, []string{"/usr/bin/python3", "-I", script, root, mode}
}

func fullstackOnce(t *testing.T, root string) {
	t.Helper()
	if !bytes.Equal(fullstackWaitFile(t, filepath.Join(root, "starts")), []byte("start\n")) {
		t.Fatal("create replay repeated the original business effect")
	}
}

// origin: hot-update S10 plan §6 / §10.2 P21 / M02P/M03P/M04P/M05P/M11P/Q
func TestFullstackPTYPendingControlAcrossLayers(t *testing.T) {
	t.Run("oracle", fullstackPTYControls)
	parent := t
	var artifacts transferArtifacts
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			if artifacts.a == "" {
				artifacts = buildFullstackTaskArtifacts(parent)
			}
			exercisePTYPendingControl(t, artifacts)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack pending PTY x5", g)
	testharness.AssertNoFDLeak(t, "fullstack pending PTY x5", fd, 0)
}

func exercisePTYPendingControl(t *testing.T, a transferArtifacts) {
	started := time.Now()
	s := newFullstackTaskStack(t, a, true)
	defer s.close()
	// The real run command owns a second, local controlling terminal. The wire
	// peer supplies explicit resize/signal operations unavailable as CLI flags.
	ctlRoot, ctlArgv := fullstackBusiness(t, "pty")
	ctl := fullstackLocalTTY(t, s, ctlArgv)
	fullstackChild(t, ctlRoot)
	root, argv := fullstackBusiness(t, "pty")
	w := newFullstackTaskPeer(t, s, proto.ProcessKindPTY)
	fullstackCreate(t, w, argv)
	id := fullstackChild(t, root)
	if id.Session != id.PID || id.TTY == 0 {
		t.Fatal("remote child did not acquire its own controlling terminal")
	}
	fullstackPoll(t, w, func(_ proto.ProcessResumeReply) bool { return bytes.Equal(w.out, []byte("PREFIX")) })
	fullstackRPC(t, w, w.create)
	fullstackOnce(t, root)
	fullstackTaskCycles(t, s, proto.ProcessKindPTY)
	fd := fullstackMasterFD(t, s, id)
	stamp := fullstackTaskStamp(t, s)
	witness, err := fullstackharness.NewOFDWitness(stamp, fd)
	if err != nil {
		t.Fatalf("SETUP-RED: original PTY kernel OFD witness unavailable: %v", err)
	}
	defer func() { transferMust(t, witness.Close()) }()
	trace := fullstackTraceWrites(t, s)
	input := make([]byte, 65536)
	for i := range input {
		input[i] = byte((i*31 + i/251) % 253)
	}
	for offset := 0; offset < len(input); offset += 32768 {
		fullstackRPC(t, w, proto.ProcessResumeReq{Op: "input", InputSeq: uint64(offset), Input: input[offset : offset+32768]})
	}
	fullstackRPC(t, w, proto.ProcessResumeReq{Op: "input", InputSeq: 0, Input: input[:32768]})
	fullstackRPC(t, w, proto.ProcessResumeReq{Op: "keepalive", ControlSeq: 1})
	resize := proto.ProcessResumeReq{Op: "resize", ControlSeq: 2, Resize: &proto.PtyResizeEvent{Cols: 91, Rows: 29}}
	fullstackRPC(t, w, resize)
	fullstackGate(t, root, "resize-observe")
	first := fullstackWaitFile(t, filepath.Join(root, "resize-first"))
	fullstackGate(t, root, "resize-change")
	fullstackWaitFile(t, filepath.Join(root, "resize-changed"))
	if w.last.InputAccepted != uint64(len(input)) || w.last.InputACK >= w.last.InputAccepted || w.ack != 0 {
		t.Fatal("PTY cut has no pending suffix and independently read unacknowledged output")
	}
	fullstackRequireShortWrite(t, trace.path, fd)
	fullstackTaskCut(t, s, "agent", false)
	trace.stop()
	fullstackRequireShortWrite(t, trace.path, fd)
	fullstackRequirePTYCheckpoint(t, s, w.last.PID)
	_, err = witness.Check(t.Context(), fullstackTaskStamp(t, s), fullstackMasterFD(t, s, id))
	transferMust(t, err)
	fullstackRPC(t, w, proto.ProcessResumeReq{Op: "input", InputSeq: 0, Input: input[:32768]})
	fullstackRPC(t, w, resize)
	fullstackGate(t, root, "resize-after")
	after := fullstackWaitFile(t, filepath.Join(root, "resize-final"))
	if !bytes.Equal(first, []byte{29, 0, 91, 0, 0, 0, 0, 0}) || !bytes.Equal(after, []byte{37, 0, 113, 0, 0, 0, 0, 0}) {
		t.Fatal("resize retry repeated the ioctl after the receiver changed winsize")
	}
	signalRequest := proto.ProcessResumeReq{Op: "signal", ControlSeq: 3, Signal: int(syscall.SIGINT)}
	fullstackRPC(t, w, signalRequest)
	if string(fullstackWaitFile(t, filepath.Join(root, "signals"))) != "1" {
		t.Fatal("first signal has no unique child handshake")
	}
	fullstackTaskCut(t, s, "broker", false)
	// This is the hermetic host seam, never a systemd/nft deployment receipt.
	// Restart the same embedded store/listener while retaining both task peers.
	s.ns.Shutdown()
	s.ns.WaitForShutdown()
	s.startJS()
	fullstackRPC(t, w, signalRequest)
	fullstackTaskCut(t, s, "broker", true)
	fullstackTaskCut(t, s, "agent", true)
	fullstackRequirePTYCheckpoint(t, s, w.last.PID)
	_, err = witness.Check(t.Context(), fullstackTaskStamp(t, s), fullstackMasterFD(t, s, id))
	transferMust(t, err)
	fullstackRPC(t, w, signalRequest)
	fullstackGate(t, root, "signal-observe")
	if string(fullstackWaitFile(t, filepath.Join(root, "signals-after"))) != "1" {
		t.Fatal("signal replay reached the child twice")
	}
	fullstackGate(t, root, "drain")
	if !bytes.Equal(fullstackWaitFile(t, filepath.Join(root, "input")), input) {
		t.Fatal("original slave lost or duplicated the short-write suffix")
	}
	fullstackGate(t, root, "finish")
	fullstackFinish(t, w, []byte("PREFIXTAIL"), nil, 23)
	fullstackGate(t, ctlRoot, "finish")
	ctl.finish(23, []byte("PREFIXTAIL"))
	fullstackOnce(t, root)
	t.Logf("PTY startup/io/cut/settle=%s", time.Since(started))
}

// origin: hot-update S10 plan §6 / §10.2 P21 / M04P/M11W
func TestFullstackPTYZombieTailAndSessionCleanup(t *testing.T) {
	t.Run("oracle", fullstackRetirementControls)
	parent := t
	var artifacts transferArtifacts
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			if artifacts.a == "" {
				artifacts = buildFullstackTaskArtifacts(parent)
			}
			exercisePTYRetirement(t, artifacts, round)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "fullstack PTY retirement x5", g)
	testharness.AssertNoFDLeak(t, "fullstack PTY retirement x5", fd, 0)
}

func exercisePTYRetirement(t *testing.T, a transferArtifacts, round int) {
	s := newFullstackTaskStack(t, a)
	defer s.close()
	root, argv := fullstackBusiness(t, "tail")
	w := newFullstackTaskPeer(t, s, proto.ProcessKindPTY)
	fullstackCreate(t, w, argv)
	id := fullstackChild(t, root)
	fullstackPoll(t, w, func(r proto.ProcessResumeReply) bool {
		return bytes.Equal(w.out, []byte("PREFIX")) && r.TerminalID == ""
	})
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { state, same := fullstackChildState(id); return same && state == "Z" }) {
		t.Fatal("PTY leader was not retained as a ReapHeld zombie")
	}
	// The gate stays shut beyond the historical 200ms cutoff; elapsed time is
	// the phenomenon being measured, rather than a readiness substitute.
	timer := time.NewTimer(250 * time.Millisecond) // sleep-fixture: prove tail survives the removed 200ms timeout.
	<-timer.C
	r := fullstackRPC(t, w, proto.ProcessResumeReq{Op: "resume"})
	state, same := fullstackChildState(id)
	transferMust(t, fullstackCheckHeldTail(state, same, r))
	fullstackTaskCycles(t, s, proto.ProcessKindPTY)
	fullstackTaskCut(t, s, "agent", false)
	fullstackTaskCut(t, s, "broker", false)
	s.ns.Shutdown()
	s.ns.WaitForShutdown()
	s.startJS()
	fullstackTaskCut(t, s, "broker", true)
	fullstackTaskCut(t, s, "agent", true)
	r = fullstackRPC(t, w, proto.ProcessResumeReq{Op: "resume"})
	state, same = fullstackChildState(id)
	transferMust(t, fullstackCheckHeldTail(state, same, r))
	fullstackGate(t, root, "tail")
	fullstackFinish(t, w, []byte("PREFIXTAIL"), nil, 23)
	if !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool { _, same := fullstackChildState(id); return !same }) {
		t.Fatal("true EOF and ACK did not retire the original leader")
	}
	victimRoot, victimArgv := fullstackBusiness(t, "victim")
	victim := newFullstackTaskPeer(t, s, proto.ProcessKindPTY)
	fullstackCreate(t, victim, victimArgv)
	leader := fullstackChild(t, victimRoot)
	var members [2]fullstackChildIdentity
	for i := range members {
		path := filepath.Join(victimRoot, fmt.Sprintf("member%d.json", i))
		fullstackWaitFile(t, path)
		fullstackReadJSON(t, path, &members[i])
		if members[i].Session != leader.Session || members[i].PGID == leader.PGID {
			t.Fatal("victim did not contain multiple process groups")
		}
	}
	if state, same := fullstackChildState(members[1]); !same || state != "T" {
		t.Fatal("victim did not contain a stopped member")
	}
	if _, err := s.command("session", "rm", s.sid); err != nil {
		t.Fatalf("SETUP-RED: NOT_RUN pending S8 P14 incarnation revocation: %v", err)
	}
	// Census is checked twice while the agent owner is alive, before fixture
	// cleanup can kill escaped or unreaped members and manufacture success.
	for pass := 0; pass < 2; pass++ {
		if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool { return len(fullstackSessionCensus(t, leader.Session)) == 0 }) {
			t.Fatal("revocation retained business session members or zombies")
		}
		_, err := s.agent.SampleIdentity()
		transferMust(t, err)
	}
	// Local removal cannot certify hard-delete/recreation with a lost event.
	// Consume S8's separate full-domain victim/survivor receipt as required
	// intake; its missing production evidence stays red and is never fabricated
	// from the successful census above. S10 P24 owns the joint late-message arm.
	// origin: hot-update S10 plan §6 / §13.2 C4/C8/C10 / S8 P24/P26
	exerciseAuthorityParallelTermination(t, round)
}

func fullstackSessionCensus(t *testing.T, sid int) []int {
	t.Helper()
	entries, err := os.ReadDir("/proc")
	transferMust(t, err)
	var found []int
	for _, entry := range entries {
		pid, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		data, err := os.ReadFile(filepath.Join("/proc", entry.Name(), "stat"))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		transferMust(t, err)
		end := strings.LastIndexByte(string(data), ')')
		fields := strings.Fields(string(data[end+1:]))
		if end < 0 || len(fields) < 4 {
			t.Fatal("unreadable session census")
		}
		value, err := strconv.Atoi(fields[3])
		transferMust(t, err)
		if value == sid {
			found = append(found, pid)
		}
	}
	return found
}

func fullstackTaskStamp(t *testing.T, s *fullstackTaskStack) contract.Stamp {
	t.Helper()
	sample := fullstackRuntimeSample(t, s)
	key := contract.Key{Role: "agent", Host: "loopback", Unit: "task-agent", SID: s.sid, NID: s.nid, Domain: s.agentPath, UID: uint32(os.Getuid())}
	process, err := fullstackharness.ReadKernelProcess(key, int(sample.PID), sample.Binding.InstanceID)
	transferMust(t, err)
	return contract.Stamp{Process: process, Generation: sample.Binding.Generation, CutID: sample.CutID,
		Sequence: sample.Sequence, AtBootNS: int64(sample.AtBootNS), ScopeDigest: sample.ScopeDigest}
}

func fullstackMasterFD(t *testing.T, s *fullstackTaskStack, child fullstackChildIdentity) int {
	t.Helper()
	link, err := os.Readlink(fmt.Sprintf("/proc/%d/fd/0", child.PID))
	transferMust(t, err)
	index := filepath.Base(link)
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fdinfo", s.agent.Receipt().PID))
	transferMust(t, err)
	var matches []int
	for _, entry := range entries {
		body, err := os.ReadFile(fmt.Sprintf("/proc/%d/fdinfo/%s", s.agent.Receipt().PID, entry.Name()))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		transferMust(t, err)
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "tty-index:") && strings.TrimSpace(strings.TrimPrefix(line, "tty-index:")) == index {
				fd, err := strconv.Atoi(entry.Name())
				transferMust(t, err)
				matches = append(matches, fd)
			}
		}
	}
	if len(matches) != 1 {
		t.Fatalf("original PTY master has %d product owners", len(matches))
	}
	return matches[0]
}

type fullstackWriteTrace struct {
	path string
	stop func()
}

// Read-only tracing starts with the agent, avoiding an attach permission race.
// raw=write records only fd/pointer/count/result; custody payloads stay private.
func fullstackTraceWrites(t *testing.T, s *fullstackTaskStack) fullstackWriteTrace {
	t.Helper()
	if s.tracer != nil {
		return *s.tracer
	}
	tracer := func() int {
		body, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", s.agent.Receipt().PID))
		transferMust(t, err)
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "TracerPid:") {
				pid, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "TracerPid:")))
				transferMust(t, err)
				return pid
			}
		}
		t.Fatal("kernel tracer identity missing")
		return 0
	}
	pid := tracer()
	if pid <= 0 || s.tracePath == "" {
		t.Fatal("SETUP-RED: original agent has no kernel syscall witness")
	}
	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		transferMust(t, syscall.Kill(pid, syscall.SIGINT))
		if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool { return tracer() == 0 }) {
			t.Fatal("syscall observer did not detach from original product")
		}
		// The sole shared supervisor reaps the tracer and joins it when the
		// daemon stops. This is not a second waiter for a product child.
	}
	s.tracer = &fullstackWriteTrace{s.tracePath, stop}
	return *s.tracer
}

// Early exits owe the same tracer join as the successful partial-write path.
// The shared supervisor still reports any surviving descendant as a failure.
// origin: S10 internal review TAGGED-SUITE-SWEEP-B F5
func TestFullstackSyscallObserverJoinsWithFixture(t *testing.T) {
	artifacts := buildFullstackObservationArtifact(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseFullstackSyscallObserver(t, artifacts) })
	}
	testharness.AssertNoGoroutineLeak(t, "syscall observer fixture x5", g)
	testharness.AssertNoFDLeak(t, "syscall observer fixture x5", fd, 0)
}

func exerciseFullstackSyscallObserver(t *testing.T, artifacts transferArtifacts) {
	t.Helper()
	s := newFullstackTaskStack(t, artifacts, true)
	defer s.close()
	fullstackRuntimeSample(t, s)
	// Natural product exit can reap the tracer too. It cannot prove that the
	// fixture detached its observer before taking the product down.
	stop, detachedLive := s.tracer.stop, false
	s.tracer.stop = func() {
		if _, err := s.agent.SampleIdentity(); err == nil && s.agent.Receipt().NativeRC == -1 {
			detachedLive = true
		}
		stop()
	}
	s.close()
	if !detachedLive {
		t.Fatal("syscall observer was not detached while its original product was alive")
	}
}

func fullstackRequireShortWrite(t *testing.T, path string, fd int) {
	t.Helper()
	var attempt, n uint64
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		body, err := os.ReadFile(path)
		transferMust(t, err)
		attempt, n = fullstackShortWrite(string(body), fd)
		return n != 0
	}) {
		t.Fatal("PTY seam has no actual kernel write with 0 < n < attemptLen")
	}
	t.Logf("kernel partial write fd=%d attempt=%d n=%d", fd, attempt, n)
}

type fullstackPTYCheckpoint struct{ Applied, Accepted, Base, End, Control, KA, Deadline uint64 }

func fullstackCheckPTYCheckpoint(a, b fullstackPTYCheckpoint) error {
	if a.Applied == 0 || a.Applied >= a.Accepted || a.Base >= a.End || a.Control == 0 || a.KA == 0 || a.Deadline == 0 || a != b {
		return errors.New("PTY cut lost pending bytes, control receipt or absolute keepalive deadline")
	}
	return nil
}

func fullstackRequirePTYCheckpoint(t *testing.T, s *fullstackTaskStack, pid string) {
	t.Helper()
	var rows []fullstackPTYCheckpoint
	for _, line := range strings.Split(s.output(s.agent), "\n") {
		if !strings.Contains(line, "agent: pty image state") || !strings.Contains(line, "pid="+pid+" ") ||
			(!strings.Contains(line, "phase=cut ") && !strings.Contains(line, "phase=activated ")) {
			continue
		}
		var row fullstackPTYCheckpoint
		for key, out := range map[string]*uint64{"input_applied": &row.Applied, "input_accepted": &row.Accepted, "output_base": &row.Base,
			"output_end": &row.End, "control_seq": &row.Control, "ka_seq": &row.KA, "ka_deadline_boot_ns": &row.Deadline} {
			match := regexp.MustCompile(`(?:^| )` + key + `=([0-9]+)(?: |$)`).FindStringSubmatch(line)
			if len(match) != 2 {
				t.Fatal("PTY checkpoint field missing")
			}
			value, err := strconv.ParseUint(match[1], 10, 64)
			transferMust(t, err)
			*out = value
		}
		rows = append(rows, row)
	}
	if len(rows) == 0 || len(rows)%2 != 0 {
		t.Fatal("PTY cut/activation lacks the exact original-task checkpoint pair")
	}
	for i := 0; i < len(rows); i += 2 {
		transferMust(t, fullstackCheckPTYCheckpoint(rows[i], rows[i+1]))
		if rows[i].Deadline != rows[0].Deadline {
			t.Fatal("reverse renewed the original absolute keepalive deadline")
		}
	}
}

type fullstackLocalTerminal struct{ finish func(int, []byte) }

func fullstackLocalTTY(t *testing.T, s *fullstackTaskStack, argv []string) fullstackLocalTerminal {
	t.Helper()
	master, slave, err := cpty.Open()
	transferMust(t, err)
	t.Cleanup(func() { _ = master.Close(); _ = slave.Close() })
	// creack's ioctl calls make its master wrapper blocking. Give the reader a
	// pollable wrapper before starting I/O, so failed setup can Close and Join.
	duplicate, err := unix.FcntlInt(master.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	transferMust(t, err)
	transferMust(t, unix.SetNonblock(duplicate, true))
	transferMust(t, master.Close())
	master = os.NewFile(uintptr(duplicate), "fullstack-local-master")
	before, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
	transferMust(t, err)
	flags, err := unix.FcntlInt(slave.Fd(), unix.F_GETFL, 0)
	transferMust(t, err)
	args := append([]string{"run", "--home", s.home, "--nats-url", s.url, s.nid, "--"}, argv...)
	cmd := exec.CommandContext(t.Context(), filepath.Join(s.h.Root, "ctl"), args...)
	cmd.Env = append(flowSandboxEnvironment(s.home), "GORACE=atexit_sleep_ms=0")
	cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	wait, err := fullstackharness.StartProcess(cmd)
	transferMust(t, err)
	done := make(chan error, 1)
	go func() { done <- wait() }()
	joined := false
	t.Cleanup(func() {
		if !joined {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
				<-done
				t.Error("original ctl required emergency cleanup")
			}
		}
	})
	if sid, err := unix.Getsid(cmd.Process.Pid); err != nil || sid != cmd.Process.Pid {
		t.Fatal("local ctl has no controlling session")
	}
	var output hotUpdateOutput
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		buffer := make([]byte, 4096)
		for {
			n, err := master.Read(buffer)
			if n > 0 {
				_, _ = output.Write(buffer[:n])
			}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = master.Close(); <-readDone })
	return fullstackLocalTerminal{finish: func(code int, expected []byte) {
		select {
		case err := <-done:
			joined = true
			if fullstackharness.ProcessExitCode(err) != code {
				t.Fatalf("original ctl supervisor outcome: %v", err)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("original ctl did not terminate")
		}
		if cmd.ProcessState == nil || cmd.ProcessState.ExitCode() != code {
			t.Fatal("original ctl returned the wrong terminal status")
		}
		after, err := unix.IoctlGetTermios(int(slave.Fd()), unix.TCGETS)
		transferMust(t, err)
		afterFlags, err := unix.FcntlInt(slave.Fd(), unix.F_GETFL, 0)
		transferMust(t, err)
		if !reflect.DeepEqual(before, after) || flags != afterFlags {
			t.Fatal("original ctl did not restore termios/FD flags")
		}
		if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return bytes.Equal(output.Bytes(), expected) }) {
			t.Fatal("original local terminal bytes differ")
		}
	}}
}

func fullstackPTYControls(t *testing.T) {
	t.Run("write-trace", fullstackWriteTraceControls)
	base := fullstackPTYCheckpoint{7, 65536, 0, 6, 2, 1, 180000000001}
	transferMust(t, fullstackCheckPTYCheckpoint(base, base))
	for _, name := range []string{"applied", "accepted", "base", "end", "control", "ka", "deadline", "empty"} {
		t.Run(name, func(t *testing.T) {
			changed := base
			switch name {
			case "applied":
				changed.Applied++
			case "accepted":
				changed.Accepted--
			case "base":
				changed.Base++
			case "end":
				changed.End--
			case "control":
				changed.Control = 0
			case "ka":
				changed.KA = 0
			case "deadline":
				changed.Deadline++
			case "empty":
				changed.Applied = changed.Accepted
			}
			if fullstackCheckPTYCheckpoint(base, changed) == nil {
				t.Fatal("PTY oracle accepted " + name)
			}
		})
	}
}

func fullstackWriteTraceControls(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       uint64
	}{
		{"complete", "71 write(0x10, 0xc000, 0x8000) = 0xe00\n", 3584},
		{"split", "71 write(0x10, 0xc000, 0x8000 <unfinished ...>\n72 write(0x11, 0xc000, 0x8000) = 0xe00\n71 <... write resumed>) = 0xe00\n", 3584},
		{"wrong-fd", "71 write(0x11, 0xc000, 0x8000) = 0xe00\n", 0},
		{"zero", "71 write(0x10, 0xc000, 0x8000) = 0\n", 0},
		{"full", "71 write(0x10, 0xc000, 0x8000) = 0x8000\n", 0},
		{"error", "71 write(0x10, 0xc000, 0x8000) = -1 EAGAIN\n", 0},
		{"other-thread", "71 write(0x10, 0xc000, 0x8000 <unfinished ...>\n72 <... write resumed>) = 0xe00\n", 0},
		{"uncompleted", "71 write(0x10, 0xc000, 0x8000 <unfinished ...>\n", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempt, n := fullstackShortWrite(tc.body, 16)
			if n != tc.want || (n > 0 && attempt != 32768) || (tc.want == 0 && attempt != 0) {
				t.Fatalf("kernel syscall oracle accepted %s: attempt=%d n=%d", tc.name, attempt, n)
			}
		})
	}
}

// origin: hot-update S10 plan §6 / M04P/M11W. A live leader does not exercise
// ReapHeld, and a reaped PID cannot certify last-slave EOF or the real exit23.
func fullstackCheckHeldTail(state string, same bool, reply proto.ProcessResumeReply) error {
	if !same || state != "Z" || reply.TerminalID != "" || reply.ExitCode != 0 || reply.Signal != 0 || reply.Stdout == nil || reply.Stdout.FIN {
		return errors.New("PTY leader was not held until its descendant's true source EOF")
	}
	return nil
}

func fullstackRetirementControls(t *testing.T) {
	for _, name := range []string{"live-leader", "reaped", "terminal", "exit", "signal", "EOF", "missing-source"} {
		t.Run(name, func(t *testing.T) {
			state, same := "Z", true
			reply := proto.ProcessResumeReply{Stdout: &proto.ProcessOutput{Data: []byte("PREFIX")}}
			transferMust(t, fullstackCheckHeldTail(state, same, reply))
			switch name {
			case "live-leader":
				state = "S"
			case "reaped":
				same = false
			case "terminal":
				reply.TerminalID = "premature"
			case "exit":
				reply.ExitCode = 23
			case "signal":
				reply.Signal = 9
			case "EOF":
				reply.Stdout.FIN = true
			case "missing-source":
				reply.Stdout = nil
			}
			if fullstackCheckHeldTail(state, same, reply) == nil {
				t.Fatal("retirement oracle accepted " + name)
			}
		})
	}
}
