//go:build linux || darwin

package cli_e2e_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"github.com/LinZiyang666/tether/internal/testharness"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	creackpty "github.com/creack/pty"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

// origin: hot-update S4 plan §8.3 T35–T39/T44. These bytes come only from
// the local master. Side receipts establish effects/identity, never repair
// missing output. Keeping the slave open lets the parent compare termios
// and shared OFD flags AFTER the original ctl has been waited.
type ptyCapture struct {
	mu   sync.Mutex
	data []byte
}

func (b *ptyCapture) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data = append(b.data, p...)
	return len(p), nil
}
func (b *ptyCapture) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.data...)
}

type terminalCtl struct {
	cmd              *exec.Cmd
	master, slave    *os.File
	state            *term.State
	flags            int
	out, stderr      ptyCapture
	done, readerDone chan struct{}
	err              error
	cancel           context.CancelFunc
}

func startTerminalCtl(t *testing.T, s *realStack, bin, url string, argv ...string) *terminalCtl {
	t.Helper()
	return startTerminalCtlStderr(t, s, bin, url, false, argv...)
}

// A real interactive shell shares stderr with the raw terminal. Tests of
// diagnostic isolation must observe that same kernel byte stream.
// origin: hot-update S4 plan §7.3 / T35; PRODUCT-R4 arm C1
func startTerminalCtlStderr(t *testing.T, s *realStack, bin, url string, stderrTTY bool, argv ...string) *terminalCtl {
	t.Helper()
	master, slave, err := creackpty.Open()
	if err != nil {
		t.Fatal(err)
	}
	master = pollableLocalMaster(t, master)
	p := &terminalCtl{master: master, slave: slave, done: make(chan struct{}), readerDone: make(chan struct{})}
	t.Cleanup(func() {
		if p.cancel != nil {
			p.cancel()
		}
		if p.cmd != nil && p.cmd.Process != nil {
			select {
			case <-p.done:
			case <-time.After(5 * time.Second):
				_ = p.cmd.Process.Kill()
				<-p.done
			}
		}
		_ = master.Close()
		_ = slave.Close()
		if p.cmd != nil && p.cmd.Process != nil {
			<-p.readerDone
		}
	})
	if err := resizeLocalPTY(master, 80, 24); err != nil {
		t.Fatal(err)
	}
	// os/exec prepares inherited stdio through File.Fd, which makes the
	// parent file blocking. Establish that fixture state before the baseline;
	// observations after Start must never call Fd and repair a product defect.
	_ = slave.Fd()
	p.state, err = localTerminalState(slave)
	if err != nil {
		t.Fatal(err)
	}
	p.flags, err = localDescriptorFlags(slave)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	p.cancel = cancel
	args := append([]string{"run", "--home", s.ctlHome, "--nats-url", url, s.nid, "--"}, argv...)
	p.cmd = exec.CommandContext(ctx, bin, args...)
	p.cmd.Env = append(ctlEnv(), "TETHER_HOME="+s.ctlHome, "TERM=xterm")
	p.cmd.Stdin, p.cmd.Stdout, p.cmd.Stderr = slave, slave, &p.stderr
	if stderrTTY {
		p.cmd.Stderr = slave
	}
	// Apply terminal attributes to the actual ctl; the supervisor has its
	// own session/group and does not become the controlling-terminal owner.
	// origin: hot-update S6 plan §9.3 / FIX-ORPHAN CLI fixtures.
	p.cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
	wait, err := testharness.StartProcess(p.cmd)
	if err != nil {
		t.Fatal(err)
	}
	go func() { _, _ = io.Copy(&p.out, master); close(p.readerDone) }()
	go func() { p.err = wait(); close(p.done) }()
	return p
}

func (p *terminalCtl) write(t *testing.T, b []byte) {
	t.Helper()
	if n, err := p.master.Write(b); err != nil || n != len(b) {
		t.Fatalf("local terminal write: n=%d/%d err=%v", n, len(b), err)
	}
}
func (p *terminalCtl) frame(t *testing.T, b []byte) { t.Helper(); p.write(t, ptyFrame(b)) }
func ptyFrame(b []byte) []byte {
	frame := make([]byte, 4+len(b))
	binary.BigEndian.PutUint32(frame, uint32(len(b)))
	copy(frame[4:], b)
	return frame
}
func (p *terminalCtl) alive(t *testing.T) {
	t.Helper()
	select {
	case <-p.done:
		t.Fatalf("original ctl exited early: %v stderr=%q output=%q", p.err, p.stderr.bytes(), p.out.bytes())
	default:
	}
}
func (p *terminalCtl) expect(t *testing.T, want []byte) {
	t.Helper()
	if !testharnessWait(10*time.Second, func() bool { return samePTYBytes(p.out.bytes(), want) }) {
		t.Fatalf("terminal bytes: got %q (%dB), want %q (%dB); stderr=%q", p.out.bytes(), len(p.out.bytes()), want, len(want), p.stderr.bytes())
	}
}
func (p *terminalCtl) restored(t *testing.T) {
	t.Helper()
	state, err := localTerminalState(p.slave)
	if err != nil {
		t.Fatal(err)
	}
	flags, err := localDescriptorFlags(p.slave)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(state, p.state) || flags != p.flags {
		t.Fatalf("ctl changed original terminal: termios restored=%v flags=%#x want=%#x", reflect.DeepEqual(state, p.state), flags, p.flags)
	}
}
func (p *terminalCtl) wait(t *testing.T, rc int) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(20 * time.Second):
		t.Fatal("original ctl did not finish/join local I/O")
	}
	if p.cmd.ProcessState == nil || p.cmd.ProcessState.ExitCode() != rc {
		t.Fatalf("original ctl exit: %v want=%d stderr=%q", p.err, rc, p.stderr.bytes())
	}
	p.restored(t)
}
func (p *terminalCtl) resize(t *testing.T, cols, rows uint16) {
	t.Helper()
	if err := resizeLocalPTY(p.master, cols, rows); err != nil {
		t.Fatal(err)
	}
	if err := p.cmd.Process.Signal(syscall.SIGWINCH); err != nil {
		t.Fatal(err)
	}
}

type ptyFixture struct{ dir, script, python string }
type ptyIdentity struct {
	PID        int    `json:"pid"`
	SID        int    `json:"sid"`
	PGID       int    `json:"pgid"`
	Foreground int    `json:"foreground"`
	TTY        string `json:"tty"`
	Dev        uint64 `json:"dev"`
	Ino        uint64 `json:"ino"`
	Rdev       uint64 `json:"rdev"`
	Ticks      string `json:"ticks"`
}

func newPTYFixture(t *testing.T) *ptyFixture {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal("PTY fixtures require python3")
	}
	f := &ptyFixture{dir: t.TempDir(), python: python}
	// Register before remote spawn; a failed identity assertion must not
	// skip cleanup of the already-created terminal group.
	// origin: hot-update S5 PROC-LEAK / refused and failed PTY setup.
	t.Cleanup(func() {
		data, err := os.ReadFile(f.path("identity"))
		var id ptyIdentity
		if err == nil && json.Unmarshal(data, &id) == nil && id.PID > 0 && identityStillAlive(id) {
			_ = syscall.Kill(-id.PID, syscall.SIGKILL)
		}
	})
	f.script = filepath.Join(f.dir, "terminal.py")
	if err := os.WriteFile(f.script, []byte(ptyFixturePython), 0o600); err != nil {
		t.Fatal(err)
	}
	return f
}
func (f *ptyFixture) argv(mode string) []string { return []string{f.python, f.script, f.dir, mode} }
func (f *ptyFixture) path(name string) string   { return filepath.Join(f.dir, name) }
func (f *ptyFixture) gate(t *testing.T, name string) {
	t.Helper()
	if err := os.WriteFile(f.path(name), nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S4 internal review rv-drill DRILL-8. A foreground
// that survives a handled signal still violates uninterrupted continuity.
func (f *ptyFixture) requireNoSignals(t *testing.T) {
	t.Helper()
	for _, sig := range []syscall.Signal{syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM} {
		path := f.path("signal-" + strconv.Itoa(int(sig)))
		if b, err := os.ReadFile(path); err == nil {
			t.Fatalf("unexpected signal %d reached original foreground: %s", sig, b)
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
	}
}

func (f *ptyFixture) receipt(t *testing.T, name string) []byte {
	t.Helper()
	var body []byte
	if !testharnessWait(15*time.Second, func() bool {
		b, err := os.ReadFile(f.path(name))
		if err == nil {
			body = b
		}
		return err == nil
	}) {
		t.Fatalf("missing terminal fixture receipt %s", name)
	}
	return body
}
func (f *ptyFixture) identity(t *testing.T) ptyIdentity {
	t.Helper()
	var id ptyIdentity
	if err := json.Unmarshal(f.receipt(t, "identity"), &id); err != nil {
		t.Fatal(err)
	}
	if id.PID <= 0 || id.PID != id.SID || id.PGID != id.Foreground || id.PGID != id.PID || id.TTY == "" {
		t.Fatalf("remote child lacks original controlling terminal: %+v", id)
	}
	t.Cleanup(func() {
		// The fixture records PID identity; the Linux oracle additionally checks
		// start ticks before signaling. Darwin cleanup is tied to the live stack.
		if identityStillAlive(id) {
			_ = syscall.Kill(-id.PID, syscall.SIGKILL)
		}
	})
	return id
}
func (f *ptyFixture) once(t *testing.T) {
	t.Helper()
	b := f.receipt(t, "starts")
	if bytes.Count(b, []byte("\n")) != 1 {
		t.Fatalf("PTY child was rebuilt: starts=%q", b)
	}
}

// origin: hot-update S4 plan §9.1 / T35–T38; P18 lifecycle records
// An exit is a second record for the same child, not another spawn. Count
// starts exactly so a duplicate spawn remains visible after settlement.
const reliablePTYStartPattern = `agent: run reliable=true started`

func requirePTYReliable(t *testing.T, s *realStack, n int) {
	t.Helper()
	if got := s.agentLogCount(reliablePTYStartPattern); got != n {
		t.Fatalf("reliable run audit=%d want=%d", got, n)
	}
}

// Fixed input includes NUL, invalid UTF-8, split escape sequences, CR/LF and
// every byte value. Raw mode makes an exact byte oracle meaningful.
func ptyPayload(round int) []byte {
	b := make([]byte, 1024)
	for i := range b {
		b[i] = byte(i + round*37)
	}
	return b
}
func ptyEcho(b []byte) []byte {
	out := append([]byte("OUT:"), b...)
	out = append(out, []byte("ERR:")...)
	return append(out, b...)
}

const ptyFixturePython = `import os, sys, tty, termios, signal, struct, json, select, time, fcntl, subprocess
root, mode = sys.argv[1:]
def receipt(name, value):
    path = os.path.join(root, name)
    with open(path + '.tmp', 'w') as f: f.write(str(value))
    os.replace(path + '.tmp', path)
def gate(name): return os.path.exists(os.path.join(root, name))
def emit(data, fd=1):
    while data:
        n = os.write(fd, data)
        data = data[n:]
def ticks():
    try:
        with open('/proc/self/stat') as f: return f.read().split(') ', 1)[1].split()[19]
    except FileNotFoundError: return subprocess.check_output(['ps','-o','lstart=','-p',str(os.getpid())], text=True).strip()
assert os.isatty(0) and os.isatty(1) and os.isatty(2)
tty.setraw(0, termios.TCSANOW)
if mode == 'signals':
    attrs=termios.tcgetattr(0); attrs[3] |= termios.ISIG; termios.tcsetattr(0,termios.TCSANOW,attrs)
counts={}
def on_signal(sig, frame):
    counts[sig]=counts.get(sig,0)+1
    receipt('signal-'+str(sig),counts[sig])
for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP): signal.signal(sig,on_signal)
def size(): return struct.unpack('HHHH', fcntl.ioctl(0,termios.TIOCGWINSZ,bytes(8)))[:2]
def winch(sig, frame): receipt('size','%d %d' % size())
signal.signal(signal.SIGWINCH,winch)
st=os.fstat(0)
identity=dict(pid=os.getpid(),sid=os.getsid(0),pgid=os.getpgrp(),foreground=os.tcgetpgrp(0),tty=os.ttyname(0),dev=st.st_dev,ino=st.st_ino,rdev=st.st_rdev,ticks=ticks())
with open(os.path.join(root,'starts'),'a') as f: f.write(str(os.getpid())+'\n')
receipt('identity',json.dumps(identity))
emit(b'READY\n')
if mode == 'flood':
    while not gate('finish'): emit(b'F'*32768)
elif mode == 'gated-exit':
    while not gate('finish'): time.sleep(.01)
    emit(b'TAIL-OUT\n'); emit(b'TAIL-ERR\n',2)
    sys.exit(23)
elif mode in ('blocked','eof'):
    while not gate('finish'): time.sleep(.01)
    emit(b'TAIL-OUT\n'); emit(b'TAIL-ERR\n',2)
    sys.exit(0)
else:
    pending=b''
    while not gate('finish'):
        if gate('alter') and not gate('altered'):
            fcntl.ioctl(0,termios.TIOCSWINSZ,struct.pack('HHHH',41,103,0,0))
            receipt('altered','%d %d' % size())
        if gate('inspect'): receipt('inspected','%d %d' % size())
        readable,_,_=select.select([0],[],[],.01)
        if not readable: continue
        try: chunk=os.read(0,32768)
        except OSError: time.sleep(.01); continue
        if not chunk: time.sleep(.01); continue
        pending += chunk
        while len(pending)>=4:
            length=struct.unpack('!I',pending[:4])[0]
            if len(pending)<4+length: break
            data,pending=pending[4:4+length],pending[4+length:]
            if data == b'EXIT':
                if mode == 'tail':
                    child=os.fork()
                    if child == 0:
                        receipt('tail-child',os.getpid())
                        receipt('tail-child-identity',json.dumps(dict(pid=os.getpid(),ticks=ticks())))
                        while not gate('late-tail'): time.sleep(.01)
                        emit(b'TAIL-OUT\n'); emit(b'TAIL-ERR\n',2); os._exit(0)
                    receipt('leader-exited','yes'); sys.exit(23)
                emit(b'TAIL-OUT\n');emit(b'TAIL-ERR\n',2);sys.exit(23)
            emit(b'OUT:'+data);emit(b'ERR:'+data,2)
            with open(os.path.join(root,'inputs'),'ab') as f: f.write(struct.pack('!I',len(data))+data)
receipt('finished','yes')
`

// readPTYStatus is deliberately strict about the virtual stderr stream:
// otherwise a missing empty FIN can leave a real ctl waiting forever.
func requirePTYStatus(t *testing.T, r proto.ProcessResumeReply) {
	t.Helper()
	if r.Kind != "status" || r.TaskKind != proto.ProcessKindPTY {
		t.Fatalf("not a PTY status: kind=%q task=%q code=%q error=%q", r.Kind, r.TaskKind, r.Code, r.Error)
	}
	if r.Stderr == nil || r.Stderr.Offset != 0 || len(r.Stderr.Data) != 0 || !r.Stderr.FIN || r.Stderr.Error != "" {
		t.Fatalf("PTY virtual stderr is not an empty FIN: %+v", r.Stderr)
	}
	if r.InputACK > r.InputAccepted {
		t.Fatalf("input ACK exceeded accepted bytes: %d > %d", r.InputACK, r.InputAccepted)
	}
}

// The capture reader must be joined on cleanup, even if a mutation leaves
// ctl alive. A nonblocking wrapper lets Close wake Go's poller. Only this
// fixture owns the master OFD; the slave flags remain the caller's oracle.
func pollableLocalMaster(t *testing.T, original *os.File) *os.File {
	t.Helper()
	fd, err := unix.FcntlInt(original.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		_ = original.Close()
		t.Fatal(err)
	}
	if err := unix.SetNonblock(fd, true); err != nil {
		_ = unix.Close(fd)
		_ = original.Close()
		t.Fatal(err)
	}
	_ = original.Close()
	return os.NewFile(uintptr(fd), "local-terminal-master")
}
func resizeLocalPTY(master *os.File, cols, rows uint16) error {
	raw, err := master.SyscallConn()
	if err != nil {
		return err
	}
	var ioctlErr error
	err = raw.Control(func(fd uintptr) {
		ioctlErr = unix.IoctlSetWinsize(int(fd), unix.TIOCSWINSZ, &unix.Winsize{Col: cols, Row: rows})
	})
	if err != nil {
		return err
	}
	return ioctlErr
}

func connectPTYPeer(t *testing.T, s *realStack, url string) *nats.Conn {
	t.Helper()
	kp, err := nkeys.FromSeed(s.ctlID.Seed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kp.Wipe)
	nc, fallback, err := natsinbox.Connect(url, s.ctlID.PublicKey, []nats.Option{nats.Nkey(s.ctlID.PublicKey, kp.Sign), nats.Timeout(2 * time.Second), nats.MaxReconnects(-1), nats.ReconnectWait(100 * time.Millisecond)})
	if err != nil {
		t.Fatal(err)
	}
	if fallback {
		t.Fatal("test peer did not get a private reply inbox")
	}
	t.Cleanup(nc.Close)
	return nc
}

// origin: hot-update S4 plan §8.3 T35. Negative traces must stay red even
// when the remote source ledger contains every intended byte. The oracle
// receives only the observed terminal bytes and the independent input model.
func TestPTYDuplexOracleRejectsMissingDuplicateAndReorderedBytes(t *testing.T) {
	want := append([]byte("READY\n"), ptyEcho(ptyPayload(0))...)
	missing := append([]byte(nil), want[:100]...)
	missing = append(missing, want[101:]...)
	duplicate := append([]byte(nil), want[:100]...)
	duplicate = append(duplicate, want[99:]...)
	reordered := append([]byte(nil), want...)
	reordered[100], reordered[101] = reordered[101], reordered[100]
	for _, tc := range []struct {
		name  string
		got   []byte
		valid bool
	}{
		{"complete", want, true}, {"missing", missing, false}, {"duplicate", duplicate, false}, {"reordered", reordered, false}, {"empty", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := samePTYBytes(tc.got, want); got != tc.valid {
				t.Fatalf("terminal byte oracle accepted %s=%v; expected %v", tc.name, got, tc.valid)
			}
		})
	}
}
func samePTYBytes(got, want []byte) bool { return bytes.Equal(got, want) }

// origin: hot-update S4 plan §7.3 / T39
// File.Fd can clear O_NONBLOCK on a pollable file, repairing the exact shared
// OFD mutation this oracle must observe. Pin and inspect without changing it.
func localDescriptorFlags(file *os.File) (flags int, err error) {
	raw, err := file.SyscallConn()
	if err != nil {
		return 0, err
	}
	var inspectErr error
	err = raw.Control(func(fd uintptr) { flags, inspectErr = unix.FcntlInt(fd, unix.F_GETFL, 0) })
	return flags, errors.Join(err, inspectErr)
}

func localTerminalState(file *os.File) (state *term.State, err error) {
	raw, err := file.SyscallConn()
	if err != nil {
		return nil, err
	}
	var inspectErr error
	err = raw.Control(func(fd uintptr) { state, inspectErr = term.GetState(int(fd)) })
	return state, errors.Join(err, inspectErr)
}
