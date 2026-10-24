//go:build linux

package ptysession

import (
	"bytes"
	"errors"
	"io"
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
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

const actorTestBudget = 10 * time.Second

// Pure table tests never start a fake I/O pump. Every kernel test below uses
// Session.TakeMaster and an actual adopted child with its original slave.
type tableMaster struct {
	file            *os.File
	closes, resizes int
	closeErr        error
}

func (*tableMaster) Read([]byte) (int, error)                  { panic("table test attempted kernel read") }
func (*tableMaster) Write([]byte) (int, error)                 { panic("table test attempted kernel write") }
func (*tableMaster) SetReadDeadline(time.Time) error           { return nil }
func (*tableMaster) SetWriteDeadline(time.Time) error          { return nil }
func (m *tableMaster) Resize(uint16, uint16) error             { m.resizes++; return nil }
func (*tableMaster) Observe() (pty.TerminalObservation, error) { return pty.TerminalObservation{}, nil }
func (m *tableMaster) Borrow() *os.File                        { return m.file }
func (m *tableMaster) Close() error                            { m.closes++; return m.closeErr }

func tableActor(t *testing.T, now *int64) *Actor {
	t.Helper()
	budget := &workload.ReplayBudget{}
	in, err := workload.NewReplay(budget, upgradebudget.PTYInputBytes)
	if err != nil {
		t.Fatal(err)
	}
	out, err := workload.NewReplay(budget, upgradebudget.StreamReplayBytes)
	if err != nil {
		_ = in.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = in.Close(); _ = out.Close() })
	file, err := os.CreateTemp(t.TempDir(), "table-master")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = file.Close() })
	cfg := Config{Manager: &workload.Manager{}, PID: "table", Input: in, Output: out, Master: &tableMaster{file: file}, Clock: func() (int64, error) { return *now, nil }, Identity: pty.TerminalIdentity{Version: 1, Started: true, LeaderPID: 1, BootID: "table", StartTimeTicks: 1}}
	return newActor(cfg, Image{Version: 1, Identity: cfg.Identity}, phaseActive)
}

func requireDependency(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// An unavailable dependency must fail the test, never leave an all-skipped
// loop whose parent looks like a kernel-semantics PASS in a machine receipt.
func requirePTYImplementation(t *testing.T) {
	t.Helper()
	var session pty.Session
	_, _, err := session.TakeMaster()
	// origin: S10 internal review NO-SKIP-AS-PASS F3
	// TakeMaster on an unstarted session must refuse without a kernel master.
	if err == nil {
		t.Fatal("unstarted PTY supplied a master")
	}
	var manager workload.Manager
	_, err = manager.FreezeUntil(time.Now().Add(actorTestBudget))
	requireDependency(t, err)
	manager.Thaw()
}

func awaitActor(t *testing.T, why string, fn func() bool) {
	t.Helper()
	if !testharness.WaitFor(t, actorTestBudget, time.Millisecond, fn) {
		t.Fatal(why)
	}
}

const pythonPreamble = `import os,sys,time,termios,tty,signal
root=sys.argv[1]
assert not os.environ.get('TETHER_TEST_PTY_ACTOR_MODE')
assert not os.environ.get('_TETHER_HANDOFF')
def mark(name,data=b'1'):
 with open(root+'/'+name+'.tmp','wb') as f: f.write(data)
 os.rename(root+'/'+name+'.tmp',root+'/'+name)
def wait(name):
 while not os.path.exists(root+'/'+name): time.sleep(.002)
`

type actorFixture struct {
	a       *Actor
	manager *workload.Manager
	session *pty.Session
	dir     string
	pid     int
	budget  *workload.ReplayBudget
}

func startActorFixture(t *testing.T, body string) *actorFixture {
	t.Helper()
	f := &actorFixture{manager: &workload.Manager{}, budget: &workload.ReplayBudget{}}
	if root := os.Getenv("TETHER_TEST_PTY_ACTOR_ROOT"); root != "" {
		f.dir = root
	} else {
		f.dir = t.TempDir()
	}

	session, err := pty.Allocate(80, 24)
	requireDependency(t, err)
	f.session = session
	adopted := false
	t.Cleanup(func() {
		if f.pid > 0 {
			_ = syscall.Kill(-f.pid, syscall.SIGKILL)
		}
		if adopted {
			f.manager.Thaw()
			if f.a != nil {
				_ = f.a.Close()
			}
			_ = f.manager.AllowReap("terminal")
			_, _ = f.manager.Wait("terminal")
			_ = f.manager.Forget("terminal")
		} else if f.pid > 0 {
			_, _ = session.Wait()
		}
		_ = session.Close()
	})
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Fatal(err)
	}
	err = session.Start([]string{python, "-u", "-c", pythonPreamble + body, f.dir}, actorEnvironment(""), f.dir, python)
	requireDependency(t, err)
	f.pid = session.OSPID()
	// origin: hot-update S5 PROC-LEAK. The parent retains the PTY group
	// identity across helper exec, including rejection before actor import.
	if err := os.WriteFile(filepath.Join(f.dir, "leader.pid"), []byte(strconv.Itoa(f.pid)), 0600); err != nil {
		t.Fatal(err)
	}
	err = session.AdoptDraining(f.manager, actorChildIdentity(t, f.pid))
	requireDependency(t, err)
	adopted = true
	master, id, err := session.TakeMaster()
	requireDependency(t, err)
	in, err := workload.NewReplay(f.budget, upgradebudget.PTYInputBytes)
	if err != nil {
		_ = master.Close()
		t.Fatal(err)
	}
	out, err := workload.NewReplay(f.budget, upgradebudget.StreamReplayBytes)
	if err != nil {
		_ = master.Close()
		_ = in.Close()
		t.Fatal(err)
	}
	f.a, err = New(Config{Manager: f.manager, PID: "terminal", Master: master, Identity: id, Input: in, Output: out, Clock: func() (int64, error) { return int64(time.Hour), nil }})
	if err != nil {
		_ = master.Close()
		_ = in.Close()
		_ = out.Close()
		t.Fatal(err)
	}
	return f
}

// The transfer contract requires the kernel identity established at adoption,
// not an identity filled later by the process-table exporter.
// origin: hot-update S4 plan §3.1 / T04
func actorChildIdentity(t *testing.T, pid int) workload.Identity {
	t.Helper()
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	end := strings.LastIndexByte(text, ')')
	if end < 0 {
		t.Fatal("malformed child stat")
	}
	fields := strings.Fields(text[end+1:])
	if len(fields) < 20 {
		t.Fatal("incomplete child stat")
	}
	ticks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return workload.Identity{ID: "terminal", OSPID: pid, BootID: strings.TrimSpace(string(boot)), StartTimeTicks: ticks, StartedAt: time.Now()}
}

func markActor(t *testing.T, dir, name string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte("1"), 0600); err != nil {
		t.Fatal(err)
	}
}
func waitMark(t *testing.T, dir, name string) {
	t.Helper()
	awaitActor(t, "child marker "+name, func() bool { _, err := os.Stat(filepath.Join(dir, name)); return err == nil })
}
func actorBytes(t *testing.T, a *Actor) []byte {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	s := a.cfg.Output.Snapshot()
	b := make([]byte, s.End-s.Base)
	_, err := a.cfg.Output.Read(s.Base, b)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	return b
}
func waitOutput(t *testing.T, a *Actor, text string) {
	t.Helper()
	awaitActor(t, "output "+text, func() bool { return bytes.Contains(actorBytes(t, a), []byte(text)) })
}
func freezeActor(t *testing.T, a *Actor) (Image, map[string]*os.File) {
	t.Helper()
	s, files, err := a.Freeze(time.Now().Add(actorTestBudget))
	requireDependency(t, err)
	return s, files
}
func activateActor(t *testing.T, a *Actor) {
	t.Helper()
	requireDependency(t, a.PreActivate())
	requireDependency(t, a.Activate())
}

func processZombie(pid int) bool {
	data, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return false
	}
	s := string(data)
	end := strings.LastIndexByte(s, ')')
	return end >= 0 && strings.HasPrefix(s[end+1:], " Z ")
}

type writeReceipt struct {
	Attempt, N int
	Error      string
}

// observedMaster only instruments real kernel I/O; it never manufactures a
// short return. Tests can delay a completed return to expose accounting races.
type observedMaster struct {
	readReturned chan struct{}
	releaseRead  <-chan struct{}
	pty.Master
	mu                                         sync.Mutex
	writes                                     []writeReceipt
	writeReturned                              chan struct{}
	releaseWrite                               <-chan struct{}
	closeEntered                               chan struct{}
	releaseClose                               <-chan struct{}
	readActive, writeActive, maxRead, maxWrite int
}

func (m *observedMaster) Write(b []byte) (int, error) {
	m.mu.Lock()
	m.writeActive++
	m.maxWrite = max(m.maxWrite, m.writeActive)
	m.mu.Unlock()
	n, err := m.Master.Write(b)
	m.mu.Lock()
	r := writeReceipt{Attempt: len(b), N: n}
	if err != nil {
		r.Error = err.Error()
	}
	m.writes = append(m.writes, r)
	m.mu.Unlock()
	if m.writeReturned != nil {
		select {
		case m.writeReturned <- struct{}{}:
		default:
		}
		if m.releaseWrite != nil {
			<-m.releaseWrite
		}
	}
	m.mu.Lock()
	m.writeActive--
	m.mu.Unlock()
	return n, err
}
func (m *observedMaster) Read(b []byte) (int, error) {
	m.mu.Lock()
	m.readActive++
	m.maxRead = max(m.maxRead, m.readActive)
	m.mu.Unlock()
	n, err := m.Master.Read(b)
	if n > 0 && m.readReturned != nil {
		select {
		case m.readReturned <- struct{}{}:
		default:
		}
		if m.releaseRead != nil {
			<-m.releaseRead
		}
	}
	m.mu.Lock()
	m.readActive--
	m.mu.Unlock()
	return n, err
}
func (m *observedMaster) Close() error {
	if m.closeEntered != nil {
		select {
		case m.closeEntered <- struct{}{}:
		default:
		}
		if m.releaseClose != nil {
			<-m.releaseClose
		}
	}
	return m.Master.Close()
}

func instrumentActor(t *testing.T, a *Actor) *observedMaster {
	t.Helper()
	freezeActor(t, a)
	m := &observedMaster{Master: a.cfg.Master}
	a.cfg.Master = m
	activateActor(t, a)
	return m
}

func inputPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(33 + i%90)
	}
	return b
}
func enqueueActor(t *testing.T, a *Actor, data []byte, fin bool) {
	t.Helper()
	for offset := 0; offset < len(data); {
		n := min(upgradebudget.PTYBatchBytes, len(data)-offset)
		_, err := a.AcceptInput(uint64(offset), data[offset:offset+n], fin && offset+n == len(data))
		if err != nil {
			t.Fatal(err)
		}
		offset += n
	}
	if len(data) == 0 && fin {
		if _, err := a.AcceptInput(0, nil, true); err != nil {
			t.Fatal(err)
		}
	}
}

const actorImageMode = "TETHER_TEST_PTY_ACTOR_MODE"

func actorEnvironment(mode string) []string {
	var env []string
	for _, v := range handoff.CleanEnvironment(os.Environ()) {
		if !strings.HasPrefix(v, actorImageMode+"=") && !strings.HasPrefix(v, "TETHER_TEST_PTY_ACTOR_ROOT=") {
			env = append(env, v)
		}
	}
	if mode != "" {
		env = append(env, actorImageMode+"="+mode)
	}
	return env
}

// Real: both frozen replay owners and their descriptors. Controlled: terminal
// metadata only; no fake Master Read/Write is ever invoked by this image test.
// origin: hot-update S4 plan §5.1 / T18
func TestPtyImportBorrowsFrozenOwnersWithoutActivating(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("frozen-owner", func(t *testing.T) {
			now := int64(1)
			original := tableActor(t, &now)
			if _, err := original.AcceptInput(0, []byte("queued"), true); err != nil {
				t.Fatal(err)
			}
			original.phase = phaseFrozen
			if _, _, err := original.cfg.Input.Freeze(); err != nil {
				t.Fatal(err)
			}
			if _, _, err := original.cfg.Output.Freeze(); err != nil {
				t.Fatal(err)
			}
			state := original.snapshotLocked()
			imported, err := Import(original.cfg, state)
			if err != nil {
				t.Fatal(err)
			}
			if imported.read.running || imported.write.running || imported.phase != phaseFrozen {
				t.Fatal("import activated a borrower")
			}
			if err := imported.Revoke("session destroyed"); err != nil {
				t.Fatal(err)
			}
			updated, files, err := imported.Freeze(time.Now().Add(actorTestBudget))
			if err != nil {
				t.Fatal(err)
			}
			if updated.Revoked == "" || !updated.InputEnded || updated.Input.Base != 0 || updated.Input.End != 6 || len(files) != 3 {
				t.Fatalf("frozen export lost revocation: %+v", updated)
			}
			detached, err := imported.Detach()
			if err != nil {
				t.Fatal(err)
			}
			for name, file := range detached {
				if file != files[name] {
					t.Fatal("detach replaced original", name)
				}
				if _, err := file.Stat(); err != nil {
					t.Fatal("detach closed file", err)
				}
				_ = file.Close()
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "frozen import", g0)
	testharness.AssertNoFDLeak(t, "frozen import", fd0, 0)
}

// origin: hot-update S4 plan §5.1
func TestPtyImageRejectsInconsistentResponsibility(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("invariants", func(t *testing.T) {
			now := int64(1)
			a := tableActor(t, &now)
			_, _, err := a.cfg.Input.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = a.cfg.Output.Freeze()
			if err != nil {
				t.Fatal(err)
			}
			image := a.snapshotLocked()
			for _, mutate := range []func(*Image){
				func(s *Image) { s.Version++ }, func(s *Image) { s.Identity.PTYIndex++ }, func(s *Image) { s.Input.End++ }, func(s *Image) { s.Output.End++ },
				func(s *Image) { s.InputFIN = true }, func(s *Image) { s.InputEnded = true }, func(s *Image) { s.InputError = "lost input" },
				func(s *Image) { s.MasterClosed = true }, func(s *Image) { s.Pending = []byte("orphan") }, func(s *Image) { s.Discarding = true },
				func(s *Image) { s.ControlDigest = "uncommitted" }, func(s *Image) { s.ControlSeq = 1 }, func(s *Image) { s.LastKASeq = 1 },
				func(s *Image) { s.KAIntervalMS = 1 }, func(s *Image) { s.KADeadlineBootNS = 1 }, func(s *Image) { s.ExpiredSinceBootNS = -1 },
				func(s *Image) { s.RecoveryNotBeforeBootNS = 1 }, func(s *Image) { s.CleanupMembers = []workload.SessionMember{{}} },
			} {
				broken := image
				mutate(&broken)
				if _, err := Import(a.cfg, broken); err == nil {
					t.Fatalf("accepted invalid responsibility %+v", broken)
				}
			}
			// Active replay owners are not importable even when their bytes match.
			a.cfg.Input.Thaw()
			if _, err := Import(a.cfg, image); err == nil {
				t.Fatal("import borrowed a live replay")
			}
		})
	}
	testharness.AssertNoGoroutineLeak(t, "image rejection", g0)
	testharness.AssertNoFDLeak(t, "image rejection", fd0, 0)
}
