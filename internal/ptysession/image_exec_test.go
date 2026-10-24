//go:build linux

package ptysession

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

type actorExecImage struct {
	OwnerPID      int
	Dir, Scenario string
	Image         Image
	Processes     workload.ProcessState
	Writes        []writeReceipt
}

type actorExecReceipt struct {
	PID      int
	Scenario string
	Budget   int64
	FDs      map[string]int
	Output   []byte
	Input    []byte
	Exit     int
}

// Real: original master/slave, one measured short Write, same-PID exec,
// process table and replay import. Controlled: file gates for slave progress.
// The helper is this already-built test binary; spawnexec owns init dispatch.
// origin: hot-update S4 plan §4.1 / T04
func TestPtyActorSurvivesSamePIDExecWithBlockedSlave(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if mode := os.Getenv(actorImageMode); mode != "" {
		runActorImage(t, mode, "blocked")
		return
	}
	requirePTYImplementation(t)
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		t.Run("exec", func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			runActorExecParent(t, "blocked")
		})
	}
	testharness.AssertNoGoroutineLeak(t, "same PID actor exec", g0)
	testharness.AssertNoFDLeak(t, "same PID actor exec", fd0, 0)
}

func runActorExecParent(t *testing.T, scenario string) {
	t.Helper()
	// Do not fork a second go command: the race/overlay-instrumented binary is
	// the exact binary exec'd in both generations of this harness.
	ctx, cancel := context.WithTimeout(t.Context(), 3*actorTestBudget)
	defer cancel()
	name := strings.Split(t.Name(), "/")[0]
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+name+"$", "-test.count=1", "-test.v")
	root := t.TempDir()

	t.Cleanup(func() {
		data, err := os.ReadFile(filepath.Join(root, "leader.pid"))
		if err == nil {
			pid, err := strconv.Atoi(string(data))
			if err == nil && pid > 0 {
				_ = syscall.Kill(-pid, syscall.SIGKILL)
			}
		}
	})
	cmd.Dir = root
	cmd.Env = append(actorEnvironment("before:"+scenario), "TETHER_TEST_PTY_ACTOR_ROOT="+root)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	defer func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	}()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("same-PID actor helper: %v\n%s", err, output)
	}
	if bytes.Contains(output, []byte(`"secret"`)) || bytes.Contains(output, []byte("[redacted]")) {
		t.Fatal("helper leaked image secret")
	}
	data, err := os.ReadFile(filepath.Join(root, "receipt.json"))
	if err != nil {
		t.Fatal(err)
	}
	var receipt actorExecReceipt
	if err := json.Unmarshal(data, &receipt); err != nil {
		t.Fatal(err)
	}
	if receipt.PID != cmd.Process.Pid || receipt.Scenario != scenario || receipt.Budget != 0 {
		t.Fatalf("invalid live-owner receipt: %+v", receipt)
	}
	for kind, n := range receipt.FDs {
		if n != 0 {
			t.Fatalf("live owner retained %s=%d", kind, n)
		}
	}
	wantOutput, wantExit := "OUTPUT_BEFORE_CUTOUTPUT_AFTER_CUT", 23
	if scenario == "tail" {
		wantOutput = "LEADER_PREFIXDESCENDANT_TAIL_AFTER_CUT"
	}
	if strings.HasPrefix(scenario, "fin-") {
		wantOutput = "AFTER_FIN"
		wantExit = 0
	}
	if string(receipt.Output) != wantOutput || receipt.Exit != wantExit {
		t.Fatalf("original consumer saw output=%q exit=%d", receipt.Output, receipt.Exit)
	}
	if scenario != "tail" && !bytes.Equal(receipt.Input, inputPayload(65536)) {
		t.Fatal("original slave lost or duplicated input")
	}

}

func runActorImage(t *testing.T, mode, kind string) {
	t.Helper()
	stage, scenario, ok := strings.Cut(mode, ":")
	if !ok {
		t.Fatal("invalid actor image mode")
	}
	if !strings.HasPrefix(scenario, kind) {
		t.Fatalf("wrong helper scenario %q for %q", scenario, kind)
	}
	switch stage {
	case "before":
		actorBeforeExec(t, scenario)
	case "after":
		actorAfterExec(t, scenario)
	default:
		t.Fatal("unknown actor helper stage")
	}
}

func blockedChild() string {
	return `tty.setraw(0)
mark('ready')
os.write(1,b'OUTPUT_BEFORE_CUT')
import fcntl,struct
while struct.unpack('I',fcntl.ioctl(0,termios.FIONREAD,struct.pack('I',0)))[0]==0: time.sleep(.002)
mark('blocked')
wait('drain')
data=b''
while len(data)<65536: data+=os.read(0,65536-len(data))
mark('received',data)
os.write(1,b'OUTPUT_AFTER_CUT')
wait('exit')
sys.exit(23)
`
}

func tailChild() string {
	return `tty.setraw(0)
signal.signal(signal.SIGHUP,signal.SIG_IGN)
child=os.fork()
if child:
 wait('descendant')
 os.write(1,b'LEADER_PREFIX')
 os._exit(23)
mark('descendant')
wait('tail')
os.write(1,b'DESCENDANT_TAIL_AFTER_CUT')
os._exit(0)
`
}

func actorBeforeExec(t *testing.T, scenario string) {
	t.Helper()
	body := blockedChild()
	if scenario == "tail" {
		body = tailChild()
	}
	if strings.HasPrefix(scenario, "fin-") {
		body = finChild(strings.TrimPrefix(scenario, "fin-"))
	}
	f := startActorFixture(t, body)
	var observed *observedMaster
	var releaseWrite chan struct{}
	if scenario == "tail" {
		waitOutput(t, f.a, "LEADER_PREFIX")
		awaitActor(t, "leader zombie before tail", func() bool { return processZombie(f.pid) })
		row, ok := f.manager.Lookup("terminal")
		if !ok || !row.ReapHeld || row.Exit != nil {
			t.Fatal("leader was reaped before descendant tail")
		}
		// Timing is the phenomenon under test: the descendant's file gate remains
		// closed beyond the removed 200ms tail timer, with no extra slave keeper.
		timer := time.NewTimer(250 * time.Millisecond)
		<-timer.C
	} else {
		waitMark(t, f.dir, "ready")
		observed = instrumentActor(t, f.a)
		if scenario == "blocked" {
			releaseWrite = make(chan struct{})
			defer func() {
				if !stopped(releaseWrite) {
					close(releaseWrite)
				}
			}()
			observed.writeReturned = make(chan struct{}, 1)
			observed.releaseWrite = releaseWrite
		}
		enqueueActor(t, f.a, inputPayload(65536), strings.HasPrefix(scenario, "fin-"))
		if scenario == "blocked" {
			waitMark(t, f.dir, "blocked")
		} else {
			awaitActor(t, "real master write in flight", func() bool { observed.mu.Lock(); defer observed.mu.Unlock(); return observed.writeActive != 0 })
			timer := time.NewTimer(30 * time.Millisecond)
			<-timer.C
		}
	}
	var image Image
	var files map[string]*os.File
	if scenario == "blocked" {
		// Freeze wakes the actual blocked write; its n>0 return is held before
		// accounting, so an omitted writer join cannot win a scheduling race.
		// origin: hot-update S4 plan §4.1 / T04
		type cutResult struct {
			image Image
			files map[string]*os.File
			err   error
		}
		done := make(chan cutResult, 1)
		go func() { s, f, err := f.a.Freeze(time.Now().Add(actorTestBudget)); done <- cutResult{s, f, err} }()
		select {
		case <-observed.writeReturned:
		case <-time.After(actorTestBudget):
			t.Fatal("freeze did not interrupt original Write")
		}
		select {
		case result := <-done:
			t.Fatalf("freeze passed before Write n accounting: %v", result.err)
		case <-time.After(20 * time.Millisecond):
		}
		close(releaseWrite)
		result := <-done
		requireDependency(t, result.err)
		image, files = result.image, result.files
	} else {
		image, files = freezeActor(t, f.a)
	}
	fixture := actorExecImage{OwnerPID: os.Getpid(), Dir: f.dir, Scenario: scenario, Image: image}
	if observed != nil {
		observed.mu.Lock()
		fixture.Writes = append([]writeReceipt(nil), observed.writes...)
		observed.mu.Unlock()
		total, short := 0, false
		for _, w := range fixture.Writes {
			total += w.N
			if w.N > 0 && w.N < w.Attempt {
				short = true
			}
		}
		if !short || uint64(total) != image.Input.Base || image.Input.Base == 0 || image.Input.Base >= image.Input.End {
			t.Fatalf("no real short Write or wrong barrier: writes=%+v image=%+v", fixture.Writes, image.Input)
		}
		if scenario == "blocked" && image.Output.End == 0 {
			t.Fatal("empty duplex output premise")
		}
		if strings.HasPrefix(scenario, "fin-") && (!image.InputFIN || image.InputEnded) {
			t.Fatal("FIN was not accepted and queued across exec")
		}
	}
	_, err := f.manager.FreezeUntil(time.Now().Add(actorTestBudget))
	requireDependency(t, err)
	fixture.Processes, err = f.manager.ExportProcesses()
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := handoff.Prepare(handoff.State{Role: "pty-actor-test", Instance: "original-terminal", Generation: 1, Data: data}, []handoff.Resource{{ID: "terminal", Kind: "pty-task-v1", Files: files}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	env := append(actorEnvironment("after:"+scenario), "TETHER_TEST_PTY_ACTOR_ROOT="+os.Getenv("TETHER_TEST_PTY_ACTOR_ROOT"))
	name := strings.Split(t.Name(), "/")[0]
	if err := prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^" + name + "$", "-test.count=1", "-test.v"}, env); err != nil {
		t.Fatal(err)
	}
	t.Fatal("exec returned")
}

func actorAfterExec(t *testing.T, scenario string) {
	t.Helper()
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatalf("restore: %v", err)
	}
	defer func() { _ = restored.Close() }()
	if twice, err := handoff.Restore(); twice != nil || err != nil {
		t.Fatal("handoff consumed twice")
	}
	var fixture actorExecImage
	if err := json.Unmarshal(restored.State.Data, &fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.OwnerPID != os.Getpid() || fixture.Scenario != scenario {
		t.Fatal("exec changed owner identity")
	}
	manager := &workload.Manager{}
	requireDependency(t, manager.RestoreProcesses(fixture.Processes))
	row, ok := manager.Lookup("terminal")
	if !ok {
		t.Fatal("missing original child")
	}
	budget := &workload.ReplayBudget{}
	inFile, err := restored.TakeFile("terminal", "input-replay")
	if err != nil {
		t.Fatal(err)
	}
	in, err := workload.RestoreReplay(budget, fixture.Image.Input, inFile)
	if err != nil {
		t.Fatal(err)
	}
	outFile, err := restored.TakeFile("terminal", "output-replay")
	if err != nil {
		t.Fatal(err)
	}
	out, err := workload.RestoreReplay(budget, fixture.Image.Output, outFile)
	if err != nil {
		t.Fatal(err)
	}
	masterFile, err := restored.TakeFile("terminal", "master")
	if err != nil {
		t.Fatal(err)
	}
	master, err := pty.RestoreMaster(masterFile, fixture.Image.Identity, row)
	requireDependency(t, err)
	a, err := Import(Config{Manager: manager, PID: "terminal", Master: master, Identity: fixture.Image.Identity, Input: in, Output: out, Clock: func() (int64, error) { return int64(time.Hour), nil }}, fixture.Image)
	requireDependency(t, err)
	settled := false
	t.Cleanup(func() {
		if settled {
			return
		}
		_ = syscall.Kill(-row.OSPID, syscall.SIGKILL)
		manager.Thaw()
		_ = a.Close()
		_ = manager.AllowReap("terminal")
		_, _ = manager.Wait("terminal")
		_ = manager.Forget("terminal")
	})
	if a.InputStatus().Applied != fixture.Image.Input.Base || budget.Used() != fixture.Image.Input.Capacity+fixture.Image.Output.Capacity {
		t.Fatal("import lost byte or budget responsibility")
	}
	requireDependency(t, a.PreActivate())
	manager.Thaw()
	requireDependency(t, a.Activate())
	if scenario == "tail" {
		if !processZombie(row.OSPID) || !row.ReapHeld {
			t.Fatal("tail did not preserve zombie leader")
		}
		markActor(t, fixture.Dir, "tail")
		waitOutput(t, a, "DESCENDANT_TAIL_AFTER_CUT")
		if got := string(actorBytes(t, a)); got != "LEADER_PREFIXDESCENDANT_TAIL_AFTER_CUT" {
			t.Fatalf("tail discontinuity: %q", got)
		}
	} else {
		markActor(t, fixture.Dir, "drain")
		waitMark(t, fixture.Dir, "received")
		requireFileBytes(t, fixture.Dir+"/received", inputPayload(65536))
		awaitActor(t, "all input applied", func() bool { return a.InputStatus().Applied == 65536 })
		if strings.HasPrefix(scenario, "fin-") {
			verifyFINAfterExec(t, a, fixture.Dir, strings.TrimPrefix(scenario, "fin-"))
		} else {
			waitOutput(t, a, "OUTPUT_AFTER_CUT")
		}
		markActor(t, fixture.Dir, "exit")
	}
	awaitActor(t, "source ended", func() bool { a.mu.Lock(); defer a.mu.Unlock(); return a.source == sourceEnded })
	result, err := manager.Wait("terminal")
	if err != nil {
		t.Fatal(err)
	}
	want := 23
	if strings.HasPrefix(scenario, "fin-") {
		want = 0
	}
	if result.Code != want {
		t.Fatalf("original terminal exit=%+v want=%d", result, want)
	}
	batch, err := a.OutputBatch(0, false, 32768)
	if err != nil || !batch.FIN || batch.Error != "" {
		t.Fatalf("final output %+v %v", batch, err)
	}
	_, err = a.OutputBatch(batch.Offset+uint64(len(batch.Data)), true, 1)
	if err != nil {
		t.Fatal(err)
	}
	requireDependency(t, a.Close())
	if err := manager.Forget("terminal"); err != nil {
		t.Fatal(err)
	}
	settled = true
	_ = restored.Close()
	receipt := actorExecReceipt{PID: os.Getpid(), Scenario: scenario, Budget: budget.Used(), FDs: actorFDKinds(t), Output: batch.Data, Exit: result.Code}
	if scenario != "tail" {
		receipt.Input, err = os.ReadFile(fixture.Dir + "/received")
		if err != nil {
			t.Fatal(err)
		}
	}
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(os.Getenv("TETHER_TEST_PTY_ACTOR_ROOT"), "receipt.json"), data, 0600); err != nil {
		t.Fatal(err)
	}
}

func verifyFINAfterExec(t *testing.T, a *Actor, dir, discipline string) {
	t.Helper()
	waitOutput(t, a, "AFTER_FIN")
	if s := a.InputStatus(); !s.FIN || !s.Ended || s.Error != "" {
		t.Fatalf("logical FIN caused input abort: %+v", s)
	}
	if _, err := os.Stat(filepath.Join(dir, "hup")); !os.IsNotExist(err) {
		t.Fatal("logical FIN sent HUP")
	}
	// A separate terminal writer is intentionally used only after proving the
	// actor's logical input ended, to test what real VEOF bytes mean to the slave.
	markActor(t, dir, "veof")
	waitMark(t, dir, "discipline-ready")
	data := []byte{4}
	want := []byte(nil)
	if discipline == "raw" {
		want = []byte{4}
	}
	if discipline == "custom" {
		data = []byte{4, 5}
		want = []byte{4}
	}
	if n, err := a.cfg.Master.Write(data); n != len(data) || err != nil {
		t.Fatalf("VEOF write=%d %v", n, err)
	}
	waitMark(t, dir, "veof-result")
	requireFileBytes(t, dir+"/veof-result", want)
}

func actorFDKinds(t *testing.T) map[string]int {
	t.Helper()
	kinds := map[string]int{"master": 0, "replay": 0, "pidfd": 0, "handoff": 0}
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		target, err := os.Readlink("/proc/self/fd/" + e.Name())
		if err != nil {
			continue
		}
		switch {
		case strings.Contains(target, "/dev/pts/ptmx"):
			kinds["master"]++
		case strings.Contains(target, "tether-replay"):
			kinds["replay"]++
		case strings.Contains(target, "pidfd"):
			kinds["pidfd"]++
		case strings.Contains(target, "tether-handoff"):
			kinds["handoff"]++
		}
	}
	return kinds
}
