//go:build linux

package agentimage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/testharness"
	"golang.org/x/sys/unix"
)

const ptyBundleMode = "TETHER_TEST_PTY_BUNDLE_MODE"
const ptyBundleReport = "TETHER_TEST_PTY_BUNDLE_REPORT"

type ptyRollbackHop struct {
	OwnerPID         int                  `json:"owner_pid"`
	OwnerTicks       int64                `json:"owner_ticks"`
	ChildPID         int                  `json:"child_pid"`
	ChildTicks       int64                `json:"child_ticks"`
	Generation       uint64               `json:"generation"`
	Identity         pty.TerminalIdentity `json:"identity"`
	Applied          uint64               `json:"applied"`
	Accepted         uint64               `json:"accepted"`
	OutputBase       uint64               `json:"output_base"`
	OutputEnd        uint64               `json:"output_end"`
	ControlSeq       uint64               `json:"control_seq"`
	ControlDigest    string               `json:"control_digest"`
	LastKASeq        uint64               `json:"last_ka_seq"`
	KADeadline       int64                `json:"ka_deadline"`
	SecondRestoreNil bool                 `json:"second_restore_nil"`
}

type ptyRollbackReceipt struct {
	Hops              [3]ptyRollbackHop `json:"hops"`
	Windows           [2]time.Duration  `json:"windows_ns"`
	Output            string            `json:"output"`
	ExitCode          int               `json:"exit_code"`
	Settled           bool              `json:"settled"`
	Spawned           [3]int            `json:"spawned"`
	ReplayAfterImport int               `json:"replay_after_import"`
	PIDFDsAfterImport int               `json:"pidfds_after_import"`
	MasterAfterClose  int               `json:"master_after_close"`
	ReplayAfterClose  int               `json:"replay_after_close"`
	HandoffAfterClose int               `json:"handoff_after_close"`
	PIDFDsAfterClose  int               `json:"pidfds_after_close"`
}

// Only the sealed handoff carries this fixture. The receipt carries neither
// the handle nor the bundle, whose registry includes its signing secret.
type ptyRollbackFixture struct {
	Handle  proto.ProcessHandle `json:"handle"`
	PID     string              `json:"pid"`
	ReadACK uint64              `json:"read_ack"`
	Receipt ptyRollbackReceipt  `json:"receipt"`
}

func ptyRollbackArgs() []string {
	return []string{os.Args[0], "-test.run=^TestPTYBundleRollbackCarriesCurrentState$", "-test.count=1"}
}

// origin: hot-update S4 plan §8 T24 — two real same-PID execs A→B→A,
// with new input, output ACKs, resize and keepalive committed by B. The third
// image checks the pre-exec oracle and resumes the original child to rc23.
func TestPTYBundleRollbackCarriesCurrentState(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	switch mode := os.Getenv(ptyBundleMode); mode {
	case "A":
		ptyRollbackBefore(t)
	case "B":
		ptyRollbackImport(t, 1)
	case "rollback-A":
		ptyRollbackImport(t, 2)
	case "":
		// No NATS in this harness: each helper's resources disappear before
		// the next round, so an exact parent FD baseline is meaningful.
		beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
		for round := range 5 {
			report := filepath.Join(t.TempDir(), fmt.Sprintf("receipt-%d.json", round))
			ctx, cancel := context.WithTimeout(t.Context(), 6*bundleTestBudget)
			cmd := exec.CommandContext(ctx, os.Args[0], ptyRollbackArgs()[1:]...)
			cmd.Dir = filepath.Dir(report)
			cmd.Env = append(ptyBundleEnvironment("A", report), "TMPDIR="+filepath.Dir(report))
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
			t.Cleanup(func() {
				if cmd.Process != nil {
					_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				}
			})
			cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
			output, err := cmd.CombinedOutput()
			cancel()
			if bytes.Contains(output, []byte(`"secret"`)) {
				t.Fatal("helper exposed registry secret")
			}
			if err != nil {
				t.Fatalf("PTY rollback helper: %v\n%s", err, output)
			}
			data, err := os.ReadFile(report)
			if err != nil {
				t.Fatal(err)
			}
			var receipt ptyRollbackReceipt
			if err := json.Unmarshal(data, &receipt); err != nil {
				t.Fatal(err)
			}
			checkPTYRollbackReceipt(t, receipt)
		}
		testharness.AssertNoGoroutineLeak(t, "pty-bundle-exec", beforeG)
		testharness.AssertNoFDLeak(t, "pty-bundle-exec", beforeFD, 0)
	default:
		t.Fatalf("unknown PTY bundle mode %q", mode)
	}
}

func ptyRollbackBefore(t *testing.T) {
	t.Helper()
	if src, err := handoff.Restore(); src != nil || err != nil {
		t.Fatal("first image received a dispatch")
	}
	f := newPTYBundleConfig(t, "pty-rollback-instance")
	var err error
	f.rt, err = execsession.New(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Disable echo and newline translation before the ready handshake. Three
	// newline-delimited inputs gate child progression without elapsed sleeps.
	script := `stty -echo -icanon min 1 time 0 -onlcr
printf '%s' "${TETHER_TEST_PTY_BUNDLE_MODE}${TETHER_TEST_PTY_BUNDLE_REPORT}${_TETHER_HANDOFF}"
printf ready
IFS= read -r first; printf '<%s>' "$first"
IFS= read -r second; printf '<%s>' "$second"; stty size
IFS= read -r final; printf '<%s>tail' "$final"; exit 23`
	h, pid := createBundlePTY(t, f, "original-terminal", script)
	fixture := ptyRollbackFixture{Handle: h, PID: pid}
	ptyRollbackRead(t, f, &fixture, "ready", false)
	ptyRollbackInput(t, f, h, 0, "one\n", 0)
	ptyRollbackRead(t, f, &fixture, "ready<one>", false)
	// Distinct payloads make stale control digests observable: sequence is
	// deliberately not part of the actor's payload digest.
	ptyRollbackControl(t, f, h, "resize", 1, &proto.PtyResizeEvent{Cols: 80, Rows: 24}, 0)
	// Both cuts retain an ACKed prefix AND replayable suffix.
	ptyRollbackACK(t, f, h, 3)
	fixture.ReadACK = 3
	cut, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Lineage: Lineage{Generation: 1}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	fixture.Receipt.Hops[0] = ptyRollbackObserve(t, cut.Bundle(), cut.resources, pid)
	fixture.Receipt.Hops[0].SecondRestoreNil = true
	fixture.Receipt.Spawned[0] = f.spawned
	ptyRollbackExec(t, cut, fixture, "B")
}

func ptyRollbackImport(t *testing.T, hop int) {
	t.Helper()
	src, err := handoff.Restore()
	if err != nil || src == nil {
		t.Fatalf("PTY handoff restore: %v", err)
	}
	defer func() { _ = src.Close() }()
	again, err := handoff.Restore()
	if err != nil || again != nil {
		t.Fatal("dispatch consumed twice")
	}
	var fixture ptyRollbackFixture
	for _, resource := range src.Resources {
		if resource.ID == "pty-rollback-fixture" {
			if err := json.Unmarshal(resource.Data, &fixture); err != nil {
				t.Fatal(err)
			}
		}
	}
	if fixture.PID == "" {
		t.Fatal("rollback fixture missing")
	}
	f := newPTYBundleConfig(t, "")
	f.cfg.StartPTY = func(string, *proto.RunReq) (*pty.Session, string, error) {
		t.Fatal("import respawned the original terminal")
		return nil, "", fmt.Errorf("unexpected spawn")
	}
	imp, salvage, err := Restore(src, Target{Runtime: f.cfg})
	if err != nil {
		t.Fatalf("PTY bundle restore: %v (salvage %v)", err, salvage)
	}
	f.rt = imp.Runtime
	_, resources, err := f.rt.Export()
	if err != nil {
		t.Fatal(err)
	}
	// Calibration remains in the handoff owner until activation/close.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
	resources, err = handoff.MergeResources(resources, src.Borrow())
	if err != nil {
		t.Fatal(err)
	}
	fixture.Receipt.Hops[hop] = ptyRollbackObserve(t, imp.Bundle, resources, fixture.PID)
	fixture.Receipt.Hops[hop].SecondRestoreNil = true
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}
	_, _, fixture.Receipt.ReplayAfterImport, _, fixture.Receipt.PIDFDsAfterImport = bundleExecCensus(t)
	if err := imp.Thaw(); err != nil {
		t.Fatal(err)
	}
	fixture.Receipt.Windows[hop-1] = imp.FreezeWindow()
	if hop == 1 {
		ptyRollbackControl(t, f, fixture.Handle, "resize", 2, &proto.PtyResizeEvent{Cols: 91, Rows: 37}, fixture.ReadACK)
		ptyRollbackControl(t, f, fixture.Handle, "keepalive", 3, nil, fixture.ReadACK)
		ptyRollbackInput(t, f, fixture.Handle, 4, "two\n", fixture.ReadACK)
		ptyRollbackRead(t, f, &fixture, "ready<one><two>37 91\n", false)
		ptyRollbackACK(t, f, fixture.Handle, 13)
		fixture.ReadACK = 13
		cut, err := Freeze(Owners{Runtime: f.rt, Manager: imp.Manager,
			Lineage: Lineage{Generation: imp.Bundle.Lineage.Generation + 1}}, time.Now().Add(bundleTestBudget))
		if err != nil {
			t.Fatal(err)
		}
		// Capture the oracle while B still owns the live child, before exec.
		fixture.Receipt.Hops[1] = ptyRollbackObserve(t, cut.Bundle(), cut.resources, fixture.PID)
		fixture.Receipt.Hops[1].SecondRestoreNil = true
		fixture.Receipt.Spawned[1] = f.spawned
		ptyRollbackExec(t, cut, fixture, "rollback-A")
		return
	}
	ptyRollbackInput(t, f, fixture.Handle, 8, "done\n", fixture.ReadACK)
	ptyRollbackRead(t, f, &fixture, "ready<one><two>37 91\n<done>tail", true)
	ctx, cancel := context.WithTimeout(t.Context(), bundleTestBudget)
	defer cancel()
	if err := f.rt.Close(ctx); err != nil {
		t.Fatal(err)
	}
	fixture.Receipt.Spawned[2] = f.spawned
	_, _, fixture.Receipt.ReplayAfterClose, fixture.Receipt.HandoffAfterClose, fixture.Receipt.PIDFDsAfterClose = bundleExecCensus(t)
	fixture.Receipt.MasterAfterClose = ptyMasterCount(t)
	data, err := json.Marshal(fixture.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv(ptyBundleReport), data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func ptyRollbackExec(t *testing.T, cut *Frozen, fixture ptyRollbackFixture, mode string) {
	t.Helper()
	data, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := cut.PrepareChecked(func(b Bundle) error {
		domains := b.RequiredDomains()
		for _, domain := range domains {
			if domain == "pty-v1" {
				return nil
			}
		}
		return fmt.Errorf("PTY cut lacks the required terminal domain")
	}, handoff.Resource{ID: "pty-rollback-fixture", Kind: "test", Data: data})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	if err := prepared.Exec(os.Args[0], ptyRollbackArgs(), ptyBundleEnvironment(mode, os.Getenv(ptyBundleReport))); err != nil {
		t.Fatal(err)
	}
	t.Fatal("successful exec returned")
}

func ptyRollbackInput(t *testing.T, f *bundleFixture, h proto.ProcessHandle, seq uint64, data string, outputACK uint64) {
	t.Helper()
	reply, err := f.handle(proto.ProcessResumeReq{Op: "input", Kind: proto.ProcessKindPTY, Handle: &h, AttachEpoch: 1, InputSeq: seq, Input: []byte(data), StdoutACK: outputACK})
	if err != nil || reply.InputAccepted != seq+uint64(len(data)) || reply.InputError != "" {
		t.Fatalf("input did not accept the full packet: ack=%d accepted=%d err=%v", reply.InputACK, reply.InputAccepted, err)
	}
}

func ptyRollbackControl(t *testing.T, f *bundleFixture, h proto.ProcessHandle, op string, seq uint64, size *proto.PtyResizeEvent, outputACK uint64) {
	t.Helper()
	reply, err := f.handle(proto.ProcessResumeReq{Op: op, Kind: proto.ProcessKindPTY, Handle: &h, AttachEpoch: 1, ControlSeq: seq, Resize: size, StdoutACK: outputACK})
	if err != nil || reply.ControlACK != seq || reply.ControlError != "" {
		t.Fatalf("control %s seq%d: ack=%d error=%s err=%v", op, seq, reply.ControlACK, reply.ControlError, err)
	}
}

func ptyRollbackACK(t *testing.T, f *bundleFixture, h proto.ProcessHandle, ack uint64) {
	t.Helper()
	reply, err := f.handle(proto.ProcessResumeReq{Op: "ack", Kind: proto.ProcessKindPTY, Handle: &h, AttachEpoch: 1, StdoutACK: ack})
	if err != nil || reply.Kind == "error" {
		t.Fatalf("output ACK: %v %s", err, reply.Code)
	}
}

func ptyRollbackRead(t *testing.T, f *bundleFixture, fixture *ptyRollbackFixture, want string, terminal bool) {
	t.Helper()
	deadline := time.Now().Add(bundleTestBudget)
	for {
		reply, err := f.handle(proto.ProcessResumeReq{Op: "resume", Kind: proto.ProcessKindPTY, Handle: &fixture.Handle, AttachEpoch: 1, StdoutACK: fixture.ReadACK})
		if err != nil || reply.Kind == "error" {
			t.Fatalf("PTY resume: %v %s", err, reply.Code)
		}
		if output := reply.Stdout; output != nil {
			if output.Offset != fixture.ReadACK || output.Error != "" {
				t.Fatal("output gap, duplicate or abort")
			}
			overlap := uint64(len(fixture.Receipt.Output)) - output.Offset
			if overlap > uint64(len(output.Data)) {
				t.Fatal("PTY output lost a retained prefix")
			}
			if string(output.Data[:overlap]) != fixture.Receipt.Output[output.Offset:] {
				t.Fatal("PTY output replay changed already observed bytes")
			}
			fixture.Receipt.Output += string(output.Data[overlap:])
		}
		if len(fixture.Receipt.Output) > len(want) || (len(fixture.Receipt.Output) == len(want) && fixture.Receipt.Output != want) {
			t.Fatalf("PTY bytes: %q want %q", fixture.Receipt.Output, want)
		}
		if fixture.Receipt.Output == want && (!terminal || (reply.Phase == "terminal" && reply.Stdout != nil && reply.Stdout.FIN)) {
			if terminal {
				fixture.Receipt.ExitCode = reply.ExitCode
				settled, err := f.handle(proto.ProcessResumeReq{Op: "terminal-ack", Kind: proto.ProcessKindPTY, Handle: &fixture.Handle, AttachEpoch: 1,
					TerminalID: reply.TerminalID, StdoutACK: uint64(len(want)), StdoutFIN: true, StderrFIN: true})
				if err != nil || settled.Phase != "settled" {
					t.Fatalf("PTY terminal did not settle: %v %s", err, settled.Phase)
				}
				fixture.Receipt.Settled = true
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("PTY bytes or terminal did not arrive: %q want %q", fixture.Receipt.Output, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func ptyRollbackObserve(t *testing.T, b Bundle, resources []handoff.Resource, pid string) ptyRollbackHop {
	t.Helper()
	if len(b.Runtime.Tasks) != 1 || len(b.Processes.Records) != 1 || b.Runtime.Tasks[0].PID != pid {
		t.Fatal("original terminal owner missing")
	}
	state := ptyTaskState(t, b.Runtime.Tasks[0])
	row := b.Processes.Records[0]
	hop := ptyRollbackHop{OwnerPID: os.Getpid(), OwnerTicks: bundleChainStartTicks(t, os.Getpid()), ChildPID: row.OSPID,
		ChildTicks: bundleChainStartTicks(t, row.OSPID), Generation: b.Lineage.Generation, Identity: state.Identity,
		Applied: state.Input.Base, Accepted: state.Input.End, OutputBase: state.Output.Base, OutputEnd: state.Output.End,
		ControlSeq: state.ControlSeq, ControlDigest: state.ControlDigest, LastKASeq: state.LastKASeq, KADeadline: state.KADeadlineBootNS}
	if row.ID != pid || row.Exit != nil || row.StartTimeTicks != hop.ChildTicks || state.Identity.LeaderPID != row.OSPID || state.Identity.StartTimeTicks != hop.ChildTicks {
		t.Fatal("PTY image and original kernel child differ")
	}
	// Runtime exports contain exactly the PTY; composed cuts additionally
	// retain one independently checked calibration resource.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility.
	if b.admissionAt != 0 {
		if len(resources) != 2 || resources[1].ID != calibrationResourceID || resources[1].Kind != calibrationResourceKind || len(resources[1].Files) != 1 || resources[1].Files["state"] == nil {
			t.Fatal("composed PTY cut lost calibration custody")
		}
		resources = resources[:1]
	}
	if len(resources) != 1 || resources[0].Kind != "pty-task-v1" || len(resources[0].Files) != 3 {
		t.Fatal("PTY resource must carry one master and two replays")
	}
	master := resources[0].Files["master"]
	if master == nil {
		t.Fatal("PTY master missing")
	}
	raw, err := master.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	var index int
	var checkErr error
	if err := raw.Control(func(fd uintptr) {
		checkErr = unix.Fstat(int(fd), &stat)
		if checkErr == nil {
			index, checkErr = unix.IoctlGetInt(int(fd), unix.TIOCGPTN)
		}
	}); err != nil || checkErr != nil {
		t.Fatalf("kernel master check: %v %v", err, checkErr)
	}
	if state.Identity.Dev != uint64(stat.Dev) || state.Identity.Ino != stat.Ino || state.Identity.Rdev != uint64(stat.Rdev) || state.Identity.PTYIndex != uint32(index) {
		t.Fatal("PTY image does not name the actual original master")
	}
	return hop
}

func ptyMasterCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		target, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err == nil && (target == "/dev/pts/ptmx" || target == "/dev/ptmx") {
			count++
		}
	}
	return count
}

func checkPTYRollbackReceipt(t *testing.T, r ptyRollbackReceipt) {
	t.Helper()
	first, middle, last := r.Hops[0], r.Hops[1], r.Hops[2]
	if first.OwnerPID <= 0 || first.OwnerTicks <= 0 || first.ChildPID <= 0 || first.ChildTicks <= 0 || !first.Identity.Started || first.Identity.MountID == 0 || first.Identity.BootID == "" {
		t.Fatal("PTY identity oracle is empty")
	}
	for _, hop := range r.Hops {
		if hop.OwnerPID != first.OwnerPID || hop.OwnerTicks != first.OwnerTicks || hop.ChildPID != first.ChildPID || hop.ChildTicks != first.ChildTicks || hop.Identity != first.Identity || !hop.SecondRestoreNil {
			t.Fatal("two-hop rollback changed the owner, child or master identity")
		}
	}
	if first.Generation != 1 || middle.Generation != 2 || last.Generation != 2 {
		t.Fatalf("rollback generations: %d %d %d", first.Generation, middle.Generation, last.Generation)
	}
	if first.Applied != 4 || first.Accepted != 4 || middle.Applied != 8 || middle.Accepted != 8 || last.Applied != 8 || last.Accepted != 8 {
		t.Fatal("rollback reused the first image's input state")
	}
	if first.OutputBase != 3 || first.OutputEnd != 10 || middle.OutputBase != 13 || middle.OutputEnd != 21 || last.OutputBase != middle.OutputBase || last.OutputEnd != middle.OutputEnd {
		t.Fatalf("rollback output ACK/base state: first=%d/%d middle=%d/%d last=%d/%d", first.OutputBase, first.OutputEnd, middle.OutputBase, middle.OutputEnd, last.OutputBase, last.OutputEnd)
	}
	if first.ControlSeq != 1 || middle.ControlSeq != 3 || last.ControlSeq != 3 || middle.ControlDigest == "" || middle.ControlDigest == first.ControlDigest || last.ControlDigest != middle.ControlDigest || middle.LastKASeq != 3 || last.LastKASeq != 3 || middle.KADeadline <= 0 || last.KADeadline != middle.KADeadline {
		t.Fatal("rollback lost the second generation's control or absolute KA receipt")
	}
	if r.Output != "ready<one><two>37 91\n<done>tail" || r.ExitCode != 23 || !r.Settled || r.Spawned != [3]int{1, 0, 0} {
		t.Fatalf("original PTY bytes/lifecycle: output=%q rc=%d settled=%t spawned=%v", r.Output, r.ExitCode, r.Settled, r.Spawned)
	}
	if r.ReplayAfterImport != 2 || r.PIDFDsAfterImport != 1 || r.MasterAfterClose != 0 || r.ReplayAfterClose != 0 || r.HandoffAfterClose != 0 || r.PIDFDsAfterClose != 0 {
		t.Fatal("PTY master/replay/pidfd/handoff resource census did not return to zero")
	}
	for _, window := range r.Windows {
		if window <= 0 {
			t.Fatal("missing freeze-window calibration")
		}
		t.Logf("PTY freeze-window calibration: %s", window)
	}
}
