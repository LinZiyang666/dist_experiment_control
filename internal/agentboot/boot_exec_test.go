//go:build linux

// boot_exec_test.go runs a dispatcher -> real tether -> receiver in one PID.
// The receiver is this test binary's init role, before Go test flag parsing:
// it deliberately receives tether's invalid Cobra arguments unchanged.
// Real: the product binary, exec, sealed handoff and original child/pipes.
// Stubbed: the previous product image (receiver); no broker or deploy tier.
// origin: hot-update S3 plan §1.2 / §1.6 / §7 T1/T2/T13.
package agentboot

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
	"golang.org/x/sys/unix"
)

const (
	bootExecPrefix   = "_TETHER_BOOT_EXEC_"
	bootExecMode     = bootExecPrefix + "MODE"
	bootExecDir      = bootExecPrefix + "DIR"
	bootExecTest     = bootExecPrefix + "TEST"
	bootExecInstance = "abcdefghijklmnopqrstuvwx23"
)

var bootExecBuild struct {
	sync.Once
	dir, path string
	err       error
}
var bootExecRace bool

func TestMain(m *testing.M) {
	code := m.Run()
	if bootExecBuild.dir != "" {
		_ = os.RemoveAll(bootExecBuild.dir)
	}
	os.Exit(code)
}

func init() {
	mode := os.Getenv(bootExecMode)
	if mode != "receiver" && mode != "corrupt-after" && mode != "corrupt-field-after" {
		return
	}
	var err error
	if mode == "receiver" {
		err = receiveBootExec()
	} else {
		err = receiveBrokenBootExec()
	}
	if err != nil {
		// Helpers return static oracle failures, never a bundle or decoder input.
		fmt.Fprintln(os.Stderr, "BOOT_ORACLE_REJECT:", err)
		os.Exit(1)
	}
	os.Exit(0)
}

func buildBootTether(t *testing.T) string {
	t.Helper()
	bootExecBuild.Do(func() {
		bootExecBuild.dir, bootExecBuild.err = os.MkdirTemp("", "tether-bootstrap-")
		if bootExecBuild.err != nil {
			return
		}
		bootExecBuild.path = filepath.Join(bootExecBuild.dir, "tether")
		cmd := exec.Command("go", "build", "-trimpath", "-o", bootExecBuild.path, "./cmd/tether")
		if bootExecRace {
			cmd.Args = append(cmd.Args[:2:2], append([]string{"-race"}, cmd.Args[2:]...)...)
		}
		cmd.Dir = "../.."
		// GOFLAGS=-overlay is inherited by this nested build for mutations.
		if out, err := cmd.CombinedOutput(); err != nil {
			bootExecBuild.err = fmt.Errorf("build tether: %w: %s", err, out)
		}
	})
	if bootExecBuild.err != nil {
		t.Fatal(bootExecBuild.err)
	}
	return bootExecBuild.path
}

func bootExecEnvironment(mode, dir, test string) []string {
	var env []string
	for _, kv := range handoff.CleanEnvironment(os.Environ()) {
		if strings.HasPrefix(kv, bootExecPrefix) || strings.HasPrefix(kv, "TETHER_INSTANCE_ID=") || strings.HasPrefix(kv, "TETHER_ROUTING_NID=") || strings.HasPrefix(kv, "TETHER_HOME=") || strings.HasPrefix(kv, "HOME=") {
			continue
		}
		env = append(env, kv)
	}
	// All command paths, including version, are isolated from the real home.
	env = append(env, "HOME="+dir, "TETHER_HOME="+dir)
	if mode != "" {
		env = append(env, bootExecMode+"="+mode, bootExecDir+"="+dir, bootExecTest+"="+test)
	}
	return env
}

func bootCopy(t *testing.T, from, to string) {
	t.Helper()
	src, err := os.Open(from)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(to, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(dst, src)
	cerr := dst.Close()
	if err != nil || cerr != nil {
		t.Fatalf("copy binary: %v %v", err, cerr)
	}
}

// drill-pin: mirrors internal/agent/upgrade_state.go upgradeMarker
// Only the boot and resource receipt fields are needed; the product decodes it.
type bootExecMarker struct {
	State          string    `json:"state"`
	PrevSHA        string    `json:"prev_sha"`
	NewSHA         string    `json:"new_sha"`
	PrevVersion    string    `json:"prev_version"`
	NewVersion     string    `json:"new_version"`
	Deadline       time.Time `json:"deadline"`
	BootCount      int       `json:"boot_count"`
	BootBudget     int       `json:"boot_budget"`
	UpgradeID      string    `json:"upgrade_id"`
	TargetInstance string    `json:"target_instance"`
	ResourcePhase  string    `json:"resource_phase,omitempty"`
	Detail         string    `json:"detail,omitempty"`
}

type bootExecIdentity struct{ Dev, Ino uint64 }
type bootExecOracle struct {
	PID         int
	ChildPID    int
	StartTicks  int64
	StateSHA    string
	Files       map[string]bootExecIdentity
	AdmissionAt int64
}
type bootExecReceipt struct {
	bootExecOracle
	Instance         string
	Output           string
	ExitCode         int
	HandoffMemfds    int
	ReplayMemfds     int
	PidFDs           int
	AfterClose       [5]int
	SecondRestoreNil bool
}

func bootDigest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }
func bootFileDigest(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bootDigest(b)
}
func bootWriteJSON(path string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func bootFileIdentities(resources []handoff.Resource) (map[string]bootExecIdentity, error) {
	out := make(map[string]bootExecIdentity)
	for _, res := range resources {
		for name, f := range res.Files {
			c, err := f.SyscallConn()
			if err != nil {
				return nil, err
			}
			var st unix.Stat_t
			var statErr error
			if err := c.Control(func(fd uintptr) { statErr = unix.Fstat(int(fd), &st) }); err != nil || statErr != nil {
				return nil, errors.New("file identity unavailable")
			}
			out[res.ID+"/"+name] = bootExecIdentity{st.Dev, st.Ino}
		}
	}
	return out, nil
}

// Excludes stdio: the parent intentionally captures those in pipes. The five
// categories are total, pipes, replay, handoff, pidfd; total alone hides leaks.
func bootFDCensus() [5]int {
	var n [5]int
	entries, _ := os.ReadDir("/proc/self/fd")
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil || fd < 3 {
			continue
		}
		name, err := os.Readlink("/proc/self/fd/" + entry.Name())
		if err != nil {
			continue
		}
		n[0]++
		switch {
		case strings.HasPrefix(name, "pipe:["):
			n[1]++
		case strings.Contains(name, "memfd:tether-replay"):
			n[2]++
		case strings.Contains(name, "memfd:tether-handoff"):
			n[3]++
		case strings.Contains(name, "anon_inode:[pidfd]"):
			n[4]++
		}
	}
	return n
}

func bootHelperCommand(t *testing.T, mode, dir string) *exec.Cmd {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+t.Name()+"$", "-test.count=1")

	cmd.Dir = dir
	cmd.Env = append(bootExecEnvironment(mode, dir, t.Name()), "TMPDIR="+dir)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
	cmd.WaitDelay = 3 * time.Second
	// A failed receiver may exit before its child. Wait ending the context
	// watcher must not orphan that child when an oracle rejects the image.
	t.Cleanup(func() {
		if cmd.Process != nil {
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		}
	})
	t.Cleanup(func() {
		// Managed children intentionally have their own process group.
		// A rejected receiver has no runtime left to reap that group.
		if _, err := os.Stat(filepath.Join(dir, "after.json")); err == nil {
			return
		}
		data, err := os.ReadFile(filepath.Join(dir, "before.json"))
		var before bootExecOracle
		if err == nil && json.Unmarshal(data, &before) == nil && before.ChildPID > 0 {
			_ = syscall.Kill(-before.ChildPID, syscall.SIGKILL)
		}
	})
	return cmd
}

func dispatchBootExec(t *testing.T) {
	t.Helper()
	dir, name := os.Getenv(bootExecDir), os.Getenv(bootExecTest)
	// Bidirectional env stripping: the business child proves the mode and
	// private dispatch keys are absent, not merely ignored by its shell.
	for _, key := range []string{bootExecMode, bootExecDir, bootExecTest, "TETHER_INSTANCE_ID", "TETHER_ROUTING_NID"} {
		_ = os.Unsetenv(key)
	}
	f := newCutFixture(t, "boot-exec-runtime")
	gate := filepath.Join(dir, "continue")
	h := f.create(t, []string{"/bin/sh", "-c", `printf 'env:%s%s;before;' "$_TETHER_BOOT_EXEC_MODE" "$_TETHER_HANDOFF"; while [ ! -e ` + gate + ` ]; do sleep 0.02; done; printf after; exit 23`})
	admissionAt, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := agentimage.Freeze(agentimage.Owners{Runtime: f.rt, Manager: f.manager, FrozenAtBootNS: admissionAt, Lineage: agentimage.Lineage{Generation: 9, InstanceID: bootExecInstance, RoutingNID: "node"}}, time.Now().Add(imageTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := frozen.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	b := frozen.Bundle()
	data, err := agentimage.Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	state := handoff.State{Role: agentimage.Role, Instance: b.Instance, Generation: b.Lineage.Generation, Data: data}
	stateBytes, _ := json.Marshal(state)
	_, resources, err := f.rt.Export()
	if err != nil {
		t.Fatal(err)
	}
	files, err := bootFileIdentities(resources)
	if err != nil {
		t.Fatal(err)
	}
	// The composed image carries one calibration descriptor in addition to the
	// exec resources. Both the frozen original and Prepared's duplicate must
	// name that same inode; the receiver must preserve its exact identity.
	// origin: hot-update S5 plan §7.1 / DRILL-RUN S3 compatibility
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	calibrationCount := 0
	for _, entry := range entries {
		path := "/proc/self/fd/" + entry.Name()
		name, _ := os.Readlink(path)
		if !strings.Contains(name, "memfd:tether-sealed-state") {
			continue
		}
		var st unix.Stat_t
		if err := unix.Stat(path, &st); err != nil {
			t.Fatal(err)
		}
		identity := bootExecIdentity{st.Dev, st.Ino}
		if calibrationCount > 0 && files["image-freeze-calibration/state"] != identity {
			t.Fatal("dispatcher calibration has multiple identities")
		}
		files["image-freeze-calibration/state"] = identity
		calibrationCount++
	}
	if calibrationCount != 2 {
		t.Fatal("dispatcher must own one calibration original and one prepared duplicate")
	}
	row := b.Processes.Records[0]
	oracle := bootExecOracle{PID: os.Getpid(), ChildPID: row.OSPID, StartTicks: row.StartTimeTicks, StateSHA: bootDigest(stateBytes), Files: files, AdmissionAt: admissionAt}
	if h.RequestID == "" || len(files) < 4 {
		t.Fatal("empty dispatcher fixture")
	}
	if err := bootWriteJSON(filepath.Join(dir, "before.json"), oracle); err != nil {
		t.Fatal(err)
	}
	exe := filepath.Join(dir, "tether")
	env := append(bootExecEnvironment("receiver", dir, name), "TETHER_INSTANCE_ID="+bootExecInstance, "TETHER_ROUTING_NID=node")
	if err := prepared.Exec(exe, []string{exe, "agent", "--flag-that-does-not-exist"}, env); err != nil {
		t.Fatal(err)
	}
}

func receiveBootExec() error {
	dir := os.Getenv(bootExecDir)
	var want bootExecOracle
	data, err := os.ReadFile(filepath.Join(dir, "before.json"))
	if err != nil || json.Unmarshal(data, &want) != nil {
		return errors.New("missing before receipt")
	}
	r, err := handoff.Restore()
	if err != nil || r == nil {
		return errors.New("receiver has no dispatch")
	}
	defer func() { _ = r.Close() }()
	stateBytes, _ := json.Marshal(r.State)
	if bootDigest(stateBytes) != want.StateSHA || os.Getpid() != want.PID {
		return errors.New("state or PID changed across real tether")
	}
	files, err := bootFileIdentities(r.Borrow())
	if err != nil {
		return errors.New("receiver file census failed")
	}
	if len(files) != len(want.Files) {
		return errors.New("receiver lost descriptors")
	}
	for key, id := range want.Files {
		if files[key] != id {
			return errors.New("receiver file identity changed")
		}
	}
	calibrationCount := 0
	for _, resource := range r.Borrow() {
		if resource.ID == "image-freeze-calibration" {
			if resource.Kind != "image-freeze-calibration-v1" || string(resource.Data) != strconv.FormatInt(want.AdmissionAt, 10) || len(resource.Files) != 1 || resource.Files["state"] == nil {
				return errors.New("receiver calibration changed across rollback")
			}
			calibrationCount++
		}
	}
	if len(r.Resources) != 2 || calibrationCount != 1 {
		return errors.New("receiver composed resource inventory changed")
	}
	if err := syscall.Kill(want.ChildPID, 0); err != nil {
		return errors.New("child died before import")
	}
	manager := &workload.Manager{}
	imp, _, err := agentimage.Restore(r, agentimage.Target{Runtime: execsession.Config{Manager: manager,
		Start: func(string, *proto.ExecReq) (*workload.Stdio, string, error) {
			return nil, "", errors.New("receiver must not spawn")
		},
		Started: func(string, proto.ExecReq, workload.Identity) {}, Exited: func(string, int) {},
	}})
	if err != nil {
		return errors.New("receiver import refused")
	}
	if len(imp.Bundle.Runtime.Tasks) != 1 {
		return errors.New("receiver did not import one task")
	}
	row := manager.Snapshot()[0]
	if row.OSPID != want.ChildPID || row.StartTimeTicks != want.StartTicks {
		return errors.New("receiver process identity changed")
	}
	if err := imp.Thaw(); err != nil {
		return errors.New("receiver thaw refused")
	}
	afterImport := bootFDCensus()
	if afterImport[3] != 0 || afterImport[2] < 2 || afterImport[4] != 1 {
		return errors.New("receiver descriptor categories differ")
	}
	task := imp.Bundle.Runtime.Tasks[0]
	h := task.Handle
	scope := procrequest.Scope{SID: h.Lease.SID, NID: h.Lease.NID, ActorFP: h.Lease.ActorFP, ClientLineage: h.Lease.ClientLineage}
	if err := os.WriteFile(filepath.Join(dir, "continue"), nil, 0o600); err != nil {
		return errors.New("cannot release child gate")
	}
	var out, stderr []byte
	var terminal proto.ProcessResumeReply
	var outFIN, errFIN bool
	deadline := time.Now().Add(imageTestBudget)
	for time.Now().Before(deadline) {
		reply, err := imp.Runtime.Handle(scope, proto.ProcessResumeReq{Version: proto.ProcessResumeVersion, CallID: 7, Op: "resume", ActorFP: scope.ActorFP, Handle: &h, AttachEpoch: 1,
			StdoutACK: uint64(len(out)), StderrACK: uint64(len(stderr)), StdoutFIN: outFIN, StderrFIN: errFIN}, "_TINBOX.boot")
		if err != nil {
			return errors.New("receiver resume refused")
		}
		if reply.Stdout != nil {
			out = append(out, reply.Stdout.Data...)
			outFIN = reply.Stdout.FIN
		}
		if reply.Stderr != nil {
			stderr = append(stderr, reply.Stderr.Data...)
			errFIN = reply.Stderr.FIN
		}
		if reply.Phase == "terminal" && outFIN && errFIN {
			terminal = reply
			break
		}
		time.Sleep(time.Millisecond)
	}
	if string(out) != "env:;before;after" || len(stderr) != 0 || terminal.ExitCode != 23 {
		return errors.New("original child did not finish with continuous output")
	}
	ctx, cancel := context.WithTimeout(context.Background(), imageTestBudget) // ctx-root: receiver teardown after exec
	defer cancel()
	if err := imp.Runtime.Close(ctx); err != nil {
		return errors.New("receiver close failed")
	}
	_ = r.Close()
	afterClose := bootFDCensus()
	if afterClose[1] != 0 || afterClose[2] != 0 || afterClose[3] != 0 || afterClose[4] != 0 || afterClose[0] >= afterImport[0] {
		return errors.New("receiver leaked carried resources")
	}
	second, err := handoff.Restore()
	if err != nil || second != nil {
		return errors.New("dispatch consumed twice")
	}
	receipt := bootExecReceipt{bootExecOracle: want, Instance: os.Getenv("TETHER_INSTANCE_ID"), Output: string(out), ExitCode: terminal.ExitCode,
		HandoffMemfds: afterImport[3], ReplayMemfds: afterImport[2], PidFDs: afterImport[4], AfterClose: afterClose, SecondRestoreNil: true}
	return bootWriteJSON(filepath.Join(dir, "after.json"), receipt)
}

func exerciseBootExec(t *testing.T, count int) {
	t.Helper()
	dir := t.TempDir()
	bin := buildBootTether(t)
	bootCopy(t, bin, filepath.Join(dir, "tether"))
	bootCopy(t, os.Args[0], filepath.Join(dir, "tether.prev"))
	m := bootExecMarker{State: "pending", NewSHA: bootFileDigest(t, filepath.Join(dir, "tether")), PrevSHA: bootFileDigest(t, filepath.Join(dir, "tether.prev")),
		PrevVersion: "before", NewVersion: "after", Deadline: time.Now().Add(time.Hour), BootCount: count, BootBudget: upgradebudget.BootBudget, UpgradeID: "boot-exec-proof", TargetInstance: bootExecInstance}
	marker := filepath.Join(dir, ".tether-upgrade.json")
	if err := bootWriteJSON(marker, m); err != nil {
		t.Fatal(err)
	}
	cmd := bootHelperCommand(t, "dispatch", dir)
	out, err := cmd.CombinedOutput()
	if strings.Contains(string(out), "secret") || strings.Contains(string(out), "[redacted]") {
		t.Fatal("helper output contains registry material")
	}
	if err != nil {
		t.Fatalf("same-PID real tether: %v\n%s", err, out)
	}
	if strings.Contains(string(out), "image restored") {
		t.Fatal("Cobra or boot rollback ran after image restore")
	}
	data, err := os.ReadFile(filepath.Join(dir, "after.json"))
	if err != nil {
		t.Fatalf("receiver receipt missing: %v\n%s", err, out)
	}
	var receipt bootExecReceipt
	if json.Unmarshal(data, &receipt) != nil {
		t.Fatal("invalid receiver receipt")
	}
	if receipt.PID != cmd.Process.Pid || receipt.Instance != bootExecInstance || receipt.Output != "env:;before;after" || receipt.ExitCode != 23 || !receipt.SecondRestoreNil || receipt.HandoffMemfds != 0 || receipt.ReplayMemfds < 2 || receipt.PidFDs != 1 {
		t.Fatalf("receiver receipt mismatch: %+v", receipt)
	}
	data, err = os.ReadFile(marker)
	if err != nil || json.Unmarshal(data, &m) != nil {
		t.Fatal("rollback marker missing")
	}
	wantCount, wantReason := count, "boot: budget/deadline exhausted"
	if count < upgradebudget.BootBudget {
		wantCount++
		wantReason = "bootstrap failed before control-ready: cobra"
	}
	// origin: hot-update S8 plan §1 / §11.2 P17; S8 review rv-fault F7. The
	// candidate records only recovery intent before its exec; rolled_back is
	// written by the restored image's own boot convergence (decideBoot's
	// prev-sha branch, covered in internal/binaryupgrade by
	// TestRollbackRecordsRolledBackOnlyForTheRunningPreviousImage). This
	// receiver is the test binary, which does not run that boot check, so the
	// marker must still be pending here.
	if m.State != "pending" || m.BootCount != wantCount || m.ResourcePhase != "redispatched" || !strings.HasPrefix(m.Detail, wantReason) {
		t.Fatalf("rollback marker: %+v", m)
	}
	if bootFileDigest(t, filepath.Join(dir, "tether")) != m.PrevSHA {
		t.Fatal("rollback did not install the previous binary")
	}
}

// origin: hot-update S3 plan §1.2 T1. Invalid Cobra flags still consume one
// boot and hand the exact held state back before any runtime restoration.
func TestAgentBootstrapRunsBootCheckBeforeRestore(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if os.Getenv(bootExecMode) == "dispatch" {
		dispatchBootExec(t)
		return
	}
	exerciseBootExec(t, 0)
}

// origin: hot-update S3 plan §1.6 T2. Exhausted budget re-dispatches before
// Cobra; the receiver imports the same task and every original file identity.
func TestBundlePresentAndBudgetExhaustedRedispatchesToPrev(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	if os.Getenv(bootExecMode) == "dispatch" {
		dispatchBootExec(t)
		return
	}
	exerciseBootExec(t, upgradebudget.BootBudget)
}

func dispatchBrokenBootExec(t *testing.T, mode string) {
	t.Helper()
	dir, name := os.Getenv(bootExecDir), os.Getenv(bootExecTest)
	r := os.NewFile(3, "parent-pipe")
	unix.CloseOnExec(3)
	fd, err := unix.MemfdCreate("boot-probe", unix.MFD_CLOEXEC)
	if err != nil {
		t.Fatal(err)
	}
	mem := os.NewFile(uintptr(fd), "boot-probe")
	prepared, err := handoff.Prepare(handoff.State{Role: agentimage.Role, Instance: "probe", Generation: 1}, []handoff.Resource{{ID: "probe", Kind: "pipe", Files: map[string]*os.File{"read": r, "mem": mem}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	_ = r.Close()
	_ = mem.Close()
	path, args, env := os.Args[0], []string{os.Args[0], "-test.run=^" + name + "$", "-test.count=1"}, bootExecEnvironment("corrupt-after", dir, name)
	if mode == "field-before" {
		env = bootExecEnvironment("corrupt-field-after", dir, name)
	}
	if mode == "version-before" {
		path = filepath.Join(dir, "tether")
		args = []string{path, "version"}
		env = bootExecEnvironment("", dir, name)
	}
	if err := prepared.Exec(path, args, env); err != nil {
		t.Fatal(err)
	}
}

func receiveBrokenBootExec() error {
	value := os.Getenv("_TETHER_HANDOFF")
	fd, _, ok := strings.Cut(value, ":")
	if !ok {
		return errors.New("missing corruption fixture")
	}
	if os.Getenv(bootExecMode) == "corrupt-field-after" {
		// Decoder errors may name untrusted fields. Keep those names out of
		// the bootstrap log even when they look like registry material.
		raw, err := os.ReadFile("/proc/self/fd/" + fd)
		if err != nil {
			return errors.New("cannot read corruption fixture")
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			return errors.New("invalid corruption fixture")
		}
		fields["secret"] = json.RawMessage(`"private"`)
		raw, _ = json.Marshal(fields)
		replacement, err := unix.MemfdCreate("tether-handoff", unix.MFD_CLOEXEC|unix.MFD_ALLOW_SEALING)
		if err != nil {
			return errors.New("cannot build corrupted manifest")
		}
		defer func() {
			if replacement >= 0 {
				_ = unix.Close(replacement)
			}
		}()
		if _, err := unix.Write(replacement, raw); err != nil {
			return errors.New("cannot write corrupted manifest")
		}
		if _, err := unix.FcntlInt(uintptr(replacement), unix.F_ADD_SEALS, unix.F_SEAL_SEAL|unix.F_SEAL_SHRINK|unix.F_SEAL_GROW|unix.F_SEAL_WRITE); err != nil {
			return errors.New("cannot seal corrupted manifest")
		}
		original, err := strconv.Atoi(fd)
		if err != nil || unix.Dup3(replacement, original, 0) != nil {
			return errors.New("cannot substitute corrupted manifest")
		}
		_ = unix.Close(replacement)
		replacement = -1
	} else {
		_ = os.Setenv("_TETHER_HANDOFF", fd+":"+strings.Repeat("0", 64))
	}
	img := Begin(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if img == nil || img.Phase() != PhaseLost || img.CrashWindow() != CrashWindowHandoffRestore {
		return errors.New("bad nonce did not enter W1")
	}
	census := bootFDCensus()
	if census[1] != 0 || census[3] != 0 {
		return errors.New("W1 left carried descriptors open")
	}
	entries, _ := os.ReadDir("/proc/self/fd")
	for _, entry := range entries {
		target, _ := os.Readlink("/proc/self/fd/" + entry.Name())
		if strings.Contains(target, "memfd:boot-probe") {
			return errors.New("W1 left probe memfd open")
		}
	}
	if r, err := handoff.Restore(); r != nil || err != nil {
		return errors.New("W1 dispatch was not consumed")
	}
	return nil
}

// origin: hot-update S3 plan §1.5 T13. W1 closes both the pipe and manifest
// after nonce validation failed, then continues; W1b still runs version.
func TestBeginCrashWindowClosesForeignDescriptorsAndContinues(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	mode := os.Getenv(bootExecMode)
	if mode == "corrupt-before" || mode == "field-before" || mode == "version-before" {
		dispatchBrokenBootExec(t, mode)
		return
	}
	for _, arm := range []string{"corrupt-before", "field-before", "version-before"} {
		t.Run(arm, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			dir := t.TempDir()
			if arm == "version-before" {
				bootCopy(t, buildBootTether(t), filepath.Join(dir, "tether"))
			}
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = w.Close() }()
			cmd := bootHelperCommand(t, arm, dir)
			// The helper test name excludes the parent's subtest component.
			name := "TestBeginCrashWindowClosesForeignDescriptorsAndContinues"
			cmd.Args = []string{os.Args[0], "-test.run=^" + name + "$", "-test.count=1"}
			cmd.Env = bootExecEnvironment(arm, dir, name)
			cmd.ExtraFiles = []*os.File{r}
			// Start before closing the parent's read copy; CombinedOutput hides
			// that ownership seam and would keep the pipe alive until after Wait.
			if arm == "version-before" {
				output := bootVersionBeforeExit(t, cmd, r, w)
				t.Log(output)
				if strings.Contains(output, "secret") {
					t.Fatal("helper leaked registry material")
				}
				if !strings.Contains(output, "crash_window=non_daemon_invocation") || !strings.Contains(output, proto.ImageABILine()) {
					t.Fatalf("W1b failed to abandon and continue version: %s", output)
				}
				return
			}
			var output strings.Builder
			cmd.Stdout = &output
			cmd.Stderr = &output
			if err := cmd.Start(); err != nil {
				_ = r.Close()
				t.Fatal(err)
			}
			_ = r.Close()
			if err := cmd.Wait(); err != nil {
				t.Fatalf("crash window helper: %v\n%s", err, output.String())
			}
			if strings.Contains(output.String(), "secret") {
				t.Fatal("helper leaked registry material")
			}
			if _, err := w.Write([]byte("x")); !errors.Is(err, syscall.EPIPE) {
				t.Fatalf("carried read end survived: %v", err)
			}
			if arm == "version-before" {
				if !strings.Contains(output.String(), "crash_window=non_daemon_invocation") || !strings.Contains(output.String(), proto.ImageABILine()) {
					t.Fatalf("W1b failed to abandon and continue version: %s", output.String())
				}
			} else if !strings.Contains(output.String(), "crash_window=handoff_restore") {
				t.Fatal("W1 signature missing")
			}
		})
	}
}

// Hold the real version command inside its stdout write. The abandonment log
// on stderr is a barrier after Begin/Abandon; the full stdout pipe prevents
// process exit from satisfying our EPIPE oracle on the unrelated carried pipe.
// origin: hot-update S3 plan §1.5 T13 (RS-2 / TD-2).
func bootVersionBeforeExit(t *testing.T, cmd *exec.Cmd, carriedRead, carriedWrite *os.File) string {
	t.Helper()
	outR, outW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = outR.Close() }()
	defer func() { _ = outW.Close() }()
	logR, logW, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logR.Close() }()
	defer func() { _ = logW.Close() }()
	capacity, err := unix.FcntlInt(outW.Fd(), unix.F_GETPIPE_SZ, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := outW.Write(make([]byte, capacity)); err != nil {
		t.Fatal(err)
	}
	cmd.Stdout, cmd.Stderr = outW, logW
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	_ = outW.Close()
	_ = logW.Close()
	_ = carriedRead.Close()
	logs := bufio.NewReader(logR)
	var output strings.Builder
	for {
		line, err := logs.ReadString('\n')
		output.WriteString(line)
		if strings.Contains(line, "crash_window=non_daemon_invocation") {
			break
		}
		if err != nil {
			t.Errorf("abandonment barrier missing: %v", err)
			break
		}
	}
	if err := cmd.Process.Signal(syscall.Signal(0)); err != nil {
		t.Errorf("version exited before descriptor probe: %v", err)
	}
	if _, err := carriedWrite.Write([]byte("x")); !errors.Is(err, syscall.EPIPE) {
		t.Errorf("live version retained carried read end: %v", err)
	}
	// Release version only after the assertion, then preserve the original
	// real-binary output, crash-window and exit-status checks.
	if _, err := io.CopyN(io.Discard, outR, int64(capacity)); err != nil {
		t.Error(err)
	}
	_, _ = io.Copy(&output, outR)
	_, _ = io.Copy(&output, logs)
	if err := cmd.Wait(); err != nil {
		t.Errorf("crash window helper: %v\n%s", err, output.String())
	}
	return output.String()
}
