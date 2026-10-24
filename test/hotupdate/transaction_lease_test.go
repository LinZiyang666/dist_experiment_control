//go:build linux

package hotupdate_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
	"golang.org/x/sys/unix"
)

const transactionLeaseMode = "_TETHER_TRANSACTION_LEASE_MODE"
const transactionLeaseRoot = "_TETHER_TRANSACTION_LEASE_ROOT"

type transactionLeaseSnapshot struct {
	State         upgradecut.State
	PID           int
	Ticks         uint64
	Device, Inode uint64
	Record        upgradetx.Record
}

type transactionLeaseReceipt struct {
	Before, After                      transactionLeaseSnapshot
	Takes                              int
	AuxiliaryEmpty, SecondRestoreEmpty bool
	GuardDevice, GuardInode            uint64
}

// The helper execs itself once. Its journal write happens while the adopted
// long lease remains held, and the parent probes contention on both sides of
// exec. Neither process exit nor a same-process borrowed lock can sign this.
// origin: hot-update S8 plan §5.1 / §9.1 / P23 / M08
func TestExecPreservesDomainLeaseThroughCommit(t *testing.T) {
	switch mode := os.Getenv(transactionLeaseMode); mode {
	case "before":
		transactionLeaseBefore(t, os.Getenv(transactionLeaseRoot))
		return
	case "after":
		transactionLeaseAfter(t, os.Getenv(transactionLeaseRoot))
		return
	case "":
	default:
		t.Fatal("unknown lease helper mode")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionLeaseExec(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transaction lease exec x5", g)
	testharness.AssertNoFDLeak(t, "transaction lease exec x5", fd, 0)
}

func exerciseTransactionLeaseExec(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	canonical := filepath.Join(root, "canonical")
	if err := os.WriteFile(canonical, []byte("registered canonical entry"), 0555); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "control"), 0700); err != nil {
		t.Fatal(err)
	}
	key := transactionLeaseRegister(t, root)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestExecPreservesDomainLeaseThroughCommit$", "-test.count=1")
	cmd.Env = transactionLeaseEnvironment("before", root)
	output := &hotUpdateOutput{}
	cmd.Stdout, cmd.Stderr = output, output
	join, err := testsupervisor.StartProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = join(); close(done) }()
	defer func() {
		cancel()
		<-done
		if waitErr != nil {
			t.Errorf("lease helper failed: %v\n%s", waitErr, output.Bytes())
		}
	}()
	transactionLeaseWait(ctx, t, filepath.Join(root, "before.json"))
	transactionLeaseContender(t, key)
	transactionLeaseWrite(t, filepath.Join(root, "exec"), struct{}{})
	transactionLeaseWait(ctx, t, filepath.Join(root, "committed.json"))
	transactionLeaseContender(t, key)
	var receipt transactionLeaseReceipt
	transactionLeaseRead(t, filepath.Join(root, "committed.json"), &receipt)
	if receipt.Before.PID != cmd.Process.Pid || receipt.After.PID != receipt.Before.PID || receipt.After.Ticks != receipt.Before.Ticks || receipt.Before.Ticks == 0 {
		t.Fatal("lease handoff did not preserve the original PID/start ticks")
	}
	if receipt.Before.Device != receipt.After.Device || receipt.Before.Inode != receipt.After.Inode || receipt.Before.Inode == 0 || receipt.Takes != 1 || !receipt.AuxiliaryEmpty || !receipt.SecondRestoreEmpty {
		t.Fatal("lease was reopened, taken twice, or left in auxiliary custody")
	}
	if receipt.GuardDevice == receipt.After.Device && receipt.GuardInode == receipt.After.Inode {
		t.Fatal("journal guard and execution lease share one inode")
	}
	if receipt.After.Record.Revision != receipt.Before.Record.Revision+1 || receipt.After.Record.Forward.Phase != upgradetx.PhaseStaged || receipt.After.Record.Grant != receipt.Before.Record.Grant {
		t.Fatal("post-exec commit failed to persist under the original lease")
	}
	transactionLeaseWrite(t, filepath.Join(root, "release"), struct{}{})
	// Read the same result twice without consuming the cleanup's join channel.
	transactionLeaseWait(ctx, t, filepath.Join(root, "closed.json"))
	var closed struct{ LeaseRefs, HandoffRefs int }
	transactionLeaseRead(t, filepath.Join(root, "closed.json"), &closed)
	if closed.LeaseRefs != 0 || closed.HandoffRefs != 0 {
		t.Fatal("released owner retained lease or handoff custody")
	}
	lease, err := upgradelock.Acquire(ctx, key, "")
	if err != nil {
		t.Fatalf("final owner did not release its lease: %v", err)
	}
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(output.Bytes()), "secret") {
		t.Fatal("private state escaped through helper output")
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("lease helper did not join after final release")
	}
}

func transactionLeaseRegister(t *testing.T, root string) upgradetx.Key {
	t.Helper()
	key := upgradetx.Key{Component: upgradetx.ComponentBroker, ConfiguredNID: "lease-broker", ConfigPath: filepath.Join(root, "broker.yaml"), UID: uint32(os.Getuid())}
	domain, err := upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: filepath.Join(root, "canonical"), ControlDir: filepath.Join(root, "control"), DeploymentID: "lease-exec", Participants: []upgradelock.Participant{{Key: key, ServiceID: "lease-broker"}}})
	if err != nil {
		t.Fatal(err)
	}
	key.DomainID = domain
	return key
}

func transactionLeaseOwner(t *testing.T) upgradetx.Owner {
	t.Helper()
	body, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		t.Fatal(err)
	}
	_, tail, ok := strings.Cut(string(body), ") ")
	fields := strings.Fields(tail)
	if !ok || len(fields) < 20 {
		t.Fatal("process start identity unavailable")
	}
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	return upgradetx.Owner{BootID: strings.TrimSpace(string(boot)), PID: os.Getpid(), StartTicks: ticks, InstanceID: strings.Repeat("a", 32)}
}

func transactionLeaseStore(t *testing.T, root string, key upgradetx.Key, owner upgradetx.Owner) *upgradestore.Store {
	t.Helper()
	s, err := upgradestore.Open(upgradestore.Options{Path: filepath.Join(root, "upgrade-state.json"), GuardPath: filepath.Join(root, "control", key.DomainID+".guard"), Key: key, Owner: owner, Transition: upgradetx.Transition})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func transactionLeaseBefore(t *testing.T, root string) {
	key, owner := transactionLeaseRegister(t, root), transactionLeaseOwner(t)
	parentToken := strings.Repeat("b", 32)
	lease, err := upgradelock.Acquire(t.Context(), key, parentToken)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := lease.Close(); err != nil {
			t.Error(err)
		}
	}()
	s := transactionLeaseStore(t, root, key, owner)
	scope := upgradetx.Scope{ID: strings.Repeat("b", 32), Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}, Expected: []upgradetx.Resource{}}
	scope.Expected = append(scope.Expected, upgradetx.Resource{Domain: upgradetx.DomainUpgrade, ID: "domain-lease", Origin: "before-exec", Owner: owner, Generation: 1, State: "held", Custody: "source"})
	grant, err := s.Prepare(key, upgradetx.PrepareRequest{ClientNonce: strings.Repeat("c", 32), ActorFP: "lease-actor", Mode: upgradetx.ModeHot, SourceRelease: "A", TargetRelease: "B", SourceSHA: strings.Repeat("d", 64), TargetSHA: strings.Repeat("e", 64), CapabilityDigest: strings.Repeat("f", 64), Scope: scope})
	if err != nil {
		t.Fatal(err)
	}
	record, err := s.Start(grant, upgradetx.Fence{Owner: owner, RunningSHA: grant.SourceSHA})
	if err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(lease.FD()), &stat); err != nil {
		t.Fatal(err)
	}
	snapshot := transactionLeaseSnapshot{State: upgradecut.State{Attempt: record.Forward.Handle, SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, ParentToken: parentToken, Owner: owner, LockFD: 0, Scope: scope, Authorities: []upgradecut.Authority{}}, PID: owner.PID, Ticks: owner.StartTicks, Device: uint64(stat.Dev), Inode: stat.Ino, Record: record}
	adjunct, err := upgradecut.Encode(snapshot.State)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	// Borrow a duplicate instead of adding an os.File finalizer to the lease's
	// own FD. Handoff makes its independent CLOEXEC duplicate of this same OFD.
	dup, err := unix.FcntlInt(lease.FD(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	file := os.NewFile(uintptr(dup), "lease-borrow")
	defer func() { _ = file.Close() }()
	prepared, err := handoff.Prepare(handoff.State{Role: "broker", Instance: owner.InstanceID, Generation: grant.Handle.Generation, Data: data}, []handoff.Resource{{ID: "upgrade-lease", Kind: "upgrade-cut-v1", Data: adjunct, Files: map[string]*os.File{"lease": file}}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = prepared.Close() }()
	transactionLeaseWrite(t, filepath.Join(root, "before.json"), snapshot)
	transactionLeaseWait(t.Context(), t, filepath.Join(root, "exec"))
	if err := prepared.Exec(os.Args[0], []string{os.Args[0], "-test.run=^TestExecPreservesDomainLeaseThroughCommit$", "-test.count=1"}, transactionLeaseEnvironment("after", root)); err != nil {
		t.Fatal(err)
	}
	t.Fatal("same-PID exec unexpectedly returned")
}

func transactionLeaseAfter(t *testing.T, root string) {
	key, owner := transactionLeaseRegister(t, root), transactionLeaseOwner(t)
	restored, err := handoff.Restore()
	if err != nil || restored == nil {
		t.Fatalf("restore lease: %v", err)
	}
	defer func() { _ = restored.Close() }()
	var before transactionLeaseSnapshot
	if err := json.Unmarshal(restored.State.Data, &before); err != nil {
		t.Fatal(err)
	}
	if len(restored.Resources) != 1 {
		t.Fatal("wrong auxiliary resource set")
	}
	state, err := upgradecut.Decode(restored.Resources[0].Data)
	if err != nil {
		t.Fatal(err)
	}
	takes := 0
	lease, err := upgradecut.Take(state, func(index uint32) (uintptr, error) {
		takes++
		if index != 0 {
			return ^uintptr(0), errors.New("wrong lease index")
		}
		file, err := restored.TakeFile("upgrade-lease", "lease")
		if err != nil {
			return ^uintptr(0), err
		}
		// Transfer raw descriptor custody without keeping a second os.File
		// wrapper/finalizer. The duplicate still owns the original kernel OFD.
		dup, dupErr := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
		closeErr := file.Close()
		if dupErr != nil {
			return ^uintptr(0), dupErr
		}
		return uintptr(dup), closeErr
	})
	if lease != nil {
		defer func() { _ = lease.Close() }()
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Close(); err != nil {
		t.Fatal(err)
	}
	if before.State.Attempt.Key != key || before.PID != owner.PID || before.Ticks != owner.StartTicks {
		t.Fatal("exec changed the reservation identity")
	}
	if err := upgradelock.WithGuard(key, func() error { return os.WriteFile(filepath.Join(root, "guard-commit"), []byte("post-exec"), 0600) }); err != nil {
		t.Fatal(err)
	}
	borrowed, err := upgradelock.Acquire(t.Context(), key, state.ParentToken)
	if err != nil {
		t.Fatal(err)
	}
	if err := borrowed.Close(); err != nil {
		t.Fatal(err)
	}
	s := transactionLeaseStore(t, root, key, owner)
	record, err := s.Apply(before.Record.Forward.Handle, upgradetx.Event{Phase: upgradetx.PhaseStaged})
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.Load(key, record.Grant.Handle.TxID)
	if err != nil || view.Historical.Revision != record.Revision {
		t.Fatalf("durable commit readback: %v", err)
	}
	var stat, guard unix.Stat_t
	if err := unix.Fstat(int(lease.FD()), &stat); err != nil {
		t.Fatal(err)
	}
	if err := unix.Stat(filepath.Join(root, "control", key.DomainID+".guard"), &guard); err != nil {
		t.Fatal(err)
	}
	second, err := handoff.Restore()
	if err != nil {
		t.Fatal(err)
	}
	after := before
	after.PID, after.Ticks, after.Device, after.Inode, after.Record = owner.PID, owner.StartTicks, uint64(stat.Dev), stat.Ino, view.Historical
	receipt := transactionLeaseReceipt{Before: before, After: after, Takes: takes, AuxiliaryEmpty: len(restored.Borrow()) == 0, SecondRestoreEmpty: second == nil, GuardDevice: uint64(guard.Dev), GuardInode: guard.Ino}
	transactionLeaseWrite(t, filepath.Join(root, "committed.json"), receipt)
	transactionLeaseWait(t.Context(), t, filepath.Join(root, "release"))
	if err := lease.Close(); err != nil {
		t.Fatal(err)
	}
	// Observe release while this receiver still lives: exit's kernel cleanup
	// must not hide a leaked inherited or borrowed descriptor.
	// origin: hot-update S8 plan §4.2 / §9.1 / P23
	leaseRefs, handoffRefs := 0, 0
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil || fd < 3 {
			continue
		}
		var actual unix.Stat_t
		if unix.Fstat(fd, &actual) == nil && actual.Dev == stat.Dev && actual.Ino == stat.Ino {
			leaseRefs++
		}
		name, _ := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if strings.HasPrefix(name, "/memfd:tether-handoff") {
			handoffRefs++
		}
	}
	if leaseRefs != 0 || handoffRefs != 0 {
		t.Fatal("live receiver retained released lease/handoff descriptors")
	}
	transactionLeaseWrite(t, filepath.Join(root, "closed.json"), struct{ LeaseRefs, HandoffRefs int }{leaseRefs, handoffRefs})
}

func transactionLeaseContender(t *testing.T, key upgradetx.Key) {
	t.Helper()
	lease, err := upgradelock.Acquire(t.Context(), key, "")
	if lease != nil {
		_ = lease.Close()
		t.Fatal("independent contender acquired the inherited lease")
	}
	var txerr *upgradetx.Error
	if !errors.As(err, &txerr) || txerr.Code != upgradetx.CodeDeferred {
		t.Fatalf("wrong lease refusal: %v", err)
	}
}

func transactionLeaseEnvironment(mode, root string) []string {
	var env []string
	for _, entry := range handoff.CleanEnvironment(os.Environ()) {
		name, _, _ := strings.Cut(entry, "=")
		if name != transactionLeaseMode && name != transactionLeaseRoot {
			env = append(env, entry)
		}
	}
	return append(env, transactionLeaseMode+"="+mode, transactionLeaseRoot+"="+root)
}

func transactionLeaseWrite(t *testing.T, path string, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".tmp", data, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
}

func transactionLeaseRead(t *testing.T, path string, value any) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, value); err != nil {
		t.Fatal(err)
	}
}

func transactionLeaseWait(ctx context.Context, t *testing.T, path string) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		if _, err := os.Stat(path); err == nil {
			return
		} else if !os.IsNotExist(err) {
			t.Fatal(err)
		}
		select {
		case <-ctx.Done():
			t.Fatal(fmt.Errorf("lease barrier %s: %w", filepath.Base(path), ctx.Err()))
		case <-ticker.C:
		}
	}
}
