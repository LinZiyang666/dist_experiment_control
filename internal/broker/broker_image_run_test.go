package broker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

// origin: hot-update S6 plan §12.1 P11 / T01
func TestBrokerRunLeakCycles(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("image reload requires Linux")
	}
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerExecReturned(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker Run rejected image cut", goroutines)
	testharness.AssertNoFDLeak(t, "broker Run rejected image cut", fds, 0)
}

// A missing owner refuses inside a real, live Broker.Run without unwinding it.
// The separate T01 fixture owns real successful exec; this exercise proves the
// refusal leaves the current DB/NC and reconciliation loop usable until Stop.
func exerciseBrokerExecReturned(t *testing.T) {
	t.Helper()
	db := openDB(t)
	ready := make(chan struct{})
	b, err := New(Config{NATSURL: startNATS(t), DB: db, Logger: silentLogger(), ReadyCh: ready})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: isolated broker Run lifetime
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Run failed to join")
		}
	}()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("Run startup: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not become ready")
	}
	b.SetReloadTrigger(cancel)
	b.RequestReExec()
	// Arming is cleared only after Run consumed the coalesced request; inspect
	// the actual current NC/DB before allowing the terminal cancellation.
	until := time.Now().Add(5 * time.Second)
	for b.ReExecRequested() && time.Now().Before(until) {
		runtime.Gosched()
	}
	if b.ReExecRequested() {
		t.Fatal("Run did not consume reload")
	}
	select {
	case err := <-done:
		t.Fatalf("reload unwound Run: %v", err)
	default:
	}
	if b.nc.Load() == nil || b.nc.Load().IsClosed() {
		t.Fatal("reload closed the current NATS connection")
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("reload closed current DB: %v", err)
	}
	status := brokerImageSnapshot(b)
	if status == nil || status.RunningSHA256 == "" || status.Phase != proto.BrokerImagePhaseControlReady {
		t.Fatalf("missing running image status: %+v", status)
	}
}

// origin: hot-update S6 plan §5 / T45
func TestBrokerTransferFenceIncludesProvisionAndLedger(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("CLOCK_BOOTTIME required")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerTransferFence(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker transfer fence", g)
	testharness.AssertNoFDLeak(t, "broker transfer fence", fd, 0)
}

func exerciseBrokerTransferFence(t *testing.T) {
	t.Helper()
	b := &Broker{cfg: Config{DBPath: filepath.Join(t.TempDir(), "state.db")}, transfers: newTransferTracker()}
	if !beginBrokerTransfer(b) {
		t.Fatal("cold prepare refused")
	}
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	// An admitted provisioning handler remains active for the whole short join
	// budget. Returning success here is observably different from timing out.
	if _, err := FenceBrokerTransfers(b, controlbudget.Deadline(now+int64(20*time.Millisecond))); err == nil {
		endBrokerTransfer(b)
		ResumeBrokerTransfers(b)
		t.Fatal("fence did not join provisioning")
	}
	defer ResumeBrokerTransfers(b)
	if beginBrokerTransfer(b) {
		endBrokerTransfer(b)
		endBrokerTransfer(b)
		t.Fatal("new prepare escaped the fence")
	}
	endBrokerTransfer(b)
	deadline, err := brokerCutDeadline()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := FenceBrokerTransfers(b, deadline); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(b.xferInflightDir(), 0700); err != nil {
		t.Fatal(err)
	}
	row := xferInflightRecord{TransferID: "durable", Session: "sid", Node: "nid", Verb: "push", Tier: "B", StartedAt: time.Now()}
	data, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(b.xferInflightDir(), "row.json")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	count, err := FenceBrokerTransfers(b, deadline)
	if err != nil || count != 1 {
		t.Fatalf("durable transfer omitted: %d, %v", count, err)
	}
	// The live-only entry has not acquired a disk row. Its copied census must
	// still prevent a zero-transfer cut, without exposing mutable tracker state.
	// origin: hot-update S7 plan §8.2 / T39
	entry := xfertrack.NewLegacyEntry(xfertrack.LegacyRecord{TransferID: "memory", SID: "sid", Tier: "a"})
	if code := putLegacyTransfer(b.transfers, entry); code != "" {
		t.Fatal(code)
	}
	count, err = FenceBrokerTransfers(b, deadline)
	if err != nil || count != 2 {
		t.Fatalf("live entry omitted from cut census: %d %v", count, err)
	}
	ids := b.transfers.TransferIDs()
	if len(ids) != 1 {
		t.Fatalf("live census=%v", ids)
	}
	ids[0] = "foreign"
	if b.transfers.TransferIDs()[0] != "memory" {
		t.Fatal("cut caller mutated retained identity")
	}
	b.transfers.Remove(entry)
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := FenceBrokerTransfers(b, deadline); err == nil {
		t.Fatal("corrupt ledger read became empty census")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(b.xferInflightDir()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(b.xferInflightDir(), []byte("not a directory"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := FenceBrokerTransfers(b, deadline); err == nil {
		t.Fatal("ledger census error was ignored")
	}
	ResumeBrokerTransfers(b)
	if !beginBrokerTransfer(b) {
		t.Fatal("refused cut retained creation fence")
	}
	endBrokerTransfer(b)
}

// origin: hot-update S6 plan §3.2 / T01
func TestBrokerLoopJoinRequiresActualExit(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("CLOCK_BOOTTIME required")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerLoopJoin(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker loop join", g)
	testharness.AssertNoFDLeak(t, "broker loop join", fd, 0)
}

func exerciseBrokerLoopJoin(t *testing.T) {
	t.Helper()
	ls := newLoopSet()
	ctx := context.Background() // ctx-root: test owns release of this loop
	release := make(chan struct{})
	ls.Go(ctx, "store-user", func(context.Context) { <-release })
	now, err := rawsocket.BootNow()
	if err != nil {
		close(release)
		t.Fatal(err)
	}
	if err := joinBrokerLoops(ls, controlbudget.Deadline(now+int64(20*time.Millisecond))); err == nil {
		close(release)
		t.Fatal("join pretended the loop exited")
	}
	close(release)
	deadline, err := brokerCutDeadline()
	if err != nil {
		t.Fatal(err)
	}
	if err := joinBrokerLoops(ls, deadline); err != nil {
		t.Fatal(err)
	}
	if err := joinBrokerLoops(ls, deadline); err != nil {
		t.Fatalf("completion receipt was consumed: %v", err)
	}
	if err := joinBrokerLoops(ls, 0); err == nil {
		t.Fatal("zero absolute deadline accepted")
	}
}

// origin: hot-update S6 plan §5 / §9.1
func TestBrokerImageStatusReportsRunningArtifact(t *testing.T) {
	b := &Broker{}
	setBrokerImagePhase(b, proto.BrokerImagePhaseControlReady)
	got := brokerImageSnapshot(b)
	running, err := runningBinarySHA256()
	if err != nil {
		t.Skip(err)
	}
	if got == nil || got.RunningSHA256 != running || got.Generation != 1 || got.Mode != proto.BrokerImageModeSingle {
		t.Fatalf("running image status: %+v", got)
	}
	imported := &Broker{}
	if err := InitializeBrokerImageStatus(imported, 0, "cut"); err == nil {
		t.Fatal("zero generation accepted")
	}
	if err := InitializeBrokerImageStatus(imported, 7, "cut"); err != nil {
		t.Fatal(err)
	}
	setBrokerImagePhase(imported, proto.BrokerImagePhaseControlReady)
	if image := brokerImageSnapshot(imported); image.Generation != 7 || image.CutID != "cut" || image.RunningSHA256 != running {
		t.Fatalf("imported lineage reset: %+v", image)
	}
	if err := InitializeBrokerImageStatus(imported, 8, "other"); err == nil {
		t.Fatal("boot lineage replaced after initialization")
	}
	got.Phase = "changed"
	if brokerImageSnapshot(b).Phase != proto.BrokerImagePhaseControlReady {
		t.Fatal("status exposes mutable owner state")
	}
	if err := validateBrokerImageRun(nil); err == nil {
		t.Fatal("missing image owners accepted")
	}
	if err := runBrokerImageReload(&Broker{}, brokerimage.StoreShutdown{Close: func() error { t.Fatal("unwired shutdown called"); return nil }}); err == nil {
		t.Fatal("unwired image cut returned success")
	}
}

// origin: hot-update S6 plan §3.1 / §12.1 P11
func TestBrokerSingleStoreRequiresProducerAdapters(t *testing.T) {
	if err := brokerimage.InstallSingleStoreSupport(brokerimage.SingleStoreSupport{}); err == nil {
		t.Fatal("incomplete store support was installed")
	}
	owner, db, err := brokerimage.OpenSingleStore(brokerimage.SingleStoreConfig{DBPath: filepath.Join(t.TempDir(), "state.db")})
	if err == nil || owner != nil || db != nil {
		t.Fatal("missing producer adapters opened a single store")
	}
}

// origin: hot-update S6 plan §5 / T01
func TestBrokerRunningSHAIgnoresStagedReplacement(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("running inode receipt requires /proc")
	}
	const mode = "S6_P11_HASH_HELPER"
	if os.Getenv(mode) == "1" {
		// Leave the process witness cold until after staging. A pathname-based
		// cache warmed before the rename would accidentally satisfy this oracle.
		// origin: hot-update S6 plan §3.1 / T01 / T37
		before, err := brokerimage.StagedArtifactSHA256()
		if err != nil {
			t.Fatal(err)
		}
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		// Replace only this sandbox copy's pathname. The kernel retains the running
		// executable inode; a staged-path hash now describes a different artifact.
		staged := []byte("same release, different staged artifact\n")
		next := exe + ".staged"
		if err := os.WriteFile(next, staged, 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(next, exe); err != nil {
			t.Fatal(err)
		}
		running, err := runningBinarySHA256()
		if err != nil {
			t.Fatal(err)
		}
		disk, err := onDiskBinarySHA256()
		if err != nil {
			t.Fatal(err)
		}
		if running != before || disk == before {
			t.Fatal("running SHA followed the staged pathname")
		}
		if brokerimage.ReloadAlreadyRunning(proto.ReleaseVersion, disk, running) {
			t.Fatal("same release skipped staged replacement")
		}
		fmt.Println("BROKER_RUNNING_INODE_VERIFIED")
		return
	}
	sandbox := t.TempDir()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	input, err := os.Open(exe)
	if err != nil {
		t.Fatal(err)
	}
	copyPath := filepath.Join(sandbox, "broker-hash-helper")
	output, err := os.OpenFile(copyPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
	if err != nil {
		_ = input.Close()
		t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	closeErr := errors.Join(output.Close(), input.Close())
	if err := errors.Join(copyErr, closeErr); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // ctx-root: sandbox helper lifetime
	defer cancel()
	cmd := exec.CommandContext(ctx, copyPath, "-test.run=^TestBrokerRunningSHAIgnoresStagedReplacement$", "-test.count=1")
	env := handoff.CleanEnvironment(os.Environ())
	clean := make([]string, 0, len(env)+2)
	for _, row := range env {
		if !strings.HasPrefix(row, mode+"=") && !strings.HasPrefix(row, "HOME=") {
			clean = append(clean, row)
		}
	}
	cmd.Env = append(clean, mode+"=1", "HOME="+sandbox)
	result, err := cmd.CombinedOutput() // CommandContext always Waits/reaps its child.
	if err != nil {
		t.Fatalf("running-inode helper: %v\n%s", err, result)
	}
	if !bytes.Contains(result, []byte("BROKER_RUNNING_INODE_VERIFIED")) {
		t.Fatal("missing running inode receipt")
	}
}

// origin: hot-update S6 plan §4.2 / T02
func TestBrokerCurrentOwnerSkipsColdCleanup(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerColdCleanup(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker current owner cleanup", g)
	testharness.AssertNoFDLeak(t, "broker current owner cleanup", fd, 0)
}

func exerciseBrokerColdCleanup(t *testing.T) {
	t.Helper()
	oldDB, currentDB := openDB(t), openDB(t)
	managed := true
	actions := 0
	brokerimage.ColdCleanupAction(&managed, func() { actions++ })
	if actions != 0 {
		t.Fatal("managed runtime ran a captured cold action")
	}
	brokerimage.ColdCleanup(&managed, oldDB.Close)
	if err := oldDB.Ping(); err != nil {
		t.Fatal("managed runtime ran a captured cold DB defer")
	}
	if err := currentDB.Ping(); err != nil {
		t.Fatal("current DB was closed by cold cleanup")
	}
	managed = false
	brokerimage.ColdCleanupAction(&managed, func() { actions++ })
	if actions != 1 {
		t.Fatal("startup error omitted cold action cleanup")
	}
	brokerimage.ColdCleanup(&managed, oldDB.Close)
	if err := oldDB.Ping(); err == nil {
		t.Fatal("startup error omitted cold DB cleanup")
	}
	if err := currentDB.Ping(); err != nil {
		t.Fatal("cleanup closed a different generation")
	}
	if err := currentDB.Close(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 plan §3.2 / §5 / T01
func TestBrokerReloadKeepsAbsoluteBudget(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("CLOCK_BOOTTIME required")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerReloadBudget(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker reload signal", g)
	testharness.AssertNoFDLeak(t, "broker reload signal", fd, 0)
}
func exerciseBrokerReloadBudget(t *testing.T) {
	t.Helper()
	var signal brokerimage.StoreReloadSignal
	if err := signal.Arm(0); err == nil || signal.Requested() {
		t.Fatal("invalid budget armed a cut")
	}
	now, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	deadline := controlbudget.Deadline(now + int64(time.Second))
	done := make(chan error, 1)
	go func() { done <- signal.Arm(deadline) }()
	select {
	case <-signal.Requests():
	case <-time.After(5 * time.Second):
		t.Fatal("reload was not queued")
	}
	actual, err := signal.Deadline()
	if err != nil || actual != deadline {
		signal.Notify(err)
		<-done
		t.Fatalf("Run reset admission budget: %d != %d (%v)", actual, deadline, err)
	}
	if !signal.Requested() {
		signal.Notify(nil)
		<-done
		t.Fatal("queued cut is not pending")
	}
	signal.Notify(nil)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	signal.Clear()
	if signal.Requested() {
		t.Fatal("returned cut remained pending")
	}
}

// origin: hot-update S6 plan §4.1 / §6.3 / T28
func TestBrokerRestoredStoreSkipsColdGeneration(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerRestoredRefusal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker restored startup refusal", g)
	testharness.AssertNoFDLeak(t, "broker restored startup refusal", fd, 0)
}
func exerciseBrokerRestoredRefusal(t *testing.T) {
	t.Helper()
	db := openDB(t)
	var before, after int64
	if err := db.QueryRow(`SELECT generation FROM proxy_meta WHERE id=1`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	b, err := New(Config{DB: db, NATSURL: "nats://127.0.0.1:1", Logger: silentLogger(), ImageRun: &BrokerImageRun{Restored: true}})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT generation FROM proxy_meta WHERE id=1`).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("restored constructor ran cold generation write: %d -> %d", before, after)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // ctx-root: restored-owner refusal test
	defer cancel()
	err = b.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "image owners are not installed") {
		t.Fatalf("uninstalled restored graph entered cold startup: %v", err)
	}
	if err := db.PingContext(ctx); err != nil {
		t.Fatal("refused restored graph closed the caller's DB")
	}
}

// origin: hot-update S6 internal review rv-fd-6 / plan §1.2 / §4.2
func TestBrokerStopDuringCutLeavesFDsWithCurrentOwner(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("image keepers require Linux")
	}
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerStopDuringCut(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker stop during image cut", goroutines)
	testharness.AssertNoFDLeak(t, "broker stop during image cut", fds, 0)
}

func exerciseBrokerStopDuringCut(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	if err := brokerimage.InstallColdSingleStoreSupport(); err != nil {
		t.Fatal(err)
	}
	store, db, err := brokerimage.OpenSingleStore(brokerimage.SingleStoreConfig{DBPath: filepath.Join(dir, "state.db"), Registry: storage.DefaultDriverRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	image, err := NewImageRun([]string{"broker-cut-fixture", "serve"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	image.Preflight = func(b *Broker, _ controlbudget.Deadline) error {
		close(entered)
		<-release
		return b.runCtx.Err()
	}
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	controlAddr := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	b, err := New(Config{NATSURL: startNATS(t), DB: db, DBPath: filepath.Join(dir, "state.db"), SingleStore: store,
		ImageRun: image, ReadyCh: ready, Logger: silentLogger(), TunnelControlAddr: controlAddr, MetricsAddr: "127.0.0.1:0", AdminSocketPath: filepath.Join(dir, "admin.sock")})
	if err != nil {
		_ = brokerimage.CloseStoreOwner(store)
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: isolated Run stop/cut interleaving.
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	released := false
	defer func() {
		cancel()
		if !released {
			close(release)
		}
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("Run did not join after stop")
		}
	}()
	select {
	case <-ready:
	case err := <-done:
		done <- err
		t.Fatalf("startup: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not become ready")
	}
	b.RequestReExec()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not enter its cut")
	}
	cancel()
	// Cancellation must win the final exec latch while Run still owns the cut.
	if err := image.WithExecLatch(func() error { t.Error("stop request reached exec"); return nil }); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost stop request: %v", err)
	}
	for _, keeper := range b.imageControl.keepers {
		desc := keeper.Descriptor()
		conn, err := net.DialTimeout(desc.Network, desc.Address, time.Second)
		if err != nil {
			t.Fatalf("Run cancellation closed exported keeper %s: %v", desc.ID, err)
		}
		_ = conn.Close()
	}
	if conn, err := net.DialTimeout("tcp", controlAddr, time.Second); err != nil {
		t.Fatalf("Run cancellation closed data-plane listener: %v", err)
	} else {
		_ = conn.Close()
	}
	if _, err := os.Stat(filepath.Join(dir, "admin.sock")); err != nil {
		t.Fatalf("Run cancellation unlinked admin pathname: %v", err)
	}
	close(release)
	released = true
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("terminal Run: %v", err)
		}
		done <- err // deferred cleanup consumes the one actual Run receipt
	case <-time.After(10 * time.Second):
		t.Fatal("Run ignored cancellation after cut returned")
	}
	if _, err := os.Stat(filepath.Join(dir, "admin.sock")); !os.IsNotExist(err) {
		t.Fatalf("terminal current owner left admin pathname: %v", err)
	}
	if _, err := db.Exec("SELECT 1"); err == nil {
		t.Fatal("terminal current owner retained SQLite")
	}
	lock, err := cluster.AcquireImageDataDirLock(dir)
	if err != nil {
		t.Fatalf("terminal current owner retained flock: %v", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S6 internal review rv-ctl-3 / plan §8.1
func TestBrokerImageRecordsCarryCutAndProducerClock(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("CLOCK_BOOTTIME required")
	}
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerImageRecords(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker phase records", goroutines)
	testharness.AssertNoFDLeak(t, "broker phase records", fds, 0)
}

func exerciseBrokerImageRecords(t *testing.T) {
	t.Helper()
	b, nc, _ := brokerImageAuthFixture(t, true, true)
	var records bytes.Buffer
	b.cfg.Logger = slog.New(slog.NewJSONHandler(&records, nil))
	b.imageControl = &brokerImageControl{cutID: "phase-fixture"}
	before, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	emitBrokerImageEvent(b, "freeze_begin", 0)
	owner, err := startBrokerImageAuth(b, nc, brokerAuthorityDeadline(t, 3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	emitBrokerImageEvent(b, "control_ready", 0)
	if err := stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	after, err := rawsocket.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]int{}
	for _, data := range bytes.Split(bytes.TrimSpace(records.Bytes()), []byte{'\n'}) {
		var record struct {
			Message string `json:"msg"`
			CutID   string `json:"cut_id"`
			Event   string `json:"event"`
			BootNS  int64  `json:"boot_ns"`
		}
		if err := json.Unmarshal(data, &record); err != nil {
			t.Fatal(err)
		}
		if record.Message != "broker: image phase" || record.CutID != "phase-fixture" || record.BootNS < before || record.BootNS > after {
			t.Fatalf("collector cannot use producer record: %s", data)
		}
		seen[record.Event]++
	}
	for _, event := range []string{"freeze_begin", "auth_sub_flush", "auth_reply", "auth_usable", "control_ready", "auth_off"} {
		if seen[event] != 1 {
			t.Fatalf("missing/duplicate %s: %v", event, seen)
		}
	}
}

// An ACTIVE refusal follows a candidate ACK and original TCP/SS progress. Only
// a new current cut may redispatch; restoring the first cut loses this ACK.
