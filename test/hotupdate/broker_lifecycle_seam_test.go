//go:build linux && hotupdate_integration

// Repeated real-process lifecycle cuts run in the existing per-test matrix.
// origin: hot-update S10 plan §11.1 / TEST-BUDGET

package hotupdate_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"golang.org/x/sys/unix"
)

// P26 owns the real serve process and its current-cut fault barrier. Bindings
// keep the frozen signatures; unsupported lifecycle receipts fail explicitly
// until the full collector supplies the requested public evidence.
// origin: hot-update S6 plan §2.1 / §12.1 P28
var brokerSeamStart clusterharness.StartBrokerImageFixtureFunc = clusterharness.StartBrokerLifecycleFixture
var brokerSeamCheckpoint clusterharness.BrokerImageCheckpointFunc = (*clusterharness.BrokerImageFixture).Checkpoint
var brokerSeamFault clusterharness.BrokerImageArmFaultFunc = (*clusterharness.BrokerImageFixture).ArmFault
var brokerSeamClose clusterharness.BrokerImageCloseFunc = (*clusterharness.BrokerImageFixture).Close

// Only public counters enter this consumer. ACK rows come from the independent
// P26 pre-replay query, not the bundle's StoreStamp; FD identities are checked
// against /proc while the owner is alive. No ticket/key/SS state is decoded here.
// origin: hot-update S6 plan §14 / T02 / T49 / T52
type brokerSeamReceipt struct {
	Claim              string             `json:"claim"`
	TestOrArm          string             `json:"test_or_arm"`
	Command            string             `json:"command"`
	NativeRC           *int               `json:"native_rc"`
	Verdict            string             `json:"verdict"`
	SourceSHA          string             `json:"source_sha"`
	ArtifactSHA        string             `json:"artifact_sha"`
	PreviousSHA        string             `json:"previous_sha"`
	PID                int                `json:"pid"`
	StartTicks         uint64             `json:"startticks"`
	WallSeconds        float64            `json:"wall_seconds"`
	PhaseIntersections json.RawMessage    `json:"phase_intersections"`
	OracleInputs       json.RawMessage    `json:"oracle_inputs"`
	MutationDigest     *string            `json:"mutation_digest"`
	StdoutPath         string             `json:"stdout_path"`
	StderrPath         string             `json:"stderr_path"`
	LogMapping         json.RawMessage    `json:"log_mapping"`
	Counters           brokerSeamCounters `json:"counters"`
}

type brokerSeamCounters struct {
	Mode               string             `json:"mode"`
	Generation         uint64             `json:"generation"`
	CutID              string             `json:"cut_id"`
	Boundary           string             `json:"rollback_boundary"`
	RunningSHA         string             `json:"running_sha256"`
	ExecErrors         uint32             `json:"exec_errors"`
	Reopens            uint32             `json:"current_reopens"`
	OldDefers          uint32             `json:"old_defer_count"`
	StopCurrentCalls   uint32             `json:"stop_current_calls"`
	Store              storage.StoreStamp `json:"store"`
	TerminalIndex      *uint64            `json:"terminal_index"`
	ReadStage          string             `json:"ack_read_stage"`
	ACKRows            []brokerSeamACK    `json:"ack_rows"`
	NewACKIDs          []string           `json:"new_ack_ids"`
	DriverWriters      uint32             `json:"driver_writers"`
	DriverReaders      uint32             `json:"driver_readers"`
	ActiveRPCs         uint32             `json:"active_rpcs"`
	ActiveBackups      uint32             `json:"active_backups"`
	ActiveMonitors     *uint32            `json:"active_monitors"`
	ActiveCallbacks    *uint32            `json:"active_callbacks"`
	CallbackEnteredSeq uint64             `json:"callback_entered_seq"`
	CallbackJoinedSeq  uint64             `json:"callback_joined_seq"`
	NCClosed           bool               `json:"nc_closed"`
	NodeClosed         bool               `json:"node_closed"`
	FDs                []brokerSeamFD     `json:"owned_fds"`
	UnixPath           string             `json:"unix_path"`
	Tunnel             tunnel.ServerImage `json:"tunnel"`
	RevokeACKSeq       uint64             `json:"revoke_ack_seq"`
	PrepareSeq         uint64             `json:"prepare_seq"`
	NextPrepareSeq     uint64             `json:"next_prepare_seq"`
	ExecSeq            uint64             `json:"exec_seq"`
	StaleExecRefused   bool               `json:"stale_exec_refused"`
	OldResumeRefused   bool               `json:"old_resume_refused"`
	VictimRawIDs       []string           `json:"victim_raw_ids"`
	VictimListenIDs    []string           `json:"victim_listener_ids"`
	VictimPeerEOF      bool               `json:"victim_peer_eof"`
	VictimWriteRefused bool               `json:"victim_write_refused"`
}

type brokerSeamACK struct {
	RequestID string `json:"request_id"`
	ValueSHA  string `json:"value_sha256"`
	Effects   uint32 `json:"effects"`
}

type brokerSeamFD struct {
	ID     string `json:"id"`
	Kind   string `json:"kind"` // raw | listener | lock | private
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Count  int    `json:"count"`
}

// The full multiset includes flags and type: equal totals cannot hide a leaked
// Taken file replacing a correctly closed raw/listener. Numeric FD slots may
// change during adoption, so the comparison is about objects, not slot numbers.
// origin: hot-update S6 plan §6.2 / §11 T52 / T56
type brokerSeamFDKey struct {
	Device, Inode uint64
	Mode          uint32
	Flags         uint64
}

func brokerSeamFDMultiset(pid int) (map[brokerSeamFDKey]int, error) {
	base := filepath.Join("/proc", strconv.Itoa(pid), "fd")
	entries, err := os.ReadDir(base)
	if err != nil {
		return nil, err
	}
	result := make(map[brokerSeamFDKey]int)
	for _, entry := range entries {
		var stat syscall.Stat_t
		if err := syscall.Stat(filepath.Join(base, entry.Name()), &stat); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue // ReadDir's own transient FD, or a joined control worker.
			}
			return nil, err
		}
		info, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "fdinfo", entry.Name()))
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, err
		}
		var flags uint64
		found := false
		for _, line := range strings.Split(string(info), "\n") {
			if value, ok := strings.CutPrefix(line, "flags:\t"); ok {
				flags, err = strconv.ParseUint(strings.TrimSpace(value), 8, 64)
				if err != nil {
					return nil, err
				}
				found = true
			}
		}
		if !found {
			return nil, errors.New("FD census lacks flags")
		}
		result[brokerSeamFDKey{Device: uint64(stat.Dev), Inode: stat.Ino, Mode: stat.Mode & syscall.S_IFMT, Flags: flags}]++
	}
	return result, nil
}

func brokerSeamFDCount(set map[brokerSeamFDKey]int, resource brokerSeamFD) int {
	count := 0
	for key, n := range set {
		if key.Device == resource.Device && key.Inode == resource.Inode {
			count += n
		}
	}
	return count
}

func brokerSeamSHA(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if c < '0' || c > '9' {
			if c < 'a' || c > 'f' {
				return false
			}
		}
	}
	return true
}

func decodeBrokerSeamReceipt(row clusterharness.BrokerImageReceipt, label string) (brokerSeamReceipt, error) {
	var receipt brokerSeamReceipt
	if row.Label != label || len(row.JSON) == 0 || len(row.JSON) > 4<<20 || row.Digest != fmt.Sprintf("%x", sha256.Sum256(row.JSON)) {
		return receipt, errors.New("fixture receipt label/size/digest mismatch")
	}
	if err := json.Unmarshal(row.JSON, &receipt); err != nil {
		return receipt, errors.New("fixture receipt JSON invalid")
	}
	if receipt.Claim == "" || receipt.TestOrArm == "" || receipt.Command == "" || receipt.NativeRC == nil || *receipt.NativeRC != 0 || receipt.Verdict != "GREEN" ||
		!brokerSeamSHA(receipt.SourceSHA, 40) || !brokerSeamSHA(receipt.ArtifactSHA, 64) || !brokerSeamSHA(receipt.PreviousSHA, 64) ||
		receipt.PID <= 0 || receipt.StartTicks == 0 || receipt.Counters.Generation == 0 || receipt.Counters.RunningSHA != receipt.ArtifactSHA ||
		receipt.WallSeconds <= 0 || len(receipt.PhaseIntersections) == 0 || string(receipt.PhaseIntersections) == "null" ||
		len(receipt.OracleInputs) == 0 || string(receipt.OracleInputs) == "null" || receipt.MutationDigest == nil ||
		!brokerSeamReceiptPath(receipt.StdoutPath) || !brokerSeamReceiptPath(receipt.StderrPath) || len(receipt.LogMapping) == 0 || string(receipt.LogMapping) == "null" {
		return receipt, errors.New("fixture receipt lacks native/source/process evidence")
	}
	return receipt, nil
}

func brokerSeamReceiptPath(path string) bool {
	return path != "" && path != "." && !filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.HasPrefix(path, "..")
}

func startBrokerSeam(t *testing.T, topology string) (*clusterharness.BrokerImageFixture, func(), string) {
	t.Helper()
	if brokerSeamStart == nil || brokerSeamCheckpoint == nil || brokerSeamFault == nil || brokerSeamClose == nil {
		t.Fatal("NOT_RUN pending P26: frozen real-process/store fixture functions are not installed")
	}
	root := filepath.Dir(t.TempDir())
	testharness.GuardProcessDirectory(t, root)
	fixture, err := brokerSeamStart(t, clusterharness.BrokerImageFixtureConfig{Topology: topology})
	var once sync.Once
	closeFixture := func() {
		once.Do(func() {
			if fixture != nil {
				if err := brokerSeamClose(fixture); err != nil {
					t.Errorf("fixture close/join: %v", err)
				}
			}
		})
	}
	// Register partial custody before inspecting err, and also close explicitly
	// before the leak baseline is checked. A failed constructor can own children.
	t.Cleanup(closeFixture)
	if err != nil || fixture == nil {
		closeFixture()
		t.Fatalf("real broker fixture startup failed: %v", err)
	}
	return fixture, closeFixture, root
}

func readBrokerSeam(t *testing.T, fixture *clusterharness.BrokerImageFixture, label string) brokerSeamReceipt {
	t.Helper()
	row, err := brokerSeamCheckpoint(fixture, label)
	if err != nil {
		t.Fatalf("checkpoint %s: %v", label, err)
	}
	receipt, err := decodeBrokerSeamReceipt(row, label)
	if err != nil {
		t.Fatalf("checkpoint %s: %v", label, err)
	}
	// Do not let an exited process or a recycled PID certify product cleanup.
	stat, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(receipt.PID), "stat"))
	if err != nil {
		t.Fatalf("checkpoint %s owner is not alive", label)
	}
	end := strings.LastIndexByte(string(stat), ')')
	if end < 0 {
		t.Fatal("invalid process identity")
	}
	fields := strings.Fields(string(stat)[end+1:])
	if len(fields) < 20 || fields[0] == "Z" || fields[0] == "X" || fields[19] != strconv.FormatUint(receipt.StartTicks, 10) {
		t.Fatal("checkpoint process identity changed or exited")
	}
	if sha := hotUpdateFileSHA(t, filepath.Join("/proc", strconv.Itoa(receipt.PID), "exe")); sha != receipt.Counters.RunningSHA {
		t.Fatal("receipt SHA does not identify the running inode")
	}
	return receipt
}

func requireBrokerSeamFDs(t *testing.T, receipt brokerSeamReceipt) map[brokerSeamFDKey]int {
	t.Helper()
	set, err := brokerSeamFDMultiset(receipt.PID)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	objects := map[[2]uint64]bool{}
	for _, resource := range receipt.Counters.FDs {
		object := [2]uint64{resource.Device, resource.Inode}
		if resource.ID == "" || ids[resource.ID] || objects[object] || resource.Inode == 0 || resource.Count <= 0 || brokerSeamFDCount(set, resource) != resource.Count {
			t.Fatal("owned FD inventory differs from live kernel identities/aliases")
		}
		ids[resource.ID], objects[object] = true, true
	}
	return set
}

func requireBrokerSeamLiveResources(t *testing.T, receipt brokerSeamReceipt) {
	t.Helper()
	kinds := map[string]int{}
	for _, resource := range receipt.Counters.FDs {
		kinds[resource.Kind]++
	}
	if kinds["lock"] != 1 || kinds["raw"] == 0 || kinds["listener"] == 0 {
		t.Fatal("current lifecycle has no real lock/raw/listener closure")
	}
	if receipt.Counters.ActiveMonitors == nil || *receipt.Counters.ActiveMonitors == 0 || receipt.Counters.ActiveCallbacks == nil {
		t.Fatal("current carrier monitor/callback census is absent or never exercised")
	}
}

func brokerSeamUnixIdentity(t *testing.T, root, path string) brokerSeamFDKey {
	t.Helper()
	rel, err := filepath.Rel(root, path)
	if err != nil || path == "" || strings.HasPrefix(rel, "..") || !filepath.IsAbs(path) {
		t.Fatal("Unix listener is outside the private fixture")
	}
	var stat syscall.Stat_t
	if err := syscall.Lstat(path, &stat); err != nil || stat.Mode&syscall.S_IFMT != syscall.S_IFSOCK {
		t.Fatal("original Unix listener pathname is not present")
	}
	return brokerSeamFDKey{Device: uint64(stat.Dev), Inode: stat.Ino, Mode: stat.Mode & syscall.S_IFMT}
}

func requireBrokerSeamLock(t *testing.T, root, lockPath string, held bool) {
	t.Helper()
	rel, err := filepath.Rel(root, lockPath)
	if err != nil || rel == "." || strings.HasPrefix(rel, "..") || !filepath.IsAbs(lockPath) {
		t.Fatal("lock probe is outside the private fixture")
	}
	// A contender never opens SQLite or Bolt and never creates a missing lock.
	// A successful acquisition while a live generation exists is product red.
	file, err := os.OpenFile(lockPath, os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		t.Fatal("canonical lock file disappeared")
	}
	defer func() { _ = file.Close() }()
	err = unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if held {
		if !errors.Is(err, unix.EWOULDBLOCK) && !errors.Is(err, unix.EAGAIN) {
			t.Fatal("current owner did not continuously hold the original flock")
		}
	} else if err != nil {
		t.Fatal("normal stop retained the current flock")
	}
}

// Exec error must be followed by a genuinely new durable ACK, then ordinary
// stop of the replacement owners. Component Close tests cannot certify this.
// origin: hot-update S6 plan §4.2 / T02
func TestCurrentRuntimeStopAfterExecError(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerExecErrorStop(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "current exec-error stop x5", g)
	testharness.AssertNoFDLeak(t, "current exec-error stop x5", fds, 0)
}

func exerciseBrokerExecErrorStop(t *testing.T) {
	t.Helper()
	fixture, closeFixture, root := startBrokerSeam(t, "single")
	defer closeFixture()
	idle := readBrokerSeam(t, fixture, "runtime-idle")
	idleFDs := requireBrokerSeamFDs(t, idle)
	before := readBrokerSeam(t, fixture, "before-reload")
	requireBrokerSeamFDs(t, before)
	requireBrokerSeamLiveResources(t, before)
	unixPath := brokerSeamUnixIdentity(t, root, before.Counters.UnixPath)
	requireBrokerSeamLock(t, root, before.Counters.Store.Identity.LockPath, true)
	if err := brokerSeamFault(fixture, clusterharness.BrokerImageFault{Point: "prepared-exec-error", Ordinal: 1, Once: true}); err != nil {
		t.Fatal(err)
	}
	after := readBrokerSeam(t, fixture, "exec-error-new-ack")
	if err := brokerExecErrorEvidence(before, after); err != nil {
		t.Fatal(err)
	}
	requireBrokerSeamFDs(t, after)
	requireBrokerSeamLiveResources(t, after)
	if next := brokerSeamUnixIdentity(t, root, after.Counters.UnixPath); next != unixPath {
		t.Fatal("exec-error reopen replaced the original Unix pathname inode")
	}
	requireBrokerSeamLock(t, root, after.Counters.Store.Identity.LockPath, true)
	stopped := readBrokerSeam(t, fixture, "normal-stop")
	requireBrokerSeamStopped(t, root, idleFDs, after, stopped)
}

func brokerExecErrorEvidence(before, after brokerSeamReceipt) error {
	if !brokerSeamSameProcess(before, after) || after.Counters.Generation != before.Counters.Generation || after.Counters.RunningSHA != before.Counters.RunningSHA ||
		after.Counters.ExecErrors != 1 || after.Counters.Reopens != 1 || after.Counters.OldDefers != 0 || after.Counters.DriverWriters != 1 {
		return errors.New("exec error did not recover the same process through current owners exactly once")
	}
	return brokerSeamACKProgress(before.Counters, after.Counters)
}

func brokerSeamSameProcess(before, after brokerSeamReceipt) bool {
	return before.PID == after.PID && before.StartTicks == after.StartTicks && before.SourceSHA == after.SourceSHA &&
		(before.Counters.Mode == "single" || before.Counters.Mode == "sole-voter" || before.Counters.Mode == "quorum") && before.Counters.Mode == after.Counters.Mode &&
		reflect.DeepEqual(before.Counters.Store.Identity, after.Counters.Store.Identity) &&
		reflect.DeepEqual(before.Counters.Store.Schema, after.Counters.Store.Schema) && before.Counters.Store.Journal == after.Counters.Store.Journal &&
		after.Counters.Store.Synchronous == 2 // SQLite FULL; reopen must not silently lower it.
}

func brokerSeamACKProgress(before, after brokerSeamCounters) error {
	// Single has no Raft SQL cursor. New independently queried ACK IDs prove
	// progress there; manufacturing an apply index would change the store contract.
	// origin: hot-update S6 plan §3.3 / T02 / T49
	cursorValid := after.Store.AppliedIndex > before.Store.AppliedIndex && after.Store.AppliedTerm >= before.Store.AppliedTerm
	if before.Mode == "single" {
		cursorValid = before.Store.AppliedIndex == 0 && before.Store.AppliedTerm == 0 && after.Store.AppliedIndex == 0 && after.Store.AppliedTerm == 0
	}
	if after.ReadStage != "before-replay" || !cursorValid || len(before.ACKRows) == 0 || len(after.NewACKIDs) == 0 {
		return errors.New("missing independent pre-replay ACK prefix or new durable ACK")
	}
	rows := make(map[string]brokerSeamACK)
	for _, row := range after.ACKRows {
		if row.RequestID == "" || !brokerSeamSHA(row.ValueSHA, 64) || row.Effects != 1 || rows[row.RequestID].RequestID != "" {
			return errors.New("ACK rows are absent, duplicated or changed")
		}
		rows[row.RequestID] = row
	}
	old := make(map[string]bool)
	for _, row := range before.ACKRows {
		if row.RequestID == "" || old[row.RequestID] || row.Effects != 1 || rows[row.RequestID] != row {
			return errors.New("previous ACK prefix was lost before replay")
		}
		old[row.RequestID] = true
	}
	newIDs := make(map[string]bool)
	for _, id := range after.NewACKIDs {
		if id == "" || old[id] || newIDs[id] || rows[id].RequestID == "" {
			return errors.New("new ACK is not an independent durable effect")
		}
		newIDs[id] = true
	}
	return nil
}

func requireBrokerSeamStopped(t *testing.T, root string, idleFDs map[brokerSeamFDKey]int, current, stopped brokerSeamReceipt) {
	t.Helper()
	if err := brokerSeamStopEvidence(current, stopped); err != nil {
		t.Fatal(err)
	}
	if set := requireBrokerSeamFDs(t, stopped); !reflect.DeepEqual(set, idleFDs) {
		t.Fatal("normal stop changed the idle FD identity multiset or retained a Taken/private/raw/listener alias")
	}
	requireBrokerSeamLock(t, root, current.Counters.Store.Identity.LockPath, false)
	path := current.Counters.UnixPath
	rel, err := filepath.Rel(root, path)
	if err != nil || path == "" || strings.HasPrefix(rel, "..") || !filepath.IsAbs(path) {
		t.Fatal("missing private original Unix pathname")
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("normal stop did not unlink the current original Unix pathname")
	}
}

func brokerSeamStopEvidence(current, stopped brokerSeamReceipt) error {
	c := stopped.Counters
	if !brokerSeamSameProcess(current, stopped) || c.Generation != current.Counters.Generation || c.RunningSHA != current.Counters.RunningSHA ||
		!c.NCClosed || !c.NodeClosed || c.DriverWriters != 0 || c.DriverReaders != 0 || c.ActiveRPCs != 0 || c.ActiveBackups != 0 ||
		c.ActiveMonitors == nil || *c.ActiveMonitors != 0 || c.ActiveCallbacks == nil || *c.ActiveCallbacks != 0 ||
		c.StopCurrentCalls != 1 || len(c.FDs) != 0 || c.OldDefers != 0 {
		return errors.New("normal stop failed to close the current complete owner graph")
	}
	// The live cursor may advance before stop fences its last background commit.
	// Require exact agreement with the independent pre-close cursor and retain
	// both monotonicity and the exact durable ACK rows across terminal cleanup.
	// origin: hot-update S6 plan §4.2 / T52
	if !reflect.DeepEqual(c.ACKRows, current.Counters.ACKRows) || c.TerminalIndex == nil || c.Store.AppliedIndex != *c.TerminalIndex || c.Store.AppliedIndex < current.Counters.Store.AppliedIndex || c.Store.AppliedTerm < current.Counters.Store.AppliedTerm {
		return fmt.Errorf("normal stop lost the latest durable ACK state: index %d -> %d, ACKs %v -> %v", current.Counters.Store.AppliedIndex, c.Store.AppliedIndex, current.Counters.ACKRows, c.ACKRows)
	}
	return nil
}

// An admitted hard revoke wins a barrier after Prepare and before ExecCommit.
// The stale Prepared must be refused; the next real exec must carry the new
// fences and keep the surviving originals. All victim aliases disappear while
// the process is alive, rather than being cleaned up by fixture termination.
// origin: hot-update S6 plan §6.2 / T25
func TestBrokerPreparedListenerRevokeCannotResurrectEndpoint(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerPreparedRevoke(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "prepared late revoke x5", g)
	testharness.AssertNoFDLeak(t, "prepared late revoke x5", fds, 0)
}

func exerciseBrokerPreparedRevoke(t *testing.T) {
	t.Helper()
	fixture, closeFixture, _ := startBrokerSeam(t, "single")
	defer closeFixture()
	if err := brokerSeamFault(fixture, clusterharness.BrokerImageFault{Point: "after-prepare-before-exec", Ordinal: 1, Once: true}); err != nil {
		t.Fatal(err)
	}
	prepared := readBrokerSeam(t, fixture, "prepared-revoke-barrier")
	requireBrokerSeamFDs(t, prepared)
	// The fixture issues the real full-allocation revoke and waits for its ACK;
	// it never closes the external victim peers to help the product pass.
	revoked := readBrokerSeam(t, fixture, "hard-revoke-acked")
	if err := brokerSeamRevokeEvidence(prepared, revoked); err != nil {
		t.Fatal(err)
	}
	set := requireBrokerSeamFDs(t, revoked)
	victims := append(append([]string(nil), prepared.Counters.VictimRawIDs...), prepared.Counters.VictimListenIDs...)
	for _, id := range victims {
		for _, resource := range prepared.Counters.FDs {
			if resource.ID == id && brokerSeamFDCount(set, resource) != 0 {
				t.Fatal("hard revoke left an original/Prepared/salvage victim alias alive")
			}
		}
	}
	after := readBrokerSeam(t, fixture, "revoke-next-prepared-control-ready")
	if !brokerSeamSameProcess(prepared, after) || after.Counters.Generation != prepared.Counters.Generation+1 || after.Counters.RunningSHA == prepared.Counters.RunningSHA ||
		after.Counters.NextPrepareSeq <= revoked.Counters.RevokeACKSeq || after.Counters.ExecSeq <= after.Counters.NextPrepareSeq || !after.Counters.OldResumeRefused {
		t.Fatal("late revoke did not force a new Prepared and a real same-PID exec")
	}
	requireBrokerSeamFDs(t, after)
	for _, resource := range prepared.Counters.FDs {
		victim := false
		for _, id := range victims {
			victim = victim || id == resource.ID
		}
		if victim {
			for _, current := range after.Counters.FDs {
				if current.ID == resource.ID || current.Device == resource.Device && current.Inode == resource.Inode {
					t.Fatal("next Prepared resurrected a revoked resource")
				}
			}
			continue
		}
		if resource.Kind == "raw" || resource.Kind == "listener" {
			found := false
			for _, current := range after.Counters.FDs {
				found = found || current.ID == resource.ID && current.Device == resource.Device && current.Inode == resource.Inode
			}
			if !found {
				t.Fatal("late revoke replaced a surviving original socket/listener")
			}
		}
	}
	// The fixture's projection at this barrier contains survivors only, so its
	// byte oracle cannot mistake the intentionally discarded victim tail for loss.
	if err := brokerSeamFlowProgress(revoked.Counters.Tunnel, after.Counters.Tunnel, true); err != nil {
		t.Fatal(err)
	}
}

func brokerSeamRevokeEvidence(prepared, revoked brokerSeamReceipt) error {
	p, r := prepared.Counters, revoked.Counters
	if !brokerSeamSameProcess(prepared, revoked) || p.PrepareSeq == 0 || r.RevokeACKSeq <= p.PrepareSeq ||
		p.CallbackEnteredSeq == 0 || p.CallbackJoinedSeq <= p.CallbackEnteredSeq || p.CallbackJoinedSeq >= p.PrepareSeq ||
		!r.StaleExecRefused || !r.OldResumeRefused || !r.VictimPeerEOF || !r.VictimWriteRefused || len(p.VictimRawIDs) == 0 || len(p.VictimListenIDs) == 0 {
		return errors.New("missing ordered prepared/ACK revoke, irreversible raw shutdown or old RESUME refusal")
	}
	seen := make(map[string]bool)
	for _, group := range []struct {
		ids  []string
		kind string
	}{{p.VictimRawIDs, "raw"}, {p.VictimListenIDs, "listener"}} {
		for _, id := range group.ids {
			if id == "" || seen[id] {
				return errors.New("victim census is empty or duplicated")
			}
			seen[id] = true
			found := false
			for _, resource := range p.FDs {
				if resource.ID == id && resource.Kind == group.kind && resource.Count >= 2 {
					found = true // Original plus a real Prepared alias is mandatory.
				}
			}
			if !found {
				return errors.New("late revoke never exercised a prepared victim alias")
			}
			for _, resource := range r.FDs {
				if resource.ID == id {
					return errors.New("hard revoke retained a victim resource")
				}
			}
		}
	}
	return nil
}

// Controls exercise the same ACK/close/revoke comparators used by the pending
// real fixture, with counterexamples for every required product fault. They
// cannot certify that any actual Broker.Run has made these observations.
// origin: hot-update S6 plan §11 T02 / T25 / §14
func TestBrokerLifecycleReceiptOracleControls(t *testing.T) {
	before, _, _ := brokerSeamOracleInputs()
	after := cloneBrokerSeamReceipt(t, before)
	after.Counters.ExecErrors, after.Counters.Reopens = 1, 1
	after.Counters.Store.AppliedIndex++
	after.Counters.ReadStage = "before-replay"
	after.Counters.NewACKIDs = []string{"new"}
	after.Counters.ACKRows = append(after.Counters.ACKRows, brokerSeamACK{RequestID: "new", ValueSHA: strings.Repeat("e", 64), Effects: 1})
	if err := brokerExecErrorEvidence(before, after); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*brokerSeamReceipt)
	}{
		{"captured-old-db", func(r *brokerSeamReceipt) { r.Counters.ACKRows = before.Counters.ACKRows }},
		{"captured-old-nc", func(r *brokerSeamReceipt) { r.Counters.OldDefers = 1 }},
		{"exec-never-called", func(r *brokerSeamReceipt) { r.Counters.ExecErrors = 0 }},
		{"second-opener", func(r *brokerSeamReceipt) { r.Counters.Reopens = 2 }},
		{"new-ack-not-new", func(r *brokerSeamReceipt) { r.Counters.NewACKIDs = []string{"old"} }},
		{"duplicate-new-ack", func(r *brokerSeamReceipt) { r.Counters.NewACKIDs = []string{"new", "new"} }},
		{"no-new-ack", func(r *brokerSeamReceipt) { r.Counters.NewACKIDs = nil }},
		{"ack-after-replay", func(r *brokerSeamReceipt) { r.Counters.ReadStage = "after-replay" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := cloneBrokerSeamReceipt(t, after)
			tc.mutate(&changed)
			if brokerExecErrorEvidence(before, changed) == nil {
				t.Fatal("exec-error oracle accepted the injected defect")
			}
		})
	}
	// A real single store has no Raft cursor, including after durable new rows.
	singleBefore, singleAfter := cloneBrokerSeamReceipt(t, before), cloneBrokerSeamReceipt(t, after)
	for _, row := range []*brokerSeamReceipt{&singleBefore, &singleAfter} {
		row.Counters.Mode = "single"
		row.Counters.Store.AppliedIndex, row.Counters.Store.AppliedTerm = 0, 0
	}
	if err := brokerExecErrorEvidence(singleBefore, singleAfter); err != nil {
		t.Fatal(err)
	}
	prepared := cloneBrokerSeamReceipt(t, before)
	prepared.Counters.PrepareSeq = 10
	prepared.Counters.CallbackEnteredSeq, prepared.Counters.CallbackJoinedSeq = 1, 2
	prepared.Counters.VictimRawIDs = []string{"victim.raw"}
	prepared.Counters.VictimListenIDs = []string{"victim.listener"}
	prepared.Counters.FDs = []brokerSeamFD{{ID: "victim.raw", Kind: "raw", Device: 1, Inode: 2, Count: 2}, {ID: "victim.listener", Kind: "listener", Device: 1, Inode: 3, Count: 2}}
	revoked := cloneBrokerSeamReceipt(t, prepared)
	revoked.Counters.RevokeACKSeq = 11
	revoked.Counters.StaleExecRefused, revoked.Counters.OldResumeRefused = true, true
	revoked.Counters.VictimPeerEOF, revoked.Counters.VictimWriteRefused = true, true
	revoked.Counters.FDs = nil
	if err := brokerSeamRevokeEvidence(prepared, revoked); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*brokerSeamReceipt)
	}{
		{"original-only-close", func(r *brokerSeamReceipt) { r.Counters.FDs = []brokerSeamFD{{ID: "victim.raw", Kind: "raw", Count: 1}} }},
		{"listener-alias-retained", func(r *brokerSeamReceipt) {
			r.Counters.FDs = []brokerSeamFD{{ID: "victim.listener", Kind: "listener", Count: 1}}
		}},
		{"preliminary-fence", func(r *brokerSeamReceipt) { r.Counters.OldResumeRefused = false }},
		{"stale-prepared-exec", func(r *brokerSeamReceipt) { r.Counters.StaleExecRefused = false }},
		{"revoke-before-prepare", func(r *brokerSeamReceipt) { r.Counters.RevokeACKSeq = 9 }},
		{"raw-not-shutdown", func(r *brokerSeamReceipt) { r.Counters.VictimWriteRefused = false }},
		{"silent-victim-kept", func(r *brokerSeamReceipt) { r.Counters.VictimPeerEOF = false }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := cloneBrokerSeamReceipt(t, revoked)
			tc.mutate(&changed)
			if brokerSeamRevokeEvidence(prepared, changed) == nil {
				t.Fatal("prepared revoke oracle accepted the injected defect")
			}
		})
	}
	prepared.Counters.FDs[0].Count = 1
	if brokerSeamRevokeEvidence(prepared, revoked) == nil {
		t.Fatal("revoke oracle accepted a test with no actual Prepared victim alias")
	}
	prepared.Counters.FDs[0].Count = 2
	prepared.Counters.CallbackJoinedSeq = 0
	if brokerSeamRevokeEvidence(prepared, revoked) == nil {
		t.Fatal("prepared revoke oracle accepted a fired timer callback without its join")
	}
	valid := cloneBrokerSeamReceipt(t, before)
	rc := 0
	valid.Claim, valid.TestOrArm, valid.Command, valid.Verdict, valid.NativeRC = "current-owner", "T02", "sandbox broker reload", "GREEN", &rc
	valid.ArtifactSHA = valid.Counters.RunningSHA
	emptyDigest := ""
	valid.WallSeconds, valid.MutationDigest = 1, &emptyDigest
	valid.PhaseIntersections, valid.OracleInputs, valid.LogMapping = json.RawMessage("[]"), json.RawMessage("{}"), json.RawMessage("{}")
	valid.StdoutPath, valid.StderrPath = "stdout.log", "stderr.log"
	data, err := json.Marshal(valid)
	if err != nil {
		t.Fatal(err)
	}
	row := clusterharness.BrokerImageReceipt{Label: "before-reload", JSON: data, Digest: fmt.Sprintf("%x", sha256.Sum256(data))}
	if _, err := decodeBrokerSeamReceipt(row, row.Label); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*clusterharness.BrokerImageReceipt){
		func(r *clusterharness.BrokerImageReceipt) { r.Digest = strings.Repeat("0", 64) },
		func(r *clusterharness.BrokerImageReceipt) { r.Label = "different-cut" },
		func(r *clusterharness.BrokerImageReceipt) {
			r.JSON = []byte("{} ")
			r.Digest = fmt.Sprintf("%x", sha256.Sum256(r.JSON))
		},
	} {
		changed := row
		mutate(&changed)
		if _, err := decodeBrokerSeamReceipt(changed, row.Label); err == nil {
			t.Fatal("receipt accepted a digest/label/identity substitution")
		}
	}
	for _, mutate := range []func(*brokerSeamReceipt){
		func(r *brokerSeamReceipt) { r.WallSeconds = 0 },
		func(r *brokerSeamReceipt) { r.PhaseIntersections = nil },
		func(r *brokerSeamReceipt) { r.OracleInputs = nil },
		func(r *brokerSeamReceipt) { r.MutationDigest = nil },
		func(r *brokerSeamReceipt) { r.StdoutPath = "/outside/stdout.log" },
		func(r *brokerSeamReceipt) { r.StderrPath = "../outside/stderr.log" },
		func(r *brokerSeamReceipt) { r.LogMapping = nil },
	} {
		changed := cloneBrokerSeamReceipt(t, valid)
		mutate(&changed)
		data, err := json.Marshal(changed)
		if err != nil {
			t.Fatal(err)
		}
		row := clusterharness.BrokerImageReceipt{Label: "before-reload", JSON: data, Digest: fmt.Sprintf("%x", sha256.Sum256(data))}
		if _, err := decodeBrokerSeamReceipt(row, row.Label); err == nil {
			t.Fatal("receipt accepted missing phase/native evidence or an archive path escape")
		}
	}
}

// Exercise the kernel oracle independently while P26 is unavailable: original
// plus dup count, irreversible shutdown across that dup, and same-total object
// substitution. This is an oracle test, not a fake broker lifecycle fixture.
// origin: hot-update S6 plan §11 T25 / T52 / T56Integration
func TestBrokerLifecycleKernelOracleCycles(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerSeamKernelOracle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker kernel oracle x5", g)
	testharness.AssertNoFDLeak(t, "broker kernel oracle x5", fds, 0)
}

func exerciseBrokerSeamKernelOracle(t *testing.T) {
	t.Helper()
	pair, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(pair[0]); _ = unix.Close(pair[1]) }()
	alias, err := unix.FcntlInt(uintptr(pair[0]), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = unix.Close(alias) }()
	var stat unix.Stat_t
	if err := unix.Fstat(pair[0], &stat); err != nil {
		t.Fatal(err)
	}
	resource := brokerSeamFD{ID: "oracle.raw", Kind: "raw", Device: uint64(stat.Dev), Inode: stat.Ino, Count: 2}
	set, err := brokerSeamFDMultiset(os.Getpid())
	if err != nil || brokerSeamFDCount(set, resource) != 2 {
		t.Fatal("kernel census missed the original/dup OFD aliases")
	}
	if err := unix.Shutdown(pair[0], unix.SHUT_RDWR); err != nil {
		t.Fatal(err)
	}
	if n, err := unix.Write(alias, []byte("unauthorized")); n > 0 || !errors.Is(err, unix.EPIPE) {
		t.Fatal("hard shutdown left a duplicate able to send")
	}
	if n, err := unix.Read(pair[1], make([]byte, 1)); n != 0 || err != nil {
		t.Fatal("silent victim peer did not observe shutdown EOF")
	}
	// A multiset cannot accept equal-total substitution: swapping one identity
	// for another is a leak even though a simple FDCount would be unchanged.
	changed := make(map[brokerSeamFDKey]int)
	for key, count := range set {
		changed[key] = count
	}
	for key, count := range changed {
		if key.Device == resource.Device && key.Inode == resource.Inode {
			delete(changed, key)
			key.Inode++
			changed[key] = count
			break
		}
	}
	if reflect.DeepEqual(set, changed) || brokerSeamFDCount(changed, resource) != 0 {
		t.Fatal("kernel identity oracle collapsed to a total descriptor count")
	}
}
