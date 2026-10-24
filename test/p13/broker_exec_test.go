//go:build linux && p13_integration

// Repeated real-binary cuts exceed the default package budget together. The
// broker matrix runs each test separately with this suite-local tag.
// origin: hot-update S6 plan §9.3 / T48 / TESTFIX
package p13_test

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/test/clusterharness"
)

// P26 has one real-process/store fixture; P28 does not build another broker or
// substitute a fake store/exec. The real product binding refuses unsupported
// full-cohort checkpoints; SSH/agent/FIN evidence cannot be fabricated from TCP.
// origin: hot-update S6 plan §2.1 / §12.1 P28
var brokerCohortStart clusterharness.StartBrokerImageFixtureFunc = clusterharness.StartBrokerCohortFixture
var brokerCohortCheckpoint clusterharness.BrokerImageCheckpointFunc = (*clusterharness.BrokerImageFixture).Checkpoint
var brokerCohortClose clusterharness.BrokerImageCloseFunc = (*clusterharness.BrokerImageFixture).Close

type brokerCohortReceipt struct {
	Claim              string               `json:"claim"`
	TestOrArm          string               `json:"test_or_arm"`
	Command            string               `json:"command"`
	NativeRC           *int                 `json:"native_rc"`
	Verdict            string               `json:"verdict"`
	SourceSHA          string               `json:"source_sha"`
	ArtifactSHA        string               `json:"artifact_sha"`
	PreviousSHA        string               `json:"previous_sha"`
	PID                int                  `json:"pid"`
	StartTicks         uint64               `json:"startticks"`
	WallSeconds        float64              `json:"wall_seconds"`
	PhaseIntersections json.RawMessage      `json:"phase_intersections"`
	MutationDigest     *string              `json:"mutation_digest"`
	StdoutPath         string               `json:"stdout_path"`
	StderrPath         string               `json:"stderr_path"`
	LogMapping         json.RawMessage      `json:"log_mapping"`
	Counters           brokerCohortCounters `json:"counters"`
	OracleInputs       struct {
		FlowLedger string `json:"flow_ledger"`
	} `json:"oracle_inputs"`
}

type brokerCohortCounters struct {
	Generation    uint64                     `json:"generation"`
	RunningSHA    string                     `json:"running_sha256"`
	AgentPID      int                        `json:"agent_pid"`
	AgentTicks    uint64                     `json:"agent_startticks"`
	AgentSHA      string                     `json:"agent_running_sha256"`
	CutBootNS     int64                      `json:"cut_boot_ns"`
	ReadyBootNS   int64                      `json:"ready_boot_ns"`
	Streams       []brokerCohortStream       `json:"streams"`
	Listeners     []listenerimage.Descriptor `json:"listeners"`
	ExpectedSlots uint64                     `json:"expected_connect_slots"`
	Connects      []brokerCohortConnect      `json:"connects"`
	PendingOpen   brokerCohortOpening        `json:"pending_open"`
}

// Public evidence contains no SS salts, nonces, keys or plaintext. The external
// independent SS decoder supplies only counts and application-payload hashes.
// Kernel identities of the actual sockets and SSH child are checked separately.
// origin: hot-update S6 plan §6.3 / §14 / T48
type brokerCohortStream struct {
	ID                string `json:"id"`
	Protocol          string `json:"protocol"` // ssh | tcp | ss
	PublicPID         int    `json:"public_pid"`
	PublicFD          int    `json:"public_fd"`
	TargetPID         int    `json:"target_pid"`
	TargetFD          int    `json:"target_fd"`
	AppPID            int    `json:"app_pid"`
	AppTicks          uint64 `json:"app_startticks"`
	AppSHA            string `json:"app_running_sha256"`
	TargetAccepts     uint32 `json:"target_accepts"`
	SSHeaders         uint32 `json:"ss_headers"`
	SSAuthFailures    uint32 `json:"ss_auth_failures"`
	HalfClosed        bool   `json:"half_closed"`
	HalfCloseSequence uint64 `json:"half_close_sequence"`
	FinalSequence     uint64 `json:"final_sequence"`
}

// A connecting OPEN has no target bytes before connect. Its census is kept
// separate from the twenty-record duplex streams rather than forging priors.
// origin: hot-update S6 plan §6.3 / T48
type brokerCohortOpening struct {
	Phase         string             `json:"phase"`
	AgentPID      int                `json:"agent_pid"`
	AgentFD       int                `json:"agent_fd"`
	AgentSocket   brokerCohortSocket `json:"agent_socket"`
	ClientSocket  brokerCohortSocket `json:"client_socket"`
	BrokerSocket  brokerCohortSocket `json:"broker_socket"`
	ClientPID     int                `json:"client_pid"`
	ClientFD      int                `json:"client_fd"`
	TargetAccepts uint32             `json:"target_accepts"`
	Opened        int64              `json:"opened_boot_ns"`
	Started       int64              `json:"started_boot_ns"`
	Finished      int64              `json:"finished_boot_ns"`
	PayloadSHA    string             `json:"payload_sha256"`
}

type brokerCohortConnect struct {
	Slot           uint64 `json:"slot"`
	StartedBootNS  int64  `json:"started_boot_ns"`
	FinishedBootNS int64  `json:"finished_boot_ns"`
	NativeRC       *int   `json:"native_rc"`
}

type brokerCohortSample struct {
	ID             string `json:"id"`
	Direction      uint8  `json:"direction"`
	Sequence       uint64 `json:"sequence"`
	PayloadSHA     string `json:"payload_sha256"`
	StartedBootNS  int64  `json:"started_boot_ns"`
	FinishedBootNS int64  `json:"finished_boot_ns"`
	EOF            bool   `json:"eof"`
}

type brokerCohortSocket struct {
	Device, Inode uint64
	Mode          uint32
}

// One unchanged cohort crosses the actual broker exec. Original SSH/TCP/SS
// targets, socket objects and the co-located agent must retain their identities;
// twenty exact samples on each side and the declared FIN tail prevent an idle
// or already finished stream from passing as continuity.
// origin: hot-update S6 plan §6.3 / §11 T48
// origin: S10 internal review FINAL-REAL-FAILS F5
func TestBrokerExecOriginalFlowCohort(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	// Each round owns its original cohort and private store. Overlap two
	// independent lifecycles, then join every cleanup before the leak census;
	// fifteen serial cuts exceeded the unchanged 600s unit budget.
	// origin: S10 internal review FINAL-REAL-FAILS F5 / S10 plan L17 / §11.1
	lanes := make(chan struct{}, 2)
	var active atomic.Int32
	var overlapped atomic.Bool
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				lanes <- struct{}{}
				defer func() { <-lanes }()
				if active.Add(1) == 2 {
					overlapped.Store(true)
				}
				defer active.Add(-1)
				exerciseBrokerOriginalCohort(t)
			})
		}
	})
	if !overlapped.Load() {
		t.Fatal("independent cohort rounds did not overlap within the fixed unit budget")
	}
	testharness.AssertNoGoroutineLeak(t, "broker original cohort x5", g)
	testharness.AssertNoFDLeak(t, "broker original cohort x5", fds, 0)
}

// P33 registers every risk package through a measured TestBroker*LeakCycles
// site. T48's frozen name cannot be renamed to satisfy that separate contract.
// origin: hot-update S6 plan §9.3 / T54 / §12.1 P28
// origin: S10 internal review FINAL-REAL-FAILS F5
func TestBrokerFlowCohortLeakCycles(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	// The joined group keeps all five real resource rounds in the unit while
	// sharing no runtime or cleanup responsibility between the two lanes.
	// origin: S10 internal review FINAL-REAL-FAILS F5 / S10 plan L17 / §11.1
	lanes := make(chan struct{}, 2)
	var active atomic.Int32
	var overlapped atomic.Bool
	t.Run("rounds", func(t *testing.T) {
		for round := 0; round < 5; round++ {
			t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
				t.Parallel()
				lanes <- struct{}{}
				defer func() { <-lanes }()
				if active.Add(1) == 2 {
					overlapped.Store(true)
				}
				defer active.Add(-1)
				exerciseBrokerOriginalCohort(t)
			})
		}
	})
	if !overlapped.Load() {
		t.Fatal("independent cohort rounds did not overlap within the fixed unit budget")
	}
	testharness.AssertNoGoroutineLeak(t, "broker flow cohort lifecycle x5", g)
	testharness.AssertNoFDLeak(t, "broker flow cohort lifecycle x5", fds, 0)
}

func exerciseBrokerOriginalCohort(t *testing.T) {
	t.Helper()
	for _, topology := range []string{"single", "sole-voter", "quorum"} {
		t.Run(topology, func(t *testing.T) { exerciseBrokerCohortTopology(t, topology) })
	}
}

func exerciseBrokerCohortTopology(t *testing.T, topology string) {
	t.Helper()
	if brokerCohortStart == nil || brokerCohortCheckpoint == nil || brokerCohortClose == nil {
		t.Fatal("NOT_RUN pending P26: frozen real broker/flow cohort fixture is not installed")
	}
	root := filepath.Dir(t.TempDir())
	testharness.GuardProcessDirectory(t, root)
	fixture, err := brokerCohortStart(t, clusterharness.BrokerImageFixtureConfig{Topology: topology})
	var once sync.Once
	closeFixture := func() {
		once.Do(func() {
			if fixture != nil {
				if err := brokerCohortClose(fixture); err != nil {
					t.Errorf("cohort fixture close/join: %v", err)
				}
			}
		})
	}
	t.Cleanup(closeFixture)
	defer closeFixture()
	if err != nil || fixture == nil {
		t.Fatalf("real cohort startup failed: %v", err)
	}
	before := readBrokerCohort(t, fixture, "flow-cohort-before-reload")
	beforeSockets := captureBrokerCohort(t, before)
	after := readBrokerCohort(t, fixture, "flow-cohort-after-reload")
	if err := brokerCohortTransition(before, after); err != nil {
		t.Fatal(err)
	}
	if sockets := captureBrokerCohort(t, after); !reflect.DeepEqual(beforeSockets, sockets) {
		t.Fatal("broker exec replaced an original public socket or target Accept")
	}
	// Release the application FIN gate only after seeing real running SHA/ready.
	// The fixture continues the original probes; it never redials a failed stream.
	finished := readBrokerCohort(t, fixture, "flow-cohort-finished")
	if err := brokerCohortTransition(before, finished); err != nil {
		t.Fatal(err)
	}
	if sockets := captureBrokerCohort(t, finished); !reflect.DeepEqual(beforeSockets, sockets) {
		t.Fatal("final FIN/tail was delivered by a replacement socket")
	}
	path := finished.OracleInputs.FlowLedger
	if path == "" || filepath.IsAbs(path) || filepath.Clean(path) != path || strings.HasPrefix(path, "..") {
		t.Fatal("flow ledger must have a private relative archive path")
	}
	file, err := os.Open(filepath.Join(root, path))
	if err != nil {
		t.Fatal("missing independent flow byte ledger")
	}
	defer func() { _ = file.Close() }()
	if stat, err := file.Stat(); err != nil || !stat.Mode().IsRegular() || stat.Size() > 16<<20 {
		t.Fatal("external byte ledger is not a bounded regular file")
	}
	decoder := json.NewDecoder(io.LimitReader(file, 16<<20))
	var samples []brokerCohortSample
	for {
		var sample brokerCohortSample
		if err := decoder.Decode(&sample); err != nil {
			if err == io.EOF {
				break
			}
			t.Fatal("invalid external byte ledger")
		}
		samples = append(samples, sample)
	}
	if err := brokerCohortBytes(finished, samples); err != nil {
		t.Fatal(err)
	}

	last := map[string]int64{}
	var maxGap int64
	for _, sample := range samples {
		key := fmt.Sprintf("%s/%d", sample.ID, sample.Direction)
		if last[key] != 0 {
			maxGap = max(maxGap, sample.FinishedBootNS-last[key])
		}
		last[key] = sample.FinishedBootNS
	}
	t.Logf("original cohort: topology=%s samples=%d max_gap=%s cut_to_ready=%s", topology, len(samples), time.Duration(maxGap), time.Duration(finished.Counters.ReadyBootNS-finished.Counters.CutBootNS))
}

func readBrokerCohort(t *testing.T, fixture *clusterharness.BrokerImageFixture, label string) brokerCohortReceipt {
	t.Helper()
	row, err := brokerCohortCheckpoint(fixture, label)
	if err != nil {
		t.Fatalf("cohort checkpoint %s: %v", label, err)
	}
	var receipt brokerCohortReceipt
	if row.Label != label || len(row.JSON) == 0 || len(row.JSON) > 4<<20 || row.Digest != fmt.Sprintf("%x", sha256.Sum256(row.JSON)) {
		t.Fatal("cohort receipt label/digest/size mismatch")
	}
	if err := json.Unmarshal(row.JSON, &receipt); err != nil {
		t.Fatal("cohort receipt JSON invalid")
	}
	if err := brokerCohortMetadata(receipt); err != nil {
		t.Fatal("cohort receipt lacks native/source/artifact evidence")
	}
	requireBrokerCohortProcess(t, receipt.PID, receipt.StartTicks, receipt.Counters.RunningSHA)
	requireBrokerCohortProcess(t, receipt.Counters.AgentPID, receipt.Counters.AgentTicks, receipt.Counters.AgentSHA)
	return receipt
}

func brokerCohortMetadata(receipt brokerCohortReceipt) error {
	if receipt.Claim == "" || receipt.TestOrArm == "" || receipt.Command == "" || receipt.NativeRC == nil || *receipt.NativeRC != 0 || receipt.Verdict != "GREEN" ||
		!brokerCohortSHA(receipt.SourceSHA, 40) || !brokerCohortSHA(receipt.ArtifactSHA, 64) || !brokerCohortSHA(receipt.PreviousSHA, 64) ||
		receipt.Counters.RunningSHA != receipt.ArtifactSHA || receipt.WallSeconds <= 0 || len(receipt.PhaseIntersections) == 0 || string(receipt.PhaseIntersections) == "null" ||
		receipt.MutationDigest == nil || !brokerCohortReceiptPath(receipt.StdoutPath) || !brokerCohortReceiptPath(receipt.StderrPath) ||
		!brokerCohortReceiptPath(receipt.OracleInputs.FlowLedger) || len(receipt.LogMapping) == 0 || string(receipt.LogMapping) == "null" {
		return errors.New("cohort receipt lacks complete native evidence and archive paths")
	}
	return nil
}

func brokerCohortReceiptPath(path string) bool {
	return path != "" && path != "." && !filepath.IsAbs(path) && filepath.Clean(path) == path && !strings.HasPrefix(path, "..")
}

func requireBrokerCohortProcess(t *testing.T, pid int, ticks uint64, expectedSHA string) {
	t.Helper()
	if pid <= 0 || ticks == 0 {
		t.Fatal("missing original process identity")
	}
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		t.Fatal("original process is not alive")
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		t.Fatal("invalid process stat")
	}
	fields := strings.Fields(string(data)[end+1:])
	if len(fields) < 20 || fields[0] == "Z" || fields[0] == "X" || fields[19] != strconv.FormatUint(ticks, 10) {
		t.Fatal("original process exited or its PID was recycled")
	}
	if expectedSHA != "" {
		file, err := os.Open(fmt.Sprintf("/proc/%d/exe", pid))
		if err != nil {
			t.Fatal(err)
		}
		hash := sha256.New()
		_, readErr := io.Copy(hash, file)
		closeErr := file.Close()
		if readErr != nil || closeErr != nil || fmt.Sprintf("%x", hash.Sum(nil)) != expectedSHA {
			t.Fatal("running artifact SHA differs from the original-process receipt")
		}
	}
}

func captureBrokerCohort(t *testing.T, receipt brokerCohortReceipt) map[string][2]brokerCohortSocket {
	t.Helper()
	result := make(map[string][2]brokerCohortSocket)
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", receipt.PID))
	if err != nil {
		t.Fatal("cannot inspect original broker listening OFDs")
	}
	for _, listener := range receipt.Counters.Listeners {
		found := false
		for _, entry := range entries {
			var stat syscall.Stat_t
			if syscall.Stat(fmt.Sprintf("/proc/%d/fd/%s", receipt.PID, entry.Name()), &stat) == nil &&
				uint64(stat.Dev) == listener.Device && stat.Ino == listener.Inode && stat.Mode&syscall.S_IFMT == syscall.S_IFSOCK {
				found = true
			}
		}
		if !found {
			t.Fatal("original broker LISTEN object is absent from the live kernel census")
		}
	}
	opening := receipt.Counters.PendingOpen
	for _, item := range []struct {
		pid, fd  int
		identity brokerCohortSocket
	}{{opening.AgentPID, opening.AgentFD, opening.AgentSocket}, {opening.ClientPID, opening.ClientFD, opening.ClientSocket}} {
		var stat syscall.Stat_t
		if syscall.Stat(fmt.Sprintf("/proc/%d/fd/%d", item.pid, item.fd), &stat) != nil || (brokerCohortSocket{Device: uint64(stat.Dev), Inode: stat.Ino, Mode: stat.Mode & syscall.S_IFMT}) != item.identity {
			t.Fatal("connecting OPEN original kernel socket is absent")
		}
	}
	for _, stream := range receipt.Counters.Streams {
		if _, duplicate := result[stream.ID]; duplicate || stream.ID == "" {
			t.Fatal("original cohort identity is absent or duplicated")
		}
		var sockets [2]brokerCohortSocket
		for index, fd := range [][2]int{{stream.PublicPID, stream.PublicFD}, {stream.TargetPID, stream.TargetFD}} {
			var stat syscall.Stat_t
			if fd[0] <= 0 || fd[1] < 3 || syscall.Stat(fmt.Sprintf("/proc/%d/fd/%d", fd[0], fd[1]), &stat) != nil || stat.Mode&syscall.S_IFMT != syscall.S_IFSOCK {
				t.Fatal("cohort original descriptor is not a live kernel socket")
			}
			sockets[index] = brokerCohortSocket{Device: uint64(stat.Dev), Inode: stat.Ino, Mode: stat.Mode & syscall.S_IFMT}
		}
		if stream.Protocol == "ssh" {
			requireBrokerCohortProcess(t, stream.AppPID, stream.AppTicks, stream.AppSHA)
		}
		result[stream.ID] = sockets
	}
	return result
}

func brokerCohortTransition(before, after brokerCohortReceipt) error {
	b, a := before.Counters, after.Counters
	if before.PID <= 0 || before.StartTicks == 0 || before.PID != after.PID || before.StartTicks != after.StartTicks || before.SourceSHA != after.SourceSHA ||
		a.Generation != b.Generation+1 || a.RunningSHA == b.RunningSHA || after.PreviousSHA != b.RunningSHA ||
		b.AgentPID <= 0 || b.AgentTicks == 0 || b.AgentPID != a.AgentPID || b.AgentTicks != a.AgentTicks || !brokerCohortSHA(b.AgentSHA, 64) || b.AgentSHA != a.AgentSHA ||
		a.CutBootNS <= 0 || a.ReadyBootNS <= a.CutBootNS || a.ReadyBootNS-a.CutBootNS > int64(upgradebudget.LiveFlowStall) {
		return errors.New("broker cohort did not cross a bounded same-PID exec with its original agent")
	}
	opening, opened := b.PendingOpen, a.PendingOpen
	if opening.Phase != "connecting" || opening.TargetAccepts != 0 || opening.AgentPID != b.AgentPID || opening.AgentFD < 3 || opening.ClientPID <= 0 || opening.ClientFD < 3 || opening.Opened <= 0 || opening.Started != 0 || opening.Finished != 0 || opening.PayloadSHA != "" ||
		opened.Phase != "connected" || opened.TargetAccepts != 1 || opened.AgentPID != opening.AgentPID || opened.AgentFD != opening.AgentFD || opened.ClientPID != opening.ClientPID || opened.ClientFD != opening.ClientFD || opened.Opened != opening.Opened || opened.Started < a.CutBootNS || opened.Started < opening.Opened ||
		opened.Finished < a.ReadyBootNS || opened.Finished-opened.Started > int64(upgradebudget.LiveFlowStall) || opened.PayloadSHA != fmt.Sprintf("%x", sha256.Sum256([]byte("tether-broker-original-connecting-open-v1"))) {
		return fmt.Errorf("connecting OPEN did not finish once on its original agent/client within the business deadline: before=%+v after=%+v freeze=%d ready=%d", opening, opened, a.CutBootNS, a.ReadyBootNS)
	}
	for _, pair := range [][2]brokerCohortSocket{{opening.AgentSocket, opened.AgentSocket}, {opening.ClientSocket, opened.ClientSocket}, {opening.BrokerSocket, opened.BrokerSocket}} {
		if pair[0].Inode == 0 || pair[0].Mode != syscall.S_IFSOCK || pair[0] != pair[1] {
			return errors.New("connecting OPEN changed its original bound/public socket")
		}
	}
	if len(b.Listeners) == 0 || !reflect.DeepEqual(b.Listeners, a.Listeners) {
		return errors.New("broker cut replaced an original listening OFD or Unix pathname")
	}
	for _, listener := range b.Listeners {
		if listener.ID == "" || listener.Inode == 0 || listener.Cookie == 0 || !listener.Nonblock || !listener.CloseOnExec {
			return errors.New("original listener evidence lacks keeper identity/flags")
		}
	}
	originals := make(map[string]brokerCohortStream)
	protocols := make(map[string]int)
	halfClosed := false
	for _, stream := range b.Streams {
		if stream.ID == "" || originals[stream.ID].ID != "" || stream.FinalSequence < 40 || stream.TargetAccepts != 1 {
			return errors.New("original stream census is empty, duplicated or already redialed")
		}
		if stream.HalfClosed && (stream.HalfCloseSequence < 20 || stream.HalfCloseSequence > stream.FinalSequence ||
			(stream.FinalSequence-20)*brokerCohortRecordBytes <= uint64(upgradebudget.StreamReplayBytes)) {
			return errors.New("half-close cohort has no declared reverse tail larger than one replay ring")
		}
		if stream.Protocol == "ssh" && !brokerCohortSHA(stream.AppSHA, 64) {
			return errors.New("SSH foreground running artifact identity is missing")
		}
		originals[stream.ID] = stream
		protocols[stream.Protocol]++
		halfClosed = halfClosed || stream.HalfClosed
	}
	if protocols["ssh"] < 1 || protocols["tcp"] < 4 || protocols["ss"] < 1 || !halfClosed || len(originals) != len(a.Streams) {
		return errors.New("SSH/TCP/SS/half-close cohort is incomplete")
	}
	seen := make(map[string]bool)
	for _, stream := range a.Streams {
		old, ok := originals[stream.ID]
		if !ok || seen[stream.ID] || old != stream || stream.SSAuthFailures != 0 || stream.Protocol == "ss" && stream.SSHeaders != 1 {
			return errors.New("original flow, target Accept, SSH child or SS AEAD header changed")
		}
		seen[stream.ID] = true
	}
	if a.ExpectedSlots == 0 || uint64(len(a.Connects)) != a.ExpectedSlots {
		return errors.New("new-connect sampler omitted scheduled slots")
	}
	intersections := 0
	for index, connect := range a.Connects {
		if connect.Slot != uint64(index) || connect.NativeRC == nil || *connect.NativeRC != 0 || connect.StartedBootNS <= 0 ||
			connect.FinishedBootNS < connect.StartedBootNS || connect.FinishedBootNS-connect.StartedBootNS > int64(upgradebudget.LiveFlowStall) {
			return errors.New("new connection failed, was skipped or exceeded the absolute business budget")
		}
		if connect.StartedBootNS <= a.ReadyBootNS && connect.FinishedBootNS >= a.CutBootNS {
			intersections++
		}
	}
	if intersections == 0 {
		return errors.New("no new connection intersected the real cut")
	}
	return nil
}

// Hash the declared synthetic application record independently of both the
// sender ledger and the transport codec. A repeated prefix, duplicate FIN or
// missing tail cannot pass by merely matching the final source byte count.
const brokerCohortRecordBytes = 16 << 10

func brokerCohortPayloadSHA(id string, direction uint8, sequence uint64) string {
	// The application format is fixed here, rather than derived from received
	// byte counts. Large reverse tails have to cross replay-ring boundaries.
	record := make([]byte, brokerCohortRecordBytes)
	prefix := fmt.Sprintf("tether-broker-cohort-v1/%s/%d/%d\n", id, direction, sequence)
	// Keep the oracle independent, but expand the formula's 256-byte period
	// in blocks so race instrumentation does not dominate the real-flow run.
	// origin: hot-update S6 plan §6.3 / T48
	var period [256]byte
	for i := range period {
		period[i] = byte(uint64(i)*31+sequence*17+uint64(direction)) ^ byte(len(id))
	}
	for offset := 0; offset < len(record); offset += len(period) {
		copy(record[offset:], period[:])
	}
	copy(record, prefix)
	return fmt.Sprintf("%x", sha256.Sum256(record))
}

func brokerCohortBytes(receipt brokerCohortReceipt, samples []brokerCohortSample) error {
	type cursor struct {
		sequence, pre, post uint64
		last                int64
		fin                 bool
	}
	streams := make(map[string]brokerCohortStream)
	for _, stream := range receipt.Counters.Streams {
		streams[stream.ID] = stream
	}
	states := make(map[string][2]cursor)
	for _, sample := range samples {
		stream, ok := streams[sample.ID]
		if !ok || sample.Direction > 1 {
			return errors.New("byte ledger contains a foreign stream or direction")
		}
		pair := states[sample.ID]
		state := pair[sample.Direction]
		end := stream.FinalSequence
		if stream.HalfClosed && sample.Direction == 0 {
			end = stream.HalfCloseSequence
		}
		if state.fin || sample.Sequence != state.sequence || sample.StartedBootNS <= 0 || sample.FinishedBootNS < sample.StartedBootNS ||
			sample.FinishedBootNS-sample.StartedBootNS > int64(upgradebudget.LiveFlowStall) || state.last != 0 && sample.FinishedBootNS-state.last > int64(upgradebudget.LiveFlowStall) ||
			state.last != 0 && sample.FinishedBootNS < state.last {
			return fmt.Errorf("byte ledger is duplicate, reordered, missing FIN tail or stalled: id=%s direction=%d sequence=%d expected=%d fin=%v duration=%s gap=%s", sample.ID, sample.Direction, sample.Sequence, state.sequence, state.fin, time.Duration(sample.FinishedBootNS-sample.StartedBootNS), time.Duration(sample.FinishedBootNS-state.last))
		}
		if sample.EOF {
			if sample.Sequence != end || sample.PayloadSHA != "" ||
				stream.HalfClosed && sample.Direction == 0 && sample.FinishedBootNS >= receipt.Counters.CutBootNS ||
				stream.HalfClosed && sample.Direction == 1 && sample.StartedBootNS <= receipt.Counters.ReadyBootNS {
				return errors.New("FIN does not match the exact declared stream tail")
			}
			state.fin = true
		} else {
			if sample.Sequence >= end || sample.PayloadSHA != brokerCohortPayloadSHA(sample.ID, sample.Direction, sample.Sequence) {
				return errors.New("application bytes differ from the independent sequence/hash oracle")
			}
			state.sequence++
			if sample.FinishedBootNS < receipt.Counters.CutBootNS {
				state.pre++
			}
			if sample.StartedBootNS > receipt.Counters.ReadyBootNS {
				state.post++
			}
		}
		state.last = sample.FinishedBootNS
		pair[sample.Direction] = state
		states[sample.ID] = pair
	}
	for _, stream := range receipt.Counters.Streams {
		for direction, state := range states[stream.ID] {
			end, requiredPost := stream.FinalSequence, uint64(20)
			if stream.HalfClosed && direction == 0 {
				end, requiredPost = stream.HalfCloseSequence, 0
			}
			if !state.fin || state.sequence != end || state.pre < 20 || state.post < requiredPost {
				return fmt.Errorf("cohort lacks twenty exact pre/post-cut samples or final EOF in either direction: stream=%s direction=%d sequence=%d end=%d fin=%t pre=%d post=%d freeze=%d ready=%d", stream.ID, direction, state.sequence, end, state.fin, state.pre, state.post, receipt.Counters.CutBootNS, receipt.Counters.ReadyBootNS)
			}
		}
	}
	return nil
}

func brokerCohortSHA(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, c := range value {
		if !strings.ContainsRune("0123456789abcdef", c) {
			return false
		}
	}
	return true
}

// Oracle controls do not simulate a successful broker. They establish that the
// consumer rejects precisely the app-redial/header-reset/listener-close faults
// while the real T48 remains pending the unique P26 process fixture.
// origin: hot-update S6 plan §11 T48
func TestBrokerFlowCohortOracleControls(t *testing.T) {
	before, after, samples := brokerCohortOracleInputs()
	if err := brokerCohortMetadata(after); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*brokerCohortReceipt){
		func(r *brokerCohortReceipt) { r.NativeRC = nil },
		func(r *brokerCohortReceipt) { r.WallSeconds = 0 },
		func(r *brokerCohortReceipt) { r.PhaseIntersections = nil },
		func(r *brokerCohortReceipt) { r.OracleInputs.FlowLedger = "../outside.jsonl" },
		func(r *brokerCohortReceipt) { r.MutationDigest = nil },
		func(r *brokerCohortReceipt) { r.LogMapping = nil },
	} {
		changed := after
		mutate(&changed)
		if brokerCohortMetadata(changed) == nil {
			t.Fatal("cohort metadata accepted missing native evidence or archive path escape")
		}
	}
	if err := brokerCohortTransition(before, after); err != nil {
		t.Fatal(err)
	}
	if err := brokerCohortBytes(after, samples); err != nil {
		t.Fatal(err)
	}
	shortBefore, shortAfter := before, after
	shortBefore.Counters.Streams = append([]brokerCohortStream(nil), before.Counters.Streams...)
	shortAfter.Counters.Streams = append([]brokerCohortStream(nil), after.Counters.Streams...)
	shortBefore.Counters.Streams[1].FinalSequence, shortAfter.Counters.Streams[1].FinalSequence = 40, 40
	if brokerCohortTransition(shortBefore, shortAfter) == nil {
		t.Fatal("half-close oracle accepted a reverse tail shorter than one replay ring")
	}
	for _, tc := range []struct {
		name   string
		mutate func(*brokerCohortReceipt)
	}{
		{"opening-already-connected", func(r *brokerCohortReceipt) { r.Counters.PendingOpen.TargetAccepts = 2 }},
		{"opening-new-bound-socket", func(r *brokerCohortReceipt) { r.Counters.PendingOpen.AgentSocket.Inode++ }},
		{"opening-missing-result", func(r *brokerCohortReceipt) { r.Counters.PendingOpen.PayloadSHA = "" }},
		{"app-redial", func(r *brokerCohortReceipt) { r.Counters.Streams[0].TargetAccepts++ }},
		{"new-aead-header", func(r *brokerCohortReceipt) { r.Counters.Streams[5].SSHeaders++ }},
		{"ss-decryption-failure", func(r *brokerCohortReceipt) { r.Counters.Streams[5].SSAuthFailures++ }},
		{"listener-close", func(r *brokerCohortReceipt) { rc := 1; r.Counters.Connects[0].NativeRC = &rc }},
		{"listener-rebind", func(r *brokerCohortReceipt) { r.Counters.Listeners[0].Inode++ }},
		{"connect-slot-missing", func(r *brokerCohortReceipt) { r.Counters.Connects = nil }},
		{"connect-misses-cut", func(r *brokerCohortReceipt) {
			r.Counters.Connects[0].StartedBootNS = r.Counters.ReadyBootNS + 1
			r.Counters.Connects[0].FinishedBootNS = r.Counters.ReadyBootNS + 2
		}},
		{"new-agent", func(r *brokerCohortReceipt) { r.Counters.AgentTicks++ }},
		{"new-ssh-child", func(r *brokerCohortReceipt) { r.Counters.Streams[0].AppTicks++ }},
		{"missing-ssh-artifact", func(r *brokerCohortReceipt) { r.Counters.Streams[0].AppSHA = "" }},
		{"new-broker-pid", func(r *brokerCohortReceipt) { r.PID++ }},
		{"generation-reset", func(r *brokerCohortReceipt) { r.Counters.Generation = 1 }},
		{"same-artifact", func(r *brokerCohortReceipt) { r.Counters.RunningSHA = before.Counters.RunningSHA }},
		{"stall-over-budget", func(r *brokerCohortReceipt) {
			r.Counters.ReadyBootNS = r.Counters.CutBootNS + int64(upgradebudget.LiveFlowStall) + 1
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(after)
			if err != nil {
				t.Fatal(err)
			}
			var changed brokerCohortReceipt
			if err := json.Unmarshal(data, &changed); err != nil {
				t.Fatal(err)
			}
			tc.mutate(&changed)
			if brokerCohortTransition(before, changed) == nil {
				t.Fatal("flow cohort oracle accepted the injected defect")
			}
		})
	}
	for _, tc := range []struct {
		name   string
		mutate func([]brokerCohortSample) []brokerCohortSample
	}{
		{"duplicate-prefix", func(s []brokerCohortSample) []brokerCohortSample { s[1] = s[0]; return s }},
		{"wrong-bytes", func(s []brokerCohortSample) []brokerCohortSample { s[0].PayloadSHA = strings.Repeat("0", 64); return s }},
		{"missing-fin-tail", func(s []brokerCohortSample) []brokerCohortSample { return s[:len(s)-1] }},
		{"early-fin", func(s []brokerCohortSample) []brokerCohortSample { s[0].EOF = true; s[0].PayloadSHA = ""; return s }},
		{"half-close-only-a-flag", func(s []brokerCohortSample) []brokerCohortSample {
			for i := range s {
				if s[i].ID == "stream-1" && s[i].Direction == 0 && s[i].EOF {
					s[i].StartedBootNS, s[i].FinishedBootNS = after.Counters.ReadyBootNS+1, after.Counters.ReadyBootNS+2
				}
			}
			return s
		}},
		{"no-post-cut-traffic", func(s []brokerCohortSample) []brokerCohortSample {
			for i := range s {
				s[i].StartedBootNS = 1
				s[i].FinishedBootNS = 2
			}
			return s
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := tc.mutate(append([]brokerCohortSample(nil), samples...))
			if brokerCohortBytes(after, changed) == nil {
				t.Fatal("independent byte/FIN oracle accepted the injected defect")
			}
		})
	}
}
