package clusterharness

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func readStoreFixtureEvidence(t *testing.T, f *BrokerImageFixture, label string) BrokerStoreEvidence {
	t.Helper()
	row, err := f.Checkpoint(label)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(row.JSON)
	if row.Label != label || row.Digest != hex.EncodeToString(sum[:]) {
		t.Fatal("checkpoint does not bind its label and exact JSON bytes")
	}
	var evidence BrokerStoreEvidence
	if err := json.Unmarshal(row.JSON, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.Claim != label || evidence.PID <= 0 || evidence.StartTicks == 0 || evidence.RunningSHA == "" || evidence.Generation == 0 || evidence.SourceSHA == "" || evidence.SourceDigest == "" {
		t.Fatalf("checkpoint lacks process/artifact/source identity: %+v", evidence)
	}
	if strings.Contains(string(row.JSON), "PRIVATE KEY") || strings.Contains(string(row.JSON), "secret") {
		t.Fatal("receipt contains private material")
	}
	return evidence
}

// origin: hot-update S6 plan §2.1 / §12.1 P26
func TestBrokerStoreFixtureRejectsInvalidCustody(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker store fixture requires Linux")
	}
	for _, cfg := range []BrokerImageFixtureConfig{{}, {Topology: "N1"}, {Topology: "single", CandidatePath: "tether"}, {Topology: "single", PreviousPath: "/home/weiland/.local/bin/tether"}} {
		f, err := StartBrokerImageFixture(t, cfg)
		if err == nil {
			if f != nil {
				_ = f.Close()
			}
			t.Fatalf("invalid fixture custody accepted: %+v", cfg)
		}
	}
	var empty BrokerImageFixture
	if _, err := empty.Checkpoint("before-reload"); err == nil {
		t.Fatal("missing fixture returned a receipt")
	}
	if err := empty.ArmFault(BrokerImageFault{}); err == nil {
		t.Fatal("missing fixture armed a fault")
	}
	if err := empty.Close(); err != nil {
		t.Fatal(err)
	}
}

// A component process cannot sign product Run/auth readiness, and unknown
// labels/faults fail rather than becoming fabricated all-green receipts.
// origin: hot-update S6 plan §12.1 P26 / §14
func TestBrokerStoreFixtureRejectsUnobservedClaims(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker store fixture requires Linux")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStoreFixtureUnobservedClaims(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker unobserved fixture claims", g)
	testharness.AssertNoFDLeak(t, "broker unobserved fixture claims", fd, 0)
}
func exerciseStoreFixtureUnobservedClaims(t *testing.T) {
	t.Helper()
	f, err := StartBrokerImageFixture(t, BrokerImageFixtureConfig{Topology: "single"})
	if err != nil {
		t.Fatal(err)
	}
	readStoreFixtureEvidence(t, f, "before-reload")
	for _, label := range []string{"unknown", "node/missing", "reload-control-ready"} {
		if _, err := f.Checkpoint(label); err == nil {
			t.Fatalf("unobserved claim %q accepted", label)
		}
	}
	for _, fault := range []BrokerImageFault{{Point: "unknown", Ordinal: 1, Once: true}, {Point: "restore-old-db"}, {Point: "after-take", Ordinal: 1, Once: true}} {
		if err := f.ArmFault(fault); err == nil {
			t.Fatalf("unbound fault accepted: %+v", fault)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Checkpoint("before-reload"); err == nil {
		t.Fatal("closed fixture returned a live receipt")
	}
}

// origin: hot-update S6 plan §3.3 / T13 / T55Fixture
func TestBrokerStoreFixtureLeakCycles(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker store fixture requires Linux")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStoreExecRollback(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker store fixture rollback", g)
	testharness.AssertNoFDLeak(t, "broker store fixture rollback", fd, 0)
}

func exerciseStoreExecRollback(t *testing.T) {
	t.Helper()
	f, err := StartBrokerImageFixture(t, BrokerImageFixtureConfig{Topology: "single"})
	if err != nil {
		t.Fatal(err)
	}
	before := readStoreFixtureEvidence(t, f, "write/leak-cycle-original")
	after := readStoreFixtureEvidence(t, f, "component-exec-error")
	if before.PID != after.PID || before.StartTicks != after.StartTicks || !reflect.DeepEqual(after.BeforeReplay, before.ACKs) || after.Drivers != [2]uint32{1, 0} {
		t.Fatal("returned exec did not reopen the current store with one real writer")
	}
	readStoreFixtureEvidence(t, f, "write/leak-cycle-current")
	stopped := readStoreFixtureEvidence(t, f, "normal-stop")
	if stopped.Drivers != [2]uint32{} || stopped.Counters["sqlite_fds"] != 0 || stopped.Counters["handoff_fds"] != 0 || stopped.Counters["sealed_custody_fds"] != 0 {
		t.Fatal("normal stop leaked current drivers or inherited files in the live child")
	}
	owner := f.api.(*brokerStoreFixture)
	lock, err := cluster.AcquireImageDataDirLock(owner.processes[owner.selected].config.Root)
	if err != nil {
		t.Fatal("normal stop did not release the original flock", err)
	}
	if err := lock.Release(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(owner.root, "receipts.jsonl")); err != nil {
		t.Fatal("fixture did not archive its exact receipts", err)
	}
}

// NewForImage receives a nonnil real writer plus an injected constructor
// error. Its actual cleanup must join before the original lock is released.
// origin: hot-update S6 plan §3.2 / T55Fixture
func TestBrokerStoreFixtureConstructorFailureRetainsLock(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker store fixture requires Linux")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStoreConstructorRefusal(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker fixture constructor refusal", g)
	testharness.AssertNoFDLeak(t, "broker fixture constructor refusal", fd, 0)
}
func exerciseStoreConstructorRefusal(t *testing.T) {
	t.Helper()
	f, err := StartBrokerImageFixture(t, BrokerImageFixtureConfig{Topology: "sole-voter"})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.ArmFault(BrokerImageFault{Point: "constructor-error", Ordinal: 1, Once: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Checkpoint("component-exec-error"); err == nil || !strings.Contains(err.Error(), "constructor failure") {
		t.Fatalf("constructor fault did not execute: %v", err)
	}
	owner := f.api.(*brokerStoreFixture)
	lock, err := cluster.AcquireImageDataDirLock(owner.processes[0].config.Root)
	if lock != nil {
		_ = lock.Release()
		t.Fatal("constructor refusal unlocked the live owner")
	}
	if !errors.Is(err, cluster.ErrDataDirLockHeld) {
		t.Fatal(err)
	}
	stopped := readStoreFixtureEvidence(t, f, "normal-stop")
	if stopped.Drivers != [2]uint32{} || stopped.Seal.Writer != storage.CloseClosed {
		t.Fatal("partial constructor did not retain a real closed receipt")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}
