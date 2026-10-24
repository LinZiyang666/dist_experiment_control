//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/clusterharness"
)

// Repeated real-process Run and rollback cuts have independent tagged matrix
// units. Their cumulative fixture work must not exhaust broker unit-test time.
// origin: hot-update S6 plan §9.3 / T01 / T49 / TESTFIX
// P26's factory binds the real serve process; ownership and observations remain
// in the fixture so this consumer cannot synthesize a successful cut.
var brokerRunFixtureFactory clusterharness.StartBrokerImageFixtureFunc = clusterharness.StartBrokerRunFixture
var brokerRunFixtureCheckpoint clusterharness.BrokerImageCheckpointFunc = (*clusterharness.BrokerImageFixture).Checkpoint
var brokerRunFixtureClose clusterharness.BrokerImageCloseFunc = (*clusterharness.BrokerImageFixture).Close

// origin: hot-update S6 plan §3.2 / §4 / T01
func TestBrokerExecStaysInsideRun(t *testing.T) {
	if brokerRunFixtureFactory == nil || brokerRunFixtureCheckpoint == nil || brokerRunFixtureClose == nil {
		t.Fatal("NOT_RUN pending P01-P04/P07-P12/P22/P26: real broker fixture is not installed")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, topology := range []string{"single", "sole-voter"} {
			t.Run(fmt.Sprintf("%s/round-%d", topology, round), func(t *testing.T) { exerciseBrokerExecInsideRun(t, topology) })
		}
	}
	testharness.AssertNoGoroutineLeak(t, "real broker Run exec", g)
	testharness.AssertNoFDLeak(t, "real broker Run exec", fd, 0)
}

func exerciseBrokerExecInsideRun(t *testing.T, topology string) {
	t.Helper()
	fixture, err := brokerRunFixtureFactory(t, clusterharness.BrokerImageFixtureConfig{Topology: topology})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := brokerRunFixtureClose(fixture); err != nil {
			t.Error(err)
		}
	}()
	type receipt struct {
		PID                            int               `json:"pid"`
		StartTicks                     uint64            `json:"startticks"`
		NodeCloseSeq                   uint64            `json:"node_close_seq"`
		ExecSeq                        uint64            `json:"exec_seq"`
		OldDeferCount                  uint64            `json:"old_defer_count"`
		RunningSHA                     string            `json:"running_sha256"`
		Generation                     uint64            `json:"generation"`
		StoreCloseSeq                  uint64            `json:"store_close_seq"`
		ACKDigest                      string            `json:"ack_prefix_digest"`
		ACKCount                       uint64            `json:"ack_count"`
		LockProbes                     uint64            `json:"closed_store_lock_probes"`
		LockAcquired                   uint64            `json:"closed_store_lock_acquired"`
		ListenerIdentities             map[string]string `json:"listener_identities"`
		FreshConnects                  uint64            `json:"fresh_connects"`
		RefusedConnects                uint64            `json:"refused_connects"`
		FreshReplies                   uint64            `json:"fresh_replies"`
		MaxFreshReplyNS                int64             `json:"max_fresh_reply_ns"`
		WriteStallNS                   int64             `json:"write_stall_ns"`
		ExportRevokeAttempted          bool              `json:"export_revoke_attempted"`
		ExportRevokeRefusedAfterImport bool              `json:"export_revoke_refused_after_import"`
	}
	read := func(label string) receipt {
		t.Helper()
		row, err := brokerRunFixtureCheckpoint(fixture, label)
		if err != nil {
			t.Fatal(err)
		}
		if row.Label != label || len(row.JSON) == 0 || row.Digest == "" {
			t.Fatalf("missing fixture checkpoint %s", label)
		}
		var value receipt
		if err := json.Unmarshal(row.JSON, &value); err != nil {
			t.Fatal(err)
		}
		if value.PID <= 0 || value.StartTicks == 0 || value.RunningSHA == "" || value.Generation == 0 {
			t.Fatalf("incomplete %s receipt: %+v", label, value)
		}
		return value
	}
	before := read("before-reload")
	after := read("reload-control-ready")
	if before.PID != after.PID || before.StartTicks != after.StartTicks {
		t.Fatal("reload replaced the process")
	}
	if before.RunningSHA == after.RunningSHA || after.Generation != before.Generation+1 {
		t.Fatal("candidate image did not replace the running artifact")
	}
	if after.StoreCloseSeq == 0 || after.ExecSeq <= after.StoreCloseSeq {
		t.Fatal("exec preceded actual store close")
	}
	if topology == "sole-voter" && (after.NodeCloseSeq == 0 || after.ExecSeq <= after.NodeCloseSeq) {
		t.Fatal("exec preceded actual Node shutdown inside Run")
	}
	if after.OldDeferCount != 0 {
		t.Fatalf("old Run defers executed before exec: %d", after.OldDeferCount)
	}
	if before.ACKCount == 0 || before.ACKDigest == "" || after.ACKCount < before.ACKCount || after.ACKDigest != before.ACKDigest {
		t.Fatal("successful pre-cut write prefix was not readable after import")
	}
	if after.LockProbes == 0 || after.LockAcquired != 0 {
		t.Fatal("original flock was not held at the closed-store barrier")
	}
	if len(before.ListenerIdentities) == 0 || !reflect.DeepEqual(before.ListenerIdentities, after.ListenerIdentities) {
		t.Fatal("reload did not preserve original listener identities")
	}
	if after.FreshConnects == 0 || after.RefusedConnects != 0 || after.FreshReplies != after.FreshConnects || after.MaxFreshReplyNS <= 0 || after.MaxFreshReplyNS > int64(5*time.Second) {
		t.Fatal("fresh-connect cohort lacked bounded business replies without refused connects")
	}
	if after.WriteStallNS <= 0 || after.WriteStallNS > int64(5*time.Second) {
		t.Fatalf("real write stall exceeded the product budget or was not measured: %s", time.Duration(after.WriteStallNS))
	}
	if !after.ExportRevokeAttempted || !after.ExportRevokeRefusedAfterImport {
		t.Fatalf("CloseProxy during export was lost after import: attempted=%t refused=%t", after.ExportRevokeAttempted, after.ExportRevokeRefusedAfterImport)
	}
}

// origin: hot-update S6 plan §4.2 / T49
func TestBrokerActiveImportFailureUsesCurrentCut(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerActiveImportFailure(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker ACTIVE refusal current cut", g)
	testharness.AssertNoFDLeak(t, "broker ACTIVE refusal current cut", fd, 0)
}
func exerciseBrokerActiveImportFailure(t *testing.T) {
	t.Helper()
	for _, topology := range []string{"single", "sole-voter"} {
		t.Run(topology, func(t *testing.T) {
			f, err := clusterharness.StartBrokerRunFixture(t, clusterharness.BrokerImageFixtureConfig{Topology: topology})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := f.Close(); err != nil {
					t.Error(err)
				}
			}()
			read := func(label string) map[string]json.RawMessage {
				row, err := f.Checkpoint(label)
				if err != nil {
					t.Fatal(err)
				}
				var value map[string]json.RawMessage
				if err = json.Unmarshal(row.JSON, &value); err != nil {
					t.Fatal(err)
				}
				return value
			}
			before := read("before-reload")
			if err = f.ArmFault(clusterharness.BrokerImageFault{Point: "active-after-new-ack-and-bytes", Ordinal: 1, Once: true}); err != nil {
				t.Fatal(err)
			}
			after := read("reload-active-refusal")
			for _, field := range []string{"pid", "startticks", "running_sha256", "ack_prefix_digest", "listener_identities"} {
				if !bytes.Equal(before[field], after[field]) {
					t.Fatalf("current rollback changed %s", field)
				}
			}
			var generation uint64
			if err = json.Unmarshal(after["generation"], &generation); err != nil || generation != 3 {
				t.Fatal("current rollback did not create a new generation")
			}
			var durable bool
			if err = json.Unmarshal(after["candidate_ack_before_replay"], &durable); err != nil || !durable {
				t.Fatal("candidate ACK missing before previous-image replay")
			}
		})
	}
}

// An admissible pure reader at .prev is still not the reader authorized by the
// incoming cut. A third SHA installed after ACTIVE must be refused before any
// new cut/exec, even though its ABI, domains, schema and config remain valid.
// origin: hot-update S6 plan §3.1 / §4.2 / T49
func TestBrokerRollbackReaderRemainsBoundToIncomingProof(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseBrokerRollbackReaderBinding(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "rollback previous reader proof", g)
	testharness.AssertNoFDLeak(t, "rollback previous reader proof", fd, 0)
}
func exerciseBrokerRollbackReaderBinding(t *testing.T) {
	t.Helper()
	sandbox := filepath.Dir(t.TempDir())
	testharness.GuardProcessDirectory(t, sandbox)
	fixture, err := clusterharness.StartBrokerLifecycleFixture(t, clusterharness.BrokerImageFixtureConfig{Topology: "single"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	}()
	before, err := fixture.Checkpoint("before-reload")
	if err != nil {
		t.Fatal(err)
	}
	var original struct {
		Counters struct {
			Store storage.StoreStamp `json:"store"`
		} `json:"counters"`
	}
	if err = json.Unmarshal(before.JSON, &original); err != nil {
		t.Fatal(err)
	}
	dir := original.Counters.Store.Identity.DataDir
	relative, err := filepath.Rel(sandbox, dir)
	if err != nil || relative == "." || strings.HasPrefix(relative, "..") {
		t.Fatal("reader mutation is outside the private product fixture")
	}
	if err = fixture.ArmFault(clusterharness.BrokerImageFault{Point: "active-after-new-ack-and-bytes", Ordinal: 1, Once: true}); err != nil {
		t.Fatal(err)
	}
	active, err := fixture.Checkpoint("active-new-ack-and-bytes")
	if err != nil {
		t.Fatal(err)
	}
	var identity struct {
		PID      int    `json:"pid"`
		Artifact string `json:"artifact_sha"`
		Counters struct {
			Generation uint64 `json:"generation"`
		} `json:"counters"`
	}
	if err = json.Unmarshal(active.JSON, &identity); err != nil {
		t.Fatal(err)
	}
	previous, err := os.OpenFile(filepath.Join(dir, "broker-product.prev"), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := previous.WriteString("\nthird admissible rollback reader\n")
	if err = errors.Join(writeErr, previous.Close()); err != nil {
		t.Fatal(err)
	}
	if _, err = fixture.Checkpoint("active-rollback-control-ready"); err == nil {
		t.Fatal("rollback executed a previous reader not authorized by the incoming proof")
	}
	data, err := os.ReadFile(filepath.Join(dir, "runtime-stopped.json"))
	if err != nil {
		t.Fatal("rollback refusal did not finish current terminal cleanup", err)
	}
	var stopped struct {
		Generation uint64 `json:"generation"`
		Error      string `json:"error"`
	}
	if err = json.Unmarshal(data, &stopped); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stopped.Error, "preflight cut") || !strings.Contains(stopped.Error, brokerimage.ErrImage.Error()) {
		t.Fatal("rollback was refused for another cause", stopped.Error)
	}
	actual, err := os.ReadFile(fmt.Sprintf("/proc/%d/exe", identity.PID))
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(actual)) != identity.Artifact {
		t.Fatal("rollback crossed into the unproven previous image")
	}
}
