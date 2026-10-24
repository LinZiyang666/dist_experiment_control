//go:build linux

package hotupdate_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/clusterharness"
)

// Keep real Bolt held while the real candidate performs the pure probe. A
// second opener would hang, as the observed-entry watchdog control proves.
// origin: hot-update S6 plan §3.1 / T06
func TestBrokerPreflightNeverOpensStore(t *testing.T) {
	artifact := buildHotUpdateArtifact(t, "s6-store-preflight")
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerStorePreflight(t, artifact.Path)
	}
	testharness.AssertNoGoroutineLeak(t, "broker preflight process/store", g)
	testharness.AssertNoFDLeak(t, "broker preflight process/store", fd, 0)
}
func exerciseBrokerStorePreflight(t *testing.T, artifact string) {
	t.Helper()
	f, err := clusterharness.StartBrokerImageFixture(t, clusterharness.BrokerImageFixtureConfig{Topology: "sole-voter", CandidatePath: artifact, PreviousPath: artifact})
	if err != nil {
		t.Fatal(err)
	}
	before := brokerStoreACK(t, f, "preflight-original")
	dir, err := clusterharness.BrokerImageDataDirectory(f, before.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	brokerProbeBolt(t, dir, true)
	probe := brokerStoreCheckpoint(t, f, "preflight")
	if probe.OracleInputs["candidate_proof"] == "" || probe.OracleInputs["candidate_proof"] != probe.OracleInputs["previous_proof"] || probe.PID != before.PID || probe.Drivers != before.Drivers || !reflect.DeepEqual(probe.ACKs, before.ACKs) {
		t.Fatal("preflight changed live ownership or failed to bind both actual readers")
	}
	// A valid candidate cannot substitute for the supplied previous reader.
	// Change only its private custody copy, leaving the running owner untouched.
	previous := filepath.Join(filepath.Dir(dir), "product-previous")
	if err := os.WriteFile(previous, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Checkpoint("preflight"); err == nil {
		t.Fatal("preflight ignored an unreadable supplied previous artifact")
	}
	// The pure-probe control needs terminal owner release, independently of
	// hot-cut hook availability. Actual zero drivers/FDs precede the Bolt open.
	closed := brokerStoreCheckpoint(t, f, "normal-stop")
	if closed.Drivers != [2]uint32{} || closed.Counters["sqlite_fds"] != 0 {
		t.Fatal("ordinary owner stop did not close its actual store connections")
	}
	brokerProbeBolt(t, dir, false)
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// Independent driver and procfs observations accompany the producer receipts;
// a successful Shutdown alone is never accepted as an all-closed oracle.
// origin: hot-update S6 plan §3.2 / T09
func TestBrokerShutdownReceiptOrder(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerShutdownOrder(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker real shutdown receipts", g)
	testharness.AssertNoFDLeak(t, "broker real shutdown receipts", fd, 0)
}
func exerciseBrokerShutdownOrder(t *testing.T) {
	t.Helper()
	for _, topology := range []string{"single", "sole-voter"} {
		f := startBrokerStoreFixture(t, topology)
		before := brokerStoreACK(t, f, "shutdown-original")
		if before.Drivers[0] != 1 || topology != "single" && before.Drivers[1] == 0 {
			t.Fatal("fixture did not exercise real writer and RO driver handles")
		}
		closed := brokerStoreCheckpoint(t, f, "store-closed")
		brokerStoreAssertClosed(t, closed)
		if closed.Store.Identity != before.Store.Identity || closed.Store.AppliedIndex < before.Store.AppliedIndex {
			t.Fatal("closed receipt lost the final current SQL identity/floor")
		}
		var fence, join, closeSeq, seal uint64
		for _, event := range closed.Events {
			if event.BootNS <= 0 {
				t.Fatal("phase receipt lacks a monotonic observation")
			}
			switch event.Name {
			case "producers-fenced":
				fence = event.Seq
			case "snapshot-and-transport-joined":
				join = event.Seq
			case "node-store-closed":
				closeSeq = event.Seq
			case "seal-observed":
				seal = event.Seq
			case "prepare", "exec-enter":
				t.Fatal("prepare/exec preceded the closed-store barrier")
			}
		}
		if fence == 0 || closeSeq <= fence || seal <= closeSeq || topology != "single" && (join <= fence || closeSeq <= join) {
			t.Fatal("producer join/store close/final seal ordering was not observed")
		}
		if topology != "single" && !closed.Seal.Role.ObserverClosed {
			t.Fatal("Raft role observer was not actually joined")
		}
		// Consume the real P01 final-seal gate as well as P03/P04's receipt.
		// A disagreement between the two implementations is a product seam red.
		exported := brokerStoreCheckpoint(t, f, "final-export")
		if len(exported.Events) <= len(closed.Events) || exported.Events[len(exported.Events)-1].Name != "final-export" {
			t.Fatal("final export was not observed after all actual closures")
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Park deterministically after real store closure and finish at least one new
// contender attempt before allowing exec. Continuous polling covers both sides.
// origin: hot-update S6 plan §3.1 / T12
func TestBrokerLockHeldAcrossExec(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerStoreLockExec(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker original flock through exec", g)
	testharness.AssertNoFDLeak(t, "broker original flock through exec", fd, 0)
}
func exerciseBrokerStoreLockExec(t *testing.T) {
	t.Helper()
	for _, topology := range []string{"single", "sole-voter"} {
		f := startBrokerStoreFixture(t, topology)
		before := brokerStoreACK(t, f, "lock-original")
		dir, err := clusterharness.BrokerImageDataDirectory(f, before.NodeID)
		if err != nil {
			t.Fatal(err)
		}
		probe := startBrokerLockProbe(t, dir)
		brokerStoreAssertClosed(t, brokerStoreCheckpoint(t, f, "store-closed"))
		baseline, _, _ := probe.result()
		deadline := time.Now().Add(5 * time.Second)
		for {
			attempts, _, _ := probe.result()
			if attempts > baseline {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("contender did not probe the closed-store/pre-exec barrier")
			}
			time.Sleep(time.Millisecond)
		}
		if _, acquired, err := probe.result(); acquired != 0 || err != nil {
			probe.close()
			t.Fatalf("contender actually acquired the lock at the closed-store/pre-exec barrier: acquired=%d error=%v", acquired, err)
		}
		after := brokerStoreCheckpoint(t, f, "component-exec")
		brokerStoreAssertCurrent(t, before, after)
		probe.close()
		attempts, acquired, err := probe.result()
		if err != nil || attempts == 0 || acquired != 0 {
			t.Fatalf("original lock was lost across the deterministic cut: attempts=%d acquired=%d error=%v", attempts, acquired, err)
		}
		brokerStoreCheckpoint(t, f, "normal-stop")
		lock, err := cluster.AcquireImageDataDirLock(dir)
		if err != nil {
			t.Fatal("normal-stop positive control could not acquire lock", err)
		}
		if err := lock.Release(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// Prefix membership is supplied by the ACK client, not generated by replay.
// Every topology reads it inside the current opener before Raft construction.
// origin: hot-update S6 plan §3.3 / T13
func TestBrokerACKBeforeReplay(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerACKBeforeReplay(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker pre-replay durable ACK prefix", g)
	testharness.AssertNoFDLeak(t, "broker pre-replay durable ACK prefix", fd, 0)
}
func exerciseBrokerACKBeforeReplay(t *testing.T) {
	t.Helper()
	for _, topology := range []string{"single", "sole-voter", "quorum"} {
		f := startBrokerStoreFixture(t, topology)
		var before clusterharness.BrokerStoreEvidence
		for i := 0; i < 3; i++ {
			before = brokerStoreACK(t, f, fmt.Sprintf("durable-original-%d", i))
		}
		if topology == "quorum" {
			leader := before.NodeID
			for _, id := range []string{"node-0", "node-1", "node-2"} {
				if id != leader {
					brokerStoreCheckpoint(t, f, "node/"+id)
					break
				}
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				follower := brokerStoreCheckpoint(t, f, "before-reload")
				if reflect.DeepEqual(follower.ACKs, before.ACKs) {
					before = follower
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("follower did not durably apply the external ACK prefix")
				}
				time.Sleep(10 * time.Millisecond)
			}
			if before.Leader {
				t.Fatal("quorum cut requires an actual follower")
			}
		}
		after := brokerStoreCheckpoint(t, f, "component-exec")
		brokerStoreAssertCurrent(t, before, after)
		if after.Store.AppliedIndex < before.Store.AppliedIndex || !reflect.DeepEqual(after.Effects, before.Effects) {
			t.Fatal("replay changed the original exactly-once effects or regressed its command floor")
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// An extra real barrier advances Raft's all-entry domain without changing SQL's
// command cursor. Request retries after exec retain their durable dedup identity.
// origin: hot-update S6 plan §3.3 / T14
func TestBrokerAppliedIndexMonotonic(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerAppliedIndex(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker command cursor versus Raft entries", g)
	testharness.AssertNoFDLeak(t, "broker command cursor versus Raft entries", fd, 0)
}
func exerciseBrokerAppliedIndex(t *testing.T) {
	t.Helper()
	f := startBrokerStoreFixture(t, "sole-voter")
	before := brokerStoreACK(t, f, "stable-original")
	barrier := brokerStoreCheckpoint(t, f, "barrier")
	if barrier.Store.AppliedIndex != before.Store.AppliedIndex {
		t.Fatal("Raft barrier was confused with a SQL command")
	}
	closed := brokerStoreCheckpoint(t, f, "store-closed")
	if closed.Store.RaftLastIndex <= closed.Store.AppliedIndex || closed.Store.RaftConfigIndex == 0 || closed.Store.RaftConfigDigest == "" {
		t.Fatal("fixture did not exercise separate configuration/barrier and SQL index domains")
	}
	after := brokerStoreCheckpoint(t, f, "component-exec")
	brokerStoreAssertCurrent(t, before, after)
	retry := brokerStoreCheckpoint(t, f, "retry/stable-original")
	if retry.Effects["stable-original"] != 1 || retry.Store.AppliedIndex < before.Store.AppliedIndex {
		t.Fatal("durable request dedup was lost across exec")
	}
	newACK := brokerStoreACK(t, f, "stable-new")
	if newACK.Store.AppliedIndex <= before.Store.AppliedIndex || newACK.Store.AppliedTerm < before.Store.AppliedTerm {
		t.Fatal("new durable command regressed or failed to advance its cursor/term")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// Nonempty staged bytes prove that the request crossed real mTLS transport into
// P05's io.Copy. Its accepted socket remains open until the product cut joins it.
// origin: hot-update S6 plan §3.2 / §3.4 / T16
func TestRaftSnapshotRPCJoinWithinCut(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerSnapshotRPCJoin(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker runtime snapshot transport seam", g)
	testharness.AssertNoFDLeak(t, "broker runtime snapshot transport seam", fd, 0)
}
func exerciseBrokerSnapshotRPCJoin(t *testing.T) {
	t.Helper()
	f := startBrokerStoreFixture(t, "sole-voter")
	brokerStoreACK(t, f, "snapshot-original")
	brokerStoreCheckpoint(t, f, "snapshot-save")
	before := brokerStoreACK(t, f, "snapshot-new-durable")
	rejected := brokerStoreCheckpoint(t, f, "snapshot-reject-stale")
	after := brokerStoreCheckpoint(t, f, "before-reload")
	if rejected.Counters["snapshot_floor_rejected"] != 1 || !reflect.DeepEqual(before.ACKs, after.ACKs) || before.Store.AppliedIndex != after.Store.AppliedIndex || before.Store.Identity != after.Store.Identity || before.Seal.Role.Term != after.Seal.Role.Term || before.Seal.Role.ConfigIndex != after.Seal.Role.ConfigIndex || before.Seal.Role.VoterDigest != after.Seal.Role.VoterDigest {
		t.Fatal("obsolete runtime snapshot changed ACK rows, SQL identity/floor or Raft term/configuration")
	}
	stalled := brokerStoreCheckpoint(t, f, "snapshot-stall")
	if stalled.Counters["stalled_snapshot_stage_bytes"] == 0 {
		t.Fatal("snapshot body never reached actual staging")
	}
	closed := brokerStoreCheckpoint(t, f, "store-closed")
	brokerStoreAssertClosed(t, closed)
	if closed.Counters["snapshot_peer_closed"] != 1 || closed.Counters["snapshot_stage_files"] != 0 {
		t.Fatal("close receipt preceded real stalled peer/reader/staging cleanup")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// This is deliberately the product checkpoint, not the component exec label.
// It stays a hard dependency failure until Run supplies its real restored graph.
// origin: hot-update S6 plan §5 / T37
// origin: S8 internal review ROOT-STAGE-LATENCY F1 F2
func TestSoleVoterReloadWithinBudget(t *testing.T) {
	// origin: hot-update S10 lead decision D-A
	for _, mode := range testharness.CandidateBuildModes() {
		t.Run(mode, func(t *testing.T) {
			g, fd := runtime.NumGoroutine(), testharness.FDCount()
			initialFDs := productFDLinks(t)
			for round := 0; round < 5; round++ {
				exerciseSoleVoterProductReload(t)
			}
			testharness.AssertNoGoroutineLeak(t, "sole voter product reload", g)
			if testharness.FDCount() != fd {
				t.Logf("FD identities before=%v after=%v", initialFDs, productFDLinks(t))
			}
			testharness.AssertNoFDLeak(t, "sole voter product reload", fd, 0)

		})
	}
}
func exerciseSoleVoterProductReload(t *testing.T) {
	t.Helper()
	f, err := clusterharness.StartBrokerStoreRunFixture(t, clusterharness.BrokerImageFixtureConfig{Topology: "sole-voter"})
	if err != nil {
		t.Fatal(err)
	}
	before := brokerStoreACK(t, f, "sole-voter-original")
	started := time.Now()
	after := brokerStoreCheckpoint(t, f, "reload-control-ready")
	first := brokerStoreACK(t, f, "sole-voter-first-current")
	// origin: hot-update S10 lead decision D-A / sole-voter reload SLO
	elapsed := time.Since(started)
	t.Logf("sole-voter phase timings: elapsed=%s phases=%v", elapsed, after.Counters)
	testharness.RequireCandidateBudget(t, fmt.Sprintf("/proc/%d/exe", after.PID), elapsed, 5*time.Second)
	brokerStoreAssertCurrent(t, before, after)
	if after.ArtifactKind != "broker-product" || !first.Leader || first.Store.AppliedIndex <= before.Store.AppliedIndex {
		t.Fatal("sole-voter product Run did not reopen/lead with production timers and a new ACK")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// Object NUID/chunks/config, persisted consumer ACK and exact terminal audit are
// read independently from the original external server before/after owner exec.
// origin: hot-update S6 plan §9.2 / T51
func TestBrokerJSCurrentWitness(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerJSWitness(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker external JS current witness", g)
	testharness.AssertNoFDLeak(t, "broker external JS current witness", fd, 0)
}
func exerciseBrokerJSWitness(t *testing.T) {
	t.Helper()
	f := startBrokerStoreFixture(t, "single")
	brokerStoreACK(t, f, "js-original")
	before := brokerStoreCheckpoint(t, f, "js-witness")
	if before.JS == nil || before.JS.NUID == "" || before.JS.Chunks < 2 || before.JS.ConsumerAck == 0 || before.JS.ConsumerStream == 0 || before.JS.AuditSHA256 == "" {
		t.Fatal("JS witness is empty or did not exercise persisted chunks/consumer/terminal audit")
	}
	after := brokerStoreCheckpoint(t, f, "component-exec")
	brokerStoreAssertCurrent(t, before, after)
	if !reflect.DeepEqual(before.JS, after.JS) {
		t.Fatal("external current JetStream object/config/consumer/audit identity changed through exec")
	}
	brokerStoreACK(t, f, "js-current")
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// Origin receipts must detect a same-name recreation even when all bytes are
// unchanged. The control changes the real object, never the witness decoder.
// origin: hot-update S6 plan §9.2 / T51
func TestBrokerJSWitnessRejectsSameNameRecreation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerJSRecreation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "JS same-name recreation control", g)
	testharness.AssertNoFDLeak(t, "JS same-name recreation control", fd, 0)
}
func exerciseBrokerJSRecreation(t *testing.T) {
	t.Helper()
	f := startBrokerStoreFixture(t, "single")
	before := brokerStoreCheckpoint(t, f, "js-witness")
	if err := f.ArmFault(clusterharness.BrokerImageFault{Point: "js-recreate", Ordinal: 1, Once: true}); err != nil {
		t.Fatal(err)
	}
	after := brokerStoreCheckpoint(t, f, "js-witness")
	if before.JS == nil || after.JS == nil || before.JS.NUID == after.JS.NUID || before.JS.BytesSHA256 != after.JS.BytesSHA256 {
		t.Fatal("real same-name/identical-byte recreation control was not observed")
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

// A real independent opener can acquire after terminal release; it must never
// acquire while the current component owner still holds the original OFD.
// origin: hot-update S6 plan §3.1 / T12
func TestBrokerStoreLockProbePositiveControl(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerStoreLockProbeControl(t)
	}
	testharness.AssertNoGoroutineLeak(t, "broker lock probe positive control", g)
	testharness.AssertNoFDLeak(t, "broker lock probe positive control", fd, 0)
}
func exerciseBrokerStoreLockProbeControl(t *testing.T) {
	t.Helper()
	f := startBrokerStoreFixture(t, "single")
	before := brokerStoreCheckpoint(t, f, "before-reload")
	dir, err := clusterharness.BrokerImageDataDirectory(f, before.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if lock, err := cluster.AcquireImageDataDirLock(dir); lock != nil || !errors.Is(err, cluster.ErrDataDirLockHeld) {
		if lock != nil {
			_ = lock.Release()
		}
		t.Fatal("live owner did not hold the canonical lock")
	}
	brokerStoreCheckpoint(t, f, "normal-stop")
	probe := startBrokerLockProbe(t, dir)
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, acquired, err := probe.result()
		if err != nil {
			t.Fatal(err)
		}
		if acquired > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("contender cannot detect a real unlocked window")
		}
		time.Sleep(time.Millisecond)
	}
	probe.close()
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func productFDLinks(t *testing.T) map[string]string {
	t.Helper()
	files, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	links := map[string]string{}
	for _, file := range files {
		if link, err := os.Readlink(filepath.Join("/proc/self/fd", file.Name())); err == nil {
			links[file.Name()] = link
		}
	}
	return links
}
