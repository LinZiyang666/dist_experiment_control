//go:build linux && hotupdate_integration

// Repeated real-process rollback cuts run in the existing per-test matrix.
// origin: hot-update S10 plan §11.1 / TEST-BUDGET

package hotupdate_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/test/clusterharness"
)

// The candidate must ACK a new effect and advance both original byte streams
// before the rollback fault can fire. Refusal in preflight is not this test.
// origin: hot-update S6 plan §4.2 / §11 T49
func TestBrokerActiveRollbackKeepsNewACK(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseActiveRollbackStop(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "ACTIVE current rollback x5", g)
	testharness.AssertNoFDLeak(t, "ACTIVE current rollback x5", fds, 0)
}

// This separately names the terminal contract, while exercising the complete
// ACTIVE -> new ACK/bytes -> current cut -> prev -> normal stop chain each round.
// origin: hot-update S6 plan §4.2 / §11 T52
func TestBrokerCurrentRollbackAndNormalStop(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseActiveRollbackStop(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "current rollback normal stop x5", g)
	testharness.AssertNoFDLeak(t, "current rollback normal stop x5", fds, 0)
}

// Exact identity multisets guard against same-count substitution. Both the
// process and the imported graph remain alive during the terminal-stop oracle;
// fixture exit cannot supply the missing Close of a Taken memfd or monitor.
// origin: hot-update S6 plan §9.3 / §11 T56Integration
func TestBrokerLifecycleSeamLeakCycles(t *testing.T) {
	g, fds := runtime.NumGoroutine(), testharness.FDCount()
	before, err := brokerSeamFDMultiset(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
			exerciseActiveRollbackStop(t)
			exerciseBrokerPreparedRevoke(t)
			exerciseBrokerExecErrorStop(t)
		})
	}
	testharness.AssertNoGoroutineLeak(t, "broker lifecycle seam x5", g)
	testharness.AssertNoFDLeak(t, "broker lifecycle seam x5", fds, 0)
	after, err := brokerSeamFDMultiset(os.Getpid())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("parent FD identity multiset changed across five real current cuts")
	}
}

func exerciseActiveRollbackStop(t *testing.T) {
	t.Helper()
	for _, topology := range []string{"single", "sole-voter"} {
		t.Run(topology, func(t *testing.T) { exerciseBrokerActiveTopology(t, topology) })
	}
}

func exerciseBrokerActiveTopology(t *testing.T, topology string) {
	t.Helper()
	fixture, closeFixture, root := startBrokerSeam(t, topology)
	defer closeFixture()
	idle := readBrokerSeam(t, fixture, "runtime-idle")
	idleFDs := requireBrokerSeamFDs(t, idle)
	before := readBrokerSeam(t, fixture, "before-reload")
	requireBrokerSeamFDs(t, before)
	requireBrokerSeamLiveResources(t, before)
	unixPath := brokerSeamUnixIdentity(t, root, before.Counters.UnixPath)
	requireBrokerSeamLock(t, root, before.Counters.Store.Identity.LockPath, true)
	// P26 owns the executable fault artifact. The point is armed before the
	// first reload and becomes eligible only after its real ACTIVE ACK/IO barrier.
	if err := brokerSeamFault(fixture, clusterharness.BrokerImageFault{Point: "active-after-new-ack-and-bytes", Ordinal: 1, Once: true}); err != nil {
		t.Fatal(err)
	}
	active := readBrokerSeam(t, fixture, "active-new-ack-and-bytes")
	if err := brokerActiveEvidence(before, active); err != nil {
		t.Fatal(err)
	}
	requireBrokerSeamFDs(t, active)
	requireBrokerSeamLiveResources(t, active)
	if next := brokerSeamUnixIdentity(t, root, active.Counters.UnixPath); next != unixPath {
		t.Fatal("candidate replaced the original Unix pathname")
	}
	requireBrokerSeamLock(t, root, active.Counters.Store.Identity.LockPath, true)
	previous := readBrokerSeam(t, fixture, "active-rollback-control-ready")
	if err := brokerRollbackEvidence(before, active, previous); err != nil {
		t.Fatal(err)
	}
	requireBrokerSeamFDs(t, previous)
	requireBrokerSeamLiveResources(t, previous)
	if next := brokerSeamUnixIdentity(t, root, previous.Counters.UnixPath); next != unixPath {
		t.Fatal("current rollback replaced the original Unix pathname")
	}
	requireBrokerSeamLock(t, root, previous.Counters.Store.Identity.LockPath, true)
	stopped := readBrokerSeam(t, fixture, "normal-stop")
	requireBrokerSeamStopped(t, root, idleFDs, previous, stopped)
}

func brokerActiveEvidence(before, active brokerSeamReceipt) error {
	if !brokerSeamSameProcess(before, active) || active.Counters.Generation != before.Counters.Generation+1 || active.Counters.RunningSHA == before.Counters.RunningSHA ||
		active.PreviousSHA != before.Counters.RunningSHA || active.Counters.Boundary != "ACTIVE" || active.Counters.CutID == "" || active.Counters.CutID == before.Counters.CutID ||
		active.Counters.OldDefers != 0 || active.Counters.DriverWriters != 1 {
		return errors.New("candidate did not reach ACTIVE in the original process/current store")
	}
	if err := brokerSeamACKProgress(before.Counters, active.Counters); err != nil {
		return err
	}
	return brokerSeamFlowProgress(before.Counters.Tunnel, active.Counters.Tunnel, true)
}

func brokerRollbackEvidence(before, active, previous brokerSeamReceipt) error {
	if !brokerSeamSameProcess(active, previous) || previous.Counters.Generation != active.Counters.Generation+1 ||
		previous.Counters.RunningSHA != before.Counters.RunningSHA || previous.Counters.RunningSHA != active.PreviousSHA ||
		previous.Counters.Boundary != "ACTIVE" || previous.Counters.CutID == "" || previous.Counters.CutID == active.Counters.CutID || previous.Counters.CutID == before.Counters.CutID ||
		previous.Counters.OldDefers != 0 || previous.Counters.DriverWriters != 1 || previous.Counters.ReadStage != "before-replay" ||
		previous.Counters.Store.AppliedIndex < active.Counters.Store.AppliedIndex || previous.Counters.Store.AppliedTerm < active.Counters.Store.AppliedTerm {
		return errors.New("ACTIVE rollback reused the initial cut, store or generation")
	}
	rows := make(map[string]brokerSeamACK)
	for _, row := range previous.Counters.ACKRows {
		if row.RequestID == "" || row.Effects != 1 || !brokerSeamSHA(row.ValueSHA, 64) || rows[row.RequestID].RequestID != "" {
			return errors.New("rollback ACK query has missing or duplicate effects")
		}
		rows[row.RequestID] = row
	}
	for _, row := range active.Counters.ACKRows {
		if rows[row.RequestID] != row {
			return errors.New("candidate new ACK vanished before previous-image replay")
		}
	}
	// Post-rollback business has to advance, too. Comparing only the exported
	// stamp would accept a previous image that adopted FDs but never resumed them.
	return brokerSeamFlowProgress(active.Counters.Tunnel, previous.Counters.Tunnel, true)
}

func brokerSeamFlowProgress(before, after tunnel.ServerImage, advance bool) error {
	if before.IssuerBootID == "" || before.InstanceID == "" || before.OwnerGeneration == 0 || before.IssuerBootID != after.IssuerBootID ||
		before.InstanceID != after.InstanceID || before.OwnerGeneration != after.OwnerGeneration ||
		len(before.Endpoints) == 0 || len(before.Endpoints) != len(after.Endpoints) || len(before.Flows.Records) < 2 || len(before.Flows.Records) != len(after.Flows.Records) {
		return errors.New("original server/Endpoint/flow cohort is absent or replaced")
	}
	endpoints := make(map[flow.EndpointID]tunnel.EndpointImage)
	for _, endpoint := range after.Endpoints {
		if endpoint.ID == (flow.EndpointID{}) || endpoint.OwnerGen == 0 || endpoints[endpoint.ID].OwnerGen != 0 {
			return errors.New("Endpoint identity is absent or duplicated")
		}
		endpoints[endpoint.ID] = endpoint
	}
	seenEndpoints := make(map[flow.EndpointID]bool)
	for _, endpoint := range before.Endpoints {
		next, ok := endpoints[endpoint.ID]
		if !ok || seenEndpoints[endpoint.ID] || next.OwnerGen != endpoint.OwnerGen || next.CarrierGen <= endpoint.CarrierGen {
			return errors.New("broker cut changed an agent owner fence or did not rebuild its carrier")
		}
		seenEndpoints[endpoint.ID] = true
	}
	records := make(map[flow.ID]flow.RecordImage)
	for _, record := range after.Flows.Records {
		if record.ID == (flow.ID{}) || record.OpenSeq == 0 || records[record.ID].OpenSeq != 0 {
			return errors.New("flow identity is absent or duplicated")
		}
		records[record.ID] = record
	}
	seen := make(map[flow.ID]bool)
	for _, record := range before.Flows.Records {
		next, ok := records[record.ID]
		if !ok || seen[record.ID] || record.OpenSeq == 0 || record.RawRef == "" || record.ReplayRef == "" || next.OpenSeq != record.OpenSeq ||
			next.RawRef != record.RawRef || next.ReplayRef != record.ReplayRef || next.TxBase > next.TxEnd || next.RxApplied > next.RxReceived ||
			next.TxBase < record.TxBase || next.TxEnd < record.TxEnd || next.RxApplied < record.RxApplied || next.RxReceived < record.RxReceived ||
			advance && (next.TxBase == record.TxBase || next.TxEnd == record.TxEnd || next.RxApplied == record.RxApplied) {
			return errors.New("rollback lost original flow identity or acknowledged byte progress")
		}
		seen[record.ID] = true
	}
	return nil
}

// P26 is still absent. These synthetic inputs test only the independent oracle,
// and are never passed to the real-process tests or used as continuity evidence.
// Each plan mutation has a distinct failing counterexample before any fixture
// can be accepted. Product overlay receipts remain separate and mandatory.
// origin: hot-update S6 plan §11 T02 / T49 / T52
func TestBrokerRollbackOracleControls(t *testing.T) {
	before, active, previous := brokerSeamOracleInputs()
	if err := brokerActiveEvidence(before, active); err != nil {
		t.Fatal(err)
	}
	if err := brokerRollbackEvidence(before, active, previous); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*brokerSeamReceipt)
	}{
		{"initial-cut", func(r *brokerSeamReceipt) { r.Counters.Tunnel = before.Counters.Tunnel }},
		{"old-db-snapshot", func(r *brokerSeamReceipt) { r.Counters.ACKRows = before.Counters.ACKRows }},
		{"replay-conceals-loss", func(r *brokerSeamReceipt) { r.Counters.ReadStage = "after-replay" }},
		{"pristine-active", func(r *brokerSeamReceipt) { r.Counters.Boundary = "pristine" }},
		{"generation-not-advanced", func(r *brokerSeamReceipt) { r.Counters.Generation = active.Counters.Generation }},
		{"initial-cut-id", func(r *brokerSeamReceipt) { r.Counters.CutID = before.Counters.CutID }},
		{"store-inode-replaced", func(r *brokerSeamReceipt) { r.Counters.Store.Identity.Inode++ }},
		{"cursor-reset", func(r *brokerSeamReceipt) { r.Counters.Store.AppliedIndex = before.Counters.Store.AppliedIndex }},
		{"term-reset", func(r *brokerSeamReceipt) { r.Counters.Store.AppliedTerm = 0 }},
		{"effect-duplicated", func(r *brokerSeamReceipt) { r.Counters.ACKRows[1].Effects = 2 }},
		{"row-duplicated", func(r *brokerSeamReceipt) { r.Counters.ACKRows = append(r.Counters.ACKRows, r.Counters.ACKRows[0]) }},
		{"captured-old-defer", func(r *brokerSeamReceipt) { r.Counters.OldDefers = 1 }},
		{"new-process", func(r *brokerSeamReceipt) { r.StartTicks++ }},
		{"wrong-prev", func(r *brokerSeamReceipt) { r.Counters.RunningSHA = active.Counters.RunningSHA }},
		{"new-agent-owner", func(r *brokerSeamReceipt) { r.Counters.Tunnel.Endpoints[0].OwnerGen++ }},
		{"cold-ticket-issuer", func(r *brokerSeamReceipt) { r.Counters.Tunnel.IssuerBootID = "cold" }},
		{"redial-flow", func(r *brokerSeamReceipt) { r.Counters.Tunnel.Flows.Records[0].ID = flow.ID{99} }},
		{"duplicate-flow", func(r *brokerSeamReceipt) { r.Counters.Tunnel.Flows.Records[1] = r.Counters.Tunnel.Flows.Records[0] }},
		{"stale-tx-base", func(r *brokerSeamReceipt) {
			r.Counters.Tunnel.Flows.Records[0].TxBase = active.Counters.Tunnel.Flows.Records[0].TxBase
		}},
		{"stale-rx-applied", func(r *brokerSeamReceipt) {
			r.Counters.Tunnel.Flows.Records[0].RxApplied = active.Counters.Tunnel.Flows.Records[0].RxApplied
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := cloneBrokerSeamReceipt(t, previous)
			tc.mutate(&changed)
			if brokerRollbackEvidence(before, active, changed) == nil {
				t.Fatal("current rollback oracle accepted the injected defect")
			}
		})
	}
	stopped := cloneBrokerSeamReceipt(t, previous)
	stopped.Counters.NCClosed, stopped.Counters.NodeClosed = true, true
	stopped.Counters.DriverWriters, stopped.Counters.DriverReaders = 0, 0
	stopped.Counters.StopCurrentCalls = 1
	stopped.Counters.ActiveMonitors, stopped.Counters.ActiveCallbacks = new(uint32), new(uint32)
	terminalIndex := stopped.Counters.Store.AppliedIndex
	stopped.Counters.TerminalIndex = &terminalIndex
	if err := brokerSeamStopEvidence(previous, stopped); err != nil {
		t.Fatal(err)
	}
	// A commit between the live sample and shutdown is legal, but its final
	// cursor must be identical on both sides of the writer's actual close.
	// origin: hot-update S6 plan §4.2 / T52
	stopped.Counters.Store.AppliedIndex++
	terminalIndex++
	if err := brokerSeamStopEvidence(previous, stopped); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*brokerSeamReceipt)
	}{
		{"current-nc-open", func(r *brokerSeamReceipt) { r.Counters.NCClosed = false }},
		{"current-node-open", func(r *brokerSeamReceipt) { r.Counters.NodeClosed = false }},
		{"current-db-open", func(r *brokerSeamReceipt) { r.Counters.DriverWriters = 1 }},
		{"reader-leaked", func(r *brokerSeamReceipt) { r.Counters.DriverReaders = 1 }},
		{"rpc-unjoined", func(r *brokerSeamReceipt) { r.Counters.ActiveRPCs = 1 }},
		{"backup-unjoined", func(r *brokerSeamReceipt) { r.Counters.ActiveBackups = 1 }},
		{"monitor-unjoined", func(r *brokerSeamReceipt) { *r.Counters.ActiveMonitors = 1 }},
		{"callback-unjoined", func(r *brokerSeamReceipt) { *r.Counters.ActiveCallbacks = 1 }},
		{"callback-census-missing", func(r *brokerSeamReceipt) { r.Counters.ActiveCallbacks = nil }},
		{"taken-state-leaked", func(r *brokerSeamReceipt) {
			r.Counters.FDs = []brokerSeamFD{{ID: "state", Kind: "private", Inode: 1, Count: 1}}
		}},
		{"old-defer", func(r *brokerSeamReceipt) { r.Counters.OldDefers = 1 }},
		{"old-stop-owner", func(r *brokerSeamReceipt) { r.Counters.Generation-- }},
		{"double-stop", func(r *brokerSeamReceipt) { r.Counters.StopCurrentCalls = 2 }},
		{"terminal-cursor-missing", func(r *brokerSeamReceipt) { r.Counters.TerminalIndex = nil }},
		{"terminal-cursor-diverged", func(r *brokerSeamReceipt) { r.Counters.Store.AppliedIndex++ }},
		{"terminal-cursor-regressed", func(r *brokerSeamReceipt) {
			r.Counters.Store.AppliedIndex = previous.Counters.Store.AppliedIndex - 1
			*r.Counters.TerminalIndex = r.Counters.Store.AppliedIndex
		}},
		{"terminal-ack-lost", func(r *brokerSeamReceipt) { r.Counters.ACKRows = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := cloneBrokerSeamReceipt(t, stopped)
			tc.mutate(&changed)
			if brokerSeamStopEvidence(previous, changed) == nil {
				t.Fatal("normal-stop oracle accepted the injected defect")
			}
		})
	}
}

func cloneBrokerSeamReceipt(t *testing.T, receipt brokerSeamReceipt) brokerSeamReceipt {
	t.Helper()
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var result brokerSeamReceipt
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func brokerSeamOracleInputs() (brokerSeamReceipt, brokerSeamReceipt, brokerSeamReceipt) {
	monitorCount, callbackCount := uint32(1), uint32(0)
	base := brokerSeamReceipt{PID: 42, StartTicks: 73, SourceSHA: strings.Repeat("a", 40), PreviousSHA: strings.Repeat("b", 64)}
	base.Counters = brokerSeamCounters{Mode: "sole-voter", Generation: 1, CutID: "initial", RunningSHA: strings.Repeat("b", 64), DriverWriters: 1,
		ActiveMonitors: &monitorCount, ActiveCallbacks: &callbackCount,
		Store: storage.StoreStamp{Identity: storage.DBIdentity{Path: "/fixture/current.db", DataDir: "/fixture", LockPath: "/fixture/tether.lock", Device: 1, Inode: 2},
			Journal: "wal", Synchronous: 2, AppliedIndex: 10, AppliedTerm: 3},
		ACKRows: []brokerSeamACK{{RequestID: "old", ValueSHA: strings.Repeat("c", 64), Effects: 1}},
		Tunnel: tunnel.ServerImage{IssuerBootID: "issuer", InstanceID: "instance", OwnerGeneration: 7,
			Endpoints: []tunnel.EndpointImage{{ID: flow.EndpointID{1}, OwnerGen: 5, CarrierGen: 1}}}}
	for i := byte(1); i <= 2; i++ {
		base.Counters.Tunnel.Flows.Records = append(base.Counters.Tunnel.Flows.Records, flow.RecordImage{ID: flow.ID{i}, OpenSeq: uint64(i),
			RawRef: fmt.Sprintf("flow.%d", i), ReplayRef: fmt.Sprintf("replay.%d", i), TxBase: 20, TxEnd: 30, RxApplied: 20, RxReceived: 20})
	}
	active := base
	active.Counters.Generation, active.Counters.CutID, active.Counters.Boundary = 2, "active", "ACTIVE"
	active.Counters.RunningSHA, active.Counters.ReadStage = strings.Repeat("d", 64), "before-replay"
	active.Counters.Store.AppliedIndex = 11
	active.Counters.NewACKIDs = []string{"new"}
	active.Counters.ACKRows = append(append([]brokerSeamACK(nil), base.Counters.ACKRows...), brokerSeamACK{RequestID: "new", ValueSHA: strings.Repeat("e", 64), Effects: 1})
	active.Counters.Tunnel = brokerSeamAdvanceImage(base.Counters.Tunnel)
	previous := active
	previous.Counters.Generation, previous.Counters.CutID = 3, "current"
	previous.Counters.RunningSHA = base.Counters.RunningSHA
	previous.Counters.Tunnel = brokerSeamAdvanceImage(active.Counters.Tunnel)
	return base, active, previous
}

func brokerSeamAdvanceImage(image tunnel.ServerImage) tunnel.ServerImage {
	image.Endpoints = append([]tunnel.EndpointImage(nil), image.Endpoints...)
	image.Flows.Records = append([]flow.RecordImage(nil), image.Flows.Records...)
	for i := range image.Endpoints {
		image.Endpoints[i].CarrierGen++
	}
	for i := range image.Flows.Records {
		r := &image.Flows.Records[i]
		r.TxBase += 10
		r.TxEnd += 10
		r.RxApplied += 10
		r.RxReceived += 10
	}
	return image
}
