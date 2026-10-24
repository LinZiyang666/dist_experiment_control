package agentupgradeport

import (
	"errors"
	"log/slog"
	"net"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/workload"
)

// origin: S8 internal review TAGGED-SUITE-SWEEP-A F2
func TestRuntimeCensusCarriesFlowIdentities(t *testing.T) {
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 1, InstanceID: strings.Repeat("1", 32)}
	p := NewRuntime(Host{Key: upgradetx.Key{SID: "lab"}, Transfers: noTransfers,
		Incarnation: func() string { return strings.Repeat("7", 64) }}).(*runtimePort)
	endpoint := flow.EndpointID{1}
	b := agentimage.Bundle{Flow: &tunnel.ClientImage{Version: 1,
		Endpoints: []tunnel.EndpointImage{{ID: endpoint, OwnerGen: 2, CarrierGen: 3, FlowIDs: []flow.ID{{2}, {3}}}},
		Flows: flow.Image{Version: 1, ReservedBytes: upgradebudget.StreamReplayBytes,
			Records: []flow.RecordImage{{ID: flow.ID{2}, OpenSeq: 1, Intent: [32]byte{4}, Phase: flow.FlowPhaseActive},
				{ID: flow.ID{3}, OpenSeq: 2, Intent: [32]byte{5}, Phase: flow.FlowPhaseTerminal}},
			Retirement: []flow.RetirementImage{{EndpointID: endpoint, NextOpenSeq: 3, RetiredThrough: 2}}}}}
	p.host.Domains = func() (FlowSSState, error) { return FlowSSState{Flow: b.Flow}, nil }
	rows, err := p.rows(&b, owner, 2, "target")
	if err != nil {
		t.Fatalf("live and retained flow identities refused: %v", err)
	}
	if len(rows) != 5 { // authority, endpoint, two streams and retirement journal
		t.Fatalf("census omitted a retained flow responsibility: %+v", rows)
	}
	for _, row := range rows {
		if row.Owner != owner || row.Generation != 2 || row.Origin == "" {
			t.Fatalf("census lost original identity or current custody: %+v", row)
		}
	}
	// Normal ACK/FIN/retirement progress changes private contents, never the
	// original identity. All completion samples must allow such live traffic.
	b.Flow.Flows.Records[0].TxBase, b.Flow.Flows.Records[0].TxEnd = 10, 20
	b.Flow.Flows.Records[0].FINFlags = 1
	b.Flow.Flows.Retirement[0].Holes = []uint64{1}
	later, err := p.rows(&b, owner, 2, "target")
	if err != nil || !upgradetx.CensusProgressed(upgradetx.Census{Resources: rows}, upgradetx.Census{Resources: later}) {
		t.Fatalf("ordinary traffic changed census identity: %v", err)
	}
	b.Flow.Flows.Records[0].Intent = [32]byte{9}
	replaced, err := p.rows(&b, owner, 2, "target")
	if err != nil || upgradetx.CensusProgressed(upgradetx.Census{Resources: later}, upgradetx.Census{Resources: replaced}) {
		t.Fatal("replacement stream origin stood in for the original open")
	}
	b.Flow.Flows.Records[0].Intent = [32]byte{4}
	b.Flow.Flows.Records[0].Phase = flow.FlowPhaseTerminal
	b.Flow.Flows.ReservedBytes = 0
	terminal, err := p.rows(&b, owner, 2, "target")
	if err != nil || !upgradetx.CensusProgressed(upgradetx.Census{Resources: later}, upgradetx.Census{Resources: terminal}) ||
		upgradetx.CensusProgressed(upgradetx.Census{Resources: terminal}, upgradetx.Census{Resources: later}) {
		t.Fatal("terminal custody was lost or allowed to resume")
	}
	for name, mutate := range map[string]func(*tunnel.ClientImage){
		"unnamed-stream":    func(im *tunnel.ClientImage) { im.Flows.Records[0].ID = flow.ID{} },
		"orphan-stream":     func(im *tunnel.ClientImage) { im.Endpoints = nil },
		"duplicate-binding": func(im *tunnel.ClientImage) { im.Endpoints = append(im.Endpoints, im.Endpoints[0]) },
		"missing-journal":   func(im *tunnel.ClientImage) { im.Flows.Records = nil },
		"missing-origin":    func(im *tunnel.ClientImage) { im.Flows.Records[0].Intent = [32]byte{} },
	} {
		t.Run(name, func(t *testing.T) {
			im := *b.Flow
			im.Flows.Records = append([]flow.RecordImage(nil), im.Flows.Records...)
			mutate(&im)
			bad := agentimage.Bundle{Flow: &im}
			if _, err := p.rows(&bad, owner, 2, "target"); !censusRefused(err) {
				t.Fatal("incomplete stream custody signed a census")
			}
		})
	}
}

// origin: S8 internal review TAGGED-SUITE-SWEEP-A F2 / hot-update S8 plan §4.2
func TestFlowSSCensusClosesOriginalScopeAndEveryReceipt(t *testing.T) {
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 1, InstanceID: strings.Repeat("1", 32)}
	key := upgradetx.Key{Component: upgradetx.ComponentAgent, DomainID: strings.Repeat("d", 32), SID: "lab", ConfiguredNID: "node"}
	source, target := strings.Repeat("a", 64), strings.Repeat("b", 64)
	endpoint := flow.EndpointID{1}
	state := FlowSSState{Flow: &tunnel.ClientImage{Version: 1, Endpoints: []tunnel.EndpointImage{{ID: endpoint, OwnerGen: 1, FlowIDs: []flow.ID{{2}}}},
		Flows: flow.Image{Version: 1, Records: []flow.RecordImage{{ID: flow.ID{2}, OpenSeq: 1, Intent: [32]byte{3}, Phase: flow.FlowPhaseActive}},
			Retirement: []flow.RetirementImage{{EndpointID: endpoint, NextOpenSeq: 2}}}},
		SS: []ssproxy.Responsibility{{Kind: "server", ID: [16]byte{4}}, {Kind: "authority", ID: [16]byte{4}}, {Kind: "timer", ID: [16]byte{4}},
			{Kind: "material", ID: [16]byte{5}}, {Kind: "connection", ID: [16]byte{6}}, {Kind: "binding", ID: [16]byte{7}}}, Proxy: true}
	p := NewRuntime(Host{Key: key, Journal: stateJournal{}, Transfers: noTransfers,
		Incarnation: func() string { return strings.Repeat("7", 64) }, Owner: func() (upgradetx.Owner, error) { return owner, nil },
		RunningSHA: func() (string, error) { return source, nil },
		Source: func(sha string) (string, agentimage.Capabilities, error) {
			return "source", agentimage.Capabilities{ABI: 1, SHA256: sha}, nil
		},
		Snapshot: func() (execsession.Image, agentimage.CourierState, error) {
			return execsession.Image{}, agentimage.CourierState{}, nil
		},
		Domains: func() (FlowSSState, error) { return state, nil }}).(*runtimePort)
	d, err := p.DescribeRuntime(newID())
	if err != nil || len(d.Scope.Expected) != 11 {
		t.Fatalf("original scope omits live or private identities: %+v %v", d.Scope.Expected, err)
	}
	digest, err := upgradetx.ScopeDigest(d.Scope)
	if err != nil {
		t.Fatal(err)
	}
	handle := upgradetx.Handle{Key: key, TxID: newID(), CutID: newID(), Generation: 2, Revision: 9}
	a := upgradetx.Attempt{Handle: handle, Direction: upgradetx.DirectionForward, SourceSHA: source, TargetSHA: target,
		SourceGeneration: 1, Phase: upgradetx.PhaseActivating, CutStage: upgradetx.CutPartialActive, Outcome: upgradetx.OutcomePending, Scope: d.Scope}
	view := &upgradetx.View{Historical: upgradetx.Record{Key: key, Version: upgradetx.RecordVersion, Revision: 9,
		Grant: upgradetx.Grant{Handle: handle, ScopeDigest: digest}, Forward: a}}
	p.host.Journal = recordJournal{view: view}
	p.host.RunningSHA = func() (string, error) { return target, nil }
	p.host.Inherited = &upgradecut.State{Attempt: handle, Owner: owner, SourceSHA: source, TargetSHA: target, Scope: d.Scope}
	p.host.Stage = func() upgradetx.CutStage { return upgradetx.CutActive }
	p.host.Control = func(string) error { return nil }
	registry, err := CutRegistry(p, p.host.Journal, *p.host.Inherited)
	if err != nil {
		t.Fatal(err)
	}
	o, err := p.Observe(upgradetx.Stamp{Attempt: handle, Nonce: newID(), ScopeDigest: digest})
	if err != nil {
		t.Fatal(err)
	}
	migrated, err := registry.Seal(a, newID())
	if err != nil {
		t.Fatal(err)
	}
	released, err := p.Retire(handle)
	if err != nil {
		t.Fatal(err)
	}
	for _, receipt := range []upgradetx.Receipt{o.Current.Control, migrated, released} {
		if !receipt.Known || !receipt.Complete || !upgradetx.CensusCovers(d.Scope, receipt.Census) || len(receipt.Census.Resources) != len(d.Scope.Expected) {
			t.Fatalf("%s lost original/private identity membership", receipt.Kind)
		}
		for _, row := range receipt.Census.Resources {
			if row.Owner != owner || row.Generation != 2 || row.Custody != "target" {
				t.Fatalf("%s lost target custody: %+v", receipt.Kind, row)
			}
		}
	}
}

// origin: S8 internal review TAGGED-SUITE-SWEEP-A F2
func TestRuntimeCensusRequiresSSPrivateIdentities(t *testing.T) {
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 1, InstanceID: strings.Repeat("1", 32)}
	entries := []ssproxy.Responsibility{{Kind: "server", ID: [16]byte{1}}, {Kind: "authority", ID: [16]byte{1}},
		{Kind: "timer", ID: [16]byte{1}}, {Kind: "listener", ID: [16]byte{1}}, {Kind: "material", ID: [16]byte{2}},
		{Kind: "connection", ID: [16]byte{3}}, {Kind: "binding", ID: [16]byte{4}}}
	state := FlowSSState{SS: entries, Proxy: true}
	p := NewRuntime(Host{Key: upgradetx.Key{SID: "lab", ConfiguredNID: "node"}, Transfers: noTransfers,
		Incarnation: func() string { return strings.Repeat("7", 64) },
		Domains:     func() (FlowSSState, error) { return state, nil }}).(*runtimePort)
	b := agentimage.Bundle{SS: &ssproxy.Image{Version: 1, ListenerRef: "ss-listener.01000000000000000000000000000000", ConnectionCount: 1, SecretBytes: 99}}
	rows, err := p.rows(&b, owner, 2, "target")
	if err != nil || len(rows) != len(entries)+2 { // authority and proxy adjunct
		t.Fatalf("SS census dropped private retained state: %+v %v", rows, err)
	}
	for _, row := range rows {
		if row.Owner != owner || row.Generation != 2 || row.Origin == "" || row.Custody != "target" {
			t.Fatalf("SS responsibility has no current generation owner: %+v", row)
		}
	}
	for name, mutate := range map[string]func(){
		"no-enumerator":        func() { p.host.Domains = nil },
		"missing-connection":   func() { state.SS = append(append([]ssproxy.Responsibility(nil), entries[:5]...), entries[6:]...) },
		"missing-listener":     func() { b.SS.ListenerRef = "" },
		"missing-sealed-image": func() { b.SS = nil },
		"unknown-record":       func() { state.SS = append(state.SS, ssproxy.Responsibility{Kind: "unknown", ID: [16]byte{9}}) },
		"unnamed-record":       func() { state.SS = append(state.SS, ssproxy.Responsibility{Kind: "material"}) },
		"duplicate-record":     func() { state.SS = append(state.SS, entries[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			state = FlowSSState{SS: append([]ssproxy.Responsibility(nil), entries...), Proxy: true}
			image := *b.SS
			p.host.Domains = func() (FlowSSState, error) { return state, nil }
			mutate()
			if _, err := p.rows(&b, owner, 2, "target"); !censusRefused(err) {
				t.Fatal("unnamed SS custody signed a census")
			}
			b.SS = &image
		})
	}
}

// origin: S8 internal review TAGGED-SUITE-SWEEP-A F2
func TestFlowSSCensusKeepsUnknownOwnerRefusal(t *testing.T) {
	var cut sync.Mutex
	var workloads workload.Manager
	if _, _, err := SnapshotRuntime(nil, &cut, &workloads, true, func() agentimage.CourierState { return agentimage.CourierState{} }); err == nil {
		t.Fatal("unknown flow/SS owner was read as an empty runtime")
	}
	for _, c := range []tunnel.Census{{Unsupported: true}, {Legacy: 1}, {Admission: 1}, {Rehome: 1}, {Retained: 1}} {
		if _, err := CensusOwners(agentimage.Owners{}, c, false); !censusRefused(err) {
			t.Fatalf("unknown owner was accepted: %+v", c)
		}
	}
}

// origin: S8 internal review TAGGED-SUITE-SWEEP-A F2
func TestSSCensusPreservesIdleMaterialAndCutIdentity(t *testing.T) {
	fd, g := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		exerciseSSIdentityCensus(t)
	}
	testharness.AssertNoFDLeak(t, "SS identity census x5", fd, 0)
	testharness.AssertNoGoroutineLeak(t, "SS identity census x5", g)
}

func exerciseSSIdentityCensus(t *testing.T) {
	t.Helper()
	s := ssproxy.New(slog.New(slog.DiscardHandler))
	defer s.Stop()
	if err := s.SetKeys([]ssproxy.Key{{KeyID: "one", Secret: "private-test-material"}}); err != nil {
		t.Fatal(err)
	}
	before, err := ssproxy.Census(s, time.Now().Add(upgradebudget.SafePointWait))
	if err != nil || len(before) != 4 { // server/authority/timer/material, zero actors
		t.Fatalf("idle retained material was omitted: %+v %v", before, err)
	}
	port, err := s.Start(0, []ssproxy.Key{{KeyID: "one", Secret: "private-test-material"}})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	deadline := time.Now().Add(upgradebudget.SafePointWait)
	for {
		before, err = ssproxy.Census(s, deadline)
		if err != nil {
			t.Fatal(err)
		}
		if len(before) == 6 { // original server/control/material plus listener and connection
			break
		}
		if !time.Now().Before(deadline) {
			t.Fatal("live SS listener/connection census was read as empty")
		}
		time.Sleep(time.Millisecond)
	}
	cut, err := s.Freeze(time.Now().Add(upgradebudget.SafePointWait))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cut.Thaw() }()
	if _, _, err := cut.Export(); err != nil {
		t.Fatal(err)
	}
	after, err := ssproxy.Census(s, time.Now().Add(upgradebudget.SafePointWait))
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("first export replaced an original SS identity")
	}
}

// origin: S8 internal review TAGGED-SUITE-SWEEP-A F2
func TestFlowCensusKeepsTerminalReceiptsAndRetirement(t *testing.T) {
	fd, g := testharness.FDCount(), runtime.NumGoroutine()
	for round := 0; round < 5; round++ {
		exerciseFlowJournalCensus(t)
	}
	testharness.AssertNoFDLeak(t, "flow journal census x5", fd, 0)
	testharness.AssertNoGoroutineLeak(t, "flow journal census x5", g)
}

func exerciseFlowJournalCensus(t *testing.T) {
	t.Helper()
	m, err := flow.New(flow.Config{Budget: &workload.ReplayBudget{Limit: upgradebudget.NodeReplayBytes},
		Dial: func(flow.ID, uint16, time.Time, <-chan struct{}) (*rawsocket.Socket, error) {
			return nil, errors.New("controlled target refusal")
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Close() }()
	id, endpoint := flow.ID{1}, flow.EndpointID{2}
	_, err = flow.Open(m, flow.OpenRequest{ID: id, EndpointID: endpoint, OpenSeq: 1, TargetPort: 1234,
		Intent: flow.OpenIntent(endpoint, id, 1, 1234), OwnerGen: 1, CarrierGen: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(upgradebudget.SafePointWait)
	for {
		row, changed, err := flow.Observe(m, id)
		if err != nil {
			t.Fatal(err)
		}
		if row.Phase == flow.FlowPhaseTerminal {
			break
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-changed:
			timer.Stop()
		case <-timer.C:
			t.Fatal("controlled failed dial did not retain its terminal receipt")
		}
	}
	rows, retained, err := flow.Census(m, deadline)
	if err != nil || len(rows) != 1 || rows[0].ID != id || rows[0].EndpointID != endpoint ||
		rows[0].Phase != flow.FlowPhaseTerminal || len(retained) != 1 || retained[0].EndpointID != endpoint || retained[0].NextOpenSeq != 2 {
		t.Fatalf("flow census dropped retained custody: rows=%+v retained=%+v err=%v", rows, retained, err)
	}
	if live := flow.States(m, endpoint); len(live) != 0 {
		t.Fatal("terminal fixture still has an executing stream")
	}
}
