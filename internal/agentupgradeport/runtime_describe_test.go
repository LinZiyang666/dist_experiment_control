package agentupgradeport

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

// origin: hot-update S8 plan §4.2 / M06 / M14; S8 FIX-DESCRIBE (real-cut
// finding). A frozen bundle exports flow/SS/transfer images even while those
// owners are idle; an idle image has zero identities and must not refuse the
// cut, while any retained endpoint, flow, retirement hole, reservation,
// listener, connection or key still refuses (this adapter cannot name those
// identities). Transfer entries are named by the worker's own census and
// checked against the sealed count instead (TestTransferCensusNamesEveryRetainedEntry).
func TestIdleDomainImagesRefuseRetainedResponsibility(t *testing.T) {
	idle := agentimage.Bundle{Flow: &tunnel.ClientImage{Version: 1, StateRef: "flow-state"}, SS: &ssproxy.Image{Version: 1, StateRef: "ss-state"},
		Transfers: &xfervalue.ImageRef{Schema: "transfer", ResourceID: "transfers", Count: 0}}
	if !idleDomainImages(&idle) || !idleDomainImages(&agentimage.Bundle{}) {
		t.Fatal("idle domain images refused an empty cut")
	}
	carried := idle
	carriedRef := *idle.Transfers
	carriedRef.Count = 3
	carried.Transfers = &carriedRef
	if !idleDomainImages(&carried) {
		t.Fatal("enumerable transfer entries were refused as an unenumerable domain")
	}
	for name, mutate := range map[string]func(*agentimage.Bundle){
		"endpoint":      func(b *agentimage.Bundle) { b.Flow.Endpoints = []tunnel.EndpointImage{{OwnerGen: 1}} },
		"flow":          func(b *agentimage.Bundle) { b.Flow.Flows.Records = []flow.RecordImage{{OpenSeq: 1}} },
		"retirement":    func(b *agentimage.Bundle) { b.Flow.Flows.Retirement = []flow.RetirementImage{{NextOpenSeq: 2}} },
		"reservation":   func(b *agentimage.Bundle) { b.Flow.Flows.ReservedBytes = 1 },
		"ss_listener":   func(b *agentimage.Bundle) { b.SS.ListenerRef = "listener" },
		"ss_connection": func(b *agentimage.Bundle) { b.SS.ConnectionCount = 1 },
		"ss_keys":       func(b *agentimage.Bundle) { b.SS.SecretBytes = 32 },
	} {
		b := idle
		flowImage, ssImage, transfers := *idle.Flow, *idle.SS, *idle.Transfers
		b.Flow, b.SS, b.Transfers = &flowImage, &ssImage, &transfers
		mutate(&b)
		if idleDomainImages(&b) {
			t.Fatalf("%s: retained responsibility read as an idle domain", name)
		}
	}
}

// stateJournal answers Load(key, "") with a fixed Current (or no record).
type stateJournal struct {
	upgradetx.Store
	view  *upgradetx.View
	fault error
}

func (j stateJournal) Load(upgradetx.Key, string) (upgradetx.View, error) {
	if j.fault != nil {
		return upgradetx.View{}, j.fault
	}
	if j.view == nil {
		return upgradetx.View{}, &upgradetx.Error{Code: upgradetx.CodeGrantUnknown, Detail: "transaction is not recorded"}
	}
	return *j.view, nil
}

// origin: hot-update S8 plan §2.1 / §4.2 / M06; S8 FIX-DESCRIBE (P19 request R1).
// The description is the only input a ctl has for Prepare, so it must be the
// target's whole original scope: the configured Key, the running bytes and
// their release, every frozen domain and every retained identity the census
// enumerates, with its owner, generation and custody — never a partial scope
// while a domain is unenumerable, a cut is held or an earlier transaction is
// unsettled.
func TestDescribeRuntimeNamesTheWholeOriginalScope(t *testing.T) {
	owner := upgradetx.Owner{BootID: strings.Repeat("b", 32), PID: os.Getpid(), StartTicks: 7, InstanceID: strings.Repeat("9", 32)}
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node", ConfigPath: "/home/u/.tether/agent/lab/agent.yaml", UID: 1000}
	running := strings.Repeat("a", 64)
	courier := agentimage.CourierState{Version: 1, Events: []agentimage.CourierEvent{{PID: "p1", Kind: "exit", EnqueuedAtUnixNano: 5}}}
	nonce := strings.Repeat("c", 32)
	completed := upgradetx.View{Historical: upgradetx.Record{Forward: upgradetx.Attempt{Handle: upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), Generation: 3},
		SourceGeneration: 2, Phase: upgradetx.PhaseCompleted}}}
	pending := completed
	pending.Historical.Forward.Phase = upgradetx.PhaseServingNew
	live := []TransferEntry{{SID: "lab", TransferID: "t1", EntryNonce: "n1"}}
	for _, tc := range []struct {
		name         string
		journal      stateJournal
		snapshot     error
		transfers    []TransferEntry
		transferErr  error
		noEnumerator bool
		source       string
		held         bool
		generation   uint64
		refusal      string
	}{
		{name: "fresh_runtime", generation: 1},
		{name: "after_completed_transaction", journal: stateJournal{view: &completed}, generation: 3},
		// origin: S8 FIX-DESCRIBE (transfer census). A live capable transfer
		// is named, never a reason to refuse or a row to omit.
		{name: "live_transfer", transfers: live, generation: 1},
		{name: "unsettled_transaction", journal: stateJournal{view: &pending}, refusal: ReasonUpgradeInProgress},
		{name: "corrupt_journal", journal: stateJournal{fault: &upgradetx.Error{Code: upgradetx.CodeStateCorrupt, Detail: "corrupt"}}, refusal: "corrupt"},
		{name: "unenumerable_domain", snapshot: &agentimage.DeferredError{Reason: ReasonDomainCensus, Detail: "flow census unavailable"}, refusal: ReasonDomainCensus},
		{name: "unenumerable_transfer", transferErr: deferral(ReasonDomainCensus), refusal: ReasonDomainCensus},
		{name: "no_transfer_enumerator", noEnumerator: true, refusal: ReasonDomainCensus},
		{name: "running_bytes_changed", source: strings.Repeat("e", 64), refusal: ReasonRunningImageChanged},
		{name: "held_cut", held: true, refusal: ReasonUpgradeInProgress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := tc.source
			if source == "" {
				source = running
			}
			host := Host{Key: key, Journal: tc.journal, Incarnation: func() string { return strings.Repeat("7", 64) },
				Owner:      func() (upgradetx.Owner, error) { return owner, nil },
				RunningSHA: func() (string, error) { return running, nil },
				Source: func(string) (string, agentimage.Capabilities, error) {
					return "v-test", agentimage.Capabilities{ABI: 1, SHA256: source}, nil
				},
				Snapshot: func() (execsession.Image, agentimage.CourierState, error) {
					return execsession.Image{}, courier, tc.snapshot
				},
				Transfers: func() ([]TransferEntry, error) { return tc.transfers, tc.transferErr },
			}
			if tc.noEnumerator {
				host.Transfers = nil
			}
			port := NewRuntime(host).(*runtimePort)
			if tc.held {
				port.held = &heldCut{}
			}
			d, err := port.DescribeRuntime(nonce)
			if tc.refusal != "" {
				var refusal *upgradetx.Error
				if !errors.As(err, &refusal) || refusal.Detail != tc.refusal {
					t.Fatalf("partial or stale description admitted: %+v err=%v", d, err)
				}
				return
			}
			if err != nil || d.Key != key || d.Nonce != nonce || d.RunningSHA != running || d.SourceRelease != "v-test" || d.At <= 0 {
				t.Fatalf("description lost the target identity: %+v err=%v", d, err)
			}
			if len(d.Scope.Participants) != 1 || d.Scope.Participants[0] != key || len(d.Scope.Domains) != len(registryDomains) || len(d.Scope.ID) != 32 {
				t.Fatalf("scope is not the whole single-participant scope: %+v", d.Scope)
			}
			if len(d.Scope.Expected) != 2+len(tc.transfers) {
				t.Fatalf("scope omits or invents a retained identity: %+v", d.Scope.Expected)
			}
			authority := d.Scope.Expected[0]
			if authority.Domain != upgradetx.DomainAuthority || authority.ID != strings.Repeat("7", 64) || authority.Origin != "session:lab" || authority.Owner != owner || authority.Generation != tc.generation {
				t.Fatalf("authority identity missing: %+v", authority)
			}
			row := d.Scope.Expected[1]
			if row.ID != "courier:p1/exit" || row.Owner != owner || row.Generation != tc.generation || row.Custody != "retained" || upgradetx.ResourceNotApplicable(row) {
				t.Fatalf("retained identity lost its owner, generation or custody: %+v", row)
			}
			if len(tc.transfers) != 0 {
				want := upgradetx.Resource{Domain: upgradetx.DomainTransferWorker, ID: "transfer:lab/t1", Origin: "entry:n1", Owner: owner,
					Generation: tc.generation, State: "running", Custody: "source"}
				if d.Scope.Expected[2] != want {
					t.Fatalf("live transfer lost its identity, slot, owner or custody: %+v", d.Scope.Expected[2])
				}
			}
			if _, err := upgradetx.CanonicalScope(d.Scope); err != nil {
				t.Fatalf("description scope is not canonical: %v", err)
			}
		})
	}
}
