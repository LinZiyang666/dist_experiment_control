package brokerupgradeport

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func refusalDetail(err error) string {
	var typed *upgradetx.Error
	if errors.As(err, &typed) {
		return typed.Detail
	}
	return ""
}

func rowOf(rows []upgradetx.Resource, domain upgradetx.Domain, id string) (upgradetx.Resource, bool) {
	for _, row := range rows {
		if row.Domain == domain && row.ID == id {
			return row, true
		}
	}
	return upgradetx.Resource{}, false
}

// Every census row is an original object identity bound to the creation it
// belongs to: the store's inode, an allocation's epoch, a tracker slot's
// EntryNonce, a session's incarnation. A recreated object with the same name
// is a different row, never a stand-in. What the broker cannot name, or what
// the S6 cut itself cannot carry (legacy or unresolved transfers), refuses.
// origin: hot-update S8 plan §4.2 / M06 / M14 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
func TestBrokerCensusRowsBindEachObjectToItsCreation(t *testing.T) {
	owner := upgradetx.Owner{BootID: strings.Repeat("b", 32), PID: 42, StartTicks: 9, InstanceID: strings.Repeat("a", 32)}
	facts := BrokerFacts{Store: StoreIdentity{Path: "/var/lib/tether/state.db", Device: 2049, Inode: 77}, Listeners: []string{"admin"}, Tunnel: true,
		Sessions:    []TunnelSession{{Port: 4100, SID: "lab", NID: "n1", Epoch: 3}, {Port: 4100, SID: "lab", NID: "n1", Epoch: 3}},
		Trackers:    []TrackerEntry{{SID: "lab", TransferID: "push-1", EntryNonce: "e1"}, {SID: "lab", TransferID: "push-0", EntryNonce: "e0", Settled: true}},
		Authorities: []upgradecut.Authority{{SID: "lab", Incarnation: strings.Repeat("c", 64)}}}
	rows, err := censusRows(facts, owner, 4, "source")
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []upgradetx.Resource{
		{Domain: upgradetx.DomainBrokerStore, ID: "store:/var/lib/tether/state.db", Origin: "inode:2049:77", State: "active", Custody: "source"},
		{Domain: upgradetx.DomainBrokerRuntime, ID: "listener:admin", Origin: "listener:admin", State: "active", Custody: "source"},
		{Domain: upgradetx.DomainBrokerRuntime, ID: "tunnel:server", Origin: "tunnel:server", State: "active", Custody: "source"},
		{Domain: upgradetx.DomainBrokerRuntime, ID: "tunnel:4100", Origin: "allocation:lab/n1@3", State: "active", Custody: "source"},
		{Domain: upgradetx.DomainTransferTracker, ID: "tracker:lab/push-1", Origin: "entry:e1", State: "running", Custody: "source"},
		{Domain: upgradetx.DomainTransferTracker, ID: "tracker:lab/push-0", Origin: "entry:e0", State: "terminal", Custody: "retained"},
		{Domain: upgradetx.DomainAuthority, ID: "session:lab", Origin: "incarnation:" + strings.Repeat("c", 64), State: "active", Custody: "source"},
	} {
		want.Owner, want.Generation = owner, 4
		if got, ok := rowOf(rows, want.Domain, want.ID); !ok || got != want {
			t.Errorf("row %s/%s: got %+v want %+v", want.Domain, want.ID, got, want)
		}
	}
	if len(rows) != 7 {
		t.Fatalf("a listener and its flow endpoint must name one allocation once: %d rows %+v", len(rows), rows)
	}
	// A recreated store, allocation, tracker slot or session is a new origin.
	recreated := facts
	recreated.Store.Inode = 78
	recreated.Sessions = []TunnelSession{{Port: 4100, SID: "lab", NID: "n1", Epoch: 4}}
	recreated.Trackers = []TrackerEntry{{SID: "lab", TransferID: "push-1", EntryNonce: "e2"}}
	recreated.Authorities = []upgradecut.Authority{{SID: "lab", Incarnation: strings.Repeat("d", 64)}}
	later, err := censusRows(recreated, owner, 4, "source")
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []struct {
		domain upgradetx.Domain
		id     string
	}{{upgradetx.DomainBrokerStore, "store:/var/lib/tether/state.db"}, {upgradetx.DomainBrokerRuntime, "tunnel:4100"},
		{upgradetx.DomainTransferTracker, "tracker:lab/push-1"}, {upgradetx.DomainAuthority, "session:lab"}} {
		before, _ := rowOf(rows, id.domain, id.id)
		after, _ := rowOf(later, id.domain, id.id)
		if before.Origin == after.Origin {
			t.Errorf("%s/%s: a recreated object kept the original identity %q", id.domain, id.id, before.Origin)
		}
	}
	for name, defect := range map[string]struct {
		mutate func(*BrokerFacts)
		reason string
	}{
		"legacy or unresolved transfer": {func(f *BrokerFacts) { f.Unsettled = 1 }, ReasonTransfersInFlight},
		"unknown store":                 {func(f *BrokerFacts) { f.Store = StoreIdentity{} }, ReasonDomainCensus},
		"store without inode":           {func(f *BrokerFacts) { f.Store.Inode = 0 }, ReasonDomainCensus},
		"tracker slot without nonce":    {func(f *BrokerFacts) { f.Trackers = []TrackerEntry{{SID: "lab", TransferID: "push-1"}} }, ReasonDomainCensus},
		"duplicate listener":            {func(f *BrokerFacts) { f.Listeners = []string{"admin", "admin"} }, ReasonDomainCensus},
		"one port two allocations": {func(f *BrokerFacts) {
			f.Sessions = []TunnelSession{{Port: 4100, SID: "lab", NID: "n1", Epoch: 3}, {Port: 4100, SID: "lab", NID: "n1", Epoch: 4}}
		}, ReasonDomainCensus},
	} {
		changed := facts
		defect.mutate(&changed)
		if rows, err := censusRows(changed, owner, 4, "source"); refusalDetail(err) != defect.reason {
			t.Errorf("%s: census %+v err=%v, want %s", name, rows, err, defect.reason)
		}
	}
}

// The authority map is every session's creation incarnation from the current
// store. A session recreated under the same SID has another incarnation; an
// unreadable creation identity refuses the whole map instead of omitting it.
// origin: hot-update S8 plan §6.1 / §6.2 / §11.2 P17; S8 BROKER-EXPORTER R-11
func TestSessionAuthoritiesNameEveryCreation(t *testing.T) {
	f := newCutFixture(t)
	first, err := SessionAuthorities(f.db)
	if err != nil || len(first) != 1 || first[0].SID != "lab" || len(first[0].Incarnation) != 64 {
		t.Fatalf("authority map: %+v %v", first, err)
	}
	hash, _ := testPINHash()
	if _, err := session.Create(f.db, "alpha", "alpha", "owner-fp", hash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	both, err := SessionAuthorities(f.db)
	if err != nil || len(both) != 2 || both[0].SID != "alpha" || both[1] != first[0] {
		t.Fatalf("authority map lost or reordered a session: %+v %v", both, err)
	}
	if _, err := f.db.Exec(`DELETE FROM sessions WHERE sid = 'lab'`); err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(f.db, "lab", "lab", "owner-fp", hash, time.Now().UTC().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	recreated, err := SessionAuthorities(f.db)
	if err != nil || len(recreated) != 2 || recreated[1].SID != "lab" || recreated[1].Incarnation == first[0].Incarnation {
		t.Fatalf("a recreated session kept its predecessor's incarnation: %+v %v", recreated, err)
	}
	if upgradecut.MatchAuthorities(both, recreated) == nil {
		t.Fatal("authority comparison accepts a recreated session")
	}
	if _, err := SessionAuthorities(nil); refusalDetail(err) != ReasonIncarnationUnknown {
		t.Fatalf("no store described as an empty broker: %v", err)
	}
	// One session whose creation identity cannot be read refuses the whole
	// map: a carried map without it would let that session vanish in the cut.
	if _, err := f.db.Exec(`UPDATE sessions SET pin_hash = 'not-a-phc-hash' WHERE sid = 'alpha'`); err != nil {
		t.Fatal(err)
	}
	if got, err := SessionAuthorities(f.db); refusalDetail(err) != ReasonIncarnationUnknown {
		t.Fatalf("a session without a creation identity was omitted: %+v %v", got, err)
	}
	if err := f.db.Close(); err != nil {
		t.Fatal(err)
	}
	if got, err := SessionAuthorities(f.db); refusalDetail(err) != ReasonIncarnationUnknown {
		t.Fatalf("an unreadable store described as %+v: %v", got, err)
	}
}

// Broker Preflight binds the grant to the running bytes and the published
// target, checks all three readers including the S8 features, and refuses a
// broker the S6 cut could not carry — each with its public reason.
// origin: hot-update S8 plan §5.2 / §6.1 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
func TestBrokerPreflightRefusesWhatItCannotCarry(t *testing.T) {
	for name, defect := range map[string]struct {
		mutate func(*cutFixture)
		reason string
	}{
		"no broker attached":  {func(f *cutFixture) { f.runtime.port.Store(nil) }, ReasonBrokerCutUnavailable},
		"running bytes moved": {func(f *cutFixture) { f.running = strings.Repeat("e", 64) }, ReasonRunningImageChanged},
		"S7-era candidate without the S8 adapters": {func(f *cutFixture) {
			f.candidate = func(sha string) upgradecompat.Reader {
				r := brokerReaderFor(sha, "v2")
				r.Features, r.Domains = nil, r.Domains[:4]
				return r
			}
		}, "reader_capability_missing"},
		"candidate without transfer-v1 beside a transfer owner": {func(f *cutFixture) {
			f.candidate = func(sha string) upgradecompat.Reader {
				r := brokerReaderFor(sha, "v2")
				r.Domains = []string{"exec-v1", "flow-v1", "broker-image-v1", "upgrade-cut-v1"}
				return r
			}
		}, "reader_domain_missing"},
		"target pin replaced": {func(f *cutFixture) {
			f.journal.view.Metadata.Pins = []string{f.sourcePath}
		}, ReasonArtifactUnavailable},
		"legacy transfer in flight": {func(f *cutFixture) { f.facts.Unsettled = 1 }, ReasonTransfersInFlight},
		"another cut reserved": {func(f *cutFixture) {
			f.runtime.held = &heldCut{handle: upgradetx.Handle{TxID: strings.Repeat("9", 32), CutID: strings.Repeat("9", 32)}}
		}, ReasonUpgradeInProgress},
		"store authority unreadable": {func(f *cutFixture) { _ = f.db.Close() }, ReasonIncarnationUnknown},
	} {
		t.Run(name, func(t *testing.T) {
			f := newCutFixture(t)
			defect.mutate(f)
			proof, err := f.runtime.Preflight(t.Context(), f.grant, upgradeartifact.Receipt{})
			if refusalDetail(err) != defect.reason {
				t.Fatalf("preflight %+v err=%v, want %s", proof.Census, err, defect.reason)
			}
		})
	}
	f := newCutFixture(t)
	proof, err := f.runtime.Preflight(t.Context(), f.grant, upgradeartifact.Receipt{})
	if err != nil || !proof.Census.Known || len(proof.Census.Resources) != 5 {
		t.Fatalf("carriable broker refused: %+v %v", proof.Census, err)
	}
	for _, row := range proof.Census.Resources {
		if row.Owner != f.owner || row.Generation != 1 || row.Custody != "source" {
			t.Fatalf("source preflight claimed another generation's custody: %+v", row)
		}
	}
}

// A Start preflights three times inside one five-second request budget; the
// candidate is executed once per verified digest, never once per preflight.
// A probe that does not bind the digest it was asked for is refused and never
// kept — not even for the grant whose digest it claimed — and a different
// candidate digest is probed again.
// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 BROKER-EXPORTER R-11 (real-cut finding)
func TestBrokerPreflightProbesEachCandidateDigestOnce(t *testing.T) {
	f := newCutFixture(t)
	other, otherSHA := publishPin(t, f.dir, "broker image C")
	f.journal.view.Metadata.Pins = append(f.journal.view.Metadata.Pins, other)
	first, second := f.grant, f.grant
	second.TargetSHA = otherSHA
	use := func(g upgradetx.Grant) {
		f.journal.view.Historical.Grant, f.journal.view.Historical.Forward.TargetSHA = g, g.TargetSHA
	}
	probe := f.runtime.host.Reader
	probes := 0
	lie := false
	f.runtime.host.Reader = func(path string) (upgradecompat.Reader, error) {
		probes++
		r, err := probe(path)
		if lie {
			r.SHA256 = otherSHA // the first slot's declarations, claiming the second's digest
		}
		return r, err
	}
	lie = true
	if _, err := f.runtime.Preflight(t.Context(), first, upgradeartifact.Receipt{}); refusalDetail(err) != ReasonArtifactMismatch {
		t.Fatalf("probe of other bytes admitted: %v", err)
	}
	lie = false
	use(second)
	for range 3 {
		if _, err := f.runtime.Preflight(t.Context(), second, upgradeartifact.Receipt{}); err != nil {
			t.Fatal(err)
		}
	}
	if probes != 2 {
		t.Fatalf("candidate executed %d times for one digest (a mismatched probe was kept or the verified one re-run)", probes)
	}
	use(first)
	if _, err := f.runtime.Preflight(t.Context(), first, upgradeartifact.Receipt{}); err != nil || probes != 3 {
		t.Fatalf("a different candidate digest reused another probe: probes=%d %v", probes, err)
	}
}

// Only the process that imported this exact cut, running its target bytes
// and serving its image, signs Control — and only after one real request
// crossed its business subscriptions with the sample's nonce. The source, a
// target whose probe fails, and a settled attempt never sign it. Released is
// likewise signed only by the importer.
// origin: hot-update S8 plan §2.1 / §4.2 / M05 / M06 / §11.2 P15 P17; S8 BROKER-EXPORTER R-11
func TestOnlyTheServingImporterSignsControl(t *testing.T) {
	target := func(f *cutFixture) {
		f.running = f.grant.TargetSHA
		attempt := &f.journal.view.Historical.Forward
		attempt.Phase, attempt.CutStage = upgradetx.PhaseActivating, upgradetx.CutPartialActive
		state := upgradecut.State{Attempt: attempt.Handle, Owner: f.owner}
		f.runtime.host.Inherited = &state
	}
	for _, tc := range []struct {
		name    string
		mutate  func(*cutFixture)
		control bool
		stage   upgradetx.CutStage
	}{
		{"serving importer", func(*cutFixture) {}, true, upgradetx.CutActive},
		{"source process", func(f *cutFixture) { f.runtime.host.Inherited = nil; f.running = f.grant.SourceSHA }, false, upgradetx.CutPartialActive},
		{"another cut imported", func(f *cutFixture) { f.runtime.host.Inherited.Attempt.CutID = strings.Repeat("8", 32) }, false, upgradetx.CutPartialActive},
		{"source bytes running", func(f *cutFixture) { f.running = f.grant.SourceSHA }, false, upgradetx.CutPartialActive},
		{"image not serving", func(f *cutFixture) { f.facts.Serving = false }, false, upgradetx.CutPartialActive},
		{"business probe unanswered", func(f *cutFixture) { f.control = func(string) error { return errors.New("timeout") } }, false, upgradetx.CutActive},
		// The importer that cannot yet enumerate its owners, and a source whose
		// cut is armed, answer a retryable deferral (stage ""), never an unknown
		// current view that would end a waiting ctl as 74.
		{"census gap", func(f *cutFixture) { f.facts.Store = StoreIdentity{} }, false, ""},
		{"source with an armed cut", func(f *cutFixture) {
			f.runtime.host.Inherited = nil
			f.running = f.grant.SourceSHA
			f.runtime.armed.Store(&armedCut{})
		}, false, ""},
		{"attempt already completed", func(f *cutFixture) {
			f.journal.view.Historical.Forward.Phase = upgradetx.PhaseCompleted
		}, false, upgradetx.CutActive},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newCutFixture(t)
			target(f)
			var probed []string
			f.control = func(n string) error { probed = append(probed, n); return nil }
			tc.mutate(f)
			stamp := upgradetx.Stamp{Attempt: f.grant.Handle, Producer: f.owner, Nonce: strings.Repeat("7", 32), ScopeDigest: f.grant.ScopeDigest}
			o, err := f.runtime.Observe(stamp)
			if tc.stage == "" {
				var typed *upgradetx.Error
				if !errors.As(err, &typed) || typed.Code != upgradetx.CodeDeferred || typed.Accepted || typed.Detail != ReasonUpgradeInProgress {
					t.Fatalf("cut window answered %+v %v, want a retryable deferral", o.Current, err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			signed := o.Current.Control.Kind == upgradetx.ReceiptControlReady
			if signed != tc.control || o.Current.CutStage != tc.stage {
				t.Fatalf("control=%t stage=%s, want control=%t stage=%s", signed, o.Current.CutStage, tc.control, tc.stage)
			}
			if signed && (len(probed) != 1 || probed[0] != stamp.Nonce || o.Current.Control.Census.Stamp != o.Census.Stamp) {
				t.Fatalf("control receipt not bound to this sample's probe: probes=%v", probed)
			}
			released, err := f.runtime.Retire(f.grant.Handle)
			importer := f.runtime.host.Inherited != nil && upgradetx.SameCut(f.runtime.host.Inherited.Attempt, f.grant.Handle) &&
				f.running == f.grant.TargetSHA && f.facts.Store.Path != ""
			if (err == nil) != importer || err == nil && (released.Kind != upgradetx.ReceiptOldGenerationReleased || released.RunningSHA != f.grant.TargetSHA) {
				t.Fatalf("released receipt importer=%t: %+v %v", importer, released.Kind, err)
			}
			if err == nil {
				for _, row := range released.Census.Resources {
					if row.Generation != f.grant.Handle.Generation || row.Custody != "target" {
						t.Fatalf("released census keeps a source-generation row: %+v", row)
					}
				}
			}
		})
	}
}

// upgrade.describe enumerates today's broker as the original scope of a new
// grant, and refuses rather than describing a partial or busy broker.
// origin: hot-update S8 plan §4.2 / M06 / M14 / §11.2 P15; S8 BROKER-EXPORTER R-11
func TestBrokerDescribeRefusesPartialOrBusyScope(t *testing.T) {
	settled := func(f *cutFixture) {
		f.journal.view.Historical.Forward.Phase = upgradetx.PhaseCompleted
	}
	f := newCutFixture(t)
	settled(f)
	d, err := f.runtime.DescribeRuntime(strings.Repeat("6", 32))
	if err != nil || d.RunningSHA != f.grant.SourceSHA || d.SourceRelease != "v1" || len(d.Scope.Domains) != 11 || len(d.Scope.Expected) != 5 {
		t.Fatalf("description of an idle broker: %+v %v", d, err)
	}
	for _, row := range d.Scope.Expected {
		if row.Generation != f.grant.Handle.Generation || row.Custody != "source" {
			t.Fatalf("description names another generation: %+v", row)
		}
	}
	for name, defect := range map[string]struct {
		mutate func(*cutFixture)
		reason string
	}{
		"nonce":             {func(*cutFixture) {}, "describe_nonce_invalid"},
		"no broker":         {func(f *cutFixture) { f.runtime.port.Store(nil) }, ReasonDomainCensus},
		"unsettled attempt": {func(f *cutFixture) { f.journal.view.Historical.Forward.Phase = upgradetx.PhaseActivating }, ReasonUpgradeInProgress},
		"reserved cut": {func(f *cutFixture) {
			f.runtime.held = &heldCut{handle: f.grant.Handle}
		}, ReasonUpgradeInProgress},
		"in-flight legacy transfer": {func(f *cutFixture) { f.facts.Unsettled = 2 }, ReasonTransfersInFlight},
		"unknown tracker slot":      {func(f *cutFixture) { f.facts.Trackers[0].EntryNonce = "" }, ReasonDomainCensus},
	} {
		f := newCutFixture(t)
		settled(f)
		defect.mutate(f)
		nonce := strings.Repeat("6", 32)
		if name == "nonce" {
			nonce = "short"
		}
		if d, err := f.runtime.DescribeRuntime(nonce); refusalDetail(err) != defect.reason {
			t.Errorf("%s: described %+v err=%v, want %s", name, d.Scope, err, defect.reason)
		}
	}
}
