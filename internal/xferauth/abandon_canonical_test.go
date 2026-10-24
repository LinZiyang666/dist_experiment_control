package xferauth

import (
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The real abandonment encoder is checked against the real ledger, history
// publisher and embedded JetStream; no fake ACK can mask rejection before
// publication. The current API requires the tracker's revision serializer.
func canonicalAbandonment(t *testing.T) (*settlement, x.LedgerRow, x.HistoryPort) {
	t.Helper()
	nc, err := nats.Connect(testharness.StartJSNATS(t), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	if err = jsstream.EnsureHistoryStream(t.Context(), js, "lab", 1); err != nil {
		t.Fatal(err)
	}
	base := x.Base{Clock: recoveryClock, UTC: time.Now, Capacity: &recoveryCapacity{}}
	history, err := xferhistory.New(x.HistoryConfig{Base: base, JS: js, SID: "lab"})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: filepath.Join(root, "primary"), FallbackRoot: filepath.Join(root, "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	id := x.Identity{SID: "lab", NID: "node", TransferID: "transfer", ActorFP: "actor", AgentInstance: "agent", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "version"}
	origin := x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	tracker, err := xfertrack.New(x.TrackerConfig{Base: base, Origin: origin, Ledger: ledger, History: history})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := tracker.Stop(authDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	now := time.Now().UTC()
	permit := x.CreationPermit{Identity: id, Origin: origin, PermitNonce: "permit", Signature: "verified", IssuedAt: now, CreateUntil: now.Add(time.Minute)}
	if _, err = tracker.Admit(permit, x.Intent{Verb: "pull", Tier: "b", Path: "destination", ChunkSize: 32768}, authDeadline(t)); err != nil {
		t.Fatal(err)
	}
	census, err := ledger.Scan(authDeadline(t))
	if err != nil || len(census.Rows) != 1 {
		t.Fatalf("setup admission: %v", err)
	}
	return &settlement{cfg: SettlementConfig{Tracker: tracker, Ledger: ledger, UTC: time.Now}}, census.Rows[0], history
}

// origin: S7 internal review rv-wiring F4
func exerciseTransferAbandonPersistsWithRealLedger(t *testing.T) {
	s, row, _ := canonicalAbandonment(t)
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.abandon(row, d); err != nil {
		t.Fatalf("LQ1 producer cannot persist into real ledger: %v", err)
	}
	census, err := s.cfg.Ledger.Scan(d)
	if err != nil || len(census.Rows) != 1 || !census.Rows[0].OutcomeUnknown || census.Rows[0].CanonicalTerminal.Kind != "failed" {
		t.Fatalf("missing durable unknown abandonment: %+v %v", census, err)
	}
}

// origin: S7 internal review rv-wiring F4
func exerciseTransferAbandonProducesPublishableCanonical(t *testing.T) {
	s, row, publisher := canonicalAbandonment(t)
	d, err := controlbudget.After(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.abandon(row, d); err != nil {
		t.Fatal(err)
	}
	census, err := s.cfg.Ledger.Scan(d)
	if err != nil || len(census.Rows) != 1 {
		t.Fatal(err)
	}
	canonical := census.Rows[0].CanonicalTerminal
	if _, err = publisher.Confirm(*canonical, d); err != nil {
		t.Fatalf("operator abandonment is rejected before history IO: %v; canonical=%s", err, canonical.Bytes)
	}
	set, err := publisher.Lookup(x.Key{SID: row.Identity.SID, TransferID: row.Identity.TransferID}, d)
	if err != nil || len(set.Terminal) != 1 || set.Terminal[0].Digest != canonical.Digest {
		t.Fatalf("physical abandonment is not exact: %+v %v", set, err)
	}
}

// origin: S7 internal review rv-wiring F4
func TestTransferAbandonPersistsWithRealLedger(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTransferAbandonPersistsWithRealLedger(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}

// origin: S7 internal review rv-wiring F4
func TestTransferAbandonProducesPublishableCanonical(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTransferAbandonProducesPublishableCanonical(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}
