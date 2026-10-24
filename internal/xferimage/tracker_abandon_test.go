package xferimage

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercapacity"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// The real decision producer uses AuditTransfer.Code. A private reason field
// in an image fixture hides a refusal of the actual abandonment terminal.
// origin: S7 internal review rv-recovery F08 / FIX-TERM codec handoff
func TestTransferAbandonedTrackerPassesCurrentCodec(t *testing.T) {
	warmAbandonedTrackerPoller(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseAbandonedTrackerCodec(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "abandoned tracker codec", g)
	testharness.AssertNoFDLeak(t, "abandoned tracker codec", fd, 0)
}

func exerciseAbandonedTrackerCodec(t *testing.T) {
	t.Helper()
	f := newAbandonedTrackerFixture(t, "b", "push", false)
	abandoner, ok := f.manager.(interface {
		Abandon(x.Identity, x.Deadline) (x.Status, error)
	})
	if !ok {
		t.Fatal("real tracker lacks administrative abandonment")
	}
	if _, err := abandoner.Abandon(f.permit.Identity, abandonedTrackerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	state := freezeAbandonedTracker(t, f.manager)
	if len(state.Entries) != 1 || state.Entries[0].CanonicalTerminal == nil {
		t.Fatal("real tracker did not retain its abandonment decision")
	}
	if _, err := EncodeTracker(state); err != nil {
		t.Fatalf("real abandoned tracker codec refused: %v; terminal=%s; census rows=%d entries=%d; admission=%t evidence=%t phase=%t terminal=%t values=%v primitive=%q",
			err, state.Entries[0].CanonicalTerminal.Bytes, len(state.Census.Rows), len(state.Entries),
			trackerRowAdmission(state.Entries[0], state.Origin), trackerRowEvidence(state.Entries[0]),
			trackerRowPhase(state.Entries[0]), trackerRowTerminal(state.Entries[0], state.Origin),
			trackerValues(reflect.ValueOf(state), state.Origin.BootID), state.Entries[0].ReceiverOutcome.Primitive)
	}
}

// Pending publications must advance in the imported owner, then cross another
// fresh current cut with the same terminal and original receiver evidence.
// origin: S7 internal review rv-recovery F08 / FIX-TERM codec handoff
// origin: hot-update S7 plan §5.1 / §8.2 / lead LQ1 / T20
func TestTransferAbandonedTrackerRoundtripKeepsCurrentEvidence(t *testing.T) {
	warmAbandonedTrackerPoller(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseAbandonedTrackerRoundtrip(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "abandoned tracker current replay", g)
	testharness.AssertNoFDLeak(t, "abandoned tracker current replay", fd, 0)
}

func warmAbandonedTrackerPoller(t *testing.T) {
	t.Helper()
	// Warm the network poller before the exact FD baseline.
	warm, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err = warm.Close(); err != nil {
		t.Fatal(err)
	}
}

func exerciseAbandonedTrackerRoundtrip(t *testing.T) {
	t.Helper()
	for _, tier := range []string{"a", "b"} {
		for _, verb := range []string{"push", "pull"} {
			for _, landed := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%s/landed=%t", tier, verb, landed), func(t *testing.T) {
					exerciseAbandonedTrackerCurrentImage(t, tier, verb, landed)
				})
			}
		}
	}
}

func exerciseAbandonedTrackerCurrentImage(t *testing.T, tier, verb string, landed bool) {
	t.Helper()
	f := newAbandonedTrackerFixture(t, tier, verb, true)
	late := x.ReceiverOutcome{Grant: f.grant, Primitive: x.PrimitiveRename, Outcome: x.OutcomeLanded,
		Bytes: f.intent.Size, SHA256: f.intent.SHA256, ParentSynced: true}
	if landed {
		unresolved := late
		unresolved.ParentSynced = false
		if _, err := f.manager.Finish(f.permit.Identity, unresolved, abandonedTrackerDeadline(t)); err == nil || !strings.Contains(err.Error(), x.CodeDurabilityUnknown) {
			t.Fatalf("real tracker lost landed-but-unsynced evidence: %v", err)
		}
	}
	if _, err := abandonTracker(f.manager, f.permit.Identity, abandonedTrackerDeadline(t)); err == nil || !strings.Contains(err.Error(), x.CodeResultUnknown) {
		t.Fatalf("missing history binding acknowledged abandonment: %v", err)
	}
	original := freezeAbandonedTracker(t, f.manager)
	row := original.Entries[0]
	if row.CanonicalTerminal == nil || !row.OutcomeUnknown || row.CanonicalTerminal.Kind != "failed" ||
		row.TerminalDelivery != x.DeliveryPending || len(original.PendingTerminal) != 1 ||
		len(original.PendingStart) != 1 || len(original.GrantSlots) != 1 || len(original.Cleanup) != 1 {
		t.Fatal("fixture lost its pending decision, grant or cleanup responsibility")
	}
	if row.ReceiverOutcome == nil || row.ReceiverOutcome.Grant != f.grant || row.ReceiverOutcome.ParentSynced ||
		landed && (row.ReceiverOutcome.Outcome != x.OutcomeLanded || row.ReceiverOutcome.Bytes != late.Bytes ||
			row.ReceiverOutcome.SHA256 != late.SHA256 || row.ReceiverOutcome.Primitive != late.Primitive) ||
		!landed && (row.ReceiverOutcome.Outcome != x.OutcomeInflight || row.ReceiverOutcome.Primitive != "") {
		t.Fatal("abandonment invented or erased original primitive evidence")
	}
	state := original
	for generation := uint64(2); generation <= 3; generation++ {
		encoded, err := EncodeTracker(state)
		if err != nil {
			t.Fatalf("encode current generation %d: %v", state.Generation, err)
		}
		decoded, err := DecodeTracker(encoded)
		if err != nil || !reflect.DeepEqual(state, decoded) {
			t.Fatalf("sealed codec changed abandonment evidence: %v", err)
		}
		cfg := f.cfg
		cfg.History = f.history
		restored, err := xfertrack.Import(cfg, decoded)
		if err != nil {
			t.Fatal(err)
		}
		if err = f.manager.Stop(abandonedTrackerDeadline(t)); err != nil {
			t.Fatal(err)
		}
		f.manager = restored
		if err = restored.Bind(x.Bindings{Ledger: cfg.Ledger, OwnerGeneration: generation}, abandonedTrackerDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if _, err = abandonTracker(restored, row.Identity, abandonedTrackerDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if _, err = restored.Finish(row.Identity, late, abandonedTrackerDeadline(t)); err == nil || !strings.Contains(err.Error(), x.CodeTerminalConflict) {
			t.Fatalf("late complete replaced canonical abandonment: %v", err)
		}
		status, err := restored.Status(x.StatusRequest{Identity: row.Identity, Origin: row.Origin}, abandonedTrackerDeadline(t))
		if err != nil || status.State != x.StatusTerminal || status.Terminal == nil || status.Terminal.Kind != "failed" ||
			!status.Terminal.OutcomeUnknown || status.Terminal.Code != x.ReasonSettlementAbandoned ||
			status.Terminal.CanonicalDigest != row.CanonicalTerminal.Digest || status.Terminal.TerminalID != f.grant.TerminalID {
			t.Fatalf("current image changed terminal truth: %+v %v", status, err)
		}
		current := freezeAbandonedTracker(t, restored)
		got := current.Entries[0]
		if current.Generation != generation || got.Identity != row.Identity || got.Origin != row.Origin ||
			!reflect.DeepEqual(got.CanonicalTerminal, row.CanonicalTerminal) || !reflect.DeepEqual(got.CanonicalStart, row.CanonicalStart) ||
			!reflect.DeepEqual(got.ReceiverOutcome, row.ReceiverOutcome) || !reflect.DeepEqual(got.Grant, row.Grant) ||
			got.StartedAt != row.StartedAt || got.BudgetSeal != row.BudgetSeal || !reflect.DeepEqual(got.Cleanup, row.Cleanup) ||
			!reflect.DeepEqual(current.WatchdogDeadlines, original.WatchdogDeadlines) ||
			got.Phase != x.PhaseHistoryConfirmed || len(got.HistoryReceipts) != 2 ||
			len(current.PendingStart) != 0 || len(current.PendingTerminal) != 0 {
			t.Fatal("current cut erased immutable evidence, original deadline or publication progress")
		}
		if generation == 2 && got.Revision <= row.Revision || generation == 3 && !reflect.DeepEqual(got, state.Entries[0]) {
			t.Fatal("current image replay reset or rewrote the history-confirmed revision")
		}
		census, err := cfg.Ledger.Scan(abandonedTrackerDeadline(t))
		if err != nil || !census.Ready || !census.Complete || len(census.Rows) != 1 || !reflect.DeepEqual(census.Rows[0], got) {
			t.Fatalf("current image disagrees with the real retained ledger: %v", err)
		}
		assertAbandonedTrackerHistory(t, f.stream, got)
		state = current
	}
}

func abandonTracker(manager xfertrack.Manager, id x.Identity, deadline x.Deadline) (x.Status, error) {
	abandoner, ok := manager.(interface {
		Abandon(x.Identity, x.Deadline) (x.Status, error)
	})
	if !ok {
		return x.Status{}, errors.New("real tracker lacks administrative abandonment")
	}
	return abandoner.Abandon(id, deadline)
}

func assertAbandonedTrackerHistory(t *testing.T, stream jetstream.Stream, row x.LedgerRow) {
	t.Helper()
	info, err := stream.Info(t.Context())
	if err != nil || info.State.Msgs != 2 {
		t.Fatalf("physical history is not exactly one START and one TERMINAL: %+v %v", info, err)
	}
	for _, record := range []*x.CanonicalRecord{row.CanonicalStart, row.CanonicalTerminal} {
		var seq uint64
		for _, receipt := range row.HistoryReceipts {
			if receipt.RecordDigest == record.Digest {
				seq = receipt.Sequence
			}
		}
		msg, err := stream.GetMsg(t.Context(), seq)
		if err != nil || !bytes.Equal(msg.Data, record.Bytes) || msg.Subject != proto.SubjAuditTransfer(row.Identity.SID) {
			t.Fatalf("physical history disagrees with retained canonical bytes: %v", err)
		}
		var audit schema.AuditTransfer
		if err = json.Unmarshal(msg.Data, &audit); err != nil || audit.Kind != record.Kind ||
			record.Kind == "failed" && (audit.Code != x.ReasonSettlementAbandoned || !audit.OutcomeUnknown) {
			t.Fatalf("physical abandonment lost canonical code or unknown outcome: %v", err)
		}
		var fields map[string]json.RawMessage
		if err = json.Unmarshal(msg.Data, &fields); err != nil {
			t.Fatal(err)
		}
		if _, privateReason := fields["reason"]; privateReason {
			t.Fatal("producer added a private reason field to satisfy the codec")
		}
	}
}

// A self-consistent image digest cannot legitimize another failed reason,
// a known/complete outcome or lost retained receiver and cleanup evidence.
// origin: S7 internal review rv-recovery F08 / FIX-TERM codec handoff
// origin: hot-update S7 plan §4 / §5.1 / lead LQ1 / T20
func TestTransferTrackerAbandonmentRequiresCanonicalCodeAndEvidence(t *testing.T) {
	mutations := []struct {
		name string
		edit func(*x.TrackerState)
	}{
		{"private reason only", func(s *x.TrackerState) {
			rewriteAbandonedTrackerTerminal(s, `"code":"settlement_abandoned"`, `"reason":"settlement_abandoned"`)
		}},
		{"wrong code with matching private reason", func(s *x.TrackerState) {
			rewriteAbandonedTrackerTerminal(s, `"code":"settlement_abandoned"`, `"code":"deadline","reason":"settlement_abandoned"`)
		}},
		{"missing code", func(s *x.TrackerState) {
			rewriteAbandonedTrackerTerminal(s, `"code":"settlement_abandoned",`, "")
		}},
		{"wrong code", func(s *x.TrackerState) {
			rewriteAbandonedTrackerTerminal(s, `"code":"settlement_abandoned"`, `"code":"deadline"`)
		}},
		{"missing unknown flag", func(s *x.TrackerState) {
			rewriteAbandonedTrackerTerminal(s, `,"outcome_unknown":true`, "")
		}},
		{"false unknown flag", func(s *x.TrackerState) {
			rewriteAbandonedTrackerTerminal(s, `"outcome_unknown":true`, `"outcome_unknown":false`)
		}},
		{"unknown complete", func(s *x.TrackerState) {
			rewriteAbandonedTrackerTerminal(s, `"kind":"failed"`, `"kind":"complete"`)
			s.Entries[0].CanonicalTerminal.Kind = "complete"
			s.PendingTerminal[0].Kind = "complete"
			out := s.Entries[0].ReceiverOutcome
			out.Outcome, out.Primitive, out.ParentSynced = x.OutcomeLanded, x.PrimitiveLink, true
			out.SHA256 = s.Entries[0].SourceSeal.SHA256
		}},
		{"missing terminal", func(s *x.TrackerState) {
			s.Entries[0].CanonicalTerminal, s.PendingTerminal = nil, nil
		}},
		{"receiver grant replaced", func(s *x.TrackerState) { s.Entries[0].ReceiverOutcome.Grant.GrantNonce = "replacement" }},
		{"receiver outcome invented", func(s *x.TrackerState) { s.Entries[0].ReceiverOutcome.Outcome = "invented" }},
		{"primitive invented", func(s *x.TrackerState) { s.Entries[0].ReceiverOutcome.Primitive = "invented" }},
		{"landed primitive lost", func(s *x.TrackerState) {
			s.Entries[0].ReceiverOutcome.Outcome = x.OutcomeLanded
			s.Entries[0].ReceiverOutcome.SHA256 = s.Entries[0].SourceSeal.SHA256
		}},
		{"known outcome primitive lost", func(s *x.TrackerState) {
			s.Entries[0].OutcomeUnknown = false
			s.Entries[0].ReceiverOutcome.Outcome = x.OutcomeNotEntered
			rewriteAbandonedTrackerTerminal(s, `"code":"settlement_abandoned"`, `"code":"deadline"`)
			rewriteAbandonedTrackerTerminal(s, `"outcome_unknown":true`, `"outcome_unknown":false`)
		}},
		{"pending terminal lost", func(s *x.TrackerState) { s.PendingTerminal = nil }},
		{"grant slot lost", func(s *x.TrackerState) { s.GrantSlots = nil }},
		{"cleanup owner lost", func(s *x.TrackerState) { s.Cleanup = nil }},
	}
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			s := trackerAbandonmentFixture(t)
			if _, err := EncodeTracker(s); err != nil {
				t.Fatalf("valid canonical abandonment control refused: %v", err)
			}
			mutation.edit(&s)
			trackerRefreshCensus(&s)
			if encoded, err := EncodeTracker(s); err == nil || encoded != nil {
				t.Fatal("encoder accepted malformed abandonment or lost retained evidence")
			}
			body, err := json.Marshal(s)
			if err != nil {
				t.Fatal(err)
			}
			trackerAssertRefused(t, trackerFrame(body))
		})
	}
}

func trackerAbandonmentFixture(t testing.TB) x.TrackerState {
	t.Helper()
	s := trackerFixture("b", "push")
	row := &s.Entries[0]
	id := row.Identity
	row.Grant.TerminalID = schema.TransferTerminalID(id.SID, id.TransferID, id.EntryNonce)
	s.GrantSlots[0].Grant = *row.Grant
	row.OutcomeUnknown = true
	row.ReceiverOutcome = &x.ReceiverOutcome{Grant: *row.Grant, Outcome: x.OutcomeInflight, Code: x.ReasonSettlementAbandoned}
	terminal, err := schema.CanonicalTransferAudit(schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "failed", Verb: row.Intent.Verb, Ts: row.StartedAt,
		Session: id.SID, Node: id.NID, ActorFp: id.ActorFP, TransferID: id.TransferID,
		Tier: row.Intent.Tier, AgentInstance: id.AgentInstance, EntryNonce: id.EntryNonce,
		IntentDigest: id.IntentDigest, ObjectVersion: id.ObjectVersion, TerminalID: row.Grant.TerminalID,
		Code: x.ReasonSettlementAbandoned, OutcomeUnknown: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	row.CanonicalTerminal = &terminal
	s.PendingTerminal = []x.CanonicalRecord{terminal}
	trackerRefreshCensus(&s)
	return s
}

func rewriteAbandonedTrackerTerminal(s *x.TrackerState, old, replacement string) {
	r := s.Entries[0].CanonicalTerminal
	r.Bytes = bytes.ReplaceAll(r.Bytes, []byte(old), []byte(replacement))
	r.Digest = abandonedTrackerDigest(r.Bytes)
	r.ReqID = abandonedTrackerDigest(append([]byte("xferaudit-record:"), r.Bytes...))
	s.PendingTerminal = []x.CanonicalRecord{*r}
}

type abandonedTrackerFixture struct {
	manager xfertrack.Manager
	cfg     x.TrackerConfig
	permit  x.CreationPermit
	intent  x.Intent
	grant   x.CommitGrant
	history xferhistory.Publisher
	stream  jetstream.Stream
}

func newAbandonedTrackerFixture(t *testing.T, tier, verb string, pending bool) *abandonedTrackerFixture {
	t.Helper()
	nc, err := nats.Connect(testharness.StartJSNATS(t), nats.NoReconnect(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.CreateStream(t.Context(), jetstream.StreamConfig{
		Name: "tracker_history", Subjects: []string{proto.SubjAuditTransfer("session")},
		Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy, Discard: jetstream.DiscardNew, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	base := x.Base{Clock: func() (string, int64, error) {
		ns, err := controlbudget.Now()
		return "boot", ns, err
	}, UTC: time.Now, Capacity: xfercapacity.New("tracker-codec-")}
	ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: t.TempDir(), FallbackRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	history, err := xferhistory.New(x.HistoryConfig{JS: js, SID: "session"})
	if err != nil {
		t.Fatal(err)
	}
	intent := x.Intent{Verb: verb, Tier: tier, Path: "target", Size: 4, SHA256: abandonedTrackerDigest([]byte("new\n"))}
	if tier == "b" {
		intent.ChunkSize = 32768
	}
	intentDigest, err := proto.TransferIntentDigest(proto.Intent(intent))
	if err != nil {
		t.Fatal(err)
	}
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent",
		TransferID: "transfer", EntryNonce: "entry", IntentDigest: intentDigest}
	if tier == "b" {
		id.ObjectVersion = "version"
	}
	cfg := x.TrackerConfig{Base: base, Origin: x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1},
		Ledger: ledger, History: history}
	// With no history binding, the real tracker must stage the decision and
	// refuse its ACK; the current imported owner later gets the real publisher.
	if pending {
		cfg.History = nil
	}
	manager, err := xfertrack.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f := &abandonedTrackerFixture{manager: manager, cfg: cfg, intent: intent, history: history, stream: stream,
		permit: x.CreationPermit{Identity: id, Origin: cfg.Origin, IssuedAt: time.Now(), CreateUntil: time.Now().Add(time.Minute),
			PermitNonce: "permit", Signature: "verified-by-control"}}
	t.Cleanup(func() {
		if err := f.manager.Stop(abandonedTrackerDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	if _, err := manager.Admit(f.permit, intent, abandonedTrackerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	seal := x.SourceSeal{Size: intent.Size, SHA256: intent.SHA256, ChunkSize: intent.ChunkSize, SourceFingerprint: "pinned-source"}
	if tier == "b" {
		seal.Chunks = 1
	}
	if _, err := manager.SourceSeal(id, seal, abandonedTrackerDeadline(t)); err != nil {
		t.Fatal(err)
	}
	receiver := "agent"
	if verb == "pull" {
		receiver = "ctl"
	}
	f.grant, err = manager.Reserve(id, x.ReserveRequest{Origin: cfg.Origin, ReceiverInstance: receiver}, abandonedTrackerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func abandonedTrackerDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func freezeAbandonedTracker(t *testing.T, manager xfertrack.Manager) x.TrackerState {
	t.Helper()
	frozen, err := manager.Freeze(abandonedTrackerDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	state, resources, err := frozen.Export()
	if err != nil || len(resources) != 0 {
		t.Fatalf("tracker export lost pure ownership: resources=%d err=%v", len(resources), err)
	}
	return state
}

func abandonedTrackerDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
