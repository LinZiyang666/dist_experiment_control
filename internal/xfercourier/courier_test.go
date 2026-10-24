package xfercourier

// Real: the courier, its capacity reservations, snapshots and join barriers.
// Controlled: transport responses and a boot clock in the unit tests.
// Integration: embedded JS plus the real P14/P19 factories. Missing factories
// must fail rather than letting inert P00 witnesses certify integration.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// origin: hot-update S7 plan §4 / §5.2 / T16
func TestTransferCourierRequiresExactHistoryReceipt(t *testing.T) {
	for _, kind := range []string{"complete", "failed"} {
		t.Run(kind, func(t *testing.T) {
			mutations := []struct {
				name   string
				mutate func(*xfercontract.TerminalReceipt)
			}{
				{"opposite", func(r *xfercontract.TerminalReceipt) {
					if r.Kind == "complete" {
						r.Kind = "failed"
					} else {
						r.Kind = "complete"
					}
				}},
				{"terminal_slot", func(r *xfercontract.TerminalReceipt) { r.TerminalID = strings.Repeat("a", 64) }},
				{"canonical_digest", func(r *xfercontract.TerminalReceipt) { r.CanonicalDigest = strings.Repeat("b", 64) }},
				{"request_id", func(r *xfercontract.TerminalReceipt) { r.ReqID = "foreign-request" }},
				{"missing_history", func(r *xfercontract.TerminalReceipt) { r.HistoryStream = ""; r.HistorySeq = 0 }},
				{"foreign_history", func(r *xfercontract.TerminalReceipt) { r.HistoryStream = "history-other" }},
				{"zero_sequence", func(r *xfercontract.TerminalReceipt) { r.HistorySeq = 0 }},
				{"bytes", func(r *xfercontract.TerminalReceipt) { r.Bytes++ }},
				{"sha", func(r *xfercontract.TerminalReceipt) { r.SHA256 = strings.Repeat("c", 64) }},
				{"code", func(r *xfercontract.TerminalReceipt) { r.Code = "invented" }},
				{"abandoned", func(r *xfercontract.TerminalReceipt) { r.OutcomeUnknown = true }},
			}
			for _, m := range mutations {
				t.Run(m.name, func(t *testing.T) {
					f := newCourierFixture(t, kind)
					bad := f.receipt
					m.mutate(&bad)
					if err := f.owner.Acknowledge(bad); err == nil {
						t.Fatal("non-exact history retired courier")
					}
					c := f.owner.(*courier)
					c.mu.Lock()
					stopped, receipt := c.state.Stopped, c.state.Receipt
					c.mu.Unlock()
					if stopped || receipt != nil {
						t.Fatal("rejected ACK changed delivery responsibility")
					}
				})
			}
			f := newCourierFixture(t, kind)
			if err := f.owner.Acknowledge(f.receipt); err != nil {
				t.Fatal(err)
			}
			if err := f.owner.Acknowledge(f.receipt); err != nil {
				t.Fatalf("exact replay: %v", err)
			}
			for round := 0; round < 5; round++ {
				f.advance(10 * time.Second)
				if err := f.owner.Step(f.deadline()); err != nil {
					t.Fatal(err)
				}
			}
			if f.transport.count() != 0 {
				t.Fatal("terminal sent after exact ACK")
			}
			state, err := f.owner.Freeze(f.deadline())
			if err != nil || !state.Stopped || state.Receipt == nil || *state.Receipt != f.receipt {
				t.Fatalf("lost exact ACK: %v", err)
			}
			duplicate := f.receipt
			duplicate.HistorySeq++
			if err := f.owner.Acknowledge(duplicate); !errors.Is(err, ErrConflict) {
				t.Fatalf("duplicate physical terminal accepted: %v", err)
			}
			if _, err := f.owner.Freeze(f.deadline()); !errors.Is(err, ErrConflict) {
				t.Fatalf("conflicting evidence silently exported: %v", err)
			}
		})
	}
	t.Run("canonical_not_received", func(t *testing.T) {
		f := newCourierFixture(t, "complete")
		owner, err := New(f.config)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			if err := owner.Stop(f.deadline()); err != nil {
				t.Error(err)
			}
		}()
		if err := owner.Acknowledge(f.receipt); !errors.Is(err, ErrReconcile) {
			t.Fatalf("guessed broker canonical decision: %v", err)
		}
	})
	t.Run("primitive_unresolved", func(t *testing.T) {
		for _, outcome := range []xfercontract.PrimitiveOutcome{xfercontract.OutcomeInflight, xfercontract.OutcomeLanded} {
			f := newCourierFixture(t, "complete")
			f.config.Outcome.Outcome = outcome
			f.config.Outcome.ParentSynced = false
			owner, err := New(f.config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := owner.Stop(f.deadline()); err != nil {
					t.Error(err)
				}
			})
			if err := BindCanonical(owner, f.record); !errors.Is(err, ErrConflict) {
				t.Fatalf("unresolved outcome accepted canonical completion: %v", err)
			}
			if err := owner.Acknowledge(f.receipt); !errors.Is(err, ErrConflict) {
				t.Fatalf("unknown primitive/durability acknowledged: %v", err)
			}
			if err := owner.Stop(f.deadline()); err != nil {
				t.Fatal(err)
			}
		}
	})
}

// origin: hot-update S7 plan §1.2 / §4 / T16
func TestTransferCourierRetriesOriginalOutcomeAfterLostACK(t *testing.T) {
	f := newCourierFixture(t, "failed")
	f.transport.handle = func(xfercontract.Request, xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
		return xfercontract.Response{}, xfercontract.Attempt{Sent: true, Class: "sent_unknown"}, errors.New("ACK lost")
	}
	for ordinal := uint32(1); ordinal <= 4; ordinal++ {
		if err := f.owner.Step(f.deadline()); err == nil {
			t.Fatal("lost ACK treated as settled")
		}
		state, err := f.owner.Freeze(f.deadline())
		if err != nil {
			t.Fatal(err)
		}
		if state.Stopped || state.Receipt != nil || state.Outcome != f.config.Outcome || state.Capsule != f.config.Capsule ||
			!bytes.Equal(state.Canonical.Bytes, f.record.Bytes) || state.Attempt.Ordinal != ordinal || !state.Attempt.Sent {
			t.Fatal("lost ACK discarded or changed original responsibility")
		}
		wantBackoff := retryDelay(ordinal)
		if state.Attempt.BackoffNS != int64(wantBackoff) || state.NextAttempt.BootID != "courier-boot" ||
			int64(state.NextAttempt.Deadline) != f.now.Load()+int64(wantBackoff) {
			t.Fatal("retry was renewed or not boot-bound")
		}
		before := f.transport.count()
		if err := f.owner.Step(f.deadline()); err != nil || f.transport.count() != before {
			t.Fatal("retry ignored backoff")
		}
		f.advance(wantBackoff)
	}
	want, _ := json.Marshal(f.config.Outcome)
	for _, sent := range f.transport.requests() {
		got, _ := json.Marshal(sent.request.Outcome)
		if !bytes.Equal(got, want) || sent.request.CallID != f.receipt.TerminalID || sent.request.Identity != f.config.Identity ||
			sent.request.Origin != f.config.Capsule.Origin || sent.request.Op != xfercontract.OpTerminal ||
			sent.deadline-sent.at != xfercontract.Deadline(settlementAttempt) {
			t.Fatal("retry changed request or exceeded settlement attempt")
		}
	}
	// Byte budget has long expired; settlement is still attempted, using the
	// same failed decision through the already capsule-bound transport.
	f.advance(time.Hour)
	f.transport.handle = func(xfercontract.Request, xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
		copyReceipt := f.receipt
		return xfercontract.Response{Receipt: &copyReceipt}, xfercontract.Attempt{Sent: true}, nil
	}
	if err := f.owner.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if err := f.owner.Step(f.deadline()); err != nil || f.transport.count() != 5 {
		t.Fatal("ACK did not close retry admission")
	}
	for _, observation := range f.observations() {
		encoded, _ := json.Marshal(observation)
		for _, private := range []string{f.config.Capsule.ReceiverNkey, f.config.Capsule.Nonce, f.config.Capsule.Signature, f.config.Outcome.Error, string(f.record.Bytes)} {
			if private != "" && bytes.Contains(encoded, []byte(private)) {
				t.Fatal("private settlement data entered observations")
			}
		}
	}
}

// origin: hot-update S7 plan §4 / §8.1 / T16
func TestTransferCourierCanonicalBindingIsImmutable(t *testing.T) {
	f := newCourierFixture(t, "complete")
	owner, err := New(f.config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := owner.Stop(f.deadline()); err != nil {
			t.Error(err)
		}
	}()
	row := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: f.record.Kind, Session: f.config.Identity.SID,
		Node: f.config.Identity.NID, ActorFp: f.config.Identity.ActorFP, TransferID: "foreign-transfer", Bytes: f.config.Outcome.Bytes,
		SHA256: f.config.Outcome.SHA256, Code: f.config.Outcome.Code}
	foreign := f.record
	foreign.Bytes, err = json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(foreign.Bytes)
	foreign.Digest = hex.EncodeToString(sum[:])
	if err := BindCanonical(owner, foreign); !errors.Is(err, ErrConflict) {
		t.Fatalf("foreign canonical payload adopted: %v", err)
	}
	f.capacity.refuse.Store(true)
	if err := BindCanonical(owner, f.record); err == nil {
		t.Fatal("canonical bytes installed without capacity")
	}
	f.capacity.refuse.Store(false)
	if err := BindCanonical(owner, f.record); err != nil {
		t.Fatal(err)
	}
	input := f.record
	input.Bytes = bytes.Clone(f.record.Bytes)
	second, err := New(f.config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := second.Stop(f.deadline()); err != nil {
			t.Error(err)
		}
	}()
	if err := BindCanonical(second, input); err != nil {
		t.Fatal(err)
	}
	input.Bytes[0] = '!'
	retained, err := second.Freeze(f.deadline())
	if err != nil || !bytes.Equal(retained.Canonical.Bytes, f.record.Bytes) {
		t.Fatal("canonical input aliases courier state")
	}
	for _, mutate := range []func(*xfercontract.CanonicalRecord){
		func(r *xfercontract.CanonicalRecord) { r.Identity.EntryNonce = "different" },
		func(r *xfercontract.CanonicalRecord) { r.Kind = "failed" },
		func(r *xfercontract.CanonicalRecord) { r.ReqID = strings.Repeat("e", 64) },
		func(r *xfercontract.CanonicalRecord) { r.Digest = strings.Repeat("d", 64) },
		func(r *xfercontract.CanonicalRecord) {
			r.Bytes = []byte("different")
			sum := sha256.Sum256(r.Bytes)
			r.Digest = hex.EncodeToString(sum[:])
		},
	} {
		record := f.record
		mutate(&record)
		if err := BindCanonical(f.owner, record); !errors.Is(err, ErrConflict) {
			t.Fatalf("canonical decision replaced: %v", err)
		}
	}
	state, err := f.owner.Freeze(f.deadline())
	if err != nil {
		t.Fatal(err)
	}
	state.Canonical.Bytes[0] = '!'
	state.Capsule.Nonce = "different"
	state.Outcome.Bytes++
	current, err := f.owner.Freeze(f.deadline())
	if err != nil || !bytes.Equal(current.Canonical.Bytes, f.record.Bytes) || current.Capsule != f.config.Capsule || current.Outcome != f.config.Outcome {
		t.Fatal("snapshot aliases live canonical bytes or settlement state")
	}
	if err := f.owner.Acknowledge(f.receipt); err != nil {
		t.Fatalf("snapshot aliases live state: %v", err)
	}
	if err := BindCanonical(f.owner, f.record); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §8.1 / §8.2 / T16
func TestTransferCourierImportPreservesDeliveryResponsibility(t *testing.T) {
	f := newCourierFixture(t, "complete")
	f.transport.handle = func(xfercontract.Request, xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
		return xfercontract.Response{}, xfercontract.Attempt{Sent: true}, errors.New("transport offline")
	}
	if err := f.owner.Step(f.deadline()); err == nil {
		t.Fatal("lost ACK unexpectedly settled")
	}
	state, err := f.owner.Freeze(f.deadline())
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*xfercontract.CourierState){
		func(s *xfercontract.CourierState) { s.Identity.EntryNonce = "different" },
		func(s *xfercontract.CourierState) { s.Outcome.Bytes++ },
		func(s *xfercontract.CourierState) { s.Capsule.Nonce = "different" },
		func(s *xfercontract.CourierState) { s.NextAttempt.BootID = "other-boot" },
		func(s *xfercontract.CourierState) { s.Canonical.Digest = strings.Repeat("d", 64) },
		func(s *xfercontract.CourierState) { s.Receipt = &f.receipt },
	} {
		bad := cloneState(state)
		mutate(&bad)
		if owner, err := Import(f.config, bad); err == nil || owner != nil {
			t.Fatal("corrupt responsibility imported")
		}
	}
	before := f.capacity.outstanding()
	owner, err := Import(f.config, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Step(f.deadline()); err != nil || f.transport.count() != 1 {
		t.Fatal("import reset original backoff")
	}
	state.Canonical.Bytes[0] = '!'
	f.advance(retryMinimum)
	if err := owner.Step(f.deadline()); err == nil || f.transport.count() != 2 {
		t.Fatal("imported retry did not resume")
	}
	if err := owner.Acknowledge(f.receipt); err != nil {
		t.Fatal(err)
	}
	settled, err := owner.Freeze(f.deadline())
	if err != nil {
		t.Fatal(err)
	}
	if settled.Attempt.Ordinal != 2 || settled.Receipt == nil || !settled.Stopped || !bytes.Equal(settled.Canonical.Bytes, f.record.Bytes) {
		t.Fatal("import lost retry or exact bytes")
	}
	if err := owner.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if f.capacity.outstanding() != before {
		t.Fatal("import leaked reservation")
	}
	owner, err = Import(f.config, settled)
	if err != nil {
		t.Fatal(err)
	}
	f.advance(time.Hour)
	if err := owner.Step(f.deadline()); err != nil || f.transport.count() != 2 {
		t.Fatal("settled import resent terminal")
	}
	if err := owner.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
}

// origin: S10 internal review NO-SKIP-AS-PASS F3
// Bind the landed P14/P19 constructors. A stub never produces evidence; the
// integration must exercise durable history and signed settlement authority.
var integrationHistoryFactory xfercontract.HistoryFactory = xferhistory.New
var integrationAuthFactory xfercontract.AuthFactory = xferauth.New

// origin: hot-update S7 plan §4 / §5.2 / §6.4 / T16
func TestTransferCourierHistoryAndSettlementIntegration(t *testing.T) {
	f := newHistoryCourierFixture(t)
	nc, err := nats.Connect(testharness.StartJSNATS(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	// ctx-root: this component test owns the embedded-server operation budget.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: historyStream(f.config.Identity.SID),
		Subjects: []string{proto.SubjAuditTransfer(f.config.Identity.SID)}, Storage: jetstream.FileStorage,
		Discard: jetstream.DiscardNew}); err != nil {
		t.Fatal(err)
	}
	issuer, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer issuer.Wipe()
	receiver, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Wipe()
	issuerPub, err := issuer.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	receiverPub, err := receiver.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	capsule := f.config.Capsule
	capsule.Issuer = issuerPub
	capsule.ReceiverNkey = receiverPub
	capsule.Signature = ""
	message, err := json.Marshal(capsule)
	if err != nil {
		t.Fatal(err)
	}
	signature, err := issuer.Sign(message)
	if err != nil {
		t.Fatal(err)
	}
	capsule.Signature = base64.RawURLEncoding.EncodeToString(signature)
	proof := xfercontract.NKeyProof{NKey: receiverPub, Nonce: []byte("original-connect-challenge")}
	proof.Signature, err = receiver.Sign(proof.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	publisher, historyErr := integrationHistoryFactory(xfercontract.HistoryConfig{Base: f.config.Base, JS: js, SID: f.config.Identity.SID})
	verifier, authErr := integrationAuthFactory(xfercontract.AuthConfig{Base: f.config.Base,
		Issuers: signedIssuer{}, Owners: retainedOwner{identity: f.config.Identity, origin: capsule.Origin, capsule: capsule}})
	if historyErr != nil || authErr != nil {
		t.Fatalf("real dependencies: history=%v auth=%v", historyErr, authErr)
	}
	t.Cleanup(func() {
		if err := verifier.Stop(f.deadline()); err != nil {
			t.Error(err)
		}
	})
	transport := &signedHistoryTransport{history: publisher, auth: verifier, capsule: capsule, proof: proof,
		record: f.record, receipt: f.receipt, loseACK: true}
	config := f.config
	config.Capsule = capsule
	config.Transport = transport
	owner, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Stop(f.deadline()); err != nil {
			t.Error(err)
		}
	})
	if err := BindCanonical(owner, f.record); err != nil {
		t.Fatal(err)
	}
	if err := owner.Step(f.deadline()); err == nil {
		t.Fatal("lost history ACK ended settlement")
	}
	f.advance(retryMinimum)
	if err := owner.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if err := owner.Step(f.deadline()); err != nil || transport.calls != 2 {
		t.Fatal("exact ACK did not stop real settlement")
	}
	set, err := publisher.Lookup(xfercontract.Key{SID: config.Identity.SID, TransferID: config.Identity.TransferID}, f.deadline())
	if err != nil || !set.Complete || len(set.Terminal) != 1 || !bytes.Equal(set.Terminal[0].Bytes, f.record.Bytes) {
		t.Fatalf("physical exact terminal count/content: %v", err)
	}
	wrong := proof
	wrong.Signature = bytes.Repeat([]byte{0}, len(proof.Signature))
	if _, err := verifier.Verify(capsule, wrong, f.deadline()); err == nil {
		t.Fatal("capsule used as bearer token")
	}
	foreign := capsule
	foreign.Identity.TransferID = "other"
	if _, err := verifier.Verify(foreign, proof, f.deadline()); err == nil {
		t.Fatal("settlement authentication crossed transfer")
	}
	opposite := f.record
	opposite.Kind = "complete"
	if _, err := publisher.Confirm(opposite, f.deadline()); err == nil {
		t.Fatal("opposite terminal passed full history guard")
	}
}

type courierFixture struct {
	owner           Courier
	config          Config
	record          xfercontract.CanonicalRecord
	receipt         xfercontract.TerminalReceipt
	now             atomic.Int64
	capacity        *courierCapacity
	transport       *courierTransport
	observationMu   sync.Mutex
	observationRows []xfercontract.Observation
}

func newCourierFixture(t *testing.T, kind string) *courierFixture {
	t.Helper()
	f := &courierFixture{capacity: &courierCapacity{live: make(map[xfercontract.ReservationID]bool)}}
	f.now.Store(int64(time.Second))
	id := xfercontract.Identity{SID: "courier-session", NID: "courier-node", ActorFP: "courier-actor", AgentInstance: "courier-agent",
		TransferID: "courier-transfer", EntryNonce: "courier-entry", IntentDigest: strings.Repeat("a", 64), ObjectVersion: "fixed-version"}
	origin := xfercontract.Origin{BrokerID: "original-broker", InstanceID: "original-instance", BootID: "origin-boot", Generation: 7}
	// Independent frozen terminal-slot golden; the product helper is not its oracle.
	const expectedTerminalID = "a3836689fbcc953e4cc8f1b675d4eb3970aeacace71217caa915f29ba8f0eb86"
	outcome := xfercontract.ReceiverOutcome{Grant: xfercontract.CommitGrant{Identity: id, GrantNonce: "original-grant",
		ReceiverInstance: "original-receiver", TerminalID: expectedTerminalID}, Primitive: xfercontract.PrimitiveRename,
		Outcome: xfercontract.OutcomeLanded, Bytes: 123, SHA256: strings.Repeat("b", 64), ParentSynced: true}
	if kind == "failed" {
		outcome.Grant = xfercontract.CommitGrant{}
		outcome.Outcome = xfercontract.OutcomeNotEntered
		outcome.ParentSynced = false
		outcome.Code = xfercontract.CodeCanceled
		outcome.Error = "private-receiver-error"
	}
	f.config = Config{Identity: id, Outcome: outcome, Capsule: xfercontract.SettlementCapsule{Identity: id, Origin: origin,
		ReceiverNkey: "private-receiver-nkey", Issuer: "capsule-issuer", Nonce: "private-capsule-nonce", Signature: "private-capsule-signature"},
		Base: xfercontract.Base{Clock: func() (string, int64, error) { return "courier-boot", f.now.Load(), nil },
			UTC: func() time.Time { return time.Unix(100, 0) }, Capacity: f.capacity,
			Observe: func(o xfercontract.Observation) {
				f.observationMu.Lock()
				f.observationRows = append(f.observationRows, o)
				f.observationMu.Unlock()
			}}}
	f.transport = &courierTransport{now: &f.now}
	f.config.Transport = f.transport
	row := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: "push", Ts: f.config.UTC(), Session: id.SID,
		Node: id.NID, ActorFp: id.ActorFP, TransferID: id.TransferID, Tier: "b", Bytes: outcome.Bytes, SHA256: outcome.SHA256, Code: outcome.Code}
	payload, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(payload)
	reqID, err := xferaudit.TransferRecordReqID(row)
	if err != nil {
		t.Fatal(err)
	}
	f.record = xfercontract.CanonicalRecord{Identity: id, Kind: kind, ReqID: reqID, Digest: hex.EncodeToString(sum[:]), Bytes: payload}
	f.receipt = xfercontract.TerminalReceipt{TerminalID: expectedTerminalID, Kind: kind, CanonicalDigest: f.record.Digest, ReqID: f.record.ReqID,
		HistoryStream: historyStream(id.SID), HistorySeq: 9, Bytes: outcome.Bytes, SHA256: outcome.SHA256, Code: outcome.Code}
	f.owner, err = New(f.config)
	if err != nil {
		t.Fatal(err)
	}
	if err := BindCanonical(f.owner, f.record); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.owner.Stop(f.deadline()); err != nil {
			t.Error(err)
		}
		if f.capacity.outstanding() != 0 {
			t.Errorf("%d courier reservations still live", f.capacity.outstanding())
		}
	})
	return f
}

func (f *courierFixture) deadline() xfercontract.Deadline {
	return xfercontract.Deadline(f.now.Load() + int64(10*time.Second))
}
func (f *courierFixture) advance(d time.Duration) { f.now.Add(int64(d)) }
func (f *courierFixture) observations() []xfercontract.Observation {
	f.observationMu.Lock()
	defer f.observationMu.Unlock()
	return append([]xfercontract.Observation(nil), f.observationRows...)
}

type sentRequest struct {
	request  xfercontract.Request
	deadline xfercontract.Deadline
	at       xfercontract.Deadline
}
type courierTransport struct {
	mu     sync.Mutex
	now    *atomic.Int64
	sent   []sentRequest
	handle func(xfercontract.Request, xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error)
}

func (c *courierTransport) Request(r xfercontract.Request, d xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
	c.mu.Lock()
	saved := r
	if r.Outcome != nil {
		outcome := *r.Outcome
		saved.Outcome = &outcome
	}
	c.sent = append(c.sent, sentRequest{saved, d, xfercontract.Deadline(c.now.Load())})
	handle := c.handle
	c.mu.Unlock()
	if handle == nil {
		return xfercontract.Response{}, xfercontract.Attempt{}, errors.New("settlement offline")
	}
	return handle(r, d)
}
func (*courierTransport) OpenData(xfercontract.Identity, xfercontract.Deadline) (jetstream.JetStream, error) {
	panic("settlement acquired ordinary OBJ privileges")
}
func (*courierTransport) CloseData(xfercontract.Identity, xfercontract.Deadline) error {
	panic("courier closed another owner's data transport")
}
func (c *courierTransport) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.sent) }
func (c *courierTransport) requests() []sentRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]sentRequest(nil), c.sent...)
}

type retainedOwner struct {
	identity xfercontract.Identity
	origin   xfercontract.Origin
	capsule  xfercontract.SettlementCapsule
}

func (r retainedOwner) Census(o xfercontract.Origin, _ xfercontract.Deadline) (xfercontract.LedgerCensus, error) {
	if o != r.origin {
		return xfercontract.LedgerCensus{}, errors.New("wrong origin")
	}
	return xfercontract.LedgerCensus{Origin: o, Ready: true, Complete: true,
		Rows: []xfercontract.LedgerRow{{Identity: r.identity, Origin: o, EntryNonce: r.identity.EntryNonce,
			Revision: 1, Phase: xfercontract.PhaseTerminalPending, Settlement: &r.capsule}},
		Entries: []xfercontract.Protection{{Identity: r.identity, Phase: xfercontract.PhaseTerminalPending, ObjectVersion: r.identity.ObjectVersion, OwnerGeneration: o.Generation}}}, nil
}

type signedIssuer struct{}

func (signedIssuer) Verify(c xfercontract.SettlementCapsule, p xfercontract.NKeyProof) error {
	if p.NKey != c.ReceiverNkey {
		return errors.New("wrong receiver")
	}
	issuer, err := nkeys.FromPublicKey(c.Issuer)
	if err != nil {
		return err
	}
	sig, err := base64.RawURLEncoding.DecodeString(c.Signature)
	if err != nil {
		return err
	}
	c.Signature = ""
	body, err := json.Marshal(c)
	if err != nil {
		return err
	}
	if err := issuer.Verify(body, sig); err != nil {
		return err
	}
	receiver, err := nkeys.FromPublicKey(p.NKey)
	if err != nil {
		return err
	}
	return receiver.Verify(p.Nonce, p.Signature)
}

type signedHistoryTransport struct {
	history xfercontract.HistoryPort
	auth    xfercontract.SettlementVerifier
	capsule xfercontract.SettlementCapsule
	proof   xfercontract.NKeyProof
	record  xfercontract.CanonicalRecord
	receipt xfercontract.TerminalReceipt
	loseACK bool
	calls   int
}

func (s *signedHistoryTransport) Request(r xfercontract.Request, d xfercontract.Deadline) (xfercontract.Response, xfercontract.Attempt, error) {
	s.calls++
	grant, err := s.auth.Verify(s.capsule, s.proof, d)
	if err != nil {
		return xfercontract.Response{}, xfercontract.Attempt{}, err
	}
	if grant.Identity != r.Identity || r.Op != xfercontract.OpTerminal || r.Outcome == nil {
		return xfercontract.Response{}, xfercontract.Attempt{}, errors.New("settlement scope changed")
	}
	history, err := s.history.Confirm(s.record, d)
	if err != nil {
		return xfercontract.Response{}, xfercontract.Attempt{Sent: true}, err
	}
	if s.loseACK {
		s.loseACK = false
		return xfercontract.Response{}, xfercontract.Attempt{Sent: true}, errors.New("real history ACK lost")
	}
	receipt := s.receipt
	receipt.HistoryStream = history.Stream
	receipt.HistorySeq = history.Sequence
	receipt.CanonicalDigest = history.RecordDigest
	return xfercontract.Response{Receipt: &receipt}, xfercontract.Attempt{Sent: true}, nil
}
func (*signedHistoryTransport) OpenData(xfercontract.Identity, xfercontract.Deadline) (jetstream.JetStream, error) {
	panic("settlement opened data")
}
func (*signedHistoryTransport) CloseData(xfercontract.Identity, xfercontract.Deadline) error {
	panic("settlement closed data")
}
