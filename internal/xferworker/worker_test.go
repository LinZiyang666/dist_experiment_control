package xferworker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
)

// These probes exercise the registry's ownership boundary, not another leaf's
// byte/history implementation. Unimplemented IO returns an error; a snapshot
// records only the state supplied to the probe and cannot fake an IO receipt.
type capacityProbe struct {
	mu   sync.Mutex
	next int
	live map[x.ReservationID]x.Reservation
	fail bool
}

func (c *capacityProbe) Reserve(r x.Reservation) (x.ReservationID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return "", workerError(x.CodeCapacityExceeded)
	}
	c.next++
	id := x.ReservationID(fmt.Sprintf("reservation-%d", c.next))
	c.live[id] = r
	return id, nil
}
func (c *capacityProbe) Release(id x.ReservationID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.live[id]; !ok {
		return fmt.Errorf("unknown reservation %s", id)
	}
	delete(c.live, id)
	return nil
}
func (c *capacityProbe) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.live) }

type authorityProbe struct {
	check func(x.Identity, x.AccessKind, x.Deadline) error
}

func (a *authorityProbe) Check(id x.Identity, access x.AccessKind, d x.Deadline) error {
	if a.check != nil {
		return a.check(id, access, d)
	}
	return nil
}

type workerFixture struct {
	cfg      Config
	capacity *capacityProbe
	mu       sync.Mutex
	utc      time.Time
}

func newWorkerFixture() *workerFixture {
	f := &workerFixture{capacity: &capacityProbe{live: make(map[x.ReservationID]x.Reservation)}, utc: time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)}
	f.cfg = Config{Base: x.Base{Clock: func() (string, int64, error) { now, err := controlbudget.Now(); return "boot", now, err }, UTC: f.now, Capacity: f.capacity}, Identity: x.Identity{AgentInstance: "agent"}, Authority: &authorityProbe{}}
	return f
}
func (f *workerFixture) now() time.Time          { f.mu.Lock(); defer f.mu.Unlock(); return f.utc }
func (f *workerFixture) advance(d time.Duration) { f.mu.Lock(); f.utc = f.utc.Add(d); f.mu.Unlock() }
func (f *workerFixture) prepare(tier, tid string) x.Prepare {
	id := x.Identity{SID: "session-" + tid, NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: tid, EntryNonce: "nonce-" + tid, IntentDigest: "digest-" + tid}
	if tier == "b" {
		id.ObjectVersion = "object-" + tid
	}
	origin := x.Origin{BrokerID: "broker", InstanceID: "origin", BootID: "origin-boot", Generation: 1}
	p := x.Prepare{CallID: "call", Identity: id, Origin: origin, Intent: x.Intent{Verb: "push", Tier: tier, Path: "destination", ChunkSize: 32 << 10}, Permit: x.CreationPermit{Identity: id, Origin: origin, IssuedAt: f.now(), CreateUntil: f.now().Add(30 * time.Second), PermitNonce: "permit", Signature: "verified-by-control"}, Budget: x.BudgetSeal{StartedAt: f.now(), EffectiveBudgetNS: int64(30 * time.Second), RemainingNS: int64(30 * time.Second), SourceSealed: true}}
	digest := sha256.Sum256(p.Inline)
	p.Intent.SHA256 = hex.EncodeToString(digest[:])
	return p
}
func deadline(t *testing.T, d time.Duration) x.Deadline {
	t.Helper()
	value, err := controlbudget.After(d)
	if err != nil {
		t.Fatal(err)
	}
	return value
}
func newOwner(t *testing.T, f *workerFixture) *owner {
	t.Helper()
	m, err := NewWorker(f.cfg)
	if err != nil {
		t.Fatal(err)
	}
	o := m.(*owner)
	t.Cleanup(func() {
		if err := o.Stop(deadline(t, 5*time.Second)); err != nil {
			t.Error(err)
		}
	})
	return o
}
func exportWorker(t *testing.T, o *owner) (Frozen, x.WorkerState) {
	t.Helper()
	frozen, err := o.Freeze(deadline(t, 5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	return frozen, state
}
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), code) {
		t.Fatalf("error = %v, want %s", err, code)
	}
}

// origin: hot-update S7 plan §4 / §8.2 / T14
func TestTransferWorkerRetainsPreparedAndCourierOnlyStates(t *testing.T) {
	t.Run("prepared-and-zero-data", func(t *testing.T) {
		f := newWorkerFixture()
		o := newOwner(t, f)
		p := f.prepare("a", "inline")
		p.Inline = []byte("owned-inline")
		p.Intent.Size = int64(len(p.Inline))
		p.Budget.Size = p.Intent.Size
		digest := sha256.Sum256(p.Inline)
		p.Intent.SHA256 = hex.EncodeToString(digest[:])
		want := append([]byte(nil), p.Inline...)
		for _, prepare := range []x.Prepare{p, f.prepare("b", "zero")} {
			if _, err := o.Prepare(prepare, deadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
		}
		p.Inline[0] = 'X'
		old, state := exportWorker(t, o)
		if len(state.Entries) != 2 || len(state.CapacityReservations) != 2 || len(state.ResultRetention) != 2 {
			t.Fatalf("prepared closure missing: %+v", state)
		}
		if !reflect.DeepEqual(state.Entries[0].Prepare.Inline, want) || state.Entries[1].Status.Phase != x.PhasePrepared {
			t.Fatalf("prepared bytes/phase changed: %+v", state.Entries)
		}
		first := state.Entries[0].Prepare.Identity
		state.Entries[0].Prepare.Inline[0] = 'Y'
		again, _, err := old.Export()
		if err != nil || !reflect.DeepEqual(again.Entries[0].Prepare.Inline, want) {
			t.Fatalf("snapshot aliased: %v %+v", err, again)
		}
		if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := old.Export(); err == nil {
			t.Fatal("old cut handle remained valid after Thaw")
		}
		_, current := exportWorker(t, o)
		if len(current.Entries) != 2 || current.Lineage.Generation != 2 || current.Entries[0].OwnerGeneration != 2 {
			t.Fatalf("current rollback lost retained entries: %+v", current)
		}
		imported, salvage, err := ImportWorker(f.cfg, current, nil)
		if err != nil || len(salvage.Resources) != 0 {
			t.Fatalf("prepared import: %v %+v", err, salvage)
		}
		// Pristine repack transfers reservations, so the discarded old heap is
		// deliberately not stopped again against the same account in this test.
		copy := imported.(*owner)
		if err := copy.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 3}, deadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		if status, err := copy.Status(first); err != nil || status.Phase != x.PhasePrepared {
			t.Fatalf("prepared import state: %+v %v", status, err)
		}
		cut, roundtrip := exportWorker(t, copy)
		if len(roundtrip.Entries) != 2 {
			t.Fatal("prepared import dropped entries")
		}
		if _, err := cut.DetachPristine(); err == nil {
			t.Fatal("ACTIVE import accepted pristine detach")
		}
		copy.mu.Lock()
		copy.stopped = true
		copy.signalLocked()
		copy.mu.Unlock()
	})
	t.Run("courier-only-pristine-and-current", func(t *testing.T) {
		f := newWorkerFixture()
		o := newOwner(t, f)
		p := f.prepare("b", "courier")
		if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		outcome := x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled}
		canonical := x.CanonicalRecord{Identity: p.Identity, Kind: "failed", ReqID: "canonical-request", Digest: "canonical-digest", Bytes: []byte("exact-private-canonical")}
		courier := &courierProbe{state: x.CourierState{Identity: p.Identity, Outcome: outcome, Canonical: canonical, Reply: "original-reply", Attempt: x.Attempt{Ordinal: 4, Sent: true}, NextAttempt: o.entries[key(p.Identity)].byteDeadline}}
		o.entries[key(p.Identity)].courier = courier
		o.entries[key(p.Identity)].value.Status.Phase = x.PhaseTerminalPending
		_, state := exportWorker(t, o)
		if len(state.Couriers) != 1 || len(state.CanonicalOutcomes) != 1 || state.Couriers[0].Attempt.Ordinal != 4 || string(state.Couriers[0].Canonical.Bytes) != "exact-private-canonical" {
			t.Fatalf("courier responsibility missing: %+v", state)
		}
		if err := o.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 2}, deadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		_, current := exportWorker(t, o)
		if len(current.Couriers) != 1 || len(current.CanonicalOutcomes) != 1 {
			t.Fatalf("current cut lost/duplicated courier: %+v", current)
		}
		m, _, err := ImportWorker(f.cfg, current, nil)
		if err != nil {
			t.Fatal(err)
		}
		imported := m.(*owner)
		cut, roundtrip := exportWorker(t, imported)
		if !reflect.DeepEqual(current.Couriers, roundtrip.Couriers) {
			t.Fatal("pristine import changed courier")
		}
		// This rejection is a contract limitation, not fake activation success:
		// P00 has no actor-state import/rebind port (see package handoff report).
		requireCode(t, imported.Thaw(Bindings{Authority: f.cfg.Authority, OwnerGeneration: 3}, deadline(t, time.Second)), x.CodeOwnerRecovering)
		if _, err := cut.DetachPristine(); err != nil {
			t.Fatal(err)
		}
	})
}

type courierProbe struct {
	state   x.CourierState
	stopErr error
	stopped int
	steps   int
}

func (c *courierProbe) Step(x.Deadline) error                     { c.steps++; return io.EOF }
func (c *courierProbe) Freeze(x.Deadline) (x.CourierState, error) { return c.state, nil }
func (c *courierProbe) Stop(x.Deadline) error {
	if c.stopErr == nil {
		c.stopped++
	}
	return c.stopErr
}
func (c *courierProbe) Acknowledge(r x.TerminalReceipt) error {
	if r.CanonicalDigest != c.state.Canonical.Digest || r.Kind != c.state.Canonical.Kind || r.HistoryStream == "" || r.HistorySeq == 0 {
		return workerError(x.CodeTerminalConflict)
	}
	c.state.Receipt = xferimage.CloneTransferReceipt(&r)
	return nil
}

// origin: hot-update S7 plan §6.1 / T27 / M07
func TestTransferExpiredPermitNeverRecreatesWorker(t *testing.T) {
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("b", "expired")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	f.advance(time.Minute)
	if status, err := o.Prepare(p, deadline(t, time.Second)); err != nil || status.Phase != x.PhasePrepared || f.capacity.next != 1 {
		t.Fatalf("expired existing prepare did not reconcile: %+v %v", status, err)
	}
	conflict := p
	conflict.Intent.Path = "changed"
	requireCode(t, func() error { _, err := o.Prepare(conflict, deadline(t, time.Second)); return err }(), x.CodeIntentConflict)
	// Model the independently confirmed settlement at the registry boundary.
	// Real history/receiver integration belongs to P07/P09/P14, not this unit.
	e := o.entries[key(p.Identity)]
	e.value.Status.Phase = x.PhaseSettled
	e.value.Status.State = x.StatusTerminal
	e.retention.ResultUntil = f.now().Add(resultRetention)
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal("result was evicted before retention", err)
	}
	f.advance(resultRetention + time.Second)
	before := f.capacity.next
	_, err := o.Prepare(p, deadline(t, time.Second))
	requireCode(t, err, x.CodePermitExpired)
	if f.capacity.count() != 0 || f.capacity.next != before || len(o.entries) != 0 {
		t.Fatalf("expired permit allocated worker/capacity: entries=%d calls=%d live=%d", len(o.entries), f.capacity.next, f.capacity.count())
	}
	if _, err := o.Status(p.Identity); err == nil {
		t.Fatal("GC left a worker")
	}
	if _, err := o.Apply(x.Control{Kind: x.ControlGrant, Identity: p.Identity, Grant: &x.CommitGrant{Identity: p.Identity, GrantNonce: "grant", TerminalID: "terminal", ReceiverInstance: "agent"}}, deadline(t, time.Second)); err == nil {
		t.Fatal("expired entry obtained grant")
	}
	// A grant/publication still pending does not share the result TTL.
	live := f.prepare("b", "pending")
	if _, err := o.Prepare(live, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	f.advance(24 * time.Hour)
	if _, err := o.Prepare(live, deadline(t, time.Second)); err != nil || f.capacity.count() != 1 {
		t.Fatalf("unsettled responsibility was reaped: %v", err)
	}
}

// origin: hot-update S7 plan §4 / §6.1 / T14 / T27
func TestTransferWorkerAdmissionRejectsConflictsBeforeSideEffects(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*x.Prepare)
		code   string
	}{
		{"permit-identity", func(p *x.Prepare) { p.Permit.Identity.EntryNonce = "foreign" }, x.CodeIntentConflict},
		{"origin", func(p *x.Prepare) { p.Permit.Origin.Generation++ }, x.CodeIntentConflict},
		{"missing-signature", func(p *x.Prepare) { p.Permit.Signature = "" }, x.CodeIntentConflict},
		{"expired", func(p *x.Prepare) { p.Permit.CreateUntil = p.Permit.IssuedAt }, x.CodePermitExpired},
		{"foreign-instance", func(p *x.Prepare) { p.Identity.AgentInstance = "foreign"; p.Permit.Identity = p.Identity }, x.CodeIntentConflict},
		{"inline-length", func(p *x.Prepare) { p.Intent.Size = 1 }, x.CodeObjectVersionMismatch},
		{"subject-token", func(p *x.Prepare) { p.Identity.TransferID = "bad.*"; p.Permit.Identity = p.Identity }, x.CodeIntentConflict},
		{"oversize", func(p *x.Prepare) { p.Intent.Size = 2<<30 + 1 }, x.CodeBudgetExceeded},
		{"geometry", func(p *x.Prepare) {
			p.Intent.Tier = "b"
			p.Identity.ObjectVersion = "object"
			p.Permit.Identity = p.Identity
			p.Intent.ChunkSize = 1
		}, x.CodeObjectVersionMismatch},
	} {
		t.Run(test.name, func(t *testing.T) {
			f := newWorkerFixture()
			o := newOwner(t, f)
			p := f.prepare("a", "invalid")
			test.mutate(&p)
			_, err := o.Prepare(p, deadline(t, time.Second))
			requireCode(t, err, test.code)
			if f.capacity.next != 0 || len(o.entries) != 0 {
				t.Fatal("rejected prepare had side effects")
			}
		})
	}
	t.Run("capacity-before-factory", func(t *testing.T) {
		f := newWorkerFixture()
		f.capacity.fail = true
		o := newOwner(t, f)
		_, err := o.Prepare(f.prepare("b", "capacity"), deadline(t, time.Second))
		requireCode(t, err, x.CodeCapacityExceeded)
		if len(o.entries) != 0 {
			t.Fatal("failed capacity created entry")
		}
	})
	t.Run("single-flight", func(t *testing.T) {
		f := newWorkerFixture()
		o := newOwner(t, f)
		p := f.prepare("b", "first")
		if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		other := f.prepare("b", "second")
		other.Identity.SID = p.Identity.SID
		other.Permit.Identity = other.Identity
		_, err := o.Prepare(other, deadline(t, time.Second))
		requireCode(t, err, x.CodeCapacityExceeded)
		if f.capacity.next != 1 {
			t.Fatal("single-flight allocated a second reservation")
		}
	})
}

type fileSourceProbe struct {
	files        map[string]*os.File
	failAt       int
	calls        int
	fileAndError bool
}

func (s *fileSourceProbe) TakeFile(id, name string) (*os.File, error) {
	s.calls++
	file := s.files[id+"/"+name]
	delete(s.files, id+"/"+name)
	if s.calls == s.failAt {
		if s.fileAndError {
			return file, errors.New("take failed after ownership transfer")
		}
		return nil, errors.New("take refused")
	}
	return file, nil
}

// origin: hot-update S7 plan §8.2 / T14
func TestTransferWorkerImportPreservesTakenResourceSalvage(t *testing.T) {
	for _, withError := range []bool{false, true} {
		t.Run(fmt.Sprintf("file-plus-error-%v", withError), func(t *testing.T) {
			f := newWorkerFixture()
			o := newOwner(t, f)
			p := f.prepare("b", "resources")
			if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
			_, state := exportWorker(t, o)
			tmp, err := os.CreateTemp(t.TempDir(), "original")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = tmp.Close() })
			ref := x.ResourceRef{ResourceID: "original-tmp", Name: x.TmpFileName, Kind: x.ResourceTmp}
			state.Receivers = []x.ReceiverState{{Identity: p.Identity, ByteDeadline: state.ResultRetention[0].Deadline, Atomic: x.AtomicState{Tmp: ref}}}
			source := &fileSourceProbe{files: map[string]*os.File{ref.ResourceID + "/" + ref.Name: tmp}, fileAndError: withError}
			if withError {
				source.failAt = 1
			}
			m, salvage, err := ImportWorker(f.cfg, state, source)
			if source.calls != 1 || len(salvage.Resources) != 1 || salvage.Resources[0].Files[x.TmpFileName] != tmp {
				t.Fatalf("taken original missing: calls=%d salvage=%+v", source.calls, salvage)
			}
			if withError {
				if err == nil || m != nil {
					t.Fatal("file+error import pretended success")
				}
				if _, err := tmp.WriteString("still-original"); err != nil {
					t.Fatal("salvage OFD closed", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			imported := m.(*owner)
			cut, roundtrip := exportWorker(t, imported)
			if !reflect.DeepEqual(roundtrip.Receivers, state.Receivers) {
				t.Fatal("receiver state lost")
			}
			detached, err := cut.DetachPristine()
			if err != nil || len(detached.Resources) != 1 || detached.Resources[0].Files[x.TmpFileName] != tmp {
				t.Fatalf("pristine detach: %+v %v", detached, err)
			}
			if _, err := cut.DetachPristine(); err == nil {
				t.Fatal("resource detached twice")
			}
			if _, err := tmp.WriteString("same-description"); err != nil {
				t.Fatal("detached original closed", err)
			}
		})
	}
}

// origin: hot-update S7 plan §8.1 / §8.2 / T14
func TestTransferWorkerImportRejectsBrokenPrivateClosure(t *testing.T) {
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("b", "state")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	_, state := exportWorker(t, o)
	for _, test := range []struct {
		name   string
		mutate func(*x.WorkerState)
	}{
		{"schema", func(s *x.WorkerState) { s.Schema = "foreign" }},
		{"boot", func(s *x.WorkerState) { s.Lineage.BootID = "foreign" }},
		{"admission-open", func(s *x.WorkerState) { s.Admission.Closed = false }},
		{"inflight", func(s *x.WorkerState) { s.Admission.Inflight = 1 }},
		{"generation", func(s *x.WorkerState) { s.Entries[0].OwnerGeneration++ }},
		{"duplicate-key", func(s *x.WorkerState) { s.Entries = append(s.Entries, s.Entries[0]) }},
		{"reservation", func(s *x.WorkerState) { s.CapacityReservations = nil }},
		{"retention", func(s *x.WorkerState) { s.ResultRetention = nil }},
		{"phase", func(s *x.WorkerState) { s.Entries[0].Status.Phase = "invented" }},
		{"orphan-courier", func(s *x.WorkerState) { s.Couriers = []x.CourierState{{Identity: x.Identity{TransferID: "orphan"}}} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := xferimage.CloneWorkerState(state)
			test.mutate(&bad)
			m, salvage, err := ImportWorker(f.cfg, bad, nil)
			if err == nil || m != nil || len(salvage.Resources) != 0 {
				t.Fatalf("invalid closure accepted: %v %+v", err, salvage)
			}
		})
	}
}

// origin: hot-update S7 plan §4 / T14
func TestTransferWorkerControlRejectsLateNonceAndOppositeReceipt(t *testing.T) {
	f := newWorkerFixture()
	o := newOwner(t, f)
	p := f.prepare("b", "control")
	if _, err := o.Prepare(p, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("original-terminal"))
	courier := &courierProbe{state: x.CourierState{Identity: p.Identity, Canonical: x.CanonicalRecord{Identity: p.Identity, Kind: "failed", Digest: hex.EncodeToString(digest[:])}}}
	o.entries[key(p.Identity)].courier = courier
	receipt := x.TerminalReceipt{Kind: "complete", CanonicalDigest: courier.state.Canonical.Digest, HistoryStream: "history", HistorySeq: 1}
	_, err := o.Apply(x.Control{Kind: x.ControlReceipt, Identity: p.Identity, Receipt: &receipt}, deadline(t, time.Second))
	requireCode(t, err, x.CodeTerminalConflict)
	late := p.Identity
	late.EntryNonce = "previous-entry"
	_, err = o.Apply(x.Control{Kind: x.ControlReceipt, Identity: late, Receipt: &receipt}, deadline(t, time.Second))
	requireCode(t, err, x.CodeIntentConflict)
	_, err = o.Apply(x.Control{Kind: x.ControlCancel, Identity: p.Identity, Reason: x.CancelUser, Receipt: &receipt}, deadline(t, time.Second))
	requireCode(t, err, x.CodeIntentConflict)
	receipt.Kind = "failed"
	status, err := o.Apply(x.Control{Kind: x.ControlReceipt, Identity: p.Identity, Receipt: &receipt}, deadline(t, time.Second))
	if err != nil || status.State != x.StatusTerminal || status.Phase != x.PhaseSettled || courier.stopped != 1 {
		t.Fatalf("exact receipt did not settle: %+v %v stops=%d", status, err, courier.stopped)
	}
	status.Terminal.Kind = "mutated"
	current, err := o.Status(p.Identity)
	if err != nil || current.Terminal.Kind != "failed" {
		t.Fatalf("status exposed mutable receipt: %+v %v", current, err)
	}
	originalRetention := o.entries[key(p.Identity)].retention.ResultUntil
	f.advance(time.Minute)
	if _, err := o.Apply(x.Control{Kind: x.ControlReceipt, Identity: p.Identity, Receipt: &receipt}, deadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if !o.entries[key(p.Identity)].retention.ResultUntil.Equal(originalRetention) {
		t.Fatal("receipt replay renewed result retention")
	}
}

var _ x.CapacityAccount = (*capacityProbe)(nil)
var _ x.FileSource = (*fileSourceProbe)(nil)
var _ x.Courier = (*courierProbe)(nil)

// TestTransferWorkerAcceptsCanonicalActorFingerprint: the registry admits the
// canonical "SHA256:" actor fingerprint (raw standard base64 with '/' and '+')
// that every real ctl identity carries, and still refuses a malformed one or
// subject/wildcard injection before any side effect.
// origin: hot-update S7 plan §6.2 / INTEG-D agent wiring (ActorFP fingerprint)
func TestTransferWorkerAcceptsCanonicalActorFingerprint(t *testing.T) {
	for actor, accepted := range map[string]bool{
		"SHA256:6UU+o8L2ceLjl++gpcLK58HC2qpWozwYdHodCdRf/u0": true,
		"SHA256:6UU+o8L2ceLjl++gpcLK58HC2qpWozwYdHodCdRf/u":  false,
		"SHA256:6UU+o8L2ceLjl++gpcLK58HC2qpWozwYdHodCdRf>u0": false,
		"actor.subject": false,
		"actor/slash":   false,
	} {
		f := newWorkerFixture()
		o := newOwner(t, f)
		p := f.prepare("b", "actor")
		p.Identity.ActorFP = actor
		p.Permit.Identity = p.Identity
		_, err := o.Prepare(p, deadline(t, time.Second))
		if (err == nil) != accepted {
			t.Errorf("actor %q: prepared=%v, want %v (%v)", actor, err == nil, accepted, err)
		}
		if !accepted && f.capacity.count() != 0 {
			t.Errorf("actor %q: refused prepare reserved capacity", actor)
		}
	}
}
