package xfertrack

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// These port doubles exercise tracker serialization and refusal independently
// of P13/P14's durability/history tests; they are never production factories.
type ledgerProbe struct {
	mu               sync.Mutex
	origin           x.Origin
	rows             map[x.Key]x.LedgerRow
	fail             x.LedgerMutationKind
	entered, release chan struct{}
	ready            bool
}

func (l *ledgerProbe) Update(k x.Key, revision uint64, m x.LedgerMutation, d x.Deadline) (x.LedgerRow, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.entered != nil {
		close(l.entered)
		<-l.release
		l.entered = nil
	}
	if l.fail == m.Kind {
		return x.LedgerRow{}, errors.New("injected ledger failure")
	}
	old := l.rows[k]
	if old.Revision == m.Row.Revision && reflect.DeepEqual(old, m.Row) {
		return cloneRow(old), nil
	}
	if old.Revision != revision || m.EntryNonce != m.Row.EntryNonce || m.Row.Revision != revision+1 {
		return x.LedgerRow{}, errors.New("revision conflict")
	}
	l.rows[k] = cloneRow(m.Row)
	return cloneRow(m.Row), nil
}
func (l *ledgerProbe) Scan(x.Deadline) (x.LedgerCensus, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := x.LedgerCensus{Origin: l.origin, Ready: l.ready, Complete: l.ready}
	for _, r := range l.rows {
		c.Rows = append(c.Rows, cloneRow(r))
	}
	return c, nil
}
func (l *ledgerProbe) Retire(k x.Key, nonce string, p x.RetirementProof, d x.Deadline) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if row := l.rows[k]; row.EntryNonce != nonce || row.Revision != p.Revision {
		return errors.New("retire conflict")
	}
	delete(l.rows, k)
	return nil
}
func (l *ledgerProbe) row(id x.Identity) x.LedgerRow {
	l.mu.Lock()
	defer l.mu.Unlock()
	return cloneRow(l.rows[key(id)])
}

type historyProbe struct {
	mu      sync.Mutex
	records map[string]x.CanonicalRecord
	fail    bool
}

func (h *historyProbe) Confirm(r x.CanonicalRecord, d x.Deadline) (x.HistoryReceipt, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.fail {
		return x.HistoryReceipt{}, errors.New("history unavailable")
	}
	if r.Digest != digest(r.Bytes) {
		return x.HistoryReceipt{}, errors.New("bad digest")
	}
	h.records[r.Digest] = *cloneRecord(&r)
	// Sequence is stable independent of replay or map iteration.
	sequence := uint64(1)
	if r.Kind != "start" {
		sequence = 2
	}
	return x.HistoryReceipt{Stream: "history-" + r.Identity.SID, Sequence: sequence, RecordDigest: r.Digest}, nil
}
func (h *historyProbe) Lookup(x.Key, x.Deadline) (x.HistorySet, error) {
	return x.HistorySet{}, errors.New("not used by tracker")
}

type resolverFunc func(x.Identity, x.Deadline) (x.ReceiverOutcome, error)

func (f resolverFunc) Resolve(id x.Identity, d x.Deadline) (x.ReceiverOutcome, error) {
	return f(id, d)
}

type testClock struct {
	mu  sync.Mutex
	utc time.Time
	ns  int64
}

func (c *testClock) clock() (string, int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return "boot", c.ns, nil
}
func (c *testClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.utc }
func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.utc = c.utc.Add(d)
	c.ns += int64(d)
}
func (c *testClock) deadline() x.Deadline {
	_, ns, _ := c.clock()
	return x.Deadline(ns + int64(5*time.Second))
}

type fixture struct {
	owner   *owner
	ledger  *ledgerProbe
	history *historyProbe
	clock   *testClock
	permit  x.CreationPermit
	intent  x.Intent
}

func newFixture(t *testing.T, tier, verb string) *fixture {
	t.Helper()
	clock := &testClock{utc: time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC), ns: int64(time.Hour)}
	origin := x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "version"}
	ledger := &ledgerProbe{origin: origin, rows: make(map[x.Key]x.LedgerRow), ready: true}
	history := &historyProbe{records: make(map[string]x.CanonicalRecord)}
	cfg := Config{Base: x.Base{Clock: clock.clock, UTC: clock.now, Capacity: &capacityProbe{}}, Origin: origin, Ledger: ledger, History: history,
		ResolveReceiver: resolverFunc(func(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
			return x.ReceiverOutcome{}, errors.New("receiver unavailable")
		})}
	manager, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Tier A is inline and has no object geometry (plan §8.1); only Tier B has chunks.
	// origin: hot-update S7 plan §8.1 / INTEG-D Tier A inline seal
	chunkSize := uint32(32768)
	if tier == "a" {
		chunkSize = 0
	}
	f := &fixture{owner: manager.(*owner), ledger: ledger, history: history, clock: clock,
		permit: x.CreationPermit{Identity: id, Origin: origin, IssuedAt: clock.now(), CreateUntil: clock.now().Add(time.Minute), PermitNonce: "permit", Signature: "verified-by-control"},
		intent: x.Intent{Verb: verb, Tier: tier, Path: "target", Size: 4, SHA256: digest([]byte("new\n")), ChunkSize: chunkSize}}
	t.Cleanup(func() {
		if err := f.owner.Stop(f.clock.deadline()); err != nil {
			t.Error(err)
		}
	})
	return f
}
func (f *fixture) admit(t *testing.T) {
	t.Helper()
	if _, err := f.owner.Admit(f.permit, f.intent, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	seal := x.SourceSeal{Size: f.intent.Size, SHA256: f.intent.SHA256, RateLimitBPS: f.intent.RateLimitBPS, ChunkSize: 32768, Chunks: 1, SourceFingerprint: "pinned-source"}
	if f.intent.Tier == "a" {
		seal.ChunkSize, seal.Chunks = 0, 0
	}
	if _, err := f.owner.SourceSeal(f.permit.Identity, seal, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) reserve(t *testing.T) x.CommitGrant {
	t.Helper()
	receiver := "agent"
	if f.intent.Verb == "pull" {
		receiver = "ctl"
	}
	grant, err := f.owner.Reserve(f.permit.Identity, x.ReserveRequest{Origin: f.permit.Origin, ReceiverInstance: receiver}, f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	return grant
}
func (f *fixture) outcome(grant x.CommitGrant) x.ReceiverOutcome {
	return x.ReceiverOutcome{Grant: grant, Primitive: x.PrimitiveRename, Outcome: x.OutcomeLanded, Bytes: 4, SHA256: f.intent.SHA256, ParentSynced: true}
}
func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), code) {
		t.Fatalf("error = %v, want %s", err, code)
	}
}

// origin: hot-update S7 plan §5.1 / L13
func TestTransferLegacyIdentityAndBucketGuards(t *testing.T) {
	tr := newTransferTracker()
	first := &transferEntry{transferID: "id", sid: "s", verb: "push", tier: "b", bucket: "bucket"}
	if got := tr.put(first); got != "" {
		t.Fatal(got)
	}
	if code, blocker := tr.putWithBlocker(&transferEntry{transferID: "other", bucket: "bucket"}); code != "too_many_in_flight" || blocker != "id" {
		t.Fatalf("bucket admitted: %s %s", code, blocker)
	}
	if got := tr.put(&transferEntry{transferID: "id"}); got != "transfer_id_in_flight" {
		t.Fatal(got)
	}
	if !tr.markCommitted(first) {
		t.Fatal("commit refused")
	}
	// The broker words its finalize reply from the returned entry
	// (internal/broker/transfer.go via ClaimAbandonedPush), so the refusal must
	// name the live entry it read under the claim lock, not just a code.
	if got, why := tr.claimAbandonedPush(first); why != abandonCommitted || got != first {
		t.Fatalf("committed push abandoned: entry=%p why=%v", got, why)
	}
	tr.remove(first)
	replacement := &transferEntry{transferID: "id", verb: "push", tier: "b"}
	if got := tr.put(replacement); got != "" {
		t.Fatal(got)
	}
	if _, ok := tr.claimFinalize(first); ok {
		t.Fatal("stale finalize claimed replacement")
	}
	if tr.markCommitted(first) {
		t.Fatal("stale commit claimed replacement")
	}
	tr.remove(first)
	if tr.get("id") != replacement {
		t.Fatal("stale removal deleted replacement")
	}
	if got, why := tr.claimAbandonedPush(replacement); why != abandonClaimed || got != replacement || !got.finalized {
		t.Fatalf("live replacement not claimed: entry=%p why=%v", got, why)
	}
	for i := 0; i < transferTrackerMaxEntries; i++ {
		tr.put(&transferEntry{transferID: fmt.Sprint(i)})
	}
	if tr.put(&transferEntry{transferID: "overflow"}) != "too_many_in_flight" {
		t.Fatal("capacity exceeded")
	}
	tr.markReaped("old", time.Now().Add(-xferRecentlyReapedTTL))
	tr.markReaped("new", time.Now())
	if tr.recentlyReaped("old") || !tr.recentlyReaped("new") {
		t.Fatal("reaped retention changed")
	}
	if transferRequestBounds(strings.Repeat("i", 129), "p", "") == "" || transferRequestBounds("i", strings.Repeat("p", 4097), "") == "" {
		t.Fatal("retained strings unbounded")
	}
}

// origin: hot-update S7 plan §5.1 / T13
func TestTransferDecisionsRejectStaleCallbacks(t *testing.T) {
	f := newFixture(t, "b", "push")
	f.admit(t)
	grant := f.reserve(t)
	f.history.setFailure(true)
	if receipt, err := f.owner.Finish(f.permit.Identity, f.outcome(grant), f.clock.deadline()); err == nil || receipt.HistorySeq != 0 {
		t.Fatal("pending history ACK fabricated")
	}
	view, err := Decisions(f.owner)
	if err != nil {
		t.Fatal(err)
	}
	row := f.ledger.row(f.permit.Identity)
	proof := x.HistoryReceipt{Stream: "history-session", Sequence: 1, RecordDigest: row.CanonicalStart.Digest}
	requireCode(t, view.AcceptHistory(key(row.Identity), "old-nonce", row.Revision, proof, f.clock.deadline()), x.CodeIntentConflict)
	requireCode(t, view.AcceptHistory(key(row.Identity), row.EntryNonce, row.Revision-1, proof, f.clock.deadline()), x.CodeIntentConflict)
	wrong := proof
	wrong.RecordDigest = "opposite"
	requireCode(t, view.AcceptHistory(key(row.Identity), row.EntryNonce, row.Revision, wrong, f.clock.deadline()), x.CodeTerminalConflict)
	if f.ledger.row(row.Identity).Revision != row.Revision {
		t.Fatal("stale callback changed revision")
	}
	if err = view.AcceptHistory(key(row.Identity), row.EntryNonce, row.Revision, proof, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	f.history.setFailure(false)
	receipt, err := f.owner.Finish(row.Identity, f.outcome(grant), f.clock.deadline())
	if err != nil || receipt.Kind != "complete" || receipt.HistorySeq != 2 {
		t.Fatalf("terminal receipt: %+v %v", receipt, err)
	}
	decided := f.ledger.row(row.Identity)
	changed := f.outcome(grant)
	changed.Outcome = x.OutcomeNotEntered
	changed.ParentSynced = false
	changed.Bytes = 0
	_, err = f.owner.Finish(row.Identity, changed, f.clock.deadline())
	requireCode(t, err, x.CodeTerminalConflict)
	if !reflect.DeepEqual(decided, f.ledger.row(row.Identity)) {
		t.Fatal("opposite terminal replaced canonical decision")
	}
	exported, err := view.Read(key(row.Identity), f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	exported.CanonicalTerminal.Bytes[0] = '!'
	if reflect.DeepEqual(exported.CanonicalTerminal, f.ledger.row(row.Identity).CanonicalTerminal) {
		t.Fatal("caller owns canonical bytes")
	}
}

// origin: hot-update S7 plan §1.2 / §6.1 / T13
func TestTransferAdmissionKeepsOriginalBudgetAndCensus(t *testing.T) {
	f := newFixture(t, "b", "pull")
	f.intent.RateLimitBPS = 256 * 1024
	if _, err := f.owner.Admit(f.permit, f.intent, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(70 * time.Second)
	f.admit(t) // Existing expired permit is still the same entry.
	row := f.ledger.row(f.permit.Identity)
	if row.StartedAt != f.permit.IssuedAt || row.BudgetSeal.EffectiveBudgetNS != int64(5*time.Minute) || row.BudgetSeal.RemainingNS != int64(230*time.Second) {
		t.Fatalf("budget restarted: %+v", row.BudgetSeal)
	}
	other := f.permit
	other.Identity.TransferID = "other"
	other.Identity.EntryNonce = "other-entry"
	_, err := f.owner.Admit(other, f.intent, f.clock.deadline())
	requireCode(t, err, x.CodePermitExpired)
	f.ledger.setReady(false)
	f.invalidateCensus(t)
	_, err = f.owner.Status(x.StatusRequest{Identity: other.Identity, Origin: f.permit.Origin}, f.clock.deadline())
	requireCode(t, err, x.CodeOwnerRecovering)
	f.ledger.setReady(true)
	s, err := f.owner.Status(x.StatusRequest{Identity: other.Identity, Origin: f.permit.Origin}, f.clock.deadline())
	if err != nil || s.State != x.StatusUnknown {
		t.Fatalf("ready absence: %+v %v", s, err)
	}
	wrong := f.permit.Origin
	wrong.BrokerID = "non-owner"
	_, err = f.owner.Status(x.StatusRequest{Identity: other.Identity, Origin: wrong}, f.clock.deadline())
	requireCode(t, err, x.CodeOwnerRecovering)
}

// A shared account tracks exact reservation identities, including imported
// owners. Releasing an amount instead of its original token would fail here.
type capacityProbe struct {
	fail bool
	mu   sync.Mutex
	next int
	live map[x.ReservationID]x.Reservation
}

func (c *capacityProbe) Reserve(r x.Reservation) (x.ReservationID, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fail {
		return "", errors.New("injected shared capacity exhaustion")
	}
	if c.live == nil {
		c.live = make(map[x.ReservationID]x.Reservation)
	}
	c.next++
	id := x.ReservationID(fmt.Sprint(c.next))
	c.live[id] = r
	return id, nil
}
func (c *capacityProbe) Release(id x.ReservationID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.live[id]; !ok {
		return errors.New("unknown capacity reservation")
	}
	delete(c.live, id)
	return nil
}

// origin: hot-update S7 plan §1.2 / §5.1 / T13
func TestTransferSourceSealFailureCannotRenewByteDeadline(t *testing.T) {
	f := newFixture(t, "b", "pull")
	if _, err := f.owner.Admit(f.permit, f.intent, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(time.Minute)
	f.ledger.setFailure(x.MutationSourceSeal)
	seal := x.SourceSeal{Size: 4, SHA256: f.intent.SHA256, ChunkSize: 32768, Chunks: 1, SourceFingerprint: "source"}
	if _, err := f.owner.SourceSeal(f.permit.Identity, seal, f.clock.deadline()); err == nil {
		t.Fatal("source seal write unexpectedly succeeded")
	}
	f.ledger.setFailure("")
	if _, err := f.owner.SourceSeal(f.permit.Identity, seal, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(241 * time.Second)
	_, err := f.owner.Reserve(f.permit.Identity, x.ReserveRequest{Origin: f.permit.Origin, ReceiverInstance: "ctl"}, f.clock.deadline())
	requireCode(t, err, x.CodeBudgetExceeded)
	status, err := f.owner.Status(x.StatusRequest{Identity: f.permit.Identity, Origin: f.permit.Origin}, f.clock.deadline())
	if err != nil || status.Budget.RemainingNS != 0 {
		t.Fatalf("expired budget renewed: %+v %v", status.Budget, err)
	}
}

// origin: hot-update S7 plan §4 / §8.3 / T13
func TestTransferAdmissionCapacityPrecedesDurableEffects(t *testing.T) {
	f := newFixture(t, "b", "push")
	f.admit(t)
	p := f.permit
	p.Identity.TransferID = "second"
	p.Identity.EntryNonce = "second-entry"
	_, err := f.owner.Admit(p, f.intent, f.clock.deadline())
	requireCode(t, err, x.CodeCapacityExceeded)
	if f.ledger.count() != 1 {
		t.Fatal("bucket refusal still wrote a second admission")
	}
	p.Identity.SID = "second-session"
	account := f.owner.cfg.Capacity.(*capacityProbe)
	account.setFailure(true)
	_, err = f.owner.Admit(p, f.intent, f.clock.deadline())
	if err == nil {
		t.Fatal("capacity refusal ignored")
	}
	if f.ledger.count() != 1 {
		t.Fatal("capacity refusal still wrote admission")
	}
	account.setFailure(false)
	if err = f.owner.Stop(f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if account.count() != 0 {
		t.Fatal("stopped tracker retained capacity")
	}
}

// origin: hot-update S7 plan §5.1 / §5.3 / T13
func TestTransferCleanupNeedsRetirementBeforeRetention(t *testing.T) {
	f := newFixture(t, "b", "push")
	f.admit(t)
	grant := f.reserve(t)
	if _, err := f.owner.Finish(f.permit.Identity, f.outcome(grant), f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	view, err := Decisions(f.owner)
	if err != nil {
		t.Fatal(err)
	}
	row := f.ledger.row(f.permit.Identity)
	cleanup := x.CleanupReceipt{Identity: row.Identity, OwnerGeneration: 1, WriterRetired: true, ConsumersRetired: true, FenceSequence: 1,
		ObjectPurged: true, TmpUnlinked: true, DirectorySynced: true, Digest: "cleanup-proof"}
	incomplete := cleanup
	incomplete.WriterRetired = false
	requireCode(t, view.AcceptCleanup(key(row.Identity), row.EntryNonce, row.Revision, incomplete, f.clock.deadline()), x.CodeResultUnknown)
	if err = view.AcceptCleanup(key(row.Identity), row.EntryNonce, row.Revision, cleanup, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(2 * xferRecentlyReapedTTL)
	f.owner.watchdogPass(f.clock.deadline())
	s, err := f.owner.Status(x.StatusRequest{Identity: row.Identity, Origin: row.Origin}, f.clock.deadline())
	if err != nil || s.Phase != x.PhaseCleanupPending || !s.CleanupPending {
		t.Fatalf("TTL retired uncleared directories: %+v %v", s, err)
	}
	row = f.ledger.row(row.Identity)
	if err = f.ledger.Retire(key(row.Identity), row.EntryNonce, x.RetirementProof{Revision: row.Revision}, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	f.ledger.setReady(false)
	f.owner.watchdogPass(f.clock.deadline())
	if !f.hasEntry(t, row.Identity) {
		t.Fatal("incomplete census discarded cleanup")
	}
	f.ledger.setReady(true)
	f.owner.watchdogPass(f.clock.deadline())
	s, err = f.owner.Status(x.StatusRequest{Identity: row.Identity, Origin: row.Origin}, f.clock.deadline())
	if err != nil || s.State != x.StatusUnknown {
		t.Fatalf("retired cache did not expire: %+v %v", s, err)
	}
	_, err = f.owner.Admit(f.permit, f.intent, f.clock.deadline())
	requireCode(t, err, x.CodePermitExpired)
}

func (h *historyProbe) setFailure(fail bool) { h.mu.Lock(); defer h.mu.Unlock(); h.fail = fail }
func (h *historyProbe) count() int           { h.mu.Lock(); defer h.mu.Unlock(); return len(h.records) }
func (l *ledgerProbe) setFailure(kind x.LedgerMutationKind) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.fail = kind
}
func (l *ledgerProbe) setReady(ready bool)    { l.mu.Lock(); defer l.mu.Unlock(); l.ready = ready }
func (l *ledgerProbe) count() int             { l.mu.Lock(); defer l.mu.Unlock(); return len(l.rows) }
func (c *capacityProbe) setFailure(fail bool) { c.mu.Lock(); defer c.mu.Unlock(); c.fail = fail }
func (c *capacityProbe) count() int           { c.mu.Lock(); defer c.mu.Unlock(); return len(c.live) }
func (f *fixture) setResolver(t *testing.T, r resolverFunc) {
	t.Helper()
	if err := f.owner.take(f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	defer f.owner.leave()
	f.owner.cfg.ResolveReceiver = r
}
func (f *fixture) invalidateCensus(t *testing.T) {
	t.Helper()
	if err := f.owner.take(f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	defer f.owner.leave()
	f.owner.state.Census.Ready = false
}
func (f *fixture) pendingGrant(t *testing.T) x.CommitGrant {
	t.Helper()
	if err := f.owner.take(f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	defer f.owner.leave()
	return *f.owner.entries[key(f.permit.Identity)].row.Grant
}
func (f *fixture) hasEntry(t *testing.T, id x.Identity) bool {
	t.Helper()
	if err := f.owner.take(f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	defer f.owner.leave()
	return f.owner.entries[key(id)] != nil
}
