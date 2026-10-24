// Copy into internal/xferauth. Real tracker, ledger, receiver and filesystem;
// deterministic reader/history ports. No broker, child process or network.
package xferauth

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/testharness"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferfinal"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/LinZiyang666/tether/internal/xfertrack"
)

type recoveryCapacity struct{ next atomic.Uint64 }

func (c *recoveryCapacity) Reserve(x.Reservation) (x.ReservationID, error) {
	return x.ReservationID(fmt.Sprint(c.next.Add(1))), nil
}
func (*recoveryCapacity) Release(x.ReservationID) error { return nil }
func recoveryClock() (string, int64, error)             { n, err := controlbudget.Now(); return "boot", n, err }
func recoveryDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
func recoveryDigest(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

type recoveryHistory struct{}

func (recoveryHistory) Confirm(r x.CanonicalRecord, _ x.Deadline) (x.HistoryReceipt, error) {
	seq := uint64(1)
	if r.Kind != "start" {
		seq = 2
	}
	return x.HistoryReceipt{Stream: "history-" + r.Identity.SID, Sequence: seq, RecordDigest: r.Digest}, nil
}
func (recoveryHistory) Lookup(x.Key, x.Deadline) (x.HistorySet, error) {
	return x.HistorySet{}, errors.New("unused history lookup")
}

type recoveryResolver struct{ calls atomic.Uint64 }

func (r *recoveryResolver) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	r.calls.Add(1)
	return x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled}, nil
}

type recoveryFixture struct {
	base           x.Base
	m              x.TrackerManager
	l              x.LedgerPort
	id             x.Identity
	p              x.CreationPermit
	budget         x.BudgetSeal
	resolve        *recoveryResolver
	root, fallback string
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	f := &recoveryFixture{base: x.Base{Clock: recoveryClock, UTC: time.Now, Capacity: &recoveryCapacity{}}, resolve: &recoveryResolver{}}
	dir := t.TempDir()
	f.root, f.fallback = filepath.Join(dir, "primary"), filepath.Join(dir, "outbox")
	var err error
	f.l, err = xferledger.New(x.LedgerConfig{Base: f.base, Root: f.root, FallbackRoot: f.fallback})
	if err != nil {
		t.Fatal(err)
	}
	f.id = x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64)}
	o := x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	now := time.Now().UTC()
	f.p = x.CreationPermit{Identity: f.id, Origin: o, IssuedAt: now, CreateUntil: now.Add(time.Minute), PermitNonce: "permit", Signature: "verified"}
	f.m, err = xfertrack.New(x.TrackerConfig{Base: f.base, Origin: o, Ledger: f.l, History: recoveryHistory{}, ResolveReceiver: f.resolve})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.m.Stop(recoveryDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	s, err := f.m.Admit(f.p, x.Intent{Verb: "push", Tier: "a", Path: "destination", Size: 6, SHA256: recoveryDigest([]byte("abcdef"))}, recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	f.budget = s.Budget
	return f
}
func (f *recoveryFixture) seal(t *testing.T) {
	t.Helper()
	s, err := f.m.SourceSeal(f.id, x.SourceSeal{Size: 6, SHA256: recoveryDigest([]byte("abcdef")), SourceFingerprint: "original-source"}, recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	f.budget = s.Budget
}
func (f *recoveryFixture) row(t *testing.T) x.LedgerRow {
	t.Helper()
	c, err := f.l.Scan(recoveryDeadline(t))
	if err != nil || len(c.Rows) != 1 {
		t.Fatalf("census: rows=%d err=%v", len(c.Rows), err)
	}
	return c.Rows[0]
}

type recoveryReader struct {
	next int
	cp   x.SinkCheckpoint
}

func (r *recoveryReader) ReadChunk(x.Deadline) (x.Chunk, error) {
	parts := []string{"abc", "def"}
	if r.next == len(parts) {
		return x.Chunk{}, io.EOF
	}
	b := []byte(parts[r.next])
	off := int64(3 * r.next)
	r.next++
	return x.Chunk{Data: b, Receipt: x.ChunkReceipt{Index: 0, Offset: off, Length: uint32(len(b)), SHA256: sha256.Sum256(b)}}, nil
}
func (r *recoveryReader) ConfirmSynced(c x.SinkCheckpoint) error { r.cp = c; return nil }
func (r *recoveryReader) Freeze(x.Deadline) (x.ReaderState, error) {
	return x.ReaderState{SinkCheckpoint: r.cp}, nil
}
func (*recoveryReader) Stop(x.Deadline) error { return nil }

// origin: S7 recovery review; a closed gate can retain a nonzero synced prefix.
// origin: S7 internal review rv-recovery F03
func exerciseRecoveryReceiverFailureAfterSyncedBytesSettles(t *testing.T) {
	for _, scenario := range []string{"cancel_after_first_chunk", "no_force_destination_exists"} {
		t.Run(scenario, func(t *testing.T) {
			f := newRecoveryFixture(t)
			f.seal(t)
			dir := t.TempDir()
			dst := filepath.Join(dir, "destination")
			if scenario == "no_force_destination_exists" {
				if err := os.WriteFile(dst, []byte("old"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			vp, err := transferpath.ValidateForWrite(dst, []string{dir}, 0)
			if err != nil {
				t.Fatal(err)
			}
			sink, err := transferpath.NewAtomicReceiver(vp, "owned", false)
			if err != nil {
				t.Fatal(err)
			}
			a, err := xferreceive.New(x.ReceiveConfig{Base: f.base, Identity: f.id, Budget: f.budget, Reader: &recoveryReader{}, Atomic: sink})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := a.Stop(recoveryDeadline(t)); err != nil {
					t.Error(err)
				}
			})
			if err := a.Step(recoveryDeadline(t)); err != nil {
				t.Fatal(err)
			}
			if scenario == "cancel_after_first_chunk" {
				if err := a.Stop(recoveryDeadline(t)); err != nil {
					t.Fatal(err)
				}
			} else {
				g, err := f.m.Reserve(f.id, x.ReserveRequest{Origin: f.p.Origin, ReceiverInstance: "receiver"}, recoveryDeadline(t))
				if err != nil {
					t.Fatal(err)
				}
				if err := a.Reserve(g); err != nil {
					t.Fatal(err)
				}
				if err := a.Step(recoveryDeadline(t)); err != nil {
					t.Fatal(err)
				}
				if err := a.Step(recoveryDeadline(t)); err == nil {
					t.Fatal("expected EEXIST")
				}
			}
			out, err := xferreceive.Outcome(a)
			if err != nil || out.Outcome != x.OutcomeNotEntered || out.Bytes == 0 {
				t.Fatalf("closed-gate evidence: %+v %v", out, err)
			}
			receipt, err := f.m.Finish(f.id, out, recoveryDeadline(t))
			if err != nil || receipt.Kind != "failed" {
				t.Fatalf("confirmed no-effect outcome with %d synced bytes must settle failed; receipt=%+v err=%v", out.Bytes, receipt, err)
			}
			again, err := f.m.Finish(f.id, out, recoveryDeadline(t))
			if err != nil || again != receipt {
				t.Fatalf("failed-prefix replay changed its decision: %+v %v", again, err)
			}
			data, readErr := os.ReadFile(dst)
			if scenario == "no_force_destination_exists" && (readErr != nil || string(data) != "old") {
				t.Fatalf("failed no-force commit changed the original destination: %q %v", data, readErr)
			}
			if scenario == "cancel_after_first_chunk" && !errors.Is(readErr, os.ErrNotExist) {
				t.Fatalf("canceled prefix created a destination: %q %v", data, readErr)
			}

		})
	}
}

type recoveryCensus struct{ f *recoveryFixture }

func (c recoveryCensus) SessionCensus(_ string, d x.Deadline) ([]x.LedgerCensus, error) {
	v, e := c.f.l.Scan(d)
	v.Origin = c.f.p.Origin
	return []x.LedgerCensus{v}, e
}

type recoveryFinalizer struct{}

func (recoveryFinalizer) Advance(x.Identity, x.Deadline) (x.Status, error) { return x.Status{}, nil }
func (recoveryFinalizer) Stop(x.Deadline) error                            { return nil }

// origin: S7 recovery review; session deletion must reach the actual receiver.
// origin: S7 internal review rv-recovery F07
func exerciseRecoverySessionRemovalPassesOriginalOriginToCancel(t *testing.T) {
	f := newRecoveryFixture(t)
	s, err := NewSettlement(SettlementConfig{Owners: recoveryCensus{f}, Tracker: f.m, Ledger: f.l, Finalizer: recoveryFinalizer{}, UTC: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	err = s.Settle(f.id.SID, false, recoveryDeadline(t))
	if f.resolve.calls.Load() == 0 {
		t.Fatalf("session removal never asked the reachable receiver to close its gate: %v", err)
	}
}

// origin: S7 recovery review; use the real canonical/revision validator.
// origin: S7 internal review rv-recovery F08
func exerciseRecoveryExplicitAbandonmentPersistsCanonicalTerminal(t *testing.T) {
	f := newRecoveryFixture(t)
	f.seal(t)
	if _, err := f.m.Reserve(f.id, x.ReserveRequest{Origin: f.p.Origin, ReceiverInstance: "unreachable"}, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	s := &settlement{cfg: SettlementConfig{Tracker: f.m, Ledger: f.l, UTC: time.Now}}
	if err := s.abandon(f.row(t), recoveryDeadline(t)); err != nil {
		t.Fatalf("explicit abandonment rejected by real ledger: %v", err)
	}
	r := f.row(t)
	if !r.OutcomeUnknown || r.CanonicalTerminal == nil || r.CanonicalTerminal.Kind != "failed" {
		t.Fatalf("missing durable abandoned decision: %+v", r)
	}
}

// origin: S7 recovery review; a recovered disk must allow failed-exec thaw.

type recoveryFixedReceiver struct{ outcome x.ReceiverOutcome }

func (r recoveryFixedReceiver) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	return r.outcome, nil
}

type recoveryObjects struct{}

func (recoveryObjects) Retire(id x.Identity, p x.RetirementProof, _ x.Deadline) (x.CleanupReceipt, error) {
	return x.CleanupReceipt{Identity: id, OwnerGeneration: p.OwnerGeneration, WriterRetired: true, ConsumersRetired: true, TmpUnlinked: true, DirectorySynced: true, Digest: "cleanup"}, nil
}

// origin: S7 recovery review; original origin and current owner generations differ.
// origin: S7 internal review rv-recovery F06
func exerciseRecoveryFinalizerRetiresAfterTrackerGenerationAdvance(t *testing.T) {
	f := newRecoveryFixture(t)
	f.seal(t)
	g, err := f.m.Reserve(f.id, x.ReserveRequest{Origin: f.p.Origin, ReceiverInstance: "receiver"}, recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	out := x.ReceiverOutcome{Grant: g, Outcome: x.OutcomeLanded, Primitive: x.PrimitiveRename, Bytes: 6, SHA256: recoveryDigest([]byte("abcdef")), ParentSynced: true}
	if _, err := f.m.Finish(f.id, out, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	v, err := xfertrack.Decisions(f.m)
	if err != nil {
		t.Fatal(err)
	}
	r := f.row(t)
	proof, _ := (recoveryObjects{}).Retire(f.id, x.RetirementProof{OwnerGeneration: 1}, recoveryDeadline(t))
	if err := v.AcceptCleanup(x.Key{SID: f.id.SID, TransferID: f.id.TransferID}, r.EntryNonce, r.Revision, proof, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	frozen, err := f.m.Freeze(recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	m, err := xfertrack.Import(x.TrackerConfig{Base: f.base, Origin: f.p.Origin, Ledger: f.l, History: recoveryHistory{}}, state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := m.Stop(recoveryDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	if err := m.Bind(x.Bindings{Ledger: f.l, OwnerGeneration: 2}, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	current, err := xfertrack.Decisions(m)
	if err != nil {
		t.Fatal(err)
	}
	final, err := xferfinal.New(x.FinalConfig{Base: f.base, Decisions: current, Ledger: f.l, History: recoveryHistory{}, Objects: recoveryObjects{}, ResolveReceiver: recoveryFixedReceiver{out}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := final.Stop(recoveryDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	status, err := final.Advance(f.id, recoveryDeadline(t))
	if err != nil || status.Phase != x.PhaseSettled {
		t.Fatalf("current-generation finalizer cannot retire original-generation transfer: state=%s phase=%s err=%v", status.State, status.Phase, err)
	}
}

// origin: S7 recovery review; a real frozen live row must cross the actual codec.

// origin: S7 internal review rv-recovery F03
func TestRecoveryReceiverFailureAfterSyncedBytesSettles(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseRecoveryReceiverFailureAfterSyncedBytesSettles(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}

// origin: S7 internal review rv-recovery F07
func TestRecoverySessionRemovalPassesOriginalOriginToCancel(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseRecoverySessionRemovalPassesOriginalOriginToCancel(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}

// origin: S7 internal review rv-recovery F08
func TestRecoveryExplicitAbandonmentPersistsCanonicalTerminal(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseRecoveryExplicitAbandonmentPersistsCanonicalTerminal(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}

// origin: S7 internal review rv-recovery F06
func TestRecoveryFinalizerRetiresAfterTrackerGenerationAdvance(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseRecoveryFinalizerRetiresAfterTrackerGenerationAdvance(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}
