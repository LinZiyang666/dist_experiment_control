// Real tracker, ledger and filesystem with deterministic history receipts.
package xfertrack_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xferledger"
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
	return newRecoveryTransfer(t, "a", "push")
}

func newRecoveryTransfer(t *testing.T, tier, verb string) *recoveryFixture {
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
	intent := x.Intent{Verb: verb, Tier: tier, Path: "destination", Size: 6, SHA256: recoveryDigest([]byte("abcdef"))}
	if tier == "b" {
		f.id.ObjectVersion = "original-version"
		f.p.Identity = f.id
		intent.ChunkSize = 32768
	}
	s, err := f.m.Admit(f.p, intent, recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	f.budget = s.Budget
	return f
}
func (f *recoveryFixture) seal(t *testing.T) {
	t.Helper()
	seal := x.SourceSeal{Size: 6, SHA256: recoveryDigest([]byte("abcdef")), SourceFingerprint: "original-source"}
	if f.id.ObjectVersion != "" {
		seal.ChunkSize, seal.Chunks = 32768, 1
	}
	s, err := f.m.SourceSeal(f.id, seal, recoveryDeadline(t))
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

// origin: S7 internal review rv-recovery F05
func TestRecoveryDirtyLedgerCanRebindAfterWriteFailure(t *testing.T) {
	f := newRecoveryFixture(t)
	for _, p := range []string{f.root, f.fallback} {
		if err := os.Chmod(p, 0500); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = os.Chmod(f.root, 0700); _ = os.Chmod(f.fallback, 0700) })
	_, writeErr := f.m.SourceSeal(f.id, x.SourceSeal{Size: 6, SHA256: recoveryDigest([]byte("abcdef")), SourceFingerprint: "original-source"}, recoveryDeadline(t))
	if !errors.Is(writeErr, xferledger.ErrDurability) {
		t.Fatalf("filesystem fault did not prevent both writes: %v", writeErr)
	}
	retained, scanErr := f.l.Scan(recoveryDeadline(t))
	if scanErr == nil || retained.Ready || retained.Complete || len(retained.Rows) != 1 {
		t.Fatal("write failure lost its exact pending proposal")
	}
	proposal := retained.Rows[0]
	if _, err := f.m.Freeze(recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{f.root, f.fallback} {
		if err := os.Chmod(p, 0700); err != nil {
			t.Fatal(err)
		}
	}
	var rebound bool
	for attempt := 0; attempt < 2; attempt++ {
		if err := f.m.Bind(x.Bindings{Ledger: f.l, OwnerGeneration: 1}, recoveryDeadline(t)); err == nil {
			rebound = true
			break
		} else {
			t.Logf("rebind attempt %d after storage recovery: %v", attempt, err)
		}
	}
	if !rebound {
		t.Fatal("failed-exec rollback cannot retry its retained dirty decision; tracker remains sleeping")
	}
	row := f.row(t)
	if !reflect.DeepEqual(row, proposal) || row.EntryNonce != f.id.EntryNonce || !row.BudgetSeal.StartedAt.Equal(f.budget.StartedAt) ||
		row.BudgetSeal.EffectiveBudgetNS != f.budget.EffectiveBudgetNS || row.SourceSeal == nil || row.SourceSeal.SourceFingerprint != "original-source" {
		t.Fatalf("retry changed the original nonce, budget or proposal: %+v", row)
	}
	cold, err := xferledger.New(x.LedgerConfig{Base: f.base, Root: f.root, FallbackRoot: f.fallback})
	if err != nil {
		t.Fatal(err)
	}
	durable, err := cold.Scan(recoveryDeadline(t))
	if err != nil || !durable.Ready || !durable.Complete || len(durable.Rows) != 1 || !reflect.DeepEqual(durable.Rows[0], row) {
		t.Fatalf("rebind did not Sync its exact proposal: %+v %v", durable, err)
	}
}

// origin: S7 internal review rv-recovery F02
func TestRecoveryLiveTrackerSnapshotPassesActualCodec(t *testing.T) {
	for _, phase := range []string{"admitted", "source_sealed", "commit_reserved"} {
		t.Run(phase, func(t *testing.T) {
			f := newRecoveryFixture(t)
			if phase != "admitted" {
				f.seal(t)
			}
			if phase == "commit_reserved" {
				if _, err := f.m.Reserve(f.id, x.ReserveRequest{Origin: f.p.Origin, ReceiverInstance: "receiver"}, recoveryDeadline(t)); err != nil {
					t.Fatal(err)
				}
			}
			frozen, err := f.m.Freeze(recoveryDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			state, _, err := frozen.Export()
			if err != nil {
				t.Fatal(err)
			}
			encoded, err := xferimage.EncodeTracker(state)
			if err != nil {
				t.Fatalf("actual frozen %s tracker rejected by codec: %v; entries=%d census_rows=%d census_ready=%t census_complete=%t census_origin=%+v retention=%+v", phase, err, len(state.Entries), len(state.Census.Rows), state.Census.Ready, state.Census.Complete, state.Census.Origin, state.Entries[0].Retention)
			}
			decoded, err := xferimage.DecodeTracker(encoded)
			if err != nil {
				t.Fatal(err)
			}
			verifyRecoveryImport(t, f, state, decoded)
		})
	}
}

func verifyRecoveryImport(t *testing.T, f *recoveryFixture, original, decoded x.TrackerState) {
	t.Helper()
	if !reflect.DeepEqual(original, decoded) {
		t.Fatal("codec changed the actual frozen closure")
	}
	imported, err := xfertrack.Import(x.TrackerConfig{Base: f.base, Origin: f.p.Origin, Ledger: f.l, History: recoveryHistory{}, ResolveReceiver: f.resolve}, decoded)
	if err != nil {
		t.Fatalf("actual codec state could not import: %v", err)
	}
	defer func() {
		if err := imported.Stop(recoveryDeadline(t)); err != nil {
			t.Error(err)
		}
	}()
	if err := imported.Bind(x.Bindings{Ledger: f.l, OwnerGeneration: decoded.Generation + 1}, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	next, err := imported.Freeze(recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	after, _, err := next.Export()
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Entries) != len(original.Entries) {
		t.Fatal("import lost retained rows")
	}
	for i, row := range original.Entries {
		if !reflect.DeepEqual(row, after.Entries[i]) || original.WatchdogDeadlines[i] != after.WatchdogDeadlines[i] {
			t.Fatal("import changed original revisions, grants, budgets or deadlines")
		}
	}
	if _, err := xferimage.EncodeTracker(after); err != nil {
		t.Fatalf("next generation no longer encodes: %v", err)
	}
}

// origin: S7 internal review rv-recovery F02
func TestTransferLiveTrackerCodecCoversBothTiersAndDirections(t *testing.T) {
	for _, tier := range []string{"a", "b"} {
		for _, verb := range []string{"push", "pull"} {
			for _, phase := range []string{"admitted", "source_sealed", "commit_reserved", "history_confirmed"} {
				t.Run(tier+"/"+verb+"/"+phase, func(t *testing.T) {
					f := newRecoveryTransfer(t, tier, verb)
					if phase != "admitted" {
						f.seal(t)
					}
					if phase == "commit_reserved" || phase == "history_confirmed" {
						g, err := f.m.Reserve(f.id, x.ReserveRequest{Origin: f.p.Origin, ReceiverInstance: "receiver"}, recoveryDeadline(t))
						if err != nil {
							t.Fatal(err)
						}
						if phase == "history_confirmed" {
							_, err = f.m.Finish(f.id, x.ReceiverOutcome{Grant: g, Primitive: x.PrimitiveRename, Outcome: x.OutcomeLanded, Bytes: 6, SHA256: recoveryDigest([]byte("abcdef")), ParentSynced: true}, recoveryDeadline(t))
							if err != nil {
								t.Fatal(err)
							}
						}
					}
					cut, err := f.m.Freeze(recoveryDeadline(t))
					if err != nil {
						t.Fatal(err)
					}
					state, _, err := cut.Export()
					if err != nil {
						t.Fatal(err)
					}
					retention := state.Entries[0].Retention
					if phase == "history_confirmed" {
						if retention.ResultUntil.IsZero() || retention.Deadline.Deadline <= 0 {
							t.Fatal("confirmed result has no original retention clock")
						}
					} else if !retention.ResultUntil.IsZero() || retention.Deadline != (x.BootDeadline{}) {
						t.Fatal("active row started result retention before exact history")
					}
					encoded, err := xferimage.EncodeTracker(state)
					if err != nil {
						t.Fatal(err)
					}
					decoded, err := xferimage.DecodeTracker(encoded)
					if err != nil {
						t.Fatal(err)
					}
					verifyRecoveryImport(t, f, state, decoded)
					// Corrupt only the real exported retention, never build a live
					// row fixture to satisfy the codec. Confirmed phases need their
					// clock; active phases cannot carry a half-started clock.
					broken, err := xferimage.DecodeTracker(encoded)
					if err != nil {
						t.Fatal(err)
					}
					if phase == "history_confirmed" {
						broken.Entries[0].Retention.ResultUntil = time.Time{}
						broken.Entries[0].Retention.Deadline = x.BootDeadline{}
					} else {
						broken.Entries[0].Retention.Deadline = broken.WatchdogDeadlines[0].ByteDeadline
					}
					broken.Census.Rows[0] = broken.Entries[0]
					if _, err := xferimage.EncodeTracker(broken); err == nil {
						t.Fatal("codec accepted a missing confirmed clock or partial active retention")
					}
				})
			}
		}
	}
}

// origin: S7 internal review rv-recovery F02
func TestTransferFrozenCensusMatchesRowsAfterAdmissionAndRevision(t *testing.T) {
	f := newRecoveryFixture(t)
	before := f.row(t)
	f.seal(t)
	second := f.p
	second.Identity.TransferID, second.Identity.EntryNonce = "second-transfer", "second-entry"
	if _, err := f.m.Admit(second, before.Intent, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	cut, err := f.m.Freeze(recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := cut.Export()
	if err != nil {
		t.Fatal(err)
	}
	if state.Census.Ready || state.Census.Complete || state.Census.Origin != state.Origin || !reflect.DeepEqual(state.Census.Rows, state.Entries) || len(state.Census.Entries) != 2 {
		t.Fatal("frozen census reused stale ready disk rows or omitted protections")
	}
	changed := false
	for _, row := range state.Entries {
		if row.Identity == f.id {
			changed = row.Revision > before.Revision && row.SourceSeal != nil
		}
	}
	if !changed {
		t.Fatal("test never changed an existing revision")
	}
	encoded, err := xferimage.EncodeTracker(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := xferimage.DecodeTracker(encoded)
	if err != nil {
		t.Fatal(err)
	}
	// Row ordering and watchdog ordering are independent; only row closure matters here.
	if len(decoded.Entries) != 2 || !reflect.DeepEqual(decoded.Census, state.Census) {
		t.Fatal("codec changed the fresh census")
	}
}

// origin: S7 internal review rv-recovery F02 / F05
func TestTransferDirtySnapshotRetainsExactUnsyncedProposal(t *testing.T) {
	f := newRecoveryFixture(t)
	for _, path := range []string{f.root, f.fallback} {
		if err := os.Chmod(path, 0500); err != nil {
			t.Fatal(err)
		}
	}
	t.Cleanup(func() { _ = os.Chmod(f.root, 0700); _ = os.Chmod(f.fallback, 0700) })
	_, err := f.m.SourceSeal(f.id, x.SourceSeal{Size: 6, SHA256: recoveryDigest([]byte("abcdef")), SourceFingerprint: "original-source"}, recoveryDeadline(t))
	if !errors.Is(err, xferledger.ErrDurability) {
		t.Fatalf("double-write fault not exercised: %v", err)
	}
	census, err := f.l.Scan(recoveryDeadline(t))
	if err == nil || census.Ready || census.Complete || census.Code != x.CodeDurabilityUnknown || len(census.Rows) != 1 {
		t.Fatalf("dirty conflict-only census: %+v %v", census, err)
	}
	cut, err := f.m.Freeze(recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.m.Bind(x.Bindings{Ledger: f.l, OwnerGeneration: 1}, recoveryDeadline(t)); !errors.Is(err, xferledger.ErrDurability) {
		t.Fatalf("unwritable proposal activated: %v", err)
	}
	if _, err := f.m.Reserve(f.id, x.ReserveRequest{Origin: f.p.Origin, ReceiverInstance: "receiver"}, recoveryDeadline(t)); err == nil {
		t.Fatal("unsynced proposal granted a commit")
	}
	if receipt, err := f.m.Finish(f.id, x.ReceiverOutcome{Outcome: x.OutcomeNotEntered}, recoveryDeadline(t)); err == nil || receipt.Kind != "" {
		t.Fatal("unsynced proposal acknowledged a terminal")
	}
	state, _, err := cut.Export()
	if err != nil {
		t.Fatal(err)
	}
	if state.Census.Ready || state.Census.Complete || !reflect.DeepEqual(state.Entries, census.Rows) {
		t.Fatal("dirty export claimed durability or changed its proposal")
	}
	encoded, err := xferimage.EncodeTracker(state)
	if err != nil {
		t.Fatalf("retained dirty proposal cannot cross codec: %v", err)
	}
	decoded, err := xferimage.DecodeTracker(encoded)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{f.root, f.fallback} {
		if err := os.Chmod(path, 0700); err != nil {
			t.Fatal(err)
		}
	}
	verifyRecoveryImport(t, f, state, decoded)
}
