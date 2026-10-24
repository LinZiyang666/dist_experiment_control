package xfertrack

import (
	"fmt"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"reflect"
	"testing"
	"time"
)

// origin: S7 internal review rv-gc F4
func TestTransferImportedReapedResponsibilitiesUseCapacityAndExpire(t *testing.T) {
	f := newFixture(t, "b", "push")
	f.permit.Identity.IntentDigest = digest([]byte("retained intent"))
	frozen, err := f.owner.Freeze(f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	state.Reaped = []x.ReapedEntry{{Identity: f.permit.Identity, At: f.clock.now(),
		Retention: x.RetentionState{Identity: f.permit.Identity, CreateUntil: f.permit.CreateUntil, ResultUntil: f.clock.now().Add(10 * time.Minute),
			Deadline: x.BootDeadline{BootID: "boot", Deadline: x.Deadline(f.clock.ns + int64(10*time.Minute))}}}}
	state.Census = x.LedgerCensus{Origin: state.Origin, Ready: true, Complete: true}
	encoded, err := xferimage.EncodeTracker(state)
	if err != nil {
		t.Fatalf("input must satisfy the production image codec: %v", err)
	}
	state, err = xferimage.DecodeTracker(encoded)
	if err != nil {
		t.Fatal(err)
	}
	capacity := &capacityProbe{}
	cfg := f.owner.cfg
	cfg.Capacity = capacity
	m, err := Import(cfg, state)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = m.Stop(f.clock.deadline()) }()
	capacity.mu.Lock()
	held := len(capacity.live)
	capacity.mu.Unlock()
	if held == 0 {
		t.Errorf("imported retained reaped entry consumes zero capacity")
	}
	if err := m.Bind(x.Bindings{Ledger: f.ledger, OwnerGeneration: state.Generation}, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if status, err := m.Admit(f.permit, f.intent, f.clock.deadline()); err == nil && status.Phase == x.PhaseOpen {
		t.Errorf("retained reaped identity recreated a new Open entry while its original creation permit remains valid")
	}
	f.clock.advance(20 * time.Minute)
	m.(*owner).watchdogPass(f.clock.deadline())
	cut, err := m.Freeze(f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	after, _, err := cut.Export()
	if err != nil {
		t.Fatal(err)
	}
	if len(after.Reaped) != 0 {
		t.Fatalf("expired imported reaped responsibility survives forever: %d", len(after.Reaped))
	}
}

func retainedMemories(f *fixture, count int) []x.ReapedEntry {
	memories := make([]x.ReapedEntry, count)
	for i := range memories {
		id := f.permit.Identity
		id.TransferID, id.EntryNonce = fmt.Sprintf("reaped-%04d", i), fmt.Sprintf("nonce-%04d", i)
		memories[i] = x.ReapedEntry{Identity: id, At: f.clock.now(), Retention: x.RetentionState{Identity: id,
			CreateUntil: f.permit.CreateUntil, ResultUntil: f.clock.now().Add(10 * time.Minute),
			Deadline: x.BootDeadline{BootID: "boot", Deadline: x.Deadline(f.clock.ns + int64(10*time.Minute))}}}
	}
	return memories
}

// origin: S7 internal review rv-gc F4
func TestTransferReapedAndLiveShareAdmissionCapacityAcrossCuts(t *testing.T) {
	f := newFixture(t, "a", "push")
	f.permit.Identity.ObjectVersion = ""
	f.permit.Identity.IntentDigest = digest([]byte("retained intent"))
	if _, err := f.owner.Admit(f.permit, f.intent, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	cut, err := f.owner.Freeze(f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := cut.Export()
	if err != nil {
		t.Fatal(err)
	}
	state.Reaped = retainedMemories(f, transferTrackerMaxEntries-1)
	encoded, err := xferimage.EncodeTracker(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := xferimage.DecodeTracker(encoded)
	if err != nil {
		t.Fatal(err)
	}
	capacity := &capacityProbe{}
	cfg := f.owner.cfg
	cfg.Capacity = capacity
	imported, err := Import(cfg, decoded)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := imported.Stop(f.clock.deadline()); err != nil {
			t.Error(err)
		}
	}()
	capacity.mu.Lock()
	held := len(capacity.live)
	capacity.mu.Unlock()
	if held != transferTrackerMaxEntries {
		t.Fatalf("retained slot credits=%d want=%d", held, transferTrackerMaxEntries)
	}
	if err := imported.Bind(x.Bindings{Ledger: f.ledger, OwnerGeneration: 2}, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	extra := f.permit
	extra.Identity.TransferID, extra.Identity.EntryNonce = "extra", "extra-nonce"
	if _, err := imported.Admit(extra, f.intent, f.clock.deadline()); err == nil {
		t.Fatal("live+reaped capacity admitted a 1025th responsibility")
	}
	f.clock.advance(5 * time.Minute)
	recut, err := imported.Freeze(f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	after, _, err := recut.Export()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after.Reaped, state.Reaped) {
		t.Fatal("cut expired or renewed a retained result before its original deadline")
	}
	if err := imported.Bind(x.Bindings{Ledger: f.ledger, OwnerGeneration: 3}, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	f.clock.advance(6 * time.Minute)
	imported.(*owner).watchdogPass(f.clock.deadline())
	recut, err = imported.Freeze(f.clock.deadline())
	if err != nil {
		t.Fatal(err)
	}
	after, _, err = recut.Export()
	if err != nil {
		t.Fatal(err)
	}
	capacity.mu.Lock()
	held = len(capacity.live)
	capacity.mu.Unlock()
	if len(after.Reaped) != 0 || held != 1 {
		t.Fatalf("expired memories retained credits: reaped=%d held=%d", len(after.Reaped), held)
	}
}

// origin: S7 internal review rv-gc F4
func TestTransferReapedCodecAndImportRejectDuplicatesAndOverflow(t *testing.T) {
	for _, defect := range []string{"duplicate", "live_collision", "overflow", "identity", "deadline"} {
		t.Run(defect, func(t *testing.T) {
			f := newFixture(t, "a", "push")
			f.permit.Identity.ObjectVersion = ""
			f.permit.Identity.IntentDigest = digest([]byte("retained intent"))
			if _, err := f.owner.Admit(f.permit, f.intent, f.clock.deadline()); err != nil {
				t.Fatal(err)
			}
			cut, err := f.owner.Freeze(f.clock.deadline())
			if err != nil {
				t.Fatal(err)
			}
			state, _, err := cut.Export()
			if err != nil {
				t.Fatal(err)
			}
			state.Reaped = retainedMemories(f, 1)
			switch defect {
			case "duplicate":
				state.Reaped = append(state.Reaped, state.Reaped[0])
			case "live_collision":
				state.Reaped[0].Identity, state.Reaped[0].Retention.Identity = f.permit.Identity, f.permit.Identity
			case "overflow":
				state.Reaped = retainedMemories(f, transferTrackerMaxEntries)
			case "identity":
				state.Reaped[0].Retention.Identity.EntryNonce = "other"
			case "deadline":
				state.Reaped[0].Retention.Deadline.Deadline = 0
			}
			if _, err := xferimage.EncodeTracker(state); err == nil {
				t.Fatal("codec accepted malformed retained responsibilities")
			}
			capacity := &capacityProbe{}
			cfg := f.owner.cfg
			cfg.Capacity = capacity
			if imported, err := Import(cfg, state); err == nil || imported != nil {
				t.Fatal("typed import accepted malformed retained responsibilities")
			}
			capacity.mu.Lock()
			held := len(capacity.live)
			capacity.mu.Unlock()
			if held != 0 {
				t.Fatal("rejected import leaked partially acquired retained credit")
			}
		})
	}
}
