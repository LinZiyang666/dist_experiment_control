package xfercontrol

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferfinal"
	"github.com/LinZiyang666/tether/internal/xfergc"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type bindingCapacity struct{ next atomic.Uint64 }

func (c *bindingCapacity) Reserve(x.Reservation) (x.ReservationID, error) {
	return x.ReservationID(fmt.Sprint(c.next.Add(1))), nil
}
func (*bindingCapacity) Release(x.ReservationID) error { return nil }

type bindingBoundary struct{ ledger x.LedgerPort }

func (b bindingBoundary) Census(_ x.Origin, d x.Deadline) (x.LedgerCensus, error) {
	return b.ledger.Scan(d)
}
func (bindingBoundary) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	return x.ReceiverOutcome{}, errors.New("unexpected receiver resolution")
}
func (bindingBoundary) Retire(x.Identity, x.RetirementProof, x.Deadline) (x.CleanupReceipt, error) {
	return x.CleanupReceipt{}, errors.New("unexpected object retirement")
}
func (bindingBoundary) Acquire(string, x.Deadline) (x.ConfigWriterProof, error) {
	return x.ConfigWriterProof{}, errors.New("unexpected config acquisition")
}
func (bindingBoundary) RetireConfig(x.ConfigWriterProof, x.Deadline) error {
	return errors.New("unexpected config retirement")
}

// ConfigAuthority and ObjectRetirer have different Retire signatures.
type bindingConfigAuthority struct{ bindingBoundary }

func (bindingConfigAuthority) Retire(x.ConfigWriterProof, x.Deadline) error {
	return errors.New("unexpected config retirement")
}

// All data owners are real. Authority/receiver/object ports refuse unexpected
// work: rebind must settle the exact ledger write before it can activate them.
// origin: S7 internal review rv-recovery F05
func TestTransferRuntimeRebindRetriesOriginalDirtyProposal(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRuntimeDirtyRebind(t)
	}
	testharness.AssertNoGoroutineLeak(t, "runtime dirty rebind x5", beforeG)
	testharness.AssertNoFDLeak(t, "runtime dirty rebind x5", beforeFD, 0)
}

func exerciseRuntimeDirtyRebind(t *testing.T) {
	t.Helper()
	t.Run("recovered_storage", func(t *testing.T) {
		f := newControlFixture(t)
		f.boot = f.origin.BootID
		base := f.config().Base
		base.Capacity = &bindingCapacity{}
		root := t.TempDir()
		primary, fallback := filepath.Join(root, "primary"), filepath.Join(root, "outbox")
		ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: primary, FallbackRoot: fallback})
		if err != nil {
			t.Fatal(err)
		}
		manager, err := xfertrack.New(x.TrackerConfig{Base: base, Origin: f.origin, Ledger: ledger})
		if err != nil {
			t.Fatal(err)
		}
		current := Runtime{Tracker: manager, Ledger: ledger}
		defer func() {
			if err := StopRuntime(current, testDeadline(t)); err != nil {
				t.Error(err)
			}
		}()
		cfg := f.config()
		cfg.Base, cfg.Tracker = base, manager
		current.Controller, err = New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		decisions, err := xfertrack.Decisions(manager)
		if err != nil {
			t.Fatal(err)
		}
		history := bindingHistory{}
		boundary := bindingBoundary{ledger: ledger}
		current.Finalizer, err = xferfinal.New(x.FinalConfig{Base: base, Decisions: decisions, Ledger: ledger, History: history, Objects: boundary, ResolveReceiver: boundary})
		if err != nil {
			t.Fatal(err)
		}
		nc, err := nats.Connect(testharness.StartJSNATS(t))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		current.Collector, err = xfergc.New(x.GCConfig{Base: base, JS: js, Owners: boundary, Finalizer: current.Finalizer})
		if err != nil {
			t.Fatal(err)
		}
		current.Buckets, err = xferbucket.New(x.BucketConfig{Base: base, JS: js, Authority: bindingConfigAuthority{boundary}})
		if err != nil {
			t.Fatal(err)
		}
		intent := f.intent
		intent.Tier, intent.ChunkSize, intent.Size = "a", 0, 6
		permit := f.permit
		permit.Identity.ObjectVersion = ""
		admitted, err := manager.Admit(permit, intent, testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		// Keep a clean sibling beside the dirty row. Import marks all retained
		// revisions ambiguous, so retry cannot assume every row is ledger.dirty.
		second := permit
		second.Identity.TransferID, second.Identity.EntryNonce = "sibling", "sibling-entry"
		if _, err := manager.Admit(second, intent, testDeadline(t)); err != nil {
			t.Fatal(err)
		}
		for _, path := range []string{primary, fallback} {
			if err := os.Chmod(path, 0500); err != nil {
				t.Fatal(err)
			}
		}
		defer func() { _ = os.Chmod(primary, 0700); _ = os.Chmod(fallback, 0700) }()
		_, err = manager.SourceSeal(permit.Identity, x.SourceSeal{Size: intent.Size, SHA256: intent.SHA256, SourceFingerprint: "original-source"}, testDeadline(t))
		if !errors.Is(err, xferledger.ErrDurability) {
			t.Fatalf("fault did not reach both writes: %v", err)
		}
		retained, err := ledger.Scan(testDeadline(t))
		if err == nil || retained.Ready || retained.Complete || len(retained.Rows) != 2 {
			t.Fatal("dirty proposal was acknowledged or lost")
		}
		cut, err := manager.Freeze(testDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		state, _, err := cut.Export()
		if err != nil {
			t.Fatal(err)
		}
		imported, err := xfertrack.Import(x.TrackerConfig{Base: base, Origin: f.origin, Ledger: ledger}, state)
		if err != nil {
			t.Fatal(err)
		}
		current.Tracker = imported
		defer func() {
			if err := manager.Stop(testDeadline(t)); err != nil {
				t.Error(err)
			}
		}()
		bindings := x.Bindings{Ledger: ledger, JS: js, Authority: cfg.Authority, Transport: cfg.Transport, OwnerGeneration: state.Generation + 1}
		if err := BindRuntime(current, bindings, testDeadline(t)); !errors.Is(err, xferledger.ErrDurability) {
			t.Fatalf("unwritable graph activated: %v", err)
		}
		for _, path := range []string{primary, fallback} {
			if err := os.Chmod(path, 0700); err != nil {
				t.Fatal(err)
			}
		}
		if err := BindRuntime(current, bindings, testDeadline(t)); err != nil {
			t.Fatalf("outer bind cannot retry retained proposal: %v", err)
		}
		durable, err := ledger.Scan(testDeadline(t))
		if err != nil || !durable.Ready || !durable.Complete || !reflect.DeepEqual(durable.Rows, retained.Rows) {
			t.Fatal("runtime activated without syncing its exact original proposals", err)
		}
		status, err := imported.Status(x.StatusRequest{Identity: permit.Identity, Origin: permit.Origin}, testDeadline(t))
		if err != nil || status.Identity != permit.Identity || !status.Budget.StartedAt.Equal(admitted.Budget.StartedAt) || status.Budget.EffectiveBudgetNS != admitted.Budget.EffectiveBudgetNS {
			t.Fatal("runtime rebind replaced original identity or budget", err)
		}
	})
}

type bindingHistory struct{}

func (bindingHistory) Confirm(x.CanonicalRecord, x.Deadline) (x.HistoryReceipt, error) {
	return x.HistoryReceipt{}, errors.New("unexpected history confirmation")
}
func (bindingHistory) Lookup(x.Key, x.Deadline) (x.HistorySet, error) {
	return x.HistorySet{}, errors.New("unexpected history lookup")
}
