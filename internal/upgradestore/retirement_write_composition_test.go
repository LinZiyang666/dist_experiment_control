package upgradestore_test

import (
	"errors"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentupgrade"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type refusedReleaseStore struct{ *upgradestore.Store }

type reverseResultCutStore struct{ *upgradestore.Store }

func (s *reverseResultCutStore) Apply(h upgradetx.Handle, event upgradetx.Event) (upgradetx.Record, error) {
	r, err := s.Store.Apply(h, event)
	if err == nil && r.Reverse.Handle.TxID != "" && event.Action.Kind == upgradetx.ActionSelect && event.Action.State == upgradetx.ActionObserved {
		panic(journalCut{})
	}
	return r, err
}

func (*refusedReleaseStore) RecordForwardRelease(upgradetx.Handle, upgradetx.Receipt) (upgradetx.Record, error) {
	return upgradetx.Record{}, errors.New("forward release write refused")
}

// origin: S8 internal review rv3-leaves F3
func TestForwardReleaseWriteFailureKeepsRecoveryIdentity(t *testing.T) {
	r := newComposedRuntime(t, upgradetx.ComponentAgent)
	r.failActivate = true
	originalTx := r.grant.Handle.TxID
	s := &refusedReleaseStore{Store: r.store}
	v, err := agentupgrade.New(s, r, r.registry).Start(t.Context(), r.grant, r.receipt)
	if err == nil || r.effects != 1 || v.Historical.Key != r.options.Key || v.Historical.Forward.Handle.TxID != originalTx || v.Historical.Reverse.Handle.TxID == "" {
		t.Fatalf("failed write erased the last known reverse identity: key=%+v effects=%d err=%v", v.Historical.Key, r.effects, err)
	}
	r.reopen()
	loaded, err := r.store.Load(r.options.Key, "")
	if err != nil || composedAttempt(loaded.Historical).Phase != upgradetx.PhaseRecoveryRequired || loaded.Historical.Revision != v.Historical.Revision {
		t.Fatalf("failed release did not retain a resumable recovery identity: %v", err)
	}
}

// origin: S8 internal review rv3-leaves F3
func TestAutomaticReverseResumesItsOwnCutAfterJournalReopen(t *testing.T) {
	r := &oneFinalProbeFailure{composedRuntime: newComposedRuntime(t, upgradetx.ComponentAgent)}
	s := &reverseResultCutStore{Store: r.store}
	if !runUntilJournalCut(t, func() { _, _ = agentupgrade.New(s, r, r.registry).Start(t.Context(), r.grant, r.receipt) }) || r.effects != 2 {
		t.Fatal("reverse result crash did not follow both unique activations")
	}
	r.reopen()
	h := r.current().Handle
	v, err := agentupgrade.Boot(t.Context(), r.endpoint(), h)
	if err != nil || v.Historical.Forward.Phase != upgradetx.PhaseRolledBack || v.Historical.Reverse.Phase != upgradetx.PhaseCompleted || r.effects != 2 {
		t.Fatalf("reverse could not attach its own durable cut after reopen: effects=%d err=%v", r.effects, err)
	}
}
