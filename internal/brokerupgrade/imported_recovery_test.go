package brokerupgrade

import (
	"errors"
	"reflect"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review roll-recovery-stall F3
func TestImportedBrokerRecoveryRequiresTheCarriedActivationProof(t *testing.T) {
	for _, defect := range []string{"none", "before activation", "transaction", "cut", "generation", "source running", "unknown owner", "stale nonce", "stale current revision", "missing control", "completed"} {
		t.Run(defect, func(t *testing.T) {
			f := newEngineFixture()
			f.runtime.activateErr = &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: ReasonActivationDispatched, Accepted: true}
			v, err := f.endpoint.Start(t.Context(), f.artifact.Grant, f.artifact)
			if !activationDispatched(err) {
				t.Fatalf("missing armed activation: %v", err)
			}
			carried := handle(v.Historical)
			settleReturned(f.endpoint, carried, errors.New("arm reply lost"))
			switch defect {
			case "before activation":
				carried.Revision--
			case "transaction":
				carried.TxID = nonce()
			case "cut":
				carried.CutID = nonce()
			case "generation":
				carried.Generation++
			case "source running":
				f.runtime.running = f.artifact.SourceSHA
			case "unknown owner":
				f.runtime.observe = func(o *upgradetx.Observation) { o.OwnerKnown = false }
			case "stale nonce":
				f.runtime.observe = func(o *upgradetx.Observation) { o.Nonce = "previous-probe" }
			case "stale current revision":
				f.runtime.observe = func(o *upgradetx.Observation) { o.Current.Handle.Revision-- }
			case "missing control":
				f.runtime.observe = func(o *upgradetx.Observation) { o.Current.Control.Known = false }
			case "completed":
				f.store.record.Forward.Phase = upgradetx.PhaseCompleted
			}
			before := cloneRecord(f.store.record)
			got := importedRecoveryHandle(f.endpoint, carried)
			want := carried
			if defect == "none" {
				want = handle(before)
			}
			if got != want || !reflect.DeepEqual(before, f.store.record) || f.runtime.activateCalls != 1 {
				t.Fatalf("invalid recovery authority or repeated effect: got=%+v want=%+v effects=%d", got, want, f.runtime.activateCalls)
			}
		})
	}
}
