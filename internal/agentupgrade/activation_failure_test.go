package agentupgrade

import (
	"errors"
	"strings"
	"syscall"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// origin: S8 internal review activation-and-route-migration F1
func TestAgentRecoveryKeepsInitiatingActivationError(t *testing.T) {
	f := fixture(t)
	f.activation = func(upgradeport.Cut) error { return syscall.EACCES }
	_, err := f.endpoint.Start(t.Context(), f.grant, f.artifact)
	if !errors.Is(err, syscall.EACCES) {
		t.Fatalf("secondary recovery refusal erased exec errno: %v", err)
	}
	response := upgradewire.ErrorResponse(err)
	if response.Code != upgradetx.CodeUnsettled || strings.Contains(response.Error, syscall.EACCES.Error()) {
		t.Fatalf("private initiating error crossed public recovery response: %+v", response)
	}
	// A reverse has an additional unresolved-record projection. It must retain
	// the initiating cause too, including when a watchdog is its caller.
	t.Run("reverse", func(t *testing.T) {
		f := importedFixture(t, upgradetx.CutPartialActive)
		view, err := f.store.Load(f.grant.Handle.Key, f.grant.Handle.TxID)
		if err != nil {
			t.Fatal(err)
		}
		fence, request := reverseInputs(f, view)
		f.activation = func(upgradeport.Cut) error { return syscall.EACCES }
		_, err = f.endpoint.Rollback(t.Context(), fence, request)
		if !errors.Is(err, syscall.EACCES) {
			t.Fatalf("reverse recovery projection erased exec errno: %v", err)
		}
	})
}
