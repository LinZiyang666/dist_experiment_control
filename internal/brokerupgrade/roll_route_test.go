package brokerupgrade

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradeport"
)

// A nonnil decorator around absent wiring bypasses the wire's missing-port
// check and panics through its promoted Prepare/Query methods.
// origin: S8 internal review upgrade-maketest F1
func TestRollDecoratorPreservesMissingComponentEndpoint(t *testing.T) {
	var absent *engine
	for _, endpoint := range []upgradeport.Endpoint{nil, absent} {
		if WithRoll(endpoint, nil, nil, nil, nil) != nil {
			t.Fatal("missing component endpoint became callable through the roll decorator")
		}
	}
	endpoint := New(nil, nil, nil)
	if WithRoll(endpoint, nil, nil, nil, nil) == nil {
		t.Fatal("an available endpoint lost its roll route")
	}
}
