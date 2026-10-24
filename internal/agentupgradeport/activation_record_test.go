package agentupgradeport

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

type recordNoticeRuntime struct {
	upgradeport.RuntimePort
	activated *bool
}

func (r recordNoticeRuntime) Activate(upgradeport.Cut) (upgradeport.Activation, error) {
	*r.activated = true
	return upgradeport.Activation{}, errors.New("unit activation returns after notification")
}

// The early notice carries exactly the durable cut about to activate; an
// unrelated record remains unidentified and cannot authorize CLI Wait.
// origin: S8 internal review upgrade-maketest F8
func TestActivationNoticeRetainsTheAcceptedCutRecord(t *testing.T) {
	h := upgradetx.Handle{TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 3, Revision: 4}
	record := upgradetx.Record{Grant: upgradetx.Grant{Handle: h, ClientNonce: strings.Repeat("3", 32)}, Forward: upgradetx.Attempt{Handle: h, Direction: upgradetx.DirectionReverse}}
	for _, matches := range []bool{true, false} {
		var got upgradetx.Record
		activated := false
		ctx := context.WithValue(t.Context(), activationNoticeKey{}, func(r upgradetx.Record) {
			if activated {
				t.Fatal("notice followed activation")
			}
			got = r
		})
		ctx = WithActivationRecord(ctx, record)
		cut := upgradeport.Cut{Attempt: h}
		if !matches {
			cut.Attempt.Generation++
		}
		_, _ = ActivateNotifying(ctx, recordNoticeRuntime{activated: &activated}, cut)
		if !activated || matches && !reflect.DeepEqual(got, record) || !matches && !reflect.DeepEqual(got, upgradetx.Record{}) {
			t.Fatal("activation notice changed or borrowed the accepted identity")
		}
	}
}
