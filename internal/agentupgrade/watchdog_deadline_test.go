package agentupgrade

import (
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review activation-and-route-migration F10
func TestImportedWatchdogKeepsOriginalAbsoluteDeadline(t *testing.T) {
	f := fixture(t)
	record := upgradetx.Record{Grant: f.grant, Revision: 17, Forward: upgradetx.Attempt{Handle: f.grant.Handle}, RecordedPreviousSHA: f.grant.SourceSHA}
	cut := upgradecut.State{Attempt: f.grant.Handle, Owner: f.grant.Issuer, SourceSHA: f.grant.SourceSHA, TargetSHA: f.grant.TargetSHA,
		Authorities: []upgradecut.Authority{{SID: f.grant.Handle.Key.SID, Incarnation: "accepted-incarnation"}}}
	for _, age := range []time.Duration{time.Second, 90 * time.Second, 121 * time.Second} {
		deadline := time.Now().Add(upgradebudget.ComponentHandover - age).UnixNano()
		record.Grant.ExpiresAt = deadline
		var deps Dependencies
		captureWatchdog(&deps, record, cut)
		if deps.WatchdogDeadline != deadline {
			t.Fatal("import reset the original absolute handover deadline")
		}
		if deps.WatchdogFence.Current.Revision != record.Revision || !upgradetx.SameCut(deps.WatchdogFence.Current, cut.Attempt) ||
			deps.WatchdogFence.Owner != cut.Owner || deps.WatchdogFence.Incarnation != cut.Authorities[0].Incarnation || deps.WatchdogFence.RunningSHA != cut.TargetSHA {
			t.Fatal("watchdog captured a different owner or cut")
		}
		if deps.WatchdogReverse.HistoricalTxID != cut.Attempt.TxID || deps.WatchdogReverse.TargetSHA != record.RecordedPreviousSHA ||
			deps.WatchdogReverse.ExpiresAt != deadline+int64(upgradebudget.ComponentHandover) {
			t.Fatal("reverse authorization drifted from the original deadline and previous image")
		}
	}
}
