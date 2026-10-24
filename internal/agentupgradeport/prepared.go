package agentupgradeport

import (
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"time"
)

// Querying an inert grant observes its original source without accepting it.
// The runtime census is independent of the prepared journal projection.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1
func ObservePrepared(runtime upgradeport.RuntimePort, v upgradetx.View, nonce string) (upgradetx.View, error) {
	a, g := v.Historical.Forward, v.Historical.Grant
	start := time.Now().UnixNano()
	o, err := runtime.Observe(upgradetx.Stamp{Attempt: a.Handle, Producer: g.Issuer, Nonce: nonce, At: start, ScopeDigest: g.ScopeDigest})
	if err != nil {
		return v, err
	}
	if o.Nonce != nonce || o.At < start || o.At > time.Now().UnixNano() || o.Census.Stamp.Nonce != nonce || !o.OwnerKnown || !o.Census.Known || !o.Census.Complete || o.Census.Admitting != 0 || len(o.Census.OldOwners) != 0 || o.RunningSHA != g.SourceSHA || o.Census.Stamp.Producer != g.Issuer || !upgradetx.SameResources(o.Census.Resources, a.Scope.Expected) {
		return v, refusal(upgradetx.CodeDeferred, "prepared agent source census changed", false)
	}
	o.Current = a
	o.Census.Stamp.Attempt = a.Handle
	o.Census.Stamp.ScopeDigest = g.ScopeDigest
	o.Census.ExpectedDigest, _ = upgradetx.ExpectedDigest(a.Scope)
	o.At = o.Census.Stamp.At
	v.Observation = o
	return v, nil
}
