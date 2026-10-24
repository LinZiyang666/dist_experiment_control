package brokerupgrade

import (
	"github.com/LinZiyang666/tether/internal/brokerupgradeport"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Prepared is negative execution evidence only when the sole journal writer
// still holds that grant and a live source sample closes its original scope.
// It neither creates Current nor treats an unknown query as prepared.
// origin: S8 internal review cluster-roll F3
func (e *engine) observePrepared(view upgradetx.View, n string) (upgradetx.View, error) {
	a, g := view.Historical.Forward, view.Historical.Grant
	stamp := makeStamp(a.Handle, g.Issuer, g.ScopeDigest, n)
	o, err := e.runtime.Observe(stamp)
	if err != nil {
		return view, err
	}
	if !o.OwnerKnown || !o.Census.Known || !o.Census.Complete || o.Census.Admitting != 0 || len(o.Census.OldOwners) != 0 || o.RunningSHA != g.SourceSHA || o.Census.Stamp.Producer != g.Issuer || !upgradetx.SameResources(o.Census.Resources, a.Scope.Expected) {
		return view, refusal(upgradetx.CodeDeferred, "prepared source census changed")
	}
	o.Current = a
	o.Census.Stamp.Attempt = a.Handle
	o.Census.Stamp.ScopeDigest = g.ScopeDigest
	o.Census.ExpectedDigest, _ = upgradetx.ExpectedDigest(a.Scope)
	o.At = o.Census.Stamp.At
	view.Observation = o
	return view, nil
}

// Parent cuts keep the adopted host reservation after component completion.
// Only the separately observed host-release barrier owns its final Close.
func ReleaseParentLease(deps *Dependencies) error {
	if deps == nil {
		return nil
	}
	deps.heldMu.Lock()
	defer deps.heldMu.Unlock()
	if deps.held == nil {
		return nil
	}
	if err := deps.held.Close(); err != nil {
		return err
	}
	deps.held = nil
	return nil
}

// ArmedTransaction reads the actual cut already armed by Activate. The broker
// uses its parent holder and CutID for S6 maintenance admission; callers cannot
// supply an unrelated maintenance record or extend its fixed lifetime.
func ArmedTransaction(deps *Dependencies) upgradetx.Grant {
	if deps == nil {
		return upgradetx.Grant{}
	}
	return brokerupgradeport.ArmedGrant(deps.Runtime, deps.Store)
}

// Retain the neutral runtime interface without changing frozen method sets.
var _ upgradeport.Endpoint = (*engine)(nil)
