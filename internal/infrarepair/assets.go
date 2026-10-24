package infrarepair

import (
	"sort"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

func (g *Gate) readAssets(deadline c.Deadline, stop c.Stop) (c.AssetSeal, error) {
	if err := admission(deadline, stop); err != nil {
		return c.AssetSeal{}, err
	}
	assets, err := g.cfg.Assets(deadline, stop)
	if err != nil {
		return c.AssetSeal{}, err
	}
	if !assets.Complete || assets.Accounts == (c.Digest{}) || assets.Assets == (c.Digest{}) {
		return c.AssetSeal{}, c.ErrIncomplete
	}
	names := make([]string, 0, len(g.producers))
	for name := range g.producers {
		names = append(names, name)
	}
	sort.Strings(names)
	assets.Revision, assets.Participants = g.revision, digest(names)
	return assets, admission(deadline, stop)
}

// ReadSeal is P11's SealSource. Preflight uses a short joined fence and releases
// it after reading; the revision detects any mutation between P11's two reads.
// A recovery fence remains closed until VerifyBeforeReplay and Release complete.
// origin: hot-update S9 plan §3.2 / T41
func (g *Gate) ReadSeal(plan c.Plan, deadline c.Deadline, stop c.Stop) (c.AssetSeal, error) {
	if err := g.beginOperation(deadline, stop); err != nil {
		return c.AssetSeal{}, err
	}
	defer g.endOperation()
	if err := admission(deadline, stop); err != nil {
		return c.AssetSeal{}, err
	}
	g.mu.Lock()
	if plan.Tx == (c.ID{}) || plan.Domain != g.cfg.Domain || (g.closed && g.plan != plan) {
		g.mu.Unlock()
		return c.AssetSeal{}, ErrFenced
	}
	temporary := !g.closed
	g.closed = true
	g.mu.Unlock()
	if temporary {
		defer func() { g.mu.Lock(); g.closed = false; g.mu.Unlock() }()
	}
	if err := g.join(deadline); err != nil {
		return c.AssetSeal{}, err
	}
	if err := g.cfg.Exclusive(deadline, stop); err != nil {
		return c.AssetSeal{}, err
	}
	assets, err := g.readAssets(deadline, stop)
	if err != nil {
		return c.AssetSeal{}, err
	}
	if !temporary && (g.checkpoint.Proof == (c.Digest{}) || assets != g.checkpoint.Assets) {
		return c.AssetSeal{}, ErrReceipt
	}
	return assets, nil
}
