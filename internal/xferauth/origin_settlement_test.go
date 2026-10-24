package xferauth

import (
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"testing"
)

type originSettlementOwners struct {
	*settlementPorts
	target  *settlementPorts
	origins []x.Origin
}

func (o *originSettlementOwners) SettlementOwner(origin x.Origin, _ x.Deadline) (x.TrackerPort, x.FinalizerPort, error) {
	o.origins = append(o.origins, origin)
	return o.target, o.target, nil
}

// A census identifies the original authority, not the broker handling rm.
// origin: S7 internal review rv-terminal F4 / rv-recovery F07
func TestSettlementRoutesCancelThroughOriginalOwner(t *testing.T) {
	_, original := settlementFixture(t)
	_, local := settlementFixture(t)
	local.row.Identity.TransferID = "local-transfer"
	original.advance = true
	owners := &originSettlementOwners{settlementPorts: original, target: original}
	barrier, err := NewSettlement(SettlementConfig{Owners: owners, Tracker: local, Ledger: original, Finalizer: local})
	if err != nil {
		t.Fatal(err)
	}
	_ = barrier.Settle(original.row.Identity.SID, true, authDeadline(t))
	if len(owners.origins) != 1 || owners.origins[0] != original.row.Origin || len(original.cancels) != 1 || len(local.cancels) != 0 {
		t.Fatalf("session removal used its local owner: origins=%v original=%v local=%v", owners.origins, original.cancels, local.cancels)
	}
}
