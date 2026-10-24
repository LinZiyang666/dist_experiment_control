package xfertrack

import (
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"testing"
)

// Copy into internal/xfertrack. The ctl and tracker must consume the same
// frozen tier-A rate budget, without silently allocating different deadlines.
// origin: S7 internal review rv-wiring F8
func TestTransferInlineRateBudgetMatchesClient(t *testing.T) {
	for _, rate := range []int64{0, 32768, 65536, 69906, 1 << 20} {
		intent := x.Intent{Verb: "push", Tier: "a", Size: 1024 * 1024, RateLimitBPS: rate}
		client, clientErr := proto.TransferBudget(intent.Tier, intent.Size, rate)
		tracker, trackerErr := budgetFor(intent, intent.Size, false)
		refused := rate != 0 && rate < 69906
		if (clientErr != nil) != refused || (trackerErr != nil) != refused || client != tracker {
			t.Fatalf("rate=%d: ctl=%s/%v tracker=%s/%v want refused=%v", rate, client, clientErr, tracker, trackerErr, refused)
		}
		if !refused && client != proto.XferTimeoutTierA {
			t.Fatalf("inline budget escaped 30s: %v", client)
		}
	}
}
