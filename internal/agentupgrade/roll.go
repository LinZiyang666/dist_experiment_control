package agentupgrade

import (
	"context"
	"github.com/LinZiyang666/tether/internal/agentupgrade/agentroll"
	"github.com/LinZiyang666/tether/internal/agentupgradeport"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Parent orchestration uses the existing joined request lifetime and canonical
// component endpoint; its separate protocol never becomes a second engine.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
func isRollForwarded(body []byte) bool { return agentroll.IsForwarded(body) }
func replyRoll(ctx context.Context, d *Dependencies, e upgradeport.Endpoint, body []byte) ([]byte, error) {
	if d.leaseMu == nil {
		return nil, &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "agent host lease fence unavailable"}
	}
	has, release := agentupgradeport.ParentLeaseAccess(&d.Lease, d.leaseMu)
	return agentroll.Reply(ctx, &agentroll.Config{Key: d.Key, Store: d.Store, Runtime: d.Runtime, HasLease: has, ReleaseLease: release}, e, body)
}
