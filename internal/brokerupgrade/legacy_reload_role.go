package brokerupgrade

import (
	"database/sql"
	"errors"

	"github.com/LinZiyang666/tether/internal/cluster"
)

var ErrLegacyReloadLeader = errors.New("broker: transfer leadership before reloading a broker with multiple voters")

// CheckLegacyReloadRole preserves the independent S6 topology check alongside
// the common legacy admission policy. Keeping it here offsets the added gates
// in brokerimage without increasing that owner's frozen structural budget.
// origin: hot-update S6 plan §5 / T37; S8 internal review rv3-leaves F2
func CheckLegacyReloadRole(n *cluster.Node) error {
	if !n.IsLeader() {
		return nil
	}
	cfg, err := n.RaftConfiguration()
	if err != nil {
		return err
	}
	voters, self := 0, false
	for _, server := range cfg {
		if server.Voter {
			voters++
			self = server.NodeID == n.SelfID()
		}
	}
	if voters != 1 || !self {
		return ErrLegacyReloadLeader
	}
	return n.VerifyLeaderRead(func(*sql.DB) error { return nil })
}
