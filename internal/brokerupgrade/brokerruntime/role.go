package brokerruntime

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/storage"
)

// ReloadRole re-validates an admitted cut's role at every reload step against
// the CURRENT Node, never a cached sample. Before the store stops it requires
// the live maintenance record and roll holder (or a sole voter), the admitted
// role, the reload topology and an empty membership census; at the exec latch
// it compares the final role receipt within the one permitted leader edge.
// holder checks the roll lock for the record's holder.
// origin: hot-update S6 plan §5 / internal review rv-cluster-7; S8 §6.3 / §12.1 (P15 net-zero move)
func ReloadRole(node *cluster.Node, selfID string, expected storage.RoleReceipt, bound int64, record brokermaint.Record, stopped bool, holder func(string) error) error {
	if record.CutID != "" {
		now, clockErr := procrequest.BootNow()
		if clockErr != nil || now >= bound || record.Phase != brokermaint.MaintActive {
			return brokermaint.ErrExpired
		}
	}
	if !stopped {
		if record.CutID != "" {
			current, err := cluster.ReadBrokerMaintenance(node.RODB(), selfID)
			if err != nil || current != record {
				return brokermaint.ErrIdentity
			}
			if err := holder(record.Holder); err != nil {
				return err
			}
		} else if voters, err := node.NumVoters(); err != nil || voters != 1 {
			return brokermaint.ErrIdentity
		}
		if err := cluster.ValidateImageRole(node, expected); err != nil {
			return err
		}
		if err := brokerimage.CheckReloadRole(node); err != nil {
			return err
		}
		ops, err := cluster.NonTerminalOperations(node.RODB())
		if err != nil {
			return err
		}
		if len(ops) != 0 {
			return errors.New("broker: membership operation entered after reload admission")
		}
		return nil
	}
	current, err := cluster.ReadImageRole(node)
	if err != nil {
		return err
	}
	maximumEdge := expected.EdgeSeq
	if expected.LastRunningIsLeader {
		maximumEdge++
	}
	if !current.ObserverClosed || current.EdgeSeq < expected.EdgeSeq || current.EdgeSeq > maximumEdge ||
		current.Term != expected.Term || current.ConfigIndex != expected.ConfigIndex || current.VoterDigest != expected.VoterDigest {
		return errors.New("broker: image role or membership changed before exec")
	}
	return nil
}

// UpgradeLockDecision is the whole roll-lock reap decision, isolated from raft so it can be
// tested exhaustively against a plain SQLite fixture. It performs NO writes.
// origin: R7b defect P3; S8 §12.1 (P15 net-zero move)
func UpgradeLockDecision(ro *sql.DB, now time.Time) (clear bool, err error) {
	if !brokercontrol.UpgradeActive(ro) {
		return false, nil // already converged: nothing held, nothing written
	}
	expired, err := cluster.LeaseExpired(ro, cluster.MetaKeyUpgradeLease, now)
	if err != nil {
		return false, fmt.Errorf("read upgrade lease: %w", err)
	}
	return expired, nil
}
