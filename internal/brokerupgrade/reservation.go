package brokerupgrade

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
)

// CodeLockNotHeld (R7b) tells a lease renewer the roll lock is gone. TERMINAL for the renewer: a
// renewal is structurally incapable of re-creating a released lock. It is a WIRE value shared with
// the grow trigger and cmd/tether's lock keeper (TestLockNotHeldCodeIsWireStable pins the ends).
const CodeLockNotHeld = "lock_not_held"

// UpgradeLock serves the signed acquire/renew/expire/release roll-lock actions. The cluster upgrade
// expected-holder lease is the full-fleet release reservation S8 reuses: no new Raft op, and every
// write is a replicated plan plus an exact readback CAS. The caller has already verified the account
// signature, target and replay window, and supplies the live Node (nil node is refused by the broker).
// origin: hot-update S6 plan §7.1 / T40; S8 plan §6.3 / §12.1 (P15 trigger net-zero move)
func UpgradeLock(node *cluster.Node, req *proto.ClusterUpgradeReq, now time.Time) *proto.ClusterUpgradeResp {
	return handleUpgradeLock(node, req, now)
}

// handleUpgradeLock keeps its historical name: the error-code scanner's finite local-code rule for
// the lock mapper is keyed on it, so the move cannot turn this body into an unresolved emitter.
func handleUpgradeLock(node *cluster.Node, req *proto.ClusterUpgradeReq, now time.Time) *proto.ClusterUpgradeResp {
	var expected cluster.UpgradeLockState
	err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
		state, err := cluster.ReadUpgradeLock(db)
		if err != nil {
			return nil, err
		}
		// A legacy signed verb can never operate on an S6 lock, even after expiry.
		if req.Holder == "" && strings.HasPrefix(state.Marker, cluster.UpgradeHolderPrefix) {
			return nil, cluster.ErrUpgradeHolder
		}
		var cmd *cluster.Command
		switch req.Op {
		case "acquire-lock":
			ops, e := cluster.NonTerminalOperations(db)
			if e != nil {
				return nil, e
			}
			if len(ops) > 0 {
				return nil, fmt.Errorf("a cluster membership operation (join/retire) is in flight: %w", cluster.ErrUpgradeHolder)
			}
			if req.Holder != "" {
				cmd, err = cluster.PlanSetUpgradeActiveExpected(db, req.Holder, now)
			} else {
				cmd, err = cluster.PlanSetUpgradeActive(now)
			}
			if err == nil {
				expected.Marker = cluster.UpgradeMarkerStamp(now)
				if req.Holder != "" {
					expected.Marker, err = cluster.UpgradeHolderMarker(req.Holder)
				}
				expected.Lease = now.Add(cluster.LockLeaseTTL).UTC().Format(time.RFC3339Nano)
			}
		case "renew-lock":
			if req.Holder != "" {
				cmd, err = cluster.PlanRenewUpgradeLeaseExpected(db, req.Holder, now)
			} else {
				cmd, err = cluster.PlanRenewUpgradeLease(now)
			}
			expected = state
			expected.Lease = now.Add(cluster.LockLeaseTTL).UTC().Format(time.RFC3339Nano)
		case "expire-lock-lease":
			if req.Holder != "" {
				cmd, err = cluster.PlanExpireUpgradeLeaseExpected(db, req.Holder, now)
			} else {
				cmd, err = cluster.PlanExpireUpgradeLease(now)
			}
			expected = state
			expected.Lease = now.Add(-time.Second).UTC().Format(time.RFC3339Nano)
		case "release-lock":
			if req.Holder != "" {
				cmd, err = cluster.PlanClearUpgradeActiveExpected(db, req.Holder, now)
			} else {
				cmd, err = cluster.PlanClearUpgradeActive()
			}
		}
		return cmd, err
	})
	if err != nil {
		code := adminsock.CodeStoreError
		if cluster.IsNotLeader(err) {
			code = adminsock.CodeNotLeader
		}
		if errors.Is(err, cluster.ErrUpgradeHolder) {
			code = adminsock.CodeBadRequest
		}
		return &proto.ClusterUpgradeResp{Code: code, Error: "upgrade lock operation refused: " + err.Error()}
	}
	actual, err := cluster.ReadUpgradeLock(node.RODB())
	if err != nil {
		return &proto.ClusterUpgradeResp{Code: adminsock.CodeStoreError, Error: "upgrade lock readback failed"}
	}
	if actual != expected {
		code := adminsock.CodeBadRequest
		if req.Op == "renew-lock" && actual.Marker == "" {
			code = CodeLockNotHeld
		}
		return &proto.ClusterUpgradeResp{Code: code, Error: "upgrade lock compare-and-swap did not commit"}
	}
	return &proto.ClusterUpgradeResp{OK: true}
}

// CheckHolder: the local reload owner performs its own holder acquire/check. Remote mutations
// cannot bypass a live S6 roll with an old signature or a different holder. A nil node is a
// single broker, where only an empty holder is meaningful.
// origin: hot-update S6 plan §7.1 / T40
func CheckHolder(node *cluster.Node, holder string, now time.Time) error {
	if node == nil {
		if holder != "" {
			return cluster.ErrUpgradeHolder
		}
		return nil
	}
	state, err := cluster.ReadUpgradeLock(node.RODB())
	if err != nil {
		return err
	}
	if holder == "" && !strings.HasPrefix(state.Marker, cluster.UpgradeHolderPrefix) {
		return nil
	}
	marker, err := cluster.UpgradeHolderMarker(holder)
	if err != nil || marker != state.Marker {
		return cluster.ErrUpgradeHolder
	}
	expiry, err := time.Parse(time.RFC3339Nano, state.Lease)
	if err != nil || !now.Before(expiry) {
		return cluster.ErrUpgradeHolder
	}
	return nil
}
