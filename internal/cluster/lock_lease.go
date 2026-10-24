package cluster

import (
	"database/sql"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster/state"
)

// SQL planning stays below Raft; this facade preserves the cluster API.
// origin: hot-update S6 plan §9.3 / CLUSTER-SLIM

const MetaKeyUpgradeLease = state.MetaKeyUpgradeLease

const MetaKeyGrowLease = state.MetaKeyGrowLease

const LockLeaseTTL = state.LockLeaseTTL

const LockLeaseRenewInterval = state.LockLeaseRenewInterval

var ErrNoLease = state.ErrNoLease

func LeaseExpiry(ro *sql.DB, key string) (time.Time, error) {
	return state.LeaseExpiry(ro, key)
}

func LeaseExpired(ro *sql.DB, key string, now time.Time) (bool, error) {
	return state.LeaseExpired(ro, key, now)
}

func PlanExpireUpgradeLease(now time.Time) (*Command, error) {
	cmd, err := state.PlanExpireUpgradeLease(now)
	return (*Command)(cmd), err
}

func PlanRenewUpgradeLease(now time.Time) (*Command, error) {
	cmd, err := state.PlanRenewUpgradeLease(now)
	return (*Command)(cmd), err
}

func PlanRenewGrowLease(joinerID string, now time.Time) (*Command, error) {
	cmd, err := state.PlanRenewGrowLease(joinerID, now)
	return (*Command)(cmd), err
}

const UpgradeHolderPrefix = state.UpgradeHolderPrefix

type UpgradeLockState = state.UpgradeLockState

var ErrUpgradeHolder = state.ErrUpgradeHolder

func ReadUpgradeLock(db *sql.DB) (UpgradeLockState, error) {
	return state.ReadUpgradeLock(db)
}

func UpgradeHolderMarker(holder string) (string, error) {
	return state.UpgradeHolderMarker(holder)
}

func PlanSetUpgradeActiveExpected(db *sql.DB, holder string, now time.Time) (*Command, error) {
	cmd, err := state.PlanSetUpgradeActiveExpected(db, holder, now)
	return (*Command)(cmd), err
}

func PlanRenewUpgradeLeaseExpected(db *sql.DB, holder string, now time.Time) (*Command, error) {
	cmd, err := state.PlanRenewUpgradeLeaseExpected(db, holder, now)
	return (*Command)(cmd), err
}

func PlanExpireUpgradeLeaseExpected(db *sql.DB, holder string, now time.Time) (*Command, error) {
	cmd, err := state.PlanExpireUpgradeLeaseExpected(db, holder, now)
	return (*Command)(cmd), err
}

func PlanClearUpgradeActiveExpected(db *sql.DB, holder string, now time.Time) (*Command, error) {
	cmd, err := state.PlanClearUpgradeActiveExpected(db, holder, now)
	return (*Command)(cmd), err
}

func PlanReapUpgradeLease(db *sql.DB, now time.Time) (*Command, error) {
	cmd, err := state.PlanReapUpgradeLease(db, now)
	return (*Command)(cmd), err
}
