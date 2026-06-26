package session

import (
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// proxy_plan.go (C5) — the leader-baked Plan helper for the proxy master switch + HA policy. In
// cluster mode the proxy CONTROL plane is Raft-committed (majority-write); this mirrors the single-mode
// SetProxyEnabled + BumpProxyEpoch as ONE all-literal Command. CHANGE-GATED: an identical re-propose
// (same enabled + same policy) is a RowsAffected==0 no-op, so the keyset epoch never double-bumps.
//
// ProxyHAPolicy values must match the 0016 CHECK constraint.
const (
	ProxyHAFreeze  = "freeze-on-quorum-loss"
	ProxyHADisable = "disable-on-quorum-loss"
)

// ValidProxyHAPolicy is the leader-side bake-surface validator (the column also has a CHECK, but a
// pre-propose validation gives a clean ctl error instead of a fsm-level failure).
func ValidProxyHAPolicy(p string) bool { return p == ProxyHAFreeze || p == ProxyHADisable }

// PlanProxySetEnabled flips proxy_enabled + the HA policy and bumps the keyset epoch, all change-gated
// on a real change to an ACTIVE session.
func PlanProxySetEnabled(sid string, enabled bool, haPolicy string, now time.Time) (*cluster.Command, error) {
	if !ValidProxyHAPolicy(haPolicy) {
		return nil, fmt.Errorf("session: plan proxy-set-enabled: invalid ha policy %q", haPolicy)
	}
	sidL, err := cluster.LitText(sid)
	if err != nil {
		return nil, fmt.Errorf("session: plan proxy-set-enabled: sid literal: %w", err)
	}
	enabledLit := "0"
	if enabled {
		enabledLit = "1"
	}
	polL := cluster.MustLitText(haPolicy)
	sql := `UPDATE sessions SET proxy_enabled=` + enabledLit + `, proxy_ha_policy=` + polL +
		`, proxy_epoch=proxy_epoch+1` +
		` WHERE sid=` + sidL + ` AND state='ACTIVE'` +
		` AND (proxy_enabled<>` + enabledLit + ` OR proxy_ha_policy<>` + polL + `)`
	return cluster.NewCommand(cluster.OpProxySetEnabled, cluster.Stmt(sql)), nil
}
