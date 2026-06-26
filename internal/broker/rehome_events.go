package broker

import (
	"errors"

	"github.com/LinZiyang666/tether/internal/cluster"
)

// rehome_events.go (C6 建议5) — the leader-gated, change-keyed, SECRET-FREE rehome/proxy observability
// event helpers. All emits go through b.pubSysEvent (which stamps v/type/ts onto the existing
// proto.SubjSysEvents). Every payload is a HAND-BUILT scalar map of allow-listed keys — a secret
// (token/PSK/cert key/session secret) can never enter because we never struct-splat an Allocation /
// ProxyDirective and `reason` is a CLOSED ENUM (raw errors go to the logger only).

// rehome event kinds + closed reason enum.
const (
	evHomeReassignStarted   = "home_reassign_started"
	evHomeReassignSucceeded = "home_reassign_succeeded"
	evHomeReassignFailed    = "home_reassign_failed"
	evRehomeStalled         = "rehome_stalled"
	evBrokerDownRehomeSum   = "broker_down_rehome_summary"

	reasonNoEligibleTarget = "no_eligible_target"
	reasonTargetNotReady   = "target_not_ready"
	reasonNotLeader        = "not_leader"
	reasonStoreError       = "store_error"
)

// classifyRehomeErr maps a Propose failure to the closed reason enum (never the raw error string).
func classifyRehomeErr(err error) string {
	if errors.Is(err, cluster.ErrForwardNotLeader) || cluster.IsNotLeader(err) {
		return reasonNotLeader
	}
	return reasonStoreError
}

// emitRehomeOnce emits kind with fields ONLY when the change-key mark for this port differs from the
// last one — so the ~5s reaper re-entry does not re-announce a steady condition (idle-zero-writes).
func (b *Broker) emitRehomeOnce(port int, mark, kind string, fields map[string]any) {
	if prev, ok := b.rehomeEvt.Load(port); ok && prev.(string) == mark {
		return
	}
	b.rehomeEvt.Store(port, mark)
	b.pubSysEvent(kind, fields)
}

// clearRehomeMark resets a port's change-key so a future transition re-announces. Called on a
// successful rehome + on the healthy path.
func (b *Broker) clearRehomeMark(port int) { b.rehomeEvt.Delete(port) }

// emitBrokerDownRehomeSummary (BD7) fires once per broker_down false→true transition with HONEST
// counts: proxy_homes the leader-gated reaper WILL rehome, and exposes_stranded (regular exposes are
// NOT auto-rehomed on a crash — stranded until a drain/return). Secret-free (two COUNT reads).
func (b *Broker) emitBrokerDownRehomeSummary(downBroker string) {
	var proxyHomes, exposesStranded int
	_ = b.cfg.DB.QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE name='__proxy__' AND state='ALLOCATED' AND home_broker=?`,
		downBroker).Scan(&proxyHomes)
	_ = b.cfg.DB.QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE name<>'__proxy__' AND state='ALLOCATED' AND home_broker=?`,
		downBroker).Scan(&exposesStranded)
	b.pubSysEvent(evBrokerDownRehomeSum, map[string]any{
		"down_broker":      downBroker,
		"proxy_homes":      proxyHomes,
		"exposes_stranded": exposesStranded,
	})
}
