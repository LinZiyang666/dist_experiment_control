package broker

import "github.com/LinZiyang666/tether/internal/brokercontrol"

// rehome_events.go (C6 建议5) — the leader-gated, change-keyed, SECRET-FREE rehome/proxy observability
// event helpers. All emits go through b.pubSysEvent (which stamps v/type/ts onto the existing
// proto.SubjSysEvents). Every payload is a HAND-BUILT scalar map of allow-listed keys — a secret
// (token/PSK/cert key/session secret) can never enter because we never struct-splat an Allocation /
// ProxyDirective and `reason` is a CLOSED ENUM (raw errors go to the logger only).

const evHomeReassignStarted = brokercontrol.EvHomeReassignStarted
const evHomeReassignSucceeded = brokercontrol.EvHomeReassignSucceeded
const evHomeReassignFailed = brokercontrol.EvHomeReassignFailed
const evRehomeStalled = brokercontrol.EvRehomeStalled
const evBrokerDownRehomeSum = brokercontrol.EvBrokerDownRehomeSum
const reasonNoEligibleTarget = brokercontrol.ReasonNoEligibleTarget
const reasonTargetNotReady = brokercontrol.ReasonTargetNotReady

func classifyRehomeErr(err error) string { return brokercontrol.ClassifyRehomeErr(err) }

// emitRehomeOnce emits kind with fields ONLY when the change-key mark for this port differs from the
// last one — so the ~5s reaper re-entry does not re-announce a steady condition (idle-zero-writes).
func (b *Broker) emitRehomeOnce(port int, mark, kind string, fields map[string]any) {
	// Checked: on a type mismatch the mark does not match, so the event is re-announced. Announcing a
	// steady condition twice is the harmless direction; a panic in the reaper is not.
	if prev, ok := b.rehomeEvt.Load(port); ok {
		if s, isStr := prev.(string); isStr && s == mark {
			return
		}
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
	_ = b.read().QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE name='__proxy__' AND state='ALLOCATED' AND home_broker=?`,
		downBroker).Scan(&proxyHomes)
	_ = b.read().QueryRow(
		`SELECT COUNT(*) FROM port_allocations WHERE name<>'__proxy__' AND state='ALLOCATED' AND home_broker=?`,
		downBroker).Scan(&exposesStranded)
	b.pubSysEvent(evBrokerDownRehomeSum, map[string]any{
		"down_broker":      downBroker,
		"proxy_homes":      proxyHomes,
		"exposes_stranded": exposesStranded,
	})
}
