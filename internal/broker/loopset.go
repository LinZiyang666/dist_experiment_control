package broker

import (
	"github.com/LinZiyang666/tether/internal/brokercontrol"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/controlbudget"
)

// Compatibility adapters keep callers bound to the extracted control owner.
// origin: hot-update S6 plan §9.3 / BROKER-SLIM
type loopSet = brokercontrol.LoopSet
type loopStat = brokercontrol.LoopStat

func newLoopSet() *loopSet { return brokercontrol.NewLoopSet() }

// joinBrokerLoops is the image join: an expired budget is an error, not a
// warning followed by a store close. Completion is a condition, not a consumable
// token, so a refused cut can retry without losing an earlier loop receipt.
// origin: hot-update S6 plan §3.2 / T01
func joinBrokerLoops(l *loopSet, deadline controlbudget.Deadline) error {
	return brokerimage.JoinStoreUsers(deadline, func() (bool, <-chan struct{}) {
		l.Mu.Lock()
		defer l.Mu.Unlock()
		l.Started = true
		if l.Completed == len(l.Names) {
			return true, nil
		}
		if l.Changed == nil {
			l.Changed = make(chan struct{})
		}
		return false, l.Changed
	})
}

// JoinBrokerStoreLoops is used after ControlOwner cancels its current roots.
// Cancellation remains with the control owner; joining never cancels Run.
func JoinBrokerStoreLoops(b *Broker, deadline controlbudget.Deadline) error {
	if b.cl == nil || b.cl.loops == nil {
		return nil
	}
	return joinBrokerLoops(b.cl.loops, deadline)
}
