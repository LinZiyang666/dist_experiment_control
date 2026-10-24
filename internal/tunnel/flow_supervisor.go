package tunnel

import (
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/tunnel/flowcarrier"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
)

// Imported clients have no NATS/Start context yet. This supervisor belongs to
// the endpoint, and Close joins it after closing its stop channel.
func startRestoredSupervisor(c *Client, e *clientEndpoint) {
	c.mu.Lock()
	alreadyStarted := c.sessions[e.allocation.Port] != nil
	c.mu.Unlock()
	if alreadyStarted {
		return
	}
	st := c.flowState
	st.mu.Lock()
	if e.supervisorDone != nil || st.endpoints[e.allocation.Port] != e {
		st.mu.Unlock()
		return
	}
	e.supervisorDone = make(chan struct{})
	done := e.supervisorDone
	st.mu.Unlock()
	go func() {
		defer func() {
			st.mu.Lock()
			e.supervisorDone = nil
			close(done)
			st.mu.Unlock()
		}()
		for {
			st.mu.Lock()
			a := e.attachment
			st.mu.Unlock()
			if a == nil {
				return
			}
			select {
			case <-e.stop:
				return
			case <-a.Stop:
			}
			a.Join()
			c.mu.Lock()
			supervised := c.sessions[e.allocation.Port] != nil
			c.mu.Unlock()
			if supervised {
				return
			}
			// A restored endpoint has the same readiness responsibility as a
			// live port slot. Report the genuine lost attachment off all locks.
			// origin: hot-update S5 plan §4.4 / §7.1 / T38 / DRILL-RUN
			c.notifyState(e.allocation.Port, false)
			st.mu.Lock()
			if e.attachment != a {
				st.mu.Unlock()
				continue
			}
			if e.attachment == a {
				e.attachment = nil
			}
			if e.detachDeadline.IsZero() {
				e.detachDeadline = time.Now().Add(DetachedGrace)
			}
			deadline := e.detachDeadline
			frozen := st.frozen
			st.mu.Unlock()
			if frozen {
				return
			}
			if err := resumeClientEndpoint(c, e, deadline); err != nil {
				for _, r := range flow.States(st.manager, e.id) {
					_ = st.manager.RevokeFlow(r.ID, flow.ReasonRevoked)
				}
				return
			}
			c.notifyState(e.allocation.Port, true)
		}
	}()
}

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func flushControls(a *flowcarrier.Carrier, batch map[flowcarrier.ControlKey]flow.Frame) error {
	return flowcarrier.FlushControls(a, batch)
}

// Pure value operations stay below the runtime owner.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func clientDeadlineBoot(t time.Time) (uint64, error)    { return flowstate.ClientDeadlineBoot(t) }
func clientBootDeadline(boot uint64) (time.Time, error) { return flowstate.ClientBootDeadline(boot) }
