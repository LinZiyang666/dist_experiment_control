package tunnel

import (
	"context"
	"errors"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel/flowcarrier"
	"github.com/LinZiyang666/tether/internal/tunnel/flowstate"
	"github.com/hashicorp/yamux"
)

// Pin rotation changes the route retained by the image as well as the legacy
// supervisor slot; otherwise an exec would resurrect an expired certificate.
func syncClientRoute(c *Client, port int, addr string, pins proto.CertPins) {
	if st := c.flowState; st != nil {
		st.mu.Lock()
		defer st.mu.Unlock()
		if e := st.endpoints[port]; e != nil {
			if addr != "" {
				e.brokerAddr = addr
			}
			if pins.Current != "" {
				e.pins = pins
			}
		}
	}
}

// The runtime retains its API while the leaf owns this implementation.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func retainedClientOpenReceipt(manager *flow.Manager, req flow.OpenRequest) (flow.State, error) {
	return flowcarrier.RetainedClientOpenReceipt(manager, req)
}

// Pure value operations stay below the runtime owner.
// origin: hot-update S6 plan §9.3 / TUNNEL-SLIM
func minTime(a time.Time, b time.Time) time.Time { return flowstate.MinTime(a, b) }

// origin: hot-update S6 plan §9.3 / T53; carrier resume and binding stay with the client binding owner.
func (c *Client) ResumeCarriers(deadline time.Time) error {
	st := c.flowState
	if st == nil {
		return errFlowState
	}
	if deadline.IsZero() {
		deadline = time.Now().Add(DetachedGrace)
	}
	st.mu.Lock()
	st.frozen = false
	var endpoints []*clientEndpoint
	for _, e := range st.endpoints {
		endpoints = append(endpoints, e)
	}
	st.mu.Unlock()
	sort.SliceStable(endpoints, func(i, j int) bool {
		return len(flow.States(st.manager, endpoints[i].id)) > len(flow.States(st.manager, endpoints[j].id))
	})
	jobs := make(chan *clientEndpoint)
	errs := make(chan error, len(endpoints))
	var workers sync.WaitGroup
	for range min(flow.MaxFlows, len(endpoints)) {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for e := range jobs {
				c.mu.Lock()
				supervised := c.sessions[e.allocation.Port] != nil
				c.mu.Unlock()
				// A live port supervisor owns transport publication as well as
				// redial. Wait for that same slot to install its new attachment;
				// a parallel image-recovery dial could otherwise finish first and
				// leave the supervisor observing a superseded mux on the next cut.
				// origin: hot-update S5 internal review HOTUPDATE-T40
				if supervised {
					if err := waitSupervisedCarrier(c, e, deadline); err != nil {
						errs <- err
					}
					continue
				}
				st.mu.Lock()
				endpointDeadline := e.detachDeadline
				attachment := e.attachment
				st.mu.Unlock()
				if endpointDeadline.IsZero() || deadline.Before(endpointDeadline) {
					endpointDeadline = deadline
				}
				// A repeated recovery call uses its completed attachment. Replacing
				// it invites the supervisor to race another redial against Freeze.
				// origin: hot-update S5 plan §4.4 / T32
				if attachment != nil {
					select {
					case <-attachment.Stop:
					default:
						if err := waitClientBindings(c, e, endpointDeadline); err != nil {
							errs <- err
						}
						continue
					}
				}
				if err := resumeClientEndpoint(c, e, endpointDeadline); err != nil {
					errs <- err
					continue
				}
				startRestoredSupervisor(c, e)
				// Restored owners lack the ordinary port supervisor's up edge.
				// Publish only after accepted RESUME and all original bindings,
				// so planned SS readiness can retire its absolute grace timer.
				// origin: hot-update S5 plan §4.4 / §7.1 / T38 / DRILL-RUN
				c.notifyState(e.allocation.Port, true)
			}
		}()
	}
	for _, e := range endpoints {
		jobs <- e
	}
	close(jobs)
	workers.Wait()
	close(errs)
	return errors.Join(channelErrors(errs)...)
}

func waitSupervisedCarrier(c *Client, e *clientEndpoint, deadline time.Time) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	for {
		select {
		case <-e.stop:
			return errFlowState
		default:
		}
		c.mu.Lock()
		session := c.sessions[e.allocation.Port]
		var mux *yamux.Session
		if session != nil {
			mux = session.yamuxSess
		}
		c.mu.Unlock()
		c.flowState.mu.Lock()
		a := e.attachment
		c.flowState.mu.Unlock()
		if a != nil && a.Mux == mux && !mux.IsClosed() {
			if err := waitClientBindings(c, e, deadline); err == nil {
				return nil
			}
		}
		select {
		case <-e.stop:
			return errFlowState
		case <-timer.C:
			return os.ErrDeadlineExceeded
		case <-ticker.C:
		}
	}
}
func resumeClientEndpoint(c *Client, e *clientEndpoint, deadline time.Time) error {
	delay := 250 * time.Millisecond
	for time.Now().Before(deadline) {
		select {
		case <-e.stop:
			return errFlowState
		default:
		}
		attempt := time.Now().Add(ResumeAttempt)
		if deadline.Before(attempt) {
			attempt = deadline
		}
		// ctx-none: restored carrier operations have their own absolute deadline.
		ctx, cancel := context.WithDeadline(context.Background(), attempt)
		conn, mux, err := c.dialAndRegister(ctx, e.allocation.Port, e.token, e.brokerAddr, e.allocation.Epoch, e.pins)
		cancel()
		if err == nil {
			if !selectedFlow(conn) {
				_ = conn.Close()
				_ = mux.Close()
				return errFlowState
			}
			if err = waitClientBindings(c, e, deadline); err == nil {
				return nil
			}
			// REGISTER can succeed before fresh bind authority or the carrier
			// fails. Keep original sockets through this transient loss, using
			// the first absolute recovery deadline and the same bounded backoff.
			// Close the failed transport before another attachment can own it.
			// origin: hot-update S5 plan §4.4 / T13 / DRILL-RUN 75.R
			_ = conn.Close()
			_ = mux.Close()
		}
		var deny *DenyError
		if errors.As(err, &deny) && !denyIsTransient(deny.Reason) {
			rejectClientEndpoint(c, e.allocation.Port)
			return err
		}
		timer := time.NewTimer(min(delay, max(time.Until(deadline), 0)))
		select {
		case <-e.stop:
			timer.Stop()
			return errFlowState
		case <-timer.C:
		}
		delay = min(2*delay, time.Second)
	}
	return os.ErrDeadlineExceeded
}
func waitClientBindings(c *Client, e *clientEndpoint, deadline time.Time) error {
	st := c.flowState
	st.mu.Lock()
	a := e.attachment
	st.mu.Unlock()
	if a == nil {
		return errFlowState
	}
	timer := time.NewTimer(max(time.Until(deadline), 0))
	defer timer.Stop()
	for {
		select {
		case <-a.Stop:
			return errFlowState
		default:
		}
		states := flow.States(st.manager, e.id)
		a.Mu.Lock()
		ready := true
		for _, r := range states {
			if r.Phase != flow.FlowPhaseTerminal && a.Streams[r.ID] == nil {
				ready = false
				break
			}
		}
		changed := a.BoundChanged
		a.Mu.Unlock()
		if ready {
			return nil
		}
		select {
		case <-a.Stop:
			return errFlowState
		case <-changed:
		case <-timer.C:
			return os.ErrDeadlineExceeded
		}
	}
}
