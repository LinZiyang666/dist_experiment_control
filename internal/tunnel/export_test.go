package tunnel

import "time"

// Test-only seams for the reconnect tests (white-box: same package).

// DropTransport simulates a residential-link blip: it closes the raw conn +
// yamux session of the live server session for `port` WITHOUT advancing any
// kill generation, so a subsequent reconnect REGISTER is still authorized
// (the port row stays ALLOCATED). The broker's own CloseChan watcher then
// tears down the public listener exactly as on a real drop. Returns false if
// no live session owns that port.
func (s *Server) DropTransport(port int) bool {
	s.mu.Lock()
	sess, ok := s.sessions[port]
	s.mu.Unlock()
	if !ok {
		return false
	}
	_ = sess.rawConn.Close()
	_ = sess.yamuxSess.Close()
	return true
}

// SessionUp reports whether the client still owns a supervised session slot
// for `port`. It stays true across a transient reconnect (the slot is swapped
// in place, not removed) and becomes false only after a terminal-DENY drop or
// Close — so it observes loop TERMINATION, not moment-to-moment data-plane
// liveness (tests poll the public port for the latter).
func (c *Client) SessionUp(port int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.sessions[port]
	return ok
}

// SetBackoffForTest shrinks the redial backoff so reconnect tests run fast.
func (c *Client) SetBackoffForTest(base, max time.Duration) {
	c.mu.Lock()
	c.backoffBase = base
	c.backoffMax = max
	c.mu.Unlock()
}
