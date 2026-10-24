// Identity census includes terminal receipts as well as executing streams.
// origin: hot-update S8 plan §4.2 / S8 internal review TAGGED-SUITE-SWEEP-A F2
package flow

import "time"

// Census reads the journal without freezing IO or changing carrier ownership.
// States intentionally omits closed records for RESUME; an upgrade census must
// retain those records and the retirement journal that prevents a second Dial.
func Census(m *Manager, deadline time.Time) ([]State, []RetirementImage, error) {
	if m == nil || m.state == nil {
		return nil, nil, ErrClosed
	}
	st := m.state
	if !lockBefore(&st.mu, deadline) {
		return nil, nil, ErrFrozen
	}
	defer st.mu.Unlock()
	if st.closed {
		return nil, nil, ErrClosed
	}
	rows := make([]State, 0, len(st.records))
	for _, r := range st.records {
		if !lockBefore(&r.mu, deadline) {
			return nil, nil, ErrFrozen
		}
		rows = append(rows, stateLocked(r))
		r.mu.Unlock()
	}
	retained := make([]RetirementImage, 0, len(st.retirement))
	for id, r := range st.retirement {
		retained = append(retained, retirementImage(id, r))
	}
	return rows, retained, nil
}
