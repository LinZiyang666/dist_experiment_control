package workload

import (
	"os"
	"time"
)

// SnapshotUntil performs the read-only ownership precheck without extending
// the cut's absolute deadline. It does not freeze the table: legacy adoption
// must remain legal until every row has been proved to belong to the runtime.
// origin: hot-update S4 internal review rv-fd rv-fd-3
func SnapshotUntil(m *Manager, deadline time.Time) ([]Record, error) {
	if err := lockControlUntil(m, deadline); err != nil {
		return nil, err
	}
	defer m.mu.Unlock()
	return m.snapshotLocked(), nil
}

// FrozenUntil includes control-guard admission in an activation deadline.
func FrozenUntil(m *Manager, deadline time.Time) (bool, error) {
	if err := lockControlUntil(m, deadline); err != nil {
		return false, err
	}
	defer m.mu.Unlock()
	return m.frozen, nil
}

func lockControlUntil(m *Manager, deadline time.Time) error {
	for time.Now().Before(deadline) {
		if m.mu.TryLock() {
			if time.Now().Before(deadline) {
				return nil
			}
			m.mu.Unlock()
			break
		}
		time.Sleep(min(time.Millisecond, time.Until(deadline)))
	}
	return os.ErrDeadlineExceeded
}
