package infraharness

import (
	"errors"
	"path"
	"strings"
	"sync"
)

// Scope supplies the exact unit/cgroup to the independent host drill teardown.
// Detached host units are not children of a Go reaper. Their host owner must
// stop them; registration here must never imply that process exit stopped them.
type Scope struct {
	Unit   string
	Cgroup string
}

// Deathwatch records incarnations for an independent post-death census. Linux
// tree termination remains exclusively in the shared testharness supervisor.
type Deathwatch struct {
	mu     sync.Mutex
	scopes map[Identity]Scope
}

// Register rejects conflicting scope reuse instead of overwriting custody.
// Empty Scope denotes a supervised child without a detached host unit.
// origin: hot-update S9 plan §9.5 / T57 / lead L22
func (d *Deathwatch) Register(id Identity, scope Scope) error {
	if id.PID <= 0 || id.StartTicks == 0 || id.BootID == "" {
		return errors.New("infra harness: missing deathwatch incarnation")
	}
	if scope != (Scope{}) && (scope.Unit == "" || !strings.HasSuffix(scope.Unit, ".service") ||
		strings.ContainsAny(scope.Unit, "/\x00\n\r \t") || !path.IsAbs(scope.Cgroup) ||
		path.Clean(scope.Cgroup) != scope.Cgroup || scope.Cgroup == "/") {
		return errors.New("infra harness: incomplete unit scope")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if old, ok := d.scopes[id]; ok && old != scope {
		return errors.New("infra harness: deathwatch scope conflict")
	}
	if d.scopes == nil {
		d.scopes = make(map[Identity]Scope)
	}
	d.scopes[id] = scope
	return nil
}

// Snapshot returns independent storage for the host teardown's unit ledger.
func (d *Deathwatch) Snapshot() map[Identity]Scope {
	d.mu.Lock()
	defer d.mu.Unlock()
	copy := make(map[Identity]Scope, len(d.scopes))
	for id, scope := range d.scopes {
		copy[id] = scope
	}
	return copy
}

// CheckReaped requires every recorded incarnation to be absent, including
// zombies. Unit removal needs an additional independent host-side unit census.
func (d *Deathwatch) CheckReaped() error {
	rows := d.Snapshot()
	if len(rows) == 0 {
		return errors.New("infra harness: empty deathwatch census")
	}
	for id := range rows {
		exists, err := ProcessExists(id)
		if err != nil {
			return err
		}
		if exists {
			return errors.New("infra harness: recorded descendant remains unreaped")
		}
	}
	return nil
}
