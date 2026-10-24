package ingressmove

import (
	"net/url"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Admission is obtained BEFORE every control, data or consumer dial. During a
// seal it permits only the installed successor; until that target exists callers
// wait/refuse, rather than sneaking a new socket onto the retiring ingress.
// Keep this token private to the dial owner. Register revalidates its exact
// revision after Observe/physical verification, so an old in-flight dial cannot
// resurrect an old generation. Failed registration leaves close/join with P14.
type Admission struct {
	Participant c.ID
	Role        string
	Revision    uint64
	Cohort      c.ID
	Target      c.RouteTarget
}

// origin: hot-update S9 plan §4.5 / T34 / M17
func (r *Registry) Admit(id c.ID, role string, deadline c.Deadline) (Admission, error) {
	return r.admit(id, role, c.RouteTarget{}, false, deadline)
}

// AdmitTo selects one already Installed target for a late dial with a known
// transport profile. A mixed NATS/WSS cohort cannot guess a shared URL from its
// first controlNC. Each late participant still needs its own signed Prepare ACK.
// origin: hot-update S9 plan §4.5 / T34 / M17
func (r *Registry) AdmitTo(id c.ID, role string, target c.RouteTarget, deadline c.Deadline) (Admission, error) {
	return r.admit(id, role, target, true, deadline)
}

func (r *Registry) admit(id c.ID, role string, target c.RouteTarget, explicit bool, deadline c.Deadline) (Admission, error) {
	if err := check(deadline, nil); err != nil {
		return Admission{}, err
	}
	if id == (c.ID{}) || !validRole(role) {
		return Admission{}, ErrIdentity
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.participants[id]; exists {
		return Admission{}, ErrBusy
	}
	a := Admission{Participant: id, Role: role, Revision: r.revision}
	if r.active != nil {
		if !explicit {
			if len(r.active.targets) != 1 {
				return Admission{}, ErrNotReady
			}
			target = r.active.targets[0]
		}
		if !containsTarget(r.active.targets, target) || !validProcess(target.Ingress) {
			return Admission{}, ErrNotReady
		}
		a.Cohort, a.Target = r.active.checkpoint.Cohort, target
	} else if explicit && target != (c.RouteTarget{}) {
		return Admission{}, ErrNotReady
	}
	return a, nil
}

// Register is mandatory for independent dataNCs too. It does not infer an
// Installed ACK from capability flags, or inherit another NC's route receipt.
// origin: hot-update S9 plan §4.5 / T34 / M17
func (r *Registry) Register(a Admission, connection Connection, deadline c.Deadline) error {
	if err := check(deadline, nil); err != nil {
		return err
	}
	if connection == nil || a.Participant == (c.ID{}) || !validRole(a.Role) {
		return ErrIdentity
	}
	observed, err := connection.Observe(deadline)
	if err != nil {
		return err
	}
	if !validAttachment(observed) || observed.Participant != a.Participant || !onTarget(observed, a.Target) {
		return ErrIdentity
	}
	if err = r.witness.Verify(observed, a.Target, deadline); err != nil {
		return err
	}
	if err = check(deadline, nil); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if a.Revision != r.revision {
		return c.ErrIncomplete
	}
	if _, exists := r.participants[a.Participant]; exists {
		return ErrBusy
	}
	if r.active == nil {
		if a.Cohort != (c.ID{}) || a.Target != (c.RouteTarget{}) {
			return c.ErrIncomplete
		}
	} else if a.Cohort != r.active.checkpoint.Cohort || !containsTarget(r.active.targets, a.Target) || !validProcess(a.Target.Ingress) {
		return c.ErrIncomplete
	}
	r.participants[a.Participant] = &participant{role: a.Role, connection: connection, current: observed}
	// An all-zero old census has no authority after this increment. Every
	// action consumer must refresh Seal and recheck its exact checkpoint.
	r.revision++
	return nil
}

func containsTarget(targets []c.RouteTarget, target c.RouteTarget) bool {
	for _, allowed := range targets {
		if allowed == target {
			return true
		}
	}
	return false
}

// Remove is for ordinary owner teardown AFTER transport close/join. During a
// sealed step even a finished transfer retains its migration responsibility;
// removing it would let task completion stand in for an attachment ACK.
func (r *Registry) Remove(id c.ID, generation uint64, deadline c.Deadline) error {
	if err := check(deadline, nil); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active != nil || r.busy {
		return ErrBusy
	}
	p, exists := r.participants[id]
	if !exists || generation == 0 || generation != p.current.Generation {
		return ErrIdentity
	}
	delete(r.participants, id)
	r.revision++
	return nil
}

func validRole(role string) bool {
	return role == "control" || role == "data" || role == "consumer" || role == "auth"
}

func validTarget(target c.RouteTarget) bool {
	u, err := url.Parse(target.URL)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || target.NodeID == "" || (target.Component != c.NATS && target.Component != c.Caddy) {
		return false
	}
	if u.Scheme != "nats" && u.Scheme != "tls" && u.Scheme != "ws" && u.Scheme != "wss" {
		return false
	}
	if validProcess(target.Ingress) {
		return true
	}
	// N1 can prepare a verified successor before it is running. That intent
	// cannot admit a late dial or serve as physical Bind evidence yet.
	return target.Component == c.NATS && target.Ingress == (c.Process{}) && validProcess(target.Before) && target.Candidate.Kind == c.NATSBinary && target.Candidate.BinarySHA256 != (c.Digest{}) && target.Candidate.CatalogSHA256 != (c.Digest{})
}

func onTarget(a c.Attachment, target c.RouteTarget) bool {
	if target == (c.RouteTarget{}) {
		return true
	}
	if a.URL != target.URL {
		return false
	}
	if validProcess(target.Ingress) {
		return a.Ingress == target.Ingress
	}
	return validTarget(target) && a.Ingress != target.Before && a.Ingress.ExeSHA256 == target.Candidate.BinarySHA256
}
