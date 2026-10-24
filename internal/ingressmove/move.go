package ingressmove

import (
	"bytes"
	"crypto/ed25519"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// Install accepts only signed Prepare intent. A reply identifies this NC's
// current attachment; a boolean capability or an ACK forwarded on controlNC
// cannot cover its independently dialed dataNC.
// origin: hot-update S9 plan §4.5 / §4.6 / T34 / M17
func (r *Registry) Install(cp c.Checkpoint, proof c.RouteProof, deadline c.Deadline) error {
	plan, rows, err := r.begin(cp, deadline)
	if err != nil {
		return err
	}
	defer r.end()
	if !validPrepare(plan, proof) {
		return ErrIdentity
	}
	var state *participant
	for _, m := range rows {
		if m.id == proof.Participant {
			p := m.state
			state = &p
			break
		}
	}
	if state == nil {
		return c.ErrIncomplete
	}
	r.mu.Lock()
	direction, boundRevision := r.active.direction, r.active.boundRevision
	r.mu.Unlock()
	changing := direction != 0 && direction != proof.Direction
	if changing {
		if direction != c.RouteOutbound || proof.Direction != c.RouteReturn || !allBound(rows) || boundRevision != cp.Assets.Revision {
			return ErrNotReady
		}
	} else if direction == 0 && proof.Direction != c.RouteOutbound {
		return ErrNotReady
	}
	if !changing && state.prepare.Participant != (c.ID{}) {
		if !sameProof(state.prepare, proof) {
			return ErrIdentity
		}
		// A lost Installed reply is queried without renewing the old hold.
		if err = check(state.expires, nil); err != nil {
			return err
		}
	} else if state.current != proof.Old {
		return ErrIdentity
	}
	expires := state.expires
	if changing || state.prepare.Participant == (c.ID{}) {
		expires, err = controlbudget.After(time.Duration(proof.RemainingNS))
		if err != nil {
			return err
		}
	}
	if deadline > expires {
		deadline = expires
	}
	if err = r.verify(plan, cloneProof(proof), deadline); err != nil {
		return err
	}
	observed, err := state.connection.Observe(deadline)
	if err != nil {
		return err
	}
	if observed != state.current {
		return ErrIdentity
	}
	// Record the original deadline before sending an intent: a lost reply must
	// never allow a retry to issue the same nonce with a fresh eight seconds.
	r.mu.Lock()
	if !r.matchesLocked(cp) {
		r.mu.Unlock()
		return c.ErrIncomplete
	}
	if changing {
		for _, p := range r.participants {
			p.prepare = c.RouteProof{}
			p.expires = 0
			p.attached = false
			p.bound = false
		}
		r.active.targets = nil
		r.active.boundRevision = 0
		r.active.moveDeadline = 0
	}
	p := r.participants[proof.Participant]
	if p.prepare.Participant == (c.ID{}) {
		p.prepare, p.expires = cloneProof(proof), expires
	}
	r.active.direction = proof.Direction
	r.mu.Unlock()
	ack, err := state.connection.Install(cloneProof(proof), deadline)
	if err != nil {
		return err
	}
	if ack != observed {
		return ErrIdentity
	}
	if err = r.witness.Verify(ack, c.RouteTarget{}, deadline); err != nil {
		return err
	}
	confirmed, err := state.connection.Observe(deadline)
	if err != nil {
		return err
	}
	if confirmed != ack {
		return ErrIdentity
	}
	if err = check(deadline, nil); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.matchesLocked(cp) {
		return c.ErrIncomplete
	}
	p = r.participants[proof.Participant]
	r.active.direction = proof.Direction
	if !containsTarget(r.active.targets, proof.Target) {
		r.active.targets = append(r.active.targets, proof.Target)
	}
	record := &p.outbound
	if proof.Direction == c.RouteReturn {
		record = &p.returned
	}
	record.installed = proof.Old
	return nil
}

func (r *Registry) Move(cp c.Checkpoint, deadline c.Deadline, stop c.Stop) (c.MoveResult, error) {
	return r.attach(cp, c.RouteOutbound, deadline, stop)
}
func (r *Registry) Return(cp c.Checkpoint, deadline c.Deadline, stop c.Stop) (c.MoveResult, error) {
	return r.attach(cp, c.RouteReturn, deadline, stop)
}

// attach re-observes EVERY NC on both legs. A retry queries an already changed
// socket instead of minting another generation; cancellation affects transport
// work only and never closes business owners. No registry lock is held in P14.
// origin: hot-update S9 plan §4.5 / §4.6 / T35 / M17
func (r *Registry) attach(cp c.Checkpoint, direction c.RouteDirection, deadline c.Deadline, stop c.Stop) (c.MoveResult, error) {
	if err := check(deadline, stop); err != nil {
		return c.MoveResult{}, err
	}
	// Clamp the entire cohort, not each participant, to the existing 5s SSOT.
	limit, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		return c.MoveResult{}, err
	}
	if deadline > limit {
		deadline = limit
	}
	plan, rows, err := r.begin(cp, deadline)
	if err != nil {
		return c.MoveResult{}, err
	}
	defer r.end()
	r.mu.Lock()
	actualDirection := r.active.direction
	r.mu.Unlock()
	if actualDirection != direction {
		return c.MoveResult{}, ErrNotReady
	}
	// Check all Installed ACKs before the first attachment has an effect.
	for _, m := range rows {
		record := m.state.outbound
		if direction == c.RouteReturn {
			record = m.state.returned
		}
		if m.state.prepare.Participant != m.id || m.state.prepare.Direction != direction || record.installed != m.state.prepare.Old {
			return c.MoveResult{}, ErrNotReady
		}
		if err = check(m.state.expires, stop); err != nil {
			return c.MoveResult{}, err
		}
	}
	// The first effect fixes this leg's deadline. Repeated calls that query a
	// lost reply cannot renew its five seconds, including after a late reseal.
	r.mu.Lock()
	if r.active.moveDeadline == 0 {
		r.active.moveDeadline = deadline
	}
	if deadline > r.active.moveDeadline {
		deadline = r.active.moveDeadline
	}
	r.mu.Unlock()
	attachments := make([]c.Attachment, 0, len(rows))
	for _, m := range rows {
		if err = check(deadline, stop); err != nil {
			return c.MoveResult{}, err
		}
		a, e := r.attachOne(m, deadline, stop)
		if e != nil {
			return c.MoveResult{}, e
		}
		attachments = append(attachments, a)
	}
	census, err := r.census(plan, attachments, deadline)
	if err != nil {
		return c.MoveResult{}, err
	}
	if err = check(deadline, stop); err != nil {
		return c.MoveResult{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.matchesLocked(cp) {
		return c.MoveResult{}, c.ErrIncomplete
	}
	return c.MoveResult{Census: census, Attachments: attachments}, nil
}

func (r *Registry) attachOne(m member, deadline c.Deadline, stop c.Stop) (c.Attachment, error) {
	p := m.state
	observed, err := p.connection.Observe(deadline)
	if err != nil {
		return c.Attachment{}, err
	}
	if p.attached {
		if observed != p.current {
			return c.Attachment{}, ErrIdentity
		}
	} else if observed == p.prepare.Old {
		if err = check(p.expires, stop); err != nil {
			return c.Attachment{}, err
		}
		ack, attachErr := p.connection.Attach(p.prepare.Target, deadline, stop)
		observed, err = p.connection.Observe(deadline)
		if err != nil {
			return c.Attachment{}, err
		}
		if attachErr == nil && ack != observed {
			return c.Attachment{}, ErrIdentity
		}
		// An effect with a lost reply is retained once independently observed.
		// The caller still gets the original error and must query on retry.
		err = attachErr
	}
	if !changed(p.prepare.Old, observed) || !onTarget(observed, p.prepare.Target) {
		return c.Attachment{}, ErrIdentity
	}
	if e := r.witness.Verify(observed, p.prepare.Target, deadline); e != nil {
		return c.Attachment{}, e
	}
	if e := check(deadline, stop); e != nil {
		return c.Attachment{}, e
	}
	r.mu.Lock()
	current := r.participants[m.id]
	if current == nil || !sameProof(current.prepare, p.prepare) {
		r.mu.Unlock()
		return c.Attachment{}, c.ErrIncomplete
	}
	current.current, current.attached = observed, true
	record := &current.outbound
	if p.prepare.Direction == c.RouteReturn {
		record = &current.returned
	}
	record.attached = observed
	r.mu.Unlock()
	return observed, err
}

// Bind verifies a full old→new proof, actual same-NC ACK and fresh physical
// census. Duplicate replies are queried on the same generation; old outbound
// ACKs never authorize return. Partial ACKs are retained, but never yield a
// successful cohort receipt. The caller must inspect OldOwnerPending before
// retiring a process: ordinary HTTP responsibilities may still be live.
// origin: hot-update S9 plan §4.6 / T35 / M17
func (r *Registry) Bind(cp c.Checkpoint, proofs []c.RouteProof, deadline c.Deadline) (c.Census, error) {
	plan, rows, err := r.begin(cp, deadline)
	if err != nil {
		return c.Census{}, err
	}
	defer r.end()
	if len(proofs) != len(rows) {
		return c.Census{}, c.ErrIncomplete
	}
	byID := make(map[c.ID]c.RouteProof, len(proofs))
	for _, proof := range proofs {
		byID[proof.Participant] = cloneProof(proof)
	}
	// Validate the complete proof set before delivering any Bind ACK request.
	for _, m := range rows {
		proof, exists := byID[m.id]
		if !exists || !m.state.attached || !validBind(m.state, proof) {
			return c.Census{}, ErrIdentity
		}
		if err = check(m.state.expires, nil); err != nil {
			return c.Census{}, err
		}
		if err = r.verify(plan, cloneProof(proof), deadline); err != nil {
			return c.Census{}, err
		}
	}
	attachments := make([]c.Attachment, 0, len(rows))
	for _, m := range rows {
		if err = check(deadline, nil); err != nil {
			return c.Census{}, err
		}
		proof := byID[m.id]
		observed, e := m.state.connection.Observe(deadline)
		if e != nil {
			return c.Census{}, e
		}
		if observed != proof.New {
			return c.Census{}, ErrIdentity
		}
		if e = r.witness.Verify(observed, proof.Target, deadline); e != nil {
			return c.Census{}, e
		}
		ack, e := m.state.connection.Bind(cloneProof(proof), deadline)
		if e != nil {
			return c.Census{}, e
		}
		if ack != observed {
			return c.Census{}, ErrIdentity
		}
		confirmed, e := m.state.connection.Observe(deadline)
		if e != nil {
			return c.Census{}, e
		}
		if confirmed != ack {
			return c.Census{}, ErrIdentity
		}
		if e = check(m.state.expires, nil); e != nil {
			return c.Census{}, e
		}
		r.mu.Lock()
		p := r.participants[m.id]
		p.bound = true
		record := &p.outbound
		if proof.Direction == c.RouteReturn {
			record = &p.returned
		}
		record.bound = ack
		r.mu.Unlock()
		attachments = append(attachments, ack)
	}
	census, err := r.census(plan, attachments, deadline)
	if err != nil {
		return c.Census{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.matchesLocked(cp) {
		return c.Census{}, c.ErrIncomplete
	}
	r.active.boundRevision = r.revision
	return census, nil
}

// Release can abort an untouched before-effect seal. After any physical move it
// needs every return ACK and a fresh zero-old-owner census. Tasks may remain live.
// A late admission invalidates the checkpoint even after an earlier zero census.
// origin: hot-update S9 plan §2.3 / §4.5 / T34 / T35 / M17
func (r *Registry) Release(cp c.Checkpoint, deadline c.Deadline) error {
	plan, rows, err := r.begin(cp, deadline)
	if err != nil {
		return err
	}
	defer r.end()
	moved := false
	for _, m := range rows {
		moved = moved || m.state.outbound.attached != (c.Attachment{}) || m.state.current != m.state.outbound.installed && m.state.outbound.installed != (c.Attachment{})
	}
	r.mu.Lock()
	direction, boundRevision := r.active.direction, r.active.boundRevision
	r.mu.Unlock()
	if moved && (direction != c.RouteReturn || !allBound(rows) || boundRevision != cp.Assets.Revision) {
		return ErrNotReady
	}
	attachments, err := r.observe(rows, deadline)
	if err != nil {
		return err
	}
	census, err := r.census(plan, attachments, deadline)
	if err != nil {
		return err
	}
	if moved && census.OldOwnerPending != 0 {
		return ErrNotReady
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.matchesLocked(cp) {
		return c.ErrIncomplete
	}
	// No NC or business object is closed here. Release this step's intent and
	// retain both receipt ledgers until the next Seal starts another step.
	for _, p := range r.participants {
		p.prepare = c.RouteProof{}
		p.expires = 0
		p.attached = false
		p.bound = false
	}
	r.active = nil
	r.revision++
	return nil
}

func allBound(rows []member) bool {
	if len(rows) == 0 {
		return false
	}
	for _, m := range rows {
		if !m.state.attached || !m.state.bound {
			return false
		}
	}
	return true
}

func validPrepare(plan c.Plan, p c.RouteProof) bool {
	return p.Purpose == c.RoutePrepare && p.Version == 1 && validDirection(p.Direction) && p.Tx == plan.Tx && p.PolicyRevision == plan.PolicyRevision && p.Participant == p.Old.Participant && validAttachment(p.Old) && p.New == (c.Attachment{}) && validTarget(p.Target) && p.Target.Component == plan.Component && p.HomeDigest != (c.Digest{}) && p.AuthorityRevision > 0 && p.Nonce != (c.ID{}) && p.KeyID != "" && len(p.Signature) == ed25519.SignatureSize && p.RemainingNS > 0 && p.RemainingNS <= uint64(upgradebudget.BrokerMaintenanceTTL)
}

func validBind(state participant, p c.RouteProof) bool {
	a := state.prepare
	return p.Purpose == c.RouteBind && p.Version == a.Version && p.Direction == a.Direction && p.Tx == a.Tx && p.Participant == a.Participant && p.Old == a.Old && p.New == state.current && changed(p.Old, p.New) && p.Target == a.Target && p.HomeDigest == a.HomeDigest && p.AuthorityRevision == a.AuthorityRevision && p.PolicyRevision == a.PolicyRevision && p.Nonce == a.Nonce && p.KeyID == a.KeyID && len(p.Signature) == ed25519.SignatureSize && p.RemainingNS > 0 && p.RemainingNS <= a.RemainingNS && onTarget(p.New, p.Target)
}

func changed(old, next c.Attachment) bool {
	// Comparing all fields is insufficient: SetServerPool can change URL with
	// the exact same live socket. CID must change when serverID stays equal.
	// Socket reuse elsewhere cannot inherit this NC's local generation either.
	return validAttachment(next) && next.Participant == old.Participant && next.Generation > old.Generation && (next.ServerID != old.ServerID || next.CID != old.CID) && (next.Raw.NetNS != old.Raw.NetNS || next.Raw.Cookie != old.Raw.Cookie) && next.Raw != old.Raw
}

func cloneProof(p c.RouteProof) c.RouteProof { p.Signature = bytes.Clone(p.Signature); return p }
func sameProof(a, b c.RouteProof) bool {
	return a.Purpose == b.Purpose && a.Direction == b.Direction && a.Target == b.Target && a.Version == b.Version && a.Tx == b.Tx && a.Participant == b.Participant && a.Old == b.Old && a.New == b.New && a.HomeDigest == b.HomeDigest && a.AuthorityRevision == b.AuthorityRevision && a.PolicyRevision == b.PolicyRevision && a.RemainingNS == b.RemainingNS && a.Nonce == b.Nonce && a.KeyID == b.KeyID && bytes.Equal(a.Signature, b.Signature)
}
