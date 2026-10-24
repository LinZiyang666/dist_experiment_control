// Package ingressmove owns the complete control/data NC admission and migration
// barrier. Transport, physical census and signed route authority remain separate
// owners; none can replace another owner's evidence with a status projection.
// origin: hot-update S9 plan §4.5 / §4.6 / T34 / T35
package ingressmove

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sort"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
)

var (
	ErrBusy     = errors.New("infra_busy")
	ErrNotReady = errors.New("infra_not_ready")
	ErrIdentity = c.ErrIncomplete
)

// Connection is one NC, including an independent transfer/consumer NC. All ACKs
// must be read on this connection. Install/Bind query an existing same-generation
// receipt on retry; Attach owns raw cancellation and worker joins (P14). Observe
// includes an actual CONNECT cid and PONG/request, never a server-pool label.
// Calls are synchronous and must honor their original absolute deadline. The
// registry deliberately does not hide an unbounded callback in a goroutine.
type Connection interface {
	Observe(c.Deadline) (c.Attachment, error)
	Install(c.RouteProof, c.Deadline) (c.Attachment, error)
	Attach(c.RouteTarget, c.Deadline, c.Stop) (c.Attachment, error)
	Bind(c.RouteProof, c.Deadline) (c.Attachment, error)
}

// Witness is independent of the NC's ACK producer. Verify joins the raw client
// socket to the accepted ingress socket and its exact process incarnation (P02).
// Census must enumerate the complete admitted NC set and all old ingress
// responsibilities, including pending accepts/keepalive/conntrack. Unknown NCs,
// missing pages or inaccessible kernel state must return incomplete. Attachments
// are comparison input, never permission to enumerate only those sockets.
type Witness interface {
	Verify(c.Attachment, c.RouteTarget, c.Deadline) error
	Census(c.Plan, []c.Attachment, c.Deadline) (c.Census, error)
}

// VerifyProof binds the signature and current authority/home/normal REGISTER
// baseline through P16/P23. A nil verifier is not an unsigned operating mode.
type VerifyProof func(c.Plan, c.RouteProof, c.Deadline) error

type participant struct {
	role       string
	connection Connection
	current    c.Attachment
	prepare    c.RouteProof
	expires    c.Deadline
	attached   bool
	bound      bool
	outbound   ackRecord
	returned   ackRecord
}

// Records are memory-only. S9's exclusive upgrade domain keeps live registry
// objects out of S5/S6 image codecs; durable route/root receipts live with their
// respective owners. An Installed attachment is the actual Old, not a future cid.
type ackRecord struct{ installed, attached, bound c.Attachment }
type sealState struct {
	plan          c.Plan
	checkpoint    c.Checkpoint
	direction     c.RouteDirection
	targets       []c.RouteTarget
	boundRevision uint64
	moveDeadline  c.Deadline
}

type Registry struct {
	mu           sync.Mutex
	busy         bool
	revision     uint64
	participants map[c.ID]*participant
	active       *sealState
	witness      Witness
	verify       VerifyProof
}

var _ c.Cohort = (*Registry)(nil)

func New(witness Witness, verify VerifyProof) (*Registry, error) {
	if witness == nil || verify == nil {
		return nil, c.ErrContractStub
	}
	return &Registry{revision: 1, participants: make(map[c.ID]*participant), witness: witness, verify: verify}, nil
}

// Ledger returns independent outbound/return receipts. Its revision must match
// the checkpoint rechecked at the action barrier; it is not a retirement census.
type Ledger struct {
	Revision  uint64
	Direction c.RouteDirection
	Installed []c.Attachment
	Attached  []c.Attachment
	Bound     []c.Attachment
}

func (r *Registry) Ledger(direction c.RouteDirection) (Ledger, error) {
	if !validDirection(direction) {
		return Ledger{}, ErrIdentity
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	l := Ledger{Revision: r.revision, Direction: direction}
	for _, id := range r.idsLocked() {
		p := r.participants[id]
		a := p.outbound
		if direction == c.RouteReturn {
			a = p.returned
		}
		if a.installed != (c.Attachment{}) {
			l.Installed = append(l.Installed, a.installed)
		}
		if a.attached != (c.Attachment{}) {
			l.Attached = append(l.Attached, a.attached)
		}
		if a.bound != (c.Attachment{}) {
			l.Bound = append(l.Bound, a.bound)
		}
	}
	return l, nil
}

// Seal is also the only way to refresh a checkpoint after admission changed.
// Neither identical empty census reads nor a previous hop's ACK seals a new set.
func (r *Registry) Seal(plan c.Plan, deadline c.Deadline) (c.Checkpoint, error) {
	if err := check(deadline, nil); err != nil {
		return c.Checkpoint{}, err
	}
	if plan.Tx == (c.ID{}) || plan.Domain == (c.ID{}) || plan.Step == 0 || plan.PolicyRevision == 0 || !validProcess(plan.Before) || (plan.Component != c.NATS && plan.Component != c.Caddy) {
		return c.Checkpoint{}, ErrIdentity
	}
	r.mu.Lock()
	if r.busy || (r.active != nil && r.active.plan != plan) {
		r.mu.Unlock()
		return c.Checkpoint{}, ErrBusy
	}
	r.busy = true
	revision := r.revision
	rows := r.rowsLocked()
	r.mu.Unlock()
	defer r.end()
	if len(rows) == 0 {
		return c.Checkpoint{}, c.ErrIncomplete
	}
	attachments, err := r.observe(rows, deadline)
	if err != nil {
		return c.Checkpoint{}, err
	}
	if _, err = r.census(plan, attachments, deadline); err != nil {
		return c.Checkpoint{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.revision != revision {
		return c.Checkpoint{}, c.ErrIncomplete
	}
	if r.active == nil {
		var id c.ID
		if _, err = rand.Read(id[:]); err != nil {
			return c.Checkpoint{}, err
		}
		r.active = &sealState{plan: plan, checkpoint: c.Checkpoint{Cohort: id}}
		for _, p := range r.participants {
			p.outbound = ackRecord{}
			p.returned = ackRecord{}
		}
	}
	// Only the participant sub-seal belongs here. Complete for JS assets and
	// Transfers stays false until P18/P19 provide their own real inventory.
	cp := c.Checkpoint{Cohort: r.active.checkpoint.Cohort}
	cp.Assets.Revision = revision
	cp.Assets.Participants = participantDigest(rows)
	cp.Proof = checkpointDigest(checkpointBinding{plan, cp.Cohort, revision, cp.Assets.Participants})
	r.active.checkpoint = cp
	return cp, nil
}

type member struct {
	id    c.ID
	state participant
}

func (r *Registry) idsLocked() []c.ID {
	ids := make([]c.ID, 0, len(r.participants))
	for id := range r.participants {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return bytes.Compare(ids[i][:], ids[j][:]) < 0 })
	return ids
}

func (r *Registry) rowsLocked() []member {
	rows := make([]member, 0, len(r.participants))
	for _, id := range r.idsLocked() {
		p := *r.participants[id]
		p.prepare.Signature = bytes.Clone(p.prepare.Signature)
		rows = append(rows, member{id: id, state: p})
	}
	return rows
}

// Hash the exact identity/role set in canonical ID order, not connection object
// addresses or map iteration order. No credentials or private tuples leave here.
type participantBinding struct {
	ID         c.ID
	Role       string
	Attachment c.Attachment
}
type checkpointBinding struct {
	Plan         c.Plan
	Cohort       c.ID
	Revision     uint64
	Participants c.Digest
}

func participantDigest(rows []member) c.Digest {
	set := make([]participantBinding, 0, len(rows))
	for _, m := range rows {
		set = append(set, participantBinding{m.id, m.state.role, m.state.current})
	}
	// These closed structs contain only JSON primitive fields and slices;
	// encoding has no error-producing value (no interfaces, maps or floats).
	data, _ := json.Marshal(set)
	return sha256.Sum256(data)
}

func checkpointDigest(binding checkpointBinding) c.Digest {
	// The frozen Plan has the same primitive-only encoding property.
	data, _ := json.Marshal(binding)
	return sha256.Sum256(data)
}

func (r *Registry) begin(cp c.Checkpoint, deadline c.Deadline) (c.Plan, []member, error) {
	if err := check(deadline, nil); err != nil {
		return c.Plan{}, nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.busy {
		return c.Plan{}, nil, ErrBusy
	}
	if !r.matchesLocked(cp) {
		return c.Plan{}, nil, c.ErrIncomplete
	}
	r.busy = true
	return r.active.plan, r.rowsLocked(), nil
}

// Snapshot supplies P16's Prepare producer with each actually registered NC,
// including dataNCs. Existing intent nonces stay with the route owner; this
// current inventory cannot fabricate a future attachment for Prepare.
func (r *Registry) Snapshot(cp c.Checkpoint, deadline c.Deadline) ([]c.Attachment, error) {
	_, rows, err := r.begin(cp, deadline)
	if err != nil {
		return nil, err
	}
	defer r.end()
	attachments, err := r.observe(rows, deadline)
	if err != nil {
		return nil, err
	}
	if err = check(deadline, nil); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.matchesLocked(cp) {
		return nil, c.ErrIncomplete
	}
	return attachments, nil
}

func (r *Registry) matchesLocked(cp c.Checkpoint) bool {
	return r.active != nil && cp == r.active.checkpoint && cp.Assets.Revision == r.revision
}

func (r *Registry) end() { r.mu.Lock(); r.busy = false; r.mu.Unlock() }

func (r *Registry) observe(rows []member, deadline c.Deadline) ([]c.Attachment, error) {
	attachments := make([]c.Attachment, 0, len(rows))
	for _, m := range rows {
		if err := check(deadline, nil); err != nil {
			return nil, err
		}
		a, err := m.state.connection.Observe(deadline)
		if err != nil {
			return nil, err
		}
		if !validAttachment(a) || a != m.state.current {
			return nil, ErrIdentity
		}
		if err = r.witness.Verify(a, c.RouteTarget{}, deadline); err != nil {
			return nil, err
		}
		attachments = append(attachments, a)
	}
	return attachments, nil
}

func (r *Registry) census(plan c.Plan, attachments []c.Attachment, deadline c.Deadline) (c.Census, error) {
	if err := check(deadline, nil); err != nil {
		return c.Census{}, err
	}
	result, err := r.witness.Census(plan, append([]c.Attachment(nil), attachments...), deadline)
	if err != nil {
		return c.Census{}, err
	}
	if err = check(deadline, nil); err != nil {
		return c.Census{}, err
	}
	if !result.Complete || result.Generation == 0 || result.Digest == (c.Digest{}) {
		return c.Census{}, c.ErrIncomplete
	}
	result.Processes = append([]c.Process(nil), result.Processes...)
	result.Sockets = append([]c.Socket(nil), result.Sockets...)
	return result, nil
}

func check(deadline c.Deadline, stop c.Stop) error {
	select {
	case <-stop:
		return errors.New("infra_budget_exceeded")
	default:
	}
	_, err := controlbudget.Remaining(deadline)
	return err
}

func validDirection(direction c.RouteDirection) bool {
	return direction == c.RouteOutbound || direction == c.RouteReturn
}
func validProcess(p c.Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks > 0 && p.InvocationID != "" && p.ExeDevice > 0 && p.ExeInode > 0 && p.ExeSHA256 != (c.Digest{}) && p.Cgroup != ""
}
func validAttachment(a c.Attachment) bool {
	return a.Participant != (c.ID{}) && a.Generation > 0 && a.ServerID != "" && a.CID > 0 && a.URL != "" && a.HealthGeneration > 0 && validProcess(a.Ingress) && validProcess(a.Raw.Owner) && a.Raw.NetNS > 0 && a.Raw.Device > 0 && a.Raw.Inode > 0 && a.Raw.Cookie > 0 && a.Raw.Local != "" && a.Raw.Remote != ""
}
