// Package inframaint owns the S9 route bridge, independently of S6 maintenance.
// It signs intent before dial and physical attachment evidence after REGISTER;
// it neither changes a home allocation nor pauses the normal failure reaper.
// origin: hot-update S9 plan §4.6 / T36
package inframaint

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"math"
	"sync"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

// SigningDomain separates this protocol from the S6 exact-route ticket, host
// IPC requests and ordinary authority messages. The same account key may sign
// those protocols; that does not make their signed bytes interchangeable.
// Purpose and direction remain inside the signed DTO.
const SigningDomain = "infra-route-v1\n"

// Errors classify refusal without echoing a proof, tuple or key-provider error.
// These are leaf sentinels, not additions to public infra wire Code vocabulary;
// adapters classify them before presenting status. No refusal certifies that
// a physical effect occurred or permits a fallback to an old reload command.
var (
	ErrInvalid    = errors.New("infra_route_invalid")
	ErrIdentity   = errors.New("infra_route_identity")
	ErrExpired    = errors.New("infra_route_expired")
	ErrTransition = errors.New("infra_route_transition")
)

// Baseline is the full home identity, not a hostname or a convenient epoch.
// The digest may be public; the original token and signing seed never belong
// here. Empty Home is valid for single mode, which has no cluster SQL home.
// Session and node identity prevent an epoch reused by a recreated allocation
// from borrowing an earlier maintenance window. The ordinary authority owner
// supplies TokenHash; this package does not read the original token, discover
// an authority or override any of its mutations.
type Baseline struct {
	SessionID  string
	NodeID     string
	InstanceID string
	TokenHash  infracontract.Digest
	Home       string
	Epoch      uint64
}

// HomeDigest commits to one ordered struct, avoiding ambiguous concatenation
// of session/token/home fields. Home CAS remains with the normal authority
// owner; this digest only binds its baseline to a route transition.
// origin: hot-update S9 plan §4.6 / T36
func HomeDigest(b Baseline) (infracontract.Digest, error) {
	if b.SessionID == "" || b.NodeID == "" || b.InstanceID == "" || b.TokenHash == (infracontract.Digest{}) || b.Epoch == 0 {
		return infracontract.Digest{}, ErrIdentity
	}
	data, err := json.Marshal(b)
	if err != nil {
		return infracontract.Digest{}, ErrInvalid
	}
	return sha256.Sum256(data), nil
}

// SigningBytes uses P00's strict complete record, with a separate purpose.
// RouteProof has no Domain field in the frozen shape, so the trusted domain is
// a fixed-width signed prefix. A consumer must supply its acquired domain,
// rather than choose one from an untrusted message or key identifier.
// The fixed sixteen-byte prefix is unambiguous without another JSON spelling.
// P00 owns RouteProof's exact field order and nested shapes; signing its codec
// output avoids a second selected-field implementation. Clearing Signature in
// a value copy leaves the caller's transport record and backing array intact.
// origin: hot-update S9 plan §2.2 / §4.6 / T36
func SigningBytes(p infracontract.RouteProof, domain infracontract.ID) ([]byte, error) {
	if domain == (infracontract.ID{}) || p.RemainingNS > uint64(upgradebudget.BrokerMaintenanceTTL) {
		return nil, ErrInvalid
	}
	p.Signature = nil
	data, err := infracontract.EncodeRouteProof(p)
	if err != nil || !matchesTarget(p) {
		return nil, ErrInvalid
	}
	out := append([]byte(SigningDomain), domain[:]...)
	return append(out, data...), nil
}

// Sign is a credential-free callback supplied by the identity owner. This
// leaf never loads a key and never puts private material in an error string.
type Sign func([]byte) ([]byte, error)

// SignProof returns independent signature storage. Some key providers reuse
// scratch buffers; retaining one could change an already-issued proof without
// a new operation. Provider errors become a static sentinel rather than carry
// credential material into a status or log path.
func SignProof(p infracontract.RouteProof, domain infracontract.ID, sign Sign) (infracontract.RouteProof, error) {
	data, err := SigningBytes(p, domain)
	if err != nil || sign == nil {
		return infracontract.RouteProof{}, ErrInvalid
	}
	sig, err := sign(data)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return infracontract.RouteProof{}, ErrInvalid
	}
	p.Signature = bytes.Clone(sig)
	return p, nil
}

func VerifyProof(p infracontract.RouteProof, domain infracontract.ID, keyID string, key ed25519.PublicKey) error {
	data, err := SigningBytes(p, domain)
	if err != nil {
		return err
	}
	// The key label is signed, but it is never authority to select a new key.
	// origin: hot-update S9 plan §4.6 / T36 / M18
	if keyID == "" || p.KeyID != keyID || len(key) != ed25519.PublicKeySize || !ed25519.Verify(key, data, p.Signature) {
		return ErrIdentity
	}
	return nil
}

func matchesTarget(p infracontract.RouteProof) bool {
	// A future successor's Before must describe this actual old ingress.
	// Otherwise a valid candidate for another invocation could borrow its
	// maintenance window before any physical move was even attempted.
	// origin: hot-update S9 plan §4.6 / T36
	if p.Target.Ingress == (infracontract.Process{}) && p.Target.Before != p.Old.Ingress {
		return false
	}
	if p.Purpose == infracontract.RoutePrepare {
		return p.New == (infracontract.Attachment{})
	}
	n := p.New
	// A pool/URL edit can leave the same kernel socket in place. Compare the
	// actual socket incarnation separately from labels and owner metadata.
	if n.URL != p.Target.URL || sameSocket(p.Old.Raw, n.Raw) || (n.ServerID == p.Old.ServerID && n.CID == p.Old.CID) {
		return false
	}
	if p.Target.Ingress != (infracontract.Process{}) {
		return n.Ingress == p.Target.Ingress
	}
	// The future N1 variant still needs observed runtime/auth evidence from
	// Source at Bind. Matching disk SHA alone never produces that evidence.
	return p.Target.Before == p.Old.Ingress && n.Ingress != p.Target.Before && n.Ingress.BootID == p.Target.Before.BootID && n.Ingress.ExeSHA256 == p.Target.Candidate.BinarySHA256
}

// These kernel identifiers describe one socket incarnation independently of
// mutable URL/tuple labels. Keeping them separate from the complete signed
// Socket comparison catches a pool edit which merely reuses the original FD.
// The source owner is responsible for observing these identifiers, not making
// them up from a connection generation or cached component Status.
func sameSocket(a, b infracontract.Socket) bool {
	return a.NetNS == b.NetNS && a.Device == b.Device && a.Inode == b.Inode && a.Cookie == b.Cookie
}

// State is a fresh production observation. Source must read the acquired
// domain, current home CAS, REGISTER/heartbeat and physical socket owner; it
// cannot derive these flags from an upgrade Status projection. Nonce belongs
// to the participant's outstanding challenge, and survives every retry.
// Raw.Owner identifies the connecting process; Ingress identifies the actual
// accepted server socket's process. Caddy may share a NATS ServerID and cert
// with another invocation, so these two owners are separate facts. Stable
// means normal REGISTER/heartbeat and home handling have resumed, rather than
// merely reaching a component phase named Verifying or Returning.
type State struct {
	Domain            infracontract.ID
	Attachment        infracontract.Attachment
	Target            infracontract.RouteTarget
	HomeDigest        infracontract.Digest
	AuthorityRevision uint64
	PolicyRevision    uint64
	Nonce             infracontract.ID
	Healthy           bool
	Registered        bool
	Stable            bool
}

// Source is the only producer of trusted route observations. Read must return
// the complete set sealed by Checkpoint, including independent transfer NCs.
// It rereads current normal authority on Bind and every Prepare retry; it does
// not mint an alternate home epoch to authorize the planned transport move.
// Its kernel/runtime/auth readers retain their own cancellation and ownership
// obligations; a successful Read is not itself a retirement receipt.
type Source interface {
	Read(infracontract.Plan, infracontract.Checkpoint, infracontract.RouteDirection, infracontract.Deadline) ([]State, error)
}

// Clock is local BOOTTIME. Injection allows boundary tests without changing
// production timers; nil uses the shared controlbudget clock.
type Clock func() (int64, error)

// Config is assembled by the nonroot production composition owner. Domain and
// KeyID come from acquired authority, not from the request being verified.
// Source must honor its deadline and join its own transport work; this owner
// does not time out a blocked reader by creating detached goroutines. All
// clock callbacks read one local clock, even when the plan spans hosts.
type Config struct {
	Domain infracontract.ID
	KeyID  string
	Sign   Sign
	Source Source
	Clock  Clock
}

type routeKey struct {
	Tx        infracontract.ID
	Step      uint32
	Direction infracontract.RouteDirection
}

// Expired entries persist until terminal settlement. Deleting one on a timer
// would turn a lost-reply retry into a fresh eight-second grant. Bound retains
// the accepted attachment, so another actual reconnection under the same nonce
// cannot masquerade as an idempotent reply query.
type preparedRoute struct {
	Plan       infracontract.Plan
	Checkpoint infracontract.Checkpoint
	ExpiresNS  int64
	Proofs     []infracontract.RouteProof
	Bound      []infracontract.RouteProof
}

// RouteSigner serializes the intent/evidence protocol, not business work or
// home writers. There is no timer, worker, FD or image state owned by this leaf.
// Source and Sign must not reenter this owner. Entries persist until Forget,
// so a retry after expiry cannot recreate the same transaction's eight seconds.
type RouteSigner struct {
	mu       sync.Mutex
	cfg      Config
	prepared map[routeKey]preparedRoute
}

var _ infracontract.Routes = (*RouteSigner)(nil)

// NewRoutes validates bindings without probing a service, loading a key or
// advertising support. A typed Source remains a production obligation: the
// composition factory installs the real observation owner before advertising
// infra capability. The default clock shares the existing BOOTTIME budget.
func NewRoutes(cfg Config) (*RouteSigner, error) {
	if cfg.Domain == (infracontract.ID{}) || cfg.KeyID == "" || cfg.Sign == nil || cfg.Source == nil {
		return nil, ErrInvalid
	}
	if cfg.Clock == nil {
		cfg.Clock = controlbudget.Now
	}
	return &RouteSigner{cfg: cfg, prepared: make(map[routeKey]preparedRoute)}, nil
}

// Prepare emits intent for the sealed participant set as an atomic batch.
// Consumers install every proof and collect ACKs before permitting Move/LDM;
// this slice says nothing about a future connection's CID or owner. Return
// starts from a separately observed normal stable phase and challenge, rather
// than repeatedly refreshing outbound protection across the entire component
// operation. Existing home writers continue throughout the route protocol.
func (r *RouteSigner) Prepare(p infracontract.Plan, c infracontract.Checkpoint, direction infracontract.RouteDirection, deadline infracontract.Deadline) ([]infracontract.RouteProof, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now, err := r.now(p, direction, deadline)
	if err != nil {
		return nil, err
	}
	k := routeKey{p.Tx, p.Step, direction}
	states, err := r.cfg.Source.Read(p, c, direction, deadline)
	if err != nil {
		return nil, err
	}
	if prev, ok := r.prepared[k]; ok {
		// A reply retry cannot change the sealed cohort, target or allocation.
		// Re-read authority instead of treating the cached intent as a lease.
		// origin: hot-update S9 plan §4.6 / T37
		if prev.Plan != p || prev.Checkpoint != c || len(prev.Bound) != 0 {
			return nil, ErrTransition
		}
		if len(states) != len(prev.Proofs) {
			return nil, infracontract.ErrIncomplete
		}
		for _, proof := range prev.Proofs {
			matched := 0
			for _, s := range states {
				if s.Attachment.Participant == proof.Participant {
					if !r.validState(s, p) || s.Attachment != proof.Old || s.Target != proof.Target || s.HomeDigest != proof.HomeDigest || s.AuthorityRevision != proof.AuthorityRevision || s.PolicyRevision != proof.PolicyRevision || s.Nonce != proof.Nonce {
						return nil, ErrIdentity
					}
					matched++
				}
			}
			if matched != 1 {
				return nil, ErrIdentity
			}
		}
		prev.ExpiresNS = min(prev.ExpiresNS, int64(deadline))
		r.prepared[k] = prev
		return r.resign(prev.Proofs, prev.ExpiresNS, now)
	}
	// Capture t0 BEFORE reading/signing. Queueing and observation cost consume
	// the grant rather than receiving another full window after the callback.
	expiry := min(now+int64(upgradebudget.BrokerMaintenanceTTL), int64(deadline))
	proofs := make([]infracontract.RouteProof, 0, len(states))
	seen := make(map[infracontract.ID]bool)
	for _, s := range states {
		id := s.Attachment.Participant
		if !r.validState(s, p) || !s.Stable || seen[id] {
			return nil, ErrIdentity
		}
		// Step is absent from the frozen wire proof. A fresh observer nonce
		// binds each step/direction instead; never recycle it across hops.
		// origin: hot-update S9 plan §4.6 / T37 / M18
		for _, earlier := range r.prepared {
			for _, proof := range earlier.Proofs {
				if proof.Participant == id && proof.Nonce == s.Nonce {
					return nil, ErrTransition
				}
				// A new nonce is not a way around an unfinished prior move.
				// That transaction must bind or settle before another step can
				// grant protection, even if the old route still looks healthy.
				// origin: hot-update S9 plan §4.6 / T37 / M18
				if proof.Participant == id && len(earlier.Bound) == 0 {
					return nil, ErrTransition
				}
			}
		}
		seen[id] = true
		proofs = append(proofs, infracontract.RouteProof{Purpose: infracontract.RoutePrepare, Direction: direction, Target: s.Target, Version: 1, Tx: p.Tx, Participant: id, Old: s.Attachment, HomeDigest: s.HomeDigest, AuthorityRevision: s.AuthorityRevision, PolicyRevision: s.PolicyRevision, Nonce: s.Nonce, KeyID: r.cfg.KeyID})
	}
	if len(proofs) == 0 || c.Cohort == (infracontract.ID{}) || c.Proof == (infracontract.Digest{}) || !c.Assets.Complete {
		return nil, infracontract.ErrIncomplete
	}
	result, err := r.resign(proofs, expiry, now)
	if err != nil {
		return nil, err
	}
	r.prepared[k] = preparedRoute{Plan: p, Checkpoint: c, ExpiresNS: expiry, Proofs: cloneProofs(result)}
	return result, nil
}

// Bind consumes a real Move/Return result and rereads the normal authority.
// A signed batch is still subject to Cohort.Bind's participant ACK barrier; it
// does not retire an ingress, release a startup fence or make a component
// Completed. Those owners also check their physical obligations. A missing
// participant or stale allocation blocks the batch while leaving ordinary
// failure handling and the original eight-second deadline in place.
func (r *RouteSigner) Bind(p infracontract.Plan, moved infracontract.MoveResult, direction infracontract.RouteDirection, deadline infracontract.Deadline) ([]infracontract.RouteProof, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now, err := r.now(p, direction, deadline)
	if err != nil {
		return nil, err
	}
	k := routeKey{p.Tx, p.Step, direction}
	prev, ok := r.prepared[k]
	if !ok || prev.Plan != p {
		return nil, ErrTransition
	}
	if now >= prev.ExpiresNS {
		return nil, ErrExpired
	}
	// Bind signs all original participants as a batch. A lost data NC or a
	// partial census must not permit the old ingress owner to retire.
	// origin: hot-update S9 plan §4.6 / T36
	if !moved.Census.Complete || moved.Census.Digest == (infracontract.Digest{}) || moved.Census.Generation == 0 || len(moved.Attachments) != len(prev.Proofs) {
		return nil, infracontract.ErrIncomplete
	}
	states, err := r.cfg.Source.Read(p, prev.Checkpoint, direction, deadline)
	if err != nil {
		return nil, err
	}
	if len(states) != len(prev.Proofs) {
		return nil, infracontract.ErrIncomplete
	}
	proofs := cloneProofs(prev.Proofs)
	for i, proof := range proofs {
		var state *State
		var attached *infracontract.Attachment
		for j := range states {
			if states[j].Attachment.Participant == proof.Participant {
				if state != nil {
					return nil, ErrIdentity
				}
				state = &states[j]
			}
		}
		for j := range moved.Attachments {
			if moved.Attachments[j].Participant == proof.Participant {
				if attached != nil {
					return nil, ErrIdentity
				}
				attached = &moved.Attachments[j]
			}
		}
		// A MoveResult is evidence to check, not authority to manufacture a
		// signature. The independent source must agree on every identity leaf,
		// including raw owner, accepted ingress and real health generation.
		// origin: hot-update S9 plan §4.6 / T36
		if state == nil || attached == nil || !r.validState(*state, p) || state.Attachment != *attached || state.Target != proof.Target || state.Nonce != proof.Nonce || state.HomeDigest != proof.HomeDigest || state.AuthorityRevision != proof.AuthorityRevision || state.PolicyRevision != proof.PolicyRevision {
			return nil, ErrIdentity
		}
		proofs[i].Purpose, proofs[i].New = infracontract.RouteBind, *attached
		// Lost Bind replies are queried for the same physical attachment. A
		// second reconnect needs a new normal stable step and challenge; it
		// cannot silently recycle this nonce as another migration permit.
		if len(prev.Bound) != 0 && prev.Bound[i].New != *attached {
			return nil, ErrTransition
		}
	}
	result, err := r.resign(proofs, min(prev.ExpiresNS, int64(deadline)), now)
	if err != nil {
		return nil, err
	}
	prev.ExpiresNS = min(prev.ExpiresNS, int64(deadline))
	prev.Bound = cloneProofs(result)
	r.prepared[k] = prev
	return result, nil
}

// Forget is settlement, not retry: callers may only retire a transaction after
// all participant holds are released and the normal stable phase is observed.
// Transaction IDs are unique and must never be reused after this operation.
// The transaction owner supplies that terminal boundary; the cohort owner
// releases all participant holds. Forget does not close a data socket or
// cancel a long-running business operation. Retaining these records through a
// failed step is intentional protection against replay, not an automatic leak.
func (r *RouteSigner) Forget(tx infracontract.ID) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.prepared {
		if key.Tx == tx {
			delete(r.prepared, key)
		}
	}
}

func (r *RouteSigner) now(p infracontract.Plan, direction infracontract.RouteDirection, deadline infracontract.Deadline) (int64, error) {
	// The clock check precedes observations, signing and cache mutation. A
	// broken/overflowing clock has no safe interpretation as a live lease.
	// origin: hot-update S9 plan §4.6 / T37
	if p.Tx == (infracontract.ID{}) || p.Step == 0 || p.Domain != r.cfg.Domain || (p.Component != infracontract.NATS && p.Component != infracontract.Caddy) || (direction != infracontract.RouteOutbound && direction != infracontract.RouteReturn) {
		return 0, ErrIdentity
	}
	now, err := r.cfg.Clock()
	if err != nil || now <= 0 || now > math.MaxInt64-int64(upgradebudget.BrokerMaintenanceTTL) {
		return 0, ErrInvalid
	}
	if int64(deadline) <= now {
		// Admission budget expiry remains a refusal even if every cached
		// physical observation looked healthy. No callback or cache write
		// occurs after this check fails, so it cannot restart maintenance.
		return 0, ErrExpired
	}
	return now, nil
}

func (r *RouteSigner) validState(s State, p infracontract.Plan) bool {
	// This source is the production REGISTER/heartbeat and runtime observer.
	// It must not bless a not-yet-running N1 binary merely because staging
	// verified its bytes. Stock runtime/auth integration is separately P30.
	// origin: hot-update S9 plan §4.6 / T36
	if s.Domain != r.cfg.Domain || !s.Healthy || !s.Registered || s.Target.Component != p.Component || s.PolicyRevision != p.PolicyRevision {
		return false
	}
	return s.Target.Ingress != (infracontract.Process{}) || (s.Target.Before == p.Before && s.Target.Candidate == p.Candidate)
}

func (r *RouteSigner) resign(proofs []infracontract.RouteProof, expiry, started int64) ([]infracontract.RouteProof, error) {
	// Remaining is a duration, not an issuer timestamp to compare on another
	// machine. The observer maps it from its request's local send time and
	// clamps to the installed expiry; transport and signer delay consume the
	// grant. Smaller caller deadlines stay smaller on subsequent retries.
	now, err := r.cfg.Clock()
	if err != nil || now < started {
		return nil, ErrInvalid
	}
	if now >= expiry {
		return nil, ErrExpired
	}
	result := cloneProofs(proofs)
	for i := range result {
		result[i].RemainingNS = uint64(expiry - now)
		result[i], err = SignProof(result[i], r.cfg.Domain, r.cfg.Sign)
		if err != nil {
			return nil, err
		}
	}
	// A slow signer cannot return an ACK for a grant which already expired.
	end, err := r.cfg.Clock()
	if err != nil || end < now {
		return nil, ErrInvalid
	}
	if end >= expiry {
		return nil, ErrExpired
	}
	return result, nil
}

// Internal intent and bound receipts survive caller retries and transport
// buffers. Copy both levels: copying only the slice of structs leaves their
// Signature slices shared with the caller, which could corrupt later replies
// or make an authentic stored receipt appear to change across a retry.
// The structs themselves have no other mutable reference fields.
func cloneProofs(proofs []infracontract.RouteProof) []infracontract.RouteProof {
	result := append([]infracontract.RouteProof(nil), proofs...)
	for i := range result {
		result[i].Signature = bytes.Clone(result[i].Signature)
	}
	return result
}
