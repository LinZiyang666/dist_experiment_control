package inframaint

import (
	"bytes"
	"crypto/ed25519"
	"math"
	"sync"

	"github.com/LinZiyang666/tether/internal/infracontract"
)

// Challenge is created on the observer's clock before requesting a proof.
// Keeping t0, boot and nonce together consumes transport delay and prevents a
// delayed reply, reboot or new worker from granting another eight seconds.
// The observer owns this record until settlement. It cannot reconstruct t0
// from message arrival or wall time after losing its cache. Direction keeps a
// later genuine return separate from a replay of the original outbound step.
type Challenge struct {
	Domain      infracontract.ID
	Tx          infracontract.ID
	Participant infracontract.ID
	Direction   infracontract.RouteDirection
	Nonce       infracontract.ID
	BootID      string
	StartedNS   int64
}

// View is read under the caller's normal roster/home writer serialization.
// Known asserts a trusted source, not mere last-known Status. Revocation and
// home CAS changes always win; Healthy/Registered are required at Install.
// The caller reads and decides under its existing writer serialization;
// Hold's mutex only protects the local cache. These predicates never replace
// the allocation CAS inside a real home writer. Permission revocation remains
// actionable even while the old control connection is being replaced.
type View struct {
	Attachment        infracontract.Attachment
	HomeDigest        infracontract.Digest
	AuthorityRevision uint64
	PolicyRevision    uint64
	Known             bool
	Healthy           bool
	Registered        bool
	Revoked           bool
}

// Pending contains only the actual dial generation and restricted intent.
// There is no future CID, fabricated raw socket or speculative health epoch.
// Generation belongs to the current cancellable transport owner before it
// starts dialing. A URL pool edit cannot supply this fact. Bind later replaces
// this intent with a complete independently observed physical attachment.
type Pending struct {
	Participant infracontract.ID
	Generation  uint64
	Target      infracontract.RouteTarget
}

// Hold is local memory only, excluded by the common upgrade-domain guard from
// image cuts. No timer is needed: the existing reaper consults Protects inline.
// Ending is irreversible even if an old healthy/home observation returns later.
// A component timeout does not own the business lifetime: ending this cache
// only returns decisions to the existing reaper. It never kills a task/flow.
// Keeping it outside images prevents an importer from making an old challenge
// look freshly installed after losing the observer's original clock mapping.
type Hold struct {
	mu        sync.Mutex
	challenge Challenge
	proof     infracontract.RouteProof
	expiresNS int64
	pending   Pending
	installed bool
	ended     bool
}

// NewHold records a challenge without granting protection. Prepare must first
// verify under the pinned key and match the current healthy old owner. One
// instance has one challenge; it cannot be retargeted to another transaction
// or boot. A genuinely new stable step obtains a separate challenge instead.
func NewHold(c Challenge) (*Hold, error) {
	if c.Domain == (infracontract.ID{}) || c.Tx == (infracontract.ID{}) || c.Participant == (infracontract.ID{}) || c.Nonce == (infracontract.ID{}) || c.BootID == "" || c.StartedNS <= 0 || (c.Direction != infracontract.RouteOutbound && c.Direction != infracontract.RouteReturn) {
		return nil, ErrInvalid
	}
	return &Hold{challenge: c}, nil
}

// Install verifies both purposes with the acquired domain and pinned key.
// Prepare can protect only the exact Old owner. Bind replaces that owner only
// after the one authorized dial and a fresh matching REGISTER/home observation.
// Returned Remaining is the installation receipt used by the caller's ACK.
// That caller accounts for time consumed sending/signing its ACK, instead of
// claiming the same duration from this cached return value later. Signature
// verification cannot suppress revocation: both purposes also match current
// authority and policy revision before changing local protection.
// origin: hot-update S9 plan §4.6 / T36 / T37
func (h *Hold) Install(p infracontract.RouteProof, keyID string, key ed25519.PublicKey, v View, bootID string, sentNS, now int64) (uint64, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.ended || bootID != h.challenge.BootID || now < h.challenge.StartedNS {
		h.ended = true
		return 0, ErrExpired
	}
	// Each reply maps its remaining duration from that request's local t0.
	// First install uses the original challenge t0; retries clamp to the first
	// expiry, so a fresh send timestamp never refreshes an existing lease.
	if sentNS < h.challenge.StartedNS || sentNS > now || (!h.installed && sentNS != h.challenge.StartedNS) {
		return 0, ErrInvalid
	}
	if err := VerifyProof(p, h.challenge.Domain, keyID, key); err != nil {
		return 0, err
	}
	if p.Tx != h.challenge.Tx || p.Participant != h.challenge.Participant || p.Nonce != h.challenge.Nonce || p.Direction != h.challenge.Direction {
		return 0, ErrIdentity
	}
	if !v.Known || v.Revoked || p.HomeDigest != v.HomeDigest || p.AuthorityRevision != v.AuthorityRevision || p.PolicyRevision != v.PolicyRevision {
		h.ended = true
		return 0, ErrIdentity
	}
	if !v.Healthy || !v.Registered {
		return 0, ErrIdentity
	}
	if p.RemainingNS > uint64(math.MaxInt64-sentNS) {
		return 0, ErrInvalid
	}
	expiry := sentNS + int64(p.RemainingNS)
	if h.installed {
		if !sameIntent(h.proof, p) {
			return 0, ErrIdentity
		}
		expiry = min(expiry, h.expiresNS)
	}
	if now >= expiry {
		h.ended = true
		return 0, ErrExpired
	}
	if p.Purpose == infracontract.RoutePrepare {
		if v.Attachment != p.Old || (h.installed && h.proof.Purpose == infracontract.RouteBind) {
			return 0, ErrTransition
		}
	} else {
		if !h.installed || h.pending.Generation == 0 || p.New.Generation != h.pending.Generation || v.Attachment != p.New || (h.proof.Purpose == infracontract.RouteBind && h.proof.New != p.New) {
			return 0, ErrTransition
		}
	}
	p.Signature = bytes.Clone(p.Signature)
	h.proof, h.expiresNS, h.installed = p, expiry, true
	return uint64(expiry - now), nil
}

// BeginDial records one actual transport generation before its cancellable
// dial starts. Repeating that same intent is harmless; substituting another
// worker/generation is never a second authorization under this nonce.
// origin: hot-update S9 plan §4.6 / T36
func (h *Hold) BeginDial(p Pending, v View, bootID string, now int64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.live(v, bootID, now) {
		return ErrExpired
	}
	if h.proof.Purpose != infracontract.RoutePrepare || v.Attachment != h.proof.Old || p.Participant != h.proof.Participant || p.Target != h.proof.Target || p.Generation <= h.proof.Old.Generation || (h.pending.Generation != 0 && h.pending != p) {
		return ErrTransition
	}
	h.pending = p
	return nil
}

// Protects is a narrow predicate for the ordinary roster/reaper owner. Before
// Bind only Old matches; afterwards only the fully observed New matches.
// During planned silence health may decline, but the known physical/home
// identity stays exact and the original expiry still wins. Callers preserve
// historical dwell and run normal failure handling when this returns false;
// no fake ready state or reset of the failure history is produced here.
// Other homes continue through the same ordinary decision path throughout.
func (h *Hold) Protects(v View, bootID string, now int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.live(v, bootID, now) {
		return false
	}
	if h.proof.Purpose == infracontract.RoutePrepare {
		return v.Attachment == h.proof.Old
	}
	return v.Attachment == h.proof.New
}

// ProtectsPending is deliberately distinct from a completed attachment check.
// Callers pass the current dial's real generation/target; this temporary intent
// cannot produce an Attached ACK or sign an unknown physical connection.
// The authority View still comes from the ordinary home source while dialing.
// A worker consults this only for its recorded Pending; another URL, ingress
// invocation or generation cannot inherit that transient protection.
func (h *Hold) ProtectsPending(p Pending, v View, bootID string, now int64) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.live(v, bootID, now) && h.proof.Purpose == infracontract.RoutePrepare && h.pending.Generation != 0 && h.pending == p
}

// Release ends protection without touching the business owner. Cleanup and
// control-NC cancellation are separate from a task/PTY/flow lifetime. Ending
// stays sticky so a correctly signed late reply cannot undo settlement.
func (h *Hold) Release() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ended, h.installed = true, false
}

func (h *Hold) live(v View, bootID string, now int64) bool {
	if h.ended || !h.installed {
		return false
	}
	// A failure latches so restored snapshots cannot resurrect authority. The
	// reaper retains its historical dwell; this predicate never resets it.
	// origin: hot-update S9 plan §4.6 / T37
	if bootID != h.challenge.BootID || now < h.challenge.StartedNS || now >= h.expiresNS || !v.Known || v.Revoked || v.HomeDigest != h.proof.HomeDigest || v.AuthorityRevision != h.proof.AuthorityRevision || v.PolicyRevision != h.proof.PolicyRevision {
		h.ended = true
		return false
	}
	return true
}

func sameIntent(a, b infracontract.RouteProof) bool {
	return a.Version == b.Version && a.Tx == b.Tx && a.Participant == b.Participant && a.Direction == b.Direction && a.Target == b.Target && a.Old == b.Old && a.HomeDigest == b.HomeDigest && a.AuthorityRevision == b.AuthorityRevision && a.PolicyRevision == b.PolicyRevision && a.Nonce == b.Nonce && a.KeyID == b.KeyID
}
