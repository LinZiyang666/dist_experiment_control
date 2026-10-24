// Package infrapriv owns the neutral, local infrastructure authorization boundary.
// It never imports a business bootstrap, broker, agent, or NATS connection.
// origin: hot-update S9 plan §2 / §6.3 / T07
package infrapriv

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"
	"unicode/utf8"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

var (
	ErrPermission    = errors.New("infra_permission_denied")
	ErrPolicyStale   = errors.New("infra_policy_stale")
	ErrPolicy        = errors.New("infra_config_invalid")
	ErrBusy          = errors.New("infra_busy")
	ErrCredential    = fmt.Errorf("%w: invalid infrastructure credential", ErrPermission)
	ErrNotEnabled    = errors.New("infra_not_enabled: run /usr/local/libexec/tether-infra-helper bootstrap as root")
	ErrIPC           = errors.New("infra_internal")
	ErrResultUnknown = errors.New("infra_result_unknown")
	ErrBudget        = errors.New("infra_budget_exceeded")
)

// Scope is closed: sharing tether's UID cannot turn a launch key into an infra
// authority, nor turn either local key into a root management authorization.
type Scope uint8

const (
	AccountAuthority Scope = 1
	LocalAuthority   Scope = 2
	LaunchAuthority  Scope = 3
	RootAuthority    Scope = 4
)

type PolicyKey struct {
	KeyID     string
	PublicKey []byte
	UID       uint32
	GID       uint32
	Scope     Scope
}

// Policy is root-owned public material. AccountPublicKey is the fixed issuer;
// HostBootID and Domain bind local keys as tightly as the account authority.
// The whole record changes together; partial credential updates grant nothing.
type Policy struct {
	Version          uint16
	HostBootID       string
	Domain           c.ID
	Revision         uint64
	AccountPublicKey []byte
	TrustSHA256      c.Digest
	ServiceUID       uint32
	ServiceGID       uint32
	Keys             []PolicyKey
}

// PolicyDescriptors describes the actual loader/codec roots for P35, rather
// than a second shadow struct that could omit newly admitted authority fields.
func PolicyDescriptors() []c.RecordDescriptor {
	return []c.RecordDescriptor{
		{Name: "Policy", Version: 1, MaxBytes: c.MaxRequestBytes, Fields: []c.FieldDescriptor{
			{Name: "Version", Type: "uint16"}, {Name: "HostBootID", Type: "string"}, {Name: "Domain", Type: "infracontract.ID"},
			{Name: "Revision", Type: "uint64"}, {Name: "AccountPublicKey", Type: "[]uint8"}, {Name: "TrustSHA256", Type: "infracontract.Digest"}, {Name: "ServiceUID", Type: "uint32"},
			{Name: "ServiceGID", Type: "uint32"}, {Name: "Keys", Type: "[]infrapriv.PolicyKey"},
		}},
		{Name: "PolicyKey", Version: 1, MaxBytes: c.MaxRequestBytes, Fields: []c.FieldDescriptor{
			{Name: "KeyID", Type: "string"}, {Name: "PublicKey", Type: "[]uint8"}, {Name: "UID", Type: "uint32"},
			{Name: "GID", Type: "uint32"}, {Name: "Scope", Type: "infrapriv.Scope"},
		}},
	}
}

func EncodePolicy(p Policy) ([]byte, error) {
	for _, pair := range []struct {
		typ reflect.Type
		row c.RecordDescriptor
	}{
		{reflect.TypeFor[Policy](), PolicyDescriptors()[0]}, {reflect.TypeFor[PolicyKey](), PolicyDescriptors()[1]},
	} {
		if pair.typ.NumField() != len(pair.row.Fields) {
			return nil, ErrPolicy
		}
		for i, field := range pair.row.Fields {
			actual := pair.typ.Field(i)
			if actual.Name != field.Name || actual.Type.String() != field.Type || actual.Tag != "" {
				return nil, ErrPolicy
			}
		}
	}
	if err := validatePolicy(p); err != nil {
		return nil, err
	}
	data, err := json.Marshal(p)
	if err != nil || len(data) > c.MaxRequestBytes {
		return nil, ErrPolicy
	}
	return data, nil
}

// Re-encoding rejects duplicate keys, case aliases, unknowns, missing fields,
// trailing bytes and alternate byte-array spellings before the policy is used.
// origin: hot-update S9 plan §2.2 / §6.3 / T07
func DecodePolicy(data []byte) (Policy, error) {
	var p Policy
	if len(data) == 0 || len(data) > c.MaxRequestBytes || !utf8.Valid(data) {
		return p, ErrPolicy
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if d.Decode(&p) != nil {
		return Policy{}, ErrPolicy
	}
	canonical, err := EncodePolicy(p)
	if err != nil || !bytes.Equal(canonical, data) {
		return Policy{}, ErrPolicy
	}
	return p, nil
}

func validatePolicy(p Policy) error {
	if p.Version != 1 || p.HostBootID == "" || !utf8.ValidString(p.HostBootID) || p.Domain == (c.ID{}) || p.Revision == 0 ||
		len(p.AccountPublicKey) != ed25519.PublicKeySize || p.TrustSHA256 == (c.Digest{}) || p.ServiceUID == 0 || p.ServiceGID == 0 || len(p.Keys) == 0 || len(p.Keys) > 16 {
		return ErrPolicy
	}
	for i, k := range p.Keys {
		if k.KeyID == "" || len(k.KeyID) > 128 || !utf8.ValidString(k.KeyID) || len(k.PublicKey) != ed25519.PublicKeySize {
			return ErrPolicy
		}
		for _, prev := range p.Keys[:i] {
			issuerPair := ((prev.Scope == AccountAuthority && k.Scope == RootAuthority) || (prev.Scope == RootAuthority && k.Scope == AccountAuthority)) && bytes.Equal(k.PublicKey, p.AccountPublicKey)
			if prev.KeyID == k.KeyID || (bytes.Equal(prev.PublicKey, k.PublicKey) && !issuerPair) {
				return ErrPolicy
			}
		}
		switch k.Scope {
		case AccountAuthority:
			if !bytes.Equal(k.PublicKey, p.AccountPublicKey) || k.UID != p.ServiceUID || k.GID != p.ServiceGID {
				return ErrPolicy
			}
		case LocalAuthority, LaunchAuthority:
			if k.UID != p.ServiceUID || k.GID != p.ServiceGID {
				return ErrPolicy
			}
		case RootAuthority:
			if k.UID != 0 || k.GID != 0 {
				return ErrPolicy
			}
		default:
			return ErrPolicy
		}
	}
	return nil
}

func clonePolicy(p Policy) Policy {
	p.AccountPublicKey = slices.Clone(p.AccountPublicKey)
	p.Keys = slices.Clone(p.Keys)
	for i := range p.Keys {
		p.Keys[i].PublicKey = slices.Clone(p.Keys[i].PublicKey)
	}
	return p
}

// PolicyStore pins the admitted epoch while an effect is in progress, without
// holding a mutex across a socket, systemd job or launcher handshake. Persistent
// domain/sequence/nonce CAS remains P04's responsibility, including across workers.
type PolicyStore struct {
	mu             sync.Mutex
	current        Policy
	active         uint32
	rotating       bool
	rotationDigest c.Digest
	committing     bool
	failed         bool
}

func NewPolicyStore(p Policy) (*PolicyStore, error) {
	if err := validatePolicy(p); err != nil {
		return nil, err
	}
	return &PolicyStore{current: clonePolicy(p)}, nil
}

func (s *PolicyStore) Snapshot() Policy {
	s.mu.Lock()
	defer s.mu.Unlock()
	return clonePolicy(s.current)
}

// Authorize accepts only a peer obtained from PeerCredentials at the IPC edge.
// Root still needs its own signed key; SO_PEERCRED alone does not select an issuer.
// origin: hot-update S9 plan §2.2 / §6.2 / §6.3 / T07
func (s *PolicyStore) Authorize(r c.HostRequest, peer c.Peer) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.authorizeLocked(r, peer)
}

func (s *PolicyStore) authorizeLocked(r c.HostRequest, peer c.Peer) error {
	p := s.current
	if s.failed || r.PolicyRevision != p.Revision || r.HostBootID != p.HostBootID || r.Domain != p.Domain {
		return ErrPolicyStale
	}
	if peer.PID <= 0 || peer.StartTicks == 0 || peer.BootID != p.HostBootID {
		return ErrPermission
	}
	for _, k := range p.Keys {
		if k.KeyID != r.KeyID {
			continue
		}
		if peer.UID != k.UID || peer.GID != k.GID || c.VerifyRequest(r, k.PublicKey) != nil {
			return ErrPermission
		}
		if !scopeAllows(k.Scope, r.Op) {
			return ErrPermission
		}
		return nil
	}
	return ErrPolicyStale
}

func scopeAllows(scope Scope, op c.HostOp) bool {
	if op == c.RotatePolicy || op == c.UpdateSupport {
		return scope == RootAuthority
	}
	if op == c.LaunchClaim || op == c.LaunchAck {
		return scope == LaunchAuthority
	}
	return scope == AccountAuthority || scope == LocalAuthority || scope == RootAuthority
}

// begin holds admission, not a journal lock. RotatePolicy gets exclusive
// admission; an already admitted effect must finish before it can revoke keys.
func (s *PolicyStore) begin(r c.HostRequest, peer c.Peer) (func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.authorizeLocked(r, peer); err != nil {
		return nil, err
	}
	if s.rotating || (r.Op == c.RotatePolicy && s.active != 0) {
		return nil, ErrBusy
	}
	if r.Op == c.RotatePolicy {
		s.rotating = true
		data, _ := c.EncodeRequest(r) // authorizeLocked has already verified the complete canonical request.
		s.rotationDigest = sha256.Sum256(data)
	} else {
		s.active++
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			if r.Op == c.RotatePolicy {
				s.rotating = false
				s.rotationDigest = c.Digest{}
			} else {
				s.active--
			}
		})
	}, nil
}

// Rotate commits a complete replacement through the root journal owner. The
// commit callback must CAS/fsync policy and credential selectors under the SAME
// domain lease as the host request, before returning. Failed/unknown persistence
// seals this store; a new worker must load the durable state and re-authorize.
// Old pending transactions remain queryable in P04; this API does not resume them.
// origin: hot-update S9 plan §6.3 / T07
func (s *PolicyStore) Rotate(r c.HostRequest, next Policy, persist func(Policy, Policy) error) error {
	if r.Op != c.RotatePolicy || persist == nil || validatePolicy(next) != nil {
		return ErrPolicy
	}
	s.mu.Lock()
	before := clonePolicy(s.current)
	requestBytes, requestErr := c.EncodeRequest(r)
	if !s.rotating || s.committing || s.failed || s.active != 0 || requestErr != nil || sha256.Sum256(requestBytes) != s.rotationDigest || r.PolicyRevision != before.Revision || r.Domain != before.Domain || r.HostBootID != before.HostBootID ||
		next.Revision <= before.Revision || next.Domain != before.Domain || next.HostBootID != before.HostBootID ||
		next.ServiceUID != before.ServiceUID || next.ServiceGID != before.ServiceGID {
		s.mu.Unlock()
		return ErrPolicyStale
	}
	// A payload digest resolves one root-verified policy, not caller JSON or a path.
	data, err := EncodePolicy(next)
	if err != nil || sha256.Sum256(data) != r.Payload {
		s.mu.Unlock()
		return ErrPolicy
	}
	// An epoch bump alone cannot retire a still accepted signing seed. Root
	// explicitly installs fresh key IDs/public keys together with the trust ref;
	// old issuers cannot mint a new-revision ticket after their own revocation.
	// origin: hot-update S9 plan §6.3 / T07
	for _, old := range before.Keys {
		for _, fresh := range next.Keys {
			if old.KeyID == fresh.KeyID || bytes.Equal(old.PublicKey, fresh.PublicKey) {
				s.mu.Unlock()
				return ErrPolicyStale
			}
		}
	}
	s.committing = true
	s.mu.Unlock()
	if err := persist(before, clonePolicy(next)); err != nil {
		s.mu.Lock()
		s.failed = true
		s.committing = false
		s.mu.Unlock()
		return ErrPolicyStale
	}
	s.mu.Lock()
	s.current = clonePolicy(next)
	s.committing = false
	s.mu.Unlock()
	return nil
}

// PolicyBackend is the RotatePolicy-only HostBinding. Resolve must read an
// immutable root record named by the digest. The P04 worker owns durable intent,
// sequence replay and the final Journal/Revision fields in the returned receipt.
type PolicyBackend struct {
	Store   *PolicyStore
	Resolve func(c.Digest, c.Deadline, c.Stop) (Policy, error)
	Persist func(Policy, Policy, c.Deadline, c.Stop) error
}

var _ c.Host = (*PolicyBackend)(nil)

func (b *PolicyBackend) Do(r c.HostRequest, deadline c.Deadline, stop c.Stop) (c.HostReceipt, error) {
	if b == nil || b.Store == nil || b.Resolve == nil || b.Persist == nil || r.Op != c.RotatePolicy {
		return c.HostReceipt{}, ErrPolicy
	}
	if err := checkBudget(deadline, stop); err != nil {
		return c.HostReceipt{}, err
	}
	next, err := b.Resolve(r.Payload, deadline, stop)
	if err != nil {
		return c.HostReceipt{}, ErrPolicy
	}
	if err := checkBudget(deadline, stop); err != nil {
		return c.HostReceipt{}, err
	}
	if err := b.Store.Rotate(r, next, func(before, after Policy) error { return b.Persist(before, after, deadline, stop) }); err != nil {
		return c.HostReceipt{}, err
	}
	data, err := c.EncodeRequest(r)
	if err != nil {
		return c.HostReceipt{}, ErrPolicy
	}
	return c.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Op: r.Op, Applied: true, RequestDigest: sha256.Sum256(data)}, nil
}

func (*PolicyBackend) Inspect(c.ID, c.Deadline) (c.Census, error) {
	return c.Census{}, c.ErrUnsupported
}

// Formatting a credential loader or a policy store must never dump private
// fields through fmt's recursive struct formatting in diagnostics.
func (*PolicyStore) Format(state fmt.State, _ rune) {
	_, _ = state.Write([]byte("infrapriv.PolicyStore"))
}
