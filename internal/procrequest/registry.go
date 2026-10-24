// Package procrequest owns reliable process request admission. It retains
// identities independently of connections and runtime generations. Callers
// must authorize every operation and join this table to their resource fence.
package procrequest

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
)

const (
	DefaultLimit = 4096
	MaxLimit     = 16384
	DefaultTTL   = 30 * time.Second
	MaxTTL       = 5 * time.Minute
)

var (
	ErrIdentity = errors.New("process request: invalid identity or lease")
	// ErrInstance names the one identity failure a client can act on: the
	// lease was signed by another runtime instance (a cold restart or an
	// image switch that lost its table), so this task's continuity is gone.
	// It is decided BEFORE the MAC check because a foreign lease cannot be
	// verified at all, and the instance name is already public in every
	// capability reply. origin: docs/reviews/hot-update-reliable-exec-plan.md §3.7.
	ErrInstance = errors.New("process request: lease was issued by another runtime instance; task continuity lost")
	ErrExpired  = errors.New("process request: result expired; execution is not retried")
	ErrConflict = errors.New("process request: identity reused with different intent")
	ErrCapacity = errors.New("process request: admission capacity exhausted")
	ErrFrozen   = errors.New("process request: ownership frozen")
	ErrBusy     = errors.New("process request: creation still in progress")
	ErrStale    = errors.New("process request: stale attachment")
	ErrUnknown  = errors.New("process request: unknown request")
)

type Scope struct {
	SID           string
	NID           string
	ActorFP       string
	ClientLineage string
}

type Terminal struct {
	ID       string `json:"id"`
	ExitCode int    `json:"exit_code"`
	Signal   int    `json:"signal"`
	Error    string `json:"error,omitempty"`
}

// Record has no borrowed memory. Terminal delivery and broker lifecycle
// delivery are separate responsibilities; settling this record means only
// that the authorized client has taken its output and terminal result.
type Record struct {
	// Kind stays with settled receipts until their creation lease expires;
	// rollback readers still need the PTY domain after resources are released.
	// origin: hot-update S4 plan §5.1 / T17
	Kind        string              `json:"kind,omitempty"`
	Handle      proto.ProcessHandle `json:"handle"`
	IntentHash  string              `json:"intent_hash"`
	PID         string              `json:"pid"`
	Inbox       string              `json:"inbox"`
	AttachEpoch uint64              `json:"attach_epoch"`
	Phase       string              `json:"phase"`
	Terminal    *Terminal           `json:"terminal,omitempty"`
}

type requestKey struct {
	scope     Scope
	leaseID   string
	requestID string
}

type Registry struct {
	mu       sync.RWMutex
	instance string
	bootID   string
	secret   []byte
	limit    int
	ttl      time.Duration
	now      func() (int64, error)
	frozen   bool
	records  map[requestKey]Record
}

func New(instance string, limit int, ttl time.Duration) (*Registry, error) {
	return NewWithClock(instance, limit, ttl, nil)
}

// NewWithClock lets a runtime share one injected boot clock with its registry
// (nil selects the real CLOCK_BOOTTIME). Lease expiry, abandoned-terminal
// grace and tombstone TTL must all be measured on the same clock, and the
// registry's clock is private, so a test of that interplay needs this seam.
func NewWithClock(instance string, limit int, ttl time.Duration, clock func() (int64, error)) (*Registry, error) {
	if limit == 0 {
		limit = DefaultLimit
	}
	if ttl == 0 {
		ttl = DefaultTTL
	}
	if !validToken(instance) || limit < 1 || limit > MaxLimit || ttl <= 0 || ttl > MaxTTL {
		return nil, ErrIdentity
	}
	if clock == nil {
		clock = bootNow
	}
	boot, err := currentBootID()
	if err != nil {
		return nil, err
	}
	secret := make([]byte, sha256.Size)
	if _, err := rand.Read(secret); err != nil {
		return nil, err
	}
	return &Registry{instance: instance, bootID: boot, secret: secret, limit: limit,
		ttl: ttl, now: clock, records: make(map[requestKey]Record)}, nil
}

// BootNow reads the same monotonic-across-suspend clock leases are issued on.
func BootNow() (int64, error) { return bootNow() }

// TTL reports the creation lease lifetime this registry issues.
func (r *Registry) TTL() time.Duration { return r.ttl }

func (r *Registry) Issue(scope Scope) (proto.ProcessLease, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return proto.ProcessLease{}, ErrFrozen
	}
	if !validScope(scope) {
		return proto.ProcessLease{}, ErrIdentity
	}
	now, err := r.now()
	if err != nil || now < 0 || now > (1<<63-1)-int64(r.ttl) {
		return proto.ProcessLease{}, fmt.Errorf("process request: boot clock unavailable: %v", err)
	}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return proto.ProcessLease{}, err
	}
	l := proto.ProcessLease{Version: 1, LeaseID: hex.EncodeToString(id), TargetInstance: r.instance,
		BootID: r.bootID, SID: scope.SID, NID: scope.NID, ActorFP: scope.ActorFP,
		ClientLineage: scope.ClientLineage, IssuedBootNS: now, ExpiresBootNS: now + int64(r.ttl)}
	l.MAC = r.sign(l)
	return l, nil
}

// Reserve elects exactly one caller to create the child. Only a true new
// result grants creation; errors and duplicate records grant no side effect.
// A rejected Start must be completed as a terminal record, never deleted.
func (r *Registry) Reserve(scope Scope, handle proto.ProcessHandle, spec proto.ExecReq, pid, inbox string, epoch uint64) (Record, bool, error) {
	digest, err := intentHash(spec)
	if err != nil {
		return Record{}, false, err
	}
	return r.reserve(scope, handle, "", digest, pid, inbox, epoch)
}

// Both task kinds share one identity and attachment table. A request cannot
// change domains on retry, even if a corrupt image gives it the other digest.
// origin: hot-update S4 plan §2.1 / T17
func (r *Registry) reserve(scope Scope, handle proto.ProcessHandle, kind, digest, pid, inbox string, epoch uint64) (Record, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return Record{}, false, ErrFrozen
	}
	key, err := r.validate(scope, handle)
	if err != nil {
		return Record{}, false, err
	}
	if record, exists := r.records[key]; exists {
		if record.Kind != kind || record.IntentHash != digest {
			return Record{}, false, ErrConflict
		}
		if err := attach(&record, inbox, epoch); err != nil {
			return Record{}, false, err
		}
		r.records[key] = record
		return copyRecord(record), false, nil
	}
	if err := r.unseen(handle.Lease); err != nil {
		return Record{}, false, err
	}
	if len(r.records) >= r.limit {
		return Record{}, false, ErrCapacity
	}
	if !validToken(pid) {
		return Record{}, false, ErrIdentity
	}
	for _, record := range r.records {
		if record.PID == pid {
			return Record{}, false, ErrConflict
		}
	}
	record := Record{Kind: kind, Handle: handle, IntentHash: digest, PID: pid, Phase: "starting"}
	if err := attach(&record, inbox, epoch); err != nil {
		return Record{}, false, err
	}
	r.records[key] = record
	return record, true, nil
}

func (r *Registry) Lookup(scope Scope, handle proto.ProcessHandle) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, record, err := r.lookup(scope, handle)
	return copyRecord(record), err
}

func (r *Registry) Attach(scope Scope, handle proto.ProcessHandle, inbox string, epoch uint64) (Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return Record{}, ErrFrozen
	}
	key, record, err := r.lookup(scope, handle)
	if err != nil {
		return Record{}, err
	}
	if err := attach(&record, inbox, epoch); err != nil {
		return Record{}, err
	}
	r.records[key] = record
	return copyRecord(record), nil
}

func (r *Registry) Running(handle proto.ProcessHandle, pid string) error {
	return r.transition(handle, pid, nil)
}

func (r *Registry) Complete(handle proto.ProcessHandle, pid string, terminal Terminal) error {
	if !validTerminal(terminal) {
		return ErrIdentity
	}
	return r.transition(handle, pid, &terminal)
}

func (r *Registry) transition(handle proto.ProcessHandle, pid string, terminal *Terminal) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return ErrFrozen
	}
	key, record, err := r.lookup(scopeOf(handle.Lease), handle)
	if err != nil {
		return err
	}
	if record.PID != pid {
		return ErrIdentity
	}
	if terminal == nil {
		if record.Phase != "starting" && record.Phase != "running" {
			return ErrConflict
		}
		record.Phase = "running"
	} else if record.Terminal != nil {
		if *record.Terminal != *terminal {
			return ErrConflict
		}
		return nil
	} else {
		record.Phase, record.Terminal = "terminal", terminal
	}
	r.records[key] = record
	return nil
}

// Settle is called only after the stream owner has verified output offsets,
// FINs and the matching terminal ID. It does not itself acknowledge bytes.
func (r *Registry) Settle(scope Scope, handle proto.ProcessHandle, terminalID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return ErrFrozen
	}
	key, record, err := r.lookup(scope, handle)
	if err != nil {
		return err
	}
	if record.Terminal == nil || record.Terminal.ID != terminalID {
		return ErrConflict
	}
	record.Phase = "settled"
	r.records[key] = record
	return nil
}

// Collect reclaims only settled records whose original creation lease has
// expired. A clock failure retains responsibility and frees nothing.
func (r *Registry) Collect() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return 0
	}
	now, err := r.now()
	if err != nil {
		return 0
	}
	n := 0
	for key, record := range r.records {
		if record.Phase == "settled" && now >= record.Handle.Lease.ExpiresBootNS {
			delete(r.records, key)
			n++
		}
	}
	return n
}

func (r *Registry) lookup(scope Scope, handle proto.ProcessHandle) (requestKey, Record, error) {
	key, err := r.validate(scope, handle)
	if err != nil {
		return key, Record{}, err
	}
	if record, exists := r.records[key]; exists {
		return key, record, nil
	}
	if err := r.unseen(handle.Lease); err != nil {
		return key, Record{}, err
	}
	return key, Record{}, ErrUnknown
}

func (r *Registry) validate(scope Scope, h proto.ProcessHandle) (requestKey, error) {
	l := h.Lease
	key := requestKey{scope: scope, leaseID: l.LeaseID, requestID: h.RequestID}
	if l.TargetInstance != r.instance || l.BootID != r.bootID {
		return key, ErrInstance
	}
	if !validScope(scope) || scope != scopeOf(l) || !validToken(h.RequestID) ||
		l.Version != 1 ||
		!validHex(l.LeaseID, 16) || l.IssuedBootNS < 0 || l.ExpiresBootNS <= l.IssuedBootNS ||
		l.ExpiresBootNS-l.IssuedBootNS > int64(r.ttl) || !hmac.Equal([]byte(l.MAC), []byte(r.sign(l))) {
		return key, ErrIdentity
	}
	return key, nil
}

func (r *Registry) unseen(l proto.ProcessLease) error {
	now, err := r.now()
	if err != nil {
		return fmt.Errorf("process request: read boot clock: %w", err)
	}
	if now < l.IssuedBootNS || now >= l.ExpiresBootNS {
		return ErrExpired
	}
	return nil
}

func (r *Registry) sign(l proto.ProcessLease) string {
	l.MAC = ""
	body, _ := json.Marshal(l)
	mac := hmac.New(sha256.New, r.secret)
	_, _ = mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func scopeOf(l proto.ProcessLease) Scope {
	return Scope{SID: l.SID, NID: l.NID, ActorFP: l.ActorFP, ClientLineage: l.ClientLineage}
}

func validScope(s Scope) bool {
	return validToken(s.SID) && validToken(s.NID) && validToken(s.ClientLineage) && auth.ValidFingerprint(s.ActorFP)
}

func validToken(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range []byte(s) {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return false
		}
	}
	return true
}

func validHex(s string, n int) bool {
	if len(s) != 2*n {
		return false
	}
	b, err := hex.DecodeString(s)
	return err == nil && hex.EncodeToString(b) == s
}

func validTerminal(t Terminal) bool {
	return validToken(t.ID) && t.ExitCode >= -1 && t.ExitCode <= 255 && t.Signal >= 0 &&
		t.Signal <= 64 && (t.Signal == 0 || t.ExitCode == -1) && len(t.Error) <= 4096 && utf8.ValidString(t.Error)
}

func attach(record *Record, inbox string, epoch uint64) error {
	if len(inbox) == 0 || len(inbox) > 512 || epoch == 0 {
		return ErrIdentity
	}
	if epoch < record.AttachEpoch || (epoch == record.AttachEpoch && inbox != record.Inbox) {
		return ErrStale
	}
	record.Inbox, record.AttachEpoch = inbox, epoch
	return nil
}

func copyRecord(record Record) Record {
	if record.Terminal != nil {
		t := *record.Terminal
		record.Terminal = &t
	}
	return record
}

func intentHash(spec proto.ExecReq) (string, error) {
	if len(spec.Argv) == 0 || spec.Argv[0] == "" || !validArgument(spec.Cwd) {
		return "", ErrIdentity
	}
	for _, arg := range spec.Argv {
		if !validArgument(arg) {
			return "", ErrIdentity
		}
	}
	for key, value := range spec.Env {
		if key == "" || strings.Contains(key, "=") || !validArgument(key) || !validArgument(value) {
			return "", ErrIdentity
		}
	}
	// encoding/json sorts string map keys. ActorFP is authenticated metadata,
	// not part of the submitted intent; the lease already binds the actor.
	spec.ActorFP = ""
	body, err := json.Marshal(spec)
	if err != nil || len(body) > 1<<20 {
		return "", ErrIdentity
	}
	digest := sha256.Sum256(append([]byte("tether-process-intent-v1\n"), body...))
	return hex.EncodeToString(digest[:]), nil
}

func validArgument(s string) bool {
	return utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}

// ReservePTY elects a terminal's creation owner using the normalized Run
// intent. ActorFP in spec is untrusted metadata; the authenticated scope and
// signed lease must agree before either a new or retained record is exposed.
// origin: hot-update S4 plan §2.3 / T17
func (r *Registry) ReservePTY(scope Scope, handle proto.ProcessHandle, spec proto.RunReq, pid, inbox string, epoch uint64) (Record, bool, error) {
	digest, err := ptyIntentHash(spec)
	if err != nil {
		return Record{}, false, err
	}
	return r.reserve(scope, handle, proto.ProcessKindPTY, digest, pid, inbox, epoch)
}

// RetainedKinds returns the sorted, distinct kinds in every retained phase,
// including starting and settled. The empty string is the original exec kind,
// not an empty table. Reading this summary never freezes or collects records.
// origin: hot-update S4 plan §5.2 / T17
func (r *Registry) RetainedKinds() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := make(map[string]bool)
	for _, record := range r.records {
		seen[record.Kind] = true
	}
	var kinds []string
	for kind := range seen {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

// Validate before hashing: JSON can replace invalid UTF-8, and a NUL cannot
// reach execve. Accepting either would make the committed intent differ from
// the executable argv/environment rather than rejecting it at admission.
// origin: hot-update S4 plan §2.2 / §8.1 / T17
func ptyIntentHash(spec proto.RunReq) (string, error) {
	if len(spec.Argv) == 0 || spec.Argv[0] == "" || !validArgument(spec.Cwd) {
		return "", ErrIdentity
	}
	for _, arg := range spec.Argv {
		if !validArgument(arg) {
			return "", ErrIdentity
		}
	}
	for key, value := range spec.Env {
		if key == "" || strings.Contains(key, "=") || !validArgument(key) || !validArgument(value) {
			return "", ErrIdentity
		}
	}
	// Reject overflow before defaulting either dimension: an invalid peer
	// must not hide uint16 truncation behind an unspecified other dimension.
	// origin: hot-update S4 plan §8.1 / T17
	if spec.Cols > 65535 || spec.Rows > 65535 || spec.KAIntervalMS < 0 {
		return "", ErrIdentity
	}
	if spec.Cols <= 0 || spec.Rows <= 0 {
		spec.Cols, spec.Rows = 80, 24
	}
	// Clamp integer milliseconds before any duration conversion can overflow.
	// Zero remains disarmed; positive values commit the same [1s,60s] contract
	// as the actor, not a different intent for equivalent advertised intervals.
	// origin: hot-update S4 plan §6.1 / T17
	if spec.KAIntervalMS > 0 {
		spec.KAIntervalMS = max(1000, min(spec.KAIntervalMS, 60000))
	}
	// encoding/json sorts Env keys and omits both nil and empty maps. Empty
	// argv slices, including nil, were rejected above because there is no
	// executable. Keep Run's full shape and its own prefix: casting to ExecReq
	// silently drops size and keepalive intent and changes the exec ABI.
	// origin: hot-update S4 plan §8.1 / T17
	spec.ActorFP = ""
	body, err := json.Marshal(spec)
	if err != nil || len(body) > 1<<20 {
		return "", ErrIdentity
	}
	digest := sha256.Sum256(append([]byte("tether-pty-intent-v1\n"), body...))
	return hex.EncodeToString(digest[:]), nil
}
