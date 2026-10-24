// Package upgradestore owns the component journal, never the running resources.
// A grant is an authorization stored by the target, not a request to reconstruct
// missing work. Callers supply current runtime proofs at the execution boundary.
// origin: hot-update S8 plan §3 / §11.2 P02
package upgradestore

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Options binds a store to the registered path and target process identity.
// GuardPath is the registry's controlled <domain>.guard path, shared with P04.
// Callers hold the domain lease, never the short guard, across these operations.
// Transition is the component owner's validator (upgradetx.Transition for S8
// executors, the registered S9 adapter for infrastructure). A nil port refuses
// phase changes; grant and action persistence do not depend on that predicate.
// ParentReservation must verify the durable parent slot AND its held reservation
// and return its original absolute deadline. Absence never authorizes a child.
type Options struct {
	Path                 string
	GuardPath            string
	AllowLegacyMigration bool
	Key                  upgradetx.Key
	Owner                upgradetx.Owner
	SourceGeneration     uint64
	Transition           upgradetx.TransitionFunc
	ParentReservation    func(parentTxID, slot string) (int64, error)
}

// Store has no retained descriptors or background workers. Each operation reads
// the journal under the short guard, so another process's CAS is immediately
// visible. The domain's long-lived execution lease remains P04's responsibility.
type Store struct {
	mu         sync.Mutex
	options    Options
	now        func() time.Time
	beforeSync func(string) error
}

var _ upgradetx.Store = (*Store)(nil)

// Open requires an existing controlled parent directory. It does not invent a
// fallback path when the registered journal is corrupt or unwritable.
func Open(options Options) (*Store, error) {
	// The journal is shared infrastructure, not an executor capability gate.
	// S9 supplies its own transition adapter and the registered domain lease;
	// the exact Key still binds every grant, CAS and reopen to one component.
	// origin: hot-update S8 plan §2.1 / §3.1; S9 plan §8 / T44
	if !validKey(options.Key) || !validOwner(options.Owner) || !filepath.IsAbs(options.Path) || !filepath.IsAbs(options.GuardPath) || filepath.Base(options.GuardPath) != options.Key.DomainID+".guard" {
		return nil, failure(upgradetx.CodeRejected, "invalid journal identity", false)
	}
	dir, err := filepath.EvalSymlinks(filepath.Dir(options.Path))
	if err != nil {
		return nil, failure(upgradetx.CodeDeferred, "journal directory unavailable", false)
	}
	options.Path = filepath.Join(dir, filepath.Base(options.Path))
	guardDir, err := filepath.EvalSymlinks(filepath.Dir(options.GuardPath))
	if err != nil {
		return nil, failure(upgradetx.CodeDeferred, "domain control directory unavailable", false)
	}
	options.GuardPath = filepath.Join(guardDir, filepath.Base(options.GuardPath))
	if options.GuardPath == options.Path {
		return nil, failure(upgradetx.CodeRejected, "journal cannot be its own guard", false)
	}
	s := &Store{options: options, now: time.Now}
	err = s.locked(func(d *diskFile, e *diskEntry) error { return nil })
	if err != nil {
		return nil, err
	}
	return s, nil
}

// locked uses a different inode from the execution lease and the replaceable
// journal. Flock works on Linux and Darwin; no Linux-only handoff is done here.
// The guard file must never be unlinked, including after an unsuccessful write.
func (s *Store) locked(fn func(*diskFile, *diskEntry) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.options.GuardPath
	if err := regularPath(path); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return failure(upgradetx.CodeDeferred, "journal guard unavailable", false)
	}
	defer func() { _ = f.Close() }()
	for {
		err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX)
		if !errors.Is(err, syscall.EINTR) {
			break
		}
	}
	if err != nil {
		return failure(upgradetx.CodeDeferred, "journal guard unavailable", false)
	}
	defer func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }()
	guardInfo, statErr := f.Stat()
	journalInfo, journalErr := os.Stat(s.options.Path)
	if statErr != nil || journalErr == nil && os.SameFile(guardInfo, journalInfo) {
		return corrupt()
	}
	d, err := s.read()
	if err != nil {
		return err
	}
	for i := range d.Entries {
		if d.Entries[i].Key == s.options.Key {
			return fn(&d, &d.Entries[i])
		}
	}
	// A shared agent marker has independent entries for SID/NID/config/UID.
	// Broker and readonly stores are bound to one key and reject a wrong path.
	if len(d.Entries) != 0 && filepath.Base(s.options.Path) != ".tether-upgrade.json" {
		return corrupt()
	}
	d.Entries = append(d.Entries, diskEntry{Key: s.options.Key})
	return fn(&d, &d.Entries[len(d.Entries)-1])
}

func (s *Store) Prepare(key upgradetx.Key, req upgradetx.PrepareRequest) (out upgradetx.Grant, err error) {
	if key != s.options.Key {
		return out, failure(upgradetx.CodeRejected, "component key mismatch", false)
	}
	err = s.locked(func(d *diskFile, e *diskEntry) error {
		var err error
		out, err = s.prepare(d, e, req, nil)
		return err
	})
	if err != nil {
		out = upgradetx.Grant{}
	}
	return
}

func (s *Store) prepare(d *diskFile, e *diskEntry, req upgradetx.PrepareRequest, reverse *reverseBinding) (upgradetx.Grant, error) {
	if !validRequest(req, e.Key) {
		return upgradetx.Grant{}, failure(upgradetx.CodeRejected, "invalid prepare request", false)
	}
	scope, digest, err := canonicalScope(req.Scope)
	if err != nil {
		return upgradetx.Grant{}, err
	}
	req.Scope = scope
	// Nonce conflicts are not permission to mint a second execution ID, even
	// after acceptance or expiry. GC only removes grants with no execution.
	for _, p := range e.Grants {
		if p.Grant.ClientNonce == req.ClientNonce {
			if !matchesRequest(p.Grant, req, digest) || !reflect.DeepEqual(p.Reverse, reverse) {
				return upgradetx.Grant{}, failure(upgradetx.CodeRejected, "prepare nonce conflict", false)
			}
			if err := s.liveGrant(p.Grant); err != nil {
				return upgradetx.Grant{}, err
			}
			return p.Grant, nil
		}
	}
	for _, g := range e.AcceptedGrants {
		if g.ClientNonce == req.ClientNonce {
			if !matchesRequest(g, req, digest) {
				return upgradetx.Grant{}, failure(upgradetx.CodeRejected, "prepare nonce conflict", true)
			}
			return g, nil
		}
	}
	deadline, err := s.deadline(req)
	if err != nil {
		return upgradetx.Grant{}, err
	}
	collect(e, s.now().UnixNano())
	if len(e.Grants) >= maxGrants {
		return upgradetx.Grant{}, failure(upgradetx.CodeDeferred, "prepared grant capacity exhausted", false)
	}
	if e.Revision == math.MaxUint64 {
		return upgradetx.Grant{}, failure(upgradetx.CodeDeferred, "journal sequence exhausted", false)
	}
	tx, err := randomID()
	if err != nil {
		return upgradetx.Grant{}, err
	}
	// An automatic reverse belongs to the unresolved transaction. A manual
	// reverse of a completed execution gets a new transaction and leaves the
	// archived parent facts unchanged.
	if reverse != nil && !settled(*e.Current) {
		tx = e.Current.Grant.Handle.TxID
	}
	cut, err := randomID()
	if err != nil {
		return upgradetx.Grant{}, err
	}
	generation := uint64(2)
	// origin: S8 internal review rv-txn F10
	// A fresh runtime starts at generation one independently of business rows.
	// Persisted high water survives history GC; runtime imports may supply a
	// higher source generation even when there are no resources or transactions.
	if s.options.SourceGeneration > 0 {
		generation = s.options.SourceGeneration + 1
	}
	if e.Generation >= generation {
		generation = e.Generation + 1
	}
	if generation < 2 {
		return upgradetx.Grant{}, failure(upgradetx.CodeDeferred, "generation exhausted", false)
	}
	for _, record := range records(e) {
		if next := attempt(record).Handle.Generation + 1; next > generation || next == 0 {
			generation = next
		}
		if generation == 0 {
			return upgradetx.Grant{}, failure(upgradetx.CodeDeferred, "generation exhausted", false)
		}
	}
	grant := upgradetx.Grant{
		Handle:     upgradetx.Handle{Key: e.Key, TxID: tx, CutID: cut, Generation: generation, Revision: e.Revision + 1},
		ParentTxID: req.ParentTxID, ParentSlot: req.ParentSlot, ClientNonce: req.ClientNonce,
		Issuer: s.options.Owner, ActorFP: req.ActorFP, Mode: req.Mode,
		SourceRelease: req.SourceRelease, TargetRelease: req.TargetRelease,
		SourceSHA: req.SourceSHA, TargetSHA: req.TargetSHA, ScopeDigest: digest,
		CapabilityDigest: req.CapabilityDigest, ExpiresAt: deadline,
	}
	e.Grants = append(e.Grants, preparedGrant{Grant: grant, Scope: scope, Reverse: reverse})
	e.Revision++
	collect(e, s.now().UnixNano())
	if err := s.write(d, false); err != nil {
		return upgradetx.Grant{}, err
	}
	return grant, nil
}

func (s *Store) deadline(req upgradetx.PrepareRequest) (int64, error) {
	now := s.now()
	limit := now.Add(upgradebudget.ComponentHandover).UnixNano()
	// Logical parent/host journals authorize a fixed fleet plan. Their
	// initial absolute window is the frozen 900s roll budget; physical
	// standalone component grants retain the 120s handover limit.
	// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1
	if upgradetx.OrchestrationKey(s.options.Key) {
		limit = now.Add(upgradebudget.FullStackRollout).UnixNano()
	}
	if req.ParentTxID != "" {
		if s.options.ParentReservation == nil {
			return 0, failure(upgradetx.CodeDeferred, "parent reservation unavailable", false)
		}
		var err error
		limit, err = s.options.ParentReservation(req.ParentTxID, req.ParentSlot)
		if err != nil || limit > now.Add(upgradebudget.FullStackRollout).UnixNano() {
			return 0, failure(upgradetx.CodeDeferred, "parent reservation invalid", false)
		}
	}
	expires := req.ExpiresAt
	if expires == 0 {
		expires = limit
	}
	if expires <= now.UnixNano() {
		return 0, failure(upgradetx.CodeGrantExpired, "prepare deadline expired", false)
	}
	if expires > limit {
		return 0, failure(upgradetx.CodeRejected, "prepare exceeds absolute deadline", false)
	}
	return expires, nil
}

func (s *Store) liveGrant(g upgradetx.Grant) error {
	if g.ExpiresAt <= s.now().UnixNano() {
		return failure(upgradetx.CodeGrantExpired, "grant expired", false)
	}
	if g.Issuer != s.options.Owner {
		return failure(upgradetx.CodeGrantUnknown, "grant issuer changed", false)
	}
	if g.ParentTxID != "" {
		_, err := s.deadline(upgradetx.PrepareRequest{ParentTxID: g.ParentTxID, ParentSlot: g.ParentSlot, ExpiresAt: g.ExpiresAt})
		return err
	}
	return nil
}

func (s *Store) Start(grant upgradetx.Grant, fence upgradetx.Fence) (out upgradetx.Record, err error) {
	err = s.locked(func(d *diskFile, e *diskEntry) error {
		// Accepted retries are queries, including after a boot or deadline.
		for _, accepted := range e.AcceptedGrants {
			if accepted == grant {
				for _, r := range records(e) {
					if r.Grant.Handle.TxID == grant.Handle.TxID {
						out = r
						return nil
					}
				}
			}
		}
		idx := -1
		for i, p := range e.Grants {
			if reflect.DeepEqual(p.Grant, grant) {
				idx = i
				break
			}
		}
		// origin: hot-update S8 plan §3.1 / M10
		if idx < 0 {
			return failure(upgradetx.CodeGrantUnknown, "grant is not recorded", false)
		}
		p := e.Grants[idx]
		pins, err := s.publicationPins(p)
		if err != nil {
			return err
		}
		if p.Reverse != nil && e.Current != nil {
			pins = append([]string(nil), metadata(e, e.Current.Grant.Handle.TxID).Pins...)
		}
		continuing := p.Reverse != nil && e.Current != nil && grant.Handle.TxID == e.Current.Grant.Handle.TxID
		if err := s.liveGrant(grant); err != nil {
			return err
		}
		if len(e.Metadata) >= maxItems && !continuing || len(e.AcceptedGrants) >= maxItems {
			return failure(upgradetx.CodeDeferred, "journal history capacity exhausted; pending history retained", false)
		}
		if fence.Owner != grant.Issuer || fence.RunningSHA != grant.SourceSHA || fence.Current.Key != (upgradetx.Key{}) && fence.Current.Key != e.Key {
			return failure(upgradetx.CodeRejected, "source proof mismatch", false)
		}
		if p.Reverse != nil && fence != p.Reverse.Fence {
			return failure(upgradetx.CodeRejected, "reverse fence changed", false)
		}
		if e.Current == nil {
			if fence.Current != (upgradetx.Handle{}) {
				return failure(upgradetx.CodeRejected, "current transaction changed", false)
			}
		} else {
			current := *e.Current
			if fence.Current != attempt(current).Handle || fence.PreviousSHA != current.RecordedPreviousSHA {
				return failure(upgradetx.CodeRejected, "current transaction changed", false)
			}
			if grant.Handle.Generation != attempt(current).Handle.Generation+1 {
				return failure(upgradetx.CodeRejected, "grant generation is stale", false)
			}
			if p.Reverse == nil && !settled(current) {
				return failure(upgradetx.CodeUnsettled, "current transaction is unresolved", true)
			}
			if p.Reverse == nil && fence.RunningSHA != current.EffectiveSHA {
				return failure(upgradetx.CodeRejected, "running source changed", false)
			}
			// Close the old window in its own durable write before replacing
			// Current. A crash here only closes a window, never starts work.
			if p.Reverse == nil || settled(current) {
				if _, err := s.closeWindow(d, e, fence.Current); err != nil {
					return err
				}
			}
			if !continuing {
				e.Archives = append(e.Archives, current)
				m := metadata(e, current.Grant.Handle.TxID)
				m.ArchivedAt = s.now().UnixNano()
				m.Pins = nil
			}
		}
		a := upgradetx.Attempt{Handle: grant.Handle, Direction: upgradetx.DirectionForward,
			SourceSHA: grant.SourceSHA, TargetSHA: grant.TargetSHA, SourceGeneration: grant.Handle.Generation - 1,
			Phase: upgradetx.PhasePrepared, CutStage: upgradetx.CutNone, Outcome: upgradetx.OutcomePending, Scope: p.Scope}
		// origin: S8 internal review rv-fault F4
		// A cancelled candidate consumed an allocation, not source ownership.
		if e.Current != nil && p.Reverse == nil && upgradetx.CancellationSettled(*e.Current) {
			a.SourceGeneration = attempt(*e.Current).SourceGeneration
		}
		out = upgradetx.Record{Version: upgradetx.RecordVersion, Key: e.Key, Revision: grant.Handle.Revision,
			Grant: grant, Forward: a, EffectiveSHA: grant.SourceSHA, RecordedPreviousSHA: grant.SourceSHA}
		if p.Reverse != nil {
			a.Direction = upgradetx.DirectionReverse
			out.Forward = upgradetx.Attempt{}
			out.Reverse = a
			out.RecordedPreviousSHA = grant.TargetSHA
			if continuing {
				out.Grant, out.Actions = e.Current.Grant, e.Current.Actions
				out.Forward = e.Current.Forward
				out.Forward.Phase, out.Forward.Outcome = upgradetx.PhaseRollbackPending, upgradetx.OutcomePending
			}
		}
		e.Current = &out
		e.Generation = grant.Handle.Generation
		remaining := e.Grants[:0]
		for _, pending := range e.Grants {
			if pending.Grant.Handle.TxID != grant.Handle.TxID {
				remaining = append(remaining, pending)
			}
		}
		e.Grants = remaining
		e.AcceptedGrants = append(e.AcceptedGrants, grant)
		if !continuing {
			e.Metadata = append(e.Metadata, upgradetx.Metadata{TxID: grant.Handle.TxID, Window: upgradetx.WindowOpen, Pins: pins})
		}
		if err := bump(e); err != nil {
			return err
		}
		collect(e, s.now().UnixNano())
		return s.write(d, true)
	})
	if err != nil {
		out = upgradetx.Record{}
	}
	return
}

func (s *Store) Load(key upgradetx.Key, txID string) (out upgradetx.View, err error) {
	if key != s.options.Key {
		return out, failure(upgradetx.CodeRejected, "component key mismatch", false)
	}
	err = s.locked(func(_ *diskFile, e *diskEntry) error {
		for _, r := range records(e) {
			if r.Grant.Handle.TxID == txID || txID == "" && e.Current != nil && r.Grant.Handle.TxID == e.Current.Grant.Handle.TxID {
				out = upgradetx.View{Historical: r, Metadata: *metadata(e, r.Grant.Handle.TxID)}
				// A disk read is never a current running/custody observation.
				return nil
			}
		}
		return failure(upgradetx.CodeGrantUnknown, "transaction is not recorded", false)
	})
	return
}

func (s *Store) Apply(expected upgradetx.Handle, event upgradetx.Event) (out upgradetx.Record, err error) {
	err = s.locked(func(d *diskFile, e *diskEntry) error {
		if err := currentCAS(e, expected); err != nil {
			return err
		}
		if settled(*e.Current) {
			return failure(upgradetx.CodeRejected, "terminal execution is immutable", true)
		}
		var err error
		if s.options.Transition == nil {
			// Action persistence still uses the same validator as atomic phase writes.
			if event.Action.ID == "" || event.Phase != "" || event.CutStage != "" || event.Receipt.Kind != "" {
				return failure(upgradetx.CodeNotWired, "transition adapter unavailable", true)
			}
			out, err = upgradetx.Transition(*e.Current, event)
		} else {
			out, err = s.options.Transition(*e.Current, event)
		}
		if err != nil {
			if event.Action.ID != "" {
				return failure(upgradetx.CodeRejected, "invalid action event", true)
			}
			return err
		}
		// The pure predicate cannot rewrite the execution identity or history.
		if out.Key != e.Key || out.Grant != e.Current.Grant || out.RecordedPreviousSHA != e.Current.RecordedPreviousSHA || !sameAttempt(attempt(out).Handle, expected) {
			return failure(upgradetx.CodeRejected, "transition changed transaction identity", true)
		}
		if e.Current.Revision == math.MaxUint64 {
			return failure(upgradetx.CodeUnsettled, "record sequence exhausted", true)
		}
		out.Revision = e.Current.Revision + 1
		setRevision(&out, out.Revision)
		e.Current = &out
		if err := bump(e); err != nil {
			return err
		}
		return s.write(d, true)
	})
	if err != nil {
		out = upgradetx.Record{}
	}
	return
}

func currentCAS(e *diskEntry, expected upgradetx.Handle) error {
	if e.Current == nil || attempt(*e.Current).Handle != expected || e.Current.Revision != expected.Revision {
		return failure(upgradetx.CodeRejected, "stale transaction handle", false)
	}
	return nil
}

func sameAttempt(a, b upgradetx.Handle) bool {
	a.Revision, b.Revision = 0, 0
	return a == b
}

func setRevision(r *upgradetx.Record, revision uint64) {
	if r.Reverse.Handle.TxID != "" {
		r.Reverse.Handle.Revision = revision
	} else {
		r.Forward.Handle.Revision = revision
	}
}

func attempt(r upgradetx.Record) upgradetx.Attempt {
	if r.Reverse.Handle.TxID != "" {
		return r.Reverse
	}
	return r.Forward
}

func records(e *diskEntry) []upgradetx.Record {
	out := append([]upgradetx.Record(nil), e.Archives...)
	if e.Current != nil {
		out = append(out, *e.Current)
	}
	return out
}

func metadata(e *diskEntry, tx string) *upgradetx.Metadata {
	for i := range e.Metadata {
		if e.Metadata[i].TxID == tx {
			return &e.Metadata[i]
		}
	}
	return nil // The strict decoder guarantees one metadata row per execution.
}

func bump(e *diskEntry) error {
	if e.Revision == math.MaxUint64 {
		return failure(upgradetx.CodeUnsettled, "journal sequence exhausted", true)
	}
	e.Revision++
	return nil
}

func randomID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", failure(upgradetx.CodeDeferred, "grant entropy unavailable", false)
	}
	return hex.EncodeToString(b[:]), nil
}

func failure(code, detail string, accepted bool) error {
	return &upgradetx.Error{Code: code, Detail: detail, Accepted: accepted}
}
func corrupt() error { return failure(upgradetx.CodeStateCorrupt, "journal validation failed", false) }
