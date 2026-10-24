// Package upgradehost owns durable parent bindings and shared domain custody.
// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §5.1
package upgradehost

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"

	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradestore"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// RollKey explicitly separates orchestration bookkeeping from the broker's
// one Current image cut. Both journals use the same registered short guard;
// neither the ctl nor an executing SHA selects a journal pathname.
// origin: hot-update S8 plan §3.1 / §5.1; S8 internal review cluster-roll F4
func RollKey(key upgradetx.Key) upgradetx.Key { key.ConfiguredNID += "@roll"; return key }
func hostKey(key upgradetx.Key) upgradetx.Key { key.ConfiguredNID += "@host"; return key }

func RollJournal(key upgradetx.Key, owner upgradetx.Owner, host bool, route ...string) (*upgradestore.Store, error) {
	reg, err := upgradelock.RegistrationFor(key)
	if err != nil {
		return nil, err
	}
	logical := RollKey(key)
	if !host && len(route) > 0 {
		logical.ConfiguredNID = route[0] + "@roll"
	}
	if host {
		logical = hostKey(key)
	}
	return upgradestore.Open(upgradestore.Options{Path: filepath.Join(reg.ControlDir, digest(logical)[:32]+"-state.json"), GuardPath: filepath.Join(reg.ControlDir, key.DomainID+".guard"), Key: logical, Owner: owner, Transition: upgradetx.Transition,
		ParentReservation: func(parent, slot string) (int64, error) { return ParentDeadline(key, owner, parent, slot) }})
}

// The opaque slot commits to the entire target-issued parent, not just its ID
// or a replaceable pathname. It is shared by siblings in this registered domain.
func RollSlot(parent upgradetx.Grant, key upgradetx.Key, route ...string) string {
	prefix := key.ConfiguredNID
	if len(route) > 0 {
		prefix = route[0]
	}
	return prefix + ":" + digest(struct {
		Parent upgradetx.Grant
		Domain string
	}{parent, key.DomainID})
}

// ParentDeadline is the store's boot-bound parent resolver. Its source is a
// daemon-owned accepted host binding; a signed request cannot extend it or
// revive a released binding. Start also checks the live replicated holder.
func ParentDeadline(key upgradetx.Key, owner upgradetx.Owner, parent, slot string) (int64, error) {
	reg, err := upgradelock.RegistrationFor(key)
	if err != nil {
		return 0, err
	}
	logical := hostKey(key)
	path := filepath.Join(reg.ControlDir, digest(logical)[:32]+"-state.json")
	return upgradestore.ReadParentDeadline(path, logical, parent, digest(slot))
}

// BindRollParent persists an inert host authorization before child Prepare.
// It takes no execution lease and dispatches nothing. The full ordered plan
// must be sealed before ReserveRoll acquires the domain or stages any bytes.
func BindRollParent(key upgradetx.Key, owner upgradetx.Owner, parent upgradetx.Grant, scope upgradetx.Scope, route ...string) error {
	if parent.ScopeDigest != digest(scope) {
		return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "parent scope differs from its grant"}
	}
	s, err := RollJournal(key, owner, true)
	if err != nil {
		return err
	}
	req := upgradetx.PrepareRequest{ClientNonce: parent.Handle.TxID, ActorFP: parent.ActorFP, Mode: upgradetx.ModeHot, SourceRelease: "reservation", TargetRelease: "reserved", SourceSHA: digest("host-idle"), TargetSHA: digest("host-reserved"), Scope: scope, CapabilityDigest: digest(RollSlot(parent, key, route...)), ExpiresAt: parent.ExpiresAt}
	g, err := s.Prepare(hostKey(key), req)
	if err != nil {
		return err
	}
	fence := upgradetx.Fence{Owner: owner, RunningSHA: req.SourceSHA}
	if old, e := s.Load(hostKey(key), ""); e == nil {
		fence.Current = old.Historical.Forward.Handle
		fence.PreviousSHA = old.Historical.RecordedPreviousSHA
	} else {
		var typed *upgradetx.Error
		if !errors.As(e, &typed) || typed.Code != upgradetx.CodeGrantUnknown {
			return e
		}
	}
	_, err = s.Start(g, fence)
	return err
}

var rollLeases = struct {
	sync.Mutex
	held map[string]upgradelock.Lease
}{held: map[string]upgradelock.Lease{}}

func HoldRoll(ctx context.Context, key upgradetx.Key, slot string) error {
	rollLeases.Lock()
	defer rollLeases.Unlock()
	if rollLeases.held[key.DomainID] != nil {
		l, err := upgradelock.Acquire(ctx, key, slot)
		if err == nil {
			err = l.Close()
		}
		return err
	}
	l, err := upgradelock.Acquire(ctx, key, slot)
	if err == nil {
		rollLeases.held[key.DomainID] = l
	}
	return err
}
func ReleaseRoll(key upgradetx.Key) error {
	rollLeases.Lock()
	defer rollLeases.Unlock()
	l := rollLeases.held[key.DomainID]
	if l == nil {
		return nil
	}
	if err := l.Close(); err != nil {
		return err
	}
	delete(rollLeases.held, key.DomainID)
	return nil
}

func digest(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
