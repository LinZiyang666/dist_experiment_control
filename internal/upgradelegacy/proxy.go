package upgradelegacy

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"reflect"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// Journal is an extension port of the selected issuer's sole durable store.
// Load never creates a grant. It returns the accepted record and its legacy
// proof, or an error for an unknown/archived transaction. Only a known accepted
// record with no dispatch action may have a zero proof.
//
// Commit atomically CASes the original handle/revision, action and proof in that
// same journal. Success requires file Sync, rename and directory Sync. It must
// reject a second intent, a changed grant, a closed reservation and a late
// callback for another Current. A failed write may have landed; callers query.
// P02/P15 supply the adapter; P12 never opens an alternate journal.
// origin: hot-update S8 plan §3.1 / §3.2 / §7.2 / P12
type Journal interface {
	Load(context.Context, upgradetx.Key, string) (upgradetx.Record, upgradetx.LegacyUpgradeProof, error)
	Commit(context.Context, upgradetx.Handle, upgradetx.Action, upgradetx.LegacyUpgradeProof) (upgradetx.Record, error)
}

// Consumer names an actual member of the old upgrade queue, including
// non-voters. The capability sample must cover delivery, not just a probe peer.
type Consumer struct {
	Owner   upgradetx.Owner
	Capable bool
}

// Authorization is control-plane evidence, not a wire/disk DTO. Complete means
// the entire delivery consumer set is known; Reserved means the original
// parent owns an exclusive, durable full-fleet reservation until settlement.
// The policy adapter must validate the actor and reservation in their real
// serialization boundaries, and cannot infer either from cached role state.
type Authorization struct {
	Grant          upgradetx.Grant
	TargetInstance string
	Consumers      []Consumer
	Complete       bool
	Reserved       bool
}

// Policy bridges P06's compatibility/fleet checks without depending on a
// proposed S7 API. Authorize is read-only and binds the selected issuer route.
type Policy interface {
	Authorize(context.Context, upgradetx.Grant, string) (Authorization, error)
}

// Transport always targets proof.Issuer; neither operation uses the upgrade
// queue or chooses a replacement issuer. Query cannot forward an upgrade.
// The adapter resolves the original validated supplier for CandidateSHA and
// verifies any claimed Causal evidence against the original dispatch, rather
// than setting it from an equal SHA, registration or observation timestamp.
type Transport interface {
	Dispatch(context.Context, upgradetx.LegacyUpgradeProof, string) (upgradetx.LegacyUpgradeProof, error)
	Query(context.Context, upgradetx.LegacyUpgradeProof, string) (upgradetx.LegacyUpgradeProof, error)
}

// Proxy has no background loop or process/FD ownership. Its ports retain their
// own lifetime, and every control-plane operation uses the caller's context.
type Proxy struct {
	journal   Journal
	policy    Policy
	transport Transport
}

func New(journal Journal, policy Policy, transport Transport) *Proxy {
	return &Proxy{journal: journal, policy: policy, transport: transport}
}

// Preflight is used before Start; Dispatch repeats it before the first send.
// Default/hot mode, mixed or unknown consumers and an unreserved fleet never
// authorize first migration. Ordinary legacy traffic does not call this port.
func (proxy *Proxy) Preflight(ctx context.Context, grant upgradetx.Grant, targetInstance string) error {
	if err := proxy.configured(); err != nil {
		return err
	}
	if err := validateGrant(grant, targetInstance); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "legacy preflight cancelled"}
	}
	now := time.Now().UnixNano()
	if grant.ExpiresAt <= now {
		return &upgradetx.Error{Code: upgradetx.CodeGrantExpired, Detail: "legacy grant expired"}
	}
	// origin: hot-update S8 plan §3.1 / §12.2 / P12
	// The parent ceiling shares the frozen 15-minute rollout bound; a retry
	// cannot create another grant window by resetting a local duration.
	if grant.ExpiresAt-now > int64(upgradebudget.FullStackRollout) {
		return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "legacy parent deadline is invalid"}
	}
	authorization, err := proxy.policy.Authorize(ctx, grant, targetInstance)
	if err != nil {
		return err
	}
	return validateAuthorization(grant, targetInstance, authorization)
}

// Dispatch operates only on an already accepted, issuer-local grant. Even a
// causal legacy observation cannot prove S8 completion: its return is always
// upgrade_unsettled/Accepted=true once an intent exists. Query can read that
// fact successfully (CLI query rc0); wait must retain non_hot/unknown (rc74).
// origin: hot-update S8 plan §3.2 / §7.1 / §7.2 / P12
func (proxy *Proxy) Dispatch(ctx context.Context, grant upgradetx.Grant, targetInstance string) (upgradetx.LegacyUpgradeProof, error) {
	record, proof, action, err := proxy.load(ctx, grant, targetInstance)
	if err != nil {
		return proof, err
	}
	if action.ID != "" {
		// origin: hot-update S8 plan §3.2 / P12
		// Intent is already a may-have-sent boundary. Lost replies, issuer
		// restarts and even an intent-only record permit Query, never resend.
		nonce, err := randomID()
		if err != nil {
			return unknown(proof), unsettled("legacy query nonce unavailable")
		}
		observed, err := proxy.Query(ctx, grant, targetInstance, nonce)
		if err != nil {
			return observed, err
		}
		return observed, unsettled("legacy_interrupting")
	}
	if err := proxy.Preflight(ctx, grant, targetInstance); err != nil {
		return proof, unsettled("legacy dispatch preflight refused")
	}
	if record.Forward.Outcome != upgradetx.OutcomePending ||
		(record.Forward.Phase != upgradetx.PhasePrepared && record.Forward.Phase != upgradetx.PhaseReady) {
		return proof, unsettled("legacy transaction cannot dispatch")
	}
	actionID, err := randomID()
	if err != nil {
		return proof, unsettled("legacy dispatch identity unavailable")
	}
	nonce, err := randomID()
	if err != nil {
		return proof, unsettled("legacy dispatch nonce unavailable")
	}
	proof = upgradetx.LegacyUpgradeProof{
		ParentTxID: grant.ParentTxID, Issuer: grant.Issuer, GrantNonce: grant.ClientNonce,
		TargetInstance: targetInstance, TargetKey: grant.Handle.Key,
		SourceSHA: grant.SourceSHA, CandidateSHA: grant.TargetSHA,
		DispatchActionID: actionID, State: "attempted",
	}
	digest, err := proofDigest(proof)
	if err != nil {
		return proof, unsettled("legacy intent binding is invalid")
	}
	action = upgradetx.Action{ID: actionID, Handle: record.Forward.Handle, Kind: upgradetx.ActionDispatch,
		IntentDigest: digest, State: upgradetx.ActionIntent, Nonce: nonce}
	if err := ctx.Err(); err != nil {
		return proof, unsettled("legacy dispatch cancelled")
	}
	// origin: hot-update S8 plan §3.2 / P12
	// CAS is the only send entitlement. A write error (including lost fsync
	// receipt) cannot be converted into a second attempt or fake success.
	updated, err := proxy.journal.Commit(ctx, record.Forward.Handle, action, proof)
	if err != nil {
		return unknown(proof), unsettled("legacy intent durability is unknown")
	}
	if err := validateCommit(record, updated, action); err != nil {
		return unknown(proof), err
	}
	if err := ctx.Err(); err != nil {
		return unknown(proof), unsettled("legacy dispatch cancelled after intent")
	}
	observed, err := proxy.transport.Dispatch(ctx, proof, nonce)
	if err != nil {
		return unknown(proof), unsettled("legacy dispatch result is unknown")
	}
	observed, err = proxy.observe(ctx, updated, proof, action, observed, nonce)
	if err != nil {
		return observed, err
	}
	return observed, unsettled("legacy_interrupting")
}

// Query follows only the original issuer, including after expiry or restart.
// It never calls policy, prepares a grant, starts a transaction or dispatches.
// Successful reads of an uncorrelated observation intentionally return nil.
func (proxy *Proxy) Query(ctx context.Context, grant upgradetx.Grant, targetInstance, nonce string) (upgradetx.LegacyUpgradeProof, error) {
	if !validHex(nonce, 32) {
		return upgradetx.LegacyUpgradeProof{}, &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "legacy query nonce is invalid"}
	}
	record, proof, action, err := proxy.load(ctx, grant, targetInstance)
	if err != nil {
		return proof, err
	}
	if action.ID == "" {
		return proof, unsettled("legacy dispatch has not been attempted")
	}
	if err := ctx.Err(); err != nil {
		return unknown(proof), unsettled("legacy query cancelled")
	}
	observed, err := proxy.transport.Query(ctx, proof, nonce)
	if err != nil {
		return unknown(proof), unsettled("legacy issuer observation is unavailable")
	}
	return proxy.observe(ctx, record, proof, action, observed, nonce)
}

func (proxy *Proxy) observe(ctx context.Context, record upgradetx.Record, saved upgradetx.LegacyUpgradeProof,
	action upgradetx.Action, observed upgradetx.LegacyUpgradeProof, nonce string) (upgradetx.LegacyUpgradeProof, error) {
	if err := validateObservation(saved, observed, nonce); err != nil {
		return unknown(saved), unsettled("legacy observation binding is invalid")
	}
	digest, err := proofDigest(observed)
	if err != nil {
		return unknown(saved), unsettled("legacy observation cannot be encoded")
	}
	action.State, action.ResultDigest = upgradetx.ActionObserved, digest
	if observed.Causal {
		action.State = upgradetx.ActionDurable
	}
	updated, err := proxy.journal.Commit(ctx, record.Forward.Handle, action, observed)
	if err != nil {
		return unknown(saved), unsettled("legacy observation durability is unknown")
	}
	if err := validateCommit(record, updated, action); err != nil {
		return unknown(saved), err
	}
	return observed, nil
}

func (proxy *Proxy) load(ctx context.Context, grant upgradetx.Grant, targetInstance string) (upgradetx.Record, upgradetx.LegacyUpgradeProof, upgradetx.Action, error) {
	var record upgradetx.Record
	var proof upgradetx.LegacyUpgradeProof
	var action upgradetx.Action
	if err := proxy.configured(); err != nil {
		return record, proof, action, err
	}
	if err := validateGrant(grant, targetInstance); err != nil {
		return record, proof, action, err
	}
	record, proof, err := proxy.journal.Load(ctx, grant.Handle.Key, grant.Handle.TxID)
	if err != nil {
		// Only an authoritative unknown-grant or unwired refusal establishes
		// that nothing was accepted. IO/cancellation/corruption cannot invite
		// a fresh transaction, nor expose a storage/transport error payload.
		var classified *upgradetx.Error
		if errors.As(err, &classified) && !classified.Accepted &&
			(classified.Code == upgradetx.CodeGrantUnknown || classified.Code == upgradetx.CodeNotWired) {
			return upgradetx.Record{}, upgradetx.LegacyUpgradeProof{}, action, err
		}
		return upgradetx.Record{}, upgradetx.LegacyUpgradeProof{}, action, unsettled("legacy journal observation is unavailable")
	}
	if err := validateRecord(record, grant); err != nil {
		return upgradetx.Record{}, upgradetx.LegacyUpgradeProof{}, action, err
	}
	for _, candidate := range record.Actions {
		if candidate.Kind == upgradetx.ActionDispatch {
			if action.ID != "" {
				return record, upgradetx.LegacyUpgradeProof{}, action, unsettled("legacy journal has multiple dispatches")
			}
			action = candidate
		}
	}
	if action.ID == "" {
		if proof != (upgradetx.LegacyUpgradeProof{}) {
			return record, upgradetx.LegacyUpgradeProof{}, action, unsettled("legacy proof has no dispatch intent")
		}
		return record, proof, action, nil
	}
	if err := validateProof(proof); err != nil {
		return record, upgradetx.LegacyUpgradeProof{}, action, unsettled("legacy journal proof is invalid")
	}
	want := intentProof(proof)
	want.ParentTxID, want.Issuer, want.GrantNonce = grant.ParentTxID, grant.Issuer, grant.ClientNonce
	want.TargetKey, want.TargetInstance = grant.Handle.Key, targetInstance
	want.SourceSHA, want.CandidateSHA = grant.SourceSHA, grant.TargetSHA
	digest, err := proofDigest(want)
	if err != nil || want != intentProof(proof) || action.ID != proof.DispatchActionID ||
		action.IntentDigest != digest || !validHex(action.Nonce, 32) ||
		!sameAttempt(action.Handle, record.Forward.Handle) || action.Handle.Revision > record.Revision {
		return record, upgradetx.LegacyUpgradeProof{}, action, unsettled("legacy journal action binding is invalid")
	}
	switch action.State {
	case upgradetx.ActionIntent:
		if proof.State != "attempted" || action.ResultDigest != "" {
			return record, upgradetx.LegacyUpgradeProof{}, action, unsettled("legacy intent has a result")
		}
	case upgradetx.ActionObserved, upgradetx.ActionDurable:
		result, err := proofDigest(proof)
		if err != nil || action.ResultDigest != result || proof.State == "attempted" ||
			(action.State == upgradetx.ActionDurable) != proof.Causal {
			return record, upgradetx.LegacyUpgradeProof{}, action, unsettled("legacy result binding is invalid")
		}
	default:
		return record, upgradetx.LegacyUpgradeProof{}, action, unsettled("legacy action state is invalid")
	}
	return record, proof, action, nil
}

// origin: hot-update S8 plan §2.1 / §7.2 / P12
// Zero/malformed identities and implicit hot mode are refusals, never legacy
// permission. The full accepted grant is compared again after journal Load.
func validateGrant(grant upgradetx.Grant, targetInstance string) error {
	if grant.Mode != upgradetx.ModeLegacyInterrupting || !validTarget(grant.Handle.Key) ||
		!validHex(grant.Handle.TxID, 32) || !validHex(grant.Handle.CutID, 32) ||
		grant.Handle.Generation == 0 || grant.Handle.Revision == 0 ||
		!validHex(grant.ParentTxID, 32) || !validText(grant.ParentSlot, 128) ||
		!validHex(grant.ClientNonce, 32) || !validOwner(grant.Issuer) ||
		!validText(grant.ActorFP, 256) || !validText(targetInstance, 128) ||
		!validText(grant.SourceRelease, 128) || !validText(grant.TargetRelease, 128) ||
		!validHex(grant.SourceSHA, 64) || !validHex(grant.TargetSHA, 64) || grant.SourceSHA == grant.TargetSHA ||
		!validHex(grant.ScopeDigest, 64) || !validHex(grant.CapabilityDigest, 64) || grant.ExpiresAt <= 0 {
		return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "legacy interrupting grant is invalid"}
	}
	return nil
}

// origin: hot-update S8 plan §7.2 / P12
// A successful probe of one broker cannot constrain the next queue consumer.
// The synchronous policy port must name and reserve the full delivery set.
func validateAuthorization(grant upgradetx.Grant, target string, authorization Authorization) error {
	if authorization.Grant != grant || authorization.TargetInstance != target {
		return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "legacy issuer route is not bound"}
	}
	if !authorization.Complete || len(authorization.Consumers) == 0 || len(authorization.Consumers) > 4096 {
		return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "legacy upgrade consumers are unknown"}
	}
	seen := make(map[upgradetx.Owner]bool)
	for _, consumer := range authorization.Consumers {
		if !validOwner(consumer.Owner) || !consumer.Capable || seen[consumer.Owner] {
			return &upgradetx.Error{Code: upgradetx.CodeRejected, Detail: "legacy upgrade consumers are mixed"}
		}
		seen[consumer.Owner] = true
	}
	if !seen[grant.Issuer] || !authorization.Reserved {
		return &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "legacy fleet reservation is unavailable"}
	}
	return nil
}

// origin: hot-update S8 plan §3.1 / §3.3 / P12
// Neither an old actor's callback nor an equal target SHA may claim Current.
func validateRecord(record upgradetx.Record, grant upgradetx.Grant) error {
	if record.Grant != grant || record.Key != grant.Handle.Key || record.Version != upgradetx.RecordVersion ||
		!sameAttempt(record.Forward.Handle, grant.Handle) || record.Revision < grant.Handle.Revision ||
		record.Forward.Handle.Revision != record.Revision || record.Forward.Direction != upgradetx.DirectionForward ||
		record.Forward.SourceSHA != grant.SourceSHA || record.Forward.TargetSHA != grant.TargetSHA {
		return unsettled("legacy accepted record does not match the grant")
	}
	return nil
}

func validateCommit(before, after upgradetx.Record, action upgradetx.Action) error {
	if err := validateRecord(after, before.Grant); err != nil {
		return err
	}
	if after.Revision <= before.Revision {
		return unsettled("legacy journal revision did not advance")
	}
	// origin: hot-update S8 plan §7.1 / P12
	// Saving an old-protocol observation cannot change execution history or
	// fabricate hot completion. CAS excludes concurrent record transitions.
	unchanged := after
	unchanged.Revision, unchanged.Forward.Handle.Revision = before.Revision, before.Forward.Handle.Revision
	unchanged.Actions = before.Actions
	if !reflect.DeepEqual(unchanged, before) {
		return unsettled("legacy proof write changed execution history")
	}
	count := 0
	for _, saved := range after.Actions {
		if saved.Kind == upgradetx.ActionDispatch {
			if saved != action {
				return unsettled("legacy journal did not retain the action")
			}
			count++
		}
	}
	if count != 1 {
		return unsettled("legacy journal did not retain a single dispatch")
	}
	return nil
}

func sameAttempt(first, second upgradetx.Handle) bool {
	first.Revision, second.Revision = 0, 0
	return first == second
}

func (proxy *Proxy) configured() error {
	if proxy == nil || proxy.journal == nil || proxy.policy == nil || proxy.transport == nil {
		return &upgradetx.Error{Code: upgradetx.CodeNotWired, Detail: "legacy proxy ports are missing"}
	}
	return nil
}

func randomID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}

func unknown(proof upgradetx.LegacyUpgradeProof) upgradetx.LegacyUpgradeProof {
	// Preserve recorded causal facts when only the fresh observer is unavailable.
	if !proof.Causal {
		proof.State = "unknown"
	}
	return proof
}

func unsettled(detail string) error {
	return &upgradetx.Error{Code: upgradetx.CodeUnsettled, Detail: detail, Accepted: true}
}
