// Package xfertrack owns transfer admission and retained settlement responsibility.
package xfertrack

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// The legacy tracker below is moved verbatim. The v1 owner in grant.go has separate durable decisions: a legacy finalized bit cannot authorize
// v1 ACK.
// origin: hot-update S7 plan §5.1 / L13
// transferTrackerMaxEntries caps the in-memory tracker map so a fast attacker spamming push.req / pull.req can't OOM the broker before the
// per-tier watchdog reaps stale entries. At the worst case (1024 entries * ~200 bytes/entry) the cap holds memory under ~200 KiB. New requests
// past the cap are rejected with `too_many_in_flight` so callers get a clean error instead of silent slowdown. Audit shard P11 F2.
//
// batch C: the EXHAUSTION WINDOW changed. This comment used to derive the bound from a "5-min Tier-B timeout"; with a size-derived budget an
// entry can now live up to proto.XferTierBMaxBudget (35m08s), so a client declaring the maximum size and then sending nothing holds a slot
// ~6.8x longer than before. That is accepted rather than defended against: only a session MEMBER can reach this path, and a member already has
// unrestricted `run`/`exec` (per the documented boundary in docs/usage.md), so a member wanting to exhaust a broker has far more direct
// options.
//
// 2026-09-02 prerelease audit (BT-F5) CORRECTS THE PARAGRAPH ABOVE, which twice asserted the byte count was "unchanged" and therefore still
// ~200 KiB. It was not: the per-entry size depended on strings the broker accepted at any length. The bound below is what makes the arithmetic
// true, and it is now enforced rather than assumed.
const transferTrackerMaxEntries = 1024

// transferIDMaxLen / transferPathMaxLen / transferSHA256Len bound the three strings a transfer request hands the broker to KEEP.
//
// origin: prerelease audit broker-transfer/BT-F5. The comment on
// transferTrackerMaxEntries above derives a ~200 KiB memory bound from "1024 entries * ~200 bytes/entry", and re-derived it during batch C as
// "the (unchanged) byte count". It was never true: path and transfer_id arrive verbatim on the wire and the broker checked only that they were
// NON-EMPTY, so one request could carry up to max_payload and the real bound was 1024 * max_payload.
//
// The cost is not only resident bytes. Wherever a ledger directory resolves — which since #57 is single-broker installs too, not only cluster
// mode (round 2, F-T6) — every put() writes the same strings to disk through temp+write+fsync+rename+fsync(dir), and push.req / pull.req are
// plain nats.go callbacks with no goroutine wrapper, so the whole broker's transfer path is serialized behind that fsync. A large payload is
// therefore head-of-line blocking for every other transfer, not just memory.
//
// The values are generous on purpose — they are a sanity CEILING, not a policy. 4096 is the longest path any supported filesystem accepts
// (PATH_MAX), 128 comfortably holds a ULID or a UUID, and a hex SHA-256 is 64 characters.
//
// The digest is bounded by its maximum, NOT required to be exactly 64. Bounding memory is this gate's whole job; whether the digest is the
// RIGHT one is decided by the receiver, which recomputes it over the delivered bytes and refuses on mismatch. Adding a format contract here
// would reject requests the receiver would have caught anyway, and would do it in a place no one would think to look.
const (
	transferPathMaxLen   = 4096
	transferIDMaxLen     = 128
	transferSHA256MaxLen = 64
)

// transferRequestBounds validates the three unbounded-by-default strings. It returns the request_invalid detail, or "" when the request is
// within bounds. Shared by the push and pull handlers because the tracker they both feed is one map: bounding only one of them would leave the
// bound unreached.
func transferRequestBounds(transferID, path, sha string) string {
	if len(transferID) > transferIDMaxLen {
		return fmt.Sprintf("transfer_id length %d > %d", len(transferID), transferIDMaxLen)
	}
	if len(path) > transferPathMaxLen {
		return fmt.Sprintf("path length %d > %d", len(path), transferPathMaxLen)
	}
	if len(sha) > transferSHA256MaxLen {
		return fmt.Sprintf("sha256 length %d > %d", len(sha), transferSHA256MaxLen)
	}
	return ""
}

// xferRecentlyReapedTTL bounds how long the tracker remembers that it reaped a transfer id. Long enough for the receiver's real completion to
// arrive after a watchdog fire (that is the whole point — the receiver was still working), short enough that the set cannot grow without
// bound.
const xferRecentlyReapedTTL = 10 * time.Minute

// transferEntry is the broker's in-memory tracking record for one in-flight transfer. Keyed by transfer_id in transferTracker.entries.
type transferEntry struct {
	transferID string
	sid        string
	nid        string
	actor      string // NATS user nkey public key
	actorFP    string // sha-256 fingerprint
	verb       string // push | pull
	tier       string // a | b
	bucket     string // empty for tier A
	path       string
	size       int64
	startedAt  time.Time
	cancel     context.CancelFunc // cancels the timeout watchdog
	finalized  bool               // set under tracker.mu when audit complete|failed already written
	// committed is set under tracker.mu when a push-commit has been FORWARDED to the receiver. From that
	// moment the agent owns the terminal (its ev.transfer), so a creator-side finalize{failed} must be
	// refused; before it, the creator is the only party that can know the upload was abandoned.
	// origin: simcluster-speed 0a (drill 67) — a ctl whose ObjectStore.Put failed had no way to
	// release its slot, so the per-bucket serialisation refused every later tier-B push of the session
	// with too_many_in_flight until the watchdog reaped the entry at its full budget.
	committed bool
	// start is the canonical cap0 START published when the broker retained an admission identity (nil:
	// the pre-S7 shape). Immutable after put(); every terminal built from this entry binds to its nonce.
	// origin: hot-update S7 plan §4 / §5.1 / L12 (S7 integration oplegacy F2)
	start *schema.AuditTransfer
}

// transferTracker is a process-wide registry. Long-lived, attached to Broker. All transferEntry reads/writes hold tracker.mu.
type transferTracker struct {
	mu      sync.Mutex
	entries map[string]*transferEntry
	// reaped (batch C) remembers ids the WATCHDOG terminated, with the time it happened. It exists so
	// a receiver's genuine completion arriving after the budget expired can be LOGGED rather than
	// dropped in silence — that silence is the last step of "the file landed but the audit says
	// failed". Bounded by an opportunistic sweep on insert plus xferRecentlyReapedTTL.
	reaped map[string]time.Time
}

func newTransferTracker() *transferTracker {
	return &transferTracker{entries: map[string]*transferEntry{}, reaped: map[string]time.Time{}}
}

// markReaped records that the watchdog (not a normal finalize) ended this transfer.
func (t *transferTracker) markReaped(id string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.reaped == nil {
		t.reaped = map[string]time.Time{}
	}
	for k, at := range t.reaped {
		if now.Sub(at) >= xferRecentlyReapedTTL {
			delete(t.reaped, k)
		}
	}
	t.reaped[id] = now
}

// recentlyReaped reports whether the watchdog ended this transfer within the retention window.
func (t *transferTracker) recentlyReaped(id string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	at, ok := t.reaped[id]
	return ok && time.Since(at) < xferRecentlyReapedTTL
}
func (t *transferTracker) get(id string) *transferEntry {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.entries[id]
}

// put inserts an entry. Returns "" on success, else the response Code the caller must reject the request with. A transfer_id already in flight
// is never overwritten: the original entry's watchdog stays armed on that id, so replacing the entry would let the stale watchdog claim and
// reap the replacement mid-transfer.
func (t *transferTracker) put(e *transferEntry) string {
	code, _ := t.putWithBlocker(e)
	return code
}

// putWithBlocker is put plus, for a per-bucket refusal, the transfer_id that holds the bucket — the ONE piece of information that makes the
// refusal actionable. "retry shortly or use a fresh transfer id" was the advice for both refusal shapes, and for the bucket shape the second
// half is false: the slot is keyed on the bucket, so a fresh id is refused identically. An operator reading drill 67's twelve refusals had no
// way to see that they were all waiting on the same abandoned upload.
func (t *transferTracker) putWithBlocker(e *transferEntry) (code, blocker string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, exists := t.entries[e.transferID]; exists {
		return "transfer_id_in_flight", e.transferID
	}
	// The bucket is per session and intentionally sized for one maximum object plus overhead.
	// Serialize tier-B use of the same bucket so two valid objects cannot race into DiscardNew.
	// Tier A (empty bucket) and different sessions remain concurrent.
	if e.bucket != "" {
		for _, live := range t.entries {
			if live.bucket == e.bucket {
				return "too_many_in_flight", live.transferID
			}
		}
	}
	if len(t.entries) >= transferTrackerMaxEntries {
		return "too_many_in_flight", ""
	}
	t.entries[e.transferID] = e
	return "", ""
}

// inFlightRefusalText renders the operator-facing reason for a put refusal. code is what putWithBlocker returned; blocker is the holding
// transfer_id when the refusal is the per-bucket serialisation.
func inFlightRefusalText(transferID, code, blocker string) string {
	if code == "too_many_in_flight" && blocker != "" {
		return fmt.Sprintf("transfer %s rejected (%s): another tier-B transfer %s is in flight in this session's bucket; "+
			"wait for it to finish or be reaped at its budget — a fresh transfer id is refused the same way",
			transferID, code, blocker)
	}
	return fmt.Sprintf("transfer %s rejected (%s); retry shortly or use a fresh transfer id", transferID, code)
}

// remove deletes the entry. It used to return it; nothing ever read the result, and a returned value nobody reads reads like a handle somebody
// is holding.
//
// Identity-bound like the claims (external review round 3 R3-F1, main-process follow-through): only the exact entry the caller holds is
// deleted, never whatever the id maps to now. Every production remove follows a successful claim on the same entry, and a claimed entry cannot
// be replaced (put refuses a live id), so a by-id delete was safe by a two-step argument — this makes it safe by construction, and leaves put
// (refuses duplicates) and get (the preview) as the only by-id paths.
func (t *transferTracker) remove(expected *transferEntry) {
	if expected == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.entries[expected.transferID] == expected {
		delete(t.entries, expected.transferID)
	}
}

// claimFinalize atomically marks the expected entry as finalized iff:   - the map still holds that exact entry, AND   - it hasn't already been
// finalized.
//
// First caller wins; later callers receive ok=false and MUST NOT write a second audit row / delete the transfer object twice.
//
// The returned *transferEntry MAY be non-nil even when ok=false (entry exists but was already claimed) — callers can use the entry for an
// idempotent OK reply, but must not mutate it. The fields used for caller-side validation (sid, nid, actor, verb, tier, bucket, path) are
// immutable after put(), so they may be read without holding tracker.mu.
//
// Audit shard P11 F1: previous markFinalized + per-handler
// `entry.finalized = false` "unclaim" pattern was racy because the
// unclaim happened outside tracker.mu. The new contract is validate-then-claim: callers validate immutable fields on expected before claiming.
// The identity check under the lock preserves that authorization even if a completed transfer's id is reused while validation is in flight. It
// also fences stale watchdogs and cleanup callbacks out of the new entry.
func (t *transferTracker) claimFinalize(expected *transferEntry) (e *transferEntry, ok bool) {
	if expected == nil {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e = t.entries[expected.transferID]
	if e != expected {
		return nil, false
	}
	if e.finalized {
		return e, false
	}
	e.finalized = true
	return e, true
}

// markCommitted hands the expected push to the receiver. It reports false when the entry was removed, replaced or already finalized (the
// watchdog won), in which case the caller must not forward — the receiver would Get an object whose terminal audit is already written.
func (t *transferTracker) markCommitted(expected *transferEntry) bool {
	if expected == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[expected.transferID]
	if e != expected || e.finalized {
		return false
	}
	e.committed = true
	return true
}

// abandonRefusal is why claimAbandonedPush did not claim: the entry is gone, already finalized (a concurrent finalize / the watchdog won — the
// caller answers idempotent OK), not a push, a push the receiver already owns (committed), or a tier-A push (never the creator's to end).
type abandonRefusal int

const (
	abandonClaimed   abandonRefusal = iota
	abandonNoEntry                  // the expected entry is no longer tracked (removed or replaced)
	abandonFinalized                // someone else claimed the terminal first
	abandonNotPush                  // a pull — claimFinalize's job
	abandonCommitted                // the commit was forwarded; the receiver's ev.transfer is the terminal
	abandonTierA                    // tier-A: the receiver owned the terminal from the first byte
)

// claimAbandonedPush is claimFinalize restricted to a push whose creator gave up BEFORE commit: the entry must still be the validated preview,
// a push, TIER B, not yet committed, not yet finalized. The checks and the claim happen under one lock so a push-commit racing in cannot slip
// between "not committed" and "finalized" — whichever of markCommitted / claimAbandonedPush takes the mutex first wins, and the other sees it.
// The refusal reason is the entry's state AS READ UNDER THAT LOCK: the caller words its reply from it, and reading the flags from the preview
// pointer afterwards would race markCommitted's write (internal review round 1 R6-F3).
//
// The tier is the TRACKED one (stamped from the validated push.req when the entry was created), never
// the finalize body's: a tier-A push has no commit phase — its bytes went to the receiving agent in the push.req itself, so `committed` is
// false for its whole life, and without this check the creator's finalize{failed} claimed the terminal of a transfer the agent was in the
// middle of landing — audit said failed, the tracker was gone, and the agent's real terminal then found no entry (external review F2). Only
// tier B has a pre-commit window in which the creator is the only party who can know the upload was abandoned.
func (t *transferTracker) claimAbandonedPush(expected *transferEntry) (*transferEntry, abandonRefusal) {
	if expected == nil {
		return nil, abandonNoEntry
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.entries[expected.transferID]
	switch {
	case e != expected:
		return nil, abandonNoEntry
	case e.finalized:
		return e, abandonFinalized
	case e.verb != "push":
		return e, abandonNotPush
	case e.committed:
		return e, abandonCommitted
	case e.tier != "b":
		return e, abandonTierA
	}
	e.finalized = true
	return e, abandonClaimed
}

// activeOBJStreams returns a snapshot of the in-flight bucket names. Used by the boot reconciler to distinguish "stream is owned by an active
// in-process transfer" (keep) from "orphan from a previous broker that crashed" (delete). After a restart entries is empty so EVERY OBJ_xfer-*
// stream is considered orphan — which is correct because the in-memory state-machine is lost on crash.
func (t *transferTracker) activeOBJStreams() map[string]struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]struct{}{}
	for _, e := range t.entries {
		if e.bucket != "" {
			out["OBJ_"+e.bucket] = struct{}{}
		}
	}
	return out
}

// LegacyEntry and LegacyTracker let the broker retain thin adapters without keeping a second implementation of the pre-continuation tracker.
// origin: hot-update S7 plan §5.1 / L13
type LegacyEntry = transferEntry
type LegacyTracker = transferTracker
type LegacyRefusal = abandonRefusal

const (
	LegacyClaimed   = abandonClaimed
	LegacyNoEntry   = abandonNoEntry
	LegacyFinalized = abandonFinalized
	LegacyNotPush   = abandonNotPush
	LegacyCommitted = abandonCommitted
	LegacyTierA     = abandonTierA
)

type LegacyRecord struct {
	TransferID, SID, NID, Actor, ActorFP, Verb, Tier, Bucket, Path string
	Size                                                           int64
	StartedAt                                                      time.Time
	Finalized, Committed                                           bool
	Start                                                          *schema.AuditTransfer
}

// LegacyOutcome is the receiver- or broker-decided half of one cap0 terminal.
// Tier overrides the tracked tier only where the receiver reports it (a pull).
type LegacyOutcome struct {
	Kind, Tier, Code, Error string
	Bytes, DurationMs       int64
	Ts                      time.Time
}

// LegacyTerminal is the one shape of a cap0 terminal built from tracked state;
// the receiver-event, creator-finalize, prepare-cleanup and watchdog paths each
// assembled these fields by hand. Code/Error survive only on a failed kind, and
// the row binds to the admission nonce its START retained, never a fresh one.
// origin: hot-update S7 plan §4 / §5.1 / L12 (S7 integration oplegacy F2)
func LegacyTerminal(r LegacyRecord, o LegacyOutcome) schema.AuditTransfer {
	rec := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: o.Kind, Verb: r.Verb, Ts: o.Ts,
		Session: r.SID, Node: r.NID, ActorNkey: r.Actor, ActorFp: r.ActorFP, TransferID: r.TransferID,
		Path: r.Path, Tier: r.Tier, Bucket: r.Bucket, Bytes: o.Bytes, DurationMs: o.DurationMs}
	if o.Tier != "" {
		rec.Tier = o.Tier
	}
	if o.Kind == "failed" {
		rec.Code, rec.Error = o.Code, o.Error
	}
	if r.Start != nil {
		rec = schema.BindLegacyTerminal(rec, r.Start.EntryNonce)
	}
	return rec
}

func NewLegacyTracker() *LegacyTracker { return newTransferTracker() }
func NewLegacyEntry(r LegacyRecord) *LegacyEntry {
	return &transferEntry{transferID: r.TransferID, sid: r.SID, nid: r.NID, actor: r.Actor,
		actorFP: r.ActorFP, verb: r.Verb, tier: r.Tier, bucket: r.Bucket, path: r.Path,
		size: r.Size, startedAt: r.StartedAt, start: r.Start}
}
func (t *transferTracker) Get(id string) *LegacyEntry          { return t.get(id) }
func (t *transferTracker) Put(e *LegacyEntry) (string, string) { return t.putWithBlocker(e) }
func (t *transferTracker) Remove(e *LegacyEntry)               { t.remove(e) }
func (t *transferTracker) ClaimFinalize(e *LegacyEntry) (*LegacyEntry, bool) {
	return t.claimFinalize(e)
}
func (t *transferTracker) MarkCommitted(e *LegacyEntry) bool { return t.markCommitted(e) }
func (t *transferTracker) ClaimAbandonedPush(e *LegacyEntry) (*LegacyEntry, LegacyRefusal) {
	return t.claimAbandonedPush(e)
}
func (t *transferTracker) ActiveOBJStreams() map[string]struct{} { return t.activeOBJStreams() }
func (t *transferTracker) RecentlyReaped(id string) bool         { return t.recentlyReaped(id) }
func (t *transferTracker) Record(e *LegacyEntry) LegacyRecord {
	t.mu.Lock()
	defer t.mu.Unlock()
	return LegacyRecord{e.transferID, e.sid, e.nid, e.actor, e.actorFP, e.verb, e.tier, e.bucket, e.path, e.size, e.startedAt, e.finalized, e.committed, e.start}
}
func (t *transferTracker) SetSize(e *LegacyEntry, size int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.entries[e.transferID] != e || e.finalized {
		return false
	}
	e.size = size
	return true
}
func (t *transferTracker) SetCancel(e *LegacyEntry, cancel context.CancelFunc) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.entries[e.transferID] != e || e.finalized {
		cancel()
		return
	}
	e.cancel = cancel
}
func (t *transferTracker) CancelWatchdog(e *LegacyEntry) {
	t.mu.Lock()
	cancel := e.cancel
	t.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}
func LegacyRequestBounds(id, path, sha string) string { return transferRequestBounds(id, path, sha) }
func LegacyRefusalText(id, code, blocker string) string {
	return inFlightRefusalText(id, code, blocker)
}

// Decisions captures the binding generation for an asynchronous finalizer. A pre-cut callback may retain its view, but cannot acknowledge the
// new owner.
// origin: hot-update S7 plan §8.2 / T13
func Decisions(m Manager) (x.DecisionPort, error) {
	o, ok := m.(*owner)
	if !ok {
		return nil, refusal(x.CodeOwnerRecovering)
	}
	select {
	case <-o.serial:
	default:
		return nil, refusal(x.CodeOwnerRecovering)
	}
	defer o.leave()
	return &decisionView{owner: o, generation: o.state.Generation}, nil
}

// DecisionsForBinding reserves the view of a sleeping owner's next binding. Capturing the imported generation before Bind would leave every
// finalizer callback stale immediately after activation; old views remain fenced.
// origin: hot-update S7 plan §8.2 / T13 / T36
func DecisionsForBinding(m Manager, generation uint64) (x.DecisionPort, error) {
	o, ok := m.(*owner)
	if !ok {
		return nil, refusal(x.CodeOwnerRecovering)
	}
	select {
	case <-o.serial:
	default:
		return nil, refusal(x.CodeOwnerRecovering)
	}
	defer o.leave()
	if !o.sleeping || generation < o.state.Generation || generation > o.state.Generation+1 {
		return nil, refusal(x.CodeOwnerRecovering)
	}
	return &decisionView{owner: o, generation: generation}, nil
}

type decisionView struct {
	owner      *owner
	generation uint64
}

// OwnerGeneration identifies this finalizer view, not the original permit.
func (v *decisionView) OwnerGeneration() uint64 { return v.generation }

// TransferObservation borrows the tracker's exact boot deadline for private
// custody. Observation never waits for, flushes or mutates a decision; a busy
// or stale view cannot certify the boundary using a newly computed deadline.
// origin: S7 internal review INTEG-O F5
func (v *decisionView) TransferObservation(id x.Identity) (x.Observation, bool) {
	o := v.owner
	select {
	case <-o.serial:
	default:
		return x.Observation{}, false
	}
	defer o.leave()
	e := o.entries[x.Key{SID: id.SID, TransferID: id.TransferID}]
	if e == nil || e.row.Identity != id || v.generation != o.state.Generation {
		return x.Observation{}, false
	}
	return x.Observation{OriginalDeadline: e.watch.ByteDeadline, Generation: o.state.Generation}, true
}

func (v *decisionView) enter(d x.Deadline) error {
	if err := v.owner.enter(d); err != nil {
		return err
	}
	if v.generation != v.owner.state.Generation {
		v.owner.leave()
		return refusal(x.CodeOwnerRecovering)
	}
	return nil
}
func (v *decisionView) Read(k x.Key, d x.Deadline) (x.LedgerRow, error) {
	if err := v.enter(d); err != nil {
		return x.LedgerRow{}, err
	}
	defer v.owner.leave()
	e := v.owner.entries[k]
	if e == nil {
		return x.LedgerRow{}, refusal(x.CodeUnknown)
	}
	if err := v.owner.flush(e, d); err != nil {
		return x.LedgerRow{}, err
	}
	// Report a durable retirement as settled before a finalizer acts on it.
	// origin: hot-update S7 plan §5.3 — S7 production activation
	_ = v.owner.settleRetired(e, d)
	return cloneRow(e.row), nil
}
func (v *decisionView) Pending(d x.Deadline) ([]x.LedgerRow, error) {
	if err := v.enter(d); err != nil {
		return nil, err
	}
	defer v.owner.leave()
	if err := v.owner.census(d); err != nil {
		return nil, err
	}
	var rows []x.LedgerRow
	for _, e := range v.owner.entries {
		if e.row.Phase == x.PhaseSettled {
			continue
		}
		if err := v.owner.flush(e, d); err != nil {
			return nil, err
		}
		rows = append(rows, cloneRow(e.row))
	}
	return rows, nil
}
func (v *decisionView) callback(k x.Key, nonce string, revision uint64) (*tracked, error) {
	e := v.owner.entries[k]
	// origin: hot-update S7 plan §4 / T13 — both nonce and revision are mandatory.
	if e == nil || e.row.EntryNonce != nonce || e.row.Revision != revision || e.dirty {
		return nil, refusal(x.CodeIntentConflict)
	}
	return e, nil
}
func (v *decisionView) AcceptHistory(k x.Key, nonce string, revision uint64, r x.HistoryReceipt, d x.Deadline) error {
	if err := v.enter(d); err != nil {
		return err
	}
	defer v.owner.leave()
	e, err := v.callback(k, nonce, revision)
	if err != nil {
		return err
	}
	return v.owner.acceptHistory(e, r, d)
}

// AcceptWriterRetirement records the original producer's closed future gate
// and completed join before the object fence can run. The authenticated outer
// adapter supplies the Stop/Freeze state, checked against its pinned stream
// incarnation; neither an expired byte budget nor an exact history row can
// supply this witness. Keep sent-unknown expectations for the later fence.
// origin: hot-update S7 plan §2.3 / s7-p9m remaining request 3
// AcceptWriterRetirement is the optional, revision-bound witness producer. The outer adapter authenticates the original actor and pinned
// incarnation; this serializer fences pre-cut views and preserves every possible old publish expectation without claiming consumer, tombstone
// or purge acknowledgement.
// origin: hot-update S7 plan §2.3 / FIX-TERM request 3
func (v *decisionView) AcceptWriterRetirement(k x.Key, nonce string, revision uint64, state x.WriterState, d x.Deadline) error {
	if err := v.enter(d); err != nil {
		return err
	}
	defer v.owner.leave()
	e, err := v.callback(k, nonce, revision)
	if err != nil {
		return err
	}
	spec, fence := state.ObjectSpec, state.RetirementFence
	// A producer witness does not claim consumers stopped, an object purged,
	// or a receiver's tmp cleaned. Those remain the finalizer's responsibilities.
	if spec.Identity != e.row.Identity || e.row.Intent.Verb != "push" || e.row.Intent.Tier != "b" || spec.Identity.ObjectVersion == "" ||
		spec.Bucket != "xfer-"+k.SID || spec.Stream != "OBJ_"+spec.Bucket || spec.StreamCreated == "" || state.AttachmentGeneration == 0 ||
		!fence.WriterRetired || fence.ConsumersRetired || fence.Purged || fence.Receipt != nil {
		return refusal(x.CodeIntentConflict)
	}
	for _, receipt := range state.Receipts {
		fence.ExpectedStreamSeq = max(fence.ExpectedStreamSeq, receipt.StreamSeq)
	}
	if pending := state.PendingAttempt; pending != nil {
		fence.ExpectedStreamSeq = max(fence.ExpectedStreamSeq, pending.ExpectedStreamSeq)
	}
	if metadata := state.MetaReceipt; metadata != nil {
		if metadata.Spec != spec {
			return refusal(x.CodeIntentConflict)
		}
		fence.ExpectedStreamSeq = max(fence.ExpectedStreamSeq, metadata.MetadataSeq)
		fence.PreviousMetadataSeq = max(fence.PreviousMetadataSeq, metadata.MetadataSeq)
	}
	if fence.PreviousMetadataSeq > fence.ExpectedStreamSeq {
		return refusal(x.CodeIntentConflict)
	}
	row := cloneRow(e.row)
	retained := row.Cleanup.WriterFence
	retained.WriterRetired = true
	retained.ExpectedStreamSeq = max(retained.ExpectedStreamSeq, fence.ExpectedStreamSeq)
	retained.PreviousMetadataSeq = max(retained.PreviousMetadataSeq, fence.PreviousMetadataSeq)
	if retained == row.Cleanup.WriterFence {
		return nil
	}
	row.Cleanup.WriterFence = retained
	return v.owner.persist(e, row, x.MutationCleanup, d)
}
func (v *decisionView) AcceptCleanup(k x.Key, nonce string, revision uint64, r x.CleanupReceipt, d x.Deadline) error {
	if err := v.enter(d); err != nil {
		return err
	}
	defer v.owner.leave()
	e, err := v.callback(k, nonce, revision)
	if err != nil {
		return err
	}
	if r.Identity != e.row.Identity || r.OwnerGeneration != v.generation || r.Digest == "" {
		return refusal(x.CodeIntentConflict)
	}
	if _, err = terminalReceipt(e.row); err != nil {
		return err
	}
	if !r.WriterRetired || !r.ConsumersRetired || !r.DirectorySynced || !r.TmpUnlinked ||
		e.row.Intent.Tier == "b" && (!r.ObjectPurged || r.FenceSequence == 0) {
		return refusal(x.CodeResultUnknown)
	}
	row := cloneRow(e.row)
	// Preserve the actual fence ACK so the finalizer can re-read its durable
	// proof before consuming both ledger directories.
	// origin: hot-update S7 plan §2.3 / T21 / INTEG-E cleanup receipt regression
	fence := row.Cleanup.WriterFence
	fence.WriterRetired, fence.ConsumersRetired, fence.Purged = true, true, r.ObjectPurged
	if row.Intent.Tier == "b" {
		fence.Receipt = &x.HistoryReceipt{Stream: "OBJ_xfer-" + row.Identity.SID, Sequence: r.FenceSequence, RecordDigest: r.Digest}
	}
	row.Cleanup = x.CleanupState{Identity: row.Identity, WriterFence: fence, PrimaryRetirePending: true, OutboxRetirePending: true}
	row.Phase = x.PhaseCleanupPending
	return v.owner.persist(e, row, x.MutationCleanup, d)
}

// ReconcileRetirement is a cache acknowledgement, never a new ledger row.
// It uses the current generation and revision, then proves both durable
// responsibilities absent before allowing an immediate result/image read.
// origin: S7 internal review rv-recovery F08 / hot-update S7 plan §8.2
func (v *decisionView) ReconcileRetirement(k x.Key, nonce string, revision uint64, d x.Deadline) error {
	if err := v.enter(d); err != nil {
		return err
	}
	defer v.owner.leave()
	e, err := v.callback(k, nonce, revision)
	if err != nil {
		return err
	}
	if err = v.owner.settleRetired(e, d); err != nil {
		return err
	}
	if e.row.Phase != x.PhaseSettled {
		return refusal(x.CodeResultUnknown)
	}
	return nil
}

// A cleanup callback does not prove that P12 consumed both durable directories. Only their complete absent census permits the cache-only
// settled transition; never recreate a retired row merely to store that local transition.
// origin: hot-update S7 plan §5.1 / §5.3 / T13
func (o *owner) settleRetired(e *tracked, d x.Deadline) error {
	if e.row.Phase != x.PhaseCleanupPending {
		return nil
	}
	scan, err := o.cfg.Ledger.Scan(d)
	if err != nil {
		return err
	}
	if !scan.Ready || !scan.Complete {
		return refusal(x.CodeOwnerRecovering)
	}
	for _, row := range scan.Rows {
		if key(row.Identity) == key(e.row.Identity) {
			return nil
		}
	}
	for _, protection := range scan.Entries {
		if key(protection.Identity) == key(e.row.Identity) {
			return nil
		}
	}
	e.row.Phase = x.PhaseSettled
	e.row.Cleanup.PrimaryRetirePending = false
	e.row.Cleanup.OutboxRetirePending = false
	return nil
}

// IDs supplies the existing cut census without exporting the mutable map.
func (t *transferTracker) IDs() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	ids := make([]string, 0, len(t.entries))
	for id := range t.entries {
		ids = append(ids, id)
	}
	return ids
}

// Read-only legacy bounds keep broker adapters and their existing tests on the same implementation constants after the mechanical move.
const (
	LegacyTrackerMaxEntries = transferTrackerMaxEntries
	LegacyRecentlyReapedTTL = xferRecentlyReapedTTL
	LegacyIDMaxLen          = transferIDMaxLen
	LegacyPathMaxLen        = transferPathMaxLen
	LegacySHA256MaxLen      = transferSHA256MaxLen
)

func (t *transferTracker) MarkReaped(id string, now time.Time) { t.markReaped(id, now) }

// TransferIDs supplies the cut census without exposing mutable tracker state.
// origin: hot-update S7 plan §8.2 / T39
func (t *transferTracker) TransferIDs() []string { return t.IDs() }

// CensusEntry is one tracked transfer as the tracker names it: its business
// key, the EntryNonce of its slot, and whether it has settled.
type CensusEntry struct {
	SID, TransferID, EntryNonce string
	Settled                     bool
}

// Census lists every tracked entry, settled or not, for the S8 broker census.
// It takes the owner's serial token like any operation, bounded by d, and
// neither flushes, admits nor retires anything; a manager this package did
// not build cannot be enumerated and refuses instead of reporting none.
// origin: hot-update S8 plan §4.2 / M06 / M14; S8 BROKER-EXPORTER R-11
func Census(m Manager, d x.Deadline) ([]CensusEntry, error) {
	o, ok := m.(*owner)
	if !ok || o == nil {
		return nil, refusal(x.CodeOwnerRecovering)
	}
	if err := o.take(d); err != nil {
		return nil, err
	}
	defer o.leave()
	if o.stopped {
		return nil, refusal(x.CodeOwnerRecovering)
	}
	out := make([]CensusEntry, 0, len(o.entries))
	for _, e := range o.entries {
		out = append(out, CensusEntry{SID: e.row.Identity.SID, TransferID: e.row.Identity.TransferID, EntryNonce: e.row.EntryNonce, Settled: e.row.Phase == x.PhaseSettled})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SID < out[j].SID || out[i].SID == out[j].SID && out[i].TransferID < out[j].TransferID
	})
	return out, nil
}

func cloneRecord(r *x.CanonicalRecord) *x.CanonicalRecord {
	if r == nil {
		return nil
	}
	c := *r
	c.Bytes = bytes.Clone(r.Bytes)
	return &c
}

// The closure is bidirectional: dangling records and omitted responsibility both reject Import before a sleeping owner can protect an incomplete image.
// origin: hot-update S7 plan §8.2 / T13
func validateClosure(state x.TrackerState, entries map[x.Key]*tracked) error {
	grants := make(map[x.Identity]bool)
	for _, slot := range state.GrantSlots {
		e := entries[key(slot.Identity)]
		if e == nil || grants[slot.Identity] || e.row.Identity != slot.Identity || e.row.Grant == nil || *e.row.Grant != slot.Grant || e.row.Revision != slot.Revision {
			return refusal(x.CodeTerminalConflict)
		}
		grants[slot.Identity] = true
	}
	cleanup := make(map[x.Identity]bool)
	for _, c := range state.Cleanup {
		e := entries[key(c.Identity)]
		if e == nil || cleanup[c.Identity] || !reflect.DeepEqual(c, e.row.Cleanup) {
			return refusal(x.CodeTerminalConflict)
		}
		cleanup[c.Identity] = true
	}
	starts, terminals := make(map[x.Identity]bool), make(map[x.Identity]bool)
	for _, group := range []struct {
		records []x.CanonicalRecord
		seen    map[x.Identity]bool
		start   bool
	}{
		{state.PendingStart, starts, true}, {state.PendingTerminal, terminals, false},
	} {
		for _, r := range group.records {
			e := entries[key(r.Identity)]
			if e == nil || group.seen[r.Identity] {
				return refusal(x.CodeTerminalConflict)
			}
			expected := e.row.CanonicalTerminal
			if group.start {
				expected = e.row.CanonicalStart
			}
			if expected == nil || !reflect.DeepEqual(r, *expected) {
				return refusal(x.CodeTerminalConflict)
			}
			group.seen[r.Identity] = true
		}
	}
	for _, e := range entries {
		id := e.row.Identity
		if grants[id] != (e.row.Grant != nil) || !cleanup[id] || starts[id] != (e.row.StartDelivery != x.DeliveryHistoryConfirmed) || terminals[id] != (e.row.CanonicalTerminal != nil && e.row.TerminalDelivery != x.DeliveryHistoryConfirmed) {
			return refusal(x.CodeTerminalConflict)
		}
	}
	return nil
}
func cloneRow(row x.LedgerRow) x.LedgerRow {
	// Clone the closed value graph at the port boundary. No caller may mutate a
	// canonical byte slice or nested cleanup record behind the serializer.
	b, _ := json.Marshal(row)
	var copy x.LedgerRow
	_ = json.Unmarshal(b, &copy)
	return copy
}
