package broker

// cluster_forward.go — D4 write-forwarding (follower->leader) over the broker-only
// tether.v2.cluster.apply.<verb> bus (§4.1). BUILD-AND-PROVE: nothing here is wired
// into cmd/tether/serve.go (production builds no cluster.Node; cutover=D9). It is
// the broker-owned NATS adapter for cluster's raft-free primitives — internal/cluster
// stays NATS-free (L-2: raft only in internal/cluster), so this package (which already
// imports both nats and cluster) owns the wire + the cluster.IsNotLeader -> typed
// reply / authcallout.ErrNotLeader translation.
//
// ROUTING (main-process implementation ruling, refining d4-plan §2.4's "queue group"):
// the responder uses a BROADCAST Subscribe, NOT a queue group, and ONLY the broker
// that believes it is leader replies — a follower stays SILENT. A queue group lands a
// request on an arbitrary member with no leader affinity (leader-address advertisement
// is D7); broadcast + leader-only-reply guarantees the leader answers without leader
// discovery. Followers MUST stay silent rather than reply not_leader, else a follower's
// fast not_leader could race ahead of the leader's commit-round-trip ok and cause a
// spurious retry. An election (no leader) => no reply => the forwarder times out =>
// retriable (same ReqID). A deposed leader (IsLeader() true at check, lost mid-Propose)
// replies not_leader; a newly-elected leader may also answer ok — the requester takes
// the first reply and the content-addressed ReqID dedups any double-commit.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	nodepkg "github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/nats-io/nats.go"
	"golang.org/x/time/rate"
)

// Forwarded verbs (§13.7-named writes). Building the GENERIC forwarder but wiring +
// testing only these three; the other session-control verbs are D9 (dead wire now).
const (
	VerbProvision = "provision"
	VerbJoin      = "join"
	VerbReconcile = "reconcile"
	// VerbTransferAudit (D8a §9) forwards one transfer-audit record to the leader, which
	// commits it as a re-derivable OpTransferAudit entry. Unlike provision/join it does
	// NOT reject a non-empty env.ReqID at the boundary — the idempotency key is CONTENT-
	// derived inside xferaudit.PlanTransferAudit (hex(sha256(transfer_id:kind))), so any
	// leader re-derives the SAME key and the 0011 ledger collapses a forwarder retry; the
	// originating broker passes reqID="" (the key is not minted there).
	VerbTransferAudit = "xferaudit"
	// VerbAlertSignal (D8b §10.2) forwards a per-node alert raise/clear decision (currently
	// disk_pressure) to the leader's replicated alert store. The leader reads the CURRENT
	// ACTIVE state and commits a raise/clear ONLY on a transition, so the level-triggered
	// re-assert from each broker's disk monitor self-heals a dropped clear without writing a
	// raft entry per tick (idle-zero-writes).
	VerbAlertSignal = "alertsignal"
	// VerbAlertAck (D8b §10.1) forwards a cluster-level ack to the leader (idempotent UPSERT).
	VerbAlertAck = "alertack"
	// VerbSessionCreate (D9 §3 step 3) forwards a session create to the leader, which bakes
	// the created_at literal and commits OpSessionCreate. The SID == the user-given name
	// (sessions are name-keyed), so no leader-baked id read-back is needed for the key; the
	// caller reads the committed row back by SID for the authoritative created_at. PlanCreate
	// re-checks existence on the leader (ErrAlreadyExists), so no boundary ReqID is minted.
	VerbSessionCreate = "sessioncreate"
	// VerbPortFree forwards a data-less public-port free. VerbPortRevoke carries the exact
	// allocation identity selected by the leader's offline-node scan; it must not revoke by
	// bare port because the public port may already have been reused.
	// VerbBusNkeySet (C3) forwards a broker's self-reported bus nkey pub to the leader for
	// OpClusterBusNkeySet (each broker writes its OWN row; followers forward).
	VerbBusNkeySet = "busnkeyset"

	// NOTE (C5, Stage-C N0): proxy.set / proxy.sub.create / proxy.sub.revoke are ALL broadcast +
	// leader-only (isBroadcastClusterSubject), consistent with expose/run/kill — the leader proposes
	// locally (b.cl.node.Propose) and a follower stays silent, so there is NO proxy forward verb (an
	// earlier queue-group design had VerbProxySetEnabled/VerbProxySubRevoke; both were dead wire under
	// broadcast+leader-only and were removed).

	VerbPortFree   = "portfree"
	VerbPortRevoke = "portrevoke"
	// VerbPortFreeAllocation is the expose-rm path: the leader must re-check the exact
	// allocation identity selected by name before freeing, rather than freeing by port only.
	VerbPortFreeAllocation = "portfreealloc"
	// VerbNodeRegister (D9 §3, audit #1) forwards an agent register's IDENTITY columns to
	// the leader (PlanRegister writes identity only; the liveness columns last_heartbeat_at/
	// status are written locally by the originating broker, §3.5). The payload is the
	// node.RegisterInput; PlanRegister re-checks the session exists on the leader.
	VerbNodeRegister = "noderegister"
	// VerbProcInsert (D9 §3, audit #4) forwards a process-started record to the leader.
	// Error-only reply; a forwarder retry is idempotent because PlanInsert bakes INSERT OR
	// IGNORE on the pid PRIMARY KEY — a re-propose at a new raft index is a deterministic
	// Apply no-op, never a PK-violation that fail-stop panics the FSM (audit M8 / port-plan F1).
	VerbProcInsert = "procinsert"
	// VerbProcMarkExited (D9 round-1 review BLOCKER) forwards a process EXIT to the leader
	// (the proc.exit event + reconcileOnRegister's missed-exit path). Carries the agent-
	// reported exit code + end time (a FACT, not leader-baked). PlanMarkExited's
	// WHERE status='RUNNING' guard makes it idempotent on an already-exited row, so a
	// forwarder retry / a double-exit is harmless.
	VerbProcMarkExited = "procmarkexited"
	// VerbSessionTombstone (D9 round-1 review BLOCKER, audit #11) forwards a session rm's
	// state transition to DELETING (PlanTombstone). The H.3 finalize cascade (drop rows) is
	// a SEPARATE leader-local Apply (VerbSessionDrop) once the agent-side teardown completes.
	VerbSessionTombstone = "sessiontombstone"
	// VerbSessionDrop forwards the H.3 cascade delete (the DELETE of a DELETING session's
	// rows after teardown). Data-less; PlanDropSession is a no-op on an absent row, so a
	// forwarder retry is harmless.
	VerbSessionDrop = "sessiondrop"
	// VerbNodeEvict (D9 round-2 BLOCKER) forwards `tether admin evict` (the operator's agent
	// kick) to the leader: PlanEvict deletes the agent_provisioning + nodes rows in one Apply.
	// Data-less; the DELETEs are no-ops on absent rows, so a forwarder retry is harmless.
	VerbNodeEvict = "nodeevict"
	// VerbSessionCreatorSet (prerelease audit round 2) forwards a `session create`
	// admission-table write. Data-less idempotency: the INSERT is OR IGNORE and the
	// DELETE is a no-op on an absent row, so a forwarder retry is harmless.
	VerbSessionCreatorSet = "sessioncreatorset"
	// NOTE (D9 §3, audit #6): port allocate is NOT a forward verb. The expose token is
	// leaked exactly once in the Allocate RETURN (only its hash is persisted), so a
	// follower could not read it back. In v1 expose is therefore LEADER-LOCAL (allocatePort
	// bounces a follower with a retryable not_leader, like the §8.1 admin verbs); transparent
	// follower-forward of the leak-once token is a future leaf (needs a forward-result reply).
)

// PortMutatePayload (D9) carries a single public port for the data-less port verbs
// (free/revoke). The leader bakes the timestamp literal.
type PortMutatePayload struct {
	Port int `json:"port"`
}

// BusNkeySetPayload (C3) is a broker self-reporting its bus nkey pub for OpClusterBusNkeySet.
type BusNkeySetPayload struct {
	NodeID     string `json:"node_id"`
	BusNkeyPub string `json:"bus_nkey_pub"`
}

// PortFreeAllocationPayload fences expose-rm against stale name->port reads and port reuse.
type PortFreeAllocationPayload upgradeauthority.AllocationIdentity

// allocIdentity narrows a port.Allocation to the five identity columns that every
// allocation state change fences on: internal/port/plan.go's planAllocationStateChange
// (the raft path) and internal/port/port.go's updateAllocationState (the direct path)
// both SELECT and UPDATE on exactly `port, sid, nid, name, token_hash`.
//
// WHY NARROW RATHER THAN WIDEN THE PAYLOAD (batch B, plan §5 decision #4)
//
// port.Allocation carries fourteen fields; this payload carries five. Before this
// function the two halves of every port state change disagreed about which: the
// LEADER-LOCAL closure captured the full struct while dispatchForward rebuilt one from
// the five marshalled fields. That was correct only BY COINCIDENCE — both planners
// happen to read only those five today.
//
// The moment someone fences PlanFreeAllocation on a sixth field, the coincidence ends and
// the bug is invisible in the worst possible way: the leader-direct path still has the
// field, so every leader-direct test stays green, and the failure only appears when ctl
// happens to reach a FOLLOWER. HomeBroker and Epoch — the two fields most likely to be
// fenced on next — are precisely the D6 §7.1-7.2 tunnel-plane identity columns, and
// port.go:114-146's tunnelTokenLookup already gates a REGISTER on the epoch ladder, so a
// fence on it here is a small step, not a hypothetical.
//
// (An earlier version of this comment claimed PlanAllocate is already fenced on Epoch.
// Internal review showed that is false: PlanAllocate WRITES epoch=0 into the baked INSERT,
// it does not fence a read on it. The point survives without the overstated precedent.)
//
// Narrowing once, here, makes all three call paths take the SAME five fields, so a sixth
// field read becomes wrong EVERYWHERE at once, including in the fastest and most covered
// path. Widening the payload instead would add wire surface and still leave the leader
// closure reading a different value than the wire carries.
func allocIdentity(a port.Allocation) PortFreeAllocationPayload {
	p := PortFreeAllocationPayload{Port: a.Port, SID: a.SID, NID: a.NID, Name: a.Name, TokenHash: a.TokenHash}
	if a.Authority != (upgradeauthority.Guard{}) {
		g := a.Authority
		p.Authority = &g
	}
	return p
}

// allocation rebuilds the narrowed port.Allocation. It is the SAME projection
// dispatchForward performs on the receiving side, named once so the two sides cannot
// drift apart again.
func (p PortFreeAllocationPayload) allocation() port.Allocation {
	a := port.Allocation{Port: p.Port, SID: p.SID, NID: p.NID, Name: p.Name, TokenHash: p.TokenHash}
	if p.Authority != nil {
		a.Authority = *p.Authority
	}
	return a
}

// SessionCreatePayload (D9) is the forwarded session-create request. The leader bakes the
// created_at; SID == Name (name-keyed sessions), so the follower needs no id round-trip.
type SessionCreatePayload struct {
	Name    string `json:"name"`
	FP      string `json:"fp"`
	PinHash string `json:"pin_hash"`
}

// SessionMutatePayload carries a single SID for the data-less session verbs (tombstone +
// hard-delete). The leader bakes the deleting_at literal for tombstone.
type SessionMutatePayload upgradeauthority.SessionMutation

// ProcMarkExitedPayload (D9 round-1 BLOCKER) carries an agent-reported process exit. The
// exit code + end time are FACTS from the agent (not leader-baked); PlanMarkExited's
// WHERE status='RUNNING' guard makes the Apply idempotent on an already-exited row.
//
// Sid is ADDITIVE and omitempty (N-1 window, requirements §6.7), but an ABSENT Sid is
// now REFUSED rather than treated as "render the pre-fence predicate" — see
// proc.ErrUnscopedExit. An N-1 broker forwards without the field, and its own input is
// an agent-chosen PID, so honouring those payloads let one session's agent mark another
// session's process EXITED through a broker that looks like a trusted peer. The refused
// exits are recovered by reconcileOnRegister at the agent's next register, which is why
// closing the hatch costs a delay rather than the exit code.
// origin: prerelease audit external review B-2.
type ProcMarkExitedPayload upgradeauthority.ProcessExit

// EvictPayload (D9 round-2 BLOCKER) carries the (sid,nid) of an `admin evict` kick.
// SessionCreatorPayload is the forwarded form of an admission-table write.
// prerelease audit round 2.
type SessionCreatorPayload struct {
	FP      string `json:"fp"`
	AddedBy string `json:"added_by"`
	Note    string `json:"note,omitempty"`
	Allow   bool   `json:"allow"`
}

type EvictPayload struct {
	SID string `json:"sid"`
	NID string `json:"nid"`
}

// Reply status codes on the typed forward reply envelope.
const (
	fwdStatusOK        = "ok"
	fwdStatusNotLeader = "not_leader"
	fwdStatusError     = "error"
)

// forwardEnvelope is what an originating broker publishes to cluster.apply.<verb>.
type forwardEnvelope struct {
	ReqID   string          `json:"req_id"`
	Verb    string          `json:"verb"`
	Payload json.RawMessage `json:"payload"`
}

// forwardReply is the leader's typed answer. Status ok=committed (or dedup-skipped,
// indistinguishable+correct); not_leader=answerer lost/never had leadership
// (retriable); error=permanent typed business error (NOT retriable).
type forwardReply struct {
	Status  string `json:"status"`
	ErrKind string `json:"err_kind,omitempty"`
	ErrMsg  string `json:"err_msg,omitempty"`
}

// ProvisionPayload / JoinPayload / ReconcilePayload are the verb-specific forward
// payloads. The leader re-verifies the PIN against its OWN session row (§6.2) and
// bakes its own time; the raw PIN flows only between trusted brokers over mTLS routes.
type ProvisionPayload struct {
	SID string `json:"sid"`
	NID string `json:"nid"`
	FP  string `json:"fp"`
	PIN string `json:"pin"`
}

// JoinPayload is the PIN-join (member) forward payload.
type JoinPayload struct {
	SID string `json:"sid"`
	FP  string `json:"fp"`
	PIN string `json:"pin"`
}

// ReconcilePayload forwards the RAW register request; the LEADER resolves it against
// its own replicated DB and bakes the self-sufficient ReconcileBatch (§4.1 "follower
// 转 agent 清单 → leader 算结果"). The ReqID is keyed on this raw request (NOT the
// resolved decisions, which the forwarder cannot compute) so a retry re-derives it.
type ReconcilePayload struct {
	SID string                `json:"sid"`
	NID string                `json:"nid"`
	Req proto.NodeRegisterReq `json:"req"`
}

// ForwardBusinessError carries a permanent (non-retriable) typed business error back
// across the forward boundary. The PIN seam denies on it (any non-ErrNotLeader error).
// Its Is(target) matches on a STABLE per-sentinel Kind code (NOT %T, which is
// non-discriminating for errors.New sentinels), so a forwarded agentprov.ErrInvalidPIN
// satisfies errors.Is(fwdErr, agentprov.ErrInvalidPIN) at the authcallout handler and
// the forwarded path emits the same pin_failed audit + canonical deny as the local
// path (review B1 / R-8 typed re-map).
type ForwardBusinessError struct {
	Kind string
	Msg  string
}

func (e *ForwardBusinessError) Error() string { return e.Msg }

// Is reports whether target is the business sentinel this error stands for, by
// comparing stable Kind codes (forwardErrKind). Lets errors.Is(fwdErr, sentinel) hold
// across the wire even though the original typed value did not cross the boundary.
func (e *ForwardBusinessError) Is(target error) bool {
	return e.Kind != "" && forwardErrKind(target) == e.Kind
}

// forwardErrKind maps a leader-side Plan/Apply business error to a STABLE wire kind
// code (used both to fill forwardReply.ErrKind on the responder and to match in
// ForwardBusinessError.Is on the forwarder). Unknown errors get "" (carry Msg only,
// classified as a generic permanent deny — identical to the local default branch).
func forwardErrKind(err error) string {
	switch {
	case errors.Is(err, agentprov.ErrInvalidPIN), errors.Is(err, session.ErrInvalidPIN):
		return "invalid_pin"
	case errors.Is(err, agentprov.ErrSessionMissing), errors.Is(err, session.ErrNotFound):
		return "session_missing"
	case errors.Is(err, agentprov.ErrSessionDeleting), errors.Is(err, session.ErrDeleting):
		return "session_deleting"
	case errors.Is(err, session.ErrAlreadyExists):
		return "session_already_exists"
	case errors.Is(err, agentprov.ErrAlreadyProvisioned):
		return "already_provisioned"
	// audit write-forward F2: the register/proc Plan business errors must keep their typed
	// identity across the forward boundary too, else a FOLLOWER-forwarded register against a
	// missing/inactive session loses node.ErrSessionMissing/ErrSessionNotActive and handleRegister
	// falls back to a generic store_error instead of the precise "session_not_found" guidance.
	case errors.Is(err, nodepkg.ErrSessionMissing):
		return "node_session_missing"
	case errors.Is(err, nodepkg.ErrSessionNotActive):
		return "node_session_not_active"
	case errors.Is(err, proc.ErrNodeMissing):
		return "proc_node_missing"
	// h1 C3: unknown_pid may only be acked to the courier off the LEADER's
	// committed view; when a follower forwards MarkExited, the leader's
	// proc.ErrNotFound must keep its identity across the wire or the
	// follower's handler would classify it store_error and the courier would
	// retry a GC'd pid forever.
	case errors.Is(err, proc.ErrNotFound):
		return "proc_not_found"
	default:
		return ""
	}
}

// ---- ReqID minting (originating broker; content-addressed; cross-retry stable) ----
//
// Domain-separated SHA-256, 128-bit lowercase-hex prefix (matches Command.ReqID's
// charset guard). NEVER leader-minted: a per-leader re-mint cannot dedup an
// already-committed entry through the ErrLeadershipLost "committed but ack lost"
// ambiguity. Derived only from inputs the seam re-sees on a reconnect/crash retry.
//
// SCOPE (external-review F1): provision and join carry NO forwarding ReqID. Their
// writes are `INSERT OR IGNORE` (structurally idempotent) AND their (sid,nid)/(sid)
// binding is explicitly DELETABLE by an operator node-evict / kick. A pure
// (sid,nid,fp) content key is therefore too coarse: a stale ledger row from the
// pre-evict provision would dedup-skip a LEGITIMATE post-evict re-provision (a new
// logical write), returning ok while the authoritative row stays absent. The agent
// cannot supply a per-attempt epoch (D3-R3: a deny is terminal → reconnect → fresh
// Handle, no surviving nonce), so no content key can distinguish "retry of THIS
// provision" from "re-provision after evict". The ack-lost retry is instead handled
// structurally (INSERT OR IGNORE + the handler's already-provisioned fast-path), so
// the dedup ledger is simply not needed for these verbs. Only RECONCILE carries a
// ReqID — bootID is a valid epoch and it protects D5's future audit-publish from
// double-publishing one logical register. The general ReqID dedup primitive (proven
// op-agnostically by TestD4FSMReqIDDedupSemantics) stays for reconcile + future
// non-idempotent forwarded ops.

// ReconcileReqID = hex(SHA256("reconcile" || sid || nid || bootID || digest(sorted
// LocalProcesses ++ sorted LocalPorts)))[:32]. bootID is the natural epoch: an agent
// restart -> new bootID -> new key -> fresh reconcile; an ack-lost retry of the same
// register -> identical inputs -> same key -> dedup.
// Pure request identity keeps the captured creation in the deduplication key.
// origin: hot-update S8 plan §6.2 / P16
func ReconcileReqID(sid, nid string, req proto.NodeRegisterReq) string {
	return upgradeauthority.ReconcileRequestID(sid, nid, req)
}

// ---- forwarder (originating broker side) ----

// Forwarder publishes a session-control write to the leader over the broker-only
// cluster.apply bus. Its NATS conn MUST authenticate with the broker bus nkey (RF1
// grants cluster.apply.> pub/sub only to broker nkey AuthUsers).
type Forwarder struct {
	nc      *nats.Conn
	timeout time.Duration
	// limiter is the D9 §18.2.18 mass-reconnect bound: a token bucket caps the rate at
	// which THIS broker forwards writes to the leader, so ~100 agents re-REGISTERing into
	// an in-progress election drain at ≤ K/sec instead of herding the new leader with a
	// burst of ReconcileBatch Proposes. A forward that can't get a token within the request
	// timeout returns the retriable not_leader sentinel → the agent's reconnect backoff
	// smooths the herd over time. nil ⇒ unlimited (tests that don't exercise the herd).
	limiter *rate.Limiter
	// observe, when set, receives (verb, err) for EVERY Forward outcome. It is the
	// tether_broker_raft_forward_total choke point (external review B2).
	//
	// It lives here, at the network boundary, rather than at the call sites, because the call
	// sites are exactly what failed: counting was done inside proposeOrForward, and FIVE senders
	// (alert signal, alert ack, transfer audit, provision, join) call Forward directly. Four of
	// them discard the error, and the counter's own godoc named the disk-alert one as the reason
	// the metric exists — so the single path the feature was justified by was the one it did not
	// count. Any future direct sender is now counted without knowing this field exists.
	//
	// nil ⇒ uncounted, which is only correct for a Forwarder no Broker owns (a unit test
	// exercising the wire shape). Broker.newForwarder and AttachAlertSink both install it.
	observe func(verb string, err error)
}

// defaultForwardRatePerSec bounds the leader-forward rate (§18.2.18). Generous enough not
// to impede steady-state writes, low enough that a 100-agent herd drains over ~2s (past a
// PreVote election) instead of in one burst. Burst == rate so a brief idle period lets a
// small batch through immediately.
const defaultForwardRatePerSec = 50

// NewForwarder builds a Forwarder over an already-connected broker-nkey NATS conn, with
// the default mass-reconnect rate limit.
func NewForwarder(nc *nats.Conn, timeout time.Duration) *Forwarder {
	return NewForwarderWithLimit(nc, timeout, defaultForwardRatePerSec)
}

// NewForwarderWithLimit is NewForwarder with an explicit per-second forward cap (tests use
// a low cap to assert the bound; perSec<=0 disables the limiter).
func NewForwarderWithLimit(nc *nats.Conn, timeout time.Duration, perSec int) *Forwarder {
	f := &Forwarder{nc: nc, timeout: timeout}
	if perSec > 0 {
		f.limiter = rate.NewLimiter(rate.Limit(perSec), perSec)
	}
	return f
}

// newForwarder is the BROKER-owned constructor: the returned Forwarder tallies every outcome into
// b.forwards. Production must use this, never the bare NewForwarder — see Forwarder.observe, and
// TestEveryBrokerOwnedForwarderIsObserved, which pins it structurally.
func (b *Broker) newForwarder(nc *nats.Conn, timeout time.Duration) *Forwarder {
	f := NewForwarder(nc, timeout)
	f.observe = b.countForward
	return f
}

// Forward sends one verb+payload and classifies the reply. Returns nil on ok,
// cluster.ErrForwardNotLeader on not_leader OR a NATS timeout/no-responder (a timeout
// is NOT proof of non-commit — the SAME ReqID dedups on retry), or a
// *ForwardBusinessError on a permanent business error. The caller re-calls with the
// SAME reqID on a retriable error; it MUST NOT mint a fresh key.
// It is a thin wrapper over forward so that EVERY one of that function's seven exits is observed
// through one deferred hook — an early `return` added later cannot escape the counter, which is
// precisely how the metric came to miss five senders.
func (f *Forwarder) Forward(verb, reqID string, payload []byte) (err error) {
	defer func() { f.observeOutcome(verb, err) }()
	return f.forward(verb, reqID, payload)
}

// observeOutcome reports one authoritative attempt through the Forwarder-owned
// observer. Besides Forward's network boundary, the provision/join seams use it
// for their direct leader-local Propose branch, which otherwise bypasses both
// Forward and Broker.proposeOrForward. A nil receiver/observer is deliberately
// safe for package fixtures and wire-only Forwarders.
func (f *Forwarder) observeOutcome(verb string, err error) {
	if f != nil && f.observe != nil {
		f.observe(verb, err)
	}
}

func (f *Forwarder) forward(verb, reqID string, payload []byte) error {
	// §18.2.18 mass-reconnect bound: acquire a forward token (bounded by the request
	// timeout). Exceeding the deadline is treated as retriable (not a commit) — the agent
	// backs off, smoothing the herd; the SAME reqID dedups any eventual double-commit.
	if f.limiter != nil {
		ctx, cancel := context.WithTimeout(context.Background(), f.timeout)
		err := f.limiter.Wait(ctx)
		cancel()
		if err != nil {
			return cluster.ErrForwardNotLeader
		}
	}
	data, err := json.Marshal(forwardEnvelope{ReqID: reqID, Verb: verb, Payload: payload})
	if err != nil {
		return fmt.Errorf("cluster_forward: marshal envelope: %w", err)
	}
	msg, err := f.nc.Request(proto.SubjClusterApply(verb), data, f.timeout)
	if err != nil {
		return cluster.ErrForwardNotLeader // timeout / no responder => retriable
	}
	var reply forwardReply
	if err := json.Unmarshal(msg.Data, &reply); err != nil {
		return cluster.ErrForwardNotLeader // malformed reply => retriable (no commit proof)
	}
	switch reply.Status {
	case fwdStatusOK:
		return nil
	case fwdStatusNotLeader:
		return cluster.ErrForwardNotLeader
	case fwdStatusError:
		return &ForwardBusinessError{Kind: reply.ErrKind, Msg: reply.ErrMsg}
	default:
		// An empty/unknown status is ambiguous — no commit proof — so fail in the
		// RETRIABLE direction (same as a timeout / malformed reply), never permanent
		// (review m5: don't let a responder bug strand a write as a permanent deny).
		return cluster.ErrForwardNotLeader
	}
}

// ---- leader responder ----

// SubscribeClusterApply wires the leader-only responder (see the file header for the
// broadcast / leader-only-reply rationale). It subscribes the broker-nkey conn to the
// cluster.apply.> wildcard; only when this node believes it is leader does it run the
// verb's leader-only Plan through node.ProposeWithReqID and reply. now is the leader's
// clock (it bakes its own time per §3.4). Constructed only when a cluster.Node exists.
// verifyPIN is this leader's budgeted Argon2 verifier (forwardDeps.verifyPIN). It is a
// FUNCTION rather than the handler itself because the auth_callout handler is built after
// this subscription is wired — the caller passes a late-binding closure so the two orders
// cannot produce a nil budget silently. origin: prerelease audit external review M-1.
func SubscribeClusterApply(
	nc *nats.Conn, node *cluster.Node, now func() time.Time, logger *slog.Logger,
	verifyPIN func(pin, hash string) error,
) (*nats.Subscription, error) {
	return subscribeClusterApplyOwned(nc, node, now, logger, verifyPIN, nil, nil)
}

func subscribeClusterApplyOwned(nc *nats.Conn, node *cluster.Node, now func() time.Time,
	logger *slog.Logger, verifyPIN func(string, string) error, owner *brokerAuthOwner, broker *Broker,
) (*nats.Subscription, error) {
	deps := forwardDeps{verifyPIN: verifyPIN, broker: broker}
	return nc.Subscribe(proto.SubjClusterApplyWildcard, func(msg *nats.Msg) {
		// Image shutdown must join the entire Plan/commit/readback borrower,
		// not merely the PIN verifier nested inside some forwarded verbs.
		// origin: hot-update S6 plan §8.1 / T29
		if owner != nil {
			if !owner.Enter() {
				return
			}
			defer owner.Leave()
		}
		if msg.Reply == "" {
			return
		}
		var env forwardEnvelope
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			return // drop malformed (no reply)
		}
		// The exact transfer responder owns these non-Raft operations. The
		// wildcard must stay silent even when this broker is leader, or its
		// unknown-verb reply can outrun the original follower's census proof.
		// origin: hot-update S7 plan §6.3 / T39
		if isTransferForwardVerb(env.Verb) {
			return
		}
		if !node.IsLeader() {
			return // stay silent; the leader (if any) answers
		}
		respondLogged(logger, nc, msg, marshalForwardReply(dispatchForward(node, now, deps, env)))
	})
}

// ErrReqIDNotAllowed is the permanent error the responder returns when a forwarded
// provision/join carries a non-empty ReqID. The "provision/join carry NO forwarding
// ReqID" contract (external F1: their writes are INSERT OR IGNORE + the binding is
// operator-deletable, so a content key would falsely dedup-skip a legitimate post-evict
// re-provision) is enforced HERE at the cluster.apply WIRE BOUNDARY — not only at the
// production seam — so a broker-bus caller, the generic Forwarder.Forward, or a future
// seam regression cannot reintroduce the stale-ledger false-success (external RF1).
var ErrReqIDNotAllowed = errors.New("cluster_forward: this verb must not carry a forwarding ReqID (idempotent + deletable binding)")

// verbAllowsReqID is the ALLOW-LIST of forward verbs that may carry a non-empty forwarding
// ReqID (CC-4 data-driven guard). Only VerbReconcile does — its bootID-epoch key dedups an
// ack-lost reconcile retry. Every OTHER verb forwards reqID="" (provision/join are structurally
// idempotent with operator-deletable bindings, so a content key would falsely dedup-skip a
// legitimate post-evict re-write — external F1/RF1; transfer-audit's key is content-derived
// INSIDE the Plan, not in env.ReqID). The default is REJECT, so a future verb that forgets to
// guard cannot reintroduce the stale-ledger false-success.
var verbAllowsReqID = map[string]bool{VerbReconcile: true}

// verbHandler decodes one forward envelope and runs the verb's leader-only Plan.
type verbHandler func(node *cluster.Node, now func() time.Time, env forwardEnvelope, deps forwardDeps) error

// forwardDeps carries the leader-side collaborators a forwarded verb needs beyond the raft
// node and the clock. Originally that was exactly one thing, and it is here rather than closed
// over by the table because the table is a package-level var: a closure would have to be
// installed by mutating global state at wiring time, which is how a second broker in the
// same test binary would silently share (or clobber) the first one's budget.
//
// origin: prerelease audit external review M-1.
type forwardDeps struct {
	// verifyPIN runs Argon2 against the process-wide ceiling OF THIS LEADER. It is the
	// authcallout Handler's VerifyPINWithBudget in production. A nil value is NOT a
	// fallback to auth.VerifyPIN — see errPINVerifierUnwired: silently degrading to the
	// unbudgeted verifier is precisely the defect this plumbing exists to remove, and it
	// would come back invisibly the first time someone adds a caller that forgets it.
	verifyPIN func(pin, hash string) error
	// Production cold/hot responders also own external session teardown. Bare
	// SubscribeClusterApply remains the storage-only adapter used by cluster fixtures.
	// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2
	broker *Broker
}

// errPINVerifierUnwired fails a forwarded PIN write CLOSED when no budgeted verifier was
// supplied. A PIN bootstrap refused this way is retriable and costs one join attempt; the
// alternative — running Argon2 anyway — is an unmetered CPU oracle on the public auth
// surface, which is the whole finding.
var errPINVerifierUnwired = errors.New("cluster_forward: no budgeted PIN verifier wired for the leader path")

// requireVerifier returns deps.verifyPIN or the fail-closed error.
func (d forwardDeps) requireVerifier() (func(pin, hash string) error, error) {
	if d.verifyPIN == nil {
		return nil, errPINVerifierUnwired
	}
	return d.verifyPIN, nil
}

// propose builds a handler that decodes payload type P and proposes plan against the LEADER's
// committed view. The generic parameter is what keeps the table type-safe: before this, every arm
// spelled out its own `var p T; json.Unmarshal(...); node.Propose(...)` and the only thing keeping
// the decode type and the Plan call in agreement was reading them side by side.
//
// The plan closure receives (db, p, now) and may ignore any of them — several verbs need no db
// (nodepkg.PlanEvict) or no now (proc.PlanInsert), and forcing them into a narrower signature
// would mean several builders instead of one.
func propose[P any](plan func(*sql.DB, P, time.Time) (*cluster.Command, error)) verbHandler {
	return func(node *cluster.Node, now func() time.Time, env forwardEnvelope, _ forwardDeps) error {
		var p P
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return plan(db, p, now())
		})
	}
}

// proposeWithPIN is propose() for the two verbs whose Plan runs Argon2. It is a SEPARATE
// builder for the same reason proposeWithReqID is: the table should show which verbs spend
// the process-wide PIN budget, rather than hiding it in an optional argument that a new
// entry can omit and still compile. origin: prerelease audit external review M-1.
func proposeWithPIN[P any](
	plan func(*sql.DB, P, func(pin, hash string) error, time.Time) (*cluster.Command, error),
) verbHandler {
	return func(node *cluster.Node, now func() time.Time, env forwardEnvelope, deps forwardDeps) error {
		verify, err := deps.requireVerifier()
		if err != nil {
			return err
		}
		return propose(func(db *sql.DB, p P, at time.Time) (*cluster.Command, error) { return plan(db, p, verify, at) })(node, now, env, deps)
	}
}

// proposeWithReqID is propose() through node.ProposeWithReqID, for the one verb that carries an
// idempotency key minted at the wire boundary (reconcile, whose bootID-epoch key dedups a
// forwarder retry). It is a SEPARATE builder rather than a flag on propose() so that the choice
// is visible in the table: a verb either has a boundary-minted ReqID or it does not, and
// verbAllowsReqID must agree.
func proposeWithReqID[P any](plan func(*sql.DB, P, time.Time) (*cluster.Command, error)) verbHandler {
	return func(node *cluster.Node, now func() time.Time, env forwardEnvelope, _ forwardDeps) error {
		var p P
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return node.ProposeWithReqID(env.ReqID, func(db *sql.DB) (*cluster.Command, error) {
			return plan(db, p, now())
		})
	}
}

// writeVerbs is the leader-side dispatch table. It replaced a 17-arm switch (batch B, B2).
//
// WHAT DID NOT CHANGE, AND MUST NOT
//
//   - The CC-4 ReqID gate still runs at the WIRE BOUNDARY, before this table is consulted.
//     dispatchForward's comment explains why; moving it inside a handler would let a future verb
//     reintroduce the external-F1 stale-ledger false-success.
//   - The unknown-verb error string is byte-identical. It reaches an operator terminal through
//     cluster_health.go's alert-ack forward.
//   - Every payload type and every Plan call is the same one the corresponding switch arm used.
//     internal/broker/wire_freeze_test.go re-derives the type set from THIS table and requires
//     each to be frozen, so a new entry cannot skip the wire freeze.
var writeVerbs = map[string]verbHandler{
	VerbProvision: proposeWithPIN(func(db *sql.DB, p ProvisionPayload, verify func(pin, hash string) error, now time.Time) (*cluster.Command, error) {
		return agentprov.PlanProvisionWithPIN(db, p.SID, p.NID, p.FP, p.PIN, verify, now)
	}),
	VerbJoin: proposeWithPIN(func(db *sql.DB, p JoinPayload, verify func(pin, hash string) error, now time.Time) (*cluster.Command, error) {
		return session.PlanJoinWithPIN(db, p.SID, p.FP, p.PIN, verify, now)
	}),
	VerbReconcile: proposeWithReqID(func(db *sql.DB, p ReconcilePayload, now time.Time) (*cluster.Command, error) {
		procs, err := proc.ListBySessionFiltered(db, p.SID, proc.ListBySessionOpts{IncludeExited: false})
		if err != nil {
			return nil, err
		}
		ports, err := port.ListBySession(db, p.SID)
		if err != nil {
			return nil, err
		}
		return planReconcileAuthority(db, p, procs, ports, now)
	}),
	// D8a §9: PlanTransferAudit sets the content-derived ReqID itself; node.Propose PRESERVES it
	// (ProposeWithReqID only overwrites a NON-empty arg), so the 0011 ledger dedups a forwarder
	// retry. No env.ReqID gate — the key is a pure function of the payload, not minted here.
	VerbTransferAudit: propose(func(_ *sql.DB, rec schema.AuditTransfer, _ time.Time) (*cluster.Command, error) {
		return xferaudit.PlanTransferAudit(rec)
	}),
	// D8b §10.2: read the CURRENT ACTIVE state and commit a raise/clear ONLY on a transition — a
	// nil command (no transition) issues no raft write, so a broker's every-tick re-assert is free
	// and a dropped clear self-heals next tick.
	VerbAlertSignal: propose(planAlertSignal),
	// D8b §10.1: cluster-level ack (idempotent UPSERT). User-initiated + rare, so no transition
	// gate is needed (unlike the periodic signal).
	// The leader also checks the key names an alert that EXISTS. Acking a key with no
	// alert behind it wrote a permanent alert_acks row for something that never
	// happened; the table is replicated and replayed on restore, so nothing ever
	// removed it (prerelease audit broker-cluster-ops/L3-F2, second half).
	//
	// QueryRow().Scan(), not Query(): the Propose closure contract (cluster/node.go)
	// requires every row be materialised and the Rows closed before the Command is
	// returned.
	//
	// NOT the generic propose(): the existence check needs the NODE, to ask whether this
	// leader has applied what it has committed. origin: prerelease audit round 2, G-7.
	// A just-elected leader replaying a long tail is leader before its FSM has caught up,
	// so `SELECT 1 FROM alerts` can miss an alert that IS committed — and the check then
	// turns a legitimate ack into a permanent refusal naming a key the operator can see in
	// `cluster alerts ls`. internal/cluster/read.go argues that predicate and its two
	// honest limits; the large-replay case, which is exactly this one, is the case it
	// closes.
	VerbAlertAck: func(node *cluster.Node, now func() time.Time, env forwardEnvelope, _ forwardDeps) error {
		var p AlertAckPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		if !node.CaughtUp() {
			// TRANSIENT, not a refusal. The ctl's exit-class table maps
			// leader_unavailable to 75 (retry-later), which is the honest answer: we
			// cannot yet tell whether the alert exists.
			return fmt.Errorf("alert ack: %w", cluster.ErrForwardNotLeader)
		}
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			var one int
			switch err := db.QueryRow(`SELECT 1 FROM alerts WHERE dedup_key=? LIMIT 1`, p.DedupKey).Scan(&one); {
			case err == nil:
			case errors.Is(err, sql.ErrNoRows):
				return nil, fmt.Errorf("alert ack: no alert with dedup_key %q", p.DedupKey)
			default:
				return nil, fmt.Errorf("alert ack: look up dedup_key: %w", err)
			}
			return cluster.PlanAlertAck(p.DedupKey, p.AckedBy, now())
		})
	},
	// D9 §3: the leader bakes created_at via now() and commits OpSessionCreate; PlanCreate
	// re-checks existence on the LEADER's committed view (ErrAlreadyExists surfaces as a typed
	// forward error). SID == Name (name-keyed sessions).
	// THE LEADER RE-CHECKS ADMISSION, and the origin broker's check is not a substitute.
	//
	// origin: prerelease audit external review R2-B2. `MayCreateSession` ran only in the
	// handler of the broker that received the client, and this dispatch went straight to
	// `PlanCreate`. Two ways that is bypassed, both routine rather than exotic:
	//
	//   - an N-1 broker has NO admission check in its binary at all, yet forwards as a
	//     trusted peer — so during the documented rolling-upgrade window, any anonymous
	//     nkey reaching an old broker becomes the owner of a new session on the new leader;
	//   - a CURRENT follower whose policy view lags a revocation forwards a create for a
	//     fingerprint the leader has already revoked.
	//
	// Both are the same mistake as B-2's empty SID: reading TRANSPORT identity (a peer
	// inside the mTLS/system-account boundary) as BUSINESS authorization. The authoritative
	// answer lives in the leader's committed DB, which is exactly what this closure's `db`
	// is, so the check belongs here — before PlanCreate, not instead of the origin check.
	// The origin keeps its own copy as a cheap early refusal that saves a round trip.
	VerbSessionCreate: propose(func(db *sql.DB, p SessionCreatePayload, now time.Time) (*cluster.Command, error) {
		return planAuthorizedSessionCreate(db, p.Name, p.FP, p.PinHash, now)
	}),
	// C3: each broker self-reports its bus nkey pub; the leader bakes the all-literal UPDATE +
	// topology_generation bump (PlanClusterBusNkeySet re-validates the nkey on the leader).
	VerbBusNkeySet: propose(func(_ *sql.DB, p BusNkeySetPayload, now time.Time) (*cluster.Command, error) {
		return cluster.PlanClusterBusNkeySet(p.NodeID, p.BusNkeyPub, now)
	}),
	VerbPortFree: propose(func(db *sql.DB, p PortMutatePayload, now time.Time) (*cluster.Command, error) {
		return port.PlanFree(db, p.Port, now)
	}),
	// Both allocation verbs rebuild through PortFreeAllocationPayload.allocation() — the SAME
	// named projection the originating side applies (see allocIdentity). Spelling the rebuild out
	// per arm is what let the two sides drift in the first place.
	VerbPortFreeAllocation: propose(func(db *sql.DB, p PortFreeAllocationPayload, now time.Time) (*cluster.Command, error) {
		return port.PlanFreeAllocation(db, p.allocation(), now)
	}),
	VerbPortRevoke: propose(func(db *sql.DB, p PortFreeAllocationPayload, now time.Time) (*cluster.Command, error) {
		return port.PlanRevokeAllocation(db, p.allocation(), now)
	}),
	VerbNodeRegister: propose(nodepkg.PlanRegister),
	VerbProcInsert: propose(func(db *sql.DB, p proc.Process, _ time.Time) (*cluster.Command, error) {
		return proc.PlanInsert(db, p)
	}),
	VerbProcMarkExited: propose(func(db *sql.DB, p ProcMarkExitedPayload, _ time.Time) (*cluster.Command, error) {
		return planProcExitAuthority(db, p)
	}),
	VerbSessionTombstone: propose(planSessionTombstoneAuthority),
	VerbSessionDrop: propose(func(db *sql.DB, p SessionMutatePayload, _ time.Time) (*cluster.Command, error) {
		return planSessionDropAuthority(db, p)
	}),
	VerbSessionCreatorSet: propose(func(_ *sql.DB, p SessionCreatorPayload, now time.Time) (*cluster.Command, error) {
		return session.PlanSetCreator(p.FP, p.AddedBy, p.Note, p.Allow, now)
	}),
	VerbNodeEvict: propose(func(_ *sql.DB, p EvictPayload, _ time.Time) (*cluster.Command, error) {
		return nodepkg.PlanEvict(p.SID, p.NID)
	}),
}

// dispatchForward runs the verb's leader-only Plan on the leader. Plan reads the LEADER DB.
func dispatchForward(node *cluster.Node, now func() time.Time, deps forwardDeps, env forwardEnvelope) error {
	// CC-4: enforce the no-ReqID contract for EVERY verb not on the allow-list, at the wire
	// boundary, so a future verb cannot reintroduce the external-F1 stale-ledger false-success.
	// This stays BEFORE the table lookup — see writeVerbs' doc.
	if env.ReqID != "" && !verbAllowsReqID[env.Verb] {
		return ErrReqIDNotAllowed
	}
	// The follower cannot hold the leader's applyMu while deleting history/OBJ.
	// Run the captured cascade on this leader before committing the guarded SQL;
	// forwarding only the final DELETE would strand streams or race recreation.
	// origin: S7 internal review BROKER-REDS F3; hot-update S8 plan §6.2
	if env.Verb == VerbSessionDrop && deps.broker != nil {
		var p SessionMutatePayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		if p.Authority == nil || p.Authority.SID != p.SID {
			return upgradeauthority.Match("", "")
		}
		d, err := controlbudget.After(5 * time.Second)
		if err != nil {
			return err
		}
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return planSessionCascadeAuthority(deps.broker, db, d, nil, *p.Authority)
		})
	}
	h, ok := writeVerbs[env.Verb]
	if !ok {
		return fmt.Errorf("cluster_forward: unknown verb %q", env.Verb)
	}
	return h(node, now, env, deps)
}

// marshalForwardReply maps a Propose result to the typed reply envelope.
func marshalForwardReply(err error) []byte {
	var reply forwardReply
	switch {
	case err == nil:
		reply = forwardReply{Status: fwdStatusOK}
	case cluster.IsNotLeader(err):
		reply = forwardReply{Status: fwdStatusNotLeader}
	default:
		reply = forwardReply{Status: fwdStatusError, ErrKind: forwardErrKind(err), ErrMsg: err.Error()}
	}
	b, _ := json.Marshal(reply)
	return b
}

// ---- authcallout PIN-write seams (transparent follower->leader forwarding) ----

// NewProvisionSeam returns an authcallout.Handler.ProvisionAgentWrite seam: leader =>
// Propose locally; follower => Forward, mapping cluster.ErrForwardNotLeader ->
// authcallout.ErrNotLeader (transient deny). nil in production (= today's direct
// ProvisionWithPIN). Proves D3-R3's deferred transparent PIN path: a healthy follower
// forwards so the agent never sees not_leader on the happy path.
//
// NO forwarding ReqID (external-review F1): the provision write is INSERT OR IGNORE
// (idempotent) and the binding is operator-deletable (node-evict), so a content key
// would dedup-skip a legitimate post-evict re-provision. The ack-lost retry is handled
// structurally (idempotent INSERT + the handler's already-provisioned fast-path). See
// the ReqID-minting block comment.
// verifyPIN is the handler's budgeted verifier; the leader-local branch MUST use it rather
// than auth.VerifyPIN, or the broker that actually burns the CPU never charges for it
// (prerelease audit external review M-1).
func NewProvisionSeam(
	node *cluster.Node, fwd *Forwarder, verifyPIN func(pin, hash string) error,
) func(sid, nid, fp, pin string, t time.Time) error {
	return func(sid, nid, fp, pin string, t time.Time) error {
		if verifyPIN == nil {
			return errPINVerifierUnwired
		}
		if node.IsLeader() {
			// Leader-local: map a leadership loss racing between IsLeader() and the
			// Propose gate / Apply (raft.ErrNotLeader / ErrLeadershipLost) to the
			// transient sentinel, mirroring the forward branch (review M1 — the seam
			// is the single raft->authcallout translation point; both branches must map).
			err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
				return agentprov.PlanProvisionWithPIN(db, sid, nid, fp, pin, verifyPIN, t)
			})
			fwd.observeOutcome(VerbProvision, err)
			return pinWriteError(err)
		}
		payload, err := json.Marshal(ProvisionPayload{SID: sid, NID: nid, FP: fp, PIN: pin})
		if err != nil {
			return err
		}
		return pinWriteError(fwd.Forward(VerbProvision, "", payload))
	}
}

// NewJoinSeam returns an authcallout.Handler.JoinMemberWrite seam (PIN-join member),
// same leader-local / forward shape as NewProvisionSeam — and likewise carries NO
// forwarding ReqID (INSERT OR IGNORE members + deletable membership; external F1).
func NewJoinSeam(
	node *cluster.Node, fwd *Forwarder, verifyPIN func(pin, hash string) error,
) func(sid, fp, pin string, t time.Time) error {
	return func(sid, fp, pin string, t time.Time) error {
		if verifyPIN == nil {
			return errPINVerifierUnwired
		}
		if node.IsLeader() {
			err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
				return session.PlanJoinWithPIN(db, sid, fp, pin, verifyPIN, t)
			})
			fwd.observeOutcome(VerbJoin, err)
			return pinWriteError(err)
		}
		payload, err := json.Marshal(JoinPayload{SID: sid, FP: fp, PIN: pin})
		if err != nil {
			return err
		}
		return pinWriteError(fwd.Forward(VerbJoin, "", payload))
	}
}

// Both PIN seams translate the same local/forwarded leadership failures; share
// that projection so session cascade forwarding does not grow broker code.
// origin: S7 internal review BROKER-REDS F3; hot-update S7 plan §11.1
func pinWriteError(err error) error {
	if cluster.IsNotLeader(err) || errors.Is(err, cluster.ErrForwardNotLeader) {
		return authcallout.ErrNotLeader
	}
	return err
}

// Rebind only after the new current store/node and NATS connection are owned by
// P11. No old forwarder, read handle or verifier survives into the new seams.
// The apply responder precedes auth so forwarded PIN joins never depend on
// late business/JetStream wiring. P11 retains this owner and fences/joins it
// with stopBrokerImageAuth before closing the current store.
// origin: hot-update S6 plan §8.1 / T29
func rebindBrokerImageAuthority(b *Broker, nc *nats.Conn, deadline controlbudget.Deadline) (*brokerAuthOwner, error) {
	if b == nil || nc == nil || !nc.IsConnected() || deadline == 0 {
		return nil, fmt.Errorf("broker: missing current authority connection or deadline")
	}
	if _, err := brokerAuthRemaining(deadline); err != nil {
		return nil, err
	}
	b.leaderPINVerify.Store(nil)
	if !b.clusterMode {
		if b.read().SQL() == nil {
			return nil, fmt.Errorf("broker: missing current single store")
		}
		return nil, nil
	}
	if b.cl == nil || b.cl.node == nil || b.cl.node.RODB() == nil {
		return nil, fmt.Errorf("broker: missing current cluster store")
	}
	bindClusterReadDB(b, b.cl.node.RODB())
	b.cl.forwarder = b.newForwarder(nc, b.cfg.ExposeForwardTimeout())
	owner := newBrokerAuthOwner(b, deadline)
	sub, err := subscribeClusterApplyOwned(nc, b.cl.node, b.cfg.Now, b.cfg.Logger,
		func(pin, hash string) error { return verifyPINOnLeader(b, pin, hash) }, owner, b)
	if err != nil {
		return nil, err
	}
	owner.Sub = sub
	b.cl.subs = append(b.cl.subs, sub)
	sub.SetClosedHandler(func(string) { owner.Finish() })
	timeout, err := brokerAuthRemaining(deadline)
	if err == nil {
		err = nc.FlushTimeout(timeout)
	}
	if err != nil {
		return owner, errors.Join(err, stopBrokerImageAuth(owner, deadline))
	}
	return owner, nil
}

func finishSessionAuthority(b *Broker, d controlbudget.Deadline, stop <-chan struct{}, guard upgradeauthority.Guard, drop func() error) error {
	return xferauth.CascadeSession(d, stop, brokerJS(b), guard.SID, func(string) error {
		// Prune the tunnel's per-session kill-generation bookkeeping now that the
		// session is permanently gone (self-review: bounds killGenSession growth).
		// Hold the old tombstone until this retirement finishes; after SQL commit
		// a new creation could already own a distinct domain with the same SID.
		if srv := b.tunnelSrv.Load(); srv != nil {
			srv.ForgetSession(guard.SID)
		}
		// Tunnel retirement occurs while the old tombstone is still held.
		// The completion event is published only after the actual SQL commit.
		return drop()
	})
}

// The external cascade and the SQL plan share the existing applyMu boundary.
// origin: hot-update S8 plan §6.2 / P16
func planSessionCascadeAuthority(b *Broker, db *sql.DB, d controlbudget.Deadline, stop <-chan struct{}, guard upgradeauthority.Guard) (*cluster.Command, error) {
	cmd, err := planSessionDropAuthority(db, SessionMutatePayload{SID: guard.SID, Authority: &guard})
	if err != nil {
		return nil, err
	}
	// A successful read confirms only the prepared guard; actual SQL still
	// applies through the session owner after external destruction completes.
	err = finishSessionAuthority(b, d, stop, guard, func() error { _, err := upgradeauthority.ReadCreation(db, guard); return err })
	return cmd, err
}

// Shared JSON egress keeps the original marshal/error and respondBytes path.
// origin: hot-update S8 plan §6.2 / P16 budget responsibility move
func replyJSONValue(b *Broker, msg *nats.Msg, v any) {
	payload, err := json.Marshal(v)
	if err != nil {
		b.cfg.Logger.Warn("broker: marshal reply", "err", err, "subject", msg.Subject)
		return
	}
	b.respondBytes(msg, payload)
}

// Compare before interpreting DELETING as a retry: the new creation can itself
// be tombstoned, and that does not give an old remover its settlement authority.
// origin: hot-update S8 plan §6.2 / P16
func planSessionTombstoneAuthority(db *sql.DB, p SessionMutatePayload, now time.Time) (*cluster.Command, error) {
	predicate, err := upgradeauthority.MutationPredicate(db, upgradeauthority.SessionMutation(p), cluster.LitText)
	if err != nil {
		return nil, err
	}
	cmd, err := session.PlanTombstone(db, p.SID, now)
	if err == nil {
		cmd.Body[0].SQL += ` AND ` + predicate
	}
	return cmd, err
}

// origin: S7 internal review GATES-PROD F5
func markProcExitAuthority(b *Broker, p ProcMarkExitedPayload) error {
	if p.Authority != nil && (p.Authority.SID != p.Sid || p.Authority.Incarnation == "") {
		return upgradeauthority.Match("", "")
	}
	if db, ok := b.singleWriter(); ok {
		if p.Authority != nil {
			_, err := proc.MarkExitedGuarded(db, *p.Authority, p.Pid, p.ExitCode, p.EndedAt)
			return err
		}
		return proc.MarkExited(db, p.Pid, p.Sid, p.ExitCode, p.EndedAt)
	}
	return proposeValue(b, VerbProcMarkExited, p, func(db *sql.DB) (*cluster.Command, error) {
		return planProcExitAuthority(db, p)
	})
}

func planSessionDropAuthority(db *sql.DB, p SessionMutatePayload) (*cluster.Command, error) {
	if p.Authority == nil || p.Authority.SID != p.SID {
		return nil, upgradeauthority.Match("", "")
	}
	_, cmd, err := session.PlanHardDeleteGuarded(db, *p.Authority)
	return cmd, err
}

// A completion notification cannot precede SQL commit. Receivers retain the
// incarnation; receivers must compare it before revoking a same-SID domain.
// origin: hot-update S8 plan §6.2 / P16
func publishSessionDestroyedAuthority(b *Broker, g upgradeauthority.Guard, err error) error {
	if err == nil {
		b.pubSysEvent("session_destroyed", map[string]any{"sid": g.SID, "incarnation": g.Incarnation})
		b.cfg.Logger.Info("broker: session removed", "sid", g.SID)
	}
	return err
}
