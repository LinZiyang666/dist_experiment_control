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
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/cluster"
	nodepkg "github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/xferaudit"
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
type PortFreeAllocationPayload struct {
	Port      int    `json:"port"`
	SID       string `json:"sid"`
	NID       string `json:"nid"`
	Name      string `json:"name"`
	TokenHash string `json:"token_hash"`
}

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
	return PortFreeAllocationPayload{
		Port: a.Port, SID: a.SID, NID: a.NID, Name: a.Name, TokenHash: a.TokenHash,
	}
}

// allocation rebuilds the narrowed port.Allocation. It is the SAME projection
// dispatchForward performs on the receiving side, named once so the two sides cannot
// drift apart again.
func (p PortFreeAllocationPayload) allocation() port.Allocation {
	return port.Allocation{
		Port: p.Port, SID: p.SID, NID: p.NID, Name: p.Name, TokenHash: p.TokenHash,
	}
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
type SessionMutatePayload struct {
	SID string `json:"sid"`
}

// ProcMarkExitedPayload (D9 round-1 BLOCKER) carries an agent-reported process exit. The
// exit code + end time are FACTS from the agent (not leader-baked); PlanMarkExited's
// WHERE status='RUNNING' guard makes the Apply idempotent on an already-exited row.
type ProcMarkExitedPayload struct {
	Pid      string    `json:"pid"`
	ExitCode int       `json:"exit_code"`
	EndedAt  time.Time `json:"ended_at"`
}

// EvictPayload (D9 round-2 BLOCKER) carries the (sid,nid) of an `admin evict` kick.
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
func ReconcileReqID(sid, nid string, req proto.NodeRegisterReq) string {
	h := sha256.New()
	for _, s := range []string{VerbReconcile, sid, nid, req.BootID} {
		writeSeg(h, s)
	}
	// Total order over ALL hashed fields (not just PID/Port) so the digest is stable
	// even when the agent reports duplicate-key entries in a map-randomized order
	// (review M5: a PID/Port-only sort.Slice is unstable for equal keys, which would
	// derive different keys for the same logical request on retry).
	lps := append([]proto.LocalProcess(nil), req.LocalProcesses...)
	sort.SliceStable(lps, func(i, j int) bool {
		a, b := lps[i], lps[j]
		if a.PID != b.PID {
			return a.PID < b.PID
		}
		if a.State != b.State {
			return a.State < b.State
		}
		if a.StartTimeTicks != b.StartTimeTicks {
			return a.StartTimeTicks < b.StartTimeTicks
		}
		return rcKey(a.RC) < rcKey(b.RC)
	})
	for _, lp := range lps {
		for _, s := range []string{lp.PID, lp.State, strconv.FormatInt(lp.StartTimeTicks, 10), rcKey(lp.RC)} {
			writeSeg(h, s)
		}
	}
	lpo := append([]proto.LocalPort(nil), req.LocalPorts...)
	sort.SliceStable(lpo, func(i, j int) bool {
		a, b := lpo[i], lpo[j]
		if a.Port != b.Port {
			return a.Port < b.Port
		}
		if a.LocalPort != b.LocalPort {
			return a.LocalPort < b.LocalPort
		}
		if a.Name != b.Name {
			return a.Name < b.Name
		}
		return a.TokenHash < b.TokenHash
	})
	for _, p := range lpo {
		for _, s := range []string{strconv.Itoa(p.Port), p.Name, strconv.Itoa(p.LocalPort), p.TokenHash} {
			writeSeg(h, s)
		}
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// rcKey renders an optional rc for hashing/sorting: "" when nil, else the decimal.
func rcKey(rc *int) string {
	if rc == nil {
		return ""
	}
	return strconv.Itoa(*rc)
}

// writeSeg writes a NUL-terminated segment so concatenation is unambiguous (no
// "ab"+"c" == "a"+"bc" collision).
func writeSeg(h interface{ Write([]byte) (int, error) }, s string) {
	_, _ = h.Write([]byte(s))
	_, _ = h.Write([]byte{0})
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
func SubscribeClusterApply(nc *nats.Conn, node *cluster.Node, now func() time.Time) (*nats.Subscription, error) {
	return nc.Subscribe(proto.SubjClusterApplyWildcard, func(msg *nats.Msg) {
		if msg.Reply == "" {
			return
		}
		var env forwardEnvelope
		if err := json.Unmarshal(msg.Data, &env); err != nil {
			return // drop malformed (no reply)
		}
		if !node.IsLeader() {
			return // stay silent; the leader (if any) answers
		}
		_ = msg.Respond(marshalForwardReply(dispatchForward(node, now, env)))
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
type verbHandler func(node *cluster.Node, now func() time.Time, env forwardEnvelope) error

// propose builds a handler that decodes payload type P and proposes plan against the LEADER's
// committed view. The generic parameter is what keeps the table type-safe: before this, every arm
// spelled out its own `var p T; json.Unmarshal(...); node.Propose(...)` and the only thing keeping
// the decode type and the Plan call in agreement was reading them side by side.
//
// The plan closure receives (db, p, now) and may ignore any of them — several verbs need no db
// (nodepkg.PlanEvict) or no now (proc.PlanInsert), and forcing them into a narrower signature
// would mean several builders instead of one.
func propose[P any](plan func(*sql.DB, P, time.Time) (*cluster.Command, error)) verbHandler {
	return func(node *cluster.Node, now func() time.Time, env forwardEnvelope) error {
		var p P
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return plan(db, p, now())
		})
	}
}

// proposeWithReqID is propose() through node.ProposeWithReqID, for the one verb that carries an
// idempotency key minted at the wire boundary (reconcile, whose bootID-epoch key dedups a
// forwarder retry). It is a SEPARATE builder rather than a flag on propose() so that the choice
// is visible in the table: a verb either has a boundary-minted ReqID or it does not, and
// verbAllowsReqID must agree.
func proposeWithReqID[P any](plan func(*sql.DB, P, time.Time) (*cluster.Command, error)) verbHandler {
	return func(node *cluster.Node, now func() time.Time, env forwardEnvelope) error {
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
	VerbProvision: propose(func(db *sql.DB, p ProvisionPayload, now time.Time) (*cluster.Command, error) {
		return agentprov.PlanProvisionWithPIN(db, p.SID, p.NID, p.FP, p.PIN, auth.VerifyPIN, now)
	}),
	VerbJoin: propose(func(db *sql.DB, p JoinPayload, now time.Time) (*cluster.Command, error) {
		return session.PlanJoinWithPIN(db, p.SID, p.FP, p.PIN, auth.VerifyPIN, now)
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
		return proc.PlanReconcileBatch(resolveReconcile(p.SID, p.NID, p.Req, procs, ports, now))
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
	VerbAlertSignal: propose(func(db *sql.DB, p AlertSignalPayload, now time.Time) (*cluster.Command, error) {
		return planAlertSignal(db, p, now)
	}),
	// D8b §10.1: cluster-level ack (idempotent UPSERT). User-initiated + rare, so no transition
	// gate is needed (unlike the periodic signal).
	VerbAlertAck: propose(func(_ *sql.DB, p AlertAckPayload, now time.Time) (*cluster.Command, error) {
		return cluster.PlanAlertAck(p.DedupKey, p.AckedBy, now)
	}),
	// D9 §3: the leader bakes created_at via now() and commits OpSessionCreate; PlanCreate
	// re-checks existence on the LEADER's committed view (ErrAlreadyExists surfaces as a typed
	// forward error). SID == Name (name-keyed sessions).
	VerbSessionCreate: propose(func(db *sql.DB, p SessionCreatePayload, now time.Time) (*cluster.Command, error) {
		return session.PlanCreate(db, p.Name, p.Name, p.FP, p.PinHash, now)
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
	VerbNodeRegister: propose(func(db *sql.DB, in nodepkg.RegisterInput, now time.Time) (*cluster.Command, error) {
		return nodepkg.PlanRegister(db, in, now)
	}),
	VerbProcInsert: propose(func(db *sql.DB, p proc.Process, _ time.Time) (*cluster.Command, error) {
		return proc.PlanInsert(db, p)
	}),
	VerbProcMarkExited: propose(func(db *sql.DB, p ProcMarkExitedPayload, _ time.Time) (*cluster.Command, error) {
		return proc.PlanMarkExited(db, p.Pid, p.ExitCode, p.EndedAt)
	}),
	VerbSessionTombstone: propose(func(db *sql.DB, p SessionMutatePayload, now time.Time) (*cluster.Command, error) {
		return session.PlanTombstone(db, p.SID, now)
	}),
	VerbSessionDrop: propose(func(db *sql.DB, p SessionMutatePayload, _ time.Time) (*cluster.Command, error) {
		return session.PlanHardDelete(db, p.SID)
	}),
	VerbNodeEvict: propose(func(_ *sql.DB, p EvictPayload, _ time.Time) (*cluster.Command, error) {
		return nodepkg.PlanEvict(p.SID, p.NID)
	}),
}

// dispatchForward runs the verb's leader-only Plan on the leader. Plan reads the LEADER DB.
func dispatchForward(node *cluster.Node, now func() time.Time, env forwardEnvelope) error {
	// CC-4: enforce the no-ReqID contract for EVERY verb not on the allow-list, at the wire
	// boundary, so a future verb cannot reintroduce the external-F1 stale-ledger false-success.
	// This stays BEFORE the table lookup — see writeVerbs' doc.
	if env.ReqID != "" && !verbAllowsReqID[env.Verb] {
		return ErrReqIDNotAllowed
	}
	h, ok := writeVerbs[env.Verb]
	if !ok {
		return fmt.Errorf("cluster_forward: unknown verb %q", env.Verb)
	}
	return h(node, now, env)
}

// isLeaderUnavailable reports whether err is the transient "no leader committed this write
// right now" signal (cluster.ErrForwardNotLeader — returned by proposeOrForward on a
// leadership race / election). Handlers in broker.go (which deliberately does NOT import
// internal/cluster — L-2) use this to reply proto.CodeLeaderUnavailable so the client retries
// instead of treating a routine raft failover as a permanent rejection (audit M2 /
// write-forward F3).
func isLeaderUnavailable(err error) bool {
	return errors.Is(err, cluster.ErrForwardNotLeader)
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
func NewProvisionSeam(node *cluster.Node, fwd *Forwarder) func(sid, nid, fp, pin string, t time.Time) error {
	return func(sid, nid, fp, pin string, t time.Time) error {
		if node.IsLeader() {
			// Leader-local: map a leadership loss racing between IsLeader() and the
			// Propose gate / Apply (raft.ErrNotLeader / ErrLeadershipLost) to the
			// transient sentinel, mirroring the forward branch (review M1 — the seam
			// is the single raft->authcallout translation point; both branches must map).
			err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
				return agentprov.PlanProvisionWithPIN(db, sid, nid, fp, pin, auth.VerifyPIN, t)
			})
			fwd.observeOutcome(VerbProvision, err)
			if err != nil {
				if cluster.IsNotLeader(err) {
					return authcallout.ErrNotLeader
				}
				return err
			}
			return nil
		}
		payload, err := json.Marshal(ProvisionPayload{SID: sid, NID: nid, FP: fp, PIN: pin})
		if err != nil {
			return err
		}
		if err := fwd.Forward(VerbProvision, "", payload); err != nil {
			if errors.Is(err, cluster.ErrForwardNotLeader) {
				return authcallout.ErrNotLeader
			}
			return err
		}
		return nil
	}
}

// NewJoinSeam returns an authcallout.Handler.JoinMemberWrite seam (PIN-join member),
// same leader-local / forward shape as NewProvisionSeam — and likewise carries NO
// forwarding ReqID (INSERT OR IGNORE members + deletable membership; external F1).
func NewJoinSeam(node *cluster.Node, fwd *Forwarder) func(sid, fp, pin string, t time.Time) error {
	return func(sid, fp, pin string, t time.Time) error {
		if node.IsLeader() {
			err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
				return session.PlanJoinWithPIN(db, sid, fp, pin, auth.VerifyPIN, t)
			})
			fwd.observeOutcome(VerbJoin, err)
			if err != nil {
				if cluster.IsNotLeader(err) {
					return authcallout.ErrNotLeader
				}
				return err
			}
			return nil
		}
		payload, err := json.Marshal(JoinPayload{SID: sid, FP: fp, PIN: pin})
		if err != nil {
			return err
		}
		if err := fwd.Forward(VerbJoin, "", payload); err != nil {
			if errors.Is(err, cluster.ErrForwardNotLeader) {
				return authcallout.ErrNotLeader
			}
			return err
		}
		return nil
	}
}
