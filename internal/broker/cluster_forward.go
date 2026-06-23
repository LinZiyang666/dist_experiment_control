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
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proc"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/xferaudit"
	"github.com/nats-io/nats.go"
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
)

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
	case errors.Is(err, agentprov.ErrAlreadyProvisioned):
		return "already_provisioned"
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
}

// NewForwarder builds a Forwarder over an already-connected broker-nkey NATS conn.
func NewForwarder(nc *nats.Conn, timeout time.Duration) *Forwarder {
	return &Forwarder{nc: nc, timeout: timeout}
}

// Forward sends one verb+payload and classifies the reply. Returns nil on ok,
// cluster.ErrForwardNotLeader on not_leader OR a NATS timeout/no-responder (a timeout
// is NOT proof of non-commit — the SAME ReqID dedups on retry), or a
// *ForwardBusinessError on a permanent business error. The caller re-calls with the
// SAME reqID on a retriable error; it MUST NOT mint a fresh key.
func (f *Forwarder) Forward(verb, reqID string, payload []byte) error {
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
var ErrReqIDNotAllowed = errors.New("cluster_forward: provision/join must not carry a forwarding ReqID (idempotent + deletable binding)")

// dispatchForward runs the verb's leader-only Plan on the leader. provision/join go
// through node.Propose (NO ReqID, structurally idempotent, F1/RF1); reconcile goes
// through node.ProposeWithReqID with its bootID-epoch key. Plan reads the LEADER DB.
func dispatchForward(node *cluster.Node, now func() time.Time, env forwardEnvelope) error {
	switch env.Verb {
	case VerbProvision:
		if env.ReqID != "" {
			return ErrReqIDNotAllowed
		}
		var p ProvisionPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return agentprov.PlanProvisionWithPIN(db, p.SID, p.NID, p.FP, p.PIN, auth.VerifyPIN, now())
		})
	case VerbJoin:
		if env.ReqID != "" {
			return ErrReqIDNotAllowed
		}
		var p JoinPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return session.PlanJoinWithPIN(db, p.SID, p.FP, p.PIN, auth.VerifyPIN, now())
		})
	case VerbReconcile:
		var p ReconcilePayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return node.ProposeWithReqID(env.ReqID, func(db *sql.DB) (*cluster.Command, error) {
			procs, err := proc.ListBySessionFiltered(db, p.SID, proc.ListBySessionOpts{IncludeExited: false})
			if err != nil {
				return nil, err
			}
			ports, err := port.ListBySession(db, p.SID)
			if err != nil {
				return nil, err
			}
			return proc.PlanReconcileBatch(resolveReconcile(p.SID, p.NID, p.Req, procs, ports, now()))
		})
	case VerbTransferAudit:
		// D8a §9: re-derivable transfer audit. PlanTransferAudit sets the content-derived
		// ReqID itself; node.Propose (empty arg) PRESERVES it (ProposeWithReqID only
		// overwrites a NON-empty arg), so the 0011 ledger dedups a forwarder retry. No
		// env.ReqID gate — the key is a pure function of the payload, not minted here.
		var rec schema.AuditTransfer
		if err := json.Unmarshal(env.Payload, &rec); err != nil {
			return err
		}
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return xferaudit.PlanTransferAudit(rec)
		})
	case VerbAlertSignal:
		// D8b §10.2: read the CURRENT ACTIVE state and commit a raise/clear ONLY on a
		// transition — a nil command (no transition) issues no raft write, so a broker's
		// every-tick re-assert is free and a dropped clear self-heals next tick.
		var p AlertSignalPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return planAlertSignal(db, p, now())
		})
	case VerbAlertAck:
		// D8b §10.1: cluster-level ack (idempotent UPSERT). User-initiated + rare, so no
		// transition gate is needed (unlike the periodic signal).
		var p AlertAckPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return err
		}
		return node.Propose(func(db *sql.DB) (*cluster.Command, error) {
			return cluster.PlanAlertAck(p.DedupKey, p.AckedBy, now())
		})
	default:
		return fmt.Errorf("cluster_forward: unknown verb %q", env.Verb)
	}
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
			if err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
				return agentprov.PlanProvisionWithPIN(db, sid, nid, fp, pin, auth.VerifyPIN, t)
			}); err != nil {
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
			if err := node.Propose(func(db *sql.DB) (*cluster.Command, error) {
				return session.PlanJoinWithPIN(db, sid, fp, pin, auth.VerifyPIN, t)
			}); err != nil {
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
