package cluster

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

// OpType identifies a replicated FSM operation (architecture §5).
type OpType string

const (
	// OpClusterMetaSet is the D1 representative op (d1-plan §2.10): the minimal op
	// that exercises the full Apply path (exec op SQL + same-txn applied_index
	// UPSERT + commit). It upserts a single cluster_meta row under a test-reserved
	// key prefix and is the only op that still uses Statement.Args (its value is a
	// string, JSON-round-trip-stable). Retained as the cursor/test seam.
	OpClusterMetaSet OpType = "ClusterMetaSet"

	// D2 op set (architecture §5 + docs/reviews/d2-plan.md §2). Each wraps a write
	// that has a LIVE single-broker caller today, derived from an exhaustive grep
	// of every INSERT/UPDATE/DELETE to an Apply-owned table across internal/. Their
	// Plan* functions live in the mutator packages (internal/{port,proc,node,
	// session,agentprov}) and render all-literal Commands (no Statement.Args); the
	// shared genericExecApplier execs the baked SQL. Ops with no live writer today
	// (MemberKick, RotatePin, Alert*, ClusterNode*) are DEFERRED
	// — adding them would be dead code the equivalence test cannot exercise.
	// (PortReassignHome was promoted to a live op in D6 — see OpPortReassignHome.)
	OpSessionCreate     OpType = "SessionCreate"
	OpSessionTombstone  OpType = "SessionTombstone"
	OpSessionHardDelete OpType = "SessionHardDelete"
	OpMemberJoin        OpType = "MemberJoin"
	OpNodeRegister      OpType = "NodeRegister" // identity columns only; liveness is leader-local (§3.5)
	OpNodeEvict         OpType = "NodeEvict"    // adminsock evict: DELETE nodes + agent_provisioning
	OpProcCreate        OpType = "ProcCreate"
	OpProcMarkExited    OpType = "ProcMarkExited"
	OpReconcileBatch    OpType = "ReconcileBatch"
	OpPortAllocate      OpType = "PortAllocate"
	OpPortFree          OpType = "PortFree"
	OpPortRevoke        OpType = "PortRevoke"
	OpAgentProvision    OpType = "AgentProvision"

	// OpProcGC / OpPortGC (h1 B1) delete terminal-state history rows —
	// EXITED processes, FREED/REVOKED port_allocations — older than a
	// retention the LEADER computes. The retention comparison happens only in
	// the leader-side Plan SELECT; the baked Apply SQL keys on an explicit
	// pid/row_id set with the terminal-state guard re-asserted, so replay is
	// a deterministic idempotent no-op and the heterogeneous-timestamp-text
	// problem (see proc.PlanGCExited) can never reach a replica. Chunked at
	// ≤500 keys per command so one raft entry stays small. Both ride
	// genericExecApplier. Before these ops, cluster mode had NO retention at
	// all ("skip GC until it has a replicated command") — the 2026-08-04
	// incident fleet had accumulated 24k FREED + 8.5k EXITED rows.
	OpProcGC OpType = "ProcGC"
	OpPortGC OpType = "PortGC"

	// OpAuditCheckpointSet (D5, §6.3) advances the REPLICATED audit-publish cursor
	// cluster_meta.audit_published_index. It is NOT a relaxation of OpClusterMetaSet's
	// "t:" prefix guard (that exists for §2.10 collision prevention) — it is a distinct
	// op whose Plan (auditcursor.go) bakes a monotonic-guard UPSERT so a stale ex-leader
	// proposing a LOWER value is a deterministic no-op on every replica. It rides
	// genericExecApplier and DERIVES ZERO AUDIT (it is in the publisher's skip-set, so it
	// never begets another checkpoint). No migration: cluster_meta exists since 0009.
	OpAuditCheckpointSet OpType = "AuditCheckpointSet"

	// OpPortReassignHome (D6 §7.1-7.2) re-points one expose's home broker and
	// bumps its per-port epoch. Its Plan (port.PlanReassignHome) reads the current
	// epoch under applyMu and bakes an all-literal UPDATE with a monotonic CAS
	// guard (... WHERE port=<lit> AND state='ALLOCATED' AND epoch < <LitInt(cur+1)>)
	// so a stale ex-leader's lower-epoch reassign is a deterministic RowsAffected==0
	// no-op on every replica — the FSM-layer ex-home double-bind fence. It rides
	// genericExecApplier. The leader-driven backup path carries NO reqID (the D4
	// ledger requires an originating-broker-minted key, which a leader-push has
	// none of; the CAS guard is the sole idempotency anchor, like D4 provision/join).
	OpPortReassignHome OpType = "PortReassignHome"

	// D7 cluster-membership op set (§8.1). All leader-driven, carry NO reqID (no
	// originating-broker key; their CAS/phase-predecessor guards are the idempotency
	// anchors, like OpPortReassignHome). Their Plan helpers live in THIS package
	// (membership_ops.go) — they are cluster's OWN ops (cf. D1's clusterMetaPlanner),
	// NOT mutator-package ops, and internal/clusternodes must NOT import internal/
	// cluster (L-2), so the writers cannot live there.

	// OpClusterNodeUpsert (§8.1 phase-1 roster admission) is the ONE op that does
	// NOT ride genericExecApplier: clusterNodeUpsertApplier re-verifies the join PoP
	// ed25519 signature (carried apply-inert in cmd.Aux) on EVERY replica before
	// execing the baked roster UPSERT. A verify failure is a deterministic POISON
	// SKIP (errAppliedRejected → advance applied_index, run no op, never panic), so a
	// compromised/buggy leader proposing a forged entry cannot brick the cluster on
	// log replay (§2.8 never-wedge). Verify is a pure function of committed bytes
	// (auth.VerifySignature + the canonical message), so all replicas reach the
	// identical verdict — no fork.
	OpClusterNodeUpsert OpType = "ClusterNodeUpsert"

	// OpClusterNodePhase (§8.1 half-success state machine) transitions a node's
	// phase with a baked `WHERE node_id=<lit> AND phase IN (<allowed predecessors>)`
	// guard so a stale ex-leader's disallowed transition is a RowsAffected==0 no-op.
	// Rides genericExecApplier.
	OpClusterNodePhase OpType = "ClusterNodePhase"

	// OpClusterNodeRemove (§8.1 removal order) deletes a roster row only when it has
	// walked through removal (phase IN ('RETIRING','VOTER_ADD_FAILED')) — a live
	// VOTER is structurally undeletable. Idempotent. Rides genericExecApplier.
	OpClusterNodeRemove OpType = "ClusterNodeRemove"

	// OpClusterDrainSet (§8.3) raises/clears a broker_draining flag as a cluster_meta
	// KV ('draining:'+node_id -> deadline literal) with a monotonic guard (later
	// deadline wins), reusing the audit-checkpoint UPSERT pattern. Rides
	// genericExecApplier. No migration: cluster_meta exists since 0009.
	OpClusterDrainSet OpType = "ClusterDrainSet"

	// OpClusterMetaClear (review M3) DELETEs the replicated force_single_active marker
	// once HA is restored (a node added back to F>=1). Without it the marker — written
	// by the offline force-single tool into cluster_meta — rides the snapshot into
	// every regrown peer and the whole cluster reports exit 3 (FORCE_SINGLE) forever.
	// Rides genericExecApplier; bakes a fixed-key DELETE (no external input).
	OpClusterMetaClear OpType = "ClusterMetaClear"

	// OpClusterBusNkeySet (C3) records a broker's NATS bus nkey public key into cluster_nodes so the
	// topology reconciler can render that peer into auth_callout.auth_users + the static users{} ACL.
	// Each broker self-writes its OWN row (from its broker.nk) via proposeOrForward; it also backfills
	// pre-C3 voters (DEFAULT ''). All-literal UPDATE + topology_generation bump (change-gated, so an
	// idempotent re-propose is a no-op); rides genericExecApplier.
	OpClusterBusNkeySet OpType = "ClusterBusNkeySet"

	// OpClusterOpStart/Transition/Confirm (C4) drive the replicated operation log (cluster_operations):
	// a single-active-guarded INSERT, a predecessor-CAS state advance, and a (re-)confirm. All-literal
	// leader-baked SQL on genericExecApplier; guards are baked WHERE clauses (RowsAffected==0 no-op),
	// NEVER an Apply-time CHECK/UNIQUE (which would fsm-panic on every replica).
	OpClusterOpStart      OpType = "ClusterOpStart"
	OpClusterOpTransition OpType = "ClusterOpTransition"
	OpClusterOpConfirm    OpType = "ClusterOpConfirm"

	// OpProxy* (C5) drive the proxy CONTROL plane through Raft (majority-write): the per-session
	// enable/HA-policy + keyset epoch bump, the subscriber set, and the per-agent __proxy__ allocation.
	// All all-literal leader-baked on genericExecApplier (baked WHERE guards, no Apply CHECK/UNIQUE —
	// uniqueness is prechecked on the leader). The subscriber PSK is the one replicated secret (C5
	// §D-SECRET, recoverable-by-contract within the broker-only mTLS boundary); raw tokens stay hash-only.
	OpProxySetEnabled OpType = "ProxySetEnabled"
	OpProxySubCreate  OpType = "ProxySubCreate"
	OpProxySubRevoke  OpType = "ProxySubRevoke"
	OpProxyAllocate   OpType = "ProxyAllocate"

	// OpClusterSeedsPublish (C2) records the cluster's public CLIENT-DIALABLE seed endpoints +
	// bootstrap (well-known) URL into cluster_meta and bumps the dedicated monotone seed_generation
	// counter, so a brand-new agent (which has no NATS template) can be served an account-signed
	// SeedBundle for its first connection. All-literal (leader bakes the endpoints/bootstrap + the
	// MAX-floored seed_generation); rides genericExecApplier.
	OpClusterSeedsPublish OpType = "ClusterSeedsPublish"

	// OpClusterCertRotate (§15 RF3 / external review F2) rotates a node's stable tunnel
	// cert fingerprint: SET cert_fp_prev=cert_fp (the row's current value, deterministic
	// self-reference), cert_fp=<new>, cert_fp_valid_until=<now+window>. The D6 cert-pin
	// VerifyConnection accepts the previous pin until valid_until (resumption-safe
	// rotation). Rides genericExecApplier.
	OpClusterCertRotate OpType = "ClusterCertRotate"

	// OpClusterNodeReaddr (v0.4.2 phase-fluidity) rewrites a node's cluster_nodes.raft_addr —
	// the replicated copy of its raft advertise address. The raft Configuration self-address is
	// rewritten separately via node.AddVoter (an online in-place address update); this op keeps
	// the SQLite roster copy in sync so status, the :7400 liveness probe, and a future
	// force-single read the same address. Change-gated all-literal UPDATE with NO generation
	// bump: raft_addr is absent from the agent-facing roster SELECT (cluster_roster.go) and the
	// rendered nats.conf, so a raft_addr change must not spuriously recompute the roster or push
	// agents. Rides genericExecApplier.
	OpClusterNodeReaddr OpType = "ClusterNodeReaddr"

	// OpClusterNodeRoute (v0.4.2 phase-fluidity) rewrites a node's cluster_nodes.nats_route —
	// the NATS route-mesh advertise (the twin of raft_addr for the :6222 cluster{} routes).
	// UNLIKE raft_addr, nats_route IS rendered into nats.conf (topology_reconcile.go builds
	// natscluster.Broker.RouteURL from it), so the change bumps topology_generation (change-gated)
	// to drive a re-render + reload. Rides genericExecApplier.
	OpClusterNodeRoute OpType = "ClusterNodeRoute"

	// OpTransferAudit (D8a §9/§6.3) makes transfer audit (start/complete/failed)
	// re-derivable. It carries the full schema.AuditTransfer record apply-inert in
	// cmd.Aux and an EMPTY Body — transfer audit lives only in the JS history-<sid>
	// stream, never a SQL row, so Apply is a deterministic no-op (rides
	// genericExecApplier). The D5 publisher replays it via xferaudit.ReplayTransferAudit
	// with a q<reqID>:xfer dedup id; cross-retry idempotency is the derived
	// reqID=hex(sha256("xferaudit:"||transfer_id||":"||kind)) through the 0011
	// cluster_reqid_ledger (the finite JS Duplicates window is a second line only). The
	// Aux schema lives in internal/xferaudit (NOT here) so the FSM core stays free of the
	// audit schema (L-2). No migration: nothing is written to SQLite.
	OpTransferAudit OpType = "TransferAudit"

	// D8b alert store (§10). Three leader-driven ops, all riding genericExecApplier with
	// all-literal baked SQL (alert_ops.go). No migration: alerts/alert_acks exist since
	// 0009. Determinism rests on strictly-ordered Apply + a committed-state predicate
	// (every replica evaluates the same WHERE against the same committed rows → identical
	// RowsAffected), NOT on "no replica errors". They carry NO reqID — the leader-gated
	// reconcile loop proposes raise/clear ONLY on a genuine desired-vs-current transition
	// (no per-tick re-propose), and the WHERE NOT EXISTS / ON CONFLICT guards make a racing
	// double-raise / double-ack a deterministic no-op, so a forwarding key is unneeded.
	OpAlertRaise OpType = "AlertRaise" // INSERT one ACTIVE alert iff no ACTIVE row holds its dedup_key
	OpAlertClear OpType = "AlertClear" // UPDATE the ACTIVE row for a dedup_key to CLEARED
	OpAlertAck   OpType = "AlertAck"   // UPSERT the single cluster-level ack for a dedup_key
)

// Log-read sentinels returned by Node.CommittedCommandAt (D5 audit publisher, §6.3).
// The publisher treats each distinctly: truncated = a bounded loud accepted-loss
// (advance the cursor past the gap, never wedge); non-command / poison = skip (carries
// no replayable audit; the FSM already advanced applied_index past a poison entry).
var (
	ErrLogTruncated  = errors.New("cluster: log entry truncated by snapshot")
	ErrLogNonCommand = errors.New("cluster: log entry is not a command")
	ErrLogPoison     = errors.New("cluster: log entry failed to decode")
)

// metaTestKeyPrefix namespaces the keys D1's representative op may write, so it
// can never collide with the reserved cursor rows (applied_index/applied_term/
// bootstrapped) nor any real table. D1 is test-driving scaffolding (§2.10).
const metaTestKeyPrefix = "t:"

// commandVersion is the raft-log envelope version. It is DECOUPLED from
// proto.ProtoVersion: proto v2 governs NATS subject grammar, NOT the raft log
// wire (architecture §5 D1 amendment).
//
// D4 bumped this 1->2 when OpReconcileBatch gained the apply-inert Aux audit-tuple
// field (§4.1 self-sufficient replay). Safe: no production cluster.Node exists yet
// (build-and-prove, cutover=D9) so no persisted v1 log can be rejected, and harness
// DBs are fresh per test. A v1-shaped entry now decodes as POISON (loud, advances
// applied_index, never wedges) rather than silently replaying an empty audit set.
const commandVersion = 2

// CommandVersion exports the raft command-encoding version (DECOUPLED from proto.ProtoVersion). G5 #13
// folds it into ClusterHealthResp so `cluster upgrade` can refuse a rolling upgrade whose target bumps
// it (a commandVersion mismatch POISONs the log per-replica — a rolling roll would silently fork the FSM).
func CommandVersion() int { return commandVersion }

// Statement is one baked, leader-rendered SQL statement plus its already-frozen
// SQL parameters.
//
// NOTE (§5 D1 amendment): encoding/json of Args []any is NOT round-trip
// type-stable (ints decode to float64, []byte to base64). D1's only op carries a
// string value (which survives the round-trip), so this is safe at D1. D2's real
// arg-bearing ops MUST use leader-rendered SQL literals or a typed/positional
// encoding — do NOT freeze this []any envelope as the D2 arg-passing mechanism.
type Statement struct {
	SQL  string `json:"s"`
	Args []any  `json:"a,omitempty"`
}

// Command is the raft-log envelope (architecture §5: {t,v,r,b,x}).
type Command struct {
	Op      OpType `json:"t"`
	Version int    `json:"v"`
	// ReqID is the cross-retry-STABLE, originating-broker-minted idempotency key
	// (§4.1 D4). The FSM dedups an already-committed ReqID arriving at a NEW raft
	// index (a forwarder retry after the "committed but ack lost" ErrLeadershipLost
	// ambiguity) via the cluster_reqid_ledger, skipping the op while still advancing
	// applied_index. Empty = no dedup (D1/D2 ops are byte-unchanged). decodeCommand
	// fail-closes a malformed ReqID (charset/length) so it cannot round-trip raft-log
	// JSON differently per node (split-brain ledger).
	ReqID string      `json:"r,omitempty"`
	Body  []Statement `json:"b"`
	// Aux is an opaque, APPLY-INERT auxiliary payload: genericExecApplier ignores it
	// entirely (only Body statements mutate the DB). Currently only OpReconcileBatch
	// uses it, to carry the §4.1 self-sufficient ordered audit-replay tuples so any
	// new leader can byte-identically replay the audit from the committed entry alone
	// (decoded by proc.ReplayReconcileAudit). D4 publishes nothing; D5 owns publish.
	Aux json.RawMessage `json:"x,omitempty"`
}

// NewCommand builds a versioned Command for op with the given baked statements.
// The per-op Plan* functions in the mutator packages use this so commandVersion
// stays a single internal SSOT (those packages cannot see the unexported const).
func NewCommand(op OpType, body ...Statement) *Command {
	return &Command{Op: op, Version: commandVersion, Body: body}
}

// Stmt is a baked, all-literal statement (no Args) — the D2 op shape. Args are
// reserved for the inert D1 ClusterMetaSet only (see sqlbake.go on why Args is
// forbidden for real ops).
func Stmt(sql string) Statement { return Statement{SQL: sql} }

// ExecCommand applies a baked Command's statements to db in ONE transaction,
// WITHOUT the raft applied_index machinery. It is NOT the replicated apply path
// (that is fsm.Apply through raft); it exists for (a) the §13.2/DIFF-1 equivalence
// harnesses, which compare a Plan-rendered op against today's direct mutator on a
// plain DB, and (b) the D9 one-time --from-existing migration. The replicated
// write path stays genericExecApplier under the FSM txn.
func ExecCommand(db *sql.DB, cmd *Command) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("cluster: ExecCommand begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := (genericExecApplier{}).ApplyTx(tx, cmd); err != nil {
		return err
	}
	return tx.Commit()
}

func (c *Command) encode() ([]byte, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return nil, fmt.Errorf("cluster: encode command: %w", err)
	}
	return b, nil
}

// decodeCommand parses + validates a raft-log entry. A structurally-invalid entry
// (bad JSON, wrong version, unknown op) returns an error; the FSM treats such a
// committed entry as POISON and advances past it as a no-op (§2.8).
func decodeCommand(data []byte) (*Command, error) {
	var c Command
	if err := json.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("cluster: decode command: %w", err)
	}
	if c.Version != commandVersion {
		return nil, fmt.Errorf("cluster: unsupported command version %d (want %d)", c.Version, commandVersion)
	}
	if !knownOps[c.Op] {
		return nil, fmt.Errorf("cluster: unknown op %q", c.Op)
	}
	// §4.1 D4: fail-close a malformed ReqID. The minted key is lowercase hex (a
	// SHA-256 prefix); anything else (NUL, non-UTF-8, oversized) could round-trip
	// raft-log JSON differently per node and split-brain the dedup ledger. A bad
	// ReqID makes the whole entry POISON (advanced past, never honored).
	if !validReqID(c.ReqID) {
		return nil, fmt.Errorf("cluster: invalid req_id (must be empty or <=64 lowercase hex)")
	}
	return &c, nil
}

// validReqID reports whether s is an acceptable Command.ReqID: empty (no dedup) or
// 1..64 lowercase-hex chars (a SHA-256-derived idempotency key, §4.1 D4). The hex
// constraint intrinsically rejects NUL / non-UTF-8 / whitespace.
func validReqID(s string) bool {
	if len(s) > 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

var knownOps = map[OpType]bool{
	OpClusterMetaSet:      true,
	OpSessionCreate:       true,
	OpSessionTombstone:    true,
	OpSessionHardDelete:   true,
	OpMemberJoin:          true,
	OpNodeRegister:        true,
	OpNodeEvict:           true,
	OpProcCreate:          true,
	OpProcMarkExited:      true,
	OpProcGC:              true,
	OpReconcileBatch:      true,
	OpPortAllocate:        true,
	OpPortFree:            true,
	OpPortRevoke:          true,
	OpPortGC:              true,
	OpAgentProvision:      true,
	OpAuditCheckpointSet:  true,
	OpPortReassignHome:    true,
	OpClusterNodeUpsert:   true,
	OpClusterNodePhase:    true,
	OpClusterNodeRemove:   true,
	OpClusterDrainSet:     true,
	OpClusterMetaClear:    true,
	OpClusterCertRotate:   true,
	OpClusterNodeReaddr:   true,
	OpClusterNodeRoute:    true,
	OpClusterSeedsPublish: true,
	OpClusterBusNkeySet:   true,
	OpClusterOpStart:      true,
	OpClusterOpTransition: true,
	OpClusterOpConfirm:    true,
	OpProxySetEnabled:     true,
	OpProxySubCreate:      true,
	OpProxySubRevoke:      true,
	OpProxyAllocate:       true,
	OpTransferAudit:       true,
	OpAlertRaise:          true,
	OpAlertClear:          true,
	OpAlertAck:            true,
}

// KnownOpsForDocs returns a COPY of the known-op set, for the one consumer
// that must compare the FSM's real vocabulary against prose: the layer-2
// architecture doc's §5 op catalog
// (test/determinism/op_catalog_test.go).
//
// origin: docs/reviews/h1-external-review.md F5 — h1 added OpProcGC/OpPortGC
// and the binding doc kept asserting the opposite ("ProcGC = leader-local, not
// in Raft"), which per CLAUDE.md §1 outranks the code as the contract. A copy,
// not the map, so no caller can mutate the registry through the accessor.
func KnownOpsForDocs() map[OpType]bool {
	out := make(map[OpType]bool, len(knownOps))
	for op, ok := range knownOps {
		out[op] = ok
	}
	return out
}

// HasPhaseFluidityOps reports whether THIS binary's knownOps includes the v0.4.2 phase-fluidity
// membership ops (OpClusterNodeReaddr/Route). A broker advertises this in its health report so the
// leader can gate SetRaftAddr/SetNatsRoute on EVERY voter supporting them — an older binary lacking
// the ops would decodeCommand-poison the replicated entry and deterministically fork its replica
// (review F5). Self-describing: a future binary that drops the ops reports false.
func HasPhaseFluidityOps() bool {
	return knownOps[OpClusterNodeReaddr] && knownOps[OpClusterNodeRoute]
}
