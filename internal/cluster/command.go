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
	// (MemberKick, PortReassignHome, RotatePin, Alert*, ClusterNode*) are DEFERRED
	// — adding them would be dead code the equivalence test cannot exercise.
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

	// OpAuditCheckpointSet (D5, §6.3) advances the REPLICATED audit-publish cursor
	// cluster_meta.audit_published_index. It is NOT a relaxation of OpClusterMetaSet's
	// "t:" prefix guard (that exists for §2.10 collision prevention) — it is a distinct
	// op whose Plan (auditcursor.go) bakes a monotonic-guard UPSERT so a stale ex-leader
	// proposing a LOWER value is a deterministic no-op on every replica. It rides
	// genericExecApplier and DERIVES ZERO AUDIT (it is in the publisher's skip-set, so it
	// never begets another checkpoint). No migration: cluster_meta exists since 0009.
	OpAuditCheckpointSet OpType = "AuditCheckpointSet"
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
	OpClusterMetaSet:     true,
	OpSessionCreate:      true,
	OpSessionTombstone:   true,
	OpSessionHardDelete:  true,
	OpMemberJoin:         true,
	OpNodeRegister:       true,
	OpNodeEvict:          true,
	OpProcCreate:         true,
	OpProcMarkExited:     true,
	OpReconcileBatch:     true,
	OpPortAllocate:       true,
	OpPortFree:           true,
	OpPortRevoke:         true,
	OpAgentProvision:     true,
	OpAuditCheckpointSet: true,
}
