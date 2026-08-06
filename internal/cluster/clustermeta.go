package cluster

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// clusterMetaPlanner / clusterMetaApplier are D1's single representative
// Plan/Apply pair (architecture §2.10). The op is a pure baked-literal upsert
// into cluster_meta under the test-reserved key prefix: zero crypto/rand,
// math/rand, ulid or time — so it trivially satisfies the §3.4 determinism rules
// and keeps the determinism-lint banned-import baseline for internal/cluster
// empty. It is superseded by the typed §5 op set in D2.

type clusterMetaPlanner struct{}

// compile-time proof the representative planner satisfies the Plan/Apply seam.
var _ Planner = clusterMetaPlanner{}

// metaSetReq is the request a leader plans into an OpClusterMetaSet command.
type metaSetReq struct {
	Key   string
	Value string
}

func (clusterMetaPlanner) Plan(_ context.Context, _ *sql.DB, req any) (*Command, error) {
	r, ok := req.(metaSetReq)
	if !ok {
		return nil, fmt.Errorf("cluster: clusterMetaPlanner: unexpected request type %T", req)
	}
	return newClusterMetaSet(r.Key, r.Value)
}

// newClusterMetaSet builds an OpClusterMetaSet command. The key is forced under
// the test-reserved prefix so it can never collide with the reserved cursor rows
// or any real table (§2.10).
func newClusterMetaSet(key, value string) (*Command, error) {
	if !strings.HasPrefix(key, metaTestKeyPrefix) {
		return nil, fmt.Errorf("cluster: ClusterMetaSet key %q must use the %q test prefix (D1 scaffolding)", key, metaTestKeyPrefix)
	}
	return &Command{
		Op:      OpClusterMetaSet,
		Version: commandVersion,
		Body: []Statement{{
			SQL: `INSERT INTO cluster_meta(key, value) VALUES(?, ?) ` +
				`ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
			Args: []any{key, value},
		}},
	}, nil
}

// genericExecApplier execs an op's baked statements inside the FSM's
// transaction, in Body order. It is the shared Applier for every D2 op: because
// the leader bakes ALL values into Statement.SQL as literals (sqlbake.go), the
// Apply side is a pure "exec the leader-rendered SQL" with no per-op branching
// and no out-of-band Args. (OpClusterMetaSet rides the same applier carrying its
// one string value via Args, which is JSON-round-trip-stable.)
//
// Consequence the determinism lint must own (d2-plan §6): this generic applier
// SEVERS the static call graph from a per-op Plan, so an import-reachability lint
// rooted at the Applier is tautologically clean and CANNOT prove Plan-side
// determinism. The real determinism guarantor is §13.2 multi-FSM equivalence;
// the lint is a tripwire, not a proof.
type genericExecApplier struct{}

// compile-time proof the shared Applier is *sql.Tx-bound (§3.2/§3.7).
var _ Applier = genericExecApplier{}

func (genericExecApplier) ApplyTx(tx *sql.Tx, cmd *Command) error {
	for i, st := range cmd.Body {
		if _, err := tx.Exec(st.SQL, st.Args...); err != nil {
			return fmt.Errorf("cluster: exec stmt %d: %w", i, err)
		}
	}
	return nil
}

// defaultAppliers is the op→Applier dispatch table the FSM uses. Every D2 op
// shares genericExecApplier (all logic is on the leader Plan side); the per-op
// registration keeps the raft log self-describing and leaves room for a future
// op that needs bespoke Apply logic (none in D2 — Apply-side reads would break
// §3.3 determinism).
func defaultAppliers() map[OpType]Applier {
	exec := genericExecApplier{}
	return map[OpType]Applier{
		OpClusterMetaSet:     exec,
		OpSessionCreate:      exec,
		OpSessionTombstone:   exec,
		OpSessionHardDelete:  exec,
		OpMemberJoin:         exec,
		OpNodeRegister:       exec,
		OpNodeEvict:          exec,
		OpProcCreate:         exec,
		OpProcMarkExited:     exec,
		OpProcGC:             exec, // h1 B1: keyed DELETE, terminal-state guard baked
		OpReconcileBatch:     exec,
		OpPortAllocate:       exec,
		OpPortFree:           exec,
		OpPortRevoke:         exec,
		OpPortGC:             exec, // h1 B1: keyed DELETE, terminal-state guard baked
		OpAgentProvision:     exec,
		OpAuditCheckpointSet: exec, // D5 §6.3: monotonic cursor UPSERT, baked WHERE guard
		OpPortReassignHome:   exec, // D6 §7.1-7.2: home re-point + epoch bump, baked CAS guard

		// D7 §8.1. OpClusterNodeUpsert is the ONE custom applier: it re-verifies the
		// join PoP signature (apply-inert cmd.Aux) on every replica before execing the
		// baked roster UPSERT — a verify failure is a deterministic poison-skip
		// (errAppliedRejected), never a panic. The other three ride the shared applier
		// (their CAS/phase-predecessor guards are baked into the leader-rendered SQL).
		OpClusterNodeUpsert:   clusterNodeUpsertApplier{},
		OpClusterNodePhase:    exec,
		OpClusterNodeRemove:   exec,
		OpClusterDrainSet:     exec,
		OpClusterMetaClear:    exec,
		OpClusterCertRotate:   exec,
		OpClusterNodeReaddr:   exec, // v0.4.2: all-literal raft_addr UPDATE, NO generation bump (not roster/conf-facing)
		OpClusterNodeRoute:    exec, // v0.4.2: all-literal nats_route UPDATE + topology_generation bump (conf-facing)
		OpClusterSeedsPublish: exec, // C2 §D-5: all-literal endpoints/bootstrap UPSERT + seed_generation bump
		OpClusterBusNkeySet:   exec, // C3 §D-F: all-literal bus_nkey_pub UPDATE + topology_generation bump
		OpClusterOpStart:      exec, // C4: single-active-guarded operation-log INSERT
		OpClusterOpTransition: exec, // C4: predecessor-CAS operation-state advance + timeline
		OpClusterOpConfirm:    exec, // C4: operation (re-)confirm bit
		OpProxySetEnabled:     exec, // C5: proxy enable/HA-policy + keyset epoch bump
		OpProxySubCreate:      exec, // C5: subscriber INSERT (PSK literal) + epoch bump
		OpProxySubRevoke:      exec, // C5: subscriber REVOKE + epoch bump
		OpProxyAllocate:       exec, // C5: __proxy__ port allocation (home-stamped)

		// D8a §9: empty-Body, pure-Aux re-derivable transfer audit. Apply is a
		// deterministic no-op (0 statements); the publisher replays cmd.Aux.
		OpTransferAudit: exec,

		// D8b §10: replicated alert store. All-literal baked SQL with committed-state
		// guards (WHERE NOT EXISTS / ON CONFLICT); deterministic under ordered Apply.
		OpAlertRaise: exec,
		OpAlertClear: exec,
		OpAlertAck:   exec,
	}
}
