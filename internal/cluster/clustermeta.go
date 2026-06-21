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

type clusterMetaApplier struct{}

// compile-time proof that the only D1 Applier is *sql.Tx-bound (§3.2/§3.7).
var _ Applier = clusterMetaApplier{}

// ApplyTx execs the op's baked statements inside the FSM's transaction. D1's
// applier is the generic "exec the leader-rendered SQL" path; D2's typed ops
// attach richer per-op logic behind this same seam.
func (clusterMetaApplier) ApplyTx(tx *sql.Tx, cmd *Command) error {
	for i, st := range cmd.Body {
		if _, err := tx.Exec(st.SQL, st.Args...); err != nil {
			return fmt.Errorf("cluster: exec stmt %d: %w", i, err)
		}
	}
	return nil
}

// defaultAppliers is the op→Applier dispatch table the FSM uses. D1 registers one
// op; D2 grows this to the typed §5 set.
func defaultAppliers() map[OpType]Applier {
	return map[OpType]Applier{
		OpClusterMetaSet: clusterMetaApplier{},
	}
}
