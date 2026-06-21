package cluster

import (
	"encoding/json"
	"fmt"
)

// OpType identifies a replicated FSM operation (architecture §5).
type OpType string

const (
	// OpClusterMetaSet is the ONLY op D1 ships — the minimal representative op
	// (d1-plan §2.10) that exercises the full Apply path (exec op SQL + same-txn
	// applied_index UPSERT + commit) WITHOUT migrating any real mutator (that is
	// D2). It upserts a single cluster_meta row under a test-reserved key prefix.
	OpClusterMetaSet OpType = "ClusterMetaSet"
)

// metaTestKeyPrefix namespaces the keys D1's representative op may write, so it
// can never collide with the reserved cursor rows (applied_index/applied_term/
// bootstrapped) nor any real table. D1 is test-driving scaffolding (§2.10).
const metaTestKeyPrefix = "t:"

// commandVersion is the raft-log envelope version. It is DECOUPLED from
// proto.ProtoVersion: proto v2 governs NATS subject grammar, NOT the raft log
// wire (architecture §5 D1 amendment).
const commandVersion = 1

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

// Command is the raft-log envelope (architecture §5: {t,v,r,b}).
type Command struct {
	Op      OpType      `json:"t"`
	Version int         `json:"v"`
	ReqID   string      `json:"r,omitempty"` // RESERVED + INERT in D1 (no dedup; that is D4)
	Body    []Statement `json:"b"`
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
	return &c, nil
}

var knownOps = map[OpType]bool{OpClusterMetaSet: true}
