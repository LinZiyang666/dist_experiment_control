package proto

// cluster_grow.go (G4 §B) — the account-signed remote-trigger the `cluster add` grow orchestrator sends to
// a specific broker over SubjCtrlClusterGrow so the grow runs external to all brokers and re-resolves the
// leader after each restart the grow itself causes. Mirrors the G5 cluster_upgrade trigger byte-for-byte in
// shape and trust anchor: a broker acts ONLY if TargetNode is its own node_id AND the signature verifies
// against its pinned account_pub (the operator's root authority, same trust as the signed roster/seeds).
// ProtoVersion stays 2 (purely additive; older peers never subscribe the grow subject and stay silent).

// THERE IS DELIBERATELY NO ClusterGrowSchemaVersion CONSTANT (batch B, B4).
//
// One existed — `const ClusterGrowSchemaVersion = 1`, godoc'd as versioning the
// ClusterGrowReq/Resp wire shape — with ZERO references anywhere in the tree. Batch A's
// dead-symbol sweep (A4) deliberately spared it on the stated grounds that B4 would "stamp" it.
// B4 investigated what stamping would mean and found there is nothing for it to do:
//
//   - Into ClusterGrowReq, INSIDE the signed bytes: every grow request already in flight during a
//     mixed-release window stops verifying. `make test` and `make e2e-parallel` are both
//     single-binary, so neither would see it.
//   - Into ClusterGrowReq, OUTSIDE the signed bytes: it becomes the first unsigned field on a
//     signed control message — a value a caller can set freely and a verifier would be tempted to
//     trust.
//   - Into ClusterGrowResp: nothing reads it. A field with a documented contract and no consumer
//     is the same false-promise shape this batch spent its review budget removing.
//
// And the version it would carry is ALREADY carried, signed: CanonicalGrowReqBytes opens with the
// domain-separation prefix "tether-cluster-grow-v2". A future canonical format uses a different
// prefix, so a v2 verifier computes different bytes and the signature simply does not verify.
// The schema version is enforced by construction; a separate constant was redundant with it.
//
// If a future change DOES need an in-band version, TestCanonicalGrowReqBytesCoversEveryField
// (internal/proto/cluster_grow_test.go) is the guard that makes adding a field detectable — the
// field-sensitivity test enumerates fields by hand and cannot notice a new one on its own.

// ClusterGrowReq is one signed remote trigger in the grow orchestration.
type ClusterGrowReq struct {
	// Op is the grow step this trigger drives:
	//   "acquire-lock" | "release-lock" — set/clear the replicated cluster_grow_active marker (leader-only).
	//   "approve-join"                  — leader routes a join bundle into the existing OpClusterJoinApprove op.
	//   "join-status" | "confirm-op"    — read/re-arm an in-flight OpKindJoin op.
	//   "mesh-cutover"                  — former-N1 standalone→clustered cutover + JS-store reset (self-only).
	//   "mesh-peers"                    — leader returns the route-mesh peer triples so the joiner can render
	//                                     its own clustered nats.conf before it boots (it has no replicated
	//                                     cluster_nodes yet; a fresh joiner cannot boot cluster-mode standalone).
	//   "rebalance-proxy"               — even out proxy homes onto the new voter.
	Op string `json:"op"`
	// TargetNode is the broker (node_id) that must handle this — the LEADER for lock/approve/rebalance, the
	// former-N1 for mesh-cutover.
	TargetNode string `json:"target_node"`
	// JoinerNode names the broker being grown into the cluster (the cluster_grow_active marker value; the
	// leader refuses a lock for a DIFFERENT joiner while one grow is in flight).
	JoinerNode string `json:"joiner_node,omitempty"`
	// JoinBundle is the joiner's self-signed PoP bundle (approve-join).
	JoinBundle string `json:"join_bundle,omitempty"`
	// OpID targets an existing OpKindJoin op (join-status / confirm-op).
	OpID string `json:"op_id,omitempty"`
	// GrowEpoch scopes the mesh-cutover JS-store move-aside dir (jetstream.grow-bak.<GrowEpoch>) so a re-run
	// is idempotent per grow (never double-moves a freshly-clustered store).
	GrowEpoch string `json:"grow_epoch,omitempty"`
	// PreserveData acknowledges the former-N1 JS reset (like ResetAck) — the store is MOVED aside, never
	// deleted. v1 does NOT implement auto backup→restore; the moved-aside dir is the operator's restore source.
	PreserveData bool `json:"preserve_data,omitempty"`
	// ResetAck acknowledges destroying a NON-EMPTY former-N1 JS store (loud + operator-gated, plan §3 Q3);
	// without it the cutover HALTs on a data-bearing store rather than silently wiping it.
	ResetAck bool `json:"reset_ack,omitempty"`
	// IssuedAt bounds replay against the verifier's clock (RFC3339).
	IssuedAt string `json:"issued_at"`
	// Sig is hex(ed25519) over CanonicalGrowReqBytes, account-seed signed.
	Sig string `json:"sig"`
}

// ClusterGrowResp is the broker's reply to a grow trigger.
type ClusterGrowResp struct {
	OK          bool   `json:"ok,omitempty"`
	Code        string `json:"code,omitempty"` // reuses the adminsock Code* vocabulary
	Error       string `json:"error,omitempty"`
	OpID        string `json:"op_id,omitempty"`        // approve-join: the created/attached OpKindJoin op id
	OpState     string `json:"op_state,omitempty"`     // join-status: the current op state
	Terminal    bool   `json:"terminal,omitempty"`     // join-status: op reached SERVING/failed
	LastError   string `json:"last_error,omitempty"`   // join-status: the op's last recorded error
	AlreadyDone bool   `json:"already_done,omitempty"` // idempotent no-op (marker already ours / already clustered)
	BackupPath  string `json:"backup_path,omitempty"`  // mesh-cutover: where the moved-aside JS store landed
	// MeshPeers (mesh-peers) is the route-mesh peer set from the leader's cluster_nodes, one entry per broker
	// as "server_name,route_url,bus_nkey_pub" — the exact triple `reconcile nats --manual --peer` consumes, so
	// the joiner can render its own clustered nats.conf (auth_callout + routes) before its broker boots.
	MeshPeers []string `json:"mesh_peers,omitempty"`
}

// CanonicalGrowReqBytes is the EXACT byte sequence signed and verified (it EXCLUDES Sig). It carries a
// domain-separation prefix so a grow signature can never verify as an upgrade/join signature within the
// replay-skew window, even when common fields coincide. Deterministic field order, newline-separated, so an
// off-cluster signer and every broker verifier agree byte-for-byte; any field change invalidates the sig.
func CanonicalGrowReqBytes(r *ClusterGrowReq) []byte {
	preserve := "0"
	if r.PreserveData {
		preserve = "1"
	}
	ack := "0"
	if r.ResetAck {
		ack = "1"
	}
	return []byte("tether-cluster-grow-v2\n" +
		r.Op + "\n" + r.TargetNode + "\n" + r.JoinerNode + "\n" + r.JoinBundle + "\n" +
		r.OpID + "\n" + r.GrowEpoch + "\n" + preserve + "\n" + ack + "\n" + r.IssuedAt)
}
