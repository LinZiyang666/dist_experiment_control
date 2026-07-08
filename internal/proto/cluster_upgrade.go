package proto

// cluster_upgrade.go (G5 #13 W2b) — the account-signed remote-trigger the `cluster upgrade` orchestrator
// sends to a specific broker over SubjCtrlClusterUpgrade so the roll runs external to all brokers and
// re-resolves the leader after each restart. The security anchor is the account signature (the operator's
// root authority, same trust anchor as the signed roster/seeds): a broker acts ONLY if TargetNode is its
// own node_id AND the signature verifies against its pinned account_pub. ProtoVersion stays 2 (additive).

// ClusterUpgradeReq is one signed remote trigger. Op is "reload" (restart THIS broker into the staged
// on-disk binary) or "transfer-leader" (hand raft leadership to TransferTo).
type ClusterUpgradeReq struct {
	Op           string `json:"op"`                      // "reload" | "transfer-leader" | "reexec-agent" | "acquire-lock" | "release-lock"
	TargetNode   string `json:"target_node"`             // the broker (node_id) that must handle this
	TransferTo   string `json:"transfer_to,omitempty"`   // transfer-leader destination voter
	ToVersion    string `json:"to_version,omitempty"`    // reload idempotency anchor (skip if already running)
	ExpectSHA256 string `json:"expect_sha256,omitempty"` // reload/reexec-agent staged-binary digest guard
	SID          string `json:"sid,omitempty"`           // reexec-agent: the session whose co-located agent to re-exec
	IssuedAt     string `json:"issued_at"`               // RFC3339; the verifier bounds replay against its clock
	Sig          string `json:"sig"`                     // hex(ed25519) over CanonicalUpgradeReqBytes, account-seed signed
}

// ClusterUpgradeResp is the broker's reply to a remote trigger.
type ClusterUpgradeResp struct {
	OK               bool   `json:"ok,omitempty"`
	Code             string `json:"code,omitempty"` // reuses the adminsock Code* vocabulary (is_leader/sha_mismatch/…)
	Error            string `json:"error,omitempty"`
	Reloading        bool   `json:"reloading,omitempty"`
	AlreadyAtVersion bool   `json:"already_at_version,omitempty"`
	NewLeader        string `json:"new_leader,omitempty"` // transfer-leader: best-effort new leader id
}

// CanonicalUpgradeReqBytes is the EXACT byte sequence signed and verified (it EXCLUDES Sig). Deterministic
// field order with newline separators so an off-cluster signer and every broker verifier agree
// byte-for-byte. Any field change (a swapped TargetNode, a bumped ToVersion) invalidates the signature.
func CanonicalUpgradeReqBytes(r *ClusterUpgradeReq) []byte {
	return []byte(r.Op + "\n" + r.TargetNode + "\n" + r.TransferTo + "\n" +
		r.ToVersion + "\n" + r.ExpectSHA256 + "\n" + r.SID + "\n" + r.IssuedAt)
}
