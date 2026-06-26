package cluster

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
)

// join_bundle.go (C4) — the joiner-local join BUNDLE: `cluster join prepare` mints it on the NEW
// machine (which cannot call the leader), carrying this node's full membership identity + a
// self-minted nonce + the PoP signature over the UNCHANGED signed bytes (domain‖node_id‖ident_pub‖
// nonce). `cluster join approve` (leader) decodes + pre-verifies it, then admits the roster row whose
// PoP is RE-verified by every replica (clusterNodeUpsertApplier). The bundle carries NO secrets — the
// identity SEED never leaves the joiner; only its PUBLIC key + the signature travel.
type JoinBundle struct {
	NodeID       string `json:"node_id"`
	Name         string `json:"name"`
	NodeIdentPub string `json:"node_ident_pub"`
	NatsServerID string `json:"nats_server_id"`
	RaftAddr     string `json:"raft_addr"`
	NatsRoute    string `json:"nats_route"`
	TunnelAddr   string `json:"tunnel_addr"`
	PublicHost   string `json:"public_host"`
	CertFP       string `json:"cert_fp"`
	JoinNonce    string `json:"join_nonce"`
	JoinSigHex   string `json:"join_sig_hex"`
}

// EncodeJoinBundle renders a bundle as the base64url string the operator carries OOB from the joiner
// to the leader.
func EncodeJoinBundle(b JoinBundle) (string, error) {
	raw, err := json.Marshal(b)
	if err != nil {
		return "", fmt.Errorf("cluster: encode join bundle: %w", err)
	}
	return "tether-join:v1:" + base64.RawURLEncoding.EncodeToString(raw), nil
}

// DecodeJoinBundle parses + structurally validates a bundle string (NO PoP check — that is
// VerifyBundlePoP, run separately on the leader).
func DecodeJoinBundle(s string) (*JoinBundle, error) {
	const prefix = "tether-join:v1:"
	if len(s) <= len(prefix) || s[:len(prefix)] != prefix {
		return nil, fmt.Errorf("cluster: malformed join bundle: expected %q prefix", prefix)
	}
	raw, err := base64.RawURLEncoding.DecodeString(s[len(prefix):])
	if err != nil {
		return nil, fmt.Errorf("cluster: decode join bundle: %w", err)
	}
	var b JoinBundle
	if err := json.Unmarshal(raw, &b); err != nil {
		return nil, fmt.Errorf("cluster: parse join bundle: %w", err)
	}
	if err := proto.ValidateClusterNodeID(b.NodeID); err != nil {
		return nil, fmt.Errorf("cluster: join bundle node_id: %w", err)
	}
	// C8 (D10): preserve the D9 expose-home/NATS-peer identity-completeness gate the deleted `cluster
	// add` enforced — a voter that can never serve as an expose home (no tunnel_addr) or join the NATS
	// mesh (no nats_route) is admission-rejected here, leader-side authoritative (a hand-crafted bundle
	// can't bypass). cert_fp stays OPTIONAL (D6 backfills it on first reconnect — intentional divergence).
	if b.NodeIdentPub == "" || b.JoinNonce == "" || b.JoinSigHex == "" || b.RaftAddr == "" || b.TunnelAddr == "" || b.NatsRoute == "" {
		return nil, fmt.Errorf("cluster: join bundle missing required field(s) (ident_pub/nonce/sig/raft_addr/tunnel_addr/nats_route)")
	}
	return &b, nil
}

// VerifyBundlePoP checks the joiner's proof-of-possession: the signature over JoinSignBytes(node_id,
// ident_pub, nonce) verifies against the bundle's OWN ident_pub. This is the LEADER pre-check; the
// authoritative re-verify happens on every replica in clusterNodeUpsertApplier. (Per §18.2.4 the
// nonce is a consistency property; `approve` requires admin-socket access = the trust boundary.)
func VerifyBundlePoP(b *JoinBundle) error {
	sig, err := hex.DecodeString(b.JoinSigHex)
	if err != nil {
		return fmt.Errorf("cluster: join bundle signature hex: %w", err)
	}
	if err := auth.VerifySignature(b.NodeIdentPub, JoinSignBytes(b.NodeID, b.NodeIdentPub, b.JoinNonce), sig); err != nil {
		return fmt.Errorf("cluster: join bundle PoP verify: %w", err)
	}
	return nil
}

// ToUpsertInput projects a verified bundle into the roster-admission input.
func (b *JoinBundle) ToUpsertInput(now time.Time) ClusterNodeUpsertInput {
	name := b.Name
	if name == "" {
		name = b.NodeID
	}
	return ClusterNodeUpsertInput{
		NodeID: b.NodeID, Name: name, NodeIdentPub: b.NodeIdentPub, NatsServerID: b.NatsServerID,
		RaftAddr: b.RaftAddr, NatsRoute: b.NatsRoute, TunnelAddr: b.TunnelAddr, PublicHost: b.PublicHost,
		CertFP: b.CertFP, JoinNonce: b.JoinNonce, JoinSigHex: b.JoinSigHex, Now: now,
	}
}
