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
	// BusNkey is the joiner broker's NATS bus nkey pub (derived from its broker.nk). Carried at
	// ADMISSION so the leader bakes cluster_nodes.bus_nkey_pub immediately — breaking the learner
	// self-backfill DEADLOCK (the self-backfill write must forward over a mesh that cannot render
	// until bus_nkey exists). audit finding A.
	BusNkey string `json:"bus_nkey,omitempty"`

	// ProtoVer / ReleaseVersion (batch B, B4) let the LEADER refuse a proto-skewed joiner
	// before it consumes any operator intent, instead of admitting it and watching it never
	// promote. They are ADVISORY and UNSIGNED — deliberately NOT part of JoinSignBytes, so a
	// hand-crafted bundle can lie about them. That is acceptable because they are not a
	// security boundary: the PoP over (domain‖node_id‖ident_pub‖nonce) is, and it is
	// re-verified by every replica. What these buy is a 50ms named refusal in place of a
	// 2-30 minute catch-up timeout whose last_error says "check the joining broker".
	//
	// omitempty + a plain json.Unmarshal on the receiving side (DecodeJoinBundle) makes this
	// ADDITIVE in both directions: an older joiner sends neither field, the leader reads
	// ProtoVer==0 and takes the existing warn-and-allow branch; an older leader receiving a
	// newer bundle ignores both. ProtoVersion is untouched — no reinstall.
	ProtoVer       int    `json:"proto_ver,omitempty"`
	ReleaseVersion string `json:"release_version,omitempty"`

	JoinNonce  string `json:"join_nonce"`
	JoinSigHex string `json:"join_sig_hex"`
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
	// can't bypass). cert_fp + bus_nkey are NOT required HERE for backward-tolerance, but `cluster join
	// prepare` now derives + carries them fail-closed (audit A / v0.4.4 review F1), so a well-formed bundle
	// always has them; a bundle that omits cert_fp only crash-loops the joiner itself (wireClusterEarly).
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
	natsSrv := b.NatsServerID
	if natsSrv == "" {
		natsSrv = b.NodeID // §6.5 SSOT: server_name == node_id; default so the node stays home-eligible (audit A)
	}
	return ClusterNodeUpsertInput{
		NodeID: b.NodeID, Name: name, NodeIdentPub: b.NodeIdentPub, NatsServerID: natsSrv,
		RaftAddr: b.RaftAddr, NatsRoute: b.NatsRoute, TunnelAddr: b.TunnelAddr, PublicHost: b.PublicHost,
		CertFP: b.CertFP, BusNkey: b.BusNkey, JoinNonce: b.JoinNonce, JoinSigHex: b.JoinSigHex, Now: now,
	}
}
