package proto

// cluster_roster.go — B7 DOC#3: the account-signed broker roster an agent uses to discover the
// cluster's broker set without a static broker_url. It is signed with the cluster ACCOUNT seed
// (the same trust anchor agents already pin) and verified against the pinned account_pub — no new
// PKI. The roster is DISCOVERY-ONLY: it confers ZERO membership authority (a node still becomes a
// voter only via the two-phase join-PoP over a fresh leader nonce). All wire additions are
// additive/omitempty — ProtoVersion stays 2.

// ClusterRosterSchemaVersion is the roster wire-shape version (monitors/agents negotiate on it).
const ClusterRosterSchemaVersion = 1

// ClusterRoster is the signed broker set. Sig is hex(ed25519) over CanonicalRosterBytes (computed
// by internal/clusterroster, which owns the canonicalization + sign/verify so proto stays leaf).
type ClusterRoster struct {
	SchemaVersion int            `json:"schema_version"`
	Generation    uint64         `json:"generation"` // membership-domain monotone (NOT raft index — survives recover)
	AccountPub    string         `json:"account_pub"`
	Brokers       []RosterBroker `json:"brokers"`
	IssuedAt      string         `json:"issued_at"`
	ExpiresAt     string         `json:"expires_at,omitempty"`
	Sig           string         `json:"sig"` // hex(ed25519) over the canonical bytes, signed by the account seed
}

// RosterBroker is one broker the agent may dial. Public topology only — no secrets.
type RosterBroker struct {
	NodeID     string `json:"node_id"`
	NatsRoute  string `json:"nats_route"`
	PublicHost string `json:"public_host"`
	TunnelAddr string `json:"tunnel_addr"`
	CertFP     string `json:"cert_fp"`
	Phase      string `json:"phase"`
}
