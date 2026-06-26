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

// RosterPhase* are the SSOT membership-phase strings as they appear in RosterBroker.Phase (projected
// verbatim off cluster_nodes.phase). The C1 agent selector (clusterroster.DialURLs) tiers on them:
// VOTER first, transient (CATCHING_UP / JOIN_VERIFIED_PENDING_VOTER) next, and DRAINING / RETIRING /
// VOTER_ADD_FAILED LAST (de-preferred, not dropped — "停止偏好" not "drop", so an all-draining roster
// still yields URLs). Uppercase to match the producer (cluster.PlanClusterNodePhase bakes uppercase).
const (
	RosterPhaseVoter     = "VOTER"
	RosterPhasePending   = "JOIN_VERIFIED_PENDING_VOTER"
	RosterPhaseCatchUp   = "CATCHING_UP"
	RosterPhaseDraining  = "DRAINING"
	RosterPhaseRetiring  = "RETIRING"
	RosterPhaseAddFailed = "VOTER_ADD_FAILED"
)

// RosterBroker is one broker the agent may dial. Public topology only — no secrets.
type RosterBroker struct {
	NodeID     string `json:"node_id"`
	NatsRoute  string `json:"nats_route"`
	PublicHost string `json:"public_host"`
	TunnelAddr string `json:"tunnel_addr"`
	CertFP     string `json:"cert_fp"`
	Phase      string `json:"phase"`
}

// --- C2: bootstrap / agent-join discovery wire types ---

// SeedBundleSchemaVersion / ClusterManifestSchemaVersion are the C2 wire-shape versions
// (consumers negotiate on them, mirroring ClusterRosterSchemaVersion).
const (
	SeedBundleSchemaVersion      = 1
	ClusterManifestSchemaVersion = 1
)

// SeedBundle (C2) is the account-signed set of CLIENT-DIALABLE NATS endpoints a brand-new agent uses
// to make its FIRST connection — the piece the signed roster cannot provide (the roster carries only
// route addrs + public hosts to be templated onto the agent's own NATS URL, which a fresh agent does
// not have). It is signed with the cluster ACCOUNT seed and verified against the pinned account_pub
// (the same trust anchor as the roster); it carries ZERO secrets (public endpoint URLs only). Its
// Generation is the dedicated monotone `seed_generation` counter (NOT roster_generation), so endpoint
// rotation has its own anti-rollback and a seeds-publish never pollutes the roster topology-lag
// signal. All wire additions are additive/omitempty — ProtoVersion stays 2.
type SeedBundle struct {
	SchemaVersion int      `json:"schema_version"`
	Generation    uint64   `json:"generation"` // seed-domain monotone (cluster_meta seed_generation)
	AccountPub    string   `json:"account_pub"`
	Endpoints     []string `json:"endpoints"`               // client-dialable NATS URLs (nats://|wss://|tls://)
	BootstrapURL  string   `json:"bootstrap_url,omitempty"` // the well-known HTTPS manifest URL (for re-fetch)
	IssuedAt      string   `json:"issued_at"`
	ExpiresAt     string   `json:"expires_at,omitempty"`
	Sig           string   `json:"sig"` // hex(ed25519) over CanonicalSeedBytes, account-seed signed
}

// ClusterManifest (C2) is the body served at the well-known HTTP endpoint (/.well-known/tether/
// cluster.json): the signed roster + the signed seed bundle, each INDEPENDENTLY account-signed (the
// envelope itself is unsigned — the agent verifies the two children, never the envelope). The agent
// feeds Roster→VerifyAt and Seeds→VerifySeedsAt, both against the pinned account_pub.
type ClusterManifest struct {
	SchemaVersion int            `json:"schema_version"`
	Roster        *ClusterRoster `json:"roster"`
	Seeds         *SeedBundle    `json:"seeds"`
}
