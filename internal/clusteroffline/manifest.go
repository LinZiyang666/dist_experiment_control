package clusteroffline

// manifest.go — the B6 shared Manifest type + the ONE allowlist projection helper consumed
// by every B6 surface that emits node/roster identity off the FSM DB: `cluster backup` (the
// bundle's manifest.json), `recover --emit-manifest` (the identity capture taken before the
// forensic wipe), and `cluster export-incident` (the roster projection). Centralizing the
// projection here is the security invariant: a future cluster_nodes column can NEVER silently
// leak through any of the three paths, because all three call ProjectRoster / ReadSelfIdentity
// rather than `SELECT *`. The capture is IDENTITY-ONLY — business rows are never projected
// here (the recover forensic dump in offline.go stays the "not auto-mergeable" home for those).

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

// ManifestSchemaVersion is the on-disk manifest contract version. A consumer that reads a
// HIGHER schema_version than it understands must refuse (forward-incompat), so the field is
// checked, not ignored.
const ManifestSchemaVersion = 1

// ManifestKind distinguishes the two producers so a consumer can reason about provenance.
type ManifestKind string

const (
	// ManifestKindBackup is written by `cluster backup` alongside state.db.
	ManifestKindBackup ManifestKind = "backup"
	// ManifestKindRecover is written by `recover --emit-manifest` before the wipe.
	ManifestKindRecover ManifestKind = "recover-divergent"
)

// ManifestMode records whether the captured DB was a single-broker (non-cluster) DB or a
// cluster node. A single-broker bundle must never be restored into a cluster DataDir, and a
// cluster bundle never restored as a bare single broker — the mode gate enforces that.
type ManifestMode string

const (
	// ManifestModeSingleBroker is a non-cluster broker DB (no raft, no self VOTER row).
	ManifestModeSingleBroker ManifestMode = "single-broker"
	// ManifestModeCluster is a cluster node DB (has a self VOTER row + applied_index).
	ManifestModeCluster ManifestMode = "cluster"
)

// RosterEntry is the PUBLIC, allowlisted projection of one cluster_nodes row: node_id / name
// / phase / raft_addr only. The PoP material (join_nonce/join_sig) and the cert columns are
// NEVER included. This is the single shared shape across backup, recover-manifest, and
// export-incident.
type RosterEntry struct {
	NodeID   string `json:"node_id"`
	Name     string `json:"name"`
	Phase    string `json:"phase"`
	RaftAddr string `json:"raft_addr"`
}

// Manifest is the self-describing identity/provenance sidecar. It carries NO secret material:
// only fingerprints (cert_fp, account_fp — hashes, never the keys themselves), the applied
// cursor, and the operator-visible topology. The identity columns are EXACTLY the
// InitFromExistingOptions field set, so `cluster init --from-manifest` reconstructs the self
// VOTER row from the manifest instead of nine flags.
type Manifest struct {
	SchemaVersion int          `json:"schema_version"`
	Kind          ManifestKind `json:"kind"`
	CreatedAt     string       `json:"created_at"`   // RFC3339Nano UTC, stamped by the caller (Now injected)
	ToolVersion   string       `json:"tool_version"` // proto.ReleaseVersion of the producing binary
	Mode          ManifestMode `json:"mode"`

	// Provenance anchors. SelfCertFP is the load-bearing one: at restore/consume it is
	// cross-checked against the LIVE secrets dir's tunnel-cert fp (un-forgeable without the
	// private key). AccountFP (sha256 of cluster-ca.pem) is advisory defense-in-depth.
	SelfID       string `json:"self_id"`
	SelfCertFP   string `json:"self_cert_fp"`
	AccountFP    string `json:"account_fp,omitempty"`
	AppliedIndex uint64 `json:"applied_index"`

	// External-review F3 (+ re-review): online-backup freshness provenance. SourceNodeID is the node
	// the bundle was paged from; SourceRole is "leader" (freshest committed view) or "follower" (a
	// possibly-stale snapshot, taken via --allow-stale-follower); LeaderID names the leader it
	// observed. A STRING role — not a bool — so a follower's "follower" warning actually serializes
	// (a `bool false` with omitempty would silently vanish, the re-review finding). All omitempty: the
	// offline path (no live raft) omits them, byte-compatible with old manifests.
	SourceNodeID string `json:"source_node_id,omitempty"`
	SourceRole   string `json:"source_role,omitempty"` // "leader" | "follower" | "" (offline)
	LeaderID     string `json:"leader_id,omitempty"`

	// Identity columns == InitFromExistingOptions (minus the local paths, which are NEVER
	// trusted from a file). NatsServerID is the §6.5 SSOT (== SelfID for the self row).
	Name         string `json:"name"`
	NodeIdentPub string `json:"node_ident_pub"`
	RaftAddr     string `json:"raft_addr"`
	NatsRoute    string `json:"nats_route"`
	TunnelAddr   string `json:"tunnel_addr"`
	PublicHost   string `json:"public_host"`
	NatsServerID string `json:"nats_server_id"`

	Roster []RosterEntry `json:"roster,omitempty"`
}

// ProjectRoster is the SINGLE allowlist projection of cluster_nodes used by all B6 emit paths.
// It selects ONLY the four public columns (never `SELECT *`), so a column added to cluster_nodes
// in a future migration cannot leak into a backup, a recover manifest, or an incident bundle
// unless it is explicitly added here. Ordered by node_id for deterministic output.
func ProjectRoster(db *sql.DB) ([]RosterEntry, error) {
	rows, err := db.Query(`SELECT node_id, name, phase, raft_addr FROM cluster_nodes ORDER BY node_id`)
	if err != nil {
		return nil, fmt.Errorf("clusteroffline: project roster: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []RosterEntry
	for rows.Next() {
		var e RosterEntry
		if err := rows.Scan(&e.NodeID, &e.Name, &e.Phase, &e.RaftAddr); err != nil {
			return nil, fmt.Errorf("clusteroffline: scan roster row: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// ErrSelfRowMissing is returned by ReadSelfIdentity when the named self node_id has no
// cluster_nodes row (a non-cluster DB, or a wrong --self-id).
var ErrSelfRowMissing = errors.New("clusteroffline: self node_id not present in cluster_nodes")

// ReadSelfIdentity projects the self VOTER row's identity columns + cert_fp + the committed
// applied_index into a partial Manifest. The caller fills Kind / Mode / CreatedAt /
// ToolVersion / AccountFP / Roster. It reads ONLY the allowlisted identity columns (never the
// PoP material). Returns ErrSelfRowMissing if the self row is absent.
func ReadSelfIdentity(db *sql.DB, selfID string) (*Manifest, error) {
	if selfID == "" {
		return nil, errors.New("clusteroffline: ReadSelfIdentity requires a non-empty self id")
	}
	m := &Manifest{SchemaVersion: ManifestSchemaVersion, SelfID: selfID, Mode: ManifestModeCluster}
	err := db.QueryRow(`SELECT name, node_ident_pub, nats_server_id, raft_addr, nats_route, tunnel_addr, public_host, cert_fp
		FROM cluster_nodes WHERE node_id = ?`, selfID).Scan(
		&m.Name, &m.NodeIdentPub, &m.NatsServerID, &m.RaftAddr, &m.NatsRoute, &m.TunnelAddr, &m.PublicHost, &m.SelfCertFP)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return nil, ErrSelfRowMissing
	case err != nil:
		return nil, fmt.Errorf("clusteroffline: read self identity: %w", err)
	}
	idx, err := readAppliedIndex(db)
	if err != nil {
		return nil, err
	}
	m.AppliedIndex = idx
	return m, nil
}

// readAppliedIndex reads cluster_meta key='applied_index' as a uint64. A missing key (a
// single-broker DB that ran the migrations but was never seeded) reads back as 0.
func readAppliedIndex(db *sql.DB) (uint64, error) {
	var s string
	err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key = 'applied_index'`).Scan(&s)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return 0, nil
	case err != nil:
		return 0, fmt.Errorf("clusteroffline: read applied_index: %w", err)
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("clusteroffline: applied_index %q is not a uint64: %w", s, err)
	}
	return v, nil
}

// ReadSelfNodeID reads cluster_meta key='self_node_id' (seeded by `cluster init
// --from-existing`). An empty string (missing key) means the DB was never clustered — a
// single-broker DB — which the backup path records as ManifestModeSingleBroker.
func ReadSelfNodeID(db *sql.DB) (string, error) {
	var s string
	err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key = 'self_node_id'`).Scan(&s)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return "", nil
	case err != nil:
		return "", fmt.Errorf("clusteroffline: read self_node_id: %w", err)
	}
	return s, nil
}

// AccountFingerprint returns sha256(cluster-ca.pem) as lowercase hex — the advisory
// defense-in-depth provenance signal. cluster-ca.pem is a stable per-cluster file (preflight
// requires it), but it is operator-readable so this is NOT the primary restore gate (the
// un-forgeable tunnel-cert fp is). Returns an error only if the file is unreadable.
func AccountFingerprint(secretsDir string) (string, error) {
	b, err := os.ReadFile(filepath.Join(secretsDir, "cluster-ca.pem"))
	if err != nil {
		return "", fmt.Errorf("clusteroffline: read cluster-ca.pem for account_fp: %w", err)
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// WriteManifest serializes m to path with O_EXCL 0600 (never clobbers an existing manifest)
// and the same fsync-file-then-dir durability discipline dumpDivergent uses, so a kill-9 just
// after the write leaves either the full manifest or nothing.
func WriteManifest(path string, m *Manifest) error {
	buf, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("clusteroffline: marshal manifest: %w", err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("clusteroffline: create manifest %s: %w", path, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()
	if _, err := f.Write(buf); err != nil {
		return fmt.Errorf("clusteroffline: write manifest: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("clusteroffline: fsync manifest: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("clusteroffline: close manifest: %w", err)
	}
	closed = true
	if err := fsyncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("clusteroffline: fsync manifest dir: %w", err)
	}
	return nil
}

// ReadManifest parses a manifest file and refuses a schema_version newer than this binary
// understands (forward-incompatible) or an unknown kind.
func ReadManifest(path string) (*Manifest, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("clusteroffline: read manifest %s: %w", path, err)
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, fmt.Errorf("clusteroffline: parse manifest %s: %w", path, err)
	}
	if m.SchemaVersion == 0 || m.SchemaVersion > ManifestSchemaVersion {
		return nil, fmt.Errorf("clusteroffline: manifest schema_version %d is unsupported (this binary speaks %d)", m.SchemaVersion, ManifestSchemaVersion)
	}
	if m.Kind != ManifestKindBackup && m.Kind != ManifestKindRecover {
		return nil, fmt.Errorf("clusteroffline: manifest kind %q is unknown", m.Kind)
	}
	return &m, nil
}
