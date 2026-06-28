// Package clusteroffline orchestrates the D7 §8.4 force-single / recover escape
// hatch. It runs with the daemon STOPPED, directly on the on-disk raft/ +
// tether.db. It holds NO raft import (L-2): the RecoverCluster + FSM wiring lives
// in internal/cluster (RecoverSingleNode / RaftStateExists / RaftStoreLockedByDaemon);
// this package adds only the orchestration — the flock, the (b)(c)(d) hard
// preconditions, the peer TCP-liveness probe, and the divergence dump. The CLI
// (cmd/tether/cluster_offline.go) wraps these with the TTY + typed-node_id confirm
// (force-single/recover NEVER honor --yes, §8.1).
package clusteroffline

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"golang.org/x/sys/unix"
)

const (
	lockFileName    = "tether.lock"
	peerDialTimeout = 1500 * time.Millisecond
)

// ErrDaemonRunning is returned when a live daemon still holds raft.db (the (b)
// bolt-lock probe). The runbook's `systemctl mask` + stop must precede force-single.
var ErrDaemonRunning = errors.New("clusteroffline: daemon still running (raft.db locked); `systemctl mask tether-broker` and stop it first")

// Peer is one non-self roster member the operator must confirm dead.
type Peer struct {
	NodeID     string
	RaftAddr   string
	NatsRoute  string
	TunnelAddr string
}

// ForceSingleOptions configures force-single. ConfirmedDead is the node_id list the
// operator passed via --confirm-peers-dead; it MUST enumerate every non-self roster
// node (an unlisted, merely-partitioned peer would split-brain).
type ForceSingleOptions struct {
	DataDir       string
	DBPath        string
	SelfID        string
	SelfRaftAddr  string
	ConfirmedDead []string
	Now           func() time.Time
	Logger        *slog.Logger
}

// ForceSingle rewrites the on-disk raft config to {self} after enforcing the §8.4
// hard preconditions IN ORDER (all before any disk mutation): (lock) flock + bolt
// probe; (b) state exists; (d) peer-reachable HARD-REFUSE; then (c) RecoverCluster;
// then raise the force_single_active marker. Returns the roster it abandoned.
func ForceSingle(opts ForceSingleOptions) ([]Peer, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	release, err := acquireFlock(filepath.Join(opts.DataDir, lockFileName))
	if err != nil {
		return nil, err
	}
	defer release()

	// (b) live-daemon interlock: a running daemon holds raft.db's bolt lock.
	locked, err := cluster.RaftStoreLockedByDaemon(opts.DataDir)
	if err != nil {
		return nil, err
	}
	if locked {
		return nil, ErrDaemonRunning
	}
	// (b) empty-state refuse.
	exists, err := cluster.RaftStateExists(opts.DataDir)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, cluster.ErrNoExistingState
	}

	// (d) peer-reachable HARD-REFUSE (BEFORE (c) mutates).
	roster, err := readRoster(opts.DBPath, opts.SelfID)
	if err != nil {
		return nil, fmt.Errorf("clusteroffline: read roster: %w", err)
	}
	if err := checkPeersDead(roster, opts.ConfirmedDead); err != nil {
		return nil, err
	}

	// (c) reconcile + config rewrite — RecoverCluster drives the two-store replay.
	if err := cluster.RecoverSingleNode(opts.DataDir, opts.DBPath, opts.SelfID, opts.SelfRaftAddr, opts.Logger); err != nil {
		return roster, err
	}

	// Raise the force_single_active marker so status reports exit 3 + the severe
	// banner on the now single-node DB.
	if err := raiseForceSingleMarker(opts.DBPath, opts.Now()); err != nil {
		return roster, fmt.Errorf("clusteroffline: recovered but failed to mark force_single_active: %w", err)
	}
	opts.Logger.Warn("clusteroffline: force-single complete; node is now a single-voter cluster (no HA / no integrity until recover)",
		"self", opts.SelfID, "abandoned", len(roster))
	return roster, nil
}

// ResnapshotOptions configures the STEP-1 grow-onto-migrated-broker remediation.
type ResnapshotOptions struct {
	DataDir         string
	DBPath          string
	SelfID          string
	SelfRaftAddr    string
	AcceptAuditLoss bool
	Now             func() time.Time
	Logger          *slog.Logger
}

// Resnapshot makes an ALREADY-init'd single-voter migrated broker grow-ready by writing a full FSM
// snapshot + compacting the log (cluster.GrowReadySnapshot). It is the one-time remediation for a
// broker init'd BEFORE the grow-onto-migrated-broker fix (e.g. the live pc732): such a node has the
// migrated rows direct-seeded in SQLite, NO raft snapshot, and a short un-compacted log, so a fresh
// joiner replays its log from index 1 and FK-fail-stops. After Resnapshot FirstIndex>1 → the joiner
// installs the snapshot. OFFLINE: daemon STOPPED.
//
// SINGLE-VOTER ONLY: refuses if any non-self roster node exists — GrowReadySnapshot rewrites the raft
// config to {self}, which on a multi-voter cluster would silently drop the peers. AUDIT-WINDOW GUARD:
// the log truncation is unconditional, so if the D5 audit publisher has not published up to LastIndex,
// those audit entries are LOST. Resnapshot refuses unless AcceptAuditLoss, naming the remedy (restart
// the daemon briefly to let the publisher drain, then re-run); when AcceptAuditLoss it logs loudly.
func Resnapshot(opts ResnapshotOptions) error {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.SelfID == "" || opts.SelfRaftAddr == "" {
		return errors.New("clusteroffline: Resnapshot requires SelfID and SelfRaftAddr")
	}
	release, err := acquireFlock(filepath.Join(opts.DataDir, lockFileName))
	if err != nil {
		return err
	}
	defer release()

	// no live daemon (bolt lock) + state must exist.
	locked, err := cluster.RaftStoreLockedByDaemon(opts.DataDir)
	if err != nil {
		return err
	}
	if locked {
		return ErrDaemonRunning
	}
	exists, err := cluster.RaftStateExists(opts.DataDir)
	if err != nil {
		return err
	}
	if !exists {
		return cluster.ErrNoExistingState
	}

	// SINGLE-VOTER guard: GrowReadySnapshot rewrites the config to {self}; a non-self node would be dropped.
	roster, err := readRoster(opts.DBPath, opts.SelfID)
	if err != nil {
		return fmt.Errorf("clusteroffline: read roster: %w", err)
	}
	if len(roster) > 0 {
		ids := make([]string, len(roster))
		for i, p := range roster {
			ids[i] = p.NodeID
		}
		return fmt.Errorf("clusteroffline: resnapshot is SINGLE-VOTER only but the roster has %d non-self node(s) %v "+
			"— retire/remove them first (resnapshot rewrites the raft config to {self} and would drop peers)", len(roster), ids)
	}

	// AUDIT-WINDOW guard: the unconditional log truncation must not drop unpublished audit. v0.4.4 review
	// STEP1: count ONLY genuine audit-bearing ops above the cursor (OpReconcileBatch / OpTransferAudit) —
	// NOT the raw LastIndex, which on every real migrated broker sits at audit_published_index + trailing
	// config/noop/checkpoint (the D5 publisher self-skips its cursor op without advancing), so a raw
	// `LastIndex > published` guard ALWAYS fired with zero real loss and the restart-drain-stop remedy
	// could provably never clear it. With the scan, a clean broker passes and the remedy genuinely works.
	pub, err := readAuditPublishedIndex(opts.DBPath)
	if err != nil {
		return fmt.Errorf("clusteroffline: read audit cursor: %w", err)
	}
	unpub, firstIdx, err := cluster.UnpublishedAuditOpsInLog(opts.DataDir, pub)
	if err != nil {
		return err
	}
	if unpub > 0 {
		if !opts.AcceptAuditLoss {
			return fmt.Errorf("clusteroffline: resnapshot would truncate %d UNPUBLISHED audit entr(ies) "+
				"(audit_published_index=%d, first unpublished audit op at raft index=%d) — restart tether-broker "+
				"briefly so the D5 publisher drains to the head, stop it, and re-run; or pass --accept-audit-loss "+
				"(bounded loud loss)", unpub, pub, firstIdx)
		}
		opts.Logger.Warn("clusteroffline: resnapshot ACCEPTING bounded audit loss",
			"unpublished_audit_ops", unpub, "audit_published_index", pub, "first_unpublished_index", firstIdx)
	}

	if err := cluster.GrowReadySnapshot(opts.DataDir, opts.DBPath, opts.SelfID, opts.SelfRaftAddr, opts.Logger); err != nil {
		return fmt.Errorf("clusteroffline: grow-ready snapshot: %w", err)
	}
	opts.Logger.Warn("clusteroffline: resnapshot complete; this single-voter broker is now grow-ready "+
		"(a future joiner installs the snapshot instead of replaying the log)", "self", opts.SelfID)
	return nil
}

// readAuditPublishedIndex reads the D5 audit-publish cursor from cluster_meta (0 if absent).
func readAuditPublishedIndex(dbPath string) (uint64, error) {
	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()
	var v uint64
	switch err := db.QueryRow(`SELECT CAST(value AS INTEGER) FROM cluster_meta WHERE key='audit_published_index'`).Scan(&v); err {
	case nil:
		return v, nil
	case sql.ErrNoRows:
		return 0, nil
	default:
		return 0, err
	}
}

// checkPeersDead enforces §8.4(d): every non-self roster node must be in confirmed
// (an unlisted peer would split-brain), and any peer that COMPLETES a TCP connection on ANY of
// its serving ports is alive → HARD-REFUSE (a TLS-rejected-but-TCP-accepting peer is still
// alive, so TCP-completes is the conservative gate, B-8).
//
// audit CC-2: probe ALL of the peer's serving ports (raft_addr + nats_route + tunnel_addr), not
// just raft_addr. A peer whose raft port is firewalled/down but whose NATS or tunnel listener
// still answers clients is ALIVE on the data plane — force-single there would diverge two
// writers (it keeps homing exposes / answering auth_callout from its stale local replica). The
// operator's --confirm-peers-dead listing remains the PRIMARY control; this widens the
// conservative auto-refuse.
func checkPeersDead(roster []Peer, confirmed []string) error {
	confSet := map[string]bool{}
	for _, id := range confirmed {
		confSet[id] = true
	}
	for _, p := range roster {
		if !confSet[p.NodeID] {
			return fmt.Errorf("clusteroffline: --confirm-peers-dead must list EVERY peer; %q is missing "+
				"(an unlisted peer that is merely partitioned would split-brain)", p.NodeID)
		}
	}
	for _, p := range roster {
		for _, ap := range []struct{ kind, addr string }{
			{"raft", p.RaftAddr}, {"nats", p.NatsRoute}, {"tunnel", p.TunnelAddr},
		} {
			if ap.addr == "" {
				continue
			}
			conn, err := net.DialTimeout("tcp", ap.addr, peerDialTimeout)
			if err == nil {
				_ = conn.Close()
				return fmt.Errorf("clusteroffline: HARD-REFUSE — peer %q accepted a TCP connection on its %s port (%s); "+
					"it is ALIVE, force-single would split-brain", p.NodeID, ap.kind, ap.addr)
			}
		}
	}
	return nil
}

// readRoster reads every non-self cluster_nodes row from the offline DB (read-only).
func readRoster(dbPath, selfID string) ([]Peer, error) {
	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	var selfRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id = ?`, selfID).Scan(&selfRows); err != nil {
		return nil, err
	}
	if selfRows != 1 {
		return nil, fmt.Errorf("clusteroffline: self-id %q is not present in cluster_nodes; refusing to rewrite raft config for an unknown node", selfID)
	}
	rows, err := db.Query(`SELECT node_id, raft_addr, nats_route, tunnel_addr FROM cluster_nodes WHERE node_id != ?`, selfID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Peer
	for rows.Next() {
		var p Peer
		if err := rows.Scan(&p.NodeID, &p.RaftAddr, &p.NatsRoute, &p.TunnelAddr); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func raiseForceSingleMarker(dbPath string, now time.Time) error {
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	// force_single_active marks (in cluster_meta) that this node was forced to single;
	// status synthesizes the severe banner + exit 3 from it (it is NOT an alerts.kind —
	// §10.1 keeps it client-synthesized). Cleared once HA is restored (cluster.PlanClearForceSingle).
	_, err = db.Exec(
		`INSERT INTO cluster_meta(key, value) VALUES(?, ?) `+
			`ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		cluster.MetaKeyForceSingle, now.UTC().Format(time.RFC3339Nano))
	return err
}

// RecoverOptions configures recover --dump-divergent.
type RecoverOptions struct {
	DataDir  string
	DBPath   string
	DumpPath string // O_EXCL 0600; never overwrites a prior dump
	// B6 OPS#11: capture the node's IDENTITY (not the divergent business rows) into a manifest
	// BEFORE the wipe, so `cluster init --from-manifest` can rebuild the self VOTER row without
	// the operator re-typing 9 flags. Empty ManifestPath = no emit (back-compat). SelfID names
	// the self row to project; SecretsDir (optional) enables the advisory account_fp.
	SelfID       string
	ManifestPath string
	SecretsDir   string
	Now          func() time.Time
	Logger       *slog.Logger
}

// Recover takes a forensic divergence dump (DURABLY — fsync file + dir, refuse the
// wipe if it fails), OPTIONALLY emits an identity manifest (also DURABLY, also refuse-on-fail),
// then wipes raft/ + tether.db. The node must be reinitialized before the daemon can start,
// then `cluster join prepare`/`cluster join approve` admits it as a clean voter. The dump is forensic-only / not auto-mergeable
// (§8.4(b)/R-7); the manifest is identity-only. Returns the number of rows dumped.
func Recover(opts RecoverOptions) (int, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	release, err := acquireFlock(filepath.Join(opts.DataDir, lockFileName))
	if err != nil {
		return 0, err
	}
	defer release()
	locked, err := cluster.RaftStoreLockedByDaemon(opts.DataDir)
	if err != nil {
		return 0, err
	}
	if locked {
		return 0, ErrDaemonRunning
	}

	n, err := dumpDivergent(opts.DBPath, opts.DumpPath)
	if err != nil {
		return 0, fmt.Errorf("clusteroffline: dump failed, WIPE REFUSED: %w", err)
	}
	// Emit the identity manifest BEFORE the wipe (refuse the wipe if it fails, like the dump):
	// the self row is gone after the wipe, so capture it now or never.
	if opts.ManifestPath != "" {
		if err := emitRecoverManifest(opts); err != nil {
			return 0, fmt.Errorf("clusteroffline: manifest emit failed, WIPE REFUSED: %w", err)
		}
	}
	if err := wipe(opts.DataDir, opts.DBPath); err != nil {
		return n, fmt.Errorf("clusteroffline: dumped %d rows but wipe failed: %w", n, err)
	}
	opts.Logger.Warn("clusteroffline: recover complete; node wiped, re-run cluster init before daemon start, then `cluster join prepare`/`cluster join approve` to rejoin",
		"dump", opts.DumpPath, "manifest", opts.ManifestPath, "rows", n)
	return n, nil
}

// emitRecoverManifest projects the self identity + roster into a recover-divergent manifest.
func emitRecoverManifest(opts RecoverOptions) error {
	if opts.SelfID == "" {
		return fmt.Errorf("clusteroffline: --emit-manifest requires --self-id")
	}
	db, err := storage.OpenReadOnly("file:" + opts.DBPath)
	if err != nil {
		return fmt.Errorf("open db for manifest: %w", err)
	}
	defer func() { _ = db.Close() }()
	m, err := ReadSelfIdentity(db, opts.SelfID)
	if err != nil {
		return err
	}
	m.Kind = ManifestKindRecover
	m.Mode = ManifestModeCluster
	m.CreatedAt = opts.Now().UTC().Format(time.RFC3339Nano)
	m.ToolVersion = proto.ReleaseVersion
	roster, err := ProjectRoster(db)
	if err != nil {
		return err
	}
	m.Roster = roster
	if opts.SecretsDir != "" {
		if fp, ferr := AccountFingerprint(opts.SecretsDir); ferr == nil {
			m.AccountFP = fp
		}
	}
	return WriteManifest(opts.ManifestPath, m)
}

// dumpableTables enumerates EVERY user table in the DB (not a hand-maintained list —
// review B4: a static list silently dropped FSM-owned tables like members /
// proxy_subscribers / alerts on the irreversible wipe). Excludes only sqlite internals
// and the migration tracker. Derived from the live schema so a new table is never
// missed.
func dumpableTables(db *sql.DB) ([]string, error) {
	rows, err := db.Query(`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name != 'schema_migrations' ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// dumpDivergent writes EVERY user table's rows to dumpPath as JSON (O_EXCL 0600),
// fsyncs the file AND its containing dir, re-stats size>0, and returns the row count.
// Any failure returns an error so the caller refuses the wipe.
func dumpDivergent(dbPath, dumpPath string) (int, error) {
	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()

	tables, err := dumpableTables(db)
	if err != nil {
		return 0, fmt.Errorf("enumerate tables: %w", err)
	}

	// O_EXCL: never overwrite a prior dump (a second recover must not clobber forensics).
	f, err := os.OpenFile(dumpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, fmt.Errorf("create dump %s: %w", dumpPath, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = f.Close()
		}
	}()

	enc := json.NewEncoder(f)
	total := 0
	for _, tbl := range tables {
		rows, err := dumpTable(db, tbl)
		if err != nil {
			return 0, fmt.Errorf("dump table %s: %w", tbl, err)
		}
		total += len(rows)
		if err := enc.Encode(map[string]any{"table": tbl, "rows": rows}); err != nil {
			return 0, fmt.Errorf("encode dump %s: %w", tbl, err)
		}
	}

	// Durability before wipe: fsync the file, then its containing directory (so the
	// dirent is durable), then re-stat that the bytes are on disk.
	if err := f.Sync(); err != nil {
		return 0, fmt.Errorf("fsync dump: %w", err)
	}
	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("close dump: %w", err)
	}
	closed = true
	if err := fsyncDir(filepath.Dir(dumpPath)); err != nil {
		return 0, fmt.Errorf("fsync dump dir: %w", err)
	}
	st, err := os.Stat(dumpPath)
	if err != nil || st.Size() == 0 {
		return 0, fmt.Errorf("dump did not persist (size=%d, err=%v)", sizeOf(st), err)
	}
	return total, nil
}

func sizeOf(st os.FileInfo) int64 {
	if st == nil {
		return 0
	}
	return st.Size()
}

// dumpTable returns every row of tbl as a slice of column->value maps. tbl comes
// from the fixed applyOwnedTables list (never user input), so the interpolation is
// safe.
func dumpTable(db *sql.DB, tbl string) ([]map[string]any, error) {
	rows, err := db.QueryContext(context.Background(), "SELECT * FROM "+tbl)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var out []map[string]any
	for rows.Next() {
		vals := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		m := make(map[string]any, len(cols))
		for i, c := range cols {
			m[c] = normalize(vals[i])
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// normalize turns []byte (SQLite TEXT/BLOB) into a string so the JSON dump is
// human-readable forensics.
func normalize(v any) any {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

// wipe removes the raft/ directory and the tether.db (+ -wal/-shm). A subsequent
// `cluster init --from-existing` recreates local raft/ state before `cluster add`
// admits the node from clean state.
func wipe(dataDir, dbPath string) error {
	// Remove tether.db (+ WAL sidecars) FIRST, then raft/ (review m8): a partial
	// failure must never leave stale business state (tether.db) under a wiped raft
	// store, which the next bootstrap would re-bootstrap on top of.
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(dbPath + suffix); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return os.RemoveAll(filepath.Join(dataDir, "raft"))
}

// acquireFlock takes an exclusive, non-blocking advisory lock on path (created if
// absent). Returns a release closure. The lock auto-releases on process exit (correct
// for a kill-9'd holder). Bars two concurrent offline runs.
func acquireFlock(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("clusteroffline: open lock %s: %w", path, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("clusteroffline: another offline tool holds %s: %w", path, err)
	}
	return func() {
		_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
		_ = f.Close()
	}, nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
