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
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"golang.org/x/sys/unix"
)

const (
	lockFileName            = cluster.DataDirLockFile // round-5 B3: ONE SSOT shared with the daemon's lifetime lock
	peerDialTimeout         = 1500 * time.Millisecond
	probeAdviceLookupBudget = 250 * time.Millisecond // diagnostic only; never hold a recovery refusal on DNS
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

	// atomicExchangeCheck overrides the round-5 B2 capability precondition. It is UNEXPORTED and per-call
	// (never package state — a package-level test hook is exactly the data race round-4 had to remove), so
	// only same-package tests can set it and no production caller can weaken the gate. nil = the real probe.
	atomicExchangeCheck func(dataDir string) error
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
	warnRootDataDirOwner(opts.DataDir, opts.Logger) // #6: nudge if run as root against a tether-owned data dir
	release, err := cluster.AcquireDataDirLock(opts.DataDir)
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
	exchangeCheck := opts.atomicExchangeCheck
	if exchangeCheck == nil {
		exchangeCheck = cluster.AtomicExchangeCapable
	}
	if err := exchangeCheck(opts.DataDir); err != nil {
		return nil, err
	}

	// Round-5 B1 / round-6: identify an INTERRUPTED prior run FIRST — before the state and capability
	// preconditions. A half-finished force-single is exactly the case whose symptoms (exit-70 crash-loop,
	// odd on-disk state) would otherwise surface as a generic refusal that tells the operator nothing.
	prior, err := readJournal(opts.DataDir)
	if err != nil {
		return nil, err
	}
	if prior != nil && prior.SelfID != opts.SelfID {
		return nil, fmt.Errorf("clusteroffline: an interrupted force-single for a DIFFERENT node (%q) is journalled in %s — resolve it before forcing %q", prior.SelfID, opts.DataDir, opts.SelfID)
	}

	// (b) empty-state refuse.
	exists, err := cluster.RaftStateExists(opts.DataDir)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, cluster.ErrNoExistingState
	}

	// Round-5 B2: the rebuild's store swap MUST be atomic to be crash-consistent. Prove the filesystem can
	// do it NOW — before a single byte of SQLite/Raft changes — instead of discovering it after the roster
	// prune has already made the node un-recoverable. There is no non-atomic fallback.

	// (d) peer-reachable HARD-REFUSE (BEFORE (c) mutates). RecoverCluster first restores
	// the newest snapshot and then replays the tail, so the current SQLite projection is
	// not sufficient: preview that exact recovery on copies and include every peer it can
	// revive in the confirmation + liveness fence.
	roster, err := readRoster(opts.DBPath, opts.SelfID)
	if err != nil {
		return nil, fmt.Errorf("clusteroffline: read roster: %w", err)
	}
	recoveredRoster, err := previewRecoveredRoster(opts.DataDir, opts.DBPath, opts.SelfID, opts.SelfRaftAddr)
	if err != nil {
		return nil, fmt.Errorf("clusteroffline: preview recovered roster: %w", err)
	}
	roster = mergePeers(roster, recoveredRoster)
	// Round-6: compute the confirmation only AFTER the roster is known — a journalled confirmation is
	// honoured ONLY for peers a prior run already pruned away, never for one still in the roster.
	confirmed := resumeConfirmation(opts.ConfirmedDead, prior, roster)
	if prior != nil {
		opts.Logger.Warn("clusteroffline: resuming an INTERRUPTED force-single (forward-completing the remaining phases)",
			"self", opts.SelfID, "phase_reached", prior.Phase, "confirmed_dead", confirmed)
	}
	if err := checkPeersDead(roster, confirmed); err != nil {
		return nil, err
	}

	// The point of no return: journal BEFORE the first mutation so an interruption anywhere below is
	// identifiable and forward-completable rather than an un-recoverable brick (round-5 B1).
	if prior == nil {
		if err := writeJournal(opts.DataDir, &forceSingleJournal{
			SelfID: opts.SelfID, SelfRaftAddr: opts.SelfRaftAddr, ConfirmedDead: confirmed, Phase: phaseStarted,
		}); err != nil {
			return nil, err
		}
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
	// G2 #12: prune the abandoned peers from cluster_nodes so the survivor's signed roster converges to
	// {self}. This is now a hard step: RecoverCluster's snapshot was taken BEFORE this direct-SQL prune;
	// leaving that snapshot in place lets a later resnapshot restore every abandoned peer.
	if len(roster) > 0 {
		if err := pruneRosterPeers(opts.DBPath, roster, opts.Now(), opts.Logger); err != nil {
			return roster, fmt.Errorf("clusteroffline: recovered but failed to prune abandoned roster: %w", err)
		}
	}
	// Rebuild the Raft store from the post-prune DB and atomically exchange it into place.
	// Its indices start above the old durable applied_index, so the new timeline cannot
	// be skipped by the FSM and the grow-ready snapshot contains the pruned roster + marker.
	applied, err := readAppliedIndexPath(opts.DBPath)
	if err != nil {
		return roster, fmt.Errorf("clusteroffline: read recovered applied_index: %w", err)
	}
	if err := cluster.RebuildSingleNodeFromDB(opts.DataDir, opts.DBPath, opts.SelfID, opts.SelfRaftAddr, applied, opts.Logger); err != nil {
		return roster, fmt.Errorf("clusteroffline: finalize recovered single-node raft: %w", err)
	}
	// R16 A3: mint a per-incident JS-reset epoch (stable across resumes — reuse the prior journal's if this
	// is a forward-completion) so the CLI's survivor JS-store move-aside backup name is idempotent.
	jsEpoch := opts.Now().UTC().Format("20060102-150405.000000000")
	if prior != nil && prior.JSResetEpoch != "" {
		jsEpoch = prior.JSResetEpoch
	}
	if err := writeJournal(opts.DataDir, &forceSingleJournal{
		SelfID: opts.SelfID, SelfRaftAddr: opts.SelfRaftAddr, ConfirmedDead: confirmed, Phase: phaseRaftRebuilt,
		JSResetEpoch: jsEpoch,
	}); err != nil {
		return roster, err
	}
	// Round-5 B1: this node is NOT usable yet — the caller still has to de-cluster nats.conf to standalone,
	// and until it does, a clustered conf at N=1 cannot form the JS meta quorum (broker exit 70). The old
	// wording ("force-single complete") declared success one whole irreversible phase early, which is what a
	// harness `grep -q` latched onto before SIGPIPE-killing the de-cluster. Say what is actually true.
	opts.Logger.Warn("clusteroffline: raft/DB phases done; node is a single-voter cluster but NOT yet bootable — the NATS config must still be rewritten for standalone JetStream (the caller does this next; re-run force-single to forward-complete if interrupted)",
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
	warnRootDataDirOwner(opts.DataDir, opts.Logger) // #6: nudge if run as root against a tether-owned data dir
	release, err := cluster.AcquireDataDirLock(opts.DataDir)
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
	// The on-disk snapshot/tail can recover a different FSM than the current SQLite
	// projection. Refuse before mutating if that exact recovery would revive a peer.
	recoveredRoster, err := previewRecoveredRoster(opts.DataDir, opts.DBPath, opts.SelfID, opts.SelfRaftAddr)
	if err != nil {
		return fmt.Errorf("clusteroffline: preview recovered roster: %w", err)
	}
	if len(recoveredRoster) > 0 {
		ids := make([]string, len(recoveredRoster))
		for i, p := range recoveredRoster {
			ids[i] = p.NodeID
		}
		return fmt.Errorf("clusteroffline: resnapshot is SINGLE-VOTER only but recovery would revive %d non-self node(s) %v from the Raft snapshot/log — run force-single with every peer confirmed dead before resnapshot", len(recoveredRoster), ids)
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

// previewRecoveredRoster runs the exact RecoverCluster path against copies of the
// offline DB and Raft store. The current SQLite roster alone is not authoritative:
// RecoverCluster restores the newest snapshot first and then replays its log tail,
// either of which may revive peers that a later direct-SQL prune removed.
func previewRecoveredRoster(dataDir, dbPath, selfID, selfRaftAddr string) ([]Peer, error) {
	root, err := os.MkdirTemp(dataDir, ".recovery-preview-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(root) }()

	previewDB := filepath.Join(root, "tether.db")
	if err := cluster.BackupDBFile(context.Background(), dbPath, previewDB); err != nil {
		return nil, fmt.Errorf("copy DB: %w", err)
	}
	if err := cloneRegularTree(filepath.Join(dataDir, "raft"), filepath.Join(root, "raft")); err != nil {
		return nil, fmt.Errorf("copy raft store: %w", err)
	}
	discard := slog.New(slog.NewTextHandler(io.Discard, nil))
	if err := cluster.RecoverSingleNode(root, previewDB, selfID, selfRaftAddr, discard); err != nil {
		return nil, err
	}
	return readRoster(previewDB, selfID)
}

func cloneRegularTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refusing non-regular raft path %q (%s)", path, info.Mode())
		}
		return copyFileSync(path, target)
	})
}

func mergePeers(groups ...[]Peer) []Peer {
	byID := make(map[string]Peer)
	order := make([]string, 0)
	for _, peers := range groups {
		for _, p := range peers {
			if _, ok := byID[p.NodeID]; !ok {
				order = append(order, p.NodeID)
			}
			prev := byID[p.NodeID]
			if p.RaftAddr == "" {
				p.RaftAddr = prev.RaftAddr
			}
			if p.NatsRoute == "" {
				p.NatsRoute = prev.NatsRoute
			}
			if p.TunnelAddr == "" {
				p.TunnelAddr = prev.TunnelAddr
			}
			byID[p.NodeID] = p
		}
	}
	out := make([]Peer, 0, len(order))
	for _, id := range order {
		out = append(out, byID[id])
	}
	return out
}

func readAppliedIndexPath(dbPath string) (uint64, error) {
	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return 0, err
	}
	defer func() { _ = db.Close() }()
	var raw string
	switch err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key='applied_index'`).Scan(&raw); err {
	case nil:
		v, parseErr := strconv.ParseUint(raw, 10, 64)
		if parseErr != nil {
			return 0, fmt.Errorf("corrupt applied_index %q: %w", raw, parseErr)
		}
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
		if alive, kind, addr := probePeer(p); alive {
			return fmt.Errorf("clusteroffline: HARD-REFUSE — peer %q accepted a TCP connection on its %s port (%s); "+
				"it is ALIVE, force-single would split-brain%s", p.NodeID, kind, addr, untrustworthyProbeAdvice(addr))
		}
	}
	return nil
}

// probePeer TCP-probes a peer's serving ports (raft/nats/tunnel) IN ORDER and returns the FIRST
// that completes a connection (the peer is alive on that port). A completed TCP connect — even if
// a later TLS handshake would fail — proves the peer is alive (the conservative gate, B-8). Shared
// by the offline HARD-REFUSE gate (checkPeersDead) and the display-only ProbePeers, so both probe
// identically.
func probePeer(p Peer) (alive bool, kind, addr string) {
	for _, ap := range []struct{ kind, addr string }{
		{"raft", p.RaftAddr}, {"nats", stripScheme(p.NatsRoute)}, {"tunnel", p.TunnelAddr},
	} {
		if ap.addr == "" {
			continue
		}
		conn, err := net.DialTimeout("tcp", ap.addr, peerDialTimeout)
		if err == nil {
			_ = conn.Close()
			return true, ap.kind, ap.addr
		}
	}
	return false, "", ""
}

// WHY THE PROBE STAYS "A COMPLETED TCP CONNECT MEANS ALIVE", AND WHAT IS ADDED INSTEAD
//
// origin: line-2 external review follow-up, found while root-causing simcluster drill 42. On a host
// running a fake-IP resolver (mihomo/clash: 198.18.0.0/15) or any wildcard/captive DNS, EVERY name
// resolves and the TUN device completes the handshake. A peer that is genuinely dead then reads as
// ALIVE, and this gate HARD-REFUSES a legitimate force-single — permanently, with a message that
// sends the operator hunting a machine that does not exist. Measured, not theorised: three drill-42
// runs produced the same ASSERT-FAIL from exactly this.
//
// The fix is NOT to make the probe smarter. Requiring a raft/TLS handshake before believing a peer
// is alive would flip the failure into the DANGEROUS direction: a live peer with an expired cert or
// a version skew would read as dead, and force-single would split-brain — which is the accident B-8
// and audit CC-2 built this gate to prevent. Refusing is the safe verdict and it stays.
//
// What is added is the missing half: SAY WHY the verdict may be meaningless. Both signals below are
// observations about the HOST, never about the peer, and neither can turn a refusal into an approval.
var (
	// syntheticProbeNet is RFC 2544's benchmarking range. It must never carry real traffic, so a
	// cluster peer address inside it is by definition synthetic — this is the range mihomo/clash hands
	// out for fake-IP.
	syntheticProbeNet = &net.IPNet{IP: net.IPv4(198, 18, 0, 0).To4(), Mask: net.CIDRMask(15, 32)}
	// nxdomainCanary is reserved by RFC 2606 and MUST NOT resolve. If it does, the host resolver
	// fabricates answers for names that do not exist, and no TCP probe on this host means anything.
	nxdomainCanary = "tether-liveness-canary.invalid"
	// lookupHost is a seam so the canary check is testable without a resolver.
	// The context is mandatory: advice must never turn a bounded safety refusal
	// into an unbounded wait on a broken DNS server.
	lookupHost = net.DefaultResolver.LookupHost
)

// untrustworthyProbeAdvice returns a diagnostic paragraph (leading newline) when the host makes TCP
// liveness unmeasurable, or "" when it does not. Appended to the HARD-REFUSE message only.
func untrustworthyProbeAdvice(addr string) string {
	var reasons []string
	if host, _, err := net.SplitHostPort(addr); err == nil {
		if ips, lerr := lookupHostForProbeAdvice(host); lerr == nil {
			for _, s := range ips {
				if ip := net.ParseIP(s); ip != nil && syntheticProbeNet.Contains(ip) {
					reasons = append(reasons, fmt.Sprintf(
						"%s resolves to %s, inside 198.18.0.0/15 (RFC 2544 benchmarking; this is what "+
							"mihomo/clash hands out for fake-IP). That address cannot be a real cluster peer.",
						host, s))
					break
				}
			}
		}
	}
	if ips, err := lookupHostForProbeAdvice(nxdomainCanary); err == nil && len(ips) > 0 {
		reasons = append(reasons, fmt.Sprintf(
			"this host resolves %s (RFC 2606 reserved — it MUST NOT resolve) to %s, so it fabricates "+
				"addresses for names that do not exist and every TCP liveness probe on it reports ALIVE.",
			nxdomainCanary, strings.Join(ips, ", ")))
	}
	if len(reasons) == 0 {
		return ""
	}
	return "\n\n  THE PROBE ABOVE MAY BE MEANINGLESS ON THIS HOST:\n    - " +
		strings.Join(reasons, "\n    - ") +
		"\n\n  The refusal STANDS — a probe that cannot tell alive from dead is not evidence that the peer is\n" +
		"  dead, and force-single on a live peer splits the brain. Fix the host first (stop the fake-IP\n" +
		"  resolver, or point /etc/resolv.conf at one that returns NXDOMAIN), then re-run and let the probe\n" +
		"  give a real answer."
}

func lookupHostForProbeAdvice(host string) ([]string, error) {
	// ctx-none: the whole force-single path is ctx-free by construction — ForceSingleOptions carries no
	// context and the offline tool runs with the daemon stopped. The budget below is the only bound this
	// lookup gets, which is why it is a named constant rather than an inherited deadline.
	ctx, cancel := context.WithTimeout(context.Background(), probeAdviceLookupBudget)
	defer cancel()
	return lookupHost(ctx, host)
}

// PeerLiveness is one peer's DISPLAY-only probe verdict for the online force-single report. The
// authoritative anti-split-brain HARD-REFUSE remains CheckPeersDead; this only enriches the
// operator's view (the report's Alive / OnPort fields).
type PeerLiveness struct {
	NodeID string
	Alive  bool
	OnPort string // "<kind> <addr>" that answered (e.g. "raft 10.0.0.2:7400"), "" if dead
}

// ProbePeers returns a per-peer liveness verdict (TCP-probing each peer's raft/nats/tunnel ports)
// for the online force-single report. It does NOT gate — CheckPeersDead is the authoritative gate;
// this populates the report so the operator sees WHICH peer is alive on WHICH port, not just a
// terse refusal string.
func ProbePeers(roster []Peer) []PeerLiveness {
	out := make([]PeerLiveness, 0, len(roster))
	for _, p := range roster {
		pl := PeerLiveness{NodeID: p.NodeID}
		if alive, kind, addr := probePeer(p); alive {
			pl.Alive = true
			pl.OnPort = kind + " " + addr
		}
		out = append(out, pl)
	}
	return out
}

// readRoster reads every non-self cluster_nodes row from the offline DB (read-only).
func readRoster(dbPath, selfID string) ([]Peer, error) {
	db, err := storage.OpenReadOnly("file:" + dbPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()
	return ReadRoster(db, selfID)
}

// ReadRoster returns the OTHER cluster nodes (peers) off an ALREADY-OPEN read-only handle. It is the
// exported form readRoster delegates to, so the offline force-single tool (its own DB handle) and the
// online broker path (node.RODB()) share ONE roster read — guaranteed parity. Refuses if selfID is not
// in cluster_nodes (won't rewrite raft config for an unknown node).
func ReadRoster(ro *sql.DB, selfID string) ([]Peer, error) {
	var selfRows int
	if err := ro.QueryRow(`SELECT COUNT(*) FROM cluster_nodes WHERE node_id = ?`, selfID).Scan(&selfRows); err != nil {
		return nil, err
	}
	if selfRows != 1 {
		return nil, fmt.Errorf("clusteroffline: self-id %q is not present in cluster_nodes; refusing to rewrite raft config for an unknown node", selfID)
	}
	rows, err := ro.Query(`SELECT node_id, raft_addr, nats_route, tunnel_addr FROM cluster_nodes WHERE node_id != ?`, selfID)
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

// CheckPeersDead is the exported form of the peer-liveness HARD-REFUSE (the anti-split-brain gate): it
// requires confirmed to list EVERY peer and TCP-probes each peer's raft/nats/tunnel ports, refusing if
// ANY completes a connection (the peer is ALIVE → force-single would split-brain). Shared by the offline
// tool and the online broker handler so both gate identically.
func CheckPeersDead(roster []Peer, confirmed []string) error {
	return checkPeersDead(roster, confirmed)
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

// pruneRosterPeers DELETEs the abandoned peers from cluster_nodes and bumps the roster + topology
// generation counters via direct-SQL — the offline (daemon-down) equivalent of
// cluster.PlanClusterNodePrune, so the online and offline force-single paths leave the SAME cluster_nodes
// state ({self} only). One transaction: the per-peer deletes + both monotone MAX(existing+1, now) gen
// bumps. Node IDs come from the trusted recovered roster read; parameterized regardless.
func pruneRosterPeers(dbPath string, peers []Peer, now time.Time, logger *slog.Logger) error {
	db, err := storage.OpenWAL("file:" + dbPath)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var total int64
	var departedHosts []string // G3 #1: public_hosts of the peers actually deleted (for seed drop-only)
	for _, p := range peers {
		// Read the peer's public_host BEFORE deleting so we can drop its client endpoint from the seeds
		// in the same txn (INV: prune + seed-write share ONE atomic boundary — a mid-crash never leaves
		// "roster pruned but seeds still advertise the dead peer").
		var host string
		if err := tx.QueryRow(`SELECT public_host FROM cluster_nodes WHERE node_id = ?`, p.NodeID).Scan(&host); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		res, derr := tx.Exec(`DELETE FROM cluster_nodes WHERE node_id = ?`, p.NodeID)
		if derr != nil {
			return derr
		}
		if n, aerr := res.RowsAffected(); aerr == nil && n > 0 {
			total += n
			if host != "" {
				departedHosts = append(departedHosts, host)
			}
		}
	}
	// Change-gated (parity with the online rosterGenBumpStmt/topologyGenBumpStmt): bump ONLY if a row was
	// actually deleted, so a re-prune of already-absent rows advances NEITHER counter (idempotent, matching
	// PlanClusterNodePrune). MAX(existing+1, now.UnixNano()) mirrors the online monotone shape; keys are
	// cluster's exported SSOT.
	if total > 0 {
		nowNano := strconv.FormatInt(now.UTC().UnixNano(), 10)
		for _, key := range []string{cluster.MetaKeyRosterGeneration, cluster.MetaKeyTopologyGeneration} {
			if _, err := tx.Exec(
				`INSERT INTO cluster_meta(key, value) VALUES(?, ?) `+
					`ON CONFLICT(key) DO UPDATE SET value = MAX(CAST(cluster_meta.value AS INTEGER)+1, CAST(excluded.value AS INTEGER))`,
				key, nowNano); err != nil {
				return err
			}
		}
		// G3 #1: converge the published seeds too — drop the departed peers' client endpoints (drop-only:
		// the offline path synthesizes NO new endpoint, so it needs no scheme/port template and introduces
		// no un-vetted dial target). Change-gated on an actual removal; the empty-set floor (INV-2) keeps
		// the stored set if the drop would empty it (never wipe / strand cold-start clients).
		if err := convergeSeedsDropHosts(tx, departedHosts, nowNano, logger); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// convergeSeedsDropHosts drops the departed peers' client endpoints from the replicated seed set within
// the caller's txn (G3 #1 offline drop-only). It reads stored seed_endpoints, removes any URL whose host
// is a departed peer, and — ONLY if that actually changed the set AND left it non-empty (INV-2: never
// wipe / strand cold-start clients) — writes it back + MAX-floor bumps seed_generation (anti-rollback
// across a restart replay). A VIP/LB host that is not a departed peer is kept.
func convergeSeedsDropHosts(tx *sql.Tx, departedHosts []string, nowNano string, logger *slog.Logger) error {
	if len(departedHosts) == 0 {
		return nil
	}
	var epRaw string
	if err := tx.QueryRow(`SELECT value FROM cluster_meta WHERE key = ?`, cluster.MetaKeySeedEndpoints).Scan(&epRaw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil // no seeds published → nothing to converge
		}
		return err
	}
	var endpoints []string
	for _, e := range strings.Split(epRaw, "\n") {
		if e != "" {
			endpoints = append(endpoints, e)
		}
	}
	filtered := cluster.SeedEndpointsDropHosts(endpoints, departedHosts)
	if len(filtered) == len(endpoints) {
		return nil // change-gate: nothing dropped
	}
	if len(filtered) == 0 {
		// A8: the drop would EMPTY the published seed set — every published endpoint pointed at a
		// now-departed broker (self absent from the set: undialable/loopback host, or an operator-curated
		// subset). Keep the stale set (INV-2 floor: never wipe / strand cold-start clients) but WARN loudly
		// (parity with the online sibling deriveAndConvergeSeedsFromRoster's empty-derive warning), since the
		// survivor now advertises ONLY dead endpoints via the signed SeedBundle.
		if logger != nil {
			logger.Warn("clusteroffline: force-single dropped every published seed (all pointed at departed brokers) — keeping the stale set so cold-start clients are not stranded, but it now advertises only dead endpoints; run `cluster seeds publish` on the survivor to converge them",
				"stale_endpoint_count", len(endpoints))
		}
		return nil // empty-set floor (never wipe)
	}
	if _, err := tx.Exec(
		`INSERT INTO cluster_meta(key, value) VALUES(?, ?) `+
			`ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		cluster.MetaKeySeedEndpoints, strings.Join(filtered, "\n")); err != nil {
		return err
	}
	if _, err := tx.Exec(
		`INSERT INTO cluster_meta(key, value) VALUES(?, ?) `+
			`ON CONFLICT(key) DO UPDATE SET value = MAX(CAST(cluster_meta.value AS INTEGER)+1, CAST(excluded.value AS INTEGER))`,
		cluster.MetaKeySeedGeneration, nowNano); err != nil {
		return err
	}
	return nil
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
	warnRootDataDirOwner(opts.DataDir, opts.Logger) // #6: nudge if run as root against a tether-owned data dir
	release, err := cluster.AcquireDataDirLock(opts.DataDir)
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
		redactSecretColumns(rows)
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

// secretDumpColumns are the columns whose VALUE is a live credential rather than a
// record of one. They are replaced in the forensic dump.
//
// origin: prerelease audit, §3 MINOR sweep. dumpDivergent writes every user table
// verbatim, and proxy_subscribers.psk is documented in migration 0006 as "base64
// Shadowsocks password (recoverable)" — so the dump is a plaintext copy of every
// subscriber's live proxy password. The file is 0600, but its whole purpose is to be
// KEPT: the operator is told to hold it as forensics before a destructive recover, and
// the runbook has them copy it off the box. A recovery artefact should not be a secret
// store.
//
// The row is still there, and so is the fact that the column had a value — which is all
// the forensic question ("what did this divergent node hold") actually needs. What is
// removed is the ability to USE it.
//
// Hashes are deliberately NOT redacted: token_hash and pin_hash are already one-way, and
// their presence is exactly the kind of thing a forensic reader compares across nodes.
var secretDumpColumns = map[string]bool{
	"psk": true,
}

// redactSecretColumns replaces credential VALUES in place, leaving a marker so the reader
// knows something was withheld rather than absent.
func redactSecretColumns(rows []map[string]any) {
	for _, row := range rows {
		for col := range row {
			if secretDumpColumns[col] && row[col] != nil {
				row[col] = "[redacted: a live credential, withheld from the forensic dump]"
			}
		}
	}
}

func sizeOf(st os.FileInfo) int64 {
	if st == nil {
		return 0
	}
	return st.Size()
}

// dumpTable returns every row of tbl as a slice of column->value maps. tbl comes
// from dumpableTables' runtime sqlite_master enumeration (schema names, never user
// input), so the "SELECT * FROM "+tbl interpolation is safe.
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

// RootAgainstNonRootDir reports the #6 hazard: an offline op running as euid==0 against a data dir
// owned by a DIFFERENT (non-root) user. A root-run `cluster init` / offline op then creates
// root-owned tether.lock / raft/ / tether.db that a later `sudo -u tether` op cannot open — EACCES
// at the flock, or the raft.db bolt probe's HARD error (cluster.RaftStoreLockedByDaemon returns a
// hard error, not ErrTimeout, on an unreadable raft.db). Pure so it is table-testable off-root.
func RootAgainstNonRootDir(euid int, dirUID uint32) bool {
	return euid == 0 && dirUID != 0
}

// warnRootDataDirOwner emits the #6 WARN (best-effort; it NEVER fails the op) when an offline op
// runs as root against a data dir owned by the (non-root) broker user. The durable guidance is the
// docs mandate to run offline ops as `sudo -u tether`; this is a loud nudge, not a gate (a hard
// refuse could strand a legitimate root recovery). A stat error is ignored — the op's own
// preconditions handle a missing/unreadable data dir.
func warnRootDataDirOwner(dataDir string, logger *slog.Logger) {
	if logger == nil {
		return
	}
	var st unix.Stat_t
	if err := unix.Stat(dataDir, &st); err != nil {
		return
	}
	if RootAgainstNonRootDir(os.Geteuid(), st.Uid) {
		logger.Warn("clusteroffline: offline op running as root against a non-root-owned data dir; "+
			"files it creates (tether.lock, raft/, tether.db) will be root-owned and a later "+
			"`sudo -u tether` op will be denied — run this as the data-dir owner "+
			"(e.g. `sudo -u tether tether cluster …`)",
			"data_dir", dataDir, "dir_uid", st.Uid, "euid", os.Geteuid())
	}
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
