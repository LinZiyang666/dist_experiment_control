package broker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
)

// clusterCodeFor derives a stable machine Code (B2 item 4) from a broker cluster error by
// recognizing the broker's OWN error messages. These substrings are pinned by the D7 suite, so a
// drift breaks a test rather than silently mis-coding. An unrecognized error yields "" → the CLI
// classifies it exitInternal (70). This is server-side self-recognition (the broker owns both the
// string and the test); the CLI never sniffs prose — it maps code→class via a table. Free-form
// errors that match no pattern stay "" (a documented B2.1 widening target).
func clusterCodeFor(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	switch {
	case strings.Contains(s, "catch_up_stalled"):
		return adminsock.CodeCatchUpStalled
	case strings.Contains(s, "still HOMES"): // B3 item 7 ErrRemoveOwnsResources (authored literal)
		return adminsock.CodeRemoveOwnsResources
	case strings.Contains(s, "not in the raft configuration"),
		strings.Contains(s, "is not a voter"),
		strings.Contains(s, "cannot retire the last voter"):
		return adminsock.CodeNotAVoter
	default:
		return ""
	}
}

// clusterstatus.go — D7 §17 status/doctor self-report + the adminsock adapter
// (build-and-prove; part of the guard-excluded ClusterAdmin mechanism). This is ONE
// broker's self-report; the ctl/NATS view aggregates reports from every reachable
// broker (it NEVER dials :7400), and the offline mode-B view (cluster status
// --offline) is the only one that direct-pings peers. Health → exit code is the
// healthExitCode SSOT.

// Health strings + exit codes (§17 contract).
const (
	healthHealthyHA   = "HEALTHY_HA"
	healthDegraded    = "DEGRADED"
	healthQuorumLost  = "QUORUM_LOST"
	healthForceSingle = "FORCE_SINGLE"

	// statusSchemaVersion is the --json schema discriminator (§17 / external review
	// F4); bump on any breaking shape change so monitors can negotiate.
	statusSchemaVersion = 1

	// certRotationWindow is how long the previous tunnel cert pin stays valid after a
	// rotate-tunnel-cert (the cutover window agents reconnect within).
	certRotationWindow = 24 * time.Hour
)

// healthExitCode is the single source of truth mapping health → process exit code
// (0=HEALTHY-HA, 1=DEGRADED-writable, 2=read-only/quorum-lost, 3=force-single).
func healthExitCode(health string) int {
	switch health {
	case healthHealthyHA:
		return 0
	case healthDegraded:
		return 1
	case healthQuorumLost:
		return 2
	case healthForceSingle:
		return 3
	default:
		return 1
	}
}

// healthLabel maps the legacy internal health string (+ the voter count) to the C6 建议6 five-state
// operator vocab, WITHOUT changing the legacy Health string or the 0/1/2/3 ExitCode contract. NOT-HA is
// the N<=2 case (works, but no production HA) — PROMOTED from a verdict to a first-class state. It
// shares exit 1 with DEGRADED-WRITABLE (the FaultTolerance==0 branch already returns healthDegraded →
// exit 1 before any topo check, and ProjectQuorum(v,false).FaultTolerance==0 ⟺ v<=2), disambiguated by
// this label. "" for an offline snapshot with no computed health (the caller sets it inline there).
func healthLabel(health string, voters int) string {
	switch health {
	case healthHealthyHA:
		return "HEALTHY-HA"
	case healthDegraded:
		if voters <= 2 {
			return "NOT-HA"
		}
		return "DEGRADED-WRITABLE"
	case healthQuorumLost:
		return "READ-ONLY"
	case healthForceSingle:
		return "FORCE-SINGLE"
	default:
		return ""
	}
}

type rosterRow struct {
	nodeID, name, phase string
	certFP              string         // B5 OPS#7: current tunnel-cert fingerprint (public; '' if unset)
	certFPPrev          sql.NullString // previous fp during a rotation window
	certValid           sql.NullTime   // cert_fp_valid_until
}

// StatusReport builds this broker's self-report (view = "ctl-nats" or "offline").
// It joins the roster (cluster_nodes) against the live raft configuration for the
// role + the no-silent-fork INCONSISTENT cross-render, derives the leader, applied
// lag (self only; a leader cannot read a follower's cursor — that is the D9
// transport), the force-single marker, and the stream actual/target placeholder.
func (a *ClusterAdmin) StatusReport(view string) (*adminsock.ClusterStatusReport, error) {
	// Materialize roster + force-single marker FIRST (close rows) — no nested query
	// under the single-conn pool (the D6 deadlock lesson).
	var roster []rosterRow
	var forceSingle bool
	var portsUsed int
	var portsKnown bool
	selfID := a.node.SelfID()
	if err := a.node.BoundedStaleRead(func(db *sql.DB) error {
		rows, err := db.Query(`SELECT node_id, name, phase, cert_fp, cert_fp_prev, cert_fp_valid_until FROM cluster_nodes ORDER BY node_id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r rosterRow
			if err := rows.Scan(&r.nodeID, &r.name, &r.phase, &r.certFP, &r.certFPPrev, &r.certValid); err != nil {
				_ = rows.Close()
				return err
			}
			roster = append(roster, r)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}
		var v string
		err = db.QueryRow(`SELECT value FROM cluster_meta WHERE key=?`, cluster.MetaKeyForceSingle).Scan(&v)
		if err == nil {
			forceSingle = true
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		// B5 OPS#9: self-row ports_used = exposes THIS broker homes (the countOwnedExposes
		// pattern — home_broker-scoped, since port_allocations is the replicated cluster table).
		// Single COUNT after the roster rows are closed (single-conn safe).
		if a.portBandHigh > 0 && a.portBandHigh >= a.portBandLow {
			if e := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE home_broker=? AND state='ALLOCATED'`, selfID).Scan(&portsUsed); e != nil {
				return e
			}
			portsKnown = true
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cluster status: read roster: %w", err)
	}

	// B5 OPS#9: self-row disk free % via statfs (NO DB; never on the offline path — a stale/
	// unmounted store dir can hang). Gate on StoreDir set + a non-zero total (honest absence).
	diskFreePct, diskKnown := 0, false
	if a.storeDir != "" {
		if used, total, derr := diskUsage(a.storeDir); derr == nil && total > 0 {
			diskFreePct = int((float64(total-used) / float64(total)) * 100.0)
			diskKnown = true
		}
	}
	portsTotal := 0
	if portsKnown {
		portsTotal = a.portBandHigh - a.portBandLow + 1
	}

	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return nil, fmt.Errorf("cluster status: raft config: %w", err)
	}
	role := map[string]string{}
	leaderID := ""
	voters := 0
	for _, s := range cfg {
		r := "voter"
		if !s.Voter {
			r = "learner"
		}
		if s.Leader {
			r = "leader"
			leaderID = s.NodeID
		}
		if s.Voter {
			voters++
		}
		role[s.NodeID] = r
	}

	// audit-publish F5 / xx-concurrency F2: applied lag is COMMAND-DOMAIN AppliedIndex measured
	// against the leader's OWN AppliedIndex — NEVER raft CommitIndex. CommitIndex also counts
	// config/barrier entries the command cursor never advances on, so commit-vs-applied shows a
	// phantom lag that would falsely DEGRADE a healthy leader right after an election. The leader
	// is the lag reference, so its own lag is 0; a peer lags by leaderApplied - peerApplied.
	selfApplied, _ := a.node.AppliedIndex()
	selfLag := uint64(0)
	topoDesired, _ := cluster.TopologyGeneration(a.node.RODB()) // C3: cluster-wide desired topology gen
	streamTarget := jsstream.ReplicasFor(voters)
	// External-review F1: report the REAL stream actual, not a synthesized actual==target. An
	// unwired probe (N=1) keeps the optimistic default; a wired-but-incomplete observation
	// reports 0 (unknown → a visible deficit in `actual/target`, never a false green).
	streamActual := streamTarget
	if a.streamObserve != nil {
		if rep, err := a.streamObserve(); err == nil && rep.Observed {
			streamActual = minStreamActual(rep, streamTarget)
		} else {
			streamActual = 0
		}
	}

	rep := &adminsock.ClusterStatusReport{SchemaVersion: statusSchemaVersion, View: view, LeaderID: leaderID}
	// §17 row 3: scatter-gather the broker-only cursor probe ONCE (if wired) so each peer's
	// reachability + applied-lag is REAL, not a blanket self-report. self is always reachable
	// (it is answering this request). nil poll ⇒ honest "unverified" for peers.
	var health map[string]proto.ClusterHealthResp
	if a.healthPoll != nil {
		health = a.healthPoll()
	}
	reachOf := func(nodeID string) (reachable bool, source string, lag uint64) {
		if nodeID == a.node.SelfID() {
			return true, "self", selfLag
		}
		if a.healthPoll == nil {
			return false, "unverified", 0 // single-broker view: we genuinely don't know
		}
		if r, ok := health[nodeID]; ok {
			l := uint64(0)
			if selfApplied > r.AppliedIndex { // command-domain vs the leader's command cursor (F2/F5)
				l = selfApplied - r.AppliedIndex
			}
			return true, "nats-health", l
		}
		return false, "nats-health", 0 // polled but no answer ⇒ unreachable
	}
	rosterIDs := map[string]bool{}
	for _, r := range roster {
		rosterIDs[r.nodeID] = true
		ro, inCfg := role[r.nodeID]
		// INCONSISTENT cross-render: roster says VOTER but raft config disagrees (or
		// the row is absent from the config), or vice-versa.
		inconsistent := (r.phase == phaseVoter && (!inCfg || ro == "learner")) ||
			(!inCfg && r.phase != phaseRetiring && r.phase != phaseAddFailed)
		reachable, source, lag := reachOf(r.nodeID)
		ns := adminsock.ClusterNodeStatus{
			NodeID: r.nodeID, Name: r.name, Phase: r.phase, Role: ro,
			AppliedLag: lag, AccountNkMatch: true,
			StreamActual: streamActual, StreamTarget: streamTarget,
			Reachable: reachable, ReachSource: source,
			Inconsistent: inconsistent,
		}
		// B6 OPS#4: stamp each broker's live self-reported version (from the health poll).
		// C3: stamp the topology reconcile self-report (applied/observed/reason/reported).
		if hr, ok := health[r.nodeID]; ok {
			ns.ReleaseVersion = hr.ReleaseVersion
			ns.ProtoVer = hr.ProtoVer
			ns.TopoApplied = hr.TopoApplied
			ns.TopoObserved = hr.TopoObserved
			ns.TopoReconcileReason = hr.TopoReconcileReason
			ns.TopoReported = hr.TopoReported
		}
		// B5 OPS#7: cert fingerprints (public) for every row; CertValidSecs derived now→valid.
		ns.CertFP = r.certFP
		if r.certFPPrev.Valid {
			ns.CertFPPrev = r.certFPPrev.String
		}
		if r.certValid.Valid {
			if secs := int64(r.certValid.Time.Sub(a.now()).Seconds()); secs > 0 {
				ns.CertValidSecs = secs
			}
		}
		// B5 OPS#9: capacity is self-row only (a leader cannot statfs a peer's disk).
		if r.nodeID == selfID {
			if diskKnown {
				ns.DiskFreePct = diskFreePct
			}
			if portsKnown {
				ns.PortsUsed = portsUsed
				ns.PortsTotal = portsTotal
			}
			// C3: the self row's topology report is authoritative from the local reconciler (self does
			// not poll itself), overriding any health-map echo.
			if a.topoSelf != nil {
				ns.TopoReported = true
				if ts := a.topoSelf(); ts != nil {
					ns.TopoApplied = ts.Applied
					ns.TopoObserved = ts.Observed
					ns.TopoReconcileReason = ts.Reason
				}
			}
		}
		rep.Nodes = append(rep.Nodes, ns)
	}
	// A raft voter with NO roster row is the loud INCONSISTENT case (§8.1).
	for _, s := range cfg {
		if s.Voter && !rosterIDs[s.NodeID] {
			reachable, source, _ := reachOf(s.NodeID)
			rep.Nodes = append(rep.Nodes, adminsock.ClusterNodeStatus{
				NodeID: s.NodeID, Role: role[s.NodeID], Reachable: reachable,
				ReachSource: source, Inconsistent: true,
			})
		}
	}

	rep.TopoDesired = topoDesired // C3: cluster-wide desired topology generation
	rep.Health, rep.Banner, rep.NextStep = computeHealth(forceSingle, leaderID, voters, topoDesired, rep.Nodes)
	rep.ExitCode = healthExitCode(rep.Health)
	rep.HealthLabel = healthLabel(rep.Health, voters) // C6 建议6: additive 5-state label; legacy Health/ExitCode unchanged
	// B5 OPS#7: a tunnel-cert whose rotation window is closing is an ADVISORY only — a rotation
	// in-window is healthy and even past valid_until only the PREVIOUS pin lapsed (the cert still
	// serves), so it NEVER changes health/ExitCode. Appended to the banner; no-op when no cert is
	// near expiry (so the common case is byte-identical).
	if adv := certExpiryAdvisory(rep.Nodes, certRotationWindow); adv != "" {
		if rep.Banner != "" {
			rep.Banner += " "
		}
		rep.Banner += adv
	}
	// B1: stamp the view source (which broker self-reported, and whether it is the
	// authoritative leader view) and the plain-language voter-count verdict.
	rep.ViewHost = a.node.SelfID()
	rep.IsLeaderView = leaderID == a.node.SelfID()
	rep.Verdict = clusterVerdict(voters, streamsAtTarget(rep.Nodes), rep.Health)
	return rep, nil
}

// streamsAtTarget reports whether every node row with a KNOWN target is at or above it.
// StreamTarget==0 means "not observed" and never counts as a deficit (matches computeHealth F1).
func streamsAtTarget(nodes []adminsock.ClusterNodeStatus) bool {
	for _, n := range nodes {
		if n.StreamTarget > 0 && n.StreamActual < n.StreamTarget {
			return false
		}
	}
	return true
}

// clusterVerdict renders the B1 plain-language, voter-count-keyed verdict — the AUTHORITATIVE
// broker-host/socket view (it has the real raft voter count). The ctl-over-NATS path uses a
// separate reachability-based verdict because it only sees lightweight health replies, never the
// voter count. F is the literal fault tolerance from ProjectQuorum.
func clusterVerdict(voters int, streamsOK bool, health string) string {
	switch {
	case voters == 0:
		return ""
	case voters <= 1:
		return "1 broker — NO redundancy: if it dies, the cluster stops until restored."
	case voters == 2:
		return "2 brokers — NO fault-tolerant writes: losing either makes the cluster read-only (quorum needs both)."
	case streamsOK && health == healthHealthyHA:
		f := ProjectQuorum(voters, false).FaultTolerance
		return fmt.Sprintf("HA active — writes are replicated; the cluster survives %d broker failure(s).", f)
	default:
		return "HA configured but DEGRADED right now — see the table; full redundancy is not in place."
	}
}

// minStreamActual is the conservative cluster-wide stream actual: the smallest observed
// replica count across all streams (so a single under-replicated stream shows the deficit).
// No streams ⇒ the optimistic target (nothing to be under-replicated).
func minStreamActual(rep ReplicaReport, target int) int {
	if len(rep.Streams) == 0 {
		return target
	}
	m := rep.Streams[0].Actual
	for _, s := range rep.Streams[1:] {
		if s.Actual < m {
			m = s.Actual
		}
	}
	return m
}

// computeHealth derives the health verdict from the self-report. QUORUM_LOST is
// emitted ONLY from a positive no-leader observation (§B-9: never from absence of
// reports — that false-quorum-lost chain induces a wrong force-single).
func computeHealth(forceSingle bool, leaderID string, voters int, topoDesired uint64, nodes []adminsock.ClusterNodeStatus) (health, banner, nextStep string) {
	if forceSingle {
		return healthForceSingle,
			"running in force-single (single node, no integrity) — recover as soon as peers are restored",
			"on each returning node: cluster recovery rejoin prepare --self-id <node-id> --dump-divergent <file>, then re-admit it with cluster join approve from the leader"
	}
	if leaderID == "" {
		// Review M2: this is ONE broker's local view (the multi-broker NATS
		// aggregation is D9). An empty LeaderWithID() on a partitioned MINORITY broker
		// is "I can't see a leader", which must NOT be presented as authoritative
		// cluster consensus or as an immediate force-single trigger (the false-quorum-
		// lost -> wrong-force-single chain §17/B-9 forbids). Exit 2 (read-only) is
		// honest for THIS broker; the banner makes the single-broker scope explicit and
		// conditions force-single on cross-checking the others.
		return healthQuorumLost,
			"THIS broker sees no leader — it is READ-ONLY (a single-broker view, NOT cluster consensus). Check the OTHER brokers before acting; force-single ONLY after confirming a majority is PERMANENTLY dead.",
			"on each survivor: cluster status --offline --db /var/lib/tether/tether.db ; then only after confirming peers dead: cluster recovery force-single --self-id <this-node-id> --self-addr <this-host:7400> --confirm-peers-dead <ids...>"
	}
	degraded := false
	var topoStuck, topoBehind bool // C3-M5: distinguish a wedged reconcile from a still-catching-up one
	for _, n := range nodes {
		if n.Inconsistent || n.Phase == phaseCatchingUp || n.Phase == phaseAddFailed || n.Phase == phaseDraining || n.Phase == phaseRetiring {
			degraded = true
		}
		// External-review F5: a VOTER that a REAL health poll found UNREACHABLE, or that
		// trails the leader's commit beyond the §17 lag threshold, is DEGRADED — never call a
		// cluster HEALTHY_HA when a voter is observably down or behind (the §17 exit-code
		// contract). "nats-health" means it was actually polled (vs "unverified"/"self").
		if n.Phase == phaseVoter {
			if n.ReachSource == "nats-health" && !n.Reachable {
				degraded = true
			}
			if n.AppliedLag > observeLagThreshold {
				degraded = true
			}
			// C3 acceptance ("任一 broker 未完成 topology apply 时不显 HEALTHY-HA"): a REACHED, REPORTING
			// voter whose live NATS topology has not caught the desired generation is DEGRADED. Gated on
			// TopoReported (presence) — NOT a TopoObserved>0 magnitude guard — so a just-promoted voter at
			// observed=0 with topoDesired>0 still degrades. observed<desired subsumes applied<desired
			// (never rendered) AND observed<applied (swapped-not-reloaded), since observed≤applied≤desired.
			reached := n.ReachSource == "self" || (n.ReachSource == "nats-health" && n.Reachable)
			if topoDesired > 0 && n.TopoReported && reached && n.TopoObserved < topoDesired {
				degraded = true
				// C3-M5: a STUCK reconcile (rejected render / unknown directive) needs a DIFFERENT next
				// step (fix the conf / `reconcile nats --manual`) than a broker still catching up.
				if strings.Contains(n.TopoReconcileReason, "unrecognized directive") ||
					strings.Contains(n.TopoReconcileReason, "nats-server -t") ||
					strings.Contains(n.TopoReconcileReason, "render") {
					topoStuck = true
				} else {
					topoBehind = true
				}
			}
		}
		// External-review F1: a JS stream below its target replica count (observed actual <
		// target) is DEGRADED — never report HEALTHY_HA while replication is short of target
		// (a node loss could then lose data). StreamTarget==0 means "not observed" → skip.
		if n.StreamTarget > 0 && n.StreamActual < n.StreamTarget {
			degraded = true
		}
		// B6 OPS#4 (B5 fold): a self-row with low free disk (<10%) or near-exhausted ports
		// (>=90% used) is DEGRADED. These are populated ONLY on the self row (a peer carries 0 =
		// not-set, omitempty), so the >0 guards avoid false-flagging peers. This cannot override
		// FORCE_SINGLE/QUORUM_LOST (those return BEFORE this loop).
		if n.DiskFreePct > 0 && n.DiskFreePct < 10 {
			degraded = true
		}
		if n.PortsTotal > 0 && n.PortsUsed*10 >= n.PortsTotal*9 {
			degraded = true
		}
	}
	proj := ProjectQuorum(voters, false)
	if proj.FaultTolerance == 0 {
		return healthDegraded,
			fmt.Sprintf("only tolerates %d failures (%d voters, quorum=%d) — add a node for HA", proj.FaultTolerance, proj.Voters, proj.Quorum),
			"on the NEW broker: cluster join prepare --node-id <id> --raft-addr <host:7400> --nats-route nats://<host:6222> --tunnel-addr <host:7000>; then on the leader: cluster join approve <bundle> --wait"
	}
	// C3-M5: topology-specific banner + next-step (the TOPO column shows which broker). A STUCK
	// reconcile wins over a merely-behind one (it needs operator action, not just waiting).
	if topoStuck {
		return healthDegraded,
			"a broker's NATS topology reconcile is STUCK (see the TOPO column) — its nats.conf cannot be rendered/validated",
			"fix that broker's nats.conf, or run `tether cluster reconcile nats --manual` on it"
	}
	if topoBehind {
		return healthDegraded,
			"a broker's NATS topology has not caught the desired generation yet (see the TOPO column)",
			"tether cluster reconcile nats --all --wait"
	}
	if degraded {
		return healthDegraded, "a node is mid-join / draining or roster/raft INCONSISTENT — see the table", "cluster status"
	}
	return healthHealthyHA, "", ""
}

// certExpiryAdvisory (B5 OPS#7) returns an advisory string when any node's tunnel-cert rotation
// window is closing (CertValidSecs in (0, window/8] — derived from the actual rotation window, not
// hardcoded, since a future caller may rotate with a different window). Advisory ONLY — it never
// changes health/ExitCode. Returns "" when nothing is near expiry.
func certExpiryAdvisory(nodes []adminsock.ClusterNodeStatus, window time.Duration) string {
	thresh := int64((window / 8).Seconds())
	if thresh <= 0 {
		thresh = 1
	}
	var near []string
	for _, n := range nodes {
		if n.CertValidSecs > 0 && n.CertValidSecs <= thresh {
			near = append(near, fmt.Sprintf("%s(%dm)", n.NodeID, n.CertValidSecs/60))
		}
	}
	if len(near) == 0 {
		return ""
	}
	return "ADVISORY: tunnel-cert rotation window closing for " + strings.Join(near, ", ") +
		" — confirm agents repinned (rotate-tunnel-cert if needed)."
}

// --- adminsock adapter ---

// clusterAdminBackend adapts ClusterAdmin to adminsock.ClusterAdminBackend. The
// injected probes (caughtUp / streamsReady) are the build-and-prove seam for the
// transports that learn a follower's applied_index and stream state — the harness
// wires them; production wiring is D9.
type clusterAdminBackend struct {
	admin        *ClusterAdmin
	caughtUp     func(nodeID string, barrier uint64) (bool, error)
	streamsReady func(nodeID string) (bool, error)
	drainNotice  time.Duration
	// B6 OPS#12 export-incident sources (may be nil in tests that don't exercise it):
	auditTail  func(ctx context.Context, sid string, n int) ([]adminsock.AuditEntry, error)
	activeSIDs func() ([]string, error)
	// C2: the cluster account public key (for minting invites on `cluster seeds publish`). nil/"" when
	// no account seed (single mode / unwired) — the CLI then prints the invite without a pin.
	accountPub func() string
	// C-rebalance: the leader-local proxy-home spread pass (`cluster rebalance proxy`). nil in single
	// mode / tests that don't wire it ⇒ the op replies cluster_not_enabled.
	rebalanceProxy func(dryRun bool) (*adminsock.ProxyRebalanceReport, error)
}

// NewClusterAdminBackend builds the adminsock adapter. caughtUp/streamsReady may be
// nil (status/drain/remove/transfer still work; add's catch-up gate uses a
// leader-applied proxy when caughtUp is nil). auditTail/activeSIDs back export-incident
// (may be nil → export-incident returns history_unavailable).
func NewClusterAdminBackend(admin *ClusterAdmin, caughtUp func(string, uint64) (bool, error), streamsReady func(string) (bool, error),
	auditTail func(context.Context, string, int) ([]adminsock.AuditEntry, error), activeSIDs func() ([]string, error), accountPub func() string) adminsock.ClusterAdminBackend {
	return &clusterAdminBackend{admin: admin, caughtUp: caughtUp, streamsReady: streamsReady, drainNotice: 30 * time.Second,
		auditTail: auditTail, activeSIDs: activeSIDs, accountPub: accountPub}
}

func (b *clusterAdminBackend) HandleCluster(req adminsock.Request) adminsock.Response {
	// Status is leader-agnostic: any broker self-reports.
	if req.Op == adminsock.OpClusterStatus {
		rep, err := b.admin.StatusReport("ctl-nats")
		if err != nil {
			// StatusReport only fails on a DB/roster read → a store error (B2 item 4).
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
		}
		return adminsock.Response{Op: req.Op, OK: true, Cluster: rep}
	}
	// C6 建议5: `--homes` is a leader-agnostic RODB aggregate (any broker serves it) — before the gate.
	if req.Op == adminsock.OpClusterHomes {
		if b.admin.homesReport == nil {
			return adminsock.Response{Op: req.Op, Error: "cluster mode not enabled", Code: adminsock.CodeClusterNotEnabled}
		}
		rep, err := b.admin.homesReport()
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: adminsock.CodeStoreError}
		}
		return adminsock.Response{Op: req.Op, OK: true, Homes: rep}
	}
	// B6 OPS#3: an online backup is a read-only paged copy off the RO handle — ANY node serves
	// it (leader or follower), so it is handled BEFORE the leader-only gate.
	if req.Op == adminsock.OpClusterBackup {
		return b.handleBackup(req)
	}
	// B7 DOC#2: the ops view is a read-only derive off the replicated roster — any node serves it.
	if req.Op == adminsock.OpClusterOps {
		return b.handleClusterOps(req)
	}
	// C2: seeds SHOW is a read-only RODB derive — any node serves it (for `agent join`/doctor).
	if req.Op == adminsock.OpClusterSeedsShow {
		return b.handleSeedsShow(req)
	}
	// Audit MAJOR: export-incident is a read-only RODB assembler needed EXACTLY during a leaderless
	// QUORUM_LOST incident (the status card recommends it in that state) — handle it BEFORE the
	// leader gate so it is available when there is no leader, like status/backup/ops.
	if req.Op == adminsock.OpExportIncident {
		return b.handleExportIncident(req)
	}
	// Mutating verbs are leader-local (§8.1, NO forwarding): fail fast naming the leader.
	if !b.admin.node.IsLeader() {
		_, leaderID := b.admin.node.LeaderWithID()
		host := b.admin.leaderHost(leaderID)
		msg := "not leader; re-run on the leader host"
		if leaderID == "" {
			msg = "no leader (election in progress); retry"
		}
		return adminsock.Response{Op: req.Op, NotLeader: true, LeaderHost: host, Error: msg, Code: adminsock.CodeNotLeader}
	}

	switch req.Op {
	case adminsock.OpClusterDrain:
		return b.handleDrain(req)
	case adminsock.OpClusterRemove:
		if err := b.admin.RemoveNode(req.NodeID, req.Force); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	case adminsock.OpClusterTransfer:
		if err := b.admin.TransferLeaderTo(req.NodeID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	case adminsock.OpClusterAdd:
		return b.handleAdd(req)
	case adminsock.OpClusterJoinApprove:
		opID, err := b.admin.StartJoinOperation(req.JoinBundle)
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, OpID: opID}
	case adminsock.OpClusterRetire:
		opID, err := b.admin.StartRetireOperation(req.NodeID, req.Confirmed)
		var qc *ErrQuorumConfirmRequired
		if errors.As(err, &qc) {
			return adminsock.Response{Op: req.Op, QuorumProj: &adminsock.QuorumProjection{
				Voters: qc.Proj.Voters, Quorum: qc.Proj.Quorum, FaultTolerance: qc.Proj.FaultTolerance,
			}, Error: qc.Error(), Code: adminsock.CodeQuorumConfirmRequired}
		}
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, OpID: opID}
	case adminsock.OpClusterOpConfirm:
		if err := b.admin.ConfirmOp(req.OpID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, OpID: req.OpID}
	case adminsock.OpClusterOpAbort:
		if err := b.admin.AbortOp(req.OpID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, OpID: req.OpID}
	case adminsock.OpClusterRotateCrt:
		if err := b.admin.RotateTunnelCert(req.NodeID, req.CertFP, certRotationWindow); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	case adminsock.OpClusterAlertRaise:
		return b.handleAlertRaise(req)
	case adminsock.OpClusterAlertClear:
		return b.handleAlertClear(req)
	case adminsock.OpClusterSeedsPublish:
		return b.handleSeedsPublish(req)
	case adminsock.OpClusterRebalanceProxy:
		if b.rebalanceProxy == nil {
			return adminsock.Response{Op: req.Op, Error: "proxy rebalance not available (cluster mode not enabled)", Code: adminsock.CodeClusterNotEnabled}
		}
		rep, err := b.rebalanceProxy(req.DryRun)
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
		}
		return adminsock.Response{Op: req.Op, OK: true, ProxyRebalance: rep}
	default:
		return adminsock.Response{Op: req.Op, Error: "unknown cluster op: " + req.Op, Code: adminsock.CodeBadRequest}
	}
}

// TransferLeaderTo hands raft leadership to the SPECIFIC named voter (external
// review F1: the `cluster transfer-leader <node-id>` verb must target the named
// node, not let raft pick any peer). Resolves the node's raft address from the live
// configuration, verifies it is a voter and not self, then uses the TARGETED
// node.LeadershipTransferToServer.
func (a *ClusterAdmin) TransferLeaderTo(nodeID string) error {
	if nodeID == a.node.SelfID() {
		return fmt.Errorf("transfer-leader: %s is this node (already the leader)", nodeID)
	}
	cfg, err := a.node.RaftConfiguration()
	if err != nil {
		return fmt.Errorf("transfer-leader %s: raft configuration: %w", nodeID, err)
	}
	for _, s := range cfg {
		if s.NodeID == nodeID {
			if !s.Voter {
				return fmt.Errorf("transfer-leader: %s is not a voter (cannot be leader)", nodeID)
			}
			return a.node.LeadershipTransferToServer(nodeID, s.Addr)
		}
	}
	return fmt.Errorf("transfer-leader: %s is not in the raft configuration", nodeID)
}

func (b *clusterAdminBackend) handleDrain(req adminsock.Request) adminsock.Response {
	if req.Abort {
		if err := b.admin.AbortDrain(req.NodeID); err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error()}
		}
		return adminsock.Response{Op: req.Op, OK: true}
	}
	deadline := b.admin.now().Add(b.drainNotice)
	if req.Now {
		deadline = b.admin.now()
	}
	var ready func() (bool, error)
	if req.Retire && b.streamsReady != nil {
		ready = func() (bool, error) { return b.streamsReady(req.NodeID) }
	}
	err := b.admin.DrainNode(req.NodeID, req.Retire, req.Confirmed, deadline, ready)
	var qc *ErrQuorumConfirmRequired
	if errors.As(err, &qc) {
		return adminsock.Response{Op: req.Op, QuorumProj: &adminsock.QuorumProjection{
			Voters: qc.Proj.Voters, Quorum: qc.Proj.Quorum, FaultTolerance: qc.Proj.FaultTolerance,
		}, Error: qc.Error(), Code: adminsock.CodeQuorumConfirmRequired}
	}
	if err != nil {
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
	}
	return adminsock.Response{Op: req.Op, OK: true}
}

func (b *clusterAdminBackend) handleAdd(req adminsock.Request) adminsock.Response {
	// Step 1: no token yet — issue a fresh single-use nonce for the operator to sign
	// on the joiner (`cluster sign-join <nonce>`), then re-run with --join-token.
	if req.JoinToken == "" {
		nonce, err := b.admin.IssueJoinNonce()
		if err != nil {
			return adminsock.Response{Op: req.Op, Error: err.Error()}
		}
		return adminsock.Response{Op: req.Op, Nonce: nonce,
			Error: "sign-join required: run `tether cluster sign-join " + nonce + "` on the joining node, then re-run `cluster add` with --join-token <nonce>:<sig>"}
	}
	// Step 2: token = "<nonce>:<sigHex>". Consume the nonce (single-use) then admit.
	nonce, sigHex, ok := splitJoinToken(req.JoinToken)
	if !ok {
		return adminsock.Response{Op: req.Op, Error: "malformed --join-token (want <nonce>:<sigHex>)", Code: adminsock.CodeBadRequest}
	}
	// Audit SEC-MAJOR-1: validate the node_id charset fail-closed BEFORE it is persisted into the
	// roster / rendered into operator command lines (a shell-metachar/newline/path-separator id has
	// no legitimate use and would be echoed verbatim by status/apply/guided output).
	if err := proto.ValidateClusterNodeID(req.NodeID); err != nil {
		return adminsock.Response{Op: req.Op, Error: "invalid node_id: " + err.Error(), Code: adminsock.CodeBadRequest}
	}
	// B6 A3 version-skew gate — BEFORE claimJoinNonce so a rejected joiner does not burn the
	// single-use nonce.
	if resp, reject := b.versionSkewResponse(req); reject {
		return resp
	}
	// audit membership F2: ATOMICALLY claim the nonce (check + mark in-flight under one lock) so
	// two concurrent `cluster add` step-2 calls with the same token cannot both proceed. Released
	// below on AddNode failure so a retry can re-claim with the SAME token (review M9 property).
	if !b.admin.claimJoinNonce(nonce) {
		return adminsock.Response{Op: req.Op, Error: "unknown, already-used, or in-flight nonce; re-run `cluster add` (no token) for a fresh one", Code: adminsock.CodeNonceUsed}
	}
	// D9 round-1 BLOCKER: thread the joiner's full expose-home identity (else an added voter
	// has empty nats_server_id/tunnel_addr/cert_fp → resolveHomeForAgent can never home an
	// agent there → D6 failover onto grown nodes is impossible). nats_server_id == node_id
	// (the §6.5 SSOT). public_host defaults to the raft host if the operator omitted it.
	publicHost := req.PublicHost
	if publicHost == "" {
		publicHost = req.Host
	}
	in := cluster.ClusterNodeUpsertInput{
		NodeID: req.NodeID, Name: req.NodeID, NodeIdentPub: req.NodePub,
		NatsServerID: req.NodeID, RaftAddr: req.Host, NatsRoute: req.NatsRoute,
		TunnelAddr: req.TunnelAddr, PublicHost: publicHost,
		CertFP: req.CertFP, JoinNonce: nonce, JoinSigHex: sigHex, Now: b.admin.now(),
	}
	caughtUp := func(barrier uint64) (bool, error) {
		if b.caughtUp == nil {
			// No catch-up transport wired (review m2): REFUSE rather than silently pass
			// on the leader's own cursor — the production transport that learns a
			// follower's applied_index is D9; the harness injects a real probe.
			return false, fmt.Errorf("catch-up transport not wired (D9); harness must inject caughtUp")
		}
		return b.caughtUp(in.NodeID, barrier)
	}
	if err := b.admin.AddNode(in, req.Host, caughtUp, 0); err != nil {
		b.admin.releaseJoinNonce(nonce) // release on failure so the operator can retry the same token
		return adminsock.Response{Op: req.Op, Error: err.Error(), Code: clusterCodeFor(err)}
	}
	// success: the nonce stays claimed (single-use) — no separate consume needed.
	return adminsock.Response{Op: req.Op, OK: true}
}

// versionSkewResponse is the B6 A3 gate (extracted so the ALLOW paths are unit-testable without a
// live raft node). Proto mismatch is the ONLY hard reject (a different proto cannot speak the
// wire). Release skew is advisory: a rolling upgrade runs followers-first, so the leader is
// transiently OLDER than the joiner, and a re-joining drained node may be older than the
// now-upgraded leader during rollback — rejecting on release would brick exactly the rolling
// upgrade this gate exists to enable. A joiner that did not declare its proto (0, an older
// `cluster add`) is allowed with a warning — the join-PoP + the real connection remain the
// authoritative protections; this is a friendly early check, not the only gate.
func (b *clusterAdminBackend) versionSkewResponse(req adminsock.Request) (adminsock.Response, bool) {
	if req.JoinerProto != 0 && req.JoinerProto != proto.ProtoVersion {
		return adminsock.Response{Op: req.Op, Code: adminsock.CodeVersionSkew,
			Error: fmt.Sprintf("version skew: joiner speaks proto v%d but this cluster is proto v%d — reinstall the joiner on a matching release before adding it",
				req.JoinerProto, proto.ProtoVersion)}, true
	}
	if req.JoinerProto == 0 {
		b.admin.logger.Warn("cluster add: joiner did not declare its proto version (older `cluster add`?); cannot pre-verify compatibility", "node_id", req.NodeID)
	}
	if req.JoinerRelease != "" && req.JoinerRelease != proto.ReleaseVersion {
		// Mega-audit MAJ-11 (tracked enforcement gap): release skew is advisory ONLY, but a column-adding
		// migration (e.g. 0017 last_rehome_at) makes a same-proto OLDER-release member FSM-fail-stop on the
		// first committed op that bakes the new column once leadership moves to a newer-release node. The
		// reinstall-not-upgrade invariant (CLAUDE.md §5) governs this: a joiner MUST be on a
		// migration-compatible release. An IN-BAND schema/migration-version floor in the join handshake
		// (reject below the cluster's schema version, not just proto) is a tracked v2.x follow-up; until
		// then this Warn + the runbook are the enforcement.
		b.admin.logger.Warn("cluster add: joiner release differs from this node (allowed — rolling upgrades mix releases; the joiner MUST be migration-compatible, see runbook §schema)",
			"node_id", req.NodeID, "joiner_release", req.JoinerRelease, "this_release", proto.ReleaseVersion)
	}
	return adminsock.Response{}, false
}

// splitJoinToken parses "<nonce>:<sigHex>" (the `cluster sign-join` output).
func splitJoinToken(tok string) (nonce, sigHex string, ok bool) {
	for i := 0; i < len(tok); i++ {
		if tok[i] == ':' {
			return tok[:i], tok[i+1:], i > 0 && i+1 < len(tok)
		}
	}
	return "", "", false
}

// leaderHost resolves a leader node_id to its roster public_host (for the non-leader
// fail-fast hint). Empty when unknown.
func (a *ClusterAdmin) leaderHost(leaderID string) string {
	if leaderID == "" {
		return ""
	}
	var host string
	_ = a.node.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT public_host FROM cluster_nodes WHERE node_id=?`, leaderID).Scan(&host)
	})
	return host
}
