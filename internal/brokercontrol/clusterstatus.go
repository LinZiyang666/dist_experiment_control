package brokercontrol

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/proto"
)

// clusterCodeFor derives a stable machine Code (B2 item 4) from a broker cluster error by
// recognizing the broker's OWN error messages. These substrings are pinned by the D7 suite, so a
// drift breaks a test rather than silently mis-coding. An unrecognized error yields "" → the CLI
// classifies it exitInternal (70). This is server-side self-recognition (the broker owns both the
// string and the test); the CLI never sniffs prose — it maps code→class via a table. Free-form
// errors that match no pattern stay "" (a documented B2.1 widening target).
func ClusterCodeFor(err error) string {
	if err == nil {
		return ""
	}
	// R8a P1: classify by TYPE, not by prose. The data-plane gate is the one signal
	// an operator script must be able to distinguish from a real failure ("re-run,
	// it is still converging" vs "this drain is broken"), so it must not depend on
	// a reworded message the way the string cases below do.
	var dp *ErrDataPlaneNotConverged
	if errors.As(err, &dp) {
		return CodeDataplaneNotConverged
	}
	// B4: same rule, second application. A version-skewed joiner is a TERMINAL refusal —
	// falling through to `default: ""` would classify it exit 70, which docs/usage.md §9.13
	// tells automation to retry, so a mis-versioned node would be re-approved forever.
	var vs *ErrJoinVersionSkew
	if errors.As(err, &vs) {
		return adminsock.CodeVersionSkew
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

// Health strings + exit codes (§17 contract).
const (
	HealthHealthyHA   = "HEALTHY_HA"
	HealthDegraded    = "DEGRADED"
	HealthQuorumLost  = "QUORUM_LOST"
	HealthForceSingle = "FORCE_SINGLE"

	// statusSchemaVersion is the --json schema discriminator (§17 / external review
	// F4); bump on any breaking shape change so monitors can negotiate.
	//
	// v1 -> v2 (batch B2, external review B2-2): `stream_actual` gained the sentinel
	// StreamActualUnobserved (-1), meaning "the replica observation did not complete", where v1's
	// domain was non-negative measured counts only. That is a CHANGE OF MEANING for an existing key, and
	// docs/usage.md's bump policy names exactly that as requiring a bump ("仅破坏性改动（删/改键、改类型、
	// 改语义）才 bump").
	//
	// It shipped at v1 first, which was the defect: a v1 collector could read -1 as malformed, as a real
	// count, or as a catastrophic deficit, with no discriminator to tell it the field had acquired a new
	// value class. The alternative — keeping v1's domain and encoding "unobserved" additively — was
	// considered and rejected: every honest additive encoding also breaks v1, because there is no
	// non-negative integer that truthfully means "not measured" (0 is the FABRICATED value this batch
	// removed, and `*int` is a type change, which the same policy also calls breaking). So the value
	// domain has to widen, and widening it is a v2.
	//
	// v2 contract for `nodes[].stream_actual`:
	//
	//	>= 0   the observed replica count for this node's per-session streams
	//	  -1   StreamActualUnobserved: observation failed or timed out. It is NOT a count, and it is
	//	       deliberately < any stream_target, so every at-target comparison stays fail-closed (§S1).
	//	       The human renderer prints "?/target" for it.
	//
	// The NUMBER lives in adminsock next to the struct it stamps, because the offline view in
	// cmd/tether/cluster.go emits the same struct and a second literal would drift.
	StatusSchemaVersion = adminsock.ClusterStatusSchemaVersion

	// certRotationWindow is how long the previous tunnel cert pin stays valid after a
	// rotate-tunnel-cert (the cutover window agents reconnect within).
	CertRotationWindow = 24 * time.Hour
)

// healthExitCode is the single source of truth mapping health → process exit code
// (0=HEALTHY-HA, 1=DEGRADED-writable, 2=read-only/quorum-lost, 3=force-single).
func HealthExitCode(health string) int {
	switch health {
	case HealthHealthyHA:
		return 0
	case HealthDegraded:
		return 1
	case HealthQuorumLost:
		return 2
	case HealthForceSingle:
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
func HealthLabel(health string, voters int) string {
	switch health {
	case HealthHealthyHA:
		return "HEALTHY-HA"
	case HealthDegraded:
		if voters <= 2 {
			return "NOT-HA"
		}
		return "DEGRADED-WRITABLE"
	case HealthQuorumLost:
		return "READ-ONLY"
	case HealthForceSingle:
		return "FORCE-SINGLE"
	default:
		return ""
	}
}

type RosterRow struct {
	NodeID, Name, Phase string
	CertFP              string         // B5 OPS#7: current tunnel-cert fingerprint (public; '' if unset)
	CertFPPrev          sql.NullString // previous fp during a rotation window
	CertValid           sql.NullTime   // cert_fp_valid_until
}

// StatusReport builds this broker's self-report (view = "ctl-nats" or "offline").
// It joins the roster (cluster_nodes) against the live raft configuration for the
// role + the no-silent-fork INCONSISTENT cross-render, derives the leader, applied
// lag (self only; a leader cannot read a follower's cursor — that is the D9
// transport), the force-single marker, and the stream actual/target placeholder.
func (a *ClusterAdmin) StatusReport(view string) (*adminsock.ClusterStatusReport, error) {
	// Materialize roster + force-single marker FIRST (close rows) — no nested query
	// under the single-conn pool (the D6 deadlock lesson).
	var roster []RosterRow
	var forceSingle bool
	var drainMarked, activeOpTarget map[string]bool
	var portsUsed int
	var portsKnown bool
	var selfNatsRoute string
	selfID := a.Node.SelfID()
	if err := a.Node.BoundedStaleRead(func(db *sql.DB) error {
		rows, err := db.Query(`SELECT node_id, name, phase, cert_fp, cert_fp_prev, cert_fp_valid_until FROM cluster_nodes ORDER BY node_id`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r RosterRow
			if err := rows.Scan(&r.NodeID, &r.Name, &r.Phase, &r.CertFP, &r.CertFPPrev, &r.CertValid); err != nil {
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
		// batch C: the drain markers and the non-terminal op targets, read in the SAME snapshot as the
		// roster so the DRAINING-without-marker verdict below cannot be manufactured by two reads
		// landing either side of a concurrent write. Both queries run AFTER the roster rows are closed
		// — nesting a query inside an open *sql.Rows deadlocks the single-conn pool (the D6 lesson).
		marked, derr := cluster.DrainingNodes(db)
		if derr != nil {
			return derr
		}
		drainMarked = make(map[string]bool, len(marked))
		for _, id := range marked {
			drainMarked[id] = true
		}
		opRows, oerr := db.Query(`SELECT target_node FROM cluster_operations WHERE terminal = 0`)
		if oerr != nil {
			return oerr
		}
		defer func() { _ = opRows.Close() }()
		activeOpTarget = map[string]bool{}
		for opRows.Next() {
			var target string
			if serr := opRows.Scan(&target); serr != nil {
				return serr
			}
			activeOpTarget[target] = true
		}
		if oerr := opRows.Err(); oerr != nil {
			return oerr
		}
		// B5 OPS#9: self-row ports_used = exposes THIS broker homes (the countOwnedExposes
		// pattern — home_broker-scoped, since port_allocations is the replicated cluster table).
		// Single COUNT after the roster rows are closed (single-conn safe).
		if a.PortBandHigh > 0 && a.PortBandHigh >= a.PortBandLow {
			if e := db.QueryRow(`SELECT COUNT(*) FROM port_allocations WHERE home_broker=? AND state='ALLOCATED'`, selfID).Scan(&portsUsed); e != nil {
				return e
			}
			portsKnown = true
		}
		// v0.4.2 grow-readiness: the self row's nats_route (the :6222 route-mesh advertise) for the
		// loopback gate. Single read after the roster rows are closed (single-conn safe).
		if e := db.QueryRow(`SELECT nats_route FROM cluster_nodes WHERE node_id=?`, selfID).Scan(&selfNatsRoute); e != nil && !errors.Is(e, sql.ErrNoRows) {
			return e
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("cluster status: read roster: %w", err)
	}

	// B5 OPS#9: self-row disk free % via statfs (NO DB; never on the offline path — a stale/
	// unmounted store dir can hang). Gate on StoreDir set + a non-zero total (honest absence).
	diskFreePct, diskKnown := 0, false
	if a.StoreDir != "" {
		if used, total, derr := DiskUsage(a.StoreDir); derr == nil && total > 0 {
			diskFreePct = int((float64(total-used) / float64(total)) * 100.0)
			diskKnown = true
		}
	}
	portsTotal := 0
	if portsKnown {
		portsTotal = a.PortBandHigh - a.PortBandLow + 1
	}

	cfg, err := a.Node.RaftConfiguration()
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
	selfApplied, _ := a.Node.AppliedIndex()
	selfLag := uint64(0)
	topoDesired, _ := cluster.TopologyGeneration(a.Node.RODB()) // C3: cluster-wide desired topology gen
	streamTarget := jsstream.ReplicasFor(voters)
	// External-review F1: report the REAL stream actual, not a synthesized actual==target. An
	// unwired probe (N=1) keeps the optimistic default.
	//
	// B7 / internal review B7-02 — UNOBSERVED IS NOT THE SAME AS MEASURED ZERO.
	// A wired-but-failed observation used to report 0, which every consumer then read as a measured
	// deficit: `cluster status` renders "0/3", computeHealth marks the node DEGRADED, and doctor says
	// "stream replicas below target". That conflation was tolerable while the observation only failed on
	// a genuinely broken JS meta. It stopped being tolerable when B7 put a 3s deadline on the call:
	// ObserveReplicas walks the events stream, EVERY session's history stream and EVERY live OBJ_xfer
	// stream serially, so on a busy broker a timeout is now an ordinary outcome, and an ordinary outcome
	// must not render as a fault.
	//
	// -1 means UNOBSERVED. testing-standards §S1 asks for fail-closed on missing data, and it still is:
	// StreamsAllAtTarget below treats -1 as NOT at target, so nothing that gates on replica health opens
	// up. What changes is that the operator is told "not measured" instead of a number that is false.
	streamActual := streamTarget
	if a.StreamObserve != nil {
		if rep, err := a.StreamObserve(); err == nil && rep.Observed {
			streamActual = MinStreamActual(rep, streamTarget)
		} else {
			streamActual = StreamActualUnobserved
		}
	}

	rep := &adminsock.ClusterStatusReport{SchemaVersion: StatusSchemaVersion, View: view, LeaderID: leaderID}
	// §17 row 3: scatter-gather the broker-only cursor probe ONCE (if wired) so each peer's
	// reachability + applied-lag is REAL, not a blanket self-report. self is always reachable
	// (it is answering this request). nil poll ⇒ honest "unverified" for peers.
	var health map[string]proto.ClusterHealthResp
	if a.HealthPoll != nil {
		health = a.HealthPoll()
	}
	// batch B / B4: the ACCT.NK comparison baseline — the account key THIS broker signs auth_callout
	// with. Empty when the getter is unwired or there is no account seed; every row then stays
	// unreported (see ClusterAdmin.accountPubSelf).
	selfAccountPub := ""
	if a.AccountPubSelf != nil {
		selfAccountPub = a.AccountPubSelf()
	}
	reachOf := func(nodeID string) (reachable bool, source string, lag uint64) {
		if nodeID == a.Node.SelfID() {
			return true, "self", selfLag
		}
		if a.HealthPoll == nil {
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
		rosterIDs[r.NodeID] = true
		ro, inCfg := role[r.NodeID]
		// INCONSISTENT cross-render: roster says VOTER but raft config disagrees (or
		// the row is absent from the config), or vice-versa.
		rosterRaftSkew := (r.Phase == PhaseVoter && (!inCfg || ro == "learner")) ||
			(!inCfg && r.Phase != PhaseRetiring && r.Phase != PhaseAddFailed)
		drainSkew :=
			// batch C: a node stuck in DRAINING with NO drain marker and no operation driving it. Every
			// producer writes the marker before the phase and clears it after restoring the phase, so
			// this combination is not a legal transient — it means a drain died between its own steps
			// and nothing will resume it. Deliberately NOT the roadmap's literal "DRAINING with no
			// active op", which is the normal, indefinitely-stable end state of a SUCCESSFUL
			// `cluster drain` (that path never creates an op row at all) and would have made
			// `cluster doctor` exit 64 on every correctly drained node.
			DrainingWithoutMarker(r.Phase, drainMarked[r.NodeID], activeOpTarget[r.NodeID])
		inconsistent := rosterRaftSkew || drainSkew
		// EXTERNAL review m1: name WHICH inconsistency, so consumers stop printing the roster/raft cause
		// (and the roster/raft remedy) for a drain that died between its own steps. roster/raft wins when
		// both hold: it is the one that can fork membership.
		inconsistentReason := ""
		switch {
		case rosterRaftSkew:
			inconsistentReason = adminsock.InconsistentRosterRaft
		case drainSkew:
			inconsistentReason = adminsock.InconsistentDrainingWithoutMarker
		}
		reachable, source, lag := reachOf(r.NodeID)
		// batch B / B4: AccountNkMatch/AccountNkReported are deliberately NOT set here. This literal
		// used to carry `AccountNkMatch: true` unconditionally, which is where the "column is always
		// Y" lie came from. The answer now comes from the health self-report below, and a row with
		// no self-report stays unreported — the renderer prints "?".
		ns := adminsock.ClusterNodeStatus{
			NodeID: r.NodeID, Name: r.Name, Phase: r.Phase, Role: ro,
			AppliedLag:   lag,
			StreamActual: streamActual, StreamTarget: streamTarget,
			Reachable: reachable, ReachSource: source,
			Inconsistent: inconsistent, InconsistentReason: inconsistentReason,
		}
		// B6 OPS#4: stamp each broker's live self-reported version (from the health poll).
		// C3: stamp the topology reconcile self-report (applied/observed/reason/reported).
		if hr, ok := health[r.NodeID]; ok {
			// batch B / B4: the honest ACCT.NK answer. selfAccountPub is the VIEWING broker's key;
			// a node whose reported key differs signs auth_callout JWTs no nats-server pinned to
			// this key will accept, which surfaces as agents failing to join at random.
			//
			// The comparison is only made when BOTH sides have a key. If this broker cannot name its
			// own (no account seed configured), it is not entitled to call anyone else mismatched —
			// the row stays unreported rather than rendering a fabricated N for the whole cluster.
			if hr.AccountNkReported && selfAccountPub != "" {
				ns.AccountNkReported = true
				ns.AccountNkMatch = hr.AccountNkPub == selfAccountPub
			}
			ns.ReleaseVersion = hr.ReleaseVersion
			ns.ProtoVer = hr.ProtoVer
			ns.TopoApplied = hr.TopoApplied
			ns.TopoObserved = hr.TopoObserved
			ns.TopoReconcileReason = hr.TopoReconcileReason
			ns.TopoAction = hr.TopoAction
			ns.TopoReported = hr.TopoReported
		}
		// B5 OPS#7: cert fingerprints (public) for every row; CertValidSecs derived now→valid.
		ns.CertFP = r.CertFP
		if r.CertFPPrev.Valid {
			ns.CertFPPrev = r.CertFPPrev.String
		}
		if r.CertValid.Valid {
			if secs := int64(r.CertValid.Time.Sub(a.Now()).Seconds()); secs > 0 {
				ns.CertValidSecs = secs
			}
		}
		// B5 OPS#9: capacity is self-row only (a leader cannot statfs a peer's disk).
		if r.NodeID == selfID {
			if diskKnown {
				ns.DiskFreePct = diskFreePct
			}
			if portsKnown {
				ns.PortsUsed = portsUsed
				ns.PortsTotal = portsTotal
			}
			// C3: the self row's topology report is authoritative from the local reconciler (self does
			// not poll itself), overriding any health-map echo.
			if a.TopoSelf != nil {
				ns.TopoReported = true
				if ts := a.TopoSelf(); ts != nil {
					ns.TopoApplied = ts.Applied
					ns.TopoObserved = ts.Observed
					ns.TopoReconcileReason = ts.Reason
					// batch C: the SELF row must carry the action too. Omitting it here (and only
					// here) would leave the leader's own TOPO cell permanently on the legacy
					// reason-substring fallback — and a wedged reconcile is most often on the very
					// broker you are asking, so the one row that would silently keep the old
					// behaviour is the one that matters most.
					ns.TopoAction = ts.Action
				}
			}
			// batch B / B4: same authoritative-self rule for ACCT.NK. The self row's verdict is a
			// tautology (this broker's key equals its own), and stating it locally rather than via the
			// health echo is what makes it robust: at N=1 there is no healthPoll at all, and a self row
			// rendered "?" on a single-broker cluster would read as "verification unavailable" on the
			// only broker that can never be mismatched. Still gated on knowing the key — an unwired
			// account seed reports nothing, in either mode.
			if selfAccountPub != "" {
				ns.AccountNkReported = true
				ns.AccountNkMatch = true
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
				// An orphan raft voter with no roster row at all — the roster/raft class by construction.
				ReachSource: source, Inconsistent: true,
				InconsistentReason: adminsock.InconsistentRosterRaft,
			})
		}
	}

	rep.TopoDesired = topoDesired // C3: cluster-wide desired topology generation
	rep.Health, rep.Banner, rep.NextStep = ComputeHealth(forceSingle, leaderID, voters, topoDesired, rep.Nodes)
	// G2 #20: DATA-PLANE-DEGRADED signal. force-single restores the CONTROL plane, but if the survivor's
	// on-disk nats.conf still carries a cluster{} block the embedded JetStream is wedged at 503 (no
	// quorum-of-2) — SILENTLY, because the alert path itself rides JetStream (this is what let racknerd rot
	// for days). Surface it OUT-OF-BAND on the status banner, keyed on the LIVE conf. Best-effort: a
	// missing/unreadable conf just skips the extra banner (never fails the report).
	jsClusteredForceSingle := false
	if forceSingle && a.NatsConfPath != "" {
		// BOTH facts (external review RB2-1). The banner this raises asserts "JetStream is UNAVAILABLE —
		// nats.conf is still clustered after force-single … transfers/history/audit return 503", and that
		// causal claim needs JetStream to be ENABLED. On a conf with `jetstream: false` the 503s are not
		// happening and the remedy offered would be answering a question nobody asked; the honest report
		// there is the plain force-single banner without the data-plane-degraded clause.
		if own, perr := natsconf.Preflight(a.NatsConfPath); perr == nil &&
			own.IsClusteredTopology() && own.HasJetStream() {
			jsClusteredForceSingle = true
		}
	}
	// External-review m2 / round2 M1: surface the SAME runtime sustained-JS-503 signal `cluster status
	// --remote` shows, not only the force-single+clustered-conf inference. sustained-503 is LEADER-observed,
	// so an operator SSHed to a FOLLOWER (whose local jsUnavail is false) must still see it — OR-aggregate
	// `JetStreamUnavailable` across the health-poll map (the same peer replies `--remote` folds), not only
	// this broker's local flag. Emit ONE banner (dedup): force-single keeps its specific remedy; any other
	// sustained-503 gets the generic degraded banner.
	anyPeerJSUnavail := false
	for _, h := range health {
		if h.JetStreamUnavailable {
			anyPeerJSUnavail = true
			break
		}
	}
	if jsClusteredForceSingle || (a.JsUnavail != nil && a.JsUnavail()) || anyPeerJSUnavail {
		if rep.Banner != "" {
			rep.Banner += " "
		}
		if jsClusteredForceSingle {
			// R10 P4: the remedy literal is the shared natsconf SSOT (also emitted by the N=1 boot
			// FATAL and by `cluster recovery restore`'s completion text — three copies used to drift).
			rep.Banner += "DATA-PLANE DEGRADED: JetStream is UNAVAILABLE — nats.conf is still clustered after force-single (file transfers / history / audit return 503). De-cluster it: `" +
				natsconf.DeClusterRemedyCmd + "` " + natsconf.DeClusterRemedyArgHint + ", then restart nats-server. " + natsconf.DeClusterRemedyResetJSNote
		} else {
			rep.Banner += "DATA-PLANE DEGRADED: JetStream is UNAVAILABLE (sustained 503) — file transfers / history / audit are failing. Check `tether cluster status`, nats-server, and JetStream meta quorum."
		}
	}
	rep.ExitCode = HealthExitCode(rep.Health)
	rep.HealthLabel = HealthLabel(rep.Health, voters) // C6 建议6: additive 5-state label; legacy Health/ExitCode unchanged
	// B5 OPS#7: a tunnel-cert whose rotation window is closing is an ADVISORY only — a rotation
	// in-window is healthy and even past valid_until only the PREVIOUS pin lapsed (the cert still
	// serves), so it NEVER changes health/ExitCode. Appended to the banner; no-op when no cert is
	// near expiry (so the common case is byte-identical).
	if adv := CertExpiryAdvisory(rep.Nodes, CertRotationWindow); adv != "" {
		if rep.Banner != "" {
			rep.Banner += " "
		}
		rep.Banner += adv
	}
	// batch B / B4: an account-key divergence is a REAL finding, and a column an operator only sees
	// if they look at that column is a weak place to put it — the symptom is "agents fail to join at
	// random", which nobody diagnoses by scanning ACCT.NK. Surface it on the banner too.
	//
	// ADVISORY ONLY, deliberately: it does NOT touch Health or ExitCode. Those are a monitoring
	// contract (`cluster status --json || alert` branches on them), and widening what makes them
	// non-zero is a deploy-surface change that belongs in its own increment with its own runbook
	// note. Same asymmetry certExpiryAdvisory already established above.
	if adv := AccountKeySkewAdvisory(rep.Nodes); adv != "" {
		if rep.Banner != "" {
			rep.Banner += " "
		}
		rep.Banner += adv
	}
	// B1: stamp the view source (which broker self-reported, and whether it is the
	// authoritative leader view) and the plain-language voter-count verdict.
	rep.ViewHost = a.Node.SelfID()
	rep.IsLeaderView = leaderID == a.Node.SelfID()
	// batch B / B4: report the conf this broker actually uses, so the ONLINE `cluster doctor` can run
	// the auth_callout issuer-skew cross-check against the RIGHT file (TODO(n3-online-doctor)).
	rep.NatsConfPath = a.NatsConfPath
	rep.Verdict = ClusterVerdict(voters, StreamsAtTarget(rep.Nodes), rep.Health)
	// v0.4.2 phase-fluidity grow-readiness: surface a loopback self-advertise (raft and/or NATS
	// route) so the doctor/status can warn that a cross-network grow needs `cluster set-raft-addr`
	// first. Computed from the committed raft Configuration (self addr) + the self roster nats_route.
	for _, s := range cfg {
		if s.NodeID == selfID {
			rep.SelfRaftAdvertise = s.Addr
		}
	}
	rep.SelfRaftAdvertiseLoopback = IsLoopbackAdvertiseHostPort(rep.SelfRaftAdvertise)
	rep.SelfNatsRouteLoopback = IsLoopbackNatsRoute(selfNatsRoute)
	return rep, nil
}

// StreamActualUnobserved is the ClusterNodeStatus.StreamActual value meaning "the replica observation
// did not complete" — as distinct from a measured zero (B7 / internal review B7-02).
//
// It is negative on purpose: every existing consumer compares `StreamActual < StreamTarget`, so -1 keeps
// the fail-closed behaviour that 0 had (nothing gated on replica health opens up) WITHOUT claiming a
// measurement that was never taken. The CLI renders it as "?" rather than a number.
const StreamActualUnobserved = -1

// streamsAtTarget reports whether every node row with a KNOWN target is at or above it.
// StreamTarget==0 means "not observed" and never counts as a deficit (matches computeHealth F1).
// StreamActual == StreamActualUnobserved (-1) DOES count as not-at-target — fail-closed, §S1.
func StreamsAtTarget(nodes []adminsock.ClusterNodeStatus) bool {
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
func ClusterVerdict(voters int, streamsOK bool, health string) string {
	switch {
	case voters == 0:
		return ""
	case voters <= 1:
		return "1 broker — NO redundancy: if it dies, the cluster stops until restored."
	case voters == 2:
		return "2 brokers — NO fault-tolerant writes: losing either makes the cluster read-only (quorum needs both)."
	case streamsOK && health == HealthHealthyHA:
		f := ProjectQuorum(voters, false).FaultTolerance
		return fmt.Sprintf("HA active — writes are replicated; the cluster survives %d broker failure(s).", f)
	default:
		return "HA configured but DEGRADED right now — see the table; full redundancy is not in place."
	}
}

// minStreamActual is the conservative cluster-wide stream actual: the smallest observed
// replica count across all streams (so a single under-replicated stream shows the deficit).
// No streams ⇒ the optimistic target (nothing to be under-replicated).
func MinStreamActual(rep ReplicaReport, target int) int {
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
func ComputeHealth(forceSingle bool, leaderID string, voters int, topoDesired uint64, nodes []adminsock.ClusterNodeStatus) (health, banner, nextStep string) {
	if forceSingle {
		return HealthForceSingle,
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
		return HealthQuorumLost,
			"THIS broker sees no leader — it is READ-ONLY (a single-broker view, NOT cluster consensus). Check the OTHER brokers before acting; force-single ONLY after confirming a majority is PERMANENTLY dead.",
			"on each survivor: cluster status --offline --db /var/lib/tether/tether.db ; then only after confirming peers dead: cluster recovery force-single --self-id <this-node-id> --self-addr <this-host:7400> --confirm-peers-dead <ids...>"
	}
	degraded := false
	// batch C: ONE classifier (natsconf.ClassifyTopo), folded across nodes by severity. It replaces
	// the two local booleans AND the three reason substrings they were derived from — see
	// natsconf/topostate.go for the three defects that shape cost.
	var topoWorst natsconf.TopoState
	for _, n := range nodes {
		if n.Inconsistent || n.Phase == PhaseCatchingUp || n.Phase == PhaseAddFailed || n.Phase == PhaseDraining || n.Phase == PhaseRetiring {
			degraded = true
		}
		// External-review F5: a VOTER that a REAL health poll found UNREACHABLE, or that
		// trails the leader's commit beyond the §17 lag threshold, is DEGRADED — never call a
		// cluster HEALTHY_HA when a voter is observably down or behind (the §17 exit-code
		// contract). "nats-health" means it was actually polled (vs "unverified"/"self").
		if n.Phase == PhaseVoter {
			if n.ReachSource == "nats-health" && !n.Reachable {
				degraded = true
			}
			if n.AppliedLag > ObserveLagThreshold {
				degraded = true
			}
			// C3 acceptance ("任一 broker 未完成 topology apply 时不显 HEALTHY-HA"): a REACHED, REPORTING
			// voter whose live NATS topology has not caught the desired generation is DEGRADED. Gated on
			// TopoReported (presence) — NOT a TopoObserved>0 magnitude guard — so a just-promoted voter at
			// observed=0 with topoDesired>0 still degrades. observed<desired subsumes applied<desired
			// (never rendered) AND observed<applied (swapped-not-reloaded), since observed≤applied≤desired.
			//
			// batch C: the observed<desired test moved INSIDE ClassifyTopo, and the STUCK/HELD verdicts
			// deliberately do NOT depend on it. Gating them on generations was a real defect: an
			// unrecognized-directive pass returns applied/observed UNCHANGED, so a broker wedged AFTER
			// converging had observed == desired, skipped the whole branch, and the cluster reported
			// HEALTHY_HA with a permanently fail-closed reconciler on it.
			reached := n.ReachSource == "self" || (n.ReachSource == "nats-health" && n.Reachable)
			if reached {
				st := natsconf.ClassifyTopo(n.TopoAction, n.TopoReconcileReason, n.TopoObserved, topoDesired, n.TopoReported)
				if st.Degrades() {
					degraded = true
				}
				topoWorst = natsconf.WorstTopoState(topoWorst, st)
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
		return HealthDegraded,
			fmt.Sprintf("only tolerates %d failures (%d voters, quorum=%d) — add a node for HA", proj.FaultTolerance, proj.Voters, proj.Quorum),
			"on the NEW broker: cluster join prepare --node-id <id> --raft-addr <host:7400> --nats-route nats://<host:6222> --tunnel-addr <host:7000>; then on the leader: cluster join approve <bundle> --wait"
	}
	// C3-M5 / batch C: topology-specific banner + next-step (the TOPO column shows which broker).
	// WorstTopoState carries the precedence STUCK > HELD > UNKNOWN > BEHIND, so the node needing
	// hands wins over the one that only needs patience. The banner/next-step text now lives with the
	// classifier, which is what keeps this end and the CLI from wording the same verdict differently.
	if banner := topoWorst.Banner(); banner != "" {
		return HealthDegraded, banner, topoWorst.NextStep()
	}
	if degraded {
		return HealthDegraded, "a node is mid-join / draining or roster/raft INCONSISTENT — see the table", "cluster status"
	}
	return HealthHealthyHA, "", ""
}

// accountKeySkewAdvisory (batch B / B4) returns an advisory naming every node that ANSWERED with an
// auth_callout account key different from this view's. Returns "" when there is no divergence — which
// includes every row being unreported, so an old cluster or an offline snapshot adds nothing.
//
// Only reported mismatches count. A "?" row is not evidence of anything, and treating absence of an
// answer as a mismatch is precisely the fabricated-signal defect this whole item removed.
func AccountKeySkewAdvisory(nodes []adminsock.ClusterNodeStatus) string {
	var skewed []string
	for _, n := range nodes {
		if n.AccountNkReported && !n.AccountNkMatch {
			skewed = append(skewed, n.NodeID)
		}
	}
	if len(skewed) == 0 {
		return ""
	}
	return "ADVISORY: auth_callout ACCOUNT KEY DIVERGES on " + strings.Join(skewed, ", ") +
		" — those brokers sign CONNECT approvals with a key the other brokers' nats-servers do not " +
		"trust, so agent/ctl auth fails intermittently depending on which broker answers the callout. " +
		"Re-render nats.conf from the ONE intended account.nk on every broker (`cluster reconcile nats`), " +
		"then restart the odd brokers."
}

// certExpiryAdvisory (B5 OPS#7) returns an advisory string when any node's tunnel-cert rotation
// window is closing (CertValidSecs in (0, window/8] — derived from the actual rotation window, not
// hardcoded, since a future caller may rotate with a different window). Advisory ONLY — it never
// changes health/ExitCode. Returns "" when nothing is near expiry.
func CertExpiryAdvisory(nodes []adminsock.ClusterNodeStatus, window time.Duration) string {
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

// TransferLeaderTo hands raft leadership to the SPECIFIC named voter (external
// review F1: the `cluster transfer-leader <node-id>` verb must target the named
// node, not let raft pick any peer). Resolves the node's raft address from the live
// configuration, verifies it is a voter and not self, then uses the TARGETED
// node.LeadershipTransferToServer.
func (a *ClusterAdmin) TransferLeaderTo(nodeID string) error {
	if nodeID == a.Node.SelfID() {
		return fmt.Errorf("transfer-leader: %s is this node (already the leader)", nodeID)
	}
	cfg, err := a.Node.RaftConfiguration()
	if err != nil {
		return fmt.Errorf("transfer-leader %s: raft configuration: %w", nodeID, err)
	}
	for _, s := range cfg {
		if s.NodeID == nodeID {
			if !s.Voter {
				return fmt.Errorf("transfer-leader: %s is not a voter (cannot be leader)", nodeID)
			}
			return a.Node.LeadershipTransferToServer(nodeID, s.Addr)
		}
	}
	return fmt.Errorf("transfer-leader: %s is not in the raft configuration", nodeID)
}

// ErrJoinVersionSkew is the TYPED refusal for a joiner whose declared proto cannot speak this
// cluster's wire. It exists because the two callers return different envelopes — handleAdd an
// adminsock.Response, StartJoinOperation a plain error — and clusterCodeFor's own doc
// (see :24-35) records the rule: "classify by TYPE, not by prose". Without the type, the join
// path's refusal falls through clusterCodeFor's `default: ""` to exit 70, which
// docs/usage.md §9.13 tells automation to RETRY — manufacturing exactly the defect batch-A's
// A1 existed to remove, on the very gate added to prevent a different one.
type ErrJoinVersionSkew struct {
	JoinerProto  int
	ClusterProto int
	// MissingField is set when the bundle omitted evidence required to prove exact compatibility.
	// Decode remains additive/backward-compatible; admission fails closed until the operator
	// regenerates the bundle with current tooling.
	MissingField string
	// JoinerRelease / ClusterRelease are set instead of the proto pair when the refusal is a
	// RELEASE mismatch (external review B3). One type carries both because both are the same
	// refusal to the operator and to clusterCodeFor: "reinstall the joiner, then retry".
	JoinerRelease  string
	ClusterRelease string
}

// versionSkewRefusal decides whether a joiner declaring (joinerProto, joinerRelease) may join
// this cluster. It returns *ErrJoinVersionSkew on refusal and nil on accept.
//
// Extracted from versionSkewResponse (which returned an adminsock.Response and was therefore
// unusable from StartJoinOperation) so the SAME decision serves both entry points. That
// matters because the entry point it was written for — handleAdd, via adminsock.OpClusterAdd
// — is DELIBERATELY NOT ROUTED (internal/adminsock/protocol.go:150, v0.4.2), so until now the
// gate was product-unreachable while its test called it directly and stayed green.
//
// Proto mismatch is a hard reject: a different proto cannot speak the wire.
//
// RELEASE MISMATCH IS ALSO A HARD REJECT (external review B3; kept as the EXPLICIT EXEMPTION when
// §6.7 became the N-1 window — upgrade-safety S10). It was advisory-only, on the reasoning that
// "rejecting on release would brick exactly the rolling upgrade this gate exists to enable". That
// reasoning was wrong on both halves:
//
//   - It contradicted the WHAT authority. docs/requirements.md §6.7 (as then written) mandated
//     identical major.minor.patch and said a mismatch must be REFUSED with a clear error. §6.7 is
//     now the N-1 window and names THIS GATE as its explicit exemption: a joiner is a machine being
//     ADDED — reinstalling it to match blocks no deployed node's rollback, while the window clause
//     governs online interop, not cluster-membership admission.
//   - It did not protect any real flow. A rolling `cluster upgrade` rolls EXISTING voters and never
//     passes through this gate; the flows that do are `cluster join approve` / `cluster add`, i.e.
//     admitting a NEW or re-installed broker, which is exactly where the requirement applies. The
//     "re-joining drained node may be older during a rollback" case is not a counterexample: per
//     requirements that node must be reinstalled at the cluster's release first, and the code's own
//     comment already recorded why (a column-adding migration makes a same-proto OLDER-release
//     member FSM-fail-stop once leadership moves to a newer-release node — a data-integrity failure
//     the advisory Warn did nothing to prevent).
//
// WHAT THIS GATE IS NOT: the declared fields ride the join bundle UNSIGNED
// (internal/cluster/join_bundle.go — extending JoinSignBytes would invalidate every already-issued
// bundle, including the ones DR rejoin uses, so it is deliberately out of scope). A joiner that
// wanted in could therefore declare a matching release. This gate is an OPERATOR-ERROR guard — it
// stops the mismatched box an operator did not notice — not a boundary against a hostile joiner.
// The authoritative protections remain the join PoP and the operator's explicit approval.
//
// Missing declarations fail closed (external re-review R2). Exact equality cannot be established
// from absent evidence, and docs/testing-standards.md §S1 requires compatibility/safety evidence
// that is missing or unparseable to reject. Decode remains backward-compatible so an operator gets
// this typed, actionable refusal instead of a parse failure; the compliant path is to upgrade the
// joiner and regenerate its bundle with current tooling.
func VersionSkewRefusal(joinerProto int, joinerRelease, nodeID string, log *slog.Logger) error {
	if joinerProto == 0 {
		LogSkew(log, "cluster add: REFUSED — joiner did not declare its proto version; exact compatibility cannot be verified",
			"node_id", nodeID, "this_proto", proto.ProtoVersion)
		return &ErrJoinVersionSkew{
			JoinerProto:  joinerProto,
			ClusterProto: proto.ProtoVersion,
			MissingField: "protocol version",
		}
	}
	// origin: upgrade-safety N-1 window — exact equality (architecture §21.1
	// site #3): same-window peers are epoch-equal by construction; a bumper
	// owes the dual-tree subscription first (§21.4).
	if joinerProto != proto.ProtoVersion {
		return &ErrJoinVersionSkew{JoinerProto: joinerProto, ClusterProto: proto.ProtoVersion}
	}
	if joinerRelease == "" {
		LogSkew(log, "cluster add: REFUSED — joiner did not declare its release; the join gate's exact-release match (requirements §6.7 exemption) cannot be verified",
			"node_id", nodeID, "this_release", proto.ReleaseVersion)
		return &ErrJoinVersionSkew{
			ClusterRelease: proto.ReleaseVersion,
			MissingField:   "release version",
		}
	}
	// SameRelease — origin: prerelease audit round 2, the K-F* sweep's gate. requirements
	// §6.7's join exemption requires the same RELEASE, and "v0.5.1" and "0.5.1" are the
	// same release: a joiner built from source was refused admission to a cluster of
	// goreleaser-built brokers running identical code, with a message telling the
	// operator their releases differ.
	if !proto.SameRelease(joinerRelease, proto.ReleaseVersion) {
		LogSkew(log, "cluster add: REFUSED — joiner release differs from this broker (requirements §6.7 join-gate exemption: membership admission requires an exact release match)",
			"node_id", nodeID, "joiner_release", joinerRelease, "this_release", proto.ReleaseVersion)
		return &ErrJoinVersionSkew{JoinerRelease: joinerRelease, ClusterRelease: proto.ReleaseVersion}
	}
	return nil
}

// logSkew tolerates a nil logger. StartJoinOperation runs on a ClusterAdmin whose logger is
// injected by wireClusterLate, and two of the five package-internal test fixtures that build a
// ClusterAdmin literal leave it unset. A gate that panicked on the logging path would replace an
// actionable typed refusal with a process crash.
func LogSkew(log *slog.Logger, msg string, args ...any) {
	if log == nil {
		return
	}
	log.Warn(msg, args...)
}

// splitJoinToken parses "<nonce>:<sigHex>" (the `cluster sign-join` output).
func SplitJoinToken(tok string) (nonce, sigHex string, ok bool) {
	for i := 0; i < len(tok); i++ {
		if tok[i] == ':' {
			return tok[:i], tok[i+1:], i > 0 && i+1 < len(tok)
		}
	}
	return "", "", false
}

// leaderHost resolves a leader node_id to its roster public_host (for the non-leader
// fail-fast hint). Empty when unknown.
func (a *ClusterAdmin) LeaderHost(leaderID string) string {
	if leaderID == "" {
		return ""
	}
	var host string
	_ = a.Node.BoundedStaleRead(func(db *sql.DB) error {
		return db.QueryRow(`SELECT public_host FROM cluster_nodes WHERE node_id=?`, leaderID).Scan(&host)
	})
	return host
}

func (e *ErrJoinVersionSkew) Error() string {
	// Wording note (upgrade-safety S10): requirements §6.7 is now the N-1
	// window; the JOIN gate is its explicitly-named exemption (a joiner is a
	// machine being added — reinstalling it to match blocks no deployed
	// node's rollback). These strings cite the exemption, not the old
	// "identical major.minor.patch across the stack" rule it replaced.
	if e.MissingField != "" {
		return fmt.Sprintf("version skew: joiner did not declare %s — cluster membership requires an exact release match (requirements §6.7 join-gate exemption); upgrade the joiner and regenerate its join bundle before adding it",
			e.MissingField)
	}
	if e.JoinerRelease != "" || e.ClusterRelease != "" {
		return fmt.Sprintf("version skew: joiner runs release %s but this cluster runs %s — cluster membership requires an exact release match (requirements §6.7 join-gate exemption); reinstall the joiner at %s before adding it",
			e.JoinerRelease, e.ClusterRelease, e.ClusterRelease)
	}
	return fmt.Sprintf("version skew: joiner speaks proto v%d but this cluster is proto v%d — reinstall the joiner on a matching release before adding it",
		e.JoinerProto, e.ClusterProto)
}

// observePollWindow bounds the health scatter-gather (a peer that does not answer within it
// is treated as broker_down). observeTickInterval is how often the leader polls;
// observeLagThreshold is the command-domain AppliedIndex gap that counts as raft_lag.
const (
	ObservePollWindow   = 2 * time.Second
	ObserveTickInterval = 5 * time.Second
	ObserveLagThreshold = 64
)

// forceSingleActive reports whether the persisted D7 force_single_active marker is set in the
// replicated cluster_meta (read-only; any broker's local committed copy).
func ForceSingleActive(db *sql.DB) bool {
	var one int
	err := db.QueryRow(`SELECT 1 FROM cluster_meta WHERE key=? LIMIT 1`, cluster.MetaKeyForceSingle).Scan(&one)
	return err == nil
}

// upgradeActive reports whether a `cluster upgrade` roll holds the cluster-scoped lock marker
// (External-review B2). Membership ops refuse while it is set.
func UpgradeActive(db *sql.DB) bool {
	var one int
	err := db.QueryRow(`SELECT 1 FROM cluster_meta WHERE key=? LIMIT 1`, cluster.MetaKeyUpgradeActive).Scan(&one)
	return err == nil
}

// upgradeLockHeld answers the narrower question "is a roll still DRIVING", which
// is different from upgradeActive's "is the cluster mid-roll". origin: prerelease
// audit round 2, H-2.
//
// Both readings are needed and they are not interchangeable:
//
//   - `cluster join` / `cluster retire` must be fenced for as long as the cluster
//     sits in a partial-roll state, whether or not anyone is still driving it —
//     that is upgradeActive, and it is deliberately unchanged.
//   - A new roll must be excluded only by a roll that is still alive. Using the
//     existence check for THAT made a HALTed roll's own marker refuse the re-run
//     its HALT message instructs the operator to perform, and blocked the
//     emergency `--to-version <older>` rollback for the whole lease TTL.
//
// Fail closed on anything unknown: LeaseExpired already returns false for an
// absent lease (a pre-R7b lock never expires by design), and a read error is
// treated as held.
func UpgradeLockHeld(db *sql.DB, now time.Time) bool {
	if !UpgradeActive(db) {
		return false
	}
	expired, err := cluster.LeaseExpired(db, cluster.MetaKeyUpgradeLease, now)
	if err != nil {
		return true
	}
	return !expired
}

// growActiveJoiner returns the joiner node_id a `cluster add` grow holds the cluster_grow_active marker for
// (G4 §B), or "" when no grow is in flight. The VALUE (joiner id) lets the leader refuse a lock for a
// DIFFERENT joiner while one grow runs, yet recognize a re-run of the SAME joiner as its own (resume). A
// retire refuses while any grow is active; the grow's OWN join op is NOT gated on this (the carve-out in
// StartJoinOperation/driveInFlightOperations) — else the grow would deadlock its own membership change.
func GrowActiveJoiner(db *sql.DB) string {
	var joiner string
	if err := db.QueryRow(`SELECT value FROM cluster_meta WHERE key=? LIMIT 1`, cluster.MetaKeyGrowActive).Scan(&joiner); err != nil {
		return ""
	}
	return joiner
}

// leaseExpiryString (R7b) renders a lock lease's expiry for the health reply, or "" when the lock carries
// no lease (a pre-R7b acquire) or the row is unreadable. Deliberately NOT an error: the health probe must
// answer even on a broker whose lease row is malformed, and "" is the honest, fail-closed answer — the
// reaper declines to expire exactly the same cases.
func LeaseExpiryString(db *sql.DB, key string) string {
	exp, err := cluster.LeaseExpiry(db, key)
	if err != nil {
		return ""
	}
	return exp.UTC().Format(time.RFC3339Nano)
}

// drainingWithoutMarker reports whether a node in phase DRAINING is missing its marker — the
// corrected form of the roadmap's second predicate. It is a REPORTING helper: nothing here writes.
//
// Why this combination is not a legal transient, on all three producers:
//
//	DrainNode      raises the marker, then later flips VOTER -> DRAINING.
//	driveRetire    raises the marker at NO_NEW_HOME, then flips the phase at REHOME_EXPOSES.
//	AbortDrain     restores the phase to VOTER FIRST, then clears the marker.
//
// So the marker is always present before the phase becomes DRAINING and is always cleared after it
// stops being DRAINING. DRAINING-without-marker therefore means a drain died between its own steps,
// and nothing will resume it: the "no non-terminal op" conjunct rules out the one case that would
// (a retire that started from an already-DRAINING phase and has not yet reached NO_NEW_HOME).
func DrainingWithoutMarker(phase string, markerRaised, hasActiveOp bool) bool {
	return phase == PhaseDraining && !markerRaised && !hasActiveOp
}
