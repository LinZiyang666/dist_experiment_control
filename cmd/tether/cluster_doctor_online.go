package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/natsconf"
)

// cluster_doctor_online.go — B7 DOC#6(b): the ONLINE `cluster doctor` diagnostic. `cluster doctor`
// auto-detects — if a live admin socket answers OpClusterStatus, it turns each health condition of
// the authoritative ClusterStatusReport into a PASS/ADVISORY/FATAL DoctorCheck (reusing the
// existing DoctorCheck/renderDoctor types + exit-64-on-fatal contract); otherwise it falls back to
// the OFFLINE pre-init preflight (clusteroffline.Doctor). It computes NO new verdict.

// doctorLagThreshold mirrors the broker's observeLagThreshold (observability.go:188) — the §17
// applied-lag degrade threshold. Kept in sync by TestClusterDoctorOnlineThresholds.
const doctorLagThreshold = 64

func clusterDoctorOnline(rep *adminsock.ClusterStatusReport) []clusteroffline.DoctorCheck {
	check := func(name string, st clusteroffline.DoctorStatus, detail string) clusteroffline.DoctorCheck {
		return clusteroffline.DoctorCheck{Name: name, Status: st, Detail: detail}
	}
	var out []clusteroffline.DoctorCheck

	// leader present.
	if rep.LeaderID == "" {
		out = append(out, check("leader", clusteroffline.DoctorFatal, "no raft leader (quorum lost or election in progress) — cluster is READ-ONLY"))
	} else {
		out = append(out, check("leader", clusteroffline.DoctorPass, "leader is "+rep.LeaderID))
	}

	// overall health verdict (authoritative).
	switch rep.Health {
	case "HEALTHY_HA":
		out = append(out, check("health", clusteroffline.DoctorPass, "HEALTHY_HA — "+rep.Verdict))
	case "DEGRADED":
		out = append(out, check("health", clusteroffline.DoctorAdvisory, "DEGRADED — "+rep.Banner))
	case "QUORUM_LOST":
		out = append(out, check("health", clusteroffline.DoctorFatal, "QUORUM_LOST (read-only) — "+rep.Banner))
	case "FORCE_SINGLE":
		out = append(out, check("health", clusteroffline.DoctorFatal, "FORCE_SINGLE (no HA / no integrity) — recover ASAP"))
	}

	// per-node conditions.
	var inconsistent, unreachable, behind, streamShort, lowDisk, nearPorts int
	// batch C: topology had NO doctor check at all, so `cluster doctor` PASSed every item on a
	// cluster whose reconciler was permanently wedged. Counted per state because the remedies differ:
	// STUCK needs the conf fixed, HELD needs `cluster add` and must never be told to run `--manual`.
	// origin: batch-c EXTERNAL review M2. This collected only Stuck and Held, so a Behind or an
	// UnknownAction voter fell through to the PASS branch — `cluster doctor` then printed "every
	// reached voter's NATS topology reconcile is converging" while the authoritative health report said
	// DEGRADED about that same voter. UnknownAction is the worse of the two to call PASS: it means this
	// binary CANNOT judge that broker, and the remedy is to upgrade the reader, not to wait.
	// The state set is closed, so the consumer covers all of it.
	var topoStuck, topoHeld, topoBehind, topoUnknown []string
	versions := map[string]bool{}
	// EXTERNAL review m1: keep the per-reason detail so the check names the right cause and remedy.
	inconsistentDetail := map[string]bool{}
	for _, n := range rep.Nodes {
		if n.Inconsistent {
			inconsistent++
			inconsistentDetail[adminsock.InconsistencyDetail(n.InconsistentReason)] = true
		}
		if n.ReachSource == "nats-health" && !n.Reachable {
			unreachable++
		}
		// Stage-C m2: mirror computeHealth's threshold (observeLagThreshold=64) — a voter 1–64 behind
		// is normal lag + HEALTHY_HA, so the doctor must not contradict itself by ADVISORY-ing it.
		if n.AppliedLag > doctorLagThreshold && n.Phase == "VOTER" {
			behind++
		}
		if n.StreamTarget > 0 && n.StreamActual < n.StreamTarget {
			streamShort++
		}
		if n.DiskFreePct > 0 && n.DiskFreePct < 10 {
			lowDisk++
		}
		if n.PortsTotal > 0 && n.PortsUsed*10 >= n.PortsTotal*9 {
			nearPorts++
		}
		if n.ReleaseVersion != "" {
			versions[n.ReleaseVersion] = true
		}
		// Only a REACHED voter's topology report means anything — mirror computeHealth's `reached`
		// gate rather than inventing a third one.
		if n.Phase == "VOTER" && (n.ReachSource == "self" || (n.ReachSource == "nats-health" && n.Reachable)) {
			switch natsconf.ClassifyTopo(n.TopoAction, n.TopoReconcileReason, n.TopoObserved, rep.TopoDesired, n.TopoReported) {
			case natsconf.TopoStuck:
				topoStuck = append(topoStuck, n.NodeID)
			case natsconf.TopoHeld:
				topoHeld = append(topoHeld, n.NodeID)
			case natsconf.TopoBehind:
				topoBehind = append(topoBehind, n.NodeID)
			case natsconf.TopoUnknownAction:
				topoUnknown = append(topoUnknown, n.NodeID)
			case natsconf.TopoUnreported, natsconf.TopoConverged:
				// Non-degrading: this voter contributes no finding. Named explicitly so that a new
				// TopoState cannot join the enum and be silently treated as healthy here — which is
				// precisely how this function came to report PASS on a cluster that was not
				// converging (batch-C external review M2).
			}
		}
	}
	add := func(name string, n int, fatal bool, okDetail, badFmt string) {
		switch {
		case n == 0:
			out = append(out, check(name, clusteroffline.DoctorPass, okDetail))
		case fatal:
			out = append(out, check(name, clusteroffline.DoctorFatal, fmt.Sprintf(badFmt, n)))
		default:
			out = append(out, check(name, clusteroffline.DoctorAdvisory, fmt.Sprintf(badFmt, n)))
		}
	}
	// EXTERNAL review m1: the old bad-format text said "roster/raft INCONSISTENT — run cluster
	// doctor/status", which for the drain case named the wrong cause AND recursed the operator back
	// into the command they were already running. The detail is now derived per reason.
	if inconsistent == 0 {
		out = append(out, check("roster_consistency", clusteroffline.DoctorPass, "no INCONSISTENT nodes"))
	} else {
		reasons := make([]string, 0, len(inconsistentDetail))
		for d := range inconsistentDetail {
			reasons = append(reasons, d)
		}
		sort.Strings(reasons)
		out = append(out, check("roster_consistency", clusteroffline.DoctorFatal,
			fmt.Sprintf("%d node(s) INCONSISTENT: %s", inconsistent, strings.Join(reasons, "; "))))
	}
	add("reachability", unreachable, false, "every polled voter is reachable", "%d voter(s) unreachable")
	add("catch_up", behind, false, "every voter is caught up", "%d voter(s) trailing the leader's commit")
	add("stream_replicas", streamShort, false, "JS streams at target replicas", "%d stream(s) below target replica count")
	add("disk", lowDisk, false, "disk headroom ok on every reporting node", "%d node(s) low on disk (<10%% free)")
	add("ports", nearPorts, false, "port headroom ok", "%d node(s) near port exhaustion (>=90%% used)")

	// batch C: topology. STUCK is FATAL — the reconciler is fail-closed and no further topology
	// change on that broker can land, so the next grow/retire will hang at NATS_ROLLED_OUT. HELD is
	// ADVISORY and carries the NEGATIVE clause: before this batch a withheld cutover was classified
	// STUCK, whose remedy text recommends `reconcile nats --manual` — the one command that would
	// apply a clustered conf under a running standalone nats-server (G4 #10/#4).
	switch {
	case len(topoStuck) > 0:
		out = append(out, check("topology", clusteroffline.DoctorFatal, fmt.Sprintf(
			"topology reconcile STUCK on %v — %s", topoStuck, natsconf.TopoStuck.NextStep())))
	case len(topoHeld) > 0:
		out = append(out, check("topology", clusteroffline.DoctorAdvisory, fmt.Sprintf(
			"standalone→clustered cutover WITHHELD on %v — %s", topoHeld, natsconf.TopoHeld.NextStep())))
	case len(topoUnknown) > 0:
		// ADVISORY, not PASS and not FATAL: this binary cannot judge those brokers, and saying so is the
		// only honest verdict. FATAL would be an over-claim — a newer broker reporting a newer action is
		// not evidence of a fault.
		out = append(out, check("topology", clusteroffline.DoctorAdvisory, fmt.Sprintf(
			"topology action UNRECOGNIZED from %v (they are probably NEWER than this binary) — %s",
			topoUnknown, natsconf.TopoUnknownAction.NextStep())))
	case len(topoBehind) > 0:
		out = append(out, check("topology", clusteroffline.DoctorAdvisory, fmt.Sprintf(
			"topology still converging on %v — %s", topoBehind, natsconf.TopoBehind.NextStep())))
	default:
		out = append(out, check("topology", clusteroffline.DoctorPass, "every reached voter's NATS topology reconcile is converging"))
	}

	if len(versions) > 1 {
		out = append(out, check("version_skew", clusteroffline.DoctorAdvisory, fmt.Sprintf("%d distinct releases running — finish the rolling upgrade", len(versions))))
	} else {
		out = append(out, check("version_skew", clusteroffline.DoctorPass, "all nodes on one release"))
	}

	// v0.4.2 phase-fluidity: a loopback self-advertise blocks a cross-network grow (the new voter
	// cannot dial this leader). ADVISORY, not FATAL — a single-host cluster legitimately runs
	// loopback; this only matters when you intend to grow. Names set-raft-addr (NOT force-single).
	switch {
	case rep.SelfRaftAdvertiseLoopback && rep.SelfNatsRouteLoopback:
		out = append(out, check("raft_advertise", clusteroffline.DoctorAdvisory,
			fmt.Sprintf("self raft advertise (%s) AND nats_route are loopback — a cross-network grow cannot reach this broker; run `cluster set-raft-addr <public:7400> --route nats://<public:6222>` before `join approve` (NOT force-single)", rep.SelfRaftAdvertise)))
	case rep.SelfRaftAdvertiseLoopback:
		out = append(out, check("raft_advertise", clusteroffline.DoctorAdvisory,
			fmt.Sprintf("self raft advertise (%s) is loopback — a cross-network grow cannot reach this broker; run `cluster set-raft-addr <public:7400>` before `join approve` (NOT force-single)", rep.SelfRaftAdvertise)))
	case rep.SelfNatsRouteLoopback:
		out = append(out, check("nats_route", clusteroffline.DoctorAdvisory,
			"self nats_route is loopback — the NATS route mesh cannot form cross-network; run `cluster set-raft-addr --route nats://<public:6222>` before growing"))
	default:
		out = append(out, check("raft_advertise", clusteroffline.DoctorPass, "self advertise addresses are peer-reachable (grow-ready)"))
	}
	return out
}
