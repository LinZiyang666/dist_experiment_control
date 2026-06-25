package main

import (
	"fmt"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
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
	versions := map[string]bool{}
	for _, n := range rep.Nodes {
		if n.Inconsistent {
			inconsistent++
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
	add("roster_consistency", inconsistent, true, "no roster/raft INCONSISTENT nodes", "%d node(s) roster/raft INCONSISTENT — run `cluster doctor`/`status`")
	add("reachability", unreachable, false, "every polled voter is reachable", "%d voter(s) unreachable")
	add("catch_up", behind, false, "every voter is caught up", "%d voter(s) trailing the leader's commit")
	add("stream_replicas", streamShort, false, "JS streams at target replicas", "%d stream(s) below target replica count")
	add("disk", lowDisk, false, "disk headroom ok on every reporting node", "%d node(s) low on disk (<10%% free)")
	add("ports", nearPorts, false, "port headroom ok", "%d node(s) near port exhaustion (>=90%% used)")

	if len(versions) > 1 {
		out = append(out, check("version_skew", clusteroffline.DoctorAdvisory, fmt.Sprintf("%d distinct releases running — finish the rolling upgrade", len(versions))))
	} else {
		out = append(out, check("version_skew", clusteroffline.DoctorPass, "all nodes on one release"))
	}
	return out
}
