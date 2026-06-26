package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/LinZiyang666/tether/internal/adminsock"
)

// cluster_status_card.go — B7 DOC#6(a): the `cluster status --card` glance render. It is a SECOND
// RENDERER over the leader's already-computed ClusterStatusReport — it computes NO new verdict
// (Health/Verdict/Banner/NextStep are authoritative; the card only reformats them + extracts a
// human "top reason" mirroring the computeHealth degraded triggers). Plus the DOC#6(c) incident
// export footer hint when the cluster is not healthy.

// renderClusterStatusCard writes the glance card. The default table render is unchanged; this is
// opt-in via --card.
func renderClusterStatusCard(w io.Writer, rep *adminsock.ClusterStatusReport) {
	_, _ = fmt.Fprintf(w, "  CLUSTER  %s\n", cardHeadline(rep))
	if rep.Verdict != "" {
		_, _ = fmt.Fprintf(w, "  %s\n", rep.Verdict)
	}
	leader := rep.LeaderID
	if leader == "" {
		leader = "(none — election in progress or quorum lost)"
	}
	_, _ = fmt.Fprintf(w, "  leader: %s\n", leader)

	if len(rep.Nodes) > 0 {
		_, _ = fmt.Fprintln(w, "  nodes:")
		for _, n := range rep.Nodes {
			ver := n.ReleaseVersion
			if ver == "" {
				ver = "?"
			}
			reach := n.ReachSource
			if !n.Reachable {
				reach = "UNREACHABLE(" + n.ReachSource + ")"
			}
			flags := ""
			if n.Inconsistent {
				flags += " INCONSISTENT"
			}
			role := n.Role
			if role == "" {
				role = "-"
			}
			_, _ = fmt.Fprintf(w, "    %-14s %-26s %-7s ver=%-10s %s%s\n", n.NodeID, n.Phase, role, ver, reach, flags)
		}
	}

	if rep.Banner != "" {
		_, _ = fmt.Fprintf(w, "  what's wrong: %s\n", rep.Banner)
	}
	if rep.NextStep != "" {
		_, _ = fmt.Fprintf(w, "  what to do:   %s\n", rep.NextStep)
	}
	for _, e := range rep.Errors {
		_, _ = fmt.Fprintf(w, "  ! %s\n", e)
	}
	// DOC#6(c) incident-export glue: an un-healthy cluster is a forensic moment.
	if hint := incidentExportHint(rep); hint != "" {
		_, _ = fmt.Fprintf(w, "  capture an incident bundle: %s\n", hint)
	}
	_, _ = fmt.Fprintf(w, "\n  %s (exit %d)\n", rep.Health, rep.ExitCode)
}

// cardHeadline turns the authoritative Health into a one-line headline (+ a top reason when
// DEGRADED).
func cardHeadline(rep *adminsock.ClusterStatusReport) string {
	// C6 建议6: a DEGRADED at N<=2 is the NOT-HA first-class state (works, no production HA) — surfaced
	// via health_label (the legacy Health stays "DEGRADED"). Re-key the headline off the label here.
	if rep.HealthLabel == "NOT-HA" {
		base := "NOT-HA (works; no production HA — add voters to N>=3)"
		// C6-STATUS-1: a genuine degradation at N<=2 (INCONSISTENT/unreachable/stream-below-target)
		// still has a top reason — keep it in the headline (pre-C6 rendered "DEGRADED: <reason>").
		if r := cardTopReason(rep); r != "" {
			return base + " — " + r
		}
		return base
	}
	switch rep.Health {
	case "HEALTHY_HA":
		return "HEALTHY (HA)"
	case "DEGRADED":
		if r := cardTopReason(rep); r != "" {
			return "DEGRADED-WRITABLE: " + r
		}
		return "DEGRADED-WRITABLE"
	case "QUORUM_LOST":
		return "READ-ONLY (quorum lost)"
	case "FORCE_SINGLE":
		return "FORCE-SINGLE (no HA / no integrity until recovered)"
	case "":
		return "(offline roster snapshot)"
	default:
		return rep.Health
	}
}

// cardTopReason extracts the single most important degraded reason from the node table — a pure
// CLI mirror of the computeHealth degraded=true triggers (it does NOT recompute the verdict).
func cardTopReason(rep *adminsock.ClusterStatusReport) string {
	for _, n := range rep.Nodes {
		if n.Inconsistent {
			return "node " + n.NodeID + " roster/raft INCONSISTENT"
		}
	}
	for _, n := range rep.Nodes {
		if n.ReachSource == "nats-health" && !n.Reachable {
			return "voter " + n.NodeID + " unreachable"
		}
	}
	for _, n := range rep.Nodes {
		if n.StreamTarget > 0 && n.StreamActual < n.StreamTarget {
			return fmt.Sprintf("stream replicas below target (%d/%d) on %s", n.StreamActual, n.StreamTarget, n.NodeID)
		}
	}
	for _, n := range rep.Nodes {
		// Stage-C m4: mirror computeHealth's degraded-phase set exactly (it does NOT degrade on
		// JOIN_VERIFIED_PENDING_VOTER), so the card headline never misattributes.
		switch n.Phase {
		case "CATCHING_UP", "VOTER_ADD_FAILED", "DRAINING", "RETIRING":
			return "node " + n.NodeID + " mid-join/drain (" + strings.ToLower(n.Phase) + ")"
		}
	}
	for _, n := range rep.Nodes {
		if n.DiskFreePct > 0 && n.DiskFreePct < 10 {
			return fmt.Sprintf("low disk on %s (%d%% free)", n.NodeID, n.DiskFreePct)
		}
		if n.PortsTotal > 0 && n.PortsUsed*10 >= n.PortsTotal*9 {
			return fmt.Sprintf("ports near exhausted on %s (%d/%d)", n.NodeID, n.PortsUsed, n.PortsTotal)
		}
	}
	return "fault tolerance reduced — see the table"
}

// incidentExportHint returns the exact `cluster export-incident` line to run when the cluster is
// in a forensic state (DEGRADED / QUORUM_LOST / FORCE_SINGLE), else "".
func incidentExportHint(rep *adminsock.ClusterStatusReport) string {
	switch rep.Health {
	case "DEGRADED", "QUORUM_LOST", "FORCE_SINGLE":
		return "tether cluster recovery incident export --since 24h --out incident.json"
	default:
		return ""
	}
}
