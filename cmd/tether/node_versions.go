package main

import (
	"fmt"
	"sort"
	"text/tabwriter"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// node_versions.go — G5 #19: correlate each broker HOST's daemon version (from the cluster-health VER +
// its self-declared co-located agent nid) with that agent's RELEASE (from the node list), yielding ONE
// whole-host upgrade verdict + a same-host version-skew flag. Pure — `node ls --brokers` and the
// `cluster upgrade` idempotency read both fold live state into these inputs and call this.

// brokerDaemon is one broker's daemon self-report needed for the #19 correlation.
type brokerDaemon struct {
	BrokerVer         string // the broker daemon's running release ("" ⇒ did not report → rendered "?")
	ColocatedAgentNID string // the co-located agent nid it self-declared ("" ⇒ fall back to node_id==nid)
}

// brokerVersionRow is one whole-host dual-version verdict.
// JSON keys are snake_case (schema node_ls_brokers v2) — matching proto.NodeListEntry and every other
// --json payload in this package. v1 emitted the bare Go field names (PascalCase); renaming the keys is
// a breaking change, hence the schema_version bump below.
type brokerVersionRow struct {
	NodeID      string `json:"node_id"`       // broker cluster node_id
	AgentNID    string `json:"agent_nid"`     // its co-located agent nid
	Assumed     bool   `json:"assumed"`       // AgentNID came from the node_id==nid convention (a pre-G5 broker), not self-declared
	BrokerVer   string `json:"broker_ver"`    // broker daemon release ("?" if unreported)
	AgentVer    string `json:"agent_ver"`     // co-located agent release ("?" if the agent is absent from the node list)
	Skew        bool   `json:"skew"`          // broker and agent releases differ → whole-host upgrade is INCOMPLETE
	WholeHostAt bool   `json:"whole_host_at"` // both == target (the single #19 "done" criterion); only meaningful when target != ""
}

// correlateBrokerVersions joins broker daemons (nodeID → {brokerVer, colocatedAgentNID}) with agent
// releases (nid → release). target may be "" (no WholeHostAt verdict). Deterministic — sorted by NodeID.
// A broker that did not self-declare its agent nid falls back to the node_id==nid convention, flagged
// Assumed so the operator is never shown a silently-wrong pairing.
func correlateBrokerVersions(brokers map[string]brokerDaemon, agentRelease map[string]string, target string) []brokerVersionRow {
	ids := make([]string, 0, len(brokers))
	for id := range brokers {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	out := make([]brokerVersionRow, 0, len(ids))
	for _, id := range ids {
		d := brokers[id]
		row := brokerVersionRow{NodeID: id, BrokerVer: orQ(d.BrokerVer)}
		row.AgentNID = d.ColocatedAgentNID
		if row.AgentNID == "" {
			row.AgentNID = id // node_id==nid convention (pre-G5 broker)
			row.Assumed = true
		}
		av, ok := agentRelease[row.AgentNID]
		row.AgentVer = orQ(av) // C7: orQ already maps a missing/empty release ("" when !ok) to "?" — no separate !ok branch needed
		// Skew: both sides KNOWN and different. Two "?" (both unknown) is not a skew claim.
		row.Skew = d.BrokerVer != "" && ok && av != "" && d.BrokerVer != av
		if target != "" {
			row.WholeHostAt = d.BrokerVer == target && ok && av == target
		}
		out = append(out, row)
	}
	return out
}

func orQ(s string) string {
	if s == "" {
		return "?"
	}
	return s
}

// renderNodeLsBrokers (G5 #19 W12) folds the cluster-health probe (broker daemons: VER + self-declared
// co-located agent nid) with this session's node list (agent RELEASEs) and prints the per-broker-host
// dual-version table + a same-host skew flag. Single-broker / no responder → a plain note (no table), so
// a non-clustered deployment stays byte-identical in spirit to plain `node ls`.
func renderNodeLsBrokers(cmd *cobra.Command, nc *nats.Conn, actor string, nodes []proto.NodeListEntry, asJSON bool) error {
	agentRelease := make(map[string]string, len(nodes))
	for _, n := range nodes {
		agentRelease[n.NID] = n.ReleaseVersion
	}
	brokers := map[string]brokerDaemon{}
	for _, h := range probeClusterHealth(nc, actor) {
		if h.NodeID != "" {
			brokers[h.NodeID] = brokerDaemon{BrokerVer: h.ReleaseVersion, ColocatedAgentNID: h.ColocatedAgentNID}
		}
	}
	out := cmd.OutOrStdout()
	if len(brokers) == 0 {
		_, _ = fmt.Fprintln(out, "(no clustered broker answered over NATS — single broker, or brokers unreachable)")
		return nil
	}
	rows := correlateBrokerVersions(brokers, agentRelease, "")
	if asJSON {
		return emitJSON(out, struct {
			Schema        string             `json:"schema"`
			SchemaVersion int                `json:"schema_version"`
			Brokers       []brokerVersionRow `json:"brokers"`
		}{Schema: "node_ls_brokers", SchemaVersion: 2, Brokers: rows})
	}
	tw := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(tw, "BROKER\tAGENT_NID\tBROKER_VER\tAGENT_VER\tSKEW")
	for _, r := range rows {
		anid := r.AgentNID
		if r.Assumed {
			anid += " (assumed)"
		}
		skew := ""
		if r.Skew {
			skew = "SKEW"
		}
		_, _ = fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", r.NodeID, anid, r.BrokerVer, r.AgentVer, skew)
	}
	return tw.Flush()
}
