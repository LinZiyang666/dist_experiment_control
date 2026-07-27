package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/spf13/cobra"
)

// acct_nk_render_test.go (batch B, B4 — plan §15.2 "诚实的 ACCT.NK 列")
//
// The renderer half. Before this, ACCT.NK was:
//
//	acct := "N"; if n.AccountNkMatch { acct = "Y" }
//
// Two states over a bool that only ONE of the three producers ever set. The offline disk-snapshot
// view (renderOfflineClusterStatus, which never contacts anything) therefore printed **N** for every
// roster node — "every broker's account key is wrong" — on the exact command an operator runs when
// the cluster is down and they are looking for a cause.

func TestAcctCellIsThreeStates(t *testing.T) {
	cases := []struct {
		name string
		in   adminsock.ClusterNodeStatus
		want string
	}{
		{"reported match", adminsock.ClusterNodeStatus{AccountNkReported: true, AccountNkMatch: true}, "Y"},
		{"reported mismatch", adminsock.ClusterNodeStatus{AccountNkReported: true, AccountNkMatch: false}, "N"},
		{"not reported", adminsock.ClusterNodeStatus{}, "?"},
		// The load-bearing one: an unreported row must NOT be rendered from the stale Match bool.
		// A `Match:true, Reported:false` row is what a pre-v6 broker's status would decode to if
		// anything ever set Match without Reported, and rendering Y there is the original defect.
		{"match set but NOT reported ⇒ still ?", adminsock.ClusterNodeStatus{AccountNkMatch: true}, "?"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := acctCell(c.in); got != c.want {
				t.Fatalf("acctCell(%+v) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestOfflineViewDoesNotClaimAKeyMismatch is the regression this item exists for. The offline view
// compares nothing, so every one of its rows must render "?" — never N.
func TestOfflineViewDoesNotClaimAKeyMismatch(t *testing.T) {
	rep := &adminsock.ClusterStatusReport{
		SchemaVersion: 1, View: "offline", Health: "ROSTER_UNREACHABLE", ExitCode: 2,
		Nodes: []adminsock.ClusterNodeStatus{
			{NodeID: "brk-a", Name: "a", Phase: "VOTER", ReachSource: "raft-ping"},
			{NodeID: "brk-b", Name: "b", Phase: "VOTER", ReachSource: "raft-ping"},
		},
	}
	out := renderToString(t, rep)

	// Read the column by NAME, not by index. An index-based check is what a first draft of this test
	// did, and it silently stopped discriminating: the offline rows leave ROLE and VER empty, so
	// strings.Fields collapses them and field 6 is no longer ACCT.NK. It passed under a mutation that
	// restored the two-state renderer — i.e. it was green for the wrong reason.
	rows := statusTableRows(t, out)
	if len(rows) != 2 {
		t.Fatalf("expected 2 data rows, parsed %d from:\n%s", len(rows), out)
	}
	for _, row := range rows {
		if row["ACCT.NK"] == "N" {
			t.Fatalf("the offline view renders ACCT.NK=N for %s. It never contacted anything and "+
				"never compared a key — printing N tells an operator mid-outage that their account "+
				"keys diverged, which is a fabricated finding.", row["NODE_ID"])
		}
		if row["ACCT.NK"] != "?" {
			t.Errorf("offline row %s rendered ACCT.NK=%q, want \"?\" (no answer)", row["NODE_ID"], row["ACCT.NK"])
		}
	}
}

// statusTableRows parses the tabwriter table by column HEADER offsets, so a blank cell in any other
// column cannot shift what this test believes ACCT.NK is. Rows stop at the first blank line (the
// legend follows).
func statusTableRows(t *testing.T, out string) []map[string]string {
	t.Helper()
	lines := strings.Split(out, "\n")
	if len(lines) == 0 {
		t.Fatal("no output")
	}
	header := lines[0]
	if !strings.Contains(header, "ACCT.NK") {
		t.Fatalf("first line is not the status header: %q", header)
	}
	// Column start offsets, taken from the header's own layout (tabwriter pads to fixed widths).
	type col struct {
		name  string
		start int
	}
	var cols []col
	inField := false
	for i, r := range header {
		switch {
		case r != ' ' && !inField:
			inField = true
			cols = append(cols, col{start: i})
		case r == ' ' && inField:
			inField = false
			cols[len(cols)-1].name = strings.TrimSpace(header[cols[len(cols)-1].start:i])
		}
	}
	if inField {
		cols[len(cols)-1].name = strings.TrimSpace(header[cols[len(cols)-1].start:])
	}

	var rows []map[string]string
	for _, line := range lines[1:] {
		if strings.TrimSpace(line) == "" {
			break
		}
		row := map[string]string{}
		for i, c := range cols {
			end := len(line)
			if i+1 < len(cols) && cols[i+1].start < end {
				end = cols[i+1].start
			}
			if c.start >= len(line) {
				row[c.name] = ""
				continue
			}
			row[c.name] = strings.TrimSpace(line[c.start:end])
		}
		rows = append(rows, row)
	}
	return rows
}

// TestLegendDescribesTheColumnItActuallyRenders pins the legend against the renderer. The legend used
// to say "currently always Y — per-node verification not yet wired", which was accurate when written
// and became a lie the moment the column started carrying real answers. A legend that documents a
// fabrication is not honesty; it is a fabrication with a footnote.
func TestLegendDescribesTheColumnItActuallyRenders(t *testing.T) {
	rep := &adminsock.ClusterStatusReport{
		SchemaVersion: 1, View: "ctl-nats", Health: "HEALTHY_HA",
		Nodes: []adminsock.ClusterNodeStatus{{NodeID: "brk-a", AccountNkReported: true, AccountNkMatch: true}},
	}
	out := renderToString(t, rep)

	if strings.Contains(out, "always Y") || strings.Contains(out, "not yet wired") {
		t.Error("the legend still says the column is unwired / always Y, but the producer now " +
			"derives it from a per-node self-report")
	}
	// Every state the renderer can emit must be documented, or an operator cannot read the table.
	for _, state := range []string{"Y=", "N=", "?="} {
		if !strings.Contains(out, state) {
			t.Errorf("the legend does not document the %q state that acctCell can render:\n%s", state, out)
		}
	}
}

func renderToString(t *testing.T, rep *adminsock.ClusterStatusReport) string {
	t.Helper()
	buf := &bytes.Buffer{}
	cmd := &cobra.Command{}
	cmd.SetOut(buf)
	renderClusterStatus(cmd, rep)
	return buf.String()
}
