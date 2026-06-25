package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
)

// D8b (§10) ctl-side client-synthesized gating + banner. These are genuinely LIVE in ctl but
// INERT at N=1: the probe/fetch get ErrNoResponders (no broker answers the member-reachable
// ctrl.by.<actor>.cluster-health/alert.ls subjects until D9 wires the responders), so the
// gate never fires and the banner never renders — byte-identical to today.

const (
	clusterHealthWindow = 600 * time.Millisecond // broadcast corroboration collection window
	alertLsTimeout      = 500 * time.Millisecond // banner fetch (best-effort, one round-trip)
)

// probeClusterHealth broadcasts a cluster-health request and collects ALL broker replies
// within a window (a destructive op must corroborate every reachable view, §10.4). Zero
// replies — production N=1, or ctl unable to reach any broker — yields nil ⇒ no gate.
func probeClusterHealth(nc *nats.Conn, actor string) []proto.ClusterHealthResp {
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.PublishRequest(proto.SubjCtrlClusterHealth(actor), inbox, nil); err != nil {
		return nil
	}
	deadline := time.Now().Add(clusterHealthWindow)
	var replies []proto.ClusterHealthResp
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		msg, err := sub.NextMsg(remaining)
		if err != nil {
			break // timeout / no more responders
		}
		// No-responders fast-path (M1): a manual SubscribeSync+PublishRequest does NOT get
		// Conn.Request's ErrNoResponders, so at N=1 (no broker subscribes the health subject)
		// the server's 503 sentinel arrives as a successful empty message — detect it and
		// return at once, else `tether session rm` would block the full window every time
		// (breaking the build-and-prove "byte-identical at N=1" wall-clock).
		if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
			break
		}
		var r proto.ClusterHealthResp
		if json.Unmarshal(msg.Data, &r) == nil {
			replies = append(replies, r)
		}
	}
	return replies
}

// gateDestructive runs the client-synth severe gate before a destructive op (§10.4). It is an
// ADVISORY pre-check — the authoritative protection is the broker rejecting a write it cannot
// quorum-serve. At N=1 (no responder) the gate never fires. A real cluster reporting
// quorum_lost or force_single_active BLOCKS unless ackAlerts (the --ack-alerts override).
func gateDestructive(nc *nats.Conn, actor string, ackAlerts bool) error {
	if ackAlerts {
		return nil // explicit --ack-alerts override: no need to probe the cluster at all
	}
	gate := proto.EvalDestructiveGate(probeClusterHealth(nc, actor))
	if !gate.Blocked() {
		return nil
	}
	return errors.New(gateBlockMessage(gate))
}

// gateBlockMessage renders the BLOCKED message for a fired destructive gate (B1 item 5). It is a
// PURE function (no NATS) so the no-nuclear-leak property is unit-testable. It deliberately does
// NOT name the operator-only escape hatches (`cluster force-single` / `cluster recover`): a
// regular ctl user who is merely blocked from a write must never be nudged into a split-brain-
// risking or DB-wiping command. It points at the read-only `tether cluster status` (now runnable
// from a laptop via --remote, B1 item 1) + the runbook, splits the transient (quorum_lost, may
// self-clear) from the persistent (force_single_active) case, and says "condition" not "alert" so
// the user is not misrouted to `alert ack` (which cannot clear these cluster-health gates, §5.7).
func gateBlockMessage(gate proto.DestructiveGate) string {
	var b strings.Builder
	b.WriteString("BLOCKED: this command needs a healthy broker cluster, and the cluster is reporting a severe\n")
	b.WriteString("condition right now (re-run with --ack-alerts to force just this one command through).\n")
	if gate.QuorumLost {
		b.WriteString("  • quorum_lost — the brokers cannot agree on a leader, so the cluster is read-only.\n")
		b.WriteString("      → This sometimes clears on its own: wait ~30s and retry.\n")
		b.WriteString("      → If it does not clear, check with `tether cluster status` and see docs/cluster-runbook.md §3.\n")
	}
	if gate.ForceSingleActive {
		b.WriteString("  • force_single_active — the cluster is running on one emergency broker with no backup.\n")
		b.WriteString("      → Waiting will NOT fix this; full redundancy must be restored on a broker host.\n")
		b.WriteString("      → Check with `tether cluster status` and see docs/cluster-runbook.md §3.\n")
	}
	return b.String()
}

// fetchAlertsStrict pulls the active alert set from any one broker (queue-group). It returns a
// real error on no-responder / timeout / malformed reply — for the EXPLICIT `alert ls` command,
// which must NOT show a false "no active alerts" when the store could not be queried (F4).
func fetchAlertsStrict(nc *nats.Conn, actor string) ([]proto.AlertView, error) {
	msg, err := nc.Request(proto.SubjCtrlAlertLs(actor), nil, alertLsTimeout)
	if err != nil {
		return nil, fmt.Errorf("alert ls: query: %w (no broker answered the alert store)", err)
	}
	var resp proto.AlertLsResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("alert ls: malformed reply: %w", err)
	}
	return resp.Alerts, nil
}

// fetchAlerts is the BEST-EFFORT banner variant: a timeout / no responder / malformed reply
// yields nil so the banner never breaks a read command (the always-on banner is advisory).
func fetchAlerts(nc *nats.Conn, actor string) []proto.AlertView {
	alerts, err := fetchAlertsStrict(nc, actor)
	if err != nil {
		return nil
	}
	return alerts
}

// renderBanner writes the active SEVERE alerts to w (stderr) so stdout stays script-parseable.
// INFO kinds (below_quorum, broker_draining, …) live in `alert ls`, not the always-on banner
// (no alert fatigue). jsonMode suppresses it (a --json command already carries its own data).
func renderBanner(w io.Writer, alerts []proto.AlertView, jsonMode bool) {
	if jsonMode {
		return
	}
	for _, a := range alerts {
		if a.Severity != proto.SeveritySevere {
			continue
		}
		_, _ = fmt.Fprintf(w, "‼ ALERT [%s] %s\n", a.Kind, a.Message)
	}
}

// withBanner fetches + renders the severe-alert banner to stderr after a read command.
// Best-effort; jsonMode suppresses it.
func withBanner(nc *nats.Conn, actor string, jsonMode bool) {
	renderBanner(os.Stderr, fetchAlerts(nc, actor), jsonMode)
}

// printAlertLs renders the full alert list (all severities) for the `alert ls` command, with
// the cluster-level ack shown per row (§10.1). Sorted by severity (severe first) then kind.
func printAlertLs(w io.Writer, alerts []proto.AlertView) {
	sort.SliceStable(alerts, func(i, j int) bool {
		if alerts[i].Severity != alerts[j].Severity {
			return alerts[i].Severity == proto.SeveritySevere // severe first
		}
		return alerts[i].Kind < alerts[j].Kind
	})
	if len(alerts) == 0 {
		_, _ = fmt.Fprintln(w, "no active alerts")
		return
	}
	for _, a := range alerts {
		line := fmt.Sprintf("[%s] %-22s %s", a.Severity, a.Kind, a.Message)
		if a.AckedBy != "" {
			line += fmt.Sprintf("  (acked by %s at %s)", a.AckedBy, a.AckedAt)
		}
		_, _ = fmt.Fprintln(w, line)
	}
}

// ackAlert sends a cluster-level ack for dedupKey (one broker forwards it to the leader). It is
// FAIL-CLOSED (F3): only the exact reply "ok" is success — a broker "error: …" (e.g. no leader
// accepted the ack), an empty/malformed reply, or a transport failure all return a non-nil
// error, so an operator/script never treats an un-committed ack as acknowledged.
func ackAlert(nc *nats.Conn, actor, dedupKey string) (string, error) {
	body, err := json.Marshal(proto.AlertAckReq{DedupKey: dedupKey})
	if err != nil {
		return "", err
	}
	msg, err := nc.Request(proto.SubjCtrlAlertAck(actor), body, alertLsTimeout)
	if err != nil {
		return "", fmt.Errorf("alert ack: request: %w (no broker forwarded the ack)", err)
	}
	reply := string(msg.Data)
	if reply != "ok" {
		if reply == "" {
			reply = "(empty reply)"
		}
		return reply, fmt.Errorf("alert ack not accepted: %s", reply)
	}
	return reply, nil
}
