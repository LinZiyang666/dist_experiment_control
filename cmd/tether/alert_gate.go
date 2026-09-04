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

// THREE DEADLINES, NOT ONE, because the three requests behind them are not the same
// shape (prerelease audit cli-ctl/CLI-F3).
//
// The load-bearing one is alertAckTimeout. `alert ack` is not a local read: the
// receiving broker FORWARDS it to the raft leader and waits for the commit, and the
// broker's own budget for that forward is ExposeForwardTimeout — 5 seconds by default
// (internal/broker/broker.go). Giving the ctl 500ms meant the ctl gave up first on every
// ack that had to cross to a leader under any load at all, and reported "no broker
// forwarded the ack" for an ack that was, at that moment, being committed. The ctl's
// deadline has to be at least the broker's, or the ctl is timing its own patience
// against a number it does not control.
//
// The other two are margins rather than fixed defects: 500ms is enough for a
// single-round-trip read on a healthy bus, and both are best-effort paths that degrade
// to "no banner" rather than to a wrong answer.
const (
	clusterHealthWindow = 600 * time.Millisecond // broadcast corroboration collection window
	alertLsTimeout      = 500 * time.Millisecond // banner fetch (best-effort, one round-trip)

	// alertAckTimeout MUST be >= the broker's ExposeForwardTimeout (5s), which is the
	// budget the broker itself spends forwarding the ack to the leader.
	// origin: internal/broker/broker.go, Config.ExposeForwardTimeout.
	alertAckTimeout = 6 * time.Second
)

// probeClusterHealth broadcasts a cluster-health request and collects ALL broker replies
// within a window (a destructive op must corroborate every reachable view, §10.4).
//
// IT DISTINGUISHES "NOBODY IS THERE" FROM "I COULD NOT ASK", and that distinction is the
// whole safety property — origin: prerelease audit increment 2 internal review,
// pairing-sweep/IMPACT-F1.
//
//	(nil, nil)   nobody answered. Production N=1, or a cluster with no health responder.
//	             The gate legitimately does not fire.
//	(nil, err)   the probe could not be MADE: the subscribe or the publish was refused.
//	             The caller has learned nothing, and a destructive operation must not
//	             read that as "healthy".
//
// It used to return a bare slice and collapse both into nil. A ctl whose reply inbox was
// refused therefore reported an empty cluster, EvalDestructiveGate(nil) is an unblocked
// gate, and `tether session rm` proceeded — the safety gate failing OPEN on exactly the
// misconfiguration it was written to survive. An earlier external review (H3/H-4) saw
// the collapse and worked around it at ONE call site by counting responders; this is the
// root fix the workaround was standing in for.
func probeClusterHealth(nc *nats.Conn, actor string) ([]proto.ClusterHealthResp, error) {
	// nc.NewInbox(), NOT the package-level nats.NewInbox(): only the method honours
	// this connection's CustomInboxPrefix. With per-identity inbox subtrees
	// (auth.InboxPrefixFor) the package-level helper mints `_INBOX.<nuid>`, which
	// this connection is no longer granted, so the SubscribeSync below would fail.
	// origin: prerelease audit proto-auth-acl/L1-F1 verifier.
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, fmt.Errorf("cluster health probe: subscribe to own reply inbox %q: %w", inbox, err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	// A REFUSED SUBSCRIBE DOES NOT COME BACK FROM SubscribeSync. nats-server answers
	// `-ERR 'Permissions Violation for Subscription to "…"'` asynchronously, so the call
	// above returns nil and the subscription is simply dead. Flush and look: the server
	// processes SUB before the PING that Flush sends, and nats.go's readLoop sets
	// LastError() synchronously in processPermissionsViolation, so by the time Flush
	// returns a refusal is already observable. No sleep, no polling.
	//
	// Without this the whole error return below is decorative — which the guard in
	// alerts_test.go demonstrated on the first attempt at this fix.
	if ferr := nc.FlushTimeout(2 * time.Second); ferr == nil {
		if last := nc.LastError(); last != nil && strings.Contains(last.Error(), inbox) {
			return nil, fmt.Errorf("cluster health probe: this connection may not subscribe to its "+
				"own reply inbox %q: %w", inbox, last)
		}
	}
	if err := nc.PublishRequest(proto.SubjCtrlClusterHealth(actor), inbox, nil); err != nil {
		return nil, fmt.Errorf("cluster health probe: publish: %w", err)
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
	return replies, nil
}

// probeClusterHealthAdvisory is probeClusterHealth for callers that DISPLAY the answer or
// POLL for a condition to become true, where "could not ask" and "nobody answered" lead
// to the same correct behaviour: show nothing, or keep waiting until the deadline.
//
// It exists so that folding the error away is a DECISION WITH A NAME rather than a `_`
// somebody added to make the compiler quiet. Every caller of this function must be one
// whose failure mode on an empty slice is to do LESS, never more — a caller that would
// treat empty as permission to proceed belongs on probeClusterHealth with the error
// handled. That distinction is what pairing-sweep/IMPACT-F1 was about: the destructive
// gate read a refused probe as a healthy cluster.
func probeClusterHealthAdvisory(nc *nats.Conn, actor string) []proto.ClusterHealthResp {
	replies, err := probeClusterHealth(nc, actor)
	if err != nil {
		return nil
	}
	return replies
}

// gateDestructive runs the client-synth severe gate before a destructive op (§10.4). It is an
// ADVISORY pre-check — the authoritative protection is the broker rejecting a write it cannot
// quorum-serve. At N=1 (no responder) the gate never fires. A real cluster reporting
// quorum_lost or force_single_active BLOCKS unless ackAlerts (the --ack-alerts override).
//
// A PROBE THAT COULD NOT BE MADE BLOCKS TOO, and is reported as its own condition rather
// than folded into quorum_lost: "your ctl cannot subscribe to its own reply inbox" and
// "the cluster lost quorum" call for completely different operator actions, and the
// second one's message would send somebody to look at brokers that are fine.
// --ack-alerts still overrides, above, which is the deliberate escape hatch.
func gateDestructive(nc *nats.Conn, actor string, ackAlerts bool) error {
	if ackAlerts {
		return nil // explicit --ack-alerts override: no need to probe the cluster at all
	}
	replies, err := probeClusterHealth(nc, actor)
	if err != nil {
		// 77 when the server REFUSED us and 69 when we simply could not reach it: the
		// first is a deployment that will keep refusing until a human changes something,
		// the second is worth a retry. Collapsing them would put a permanent condition in
		// the class automation retries.
		class := exitUnavailable
		if strings.Contains(err.Error(), "Permissions Violation") {
			class = exitNoPerm
		}
		return &ExitError{Class: class, Err: fmt.Errorf(
			"refusing a destructive operation: this ctl could not ask the cluster whether it is "+
				"healthy, so it has NOT been told the cluster is fine — it has been told nothing.\n"+
				"  cause: %w\n"+
				"  re-run with --ack-alerts to proceed anyway", err)}
	}
	gate := proto.EvalDestructiveGate(replies)
	if !gate.Blocked() {
		return nil
	}
	return &ExitError{Class: gateExitClass(gate), Err: errors.New(gateBlockMessage(gate))}
}

// gateExitClass splits the two blocking conditions by whether WAITING can fix them.
//
// origin: prerelease audit cli-ctl/CLI-F2. The gate returned a bare error, so every
// block came out as 70 — which docs/usage.md §9.13 defines as "back off and retry".
// That is right for exactly one of the two conditions and wrong for the other:
//
//	quorum_lost         transient. The brokers may re-elect on their own, and a retry
//	                    in thirty seconds is genuinely the correct next step. 75.
//	force_single_active persistent. It clears when a human restores redundancy on a
//	                    broker host and not before, so telling automation to retry is
//	                    telling it to spin. 64.
//
// When BOTH are true the persistent one wins: the cluster cannot become healthy by
// waiting while it is running on a single emergency broker, whatever quorum does.
func gateExitClass(gate proto.DestructiveGate) int {
	if gate.ForceSingleActive {
		return exitUsage
	}
	return exitTransient
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
	msg, err := nc.Request(proto.SubjCtrlAlertAck(actor), body, alertAckTimeout)
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
