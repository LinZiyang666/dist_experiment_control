package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
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
	// ctx-none: compatibility helper has no caller context; cli bounds the wire operation.
	ctx := context.Background()
	deadline, err := cli.AdmissionDeadline(ctx)
	if err != nil {
		return nil, err
	}
	return cli.ProbeClusterHealthWithin(ctx, nc, actor, cli.RetryConfig{Deadline: deadline, Family: cli.RetryReadOnly, LogicalID: "compat-health"})
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
		if errors.Is(err, nats.ErrPermissionViolation) || strings.Contains(err.Error(), "Permissions Violation") {
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
	// ctx-none: compatibility banner helper has no caller context.
	return cli.FetchAlertsWithin(context.Background(), nc, actor, alertLsTimeout)
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
	// ctx-none: legacy alert command supplies no caller context; one mutation is sent.
	reply, err := cli.AckAlertWithin(context.Background(), nc, actor, dedupKey, alertAckTimeout)
	if err != nil && strings.HasPrefix(err.Error(), proto.CodeAgentNoResponders+": ") {
		return reply, brokerErrorMessage("alert ack", proto.CodeAgentNoResponders, strings.TrimPrefix(err.Error(), proto.CodeAgentNoResponders+": "))
	}
	return reply, err
}

// Admission callers keep health and banner work on the same deadline as
// CONNECT. The wire helpers live in cli with the shared retry policy.
// origin: hot-update S6 plan §8.2 / T30
func probeClusterHealthWithin(cmd *cobra.Command, nc *nats.Conn, actor string) ([]proto.ClusterHealthResp, error) {
	a, err := ensureCtlAdmission(cmd)
	if err != nil {
		return nil, err
	}
	return cli.ProbeClusterHealthWithin(cmd.Context(), nc, actor, cli.RetryConfig{Deadline: a.deadline, Family: cli.RetryReadOnly, LogicalID: a.logicalID, Observer: a.observer})
}

func gateDestructiveWithin(cmd *cobra.Command, nc *nats.Conn, actor string, ackAlerts bool) error {
	if ackAlerts {
		return nil
	}
	replies, err := probeClusterHealthWithin(cmd, nc, actor)
	if err != nil {
		class := exitUnavailable
		if errors.Is(err, nats.ErrPermissionViolation) || strings.Contains(err.Error(), "Permissions Violation") {
			class = exitNoPerm
		}
		return &ExitError{Class: class, Err: fmt.Errorf("refusing a destructive operation: cluster health could not be checked: %w", err)}
	}
	gate := proto.EvalDestructiveGate(replies)
	if !gate.Blocked() {
		return nil
	}
	return &ExitError{Class: gateExitClass(gate), Err: errors.New(gateBlockMessage(gate))}
}

// A banner is advisory and never expands admission. Skipping it is observed,
// including JSON output, so the collector can account for every phase.
// origin: hot-update S6 plan §8.2 / T30
func withBannerWithin(cmd *cobra.Command, nc *nats.Conn, actor string, jsonMode bool) error {
	a, _ := cmd.Context().Value(ctlAdmissionKey{}).(*ctlAdmission)
	if a == nil {
		return nil
	}
	var attempt controlbudget.Attempt
	if a.observer != nil {
		var err error
		attempt, err = a.observer.Begin(a.logicalID, controlbudget.PhaseBanner)
		if err != nil {
			return err
		}
	}
	attempt.Class = "skipped"
	remaining, remainingErr := controlbudget.Remaining(a.deadline)
	if !jsonMode && remainingErr == nil && remaining >= cli.ControlBannerWindow {
		ctx, cancel, err := ctlAdmissionContext(cmd.Context(), a.deadline)
		if err == nil {
			alerts, fetchErr := cli.FetchAlertsWithin(cli.WithControlObservation(ctx, a.logicalID, a.observer), nc, actor, alertLsTimeout)
			cancel()
			attempt.Class = "error"
			if fetchErr == nil {
				renderBanner(cmd.ErrOrStderr(), alerts, false)
				attempt.Class = "success"
			}
		}
	}
	if a.observer == nil {
		return nil
	}
	now, err := controlbudget.Now()
	attempt.FinishedMonoNS = now
	return errors.Join(err, a.observer.Record(attempt))
}
