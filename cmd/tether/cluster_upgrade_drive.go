package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_upgrade_drive.go — G5 #13/#14 W4: the execute loop. One host at a time, transfer-leader-first,
// signed reload, converge-barrier wait, canary command-version skew check. HALTS (never de-clusters) on
// any refusal; the roll is idempotent-resumable (re-run skips hosts already at target).

func driveUpgrade(cmd *cobra.Command, nc *nats.Conn, actor, sid string, seed []byte, plan clusterupgrade.Plan, toVersion, expectSHA, webhook string) error {
	out := cmd.OutOrStdout()
	ctx := cmd.Context()
	notifyUpgrade(webhook, "start", map[string]any{"to_version": toVersion, "hosts": plan.Upgrades()})
	// External-review B2: take a cluster-scoped roll lock (idempotent UPSERT in replicated cluster_meta) so a
	// concurrent `cluster join`/`cluster retire` cannot cross the rolling restart. The marker is replicated
	// state, so it survives the leadership transfers the roll itself triggers. We release it ONLY on clean
	// completion — a HALT/cancel deliberately LEAVES it held so membership stays blocked while the cluster
	// sits in a partial-roll state; a re-run (forward OR rollback) re-acquires (UPSERT) and eventually
	// releases, so the lock self-heals via the documented "fix and re-run" recovery.
	//
	// R7b: the lock is now LEASED. The paragraph above described a lock that a HALT left held forever —
	// "fix and re-run" is only a self-heal while `plan.Upgrades() > 0`, and back then an agentless host had
	// AtTarget structurally false, so the `plan.Upgrades() == 0` self-heal branch was UNREACHABLE there.
	// (P3 has since fixed the root cause: an agentless host now reaches AtTarget on the broker alone, so
	// that branch is reachable again. The lease remains the general answer — it covers every HALT, not just
	// this one.) A HALT used to fence join/retire permanently. The lease closes that: this process keeps
	// renewing for as long as it is driving, and when it stops (HALT, kill -9, snapped SSH) the reaper
	// on the leader releases the lock
	// within one reap interval after LockLeaseTTL. `tether cluster unlock` is the impatient operator's out.
	{
		leader, lerr := currentLeader(ctx, nc, actor)
		if lerr != nil {
			return haltUpgrade(webhook, "", lerr)
		}
		lresp, lrerr := sendUpgradeTrigger(ctx, nc, actor, seed, &proto.ClusterUpgradeReq{Op: "acquire-lock", TargetNode: leader})
		if lrerr != nil {
			return haltUpgrade(webhook, leader, fmt.Errorf("acquire upgrade lock: %w", lrerr))
		}
		if !lresp.OK {
			return haltUpgrade(webhook, leader, fmt.Errorf("acquire upgrade lock refused: %s %s", lresp.Code, lresp.Error))
		}
	}
	// Renew for the whole roll. defer Stop() covers EVERY exit below — each `return haltUpgrade(...)` is a
	// path on which the operator walks away and the lock must decay on its own.
	keeper := startLockKeeper(ctx, "upgrade", upgradeLeaseRenewInterval,
		func(rctx context.Context) (bool, error) { return renewUpgradeLease(rctx, nc, actor, seed) }, out)
	defer keeper.Stop()

	canaryChecked := false
	for _, s := range plan.Steps {
		if s.Kind != clusterupgrade.StepUpgrade {
			continue
		}
		if s.TransferTo != "" {
			_, _ = fmt.Fprintf(out, "→ transfer leadership off %s to %s\n", s.NodeID, s.TransferTo)
			leader, err := currentLeader(ctx, nc, actor)
			if err != nil {
				return haltUpgrade(webhook, s.NodeID, err)
			}
			resp, err := sendUpgradeTrigger(ctx, nc, actor, seed, &proto.ClusterUpgradeReq{Op: "transfer-leader", TargetNode: leader, TransferTo: s.TransferTo})
			if err != nil {
				return haltUpgrade(webhook, s.NodeID, err)
			}
			if !resp.OK {
				return haltUpgrade(webhook, s.NodeID, fmt.Errorf("transfer-leader refused: %s %s", resp.Code, resp.Error))
			}
			if err := waitLeader(ctx, nc, actor, s.TransferTo); err != nil {
				return haltUpgrade(webhook, s.NodeID, err)
			}
		}
		_, _ = fmt.Fprintf(out, "→ reload %s into %s\n", s.NodeID, toVersion)
		resp, err := sendUpgradeTrigger(ctx, nc, actor, seed, &proto.ClusterUpgradeReq{Op: "reload", TargetNode: s.NodeID, ToVersion: toVersion, ExpectSHA256: expectSHA})
		if err != nil {
			return haltUpgrade(webhook, s.NodeID, err)
		}
		if !resp.OK && !resp.AlreadyAtVersion {
			return haltUpgrade(webhook, s.NodeID, fmt.Errorf("reload refused: %s %s", resp.Code, resp.Error))
		}
		if resp.AlreadyAtVersion {
			_, _ = fmt.Fprintf(out, "  (broker already at %s)\n", toVersion)
		} else if err := waitVersion(ctx, nc, actor, s.NodeID, toVersion); err != nil {
			return haltUpgrade(webhook, s.NodeID, err)
		}
		// Stage-C B1: a WHOLE-HOST upgrade needs the co-located AGENT re-exec'd too (the broker daemon is
		// only half the host). Do this even when the broker was AlreadyAtVersion (its agent may be stale);
		// skip only when the agent is ALREADY at target (idempotent — re-run does not re-restart agents).
		//
		// P3: unless this host HAS no agent. The decision is the PLAN's (Step.BrokerOnly), not re-derived
		// here, so the executor can never disagree with what the operator was shown. Re-deriving it live
		// would also let a mid-roll registration flap silently flip a host between whole-host and
		// broker-only halfway through its own step.
		if s.BrokerOnly {
			_, _ = fmt.Fprintf(out, "  (no co-located agent on %s — broker-only host, nothing else to re-exec)\n", s.NodeID)
		} else if av, ok := agentVersionOf(ctx, nc, actor, sid, s.NodeID); ok && av == toVersion {
			_, _ = fmt.Fprintf(out, "  (agent already at %s)\n", toVersion)
		} else {
			_, _ = fmt.Fprintf(out, "→ re-exec %s's co-located agent into %s\n", s.NodeID, toVersion)
			aresp, aerr := sendUpgradeTrigger(ctx, nc, actor, seed, &proto.ClusterUpgradeReq{Op: "reexec-agent", TargetNode: s.NodeID, SID: sid, ExpectSHA256: expectSHA})
			if aerr != nil {
				return haltUpgrade(webhook, s.NodeID, aerr)
			}
			if !aresp.OK {
				return haltUpgrade(webhook, s.NodeID, fmt.Errorf("agent re-exec refused: %s %s", aresp.Code, aresp.Error))
			}
			if err := waitAgentVersion(ctx, nc, actor, sid, s.NodeID, toVersion); err != nil {
				return haltUpgrade(webhook, s.NodeID, err)
			}
		}
		scope := "broker+agent"
		if s.BrokerOnly {
			scope = "broker-only host"
		}
		_, _ = fmt.Fprintf(out, "  ✓ %s (%s) at %s\n", s.NodeID, scope, toVersion)
		notifyUpgrade(webhook, "host_done", map[string]any{"node": s.NodeID, "version": toVersion})
		if !canaryChecked {
			canaryChecked = true
			if err := canaryCommandVerCheck(ctx, nc, actor, s.NodeID, out); err != nil {
				return haltUpgrade(webhook, s.NodeID, err)
			}
		}
	}
	// R8a P1 — the upgrade verb's rc-semantics gate. Every host now reports the target
	// version, which is a CONTROL-plane fact. A rolling upgrade restarts brokers
	// underneath live reverse tunnels, so it can finish with an expose whose agent never
	// re-confirmed its home — the data plane down while the roll prints success. Ask the
	// LEADER for the agent-confirmed verdict before declaring completion, holding the roll
	// lock throughout (the keeper is still renewing), and refuse rc=0 if it does not come.
	//
	// This mirrors the keeper.Lost() precedent immediately below: "every host reached the
	// target" has never been sufficient for rc=0 in this function.
	if err := waitHomesConverged(ctx, nc, actor, seed, out); err != nil {
		keeper.Stop()
		releaseUpgradeLock(ctx, nc, actor, seed, out)
		notifyUpgrade(webhook, "halt", map[string]any{"to_version": toVersion, "error": err.Error()})
		return err
	}
	// R7b: stop renewing BEFORE releasing. Not for safety (a renewal cannot re-create a released lock — its
	// statements are gated on the marker existing), but so the release is never racing a renewal that would
	// otherwise land microseconds after it and make the log confusing to read.
	keeper.Stop()
	// External-review B2: clean completion → release the cluster-scoped roll lock so membership resumes.
	releaseUpgradeLock(ctx, nc, actor, seed, out)
	// R7b: a roll that lost its lock partway through was NOT serialized against other membership changes,
	// even though every host reached the target. That is not an rc=0 outcome.
	if keeper.Lost() {
		notifyUpgrade(webhook, "halt", map[string]any{"to_version": toVersion, "error": "upgrade lock lost mid-roll"})
		return keeper.LostErr("cluster upgrade rolled every host but LOST its roll lock partway through")
	}
	_, _ = fmt.Fprintln(out, "rolling upgrade complete.")
	notifyUpgrade(webhook, "complete", map[string]any{"to_version": toVersion})
	return nil
}

// upgradeLeaseRenewInterval / growLeaseRenewInterval are vars, not consts, ONLY so hermetic tests can
// compress the 5-minute production cadence into microseconds. Production never reassigns them; both are
// cluster.LockLeaseRenewInterval, whose derivation lives in internal/cluster/lock_lease.go.
var (
	upgradeLeaseRenewInterval = cluster.LockLeaseRenewInterval
	growLeaseRenewInterval    = cluster.LockLeaseRenewInterval
)

// renewUpgradeLease pushes the roll lock's lease forward by one TTL.
//
// The leader is re-resolved on EVERY call, deliberately: `cluster upgrade` moves leadership off each host
// it rolls, so a leader cached at acquire time is wrong for most of the roll, and every renewal after the
// first transfer would fail — letting the lease expire under a live roll, which is the corruption this
// whole mechanism exists to avoid.
func renewUpgradeLease(ctx context.Context, nc *nats.Conn, actor string, seed []byte) (bool, error) {
	leader, err := currentLeader(ctx, nc, actor)
	if err != nil {
		return true, fmt.Errorf("resolve leader: %w", err) // transient: still ours as far as we know
	}
	resp, err := sendUpgradeTrigger(ctx, nc, actor, seed, &proto.ClusterUpgradeReq{Op: "renew-lock", TargetNode: leader})
	switch {
	case err != nil:
		return true, err
	case resp == nil:
		return true, fmt.Errorf("no reply from leader %s", leader)
	case resp.OK:
		return true, nil
	case resp.Code == clusterLockNotHeldCode:
		return false, nil // TERMINAL
	default:
		return true, fmt.Errorf("%s %s", resp.Code, resp.Error)
	}
}

// releaseUpgradeLock best-effort clears the B2 cluster-scoped roll lock via the current leader. Failure is
// non-fatal: the marker is an idempotent UPSERT, so a subsequent `cluster upgrade` re-run (forward or
// rollback) re-acquires and releases it — a stuck lock only ever blocks membership, never the roll itself.
func releaseUpgradeLock(ctx context.Context, nc *nats.Conn, actor string, seed []byte, out io.Writer) {
	leader, err := currentLeader(ctx, nc, actor)
	if err != nil {
		_, _ = fmt.Fprintf(out, "  ⚠ WARNING: could not resolve the leader to release the upgrade lock: %v — cluster_upgrade_active is still set, so `cluster join`/`cluster retire` stay BLOCKED. Re-run `tether cluster upgrade` with the same --to-version AND --account-seed to clear it.\n", err)
		return
	}
	// round2 B1: release-lock now surfaces a Propose failure (no false OK), so a non-OK reply is a REAL
	// unreleased lock. Warn loudly and point at the reachable self-heal (re-running the roll clears it).
	if resp, err := sendUpgradeTrigger(ctx, nc, actor, seed, &proto.ClusterUpgradeReq{Op: "release-lock", TargetNode: leader}); err != nil || resp == nil || !resp.OK {
		detail := "no reply"
		if resp != nil {
			detail = resp.Code + " " + resp.Error
		}
		if err != nil {
			detail = err.Error()
		}
		_, _ = fmt.Fprintf(out, "  ⚠ WARNING: upgrade lock release did NOT confirm (leader=%s: %s) — cluster_upgrade_active is still set, so `cluster join`/`cluster retire` stay BLOCKED. Re-run `tether cluster upgrade` with the same --to-version AND --account-seed to clear it.\n", leader, detail)
	}
}

func haltUpgrade(webhook, node string, err error) error {
	notifyUpgrade(webhook, "halt", map[string]any{"node": node, "error": err.Error()})
	return unavailErr("cluster upgrade HALTED at %s: %v (the cluster is left in a safe partial state; fix and re-run to resume)", node, err)
}

// currentLeader re-resolves the writable leader from a fresh health probe.
func currentLeader(ctx context.Context, nc *nats.Conn, actor string) (string, error) {
	for _, h := range probeClusterHealth(nc, actor) {
		if h.WritableLeaderConfirmed && h.NodeID != "" {
			return h.NodeID, nil
		}
	}
	return "", unavailErr("no writable leader visible (election in progress?) — retry")
}

// waitLeader polls until the named node is the writable leader (or times out).
func waitLeader(ctx context.Context, nc *nats.Conn, actor, target string) error {
	return pollUntil(ctx, upgradeConvergeTimeout, func() bool {
		l, err := currentLeader(ctx, nc, actor)
		return err == nil && l == target
	}, fmt.Sprintf("leadership did not move to %s within %s", target, upgradeConvergeTimeout))
}

// waitVersion is the converge barrier: it polls until the named broker re-registers at the target version
// AND has caught up (Stage-C M1 — its command-domain AppliedIndex within a small lag of the writable
// leader's). Version alone is NOT "done": a still-catching-up voter counted done would dip quorum when the
// NEXT voter restarts. If no writable leader is visible yet (mid-election), it keeps waiting.
func waitVersion(ctx context.Context, nc *nats.Conn, actor, node, version string) error {
	const maxAppliedLag = 64 // mirror observeLagThreshold
	return pollUntil(ctx, upgradeConvergeTimeout, func() bool {
		var leaderApplied, nodeApplied uint64
		var nodeVer string
		var haveLeader, haveNode, nodeVoter, nodeStale bool
		for _, h := range probeClusterHealth(nc, actor) {
			if h.WritableLeaderConfirmed && h.AppliedIndex >= leaderApplied {
				leaderApplied, haveLeader = h.AppliedIndex, true
			}
			if h.NodeID == node {
				nodeApplied, nodeVer, haveNode = h.AppliedIndex, h.ReleaseVersion, true
				nodeVoter, nodeStale = h.IsVoter, h.LeaderContactStale
			}
		}
		// External-review M3: version alone is NOT "done". The rolled host must be back at target AND still
		// a configured VOTER AND not fenced from the leader AND caught up (applied-lag small) — else counting
		// it done would dip quorum when the NEXT voter restarts. (Stream-replica readiness is not in the
		// health reply; it is a documented follow-up — the applied-lag + voter + reachable gate is the
		// achievable-over-NATS subset of the plan's barrier.)
		if !haveNode || nodeVer != version || !nodeVoter || nodeStale {
			return false
		}
		return haveLeader && leaderApplied <= nodeApplied+maxAppliedLag
	}, fmt.Sprintf("%s did not re-register at %s + catch up (voter, reachable, applied) within %s", node, version, upgradeConvergeTimeout))
}

// agentVersionOf resolves brokerNode's co-located agent's running release: the health probe gives the
// broker's self-declared ColocatedAgentNID (falling back to node_id==nid), and the node list gives that
// agent's ReleaseVersion. Returns ("", false) if the agent is not (yet) visible.
func agentVersionOf(ctx context.Context, nc *nats.Conn, actor, sid, brokerNode string) (string, bool) {
	agentNID := brokerNode
	for _, h := range probeClusterHealth(nc, actor) {
		if h.NodeID == brokerNode && h.ColocatedAgentNID != "" {
			agentNID = h.ColocatedAgentNID
			break
		}
	}
	body, _ := json.Marshal(proto.NodeListReq{})
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	msg, err := nc.RequestWithContext(rctx, proto.SubjCtrlNodeList(actor, sid), body)
	if err != nil {
		return "", false
	}
	var resp proto.NodeListResp
	if json.Unmarshal(msg.Data, &resp) != nil {
		return "", false
	}
	for _, n := range resp.Nodes {
		if n.NID == agentNID {
			return n.ReleaseVersion, true
		}
	}
	return "", false
}

// waitAgentVersion polls until brokerNode's co-located agent re-registers at the target version.
func waitAgentVersion(ctx context.Context, nc *nats.Conn, actor, sid, brokerNode, version string) error {
	return pollUntil(ctx, upgradeConvergeTimeout, func() bool {
		v, ok := agentVersionOf(ctx, nc, actor, sid, brokerNode)
		return ok && v == version
	}, fmt.Sprintf("%s's co-located agent did not re-register at %s within %s", brokerNode, version, upgradeConvergeTimeout))
}

// canaryCommandVerCheck HALTS the roll if the just-upgraded canary reports a DIFFERENT raft command
// version than an un-upgraded voter — a commandVersion bump POISONs the log per-replica, so it must be a
// flag-day reinstall, never a rolling upgrade.
func canaryCommandVerCheck(ctx context.Context, nc *nats.Conn, actor, canary string, out io.Writer) error {
	replies := probeClusterHealth(nc, actor)
	var canaryCmd int
	found := false
	for _, h := range replies {
		if h.NodeID == canary {
			canaryCmd, found = h.CommandVer, true
		}
	}
	if !found {
		return nil // canary not visible this instant — the converge wait already gated version; don't false-halt
	}
	unverifiable := false
	for _, h := range replies {
		if h.NodeID == canary {
			continue
		}
		if h.CommandVer == 0 {
			unverifiable = true // a pre-G5 broker that predates the command-ver self-report — cannot compare
			continue
		}
		if h.CommandVer != canaryCmd {
			return fmt.Errorf("command-version skew after the canary (%s=%d vs %s=%d) — this release changes the raft command encoding and CANNOT be rolled; reinstall the whole cluster (architecture J.3)", canary, canaryCmd, h.NodeID, h.CommandVer)
		}
	}
	// Stage-C M3: don't silently FAIL-OPEN. If un-upgraded brokers predate the command-ver self-report we
	// cannot verify the axis — WARN loudly (the operator must confirm the release does not bump it) rather
	// than either mis-HALT a safe roll or pass in silence.
	if unverifiable && canaryCmd != 0 {
		_, _ = fmt.Fprintf(out, "  ⚠ command-version axis UNVERIFIABLE: some un-upgraded brokers predate the command-ver self-report (report 0). If this release BUMPS the raft command encoding, rolling it would silently fork the FSM — confirm the release notes say it does NOT before proceeding.\n")
	}
	return nil
}

// pollUntil re-checks pred every upgradeConvergePoll until it holds or the deadline passes.
// upgradeHomesConvergedOp mirrors internal/broker's upgradeOpHomesConverged. ClusterUpgradeReq.Op
// is a free-form string, so the two literals have no compile-time link — renaming one side would
// make this gate silently answer "unknown op" forever, i.e. turn the assertion off without any
// build/test/lint failure. TestUpgradeHomesConvergedOpIsWireStable pins the pair.
const upgradeHomesConvergedOp = "homes-converged"

// homesConvergeTimeout bounds the roll-tail data-plane wait. The broker re-delivers home
// directives on its own reconcile cadence, so this is "how long the operator's command is willing
// to stand and watch", not a correctness bound: on timeout the roll exits NONZERO and re-running
// (or `cluster status --homes`) picks the convergence back up.
const homesConvergeTimeout = 2 * time.Minute

// waitHomesConverged asks the CURRENT leader whether every homed expose's agent has re-confirmed
// its home, retrying while the answer is "not converged" or "not leader" (a leadership change
// mid-roll is routine — the roll transfers leadership on purpose).
func waitHomesConverged(ctx context.Context, nc *nats.Conn, actor string, seed []byte, out io.Writer) error {
	_, _ = fmt.Fprintln(out, "→ verifying the data plane re-converged onto its homes")
	deadline := time.Now().Add(homesConvergeTimeout)
	var last string
	for {
		leader, lerr := currentLeader(ctx, nc, actor)
		if lerr == nil && leader != "" {
			resp, err := sendUpgradeTriggerOnce(ctx, nc, actor, seed, &proto.ClusterUpgradeReq{
				Op: upgradeHomesConvergedOp, TargetNode: leader,
			})
			switch {
			case err != nil:
				last = err.Error()
			case resp.OK:
				_, _ = fmt.Fprintln(out, "  ✓ every homed expose confirmed by its agent")
				return nil
			default:
				last = resp.Code + ": " + resp.Error
			}
		} else if lerr != nil {
			last = lerr.Error()
		}
		if time.Now().After(deadline) {
			// EX_TEMPFAIL, not EX_SOFTWARE: the brokers keep re-delivering, so "run it
			// again" is the correct operator response — but it is emphatically not rc=0.
			return &ExitError{Class: exitTransient, Err: fmt.Errorf(
				"cluster upgrade: every host reached the target version but the DATA PLANE has not re-converged onto its homes within %s (%s)"+
					" — the roll is NOT complete; re-run, or inspect `tether cluster status --homes`", homesConvergeTimeout, last)}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(upgradeConvergePoll):
		}
	}
}

// notifyUpgrade POSTs a best-effort JSON milestone to the webhook (no-op if empty). Failures are ignored
// — the roll's correctness never depends on the webhook.
func notifyUpgrade(webhook, event string, fields map[string]any) {
	notifyMilestone(webhook, "cluster_upgrade_"+event, fields)
}

// notifyMilestone POSTs a best-effort JSON milestone {"event": fullEvent, ...fields} to the webhook (no-op if
// empty). Failures are ignored — orchestration correctness never depends on the webhook. Shared by the
// upgrade and grow orchestrators.
func notifyMilestone(webhook, fullEvent string, fields map[string]any) {
	if webhook == "" {
		return
	}
	body := map[string]any{"event": fullEvent}
	for k, v := range fields {
		body[k] = v
	}
	b, _ := json.Marshal(body)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, webhook, bytes.NewReader(b))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_ = resp.Body.Close()
	}
}
