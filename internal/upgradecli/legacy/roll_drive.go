package legacy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
)

// The legacy (S6) `cluster upgrade` executor helpers moved verbatim from
// cmd/tether/cluster_upgrade_drive.go with their comments (hot-update S8 plan
// §11.2 P19 slim move). cmd keeps wrappers under the original names for its
// other verbs (cluster add/unlock) and their tests; signing, exit classes and
// the health probe arrive through RollIO.

// RollIO is the executor's view of the control plane. Trigger retries the
// retriable cluster_not_ready answer; TriggerOnce sends exactly once.
type RollIO struct {
	Trigger, TriggerOnce func(context.Context, *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error)
	Health               func(context.Context) ([]proto.ClusterHealthResp, error)
	Probe                func() ([]proto.ClusterHealthResp, error)
	Request              RequestFunc
	Unavailable          func(format string, a ...any) error
	Transient            func(error) error
	LockNotHeldCode      string
	Poll                 time.Duration
}

// HomesConvergedOp mirrors internal/broker's upgradeOpHomesConverged. ClusterUpgradeReq.Op
// is a free-form string, so the two literals have no compile-time link — renaming one side would
// make this gate silently answer "unknown op" forever, i.e. turn the assertion off without any
// build/test/lint failure. TestUpgradeHomesConvergedOpIsWireStable pins the pair (through cmd's
// upgradeHomesConvergedOp alias).
const HomesConvergedOp = "homes-converged"

// homesConvergeTimeout bounds the roll-tail data-plane wait. The broker re-delivers home
// directives on its own reconcile cadence, so this is "how long the operator's command is willing
// to stand and watch", not a correctness bound: on timeout the roll exits NONZERO and re-running
// (or `cluster status --homes`) picks the convergence back up.
const homesConvergeTimeout = 2 * time.Minute

// RollHolder preserves old callers' wire bytes; the S6 driver always passes
// its acquired holder through renewal, release and halted-lease handback.
// origin: hot-update S6 plan §7.1 / T40
func RollHolder(holders []string) string {
	if len(holders) > 0 {
		return holders[0]
	}
	return ""
}

// RollLeader (cmd: currentLeader) re-resolves the writable leader from a fresh health probe.
// origin: hot-update S6 plan §5 / T38
func RollLeader(ctx context.Context, rio RollIO) (string, error) {
	health, err := rio.Health(ctx)
	if err != nil {
		return "", err
	}
	leader := ""
	for _, h := range health {
		if h.WritableLeaderConfirmed && h.NodeID != "" {
			if leader != "" {
				return "", rio.Unavailable("ambiguous writable leader snapshot")
			}
			leader = h.NodeID
		}
	}
	if leader != "" {
		return leader, nil
	}
	return "", rio.Unavailable("no writable leader visible (election in progress?) — retry")
}

// RenewRollLease (cmd: renewUpgradeLease) pushes the roll lock's lease forward by one TTL.
//
// The leader is re-resolved on EVERY call, deliberately: `cluster upgrade` moves leadership off each host
// it rolls, so a leader cached at acquire time is wrong for most of the roll, and every renewal after the
// first transfer would fail — letting the lease expire under a live roll, which is the corruption this
// whole mechanism exists to avoid.
func RenewRollLease(ctx context.Context, rio RollIO, holders ...string) (bool, error) {
	leader, err := RollLeader(ctx, rio)
	if err != nil {
		return true, fmt.Errorf("resolve leader: %w", err) // transient: still ours as far as we know
	}
	resp, err := rio.Trigger(ctx, &proto.ClusterUpgradeReq{Op: "renew-lock", TargetNode: leader, Holder: RollHolder(holders)})
	switch {
	case err != nil:
		return true, err
	case resp == nil:
		return true, fmt.Errorf("no reply from leader %s", leader)
	case resp.OK:
		return true, nil
	case resp.Code == rio.LockNotHeldCode:
		return false, nil // TERMINAL
	default:
		return true, fmt.Errorf("%s %s", resp.Code, resp.Error)
	}
}

// ReleaseRollLock (cmd: releaseUpgradeLock) clears the B2 cluster-scoped roll lock via the current leader.
// Older releases treated this as best effort: a stuck lock blocks membership and
// the no-op re-run can clear a legacy marker. A holder-protected S6 marker cannot
// be cleared by a fresh orchestrator, so a failed release must fail completion.
// origin: hot-update S6 plan §7.1 / T40
func ReleaseRollLock(ctx context.Context, rio RollIO, out io.Writer, holders ...string) error {
	leader, err := RollLeader(ctx, rio)
	if err != nil {
		_, _ = fmt.Fprintf(out, "  ⚠ WARNING: could not resolve the leader to release the upgrade lock: %v — cluster_upgrade_active is still set, so `cluster join`/`cluster retire` stay BLOCKED. Re-run `tether cluster upgrade` with the same --to-version AND --account-seed to clear it.\n", err)
		return err
	}
	// round2 B1: release-lock now surfaces a Propose failure (no false OK), so a non-OK reply is a REAL
	// unreleased lock. Warn loudly and point at the reachable self-heal (re-running the roll clears it).
	if resp, err := rio.Trigger(ctx, &proto.ClusterUpgradeReq{Op: "release-lock", TargetNode: leader, Holder: RollHolder(holders)}); err != nil || resp == nil || !resp.OK {
		detail := "no reply"
		if resp != nil {
			detail = resp.Code + " " + resp.Error
		}
		if err != nil {
			detail = err.Error()
		}
		_, _ = fmt.Fprintf(out, "  ⚠ WARNING: upgrade lock release did NOT confirm (leader=%s: %s) — cluster_upgrade_active is still set, so `cluster join`/`cluster retire` stay BLOCKED. Re-run `tether cluster upgrade` with the same --to-version AND --account-seed to clear it.\n", leader, detail)
		return rio.Unavailable("upgrade lock release did not confirm: %s", detail)
	}
	return nil
}

// ExpireRollLease (cmd: expireUpgradeLockLease) tells the leader this orchestrator has stopped driving,
// WITHOUT releasing the lock. origin: prerelease audit round 2, H-2.
//
// Best-effort by construction and silent on the happy path: the only thing it
// buys is not waiting out LockLeaseTTL, and every failure mode — no leader, an
// older broker that does not know the op — degrades to exactly the pre-H-2
// behaviour. It is deliberately NOT reported as a warning: an operator reading a
// HALT does not need a second scary line about an optimisation that did not fire.
func ExpireRollLease(ctx context.Context, rio RollIO, out io.Writer, holders ...string) {
	leader, err := RollLeader(ctx, rio)
	if err != nil {
		return
	}
	resp, err := rio.Trigger(ctx,
		&proto.ClusterUpgradeReq{Op: "expire-lock-lease", TargetNode: leader, Holder: RollHolder(holders)})
	if err != nil || resp == nil || !resp.OK {
		return
	}
	_, _ = fmt.Fprintf(out, "  upgrade lock lease handed back — a re-run (or a rollback) can start immediately; "+
		"`cluster join`/`cluster retire` stay fenced until a roll completes.\n")
}

// ColocatedAgentVersion (cmd: agentVersionOf) resolves brokerNode's co-located agent's running release: the health probe gives the
// broker's self-declared ColocatedAgentNID (falling back to node_id==nid), and the node list gives that
// agent's ReleaseVersion. Returns ("", false) if the agent is not (yet) visible.
// S8: this is an observation for convergence waits only; a release string is
// never a reason to skip the agent leg (§4.2 / IL14).
func ColocatedAgentVersion(ctx context.Context, rio RollIO, actor, sid, brokerNode string) (string, bool) {
	agentNID := brokerNode
	if health, err := rio.Probe(); err == nil {
		for _, h := range health {
			if h.NodeID == brokerNode && h.ColocatedAgentNID != "" {
				agentNID = h.ColocatedAgentNID
				break
			}
		}
	}
	entry, err := LookupNodeEntry(ctx, rio.Request, sid, actor, agentNID)
	if err != nil {
		return "", false
	}
	return entry.ReleaseVersion, true
}

// CanaryCommandVersion (cmd: canaryCommandVerCheck) HALTS the roll if the just-upgraded canary reports a DIFFERENT raft command
// version than an un-upgraded voter — a commandVersion bump POISONs the log per-replica, so it must be a
// flag-day reinstall, never a rolling upgrade.
func CanaryCommandVersion(rio RollIO, canary string, out io.Writer) error {
	replies, err := rio.Probe()
	if err != nil {
		// HALT rather than "don't false-halt". The `return nil` below is justified by
		// "the canary is not visible this instant, and the converge wait already gated
		// version" — reasoning that assumes the probe WORKED and simply found nothing.
		// A probe that could not be made supports no such conclusion, and what this
		// function guards is a command-version mismatch that poisons the raft log
		// per-replica. origin: increment 2 internal review, pairing-sweep/IMPACT-F1.
		return fmt.Errorf("canary command-version check: could not ask the cluster: %w", err)
	}
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

// WaitHomesConverged asks the CURRENT leader whether every homed expose's agent has re-confirmed
// its home, retrying while the answer is "not converged" or "not leader" (a leadership change
// mid-roll is routine — the roll transfers leadership on purpose).
func WaitHomesConverged(ctx context.Context, rio RollIO, out io.Writer) error {
	_, _ = fmt.Fprintln(out, "→ verifying the data plane re-converged onto its homes")
	deadline := time.Now().Add(homesConvergeTimeout)
	var last string
	for {
		leader, lerr := RollLeader(ctx, rio)
		if lerr == nil && leader != "" {
			resp, err := rio.TriggerOnce(ctx, &proto.ClusterUpgradeReq{
				Op: HomesConvergedOp, TargetNode: leader,
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
			return rio.Transient(fmt.Errorf(
				"cluster upgrade: every host reached the target version but the DATA PLANE has not re-converged onto its homes within %s (%s)"+
					" — the roll is NOT complete; re-run, or inspect `tether cluster status --homes`", homesConvergeTimeout, last))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(rio.Poll):
		}
	}
}

// NotifyMilestone (cmd: notifyMilestone) POSTs a best-effort JSON milestone {"event": fullEvent, ...fields} to the webhook (no-op if
// empty). Failures are ignored — orchestration correctness never depends on the webhook. Shared by the
// upgrade and grow orchestrators.
func NotifyMilestone(webhook, fullEvent string, fields map[string]any) {
	if webhook == "" {
		return
	}
	body := map[string]any{"event": fullEvent}
	for k, v := range fields {
		body[k] = v
	}
	b, _ := json.Marshal(body)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second) // ctx-none: milestone helper has no caller context
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
