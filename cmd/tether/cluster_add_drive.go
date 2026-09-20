package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/serveconf"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_add_drive.go (G4 §B) — the P0–P9 grow sequencer. It is a resumable, HALT-on-refusal state machine:
// each phase checks its own postcondition and skips if satisfied, so a crash/re-run converges by re-observing
// state; it never double-applies a raft change (all membership flows through the leader's OpKindJoin ladder)
// and never de-clusters (a HALT leaves a safe partial state; re-run to resume). The local init/prepare steps
// shell out to THIS binary (reusing the exact tested code paths, as an operator does by hand); the cross-host
// steps go through the account-signed grow trigger. The joiner's cold daemon start is provisioning's job, so
// the sequencer HALTs at that boundary with the exact command. End-to-end ordering is validated by the
// deploy-tier simcluster grow drill (docs/reviews/g4-plan.md §9).

func driveAdd(cmd *cobra.Command, nc *nats.Conn, actor, sid string, accountSeed []byte, seedPath string, jp joinerParams, socketPath string, dryRun bool, timeout time.Duration, webhook string) error {
	out := cmd.OutOrStdout()
	ctx := cmd.Context()

	// A5: a --dry-run promises "touching nothing", yet the P0 preflight runs BEFORE the dry-run short-circuit,
	// so a preflight failure would fire a real "halt" webhook POST. Suppress the webhook for the whole dry-run
	// (the read-only preflight still runs so the plan can be printed; the only reachable notify in dry-run is
	// the two preflight halts below).
	if dryRun {
		webhook = ""
	}

	// P0 PREFLIGHT: resolve the writable leader + the joiner's SAN. Read-only; always safe to re-run.
	leader, lerr := currentLeader(ctx, nc, actor)
	if lerr != nil {
		return haltAdd(webhook, "preflight", jp.Joiner, lerr)
	}
	if leader == jp.Joiner {
		return usageErr("cluster add: %s is already the leader — it cannot join itself", jp.Joiner)
	}
	// #24 fail-fast: a CN-only route cert mounts raft but silently breaks the nats route mesh.
	if serr := routeCertSANPreflight(jp.SecretsDir, jp.NatsRoute); serr != nil {
		return haltAdd(webhook, "preflight", jp.Joiner, serr)
	}
	votersBefore, verr := countVoters(nc, actor)
	if verr != nil {
		return haltAdd(webhook, "preflight", jp.Joiner, verr)
	}
	_, _ = fmt.Fprintf(out, "cluster add %s → cluster (leader=%s, voters=%d)\n", jp.Joiner, leader, votersBefore)

	if dryRun {
		_, _ = fmt.Fprintf(out, "  plan: acquire-lock → init(local) → approve-join → %s → start joiner (provisioning) → catch-up → rebalance\n",
			formerN1Note(votersBefore))
		_, _ = fmt.Fprintln(out, "(dry-run — nothing was touched)")
		return nil
	}
	if seedPath == "" {
		return usageErr("--account-seed <path> is required to execute (the cluster account seed signs each broker trigger)")
	}
	notifyGrow(webhook, "start", map[string]any{"joiner": jp.Joiner, "leader": leader})

	// B1: VOTER is necessary but NOT sufficient for a completed grow: the join operation promotes raft
	// membership before NATS_ROLLED_OUT reaches terminal SERVING. Check for a live op first, otherwise this
	// shortcut false-greens the exact topology stall and releases the grow lock under an active controller.
	// Only a VOTER with no live join op is the crash-post-SERVING idempotent resume (§4 P9).
	if joinerIsVoter(nc, actor, jp.Joiner) {
		liveOp, err := findJoinOp(ctx, nc, actor, accountSeed, leader, jp.Joiner)
		if err != nil {
			return haltAdd(webhook, "find-join-op", jp.Joiner, err)
		}
		if liveOp == "" {
			_, _ = fmt.Fprintf(out, "  %s is already a VOTER with no live join op — grow complete\n", jp.Joiner)
			// R7 (#31): a failed release leaves membership fenced — that is NOT a rc=0 outcome.
			if rerr := releaseGrowLock(ctx, nc, actor, accountSeed, jp.Joiner, out); rerr != nil {
				notifyGrow(webhook, "halt", map[string]any{"phase": "release-lock", "joiner": jp.Joiner, "error": rerr.Error()})
				return rerr
			}
			notifyGrow(webhook, "complete", map[string]any{"joiner": jp.Joiner, "already": true})
			return nil
		}
		_, _ = fmt.Fprintf(out, "  %s is a VOTER but join op %s is still active — resuming to terminal SERVING\n", jp.Joiner, liveOp)
	}

	// P1 ACQUIRE LOCK (before the join op exists, so no window where a concurrent grow slips in).
	if resp, err := sendGrowTrigger(ctx, nc, actor, accountSeed, &proto.ClusterGrowReq{Op: "acquire-lock", TargetNode: leader, JoinerNode: jp.Joiner}); err != nil {
		return haltAdd(webhook, "acquire-lock", jp.Joiner, err)
	} else if !resp.OK {
		return haltAdd(webhook, "acquire-lock", jp.Joiner, fmt.Errorf("%s %s", resp.Code, resp.Error))
	}

	// R7b (#31): the ACQUIRE-LOCK WINDOW. From here until P4 creates the join operation row there is no
	// operation for the broker-side reconciler to read, so a HALT at P2 (init) or P3 (verify-seam) used to
	// leave cluster_grow_active set with literally nothing in the cluster that could ever judge it — R7a's
	// identifying pass is blind here by construction. The lease is what makes this window judgeable: while
	// this process lives it keeps saying so, and when it stops the marker decays on its own.
	//
	// The renewal is JOINER-BOUND, so it can only ever extend THIS grow's lock, never another's.
	//
	// defer Stop() covers every HALT return below — those are precisely the abandonment paths.
	keeper := startLockKeeper(ctx, "grow", growLeaseRenewInterval,
		func(rctx context.Context) (bool, error) {
			return renewGrowLease(rctx, nc, actor, accountSeed, jp.Joiner)
		}, out)
	defer keeper.Stop()

	// P2 LOCAL OFFLINE INIT: bootstrap raft/ + apply the broker.yaml seam. Skip if raft/ already present.
	//
	// initRanThisInvocation is the ONE fact the start-joiner boundary (P3b) is allowed to shorten its
	// grace on — see joinerStartGrace. It is set only by a successful runSelfInit in THIS process, never
	// inferred from "raft/ is present now" (that is true on every resume too).
	initRanThisInvocation := false
	if !dirExists(jp.DataDir + "/raft") {
		_, _ = fmt.Fprintf(out, "→ init %s (bootstrap raft/ locally)\n", jp.Joiner)
		if err := runSelfInit(cmd, jp); err != nil {
			return haltAdd(webhook, "init", jp.Joiner, err)
		}
		initRanThisInvocation = true
	} else {
		_, _ = fmt.Fprintf(out, "  (raft/ present — init already done)\n")
	}

	// External review B1: FAIL-CLOSED on the broker.cluster seam. `cluster init` applies it best-effort — it runs
	// as the tether user, so on a ROOT-OWNED /etc/tether/broker.yaml (G1) the write fails and init only PRINTS a
	// note; its exit code alone never proves the joiner will boot in CLUSTER mode. Verify the seam actually
	// decodes into broker.cluster; if it does not, HALT HERE with the exact operator step. Proceeding would
	// approve-join + cutover the former-N1 and then wait forever on a joiner that boots SINGLE mode (whose local
	// admin socket B1 also used to misjudge as clustered). Setting the seam on a root-owned config is
	// provisioning's job (the sim provisions it as root before `cluster add`).
	if jp.ConfigPath != "" {
		if err := verifyClusterSeam(jp.ConfigPath, jp.RaftAddr, jp.DataDir, jp.SecretsDir, defaultNatsConfPath); err != nil {
			return haltAdd(webhook, "verify-seam", jp.Joiner, err)
		}
	}

	// P4 APPROVE-JOIN: prepare a bundle locally, then approve NON-BLOCKING on the leader (#8). If a join op
	// already exists for this joiner (a resume), skip prepare (a fresh nonce → a different op → refused).
	opID, foErr := findJoinOp(ctx, nc, actor, accountSeed, leader, jp.Joiner)
	if foErr != nil {
		return haltAdd(webhook, "find-join-op", jp.Joiner, foErr)
	}
	if opID == "" {
		bundle, berr := runSelfJoinPrepare(cmd, jp)
		if berr != nil {
			return haltAdd(webhook, "prepare", jp.Joiner, berr)
		}
		_, _ = fmt.Fprintf(out, "→ approve join on %s (non-blocking)\n", leader)
		resp, err := sendGrowTrigger(ctx, nc, actor, accountSeed, &proto.ClusterGrowReq{Op: "approve-join", TargetNode: leader, JoinerNode: jp.Joiner, JoinBundle: bundle})
		if err != nil {
			return haltAdd(webhook, "approve-join", jp.Joiner, err)
		}
		if !resp.OK {
			return haltAdd(webhook, "approve-join", jp.Joiner, fmt.Errorf("%s %s", resp.Code, resp.Error))
		}
		opID = resp.OpID
		_, _ = fmt.Fprintf(out, "  ✓ staged as nonvoter (op %s)\n", opID)
	} else {
		_, _ = fmt.Fprintf(out, "  (join op %s already in flight — resuming)\n", opID)
	}

	// The join op must be past AddNonvoter (RAFT_ADDING committed → CATCHING_UP) before we prep the mesh — the
	// leader's cluster_nodes must carry the joiner + the cutover's R3 gate requires a committed >=2-server config.
	if err := waitOpCatchingUp(ctx, nc, actor, accountSeed, leader, opID, out); err != nil {
		return haltAdd(webhook, "await-nonvoter", jp.Joiner, err)
	}

	// P5 PREP THE MESH — done BEFORE the joiner's daemons start, because a fresh joiner CANNOT boot cluster-mode
	// with a standalone nats.conf (its broker connects with cluster nkeys the standalone conf does not carry,
	// and a clustered-alone JS meta cannot form). Both brokers must be clustered when the joiner boots:
	//   (a) render the JOINER's own clustered nats.conf from the leader's mesh-peer triples (local, idempotent);
	//   (b) cut the FORMER-N1 over to clustered (over the signed NATS trigger). Skip (a) once the joiner is up.
	if !joinerBrokerUpLocal(socketPath) {
		_, _ = fmt.Fprintf(out, "→ render %s clustered nats.conf (from leader mesh peers)\n", jp.Joiner)
		if err := renderJoinerClusteredConf(cmd, nc, actor, accountSeed, leader, jp, out); err != nil {
			return haltAdd(webhook, "render-joiner-conf", jp.Joiner, err)
		}
		// R16 A1: reset the JOINER's OWN stale JetStream store BEFORE it boots clustered. A RETURNING node
		// (rejoin-prepare wiped raft/+tether.db but NOT the JS store) carries a dead-epoch clustered JS meta;
		// booting the freshly-rendered clustered conf onto it wedges the 1->2 JS-meta formation and the joiner
		// crash-loops on the lone-clustered fatal (n1ClusteredJetStreamFatal), stalling the op at CATCHING_UP
		// (drill 42 #GROW-ONTO-FORCE-SINGLE). A FRESH joiner's store is empty → no-op (drills 10/11). Non-empty
		// (data-bearing) → operator-gated move-aside (never delete). Backup-dir-first idempotent (keyed on opID)
		// so a resume after the move never double-moves the fresh clustered store.
		if err := resetJoinerJSStore(out, jp, opID); err != nil {
			return haltAdd(webhook, "reset-joiner-js", jp.Joiner, err)
		}
	}
	if votersBefore == 1 {
		if former := formerSoleVoter(nc, actor, jp.Joiner); former != "" {
			_, _ = fmt.Fprintf(out, "→ former-N1 cutover on %s (standalone→clustered + JS reset)\n", former)
			if err := cutoverBroker(ctx, nc, actor, accountSeed, former, jp.Joiner, opID, jp.ResetFormerJS, jp.PreserveJSData, out); err != nil {
				return haltAdd(webhook, "mesh-cutover", jp.Joiner, err)
			}
		}
	}

	// P3b START-JOINER BOUNDARY: the joiner's cold daemon start is provisioning's job (tether never runs
	// systemctl). Liveness is on the joiner's LOCAL admin socket (we run ON the joiner; its nats health is
	// unreachable over NATS until the mesh forms). HALT with the resume hint if not up yet — its conf is now
	// clustered, so provisioning's start brings it up meshed.
	//
	// The grace is joinerStartGrace(initRanThisInvocation, natsState): a fresh joiner whose raft/ this very
	// process just created cannot have a cluster-mode broker to wait for, so it HALTs at once instead of
	// sitting out joinerBootGrace; so does a returning node whose nats-server still runs the standalone
	// conf (provisioning has not done its half yet — gotcha #83); a resume with nats already clustered
	// keeps the full window (that window is why drill 42's returning node stopped failing 3 runs in 4).
	natsState, procState := natsLocalUnknown, brokerProcUnknown
	if !initRanThisInvocation && !joinerBrokerUpLocal(socketPath) {
		natsState = joinerNatsState(joinerNatsURL(jp.ConfigPath), 2*time.Second)
		// 3 samples 2 s apart: longer than one RestartSec gap, far shorter than the grace it decides on.
		procState = joinerBrokerProcess(3, 2*time.Second)
	}
	grace := joinerStartGrace(initRanThisInvocation, natsState, procState)
	if !awaitJoinerBrokerUpLocal(ctx, socketPath, jp.Joiner, grace, out) {
		if !initRanThisInvocation && grace == 0 {
			_, _ = fmt.Fprintf(out, "  (%s: nats-server %s, tether serve process %s — nothing can come up until provisioning restarts the daemons; not waiting)\n", jp.Joiner, natsState, procState)
		}
		// Two different pauses. The daemons are NOT running (fresh joiner, or provisioning has not done
		// its half): tell the operator to start them. The daemons ARE running and the broker still did
		// not serve within the grace (simcluster-speed G1, drill 91 at 8 concurrent grows: the joiner's
		// clustered JetStream meta group took longer than 2 min to form under load): telling the operator
		// to `systemctl restart nats-server` there is worse than useless — it aborts the very boot being
		// waited for. That pause names the state and asks only for a re-run.
		booting := joinerIsBooting(initRanThisInvocation, grace, natsState, procState)
		notifyGrow(webhook, "paused_start_joiner", map[string]any{"joiner": jp.Joiner, "daemons_running": booting})
		if booting {
			_, _ = fmt.Fprintf(out, "  (%s: nats-server %s, tether serve process %s — the broker is running but did not serve cluster status within %s; it is most likely still forming its clustered JetStream meta group, which is slow on a loaded host)\n", jp.Joiner, natsState, procState, grace)
			_, _ = fmt.Fprint(out, startJoinerBootingHint(jp.Joiner))
			return &ExitError{Class: exitTransient, Err: fmt.Errorf("RESUME: %s's broker is still booting — do not restart its daemons; re-run `tether cluster add %s` once `systemctl status tether-broker` on %s shows it serving", jp.Joiner, jp.Joiner, jp.Joiner)}
		}
		_, _ = fmt.Fprint(out, startJoinerHint(jp.Joiner))
		return &ExitError{Class: exitTransient, Err: fmt.Errorf("RESUME: start the joiner's daemons, then re-run `tether cluster add %s`", jp.Joiner)}
	}

	// P3c RESUME A JOIN THE BOUNDARY BLOCKED: the joiner is up (P3b just proved it), so a join op that went
	// BLOCKED on its catch-up deadline WHILE the joiner was down is re-entered with one confirm — the
	// documented "fix and re-run `tether cluster add`" recovery, made true. Not charged to
	// --auto-confirm-catchup: that budget bounds stalls of a joiner that IS running and not catching up;
	// this stall was manufactured by the boundary itself (see catchupBarrier), so it is spent here or never.
	if err := resumeBlockedJoin(ctx, nc, actor, accountSeed, leader, opID, out); err != nil {
		return haltAdd(webhook, "resume-blocked-join", jp.Joiner, err)
	}

	// P6 CATCH-UP + PROMOTE: the leader's driveJoin promotes the caught-up nonvoter to VOTER once the mesh + JS
	// meta are up. Poll op-status to SERVING.
	_, _ = fmt.Fprintf(out, "→ wait join → SERVING (timeout %s)\n", timeout)
	if err := waitJoinServing(ctx, nc, actor, accountSeed, leader, opID, jp, timeout, out); err != nil {
		return haltAdd(webhook, "catch-up", jp.Joiner, err)
	}
	_, _ = fmt.Fprintf(out, "  ✓ %s is now a VOTER\n", jp.Joiner)
	notifyGrow(webhook, "voter", map[string]any{"joiner": jp.Joiner})

	// P8 REBALANCE: spread proxy homes onto the new voter (idempotent).
	if resp, err := sendGrowTrigger(ctx, nc, actor, accountSeed, &proto.ClusterGrowReq{Op: "rebalance-proxy", TargetNode: leader, JoinerNode: jp.Joiner}); err != nil || (resp != nil && !resp.OK) {
		_, _ = fmt.Fprintf(out, "  ⚠ rebalance-proxy did not confirm (non-fatal; run `cluster rebalance proxy` later)\n")
	}

	// P9 RELEASE LOCK on clean completion. R7 (#31): retried with backoff, and a failure is FATAL to the exit
	// code — the grow itself succeeded, but the cluster is left with membership fenced, so reporting success
	// would hand automation a green light over a blocked control plane.
	keeper.Stop() // stop renewing before releasing (see driveUpgrade — ordering is for log clarity, not safety)
	if rerr := releaseGrowLock(ctx, nc, actor, accountSeed, jp.Joiner, out); rerr != nil {
		notifyGrow(webhook, "halt", map[string]any{"phase": "release-lock", "joiner": jp.Joiner, "error": rerr.Error()})
		return rerr
	}
	// R7b: a grow that lost its lock partway through ran unserialized against other membership changes.
	// The joiner may well be SERVING, but the safety property the lock exists to provide was not held.
	if keeper.Lost() {
		notifyGrow(webhook, "halt", map[string]any{"phase": "lease", "joiner": jp.Joiner, "error": "grow lock lost mid-grow"})
		return keeper.LostErr("cluster add completed the grow but LOST its grow lock partway through")
	}
	_, _ = fmt.Fprintln(out, "cluster add complete.")
	notifyGrow(webhook, "complete", map[string]any{"joiner": jp.Joiner})
	return nil
}

// renewGrowLease pushes THIS joiner's grow-lock lease forward by one TTL. Like the upgrade renewal it
// re-resolves the leader every call: a grow can outlive several elections, and a cached leader would make
// every renewal after the first one fail.
func renewGrowLease(ctx context.Context, nc *nats.Conn, actor string, seed []byte, joiner string) (bool, error) {
	leader, err := currentLeader(ctx, nc, actor)
	if err != nil {
		return true, fmt.Errorf("resolve leader: %w", err) // transient: still ours as far as we know
	}
	resp, err := sendGrowTrigger(ctx, nc, actor, seed, &proto.ClusterGrowReq{Op: "renew-lock", TargetNode: leader, JoinerNode: joiner})
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

// --- phase helpers -------------------------------------------------------------------------------------

func haltAdd(webhook, phase, joiner string, err error) error {
	notifyGrow(webhook, "halt", map[string]any{"phase": phase, "joiner": joiner, "error": err.Error()})
	return unavailErr("cluster add HALTED at %s (%s): %v (the cluster is left in a safe partial state; fix and re-run `tether cluster add %s` to resume)", phase, joiner, err, joiner)
}

// startJoinerHint is the PAUSED-at-start-joiner operator step. External review B2: it must say `restart
// nats-server`, NOT `start`. This boundary is reached AFTER renderJoinerClusteredConf rewrote the joiner's
// nats.conf to clustered; if nats-server is already running its old STANDALONE conf (the provisioning
// standalone-boot), a bare `systemctl start` is a no-op that never loads the clustered conf — the broker then
// connects with cluster nkeys the standalone conf lacks and the grow stalls. `restart` reloads reliably whether
// nats is running standalone or not yet up.
func startJoinerHint(joiner string) string {
	return fmt.Sprintf("PAUSED at start-joiner:\n  %s\n  on %s:  systemctl restart nats-server && systemctl start tether-broker\n  then:   tether cluster add %s …   # resumes from here\n", pauseKindStartDaemons, joiner, joiner)
}

// The machine-readable second line of each start-joiner pause. Both pauses share the first line
// (`PAUSED at start-joiner:`) because provisioning already keys on it; but the two ask for OPPOSITE
// actions, and automation that greps only the first line performs the restart the booting pause
// forbids (round-2 review R2-F7). A tool that wants to act on the pause reads this line (the webhook
// carries the same fact as `daemons_running`); a human reads the sentence after it.
const (
	pauseKindStartDaemons = "PAUSE-KIND: start-daemons"
	pauseKindBooting      = "PAUSE-KIND: booting"
)

// startJoinerBootingHint is the OTHER pause at the same boundary: the joiner's daemons are running (nats-server
// clustered, a `tether serve` process present) and the broker still did not serve within the grace. It keeps
// the `PAUSED at start-joiner` signature provisioning keys on, but it must NOT repeat the restart line — a
// `systemctl restart nats-server` against a broker that is mid clustered-JetStream-meta formation restarts the
// boot the operator is waiting for (simcluster-speed G1, drill 91 under 8 concurrent grows).
// joinerIsBooting decides which pause the expired boundary prints: true only when the boundary WAITED
// (grace > 0, so the daemons looked alive) AND both facts say the joiner is running — nats-server loaded the
// clustered conf and a `tether serve` process is present. A fresh joiner (init ran here), an unknown reading,
// or a missing process is the ordinary "start the daemons" pause.
func joinerIsBooting(initRan bool, grace time.Duration, nats natsLocalState, proc brokerProcState) bool {
	return !initRan && grace > 0 && proc == brokerProcPresent && nats == natsLocalClustered
}

func startJoinerBootingHint(joiner string) string {
	return fmt.Sprintf("PAUSED at start-joiner:\n  %s\n  %s's daemons are running; its broker is not serving cluster status yet (clustered JetStream meta still forming) — do not restart them\n  then:   tether cluster add %s …   # re-run resumes from here once the broker serves\n", pauseKindBooting, joiner, joiner)
}

// runSelfInit shells out to THIS binary's `cluster init --from-existing` (reusing the exact tested migration +
// seam-apply path) with the machine-escape confirm so no TTY is needed.
//
// N-4c (record-only): this does NOT pass --nats-conf, so a grown joiner's seam records the DEFAULT
// nats_conf_path. Threading a custom conf path through join bundles / joinerParams is deliberate scope
// creep for a flow that only targets stock installs; a custom-conf-path deployment growing via `cluster
// add` gets the default-path seam (harmless on stock installs, where the path IS the default).
func runSelfInit(cmd *cobra.Command, jp joinerParams) error {
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve self binary: %w", err)
	}
	args := []string{"cluster", "init", "--from-existing",
		"--self-id", jp.Joiner, "--name", jp.Name, "--node-ident-pub", jp.NodeIdentPub,
		"--raft-addr", jp.RaftAddr, "--nats-route", jp.NatsRoute, "--tunnel-addr", jp.TunnelAddr,
		"--public-host", jp.PublicHost, "--data-dir", jp.DataDir, "--db", jp.DBPath, "--secrets-dir", jp.SecretsDir,
		"--config", jp.ConfigPath,
		"--confirm-node-id", firstNonEmpty(jp.ConfirmNodeID, jp.Joiner)}
	c := exec.CommandContext(cmd.Context(), self, args...)
	c.Env = append(os.Environ(), "TETHER_CONFIRM_NODE_ID="+firstNonEmpty(jp.ConfirmNodeID, jp.Joiner))
	var stderr bytes.Buffer
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("cluster init: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// runSelfJoinPrepare shells out to `cluster join prepare` and returns the single tether-join: bundle line.
func runSelfJoinPrepare(cmd *cobra.Command, jp joinerParams) (string, error) {
	self, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("resolve self binary: %w", err)
	}
	c := exec.CommandContext(cmd.Context(), self, "cluster", "join", "prepare",
		"--node-id", jp.Joiner, "--raft-addr", jp.RaftAddr, "--nats-route", jp.NatsRoute,
		"--tunnel-addr", jp.TunnelAddr, "--public-host", jp.PublicHost, "--secrets-dir", jp.SecretsDir)
	var stdout bytes.Buffer
	c.Stdout = &stdout
	if err := c.Run(); err != nil {
		return "", fmt.Errorf("cluster join prepare: %w", err)
	}
	for _, line := range strings.Split(stdout.String(), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "tether-join:") {
			return strings.TrimSpace(line), nil
		}
	}
	return "", fmt.Errorf("cluster join prepare produced no tether-join: bundle")
}

// findJoinOp returns the op_id of an in-flight join op for the joiner ("" if none), via a join-status probe on
// a scan — used to skip a fresh prepare on a resume. It reads the leader's ops through the grow trigger.
func findJoinOp(ctx context.Context, nc *nats.Conn, actor string, seed []byte, leader, joiner string) (string, error) {
	// A cheap targeted read: ask the leader for any op whose op_id OR target is the joiner. join-status keys on
	// OpID; since we do not know the op_id yet, resolve it from the cluster ops list via a status probe.
	resp, err := sendGrowTrigger(ctx, nc, actor, seed, &proto.ClusterGrowReq{Op: "join-status", TargetNode: leader, OpID: joiner})
	return resolveJoinOp(resp, err, leader)
}

// resolveJoinOp classifies a join-status probe reply into (op_id-to-resume, error) — pure, so the A4
// transient-vs-absent distinction is table-testable without a NATS harness. Three outcomes:
//   - a LIVE (OK, non-terminal) op        → its op_id (resume it)
//   - a genuine absence (node_unknown) OR a stale TERMINAL op → ("", nil): a fresh prepare/approve
//   - a TRANSPORT error or any unexpected non-OK reply       → ("", err): driveAdd HALTs with a retry hint
//
// A4: the transport-error case is the fix. Conflating it with "no op" (the old `err!=nil → ""`) made a
// resume-after-cutover — the leader's nats was just SIGKILL-restarted and is mid-reconnect — fall to a
// FRESH prepare → a new nonce → a different op_id → StartJoinOperation refuses "another operation is in
// flight — abort first", defeating the resume promise and tempting the operator to destroy healthy progress.
func resolveJoinOp(resp *proto.ClusterGrowResp, err error, leader string) (string, error) {
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", fmt.Errorf("join-status: empty reply from leader %s", leader)
	}
	// Genuine absence: the leader replies node_unknown (no op for this joiner). → "" → a fresh prepare/approve
	// (first run, or a resume after a terminal op was `recovery node remove`d). This is the ONLY "" absence.
	if resp.Code == adminsock.CodeNodeUnknown {
		return "", nil
	}
	if !resp.OK {
		// Any OTHER non-OK reply (cluster_not_ready is already retried inside sendGrowTrigger) is unexpected —
		// fail closed so a resume does not fork on it.
		return "", fmt.Errorf("join-status probe failed: %s %s", resp.Code, resp.Error)
	}
	// B1: attach ONLY to a LIVE (non-terminal) op. A stale TERMINAL row (an ABORTED op after an operator
	// `cluster ops abort`, or a SERVING op left after the node was later `recovery node remove`d) must NOT be
	// treated as "in flight — resume": that would skip prepare/approve and either dead-end the grow forever or
	// falsely report success on an absent node. A terminal op → "" → a fresh prepare/approve (StartJoinOperation
	// mints a new op from the fresh nonce; the already-VOTER case is short-circuited earlier in driveAdd).
	if resp.Terminal {
		return "", nil
	}
	return resp.OpID, nil
}

// waitJoinServing polls join-status until the op is terminal SERVING, optionally auto-confirming a BLOCKED op.
func waitJoinServing(ctx context.Context, nc *nats.Conn, actor string, seed []byte, leader, opID string, jp joinerParams, timeout time.Duration, out interface{ Write([]byte) (int, error) }) error {
	deadline := time.Now().Add(timeout)
	confirms := 0
	prevBlocked := false // C1: track the BLOCKED edge so one stall spends at most one confirm
	lastBlockedErr := "" // C1 (Stage-C): remember the last BLOCKED cause so a stuck stall's timeout surfaces it
	for {
		resp, err := sendGrowTrigger(ctx, nc, actor, seed, &proto.ClusterGrowReq{Op: "join-status", TargetNode: leader, OpID: opID})
		if err == nil && resp != nil && resp.OK {
			switch {
			case resp.OpState == "SERVING":
				return nil
			case resp.Terminal:
				return fmt.Errorf("join op %s ended non-SERVING: %s (%s)", opID, resp.OpState, resp.LastError)
			case resp.OpState == "BLOCKED":
				// C1: --auto-confirm-catchup N means N distinct catch-up STALLS, not N polls. Spend a confirm
				// only on the ENTER-BLOCKED edge (!prevBlocked); a single op that sits BLOCKED across several
				// polls previously burned the whole budget in ~3*N s.
				lastBlockedErr = resp.LastError
				errBudget, spendConfirm := blockedConfirmDecision(confirms, jp.AutoConfirmCatchup, prevBlocked)
				switch {
				case errBudget:
					return fmt.Errorf("join op %s is BLOCKED (%s) — the joiner is not catching up; check it, then `cluster ops confirm %s` on the leader (or re-run with --auto-confirm-catchup N)", opID, resp.LastError, opID)
				case spendConfirm:
					// F1 (external review): send the confirm-op and only COUNT it (spend budget + arm the edge)
					// if it actually LANDED. A transient confirm failure — its reply lost during the grow mesh/
					// cutover restart, or a non-OK reply other than the already-retried cluster_not_ready — must
					// NOT burn the budget or arm the edge, else the same BLOCKED state would never re-send the
					// confirm and would stall to the full join timeout. On a failure, leave prevBlocked false so
					// the NEXT BLOCKED poll re-sends it (restoring the old repeated-confirm resilience).
					cresp, cerr := sendGrowTrigger(ctx, nc, actor, seed, &proto.ClusterGrowReq{Op: "confirm-op", TargetNode: leader, OpID: opID})
					if confirmLanded(cresp, cerr) {
						confirms++
						_, _ = fmt.Fprintf(out, "  join op BLOCKED — auto-confirmed (%d/%d)\n", confirms, jp.AutoConfirmCatchup)
						prevBlocked = true
					} else {
						_, _ = fmt.Fprintf(out, "  join op BLOCKED — confirm did not land (%s); retrying next poll\n", confirmFailDetail(cresp, cerr))
						prevBlocked = false
					}
				default:
					// same stall, budget remains, the prior confirm LANDED → keep polling for it to take effect.
					prevBlocked = true
				}
			default:
				prevBlocked = false // any other live state clears the edge so a distinct later stall re-arms
			}
		}
		if time.Now().After(deadline) {
			// C1 (Stage-C): a stall the confirm(s) never cleared must SURFACE the BLOCKED cause, not decay into
			// a bare generic timeout that hides it (the M2-class regression the cutover path also guards against).
			if lastBlockedErr != "" {
				return fmt.Errorf("join op %s did not reach SERVING within %s — last state BLOCKED (%s); the joiner is not catching up, `cluster ops confirm %s` on the leader (or re-run with a larger --auto-confirm-catchup)", opID, timeout, lastBlockedErr, opID)
			}
			return fmt.Errorf("join op %s did not reach SERVING within %s", opID, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(growConvergePoll):
		}
	}
}

// blockedConfirmDecision is waitJoinServing's pure BLOCKED-handling decision (C1). Given the confirm budget,
// how many confirms are already spent, and whether the op was BLOCKED on the PREVIOUS poll, it decides
// whether to error out (budget exhausted → surface the actionable BLOCKED hint now) or spend one confirm
// (only on the ENTER-BLOCKED edge). Extracted so the edge-count semantics are table-testable without a live
// NATS poll loop. A same-stall poll with budget remaining returns (false,false) → keep polling, do NOT
// re-confirm (a persistent stall the first confirm did not clear is surfaced by the deadline path above).
func blockedConfirmDecision(confirms, budget int, prevBlocked bool) (errBudgetExhausted, spendConfirm bool) {
	if confirms >= budget {
		return true, false
	}
	if !prevBlocked {
		return false, true
	}
	return false, false
}

// confirmLanded reports whether a confirm-op reply actually took effect (F1). A transport error or a non-OK
// reply means the confirm did NOT land and must be RETRIED on the next poll, not counted against the
// --auto-confirm-catchup budget or used to arm the BLOCKED edge. (cluster_not_ready is already retried
// inside sendGrowTrigger before we ever see the reply here.)
func confirmLanded(resp *proto.ClusterGrowResp, err error) bool {
	return err == nil && resp != nil && resp.OK
}

// confirmFailDetail renders a short reason for a confirm-op that did not land (for the retry log line).
func confirmFailDetail(resp *proto.ClusterGrowResp, err error) string {
	if err != nil {
		return err.Error()
	}
	if resp != nil {
		return strings.TrimSpace(resp.Code + " " + resp.Error)
	}
	return "no reply"
}

// catchupBarrier decides whether a join-status reply clears the AddNonvoter barrier. External review M3:
// CATCHING_UP / SERVING clear it; a TERMINAL non-SERVING op (ABORTED / failed before AddNonvoter) is a HARD
// error — the driver must NOT proceed into the mesh render/cutover on a dead op (the same terminal-op-resume
// class as B1's findJoinOp, one phase later). A non-OK / still-progressing reply neither clears nor errors.
func catchupBarrier(resp *proto.ClusterGrowResp) (met bool, err error) {
	if resp == nil || !resp.OK {
		return false, nil
	}
	if resp.OpState == "CATCHING_UP" || resp.OpState == "SERVING" {
		return true, nil
	}
	if resp.Terminal && resp.OpState != "SERVING" {
		return false, fmt.Errorf("ended %s before catch-up: %s", resp.OpState, resp.LastError)
	}
	// A join BLOCKED after it was already CATCHING_UP has passed AddNonvoter — the leader's timeline says
	// so (NonvoterCommitted, join-status) — which is the only fact this barrier exists to establish (the
	// cutover's R3 gate wants the committed >=2-server config). It went BLOCKED because its catch-up
	// deadline ran out while the joiner's daemons were not started yet: on a resume of a RETURNING node
	// (raft/ present, so the fresh-grow zero grace does not apply) the start-joiner boundary waits the
	// whole joinerBootGrace before pausing, and provisioning only starts the daemons after the pause — the
	// op's opCatchupTimeout (2 m) is spent entirely on a joiner that could not exist yet. Waiting here for
	// CATCHING_UP again would wait two more minutes for a state the op will never re-enter on its own;
	// resumeBlockedJoin re-enters it, once the joiner is actually up. An older leader never sets the field
	// and this branch is simply never taken (the pre-existing behaviour).
	if resp.OpState == "BLOCKED" && resp.NonvoterCommitted {
		return true, nil
	}
	return false, nil
}

// resumeBlockedJoin sends ONE confirm-op for a join that is BLOCKED after having committed AddNonvoter
// (NonvoterCommitted), and nothing otherwise. It runs right after the start-joiner boundary, i.e. the first
// point at which the joiner's broker is known to be up, which is what the op's catch-up needs. The
// confirm re-enters the ladder at ROSTER_COMMITTED with a fresh barrier + deadline (ClusterAdmin.ConfirmOp),
// so waitJoinServing then sees a live CATCHING_UP, not a stale BLOCKED it would have to spend
// --auto-confirm-catchup budget on (or, with the default 0, HALT on at once).
//
// Any other BLOCKED (never CATCHING_UP, AddVoter attempts exhausted) is left to waitJoinServing's policy:
// those are stalls of a running joiner, which the operator's budget governs. A lost confirm reply is
// reported and left to waitJoinServing too (it resends on the next BLOCKED poll when a budget allows);
// a confirm that answers a refusal is a HALT with that refusal, never a silent fall-through.
func resumeBlockedJoin(ctx context.Context, nc *nats.Conn, actor string, seed []byte, leader, opID string, out interface{ Write([]byte) (int, error) }) error {
	return resumeBlockedJoinWith(func(r *proto.ClusterGrowReq) (*proto.ClusterGrowResp, error) {
		return sendGrowTrigger(ctx, nc, actor, seed, r)
	}, leader, opID, out)
}

// resumeBlockedJoinWith is resumeBlockedJoin with the transport injected (one signed round trip per call),
// so the decision table — not BLOCKED / BLOCKED-never-CATCHING_UP → nothing sent; BLOCKED-after-CATCHING_UP →
// exactly one confirm; refusal → HALT; lost reply → deferred — is testable without a NATS connection.
func resumeBlockedJoinWith(send func(*proto.ClusterGrowReq) (*proto.ClusterGrowResp, error), leader, opID string, out interface{ Write([]byte) (int, error) }) error {
	resp, err := send(&proto.ClusterGrowReq{Op: "join-status", TargetNode: leader, OpID: opID})
	if err != nil {
		// A lost status reply is not a decision: waitJoinServing polls join-status again and, when a
		// budget allows, resends the confirm itself. Swallowing it here is the documented shape.
		return nil //nolint:nilerr // best-effort pre-check; waitJoinServing owns the retry
	}
	if resp == nil || !resp.OK || resp.OpState != "BLOCKED" || !resp.NonvoterCommitted {
		return nil
	}
	// Only the catch-up-DEADLINE block is the boundary's doing. A join BLOCKED because AddVoter attempts
	// were exhausted (blockAfterAttempts) is a running joiner that is not being promoted — that stall
	// belongs to waitJoinServing's --auto-confirm-catchup policy, not to an unbudgeted resume confirm
	// (internal review round 1 R2-F1 / R4-F7).
	if !strings.HasPrefix(resp.LastError, broker.OpBlockedCatchupDeadlineMsg) {
		return nil
	}
	_, _ = fmt.Fprintf(out, "  join op %s went BLOCKED while the joiner's daemons were still down (%s) — the joiner is up now, re-entering catch-up (resume confirm; not counted against --auto-confirm-catchup)\n", opID, resp.LastError)
	cresp, cerr := send(&proto.ClusterGrowReq{Op: "confirm-op", TargetNode: leader, OpID: opID})
	switch {
	case confirmLanded(cresp, cerr):
		return nil
	case cerr == nil && cresp != nil && !cresp.OK && cresp.Code != growTriggerCodeNotReadyCLI:
		return fmt.Errorf("join op %s is BLOCKED (%s) and the resume confirm was refused: %s %s — `cluster ops show %s` on the leader", opID, resp.LastError, cresp.Code, cresp.Error, opID)
	default:
		_, _ = fmt.Fprintf(out, "  resume confirm did not land (%s); the catch-up wait retries it\n", confirmFailDetail(cresp, cerr))
		return nil
	}
}

// waitOpCatchingUp polls join-status until the op is past AddNonvoter (CATCHING_UP / SERVING, or BLOCKED
// after having been CATCHING_UP — catchupBarrier), so the former-N1 is in a committed >=2-server raft config
// before a cutover's R3 gate checks for it. A terminal non-SERVING op aborts the wait immediately.
func waitOpCatchingUp(ctx context.Context, nc *nats.Conn, actor string, seed []byte, leader, opID string, out interface{ Write([]byte) (int, error) }) error {
	deadline := time.Now().Add(2 * time.Minute)
	lastState, lastErrText := "", ""
	for {
		resp, err := sendGrowTrigger(ctx, nc, actor, seed, &proto.ClusterGrowReq{Op: "join-status", TargetNode: leader, OpID: opID})
		if err == nil {
			if met, berr := catchupBarrier(resp); berr != nil {
				return fmt.Errorf("join op %s %v — re-run to start a fresh join", opID, berr)
			} else if met {
				return nil
			}
			if resp != nil && resp.OK {
				lastState, lastErrText = resp.OpState, resp.LastError
			}
		}
		if time.Now().After(deadline) {
			// The message names the state the op was actually in. "did not commit AddNonvoter" over an op
			// that was BLOCKED for a recorded reason hid drill 42's real mechanism for a whole A/B round.
			if lastState != "" {
				return fmt.Errorf("join op %s did not reach CATCHING_UP within 2m — last state %s (%s)", opID, lastState, lastErrText)
			}
			return fmt.Errorf("join op %s did not commit AddNonvoter (reach CATCHING_UP) within 2m", opID)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(growConvergePoll):
		}
	}
}

// cutoverBroker drives a standalone→clustered mesh-cutover on target, retried idempotently: the cutover
// SIGKILLs the target's own nats-server, so its NATS reply is usually LOST (a transport error is EXPECTED on
// success) — re-sending is staged-idempotent (AlreadyDone once the target is live-clustered).
//
// External review M2: an EXPLICIT broker refusal must NOT be swallowed. When the broker REPLIES with a non-OK
// response it did NOT kill its own nats (the reply path is alive), so the refusal is a stable, deterministic
// one — a non-empty former-N1 JS store without --reset-former-js/--preserve-js-data, the R3 committed-config
// gate, a clustered-conf dry-run/render failure, or a revival failure. Those are operator-actionable and must
// HALT at the cutover with the original message, not decay into a later generic catch-up timeout that hides the
// real cause (and that simcluster, always passing --reset-former-js, could never exercise). A TRANSPORT error is
// the opposite — the post-SIGKILL lost reply while systemd Restart=always revives nats — and legitimately
// retries; the catch-up wait is its backstop. So: retry through transport errors and briefly-transient refusals
// (R3 apply-lag can clear within a poll or two), but if the LAST attempt was still a hard refusal, return it.
//
// gotcha #70 (simcluster-speed): the retry ladder used to fall off its end into `return nil` — six transport
// errors in a row were read as "the SIGKILL fired, so it must have worked" and the grow marched on to a
// catch-up wait that could only time out, four minutes later, under a signature indistinguishable from a
// real grow regression (`grow_to_3` VOTER timeout). A lost reply is not a confirmation. The ladder now ends
// in ONE more probe that must answer OK or AlreadyDone; anything else HALTs here, at the cutover, with the
// former-N1's actual state in the message — the same idempotent re-run that resumes every other HALT
// resumes this one. Each transport error is also printed, so a grow that DID recover this way says so in
// its own log instead of leaving the operator to infer it from a later timing anomaly.
func cutoverBroker(ctx context.Context, nc *nats.Conn, actor string, seed []byte, target, joiner, epoch string, ack, preserve bool, out interface{ Write([]byte) (int, error) }) error {
	req := &proto.ClusterGrowReq{Op: "mesh-cutover", TargetNode: target, JoinerNode: joiner, GrowEpoch: epoch, ResetAck: ack, PreserveData: preserve}
	return cutoverBrokerWithPoll(ctx, func(r *proto.ClusterGrowReq) (*proto.ClusterGrowResp, error) {
		return sendGrowTrigger(ctx, nc, actor, seed, r)
	}, target, req, out, growConvergePoll, func() string { return formerN1HealthSummary(nc, actor, target) })
}

// formerN1HealthSummary is the one-line state of the former-N1 that the NOT-confirmed HALT carries (plan
// §5.3 #70①: "HALT 文案含 former-N1 的状态摘要"): whether it answered the cluster-health broadcast at all,
// and if so its leader view, writable-leader confirmation and force-single marker. Advisory and
// best-effort — a probe that fails simply says so; it never changes the verdict.
func formerN1HealthSummary(nc *nats.Conn, actor, target string) string {
	if nc == nil {
		return "health probe not attempted (no connection)"
	}
	for _, h := range probeClusterHealthAdvisory(nc, actor) {
		if h.NodeID == target {
			return fmt.Sprintf("%s answered the health probe: leader_id=%q writable_leader_confirmed=%v force_single_active=%v applied_index=%d",
				target, h.LeaderID, h.WritableLeaderConfirmed, h.ForceSingleActive, h.AppliedIndex)
		}
	}
	return target + " did NOT answer the cluster-health probe (nats down, or the broker still restarting)"
}

// cutoverAttempts is the retry ladder length BEFORE the confirming probe; cutoverBrokerWithPoll sends at
// most cutoverAttempts+1 requests.
const cutoverAttempts = 6

// cutoverBrokerWithPoll is cutoverBroker with the transport and the inter-attempt spacing injected, so
// the ladder's end-state rules — refusal → HALT with the refusal, silence → HALT as "not confirmed",
// never silence → success — are table-testable without a NATS connection and without paying six
// growConvergePoll sleeps per row. `send` is one signed mesh-cutover round trip.
//
// `summary` (may be nil) renders the former-N1's state for the NOT-confirmed HALT text; it is called only
// on that path, so the table tests pass nil and the live caller pays the probe only when it matters.
func cutoverBrokerWithPoll(ctx context.Context, send func(*proto.ClusterGrowReq) (*proto.ClusterGrowResp, error), target string, req *proto.ClusterGrowReq, out interface{ Write([]byte) (int, error) }, poll time.Duration, summary func() string) error {
	var lastRefusal *proto.ClusterGrowResp
	for i := 0; i < cutoverAttempts; i++ {
		resp, err := send(req)
		if err == nil && resp != nil && (resp.OK || resp.AlreadyDone) {
			return nil
		}
		if stableCutoverRefusal(resp, err) {
			// The broker replied with a refusal — record it; a subsequent transport error (the SIGKILL fired) or
			// AlreadyDone clears it, so we only HALT on a refusal that never progressed.
			lastRefusal = resp
			_, _ = fmt.Fprintf(out, "  (cutover %s: %s %s — retry)\n", target, resp.Code, resp.Error)
		} else {
			// Transport error: the expected post-SIGKILL lost reply (nats reviving) or the target mid-restart.
			lastRefusal = nil
			_, _ = fmt.Fprintf(out, "  (cutover %s: transport error, attempt %d/%d — nats reviving?: %v)\n", target, i+1, cutoverAttempts, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(poll):
		}
	}
	if lastRefusal != nil {
		return fmt.Errorf("former-N1 %s refused the cutover: %s %s", target, lastRefusal.Code, lastRefusal.Error)
	}
	// Silence all the way down. Confirm rather than assume: the target is either live-clustered by now
	// (AlreadyDone) or it is not, and only it can say which. A caller that has already been cancelled
	// gets its own ctx error, not a "not confirmed" verdict about a probe that never went out (internal
	// review round 1 R2-F4).
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	resp, err := send(req)
	if err == nil && resp != nil && (resp.OK || resp.AlreadyDone) {
		return nil
	}
	if stableCutoverRefusal(resp, err) {
		return fmt.Errorf("former-N1 %s refused the cutover: %s %s", target, resp.Code, resp.Error)
	}
	if cerr := ctx.Err(); cerr != nil {
		return cerr
	}
	state := ""
	if summary != nil {
		state = "; " + summary()
	}
	return fmt.Errorf("former-N1 %s cutover NOT confirmed after %d attempts: every reply was lost (last: %v)%s; "+
		"its nats-server may still be reviving — check `systemctl status nats-server tether-broker` on %s and re-run this command (idempotent)",
		target, cutoverAttempts+1, err, state, target)
}

// stableCutoverRefusal reports whether a mesh-cutover attempt was an EXPLICIT broker refusal (a non-OK, non-done
// reply — NOT a transport error). Such a refusal is deterministic: the broker replied, so it did NOT SIGKILL its
// own nats, meaning the refusal (non-empty former-N1 store without ack, the R3 gate, a dry-run/render failure,
// or a revival failure) will recur on every retry and must HALT the grow rather than decay into a later generic
// catch-up timeout that hides the cause (external review M2). A transport error is the OPPOSITE — the expected
// post-SIGKILL lost reply while systemd revives nats — and legitimately retries.
func stableCutoverRefusal(resp *proto.ClusterGrowResp, err error) bool {
	return err == nil && resp != nil && !resp.OK && !resp.AlreadyDone
}

// growLockReleaseAttempts / growLockReleaseBackoff bound the release retry. Four attempts spanning ~7s cover
// the transient shapes that actually cause a dropped release — a leadership change mid-grow, a lost NATS
// reply — without turning a genuinely unreachable leader into a multi-minute hang.
const growLockReleaseAttempts = 4

// growLockReleaseBackoff is a var, not a const, ONLY so the hermetic retry test can
// compress ~7s of real backoff into microseconds. Production never reassigns it.
var growLockReleaseBackoff = 1 * time.Second

// releaseGrowLock clears THIS joiner's grow marker via the leader (mirrors releaseUpgradeLock), retrying with
// backoff, and REPORTS FAILURE to its caller.
//
// External review M1: the release is JOINER-BOUND — it carries JoinerNode, and the leader clears the marker
// ONLY if its value is this joiner. So a completed/aborted re-run of `cluster add <joinerA>` can never wipe a
// DIFFERENT grow's in-flight marker (<joinerB>), which would drop the strict-serialize mutex mid-grow and let a
// concurrent join/retire/upgrade slip in (breaking the Q7 safety model).
//
// R7 (#31): this used to be fire-and-forget — one attempt, a warning on stderr, and `cluster add` exited 0
// regardless. Two things were wrong with that. First, no retry: the overwhelmingly common cause of a failed
// release is a transient (an election, a dropped reply), and a second attempt a second later almost always
// succeeds. Second, and worse, exit 0: a caller that succeeds here has left the cluster's membership control
// plane FENCED — every later grow/retire/upgrade is refused — and reported success while doing so. Automation
// downstream of `cluster add` cannot distinguish that from a clean grow, which is precisely the failure shape
// the R7 exit criterion "the CLI must not return rc=0 when convergence has not been reached" exists to kill.
//
// The broker-side grow-lock reconciliation pass (reconcile_grow_lock.go) will clear the marker on its own
// within GrowLockReapInterval, so this is no longer an unrecoverable state — but it is still NOT converged at
// the moment this process exits, and the exit code must say so.
func releaseGrowLock(ctx context.Context, nc *nats.Conn, actor string, seed []byte, joiner string, out interface{ Write([]byte) (int, error) }) error {
	var last error
	for attempt := 1; attempt <= growLockReleaseAttempts; attempt++ {
		if attempt > 1 {
			delay := growLockReleaseBackoff << uint(attempt-2)
			select {
			case <-ctx.Done():
				last = ctx.Err()
				goto failed
			case <-time.After(delay):
			}
			_, _ = fmt.Fprintf(out, "  … retrying grow lock release (attempt %d/%d)\n", attempt, growLockReleaseAttempts)
		}
		leader, err := currentLeader(ctx, nc, actor)
		if err != nil {
			last = fmt.Errorf("could not resolve the leader: %w", err)
			continue
		}
		resp, err := sendGrowTrigger(ctx, nc, actor, seed, &proto.ClusterGrowReq{Op: "release-lock", TargetNode: leader, JoinerNode: joiner})
		switch {
		case err != nil:
			last = err
		case resp == nil:
			last = fmt.Errorf("no reply from leader %s", leader)
		case !resp.OK:
			last = fmt.Errorf("%s %s", resp.Code, resp.Error)
		default:
			return nil
		}
	}
failed:
	return unavailErr("cluster add: the grow of %s completed but its lock release did NOT confirm after %d attempts (%v) — "+
		"cluster_grow_active is still set, so `cluster join`/`cluster retire`/`cluster upgrade` stay BLOCKED. The leader's "+
		"grow-lock reconciler clears a finished grow's marker on its own within ~30s; verify with `tether cluster status`, "+
		"or re-run `tether cluster add %s` with --account-seed to clear it now",
		joiner, growLockReleaseAttempts, last, joiner)
}

// --- cluster-health readers ----------------------------------------------------------------------------

// A PROBE THAT COULD NOT BE MADE IS AN ERROR, not zero voters — origin: increment 2
// internal review, pairing-sweep/IMPACT-F1. `cluster add` prints this count to the
// operator and uses it to decide whether the existing cluster is a former-N1, so a
// silent 0 makes a healthy three-voter cluster look empty at exactly the moment a
// membership change is about to start.
func countVoters(nc *nats.Conn, actor string) (int, error) {
	replies, err := probeClusterHealth(nc, actor)
	if err != nil {
		return 0, fmt.Errorf("count voters: %w", err)
	}
	n := 0
	for _, h := range replies {
		if h.IsVoter {
			n++
		}
	}
	return n, nil
}

// formerSoleVoter returns the single existing VOTER that is NOT the joiner (the former-N1), or "".
func formerSoleVoter(nc *nats.Conn, actor, joiner string) string {
	for _, h := range probeClusterHealthAdvisory(nc, actor) {
		if h.IsVoter && h.NodeID != "" && h.NodeID != joiner {
			return h.NodeID
		}
	}
	return ""
}

// joinerBrokerUpLocal reports whether the joiner's OWN broker is up AND running in CLUSTER mode, via its local
// admin socket (we run ON the joiner). It must NOT use NATS: an unmeshed joiner's health responder is
// unreachable from the leader's nats until the cutover forms the mesh.
//
// External review B1: a broker that booted in SINGLE mode (its broker.yaml cluster seam never took effect —
// root-owned config, wrong seam placement) ALSO answers this socket, but with {OK:false, Code:cluster_not_enabled,
// Cluster:nil}. Accepting any reply misjudged that single-mode broker as "cluster-up + joined": the driver would
// skip re-rendering the clustered conf and wait for a SERVING that can never happen. Require a genuine clustered
// status report — OK AND a non-nil Cluster — so a single-mode boot fails this gate and the driver HALTs at the
// start-joiner boundary with the resume hint (its cluster seam must be fixed first).
func joinerBrokerUpLocal(socketPath string) bool {
	resp, err := callAdmin(socketPath, adminsock.Request{Op: adminsock.OpClusterStatus})
	return err == nil && adminStatusIsClustered(resp)
}

// joinerBootGrace bounds how long the start-joiner boundary waits for a joiner whose daemons provisioning has
// ALREADY started. It is not a guess at "how slow is slow": it covers the two REAL transients a just-restarted
// joiner goes through — the broker serving its admin socket at the END of Run (after the cluster backend wires),
// and, during a grow specifically, a short crash-restart cycle while its clustered JetStream still has no quorum
// (the former-N1's nats is meshing) and the broker fail-stops on the lone-clustered-JS guard until systemd's
// Restart=always brings it back.
//
// "Both resolve in seconds; a minute is generous" was the sentence here until simcluster-speed A, and it was
// wrong by construction: the joiner's broker WAITS for its clustered JetStream for up to
// broker.ClusteredJetStreamBootWait (90 s) before it serves the admin socket this probe reads, so a 60 s grace
// could expire on a joiner that was still inside its own legitimate boot. Nobody saw it because the fresh-grow
// invocation used to sit out the same 60 s BEFORE printing the resume hint, and that dead time gave the
// former-N1's JetStream a head start; remove it (A) and the first deploy-tier run HALTed the resume at
// CATCHING_UP/reachable:false on a loaded host. The grace is therefore derived from the boot wait plus one
// systemd restart cycle of margin, never a number of its own — and it is the RESUME grace only; the fresh-grow
// invocation gets zero (joinerStartGrace).
var joinerBootGrace = broker.ClusteredJetStreamBootWait() + joinerBootGraceMargin

// joinerBootGraceMargin covers the joiner's process start + one Restart=always cycle (RestartSec=2) around the
// boot wait, so a joiner that fail-stopped once at the very end of its wait is still caught on the way back.
const joinerBootGraceMargin = 30 * time.Second

// joinerStartGrace is how long the start-joiner boundary waits for the joiner's broker before HALTing
// with the resume hint. It is the WHOLE joinerBootGrace on a resume and ZERO on the invocation that ran
// the local init — the only two cases, decided by one fact this process established itself:
//
//   - initRan == true: THIS process created raft/ moments ago (P2). Cluster mode is switched on by the
//     presence of on-disk raft state (distributed-broker-architecture.md §8.3 (e); the #I1 fail-closed
//     invariant drill 11 pins: a cluster-mode serve with no raft state refuses), so before P2 no broker
//     on this host could have been running clustered, and adminStatusIsClustered would have answered
//     false for any that was running single. Nothing has asked provisioning to start anything since —
//     the resume hint is printed AFTER this decision. Waiting here waits for an event that cannot
//     happen; measured on the deploy tier it was a flat 60 s per fresh grow (2026-09-18: invocation 1
//     took 97 s / 80 s, 60 s of which was this window). An operator running `cluster add` by hand sat
//     through the same minute before being told to start the daemons.
//   - initRan == false: raft/ already existed when we started — EITHER a RESUME after provisioning
//     started the daemons (the boot / crash-restart transient joinerBootGrace exists for, below), OR the
//     FIRST invocation for a RETURNING node (drill 42: rejoin-prepare + `init --from-manifest` left raft/
//     behind, nothing has been asked to start yet). The second fact tells them apart: the joiner's
//     LOCAL nats-server. Provisioning's half of the boundary is `systemctl restart nats-server` — loading
//     the clustered conf this invocation just rendered — and until that has happened the broker cannot
//     mesh whatever it does (a broker restarting against the standalone conf fail-stops on cluster nkeys
//     the conf lacks, forever). So: nats running CLUSTERED → provisioning has done its half → the full
//     window; nats standalone or not listening → nothing can come up → zero, HALT at once with the hint.
//     gotcha #83: the 120 s spent here on a returning node was the whole reason its join op went BLOCKED
//     (the leader's opCatchupTimeout runs from AddNonvoter, i.e. from before this wait).
//   - natsClustered unknown (no broker.yaml to read the nats URL from, or the INFO read failed): keep
//     the full window — the conservative direction, and the pre-#83 behaviour.
//
// NOT the "render branch" (!joinerBrokerUpLocal at P5) and NOT "no join op yet": both are re-evaluated
// on every invocation and are true mid crash-restart on a resume too, which would reintroduce the 3-in-4
// failure the grace was added to cure. A pure function so the rule is table-testable on its own.
//
// The third fact is whether a `tether serve` PROCESS exists on this host at all (joinerBrokerProcess):
// the admin socket this boundary polls is served only at the END of the broker's Run, after its
// clustered-JetStream boot wait, so "socket not answering" cannot tell a broker still booting from no
// broker whatsoever. A returning node whose unit was stopped for the rejoin (drill 42 stops it before
// `rejoin prepare`) has nothing that could ever answer — and its nats-server may well still run its OLD
// clustered conf, so the nats fact alone reads "clustered" there (measured, image #4). No process after
// several samples (bridging systemd's RestartSec gap) → zero grace.
func joinerStartGrace(initRan bool, nats natsLocalState, proc brokerProcState) time.Duration {
	if initRan {
		return 0
	}
	if nats == natsLocalDown || nats == natsLocalStandalone {
		return 0
	}
	if proc == brokerProcAbsent {
		return 0
	}
	return joinerBootGrace // nats clustered/unknown AND a broker process present/unknown
}

// brokerProcState is what joinerBrokerProcess learned about a `tether serve` process on this host.
type brokerProcState int

const (
	brokerProcUnknown brokerProcState = iota // /proc not readable (not Linux, or no permission)
	brokerProcAbsent                         // no `tether serve` process in any sample
	brokerProcPresent                        // a `tether serve` process was seen in at least one sample
)

func (s brokerProcState) String() string {
	switch s {
	case brokerProcAbsent:
		return "absent"
	case brokerProcPresent:
		return "present"
	case brokerProcUnknown:
		return "unknown"
	}
	return "unknown"
}

// joinerBrokerProcess scans /proc for a `tether serve` process other than this one, `samples` times
// `gap` apart, and reports present as soon as one sample sees it. The repeated samples exist for systemd's
// Restart=always: between a fail-stop and the revival (RestartSec=2 in install.sh's unit) there is a short
// window with no process, and a single probe landing in it would misread a crash-looping broker as
// absent. An unreadable /proc (non-Linux, or an unreadable procfs) is Unknown, never Absent.
func joinerBrokerProcess(samples int, gap time.Duration) brokerProcState {
	return joinerBrokerProcessIn("/proc", os.Getpid(), samples, gap, time.Sleep)
}

func joinerBrokerProcessIn(procRoot string, selfPID, samples int, gap time.Duration, sleep func(time.Duration)) brokerProcState {
	if samples < 1 {
		samples = 1
	}
	sawAnyProc := false
	for i := 0; i < samples; i++ {
		if i > 0 {
			sleep(gap)
		}
		entries, err := os.ReadDir(procRoot)
		if err != nil {
			return brokerProcUnknown
		}
		for _, e := range entries {
			pid, perr := strconv.Atoi(e.Name())
			if perr != nil || pid == selfPID {
				continue
			}
			raw, rerr := os.ReadFile(filepath.Join(procRoot, e.Name(), "cmdline"))
			if rerr != nil {
				continue
			}
			sawAnyProc = true
			if isTetherServeArgv(bytes.Split(raw, []byte{0})) {
				return brokerProcPresent
			}
		}
	}
	if !sawAnyProc {
		return brokerProcUnknown // a procfs with no readable cmdlines is not evidence of anything
	}
	return brokerProcAbsent
}

// isTetherServeArgv recognises the broker daemon's argv: argv[0] basename `tether` (any install path)
// and `serve` as the first non-flag argument, i.e. install.sh's `ExecStart=… tether serve --config …`.
func isTetherServeArgv(argv [][]byte) bool {
	if len(argv) < 2 || filepath.Base(string(argv[0])) != "tether" {
		return false
	}
	for _, a := range argv[1:] {
		s := string(a)
		if s == "" || strings.HasPrefix(s, "-") {
			continue
		}
		return s == "serve"
	}
	return false
}

// natsLocalState is what joinerNatsState learned about the joiner's own nats-server.
type natsLocalState int

const (
	natsLocalUnknown    natsLocalState = iota // could not ask (no config path / no URL / read error)
	natsLocalDown                             // nothing listening on the configured client address
	natsLocalStandalone                       // listening, INFO carries no cluster name (standalone conf)
	natsLocalClustered                        // listening, INFO carries a cluster name (clustered conf loaded)
)

func (s natsLocalState) String() string {
	switch s {
	case natsLocalDown:
		return "down"
	case natsLocalStandalone:
		return "standalone"
	case natsLocalClustered:
		return "clustered"
	case natsLocalUnknown:
		return "unknown"
	}
	return "unknown"
}

// joinerNatsState reads the joiner's nats-server INFO line — the server sends it before any CONNECT, so
// no credentials are needed — and reports whether it is up and whether it runs a CLUSTERED configuration
// (nats-server sets INFO.cluster from the `cluster {}` block; a standalone conf has none). natsURL comes
// from the joiner's broker.yaml (broker.nats.url, the address its own broker dials); an empty URL or an
// unreadable answer is Unknown, never a guess.
func joinerNatsState(natsURL string, timeout time.Duration) natsLocalState {
	addr := natsInfoDialAddr(natsURL)
	if addr == "" {
		return natsLocalUnknown
	}
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return natsLocalDown
	}
	defer func() { _ = conn.Close() }()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	line, err := bufio.NewReaderSize(conn, 64*1024).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "INFO ") {
		return natsLocalUnknown
	}
	var info struct {
		Cluster string `json:"cluster"`
	}
	if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "INFO "))), &info) != nil {
		return natsLocalUnknown
	}
	if info.Cluster != "" {
		return natsLocalClustered
	}
	return natsLocalStandalone
}

// natsInfoDialAddr turns a broker.nats.url (nats://host:port, host:port, or a comma list — the first
// entry wins) into a dial address; "" when there is nothing usable.
func natsInfoDialAddr(natsURL string) string {
	first := strings.TrimSpace(strings.Split(natsURL, ",")[0])
	if first == "" {
		return ""
	}
	if !strings.Contains(first, "://") {
		first = "nats://" + first
	}
	u, err := url.Parse(first)
	if err != nil || u.Hostname() == "" {
		return ""
	}
	port := u.Port()
	if port == "" {
		port = "4222"
	}
	return net.JoinHostPort(u.Hostname(), port)
}

// joinerNatsURL is the joiner's own broker.nats.url from its broker.yaml, or "" when there is no config
// to read (then the boundary keeps its full grace — Unknown).
func joinerNatsURL(configPath string) string {
	if configPath == "" {
		return ""
	}
	c, err := serveconf.Load(configPath)
	if err != nil {
		return ""
	}
	return c.Broker.NATS.URL
}

// awaitJoinerBrokerUpLocal polls joinerBrokerUpLocal for a bounded window instead of probing ONCE.
//
// A one-shot probe is wrong at exactly this boundary. Provisioning has just restarted the joiner's nats +
// broker, so "not up in cluster mode" right now is far more often "still coming up" than "misconfigured" —
// and the one-shot HALT told the operator to start daemons that were already running, then aborted a grow
// whose joiner became healthy seconds later. Observed on the deploy tier: drill 42's returning-node re-grow
// failed this way in 3 of 4 runs (invocation 2, rc=75) while brk2 was mid boot/crash-restart, even though the
// grow itself was correct — the former-N1 had already been cut over and was sitting clustered-alone waiting.
// Polling turns that transient back into what it is; a joiner that is genuinely single-mode or truly not
// started still fails the whole window and gets the same actionable HALT.
//
// grace is the window (joinerStartGrace). grace <= 0 keeps the single probe and returns at once: the
// caller has established that nothing can come up, so the honest answer is the immediate HALT.
func awaitJoinerBrokerUpLocal(ctx context.Context, socketPath, joiner string, grace time.Duration, out interface{ Write([]byte) (int, error) }) bool {
	if joinerBrokerUpLocal(socketPath) {
		return true
	}
	if grace <= 0 {
		return false
	}
	// The operator sees the boundary BEFORE the wait, not only after it: the old order printed "waiting
	// up to 60s" and then, a minute later, the PAUSED block that named the command to run. The word
	// "PAUSED" is deliberately absent here — provisioning (simcluster:311) recognises the HALT by the
	// `PAUSED at start-joiner|RESUME` signature over the whole stdout, and an early match would blind it.
	_, _ = fmt.Fprintf(out, "→ start-joiner boundary: if %s's daemons are not running yet, on %s: systemctl restart nats-server && systemctl start tether-broker (this command waits up to %s, then pauses with a resume hint)\n", joiner, joiner, grace)
	_, _ = fmt.Fprintf(out, "  … waiting up to %s for %s's broker to serve cluster status (provisioning just restarted it)\n", grace, joiner)
	deadline := time.Now().Add(grace)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(2 * time.Second):
		}
		if joinerBrokerUpLocal(socketPath) {
			return true
		}
	}
	return false
}

// adminStatusIsClustered reports whether an OpClusterStatus reply proves the broker is up AND running in CLUSTER
// mode. A SINGLE-mode broker answers the socket too, but with OK=false / Cluster=nil / Code=cluster_not_enabled;
// accepting any reply misjudged it as clustered (external review B1).
func adminStatusIsClustered(resp *adminsock.Response) bool {
	return resp != nil && resp.OK && resp.Cluster != nil
}

// renderJoinerClusteredConf renders the joiner's OWN clustered nats.conf on disk BEFORE its broker boots — a
// fresh joiner has no replicated cluster_nodes yet and cannot boot cluster-mode with a standalone conf. It
// fetches the route-mesh peer triples from the leader (mesh-peers), then shells out to `cluster reconcile nats
// --manual` (the same render an operator would run) with self + peers. Idempotent: re-rendering writes the
// same bytes. It runs as the tether user (nats.d/ is tether-owned per G1), so no root is needed.
func renderJoinerClusteredConf(cmd *cobra.Command, nc *nats.Conn, actor string, accountSeed []byte, leader string, jp joinerParams, out interface{ Write([]byte) (int, error) }) error {
	resp, err := sendGrowTrigger(cmd.Context(), nc, actor, accountSeed, &proto.ClusterGrowReq{Op: "mesh-peers", TargetNode: leader, JoinerNode: jp.Joiner})
	if err != nil {
		return fmt.Errorf("fetch mesh peers: %w", err)
	}
	if resp == nil || !resp.OK {
		code, msg := "", ""
		if resp != nil {
			code, msg = resp.Code, resp.Error
		}
		return fmt.Errorf("fetch mesh peers refused: %s %s", code, msg)
	}
	var selfBusNkey string
	var peers []string
	for _, t := range resp.MeshPeers {
		parts := strings.SplitN(t, ",", 3)
		if len(parts) != 3 {
			continue
		}
		if parts[0] == jp.Joiner {
			selfBusNkey = parts[2]
		} else {
			peers = append(peers, t)
		}
	}
	if selfBusNkey == "" {
		return fmt.Errorf("joiner %s not yet in the leader's mesh roster (bus nkey absent) — retry", jp.Joiner)
	}
	if len(peers) == 0 {
		return fmt.Errorf("no route-mesh peers returned by the leader (roster not converged) — retry")
	}
	acctPub, err := auth.PublicKeyFromSeed(accountSeed)
	if err != nil {
		return fmt.Errorf("derive account pub: %w", err)
	}
	self, err := os.Executable()
	if err != nil {
		return fmt.Errorf("resolve self binary: %w", err)
	}
	args := []string{"cluster", "reconcile", "nats", "--manual", "--secrets-dir", jp.SecretsDir,
		"--server-name", jp.Joiner, "--route-url", jp.NatsRoute,
		"--account-issuer", acctPub, "--broker-nkey", selfBusNkey, "--conf", defaultNatsConfPath}
	for _, p := range peers {
		args = append(args, "--peer", p)
	}
	c := exec.CommandContext(cmd.Context(), self, args...)
	var stderr bytes.Buffer
	c.Stderr = &stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("reconcile nats --manual: %v: %s", err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

// resetJoinerJSStore moves aside the JOINER's own JetStream store before it boots clustered (R16 A1). A
// returning node's dead-epoch clustered JS meta would otherwise wedge the 1->2 meta and crash-loop the
// joiner (n1ClusteredJetStreamFatal). Discovery: preflight the joiner's freshly-rendered nats.conf for its
// JS store dir. A non-empty store is operator-gated (--reset-former-js / --preserve-js-data); an empty
// store (a fresh joiner) is a no-op. The move is backup-dir-first idempotent (keyed on the op id).
func resetJoinerJSStore(out interface{ Write([]byte) (int, error) }, jp joinerParams, opID string) error {
	own, err := natsconf.Preflight(defaultNatsConfPath)
	if err != nil {
		// minor-1: fail CLOSED. The render step just rewrote the joiner's clustered conf, so a preflight
		// failure here is abnormal — surfacing it (rather than silently disarming the drill-42 fix) matches
		// the m4 fail-closed posture in MoveAsideJSStore one layer down.
		return fmt.Errorf("preflight the joiner's nats.conf to resolve its JS store: %w", err)
	}
	storeDir := own.JSStoreDir()
	// M3: only a DATA-BEARING store (a returning node's stale streams/clustered meta) needs resetting. A
	// truly-fresh joiner's store is absent, empty, or only the structural skeleton a booted JS nats lays
	// down — treat it as a no-op so a fresh grow (drills 10/11) is byte-equivalent (no backup, no sentinel,
	// no spurious "moved aside" line, no false residue evidence) and never a false data-loss HALT.
	hasData, hasErr := natsconf.JSStoreHasData(storeDir)
	if hasErr != nil {
		// F4: an unreadable store is DATA-BEARING by contract — proceeding as if it were empty is what
		// re-opens the grow wedge. Surface it instead of silently skipping the reset.
		return fmt.Errorf("cannot determine whether the joiner's JetStream store holds data: %w", hasErr)
	}
	if storeDir == "" || !hasData {
		return nil
	}
	// The op id is the STABLE per-grow epoch (set at P4, always non-empty by P5). Fail LOUD rather than mint
	// a fixed collision-prone "grow-bak.join" that a second grow would treat as a same-epoch no-op.
	if opID == "" {
		return fmt.Errorf("reset joiner JS store: no op id (grow epoch) in hand — refusing a fixed-name backup that a later grow could collide with")
	}
	backup := storeDir + ".grow-bak." + opID
	sentinel := filepath.Join(jp.DataDir, ".grow-joiner-reset-"+opID+".done")
	moved, err := natsconf.MoveAsideJSStore(storeDir, backup, sentinel, jp.ResetFormerJS || jp.PreserveJSData)
	if err != nil {
		// origin: line-2 closure verification m9 — see the sibling branch in cluster_offline.go.
		if !errors.Is(err, natsconf.ErrJSStoreNeedsAck) {
			return fmt.Errorf("the JOINER %s has a JetStream store that could not be inspected or moved, and it "+
				"must be reset before the node can boot clustered — %w\n"+
				"  --reset-former-js / --preserve-js-data will NOT help here: they only acknowledge a "+
				"data-bearing store, and this is a failure to read or write it. Fix the condition named above "+
				"on %s, then re-run", jp.Joiner, err, jp.Joiner)
		}
		// Widen the refusal to name the JOINER end of the grow + the exact re-run (M2 stable-refusal).
		return fmt.Errorf("the JOINER %s carries a pre-existing JetStream store that must be reset before it can "+
			"boot clustered — %w\n  re-run `tether cluster add %s … --reset-former-js` (or --preserve-js-data): "+
			"the flags move ANY pre-existing JS store on EITHER end of the grow aside (grow-bak, NEVER deleted)",
			jp.Joiner, err, jp.Joiner)
	}
	if moved != "" {
		_, _ = fmt.Fprintf(out, "→ reset joiner %s JetStream store (moved aside to %s; a returning node's stale clustered meta would wedge the grow)\n", jp.Joiner, moved)
	}
	return nil
}

func joinerIsVoter(nc *nats.Conn, actor, joiner string) bool {
	for _, h := range probeClusterHealthAdvisory(nc, actor) {
		if h.NodeID == joiner && h.IsVoter {
			return true
		}
	}
	return false
}

func formerN1Note(votersBefore int) string {
	if votersBefore == 1 {
		return "former-N1 cutover (JS reset)"
	}
	return "existing voters SIGHUP-reload (no former-N1 cutover)"
}

func routeCertSANPreflight(secretsDir, natsRoute string) error {
	return cluster.RouteCertSANMatches(secretsDir, natsRoute)
}

func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// verifyClusterSeam confirms the joiner's broker.yaml carries a broker.cluster seam whose EVERY cluster-mode
// field MATCHES this `cluster add`'s parameters — not merely present/non-empty. External re-review
// R2-B1/R3-B1/R4-B1 walked this in: raft_addr must equal the roster's raft addr (R2), the cluster-mode-trigger
// fields must be present (R3 — serve keys cluster mode on a non-empty data_dir, cutover.go), and they must be
// the RIGHT values (R4 — a stale-but-full-looking data_dir would decode + pass a presence check yet boot the
// joiner against the wrong raft path). So the 2-arg presence check was the root cause; this takes the full
// expected tuple and fail-closes on ANY mismatch, printing actual vs want.
func verifyClusterSeam(configPath, wantRaftAddr, wantDataDir, wantSecretsDir, wantNatsConfPath string) error {
	c, err := serveconf.Load(configPath)
	if err != nil {
		// review Mi10: since #75 the most common Load failure is a strict-decode
		// error (a typo'd / mis-nested key), NOT a missing seam — pointing the
		// operator at "apply the seam" would send them the wrong way and loop.
		// Fix the named key first (the wrapped error carries key + line).
		return fmt.Errorf("broker.yaml %s does not parse (strict since #75) — fix the named key, then re-run: %w", configPath, err)
	}
	cs := c.Broker.Cluster
	if cs.RaftAddr == "" && cs.DataDir == "" && cs.SecretsDir == "" && cs.NatsConfPath == "" {
		return fmt.Errorf("broker.cluster seam is NOT set in %s — the joiner would boot in SINGLE mode. Apply it as root "+
			"(set broker.cluster.{data_dir,raft_addr,secrets_dir,nats_conf_path}, e.g. raft_addr: %s), then re-run", configPath, wantRaftAddr)
	}
	for _, m := range []struct{ name, got, want string }{
		{"raft_addr", cs.RaftAddr, wantRaftAddr},
		{"data_dir", cs.DataDir, wantDataDir},
		{"secrets_dir", cs.SecretsDir, wantSecretsDir},
		{"nats_conf_path", cs.NatsConfPath, wantNatsConfPath},
	} {
		if m.got != m.want {
			return fmt.Errorf("broker.cluster.%s in %s is %q but this `cluster add` needs %q — refusing a stale/incomplete/wrong cluster seam "+
				"(serve keys cluster mode + raft state on these; a mismatch boots the joiner SINGLE or against the wrong raft path); "+
				"fix broker.cluster to match this node, then re-run", m.name, configPath, m.got, m.want)
		}
	}
	return nil
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
