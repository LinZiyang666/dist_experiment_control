package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
)

// cluster_upgrade.go — G5 #13/#14 W4: the ctl-driven rolling broker-upgrade orchestrator. It runs
// EXTERNAL to all brokers (so it survives every restart including the leader's) and drives each host over
// the account-signed remote-trigger subject (W2b): plan the roll (followers-first / leader-last), then per
// host transfer-leader-if-needed → reload → wait for it to re-register at the target. Binary STAGING is
// the privileged precondition (§0.1) — the reload verifies the on-disk sha and refuses a stale image.
//
// v1 scope: the ordered roll + signed triggers + per-host version-converge wait + N=2 write-fence ack +
// dry-run + a canary command-version skew check. The mandatory pre-roll backup + single-active-op lock are
// documented follow-ups (the roll HALTS on any refusal, never de-clusters).

const (
	upgradeTriggerTimeout = 10 * time.Second
	upgradeConvergeTimeout = 3 * time.Minute
	upgradeConvergePoll    = 3 * time.Second
)

func newClusterUpgradeCmd() *cobra.Command {
	var (
		natsURL, home, seedPath, toVersion, expectSHA, webhook string
		dryRun, ackWriteFence, backupTaken                     bool
	)
	cmd := &cobra.Command{
		Use:   "upgrade",
		Short: "Rolling broker-daemon upgrade (transfer-leader-first, one host at a time)",
		Example: "  tether cluster upgrade --to-version v0.5.0 --dry-run                          # preview the ordered roll\n" +
			"  tether cluster upgrade --to-version v0.5.0 --expect-sha256 <hex> \\\n" +
			"    --account-seed /etc/tether/secrets/account.nk --backup-taken               # execute (staging + backup done first)",
		Long: "Rolls every broker host to --to-version ONE AT A TIME, transferring leadership off a host\n" +
			"before restarting it, so an N>=3 cluster keeps quorum throughout. The target binary must be\n" +
			"STAGED on each host first (a privileged step — /usr/local/bin/tether is root-owned); the reload\n" +
			"verifies the on-disk sha and refuses a stale image. Requests are signed with the cluster account\n" +
			"seed (--account-seed) and verified by each broker. N=2 fences writes during each restart (inherent\n" +
			"F=0) — pass --ack-writefence to proceed; production should run N>=3.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if toVersion == "" {
				return usageErr("--to-version is required (the release every broker host should end on)")
			}
			sid := cli.ReadCurrentSession(home)
			if sid == "" {
				return fmt.Errorf("no active session — run `tether login -s <sid>` first")
			}
			natsURL = cli.ResolveNATSURLFromHome(natsURL, cmd.Flags().Changed("nats-url"), home)
			id, err := cli.EnsureIdentity(home)
			if err != nil {
				return err
			}
			nc, err := connectCtl(cmd, "cluster upgrade", home, natsURL, id, nats.Name(cli.CtlNameForSession(sid)))
			if err != nil {
				return err
			}
			defer nc.Close()

			// External-review round2 B3: read the account seed EARLY (when provided) so the upgrade planner can
			// VERIFY the signed roster (signature + expiry) against the account pub, rather than trusting a
			// discovery-cache manifest blindly. It also lets the no-op path (below) send a signed release-lock to
			// self-heal a stale lock. On execute the seed is required (checked later); dry-run may omit it.
			var accountSeed []byte
			var accountPub string
			if seedPath != "" {
				var rerr error
				if accountSeed, rerr = os.ReadFile(seedPath); rerr != nil {
					return unavailErr("read account seed: %v", rerr)
				}
				if accountPub, rerr = auth.PublicKeyFromSeed(accountSeed); rerr != nil {
					return unavailErr("derive account pub from seed: %v", rerr)
				}
			}

			out := cmd.OutOrStdout()
			nodes, err := buildUpgradeNodes(cmd.Context(), nc, id.PublicKey, sid, accountPub, out)
			if err != nil {
				return err
			}
			plan := clusterupgrade.Compute(nodes, toVersion)
			renderUpgradePlan(out, plan, toVersion)
			if len(plan.Refused) > 0 {
				return unavailErr("cluster upgrade refused (restore HA first): %v", plan.Refused)
			}
			if plan.N2WriteFence && !ackWriteFence {
				return usageErr("N=2: each broker restart fences writes for its restart duration (inherent F=0). " +
					"Pass --ack-writefence to proceed, or grow to N>=3 first.")
			}
			if plan.Upgrades() == 0 {
				// External-review round2 B1: a prior roll that COMPLETED but whose release-lock did not confirm
				// leaves cluster_upgrade_active set — and since every host is now at target, the roll loop that
				// would clear it never runs. Detect that stale lock here and clear it (a reachable self-heal, not
				// an undocumented manual path). Needs the account seed to sign release-lock; a pure dry-run/no-seed
				// preview never set a lock, so skipping the clear there is correct.
				if upgradeLockHeld(nc, id.PublicKey) {
					// External-review round3 doubt: clearing needs the account seed to SIGN release-lock. If the
					// operator re-ran WITHOUT --account-seed, do NOT print a bare "nothing to do" while membership
					// stays blocked — tell them exactly how to clear it.
					if accountSeed == nil {
						_, _ = fmt.Fprintln(out, "  ⚠ WARNING: a stale cluster_upgrade_active lock is set (a prior roll finished but its release did not confirm) — `cluster join`/`cluster retire` stay BLOCKED. Re-run `tether cluster upgrade --to-version "+toVersion+" --account-seed <path>` to clear it (the seed is needed to sign the release).")
					} else {
						leader, lerr := currentLeader(cmd.Context(), nc, id.PublicKey)
						if lerr != nil {
							return unavailErr("all hosts at target, but a stale cluster_upgrade_active lock is set and no leader is visible to clear it (%v) — membership stays blocked until it is released", lerr)
						}
						resp, terr := sendUpgradeTrigger(cmd.Context(), nc, id.PublicKey, accountSeed, &proto.ClusterUpgradeReq{Op: "release-lock", TargetNode: leader})
						if terr != nil || resp == nil || !resp.OK {
							return unavailErr("all hosts at target, but a stale cluster_upgrade_active lock could NOT be cleared (leader=%s): membership stays blocked until it is released — retry, or check the leader", leader)
						}
						_, _ = fmt.Fprintln(out, "cleared a stale upgrade lock (a prior roll finished but its release did not confirm).")
					}
				}
				_, _ = fmt.Fprintln(out, "all broker hosts are already at the target — nothing to do.")
				return nil
			}
			if dryRun {
				_, _ = fmt.Fprintln(out, "(dry-run — no host was touched)")
				return nil
			}
			if seedPath == "" {
				return usageErr("--account-seed <path> is required to execute (the cluster account seed signs each broker trigger)")
			}
			// Stage-C M6: the reload verifies the on-disk binary against ExpectSHA256 — require it so a
			// default roll NEVER re-execs an unverified/unstaged image (inv: no-restart-into-a-bad-binary).
			if expectSHA == "" {
				return usageErr("--expect-sha256 <hex> is required to execute — each broker reload verifies the staged on-disk binary against it and refuses a mismatch")
			}
			// Stage-C M5: a target's first-boot may run a ONE-WAY forward migration; the only recovery is
			// restore-not-reinstall. The orchestrator (over NATS) cannot take the socket-local `cluster
			// backup`, so require the operator to confirm they took one first (documented in the runbook).
			if !backupTaken {
				return usageErr("take a verified `tether cluster backup` on the leader FIRST (a target's first-boot migration is one-way; restore is the only recovery), then re-run with --backup-taken")
			}
			return driveUpgrade(cmd, nc, id.PublicKey, sid, accountSeed, plan, toVersion, expectSHA, webhook)
		},
	}
	cmd.Flags().StringVar(&toVersion, "to-version", "", "the release every broker host should end on (required)")
	cmd.Flags().StringVar(&expectSHA, "expect-sha256", "", "hex sha256 of the STAGED on-disk binary; each reload refuses a mismatch")
	cmd.Flags().StringVar(&seedPath, "account-seed", "", "path to the cluster account nkey seed (signs each broker trigger; required to execute)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print the ordered roll plan and exit without touching any host")
	cmd.Flags().BoolVar(&ackWriteFence, "ack-writefence", false, "acknowledge the inherent N=2 write-fence and proceed")
	cmd.Flags().BoolVar(&backupTaken, "backup-taken", false, "confirm a verified `cluster backup` was taken first (a target's first-boot migration is one-way — restore is the only recovery)")
	cmd.Flags().StringVar(&webhook, "notify-webhook", "", "POST a JSON milestone to this URL on start / per-host / complete / HALT")
	cmd.Flags().StringVar(&natsURL, "nats-url", "", "broker NATS URL")
	cmd.Flags().StringVar(&home, "home", cli.DefaultHome(), "tether home dir")
	return cmd
}

// buildUpgradeNodes folds the live cluster-health probe (broker daemons: VER + leader + co-located agent
// nid) with the node list (agent RELEASEs) into the planner's []Node. A broker that answers the health
// probe is a current voter (the observe target IS the voter roster); CaughtUp is approximated true here and
// re-verified live by the per-host converge wait during the roll.
func buildUpgradeNodes(ctx context.Context, nc *nats.Conn, actor, sid, accountPub string, out io.Writer) ([]clusterupgrade.Node, error) {
	// A7: the node list gives each host's co-located AGENT release (the #19 whole-host at-target check needs
	// it). FAIL CLOSED on a transport/decode failure — silently swallowing it left agentRelease empty, so
	// EVERY host looked stale (AgentVer=="") and an already-at-target cluster got planned into a full,
	// disruptive roll (a spurious leader transfer + agent re-execs) with no diagnostic. A genuinely-unpaired
	// agent (the reply arrives but its nid is absent from Nodes) still yields "" → not-at-target, intended.
	agentRelease := map[string]string{}
	body, merr := json.Marshal(proto.NodeListReq{})
	if merr != nil {
		return nil, unavailErr("marshal node-list request: %v", merr)
	}
	rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	msg, rerr := nc.RequestWithContext(rctx, proto.SubjCtrlNodeList(actor, sid), body)
	cancel()
	if rerr != nil {
		return nil, unavailErr("node-list RPC failed (%v) — cannot determine co-located agent versions; refusing to plan a roll on incomplete data (every host would falsely look stale)", rerr)
	}
	var nlResp proto.NodeListResp
	if uerr := json.Unmarshal(msg.Data, &nlResp); uerr != nil {
		return nil, unavailErr("decode node-list reply: %v — refusing to plan a roll on incomplete data", uerr)
	}
	// A7 (Stage-C): an application-error reply (store_error / not_a_member / session_not_found_or_deleting /
	// actor_invalid) decodes cleanly but carries a non-empty Code and an EMPTY Nodes slice — which would
	// leave agentRelease empty and make EVERY host look stale, the exact symptom the transport/decode guards
	// above exist to prevent. Fail closed on it too. A legitimately-empty node list returns Code=="".
	if nlResp.Code != "" {
		return nil, unavailErr("node-list RPC returned %s (%s) — refusing to plan a roll on incomplete data (every host would falsely look stale)", nlResp.Code, nlResp.Error)
	}
	for _, n := range nlResp.Nodes {
		agentRelease[n.NID] = n.ReleaseVersion
	}
	replies := probeClusterHealth(nc, actor)
	if len(replies) == 0 {
		return nil, unavailErr("no broker answered the cluster-health probe (single broker, or brokers unreachable) — nothing to roll")
	}
	byNode := map[string]proto.ClusterHealthResp{}
	for _, h := range replies {
		if h.NodeID != "" {
			if _, dup := byNode[h.NodeID]; !dup {
				byNode[h.NodeID] = h // first-wins dedup (a flapping broker cannot answer twice)
			}
		}
	}
	build := func(id string, h proto.ClusterHealthResp, voter bool) clusterupgrade.Node {
		agentNID := h.ColocatedAgentNID
		if agentNID == "" {
			agentNID = id
		}
		return clusterupgrade.Node{
			ID: id, IsLeader: h.WritableLeaderConfirmed, BrokerVer: h.ReleaseVersion,
			AgentVer: agentRelease[agentNID], Voter: voter, CaughtUp: true,
		}
	}
	// External-review B1: the AUTHORITATIVE voter set is the leader's account-signed roster, NOT "who
	// answered the probe". A configured voter that is momentarily absent must fail the roll CLOSED — never
	// be silently dropped (dropping it can dip quorum, miscount the N=2 fence, or print complete while a
	// real voter is never upgraded). Only when there is NO roster (single mode / a pre-G3 broker) do we
	// fall back to the responders — there is no quorum to lose there.
	// A2: the signed roster is the SAFETY basis for a quorum-touching roll, but fetchManifestOverNATS
	// returns nil on ANY transient failure (timeout / no responder / decode). RETRY a few times before
	// conceding — a single blip must not silently drop to the fail-OPEN responder path below (which cannot
	// detect a momentarily-absent voter or miscount the N=2 fence). A genuine no-roster cluster (single
	// mode / pre-G3) returns nil on every attempt and correctly reaches the fallback.
	if m := fetchUpgradeRosterWithRetry(ctx, nc, actor); m != nil && m.Roster != nil && len(m.Roster.Brokers) > 0 {
		// External-review round2 B3: this roster is the SAFETY basis for a quorum-touching roll, but the fetch
		// path does not adopt/VerifyAt it. When the operator supplied the account seed, VERIFY the roster's
		// account signature + expiry against the pinned account pub before trusting it — never plan a roll over
		// an unverified/expired/wrong-account roster. (A dry-run without a seed still gets the bidirectional
		// responder cross-check below, which needs no key.)
		if accountPub != "" {
			if verr := clusterroster.VerifyAt(m.Roster, accountPub, time.Now()); verr != nil {
				return nil, unavailErr("the signed roster used to plan the roll failed verification (%v) — refusing to plan a quorum-touching upgrade over an unverified/expired roster", verr)
			}
		}
		rosterSet := map[string]bool{} // ALL roster brokers, ANY phase (voter/learner/draining/…)
		var voters, absent []string
		for _, b := range m.Roster.Brokers {
			rosterSet[b.NodeID] = true
			if b.Phase == proto.RosterPhaseVoter {
				voters = append(voters, b.NodeID)
				if _, ok := byNode[b.NodeID]; !ok {
					absent = append(absent, b.NodeID)
				}
			}
		}
		if len(absent) > 0 {
			return nil, unavailErr("configured voter(s) %v did not answer the cluster-health probe — cannot verify a quorum-safe roll (restore them or wait); refusing to infer the voter set from responders", absent)
		}
		// External-review round2 B3 / round3 B1: the roster manifest is served from a >=30s discovery cache, so
		// a RECENT grow can leave a broker answering cluster-health that the stale roster omits — the
		// manifest-only loop above would silently drop it from the plan (miss a real voter / miscount the N=2
		// fence). Cross-check the OTHER direction VERSION-AGNOSTICALLY: any broker that answered health but is
		// absent from the signed roster means the snapshot is stale → fail closed. This must NOT rely on the
		// additive IsVoter field — a pre-G5 voter answers health WITHOUT it (IsVoter decodes false), so keying
		// the check on IsVoter would let exactly that mixed-version voter slip through (round3 B1). Membership
		// is the roster's job; presence-in-roster is the invariant, phase/voterness is not the discriminator.
		var unexpected []string
		for id := range byNode {
			if id != "" && !rosterSet[id] {
				unexpected = append(unexpected, id)
			}
		}
		if len(unexpected) > 0 {
			return nil, unavailErr("broker(s) %v answered cluster-health but are absent from the signed roster snapshot (a recent membership change has not reached the roster cache yet) — refusing to plan over a stale roster; wait ~30s and re-run", unexpected)
		}
		nodes := make([]clusterupgrade.Node, 0, len(voters))
		for _, v := range voters {
			nodes = append(nodes, build(v, byNode[v], true))
		}
		return nodes, nil
	}
	// Fallback (no signed roster ⇒ single mode / a pre-G3 broker): plan over the responders. There is no
	// quorum to lose in the single/degraded case the authoritative-roster path does not cover. A2: this path
	// is fail-OPEN (no signature, no absent-voter guard), legitimately needed for a pre-G3 cluster that
	// answers health but has no roster responder — but WARN loudly when >1 broker answered, so an operator
	// whose roster fetch merely blipped is not silently planning a quorum-touching roll over responders.
	if len(replies) > 1 {
		_, _ = fmt.Fprintf(out, "  ⚠ WARNING: the signed cluster roster was unavailable — planning the roll over the %d broker(s) that answered the health probe. A momentarily-absent voter CANNOT be detected this way; verify every voter is present (`tether cluster status`) before proceeding, or retry.\n", len(replies))
	}
	seen := map[string]bool{}
	var nodes []clusterupgrade.Node
	for _, h := range replies {
		if h.NodeID == "" || seen[h.NodeID] {
			continue
		}
		seen[h.NodeID] = true
		nodes = append(nodes, build(h.NodeID, h, h.IsVoter))
	}
	anyVoter := false
	for _, n := range nodes {
		if n.Voter {
			anyVoter = true
			break
		}
	}
	if !anyVoter {
		for i := range nodes {
			nodes[i].Voter = true
		}
	}
	return nodes, nil
}

// fetchUpgradeRosterWithRetry pulls the signed cluster manifest, retrying a few times (A2). fetchManifest
// OverNATS collapses every transient failure (timeout / no responder / decode) to nil, so a single blip
// must not drop the quorum-touching roll to the fail-OPEN responder path. Returns nil only after the
// retries are exhausted — a genuine no-roster cluster (single mode / pre-G3) returns nil on every attempt.
func fetchUpgradeRosterWithRetry(ctx context.Context, nc *nats.Conn, actor string) *proto.ClusterManifest {
	for attempt := 0; attempt < 3; attempt++ {
		if m := fetchManifestOverNATS(ctx, nc, actor); m != nil && m.Roster != nil && len(m.Roster.Brokers) > 0 {
			return m
		}
		if attempt < 2 {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(400 * time.Millisecond):
			}
		}
	}
	return nil
}

// upgradeLockHeld probes cluster-health and reports whether ANY broker still advertises the cluster-scoped
// roll lock (External-review round2 B1). Used by the no-op re-run path to detect a stale lock left by a
// prior roll whose release-lock did not confirm.
func upgradeLockHeld(nc *nats.Conn, actor string) bool {
	for _, h := range probeClusterHealth(nc, actor) {
		if h.UpgradeLockActive {
			return true
		}
	}
	return false
}

func renderUpgradePlan(w interface{ Write([]byte) (int, error) }, plan clusterupgrade.Plan, target string) {
	_, _ = fmt.Fprintf(w, "rolling upgrade plan → %s (%d host(s) to upgrade):\n", target, plan.Upgrades())
	for _, s := range plan.Steps {
		switch s.Kind {
		case clusterupgrade.StepSkip:
			_, _ = fmt.Fprintf(w, "  SKIP    %s (already at target)\n", s.NodeID)
		case clusterupgrade.StepUpgrade:
			if s.TransferTo != "" {
				_, _ = fmt.Fprintf(w, "  UPGRADE %s (leader — transfer to %s first)\n", s.NodeID, s.TransferTo)
			} else {
				_, _ = fmt.Fprintf(w, "  UPGRADE %s\n", s.NodeID)
			}
		}
	}
	if plan.N2WriteFence {
		_, _ = fmt.Fprintln(w, "  ⚠ N=2: each restart fences writes for its restart duration (inherent F=0); production should run N>=3.")
	}
	for _, r := range plan.Refused {
		_, _ = fmt.Fprintf(w, "  ✗ REFUSED: %s\n", r)
	}
}

// signUpgradeTrigger stamps IssuedAt + the account signature onto a request and marshals it.
func signUpgradeTrigger(seed []byte, req *proto.ClusterUpgradeReq, now time.Time) ([]byte, error) {
	req.IssuedAt = now.UTC().Format(time.RFC3339)
	sig, err := auth.SignWithSeed(seed, proto.CanonicalUpgradeReqBytes(req))
	if err != nil {
		return nil, err
	}
	req.Sig = hex.EncodeToString(sig)
	return json.Marshal(req)
}

// upgradeTriggerCodeNotReady mirrors the broker's retriable "admin backend not wired yet" code.
const upgradeTriggerCodeNotReady = "cluster_not_ready"

// sendUpgradeTrigger signs + publishes a trigger and awaits the target broker's reply, RETRYING the
// retriable cluster_not_ready (External-review M1: a broker that just re-exec'd answers health before its
// admin backend wires — wait it out rather than HALT the roll). Bounded by upgradeConvergeTimeout.
func sendUpgradeTrigger(ctx context.Context, nc *nats.Conn, actor string, seed []byte, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
	deadline := time.Now().Add(upgradeConvergeTimeout)
	for {
		resp, err := sendUpgradeTriggerOnce(ctx, nc, actor, seed, req)
		if err != nil || resp.Code != upgradeTriggerCodeNotReady || time.Now().After(deadline) {
			return resp, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(upgradeConvergePoll):
		}
	}
}

func sendUpgradeTriggerOnce(ctx context.Context, nc *nats.Conn, actor string, seed []byte, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
	data, err := signUpgradeTrigger(seed, req, time.Now())
	if err != nil {
		return nil, err
	}
	rctx, cancel := context.WithTimeout(ctx, upgradeTriggerTimeout)
	defer cancel()
	msg, err := nc.RequestWithContext(rctx, proto.SubjCtrlClusterUpgrade(actor), data)
	if err != nil {
		return nil, unavailErr("trigger %s/%s: %v (broker unreachable, or too old to have the responder)", req.Op, req.TargetNode, err)
	}
	var resp proto.ClusterUpgradeResp
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return nil, fmt.Errorf("decode trigger reply: %w", err)
	}
	return &resp, nil
}
