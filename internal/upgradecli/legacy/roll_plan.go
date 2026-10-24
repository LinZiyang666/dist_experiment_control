package legacy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/clusterupgrade"
	"github.com/LinZiyang666/tether/internal/proto"
)

// The legacy (S6) `cluster upgrade` planner moved verbatim from
// cmd/tether/cluster_upgrade.go with its comments (hot-update S8 plan §11.2
// P19 slim move). cmd keeps buildUpgradeNodes/renderUpgradePlan wrappers with
// the original signatures; transport and error-class constructors arrive
// through RollPlanIO so this leaf neither dials NATS nor owns exit classes.

// RollPlanIO is the planner's view of the control plane.
type RollPlanIO struct {
	Request     RequestFunc
	Probe       func() ([]proto.ClusterHealthResp, error)
	Manifest    func(context.Context) *proto.ClusterManifest
	Unavailable func(format string, a ...any) error
}

// BuildLegacyRollNodes (cmd: buildUpgradeNodes) folds the live cluster-health probe (broker daemons: VER + leader + co-located agent
// nid) with the node list (agent RELEASEs) into the planner's []Node. A broker that answers the health
// probe is a current voter (the observe target IS the voter roster); CaughtUp is approximated true here and
// re-verified live by the per-host converge wait during the roll.
func BuildLegacyRollNodes(ctx context.Context, rio RollPlanIO, actor, sid, accountPub string, out io.Writer) ([]clusterupgrade.Node, error) {
	unavailErr := rio.Unavailable
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
	data, rerr := rio.Request(rctx, proto.SubjCtrlNodeList(actor, sid), body)
	cancel()
	if rerr != nil {
		return nil, unavailErr("node-list RPC failed (%v) — cannot determine co-located agent versions; refusing to plan a roll on incomplete data (every host would falsely look stale)", rerr)
	}
	var nlResp proto.NodeListResp
	if uerr := json.Unmarshal(data, &nlResp); uerr != nil {
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
	replies, err := rio.Probe()
	if err != nil {
		// Distinct from "nobody answered": the roll planner has learned nothing at all,
		// and the message below would send an operator to look at brokers that are fine.
		return nil, unavailErr("could not ask the cluster for its health: %v", err)
	}
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
		agentNID, presence := ResolveColocatedAgent(id, h, agentRelease)
		if presence == clusterupgrade.AgentAbsent {
			// LOUD, never silent: the operator must be able to see why this host's verdict is
			// broker-only. A host that DOES run an agent but under another session would show up
			// here, and this line is how they notice.
			_, _ = fmt.Fprintf(out, "  note: %s runs no co-located tether agent (none declared via `serve --colocated-agent-nid`, none registered under nid %q in this session) — its upgrade verdict is broker-only.\n", id, id)
		}
		return clusterupgrade.Node{
			ID: id, IsLeader: h.WritableLeaderConfirmed, BrokerVer: h.ReleaseVersion,
			AgentVer: agentRelease[agentNID], Agent: presence, Voter: voter, CaughtUp: true,
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
	if m := FetchRollRosterWithRetry(ctx, rio.Manifest); m != nil && m.Roster != nil && len(m.Roster.Brokers) > 0 {
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

// ResolveColocatedAgent (cmd: resolveColocatedAgent) answers P3's three-state question for ONE broker host: which nid is its
// co-located agent, and does it have one at all? agentRelease is keyed by every nid the session's node
// list returned.
//
// The two branches are deliberately asymmetric, because the evidence is:
//
//   - DECLARED (`serve --colocated-agent-nid <nid>`): presence is a CONFIGURATION fact, so it holds even
//     when that agent is currently down or absent from the node list. Such a host stays state (b) —
//     not-at-target — and the roll HALTs loudly on it. An operator who declared an agent gets told when
//     it cannot be upgraded; we never "helpfully" downgrade their host to broker-only.
//
//   - UNDECLARED: the only signal is the node_id==nid convention, so presence must be OBSERVED. The node
//     list is the right observer — handleNodeListReq projects every node EVER registered in the session
//     (Status ONLINE | STALE | OFFLINE), so an agent that merely stopped is still listed and still
//     counts as PRESENT. Absence from that list therefore means "no such agent has ever registered
//     here", i.e. state (c): the dedicated broker host.
func ResolveColocatedAgent(brokerID string, h proto.ClusterHealthResp, agentRelease map[string]string) (string, clusterupgrade.AgentPresence) {
	if h.ColocatedAgentNID != "" {
		return h.ColocatedAgentNID, clusterupgrade.AgentPresent
	}
	if _, ok := agentRelease[brokerID]; ok {
		return brokerID, clusterupgrade.AgentPresent
	}
	return "", clusterupgrade.AgentAbsent
}

// FetchRollRosterWithRetry (cmd: fetchUpgradeRosterWithRetry) pulls the signed cluster manifest, retrying a few times (A2). fetchManifest
// OverNATS collapses every transient failure (timeout / no responder / decode) to nil, so a single blip
// must not drop the quorum-touching roll to the fail-OPEN responder path. Returns nil only after the
// retries are exhausted — a genuine no-roster cluster (single mode / pre-G3) returns nil on every attempt.
func FetchRollRosterWithRetry(ctx context.Context, fetch func(context.Context) *proto.ClusterManifest) *proto.ClusterManifest {
	for attempt := 0; attempt < 3; attempt++ {
		if m := fetch(ctx); m != nil && m.Roster != nil && len(m.Roster.Brokers) > 0 {
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

// RenderLegacyRollPlan (cmd: renderUpgradePlan) prints the ordered S6 roll.
func RenderLegacyRollPlan(w io.Writer, plan clusterupgrade.Plan, target string) {
	_, _ = fmt.Fprintf(w, "rolling upgrade plan → %s (%d host(s) to upgrade):\n", target, plan.Upgrades())
	for _, s := range plan.Steps {
		switch s.Kind {
		case clusterupgrade.StepSkip:
			_, _ = fmt.Fprintf(w, "  SKIP    %s (already at target)\n", s.NodeID)
		case clusterupgrade.StepUpgrade:
			scope := ""
			if s.BrokerOnly {
				scope = " [broker-only — no co-located agent on this host]"
			}
			if s.TransferTo != "" {
				_, _ = fmt.Fprintf(w, "  UPGRADE %s (leader — transfer to %s first)%s\n", s.NodeID, s.TransferTo, scope)
			} else {
				_, _ = fmt.Fprintf(w, "  UPGRADE %s%s\n", s.NodeID, scope)
			}
		}
	}
	_, _ = fmt.Fprintf(w, "  scope: %s\n", plan.Scope)
	if plan.N1WriteStall {
		_, _ = fmt.Fprintln(w, "  N=1: writes briefly pause while the sole voter reloads (budget <=5s).")
	}
	if plan.N2WriteFence {
		_, _ = fmt.Fprintln(w, "  ⚠ N=2: each restart fences writes for its restart duration (inherent F=0); production should run N>=3.")
	}
	for _, r := range plan.Refused {
		_, _ = fmt.Fprintf(w, "  ✗ REFUSED: %s\n", r)
	}
}

// origin: S10 internal review MAKETEST-GATES F4
// upgradeLockHeld probes cluster-health and reports whether ANY broker still advertises the cluster-scoped
// roll lock (External-review round2 B1). Used by the no-op re-run path to detect a stale lock left by a
// prior roll whose release-lock did not confirm.
//
// A PROBE THAT COULD NOT BE MADE REPORTS "held", not "clear" — origin: increment 2
// internal review, pairing-sweep/IMPACT-F1. The caller's only reaction to `true` is to
// TELL THE OPERATOR the lock may be stale and how to clear it; its reaction to `false` is
// to print "nothing to do" while membership may still be blocked. Between a spurious
// warning and a silent block, the warning is the one an operator can act on.
func UpgradeLockHeld(probe func() ([]proto.ClusterHealthResp, error)) bool {
	replies, err := probe()
	if err != nil {
		return true
	}
	for _, h := range replies {
		if h.UpgradeLockActive {
			return true
		}
	}
	return false
}

// External-review round2 B1: a prior roll that COMPLETED but whose release-lock did not confirm
// leaves cluster_upgrade_active set — and since every host is now at target, the roll loop that
// would clear it never runs. Detect that stale lock here and clear it (a reachable self-heal, not
// an undocumented manual path). Needs the account seed to sign release-lock; a pure dry-run/no-seed
// preview never set a lock, so skipping the clear there is correct.
// External-review round3 doubt: clearing needs the account seed to SIGN release-lock. If the
// operator re-ran WITHOUT --account-seed, do NOT print a bare "nothing to do" while membership
// stays blocked — tell them exactly how to clear it.
