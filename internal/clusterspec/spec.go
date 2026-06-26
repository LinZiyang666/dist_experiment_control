// Package clusterspec is the B7 AUTO#9 desired-topology differ: it parses a `roster.yaml`
// (the operator's intended cluster membership) and diffs it against the live roster to render a
// QUORUM-SAFE, topologically-ordered convergence PLAN as the existing `cluster` verbs. It is a
// pure leaf — no nats, no raft, no mutation — so `cluster apply` is plan-only (execution is a
// deferred managed-convergence increment). The YAML schema is kept additive so a future executor
// can consume the same file.
//
// NOTE on this package name vs internal/clusterroster: DOC#3's clusterroster is the *signed
// broker-set an agent trusts*; this clusterspec is the *operator's desired-topology YAML*. Two
// distinct concepts, two distinct packages (the Stage-A byte-equiv critique caught the collision).
package clusterspec

import (
	"fmt"
	"sort"

	"github.com/LinZiyang666/tether/internal/proto"
	"gopkg.in/yaml.v3"
)

// Spec is the parsed roster.yaml.
type Spec struct {
	Nodes []NodeSpec `yaml:"nodes"`
}

// NodeSpec is one desired node. Desired is "voter" (should be a member) or "absent" (should be
// removed). Empty Desired defaults to "voter".
type NodeSpec struct {
	NodeID     string `yaml:"node_id"`
	RaftAddr   string `yaml:"raft_addr"`
	NatsRoute  string `yaml:"nats_route"`
	TunnelAddr string `yaml:"tunnel_addr"`
	CertFP     string `yaml:"cert_fp"`
	Desired    string `yaml:"desired"`
}

// LiveNode is the live roster view (projected from the cluster status report).
type LiveNode struct {
	NodeID string
	Phase  string
	Role   string // leader | voter | learner | ""
}

// Step is one convergence action, rendered as an existing verb. Order is the quorum-safe rank
// (lower runs first): adds (0) → leader transfer off a retiring leader (5) → retires (9).
type Step struct {
	Order  int
	Verb   string // the exact `tether cluster …` line (or a "→ requires …" note)
	NodeID string
	Reason string
}

// Parse reads a roster.yaml.
func Parse(data []byte) (*Spec, error) {
	var s Spec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("clusterspec: parse: %w", err)
	}
	seen := map[string]bool{}
	for i, n := range s.Nodes {
		if n.NodeID == "" {
			return nil, fmt.Errorf("clusterspec: nodes[%d] missing node_id", i)
		}
		// Audit SEC-MAJOR-1: a node_id is rendered verbatim into printed copy-paste command lines —
		// validate the charset fail-closed (no shell-metachars / newlines / path separators).
		if err := proto.ValidateClusterNodeID(n.NodeID); err != nil {
			return nil, fmt.Errorf("clusterspec: nodes[%d] %v", i, err)
		}
		if seen[n.NodeID] { // Stage-C m10: a repeated node_id (a typo) would silently last-win / dup-add
			return nil, fmt.Errorf("clusterspec: duplicate node_id %q", n.NodeID)
		}
		seen[n.NodeID] = true
		if n.Desired == "" {
			s.Nodes[i].Desired = "voter"
		}
		if d := s.Nodes[i].Desired; d != "voter" && d != "absent" {
			return nil, fmt.Errorf("clusterspec: nodes[%d] (%s) desired=%q must be voter|absent", i, n.NodeID, d)
		}
	}
	return &s, nil
}

// Diff renders the quorum-safe, topologically-ordered convergence plan. It NEVER mutates — every
// step is a printed verb the operator runs (C8: must be a command that EXISTS). A node desired=voter
// but absent from the live roster → `cluster join prepare`/`approve` (admission needs the joiner's
// self-signed PoP, which a YAML row cannot produce, so unattended auto-join is code-forced out of
// scope). A live node desired=absent → `cluster retire`, ordered LAST and refusing the last voter;
// a retiring leader gets a transfer-leader step first.
func Diff(spec *Spec, live []LiveNode, leaderID string) []Step {
	liveByID := map[string]LiveNode{}
	for _, n := range live {
		liveByID[n.NodeID] = n
	}
	desired := map[string]NodeSpec{}
	for _, n := range spec.Nodes {
		desired[n.NodeID] = n
	}

	var steps []Step
	// Adds: desired=voter but not a live member.
	for _, n := range spec.Nodes {
		if n.Desired != "voter" {
			continue
		}
		if _, ok := liveByID[n.NodeID]; ok {
			continue // already present
		}
		steps = append(steps, Step{
			Order: 0, NodeID: n.NodeID, Reason: "desired voter, not in the cluster",
			Verb: fmt.Sprintf("tether cluster join prepare --node-id %s …   # on %s, then on the leader: tether cluster join approve <bundle> --wait", n.NodeID, n.NodeID),
		})
	}

	// Retires: live members that are desired=absent OR not in the spec at all.
	retires := []string{}
	for _, n := range live {
		spc, inSpec := desired[n.NodeID]
		if inSpec && spc.Desired == "voter" {
			continue // keep
		}
		retires = append(retires, n.NodeID)
	}
	sort.Strings(retires)
	retireSet := map[string]bool{}
	for _, id := range retires {
		retireSet[id] = true
	}
	isVoter := func(id string) bool { return liveByID[id].Role == "voter" || liveByID[id].Role == "leader" }
	// Stage-C M2: count ONLY actual voters — a learner / INCONSISTENT (Role=="") retire touches no quorum.
	liveVoters := 0
	for _, n := range live {
		if isVoter(n.NodeID) {
			liveVoters++
		}
	}
	// Stage-C M4: an add is NOT a voter until it completes the out-of-band PoP join — DON'T credit
	// pending adds against the quorum floor.
	pendingAdds := 0
	for _, n := range spec.Nodes {
		if n.Desired == "voter" {
			if _, ok := liveByID[n.NodeID]; !ok {
				pendingAdds++
			}
		}
	}
	// resolveSurvivingVoter picks a real voter that is staying — the concrete transfer-leader target
	// (Stage-C M3: never the literal "<another-voter>").
	resolveSurvivingVoter := func() string {
		for _, n := range live {
			if n.NodeID != leaderID && !retireSet[n.NodeID] && isVoter(n.NodeID) {
				return n.NodeID
			}
		}
		return ""
	}

	for _, id := range retires {
		// Stage-C M2: a non-voter (learner / INCONSISTENT) retire is plain roster cleanup — no quorum impact.
		if !isVoter(id) {
			// External-review F12: `cluster remove` is accepted by the backend ONLY for a RETIRING or
			// VOTER_ADD_FAILED node. A mid-join non-voter (CATCHING_UP / JOIN_VERIFIED_PENDING_VOTER) or
			// an INCONSISTENT row would have its `cluster remove` REJECTED — printing an un-executable
			// command is worse than an explicit diagnostic, so emit phase-appropriate remediation.
			switch liveByID[id].Phase {
			case "RETIRING", "VOTER_ADD_FAILED":
				steps = append(steps, Step{
					Order: 9, NodeID: id, Reason: "finish a RETIRING / VOTER_ADD_FAILED node (no quorum impact)",
					Verb: fmt.Sprintf("tether cluster recovery node remove %s --manual   # roster cleanup — %s is %s", id, id, liveByID[id].Phase),
				})
			default:
				steps = append(steps, Step{
					Order: 9, NodeID: id, Reason: fmt.Sprintf("DIAGNOSE — %s is %s (a non-voter mid-join / inconsistent row); a bare `recovery node remove` is refused for this phase", id, liveByID[id].Phase),
					Verb: fmt.Sprintf("tether cluster doctor   # inspect %s (phase %s); let it reach VOTER then `cluster retire %s`, or recover the inconsistency — do NOT bare-remove", id, liveByID[id].Phase, id),
				})
			}
			continue
		}
		projected := liveVoters - 1
		// Stage-C M3/M4: decide the refusal FIRST; only emit the leader-transfer step when the retire
		// actually proceeds (else the operator runs a transfer for a retire that is then refused).
		if projected < 1 {
			steps = append(steps, Step{
				Order: 9, NodeID: id, Reason: "REFUSED — retiring the last voter would destroy the cluster (add a replacement first)",
				Verb: fmt.Sprintf("# REFUSED: %s is the last voter; retiring it leaves no cluster.", id),
			})
			continue
		}
		if pendingAdds > 0 && projected < 2 {
			steps = append(steps, Step{
				Order: 9, NodeID: id, Reason: "REFUSED — would drop below 2 voters while adds are still pending",
				Verb: fmt.Sprintf("# REFUSED: complete the pending join(s) (cluster join approve, confirm VOTER), then re-run apply before retiring %s below 2 voters.", id),
			})
			continue
		}
		if id == leaderID {
			target := resolveSurvivingVoter()
			if target == "" {
				// Audit QS-MAJOR-1: no surviving voter to hand leadership to (the only other voter is
				// also being retired). Emit a single REFUSED step — NOT a placeholder transfer + drain
				// the executor would reject (the Stage-C M3 fix must hold for two real voters too).
				steps = append(steps, Step{
					Order: 9, NodeID: id, Reason: "REFUSED — retiring the leader leaves no surviving voter to hand off to (add a replacement first, or this is force-single territory)",
					Verb: fmt.Sprintf("# REFUSED: %s is the leader and no other voter survives this plan — cannot transfer leadership.", id),
				})
				continue
			}
			steps = append(steps, Step{
				Order: 5, NodeID: id, Reason: "retiring node is the leader — transfer leadership first",
				Verb: fmt.Sprintf("tether cluster transfer-leader %s --wait   # %s is the leader; hand off before retiring it", target, id),
			})
		}
		steps = append(steps, Step{
			Order: 9, NodeID: id, Reason: "live voter, desired absent",
			Verb: fmt.Sprintf("tether cluster retire %s   # migrate exposes off + remove (resumable)", id),
		})
		liveVoters-- // projected voter count after this retire (so a later retire sees the right floor)
	}

	sort.SliceStable(steps, func(i, j int) bool { return steps[i].Order < steps[j].Order })
	return steps
}
