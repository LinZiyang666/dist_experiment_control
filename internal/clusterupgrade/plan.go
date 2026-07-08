// Package clusterupgrade holds the PURE, IO-free planning logic for `tether cluster upgrade` (G5 #13/#14).
// The orchestrator (cmd/tether) folds live cluster state into []Node and consumes the ordered Plan;
// keeping the ordering + refusal logic here makes it adversarially table-testable without a raft/systemd
// harness. This package computes the ROLL ORDER only — the three-axis wire-compat gate (proto/command/
// schema) is a canary LIVE-read the orchestrator performs after upgrading the first host, not a plan-time
// input (the target's axes are unknown until its binary runs).
package clusterupgrade

import "sort"

// Node is one broker host's pre-roll view — LIVE self-reports folded ctl-side (no IO here).
type Node struct {
	ID        string
	IsLeader  bool
	BrokerVer string // the broker daemon's running release
	AgentVer  string // the co-located agent's running release ("" = unknown/unpaired → treated as not-at-target)
	Voter     bool   // phase == VOTER (only voters gate quorum and can take a leadership transfer)
	CaughtUp  bool   // AppliedLag==0 && streams-at-target && reachable && !inconsistent
}

// AtTarget is the whole-host #19 criterion knowable pre-canary: BOTH the broker daemon AND its co-located
// agent are running the target release. A broker at target whose agent is stale is NOT done (the #19
// half-upgrade the dual-version view exists to catch).
func (n Node) AtTarget(target string) bool {
	return target != "" && n.BrokerVer == target && n.AgentVer == target
}

// StepKind is one roll action.
type StepKind string

const (
	StepSkip    StepKind = "skip"    // already at target — nothing to do
	StepUpgrade StepKind = "upgrade" // stage + reload this host (leader-last hosts carry a TransferTo)
)

// Step is one ordered roll action.
type Step struct {
	Kind       StepKind
	NodeID     string
	TransferTo string // non-empty ⇒ transfer leadership HERE before upgrading NodeID (the leader-last case)
}

// Plan is the ordered roll + any fatal refusal + the N=2 honesty flag.
type Plan struct {
	Steps        []Step
	Refused      []string // non-empty ⇒ the whole roll MUST NOT proceed (HALT, restore HA first)
	N2WriteFence bool     // exactly 2 voters ⇒ a restart fences writes regardless of transfer-leader (inherent F=0)
}

// Upgrades counts the non-skip steps (how many hosts the roll will actually touch).
func (p Plan) Upgrades() int {
	n := 0
	for _, s := range p.Steps {
		if s.Kind == StepUpgrade {
			n++
		}
	}
	return n
}

// Compute orders the roll: SKIP hosts already at target, UPGRADE the rest FOLLOWERS-FIRST / LEADER-LAST,
// and attach the leader's transfer target (a caught-up staying voter — since followers upgrade first it is
// already at target by the time the leader's turn comes). Deterministic (sort.SliceStable by ID). Refuses
// if the leader must upgrade but no OTHER caught-up voter can take leadership (restart HA first).
func Compute(nodes []Node, target string) Plan {
	sorted := append([]Node(nil), nodes...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	var p Plan
	voters := 0
	for _, n := range sorted {
		if n.Voter {
			voters++
		}
	}
	p.N2WriteFence = voters == 2

	// External-review M2: a cluster-health broadcast can collect replies ACROSS a leadership transition,
	// so two brokers may both self-report IsLeader in one folded snapshot. Fail CLOSED — silently keeping
	// one leader would skip a broker and transfer leadership to an un-upgraded target.
	leaderCount := 0
	for _, n := range sorted {
		if n.IsLeader {
			leaderCount++
		}
	}
	if leaderCount > 1 {
		p.Refused = append(p.Refused,
			"ambiguous snapshot: more than one broker self-reports as the writable leader (a leadership transition spanned the probe window) — wait for leadership to stabilize, then re-run")
		return p
	}

	var leader *Node
	for i := range sorted {
		n := sorted[i]
		if n.AtTarget(target) {
			p.Steps = append(p.Steps, Step{Kind: StepSkip, NodeID: n.ID})
			continue
		}
		if n.IsLeader {
			ld := n
			leader = &ld
			continue // leader is appended LAST, below
		}
		p.Steps = append(p.Steps, Step{Kind: StepUpgrade, NodeID: n.ID})
	}

	if leader != nil {
		transferTo := ""
		if voters > 1 {
			for _, n := range sorted {
				if n.ID != leader.ID && n.Voter && n.CaughtUp {
					transferTo = n.ID
					break
				}
			}
			if transferTo == "" {
				p.Refused = append(p.Refused,
					"the leader "+leader.ID+" must upgrade but no other caught-up voter can take leadership — restore HA (a caught-up second voter) first")
				return p
			}
		}
		p.Steps = append(p.Steps, Step{Kind: StepUpgrade, NodeID: leader.ID, TransferTo: transferTo})
	}
	return p
}
