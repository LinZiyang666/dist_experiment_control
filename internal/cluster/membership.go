package cluster

import (
	"fmt"

	"github.com/hashicorp/raft"
)

// membership.go — D7 §8.1 raft config-change wrappers. These are raft CONFIG
// changes (AddVoter/RemoveServer/LeadershipTransfer), NOT FSM Propose ops: they
// are not interceptable by FSM.Apply, so the join PoP is verified on the BUSINESS
// op (OpClusterNodeUpsert) which the orchestrator commits FIRST (§8.1 phase 1)
// before calling AddVoter (phase 2). All take/return PLAIN STRINGS, never
// raft.ServerID/ServerAddress, so internal/broker (the orchestrator) never imports
// hashicorp/raft — raft stays confined to internal/cluster (L-2).

// ServerInfo is a raft-free projection of one raft.Configuration server, so the
// broker can render `role` (status/doctor) and the orchestrator can check
// membership idempotency without importing raft.
type ServerInfo struct {
	NodeID string
	Addr   string
	Voter  bool // Suffrage == Voter (vs Nonvoter/Staging)
	Leader bool // this server is the current raft leader
}

// AddVoter adds (or, idempotently, re-affirms) a voter to the raft configuration.
// Leader-only. It is the §8.1 PHASE-2 action, called by the orchestrator ONLY
// after the OpClusterNodeUpsert roster row (phase 1) is committed. raft.AddVoter
// is idempotent by node_id (an already-present voter is a no-op), so a retry — even
// across a leadership change that re-runs the orchestrator — is safe.
func (n *Node) AddVoter(nodeID, raftAddr string) error {
	if n.raft.Load().State() != raft.Leader {
		return raft.ErrNotLeader
	}
	fut := n.raft.Load().AddVoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, n.applyTimeout)
	if err := fut.Error(); err != nil {
		return fmt.Errorf("cluster: add voter %s: %w", nodeID, err)
	}
	return nil
}

// AddNonvoter adds (or idempotently re-affirms) a NON-VOTER to the raft configuration.
// Leader-only. A nonvoter receives the replicated log and catches up but does NOT count toward
// quorum, so staging a joiner as a nonvoter FIRST means an UNREACHABLE peer can never wedge the
// cluster: the config-change entry commits at the OLD quorum, the peer simply never catches up,
// and it stays online-RemoveServer-able. The join controller promotes it to a Voter via AddVoter
// once it is caught up (§8.1 / v0.4.2 phase-fluidity wedge-prevention — the keystone fix).
// Idempotent by node_id (re-affirming an already-present server is a no-op).
func (n *Node) AddNonvoter(nodeID, raftAddr string) error {
	if n.raft.Load().State() != raft.Leader {
		return raft.ErrNotLeader
	}
	fut := n.raft.Load().AddNonvoter(raft.ServerID(nodeID), raft.ServerAddress(raftAddr), 0, n.applyTimeout)
	if err := fut.Error(); err != nil {
		return fmt.Errorf("cluster: add nonvoter %s: %w", nodeID, err)
	}
	return nil
}

// RemoveServer removes a server from the raft configuration. Leader-only. The §8.1
// removal ORDER is roster ClusterNodePhase(RETIRING) -> RemoveServer -> roster
// ClusterNodeRemove; this is the middle step. Idempotent (removing an absent server
// is a no-op).
func (n *Node) RemoveServer(nodeID string) error {
	if n.raft.Load().State() != raft.Leader {
		return raft.ErrNotLeader
	}
	fut := n.raft.Load().RemoveServer(raft.ServerID(nodeID), 0, n.applyTimeout)
	if err := fut.Error(); err != nil {
		return fmt.Errorf("cluster: remove server %s: %w", nodeID, err)
	}
	return nil
}

// LeadershipTransferToServer hands leadership to a SPECIFIC caught-up voter (drain
// step: transfer-leader off a node before removing it). Leader-only. Distinct from
// TransferLeadership (which lets raft pick any follower).
func (n *Node) LeadershipTransferToServer(nodeID, raftAddr string) error {
	if n.raft.Load().State() != raft.Leader {
		return raft.ErrNotLeader
	}
	fut := n.raft.Load().LeadershipTransferToServer(raft.ServerID(nodeID), raft.ServerAddress(raftAddr))
	if err := fut.Error(); err != nil {
		return fmt.Errorf("cluster: transfer leadership to %s: %w", nodeID, err)
	}
	return nil
}

// RaftConfiguration returns the committed raft configuration as raft-free
// ServerInfo (status `role`, membership idempotency, the leader-startup
// reconciliation pass). Generalizes NumVoters' GetConfiguration. Marks the leader
// via LeaderWithID (empty mid-election).
func (n *Node) RaftConfiguration() ([]ServerInfo, error) {
	fut := n.raft.Load().GetConfiguration()
	if err := fut.Error(); err != nil {
		return nil, fmt.Errorf("cluster: get configuration: %w", err)
	}
	_, leaderID := n.raft.Load().LeaderWithID()
	servers := fut.Configuration().Servers
	out := make([]ServerInfo, 0, len(servers))
	for _, s := range servers {
		out = append(out, ServerInfo{
			NodeID: string(s.ID),
			Addr:   string(s.Address),
			Voter:  s.Suffrage == raft.Voter,
			Leader: s.ID == leaderID,
		})
	}
	return out, nil
}

// LeaderWithID returns the current raft leader's (addr, node_id) as plain strings,
// both empty mid-election. The non-leader fail-fast (§8.1) uses node_id to look up
// the leader's public_host in the roster and tell the operator where to re-run.
func (n *Node) LeaderWithID() (addr, nodeID string) {
	a, id := n.raft.Load().LeaderWithID()
	return string(a), string(id)
}
