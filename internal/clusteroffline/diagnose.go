package clusteroffline

import "net"

// diagnose.go — B7 DOC#7: the read-only diagnose helpers behind `cluster force-single --guided` /
// `recover --guided`. They auto-derive the peer list + TCP-probe each peer's serving ports (the
// #1 hand-transcription footgun), so the guided front-end can PRINT the exact command + block on a
// still-alive peer BEFORE anything irreversible is typed. They add NO disk-mutating path — the
// unchanged ForceSingle/checkPeersDead/confirmTypedNodeID gates stay the only mutating path.

// PeerDiag is one peer's liveness diagnosis.
type PeerDiag struct {
	NodeID  string
	Alive   bool   // a TCP connection COMPLETED on at least one serving port (conservative — alive)
	AliveOn string // "raft@addr" / "nats@addr" / "tunnel@addr" that answered, or "" if dead
}

// DiagnosePeers reads the on-disk roster (RO) and TCP-probes every non-self peer's raft/nats/tunnel
// ports (the same conservative probe checkPeersDead HARD-enforces). It returns one PeerDiag per
// peer; Alive=true means at least one port completed a TCP connection (so force-single there would
// split-brain). It NEVER mutates.
func DiagnosePeers(dbPath, selfID string) ([]PeerDiag, error) {
	roster, err := readRoster(dbPath, selfID)
	if err != nil {
		return nil, err
	}
	out := make([]PeerDiag, 0, len(roster))
	for _, p := range roster {
		d := PeerDiag{NodeID: p.NodeID}
		for _, ap := range []struct{ kind, addr string }{
			{"raft", p.RaftAddr}, {"nats", p.NatsRoute}, {"tunnel", p.TunnelAddr},
		} {
			if ap.addr == "" {
				continue
			}
			conn, derr := net.DialTimeout("tcp", ap.addr, peerDialTimeout)
			if derr == nil {
				_ = conn.Close()
				d.Alive = true
				d.AliveOn = ap.kind + "@" + ap.addr
				break
			}
		}
		out = append(out, d)
	}
	return out, nil
}

// DeadPeerIDs returns the node_ids of every peer (alive or dead) — the value an operator must pass
// to `force-single --confirm-peers-dead` (it must enumerate EVERY peer; the gate hard-refuses a
// still-alive one). Derived so the operator never hand-transcribes the list.
func DeadPeerIDs(diags []PeerDiag) []string {
	ids := make([]string, 0, len(diags))
	for _, d := range diags {
		ids = append(ids, d.NodeID)
	}
	return ids
}

// AnyAlive reports whether any peer answered a probe (force-single must be BLOCKED).
func AnyAlive(diags []PeerDiag) bool {
	for _, d := range diags {
		if d.Alive {
			return true
		}
	}
	return false
}
