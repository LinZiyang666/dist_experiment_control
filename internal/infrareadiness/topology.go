// Package infrareadiness observes infrastructure without changing its placement.
// The host census, configuration fence and ordinary auth owners are independent
// bindings: a successful LIST or CONNECT cannot stand in for any of those owners.
// origin: hot-update S9 plan §3.2 / T24
package infrareadiness

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"sort"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infracontract"
)

type Broker struct {
	NodeID string
	Host   string
	Voter  bool
}

// Host is the physical host identity from the census, never a server name or URL.
// Version is observed from INFO/SYS and checked against the running executable by
// the census producer. Disk version is deliberately absent from this interface.
type Server struct {
	ID        string
	Name      string
	Host      string
	Version   string
	Running   infracontract.Process
	JetStream bool
	Online    bool
}

type Topology struct {
	Revision   uint64
	Brokers    []Broker
	Servers    []Server
	Standalone bool
	Complete   bool
}

type Member struct {
	ServerID string
	Current  bool
	Online   bool
	Lag      uint64
}

// ID is the observed Raft group identity. Unreplicated assets instead bind the
// observed owner and creation identity; names alone do not survive delete/create.
type Group struct {
	ID           string
	Account      string
	Stream       string
	Consumer     string
	CreatedNS    int64
	ConfigSHA256 infracontract.Digest
	Replicas     int
	Leader       string
	Members      []Member
}

type TopologySource interface {
	ReadTopology(infracontract.Plan, infracontract.Deadline, infracontract.Stop) (Topology, error)
}

type InventorySource interface {
	ReadInventory(Topology, infracontract.Deadline, infracontract.Stop) (Snapshot, error)
}

// ReadSeal must come from the joined, closed configuration admission gate. Two
// equal inventories alone cannot exclude an unregistered external writer.
type SealSource interface {
	ReadSeal(infracontract.Plan, infracontract.Deadline, infracontract.Stop) (infracontract.AssetSeal, error)
}

type Probe struct {
	topology  TopologySource
	inventory InventorySource
	seal      SealSource
	auth      AuthSource
}

var _ infracontract.ReadyProbe = (*Probe)(nil)

func New(topology TopologySource, inventory InventorySource, seal SealSource, auth AuthSource) (*Probe, error) {
	if topology == nil || inventory == nil || seal == nil || auth == nil {
		return nil, infracontract.ErrIncomplete
	}
	return &Probe{topology: topology, inventory: inventory, seal: seal, auth: auth}, nil
}

// Read publishes no partial success. Every stage consumes the same deadline and
// the final fence revision still has to match after all network evidence is read.
// origin: hot-update S9 plan §3.2–§3.3 / T24 / T25 / T26
func (p *Probe) Read(plan infracontract.Plan, deadline infracontract.Deadline, stop infracontract.Stop) (infracontract.Readiness, error) {
	if p == nil {
		return infracontract.Readiness{}, infracontract.ErrIncomplete
	}
	if plan.Component != infracontract.NATS {
		return infracontract.Readiness{}, infracontract.ErrUnsupported
	}
	if err := admission(deadline, stop); err != nil {
		return infracontract.Readiness{}, err
	}
	before, err := p.seal.ReadSeal(plan, deadline, stop)
	if err != nil || !validSeal(before) {
		return infracontract.Readiness{}, infracontract.ErrIncomplete
	}
	topology, err := p.topology.ReadTopology(plan, deadline, stop)
	if err != nil {
		return infracontract.Readiness{}, infracontract.ErrIncomplete
	}
	// Keep the first observation independent of storage reused by a producer on
	// its next read; otherwise a restart could mutate both sides of the comparison.
	// origin: hot-update S9 plan §3.2 / T25
	topology = canonicalTopology(topology)
	target, err := validateTopology(topology, plan.Before)
	if err != nil {
		return infracontract.Readiness{}, err
	}
	snapshot, err := p.inventory.ReadInventory(topology, deadline, stop)
	if err != nil || validateSnapshot(topology, snapshot, target.Host) != nil {
		return infracontract.Readiness{}, infracontract.ErrIncomplete
	}
	snapshot = canonicalSnapshot(snapshot)
	accounts, assets, err := SealDigests(snapshot)
	if err != nil || accounts != before.Accounts || assets != before.Assets {
		return infracontract.Readiness{}, infracontract.ErrIncomplete
	}
	auth, wss, err := readAuth(p.auth, topology, deadline, stop)
	if err != nil {
		return infracontract.Readiness{}, err
	}
	// Reobserve independently rather than treating the first inventory as a lease.
	// The producer fence is still necessary for the A->B->A and sent-unknown cases.
	afterInventory, err := p.inventory.ReadInventory(topology, deadline, stop)
	if err != nil || digest(snapshot) != digest(canonicalSnapshot(afterInventory)) {
		return infracontract.Readiness{}, infracontract.ErrIncomplete
	}
	afterTopology, err := p.topology.ReadTopology(plan, deadline, stop)
	if err != nil || digest(canonicalTopology(topology)) != digest(canonicalTopology(afterTopology)) {
		return infracontract.Readiness{}, infracontract.ErrIncomplete
	}
	after, err := p.seal.ReadSeal(plan, deadline, stop)
	if err != nil || !validSeal(after) || before != after {
		return infracontract.Readiness{}, infracontract.ErrIncomplete
	}
	if err := admission(deadline, stop); err != nil {
		return infracontract.Readiness{}, err
	}
	return infracontract.Readiness{Topology: digest(canonicalTopology(topology)), Seal: after,
		Auth: auth, WSS: wss, Running: target.Running, MetaLeader: snapshot.Meta.Leader,
		Groups: digest(snapshot), Complete: true}, nil
}

func admission(deadline infracontract.Deadline, stop infracontract.Stop) error {
	select {
	case <-stop:
		return context.Canceled
	default:
	}
	_, err := controlbudget.Remaining(deadline)
	return err
}

func validProcess(p infracontract.Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks != 0 && p.InvocationID != "" &&
		p.ExeDevice != 0 && p.ExeInode != 0 && p.ExeSHA256 != (infracontract.Digest{}) && p.Cgroup != ""
}

func validSeal(s infracontract.AssetSeal) bool {
	return s.Complete && s.Revision != 0 && s.Accounts != (infracontract.Digest{}) &&
		s.Assets != (infracontract.Digest{}) && s.Participants != (infracontract.Digest{})
}

func validateTopology(t Topology, before infracontract.Process) (Server, error) {
	// Default single has no Raft voter configuration. Its explicitly observed
	// standalone NATS topology is still valid; an absent clustered B source is not.
	// origin: hot-update S9 plan §3.2 / §4.3 / T24
	if !t.Complete || t.Revision == 0 || !validProcess(before) || (len(t.Brokers) == 0 && !t.Standalone) || len(t.Servers) == 0 {
		return Server{}, infracontract.ErrIncomplete
	}
	brokers := make(map[string]bool)
	for _, b := range t.Brokers {
		if b.NodeID == "" || b.Host == "" || brokers[b.NodeID] {
			return Server{}, infracontract.ErrIncomplete
		}
		brokers[b.NodeID] = true
	}
	ids, names := make(map[string]bool), make(map[string]bool)
	var target Server
	js := 0
	for _, s := range t.Servers {
		if s.ID == "" || s.Name == "" || s.Host == "" || s.Version == "" ||
			!validProcess(s.Running) || ids[s.ID] || names[s.Name] {
			return Server{}, infracontract.ErrIncomplete
		}
		ids[s.ID], names[s.Name] = true, true
		if s.JetStream {
			js++
		}
		if s.Running == before {
			if target.ID != "" || !s.Online || !s.JetStream {
				return Server{}, infracontract.ErrIncomplete
			}
			target = s
		}
	}
	// NATS/JS topology is independent of the tether voter count. In particular a
	// sole voter can still use standalone JS, and three brokers cannot certify N2.
	// origin: hot-update S9 plan §3.2 / T24 / M13
	if target.ID == "" || js == 2 || js == 0 || (t.Standalone && (js != 1 || len(t.Servers) != 1)) || (!t.Standalone && js < 3) {
		return Server{}, infracontract.ErrIncomplete
	}
	return target, nil
}

func validateGroup(t Topology, g Group, targetHost string, meta bool) error {
	if g.ID == "" || g.Replicas < 1 || len(g.Members) != g.Replicas || g.Leader == "" {
		return infracontract.ErrIncomplete
	}
	servers := make(map[string]Server)
	for _, s := range t.Servers {
		servers[s.ID] = s
	}
	leader, ok := servers[g.Leader]
	if !ok || !leader.Online || !leader.JetStream || (meta && leader.Host == targetHost) {
		return infracontract.ErrIncomplete
	}
	seen, hosts := make(map[string]bool), make(map[string]bool)
	leaderSeen, survivors := false, 0
	for _, m := range g.Members {
		s, exists := servers[m.ServerID]
		// Same-host replicas and unknown peers are not independent surviving votes.
		// Reject them even when another majority would otherwise appear available.
		// origin: hot-update S9 plan §3.2 / T24 / M13
		if !exists || !s.JetStream || seen[m.ServerID] || hosts[s.Host] {
			return infracontract.ErrIncomplete
		}
		seen[m.ServerID], hosts[s.Host] = true, true
		leaderSeen = leaderSeen || m.ServerID == g.Leader
		if !s.Online || !m.Online || !m.Current || m.Lag != 0 {
			return infracontract.ErrIncomplete
		}
		if s.Host != targetHost {
			survivors++
		}
	}
	// Standalone uses the separate stop/restart algorithm. It still needs a
	// complete, current R1 inventory; this exception never applies to clustered JS.
	if !leaderSeen || (t.Standalone && g.Replicas != 1) || (!t.Standalone && survivors < g.Replicas/2+1) {
		return infracontract.ErrIncomplete
	}
	return nil
}

func canonicalTopology(t Topology) Topology {
	t.Brokers = append([]Broker(nil), t.Brokers...)
	t.Servers = append([]Server(nil), t.Servers...)
	sort.Slice(t.Brokers, func(i, j int) bool { return t.Brokers[i].NodeID < t.Brokers[j].NodeID })
	sort.Slice(t.Servers, func(i, j int) bool { return t.Servers[i].ID < t.Servers[j].ID })
	return t
}

func digest(v interface{}) infracontract.Digest {
	// All callers use closed, JSON-safe records containing no credentials or keys.
	// encoding/json cannot fail for these types; no arbitrary maps cross the seam.
	b, err := json.Marshal(v)
	if err != nil {
		panic("infra readiness digest requires a closed record")
	}
	return sha256.Sum256(b)
}
