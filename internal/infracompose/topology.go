package infracompose

import (
	"encoding/json"
	"net/netip"
	"runtime"
	"slices"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraartifact"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrareadiness"
	"github.com/nats-io/nats.go"
)

// ManagedNode borrows a direct SYS connection and the authenticated host for
// that physical machine. A WSS proxy cannot identify the NATS executable here.
type ManagedNode struct {
	Domain c.ID
	Host   c.Host
	System *nats.Conn
}

// ManagedTopology reads B from normal membership, S from actual INFO/SYS and
// J from VARZ. Standalone describes the configured NATS topology independently
// of B. The accepted SYS socket in the root census binds INFO to the executable;
// a URL, a catalog entry or a convenient server name alone is insufficient.
// origin: hot-update S9 plan §3.2 / P22
type ManagedTopology struct {
	Nodes      []ManagedNode
	Brokers    func(c.Deadline, c.Stop) (uint64, []infrareadiness.Broker, error)
	Standalone bool
}

var _ infrareadiness.TopologySource = (*ManagedTopology)(nil)

func (o *ManagedTopology) ReadTopology(_ c.Plan, d c.Deadline, stop c.Stop) (infrareadiness.Topology, error) {
	if runtime.GOOS != "linux" || runtime.GOARCH != "amd64" {
		return infrareadiness.Topology{}, c.ErrUnsupported
	}
	if err := topologyBudget(d, stop); err != nil {
		return infrareadiness.Topology{}, err
	}
	if o == nil || o.Brokers == nil || len(o.Nodes) == 0 || len(o.Nodes) > c.MaxCensusPages {
		return infrareadiness.Topology{}, c.ErrIncomplete
	}
	revision, brokers, err := o.Brokers(d, stop)
	if err != nil || revision == 0 {
		return infrareadiness.Topology{}, c.ErrIncomplete
	}
	topology := infrareadiness.Topology{Revision: revision, Brokers: slices.Clone(brokers), Standalone: o.Standalone}
	seen := make(map[string]bool)
	for _, node := range o.Nodes {
		server, err := readServer(node, d, stop)
		if err != nil || seen[server.ID] {
			return infrareadiness.Topology{}, c.ErrIncomplete
		}
		seen[server.ID] = true
		topology.Servers = append(topology.Servers, server)
	}
	afterRevision, afterBrokers, err := o.Brokers(d, stop)
	if err != nil || afterRevision != revision || !slices.Equal(brokers, afterBrokers) {
		return infrareadiness.Topology{}, c.ErrIncomplete
	}
	if err := topologyBudget(d, stop); err != nil {
		return infrareadiness.Topology{}, err
	}
	topology.Complete = true
	return topology, nil
}

func topologyBudget(d c.Deadline, stop c.Stop) error {
	select {
	case <-stop:
		return c.ErrIncomplete
	default:
	}
	_, err := controlbudget.Remaining(d)
	return err
}

type varzResponse struct {
	Server *struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Version string `json:"ver"`
	} `json:"server"`
	Data *struct {
		ID        string `json:"server_id"`
		Name      string `json:"server_name"`
		Version   string `json:"version"`
		JetStream *struct {
			Config json.RawMessage `json:"config"`
		} `json:"jetstream"`
	} `json:"data"`
	Error *nats.APIError `json:"error"`
}

func readServer(node ManagedNode, d c.Deadline, stop c.Stop) (infrareadiness.Server, error) {
	if absent(node.Host) || node.Domain == (c.ID{}) || node.System == nil || topologyBudget(d, stop) != nil {
		return infrareadiness.Server{}, c.ErrIncomplete
	}
	nc := node.System
	id, name, version := nc.ConnectedServerId(), nc.ConnectedServerName(), nc.ConnectedServerVersion()
	local, remote := nc.LocalAddr(), nc.ConnectedAddr()
	cid, err := nc.GetClientID()
	if err != nil || cid == 0 || id == "" || name == "" || local == "" || remote == "" {
		return infrareadiness.Server{}, c.ErrIncomplete
	}
	data, err := (infrareadiness.NATSRequester{Conn: nc}).Request("$SYS.REQ.SERVER."+id+".VARZ", nil, d, stop)
	var response varzResponse
	if err != nil || len(data) == 0 || len(data) > c.MaxCensusPageBytes || json.Unmarshal(data, &response) != nil ||
		response.Error != nil || response.Server == nil || response.Data == nil ||
		response.Server.ID != id || response.Data.ID != id || response.Server.Name != name || response.Data.Name != name ||
		response.Server.Version != version || response.Data.Version != version {
		return infrareadiness.Server{}, c.ErrIncomplete
	}
	census, err := node.Host.Inspect(node.Domain, d)
	if err != nil || !validCensus(census) {
		return infrareadiness.Server{}, c.ErrIncomplete
	}
	accepted, err := acceptedSocket(census, local, remote)
	if err != nil {
		return infrareadiness.Server{}, err
	}
	// BootID is the physical failure domain. Lab aliases for three processes
	// on one host must not become three independent surviving hosts.
	process := accepted.Owner
	catalog, err := infraartifact.New(infraartifact.Config{})
	if err != nil {
		return infrareadiness.Server{}, err
	}
	artifact, err := catalog.Resolve(c.NATSBinary, version, "linux", "amd64")
	if err != nil || process.ExeSHA256 != artifact.BinarySHA256 || !slices.Contains(census.Processes, process) || !completeProcess(process) {
		return infrareadiness.Server{}, c.ErrIncomplete
	}
	after, err := node.Host.Inspect(node.Domain, d)
	afterCID, cidErr := nc.GetClientID()
	if err != nil || !validCensus(after) || !slices.Contains(after.Processes, process) || !slices.Contains(after.Sockets, accepted) ||
		cidErr != nil || afterCID != cid || nc.ConnectedServerId() != id || nc.ConnectedServerVersion() != version ||
		nc.LocalAddr() != local || nc.ConnectedAddr() != remote || topologyBudget(d, stop) != nil {
		return infrareadiness.Server{}, c.ErrIncomplete
	}
	js := response.Data.JetStream
	return infrareadiness.Server{ID: id, Name: name, Version: version, Host: process.BootID,
		Running: process, JetStream: js != nil && len(js.Config) > 0 && string(js.Config) != "null", Online: true}, nil
}

func completeProcess(p c.Process) bool {
	return p.BootID != "" && p.PID > 0 && p.StartTicks != 0 && p.InvocationID != "" &&
		p.ExeDevice != 0 && p.ExeInode != 0 && p.ExeSHA256 != (c.Digest{}) && p.Cgroup != ""
}

func validCensus(census c.Census) bool {
	return census.Complete && census.Generation != 0 && census.Digest != (c.Digest{})
}

func sameEndpoint(a, b string) bool {
	x, e1 := netip.ParseAddrPort(a)
	y, e2 := netip.ParseAddrPort(b)
	return e1 == nil && e2 == nil && x.Port() == y.Port() && x.Addr().Unmap() == y.Addr().Unmap()
}

func acceptedSocket(census c.Census, local, remote string) (c.Socket, error) {
	var accepted c.Socket
	count := 0
	for _, socket := range census.Sockets {
		if sameEndpoint(socket.Local, remote) && sameEndpoint(socket.Remote, local) {
			accepted, count = socket, count+1
		}
	}
	if count != 1 || accepted.NetNS == 0 || accepted.Device == 0 || accepted.Inode == 0 || accepted.Cookie == 0 {
		return c.Socket{}, c.ErrIncomplete
	}
	return accepted, nil
}
