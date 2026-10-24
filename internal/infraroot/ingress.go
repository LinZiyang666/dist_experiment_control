package infraroot

import (
	"crypto/tls"
	"errors"
	"net"
	"os"
	"strings"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrahost"
	"github.com/LinZiyang666/tether/internal/ingressswitch"
)

type installedChanges struct{ records *installedRecords }

func (r installedChanges) Load(digest c.Digest) (ingressswitch.Change, error) {
	var change ingressswitch.Change
	path := stateDirectory + "/records/" + digestName(digest) + ".json"
	if err := r.records.installation.files.Decode(path, &change); err != nil {
		return change, err
	}
	if ingressswitch.ChangeDigest(change) != digest || change.Domain != r.records.policy.Domain {
		return change, c.ErrIncomplete
	}
	return change, nil
}

// The probe endpoint is approved root configuration. It is neither an executable
// path nor a caller URL, and a recorded readiness digest never replaces the
// current raw socket/accepted socket/process observation below.
type ingressEndpoint struct {
	Version    uint16
	Purpose    string
	Address    string
	ServerName string
	ReadyProof c.Digest
}

type installedEvidence struct{ records *installedRecords }

func (e installedEvidence) Census(domain c.ID, d c.Deadline) (c.Census, error) {
	return e.records.Inspect(domain, d)
}
func (e installedEvidence) Probe(expected c.Process, bypass bool, d c.Deadline, stop c.Stop) (_ ingressswitch.ProbeProof, resultErr error) {
	var endpoint ingressEndpoint
	if err := e.records.installation.files.Decode(stateDirectory+"/ingress-endpoint.json", &endpoint); err != nil {
		return ingressswitch.ProbeProof{}, err
	}
	if endpoint.Version != 1 || endpoint.Purpose != "tether-infra-ingress-endpoint-v1" || endpoint.ReadyProof == (c.Digest{}) || endpoint.ServerName == "" {
		return ingressswitch.ProbeProof{}, c.ErrIncomplete
	}
	host, port, err := net.SplitHostPort(endpoint.Address)
	if err != nil || net.ParseIP(host) == nil || port != "443" {
		return ingressswitch.ProbeProof{}, c.ErrIncomplete
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: endpoint.ServerName}
	var conn *tls.Conn
	var attempt ingressswitch.TLSAttempt
	if bypass {
		conn, attempt, err = ingressswitch.DialPrimary(endpoint.Address, config, d, stop)
	} else {
		conn, attempt, err = ingressswitch.DialCanonical(endpoint.Address, config, d, stop)
	}
	if err != nil {
		return ingressswitch.ProbeProof{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, conn.Close()) }()
	self, err := e.records.installation.process(int32(os.Getpid()), d)
	if err != nil {
		return ingressswitch.ProbeProof{}, err
	}
	client, err := installedClientSocket(conn.NetConn(), self, d)
	if err != nil {
		return ingressswitch.ProbeProof{}, err
	}
	census, err := e.records.Inspect(e.records.policy.Domain, d)
	if err != nil {
		return ingressswitch.ProbeProof{}, err
	}
	var accepted, listener c.Socket
	for _, socket := range census.Sockets {
		if socket.Owner != expected {
			continue
		}
		if infrahost.VerifyIngressProof(client, socket, expected, census) == nil {
			accepted = socket
		}
		if strings.HasSuffix(socket.Remote, ":0") {
			listener = socket
		}
	}
	if accepted == (c.Socket{}) || listener == (c.Socket{}) {
		return ingressswitch.ProbeProof{}, c.ErrIncomplete
	}
	mark := uint32(0)
	if bypass {
		mark = ingressswitch.PrimaryMark
	}
	return ingressswitch.ProbeProof{Version: 1, TLS: true, FirstAttempt: true, Mark: mark, StartedNS: attempt.StartedNS, FinishedNS: attempt.FinishedNS, Client: client, Accepted: accepted, Listener: listener, ReadyProof: endpoint.ReadyProof}, nil
}
func (e installedEvidence) Seal(rules ingressswitch.Snapshot, expected c.Process, d c.Deadline, stop c.Stop) (ingressswitch.Seal, error) {
	if err := installedBudget(d, stop); err != nil {
		return ingressswitch.Seal{}, err
	}
	before, err := e.records.installation.ingress.Read(d, stop)
	if err != nil || before != rules {
		return ingressswitch.Seal{}, c.ErrIncomplete
	}
	scope := e.records.record.Scope
	scope.Before, scope.Cgroups = []c.Process{expected}, []string{expected.Cgroup}
	census, err := e.records.installation.census(scope, d)
	if err != nil || !census.Complete {
		return ingressswitch.Seal{}, c.ErrIncomplete
	}
	after, err := e.records.installation.ingress.Read(d, stop)
	if err != nil || after != before {
		return ingressswitch.Seal{}, c.ErrIncomplete
	}
	seal := ingressswitch.Seal{Version: 1, Rules: rules, Owner: expected, BarrierClosed: true, AcceptedComplete: true, ListenersComplete: true, SYNComplete: true, ConntrackComplete: true, Census: census, PendingConntrack: census.OldOwnerPending}
	for _, socket := range census.Sockets {
		if socket.Owner == expected && !strings.HasSuffix(socket.Remote, ":0") {
			seal.AcceptedTCP++
		}
	}
	return seal, nil
}
