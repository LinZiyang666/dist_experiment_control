package transferharness

// Real: routed file-store JetStream and cold, private Raft/SQLite directories.
// The caller launches workspace-built serve/agent/ctl through Harness; this
// helper never replaces a running product store or implements transfer routing.

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// ClusterPeer owns independent binary, stores, authority and listeners. Port
// reservations survive cold bootstrap until the supervised serve is launched.
// origin: S7 internal review INTEG-O F2 / hot-update S7 plan §6.3
type ClusterPeer struct {
	ID, Root, Binary, DB, Admin, Secrets, Raft, Tunnel, URL, Host string
	Process                                                       *Process
	reservations                                                  []net.Listener
}

type Cluster struct {
	Topology    string
	Replicas    int
	Peers       []*ClusterPeer
	Servers     []*server.Server
	connections []*nats.Conn
}

// NewCluster prepares only a stopped sandbox. SeedDB contains the original
// session and provisioned agent; no live ledger or transfer is manufactured.
// Every member has its own SQLite/Bolt and NATS file store, including N=1.
// origin: S7 internal review INTEG-O F2 / hot-update S7 plan §10.1 / L16
func NewCluster(t *testing.T, root, topology, seedDB, authority string) *Cluster {
	t.Helper()
	count := 1
	if topology == "quorum" {
		count = 3
	} else if topology != "sole-voter" {
		t.Fatalf("transfer cluster: unsupported topology %q", topology)
	}
	c := &Cluster{Topology: topology, Replicas: count}
	// Register before allocating anything: partial cold bootstrap releases the
	// listeners, SDK clients and JetStream actors even when a guard calls Fatal.
	t.Cleanup(c.Close)
	brokerSeed, err := os.ReadFile(filepath.Join(authority, "broker.nk"))
	clusterMust(t, err)
	accountSeed, err := os.ReadFile(filepath.Join(authority, "account.nk"))
	clusterMust(t, err)
	brokerPub, err := auth.PublicKeyFromSeed(brokerSeed)
	clusterMust(t, err)
	accountPub, err := auth.PublicKeyFromSeed(accountSeed)
	clusterMust(t, err)
	ca, caKey := clusterCA(t)
	listenerHost := clusterListenerHost(t)
	var routeReservations []net.Listener
	var routes []string
	if count == 3 {
		for i := 0; i < count; i++ {
			l, err := net.Listen("tcp4", net.JoinHostPort(listenerHost, "0"))
			clusterMust(t, err)
			t.Cleanup(func() { _ = l.Close() })
			routeReservations = append(routeReservations, l)
			routes = append(routes, "nats://"+l.Addr().String())
		}
	}
	for i := 0; i < count; i++ {
		p := &ClusterPeer{ID: fmt.Sprintf("transfer-voter-%d", i), Root: filepath.Join(root, fmt.Sprintf("voter-%d", i)), Host: listenerHost}
		c.Peers = append(c.Peers, p)
		clusterMust(t, os.MkdirAll(p.Root, 0700))
		p.Binary, p.DB, p.Admin, p.Secrets = filepath.Join(p.Root, "broker"), filepath.Join(p.Root, "broker.db"), filepath.Join(p.Root, "admin.sock"), filepath.Join(p.Root, "authority")
		clusterMust(t, os.Mkdir(p.Secrets, 0700))
		for name, data := range map[string][]byte{"broker.nk": brokerSeed, "account.nk": accountSeed} {
			clusterMust(t, os.WriteFile(filepath.Join(p.Secrets, name), data, 0600))
		}
		nodeSeed, err := auth.GenerateUserSeed()
		clusterMust(t, err)
		clusterMust(t, os.WriteFile(filepath.Join(p.Secrets, "node-ident.nk"), nodeSeed, 0600))
		clusterCertificates(t, ca, caKey, p)
		p.Raft, p.Tunnel = p.reserve(t), p.reserve(t)
		opts := &server.Options{Host: "127.0.0.1", Port: -1, ServerName: p.ID, JetStream: true, StoreDir: filepath.Join(p.Root, "jetstream"), NoSigs: true, NoLog: true, AlwaysEnableNonce: true, MaxPayload: 12 << 20,
			Nkeys: []*server.NkeyUser{{Nkey: brokerPub}}, AuthCallout: &server.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}}
		if count == 3 {
			_, port, err := net.SplitHostPort(routeReservations[i].Addr().String())
			clusterMust(t, err)
			number, err := strconv.Atoi(port)
			clusterMust(t, err)
			opts.Cluster = server.ClusterOpts{Name: filepath.Base(filepath.Dir(root)), Host: listenerHost, Port: number}
			opts.Routes = server.RoutesFromStr(strings.Join(routes, ","))
			clusterMust(t, routeReservations[i].Close())
		}
		ns, err := server.NewServer(opts)
		clusterMust(t, err)
		c.Servers = append(c.Servers, ns)
		ns.Start()
		if !ns.ReadyForConnections(5 * time.Second) {
			t.Fatal("transfer cluster: JetStream listener did not start")
		}
		p.URL = ns.ClientURL()
		nc, err := nats.Connect(p.URL, nats.NoReconnect(), nats.Nkey(brokerPub, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(brokerSeed, nonce) }))
		clusterMust(t, err)
		c.connections = append(c.connections, nc)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	clusterMust(t, c.waitJetStream(ctx))
	data, err := os.ReadFile(seedDB)
	clusterMust(t, err)
	first := c.Peers[0]
	clusterMust(t, os.WriteFile(first.DB, data, 0600))
	identSeed, err := os.ReadFile(filepath.Join(first.Secrets, "node-ident.nk"))
	clusterMust(t, err)
	ident, err := auth.PublicKeyFromSeed(identSeed)
	clusterMust(t, err)
	route := first.URL
	if count == 3 {
		route = c.Servers[0].ClusterAddr().String()
	}
	clusterMust(t, clusteroffline.InitFromExisting(clusteroffline.InitFromExistingOptions{DataDir: first.Root, DBPath: first.DB, SecretsDir: first.Secrets, SelfID: first.ID, Name: first.ID, NodeIdentPub: ident, RaftAddr: first.Raft, NatsRoute: route, TunnelAddr: first.Tunnel, PublicHost: "127.0.0.1", Logger: testharness.SilentLog()}))
	if count == 1 {
		return c
	}
	db, err := storage.Open(first.DB)
	clusterMust(t, err)
	for i, p := range c.Peers[1:] {
		seed, err := os.ReadFile(filepath.Join(p.Secrets, "node-ident.nk"))
		clusterMust(t, err)
		pub, err := auth.PublicKeyFromSeed(seed)
		clusterMust(t, err)
		fp, err := clusteroffline.TunnelCertFingerprint(p.Secrets)
		clusterMust(t, err)
		_, err = db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) SELECT ?,?,?,?,?,?,?,'127.0.0.1',?,'VOTER',added_at FROM cluster_nodes WHERE node_id=?`, p.ID, p.ID, pub, p.ID, p.Raft, c.Servers[i+1].ClusterAddr().String(), p.Tunnel, fp, first.ID)
		if err != nil {
			clusterMust(t, errors.Join(err, db.Close()))
		}
	}
	clusterMust(t, db.Close())
	data, err = os.ReadFile(first.DB)
	clusterMust(t, err)
	peers := make([]raft.Server, count)
	for i, p := range c.Peers {
		peers[i] = raft.Server{ID: raft.ServerID(p.ID), Address: raft.ServerAddress(p.Raft), Suffrage: raft.Voter}
	}
	for _, p := range c.Peers {
		if p != first {
			clusterMust(t, os.WriteFile(p.DB, data, 0600))
			db, err := storage.Open(p.DB)
			clusterMust(t, err)
			_, err = db.Exec(`UPDATE cluster_meta SET value=? WHERE key='self_node_id'`, p.ID)
			clusterMust(t, errors.Join(err, db.Close()))
		}
		// Rebuild the initial singleton ONLY before serve starts. Running Raft
		// timers, membership, snapshots and product recovery remain untouched.
		dir := filepath.Join(p.Root, "raft")
		clusterMust(t, os.RemoveAll(dir))
		clusterMust(t, os.Mkdir(dir, 0700))
		store, err := raftboltdb.NewBoltStore(filepath.Join(dir, "raft.db"))
		clusterMust(t, err)
		snaps, err := raft.NewFileSnapshotStore(dir, 2, io.Discard)
		if err != nil {
			clusterMust(t, errors.Join(err, store.Close()))
		}
		_, transport := raft.NewInmemTransport(raft.ServerAddress(p.Raft))
		cfg := raft.DefaultConfig()
		cfg.LocalID = raft.ServerID(p.ID)
		err = raft.BootstrapCluster(cfg, store, store, snaps, transport, raft.Configuration{Servers: peers})
		clusterMust(t, errors.Join(err, transport.Close(), store.Close()))
	}
	return c
}

func (p *ClusterPeer) reserve(t *testing.T) string {
	t.Helper()
	// Reserve the port across local addresses, including the outbound client's
	// 127.0.0.1. A private-address-only reservation may select a number already
	// in use there, making the namespace witness fail before the product binds.
	// Release still advertises the private host, so the real outbound socket
	// and tunnel listener can then coexist on the same port number.
	// origin: S7 internal review TRANSFER-MAKETEST F5 / S7 plan §10 / L16
	l, err := net.Listen("tcp4", "0.0.0.0:0")
	clusterMust(t, err)
	p.reservations = append(p.reservations, l)
	_, port, err := net.SplitHostPort(l.Addr().String())
	clusterMust(t, err)
	return net.JoinHostPort(p.Host, port)
}

// ReleasePorts is called immediately before the real serve process binds them.
func (p *ClusterPeer) ReleasePorts() error {
	var errs []error
	for _, l := range p.reservations {
		errs = append(errs, l.Close())
	}
	p.reservations = nil
	return errors.Join(errs...)
}

func (c *Cluster) waitJetStream(ctx context.Context) error {
	clients := make([]nats.JetStreamContext, len(c.connections))
	for i, nc := range c.connections {
		js, err := nc.JetStream()
		if err != nil {
			return err
		}
		clients[i] = js
	}
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		ready, leaders := true, 0
		for _, ns := range c.Servers {
			if c.Replicas == 3 {
				if ns.NumRoutes() < 2 || !ns.JetStreamIsClustered() || !ns.JetStreamIsCurrent() {
					ready = false
					continue
				}
				if ns.JetStreamIsLeader() {
					leaders++
					if len(ns.JetStreamClusterPeers()) != 3 {
						ready = false
					}
				}
			}
		}
		if c.Replicas == 3 && leaders != 1 {
			ready = false
		}
		if ready {
			for _, js := range clients {
				probe, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				info, err := js.AccountInfo(nats.Context(probe))
				cancel()
				if err != nil || info == nil {
					ready = false
					break
				}
			}
		}
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("transfer cluster: routed JetStream readiness: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// Leader requires every live real broker to report the same membership and
// leader. A listening admin socket alone does not establish a Raft topology.
// origin: S7 internal review INTEG-O F2 / hot-update S7 plan §6.3
func (c *Cluster) Leader() (int, error) {
	type result struct {
		peer     *ClusterPeer
		response *adminsock.Response
		err      error
	}
	results := make(chan result, len(c.Peers))
	// Status includes the production health poll. Read all independent views
	// together and join every reply; serial polls consume a live byte window.
	for _, p := range c.Peers {
		go func() {
			r, err := (&adminsock.Client{Path: p.Admin, Timeout: 5 * time.Second}).Call(adminsock.Request{Op: adminsock.OpClusterStatus})
			results <- result{p, r, err}
		}()
	}
	views := make([]result, 0, len(c.Peers))
	for range c.Peers {
		views = append(views, <-results)
	}
	leader := ""
	for _, view := range views {
		p, r := view.peer, view.response
		if view.err != nil {
			return -1, view.err
		}
		if !r.OK || r.Cluster == nil || r.Cluster.ViewHost != p.ID || len(r.Cluster.Nodes) != c.Replicas || r.Cluster.LeaderID == "" {
			return -1, errors.New("transfer cluster: real membership not ready")
		}
		voters := 0
		for _, node := range r.Cluster.Nodes {
			if node.Role == "leader" || node.Role == "voter" {
				voters++
			}
		}
		if voters != c.Replicas {
			return -1, errors.New("transfer cluster: roster is not the committed voter configuration")
		}
		if leader != "" && leader != r.Cluster.LeaderID {
			return -1, errors.New("transfer cluster: leader views disagree")
		}
		leader = r.Cluster.LeaderID
	}
	for i, p := range c.Peers {
		if p.ID == leader {
			return i, nil
		}
	}
	return -1, errors.New("transfer cluster: leader outside private roster")
}

// Census hashes actual executable mappings and binds every supervised serve
// to PID/start ticks/boot ID. Path labels cannot certify a live broker process.
// origin: S7 internal review INTEG-O F2 / hot-update S7 plan lead L16
func (c *Cluster) Census() ([]Receipt, error) {
	rows := make([]Receipt, 0, len(c.Peers))
	seen := make(map[int]bool)
	for _, p := range c.Peers {
		if p.Process == nil {
			return nil, errors.New("transfer cluster: broker has no supervisor custody")
		}
		r, err := p.Process.SampleIdentity()
		if err != nil {
			return nil, err
		}
		if r.ProcessStart == 0 || r.BootID == "" || seen[r.PID] || r.RunningSHA != r.ArtifactSHA {
			return nil, errors.New("transfer cluster: original process census mismatch")
		}
		seen[r.PID] = true
		rows = append(rows, r)
	}
	return rows, nil
}

func (c *Cluster) Close() {
	for _, p := range c.Peers {
		_ = p.ReleasePorts()
	}
	for _, nc := range c.connections {
		nc.Close()
	}
	for _, ns := range c.Servers {
		ns.Shutdown()
	}
	for _, ns := range c.Servers {
		ns.WaitForShutdown()
	}
}

func clusterCA(t *testing.T) (*x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	clusterMust(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "transfer-private-ca"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, key)
	clusterMust(t, err)
	ca, err := x509.ParseCertificate(der)
	clusterMust(t, err)
	return ca, key
}
func clusterCertificates(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey, p *ClusterPeer) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	clusterMust(t, err)
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: p.ID}, IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.ParseIP(p.Host)}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, pub, caKey)
	clusterMust(t, err)
	encodedKey, err := x509.MarshalPKCS8PrivateKey(key)
	clusterMust(t, err)
	for name, block := range map[string]*pem.Block{"cluster-ca.pem": {Type: "CERTIFICATE", Bytes: ca.Raw}, "route-cert.pem": {Type: "CERTIFICATE", Bytes: der}, "route-key.pem": {Type: "PRIVATE KEY", Bytes: encodedKey}, "tunnel-cert.pem": {Type: "CERTIFICATE", Bytes: der}, "tunnel-key.pem": {Type: "PRIVATE KEY", Bytes: encodedKey}} {
		clusterMust(t, os.WriteFile(filepath.Join(p.Secrets, name), pem.EncodeToMemory(block), 0600))
	}
}
func clusterMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
