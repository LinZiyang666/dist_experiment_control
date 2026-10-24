//go:build linux

package clusterharness

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecli/clusterroll"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Three real serve processes use the fixture's routed authenticated NATS.
// Each owns separate SQLite/Bolt/mTLS stores; no in-memory Raft substitutes for
// the actual follower cut. The signed production roll driver supplies observer
// proofs, fresh leadership transfer, holder CAS and maintenance completion.
// origin: hot-update S6 plan §5 / §7 / T48 / EXITS-2-3
type productQuorum struct {
	peers                   []*productQuorumPeer
	brokerSeed, accountSeed []byte
	nc                      *nats.Conn
	mu                      sync.Mutex
	refusal                 string
	nats                    []*server.Server
	cutCancel               context.CancelFunc
	cutDone                 chan struct{}
	cutErr                  error
	cutStarted              bool
}
type productQuorumPeer struct {
	root, id, raft, tunnel string
	args                   []string
	cmd                    *exec.Cmd
	done                   chan error
	logs                   productLogs
}

func (f *brokerProductFixture) setupQuorum(secrets string, brokerSeed, accountSeed []byte, args []string) error {
	q := &productQuorum{brokerSeed: brokerSeed, accountSeed: append([]byte(nil), accountSeed...)}
	f.quorum = q
	peers := []raft.Server{}
	primaryRaft := ""
	for i, a := range args {
		if a == "--cluster-raft-addr" {
			primaryRaft = args[i+1]
		}
	}
	peers = append(peers, raft.Server{ID: "product", Address: raft.ServerAddress(primaryRaft), Suffrage: raft.Voter})
	for i := 1; i < 3; i++ {
		p := &productQuorumPeer{root: filepath.Join(f.root, fmt.Sprintf("voter-%d", i)), id: fmt.Sprintf("product-%d", i), raft: productFixtureAddress(f), tunnel: productFixtureAddress(f)}
		if err := os.MkdirAll(filepath.Join(p.root, "credentials"), 0700); err != nil {
			return err
		}
		entries, err := os.ReadDir(secrets)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if err := storeFixtureCopy(filepath.Join(secrets, entry.Name()), filepath.Join(p.root, "credentials", entry.Name()), nil); err != nil {
				return err
			}
		}

		pub, err := auth.PublicKeyFromSeed(brokerSeed)
		if err != nil {
			return err
		}
		accountPub, err := auth.PublicKeyFromSeed(q.accountSeed)
		if err != nil {
			return err
		}
		_, port, _ := net.SplitHostPort(strings.TrimPrefix(f.natsRoutes[i], "nats://"))
		routePort, _ := strconv.Atoi(port)
		ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, ServerName: p.id, JetStream: true, StoreDir: filepath.Join(p.root, "nats"), NoLog: true, NoSigs: true, Cluster: server.ClusterOpts{Name: "s6-cohort", Host: "127.0.0.1", Port: routePort}, Routes: server.RoutesFromStr(strings.Join(f.natsRoutes, ",")), Nkeys: []*server.NkeyUser{{Nkey: pub}}, AuthCallout: &server.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{pub}}})
		if err != nil {
			return err
		}
		q.nats = append(q.nats, ns)
		ns.Start()
		if !ns.ReadyForConnections(5 * time.Second) {
			return errors.New("quorum NATS startup expired")
		}
		p.args = append([]string(nil), args...)
		for j, a := range p.args {
			switch a {
			case "--nats-url":
				p.args[j+1] = ns.ClientURL()
			case "--db":
				p.args[j+1] = filepath.Join(p.root, "tether.db")
			case "--admin-socket":
				p.args[j+1] = filepath.Join(p.root, "admin.sock")
			case "--cluster-data-dir":
				p.args[j+1] = p.root
			case "--auth-callout-seeds-dir", "--cluster-secrets-dir":
				p.args[j+1] = filepath.Join(p.root, "credentials")
			case "--cluster-raft-addr":
				p.args[j+1] = p.raft
			case "--tunnel-addr":
				p.args[j+1] = p.tunnel
			}
		}
		q.peers = append(q.peers, p)
		peers = append(peers, raft.Server{ID: raft.ServerID(p.id), Address: raft.ServerAddress(p.raft), Suffrage: raft.Voter})
	}
	db, err := storage.Open(filepath.Join(f.root, "tether.db"))
	if err != nil {
		return err
	}
	nodeSeed, err := os.ReadFile(filepath.Join(secrets, "node-ident.nk"))
	if err != nil {
		_ = db.Close()
		return err
	}
	pub, err := auth.PublicKeyFromSeed(nodeSeed)
	if err != nil {
		_ = db.Close()
		return err
	}
	for i, p := range q.peers {
		if _, err = db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) SELECT ?,?,?,?,?,?,?,'127.0.0.1',cert_fp,'VOTER',added_at FROM cluster_nodes WHERE node_id='product'`, p.id, p.id, pub, p.id, p.raft, strings.TrimPrefix(f.natsRoutes[i+1], "nats://"), p.tunnel); err != nil {
			_ = db.Close()
			return err
		}
	}
	if _, err = db.Exec(`UPDATE cluster_nodes SET nats_route=? WHERE node_id='product'`, strings.TrimPrefix(f.natsRoutes[0], "nats://")); err != nil {
		_ = db.Close()
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		root, id := f.root, "product"
		if i > 0 {
			root, id = q.peers[i-1].root, q.peers[i-1].id
			if err = storeFixtureCopy(filepath.Join(f.root, "tether.db"), filepath.Join(root, "tether.db"), nil); err != nil {
				return err
			}
			db, err = storage.Open(filepath.Join(root, "tether.db"))
			if err != nil {
				return err
			}
			_, err = db.Exec(`UPDATE cluster_meta SET value=? WHERE key='self_node_id'`, id)
			err = errors.Join(err, db.Close())
			if err != nil {
				return err
			}
		}
		// Replace only the cold sandbox's initial singleton Bolt bootstrap. No
		// running store, image reopen or product RecoverCluster path is altered.
		raftDir := filepath.Join(root, "raft")
		if err = os.RemoveAll(raftDir); err != nil {
			return err
		}
		if err = os.Mkdir(raftDir, 0700); err != nil {
			return err
		}
		store, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
		if err != nil {
			return err
		}
		snaps, err := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
		if err != nil {
			_ = store.Close()
			return err
		}
		_, trans := raft.NewInmemTransport(peers[i].Address)
		config := raft.DefaultConfig()
		config.LocalID = raft.ServerID(id)
		err = raft.BootstrapCluster(config, store, store, snaps, trans, raft.Configuration{Servers: peers})
		err = errors.Join(err, trans.Close(), store.Close())
		if err != nil {
			return err
		}
	}
	accountPub, err := auth.PublicKeyFromSeed(q.accountSeed)
	if err != nil {
		return err
	}
	if f.cohort != nil {
		if err = os.WriteFile(filepath.Join(f.cohort.agentHome, "agent", "product", "agent.yaml"), []byte("account_pub: "+accountPub+"\nproxy:\n  participate: false\n"), 0600); err != nil {
			return err
		}
	}
	return nil
}
func (q *productQuorum) start(f *brokerProductFixture, binary string) error {
	pub, err := auth.PublicKeyFromSeed(q.brokerSeed)
	if err != nil {
		return err
	}
	q.nc, err = nats.Connect(f.nats.ClientURL(), nats.Nkey(pub, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(q.brokerSeed, nonce) }))
	if err != nil {
		return err
	}
	// ReadyForConnections above witnesses only NATS client listeners. Do not
	// spend the broker's unchanged 15s readiness window on the independent
	// routed JetStream election (the product retains its real boot refusal).
	// origin: hot-update S6 plan §5 / §10.1 / HARDGATE-B request 3
	ctx, cancel := context.WithTimeout(f.test.Context(), 15*time.Second)
	defer cancel()
	deadline, _ := ctx.Deadline()
	servers := append([]*server.Server{f.nats}, q.nats...)
	connections := []*nats.Conn{q.nc}
	defer func() {
		for _, nc := range connections[1:] {
			nc.Close()
		}
	}()
	for _, ns := range q.nats {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		nc, err := nats.Connect(ns.ClientURL(), nats.NoReconnect(), nats.Timeout(min(remaining, time.Second)), nats.Nkey(pub, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(q.brokerSeed, nonce) }))
		if err != nil {
			return err
		}
		connections = append(connections, nc)
	}
	if err := waitRoutedJetStream(ctx, servers, connections); err != nil {
		return err
	}
	for i := range q.peers {
		p := q.peers[i]
		// Every voter has its own canonical installation and config before its
		// first start. Sharing the primary's executable would merge unrelated
		// component domains and leave peer journal discovery unregistered.
		// origin: hot-update S8 plan §5.1 / §5.2; S8 internal review P13-COHORT F2
		canonical := filepath.Join(p.root, "broker-product")
		if err := storeFixtureCopy(binary, canonical, nil); err != nil {
			return err
		}
		if err := storeFixtureCopy(binary, filepath.Join(p.root, "candidate"), nil); err != nil {
			return err
		}
		transaction, err := newProductTransaction(p.root, canonical, filepath.Join(p.root, "tether.db"))
		if err != nil {
			return err
		}
		if err := transaction.register(filepath.Join(p.root, "tether.db")); err != nil {
			return err
		}
		p.args = append(p.args, "--config", transaction.config)
		p.cmd = exec.Command(canonical, p.args...)
		p.cmd.Dir = p.root
		p.cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "HOME="+p.root, "TETHER_HOME="+p.root, "GORACE=atexit_sleep_ms=0", "TETHER_AUTO_REBALANCE=on")
		p.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
		p.cmd.Stdout, p.cmd.Stderr = &p.logs, &p.logs
		waitProcess, err := testharness.StartProcess(p.cmd)
		if err != nil {
			return err
		}
		p.done = make(chan error, 1)
		go func() { p.done <- waitProcess() }()
	}
	return nil
}
func (q *productQuorum) health(ctx context.Context) ([]proto.ClusterHealthResp, error) {
	pub, _ := auth.PublicKeyFromSeed(q.brokerSeed)
	return cli.QueryBrokerRollHealthForVoters(ctx, q.nc, pub, []string{"product", "product-1", "product-2"})
}
func (q *productQuorum) trigger(ctx context.Context, r *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
	r.IssuedAt = time.Now().UTC().Format(time.RFC3339Nano)
	sig, err := auth.SignWithSeed(q.accountSeed, proto.CanonicalUpgradeReqBytes(r))
	if err != nil {
		return nil, err
	}
	r.Sig = hex.EncodeToString(sig)
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, 6*time.Second)
	defer cancel()
	actor, err := auth.PublicKeyFromSeed(q.brokerSeed)
	if err != nil {
		return nil, err
	}
	msg, err := q.nc.RequestWithContext(ctx, proto.SubjCtrlClusterUpgrade(actor), data)
	if err != nil {
		return nil, err
	}
	var resp proto.ClusterUpgradeResp
	err = json.Unmarshal(msg.Data, &resp)
	if err == nil && !resp.OK {
		q.mu.Lock()
		q.refusal = r.Op + ": " + resp.Code + ": " + resp.Error
		q.mu.Unlock()
	}
	return &resp, err
}

// The busy cohort must use a sealed S8 parent; the ordinary legacy reload is
// correctly rejected with legacy_busy. Preparation and publication precede the
// measured cut. RunCluster preserves the S6 leadership/observer/holder proofs,
// dispatches the original child once and waits on its three current receipts.
// origin: hot-update S6 plan lead L2 / §5; hot-update S8 plan §3.1 / §7.2;
// S8 internal review P13-COHORT F2
func (q *productQuorum) prepare(f *brokerProductFixture, sha string) (func() error, error) {
	actor, err := auth.PublicKeyFromSeed(q.brokerSeed)
	if err != nil {
		return nil, err
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(f.test.Context())
	q.cutCancel = cancel
	ports := clusterroll.ClusterPorts{Trigger: q.trigger, Health: q.health, ActorFP: fp, Home: filepath.Join(f.root, "roll-ctl")}
	parent, plan, err := clusterroll.PrepareCluster(ctx, ports, []string{"product"}, proto.ReleaseVersion, sha)
	if err != nil {
		return nil, err
	}
	if len(plan.Steps) == 0 {
		return nil, errors.New("quorum fixture: original cohort requires a real broker cut")
	}
	if _, err := f.transaction.stage(ctx, f.cmd.Process.Pid, plan.Steps[0].Grant); err != nil {
		return nil, err
	}
	// Let the real driver reserve, collect and preflight the sealed plan while
	// the source serves. Hold only its original Start at the transport boundary;
	// releasing that call defines the same measured cut as standalone Start.
	// No product response, ID, maintenance expiry or completion is substituted.
	ready, release := make(chan struct{}), make(chan struct{})
	armed := make(chan error, 1)
	ports.Trigger = func(ctx context.Context, r *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
		var op struct {
			Op string `json:"op"`
		}
		if r.Op == "upgrade-tx" && json.Unmarshal(r.Upgrade, &op) == nil && op.Op == upgradewire.OpStart {
			close(ready)
			select {
			case <-release:
			case <-ctx.Done():
				armed <- ctx.Err()
				return nil, ctx.Err()
			}
		}
		response, err := q.trigger(ctx, r)
		if op.Op == upgradewire.OpStart {
			result := err
			if err == nil {
				body, decodeErr := upgradewire.DecodeResponse(response.Upgrade)
				if decodeErr != nil {
					result = decodeErr
				} else if body.Code != "" && (body.Code != "upgrade_unsettled" || body.Error != "broker_activation_dispatched") {
					result = fmt.Errorf("quorum original Start: %s: %s", body.Code, body.Error)
				}
			}
			armed <- result
		}
		return response, err
	}
	q.cutDone = make(chan struct{})
	go func() {
		defer close(q.cutDone)
		_, q.cutErr = clusterroll.RunCluster(ctx, ports, parent, plan)
	}()
	select {
	case <-ready:
	case <-q.cutDone:
		cancel()
		return nil, fmt.Errorf("quorum driver did not reach original Start: %v", q.cutErr)
	}
	return func() error {
		q.cutStarted = true
		close(release)
		return <-armed
	}, nil
}

// The armed checkpoint must let the original FIN gate proceed while the parent
// proves its host/cluster releases. Join that same driver before any daemon or
// original resource is stopped, rather than creating a fixture-induced FIN gap.
// origin: hot-update S8 plan §4.2 / §5.1; S8 internal review P13-COHORT F2
func (q *productQuorum) finish() error {
	if q.cutDone == nil {
		return nil
	}
	if !q.cutStarted {
		q.cutCancel()
	}
	select {
	case <-q.cutDone:
	case <-time.After(productTransactionBudget):
		q.cutCancel()
		<-q.cutDone
		return errors.New("quorum original parent did not settle within fixture cleanup budget")
	}
	if q.cutStarted && q.cutErr != nil {
		return fmt.Errorf("quorum original parent settlement: %w", q.cutErr)
	}
	return nil
}
func (q *productQuorum) close() error {
	if q.cutCancel != nil {
		q.cutCancel()
	}
	if q.cutDone != nil {
		<-q.cutDone
	}
	if q.nc != nil {
		q.nc.Close()
	}
	for i := range q.peers {
		p := q.peers[i]
		if p.done == nil {
			continue
		}
		_ = p.cmd.Process.Signal(os.Interrupt)
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			<-p.done
		}
	}
	for _, ns := range q.nats {
		ns.Shutdown()
	}
	for _, ns := range q.nats {
		ns.WaitForShutdown()
	}
	return nil
}
