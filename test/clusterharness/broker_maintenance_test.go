//go:build linux

package clusterharness

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/x509"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradecli/clusterroll"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// Three real serve processes, the real agent entry, routed authenticated NATS,
// production Raft timers and on-disk SQLite exercise the signed rolling driver.
// Observations and one bounded barrier are overlaid; every planner, proof, ACK,
// import and exec remains the production implementation.
// origin: hot-update S6 plan §5 / §7 / T38 / T41 / T43
// origin: S8 internal review cluster-roll F9
func TestBrokerN3MaintenancePreservesHomeAcrossExec(t *testing.T) {
	binary := buildN3MaintenanceArtifact(t)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseN3MaintenanceExec(t, binary) })
	}
	testharness.AssertNoGoroutineLeak(t, "N3 maintenance exec x5", g)
	testharness.AssertNoFDLeak(t, "N3 maintenance exec x5", fd, 0)
}

type n3MaintenanceProcess struct {
	dir, id, raftAddr, tunnelAddr string
	cmd                           *exec.Cmd
	done                          chan struct{}
	waitErr                       error
	logs                          productLogs
}

func (p *n3MaintenanceProcess) close(t *testing.T) {
	t.Helper()
	if p.cmd == nil || p.cmd.Process == nil {
		return
	}
	select {
	case <-p.done:
		return
	default:
	}
	_ = p.cmd.Process.Signal(syscall.SIGTERM)
	select {
	case <-p.done:
		if p.waitErr != nil {
			t.Errorf("%s terminal cleanup: %v\n%s", p.id, p.waitErr, p.logs.String())
		}
	case <-time.After(15 * time.Second):
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		<-p.done
		t.Errorf("%s did not join after SIGTERM\n%s", p.id, p.logs.String())
	}
}

func n3MaintenanceStart(t *testing.T, p *n3MaintenanceProcess, binary string, args []string) {
	t.Helper()
	p.cmd = exec.Command(binary, args...)
	p.cmd.Dir = p.dir
	p.cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "HOME="+p.dir, "TETHER_HOME="+p.dir, "TETHER_AUTO_REBALANCE=on", "GORACE=atexit_sleep_ms=0")
	p.cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	p.cmd.Stdout, p.cmd.Stderr = &p.logs, &p.logs
	waitProcess, err := testharness.StartProcess(p.cmd)
	if err != nil {
		t.Fatal(err)
	}
	p.done = make(chan struct{})
	go func() { p.waitErr = waitProcess(); close(p.done) }()
	t.Cleanup(func() { p.close(t) })
}

func n3MaintenanceWait(t *testing.T, within time.Duration, predicate func() bool, processes []*n3MaintenanceProcess) {
	t.Helper()
	if WaitForCond(within, predicate) {
		return
	}
	for _, p := range processes {
		t.Logf("%s: %s", p.id, p.logs.String())
	}
	_, file, line, _ := runtime.Caller(1)
	t.Fatalf("N3 product observation deadline expired at %s:%d", file, line)
}

func exerciseN3MaintenanceExec(t *testing.T, binary string) {
	t.Helper()
	root := testharness.GuardProcessDirectory(t, t.TempDir())
	brokerSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	brokerPub, _ := auth.PublicKeyFromSeed(brokerSeed)
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer account.Wipe()
	accountSeed, err := account.Seed()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, _ := account.PublicKey()
	agentSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	agentPub, _ := auth.PublicKeyFromSeed(agentSeed)
	agentFP, _ := auth.FingerprintFromActor(agentPub)
	ca := NewRouteCA(t, "n3-maintenance")
	echo, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Uint64
	var echoWorkers sync.WaitGroup
	var echoMu sync.Mutex
	var echoConns []net.Conn
	echoDone := make(chan struct{})
	go func() {
		defer close(echoDone)
		for {
			conn, err := echo.Accept()
			if err != nil {
				return
			}
			accepted.Add(1)
			echoMu.Lock()
			echoConns = append(echoConns, conn)
			echoMu.Unlock()
			echoWorkers.Add(1)
			go func() {
				defer echoWorkers.Done()
				defer closeBrokerFixtureWitness(t, conn)
				_, _ = io.Copy(struct{ io.Writer }{conn}, struct{ io.Reader }{conn})
			}()
		}
	}()
	t.Cleanup(func() {
		_ = echo.Close()
		<-echoDone
		echoMu.Lock()
		for _, conn := range echoConns {
			_ = conn.Close()
		}
		echoMu.Unlock()
		echoWorkers.Wait()
	})
	var processes []*n3MaintenanceProcess
	var peers []raft.Server
	var routeURLs []string
	addresses := &brokerProductFixture{test: t}
	for i := 0; i < 3; i++ {
		p := &n3MaintenanceProcess{dir: filepath.Join(root, fmt.Sprint(i)), id: fmt.Sprintf("n3-%d", i), raftAddr: productFixtureAddress(addresses), tunnelAddr: productFixtureAddress(addresses)}
		if err := os.MkdirAll(filepath.Join(p.dir, "credentials"), 0700); err != nil {
			t.Fatal(err)
		}
		processes = append(processes, p)
		peers = append(peers, raft.Server{ID: raft.ServerID(p.id), Address: raft.ServerAddress(p.raftAddr), Suffrage: raft.Voter})
		routeURLs = append(routeURLs, fmt.Sprintf("nats://127.0.0.1:%d", FreePort(t)))
	}
	var servers []*natsserver.Server
	for i, p := range processes {
		_, routePort, _ := net.SplitHostPort(strings.TrimPrefix(routeURLs[i], "nats://"))
		clusterPort, err := strconv.Atoi(routePort)
		if err != nil {
			t.Fatal(err)
		}
		s, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, ServerName: p.id, JetStream: true, StoreDir: filepath.Join(p.dir, "nats"), NoLog: true, NoSigs: true,
			Cluster: natsserver.ClusterOpts{Name: "s6-n3", Host: "127.0.0.1", Port: clusterPort}, Routes: natsserver.RoutesFromStr(strings.Join(routeURLs, ",")),
			Nkeys: []*natsserver.NkeyUser{{Nkey: brokerPub}}, AuthCallout: &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}})
		if err != nil {
			t.Fatal(err)
		}
		servers = append(servers, s)
		s.Start()
	}
	t.Cleanup(func() {
		for _, s := range servers {
			s.Shutdown()
		}
		for _, s := range servers {
			s.WaitForShutdown()
		}
	})
	for _, s := range servers {
		if !s.ReadyForConnections(10 * time.Second) {
			t.Fatal("NATS not ready")
		}
	}
	n3MaintenanceWait(t, 20*time.Second, func() bool {
		for _, s := range servers {
			if s.NumRoutes() < 2 {
				return false
			}
		}
		return true
	}, processes)
	certFPs := map[string]string{}
	for _, p := range processes {
		leaf := ca.Leaf(t, p.id)
		template, err := x509.ParseCertificate(leaf.Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		template.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
		leaf.Certificate[0], err = x509.CreateCertificate(rand.Reader, template, ca.Cert, leaf.PrivateKey.(crypto.Signer).Public(), ca.Key)
		if err != nil {
			t.Fatal(err)
		}
		key, err := x509.MarshalPKCS8PrivateKey(leaf.PrivateKey)
		if err != nil {
			t.Fatal(err)
		}
		cert, err := x509.ParseCertificate(leaf.Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		certFPs[p.id] = tunnel.CertFingerprint(cert)
		sec := filepath.Join(p.dir, "credentials")
		for name, block := range map[string]*pem.Block{"cluster-ca.pem": {Type: "CERTIFICATE", Bytes: ca.Cert.Raw}, "route-cert.pem": {Type: "CERTIFICATE", Bytes: cert.Raw}, "tunnel-cert.pem": {Type: "CERTIFICATE", Bytes: cert.Raw}, "route-key.pem": {Type: "PRIVATE KEY", Bytes: key}, "tunnel-key.pem": {Type: "PRIVATE KEY", Bytes: key}} {
			if err := os.WriteFile(filepath.Join(sec, name), pem.EncodeToMemory(block), 0600); err != nil {
				t.Fatal(err)
			}
		}
		for name, seed := range map[string][]byte{"broker.nk": brokerSeed, "account.nk": accountSeed, "node-ident.nk": brokerSeed} {
			if err := os.WriteFile(filepath.Join(sec, name), seed, 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Identical initial SQL content on all voters; only the local identity marker
	// differs. Raft starts from a real three-voter configuration in its Bolt log.
	publicPort := FreePort(t)
	creationTime := time.Now() // The session creation identity is identical on every initial replica.
	for _, p := range processes {
		db, err := storage.Open(filepath.Join(p.dir, "tether.db"))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		if _, err = session.Create(db, "n3", "n3", agentFP, n3RollPIN(t), creationTime); err != nil {
			t.Fatal(err)
		}
		if err = node.Register(db, node.RegisterInput{SID: "n3", NID: "agent", ProtoVersion: proto.ProtoVersion, NatsServer: "n3-0"}, time.Now()); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(`INSERT INTO agent_provisioning(sid,nid,agent_fp) VALUES('n3','agent',?)`, agentFP); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('applied_index','0'),('applied_term','0'),('self_node_id',?)`, p.id); err != nil {
			t.Fatal(err)
		}
		for i, peer := range processes {
			if _, err = db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES(?,?,?,?,?,?,?,?,?,'VOTER',?)`, peer.id, peer.id, brokerPub, peer.id, peer.raftAddr, strings.TrimPrefix(routeURLs[i], "nats://"), peer.tunnelAddr, "127.0.0.1", certFPs[peer.id], time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
		}
		allocation, err := port.Allocate(db, "n3", "agent", "kept", echo.Addr().(*net.TCPAddr).Port, publicPort, agentFP, true, &port.Config{BandLow: publicPort, BandHigh: publicPort})
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(`UPDATE port_allocations SET home_broker='n3-0',token_hash=? WHERE port=?`, port.HashToken("n3-maintenance-token"), allocation.Port); err != nil {
			t.Fatal(err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		raftDir := filepath.Join(p.dir, "raft")
		if err = os.Mkdir(raftDir, 0700); err != nil {
			t.Fatal(err)
		}
		store, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
		if err != nil {
			t.Fatal(err)
		}
		snaps, err := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
		if err != nil {
			_ = store.Close()
			t.Fatal(err)
		}
		_, trans := raft.NewInmemTransport(raft.ServerAddress(p.raftAddr))
		cfg := raft.DefaultConfig()
		cfg.LocalID = raft.ServerID(p.id)
		err = raft.BootstrapCluster(cfg, store, store, snaps, trans, raft.Configuration{Servers: peers})
		err = errors.Join(err, trans.Close(), store.Close())
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(p.dir, "n3-product")
		if err = storeFixtureCopy(binary, path, nil); err != nil {
			t.Fatal(err)
		}
		if err = storeFixtureCopy(binary, path+".prev", nil); err != nil {
			t.Fatal(err)
		}
	}
	for _, p := range processes {
		registerN3Roll(t, p)
	}
	for i, p := range processes {
		args := []string{"serve", "--config", filepath.Join(p.dir, "broker.yaml"), "--nats-url", servers[i].ClientURL(), "--db", filepath.Join(p.dir, "tether.db"), "--admin-socket", filepath.Join(p.dir, "admin.sock"), "--tunnel-addr", p.tunnelAddr, "--tunnel-public-host", "127.0.0.1", "--auth-callout-seeds-dir", filepath.Join(p.dir, "credentials"), "--cluster-data-dir", p.dir, "--cluster-secrets-dir", filepath.Join(p.dir, "credentials"), "--cluster-raft-addr", p.raftAddr, "--cluster-manifest-listen=", "--nats-conf-path=", "--log-file", "-", "--log-json"}
		n3MaintenanceStart(t, p, filepath.Join(p.dir, "n3-product"), args)
	}
	nc, err := nats.Connect(servers[0].ClientURL(), nats.Nkey(brokerPub, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(brokerSeed, nonce) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	voters := []string{"n3-0", "n3-1", "n3-2"}
	health := func(ctx context.Context) ([]proto.ClusterHealthResp, error) {
		return cli.QueryBrokerRollHealthForVoters(ctx, nc, brokerPub, voters)
	}
	var current []proto.ClusterHealthResp
	n3MaintenanceWait(t, 20*time.Second, func() bool {
		rows, err := health(ctx)
		if err != nil {
			return false
		}
		leaders := 0
		for _, r := range rows {
			if r.WritableLeaderConfirmed {
				leaders++
			}
			if r.BrokerImage == nil || r.BrokerImage.Phase != proto.BrokerImagePhaseServing {
				return false
			}
		}
		current = rows
		return leaders == 1
	}, processes)
	trigger := func(ctx context.Context, request *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
		request.IssuedAt = time.Now().UTC().Format(time.RFC3339Nano)
		sig, err := auth.SignWithSeed(accountSeed, proto.CanonicalUpgradeReqBytes(request))
		if err != nil {
			return nil, err
		}
		request.Sig = hex.EncodeToString(sig)
		data, err := json.Marshal(request)
		if err != nil {
			return nil, err
		}
		rpcCtx, stop := context.WithTimeout(ctx, 6*time.Second)
		defer stop()
		message, err := nc.RequestWithContext(rpcCtx, proto.SubjCtrlClusterUpgrade(brokerPub), data)
		if err != nil {
			return nil, err
		}
		var response proto.ClusterUpgradeResp
		err = json.Unmarshal(message.Data, &response)
		return &response, err
	}
	leaderID := ""
	for _, row := range current {
		if row.WritableLeaderConfirmed {
			leaderID = row.NodeID
		}
	}
	if leaderID != "n3-0" {
		response, err := trigger(ctx, &proto.ClusterUpgradeReq{Op: "transfer-leader", TargetNode: leaderID, TransferTo: "n3-0"})
		if err != nil || !response.OK {
			t.Fatalf("setup transfer: %v %+v", err, response)
		}
		n3MaintenanceWait(t, 10*time.Second, func() bool {
			rows, err := health(ctx)
			if err != nil {
				return false
			}
			for _, r := range rows {
				if r.NodeID == "n3-0" && r.WritableLeaderConfirmed {
					return true
				}
			}
			return false
		}, processes)
	}
	agentDir := filepath.Join(root, "agent")
	if err = os.MkdirAll(filepath.Join(agentDir, "agent", "n3", "keys"), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(agentDir, "agent", "n3", "keys", "agent.nk"), agentSeed, 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(agentDir, "agent", "n3", "agent.yaml"), []byte("account_pub: "+accountPub+"\nproxy:\n  participate: false\n"), 0600); err != nil {
		t.Fatal(err)
	}
	a := &n3MaintenanceProcess{dir: agentDir, id: "real-agent"}
	n3MaintenanceStart(t, a, binary, []string{"agent", "--session", "n3", "--nid", "agent", "--nats-url", servers[0].ClientURL(), "--tunnel-addr", processes[0].tunnelAddr, "--log-file", "-", "--log-json"})
	n3MaintenanceWait(t, 15*time.Second, func() bool {
		_, err := nc.Request(proto.SubjCmdForwarded("n3", "agent", proto.ClaimProbeVerb), nil, 100*time.Millisecond)
		return err == nil
	}, append(processes, a))
	reads := make([]*sql.DB, 3)
	for i, p := range processes {
		db, err := storage.OpenReadOnly(filepath.Join(p.dir, "tether.db"))
		if err != nil {
			t.Fatal(err)
		}
		reads[i] = db
		t.Cleanup(func() { _ = db.Close() })
	}
	n3MaintenanceWait(t, 10*time.Second, func() bool {
		var count int
		err := reads[0].QueryRow(`SELECT COUNT(*) FROM cluster_meta WHERE key='broker_image_observer.n3/agent'`).Scan(&count)
		return err == nil && count == 1
	}, append(processes, a))
	before, err := port.LookupByTokenHash(reads[0], port.HashToken("n3-maintenance-token"))
	if err != nil {
		t.Fatal(err)
	}
	flowClient := tunnel.NewClient(processes[0].tunnelAddr, "n3", "agent", func(int) (int, error) { return before.LocalPort, nil }, testharness.SilentLog())
	if err := flowClient.SetFlowConfig(tunnel.FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "n3-original", BootID: "n3-original-boot", OwnerGen: 1}); err != nil {
		t.Fatal(err)
	}
	flowCtx, stopFlow := context.WithCancel(t.Context())
	flowClient.Start(flowCtx)
	t.Cleanup(func() {
		stopFlow()
		if err := tunnel.StopClient(flowClient, 5*time.Second); err != nil {
			t.Error(err)
		}
	})
	if err := flowClient.OpenHome(flowCtx, before.Port, before.LocalPort, "n3-maintenance-token", processes[0].tunnelAddr, before.Epoch, proto.CertPins{Current: certFPs["n3-0"]}); err != nil {
		t.Fatal(err)
	}
	original, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", fmt.Sprint(before.Port)), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = original.Close() })
	exchange := func(body string) {
		t.Helper()
		_ = original.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.WriteString(original, body); err != nil {
			t.Fatal(err)
		}
		reply := make([]byte, len(body))
		if _, err := io.ReadFull(original, reply); err != nil || string(reply) != body {
			t.Fatalf("original TCP byte oracle: %q %v", reply, err)
		}
	}
	exchange("original-before-exec")
	if accepted.Load() != 1 {
		t.Fatal("original target accept was not unique")
	}
	pid := processes[0].cmd.Process.Pid
	ticks, err := storeFixtureStartTicks(pid)
	if err != nil {
		t.Fatal(err)
	}
	agentPID := a.cmd.Process.Pid
	agentTicks, err := storeFixtureStartTicks(agentPID)
	if err != nil {
		t.Fatal(err)
	}
	candidate := filepath.Join(processes[0].dir, "candidate")
	if err = storeFixtureCopy(binary, candidate, []byte("\nN3 candidate\n")); err != nil {
		t.Fatal(err)
	}
	sha, err := storeFixtureHash(candidate)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Rename(candidate, filepath.Join(processes[0].dir, "n3-product")); err != nil {
		t.Fatal(err)
	}
	actorFP, err := auth.FingerprintFromActor(brokerPub)
	if err != nil {
		t.Fatal(err)
	}
	ports := clusterroll.ClusterPorts{Trigger: trigger, Health: health, ActorFP: actorFP, Home: filepath.Join(root, "ctl"), Output: io.Discard}
	parent, plan, err := clusterroll.PrepareCluster(ctx, ports, []string{"n3-0"}, proto.ReleaseVersion, sha)
	if err != nil {
		t.Fatal("prepare original plan:", err)
	}
	cutID := plan.Steps[0].Grant.Handle.CutID
	var operations []string
	var prepared *brokermaint.Record
	var refusal string
	interrupted := false
	starts := 0
	ports.Trigger = func(ctx context.Context, req *proto.ClusterUpgradeReq) (*proto.ClusterUpgradeResp, error) {
		var transaction struct {
			Op string `json:"op"`
		}
		_ = json.Unmarshal(req.Upgrade, &transaction)
		op := req.Op
		if op == "upgrade-tx" {
			op = transaction.Op
		}
		if op == upgradewire.OpStart {
			starts++
		}
		if op == "maintenance" || op == "transfer-leader" || op == upgradewire.OpStart {
			operations = append(operations, op)
		}
		r, err := trigger(ctx, req)
		if err == nil && !r.OK {
			refusal = op + ": " + r.Error
		}
		if err == nil && req.Op == "maintenance" && req.Maintenance == nil && r.Maintenance != nil {
			copy := *r.Maintenance
			prepared = &copy
		}
		// Lose the durable child-result reply, then replace the ctl driver.
		// The new driver must load the daemon plan and query the SAME child.
		if err == nil && op == "roll.apply" && !interrupted {
			var request rollcontrol.Request
			if json.Unmarshal(req.Upgrade, &request) == nil && request.Event.Action.Kind == upgradetx.ActionDispatch && request.Event.Action.State == upgradetx.ActionDurable {
				interrupted = true
				return nil, errors.New("original ctl reply lost after durable child result")
			}
		}
		return r, err
	}
	rollDone := make(chan error, 1)
	rollJoined := false
	defer func() {
		if !rollJoined {
			cancel()
			_ = os.WriteFile(filepath.Join(processes[0].dir, "release-cut"), nil, 0600)
			select {
			case <-rollDone:
			case <-time.After(10 * time.Second):
				t.Error("roll driver did not join")
			}
		}
	}()
	go func() {
		_, err := clusterroll.RunCluster(ctx, ports, parent, plan)
		if err != nil && interrupted {
			err = resumeN3RollCLI(ctx, t, binary, ports.Home, servers[0].ClientURL(), filepath.Join(processes[0].dir, "credentials", "account.nk"), parent.Handle.TxID, brokerSeed)
		}
		rollDone <- err
	}()
	// The driver owns operations/prepared until it joins. Only files and SQL
	// are inspected while its callback is running.
	n3MaintenanceWait(t, 15*time.Second, func() bool {
		select {
		case err := <-rollDone:
			rollJoined = true
			for _, p := range processes {
				if data, e := os.ReadFile(filepath.Join(p.dir, "arm-error.json")); e == nil {
					t.Logf("%s arm: %s", p.id, data)
				}
				if data, e := os.ReadFile(filepath.Join(p.dir, "observer-error.json")); e == nil {
					t.Logf("%s observer: %s", p.id, data)
				}
				if data, e := os.ReadFile(filepath.Join(p.dir, "record-mismatch.json")); e == nil {
					t.Logf("%s record: %s", p.id, data)
				}
			}
			t.Fatalf("roll returned before cut: %v; broker refusal: %s", err, refusal)
		default:
		}
		_, err := os.Stat(filepath.Join(processes[0].dir, "cut.json"))
		return err == nil
	}, append(processes, a))
	for i, p := range processes {
		data, err := os.ReadFile(filepath.Join(p.dir, "observer.json"))
		if err != nil {
			t.Fatal(err)
		}
		var ack proto.BrokerMaintenanceAck
		if json.Unmarshal(data, &ack) != nil || !ack.Accepted || ack.CutID != cutID || ack.RemainingNS <= 0 || ack.ProofDigest == "" {
			t.Fatalf("voter %d did not install real proof: %s", i, data)
		}
		current, err := cluster.ReadBrokerMaintenance(reads[i], "n3-0")
		if err != nil || current.Phase != brokermaint.MaintActive || current.CutID != cutID {
			t.Fatalf("voter maintenance readback: %v %+v", err, current)
		}
	}
	data, err := os.ReadFile(filepath.Join(processes[0].dir, "cut.json"))
	if err != nil {
		t.Fatal(err)
	}
	var cut struct {
		CutID       string `json:"cut_id"`
		CutDeadline int64  `json:"cut_deadline"`
		History     struct {
			Cache *struct {
				Observations []struct{ Lease brokermaint.LocalLease }
			} `json:"maintenance_cache"`
		} `json:"history"`
	}
	if json.Unmarshal(data, &cut) != nil || cut.CutID != cutID || cut.CutDeadline <= 0 || cut.History.Cache == nil || len(cut.History.Cache.Observations) == 0 {
		t.Fatalf("sealed history omitted accepted cut: %s", data)
	}
	if err = os.WriteFile(filepath.Join(processes[0].dir, "release-cut"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-rollDone:
		rollJoined = true
		if err != nil {
			for _, p := range processes {
				t.Logf("%s: %s", p.id, p.logs.String())
			}
			t.Fatal(err)
		}
	case <-ctx.Done():
		for _, p := range processes {
			t.Logf("%s: %s", p.id, p.logs.String())
		}
		t.Fatal(ctx.Err())
	}
	if len(operations) < 4 || operations[0] != "transfer-leader" || operations[1] != "maintenance" || operations[2] != upgradewire.OpStart || operations[len(operations)-1] != "maintenance" {
		t.Fatalf("roll did not transfer, prepare, cut and end: %v", operations)
	}
	journal, _, err := clusterroll.ClusterTransport(ctx, ports, parent, plan)
	if err != nil {
		t.Fatal(err)
	}
	settled, err := journal.Load(parent.Handle.Key, parent.Handle.TxID)
	if err != nil || settled.Historical.Forward.Phase != upgradetx.PhaseCompleted || settled.Historical.Forward.Outcome != upgradetx.OutcomeSuccess || !upgradetx.SameCut(settled.Historical.Forward.Handle, parent.Handle) {
		t.Fatalf("replacement ctl returned before durable parent completion: %v %+v", err, settled.Historical.Forward)
	}
	if !interrupted || starts != 1 {
		t.Fatalf("resume did not preserve the original execution: interrupted=%v starts=%d", interrupted, starts)
	}
	if prepared == nil || prepared.Phase != brokermaint.MaintActive || prepared.ExpiresBootNS-prepared.IssuedBootNS > int64(brokermaint.MaxLifetime) {
		t.Fatal("missing fixed lifetime preparation")
	}
	n3MaintenanceWait(t, 5*time.Second, func() bool {
		for _, db := range reads {
			ended, err := cluster.ReadBrokerMaintenance(db, "n3-0")
			if err != nil || ended.Phase != brokermaint.MaintEnded {
				return false
			}
		}
		return true
	}, processes)
	exchange("original-after-exec")
	if accepted.Load() != 1 {
		t.Fatal("broker reload replaced the original target socket")
	}
	proofData, err := os.ReadFile(filepath.Join(processes[0].dir, "current-proof.json"))
	if err != nil {
		t.Fatal(err)
	}
	var currentProof struct {
		CutID     string `json:"cut_id"`
		Protected bool   `json:"protected"`
		Expiry    int64  `json:"expiry"`
	}
	if json.Unmarshal(proofData, &currentProof) != nil || !currentProof.Protected || currentProof.CutID != cutID || currentProof.Expiry != cut.History.Cache.Observations[0].Lease.ExpiresLocalBootNS {
		t.Fatalf("import lost/rebased original protection: %s", proofData)
	}
	for i, db := range reads {
		allocation, err := port.LookupByTokenHash(db, before.TokenHash)
		if err != nil || allocation.HomeBroker != before.HomeBroker || allocation.Epoch != before.Epoch || allocation.Port != before.Port || allocation.State != before.State {
			t.Fatalf("voter %d migrated/rotated upgrading home: %v %+v", i, err, allocation)
		}
		ended, err := cluster.ReadBrokerMaintenance(db, "n3-0")
		if err != nil || ended.Phase != brokermaint.MaintEnded || ended.CutID != cutID || ended.ExpiresBootNS != prepared.ExpiresBootNS {
			t.Fatalf("maintenance did not end without renewed lifetime: %v %+v", err, ended)
		}
	}
	afterTicks, err := storeFixtureStartTicks(pid)
	if err != nil || afterTicks != ticks {
		t.Fatal("broker exec replaced PID/startticks")
	}
	afterAgentTicks, err := storeFixtureStartTicks(agentPID)
	if err != nil || afterAgentTicks != agentTicks {
		t.Fatal("broker-only roll replaced colocated agent")
	}
	status, err := (&adminsock.Client{Path: filepath.Join(processes[0].dir, "admin.sock"), Timeout: time.Second}).Call(adminsock.Request{Op: adminsock.OpRuntime})
	if err != nil || status.Runtime == nil || !cli.BrokerImageComplete(status.Runtime.BrokerImage, sha, 2, cutID) {
		t.Fatalf("candidate did not become serving with accepted CutID: %v %+v", err, status)
	}
	t.Logf("same-PID N3 roll: broker=%d/%d agent=%d/%d sha=%s cut=%s home=%s epoch=%d", pid, ticks, agentPID, agentTicks, sha, cutID, before.HomeBroker, before.Epoch)
}
