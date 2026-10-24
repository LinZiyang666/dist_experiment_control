//go:build linux

package main

// Three registered serve processes use real SQLite/Raft journals, signed NATS
// requests, root publication and same-PID exec. Only installation and the wire
// observer are fixtures; no responder manufactures transaction evidence.
// origin: hot-update S8 plan §3.1 / §4.2 / §5.1 / §9.4; S8 internal review upgrade-maketest-2 F1-F5

import (
	"crypto"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"debug/buildinfo"
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
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/upgradecli"
	"github.com/LinZiyang666/tether/internal/upgradelock"
	"github.com/LinZiyang666/tether/internal/upgradestage"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/clusterharness"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/spf13/cobra"
)

const signedRollRelease = "v0.0.0-signed-roll"

type signedRollProcess struct {
	dir, id, raftAddr, tunnelAddr string
	cmd                           *exec.Cmd
	done                          chan error
	logs                          syncBuffer
	original                      upgradetx.Owner
}
type signedRollFixture struct {
	root, home, binary, sha, actor string
	seed                           []byte
	nc                             *nats.Conn
	processes                      []*signedRollProcess
	agent                          *signedRollProcess
	mu                             sync.Mutex
	requests                       []protoRollRequest
}
type protoRollRequest struct{ Op, Target, Holder, Inner string }

func signedRollWrite(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatal(err)
	}
}
func signedRollCopy(t *testing.T, source, target string, suffix []byte) {
	t.Helper()
	in, err := os.Open(source)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := in.Close(); err != nil {
			t.Errorf("close copied source: %v", err)
		}
	}()
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0755)
	if err != nil {
		t.Fatal(err)
	}
	_, err = io.Copy(out, in)
	if err == nil && len(suffix) > 0 {
		_, err = out.Write(suffix)
	}
	if err = errors.Join(err, out.Close()); err != nil {
		t.Fatal(err)
	}
}
func signedRollSHA(t *testing.T, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Errorf("close hashed image: %v", err)
		}
	}()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}
func buildSignedRollArtifact(t *testing.T, keeper ...bool) string {
	t.Helper()
	binary := filepath.Join(testharness.GuardProcessDirectory(t, t.TempDir()), "roll-product")
	var overlayArgs []string
	if len(keeper) > 0 && keeper[0] {
		// Keep the original negative port on the real daemon,
		// while the live command owns its compressed courier timer. The
		// negative preflight port makes a live roll return through its HALT path.
		// origin: hot-update S8 plan §6.3 / L21; S8 internal review upgrade-maketest-2 F1
		repo, err := filepath.Abs("../..")
		if err != nil {
			t.Fatal(err)
		}
		replacements := map[string]string{}
		for name, edit := range map[string]func(string) string{
			"internal/brokerupgrade/rollcontrol/service.go": func(source string) string {
				return strings.Replace(source, `case "roll.preflight":`, `case "roll.preflight":
    if len(r.Plan.Steps)>0 { time.Sleep(50*time.Millisecond);return out,Error(upgradetx.CodeDeferred,"injected preflight refusal",false) }`, 1)
			},
		} {
			path := filepath.Join(repo, name)
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			patched := edit(string(data))
			if patched == string(data) {
				t.Fatalf("keeper fixture overlay anchor missing: %s", name)
			}
			output := filepath.Join(filepath.Dir(binary), filepath.Base(path))
			signedRollWrite(t, output, []byte(patched), 0600)
			replacements[path] = output
		}
		data, err := json.Marshal(map[string]any{"Replace": replacements})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(filepath.Dir(binary), "keeper-overlay.json")
		signedRollWrite(t, path, data, 0600)
		overlayArgs = []string{"-overlay=" + path}
	}
	// Keep build explicit so the single go-test entry gate can prove this
	// nested artifact builder never starts another test runner.
	// origin: S10 internal review ARCH-REDS-3 F2; hot-update S10 plan §11.1
	cmd := exec.CommandContext(t.Context(), "go", "build", "-race")
	cmd.Args = append(cmd.Args, overlayArgs...)
	cmd.Args = append(cmd.Args, "-ldflags", "-X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion="+signedRollRelease, "-o", binary, ".")
	cmd.Env = append(os.Environ(), "GORACE=atexit_sleep_ms=0")
	if out, err := testharness.CombinedOutput(cmd); err != nil {
		t.Fatalf("build signed roll product: %v\n%s", err, out)
	}
	info, err := buildinfo.ReadFile(binary)
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{}
	for _, setting := range info.Settings {
		settings[setting.Key] = setting.Value
	}
	t.Logf("product path=%s sha256=%s toolchain=%s race=%s commit=%s dirty=%s", binary, signedRollSHA(t, binary), info.GoVersion, settings["-race"], settings["vcs.revision"], settings["vcs.modified"])
	return binary
}
func signedRollStart(t *testing.T, p *signedRollProcess, binary string, args ...string) {
	t.Helper()
	p.cmd = exec.Command(binary, args...)
	p.cmd.Dir = p.dir
	p.cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "HOME="+p.dir, "TETHER_HOME="+p.dir, "GORACE=atexit_sleep_ms=0")
	p.cmd.Stdout, p.cmd.Stderr = &p.logs, &p.logs
	// Hand future listeners to their process only when it is ready to start.
	// origin: S8 internal review ROLL-SETTLEMENT F4; hot-update S8 plan L21
	signedRollHandoffAddress(t, p.raftAddr)
	if p.raftAddr != "" {
		signedRollHandoffAddress(t, p.tunnelAddr)
	}
	wait, err := testharness.StartProcess(p.cmd)
	if err != nil {
		t.Fatal(err)
	}
	p.done = make(chan error, 1)
	go func() { p.done <- wait() }()
	t.Cleanup(func() {
		_ = p.cmd.Process.Signal(os.Interrupt)
		select {
		case err := <-p.done:
			if err != nil {
				t.Errorf("%s stop: %v\n%s", p.id, err, p.logs.String())
			}
		case <-time.After(15 * time.Second):
			_ = p.cmd.Process.Kill()
			<-p.done
			t.Errorf("%s failed to join\n%s", p.id, p.logs.String())
		}
	})
	p.original, err = upgradestage.ProcessOwner(p.cmd.Process.Pid)
	if err != nil {
		t.Fatal(err)
	}
}

func startSignedRollFixture(t *testing.T, binary string, withAgent bool, runningTargets ...string) *signedRollFixture {
	t.Helper()
	f := &signedRollFixture{root: testharness.GuardProcessDirectory(t, t.TempDir()), binary: binary}
	f.home = filepath.Join(f.root, "ctl")
	if err := cli.WriteCurrentSession(f.home, "signed-roll"); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(f.home, 0700); err != nil {
		t.Fatal(err)
	}
	brokerSeed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	f.actor, err = auth.PublicKeyFromSeed(brokerSeed)
	if err != nil {
		t.Fatal(err)
	}
	f.seed, _ = cliExternalAccount(t)
	accountPub, err := auth.PublicKeyFromSeed(f.seed)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := auth.FingerprintFromActor(f.actor)
	if err != nil {
		t.Fatal(err)
	}
	ca := clusterharness.NewRouteCA(t, "signed-roll")
	peers := []raft.Server{}
	for i := 1; i <= 3; i++ {
		p := &signedRollProcess{dir: filepath.Join(f.root, fmt.Sprint(i)), id: fmt.Sprintf("brk%d", i), raftAddr: fmt.Sprintf("127.0.0.1:%d", signedRollPort(t)), tunnelAddr: fmt.Sprintf("127.0.0.1:%d", signedRollPort(t))}
		if err := os.MkdirAll(filepath.Join(p.dir, "credentials"), 0700); err != nil {
			t.Fatal(err)
		}
		f.processes = append(f.processes, p)
		peers = append(peers, raft.Server{ID: raft.ServerID(p.id), Address: raft.ServerAddress(p.raftAddr), Suffrage: raft.Voter})
	}
	// Each ingress is an actual routed JetStream peer, matching the three-voter
	// replica policy used by the production continuation directory.
	var servers []*natsserver.Server
	var routes []string
	for range f.processes {
		routes = append(routes, fmt.Sprintf("nats://127.0.0.1:%d", signedRollPort(t)))
	}
	for i, p := range f.processes {
		_, port, _ := net.SplitHostPort(strings.TrimPrefix(routes[i], "nats://"))
		n, err := strconv.Atoi(port)
		if err != nil {
			t.Fatal(err)
		}
		ns, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, ServerName: p.id, JetStream: true, StoreDir: filepath.Join(p.dir, "nats"), NoLog: true, NoSigs: true, Cluster: natsserver.ClusterOpts{Name: "signed-roll", Host: "127.0.0.1", Port: n}, Routes: natsserver.RoutesFromStr(strings.Join(routes, ",")), Nkeys: []*natsserver.NkeyUser{{Nkey: f.actor}}, AuthCallout: &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{f.actor}}})
		if err != nil {
			t.Fatal(err)
		}
		servers = append(servers, ns)
		signedRollHandoffPort(t, n)
		ns.Start()
	}
	t.Cleanup(func() {
		for _, ns := range servers {
			ns.Shutdown()
		}
		for _, ns := range servers {
			ns.WaitForShutdown()
		}
	})
	for _, ns := range servers {
		if !ns.ReadyForConnections(10 * time.Second) {
			t.Fatal("signed roll NATS not ready")
		}
	}
	if !testharness.WaitFor(t, 20*time.Second, 20*time.Millisecond, func() bool {
		for _, ns := range servers {
			if ns.NumRoutes() < 2 {
				return false
			}
		}
		return true
	}) {
		t.Fatal("signed roll NATS routes not ready")
	}
	ns := servers[0]
	f.nc, err = nats.Connect(ns.ClientURL(), nats.CustomInboxPrefix(auth.InboxPrefixFor(f.actor)), nats.Nkey(f.actor, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(brokerSeed, nonce) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(f.nc.Close)
	if !testharness.WaitFor(t, 20*time.Second, 20*time.Millisecond, func() bool {
		leaders := 0
		for _, server := range servers {
			if !server.JetStreamIsClustered() || !server.JetStreamIsCurrent() {
				return false
			}
			if server.JetStreamIsLeader() {
				leaders++
				if len(server.JetStreamClusterPeers()) != 3 {
					return false
				}
			}
		}
		if leaders != 1 {
			return false
		}
		js, err := f.nc.JetStream()
		if err != nil {
			return false
		}
		_, err = js.AccountInfo(nats.MaxWait(100 * time.Millisecond))
		return err == nil
	}) {
		t.Fatal("signed roll JetStream metadata did not converge")
	}
	js, err := jetstream.New(f.nc)
	if err != nil {
		t.Fatal(err)
	}
	if err = jsstream.EnsureEventsStream(t.Context(), js, 3); err != nil {
		t.Fatal(err)
	}
	if err = jsstream.EnsureHistoryStream(t.Context(), js, "signed-roll", 3); err != nil {
		t.Fatal(err)
	}
	certFP := map[string]string{}
	for _, p := range f.processes {
		leaf := ca.Leaf(t, p.id)
		cert, err := x509.ParseCertificate(leaf.Certificate[0])
		if err != nil {
			t.Fatal(err)
		}
		cert.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
		der, err := x509.CreateCertificate(rand.Reader, cert, ca.Cert, leaf.PrivateKey.(crypto.Signer).Public(), ca.Key)
		if err != nil {
			t.Fatal(err)
		}
		cert, err = x509.ParseCertificate(der)
		if err != nil {
			t.Fatal(err)
		}
		key, err := x509.MarshalPKCS8PrivateKey(leaf.PrivateKey)
		if err != nil {
			t.Fatal(err)
		}
		certFP[p.id] = tunnel.CertFingerprint(cert)
		sec := filepath.Join(p.dir, "credentials")
		for name, block := range map[string]*pem.Block{"cluster-ca.pem": {Type: "CERTIFICATE", Bytes: ca.Cert.Raw}, "route-cert.pem": {Type: "CERTIFICATE", Bytes: der}, "tunnel-cert.pem": {Type: "CERTIFICATE", Bytes: der}, "route-key.pem": {Type: "PRIVATE KEY", Bytes: key}, "tunnel-key.pem": {Type: "PRIVATE KEY", Bytes: key}} {
			signedRollWrite(t, filepath.Join(sec, name), pem.EncodeToMemory(block), 0600)
		}
		for name, seed := range map[string][]byte{"broker.nk": brokerSeed, "node-ident.nk": brokerSeed, "account.nk": f.seed} {
			signedRollWrite(t, filepath.Join(sec, name), seed, 0600)
		}
	}
	agentDir := filepath.Join(f.root, "agent")
	if withAgent {
		if err = os.MkdirAll(filepath.Join(agentDir, "agent", "signed-roll", "keys"), 0700); err != nil {
			t.Fatal(err)
		}
		signedRollWrite(t, filepath.Join(agentDir, "agent", "signed-roll", "keys", "agent.nk"), brokerSeed, 0600)
		signedRollWrite(t, filepath.Join(agentDir, "agent", "signed-roll", "agent.yaml"), []byte("account_pub: "+accountPub+"\nproxy:\n  participate: false\n"), 0600)
	}
	created := time.Now()
	for processIndex, p := range f.processes {
		db, err := storage.Open(filepath.Join(p.dir, "tether.db"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = session.Create(db, "signed-roll", "signed-roll", fp, clusterharnessPIN(t), created); err != nil {
			t.Fatal(err)
		}
		if withAgent {
			if err = node.Register(db, node.RegisterInput{SID: "signed-roll", NID: "agent-1", ProtoVersion: proto.ProtoVersion}, created); err != nil {
				t.Fatal(err)
			}
			if _, err = db.Exec(`INSERT INTO agent_provisioning(sid,nid,agent_fp) VALUES('signed-roll','agent-1',?)`, fp); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = db.Exec(`INSERT INTO cluster_meta(key,value) VALUES('applied_index','0'),('applied_term','0'),('self_node_id',?)`, p.id); err != nil {
			t.Fatal(err)
		}
		for i, peer := range f.processes {
			if _, err = db.Exec(`INSERT INTO cluster_nodes(node_id,name,node_ident_pub,nats_server_id,raft_addr,nats_route,tunnel_addr,public_host,cert_fp,phase,added_at) VALUES(?,?,?,?,?,?,?,?,?,'VOTER',?)`, peer.id, peer.id, f.actor, peer.id, peer.raftAddr, strings.TrimPrefix(routes[i], "nats://"), peer.tunnelAddr, "127.0.0.1", certFP[peer.id], created.UTC().Format(time.RFC3339Nano)); err != nil {
				t.Fatal(err)
			}
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}
		raftDir := filepath.Join(p.dir, "raft")
		if err = os.Mkdir(raftDir, 0700); err != nil {
			t.Fatal(err)
		}
		bolt, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
		if err != nil {
			t.Fatal(err)
		}
		snaps, err := raft.NewFileSnapshotStore(raftDir, 2, io.Discard)
		if err != nil {
			t.Fatal(err)
		}
		_, transport := raft.NewInmemTransport(raft.ServerAddress(p.raftAddr))
		cfg := raft.DefaultConfig()
		cfg.LocalID = raft.ServerID(p.id)
		err = raft.BootstrapCluster(cfg, bolt, bolt, snaps, transport, raft.Configuration{Servers: peers})
		if err = errors.Join(err, transport.Close(), bolt.Close()); err != nil {
			t.Fatal(err)
		}
		canonical := filepath.Join(p.dir, "roll-product")
		// Skip participants must actually run the requested bytes before business.
		// They start those bytes normally; no transaction receipt is fabricated.
		// origin: S8 internal review SIGNED-ROLL-FIXTURES F5; S8 plan §4.2
		var initialSuffix []byte
		for _, id := range runningTargets {
			if p.id == id {
				initialSuffix = []byte("\nsigned roll candidate\n")
			}
		}
		signedRollCopy(t, binary, canonical, initialSuffix)
		config := filepath.Join(p.dir, "broker.yaml")
		configBody := "broker:\n  storage:\n    db: " + filepath.Join(p.dir, "tether.db") + "\n"
		if withAgent && p.id == "brk1" {
			configBody += "  cluster:\n    colocated_agent_nid: agent-1\n"
		}
		signedRollWrite(t, config, []byte(configBody), 0600)
		role := upgradecli.BrokerBootRole(p.dir, config, "", uint32(os.Geteuid()))
		if err = os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.Chmod(role.ControlDir, 0700); err != nil {
			t.Fatal(err)
		}
		if _, err = upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: canonical, ControlDir: role.ControlDir, DeploymentID: "signed-roll-" + p.id, Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "broker-" + p.id}}}); err != nil {
			t.Fatal(err)
		}
		signedRollStart(t, p, canonical, "serve", "--config", config, "--nats-url", servers[processIndex].ClientURL(), "--db", filepath.Join(p.dir, "tether.db"), "--admin-socket", filepath.Join(p.dir, "admin.sock"), "--tunnel-addr", p.tunnelAddr, "--tunnel-public-host", "127.0.0.1", "--auth-callout-seeds-dir", filepath.Join(p.dir, "credentials"), "--cluster-data-dir", p.dir, "--cluster-secrets-dir", filepath.Join(p.dir, "credentials"), "--cluster-raft-addr", p.raftAddr, "--cluster-manifest-listen=", "--nats-conf-path=", "--log-file", "-", "--log-json")
	}
	mustSub(t, f.nc, proto.SubjCtrlClusterUpgrade(f.actor), func(m *nats.Msg) {
		var req proto.ClusterUpgradeReq
		if err := json.Unmarshal(m.Data, &req); err != nil {
			t.Error(err)
			return
		}
		sig, err := hex.DecodeString(req.Sig)
		if err != nil || auth.VerifySignature(accountPub, proto.CanonicalUpgradeReqBytes(&req), sig) != nil {
			t.Error("roll observer saw an invalid account signature")
			return
		}
		var inner struct {
			Op string `json:"op"`
		}
		_ = json.Unmarshal(req.Upgrade, &inner)
		f.mu.Lock()
		f.requests = append(f.requests, protoRollRequest{req.Op, req.TargetNode, req.Holder, inner.Op})
		f.mu.Unlock()
	})
	mustSub(t, f.nc, proto.SubjCtrlNodeList(f.actor, "signed-roll"), func(*nats.Msg) {
		f.mu.Lock()
		f.requests = append(f.requests, protoRollRequest{Op: "node-list"})
		f.mu.Unlock()
	})
	mustSub(t, f.nc, "_INBOX.>", func(m *nats.Msg) {
		var out struct {
			Code    string          `json:"code"`
			Error   string          `json:"error"`
			Upgrade json.RawMessage `json:"upgrade"`
		}
		if json.Unmarshal(m.Data, &out) == nil && out.Code != "" {
			var inner struct {
				Detail string `json:"detail"`
			}
			_ = json.Unmarshal(out.Upgrade, &inner)
			t.Logf("control refusal: %s %s %s", out.Code, out.Error, inner.Detail)
		}
	})
	if err = f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	f.await(t, 20*time.Second, func() bool {
		rows, err := cli.QueryBrokerRollHealthForVoters(t.Context(), f.nc, f.actor, []string{"brk1", "brk2", "brk3"})
		if err != nil || len(rows) != 3 {
			return false
		}
		leaders := 0
		for _, row := range rows {
			if row.WritableLeaderConfirmed {
				leaders++
			}
			if row.BrokerImage == nil || row.BrokerImage.Phase != "serving" {
				return false
			}
		}
		return leaders == 1
	})
	if withAgent {
		f.agent = &signedRollProcess{dir: agentDir, id: "agent-1"}
		agentCanonical := filepath.Join(agentDir, "roll-agent")
		signedRollCopy(t, binary, agentCanonical, nil)
		role := upgradecli.AgentBootRole(agentDir, "signed-roll", "agent-1", uint32(os.Geteuid()))
		if err = os.MkdirAll(filepath.Join(role.ControlDir, "artifacts"), 0700); err != nil {
			t.Fatal(err)
		}
		if err = os.Chmod(role.ControlDir, 0700); err != nil {
			t.Fatal(err)
		}
		if _, err = upgradelock.RegisterDomain(upgradelock.Registration{CanonicalPath: agentCanonical, ControlDir: role.ControlDir, DeploymentID: "signed-roll-agent", Participants: []upgradelock.Participant{{Key: role.Want, ServiceID: "agent-1"}}}); err != nil {
			t.Fatal(err)
		}
		signedRollStart(t, f.agent, agentCanonical, "agent", "--session", "signed-roll", "--nid", "agent-1", "--nats-url", ns.ClientURL(), "--tunnel-addr", f.processes[0].tunnelAddr, "--log-file", "-", "--log-json")
		f.await(t, 15*time.Second, func() bool {
			m, e := f.nc.Request(proto.SubjCtrlNodeList(f.actor, "signed-roll"), nil, time.Second)
			if e != nil {
				return false
			}
			var out proto.NodeListResp
			if json.Unmarshal(m.Data, &out) != nil {
				return false
			}
			return len(out.Nodes) == 1 && out.Nodes[0].NID == "agent-1" && out.Nodes[0].Status == "ONLINE"
		})
	}
	if withAgent {
		mustSub(t, f.nc, proto.SubjCmdForwarded("signed-roll", "agent-1", "upgrade-tx"), func(m *nats.Msg) {
			// Observe the authenticated inner request, not the forwarding envelope.
			// origin: S8 internal review SIGNED-ROLL-FIXTURES F3
			var envelope proto.UpgradeTxForwardedReq
			if err := json.Unmarshal(m.Data, &envelope); err != nil {
				t.Error(err)
				return
			}
			var req struct {
				Op string `json:"op"`
			}
			if err := json.Unmarshal(envelope.Request, &req); err != nil {
				t.Error(err)
				return
			}
			f.mu.Lock()
			f.requests = append(f.requests, protoRollRequest{Op: "agent-tx", Target: "agent-1", Inner: req.Op})
			f.mu.Unlock()
		})
		candidate := filepath.Join(agentDir, "agent-candidate")
		signedRollCopy(t, binary, candidate, []byte("\nsigned roll candidate\n"))
		if err = os.Rename(candidate, filepath.Join(agentDir, "roll-agent")); err != nil {
			t.Fatal(err)
		}
	}
	// Stage new bytes at the canonical installation pathname by rename. /proc
	// continues to name the old inode; publication must pin that running source.
	for _, p := range f.processes {
		candidate := filepath.Join(p.dir, "candidate")
		signedRollCopy(t, binary, candidate, []byte("\nsigned roll candidate\n"))
		if err = os.Rename(candidate, filepath.Join(p.dir, "roll-product")); err != nil {
			t.Fatal(err)
		}
	}
	f.sha = signedRollSHA(t, filepath.Join(f.processes[0].dir, "roll-product"))
	t.Logf("candidate sha256=%s ctl_home=%s", f.sha, f.home)
	return f
}

func clusterharnessPIN(t *testing.T) string {
	t.Helper()
	pin, err := signedRollPIN()
	if err != nil {
		t.Fatal(err)
	}
	return pin
}

var signedRollPIN = sync.OnceValues(func() (string, error) { return auth.HashPIN("signed-roll-fixture") })

func (f *signedRollFixture) await(t *testing.T, within time.Duration, pred func() bool) {
	t.Helper()
	if testharness.WaitFor(t, within, 20*time.Millisecond, pred) {
		return
	}
	for _, p := range f.processes {
		t.Logf("%s: %s", p.id, p.logs.String())
	}
	t.Fatal("signed roll fixture did not reach the required observation")
}
func (f *signedRollFixture) wire() []protoRollRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]protoRollRequest(nil), f.requests...)
}
func (f *signedRollFixture) command(t *testing.T) *cobra.Command {
	t.Helper()
	cmd := newClusterUpgradeCmd()
	if err := cmd.Flags().Set("home", f.home); err != nil {
		t.Fatal(err)
	}
	cmd.SetContext(t.Context())
	cmd.SetOut(&syncBuffer{})
	return cmd
}
func (f *signedRollFixture) status(t *testing.T, p *signedRollProcess) proto.BrokerImageStatus {
	t.Helper()
	resp, err := (&adminsock.Client{Path: filepath.Join(p.dir, "admin.sock"), Timeout: time.Second}).Call(adminsock.Request{Op: adminsock.OpRuntime})
	if err != nil || resp.Runtime == nil || resp.Runtime.BrokerImage == nil {
		t.Fatalf("%s runtime: %v %+v", p.id, err, resp)
	}
	return *resp.Runtime.BrokerImage
}
func (f *signedRollFixture) assertImages(t *testing.T) {
	t.Helper()
	for _, p := range f.processes {
		image := f.status(t, p)
		actual := signedRollSHA(t, fmt.Sprintf("/proc/%d/exe", p.cmd.Process.Pid))
		owner, err := upgradestage.ProcessOwner(p.cmd.Process.Pid)
		if err != nil || owner != p.original || actual != f.sha || image.RunningSHA256 != f.sha || image.Generation != 2 || image.Phase != "serving" {
			t.Fatalf("%s did not retain its PID and reach the target image: actual=%s image=%+v owner=%+v err=%v", p.id, actual, image, owner, err)
		}
		t.Logf("broker=%s pid=%d start_ticks=%d running_sha=%s generation=%d cut=%s", p.id, p.cmd.Process.Pid, owner.StartTicks, actual, image.Generation, image.CutID)
	}
}

// The former scripted fixture recorded these protocol obligations. The live
// product now supplies the exchange and evidence rather than a test responder.
// agentlessRollFixture is a three-broker cluster in which NO host runs a co-located agent. It answers
// cluster-health for all three and the signed upgrade-trigger subject, tracking exactly what the roll did.
// origin: hot-update S6 plan §5 / T45; fixtures report running bytes, not staged versions.
// per-broker self-reported release; a `reload` flips it to the target, like a real re-exec.
// The fixture answers the same holder/cut/home/generation exchange as the
// production driver. Generic OK cannot authorize a reload; retain the hold
// through convergence and require the matching ended record before release.
// origin: hot-update S6 plan §5 / §7.1 / T45 / GATES-S6-E
// origin: hot-update S6 plan §5 / §7.1 / T45 / GATES-S6-E
// ColocatedAgentNID deliberately EMPTY on every host — this is the agentless shape.
// startAgentlessRollFixture wires the fixture onto NATS and returns it.
// Each broker answers separately, exactly as in production.
// The session node list: NO agent has ever registered here. This is the observation that makes the
// hosts agentless rather than "agent temporarily missing".
// Verbatim production behaviour: internal/broker forwards to the broker's own node_id when
// colocated_agent_nid is unset, and on an agentless host nobody is listening.

// MUST stay empty: a broker-only host has no agent leg
// leader-last: the roll must transfer off it
