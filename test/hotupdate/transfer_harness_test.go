//go:build linux && hotupdate_integration

package hotupdate_test

// Real: workspace-built serve/agent/ctl, embedded file-store JetStream and the
// shared process supervisor. Simulated: loopback infrastructure and payloads.
// Not covered: deploy/WSS, S8 reacquisition or S9 infrastructure upgrades.

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/natsinbox"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// This is only the P29 interface adapter. StartProcess owns the subreaper,
// caller-death lease and descendant census; no private Pdeathsig substitute.
// origin: hot-update S7 plan lead L16 / §11.3
type transferSupervisor struct{}

func (transferSupervisor) Launch(cmd *exec.Cmd) (transferharness.Child, error) {
	wait, err := testharness.StartProcess(cmd)
	if err != nil {
		return transferharness.Child{}, err
	}
	exits := make(chan transferharness.Exit, 1)
	joined := make(chan struct{})
	go func() {
		err := wait()
		rc := -1
		if cmd.ProcessState != nil {
			rc = cmd.ProcessState.ExitCode()
		}
		exits <- transferharness.Exit{NativeRC: rc, Err: err}
		close(exits)
		close(joined)
	}()
	var stop sync.Once
	return transferharness.Child{PID: cmd.Process.Pid, Done: exits, Stop: func() error {
		stop.Do(func() {
			select {
			case <-joined:
				return
			default:
			}
			_ = cmd.Process.Signal(syscall.SIGTERM)
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				_ = cmd.Process.Kill()
			}
		})
		select {
		case <-joined:
			return nil
		case <-time.After(10 * time.Second):
			return errors.New("transfer supervisor did not join the process tree")
		}
	}}, nil
}

type transferArtifacts struct {
	a, b, shaA, shaB, source, dirty string
	archiveA, archiveB              []byte
}

// Compile before opening a transfer. GOFLAGS is deliberately inherited by the
// supervised compiler so an outer production mutation reaches both artifacts.
// origin: hot-update S7 plan §10.2 / T35 / testing-standards §0.4
func buildTransferArtifacts(t *testing.T) transferArtifacts {
	t.Helper()
	root, err := filepath.Abs("../..")
	transferMust(t, err)
	dir := testharness.GuardProcessDirectory(t, t.TempDir())
	a := transferArtifacts{a: filepath.Join(dir, "original"), b: filepath.Join(dir, "candidate")}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "build", "-race", "-trimpath", "-o", a.a, "./cmd/tether")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOFLAGS="+transferFaultOverlay(t, root, dir))
	buildTransferCandidate(t, cmd, a.a)
	// A distinct immutable ELF artifact is permitted by testing-standards §0.4.
	// Both images have the identical capable ABI and include the same overlay.
	body, err := os.ReadFile(a.a)
	transferMust(t, err)
	transferMust(t, os.WriteFile(a.b, append(body, []byte("\nS7 transfer candidate\n")...), 0700))
	a.shaA, err = transferharness.FileSHA(a.a)
	transferMust(t, err)
	a.shaB, err = transferharness.FileSHA(a.b)
	transferMust(t, err)
	if a.shaA == a.shaB {
		t.Fatal("transfer artifacts have identical SHA")
	}
	cmd = exec.CommandContext(ctx, "git", "rev-parse", "HEAD")
	cmd.Dir = root
	out, err := testharness.Output(cmd)
	transferMust(t, err)
	a.source, a.dirty = strings.TrimSpace(string(out)), hotUpdateSourceDigest(t, root)
	a.archiveA, a.archiveB = transferArchive(t, a.a), transferArchive(t, a.b)
	t.Logf("source_sha=%s dirty_digest=%s artifact_A=%s artifact_B=%s", a.source, a.dirty, a.shaA, a.shaB)
	return a
}

type transferStack struct {
	t                                                            *testing.T
	h                                                            *transferharness.Harness
	a                                                            transferArtifacts
	ns                                                           *server.Server
	options                                                      server.Options
	nc                                                           *nats.Conn
	js                                                           jetstream.JetStream
	broker, agent                                                *transferharness.Process
	ctl                                                          *transferharness.Process
	home, agentHome, brokerPath, agentPath, admin, url, sid, nid string
	artifact                                                     *httptest.Server
	collectors                                                   map[string]*transferCollector
	wireMu                                                       sync.Mutex
	wire                                                         []*nats.Msg
	closed                                                       bool
	carrier                                                      *transferCarrier
	brokerSeed                                                   []byte
	cluster                                                      *transferharness.Cluster
	originIndex, ingressIndex                                    int
	// An authorized transaction cut enters at its Start: describe, prepare
	// and the root stage before it freeze no owner. A cell whose cut is one
	// sets transactionCuts, and runTransferCell then defers a cut-entry
	// release to cutEntry, which the cut runs right before Start.
	// origin: S8 FIX-DESCRIBE (P19 request R7)
	transactionCuts bool
	cutEntry        func()
	// gate owns the client listener of a restart-lane fixture; see
	// transferNATSGate. Other fixtures connect to the server directly.
	gate *transferNATSGate
}

// Bound product processes independently of go test's host-wide -parallel
// default. Every slot is held through descendant join and server shutdown.
// origin: hot-update S7 plan lead L17 / §11.3
var transferFixtureSlots = make(chan struct{}, 24)

func newTransferStack(t *testing.T, a transferArtifacts) *transferStack {
	return newTransferStackInTopology(t, a, "single")
}

// A same-store restart needs every client behind the harness gate before the
// first CONNECT. The S10 relay fixture instead takes over the server's own
// address, so the gate is opt-in rather than the single lane's default.
// origin: S7 internal review CUT-CAPACITY F5 / hot-update S7 plan §10.3 / T46
func newTransferRestartStack(t *testing.T, a transferArtifacts) *transferStack {
	return newTransferStackInTopology(t, a, transferGatedSingle)
}

const transferGatedSingle = "single-gated"

// The single lane retains its standalone store. Cluster lanes share only the
// authenticated routed NATS fabric, never the SQLite/Bolt or binary paths.
// origin: S7 internal review INTEG-O F2 / hot-update S7 plan §6.3
func newTransferStackInTopology(t *testing.T, a transferArtifacts, topology string) *transferStack {
	t.Helper()
	select {
	case transferFixtureSlots <- struct{}{}:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	s := &transferStack{t: t, h: transferharness.New(t, transferSupervisor{}), a: a,
		sid: "transfer-lab", nid: "transfer-agent", collectors: make(map[string]*transferCollector)}
	t.Cleanup(s.close)
	s.home, s.agentHome = filepath.Join(s.h.Root, "ctl-home"), filepath.Join(s.h.Root, "agent-home")
	for _, home := range []string{s.home, s.agentHome} {
		transferMust(t, os.MkdirAll(home, 0700))
	}
	transferMust(t, os.Mkdir(filepath.Join(s.h.Root, "agent-bin"), 0700))
	s.brokerPath, s.agentPath = filepath.Join(s.h.Root, "broker"), filepath.Join(s.h.Root, "agent-bin", "agent")
	for _, path := range []string{s.brokerPath, s.agentPath, filepath.Join(s.h.Root, "ctl")} {
		transferCopy(t, a.a, path)
	}
	id, err := cli.EnsureIdentity(s.home)
	transferMust(t, err)
	transferMust(t, os.WriteFile(filepath.Join(s.home, "current_session"), []byte(s.sid+"\n"), 0600))
	dbPath := filepath.Join(s.h.Root, "broker.db")
	db, err := storage.Open(dbPath)
	transferMust(t, err)
	// A real PHC hash, as `session create` writes: the session incarnation an
	// S8 cut carries is derived from its salt, and a placeholder string left
	// every registered agent with no incarnation (authority_incarnation_unknown).
	// origin: S8 FIX-DESCRIBE (P19 request R7)
	pinHash, err := auth.HashPIN("transfer-fixture")
	transferMust(t, err)
	_, createErr := session.Create(db, s.sid, s.sid, id.Fingerprint, pinHash, time.Now().UTC())
	transferMust(t, createErr)
	authDir, authPolicy := transferMaintenanceSeeds(t, s, db)
	transferMust(t, errors.Join(createErr, db.Close()))
	s.options = server.Options{Host: "127.0.0.1", Port: -1, ServerName: "transfer-private",
		JetStream: true, StoreDir: filepath.Join(s.h.Root, "jetstream"), NoSigs: true,
		NoLog: true, AlwaysEnableNonce: true, MaxPayload: 12 << 20, AuthCallout: authPolicy,
		Nkeys: []*server.NkeyUser{{Nkey: authPolicy.AuthUsers[0]}}}
	replicas := 1
	if topology == "single" || topology == transferGatedSingle {
		s.startJS()
	} else {
		s.cluster = transferharness.NewCluster(t, s.h.Root, topology, dbPath, authDir)
		s.ns = s.cluster.Servers[0]
		replicas = s.cluster.Replicas
	}
	s.url = s.ns.ClientURL()
	agentURL := s.url
	if topology == transferGatedSingle {
		s.gate = newTransferNATSGate(t, s.ns.Addr().String())
		s.url, agentURL = s.gate.url(), s.gate.url()
	}
	s.nc, err = nats.Connect(s.url, nats.Name("transfer-independent-oracle"), nats.MaxReconnects(-1),
		nats.Nkey(authPolicy.AuthUsers[0], func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(s.brokerSeed, nonce) }))
	transferMust(t, err)
	s.js, err = jetstream.New(s.nc)
	transferMust(t, err)
	transferMust(t, jsstream.EnsureHistoryStream(t.Context(), s.js, s.sid, replicas))
	_, err = s.nc.Subscribe(proto.SubjectPrefix+".>", func(m *nats.Msg) {
		// Keep request identity bytes, never credentials or settlement capsules.
		if !strings.HasSuffix(m.Subject, ".req") {
			return
		}
		var v struct {
			CallID     string `json:"call_id"`
			TransferID string `json:"transfer_id"`
			Identity   struct {
				TransferID string `json:"transfer_id"`
			} `json:"identity"`
			// Status readers retain public authority, never the settlement capsule.
			// origin: S7 internal review INTEG-M F5
			Continuation *struct {
				Identity proto.Identity `json:"identity"`
				Origin   proto.Origin   `json:"origin"`
			} `json:"continuation,omitempty"`
		}
		if json.Unmarshal(m.Data, &v) != nil || v.CallID == "" {
			return
		}
		if v.TransferID == "" {
			v.TransferID = v.Identity.TransferID
		}
		if v.TransferID == "" {
			return
		}
		data, marshalErr := json.Marshal(v)
		if marshalErr != nil {
			return
		}
		s.wireMu.Lock()
		s.wire = append(s.wire, &nats.Msg{Subject: m.Subject, Data: data})
		s.wireMu.Unlock()
	})
	transferMust(t, err)
	transferMust(t, s.nc.FlushTimeout(time.Second))
	s.artifact = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		archive := a.archiveB
		if r.URL.Path == "/original.tar.gz" {
			archive = a.archiveA
		}
		_, _ = w.Write(archive)
	}))
	s.admin = filepath.Join(s.h.Root, "admin.sock")
	if s.cluster != nil {
		s.startClusterBrokers()
	} else {
		// The broker reserves its own kernel-selected listener. No free-port gap
		// lets the next parallel fixture or this fixture's carrier claim it first.
		// origin: S7 internal review INTEG-M F6
		tunnel := "127.0.0.1:0"
		config := registerTransferBroker(t, s, s.brokerPath, s.h.Root, dbPath, false)
		s.broker = s.launch("broker", s.brokerPath, s.h.Root, []string{"serve", "--config", config, "--nats-url", s.url,
			"--auth-callout-seeds-dir", authDir,
			"--db", dbPath, "--admin-socket", s.admin, "--tunnel-addr", tunnel,
			"--tunnel-public-host", "127.0.0.1", "--upgrade-url-allow", s.artifact.URL + "/", "--log-file", "-"}, true)
	}
	if !testharness.WaitFor(t, 15*time.Second, 20*time.Millisecond, func() bool {
		r, err := (&adminsock.Client{Path: s.admin, Timeout: 100 * time.Millisecond}).Call(adminsock.Request{Op: adminsock.OpRuntime})
		return err == nil && r.OK
	}) {
		t.Fatal("private real broker failed readiness: " + s.output(s.broker))
	}
	s.carrier = newTransferCarrier(t, transferTunnelAddress(t, s.output(s.broker)))
	if s.gate == nil {
		agentURL = s.ns.ClientURL()
	}
	// The authorized S8 cut route needs the agent's registered domain before
	// its first start (installer retrofit).
	// origin: S8 FIX-DESCRIBE (P19 request R7)
	registerTransferAgent(t, s)
	s.agent = s.launch("agent", s.agentPath, s.agentHome, []string{"agent", "--session", s.sid,
		"--nid", s.nid, "--nats-url", agentURL, "--tunnel-addr", s.carrier.listener.Addr().String(),
		"--upgrade-url-allow", s.artifact.URL + "/", "--log-file", "-"}, true)
	if !testharness.WaitFor(t, 15*time.Second, 20*time.Millisecond, func() bool {
		info, err := s.ns.Connz(&server.ConnzOptions{Subscriptions: true})
		if err != nil {
			return false
		}
		for _, conn := range info.Conns {
			for _, sub := range conn.Subs {
				if sub == proto.SubjCmdForwarded(s.sid, s.nid, "*") {
					return true
				}
			}
		}
		return false
	}) {
		t.Fatal("private real agent failed readiness: " + s.output(s.agent))
	}
	if s.cluster != nil {
		_, err := s.cluster.Census()
		transferMust(t, err)
	}
	return s
}

// Launch every voter before selecting the original follower. The ctl connects
// to the third server, so no single-server shortcut can pass these cells.
// origin: S7 internal review INTEG-O F2 / hot-update S7 plan §6.3 / T39
func (s *transferStack) startClusterBrokers() {
	s.t.Helper()
	for i, p := range s.cluster.Peers {
		transferCopy(s.t, s.a.a, p.Binary)
		transferMust(s.t, p.ReleasePorts())
		config := registerTransferBroker(s.t, s, p.Binary, p.Root, p.DB, true)
		p.Process = s.launch(fmt.Sprintf("broker-%d", i), p.Binary, p.Root, []string{"serve", "--config", config, "--nats-url", p.URL,
			"--db", p.DB, "--admin-socket", p.Admin, "--tunnel-addr", p.Tunnel, "--tunnel-public-host", "127.0.0.1",
			"--auth-callout-seeds-dir", p.Secrets, "--cluster-data-dir", p.Root, "--cluster-secrets-dir", p.Secrets,
			"--cluster-raft-addr", p.Raft, "--cluster-manifest-listen=", "--nats-conf-path=",
			"--upgrade-url-allow", s.artifact.URL + "/", "--log-file", "-"}, true)
	}
	leader := -1
	if !testharness.WaitFor(s.t, 15*time.Second, 20*time.Millisecond, func() bool {
		var err error
		leader, err = s.cluster.Leader()
		return err == nil
	}) {
		for _, p := range s.cluster.Peers {
			s.t.Log(s.output(p.Process))
		}
		s.t.Fatal("real transfer cluster failed membership readiness")
	}
	s.originIndex, s.ingressIndex = 0, 0
	if s.cluster.Replicas == 3 {
		s.originIndex, s.ingressIndex = (leader+1)%3, (leader+2)%3
	}
	p := s.cluster.Peers[s.originIndex]
	s.broker, s.brokerPath, s.admin = p.Process, p.Binary, p.Admin
	s.ns = s.cluster.Servers[s.originIndex]
	s.url = s.cluster.Peers[s.ingressIndex].URL
	role := fmt.Sprintf("broker-%d", s.originIndex)
	s.collectors["broker"] = s.collectors[role]
	delete(s.collectors, role)
}

// A local cut now requires the original agent's installed signed hold. Give
// the real registration its provisioned NKey and pin the private account key;
// an anonymous fixture cannot invent the missing maintenance authority.
// origin: S7 INTEG-O / S6 carried rv-final F4 / hot-update S7 plan §10.4
func transferMaintenanceSeeds(t *testing.T, s *transferStack, db *sql.DB) (string, *server.AuthCallout) {
	t.Helper()
	identity, err := cli.EnsureAgentIdentity(s.agentHome, s.sid)
	transferMust(t, err)
	transferMust(t, agentprov.Provision(db, s.sid, s.nid, identity.Fingerprint, time.Now().UTC()))
	account, err := nkeys.CreateAccount()
	transferMust(t, err)
	defer account.Wipe()
	seed, err := account.Seed()
	transferMust(t, err)
	public, err := account.PublicKey()
	transferMust(t, err)
	s.brokerSeed, err = auth.GenerateUserSeed()
	transferMust(t, err)
	brokerPub, err := auth.PublicKeyFromSeed(s.brokerSeed)
	transferMust(t, err)
	dir := filepath.Join(s.h.Root, "authority")
	transferMust(t, os.Mkdir(dir, 0700))
	transferMust(t, os.WriteFile(filepath.Join(dir, "broker.nk"), s.brokerSeed, 0600))
	transferMust(t, os.WriteFile(filepath.Join(dir, "account.nk"), seed, 0600))
	agentDir := filepath.Join(s.agentHome, "agent", s.sid)
	transferMust(t, os.MkdirAll(agentDir, 0700))
	// This fixture owns transfer/tunnel traffic, with no public egress proxy.
	// Declare that scope so maintenance need not wait for an unconfigured pool.
	transferMust(t, os.WriteFile(filepath.Join(agentDir, "agent.yaml"), []byte("account_pub: "+public+"\nproxy:\n  participate: false\n"), 0600))
	return dir, &server.AuthCallout{Issuer: public, Account: "$G", AuthUsers: []string{brokerPub}}
}

func (s *transferStack) startJS() {
	var err error
	s.ns, err = server.NewServer(&s.options)
	transferMust(s.t, err)
	s.ns.Start()
	if !s.ns.ReadyForConnections(5 * time.Second) {
		s.t.Fatal("private JetStream did not start")
	}
	_, port, err := net.SplitHostPort(s.ns.Addr().String())
	transferMust(s.t, err)
	_, err = fmt.Sscan(port, &s.options.Port)
	transferMust(s.t, err)
}

func (s *transferStack) launch(role, path, home string, args []string, observe bool) *transferharness.Process {
	s.t.Helper()
	digest, err := transferharness.FileSHA(path)
	transferMust(s.t, err)
	var env []string
	for _, entry := range os.Environ() {
		key, _, _ := strings.Cut(entry, "=")
		if key == "HOME" || key == "GORACE" || strings.HasPrefix(key, "TETHER_") || strings.HasPrefix(key, "_TETHER_") {
			continue
		}
		env = append(env, entry)
	}
	env = append(env, "HOME="+home, "TETHER_HOME="+home, "GORACE=atexit_sleep_ms=0", "S7_TRANSFER_FAULT_ROOT="+s.h.Root)
	spec := transferharness.Spec{Binary: path, ArtifactSHA: digest, Args: args, Env: env}
	if observe {
		c := newTransferCollector(s.t)
		s.collectors[role] = c
		spec.Env = append(spec.Env, c.environment()...)
		spec.ExtraFiles = c.files()
	}
	p, err := s.h.Start(s.t.Context(), spec)
	transferMust(s.t, err)
	if observe {
		s.collectors[role].process = p
		s.collectors[role].started()
		// The private init handshake precedes main and its admission clock.
		// Release only after the independent identity sample and custody reader.
		// origin: S7 internal review FIX-TIERA2 F3 / hot-update S7 plan §10.4 / T46
		if role == "ctl" {
			transferWriteAtomic(s.t, filepath.Join(s.h.Root, "ctl-identity-release"), "ready\n")
		}
	}
	return p
}

func (s *transferStack) output(p *transferharness.Process) string {
	r := p.Receipt()
	b, _ := os.ReadFile(r.StderrPath)
	return string(b)
}

func (s *transferStack) command(args ...string) (transferharness.Receipt, error) {
	args = append(args, "--home", s.home, "--nats-url", s.url)
	p := s.launch("command", filepath.Join(s.h.Root, "ctl"), s.home, args, false)
	ctx, cancel := context.WithTimeout(s.t.Context(), 20*time.Second)
	defer cancel()
	r, err := p.Wait(ctx)
	if err != nil {
		return r, fmt.Errorf("native rc=%d: %w: %s", r.NativeRC, err, s.output(p))
	}
	return r, nil
}

// Fail at the actual missing product route, before labelling any cell covered.
// Absence/timeout never authorizes a legacy retry or a synthetic fixture owner.
// origin: hot-update S7 plan §6.1 / §7 / T35
func (s *transferStack) requireCapable() {
	id, err := cli.EnsureIdentity(s.home)
	transferMust(s.t, err)
	option := nats.Nkey(id.PublicKey, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(id.Seed, nonce) })
	// The real callout grants private replies only with both prefix and marker.
	// origin: S7 INTEG-O authenticated fixture / hot-update S7 plan §6.3
	opts := append(natsinbox.InboxConnectOptions(id.PublicKey), option, nats.Name(cli.CtlNameForSession(s.sid)))
	nc, err := nats.Connect(s.url, opts...)
	transferMust(s.t, err)
	defer nc.Close()
	subject, data, err := proto.EncodeTransferRequest(id.PublicKey, x.Request{Op: x.OpProbe,
		CallID: "binary-capability", Identity: x.Identity{SID: s.sid, NID: s.nid, TransferID: "binary-capability"}})
	transferMust(s.t, err)
	msg, err := nc.Request(subject, data, 5*time.Second)
	if err != nil {
		s.t.Fatalf("NOT_RUN pending s7-p9m: real transfer probe %s: %v", subject, err)
	}
	response, err := proto.DecodeTransferResponse(x.OpProbe, "push", msg.Data)
	if err != nil || response.Probe == nil || response.Probe.Version != proto.TransferVersion {
		s.t.Fatalf("NOT_RUN pending s7-p9m: real transfer capability unavailable: decode=%v response=%s", err, msg.Data)
	}
}

func (s *transferStack) close() {
	if s.closed {
		return
	}
	s.closed = true
	if err := s.h.Close(); err != nil {
		s.t.Error(err)
	}
	saveTransferObservations(s)
	for _, c := range s.collectors {
		c.close()
	}
	if s.nc != nil {
		s.nc.Close()
	}
	// Every product process has joined; closing the gate first leaves the
	// server with no client socket, hence no CONNECT, during its Shutdown.
	if s.gate != nil {
		s.gate.close()
	}
	if s.cluster != nil {
		s.cluster.Close()
	} else if s.ns != nil {
		s.ns.Shutdown()
		s.ns.WaitForShutdown()
	}
	if s.carrier != nil {
		s.carrier.close()
	}
	if s.artifact != nil {
		s.artifact.Close()
	}
	<-transferFixtureSlots
}

func transferMust(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func transferCopy(t *testing.T, from, to string) {
	t.Helper()
	data, err := os.ReadFile(from)
	transferMust(t, err)
	transferMust(t, os.WriteFile(to, data, 0700))
}
func transferAddress(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	transferMust(t, err)
	a := l.Addr().String()
	transferMust(t, l.Close())
	return a
}

// origin: S7 internal review INTEG-M F6
func transferTunnelAddress(t *testing.T, output string) string {
	t.Helper()
	for _, line := range strings.Split(output, "\n") {
		if !strings.Contains(line, `msg="tunnel: server listening"`) {
			continue
		}
		for _, field := range strings.Fields(line) {
			address, ok := strings.CutPrefix(field, "addr=")
			if !ok {
				continue
			}
			address = strings.Trim(address, `"`)
			host, port, err := net.SplitHostPort(address)
			if ip := net.ParseIP(host); err == nil && ip != nil && ip.IsLoopback() && port != "0" && port != "" {
				return address
			}
		}
	}
	t.Fatal("broker supplied no actual bound tunnel address")
	return ""
}
func transferArchive(t *testing.T, path string) []byte {
	t.Helper()
	body, err := os.ReadFile(path)
	transferMust(t, err)
	var b bytes.Buffer
	// Compression ratio is irrelevant to the product archive contract. The
	// default compressor under race otherwise dominates the per-test budget.
	gz, err := gzip.NewWriterLevel(&b, gzip.BestSpeed)
	transferMust(t, err)
	tw := tar.NewWriter(gz)
	transferMust(t, tw.WriteHeader(&tar.Header{Name: "tether", Mode: 0755, Size: int64(len(body))}))
	_, err = tw.Write(body)
	transferMust(t, err)
	transferMust(t, tw.Close())
	transferMust(t, gz.Close())
	return b.Bytes()
}
func transferDigest(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }
