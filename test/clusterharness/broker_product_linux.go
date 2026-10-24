//go:build linux

package clusterharness

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/clusteroffline"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nkeys"
)

// The product lane shares P26's custody and receipt interface but launches the
// actual serve entry. Component store checkpoints remain explicitly separate.
// origin: hot-update S6 plan §3.2 / §12.1 P26 / T01 / T37
func StartBrokerRunFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return startBrokerProductFixture(t, cfg, false, false, false)
}

// Store consumers schedule their own first write after control readiness. Flow
// consumers retain the separate full byte-cohort validation and stall oracle.
// origin: hot-update S6 plan §12.1 P26 / T37
func StartBrokerStoreRunFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return startMaintainedBrokerStoreFixture(t, cfg)
}

// StartBrokerLifecycleFixture leaves the real serve process at its pre-owner
// barrier, so the consumer can compare an actual idle FD multiset to normal stop.
// origin: hot-update S6 plan §4.2 / T02 / T49 / T52
func StartBrokerLifecycleFixture(t *testing.T, cfg BrokerImageFixtureConfig) (*BrokerImageFixture, error) {
	return startBrokerProductFixture(t, cfg, false, true, false)
}

func startBrokerProductFixture(t *testing.T, cfg BrokerImageFixtureConfig, storeOnly, lifecycle, cohort bool) (*BrokerImageFixture, error) {
	t.Helper()
	if runtime.GOOS != "linux" {
		return nil, handoff.ErrUnsupported
	}
	if cfg.Topology != "single" && cfg.Topology != "sole-voter" && cfg.Topology != "quorum" {
		return nil, errors.New("broker product fixture: single, sole-voter or quorum required")
	}
	root := testharness.GuardProcessDirectory(t, t.TempDir())
	f := &brokerProductFixture{storeOnly: storeOnly, lifecycle: lifecycle, started: time.Now(), root: root, topology: cfg.Topology}
	fixture := &BrokerImageFixture{api: f}
	f.test = t
	f.cohort = newMaintainedProductCohort(f, cohort)
	f.logs.path = filepath.Join(root, "product.stderr")
	t.Cleanup(func() {
		if err := fixture.Close(); err != nil {
			t.Error(err)
		}
	})
	_, source, _, _ := runtime.Caller(0)
	repo := filepath.Dir(filepath.Dir(filepath.Dir(source)))
	sourceCommand := exec.Command("git", "rev-parse", "HEAD")
	sourceCommand.Dir = repo
	sourceOutput, err := testharness.Output(sourceCommand)
	if err != nil {
		return fixture, err
	}
	f.sourceSHA = strings.TrimSpace(string(sourceOutput))
	f.sourceDigest, err = storeFixtureSourceDigest(repo)
	if err != nil {
		return fixture, err
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	binary := filepath.Join(root, "broker-product")

	secrets := filepath.Join(root, "credentials")
	if err := os.Mkdir(secrets, 0700); err != nil {
		return fixture, err
	}
	ca := NewRouteCA(t, "s6-product")
	cert := ca.Leaf(t, "product")
	template, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fixture, err
	}
	template.IPAddresses = []net.IP{net.IPv4(127, 0, 0, 1)}
	publicKey, err := brokerFixturePublicKey(cert.PrivateKey)
	if err != nil {
		return fixture, err
	}
	cert.Certificate[0], err = x509.CreateCertificate(rand.Reader, template, ca.Cert, publicKey, ca.Key)
	if err != nil {
		return fixture, err
	}
	key, err := x509.MarshalPKCS8PrivateKey(cert.PrivateKey)
	if err != nil {
		return fixture, err
	}
	for name, block := range map[string]*pem.Block{"cluster-ca.pem": {Type: "CERTIFICATE", Bytes: ca.Cert.Raw}, "route-cert.pem": {Type: "CERTIFICATE", Bytes: cert.Certificate[0]}, "route-key.pem": {Type: "PRIVATE KEY", Bytes: key}, "tunnel-cert.pem": {Type: "CERTIFICATE", Bytes: cert.Certificate[0]}, "tunnel-key.pem": {Type: "PRIVATE KEY", Bytes: key}} {
		if err := os.WriteFile(filepath.Join(secrets, name), pem.EncodeToMemory(block), 0600); err != nil {
			return fixture, err
		}
	}
	brokerSeed, err := auth.GenerateUserSeed()
	if err != nil {
		return fixture, err
	}
	nodeSeed, err := auth.GenerateUserSeed()
	if err != nil {
		return fixture, err
	}
	account, err := nkeys.CreateAccount()
	if err != nil {
		return fixture, err
	}
	defer account.Wipe()
	accountSeed, err := account.Seed()
	if err != nil {
		return fixture, err
	}
	for name, seed := range map[string][]byte{"broker.nk": brokerSeed, "account.nk": accountSeed, "node-ident.nk": nodeSeed} {
		if err := os.WriteFile(filepath.Join(secrets, name), seed, 0600); err != nil {
			return fixture, err
		}
	}
	brokerPub, err := auth.PublicKeyFromSeed(brokerSeed)
	if err != nil {
		return fixture, err
	}
	accountPub, err := account.PublicKey()
	if err != nil {
		return fixture, err
	}
	natsOptions := &server.Options{Host: "127.0.0.1", Port: -1, ServerName: "product", JetStream: true, StoreDir: filepath.Join(root, "nats"), NoLog: true, NoSigs: true, Nkeys: []*server.NkeyUser{{Nkey: brokerPub}}, AuthCallout: &server.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}}
	if cfg.Topology == "quorum" {
		for i := 0; i < 3; i++ {
			f.natsRoutes = append(f.natsRoutes, "nats://"+productFixtureAddress(f))
		}
		_, port, _ := net.SplitHostPort(strings.TrimPrefix(f.natsRoutes[0], "nats://"))
		routePort, _ := strconv.Atoi(port)
		natsOptions.Cluster = server.ClusterOpts{Name: "s6-cohort", Host: "127.0.0.1", Port: routePort}
		natsOptions.Routes = server.RoutesFromStr(strings.Join(f.natsRoutes, ","))
	}
	ns, err := server.NewServer(natsOptions)
	if err != nil {
		return fixture, err
	}
	f.nats = ns
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		return fixture, errors.New("product fixture: NATS not ready")
	}
	f.tunnel = productFixtureAddress(f)
	f.target, err = net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return fixture, err
	}
	f.targetDone = make(chan struct{})
	go func() {
		defer close(f.targetDone)
		for {
			conn, err := f.target.Accept()
			if err != nil {
				return
			}
			f.targetMu.Lock()
			f.targetConns = append(f.targetConns, conn)
			f.targetMu.Unlock()
			f.targetWorkers.Add(1)
			go func() {
				defer f.targetWorkers.Done()
				defer closeBrokerFixtureWitness(t, conn)
				// TCP's splice path caches pipe pairs in a process-wide pool. The
				// external echo witness owns only its accepted socket and worker;
				// use the ordinary copy path so Close can join its entire FD graph.
				_, _ = io.Copy(struct{ io.Writer }{conn}, struct{ io.Reader }{conn})
			}()
		}
	}()
	db, err := storage.Open(filepath.Join(root, "tether.db"))
	if err != nil {
		return fixture, err
	}
	pinHash, err := productPINHash()
	if err != nil {
		_ = db.Close()
		return fixture, err
	}
	if _, err = session.Create(db, "product", "product", "owner", pinHash, time.Now()); err != nil {
		_ = db.Close()
		return fixture, err
	}
	if err = node.Register(db, node.RegisterInput{SID: "product", NID: "agent", ProtoVersion: proto.ProtoVersion}, time.Now()); err != nil {
		_ = db.Close()
		return fixture, err
	}
	passwordBytes := make([]byte, 32)
	if _, err = rand.Read(passwordBytes); err != nil {
		_ = db.Close()
		return fixture, err
	}
	password := base64.RawStdEncoding.EncodeToString(passwordBytes)
	f.password = password
	f.ss = ssproxy.New(testharness.SilentLog())
	ssPort, err := f.ss.Start(0, []ssproxy.Key{{KeyID: "product", Secret: password}})
	if err != nil {
		_ = db.Close()
		return fixture, err
	}
	localTargetPort, err := brokerFixtureTCPPort(f.target.Addr())
	if err != nil {
		return fixture, errors.Join(err, db.Close())
	}
	for _, name := range []string{"live", "ss", "victim"} {
		address := productReservePublicAddress(f)
		_, value, _ := net.SplitHostPort(address)
		public, _ := strconv.Atoi(value)
		localPort := localTargetPort
		if name == "ss" {
			localPort = ssPort
		}
		alloc, err := port.Allocate(db, "product", "agent", name, localPort, public, "owner", false, &port.Config{BandLow: public, BandHigh: public})
		if err != nil {
			_ = db.Close()
			return fixture, err
		}
		f.allocations = append(f.allocations, *alloc)
	}
	if f.cohort != nil {
		if err := setupMaintainedProductCohort(f.cohort, db, accountPub); err != nil {
			_ = db.Close()
			return fixture, err
		}
	}
	if err = db.Close(); err != nil {
		return fixture, err
	}
	args := []string{"serve", "--nats-url", ns.ClientURL(), "--db", filepath.Join(root, "tether.db"), "--admin-socket", filepath.Join(root, "admin.sock"), "--tunnel-addr", f.tunnel, "--tunnel-public-host", "127.0.0.1", "--log-file", "-", "--log-json", "--auth-callout-seeds-dir", secrets}
	if cfg.Topology == "sole-voter" || cfg.Topology == "quorum" {
		raftAddr := productFixtureAddress(f)
		ident, err := auth.PublicKeyFromSeed(nodeSeed)
		if err != nil {
			return fixture, err
		}
		if err := clusteroffline.InitFromExisting(clusteroffline.InitFromExistingOptions{DataDir: root, DBPath: filepath.Join(root, "tether.db"), SecretsDir: secrets, SelfID: "product", Name: "product", NodeIdentPub: ident, RaftAddr: raftAddr, NatsRoute: "nats://127.0.0.1:6222", TunnelAddr: f.tunnel, PublicHost: "127.0.0.1", Logger: testharness.SilentLog()}); err != nil {
			return fixture, err
		}
		args = append(args, "--cluster-data-dir", root, "--cluster-secrets-dir", secrets, "--cluster-raft-addr", raftAddr, "--cluster-manifest-listen=", "--nats-conf-path=")
	}

	if cfg.Topology == "quorum" {
		if err := f.setupQuorum(secrets, brokerSeed, accountSeed, args); err != nil {
			return fixture, err
		}
	}
	// The authorized S8 route needs the broker's registration and config
	// before its first start (installer retrofit).
	if f.transaction, err = newProductTransaction(root, binary, filepath.Join(root, "tether.db")); err != nil {
		return fixture, err
	}
	args = append(args, "--config", f.transaction.config)

	victimData, _ := json.Marshal(map[string]int{"port": f.allocations[2].Port})
	if err := os.WriteFile(filepath.Join(root, "victim-port.json"), victimData, 0600); err != nil {
		return fixture, err
	}
	if err := productCandidateBuild(t, ctx, repo, binary, f.sourceDigest, productObservationOverlay(t, repo, root, f.allocations[2].Port)); err != nil {
		return fixture, err
	}
	if err := storeFixtureCopy(binary, binary+".prev", nil); err != nil {
		return fixture, err
	}
	if err := storeFixtureCopy(binary, filepath.Join(root, "candidate"), []byte("\nS6 product candidate\n")); err != nil {
		return fixture, err
	}
	f.previousSHA, err = storeFixtureHash(binary + ".prev")
	if err != nil {
		return fixture, err
	}
	f.candidateSHA, err = storeFixtureHash(filepath.Join(root, "candidate"))
	if err != nil {
		return fixture, err
	}

	if f.quorum != nil {
		if err := f.quorum.start(f, binary); err != nil {
			return fixture, err
		}
	}
	if f.transaction != nil {
		if err := f.transaction.register(filepath.Join(root, "tether.db")); err != nil {
			return fixture, err
		}
	}
	if lifecycle {
		if err := os.WriteFile(filepath.Join(root, "lifecycle-mode.json"), []byte("{}"), 0600); err != nil {
			return fixture, err
		}
	}
	f.certificate, f.password = cert, password
	f.cmd = exec.Command(binary, args...)
	f.cmd.Dir = root
	f.cmd.Env = append(handoff.CleanEnvironment(os.Environ()), "HOME="+root, "TETHER_HOME="+root, "GORACE=atexit_sleep_ms=0", "TETHER_AUTO_REBALANCE=on")
	f.cmd.Stdout, f.cmd.Stderr = &f.logs, &f.logs
	waitProcess, err := testharness.StartProcess(f.cmd)
	if err != nil {
		return fixture, err
	}
	f.done = make(chan struct{})
	go func() { f.waitErr = waitProcess(); close(f.done) }()
	if lifecycle {
		if err := f.awaitMarker("runtime-idle"); err != nil {
			return fixture, err
		}
	} else if err := f.startPeers(); err != nil {
		return fixture, err
	}

	return fixture, nil
}

func (f *brokerProductFixture) startPeers() error {
	if _, err := f.waitStatus(1); err != nil {
		return err
	}

	if f.cohort != nil {
		for port := range f.publicReservations {
			if err := productReleasePublicPort(f, port); err != nil {
				return err
			}
		}
		if err := f.cohort.startAgent(); err != nil {
			logs := f.logs.String()
			if f.quorum != nil {
				for i := range f.quorum.peers {
					logs += f.quorum.peers[i].logs.String()
				}
			}
			return fmt.Errorf("%w\n%s", err, logs)
		}
	} else {
		f.client = tunnel.NewClient(f.tunnel, "product", "agent", func(public int) (int, error) {
			for _, a := range f.allocations {
				if a.Port == public {
					return a.LocalPort, nil
				}
			}
			return 0, errors.New("unknown product allocation")
		}, slog.New(slog.NewJSONHandler(&f.logs, nil)))
		if err := f.client.SetFlowConfig(tunnel.FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "product-agent", BootID: "product-agent-boot", OwnerGen: 1}); err != nil {
			return err
		}
		clientCtx, cancelClient := context.WithCancel(f.test.Context())
		f.cancelClient = cancelClient
		f.client.Start(clientCtx)
	}
	leaf, err := x509.ParseCertificate(f.certificate.Certificate[0])
	if err != nil {
		return err
	}
	f.pins = proto.CertPins{Current: tunnel.CertFingerprint(leaf)}
	if f.client != nil {
		for _, alloc := range f.allocations {
			if err := productReleasePublicPort(f, alloc.Port); err != nil {
				return err
			}
			if err := f.client.OpenHome(f.test.Context(), alloc.Port, alloc.LocalPort, alloc.Token, f.tunnel, alloc.Epoch, f.pins); err != nil {
				return err
			}
		}
	}
	f.peer, err = net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.allocations[0].Port)), time.Second)
	if err != nil {
		return err
	}
	if err := f.exchange(); err != nil {
		return err
	}
	ssConn, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.allocations[1].Port)), time.Second)
	if err != nil {
		return err
	}
	localTargetPort, err := brokerFixtureTCPPort(f.target.Addr())
	if err != nil {
		return errors.Join(err, ssConn.Close())
	}
	f.ssPeer, err = newProductSSPeer(ssConn, f.password, localTargetPort)
	if err != nil {
		_ = ssConn.Close()
		return err
	}
	if err := f.exchangeSS(); err != nil {
		return err
	}
	if err := f.writeACK("ongoing"); err != nil {
		return err
	}
	if err := f.writeACK("original"); err != nil {
		return err
	}
	if f.lifecycle {
		if err := f.openVictim(); err != nil {
			return err
		}
	}
	return nil
}

type productLogs struct {
	mu   sync.Mutex
	data []byte
	path string
}

func (l *productLogs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.data = append(l.data, p...)
	if l.path != "" {
		file, err := os.OpenFile(l.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return 0, err
		}
		_, err = file.Write(p)
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return 0, errors.Join(err, closeErr)
		}
	}
	return len(p), nil
}
func (l *productLogs) String() string { l.mu.Lock(); defer l.mu.Unlock(); return string(l.data) }

type brokerProductFixture struct {
	natsRoutes              []string
	quorum                  *productQuorum
	cohort                  *productCohort
	root, topology, tunnel  string
	addresses               map[string]bool
	publicReservations      map[int]net.Listener
	storeOnly               bool
	lifecycle               bool
	test                    *testing.T
	certificate             tls.Certificate
	password                string
	lifecycleState          map[string]any
	activeHeld              bool
	faultPoint              string
	started                 time.Time
	cutTimes                map[string]uint64
	sourceSHA, sourceDigest string
	candidateSHA            string
	previousSHA             string
	mappedIdentity          string
	mappedSHA               string
	ackIDs                  map[string]string
	originalIDs             map[string]string
	nats                    *server.Server
	cmd                     *exec.Cmd
	done                    chan struct{}
	waitErr                 error
	logs                    productLogs
	ss                      *ssproxy.Server
	ssPeer                  *productSSPeer
	client                  *tunnel.Client
	cancelClient            context.CancelFunc
	target                  net.Listener
	targetDone              chan struct{}
	targetMu                sync.Mutex
	targetConns             []net.Conn
	targetWorkers           sync.WaitGroup
	peer                    net.Conn
	victimPeer              net.Conn
	freshPeers              []net.Conn
	preparedResponse        chan error
	// transaction is the authorized S8 cut route of a single/sole-voter
	// broker (broker_product_transaction_linux.go); nil for the quorum roll.
	transaction *productTransaction
	pins        proto.CertPins
	allocations []port.Allocation
	sequence    atomic.Uint64

	lockProbes, lockAcquired                     uint64
	freshConnects, freshReplies, refusedConnects uint64
	maxFreshNS, writeStallNS                     int64
	seal                                         storage.StoreSeal
	originalACK                                  map[string]string
	beforeListeners                              map[string]string

	acknowledged map[string]string
	lastACK      atomic.Int64

	closed bool
}

func (f *brokerProductFixture) waitStatus(generation uint64) (*adminsock.Response, error) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		response, err := (&adminsock.Client{Path: filepath.Join(f.root, "admin.sock"), Timeout: 100 * time.Millisecond}).Call(adminsock.Request{Op: adminsock.OpRuntime})
		if err == nil && response.Runtime != nil && response.Runtime.BrokerImage != nil && response.Runtime.BrokerImage.Generation == generation && (generation != 1 || strings.Contains(f.logs.String(), `"event":"control_ready"`)) {
			leaderReady := true
			if generation == 1 && f.topology == "sole-voter" {
				data, err := os.ReadFile(filepath.Join(f.root, "leader.json"))
				var current struct{ Leader bool }
				leaderReady = err == nil && json.Unmarshal(data, &current) == nil && current.Leader
			}
			if generation == 1 && f.quorum != nil {
				ctx, cancel := context.WithTimeout(f.test.Context(), 2*time.Second)
				rows, err := f.quorum.health(ctx)
				cancel()
				leaders := 0
				leaderReady = err == nil && len(rows) == 3
				for _, row := range rows {
					if row.WritableLeaderConfirmed {
						leaders++
					}
					leaderReady = leaderReady && row.BrokerImage != nil && row.BrokerImage.Phase == proto.BrokerImagePhaseServing
				}
				leaderReady = leaderReady && leaders == 1
			}
			if leaderReady {
				return response, nil
			}
		}
		select {
		case <-f.done:
			return nil, fmt.Errorf("product exited: %v\n%s", f.waitErr, f.logs.String())
		default:
		}
		time.Sleep(10 * time.Millisecond)
	}
	return nil, fmt.Errorf("product readiness expired\n%s", f.logs.String())
}
func (f *brokerProductFixture) Checkpoint(label string) (BrokerImageReceipt, error) {
	if f.closed {
		return BrokerImageReceipt{}, errors.New("product fixture: closed")
	}
	if f.cohort != nil && (strings.HasPrefix(label, "flow-cohort-") || label == "control-probe-config") {
		return f.cohort.checkpoint(label)
	}
	if f.lifecycle {
		return f.lifecycleCheckpoint(label)
	}
	return f.productCheckpoint(label)
}

var errProductReloadUnknown = errors.New("product reload arm result unknown")

func (f *brokerProductFixture) productCheckpoint(label string) (receipt BrokerImageReceipt, resultErr error) {
	if id, ok := strings.CutPrefix(label, "write/"); ok {
		if err := f.writeACK(id); err != nil {
			return BrokerImageReceipt{}, err
		}
	}
	activeRefusal := label == "reload-active-refusal"
	if label == "reload-control-ready" || activeRefusal {
		f.cutTimes = map[string]uint64{"reload-start": uint64(time.Now().UnixNano())}
		if f.cohort != nil && f.cohort.opening != nil {
			join := f.cohort.releaseOnReady()
			defer func() {
				if err := join(); err != nil {
					f.test.Error(err)
				}
			}()
		}
		if err := os.Rename(filepath.Join(f.root, "candidate"), filepath.Join(f.root, "broker-product")); err != nil {
			return BrokerImageReceipt{}, err
		}
		// The build's expected artifact SHA is fixed before the timed command,
		// just like a downloaded release manifest. Production hashes the staged
		// pathname again during admission and both readers during preflight.
		sha := f.candidateSHA
		// The authorized route's describe/Prepare/root stage are inert and run
		// before the timed cut; only Start cuts (S8 BROKER-EXPORTER R-11).
		start, err := prepareProductStart(f, sha)
		if err != nil {
			return BrokerImageReceipt{}, fmt.Errorf("authorized broker cut before Start: %w\n%s", err, f.logs.String())
		}
		f.cutTimes["reload-start"] = uint64(time.Now().UnixNano())
		writesStop, writesDone := make(chan struct{}), make(chan struct{})
		writesJoined := false
		go func() {
			defer close(writesDone)
			for {
				select {
				case <-writesStop:
					return
				default:
				}
				sum := sha256.Sum256([]byte("ongoing"))
				if f.cutTimes != nil {
					f.cutTimes["status-begin"] = uint64(time.Now().UnixNano())
				}
				response, err := (&adminsock.Client{Path: filepath.Join(f.root, "admin.sock"), Timeout: 100 * time.Millisecond}).Call(adminsock.Request{Op: adminsock.OpSessionAllow, FP: "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:]), Note: storeFixtureDigest([]byte("ack:ongoing"))})
				if err == nil && response.OK {
					f.lastACK.Store(time.Now().UnixNano())
				}
				select {
				case <-writesStop:
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		}()
		defer func() {
			if !writesJoined {
				close(writesStop)
				<-writesDone
			}
		}()
		// Issue the arm concurrently; it cannot reply before the real closed seal.
		responseCh := make(chan error, 1)
		go func() {
			if start != nil {
				responseCh <- start()
				return
			}
			response, err := (&adminsock.Client{Path: filepath.Join(f.root, "admin.sock"), Timeout: 6 * time.Second}).Call(adminsock.Request{Op: adminsock.OpBrokerUpgradeReload, ExpectSHA256: sha})
			if err == nil && (response == nil || !response.OK) {
				err = fmt.Errorf("reload refused: %+v", response)
				if response != nil && response.Code == adminsock.CodeStoreError && response.Error == brokerimage.DetailControlResultUnknown {
					err = errProductReloadUnknown
				}
			}
			responseCh <- err
		}()
		until := time.Now().Add(6 * time.Second)
		observed := false
		for time.Now().Before(until) {
			if data, err := os.ReadFile(filepath.Join(f.root, "closed.json")); err == nil {
				var closed struct {
					Seal storage.StoreSeal
					Seq  uint64
				}
				if err = json.Unmarshal(data, &closed); err != nil {
					return BrokerImageReceipt{}, err
				}
				f.seal = closed.Seal
				observed = true
				break
			}
			select {
			case err := <-responseCh:
				// An uncertain arm is not a refused cut. Keep observing this
				// original attempt; never send another unsafe reload. The closed
				// seal, new mapped artifact and ready generation must still pass.
				// origin: hot-update S6 plan §5 / §8.1 / T50
				if errors.Is(err, errProductReloadUnknown) {
					responseCh = nil
					continue
				}
				return BrokerImageReceipt{}, fmt.Errorf("reload failed before closed-store barrier: %v\n%s", err, f.logs.String())
			default:
			}
			time.Sleep(time.Millisecond)
		}
		if !observed {
			return BrokerImageReceipt{}, fmt.Errorf("closed-store barrier absent\n%s", f.logs.String())
		}
		if f.cohort != nil && f.cohort.opening != nil {
			f.cohort.opening.beginWork()
		}
		// The contender spans the closed-store window and exec. Every acquisition
		// uses a distinct OFD; a local alias cannot accidentally make this green.
		probeStop, probeDone := make(chan struct{}), make(chan struct{})
		var probeErr error
		go func() {
			defer close(probeDone)
			for {
				lock, err := cluster.AcquireImageDataDirLock(f.root)
				f.lockProbes++
				if lock != nil {
					f.lockAcquired++
					_ = lock.Release()
					f.test.Error("closed-store independent contender actually acquired canonical flock")
					probeErr = errors.New("closed-store lock was acquired by an independent contender")
					return
				}
				if lock == nil && !errors.Is(err, cluster.ErrDataDirLockHeld) {
					probeErr = err
					return
				}
				select {
				case <-probeStop:
					return
				case <-time.After(time.Millisecond):
				}
			}
		}()
		probeJoined := false
		defer func() {
			if !probeJoined {
				close(probeStop)
				<-probeDone
			}
		}()
		var freshDone chan error
		var freshNS int64
		var freshBootStart, freshBootEnd int64
		freshJoined := false
		if !f.storeOnly {
			// Dial while the old store is closed. The inherited LISTEN queue must
			// accept this request and deliver its duplex reply in the new image.
			freshStart := time.Now()
			var clockErr error
			freshBootStart, clockErr = controlbudget.Now()
			if clockErr != nil {
				return BrokerImageReceipt{}, clockErr
			}
			fresh, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.allocations[0].Port)), time.Second)
			f.freshConnects++
			if err != nil {
				f.refusedConnects++
				return BrokerImageReceipt{}, err
			}
			if f.lifecycle {
				// Give the lifecycle observer custody until it can close and
				// join this temporary flow before taking its live FD census.
				// origin: hot-update S6 plan §9.3 / T52
				f.freshPeers = append(f.freshPeers, fresh)
			} else {
				defer func() { resultErr = errors.Join(resultErr, fresh.Close()) }()
			}
			freshDone = make(chan error, 1)

			go func() {
				_ = fresh.SetDeadline(freshStart.Add(5 * time.Second))
				value := []byte("fresh-inside-closed-store")
				_, err := fresh.Write(value)
				got := make([]byte, len(value))
				if err == nil {
					_, err = io.ReadFull(fresh, got)
				}
				if err == nil && !bytes.Equal(value, got) {
					err = errors.New("fresh listener returned wrong bytes")
				}
				freshNS = time.Since(freshStart).Nanoseconds()
				freshBootEnd, _ = controlbudget.Now()
				freshDone <- err
			}()

			defer func() {
				if !freshJoined {
					_ = fresh.Close()
					<-freshDone
				}
			}()

		}
		close(writesStop)
		<-writesDone
		writesJoined = true
		var tcpDone, ssDone chan error
		cohortJoined := false
		if !f.storeOnly {
			tcpDone, ssDone = make(chan error, 1), make(chan error, 1)
			go func() { tcpDone <- f.exchange() }()
			go func() { ssDone <- f.exchangeSS() }()

			defer func() {
				if !cohortJoined {
					<-tcpDone
					<-ssDone
				}
			}()

		}
		lastOldACK := f.lastACK.Load()
		if err := os.WriteFile(filepath.Join(f.root, "release.json"), []byte("{}"), 0600); err != nil {
			return BrokerImageReceipt{}, err
		}
		if responseCh != nil {
			if err := <-responseCh; err != nil && !errors.Is(err, errProductReloadUnknown) {
				return BrokerImageReceipt{}, fmt.Errorf("reload arm: %w\n%s", err, f.logs.String())
			}
		}
		expectedGeneration := uint64(2)
		if f.faultPoint == "prepared-exec-error" {
			expectedGeneration = 1
			if err := f.awaitMarker("exec-recovered"); err != nil {
				return BrokerImageReceipt{}, err
			}
		}
		if _, err := f.waitStatus(expectedGeneration); err != nil {
			return BrokerImageReceipt{}, err
		}
		f.cutTimes["current-ready"] = uint64(time.Now().UnixNano())
		// Release application traffic on actual product readiness, before archive
		// hashing and SQL receipts. Observation work is not a business pause.
		if f.cohort != nil && f.cohort.opening != nil {
			if err := awaitCohortReady(f.test.Context(), f.cohort); err != nil {
				return BrokerImageReceipt{}, err
			}
		}
		if !f.storeOnly {
			// Measure the first durable write as soon as current control is usable;
			// byte-cohort validation continues independently of SQL admission.
			if err := f.writeACK("current"); err != nil {
				return BrokerImageReceipt{}, fmt.Errorf("first current write: %w\n%s", err, f.logs.String())
			}
			f.writeStallNS = time.Now().UnixNano() - lastOldACK
			f.cutTimes["first-write"] = uint64(time.Now().UnixNano())
			f.cutTimes["last-old-write"] = uint64(lastOldACK)
			tcpErr, ssErr := <-tcpDone, <-ssDone
			cohortJoined = true
			if f.cutTimes != nil {
				f.cutTimes["cohort-finished"] = uint64(time.Now().UnixNano())
			}
			if err := errors.Join(tcpErr, ssErr); err != nil {
				return BrokerImageReceipt{}, fmt.Errorf("original S5/SS cohort after exec: %w\n%s", err, f.logs.String())
			}

		}
		if activeRefusal {
			if err := f.exchangeVictim(); err != nil {
				return BrokerImageReceipt{}, err
			}
			if err := f.writeACK("candidate"); err != nil {
				return BrokerImageReceipt{}, err
			}
			if err := f.exchangeSS(); err != nil {
				return BrokerImageReceipt{}, err
			}
			if _, err := os.Stat(filepath.Join(f.root, "active-fault-entered.json")); err != nil {
				return BrokerImageReceipt{}, err
			}
			if _, holdErr := os.Stat(filepath.Join(f.root, "active-observe-only.json")); holdErr != nil {
				if err := os.WriteFile(filepath.Join(f.root, "active-fault-release.json"), []byte("{}"), 0600); err != nil {
					return BrokerImageReceipt{}, err
				}
				if _, err := f.waitStatus(3); err != nil {
					return BrokerImageReceipt{}, err
				}
				if err := f.exchange(); err != nil {
					return BrokerImageReceipt{}, err
				}
			}
		}
		if activeRefusal {
			if err := f.exchangeSS(); err != nil {
				return BrokerImageReceipt{}, fmt.Errorf("original SS after rollback: %w", err)
			}
		}
		close(probeStop)
		<-probeDone
		probeJoined = true
		if probeErr != nil {
			return BrokerImageReceipt{}, probeErr
		}
		if !f.storeOnly {
			freshErr := <-freshDone
			freshJoined = true
			if f.cutTimes != nil {
				f.cutTimes["fresh-finished"] = uint64(time.Now().UnixNano())
			}
			if freshErr != nil {
				return BrokerImageReceipt{}, freshErr
			}
			f.freshReplies++
			f.maxFreshNS = freshNS
			if f.cohort != nil {
				f.cohort.connects = []cohortConnect{{Slot: 0, Started: freshBootStart, Finished: freshBootEnd, RC: 0}}
			}

		}

		data, err := os.ReadFile(filepath.Join(f.root, "pre-replay.json"))
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		var gate struct {
			Rows   map[string]string
			Cursor uint64
		}
		if err = json.Unmarshal(data, &gate); err != nil {
			return BrokerImageReceipt{}, err
		}
		observedPrefix := map[string]string{}
		for fp := range f.originalACK {
			observedPrefix[fp] = gate.Rows[fp]
		}
		if !reflect.DeepEqual(observedPrefix, f.originalACK) || gate.Cursor < f.seal.Stamp.AppliedIndex {
			return BrokerImageReceipt{}, errors.New("ACK prefix missing before replay")
		}

	} else if label != "before-reload" && !strings.HasPrefix(label, "write/") {
		return BrokerImageReceipt{}, errors.New("product fixture: unobserved checkpoint " + label)
	}
	response, err := (&adminsock.Client{Path: filepath.Join(f.root, "admin.sock"), Timeout: 5 * time.Second}).Call(adminsock.Request{Op: adminsock.OpRuntime})
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	pid := f.cmd.Process.Pid
	ticks, err := storeFixtureStartTicks(pid)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	if f.cutTimes != nil {
		f.cutTimes["sha-begin"] = uint64(time.Now().UnixNano())
	}
	// Linux prevents writes to the mapped executable. Cache only its kernel
	// object identity, and hash anew whenever exec installs another inode.
	// Receipt callers still observe PID/startticks and the current /proc inode.
	executable := fmt.Sprintf("/proc/%d/exe", pid)
	stat, err := os.Stat(executable)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	identity, err := brokerFixtureExecutableIdentity(stat)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	if f.mappedIdentity != identity {
		f.mappedSHA, err = storeFixtureHash(executable)
		if err != nil {
			return BrokerImageReceipt{}, err
		}
		f.mappedIdentity = identity
	}
	sha := f.mappedSHA
	if f.cutTimes != nil {
		f.cutTimes["rows-begin"] = uint64(time.Now().UnixNano())
	}
	db, err := storage.OpenReadOnly(filepath.Join(f.root, "tether.db"))
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	rows, err := db.Query("SELECT fp,note FROM session_creators ORDER BY fp")
	if err != nil {
		_ = db.Close()
		return BrokerImageReceipt{}, err
	}
	acks := map[string]string{}
	for rows.Next() {
		var fp, note string
		if err = rows.Scan(&fp, &note); err != nil {
			_ = rows.Close()
			_ = db.Close()
			return BrokerImageReceipt{}, err
		}
		acks[fp] = note
	}
	_ = rows.Close()
	_ = db.Close()
	if f.cutTimes != nil {
		f.cutTimes["listeners-begin"] = uint64(time.Now().UnixNano())
	}
	listeners, err := f.listenerIdentities()
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	if label == "before-reload" || strings.HasPrefix(label, "write/") && response.Runtime.BrokerImage.Generation == 1 {
		f.originalACK = map[string]string{}
		for fp, expected := range f.acknowledged {
			if acks[fp] != expected {
				return BrokerImageReceipt{}, errors.New("ACK was not committed")
			}
			f.originalACK[fp] = acks[fp]
		}
		f.beforeListeners = listeners
		f.originalIDs = map[string]string{}
		for id, fp := range f.ackIDs {
			f.originalIDs[id] = fp
		}
	}
	// Prefix digest includes only independently ACKed pre-cut rows. New effects
	// are tested separately and cannot overwrite the expected prefix.
	prefix := map[string]string{}
	for fp, expected := range f.originalACK {
		if acks[fp] != expected {
			return BrokerImageReceipt{}, errors.New("current ACK prefix changed")
		}
		prefix[fp] = acks[fp]
	}
	prefixData, _ := json.Marshal(prefix)
	readSequence := func(name string) uint64 {
		data, err := os.ReadFile(filepath.Join(f.root, name+".json"))
		if err != nil {
			return 0
		}
		var row struct{ Seq uint64 }
		_ = json.Unmarshal(data, &row)
		return row.Seq
	}
	nodeSeq, execSeq, storeSeq := readSequence("node-closed"), readSequence("exec"), readSequence("closed")

	oldDefers := uint64(0)
	if _, err := os.Stat(filepath.Join(f.root, "run-defer.json")); err == nil {
		oldDefers = 1
	}
	revoked := false
	if data, err := os.ReadFile(filepath.Join(f.root, "revoked.json")); err == nil {
		var row struct{ Effective bool }
		_ = json.Unmarshal(data, &row)
		revoked = row.Effective
	}
	absent := false
	if label == "reload-control-ready" {
		conn, err := net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.allocations[2].Port)), time.Second)
		if conn != nil {
			_ = conn.Close()
		}
		absent = err != nil
	}

	candidateBeforeReplay := false
	if data, err := os.ReadFile(filepath.Join(f.root, "pre-replay.json")); err == nil {
		var gate struct{ Rows map[string]string }
		if json.Unmarshal(data, &gate) == nil {
			sum := sha256.Sum256([]byte("candidate"))
			candidateBeforeReplay = gate.Rows["SHA256:"+base64.RawStdEncoding.EncodeToString(sum[:])] == storeFixtureDigest([]byte("ack:candidate"))
		}
	}
	value := map[string]any{"pid": pid, "startticks": ticks, "running_sha256": sha, "generation": response.Runtime.BrokerImage.Generation, "store_close_seq": storeSeq, "node_close_seq": nodeSeq, "exec_seq": execSeq, "old_defer_count": oldDefers, "ack_count": len(prefix), "ack_prefix_digest": storeFixtureDigest(prefixData), "closed_store_lock_probes": f.lockProbes, "closed_store_lock_acquired": f.lockAcquired, "listener_identities": listeners, "fresh_connects": f.freshConnects, "fresh_replies": f.freshReplies, "refused_connects": f.refusedConnects, "max_fresh_reply_ns": f.maxFreshNS, "write_stall_ns": f.writeStallNS, "export_revoke_attempted": revoked, "export_revoke_refused_after_import": absent, "ack_rows": acks, "candidate_ack_before_replay": candidateBeforeReplay}

	if err := f.addStoreEvidence(value, label, acks); err != nil {
		return BrokerImageReceipt{}, err
	}
	if counters, ok := value["counters"].(map[string]uint64); ok {
		counters["receipt-finished"] = uint64(time.Now().UnixNano())
	}
	data, err := json.Marshal(value)
	if err != nil {
		return BrokerImageReceipt{}, err
	}
	return BrokerImageReceipt{Label: label, JSON: data, Digest: storeFixtureDigest(data)}, nil
}
func (f *brokerProductFixture) ArmFault(fault BrokerImageFault) error {
	f.faultPoint = fault.Point
	if fault.Point == "after-prepare-before-exec" && fault.Ordinal == 1 && fault.Once {
		return os.WriteFile(filepath.Join(f.root, "prepared-fault.json"), []byte("{}"), 0600)
	}
	if fault.Point == "prepared-exec-error" && fault.Ordinal == 1 && fault.Once {
		return os.WriteFile(filepath.Join(f.root, "exec-error-fault.json"), []byte("{}"), 0600)
	}
	if fault.Point != "active-after-new-ack-and-bytes" || fault.Ordinal != 1 || !fault.Once {
		return errors.New("product fixture: unsupported fault")
	}
	return os.WriteFile(filepath.Join(f.root, "active-fault.json"), []byte("{}"), 0600)
}
func (f *brokerProductFixture) Close() error {
	if f.closed {
		return nil
	}
	f.closed = true
	var closeErr error
	if f.quorum != nil {
		closeErr = errors.Join(closeErr, f.quorum.finish())
	}
	for port := range f.publicReservations {
		closeErr = errors.Join(closeErr, productReleasePublicPort(f, port))
	}
	if f.cohort != nil {
		closeErr = errors.Join(closeErr, f.cohort.close())
	}
	if f.cmd != nil && f.cmd.Process != nil && f.done != nil {
		_ = os.WriteFile(filepath.Join(f.root, "idle-release.json"), []byte("{}"), 0600)
		_ = os.WriteFile(filepath.Join(f.root, "stop-release.json"), []byte("{}"), 0600)
		_ = f.cmd.Process.Signal(os.Interrupt)
		select {
		case <-f.done:
		case <-time.After(10 * time.Second):
			_ = f.cmd.Process.Kill()
			<-f.done
		}
	}
	if f.ssPeer != nil {
		_ = f.ssPeer.conn.Close()
	}
	if f.ss != nil {
		f.ss.Stop()
	}
	if f.peer != nil {
		_ = f.peer.Close()
	}
	if f.victimPeer != nil {
		_ = f.victimPeer.Close()
	}
	for _, peer := range f.freshPeers {
		_ = peer.Close()
	}
	if f.client != nil {
		for _, a := range f.allocations {
			f.client.Close(a.Port)
		}
	}
	if f.cancelClient != nil {
		f.cancelClient()
	}
	closeErr = errors.Join(closeErr, tunnel.StopClient(f.client, 5*time.Second))
	if f.target != nil {
		_ = f.target.Close()
		<-f.targetDone
		f.targetMu.Lock()
		for _, conn := range f.targetConns {
			_ = conn.Close()
		}
		f.targetMu.Unlock()
		f.targetWorkers.Wait()
	}
	if f.quorum != nil {
		closeErr = errors.Join(closeErr, f.quorum.close())
	}
	if f.nats != nil {
		f.nats.Shutdown()
		f.nats.WaitForShutdown()
	}
	return errors.Join(closeErr, productRaceEvidence(f.logs.String()))
}

func storeFixtureStartTicks(pid int) (uint64, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	_, suffix, ok := strings.Cut(string(data), ") ")
	if !ok {
		return 0, errors.New("product fixture: invalid process stat")
	}
	fields := strings.Fields(suffix)
	if len(fields) < 20 {
		return 0, errors.New("product fixture: short process stat")
	}
	return strconv.ParseUint(fields[19], 10, 64)
}

// Future listeners are selected before they bind. Keep one fixture-local
// reservation set so public/SSH/SS/Raft ports cannot collide with the already
// chosen tunnel control address merely because it is not listening yet.
// origin: hot-update S6 plan §6.3 / T48
func productFixtureAddress(f *brokerProductFixture) string {
	return productFixtureAddressUsing(f, func() string { return productAddress(f.test) })
}
func productFixtureAddressUsing(f *brokerProductFixture, candidate func() string) string {
	if f.addresses == nil {
		f.addresses = map[string]bool{}
	}
	for {
		address := candidate()
		if !f.addresses[address] {
			f.addresses[address] = true
			return address
		}
	}
}

// A remembered address is not a kernel reservation. Keep each public port
// bound across artifact builds and cold startup, until its first REGISTER;
// no business flow exists during this fixture-only ownership transfer.
// origin: hot-update S6 plan §6.3 / T28 / T56 / TESTFIX-2
func productReservePublicAddress(f *brokerProductFixture) string {
	for attempt := 0; attempt < 8; attempt++ {
		address := productFixtureAddress(f)
		reservation, err := net.Listen("tcp4", address)
		if err != nil {
			continue
		}
		if f.publicReservations == nil {
			f.publicReservations = make(map[int]net.Listener)
		}
		// origin: hot-update S6 plan §6.3 / T28; S10 LINT-L4
		addr, ok := reservation.Addr().(*net.TCPAddr)
		if !ok {
			if err := reservation.Close(); err != nil {
				f.test.Errorf("product fixture: close public reservation: %v", err)
			}
			f.test.Fatal("product fixture: public reservation is not TCP")
		}
		f.publicReservations[addr.Port] = reservation
		return address
	}
	f.test.Fatal("product fixture: public port reservation unavailable")
	return ""
}

func productReleasePublicPort(f *brokerProductFixture, port int) error {
	reservation := f.publicReservations[port]
	if reservation == nil {
		return nil
	}
	delete(f.publicReservations, port)
	return reservation.Close()
}

func productAddress(t *testing.T) string {
	t.Helper()
	// Bind outside the kernel's ephemeral source-port band. Otherwise our own
	// NATS/TLS dials can claim a released future Raft/public port during the build.
	limit := 30000
	if data, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range"); err == nil {
		var first, last int
		if _, err = fmt.Sscan(string(data), &first, &last); err == nil {
			limit = min(limit, first)
		}
	}
	for attempt := 0; attempt < 100; attempt++ {
		var random [4]byte
		if _, err := rand.Read(random[:]); err != nil {
			t.Fatal(err)
		}
		port := 1024 + int(binary.BigEndian.Uint32(random[:])%uint32(limit-1024))
		listener, err := net.Listen("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
		if err != nil {
			continue
		}
		address := listener.Addr().String()
		if err := listener.Close(); err != nil {
			t.Fatal(err)
		}
		return address
	}
	t.Fatal("product fixture: private listening port unavailable")
	return ""
}

func (f *brokerProductFixture) exchange() error {
	sequence := f.sequence.Add(1)
	value := []byte(fmt.Sprintf("original-duplex-%d", sequence))
	_ = f.peer.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := f.peer.Write(value); err != nil {
		return err
	}
	got := make([]byte, len(value))
	if _, err := io.ReadFull(f.peer, got); err != nil {
		return err
	}
	if !bytes.Equal(value, got) {
		return errors.New("product fixture: original flow byte mismatch")
	}
	return nil
}
func (f *brokerProductFixture) writeACK(id string) error {
	sum := sha256.Sum256([]byte(id))
	response, err := (&adminsock.Client{Path: filepath.Join(f.root, "admin.sock"), Timeout: 5 * time.Second}).Call(adminsock.Request{Op: adminsock.OpSessionAllow, FP: "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:]), Note: storeFixtureDigest([]byte("ack:" + id))})
	if err != nil {
		return err
	}
	if !response.OK {
		return fmt.Errorf("ACK refused: %+v", response)
	}
	if f.acknowledged == nil {
		f.acknowledged = map[string]string{}
	}
	if f.ackIDs == nil {
		f.ackIDs = map[string]string{}
	}
	f.ackIDs[id] = "SHA256:" + base64.RawStdEncoding.EncodeToString(sum[:])
	f.acknowledged["SHA256:"+base64.RawStdEncoding.EncodeToString(sum[:])] = storeFixtureDigest([]byte("ack:" + id))
	f.lastACK.Store(time.Now().UnixNano())
	return nil
}

func (f *brokerProductFixture) listenerIdentities() (map[string]string, error) {
	// Match the live LISTEN table to this process's actual socket descriptors.
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/net/tcp", f.cmd.Process.Pid))
	if err != nil {
		return nil, err
	}
	wanted := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 || fields[3] != "0A" {
			continue
		}
		_, portHex, _ := strings.Cut(fields[1], ":")
		portNumber, _ := strconv.ParseUint(portHex, 16, 16)
		_, ctrlPort, _ := net.SplitHostPort(f.tunnel)
		live := strconv.Itoa(f.allocations[0].Port)
		if strconv.FormatUint(portNumber, 10) == ctrlPort {
			wanted[fields[9]] = "control"
		}
		if strconv.FormatUint(portNumber, 10) == live {
			wanted[fields[9]] = "live"
		}
	}
	entries, err := os.ReadDir(fmt.Sprintf("/proc/%d/fd", f.cmd.Process.Pid))
	if err != nil {
		return nil, err
	}
	found := map[string]string{}
	for _, entry := range entries {
		link, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%s", f.cmd.Process.Pid, entry.Name()))
		if strings.HasPrefix(link, "socket:[") {
			inode := strings.TrimSuffix(strings.TrimPrefix(link, "socket:["), "]")
			if name := wanted[inode]; name != "" {
				found[name] = inode
			}
		}
	}
	if len(found) != 2 {
		return nil, errors.New("original LISTEN identities missing")
	}
	return found, nil
}

func (f *brokerProductFixture) exchangeSS() error {
	value := []byte(fmt.Sprintf("original-SS-sequence-%d", f.sequence.Add(1)))
	_ = f.ssPeer.conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := f.ssPeer.write(value); err != nil {
		return err
	}
	var got []byte
	for len(got) < len(value) {
		next, err := f.ssPeer.read()
		if err != nil {
			return err
		}
		got = append(got, next...)
	}
	if !bytes.Equal(got, value) {
		return errors.New("original SS byte sequence changed")
	}
	return nil
}

func (f *brokerProductFixture) openVictim() error {
	if f.victimPeer == nil {
		var err error
		f.victimPeer, err = net.DialTimeout("tcp4", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.allocations[2].Port)), time.Second)
		if err != nil {
			return err
		}
	}
	return f.exchangeVictim()
}
func (f *brokerProductFixture) exchangeVictim() error {
	if f.victimPeer == nil {
		return nil
	}
	if err := f.victimPeer.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return err
	}
	payload := []byte(fmt.Sprintf("original-victim-%d", f.sequence.Add(1)))
	if _, err := f.victimPeer.Write(payload); err != nil {
		return err
	}
	got := make([]byte, len(payload))
	if _, err := io.ReadFull(f.victimPeer, got); err != nil {
		return err
	}
	if !bytes.Equal(got, payload) {
		return errors.New("original victim stream changed")
	}
	return f.victimPeer.SetDeadline(time.Time{})
}

// The mapped-file cache is valid only with an observed kernel identity.
// origin: hot-update S6 plan §4.1 / T48
func brokerFixtureExecutableIdentity(stat os.FileInfo) (string, error) {
	kernel, ok := stat.Sys().(*syscall.Stat_t)
	if !ok || kernel == nil {
		return "", errors.New("product fixture: executable lacks kernel file identity")
	}
	return fmt.Sprintf("%d:%d:%d", kernel.Dev, kernel.Ino, kernel.Size), nil
}
