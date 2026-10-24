package broker

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/flow"
	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/port"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/workload"
	"github.com/hashicorp/raft"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// A real peer signs the CONNECT grant while the local signed observer is held.
// CONNECT success must wait for that local observation, without minting another
// request budget or accepting a static-auth connection as proof.
// origin: hot-update S6 plan §8.1 / T29 / N3 maintenance integration
func TestBrokerAuthFirstWaitsForLocalProofAfterPeerConnect(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exercisePeerConnectBeforeLocalProof(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "peer CONNECT local proof x5", g)
	testharness.AssertNoFDLeak(t, "peer CONNECT local proof x5", fd, 0)
}

func exercisePeerConnectBeforeLocalProof(t *testing.T) {
	t.Helper()
	b, nc, url := brokerImageAuthFixture(t, true)
	peer := newBrokerAuthOwner(b, 0)
	if _, err := b.installAuthCallout(nc, peer); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stopBrokerImageAuth(peer, brokerAuthorityDeadline(t, time.Second)) })
	kp, pub := brokerImageUser(t)
	owner := newBrokerAuthOwner(b, brokerAuthorityDeadline(t, 3*time.Second))
	key, err := nkeys.FromSeed(b.cfg.AuthCallout.AccountSeed)
	if err != nil {
		t.Fatal(err)
	}
	defer key.Wipe()
	owner.Key, owner.ProbeNkey = key, pub
	handler := &authcallout.Handler{DB: b.read().SQL(), AccountKp: key, Now: time.Now, Logger: silentLogger(), JWTTTL: 24 * time.Hour}
	entered, release, localDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer func() {
		releaseOnce.Do(func() { close(release) })
		select {
		case <-entered:
			<-localDone
		default:
		}
	}()
	sub, err := nc.Subscribe("$SYS.REQ.USER.AUTH", func(msg *nats.Msg) {
		request, err := jwt.DecodeAuthorizationRequestClaims(string(msg.Data))
		if err != nil || request.ConnectOptions.Nkey != pub {
			return
		}
		defer close(localDone)
		close(entered)
		<-release
		response, err := handler.Handle(string(msg.Data))
		if err == nil {
			owner.ObserveProbe(string(msg.Data), response)
			_ = msg.Respond([]byte(response))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe: %v", err)
		}
	}()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	// The actual callout server verifies the peer JWT before CONNECT returns.
	probe, err := nats.Connect(url, nats.Nkey(pub, kp.Sign), nats.Name("tether-cli"), nats.MaxReconnects(0), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer probe.Close()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("local observer was not exercised")
	}
	owner.Mu.Lock()
	premature := owner.ProbeReply
	owner.Mu.Unlock()
	if premature {
		t.Fatal("held local observer already supplied proof")
	}
	done := make(chan error, 1)
	go func() { done <- waitBrokerImageAuthProof(owner, owner.Deadline) }()
	// The unproved connection must remain pending before releasing the real
	// local handler; no fake boolean or pre-seeded receipt satisfies the wait.
	select {
	case err := <-done:
		t.Fatalf("peer CONNECT completed the local proof barrier: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	<-localDone
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	owner.Mu.Lock()
	proved := owner.ProbeReply && owner.ReplyBootNS > 0
	owner.Mu.Unlock()
	if !proved {
		t.Fatal("local signed JWT was not observed after CONNECT")
	}
}

// The factory cannot generate new identity/key material or copy the budget.
// P07's sealed ticket round trip is a separate integration dependency; these
// unit controls keep the broker factory from selecting the cold path at all.
// origin: hot-update S6 plan §6.3 / T28
func TestBrokerIssuerAndBudgetPreserved(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker live image requires Linux")
	}
	exerciseBrokerImageFactory(t)
	fn := findFuncDecl(t, "expose.go", "brokerTunnelImportConfig")
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		var name string
		switch callee := call.Fun.(type) {
		case *ast.Ident:
			name = callee.Name
		case *ast.SelectorExpr:
			name = callee.Sel.Name
		}
		switch name {
		case "configureTunnelAuthority", "SetFlowConfig", "NewServer", "NewServerWithCert":
			t.Errorf("hot factory invokes cold ticket-key constructor %s", name)
		}
		return true
	})
}

func exerciseBrokerImageFactory(t *testing.T) {
	t.Helper()
	old, request := authorityFixture(t, openDB(t))
	current, _ := authorityFixture(t, openDB(t))
	current.cfg.PublicHost = "advertised.example.invalid"
	current.cfg.TunnelPublicHost = "127.0.0.1"
	cert, _ := registerAuthorityCertificate(t)
	current.tunnelCert = &cert
	if _, err := current.cfg.DB.Exec(`UPDATE proxy_subscribers SET psk='current-secret' WHERE sub_id='bob'; UPDATE sessions SET proxy_epoch=8`); err != nil {
		t.Fatal(err)
	}
	budget := &workload.ReplayBudget{Limit: workload.DefaultReplayBudget}
	// A real unrelated reservation makes global Used differ from server Used.
	other, err := workload.NewReplay(budget, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = other.Close() }()
	image := tunnel.ServerImage{IssuerBootID: "original-issuer", InstanceID: "original-instance",
		OwnerGeneration: 7, Generation: 99, ReplayLimit: budget.Limit, ReplayUsed: 4096,
		Flows: flow.Image{ReservedBytes: 4096}}
	cfg, err := brokerTunnelImportConfig(current, image, budget)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Flow.BootID != image.IssuerBootID || cfg.Flow.InstanceID != image.InstanceID || cfg.Flow.OwnerGen != 7 || cfg.Flow.Budget != budget {
		t.Fatal("image identity, agent owner generation or shared budget pointer changed")
	}
	if cfg.PublicHost != "127.0.0.1" || cfg.Certificate != current.tunnelCert || cfg.Logger != current.cfg.Logger {
		t.Fatal("image factory changed listener bind policy, certificate or logger")
	}
	current.cfg.TunnelPublicHost = ""
	defaults, err := brokerTunnelImportConfig(current, image, budget)
	if err != nil || defaults.PublicHost != "0.0.0.0" {
		t.Fatalf("image listener bind default differs from cold start: %v", err)
	}
	current.cfg.TunnelPublicHost = "127.0.0.1"
	log, err := workload.NewReplay(cfg.Flow.Budget, 2048)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = log.Close() }()
	if budget.Used() != 3072 || cfg.Flow.Budget.Used() != 3072 {
		t.Fatal("import factory reservations do not charge the one receiving budget")
	}
	grant, err := cfg.Flow.Authorize(request)
	if err != nil || grant.ProxyEpoch != 8 || len(grant.Keys) != 2 || string(grant.Keys[1].Secret) != "current-secret" {
		t.Fatalf("factory does not consult current SQL authority: %v", err)
	}
	if oldGrant, err := tunnelAuthorityLookup(old, request); err != nil || oldGrant.ProxyEpoch != 7 {
		t.Fatalf("old/current authority fixture is not distinct: %v", err)
	}
	if err := cfg.Lookup("lab", "lab-1", 14000, "authority-token-hash", 4); err != nil {
		t.Fatalf("legacy callback was not rebound: %v", err)
	}
	if _, err := current.cfg.DB.Exec(`UPDATE sessions SET proxy_enabled=0`); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Flow.Authorize(request); !errors.Is(err, errTunnelAuthorityRevoked) {
		t.Fatalf("fresh current-store revocation was ignored: %v", err)
	}
	if err := cfg.Flow.TokenLookup(request); !errors.Is(err, errTunnelAuthorityRevoked) {
		t.Fatalf("REGISTER retained old authority: %v", err)
	}
	for _, mutate := range []func(*tunnel.ServerImage){
		func(im *tunnel.ServerImage) { im.IssuerBootID = "" },
		func(im *tunnel.ServerImage) { im.InstanceID = "" },
		func(im *tunnel.ServerImage) { im.OwnerGeneration = 0 },
		func(im *tunnel.ServerImage) { im.ReplayUsed = budget.Used() },
		func(im *tunnel.ServerImage) { im.ReplayUsed = -1 },
		func(im *tunnel.ServerImage) { im.ReplayLimit-- },
		func(im *tunnel.ServerImage) { im.ReplayUsed, im.Flows.ReservedBytes = budget.Limit, budget.Limit },
	} {
		bad := image
		mutate(&bad)
		if _, err := brokerTunnelImportConfig(current, bad, budget); err == nil {
			t.Fatal("invalid image identity/domain reservation admitted")
		}
	}
	if _, err := brokerTunnelImportConfig(current, image, nil); err == nil {
		t.Fatal("missing shared budget admitted")
	}
}

// Exercise a true NATS auth_callout CONNECT, local JWT proof, checked flush,
// current SQL/PIN/commit seams and the real store-user fence. Full business/JS
// ReadyCh ordering belongs to P11/P27; this owner never emits that barrier.
// origin: hot-update S6 plan §8.1 / T29
func TestBrokerAuthFirstReadyBarrier(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker image uses CLOCK_BOOTTIME")
	}
	t.Run("local-callout", exerciseAuthorityRebind)
	t.Run("flush-failure", exerciseBrokerAuthFlushFailure)
	t.Run("static-auth-is-not-callout", exerciseBrokerStaticAuthRefusal)
	t.Run("cluster-PIN", exerciseBrokerImagePINCommit)
	t.Run("invalid-entry", exerciseBrokerAuthEntryRefusal)
}

// origin: hot-update S6 plan §11 / §12.1 P12
func TestBrokerAuthorityLeakCycles(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("broker live image requires Linux")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseAuthorityRebind(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker authority rebind", g)
	testharness.AssertNoFDLeak(t, "broker authority rebind", fd, 0)
}

func brokerAuthorityDeadline(t *testing.T, budget time.Duration) controlbudget.Deadline {
	t.Helper()
	now, err := procrequest.BootNow()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(budget))
}

// No Broker.Run business subscriptions are installed here. CONNECT must finish
// against the freshly installed local callout, before any business handler exists.
func exerciseAuthorityRebind(t *testing.T) {
	t.Helper()
	exerciseBrokerImageFactory(t)
	exerciseBrokerImageTunnelAuthority(t)
	b, nc, url := brokerImageAuthFixture(t, true, true)
	owner, err := startBrokerImageAuth(b, nc, brokerAuthorityDeadline(t, 3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, time.Second)) })
	if !owner.ProbeReply || owner.SubFlushBootNS <= 0 || owner.ReplyBootNS < owner.SubFlushBootNS || owner.UsableBootNS < owner.ReplyBootNS {
		t.Fatal("auth usable lacks ordered local SUB/JWT/CONNECT observations")
	}
	if nc.NumSubscriptions() != 1 {
		t.Fatalf("business subscription appeared before the auth-only barrier: %d", nc.NumSubscriptions())
	}
	// Hold the real SQL pool so an activated CONNECT is inside Handle. The
	// fence must time out while that user exists, rather than declaring joined.
	clientKP, clientPub := brokerImageUser(t)
	fp, err := auth.FingerprintFromActor(clientPub)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.cfg.DB.Exec(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab','owner','hash'); INSERT INTO members(sid,pubkey_fp,role,via,joined_at) VALUES('lab',?,'member','pin',CURRENT_TIMESTAMP)`, fp); err != nil {
		t.Fatal(err)
	}
	held, err := b.cfg.DB.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	waits := b.cfg.DB.Stats().WaitCount
	result := make(chan error, 1)
	go func() {
		client, err := nats.Connect(url, nats.Name("tether-cli:lab"), nats.Nkey(clientPub, clientKP.Sign), nats.Timeout(2*time.Second), nats.MaxReconnects(0), nats.Secure(nc.Opts.TLSConfig))
		if client != nil {
			client.Close()
		}
		result <- err
	}()
	defer func() {
		_ = held.Close()
		select {
		case <-result:
		case <-time.After(3 * time.Second):
			t.Error("CONNECT helper did not join")
		}
	}()
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return b.cfg.DB.Stats().WaitCount > waits }) {
		t.Fatal("CONNECT never borrowed the current SQL pool")
	}
	if err := stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, 40*time.Millisecond)); err == nil {
		t.Fatal("auth fence declared joined with a live SQL borrower")
	}
	select {
	case <-owner.Done:
		t.Fatal("auth signing key retired before its SQL borrower joined")
	default:
	}
	if owner.Enter() {
		owner.Leave()
		t.Fatal("fenced owner accepted a new borrower")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, 2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Key.PublicKey(); err == nil {
		t.Fatal("joined auth owner retained its signing key")
	}
	// The same current connection admits the next owner; a stale close cannot
	// clear another owner's verifier or remove its subscription.
	next, err := startBrokerImageAuth(b, nc, brokerAuthorityDeadline(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	nc.Close()
	if err := stopBrokerImageAuth(next, brokerAuthorityDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

func brokerImageUser(t *testing.T) (nkeys.KeyPair, string) {
	t.Helper()
	kp, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kp.Wipe)
	pub, err := kp.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	return kp, pub
}

func brokerImageAuthFixture(t *testing.T, callout bool, secure ...bool) (*Broker, *nats.Conn, string) {
	t.Helper()
	brokerKP, brokerPub := brokerImageUser(t)
	accountKP, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(accountKP.Wipe)
	brokerSeed, _ := brokerKP.Seed()
	accountSeed, _ := accountKP.Seed()
	accountPub, _ := accountKP.PublicKey()
	cfg := &AuthCalloutConfig{BrokerNkeySeed: append([]byte(nil), brokerSeed...), AccountSeed: append([]byte(nil), accountSeed...)}
	t.Cleanup(func() { clear(cfg.BrokerNkeySeed); clear(cfg.AccountSeed) })
	opts := natstest.DefaultTestOptions
	opts.Port, opts.AlwaysEnableNonce = -1, true
	opts.Nkeys = []*natsserver.NkeyUser{{Nkey: brokerPub}}
	var tlsOpts []nats.Option
	if len(secure) != 0 && secure[0] {
		cert, _ := registerAuthorityCertificate(t)
		opts.TLSConfig = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
		roots := x509.NewCertPool()
		roots.AddCert(cert.Leaf)
		tlsOpts = []nats.Option{nats.Secure(&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12})}
	}
	if callout {
		opts.AuthCallout = &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}
	} else {
		// The static negative control admits arbitrary users. A mere successful
		// CONNECT here cannot prove that the local callout signed a grant.
		opts.Nkeys = nil
	}
	ns := natstest.RunServer(&opts)
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(time.Second) {
		t.Fatal("auth fixture server did not start")
	}
	b := &Broker{cfg: Config{DB: openDB(t), NATSURL: ns.ClientURL(), Logger: silentLogger(), Now: time.Now, AuthCallout: cfg}}
	nc, err := nats.Connect(ns.ClientURL(), append(tlsOpts, nats.Nkey(brokerPub, brokerKP.Sign), nats.MaxReconnects(0))...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return b, nc, ns.ClientURL()
}

func exerciseBrokerStaticAuthRefusal(t *testing.T) {
	t.Helper()
	b, nc, _ := brokerImageAuthFixture(t, false)
	owner, err := startBrokerImageAuth(b, nc, brokerAuthorityDeadline(t, time.Second))
	if owner == nil || err == nil {
		t.Fatal("static/peer auth was accepted as a local callout readiness proof")
	}
	if err := stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

// Drop only real server PONG frames after a successful initial connection.
// This leaves SUB and AUTH traffic real while deterministically failing Flush.
func exerciseBrokerAuthFlushFailure(t *testing.T) {
	t.Helper()
	b, _, url := brokerImageAuthFixture(t, false)
	upstream, err := net.Dial("tcp", url[len("nats://"):])
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = upstream.Close() }()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	var block atomic.Bool
	var dropped atomic.Int32
	var workers sync.WaitGroup
	workers.Add(1)
	accepted := make(chan net.Conn, 1)
	go func() {
		defer workers.Done()
		downstream, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- downstream
		defer func() { _ = downstream.Close() }()
		workers.Add(1)
		go func() { defer workers.Done(); _, _ = io.Copy(upstream, downstream); _ = upstream.Close() }()
		reader := bufio.NewReader(upstream)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			if string(line) == "PONG\r\n" && block.Load() {
				dropped.Add(1)
				continue
			}
			if _, err := downstream.Write(line); err != nil {
				return
			}
		}
	}()
	var downstream net.Conn
	defer func() {
		_ = ln.Close()
		_ = upstream.Close()
		if downstream != nil {
			_ = downstream.Close()
		}
		workers.Wait()
	}()
	nc, err := nats.Connect("nats://"+ln.Addr().String(), nats.MaxReconnects(0))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	downstream = <-accepted
	block.Store(true)
	owner := newBrokerAuthOwner(b, brokerAuthorityDeadline(t, 80*time.Millisecond))
	sub, err := b.installAuthCallout(nc, owner)
	if sub != nil || err == nil || dropped.Load() == 0 {
		t.Fatalf("unchecked auth SUB flush: sub=%v err=%v dropped-pongs=%d", sub != nil, err, dropped.Load())
	}
	block.Store(false)
	if err := stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

func exerciseBrokerImagePINCommit(t *testing.T) {
	t.Helper()
	b, nc, url := brokerImageAuthFixture(t, true)
	dir := t.TempDir()
	addr, transport := raft.NewInmemTransport("image-pin")
	node, err := cluster.New(cluster.Config{LocalID: "image-pin", DataDir: dir, DBPath: filepath.Join(dir, "state.db"),
		Transport: transport, BootstrapPeers: []raft.Server{{ID: "image-pin", Address: addr, Suffrage: raft.Voter}}, Logger: silentLogger()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Shutdown() }()
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, node.IsLeader) {
		t.Fatal("PIN fixture has no real Raft leader")
	}
	hash, err := auth.HashPIN("image-pin-value")
	if err != nil {
		t.Fatal(err)
	}
	if err := node.Propose(func(_ *sql.DB) (*cluster.Command, error) {
		return cluster.NewCommand(cluster.OpSessionCreate, cluster.Stmt(`INSERT INTO sessions(sid,name,owner_pubkey_fp,pin_hash) VALUES('lab','lab','owner',`+cluster.MustLitText(hash)+`)`)), nil
	}); err != nil {
		t.Fatal(err)
	}
	before, err := node.AppliedIndex()
	if err != nil {
		t.Fatal(err)
	}
	b.clusterMode = true
	oldForwarder := NewForwarder(nil, time.Second)
	b.cl = &clusterRuntime{node: node, forwarder: oldForwarder}
	apply, err := rebindBrokerImageAuthority(b, nc, brokerAuthorityDeadline(t, 3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stopBrokerImageAuth(apply, brokerAuthorityDeadline(t, time.Second)) }()
	if b.cfg.DB != node.RODB() || b.cl.forwarder.nc != nc || b.cl.forwarder == oldForwarder {
		t.Fatal("image authority retained old DB/forwarder")
	}
	owner, err := startBrokerImageAuth(b, nc, brokerAuthorityDeadline(t, 3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, time.Second)) }()
	kp, pub := brokerImageUser(t)
	client, err := nats.Connect(url, nats.Name("tether-cli:lab"), nats.Nkey(pub, kp.Sign), nats.Token("image-pin-value"), nats.MaxReconnects(0), nats.Timeout(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	client.Close()
	fp, err := auth.FingerprintFromActor(pub)
	if err != nil {
		t.Fatal(err)
	}
	var count int
	if err := node.RODB().QueryRow(`SELECT COUNT(*) FROM members WHERE sid='lab' AND pubkey_fp=?`, fp).Scan(&count); err != nil || count != 1 {
		t.Fatalf("CONNECT did not commit PIN membership: count=%d err=%v", count, err)
	}
	after, err := node.AppliedIndex()
	if err != nil || after <= before {
		t.Fatalf("PIN bypassed real Raft command commit: before=%d after=%d err=%v", before, after, err)
	}
	borrowedVerifier := b.leaderPINVerify.Load()
	if borrowedVerifier == nil {
		t.Fatal("budgeted current leader PIN verifier not published")
	}
	if err := stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := (*borrowedVerifier)("image-pin-value", hash); !errors.Is(err, errPINVerifierUnwired) {
		t.Fatalf("copied old PIN verifier escaped its image fence: %v", err)
	}
	if b.leaderPINVerify.Load() != nil {
		t.Fatal("retired owner left the current PIN verifier published")
	}
	next, err := startBrokerImageAuth(b, nc, brokerAuthorityDeadline(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	currentVerifier := b.leaderPINVerify.Load()
	if err := stopBrokerImageAuth(owner, brokerAuthorityDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if currentVerifier == nil || b.leaderPINVerify.Load() != currentVerifier {
		t.Fatal("stale auth owner cleared its successor's verifier")
	}
	if err := stopBrokerImageAuth(next, brokerAuthorityDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	// Block a real forwarded non-PIN command inside the FSM writer. Joining
	// only auth/verification would miss this independent store borrower.
	held, err := node.DB().Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = held.Close() }()
	waits := node.DB().Stats().WaitCount
	payload, err := json.Marshal(BusNkeySetPayload{NodeID: "image-pin", BusNkeyPub: pub})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- b.cl.forwarder.Forward(VerbBusNkeySet, "", payload) }()
	defer func() {
		_ = held.Close()
		select {
		case <-result:
		case <-time.After(5 * time.Second):
			t.Error("forwarded Plan helper did not join")
		}
	}()
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return node.DB().Stats().WaitCount > waits }) {
		t.Fatal("forwarded command never entered the current FSM writer")
	}
	if err := stopBrokerImageAuth(apply, brokerAuthorityDeadline(t, 40*time.Millisecond)); err == nil {
		t.Fatal("apply responder declared joined with a live FSM writer")
	}
	if err := held.Close(); err != nil {
		t.Fatal(err)
	}
	if err := stopBrokerImageAuth(apply, brokerAuthorityDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// Real v1 REGISTER and an original TCP stream make reconciliation nonvacuous.
// A transient current-store failure blocks activation and keeps the raw socket;
// current revocation closes that allocation instead of trusting the old image.
// origin: hot-update S6 plan §6.3 / T28
func exerciseBrokerImageTunnelAuthority(t *testing.T) {
	t.Helper()
	publicPort := flowReaperFreePort(t)
	b := &Broker{cfg: Config{DB: openDB(t), Logger: silentLogger()}, selfID: "home"}
	seedHomedExpose(t, b, "lab", "node", "__proxy__", publicPort, port.HashToken("image-tunnel-token"), "home", 4)
	if _, err := b.cfg.DB.Exec(`UPDATE sessions SET proxy_enabled=1; UPDATE nodes SET proxy_capable=1;
		INSERT INTO proxy_subscribers(sid,sub_id,name,token_hash,psk,cipher,created_by_fp) VALUES('lab','alice','alice','hash','password','aes-256-gcm','owner')`); err != nil {
		t.Fatal(err)
	}
	addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(flowReaperFreePort(t)))
	cert, pin := registerAuthorityCertificate(t)
	srv := tunnel.NewServerWithCert(addr, "127.0.0.1", b.tunnelTokenLookup, &cert, silentLogger())
	if err := configureTunnelAuthority(b, srv); err != nil {
		t.Fatal(err)
	}
	if err := srv.Start(t.Context()); err != nil {
		srv.Close()
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	target, accepts := brokerImageEchoTarget(t)
	_, targetPortString, err := net.SplitHostPort(target)
	if err != nil {
		t.Fatal(err)
	}
	targetPort, err := strconv.Atoi(targetPortString)
	if err != nil {
		t.Fatal(err)
	}
	client := tunnel.NewClient(addr, "lab", "node", func(int) (int, error) { return targetPort, nil }, silentLogger())
	if err := client.SetFlowConfig(tunnel.FlowConfig{Budget: &workload.ReplayBudget{}, InstanceID: "image-agent", BootID: "agent-boot", OwnerGen: 1}); err != nil {
		t.Fatal(err)
	}
	client.Start(t.Context())
	t.Cleanup(func() { client.Close(publicPort) })
	if err := client.OpenHome(t.Context(), publicPort, targetPort, "image-tunnel-token", addr, 4, proto.CertPins{Current: pin}); err != nil {
		t.Fatal(err)
	}
	public, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(publicPort)), tunnel.ResumeAttempt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = public.Close() })
	flowReaperExchange(t, public, []byte("original-before-current-authority"))
	if accepts.Load() != 1 || len(srv.SessionInfos()) != 1 {
		t.Fatal("image reconciliation fixture lacks an original socket/Endpoint")
	}
	if n, err := reconcileBrokerImageTunnel(b, srv); n != 0 || err != nil {
		t.Fatalf("current positive authority closed the original allocation: %d/%v", n, err)
	}
	if _, err := b.cfg.DB.Exec(`ALTER TABLE proxy_subscribers RENAME TO image_unavailable_keys`); err != nil {
		t.Fatal(err)
	}
	if n, err := reconcileBrokerImageTunnel(b, srv); n != 0 || !errors.Is(err, errTunnelAuthorityRetry) {
		t.Fatalf("transient authority must block activation without closing raw: %d/%v", n, err)
	}
	if _, err := b.cfg.DB.Exec(`ALTER TABLE image_unavailable_keys RENAME TO proxy_subscribers`); err != nil {
		t.Fatal(err)
	}
	flowReaperExchange(t, public, []byte("original-after-transient-authority"))
	if accepts.Load() != 1 {
		t.Fatal("authority recheck redialed the original business socket")
	}
	if _, err := b.cfg.DB.Exec(`UPDATE sessions SET proxy_enabled=0`); err != nil {
		t.Fatal(err)
	}
	if n, err := reconcileBrokerImageTunnel(b, srv); n != 1 || err != nil {
		t.Fatalf("current revocation did not close the imported allocation: %d/%v", n, err)
	}
	if len(srv.SessionInfos()) != 0 {
		t.Fatal("revoked Endpoint survived image revalidation")
	}
	if err := public.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := public.Write([]byte("must-not-forward")); err == nil {
		var one [1]byte
		if _, err := public.Read(one[:]); err == nil {
			t.Fatal("revoked original flow still delivered bytes")
		}
	}
}

// Hide the TCP ReaderFrom/WriterTo fast paths in this fixture. Linux splice
// caches pipe pairs in a process-wide sync.Pool, obscuring an owner-level
// zero-FD-delta oracle despite complete socket cleanup.
func brokerImageEchoTarget(t *testing.T) (string, *atomic.Int64) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepts := &atomic.Int64{}
	var mu sync.Mutex
	var conns []net.Conn
	var workers sync.WaitGroup
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepts.Add(1)
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				_, _ = io.Copy(struct{ io.Writer }{conn}, struct{ io.Reader }{conn})
				_ = conn.Close()
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-done
		mu.Lock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return ln.Addr().String(), accepts
}

// Missing/expired current dependencies are entry refusals, never usable auth.
// origin: hot-update S6 plan §8.1 / T29
func exerciseBrokerAuthEntryRefusal(t *testing.T) {
	t.Helper()
	// The extracted barrier keeps the original nil-broker idle constructor.
	// origin: hot-update S6 plan §8.1 / T29 / BROKER-SLIM
	idle := newBrokerAuthOwner(nil, 0)
	if !idle.Enter() {
		t.Fatal("unbound cold barrier refused its first borrower")
	}
	idle.Leave()
	if err := stopBrokerImageAuth(idle, 0); err != nil {
		t.Fatal(err)
	}
	b, nc, _ := brokerImageAuthFixture(t, true)
	d := brokerAuthorityDeadline(t, time.Second)
	for _, request := range []struct {
		broker   *Broker
		conn     *nats.Conn
		deadline controlbudget.Deadline
	}{
		{nil, nc, d}, {b, nil, d}, {b, nc, 0},
		{&Broker{cfg: Config{AuthCallout: b.cfg.AuthCallout}}, nc, d},
		{&Broker{cfg: Config{DB: b.cfg.DB}}, nc, d},
	} {
		if owner, err := startBrokerImageAuth(request.broker, request.conn, request.deadline); owner != nil || err == nil {
			t.Fatal("missing auth current dependency admitted")
		}
	}
	if owner, err := startBrokerImageAuth(b, nc, 1); owner != nil || err == nil {
		t.Fatal("expired image auth deadline admitted")
	}
	for _, request := range []struct {
		broker   *Broker
		conn     *nats.Conn
		deadline controlbudget.Deadline
	}{
		{nil, nc, d}, {b, nil, d}, {b, nc, 0}, {b, nc, 1},
		{&Broker{}, nc, d}, {&Broker{clusterMode: true}, nc, d},
	} {
		if owner, err := rebindBrokerImageAuthority(request.broker, request.conn, request.deadline); owner != nil || err == nil {
			t.Fatal("missing/expired current authority admitted")
		}
	}
	b.clusterMode = true
	b.cl = &clusterRuntime{}
	if sub, err := b.installAuthCallout(nc); sub != nil || err == nil {
		t.Fatal("cluster auth admitted unwired current PIN/forwarder/store dependencies")
	}
	b.clusterMode = false
	if n, err := reconcileBrokerImageTunnel(b, nil); n != 0 || err == nil {
		t.Fatal("nil imported server treated as reconciled authority")
	}
}

// Hot control activation asks both stale agents for real liveness. Sending
// requests must not fabricate ONLINE or renew any retained heartbeat timestamp.
// origin: hot-update S6 plan §8.1 / T50 / EXITS-2-3
func TestImageBusinessWakesEveryAgentWithoutInventingLiveness(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseImageAgentWake(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "image agent heartbeat wake x5", beforeG)
	testharness.AssertNoFDLeak(t, "image agent heartbeat wake x5", beforeFD, 0)
}
func exerciseImageAgentWake(t *testing.T) {
	t.Helper()
	b, nc, _ := brokerImageAuthFixture(t, false)
	if _, err := session.Create(b.cfg.DB, "wake", "wake", "owner", "pin", time.Now()); err != nil {
		t.Fatal(err)
	}
	for _, nid := range []string{"one", "two"} {
		if err := node.Register(b.cfg.DB, node.RegisterInput{SID: "wake", NID: nid, ProtoVersion: proto.ProtoVersion}, time.Now().Add(-time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.cfg.DB.Exec(`UPDATE nodes SET status='STALE' WHERE sid='wake'`); err != nil {
		t.Fatal(err)
	}
	requests, err := nc.SubscribeSync(proto.SubjCmdForwarded("wake", "*", proto.ClaimProbeVerb))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := requests.Unsubscribe(); err != nil {
			t.Errorf("unsubscribe: %v", err)
		}
	}()
	if err = nc.Flush(); err != nil {
		t.Fatal(err)
	}
	if err = wakeBrokerImageAgents(b, nc); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		msg, err := requests.NextMsg(time.Second)
		if err != nil {
			t.Fatal("current business did not wake every agent", err)
		}
		if msg.Reply == "" || len(msg.Data) != 0 {
			t.Fatal("image wake did not preserve claim probe wire")
		}
		seen[msg.Subject] = true
	}
	for _, nid := range []string{"one", "two"} {
		if !seen[proto.SubjCmdForwarded("wake", nid, proto.ClaimProbeVerb)] {
			t.Fatal("image wake duplicated one agent")
		}
		var status string
		if err = b.cfg.DB.QueryRow(`SELECT status FROM nodes WHERE sid='wake' AND nid=?`, nid).Scan(&status); err != nil || status != "STALE" {
			t.Fatal("image wake synthesized liveness without an agent heartbeat", err)
		}
	}
}
