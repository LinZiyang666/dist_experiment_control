//go:build linux

// broker_auth_test.go observes the real NATS callout and Run readiness. It uses
// shared ephemeral store setup; the closed-store/exec fixture belongs to P26.
// origin: hot-update S6 plan §8.1 / T29 / §12.1 P27
package hotupdate_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

// A static observer sees the real server-issued AUTH request and the actual
// response. It never responds or mints a grant: a successful static CONNECT is
// explicitly excluded from proof of callout readiness.
type brokerAuthWireWitness struct {
	mu        sync.Mutex
	requests  map[string]*jwt.AuthorizationRequestClaims
	responses map[string]*jwt.AuthorizationResponseClaims
}

func (w *brokerAuthWireWitness) observe(msg *nats.Msg) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if msg.Subject == "$SYS.REQ.USER.AUTH" {
		if request, err := jwt.DecodeAuthorizationRequestClaims(string(msg.Data)); err == nil {
			w.requests[request.UserNkey] = request
		}
		return
	}
	if response, err := jwt.DecodeAuthorizationResponseClaims(string(msg.Data)); err == nil {
		w.responses[response.Subject] = response
	}
}

func (w *brokerAuthWireWitness) validated(pub, account, serverID string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	for user, request := range w.requests {
		if request.ConnectOptions.Nkey != pub || request.Server.ID != serverID {
			continue
		}
		response := w.responses[user]
		if response == nil || response.Error != "" || response.Issuer != account || response.Subject != request.UserNkey || response.Audience != serverID {
			continue
		}
		grant, err := jwt.DecodeUserClaims(response.Jwt)
		if err == nil && grant.Subject == user && grant.Issuer == account {
			return true
		}
	}
	return false
}

// Hold the actual SQL pool during startup and observe readiness independently.
// After release, fresh unactivated and activated users must both traverse the
// callout; ACL enforcement and the business response are checked separately.
// origin: hot-update S6 plan §8.1 / T29
func TestBrokerAuthFirstReadyBarrier(t *testing.T) {
	db := testharness.OpenDB(t)
	user, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer user.Wipe()
	userPub, err := user.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := auth.FingerprintFromActor(userPub)
	if err != nil {
		t.Fatal(err)
	}
	pinHash, err := auth.HashPIN("image-ready-pin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "ready-lab", "ready-lab", fingerprint, pinHash, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	brokerKey, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer brokerKey.Wipe()
	brokerPub, err := brokerKey.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer account.Wipe()
	accountPub, err := account.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	brokerSeed, err := brokerKey.Seed()
	if err != nil {
		t.Fatal(err)
	}
	accountSeed, err := account.Seed()
	if err != nil {
		t.Fatal(err)
	}
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.JetStream, opts.StoreDir = true, t.TempDir()
	opts.Nkeys = []*natsserver.NkeyUser{{Nkey: brokerPub}}
	opts.AuthCallout = &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}
	ns := natstest.RunServer(&opts)
	defer func() { ns.Shutdown(); ns.WaitForShutdown() }()
	monitor, err := nats.Connect(ns.ClientURL(), nats.Name("auth-wire-observer"), nats.Nkey(brokerPub, brokerKey.Sign), nats.MaxReconnects(0))
	if err != nil {
		t.Fatal(err)
	}
	defer monitor.Close()
	witness := &brokerAuthWireWitness{requests: make(map[string]*jwt.AuthorizationRequestClaims), responses: make(map[string]*jwt.AuthorizationResponseClaims)}
	if _, err := monitor.Subscribe("$SYS.>", witness.observe); err != nil {
		t.Fatal(err)
	}
	if err := monitor.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	b, err := broker.New(broker.Config{NATSURL: ns.ClientURL(), DB: db, Logger: testharness.SilentLog(), ReadyCh: ready, AuthCallout: &broker.AuthCalloutConfig{BrokerNkeySeed: append([]byte(nil), brokerSeed...), AccountSeed: append([]byte(nil), accountSeed...)}})
	if err != nil {
		t.Fatal(err)
	}
	borrowed, err := db.Conn(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = borrowed.Close() }()
	waits := db.Stats().WaitCount
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	defer func() {
		_ = borrowed.Close()
		cancel()
		select {
		case err := <-done:
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("normal broker stop: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("broker Run did not join")
		}
	}()
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool { return db.Stats().WaitCount > waits }) {
		t.Fatal("startup never borrowed the current SQL pool")
	}
	select {
	case <-ready:
		t.Fatal("READY was published while current-store startup work was blocked")
	default:
	}
	if err := borrowed.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ready:
	case <-time.After(10 * time.Second):
		t.Fatal("full Ready did not follow store/JS/late wiring")
	}
	unactivated, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer unactivated.Wipe()
	probePub, err := unactivated.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	probe, err := nats.Connect(ns.ClientURL(), nats.Name(cli.CtlNameUnactivated), nats.Nkey(probePub, unactivated.Sign), nats.Timeout(time.Second), nats.MaxReconnects(0))
	if err != nil {
		t.Fatalf("fresh callout CONNECT at Ready: %v", err)
	}
	defer probe.Close()
	active, err := nats.Connect(ns.ClientURL(), nats.Name(cli.CtlNameForSession("ready-lab")), nats.Nkey(userPub, user.Sign), nats.Timeout(time.Second), nats.MaxReconnects(0))
	if err != nil {
		t.Fatalf("activated CONNECT at Ready: %v", err)
	}
	defer active.Close()
	if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
		return witness.validated(probePub, accountPub, ns.ID()) && witness.validated(userPub, accountPub, ns.ID())
	}) {
		t.Fatal("static auth cannot substitute for two verified local callout JWT/CONNECTs")
	}
	body, _ := json.Marshal(proto.NodeListReq{})
	message, err := active.Request(proto.SubjCtrlNodeList(userPub, "ready-lab"), body, time.Second)
	if err != nil {
		t.Fatalf("business request at full Ready: %v", err)
	}
	var response proto.NodeListResp
	if err := json.Unmarshal(message.Data, &response); err != nil || response.Code != "" {
		t.Fatalf("business response at Ready: %v code=%s", err, response.Code)
	}
	denied, err := probe.Request(proto.SubjCtrlNodeList(probePub, "ready-lab"), body, 100*time.Millisecond)
	if err == nil {
		t.Fatalf("unactivated CONNECT received business authority: %s", denied.Data)
	}
	// JS identity is a real server witness, not the presence of a log line.
	js, err := monitor.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := js.AccountInfo(); err != nil {
		t.Fatalf("full Ready preceded usable JetStream: %v", err)
	}
}

// A positive wire witness and acceptance-only/static negatives guard the oracle
// independently of startup. Claims are still signed and decoded by nats jwt.
// origin: hot-update S6 plan §8.1 / T29
func TestBrokerAuthWitnessRejectsStaticConnect(t *testing.T) {
	server, err := nkeys.CreateServer()
	if err != nil {
		t.Fatal(err)
	}
	defer server.Wipe()
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer account.Wipe()
	user, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer user.Wipe()
	serverPub, _ := server.PublicKey()
	accountPub, _ := account.PublicKey()
	userPub, _ := user.PublicKey()
	request := jwt.NewAuthorizationRequestClaims(userPub)
	request.UserNkey = userPub
	request.Server.ID, request.ConnectOptions.Nkey = serverPub, userPub
	requestJWT, err := request.Encode(server)
	if err != nil {
		t.Fatal(err)
	}
	grant := jwt.NewUserClaims(userPub)
	grantJWT, err := grant.Encode(account)
	if err != nil {
		t.Fatal(err)
	}
	response := jwt.NewAuthorizationResponseClaims(userPub)
	response.Audience, response.Jwt = serverPub, grantJWT
	responseJWT, err := response.Encode(account)
	if err != nil {
		t.Fatal(err)
	}
	newWitness := func() *brokerAuthWireWitness {
		return &brokerAuthWireWitness{requests: make(map[string]*jwt.AuthorizationRequestClaims), responses: make(map[string]*jwt.AuthorizationResponseClaims)}
	}
	w := newWitness()
	w.observe(&nats.Msg{Subject: "$SYS.REQ.USER.AUTH", Data: []byte(requestJWT)})
	w.observe(&nats.Msg{Subject: "$SYS.AUTH.REPLY", Data: []byte(responseJWT)})
	if !w.validated(userPub, accountPub, serverPub) {
		t.Fatalf("signed JWT positive control failed: requests=%d responses=%d", len(w.requests), len(w.responses))
	}
	for label, bad := range map[string]*brokerAuthWireWitness{"static-CONNECT": newWitness(), "request-only": newWitness(), "response-only": newWitness()} {
		if strings.Contains(label, "request") {
			bad.observe(&nats.Msg{Subject: "$SYS.REQ.USER.AUTH", Data: []byte(requestJWT)})
		}
		if strings.Contains(label, "response") {
			bad.observe(&nats.Msg{Subject: "$SYS.AUTH.REPLY", Data: []byte(responseJWT)})
		}
		if bad.validated(userPub, accountPub, serverPub) {
			t.Fatal(fmt.Sprint(label, " replaced the local callout witness"))
		}
	}
	if w.validated(userPub, accountPub, "peer-server") {
		t.Fatal("peer identity substituted for local callout")
	}
}

// Drop the server's real PONG only after the auth SUB has passed through the
// relay. Initial CONNECT/private-inbox probes still exchange their actual bytes.
// A buffered SUB cannot be treated as the checked auth-first barrier.
// origin: hot-update S6 plan §8.1 / T29
func TestBrokerAuthFlushRequiresServerAcknowledgement(t *testing.T) {
	brokerKey, err := nkeys.CreateUser()
	if err != nil {
		t.Fatal(err)
	}
	defer brokerKey.Wipe()
	brokerPub, _ := brokerKey.PublicKey()
	brokerSeed, _ := brokerKey.Seed()
	accountKey, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer accountKey.Wipe()
	accountPub, _ := accountKey.PublicKey()
	accountSeed, _ := accountKey.Seed()
	opts := natstest.DefaultTestOptions
	opts.Port = -1
	opts.Nkeys = []*natsserver.NkeyUser{{Nkey: brokerPub}}
	opts.AuthCallout = &natsserver.AuthCallout{Issuer: accountPub, Account: "$G", AuthUsers: []string{brokerPub}}
	ns := natstest.RunServer(&opts)
	defer func() { ns.Shutdown(); ns.WaitForShutdown() }()
	url, sawSUB, dropped := brokerAuthFlushRelay(t, ns.ClientURL())
	ready := make(chan struct{})
	b, err := broker.New(broker.Config{NATSURL: url, DB: testharness.OpenDB(t), Logger: testharness.SilentLog(), ReadyCh: ready, AuthCallout: &broker.AuthCalloutConfig{BrokerNkeySeed: append([]byte(nil), brokerSeed...), AccountSeed: append([]byte(nil), accountSeed...)}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx) }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("failed auth startup did not join")
			}
		}
	}()
	select {
	case <-sawSUB:
	case <-time.After(5 * time.Second):
		t.Fatal("relay never saw the actual callout SUB")
	}
	select {
	case <-ready:
		t.Fatal("unchecked auth Flush published READY")
	case err := <-done:
		joined = true
		if err == nil || !strings.Contains(err.Error(), "auth subscription flush") {
			t.Fatalf("auth Flush failure was not returned: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("auth Flush failure did not stop startup")
	}
	if dropped.Load() == 0 {
		t.Fatal("PONG loss premise was empty")
	}
}

func brokerAuthFlushRelay(t *testing.T, upstream string) (string, <-chan struct{}, *atomic.Int64) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var authSUB atomic.Bool
	var dropped atomic.Int64
	sawSUB := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	connections := make(map[net.Conn]struct{})
	var workers sync.WaitGroup
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.DialTimeout("tcp", strings.TrimPrefix(upstream, "nats://"), time.Second)
			if err != nil {
				_ = client.Close()
				continue
			}
			mu.Lock()
			connections[client], connections[server] = struct{}{}, struct{}{}
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer func() {
					_ = client.Close()
					_ = server.Close()
					mu.Lock()
					delete(connections, client)
					delete(connections, server)
					mu.Unlock()
				}()
				done := make(chan struct{}, 2)
				go func() {
					defer func() { _ = server.Close(); done <- struct{}{} }()
					reader := bufio.NewReader(client)
					for {
						line, err := reader.ReadString('\n')
						if err != nil {
							return
						}
						if strings.HasPrefix(line, "SUB $SYS.REQ.USER.AUTH ") {
							authSUB.Store(true)
							once.Do(func() { close(sawSUB) })
						}
						if _, err := io.WriteString(server, line); err != nil {
							return
						}
					}
				}()
				go func() {
					defer func() { _ = client.Close(); done <- struct{}{} }()
					reader := bufio.NewReader(server)
					for {
						line, err := reader.ReadString('\n')
						if err != nil {
							return
						}
						if line == "PONG\r\n" && authSUB.Load() {
							dropped.Add(1)
							continue
						}
						if _, err := io.WriteString(client, line); err != nil {
							return
						}
					}
				}()
				<-done
				_ = client.Close()
				_ = server.Close()
				<-done
			}()
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-accepted
		mu.Lock()
		for c := range connections {
			_ = c.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return "nats://" + listener.Addr().String(), sawSUB, &dropped
}
