package infraauth

import (
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func openReady(t *testing.T, f *authFixture) (*Owner, Receipt) {
	t.Helper()
	owner := f.newOwner()
	receipt, err := owner.Open(testDeadline(t, 3*time.Second), nil)
	if err != nil {
		t.Fatal(err)
	}
	requireOrder(t, f, receipt, 0)
	return owner, receipt
}

func removeNormalQueue(t *testing.T, owner *Owner) *generation {
	t.Helper()
	owner.mu.Lock()
	g := owner.current
	owner.mu.Unlock()
	if err := g.normalQueue.Unsubscribe(); err != nil {
		t.Fatal(err)
	}
	if err := g.auth.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	return g
}

func dialOrdinary(t *testing.T, f *authFixture, endpoint string, identity Identity) (*nats.Conn, error) {
	t.Helper()
	return nats.Connect(endpoint, nats.Nkey(identity.PublicKey, identity.Sign), nats.Name(identity.Name), nats.UserInfo(auth.InboxCapableMarker, ""), nats.CustomInboxPrefix(auth.InboxPrefixFor(identity.PublicKey)), nats.NoReconnect(), nats.Timeout(time.Second), nats.IgnoreDiscoveredServers())
}

// origin: hot-update S9 plan §4.4 / T39
func TestOrdinaryAuthProbeNeedsNormalQueue(t *testing.T) {
	t.Run("normal_queue_has_both_ordinary_identities", func(t *testing.T) {
		f := newAuthFixture(t)
		_, receipt := openReady(t, f)
		if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool {
			clients, err := f.server.Connz(&natsserver.ConnzOptions{})
			return err == nil && len(clients.Conns) == 2
		}) {
			t.Fatal("ordinary probe sockets were retained after readiness")
		}
		clients, err := f.server.Connz(&natsserver.ConnzOptions{Subscriptions: true, SubscriptionsDetail: true})
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, client := range clients.Conns {
			if client.Cid != receipt.AuthCID {
				continue
			}
			for _, sub := range client.SubsDetail {
				if sub.Subject == authSubject {
					if sub.Queue != authQueue {
						t.Fatal("temporary nonqueue responder signed normal readiness")
					}
					found = true
				}
			}
		}
		if !found {
			t.Fatal("normal auth queue absent from actual server census")
		}
	})
	t.Run("static_connect_cannot_replace_callout", func(t *testing.T) {
		f := newAuthFixture(t)
		opts := natstest.DefaultTestOptions
		opts.Host, opts.Port, opts.AlwaysEnableNonce = "127.0.0.1", -1, true
		anonymous := natstest.RunServer(&opts)
		defer func() { anonymous.Shutdown(); anonymous.WaitForShutdown() }()
		f.config.URL = anonymous.ClientURL()
		owner := f.newOwner()
		receipt, err := owner.Open(testDeadline(t, 300*time.Millisecond), nil)
		if err == nil || receipt.AuthUsable != 0 || receipt.ControlReady != 0 {
			t.Fatal("static CONNECT bypassed the real normal queue proof")
		}
		requireNoBusiness(t, f, owner)
	})
	t.Run("removed_queue_cannot_authenticate", func(t *testing.T) {
		f := newAuthFixture(t)
		owner, _ := openReady(t, f)
		removeNormalQueue(t, owner)
		for _, identity := range []Identity{f.config.Member, f.config.Agent} {
			if nc, err := dialOrdinary(t, f, f.config.URL, identity); err == nil {
				nc.Close()
				t.Fatal("deleted normal queue still authenticated an ordinary identity")
			}
		}
	})
	t.Run("auxiliary_observer_cannot_sign_owner", func(t *testing.T) {
		f := newAuthFixture(t)
		owner, _ := openReady(t, f)
		g := removeNormalQueue(t, owner)
		// This is the adversarial S6-style observer, deliberately outside the
		// normal queue. It really authenticates the user, but must not prove the
		// normal owner. No production probe installs this subscription.
		aux, err := g.auth.Subscribe(authSubject, func(msg *nats.Msg) {
			response, err := f.config.HandleAuth(string(msg.Data))
			if err == nil {
				_ = msg.Respond([]byte(response))
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = aux.Unsubscribe() }()
		if err := g.auth.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		nc, err := dialOrdinary(t, f, f.config.URL, f.config.Member)
		if err != nil {
			t.Fatal("auxiliary negative control was not live")
		}
		nc.Close()
		if nc, err := owner.authenticate(g, f.config.Member, 0, testDeadline(t, time.Second)); err == nil {
			nc.Close()
			t.Fatal("auxiliary response replaced local normal queue proof")
		}
	})
	t.Run("bad_issuer_and_acl", func(t *testing.T) {
		for _, defect := range []string{"issuer", "private_inbox", "member_rpc", "agent_rpc"} {
			t.Run(defect, func(t *testing.T) {
				f := newAuthFixture(t)
				badKey, err := nkeys.CreateAccount()
				if err != nil {
					t.Fatal(err)
				}
				defer badKey.Wipe()
				handle := f.config.HandleAuth
				f.config.HandleAuth = func(request string) (string, error) {
					response, err := handle(request)
					if err != nil {
						return "", err
					}
					claims, err := jwt.DecodeAuthorizationResponseClaims(response)
					if err != nil {
						return "", err
					}
					if defect == "issuer" {
						return claims.Encode(badKey)
					}
					user, err := jwt.DecodeUserClaims(claims.Jwt)
					if err != nil {
						return "", err
					}
					switch defect {
					case "private_inbox":
						user.Sub = jwt.Permission{Deny: []string{auth.InboxRoot + ".>"}}
					case "member_rpc":
						user.Pub = jwt.Permission{Deny: []string{f.config.Member.RPCSubject}}
					case "agent_rpc":
						user.Pub = jwt.Permission{Deny: []string{f.config.Agent.RPCSubject}}
					default:
						return "", ErrBinding
					}
					claims.Jwt, err = user.Encode(f.keys[4])
					if err != nil {
						return "", err
					}
					return claims.Encode(f.keys[4])
				}
				owner := f.newOwner()
				receipt, err := owner.Open(testDeadline(t, 400*time.Millisecond), nil)
				if err == nil || receipt.Phase == ControlReady || receipt.ControlReady != 0 {
					t.Fatal("bad ordinary issuer/ACL reached control-ready")
				}
				if defect == "issuer" {
					requireNoBusiness(t, f, owner)
				}
			})
		}
	})
	t.Run("peer_queue_cannot_cover_local_omission", func(t *testing.T) {
		f := newAuthFixture(t)
		owner, _ := openReady(t, f)
		g := removeNormalQueue(t, owner)
		// Create a real routed peer using the SAME issuer and DB handler, so
		// CONNECT to the local server can succeed through the healthy peer.
		peerOpts := natstest.DefaultTestOptions
		peerOpts.Host, peerOpts.Port, peerOpts.AlwaysEnableNonce = "127.0.0.1", -1, true
		peerOpts.Cluster = natsserver.ClusterOpts{Host: "127.0.0.1", Port: -1, Name: "authgate"}
		route, err := url.Parse("nats://" + f.server.ClusterAddr().String())
		if err != nil {
			t.Fatal(err)
		}
		peerOpts.Routes = []*url.URL{route}
		brokerPublic, _ := f.keys[0].PublicKey()
		peerOpts.Nkeys = []*natsserver.NkeyUser{{Nkey: brokerPublic}}
		peerOpts.AuthCallout = &natsserver.AuthCallout{Issuer: f.config.AccountPublicKey, Account: "$G", AuthUsers: []string{brokerPublic}}
		peer := natstest.RunServer(&peerOpts)
		defer func() { peer.Shutdown(); peer.WaitForShutdown() }()
		if !testharness.WaitFor(t, 3*time.Second, time.Millisecond, func() bool { return peer.NumRoutes() > 0 && f.server.NumRoutes() > 0 }) {
			t.Fatal("peer route never became live")
		}
		peerNC, err := nats.Connect(peer.ClientURL(), nats.Nkey(brokerPublic, f.keys[0].Sign), nats.NoReconnect())
		if err != nil {
			t.Fatal(err)
		}
		defer peerNC.Close()
		var peerReplies atomic.Int32
		if _, err := peerNC.QueueSubscribe(authSubject, authQueue, func(msg *nats.Msg) {
			response, err := f.config.HandleAuth(string(msg.Data))
			if err == nil && msg.Respond([]byte(response)) == nil {
				peerReplies.Add(1)
			}
		}); err != nil {
			t.Fatal(err)
		}
		if err := peerNC.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		if !testharness.WaitFor(t, 3*time.Second, 10*time.Millisecond, func() bool {
			nc, err := dialOrdinary(t, f, f.config.URL, f.config.Member)
			if err != nil {
				return false
			}
			nc.Close()
			return true
		}) {
			t.Fatal("healthy peer did not really authenticate the local CONNECT")
		}
		if peerReplies.Load() == 0 {
			t.Fatal("peer negative control produced no real reply")
		}
		if nc, err := owner.authenticate(g, f.config.Member, 0, testDeadline(t, time.Second)); err == nil {
			nc.Close()
			t.Fatal("peer queue covered the missing local normal queue")
		}
		// Once the route is actually gone, even raw ordinary CONNECT fails.
		peer.Shutdown()
		peer.WaitForShutdown()
		if !testharness.WaitFor(t, time.Second, time.Millisecond, func() bool { return f.server.NumRoutes() == 0 }) {
			t.Fatal("peer route did not retire")
		}
		if nc, err := dialOrdinary(t, f, f.config.URL, f.config.Agent); err == nil {
			nc.Close()
			t.Fatal("ordinary auth survived without any normal queue")
		}
	})
	t.Run("broker_never_roams_to_configured_peer", func(t *testing.T) {
		f := newAuthFixture(t)
		peer := newAuthFixture(t)
		// A supplied peer plus reconnect options must be overridden as well as
		// INFO discovery. The peer is healthy; only the configured local server
		// is shut down, so a roaming mutation visibly CONNECTs to the peer.
		options := append(f.config.Options(), func(opts *nats.Options) error {
			opts.Servers = []string{peer.config.URL}
			opts.Url = peer.config.URL
			opts.AllowReconnect = true
			opts.IgnoreDiscoveredServers = false
			return nil
		})
		f.config.Options = func() []nats.Option { return options }
		f.server.Shutdown()
		f.server.WaitForShutdown()
		owner := f.newOwner()
		receipt, err := owner.Open(testDeadline(t, 500*time.Millisecond), nil)
		if err == nil || receipt.Phase == ControlReady {
			t.Fatal("local broker roamed to healthy peer")
		}
		for _, event := range f.wire() {
			if event.Kind == "CONNECT" {
				t.Fatal("local broker sent CONNECT after roaming to peer")
			}
		}
	})
}

// startAuthPeers routes n healthy peer servers to f's local server. Each peer
// broker answers callouts on the SAME normal queue with handle, counting (before
// it responds) its answers to the local server's ordinary probe requests.
func startAuthPeers(t *testing.T, f *authFixture, n int, handle func(string) (string, error)) *atomic.Int32 {
	t.Helper()
	brokerPublic, _ := f.keys[0].PublicKey()
	route, err := url.Parse("nats://" + f.server.ClusterAddr().String())
	if err != nil {
		t.Fatal(err)
	}
	answered := new(atomic.Int32)
	for range n {
		opts := natstest.DefaultTestOptions
		opts.Host, opts.Port, opts.AlwaysEnableNonce, opts.Routes = "127.0.0.1", -1, true, []*url.URL{route}
		opts.Cluster = natsserver.ClusterOpts{Host: "127.0.0.1", Port: -1, Name: "authgate"}
		opts.Nkeys = []*natsserver.NkeyUser{{Nkey: brokerPublic}}
		opts.AuthCallout = &natsserver.AuthCallout{Issuer: f.config.AccountPublicKey, Account: "$G", AuthUsers: []string{brokerPublic}}
		peer := natstest.RunServer(&opts)
		t.Cleanup(func() { peer.Shutdown(); peer.WaitForShutdown() })
		nc, err := nats.Connect(peer.ClientURL(), nats.Nkey(brokerPublic, f.keys[0].Sign), nats.NoReconnect())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		if _, err := nc.QueueSubscribe(authSubject, authQueue, func(msg *nats.Msg) {
			response, err := handle(string(msg.Data))
			if err != nil {
				return
			}
			if localProbe(f, string(msg.Data)) {
				answered.Add(1)
			}
			_ = msg.Respond([]byte(response))
		}); err != nil {
			t.Fatal(err)
		}
		if err := nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if !testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool { return f.server.NumRoutes() >= n }) {
		t.Fatal("peer routes never became live")
	}
	return answered
}

// localProbe reports whether request is the local server's callout for one of
// the two ordinary probe identities.
func localProbe(f *authFixture, request string) bool {
	req, err := jwt.DecodeAuthorizationRequestClaims(request)
	return err == nil && req.Server.ID == f.server.ID() &&
		(req.ConnectOptions.Nkey == f.config.Member.PublicKey || req.ConnectOptions.Nkey == f.config.Agent.PublicKey)
}

// nats-server hands each callout to ONE queue member chosen uniformly across
// local and routed members, so with three brokers a peer answers the local
// probe CONNECT two times in three. The local proof must not depend on that
// draw: it never passes on a peer's answer and never fails because a peer
// answered, and the probe witness never becomes a second responder.
// origin: hot-update S9 plan §4.4 / T39; S9 P22 resume — design finding (callout queue selection)
func TestOrdinaryAuthLocalProofIgnoresQueueSelection(t *testing.T) {
	t.Run("healthy_local_owner_never_fails_when_peers_answer", func(t *testing.T) {
		f := newAuthFixture(t)
		handle := f.config.HandleAuth
		var decisions, replies atomic.Int32
		f.config.HandleAuth = func(request string) (string, error) {
			if localProbe(f, request) {
				decisions.Add(1)
			}
			return handle(request)
		}
		f.config.Replied = func(request, _ string) {
			if localProbe(f, request) {
				replies.Add(1)
			}
		}
		peers := startAuthPeers(t, f, 2, handle)
		brokerPublic, _ := f.keys[0].PublicKey()
		observer, err := nats.Connect(f.config.URL, nats.Nkey(brokerPublic, f.keys[0].Sign), nats.NoReconnect())
		if err != nil {
			t.Fatal(err)
		}
		defer observer.Close()
		var mu sync.Mutex
		answers := map[string]int{}
		if _, err := observer.Subscribe("$SYS._INBOX.>", func(msg *nats.Msg) { mu.Lock(); answers[msg.Subject]++; mu.Unlock() }); err != nil {
			t.Fatal(err)
		}
		if err := observer.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		owner := f.newOwner()
		for round := 0; round < 6 || ((peers.Load() == 0 || replies.Load() == 0) && round < 60); round++ {
			receipt, err := owner.Open(testDeadline(t, 3*time.Second), nil)
			if err != nil || receipt.Phase != ControlReady {
				t.Fatalf("round %d: healthy local owner refused after %d peer answers: %v", round, peers.Load(), err)
			}
		}
		if peers.Load() == 0 || replies.Load() == 0 {
			t.Fatalf("non-vacuity: peer answers=%d local answers=%d; both queue choices must occur", peers.Load(), replies.Load())
		}
		// Each probe is decided by the local owner exactly once: by its own
		// queue when chosen, otherwise once in process — never twice, never not.
		if decisions.Load() != peers.Load()+replies.Load() {
			t.Fatalf("local decisions=%d, want peer answers %d + local answers %d", decisions.Load(), peers.Load(), replies.Load())
		}
		if err := observer.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		defer mu.Unlock()
		for subject, n := range answers {
			if n != 1 {
				t.Fatalf("callout %s answered %d times: the probe witness responded", subject, n)
			}
		}
		if len(answers) < int(peers.Load()+replies.Load()) {
			t.Fatalf("answer observer saw %d replies for %d probes", len(answers), peers.Load()+replies.Load())
		}
		t.Logf("probe CONNECTs answered by peers=%d, by the local queue=%d; local decisions=%d", peers.Load(), replies.Load(), decisions.Load())
	})
	t.Run("broken_local_owner_never_passes_on_peer_answers", func(t *testing.T) {
		f := newAuthFixture(t)
		handle := f.config.HandleAuth
		// The peers keep the healthy handler; only this broker's owner is broken.
		f.config.HandleAuth = func(string) (string, error) { return "", errors.New("local auth store unavailable") }
		peers := startAuthPeers(t, f, 2, handle)
		owner := f.newOwner()
		for round := 0; round < 6 || (peers.Load() < 2 && round < 40); round++ {
			if receipt, err := owner.Open(testDeadline(t, time.Second), nil); err == nil || receipt.AuthUsable != 0 {
				t.Fatalf("round %d: a peer's answer signed a broken local owner", round)
			}
		}
		if peers.Load() < 2 {
			t.Fatalf("non-vacuity: peers admitted only %d probe CONNECTs", peers.Load())
		}
	})
	t.Run("removed_local_queue_never_passes_on_peer_answers", func(t *testing.T) {
		f := newAuthFixture(t)
		peers := startAuthPeers(t, f, 2, f.config.HandleAuth)
		owner := f.newOwner()
		if _, err := owner.Open(testDeadline(t, 3*time.Second), nil); err != nil {
			t.Fatal(err)
		}
		g := removeNormalQueue(t, owner)
		for round := 0; round < 5; round++ {
			before := peers.Load()
			nc, err := dialOrdinary(t, f, f.config.URL, f.config.Member)
			if err != nil || peers.Load() == before {
				t.Fatal("peer negative control did not admit the local CONNECT")
			}
			nc.Close()
			if nc, err := owner.authenticate(g, f.config.Member, 0, testDeadline(t, time.Second)); err == nil {
				nc.Close()
				t.Fatalf("round %d: a peer's answer signed a removed local queue", round)
			}
		}
	})
}

// origin: hot-update S9 plan §4.4 / T38 / TestInfraAuthLifecycle
func TestOrdinaryAuthFenceJoinsAdmittedCallbacks(t *testing.T) {
	f := newAuthFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	releaseAuth := releaseOnCleanup(t, release)
	var once sync.Once
	handle := f.config.HandleAuth
	f.config.HandleAuth = func(request string) (string, error) {
		once.Do(func() { close(entered); <-release })
		return handle(request)
	}
	owner := f.newOwner()
	openDone := make(chan error, 1)
	go func() { _, err := owner.Open(testDeadline(t, time.Second), nil); openDone <- err }()
	waitSignal(t, entered)
	if err := owner.Close(testDeadline(t, 30*time.Millisecond)); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("unjoined callback reported closed: %v", err)
	}
	if _, _, err := owner.Business(); !errors.Is(err, ErrFenced) {
		t.Fatal("blocked callback left business admission open")
	}
	releaseAuth()
	if err := <-openDone; err == nil {
		t.Fatal("cancelled CONNECT reached ready")
	}
	if err := owner.Close(testDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §4.4 / T38 / T39
func TestOrdinaryAuthBindingRejectsBypasses(t *testing.T) {
	f := newAuthFixture(t)
	for _, defect := range []string{"peer_list", "userinfo", "missing_transport", "unactivated_member", "unprovisioned_role", "auth_on_business", "duplicate_sub", "missing_handler", "missing_rpc"} {
		t.Run(defect, func(t *testing.T) {
			config := f.config
			config.Business = append([]Binding(nil), config.Business...)
			switch defect {
			case "peer_list":
				config.URL += "," + f.config.URL
			case "userinfo":
				config.URL = "nats://secret@127.0.0.1:1"
			case "missing_transport":
				config.Transport = nil
			case "unactivated_member":
				config.Member.Name = "tether-cli"
			case "unprovisioned_role":
				config.Agent.Name = "transfer-settlement:authgate:node"
			case "auth_on_business":
				config.Business[0].Subject = authSubject
			case "duplicate_sub":
				config.Business = append(config.Business, config.Business[0])
			case "missing_handler":
				config.HandleAuth = nil
			case "missing_rpc":
				config.Agent.RPCResponse = nil
			default:
				t.Fatal("unknown defect")
			}
			if owner, err := New(config); owner != nil || !errors.Is(err, ErrBinding) {
				t.Fatal("invalid descriptor accepted")
			}
		})
	}
	owner := f.newOwner()
	if _, err := owner.Open(0, nil); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatal("zero deadline created a connection")
	}
	stopped := make(chan struct{})
	close(stopped)
	if _, err := owner.Open(testDeadline(t, time.Second), stopped); !errors.Is(err, ErrFenced) {
		t.Fatal("stopped operation created a connection")
	}
	if len(f.wire()) != 0 {
		t.Fatal("rejected operation touched the network")
	}
}

// origin: hot-update S9 plan §4.4 / T39
func TestOrdinaryAuthReplyBindsLocalRequest(t *testing.T) {
	f := newAuthFixture(t)
	owner := f.newOwner()
	serverKey, err := nkeys.CreateServer()
	if err != nil {
		t.Fatal(err)
	}
	defer serverKey.Wipe()
	serverID, _ := serverKey.PublicKey()
	otherAccount, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	defer otherAccount.Wipe()
	otherServer, err := nkeys.CreateServer()
	if err != nil {
		t.Fatal(err)
	}
	defer otherServer.Wipe()
	otherServerID, _ := otherServer.PublicKey()
	ephemeral, _ := f.keys[3].PublicKey()
	for _, defect := range []string{"valid", "request_signature", "request_issuer", "request_server", "zero_cid", "response_signature", "denied", "response_issuer", "response_subject", "response_audience", "user_signature", "user_issuer", "user_subject", "identity_key", "identity_name"} {
		t.Run(defect, func(t *testing.T) {
			req := jwt.NewAuthorizationRequestClaims(ephemeral)
			req.UserNkey, req.Server.ID, req.ClientInformation.ID = ephemeral, serverID, 42
			req.ConnectOptions.Nkey, req.ConnectOptions.Name = f.config.Member.PublicKey, f.config.Member.Name
			user := jwt.NewUserClaims(ephemeral)
			user.Permissions = auth.PermissionsForActivatedMember(f.config.Member.PublicKey, "authgate", false)
			response := jwt.NewAuthorizationResponseClaims(ephemeral)
			response.Audience = serverID
			reqKey, responseKey, userKey := serverKey, f.keys[4], f.keys[4]
			switch defect {
			case "valid", "request_signature", "response_signature", "user_signature":
			case "request_issuer":
				reqKey = otherServer
			case "request_server":
				req.Server.ID = otherServerID
				response.Audience = otherServerID
			case "zero_cid":
				req.ClientInformation.ID = 0
			case "denied":
				response.Error = "denied"
			case "response_issuer":
				responseKey = otherAccount
			case "response_subject":
				response.Subject = f.config.Agent.PublicKey
			case "response_audience":
				response.Audience = otherServerID
			case "user_issuer":
				userKey = otherAccount
			case "user_subject":
				user.Subject = f.config.Agent.PublicKey
			case "identity_key":
				req.ConnectOptions.Nkey = f.config.Agent.PublicKey
			case "identity_name":
				req.ConnectOptions.Name = "tether-cli"
			default:
				t.Fatal("unknown defect")
			}
			requestJWT, err := req.Encode(reqKey)
			if err != nil {
				t.Fatal(err)
			}
			response.Jwt, err = user.Encode(userKey)
			if err != nil {
				t.Fatal(err)
			}
			if defect == "user_signature" {
				response.Jwt += "broken"
			}
			responseJWT, err := response.Encode(responseKey)
			if err != nil {
				t.Fatal(err)
			}
			if defect == "request_signature" {
				requestJWT += "broken"
			}
			if defect == "response_signature" {
				responseJWT += "broken"
			}
			g := &generation{receipt: Receipt{Generation: 9, ServerID: serverID}, responseReady: [2]chan struct{}{make(chan struct{}), make(chan struct{})}}
			owner.observeResponse(g, requestJWT, responseJWT)
			proof := g.responses[0]
			if defect == "valid" {
				if proof.Generation != 9 || proof.ServerID != serverID || proof.CID != 42 || proof.ReplyBootNS == 0 {
					t.Fatal("valid local request produced no witness")
				}
			} else if proof.ReplyBootNS != 0 {
				t.Fatalf("%s produced a local queue witness", defect)
			}
			encoded, err := json.Marshal(snapshot(g))
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), requestJWT) || strings.Contains(string(encoded), responseJWT) {
				t.Fatal("private auth JWT escaped through receipt")
			}
		})
	}
}
