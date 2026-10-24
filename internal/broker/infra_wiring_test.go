package broker

import (
	"encoding/base64"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/infraattach"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/natsconf"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/jwt/v2"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// One descriptor serves cold Run, the S6 restored graph's StartAuth and every
// ordinary SDK reconnect; each entry is checked on the wire the local server
// receives, with a healthy routed peer queue and with a sealed ACK prefix.
// origin: hot-update S9 plan §4.4 / §4.3 / T48 / M24; rv-business F2 / rv-infra F10
func TestBrokerInfraColdHotReconnectBindings(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	// Delayed EOF represents the matrix's independently scheduled relay/client
	// readers. Restart must observe retirement before a fresh admission can be
	// accepted; the previous generation's connected bit is not that receipt.
	// origin: S10 internal review POLISH-A F3 / hot-update S9 plan §4.4 / T48
	t.Run("restart-waits-for-old-admission-retirement", func(t *testing.T) {
		f := newBrokerInfraFixture(t, true, false)
		b := f.run(t, true)
		entered, release := make(chan struct{}), make(chan struct{})
		var once, releaseOnce sync.Once
		unblock := func() { releaseOnce.Do(func() { close(release) }) }
		defer unblock()
		f.wire.mu.Lock()
		f.wire.beforeClientClose = func() { once.Do(func() { close(entered) }); <-release }
		f.wire.mu.Unlock()
		mark := f.wire.mark()
		returned := make(chan struct{})
		go func() { defer close(returned); f.restart(t, b) }()
		defer func() {
			unblock()
			select {
			case <-returned:
			case <-time.After(20 * time.Second):
				t.Error("restart did not join after EOF release")
			}
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("server EOF did not reach the relay")
		}
		if !b.infra.Admitted() || !b.nc.Load().IsConnected() {
			t.Fatal("delayed EOF did not retain the old admission")
		}
		select {
		case <-returned:
			t.Fatal("restart returned while the old business connection remained admitted")
		case <-time.After(time.Second):
		}
		unblock()
		select {
		case <-returned:
		case <-time.After(20 * time.Second):
			t.Fatal("restart did not observe old admission retirement")
		}
		infraAdmitted(t, b)
		requireInfraAdmission(t, "fresh restart", f.wire.since(mark), false)
	})
	t.Run("cold-and-ordinary-reconnect-with-healthy-peer-queue", func(t *testing.T) {
		f := newBrokerInfraFixture(t, false, true)
		b := f.run(t, false)
		if !b.infra.Configured() {
			t.Fatal("configured broker kept the legacy dial")
		}
		requireInfraAdmission(t, "cold", f.wire.since(0), false)
		// gotcha #89: the business owner never roams to a healthy peer.
		for _, peer := range f.peers {
			if _, err := b.infra.Dial("tcp", peer.Addr().String()); !errors.Is(err, c.ErrIncomplete) {
				t.Fatalf("business dial accepted the peer address: %v", err)
			}
		}
		mark := f.wire.mark()
		// Disconnect closes business admission synchronously: a queued business
		// callback cannot run under the lost generation while the server is down.
		var ran atomic.Int64
		business := brokerBusinessHandler(b, func(*nats.Msg) { ran.Add(1) })
		f.down()
		if !testharness.WaitFor(t, 10*time.Second, 5*time.Millisecond, func() bool { return !b.infra.Admitted() }) {
			t.Fatal("disconnect did not close business admission")
		}
		business(&nats.Msg{Subject: infraRegisterSubject})
		f.up(t)
		infraAdmitted(t, b)
		requireInfraAdmission(t, "ordinary reconnect", f.wire.since(mark), false)
		business(&nats.Msg{Subject: infraRegisterSubject})
		if ran.Load() != 1 {
			t.Fatalf("business handler ran %d times, want only after re-admission", ran.Load())
		}
		// Non-vacuity of "a peer answer never costs admission": peers must really
		// have answered this broker's own probes. Auth-only losses re-prove the
		// local owner (each must admit again) until one probe went to a peer.
		// origin: S9 P22 resume — design finding (callout queue selection)
		for round := 0; f.peerProbes.Load() == 0 && round < 20; round++ {
			old := infraConnCID(t, f.server, "tetherd-auth")
			if err := f.server.DisconnectClientByID(old); err != nil {
				t.Fatal(err)
			}
			if !testharness.WaitFor(t, 20*time.Second, 5*time.Millisecond, func() bool {
				cid := infraConnCID(t, f.server, "tetherd-auth")
				return cid != 0 && cid != old && b.infra.Admitted() && b.nc.Load().IsConnected()
			}) {
				t.Fatalf("round %d: auth-only loss was not re-admitted beside healthy peers", round)
			}
		}
		if f.peerProbes.Load() == 0 {
			t.Fatal("no peer ever answered the broker's probes: the three-broker arm proved nothing")
		}
		for _, peer := range f.peers {
			if peerConnz, _ := peer.Connz(&natsserver.ConnzOptions{}); peerConnz != nil {
				for _, conn := range peerConnz.Conns {
					if strings.HasPrefix(conn.Name, "tetherd") {
						t.Fatalf("broker NC %q roamed to the peer", conn.Name)
					}
				}
			}
		}
	})
	t.Run("ordinary-reconnect-reads-ack-prefix-before-replay", func(t *testing.T) {
		f := newBrokerInfraFixture(t, true, false)
		b := f.run(t, true)
		requireInfraAdmission(t, "cold", f.wire.since(0), false)
		if err := b.publishAudit(proto.SubjSysEvents, []byte(`{"type":"t48-original"}`)); err != nil {
			t.Fatal(err)
		}
		recovery := b.infra.Recovery()
		cp, err := recovery.Seal(c.Plan{Tx: c.ID{4}, Domain: c.ID{7}, Component: c.NATS}, infraCutDeadline(t))
		if err != nil {
			t.Fatal(err)
		}
		mark := f.wire.mark()
		// Business keeps trying to produce across the cut; every attempt must be
		// refused before it can reach the restarted store.
		stop, joined := make(chan struct{}), make(chan struct{})
		var refused atomic.Int64
		go func() {
			defer close(joined)
			for {
				select {
				case <-stop:
					return
				case <-time.After(10 * time.Millisecond):
				}
				if errors.Is(b.publishAudit(proto.SubjSysEvents, []byte(`{"type":"t48-repair"}`)), infrarepair.ErrFenced) {
					refused.Add(1)
				}
			}
		}()
		f.restart(t, b)
		infraAdmitted(t, b)
		window := f.wire.since(mark)
		requireInfraAdmission(t, "sealed reconnect", window, true)
		if i := infraFind(window, 0, infraMutation); i >= 0 {
			t.Fatalf("recovery producer reached the store before verified release: %+v", window[i])
		}
		close(stop)
		<-joined
		if refused.Load() == 0 {
			t.Fatal("producer loop never exercised the sealed gate")
		}
		if err := recovery.VerifyBeforeReplay(cp, infraCutDeadline(t), nil); err != nil {
			t.Fatal(err)
		}
		if err := recovery.Release(cp, infraCutDeadline(t)); err != nil {
			t.Fatal(err)
		}
		if err := b.publishAudit(proto.SubjSysEvents, []byte(`{"type":"t48-after"}`)); err != nil {
			t.Fatalf("released producer still fenced: %v", err)
		}
	})
	t.Run("hot-restored-graph-uses-the-same-descriptor", func(t *testing.T) {
		f := newBrokerInfraFixture(t, false, false)
		b := &Broker{cfg: f.config(false)}
		ctl := newBrokerImageControl(b)
		b.imageControl = ctl
		t.Cleanup(func() {
			ctl.cancel()
			_ = stopBrokerImageAuth(ctl.auth, brokerAuthorityDeadline(t, time.Second))
			if nc := b.nc.Load(); nc != nil {
				nc.Close()
			}
			_ = b.infra.Close(0)
		})
		if err := ctl.StartAuth(brokerAuthorityDeadline(t, 10*time.Second)); err != nil {
			t.Fatal(err)
		}
		if !b.infra.Configured() || ctl.auth == nil || ctl.auth.UsableBootNS == 0 {
			t.Fatal("hot entry did not admit through the S9 descriptor and the S6 callout proof")
		}
		requireInfraAdmission(t, "hot", f.wire.since(0), false)
	})
	t.Run("hot-cluster-graph-binds-apply-after-auth", func(t *testing.T) {
		f := newBrokerInfraFixture(t, false, false)
		node, _ := d7SingleNode(t, "infra-hot")
		f.seed(t, node.DB())
		b := &Broker{cfg: f.config(false), clusterMode: true, cl: &clusterRuntime{node: node}}
		b.cfg.DB = node.RODB()
		ctl := newBrokerImageControl(b)
		b.imageControl = ctl
		t.Cleanup(func() {
			ctl.cancel()
			_ = stopBrokerImageAuth(ctl.auth, brokerAuthorityDeadline(t, time.Second))
			if nc := b.nc.Load(); nc != nil {
				nc.Close()
			}
			_ = b.infra.Close(0)
		})
		// The apply responder needs a connected NC; S6's pre-auth rebind would
		// demand one before the auth-only generation exists. The configured graph
		// binds only the forwarder object and leaves the responder to wireClusterLate.
		if err := ctl.StartAuth(brokerAuthorityDeadline(t, 10*time.Second)); err != nil {
			t.Fatal(err)
		}
		if ctl.apply != nil || b.cl.forwarder == nil || b.cl.forwarder.nc != b.nc.Load() || b.leaderPINVerify.Load() == nil {
			t.Fatalf("cluster hot entry bound the wrong authority: apply=%v forwarder=%v verifier=%v", ctl.apply != nil, b.cl.forwarder != nil, b.leaderPINVerify.Load() != nil)
		}
		requireInfraAdmission(t, "hot cluster", f.wire.since(0), false)
	})
}

// GC, audit and cold repair are recovery producers: with the seal held none
// reaches JetStream, and the same entries reach it again after release. The
// S6 image cut is refused for the whole sealed interval.
// origin: hot-update S9 plan §4.3 / §7.2 / T49 / M24; S9 P18 requests
func TestBrokerInfraGatesAllJSProducers(t *testing.T) {
	f := newBrokerInfraFixture(t, true, false)
	b := f.run(t, true)
	nc := b.nc.Load()
	producers := []struct {
		name string
		run  func() error
		wire func(infraWireEvent) bool
	}{
		{"audit", func() error { return b.publishAudit(proto.SubjSysEvents, []byte(`{"type":"t49"}`)) }, infraMutation},
		{"audit-dedup", func() error {
			return publishAuditDeduped(b, proto.SubjSysEvents, []byte(`{"type":"t49d"}`), "t49-dedup")
		}, infraMutation},
		{"cold-repair", func() error { return enableJetStream(t.Context(), b, nc) }, infraMutation},
		{"history-repair", func() error { return b.reconcileHistoryStreamsOnBoot(t.Context()) }, infraMutation},
		{"gc", func() error {
			b.reconcilers.runMu.Lock()
			defer b.reconcilers.runMu.Unlock()
			return b.reconcilers.byName["xfer-orphan-reap"].fn(t.Context(), time.Now())
		}, func(e infraWireEvent) bool {
			return e.name == "tetherd" && (strings.HasPrefix(e.subject, "$JS.API.STREAM.LIST") || strings.HasPrefix(e.subject, "$JS.API.STREAM.NAMES"))
		}},
		// The terminal object delete (watchdog, failure and stranded-ledger cleanup).
		{"xfer-object-delete", func() error { return b.deleteXferObject(t.Context(), infraSID, "t49-object") }, infraMutation},
	}
	// Remove the session history stream out of band, so history repair has a
	// real CREATE to perform — and to withhold while sealed. The staged object
	// gives the terminal delete a real purge to withhold.
	if err := f.admin.DeleteStream(t.Context(), jsstream.HistoryStreamName(infraSID)); err != nil && !errors.Is(err, jetstream.ErrStreamNotFound) {
		t.Fatal(err)
	}
	staged, err := f.admin.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName(infraSID)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := staged.PutBytes(t.Context(), "t49-object", []byte("staged")); err != nil {
		t.Fatal(err)
	}
	recovery := b.infra.Recovery()
	cp, err := recovery.Seal(c.Plan{Tx: c.ID{5}, Domain: c.ID{7}, Component: c.NATS}, infraCutDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range producers {
		mark := f.wire.mark()
		err := p.run()
		if p.name != "history-repair" && !errors.Is(err, infrarepair.ErrFenced) {
			t.Errorf("%s: sealed producer was not refused by the recovery gate: %v", p.name, err)
		}
		if i := infraFind(f.wire.since(mark), 0, p.wire); i >= 0 {
			t.Errorf("%s: sealed producer reached JetStream: %+v", p.name, f.wire.since(mark)[i])
		}
	}
	// M24 image/infra overlap: refused before any Freeze or Fence can run.
	if err := runBrokerImageReloadUsing(b, nil, brokerimage.StoreShutdown{}); !errors.Is(err, infraattach.ErrInfraPending) {
		t.Fatalf("sealed infra domain admitted an image cut: %v", err)
	}
	if err := recovery.VerifyBeforeReplay(cp, infraCutDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Release(cp, infraCutDeadline(t)); err != nil {
		t.Fatal(err)
	}
	// Non-vacuity: the same entries are live producers once released.
	for _, p := range producers {
		mark := f.wire.mark()
		if err := p.run(); err != nil {
			t.Errorf("%s: released producer failed: %v", p.name, err)
		}
		if !testharness.WaitFor(t, 5*time.Second, 5*time.Millisecond, func() bool { return infraFind(f.wire.since(mark), 0, p.wire) >= 0 }) {
			t.Errorf("%s: released producer never reached JetStream — the sealed refusal above proved nothing", p.name)
		}
	}
	if err := runBrokerImageReloadUsing(b, nil, brokerimage.StoreShutdown{}); errors.Is(err, infraattach.ErrInfraPending) {
		t.Fatal("released infra domain still refuses image cuts")
	}
}

// origin: hot-update S9 plan §4.4 / P22 / L (TestBrokerInfraLifecycle→exerciseBrokerInfra)
func TestBrokerInfraLifecycle(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerInfra(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "broker infra lifecycle", goroutines)
	testharness.AssertNoFDLeak(t, "broker infra lifecycle", fds, 0)
}

func exerciseBrokerInfra(t *testing.T) {
	f := newBrokerInfraFixture(t, true, false)
	b := f.run(t, true)
	recovery := b.infra.Recovery()
	cp, err := recovery.Seal(c.Plan{Tx: c.ID{6}, Domain: c.ID{7}, Component: c.NATS}, infraCutDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	f.restart(t, b)
	infraAdmitted(t, b)
	if err := recovery.VerifyBeforeReplay(cp, infraCutDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Release(cp, infraCutDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := b.publishAudit(proto.SubjSysEvents, []byte(`{"type":"lifecycle"}`)); err != nil {
		t.Fatal(err)
	}
}

// The production callout — built by installAuthCallout from the broker's own
// NatsConfPath — admits exactly the managed $SYS principals of the CURRENT
// file: a reload that rotates a key moves admission with it, and a key never
// in the file stays on the ordinary role path.
// origin: S9 internal review rv-infra F11 (P22 re-check of the s7-p9m binding)
func TestBrokerCalloutBindsLoadedSystemPrincipals(t *testing.T) {
	url := startNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	keys := map[string]nkeys.KeyPair{}
	pub := func(name string) string {
		if keys[name] == nil {
			kp, err := nkeys.CreateUser()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(kp.Wipe)
			keys[name] = kp
		}
		p, _ := keys[name].PublicKey()
		return p
	}
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(account.Wipe)
	accountSeed, _ := account.Seed()
	brokerKey, _ := nkeys.CreateUser()
	t.Cleanup(brokerKey.Wipe)
	brokerSeed, _ := brokerKey.Seed()
	conf := filepath.Join(t.TempDir(), "nats.conf")
	stamp := time.Now().Add(-time.Hour)
	write := func(read, step string) {
		t.Helper()
		block, err := natsconf.SystemPrincipalBlock(natsconf.SystemPrincipals{ReadNkey: read, StepNkey: step})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(conf, []byte("port: -1\n"+block), 0o600); err != nil {
			t.Fatal(err)
		}
		stamp = stamp.Add(time.Second)
		if err := os.Chtimes(conf, stamp, stamp); err != nil {
			t.Fatal(err)
		}
	}
	write(pub("read"), pub("step"))
	b := &Broker{cfg: Config{DB: openDB(t), NATSURL: url, Logger: silentLogger(), Now: time.Now, NatsConfPath: conf,
		AuthCallout: &AuthCalloutConfig{BrokerNkeySeed: brokerSeed, AccountSeed: accountSeed}}}
	sub, err := b.installAuthCallout(nc)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	server, _ := nkeys.CreateServer()
	serverPub, _ := server.PublicKey()
	// ask sends the request a server would send for a CONNECT that signed its
	// nonce with key, through the production queue subscription.
	ask := func(name string) (*jwt.UserClaims, string) {
		t.Helper()
		ephemeral, _ := nkeys.CreateUser()
		ephemeralPub, _ := ephemeral.PublicKey()
		rc := jwt.NewAuthorizationRequestClaims(serverPub)
		rc.UserNkey, rc.Server = ephemeralPub, jwt.ServerID{ID: serverPub, Name: serverPub}
		rc.ConnectOptions = jwt.ConnectOptions{Nkey: pub(name)}
		rc.ClientInformation.Nonce = "nonce-" + name
		sig, err := keys[name].Sign([]byte(rc.ClientInformation.Nonce))
		if err != nil {
			t.Fatal(err)
		}
		rc.ConnectOptions.SignedNonce = base64.RawURLEncoding.EncodeToString(sig)
		token, err := rc.Encode(server)
		if err != nil {
			t.Fatal(err)
		}
		reply, err := nc.Request("$SYS.REQ.USER.AUTH", []byte(token), 2*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := jwt.DecodeAuthorizationResponseClaims(string(reply.Data))
		if err != nil {
			t.Fatal(err)
		}
		if resp.Error != "" {
			return nil, resp.Error
		}
		uc, err := jwt.DecodeUserClaims(resp.Jwt)
		if err != nil {
			t.Fatal(err)
		}
		return uc, ""
	}
	descriptors, err := natsconf.LoadedSystemPrincipalDescriptors(conf)
	if err != nil || len(descriptors) != 2 {
		t.Fatalf("loaded descriptors: %v", err)
	}
	for _, d := range descriptors {
		name := "read"
		if d.Nkey == pub("step") {
			name = "step"
		}
		uc, denied := ask(name)
		if uc == nil || uc.Audience != authcallout.SystemAccount || !slices.Equal([]string(uc.Pub.Allow), d.Publish) || !slices.Equal([]string(uc.Sub.Allow), d.Subscribe) {
			t.Fatalf("managed %s principal not admitted with its exact authority (denied=%q claims=%+v)", name, denied, uc)
		}
	}
	if uc, _ := ask("forged"); uc != nil && uc.Audience == authcallout.SystemAccount {
		t.Fatal("a key absent from the loaded file was placed in $SYS")
	}
	// Reload: rotation replaces the read principal; the old key leaves $SYS.
	write(pub("rotated"), pub("step"))
	if uc, _ := ask("rotated"); uc == nil || uc.Audience != authcallout.SystemAccount {
		t.Fatal("rotated SYS principal was not admitted after reload")
	}
	if uc, _ := ask("read"); uc != nil && uc.Audience == authcallout.SystemAccount {
		t.Fatal("rotated-out SYS principal kept $SYS after reload")
	}
}
