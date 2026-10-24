//go:build linux

package broker

import (
	"fmt"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// origin: S6 internal review rv-final F3
func TestImageAuthBecomesUsableBeforeJetStreamWitness(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseImageAuthBecomesUsableBeforeJetStreamWitness(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestImageAuthBecomesUsableBeforeJetStreamWitness x5", g)
	testharness.AssertNoFDLeak(t, "TestImageAuthBecomesUsableBeforeJetStreamWitness x5", fd, 0)
}

func exerciseImageAuthBecomesUsableBeforeJetStreamWitness(t *testing.T) {
	t.Helper()
	b, nc, _ := brokerImageAuthFixture(t, true)
	b.nc.Store(nc)
	var witnessAt atomic.Int64
	sub, err := nc.Subscribe("$JS.API.INFO", func(m *nats.Msg) {
		now, _ := procrequest.BootNow()
		witnessAt.Store(now)
		_ = m.Respond([]byte(`{"type":"io.nats.jetstream.api.v1.account_info_response","error":{"code":503,"err_code":10008,"description":"JetStream unavailable"}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	c := newBrokerImageControl(b)
	b.imageControl = c
	defer c.cancel()
	defer func() { _ = stopBrokerImageAuth(c.auth, brokerAuthorityDeadline(t, time.Second)) }()
	if err := c.StartAuth(brokerAuthorityDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if c.auth == nil || c.auth.UsableBootNS == 0 {
		t.Fatal("no actual JWT/CONNECT proof")
	}
	if witnessAt.Load() != 0 && c.auth.UsableBootNS > witnessAt.Load() {
		t.Fatalf("JetStream witness preceded usable callout by %s", time.Duration(c.auth.UsableBootNS-witnessAt.Load()))
	}
}

// The clustered production branch returns before installing callout at all.
// origin: S6 internal review rv-final F3
func TestImageAuthSurvivesJetStreamWitnessFailure(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseImageAuthSurvivesJetStreamWitnessFailure(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "TestImageAuthSurvivesJetStreamWitnessFailure x5", g)
	testharness.AssertNoFDLeak(t, "TestImageAuthSurvivesJetStreamWitnessFailure x5", fd, 0)
}

func exerciseImageAuthSurvivesJetStreamWitnessFailure(t *testing.T) {
	t.Helper()
	b, nc, _ := brokerImageAuthFixture(t, true)
	n, _ := d7SingleNode(t, "auth-first")
	b.clusterMode = true
	b.cl = &clusterRuntime{node: n}
	b.cfg.DB = n.RODB()
	b.nc.Store(nc)
	sub, err := nc.Subscribe("$JS.API.INFO", func(m *nats.Msg) {
		_ = m.Respond([]byte(`{"type":"io.nats.jetstream.api.v1.account_info_response","error":{"code":503,"err_code":10008,"description":"JetStream unavailable"}}`))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	c := newBrokerImageControl(b)
	b.imageControl = c
	defer c.cancel()
	defer func() {
		_ = stopBrokerImageAuth(c.auth, brokerAuthorityDeadline(t, time.Second))
		_ = stopBrokerImageAuth(c.apply, brokerAuthorityDeadline(t, time.Second))
	}()
	err = c.StartAuth(brokerAuthorityDeadline(t, 3*time.Second))
	if err != nil || c.auth == nil || c.auth.UsableBootNS == 0 {
		t.Fatalf("JetStream failure suppresses clustered auth: authInstalled=%v err=%v", c.auth != nil, err)
	}
}

// origin: S6 internal review rv-final F3
func TestImageJetStreamFailureKeepsAuthUsableWithoutFullReady(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseImageJetStreamFailure(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "JS witness after usable auth x5", g)
	testharness.AssertNoFDLeak(t, "JS witness after usable auth x5", fd, 0)
}

func exerciseImageJetStreamFailure(t *testing.T) {
	t.Helper()
	for _, mode := range []string{"503", "slow", "silent"} {
		t.Run(mode, func(t *testing.T) {
			b, nc, url := brokerImageAuthFixture(t, true)
			b.nc.Store(nc)
			c := newBrokerImageControl(b)
			b.imageControl = c
			t.Cleanup(func() { c.cancel(); _ = stopBrokerImageAuth(c.auth, brokerAuthorityDeadline(t, time.Second)) })
			entered := make(chan struct{}, 1)
			var witnessAt atomic.Int64
			sub, err := nc.Subscribe("$JS.API.INFO", func(m *nats.Msg) {
				now, _ := procrequest.BootNow()
				witnessAt.Store(now)
				select {
				case entered <- struct{}{}:
				default:
				}
				if mode == "503" {
					_ = m.Respond([]byte(`{"type":"io.nats.jetstream.api.v1.account_info_response","error":{"code":503,"err_code":10008,"description":"JetStream unavailable"}}`))
				}
				if mode == "slow" {
					time.Sleep(150 * time.Millisecond)
					_ = m.Respond([]byte(`{"type":"io.nats.jetstream.api.v1.account_info_response","error":{"code":503,"err_code":10008,"description":"JetStream unavailable"}}`))
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = sub.Unsubscribe() })
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatal(err)
			}
			if err := c.StartAuth(brokerAuthorityDeadline(t, 3*time.Second)); err != nil {
				t.Fatal(err)
			}
			if c.auth == nil || c.auth.UsableBootNS == 0 || witnessAt.Load() != 0 {
				t.Fatal("auth depended on JetStream")
			}
			if err := c.StartBusiness(brokerAuthorityDeadline(t, 300*time.Millisecond)); err == nil {
				t.Fatal("failed witness announced full-ready")
			}
			select {
			case <-entered:
			default:
				t.Fatal("production business did not check JS")
			}
			if c.auth.UsableBootNS > witnessAt.Load() {
				t.Fatal("JS preceded real auth proof")
			}
			if len(c.subs) != 0 {
				t.Fatal("failed witness installed business")
			}
			if status := brokerImageSnapshot(b); status != nil && status.Phase == proto.BrokerImagePhaseServing {
				t.Fatal("failed witness advertised serving")
			}
			key, pub := brokerImageUser(t)
			probe, err := nats.Connect(url, nats.Nkey(pub, key.Sign), nats.Name("tether-cli"), nats.MaxReconnects(0), nats.Timeout(time.Second))
			if err != nil {
				t.Fatalf("JS failure disabled proved callout: %v", err)
			}
			probe.Close()
		})
	}
}

// Resumed transfer factories run between auth and full-ready. They need the
// current client even when the external JS witness is unavailable.
// origin: S6 internal review rv-final F3 / S7 plan §8.2
func TestImageAuthRebindsJetStreamToCurrentTransport(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseImageAuthCurrentJetStream(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "auth current JS client x5", g)
	testharness.AssertNoFDLeak(t, "auth current JS client x5", fd, 0)
}

func exerciseImageAuthCurrentJetStream(t *testing.T) {
	t.Helper()
	b, nc, url := brokerImageAuthFixture(t, true)
	pub, err := auth.PublicKeyFromSeed(b.cfg.AuthCallout.BrokerNkeySeed)
	if err != nil {
		t.Fatal(err)
	}
	old, err := nats.Connect(url, nats.Nkey(pub, func(nonce []byte) ([]byte, error) { return auth.SignWithSeed(b.cfg.AuthCallout.BrokerNkeySeed, nonce) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(old.Close)
	js, err := jetstream.New(old)
	if err != nil {
		t.Fatal(err)
	}
	setBrokerJS(b, js)
	old.Close()
	b.nc.Store(nc)
	c := newBrokerImageControl(b)
	b.imageControl = c
	t.Cleanup(func() { c.cancel(); _ = stopBrokerImageAuth(c.auth, brokerAuthorityDeadline(t, time.Second)) })
	if err := c.StartAuth(brokerAuthorityDeadline(t, 3*time.Second)); err != nil {
		t.Fatal(err)
	}
	current := brokerJS(b)
	if c.auth == nil || c.auth.UsableBootNS == 0 || current == nil || current.Conn() != nc || current.Conn().IsClosed() {
		t.Fatal("auth left resumed owners bound to a retired JetStream transport")
	}
}
