//go:build d6_integration

package d6_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/tunnel"
)

const (
	hSID = "lab"
	hNID = "lab-1"
)

func newAgentClient(t *testing.T, fallbackAddr string, echoPort int) *tunnel.Client {
	t.Helper()
	cli := tunnel.NewClient(fallbackAddr, hSID, hNID, func(int) (int, error) { return echoPort, nil }, silent())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cli.Start(ctx)
	return cli
}

// TestD6LadderEnforcedEndToEnd: with a row homed to node-A, the REAL home A
// (selfID=node-A) authorizes the agent and the data plane flows; a different
// broker B (selfID=node-B) reading the SAME replicated row TERMINALLY denies the
// same REGISTER — exactly one broker is the home.
func TestD6LadderEnforcedEndToEnd(t *testing.T) {
	db := openSharedDB(t)
	echo := startEcho(t)
	publicPort := freePort(t)
	const token = "tok-ladder-deadbeef"

	homeA := newHomeBroker(t, db, "node-A")
	t.Cleanup(homeA.stop)
	homeB := newHomeBroker(t, db, "node-B")
	t.Cleanup(homeB.stop)
	seedClusterNode(t, db, "node-A", "tether-1", homeA.addr, homeA.certFP)
	seedHomedExpose(t, db, hSID, hNID, "svc", publicPort, echo, token, "node-A", 0)

	cli := newAgentClient(t, homeA.addr, echo)
	if err := cli.OpenHome(publicPort, echo, token, homeA.addr, 0, pinsFor(homeA)); err != nil {
		t.Fatalf("OpenHome to A: %v", err)
	}
	waitEcho(t, publicPort, "hello-A", 5*time.Second)

	// Broker B is NOT the home for this row → terminal deny (anti-enum code).
	err := homeB.b.TunnelTokenLookupForTest()(hSID, hNID, publicPort, sha256hex(token), 0)
	if err == nil || err.Error() != "token_unknown_or_revoked" {
		t.Fatalf("non-home broker must terminally deny, got %v", err)
	}
}

// TestD6RehomeAtoB: home A dies; the leader re-points the row to node-B (epoch
// bumped); the agent rehomes (ApplyHome) and the data plane flows through B on
// the SAME public port.
func TestD6RehomeAtoB(t *testing.T) {
	db := openSharedDB(t)
	echo := startEcho(t)
	publicPort := freePort(t)
	const token = "tok-rehome-cafebabe"

	homeA := newHomeBroker(t, db, "node-A")
	homeB := newHomeBroker(t, db, "node-B")
	t.Cleanup(homeB.stop)
	seedClusterNode(t, db, "node-A", "tether-1", homeA.addr, homeA.certFP)
	seedClusterNode(t, db, "node-B", "tether-2", homeB.addr, homeB.certFP)
	seedHomedExpose(t, db, hSID, hNID, "svc", publicPort, echo, token, "node-A", 0)

	cli := newAgentClient(t, homeA.addr, echo)
	if err := cli.OpenHome(publicPort, echo, token, homeA.addr, 0, pinsFor(homeA)); err != nil {
		t.Fatalf("OpenHome to A: %v", err)
	}
	waitEcho(t, publicPort, "via-A", 5*time.Second)

	// Home A dies (releases the public port); the leader re-points to B @ epoch 1.
	homeA.stop()
	reseedHome(t, db, publicPort, "node-B", 1)

	// RTO bound (review A6 M2 / §4.5): the rehome data plane must converge within
	// a generous wall-time budget so a regression that blows the backoff/dial
	// budget is caught.
	t0 := time.Now()
	if err := cli.ApplyHome(publicPort, homeB.addr, 1, pinsFor(homeB)); err != nil {
		t.Fatalf("ApplyHome to B: %v", err)
	}
	waitEcho(t, publicPort, "via-B", 45*time.Second)
	if rto := time.Since(t0); rto > 45*time.Second {
		t.Fatalf("rehome RTO %v exceeded the budget", rto)
	}
}

// TestD6PerExposeScatter (§7.5 / review A6 M1): ONE agent with TWO exposes homed
// to DIFFERENT brokers (A and B) — each clientSession dials its own home; the
// single Client fans out to N homes and both data planes flow independently.
func TestD6PerExposeScatter(t *testing.T) {
	db := openSharedDB(t)
	echo := startEcho(t)
	portA := freePort(t)
	portB := freePort(t)
	const tokA, tokB = "tok-scatter-aaaa", "tok-scatter-bbbb"

	homeA := newHomeBroker(t, db, "node-A")
	t.Cleanup(homeA.stop)
	homeB := newHomeBroker(t, db, "node-B")
	t.Cleanup(homeB.stop)
	seedHomedExpose(t, db, hSID, hNID, "svcA", portA, echo, tokA, "node-A", 0)
	seedHomedExpose(t, db, hSID, hNID, "svcB", portB, echo, tokB, "node-B", 0)

	cli := newAgentClient(t, homeA.addr, echo)
	if err := cli.OpenHome(portA, echo, tokA, homeA.addr, 0, pinsFor(homeA)); err != nil {
		t.Fatalf("OpenHome A: %v", err)
	}
	if err := cli.OpenHome(portB, echo, tokB, homeB.addr, 0, pinsFor(homeB)); err != nil {
		t.Fatalf("OpenHome B: %v", err)
	}
	waitEcho(t, portA, "scatter-A", 5*time.Second)
	waitEcho(t, portB, "scatter-B", 5*time.Second)
}

// TestD6CertPinRejectsRogue: an agent that pins a DIFFERENT cert than the home
// presents fails the TLS handshake BEFORE writing the REGISTER token — the data
// plane never comes up and the token is never disclosed.
func TestD6CertPinRejectsRogue(t *testing.T) {
	db := openSharedDB(t)
	echo := startEcho(t)
	publicPort := freePort(t)
	const token = "tok-pin-feedface"

	homeA := newHomeBroker(t, db, "node-A")
	t.Cleanup(homeA.stop)
	// A SECOND cert the agent will wrongly pin (not the one A serves).
	_, _, wrongFP := genStableCert(t)
	seedClusterNode(t, db, "node-A", "tether-1", homeA.addr, homeA.certFP)
	seedHomedExpose(t, db, hSID, hNID, "svc", publicPort, echo, token, "node-A", 0)

	cli := newAgentClient(t, homeA.addr, echo)
	err := cli.OpenHome(publicPort, echo, token, homeA.addr, 0, proto.CertPins{Current: wrongFP})
	if err == nil {
		t.Fatal("OpenHome with a mismatched pin must fail the handshake")
	}
	if _, derr := dialEcho(publicPort, "should-not-flow"); derr == nil {
		t.Fatal("public port must NOT be served after a pin-rejected dial")
	}
}

// TestD6CatchUpTransientEndToEnd: presenting an epoch HIGHER than the home's
// local row (the home has not yet applied the reassign) yields the TRANSIENT
// home_catching_up deny — never terminal.
func TestD6CatchUpTransientEndToEnd(t *testing.T) {
	db := openSharedDB(t)
	echo := startEcho(t)
	publicPort := freePort(t)
	const token = "tok-catchup-0ddba11"

	homeA := newHomeBroker(t, db, "node-A")
	t.Cleanup(homeA.stop)
	seedClusterNode(t, db, "node-A", "tether-1", homeA.addr, homeA.certFP)
	seedHomedExpose(t, db, hSID, hNID, "svc", publicPort, echo, token, "node-A", 0)

	cli := newAgentClient(t, homeA.addr, echo)
	// Agent presents epoch 1; the home's row is still epoch 0 → home_catching_up.
	err := cli.OpenHome(publicPort, echo, token, homeA.addr, 1, pinsFor(homeA))
	var de *tunnel.DenyError
	if !errors.As(err, &de) || de.Reason != proto.ReasonHomeCatchingUp {
		t.Fatalf("want transient home_catching_up DENY, got %v", err)
	}
}

// TestD6ConcurrentRehomeRace (R-13): many concurrent ApplyHome (Open-replace)
// calls on one port, with the supervisor running, must be race-clean (the
// supervisor reads brokerAddr/epoch/certPins as VALUE PARAMS, never back from the
// shared sessions map). The highest epoch wins (epoch-monotone replace) and the
// data plane converges on it. Run under -race in TestD6Matrix.
func TestD6ConcurrentRehomeRace(t *testing.T) {
	db := openSharedDB(t)
	echo := startEcho(t)
	publicPort := freePort(t)
	const token = "tok-race-12345678"

	homeA := newHomeBroker(t, db, "node-A")
	homeB := newHomeBroker(t, db, "node-B")
	t.Cleanup(homeB.stop)
	seedClusterNode(t, db, "node-A", "tether-1", homeA.addr, homeA.certFP)
	seedClusterNode(t, db, "node-B", "tether-2", homeB.addr, homeB.certFP)
	seedHomedExpose(t, db, hSID, hNID, "svc", publicPort, echo, token, "node-A", 0)

	cli := newAgentClient(t, homeA.addr, echo)
	if err := cli.OpenHome(publicPort, echo, token, homeA.addr, 0, pinsFor(homeA)); err != nil {
		t.Fatalf("OpenHome to A: %v", err)
	}
	waitEcho(t, publicPort, "pre-race", 5*time.Second)

	// A dies (frees the port); the row moves to B at the top epoch.
	homeA.stop()
	const top = 8
	reseedHome(t, db, publicPort, "node-B", top)

	var wg sync.WaitGroup
	for e := 1; e <= top; e++ {
		wg.Add(1)
		go func(epoch int64) {
			defer wg.Done()
			_ = cli.ApplyHome(publicPort, homeB.addr, epoch, pinsFor(homeB))
		}(int64(e))
	}
	wg.Wait()
	// The winning (highest-epoch) session serves B; data plane converges.
	waitEcho(t, publicPort, "post-race", 8*time.Second)
}
