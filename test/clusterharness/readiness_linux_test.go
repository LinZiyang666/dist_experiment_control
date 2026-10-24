//go:build linux

package clusterharness

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/testharness"
	server "github.com/nats-io/nats-server/v2/server"
)

// The fixture entry must consume the witness before launching any broker. Real
// NATS listeners without JetStream must retain the bounded startup refusal;
// calling a disconnected readiness helper only from tests would not fix boot.
// origin: hot-update S6 plan §5 / §10.1 / HARDGATE-B request 3
func TestBrokerQuorumStartupRequiresInfrastructureWitness(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseBrokerQuorumInfrastructureRefusal(t)
	}
	testharness.AssertNoGoroutineLeak(t, "quorum infrastructure startup refusal", g)
	testharness.AssertNoFDLeak(t, "quorum infrastructure startup refusal", fd, 0)
}

func exerciseBrokerQuorumInfrastructureRefusal(t *testing.T) {
	t.Helper()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	q := &productQuorum{brokerSeed: seed}
	f := &brokerProductFixture{test: t, quorum: q}
	defer func() {
		if err := q.close(); err != nil {
			t.Error(err)
		}
		if f.nats != nil {
			f.nats.Shutdown()
			f.nats.WaitForShutdown()
		}
	}()
	for i := 0; i < 3; i++ {
		s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, Nkeys: []*server.NkeyUser{{Nkey: pub}}})
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			f.nats = s
		} else {
			q.nats = append(q.nats, s)
		}
		s.Start()
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatal("unready infrastructure client listener expired")
		}
	}
	// No child is launched in this refusal case. There is deliberately no
	// executable supplied: infrastructure failure must precede process setup.
	err = q.start(f, "")
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "quorum infrastructure readiness") {
		t.Fatalf("quorum fixture bypassed the real infrastructure refusal: %v", err)
	}
}
