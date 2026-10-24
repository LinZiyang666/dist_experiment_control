package infraattach

import (
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrarepair"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// broker_recovery_test.go covers the broker's P18 composition beyond the ACK
// prefix (broker_test.go): retirement of the shared producer transport.

// A sent-unknown producer request — here a JetStream publish that got no
// stream response; any JS timeout behaves alike — must not wedge the domain.
// P18 keeps a gate closed after a failed join, so without a retirement the
// next Seal fails and every broker producer stays fenced until restart. The
// broker retires the shared business transport once (a fresh socket under a
// fresh auth generation) and seals the same plan again.
// origin: S9 P18 request "transport retirement"; S9 P22 resume
func TestBrokerRecoveryRetiresUnknownProducerTransport(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "original data retirement", goroutines)
	defer testharness.AssertNoFDLeak(t, "original data retirement", fds, 0)
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseBrokerUnknownRetirement(t) })
	}
}

func exerciseBrokerUnknownRetirement(t *testing.T) {
	f := newBrokerAttachFixture(t, true)
	ports, admin := recoveryPorts(t, f)
	if _, err := admin.CreateStream(t.Context(), jetstream.StreamConfig{Name: "AUDITU", Subjects: []string{"auditu.>"}, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	f.open(t, ports)
	f.arm(t)
	rawJS, err := jetstream.New(f.nc)
	if err != nil {
		t.Fatal(err)
	}
	js, err := f.owner.BindJetStream(rawJS, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := PublishAudit(f.owner, js, &nats.Msg{Subject: "auditu.original", Data: []byte(`{}`)}, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	// No stream binds this subject: the client sees no stream response, which
	// carries no server-side retirement evidence — an unknown result.
	if err := PublishAudit(f.owner, js, &nats.Msg{Subject: "unbound.audit", Data: []byte(`{}`)}, 2*time.Second); err == nil {
		t.Fatal("an unbound publish was acknowledged")
	}
	plan := c.Plan{Tx: c.ID{4}, Domain: ports.Domain, Component: c.NATS}
	// Non-vacuity: P18 alone refuses the seal over that request and keeps the
	// gate closed — exactly the wedge the broker composition must clear.
	gate, _ := f.owner.recovery()
	// An original independent data NC is not one of LegacyRoots. Its unknown
	// request must run its own retirement, in addition to the shared business
	// retirement, before the broker may retry the same sealed generation.
	// origin: S9 internal review NC-EXPORTS handoff original ACK registration
	dataNC, err := nats.Connect(f.url(), nats.Nkey(f.pubs["member"], f.keys["member"].Sign), nats.Name("tether-cli:"+attachSID), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(dataNC.Close)
	if err = gate.Register(infrarepair.Producer{Name: "transfer:original", Capture: func(c.Deadline) (infrarepair.Responsibilities, error) { return infrarepair.Responsibilities{}, nil },
		Check: func(infrarepair.Responsibilities, c.Deadline, c.Stop) error { return nil }, Retire: func(c.Deadline, c.Stop) error { dataNC.Close(); return nil }}); err != nil {
		t.Fatal(err)
	}
	dataJS, err := jetstream.New(dataNC)
	if err != nil {
		t.Fatal(err)
	}
	permit, err := gate.Enter("transfer:original")
	if err != nil {
		t.Fatal(err)
	}
	_, unknown := dataJS.Publish(t.Context(), "unbound.original-transfer", []byte("original pending request"), jetstream.WithRetryAttempts(0))
	permit.Finish(unknown)
	if unknown == nil || !dataNC.IsConnected() {
		t.Fatal("independent original did not retain an unknown live transport")
	}
	if _, err := gate.Seal(plan, brokerAttachDeadline(t, 5*time.Second)); !errors.Is(err, infrarepair.ErrUnknown) {
		t.Fatalf("the sent-unknown request did not reach the gate: %v", err)
	}
	if err := PublishAudit(f.owner, js, &nats.Msg{Subject: "auditu.wedged", Data: []byte(`{}`)}, time.Second); !errors.Is(err, infrarepair.ErrFenced) {
		t.Fatalf("a failed join reopened producers: %v", err)
	}
	business := f.cid(t, "tetherd")
	_, before, err := f.owner.auth.Business()
	if err != nil {
		t.Fatal(err)
	}
	recovery := f.owner.Recovery()
	cp, err := recovery.Seal(plan, brokerAttachDeadline(t, 10*time.Second))
	if err != nil {
		t.Fatalf("a past sent-unknown request wedged the broker's seal: %v", err)
	}
	if !dataNC.IsClosed() {
		t.Fatal("broker omitted dynamically registered original transfer retirement")
	}
	// The retirement was real: a fresh auth generation, and the old business
	// socket has left the server.
	_, after, err := f.owner.auth.Business()
	if err != nil || after.Generation <= before.Generation {
		t.Fatalf("seal passed without re-proving the business transport: before=%d after=%d err=%v", before.Generation, after.Generation, err)
	}
	waitAttach(t, "retired business socket leaves the server", func() bool {
		cid := f.cid(t, "tetherd")
		return cid != 0 && cid != business
	})
	if err := recovery.VerifyBeforeReplay(cp, brokerAttachDeadline(t, 5*time.Second), nil); err != nil {
		t.Fatal(err)
	}
	if err := recovery.Release(cp, brokerAttachDeadline(t, 5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := PublishAudit(f.owner, js, &nats.Msg{Subject: "auditu.after", Data: []byte(`{}`)}, 2*time.Second); err != nil {
		t.Fatalf("released producer still fenced: %v", err)
	}
}
