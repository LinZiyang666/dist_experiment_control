package main

import (
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/broker"
	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/stackharness"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Configured capability cannot become cap0 while the real broker waits for its
// graph factory. The client must refuse that unavailable probe before creation.
// origin: hot-update S7 plan §6.1 / §9 / INTEG-D2 startup downgrade regression
// origin: S7 internal review MAKETEST-TRANSFER F2
func TestTransferCapabilitiesNeverDowngradeDuringStartup(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("startup", func(t *testing.T) { exerciseTransferStartup(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer startup", g)
	testharness.AssertNoFDLeak(t, "transfer startup", fd, 0)
}

func exerciseTransferStartup(t *testing.T) {
	t.Helper()
	url, db := startJetStreamNATS(t), testharness.OpenDB(t)
	id, err := cli.EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = session.Create(db, "lab", "lab", id.Fingerprint, stackharness.SessionPINHash(t), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	b, err := broker.New(broker.Config{NATSURL: url, DB: db, Logger: testharness.SilentLog(), TransferFactory: func(*broker.Broker, jetstream.JetStream, controlbudget.Deadline) (*broker.BrokerTransfers, error) {
		close(entered)
		<-release
		return nil, errors.New("injected graph startup refusal")
	}})
	if err != nil {
		t.Fatal(err)
	}
	stop := runCapableOwner(t, "starting broker", b.Run)
	defer stop()
	defer close(release)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("broker never entered transfer graph construction")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	cfg, closeClient, err := cli.TransferClientConfig(nc, url, id, "lab")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeClient(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg.Identity = x.Identity{SID: "lab", NID: "node", ActorFP: id.Fingerprint, TransferID: "startup-transfer"}
	caps, err := cli.ProbeTransferCaps(t.Context(), nc, id.PublicKey, "lab", time.Second)
	if err != nil || !caps.OK || caps.TransferVersion != proto.TransferVersion {
		t.Fatalf("starting capable broker advertised legacy: %+v %v", caps, err)
	}
	d, err := controlbudget.After(200 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := cli.SelectTransferClient(&cfg, d)
	if legacy || err == nil {
		t.Fatalf("unready graph selected a working invocation: legacy=%t err=%v", legacy, err)
	}
	// A not-yet-installed census is retained responsibility, including when the
	// operator asks to abandon: the original owner set is still unknown.
	// origin: hot-update S7 plan lead LQ1 / §6.4 / T31
	for _, body := range []string{`{}`, `{"abandon_unsettled_transfers":true}`} {
		msg, err := nc.Request(proto.SubjCtrlSessionRm(id.PublicKey, "lab"), []byte(body), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		var response proto.SessionRmResp
		if err := json.Unmarshal(msg.Data, &response); err != nil || response.OK || response.Code != x.CodeOwnerRecovering {
			t.Fatalf("unready census allowed session removal: %+v %v", response, err)
		}
		var state string
		if err := db.QueryRow(`SELECT state FROM sessions WHERE sid = 'lab'`).Scan(&state); err != nil || state != "DELETING" {
			t.Fatalf("unready census lost the session: state=%q err=%v", state, err)
		}
	}
}
