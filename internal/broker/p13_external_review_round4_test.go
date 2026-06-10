package broker

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/node"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/nats-io/nats.go"
)

func TestExternalReviewBrokerGenerationSurvivesClockRollback(t *testing.T) {
	db := openDB(t)

	first, err := New(Config{
		NATSURL: "nats://unused",
		DB:      db,
		Now:     func() time.Time { return time.Unix(200, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := New(Config{
		NATSURL: "nats://unused",
		DB:      db,
		// The second broker starts later in real process order, but its wall
		// clock has moved backwards (NTP/manual correction/VM restore).
		Now: func() time.Time { return time.Unix(100, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}

	if second.proxyGen <= first.proxyGen {
		t.Fatalf(
			"later broker generation=%d did not advance past prior generation=%d after clock rollback",
			second.proxyGen, first.proxyGen,
		)
	}
}

func TestExternalReviewHeartbeatRepairsGenerationMismatchAtSameEpoch(t *testing.T) {
	nc, _, sid, b := proxyTestBroker(t)
	if err := node.Register(b.cfg.DB, node.RegisterInput{
		SID: sid, NID: "lab-1", ProtoVersion: proto.ProtoVersion,
		ReleaseVersion: "v0.2.9", ProxyCapable: true,
	}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := session.SetProxyEnabled(b.cfg.DB, sid, true); err != nil {
		t.Fatal(err)
	}
	epoch, err := session.BumpProxyEpoch(b.cfg.DB, sid)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.SetProxyReady(b.cfg.DB, sid, "lab-1", true); err != nil {
		t.Fatal(err)
	}

	ch := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded(sid, "lab-1", "proxy-keys"), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	// Model a ready agent that still has the prior broker incarnation's state.
	// Epoch alone is insufficient: two DB snapshots can carry the same scalar
	// epoch while containing different active subscriber keys.
	body, err := json.Marshal(map[string]any{
		"ts":               time.Now(),
		"proxy_generation": b.proxyGen - 1,
		"proxy_epoch":      epoch,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := nc.Publish(proto.SubjNodeHeartbeat(sid, "lab-1"), body); err != nil {
		t.Fatal(err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	select {
	case msg := <-ch:
		var d proto.ProxyDirective
		if err := json.Unmarshal(msg.Data, &d); err != nil {
			t.Fatal(err)
		}
		if d.Generation != b.proxyGen || d.Epoch != epoch {
			t.Fatalf("repair directive=%+v want generation=%d epoch=%d", d, b.proxyGen, epoch)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("same-epoch heartbeat from an older generation was treated as converged")
	}
}
