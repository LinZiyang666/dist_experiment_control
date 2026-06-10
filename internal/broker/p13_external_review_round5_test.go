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

func TestExternalReviewBrokerRefusesUnpersistedGeneration(t *testing.T) {
	db := openDB(t)

	first, err := New(Config{
		NATSURL: "nats://unused",
		DB:      db,
		Now:     func() time.Time { return time.Unix(200, 0) },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DROP TABLE proxy_meta`); err != nil {
		t.Fatal(err)
	}

	second, err := New(Config{
		NATSURL: "nats://unused",
		DB:      db,
		Now:     func() time.Time { return time.Unix(100, 0) },
	})
	if err == nil {
		t.Fatalf(
			"broker started without durable fencing: prior generation=%d fallback generation=%d",
			first.proxyGen, second.proxyGen,
		)
	}
}

func TestExternalReviewHeartbeatEscalatesPastRestoredAgentGeneration(t *testing.T) {
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

	// Model a broker restored from an older DB snapshot while an agent still
	// holds a generation issued before the restore.
	b.proxyGen = 100
	const agentGen int64 = 200

	ch := make(chan *nats.Msg, 1)
	sub, err := nc.ChanSubscribe(proto.SubjCmdForwarded(sid, "lab-1", "proxy-keys"), ch)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := nc.Flush(); err != nil {
		t.Fatal(err)
	}

	body, err := json.Marshal(proto.HeartbeatPayload{
		Ts:              time.Now(),
		ProxyGeneration: agentGen,
		ProxyEpoch:      epoch,
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
		if d.Generation <= agentGen {
			t.Fatalf(
				"restored broker pushed generation=%d behind applied agent generation=%d",
				d.Generation, agentGen,
			)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("restored generation mismatch produced no repair directive")
	}
}
