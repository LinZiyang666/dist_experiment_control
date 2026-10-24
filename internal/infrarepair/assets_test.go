package infrarepair

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func awaitFence(t *testing.T, g *Gate) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		g.mu.Lock()
		closed := g.closed
		g.mu.Unlock()
		if closed {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("seal never closed admission")
		}
	}
}

// origin: hot-update S9 plan §3.2 / T41 / M20
func TestAssetSealFencesEveryProducer(t *testing.T) {
	t.Run("joins_real_asset_request", func(t *testing.T) {
		f := newRecovery(t)
		defer f.close()
		permit, err := f.gate.Enter("transfer")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "late", Subjects: []string{"late"}, Storage: jetstream.FileStorage}); err != nil {
			t.Fatal(err)
		}

		readAssets := f.gate.cfg.Assets
		inspected := false
		f.gate.cfg.Assets = func(d c.Deadline, stop c.Stop) (c.AssetSeal, error) { inspected = true; return readAssets(d, stop) }
		deadline, err := controlbudget.After(30 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.gate.Seal(f.plan, deadline)
		if inspected {
			permit.Finish(nil)
			t.Fatal("inventory read began before admitted producer joined")
		}
		if !controlbudget.IsExpired(err) {
			permit.Finish(nil)
			t.Fatalf("unjoined seal did not consume deadline: %v", err)
		}
		permit.Finish(nil)
		if _, err = f.gate.Seal(f.plan, recoveryDeadline(t)); err != nil {
			t.Fatal(err)
		}

		for _, name := range []string{"transfer", "unregistered-consumer"} {
			p, err := f.gate.Enter(name)
			if err == nil {
				p.Finish(nil)
				t.Fatalf("sealed producer admitted: %s", name)
			}
		}
		if err = f.gate.Register(Producer{Name: "late-owner", Capture: f.source.Capture, Check: f.source.Check}); !errors.Is(err, ErrFenced) {
			t.Fatalf("late owner: %v", err)
		}
	})
	t.Run("missing_producer", func(t *testing.T) {
		f := newRecovery(t)
		defer f.close()
		f.gate.cfg.Required = append(f.gate.cfg.Required, "audit-replay")
		if _, err := f.gate.Seal(f.plan, recoveryDeadline(t)); !errors.Is(err, c.ErrIncomplete) {
			t.Fatalf("missing producer certified: %v", err)
		}
	})
	t.Run("foreign_writer", func(t *testing.T) {
		f := newRecovery(t)
		defer f.close()
		f.gate.cfg.Exclusive = func(c.Deadline, c.Stop) error { return c.ErrIncomplete }
		if _, err := f.gate.Seal(f.plan, recoveryDeadline(t)); !errors.Is(err, c.ErrIncomplete) {
			t.Fatalf("foreign writer certified: %v", err)
		}
	})
	t.Run("create_delete_recreate", func(t *testing.T) {
		f := newRecovery(t)
		defer f.close()
		cp := sealRecovery(t, f)
		old, err := f.js.Stream(t.Context(), "audit")
		if err != nil {
			t.Fatal(err)
		}
		info, err := old.Info(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		original := make([]*jetstream.RawStreamMsg, 0, 2)
		for seq := uint64(1); seq <= 2; seq++ {
			msg, err := old.GetMsg(t.Context(), seq)
			if err != nil {
				t.Fatal(err)
			}
			original = append(original, msg)
		}
		if err = f.js.DeleteStream(t.Context(), "audit"); err != nil {
			t.Fatal(err)
		}
		replacement, err := f.js.CreateStream(t.Context(), info.Config)
		if err != nil {
			t.Fatal(err)
		}
		// Recreate the same names, exact sequence numbers, headers and payloads.
		// Only the asset's physical creation identity exposes this replacement.
		for _, msg := range original {
			ack, err := f.js.PublishMsg(t.Context(), &nats.Msg{Subject: msg.Subject, Header: msg.Header, Data: msg.Data})
			if err != nil {
				t.Fatal(err)
			}
			if ack.Sequence != msg.Sequence {
				t.Fatal("replacement sequence differs")
			}
		}
		after, err := replacement.Info(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if after.Created.Equal(info.Created) {
			t.Fatal("replacement kept creation identity")
		}
		if err = f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), nil); !errors.Is(err, ErrReceipt) {
			t.Fatalf("recreated asset accepted: %v", err)
		}
	})
	t.Run("revision_remembers_configuration_ABA", func(t *testing.T) {
		f := newRecovery(t)
		defer f.close()
		before, err := f.gate.ReadSeal(f.plan, recoveryDeadline(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		permit, err := f.gate.Enter("transfer")
		if err != nil {
			t.Fatal(err)
		}
		if _, err = f.js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "temporary", Subjects: []string{"temporary"}, Storage: jetstream.FileStorage}); err != nil {
			t.Fatal(err)
		}
		if err = f.js.DeleteStream(t.Context(), "temporary"); err != nil {
			t.Fatal(err)
		}
		permit.Finish(nil)
		after, err := f.gate.ReadSeal(f.plan, recoveryDeadline(t), nil)
		if err != nil {
			t.Fatal(err)
		}
		if before.Assets != after.Assets {
			t.Fatal("ABA fixture did not restore original inventory")
		}
		if before.Revision == after.Revision {
			t.Fatal("asset revision forgot a complete create/delete cycle")
		}
	})
	t.Run("sent_unknown_requires_transport_retirement", func(t *testing.T) { exerciseUnknownAsset(t) })
}

func exerciseUnknownAsset(t *testing.T) {
	t.Helper()
	f := newRecovery(t)
	defer f.close()
	// Forward the real create request to the embedded server, then drop only the
	// successful response. The actual stream exists before the client times out.
	forwarded := make(chan error, 1)
	sub, err := f.nc.Subscribe("held.>", func(msg *nats.Msg) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := f.nc.RequestWithContext(ctx, "$JS.API."+strings.TrimPrefix(msg.Subject, "held."), msg.Data)
		forwarded <- err
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err = f.nc.Flush(); err != nil {
		t.Fatal(err)
	}
	lossyNC, err := nats.Connect(f.server.ClientURL(), nats.UserInfo("business", "test"), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer lossyNC.Close()
	lossy, err := jetstream.NewWithAPIPrefix(lossyNC, "held")
	if err != nil {
		t.Fatal(err)
	}
	bound, err := BindLegacy(lossy, f.gate, "business")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	err = RunLegacy(ctx, bound, "transfer", func(ctx context.Context) error {
		_, err := bound.CreateStream(ctx, jetstream.StreamConfig{Name: "unknown", Subjects: []string{"unknown"}, Storage: jetstream.FileStorage})
		return err
	})
	if err == nil {
		t.Fatal("dropped reply unexpectedly acknowledged")
	}
	if err = <-forwarded; err != nil {
		t.Fatal(err)
	}
	if _, err = f.js.Stream(t.Context(), "unknown"); err != nil {
		t.Fatalf("lost-ACK request never took effect: %v", err)
	}
	if _, err = f.gate.Seal(f.plan, recoveryDeadline(t)); !errors.Is(err, ErrUnknown) {
		t.Fatalf("sent-unknown asset certified: %v", err)
	}
	// The owner must retire its actual request transport. Use a separate read NC
	// for subsequent management verification; no producer is resumed here.
	retired := false
	f.gate.producers["transfer"] = Producer{Name: "transfer", Capture: f.source.Capture, Check: f.source.Check, Retire: func(d c.Deadline, stop c.Stop) error {
		if err := admission(d, stop); err != nil {
			return err
		}
		lossyNC.Close()
		if err := sub.Unsubscribe(); err != nil {
			return err
		}
		if err := f.nc.Flush(); err != nil {
			return err
		}
		retired = true
		return nil
	}}
	if err = f.gate.RetireUnknown("transfer", recoveryDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if !retired {
		t.Fatal("unknown request vanished without retirement")
	}
	if _, err = f.gate.Seal(f.plan, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §4.3 / T40 / T41
func TestRecoveryDeadlineAndUnknownRetainFence(t *testing.T) {
	f := newRecovery(t)
	defer f.close()
	permit, err := f.gate.Enter("transfer")
	if err != nil {
		t.Fatal(err)
	}
	d, err := controlbudget.After(time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.gate.Seal(f.plan, d); !controlbudget.IsExpired(err) {
		t.Fatalf("unjoined deadline: %v", err)
	}
	permit.Finish(nil)
	cp := sealRecovery(t, f)
	if err = f.gate.Release(cp, recoveryDeadline(t)); err == nil {
		t.Fatal("expired join authorized release")
	}
	// A real closed management connection is unknown, never a successful empty
	// receipt read or a terminal transfer failure. All producers remain fenced.
	f.nc.Close()
	if err = f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), nil); err == nil {
		t.Fatal("unavailable JetStream verified")
	}
	if p, err := f.gate.Enter("transfer"); err == nil {
		p.Finish(nil)
		t.Fatal("unavailable JetStream opened repair")
	}
}

// Keep parallel helper ownership observable with a real joined callback.
// origin: hot-update S9 plan §4.3 / T40
func TestLegacyWorkersJoinBeforePermitEnds(t *testing.T) {
	f := newRecovery(t)
	defer f.close()
	bound, err := BindLegacy(f.js, f.gate, "business")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	completed := 0
	err = RunLegacy(t.Context(), bound, "transfer", func(ctx context.Context) error {
		return ParallelSessions(ctx, []string{"one", "two", "three"}, 2, func(ctx context.Context, sid string) error {
			_, err := bound.CreateStream(ctx, jetstream.StreamConfig{Name: sid, Subjects: []string{sid}, Storage: jetstream.FileStorage})
			mu.Lock()
			completed++
			mu.Unlock()
			return err
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if completed != 3 {
		t.Fatalf("joined %d/3 callbacks", completed)
	}
	sealRecovery(t, f)
}
