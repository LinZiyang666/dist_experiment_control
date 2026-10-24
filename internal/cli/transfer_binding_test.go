package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This exercises the production factories with embedded JetStream, raw fixed
// NUID metadata and a real atomic destination. Broker admission is tested by
// its own owner; this does not replace that unbound routing dependency.
// origin: hot-update S7 plan §2–§3 / §7 / T32–T34 / INTEG-D
func TestTransferClientFactoriesUseDedicatedReceiptIO(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferFactories(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer factory bindings", g)
	testharness.AssertNoFDLeak(t, "transfer factory bindings", fd, 0)
}
func exerciseTransferFactories(t *testing.T) {
	t.Helper()
	s, control, identity, stop := transferConnectionFixture(t)
	defer stop()
	for _, invalid := range []struct {
		nc  *nats.Conn
		id  *Identity
		url string
		sid string
	}{
		{nil, identity, s.ClientURL(), "session"},
		{control, nil, s.ClientURL(), "session"},
		{control, identity, "", "session"},
		{control, identity, s.ClientURL(), ""},
	} {
		if _, cleanup, err := TransferClientConfig(invalid.nc, invalid.url, invalid.id, invalid.sid); err == nil || cleanup != nil {
			t.Fatal("partial factory construction returned an owner")
		}
	}
	cfg, closeOwner, err := TransferClientConfig(control, s.ClientURL(), identity, "session")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	if cfg.MakeWriter == nil || cfg.MakeReader == nil || cfg.MakeCourier == nil {
		t.Fatal("production receipt IO or courier factory is unbound")
	}
	if _, err := cfg.Capacity.Reserve(x.Reservation{NonReplayBytes: 64<<20 + 1}); err == nil {
		t.Fatal("factory permits unaccounted client buffers")
	}
	cfg.Identity.NID, cfg.Identity.TransferID, cfg.Identity.AgentInstance = "node", "business", "agent"
	cfg.Identity.EntryNonce, cfg.Identity.IntentDigest, cfg.Identity.ObjectVersion = "entry", strings.Repeat("a", 64), "fixedNUID"
	d := retryDeadline(t, 5*time.Second)
	js, err := cfg.Transport.OpenData(cfg.Identity, d)
	if err != nil {
		t.Fatal(err)
	}
	transport := cfg.Transport.(*TransferTransport)
	if transport.data == control || transport.data.Opts.ReconnectBufSize != -1 {
		t.Fatal("production byte factory reused control or buffered reconnect bytes")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	bucket := proto.XferBucketName("session")
	store, err := js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	si, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sc := si.Config
	sc.Duplicates = proto.ObjectRecoveryHorizon
	if _, err = js.UpdateStream(ctx, sc); err != nil {
		t.Fatal(err)
	}
	data := bytes.Repeat([]byte("receipt"), 5000)
	sum := sha256.Sum256(data)
	spec := x.ObjectSpec{Identity: cfg.Identity, Bucket: bucket, Stream: sc.Name, StreamCreated: si.Created.UTC().Format(time.RFC3339Nano), Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), ChunkSize: 32768, Chunks: 2}
	writer, err := cfg.MakeWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := writer.Stop(d); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	for index, offset := uint32(0), 0; offset < len(data); index++ {
		end := min(offset+32768, len(data))
		if _, err = writer.Append(index, data[offset:end], d); err != nil {
			t.Fatal(err)
		}
		offset = end
	}
	if _, err = writer.Finalize(d); err != nil {
		t.Fatal(err)
	}
	meta, err := store.GetInfo(ctx, cfg.Identity.TransferID)
	if err != nil || meta.NUID != cfg.Identity.ObjectVersion {
		t.Fatalf("receipt writer replaced NUID: %+v %v", meta, err)
	}
	reader, err := cfg.MakeReader(js, spec, x.SinkCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Stop(d); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	path := filepath.Join(t.TempDir(), "landed")
	receiver, err := NewTransferReceiver(path, false)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := receiver.Stop(d); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	hash := sha256.New()
	var copied []byte
	for index := uint32(0); index < spec.Chunks; index++ {
		chunk, err := reader.ReadChunk(d)
		if err != nil {
			t.Fatal(err)
		}
		n, err := receiver.WriteAt(chunk.Data, chunk.Receipt.Offset)
		if err != nil || n != len(chunk.Data) {
			t.Fatalf("write: %d %v", n, err)
		}
		if err = receiver.Sync(); err != nil {
			t.Fatal(err)
		}
		copied = append(copied, chunk.Data...)
		_, _ = hash.Write(chunk.Data)
		state, _ := hash.(encoding.BinaryMarshaler).MarshalBinary()
		checkpoint := x.SinkCheckpoint{ChunkIndex: index + 1, DurableOffset: int64(len(copied)), LastConsumedStreamSeq: chunk.Receipt.StreamSeq, SHA256State: state}
		if err = reader.ConfirmSynced(checkpoint); err != nil {
			t.Fatal(err)
		}
	}
	grant := x.CommitGrant{Identity: cfg.Identity, GrantNonce: "grant", ReceiverInstance: "ctl-business", TerminalID: "terminal"}
	outcome := receiver.Commit(grant, d)
	if outcome.Outcome != x.OutcomeLanded || !outcome.ParentSynced {
		t.Fatalf("real primitive failed: %+v", outcome)
	}
	landed, err := os.ReadFile(path)
	if err != nil || !bytes.Equal(landed, data) {
		t.Fatal("factory reader/atomic binding changed bytes")
	}
	if _, err = cfg.Transport.OpenData(x.Identity{SID: "session", TransferID: "other"}, d); err == nil {
		t.Fatal("another business ID borrowed original data attachment")
	}
}

// origin: hot-update S7 plan §7 / T32 / INTEG-D
func TestTransferSelectionRequiresExplicitCapZero(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferSelection(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer selection", g)
	testharness.AssertNoFDLeak(t, "transfer selection", fd, 0)
}
func exerciseTransferSelection(t *testing.T) {
	t.Helper()
	s, control, id, stop := transferConnectionFixture(t)
	defer stop()
	cfg, closeOwner, err := TransferClientConfig(control, s.ClientURL(), id, "session")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := closeOwner(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg.Identity.NID, cfg.Identity.TransferID = "node", "original"
	path := filepath.Join(t.TempDir(), "source")
	data := bytes.Repeat([]byte("a"), 1025)
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	cfg.Source, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := cfg.Source.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cfg.Intent = x.Intent{Verb: "push", Path: "/tmp/target", Size: int64(len(data))}
	subject, _ := proto.SubjCtrlTransferRequest(id.PublicKey, "session", "node", "original", x.OpProbe)
	for _, mode := range []string{"cap0", "capable", "partial", "refused", "malformed", "missing", "caps-cap0", "caps-refused", "caps-missing"} {
		// The broker's caps verb comes first (§9): a transfer-v1 broker answers
		// its version, an old broker completely without one. Only that complete
		// answer is cap0 before any probe; an unanswered or refused caps request
		// is an error even when a capable probe responder exists.
		// origin: hot-update S7 plan §9 / INTEG-D capability compatibility
		capsVersion, capsCode := x.Version, ""
		switch mode {
		case "caps-cap0":
			capsVersion = ""
		case "caps-refused":
			capsCode = x.CodeNotMember
		}
		var caps *nats.Subscription
		if mode != "caps-missing" {
			caps = answerTransferCaps(t, control, id.PublicKey, "session", capsVersion, capsCode)
		}
		var sub *nats.Subscription
		if mode != "missing" && mode != "caps-cap0" {
			sub, err = control.Subscribe(subject, func(m *nats.Msg) {
				p := proto.TransferProbeResp{InstanceID: "agent", MaxPayload: 1 << 20, MaxMsgSize: 1 << 20}
				if mode == "capable" || strings.HasPrefix(mode, "caps-") {
					p.Version = x.Version
					p.HistoryGuardVersion = x.Version
					p.BucketConfigVersion = x.Version
				}
				if mode == "partial" {
					p.Version = x.Version
				}
				if mode == "refused" {
					p.Code = x.CodeNotMember
				}
				body, _ := json.Marshal(p)
				if mode == "malformed" {
					body = []byte(`{}`)
				}
				_ = m.Respond(body)
			})
			if err != nil {
				t.Fatal(err)
			}
		}
		if err = control.Flush(); err != nil {
			t.Fatal(err)
		}
		selected := cfg
		legacy, selectErr := SelectTransferClient(&selected, retryDeadline(t, time.Second))
		if sub != nil {
			if err = sub.Unsubscribe(); err != nil {
				t.Fatal(err)
			}
		}
		switch mode {
		case "cap0", "caps-cap0":
			if !legacy || selectErr != nil {
				t.Fatalf("explicit cap0 refused: %v", selectErr)
			}
		case "capable":
			if legacy || selectErr != nil || selected.Intent.Tier != "a" || selected.Intent.SHA256 == "" || selected.Identity.TransferID != "original" {
				t.Fatalf("capable selection lost source/identity: %+v %v", selected.Intent, selectErr)
			}
			// A slow rate must be refused before permit or first byte publication.
			selected = cfg
			selected.Intent.RateLimitBPS = 1
			sub, err = control.Subscribe(subject, func(m *nats.Msg) {
				p := proto.TransferProbeResp{Version: x.Version, HistoryGuardVersion: x.Version, BucketConfigVersion: x.Version, InstanceID: "agent", MaxPayload: 1 << 20, MaxMsgSize: 1 << 20}
				b, _ := json.Marshal(p)
				_ = m.Respond(b)
			})
			if err != nil {
				t.Fatal(err)
			}
			_ = control.Flush()
			legacy, selectErr = SelectTransferClient(&selected, retryDeadline(t, time.Second))
			_ = sub.Unsubscribe()
			if legacy || xferclient.ErrorCode(selectErr) != x.CodeBudgetExceeded {
				t.Fatalf("rate cap escaped precreation budget: %v", selectErr)
			}
		default:
			if legacy || selectErr == nil {
				t.Fatalf("%s was mistaken for explicit cap0", mode)
			}
		}
		if caps != nil {
			if err = caps.Unsubscribe(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// answerTransferCaps plays the broker's existing caps verb on the control
// connection: a transfer-v1 graph answers its version, "" is an old broker's
// complete cap0 answer, and a code is a refusal.
// origin: hot-update S7 plan §9 / INTEG-D capability compatibility
func answerTransferCaps(t *testing.T, control *nats.Conn, actor, sid, version, code string) *nats.Subscription {
	t.Helper()
	sub, err := control.Subscribe(proto.SubjCtrlCaps(actor, sid), func(m *nats.Msg) {
		body, _ := json.Marshal(proto.CapsResp{OK: code == "", JetStreamReady: true, MaxPayload: 1 << 20, TransferVersion: version, Code: code})
		_ = m.Respond(body)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = control.Flush(); err != nil {
		t.Fatal(err)
	}
	return sub
}
