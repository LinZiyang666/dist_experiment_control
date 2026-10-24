package infranats_test

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentprov"
	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/authcallout"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/infraauth"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infratransport"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/session"
	"github.com/LinZiyang666/tether/internal/storage"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nkeys"
)

// Adapt the real interruptible raw owner to the auth leaf's join contract.
// Interrupt joins its workers synchronously; no parallel test transport exists.
type authTransport struct{ *infratransport.Transport }

func (t authTransport) Cancel()                 { t.Interrupt() }
func (t authTransport) Join(d c.Deadline) error { _, err := controlbudget.Remaining(d); return err }

// This L1 cell runs the normal signed callout handler against actual activated
// member/provisioned agent rows. No auxiliary auth responder is installed.
// origin: hot-update S9 plan §4.4 / T59 / M19
func exerciseOrdinaryAuth(t *testing.T) {
	var keys []nkeys.KeyPair
	var pubs []string
	for range 3 {
		key, err := nkeys.CreateUser()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(key.Wipe)
		keys = append(keys, key)
		pub, err := key.PublicKey()
		if err != nil {
			t.Fatal(err)
		}
		pubs = append(pubs, pub)
	}
	account, err := nkeys.CreateAccount()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(account.Wipe)
	issuer, err := account.PublicKey()
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	memberFP, err := auth.FingerprintFromActor(pubs[1])
	if err != nil {
		t.Fatal(err)
	}
	agentFP, err := auth.FingerprintFromActor(pubs[2])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Create(db, "infraauth", "infraauth", memberFP, "unused", time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := agentprov.Provision(db, "infraauth", "node", agentFP, time.Now()); err != nil {
		t.Fatal(err)
	}
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true, AlwaysEnableNonce: true,
		Nkeys: []*server.NkeyUser{{Nkey: pubs[0]}}, AuthCallout: &server.AuthCallout{Issuer: issuer, Account: "$G", AuthUsers: []string{pubs[0]}}})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	if !s.ReadyForConnections(time.Second) {
		t.Fatal("ordinary auth server not ready")
	}
	handler := &authcallout.Handler{DB: db, AccountKp: account, Now: time.Now, JWTTTL: time.Hour, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	memberSubject := proto.SubjCtrlNodeList(pubs[1], "infraauth")
	agentSubject := proto.SubjNodeRegister("infraauth", "node")
	respond := func(msg *nats.Msg) {
		if !strings.HasPrefix(msg.Reply, auth.InboxRoot+".") {
			t.Error("business probe did not use private inbox")
			return
		}
		if err := msg.Respond([]byte(`{"ok":true}`)); err != nil {
			t.Error(err)
		}
	}
	var owner *infraauth.Owner
	checks := 0
	cfg := infraauth.Config{URL: s.ClientURL(), AccountPublicKey: issuer, Options: func() []nats.Option { return []nats.Option{nats.Nkey(pubs[0], keys[0].Sign)} }, HandleAuth: handler.Handle,
		Transport: func(d c.Deadline, stop c.Stop) (infraauth.Transport, error) {
			raw, err := infratransport.New(uint64(checks+1), d, stop, nil)
			if err != nil {
				return nil, err
			}
			return authTransport{raw}, nil
		},
		Business: []infraauth.Binding{{Subject: memberSubject, Queue: "tether-ctl", Handle: respond}, {Subject: agentSubject, Handle: respond}},
		Member:   infraauth.Identity{Name: "tether-cli:infraauth", PublicKey: pubs[1], Sign: keys[1].Sign, RPCSubject: memberSubject, RPCRequest: []byte(`{}`), RPCResponse: []byte(`{"ok":true}`)},
		Agent:    infraauth.Identity{Name: "tether-agent:infraauth:node", PublicKey: pubs[2], Sign: keys[2].Sign, RPCSubject: agentSubject, RPCRequest: []byte(`{}`), RPCResponse: []byte(`{"ok":true}`)},
		Recovery: func(generation uint64, d c.Deadline, stop c.Stop) error {
			// Actual server subscription census witnesses that business replay
			// has not happened yet, rather than trusting post-hoc log ordering.
			info, err := s.Subsz(&server.SubszOptions{Subscriptions: true})
			if err != nil {
				return err
			}
			queue := 0
			for _, sub := range info.Subs {
				if sub.Subject == memberSubject || sub.Subject == agentSubject {
					return fmt.Errorf("business SUB preceded original-prefix verification")
				}
				if sub.Subject == "$SYS.REQ.USER.AUTH" && sub.Queue == "tether-authcallout" {
					queue++
				}
			}
			if queue != 1 || generation != uint64(checks+1) {
				return fmt.Errorf("normal auth queue generation missing")
			}
			if _, _, err := owner.Business(); err == nil {
				return fmt.Errorf("business admission opened before recovery")
			}
			checks++
			_, err = controlbudget.Remaining(d)
			return err
		},
	}
	owner, err = infraauth.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(budget(t, 3*time.Second)); err != nil {
			t.Error(err)
		}
	})
	var before infraauth.Receipt
	for generation := uint64(1); generation <= 2; generation++ {
		if generation == 2 {
			nc, _, err := owner.Business()
			if err != nil {
				t.Fatal(err)
			}
			nc.Close()
		}
		receipt, err := owner.Open(budget(t, 4*time.Second), nil)
		if err != nil || receipt.Phase != infraauth.ControlReady || receipt.Generation != generation || checks != int(generation) {
			t.Fatalf("ordinary reconnect did not prove local auth-first readiness: %v", err)
		}
		if receipt.AuthPONG >= receipt.AuthUsable || receipt.AuthUsable >= receipt.BusinessPONG || receipt.BusinessPONG >= receipt.ControlReady {
			t.Fatal("actual auth/business barrier order changed")
		}
		for _, probe := range receipt.Probes {
			if probe.Generation != generation || probe.ServerID != s.ID() || probe.CID == 0 || probe.RequestDigest == (c.Digest{}) {
				t.Fatal("ordinary queue probe lacks actual CONNECT/RPC identity")
			}
		}
		if generation == 2 && (receipt.AuthCID == before.AuthCID || receipt.BusinessCID == before.BusinessCID) {
			t.Fatal("ordinary reconnect reused stale queue/connection proof")
		}
		before = receipt
	}
}

// A real disabled JS service produces 503/no responders while core NATS stays
// connected. The pending bytes/version remain owned; reenabling the same store
// resolves that attempt rather than resetting the writer or its checkpoint.
// origin: hot-update S9 plan §4.3 / T59 / M20–M21
func exerciseJSUnavailable(t *testing.T) {
	original := embedded(t)
	fence, url := newReplyFence(t, original.ClientURL())
	js := dataConnection(t, url)
	body := bytes.Repeat([]byte("503 original checkpoint"), 5000)
	spec := objectSpec(t, js, body)
	w, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Stop(budget(t, time.Second)); err != nil {
			t.Error(err)
		}
	})
	first, err := w.Append(0, body[:spec.ChunkSize], budget(t, time.Second))
	if err != nil {
		t.Fatal(err)
	}
	lost := fence.arm(func(subject string) bool { return subject == "$O.payload.C."+spec.Identity.ObjectVersion })
	result := make(chan error, 1)
	go func() {
		_, err := w.Append(1, body[spec.ChunkSize:2*spec.ChunkSize], budget(t, 400*time.Millisecond))
		result <- err
	}()
	loss := requireLoss(t, lost)
	var ack jetstream.PubAck
	if err := json.Unmarshal(loss.Reply, &ack); err != nil || ack.Sequence <= first.StreamSeq {
		t.Fatal("second pending publication lacked a real ACK")
	}
	fence.resume()
	if err := <-result; err == nil {
		t.Fatal("pending ACK was delivered")
	}
	before, err := w.Freeze(budget(t, time.Second))
	if err != nil || before.PendingAttempt == nil {
		t.Fatal("pending checkpoint missing")
	}
	unavailable, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go unavailable.Start()
	t.Cleanup(func() { unavailable.Shutdown(); unavailable.WaitForShutdown() })
	if !unavailable.ReadyForConnections(time.Second) {
		t.Fatal("core NATS unavailable-JS fixture not ready")
	}
	failedJS := dataConnection(t, unavailable.ClientURL())
	if _, err := failedJS.Conn().Request("$JS.API.INFO", nil, time.Second); !errors.Is(err, nats.ErrNoResponders) {
		t.Fatalf("fault did not hit actual JS 503: %v", err)
	}
	waiting, err := jsstream.ImportTransferWriter(failedJS, before)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := waiting.Stop(budget(t, time.Second)); err != nil {
			t.Error(err)
		}
	})
	if err := jsstream.ThawTransferWriter(waiting, budget(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := waiting.ResolvePending(budget(t, 300*time.Millisecond)); err == nil {
		t.Fatal("JS503 returned a fake resolved attempt")
	}
	state, err := waiting.Freeze(budget(t, time.Second))
	if err != nil || state.ObjectSpec != spec || !reflect.DeepEqual(state.PendingAttempt, before.PendingAttempt) || !reflect.DeepEqual(state.Receipts, before.Receipts) {
		t.Fatal("JS503 discarded original pending bytes/NUID/checkpoint")
	}
	next := dataConnection(t, original.ClientURL())
	resumed, err := jsstream.ImportTransferWriter(next, state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := resumed.Stop(budget(t, time.Second)); err != nil {
			t.Error(err)
		}
	})
	if err := jsstream.ThawTransferWriter(resumed, budget(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	receipt, found, err := resumed.ResolvePending(budget(t, time.Second))
	if err != nil || !found || receipt.StreamSeq != ack.Sequence {
		t.Fatal("503 recovery replaced the original pending physical sequence")
	}
	for idx := uint32(2); idx < spec.Chunks; idx++ {
		start := int(idx * spec.ChunkSize)
		if _, err := resumed.Append(idx, body[start:min(start+int(spec.ChunkSize), len(body))], budget(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := resumed.Finalize(budget(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	after, err := resumed.Freeze(budget(t, time.Second))
	if err != nil || len(after.Receipts) != int(spec.Chunks) || after.ObjectSpec != state.ObjectSpec || !reflect.DeepEqual(after.Receipts[:1], state.Receipts) {
		t.Fatal("JS recovery minted a replacement object or original ACK")
	}
	verifyRaw(t, next, after)
}

func syncChunk(t *testing.T, r x.ObjectReader, sink *os.File, h io.Writer, chunk x.Chunk) x.SinkCheckpoint {
	t.Helper()
	n, err := sink.WriteAt(chunk.Data, chunk.Receipt.Offset)
	if err != nil || n != len(chunk.Data) {
		t.Fatal("original sink write was incomplete")
	}
	if err := sink.Sync(); err != nil {
		t.Fatal(err)
	}
	if _, err := h.Write(chunk.Data); err != nil {
		t.Fatal(err)
	}
	state, err := h.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	cp := x.SinkCheckpoint{ChunkIndex: chunk.Receipt.Index + 1, DurableOffset: chunk.Receipt.Offset + int64(n), LastConsumedStreamSeq: chunk.Receipt.StreamSeq, SHA256State: state}
	if err := r.ConfirmSynced(cp); err != nil {
		t.Fatal(err)
	}
	return cp
}

func verifyRaw(t *testing.T, js jetstream.JetStream, state x.WriterState) {
	t.Helper()
	stream, err := js.Stream(t.Context(), state.ObjectSpec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(t.Context())
	if err != nil || info.State.Msgs != uint64(state.ObjectSpec.Chunks)+1 {
		t.Fatal("raw object gained duplicate or missing publications")
	}
	for idx, receipt := range state.Receipts {
		raw, err := stream.GetMsg(t.Context(), receipt.StreamSeq)
		if err != nil || raw.Subject != "$O."+state.ObjectSpec.Bucket+".C."+state.ObjectSpec.Identity.ObjectVersion ||
			raw.Header.Get("Tether-Xfer-Index") != fmt.Sprint(idx) || sha256.Sum256(raw.Data) != receipt.SHA256 {
			t.Fatalf("original ACK multiset changed at %d: %v", idx, err)
		}
	}
}

func requireConsumers(t *testing.T, stream jetstream.Stream, want uint64) {
	t.Helper()
	info, err := stream.Info(t.Context())
	if err != nil || uint64(info.State.Consumers) != want {
		t.Fatalf("consumer retirement count=%v want=%d: %v", info, want, err)
	}
}
