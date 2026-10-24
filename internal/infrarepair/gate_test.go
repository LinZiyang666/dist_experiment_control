package infrarepair

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/infrareadiness"
	"github.com/LinZiyang666/tether/internal/testharness"
	ns "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type recoveryFixture struct {
	gate     *Gate
	plan     c.Plan
	js       jetstream.JetStream
	nc       *nats.Conn
	server   *ns.Server
	close    func()
	mu       sync.Mutex
	original Responsibilities
	file     *os.File
	source   Producer
}

func recoveryDeadline(t *testing.T) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// The fixture uses two real accounts and the production P11 inventory parser.
// No process is spawned and no installed service or user store is accessed.
func newRecovery(t *testing.T) *recoveryFixture {
	t.Helper()
	sys, biz := ns.NewAccount("$SYS"), ns.NewAccount("business")
	server, err := ns.NewServer(&ns.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, ServerName: "recovery",
		JetStream: true, StoreDir: t.TempDir(), SystemAccount: "$SYS", Accounts: []*ns.Account{sys, biz},
		Users: []*ns.User{{Username: "system", Password: "test", Account: sys}, {Username: "business", Password: "test", Account: biz}}})
	if err != nil {
		t.Fatal(err)
	}
	server.Start()
	if !server.ReadyForConnections(3 * time.Second) {
		server.Shutdown()
		server.WaitForShutdown()
		t.Fatal("NATS did not start")
	}
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
	biz, err = server.LookupAccount("business")
	if err != nil {
		t.Fatal(err)
	}
	if err := biz.EnableJetStream(nil, nil); err != nil {
		t.Fatal(err)
	}
	connect := func(user string) *nats.Conn {
		nc, err := nats.Connect(server.ClientURL(), nats.UserInfo(user, "test"), nats.NoReconnect())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(nc.Close)
		return nc
	}
	system, business := connect("system"), connect("business")
	js, err := jetstream.New(business)
	if err != nil {
		t.Fatal(err)
	}
	reader, err := infrareadiness.NewJetStream(infrareadiness.NATSRequester{Conn: system}, []infrareadiness.AccountAccess{{ID: "business", Query: infrareadiness.NATSRequester{Conn: business}}})
	if err != nil {
		t.Fatal(err)
	}
	topology := infrareadiness.Topology{Standalone: true, Complete: true, Revision: 1,
		Servers: []infrareadiness.Server{{ID: server.ID(), Name: server.Name(), Host: "fixture", Version: ns.VERSION, JetStream: true, Online: true}}}
	f := &recoveryFixture{js: js, nc: business, server: server, plan: c.Plan{Tx: c.ID{1}, Domain: c.ID{2}, Component: c.NATS, Step: 1}}
	f.file, err = os.CreateTemp(t.TempDir(), "synced-prefix-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.file.Close() })
	if _, err = f.file.Write([]byte("original synced prefix")); err != nil {
		t.Fatal(err)
	}
	if err = f.file.Sync(); err != nil {
		t.Fatal(err)
	}
	f.original.Local = []LocalReceipt{{Identity: digest("original reader FD"), SyncedPrefix: 22, PrefixDigest: digest([]byte("original synced prefix")), PublisherCursor: 7, PendingAttempt: digest("original pending attempt")}}
	f.source = Producer{AssetWriter: true, Name: "transfer", Capture: func(c.Deadline) (Responsibilities, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		return cloneResponsibilities(f.original), nil
	},
		Check: func(r Responsibilities, d c.Deadline, stop c.Stop) error {
			if err := admission(d, stop); err != nil {
				return err
			}
			bytes := make([]byte, 22)
			if _, err := f.file.ReadAt(bytes, 0); err != nil {
				return err
			}
			if len(r.Local) != 1 || digest(bytes) != r.Local[0].PrefixDigest {
				return ErrReceipt
			}
			return nil
		}, Retire: func(c.Deadline, c.Stop) error { f.nc.Close(); return nil }}
	f.gate, err = New(Config{Domain: f.plan.Domain, Required: []string{"transfer"}, Accounts: map[string]jetstream.JetStream{"business": js},
		Assets: func(d c.Deadline, stop c.Stop) (c.AssetSeal, error) {
			snapshot, err := reader.ReadInventory(topology, d, stop)
			if err != nil {
				return c.AssetSeal{}, err
			}
			accounts, assets, err := infrareadiness.SealDigests(snapshot)
			return c.AssetSeal{Accounts: accounts, Assets: assets, Complete: err == nil}, err
		},
		// The embedded fixture creates all credentials/clients and has no external
		// writers. Production must bind the actual managed-account writer policy.
		Exclusive: func(d c.Deadline, stop c.Stop) error { return admission(d, stop) },
		Transfers: func(c.Deadline) (c.TransferSeal, error) {
			return c.TransferSeal{Version: 1, SourceSHA256: digest("fixture"), SchemaSHA256: digest("receipt-schema"), InvocationSet: digest("original invocation"), ReceiptSet: digest("original ACK set"), ConsumerSet: digest("original consumer"), Complete: true}, nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	if err = f.gate.Register(f.source); err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	for _, name := range []string{"chunks", "audit"} {
		if _, err = js.CreateStream(ctx, jetstream.StreamConfig{Name: name, Subjects: []string{name + ".>"}, Storage: jetstream.FileStorage, Replicas: 1}); err != nil {
			t.Fatal(err)
		}
	}
	for i, subject := range []string{"chunks.original.1", "chunks.original.2", "chunks.original.3", "audit.start", "audit.terminal"} {
		msg := &nats.Msg{Subject: subject, Header: nats.Header{"Original-NUID": []string{"original"}, "Chunk-Index": []string{fmt.Sprint(i)}}, Data: []byte("acknowledged " + subject)}
		ack, err := js.PublishMsg(ctx, msg)
		if err != nil {
			t.Fatal(err)
		}
		f.original.ACKs = append(f.original.ACKs, ACK{Account: "business", Stream: ack.Stream, Subject: subject, NUID: "original", Index: uint64(i), Sequence: ack.Sequence, Message: MessageDigest(msg.Header, msg.Data)})
	}
	consumer, err := js.CreateConsumer(ctx, "chunks", jetstream.ConsumerConfig{Durable: "reader", AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		t.Fatal(err)
	}
	msg, err := consumer.Next(jetstream.FetchMaxWait(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err = msg.DoubleAck(ctx); err != nil {
		t.Fatal(err)
	}
	info, err := consumer.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f.original.Consumers = []ConsumerReceipt{{Account: "business", Stream: "chunks", Name: "reader", CreatedNS: info.Created.UnixNano(), DeliveredConsumer: info.Delivered.Consumer, DeliveredStream: info.Delivered.Stream, ACKConsumer: info.AckFloor.Consumer, ACKStream: info.AckFloor.Stream}}
	var once sync.Once
	f.close = func() {
		once.Do(func() {
			_ = f.file.Close()
			business.Close()
			system.Close()
			server.Shutdown()
			server.WaitForShutdown()
		})
	}
	t.Cleanup(f.close)
	return f
}

func sealRecovery(t *testing.T, f *recoveryFixture) c.Checkpoint {
	t.Helper()
	cp, err := f.gate.Seal(f.plan, recoveryDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	return cp
}

// origin: hot-update S9 plan §4.3 / T40 / M20
func TestACKPrefixIsReadBeforeRepair(t *testing.T) {
	for _, defect := range []string{"intact", "empty_message", "chunk_hole", "audit_start_hole", "audit_terminal_hole", "consumer_floor", "synced_prefix", "header_digest", "checkpoint"} {
		t.Run(defect, func(t *testing.T) {
			f := newRecovery(t)
			defer f.close()
			if defect == "empty_message" {
				ack, err := f.js.Publish(t.Context(), "chunks.empty", []byte{})
				if err != nil {
					t.Fatal(err)
				}
				f.original.ACKs = append(f.original.ACKs, ACK{Account: "business", Stream: ack.Stream, Subject: "chunks.empty", Sequence: ack.Sequence, Message: MessageDigest(nil, []byte{})})
			}
			cp := sealRecovery(t, f)
			if err := f.gate.Release(cp, recoveryDeadline(t)); !errors.Is(err, ErrFenced) {
				t.Fatalf("repair opened before verification: %v", err)
			}
			var missing *ACK
			switch defect {
			case "chunk_hole":
				missing = &f.original.ACKs[1]
			case "audit_start_hole":
				missing = &f.original.ACKs[3]
			case "audit_terminal_hole":
				missing = &f.original.ACKs[4]
			case "consumer_floor":
				consumer, err := f.js.Consumer(t.Context(), "chunks", "reader")
				if err != nil {
					t.Fatal(err)
				}
				msg, err := consumer.Next(jetstream.FetchMaxWait(time.Second))
				if err != nil {
					t.Fatal(err)
				}
				if err = msg.DoubleAck(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "synced_prefix":
				if _, err := f.file.WriteAt([]byte("altered!"), 0); err != nil {
					t.Fatal(err)
				}
			case "header_digest":
				// Model a transport/store returning an altered original header: the frozen
				// receipt records the correct original content, independent of LastSeq.
				f.gate.receipts["transfer"].ACKs[0].Message = MessageDigest(nats.Header{"Original-NUID": []string{"changed"}}, []byte("acknowledged chunks.original.1"))
			case "checkpoint":
				cp.Proof[0] ^= 1
			}
			if missing != nil {
				stream, err := f.js.Stream(t.Context(), missing.Stream)
				if err != nil {
					t.Fatal(err)
				}
				before, err := stream.Info(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if err = stream.DeleteMsg(t.Context(), missing.Sequence); err != nil {
					t.Fatal(err)
				}
				if _, err = f.js.Publish(t.Context(), missing.Stream+".tail", []byte("new tail hides global high-water loss")); err != nil {
					t.Fatal(err)
				}
				after, err := stream.Info(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				if after.State.LastSeq <= before.State.LastSeq {
					t.Fatal("negative control did not preserve increasing LastSeq")
				}
			}
			permit, err := f.gate.Enter("transfer")
			if err == nil {
				permit.Finish(nil)
				t.Fatal("replay admitted before original receipt read")
			}
			err = f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), nil)
			if defect == "intact" || defect == "empty_message" {
				if err != nil {
					t.Fatal(err)
				}
				if err = f.gate.Release(cp, recoveryDeadline(t)); err != nil {
					t.Fatal(err)
				}
				permit, err = f.gate.Enter("transfer")
				if err != nil {
					t.Fatal(err)
				}
				permit.Finish(nil)
				if err = f.gate.Release(cp, recoveryDeadline(t)); err == nil {
					t.Fatal("stale generation reopened gate")
				}
			} else {
				if !errors.Is(err, ErrReceipt) {
					t.Fatalf("%s: missing original receipt accepted: %v", defect, err)
				}
				if err = f.gate.Release(cp, recoveryDeadline(t)); err == nil {
					t.Fatal("failed verification opened repair")
				}
				if missing != nil {
					stream, err := f.js.Stream(t.Context(), missing.Stream)
					if err != nil {
						t.Fatal(err)
					}
					if _, err = stream.GetMsg(t.Context(), missing.Sequence); !errors.Is(err, jetstream.ErrMsgNotFound) {
						t.Fatalf("repair concealed original hole: %v", err)
					}
					replay := &nats.Msg{Subject: missing.Subject, Header: nats.Header{"Original-NUID": []string{"original"}, "Chunk-Index": []string{fmt.Sprint(missing.Index)}}, Data: []byte("acknowledged " + missing.Subject)}
					ack, err := f.js.PublishMsg(t.Context(), replay)
					if err != nil {
						t.Fatal(err)
					}
					restored, err := stream.GetLastMsgForSubject(t.Context(), missing.Subject)
					if err != nil || MessageDigest(restored.Header, restored.Data) != missing.Message || ack.Sequence == missing.Sequence {
						t.Fatalf("replay did not restore original bytes at a later sequence: %v", err)
					}
					if err := f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), nil); !errors.Is(err, ErrReceipt) {
						t.Fatalf("replay concealed confirmed historical loss: %v", err)
					}

				}
			}
		})
	}
}

// origin: hot-update S9 plan §4.3 / T40
func TestInfraRecoveryLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "infra recovery", goroutines)
	testharness.AssertNoFDLeak(t, "infra recovery", fds, 0)
}

func exerciseRecovery(t *testing.T) {
	t.Helper()
	f := newRecovery(t)
	defer f.close()
	cp := sealRecovery(t, f)
	stop := make(chan struct{})
	close(stop)
	if err := f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), stop); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
	if err := f.gate.Release(cp, recoveryDeadline(t)); err == nil {
		t.Fatal("canceled verification released gate")
	}
	if err := f.gate.VerifyBeforeReplay(cp, recoveryDeadline(t), nil); err != nil {
		t.Fatal(err)
	}
	if err := f.gate.Release(cp, recoveryDeadline(t)); err != nil {
		t.Fatal(err)
	}
	permit, err := f.gate.Enter("transfer")
	if err != nil {
		t.Fatal(err)
	}
	permit.Finish(nil)
}
