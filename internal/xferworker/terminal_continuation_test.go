// Copy into internal/xferworker; this file has no dependency on existing test helpers.
package xferworker

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type terminalCapacity struct{ seq atomic.Uint64 }

func (c *terminalCapacity) Reserve(x.Reservation) (x.ReservationID, error) {
	return x.ReservationID(fmt.Sprint(c.seq.Add(1))), nil
}
func (*terminalCapacity) Release(x.ReservationID) error { return nil }

type terminalAuthority struct{}

func (terminalAuthority) Check(x.Identity, x.AccessKind, x.Deadline) error { return nil }

type terminalReader struct {
	data []byte
	read bool
	cp   x.SinkCheckpoint
}

func (r *terminalReader) ReadChunk(x.Deadline) (x.Chunk, error) {
	if r.read {
		return x.Chunk{}, io.EOF
	}
	r.read = true
	return x.Chunk{Data: r.data, Receipt: x.ChunkReceipt{Length: uint32(len(r.data)), SHA256: sha256.Sum256(r.data)}}, nil
}
func (r *terminalReader) ConfirmSynced(cp x.SinkCheckpoint) error { r.cp = cp; return nil }
func (r *terminalReader) Freeze(x.Deadline) (x.ReaderState, error) {
	return x.ReaderState{SinkCheckpoint: r.cp}, nil
}
func (*terminalReader) Stop(x.Deadline) error { return nil }
func terminalDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

type terminalFixture struct {
	base     x.Base
	prepare  x.Prepare
	tracker  x.TrackerManager
	ledger   x.LedgerPort
	history  x.HistoryPort
	receiver x.Receiver
	target   string
}

func terminalSetup(t *testing.T) *terminalFixture {
	t.Helper()
	f := &terminalFixture{}
	f.base = x.Base{Clock: func() (string, int64, error) { n, e := controlbudget.Now(); return "boot", n, e }, UTC: time.Now, Capacity: &terminalCapacity{}}
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "nonce", IntentDigest: hex.EncodeToString(sha256.New().Sum(nil))}
	origin := x.Origin{BrokerID: "broker", InstanceID: "origin", BootID: "boot", Generation: 1}
	data := []byte("new bytes")
	sum := sha256.Sum256(data)
	now := time.Now().UTC()
	intent := x.Intent{Verb: "push", Tier: "a", Path: "destination", Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:])}
	permit := x.CreationPermit{Identity: id, Origin: origin, PermitNonce: "permit", Signature: "verified", IssuedAt: now, CreateUntil: now.Add(30 * time.Second)}
	ledger, err := xferledger.New(x.LedgerConfig{Base: f.base, Root: filepath.Join(t.TempDir(), "primary"), FallbackRoot: filepath.Join(t.TempDir(), "outbox")})
	if err != nil {
		t.Fatal(err)
	}
	f.ledger = ledger
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("NATS not ready")
	}
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	_, err = js.CreateStream(t.Context(), jetstream.StreamConfig{Name: "history-session", Subjects: []string{proto.SubjAuditTransfer(id.SID)}, Discard: jetstream.DiscardNew})
	if err != nil {
		t.Fatal(err)
	}
	f.history, err = xferhistory.New(x.HistoryConfig{Base: f.base, JS: js, SID: id.SID})
	if err != nil {
		t.Fatal(err)
	}
	f.tracker, err = xfertrack.New(x.TrackerConfig{Base: f.base, Origin: origin, Ledger: ledger, History: f.history, ResolveReceiver: terminalClosingReceiver{f}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.tracker.Stop(terminalDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	_, err = f.tracker.Admit(permit, intent, terminalDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	status, err := f.tracker.SourceSeal(id, x.SourceSeal{Size: intent.Size, SHA256: intent.SHA256, SourceFingerprint: "source", ChunkSize: 0, Chunks: 0}, terminalDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	f.prepare = x.Prepare{CallID: "call", Identity: id, Origin: origin, Permit: permit, Intent: intent, Inline: data, Budget: status.Budget}
	f.target = filepath.Join(t.TempDir(), "destination")
	if err = os.WriteFile(f.target, []byte("old bytes"), 0600); err != nil {
		t.Fatal(err)
	}
	vp, err := transferpath.ValidateForWrite(f.target, []string{filepath.Dir(f.target)}, transferpath.ModeNarrow)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := transferpath.NewAtomicReceiver(vp, "reviewtmp", true)
	if err != nil {
		t.Fatal(err)
	}
	f.receiver, err = xferreceive.New(x.ReceiveConfig{Base: f.base, Identity: id, Budget: status.Budget, Reader: &terminalReader{data: data}, Atomic: sink})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := f.receiver.Stop(terminalDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	return f
}
func (f *terminalFixture) reserve(t *testing.T) {
	t.Helper()
	grant, err := f.tracker.Reserve(f.prepare.Identity, x.ReserveRequest{Origin: f.prepare.Origin, ReceiverInstance: "receiver"}, terminalDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if err = f.receiver.Reserve(grant); err != nil {
		t.Fatal(err)
	}
}
func terminalWorker(t *testing.T, f *terminalFixture) (*owner, *entry) {
	t.Helper()
	manager, err := NewWorker(x.WorkerConfig{Base: f.base, Identity: x.Identity{AgentInstance: "agent"}, Authority: terminalAuthority{}})
	if err != nil {
		t.Fatal(err)
	}
	o := manager.(*owner)
	t.Cleanup(func() {
		if err := o.Stop(terminalDeadline(t)); err != nil {
			t.Error(err)
		}
	})
	if _, err = o.Prepare(f.prepare, terminalDeadline(t)); err != nil {
		t.Fatal(err)
	}
	e := o.entries[key(f.prepare.Identity)]
	e.receiver = f.receiver
	return o, e
}

// Snapshotting progress must not strand the original receiver before its only commit.
// origin: S7 internal review rv-terminal F1
func TestTerminalWorkerProgressKeepsReceiverRunnable(t *testing.T) {
	f := terminalSetup(t)
	f.reserve(t)
	o, e := terminalWorker(t, f)
	if err := o.step(e, terminalDeadline(t)); err != nil {
		t.Fatal(err)
	}
	err := o.step(e, terminalDeadline(t))
	data, readErr := os.ReadFile(f.target)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(data) != "new bytes" {
		t.Fatalf("worker stopped before terminal: second step=%v destination=%q", err, data)
	}
}

// Reading the closed outcome after Stop must reach courier construction, without Freeze on a stopped actor.
// origin: S7 internal review rv-terminal F1
func TestTerminalWorkerCancelReadsClosedOutcome(t *testing.T) {
	f := terminalSetup(t)
	o, _ := terminalWorker(t, f)
	if err := f.receiver.Step(terminalDeadline(t)); err != nil {
		t.Fatal(err)
	}
	called := false
	o.cfg.MakeCourier = func(x.CourierConfig) (x.Courier, error) {
		called = true
		return nil, errors.New("courier factory reached")
	}
	_, err := o.Apply(x.Control{Identity: f.prepare.Identity, Kind: x.ControlCancel, Reason: x.CancelUser}, terminalDeadline(t))
	if !called {
		t.Fatalf("closed outcome never reaches courier: %v", err)
	}
}

type terminalClosingReceiver struct{ fixture *terminalFixture }

func (r terminalClosingReceiver) Resolve(_ x.Identity, d x.Deadline) (x.ReceiverOutcome, error) {
	if r.fixture.receiver == nil {
		return x.ReceiverOutcome{}, errors.New("receiver not installed")
	}
	if err := r.fixture.receiver.Stop(d); err != nil {
		return x.ReceiverOutcome{}, err
	}
	return xferreceive.Outcome(r.fixture.receiver)
}

// A normal deletion must actually close the retained receiver and decide its terminal.
