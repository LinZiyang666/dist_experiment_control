// Real receiver, tracker, disk ledger and exact history exercise the terminal boundary.
package xfertrack_test

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/LinZiyang666/tether/internal/testharness"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferfinal"
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
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "nonce", IntentDigest: "digest"}
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
	status, err := f.tracker.SourceSeal(id, x.SourceSeal{Size: intent.Size, SHA256: intent.SHA256, SourceFingerprint: "source"}, terminalDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	f.prepare = x.Prepare{Identity: id, Origin: origin, Permit: permit, Intent: intent, Inline: data, Budget: status.Budget}
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

// A real synced prefix followed by irreversible cancellation must still produce one failed terminal.
// origin: S7 internal review rv-terminal F2
func exerciseTerminalCancellationAfterSyncedBytes(t *testing.T) {
	for _, reserved := range []bool{false, true} {
		t.Run(fmt.Sprint(reserved), func(t *testing.T) {
			f := terminalSetup(t)
			if reserved {
				f.reserve(t)
			}
			if err := f.receiver.Step(terminalDeadline(t)); err != nil {
				t.Fatal(err)
			}
			if err := f.receiver.Stop(terminalDeadline(t)); err != nil {
				t.Fatal(err)
			}
			out, err := xferreceive.Outcome(f.receiver)
			if err != nil {
				t.Fatal(err)
			}
			if out.Outcome != x.OutcomeNotEntered {
				t.Fatalf("not closed: %+v", out)
			}
			receipt, finishErr := f.tracker.Finish(f.prepare.Identity, out, terminalDeadline(t))
			set, scanErr := f.history.Lookup(key(f.prepare.Identity), terminalDeadline(t))
			if scanErr != nil {
				t.Fatal(scanErr)
			}
			if finishErr != nil || receipt.Kind != "failed" || len(set.Terminal) != 1 {
				t.Fatalf("closed receiver cannot settle: bytes=%d finish=%v receipt=%+v physical terminals=%d", out.Bytes, finishErr, receipt, len(set.Terminal))
			}
			again, err := f.tracker.Finish(f.prepare.Identity, out, terminalDeadline(t))
			if err != nil || again != receipt {
				t.Fatalf("replayed failure changed its terminal: %+v %v", again, err)
			}
			late, grantErr := f.tracker.Reserve(f.prepare.Identity, x.ReserveRequest{Origin: f.prepare.Origin, ReceiverInstance: "receiver"}, terminalDeadline(t))
			if !reserved && grantErr == nil {
				t.Fatal("late Reserve reopened a canceled entry")
			}
			if grantErr == nil && f.receiver.Reserve(late) == nil {
				t.Fatal("closed receiver accepted a late grant")
			}
			if err := f.receiver.Step(terminalDeadline(t)); err == nil || errors.Is(err, io.EOF) {
				t.Fatal("closed receiver resumed byte work")
			}
			current, err := os.ReadFile(f.target)
			if err != nil || string(current) != "old bytes" {
				t.Fatalf("late commit changed the destination: %q %v", current, err)
			}
			set, err = f.history.Lookup(key(f.prepare.Identity), terminalDeadline(t))
			if err != nil || len(set.Terminal) != 1 || set.Terminal[0].Kind != "failed" {
				t.Fatalf("late work changed physical history: %+v %v", set, err)
			}

		})
	}
}

// Snapshotting progress must not strand the original receiver before its only commit.

// Reading the closed outcome after Stop must reach courier construction, without Freeze on a stopped actor.

type terminalSessionOwners struct {
	ledger x.LedgerPort
	origin x.Origin
}

func (o terminalSessionOwners) SessionCensus(_ string, d x.Deadline) ([]x.LedgerCensus, error) {
	c, e := o.ledger.Scan(d)
	c.Origin = o.origin
	return []x.LedgerCensus{c}, e
}

type terminalFinalizer struct{}

func (terminalFinalizer) Advance(x.Identity, x.Deadline) (x.Status, error) {
	return x.Status{}, errors.New("not yet settled")
}
func (terminalFinalizer) Stop(x.Deadline) error { return nil }

// The explicit abandonment path must stage a canonical terminal accepted by the actual ledger.
// origin: S7 internal review rv-terminal F5

type terminalReceiverResolver struct{ receiver x.Receiver }

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
// origin: S7 internal review rv-terminal F4
func exerciseTerminalSessionRemovalClosesReceiver(t *testing.T) {
	for _, barrierPath := range []bool{false, true} {
		t.Run(fmt.Sprint(barrierPath), func(t *testing.T) {
			f := terminalSetup(t)
			f.reserve(t)
			var cancelErr error
			if barrierPath {
				barrier, err := xferauth.NewSettlement(xferauth.SettlementConfig{Owners: terminalSessionOwners{f.ledger, f.prepare.Origin}, Tracker: f.tracker, Ledger: f.ledger, Finalizer: terminalFinalizer{}, UTC: time.Now})
				if err != nil {
					t.Fatal(err)
				}
				cancelErr = barrier.Settle(f.prepare.Identity.SID, false, terminalDeadline(t))
			} else {
				_, cancelErr = f.tracker.Cancel(f.prepare.Identity, x.CancelRequest{Origin: f.prepare.Origin, Reason: x.CancelRevoked}, terminalDeadline(t))
			}
			set, err := f.history.Lookup(key(f.prepare.Identity), terminalDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if len(set.Terminal) != 1 || set.Terminal[0].Kind != "failed" {
				t.Fatalf("reachable receiver never canceled: barrier=%v error=%v physical terminals=%d", barrierPath, cancelErr, len(set.Terminal))
			}
		})
	}
}

func (r terminalReceiverResolver) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	return xferreceive.Outcome(r.receiver)
}

type terminalObjects struct{}

func (terminalObjects) Retire(id x.Identity, p x.RetirementProof, _ x.Deadline) (x.CleanupReceipt, error) {
	return x.CleanupReceipt{Identity: id, OwnerGeneration: p.OwnerGeneration, Digest: "proof", WriterRetired: true, ConsumersRetired: true, DirectorySynced: true, TmpUnlinked: true}, nil
}

// Cleanup must remain consumable after a real tracker export/import advances runtime generation.
// origin: S7 internal review rv-terminal F3
func exerciseTerminalCleanupAfterTrackerImageCut(t *testing.T) {
	for _, cut := range []bool{false, true} {
		t.Run(fmt.Sprint(cut), func(t *testing.T) {
			f := terminalSetup(t)
			f.reserve(t)
			if err := f.receiver.Step(terminalDeadline(t)); err != nil {
				t.Fatal(err)
			}
			if err := f.receiver.Step(terminalDeadline(t)); !errors.Is(err, io.EOF) {
				t.Fatalf("commit: %v", err)
			}
			out, err := xferreceive.Outcome(f.receiver)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.tracker.Finish(f.prepare.Identity, out, terminalDeadline(t)); err != nil {
				t.Fatal(err)
			}
			decisions, err := xfertrack.Decisions(f.tracker)
			if err != nil {
				t.Fatal(err)
			}
			row, err := decisions.Read(key(f.prepare.Identity), terminalDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			cleanup := x.CleanupReceipt{Identity: row.Identity, OwnerGeneration: 1, Digest: "proof", WriterRetired: true, ConsumersRetired: true, DirectorySynced: true, TmpUnlinked: true}
			if err = decisions.AcceptCleanup(key(row.Identity), row.EntryNonce, row.Revision, cleanup, terminalDeadline(t)); err != nil {
				t.Fatal(err)
			}
			if cut {
				frozen, err := f.tracker.Freeze(terminalDeadline(t))
				if err != nil {
					t.Fatal(err)
				}
				state, _, err := frozen.Export()
				if err != nil {
					t.Fatal(err)
				}
				restored, err := xfertrack.Import(x.TrackerConfig{Base: f.base, Origin: f.prepare.Origin, Ledger: f.ledger, History: f.history}, state)
				if err != nil {
					t.Fatal(err)
				}
				if err = f.tracker.Stop(terminalDeadline(t)); err != nil {
					t.Fatal(err)
				}
				f.tracker = restored
				if err = restored.Bind(x.Bindings{Ledger: f.ledger, OwnerGeneration: 2}, terminalDeadline(t)); err != nil {
					t.Fatal(err)
				}
				decisions, err = xfertrack.Decisions(restored)
				if err != nil {
					t.Fatal(err)
				}
			}
			finalizer, err := xferfinal.New(x.FinalConfig{Base: f.base, Decisions: decisions, Ledger: f.ledger, History: f.history, Objects: terminalObjects{}, ResolveReceiver: terminalReceiverResolver{f.receiver}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := finalizer.Stop(terminalDeadline(t)); err != nil {
					t.Error(err)
				}
			}()
			status, advanceErr := finalizer.Advance(f.prepare.Identity, terminalDeadline(t))
			set, err := f.history.Lookup(key(f.prepare.Identity), terminalDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			if len(set.Start) != 1 || len(set.Terminal) != 1 {
				t.Fatalf("history changed: %+v", set)
			}
			if advanceErr != nil || status.Phase != x.PhaseSettled {
				t.Fatalf("cut=%v exact history exists but cleanup cannot retire: phase=%s error=%v", cut, status.Phase, advanceErr)
			}
		})
	}
}

func key(id x.Identity) x.Key { return x.Key{SID: id.SID, TransferID: id.TransferID} }

// The unavailable receiver is a transport boundary, while ledger, tracker,
// exact history and finalizer all use their real implementations.
type absentTerminalReceiver struct{}

func (absentTerminalReceiver) Resolve(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
	return x.ReceiverOutcome{}, errors.New("original receiver is permanently absent")
}

// origin: S7 internal review rv-terminal F5 / rv-recovery F08 / rv-wiring F5
func TestTransferAbandonmentSerializesLateFinishAcrossImageCuts(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAbandonmentImageReplay(t)
	}
	testharness.AssertNoGoroutineLeak(t, "abandonment image replay", g)
	testharness.AssertNoFDLeak(t, "abandonment image replay", fd, 0)
}
func exerciseAbandonmentImageReplay(t *testing.T) {
	t.Helper()
	for _, granted := range []bool{false, true} {
		t.Run(fmt.Sprint(granted), func(t *testing.T) {
			f := terminalSetup(t)
			if granted {
				f.reserve(t)
			}
			if err := f.receiver.Step(terminalDeadline(t)); err != nil {
				t.Fatal(err)
			}
			if err := f.receiver.Stop(terminalDeadline(t)); err != nil {
				t.Fatal(err)
			}
			late, err := xferreceive.Outcome(f.receiver)
			if err != nil {
				t.Fatal(err)
			}
			// Move the original state to the current image with the receiver's
			// transport permanently absent, without changing its permit or grant.
			for generation := uint64(2); generation <= 3; generation++ {
				frozen, err := f.tracker.Freeze(terminalDeadline(t))
				if err != nil {
					t.Fatal(err)
				}
				state, _, err := frozen.Export()
				if err != nil {
					t.Fatal(err)
				}
				restored, err := xfertrack.Import(x.TrackerConfig{Base: f.base, Origin: f.prepare.Origin, Ledger: f.ledger, History: f.history, ResolveReceiver: absentTerminalReceiver{}}, state)
				if err != nil {
					t.Fatal(err)
				}
				if err = f.tracker.Stop(terminalDeadline(t)); err != nil {
					t.Fatal(err)
				}
				f.tracker = restored
				if err = restored.Bind(x.Bindings{Ledger: f.ledger, OwnerGeneration: generation}, terminalDeadline(t)); err != nil {
					t.Fatal(err)
				}
				view, err := xfertrack.Decisions(restored)
				if err != nil {
					t.Fatal(err)
				}
				final, err := xferfinal.New(x.FinalConfig{Base: f.base, Decisions: view, Ledger: f.ledger, History: f.history, Objects: terminalObjects{}, ResolveReceiver: absentTerminalReceiver{}})
				if err != nil {
					t.Fatal(err)
				}
				barrier, err := xferauth.NewSettlement(xferauth.SettlementConfig{Owners: terminalSessionOwners{f.ledger, f.prepare.Origin}, Tracker: restored, Ledger: f.ledger, Finalizer: final})
				if err != nil {
					t.Fatal(err)
				}
				if generation == 2 {
					if err = barrier.Settle(f.prepare.Identity.SID, false, terminalDeadline(t)); err == nil {
						t.Fatal("unflagged removal released an absent receiver")
					}
					census, err := f.ledger.Scan(terminalDeadline(t))
					if err != nil || len(census.Rows) != 1 || census.Rows[0].CanonicalTerminal != nil {
						t.Fatalf("unflagged removal decided or erased responsibility: %+v %v", census, err)
					}
					if err = barrier.Settle(f.prepare.Identity.SID, true, terminalDeadline(t)); err != nil {
						t.Fatal(err)
					}
				}
				status, err := restored.Status(x.StatusRequest{Identity: f.prepare.Identity, Origin: f.prepare.Origin}, terminalDeadline(t))
				if err != nil || status.Terminal == nil || status.Terminal.Kind != "failed" || !status.Terminal.OutcomeUnknown || status.Terminal.Code != x.ReasonSettlementAbandoned || status.Phase != x.PhaseSettled || status.Origin != f.prepare.Origin {
					t.Fatalf("administrative terminal or immutable origin changed: %+v %v", status, err)
				}
				errorsFromLate := make(chan error, 1)
				d := terminalDeadline(t)
				go func() { _, err := restored.Finish(f.prepare.Identity, late, d); errorsFromLate <- err }()
				if err = barrier.Settle(f.prepare.Identity.SID, true, terminalDeadline(t)); err != nil {
					t.Fatal(err)
				}
				if err = <-errorsFromLate; err == nil {
					t.Fatal("late Finish replaced the administrative terminal")
				}
				set, err := f.history.Lookup(key(f.prepare.Identity), terminalDeadline(t))
				if err != nil || len(set.Start) != 1 || len(set.Terminal) != 1 || set.Terminal[0].Digest != status.Terminal.CanonicalDigest {
					t.Fatalf("image replay changed exact physical history: %+v %v", set, err)
				}
				if err = final.Stop(terminalDeadline(t)); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

// origin: S7 internal review rv-terminal F2
func TestTerminalCancellationAfterSyncedBytes(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTerminalCancellationAfterSyncedBytes(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}

// origin: S7 internal review rv-terminal F4
func TestTerminalSessionRemovalClosesReceiver(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTerminalSessionRemovalClosesReceiver(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}

// origin: S7 internal review rv-terminal F3
func TestTerminalCleanupAfterTrackerImageCut(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round-"+strconv.Itoa(round), func(t *testing.T) { exerciseTerminalCleanupAfterTrackerImageCut(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "terminal settlement", g)
	testharness.AssertNoFDLeak(t, "terminal settlement", fd, 0)
}
