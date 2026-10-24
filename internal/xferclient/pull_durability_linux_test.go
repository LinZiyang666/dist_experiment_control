//go:build linux

package xferclient

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sys/unix"
)

type durabilityCapacity struct{ next atomic.Uint64 }

func (a *durabilityCapacity) Reserve(x.Reservation) (x.ReservationID, error) {
	return x.ReservationID(fmt.Sprint(a.next.Add(1))), nil
}
func (*durabilityCapacity) Release(x.ReservationID) error { return nil }

type durabilityTransport struct {
	recoveryTransport
	tracker x.TrackerManager
}

func (p *durabilityTransport) Request(q x.Request, d x.Deadline) (x.Response, x.Attempt, error) {
	if q.Op != x.OpTerminal || q.Outcome == nil {
		return x.Response{}, x.Attempt{}, errors.New("unexpected control operation")
	}
	p.sent = append(p.sent, *q.Outcome)
	receipt, err := p.tracker.Finish(q.Identity, *q.Outcome, d)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	return x.Response{Receipt: &receipt}, x.Attempt{}, nil
}

// Real Atomic, directory fsync, ledger and exact JetStream history. The fault
// installs an O_PATH descriptor at the retained parent's descriptor number only
// after the real rename returned. fsync then really returns EBADF; recovery
// reinstalls a duplicate of the original OFD, never a pathname reopen.
// origin: S7 internal review rv-recovery F04
func TestTransferPullDurabilityRecoveryUsesOriginalAtomicAndExactHistory(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exercisePullDurabilityRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "pull directory durability x5", beforeG)
	testharness.AssertNoFDLeak(t, "pull directory durability x5", beforeFD, 0)
}

func exercisePullDurabilityRecovery(t *testing.T) {
	t.Helper()
	for _, permanent := range []bool{false, true} {
		t.Run(fmt.Sprintf("permanent=%t", permanent), func(t *testing.T) {
			base := x.Base{Clock: func() (string, int64, error) { n, err := controlbudget.Now(); return "boot", n, err }, UTC: time.Now, Capacity: &durabilityCapacity{}}
			nc, err := nats.Connect(testharness.StartJSNATS(t))
			if err != nil {
				t.Fatal(err)
			}
			defer nc.Close()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "history-session", Subjects: []string{proto.SubjAuditTransfer("session")}, Storage: jetstream.FileStorage, Discard: jetstream.DiscardNew})
			if err != nil {
				t.Fatal(err)
			}
			history, err := xferhistory.New(x.HistoryConfig{Base: base, JS: js, SID: "session"})
			if err != nil {
				t.Fatal(err)
			}
			root := t.TempDir()
			ledger, err := xferledger.New(x.LedgerConfig{Base: base, Root: filepath.Join(root, "ledger"), FallbackRoot: filepath.Join(root, "outbox")})
			if err != nil {
				t.Fatal(err)
			}
			id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64)}
			origin := x.Origin{BrokerID: "broker", InstanceID: "broker-instance", BootID: "boot", Generation: 1}
			now := time.Now().UTC()
			permit := x.CreationPermit{Identity: id, Origin: origin, IssuedAt: now, CreateUntil: now.Add(time.Minute), PermitNonce: "permit", Signature: "verified"}
			intent := x.Intent{Verb: "pull", Tier: "a", Path: "destination", Size: 3, SHA256: digestOf([]byte("new")), Force: true}
			tracker, err := xfertrack.New(x.TrackerConfig{Base: base, Origin: origin, Ledger: ledger, History: history})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := tracker.Stop(recoveryDeadline(t)); err != nil {
					t.Error(err)
				}
			}()
			if _, err := tracker.Admit(permit, intent, recoveryDeadline(t)); err != nil {
				t.Fatal(err)
			}
			if _, err := tracker.SourceSeal(id, x.SourceSeal{Size: 3, SHA256: intent.SHA256, SourceFingerprint: "pinned-source"}, recoveryDeadline(t)); err != nil {
				t.Fatal(err)
			}
			grant, err := tracker.Reserve(id, x.ReserveRequest{Origin: origin, ReceiverInstance: "receiver"}, recoveryDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			// Drive the exact START publication explicitly at its real port. Its
			// background cadence is independent of the local fsync failure window.
			census, err := ledger.Scan(recoveryDeadline(t))
			if err != nil || len(census.Rows) != 1 {
				t.Fatal("START ledger unavailable", err)
			}
			start, err := history.Confirm(*census.Rows[0].CanonicalStart, recoveryDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			decisions, err := xfertrack.Decisions(tracker)
			if err != nil {
				t.Fatal(err)
			}
			if err := decisions.AcceptHistory(x.Key{SID: id.SID, TransferID: id.TransferID}, id.EntryNonce, census.Rows[0].Revision, start, recoveryDeadline(t)); err != nil {
				t.Fatal(err)
			}
			destination := filepath.Join(root, "destination")
			if err := os.WriteFile(destination, []byte("old"), 0600); err != nil {
				t.Fatal(err)
			}
			vp, err := transferpath.ValidateForWrite(destination, []string{root}, transferpath.ModeOpen)
			if err != nil {
				t.Fatal(err)
			}
			receiver, err := transferpath.NewAtomicReceiver(vp, "owned", true)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := receiver.Stop(recoveryDeadline(t)); err != nil {
					t.Error(err)
				}
			}()
			if err := receiver.(*transferpath.Atomic).BindObjectVersion(""); err != nil {
				t.Fatal(err)
			}
			state, resources, err := receiver.Freeze(recoveryDeadline(t))
			if err != nil {
				t.Fatal(err)
			}
			var parent *os.File
			for _, resource := range resources {
				if resource.ID == state.Parent.ResourceID {
					parent = resource.Files[state.Parent.Name]
				}
			}
			if parent == nil {
				t.Fatal("no original parent descriptor")
			}
			parentFD := durabilityFD(t, parent)
			originalFD, err := unix.FcntlInt(uintptr(parentFD), unix.F_DUPFD_CLOEXEC, 3)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = unix.Close(originalFD) }()
			faultFD, err := unix.Open(root, unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = unix.Close(faultFD) }()
			faultActive := false
			defer func() {
				if faultActive {
					_ = unix.Dup3(originalFD, parentFD, unix.O_CLOEXEC)
				}
			}()
			if err := transferpath.ThawAtomic(receiver, recoveryDeadline(t)); err != nil {
				t.Fatal(err)
			}
			var enters, effects, syncFailures int
			var faultErr error
			if err := transferpath.SetAtomicObserver(receiver, func(event x.Observation) {
				switch event.Kind {
				case x.ObservePrimitiveEnter:
					enters++
				case x.ObservePrimitiveExit:
					if event.Outcome == x.OutcomeLanded {
						effects++
						faultErr = unix.Dup3(faultFD, parentFD, unix.O_CLOEXEC)
						faultActive = faultErr == nil
					}
				case x.ObserveDirectorySync:
					if event.Errno != 0 {
						syncFailures++
					}
				}
			}); err != nil {
				t.Fatal(err)
			}
			if n, err := receiver.WriteAt([]byte("new"), 0); err != nil || n != 3 {
				t.Fatal("source write failed", err)
			}
			if err := receiver.Sync(); err != nil {
				t.Fatal(err)
			}
			first := receiver.Commit(grant, recoveryDeadline(t))
			first.SHA256 = intent.SHA256
			if faultErr != nil || first.Outcome != x.OutcomeLanded || first.ParentSynced || first.Code != x.CodeDurabilityUnknown || syncFailures != 1 {
				t.Fatalf("real post-rename fsync fault missing: %+v %v failures=%d", first, faultErr, syncFailures)
			}
			transport := &durabilityTransport{tracker: tracker}
			c := &Client{cfg: Config{Base: base, Intent: intent, Destination: receiver, Transport: transport}, identity: id, origin: origin, grant: &grant, bootID: "boot", byteStop: make(chan struct{}), spec: &x.ObjectSpec{Identity: id, Size: 3, SHA256: intent.SHA256}, checkpoint: x.SinkCheckpoint{DurableOffset: 3}, outcome: &first}
			// The original byte budget has expired. Only settlement may still run.
			c.deadline = 1
			if err := c.pull(recoveryDeadline(t)); err == nil || c.receipt != nil || len(transport.sent) != 0 {
				t.Fatal("unsynced primitive submitted terminal evidence")
			}
			if !permanent {
				if err := unix.Dup3(originalFD, parentFD, unix.O_CLOEXEC); err != nil {
					t.Fatal(err)
				}
				faultActive = false
				assertDurabilityOFD(t, originalFD, parentFD)
				if err := c.pull(recoveryDeadline(t)); err != nil {
					t.Fatal(err)
				}
				for range 2 {
					if err := c.pull(recoveryDeadline(t)); err != nil {
						t.Fatal(err)
					}
				}
				if c.receipt == nil || c.receipt.Kind != "complete" || !c.outcome.ParentSynced || len(transport.sent) != 1 {
					t.Fatal("storage recovery did not yield one exact complete")
				}
			} else {
				for range 2 {
					if err := c.pull(recoveryDeadline(t)); err == nil || c.receipt != nil {
						t.Fatal("permanent fsync failure reported complete")
					}
				}
				// Restore only for owner cleanup, after proving no complete was reported.
				if err := unix.Dup3(originalFD, parentFD, unix.O_CLOEXEC); err != nil {
					t.Fatal(err)
				}
				faultActive = false
			}
			if enters != 1 || effects != 1 {
				t.Fatalf("settlement repeated primitive: enters=%d effects=%d", enters, effects)
			}
			body, err := os.ReadFile(destination)
			if err != nil || !bytes.Equal(body, []byte("new")) {
				t.Fatal("landed bytes changed", err)
			}
			set, err := history.Lookup(x.Key{SID: id.SID, TransferID: id.TransferID}, recoveryDeadline(t))
			if err != nil || !set.Complete || len(set.Start) != 1 {
				t.Fatal("real START history unavailable", err)
			}
			wantTerminal := 1
			if permanent {
				wantTerminal = 0
			}
			if len(set.Terminal) != wantTerminal {
				t.Fatalf("physical terminal count=%d want=%d", len(set.Terminal), wantTerminal)
			}
			if !permanent {
				raw, err := stream.GetMsg(ctx, c.receipt.HistorySeq)
				if err != nil || !bytes.Equal(raw.Data, set.Terminal[0].Bytes) || c.receipt.CanonicalDigest != set.Terminal[0].Digest || set.Terminal[0].Kind != "complete" {
					t.Fatal("receipt did not match exact raw canonical complete", err)
				}
			}
		})
	}
}

func durabilityFD(t *testing.T, file *os.File) int {
	t.Helper()
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	fd := -1
	if err := raw.Control(func(value uintptr) { fd = int(value) }); err != nil {
		t.Fatal(err)
	}
	return fd
}
func assertDurabilityOFD(t *testing.T, original, current int) {
	t.Helper()
	flags, err := unix.FcntlInt(uintptr(original), unix.F_GETFL, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := unix.FcntlInt(uintptr(original), unix.F_SETFL, flags^unix.O_NONBLOCK); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = unix.FcntlInt(uintptr(original), unix.F_SETFL, flags) }()
	observed, err := unix.FcntlInt(uintptr(current), unix.F_GETFL, 0)
	if err != nil || observed&unix.O_NONBLOCK != (flags^unix.O_NONBLOCK)&unix.O_NONBLOCK {
		t.Fatal("recovery reopened the parent instead of preserving its OFD", err)
	}
}
