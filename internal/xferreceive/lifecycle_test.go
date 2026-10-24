package xferreceive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sys/unix"
)

// origin: hot-update S7 plan §4 / T08 (P11 owns the real broker T12 matrix).
func TestReceiverGrantGateRejectsLateCommitAfterFailure(t *testing.T) {
	for _, inline := range []bool{false, true} {
		for _, role := range []string{"push-agent", "pull-ctl"} {
			t.Run(role+map[bool]string{true: "/inline", false: "/object"}[inline], func(t *testing.T) {
				f := newReceiverFixture(t, inline)
				grant := testGrant(f.config.Identity)
				grant.ReceiverInstance = role
				for _, defect := range []string{"empty", "identity", "terminal", "nonce", "receiver"} {
					bad := grant
					switch defect {
					case "empty":
						bad = x.CommitGrant{}
					case "identity":
						bad.Identity.AgentInstance = "replacement"
					case "terminal":
						bad.TerminalID = "other"
					case "nonce":
						bad.GrantNonce = ""
					case "receiver":
						bad.ReceiverInstance = ""
					default:
						t.Fatal(defect)
					}
					requireCode(t, f.actor.Reserve(bad), x.CodeIntentConflict)
				}
				for range f.chunks {
					requireStep(t, f.actor)
				}
				if err := f.actor.Stop(testDeadline(t)); err != nil {
					t.Fatal(err)
				}
				if err := f.actor.Reserve(grant); err == nil {
					t.Error("canceled receiver accepted a late grant")
				}
				if err := f.actor.Step(testDeadline(t)); err == nil || errors.Is(err, io.EOF) {
					t.Errorf("canceled receiver attempted a late commit: %v", err)
				}
				state := receiverState(f.actor)
				outcome, err := Outcome(f.actor)
				if err != nil || !state.GateClosed || outcome.Outcome != x.OutcomeNotEntered || f.sink.commits != 0 {
					t.Fatalf("cancel lost its closed-gate proof: %+v", state.Outcome)
				}
				if _, err := os.Stat(f.sink.destination); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("canceled receiver landed: %v", err)
				}
			})
		}
	}
}

// origin: hot-update S7 plan §3 / §4 / T08
func TestReceiverCommitJoinCannotInventFailure(t *testing.T) {
	f := newReceiverFixture(t, false)
	for range f.chunks {
		requireStep(t, f.actor)
	}
	grant := testGrant(f.config.Identity)
	if err := f.actor.Reserve(grant); err != nil {
		t.Fatal(err)
	}
	if err := f.actor.Reserve(grant); err != nil {
		t.Fatal(err)
	}
	changed := grant
	changed.GrantNonce = "second-grant"
	requireCode(t, f.actor.Reserve(changed), x.CodeTerminalConflict)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	f.sink.commit = func(g x.CommitGrant) x.ReceiverOutcome {
		close(entered)
		<-release
		if err := os.Rename(f.sink.file.Name(), f.sink.destination); err != nil {
			return x.ReceiverOutcome{Grant: g, Outcome: x.OutcomeNotEntered}
		}
		return x.ReceiverOutcome{Grant: g, Outcome: x.OutcomeLanded, Primitive: x.PrimitiveRename}
	}
	done := make(chan error, 1)
	stepDeadline := testDeadline(t)
	go func() { done <- f.actor.Step(stepDeadline); close(done) }()
	t.Cleanup(func() { once.Do(func() { close(release) }); <-done })
	awaitSignal(t, entered)
	if outcome, err := Outcome(f.actor); err == nil || outcome.Outcome != x.OutcomeInflight {
		t.Fatalf("unresolved primitive became proof: %+v, %v", outcome, err)
	}
	deadline, err := controlbudget.After(20 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.actor.Stop(deadline); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("unjoined stop = %v", err)
	}
	if receiverState(f.actor).Outcome.Outcome != x.OutcomeInflight || f.sink.stops != 0 {
		t.Fatal("Stop guessed failure or closed the inflight syscall's file")
	}
	once.Do(func() { close(release) })
	if err := awaitError(t, done); err == nil {
		t.Fatal("missing parent Sync was reported complete")
	}
	if err := f.actor.Stop(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if receiverState(f.actor).Outcome.Outcome != x.OutcomeLanded || f.sink.commits != 1 {
		t.Fatal("cancellation replaced landed outcome")
	}
}

// origin: hot-update S7 plan §1.2 / §3 / §4 / T08
func TestReceiverDurabilitySettlementSurvivesByteDeadline(t *testing.T) {
	f := newReceiverFixture(t, false)
	for range f.chunks {
		requireStep(t, f.actor)
	}
	grant := testGrant(f.config.Identity)
	if err := f.actor.Reserve(grant); err != nil {
		t.Fatal(err)
	}
	f.sink.commit = func(g x.CommitGrant) x.ReceiverOutcome {
		return x.ReceiverOutcome{Grant: g, Outcome: x.OutcomeLanded, Primitive: x.PrimitiveLink, Code: x.CodeDurabilityUnknown}
	}
	requireCode(t, f.actor.Step(testDeadline(t)), x.CodeDurabilityUnknown)
	r := f.actor.(*receiver)
	now := int64(r.state.ByteDeadline.Deadline) + 1
	r.config.Clock = func() (string, int64, error) { return r.state.ByteDeadline.BootID, now, nil }
	f.sink.commit = func(g x.CommitGrant) x.ReceiverOutcome {
		return x.ReceiverOutcome{Grant: g, Outcome: x.OutcomeNotEntered}
	}
	requireCode(t, r.Step(x.Deadline(now+int64(time.Second))), x.CodeResultUnknown)
	if receiverState(r).Outcome.Outcome != x.OutcomeLanded {
		t.Fatal("inconsistent port evidence erased a landed effect")
	}
	f.sink.commit = func(g x.CommitGrant) x.ReceiverOutcome {
		return x.ReceiverOutcome{Grant: g, Outcome: x.OutcomeLanded, Primitive: x.PrimitiveLink, ParentSynced: true}
	}
	if err := r.Step(x.Deadline(now + int64(time.Second))); !errors.Is(err, io.EOF) {
		t.Fatalf("directory settlement after byte expiry = %v", err)
	}
}

// origin: hot-update S7 plan §3 / §8.2 / T08
func TestReceiverFreezeRejectsMismatchedOwnerEvidence(t *testing.T) {
	for _, owner := range []string{"atomic", "reader", "atomic_outcome"} {
		t.Run(owner, func(t *testing.T) {
			f := newReceiverFixture(t, false)
			requireStep(t, f.actor)
			code := x.CodeObjectConflict
			switch owner {
			case "atomic":
				f.sink.freeze = func(s *x.AtomicState) { s.DurableOffset++ }
			case "atomic_outcome":
				code = x.CodeResultUnknown
				f.sink.freeze = func(s *x.AtomicState) { s.PrimitiveOutcome.Outcome = x.OutcomeLanded }
			default:
				f.reader.freeze = func(s *x.ReaderState) { s.SinkCheckpoint.LastConsumedStreamSeq++ }
			}
			_, err := f.actor.Freeze(testDeadline(t))
			requireCode(t, err, code)
			if f.sink.stops != 0 || f.reader.stops != 0 {
				t.Fatal("refused cut stopped the old owner")
			}
			f.sink.freeze, f.reader.freeze = nil, nil
			requireStep(t, f.actor)
			assertPrefix(t, f, 2)
		})
	}
}

// origin: hot-update S7 plan §1.2 / §3 / T08
func TestReceiverAdmissionUsesOriginalDeadlineAndSyncObservation(t *testing.T) {
	f := newReceiverFixture(t, false)
	r := f.actor.(*receiver)
	var observations []x.Observation
	r.config.Observe = func(o x.Observation) { observations = append(observations, o) }
	requireStep(t, r)
	if len(observations) != 2 || observations[0].FileSynced || observations[0].DurableOffset != 0 ||
		observations[0].Index != 0 || observations[0].LastConsumedStreamSeq != 0 ||
		!observations[1].FileSynced || observations[1].DurableOffset != int64(len(f.chunks[0])) ||
		observations[1].HashStateDigest == "" || observations[1].LastConsumedStreamSeq != 7 {
		t.Fatalf("Sync observations = %+v", observations)
	}
	r.config.Clock = func() (string, int64, error) {
		return r.state.ByteDeadline.BootID, int64(r.state.ByteDeadline.Deadline), nil
	}
	if err := r.Step(r.state.ByteDeadline.Deadline + 100); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("renewed byte budget: %v", err)
	}
	if f.sink.writes != 1 {
		t.Fatal("expired receiver wrote another chunk")
	}
	for _, defect := range []string{"clock", "reader", "atomic", "identity", "unsealed", "remaining", "overflow"} {
		bad := f.config
		switch defect {
		case "clock":
			bad.Clock = nil
		case "reader":
			bad.Reader = nil
		case "atomic":
			bad.Atomic = nil
		case "identity":
			bad.Identity.EntryNonce = ""
		case "unsealed":
			bad.Budget.SourceSealed = false
		case "remaining":
			bad.Budget.RemainingNS = 0
		case "overflow":
			bad.Budget.RemainingNS = 1<<63 - 1
			bad.Budget.EffectiveBudgetNS = bad.Budget.RemainingNS
		default:
			t.Fatal(defect)
		}
		if _, err := New(bad); err == nil {
			t.Fatalf("accepted invalid configuration: %s", defect)
		}
	}
}

// origin: hot-update S7 plan §3 / §4 / T08
func TestReceiverUnresolvedPrimitiveAndForeignOutcomeCannotCrossCut(t *testing.T) {
	for _, defect := range []string{"grant", "kind", "bytes", "hash", "inflight"} {
		t.Run(defect, func(t *testing.T) {
			f := newReceiverFixture(t, false)
			for range f.chunks {
				requireStep(t, f.actor)
			}
			grant := testGrant(f.config.Identity)
			if err := f.actor.Reserve(grant); err != nil {
				t.Fatal(err)
			}
			f.sink.commit = func(g x.CommitGrant) x.ReceiverOutcome {
				out := x.ReceiverOutcome{Grant: g, Outcome: x.OutcomeLanded, ParentSynced: true}
				switch defect {
				case "grant":
					out.Grant.GrantNonce = "foreign"
				case "kind":
					out.Outcome = "invented"
				case "bytes":
					out.Bytes = f.config.Budget.Size + 1
				case "hash":
					out.SHA256 = "different-file"
				case "inflight":
					out.Outcome, out.ParentSynced = x.OutcomeInflight, false
				default:
					t.Fatal(defect)
				}
				return out
			}
			requireCode(t, f.actor.Step(testDeadline(t)), x.CodeResultUnknown)
			_, err := f.actor.Freeze(testDeadline(t))
			requireCode(t, err, x.CodeCommitInflight)
			if outcome, err := Outcome(f.actor); err == nil || outcome.Outcome != x.OutcomeInflight {
				t.Fatalf("unknown effect was fabricated: %+v, %v", outcome, err)
			}
		})
	}
}

// origin: hot-update S7 plan §3 / T08
func TestReceiverEmptyAndZeroProgressWritesCannotSkipSync(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		f := newReceiverFixture(t, false)
		f.config.Budget.Size = 0
		f.reader.chunks = nil
		actor, err := New(f.config)
		if err != nil {
			t.Fatal(err)
		}
		f.sink.sync = func() error { return errInjected }
		if err := actor.Step(testDeadline(t)); !errors.Is(err, errInjected) {
			t.Fatalf("empty file skipped Sync: %v", err)
		}
		if f.sink.commits != 0 {
			t.Fatal("unsynced empty file entered a primitive")
		}
		f.sink.sync = nil
		if err := actor.Reserve(testGrant(f.config.Identity)); err != nil {
			t.Fatal(err)
		}
		if err := actor.Step(testDeadline(t)); !errors.Is(err, io.EOF) || f.sink.syncs != 1 {
			t.Fatalf("empty file settlement = %v, syncs=%d", err, f.sink.syncs)
		}
	})
	for _, n := range []int{0, -1, 1 << 20} {
		f := newReceiverFixture(t, false)
		f.sink.write = func([]byte, int64) (int, error) { return n, nil }
		if err := f.actor.Step(testDeadline(t)); err == nil {
			t.Fatalf("invalid or stalled write count %d accepted", n)
		}
		if f.sink.syncs != 0 || f.reader.confirms != 0 {
			t.Fatal("invalid write count advanced durability")
		}
		assertCheckpoint(t, receiverState(f.actor).Checkpoint, nil)
	}
}

// origin: hot-update S7 plan §11.3 / T08
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "receiver lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "receiver lifecycle", beforeFD, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	// Child courier/history ACKs belong to P09. This leaf exercises its own
	// success, blocked-cut refusal, current-state import, cancel and Stop.
	exerciseSyncedPrefix(t, "sync_blocked_cut")
	f := newReceiverFixture(t, true)
	requireStep(t, f.actor)
	state := requireFreeze(t, f.actor)
	if state.InlineOffset != f.config.Budget.Size || state.Checkpoint.ChunkIndex != 0 || state.Checkpoint.LastConsumedStreamSeq != 0 {
		t.Fatal("inline checkpoint fabricated an object cursor")
	}
	f.actor = requireImport(t, f, state)
	if err := f.actor.Stop(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := f.actor.Stop(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if f.sink.stops != 1 || f.reader.stops != 1 {
		t.Fatal("repeated Stop repeated resource ownership")
	}
	requireCode(t, f.actor.Reserve(testGrant(f.config.Identity)), x.CodeCanceled)
	requireCode(t, f.actor.Step(testDeadline(t)), x.CodeCanceled)
}

// Bind the frozen factories to the real reader and original-OFD importer.
var productionReader x.ReaderFactory = jsstream.NewTransferReader
var productionAtomic x.AtomicImportFactory = transferpath.ImportAtomic

func exerciseProductionReceiver(t *testing.T) {
	t.Helper()
	// Import the real WRONLY tmp and original parent via the frozen port. This
	// path must not repair them or reopen their names during a receiver cut.
	dir := t.TempDir()
	tmp, err := os.OpenFile(filepath.Join(dir, "pending"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tmp.Close() })
	parent, err := os.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = parent.Close() })
	atomicState := x.AtomicState{Phase: x.PhaseReceiving, TmpName: "pending", Destination: "destination", Force: true,
		Tmp:         x.ResourceRef{ResourceID: "receiver-tmp", Name: x.TmpFileName, Kind: x.ResourceTmp},
		Parent:      x.ResourceRef{ResourceID: "receiver-parent", Name: x.ParentFileName, Kind: x.ResourceParent},
		TmpIdentity: fileIdentity(t, tmp), ParentIdentity: fileIdentity(t, parent)}
	files := receiverFiles{"receiver-tmp/tmp": tmp, "receiver-parent/parent": parent}
	atomic, salvage, err := productionAtomic(atomicState, files)
	if err != nil {
		for _, resource := range salvage.Resources {
			for _, file := range resource.Files {
				_ = file.Close()
			}
		}
		t.Fatalf("NOT_RUN pending P05 production atomic import: %v", err)
	}
	t.Cleanup(func() { _ = atomic.Stop(testDeadline(t)) })
	if err := transferpath.ThawAtomic(atomic, testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	options := natstest.DefaultTestOptions
	options.Port, options.JetStream, options.StoreDir = -1, true, t.TempDir()
	server := natstest.RunServer(&options)
	t.Cleanup(func() { server.Shutdown(); server.WaitForShutdown() })
	nc, err := nats.Connect(server.ClientURL(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second) // ctx-root: embedded-server test fixture.
	defer cancel()
	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: "receiver", Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	body := bytes.Repeat([]byte("production receiver payload\x00\xff"), 20000)
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64), ObjectVersion: "originalversion"}
	digest := sha256.Sum256(body)
	stream, err := js.Stream(ctx, "OBJ_receiver")
	if err != nil {
		t.Fatal(err)
	}
	streamInfo, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	spec := x.ObjectSpec{Identity: id, Bucket: "receiver", Stream: "OBJ_receiver", StreamCreated: streamInfo.Created.UTC().Format(time.RFC3339Nano),
		SHA256: hex.EncodeToString(digest[:]), Size: int64(len(body)), ChunkSize: 32 << 10, Chunks: uint32((len(body) + (32 << 10) - 1) / (32 << 10))}
	writer, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint32(0); index < spec.Chunks; index++ {
		start := int(index) * int(spec.ChunkSize)
		if _, err := writer.Append(index, body[start:min(start+int(spec.ChunkSize), len(body))], testDeadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := writer.Finalize(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := writer.Stop(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := emptyCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	reader, err := productionReader(js, spec, checkpoint)
	if err != nil {
		t.Fatalf("NOT_RUN pending P03 production reader: %v", err)
	}
	t.Cleanup(func() { _ = reader.Stop(testDeadline(t)) })
	fault := &partialAtomic{AtomicPort: atomic}
	config := Config{Base: x.Base{Clock: receiverTestClock}, Identity: id, Reader: reader, Atomic: fault,
		Budget: x.BudgetSeal{StartedAt: time.Now().UTC(), Size: int64(len(body)), EffectiveBudgetNS: int64(time.Minute), RemainingNS: int64(time.Minute), SourceSealed: true}}
	actor, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	requireStep(t, actor)
	before := receiverState(actor).Checkpoint
	if before.DurableOffset != int64(spec.ChunkSize) || before.ChunkIndex != 1 || before.LastConsumedStreamSeq == 0 {
		t.Fatalf("production reader did not confirm the first complete chunk: %+v", before)
	}
	raw, err := stream.GetMsg(ctx, before.LastConsumedStreamSeq)
	if err != nil || !bytes.Equal(raw.Data, body[:spec.ChunkSize]) {
		t.Fatalf("checkpoint does not identify the physical first chunk: %v", err)
	}
	firstHash := sha256.Sum256(body[:spec.ChunkSize])
	if digest, err := checkpointDigest(before); err != nil || digest != hex.EncodeToString(firstHash[:]) {
		t.Fatalf("production prefix digest = %s, %v", digest, err)
	}
	fault.first = true
	if err := actor.Step(testDeadline(t)); !errors.Is(err, errInjected) {
		t.Fatalf("production partial write = %v", err)
	}
	state := requireFreeze(t, actor)
	stat, err := tmp.Stat()
	if err != nil || stat.Size() != int64(spec.ChunkSize) || !sameCheckpoint(state.Checkpoint, before) {
		t.Fatalf("production partial cut retained unsynced suffix: stat=%v state=%+v err=%v", stat, state.Checkpoint, err)
	}
	prefix, err := os.ReadFile(tmp.Name())
	if err != nil || !bytes.Equal(prefix, body[:spec.ChunkSize]) {
		t.Fatalf("production cut changed the synced disk prefix: bytes=%d, err=%v", len(prefix), err)
	}
	// Reconstruct using the original resources/checkpoint, not a pathname reopen.
	atomic, _, err = productionAtomic(state.Atomic, receiverFiles{"receiver-tmp/tmp": tmp, "receiver-parent/parent": parent})
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Stop(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	reader, err = productionReader(js, spec, state.Checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	if err := transferpath.ThawAtomic(atomic, testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	config.Reader, config.Atomic = reader, atomic
	actor, err = Import(config, state)
	if err != nil {
		t.Fatal(err)
	}
	if err := Thaw(actor); err != nil {
		t.Fatal(err)
	}
	if err := actor.Reserve(testGrant(id)); err != nil {
		t.Fatal(err)
	}
	for index := state.Checkpoint.ChunkIndex; index < spec.Chunks; index++ {
		requireStep(t, actor)
	}
	if err := actor.Step(testDeadline(t)); !errors.Is(err, io.EOF) {
		t.Fatalf("production commit = %v", err)
	}
	got, err := os.ReadFile(filepath.Join(dir, "destination"))
	if err != nil || !bytes.Equal(got, body) {
		t.Fatalf("production destination mismatch: bytes=%d, err=%v", len(got), err)
	}
	if err := actor.Stop(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

type partialAtomic struct {
	x.AtomicPort
	first bool
}

func (a *partialAtomic) WriteAt(p []byte, offset int64) (int, error) {
	if !a.first {
		return a.AtomicPort.WriteAt(p, offset)
	}
	a.first = false
	n, err := a.AtomicPort.WriteAt(p[:3], offset)
	return n, errors.Join(err, errInjected)
}

type receiverFiles map[string]*os.File

func (f receiverFiles) TakeFile(id, name string) (*os.File, error) {
	key := id + "/" + name
	file := f[key]
	if file == nil {
		return nil, os.ErrNotExist
	}
	delete(f, key)
	return file, nil
}

func fileIdentity(t *testing.T, file *os.File) x.FileIdentity {
	t.Helper()
	var stat unix.Stat_t
	var flags int
	raw, err := file.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var inner error
	err = raw.Control(func(fd uintptr) {
		inner = unix.Fstat(int(fd), &stat)
		if inner == nil {
			flags, inner = unix.FcntlInt(fd, unix.F_GETFL, 0)
		}
	})
	if err != nil || inner != nil {
		t.Fatalf("stat original descriptor: %v, %v", err, inner)
	}
	return x.FileIdentity{Dev: uint64(stat.Dev), Ino: uint64(stat.Ino), Mode: uint32(stat.Mode), Flags: int32(flags)}
}
