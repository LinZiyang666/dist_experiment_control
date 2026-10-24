package xferreceive

// These unit cases use real WRONLY files and real WriteAt/Sync calls with
// controlled port faults. They do not certify NATS recovery or path security.
// T08's production_ports arm separately requires the P03/P05 implementations.

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

var errInjected = errors.New("injected filesystem or reader failure")

// origin: hot-update S7 plan §3 / T08 / M08
func TestTransferSyncedPrefixSurvivesShortWriteAndCut(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for _, scenario := range []string{"short_success", "partial_error", "full_error", "partial_cut", "sync_failure", "sync_blocked_cut", "confirmation_blocked_cut", "confirmation_cut"} {
		t.Run(scenario, func(t *testing.T) { exerciseSyncedPrefix(t, scenario) })
	}
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("production_ports_%d", round), func(t *testing.T) { exerciseProductionReceiver(t) })
	}
	// Direct after the rounds (formerly deferred: same point, before t.Cleanup).
	// origin: hot-update S7 plan §11.3 / T52
	testharness.AssertNoGoroutineLeak(t, "receiver real storage", goroutines)
	testharness.AssertNoFDLeak(t, "receiver real storage", fds, 0)
}

func exerciseSyncedPrefix(t *testing.T, scenario string) {
	t.Helper()
	f := newReceiverFixture(t, false)
	requireStep(t, f.actor)
	assertPrefix(t, f, 1)
	before := receiverState(f.actor)
	var cut x.ReceiverState
	switch scenario {
	case "short_success", "partial_error", "full_error", "partial_cut":
		f.sink.write = func(p []byte, off int64) (int, error) {
			f.sink.write = nil
			length := 3
			if scenario == "full_error" {
				length = len(p)
			}
			n, err := f.sink.file.WriteAt(p[:length], off)
			if err == nil && scenario != "short_success" {
				err = errInjected
			}
			return n, err
		}
		err := f.actor.Step(testDeadline(t))
		if scenario == "short_success" {
			if err != nil {
				t.Fatal(err)
			}
		} else {
			if !errors.Is(err, errInjected) {
				t.Fatalf("short write error = %v", err)
			}
			assertCheckpoint(t, receiverState(f.actor).Checkpoint, f.chunks[:1])
			if scenario == "partial_cut" {
				cut = requireFreeze(t, f.actor)
				assertPrefix(t, f, 1)
				f.actor = requireImport(t, f, cut)
			}
			requireStep(t, f.actor)
		}
	case "sync_failure":
		f.sink.sync = func() error { return errInjected }
		if err := f.actor.Step(testDeadline(t)); !errors.Is(err, errInjected) {
			t.Fatalf("Sync failure = %v", err)
		}
		assertCheckpoint(t, receiverState(f.actor).Checkpoint, f.chunks[:1])
		if f.reader.confirms != 1 {
			t.Fatal("failed Sync advanced reader")
		}
		f.sink.sync = nil
		cut = requireFreeze(t, f.actor)
		assertPrefix(t, f, 1)
		f.actor = requireImport(t, f, cut)
		requireStep(t, f.actor)
	case "sync_blocked_cut", "confirmation_blocked_cut":
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		t.Cleanup(func() { once.Do(func() { close(release) }) })
		if scenario == "sync_blocked_cut" {
			f.sink.sync = func() error { close(entered); <-release; return nil }
		} else {
			f.reader.confirm = func(x.SinkCheckpoint) error { close(entered); <-release; return nil }
		}
		done := make(chan error, 1)
		stepDeadline := testDeadline(t)
		go func() { done <- f.actor.Step(stepDeadline); close(done) }()
		t.Cleanup(func() { once.Do(func() { close(release) }); <-done })
		awaitSignal(t, entered)
		if scenario == "sync_blocked_cut" {
			assertCheckpoint(t, receiverState(f.actor).Checkpoint, f.chunks[:1])
		} else {
			assertPrefix(t, f, 2)
		}
		if f.reader.confirms != 1 {
			t.Fatal("blocked Sync advanced reader")
		}
		deadline, err := controlbudget.After(20 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.actor.Freeze(deadline); !errors.Is(err, controlbudget.ErrExpired) {
			t.Fatalf("cut through blocked Sync = %v", err)
		}
		if f.sink.freezes != 0 {
			t.Fatal("Freeze reached sink while its Sync was unjoined")
		}
		once.Do(func() { close(release) })
		if err := awaitError(t, done); err != nil {
			t.Fatal(err)
		}
		f.sink.sync = nil
		f.reader.confirm = nil
	case "confirmation_cut":
		f.reader.confirm = func(x.SinkCheckpoint) error { return errInjected }
		if err := f.actor.Step(testDeadline(t)); !errors.Is(err, errInjected) {
			t.Fatalf("confirmation failure = %v", err)
		}
		assertPrefix(t, f, 2)
		if _, err := f.actor.Freeze(testDeadline(t)); !errors.Is(err, errInjected) {
			t.Fatalf("unconfirmed cut = %v", err)
		}
		f.reader.confirm = nil
		cut = requireFreeze(t, f.actor)
		f.actor = requireImport(t, f, cut)
	}
	assertPrefix(t, f, 2)
	cut = requireFreeze(t, f.actor)
	if cut.ByteDeadline != before.ByteDeadline || cut.Budget != before.Budget {
		t.Fatal("cut refreshed the original transfer budget")
	}
	assertCheckpoint(t, cut.Reader.SinkCheckpoint, f.chunks)
	f.actor = requireImport(t, f, cut)
	if err := f.actor.Step(testDeadline(t)); err == nil || errors.Is(err, io.EOF) || f.sink.commits != 0 {
		t.Fatalf("EOF without grant authorized a primitive: %v", err)
	}
	if err := f.actor.Reserve(testGrant(f.config.Identity)); err != nil {
		t.Fatal(err)
	}
	if err := f.actor.Step(testDeadline(t)); !errors.Is(err, io.EOF) {
		t.Fatalf("commit = %v", err)
	}
	if err := f.actor.Step(testDeadline(t)); !errors.Is(err, io.EOF) || f.sink.commits != 1 {
		t.Fatalf("repeated commit = %v, calls=%d", err, f.sink.commits)
	}
	got, err := os.ReadFile(f.sink.destination)
	if err != nil || !bytes.Equal(got, bytes.Join(f.chunks, nil)) {
		t.Fatalf("destination = %q, %v", got, err)
	}
	if err := f.actor.Stop(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S7 plan §3 / §4 / T08
func TestReceiverRejectsChangedChunksAndPrematureEOF(t *testing.T) {
	for _, defect := range []string{"index", "offset", "sequence", "length", "digest", "size", "empty", "eof"} {
		t.Run(defect, func(t *testing.T) {
			f := newReceiverFixture(t, false)
			chunk := &f.reader.chunks[0]
			switch defect {
			case "index":
				chunk.Receipt.Index++
			case "offset":
				chunk.Receipt.Offset++
			case "sequence":
				chunk.Receipt.StreamSeq = 0
			case "length":
				chunk.Receipt.Length++
			case "digest":
				chunk.Receipt.SHA256[0]++
			case "size":
				f.actor.(*receiver).state.Budget.Size = 1
			case "empty":
				chunk.Data = nil
			case "eof":
				f.reader.chunks = nil
			default:
				t.Fatal(defect)
			}
			if err := f.actor.Step(testDeadline(t)); err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("accepted %s: %v", defect, err)
			}
			if f.sink.writes != 0 || f.reader.confirms != 0 || f.sink.commits != 0 {
				t.Fatal("malformed input caused a sink effect")
			}
			if err := f.actor.Reserve(testGrant(f.config.Identity)); err == nil {
				t.Fatal("failed receiver accepted a late grant")
			}
			if err := f.actor.Step(testDeadline(t)); err == nil || f.sink.commits != 0 {
				t.Fatal("failed receiver entered a primitive")
			}
		})
	}
}

// origin: hot-update S7 plan §3 / §8.2 / T08 / M08
func TestReceiverImportRejectsInconsistentPrefixAndKeepsDeadline(t *testing.T) {
	f := newReceiverFixture(t, false)
	requireStep(t, f.actor)
	state := requireFreeze(t, f.actor)
	for _, defect := range []string{"identity", "boot", "budget", "offset", "hash_ahead", "hash_corrupt", "reader", "object_version", "sequence", "grant", "inflight", "outcome"} {
		t.Run(defect, func(t *testing.T) {
			bad := state
			bad.Checkpoint = copyCheckpoint(state.Checkpoint)
			switch defect {
			case "identity":
				bad.Identity.EntryNonce = "replacement"
			case "boot":
				bad.ByteDeadline.BootID = "another-boot"
			case "budget":
				bad.Budget.RemainingNS++
			case "offset":
				bad.Atomic.DurableOffset++
			case "hash_ahead":
				h := sha256.New()
				_, _ = h.Write(bytes.Join(f.chunks, nil))
				bad.Checkpoint.SHA256State, _ = h.(encoding.BinaryMarshaler).MarshalBinary()
				bad.Reader.SinkCheckpoint = copyCheckpoint(bad.Checkpoint)
			case "hash_corrupt":
				bad.Checkpoint.SHA256State[0]++
			case "reader":
				bad.Reader.SinkCheckpoint.ChunkIndex++
			case "object_version":
				bad.Reader.ObjectSpec.Identity.ObjectVersion = "other-object"
			case "sequence":
				bad.Checkpoint.LastConsumedStreamSeq = 0
				bad.Reader.SinkCheckpoint = copyCheckpoint(bad.Checkpoint)
			case "grant":
				bad.Grant = testGrant(state.Identity)
				bad.Grant.Identity.EntryNonce = "replacement"
			case "inflight":
				bad.GateClosed = true
				bad.Outcome.Outcome = x.OutcomeInflight
			case "outcome":
				bad.Outcome.Outcome = x.OutcomeLanded
			default:
				t.Fatal(defect)
			}
			if _, err := Import(f.config, bad); err == nil {
				t.Fatalf("import accepted %s", defect)
			}
		})
	}
	f.actor = requireImport(t, f, state)
	if !reflect.DeepEqual(receiverState(f.actor), state) {
		t.Fatal("import changed state")
	}
	state.Checkpoint.SHA256State[0]++
	state.Reader.SinkCheckpoint.SHA256State[0]++
	requireStep(t, f.actor)
	assertPrefix(t, f, 2)
	if f.sink.freezes != 1 || f.sink.writes != 2 {
		t.Fatal("import repaired or rewrote the original sink")
	}
}

type receiverFixture struct {
	actor  Receiver
	config Config
	reader *memoryReader
	sink   *fileSink
	chunks [][]byte
}

func newReceiverFixture(t *testing.T, inline bool) *receiverFixture {
	t.Helper()
	dir := t.TempDir()
	file, err := os.OpenFile(filepath.Join(dir, "pending"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	sink := &fileSink{file: file, destination: filepath.Join(dir, "destination")}
	t.Cleanup(func() { _ = sink.file.Close() })
	chunks := [][]byte{[]byte("first synced prefix\x00\xff"), []byte("distinct second chunk with a partial suffix")}
	if inline {
		chunks = [][]byte{bytes.Join(chunks, nil)}
	}
	reader := &memoryReader{sink: sink}
	reader.checkpoint, _ = emptyCheckpoint()
	var offset int64
	for index, data := range chunks {
		seq := uint64(7 + index*5) // A real stream can contain other subjects between chunks.
		if inline {
			seq = 0
		}
		reader.chunks = append(reader.chunks, x.Chunk{Data: bytes.Clone(data), Receipt: x.ChunkReceipt{
			Index: uint32(index), Offset: offset, Length: uint32(len(data)), StreamSeq: seq, SHA256: sha256.Sum256(data),
		}})
		offset += int64(len(data))
	}
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer",
		EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "version"}
	if inline {
		id.ObjectVersion = ""
	}
	reader.spec = x.ObjectSpec{Identity: id, Size: offset}
	config := Config{Base: x.Base{Clock: receiverTestClock}, Identity: id,
		Budget: x.BudgetSeal{StartedAt: time.Now().UTC(), Size: offset, EffectiveBudgetNS: int64(time.Minute), RemainingNS: int64(time.Minute), SourceSealed: true},
		Reader: reader, Atomic: sink}
	actor, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	return &receiverFixture{actor: actor, config: config, reader: reader, sink: sink, chunks: chunks}
}

func receiverTestClock() (string, int64, error) {
	now, err := controlbudget.Now()
	return "receiver-test-boot", now, err
}

func testDeadline(t *testing.T) x.Deadline {
	t.Helper()
	deadline, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return deadline
}

func testGrant(id x.Identity) x.CommitGrant {
	h := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	return x.CommitGrant{Identity: id, GrantNonce: "grant", ReceiverInstance: "receiver", TerminalID: hex.EncodeToString(h[:])}
}

func receiverState(actor Receiver) x.ReceiverState {
	r := actor.(*receiver)
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.state
	state.Checkpoint = copyCheckpoint(state.Checkpoint)
	state.Reader.SinkCheckpoint = copyCheckpoint(state.Reader.SinkCheckpoint)
	return state
}

func requireStep(t *testing.T, actor Receiver) {
	t.Helper()
	if err := actor.Step(testDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

func requireFreeze(t *testing.T, actor Receiver) x.ReceiverState {
	t.Helper()
	state, err := actor.Freeze(testDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	return state
}

func requireImport(t *testing.T, f *receiverFixture, state x.ReceiverState) Receiver {
	t.Helper()
	actor, err := Import(f.config, state)
	if err != nil {
		t.Fatal(err)
	}
	reads, writes := f.reader.reads, f.sink.writes
	if err := actor.Step(testDeadline(t)); err == nil {
		t.Fatal("imported receiver was not sleeping")
	}
	if f.reader.reads != reads || f.sink.writes != writes {
		t.Fatal("sleeping import touched a reader or sink before Thaw")
	}
	if err := Thaw(actor); err != nil {
		t.Fatal(err)
	}
	return actor
}

func assertPrefix(t *testing.T, f *receiverFixture, chunks int) {
	t.Helper()
	assertCheckpoint(t, receiverState(f.actor).Checkpoint, f.chunks[:chunks])
	data, err := os.ReadFile(f.sink.file.Name())
	if err != nil || !bytes.Equal(data, bytes.Join(f.chunks[:chunks], nil)) {
		t.Fatalf("independent disk prefix = %q, %v", data, err)
	}
}

func assertCheckpoint(t *testing.T, cp x.SinkCheckpoint, chunks [][]byte) {
	t.Helper()
	body := bytes.Join(chunks, nil)
	h := sha256.New()
	if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(cp.SHA256State); err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256(body)
	wantSeq := uint64(0)
	if len(chunks) > 0 {
		wantSeq = uint64(7 + (len(chunks)-1)*5)
	}
	if cp.DurableOffset != int64(len(body)) || cp.ChunkIndex != uint32(len(chunks)) ||
		cp.LastConsumedStreamSeq != wantSeq || !bytes.Equal(h.Sum(nil), wantHash[:]) {
		t.Fatalf("checkpoint offset/index/seq/hash = %d/%d/%d/%x, want %d/%d/%d/%x",
			cp.DurableOffset, cp.ChunkIndex, cp.LastConsumedStreamSeq, h.Sum(nil), len(body), len(chunks), wantSeq, wantHash)
	}
}

func awaitSignal(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not reach the requested boundary")
	}
}

func awaitError(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("operation did not join")
		return nil
	}
}

type memoryReader struct {
	sink       *fileSink
	spec       x.ObjectSpec
	chunks     []x.Chunk
	next       int
	reads      int
	checkpoint x.SinkCheckpoint
	confirms   int
	stops      int
	confirm    func(x.SinkCheckpoint) error
	freeze     func(*x.ReaderState)
}

func (m *memoryReader) ReadChunk(x.Deadline) (x.Chunk, error) {
	m.reads++
	if m.next == len(m.chunks) {
		return x.Chunk{}, io.EOF
	}
	chunk := m.chunks[m.next]
	m.next++
	return chunk, nil
}

func (m *memoryReader) ConfirmSynced(cp x.SinkCheckpoint) error {
	if m.confirm != nil {
		if err := m.confirm(cp); err != nil {
			return err
		}
	}
	if cp.DurableOffset > m.sink.durable {
		return errors.New("confirmation preceded successful Sync")
	}
	m.checkpoint = copyCheckpoint(cp)
	m.confirms++
	return nil
}

func (m *memoryReader) Freeze(x.Deadline) (x.ReaderState, error) {
	m.next = int(m.checkpoint.ChunkIndex)
	if m.checkpoint.ChunkIndex == 0 && m.checkpoint.DurableOffset > 0 {
		m.next = len(m.chunks)
	}
	state := x.ReaderState{ObjectSpec: m.spec, Phase: x.ReaderOpen, SinkCheckpoint: copyCheckpoint(m.checkpoint)}
	if m.freeze != nil {
		m.freeze(&state)
	}
	return state, nil
}

func (m *memoryReader) Stop(x.Deadline) error { m.stops++; return nil }

// This port is only a unit fixture: it performs real IO, while omitting the
// transferpath traversal/import/OFD policy that the production_ports arm owns.
type fileSink struct {
	file                          *os.File
	destination                   string
	durable                       int64
	writes, syncs, freezes, stops int
	commits                       int
	outcome                       x.ReceiverOutcome
	write                         func([]byte, int64) (int, error)
	sync                          func() error
	freeze                        func(*x.AtomicState)
	commit                        func(x.CommitGrant) x.ReceiverOutcome
}

func (s *fileSink) WriteAt(p []byte, off int64) (int, error) {
	s.writes++
	if s.write != nil {
		return s.write(p, off)
	}
	return s.file.WriteAt(p, off)
}

func (s *fileSink) Sync() error {
	if s.sync != nil {
		if err := s.sync(); err != nil {
			return err
		}
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	stat, err := s.file.Stat()
	if err == nil {
		s.durable = stat.Size()
		s.syncs++
	}
	return err
}

func (s *fileSink) Freeze(x.Deadline) (x.AtomicState, []handoff.Resource, error) {
	s.freezes++
	stat, err := s.file.Stat()
	if err != nil {
		return x.AtomicState{}, nil, err
	}
	if stat.Size() != s.durable {
		if err := s.file.Truncate(s.durable); err != nil {
			return x.AtomicState{}, nil, err
		}
		if err := s.file.Sync(); err != nil {
			return x.AtomicState{}, nil, err
		}
	}
	state := x.AtomicState{DurableOffset: s.durable, PrimitiveOutcome: s.outcome}
	if s.freeze != nil {
		s.freeze(&state)
	}
	return state, nil, nil
}

func (s *fileSink) Commit(grant x.CommitGrant, _ x.Deadline) x.ReceiverOutcome {
	s.commits++
	if s.commit != nil {
		return s.commit(grant)
	}
	if err := os.Rename(s.file.Name(), s.destination); err != nil {
		return x.ReceiverOutcome{Grant: grant, Outcome: x.OutcomeNotEntered, Error: err.Error()}
	}
	s.outcome = x.ReceiverOutcome{Grant: grant, Outcome: x.OutcomeLanded, Primitive: x.PrimitiveRename}
	parent, err := os.Open(filepath.Dir(s.destination))
	if err == nil {
		err = parent.Sync()
		_ = parent.Close()
	}
	s.outcome.ParentSynced = err == nil
	return s.outcome
}

func (s *fileSink) Stop(x.Deadline) error {
	s.stops++
	err := s.file.Close()
	if errors.Is(err, os.ErrClosed) {
		err = nil
	}
	return err
}

func requireCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), code) {
		t.Fatalf("error = %v, want code %s", err, code)
	}
}
