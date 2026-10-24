package xferclient

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §11.3 / P25
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeGoroutines, beforeFDs := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer client", beforeGoroutines)
	testharness.AssertNoFDLeak(t, "transfer client", beforeFDs, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	legacyCalls := 0
	legacy, err := NewLegacy(LegacyConfig{TransferID: "legacy-original", PutOnce: func(x.Deadline) (x.LegacyResult, error) {
		legacyCalls++
		return x.LegacyResult{}, errors.New("legacy reply lost")
	}})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		_, _ = legacy.Run(testDeadline(t, time.Second))
	}
	if err := legacy.Stop(testDeadline(t, time.Second)); err != nil || legacyCalls != 1 {
		t.Fatal("legacy lifecycle repeated its one-shot operation")
	}
	for _, name := range []string{"success", "timeout", "cancel", "revoke", "pending_ack", "partial_factory", "stop_failure", "directory_sync"} {
		verb := "push"
		if name == "pending_ack" || name == "directory_sync" {
			verb = "pull"
		}
		f := newClientFixture(t, verb)
		var cancelDone chan error
		switch name {
		case "timeout":
			f.transport.lost, f.transport.statusMode = x.OpCommit, "unavailable"
		case "cancel":
			cancelDone = make(chan error, 1)
			f.transport.onRequest = func(request x.Request) (x.Response, x.Attempt, error, bool) {
				if request.Op == x.OpSource {
					go func() { _, err := f.client.Cancel(x.CancelUser, testDeadline(t, time.Second)); cancelDone <- err }()
					<-f.client.byteStop
				}
				return x.Response{}, x.Attempt{}, nil, false
			}
		case "revoke":
			if _, err := f.client.Cancel(x.CancelRevoked, testDeadline(t, time.Second)); ErrorCode(err) != x.CodeCanceled {
				t.Fatalf("unadmitted revoke: %v", err)
			}
		case "pending_ack":
			f.transport.lost, f.transport.statusMode = x.OpTerminal, "missing"
		case "partial_factory":
			f.client.cfg.MakeWriter = func(_ jetstream.JetStream, spec x.ObjectSpec) (x.ObjectWriter, error) {
				f.writers++
				f.writer.spec = spec
				return f.writer, errors.New("partial writer construction")
			}
		case "stop_failure":
			f.writer.failStop = true
		case "directory_sync":
			f.atomic.failParent = true
		}
		budget := time.Second
		if name == "timeout" {
			budget = 70 * time.Millisecond
		}
		receipt, err := f.client.Run(testDeadline(t, budget))
		if name == "success" || name == "stop_failure" {
			if err != nil || receipt.Kind != "complete" {
				t.Fatalf("lifecycle success: %v", err)
			}
		} else if err == nil || receipt.Kind != "" {
			t.Fatalf("%s claimed a terminal before proof: %+v %v", name, receipt, err)
		}
		if cancelDone != nil {
			if err := <-cancelDone; err != nil {
				t.Fatal(err)
			}
			if f.writer.appends != 0 || f.transport.counts[x.OpCommit] != 0 || f.transport.counts[x.OpCancel] == 0 {
				t.Fatal("cancel did not close byte admission")
			}
		}
		if name == "revoke" && len(f.transport.trace) != 0 {
			t.Fatal("precreation revoke invoked transport")
		}
		if name == "timeout" {
			original := f.client.deadline
			f.transport.statusMode = "live"
			if _, err := f.client.Run(testDeadline(t, time.Second)); err == nil || f.client.deadline != original || f.writers != 1 {
				t.Fatal("a second Run refreshed the original deadline")
			}
		}
		if name == "pending_ack" {
			f.transport.statusMode = "live"
			if receipt, err := f.client.Run(testDeadline(t, time.Second)); err != nil || receipt.Kind != "complete" || f.atomic.commits != 1 {
				t.Fatalf("pending settlement reentry: %v", err)
			}
		}
		if name == "directory_sync" {
			if _, err := f.client.Run(testDeadline(t, time.Second)); err == nil || f.atomic.commits != 1 || f.client.outcome.Outcome != x.OutcomeLanded {
				t.Fatal("durability uncertainty reentered the landed primitive")
			}
		}
		if name == "stop_failure" {
			if err := f.client.Stop(testDeadline(t, time.Second)); err == nil || f.client.writer == nil || len(f.capacity.held) != 1 || f.transport.dataClosed != 0 {
				t.Fatal("failed writer retirement discarded its attachment or capacity")
			}
			f.writer.failStop = false
		}
		if err := f.client.Stop(testDeadline(t, time.Second)); err != nil {
			t.Fatal(err)
		}
		if len(f.capacity.held) != 0 || f.transport.dataClosed != f.transport.dataOpened {
			t.Fatal("Stop leaked a data attachment or reservation")
		}
		if name == "partial_factory" && f.writer.stops != 1 {
			t.Fatal("partial factory result was not owned by Stop")
		}
		f.close()
	}
}

// origin: hot-update S7 plan §3 / §7 / T33
func TestTransferClientSyncPrefixControlsPrimitiveAdmission(t *testing.T) {
	for _, name := range []string{"short", "primitive_only", "sync_failure", "corrupt_chunk"} {
		t.Run(name, func(t *testing.T) {
			f := newClientFixture(t, "pull")
			f.atomic.short, f.atomic.failSync, f.reader.corrupt = name == "short", name == "sync_failure", name == "corrupt_chunk"
			f.atomic.primitiveOnly = name == "primitive_only"
			receipt, err := f.client.Run(testDeadline(t, 3*time.Second))
			if name == "short" || name == "primitive_only" {
				if err != nil || receipt.Kind != "complete" || name == "short" && f.atomic.writes <= 3 || f.atomic.commits != 1 || f.reader.confirms != 3 {
					t.Fatalf("short writes broke durable prefix: %v", err)
				}
				body, err := os.ReadFile(f.atomic.target)
				if err != nil || !bytes.Equal(body, f.data) {
					t.Fatal("short syscall suffix was lost or repeated")
				}
				if f.transport.finalizedOutcome.Bytes != int64(len(f.data)) || f.transport.finalizedOutcome.SHA256 != digestOf(f.data) {
					t.Fatal("the receiver omitted its verified source facts")
				}
			} else {
				if err == nil || f.client.checkpoint.DurableOffset != 0 || f.reader.confirms != 0 || f.atomic.commits != 0 || f.transport.counts[x.OpReserve] != 0 {
					t.Fatal("unverified/unsynced bytes reached commit admission")
				}
				body, err := os.ReadFile(f.atomic.target)
				if err != nil || string(body) != "old complete contents" {
					t.Fatal("failure modified the existing target")
				}
			}
		})
	}
}

// origin: hot-update S7 plan §1.2 / §7 / lead LQ2 / T33
func TestTransferClientRateBudgetKeepsOriginalDeadline(t *testing.T) {
	for _, test := range []struct {
		size, rate int64
		seconds    int64
		refuse     bool
	}{
		{0, 0, 300, false}, {1, 0, 300, false}, {proto.XferMaxBytes, 0, 2108, false},
		{proto.XferMaxBytes, proto.XferMinThroughput, 2108, false}, {proto.XferMaxBytes, proto.XferMinThroughput + 1, 2108, false},
		{proto.XferMaxBytes, proto.XferMinThroughput - 1, 0, true}, {proto.XferMaxBytes, 1, 0, true}, {10, -1, 0, true},
	} {
		budget, err := rateBudget(test.size, test.rate)
		if test.refuse {
			if err == nil {
				t.Fatal("rate budget exceeded 2108 seconds without refusal")
			}
		} else if err != nil || budget != time.Duration(test.seconds)*time.Second {
			t.Fatalf("budget=%s err=%v", budget, err)
		}
	}
	f := newClientFixture(t, "push")
	if err := f.client.start(testDeadline(t, 20*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	deadline, started := f.client.deadline, f.client.startedNS
	f.client.cfg.Clock = func() (string, int64, error) { return "client-boot", int64(deadline) + 1, nil }
	if _, err := f.client.Run(testDeadline(t, time.Second)); err == nil || f.client.deadline != deadline || f.client.startedNS != started || len(f.transport.trace) != 0 {
		t.Fatal("reentry reset original start or admitted work after the byte deadline")
	}
}

// origin: hot-update S7 plan §7 / T33
func TestTransferClientCancellationNeverReplaysCommit(t *testing.T) {
	f := newClientFixture(t, "push")
	f.transport.lost, f.transport.statusMode = x.OpCommit, "live"
	joined := make(chan error, 1)
	f.transport.onRequest = func(request x.Request) (x.Response, x.Attempt, error, bool) {
		if request.Op == x.OpStatus && f.transport.counts[x.OpStatus] == 1 {
			go func() { _, err := f.client.Cancel(x.CancelUser, testDeadline(t, time.Second)); joined <- err }()
			<-f.client.byteStop
		}
		return x.Response{}, x.Attempt{}, nil, false
	}
	receipt, err := f.client.Run(testDeadline(t, time.Second))
	if cancelErr := <-joined; cancelErr != nil {
		t.Fatal(cancelErr)
	}
	if err == nil || ExitCode(err) != 74 || receipt.Kind != "" || f.transport.counts[x.OpCommit] != 1 || f.transport.counts[x.OpCancel] == 0 {
		t.Fatalf("cancellation replayed commit or claimed a terminal: receipt=%+v err=%v commits=%d", receipt, err, f.transport.counts[x.OpCommit])
	}
}

// origin: hot-update S7 plan §1.2 / §7 / lead LQ2 / T33
func TestTransferClientRateLimitPacesOriginalBytes(t *testing.T) {
	for _, verb := range []string{"push", "pull"} {
		t.Run(verb, func(t *testing.T) {
			f := newClientFixture(t, verb, func(cfg *Config) { cfg.Intent.RateLimitBPS = 720000 })
			receipt, err := f.client.Run(testDeadline(t, 3*time.Second))
			if err != nil || receipt.Kind != "complete" {
				t.Fatal(err)
			}
			times := f.writer.ioTimes
			if verb == "pull" {
				times = f.atomic.ioTimes
			}
			if len(times) != 3 {
				t.Fatal("pacing did not exercise all three original chunks")
			}
			// The remaining 39,232 bytes after the first chunk need over 54 ms
			// at this requested cap. Compare observed IO, not a fixture sleep.
			if time.Duration(times[2]-times[0]) < 50*time.Millisecond {
				t.Fatal("the requested rate did not pace actual byte IO")
			}
		})
	}
}

// origin: hot-update S7 plan §4 / §7 / T33
func TestTransferClientReceiptCannotContradictLandedPrimitive(t *testing.T) {
	for _, defect := range []string{"failed", "wrong_slot", "abandoned"} {
		t.Run(defect, func(t *testing.T) {
			f := newClientFixture(t, "pull")
			f.transport.onRequest = func(request x.Request) (x.Response, x.Attempt, error, bool) {
				if request.Op != x.OpTerminal {
					return x.Response{}, x.Attempt{}, nil, false
				}
				receipt := f.transport.terminal()
				if defect == "wrong_slot" {
					receipt.TerminalID = "foreign terminal"
				} else {
					receipt.Kind, receipt.Code = "failed", x.CodeBudgetExceeded
					if defect == "abandoned" {
						receipt.Code, receipt.OutcomeUnknown = x.ReasonSettlementAbandoned, true
					}
				}
				return x.Response{Receipt: &receipt}, x.Attempt{Sent: true}, nil, true
			}
			receipt, err := f.client.Run(testDeadline(t, time.Second))
			if err == nil || ExitCode(err) != 74 || f.atomic.commits != 1 {
				t.Fatalf("contradictory receipt acknowledged: %+v %v", receipt, err)
			}
			if defect == "abandoned" {
				if !receipt.OutcomeUnknown || receipt.Kind != "failed" {
					t.Fatal("abandonment lost its explicit unknown outcome")
				}
			} else if receipt.Kind != "" || ErrorCode(err) != x.CodeTerminalConflict {
				t.Fatal("opposite receipt discharged the original outcome")
			}
		})
	}
}

// origin: hot-update S7 plan §1.2 / §7 / T33
func TestTransferClientSettlementOutlivesByteDeadline(t *testing.T) {
	for _, verb := range []string{"push", "pull"} {
		t.Run(verb, func(t *testing.T) {
			f := newClientFixture(t, verb)
			f.transport.lost, f.transport.statusMode = x.OpCommit, "missing"
			if verb == "pull" {
				f.transport.lost = x.OpTerminal
			}
			if _, err := f.client.Run(testDeadline(t, time.Second)); ExitCode(err) != 74 {
				t.Fatal("lost terminal exercise did not remain unresolved")
			}
			originalDeadline := f.client.deadline
			f.client.cfg.Clock = func() (string, int64, error) { return "client-boot", int64(originalDeadline) + 1, nil }
			if verb == "push" {
				f.transport.statusMode = "live"
				if _, err := f.client.Run(testDeadline(t, 3*time.Second)); ErrorCode(err) != x.CodeCommitInflight || f.transport.counts[x.OpCommit] != 1 {
					t.Fatal("expired byte admission replayed remote commit")
				}
			}
			f.transport.statusMode = ""
			receipt, err := f.client.Run(testDeadline(t, 3*time.Second))
			if err != nil || receipt.Kind != "complete" || f.client.deadline != originalDeadline {
				t.Fatalf("byte deadline discarded settlement: %+v %v", receipt, err)
			}
			if verb == "pull" && (f.atomic.commits != 1 || f.readers != 1) {
				t.Fatal("settlement repeated the local primitive or byte IO")
			}
			if verb == "push" && (f.writers != 1 || f.transport.counts[x.OpCommit] != 1) {
				t.Fatal("settlement started another writer or commit")
			}
		})
	}
}

type clientReader struct {
	fixture                *clientFixture
	spec                   x.ObjectSpec
	checkpoint             x.SinkCheckpoint
	reads, confirms, stops int
	corrupt                bool
}

func (r *clientReader) ReadChunk(_ x.Deadline) (x.Chunk, error) {
	r.reads++
	offset := r.checkpoint.DurableOffset
	if offset >= int64(len(r.fixture.data)) {
		return x.Chunk{}, io.EOF
	}
	length := min(int64(r.spec.ChunkSize), int64(len(r.fixture.data))-offset)
	data := append([]byte(nil), r.fixture.data[offset:offset+length]...)
	receipt := x.ChunkReceipt{Index: r.checkpoint.ChunkIndex, Offset: offset, Length: uint32(length), StreamSeq: uint64(r.checkpoint.ChunkIndex) + 10, SHA256: sha256.Sum256(data)}
	if r.corrupt {
		receipt.Offset++
	}
	return x.Chunk{Receipt: receipt, Data: data}, nil
}
func (r *clientReader) ConfirmSynced(checkpoint x.SinkCheckpoint) error {
	r.confirms++
	if r.fixture.atomic.syncedOffset < checkpoint.DurableOffset {
		r.fixture.t.Error("reader confirmed bytes before Sync")
	}
	r.checkpoint = checkpoint
	return nil
}
func (r *clientReader) Freeze(_ x.Deadline) (x.ReaderState, error) {
	return x.ReaderState{ObjectSpec: r.spec, SinkCheckpoint: r.checkpoint}, nil
}
func (r *clientReader) Stop(_ x.Deadline) error { r.stops++; return nil }

type clientAtomic struct {
	fixture                                    *clientFixture
	file, parent                               *os.File
	target                                     string
	writes, commits, stops, syncs              int
	syncedOffset                               int64
	short, failSync, failParent, primitiveOnly bool
	unknownOutcome                             bool
	ioTimes                                    []int64
	retained                                   *x.ReceiverOutcome
}

func (a *clientAtomic) WriteAt(data []byte, offset int64) (int, error) {
	a.writes++
	now, _ := controlbudget.Now()
	a.ioTimes = append(a.ioTimes, now)
	if a.short && len(data) > 5 {
		data = data[:5]
	}
	return a.file.WriteAt(data, offset)
}
func (a *clientAtomic) Sync() error {
	a.syncs++
	if a.failSync {
		return errors.New("disk Sync failed")
	}
	if err := a.file.Sync(); err != nil {
		return err
	}
	info, err := a.file.Stat()
	if err != nil {
		return err
	}
	a.syncedOffset = info.Size()
	return nil
}
func (a *clientAtomic) Commit(grant x.CommitGrant, _ x.Deadline) x.ReceiverOutcome {
	// Match AtomicPort's post-effect contract: commits counts real rename
	// attempts, while reentry only retries Sync on the retained parent.
	// origin: S7 internal review rv-recovery F04
	if a.retained != nil {
		outcome := *a.retained
		if !a.failParent && a.parent.Sync() == nil {
			outcome.ParentSynced, outcome.Code, outcome.Error = true, "", ""
			a.retained = &outcome
		}
		return outcome
	}
	a.commits++
	outcome := x.ReceiverOutcome{Grant: grant, Primitive: x.PrimitiveRename, Outcome: x.OutcomeNotEntered, Bytes: int64(len(a.fixture.data)), SHA256: digestOf(a.fixture.data)}
	if a.primitiveOnly {
		outcome.Bytes, outcome.SHA256 = 0, ""
	}
	if err := os.Rename(a.file.Name(), a.target); err != nil {
		outcome.Code, outcome.Error = x.CodeObjectConflict, err.Error()
		return outcome
	}
	outcome.Outcome = x.OutcomeLanded
	defer func() { a.retained = &outcome }()
	if a.failParent {
		outcome.Code = x.CodeDurabilityUnknown
		return outcome
	}
	if err := a.parent.Sync(); err != nil {
		outcome.Code, outcome.Error = x.CodeDurabilityUnknown, err.Error()
		return outcome
	}
	outcome.ParentSynced = true
	if a.unknownOutcome {
		outcome.Outcome = ""
	}
	return outcome
}

// origin: hot-update S7 plan §4 / §7 / T33
func TestTransferClientUnresolvedPrimitiveNeverPublishesOutcome(t *testing.T) {
	f := newClientFixture(t, "pull")
	f.atomic.unknownOutcome = true
	for range 2 {
		if _, err := f.client.Run(testDeadline(t, time.Second)); ExitCode(err) != 74 {
			t.Fatal("unresolved primitive claimed a terminal")
		}
	}
	if f.atomic.commits != 1 || f.transport.counts[x.OpTerminal] != 0 {
		t.Fatal("unresolved primitive was repeated or published without outcome evidence")
	}
}
func (a *clientAtomic) Freeze(_ x.Deadline) (x.AtomicState, []handoff.Resource, error) {
	return x.AtomicState{}, nil, errors.New("unit IO port has no image implementation")
}
func (a *clientAtomic) Stop(_ x.Deadline) error {
	a.stops++
	return errors.Join(a.file.Close(), a.parent.Close())
}
