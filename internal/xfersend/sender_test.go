package xfersend

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §2.2 / §12.2 P08 / T19
func TestTransferSourceMutationNeverPublishesMetadata(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for _, name := range []string{"path_replacement", "same_inode", "growth", "truncation", "after_last_receipt", "changed_then_restored", "empty_source"} {
		t.Run(name, func(t *testing.T) {
			data := bytes.Repeat([]byte("original source bytes\n"), 10000)
			if name == "empty_source" {
				data = nil
			}
			f := newSenderFixture(t, data)
			seal := requireSeal(t, f)
			if seal.SHA256 != digest(data) || seal.Size != int64(len(data)) {
				t.Fatalf("seal does not describe original bytes: %+v", seal)
			}
			original, err := f.file.Stat()
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "path_replacement":
				if err := os.Rename(f.path, f.path+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(f.path, []byte("replacement inode"), 0600); err != nil {
					t.Fatal(err)
				}
			case "same_inode":
				rewriteSource(t, f, 0, []byte("changed!"), original.ModTime())
			case "growth":
				if err := f.file.Truncate(int64(len(data) + 1)); err != nil {
					t.Fatal(err)
				}
			case "truncation":
				if err := f.file.Truncate(int64(len(data) - 1)); err != nil {
					t.Fatal(err)
				}
			case "after_last_receipt":
				f.writer.onAppend = func(index uint32) {
					if index+1 == seal.Chunks {
						rewriteSource(t, f, 0, []byte("changed!"), original.ModTime())
					}
				}
			case "changed_then_restored":
				rewriteSource(t, f, 0, []byte("changed!"), original.ModTime())
				f.writer.onAppend = func(index uint32) {
					if index == 0 {
						rewriteSource(t, f, 0, data[:8], original.ModTime())
					}
				}
			}
			var stepErr error
			for step := uint32(0); step <= seal.Chunks; step++ {
				stepErr = f.actor.Step(f.deadline())
				if stepErr != nil {
					break
				}
			}
			calls, published := f.writer.snapshot()
			if name == "path_replacement" || name == "empty_source" {
				if stepErr != nil || calls != 1 || !bytes.Equal(published, data) {
					t.Fatalf("original FD did not finalize exactly once: err=%v metadata=%d bytes=%d", stepErr, calls, len(published))
				}
				if err := f.actor.Step(f.deadline()); err != nil {
					t.Fatal(err)
				}
				if n, _ := f.writer.snapshot(); n != 1 {
					t.Fatalf("repeated Step republished metadata: %d", n)
				}
			} else if stepErr == nil || !strings.Contains(stepErr.Error(), x.CodeSourceChanged) || calls != 0 {
				t.Fatalf("changed source reached metadata: err=%v metadata=%d", stepErr, calls)
			}
			position, err := f.file.Seek(0, io.SeekCurrent)
			if err != nil || position != 7 {
				t.Fatalf("ReadAt changed original OFD offset: offset=%d err=%v", position, err)
			}
			requireStop(t, f)
		})
	}

	// Real seals and receipts must reject source changes before metadata.
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("production_storage_%d", round), func(t *testing.T) { exerciseSourceMutationStorage(t) })
	}
	// Direct after the rounds (formerly deferred: same point, before t.Cleanup).
	// origin: hot-update S7 plan §11.3 / T52
	testharness.AssertNoGoroutineLeak(t, "sender real storage", goroutines)
	testharness.AssertNoFDLeak(t, "sender real storage", fds, 0)
}

// origin: hot-update S7 plan §12.2 P08 / T19: no unsealed first byte.
func TestTransferSenderRequiresDurableSealBeforeFirstChunk(t *testing.T) {
	f := newSenderFixture(t, []byte("unsealed source"))
	if err := f.actor.Step(f.deadline()); err == nil || !strings.Contains(err.Error(), x.CodeIntentConflict) {
		t.Fatalf("unsealed Step: %v", err)
	}
	_, data := f.writer.snapshot()
	if len(data) != 0 || len(f.transport.requests) != 0 {
		t.Fatal("unsealed Step produced a byte or source admission")
	}
	f.transport.reply = func(x.Request) (x.Response, x.Attempt, error) {
		return x.Response{}, x.Attempt{Sent: true}, errors.New("lost source reply")
	}
	if _, err := f.actor.Seal(f.deadline()); err == nil {
		t.Fatal("lost source admission was accepted")
	}
	if err := f.actor.Step(f.deadline()); err == nil || !strings.Contains(err.Error(), x.CodeIntentConflict) {
		t.Fatalf("locally hashed but unacknowledged source sent bytes: %v", err)
	}
	f.transport.reply = nil
	f.transport.budget.SourceSealed = true
	f.transport.budget.Size = int64(len("unsealed source"))
	requireSeal(t, f)
	if got := f.transport.requests; len(got) != 2 || got[0].Op != x.OpSource || got[1].Op != x.OpStatus || got[0].CallID != got[1].CallID {
		t.Fatalf("source replay preceded reconciliation: %+v", got)
	}
	requireStop(t, f)
}

// origin: hot-update S7 plan §2.2 / §3 / T19
func TestTransferSenderAdvancesOnlyExactReceipts(t *testing.T) {
	for _, name := range []string{"lost_ack", "wrong_index", "wrong_offset", "wrong_length", "wrong_hash", "zero_seq", "metadata_mismatch"} {
		t.Run(name, func(t *testing.T) {
			f := newSenderFixture(t, bytes.Repeat([]byte("receipt"), 16000))
			seal := requireSeal(t, f)
			f.writer.fault = name
			err := f.actor.Step(f.deadline())
			if name == "metadata_mismatch" {
				for n := uint32(1); n <= seal.Chunks && err == nil; n++ {
					err = f.actor.Step(f.deadline())
				}
				if err == nil || !strings.Contains(err.Error(), x.CodeObjectVersionMismatch) {
					t.Fatalf("foreign metadata receipt accepted: %v", err)
				}
			} else {
				if err == nil {
					t.Fatal("unknown or nonmatching receipt advanced")
				}
				state, err := f.actor.Freeze(f.deadline())
				if err != nil || state.Offset != 0 || !bytes.Equal(state.SHA256State, emptyHashState(t)) {
					t.Fatalf("unconfirmed source prefix exported: offset=%d err=%v", state.Offset, err)
				}
				if name == "lost_ack" {
					if state.Writer.PendingAttempt == nil || state.Writer.PendingAttempt.Index != 0 {
						t.Fatal("freeze lost original pending bytes")
					}
					if err := f.actor.Step(f.deadline()); err != nil {
						t.Fatal(err)
					}
					state, err = f.actor.Freeze(f.deadline())
					if err != nil || state.Offset != int64(seal.ChunkSize) || f.writer.appends != 1 || f.writer.resolves != 1 {
						t.Fatalf("lost ACK caused append replay or lost progress: offset=%d appends=%d resolves=%d err=%v", state.Offset, f.writer.appends, f.writer.resolves, err)
					}
				}
			}
			requireStop(t, f)
		})
	}
}

// origin: hot-update S7 plan §1.2 / LQ2 / T19
func TestTransferSenderRateAndOriginalBudgetSurviveRetry(t *testing.T) {
	f := newSenderFixture(t, bytes.Repeat([]byte("r"), 3*65536))
	f.config.Intent.RateLimitBPS = 32768
	f.config.Budget.RateLimitBPS = 32768
	f.transport.budget.RateLimitBPS = 32768
	f.recreate(t)
	seal := requireSeal(t, f)
	if seal.RateLimitBPS != 32768 {
		t.Fatal("seal omitted the original rate")
	}
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	// A forward wall-clock adjustment cannot grant a faster second chunk.
	f.utcDelta.Store(int64(time.Hour))
	short := f.deadlineAfter(30 * time.Millisecond)
	if err := f.actor.Step(short); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("rate wait did not obey operation deadline: %v", err)
	}
	if f.writer.appends != 1 {
		t.Fatal("rate limit admitted the second chunk early")
	}
	f.utcDelta.Store(0)
	f.clockDelta.Store(int64(2 * time.Second))
	if err := f.actor.Step(f.deadline()); err != nil {
		t.Fatal(err)
	}
	state, err := f.actor.Freeze(f.deadline())
	if err != nil || !state.Budget.StartedAt.Equal(f.config.Budget.StartedAt) || state.Budget.RateLimitBPS != 32768 || state.Offset != 2*65536 {
		t.Fatalf("rate/start/prefix changed on retry: %+v err=%v", state.Budget, err)
	}
	originalDeadline := state.ByteDeadline
	f.clockDelta.Store(int64(301 * time.Second))
	if err := f.actor.Step(f.deadline()); !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("byte deadline renewed: %v", err)
	}
	state, err = f.actor.Freeze(f.deadline())
	if err != nil || state.ByteDeadline != originalDeadline {
		t.Fatalf("freeze changed expired byte deadline: %+v err=%v", state.ByteDeadline, err)
	}
	requireStop(t, f)
}

// origin: hot-update S7 plan §1.2 / §12.2 P08 / T19
func TestTransferSenderRejectsInvalidSourceAdmission(t *testing.T) {
	for _, name := range []string{"wrong_declared_hash", "wrong_declared_size", "too_slow", "wrong_owner", "renewed_start", "renewed_budget", "unsealed_reply", "foreign_writer"} {
		t.Run(name, func(t *testing.T) {
			f := newSenderFixture(t, []byte("source facts"))
			switch name {
			case "wrong_declared_hash":
				f.config.Intent.SHA256 = strings.Repeat("0", 64)
				f.recreate(t)
			case "wrong_declared_size":
				f.config.Intent.Size++
				f.recreate(t)
			case "too_slow":
				f.config.Intent.Verb = "pull"
				f.config.Intent.RateLimitBPS = 1
				f.config.Budget.RateLimitBPS = 1
				f.recreate(t)
				if err := f.file.Truncate(2048); err != nil {
					t.Fatal(err)
				}
			case "foreign_writer":
				f.writer.spec.Identity.ObjectVersion = "foreign"
			default:
				f.transport.reply = func(req x.Request) (x.Response, x.Attempt, error) {
					status := f.transport.status(req)
					switch name {
					case "wrong_owner":
						status.Identity.EntryNonce = "replacement"
					case "renewed_start":
						status.Budget.StartedAt = status.Budget.StartedAt.Add(-time.Second)
					case "renewed_budget":
						status.Budget.EffectiveBudgetNS++
					case "unsealed_reply":
						status.Budget.SourceSealed = false
					}
					return x.Response{Status: &status}, x.Attempt{Sent: true}, nil
				}
			}
			_, err := f.actor.Seal(f.deadline())
			if name == "foreign_writer" && err == nil {
				err = f.actor.Step(f.deadline())
			}
			if err == nil {
				t.Fatal("invalid source admission accepted")
			}
			// Repeating Seal must not bypass cached intent validation.
			if name == "wrong_declared_hash" || name == "wrong_declared_size" {
				if _, err := f.actor.Seal(f.deadline()); err == nil {
					t.Fatal("cached invalid source seal accepted")
				}
			}
			if n, data := f.writer.snapshot(); n != 0 || len(data) != 0 {
				t.Fatal("invalid admission produced object bytes")
			}
			requireStop(t, f)
		})
	}
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type senderFixture struct {
	actor                Sender
	config               Config
	file                 *os.File
	path                 string
	writer               *receiptWriter
	transport            *sealTransport
	capacity             *senderCapacity
	clockDelta, utcDelta atomic.Int64
}

func newSenderFixture(t *testing.T, data []byte) *senderFixture {
	t.Helper()
	f := &senderFixture{path: filepath.Join(t.TempDir(), "source"), capacity: &senderCapacity{}}
	file, err := os.OpenFile(f.path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
	if err != nil {
		t.Fatal(err)
	}
	f.file = file
	t.Cleanup(func() { _ = file.Close() })
	if _, err := file.Write(data); err != nil {
		t.Fatal(err)
	}
	if _, err := file.Seek(7, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	identity := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer", EntryNonce: "entry", IntentDigest: "intent", ObjectVersion: "version"}
	f.writer = &receiptWriter{spec: x.ObjectSpec{Identity: identity, Size: int64(len(data)), SHA256: digest(data), ChunkSize: 65536,
		Chunks: uint32((len(data) + 65535) / 65536)}}
	f.config = Config{Base: x.Base{Clock: func() (string, int64, error) {
		now, err := controlbudget.Now()
		return "test-boot", now + f.clockDelta.Load(), err
	}, UTC: func() time.Time { return time.Now().Add(time.Duration(f.utcDelta.Load())) }, Capacity: f.capacity},
		Identity: identity, Intent: x.Intent{Verb: "push", Tier: "b", Size: int64(len(data)), SHA256: digest(data), ChunkSize: 65536},
		Budget: x.BudgetSeal{StartedAt: time.Now(), EffectiveBudgetNS: int64(300 * time.Second), RemainingNS: int64(300 * time.Second)}, Source: file, Writer: f.writer}
	f.transport = &sealTransport{budget: f.config.Budget}
	f.config.Transport = f.transport
	f.actor, err = New(f.config)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *senderFixture) recreate(t *testing.T) {
	t.Helper()
	// Reconfiguration occurs only before byte work; constructor ownership stays
	// with this test fixture. Release its previous accounting without closing FD.
	if err := f.capacity.Release(f.actor.(*sender).reservation); err != nil {
		t.Fatal(err)
	}
	actor, err := New(f.config)
	if err != nil {
		t.Fatal(err)
	}
	f.actor = actor
}

func (f *senderFixture) deadline() x.Deadline { return f.deadlineAfter(5 * time.Second) }
func (f *senderFixture) deadlineAfter(d time.Duration) x.Deadline {
	_, now, _ := f.config.Clock()
	return x.Deadline(now + int64(d))
}
func requireSeal(t *testing.T, f *senderFixture) x.SourceSeal {
	t.Helper()
	seal, err := f.actor.Seal(f.deadline())
	if err != nil {
		t.Fatal(err)
	}
	return seal
}
func requireStop(t *testing.T, f *senderFixture) {
	t.Helper()
	if err := f.actor.Stop(f.deadline()); err != nil {
		t.Fatal(err)
	}
	if f.capacity.held {
		t.Fatal("Stop retained source accounting")
	}
}
func rewriteSource(t *testing.T, f *senderFixture, offset int64, data []byte, mtime time.Time) {
	t.Helper()
	if n, err := f.file.WriteAt(data, offset); err != nil || n != len(data) {
		t.Fatalf("mutate original inode: n=%d err=%v", n, err)
	}
	if err := os.Chtimes(f.path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}
func emptyHashState(t *testing.T) []byte {
	t.Helper()
	state, err := sha256.New().(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return state
}

// These adversarial ports test sender decisions, not JetStream durability or
// ledger correctness. The separate production_storage branch uses real owners.
type receiptWriter struct {
	mu                       sync.Mutex
	spec                     x.ObjectSpec
	receipts                 []x.ChunkReceipt
	pending                  *x.PendingAttempt
	data                     []byte
	fault                    string
	appends, resolves, metas int
	onAppend                 func(uint32)
	blocked, entered         chan struct{}
	stopErr                  error
}

func (w *receiptWriter) Append(index uint32, data []byte, _ x.Deadline) (x.ChunkReceipt, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.entered != nil {
		close(w.entered)
		w.entered = nil
		<-w.blocked
	}
	w.appends++
	r := x.ChunkReceipt{Index: index, Offset: int64(len(w.data)), Length: uint32(len(data)), SHA256: sha256.Sum256(data), StreamSeq: uint64(7 + index*3)}
	if w.onAppend != nil {
		w.onAppend(index)
	}
	w.data = append(w.data, data...)
	switch w.fault {
	case "lost_ack":
		w.pending = &x.PendingAttempt{Index: index, Offset: r.Offset, Length: r.Length, SHA256: r.SHA256, Bytes: bytes.Clone(data)}
		w.receipts = append(w.receipts, r)
		return x.ChunkReceipt{}, errors.New("lost append ACK")
	case "wrong_index":
		r.Index++
	case "wrong_offset":
		r.Offset++
	case "wrong_length":
		r.Length++
	case "wrong_hash":
		r.SHA256[0]++
	case "zero_seq":
		r.StreamSeq = 0
	}
	w.receipts = append(w.receipts, r)
	return r, nil
}
func (w *receiptWriter) ResolvePending(x.Deadline) (x.ChunkReceipt, bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.resolves++
	if w.pending == nil {
		return x.ChunkReceipt{}, false, nil
	}
	w.pending = nil
	return w.receipts[len(w.receipts)-1], true, nil
}
func (w *receiptWriter) Finalize(x.Deadline) (x.ObjectReceipt, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.metas++
	spec := w.spec
	if w.fault == "metadata_mismatch" {
		spec.Identity.ObjectVersion = "foreign"
	}
	return x.ObjectReceipt{Spec: spec, MetadataSeq: 99, MetadataDigest: "metadata"}, nil
}
func (w *receiptWriter) Freeze(x.Deadline) (x.WriterState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	state := x.WriterState{ObjectSpec: w.spec, Phase: x.WriteOpen, Receipts: append([]x.ChunkReceipt(nil), w.receipts...)}
	if w.pending != nil {
		pending := *w.pending
		pending.Bytes = bytes.Clone(pending.Bytes)
		state.PendingAttempt = &pending
		state.Phase = x.WritePending
		state.Receipts = state.Receipts[:len(state.Receipts)-1]
	}
	if w.metas > 0 {
		state.Phase = x.WriteFinalized
		state.MetaReceipt = &x.ObjectReceipt{Spec: w.spec, MetadataSeq: 99, MetadataDigest: "metadata"}
		if w.fault == "corrupt_metadata" {
			state.MetaReceipt.Spec.Identity.ObjectVersion = "foreign"
		}
	}
	if w.fault == "retired" {
		state.Phase = x.WriteRetired
	}
	return state, nil
}
func (w *receiptWriter) Stop(x.Deadline) error { return w.stopErr }
func (w *receiptWriter) snapshot() (int, []byte) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.metas, bytes.Clone(w.data)
}

type senderCapacity struct {
	held bool
	fail bool
}

func (c *senderCapacity) Reserve(x.Reservation) (x.ReservationID, error) {
	if c.fail || c.held {
		return "", errors.New("capacity unavailable")
	}
	c.held = true
	return "reservation", nil
}
func (c *senderCapacity) Release(x.ReservationID) error {
	if !c.held {
		return errors.New("double capacity release")
	}
	c.held = false
	return nil
}

type sealTransport struct {
	budget   x.BudgetSeal
	requests []x.Request
	reply    func(x.Request) (x.Response, x.Attempt, error)
}

func (p *sealTransport) status(req x.Request) x.Status {
	budget := p.budget
	if req.Source != nil {
		budget.SourceSealed = true
		budget.Size = req.Source.Size
	}
	return x.Status{State: x.StatusLive, Phase: x.PhaseSending, Identity: req.Identity, Budget: budget}
}
func (p *sealTransport) Request(req x.Request, _ x.Deadline) (x.Response, x.Attempt, error) {
	p.requests = append(p.requests, req)
	if p.reply != nil {
		return p.reply(req)
	}
	status := p.status(req)
	return x.Response{Status: &status}, x.Attempt{Sent: true}, nil
}
func (*sealTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New("sender must use its supplied writer")
}
func (*sealTransport) CloseData(x.Identity, x.Deadline) error {
	return errors.New("data attachment belongs to the transport owner")
}

func exerciseSourceMutationStorage(t *testing.T) {
	t.Helper()
	jsServer, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	jsServer.Start()
	t.Cleanup(func() { jsServer.Shutdown(); jsServer.WaitForShutdown() })
	if !jsServer.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded JetStream failed to start")
	}
	nc, err := nats.Connect(jsServer.ClientURL(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	f := newSenderFixture(t, bytes.Repeat([]byte("real source"), 20000))
	spec := f.writer.spec
	spec.Bucket, spec.Stream = "xfer_source", "OBJ_xfer_source"
	if _, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: spec.Bucket, Storage: jetstream.FileStorage}); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	streamInfo, err := stream.Info(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	spec.StreamCreated = streamInfo.Created.Format(time.RFC3339Nano)
	// The source seal and object receipts use the landed durable leaves.
	writer, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatalf("NOT_RUN pending P02: %v", err)
	}
	ledger, err := xferledger.New(x.LedgerConfig{Base: f.config.Base, Root: filepath.Join(t.TempDir(), "primary"), FallbackRoot: filepath.Join(t.TempDir(), "outbox")})
	if err != nil {
		t.Fatalf("NOT_RUN pending P13: %v", err)
	}
	f.config.Writer = writer
	origin := x.Origin{BrokerID: "origin", InstanceID: "broker", BootID: "test-boot", Generation: 1}
	row := x.LedgerRow{Identity: f.config.Identity, Origin: origin, EntryNonce: f.config.Identity.EntryNonce, Revision: 1, Phase: x.PhaseOpen,
		Intent: f.config.Intent, StartedAt: f.config.Budget.StartedAt.Round(0), BudgetSeal: f.config.Budget, StartDelivery: x.DeliveryPending}
	key := x.Key{SID: row.Identity.SID, TransferID: row.Identity.TransferID}
	row.BudgetSeal.StartedAt = row.StartedAt
	row.Permit = x.CreationPermit{Identity: row.Identity, Origin: origin, IssuedAt: row.StartedAt, CreateUntil: row.StartedAt.Add(time.Minute), PermitNonce: "permit"}
	data, err := json.Marshal(schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "start", Verb: row.Intent.Verb, Tier: row.Intent.Tier, Ts: row.StartedAt, Session: row.Identity.SID, Node: row.Identity.NID, ActorFp: row.Identity.ActorFP, TransferID: row.Identity.TransferID})
	if err != nil {
		t.Fatal(err)
	}
	row.CanonicalStart = &x.CanonicalRecord{Identity: row.Identity, Kind: "start", Bytes: data, Digest: digest(data), ReqID: digest(append([]byte("xferaudit-record:"), data...))}
	row, err = ledger.Update(key, 0, x.LedgerMutation{Kind: x.MutationAdmit, EntryNonce: row.EntryNonce, Row: row}, f.deadline())
	if err != nil {
		t.Fatal(err)
	}
	f.transport.reply = func(req x.Request) (x.Response, x.Attempt, error) {
		updated := row
		updated.Revision++
		updated.SourceSeal = req.Source
		updated.BudgetSeal.Size, updated.BudgetSeal.SourceSealed = req.Source.Size, true
		updated, err := ledger.Update(key, row.Revision, x.LedgerMutation{Kind: x.MutationSourceSeal, EntryNonce: row.EntryNonce, Row: updated}, f.deadline())
		if err != nil {
			return x.Response{}, x.Attempt{Sent: true}, err
		}
		row = updated
		return x.Response{Status: &x.Status{State: x.StatusLive, Identity: row.Identity, Budget: row.BudgetSeal}}, x.Attempt{Sent: true}, nil
	}
	f.recreate(t)
	seal := requireSeal(t, f)
	info, err := f.file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	rewriteSource(t, f, 0, []byte("mutation"), info.ModTime())
	var sendErr error
	for n := uint32(0); n <= seal.Chunks && sendErr == nil; n++ {
		sendErr = f.actor.Step(f.deadline())
	}
	if sendErr == nil || !strings.Contains(sendErr.Error(), x.CodeSourceChanged) {
		t.Fatalf("real writer accepted source mutation: %v", sendErr)
	}
	// No metadata exists on the raw SDK-compatible object metadata subject.
	streamInfo, err = stream.Info(t.Context(), jetstream.WithSubjectFilter("$O."+spec.Bucket+".M.>"))
	if err != nil || len(streamInfo.State.Subjects) != 0 {
		t.Fatalf("mutated source published real metadata: err=%v info=%+v", err, streamInfo)
	}
	requireStop(t, f)
}

func (w *receiptWriter) Snapshot(d x.Deadline) (x.WriterState, error) { return w.Freeze(d) }
