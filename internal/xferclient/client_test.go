package xferclient

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// These adversarial ports test engine orchestration, not P02/P03/P05/P16's
// production implementations. Embedded JS independently detects an SDK reput's
// new physical NUID; the original file ports record actual IO and rename calls.
type clientFixture struct {
	t                *testing.T
	client           *Client
	transport        *clientTransport
	writer           *clientWriter
	reader           *clientReader
	atomic           *clientAtomic
	capacity         *clientCapacity
	js               jetstream.JetStream
	store            jetstream.ObjectStore
	server           *natsserver.Server
	nc               *nats.Conn
	data             []byte
	version          string
	writers, readers int
	closeOnce        sync.Once
}

func newClientFixture(t *testing.T, verb string, configure ...func(*Config)) *clientFixture {
	t.Helper()
	f := &clientFixture{t: t, data: bytes.Repeat([]byte("0123456789abcdef"), 4500)}
	server, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	f.server = server
	go server.Start()
	if !server.ReadyForConnections(3 * time.Second) {
		t.Fatal("embedded JetStream did not start")
	}
	t.Cleanup(f.close)
	nc, err := nats.Connect(server.ClientURL(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	f.nc = nc
	f.js, err = jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	// ctx-root: hermetic fixture setup owns its bounded embedded-JS operations.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	f.store, err = f.js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: proto.XferBucketName("session")})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := f.js.Stream(ctx, "OBJ_"+proto.XferBucketName("session"))
	if err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatal(err)
	}
	config := info.Config
	config.Duplicates = proto.XferTierBMaxBudget + 120*time.Second
	if _, err := f.js.UpdateStream(ctx, config); err != nil {
		t.Fatal(err)
	}
	object, err := f.store.Put(ctx, jetstream.ObjectMeta{Name: "invocation", Opts: &jetstream.ObjectMetaOptions{ChunkSize: 32 * 1024}}, bytes.NewReader(f.data))
	if err != nil {
		t.Fatal(err)
	}
	f.version = object.NUID
	f.capacity = &clientCapacity{held: make(map[x.ReservationID]bool)}
	f.writer = &clientWriter{fixture: f}
	f.reader = &clientReader{fixture: f}
	f.transport = &clientTransport{fixture: f, origin: x.Origin{BrokerID: "broker", InstanceID: "original-process", BootID: "original-boot", Generation: 1},
		started: time.Now().UTC(), counts: make(map[x.Operation]int), calls: make(map[x.Operation]string)}
	intent := x.Intent{Verb: verb, Tier: "b", Path: "/original/destination", ChunkSize: 32 * 1024}
	cfg := Config{Base: x.Base{Clock: func() (string, int64, error) { now, err := controlbudget.Now(); return "client-boot", now, err }, UTC: time.Now, Capacity: f.capacity},
		Identity: x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent-instance", TransferID: "invocation"}, Intent: intent, Transport: f.transport}
	if verb == "push" {
		path := filepath.Join(t.TempDir(), "pinned-source")
		if err := os.WriteFile(path, f.data, 0o600); err != nil {
			t.Fatal(err)
		}
		cfg.Source, err = os.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		cfg.Intent.Size, cfg.Intent.SHA256 = int64(len(f.data)), digestOf(f.data)
		cfg.MakeWriter = func(_ jetstream.JetStream, spec x.ObjectSpec) (x.ObjectWriter, error) {
			f.writers++
			f.writer.spec = spec
			return f.writer, nil
		}
	} else {
		parent := t.TempDir()
		file, err := os.CreateTemp(parent, ".original-tmp-")
		if err != nil {
			t.Fatal(err)
		}
		parentFD, err := os.Open(parent)
		if err != nil {
			_ = file.Close()
			t.Fatal(err)
		}
		f.atomic = &clientAtomic{fixture: f, file: file, parent: parentFD, target: filepath.Join(parent, "destination")}
		if err := os.WriteFile(f.atomic.target, []byte("old complete contents"), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg.Destination = f.atomic
		cfg.MakeReader = func(_ jetstream.JetStream, spec x.ObjectSpec, checkpoint x.SinkCheckpoint) (x.ObjectReader, error) {
			f.readers++
			f.reader.spec, f.reader.checkpoint = spec, checkpoint
			return f.reader, nil
		}
	}
	for _, apply := range configure {
		apply(&cfg)
	}
	invocation, err := NewClient(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.client = invocation.(*Client)
	f.transport.identity = f.client.identity
	f.transport.identity.EntryNonce, f.transport.identity.ObjectVersion = "entry-nonce", f.version
	return f
}

func testDeadline(t *testing.T, duration time.Duration) x.Deadline {
	t.Helper()
	deadline, err := controlbudget.After(duration)
	if err != nil {
		t.Fatal(err)
	}
	return deadline
}

func digestOf(data []byte) string {
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:])
}

func (f *clientFixture) close() {
	f.closeOnce.Do(func() {
		if f.client != nil {
			if err := f.client.Stop(testDeadline(f.t, 3*time.Second)); err != nil {
				f.t.Error(err)
			}
		}
		if f.nc != nil {
			f.nc.Close()
		}
		if f.server != nil {
			f.server.Shutdown()
			f.server.WaitForShutdown()
		}
	})
}

type clientCapacity struct {
	held        map[x.ReservationID]bool
	released    int
	failRelease bool
}

func (c *clientCapacity) Reserve(reservation x.Reservation) (x.ReservationID, error) {
	if reservation.NonReplayBytes <= 0 {
		return "", errors.New("empty buffer reservation")
	}
	id := x.ReservationID(fmt.Sprintf("buffer-%d", len(c.held)+1))
	c.held[id] = true
	return id, nil
}
func (c *clientCapacity) Release(id x.ReservationID) error {
	if c.failRelease {
		return errors.New("capacity release unavailable")
	}
	if !c.held[id] {
		return errors.New("unknown reservation")
	}
	delete(c.held, id)
	c.released++
	return nil
}

type clientTransport struct {
	fixture                *clientFixture
	identity               x.Identity
	origin                 x.Origin
	started                time.Time
	counts                 map[x.Operation]int
	calls                  map[x.Operation]string
	trace                  []x.Operation
	lost                   x.Operation
	statusMode             string
	finished, sealed       bool
	finalizedOutcome       *x.ReceiverOutcome
	dataOpened, dataClosed int
	onRequest              func(x.Request) (x.Response, x.Attempt, error, bool)
}

func (p *clientTransport) status() x.Status {
	budget, _ := proto.TransferBudget(p.fixture.client.cfg.Intent.Tier, int64(len(p.fixture.data)), p.fixture.client.cfg.Intent.RateLimitBPS)
	phase := x.PhasePrepared
	if p.sealed {
		phase = x.PhaseReceiving
	}
	return x.Status{State: x.StatusLive, Phase: phase, Identity: p.identity, Origin: p.origin,
		Budget: x.BudgetSeal{StartedAt: p.started, Size: int64(len(p.fixture.data)), RateLimitBPS: p.fixture.client.cfg.Intent.RateLimitBPS,
			EffectiveBudgetNS: int64(budget), RemainingNS: int64(budget - time.Since(p.started)), SourceSealed: p.sealed || p.fixture.client.cfg.Intent.Verb == "pull"}}
}

func (p *clientTransport) terminal() x.TerminalReceipt {
	return x.TerminalReceipt{TerminalID: terminalSlot(p.identity), Kind: "complete", CanonicalDigest: digestOf([]byte("canonical terminal")), ReqID: "original-request",
		HistoryStream: "history-session", HistorySeq: 42, Bytes: int64(len(p.fixture.data)), SHA256: digestOf(p.fixture.data)}
}

func (p *clientTransport) Request(request x.Request, deadline x.Deadline) (x.Response, x.Attempt, error) {
	p.counts[request.Op]++
	p.trace = append(p.trace, request.Op)
	if request.Identity.TransferID != "invocation" {
		p.fixture.t.Errorf("a retry minted a business ID: %s", request.Identity.TransferID)
	}
	if previous := p.calls[request.Op]; previous != "" && previous != request.CallID {
		p.fixture.t.Errorf("%s replay changed CallID", request.Op)
	}
	p.calls[request.Op] = request.CallID
	if _, err := controlbudget.Remaining(deadline); err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	if p.onRequest != nil {
		response, attempt, err, handled := p.onRequest(request)
		if handled {
			return response, attempt, err
		}
	}
	status := p.status()
	response := x.Response{}
	switch request.Op {
	case x.OpProbe:
		response.Probe = &x.ProbeResult{Version: x.Version, HistoryGuardVersion: x.Version, BucketConfigVersion: x.Version,
			InstanceID: "agent-instance", MaxPayload: 1024 * 1024, MaxMsgSize: 1024 * 1024}
	case x.OpPermit:
		response.Permit = &x.CreationPermit{Identity: p.identity, Origin: p.origin, IssuedAt: p.started, CreateUntil: p.started.Add(time.Minute), PermitNonce: "permit", Signature: "unit-port-signature"}
	case x.OpPrepare, x.OpSource:
		if request.Op == x.OpSource {
			p.sealed = true
			status = p.status()
		}
		response.Status = &status
		if request.Op == x.OpPrepare && p.fixture.client.cfg.Intent.Verb == "pull" {
			response.Source = &x.SourceSeal{Size: int64(len(p.fixture.data)), SHA256: digestOf(p.fixture.data), ChunkSize: 32 * 1024, Chunks: 3, RateLimitBPS: p.fixture.client.cfg.Intent.RateLimitBPS, SourceFingerprint: "source-stat"}
			if p.fixture.client.cfg.Intent.Tier == "a" {
				response.Source.ChunkSize = 0
				response.Source.Chunks = 0
				response.Inline = append([]byte{}, p.fixture.data...)
			}
		}
	case x.OpCommit:
		p.finished = true
		response.Status = &status
	case x.OpReserve:
		response.Grant = &x.CommitGrant{Identity: p.identity, GrantNonce: "original-grant", TerminalID: terminalSlot(p.identity), ReceiverInstance: request.Reserve.ReceiverInstance}
	case x.OpTerminal:
		if request.Outcome == nil {
			p.fixture.t.Error("finalize omitted the original outcome")
		} else {
			if p.finalizedOutcome != nil && *p.finalizedOutcome != *request.Outcome {
				p.fixture.t.Error("finalize changed the original primitive outcome")
			}
			copy := *request.Outcome
			p.finalizedOutcome = &copy
		}
		p.finished = true
		receipt := p.terminal()
		response.Receipt = &receipt
	case x.OpCancel:
		response.Status = &status
	case x.OpStatus:
		switch p.statusMode {
		case "missing":
			return x.Response{}, x.Attempt{Sent: true}, nil
		case "unavailable":
			return x.Response{}, x.Attempt{Sent: true}, errors.New("original owner did not reply")
		case "recovering":
			return x.Response{Code: x.CodeOwnerRecovering, Error: "original census not ready"}, x.Attempt{Sent: true}, nil
		case "unknown":
			status.State, status.Phase = x.StatusUnknown, ""
		case "live":
		default:
			if p.finished {
				receipt := p.terminal()
				status.State, status.Phase, status.Terminal = x.StatusTerminal, x.PhaseHistoryConfirmed, &receipt
			}
		}
		response.Status = &status
	default:
		return x.Response{}, x.Attempt{}, errors.New("unexpected engine operation")
	}
	if request.Op == p.lost && p.counts[request.Op] == 1 {
		return x.Response{}, x.Attempt{Sent: true}, errors.New("accepted but reply lost")
	}
	return response, x.Attempt{Sent: true, LogicalID: request.CallID}, nil
}

func (p *clientTransport) OpenData(identity x.Identity, _ x.Deadline) (jetstream.JetStream, error) {
	if identity != p.identity {
		return nil, errors.New("data attachment lost the original identity")
	}
	p.dataOpened++
	return p.fixture.js, nil
}
func (p *clientTransport) CloseData(identity x.Identity, _ x.Deadline) error {
	if identity != p.identity {
		return errors.New("data close has the wrong identity")
	}
	p.dataClosed++
	return nil
}

type clientWriter struct {
	fixture                             *clientFixture
	spec                                x.ObjectSpec
	appends, finalizes, stops, resolves int
	failStop                            bool
	loseChunk                           bool
	pending                             *x.ChunkReceipt
	corruptReceipt                      bool
	ioTimes                             []int64
}

func (w *clientWriter) Append(index uint32, data []byte, _ x.Deadline) (x.ChunkReceipt, error) {
	w.appends++
	now, _ := controlbudget.Now()
	w.ioTimes = append(w.ioTimes, now)
	r := x.ChunkReceipt{Index: index, Offset: int64(index) * int64(w.spec.ChunkSize), Length: uint32(len(data)), StreamSeq: uint64(index) + 10, SHA256: sha256.Sum256(data)}
	if w.corruptReceipt {
		r.SHA256[0] ^= 1
	}
	if w.loseChunk && index == 0 {
		w.loseChunk = false
		w.pending = &r
		return x.ChunkReceipt{}, errors.New("chunk ACK lost")
	}
	return r, nil
}
func (w *clientWriter) ResolvePending(_ x.Deadline) (x.ChunkReceipt, bool, error) {
	w.resolves++
	if w.pending != nil {
		r := *w.pending
		w.pending = nil
		return r, true, nil
	}
	return x.ChunkReceipt{}, false, nil
}
func (w *clientWriter) Finalize(_ x.Deadline) (x.ObjectReceipt, error) {
	w.finalizes++
	return x.ObjectReceipt{Spec: w.spec, MetadataSeq: 100, MetadataDigest: digestOf([]byte("metadata"))}, nil
}
func (w *clientWriter) Freeze(_ x.Deadline) (x.WriterState, error) {
	return x.WriterState{ObjectSpec: w.spec, AttachmentGeneration: 1, RetirementFence: x.RetirementFence{WriterRetired: w.stops > 0 && !w.failStop}}, nil
}
func (w *clientWriter) Stop(_ x.Deadline) error {
	w.stops++
	if w.failStop {
		return errors.New("writer retirement pending")
	}
	return nil
}

// origin: hot-update S7 plan §7 / T33
func TestTransferClientNeverReputsAfterCommitUnknown(t *testing.T) {
	for _, verb := range []string{"push", "pull"} {
		for _, mode := range []string{"confirmed", "missing", "unavailable", "recovering", "unknown", "retry", "cancel"} {
			t.Run(verb+"/"+mode, func(t *testing.T) {
				f := newClientFixture(t, verb)
				originalPhysical := clientPhysicalChunks(t, f)
				f.transport.lost = x.OpCommit
				if verb == "pull" {
					f.transport.lost = x.OpTerminal
				}
				if mode != "confirmed" {
					f.transport.statusMode = mode
				}
				if mode == "retry" || mode == "cancel" {
					f.transport.statusMode = "missing"
				}
				receipt, err := f.client.Run(testDeadline(t, 1200*time.Millisecond))
				if mode == "confirmed" {
					if err != nil || receipt.Kind != "complete" {
						t.Fatalf("lost reply failed reconciliation: receipt=%+v err=%v", receipt, err)
					}
				} else {
					if err == nil || ExitCode(err) != 74 || receipt.Kind != "" {
						t.Fatalf("unconfirmed result claimed success: receipt=%+v err=%v exit=%d", receipt, err, ExitCode(err))
					}
					if mode == "retry" {
						f.transport.statusMode = "live"
						if verb == "push" {
							f.transport.statusMode = ""
						}
						receipt, err = f.client.Run(testDeadline(t, time.Second))
						if err != nil || receipt.Kind != "complete" {
							t.Fatalf("same invocation did not resume settlement: %v", err)
						}
					}
					if mode == "cancel" {
						f.transport.statusMode = "live"
						status, cancelErr := f.client.Cancel(x.CancelUser, testDeadline(t, time.Second))
						if cancelErr != nil {
							t.Fatal(cancelErr)
						}
						if verb == "pull" && (status.State != x.StatusTerminal || status.Terminal.Kind != "complete") {
							t.Fatal("cancellation contradicted the landed primitive")
						}
						if verb == "push" && status.Terminal != nil {
							t.Fatal("cancellation invented a terminal")
						}
					}
				}
				if f.transport.counts[x.OpPermit] != 1 || f.transport.counts[x.OpPrepare] != 1 {
					t.Fatal("reply loss created another permit or worker")
				}
				if f.transport.counts[x.OpStatus] == 0 {
					t.Fatal("sent-unknown was not reconciled")
				}
				previous := -1
				for index, op := range f.transport.trace {
					if op != f.transport.lost {
						continue
					}
					if previous >= 0 {
						queried := false
						for _, between := range f.transport.trace[previous+1 : index] {
							queried = queried || between == x.OpStatus
						}
						if !queried {
							t.Fatal("sent mutation replay preceded its status query")
						}
					}
					previous = index
				}
				if verb == "push" && (f.writers != 1 || f.writer.appends != 3 || f.writer.finalizes != 1) {
					t.Fatalf("commit uncertainty retransferred the object: factories=%d appends=%d finalize=%d", f.writers, f.writer.appends, f.writer.finalizes)
				}
				if verb == "pull" && (f.readers != 1 || f.atomic.commits != 1 || f.reader.reads != 3) {
					t.Fatalf("landed pull reentered IO: readers=%d commits=%d chunks=%d", f.readers, f.atomic.commits, f.reader.reads)
				}
				assertClientPhysicalChunks(t, f, originalPhysical)
				// ctx-root: independent raw metadata oracle belongs to this fixture.
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				info, infoErr := f.store.GetInfo(ctx, "invocation")
				if infoErr != nil || info.NUID != f.version {
					t.Fatalf("capable path used SDK Put/new NUID: info=%+v err=%v", info, infoErr)
				}
				if verb == "pull" {
					body, err := os.ReadFile(f.atomic.target)
					if err != nil || !bytes.Equal(body, f.data) {
						t.Fatal("original local primitive did not land complete contents")
					}
				}
			})
		}
	}
}

// origin: hot-update S7 plan §1.2 / §7 / lead LQ2 / T33
func TestTransferClientAdmissionRejectsBeforeSideEffects(t *testing.T) {
	f := newClientFixture(t, "push")
	for _, change := range []func(*Config){
		func(c *Config) { c.Intent.Tier = "a" },
		func(c *Config) { c.Identity.TransferID = "" },
		func(c *Config) { c.Intent.Size = proto.XferMaxBytes; c.Intent.RateLimitBPS = 1 },
		func(c *Config) { c.Intent.RateLimitBPS = -1 },
		func(c *Config) { c.Intent.ChunkSize = 1 },
		func(c *Config) { c.Identity.IntentDigest = "different intent" },
		func(c *Config) { c.MakeWriter = nil },
	} {
		cfg := f.client.cfg
		cfg.Identity.IntentDigest = ""
		change(&cfg)
		if invocation, err := NewClient(cfg); err == nil || invocation != nil {
			t.Fatal("invalid config admitted a capable invocation")
		}
	}
	if len(f.transport.trace) != 0 || f.transport.dataOpened != 0 {
		t.Fatal("rejected admission had a side effect")
	}
}

// origin: hot-update S7 plan §2.2 / §7 / T33
func TestTransferClientSourceAndChunkReceiptsRemainBound(t *testing.T) {
	for _, defect := range []string{"source", "receipt", "lost_ack"} {
		t.Run(defect, func(t *testing.T) {
			f := newClientFixture(t, "push")
			if defect == "source" {
				if err := os.WriteFile(f.client.cfg.Source.Name(), bytes.Repeat([]byte("x"), len(f.data)), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if defect == "receipt" {
				f.writer.corruptReceipt = true
			}
			if defect == "lost_ack" {
				f.writer.loseChunk = true
			}
			receipt, err := f.client.Run(testDeadline(t, 2*time.Second))
			if defect == "lost_ack" {
				if err != nil || receipt.Kind != "complete" || f.writer.resolves != 1 || f.writer.appends != 3 {
					t.Fatalf("chunk resolution resent bytes: %v", err)
				}
			} else if err == nil || f.writer.finalizes != 0 || f.transport.counts[x.OpCommit] != 0 {
				t.Fatal("changed source or receipt reached metadata/commit")
			}
		})
	}
}

type statusTransport struct {
	response x.Response
	err      error
	calls    int
}

func (p *statusTransport) Request(request x.Request, _ x.Deadline) (x.Response, x.Attempt, error) {
	p.calls++
	if request.Op != x.OpStatus {
		return x.Response{}, x.Attempt{}, errors.New("read-only query attempted a mutation")
	}
	return p.response, x.Attempt{}, p.err
}
func (*statusTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New("read-only query opened data")
}
func (*statusTransport) CloseData(x.Identity, x.Deadline) error {
	return errors.New("read-only query closed data")
}

// origin: hot-update S7 plan §6.1 / §7 / T33
func TestTransferClientStatusRequiresExactOriginalEvidence(t *testing.T) {
	identity := x.Identity{SID: "session", NID: "node", TransferID: "original", EntryNonce: "nonce", ObjectVersion: "version"}
	origin := x.Origin{BrokerID: "broker", InstanceID: "process", BootID: "boot"}
	receipt := x.TerminalReceipt{Kind: "complete", TerminalID: terminalSlot(identity), CanonicalDigest: digestOf([]byte("canonical")), ReqID: "request", HistoryStream: "history", HistorySeq: 10, SHA256: digestOf(nil)}
	status := x.Status{State: x.StatusTerminal, Phase: x.PhaseHistoryConfirmed, Identity: identity, Origin: origin, Terminal: &receipt}
	for _, name := range []string{"complete", "unknown", "inline_live", "empty", "nonowner", "nonce", "version", "missing_proof", "opposite", "abandoned", "unavailable"} {
		t.Run(name, func(t *testing.T) {
			copy, terminal := status, receipt
			queryIdentity := identity
			copy.Terminal = &terminal
			transport := &statusTransport{response: x.Response{Status: &copy}}
			switch name {
			case "complete":
			case "unknown":
				copy.State, copy.Phase, copy.Terminal = x.StatusUnknown, "", nil
			case "inline_live":
				copy.State, copy.Phase, copy.Terminal, copy.Identity.ObjectVersion = x.StatusLive, x.PhasePrepared, nil, ""
				queryIdentity.ObjectVersion = ""
			case "empty":
				copy.State = ""
			case "nonowner":
				copy.Origin.InstanceID = "another owner"
			case "nonce":
				copy.Identity.EntryNonce = "replacement"
			case "version":
				copy.Identity.ObjectVersion = "foreign"
			case "missing_proof":
				terminal.HistorySeq = 0
			case "opposite":
				terminal.OutcomeUnknown = true
			case "abandoned":
				terminal.Kind, terminal.Code, terminal.OutcomeUnknown = "failed", x.ReasonSettlementAbandoned, true
			case "unavailable":
				transport.response.Status = nil
				transport.err = errors.New("unavailable")
			}
			result, err := QueryStatus(transport, x.StatusRequest{CallID: "read-only", Identity: queryIdentity, Origin: origin}, testDeadline(t, time.Second))
			if name == "complete" || name == "unknown" || name == "inline_live" || name == "abandoned" {
				if err != nil || result.State != copy.State {
					t.Fatalf("authoritative read failed: %v", err)
				}
			} else if err == nil {
				t.Fatalf("%s was mistaken for authoritative status", name)
			}
			if transport.calls != 1 {
				t.Fatal("read-only status retried or created responsibility")
			}
		})
	}
}

// origin: hot-update S7 plan §7 / lead LQ1–LQ3 / T33
func TestTransferClientErrorsKeepExitAndReceiptSemantics(t *testing.T) {
	for _, test := range []struct {
		code string
		exit int
	}{
		{x.CodeIntentConflict, 64}, {x.CodePermitExpired, 64}, {x.CodeObjectConflict, 64}, {x.CodeObjectVersionMismatch, 64}, {x.CodeSourceChanged, 64}, {x.CodePayloadUnsupported, 64},
		{x.CodeNotMember, 77}, {x.CodeNotOwner, 77},
		{"transfer_id_in_flight", 75}, {"transfer_disabled", 64},
		{x.CodeStorageHorizon, 75}, {x.CodeCapacityExceeded, 75}, {x.CodeOwnerRecovering, 75}, {x.CodeSessionDeleting, 75},
		{x.CodeBudgetExceeded, 75}, {x.CodeCanceled, 64}, {x.CodeUnknown, 64},
		{x.CodeResultUnknown, 74}, {x.CodeTerminalConflict, 74}, {x.CodeCommitInflight, 74}, {x.CodeDurabilityUnknown, 74}, {x.CodeReconcileRequired, 74},
	} {
		err := clientError(test.code, x.Identity{TransferID: "original"}, x.PhasePrepared, false, errors.New("unclassified peer prose"))
		if ExitCode(err) != test.exit || ErrorCode(err) != test.code {
			t.Fatalf("code %s classified as %d", test.code, ExitCode(err))
		}
		if test.exit == 64 {
			want := 64
			// A confirmed cancellation is determinate; an unresolved one
			// still cannot claim that the receiver closed its commit gate.
			// origin: S7 internal review rv-n1compat F6
			if test.code == x.CodeCanceled {
				want = 74
			}
			if got := ExitCode(clientError(test.code, x.Identity{}, x.PhasePrepared, true, nil)); got != want {
				t.Fatalf("refusal %s changed exit class after admission: got %d want %d", test.code, got, want)
			}
		}
	}
	if ExitCode(nil) != 0 || ExitCode(errors.New("complete")) != 74 || ErrorCode(errors.New("complete")) != "" {
		t.Fatal("peer prose became a success or a wire code")
	}
	if ExitCode(clientError(x.CodeCanceled, x.Identity{}, x.PhaseCommitReserved, true, nil)) != 74 {
		t.Fatal("cancel uncertainty was presented as a settled failure")
	}
}

// origin: hot-update S7 plan §6.1 / §7 / T33
func TestTransferClientRepliesCannotReplaceBindings(t *testing.T) {
	for _, name := range []string{"permit", "prepare", "source_budget", "source_start", "grant", "early_complete"} {
		t.Run(name, func(t *testing.T) {
			verb := "push"
			if name == "grant" || name == "early_complete" {
				verb = "pull"
			}
			f := newClientFixture(t, verb)
			f.transport.onRequest = func(request x.Request) (x.Response, x.Attempt, error, bool) {
				response := x.Response{}
				status := f.transport.status()
				switch {
				case name == "permit" && request.Op == x.OpPermit:
					identity := f.transport.identity
					identity.AgentInstance = "replacement instance"
					response.Permit = &x.CreationPermit{Identity: identity, Origin: f.transport.origin, PermitNonce: "permit", Signature: "signature"}
				case name == "prepare" && request.Op == x.OpPrepare:
					status.Identity.EntryNonce = "replacement entry"
					response.Status = &status
				case name == "source_budget" && request.Op == x.OpSource:
					status.Budget.SourceSealed = true
					status.Budget.RateLimitBPS++
					response.Status = &status
				case name == "source_start" && request.Op == x.OpSource:
					status.Budget.SourceSealed = true
					status.Budget.StartedAt = status.Budget.StartedAt.Add(time.Minute)
					response.Status = &status
				case name == "grant" && request.Op == x.OpReserve:
					response.Grant = &x.CommitGrant{Identity: f.transport.identity, GrantNonce: "grant", TerminalID: "terminal", ReceiverInstance: "foreign receiver"}
				case name == "early_complete" && request.Op == x.OpReserve:
					receipt := f.transport.terminal()
					response.Receipt = &receipt
				default:
					return x.Response{}, x.Attempt{}, nil, false
				}
				return response, x.Attempt{Sent: true}, nil, true
			}
			receipt, err := f.client.Run(testDeadline(t, time.Second))
			if err == nil || receipt.Kind != "" || f.transport.counts[x.OpCommit] != 0 || f.atomic != nil && f.atomic.commits != 0 {
				t.Fatalf("%s reply replaced original authority: receipt=%+v err=%v", name, receipt, err)
			}
			if f.client.pending == nil {
				t.Fatal("malformed sent reply discarded the pending mutation")
			}
		})
	}
}
