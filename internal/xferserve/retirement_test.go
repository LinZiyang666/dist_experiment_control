package xferserve

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/LinZiyang666/tether/internal/xfergc"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"path/filepath"
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Cleanup can reopen the retained checkpoint after an image cut, but cannot
// replace its original stream incarnation or forget an acknowledged fence.
// origin: hot-update S7 plan §2.3 / §8.2 / T21
func TestTransferRetirementJournalRetainsOriginalIncarnation(t *testing.T) {
	id := x.Identity{SID: "s", TransferID: "id", EntryNonce: "original", ObjectVersion: "original-version"}
	j := &RetirementJournal{Root: filepath.Join(t.TempDir(), "retirement")}
	spec := x.ObjectSpec{Identity: id, Bucket: "xfer-s", Stream: "OBJ_xfer-s", StreamCreated: "original-incarnation"}
	if err := j.Pin(id, func() (x.ObjectSpec, error) { return spec, nil }); err != nil {
		t.Fatal(err)
	}
	j = &RetirementJournal{Root: j.Root}
	if err := j.Pin(id, func() (x.ObjectSpec, error) { return x.ObjectSpec{}, errors.New("must not read a replacement stream") }); err != nil {
		t.Fatal(err)
	}
	state, err := j.Load(id)
	if err != nil || state.ObjectSpec != spec {
		t.Fatalf("lost original stream after reopen: %+v %v", state, err)
	}
	state.RetirementFence = x.RetirementFence{WriterRetired: true, ConsumersRetired: true, ExpectedStreamSeq: 20, PreviousMetadataSeq: 18,
		Receipt: &x.HistoryReceipt{Stream: spec.Stream, Sequence: 21, RecordDigest: "original-fence"}}
	if err := j.Checkpoint(state); err != nil {
		t.Fatal(err)
	}
	retained, err := (&RetirementJournal{Root: j.Root}).Load(id)
	if err != nil || retained.RetirementFence.PreviousMetadataSeq != 18 || retained.RetirementFence.Receipt == nil || *retained.RetirementFence.Receipt != *state.RetirementFence.Receipt {
		t.Fatal("lost durable retirement fence")
	}
	state.ObjectSpec.StreamCreated = "replacement-incarnation"
	if err := j.Checkpoint(state); err == nil {
		t.Fatal("cleanup adopted a namesake stream")
	}
	state = retained
	state.RetirementFence.Receipt = nil
	if err := j.Checkpoint(state); err == nil {
		t.Fatal("cleanup forgot the acknowledged fence")
	}
}

// The production graph uses a real dual-directory ledger, original tracker,
// signed permit and physical history. The directed agent boundary owns a real
// courier; no byte receiver or same-PID binary cut is claimed by this exercise.
// origin: hot-update S7 plan §5.3 / §8.2 / FIX-TERM request 4
func TestTransferRetirementCollectionUsesOriginalAuthority(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exerciseRetirementAuthority(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "retirement original authority", g)
	testharness.AssertNoFDLeak(t, "retirement original authority", fd, 0)
}

func exerciseRetirementAuthority(t *testing.T) {
	host, js := assemblyHost(t, t.TempDir())
	var current *xfercontrol.Runtime
	host.Tracker = func() x.TrackerManager {
		if current == nil {
			return nil
		}
		return current.Tracker
	}
	host.Ledger = func() x.LedgerPort {
		if current == nil {
			return nil
		}
		return current.Ledger
	}
	asm, err := Assemble(host, js, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := asm.Release(); err != nil {
			t.Error(err)
		}
	}()
	cfg := asm.Config
	cfg.Authority = xferauth.ControlAuthority{}
	current, err = AssembleRuntime(cfg, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := xfercontrol.StopRuntime(*current, assemblyDeadline(t)); err != nil {
			t.Error(err)
		}
	}()
	if err := current.Tracker.Bind(x.Bindings{JS: js, Ledger: current.Ledger, Authority: cfg.Authority, Transport: asm.Link, OwnerGeneration: asm.Config.Origin.Generation}, assemblyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	owners := asm.Owners
	empty := sha256.Sum256(nil)
	intent := x.Intent{Verb: "push", Tier: "b", Path: "destination", SHA256: hex.EncodeToString(empty[:]), ChunkSize: 32768}
	permit, err := owners.Permits.IssueFor(x.Identity{SID: "retirement", NID: "node", ActorFP: "actor", AgentInstance: "agent", TransferID: "transfer"}, intent, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	id, origin := permit.Identity, permit.Origin
	owners.UTC = func() time.Time { return permit.CreateUntil.Add(time.Second) }
	if _, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "xfer-retirement"}); err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_xfer-retirement")
	if err != nil {
		t.Fatal(err)
	}
	spec := x.ObjectSpec{Identity: id, Bucket: "xfer-retirement", Stream: "OBJ_xfer-retirement", StreamCreated: stream.CachedInfo().Created.UTC().Format(time.RFC3339Nano), SHA256: intent.SHA256, ChunkSize: intent.ChunkSize}
	if err := owners.Retirement.Pin(id, func() (x.ObjectSpec, error) { return spec, nil }); err != nil {
		t.Fatal(err)
	}
	dataConn, err := nats.Connect(js.Conn().ConnectedUrl(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	defer dataConn.Close()
	dataJS, err := jetstream.New(dataConn)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := jsstream.NewTransferWriter(dataJS, spec)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Stop(assemblyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	stopped, err := writer.Freeze(assemblyDeadline(t))
	if err != nil || !stopped.RetirementFence.WriterRetired {
		t.Fatalf("original writer did not join: %+v %v", stopped, err)
	}
	audit := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "failed", Verb: "push", Ts: time.Now().UTC(), Session: id.SID, Node: id.NID, ActorFp: id.ActorFP, TransferID: id.TransferID, Tier: "b", Code: x.CodeCanceled,
		AgentInstance: id.AgentInstance, EntryNonce: id.EntryNonce, IntentDigest: id.IntentDigest, ObjectVersion: id.ObjectVersion, TerminalID: schema.TransferTerminalID(id.SID, id.TransferID, id.EntryNonce)}
	body, err := json.Marshal(audit)
	if err != nil {
		t.Fatal(err)
	}
	sum, req := sha256.Sum256(body), sha256.Sum256(append([]byte("xferaudit-record:"), body...))
	canonical := x.CanonicalRecord{Identity: id, Kind: "failed", ReqID: hex.EncodeToString(req[:]), Digest: hex.EncodeToString(sum[:]), Bytes: body}
	if err := jsstream.EnsureHistoryStream(t.Context(), js, id.SID, 1); err != nil {
		t.Fatal(err)
	}
	receipt, err := (sessionHistory{js: js}).Confirm(canonical, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	outcome := x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled}
	row := x.LedgerRow{Identity: id, Origin: origin, Intent: intent, CanonicalTerminal: &canonical, ReceiverOutcome: &outcome, HistoryReceipts: []x.HistoryReceipt{receipt}}
	row.EntryNonce = id.EntryNonce
	row.Cleanup.WriterFence = stopped.RetirementFence
	objects := asm.Config.Objects.(Objects)
	objects.Rows = func(x.Identity, x.Deadline) (x.LedgerRow, error) { return row, nil }
	cleanup, err := objects.Retire(id, x.RetirementProof{Identity: id, EntryNonce: id.EntryNonce, OwnerGeneration: 1, ReceiverRetired: true}, assemblyDeadline(t))
	if err != nil || !cleanup.ObjectPurged {
		t.Fatalf("original object retirement failed: %+v %v", cleanup, err)
	}
	checkpoint, err := owners.Retirement.Load(id)
	if err != nil || checkpoint.Phase != x.WriteRetired || !checkpoint.RetirementFence.Purged {
		t.Fatalf("real object retirement did not checkpoint: %+v %v", checkpoint, err)
	}

	courier, err := xfercourier.New(x.CourierConfig{Base: cfg.Base, Identity: id, Outcome: outcome, Capsule: x.SettlementCapsule{Identity: id, Origin: origin}, Transport: asm.Link})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := courier.Stop(assemblyDeadline(t)); err != nil {
			t.Error(err)
		}
	}()
	if err := xfercourier.BindCanonical(courier, canonical); err != nil {
		t.Fatal(err)
	}
	var replyMu sync.Mutex
	fault := ""
	sub, err := js.Conn().Subscribe(proto.SubjCmdForwarded(id.SID, id.NID, proto.TransferLinkVerb), func(msg *nats.Msg) {
		var carrier proto.TransferCommitReq
		if json.Unmarshal(msg.Data, &carrier) != nil || carrier.Link == nil || carrier.Link.Op != proto.TransferLinkReceipt {
			return
		}
		control := carrier.Link
		result := proto.TransferLinkResult{Version: x.Version, InstanceID: id.AgentInstance}
		if control.Identity != proto.Identity(id) || control.Origin != proto.Origin(origin) || control.Receipt == nil || control.Decision == nil || control.Decision.Canonical.Digest != canonical.Digest {
			result.Code = x.CodeIntentConflict
		} else if err := courier.Acknowledge(x.TerminalReceipt(*control.Receipt)); err != nil {
			result.Code = x.CodeReconcileRequired
		} else if err := courier.Stop(assemblyDeadline(t)); err != nil {
			result.Code = x.CodeOwnerRecovering
		} else {
			state, err := courier.Freeze(assemblyDeadline(t))
			if err != nil || !state.Stopped || state.Receipt == nil {
				result.Code = x.CodeOwnerRecovering
			} else {
				terminal := proto.TerminalReceipt(*state.Receipt)
				result.Status = &proto.TransferStatusResp{Identity: proto.Identity(id), Origin: proto.Origin(origin), State: proto.StatusState(x.StatusTerminal), Phase: proto.Phase(x.PhaseSettled), Terminal: &terminal}
			}
		}
		replyMu.Lock()
		selected := fault
		replyMu.Unlock()
		if result.Status != nil {
			switch selected {
			case "instance":
				result.InstanceID = "replacement"
			case "identity":
				result.Status.Identity.EntryNonce = "replacement"
			case "origin":
				result.Status.Origin.Generation++
			case "phase":
				result.Status.Phase = proto.Phase(x.PhaseCleanupPending)
			case "pending":
				result.Status.CleanupPending = true
			case "terminal":
				result.Status.Terminal.HistorySeq++
			case "code":
				result.Status.Code = x.CodeResultUnknown
			}
		}
		payload, _ := json.Marshal(proto.TransferCommitResp{Link: &result})
		_ = msg.Respond(payload)
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err := js.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	for _, selected := range []string{"instance", "identity", "origin", "phase", "pending", "terminal", "code"} {
		replyMu.Lock()
		fault = selected
		replyMu.Unlock()
		if err := owners.CollectRetirementCheckpoints(assemblyDeadline(t)); err == nil {
			t.Fatalf("%s courier reply authorized checkpoint deletion", selected)
		}
		if _, err := owners.Retirement.Load(id); err != nil {
			t.Fatalf("%s lost checkpoint: %v", selected, err)
		}
	}
	replyMu.Lock()
	fault = ""
	replyMu.Unlock()
	// Every independent owner/persistence failure retains the real candidate.
	for _, reason := range []string{"no tracker", "prior roots", "unexpired permit", "foreign origin", "permit forgery", "result live", "primary retained", "outbox retained", "pull receiver", "missing decision", "changed history", "expired courier lookup"} {
		t.Run(reason, func(t *testing.T) {
			candidate := owners
			borrowed := false
			path := owners.Retirement.decisionPath(id)
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.WriteFile(path, before, 0600); err != nil {
					t.Error(err)
				}
			}()
			switch reason {
			case "no tracker":
				candidate.Tracker = func() x.TrackerManager { return nil }
			case "prior roots":
				candidate.PreviousRoots = []string{"unresolved"}
			case "unexpired permit":
				candidate.UTC = func() time.Time { return permit.IssuedAt }
			case "foreign origin":
				candidate.Origin.InstanceID = "foreign"
				candidate.Tracker = func() x.TrackerManager { borrowed = true; return current.Tracker }
			case "permit forgery":
				permitPath := filepath.Join(owners.PermitRoot, hex.EncodeToString(sha256Sum([]byte(id.SID+"\n"+id.TransferID)))+".json")
				original, err := os.ReadFile(permitPath)
				if err != nil {
					t.Fatal(err)
				}
				defer func() {
					if err := os.WriteFile(permitPath, original, 0600); err != nil {
						t.Error(err)
					}
				}()
				fake := permit
				fake.Signature = "forged"
				if err := writeDurable(permitPath, fake); err != nil {
					t.Fatal(err)
				}
			case "result live":
				candidate.Tracker = func() x.TrackerManager {
					return retirementStatusBoundary{TrackerManager: current.Tracker, state: x.StatusLive}
				}
			case "primary retained", "outbox retained":
				candidate.Ledger = func() x.LedgerPort { return retirementLedgerBoundary{LedgerPort: current.Ledger, id: id} }
			case "expired courier lookup":
				called := false
				candidate.CourierAbsent = func(_ x.Identity, _ x.Origin, d x.Deadline) (bool, error) {
					called = true
					for {
						if _, err := controlbudget.Remaining(d); err != nil {
							return true, nil
						}
						time.Sleep(time.Millisecond)
					}
				}
				d, err := controlbudget.After(100 * time.Millisecond)
				if err != nil {
					t.Fatal(err)
				}
				proof, err := candidate.retirementResponsibility(id, d)
				if !called || err == nil || proof.Authoritative {
					t.Fatalf("expired original lookup authorized collection: %+v %v called=%v", proof, err, called)
				}
				return
			case "pull receiver", "missing decision", "changed history":
				var saved retirementDecision
				if err := json.Unmarshal(before, &saved); err != nil {
					t.Fatal(err)
				}
				switch reason {
				case "pull receiver":
					saved.Verb = "pull"
				case "missing decision":
					if err := os.Remove(path); err != nil {
						t.Fatal(err)
					}
				default:
					saved.Decision.Canonical.Bytes = append(saved.Decision.Canonical.Bytes, ' ')
				}
				if reason != "missing decision" {
					if err := writeDurable(path, saved); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := candidate.CollectRetirementCheckpoints(assemblyDeadline(t)); err == nil {
				t.Fatalf("%s authorized checkpoint collection", reason)
			}
			if reason == "foreign origin" && borrowed {
				t.Fatal("foreign original borrowed a tracker before authenticating its origin")
			}
			if _, err := owners.Retirement.Load(id); err != nil {
				t.Fatalf("%s lost checkpoint: %v", reason, err)
			}
		})
	}
	// Production's installed collector, rather than an unowned one-shot helper,
	// supplies the managed pass and proves actual durable file removal.
	if err := current.Collector.Stop(assemblyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	current.Collector, err = xfergc.New(x.GCConfig{Base: cfg.Base, JS: js, Owners: owners, Finalizer: current.Finalizer})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := current.Collector.Pass(assemblyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := owners.Retirement.Load(id); !os.IsNotExist(err) {
		t.Fatalf("settled original checkpoint retained: %v", err)
	}
	if _, err := os.Stat(owners.Retirement.decisionPath(id)); !os.IsNotExist(err) {
		t.Fatalf("retired decision replay evidence retained: %v", err)
	}
}

func sha256Sum(body []byte) []byte { sum := sha256.Sum256(body); return sum[:] }

// Explicit read-boundary defects exercise the outer absence predicates; the
// successful path above always uses the installed real tracker and ledger.
type retirementStatusBoundary struct {
	x.TrackerManager
	state x.StatusState
}

func (b retirementStatusBoundary) Status(req x.StatusRequest, d x.Deadline) (x.Status, error) {
	s, err := b.TrackerManager.Status(req, d)
	s.State = b.state
	return s, err
}

type retirementLedgerBoundary struct {
	x.LedgerPort
	id x.Identity
}

func (b retirementLedgerBoundary) Scan(d x.Deadline) (x.LedgerCensus, error) {
	c, err := b.LedgerPort.Scan(d)
	c.Rows = append(c.Rows, x.LedgerRow{Identity: b.id})
	return c, err
}

// Scan and consume use the actual producer lock across a reopen/cut, honor the
// caller's earlier deadline, and compare content after a replacement write.
// origin: hot-update S7 plan §5.3 / §8.2 / FIX-TERM request 4
func TestTransferRetirementCollectionSharesProducerMutex(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRetirementProducerMutex(t)
	}
	testharness.AssertNoGoroutineLeak(t, "retirement producer mutex", g)
	testharness.AssertNoFDLeak(t, "retirement producer mutex", fd, 0)
}
func exerciseRetirementProducerMutex(t *testing.T) {
	id := x.Identity{SID: "session", TransferID: "transfer", EntryNonce: "nonce", ObjectVersion: "version"}
	j := &RetirementJournal{Root: filepath.Join(t.TempDir(), "retirement")}
	spec := x.ObjectSpec{Identity: id, Stream: "OBJ_xfer-session", StreamCreated: "original"}
	if err := j.Pin(id, func() (x.ObjectSpec, error) { return spec, nil }); err != nil {
		t.Fatal(err)
	}
	state, err := j.Load(id)
	if err != nil {
		t.Fatal(err)
	}
	state.Phase = x.WriteRetired
	state.RetirementFence = x.RetirementFence{Purged: true, WriterRetired: true, ConsumersRetired: true, Receipt: &x.HistoryReceipt{Sequence: 3, RecordDigest: strings.Repeat("a", 64)}}
	if err := j.Checkpoint(state); err != nil {
		t.Fatal(err)
	}
	held := func(consume *xfergc.RetirementCheckpoint) {
		j.mu.Lock()
		defer j.mu.Unlock()
		d, err := controlbudget.After(10 * time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if consume == nil {
			_, err = j.ScanRetirementCheckpoints(64, d)
		} else {
			err = j.ConsumeRetirementCheckpoint(*consume, d)
		}
		if !errors.Is(err, controlbudget.ErrExpired) {
			t.Fatalf("collection bypassed original producer lock: %v", err)
		}
	}
	held(nil)
	candidates, err := j.ScanRetirementCheckpoints(64, assemblyDeadline(t))
	if err != nil || len(candidates) != 1 {
		t.Fatalf("scan %v %v", candidates, err)
	}
	held(&candidates[0])
	state.AttachmentGeneration++
	if err := j.Checkpoint(state); err != nil {
		t.Fatal(err)
	}
	if err := j.ConsumeRetirementCheckpoint(candidates[0], assemblyDeadline(t)); err == nil {
		t.Fatal("changed producer checkpoint was collected")
	}
	reopened := &RetirementJournal{Root: j.Root}
	candidates, err = reopened.ScanRetirementCheckpoints(64, assemblyDeadline(t))
	if err != nil || len(candidates) != 1 {
		t.Fatal(err)
	}
	if err := reopened.ConsumeRetirementCheckpoint(candidates[0], assemblyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Load(id); !os.IsNotExist(err) {
		t.Fatal("current content was not collected", err)
	}
}

// Relabeling public replay metadata cannot turn the original pull sending
// agent into proof that the original ctl receiver's courier stopped.
// origin: hot-update S7 plan §4 / §5.3 / FIX-TERM request 4
func TestTransferRetirementCourierRequiresPhysicalReceiverRole(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run("round", func(t *testing.T) { exercisePhysicalReceiverRole(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "physical courier receiver role", g)
	testharness.AssertNoFDLeak(t, "physical courier receiver role", fd, 0)
}
func exercisePhysicalReceiverRole(t *testing.T) {
	_, js := assemblyHost(t, t.TempDir())
	id := x.Identity{SID: "pullrole", NID: "node", ActorFP: "actor", TransferID: "transfer", AgentInstance: "agent", EntryNonce: "entry", IntentDigest: strings.Repeat("a", 64), ObjectVersion: "version"}
	origin := x.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	audit := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "failed", Verb: "pull", Ts: time.Now().UTC(), Session: id.SID, Node: id.NID, ActorFp: id.ActorFP, TransferID: id.TransferID, Tier: "b", Code: x.CodeCanceled, AgentInstance: id.AgentInstance, EntryNonce: id.EntryNonce, IntentDigest: id.IntentDigest, ObjectVersion: id.ObjectVersion, TerminalID: schema.TransferTerminalID(id.SID, id.TransferID, id.EntryNonce)}
	body, err := json.Marshal(audit)
	if err != nil {
		t.Fatal(err)
	}
	sum, req := sha256.Sum256(body), sha256.Sum256(append([]byte("xferaudit-record:"), body...))
	record := x.CanonicalRecord{Identity: id, Kind: "failed", Bytes: body, Digest: hex.EncodeToString(sum[:]), ReqID: hex.EncodeToString(req[:])}
	if err := jsstream.EnsureHistoryStream(t.Context(), js, id.SID, 1); err != nil {
		t.Fatal(err)
	}
	receipt, err := (sessionHistory{js: js}).Confirm(record, assemblyDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	outcome := x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled}
	journal := &RetirementJournal{Root: filepath.Join(t.TempDir(), "retirement")}
	if err := journal.retainDecision(x.LedgerRow{Identity: id, Origin: origin, Intent: x.Intent{Verb: "push"}, CanonicalTerminal: &record, ReceiverOutcome: &outcome, HistoryReceipts: []x.HistoryReceipt{receipt}}); err != nil {
		t.Fatal(err)
	}
	borrowed := false
	link := &Link{JS: js, Conn: func() *nats.Conn { borrowed = true; return nil }}
	absent, err := journal.courierAbsent(link, id, origin, assemblyDeadline(t))
	if err == nil || absent || borrowed {
		t.Fatalf("pull receiver role was borrowed from its sending agent: absent=%v borrowed=%v err=%v", absent, borrowed, err)
	}
}
