package xferagent

import (
	"bytes"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/nats-io/nats.go/jetstream"
)

// A Tier A push's receiver is created at the commit grant from the payload
// the worker entry retained at prepare: no object version, no data
// attachment, and the original atomic sink lands exactly those bytes once.
// Before FIX-INLINE the production factory refused Tier A, so every small
// capable push failed at its grant with owner_recovering.
// origin: hot-update S7 plan §3 / §8.1 / FIX-INLINE (T35 tier-a push cells)
func TestTransferInlinePushReceiverLandsRetainedPayload(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInlinePushReceiver(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseInlinePushReceiver(t *testing.T, round int) {
	t.Helper()
	data := bytes.Repeat([]byte(fmt.Sprintf("inline push %d\n", round)), 4000)
	target := filepath.Join(t.TempDir(), "destination")
	if err := os.WriteFile(target, []byte("complete original destination\n"), 0600); err != nil {
		t.Fatal(err)
	}
	transport := &inlineSealTransport{}
	rt, worker := inlinePullWorker(t, transport)
	link, id := inlinePushLink(target, round, data)
	prepared := rt.answerLink(nil, link)
	if prepared.Code != "" || prepared.Status == nil || prepared.Inline != nil || prepared.Source != nil {
		t.Fatalf("push prepare refused or projected inline evidence: code=%q bytes=%d", prepared.Code, len(prepared.Inline))
	}
	if granted := grantInlinePush(rt, link, id); granted.Code != "" || granted.Status == nil {
		t.Fatalf("Tier A receiver was not created at the grant: code=%q", granted.Code)
	}
	if !testharness.WaitFor(t, 10*time.Second, 5*time.Millisecond, func() bool {
		landed, err := os.ReadFile(target)
		return err == nil && bytes.Equal(landed, data)
	}) {
		t.Fatal("inline receiver did not land the retained payload")
	}
	if len(rt.data) != 0 || transport.sealed() != 0 {
		t.Fatal("inline push opened a data attachment or sealed a source")
	}
	if err := worker.Stop(inlineDeadline(t)); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(filepath.Dir(target))
	if err != nil || len(entries) != 1 {
		t.Fatalf("receiver left a tmp beside the landed destination: %d entries %v", len(entries), err)
	}
}

// An ACTIVE Tier A push receiver (created by the real factory, its whole
// inline payload written and synced into the original tmp, parked for its
// commit grant) crosses an image cut in its one exported shape: no object
// reader, and the durable tmp prefix of the entry's retained payload. The
// real codec carries it. A next image that imports it on inherited
// descriptors but whose exec then fails is detached, and the original owner
// thaws with the same tmp prefix and no landing. A second cut's import then
// continues it: the grant lands exactly the payload once and the receiver's
// courier settles on the exact terminal receipt. Before FIX-TIERA2 the
// receiver exported its reader's agreeing checkpoint, which the image
// refused, so no cut could carry an active Tier A push receiver.
// origin: hot-update S7 plan §3 / §8.1–§8.3 / S7 FIX-TIERA2 (FIX-INLINE residual)
func TestTransferInlinePushReceiverSurvivesImageCutAndRollback(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInlinePushReceiverCut(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseInlinePushReceiverCut(t *testing.T, round int) {
	t.Helper()
	for _, forward := range []bool{false, true} {
		// Larger than the sealed floor, so only the entry's grown inline
		// reservation makes this image representable.
		data := bytes.Repeat([]byte(fmt.Sprintf("inline push cut %d %t\n", round, forward)), 3000)
		old := []byte("complete original destination\n")
		target := filepath.Join(t.TempDir(), "destination")
		if err := os.WriteFile(target, old, 0600); err != nil {
			t.Fatal(err)
		}
		transport := &inlineTerminalTransport{}
		rt, worker := inlinePullWorker(t, transport)
		transport.bind(rt)
		link, id := inlinePushLink(target, round, data)
		if prepared := rt.answerLink(nil, link); prepared.Code != "" || prepared.Status == nil {
			t.Fatalf("push prepare refused: code=%q", prepared.Code)
		}
		// Start without the grant: the receiver syncs the whole payload into its
		// tmp, then parks on "awaits commit grant" with the receiver still live.
		if err := worker.Start(id); err != nil {
			t.Fatal(err)
		}
		awaitInlinePushPrefix(t, worker, id, int64(len(data)))
		state, resources := cutInlinePushReceiver(t, worker, data)
		original := state.Receivers[0].ByteDeadline
		abandonInlinePushImport(t, transport, state, resources)
		// The failed exec keeps the original owner: same prefix, same tmp, no landing.
		if err := worker.Thaw(x.Bindings{Transport: transport, Authority: rt, OwnerGeneration: state.Lineage.Generation}, inlineDeadline(t)); err != nil {
			t.Fatalf("original owner did not thaw after the failed exec: %v", err)
		}
		if status, err := worker.Status(id); err != nil || status.DurableOffset != int64(len(data)) || status.Terminal != nil {
			t.Fatalf("rollback lost the original receiver prefix: %+v %v", status, err)
		}
		requireInlinePushTmp(t, target, old, data)
		if !forward {
			// The thawed original owner itself continues: its grant lands the
			// same tmp exactly once and settles the exact terminal.
			settleInlinePushReceiver(t, transport, rt, worker, link, id, data)
		} else {
			// A second cut succeeds: the next image continues the same receiver.
			state, resources = cutInlinePushReceiver(t, worker, data)
			if state.Receivers[0].ByteDeadline != original {
				t.Fatal("rollback refreshed the receiver's original byte deadline")
			}
			continueInlinePushImport(t, transport, link, id, state, resources, data)
			// exec replaced the original heap; release its descriptors only now.
			// Its tmp name already moved to the destination the next image landed.
			if err := worker.Stop(inlineDeadline(t)); err != nil {
				t.Fatal(err)
			}
		}
		landed, err := os.ReadFile(target)
		entries, dirErr := os.ReadDir(filepath.Dir(target))
		if err != nil || !bytes.Equal(landed, data) || dirErr != nil || len(entries) != 1 {
			t.Fatalf("destination is not exactly the payload, or a tmp survived: %d entries %v %v", len(entries), err, dirErr)
		}
		if len(rt.data) != 0 || transport.opened() != 0 {
			t.Fatal("inline push opened a data attachment")
		}
	}
}

// cutInlinePushReceiver freezes the worker and carries its image through the
// real codec. The active receiver must export exactly the reader-free durable
// prefix of the retained payload, and the decoded image must carry it unchanged.
func cutInlinePushReceiver(t *testing.T, worker x.WorkerManager, data []byte) (x.WorkerState, []handoff.Resource) {
	t.Helper()
	frozen, err := worker.Freeze(inlineDeadline(t))
	if err != nil {
		t.Fatalf("an active inline push receiver refused the cut: %v", err)
	}
	state, resources, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Receivers) != 1 || len(state.Entries) != 1 || len(state.Couriers) != 0 {
		t.Fatalf("cut lost or replaced the active receiver: %d receivers %d couriers", len(state.Receivers), len(state.Couriers))
	}
	r, size := state.Receivers[0], int64(len(data))
	if !reflect.DeepEqual(r.Reader, x.ReaderState{}) || r.Checkpoint.DurableOffset != size || r.InlineOffset != size ||
		r.Grant != (x.CommitGrant{}) || r.Outcome != (x.ReceiverOutcome{}) || r.Atomic.Tmp.ResourceID == "" || r.Atomic.Parent.ResourceID == "" ||
		!xferreceive.ValidInlineState(r, data) || !bytes.Equal(state.Entries[0].Prepare.Inline, data) {
		t.Fatalf("active inline receiver exported another shape: reader=%+v offset=%d inline=%d", r.Reader, r.Checkpoint.DurableOffset, r.InlineOffset)
	}
	encoded, err := xferimage.EncodeWorker(state)
	if err != nil {
		t.Fatalf("active inline push receiver is not representable: %v", err)
	}
	decoded, err := xferimage.DecodeWorker(encoded)
	if err != nil || !reflect.DeepEqual(decoded.Receivers, state.Receivers) || !bytes.Equal(decoded.Entries[0].Prepare.Inline, data) {
		t.Fatalf("sealed image changed the inline receiver: %v", err)
	}
	return decoded, resources
}

// abandonInlinePushImport is a next image whose exec fails before activation:
// it imports the sleeping owner on duplicated descriptors (the image is
// importable), then detaches that pristine import and closes only its own
// duplicates. It never thaws, so it never touches the shared tmp.
func abandonInlinePushImport(t *testing.T, transport x.Transport, state x.WorkerState, resources []handoff.Resource) {
	t.Helper()
	files := inheritFiles(t, resources)
	defer files.close()
	rt := inlineRuntime(t)
	imported, _, err := rt.ImportWorker(inlineWorkerConfig(rt, transport), state, files)
	if err != nil {
		t.Fatalf("active inline push receiver did not import: %v", err)
	}
	frozen, err := imported.Freeze(inlineDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	salvage, err := frozen.DetachPristine()
	if err != nil {
		t.Fatal(err)
	}
	for _, resource := range salvage.Resources {
		for _, file := range resource.Files {
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// continueInlinePushImport is the next image after a successful exec: it
// imports and thaws the receiver on inherited descriptors, and the durable
// grant then lands the retained payload and settles the exact terminal.
func continueInlinePushImport(t *testing.T, transport *inlineTerminalTransport, link proto.TransferLink, id x.Identity, state x.WorkerState,
	resources []handoff.Resource, data []byte) {
	t.Helper()
	refuseForgedInlinePrefix(t, transport, state, resources)
	files := inheritFiles(t, resources)
	defer files.close()
	rt := inlineRuntime(t)
	transport.bind(rt)
	worker, _, err := rt.ImportWorker(inlineWorkerConfig(rt, transport), state, files)
	if err != nil {
		t.Fatalf("active inline push receiver did not import: %v", err)
	}
	rt.Bind(worker, func() string { return x.Version })
	if err := worker.Thaw(x.Bindings{Transport: transport, Authority: rt, OwnerGeneration: state.Lineage.Generation + 1}, inlineDeadline(t)); err != nil {
		t.Fatal(err)
	}
	settleInlinePushReceiver(t, transport, rt, worker, link, id, data)
}

// refuseForgedInlinePrefix imports a state whose checkpoint is the right
// length but not the retained payload's prefix, bypassing the codec that
// would refuse it: the reader must still never resume on it. The taken
// duplicates are closed; the original owner is untouched.
// origin: S7 FIX-TIERA2 — importInlineReader binds the reader to the payload
func refuseForgedInlinePrefix(t *testing.T, transport x.Transport, state x.WorkerState, resources []handoff.Resource) {
	t.Helper()
	forged := xferimage.CloneWorkerState(state)
	h := sha256.New()
	_, _ = h.Write(make([]byte, forged.Receivers[0].Checkpoint.DurableOffset))
	marshaled, err := h.(encoding.BinaryMarshaler).MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	forged.Receivers[0].Checkpoint.SHA256State = marshaled
	files := inheritFiles(t, resources)
	defer files.close()
	rt := inlineRuntime(t)
	_, salvage, err := rt.ImportWorker(inlineWorkerConfig(rt, transport), forged, files)
	for _, resource := range salvage.Resources {
		for _, file := range resource.Files {
			_ = file.Close()
		}
	}
	if err == nil {
		t.Fatal("an inline receiver whose checkpoint is not its payload's prefix was imported")
	}
}

// settleInlinePushReceiver delivers the durable grant to the receiver's
// current owner, which must land the payload once and settle on the exact
// complete terminal through its courier, then stop.
func settleInlinePushReceiver(t *testing.T, transport *inlineTerminalTransport, rt *Runtime, worker x.WorkerManager, link proto.TransferLink,
	id x.Identity, data []byte) {
	t.Helper()
	if granted := grantInlinePush(rt, link, id); granted.Code != "" || granted.Status == nil {
		t.Fatalf("receiver refused its grant after the cut: code=%q", granted.Code)
	}
	sum := sha256.Sum256(data)
	_, want, err := inlineTerminalEvidence(id, x.ReceiverOutcome{Outcome: x.OutcomeLanded, ParentSynced: true, Bytes: int64(len(data)), SHA256: hex.EncodeToString(sum[:])})
	if err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 10*time.Second, 5*time.Millisecond, func() bool {
		status, err := worker.Status(id)
		return err == nil && status.Phase == x.PhaseSettled
	}) {
		t.Fatal("receiver did not settle after its grant")
	}
	status, err := worker.Status(id)
	if err != nil || status.State != x.StatusTerminal || status.Terminal == nil || *status.Terminal != want {
		t.Fatalf("receiver settled without the exact complete terminal: %+v %v", status.Terminal, err)
	}
	sent := transport.sent()
	if len(sent) == 0 {
		t.Fatal("receiver settled without its courier sending the outcome")
	}
	for _, outcome := range sent {
		if outcome.Outcome != x.OutcomeLanded || outcome.Primitive != x.PrimitiveRename || outcome.Grant.GrantNonce != "grant" ||
			outcome.Bytes != int64(len(data)) || outcome.SHA256 != want.SHA256 || !outcome.ParentSynced || outcome.Code != "" {
			t.Fatalf("courier carried another receiver outcome: %+v", outcome)
		}
	}
	if err := worker.Stop(inlineDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if len(rt.data) != 0 {
		t.Fatal("inline receiver opened a data attachment")
	}
}

// inlinePushLink is the broker's prepare for a Tier A push of data.
func inlinePushLink(target string, round int, data []byte) (proto.TransferLink, x.Identity) {
	link, id := inlinePullLink(target, round)
	sum := sha256.Sum256(data)
	link.Intent.Verb, link.Intent.Size, link.Intent.SHA256, link.Intent.Force = "push", int64(len(data)), hex.EncodeToString(sum[:]), true
	link.Budget.Size, link.Budget.SourceSealed = int64(len(data)), true
	link.Inline = data
	return link, id
}

// grantInlinePush delivers the broker's durable commit grant on the link.
func grantInlinePush(rt *Runtime, link proto.TransferLink, id x.Identity) proto.TransferLinkResult {
	terminal := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	grant := proto.CommitGrant{Identity: proto.Identity(id), GrantNonce: "grant", ReceiverInstance: id.AgentInstance, TerminalID: hex.EncodeToString(terminal[:])}
	return rt.answerLink(nil, proto.TransferLink{Op: proto.TransferLinkGrant, CallID: "xfer-grant-" + id.TransferID, Identity: link.Identity,
		Origin: link.Origin, Intent: link.Intent, Grant: &grant})
}

func awaitInlinePushPrefix(t *testing.T, worker x.WorkerManager, id x.Identity, size int64) {
	t.Helper()
	if !testharness.WaitFor(t, 10*time.Second, 5*time.Millisecond, func() bool {
		status, err := worker.Status(id)
		return err == nil && status.DurableOffset == size
	}) {
		t.Fatal("inline receiver did not sync its whole payload into its tmp")
	}
}

// requireInlinePushTmp reads the independent filesystem: the destination is
// still the old complete file and the one tmp beside it holds the payload.
func requireInlinePushTmp(t *testing.T, target string, old, data []byte) {
	t.Helper()
	if current, err := os.ReadFile(target); err != nil || !bytes.Equal(current, old) {
		t.Fatalf("destination changed before the grant: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(target))
	if err != nil || len(entries) != 2 {
		t.Fatalf("expected the destination and one tmp: %d entries %v", len(entries), err)
	}
	for _, entry := range entries {
		if entry.Name() == filepath.Base(target) {
			continue
		}
		if tmp, err := os.ReadFile(filepath.Join(filepath.Dir(target), entry.Name())); err != nil || !bytes.Equal(tmp, data) {
			t.Fatalf("tmp is not the durable payload prefix: %v", err)
		}
	}
}

// inlineTerminalEvidence is the deciding broker's exact canonical TERMINAL and
// history receipt for a receiver outcome.
func inlineTerminalEvidence(id x.Identity, o x.ReceiverOutcome) (x.CanonicalRecord, x.TerminalReceipt, error) {
	kind := "failed"
	if o.Outcome == x.OutcomeLanded && o.ParentSynced && o.Code == "" {
		kind = "complete"
	}
	row, err := json.Marshal(schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: "push", Session: id.SID, Node: id.NID,
		ActorFp: id.ActorFP, TransferID: id.TransferID, Bytes: o.Bytes, SHA256: o.SHA256, Code: o.Code})
	digest, request := sha256.Sum256(row), sha256.Sum256([]byte("request "+id.TransferID))
	terminal := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	record := x.CanonicalRecord{Identity: id, Kind: kind, ReqID: hex.EncodeToString(request[:]), Digest: hex.EncodeToString(digest[:]), Bytes: row}
	return record, x.TerminalReceipt{TerminalID: hex.EncodeToString(terminal[:]), Kind: kind, CanonicalDigest: record.Digest, ReqID: record.ReqID,
		HistoryStream: "history-" + id.SID, HistorySeq: 7, Bytes: o.Bytes, SHA256: o.SHA256, Code: o.Code}, err
}

// inlineTerminalTransport is the deciding broker for a push receiver's
// courier: it answers the receiver's actual outcome with the exact terminal
// evidence and hands the canonical bytes to the runtime that owns the courier,
// as the agent transport does with a broker reply. An inline push opens no
// data attachment, so any OpenData is counted as a defect.
type inlineTerminalTransport struct {
	mu       sync.Mutex
	rt       *Runtime
	outcomes []x.ReceiverOutcome
	attached int
}

func (p *inlineTerminalTransport) bind(rt *Runtime) { p.mu.Lock(); p.rt = rt; p.mu.Unlock() }

func (p *inlineTerminalTransport) Request(req x.Request, _ x.Deadline) (x.Response, x.Attempt, error) {
	if req.Op != x.OpTerminal || req.Outcome == nil {
		return x.Response{}, x.Attempt{}, fmt.Errorf("inline push receiver sent %s", req.Op)
	}
	record, receipt, err := inlineTerminalEvidence(req.Identity, *req.Outcome)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	p.mu.Lock()
	p.outcomes = append(p.outcomes, *req.Outcome)
	rt := p.rt
	p.mu.Unlock()
	rt.mu.Lock()
	rt.canons[x.Key{SID: req.Identity.SID, TransferID: req.Identity.TransferID}] = record
	rt.mu.Unlock()
	return x.Response{Receipt: &receipt}, x.Attempt{Sent: true}, nil
}

func (p *inlineTerminalTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	p.mu.Lock()
	p.attached++
	p.mu.Unlock()
	return nil, errors.New("inline push opened a data attachment")
}
func (*inlineTerminalTransport) CloseData(x.Identity, x.Deadline) error { return nil }
func (p *inlineTerminalTransport) opened() int                          { p.mu.Lock(); defer p.mu.Unlock(); return p.attached }
func (p *inlineTerminalTransport) sent() []x.ReceiverOutcome {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]x.ReceiverOutcome(nil), p.outcomes...)
}
