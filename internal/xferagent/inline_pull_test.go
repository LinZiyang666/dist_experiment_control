package xferagent

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xferworker"
	"github.com/nats-io/nats.go/jetstream"
	"golang.org/x/sys/unix"
)

// The production Tier A pull path: the real agent factory pins and seals the
// source without a data attachment, the prepare link answers with the exact
// retained copy and the durably acknowledged seal, the copy crosses a real
// worker freeze/codec/import and a pending prepare replay after it returns the
// same bytes (the source path now names another file), a rollback keeps it on
// the original owner, and the receiver's exact terminal receipt retires it.
// origin: hot-update S7 plan §8.1–§8.2 / §6.1 / FIX-INLINE (FIX-TIERA report item 3)
func TestTransferInlinePullSurvivesCutReplayAndSettlement(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseInlinePullCut(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, t.Name(), g)
	testharness.AssertNoFDLeak(t, t.Name(), fd, 0)
}

func exerciseInlinePullCut(t *testing.T, round int) {
	t.Helper()
	// Larger than the sealed floor, so only a grown reservation can cut it.
	data := bytes.Repeat([]byte(fmt.Sprintf("inline pull %d\n", round)), 9000)
	path := filepath.Join(t.TempDir(), "source")
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	link, id := inlinePullLink(path, round)
	transport := &inlineSealTransport{budget: x.BudgetSeal(*link.Budget)}
	rt, worker := inlinePullWorker(t, transport)
	result := rt.answerLink(nil, link)
	if result.Code != "" || result.Status == nil || result.Source == nil || !bytes.Equal(result.Inline, data) {
		t.Fatalf("prepare did not return the retained copy: code=%q bytes=%d seal=%v", result.Code, len(result.Inline), result.Source)
	}
	sum := sha256.Sum256(data)
	if result.Source.Size != int64(len(data)) || result.Source.SHA256 != hex.EncodeToString(sum[:]) || result.Source.Chunks != 0 || result.Source.ChunkSize != 0 ||
		transport.sealed() != 1 || transport.seal() != x.SourceSeal(*result.Source) {
		t.Fatalf("prepare seal is not the durably acknowledged inline seal: %+v", result.Source)
	}
	if len(rt.data) != 0 {
		t.Fatal("inline pull opened a data attachment")
	}
	frozen, err := worker.Freeze(inlineDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	state, resources, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := state.CapacityReservations[0].Reservation, xferimage.WorkerInlineReservation(state.Entries[0].Prepare, int64(len(data))); got != want {
		t.Fatalf("entry reservation was not grown for the copy: got %+v want %+v", got, want)
	}
	encoded, err := xferimage.EncodeWorker(state)
	if err != nil {
		t.Fatalf("inline pull image is not representable: %v", err)
	}
	decoded, err := xferimage.DecodeWorker(encoded)
	if err != nil || len(decoded.Senders) != 1 || !bytes.Equal(decoded.Senders[0].Inline, data) {
		t.Fatalf("sealed image lost the inline copy: %v", err)
	}
	if err := os.Rename(path, path+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	importInlinePull(t, transport, link, id, decoded, resources, result)
	// The refused/rolled-back cut keeps the original owner and its copy.
	if err := worker.Thaw(x.Bindings{Transport: transport, Authority: rt, OwnerGeneration: state.Lineage.Generation}, inlineDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if again := rt.answerLink(nil, link); again.Code != "" || !bytes.Equal(again.Inline, data) || *again.Source != *result.Source {
		t.Fatalf("rollback lost the original copy: code=%q bytes=%d", again.Code, len(again.Inline))
	}
	// The original (never imported) sender finished its own copy, so the
	// receiver's complete receipt settles it too, not only an imported one.
	settleInlinePull(t, rt, worker, id, result)
}

// settleInlinePull delivers the receiver's exact complete receipt to a sender
// that owns no courier; only a finished copy may be settled complete.
func settleInlinePull(t *testing.T, rt *Runtime, worker x.WorkerManager, id x.Identity, original proto.TransferLinkResult) {
	t.Helper()
	terminal := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	receipt := x.TerminalReceipt{TerminalID: hex.EncodeToString(terminal[:]), Kind: "complete", CanonicalDigest: "canonical", ReqID: "request",
		HistoryStream: "history", HistorySeq: 7, Bytes: int64(len(original.Inline)), SHA256: original.Source.SHA256}
	status, err := worker.Apply(x.Control{Kind: x.ControlReceipt, Identity: id, Receipt: &receipt}, inlineDeadline(t))
	if err != nil || status.State != x.StatusTerminal || status.Phase != x.PhaseSettled {
		t.Fatalf("receiver's exact terminal did not settle the inline sender: %+v %v", status, err)
	}
	if _, _, ok := rt.inlineOf(id, inlineDeadline(t)); ok {
		t.Fatal("settled inline sender still serves its copy")
	}
	if err := worker.Stop(inlineDeadline(t)); err != nil {
		t.Fatal(err)
	}
}

// importInlinePull is the next image: it inherits duplicated original
// descriptors only, replays the pending prepare, then settles exactly.
func importInlinePull(t *testing.T, transport *inlineSealTransport, link proto.TransferLink, id x.Identity, state x.WorkerState, resources []handoff.Resource, original proto.TransferLinkResult) {
	t.Helper()
	files := inheritFiles(t, resources)
	defer files.close()
	rt := inlineRuntime(t)
	cfg := inlineWorkerConfig(rt, transport)
	worker, _, err := rt.ImportWorker(cfg, state, files)
	if err != nil {
		t.Fatal(err)
	}
	rt.Bind(worker, func() string { return x.Version })
	if err := worker.Thaw(x.Bindings{Transport: transport, Authority: rt, OwnerGeneration: state.Lineage.Generation + 1}, inlineDeadline(t)); err != nil {
		t.Fatal(err)
	}
	replay := rt.answerLink(nil, link)
	if replay.Code != "" || !bytes.Equal(replay.Inline, original.Inline) || replay.Source == nil || *replay.Source != *original.Source {
		t.Fatalf("pending prepare replay after import lost the original copy: code=%q bytes=%d", replay.Code, len(replay.Inline))
	}
	if transport.sealed() != 1 {
		t.Fatal("imported sender sealed its source again")
	}
	settleInlinePull(t, rt, worker, id, original)
}

// An inline answer that exceeds the agent connection's payload ceiling would
// be dropped by the server and read by the broker as sent-unknown; it must
// leave as an explicit payload_unsupported refusal carrying no bytes. Replies
// without inline bytes are never rewritten by that ceiling.
// origin: hot-update S7 plan §2.1 / §8.1 / FIX-INLINE
func TestTransferInlineReplyRefusesOversizedPayload(t *testing.T) {
	data := bytes.Repeat([]byte{'i'}, 4096)
	sum := sha256.Sum256(data)
	seal := proto.SourceSeal{Size: int64(len(data)), SHA256: hex.EncodeToString(sum[:]), SourceFingerprint: "pinned"}
	status := &proto.TransferStatusResp{State: proto.StatusState(x.StatusLive), Phase: proto.Phase(x.PhasePrepared)}
	result := proto.TransferLinkResult{Version: x.Version, InstanceID: "agent-instance", Status: status, Inline: data, Source: &seal}
	for _, tc := range []struct {
		name  string
		limit int64
		fits  bool
	}{{"within ceiling", 1 << 20, true}, {"over ceiling", int64(len(data)), false}} {
		body, err := encodeLinkReply(result, tc.limit)
		if err != nil || int64(len(body)) > max(tc.limit, int64(len(data))) {
			t.Fatalf("%s: reply %d bytes over limit %d: %v", tc.name, len(body), tc.limit, err)
		}
		got, legacy, err := proto.DecodeTransferLinkResult(body)
		if err != nil || legacy {
			t.Fatalf("%s: undecodable reply: %v", tc.name, err)
		}
		if tc.fits && (got.Code != "" || !bytes.Equal(got.Inline, data) || got.Source == nil || *got.Source != seal || got.Status == nil) {
			t.Fatalf("%s: reply lost the inline evidence: %+v", tc.name, got)
		}
		if !tc.fits && (got.Code != x.CodePayloadUnsupported || got.Inline != nil || got.Source != nil || got.Status != nil) {
			t.Fatalf("%s: oversized inline reply was not an explicit refusal: code=%q bytes=%d", tc.name, got.Code, len(got.Inline))
		}
	}
	plain := proto.TransferLinkResult{Version: x.Version, InstanceID: "agent-instance", Status: status}
	body, err := encodeLinkReply(plain, 1)
	if got, _, decodeErr := proto.DecodeTransferLinkResult(body); err != nil || decodeErr != nil || got.Code != "" || got.Status == nil {
		t.Fatalf("a reply without inline bytes was rewritten: %+v %v %v", got, err, decodeErr)
	}
}

func inlineRuntime(t *testing.T) *Runtime {
	t.Helper()
	rt, err := New(Host{SID: "session", NID: func() string { return "node" }, Instance: func() string { return "agent-instance" },
		JetStream: func() jetstream.JetStream { return nil }, Mode: 0})
	if err != nil {
		t.Fatal(err)
	}
	return rt
}

func inlineWorkerConfig(rt *Runtime, transport x.Transport) x.WorkerConfig {
	cfg := rt.WorkerConfig()
	cfg.Identity.NID, cfg.Identity.AgentInstance, cfg.Transport = "node", "agent-instance", transport
	return cfg
}

func inlinePullWorker(t *testing.T, transport x.Transport) (*Runtime, x.WorkerManager) {
	t.Helper()
	rt := inlineRuntime(t)
	worker, err := xferworker.NewWorker(inlineWorkerConfig(rt, transport))
	if err != nil {
		t.Fatal(err)
	}
	rt.Bind(worker, func() string { return x.Version })
	return rt, worker
}

// inlinePullLink is the broker's original prepare for an unknown-size pull.
func inlinePullLink(path string, round int) (proto.TransferLink, x.Identity) {
	digest := sha256.Sum256([]byte(path))
	id := x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent-instance", TransferID: fmt.Sprintf("inline-%d", round),
		EntryNonce: fmt.Sprintf("entry-%d", round), IntentDigest: hex.EncodeToString(digest[:])}
	origin := proto.Origin{BrokerID: "broker", InstanceID: "broker-instance", BootID: "broker-boot", Generation: 1}
	now := time.Now().UTC()
	intent := proto.Intent{Verb: "pull", Tier: "a", Path: path}
	permit := proto.CreationPermit{Identity: proto.Identity(id), Origin: origin, IssuedAt: now, CreateUntil: now.Add(30 * time.Second), PermitNonce: "permit", Signature: "verified-by-broker"}
	budget := proto.BudgetSeal{StartedAt: now, EffectiveBudgetNS: int64(proto.XferTimeoutTierA), RemainingNS: int64(proto.XferTimeoutTierA)}
	return proto.TransferLink{Op: proto.TransferLinkPrepare, CallID: "call-" + id.TransferID, Identity: proto.Identity(id), Origin: origin,
		Intent: &intent, Permit: &permit, Budget: &budget}, id
}

func inlineDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// inlineSealTransport is the broker's durable source admission: it records
// every seal and answers with the original budget tightened to that seal.
type inlineSealTransport struct {
	mu     sync.Mutex
	budget x.BudgetSeal
	seals  []x.SourceSeal
}

func (p *inlineSealTransport) Request(req x.Request, _ x.Deadline) (x.Response, x.Attempt, error) {
	if req.Op != x.OpSource || req.Source == nil {
		return x.Response{}, x.Attempt{}, fmt.Errorf("inline pull sent %s", req.Op)
	}
	required, err := proto.TransferBudget("a", req.Source.Size, req.Source.RateLimitBPS)
	if err != nil {
		return x.Response{}, x.Attempt{}, err
	}
	p.mu.Lock()
	p.seals = append(p.seals, *req.Source)
	budget := p.budget
	p.mu.Unlock()
	budget.Size, budget.SourceSealed, budget.EffectiveBudgetNS = req.Source.Size, true, int64(required)
	status := x.Status{State: x.StatusLive, Phase: x.PhaseOpen, Identity: req.Identity, Budget: budget}
	return x.Response{Status: &status}, x.Attempt{Sent: true}, nil
}
func (*inlineSealTransport) OpenData(x.Identity, x.Deadline) (jetstream.JetStream, error) {
	return nil, errors.New("inline pull opened a data attachment")
}
func (*inlineSealTransport) CloseData(x.Identity, x.Deadline) error { return nil }
func (p *inlineSealTransport) sealed() int                          { p.mu.Lock(); defer p.mu.Unlock(); return len(p.seals) }
func (p *inlineSealTransport) seal() x.SourceSeal {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.seals) == 0 {
		return x.SourceSeal{}
	}
	return p.seals[0]
}

// inheritedFiles hands each duplicated descriptor to the importer once.
type inheritedFiles map[string]*os.File

// inheritFiles duplicates every exported original, as exec inherits them; the
// originals stay with the frozen owner for its rollback. The handoff marks
// inherited descriptors close-on-exec, which the atomic importer requires.
func inheritFiles(t *testing.T, resources []handoff.Resource) inheritedFiles {
	t.Helper()
	files := inheritedFiles{}
	for _, resource := range resources {
		for name, file := range resource.Files {
			fd, err := unix.FcntlInt(file.Fd(), unix.F_DUPFD_CLOEXEC, 0)
			if err != nil {
				files.close()
				t.Fatal(err)
			}
			files[resource.ID+"/"+name] = os.NewFile(uintptr(fd), name)
		}
	}
	return files
}

func (f inheritedFiles) TakeFile(id, name string) (*os.File, error) {
	file := f[id+"/"+name]
	if file == nil {
		return nil, errors.New("missing inherited transfer file")
	}
	delete(f, id+"/"+name)
	return file, nil
}
func (f inheritedFiles) close() {
	for _, file := range f {
		_ = file.Close()
	}
}
