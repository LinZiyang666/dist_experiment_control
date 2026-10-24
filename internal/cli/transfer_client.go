package cli

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/transferpath"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/nats-io/nats.go"
)

// RetireWriter borrows the original authenticated ctl connection. Revocation
// cannot turn this evidence-only request into byte or terminal authority, and
// its digest-bound ACK is required before the client releases writer custody.
// origin: hot-update S7 plan §2.3 / FIX-TERM request 3
func (t *TransferTransport) RetireWriter(id x.Identity, origin x.Origin, state x.WriterState, d x.Deadline) error {
	if state.ObjectSpec.Identity != id || !state.RetirementFence.WriterRetired || id.ActorFP != t.id.Fingerprint || id.ObjectVersion == "" {
		return errors.New(x.CodeIntentConflict)
	}
	s := state.ObjectSpec
	f := state.RetirementFence
	for _, receipt := range state.Receipts {
		f.ExpectedStreamSeq = max(f.ExpectedStreamSeq, receipt.StreamSeq)
	}
	if pending := state.PendingAttempt; pending != nil {
		f.ExpectedStreamSeq = max(f.ExpectedStreamSeq, pending.ExpectedStreamSeq)
	}
	if metadata := state.MetaReceipt; metadata != nil {
		f.ExpectedStreamSeq = max(f.ExpectedStreamSeq, metadata.MetadataSeq)
		f.PreviousMetadataSeq = max(f.PreviousMetadataSeq, metadata.MetadataSeq)
	}
	req := proto.TransferWriterRetirementReq{Version: x.Version, CallID: id.TransferID + "-writer-retired", Identity: proto.Identity(id), Origin: proto.Origin(origin),
		Writer: proto.TransferWriterRetirement{Bucket: s.Bucket, Stream: s.Stream, StreamCreated: s.StreamCreated, SHA256: s.SHA256, Size: s.Size,
			ChunkSize: s.ChunkSize, Chunks: s.Chunks, AttachmentGeneration: state.AttachmentGeneration, WriterRetired: true,
			ExpectedStreamSeq: f.ExpectedStreamSeq, PreviousMetadataSeq: f.PreviousMetadataSeq}}
	body, err := json.Marshal(req)
	if err != nil {
		return err
	}
	// ctx-none: frozen retirement port supplies an absolute deadline, no caller context.
	reply, _, err := RequestTransferWithin(context.Background(), t.control, proto.SubjCtrlTransferFinalize(t.id.PublicKey, id.SID, id.TransferID), body,
		RetryConfig{Deadline: d, LogicalID: req.CallID, Observer: t.observer})
	if err != nil {
		return err
	}
	var ack proto.TransferWriterRetirementResp
	if len(reply.Data) > 4096 || proto.DecodeTransferWire(reply.Data, &ack) != nil {
		return errors.New(x.CodeReconcileRequired)
	}
	if ack.Code != "" {
		return &xferclient.Error{Code: ack.Code, TransferID: id.TransferID}
	}
	sum := sha256.Sum256(body)
	if ack.Error != "" || ack.CallID != req.CallID || ack.Identity != req.Identity || ack.Origin != req.Origin || ack.Revision == 0 || ack.WitnessDigest != hex.EncodeToString(sum[:]) {
		return errors.New(x.CodeReconcileRequired)
	}
	return nil
}

// CanonicalTransferWire retains the request's verb solely to decode its legacy
// outer prepare envelope. Origin and CallID travel unchanged on every attempt.
// origin: hot-update S7 plan §6.2 / §7 / INTEG-D
type CanonicalTransferWire struct {
	Actor string
	mu    sync.Mutex
	verb  string
}

func (w *CanonicalTransferWire) Encode(r x.Request) (string, []byte, error) {
	if r.Op == x.OpProbe && r.Intent != nil {
		return xfercontrol.EncodePullSelectionProbe(w.Actor, r)
	}
	if r.Op == x.OpPrepare && r.Intent != nil {
		w.mu.Lock()
		w.verb = r.Intent.Verb
		w.mu.Unlock()
	}
	return proto.EncodeTransferRequest(w.Actor, r)
}
func (w *CanonicalTransferWire) Decode(op x.Operation, b []byte) (x.Response, error) {
	if op == x.OpProbe {
		return xfercontrol.DecodePullSelectionProbe(b)
	}
	w.mu.Lock()
	verb := w.verb
	w.mu.Unlock()
	return proto.DecodeTransferResponse(op, verb, b)
}

// TransferClientConfig binds the real IO owners. Control is borrowed; the
// returned close function joins both dedicated attachments before control can
// be closed by the command. No configured factory can call SDK Put.
// origin: hot-update S7 plan §7 / T34 / INTEG-D
func TransferClientConfig(nc *nats.Conn, url string, id *Identity, sid string) (xferclient.Config, func() error, error) {
	var cfg xferclient.Config
	if nc == nil || nc.IsClosed() || id == nil || id.PublicKey == "" || id.Fingerprint == "" || url == "" || sid == "" {
		return cfg, nil, errors.New("transfer client dependencies unavailable")
	}
	boot, err := procrequest.CurrentBootID()
	if err != nil {
		return cfg, nil, err
	}
	transport, err := NewTransferTransport(nc, url, id, &CanonicalTransferWire{Actor: id.PublicKey}, nil)
	if err != nil {
		return cfg, nil, err
	}
	cfg = xferclient.Config{Base: x.Base{Clock: func() (string, int64, error) {
		now, err := controlbudget.Now()
		return boot, now, err
	}, UTC: time.Now, Capacity: &transferCapacity{held: make(map[x.ReservationID]x.Reservation)}},
		Identity: x.Identity{SID: sid, ActorFP: id.Fingerprint}, Transport: transport,
		MakeWriter: jsstream.NewTransferWriter, MakeReader: jsstream.NewTransferReader, MakeCourier: xfercourier.New}
	return cfg, transport.Close, nil
}

// SelectTransferClient performs only precreation negotiation. An unavailable
// probe never selects legacy; only a complete authenticated cap0 response can
// do that. Tier/geometry and source digest are sealed before the permit.
// origin: hot-update S7 plan §7 / LQ2 / INTEG-D
func SelectTransferClient(cfg *xferclient.Config, d x.Deadline) (legacy bool, resultErr error) {
	fail := func(code string) (bool, error) {
		return false, &xferclient.Error{Code: code, TransferID: cfg.Identity.TransferID}
	}
	// Creation fixes the engine for this invocation. A caller retaining an
	// entry or fixed object must reconcile it, even if today's caps are zero.
	// origin: S7 internal review rv-wiring F6 / rv-n1compat F2
	if cfg.Identity.EntryNonce != "" || cfg.Identity.ObjectVersion != "" {
		return fail(x.CodeIntentConflict)
	}
	// An old broker (or one without an installed graph) has no transfer probe
	// subject. Its complete, authenticated caps reply WITHOUT a transfer
	// version is the explicit cap0 answer; an unanswered caps request is an
	// error, never a reason to downgrade. Only a transfer-v1 broker is probed.
	// origin: hot-update S7 plan §9 / INTEG-D capability compatibility
	if broker, ok := cfg.Transport.(interface {
		Capabilities(string, controlbudget.Deadline) (proto.CapsResp, error)
	}); ok {
		caps, err := broker.Capabilities(cfg.Identity.SID, d)
		if err != nil {
			return false, err
		}
		if caps.Code != "" {
			return fail(caps.Code)
		}
		if !caps.OK {
			return fail(x.CodeResultUnknown)
		}
		if caps.TransferVersion == "" {
			return true, nil
		}
		if caps.TransferVersion != x.Version {
			return fail(x.CodePayloadUnsupported)
		}
	}
	request := x.Request{Op: x.OpProbe, CallID: cfg.Identity.TransferID + "-select", Identity: cfg.Identity}
	if cfg.Intent.Verb == "pull" && cfg.Intent.Tier == "" {
		// Inspect before any creation authority exists. The eventual pinned
		// source seal still owns size/SHA and the original budget; this query
		// chooses the engine without turning a Tier B entry into inline work.
		// origin: hot-update S7 plan §2 / §7 / S7 internal review INTEG-O F4
		request.Intent = &x.Intent{Verb: "pull", Path: cfg.Intent.Path}
	}
	response, _, err := cfg.Transport.Request(request, d)
	if err != nil {
		return false, err
	}
	if response.Code != "" {
		return fail(response.Code)
	}
	p := response.Probe
	if p == nil || p.Error != "" || response.Error != "" {
		return fail(x.CodeResultUnknown)
	}
	if p.Code != "" {
		return fail(p.Code)
	}
	if p.Version == "" && p.HistoryGuardVersion == "" && p.BucketConfigVersion == "" {
		return true, nil
	}
	if p.Version != x.Version || p.HistoryGuardVersion != x.Version || p.BucketConfigVersion != x.Version || p.InstanceID == "" || p.MaxPayload <= 0 || p.MaxMsgSize <= 0 {
		return fail(x.CodePayloadUnsupported)
	}
	cfg.Identity.AgentInstance = p.InstanceID
	// Unknown pull size used tier B, preserving the immutable request intent.
	// Callers with known small sources could explicitly select inline pull;
	// automatic pulls now inspect their source before that intent is sealed.
	if cfg.Intent.Tier == "" {
		tier := "b"
		if cfg.Intent.Verb == "pull" {
			if response.Source == nil || response.Source.Size < 0 || response.Source.Size > proto.XferMaxBytes {
				return fail(x.CodePayloadUnsupported)
			}
			if response.Source.Size < min(int64(proto.XferTierAMaxBytes), p.MaxPayload/2-4096, p.MaxMsgSize/2-4096) {
				tier = "a"
			}
		}
		if cfg.Intent.Verb == "push" && cfg.Intent.Size <= min(int64(proto.XferTierAMaxBytes), p.MaxPayload/2-4096) {
			tier = "a"
		}
		cfg.Intent.Tier = tier
	}
	if cfg.Intent.Tier == "b" {
		limit := min(p.MaxPayload, p.MaxMsgSize) - 4096
		if limit < proto.TransferMinChunkSize {
			return fail(x.CodePayloadUnsupported)
		}
		cfg.Intent.ChunkSize = uint32(min(int64(128*1024), limit))
	}
	if cfg.Intent.Verb == "push" {
		if _, err = proto.TransferBudget(cfg.Intent.Tier, cfg.Intent.Size, cfg.Intent.RateLimitBPS); err != nil {
			return fail(x.CodeBudgetExceeded)
		}
		reservation, reserveErr := cfg.Capacity.Reserve(x.Reservation{NonReplayBytes: 32 * 1024})
		if reserveErr != nil || reservation == "" {
			return fail(x.CodeCapacityExceeded)
		}
		defer finishTransferCleanup(&resultErr, func() error { return cfg.Capacity.Release(reservation) })
		h := sha256.New()
		buffer := make([]byte, 32*1024)
		for offset := int64(0); offset < cfg.Intent.Size; {
			if _, err = controlbudget.Remaining(d); err != nil {
				return false, err
			}
			length := min(int64(len(buffer)), cfg.Intent.Size-offset)
			n, readErr := cfg.Source.ReadAt(buffer[:length], offset)
			if int64(n) != length || readErr != nil && !errors.Is(readErr, io.EOF) {
				return fail(x.CodeSourceChanged)
			}
			_, _ = h.Write(buffer[:n])
			offset += int64(n)
		}
		cfg.Intent.SHA256 = hex.EncodeToString(h.Sum(nil))
	}
	return false, nil
}

func NewTransferReceiver(path string, force bool) (x.AtomicPort, error) {
	vp, err := transferpath.ValidateForWrite(path, nil, transferpath.ModeOpen)
	if err != nil {
		return nil, err
	}
	// The tmp nonce is not another business transfer ID.
	now, err := controlbudget.Now()
	if err != nil {
		return nil, err
	}
	return transferpath.NewAtomicReceiver(vp, strconv.FormatInt(now, 16), force)
}

// A ctl process owns one bounded pool for the invocation and its courier.
// Accounting rejects before allocation and retains failed-release obligations.
// origin: hot-update S7 plan §8.3 / INTEG-D
type transferCapacity struct {
	mu   sync.Mutex
	next uint64
	held map[x.ReservationID]x.Reservation
	used x.Reservation
}

func (a *transferCapacity) Reserve(r x.Reservation) (x.ReservationID, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.NonReplayBytes < 0 || r.ReplayBytes < 0 || r.SealedBytes < 0 || r.PublicBytes < 0 || r.HandoffBytes < 0 ||
		r.NonReplayBytes > 64<<20-a.used.NonReplayBytes || r.ReplayBytes > 256<<20-a.used.ReplayBytes || r.SealedBytes > 32<<20-a.used.SealedBytes || r.PublicBytes > 16<<20-a.used.PublicBytes || r.HandoffBytes > 64<<20-a.used.HandoffBytes || r.FDs > 16384-a.used.FDs || r.Resources > 16384-a.used.Resources {
		return "", errors.New(x.CodeCapacityExceeded)
	}
	a.next++
	id := x.ReservationID(strconv.FormatUint(a.next, 10))
	a.held[id] = r
	a.used.NonReplayBytes += r.NonReplayBytes
	a.used.ReplayBytes += r.ReplayBytes
	a.used.SealedBytes += r.SealedBytes
	a.used.PublicBytes += r.PublicBytes
	a.used.HandoffBytes += r.HandoffBytes
	a.used.FDs += r.FDs
	a.used.Resources += r.Resources
	return id, nil
}
func (a *transferCapacity) Release(id x.ReservationID) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	r, ok := a.held[id]
	if !ok {
		return errors.New("unknown transfer reservation")
	}
	delete(a.held, id)
	a.used.NonReplayBytes -= r.NonReplayBytes
	a.used.ReplayBytes -= r.ReplayBytes
	a.used.SealedBytes -= r.SealedBytes
	a.used.PublicBytes -= r.PublicBytes
	a.used.HandoffBytes -= r.HandoffBytes
	a.used.FDs -= r.FDs
	a.used.Resources -= r.Resources
	return nil
}

// OwnTransferClient transfers control ownership only after authenticated command
// admission, and closes it after every owned attachment has joined.
// origin: hot-update S7 plan §7 / INTEG-D
func OwnTransferClient(nc *nats.Conn, id *Identity, sid string) (xferclient.Config, func() error, error) {
	cfg, closeData, err := TransferClientConfig(nc, nc.ConnectedUrl(), id, sid)
	if err != nil {
		nc.Close()
		return cfg, nil, err
	}
	return cfg, func() error {
		err := closeData()
		nc.Close()
		return err
	}, nil
}
