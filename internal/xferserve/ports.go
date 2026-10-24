package xferserve

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go/jetstream"
)

// Rows returns the exact retained decision row for id.
type Rows func(x.Identity, x.Deadline) (x.LedgerRow, error)

// RowsFrom reads the durable ledger before borrowing the tracker serializer:
// a receiver callback can already hold that serializer. The tracker's own
// retained row survives ledger retirement for the result-retention window, so
// a courier replay after eager settlement still receives exact canonical bytes.
// origin: hot-update S7 plan §5.1 / §5.2 — S7 production activation
// The original ordering was: "RowsFrom reads the tracker's own retained row
// first: it survives the ledger's durable retirement for the result-retention
// window, so a courier replay after eager settlement still receives the exact
// canonical bytes. The durable ledger census is the fallback (for example while
// the view is busy)." That ordering reentered the receiver's held serializer;
// preserve the retained-row responsibility with a durable-first lookup instead.
// origin: hot-update S7 plan §2.3 / INTEG-W retained comment reconciliation
func RowsFrom(decisions func() x.DecisionPort, ledger func() x.LedgerPort) Rows {
	return func(id x.Identity, d x.Deadline) (x.LedgerRow, error) {
		// A recursive decision read would exhaust the inherited deadline before
		// the ledger fallback. Durable rows already contain the required evidence.
		// origin: hot-update S7 plan §2.3 / FIX-TERM request 3
		if row, err := rowOf(ledger, id, d); err == nil {
			return row, nil
		}
		if view := decisions(); view != nil {
			row, err := view.Read(x.Key{SID: id.SID, TransferID: id.TransferID}, d)
			if err == nil && row.Identity == id {
				return row, nil
			}
		}
		return rowOf(ledger, id, d)
	}
}

// rowOf returns the exact retained row for id from the current ledger census.
// A missing or incomplete census is unknown, never an absent responsibility.
func rowOf(ledger func() x.LedgerPort, id x.Identity, d x.Deadline) (x.LedgerRow, error) {
	l := ledger()
	if l == nil {
		return x.LedgerRow{}, errors.New(x.CodeOwnerRecovering)
	}
	census, err := l.Scan(d)
	if err != nil {
		return x.LedgerRow{}, err
	}
	if !census.Ready || !census.Complete {
		return x.LedgerRow{}, fmt.Errorf("%s: ledger census is incomplete", x.CodeOwnerRecovering)
	}
	for _, row := range census.Rows {
		if row.Identity == id {
			return row, nil
		}
	}
	return x.LedgerRow{}, fmt.Errorf("%s: no retained row", x.CodeResultUnknown)
}

// Receiver resolves receiver evidence from what the original receiver itself
// reported into the durable decision. Without that evidence it returns an
// error: a deadline or an unreachable receiver never supplies a synthetic
// not-entered outcome (the watchdog keeps the reservation).
// origin: hot-update S7 plan §4 / T12 / M06
type Receiver struct {
	Rows Rows
}

func (r Receiver) Resolve(id x.Identity, d x.Deadline) (x.ReceiverOutcome, error) {
	row, err := r.Rows(id, d)
	if err != nil {
		return x.ReceiverOutcome{}, err
	}
	if row.ReceiverOutcome != nil {
		return *row.ReceiverOutcome, nil
	}
	if row.Grant == nil {
		// The tracker closes its reservation gate (watch fired) before asking.
		// With no durable grant no receiver can ever enter the primitive, so
		// not-entered is proven by the reservation owner itself.
		// origin: hot-update S7 plan §4 / T12
		return x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled}, nil
	}
	return x.ReceiverOutcome{}, fmt.Errorf("%s: the granted receiver has not reported an outcome", x.CodeCommitInflight)
}

// Objects retires the exact original object version after the finalizer has
// confirmed both history records and the receiver's settled outcome. It
// publishes the plan's finalize-only tombstone fence (metadata CAS against the
// stream's last sequence) before purging only that NUID: any attempt a writer
// sent earlier carries an older stream CAS and can no longer land, so the
// fence — not a writer's word — is what retires late publishes. An original
// sending agent is told about the receipt first so it releases its source and
// writer; its silence does not keep the object alive forever.
// origin: hot-update S7 plan §2.3 / §5.3 / T04 / T21
type Objects struct {
	Retirement *RetirementJournal
	JS         func() jetstream.JetStream
	Rows       Rows
	Link       *Link
	UTC        func() time.Time
}

func (o Objects) Retire(id x.Identity, proof x.RetirementProof, d x.Deadline) (x.CleanupReceipt, error) {
	row, err := o.Rows(id, d)
	if err != nil {
		return x.CleanupReceipt{}, err
	}
	if row.CanonicalTerminal == nil || !proof.ReceiverRetired || proof.Identity != id || proof.EntryNonce != id.EntryNonce {
		return x.CleanupReceipt{}, fmt.Errorf("%s: retirement requires a decided, receiver-retired entry", x.CodeReconcileRequired)
	}
	receipt := x.CleanupReceipt{Identity: id, OwnerGeneration: proof.OwnerGeneration, WriterRetired: true, ConsumersRetired: true,
		TmpUnlinked: true, DirectorySynced: true}
	// The original agent learns the exact receipt when it owns no courier: a
	// pull sender, or a push preparation that never received its grant. A
	// granted push receiver settles through its own courier; delivering there
	// would wait on the very courier whose reply this cleanup precedes.
	agentAcked := false
	if terminal, ok := rowReceipt(row); ok && o.Link != nil && (row.Intent.Verb == "pull" || row.Grant == nil) {
		short := shortDeadline(d)
		status, _, linkErr := o.Link.DeliverReceipt(id, row.Origin, terminal, short)
		agentAcked = linkErr == nil && status.Identity == id && status.Origin == row.Origin &&
			status.Phase == x.PhaseSettled && !status.CleanupPending && status.Terminal != nil && *status.Terminal == terminal
	}
	// An immutable inline copy or finalized OBJ proves no more byte writes,
	// but neither releases the sender's source, replay bytes or registry slot.
	// Keep the exact decision in cleanup_pending so the finalizer retries this
	// same receipt after reconnect; a lost delivery or ACK is never retirement.
	// origin: S7 internal review FIX-TIERA2 F5 / hot-update S7 plan §1.2 / §4 / §10.4 / T46
	// Explicit audited abandonment releases the receiver duty under LQ1,
	// without claiming a physical sender ACK; OBJ fences remain mandatory below.
	// origin: S7 internal review PULL-B F10 / hot-update S7 plan lead LQ1 / §6.4
	if row.Intent.Verb == "pull" && !row.OutcomeUnknown && !agentAcked {
		return x.CleanupReceipt{}, errors.New(x.CodeReconcileRequired + ": original pull sender has not acknowledged retirement")
	}
	if err := writerRetired(row, agentAcked); err != nil {
		return x.CleanupReceipt{}, err
	}
	if id.ObjectVersion == "" {
		digest := sha256.Sum256([]byte("xfer-inline-cleanup\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
		receipt.Digest = hex.EncodeToString(digest[:])
		return receipt, nil
	}
	js := o.JS()
	if js == nil {
		return x.CleanupReceipt{}, fmt.Errorf("%s: JetStream unavailable", x.CodeOwnerRecovering)
	}
	if o.Retirement == nil {
		return x.CleanupReceipt{}, errors.New(x.CodeReconcileRequired + ": original stream incarnation is not retained")
	}
	state, err := o.Retirement.Load(id)
	if err != nil {
		return x.CleanupReceipt{}, err
	}
	if seal := row.SourceSeal; seal != nil {
		state.ObjectSpec.Size, state.ObjectSpec.SHA256, state.ObjectSpec.ChunkSize, state.ObjectSpec.Chunks = seal.Size, seal.SHA256, seal.ChunkSize, seal.Chunks
	}
	// Retained producer expectations survive an ACK loss and a broker cut.
	// The tombstone ACK must advance past every original possible publish.
	// origin: hot-update S7 plan §2.3 / FIX-TERM request 3
	state.RetirementFence.ExpectedStreamSeq = max(state.RetirementFence.ExpectedStreamSeq, row.Cleanup.WriterFence.ExpectedStreamSeq)
	state.RetirementFence.PreviousMetadataSeq = max(state.RetirementFence.PreviousMetadataSeq, row.Cleanup.WriterFence.PreviousMetadataSeq)
	state.Phase, state.RetirementFence.WriterRetired = x.WriteRetiring, true
	proof.WriterRetired, proof.ConsumersRetired = true, true
	fenced, err := jsstream.RetireTransferObjectRecorded(js, &state, proof, d, o.Retirement.Checkpoint)
	if err != nil {
		return x.CleanupReceipt{}, err
	}
	if err := o.Retirement.retainDecision(row); err != nil {
		return x.CleanupReceipt{}, err
	}
	fenced.TmpUnlinked, fenced.DirectorySynced = true, true
	return fenced, nil
}

// retiredSpec pins the incarnation at preparation, before any byte owner can
// publish. Cleanup loads that checkpoint and never re-derives the incarnation.
func retiredSpec(js jetstream.JetStream, row x.LedgerRow, d x.Deadline) (x.ObjectSpec, error) {
	remaining, err := controlbudget.Remaining(d)
	if err != nil {
		return x.ObjectSpec{}, err
	}
	// ctx-none: ctx-free retirer port; this lookup is bounded by its deadline.
	ctx, cancel := context.WithTimeout(context.Background(), min(remaining, 5*time.Second))
	defer cancel()
	bucket := proto.XferBucketName(row.Identity.SID)
	stream, err := js.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		return x.ObjectSpec{}, err
	}
	info := stream.CachedInfo()
	size, sha, chunk := row.Intent.Size, row.Intent.SHA256, row.Intent.ChunkSize
	if seal := row.SourceSeal; seal != nil {
		size, sha, chunk = seal.Size, seal.SHA256, seal.ChunkSize
	}
	if sha == "" {
		// No seal and no intent digest: the sender never sealed, so it never
		// wrote metadata. The empty-object digest only names the fence.
		empty := sha256.Sum256(nil)
		sha, size = hex.EncodeToString(empty[:]), 0
	}
	chunks := uint32((size + int64(chunk) - 1) / max(int64(chunk), 1))
	return x.ObjectSpec{Identity: row.Identity, Bucket: bucket, Stream: info.Config.Name, StreamCreated: info.Created.UTC().Format(time.RFC3339Nano),
		SHA256: sha, Size: size, ChunkSize: chunk, Chunks: chunks}, nil
}

func rowReceipt(row x.LedgerRow) (x.TerminalReceipt, bool) {
	rec := row.CanonicalTerminal
	if rec == nil {
		return x.TerminalReceipt{}, false
	}
	for _, r := range row.HistoryReceipts {
		if r.RecordDigest == rec.Digest {
			out := x.TerminalReceipt{TerminalID: terminalID(row.Identity), Kind: rec.Kind, CanonicalDigest: rec.Digest, ReqID: rec.ReqID,
				HistoryStream: r.Stream, HistorySeq: r.Sequence, OutcomeUnknown: row.OutcomeUnknown}
			if o := row.ReceiverOutcome; o != nil {
				out.Bytes, out.SHA256, out.Code = o.Bytes, o.SHA256, o.Code
			}
			return out, true
		}
	}
	return x.TerminalReceipt{}, false
}

func terminalID(id x.Identity) string {
	digest := sha256.Sum256([]byte("xfer-terminal-v1\n" + id.SID + "\n" + id.TransferID + "\n" + id.EntryNonce))
	return hex.EncodeToString(digest[:])
}

func shortDeadline(d x.Deadline) x.Deadline {
	short, err := controlbudget.After(2 * time.Second)
	if err != nil || short > d {
		return d
	}
	return short
}

// writerRetired is the positive evidence that no original writer can publish
// another attempt, required before the fence and purge (plan §2.3). It is one
// of: the original ctl's authenticated, serialized Stop witness; the immutable
// inline/no-OBJ disposition; the receiver landed a complete object (its writer finalized metadata and
// a correct writer appends nothing after that); the original sending agent
// stopped its writer on the exact receipt. Elapsed time proves neither that
// a writer joined nor that its earlier requests left the network.
// Otherwise the entry is retained and retried — a failed push whose ctl may
// still be uploading is not purged under it.
// origin: hot-update S7 plan §2.3 / §5.3 / T04 / T21
func writerRetired(row x.LedgerRow, agentAcked bool) error {
	// Ctl witnesses reach this bit through the authenticated serializer. Tier A's
	// immutable no-object disposition has no OBJ publisher to join or fence.
	// origin: hot-update S7 plan §2.3 / FIX-TERM request 3
	if row.Cleanup.WriterFence.WriterRetired || row.Intent.Tier == "a" && row.Identity.ObjectVersion == "" {
		return nil
	}
	if row.CanonicalTerminal != nil && row.CanonicalTerminal.Kind == "complete" {
		return nil
	}
	if row.Intent.Verb == "pull" && agentAcked {
		return nil
	}
	return fmt.Errorf("%s: the original writer has not retired", x.CodeReconcileRequired)
}

// Owners answers census questions for the single local origin. A request for
// another origin is refused (unknown), so a GC or settlement barrier never
// treats a foreign broker's silence as an empty responsibility set.
// origin: hot-update S7 plan §5.3 / §6.4 / T29
type Owners struct {
	Retirement    *RetirementJournal
	Permits       *xferauth.PermitAuthority
	PermitRoot    string
	Tracker       func() x.TrackerManager
	CourierAbsent func(x.Identity, x.Origin, x.Deadline) (bool, error)
	UTC           func() time.Time
	Directory     *Directory
	Capsules      *CapsuleStore
	Rows          Rows
	PreviousRoots []string
	Origin        x.Origin
	Ledger        func() x.LedgerPort
	Settlement    func() SettlementView
	Remote        x.OwnerResolver // cluster: original-origin census RPC; nil in single mode
	Peers         func(x.Deadline) ([]x.Origin, error)
}

var _ xferauth.OriginalSettlementOwners = Owners{}

// SettlementView is one installed graph snapshot. Resolving its ports
// together prevents a cut from pairing an old tracker with a new finalizer.
// It borrows the graph: subsequent mutations retain that owner's own fences.
type SettlementView struct {
	Origin    x.Origin
	Tracker   x.TrackerPort
	Finalizer x.FinalizerPort
}

// SettlementOwner resolves mutation authority only at the exact original
// origin. A remote census is read authority; it cannot lend this broker's
// local tracker for Cancel or Abandon. Uninstalled or incomplete graphs refuse.
// origin: hot-update S7 plan §6.4 / §8.2 / S7 internal review s7-p9term-request F2
func (o Owners) SettlementOwner(origin x.Origin, d x.Deadline) (x.TrackerPort, x.FinalizerPort, error) {
	if _, err := controlbudget.Remaining(d); err != nil {
		return nil, nil, err
	}
	if origin == (x.Origin{}) || origin != o.Origin || len(o.PreviousRoots) != 0 || o.Settlement == nil {
		return nil, nil, errors.New(x.CodeOwnerRecovering + ": original settlement owner unavailable")
	}
	view := o.Settlement()
	if view.Origin != origin || view.Tracker == nil || view.Finalizer == nil {
		return nil, nil, errors.New(x.CodeOwnerRecovering + ": original settlement graph unavailable")
	}
	if _, ok := view.Tracker.(xferauth.TrackerAbandoner); !ok {
		return nil, nil, errors.New(x.CodeOwnerRecovering + ": original tracker cannot record abandonment")
	}
	if _, err := controlbudget.Remaining(d); err != nil {
		return nil, nil, err
	}
	return view.Tracker, view.Finalizer, nil
}

func (o Owners) Census(origin x.Origin, d x.Deadline) (x.LedgerCensus, error) {
	if len(o.PreviousRoots) != 0 {
		return x.LedgerCensus{}, errors.New(x.CodeOwnerRecovering + ": retained prior-origin roots require reconciliation")
	}
	if origin == (x.Origin{}) && o.Remote != nil {
		// The zero-origin aggregate needs every retained origin; a cluster
		// member cannot claim it from its local ledger alone. The broker-only
		// session census supplies canonical rows with credentials stripped.
		// origin: hot-update S7 plan §5.3 / T29 / INTEG-C collection wiring
		censuses, err := o.SessionCensus("", d)
		if err != nil {
			return x.LedgerCensus{}, err
		}
		aggregate := x.LedgerCensus{Origin: o.Origin, Ready: true, Complete: true}
		for _, census := range censuses {
			if !census.Ready || !census.Complete || census.Code != "" {
				return x.LedgerCensus{}, errors.New(x.CodeOwnerRecovering)
			}
			aggregate.Rows = append(aggregate.Rows, census.Rows...)
			aggregate.Entries = append(aggregate.Entries, census.Entries...)
		}
		public, _ := json.Marshal(aggregate.Entries)
		sum := sha256.Sum256(public)
		aggregate.Digest = hex.EncodeToString(sum[:])
		return aggregate, nil
	}
	if origin == (x.Origin{}) || origin == o.Origin {
		l := o.Ledger()
		if l == nil {
			return x.LedgerCensus{}, errors.New(x.CodeOwnerRecovering)
		}
		census, err := l.Scan(d)
		if err == nil && census.Origin == (x.Origin{}) {
			census.Origin = o.Origin
		}
		if err == nil && o.Directory != nil {
			err = o.Directory.protect(&census, o.Rows, d)
		}
		if err == nil && o.Capsules != nil {
			census, err = o.Capsules.Census(census)
		}
		return census, err
	}
	if o.Remote == nil {
		return x.LedgerCensus{}, fmt.Errorf("%s: origin %s is not retained here", x.CodeOwnerRecovering, origin.BrokerID)
	}
	return o.Remote.Census(origin, d)
}

// SessionCensus returns the census of every origin that may hold session
// responsibility: this broker's own, plus each peer the cluster reports. Any
// unreachable peer makes the whole answer an error (the barrier stays rc75).
func (o Owners) SessionCensus(sid string, d x.Deadline) ([]x.LedgerCensus, error) {
	own, err := o.Census(o.Origin, d)
	if err != nil {
		return nil, err
	}
	out := []x.LedgerCensus{own}
	if o.Peers == nil {
		if o.Remote != nil {
			return nil, errors.New(x.CodeOwnerRecovering + ": original-origin discovery unavailable")
		}
		return out, nil
	}
	peers, err := o.Peers(d)
	if o.Directory != nil {
		peers, err = o.Directory.Peers(o.Peers, d)
	}
	if err != nil {
		return nil, err
	}
	for _, peer := range peers {
		if peer == o.Origin {
			continue
		}
		var census x.LedgerCensus
		if o.Directory != nil {
			census, err = o.Directory.sessionCensus(peer, sid, d)
		} else {
			census, err = o.Census(peer, d)
		}
		if err != nil || census.Origin != peer || !census.Ready || !census.Complete || census.Code != "" {
			if err == nil {
				err = errors.New(x.CodeOwnerRecovering)
			}
			return nil, err
		}
		out = append(out, census)
	}
	return out, nil
}
