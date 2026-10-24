package xferserve

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferauth"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfergc"
)

// WriterRetirementRequest consumes only the evidence extension of the original
// ctl finalize entry. Subject actor authentication comes from the NATS ACL;
// retained identity, not today's membership or node home, authorizes closure.
// A receiver capsule, body actor, replacement stream or elapsed deadline can
// never supply this original producer's closed-future-gate/join witness.
// origin: hot-update S7 plan §2.3 / FIX-TERM request 3
func WriterRetirementRequest(g Graph, gate func(x.Origin) bool, actor, sid, tid string, body []byte, d x.Deadline) (reply []byte, handled, silent bool) {
	if !proto.TransferWriterRetirementMarked(body) {
		return nil, false, false
	}
	refuse := func(code string) ([]byte, bool, bool) {
		return marshal(proto.TransferWriterRetirementResp{Code: code}), true, false
	}
	var req proto.TransferWriterRetirementReq
	if len(body) > 4096 || proto.DecodeTransferWire(body, &req) != nil || req.Version != x.Version || req.CallID == "" ||
		req.Identity.SID != sid || req.Identity.TransferID != tid || req.Identity.EntryNonce == "" || req.Identity.ObjectVersion == "" || req.Origin.Generation == 0 {
		return refuse(x.CodeIntentConflict)
	}
	if !gate(x.Origin(req.Origin)) {
		return nil, true, true
	}
	if _, err := controlbudget.Remaining(d); err != nil {
		return refuse(x.CodeBudgetExceeded)
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil || fp != req.Identity.ActorFP {
		return refuse(x.CodeNotOwner)
	}
	if g.Decisions == nil || g.Link == nil || g.Link.Retirement == nil {
		return refuse(x.CodeOwnerRecovering)
	}
	k := x.Key{SID: sid, TransferID: tid}
	row, err := g.Decisions.Read(k, d)
	if err != nil {
		return refuse(codeOf(err))
	}
	id, origin := x.Identity(req.Identity), x.Origin(req.Origin)
	if row.Identity != id || row.EntryNonce != id.EntryNonce || row.Origin != origin || row.Intent.Verb != "push" || row.Intent.Tier != "b" {
		return refuse(x.CodeNotOwner)
	}
	pinned, err := g.Link.Retirement.Load(id)
	if err != nil {
		return refuse(x.CodeObjectVersionMismatch)
	}
	w, spec := req.Writer, pinned.ObjectSpec
	if !w.WriterRetired || w.AttachmentGeneration == 0 || w.Bucket != spec.Bucket || w.Stream != spec.Stream || w.StreamCreated != spec.StreamCreated ||
		w.SHA256 != spec.SHA256 || w.Size != spec.Size || w.ChunkSize != spec.ChunkSize || w.Chunks != spec.Chunks ||
		w.PreviousMetadataSeq > w.ExpectedStreamSeq {
		return refuse(x.CodeObjectVersionMismatch)
	}
	producer, ok := g.Decisions.(interface {
		AcceptWriterRetirement(x.Key, string, uint64, x.WriterState, x.Deadline) error
	})
	if !ok {
		return refuse(x.CodeOwnerRecovering)
	}
	state := x.WriterState{ObjectSpec: spec, AttachmentGeneration: w.AttachmentGeneration, RetirementFence: x.RetirementFence{
		WriterRetired: true, ExpectedStreamSeq: w.ExpectedStreamSeq, PreviousMetadataSeq: w.PreviousMetadataSeq}}
	if err = producer.AcceptWriterRetirement(k, row.EntryNonce, row.Revision, state, d); err != nil {
		return refuse(codeOf(err))
	}
	updated, err := g.Decisions.Read(k, d)
	if err != nil || updated.Identity != id || updated.Origin != origin || !updated.Cleanup.WriterFence.WriterRetired ||
		updated.Cleanup.WriterFence.ExpectedStreamSeq < w.ExpectedStreamSeq || updated.Cleanup.WriterFence.PreviousMetadataSeq < w.PreviousMetadataSeq {
		return refuse(x.CodeReconcileRequired)
	}
	sum := sha256.Sum256(body)
	return marshal(proto.TransferWriterRetirementResp{CallID: req.CallID, Identity: req.Identity, Origin: req.Origin, Revision: updated.Revision,
		WitnessDigest: hex.EncodeToString(sum[:])}), true, false
}

// RetirementJournal pins the stream incarnation before the first byte owner
// is prepared. Its synced checkpoints outlive a broker image cut or crash;
// a fresh INFO during cleanup cannot silently adopt a recreated stream.
// Records contain public object evidence only, never a receiver credential.
// origin: hot-update S7 plan §2.3 / §5.3 / T04 / T21
type RetirementJournal struct {
	mu   sync.Mutex
	Root string
}

func (j *RetirementJournal) path(id x.Identity) string {
	body, _ := json.Marshal(id)
	sum := sha256.Sum256(body)
	return filepath.Join(j.Root, hex.EncodeToString(sum[:])+".json")
}

func (j *RetirementJournal) load(id x.Identity) (x.WriterState, error) {
	body, err := os.ReadFile(j.path(id))
	if err != nil {
		return x.WriterState{}, err
	}
	var state x.WriterState
	if json.Unmarshal(body, &state) != nil || state.ObjectSpec.Identity != id || state.ObjectSpec.StreamCreated == "" {
		return x.WriterState{}, errors.New(x.CodeObjectVersionMismatch + ": invalid retirement checkpoint")
	}
	return state, nil
}

// Pin is exclusive and idempotent. A replay always returns the original
// incarnation, even if the current stream now has the same name and bytes.
func (j *RetirementJournal) Pin(id x.Identity, inspect func() (x.ObjectSpec, error)) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if _, err := j.load(id); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	spec, err := inspect()
	if err != nil {
		return err
	}
	if spec.Identity != id || spec.StreamCreated == "" {
		return errors.New(x.CodeObjectVersionMismatch)
	}
	if err := os.MkdirAll(j.Root, 0700); err != nil {
		return err
	}
	return writeDurable(j.path(id), x.WriterState{ObjectSpec: spec, AttachmentGeneration: 1})
}

func (j *RetirementJournal) Load(id x.Identity) (x.WriterState, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.load(id)
}

func (j *RetirementJournal) Checkpoint(state x.WriterState) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	old, err := j.load(state.ObjectSpec.Identity)
	if err != nil {
		return err
	}
	if old.ObjectSpec.StreamCreated != state.ObjectSpec.StreamCreated || old.ObjectSpec.Stream != state.ObjectSpec.Stream ||
		old.RetirementFence.Purged && !state.RetirementFence.Purged ||
		old.RetirementFence.Receipt != nil && (state.RetirementFence.Receipt == nil || *old.RetirementFence.Receipt != *state.RetirementFence.Receipt) {
		return errors.New(x.CodeObjectVersionMismatch + ": retirement checkpoint regressed")
	}
	return writeDurable(j.path(state.ObjectSpec.Identity), state)
}

// Collection borrows the producer's exact mutex. A replacement checkpoint
// cannot land between the collector's content comparison and durable unlink.
// origin: hot-update S7 plan §5.3 / FIX-TERM request 4
func (j *RetirementJournal) ScanRetirementCheckpoints(limit int, d x.Deadline) ([]xfergc.RetirementCheckpoint, error) {
	journal, err := xfergc.NewRetirementCheckpointJournal(j.Root, &j.mu)
	if err != nil {
		return nil, err
	}
	return journal.ScanRetirementCheckpoints(limit, d)
}

func (j *RetirementJournal) ConsumeRetirementCheckpoint(checkpoint xfergc.RetirementCheckpoint, d x.Deadline) error {
	journal, err := xfergc.NewRetirementCheckpointJournal(j.Root, &j.mu)
	if err != nil {
		return err
	}
	if err := journal.ConsumeRetirementCheckpoint(checkpoint, d); err != nil {
		return err
	}
	// The public replay evidence has served its purpose. Serialize its cleanup
	// with any new producer; never remove evidence for a replaced checkpoint.
	for !j.mu.TryLock() {
		remaining, err := controlbudget.Remaining(d)
		if err != nil {
			return err
		}
		timer := time.NewTimer(min(remaining, 5*time.Millisecond))
		<-timer.C
	}
	defer j.mu.Unlock()
	if _, err := os.Stat(j.path(checkpoint.Identity)); !os.IsNotExist(err) {
		return err
	}
	if err := os.Remove(j.decisionPath(checkpoint.Identity)); err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	dir, err := os.Open(filepath.Dir(j.decisionPath(checkpoint.Identity)))
	if err != nil {
		return err
	}
	return errors.Join(dir.Sync(), dir.Close())
}

// A terminal reply can be lost after both ledger directories retire. Retain
// the public decision before that happens so a later bounded pass can ask the
// original agent to acknowledge and join its courier. No capsule is copied.
// origin: hot-update S7 plan §4 / §5.3 / FIX-TERM request 4
type retirementDecision struct {
	Origin   x.Origin
	Verb     string
	Decision proto.TransferDecision
}

func (j *RetirementJournal) decisionPath(id x.Identity) string {
	return filepath.Join(j.Root, "decisions", filepath.Base(j.path(id)))
}

func (j *RetirementJournal) retainDecision(row x.LedgerRow) error {
	receipt, ok := rowReceipt(row)
	if !ok || row.ReceiverOutcome == nil || row.CanonicalTerminal == nil {
		return errors.New(x.CodeReconcileRequired + ": retirement decision unavailable")
	}
	decision := retirementDecision{Origin: row.Origin, Verb: row.Intent.Verb, Decision: proto.TransferDecision{
		Identity: row.Identity, Outcome: *row.ReceiverOutcome, Canonical: *row.CanonicalTerminal, Receipt: &receipt}}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err := os.MkdirAll(filepath.Dir(j.decisionPath(row.Identity)), 0700); err != nil {
		return err
	}
	return writeDurable(j.decisionPath(row.Identity), decision)
}

func readRetirementEvidence(path string, target any) (resultErr error) {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { resultErr = errors.Join(resultErr, file.Close()) }()
	body, err := io.ReadAll(io.LimitReader(file, (64<<10)+1))
	if err != nil || len(body) > 64<<10 {
		return errors.Join(err, errors.New(x.CodeResultUnknown+": retirement evidence exceeds read bound"))
	}
	return decodeClosed(body, target)
}

// Only a same-instance agent's exact settled receipt proves that its courier
// stopped. Pull's receiver belongs to the original ctl; no agent reply proves
// that ctl's absence, so those checkpoints remain until that owner supplies it.
func (j *RetirementJournal) courierAbsent(link *Link, id x.Identity, origin x.Origin, d x.Deadline) (bool, error) {
	var retained retirementDecision
	if err := readRetirementEvidence(j.decisionPath(id), &retained); err != nil {
		return false, err
	}
	decision := retained.Decision
	if retained.Origin != origin || retained.Verb != "push" || decision.Identity != id || decision.Canonical.Identity != id ||
		decision.Receipt == nil || decision.Receipt.CanonicalDigest != decision.Canonical.Digest || decision.Receipt.ReqID != decision.Canonical.ReqID {
		return false, errors.New(x.CodeResultUnknown + ": original courier authority unavailable")
	}
	// Disk replay evidence is not a second publication authority. Authenticate
	// it against the exact existing physical history, without creating a stream
	// or publishing a replacement terminal when that history is unavailable.
	// origin: hot-update S7 plan §5.2 / §5.3 / FIX-TERM request 4
	history, err := (sessionHistory{js: link.JS}).Lookup(x.Key{SID: id.SID, TransferID: id.TransferID}, d)
	if err != nil || !history.Complete || len(history.Terminal) != 1 {
		return false, errors.Join(err, errors.New(x.CodeResultUnknown))
	}
	original := history.Terminal[0]
	// The local replay envelope cannot relabel a pull's ctl receiver as an
	// agent. Its physical canonical verb owns that responsibility.
	// origin: hot-update S7 plan §4 / §5.3 / FIX-TERM request 4
	var audit schema.AuditTransfer
	if json.Unmarshal(original.Bytes, &audit) != nil || audit.Verb != "push" {
		return false, xferauth.ErrDenied
	}
	if original.Identity != id || original.Kind != decision.Canonical.Kind || original.ReqID != decision.Canonical.ReqID ||
		original.Digest != decision.Canonical.Digest || !bytes.Equal(original.Bytes, decision.Canonical.Bytes) {
		return false, xferauth.ErrDenied
	}
	confirmed := false
	for _, receipt := range history.Receipts {
		confirmed = confirmed || receipt.Stream == decision.Receipt.HistoryStream && receipt.Sequence == decision.Receipt.HistorySeq && receipt.RecordDigest == original.Digest
	}
	if !confirmed {
		return false, xferauth.ErrDenied
	}
	wr := proto.TerminalReceipt(*decision.Receipt)
	result, legacy, _, err := link.send(id.SID, id.NID, proto.TransferLink{Op: proto.TransferLinkReceipt,
		CallID: "retirement-" + id.TransferID, Identity: proto.Identity(id), Origin: proto.Origin(origin), Receipt: &wr, Decision: &decision}, d)
	if err != nil {
		return false, err
	}
	status := result.Status
	if legacy || result.Code != "" || result.Version != x.Version || result.InstanceID != id.AgentInstance || status == nil ||
		x.Identity(status.Identity) != id || x.Origin(status.Origin) != origin || status.Phase != proto.Phase(x.PhaseSettled) ||
		status.State != proto.StatusState(x.StatusTerminal) || status.Code != "" || status.CleanupPending || status.Terminal == nil || x.TerminalReceipt(*status.Terminal) != *decision.Receipt {
		return false, errors.New(x.CodeResultUnknown + ": original courier has not joined")
	}
	return true, nil
}

// The journal contains only this original origin's checkpoints. Prove every
// absence independently; a missing graph, retained result, courier or permit
// is uncertainty, even when both durable directories happen to be empty.
// origin: hot-update S7 plan §5.3 / §6.1 / FIX-TERM request 4
func (o Owners) retirementResponsibility(id x.Identity, d x.Deadline) (xfergc.RetirementResponsibility, error) {
	evidence := xfergc.RetirementResponsibility{Identity: id}
	if _, err := controlbudget.Remaining(d); err != nil {
		return evidence, err
	}
	if len(o.PreviousRoots) != 0 || o.Permits == nil || o.Tracker == nil || o.CourierAbsent == nil || o.Ledger == nil || o.UTC == nil {
		return evidence, errors.New(x.CodeOwnerRecovering)
	}
	var permit x.CreationPermit
	sum := sha256.Sum256([]byte(id.SID + "\n" + id.TransferID))
	if err := readRetirementEvidence(filepath.Join(o.PermitRoot, hex.EncodeToString(sum[:])+".json"), &permit); err != nil {
		return evidence, err
	}
	if permit.Identity != id || permit.Origin != o.Origin || o.Permits.Verify(permit) != nil {
		return evidence, xferauth.ErrDenied
	}
	if o.Directory != nil {
		retained, err := o.Directory.lookup(id.SID, id.TransferID, d)
		if err != nil || retained == nil || retained.Permit != permit {
			return evidence, errors.Join(err, xferauth.ErrDenied)
		}
	}
	evidence.PermitExpired = !o.UTC().Before(permit.CreateUntil)
	ledger, tracker := o.Ledger(), o.Tracker()
	if ledger == nil || tracker == nil {
		return evidence, errors.New(x.CodeOwnerRecovering)
	}
	census, err := ledger.Scan(d)
	if err != nil || !census.Ready || !census.Complete || census.Code != "" || census.Origin != (x.Origin{}) && census.Origin != o.Origin {
		return evidence, errors.Join(err, errors.New(x.CodeOwnerRecovering))
	}
	for _, row := range census.Rows {
		if row.Identity.SID == id.SID && row.Identity.TransferID == id.TransferID {
			return evidence, errors.New(x.CodeReconcileRequired)
		}
	}
	for _, held := range census.Entries {
		if held.Identity.SID == id.SID && held.Identity.TransferID == id.TransferID {
			return evidence, errors.New(x.CodeReconcileRequired)
		}
	}
	evidence.PrimaryAbsent, evidence.OutboxAbsent = true, true
	status, err := tracker.Status(x.StatusRequest{Identity: id, Origin: o.Origin}, d)
	if err != nil || status.Code != "" || status.State != x.StatusUnknown || status.Identity != id || status.Origin != o.Origin {
		return evidence, errors.Join(err, errors.New(x.CodeResultUnknown))
	}
	evidence.ResultExpired = true
	if !evidence.PermitExpired {
		return evidence, errors.New(x.CodePermitExpired + ": creation authority remains live")
	}
	evidence.CourierAbsent, err = o.CourierAbsent(id, o.Origin, d)
	if _, deadlineErr := controlbudget.Remaining(d); deadlineErr != nil {
		return evidence, errors.Join(err, deadlineErr)
	}
	evidence.Authoritative = err == nil && evidence.CourierAbsent
	return evidence, err
}

// Collector.Pass owns this synchronous work, so its Stop/cut join covers the
// producer lock, original-owner lookup and courier reply under one deadline.
func (o Owners) CollectRetirementCheckpoints(d x.Deadline) error {
	if o.Retirement == nil {
		return errors.New(x.CodeOwnerRecovering + ": retirement journal unavailable")
	}
	return xfergc.CollectRetirementCheckpoints(o.Retirement, o.retirementResponsibility, d)
}
