//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"

	"github.com/LinZiyang666/tether/test/transferharness"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Pull's sender may have finished its byte leg before the cut; its original
// ctl still owns the receiver and settlement. Credit only that pinned process
// and a successful primitive or directory Sync, never an inflight syscall or
// a same-ID replacement. Import evidence remains independently required.
// origin: S7 internal review FIX-TIERA2 F2 / hot-update S7 plan §10.2 / T46
func transferPullReceiverProgress(cell transferCell, before, row x.Observation, ctl transferharness.Receipt) bool {
	if cell.direction != "pull" || row.Leg != "ctl" || ctl.PID <= 0 || ctl.ProcessStart == 0 || ctl.BootID == "" ||
		row.PID != ctl.PID || row.ProcessStart != ctl.ProcessStart || row.BootID != ctl.BootID || row.Generation != 1 ||
		row.OriginalDeadline.Deadline <= 0 || row.OriginalDeadline.BootID != row.BootID ||
		row.BoottimeNS <= before.BoottimeNS || row.Outcome != x.OutcomeLanded || row.Errno != 0 || row.GrantNonce == "" ||
		(row.Primitive != x.PrimitiveRename && row.Primitive != x.PrimitiveLink) || row.DurableOffset <= 0 ||
		row.Tmp.ResourceID == "" || row.Parent.ResourceID == "" {
		return false
	}
	return row.Kind == x.ObservePrimitiveExit || row.Kind == x.ObserveDirectorySync && row.DirectorySynced && row.ParentSynced
}

// A full inline receiver cannot grow its byte offset after the pre-primitive
// cut. Its own landed primitive and directory settlement are durable progress.
// origin: S7 internal review FIX-TIERA2 F2 / hot-update S7 plan §10.2 / T46
func TestTransferPullContinuationCreditsOriginalReceiverProgress(t *testing.T) {
	cell := transferCell{"a", "pull", "js", "pre-primitive"}
	before := transferOracleObservation()
	before.Leg, before.ObjectVersion = "ctl", ""
	before.Kind, before.Outcome, before.DurableOffset = x.ObservePrimitiveEnter, x.OutcomeNotEntered, 100
	before.GrantNonce, before.Primitive = "grant", x.PrimitiveRename
	ctl := transferharness.Receipt{PID: before.PID, ProcessStart: before.ProcessStart, BootID: before.BootID}
	for _, kind := range []x.ObservationKind{x.ObservePrimitiveExit, x.ObserveDirectorySync} {
		row := before
		row.Kind, row.Outcome = kind, x.OutcomeLanded
		row.BoottimeNS++
		row.EventSeq++
		row.ParentSynced, row.DirectorySynced = true, true
		transferMust(t, validateTransferContinuation(cell, before, x.Observation{}, ctl, []x.Observation{row}))
		// A source-pinned cut belongs to the agent; the original ctl's
		// receiver can make progress without another sender byte or an import.
		source := before
		source.PID, source.Leg = 8, "sender"
		transferMust(t, validateTransferContinuation(cell, source, x.Observation{}, ctl, []x.Observation{row}))
		for name, mutate := range map[string]func(*x.Observation){
			"unentered":          func(o *x.Observation) { o.Outcome = x.OutcomeNotEntered },
			"inflight":           func(o *x.Observation) { o.Outcome = x.OutcomeInflight },
			"syscall-error":      func(o *x.Observation) { o.Errno = 5 },
			"no-grant":           func(o *x.Observation) { o.GrantNonce = "" },
			"no-primitive":       func(o *x.Observation) { o.Primitive = "" },
			"replacement":        func(o *x.Observation) { o.PID++ },
			"recycled-pid":       func(o *x.Observation) { o.ProcessStart++ },
			"foreign-boot":       func(o *x.Observation) { o.BootID = "other" },
			"foreign-generation": func(o *x.Observation) { o.Generation++ },
			"no-tmp":             func(o *x.Observation) { o.Tmp = x.ResourceRef{} },
			"no-parent":          func(o *x.Observation) { o.Parent = x.ResourceRef{} },
			"old-record":         func(o *x.Observation) { o.BoottimeNS = before.BoottimeNS },
			"no-deadline":        func(o *x.Observation) { o.OriginalDeadline.Deadline = 0 },
			"foreign-clock":      func(o *x.Observation) { o.OriginalDeadline.BootID = "other" },
		} {
			bad := row
			mutate(&bad)
			if validateTransferContinuation(cell, before, x.Observation{}, ctl, []x.Observation{bad}) == nil {
				t.Fatalf("%s: unproven receiver progress accepted", name)
			}
		}
		if kind == x.ObserveDirectorySync {
			row.ParentSynced, row.DirectorySynced = false, false
			if validateTransferContinuation(cell, before, x.Observation{}, ctl, []x.Observation{row}) == nil {
				t.Fatal("unconfirmed directory Sync counted as settlement progress")
			}
		}
	}
}

// origin: S7 internal review FIX-TIERA2 F2 / hot-update S7 plan §10.2 / T46
func TestTransferPullContinuationCreditsAcceptedSourceSeal(t *testing.T) {
	before := transferOracleObservation()
	before.Kind, before.Leg, before.ObjectVersion = x.ObserveAttempt, "sender", ""
	before.Attempt = x.Attempt{LogicalID: "xfer-source-" + before.EntryNonce, Sent: true, Class: "", Phase: "request"}
	row := before
	row.Source.ResourceID = "source"
	row.BoottimeNS++
	row.Attempt.Class = "ok"
	cell := transferCell{"a", "pull", "js", "inline-send-unconfirmed"}
	transferMust(t, validateTransferContinuation(cell, before, x.Observation{}, transferharness.Receipt{}, []x.Observation{row}))
	for name, mutate := range map[string]func(*x.Observation){
		"unconfirmed":    func(o *x.Observation) { o.Attempt.Class = "" },
		"reply-lost":     func(o *x.Observation) { o.Attempt.Class = "reply" },
		"unsent":         func(o *x.Observation) { o.Attempt.Sent = false },
		"foreign-call":   func(o *x.Observation) { o.Attempt.LogicalID += "other" },
		"foreign-source": func(o *x.Observation) { o.Source.ResourceID = "" },
	} {
		bad := row
		mutate(&bad)
		if validateTransferContinuation(cell, before, x.Observation{}, transferharness.Receipt{}, []x.Observation{bad}) == nil {
			t.Fatalf("%s: unconfirmed seal counted as progress", name)
		}
	}
	before.Attempt.Class = "ok"
	if validateTransferContinuation(cell, before, x.Observation{}, transferharness.Receipt{}, []x.Observation{row}) == nil {
		t.Fatal("replayed acknowledgement of an already sealed source counted as progress")
	}
}

func validateTransferContinuation(cell transferCell, before, owner x.Observation, ctl transferharness.Receipt, rows []x.Observation) error {
	var progressed, installed bool
	for _, row := range rows {
		if row.TransferID != before.TransferID || row.BoottimeNS <= before.BoottimeNS {
			continue
		}
		if row.EntryNonce != before.EntryNonce || row.ObjectVersion != before.ObjectVersion {
			return errors.New("continuation minted a new identity or object")
		}
		// Each process owns its own monotonic deadline and checkpoint. In a pull,
		// ctl keeps its receiver while the agent imports the original sender; a
		// broker cut imports a tracker while both endpoint processes stay alive.
		// origin: hot-update S7 plan §10.2 / T35 / T36
		// A courier's own import row (it names its attempt) carries no byte
		// checkpoint or deadline; the worker's projection row beside it does.
		// Each projected responsibility is compared with its own last pre-cut
		// row. Comparing every import with the cut process's latest row of any
		// kind failed real continuations: a courier row has no deadline, a
		// settled entry keeps no receiver checkpoint, and a receiver or sealed
		// sender derives its deadline from the seal after the entry did.
		// origin: S7 internal review CUT-CAPACITY F3 / hot-update S7 plan §1.2 / §8.2
		if row.PID == owner.PID && (row.Kind == x.ObserveImport || row.Kind == x.ObserveInstall || row.Kind == x.ObserveRollback) && row.Attempt.LogicalID == "" {
			class := transferDeadlineOwner(row)
			// A byte phase without its actor dropped the checkpoint outright.
			if class == "entry" && (row.Phase == x.PhaseSending || row.Phase == x.PhaseReceiving || row.Phase == x.PhaseCommitReserved) {
				return errors.New("image import lost the synced checkpoint")
			}
			ref := transferCutReference(rows, owner, class)
			if row.DurableOffset < ref.DurableOffset || row.Index < ref.Index || row.LastConsumedStreamSeq < ref.LastConsumedStreamSeq {
				return errors.New("image import lost the synced checkpoint")
			}
			// Atomic primitive rows carry the durable offset but no hash state.
			if row.DurableOffset == ref.DurableOffset && row.HashStateDigest != "" && ref.HashStateDigest != "" && row.HashStateDigest != ref.HashStateDigest {
				return errors.New("image import changed checkpoint hash")
			}
			if row.ProcessStart != owner.ProcessStart || row.BootID != owner.BootID || row.Generation <= owner.Generation || row.CutID == "" {
				return errors.New("image import lacks original process and fresh generation")
			}
			// A source seal tightens a pull sender's provisional ceiling from its
			// original start, so an owner's deadline may only stay or shorten.
			// Freeze hands an entry its actor's deadline, which the actor derived
			// from the same seal a little later; a settled entry may name either.
			// Any fresh deadline is later than both.
			// origin: S7 internal review CUT-CAPACITY F3 / hot-update S7 plan §1.2 / M11
			limit := ref.OriginalDeadline.Deadline
			for _, actor := range []string{"sender", "receiver"} {
				if adopted := transferCutReference(rows, owner, actor); class == "entry" && adopted.PID != 0 {
					limit = max(limit, adopted.OriginalDeadline.Deadline)
				}
			}
			if row.OriginalDeadline.BootID != row.BootID || row.OriginalDeadline.Deadline <= 0 || limit != 0 && row.OriginalDeadline.Deadline > limit {
				return errors.New("image import refreshed original owner deadline")
			}
			installed = true
		}
		if transferPullReceiverProgress(cell, before, row, ctl) {
			progressed = true
		}
		if row.PID != before.PID {
			continue
		}
		// Cross-host remaining time may only tighten an owner's deadline (the
		// client loses each request's elapsed time); a later one is a refresh.
		// origin: S7 FIX-CUT / hot-update S7 plan §1.2 / T33 / M11
		if row.OriginalDeadline.Deadline != 0 && transferSameDeadlineOwner(before, row) &&
			(row.OriginalDeadline.BootID != before.OriginalDeadline.BootID || row.OriginalDeadline.Deadline > before.OriginalDeadline.Deadline) {
			return errors.New("continuation refreshed original deadline")
		}
		if row.DurableOffset > before.DurableOffset || (row.HistorySeq > 0 && row.CanonicalDigest != "") || (row.Kind == x.ObserveRetirement && row.RetirementDigest != "") ||
			transferSenderProgress(before, row) {
			progressed = true
		}
	}
	needsImport := cell.layer != "js"
	if !progressed || needsImport && !installed {
		return errors.New("no current-owner installation and durable continuation")
	}
	return nil
}

// This pure view is compiled into the binary observation overlay too. It
// projects only the sealed actor state; it never calls Freeze or repairs IO.
func transferWorkerImageObservation(state x.WorkerState, entry x.WorkerEntry, deadline x.BootDeadline, event x.Observation) x.Observation {
	id := entry.Prepare.Identity
	row := event
	row.TransferID, row.EntryNonce, row.ObjectVersion = id.TransferID, id.EntryNonce, id.ObjectVersion
	if row.Phase == "" {
		row.Phase = entry.Status.Phase
	}
	row.OriginalDeadline = deadline
	row.CutID = fmt.Sprintf("worker.%d", event.Generation)
	for _, actor := range state.Receivers {
		if actor.Identity != id {
			continue
		}
		cp := actor.Checkpoint
		row.FetchedOffset, row.DurableOffset, row.Index, row.LastConsumedStreamSeq = cp.DurableOffset, cp.DurableOffset, cp.ChunkIndex, cp.LastConsumedStreamSeq
		row.Tmp, row.Parent = actor.Atomic.Tmp, actor.Atomic.Parent
		row.OriginalDeadline = actor.ByteDeadline
		row.HashStateDigest = fmt.Sprintf("%x", sha256.Sum256(cp.SHA256State))
	}
	for _, actor := range state.Senders {
		if actor.Identity == id {
			row.Source, row.Offset, row.OriginalDeadline = actor.Source, actor.Offset, actor.ByteDeadline
			// A sender's next index is its confirmed receipt count. Leaving it
			// zero made every nonempty imported sender look rolled back.
			// origin: S7 internal review CUT-CAPACITY F3
			row.Index = uint32(len(actor.Writer.Receipts))
		}
	}
	return row
}

// origin: S7 internal review CUT-CAPACITY F3
func TestTransferImportObservationKeepsSenderReceiptCheckpoint(t *testing.T) {
	id := x.Identity{TransferID: "transfer", EntryNonce: "entry", ObjectVersion: "version"}
	until := x.BootDeadline{BootID: "boot", Deadline: 3000}
	entry := x.WorkerEntry{Prepare: x.Prepare{Identity: id}, Status: x.Status{Phase: x.PhaseSending}}
	actor := x.SenderState{Identity: id, Offset: 65536, ByteDeadline: until,
		Writer: x.WriterState{Receipts: []x.ChunkReceipt{{Index: 0}, {Index: 1}}}}
	state := x.WorkerState{Senders: []x.SenderState{actor}}
	row := transferWorkerImageObservation(state, entry, until, x.Observation{Kind: x.ObserveImport, Generation: 2})
	if row.Index != 2 || row.Offset != actor.Offset || row.OriginalDeadline != until {
		t.Fatalf("import observation lost the sender's confirmed checkpoint: index=%d offset=%d deadline=%+v", row.Index, row.Offset, row.OriginalDeadline)
	}
}

// transferCutReference is the cut process's last pre-cut row of one
// responsibility that names a deadline. The cut point is the owner row: the
// latest row of that process and generation when the boundary was parked.
// origin: S7 internal review CUT-CAPACITY F3 / hot-update S7 plan §10.2
func transferCutReference(rows []x.Observation, owner x.Observation, class string) x.Observation {
	var ref x.Observation
	for _, o := range append([]x.Observation{owner}, rows...) {
		if o.PID == owner.PID && o.ProcessStart == owner.ProcessStart && o.Generation == owner.Generation && o.EventSeq <= owner.EventSeq &&
			o.TransferID == owner.TransferID && o.OriginalDeadline.Deadline != 0 && transferDeadlineOwner(o) == class && o.EventSeq >= ref.EventSeq {
			ref = o
		}
	}
	return ref
}

// The real agent emits a sender row, a courier attempt without a deadline and
// later a settled entry, each its own owner; every import is judged against
// its own responsibility's pre-cut row and a later deadline is still a refresh.
// origin: S7 internal review CUT-CAPACITY F3 / hot-update S7 plan §1.2 / §10.2 / M11
func TestTransferImportKeepsEachResponsibilitysOwnCheckpoint(t *testing.T) {
	entry := transferOracleObservation()
	entry.Leg, entry.Kind, entry.Phase, entry.EventSeq, entry.Generation = "agent", x.ObserveControl, x.PhasePrepared, 1, 1
	entry.OriginalDeadline = x.BootDeadline{BootID: entry.BootID, Deadline: 3000}
	entry.DurableOffset, entry.FetchedOffset, entry.Index, entry.LastConsumedStreamSeq, entry.HashStateDigest = 0, 0, 0, 0, ""
	entry.Tmp, entry.Parent, entry.FileSynced = x.ResourceRef{}, x.ResourceRef{}, false
	// The sender emits its leaf responsibility as "sender", while registry
	// controls use "agent". They are different deadline owners in one PID.
	sender := entry
	sender.Leg, sender.Kind, sender.Source.ResourceID, sender.EventSeq, sender.Index = "sender", x.ObserveAppend, "source", 2, 3
	sender.OriginalDeadline.Deadline, sender.BoottimeNS = 2990, entry.BoottimeNS+1
	if transferSameDeadlineOwner(sender, entry) {
		t.Fatal("sender and registry observations were treated as one deadline owner")
	}
	courier := entry
	courier.Kind, courier.EventSeq, courier.BoottimeNS = x.ObserveAttempt, 3, entry.BoottimeNS+2
	courier.OriginalDeadline, courier.Attempt = x.BootDeadline{}, x.Attempt{LogicalID: "terminal", Sent: true}
	pre := []x.Observation{entry, sender, courier}
	cell := transferCell{"b", "pull", "agent", "source-pinned"}
	importRow := func(of x.Observation) x.Observation {
		row := of
		row.Kind, row.Generation, row.EventSeq, row.BoottimeNS, row.CutID = x.ObserveImport, 2, 1, courier.BoottimeNS+10, "worker.2"
		return row
	}
	progress := sender
	progress.Kind, progress.Generation, progress.EventSeq, progress.BoottimeNS = x.ObserveResolve, 2, 2, courier.BoottimeNS+20
	progress.ActualACKSeq, progress.Index = 9, 3
	// The cut point is the courier row, the cut process's last pre-cut row; the
	// courier's own import row beside the projection names no checkpoint.
	settled := importRow(entry)
	settled.Phase = x.PhaseSettled
	// Freeze hands the entry its sender's deadline before the entry settles.
	adopted := settled
	adopted.OriginalDeadline = sender.OriginalDeadline
	refreshed := settled
	refreshed.OriginalDeadline.Deadline += 5
	if validateTransferContinuation(cell, sender, courier, transferharness.Receipt{}, append(append([]x.Observation(nil), pre...), refreshed, progress)) == nil {
		t.Fatal("settled entry import with a fresh deadline accepted")
	}
	// The source seal tightened the sender's provisional ceiling after the cut point.
	tightened := importRow(sender)
	tightened.OriginalDeadline.Deadline -= 1000
	for name, rows := range map[string][]x.Observation{
		"sender-with-courier": {importRow(sender), importRow(courier), progress},
		"sealed-tightened":    {tightened, progress},
		"settled-entry":       {settled, progress},
		"settled-adopted":     {adopted, progress},
	} {
		if err := validateTransferContinuation(cell, sender, courier, transferharness.Receipt{}, append(append([]x.Observation(nil), pre...), rows...)); err != nil {
			t.Fatalf("%s: real continuation refused: %v", name, err)
		}
	}
	for name, mutate := range map[string]func(*x.Observation){
		"refreshed":     func(o *x.Observation) { o.OriginalDeadline.Deadline++ },
		"foreign-clock": func(o *x.Observation) { o.OriginalDeadline.BootID = "foreign" },
		"unbound":       func(o *x.Observation) { o.OriginalDeadline.Deadline = 0 },
		"lost-index":    func(o *x.Observation) { o.Index-- },
		"dropped-actor": func(o *x.Observation) { o.Source, o.Phase = x.ResourceRef{}, x.PhaseSending },
		"stale-gen":     func(o *x.Observation) { o.Generation = 1 },
		"other-process": func(o *x.Observation) { o.ProcessStart++ },
	} {
		bad := importRow(sender)
		mutate(&bad)
		rows := append(append([]x.Observation(nil), pre...), bad, progress)
		if validateTransferContinuation(cell, sender, courier, transferharness.Receipt{}, rows) == nil {
			t.Fatalf("%s: unproven import accepted", name)
		}
	}
}
