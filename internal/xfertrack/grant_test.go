package xfertrack

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/transferpath"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"golang.org/x/sys/unix"
)

// origin: hot-update S7 plan §4 / T12 / M06
func TestTransferReceiverCannotLandAfterBrokerFailure(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	t.Run("tracker", func(t *testing.T) {
		for _, tier := range []string{"a", "b"} {
			for _, verb := range []string{"push", "pull"} {
				for _, first := range []string{"cancel", "grant"} {
					t.Run(tier+"/"+verb+"/"+first, func(t *testing.T) {
						f := newFixture(t, tier, verb)
						f.admit(t)
						dir := t.TempDir()
						dst := filepath.Join(dir, "destination")
						tmp := filepath.Join(dir, "temporary")
						if err := os.WriteFile(dst, []byte("old\n"), 0600); err != nil {
							t.Fatal(err)
						}
						if err := os.WriteFile(tmp, []byte("new\n"), 0600); err != nil {
							t.Fatal(err)
						}
						closed := false
						primitiveEntries := 0
						// The oracle performs a real rename only with a returned Grant. The
						// receiver's own gate implementation is a separate integration branch.
						land := func(g x.CommitGrant) {
							if closed {
								return
							}
							if g.GrantNonce == "" {
								t.Fatal("empty grant entered primitive")
							}
							primitiveEntries++
							if err := os.Rename(tmp, dst); err != nil {
								t.Fatal(err)
							}
						}
						if first == "cancel" {
							f.setResolver(t, resolverFunc(func(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
								closed = true
								return x.ReceiverOutcome{Outcome: x.OutcomeNotEntered, Code: x.CodeCanceled}, nil
							}))
							s, err := f.owner.Cancel(f.permit.Identity, x.CancelRequest{Origin: f.permit.Origin, Reason: x.CancelUser}, f.clock.deadline())
							if err != nil || s.State != x.StatusTerminal || s.Terminal.Kind != "failed" {
								t.Fatalf("closed-gate failure: %+v %v", s, err)
							}
							late, err := f.owner.Reserve(f.permit.Identity, x.ReserveRequest{Origin: f.permit.Origin, ReceiverInstance: "receiver"}, f.clock.deadline())
							requireCode(t, err, x.CodeBudgetExceeded)
							if late != (x.CommitGrant{}) {
								t.Fatal("failure returned commit authority")
							}
							if primitiveEntries != 0 {
								t.Fatal("failed transfer entered primitive")
							}
							data, _ := os.ReadFile(dst)
							if string(data) != "old\n" {
								t.Fatal("failed transfer changed destination")
							}
						} else {
							grant := f.reserve(t)
							// Lose contact before the broker sees the primitive outcome. A
							// watchdog must not manufacture the receiver's closed-gate proof.
							f.clock.advance(time.Duration(f.ledger.row(f.permit.Identity).BudgetSeal.EffectiveBudgetNS) + time.Second)
							f.owner.watchdogPass(f.clock.deadline())
							row := f.ledger.row(f.permit.Identity)
							if row.CanonicalTerminal != nil || row.Grant == nil || *row.Grant != grant {
								t.Fatal("watchdog failed a reserved receiver")
							}
							land(grant)
							receipt, err := f.owner.Finish(f.permit.Identity, f.outcome(grant), f.clock.deadline())
							if err != nil || receipt.Kind != "complete" {
								t.Fatalf("landed outcome: %+v %v", receipt, err)
							}
							if primitiveEntries != 1 {
								t.Fatalf("primitive entries = %d", primitiveEntries)
							}
							data, _ := os.ReadFile(dst)
							if string(data) != "new\n" {
								t.Fatal("complete without real effect")
							}
							if f.history.count() != 2 {
								t.Fatalf("history records = %d, want one START and one TERMINAL", f.history.count())
							}
						}
					})
				}
			}
		}
	})
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint("receiver_integration-", round), func(t *testing.T) { exerciseReceiverDependency(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "tracker receiver integration", g)
	testharness.AssertNoFDLeak(t, "tracker receiver integration", fd, 0)
}

// The real receiver and atomic importer own the commit gate and syscall.
// This fixture supplies bytes and observes the effect, never a substitute gate.
func exerciseReceiverDependency(t *testing.T) {
	for _, tier := range []string{"a", "b"} {
		for _, verb := range []string{"push", "pull"} {
			for _, first := range []string{"cancel", "reserved_cancel", "grant"} {
				t.Run(tier+"/"+verb+"/"+first, func(t *testing.T) {
					f := newFixture(t, tier, verb)
					f.clock.ns, _ = controlbudget.Now()
					if tier == "a" {
						f.permit.Identity.ObjectVersion = ""
					}
					f.intent.SHA256 = digest([]byte("new\n"))
					f.admit(t)
					receiver, target, err := receiverFixture(t, f)
					if err != nil {
						t.Fatalf("construct real atomic/receiver owners: %v", err)
					}
					t.Cleanup(func() {
						if err := receiver.Stop(realAtomicDeadline(t)); err != nil {
							t.Error(err)
						}
					})
					var grant x.CommitGrant
					// Retain both orderings: cancellation before any Grant and a
					// stopped receiver replaying its already installed Grant.
					// origin: hot-update S7 plan §4 / T12 / INTEG-R fixture reconciliation
					if first == "reserved_cancel" {
						grant = f.reserve(t)
						if err := receiver.Reserve(grant); err != nil {
							t.Fatal(err)
						}
					}
					if first != "grant" {
						if err := receiver.Stop(realAtomicDeadline(t)); err != nil {
							t.Fatal(err)
						}
						if first == "cancel" {
							grant = x.CommitGrant{Identity: f.permit.Identity, GrantNonce: "late", ReceiverInstance: "receiver", TerminalID: terminalID(f.permit.Identity)}
						}
						if err := receiver.Reserve(grant); err == nil {
							t.Fatal("stopped receiver accepted late Grant")
						}
					} else {
						grant = f.reserve(t)
						if err := receiver.Reserve(grant); err != nil {
							t.Fatal(err)
						}
						for step := 0; step < 4; step++ {
							if err := receiver.Step(realAtomicDeadline(t)); err != nil && !errors.Is(err, io.EOF) {
								t.Fatal(err)
							}
						}
					}
					outcome, err := xferreceive.Outcome(receiver)
					if err != nil {
						t.Fatal(err)
					}
					if first != "grant" && outcome.Outcome != x.OutcomeNotEntered {
						t.Fatal("cancel failed to close primitive gate")
					}
					if first == "grant" && outcome.Outcome != x.OutcomeLanded {
						t.Fatal("granted receiver did not land")
					}
					receipt, err := f.owner.Finish(f.permit.Identity, outcome, f.clock.deadline())
					if first != "grant" {
						if late, lateErr := f.owner.Reserve(f.permit.Identity, x.ReserveRequest{Origin: f.permit.Origin, ReceiverInstance: "receiver"}, f.clock.deadline()); lateErr == nil || late != (x.CommitGrant{}) {
							t.Fatal("failed receiver regained broker commit authority")
						}
					}
					if err != nil {
						t.Fatal(err)
					}
					want := "complete"
					if first != "grant" {
						want = "failed"
					}
					data, err := os.ReadFile(target)
					if err != nil {
						t.Fatal(err)
					}
					expected := "new\n"
					if first != "grant" {
						expected = "old\n"
					}
					if string(data) != expected {
						t.Fatalf("destination = %q, want %q", data, expected)
					}
					if receipt.Kind != want {
						t.Fatalf("terminal = %s, want %s", receipt.Kind, want)
					}
				})
			}
		}
	}
}

// origin: hot-update S7 plan §4 / T13 / M06
func TestTransferReservationSurvivesLostReplyAndExpiry(t *testing.T) {
	t.Run("tracker", func(t *testing.T) {
		for _, tier := range []string{"a", "b"} {
			for _, verb := range []string{"push", "pull"} {
				t.Run(tier+"/"+verb, func(t *testing.T) {
					f := newFixture(t, tier, verb)
					f.admit(t)
					f.ledger.setFailure(x.MutationReserve)
					denied, err := f.owner.Reserve(f.permit.Identity, x.ReserveRequest{Origin: f.permit.Origin, ReceiverInstance: "receiver"}, f.clock.deadline())
					if err == nil || denied != (x.CommitGrant{}) {
						t.Fatal("unsynced Grant escaped")
					}
					retained := f.pendingGrant(t)
					f.ledger.setFailure("")
					grant, err := f.owner.Reserve(f.permit.Identity, x.ReserveRequest{Origin: f.permit.Origin, ReceiverInstance: "receiver"}, f.clock.deadline())
					if grant.TerminalID != "75a6c57ce6d6567607847c04da145dc4990586f1f6b26b738e8fb2a1cc14edab" {
						t.Fatal("terminal slot derivation changed")
					}
					if err != nil || grant != retained {
						t.Fatalf("grant changed across durability retry: %v", err)
					}
					durable := f.ledger.row(f.permit.Identity)
					if durable.Grant == nil || *durable.Grant != grant {
						t.Fatal("reply preceded durable Grant")
					}
					// The caller discards that successful reply. Deadline, permit expiry
					// and result-cache TTL all pass before the original receiver returns.
					f.clock.advance(time.Duration(durable.BudgetSeal.EffectiveBudgetNS) + 2*xferRecentlyReapedTTL)
					f.owner.watchdogPass(f.clock.deadline())
					s, err := f.owner.Cancel(f.permit.Identity, x.CancelRequest{Origin: f.permit.Origin, Reason: x.CancelRevoked}, f.clock.deadline())
					if err == nil || s.State != x.StatusLive {
						t.Fatalf("unreachable revoke = %+v %v", s, err)
					}
					again, err := f.owner.Reserve(f.permit.Identity, x.ReserveRequest{Origin: f.permit.Origin, ReceiverInstance: "receiver"}, f.clock.deadline())
					if err != nil || again != grant {
						t.Fatalf("lost reply renewed/dropped Grant: %v", err)
					}
					if row := f.ledger.row(f.permit.Identity); row.CanonicalTerminal != nil {
						t.Fatal("TTL synthesized failure for unresolved Grant")
					}
					frozen, err := f.owner.Freeze(f.clock.deadline())
					if err != nil {
						t.Fatal(err)
					}
					image, _, err := frozen.Export()
					if err != nil {
						t.Fatal(err)
					}
					restored, err := Import(f.owner.cfg, image)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = restored.Stop(f.clock.deadline()) })
					if err = restored.Bind(Bindings{Ledger: f.ledger, OwnerGeneration: 2}, f.clock.deadline()); err != nil {
						t.Fatal(err)
					}
					receipt, err := restored.Finish(f.permit.Identity, f.outcome(grant), f.clock.deadline())
					if err != nil || receipt.Kind != "complete" {
						t.Fatalf("late receiver = %+v %v", receipt, err)
					}
					if receipt.TerminalID != grant.TerminalID || receipt.HistorySeq == 0 || f.history.count() != 2 {
						t.Fatal("wrong terminal slot/history proof")
					}
				})
			}
		}
	})
	t.Run("durable_ledger_integration", func(t *testing.T) {
		f := newFixture(t, "b", "push")
		cfg := x.LedgerConfig{Base: f.owner.cfg.Base, Root: t.TempDir(), FallbackRoot: t.TempDir()}
		ledger, err := xferledger.New(cfg)
		if err != nil {
			t.Fatalf("construct durable ledger owner: %v", err)
		}
		f.owner.cfg.Ledger = ledger
		f.admit(t)
		grant := f.reserve(t)
		before, err := ledger.Scan(f.clock.deadline())
		if err != nil || len(before.Rows) != 1 {
			t.Fatalf("reserve census: %+v %v", before, err)
		}
		f.clock.advance(2*xferRecentlyReapedTTL + time.Duration(before.Rows[0].BudgetSeal.EffectiveBudgetNS))
		reopened, err := xferledger.New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		census, err := reopened.Scan(f.clock.deadline())
		if err != nil {
			t.Fatal(err)
		}
		if !census.Ready || !census.Complete || len(census.Rows) != 1 {
			t.Fatal("durable census lost reserved slot")
		}
		row := census.Rows[0]
		if row.Grant == nil || *row.Grant != grant || row.CanonicalTerminal != nil {
			t.Fatal("lost reply/expiry changed durable Grant")
		}
	})
}

// origin: hot-update S7 plan §4 / T12
func TestTransferLandedOutcomeRetainsDurabilityResponsibility(t *testing.T) {
	f := newFixture(t, "a", "push")
	f.admit(t)
	grant := f.reserve(t)
	out := f.outcome(grant)
	out.ParentSynced = false
	out.TmpCleanupPending = true
	receipt, err := f.owner.Finish(f.permit.Identity, out, f.clock.deadline())
	requireCode(t, err, x.CodeDurabilityUnknown)
	if receipt.HistorySeq != 0 {
		t.Fatal("unknown durability was acknowledged")
	}
	f.setResolver(t, resolverFunc(func(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
		return x.ReceiverOutcome{Grant: grant, Outcome: x.OutcomeNotEntered}, nil
	}))
	_, err = f.owner.Cancel(f.permit.Identity, x.CancelRequest{Origin: f.permit.Origin, Reason: x.CancelDeadline}, f.clock.deadline())
	requireCode(t, err, x.CodeTerminalConflict)
	before := f.ledger.row(f.permit.Identity)
	if before.CanonicalTerminal != nil || before.ReceiverOutcome.Outcome != x.OutcomeLanded {
		t.Fatal("landed outcome regressed")
	}
	out.ParentSynced = true
	receipt, err = f.owner.Finish(f.permit.Identity, out, f.clock.deadline())
	if err != nil || receipt.Kind != "complete" {
		t.Fatalf("directory sync settlement: %+v %v", receipt, err)
	}
	original := f.ledger.row(f.permit.Identity)
	if _, err = f.owner.Finish(f.permit.Identity, out, f.clock.deadline()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, f.ledger.row(f.permit.Identity)) {
		t.Fatal("replay changed immutable decision")
	}
	bad := out
	bad.Grant.GrantNonce = "late-old-grant"
	_, err = f.owner.Finish(f.permit.Identity, bad, f.clock.deadline())
	requireCode(t, err, x.CodeTerminalConflict)
	f.setResolver(t, resolverFunc(func(x.Identity, x.Deadline) (x.ReceiverOutcome, error) {
		return x.ReceiverOutcome{}, errors.New("offline")
	}))
}

// Real original file descriptions feed the P05 importer. This fixture owns
// only the input chunk; Atomic.Commit and the receiver gate stay production.
func receiverFixture(t *testing.T, f *fixture) (x.Receiver, string, error) {
	t.Helper()
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("old\n"), 0600); err != nil {
		return nil, "", err
	}
	tmp, err := os.OpenFile(filepath.Join(dir, "temporary"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, "", err
	}
	t.Cleanup(func() { _ = tmp.Close() })
	parent, err := os.Open(dir)
	if err != nil {
		return nil, "", err
	}
	t.Cleanup(func() { _ = parent.Close() })
	tmpID, err := fileIdentity(tmp)
	if err != nil {
		return nil, "", err
	}
	parentID, err := fileIdentity(parent)
	if err != nil {
		return nil, "", err
	}
	state := x.AtomicState{Phase: x.PhaseReceiving, Tmp: x.ResourceRef{ResourceID: "tmp", Name: x.TmpFileName, Kind: x.ResourceTmp},
		Parent: x.ResourceRef{ResourceID: "parent", Name: x.ParentFileName, Kind: x.ResourceParent}, TmpIdentity: tmpID, ParentIdentity: parentID,
		TmpName: "temporary", Destination: "target", Force: true}
	atomic, _, err := transferpath.ImportAtomic(state, &filesFixture{files: map[string]*os.File{"tmp/tmp": tmp, "parent/parent": parent}})
	if err != nil {
		return nil, "", err
	}
	t.Cleanup(func() { _ = atomic.Stop(realAtomicDeadline(t)) })
	if err := transferpath.ThawAtomic(atomic, realAtomicDeadline(t)); err != nil {
		return nil, "", err
	}
	base := f.owner.cfg.Base
	base.Clock = func() (string, int64, error) { ns, err := controlbudget.Now(); return "boot", ns, err }
	receiver, err := xferreceive.New(x.ReceiveConfig{Base: base, Identity: f.permit.Identity,
		Budget: f.ledger.row(f.permit.Identity).BudgetSeal, Atomic: atomic, Reader: &chunkFixture{data: []byte("new\n"), inline: f.intent.Tier == "a"}})
	return receiver, target, err
}
func fileIdentity(file *os.File) (x.FileIdentity, error) {
	var st unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &st); err != nil {
		return x.FileIdentity{}, err
	}
	flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFL, 0)
	return x.FileIdentity{Dev: uint64(st.Dev), Ino: st.Ino, Mode: uint32(st.Mode), Flags: int32(flags)}, err
}

type filesFixture struct{ files map[string]*os.File }

func (f *filesFixture) TakeFile(id, name string) (*os.File, error) {
	key := id + "/" + name
	file := f.files[key]
	if file == nil {
		return nil, fmt.Errorf("file already taken: %s", key)
	}
	delete(f.files, key)
	return file, nil
}

type chunkFixture struct {
	inline     bool
	data       []byte
	read       bool
	checkpoint x.SinkCheckpoint
}

func (c *chunkFixture) ReadChunk(x.Deadline) (x.Chunk, error) {
	if c.read {
		return x.Chunk{}, io.EOF
	}
	c.read = true
	seq := uint64(1)
	if c.inline {
		seq = 0
	}
	return x.Chunk{Data: c.data, Receipt: x.ChunkReceipt{Index: 0, StreamSeq: seq, Offset: 0, Length: uint32(len(c.data)), SHA256: sha256.Sum256(c.data)}}, nil
}
func (c *chunkFixture) ConfirmSynced(checkpoint x.SinkCheckpoint) error {
	c.checkpoint = checkpoint
	return nil
}
func (c *chunkFixture) Freeze(x.Deadline) (x.ReaderState, error) {
	return x.ReaderState{Phase: x.ReaderOpen, SinkCheckpoint: c.checkpoint}, nil
}
func (c *chunkFixture) Stop(x.Deadline) error { return nil }

func realAtomicDeadline(t *testing.T) x.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// origin: hot-update S7 plan §4 / T12 / T13
func TestTransferReceiverEvidenceAdvancesOnRealLedger(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseDurableReceiverEvidence(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "durable receiver evidence", g)
	testharness.AssertNoFDLeak(t, "durable receiver evidence", fd, 0)
}
func exerciseDurableReceiverEvidence(t *testing.T) {
	f := newFixture(t, "b", "push")
	cfg := x.LedgerConfig{Base: f.owner.cfg.Base, Root: t.TempDir(), FallbackRoot: t.TempDir()}
	ledger, err := xferledger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	f.owner.cfg.Ledger = ledger
	f.admit(t)
	grant := f.reserve(t)
	out := x.ReceiverOutcome{Grant: grant, Primitive: x.PrimitiveRename, Outcome: x.OutcomeInflight}
	if _, err = f.owner.Finish(f.permit.Identity, out, f.clock.deadline()); err == nil {
		t.Fatal("inflight primitive became a terminal")
	}
	census, err := ledger.Scan(f.clock.deadline())
	if err != nil || len(census.Rows) != 1 || census.Rows[0].ReceiverOutcome == nil || census.Rows[0].ReceiverOutcome.Outcome != x.OutcomeInflight || census.Rows[0].CanonicalTerminal != nil {
		t.Fatalf("inflight evidence was not durable: %+v %v", census, err)
	}
	out = f.outcome(grant)
	out.ParentSynced = false
	if _, err = f.owner.Finish(f.permit.Identity, out, f.clock.deadline()); err == nil {
		t.Fatal("unsynced parent became a terminal")
	}
	census, err = ledger.Scan(f.clock.deadline())
	if err != nil || !census.Rows[0].Cleanup.DirectorySyncPending || census.Rows[0].ReceiverOutcome.Outcome != x.OutcomeLanded || census.Rows[0].CanonicalTerminal != nil {
		t.Fatalf("landed evidence was not durable: %+v %v", census, err)
	}
	// A caller bypassing the tracker still cannot regress persisted landing.
	old := census.Rows[0]
	bad := old
	failure := x.ReceiverOutcome{Grant: grant, Primitive: x.PrimitiveRename, Outcome: x.OutcomeInflight}
	bad.ReceiverOutcome = &failure
	if _, err = ledger.Update(key(old.Identity), old.Revision, x.LedgerMutation{Kind: x.MutationDecide, EntryNonce: old.EntryNonce, Row: bad}, f.clock.deadline()); err == nil {
		t.Fatal("ledger forgot observed landing")
	}
	out.ParentSynced = true
	receipt, err := f.owner.Finish(f.permit.Identity, out, f.clock.deadline())
	if err != nil || receipt.Kind != "complete" {
		t.Fatalf("parent sync could not finish original grant: %+v %v", receipt, err)
	}
}

// TestTransferAbandonDecidesOneUnknownFailedTerminal runs the operator's
// abandonment (lead LQ1) through the tracker's own serializer onto the REAL
// ledger: an unresolved granted entry, and a landed-but-unsynced one, each get
// exactly one failed terminal in the shared audit schema with code
// settlement_abandoned and outcome_unknown=true, published to history; their
// retained receiver evidence is kept, never a fabricated not-entered result.
// A replay decides nothing new, a later Reserve is refused, and an entry the
// receiver already decided keeps its complete terminal.
// origin: hot-update S7 plan lead LQ1 / §6.4 / T31 / INTEG-D session abandonment
func TestTransferAbandonDecidesOneUnknownFailedTerminal(t *testing.T) {
	for _, state := range []string{"ungranted", "granted", "landed-unsynced", "decided"} {
		t.Run(state, func(t *testing.T) {
			f := newFixture(t, "b", "push")
			ledger, err := xferledger.New(x.LedgerConfig{Base: f.owner.cfg.Base, Root: t.TempDir(), FallbackRoot: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			f.owner.cfg.Ledger = ledger
			f.admit(t)
			var grant x.CommitGrant
			if state != "ungranted" {
				grant = f.reserve(t)
			}
			id := f.permit.Identity
			switch state {
			case "landed-unsynced":
				out := f.outcome(grant)
				out.ParentSynced = false
				if _, err := f.owner.Finish(id, out, f.clock.deadline()); err == nil {
					t.Fatal("unsynced parent became a terminal")
				}
			case "decided":
				if _, err := f.owner.Finish(id, f.outcome(grant), f.clock.deadline()); err != nil {
					t.Fatal(err)
				}
			}
			status, err := f.owner.Abandon(id, f.clock.deadline())
			if err != nil {
				t.Fatalf("abandon: %v", err)
			}
			census, err := ledger.Scan(f.clock.deadline())
			if err != nil || len(census.Rows) != 1 {
				t.Fatalf("census: %+v %v", census, err)
			}
			row := census.Rows[0]
			if state == "decided" {
				if row.OutcomeUnknown || row.CanonicalTerminal.Kind != "complete" || status.Terminal == nil || status.Terminal.Kind != "complete" {
					t.Fatalf("abandonment overwrote a decided outcome: %+v", row)
				}
				return
			}
			if !row.OutcomeUnknown || row.CanonicalTerminal == nil || row.CanonicalTerminal.Kind != "failed" ||
				row.TerminalDelivery != x.DeliveryHistoryConfirmed || status.Terminal == nil || !status.Terminal.OutcomeUnknown ||
				status.Terminal.Kind != "failed" || status.Terminal.Code != x.ReasonSettlementAbandoned {
				t.Fatalf("abandonment is not one exact unknown failed terminal: row=%+v status=%+v", row, status)
			}
			// Inspect the real canonical JSON keys; a Go field name is no wire oracle.
			var fields map[string]any
			if err := json.Unmarshal(row.CanonicalTerminal.Bytes, &fields); err != nil {
				t.Fatal(err)
			}
			unknown, ok := fields["outcome_unknown"].(bool)
			if !ok || !unknown || fields["kind"] != "failed" || fields["code"] != x.ReasonSettlementAbandoned || fields["terminal_id"] != terminalID(id) {
				t.Fatalf("abandoned canonical bytes: %s", row.CanonicalTerminal.Bytes)
			}
			out := row.ReceiverOutcome
			if out == nil || out.Grant != grant || out.Outcome == x.OutcomeNotEntered ||
				(state == "landed-unsynced") != (out.Outcome == x.OutcomeLanded && out.Bytes == f.intent.Size) {
				t.Fatalf("abandonment did not keep the retained receiver evidence: %+v", out)
			}
			if _, ok := f.history.records[row.CanonicalTerminal.Digest]; !ok {
				t.Fatal("abandoned terminal was not published to history")
			}
			revision, digest := row.Revision, row.CanonicalTerminal.Digest
			if _, err := f.owner.Abandon(id, f.clock.deadline()); err != nil {
				t.Fatal(err)
			}
			again, _ := ledger.Scan(f.clock.deadline())
			if again.Rows[0].Revision != revision || again.Rows[0].CanonicalTerminal.Digest != digest {
				t.Fatal("abandonment replay decided a second terminal")
			}
			// An ungranted abandoned slot never grants; a granted one only
			// replays its original Grant, never a new byte authority.
			replayed, err := f.owner.Reserve(id, x.ReserveRequest{Origin: f.permit.Origin, ReceiverInstance: "agent"}, f.clock.deadline())
			if state == "ungranted" && err == nil || state != "ungranted" && (err != nil || replayed != grant) {
				t.Fatalf("an abandoned entry changed its byte authority: %+v %v", replayed, err)
			}
		})
	}
}
