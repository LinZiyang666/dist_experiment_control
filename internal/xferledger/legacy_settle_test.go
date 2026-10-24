package xferledger

// The cap0 ledger owner against real ledger directories. The history writer is
// supplied at its Confirm port so each test can block, lose or contradict one
// exact confirmation; the real JetStream CAS path is exercised by the broker
// and the real-binary legacy quadrants.

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
)

type legacyHistoryFake struct {
	calls   []schema.AuditTransfer // every row offered, in order
	present map[string]schema.AuditTransfer
	refuse  func(schema.AuditTransfer) error
	deleted []string
	commits []schema.AuditTransfer
}

// confirm keeps the exact-writer semantics the real port has: an identical row
// already present is a receipt, a different row in the same slot is a conflict.
func (h *legacyHistoryFake) confirm(rec schema.AuditTransfer) error {
	h.calls = append(h.calls, rec)
	if _, err := schema.CanonicalTransferAudit(rec); err != nil {
		return err
	}
	if h.refuse != nil {
		if err := h.refuse(rec); err != nil {
			return err
		}
	}
	slot := rec.TransferID + "/" + map[bool]string{true: "start", false: "terminal"}[rec.Kind == "start"]
	want, _ := schema.TransferRecordReqID(rec)
	if old, ok := h.present[slot]; ok {
		if got, _ := schema.TransferRecordReqID(old); got != want {
			return errors.New("transfer_terminal_conflict")
		}
		return nil
	}
	h.present[slot] = rec
	return nil
}

func legacyOwner(t *testing.T, h *legacyHistoryFake, now time.Time) *Legacy {
	t.Helper()
	return &Legacy{Config: LegacyConfig{ClusterDataDir: t.TempDir(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now: func() time.Time { return now }, Live: func(string) bool { return false },
		Delete:  func(_, id string) error { h.deleted = append(h.deleted, id); return nil },
		Commit:  func(rec schema.AuditTransfer) bool { h.commits = append(h.commits, rec); return true },
		Publish: func(string, []byte, string) error { return errors.New("legacy MsgID path must not be used") },
		Confirm: h.confirm}}
}

func legacyStart(now time.Time) schema.AuditTransfer {
	return schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "start", Verb: "push", Ts: now,
		Session: "lab", Node: "lab-1", ActorNkey: "UACTOR", ActorFp: "fp", TransferID: "tid-cap0",
		Path: "/dst", Size: 4, SHA256: strings.Repeat("a", 64), Tier: "a"}
}

func admittedRow(t *testing.T, l *Legacy, now time.Time) LegacyRecord {
	t.Helper()
	start, retained := l.Admit(legacyStart(now))
	if retained == nil || start.EntryNonce == "" || !start.BestEffort {
		t.Fatal("an exact history owner did not retain the cap0 admission identity")
	}
	row := LegacyRecord{TransferID: start.TransferID, Session: start.Session, Node: start.Node, ActorNkey: start.ActorNkey,
		ActorFp: start.ActorFp, Verb: start.Verb, Tier: start.Tier, Path: start.Path, StartedAt: start.Ts, Size: start.Size, Start: retained}
	if err := l.WriteRecord(l.InflightDir(), row); err != nil {
		t.Fatal(err)
	}
	return row
}

func completeFor(row LegacyRecord, at time.Time) schema.AuditTransfer {
	return schema.BindLegacyTerminal(schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "complete", Verb: row.Verb, Ts: at,
		Session: row.Session, Node: row.Node, ActorNkey: row.ActorNkey, ActorFp: row.ActorFp, TransferID: row.TransferID,
		Path: row.Path, Tier: row.Tier, Bytes: row.Size}, row.Start.EntryNonce)
}

func legacyRowsOnDisk(t *testing.T, l *Legacy, id string) int {
	t.Helper()
	n := 0
	for _, dir := range []string{l.InflightDir(), l.OutboxDir()} {
		if _, err := os.Stat(filepath.Join(dir, LegacyFilename(id))); err == nil {
			n++
		}
	}
	return n
}

// Identity is retained at admission only where it can be confirmed exactly and
// durably; it is never a v1 identity and never invented for the pre-S7 shape.
// origin: hot-update S7 plan §4 / §5.1 / L12 (S7 integration oplegacy F2)
func TestLegacyAdmissionRetainsIdentityOnlyWithExactHistoryOwner(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	h := &legacyHistoryFake{present: map[string]schema.AuditTransfer{}}
	l := legacyOwner(t, h, now)
	row := admittedRow(t, l, now)
	if row.Start.AgentInstance != "" || row.Start.IntentDigest != "" || row.Start.ObjectVersion != "" || row.Start.TerminalID != "" {
		t.Fatalf("cap0 START claimed v1 receiver authority: %+v", row.Start)
	}
	again, _ := l.Admit(legacyStart(now))
	if again.EntryNonce == row.Start.EntryNonce {
		t.Fatal("two admissions shared one entry nonce")
	}
	read, err := ReadLegacyRecord(filepath.Join(l.InflightDir(), LegacyFilename(row.TransferID)))
	if err != nil || read.Start == nil || read.Start.EntryNonce != row.Start.EntryNonce || !read.Start.BestEffort {
		t.Fatalf("the admission START did not survive the durable ledger row: %+v %v", read.Start, err)
	}
	for name, owner := range map[string]*Legacy{
		"no history writer": {Config: LegacyConfig{ClusterDataDir: t.TempDir(), Logger: l.Config.Logger}},
		"no durable ledger": {Config: LegacyConfig{Logger: l.Config.Logger, Confirm: h.confirm}},
	} {
		plain, retained := owner.Admit(legacyStart(now))
		if retained != nil || schema.TransferContinuation(plain) || !reflect.DeepEqual(plain, legacyStart(now)) {
			t.Fatalf("%s: admission retained an identity it cannot confirm: %+v", name, plain)
		}
	}
}

// A blocked START never lets its TERMINAL out first; a lost TERMINAL ACK is
// replayed with the identical staged bytes, and only both exact receipts retire
// the rows (object first, then primary before outbox).
// origin: hot-update S7 plan §5.1 / §5.2 / L12 (S7 integration oplegacy F2)
func TestLegacySettleConfirmsStartBeforeTerminalAndRetainsOnRefusal(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	h := &legacyHistoryFake{present: map[string]schema.AuditTransfer{}}
	l := legacyOwner(t, h, now)
	row := admittedRow(t, l, now)
	staged, ok, applicable := l.Stage(row.TransferID, completeFor(row, now.Add(time.Second)), row.Start)
	if !ok || !applicable || staged.Start == nil || staged.Terminal == nil {
		t.Fatal("the canonical terminal was not staged beside its START")
	}
	h.refuse = func(rec schema.AuditTransfer) error {
		if rec.Kind == "start" {
			return errors.New("history unavailable")
		}
		return nil
	}
	if l.Settle(staged) {
		t.Fatal("settled while the START could not be confirmed")
	}
	if len(h.calls) != 1 || h.calls[0].Kind != "start" || len(h.present) != 0 || len(h.deleted) != 0 || legacyRowsOnDisk(t, l, row.TransferID) != 1 {
		t.Fatalf("a blocked START let its terminal out or retired rows: calls=%d present=%d deleted=%v", len(h.calls), len(h.present), h.deleted)
	}
	lost := false
	h.refuse = func(rec schema.AuditTransfer) error {
		if rec.Kind != "start" && !lost {
			lost = true
			h.present[rec.TransferID+"/terminal"] = rec // published, ACK lost
			return errors.New("ack lost")
		}
		return nil
	}
	if l.Settle(staged) || legacyRowsOnDisk(t, l, row.TransferID) != 1 || len(h.deleted) != 0 {
		t.Fatal("a lost terminal ACK retired the ledger")
	}
	replay, err := ReadLegacyRecord(filepath.Join(l.InflightDir(), LegacyFilename(row.TransferID)))
	if err != nil || !l.ReplayTerminal(replay) {
		t.Fatalf("the staged decision did not replay to exact history: %v", err)
	}
	var terminals []schema.AuditTransfer
	for _, call := range h.calls {
		if call.Kind != "start" {
			terminals = append(terminals, call)
		}
	}
	first, _ := schema.CanonicalTransferAudit(terminals[0])
	last, _ := schema.CanonicalTransferAudit(terminals[len(terminals)-1])
	if len(terminals) != 2 || first.Digest != last.Digest || h.calls[len(h.calls)-2].Kind != "start" {
		t.Fatalf("replay changed the decision or skipped its START: terminals=%d", len(terminals))
	}
	if legacyRowsOnDisk(t, l, row.TransferID) != 0 || !reflect.DeepEqual(h.deleted, []string{row.TransferID}) || len(h.commits) != 0 {
		t.Fatalf("exact settlement did not retire object+rows once, or used the Raft/MsgID path: deleted=%v commits=%d", h.deleted, len(h.commits))
	}
}

// The first staged canonical terminal is the only one this transfer may
// publish; the pre-S7 row shape keeps its historical overwrite.
// origin: hot-update S7 plan §4 / §5.1 (S7 integration oplegacy F2)
func TestLegacyStageKeepsFirstCanonicalTerminal(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	h := &legacyHistoryFake{present: map[string]schema.AuditTransfer{}}
	l := legacyOwner(t, h, now)
	row := admittedRow(t, l, now)
	complete := completeFor(row, now.Add(time.Second))
	if _, ok, _ := l.Stage(row.TransferID, complete, row.Start); !ok {
		t.Fatal("first decision not staged")
	}
	failed := complete
	failed.Kind, failed.Code, failed.Bytes = "failed", "late_event", 0
	kept, ok, _ := l.Stage(row.TransferID, failed, row.Start)
	read, err := ReadLegacyRecord(filepath.Join(l.InflightDir(), LegacyFilename(row.TransferID)))
	if !ok || err != nil || kept.Terminal.Kind != "complete" || read.Terminal == nil || read.Terminal.Kind != "complete" {
		t.Fatalf("an opposite cap0 decision replaced the staged one: kept=%+v read=%+v err=%v", kept.Terminal, read.Terminal, err)
	}
	retimed := complete
	retimed.Ts = retimed.Ts.Add(time.Millisecond)
	if kept, _, _ := l.Stage(row.TransferID, retimed, row.Start); !kept.Terminal.Ts.Equal(complete.Ts) {
		t.Fatal("a re-timed decision replaced the staged bytes")
	}
	plain := LegacyRecord{TransferID: "tid-plain", Session: "lab", Node: "lab-1", Verb: "push", Tier: "a", StartedAt: now}
	if err := l.WriteRecord(l.InflightDir(), plain); err != nil {
		t.Fatal(err)
	}
	first := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", Ts: now, Session: "lab", Node: "lab-1", TransferID: "tid-plain", Tier: "a"}
	second := first
	second.Kind = "failed"
	l.StageTerminal("tid-plain", first)
	l.StageTerminal("tid-plain", second)
	if got, err := ReadLegacyRecord(filepath.Join(l.InflightDir(), LegacyFilename("tid-plain"))); err != nil || got.Terminal.Kind != "failed" || got.Start != nil {
		t.Fatalf("the pre-S7 staging shape changed: %+v %v", got.Terminal, err)
	}
}

// A stranded canonical start-only row synthesizes the deterministic #57 failure
// bound to its admission nonce and settles it exactly; the pre-S7 row keeps the
// Raft/MsgID commit path.
// origin: hot-update S7 plan §5.1 / §5.3 / L12 (S7 integration oplegacy F2)
func TestLegacyFinalizeBindsSyntheticTerminalToAdmissionNonce(t *testing.T) {
	started := time.Unix(1_700_000_000, 0).UTC()
	now := started.Add(LegacyTimeout("a", 4) + LegacyStrandedSlack + time.Second)
	h := &legacyHistoryFake{present: map[string]schema.AuditTransfer{}}
	l := legacyOwner(t, h, now)
	row := admittedRow(t, l, started)
	plain := LegacyRecord{TransferID: "tid-plain", Session: "lab", Node: "lab-1", ActorFp: "fp", Verb: "pull", Tier: "a", StartedAt: started}
	if err := l.WriteRecord(l.InflightDir(), plain); err != nil {
		t.Fatal(err)
	}
	if n, err := l.Finalize(); err != nil || n != 2 {
		t.Fatalf("finalize = %d, %v", n, err)
	}
	terminal, ok := h.present[row.TransferID+"/terminal"]
	if !ok || terminal.Kind != "failed" || terminal.Code != "home_broker_restart" || terminal.EntryNonce != row.Start.EntryNonce ||
		terminal.TerminalID != schema.TransferTerminalID(row.Session, row.TransferID, row.Start.EntryNonce) || !terminal.BestEffort ||
		!terminal.Ts.Equal(started.Add(LegacyTierFloor("a"))) {
		t.Fatalf("stranded canonical row did not settle a bound deterministic terminal: %+v", terminal)
	}
	if _, ok := h.present[row.TransferID+"/start"]; !ok || legacyRowsOnDisk(t, l, row.TransferID) != 0 {
		t.Fatal("stranded canonical row retired without its exact START")
	}
	if len(h.commits) != 1 || h.commits[0].TransferID != "tid-plain" || schema.TransferContinuation(h.commits[0]) {
		t.Fatalf("the pre-S7 stranded row left its commit path or gained an identity: %+v", h.commits)
	}
}
