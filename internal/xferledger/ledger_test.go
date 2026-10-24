package xferledger

// Real disk writes, Sync, restart, permission failures and concurrent CAS.
// No fake store/history ACKs: history evidence is supplied at the frozen port.
// Tracker decision production and JetStream publication are P11/P14 integration.

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

func testConfig(t *testing.T) Config {
	t.Helper()
	root := t.TempDir()
	return Config{Root: filepath.Join(root, "primary"), FallbackRoot: filepath.Join(root, "outbox"),
		Base: xfercontract.Base{Clock: func() (string, int64, error) {
			now, err := controlbudget.Now()
			return "boot", now, err
		}, UTC: time.Now}}
}

func newStore(t *testing.T, cfg Config) Store {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func testHash(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func canonical(t *testing.T, row RowV1, kind, code string) *xfercontract.CanonicalRecord {
	t.Helper()
	// Embed the current production schema; the additive field is the exact
	// frozen LQ1 wire spelling awaiting P15's schema extension.
	audit := struct {
		schema.AuditTransfer
		OutcomeUnknown bool `json:"outcome_unknown,omitempty"`
	}{AuditTransfer: schema.AuditTransfer{V: 1, Kind: kind, Verb: row.Intent.Verb, Ts: row.StartedAt,
		Session: row.Identity.SID, Node: row.Identity.NID, ActorFp: row.Identity.ActorFP,
		TransferID: row.Identity.TransferID, Tier: row.Intent.Tier, Code: code}, OutcomeUnknown: row.OutcomeUnknown}
	if kind != "start" && row.ReceiverOutcome != nil {
		audit.Bytes, audit.SHA256 = row.ReceiverOutcome.Bytes, row.ReceiverOutcome.SHA256
	}
	data, err := json.Marshal(audit)
	if err != nil {
		t.Fatal(err)
	}
	return &xfercontract.CanonicalRecord{Identity: row.Identity, Kind: kind, Bytes: data,
		Digest: testHash(data), ReqID: testHash(append([]byte("xferaudit-record:"), data...))}
}

func startRow(t *testing.T) RowV1 {
	t.Helper()
	started := time.Date(2026, 10, 12, 12, 0, 0, 123, time.UTC)
	id := xfercontract.Identity{SID: "session", NID: "node", TransferID: "transfer", EntryNonce: "entry",
		ActorFP: "actor", AgentInstance: "agent", IntentDigest: "intent", ObjectVersion: "version"}
	origin := xfercontract.Origin{BrokerID: "broker", InstanceID: "instance", BootID: "boot", Generation: 1}
	row := RowV1{Identity: id, Origin: origin, EntryNonce: id.EntryNonce, StartedAt: started,
		Permit:     xfercontract.CreationPermit{Identity: id, Origin: origin, PermitNonce: "permit", IssuedAt: started, CreateUntil: started.Add(time.Minute)},
		Intent:     xfercontract.Intent{Tier: "b", Verb: "pull", RateLimitBPS: 1000000},
		BudgetSeal: xfercontract.BudgetSeal{StartedAt: started, RateLimitBPS: 1000000, EffectiveBudgetNS: int64(2108 * time.Second), RemainingNS: int64(2000 * time.Second)},
		Phase:      xfercontract.PhaseOpen, StartDelivery: xfercontract.DeliveryPending,
		Retention: xfercontract.RetentionState{Identity: id, CreateUntil: started.Add(time.Minute), ResultUntil: started.Add(10 * time.Minute)}}
	row.CanonicalStart = canonical(t, row, "start", "")
	return row
}

func clone(t *testing.T, row RowV1) RowV1 {
	t.Helper()
	data, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	var copy RowV1
	if err := json.Unmarshal(data, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func sealedRow(t *testing.T, row RowV1) RowV1 {
	t.Helper()
	row = clone(t, row)
	row.SourceSeal = &xfercontract.SourceSeal{Size: 123456, SHA256: "sha", ChunkSize: 32768, Chunks: 4,
		RateLimitBPS: row.Intent.RateLimitBPS, SourceFingerprint: "pinned"}
	row.BudgetSeal.Size, row.BudgetSeal.SourceSealed = row.SourceSeal.Size, true
	row.BudgetSeal.EffectiveBudgetNS, row.BudgetSeal.RemainingNS = int64(300*time.Second), int64(290*time.Second)
	return row
}

func decidedRow(t *testing.T, row RowV1) RowV1 {
	t.Helper()
	row = clone(t, row)
	row.CanonicalTerminal = canonical(t, row, "failed", "transfer_canceled")
	row.ReceiverOutcome = &xfercontract.ReceiverOutcome{Outcome: xfercontract.OutcomeNotEntered, Code: "transfer_canceled"}
	if row.Grant != nil {
		row.ReceiverOutcome.Grant = *row.Grant
	}
	row.Phase, row.TerminalDelivery = xfercontract.PhaseDecided, xfercontract.DeliveryPending
	return row
}

func grantedRow(t *testing.T, row RowV1) RowV1 {
	t.Helper()
	row = clone(t, row)
	row.Grant = &xfercontract.CommitGrant{Identity: row.Identity, GrantNonce: "grant", ReceiverInstance: "receiver",
		TerminalID: testHash([]byte("xfer-terminal-v1\n" + row.Identity.SID + "\n" + row.Identity.TransferID + "\n" + row.EntryNonce))}
	row.Phase = xfercontract.PhaseCommitReserved
	return row
}

func confirmedRow(t *testing.T, row RowV1) RowV1 {
	t.Helper()
	row = clone(t, row)
	row.HistoryReceipts = []xfercontract.HistoryReceipt{{Stream: "history-session", Sequence: 7, RecordDigest: row.CanonicalStart.Digest},
		{Stream: "history-session", Sequence: 9, RecordDigest: row.CanonicalTerminal.Digest}}
	row.StartDelivery, row.TerminalDelivery, row.Phase = xfercontract.DeliveryHistoryConfirmed, xfercontract.DeliveryHistoryConfirmed, xfercontract.PhaseHistoryConfirmed
	return row
}

func update(t *testing.T, s Store, old RowV1, next RowV1, kind xfercontract.LedgerMutationKind) RowV1 {
	t.Helper()
	row, err := s.Update(rowKey(next), old.Revision, xfercontract.LedgerMutation{Kind: kind, EntryNonce: next.EntryNonce, Row: next}, testDeadline(t))
	if err != nil {
		t.Fatal(err)
	}
	if row.Revision != old.Revision+1 {
		t.Fatalf("revision = %d, want %d", row.Revision, old.Revision+1)
	}
	return row
}

func onlyRow(t *testing.T, s Store) RowV1 {
	t.Helper()
	census, err := s.Scan(testDeadline(t))
	if err != nil || !census.Ready || !census.Complete || len(census.Rows) != 1 || len(census.Entries) != 1 || census.Digest == "" {
		t.Fatalf("incomplete census: rows=%d, ready=%v, complete=%v, err=%v", len(census.Rows), census.Ready, census.Complete, err)
	}
	return census.Rows[0]
}

// origin: hot-update S7 plan §5.1 / T15 / M11
func TestTransferSourceSealCannotOverwriteTerminal(t *testing.T) {
	for _, order := range []string{"decision_first", "seal_first", "concurrent"} {
		t.Run(order, func(t *testing.T) {
			cfg := testConfig(t)
			s := newStore(t, cfg)
			start := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
			seal, decision := sealedRow(t, start), decidedRow(t, start)
			switch order {
			case "decision_first":
				update(t, s, start, decision, xfercontract.MutationDecide)
				_, err := s.Update(rowKey(start), start.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationSourceSeal, EntryNonce: start.EntryNonce, Row: seal}, testDeadline(t))
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("late seal accepted: %v", err)
				}
			case "seal_first":
				sealed := update(t, s, start, seal, xfercontract.MutationSourceSeal)
				update(t, s, sealed, decidedRow(t, sealed), xfercontract.MutationDecide)
			case "concurrent":
				var wg sync.WaitGroup
				results := make(chan error, 2)
				gate := make(chan struct{})
				deadline := testDeadline(t)
				for _, mutation := range []xfercontract.LedgerMutation{{Kind: xfercontract.MutationSourceSeal, EntryNonce: start.EntryNonce, Row: seal}, {Kind: xfercontract.MutationDecide, EntryNonce: start.EntryNonce, Row: decision}} {
					wg.Go(func() {
						<-gate
						_, err := s.Update(rowKey(start), start.Revision, mutation, deadline)
						results <- err
					})
				}
				close(gate)
				wg.Wait()
				close(results)
				success := 0
				for err := range results {
					if err == nil {
						success++
					} else if !errors.Is(err, ErrConflict) {
						t.Fatal(err)
					}
				}
				if success != 1 {
					t.Fatalf("CAS winners = %d", success)
				}
				cur := onlyRow(t, s)
				if cur.CanonicalTerminal == nil {
					update(t, s, cur, decidedRow(t, cur), xfercontract.MutationDecide)
				}
			}
			got := onlyRow(t, newStore(t, cfg))
			if !reflect.DeepEqual(got.CanonicalTerminal, decision.CanonicalTerminal) || !reflect.DeepEqual(got.CanonicalStart, start.CanonicalStart) ||
				!got.StartedAt.Equal(start.StartedAt) || got.BudgetSeal.RateLimitBPS != start.BudgetSeal.RateLimitBPS || !got.BudgetSeal.StartedAt.Equal(start.StartedAt) {
				t.Fatal("restart changed canonical decision or original start/rate")
			}
			if got.SourceSeal != nil && (got.BudgetSeal.EffectiveBudgetNS != seal.BudgetSeal.EffectiveBudgetNS || got.BudgetSeal.RemainingNS != seal.BudgetSeal.RemainingNS) {
				t.Fatal("source budget was replenished")
			}
			late := sealedRow(t, got)
			_, err := s.Update(rowKey(got), got.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationSourceSeal, EntryNonce: got.EntryNonce, Row: late}, testDeadline(t))
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("seal after terminal with current revision: %v", err)
			}
		})
	}
	t.Run("budget_is_immutable", func(t *testing.T) {
		for _, field := range []string{"start", "rate", "ceiling", "remaining", "sealed_again"} {
			t.Run(field, func(t *testing.T) {
				s := newStore(t, testConfig(t))
				old := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
				if field == "sealed_again" {
					old = update(t, s, old, sealedRow(t, old), xfercontract.MutationSourceSeal)
				}
				next := sealedRow(t, old)
				switch field {
				case "start":
					next.StartedAt = next.StartedAt.Add(time.Second)
					next.BudgetSeal.StartedAt = next.StartedAt
				case "rate":
					next.Intent.RateLimitBPS++
					next.SourceSeal.RateLimitBPS++
					next.BudgetSeal.RateLimitBPS++
				case "ceiling":
					next.BudgetSeal.EffectiveBudgetNS = old.BudgetSeal.EffectiveBudgetNS + 1
				case "remaining":
					next.BudgetSeal.RemainingNS = old.BudgetSeal.RemainingNS + 1
				case "sealed_again":
					next.SourceSeal.SourceFingerprint = "changed"
				}
				_, err := s.Update(rowKey(old), old.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationSourceSeal, EntryNonce: old.EntryNonce, Row: next}, testDeadline(t))
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("changed %s accepted: %v", field, err)
				}
				if !reflect.DeepEqual(onlyRow(t, s), old) {
					t.Fatal("rejected mutation changed row")
				}
			})
		}
	})
}

func writeFixture(t *testing.T, root string, row RowV1) []byte {
	t.Helper()
	data, err := encodeRow(row)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, rowName(rowKey(row))), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return data
}

func deny(t *testing.T, path string, mode os.FileMode) func() {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	restore := func() {
		if err := os.Chmod(path, info.Mode().Perm()); err != nil {
			t.Error(err)
		}
	}
	t.Cleanup(restore)
	return restore
}

func retirement(row RowV1) xfercontract.RetirementProof {
	return xfercontract.RetirementProof{Identity: row.Identity, Origin: row.Origin, OwnerGeneration: 1,
		EntryNonce: row.EntryNonce, Revision: row.Revision, CreationUntil: row.Permit.CreateUntil,
		WriterRetired: true, ReceiverRetired: true, ConsumersRetired: true,
		StartReceipt: row.HistoryReceipts[0], TerminalReceipt: row.HistoryReceipts[1],
		Cleanup: xfercontract.CleanupReceipt{Identity: row.Identity, OwnerGeneration: 1, WriterRetired: true, ConsumersRetired: true,
			FenceSequence: 10, ObjectPurged: true, TmpUnlinked: true, DirectorySynced: true, Digest: "cleanup-proof"}}
}

// origin: hot-update S7 plan §5.1 / T22
func TestTransferLedgerConflictNeverAcknowledges(t *testing.T) {
	t.Run("copies", func(t *testing.T) {
		for _, fault := range []string{"start", "terminal", "nonce", "same_revision", "newer_start"} {
			t.Run(fault, func(t *testing.T) {
				cfg := testConfig(t)
				s := newStore(t, cfg)
				start := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
				terminal := update(t, s, start, decidedRow(t, start), xfercontract.MutationDecide)
				other := clone(t, terminal)
				switch fault {
				case "start":
					other.CanonicalStart = canonical(t, other, "start", "different")
				case "terminal":
					other.CanonicalTerminal = canonical(t, other, "failed", "different")
					other.Revision++
				case "nonce":
					other.EntryNonce = "replacement"
					other.Identity.EntryNonce = other.EntryNonce
				case "same_revision":
					other.Phase = xfercontract.PhaseTerminalPending
				case "newer_start":
					other = clone(t, start)
					other.Revision = terminal.Revision + 1
				}
				outbox := writeFixture(t, cfg.FallbackRoot, other)
				primary, err := os.ReadFile(filepath.Join(cfg.Root, rowName(rowKey(terminal))))
				if err != nil {
					t.Fatal(err)
				}
				for _, store := range []Store{s, newStore(t, cfg)} {
					census, err := store.Scan(testDeadline(t))
					if err == nil || census.Ready || census.Complete {
						t.Fatal("conflicting census acknowledged")
					}
					_, err = store.Update(rowKey(terminal), terminal.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationHistory, EntryNonce: terminal.EntryNonce, Row: confirmedRow(t, terminal)}, testDeadline(t))
					if err == nil {
						t.Fatal("conflicting copies overwritten")
					}
					if err := store.Retire(rowKey(terminal), terminal.EntryNonce, xfercontract.RetirementProof{}, testDeadline(t)); err == nil {
						t.Fatal("conflict retired")
					}
				}
				for path, want := range map[string][]byte{cfg.Root: primary, cfg.FallbackRoot: outbox} {
					got, err := os.ReadFile(filepath.Join(path, rowName(rowKey(terminal))))
					if err != nil || string(got) != string(want) {
						t.Fatal("conflict evidence changed")
					}
				}
			})
		}
	})
	t.Run("image_and_history", func(t *testing.T) {
		for _, fault := range []string{"image_terminal", "image_start", "history_digest", "history_duplicate", "history_sequence", "history_stream", "history_missing", "history_removed", "nonce", "revision", "unknown_kind", "unrelated_field", "unrelated_cleanup", "grant_replacement"} {
			t.Run(fault, func(t *testing.T) {
				s := newStore(t, testConfig(t))
				old := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
				old = update(t, s, old, grantedRow(t, old), xfercontract.MutationReserve)
				old = update(t, s, old, decidedRow(t, old), xfercontract.MutationDecide)
				if fault == "history_removed" {
					old = update(t, s, old, confirmedRow(t, old), xfercontract.MutationHistory)
				}
				next := confirmedRow(t, old)
				kind, expected, nonce := xfercontract.MutationHistory, old.Revision, old.EntryNonce
				switch fault {
				case "image_terminal":
					next.CanonicalTerminal = canonical(t, next, "failed", "opposite")
				case "image_start":
					next.CanonicalStart = canonical(t, next, "start", "different")
				case "history_digest":
					next.HistoryReceipts[1].RecordDigest = "opposite"
				case "history_duplicate":
					next.HistoryReceipts = append(next.HistoryReceipts, next.HistoryReceipts[1])
				case "history_sequence":
					next.HistoryReceipts[1].Sequence = 0
				case "history_stream":
					next.HistoryReceipts[1].Stream = "different"
				case "history_missing":
					next.HistoryReceipts = nil
				case "history_removed":
					next.HistoryReceipts[1].Sequence++
				case "nonce":
					nonce = "old-entry"
				case "revision":
					expected--
					next.Revision = 0
				case "unknown_kind":
					kind = "unknown"
				case "unrelated_field":
					next.Retention.ResultUntil = next.Retention.ResultUntil.Add(time.Hour)
				case "unrelated_cleanup":
					next.Cleanup.DirectorySyncPending = true
				case "grant_replacement":
					next.Grant.GrantNonce = "replacement"
					next.ReceiverOutcome.Grant = *next.Grant
				}
				_, err := s.Update(rowKey(old), expected, xfercontract.LedgerMutation{Kind: kind, EntryNonce: nonce, Row: next}, testDeadline(t))
				if !errors.Is(err, ErrConflict) {
					t.Fatalf("conflict accepted: %v", err)
				}
				if !reflect.DeepEqual(old, onlyRow(t, s)) {
					t.Fatal("rejected conflict mutated memory or disk")
				}
			})
		}
	})
	t.Run("fallback_and_dual_write_failure", func(t *testing.T) { exerciseWriteFailure(t) })
	t.Run("unreadable", func(t *testing.T) {
		for _, fault := range []string{"directories", "rows", "corrupt", "quarantine", "missing_root", "symlink", "codec_digest", "codec_schema", "codec_duplicate"} {
			t.Run(fault, func(t *testing.T) {
				cfg := testConfig(t)
				s := newStore(t, cfg)
				old := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
				writeFixture(t, cfg.FallbackRoot, old)
				name := rowName(rowKey(old))
				switch fault {
				case "directories":
					deny(t, cfg.Root, 0)
					deny(t, cfg.FallbackRoot, 0)
				case "rows":
					deny(t, filepath.Join(cfg.Root, name), 0)
					deny(t, filepath.Join(cfg.FallbackRoot, name), 0)
				case "missing_root":
					if err := os.Rename(cfg.FallbackRoot, cfg.FallbackRoot+"-hidden"); err != nil {
						t.Fatal(err)
					}
				case "symlink":
					if err := os.Remove(filepath.Join(cfg.FallbackRoot, name)); err != nil {
						t.Fatal(err)
					}
					if err := os.Symlink(filepath.Join(cfg.Root, name), filepath.Join(cfg.FallbackRoot, name)); err != nil {
						t.Fatal(err)
					}
				default:
					data := []byte("{broken")
					if strings.HasPrefix(fault, "codec_") {
						data, _ = encodeRow(old)
						switch fault {
						case "codec_digest":
							data = []byte(strings.Replace(string(data), `"digest":"`, `"digest":"wrong`, 1))
						case "codec_schema":
							data = []byte(strings.Replace(string(data), ledgerSchema, "future-schema", 1))
						case "codec_duplicate":
							data = append([]byte(`{"schema":"ignored",`), data[1:]...)
						}
					}
					if fault == "quarantine" {
						name += ".corrupt"
					}
					if err := os.WriteFile(filepath.Join(cfg.FallbackRoot, name), data, 0o600); err != nil {
						t.Fatal(err)
					}
				}
				census, err := s.Scan(testDeadline(t))
				if err == nil || census.Ready || census.Complete || len(census.Rows) == 0 {
					t.Fatal("unreadable evidence became absence")
				}
				if err := s.Retire(rowKey(old), old.EntryNonce, xfercontract.RetirementProof{}, testDeadline(t)); err == nil {
					t.Fatal("unreadable evidence retired")
				}
				_, err = s.Update(rowKey(old), old.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationDecide,
					EntryNonce: old.EntryNonce, Row: decidedRow(t, old)}, testDeadline(t))
				if err == nil {
					t.Fatal("unknown evidence was overwritten by a decision")
				}
			})
		}
	})
	t.Run("retirement", func(t *testing.T) { exerciseRetirement(t) })
	t.Run("sync_failures", func(t *testing.T) { exerciseSyncFailures(t) })
	t.Run("record_guards", func(t *testing.T) { exerciseRecordGuards(t) })
	t.Run("identity_and_deadline", func(t *testing.T) { exerciseKeyIsolation(t) })
}

func exerciseWriteFailure(t *testing.T) {
	t.Helper()
	cfg := testConfig(t)
	s := newStore(t, cfg)
	start := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
	restorePrimary := deny(t, cfg.Root, 0o500)
	sealed := update(t, s, start, sealedRow(t, start), xfercontract.MutationSourceSeal)
	if !reflect.DeepEqual(onlyRow(t, newStore(t, cfg)), sealed) {
		t.Fatal("fallback revision did not survive restart")
	}
	restoreOutbox := deny(t, cfg.FallbackRoot, 0o500)
	grant := grantedRow(t, sealed)
	mutation := xfercontract.LedgerMutation{Kind: xfercontract.MutationReserve, EntryNonce: sealed.EntryNonce, Row: grant}
	retained, err := s.Update(rowKey(sealed), sealed.Revision, mutation, testDeadline(t))
	if !errors.Is(err, ErrDurability) || retained.Grant == nil {
		t.Fatalf("dual write failure lost responsibility or acknowledged grant: %v", err)
	}
	census, err := s.Scan(testDeadline(t))
	if err == nil || census.Ready || census.Complete || len(census.Rows) != 1 || census.Rows[0].Grant == nil {
		t.Fatal("memory-only grant treated as durable or discarded")
	}
	if cold := onlyRow(t, newStore(t, cfg)); cold.Grant != nil {
		t.Fatal("cold recovery fabricated a durable grant")
	}
	restoreOutbox()
	restorePrimary()
	unknown := mutation
	unknown.Kind = "unknown"
	if _, err := s.Update(rowKey(sealed), sealed.Revision, unknown, testDeadline(t)); !errors.Is(err, ErrConflict) {
		t.Fatalf("unknown failed-write retry accepted: %v", err)
	}
	retried, err := s.Update(rowKey(sealed), sealed.Revision, mutation, testDeadline(t))
	if err != nil || !reflect.DeepEqual(retried, retained) {
		t.Fatalf("exact failed-write retry changed decision: %v", err)
	}
	if !reflect.DeepEqual(onlyRow(t, newStore(t, cfg)), retained) {
		t.Fatal("retried decision not durable")
	}
	// Also retain canonical terminal bytes when BOTH durable destinations fail.
	restorePrimary = deny(t, cfg.Root, 0o500)
	restoreOutbox = deny(t, cfg.FallbackRoot, 0o500)
	decision := decidedRow(t, retained)
	_, err = s.Update(rowKey(retained), retained.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationDecide, EntryNonce: retained.EntryNonce, Row: decision}, testDeadline(t))
	if !errors.Is(err, ErrDurability) {
		t.Fatalf("memory terminal acknowledged: %v", err)
	}
	census, err = s.Scan(testDeadline(t))
	if err == nil || len(census.Rows) != 1 || !reflect.DeepEqual(census.Rows[0].CanonicalTerminal, decision.CanonicalTerminal) {
		t.Fatal("memory terminal lost during failed persistence")
	}
	restoreOutbox()
	restorePrimary()
}

func exerciseRetirement(t *testing.T) {
	t.Helper()
	cfg := testConfig(t)
	s := newStore(t, cfg)
	old := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
	writeFixture(t, cfg.FallbackRoot, old) // Old fallback START must never outlive the only terminal.
	old = update(t, s, old, decidedRow(t, old), xfercontract.MutationDecide)
	old = update(t, s, old, confirmedRow(t, old), xfercontract.MutationHistory)
	proof := retirement(old)
	for _, fault := range []string{"nonce", "revision", "history", "cleanup", "generation", "receiver"} {
		bad := proof
		switch fault {
		case "nonce":
			bad.EntryNonce = "replacement"
		case "revision":
			bad.Revision--
		case "history":
			bad.TerminalReceipt.Sequence++
		case "cleanup":
			bad.Cleanup.DirectorySynced = false
		case "generation":
			bad.OwnerGeneration++
		case "receiver":
			bad.ReceiverRetired = false
		}
		if err := s.Retire(rowKey(old), old.EntryNonce, bad, testDeadline(t)); !errors.Is(err, ErrConflict) {
			t.Fatalf("retirement accepted %s: %v", fault, err)
		}
	}
	restore := deny(t, cfg.Root, 0o500)
	if err := s.Retire(rowKey(old), old.EntryNonce, proof, testDeadline(t)); err == nil {
		t.Fatal("failed primary unlink acknowledged")
	}
	if _, err := os.Stat(filepath.Join(cfg.FallbackRoot, rowName(rowKey(old)))); err != nil {
		t.Fatal("outbox deleted before primary")
	}
	restore()
	if err := s.Retire(rowKey(old), old.EntryNonce, proof, testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	census, err := newStore(t, cfg).Scan(testDeadline(t))
	if err != nil || !census.Ready || !census.Complete || len(census.Rows) != 0 {
		t.Fatalf("retired rows survived: %v", err)
	}
	// A delayed callback cannot consume a new entry under the same business key.
	replacement := startRow(t)
	replacement.Identity.EntryNonce, replacement.EntryNonce = "new-entry", "new-entry"
	replacement.Permit.Identity, replacement.Retention.Identity = replacement.Identity, replacement.Identity
	replacement.CanonicalStart = canonical(t, replacement, "start", "")
	replacement = update(t, s, RowV1{}, replacement, xfercontract.MutationAdmit)
	if err := s.Retire(rowKey(old), old.EntryNonce, proof, testDeadline(t)); !errors.Is(err, ErrConflict) {
		t.Fatalf("old callback consumed new entry: %v", err)
	}
	if !reflect.DeepEqual(onlyRow(t, s), replacement) {
		t.Fatal("replacement entry was changed")
	}
}
