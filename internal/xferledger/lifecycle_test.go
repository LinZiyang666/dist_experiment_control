package xferledger

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

// The ledger has no Stop/Thaw actors: each operation joins its own IO. Exercise
// retained owners across expiry/restart, failed persistence, ACK and retirement.
// origin: hot-update S7 plan §11.3 / P13
func TestTransferLifecycleNoLeaks(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer ledger", goroutines)
	testharness.AssertNoFDLeak(t, "transfer ledger", fds, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	exerciseWriteFailure(t)
	exerciseRetirement(t)
	cfg := testConfig(t)
	var observations []xfercontract.Observation
	cfg.Observe = func(obs xfercontract.Observation) { observations = append(observations, obs) }
	s := newStore(t, cfg)
	old := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
	concrete := s.(*ledger)
	concrete.gate <- struct{}{}
	expired, err := controlbudget.After(time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, err = s.Scan(expired)
	concrete.unlock()
	if !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("serializer wait did not expire: %v", err)
	}
	// Timeout preserves the owner; its next absolute deadline resumes normally.
	if !reflect.DeepEqual(onlyRow(t, s), old) {
		t.Fatal("timeout discarded original owner")
	}
	if _, err := s.Scan(0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("zero deadline accepted: %v", err)
	}
	old = update(t, s, old, sealedRow(t, old), xfercontract.MutationSourceSeal)
	old = update(t, s, old, grantedRow(t, old), xfercontract.MutationReserve)
	// Lost grant reply/current-owner restart cannot mint a new grant or budget.
	s = newStore(t, cfg)
	if !reflect.DeepEqual(onlyRow(t, s), old) {
		t.Fatal("restart changed grant or source budget")
	}
	view := onlyRow(t, s)
	view.CanonicalStart.Bytes[0] = '!'
	view.Grant.GrantNonce = "caller-mutated"
	if !reflect.DeepEqual(onlyRow(t, s), old) {
		t.Fatal("returned census aliases store state")
	}
	landed := clone(t, old)
	landed.ReceiverOutcome = &xfercontract.ReceiverOutcome{Grant: *landed.Grant, Primitive: xfercontract.PrimitiveRename,
		Outcome: xfercontract.OutcomeLanded, ParentSynced: true, Bytes: landed.BudgetSeal.Size, SHA256: landed.SourceSeal.SHA256}
	landed.CanonicalTerminal = canonical(t, landed, "complete", "")
	landed.Phase, landed.TerminalDelivery = xfercontract.PhaseTerminalPending, xfercontract.DeliveryPending
	old = update(t, s, old, landed, xfercontract.MutationDecide)
	if err := s.Retire(rowKey(old), old.EntryNonce, xfercontract.RetirementProof{}, testDeadline(t)); err == nil {
		t.Fatal("terminal without ACK retired")
	}
	old = update(t, s, old, confirmedRow(t, old), xfercontract.MutationHistory)
	cleanup := clone(t, old)
	cleanup.Phase = xfercontract.PhaseCleanupPending
	old = update(t, s, old, cleanup, xfercontract.MutationCleanup)
	if err := s.Retire(rowKey(old), old.EntryNonce, retirement(old), testDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if len(observations) < 7 || observations[len(observations)-1].Kind != xfercontract.ObserveRetirement {
		t.Fatal("missing durable revision/retirement observations")
	}
	for _, obs := range observations {
		if obs.EntryNonce != old.EntryNonce || obs.Revision == 0 || !obs.DirectorySynced {
			t.Fatal("observation omitted durable identity")
		}
		if obs.Kind == xfercontract.ObserveLedgerSync && (!obs.FileSynced || (obs.PrimaryRevision == 0 && obs.OutboxRevision == 0)) {
			t.Fatal("unproven ledger Sync observation")
		}
	}
	// Cancel/revoke before reservation has a real no-syscall outcome. A granted
	// unresolved receiver instead requires the operator's explicit abandonment.
	for _, abandon := range []bool{false, true} {
		cfg := testConfig(t)
		s := newStore(t, cfg)
		old := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
		if abandon {
			old = update(t, s, old, grantedRow(t, old), xfercontract.MutationReserve)
		}
		next := decidedRow(t, old)
		kind := xfercontract.MutationDecide
		if abandon {
			kind = xfercontract.MutationAbandon
			next.OutcomeUnknown = true
			next.ReceiverOutcome.Outcome = xfercontract.OutcomeInflight
			next.ReceiverOutcome.Code = xfercontract.ReasonSettlementAbandoned
			next.CanonicalTerminal = canonical(t, next, "failed", xfercontract.ReasonSettlementAbandoned)
		}
		old = update(t, s, old, next, kind)
		if got := onlyRow(t, newStore(t, cfg)); got.OutcomeUnknown != abandon || got.CanonicalTerminal.Kind != "failed" {
			t.Fatal("failure/abandonment changed after recovery")
		}
		old = update(t, s, old, confirmedRow(t, old), xfercontract.MutationHistory)
		if err := s.Retire(rowKey(old), old.EntryNonce, retirement(old), testDeadline(t)); err != nil {
			t.Fatal(err)
		}
	}
}

func exerciseSyncFailures(t *testing.T) {
	t.Helper()
	for _, stage := range []string{"file", "directory", "primary_unlink", "outbox_unlink"} {
		t.Run(stage, func(t *testing.T) {
			cfg := testConfig(t)
			s := newStore(t, cfg)
			old := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
			writeFixture(t, cfg.FallbackRoot, old)
			old = update(t, s, old, decidedRow(t, old), xfercontract.MutationDecide)
			concrete := s.(*ledger)
			failure := errors.New("injected disk Sync failure")
			if stage == "file" || stage == "directory" {
				concrete.syncFile = func(f *os.File) error {
					info, err := f.Stat()
					if err != nil {
						return err
					}
					if info.IsDir() == (stage == "directory") {
						return failure
					}
					return f.Sync()
				}
				_, err := s.Update(rowKey(old), old.Revision, xfercontract.LedgerMutation{Kind: xfercontract.MutationHistory, EntryNonce: old.EntryNonce, Row: confirmedRow(t, old)}, testDeadline(t))
				if !errors.Is(err, ErrDurability) || !errors.Is(err, failure) {
					t.Fatalf("%s Sync failure acknowledged: %v", stage, err)
				}
				census, err := s.Scan(testDeadline(t))
				if err == nil || census.Complete {
					t.Fatal("unsynced decision became ready")
				}
				return
			}
			old = update(t, s, old, confirmedRow(t, old), xfercontract.MutationHistory)
			name := rowName(rowKey(old))
			concrete.syncFile = func(f *os.File) error {
				info, err := f.Stat()
				if err != nil {
					return err
				}
				if info.IsDir() {
					path := cfg.Root
					if stage == "outbox_unlink" {
						path = cfg.FallbackRoot
					}
					if _, err := os.Stat(filepath.Join(path, name)); os.IsNotExist(err) {
						return failure
					}
				}
				return f.Sync()
			}
			if err := s.Retire(rowKey(old), old.EntryNonce, retirement(old), testDeadline(t)); !errors.Is(err, failure) {
				t.Fatalf("unlink Sync failure acknowledged: %v", err)
			}
			if stage == "primary_unlink" {
				// Restart in the crash window must replay terminal, never synthesize
				// from a fallback START left over from an earlier admission failure.
				got := onlyRow(t, newStore(t, cfg))
				if !reflect.DeepEqual(got, old) {
					t.Fatal("primary unlink lost current exact outbox barrier")
				}
			}
			concrete.syncFile = (*os.File).Sync
			if err := s.Retire(rowKey(old), old.EntryNonce, retirement(old), testDeadline(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func exerciseRecordGuards(t *testing.T) {
	t.Helper()
	for _, fault := range []string{"empty_id", "nonce", "origin", "permit", "start_digest", "start_reqid", "phase", "tier_version", "budget_start", "budget_rate", "missing_start", "missing_seal", "terminal_without_outcome", "inflight_decision", "grant_identity", "grant_terminal_id", "abandon_complete", "payload_kind", "payload_identity", "payload_null", "canonical_start_time", "terminal_body_outcome", "abandon_missing_flag"} {
		t.Run(fault, func(t *testing.T) {
			cfg := testConfig(t)
			s := newStore(t, cfg)
			row := startRow(t)
			switch fault {
			case "empty_id":
				row.Identity.TransferID = ""
			case "nonce":
				row.EntryNonce = "other"
			case "origin":
				row.Origin.BootID = ""
			case "permit":
				row.Permit.Identity.ActorFP = "other"
			case "start_digest":
				row.CanonicalStart.Digest = "other"
			case "start_reqid":
				row.CanonicalStart.ReqID = "other"
			case "phase":
				row.Phase = "unknown"
			case "tier_version":
				row.Intent.Tier = "a"
			case "budget_start":
				row.BudgetSeal.StartedAt = row.StartedAt.Add(time.Second)
			case "budget_rate":
				row.BudgetSeal.RateLimitBPS++
			case "missing_start":
				row.CanonicalStart = nil
			case "missing_seal":
				row.BudgetSeal.SourceSealed = true
			case "terminal_without_outcome":
				row = decidedRow(t, row)
				row.ReceiverOutcome = nil
			case "inflight_decision":
				row = decidedRow(t, row)
				row.ReceiverOutcome.Outcome = xfercontract.OutcomeInflight
			case "grant_identity":
				row = grantedRow(t, row)
				row.Grant.Identity.ActorFP = "other"
			case "grant_terminal_id":
				row = grantedRow(t, row)
				row.Grant.TerminalID = "other"
			case "abandon_complete":
				row = decidedRow(t, row)
				row.OutcomeUnknown = true
				row.CanonicalTerminal = canonical(t, row, "complete", "")
				row.ReceiverOutcome.Code = xfercontract.ReasonSettlementAbandoned
			case "payload_kind", "payload_identity", "canonical_start_time":
				var audit canonicalAudit
				if err := json.Unmarshal(row.CanonicalStart.Bytes, &audit); err != nil {
					t.Fatal(err)
				}
				switch fault {
				case "payload_kind":
					audit.Kind = "failed"
				case "payload_identity":
					audit.ActorFp = "another-actor"
				case "canonical_start_time":
					audit.Ts = audit.Ts.Add(time.Second)
				}
				data, err := json.Marshal(audit)
				if err != nil {
					t.Fatal(err)
				}
				row.CanonicalStart.Bytes = data
				row.CanonicalStart.Digest = testHash(data)
				row.CanonicalStart.ReqID = testHash(append([]byte("xferaudit-record:"), data...))
			case "payload_null":
				row.CanonicalStart.Bytes = []byte("null")
				row.CanonicalStart.Digest = testHash(row.CanonicalStart.Bytes)
				row.CanonicalStart.ReqID = testHash([]byte("xferaudit-record:null"))
			case "terminal_body_outcome":
				row = decidedRow(t, row)
				row.ReceiverOutcome.Bytes++
			case "abandon_missing_flag":
				row = decidedRow(t, row)
				row.CanonicalTerminal = canonical(t, row, "failed", xfercontract.ReasonSettlementAbandoned)
				row.OutcomeUnknown = true
				row.ReceiverOutcome.Code = xfercontract.ReasonSettlementAbandoned
			}
			row.Revision = 1
			// Exercise decode as well as Update: a corrupt but checksummed row
			// cannot evade validation by arriving after a process restart.
			writeFixture(t, cfg.Root, row)
			census, err := s.Scan(testDeadline(t))
			if err == nil || census.Ready || census.Complete {
				t.Fatal("invalid durable row accepted")
			}
		})
	}
}

func exerciseKeyIsolation(t *testing.T) {
	t.Helper()
	cfg := testConfig(t)
	s := newStore(t, cfg)
	first := update(t, s, RowV1{}, startRow(t), xfercontract.MutationAdmit)
	second := startRow(t)
	second.Identity.SID = "../different/session"
	second.Permit.Identity, second.Retention.Identity = second.Identity, second.Identity
	second.CanonicalStart = canonical(t, second, "start", "")
	update(t, s, RowV1{}, second, xfercontract.MutationAdmit)
	census, err := newStore(t, cfg).Scan(testDeadline(t))
	if err != nil || !census.Complete || len(census.Rows) != 2 {
		t.Fatalf("sessions sharing a transfer ID collided: %v", err)
	}
	// Update results must not grant a caller access to retained byte slices.
	first.CanonicalStart.Bytes[0] = '!'
	if census, err := s.Scan(testDeadline(t)); err != nil || !census.Complete {
		t.Fatalf("caller mutation poisoned owner: %v", err)
	}
	concrete := s.(*ledger)
	concrete.cfg.Clock = func() (string, int64, error) {
		now, err := controlbudget.Now()
		return "other-boot", now, err
	}
	if census, err := s.Scan(testDeadline(t)); err == nil || census.Ready {
		t.Fatal("foreign clock boot accepted")
	}
	for _, fault := range []string{"same_root", "nested_root", "no_clock", "no_root"} {
		bad := cfg
		switch fault {
		case "same_root":
			bad.FallbackRoot = bad.Root
		case "nested_root":
			bad.FallbackRoot = filepath.Join(bad.Root, "outbox")
		case "no_clock":
			bad.Clock = nil
		case "no_root":
			bad.Root = ""
		}
		if _, err := New(bad); err == nil {
			t.Fatalf("invalid config accepted: %s", fault)
		}
	}
}
