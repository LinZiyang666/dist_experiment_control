package xfergc

import (
	"errors"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"testing"
	"time"
)

type checkpointJournal struct {
	rows      []RetirementCheckpoint
	consumed  []RetirementCheckpoint
	limit     int
	uncertain bool
}

func (j *checkpointJournal) ScanRetirementCheckpoints(limit int, _ x.Deadline) ([]RetirementCheckpoint, error) {
	j.limit = limit
	return j.rows[:min(limit, len(j.rows))], nil
}
func (j *checkpointJournal) ConsumeRetirementCheckpoint(checkpoint RetirementCheckpoint, _ x.Deadline) error {
	if j.uncertain {
		return errors.New("durable deletion unconfirmed")
	}
	j.consumed = append(j.consumed, checkpoint)
	return nil
}

// Every retained responsibility is independently necessary. An uncertain
// origin or checkpoint cannot become an age-based deletion permission.
// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
func TestTransferRetirementJournalRequiresAuthoritativeAbsence(t *testing.T) {
	id := x.Identity{SID: "session", TransferID: "transfer", EntryNonce: "entry", ObjectVersion: "version"}
	cp := RetirementCheckpoint{Identity: id, Digest: "checkpoint"}
	all := RetirementResponsibility{Identity: id, Authoritative: true, PrimaryAbsent: true, OutboxAbsent: true, CourierAbsent: true, PermitExpired: true, ResultExpired: true}
	for _, reason := range []string{"authority", "primary", "outbox", "courier", "permit", "result", "identity", "delete_unknown", "none"} {
		t.Run(reason, func(t *testing.T) {
			journal := &checkpointJournal{rows: []RetirementCheckpoint{cp}}
			evidence := all
			switch reason {
			case "authority":
				evidence.Authoritative = false
			case "primary":
				evidence.PrimaryAbsent = false
			case "outbox":
				evidence.OutboxAbsent = false
			case "courier":
				evidence.CourierAbsent = false
			case "permit":
				evidence.PermitExpired = false
			case "result":
				evidence.ResultExpired = false
			case "identity":
				evidence.Identity.EntryNonce = "replacement"
			case "delete_unknown":
				journal.uncertain = true
			}
			err := CollectRetirementCheckpoints(journal, func(x.Identity, x.Deadline) (RetirementResponsibility, error) { return evidence, nil }, gcDeadline(t, 5*time.Second))
			if reason == "none" {
				if err != nil || len(journal.consumed) != 1 {
					t.Fatalf("authoritatively absent checkpoint retained: %v", err)
				}
				return
			}
			if len(journal.consumed) != 0 {
				t.Fatal("uncertain responsibility deleted its retirement checkpoint")
			}
		})
	}
}

// origin: hot-update S7 plan §5.3 / s7-p9m remaining request 4
func TestTransferRetirementJournalCollectionHasFixedPassBound(t *testing.T) {
	journal := &checkpointJournal{rows: make([]RetirementCheckpoint, 1000)}
	calls := 0
	_ = CollectRetirementCheckpoints(journal, func(id x.Identity, _ x.Deadline) (RetirementResponsibility, error) {
		calls++
		return RetirementResponsibility{}, errors.New("unknown origin")
	}, gcDeadline(t, 5*time.Second))
	if journal.limit != 64 || calls > 64 || len(journal.consumed) != 0 {
		t.Fatalf("unbounded or unsafe journal pass: limit=%d calls=%d", journal.limit, calls)
	}
}
