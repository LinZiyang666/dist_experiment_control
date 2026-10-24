package xferhistory

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

func cap0HistoryRecord(t *testing.T, sid, id, kind string, change func(*schema.AuditTransfer)) xfercontract.CanonicalRecord {
	t.Helper()
	audit := schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: kind, Verb: "pull", Tier: "a",
		Ts: time.Date(2026, 10, 21, 1, 2, 3, 0, time.UTC), Session: sid, Node: "node", ActorFp: "actor",
		TransferID: id, EntryNonce: "CAP0NONCE", BestEffort: true}
	if kind == "start" {
		audit.Tier = "?"
	} else {
		audit = schema.BindLegacyTerminal(audit, audit.EntryNonce)
		audit.Bytes = 7
	}
	if change != nil {
		change(&audit)
	}
	record, err := schema.CanonicalTransferAudit(audit)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// A session mixes cap0 and v1 transfers. A best-effort cap0 row with a retained
// admission identity must be readable by every exact scan — otherwise one cap0
// transfer would leave every later v1 confirmation in its session unknown —
// while a cap0 row claiming v1 receiver authority stays unreadable and fails
// the scan closed instead of being skipped.
// origin: hot-update S7 plan §4 / §5.2 / §9 (S7 integration oplegacy F2)
func TestTransferHistoryScansBestEffortCap0BesideV1(t *testing.T) {
	js, stream := historyFixture(t, "mixed", nil)
	p := newPublisher(t, js, "mixed")
	for _, kind := range []string{"start", "complete"} {
		record := cap0HistoryRecord(t, "mixed", "legacy", kind, nil)
		receipt, err := p.Confirm(record, historyDeadline(t))
		if err != nil || receipt.RecordDigest != record.Digest || receipt.Sequence == 0 {
			t.Fatalf("exact cap0 %s was not confirmed: %+v %v", kind, receipt, err)
		}
		again, err := p.Confirm(record, historyDeadline(t))
		if err != nil || again != receipt {
			t.Fatalf("exact cap0 %s replay changed its receipt: %+v %v", kind, again, err)
		}
	}
	for _, kind := range []string{"start", "complete"} {
		if _, err := p.Confirm(historyRecord(t, "mixed", "capable", kind, nil), historyDeadline(t)); err != nil {
			t.Fatalf("a cap0 row in the session blocked v1 %s confirmation: %v", kind, err)
		}
	}
	assertRawHistory(t, stream, "mixed", "legacy", 1, 1)
	assertRawHistory(t, stream, "mixed", "capable", 1, 1)
	set, err := p.Lookup(xfercontract.Key{SID: "mixed", TransferID: "legacy"}, historyDeadline(t))
	if err != nil || !set.Complete || len(set.Start) != 1 || len(set.Terminal) != 1 {
		t.Fatalf("cap0 lookup = %+v, %v", set, err)
	}
	if id := set.Terminal[0].Identity; id.EntryNonce != "CAP0NONCE" || id.AgentInstance != "" || id.IntentDigest != "" || id.ObjectVersion != "" {
		t.Fatalf("cap0 history identity certified v1 authority: %+v", id)
	}
	if _, err := p.Confirm(cap0HistoryRecord(t, "mixed", "legacy", "failed", nil), historyDeadline(t)); !errors.Is(err, ErrConflict) {
		t.Fatalf("an opposite cap0 decision got a second terminal slot: %v", err)
	}
	forged := cap0HistoryRecord(t, "mixed", "forged", "complete", nil)
	body := strings.Replace(string(forged.Bytes), `"entry_nonce"`, `"agent_instance":"agent","entry_nonce"`, 1)
	if _, err := js.Publish(t.Context(), proto.SubjAuditTransfer("mixed"), []byte(body)); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Confirm(historyRecord(t, "mixed", "after", "start", nil), historyDeadline(t)); !errors.Is(err, ErrUnknown) {
		t.Fatalf("a cap0 row claiming v1 authority was skipped instead of failing the scan closed: %v", err)
	}
	assertRawHistory(t, stream, "mixed", "after", 0, 0)
}
