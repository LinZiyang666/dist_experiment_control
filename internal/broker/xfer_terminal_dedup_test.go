package broker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
	"github.com/LinZiyang666/tether/internal/xferaudit"
)

// xfer_terminal_dedup_test.go — the first publish of a transfer terminal and its
// crash-recovery replay are the SAME message.
//
// origin: prerelease audit external review M-3. A single broker stages the decided
// terminal into its ledger and then publishes it; a crash between those two steps left a
// transfer with no terminal at all, and replayStagedTerminal deliberately RETAINED the row
// forever rather than replay it. The reason was real: replaying needed a
// duplicate-suppression key, JetStream dedups only on Nats-Msg-Id, and pubAuditTransfer
// set none — so a replay after a crash in the OTHER window (published, not yet unlinked)
// would have produced a second, contradictory terminal. Fixing only the replay side would
// have swapped a dangling start for a corrupted record.
//
// Both sides now derive the id from the record's own content, which is what lets a single
// rule cover both windows. These tests pin the id AGREEMENT, not just its presence — an id
// that differs between the two paths dedups nothing while looking like it does.

// idCapturingJS records the subject and Nats-Msg-Id of every publish.
type idCapturingJS struct {
	jetstream.JetStream
	mu   sync.Mutex
	subs []string
	ids  []string
}

func (c *idCapturingJS) PublishMsg(_ context.Context, m *nats.Msg, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.subs = append(c.subs, m.Subject)
	c.ids = append(c.ids, m.Header.Get(jetstream.MsgIDHeader))
	return &jetstream.PubAck{}, nil
}

// StreamNameBySubject reports NO stream carrying the audit subject, i.e. "this terminal has
// not been committed". These tests are about the dedup ID that recovery uses when it decides
// to publish; the durable already-committed lookup (external review R2-M2) has its own
// coverage in the reviewer's dedup-window guard. Without this method the embedded nil
// interface panics — the same half-implemented-double failure `countingJS` hit one method
// over, which is why it is spelled out rather than left to the embedding.
func (c *idCapturingJS) StreamNameBySubject(context.Context, string) (string, error) {
	return "", jetstream.ErrStreamNotFound
}

func (c *idCapturingJS) snapshot() ([]string, []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.subs...), append([]string(nil), c.ids...)
}

func dedupTestTerminal(now time.Time) schema.AuditTransfer {
	return schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push",
		Ts: now, Session: "lab", Node: "lab-1", TransferID: "tid-dedup", Tier: "a",
	}
}

// singleModeDedupBroker is a broker with NO cluster runtime (b.cl == nil) — the shape the
// finding is about — and a JetStream double that records dedup ids.
func singleModeDedupBroker(t *testing.T, now time.Time) (*Broker, *idCapturingJS, string) {
	t.Helper()
	root := t.TempDir()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: root, Logger: silentLogger(), Now: func() time.Time { return now }}
	js := &idCapturingJS{}
	setBrokerJS(b, js)
	t.Cleanup(func() { setBrokerJS(b, nil) })
	return b, js, root
}

// TestTheFirstTerminalAndItsReplayCarryTheSameDedupID is the whole of M-3 in one
// assertion. Window 1 (crash before publish) is covered by the replay emitting at all;
// window 2 (crash after publish, before unlink) is covered by the two ids agreeing, since
// that is exactly what makes JetStream collapse the second one.
func TestTheFirstTerminalAndItsReplayCarryTheSameDedupID(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	b, js, root := singleModeDedupBroker(t, now)
	rec := dedupTestTerminal(now)

	// --- the normal first publish ---
	b.pubAuditTransfer(rec)

	// --- the crash-recovery replay of the SAME staged terminal ---
	staged := xferInflightRecord{
		TransferID: rec.TransferID, Session: rec.Session, Node: rec.Node,
		Verb: rec.Verb, Tier: rec.Tier, Bucket: "OBJ_xfer-lab", Path: "/dst",
		StartedAt: now.Add(-time.Minute),
		Terminal:  &rec,
	}
	if !b.replayStagedTerminal(context.Background(), staged) {
		t.Fatal("a single broker must now DISPOSE of a staged terminal by replaying it. " +
			"Retaining leaves a decided transfer with no terminal at all and a ledger row " +
			"that nothing ever drains — the OPEN this closes.")
	}

	subs, ids := js.snapshot()
	if len(ids) != 2 {
		t.Fatalf("want exactly two publishes (first + replay), got %d: %v", len(ids), subs)
	}
	for i, s := range subs {
		if want := proto.SubjAuditTransfer(rec.Session); s != want {
			t.Fatalf("publish %d went to %q, want %q", i, s, want)
		}
	}
	if ids[0] == "" {
		t.Fatal("the FIRST terminal publish carried no Nats-Msg-Id. JetStream dedups on that " +
			"header and never on payload bytes, so without it the recovery replay becomes a " +
			"second, contradictory terminal that no consumer can resolve.")
	}
	if ids[0] != ids[1] {
		t.Fatalf("first publish id %q != replay id %q. Two different ids dedup NOTHING while "+
			"looking like they do; the crash-after-publish window would emit a duplicate.",
			ids[0], ids[1])
	}
	want, err := xferaudit.TransferRecordReqID(rec)
	if err != nil {
		t.Fatal(err)
	}
	if ids[0] != want {
		t.Fatalf("dedup id %q is not derived from the record content (want %q); an id that is "+
			"not content-derived cannot be reproduced by a process that has only the ledger",
			ids[0], want)
	}
	_ = root
}

// TestADifferentTerminalGetsADifferentDedupID is the negative control. If the id were
// keyed on the transfer id alone, a genuinely different outcome for the same transfer —
// the failed terminal after a retried complete, say — would be silently dedup-SKIPPED, and
// the audit trail would keep the wrong one.
func TestADifferentTerminalGetsADifferentDedupID(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	b, js, _ := singleModeDedupBroker(t, now)

	complete := dedupTestTerminal(now)
	failed := complete
	failed.Kind = "failed"

	b.pubAuditTransfer(complete)
	b.pubAuditTransfer(failed)

	_, ids := js.snapshot()
	if len(ids) != 2 {
		t.Fatalf("want two publishes, got %d", len(ids))
	}
	if ids[0] == ids[1] {
		t.Fatalf("a complete and a failed terminal for the same transfer share dedup id %q; "+
			"the second would be discarded as a duplicate", ids[0])
	}
}

// TestAStagedTerminalIsRetainedWhenItsIDCannotBeDerived pins the ONE case that still
// retains. "Cannot prove this is not a duplicate" must mean retain-and-retry, never
// publish-and-hope: a retained row is detectable and repairable, a duplicate terminal is
// not.
func TestAStagedTerminalIsRetainedWhenItsIDCannotBeDerived(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	b, js, _ := singleModeDedupBroker(t, now)

	// A record with no Terminal is not a staged-terminal row at all; the replay must
	// decline it rather than invent one.
	staged := xferInflightRecord{
		TransferID: "tid-no-terminal", Session: "lab", Node: "lab-1",
		Verb: "push", Tier: "a", Bucket: "OBJ_xfer-lab", Path: "/dst",
		StartedAt: now.Add(-time.Minute),
	}
	if b.replayStagedTerminal(context.Background(), staged) {
		t.Fatal("a row with no staged terminal must not be reported as disposed")
	}
	if _, ids := js.snapshot(); len(ids) != 0 {
		t.Fatalf("nothing may be published for a row with no staged terminal, got %v", ids)
	}
}

// TestStagedTerminalSurvivesADiskRoundTrip guards the assumption the dedup argument rests
// on: the ledger row a recovering process reads back must marshal to the same bytes the
// crashed one staged, or the derived id differs and the replay duplicates.
func TestStagedTerminalSurvivesADiskRoundTrip(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	rec := dedupTestTerminal(now)
	staged := xferInflightRecord{
		TransferID: rec.TransferID, Session: rec.Session, Node: rec.Node,
		Verb: rec.Verb, Tier: rec.Tier, Bucket: "OBJ_xfer-lab", Path: "/dst",
		StartedAt: now.Add(-time.Minute), Terminal: &rec,
	}
	body, err := json.Marshal(staged)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "row.json")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	back, err := readXferInflight(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if back.Terminal == nil {
		t.Fatal("the staged terminal did not survive the round trip")
	}
	before, err := xferaudit.TransferRecordReqID(rec)
	if err != nil {
		t.Fatal(err)
	}
	after, err := xferaudit.TransferRecordReqID(*back.Terminal)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("the dedup id changed across the ledger round trip (%q -> %q); a recovering "+
			"process would then publish a SECOND terminal instead of a collapsing duplicate",
			before, after)
	}
}
