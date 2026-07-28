package broker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
)

// xfer_corrupt_recovery_test.go — the two operator recovery actions the unfinalizable-transfer warning
// prescribes must actually WORK, and the one it must not prescribe must be shown to be fatal.
//
// origin: batch B2 independent external review B2-3
//
// WHY A BEHAVIOURAL TEST AND NOT ONLY A STRING TEST
// -------------------------------------------------
// TestUnfinalizableTransferIsReportedOnEveryPass asserts on the LOG TEXT — it is what caught the
// original guidance, and it is the right shape for "this state must be reported on every pass". But a
// string test can only ever pin that the text does not say a known-wrong thing. It cannot tell you that
// the text now says a RIGHT thing, and the external review asked for exactly that distinction:
//
//	"Add a behavioral recovery test, not only a string test."
//
// So this file runs the finalizer through each prescribed action and checks the outcome:
//
//	(1) RECOVERABLE — repair the JSON and RENAME it back to <hash>.json  => the exact terminal replays
//	(2) UNRECOVERABLE — delete ONLY the .corrupt row, KEEP the start row => a synthetic terminal is written
//	(3) THE OLD ADVICE — delete the .corrupt row AND the start row       => NOTHING is ever emitted
//
// Row (3) is the load-bearing one. It is not a hypothetical: it is verbatim what the product told
// operators to do ("remove BOTH the .corrupt file and the matching in-flight row to let the finalizer
// synthesize a terminal"), and it is the one outcome from which there is no recovery — the transfer has
// no terminal and no row to derive one from, permanently. Keeping it as a test means the claim "this
// advice was destructive" is checked rather than asserted.

func TestCorruptOutboxRecoveryActionsBehaveAsDocumented(t *testing.T) {
	cases := []struct {
		name string
		// act performs the operator action on the quarantined outbox row and the primary start row.
		act func(t *testing.T, b *Broker, corruptPath, primaryPath string)
		// wantKind is the terminal kind the NEXT pass must commit; "" means nothing may be committed.
		wantKind string
		why      string
	}{
		{
			name: "recoverable: repair AND rename back to the canonical .json name",
			act: func(t *testing.T, b *Broker, corruptPath, _ string) {
				repaired := repairedTerminalBytes(t, b, corruptTestTID)
				canonical := filepath.Join(b.xferTerminalOutboxDir(), xferInflightFilename(corruptTestTID))
				if err := os.WriteFile(canonical, repaired, 0o600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(corruptPath); err != nil {
					t.Fatal(err)
				}
			},
			wantKind: "complete",
			why: "the operator's repair recovers the REAL terminal (complete), which is the whole point of " +
				"quarantining rather than deleting. A synthetic failed/home_broker_restart here would mean " +
				"the repair was ignored and a transfer that actually succeeded is recorded as failed",
		},
		{
			name: "recoverable: repair the bytes but LEAVE the .corrupt name (what the old text said)",
			act: func(t *testing.T, b *Broker, corruptPath, _ string) {
				repaired := repairedTerminalBytes(t, b, corruptTestTID)
				if err := os.WriteFile(corruptPath, repaired, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			wantKind: "",
			why: "THE FIRST HALF OF THE OLD ADVICE. forEachLedgerRecord only calls fn for names ending in " +
				".json, so a perfectly valid record under a .json.corrupt name is never replayed — and it " +
				"still counts as an unresolved name, so synthesis stays blocked too. The operator did the " +
				"work and nothing happened, forever. This is why the text now says RENAME",
		},
		{
			name: "unrecoverable: delete ONLY the .corrupt row, keep the start row",
			act: func(t *testing.T, b *Broker, corruptPath, _ string) {
				if err := os.Remove(corruptPath); err != nil {
					t.Fatal(err)
				}
			},
			wantKind: "failed",
			why: "with the unreadable name gone the census is clean, the start row is a plain stale start " +
				"row, and the finalizer synthesizes failed/home_broker_restart from it. This is the action " +
				"the warning now prescribes",
		},
		{
			name: "THE OLD ADVICE: delete the .corrupt row AND the start row",
			act: func(t *testing.T, b *Broker, corruptPath, primaryPath string) {
				if err := os.Remove(corruptPath); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(primaryPath); err != nil {
					t.Fatal(err)
				}
			},
			wantKind: "",
			why: "THE SECOND HALF OF THE OLD ADVICE, and the destructive one. The start row is the only " +
				"source of the transfer id / verb / tier / bucket / path / startedAt a deterministic " +
				"terminal is derived from. With both files gone there is no terminal and nothing to " +
				"synthesize one from: the transfer is permanently unauditable. An operator following the " +
				"product's own text reached this state believing they were repairing it",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b, committed := newCorruptOutboxFixture(t)
			corruptPath, primaryPath := quarantineTheOutboxRow(t, b)

			tc.act(t, b, corruptPath, primaryPath)

			// Two further passes: one to act on the operator's change, one to prove the outcome is stable
			// (a replayed terminal must not be emitted twice, and a permanently-stuck transfer must not
			// spontaneously acquire a terminal later).
			for pass := 1; pass <= 2; pass++ {
				if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
					t.Fatalf("pass %d after the operator action: %v", pass, err)
				}
			}

			got := *committed
			if tc.wantKind == "" {
				if len(got) != 0 {
					t.Errorf("expected NO terminal to be committed, got %d: %+v\nwhy it matters: %s",
						len(got), got, tc.why)
				}
				return
			}
			if len(got) != 1 {
				t.Fatalf("expected EXACTLY ONE terminal (the audit invariant), got %d: %+v\nwhy it matters: %s",
					len(got), got, tc.why)
			}
			if got[0].Kind != tc.wantKind {
				t.Errorf("committed %q, want %q\nwhy it matters: %s", got[0].Kind, tc.wantKind, tc.why)
			}
			if got[0].TransferID != corruptTestTID {
				t.Errorf("terminal names transfer %q, want %q", got[0].TransferID, corruptTestTID)
			}
		})
	}
}

const corruptTestTID = "corrupt-outbox-recovery"

// newCorruptOutboxFixture builds a broker whose ledger holds a stale primary start row and an outbox
// row for the same transfer, and returns the slice every committed terminal lands in.
func newCorruptOutboxFixture(t *testing.T) (*Broker, *[]schema.AuditTransfer) {
	t.Helper()
	n, _ := d7SingleNode(t, corruptTestTID)
	now := time.Now().UTC()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{ClusterDataDir: t.TempDir(), Logger: silentLogger(), Now: func() time.Time { return now }}
	b.cl = &clusterRuntime{node: n}

	start := corruptFixtureStart(now)
	if err := b.writeLedgerRecord(b.xferInflightDir(), start); err != nil {
		t.Fatalf("seed primary start: %v", err)
	}
	staged := start
	staged.Terminal = corruptFixtureTerminal(now)
	if err := b.writeLedgerRecord(b.xferTerminalOutboxDir(), staged); err != nil {
		t.Fatalf("seed exact outbox terminal: %v", err)
	}

	committed := &[]schema.AuditTransfer{}
	b.transferAuditForwardSync = func(payload []byte) error {
		var rec schema.AuditTransfer
		if err := json.Unmarshal(payload, &rec); err != nil {
			t.Fatal(err)
		}
		*committed = append(*committed, rec)
		return nil
	}
	return b, committed
}

// quarantineTheOutboxRow corrupts the staged outbox row and runs ONE pass, which is what renames it to
// <name>.json.corrupt. Returns the quarantined path and the primary row's path — the two files every
// prescribed operator action manipulates.
//
// Running a real pass rather than writing the .corrupt file directly matters: the quarantine name is
// chosen by production code (and gains a nanosecond suffix if a .corrupt already exists), so a test that
// synthesized the name would stop tracking it.
func quarantineTheOutboxRow(t *testing.T, b *Broker) (corruptPath, primaryPath string) {
	t.Helper()
	outboxRow := filepath.Join(b.xferTerminalOutboxDir(), xferInflightFilename(corruptTestTID))
	if err := os.WriteFile(outboxRow, []byte("{ truncated"), 0o600); err != nil {
		t.Fatalf("corrupt the outbox row: %v", err)
	}
	if _, err := b.finalizeStrandedXfers(context.Background()); err != nil {
		t.Fatalf("quarantine pass: %v", err)
	}
	matches, err := filepath.Glob(outboxRow + ".corrupt*")
	if err != nil || len(matches) != 1 {
		t.Fatalf("expected exactly one quarantined sibling after the first pass, got %v (%v)", matches, err)
	}
	primaryPath = filepath.Join(b.xferInflightDir(), xferInflightFilename(corruptTestTID))
	if _, err := os.Stat(primaryPath); err != nil {
		t.Fatalf("the primary start row must survive the quarantine pass: %v", err)
	}
	return matches[0], primaryPath
}

// repairedTerminalBytes is what a successful operator repair produces: the ledger record the corrupt
// file was supposed to contain, re-serialised. It is built from the same fields the fixture staged, so a
// replay of it is indistinguishable from the row never having been corrupted.
func repairedTerminalBytes(t *testing.T, b *Broker, tid string) []byte {
	t.Helper()
	rec := corruptFixtureStart(b.cfg.Now())
	rec.TransferID = tid
	rec.Terminal = corruptFixtureTerminal(b.cfg.Now())
	out, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func corruptFixtureStart(now time.Time) xferInflightRecord {
	return xferInflightRecord{
		TransferID: corruptTestTID, Session: "sess", Node: "n1", Verb: "push", Tier: "a",
		Bucket: "OBJ_xfer-sess", Path: "/dst",
		StartedAt: now.Add(-transferTimeoutTierA - xferStrandedSlack - time.Second),
	}
}

func corruptFixtureTerminal(now time.Time) *schema.AuditTransfer {
	return &schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", Ts: now,
		Session: "sess", Node: "n1", TransferID: corruptTestTID,
		Path: "/dst", Tier: "a", Bucket: "OBJ_xfer-sess",
	}
}
