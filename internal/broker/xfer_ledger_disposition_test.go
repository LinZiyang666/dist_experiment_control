package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/schema"
)

// xfer_ledger_disposition_test.go — row-by-row coverage of the durable-precedence rule that used to
// exist only as the branch order inside finalizeStrandedXfers (B8 half B).
//
// WHY A TABLE AND NOT MORE CRASH FIXTURES
// ---------------------------------------
// Every combination below is a real crash sequence, but staging each one on disk costs a temp dir, a
// fake clock, a tracker and a raft node, so the existing coverage reached the states someone thought
// to stage. The rule is now a pure function, so the states are enumerable. Two of them
// (census-failed × start-only, and census-failed × staged-terminal) differ in the ONE way that
// matters and were previously only distinguishable by reading the branch order.
//
// THE REASON STRING IS PART OF THE ASSERTION, deliberately. A disposition can be right for the wrong
// reason — "leave this row alone" is the answer for a live transfer, for a fresh transfer, for an
// unreadable census and for an outranking terminal — and a rule that returns the right verdict from
// the wrong branch will return the wrong verdict as soon as the inputs shift. Asserting the branch is
// how the table tells those four apart.
func TestLedgerRowDisposition(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	// Tier "a" ⇒ 30s timeout + 60s slack = 90s. "stale" is comfortably past it; "fresh" is inside it.
	stale := now.Add(-(transferTimeoutTierA + xferStrandedSlack + time.Second))
	fresh := now.Add(-time.Second)
	terminal := &schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", TransferID: "t1",
	}

	row := func(startedAt time.Time, term *schema.AuditTransfer) xferInflightRecord {
		return xferInflightRecord{TransferID: "t1", Tier: "a", StartedAt: startedAt, Terminal: term}
	}

	cases := []struct {
		name     string
		in       ledgerRowInputs
		want     ledgerDisposition
		wantWhy  string // substring — the BRANCH that must have fired
		mutation string // the injection this row is the detector for
	}{
		// ---- the start-only row: the ONLY path that can manufacture a terminal ----
		{
			name: "primary/start/stale/census-ok/no-outbox-terminal ⇒ synthesize",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(stale, nil),
				OutboxCensusOK: true, Now: now},
			want: ledgerSynthesize, wantWhy: "stranded past timeout",
			mutation: "this is the only row that reaches #57's synthesizer at all",
		},
		{
			name: "primary/start/stale/CENSUS-FAILED ⇒ leave (R6-F1)",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(stale, nil),
				OutboxCensusOK: false, Now: now},
			want: ledgerLeave, wantWhy: "outbox census failed",
			mutation: "delete the !OutboxCensusOK gate ⇒ this row synthesizes a failure beside a terminal " +
				"that was merely unreadable",
		},
		{
			name: "primary/start/FRESH/census-ok ⇒ leave (slow, not stranded)",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(fresh, nil),
				OutboxCensusOK: true, Now: now},
			want: ledgerLeave, wantWhy: "younger than its tier timeout",
			mutation: "delete the freshness gate ⇒ every in-flight transfer is finalized as failed on the " +
				"first pass after a restart",
		},
		{
			name: "primary/start/stale/census-ok/outbox HAS terminal ⇒ leave (outranked)",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(stale, nil),
				OutboxHasExactTerminal: true, OutboxCensusOK: true, Now: now},
			want: ledgerLeave, wantWhy: "outranks this start row",
			mutation: "drop the outboxOwned consultation ⇒ a contradictory pair: the outbox's exact " +
				"terminal AND a synthetic failure for the same transfer",
		},
		{
			name: "primary/start/stale/census-ok/LIVE ⇒ leave (live outranks everything)",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(stale, nil),
				Live: true, OutboxCensusOK: true, Now: now},
			want: ledgerLeave, wantWhy: "live tracker entry",
			mutation: "move the Live check below the staleness gate ⇒ a transfer that is genuinely running " +
				"right now is declared failed",
		},
		{
			name: "primary/start/stale/census-ok/outbox ROW UNREADABLE ⇒ leave (row-level twin of R6-F1)",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(stale, nil),
				OutboxRowUnreadable: true, OutboxCensusOK: true, Now: now},
			want: ledgerLeave, wantWhy: "UNREADABLE row for this transfer",
			mutation: "drop the OutboxRowUnreadable gate ⇒ a decided terminal that merely failed to PARSE " +
				"reads as an absent terminal, and a contradicting synthetic failure is committed while the " +
				"real outcome sits in a quarantined .corrupt file. The directory-level census cannot see " +
				"this: os.ReadDir succeeded and only the row did not.",
		},

		// ---- a terminal staged in place: replayed WITHOUT consulting the outbox ----
		{
			name: "primary/terminal ⇒ replay",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(stale, terminal),
				OutboxCensusOK: true, Now: now},
			want: ledgerReplay, wantWhy: "staged in place",
		},
		{
			name: "primary/terminal/CENSUS-FAILED ⇒ replay anyway (the census gates only the start row)",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(stale, terminal),
				OutboxCensusOK: false, Now: now},
			want: ledgerReplay, wantWhy: "staged in place",
			mutation: "hoist the census gate above the staged-terminal branch ⇒ an already-DECIDED terminal " +
				"is withheld because an unrelated directory is unreadable, and the audit keeps a dangling " +
				"start row for as long as that lasts",
		},
		{
			name: "primary/terminal/FRESH ⇒ replay anyway (a decided outcome has nothing to wait for)",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(fresh, terminal),
				OutboxCensusOK: true, Now: now},
			want: ledgerReplay, wantWhy: "staged in place",
			mutation: "apply the freshness gate to staged terminals ⇒ the post-commit crash window stays " +
				"open for a whole timeout",
		},
		{
			name: "primary/terminal/LIVE ⇒ leave",
			in: ledgerRowInputs{Site: ledgerSitePrimary, Rec: row(stale, terminal),
				Live: true, OutboxCensusOK: true, Now: now},
			want: ledgerLeave, wantWhy: "live tracker entry",
		},

		// ---- the outbox site: same rule, different gate order ----
		{
			name: "outbox/terminal ⇒ replay",
			in:   ledgerRowInputs{Site: ledgerSiteOutbox, Rec: row(stale, terminal), Now: now},
			want: ledgerReplay, wantWhy: "staged in the outbox",
		},
		{
			name: "outbox/terminal/LIVE ⇒ leave",
			in:   ledgerRowInputs{Site: ledgerSiteOutbox, Rec: row(stale, terminal), Live: true, Now: now},
			want: ledgerLeave, wantWhy: "live tracker entry",
		},
		{
			name: "outbox/start-only ⇒ leave (the outbox stages terminals only)",
			in:   ledgerRowInputs{Site: ledgerSiteOutbox, Rec: row(stale, nil), Now: now},
			want: ledgerLeave, wantWhy: "outbox row carries no terminal",
			mutation: "let the outbox site fall through to the synthesizer ⇒ a start row that landed in the " +
				"fallback dir is finalized as failed with no timeout discipline at all",
		},
		{
			name: "outbox/start-only/FRESH ⇒ leave (freshness is irrelevant at this site)",
			in:   ledgerRowInputs{Site: ledgerSiteOutbox, Rec: row(fresh, nil), Now: now},
			want: ledgerLeave, wantWhy: "outbox row carries no terminal",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, why := ledgerRowDisposition(tc.in)
			if got != tc.want {
				t.Fatalf("disposition = %v, want %v (reason given: %q)\nmutation this row detects: %s",
					got, tc.want, why, tc.mutation)
			}
			if !strings.Contains(why, tc.wantWhy) {
				t.Errorf("the RIGHT verdict from the WRONG branch: reason = %q, must contain %q.\n"+
					"A disposition that is correct by accident stops being correct as soon as the inputs shift.",
					why, tc.wantWhy)
			}
		})
	}
}

// TestLedgerRowDispositionSynthesizeIsReachableOnlyFromOneShape is the non-vacuity companion: exactly
// one shape in the whole input space may manufacture a terminal. It is asserted over SYNTHESIZED
// inputs rather than by counting anything on disk, so it cannot go blind.
func TestLedgerRowDispositionSynthesizeIsReachableOnlyFromOneShape(t *testing.T) {
	now := time.Date(2026, 7, 27, 12, 0, 0, 0, time.UTC)
	stale := now.Add(-(transferTimeoutTierA + xferStrandedSlack + time.Second))
	terminal := &schema.AuditTransfer{V: schema.AuditSchemaVersion, Kind: "complete", TransferID: "t1"}

	synthesizing := 0
	total := 0
	for _, site := range []ledgerSite{ledgerSiteOutbox, ledgerSitePrimary} {
		for _, term := range []*schema.AuditTransfer{nil, terminal} {
			for _, live := range []bool{false, true} {
				for _, owned := range []bool{false, true} {
					for _, census := range []bool{false, true} {
						for _, rowBad := range []bool{false, true} {
							total++
							d, _ := ledgerRowDisposition(ledgerRowInputs{
								Site: site,
								Rec: xferInflightRecord{
									TransferID: "t1", Tier: "a", StartedAt: stale, Terminal: term,
								},
								Live: live, OutboxHasExactTerminal: owned, OutboxCensusOK: census,
								OutboxRowUnreadable: rowBad, Now: now,
							})
							if d != ledgerSynthesize {
								continue
							}
							synthesizing++
							// Assert the shape rather than just the count, so a rule that starts synthesizing
							// from a DIFFERENT single shape is still caught.
							if site != ledgerSitePrimary || term != nil || live || owned || !census || rowBad {
								t.Errorf("synthesize reached from an illegal shape: site=%v terminal=%v live=%v "+
									"outboxOwned=%v censusOK=%v rowUnreadable=%v",
									site, term != nil, live, owned, census, rowBad)
							}
						}
					}
				}
			}
		}
	}
	if total != 64 {
		t.Fatalf("the sweep is not covering the input space: %d combinations, want 64", total)
	}
	if synthesizing != 1 {
		t.Fatalf("exactly ONE of the %d stale combinations may manufacture a terminal; %d did.\n"+
			"Every additional one is a path that can commit a failure for a transfer whose real outcome "+
			"is recorded somewhere this pass did not look.", total, synthesizing)
	}
}
