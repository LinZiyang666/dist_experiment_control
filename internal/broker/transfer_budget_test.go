package broker

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/schema"
)

// transfer_budget_test.go — the broker half of the size-derived tier-B budget: the watchdog, the
// crash-recovery stranded decision, and the leader's cross-home GC floor. All three read the same
// size; the point of the batch is that they cannot disagree.

// TestWatchdogAndStrandedDecisionUseTheSameBudget. The live watchdog and the recovering broker must
// agree, or crash recovery writes a `failed` terminal for a transfer the crashed process would still
// have been waiting on — the "healthy but slow" case this whole batch is about.
//
// Mutation: make transferTimeoutFor ignore size (return the tier floor) — reddens at every size past
// the floor.
func TestWatchdogAndStrandedDecisionUseTheSameBudget(t *testing.T) {
	// origin: batch-c internal review tests-F5 — `live` reads what startTransferWatchdog ACTUALLY arms
	// (watchdogBudget), not a restatement of the formula. The previous version compared
	// proto.XferBudget(...) to transferTimeoutFor(...) and therefore stayed green when the real arm
	// site was changed to cover one leg instead of two.
	for _, size := range []int64{0, 1, 64 * 1024 * 1024, 512 * 1024 * 1024, proto.XferMaxBytes} {
		live := watchdogBudget(&transferEntry{tier: "b", size: size})
		recovered := transferTimeoutFor("b", size)
		if live != recovered {
			t.Errorf("size=%d: the live watchdog budgets %s but a recovering broker budgets %s — the "+
				"shorter one declares a healthy transfer stranded", size, live, recovered)
		}
	}
	// A ledger record written before batch C carries no size. It must degrade to the fixed floor,
	// never to a zero budget (which would declare every pre-upgrade record instantly stranded).
	if got := transferTimeoutFor("b", 0); got != transferTimeoutTierB {
		t.Errorf("a pre-batch-C ledger record (no size) must budget the fixed tier floor %s, got %s",
			transferTimeoutTierB, got)
	}
	if got := transferTimeoutFor("a", proto.XferMaxBytes); got != transferTimeoutTierA {
		t.Errorf("tier A stays fixed, got %s", got)
	}
}

// TestSyntheticTerminalTimestampStaysOnTheTierFloor is the deliberate NON-alignment, and it is the
// one place the two questions genuinely differ.
//
// The synthetic terminal's Ts is the dedup carrier: TransferRecordReqID hashes the whole normalized
// record, so two incarnations that disagree about Ts mint two different reqIDs for one transfer and
// the replicated dedup ledger cannot collapse them — the audit then claims the same transfer failed
// twice, at two timestamps. If Ts tracked the size-derived budget it would depend on
// XferMinThroughput, so a ROLLBACK (or any later retune of that constant) would have the old and new
// binaries stamp different Ts from the identical ledger record.
//
// Mutation: change transferTierFloorFor to return transferTimeoutFor(tier, size) — reddens.
func TestSyntheticTerminalTimestampStaysOnTheTierFloor(t *testing.T) {
	if got := transferTierFloorFor("b"); got != transferTimeoutTierB {
		t.Errorf("tier-B floor = %s, want the fixed %s", got, transferTimeoutTierB)
	}
	if got := transferTierFloorFor("a"); got != transferTimeoutTierA {
		t.Errorf("tier-A floor = %s, want %s", got, transferTimeoutTierA)
	}
	// origin: batch-c internal review tests-F4. What was here compared f(x) to f(x) in its second
	// conjunct — tautologically false, so the loop body was dead — and in any case it only exercised
	// the helper, while the decision it guards lives at the ONE call site in finalizeStrandedXfers.
	// Assert the call site structurally instead: the synthetic terminal's Ts must be stamped from the
	// tier floor, never from the size-derived budget.
	src, err := os.ReadFile("xfer_inflight.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	fset := token.NewFileSet()
	f, perr := parser.ParseFile(fset, "xfer_inflight.go", src, 0)
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	var floorCalls, budgetCalls int
	ast.Inspect(f, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 || len(as.Lhs) != 1 {
			return true
		}
		id, ok := as.Lhs[0].(*ast.Ident)
		if !ok || id.Name != "timeout" {
			return true
		}
		call, ok := as.Rhs[0].(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fn, _ := call.Fun.(*ast.Ident); {
		case fn != nil && fn.Name == "transferTierFloorFor":
			floorCalls++
		case fn != nil && fn.Name == "transferTimeoutFor":
			budgetCalls++
		}
		return true
	})
	if floorCalls == 0 {
		t.Fatal("SELF-CHECK FAILED: no `timeout := transferTierFloorFor(...)` found in xfer_inflight.go — " +
			"the scanner is looking for a shape that no longer exists, so a clean result means nothing")
	}
	if budgetCalls > 0 {
		t.Errorf("the synthetic terminal's timeout is derived from transferTimeoutFor (the size-derived "+
			"budget) at %d site(s). Ts would then depend on XferMinThroughput, so a rollback or any "+
			"future retune of that constant makes two binaries stamp different Ts for the SAME ledger "+
			"record — two reqIDs, two contradictory terminals for one transfer.", budgetCalls)
	}
}

// TestCrossHomeGraceCoversALiveTransferOfThatSize is the load-bearing relation of the whole change:
// the LEADER must never delete an object that the owning home's watchdog is still covering.
//
// Before batch C one global constant sufficed (15m > a fixed 5m watchdog). With a size-derived
// watchdog reaching 35m08s it does not, and the fix has to be per-object — raising the global
// constant was rejected because serveconf.MinXferCrossHomeReapAge is a HARD floor enforced on
// production YAML, so a broker that set the knob explicitly would REFUSE TO START after an upgrade.
//
// Mutation: make xferCrossHomeExtraFor return 0 — the large-size rows redden.
func TestCrossHomeGraceCoversALiveTransferOfThatSize(t *testing.T) {
	for _, size := range []int64{0, 1, 64 * 1024 * 1024, 512 * 1024 * 1024, proto.XferMaxBytes} {
		grace := xferCrossHomeReapAge + xferCrossHomeExtraFor(size)
		budget := proto.XferBudget("b", size, proto.XferPushLegs)
		if grace <= budget {
			t.Errorf("size=%d: the cross-home GC would delete at %s while the owning home's watchdog "+
				"still covers the transfer until %s", size, grace, budget)
		}
	}
	// The historical margin over the tier floor is preserved for everything small.
	if xferCrossHomeReapAge != 3*transferTimeoutTierB {
		t.Errorf("the base floor must stay 3x the tier-B floor (%s), got %s — that is the clock-skew "+
			"margin the leader needs to judge another broker's ModTime", 3*transferTimeoutTierB, xferCrossHomeReapAge)
	}
}

// TestCrossHomeExtraIsAnIncrementNotAFloor. If the per-object grace were an ABSOLUTE floor it would
// (a) override the serveconf knob and the deploy-tier drill that COMPRESSES it to seconds, and
// (b) give every small object the worst object's grace on exactly the small-disk brokers that can
// least afford holding garbage.
//
// Mutation: make xferCrossHomeExtraFor return the absolute floor instead of the increment — the
// "small object earns nothing" assertion reddens.
func TestCrossHomeExtraIsAnIncrementNotAFloor(t *testing.T) {
	for _, size := range []int64{0, 1, 1024, proto.XferTierAMaxBytes, 32 * 1024 * 1024} {
		if extra := xferCrossHomeExtraFor(size); extra != 0 {
			t.Errorf("size=%d earned %s of extra cross-home grace; anything whose budget is the plain "+
				"tier floor must earn NOTHING, or a compressed drill floor stops working and small "+
				"objects linger", size, extra)
		}
	}
	// A maximum-size object earns exactly what its own budget bought beyond the floor.
	wantExtra := proto.XferTierBMaxBudget - transferTimeoutTierB
	if got := xferCrossHomeExtraFor(proto.XferMaxBytes); got != wantExtra {
		t.Errorf("a 2 GiB object earned %s, want %s (its budget minus the tier floor)", got, wantExtra)
	}
	// And the increment is monotonic, so a bigger object never earns less grace.
	prev := time.Duration(0)
	for size := int64(0); size <= proto.XferMaxBytes; size += proto.XferMaxBytes / 32 {
		got := xferCrossHomeExtraFor(size)
		if got < prev {
			t.Fatalf("cross-home extra decreased at size=%d: %s after %s", size, got, prev)
		}
		prev = got
	}
}

// TestWatchdogBudgetIsBoundedByAdmission: an unbounded watchdog is not a longer timeout, it is no
// timeout. Admission refuses anything past transferMaxBytes before the watchdog is ever armed, so
// the ceiling is a compile-time constant — this pins that the two agree.
//
// Mutation: raise transferMaxBytes without re-deriving proto.XferTierBMaxBudget — reddens.
func TestWatchdogBudgetIsBoundedByAdmission(t *testing.T) {
	if transferMaxBytes != proto.XferMaxBytes {
		t.Fatalf("the broker's admission ceiling (%d) diverged from proto's (%d) — the budget's bound "+
			"is derived from proto's", transferMaxBytes, proto.XferMaxBytes)
	}
	if got := proto.XferBudget("b", transferMaxBytes, proto.XferPushLegs); got != proto.XferTierBMaxBudget {
		t.Fatalf("the budget at the admission ceiling is %s but the declared bound is %s", got, proto.XferTierBMaxBudget)
	}
}

// TestRecentlyReapedRemembersOnlyWatchdogKills. The Warn on a late completion is only meaningful if
// the tracker can tell "we reaped this" from "we never heard of it" — otherwise it either stays
// silent on the case that matters or shouts on every stray id.
//
// Mutation: make recentlyReaped return true unconditionally — the unknown-id assertion reddens.
func TestRecentlyReapedRemembersOnlyWatchdogKills(t *testing.T) {
	tr := newTransferTracker()
	now := time.Now()
	if tr.recentlyReaped("never-seen") {
		t.Error("an id the watchdog never touched must not be reported as reaped")
	}
	tr.markReaped("t1", now)
	if !tr.recentlyReaped("t1") {
		t.Error("a just-reaped id must be remembered so a late completion can be surfaced")
	}
	// The set must not grow without bound: an old entry is swept by a later insert.
	tr.markReaped("t2", now.Add(-2*xferRecentlyReapedTTL))
	tr.markReaped("t3", now)
	if tr.recentlyReaped("t2") {
		t.Error("an entry past the retention window was not swept")
	}
	if !tr.recentlyReaped("t1") || !tr.recentlyReaped("t3") {
		t.Error("the sweep took live entries with it")
	}
}

// TestHomeReapConsultsTheDurableLedgerNotJustTheTracker replaces a test that used to assert this
// defect must CONTINUE to exist.
//
// origin: batch-c EXTERNAL review B2. The internal review recorded "a restart lets the home reaper
// delete a live object two minutes in" as permanent decision N15, on the stated grounds that fixing it
// needed durable evidence of object ownership which did not exist. That reasoning was wrong: the #57
// xfer-inflight ledger has recorded the transfer id, bucket, size and start time since long before
// this batch, and the object's NAME IS THE TRANSFER ID at every writer. N15 is withdrawn; the old
// test — which would have gone red the day someone FIXED the bug — is deleted with it.
//
// Mutation: make ledgerProtectedObjects return an empty set — reddens.
func TestHomeReapConsultsTheDurableLedgerNotJustTheTracker(t *testing.T) {
	b := &Broker{transfers: newTransferTracker()}
	now := time.Now().UTC()
	b.cfg = Config{Logger: silentLogger(), Now: func() time.Time { return now }, ClusterDataDir: t.TempDir()}

	// A maximum-size transfer that started one minute ago: far past the 2-minute orphan grace once the
	// clock moves, and far inside its own size-derived budget.
	b.writeXferInflight(&transferEntry{
		transferID: "tid-big", sid: "s1", nid: "n1", verb: "push", tier: "b",
		bucket: "xfer-s1", path: "/dst", size: proto.XferMaxBytes, startedAt: now,
	})

	// Inside the budget: protected, even with an EMPTY tracker (the restart shape).
	atThreeMin, _, err := b.ledgerProtectedObjects(now.Add(3 * time.Minute))
	if err != nil {
		t.Fatalf("ledgerProtectedObjects: %v", err)
	}
	if !atThreeMin["xfer-s1/tid-big"] {
		t.Fatalf("a %s transfer one minute in is not protected, though its budget is %s. The in-memory "+
			"tracker is EMPTY after a restart, so it cannot be the evidence; the durable ledger is.",
			proto.HumanBytes(proto.XferMaxBytes), proto.XferBudget("b", proto.XferMaxBytes, proto.XferPushLegs))
	}

	// Past budget+slack: no longer protected, or a genuinely stranded object would be immortal.
	past := now.Add(proto.XferBudget("b", proto.XferMaxBytes, proto.XferPushLegs) + xferStrandedSlack + time.Minute)
	if got, _, _ := b.ledgerProtectedObjects(past); got["xfer-s1/tid-big"] {
		t.Error("an object past its own budget plus slack is still protected — real garbage would never " +
			"be reclaimed, which is the failure this reaper exists to prevent")
	}

	// A row that already carries a TERMINAL is a decided outcome: its object is disposable.
	b.writeXferInflight(&transferEntry{
		transferID: "tid-done", sid: "s1", nid: "n1", verb: "push", tier: "b",
		bucket: "xfer-s1", path: "/dst2", size: 1024, startedAt: now,
	})
	if _, ok := b.stageXferInflightTerminal("tid-done", schema.AuditTransfer{
		V: schema.AuditSchemaVersion, Kind: "complete", Verb: "push", Ts: now,
		Session: "s1", Node: "n1", TransferID: "tid-done", Tier: "b", Bucket: "xfer-s1",
	}); !ok {
		t.Fatal("stage terminal did not persist")
	}
	if got, _, _ := b.ledgerProtectedObjects(now.Add(time.Second)); got["xfer-s1/tid-done"] {
		t.Error("a row carrying a decided terminal still protects its object")
	}
}

// TestHomeReapIsNotSizeAware pins the deliberate `false` at the home-reap call site.
//
// origin: batch-c internal review tests-F6. Flipping that argument to true closes the N15 residual —
// which sounds like an improvement, but it is a decision, not an accident: widening the home reap's
// grace makes orphan garbage linger on exactly the small-disk brokers the sweep exists to protect,
// and it was left false on purpose. Before this test the flag had ZERO coverage: flipping it was
// invisible to the whole suite.
//
// Mutation: pass true at the home-reap call site — reddens. (And if that is ever done deliberately,
// this test and permanent decision N15 must be retired together.)
func TestHomeReapIsNotSizeAware(t *testing.T) {
	src, err := os.ReadFile("transfer_reconcile.go")
	if err != nil {
		t.Fatalf("read source: %v", err)
	}
	fset := token.NewFileSet()
	f, perr := parser.ParseFile(fset, "transfer_reconcile.go", src, 0)
	if perr != nil {
		t.Fatalf("parse: %v", perr)
	}
	sizeAware := map[string]bool{} // last-arg literal, keyed by the minAge expression it was called with
	ast.Inspect(f, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		// 6 args: EXTERNAL-review B2 threaded the durable-ledger protection set through, and the
		// 2026-09-02 prerelease audit (BT-F3) added the unreadable-row set beside it. The arity is
		// asserted rather than ignored so that a signature change forces someone to re-read THIS
		// scanner — the self-check below turns a silent zero-match into a loud failure, which is the
		// only reason the N15 decision it pins is still being checked at all.
		if !ok || sel.Sel.Name != "reapBucketObjects" || len(call.Args) != 6 {
			return true
		}
		flag, ok := call.Args[3].(*ast.Ident)
		if !ok {
			return true
		}
		var minAge string
		switch a := call.Args[2].(type) {
		case *ast.SelectorExpr:
			minAge = a.Sel.Name
		case *ast.CallExpr:
			if s, ok := a.Fun.(*ast.SelectorExpr); ok {
				minAge = s.Sel.Name
			}
		}
		sizeAware[minAge] = flag.Name == "true"
		return true
	})
	if len(sizeAware) != 2 {
		t.Fatalf("SELF-CHECK FAILED: found %d reapBucketObjects call site(s) (%v), want the home reap and "+
			"the cross-home GC — the scanner no longer matches the real shape", len(sizeAware), sizeAware)
	}
	if aware, ok := sizeAware["xferReapMinAge"]; !ok || aware {
		t.Errorf("the HOME reap must pass sizeAware=false (got %v) — its short grace is protected by the "+
			"per-bucket busy re-read, and widening it makes orphan garbage linger on the small-disk "+
			"brokers this sweep exists to protect (permanent decision N15)", sizeAware)
	}
	if aware, ok := sizeAware["crossHomeReapAge"]; !ok || !aware {
		t.Errorf("the CROSS-HOME GC must pass sizeAware=true (got %v) — it deletes another home's "+
			"objects, and since the watchdog became size-derived a single global floor can no longer "+
			"bound \"still live over there\"", sizeAware)
	}
}

// origin: prerelease audit broker-transfer/BT-F3.
//
// ONE UNREADABLE ROW MUST NOT STOP RECLAMATION FOR EVERYONE, FOREVER.
//
// ledgerProtectedObjects used to fold "some row would not parse" into its error, and
// reconcileXferObjects skipped the entire pass on any error. Unreadable rows are
// PERMANENT — forEachLedgerRecord renames them to .corrupt and nothing ever deletes
// those — so a single corrupt row, belonging to any session, disabled orphan
// reclamation for every session for the life of the deployment. That is the
// "small disk fills up with immortal tier-B garbage" failure this sweep exists to
// prevent, delivered by the guard instead of by the thing it guards.
func TestAnUnreadableLedgerRowProtectsOnlyItsOwnObject(t *testing.T) {
	now := time.Now().UTC()
	b := &Broker{transfers: newTransferTracker()}
	b.cfg = Config{Logger: silentLogger(), Now: func() time.Time { return now }, ClusterDataDir: t.TempDir()}

	// A healthy row, well past its budget: reapable.
	b.writeXferInflight(&transferEntry{
		transferID: "tid-old", sid: "s1", nid: "n1", verb: "push", tier: "b",
		bucket: "xfer-s1", path: "/dst", size: 1024, startedAt: now.Add(-time.Hour),
	})
	// A row that cannot be parsed at all.
	dir := b.xferInflightDir()
	if err := os.WriteFile(filepath.Join(dir, xferInflightFilename("tid-corrupt")), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	protected, unresolved, err := b.ledgerProtectedObjects(now)
	if err != nil {
		t.Fatalf("an unreadable ROW must not be reported as a pass-level error: %v.\n\n"+
			"The caller skips the whole sweep on an error, and this row will still be unreadable "+
			"on every future pass, so that is a permanent shutdown of orphan reclamation.", err)
	}
	if len(unresolved) != 1 {
		t.Fatalf("unresolved=%v, want exactly the one corrupt row.\n\n"+
			"The set is what the reaper fails closed on, per object. Too few and a live object is "+
			"deleted; too many and reclamation stalls for objects we CAN account for.", unresolved)
	}
	if !unresolved[xferInflightFilename("tid-corrupt")] {
		t.Errorf("the unresolved set does not name the corrupt row: %v", unresolved)
	}
	if protected["xfer-s1/tid-old"] {
		t.Error("an aged, readable row was reported protected — the corrupt row's failure leaked " +
			"into a decision about a different object")
	}
}

// origin: prerelease audit broker-transfer/BT-F4.
//
// A YOUNG PROCESS MUST NOT REAP A PREVIOUS INCARNATION'S LIVE OBJECT.
//
// The home reap's 2-minute grace is protected by the per-bucket busy re-read, which
// only sees transfers THIS process started. After a restart the tracker is empty, so
// a 2-GiB tier-B push three minutes into its 35m08s budget was deleted out from under
// the agent reading it. The floor therefore falls back toward the ordinary grace as
// the process ages, and is fully retired once this incarnation has been up for a whole
// maximum budget — at which point no object can belong to an earlier one.
//
// Deliberately NOT size-aware: that would reopen permanent decision N15, which
// TestHomeReapIsNotSizeAware pins in the AST, and would give a 2-GiB object a
// ~32-minute floor in buckets sized for one max object plus margin.
func TestTheHomeReapFloorShrinksAsTheProcessAges(t *testing.T) {
	const grace = 2 * time.Minute
	cases := []struct {
		name   string
		uptime time.Duration
		want   time.Duration
	}{
		{"just booted: the full budget", 0, proto.XferTierBMaxBudget},
		{"halfway", proto.XferTierBMaxBudget / 2, proto.XferTierBMaxBudget / 2},
		{"one full budget of uptime: back to the ordinary grace", proto.XferTierBMaxBudget, grace},
		{"long-running", 24 * time.Hour, grace},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := xferObjectReapFloor(grace, false, proto.XferMaxBytes, tc.uptime)
			if got != tc.want {
				t.Fatalf("floor=%v, want %v at uptime %v.\n\n"+
					"Too small and a restart deletes a live object mid-transfer; too large and "+
					"orphan garbage lingers on exactly the small-disk brokers this sweep protects.",
					got, tc.want, tc.uptime)
			}
		})
	}

	// The floor must be SIZE-INDEPENDENT on this path, or N15 is reopened by accident.
	small := xferObjectReapFloor(grace, false, 1, 0)
	large := xferObjectReapFloor(grace, false, proto.XferMaxBytes, 0)
	if small != large {
		t.Errorf("the home-reap floor varies with object size (%v vs %v).\n\n"+
			"That is exactly what permanent decision N15 forbids, and it is why the protection is "+
			"keyed on process uptime instead.", small, large)
	}

	// The cross-home GC keeps its size-derived extension, untouched.
	if aware := xferObjectReapFloor(grace, true, proto.XferMaxBytes, 24*time.Hour); aware <= grace {
		t.Errorf("the cross-home floor lost its size extension: %v", aware)
	}

	// minAge<=0 is the zero-value test broker's "reap everything now" and must stay so
	// however young the process is.
	if now := xferObjectReapFloor(0, false, proto.XferMaxBytes, 0); now > 0 {
		t.Errorf("a zero minAge grew a floor of %v; the prompt-reap semantics are gone", now)
	}
}

// origin: prerelease audit broker-transfer/#57.
//
// THE SINGLE BROKER HAS A LEDGER. It used to have none, because the directory was
// derived from ClusterDataDir alone and install.sh renders a production broker.yaml
// with data_dir commented out. Two things silently followed on every single-broker
// deployment: a restart could never finalize a stranded transfer, and the reaper's
// protected set was always empty — which it reads as "nobody owns any of this".
func TestTheLedgerDirectoryFallsBackToTheDatabaseDirectory(t *testing.T) {
	if got := xferLedgerSubdir("/data/cluster", "/var/lib/tether/tether.db", "xfer-inflight"); got != "/data/cluster/xfer-inflight" {
		t.Errorf("an explicit data_dir must win: %q", got)
	}
	if got := xferLedgerSubdir("", "/var/lib/tether/tether.db", "xfer-inflight"); got != "/var/lib/tether/xfer-inflight" {
		t.Errorf("with no data_dir the ledger must fall back beside the database, got %q.\n\n"+
			"Returning \"\" here is what switched the whole of #57 off on the deployment shape it "+
			"was written for, while the dangling audit start row it prevents is just as permanent "+
			"there: publishAudit writes to JetStream whenever brokerJS(b) is non-nil.", got)
	}
	if got := xferLedgerSubdir("", "", "xfer-inflight"); got != "" {
		t.Errorf("with nothing durable to hang it off the answer must stay empty, got %q", got)
	}
}

// origin: prerelease audit broker-transfer/BT-F5.
//
// THE TRACKER'S MEMORY BOUND HAS TO BE ENFORCED, NOT ASSERTED.
//
// transferTrackerMaxEntries' comment derived "~200 KiB" from 1024 entries at ~200
// bytes each, and re-derived it during batch C as "the (unchanged) byte count". Both
// were false: path and transfer_id arrive verbatim on the wire and were only checked
// for non-emptiness, so one request could carry up to max_payload and the real bound
// was 1024 * max_payload. In cluster mode each of those strings is also written to
// disk with two fsyncs, on a handler that has no goroutine wrapper — so an oversized
// payload is head-of-line blocking for every transfer on the broker, not just memory.
func TestTransferRequestStringsAreBounded(t *testing.T) {
	long := func(n int) string { return strings.Repeat("x", n) }
	cases := []struct {
		name              string
		id, path, sha     string
		wantRejectMention string // "" means the request must be ACCEPTED
	}{
		{name: "ordinary request", id: "01hxxxxxxxxxxxxxxxxxxxxxxx", path: "/home/u/data.bin", sha: long(64)},
		{name: "path at the ceiling", id: "t", path: long(transferPathMaxLen), sha: ""},
		{name: "id at the ceiling", id: long(transferIDMaxLen), path: "/x", sha: ""},
		{name: "oversized path", id: "t", path: long(transferPathMaxLen + 1), sha: "", wantRejectMention: "path length"},
		{name: "oversized id", id: long(transferIDMaxLen + 1), path: "/x", sha: "", wantRejectMention: "transfer_id length"},
		{name: "oversized digest", id: "t", path: "/x", sha: long(transferSHA256MaxLen + 1), wantRejectMention: "sha256 length"},
		// A short digest is NOT this gate's business: the receiver recomputes the
		// digest over the delivered bytes and refuses on mismatch. Rejecting it here
		// would invent a format contract in a place nobody would look for one.
		{name: "short digest passes; the receiver decides", id: "t", path: "/x", sha: "deadbeef"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := transferRequestBounds(tc.id, tc.path, tc.sha)
			if tc.wantRejectMention == "" {
				if got != "" {
					t.Fatalf("a legitimate request was refused: %s", got)
				}
				return
			}
			if got == "" {
				t.Fatalf("a %d-byte field was accepted.\n\n"+
					"The tracker holds 1024 entries; unbounded strings make its memory ceiling "+
					"1024 * max_payload instead of the ~200 KiB its own comment claims, and in "+
					"cluster mode each one is fsynced twice on a serialized handler.",
					max(len(tc.id), max(len(tc.path), len(tc.sha))))
			}
			if !strings.Contains(got, tc.wantRejectMention) {
				t.Errorf("refusal %q does not name the offending field (%q)", got, tc.wantRejectMention)
			}
		})
	}
}
