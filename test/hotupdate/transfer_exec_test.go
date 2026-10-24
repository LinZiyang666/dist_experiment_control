//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/test/transferharness"
)

type transferCell struct{ tier, direction, layer, window string }

func (c transferCell) name() string { return "tier-" + c.tier + "/" + c.direction + "/" + c.window }

// Keep structural N/A local to the OBJ operation: tier A instead exercises its
// inline send-unknown window, with the same receiver/commit/history obligations.
// origin: hot-update S7 plan §10.2 / T35
func transferCells(layer string) []transferCell {
	var cells []transferCell
	for _, cell := range transferharness.BoundaryCells() {
		cells = append(cells, transferCell{cell.Tier, cell.Direction, layer, cell.Window})
	}
	return cells
}

// The agent cuts take the authorized S8 transaction route, whose preflight
// requires both readers to declare the S8 journal, incarnation and adjunct
// features. Production does not advertise them until every exporter exists,
// so both images carry exactly that one declaration overlay on top of the
// transfer observation/fault overlay (buildAdvertisingArtifacts).
// origin: hot-update S7 plan §10.1 / T35; S8 FIX-DESCRIBE (P19 request R7)
// origin: S7 internal review CUT-CAPACITY F1–F4
func TestTransferAgentExecAcrossEveryBoundary(t *testing.T) {
	// origin: hot-update S10 lead decision D-A
	for _, mode := range testharness.CandidateBuildModes() {
		t.Run(mode, func(t *testing.T) {
			a := buildAdvertisingArtifacts(t)
			requireTransferIntegration(t, a)
			g, fd := runtime.NumGoroutine(), testharness.FDCount()
			// The blocking parent joins all five rounds before the direct leak checks.
			// In particular, five real 120s watchdog rounds cannot run serially inside
			// the lead's 600s per-test partition. Fixture concurrency is bounded below.
			// origin: hot-update S7 plan lead L17 / §11.3
			t.Run("rounds", func(t *testing.T) {
				for round := 0; round < 5; round++ {
					t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) {
						t.Parallel()
						exerciseTransferAgentExec(t, a)
					})
				}
			})
			testharness.AssertNoGoroutineLeak(t, "transfer agent binary boundaries", g)
			testharness.AssertNoFDLeak(t, "transfer agent binary boundaries", fd, 0)

		})
	}
}
func exerciseTransferAgentExec(t *testing.T, a transferArtifacts) {
	for _, cell := range transferCells("agent") {
		t.Run(cell.name(), func(t *testing.T) {
			t.Parallel()
			s := newTransferStack(t, a)
			s.requireCapable()
			s.transactionCuts = true
			runTransferCell(t, s, cell, func(func()) { s.agentCut(false); s.agentCut(true) })
		})
	}
}

type transferRun struct {
	cell           transferCell
	id             string
	source, target string
	old, body      []byte
	before         transferharness.Receipt
	boundary       x.Observation
	cutOwner       x.Observation
	watch          *transferTargetWatch
}

func startTransferRun(t *testing.T, s *transferStack, cell transferCell) *transferRun {
	t.Helper()
	// The setup capability probe is a separate read-only call. Start the
	// business identity census before launching the one original transfer ctl.
	transferMust(t, s.nc.FlushTimeout(time.Second))
	s.wireMu.Lock()
	s.wire = nil
	s.wireMu.Unlock()
	rate := "131072"
	if cell.tier == "b" {
		rate = "1048576"
	}
	r := &transferRun{cell: cell, source: filepath.Join(s.h.Root, "source"), target: filepath.Join(s.h.Root, "destination"),
		body: make([]byte, transferCellSize(cell)), old: []byte("complete original destination\n")}
	for i := range r.body {
		r.body[i] = byte((i*131 + i/4093) % 251)
	}
	transferMust(t, os.WriteFile(r.source, r.body, 0600))
	transferMust(t, os.WriteFile(r.target, r.old, 0600))
	r.watch = startTransferTargetWatch(t, r.target, r.old, r.body)
	args := []string{cell.direction, "--force", "--rate-limit", rate, "--home", s.home, "--nats-url", s.url}
	if cell.direction == "push" {
		args = append(args, r.source, s.nid+":"+r.target)
	} else {
		args = append(args, s.nid+":"+r.source, r.target)
	}
	s.ctl = s.launch("ctl", filepath.Join(s.h.Root, "ctl"), s.home, args, true)
	// launch pinned the original ctl before releasing its pre-main handshake.
	// A second expensive hash could outlive a valid fast/N/A transfer.
	// origin: S7 internal review FIX-TIERA2 F3 / hot-update S7 plan §10.4 / T46
	r.before = s.ctl.Receipt()
	return r
}

// transferCellSize keeps tier A below its inline ceiling and tier B multi-chunk.
func transferCellSize(cell transferCell) int {
	if cell.tier == "b" {
		return 9 << 20
	}
	return 1 << 20
}

// transferStructuralNA names the cells plan §10.2 does not define for a tier.
// Every receiver data cell needs a durable prefix with 0<durable_offset<size,
// but a Tier A receiver lands its whole inline payload with one write and one
// Sync, so its durable prefix is only ever empty or complete. Its primitive,
// grant and settlement windows remain applicable; its OBJ chunk cell is
// replaced by the inline send-unconfirmed window in transferCells instead.
// origin: hot-update S7 plan §10.2 / T35 / S7 FIX-TIERA2
func transferStructuralNA(cell transferCell) string {
	if cell.tier == "a" && (cell.window == "receiver-write" || cell.window == "Sync-pending" || cell.window == "synced-prefix") {
		return "a tier A receiver has no durable prefix with 0<durable_offset<size (one inline write, one Sync)"
	}
	return ""
}

// proveTransferStructuralNA never assumes the N/A: the cell's real transfer
// runs to native success with no barrier armed, no producer record may satisfy
// the window's own boundary predicate, and the receiver must have entered its
// one primitive with the whole payload durable. A Tier A that ever gains a
// partial durable prefix turns this red instead of skipping a window it owns.
// origin: hot-update S7 plan §10.2 / T35 / S7 FIX-TIERA2
func proveTransferStructuralNA(t *testing.T, s *transferStack, cell transferCell) {
	t.Helper()
	r := startTransferRun(t, s, cell)
	defer r.watch.close()
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	result, err := s.ctl.Wait(ctx)
	if err != nil || result.NativeRC != 0 {
		t.Fatalf("structural N/A proof: original transfer did not complete: native_rc=%d err=%v %s", result.NativeRC, err, s.output(s.ctl))
	}
	// origin: hot-update S10 lead decision D-A / measured structural N/A transfer
	if cell.tier == "a" {
		testharness.RequireCandidateBudget(t, fmt.Sprintf("/proc/%d/exe", s.agent.Receipt().PID), time.Duration(result.WallNS), proto.XferTimeoutTierA)
	}
	transferMust(t, r.watch.check())
	if target, err := os.ReadFile(r.target); err != nil || !bytes.Equal(target, r.body) {
		t.Fatalf("structural N/A proof: destination is not the complete source: %v", err)
	}
	// Only the window's own producers (its receiver) can hold its boundary.
	// Reading every custody stream would also make an exited ctl's late first
	// batch, whose identity can no longer be sampled, poison an agent-side proof.
	size, entered := int64(len(r.body)), false
	if !testharness.WaitFor(t, 10*time.Second, 10*time.Millisecond, func() bool {
		for _, role := range transferWindowProducers(cell) {
			rows, err := s.collectors[role].snapshot()
			transferMust(t, err)
			for _, row := range rows {
				if validateTransferBoundary(cell, row, size) == nil {
					t.Fatalf("structural N/A disproved: %s produced a %s boundary at durable_offset=%d of %d", role, cell.window, row.DurableOffset, size)
				}
				entered = entered || row.Kind == x.ObservePrimitiveEnter && row.DurableOffset == size && row.Tmp.ResourceID != "" && row.Parent.ResourceID != ""
			}
		}
		return entered
	}) {
		t.Fatal("structural N/A proof is vacuous: no receiver entered its primitive with the whole payload durable")
	}
}

// The cut receives the barrier release. It calls it once its own cut is
// complete when a later phase needs live product progress (T37 ACTIVE progress,
// T38's 120s watchdog); otherwise runTransferCell releases after the cut.
// origin: S7 FIX-CUT deterministic live-cut barrier / hot-update S7 plan §10.2
func runTransferCell(t *testing.T, s *transferStack, cell transferCell, cut func(release func())) {
	t.Helper()
	if reason := transferStructuralNA(cell); reason != "" {
		proveTransferStructuralNA(t, s, cell)
		t.Skipf("NOT_APPLICABLE (plan §10.2 structural N/A, proven on a real transfer): %s", reason)
	}
	// Arm before launch: the first qualifying producer record parks. Timing,
	// polling or a later phase-matching receipt can no longer select the cut.
	hold := armTransferHold(t, s.h.Root, cell, int64(transferCellSize(cell)))
	defer hold.releaseQuietly()
	armTransferReplyDrop(t, s.h.Root, cell)
	requests := watchTransferRequests(t, s, cell)
	r := startTransferRun(t, s, cell)
	defer r.watch.close()
	r.boundary = hold.await(s, r)
	r.id = r.boundary.TransferID
	transferMust(t, validateTransferBoundary(cell, r.boundary, int64(len(r.body))))
	if cell.layer != "js" {
		rows, err := s.collectors[cell.layer].snapshot()
		transferMust(t, err)
		for _, row := range rows {
			if row.TransferID == r.id && row.EventSeq > r.cutOwner.EventSeq {
				r.cutOwner = row
			}
		}
		if r.cutOwner.PID == 0 || r.cutOwner.EntryNonce == "" {
			t.Fatalf("INCOMPLETE: %s cut has no original transfer responsibility", cell.layer)
		}
	}
	// The observation producer owns its descriptors. A push's source belongs
	// to ctl even though its receiver belongs to agent; role guesses lose custody.
	// origin: S7 internal review INTEG-M F3 / hot-update S7 plan §10.4
	pids := make(map[string]int)
	for role, collector := range s.collectors {
		if collector.process != nil {
			pids[role] = collector.process.Receipt().PID
		}
	}
	owner, err := transferCustodyOwner(pids, r.boundary.PID)
	transferMust(t, err)
	// Pull's local atomic receiver stays in the original ctl. Agent cuts must
	// preserve its actual sender/courier; they do not claim to migrate ctl FDs.
	// The producer is parked, so this samples the original descriptors before
	// it can legitimately retire them.
	// origin: S7 FIX-CUT deterministic original-FD sampling / hot-update S7 plan §10.4
	inodes, err := s.collectors[owner].originalInodes(r.boundary)
	transferMust(t, err)
	raw := startTransferRawCensus(t, s, r)
	defer raw.close()
	// The same parked owner's Freeze would need what it holds: release at cut
	// entry and do not claim the cut happened while it was parked.
	// An authorized transaction cut enters at its Start: releasing before its
	// describe, prepare (a candidate download and probe) and root stage let a
	// tier-B producer finish the whole transfer before any owner was frozen,
	// so that cut carried only a settled entry. Such a cut releases at its
	// own entry instead (transactionCut); it must have entered by its return.
	// origin: S8 FIX-DESCRIBE (P19 request R7)
	if hold.cutEntryRelease {
		if s.transactionCuts {
			// Released inside the cut, the producer can finish and retire the
			// original object before an authorized cut (30–50s) returns, so the
			// independent readers attach and read it while it is still parked.
			// No restart happens here, so an in-flight SDK read is never lost.
			// origin: S7 internal review CUT-CAPACITY F7 / S7 FIX-SDKGET F1 / F2
			raw.startObjectReads()
			transferMust(t, raw.awaitPublishedObjectReads())
			s.cutEntry = hold.release
		} else {
			hold.release()
		}
	}
	var after transferharness.Receipt
	cutDone := false
	releasedInCut := false
	releaseAfterCut := func() {
		if hold.released {
			return
		}
		releasedInCut = !cutDone
		// Still parked across the whole cut: the cut itself closed or reopened
		// nothing, and the original ctl cannot yet have finished. Once
		// released, retirement and completion are legitimate continuation.
		transferMust(t, s.collectors[owner].compareOFDs(r.boundary))
		after = transferCtlAcrossCut(t, s, cell, false)
		raw.startObjectReads()
		transferMust(t, raw.awaitPublishedObjectReads())
		hold.release()
	}
	cutStart := transferBoottime(t)
	cut(releaseAfterCut)
	cutEnd := transferBoottime(t)
	if s.cutEntry != nil {
		t.Fatal("transaction cut returned without entering its cut: the deferred cut-entry release never ran")
	}
	cutDone = true
	releaseAfterCut()
	transferMust(t, validateTransferHoldOrder(hold, cutStart, cutEnd, releasedInCut))
	if after.PID == 0 {
		// Released at cut entry or by the cut itself for its own later phase.
		after = transferCtlAcrossCut(t, s, cell, true)
	}
	raw.startObjectReads()
	transferMust(t, validateTransferProcess(r.before, after, r.before.RunningSHA))
	transferMust(t, r.watch.check())
	rows := awaitTransferContinuation(t, s, r)
	transferMust(t, validateTransferContinuation(cell, r.boundary, r.cutOwner, r.before, rows))
	// After release the producer may retire an original descriptor, but no
	// descriptor on the same file may be a different open file description.
	transferMust(t, s.collectors[owner].checkNoReopen(r.boundary, inodes))
	ctx, cancel := context.WithTimeout(t.Context(), 90*time.Second)
	defer cancel()
	result, err := s.ctl.Wait(ctx)
	// origin: hot-update S10 lead decision D-A / Tier-A absolute deadline
	if cell.tier == "a" {
		testharness.RequireCandidateBudget(t, fmt.Sprintf("/proc/%d/exe", s.agent.Receipt().PID), time.Duration(result.WallNS), proto.XferTimeoutTierA)
	}
	if err != nil || result.NativeRC != 0 {
		t.Fatalf("original transfer ctl did not complete: native_rc=%d err=%v %s", result.NativeRC, err, s.output(s.ctl))
	}
	if result.PID != r.before.PID || result.ProcessStart != r.before.ProcessStart {
		t.Fatal("completion belongs to a replacement ctl")
	}
	target, err := os.ReadFile(r.target)
	transferMust(t, err)
	if !bytes.Equal(target, r.body) {
		t.Fatal("destination does not contain the complete source")
	}
	// Join/retirement must be observed while daemon owners remain alive. Killing
	// them to silence a late courier would manufacture unique history.
	awaitTransferRetired(t, s, r.id)
	for _, role := range []string{"agent", "broker"} {
		transferMust(t, s.collectors[role].drain())
	}
	transferMust(t, raw.scan())
	quiet := time.NewTimer(2 * time.Second) // sleep-fixture: contractual post-join late-effect observation window.
	select {
	case <-quiet.C:
	case <-t.Context().Done():
		quiet.Stop()
		t.Fatal(t.Context().Err())
	}
	for _, role := range []string{"agent", "broker"} {
		transferMust(t, s.collectors[role].drain())
	}
	transferMust(t, raw.scan())
	transferMust(t, raw.verify())
	transferMust(t, r.watch.check())
	transferMust(t, s.verifyCalls(r.id))
	if cell.window == "grant-reply-lost" {
		brokerRows, err := s.collectors["broker"].snapshot()
		transferMust(t, err)
		transferMust(t, validateTransferReplyDrop(s.h.Root, cell.direction, r.boundary, brokerRows, requests.snapshot()))
	}
	t.Logf("source_sha=%s dirty_digest=%s artifact_sha=%s running_sha=%s test_or_arm=%s native_rc=%d pid=%d process_start=%d boot_id=%s transfer_id=%s object_version=%s",
		s.a.source, s.a.dirty, r.before.ArtifactSHA, after.RunningSHA, cell.name(), result.NativeRC,
		result.PID, result.ProcessStart, result.BootID, r.id, r.boundary.ObjectVersion)
	t.Logf("live_cut_barrier producer_pid=%d generation=%d event_seq=%d entered_ns=%d released_ns=%d woke_ns=%d cut_entry_release=%t",
		hold.pid, hold.gen, hold.ordinal, hold.enteredNS, hold.releasedNS, hold.wokeNS, hold.cutEntryRelease)
}

func transferWindowMatches(window string, o x.Observation) bool {
	switch window {
	case "prepare-accepted":
		return o.Kind == x.ObserveControl && o.Phase == x.PhasePrepared
	case "source-pinned":
		return o.Source.ResourceID != "" && o.Phase == x.PhaseSending
	case "sender-chunk-pending":
		return o.Kind == x.ObserveAppend && o.ActualACKSeq == 0 && o.Length > 0
	case "inline-send-unconfirmed":
		return o.Kind == x.ObserveAttempt && o.Attempt.Sent && o.ObjectVersion == ""
	case "receiver-write":
		return o.Kind == x.ObserveFetched && o.DurableOffset > 0 && o.FetchedOffset > o.DurableOffset
	case "Sync-pending":
		return o.Kind == x.ObserveSynced && o.DurableOffset > 0 && !o.FileSynced
	case "synced-prefix":
		return o.Kind == x.ObserveSynced && o.DurableOffset > 0 && o.FileSynced
	case "grant-reply-lost":
		return o.Kind == x.ObserveReserve && o.GrantNonce != "" && o.Attempt.Sent
	case "pre-primitive":
		return o.Kind == x.ObservePrimitiveEnter && o.Outcome == x.OutcomeNotEntered
	case "post-primitive-pre-outcome":
		return o.Kind == x.ObservePrimitiveExit && o.Outcome == x.OutcomeLanded && o.CanonicalDigest == ""
	case "terminal-pre-history-ACK":
		return o.Phase == x.PhaseTerminalPending && o.CanonicalDigest != "" && o.HistorySeq == 0
	case "cleanup-pending":
		return o.Phase == x.PhaseCleanupPending && o.RetirementDigest == ""
	default:
		return false
	}
}
func awaitTransferBoundary(t *testing.T, s *transferStack, r *transferRun) x.Observation {
	t.Helper()
	var found x.Observation
	ok := testharness.WaitFor(t, 20*time.Second, 5*time.Millisecond, func() bool {
		for _, role := range []string{"agent", "ctl", "broker"} {
			rows, err := s.collectors[role].snapshot()
			transferMust(t, err)
			for _, row := range rows {
				if transferWindowMatches(r.cell.window, row) && drillobserve.TransferPendingBoundary(r.cell.window, row) {
					found = row
					return true
				}
			}
		}
		// A command's native refusal is a more useful dependency diagnostic than
		// spending the remaining live-window budget waiting for nonexistent IO.
		if receipt := s.ctl.Receipt(); receipt.NativeRC != -1 {
			t.Fatalf("INCOMPLETE: original ctl exited before %s: native_rc=%d %s", r.cell.window, receipt.NativeRC, s.output(s.ctl))
		}
		return false
	})
	if !ok {
		t.Fatalf("NOT_RUN pending P28/P29 integration: no nonempty production boundary %s; completed/missed windows cannot certify a cut", r.cell.window)
	}
	return found
}
func awaitTransferContinuation(t *testing.T, s *transferStack, r *transferRun) []x.Observation {
	t.Helper()
	var rows []x.Observation
	var last error
	if !testharness.WaitFor(t, 10*time.Second, 10*time.Millisecond, func() bool {
		rows = nil
		for _, c := range s.collectors {
			current, err := c.snapshot()
			transferMust(t, err)
			rows = append(rows, current...)
		}
		last = validateTransferContinuation(r.cell, r.boundary, r.cutOwner, r.before, rows)
		return last == nil
	}) {
		// The last verdict names which continuation obligation is missing.
		// origin: S8 FIX-DESCRIBE (diagnostic only; the oracle is unchanged)
		t.Fatalf("original transfer lacks current-generation continuation within the 10s durable-progress window: %v; native_rc=%d %s", last, s.ctl.Receipt().NativeRC, s.output(s.ctl))
	}
	return rows
}
func awaitTransferRetired(t *testing.T, s *transferStack, id string) {
	t.Helper()
	if !testharness.WaitFor(t, 10*time.Second, 20*time.Millisecond, func() bool {
		for _, role := range []string{"agent", "broker"} {
			rows, err := s.collectors[role].snapshot()
			transferMust(t, err)
			retired := false
			for _, row := range rows {
				if row.TransferID == id && row.Kind == x.ObserveRetirement && row.Phase == x.PhaseSettled && row.AdmissionClosed {
					retired = true
				}
			}
			if !retired {
				return false
			}
		}
		return true
	}) {
		for _, role := range []string{"agent", "broker"} {
			rows, _ := s.collectors[role].snapshot()
			for _, row := range rows {
				if row.TransferID == id && (row.Kind == x.ObserveRetirement || row.Kind == x.ObserveFinish || row.Kind == x.ObserveAcknowledge) {
					t.Logf("pending retirement role=%s kind=%s phase=%s history_seq=%d closed=%t", role, row.Kind, row.Phase, row.HistorySeq, row.AdmissionClosed)
				}
			}
		}
		t.Fatal("courier/finalizer/watchdog did not join before final history seal")
	}
}
func (s *transferStack) agentCut(original bool) {
	path, expected, candidate := "/candidate.tar.gz", s.a.shaB, s.a.b
	if original {
		path, expected, candidate = "/original.tar.gz", s.a.shaA, s.a.a
	}
	before, err := s.agent.SampleIdentity()
	transferMust(s.t, err)
	archive := s.a.archiveB
	if original {
		archive = s.a.archiveA
	}
	// A and B deliberately share a release version. Observe the real executable
	// identity independently; CLI version polling cannot distinguish these images.
	// origin: S8 FIX-DESCRIBE (P19 request R7) — the cut takes the authorized
	// transaction route (prepare → stage → start → wait); the old
	// `node upgrade --url` dispatch is refused by the shared legacy policy.
	if _, err := s.transactionCut(candidate, s.artifact.URL+path, archive); err != nil {
		s.t.Fatalf("agent transaction cut to %s: %v", expected, err)
	}
	if !testharness.WaitFor(s.t, 20*time.Second, 20*time.Millisecond, func() bool {
		after, err := s.agent.SampleIdentity()
		return err == nil && after.RunningSHA == expected
	}) {
		s.t.Fatal("agent did not exec the requested artifact at its original PID")
	}
	after, err := s.agent.SampleIdentity()
	transferMust(s.t, err)
	transferMust(s.t, validateTransferProcess(before, after, expected))
}

// armAgentCut dispatches the original authorized transaction without waiting
// for completion: a fault arm must prevent completion and prove recovery.
// origin: S8 internal review activation-and-route-migration F3
func (s *transferStack) armAgentCut() string {
	before, err := s.agent.SampleIdentity()
	transferMust(s.t, err)
	cut, err := s.startAgentTransaction(s.a.b, s.artifact.URL+"/candidate.tar.gz", s.a.archiveB)
	if err != nil {
		s.t.Fatal(err)
	}
	if !testharness.WaitFor(s.t, 20*time.Second, 20*time.Millisecond, func() bool { after, err := s.agent.SampleIdentity(); return err == nil && after.RunningSHA == s.a.shaB }) {
		s.t.Fatal("agent did not exec the requested artifact at its original PID")
	}
	after, err := s.agent.SampleIdentity()
	transferMust(s.t, err)
	transferMust(s.t, validateTransferProcess(before, after, s.a.shaB))
	return cut.tx
}
