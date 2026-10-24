//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// These inputs contain receipts from the supervised product exercises, never
// synthesized success from an unfinished engine. Missing input is SETUP-RED.
// Keeping the reader independent lets P26/P27 use the same frozen cases.
var transactionEvidenceDir = flag.String("upgrade-transaction-evidence", "", "S8 supervised fault/crash/mutant evidence directory")

type transactionFaultCase struct {
	Window, Fault, Role, OwnerLane string
	Phase, Outcome                 string
	RC                             int
	Proof, Inapplicable, Test, Arm string
	CurrentOwner, ReleaseTiming    string
}

var transactionFaultHeader = []string{"window", "fault", "role", "owner_lane", "expected_phase", "expected_outcome", "expected_rc", "proof_required", "reason_if_inapplicable", "test", "drill_arm", "current_owner", "release_timing"}

func readTransactionFaultCases() ([]transactionFaultCase, error) {
	body, err := os.ReadFile("testdata/upgrade_fault_cases.tsv")
	if err != nil {
		return nil, err
	}
	return parseTransactionFaultCases(body)
}

func parseTransactionFaultCases(body []byte) ([]transactionFaultCase, error) {
	var data bytes.Buffer
	for _, line := range strings.Split(string(body), "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			data.WriteString(line + "\n")
		}
	}
	r := csv.NewReader(&data)
	r.Comma, r.FieldsPerRecord = '\t', len(transactionFaultHeader)
	header, err := r.Read()
	if err != nil || !reflect.DeepEqual(header, transactionFaultHeader) {
		return nil, fmt.Errorf("fault manifest header: %v", err)
	}
	var cases []transactionFaultCase
	for {
		row, err := r.Read()
		if err == io.EOF {
			return cases, nil
		}
		if err != nil {
			return nil, err
		}
		rc, err := strconv.Atoi(row[6])
		if err != nil {
			return nil, err
		}
		cases = append(cases, transactionFaultCase{row[0], row[1], row[2], row[3], row[4], row[5], rc, row[7], row[8], row[9], row[10], row[11], row[12]})
	}
}

// Rules are encoded separately from the checked-in manifest. Cardinality alone
// would let a removed dead-owner cell be replaced by a duplicate happy cell.
// origin: hot-update S8 plan §8.2 / P25
func expectedTransactionFaultCase(w, f int, role, lane, current, timing string) transactionFaultCase {
	c := transactionFaultCase{Window: fmt.Sprintf("W%02d", w), Fault: fmt.Sprintf("F%d", f), Role: role, OwnerLane: lane, CurrentOwner: current, ReleaseTiming: timing, Test: "TestTransactionFaultReceipts"}
	c.Arm = strings.Join([]string{c.Window, c.Fault, role, lane, current, timing}, "-")
	if f == 8 && role == "single" {
		c.Inapplicable = "Single has no Raft leader or leader-transfer effect."
	} else if w >= 8 && lane == "L" {
		c.Inapplicable = "The original runtime cannot remain live after same-PID exec reaches partial ACTIVE; a retained helper is a different owner."
	}
	if c.Inapplicable != "" {
		c.Phase, c.Outcome, c.RC = "rejected", "rejected", 64
		c.Proof, c.Test = "applicability_refusal,no_effect", "TestTransactionFaultApplicabilityRefusesFabricatedOwner"
		return c
	}
	proof := []string{"original_set", "owner_identity", "current_owner", "custody", "hit_interval"}
	switch {
	case current == "unknown":
		c.Phase, c.Outcome, c.RC = "recovery_required", "unknown", 74
		proof = append(proof, "no_auto_reverse", "pending_retained")
	case f == 9:
		c.Phase, c.Outcome, c.RC = "deferred", "deferred", 75
		proof = append(proof, "competitor_pre_accept", "original_continues", "stable_handle")
	case f == 3 || f == 4:
		c.Phase, c.Outcome, c.RC = "recovery_required", "unknown", 74
		proof = append(proof, "actual_errno", "intent_retained", "pins_retained", "no_false_durable")
	case f == 6:
		c.Phase, c.Outcome, c.RC = "rejected", "rejected", 77
		if w >= 8 {
			c.Phase, c.Outcome = "recovery_required", "unknown"
		}
		proof = append(proof, "revocation_first", "new_request_77", "terminal_settled", "no_permission_resurrection")
	case (f == 5 || f == 7) && timing == "inside":
		c.Phase, c.Outcome, c.RC = "completed", "success", 0
		proof = append(proof, "query_only", "stable_handle", "absolute_deadline", "control_ready", "resources_migrated", "old_generation_released", "fresh_running_sha", "admission_barrier")
	case f == 8 && w < 8:
		c.Phase, c.Outcome, c.RC = "deferred", "deferred", 75
		proof = append(proof, "fresh_role", "pre_accept_step", "safe_defer")
	case lane == "D":
		c.Phase, c.Outcome, c.RC = "rolled_back", "rolled_back", 64
		proof = append(proof, "current_export", "fresh_reverse", "salvage_all_fds", "retire_before_third_generation", "control_ready", "resources_migrated", "old_generation_released", "fresh_running_sha", "admission_barrier")
	case w == 0:
		c.Phase, c.Outcome, c.RC = "rejected", "rejected", 64
		if f == 2 || f == 5 || f == 7 {
			c.Phase, c.Outcome, c.RC = "deferred", "deferred", 75
		}
		proof = append(proof, "pre_accept", "no_effect", "original_preserved")
	default:
		c.Phase, c.Outcome, c.RC = "cancelled", "cancelled", 64
		proof = append(proof, "pristine", "candidate_reclaimed", "thaw_original", "original_preserved")
	}
	if timing == "outside" {
		proof = append(proof, "deadline_exceeded")
	}
	if w >= 9 {
		proof = append(proof, "new_request_identity", "latest_ack_before_replay")
	}
	if w == 8 {
		proof = append(proof, "partial_take", "salvage_all_fds")
	}
	sort.Strings(proof)
	c.Proof = strings.Join(uniqueTransactionStrings(proof), ",")
	return c
}

func uniqueTransactionStrings(sorted []string) []string {
	var result []string
	for _, s := range sorted {
		if len(result) == 0 || s != result[len(result)-1] {
			result = append(result, s)
		}
	}
	return result
}

func validateTransactionFaultCases(cases []transactionFaultCase) error {
	want := make(map[string]transactionFaultCase)
	for w := 0; w < 13; w++ {
		for f := 0; f < 10; f++ {
			for _, role := range []string{"agent", "single", "sole-voter", "N3"} {
				for _, owner := range [][2]string{{"L", "known"}, {"D", "known"}, {"D", "unknown"}} {
					timings := []string{"none"}
					if f == 5 || f == 7 {
						timings = []string{"inside", "outside"}
					}
					for _, timing := range timings {
						c := expectedTransactionFaultCase(w, f, role, owner[0], owner[1], timing)
						want[c.Arm] = c
					}
				}
			}
		}
	}
	if len(cases) != 1872 || len(want) != 1872 {
		return fmt.Errorf("fault cells: got %d, require 1872", len(cases))
	}
	for _, c := range cases {
		expected, exists := want[c.Arm]
		if !exists || !reflect.DeepEqual(c, expected) {
			return fmt.Errorf("missing, duplicate or changed expectation: %s", c.Arm)
		}
		delete(want, c.Arm)
	}
	if len(want) != 0 {
		return fmt.Errorf("uncovered fault cells: %d", len(want))
	}
	return nil
}

// origin: hot-update S8 plan §8.2 / §11.2 P25
func TestTransactionFaultManifestComplete(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionFaultManifest(t)
	}
	testharness.AssertNoGoroutineLeak(t, "fault manifest reads x5", g)
	testharness.AssertNoFDLeak(t, "fault manifest reads x5", fd, 0)
}

func exerciseTransactionFaultManifest(t *testing.T) {
	t.Helper()
	cases, err := readTransactionFaultCases()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateTransactionFaultCases(cases); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, c := range cases {
		counts[c.Phase]++
	}
	// Pin the priority partitions independently of the rule generator. This
	// prevents turning every dead lane into a pristine thaw or a blanket N/A.
	if counts["completed"] != 168 || counts["rolled_back"] != 275 || counts["recovery_required"] != 799 || counts["rejected"] != 346 || counts["deferred"] != 144 || counts["cancelled"] != 140 {
		t.Fatalf("changed fault priority partitions: %v", counts)
	}
}

// origin: hot-update S8 plan §8.2 / P25
func TestTransactionFaultManifestControls(t *testing.T) {
	cases, err := readTransactionFaultCases()
	if err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func([]transactionFaultCase) []transactionFaultCase{
		"missing":   func(c []transactionFaultCase) []transactionFaultCase { return c[1:] },
		"duplicate": func(c []transactionFaultCase) []transactionFaultCase { c[1] = c[0]; return c },
		"unknown-as-success": func(c []transactionFaultCase) []transactionFaultCase {
			c[2].Phase, c[2].Outcome, c[2].RC = "completed", "success", 0
			return c
		},
		"no-proof": func(c []transactionFaultCase) []transactionFaultCase { c[0].Proof = ""; return c },
		"post-hoc-na": func(c []transactionFaultCase) []transactionFaultCase {
			c[0].Inapplicable = "injection did not hit"
			return c
		},
		"missing-test":     func(c []transactionFaultCase) []transactionFaultCase { c[0].Test = ""; return c },
		"timing-collapsed": func(c []transactionFaultCase) []transactionFaultCase { c[60].ReleaseTiming = "none"; return c },
	} {
		t.Run(name, func(t *testing.T) {
			copyCases := append([]transactionFaultCase(nil), cases...)
			if validateTransactionFaultCases(change(copyCases)) == nil {
				t.Fatal("fault manifest defect accepted")
			}
		})
	}
	for _, body := range []string{"", "window\tfault\nW00\tF0\n", strings.Join(transactionFaultHeader, "\t") + "\nW00\n"} {
		if _, err := parseTransactionFaultCases([]byte(body)); err == nil {
			t.Fatal("malformed fault manifest accepted")
		}
	}
}

func transactionApplicability(c transactionFaultCase, claimsLeader, claimsOriginalLive bool) error {
	if c.Role == "single" && c.Fault == "F8" && claimsLeader {
		return fmt.Errorf("single cannot supply a leader-transfer receipt")
	}
	w, err := strconv.Atoi(strings.TrimPrefix(c.Window, "W"))
	if err != nil {
		return err
	}
	if w >= 8 && claimsOriginalLive {
		return fmt.Errorf("retained helper cannot resurrect the original runtime after exec")
	}
	return nil
}

// origin: hot-update S8 plan §8.2 / P25
func TestTransactionFaultApplicabilityRefusesFabricatedOwner(t *testing.T) {
	cases, err := readTransactionFaultCases()
	if err != nil {
		t.Fatal(err)
	}
	refusals := 0
	for _, c := range cases {
		if c.Inapplicable == "" {
			continue
		}
		refusals++
		if transactionApplicability(c, c.Fault == "F8", c.OwnerLane == "L") == nil {
			t.Fatalf("fabricated applicability accepted: %s", c.Arm)
		}
	}
	if refusals != 274 {
		t.Fatalf("applicability refusals: got %d, require 274", refusals)
	}
	if err := transactionApplicability(expectedTransactionFaultCase(8, 8, "N3", "D", "known", "none"), true, false); err != nil {
		t.Fatal(err)
	}
}

type transactionFaultHit struct {
	Window, Fault, Syscall, Errno string
	Owner                         upgradetx.Owner
	Stage                         upgradetx.CutStage
	BeginNS, EndNS, EffectNS      int64
	DeadlineNS, ReleasedNS        int64
}

// A log/event outside the syscall/owner interval cannot turn preflight into R1.
// The monotonic interval and kernel identity come from the injector, not phase
// text inferred after the run. Network/revoke/leader faults have their own hit.
// origin: hot-update S8 plan §8.2 / M19
func judgeTransactionFaultHit(c transactionFaultCase, h transactionFaultHit, owner upgradetx.Owner) error {
	if h.Window != c.Window || h.Fault != c.Fault || h.Owner != owner || !transactionOwnerKnown(owner) || h.BeginNS <= 0 || h.EndNS < h.BeginNS || h.EffectNS < h.BeginNS || h.EffectNS > h.EndNS {
		return fmt.Errorf("M19: fault did not hit the declared owner/window interval")
	}
	if h.Syscall == "" || ((c.Fault == "F3" && h.Errno != "ENOSPC") || (c.Fault == "F4" && h.Errno != "EROFS")) {
		return fmt.Errorf("M19: missing mechanism or actual errno")
	}
	w, err := strconv.Atoi(strings.TrimPrefix(c.Window, "W"))
	if err != nil || (w <= 1 && h.Stage != upgradetx.CutNone) || (w >= 2 && w <= 7 && h.Stage != upgradetx.CutPristine) || (w == 8 && h.Stage != upgradetx.CutPartialActive) || (w >= 9 && h.Stage != upgradetx.CutActive && h.Stage != upgradetx.CutRetiring && h.Stage != upgradetx.CutReleased) {
		return fmt.Errorf("M19: hit cut stage does not match the declared window")
	}
	if c.ReleaseTiming != "none" && (h.DeadlineNS <= 0 || h.ReleasedNS <= 0 || (c.ReleaseTiming == "inside" && h.ReleasedNS >= h.DeadlineNS) || (c.ReleaseTiming == "outside" && h.ReleasedNS <= h.DeadlineNS)) {
		return fmt.Errorf("M19: fault release changed deadline cell")
	}
	return nil
}

// origin: hot-update S8 plan §8.2 / M19
func TestTransactionFaultHitRequiresInterval(t *testing.T) {
	c := expectedTransactionFaultCase(8, 3, "single", "D", "known", "none")
	owner := transactionTestOwner(2)
	h := transactionFaultHit{Window: c.Window, Fault: c.Fault, Owner: owner, Stage: upgradetx.CutPartialActive, Syscall: "write", Errno: "ENOSPC", BeginNS: 100, EndNS: 120, EffectNS: 110}
	if err := judgeTransactionFaultHit(c, h, owner); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*transactionFaultHit){
		"event-before-hit":  func(h *transactionFaultHit) { h.EffectNS = 99 },
		"event-after-hit":   func(h *transactionFaultHit) { h.EffectNS = 121 },
		"pid-reused":        func(h *transactionFaultHit) { h.Owner.StartTicks++ },
		"preflight-not-r1":  func(h *transactionFaultHit) { h.Stage = upgradetx.CutNone },
		"wrong-errno":       func(h *transactionFaultHit) { h.Errno = "EACCES" },
		"no-syscall":        func(h *transactionFaultHit) { h.Syscall = "" },
		"reversed-interval": func(h *transactionFaultHit) { h.EndNS = 90 },
	} {
		t.Run(name, func(t *testing.T) {
			bad := h
			change(&bad)
			if judgeTransactionFaultHit(c, bad, owner) == nil {
				t.Fatal("unhit fault accepted")
			}
		})
	}
	for _, timing := range []string{"inside", "outside"} {
		c = expectedTransactionFaultCase(9, 7, "N3", "D", "known", timing)
		h.Window, h.Fault, h.Stage, h.DeadlineNS, h.ReleasedNS = c.Window, c.Fault, upgradetx.CutActive, 200, 190
		if timing == "outside" {
			h.ReleasedNS = 210
		}
		if err := judgeTransactionFaultHit(c, h, owner); err != nil {
			t.Fatal(err)
		}
		h.ReleasedNS = 200
		if judgeTransactionFaultHit(c, h, owner) == nil {
			t.Fatal("ambiguous deadline boundary accepted")
		}
	}
}

func readTransactionEvidence(name string, value any) error {
	if *transactionEvidenceDir == "" {
		return fmt.Errorf("NOT_RUN pending P01-P12/P14-P19/P21/P26: supervised product evidence is required via -upgrade-transaction-evidence")
	}
	f, err := os.Open(filepath.Join(*transactionEvidenceDir, name+".json"))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	decoder := json.NewDecoder(io.LimitReader(f, 16<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("trailing or over-limit evidence: %v", err)
	}
	return nil
}

// This is the production receipt check, deliberately distinct from manifest
// and oracle controls. It must remain red without real supervised exercises.
// Each round has its own original seal and receipts; reusing a run is rejected.
// origin: hot-update S8 plan §8.2 / §11.2 P25
func TestTransactionFaultReceipts(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransactionFaultReceipts(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "fault receipts x5", g)
	testharness.AssertNoFDLeak(t, "fault receipts x5", fd, 0)
}

func exerciseTransactionFaultReceipts(t *testing.T, round int) {
	t.Helper()
	cases, err := readTransactionFaultCases()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateTransactionFaultCases(cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if c.Inapplicable != "" {
			continue
		}
		var seal transactionOriginalSeal
		var evidence transactionCaseEvidence
		prefix := fmt.Sprintf("round-%d/%s", round, c.Arm)
		if err := readTransactionEvidence(prefix+".original", &seal); err != nil {
			t.Fatal(err)
		}
		if err := readTransactionEvidence(prefix, &evidence); err != nil {
			t.Fatal(err)
		}
		if err := judgeTransactionCase(c, seal, evidence, round); err != nil {
			t.Fatalf("%s: %v", prefix, err)
		}
	}
}

func transactionCaseOracleControl(c transactionFaultCase) (transactionOriginalSeal, transactionCaseEvidence) {
	seal, p := transactionOracleControl()
	seal.RunID = c.Arm + "/round-0"
	seal.FaultOwner = transactionTestOwner(2)
	e := transactionCaseEvidence{SchemaVersion: 1, RunID: seal.RunID, Arm: c.Arm, ScopeDigest: seal.ScopeDigest, Phase: c.Phase, Outcome: c.Outcome, NativeRC: c.RC, OriginalOwner: seal.OriginalOwner, OriginalLive: c.OwnerLane == "L", CurrentOwner: transactionTestOwner(2), CurrentKnown: c.CurrentOwner == "known", Facts: map[string]string{}, Proof: p}
	if !e.CurrentKnown {
		e.CurrentOwner = upgradetx.Owner{}
	}
	e.Hit = transactionFaultHit{Window: c.Window, Fault: c.Fault, Syscall: "test-only-mechanism", Owner: seal.FaultOwner, BeginNS: 100, EndNS: 120, EffectNS: 110, DeadlineNS: 200, ReleasedNS: 190, Stage: upgradetx.CutPristine}
	if c.Fault == "F3" {
		e.Hit.Errno = "ENOSPC"
	}
	if c.Fault == "F4" {
		e.Hit.Errno = "EROFS"
	}
	if c.ReleaseTiming == "outside" {
		e.Hit.ReleasedNS = 210
	}
	w, _ := strconv.Atoi(strings.TrimPrefix(c.Window, "W"))
	if w <= 1 {
		e.Hit.Stage = upgradetx.CutNone
	} else if w == 8 {
		e.Hit.Stage = upgradetx.CutPartialActive
	} else if w >= 9 {
		e.Hit.Stage = upgradetx.CutActive
	}
	for _, fact := range strings.Split(c.Proof, ",") {
		e.Facts[fact] = strings.Repeat("1", 64)
	}
	if c.Outcome == "rolled_back" {
		e.Proof.Attempt.Direction = upgradetx.DirectionReverse
		e.Proof.Attempt.Handle.CutID = strings.Repeat("9", 32)
		e.Proof.Attempt.Handle.Generation++
		e.Proof.Attempt.SourceSHA, e.Proof.Attempt.TargetSHA = seal.Attempt.TargetSHA, seal.Attempt.SourceSHA
		for _, r := range []*upgradetx.Receipt{&e.Proof.Attempt.Control, &e.Proof.Attempt.Resources, &e.Proof.Attempt.Released} {
			r.Stamp.Attempt = e.Proof.Attempt.Handle
			r.Census.Stamp = r.Stamp
			r.RunningSHA = e.Proof.Attempt.TargetSHA
		}
		e.Proof.Observation.RunningSHA, e.Proof.Observation.EffectiveSHA = e.Proof.Attempt.TargetSHA, e.Proof.Attempt.TargetSHA
		e.Proof.Observation.Census.Stamp.Attempt = e.Proof.Attempt.Handle
		for _, resources := range [][]upgradetx.Resource{e.Proof.Attempt.Resources.Census.Resources, e.Proof.Observation.Census.Resources} {
			for i := range resources {
				resources[i].Generation = e.Proof.Attempt.Handle.Generation
			}
		}
		e.Proof.Observation.Current = e.Proof.Attempt
	}
	if c.CurrentOwner == "unknown" {
		e.Proof.Attempt.Phase, e.Proof.Attempt.Outcome = upgradetx.PhaseRecoveryRequired, upgradetx.OutcomeUnknown
	}
	return seal, e
}

// Positive controls reach every applicable expectation; negative controls bind
// results, original/current identities, seal, per-case obligations and rounds.
// These are evidence-oracle tests, distinct from actual product receipts.
// origin: hot-update S8 plan §8.2 / P25
func TestTransactionFaultReceiptOracleControls(t *testing.T) {
	cases, err := readTransactionFaultCases()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		if c.Inapplicable != "" {
			continue
		}
		seal, e := transactionCaseOracleControl(c)
		if err := judgeTransactionCase(c, seal, e, 0); err != nil {
			t.Fatalf("%s: %v", c.Arm, err)
		}
	}
	c := expectedTransactionFaultCase(9, 7, "N3", "D", "known", "inside")
	for name, change := range map[string]func(*transactionCaseEvidence){
		"different-result":  func(e *transactionCaseEvidence) { e.NativeRC = 64 },
		"stale-round":       func(e *transactionCaseEvidence) { e.Round++ },
		"wrong-arm":         func(e *transactionCaseEvidence) { e.Arm = "W00-F0" },
		"old-owner-reused":  func(e *transactionCaseEvidence) { e.OriginalOwner.StartTicks++ },
		"original-live-bit": func(e *transactionCaseEvidence) { e.OriginalLive = true },
		"current-unknown":   func(e *transactionCaseEvidence) { e.CurrentKnown = false },
		"smaller-original":  func(e *transactionCaseEvidence) { e.ScopeDigest = strings.Repeat("9", 64) },
		"missing-proof":     func(e *transactionCaseEvidence) { delete(e.Facts, "original_set") },
	} {
		t.Run(name, func(t *testing.T) {
			seal, e := transactionCaseOracleControl(c)
			change(&e)
			if judgeTransactionCase(c, seal, e, 0) == nil {
				t.Fatal("fault receipt defect accepted")
			}
		})
	}
	unknown := expectedTransactionFaultCase(9, 0, "agent", "D", "unknown", "none")
	seal, e := transactionCaseOracleControl(unknown)
	e.Proof.Attempt.Direction = upgradetx.DirectionReverse
	if judgeTransactionCase(unknown, seal, e, 0) == nil {
		t.Fatal("unknown custody authorized automatic rollback")
	}
	for _, defect := range []string{"source", "binary", "original-digest"} {
		seal, e := transactionCaseOracleControl(c)
		switch defect {
		case "source":
			seal.SourceCommit = ""
		case "binary":
			seal.BinarySHA = ""
		case "original-digest":
			seal.OriginalDigest = strings.Repeat("9", 64)
		}
		if judgeTransactionCase(c, seal, e, 0) == nil {
			t.Fatal("missing independent provenance accepted")
		}
	}
	reverse := expectedTransactionFaultCase(8, 0, "single", "D", "known", "none")
	for _, defect := range []string{"cut", "generation", "source", "target", "direction"} {
		seal, e := transactionCaseOracleControl(reverse)
		switch defect {
		case "cut":
			e.Proof.Attempt.Handle.CutID = seal.Attempt.Handle.CutID
		case "generation":
			e.Proof.Attempt.Handle.Generation = seal.Attempt.Handle.Generation
		case "source":
			e.Proof.Attempt.SourceSHA = seal.Attempt.SourceSHA
		case "target":
			e.Proof.Attempt.TargetSHA = seal.Attempt.TargetSHA
		case "direction":
			e.Proof.Attempt.Direction = upgradetx.DirectionForward
		}
		if judgeTransactionCase(reverse, seal, e, 0) == nil {
			t.Fatal("stale/foreign reverse identity accepted")
		}
	}
}

// Each receipt has its own absent/unknown/binding counterexample. Terminal
// resources are accepted only with independent durable terminal evidence.
// origin: hot-update S8 plan §4.2 / §8.3 / P25
func TestTransactionCompletionOracleControls(t *testing.T) {
	seal, good := transactionOracleControl()
	if err := judgeTransactionCompletion(seal, good); err != nil {
		t.Fatal(err)
	}
	// Scope, expected-set and test-artifact hashes have separate owners. Matching
	// the captured native proofs does not require inventing their hash algorithm.
	otherSeal, other := seal, copyTransactionProof(good)
	otherSeal.ExpectedDigest = strings.Repeat("f", 64)
	other.Observation.Census.ExpectedDigest = otherSeal.ExpectedDigest
	for _, r := range []*upgradetx.Receipt{&other.Attempt.Control, &other.Attempt.Resources, &other.Attempt.Released} {
		r.Census.ExpectedDigest = otherSeal.ExpectedDigest
	}
	other.Observation.Current = other.Attempt
	if err := judgeTransactionCompletion(otherSeal, other); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func(*transactionProof){
		"disk-only":     func(p *transactionProof) { p.Observation.RunningSHA = p.Attempt.SourceSHA },
		"unknown-owner": func(p *transactionProof) { p.Observation.OwnerKnown = false },
		"wrong-current": func(p *transactionProof) { p.Observation.Current.Handle.Generation++ },
		"partial-active": func(p *transactionProof) {
			p.Attempt.CutStage = upgradetx.CutPartialActive
			p.Observation.Current = p.Attempt
		},
		"unknown-empty-census": func(p *transactionProof) { p.Observation.Census.Known = false; p.Observation.Census.Resources = nil },
		"incomplete-census":    func(p *transactionProof) { p.Observation.Census.Complete = false },
		"zero-sequence":        func(p *transactionProof) { p.Observation.Census.Stamp.Sequence = 0 },
		"new-admitting":        func(p *transactionProof) { p.Observation.Census.Admitting = 1 },
		"missing-domain": func(p *transactionProof) {
			p.Attempt.Scope.Domains = p.Attempt.Scope.Domains[1:]
			p.Observation.Current = p.Attempt
		},
		"omitted-new-request": func(p *transactionProof) { p.Admissions = nil },
		"register-only":       func(p *transactionProof) { p.ObservedSubscriptions = nil },
		"flush-failed":        func(p *transactionProof) { p.Flush = false },
		"probe-missing":       func(p *transactionProof) { p.ProbeNonce = "" },
		"old-live-worker":     func(p *transactionProof) { p.Live[0].Generation-- },
	} {
		t.Run(name, func(t *testing.T) {
			bad := copyTransactionProof(good)
			change(&bad)
			if judgeTransactionCompletion(seal, bad) == nil {
				t.Fatal("current/census completion defect accepted")
			}
		})
	}
	for _, kind := range []string{"control", "resources", "released"} {
		for _, defect := range []string{"missing", "unknown", "incomplete", "wrong-cut", "wrong-nonce", "wrong-producer", "stale-sequence", "wrong-sha"} {
			t.Run(kind+"/"+defect, func(t *testing.T) {
				bad := copyTransactionProof(good)
				r := &bad.Attempt.Control
				switch kind {
				case "resources":
					r = &bad.Attempt.Resources
				case "released":
					r = &bad.Attempt.Released
				}
				switch defect {
				case "missing":
					r.EvidenceDigest = ""
				case "unknown":
					r.Known = false
				case "incomplete":
					r.Complete = false
				case "wrong-cut":
					r.Stamp.Attempt.CutID = strings.Repeat("9", 32)
				case "wrong-nonce":
					r.Stamp.Nonce = strings.Repeat("9", 32)
				case "wrong-producer":
					r.Stamp.Producer.StartTicks++
				case "stale-sequence":
					r.Stamp.Sequence--
				case "wrong-sha":
					r.RunningSHA = bad.Attempt.SourceSHA
				}
				bad.Observation.Current = bad.Attempt
				if err := judgeTransactionCompletion(seal, bad); err == nil || !strings.HasPrefix(err.Error(), string(r.Kind)+":") {
					t.Fatalf("receipt defect was not independently rejected: %v", err)
				}
			})
		}
	}
	terminal := copyTransactionProof(good)
	original := seal.Original.Expected[0]
	original.State, original.Custody = "terminal_durable", "released"
	terminal.Attempt.Resources.Census.Resources[0], terminal.Observation.Census.Resources[0] = original, original
	terminal.TerminalDigests = map[string]string{transactionIdentity(original): strings.Repeat("1", 64)}
	terminal.Observation.Current = terminal.Attempt
	if err := judgeTransactionCompletion(seal, terminal); err != nil {
		t.Fatal(err)
	}
	delete(terminal.TerminalDigests, transactionIdentity(original))
	if judgeTransactionCompletion(seal, terminal) == nil {
		t.Fatal("terminal without durable receipt accepted")
	}
}
