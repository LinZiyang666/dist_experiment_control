// budget_test.go pins the frozen population, clock boundaries and simultaneous
// reservation arithmetic. These are hermetic judge inputs, not product receipts.
// origin: hot-update S10 plan §10.2 / P02
package acceptance

import (
	"math"
	"reflect"
	"strconv"
	"testing"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// origin: hot-update S10 plan §3.4 / §4.4 / §10.2 / P02
func TestFullstackBudgetIncludesScheduledAndCensoredSlots(t *testing.T) {
	t.Run("scheduled_population_and_failed_durations", func(t *testing.T) {
		slots := budgetSlots(5)
		attempts := []contract.Attempt{
			budgetResult(slots[0], 4_000_000_000, 0),
			budgetResult(slots[1], 6_000_000_000, 75),
			{SlotID: slots[3].ID, CallID: "call-3", Phase: "request", StartedNS: slots[3].ScheduledNS, Sent: true},
			budgetResult(slots[4], 1_000_000_000, 1),
		}
		// A completed pre-send failure belongs to the population just like
		// a successful sent result. The slot with no launch stays missing.
		attempts[3].Sent = false
		r, err := ControlBudget(attempts, slots)
		if err != nil {
			t.Fatal(err)
		}
		want := contract.Distribution{Count: 5, Failed: 2, Censored: 1, Missing: 1,
			MinNS: 1_000_000_000, P50NS: 4_000_000_000, P95NS: 6_000_000_000, P99NS: 6_000_000_000, MaxNS: 6_000_000_000}
		if r.Distribution != want {
			t.Fatalf("scheduled/failed/censored distribution = %+v, want %+v", r.Distribution, want)
		}
		if r.Scheduled != 5 || r.Started != 4 || r.Completed != 3 || r.AttemptFailed != 2 || r.AttemptCensored != 1 {
			t.Fatalf("slot and attempt counts lost: %+v", r)
		}
		if r.Result.RC != 1 || r.Result.Verdict != "ASSERT-FAIL" || len(r.Result.Missing) != 2 ||
			len(r.Result.Failures) != 3 || len(r.Result.Inputs) != len(attempts) || r.WorstID != slots[1].ID || r.WorstRaw != attempts[1].Raw {
			t.Fatalf("failure/worst-slot raw evidence lost: %+v", r)
		}
		for i, raw := range r.Result.Inputs {
			if raw != attempts[i].Raw {
				t.Fatalf("attempt %d raw reference changed: %+v, want %+v", i, raw, attempts[i].Raw)
			}
		}
		d, err := Quantiles(attempts, slots)
		if err != nil || d != want {
			t.Fatalf("frozen Quantiles projection = %+v, %v", d, err)
		}
	})
	t.Run("queue_and_internal_attempts_share_absolute_deadline", func(t *testing.T) {
		slot := budgetSlots(1)[0]
		end := budgetResult(slot, 5_000_000_001, 0)
		end.StartedNS = slot.ScheduledNS + 4_900_000_000
		connect := end
		connect.Phase, connect.Index, connect.RC = "connect", 1, 75
		connect.StartedNS, connect.FinishedNS = slot.ScheduledNS, slot.ScheduledNS+1_000_000_000
		r, err := ControlBudget([]contract.Attempt{end, connect}, []contract.Slot{slot})
		if err != nil || r.Distribution.MaxNS != 5_000_000_001 || r.Result.RC != 1 || r.AttemptFailed != 1 || r.Distribution.Failed != 0 {
			t.Fatalf("queue/retry time escaped original deadline: %+v, %v", r, err)
		}
		end.FinishedNS--
		r, err = ControlBudget([]contract.Attempt{connect, end}, []contract.Slot{slot})
		if err != nil || r.Result.RC != 0 || r.Distribution.MaxNS != 5_000_000_000 || len(r.Result.Inputs) != 2 {
			t.Fatalf("exact deadline or retained retry failed: %+v, %v", r, err)
		}
	})
	t.Run("censored_and_missing_are_not_zero_latency_success", func(t *testing.T) {
		slots := budgetSlots(2)
		pending := budgetResult(slots[0], 0, 0)
		pending.Completed, pending.FinishedNS = false, 0
		r, err := ControlBudget([]contract.Attempt{pending}, slots)
		if err != nil || r.Distribution.Count != 2 || r.Distribution.Censored != 1 || r.Distribution.Missing != 1 ||
			r.Distribution.MinNS != -1 || r.Distribution.MaxNS != -1 || r.Distribution.P50NS != -1 || r.Distribution.P95NS != -1 || r.Distribution.P99NS != -1 ||
			r.Result.RC != 4 || r.Result.Verdict != "INCOMPLETE" {
			t.Fatalf("absent duration reported as zero success: %+v, %v", r, err)
		}
	})
	t.Run("terminal_success_cannot_hide_unsent_or_censored_attempts", func(t *testing.T) {
		slot := budgetSlots(1)[0]
		result := budgetResult(slot, 1, 0)
		result.Sent = false
		r, err := ControlBudget([]contract.Attempt{result}, []contract.Slot{slot})
		if err != nil || r.Result.RC != 1 || r.Distribution.Failed != 1 {
			t.Fatalf("unsent operation became successful: %+v, %v", r, err)
		}
		result.Sent = true
		pending := result
		pending.Phase, pending.Index, pending.Completed, pending.FinishedNS = "connect", 1, false, 0
		r, err = ControlBudget([]contract.Attempt{result, pending}, []contract.Slot{slot})
		if err != nil || r.Result.RC != 4 || r.AttemptCensored != 1 || r.Distribution.Failed != 0 || r.Distribution.Missing != 1 || len(r.Result.Missing) != 1 {
			t.Fatalf("terminal erased censored phase: %+v, %v", r, err)
		}
		d, err := Quantiles([]contract.Attempt{result, pending}, []contract.Slot{slot})
		if err != nil || d.Missing != 1 {
			t.Fatalf("frozen quantiles hid censored phase: %+v, %v", d, err)
		}
	})
	t.Run("valid_zero_duration_and_all_slot_classes", func(t *testing.T) {
		for _, class := range []string{"read", "write", "short-exec"} {
			slot := budgetSlots(1)[0]
			slot.Class, slot.ScheduledNS, slot.DeadlineNS = class, 0, ControlLatencyNS
			d, err := Quantiles([]contract.Attempt{budgetResult(slot, 0, 0)}, []contract.Slot{slot})
			if err != nil || d.Count != 1 || d.MinNS != 0 || d.P50NS != 0 || d.MaxNS != 0 {
				t.Fatalf("valid zero duration or class %s rejected: %+v, %v", class, d, err)
			}
		}
	})
	t.Run("closed_phase_union_keeps_all_connection_costs", func(t *testing.T) {
		slot := budgetSlots(1)[0]
		phases := []string{"dns", "proxy", "tls", "ws", "connect", "inbox_probe", "inbox_fallback", "discovery", "banner", "probe", "create", "request", "backoff", "cancel", "result", "freeze", "exec", "import", "auth", "ready", "maintenance"}
		attempts := make([]contract.Attempt, len(phases))
		for i, phase := range phases {
			attempts[i] = budgetResult(slot, int64(i+1), 0)
			attempts[i].Phase, attempts[i].Index = phase, uint64(i)
		}
		// The result is the whole command, not an intermediate phase. Its
		// completion includes every connection/import observation.
		attempts[14].FinishedNS = slot.ScheduledNS + int64(len(phases))
		r, err := ControlBudget(attempts, []contract.Slot{slot})
		if err != nil || r.Result.RC != 0 || len(r.Result.Inputs) != len(phases) {
			t.Fatalf("phase union dropped evidence: %+v, %v", r, err)
		}
	})
	t.Run("nearest_rank_and_max_are_independent", func(t *testing.T) {
		slots := budgetSlots(101)
		attempts := make([]contract.Attempt, len(slots))
		for i, slot := range slots {
			attempts[len(slots)-1-i] = budgetResult(slot, int64(i+1), 0)
		}
		attempts[0].FinishedNS = slots[100].ScheduledNS + ControlLatencyNS + 1
		original := append([]contract.Attempt(nil), attempts...)
		r, err := ControlBudget(attempts, slots)
		if err != nil || r.Distribution.MinNS != 1 || r.Distribution.P50NS != 51 || r.Distribution.P95NS != 96 ||
			r.Distribution.P99NS != 100 || r.Distribution.MaxNS != ControlLatencyNS+1 || r.Result.RC != 1 {
			t.Fatalf("nearest-rank quantile hid maximum: %+v, %v", r, err)
		}
		if !reflect.DeepEqual(attempts, original) {
			t.Fatal("judge reordered caller's raw evidence")
		}
		for _, tc := range []struct{ n, p, want int }{{1, 99, 1}, {2, 50, 1}, {20, 95, 19}, {100, 99, 99}, {101, 99, 100}, {math.MaxInt, 99, math.MaxInt/100*99 + (math.MaxInt%100*99+99)/100}} {
			if got := nearestRank(tc.n, tc.p); got != tc.want {
				t.Fatalf("nearest rank %d/%d = %d, want %d", tc.n, tc.p, got, tc.want)
			}
		}
	})
	t.Run("malformed_slot_or_attempt_is_rejected", func(t *testing.T) {
		slot := budgetSlots(1)[0]
		good := budgetResult(slot, 1, 0)
		cases := []struct {
			name     string
			slots    []contract.Slot
			attempts []contract.Attempt
		}{
			{"empty", nil, nil},
			{"duplicate-slot", []contract.Slot{slot, slot}, nil},
			{"foreign-attempt", []contract.Slot{slot}, []contract.Attempt{{SlotID: "foreign"}}},
			{"duplicate-attempt", []contract.Slot{slot}, []contract.Attempt{good, good}},
		}
		for _, change := range []struct {
			name string
			fn   func(*contract.Slot)
		}{
			{"empty-id", func(s *contract.Slot) { s.ID = "" }},
			{"unknown-class", func(s *contract.Slot) { s.Class = "unknown" }},
			{"negative-schedule", func(s *contract.Slot) { s.ScheduledNS = -1 }},
			{"expired-deadline", func(s *contract.Slot) { s.DeadlineNS = s.ScheduledNS }},
			{"renewed-deadline", func(s *contract.Slot) { s.DeadlineNS++ }},
		} {
			changed := slot
			change.fn(&changed)
			cases = append(cases, struct {
				name     string
				slots    []contract.Slot
				attempts []contract.Attempt
			}{change.name, []contract.Slot{changed}, nil})
		}
		for _, change := range []struct {
			name string
			fn   func(*contract.Attempt)
		}{
			{"empty-call", func(a *contract.Attempt) { a.CallID = "" }},
			{"unknown-phase", func(a *contract.Attempt) { a.Phase = "unknown" }},
			{"before-schedule", func(a *contract.Attempt) { a.StartedNS = slot.ScheduledNS - 1 }},
			{"backward-time", func(a *contract.Attempt) { a.FinishedNS = a.StartedNS - 1 }},
			{"negative-finish", func(a *contract.Attempt) { a.FinishedNS = -1 }},
			{"pending-backward-time", func(a *contract.Attempt) { a.Completed = false; a.FinishedNS = a.StartedNS - 1 }},
		} {
			changed := good
			change.fn(&changed)
			cases = append(cases, struct {
				name     string
				slots    []contract.Slot
				attempts []contract.Attempt
			}{change.name, []contract.Slot{slot}, []contract.Attempt{changed}})
		}
		other := good
		other.Phase, other.Index = "connect", 1
		cases = append(cases, struct {
			name     string
			slots    []contract.Slot
			attempts []contract.Attempt
		}{"duplicate-nonresult", []contract.Slot{slot}, []contract.Attempt{good, other, other}})
		other.Phase, other.Index, other.CallID = "connect", 1, "replacement-call"
		cases = append(cases, struct {
			name     string
			slots    []contract.Slot
			attempts []contract.Attempt
		}{"replacement-call", []contract.Slot{slot}, []contract.Attempt{good, other}})
		other = good
		other.Index++
		cases = append(cases, struct {
			name     string
			slots    []contract.Slot
			attempts []contract.Attempt
		}{"second-result", []contract.Slot{slot}, []contract.Attempt{good, other}})
		other = good
		other.Phase, other.Index = "connect", 1
		other.FinishedNS++
		cases = append(cases, struct {
			name     string
			slots    []contract.Slot
			attempts []contract.Attempt
		}{"post-result-phase", []contract.Slot{slot}, []contract.Attempt{good, other}})
		other.StartedNS, other.FinishedNS = good.FinishedNS+1, 0
		other.Completed = false
		cases = append(cases, struct {
			name     string
			slots    []contract.Slot
			attempts []contract.Attempt
		}{"post-result-start", []contract.Slot{slot}, []contract.Attempt{good, other}})
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := ControlBudget(tc.attempts, tc.slots); err == nil {
					t.Fatal("malformed evidence accepted")
				}
			})
		}
		good.Code = "control_result_unknown"
		r, err := ControlBudget([]contract.Attempt{good}, []contract.Slot{slot})
		if err != nil || r.Result.RC != 1 || r.Distribution.Failed != 1 || r.AttemptFailed != 1 {
			t.Fatalf("unknown outcome accepted: %+v, %v", r, err)
		}
	})
	t.Run("freeze_uses_earliest_pause_and_last_usable", func(t *testing.T) {
		paused := []BudgetMark{budgetMark("output", 200_000_000), budgetMark("input", 100_000_000)}
		usable := []BudgetMark{budgetMark("input", 300_000_000), budgetMark("output", 650_000_000)}
		r, err := FreezeBudget([]string{"output", "input"}, paused, usable)
		if err != nil || r.Distribution.MaxNS != 550_000_000 || r.Result.RC != 1 || len(r.Result.Inputs) != 4 || r.WorstRaw != usable[1].Raw {
			t.Fatalf("earliest-pump/last-usable freeze boundary lost: %+v, %v", r, err)
		}
		usable[1].AtNS = 600_000_000
		r, err = FreezeBudget([]string{"output", "input"}, paused, usable)
		if err != nil || r.Result.RC != 0 || r.Distribution.MaxNS != LocalFreezeNS {
			t.Fatalf("exact freeze limit rejected: %+v, %v", r, err)
		}
		cases := []struct {
			name           string
			required       []string
			paused, usable []BudgetMark
		}{
			{"empty", nil, nil, nil},
			{"missing", []string{"input", "output"}, paused, usable[:1]},
			{"extra-unrequired-pumps", []string{"input"}, paused, usable},
			{"duplicate-required", []string{"input", "input"}, paused, usable},
			{"foreign", []string{"input", "control"}, paused, usable},
			{"duplicate-pause", []string{"input", "output"}, []BudgetMark{paused[0], paused[0]}, usable},
			{"duplicate-usable", []string{"input", "output"}, paused, []BudgetMark{usable[0], usable[0]}},
		}
		remote := append([]BudgetMark(nil), usable...)
		remote[1].Clock = "remote-boot"
		cases = append(cases, struct {
			name           string
			required       []string
			paused, usable []BudgetMark
		}{"remote-clock", []string{"input", "output"}, paused, remote})
		remotePause := append([]BudgetMark(nil), paused...)
		remotePause[0].Clock = "remote-boot"
		// A foreign middle pump must be rejected even when the summary's
		// earliest and latest marks both happen to use the local clock.
		remote[1].AtNS = paused[0].AtNS
		cases = append(cases, struct {
			name           string
			required       []string
			paused, usable []BudgetMark
		}{"different-pump-clocks", []string{"input", "output"}, remotePause, remote})
		backward := append([]BudgetMark(nil), usable...)
		backward[1].AtNS = paused[0].AtNS - 1
		cases = append(cases, struct {
			name           string
			required       []string
			paused, usable []BudgetMark
		}{"backward", []string{"input", "output"}, paused, backward})
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := FreezeBudget(tc.required, tc.paused, tc.usable); err == nil {
					t.Fatal("incomplete or foreign freeze evidence accepted")
				}
			})
		}
	})
	t.Run("flow_includes_first_gap_and_censored_tail", func(t *testing.T) {
		window := BudgetWindow{budgetMark("tcp-forward", 0), budgetMark("tcp-forward", 10_000_000_001)}
		arrival := budgetMark("tcp-forward", 5_000_000_000)
		r, err := FlowGapBudget(window, []BudgetMark{arrival})
		if err != nil || r.Distribution.Count != 2 || r.Distribution.MaxNS != 5_000_000_001 || r.Result.RC != 1 || r.WorstRaw != window.End.Raw {
			t.Fatalf("flow tail omitted: %+v, %v", r, err)
		}
		window.End.AtNS--
		r, err = FlowGapBudget(window, []BudgetMark{arrival})
		if err != nil || r.Result.RC != 0 || r.Distribution.MinNS != LiveFlowGapNS {
			t.Fatalf("exact flow gap rejected: %+v, %v", r, err)
		}
		arrival.AtNS = 5_000_000_001
		r, err = FlowGapBudget(window, []BudgetMark{arrival})
		if err != nil || r.Result.RC != 1 || r.Distribution.MaxNS != 5_000_000_001 {
			t.Fatalf("first gap omitted: %+v, %v", r, err)
		}
		window.End.AtNS = 1
		r, err = FlowGapBudget(window, nil)
		if err != nil || r.Result.RC != 4 || r.Distribution.Missing != 1 {
			t.Fatalf("empty flow coverage became green: %+v, %v", r, err)
		}
		window.End.AtNS = 5_000_000_001
		r, err = FlowGapBudget(window, nil)
		if err != nil || r.Result.RC != 1 {
			t.Fatalf("no-arrival stall disappeared: %+v, %v", r, err)
		}
	})
	t.Run("transfer_metadata_does_not_renew_durable_progress", func(t *testing.T) {
		window := BudgetWindow{budgetMark("push-leg2", 0), budgetMark("push-leg2", 15_000_000_001)}
		progress := []ProgressMark{{budgetMark("push-leg2", 5_000_000_000), 64}, {budgetMark("push-leg2", 14_000_000_000), 64}}
		r, err := TransferProgressBudget(window, 0, progress)
		if err != nil || r.Result.RC != 1 || r.Distribution.MaxNS != TransferProgressNS+1 || len(r.Result.Inputs) != len(progress)+2 {
			t.Fatalf("unchanged watermark renewed timer: %+v, %v", r, err)
		}
		progress[1].Watermark = 65
		r, err = TransferProgressBudget(window, 0, progress)
		if err != nil || r.Result.RC != 0 || r.Distribution.Count != 3 {
			t.Fatalf("real progress rejected: %+v, %v", r, err)
		}
		progress[1].Watermark = 63
		if _, err = TransferProgressBudget(window, 0, progress); err == nil {
			t.Fatal("regressed durable watermark accepted")
		}
		progress[1].Watermark = 65
		progress[1].Mark.Clock = "remote"
		if _, err = TransferProgressBudget(window, 0, progress); err == nil {
			t.Fatal("cross-clock transfer accepted")
		}
		progress[1].Mark.Clock = "local-boot"
		progress[1].Mark.AtNS = 1
		if _, err = TransferProgressBudget(window, 0, progress); err == nil {
			t.Fatal("reordered progress accepted")
		}
		progress[1].Mark.AtNS = window.End.AtNS + 1
		progress[1].Watermark = progress[0].Watermark
		if _, err = TransferProgressBudget(window, 0, progress); err == nil {
			t.Fatal("post-window progress accepted")
		}
		r, err = TransferProgressBudget(window, 64, progress[:1])
		if err != nil || r.Result.RC != 1 || r.Distribution.Missing != 1 {
			t.Fatalf("metadata-only window accepted: %+v, %v", r, err)
		}
	})
	t.Run("component_rollout_and_maintenance_keep_original_starts", func(t *testing.T) {
		for _, limit := range []int64{ComponentHandoverNS, FullStackRolloutNS, MaintenanceNS} {
			window := BudgetWindow{budgetMark("prepare-to-complete", math.MaxInt64-limit), budgetMark("prepare-to-complete", math.MaxInt64)}
			r, err := TimeBudget([]BudgetWindow{window}, limit)
			if err != nil || r.Result.RC != 0 || r.Distribution.MaxNS != limit || r.WorstID != window.Start.ID || r.WorstRaw != window.End.Raw ||
				!reflect.DeepEqual(r.Result.Inputs, []contract.RawRef{window.Start.Raw, window.End.Raw}) {
				t.Fatalf("exact component budget rejected: %+v, %v", r, err)
			}
			window.Start.AtNS--
			r, err = TimeBudget([]BudgetWindow{window}, limit)
			if err != nil || r.Result.RC != 1 {
				t.Fatalf("extended component deadline accepted: %+v, %v", r, err)
			}
		}
		window := BudgetWindow{budgetMark("cut", 1), budgetMark("cut", 2)}
		if _, err := TimeBudget(nil, LocalFreezeNS); err == nil {
			t.Fatal("empty timing population accepted")
		}
		if _, err := TimeBudget([]BudgetWindow{window}, 0); err == nil {
			t.Fatal("invalid ceiling accepted")
		}
		if _, err := TimeBudget([]BudgetWindow{window, window}, LocalFreezeNS); err == nil {
			t.Fatal("duplicate window accepted")
		}
		for _, change := range []func(*BudgetWindow){
			func(w *BudgetWindow) { w.Start.ID = "" },
			func(w *BudgetWindow) { w.End.ID = "foreign" },
			func(w *BudgetWindow) { w.Start.Clock = "" },
			func(w *BudgetWindow) { w.End.Clock = "remote" },
			func(w *BudgetWindow) { w.Start.AtNS = -1 },
			func(w *BudgetWindow) { w.End.AtNS = 0 },
		} {
			changed := window
			change(&changed)
			if _, err := TimeBudget([]BudgetWindow{changed}, LocalFreezeNS); err == nil {
				t.Fatal("unpaired time evidence accepted")
			}
			if _, err := FlowGapBudget(changed, nil); err == nil {
				t.Fatal("unpaired flow window accepted")
			}
		}
		for _, arrival := range []BudgetMark{budgetMark("cut", 0), budgetMark("cut", 3), budgetMark("foreign", 1), {ID: "cut", Clock: "remote", AtNS: 1}} {
			if _, err := FlowGapBudget(window, []BudgetMark{arrival}); err == nil {
				t.Fatal("out-of-window flow arrival accepted")
			}
		}
	})
	t.Run("reservations_use_real_input_rings_and_per_process_peaks", func(t *testing.T) {
		// §3.4's fixed worst agent: three flows (original/two short),
		// nine execs (original/six control/two finite), two PTYs and one
		// full tier-A inline witness, charged by S7's actual payload length.
		rows := []ReplayReservation{{"a0", "flows", "flow", 3, 0}, {"a0", "execs", "exec", 9, 64 << 10}, {"a0", "ptys", "pty", 2, 0}, {"a0", "tier-a", "inline", 1, 8 << 20}}
		totals, err := ReplayPeak(rows, true)
		const expectedPeak int64 = 202440704 // 193.0625MiB, calculated independently in bytes.
		if err != nil || totals["a0"] != expectedPeak || NormalReplayBytes-totals["a0"] != 32440320 {
			t.Fatalf("full normal reservation peak miscounted: %v, %v", totals, err)
		}
		for _, kind := range []struct {
			name        string
			input, want int64
		}{
			{"exec", 0, 16842752}, {"exec", 64 << 10, 16842752}, {"exec", 65537, 16842753}, {"exec", 8 << 20, 25165824},
			{"pty", 0, 8650752}, {"pty", 256 << 10, 8650752}, {"flow", 0, 8388608},
			{"inline", 0, 0}, {"inline", 1, 1}, {"inline", 8 << 20, 8388608},
		} {
			total, err := ReplayPeak([]ReplayReservation{{"a0", "task", kind.name, 1, kind.input}}, true)
			if err != nil || total["a0"] != kind.want {
				t.Fatalf("%s input reservation = %v, %v, want %d", kind.name, total, err, kind.want)
			}
		}
		// 28 flow capacities are exactly224MiB. An additional full-input
		// exec makes225MiB when starting with26 flows; it must be refused.
		for _, tc := range []struct {
			name             string
			rows             []ReplayReservation
			normal, accepted bool
		}{
			{"exact-normal", []ReplayReservation{{"a0", "flow", "flow", 28, 0}}, true, true},
			{"225-MiB", []ReplayReservation{{"a0", "flow", "flow", 26, 0}, {"a0", "exec", "exec", 1, 1 << 20}}, true, false},
			{"exact-hard", []ReplayReservation{{"a0", "flow", "flow", 32, 0}}, false, true},
			{"above-hard", []ReplayReservation{{"a0", "flow", "flow", 33, 0}}, false, false},
			{"normal-not-hard", []ReplayReservation{{"a0", "flow", "flow", 32, 0}}, true, false},
			{"separate-processes", []ReplayReservation{{"a0", "flow", "flow", 28, 0}, {"a1", "flow", "flow", 28, 0}}, true, true},
			{"broker-not-averaged", []ReplayReservation{{"a0", "flow", "flow", 1, 0}, {"broker", "flow", "flow", 29, 0}}, true, false},
		} {
			t.Run(tc.name, func(t *testing.T) {
				if _, err := ReplayPeak(tc.rows, tc.normal); (err == nil) != tc.accepted {
					t.Fatalf("reservation admission = %v, accepted=%v", err, tc.accepted)
				}
			})
		}
		for _, bad := range []ReplayReservation{
			{"", "task", "flow", 1, 0}, {"a0", "", "flow", 1, 0}, {"a0", "task", "flow", 0, 0},
			{"a0", "task", "exec", 1, -1}, {"a0", "task", "unknown", 1, 0}, {"a0", "task", "flow", 1, 1},
			{"a0", "task", "exec", 1, (8 << 20) + 1}, {"a0", "task", "pty", 1, (256 << 10) + 1}, {"a0", "task", "flow", math.MaxInt64, 0},
			{"a0", "task", "inline", 1, (8 << 20) + 1},
		} {
			if _, err := ReplayPeak([]ReplayReservation{bad}, true); err == nil {
				t.Fatalf("malformed reservation accepted: %+v", bad)
			}
		}
		if _, err := ReplayPeak(nil, true); err == nil {
			t.Fatal("empty reservation population accepted")
		}
		if _, err := ReplayPeak([]ReplayReservation{rows[0], rows[0]}, true); err == nil {
			t.Fatal("duplicate reservation identity accepted")
		}
		nearMax := math.MaxInt64 / StreamReplayBytes
		if _, err := ReplayPeak([]ReplayReservation{{"a0", "first", "flow", nearMax, 0}, {"a0", "second", "flow", 1, 0}}, true); err == nil {
			t.Fatal("summed reservation overflow accepted")
		}
	})
	t.Run("frozen_units_and_limits", func(t *testing.T) {
		got := []int64{LocalFreezeNS, ControlLatencyNS, LiveFlowGapNS, TransferProgressNS, ComponentHandoverNS, FullStackRolloutNS, MaintenanceNS,
			ProbeRatePerSecond, MiB, StreamReplayBytes, ExecInputFloorBytes, PTYInputBytes, NormalReplayBytes, NodeReplayBytes, PublicStateBytes, SealedStateBytes, NonReplayBytes, HandoffBytes, HandoffFDs}
		want := []int64{500000000, 5000000000, 5000000000, 10000000000, 120000000000, 900000000000, 8000000000,
			10, 1048576, 8388608, 65536, 262144, 234881024, 268435456, 16777216, 33554432, 67108864, 67108864, 16384}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("frozen budget units changed: %v, want %v", got, want)
		}
	})
}

func budgetSlots(n int) []contract.Slot {
	slots := make([]contract.Slot, n)
	for i := range slots {
		slots[i] = contract.Slot{ID: "slot-" + strconv.Itoa(i), Class: "read", ScheduledNS: 10_000_000_000, DeadlineNS: 15_000_000_000}
	}
	return slots
}

func budgetResult(slot contract.Slot, duration int64, rc int) contract.Attempt {
	return contract.Attempt{SlotID: slot.ID, CallID: "call-" + slot.ID, Phase: "result", Index: 0,
		StartedNS: slot.ScheduledNS, FinishedNS: slot.ScheduledNS + duration, Sent: true, Completed: true, RC: rc,
		Raw: contract.RawRef{Path: slot.ID + ".json", SHA: contract.Digest{1}, Bytes: 1, Schema: "attempt-v1"}}
}

func budgetMark(id string, at int64) BudgetMark {
	return BudgetMark{ID: id, Clock: "local-boot", AtNS: at,
		Raw: contract.RawRef{Path: id + "-" + strconv.FormatInt(at, 10) + ".json", SHA: contract.Digest{1}, Bytes: 1, Schema: "budget-mark-v1"}}
}

// origin: S10 internal review rv-harness F7
func TestControlBudgetRejectsEarlierBusinessRefusal(t *testing.T) {
	slots := []contract.Slot{{ID: "slot", Class: "write", ScheduledNS: 1, DeadlineNS: 1_000_000_000}}
	raw := fixtureRaw("attempt", "attempt")
	attempts := []contract.Attempt{
		{SlotID: "slot", CallID: "original-call", Phase: "create", Index: 1, StartedNS: 1, FinishedNS: 2, Sent: true, Completed: true, RC: 75, Code: "upgrade_deferred", Raw: raw},
		{SlotID: "slot", CallID: "original-call", Phase: "result", Index: 2, StartedNS: 3, FinishedNS: 4, Sent: true, Completed: true, Raw: raw},
	}
	result, err := ControlBudget(attempts, slots)
	if err != nil {
		t.Fatal(err)
	}
	if result.Result.RC == 0 {
		t.Fatalf("business refusal erased: verdict=%s attempt_failed=%d logical_failed=%d", result.Result.Verdict, result.AttemptFailed, result.Distribution.Failed)
	}
}

// origin: S10 internal review rv-harness F7
func TestControlBudgetKeepsRefusalSeparateFromConnectionRetry(t *testing.T) {
	for _, tc := range []struct {
		name, phase, code           string
		rc                          int
		completed, terminal, failed bool
	}{
		{"connection-retry", "connect", "", 75, true, true, false},
		{"dns-retry", "dns", "", 75, true, true, false},
		{"create-refusal", "create", "", 75, true, true, true},
		{"request-refusal", "request", "", 1, true, true, true},
		{"typed-connection-refusal", "connect", "upgrade_deferred", 75, true, true, true},
		{"unknown-business-result", "request", "control_result_unknown", 74, true, true, true},
		{"typed-zero-rc", "create", "upgrade_deferred", 0, true, true, true},
		{"refused-and-censored", "create", "upgrade_deferred", 75, false, true, true},
		{"refused-no-result", "request", "upgrade_deferred", 75, true, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			slot := budgetSlots(1)[0]
			terminal := budgetResult(slot, 4, 0)
			prior := terminal
			prior.Phase, prior.Index, prior.Code, prior.RC = tc.phase, 1, tc.code, tc.rc
			prior.Completed, prior.Sent, prior.FinishedNS = tc.completed, false, slot.ScheduledNS+2
			attempts := []contract.Attempt{prior}
			if tc.terminal {
				attempts = append(attempts, terminal)
			}
			r, err := ControlBudget(attempts, []contract.Slot{slot})
			if err != nil {
				t.Fatal(err)
			}
			wantFailed := uint64(0)
			if tc.failed {
				wantFailed = 1
			}
			if (r.Result.RC != 0) != tc.failed || r.Distribution.Failed != wantFailed || len(r.Result.Inputs) != len(attempts) {
				t.Fatalf("refusal/retry classification lost: %+v", r)
			}
			wantAttemptFailed, wantCensored := uint64(1), uint64(0)
			if !tc.completed {
				wantAttemptFailed, wantCensored = 0, 1
			}
			if r.AttemptFailed != wantAttemptFailed || r.AttemptCensored != wantCensored || r.Scheduled != 1 || r.Started != 1 {
				t.Fatalf("attempt population lost: %+v", r)
			}
			if tc.terminal && (r.Completed != 1 || r.Distribution.MaxNS != 4 || r.WorstRaw != terminal.Raw) {
				t.Fatalf("completed duration lost after refusal: %+v", r)
			}
			if !tc.terminal && (r.Completed != 0 || r.Distribution.Censored != 1 || r.Distribution.MaxNS != -1) {
				t.Fatalf("missing result fabricated after refusal: %+v", r)
			}
		})
	}
}
