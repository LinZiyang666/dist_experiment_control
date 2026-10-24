// budget.go judges local time and reservation evidence without opening raw
// paths or controlling a product. Collectors must supply independently sealed
// boundaries; the limits below never follow an observed maximum upward.
// origin: hot-update S10 plan §3.4 / §4.4 / P02
package acceptance

import (
	"errors"
	"fmt"
	"math"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// These nanosecond/byte pins mirror the frozen product leaf without importing
// a runtime into the pure judge. P29 reconciles them with upgradebudget and the
// actual allocators; MiB is binary, never a rounded display value.
const (
	LocalFreezeNS       int64 = 500_000_000
	ControlLatencyNS    int64 = 5_000_000_000
	LiveFlowGapNS       int64 = 5_000_000_000
	TransferProgressNS  int64 = 10_000_000_000
	ComponentHandoverNS int64 = 120_000_000_000
	FullStackRolloutNS  int64 = 900_000_000_000
	MaintenanceNS       int64 = 8_000_000_000
	ProbeRatePerSecond        = 10
	MiB                 int64 = 1 << 20
	StreamReplayBytes   int64 = 8 * MiB
	ExecInputFloorBytes int64 = 64 << 10
	PTYInputBytes       int64 = 256 << 10
	NormalReplayBytes   int64 = 224 * MiB
	NodeReplayBytes     int64 = 256 * MiB
	PublicStateBytes    int64 = 16 * MiB
	SealedStateBytes    int64 = 32 * MiB
	NonReplayBytes      int64 = 64 * MiB
	HandoffBytes        int64 = 64 * MiB
	HandoffFDs          int64 = 16384
)

// Clock identifies one local BOOTTIME domain, including the process when
// measuring freeze. A collector must not relabel remote samples to subtract
// clocks. ID names the required pump, flow direction, cut or component.
type BudgetMark struct {
	ID, Clock string
	AtNS      int64
	Raw       contract.RawRef
}
type BudgetWindow struct {
	Start, End BudgetMark
}

// TimeBudget retains every sample and enforces its maximum independently of
// percentile summaries. Component windows begin at first Prepare, not Stage's
// return; rollout and maintenance windows keep their original absolute start.
func TimeBudget(windows []BudgetWindow, ceilingNS int64) (BudgetReport, error) {
	r := newBudgetReport()
	if len(windows) == 0 || ceilingNS <= 0 {
		return r, errors.New("time budget: empty population or invalid ceiling")
	}
	seen := make(map[string]bool, len(windows))
	durations := make([]int64, 0, len(windows))
	for _, w := range windows {
		if err := validBudgetWindow(w); err != nil {
			return r, err
		}
		if seen[w.Start.ID] {
			return r, errors.New("time budget: duplicate window identity")
		}
		seen[w.Start.ID] = true
		duration := w.End.AtNS - w.Start.AtNS
		durations = append(durations, duration)
		r.Result.Inputs = append(r.Result.Inputs, w.Start.Raw, w.End.Raw)
		if r.WorstID == "" || duration > r.Distribution.MaxNS {
			r.WorstID, r.WorstRaw = w.Start.ID, w.End.Raw
			r.Distribution.MaxNS = duration
		}
		if duration > ceilingNS {
			r.Result.Failures = append(r.Result.Failures, "window "+w.Start.ID+": duration exceeds budget")
		}
	}
	r.Distribution.Count = uint64(len(windows))
	r.Scheduled, r.Started, r.Completed = r.Distribution.Count, r.Distribution.Count, r.Distribution.Count
	setQuantiles(&r.Distribution, durations)
	finishBudgetReport(&r)
	return r, nil
}

func validBudgetWindow(w BudgetWindow) error {
	if w.Start.ID == "" || w.Start.ID != w.End.ID || w.Start.Clock == "" ||
		w.Start.Clock != w.End.Clock || w.Start.AtNS < 0 || w.End.AtNS < w.Start.AtNS {
		return errors.New("time budget: unpaired identity, clock or timestamp")
	}
	return nil
}

// FreezeBudget joins the exact required pump set. Taking the last pause drops
// earlier downtime; taking the first usable mark drops import/thaw/carrier time.
// Missing endpoints are errors, not a smaller successfully observed pump set.
// origin: hot-update S10 plan §4.4 / TestFullstackBudgetIncludesScheduledAndCensoredSlots
func FreezeBudget(required []string, paused, usable []BudgetMark) (BudgetReport, error) {
	r := newBudgetReport()
	if len(required) == 0 || len(paused) != len(required) || len(usable) != len(required) {
		return r, errors.New("freeze budget: required pump evidence missing")
	}
	starts, ends := budgetMarkSet(paused), budgetMarkSet(usable)
	seen := make(map[string]bool, len(required))
	var first, last BudgetMark
	for i, id := range required {
		start, okStart := starts[id]
		end, okEnd := ends[id]
		if id == "" || seen[id] || !okStart || !okEnd {
			return r, errors.New("freeze budget: foreign or duplicate pump identity")
		}
		seen[id] = true
		if err := validBudgetWindow(BudgetWindow{start, end}); err != nil {
			return r, err
		}
		if i != 0 && start.Clock != first.Clock {
			return r, errors.New("freeze budget: pumps use different clocks")
		}
		if i == 0 || start.AtNS < first.AtNS {
			first = start
		}
		if i == 0 || end.AtNS > last.AtNS {
			last = end
		}
	}
	// The summary is one cut, while the raw list keeps every required pump.
	first.ID, last.ID = "freeze", "freeze"
	r, err := TimeBudget([]BudgetWindow{{first, last}}, LocalFreezeNS)
	r.Result.Inputs = nil
	for _, mark := range paused {
		r.Result.Inputs = append(r.Result.Inputs, mark.Raw)
	}
	for _, mark := range usable {
		r.Result.Inputs = append(r.Result.Inputs, mark.Raw)
	}
	return r, err
}

func budgetMarkSet(marks []BudgetMark) map[string]BudgetMark {
	set := make(map[string]BudgetMark, len(marks))
	for _, mark := range marks {
		set[mark.ID] = mark
	}
	// FreezeBudget checks equal cardinalities and the exact required set.
	// A duplicate necessarily loses a required identity in that bijection.
	return set
}

// FlowGapBudget measures the first gap and the still-open tail as well as
// gaps between arrivals. No arrivals remains missing coverage, even when the
// observation is too short to violate the five-second stall limit.
// origin: hot-update S10 plan §4.4 / P02
func FlowGapBudget(window BudgetWindow, arrivals []BudgetMark) (BudgetReport, error) {
	return gapBudget(window, arrivals, LiveFlowGapNS)
}

// ProgressMark must describe a real JS ACK or Sync durable watermark. Repeated
// metadata reads do not advance Watermark and cannot restart the progress timer.
type ProgressMark struct {
	Mark      BudgetMark
	Watermark uint64
}

// TransferProgressBudget is for a preclassified healthy byte-transfer window;
// metadata/courier waiting has its own evidence and must not be passed as Sync.
func TransferProgressBudget(window BudgetWindow, initial uint64, progress []ProgressMark) (BudgetReport, error) {
	marks := make([]BudgetMark, 0, len(progress))
	previous := window.Start
	for _, p := range progress {
		if err := validBudgetWindow(BudgetWindow{previous, p.Mark}); err != nil || p.Mark.AtNS > window.End.AtNS || p.Watermark < initial {
			return newBudgetReport(), errors.New("transfer budget: invalid durable watermark")
		}
		previous = p.Mark
		if p.Watermark > initial {
			marks = append(marks, p.Mark)
			initial = p.Watermark
		}
	}
	r, err := gapBudget(window, marks, TransferProgressNS)
	// Non-advancing ACK/Sync observations are not new progress, but remain
	// in the evidence population rather than disappearing from raw custody.
	r.Result.Inputs = []contract.RawRef{window.Start.Raw}
	for _, p := range progress {
		r.Result.Inputs = append(r.Result.Inputs, p.Mark.Raw)
	}
	r.Result.Inputs = append(r.Result.Inputs, window.End.Raw)
	return r, err
}

func gapBudget(window BudgetWindow, arrivals []BudgetMark, ceiling int64) (BudgetReport, error) {
	r := newBudgetReport()
	if err := validBudgetWindow(window); err != nil {
		return r, err
	}
	previous := window.Start
	durations := make([]int64, 0, len(arrivals)+1)
	r.Result.Inputs = append(r.Result.Inputs, window.Start.Raw)
	for _, mark := range arrivals {
		if err := validBudgetWindow(BudgetWindow{previous, mark}); err != nil || mark.AtNS > window.End.AtNS {
			return r, errors.New("progress budget: arrival outside local observation window")
		}
		addBudgetGap(&r, &durations, previous, mark, ceiling)
		previous = mark
	}
	addBudgetGap(&r, &durations, previous, window.End, ceiling)
	if len(arrivals) == 0 {
		r.Distribution.Missing = 1
		r.Result.Missing = append(r.Result.Missing, "window "+window.Start.ID+": progress coverage missing")
	}
	r.Distribution.Count = uint64(len(durations))
	r.Scheduled, r.Started, r.Completed = r.Distribution.Count, r.Distribution.Count, r.Distribution.Count
	setQuantiles(&r.Distribution, durations)
	finishBudgetReport(&r)
	return r, nil
}

func addBudgetGap(r *BudgetReport, durations *[]int64, start, end BudgetMark, ceiling int64) {
	duration := end.AtNS - start.AtNS
	gapID := fmt.Sprintf("%s@%d", start.ID, start.AtNS)
	*durations = append(*durations, duration)
	r.Result.Inputs = append(r.Result.Inputs, end.Raw)
	if r.WorstID == "" || duration > r.Distribution.MaxNS {
		r.WorstID = gapID
		r.WorstRaw, r.Distribution.MaxNS = end.Raw, duration
	}
	if duration > ceiling {
		r.Result.Failures = append(r.Result.Failures, "gap "+gapID+": progress exceeds budget")
	}
}

// ReplayReservation describes simultaneous capacity, including retained work
// and duplicates. Process keeps broker and each agent independent: averaging
// four agents' usage must never hide an over-budget broker.
type ReplayReservation struct {
	Process, ID, Kind string
	Count, InputBytes int64
}

// ReplayPeak includes stdin's max(64KiB,input), PTY's 256KiB input ring and
// S7's copied inline payload length (InputBytes for an inline reservation).
// The normal profile uses 224MiB; the independent saturation arm uses 256MiB.
// Caller supplies the complete concurrent population, not observed used bytes.
// origin: hot-update S10 plan §3.4 / P02
func ReplayPeak(reservations []ReplayReservation, normal bool) (map[string]int64, error) {
	if len(reservations) == 0 {
		return nil, errors.New("replay budget: empty reservation population")
	}
	limit := NodeReplayBytes
	if normal {
		limit = NormalReplayBytes
	}
	totals := make(map[string]int64)
	seen := make(map[[2]string]bool)
	for _, row := range reservations {
		key := [2]string{row.Process, row.ID}
		if row.Process == "" || row.ID == "" || seen[key] || row.Count <= 0 || row.InputBytes < 0 {
			return nil, errors.New("replay budget: invalid or duplicate reservation")
		}
		seen[key] = true
		var capacity int64
		switch row.Kind {
		case "flow":
			if row.InputBytes != 0 {
				return nil, errors.New("replay budget: flow has foreign input capacity")
			}
			capacity = StreamReplayBytes
		case "exec":
			if row.InputBytes > StreamReplayBytes {
				return nil, errors.New("replay budget: exec input exceeds allocator limit")
			}
			capacity = 2*StreamReplayBytes + max(ExecInputFloorBytes, row.InputBytes)
		case "pty":
			if row.InputBytes > PTYInputBytes {
				return nil, errors.New("replay budget: PTY input exceeds allocator limit")
			}
			capacity = StreamReplayBytes + PTYInputBytes
		case "inline":
			// S7 prepareReservation reserves len(Inline), including zero for
			// an empty/prepared pull. Its sealed copies belong to SealedBytes.
			capacity = row.InputBytes
			if capacity > StreamReplayBytes {
				return nil, errors.New("replay budget: inline payload exceeds allocator limit")
			}
		default:
			return nil, errors.New("replay budget: unknown reservation kind")
		}
		if capacity != 0 && (row.Count > math.MaxInt64/capacity || totals[row.Process] > math.MaxInt64-row.Count*capacity) {
			return nil, errors.New("replay budget: reservation overflow")
		}
		totals[row.Process] += row.Count * capacity
	}
	for process, total := range totals {
		if total > limit {
			return totals, fmt.Errorf("replay budget: process %s exceeds reservation limit", process)
		}
	}
	return totals, nil
}
