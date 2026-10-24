// quantiles.go keeps the predeclared scheduler population independent of the
// attempt stream. A lost launch or terminal receipt cannot shrink that ledger.
// origin: hot-update S10 plan §4.4 / P02
package acceptance

import (
	"errors"
	"fmt"
	"slices"

	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// BudgetReport preserves counts and the worst raw witness alongside quantiles.
// Distribution.Count is the scheduled population. Its numeric quantiles use
// only complete durations, including failures; censored durations are unknown.
// With no complete durations every numeric statistic is -1, never a false zero.
// Missing also covers lost phase evidence, even with a known logical result.
type BudgetReport struct {
	Distribution                   contract.Distribution
	Scheduled, Started, Completed  uint64
	AttemptFailed, AttemptCensored uint64
	WorstID                        string
	WorstRaw                       contract.RawRef
	Result                         contract.Result
}

var _ contract.QuantilesFunc = Quantiles

// Quantiles expects times already projected onto the scheduler's single clock.
// Attempts include every connection phase; only a completed result settles a
// logical slot. A failed connection followed by a successful result is retained
// in ControlBudget's attempt counters without fabricating a failed operation.
// Business failures are data, not parser errors; callers must inspect the
// counters and maximum or use ControlBudget's complete native success judgment.
func Quantiles(attempts []contract.Attempt, slots []contract.Slot) (contract.Distribution, error) {
	report, err := ControlBudget(attempts, slots)
	return report.Distribution, err
}

// ControlBudget measures scheduled-to-result, including queueing and discovery.
// Missing/censored slots keep their identities and raw evidence; a percentile
// below five seconds cannot override a single over-budget operation.
// origin: hot-update S10 plan §3.2 / §4.4 / P02
func ControlBudget(attempts []contract.Attempt, slots []contract.Slot) (BudgetReport, error) {
	r := newBudgetReport()
	r.Scheduled = uint64(len(slots))
	r.Distribution.Count = r.Scheduled
	if len(slots) == 0 {
		return r, errors.New("control budget: empty scheduled population")
	}
	population := make(map[string]contract.Slot, len(slots))
	for _, s := range slots {
		if s.ID == "" || s.ScheduledNS < 0 || s.DeadlineNS <= s.ScheduledNS ||
			s.DeadlineNS-s.ScheduledNS > ControlLatencyNS {
			return r, errors.New("control budget: invalid slot identity or deadline")
		}
		if s.Class != "read" && s.Class != "write" && s.Class != "short-exec" {
			return r, errors.New("control budget: unknown slot class")
		}
		if _, exists := population[s.ID]; exists {
			return r, errors.New("control budget: duplicate scheduled slot")
		}
		population[s.ID] = s
	}
	bySlot := make(map[string][]contract.Attempt, len(slots))
	seen := make(map[attemptIdentity]bool, len(attempts))
	for _, a := range attempts {
		s, exists := population[a.SlotID]
		if !exists || a.CallID == "" || !budgetAttemptPhase(a.Phase) || a.StartedNS < s.ScheduledNS ||
			a.FinishedNS < 0 || a.Completed && a.FinishedNS < a.StartedNS ||
			!a.Completed && a.FinishedNS != 0 && a.FinishedNS < a.StartedNS {
			return r, errors.New("control budget: foreign or invalid attempt")
		}
		key := attemptIdentity{a.SlotID, a.CallID, a.Phase, a.Index}
		if seen[key] {
			return r, errors.New("control budget: duplicate attempt receipt")
		}
		seen[key] = true
		if a.Completed && (a.RC != 0 || a.Code != "") {
			r.AttemptFailed++
		}
		if !a.Completed {
			r.AttemptCensored++
		}
		bySlot[a.SlotID] = append(bySlot[a.SlotID], a)
		r.Result.Inputs = append(r.Result.Inputs, a.Raw)
	}
	durations := make([]int64, 0, len(slots))
	for _, s := range slots {
		rows := bySlot[s.ID]
		if len(rows) == 0 {
			r.Distribution.Missing++
			r.Result.Missing = append(r.Result.Missing, "slot "+s.ID+": launch evidence missing")
			continue
		}
		r.Started++
		var terminal *contract.Attempt
		var sent, censored, refused bool
		for i := range rows {
			a := &rows[i]
			sent = sent || a.Sent
			censored = censored || !a.Completed
			// Connection failures may recover under the same CallID, but a
			// business refusal/unknown is sticky even if a later result succeeds.
			// Typed codes retain that meaning in every phase, as in the scheduler.
			// origin: S10 internal review rv-harness F7
			refused = refused || a.Code != "" || a.RC != 0 && (a.Phase == "create" || a.Phase == "request" || a.Phase == "result")
			// Stable CallID is the boundary between an internal retry and a
			// second business invocation masquerading as the same slot.
			if a.CallID != rows[0].CallID {
				return r, errors.New("control budget: logical call identity changed")
			}
			if a.Phase == "result" && a.Completed {
				if terminal != nil {
					return r, errors.New("control budget: duplicate logical result")
				}
				terminal = a
			}
		}
		if refused {
			r.Distribution.Failed++
			r.Result.Failures = append(r.Result.Failures, "slot "+s.ID+": logical operation failed")
		}
		if terminal == nil {
			r.Distribution.Censored++
			r.Result.Missing = append(r.Result.Missing, "slot "+s.ID+": terminal evidence censored")
			continue
		}
		for _, a := range rows {
			if a.StartedNS > terminal.FinishedNS || a.FinishedNS > terminal.FinishedNS {
				return r, errors.New("control budget: attempt follows logical result")
			}
		}
		r.Completed++
		// A result cannot erase a lost phase completion or claim an
		// unsent operation succeeded. Pre-send failures remain valid data.
		if censored {
			r.Distribution.Missing += 1
			r.Result.Missing = append(r.Result.Missing, "slot "+s.ID+": attempt evidence censored")
		}
		if !refused && !sent {
			r.Distribution.Failed++
			r.Result.Failures = append(r.Result.Failures, "slot "+s.ID+": logical operation failed")
		}
		duration := terminal.FinishedNS - s.ScheduledNS
		durations = append(durations, duration)
		if r.WorstID == "" || duration > r.Distribution.MaxNS {
			r.WorstID, r.WorstRaw = s.ID, terminal.Raw
			r.Distribution.MaxNS = duration
		}
		if terminal.FinishedNS > s.DeadlineNS || duration > ControlLatencyNS {
			r.Result.Failures = append(r.Result.Failures, fmt.Sprintf("slot %s: control latency exceeds deadline", s.ID))
		}
	}
	setQuantiles(&r.Distribution, durations)
	finishBudgetReport(&r)
	return r, nil
}

type attemptIdentity struct {
	slot, call, phase string
	index             uint64
}

// Keep the actual P00/S6 phase union: filtering to sent requests loses DNS,
// auth, cancellation and pre-start failures that consume the same deadline.
func budgetAttemptPhase(phase string) bool {
	switch phase {
	case "dns", "proxy", "tls", "ws", "connect", "inbox_probe", "inbox_fallback", "discovery", "banner", "probe", "create", "request", "backoff", "cancel", "result", "freeze", "exec", "import", "auth", "ready", "maintenance":
		return true
	default:
		return false
	}
}

func newBudgetReport() BudgetReport {
	return BudgetReport{Distribution: contract.Distribution{MinNS: -1, P50NS: -1, P95NS: -1, P99NS: -1, MaxNS: -1},
		Result: contract.Result{Verdict: "SETUP-RED", RC: 2}}
}

func finishBudgetReport(r *BudgetReport) {
	r.Result.Verdict, r.Result.RC = "GREEN", 0
	if len(r.Result.Failures) != 0 {
		r.Result.Verdict, r.Result.RC = "ASSERT-FAIL", 1
	} else if len(r.Result.Missing) != 0 {
		r.Result.Verdict, r.Result.RC = "INCOMPLETE", 4
	}
}

func setQuantiles(d *contract.Distribution, durations []int64) {
	if len(durations) == 0 {
		return
	}
	slices.Sort(durations)
	d.MinNS, d.MaxNS = durations[0], durations[len(durations)-1]
	d.P50NS = durations[nearestRank(len(durations), 50)-1]
	d.P95NS = durations[nearestRank(len(durations), 95)-1]
	d.P99NS = durations[nearestRank(len(durations), 99)-1]
}

// Integer arithmetic avoids float rounding and n*p overflow at large n.
func nearestRank(n, percentile int) int {
	return n/100*percentile + (n%100*percentile+99)/100
}
