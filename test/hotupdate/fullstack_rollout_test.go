//go:build linux && hotupdate_integration

package hotupdate_test

import (
	"encoding/hex"
	"fmt"
	"reflect"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/fullstackharness"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// The component controls are deliberately separately selectable. Passing them
// cannot discharge a missing live assembly, even when every oracle rejects its
// negative input. Each live round owns a fresh original cohort and full plan.
// origin: hot-update S10 plan §10.2 P25 / §11.1–§11.2
func TestMixedWorkloadSurvivesFullStackRollout(t *testing.T) {
	t.Run("contracts", fullstackRolloutControls)
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseMixedRollout(t, round) })
	}
	testharness.AssertNoGoroutineLeak(t, "mixed fixture Start/Sample/Drain/Stop/Join", g)
	testharness.AssertNoFDLeak(t, "mixed fixture Start/Sample/Drain/Stop/Join", fd, 0)
}

func exerciseMixedRollout(t *testing.T, round int) {
	t.Helper()
	begin := time.Now()
	run := newFullstackRollout(t, round)
	defer run.close(t)
	raw, err := run.fixture.Start(t.Context(), run.profile)
	transferMust(t, err)
	transferMust(t, fullstackRolloutRaw(run.profile.EvidenceRoot, raw))
	t.Logf("startup=%s tier=hermetic", time.Since(begin))
	previous := run.capture(t, "originals")
	transferMust(t, fullstackMixedPopulation(run.profile, previous.Work))
	for _, step := range run.profile.Steps {
		before := run.capture(t, "before-"+step.ID)
		transferMust(t, fullstackMixedProgress(previous.Work, before.Work, false))
		transferMust(t, fullstackMixedTransferWindow(step, before.Work))
		// The still-live owner must finish its own five cycles before it can
		// exec. A later clean image cannot erase this image's resource debt.
		transferMust(t, fullstackImageCycles(before.Evidence, step.Target))
		cut := time.Now()
		raw, err = fullstackharness.ApplyFixtureStep(t.Context(), run.fixture, step.ID)
		transferMust(t, err)
		transferMust(t, fullstackRolloutRaw(run.profile.EvidenceRoot, raw))
		after := run.capture(t, "after-"+step.ID)
		transferMust(t, fullstackMixedProgress(before.Work, after.Work, true))
		transferMust(t, fullstackCutIdentity(step, run.profile, before.Evidence, after.Evidence))
		run.executing(t, step, after.Evidence)
		if time.Since(cut) > 120*time.Second {
			t.Fatal("component exceeded its original 120 second budget")
		}
		t.Logf("cut=%s duration=%s", step.ID, time.Since(cut))
		previous = after
	}
	// Capture final live product floors BEFORE natural drain and supervisor
	// cleanup. Kill/Close cannot manufacture a successful retirement census.
	for _, key := range run.profile.Required {
		transferMust(t, fullstackImageCycles(previous.Evidence, key))
	}
	settle := time.Now()
	raw, err = run.fixture.Drain(t.Context(), fullstackRolloutNonce())
	transferMust(t, err)
	transferMust(t, fullstackRolloutRaw(run.profile.EvidenceRoot, raw))
	// P19 does not permit Sample after Drain. The host archives the complete
	// sealed evidence before replying, and that immutable archive is read here.
	final := run.sealed(t)
	transferMust(t, fullstackMixedProgress(previous.Work, final.Work, true))
	for _, work := range final.Work {
		if !work.Settled || work.Sent != work.Received || work.SentDigest != work.ReceivedDigest || work.Waits != 1 {
			t.Fatal("original workload did not naturally settle its exact bytes and sole waiter")
		}
	}
	transferMust(t, fullstackScheduledRequests(run.profile, final.Evidence))
	for _, judge := range []c.JudgeFunc{acceptance.Judge, acceptance.JudgeResources} {
		result, err := judge(final.Evidence)
		if err != nil || result.RC != 0 || result.Verdict != "GREEN" || len(result.Failures)+len(result.Missing) != 0 {
			t.Fatalf("fullstack evidence rejected: result=%+v error=%v", result, err)
		}
	}
	t.Logf("I/O=all-originals settle=%s total=%s", time.Since(settle), time.Since(begin))
}

// These rows are observations of original workload owners, not product state
// that the fixture may reconstruct. Digests contain no plaintext or SS state.
type fullstackMixedWork struct {
	ID, Kind, NID, Handle, Nonce string
	Cohort                       string
	Leg                          int
	Process, Controller          c.Process
	Sent, Received               [2]uint64
	SentDigest, ReceivedDigest   [2]string
	Accepts, Waits               uint64
	DeadlineNS, AtNS, MaxGapNS   int64
	Settled                      bool
}

// origin: hot-update S10 plan §3.1–§3.3 / §8 role cross-wiring
func fullstackMixedPopulation(p c.Profile, work []fullstackMixedWork) error {
	want := map[string]int{}
	for _, key := range p.Required {
		if key.Role != "agent" {
			continue
		}
		want[key.NID+"/exec"] = 1
		want[key.NID+"/transfer"] = 1
		kind := "tcp"
		if key.NID == "a2" || key.NID == "a3" {
			kind = "ssh"
		}
		want[key.NID+"/"+kind] = 1
		if key.NID == "a0" || key.NID == "a2" {
			want[key.NID+"/pty"] = 1
		}
	}
	if len(want) != 14 || len(work) != 14 {
		return fmt.Errorf("mixed cohort requires four flows, four execs, two PTYs and four transfers")
	}
	ids, handles := map[string]bool{}, map[string]bool{}
	for _, row := range work {
		key := row.NID + "/" + row.Kind
		if want[key] != 1 || row.ID == "" || ids[row.ID] || row.Handle == "" || handles[row.Handle] || row.Nonce == "" ||
			row.Process.PID <= 0 || row.Process.StartTicks == 0 || row.Controller.PID <= 0 || row.Controller.StartTicks == 0 ||
			row.Process.Key.NID != row.NID || row.Settled || row.Waits != 0 || row.Accepts != 1 ||
			row.DeadlineNS <= row.AtNS || row.AtNS <= 0 {
			return fmt.Errorf("mixed cohort changed runtime, original identity or live responsibility")
		}
		for side := range 2 {
			if row.Kind == "transfer" && side != row.Leg {
				continue
			}
			digest, err := hex.DecodeString(row.ReceivedDigest[side])
			if err != nil || len(digest) != 32 || row.Received[side] == 0 || row.Sent[side] < row.Received[side] {
				return fmt.Errorf("mixed workload has an empty direction")
			}
		}
		want[key]--
		ids[row.ID], handles[row.Handle] = true, true
	}
	return nil
}

func fullstackMixedProgress(before, after []fullstackMixedWork, advance bool) error {
	if len(before) == 0 || len(before) != len(after) {
		return fmt.Errorf("mixed workload population changed across the cut")
	}
	seen := map[string]bool{}
	for _, old := range before {
		i := slices.IndexFunc(after, func(row fullstackMixedWork) bool { return row.Kind == old.Kind && row.NID == old.NID })
		if i < 0 || seen[old.ID] {
			return fmt.Errorf("original mixed workload disappeared")
		}
		seen[old.ID] = true
		now := after[i]
		if old.Kind == "transfer" && old.Cohort != now.Cohort {
			if advance || !old.Settled || old.Waits != 1 || old.Sent != old.Received || old.SentDigest != old.ReceivedDigest ||
				old.Cohort != "C0" || now.Cohort != "C1" || now.ID == old.ID || now.Handle == old.Handle || now.Nonce == old.Nonce || now.Settled {
				return fmt.Errorf("transfer relay replaced an unsettled original or reused its identity")
			}
			continue
		}
		if now.ID != old.ID || now.Kind != old.Kind || now.NID != old.NID || now.Handle != old.Handle || now.Nonce != old.Nonce ||
			now.Process != old.Process || now.Controller != old.Controller || now.Accepts != old.Accepts ||
			now.DeadlineNS != old.DeadlineNS || now.AtNS <= old.AtNS || now.MaxGapNS > int64(5*time.Second) ||
			(!now.Settled && (now.Waits != 0 || now.AtNS >= now.DeadlineNS)) || now.Waits > 1 {
			return fmt.Errorf("mixed workload reconnected, restarted, renewed its budget or lost its sole waiter")
		}
		for side := range 2 {
			if now.Received[side] < old.Received[side] || now.Sent[side] < old.Sent[side] || now.Received[side] > now.Sent[side] ||
				(advance && !old.Settled && (old.Kind != "transfer" || side == old.Leg) && now.Received[side] == old.Received[side]) ||
				(now.Received[side] == now.Sent[side] && now.ReceivedDigest[side] != now.SentDigest[side]) {
				return fmt.Errorf("original mixed workload lost, duplicated or stopped bytes")
			}
		}
	}
	return nil
}

func fullstackMixedTransferWindow(step c.Step, work []fullstackMixedWork) error {
	want := 0
	if step.Leg == "leg2" {
		want = 1
	}
	count := 0
	for _, row := range work {
		if row.Kind != "transfer" {
			continue
		}
		count++
		if row.Cohort != step.Cohort || row.Leg != want || row.Settled || row.Received[want] == 0 || row.Sent[want] <= row.Received[want] || row.AtNS >= row.DeadlineNS {
			return fmt.Errorf("component cut lacks four original nonempty live transfer legs")
		}
	}
	if count != 4 {
		return fmt.Errorf("component cut omitted a transfer runtime")
	}
	return nil
}

// The expected schedule is reconstructed from the PRE-START profile, never
// from observed attempts. A dropped/censored tail slot remains in its divisor.
// origin: hot-update S10 plan §3.2 / §8 M10C
func fullstackScheduledRequests(p c.Profile, e c.Evidence) error {
	slots, err := fullstackharness.RequestSchedule(p)
	if err != nil || !reflect.DeepEqual(slots, e.Slots) {
		return fmt.Errorf("M10C: observed requests changed the sealed scheduled denominator")
	}
	d, err := acceptance.Quantiles(e.Attempts, slots)
	if err != nil || d.Count != uint64(len(slots)) || d.Failed+d.Censored+d.Missing != 0 || d.MaxNS > int64(5*time.Second) {
		return fmt.Errorf("M10C: failed, censored, missing or late scheduled request: %+v: %v", d, err)
	}
	return nil
}

func fullstackRolloutControls(t *testing.T) {
	p := fullstackControlProfile()
	before := fullstackControlWork(p)
	if err := fullstackMixedPopulation(p, before); err != nil {
		t.Fatal(err)
	}
	after := slices.Clone(before)
	for i := range after {
		after[i].AtNS++
		for side := range 2 {
			after[i].Sent[side]++
			after[i].Received[side]++
		}
	}
	if err := fullstackMixedProgress(before, after, true); err != nil {
		t.Fatal(err)
	}
	for name, change := range map[string]func([]fullstackMixedWork){
		"wrong-runtime":  func(w []fullstackMixedWork) { w[0].NID = "a3" },
		"replace-child":  func(w []fullstackMixedWork) { w[0].Process.StartTicks++ },
		"replace-ctl":    func(w []fullstackMixedWork) { w[0].Controller.PID++ },
		"reconnect":      func(w []fullstackMixedWork) { w[0].Accepts++ },
		"renew-deadline": func(w []fullstackMixedWork) { w[0].DeadlineNS++ },
		"lose-bytes":     func(w []fullstackMixedWork) { w[0].Received[1]-- },
		"corrupt-bytes":  func(w []fullstackMixedWork) { w[0].ReceivedDigest[0] = "corrupt" },
		"duplicate-wait": func(w []fullstackMixedWork) { w[0].Waits = 2 },
	} {
		t.Run(name, func(t *testing.T) {
			rows := slices.Clone(after)
			change(rows)
			if err := fullstackMixedProgress(before, rows, true); err == nil {
				t.Fatal("mixed workload defect accepted")
			}
		})
	}
	slots, err := fullstackharness.RequestSchedule(p)
	transferMust(t, err)
	e := c.Evidence{Slots: slots}
	for _, slot := range slots {
		e.Attempts = append(e.Attempts, c.Attempt{SlotID: slot.ID, CallID: slot.ID, Index: 1, Phase: "result",
			StartedNS: slot.ScheduledNS, FinishedNS: slot.ScheduledNS + 1, Sent: true, Completed: true, Raw: fullstackControlRaw()})
	}
	transferMust(t, fullstackScheduledRequests(p, e))
	t.Run("M10C", func(t *testing.T) {
		e.Attempts = e.Attempts[:len(e.Attempts)-1]
		if err := fullstackScheduledRequests(p, e); err == nil {
			t.Fatal("unlaunched tail request disappeared from the denominator")
		}
	})
	t.Run("transfer-window", func(t *testing.T) {
		rows := fullstackControlWork(p)
		for i := range rows {
			if rows[i].Kind == "transfer" {
				rows[i].Sent[0]++
			}
		}
		step := c.Step{Cohort: "C0", Leg: "leg1"}
		transferMust(t, fullstackMixedTransferWindow(step, rows))
		rows[1].Leg = 1
		if fullstackMixedTransferWindow(step, rows) == nil {
			t.Fatal("wrong live transfer leg accepted")
		}
	})
	t.Run("transfer-relay", func(t *testing.T) {
		old, next := fullstackControlWork(p), fullstackControlWork(p)
		for i := range old {
			next[i].AtNS++
			if old[i].Kind == "transfer" {
				old[i].Settled, old[i].Waits = true, 1
				next[i].Cohort, next[i].ID, next[i].Handle, next[i].Nonce = "C1", "new-"+old[i].ID, "new-"+old[i].Handle, "new-"+old[i].Nonce
			}
		}
		transferMust(t, fullstackMixedProgress(old, next, false))
		old[1].Settled = false
		if fullstackMixedProgress(old, next, false) == nil {
			t.Fatal("unsettled transfer replaced by relay")
		}
	})
}
