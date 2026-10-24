package xferimage

import (
	"reflect"
	"testing"
	"time"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// Live rows have no result-retention clock yet; an ensure patch may retain
// MaxBytes=0 while its resulting bucket seal must still be bounded.
// origin: hot-update S7 plan §2.1 / §2.3 / §8.2 / T20
func TestTransferTrackerImageCarriesUnstartedRetentionAndEnsureHints(t *testing.T) {
	s := trackerFixture("b", "pull")
	s.Entries[0].Retention.ResultUntil = time.Time{}
	s.Entries[0].Retention.Deadline = x.BootDeadline{}
	s.ConfigAttempts[0].Patch.MaxBytes = 0
	trackerRefreshCensus(&s)
	encoded, err := EncodeTracker(s)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeTracker(encoded)
	if err != nil || !reflect.DeepEqual(s, decoded) {
		t.Fatalf("changed live clock or ensure intent: %v", err)
	}
	for _, mutate := range []func(*x.TrackerState){
		func(s *x.TrackerState) {
			s.Entries[0].Retention.Deadline = x.BootDeadline{BootID: s.Origin.BootID, Deadline: 1}
		},
		func(s *x.TrackerState) {
			s.Entries[0].Retention.ResultUntil = s.Entries[0].Permit.CreateUntil.Add(time.Hour)
		},
		func(s *x.TrackerState) { s.Entries[0].Phase = x.PhaseHistoryConfirmed },
		func(s *x.TrackerState) { s.ConfigAttempts[0].Patch.MaxBytes = -1 },
		func(s *x.TrackerState) { s.ConfigAttempts[0].Seal.MaxBytes = 0 },
		func(s *x.TrackerState) { s.ConfigAttempts[0].Attempt.Ordinal = 0 },
	} {
		bad, err := DecodeTracker(encoded)
		if err != nil {
			t.Fatal(err)
		}
		mutate(&bad)
		trackerRefreshCensus(&bad)
		if _, err := EncodeTracker(bad); err == nil {
			t.Fatal("partial retention clock or unsafe config evidence was accepted")
		}
	}
}
