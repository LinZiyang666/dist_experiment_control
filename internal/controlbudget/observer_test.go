package controlbudget_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S6 plan §8.3 / T35
func TestAttemptObserverNoSilentLoss(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempts.jsonl")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := cli.NewAttemptSink(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	deadline, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		t.Fatal(err)
	}
	// ctx-root: test operation entry.
	ctx := context.Background()
	calls := 0
	err = cli.DoPhaseWithin(ctx, cli.RetryConfig{Deadline: deadline, Family: cli.RetryStableProcess, LogicalID: "slot-17", Observer: sink}, controlbudget.PhaseCreate, func(time.Duration) error {
		calls++
		if calls == 1 {
			return &cli.AttemptError{Err: nats.ErrTimeout, Sent: true, Retryable: true}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A separate slot must survive even if the preceding slot succeeded.
	a, err := controlbudget.Bind(sink, "slot-18").Begin("", controlbudget.PhaseConnect)
	if err != nil {
		t.Fatal(err)
	}
	a.FinishedMonoNS, err = controlbudget.Now()
	if err != nil {
		t.Fatal(err)
	}
	a.Class = "auth"
	if err = sink.Record(a); err != nil {
		t.Fatal(err)
	}
	if err = sink.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	decoder := json.NewDecoder(f)
	want := []struct {
		id      string
		phase   controlbudget.AttemptPhase
		class   string
		sent    bool
		ordinal uint32
	}{
		{"slot-17", controlbudget.PhaseCreate, "timeout", true, 1},
		{"slot-17", controlbudget.PhaseBackoff, "ok", false, 2},
		{"slot-17", controlbudget.PhaseCreate, "ok", true, 3},
		{"slot-17", controlbudget.PhaseResult, "ok", false, 4},
		{"slot-18", controlbudget.PhaseConnect, "auth", false, 1},
	}
	for _, w := range want {
		var got controlbudget.Attempt
		if err = decoder.Decode(&got); err != nil {
			t.Fatalf("missing attempt: %v", err)
		}
		if got.LogicalID != w.id || got.Phase != w.phase || got.Class != w.class || got.Sent != w.sent || got.Ordinal != w.ordinal || got.StartedMonoNS <= 0 || got.FinishedMonoNS < got.StartedMonoNS {
			t.Fatalf("attempt lost or changed: got %+v, want %+v", got, w)
		}
		if got.Phase == controlbudget.PhaseBackoff && got.BackoffNS <= 0 {
			t.Fatal("missing backoff receipt")
		}
	}
	// origin: hot-update S6 internal review rv-ctl-2
	// The terminal census is additive to the attempts above and must match them.
	for _, expected := range []struct {
		id    string
		count uint32
	}{{"slot-17", 4}, {"slot-18", 1}} {
		var closeRow struct {
			Kind         string `json:"kind"`
			LogicalID    string `json:"logical_id"`
			AttemptCount uint32 `json:"attempt_count"`
			Overflow     bool   `json:"overflow"`
		}
		if err = decoder.Decode(&closeRow); err != nil || closeRow.Kind != "observer_close" || closeRow.LogicalID != expected.id || closeRow.AttemptCount != expected.count || closeRow.Overflow {
			t.Fatalf("missing or invalid terminal census: %+v %v", closeRow, err)
		}
	}
	if decoder.More() {
		t.Fatal("unexpected extra receipt")
	}
	if calls != 2 {
		t.Fatalf("calls=%d, want 2", calls)
	}

	// Completed attempts still count toward the limit; success must not reset it.
	file, err = os.Create(filepath.Join(t.TempDir(), "bounded.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	sink, err = cli.NewAttemptSink(file)
	if err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	for i := 0; i < cli.AttemptLimit; i++ {
		a, err = sink.Begin("bounded-slot", controlbudget.PhaseRequest)
		if err != nil {
			t.Fatal(err)
		}
		a.FinishedMonoNS, err = controlbudget.Now()
		if err != nil {
			t.Fatal(err)
		}
		a.Class = "ok"
		if err = sink.Record(a); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = sink.Begin("bounded-slot", controlbudget.PhaseRequest); !errors.Is(err, cli.ErrAttemptOverflow) {
		t.Fatalf("overflow silently lost: %v", err)
	}
	if err = sink.Close(); !errors.Is(err, cli.ErrAttemptOverflow) {
		t.Fatalf("Close erased overflow: %v", err)
	}
	// origin: hot-update S6 internal review rv-ctl-2 / plan §8.3
	// The collector sees the capacity failure in the terminal census too.
	bounded, err := os.Open(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bounded.Close() }()
	boundedDecoder := json.NewDecoder(bounded)
	for range cli.AttemptLimit {
		var a controlbudget.Attempt
		if err = boundedDecoder.Decode(&a); err != nil {
			t.Fatal(err)
		}
	}
	var closed struct {
		Kind         string `json:"kind"`
		AttemptCount uint32 `json:"attempt_count"`
		Overflow     bool   `json:"overflow"`
	}
	if err = boundedDecoder.Decode(&closed); err != nil || closed.Kind != "observer_close" || closed.AttemptCount != cli.AttemptLimit || !closed.Overflow {
		t.Fatalf("terminal census hid capacity failure: %+v %v", closed, err)
	}
}

// origin: hot-update S6 plan §2.1 / §8.2
func TestAdmissionClockRejectsInvalidAndExpiredDeadlines(t *testing.T) {
	for _, deadline := range []controlbudget.Deadline{0, -1, 1} {
		if _, err := controlbudget.Remaining(deadline); err == nil {
			t.Fatalf("accepted invalid/expired deadline %d", deadline)
		}
	}
	for _, budget := range []time.Duration{0, -1, time.Duration(1<<63 - 1)} {
		if _, err := controlbudget.After(budget); err == nil {
			t.Fatalf("accepted invalid budget %s", budget)
		}
	}
	d, err := controlbudget.After(upgradebudget.RequestLatency)
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil || remaining <= 0 || remaining > upgradebudget.RequestLatency {
		t.Fatalf("remaining=%s err=%v", remaining, err)
	}
	if controlbudget.ValidPhase("unregistered") {
		t.Fatal("open phase vocabulary")
	}
}
