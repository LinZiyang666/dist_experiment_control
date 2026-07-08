package broker

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cluster"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// g7_js_health_test.go — G7b #20③: the JS-503 classifier must fire on code 10008 (incl. wrapped) and
// on NOTHING else — a stream-not-found, a timeout, a context cancel, ErrNoResponders, or nil are not
// "JetStream is wedged at 503" and must not raise the sustained-outage alert.
func TestG7ClassifyJSUnavailable(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"js 10008 unavailable", &jetstream.APIError{ErrorCode: jsErrCodeUnavailable}, true},
		{"wrapped 10008", fmt.Errorf("observe replicas: %w", &jetstream.APIError{ErrorCode: jsErrCodeUnavailable}), true},
		{"double-wrapped 10008", fmt.Errorf("a: %w", fmt.Errorf("b: %w", &jetstream.APIError{ErrorCode: jsErrCodeUnavailable})), true},
		{"stream not found (10059)", &jetstream.APIError{ErrorCode: 10059}, false},
		{"no responders", nats.ErrNoResponders, false},
		{"context deadline", context.DeadlineExceeded, false},
		{"context canceled", context.Canceled, false},
		{"plain error", errors.New("boom"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := classifyJSUnavailable(c.err); got != c.want {
				t.Fatalf("classifyJSUnavailable(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

// TestG7JSUnavailableSustainedDetection drives the AlertReconciler's #20③ sustained-503 detection with a
// controllable clock: a single blip does NOT raise, a 503 sustained past jsDownThreshold raises, a
// positive observation clears, a flap re-accrues the FULL threshold (no early re-raise), and a demoted
// leader clears its self-report even mid-outage.
func TestG7JSUnavailableSustainedDetection(t *testing.T) {
	db := reconTestDB(t)
	f := &alertFake{leader: true, voters: 3}
	now := time.Date(2026, 7, 7, 12, 0, 0, 0, time.UTC)
	var jsFlag bool
	var observeErr error
	var observeRep ReplicaReport
	rec := NewAlertReconciler(AlertReconcilerConfig{
		Node: f, DB: db,
		Now:              func() time.Time { return now },
		Observe:          func(context.Context) (ReplicaReport, error) { return observeRep, observeErr },
		SetJSUnavailable: func(v bool) { jsFlag = v },
		Propose: func(plan func(*sql.DB) (*cluster.Command, error)) error {
			cmd, err := plan(db)
			if err != nil {
				return err
			}
			if cmd == nil {
				return nil
			}
			return cluster.ExecCommand(db, cmd)
		},
	})
	js503 := &jetstream.APIError{ErrorCode: jsErrCodeUnavailable}
	tick := func() { _ = rec.ReconcileAlertsOnce(context.Background()) }

	// 1. a single 503 (blip) under the threshold must NOT raise.
	observeErr = js503
	tick()
	if jsFlag {
		t.Fatal("a single 503 under the threshold must not raise")
	}

	// 2. sustained past the threshold → raise.
	now = now.Add(jsDownThreshold + time.Second)
	tick()
	if !jsFlag {
		t.Fatal("a 503 sustained past jsDownThreshold must raise")
	}

	// 3. a positive observation clears the signal.
	observeErr = nil
	observeRep = reportAtTarget()
	tick()
	if jsFlag {
		t.Fatal("a positive observation must clear the JS-503 signal")
	}

	// 4. flap: 503 → positive (resets) → 503 again accrues from the RESET, not the original down-since.
	observeErr, observeRep = js503, ReplicaReport{}
	now = now.Add(time.Minute)
	tick() // jsDownSince = here
	observeErr, observeRep = nil, reportAtTarget()
	now = now.Add(10 * time.Second)
	tick() // positive → reset
	observeErr, observeRep = js503, ReplicaReport{}
	now = now.Add(20 * time.Second)
	tick() // fresh down-since; only 20s elapsed → still under threshold
	if jsFlag {
		t.Fatal("after a positive reset, a fresh 503 must re-accrue the full threshold before raising")
	}

	// 5. a demoted leader clears its self-report even while the outage continues.
	now = now.Add(jsDownThreshold + time.Second)
	tick()
	if !jsFlag {
		t.Fatal("precondition: the sustained 503 should have raised again")
	}
	f.leader = false
	tick()
	if jsFlag {
		t.Fatal("a non-leader must clear its JS-503 self-report (no stale-true answer)")
	}
}
