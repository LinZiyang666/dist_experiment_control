package agent

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: simcluster-speed plan §5.3 I₁ — every roster refresh records the wake that drove it, and a
// dropped wake says so. Drill 41's fast-path arm reads these lines to tell "the topology nudge never
// arrived" from "it arrived and was discarded during a rebuild"; without the distinction the
// RosterRefreshInterval cannot be reasoned about at all (research R14).

// recordingHandler keeps every slog record so a test can assert on message + attributes.
type recordingHandler struct {
	mu   sync.Mutex
	recs []map[string]any
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	m := map[string]any{"msg": r.Message}
	r.Attrs(func(a slog.Attr) bool { m[a.Key] = a.Value.Any(); return true })
	h.mu.Lock()
	h.recs = append(h.recs, m)
	h.mu.Unlock()
	return nil
}
func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func (h *recordingHandler) find(msg string, attrs map[string]any) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.recs {
		if r["msg"] != msg {
			continue
		}
		ok := true
		for k, v := range attrs {
			if r[k] != v {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// waitForLine is waitFor (roster_runtime_test.go) with the thing being waited for in the failure text.
func waitForLine(t *testing.T, what string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", what)
}

// TestRosterRefreshLogsItsWakeSource drives the real loop against a responding fixture broker and
// checks the three shapes: a timer-driven refresh, a topology-nudge-driven refresh, and a nudge that
// is dropped because a rebuild is in flight. Swapping the two source labels, or dropping the reason
// from the dropped line, turns exactly one arm red.
func TestRosterRefreshLogsItsWakeSource(t *testing.T) {
	url := startNATS(t)
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	accountPub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingHandler{}
	a := newRosterRehomeAgent(t, accountPub)
	a.cfg.Logger = slog.New(rec)
	a.rosterRefreshNow = make(chan struct{}, 1)
	// A short cadence is the injection seam this package exposes for the loop (Config.RosterRefreshInterval);
	// the production default (3 min, full-jittered) is not what this test is about.
	a.cfg.RosterRefreshInterval = 40 * time.Millisecond
	a.setSessionCancel(func() {})
	host := connectedHostOf(t, url)
	roster := mustBuildRoster(t, seed, accountPub, 7, []proto.RosterBroker{{NodeID: "brk1", PublicHost: "brk1.example",
		NatsRoute: "nats://" + host + ":6222", Phase: proto.RosterPhaseVoter}})
	if !a.adoptRoster(roster) {
		t.Fatal("fixture: adopt initial roster")
	}
	nc, stop := serveRosterRefresh(t, url, a, roster)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); a.rosterRefreshLoop(ctx, nc) }()

	// Arm 1: the timer fires on its own and the outcome names it.
	waitForLine(t, "a timer-driven refresh line", func() bool {
		return rec.find("agent: roster refreshed", map[string]any{"source": rosterWakeTimer})
	})

	// Arm 2: a topology nudge wakes the loop and the outcome names THAT source, not the timer.
	a.rosterRefreshNow <- struct{}{}
	waitForLine(t, "a topology-driven refresh line", func() bool {
		return rec.find("agent: roster refreshed", map[string]any{"source": rosterWakeTopology})
	})

	// Arm 3: a nudge during a rebuild is dropped, and the drop is logged with its source and reason.
	a.rebuilding.Store(true)
	a.rosterRefreshNow <- struct{}{}
	waitForLine(t, "a dropped-wake line naming topology_event/rebuilding", func() bool {
		return rec.find("agent: roster refresh wake dropped", map[string]any{"source": rosterWakeTopology, "reason": "rebuilding"})
	})
	a.rebuilding.Store(false)

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("rosterRefreshLoop did not exit on ctx cancel")
	}
}
