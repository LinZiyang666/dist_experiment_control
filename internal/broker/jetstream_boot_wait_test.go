package broker

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// jetstream_boot_wait_test.go — a clustered broker WAITS for a transient JetStream absence
// instead of exiting into a systemd restart loop.
//
// origin: prerelease audit external review follow-up, found by the deploy tier and by
// nothing else. The boot path probed JetStream exactly once with a 1s timeout, and cluster
// mode treats a miss as fatal — so a meta quorum that reformed a few seconds late made the
// broker exit 70, systemd revive it under Restart=always, and the two loop forever. Drill 95
// measured NRestarts reaching 137 with the ready line never printed again.
//
// Every hermetic gate was green throughout: the fatal needs a REAL clustered JetStream that
// is slow to form, which no unit test constructs and no e2e matrix has. What IS testable —
// and what these two cover — is the property that replaced the single shot: it retries
// within a budget, and it stops.

func withShortJetStreamBootBudget(t *testing.T) {
	t.Helper()
	wait, retry := clusteredJetStreamBootWait, clusteredJetStreamBootRetry
	clusteredJetStreamBootWait, clusteredJetStreamBootRetry = 400*time.Millisecond, 40*time.Millisecond
	t.Cleanup(func() { clusteredJetStreamBootWait, clusteredJetStreamBootRetry = wait, retry })
}

func bootWaitBroker(t *testing.T, url string) (*Broker, *nats.Conn) {
	t.Helper()
	b := &Broker{}
	// A DB is required by the SUCCESS path only (the boot reconciles run once JetStream
	// answers), which is exactly the half the positive control exercises.
	b.cfg = Config{Logger: silentLogger(), Now: time.Now, DB: openDB(t)}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return b, nc
}

// TestTheJetStreamBootWaitGivesUpInsteadOfHanging pins the BOUND. An unbounded wait would
// be worse than the crash loop it replaces: a force-singled-out node never recovers, and a
// process that sits there forever looks alive in `systemctl status` while serving nothing.
func TestTheJetStreamBootWaitGivesUpInsteadOfHanging(t *testing.T) {
	withShortJetStreamBootBudget(t)
	b, nc := bootWaitBroker(t, testharness.StartNATS(t)) // deliberately NO JetStream

	start := time.Now()
	_ = waitForClusteredJetStream(context.Background(), b, nc, nil)
	elapsed := time.Since(start)

	if brokerJS(b) != nil {
		t.Fatal("a server without JetStream must not end up with a JetStream handle")
	}
	if elapsed > 20*clusteredJetStreamBootWait {
		t.Fatalf("the boot wait ran for %v against a %v budget — it is not bounded, and an "+
			"ejected node would hang here forever instead of reaching its actionable fatal",
			elapsed, clusteredJetStreamBootWait)
	}
	if elapsed < clusteredJetStreamBootRetry {
		t.Fatalf("the boot wait returned in %v without retrying even once; a transient miss is "+
			"exactly what it exists to survive", elapsed)
	}
}

// TestTheJetStreamBootWaitPicksUpALateJetStream is the positive control: without it the
// test above would pass just as well against a wait that never retries anything.
func TestTheJetStreamBootWaitPicksUpALateJetStream(t *testing.T) {
	withShortJetStreamBootBudget(t)
	clusteredJetStreamBootWait = 10 * time.Second // room for the ensure round trip
	b, nc := bootWaitBroker(t, testharness.StartJSNATS(t))

	_ = waitForClusteredJetStream(context.Background(), b, nc, nil)

	if brokerJS(b) == nil {
		t.Fatal("JetStream was available and the boot wait did not pick it up; the retry either " +
			"never ran or never re-probed, and a clustered broker would then take the fatal path " +
			"against a healthy cluster")
	}
}

// TestTheJetStreamBootWaitHonoursCancellation: shutdown must not have to wait out the budget.
func TestTheJetStreamBootWaitHonoursCancellation(t *testing.T) {
	withShortJetStreamBootBudget(t)
	clusteredJetStreamBootWait = time.Hour // only cancellation can end this
	b, nc := bootWaitBroker(t, testharness.StartNATS(t))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = waitForClusteredJetStream(ctx, b, nc, nil); close(done) }()
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the boot wait ignored context cancellation; broker shutdown would block on it")
	}
}

// TestTheJetStreamBootWaitKeepsRetryingAnEnsureFailure is R2-M3's behavioural half.
//
// origin: prerelease audit external review R2-M3. The first version of the bounded wait
// covered exactly one transient — "AccountInfo does not answer". If AccountInfo SUCCEEDED
// and EnsureEventsStream then failed (a 5s deadline, meta placement still settling, a
// momentary server error), both `Run` and the loop returned immediately, and the crash loop
// came back through a different door. The two developer tests at the time only had the
// no-JetStream and fully-healthy poles, so neither could see the gap between them.
//
// The failure is made DETERMINISTIC rather than timed: a foreign stream is created first
// that already claims the events subject, and JetStream refuses overlapping subjects across
// streams. So AccountInfo answers, ensure fails every attempt, and a correct implementation
// spends its whole budget before giving up — while the old one returned on the first try.
func TestTheJetStreamBootWaitKeepsRetryingAnEnsureFailure(t *testing.T) {
	withShortJetStreamBootBudget(t)
	url := testharness.StartJSNATS(t)
	b, nc := bootWaitBroker(t, url)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "SQUATTER",
		Subjects: []string{proto.SubjSysEvents},
		Storage:  jetstream.MemoryStorage,
	}); err != nil {
		t.Fatalf("could not occupy the events subject, so this test cannot fail the ensure: %v", err)
	}

	start := time.Now()
	retErr := waitForClusteredJetStream(ctx, b, nc, nil)
	elapsed := time.Since(start)

	if brokerJS(b) != nil {
		t.Fatal("the events stream could not be ensured, so the JetStream handle must NOT be published")
	}
	if retErr == nil {
		t.Fatal("the wait swallowed the ensure failure; the caller's fatal then cannot tell " +
			"'the mesh never formed' from 'JetStream answered but would not create the stream', " +
			"which are different operator actions")
	}
	// THE POINT: it kept trying. Returning on the first ensure error would land far below
	// the budget — that is exactly the bug, and it is what this bound distinguishes.
	if elapsed < clusteredJetStreamBootWait {
		t.Fatalf("the wait gave up after %v of a %v budget; an ensure failure is treated as "+
			"terminal instead of transient, so a cluster that is still settling still crash-loops",
			elapsed, clusteredJetStreamBootWait)
	}
}

// TestTheBootWaitStaysObservableWhileItWaits pins the property that actually separates a
// bounded wait from a hang: WHILE IT WAITS, IT SAYS SO.
//
// origin: the deploy sweep that followed external review R2-M3 — and this guard's own
// correction, which is the part worth reading.
//
// Its first version pinned a RATIO: the budget had to be at most half of a 90s "readiness
// window", because drill 95 polls `poll_until 90` for a new `broker: ready` line and a
// broker that waits 90s silently was said to consume the whole budget of the thing deciding
// whether it came back. The deploy-tier logs refute that story outright. Drill 95 kills
// tether-broker and leaves nats-server running, so JetStream is never absent and THIS WAIT
// IS NEVER ENTERED THERE: measured on a quiet host, SIGKILL at 16:27:43 -> `broker: ready`
// at 16:27:45.787, 2.8s end to end, with not one "still waiting for JetStream" line in the
// entire run. The red that prompted the ratio was concurrent load on the drill host.
//
// A GUARD WHOSE STATED REASON IS FALSE IS WORSE THAN NO GUARD: it survives by being green,
// and it teaches its next reader something untrue about the system. So the ratio is gone.
// What is true — and what makes a long wait safe rather than dangerous — is that the wait is
// not silent: anything polling for readiness can see progress instead of having to infer it
// from a process that has printed nothing since boot. That is asserted BEHAVIOURALLY here,
// against the real log, rather than as arithmetic about a window nobody enforces.
func TestTheBootWaitStaysObservableWhileItWaits(t *testing.T) {
	// PRODUCTION values first — withShortJetStreamBootBudget below replaces them.
	if clusteredJetStreamBootWait < 10*clusteredJetStreamBootRetry {
		t.Fatalf("boot wait %v is less than 10 retries of %v; that is not a wait, it is the "+
			"restart loop with extra steps", clusteredJetStreamBootWait, clusteredJetStreamBootRetry)
	}
	if clusteredJetStreamBootRetry > 5*time.Second {
		t.Fatalf("the retry interval is %v: an observer would see the process go quiet for that "+
			"long at a stretch, which is the silence this guard exists to prevent",
			clusteredJetStreamBootRetry)
	}

	withShortJetStreamBootBudget(t)
	var logged bytes.Buffer
	b, nc := bootWaitBroker(t, testharness.StartNATS(t)) // deliberately NO JetStream
	b.cfg.Logger = slog.New(slog.NewTextHandler(&logged, nil))

	_ = waitForClusteredJetStream(context.Background(), b, nc, nil)

	// Requiring MORE THAN ONE is what fails the shape this replaced: a wait that announces
	// itself once at the start and then says nothing until the fatal.
	if n := strings.Count(logged.String(), "still waiting for JetStream"); n < 3 {
		t.Fatalf("the boot wait emitted %d progress lines across a %v budget with a %v retry — a "+
			"process that prints nothing while it waits is indistinguishable from one that has "+
			"hung, and THAT is what would make a long budget dangerous, not the number itself.\n"+
			"log:\n%s", n, clusteredJetStreamBootWait, clusteredJetStreamBootRetry, logged.String())
	}
}
