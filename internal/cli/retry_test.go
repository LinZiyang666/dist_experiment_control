package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var (
	_ DoWithinFunc      = DoWithin
	_ ConnectWithinFunc = ConnectWithin
)

func retryDeadline(t *testing.T, budget time.Duration) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(budget)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func retrySink(t *testing.T) (*AttemptSink, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "receipts.jsonl")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	sink, err := NewAttemptSink(f)
	if err != nil {
		_ = f.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sink.Close() })
	return sink, path
}

// origin: hot-update S6 plan §8.2 / T30 (shared-helper predicate)
func TestRetryUsesRemainingAbsoluteBudget(t *testing.T) {
	budget := upgradebudget.RequestLatency / 100
	d := retryDeadline(t, budget)
	calls := 0
	start := time.Now()
	err := DoWithin(RetryConfig{Deadline: d, Family: RetryReadOnly, LogicalID: "read-budget"}, func(remaining time.Duration) error {
		calls++
		if remaining <= 0 || remaining > budget {
			t.Fatalf("reset deadline: remaining=%s budget=%s", remaining, budget)
		}
		return nats.ErrTimeout
	})
	if !errors.Is(err, nats.ErrTimeout) || calls != 1 {
		t.Fatalf("deadline granted another attempt: calls=%d err=%v", calls, err)
	}
	if elapsed := time.Since(start); elapsed > upgradebudget.RequestLatency {
		t.Fatalf("budget stacked: %s", elapsed)
	}
	for _, invalid := range []RetryFamily{0, 99} {
		if err = DoWithin(RetryConfig{Deadline: retryDeadline(t, upgradebudget.RequestLatency), Family: invalid, LogicalID: "invalid"}, func(time.Duration) error { t.Fatal("invalid policy admitted callback"); return nil }); err == nil {
			t.Fatal("invalid family accepted")
		}
	}
	if err = DoWithin(RetryConfig{Deadline: 0, Family: RetryReadOnly, LogicalID: "invalid"}, func(time.Duration) error { t.Fatal("zero deadline admitted callback"); return nil }); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("zero deadline: %v", err)
	}
}

// origin: hot-update S6 plan §8.2 / T33 / T36 (helper classification)
func TestRetryAuthorityAndSentBoundary(t *testing.T) {
	for _, family := range []RetryFamily{RetryReadOnly, RetryStableProcess, RetryUnsafe, RetryTransfer} {
		for _, refusal := range []error{nats.ErrPermissionViolation, nats.ErrAuthRevoked, nats.ErrAuthExpired, errors.New("invalid signature"), errors.New("explicit unsupported")} {
			calls := 0
			err := DoWithin(RetryConfig{Deadline: retryDeadline(t, upgradebudget.RequestLatency), Family: family, HasAuthReceipt: true, LogicalID: "refusal"}, func(time.Duration) error { calls++; return &AttemptError{Err: refusal, Sent: true, Retryable: true} })
			if calls != 1 || !errors.Is(err, refusal) {
				t.Fatalf("terminal refusal replayed: family=%d calls=%d err=%v", family, calls, err)
			}
		}
	}
	for _, tc := range []struct {
		phase   controlbudget.AttemptPhase
		receipt bool
		sent    bool
		retry   bool
	}{
		{controlbudget.PhaseConnect, false, false, false},
		{controlbudget.PhaseConnect, true, false, true},
		{controlbudget.PhaseRequest, true, true, false},
	} {
		cfg := RetryConfig{Family: RetryReadOnly, HasAuthReceipt: tc.receipt}
		sent, retry, _ := retryDisposition(cfg, tc.phase, nats.ErrAuthorization)
		if sent != tc.sent || retry != tc.retry {
			t.Fatalf("auth boundary changed: %+v: sent=%t retry=%t", tc, sent, retry)
		}
	}
	calls := 0
	err := DoWithin(RetryConfig{Deadline: retryDeadline(t, upgradebudget.RequestLatency), Family: RetryUnsafe, LogicalID: "unsafe"}, func(time.Duration) error {
		calls++
		return &AttemptError{Err: nats.ErrTimeout, Sent: true, Retryable: true}
	})
	if calls != 1 || !errors.Is(err, ErrControlResultUnknown) || !errors.Is(err, nats.ErrTimeout) {
		t.Fatalf("unsafe mutation replayed: calls=%d err=%v", calls, err)
	}
}

// origin: hot-update S6 plan §8.2 / T30
func TestAdmissionEntryAndLateSuccess(t *testing.T) {
	t.Run("observer-consumes-backoff", func(t *testing.T) {
		sink, path := retrySink(t)
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		deadline, err := AdmissionDeadline(ctx)
		if err != nil {
			t.Fatal(err)
		}
		observer := retrySlowObserver{Observer: sink, wait: ctx.Done(), phase: controlbudget.PhaseBackoff}
		err = DoPhaseWithin(t.Context(), RetryConfig{Deadline: deadline, Family: RetryReadOnly, LogicalID: "slow-backoff", Observer: observer}, controlbudget.PhaseRequest, func(time.Duration) error { return nats.ErrTimeout })
		if !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("backoff lost final transport error: %v", err)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			var a controlbudget.Attempt
			if err := json.Unmarshal([]byte(line), &a); err != nil {
				t.Fatal(err)
			}
			if a.Phase == controlbudget.PhaseBackoff && a.BackoffNS != 0 {
				t.Fatalf("observer time started an expired backoff: %+v", a)
			}
		}
	})

	t.Run("observer-consumes-admission", func(t *testing.T) {
		sink, _ := retrySink(t)
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()
		deadline, err := AdmissionDeadline(ctx)
		if err != nil {
			t.Fatal(err)
		}
		observer := retrySlowObserver{Observer: sink, wait: ctx.Done()}
		err = DoPhaseWithin(t.Context(), RetryConfig{Deadline: deadline, Family: RetryReadOnly, LogicalID: "slow-observer", Observer: observer}, controlbudget.PhaseRequest, func(time.Duration) error {
			t.Error("observer time admitted expired I/O")
			return nil
		})
		if !controlbudget.IsExpired(err) {
			t.Fatalf("observer time escaped admission: %v", err)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	})

	// ctx-root: test RunE entry with a user deadline shorter than admission.
	ctx, cancel := context.WithTimeout(context.Background(), upgradebudget.RequestLatency/100)
	defer cancel()
	d, err := AdmissionDeadline(ctx)
	if err != nil {
		t.Fatal(err)
	}
	remaining, err := controlbudget.Remaining(d)
	if err != nil || remaining > upgradebudget.RequestLatency/100 {
		t.Fatalf("user deadline expanded: %s %v", remaining, err)
	}
	cancel()
	if _, err = AdmissionDeadline(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled entry: %v", err)
	}
	d = retryDeadline(t, upgradebudget.RequestLatency/100)
	err = DoWithin(RetryConfig{Deadline: d, Family: RetryReadOnly, LogicalID: "late"}, func(remaining time.Duration) error {
		timer := time.NewTimer(remaining)
		defer timer.Stop()
		<-timer.C
		return nil
	})
	if !errors.Is(err, controlbudget.ErrExpired) {
		t.Fatalf("late success accepted: %v", err)
	}
}

// origin: hot-update S6 plan §8.3 / T35
func TestAttemptSinkRejectsForgeryAndSecretClasses(t *testing.T) {
	for _, mutate := range []func(*controlbudget.Attempt){
		func(a *controlbudget.Attempt) { a.LogicalID = "other-slot" },
		func(a *controlbudget.Attempt) { a.Ordinal++ },
		func(a *controlbudget.Attempt) { a.Phase = controlbudget.PhaseConnect },
		func(a *controlbudget.Attempt) { a.StartedMonoNS-- },
		func(a *controlbudget.Attempt) { a.FinishedMonoNS = a.StartedMonoNS - 1 },
		func(a *controlbudget.Attempt) { a.Class = "secret-token-from-raw-error" },
		func(a *controlbudget.Attempt) { a.BackoffNS = 1 },
	} {
		sink, _ := retrySink(t)
		a, err := sink.Begin("slot", controlbudget.PhaseRequest)
		if err != nil {
			t.Fatal(err)
		}
		a.FinishedMonoNS, err = controlbudget.Now()
		if err != nil {
			t.Fatal(err)
		}
		a.Class = "ok"
		mutate(&a)
		if err = sink.Record(a); !errors.Is(err, ErrAttemptInvalid) {
			t.Fatalf("forged receipt accepted: %v", err)
		}
		if err = sink.Close(); !errors.Is(err, ErrAttemptInvalid) {
			t.Fatalf("invalidity erased: %v", err)
		}
	}
	sink, path := retrySink(t)
	a, err := sink.Begin("slot", controlbudget.PhaseRequest)
	if err != nil {
		t.Fatal(err)
	}
	secretErr := errors.New("secret-token-from-raw-error")
	if err = completeAttempt(sink, a, true, errorClass(secretErr), 0); err != nil {
		t.Fatal(err)
	}
	if err = sink.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), secretErr.Error()) {
		t.Fatal("receipt leaked raw error")
	}
}

// origin: hot-update S6 plan §8.3 / T35
func TestAttemptSinkCapacityIsExplicit(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reader.Close() }()
	sink, err := NewAttemptSink(writer)
	if err != nil {
		_ = writer.Close()
		t.Fatal(err)
	}
	var overflow error
	for i := 0; i < AttemptQueueLimit*4; i++ {
		a, e := sink.Begin(fmt.Sprintf("slot-%d", i), controlbudget.PhaseRequest)
		if e != nil {
			overflow = e
			break
		}
		if e = completeAttempt(sink, a, true, "ok", 0); e != nil {
			overflow = e
			break
		}
	}
	// Unblock the real writer before assertions/Close, including failure paths.
	drained := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, reader); close(drained) }()
	closeErr := sink.Close()
	<-drained
	if !errors.Is(overflow, ErrAttemptOverflow) || !errors.Is(closeErr, ErrAttemptOverflow) {
		t.Fatalf("queue overflow silently dropped: record=%v close=%v", overflow, closeErr)
	}

	sink, _ = retrySink(t)
	a, err := sink.Begin("byte-limit", controlbudget.PhaseRequest)
	if err != nil {
		t.Fatal(err)
	}
	sink.mu.Lock()
	sink.bytes = AttemptByteLimit
	sink.mu.Unlock()
	if err = completeAttempt(sink, a, true, "ok", 0); !errors.Is(err, ErrAttemptOverflow) {
		t.Fatalf("byte overflow silently lost: %v", err)
	}
	if err = sink.Close(); !errors.Is(err, ErrAttemptOverflow) {
		t.Fatalf("Close erased byte overflow: %v", err)
	}
}

// origin: hot-update S6 plan §8.3 / T35
func TestAttemptSinkConcurrentOrdinalsAndWriteFailure(t *testing.T) {
	sink, path := retrySink(t)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Go(func() {
			a, err := sink.Begin("concurrent", controlbudget.PhaseRequest)
			if err == nil {
				err = completeAttempt(sink, a, true, "ok", 0)
			}
			if err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	seen := map[uint32]bool{}
	decoder := json.NewDecoder(f)
	for i := 0; i < 32; i++ {
		var a controlbudget.Attempt
		if err = decoder.Decode(&a); err != nil {
			t.Fatal(err)
		}
		if a.Ordinal == 0 || a.Ordinal > 32 || seen[a.Ordinal] {
			t.Fatalf("ordinal collision: %+v", a)
		}
		seen[a.Ordinal] = true
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	sink, err = NewAttemptSink(writer)
	if err != nil {
		_ = reader.Close()
		_ = writer.Close()
		t.Fatal(err)
	}
	_ = reader.Close()
	a, err := sink.Begin("broken-pipe", controlbudget.PhaseResult)
	if err != nil {
		t.Fatal(err)
	}
	_ = completeAttempt(sink, a, false, "ok", 0)
	if err = sink.Close(); err == nil {
		t.Fatal("writer failure erased by successful control result")
	}
}

// origin: hot-update S6 plan §8.3 / T35
func TestAttemptSinkBlockedWriterJoinsAtDeadline(t *testing.T) {
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	deadline := retryDeadline(t, upgradebudget.RequestLatency/20)
	sink, err := NewAttemptSinkWithin(w, deadline)
	if err != nil {
		_ = w.Close()
		t.Fatal(err)
	}
	for i := 0; i < AttemptQueueLimit/2; i++ {
		a, e := sink.Begin(fmt.Sprintf("blocked-%d", i), controlbudget.PhaseRequest)
		if e != nil {
			break
		}
		if e = completeAttempt(sink, a, true, "ok", 0); e != nil {
			break
		}
	}
	start := time.Now()
	if err = sink.Close(); err == nil {
		t.Fatal("blocked collector claimed successful drain")
	}
	if time.Since(start) > upgradebudget.RequestLatency {
		t.Fatal("blocked writer exceeded exit budget")
	}
	if _, err = w.Write([]byte("late")); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("writer remains open after join: %v", err)
	}
}

// origin: hot-update S6 plan §8.2 / T57
func TestBrokerCLIRetryLeakCycles(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseCLIRetryCancel(t)
		t.Run(fmt.Sprintf("private-fallback-%d", round), func(t *testing.T) { exercisePrivateInboxAdmission(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "CLI admission cancel", beforeG)
	testharness.AssertNoFDLeak(t, "CLI admission cancel", beforeFD, 0)
}

func exerciseCLIRetryCancel(t *testing.T) {
	t.Helper()
	sink, _ := retrySink(t)
	// ctx-root: a single test RunE admission and its cancellation owner.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, peer := net.Pipe()
	defer func() { _ = peer.Close() }()
	started, done, hookDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	blocked, release := make(chan struct{}), make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- DoPhaseWithin(ctx, RetryConfig{Deadline: retryDeadline(t, upgradebudget.RequestLatency), Family: RetryStableProcess, LogicalID: "cancel", Observer: sink}, controlbudget.PhaseRequest, func(remaining time.Duration) error {
			defer close(done)
			_ = client.SetDeadline(time.Now().Add(remaining))
			stop := context.AfterFunc(ctx, func() { _ = client.Close(); close(hookDone) })
			close(started)
			var buf [1]byte
			_, err := client.Read(buf[:])
			if !stop() {
				<-hookDone
			}
			_ = client.Close()
			if ctx.Err() != nil {
				close(blocked)
				<-release // force cleanup to remain in progress after cancellation
				return ctx.Err()
			}
			return err
		})
	}()
	<-started
	cancel()
	<-blocked
	joinProbe := time.NewTimer(upgradebudget.RequestLatency / 100)
	select {
	case early := <-result:
		joinProbe.Stop()
		close(release)
		<-done
		t.Fatalf("retry abandoned callback cleanup after cancellation: %v", early)
	case <-joinProbe.C:
	}
	close(release)
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel result: %v", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("retry returned before the real I/O callback joined")
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	// Exercise a canceled backoff as well as a callback with real blocked I/O.
	// ctx-root: independent command canceled while its retry backoff starts.
	ctx, cancel = context.WithCancel(context.Background())
	err := DoPhaseWithin(ctx, RetryConfig{Deadline: retryDeadline(t, upgradebudget.RequestLatency), Family: RetryReadOnly, LogicalID: "backoff-cancel"}, controlbudget.PhaseConnect, func(time.Duration) error { cancel(); return nats.ErrNoServers })
	cancel()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancel: %v", err)
	}
}

// origin: hot-update S6 plan §8.2 / T35
func TestBannerSkipsAndSharesAdmission(t *testing.T) {
	sink, path := retrySink(t)
	// ctx-root: test command entry.
	ctx := context.Background()
	err := BannerWithin(ctx, RetryConfig{Deadline: retryDeadline(t, ControlBannerWindow/2), LogicalID: "banner-skip", Observer: sink}, func(context.Context) error { t.Fatal("banner consumed remaining primary budget"); return nil })
	if err != nil {
		t.Fatal(err)
	}
	if err = sink.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `"class":"skipped"`) || !strings.Contains(string(body), `"sent":false`) {
		t.Fatalf("missing explicit skipped receipt: %s", body)
	}
}

func retryNATSServer(t *testing.T) (*server.Server, *nats.Conn) {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true, AlwaysEnableNonce: true})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	if !s.ReadyForConnections(upgradebudget.RequestLatency) {
		s.Shutdown()
		s.WaitForShutdown()
		t.Fatal("NATS fixture not ready")
	}
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	return s, nc
}

// origin: hot-update S6 plan §8.2 / T30 (local health seam)
func TestHealthProbeDistinguishesDeadlineFromEmptyCluster(t *testing.T) {
	t.Run("corrupt-health", func(t *testing.T) {
		_, nc := retryNATSServer(t)
		sub, err := nc.Subscribe(proto.SubjCtrlClusterHealth("actor"), func(m *nats.Msg) {
			_ = m.Respond([]byte(`{"node_id":"broker-a"}`))
			_ = m.Respond([]byte(`{`))
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
		if err := nc.FlushTimeout(upgradebudget.RequestLatency); err != nil {
			t.Fatal(err)
		}
		sink, _ := retrySink(t)
		replies, err := ProbeClusterHealthWithin(t.Context(), nc, "actor", RetryConfig{Deadline: retryDeadline(t, time.Second), LogicalID: "corrupt-health", Observer: sink})
		if err == nil || replies != nil {
			t.Fatalf("corrupt health probe failed open: replies=%v err=%v", replies, err)
		}
		if err := sink.Close(); err != nil {
			t.Fatal(err)
		}
	})

	_, nc := retryNATSServer(t)
	// ctx-root: test command entry.
	ctx := context.Background()
	cfg := RetryConfig{Deadline: retryDeadline(t, upgradebudget.RequestLatency), LogicalID: "empty"}
	replies, err := ProbeClusterHealthWithin(ctx, nc, "actor", cfg)
	if err != nil || len(replies) != 0 {
		t.Fatalf("no responders: %v %v", replies, err)
	}
	sub, err := nc.Subscribe(proto.SubjCtrlClusterHealth("actor"), func(*nats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err = nc.FlushTimeout(upgradebudget.RequestLatency); err != nil {
		t.Fatal(err)
	}
	cfg.Deadline = retryDeadline(t, ControlHealthWindow/10)
	replies, err = ProbeClusterHealthWithin(ctx, nc, "actor", cfg)
	if err == nil || replies != nil {
		t.Fatalf("deadline masqueraded as an empty healthy cluster: %v %v", replies, err)
	}
}

// origin: hot-update S6 plan §8.2 / T34; real P19/P20 implementations required.
func TestConnectWithinUsesRealBudgetedTransport(t *testing.T) {
	_, nc := retryNATSServer(t)
	id, err := EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	deadline := retryDeadline(t, time.Second)
	sink, path := retrySink(t)
	observer := BindControlObserver(sink, "deep-connect")
	connected, err := ConnectWithin(strings.Replace(nc.ConnectedUrl(), "127.0.0.1", "localhost", 1), id, deadline, observer,
		nats.Timeout(time.Hour), nats.SetCustomDialer(retryForbiddenDialer{t: t}),
		nats.SkipHostLookup())
	if err != nil {
		t.Fatal(err)
	}
	defer connected.Close()
	if err = connected.FlushTimeout(upgradebudget.RequestLatency); err != nil {
		t.Fatal(err)
	}
	if err = sink.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"dns", "connect", "inbox_probe"} {
		if !strings.Contains(string(body), `"phase":"`+phase+`"`) {
			t.Fatalf("bounded CONNECT lost deep receipt %s: %s", phase, body)
		}
	}
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		t.Fatal(err)
	}
	<-time.After(remaining + 20*time.Millisecond)
	if err = connected.FlushTimeout(upgradebudget.RequestLatency); err != nil {
		t.Fatalf("completed CONNECT kept sticky admission deadline: %v", err)
	}
}

type retryForbiddenDialer struct{ t *testing.T }

func (d retryForbiddenDialer) Dial(string, string) (net.Conn, error) {
	d.t.Error("caller dialer overrode bounded transport")
	return nil, errors.New("unbounded caller dialer")
}

// origin: hot-update S6 plan §8.3 / T35
func TestAttemptSinkCloseOnExec(t *testing.T) {
	const modeKey = "_TETHER_TEST_ATTEMPT_SINK"
	const pathKey = "_TETHER_TEST_ATTEMPT_SINK_PATH"
	if os.Getenv(modeKey) == "inspect" {
		identity, err := os.Stat(os.Getenv(pathKey))
		if err != nil {
			t.Fatal(err)
		}
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			if current, statErr := os.Stat(filepath.Join("/proc/self/fd", entry.Name())); statErr == nil && os.SameFile(identity, current) {
				t.Fatal("receipt descriptor inherited by business child")
			}
		}
		return
	}
	if os.Getenv(modeKey) == "sink" {
		sink, err := AttemptSinkFromEnv(retryDeadline(t, upgradebudget.RequestLatency))
		if err != nil {
			t.Fatal(err)
		}
		if sink == nil {
			t.Fatal("configured receipt sink missing")
		}
		defer func() { _ = sink.Close() }()
		a, err := sink.Begin("inherited", controlbudget.PhaseResult)
		if err != nil {
			t.Fatal(err)
		}
		if err = completeAttempt(sink, a, false, "ok", 0); err != nil {
			t.Fatal(err)
		}
		// ctx-root: isolated child descriptor inspection.
		ctx, cancel := context.WithTimeout(context.Background(), upgradebudget.RequestLatency)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAttemptSinkCloseOnExec$", "-test.count=1")
		cmd.Env = append(cleanReceiptTestEnv(modeKey), modeKey+"=inspect", pathKey+"="+os.Getenv(pathKey))
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("descriptor custody: %v: %s", err, output)
		}
		return
	}
	if runtime.GOOS != "linux" {
		t.Skip("native /proc descriptor identity requires Linux")
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "child-receipt.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	// ctx-root: isolated inherited-FD helper, never an installed tether.
	ctx, cancel := context.WithTimeout(context.Background(), upgradebudget.RequestLatency*2)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAttemptSinkCloseOnExec$", "-test.count=1")
	cmd.ExtraFiles = []*os.File{f}
	cmd.Env = append(cleanReceiptTestEnv(modeKey), modeKey+"=sink", pathKey+"="+f.Name(), AttemptReceiptFDEnv+"=3")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("inherited descriptor helper: %v: %s", err, output)
	}
}

func cleanReceiptTestEnv(key string) []string {
	var env []string
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, key+"=") && !strings.HasPrefix(entry, "_TETHER_TEST_ATTEMPT_SINK_PATH=") && !strings.HasPrefix(entry, AttemptReceiptFDEnv+"=") {
			env = append(env, entry)
		}
	}
	return env
}

// origin: hot-update S6 plan §8.2 / T30 / T57 (real request cancellation)
func TestRequestWithinCancellationKeepsConnectionUsable(t *testing.T) {
	_, nc := retryNATSServer(t)
	observed := make(chan struct{})
	sub, err := nc.Subscribe("admission-cancel", func(*nats.Msg) { close(observed) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err = nc.FlushTimeout(upgradebudget.RequestLatency); err != nil {
		t.Fatal(err)
	}
	// ctx-root: one user operation canceled after its real publish arrived.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sink, _ := retrySink(t)
	result := make(chan error, 1)
	go func() {
		_, requestErr := RequestWithin(ctx, nc, "admission-cancel", []byte("canonical-request"), RetryConfig{Deadline: retryDeadline(t, upgradebudget.RequestLatency), Family: RetryReadOnly, LogicalID: "request-cancel", Observer: sink})
		result <- requestErr
	}()
	select {
	case <-observed:
	case <-time.After(upgradebudget.RequestLatency):
		t.Fatal("real request did not arrive")
	}
	cancel()
	if err = <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("request cancellation: %v", err)
	}
	if err = sink.Close(); err != nil {
		t.Fatal(err)
	}
	if nc.IsClosed() {
		t.Fatal("admission cancellation killed the long-task connection")
	}
	if err = nc.FlushTimeout(upgradebudget.RequestLatency); err != nil {
		t.Fatalf("connection unusable after admission: %v", err)
	}
}

// origin: hot-update S6 plan §8.3 / T35 / T57
func TestAttemptSinkInheritedPipeUsesPoller(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("native inherited FD custody requires Linux")
	}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close(); _ = w.Close() }()
	raw, err := w.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var fd uintptr
	if err = raw.Control(func(current uintptr) { fd = current }); err != nil {
		t.Fatal(err)
	}
	// ctx-root: isolated CLI receipt pipe inherited as non-CLOEXEC fd 3.
	ctx, cancel := context.WithTimeout(context.Background(), upgradebudget.RequestLatency*2)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAttemptSinkCloseOnExec$", "-test.count=1")
	cmd.ExtraFiles = []*os.File{w}
	cmd.Env = append(cleanReceiptTestEnv("_TETHER_TEST_ATTEMPT_SINK"), "_TETHER_TEST_ATTEMPT_SINK=sink", fmt.Sprintf("_TETHER_TEST_ATTEMPT_SINK_PATH=/proc/%d/fd/%d", os.Getpid(), fd), AttemptReceiptFDEnv+"=3")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("inherited receipt pipe: %v: %s", err, output)
	}
	_ = w.Close()
	var a controlbudget.Attempt
	if err = json.NewDecoder(r).Decode(&a); err != nil || a.LogicalID != "inherited" {
		t.Fatalf("inherited pipe lost receipt: %+v %v", a, err)
	}
}

// origin: hot-update S6 plan §8.3 / T35
func TestAttemptSinkConfigurationFailsClosed(t *testing.T) {
	for _, value := range []string{"", "0", "1", "2", "-1", "secret-token", "4294967296"} {
		t.Setenv(AttemptReceiptFDEnv, value)
		if sink, err := AttemptSinkFromEnv(retryDeadline(t, upgradebudget.RequestLatency)); sink != nil || !errors.Is(err, ErrAttemptInvalid) {
			t.Fatalf("bad receipt FD accepted: %q %v", value, err)
		}
	}
	// origin: hot-update S6 internal review rv-ctl-2
	for _, value := range []string{"", "bad/id", "bad\nid", "secret value", strings.Repeat("a", 129)} {
		t.Setenv(AttemptLogicalIDEnv, value)
		if id, err := AttemptLogicalID("fallback"); id != "" || !errors.Is(err, ErrAttemptInvalid) {
			t.Fatalf("invalid collector identity accepted: %q %v", id, err)
		}
	}
}

// The real server denies only the private inbox; the compatibility connection
// must still use bounded DNS and preserve every failure/fallback receipt.
// origin: hot-update S6 plan §8.2 / T34 / T35 / T57CLI
func exercisePrivateInboxAdmission(t *testing.T) {
	t.Helper()
	id, err := EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ns, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		Nkeys: []*server.NkeyUser{{Nkey: id.PublicKey, Permissions: &server.Permissions{
			Publish:   &server.SubjectPermission{Allow: []string{">"}},
			Subscribe: &server.SubjectPermission{Allow: []string{"_INBOX.>"}},
		}}}})
	if err != nil {
		t.Fatal(err)
	}
	ns.Start()
	t.Cleanup(func() { ns.Shutdown(); ns.WaitForShutdown() })
	if !ns.ReadyForConnections(upgradebudget.RequestLatency) {
		t.Fatal("fallback NATS not ready")
	}
	deadline := retryDeadline(t, time.Second)
	sink, path := retrySink(t)
	nc, err := ConnectWithin(strings.Replace(ns.ClientURL(), "127.0.0.1", "localhost", 1), id, deadline, BindControlObserver(sink, "fallback"),
		nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	if !strings.HasPrefix(nc.NewInbox(), "_INBOX.") {
		t.Fatal("proved private refusal did not fall back")
	}
	if err := nc.FlushTimeout(upgradebudget.RequestLatency); err != nil {
		t.Fatal(err)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(body), `"phase":"dns"`) != 2 || !strings.Contains(string(body), `"phase":"inbox_fallback"`) || !strings.Contains(string(body), `"class":"refused"`) {
		t.Fatalf("fallback detached admission or lost receipt: %s", body)
	}
}

// origin: hot-update S6 plan §8.3 / T35; force allocation to consume admission.
type retrySlowObserver struct {
	controlbudget.Observer
	wait  <-chan struct{}
	phase controlbudget.AttemptPhase
}

func (o retrySlowObserver) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	a, err := o.Observer.Begin(id, phase)
	blocked := o.phase
	if blocked == "" {
		blocked = controlbudget.PhaseRequest
	}
	if phase == blocked {
		<-o.wait
	}
	return a, err
}
