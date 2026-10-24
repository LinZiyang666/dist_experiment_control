package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// origin: hot-update S7 plan §7 / T32
func TestTransferRetryReconcilesBeforeEverySentReplay(t *testing.T) {
	t.Run("shared-helper-sent-boundary", func(t *testing.T) {
		for _, failure := range []error{nats.ErrTimeout, nats.ErrNoResponders,
			nats.ErrConnectionClosed, nats.ErrAuthorization, nats.ErrPermissionViolation,
			context.Canceled, context.DeadlineExceeded, errors.New("invalid reply")} {
			calls := 0
			err := DoPhaseWithin(t.Context(), RetryConfig{Deadline: retryDeadline(t, time.Second),
				Family: RetryTransfer, LogicalID: "same-call"}, controlbudget.PhaseRequest, func(time.Duration) error {
				calls++
				if calls > 1 {
					return nil // expose a helper that secretly resends before status
				}
				return &AttemptError{Err: failure, Sent: true, Retryable: true}
			})
			if calls != 1 || !errors.Is(err, ErrTransferReconcileRequired) || !errors.Is(err, failure) {
				t.Fatalf("sent failure escaped reconciliation: calls=%d failure=%v err=%v", calls, failure, err)
			}
		}
	})

	t.Run("lost-reply-status-same-call", func(t *testing.T) {
		_, nc := retryNATSServer(t)
		var mu sync.Mutex
		var trace []string
		var bodies [][]byte
		reconciled := false
		mutation, err := nc.Subscribe("transfer-retry-mutation", func(m *nats.Msg) {
			mu.Lock()
			bodies = append(bodies, append([]byte(nil), m.Data...))
			trace = append(trace, "send")
			count, allowed := len(bodies), reconciled
			mu.Unlock()
			if count == 1 {
				return // actual accepted request; its reply never arrives
			}
			if !allowed {
				t.Error("second send arrived before original-owner status")
			}
			_ = m.Respond([]byte("exact-response"))
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = mutation.Unsubscribe() }()
		status, err := nc.Subscribe("transfer-retry-status", func(m *nats.Msg) {
			mu.Lock()
			trace = append(trace, "status", "resolve")
			reconciled = true
			mu.Unlock()
			_ = m.Respond([]byte("original-mutation-replay-allowed"))
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = status.Unsubscribe() }()
		if err = nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		sink, path := retrySink(t)
		deadline := retryDeadline(t, time.Second)
		cfg := RetryConfig{Deadline: deadline, LogicalID: "original-call", Observer: sink}
		payload := []byte(`{"call_id":"original-call","transfer_id":"original-transfer","object_version":"original-nuid"}`)
		lostCtx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
		reply, attempt, err := RequestTransferWithin(lostCtx, nc, "transfer-retry-mutation", payload, cfg)
		cancel()
		if reply != nil || !attempt.Sent || !errors.Is(err, ErrTransferReconcileRequired) {
			t.Fatalf("lost reply was retried or hidden: reply=%v attempt=%+v err=%v", reply, attempt, err)
		}
		mu.Lock()
		beforeStatus := append([]string(nil), trace...)
		mu.Unlock()
		if !reflect.DeepEqual(beforeStatus, []string{"send"}) {
			t.Fatalf("helper performed an extra operation: %v", beforeStatus)
		}
		// This caller explicitly supplies the replay proof; the helper has no
		// authority to query status or to mint a different business identity.
		reply, _, err = RequestTransferWithin(t.Context(), nc, "transfer-retry-status", []byte("original-transfer"),
			RetryConfig{Deadline: deadline, LogicalID: "original-status", Observer: sink})
		if err != nil || string(reply.Data) != "original-mutation-replay-allowed" {
			t.Fatalf("status did not prove replay eligibility: %v %v", reply, err)
		}
		reply, attempt, err = RequestTransferWithin(t.Context(), nc, "transfer-retry-mutation", payload, cfg)
		if err != nil || !attempt.Sent || string(reply.Data) != "exact-response" {
			t.Fatalf("same-call replay failed: reply=%v attempt=%+v err=%v", reply, attempt, err)
		}
		mu.Lock()
		gotTrace, gotBodies := append([]string(nil), trace...), append([][]byte(nil), bodies...)
		mu.Unlock()
		if !reflect.DeepEqual(gotTrace, []string{"send", "status", "resolve", "send"}) || len(gotBodies) != 2 ||
			!reflect.DeepEqual(gotBodies[0], payload) || !reflect.DeepEqual(gotBodies[1], payload) {
			t.Fatalf("replay changed order or immutable request: trace=%v bodies=%q", gotTrace, gotBodies)
		}
		if err = sink.Close(); err != nil {
			t.Fatal(err)
		}
		assertTransferAttemptTrace(t, path)
	})

	t.Run("native-no-responders-does-not-resend", func(t *testing.T) {
		_, nc := retryNATSServer(t)
		before := nc.Stats().OutMsgs
		reply, attempt, err := RequestTransferWithin(t.Context(), nc, "transfer-no-responder", nil,
			RetryConfig{Deadline: retryDeadline(t, time.Second), LogicalID: "missing-origin"})
		if reply != nil || !attempt.Sent || !errors.Is(err, nats.ErrNoResponders) ||
			!errors.Is(err, ErrTransferReconcileRequired) || nc.Stats().OutMsgs-before != 1 {
			t.Fatalf("native helper secretly replayed: reply=%v attempt=%+v publishes=%d err=%v",
				reply, attempt, nc.Stats().OutMsgs-before, err)
		}
	})

	t.Run("unsent-connect-may-retry-original-budget", func(t *testing.T) {
		calls := 0
		var budgets []time.Duration
		err := DoPhaseWithin(t.Context(), RetryConfig{Deadline: retryDeadline(t, time.Second), Family: RetryTransfer,
			LogicalID: "original-unsent"}, controlbudget.PhaseConnect, func(remaining time.Duration) error {
			calls++
			budgets = append(budgets, remaining)
			if calls == 1 {
				return &AttemptError{Err: nats.ErrNoServers, Sent: false}
			}
			return nil
		})
		if err != nil || calls != 2 || budgets[1] >= budgets[0] {
			t.Fatalf("unsent retry reset or lost budget: calls=%d budgets=%v err=%v", calls, budgets, err)
		}
	})

	t.Run("native-pre-publish-error-is-unsent", func(t *testing.T) {
		_, nc := retryNATSServer(t)
		before := nc.Stats().OutMsgs
		reply, attempt, err := RequestTransferWithin(t.Context(), nc, "bad subject", []byte("immutable"),
			RetryConfig{Deadline: retryDeadline(t, time.Second), LogicalID: "bad-subject"})
		if reply != nil || attempt.Sent || !errors.Is(err, nats.ErrBadSubject) ||
			errors.Is(err, ErrTransferReconcileRequired) || nc.Stats().OutMsgs != before {
			t.Fatalf("pre-publish failure claimed sent: reply=%v attempt=%+v err=%v", reply, attempt, err)
		}
	})

	t.Run("reply-subscription-denied-before-publish", func(t *testing.T) {
		s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
			Users: []*server.User{{Username: "transport-fixture", Password: "fixture-only", Permissions: &server.Permissions{
				Publish:   &server.SubjectPermission{Allow: []string{"transfer-denied-inbox"}},
				Subscribe: &server.SubjectPermission{Deny: []string{"_INBOX.>"}},
			}}}})
		if err != nil {
			t.Fatal(err)
		}
		s.Start()
		defer func() { s.Shutdown(); s.WaitForShutdown() }()
		if !s.ReadyForConnections(time.Second) {
			t.Fatal("permission fixture not ready")
		}
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("transport-fixture", "fixture-only"),
			nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) {}))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		before := nc.Stats().OutMsgs
		_, attempt, err := RequestTransferWithin(t.Context(), nc, "transfer-denied-inbox", nil,
			RetryConfig{Deadline: retryDeadline(t, time.Second), LogicalID: "denied-inbox"})
		if !errors.Is(err, nats.ErrPermissionViolation) || attempt.Sent || nc.Stats().OutMsgs != before {
			t.Fatalf("reply subscription denial admitted a mutation: attempt=%+v sends=%d err=%v", attempt, nc.Stats().OutMsgs-before, err)
		}
	})

	t.Run("observer-failure-after-send-requires-reconcile", func(t *testing.T) {
		_, nc := retryNATSServer(t)
		sub, err := nc.Subscribe("transfer-observer-failure", func(m *nats.Msg) { _ = m.Respond([]byte("proof")) })
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
		if err = nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		reply, attempt, err := RequestTransferWithin(t.Context(), nc, "transfer-observer-failure", nil,
			RetryConfig{Deadline: retryDeadline(t, time.Second), LogicalID: "observer-failed", Observer: transferRejectObserver{}})
		if reply != nil || !attempt.Sent || !errors.Is(err, ErrTransferReconcileRequired) || !errors.Is(err, ErrAttemptOverflow) {
			t.Fatalf("lost receipt authorized replay: %v %+v %v", reply, attempt, err)
		}
	})

	t.Run("late-reply-is-sent-and-unresolved", func(t *testing.T) {
		calls := 0
		err := DoPhaseWithin(t.Context(), RetryConfig{Deadline: retryDeadline(t, 20*time.Millisecond),
			Family: RetryTransfer, LogicalID: "late-call"}, controlbudget.PhaseRequest, func(remaining time.Duration) error {
			calls++
			timer := time.NewTimer(remaining)
			defer timer.Stop()
			<-timer.C
			return nil
		})
		if calls != 1 || !errors.Is(err, ErrTransferReconcileRequired) || !errors.Is(err, controlbudget.ErrExpired) {
			t.Fatalf("late reply renewed transfer admission: calls=%d err=%v", calls, err)
		}
	})

	t.Run("control-budget-cannot-use-byte-deadline", func(t *testing.T) {
		err := DoPhaseWithin(t.Context(), RetryConfig{Deadline: retryDeadline(t, time.Hour),
			Family: RetryTransfer, LogicalID: "long-byte-deadline"}, controlbudget.PhaseRequest, func(remaining time.Duration) error {
			if remaining > 5*time.Second {
				t.Fatalf("shared helper lent the byte lifetime to control: %v", remaining)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("late-connect-is-unsent", func(t *testing.T) {
		err := DoPhaseWithin(t.Context(), RetryConfig{Deadline: retryDeadline(t, 20*time.Millisecond),
			Family: RetryTransfer, LogicalID: "late-connect"}, controlbudget.PhaseConnect, func(remaining time.Duration) error {
			timer := time.NewTimer(remaining)
			defer timer.Stop()
			<-timer.C
			return nil
		})
		if !errors.Is(err, controlbudget.ErrExpired) || errors.Is(err, ErrTransferReconcileRequired) {
			t.Fatalf("CONNECT invented a sent mutation: %v", err)
		}
	})

	t.Run("attempt-freezes-original-payload", func(t *testing.T) {
		_, nc := retryNATSServer(t)
		sub, err := nc.Subscribe("transfer-payload-copy", func(m *nats.Msg) { _ = m.Respond(m.Data) })
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sub.Unsubscribe() }()
		if err = nc.FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		body := []byte("original")
		observer := &transferBodyObserver{body: body}
		reply, attempt, err := RequestTransferWithin(t.Context(), nc, "transfer-payload-copy", body,
			RetryConfig{Deadline: retryDeadline(t, time.Second), LogicalID: "original-payload", Observer: observer})
		if err != nil || !attempt.Sent || string(reply.Data) != "original" || string(body) == "original" {
			t.Fatalf("caller mutation changed an immutable attempt: reply=%v attempt=%+v err=%v", reply, attempt, err)
		}
	})
}

func assertTransferAttemptTrace(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	decoder := json.NewDecoder(f)
	var sent []string
	var resultUnknown bool
	var lastOrdinal uint32
	counts, closed := map[string]uint32{}, map[string]bool{}
	for {
		var record struct {
			controlbudget.Attempt
			Kind         string `json:"kind"`
			AttemptCount uint32 `json:"attempt_count"`
			Overflow     bool   `json:"overflow"`
		}
		if err = decoder.Decode(&record); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		a := record.Attempt
		if record.Kind != "" {
			if record.Kind != "observer_close" || record.Overflow || closed[a.LogicalID] || record.AttemptCount != counts[a.LogicalID] {
				t.Fatalf("invalid observer close census: %+v", record)
			}
			closed[a.LogicalID] = true
			continue
		}
		counts[a.LogicalID]++
		if a.LogicalID == "original-call" {
			if a.Ordinal != lastOrdinal+1 {
				t.Fatalf("same logical operation lost attempt identity: %+v", a)
			}
			lastOrdinal = a.Ordinal
		}
		if a.Phase == controlbudget.PhaseBackoff {
			t.Fatal("sent failure entered helper backoff")
		}
		if a.Phase == controlbudget.PhaseResult && a.Class == "result_unknown" {
			resultUnknown = true
		}
		if a.Phase == controlbudget.PhaseRequest && a.Sent {
			sent = append(sent, a.LogicalID)
		}
	}
	if !resultUnknown || !reflect.DeepEqual(sent, []string{"original-call", "original-status", "original-call"}) {
		t.Fatalf("attempt trace hid reconciliation: sent=%v unknown=%t", sent, resultUnknown)
	}
	if len(closed) != len(counts) {
		t.Fatal("observer omitted a logical operation's close census")
	}
}

type transferRejectObserver struct{}

func (transferRejectObserver) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	now, err := controlbudget.Now()
	return controlbudget.Attempt{LogicalID: id, Ordinal: 1, Phase: phase, StartedMonoNS: now}, err
}

func (transferRejectObserver) Record(controlbudget.Attempt) error { return ErrAttemptOverflow }
func (transferRejectObserver) Close() error                       { return nil }

type transferBodyObserver struct {
	transferRejectObserver
	body []byte
}

func (o *transferBodyObserver) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	if phase == controlbudget.PhaseRequest {
		copy(o.body, "mutated!")
	}
	return o.transferRejectObserver.Begin(id, phase)
}

func (*transferBodyObserver) Record(controlbudget.Attempt) error { return nil }

// origin: hot-update S7 plan §7 / T32
func TestTransferTransportRejectsUnprovenReplies(t *testing.T) {
	_, nc := retryNATSServer(t)
	id, err := EnsureIdentity(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var requests atomic.Int32
	sub, err := nc.Subscribe("transfer-wire-fixture", func(m *nats.Msg) {
		requests.Add(1)
		_ = m.Respond([]byte("wire-response"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	if err = nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	for _, code := range []string{"", "not_a_member", "transfer_owner_recovering"} {
		wire := &transferWireFixture{subject: "transfer-wire-fixture", response: xfercontract.Response{Code: code}}
		transport, err := NewTransferTransport(nc, nc.ConnectedUrl(), id, wire, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, attempt, err := transport.Request(xfercontract.Request{CallID: "same-call", Op: xfercontract.OpStatus}, retryDeadline(t, time.Second))
		if err != nil || !attempt.Sent || response.Code != code {
			t.Fatalf("validated response was converted to retry: code=%q response=%+v attempt=%+v err=%v", code, response, attempt, err)
		}
	}
	malformed := errors.New("unproven response")
	wire := &transferWireFixture{subject: "transfer-wire-fixture", decodeErr: malformed}
	transport, err := NewTransferTransport(nc, nc.ConnectedUrl(), id, wire, nil)
	if err != nil {
		t.Fatal(err)
	}
	response, attempt, err := transport.Request(xfercontract.Request{CallID: "same-call", Op: xfercontract.OpPrepare}, retryDeadline(t, time.Second))
	if !reflect.DeepEqual(response, xfercontract.Response{}) || !attempt.Sent || !errors.Is(err, ErrTransferReconcileRequired) || !errors.Is(err, malformed) || requests.Load() != 4 {
		t.Fatalf("unvalidated reply implied success or replay: response=%+v attempt=%+v requests=%d err=%v", response, attempt, requests.Load(), err)
	}
	wire.encodeErr = errors.New("invalid union")
	_, attempt, err = transport.Request(xfercontract.Request{CallID: "same-call"}, retryDeadline(t, time.Second))
	if !errors.Is(err, wire.encodeErr) || attempt.Sent || requests.Load() != 4 {
		t.Fatalf("invalid request reached transport: %+v requests=%d err=%v", attempt, requests.Load(), err)
	}
	wire.encodeErr = nil
	for _, callID := range []string{"", "bad/id", "bad\nid"} {
		if _, attempt, err = transport.Request(xfercontract.Request{CallID: callID}, retryDeadline(t, time.Second)); err == nil || attempt.Sent {
			t.Fatalf("invalid CallID sent: %q %+v %v", callID, attempt, err)
		}
	}
	if _, _, err = transport.Request(xfercontract.Request{CallID: "same-call"}, 0); !errors.Is(err, controlbudget.ErrInvalidDeadline) || requests.Load() != 4 {
		t.Fatalf("invalid deadline reached transport: requests=%d err=%v", requests.Load(), err)
	}
	// Only pre-append errors prove an attempt unsent; a socket error can hide
	// an accepted request and therefore must retain the reconciliation duty.
	if !transferPublishMayHaveEscaped(errors.New("partial socket write")) || transferPublishMayHaveEscaped(nats.ErrMaxPayload) {
		t.Fatal("partial-publish outcome was guessed absent")
	}
	deadline := retryDeadline(t, time.Hour)
	bounded, err := transferControlDeadline(deadline)
	remaining, clockErr := controlbudget.Remaining(bounded)
	if err != nil || clockErr != nil || remaining > 5*time.Second {
		t.Fatalf("byte deadline became control deadline: remaining=%v err=%v clock=%v", remaining, err, clockErr)
	}
}

// This fixture tests transport composition only. It does not claim to encode
// P01 protocol bytes or to implement P25's status/receipt decision engine.
type transferWireFixture struct {
	subject              string
	response             xfercontract.Response
	encodeErr, decodeErr error
}

func (w *transferWireFixture) Encode(xfercontract.Request) (string, []byte, error) {
	return w.subject, []byte("immutable-request"), w.encodeErr
}

func (w *transferWireFixture) Decode(xfercontract.Operation, []byte) (xfercontract.Response, error) {
	return w.response, w.decodeErr
}
