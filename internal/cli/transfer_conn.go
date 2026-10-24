package cli

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/xferclient"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TransferWire keeps the P01 wire manifest out of the retry owner. Encode must
// use the canonical subject builders and validate the request union; Decode
// must validate the complete response before it can discharge a sent attempt.
// Neither method sends, creates an ID, or makes a compatibility decision.
// origin: hot-update S7 plan §6.2 / §7 / §8.1
type TransferWire interface {
	Encode(xfercontract.Request) (subject string, body []byte, err error)
	Decode(xfercontract.Operation, []byte) (xfercontract.Response, error)
}

// TransferTransport borrows a control connection and owns one invocation's
// data attachment. Replacing control transport cannot close an active writer
// or reader. CloseData permanently fences new attachments for this invocation;
// NATS reconnects on the original attachment while its owner remains live.
// origin: hot-update S7 plan §2.3 / §7 / §8.1
type TransferTransport struct {
	settlement transferSettlement
	control    *nats.Conn
	url        string
	id         *Identity
	wire       TransferWire
	observer   controlbudget.Observer
	opts       []nats.Option
	mu         sync.Mutex
	identity   xfercontract.Identity
	bound      bool
	opening    chan struct{}
	closing    bool
	data       *nats.Conn
	js         jetstream.JetStream
	cleanup    *jsstream.ConsumerCleanupOwner
}

var _ xfercontract.Transport = (*TransferTransport)(nil)

// NewTransferTransport allocates no connection. The command supplies its P01
// wire adapter and authenticated control connection; the first OpenData opens
// a separate connection under the caller's original deadline.
func NewTransferTransport(control *nats.Conn, url string, id *Identity, wire TransferWire,
	observer controlbudget.Observer, opts ...nats.Option) (*TransferTransport, error) {
	if control == nil || url == "" || id == nil || wire == nil {
		return nil, errors.New("cli: invalid transfer transport")
	}
	identity := *id
	identity.Seed = append([]byte(nil), id.Seed...)
	return &TransferTransport{control: control, url: url, id: &identity, wire: wire,
		observer: observer, opts: append([]nats.Option(nil), opts...)}, nil
}

func (t *TransferTransport) Request(request xfercontract.Request, deadline controlbudget.Deadline) (xfercontract.Response, controlbudget.Attempt, error) {
	if !validLogicalID(request.CallID) {
		return xfercontract.Response{}, controlbudget.Attempt{}, errors.New("cli: invalid transfer call identity")
	}
	bounded, err := transferControlDeadline(deadline)
	if err != nil {
		return xfercontract.Response{}, controlbudget.Attempt{}, err
	}
	subject, body, err := t.wire.Encode(request)
	if err != nil {
		return xfercontract.Response{}, controlbudget.Attempt{}, err
	}
	nc, err := t.requestConnection(request, bounded)
	if err != nil {
		return xfercontract.Response{}, controlbudget.Attempt{}, err
	}
	// Only this synchronous control attempt owns the derived child context.
	// ctx-none: frozen Transport.Request supplies an absolute deadline, no caller ctx.
	reply, attempt, err := RequestTransferWithin(context.Background(), nc, subject, body,
		RetryConfig{Deadline: bounded, LogicalID: request.CallID, Observer: t.observer})
	if err != nil {
		return xfercontract.Response{}, attempt, err
	}
	response, err := t.wire.Decode(request.Op, reply.Data)
	if err == nil && response.Code == "" && response.Error == "" && response.Status != nil {
		err = xferclient.ValidateStatus(*response.Status, request.Identity, request.Origin)
	}
	if err == nil {
		_, err = controlbudget.Remaining(bounded)
	}
	if err != nil {
		// Receiving bytes is insufficient: malformed or incompatible replies do
		// not prove a mutation absent and must not enter a transport retry loop.
		// origin: hot-update S7 plan §7 / T32
		return xfercontract.Response{}, attempt, errors.Join(ErrTransferReconcileRequired, err)
	}
	return response, attempt, nil
}

// RequestTransferWithin reports the last actual request attempt even without
// an observer. A sent failure always returns to the engine; only a proven
// pre-publish failure may retry within this same five-second operation budget.
// origin: hot-update S7 plan §7 / T32
func RequestTransferWithin(ctx context.Context, nc *nats.Conn, subject string, body []byte, cfg RetryConfig) (*nats.Msg, controlbudget.Attempt, error) {
	if nc == nil {
		return nil, controlbudget.Attempt{}, errors.New("cli: nil transfer control connection")
	}
	deadline, err := transferControlDeadline(cfg.Deadline)
	if err != nil {
		return nil, controlbudget.Attempt{}, err
	}
	requestCtx, cancel, err := AdmissionContext(ctx, deadline)
	if err != nil {
		return nil, controlbudget.Attempt{}, err
	}
	defer cancel()
	recorder := &transferAttemptRecorder{observer: cfg.Observer}
	cfg.Deadline, cfg.Family, cfg.Observer = deadline, RetryTransfer, recorder
	payload := append([]byte(nil), body...)
	var reply *nats.Msg
	err = DoPhaseWithin(requestCtx, cfg, controlbudget.PhaseRequest, func(time.Duration) error {
		var sent bool
		var attemptErr error
		reply, sent, attemptErr = transferRequestAttempt(requestCtx, nc, subject, payload)
		if attemptErr != nil {
			return &AttemptError{Err: attemptErr, Sent: sent}
		}
		return nil
	})
	if err != nil {
		reply = nil
	}
	return reply, recorder.request, err
}

// Capabilities asks the broker's existing caps verb on the authenticated
// control connection. An old broker answers it completely but without a
// transfer version: that reply, not a probe timeout, is explicit cap0.
// origin: hot-update S7 plan §9 / INTEG-D capability compatibility
func (t *TransferTransport) Capabilities(sid string, deadline controlbudget.Deadline) (proto.CapsResp, error) {
	bounded, err := transferControlDeadline(deadline)
	if err != nil {
		return proto.CapsResp{}, err
	}
	remaining, err := controlbudget.Remaining(bounded)
	if err != nil {
		return proto.CapsResp{}, err
	}
	// ctx-none: frozen Transport callers supply an absolute deadline, no caller ctx.
	return ProbeTransferCaps(context.Background(), t.control, t.id.PublicKey, sid, remaining)
}

// ReceiverInstance names a pull's local receiver: the broker binds a ctl's
// reservation to this authenticated subject actor, never to a body string.
// origin: hot-update S7 plan §4 / §6.3 / INTEG-D outer adapter
func (t *TransferTransport) ReceiverInstance() string { return t.id.PublicKey }

func transferControlDeadline(deadline controlbudget.Deadline) (controlbudget.Deadline, error) {
	remaining, err := controlbudget.Remaining(deadline)
	if err != nil {
		return 0, err
	}
	if remaining <= upgradebudget.RequestLatency {
		return deadline, nil
	}
	// A byte deadline can be 2108s. It cannot become the control-attempt
	// deadline, and retries below never renew this locally capped deadline.
	// origin: hot-update S7 plan §1.2 / T32
	return controlbudget.After(upgradebudget.RequestLatency)
}

func transferRequestAttempt(ctx context.Context, nc *nats.Conn, subject string, body []byte) (reply *nats.Msg, sent bool, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		return nil, false, err
	}
	defer func() { resultErr = errors.Join(resultErr, sub.Unsubscribe()) }()
	if err = sub.SetPendingLimits(4, 32<<20); err != nil {
		return nil, false, err
	}
	// Flush proves the reply subscription was admitted before mutation. In
	// particular, an asynchronous SUB permission error must not be treated as
	// an unsent publish followed by an ordinary retryable missing response.
	if err = nc.FlushWithContext(ctx); err != nil {
		return nil, false, err
	}
	if last := nc.LastError(); last != nil && errors.Is(last, nats.ErrPermissionViolation) {
		return nil, false, nats.ErrPermissionViolation
	}
	if err = ctx.Err(); err != nil {
		return nil, false, err
	}
	err = nc.PublishRequest(subject, inbox, body)
	if err != nil {
		return nil, transferPublishMayHaveEscaped(err), err
	}
	sent = true
	reply, err = sub.NextMsgWithContext(ctx)
	if err != nil {
		return nil, sent, err
	}
	if len(reply.Data) == 0 && reply.Header.Get("Status") == "503" {
		return nil, sent, nats.ErrNoResponders
	}
	return reply, sent, nil
}

func transferPublishMayHaveEscaped(err error) bool {
	// These errors are returned before nats.go appends any PUB bytes. Other
	// errors can include a partial socket write: false would authorize replay
	// without reconciling a mutation already accepted by the server.
	// origin: hot-update S7 plan §7 / T32
	for _, beforeWrite := range []error{nats.ErrInvalidConnection, nats.ErrConnectionClosed,
		nats.ErrConnectionDraining, nats.ErrBadSubject, nats.ErrMaxPayload,
		nats.ErrHeadersNotSupported, nats.ErrReconnectBufExceeded} {
		if errors.Is(err, beforeWrite) {
			return false
		}
	}
	return true
}

type transferAttemptRecorder struct {
	observer controlbudget.Observer
	ordinal  uint32
	request  controlbudget.Attempt
}

func (r *transferAttemptRecorder) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	if r.observer != nil {
		return r.observer.Begin(id, phase)
	}
	now, err := controlbudget.Now()
	r.ordinal++
	return controlbudget.Attempt{LogicalID: id, Ordinal: r.ordinal, Phase: phase, StartedMonoNS: now}, err
}

func (r *transferAttemptRecorder) Record(a controlbudget.Attempt) error {
	if a.Phase == controlbudget.PhaseRequest {
		r.request = a
	}
	if r.observer != nil {
		return r.observer.Record(a)
	}
	return nil
}

func (r *transferAttemptRecorder) Close() error { return nil } // observer is borrowed

// ConnectTransferDataWithin cannot accept a caller option that reenables
// offline buffering. Zero selects nats.go's default 8MiB buffer; -1 rejects
// publishes during reconnect instead of replaying a stale chunk after retire.
// This option does not replace the writer's immutable attempt/CAS fence.
// origin: hot-update S7 plan §2.3 / T04 / T32
func ConnectTransferDataWithin(url string, id *Identity, deadline controlbudget.Deadline,
	observer controlbudget.Observer, opts ...nats.Option) (*nats.Conn, error) {
	bounded, err := transferControlDeadline(deadline)
	if err != nil {
		return nil, err
	}
	options := append([]nats.Option(nil), opts...)
	// nats.go otherwise allows a socket write to hold its connection mutex
	// for a minute. A stalled bulk socket must release that mutex within the
	// control window so Stop/Close can join rather than freezing admission.
	// origin: hot-update S7 plan §1.2 / §2.3 / T32
	options = append(options, nats.ReconnectBufSize(-1), nats.FlusherTimeout(upgradebudget.RequestLatency))
	return ConnectWithin(url, id, bounded, observer, options...)
}

func (t *TransferTransport) OpenData(identity xfercontract.Identity, deadline controlbudget.Deadline) (jetstream.JetStream, error) {
	if identity.SID == "" || identity.TransferID == "" {
		return nil, errors.New("cli: invalid transfer data identity")
	}
	bounded, err := transferControlDeadline(deadline)
	if err != nil {
		return nil, err
	}
	deadline = bounded
	for {
		remaining, err := controlbudget.Remaining(deadline)
		if err != nil {
			return nil, err
		}
		t.mu.Lock()
		// A new nonce or intent is a different responsibility even when the
		// session and transfer ID match. It cannot borrow this data connection.
		// origin: hot-update S7 plan §6.1 / §7 / T32
		if t.bound && t.identity != identity {
			t.mu.Unlock()
			return nil, errors.New(xfercontract.CodeIntentConflict + ": data attachment identity changed")
		}
		// Retirement is permanent; reconnect belongs to the original NATS
		// handle and never mints a replacement after CloseData's gate closes.
		// origin: hot-update S7 plan §2.3 / §7 / T32
		if t.closing {
			t.mu.Unlock()
			return nil, errors.New("cli: transfer data attachment retired")
		}
		if t.js != nil {
			if !t.data.IsClosed() {
				js := t.js
				t.mu.Unlock()
				return js, nil
			}
			// Physical closure retires the socket barrier, not server consumers.
			// Keep custody before replacing the closed attachment.
			// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 item 1
			t.cleanup = jsstream.ConsumerCleanupFor(t.data)
			t.data, t.js = nil, nil
		}
		if opening := t.opening; opening != nil {
			t.mu.Unlock()
			if err = waitTransferAttachment(opening, remaining); err != nil {
				return nil, err
			}
			continue
		}
		t.bound, t.identity, t.opening = true, identity, make(chan struct{})
		t.mu.Unlock()
		err = reconcileTransferConsumers(t, deadline)
		opts := append([]nats.Option(nil), t.opts...)
		opts = append(opts, nats.Name(CtlNameForSession(identity.SID)))
		var nc *nats.Conn
		if err == nil {
			nc, err = ConnectTransferDataWithin(t.url, t.id, deadline,
				BindControlObserver(t.observer, identity.TransferID), opts...)
		}
		var js jetstream.JetStream
		if err == nil {
			js, err = jetstream.New(nc)
		}
		t.mu.Lock()
		// Close can race the synchronous dial. The opener remains responsible
		// for closing a successful but now inadmissible allocation before join.
		// origin: hot-update S7 plan §2.3 / §11.3 / T32
		if t.closing {
			err = errors.Join(err, errors.New("cli: transfer data attachment retired"))
		}
		if err == nil {
			t.data, t.js = nc, js
		}
		t.mu.Unlock()
		if err != nil && nc != nil {
			// Closing WSS can flush a close frame. Keep the join channel live,
			// but release the slot mutex so a concurrent CloseData can honor
			// its own shorter deadline while this opener finishes retirement.
			nc.Close()
		}
		t.mu.Lock()
		close(t.opening)
		t.opening = nil
		t.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return js, nil
	}
}

func (t *TransferTransport) CloseData(identity xfercontract.Identity, deadline controlbudget.Deadline) error {
	t.mu.Lock()
	// Closing another entry's socket would destroy its preserved checkpoint.
	// origin: hot-update S7 plan §6.1 / §7 / T32
	if t.bound && t.identity != identity {
		t.mu.Unlock()
		return errors.New(xfercontract.CodeIntentConflict + ": cannot retire another data attachment")
	}
	t.bound, t.identity, t.closing = true, identity, true
	opening, nc := t.opening, t.data
	if t.cleanup == nil && nc != nil {
		t.cleanup = jsstream.ConsumerCleanupFor(nc)
	}
	t.mu.Unlock()
	if nc != nil {
		// Do not Drain: it would publish buffered data after the owner closes
		// its gate. Writer.Stop and reader barriers belong to their leaf owners.
		nc.Close()
	}
	if opening != nil {
		bounded, err := transferControlDeadline(deadline)
		if err != nil {
			return err
		}
		remaining, err := controlbudget.Remaining(bounded)
		if err != nil {
			return err // opener still owns the connection and will close it
		}
		if err = waitTransferAttachment(opening, remaining); err != nil {
			return err
		}
	}
	return reconcileTransferConsumers(t, deadline)
}

// The authenticated control attachment shares the data account/domain. A failed
// Bind or Wait keeps the handle for a later CloseData/OpenData reconciliation;
// no network operation or join holds the transport mutex.
// origin: S7 internal review rv-jsnats F3 / s7-p9jsn2 item 1
func reconcileTransferConsumers(t *TransferTransport, deadline controlbudget.Deadline) error {
	t.mu.Lock()
	owner := t.cleanup
	t.mu.Unlock()
	if owner == nil {
		return nil
	}
	js, err := jetstream.New(t.control)
	if err != nil {
		return err
	}
	if err = owner.Bind(js, deadline); err != nil {
		return err
	}
	if err = owner.Wait(deadline); err != nil {
		return err
	}
	t.mu.Lock()
	if t.cleanup == owner {
		t.cleanup = nil
	}
	t.mu.Unlock()
	return nil
}

func waitTransferAttachment(done <-chan struct{}, remaining time.Duration) error {
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return controlbudget.ErrExpired
	}
}
