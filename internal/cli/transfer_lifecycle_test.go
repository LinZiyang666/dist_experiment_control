package cli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §2.3 / §7 / T32
func TestTransferDataConnectionRejectsReconnectBuffer(t *testing.T) {
	s, control, id, stop := transferConnectionFixture(t)
	defer stop()
	disconnected, reconnected := make(chan struct{}, 1), make(chan struct{}, 1)
	nc, err := ConnectTransferDataWithin(s.ClientURL(), id, retryDeadline(t, time.Second), nil,
		nats.ReconnectBufSize(0), // caller cannot override the final safety option
		nats.FlusherTimeout(time.Minute),
		nats.ReconnectWait(10*time.Millisecond),
		nats.DisconnectErrHandler(func(*nats.Conn, error) { disconnected <- struct{}{} }),
		nats.ReconnectHandler(func(*nats.Conn) { reconnected <- struct{}{} }))
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	if nc == control || nc.Opts.ReconnectBufSize != -1 || nc.Opts.FlusherTimeout != upgradebudget.RequestLatency {
		t.Fatalf("data connection reused control or expanded write bounds: buffer=%d flusher=%v", nc.Opts.ReconnectBufSize, nc.Opts.FlusherTimeout)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "XFER_TRANSPORT", Subjects: []string{"transport-data.*"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = js.Publish(ctx, "transport-data.before", []byte("before")); err != nil {
		t.Fatal(err)
	}
	port := s.Addr().(*net.TCPAddr).Port
	storeDir := s.JetStreamConfig().StoreDir
	s.Shutdown()
	s.WaitForShutdown()
	select {
	case <-disconnected:
	case <-time.After(time.Second):
		t.Fatal("data attachment did not observe the real server loss")
	}
	before := nc.Stats().OutMsgs
	if err = nc.Publish("transport-data.late", []byte("retired-attempt")); !errors.Is(err, nats.ErrReconnectBufExceeded) {
		t.Fatalf("offline publish accepted for later replay: %v", err)
	}
	if pending, err := nc.Buffered(); err != nil || pending != 0 || nc.Stats().OutMsgs != before {
		t.Fatalf("reconnect retained publish bytes: buffered=%d sent=%d err=%v", pending, nc.Stats().OutMsgs-before, err)
	}
	restarted, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: port, NoLog: true, NoSigs: true,
		AlwaysEnableNonce: true, JetStream: true, StoreDir: storeDir})
	if err != nil {
		t.Fatal(err)
	}
	restarted.Start()
	defer func() { restarted.Shutdown(); restarted.WaitForShutdown() }()
	if !restarted.ReadyForConnections(time.Second) {
		t.Fatal("same-store NATS restart failed")
	}
	select {
	case <-reconnected:
	case <-time.After(3 * time.Second):
		t.Fatal("original data attachment did not reconnect")
	}
	ctxAfter, cancelAfter := context.WithTimeout(t.Context(), time.Second)
	defer cancelAfter()
	if _, err = js.Publish(ctxAfter, "transport-data.after", []byte("after")); err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(ctxAfter)
	if err != nil || info.State.Msgs != 2 {
		t.Fatalf("stale reconnect publish reached the physical stream: info=%+v err=%v", info, err)
	}
	if _, err = stream.GetLastMsgForSubject(ctxAfter, "transport-data.late"); !errors.Is(err, jetstream.ErrMsgNotFound) {
		t.Fatalf("retired bytes resurrected after reconnect: %v", err)
	}
}

// origin: hot-update S7 plan §7 / §11.3 / T32
func TestTransferLifecycleNoLeaks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferLifecycle(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer transport lifecycle", beforeG)
	testharness.AssertNoFDLeak(t, "transfer transport lifecycle", beforeFD, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	t.Helper()
	s, control, id, stop := transferConnectionFixture(t)
	defer stop()
	sub, err := control.Subscribe("transport-lifecycle-reply", func(m *nats.Msg) { _ = m.Respond([]byte("validated")) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	silent, err := control.Subscribe("transport-lifecycle-silent", func(*nats.Msg) {})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = silent.Unsubscribe() }()
	if err = control.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	wire := &transferWireFixture{subject: "transport-lifecycle-reply"}
	transport, err := NewTransferTransport(control, s.ClientURL(), id, wire, nil, nats.ReconnectBufSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	identity := transferAttachmentIdentity()
	defer func() { _ = transport.CloseData(identity, retryDeadline(t, time.Second)) }()
	var wg sync.WaitGroup
	opened := make(chan jetstream.JetStream, 5)
	for range 5 {
		wg.Go(func() {
			js, err := transport.OpenData(identity, retryDeadline(t, time.Second))
			if err != nil {
				t.Error(err)
				return
			}
			opened <- js
		})
	}
	wg.Wait()
	close(opened)
	var original jetstream.JetStream
	count := 0
	for js := range opened {
		count++
		if original == nil {
			original = js
		} else if js != original {
			t.Fatal("concurrent open installed multiple data attachments")
		}
	}
	if count != 5 || transport.data == control || transport.data.Opts.ReconnectBufSize != -1 ||
		transport.data.Opts.Name != CtlNameForSession(identity.SID) {
		t.Fatalf("data attachment did not isolate transport and identity: count=%d", count)
	}
	if _, attempt, err := transport.Request(xfercontract.Request{CallID: "lifecycle-call", Op: xfercontract.OpStatus}, retryDeadline(t, time.Second)); err != nil || !attempt.Sent {
		t.Fatalf("control request: %+v %v", attempt, err)
	}
	wire.subject = "transport-lifecycle-silent"
	if _, attempt, err := transport.Request(xfercontract.Request{CallID: "lifecycle-lost", Op: xfercontract.OpCommit}, retryDeadline(t, 40*time.Millisecond)); !attempt.Sent || !errors.Is(err, ErrTransferReconcileRequired) {
		t.Fatalf("lost reply erased original owner: %+v %v", attempt, err)
	}
	if js, err := transport.OpenData(identity, retryDeadline(t, time.Second)); err != nil || js != original {
		t.Fatalf("control timeout replaced data attachment: %v", err)
	}
	observed := make(chan struct{})
	cancelSub, err := control.Subscribe("transport-lifecycle-cancel", func(*nats.Msg) { close(observed) })
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cancelSub.Unsubscribe() }()
	if err = control.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	sentCtx, cancelSent := context.WithCancel(t.Context())
	defer cancelSent()
	sentResult := make(chan error, 1)
	go func() {
		_, attempt, err := RequestTransferWithin(sentCtx, control, "transport-lifecycle-cancel", nil,
			RetryConfig{Deadline: retryDeadline(t, time.Second), LogicalID: "lifecycle-sent-cancel"})
		if !attempt.Sent {
			err = errors.Join(err, errors.New("cancel lost the sent boundary"))
		}
		sentResult <- err
	}()
	select {
	case <-observed:
	case <-time.After(time.Second):
		cancelSent()
		<-sentResult
		t.Fatal("cancellation fixture did not receive a real request")
	}
	cancelSent()
	if err = <-sentResult; !errors.Is(err, context.Canceled) || !errors.Is(err, ErrTransferReconcileRequired) {
		t.Fatalf("sent cancellation claimed a definite result: %v", err)
	}
	if js, err := transport.OpenData(identity, retryDeadline(t, time.Second)); err != nil || js != original {
		t.Fatalf("cancellation replaced the original data attachment: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := control.Stats().OutMsgs
	if _, attempt, err := RequestTransferWithin(ctx, control, wire.subject, nil,
		RetryConfig{Deadline: retryDeadline(t, time.Second), LogicalID: "lifecycle-canceled"}); !errors.Is(err, context.Canceled) || attempt.Sent || control.Stats().OutMsgs != before {
		t.Fatalf("canceled control operation published: %+v %v", attempt, err)
	}
	foreign := identity
	foreign.EntryNonce = "replacement-entry"
	if err := transport.CloseData(foreign, retryDeadline(t, time.Second)); err == nil || transport.data.IsClosed() {
		t.Fatal("foreign close retired original data owner")
	}
	if _, err := transport.OpenData(foreign, retryDeadline(t, time.Second)); err == nil {
		t.Fatal("foreign identity reused original data owner")
	}
	// Control can disappear completely while the original data handle still
	// publishes under its own connection. This is the transport replacement
	// boundary; it is not a receiver/worker image continuity claim.
	control.Close()
	dataCtx, cancelData := context.WithTimeout(t.Context(), time.Second)
	defer cancelData()
	stream, err := original.CreateStream(dataCtx, jetstream.StreamConfig{Name: "LIFECYCLE", Subjects: []string{"lifecycle-data"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = original.Publish(dataCtx, "lifecycle-data", []byte("original-data-owner")); err != nil {
		t.Fatalf("control loss closed data attachment: %v", err)
	}
	if info, err := stream.Info(dataCtx); err != nil || info.State.Msgs != 1 {
		t.Fatalf("data owner lost progress: %+v %v", info, err)
	}
	if err = transport.CloseData(identity, retryDeadline(t, time.Second)); err != nil || !transport.data.IsClosed() {
		t.Fatalf("data close failed: %v", err)
	}
	if _, err = transport.OpenData(identity, retryDeadline(t, time.Second)); err == nil {
		t.Fatal("retired attachment reopened")
	}
	if err = transport.CloseData(identity, retryDeadline(t, time.Second)); err != nil {
		t.Fatalf("duplicate close: %v", err)
	}
	exerciseTransferOpeningRetirement(t, s.ClientURL(), id)
}

func exerciseTransferOpeningRetirement(t *testing.T, url string, id *Identity) {
	t.Helper()
	control, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer control.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	observer := &transferOpeningObserver{entered: entered, release: release}
	transport, err := NewTransferTransport(control, url, id, &transferWireFixture{}, observer)
	if err != nil {
		t.Fatal(err)
	}
	identity := transferAttachmentIdentity()
	result := make(chan error, 1)
	go func() {
		_, err := transport.OpenData(identity, retryDeadline(t, time.Second))
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		close(release)
		<-result
		t.Fatal("opener did not enter bounded admission")
	}
	if err := transport.CloseData(identity, retryDeadline(t, 25*time.Millisecond)); !errors.Is(err, controlbudget.ErrExpired) {
		close(release)
		<-result
		t.Fatalf("close did not retain in-flight opener responsibility: %v", err)
	}
	close(release)
	if err = <-result; err == nil {
		t.Fatal("late opener installed after retirement")
	}
	transport.mu.Lock()
	installed, opening := transport.data != nil || transport.js != nil, transport.opening != nil
	transport.mu.Unlock()
	if installed || opening {
		t.Fatal("partial open left an attachment or join responsibility")
	}
	if err = transport.CloseData(identity, retryDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = control.FlushTimeout(time.Second); err != nil {
		t.Fatalf("data retirement closed borrowed control connection: %v", err)
	}
}

type transferOpeningObserver struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
	count   uint32
}

func (o *transferOpeningObserver) Begin(id string, phase controlbudget.AttemptPhase) (controlbudget.Attempt, error) {
	if phase == controlbudget.PhaseConnect {
		o.once.Do(func() { close(o.entered); <-o.release })
	}
	o.count++
	now, err := controlbudget.Now()
	return controlbudget.Attempt{LogicalID: id, Ordinal: o.count, Phase: phase, StartedMonoNS: now}, err
}

func (*transferOpeningObserver) Record(controlbudget.Attempt) error { return nil }
func (*transferOpeningObserver) Close() error                       { return nil }

func transferAttachmentIdentity() xfercontract.Identity {
	return xfercontract.Identity{SID: "transfer-session", NID: "transfer-node", ActorFP: "transfer-actor",
		AgentInstance: "agent-instance", TransferID: "transfer-id", EntryNonce: "entry-nonce",
		IntentDigest: "intent-digest", ObjectVersion: "original-version"}
}

func transferConnectionFixture(t *testing.T) (*server.Server, *nats.Conn, *Identity, func()) {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, NoLog: true, NoSigs: true,
		AlwaysEnableNonce: true, JetStream: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	s.Start()
	stopServer := func() { s.Shutdown(); s.WaitForShutdown() }
	if !s.ReadyForConnections(time.Second) {
		stopServer()
		t.Fatal("transfer fixture server not ready")
	}
	control, err := nats.Connect(s.ClientURL())
	if err != nil {
		stopServer()
		t.Fatal(err)
	}
	id, err := EnsureIdentity(t.TempDir())
	if err != nil {
		control.Close()
		stopServer()
		t.Fatal(err)
	}
	var once sync.Once
	stop := func() { once.Do(func() { control.Close(); stopServer() }) }
	t.Cleanup(stop)
	return s, control, id, stop
}

// origin: hot-update S7 plan §7 / T32
func TestTransferDataAdmissionRejectsPartialConstruction(t *testing.T) {
	s, control, id, stop := transferConnectionFixture(t)
	defer stop()
	identity := transferAttachmentIdentity()
	for _, invalid := range []struct {
		control *nats.Conn
		url     string
		id      *Identity
		wire    TransferWire
	}{
		{nil, s.ClientURL(), id, &transferWireFixture{}},
		{control, "", id, &transferWireFixture{}},
		{control, s.ClientURL(), nil, &transferWireFixture{}},
		{control, s.ClientURL(), id, nil},
	} {
		if transport, err := NewTransferTransport(invalid.control, invalid.url, invalid.id, invalid.wire, nil); transport != nil || err == nil {
			t.Fatalf("invalid transport inputs claimed ready: %v %v", transport, err)
		}
	}
	for index, option := range []nats.Option{
		func(*nats.Options) error { return errors.New("invalid option") },
	} {
		t.Run(fmt.Sprint(index), func(t *testing.T) {
			transport, err := NewTransferTransport(control, s.ClientURL(), id, &transferWireFixture{}, nil, option)
			if err != nil {
				t.Fatal(err)
			}
			if js, err := transport.OpenData(identity, retryDeadline(t, time.Second)); js != nil || err == nil {
				t.Fatalf("partial construction claimed ready: %v %v", js, err)
			}
			if transport.opening != nil || transport.data != nil {
				t.Fatal("failed opener left resources")
			}
			if err = transport.CloseData(identity, retryDeadline(t, time.Second)); err != nil {
				t.Fatal(err)
			}
		})
	}
	transport, err := NewTransferTransport(control, s.ClientURL(), id, &transferWireFixture{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, invalid := range []xfercontract.Identity{{}, {SID: identity.SID}, {TransferID: identity.TransferID}} {
		if js, err := transport.OpenData(invalid, retryDeadline(t, time.Second)); js != nil || err == nil || transport.bound {
			t.Fatalf("invalid data identity reached admission: js=%v bound=%t err=%v", js, transport.bound, err)
		}
	}
	if _, err = transport.OpenData(identity, 0); !errors.Is(err, controlbudget.ErrInvalidDeadline) {
		t.Fatalf("invalid deadline admitted data dial: %v", err)
	}
	if err = transport.CloseData(identity, retryDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err = transport.OpenData(identity, retryDeadline(t, time.Second)); err == nil {
		t.Fatal("close-before-open did not fence future admission")
	}
}
