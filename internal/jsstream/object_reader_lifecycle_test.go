package jsstream

// The server, object metadata, and consumer creation are real. These wrappers
// pause after a successful API reply to make constructor cancellation repeatable.
// They do not model WebSocket framing or a lost API reply.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type pausedTransferObjectStore struct {
	jetstream.ObjectStore
	replied chan struct{}
}

// Management APIs use a separate real connection, so a withheld data-socket
// PONG cannot incidentally prevent Open from reaching its inherited barrier.
// No API results or object bytes are fabricated by this adapter.
type transferReaderManagementJS struct {
	jetstream.JetStream
	data *nats.Conn
}

func (j transferReaderManagementJS) Conn() *nats.Conn { return j.data }

// origin: hot-update S7 plan §3 / T07 / M05
func TestTransferDirtyWindowSurvivesLastReaderClose(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransferReaderDirtyWindow(t)
	}
	testharness.AssertNoGoroutineLeak(t, "transfer dirty window", goroutines)
	testharness.AssertNoFDLeak(t, "transfer dirty window", fds, 0)
}

func exerciseTransferReaderDirtyWindow(t *testing.T) {
	t.Helper()
	t.Run("last_close_then_open", func(t *testing.T) {
		url := startJSNATS(t)
		management := newJS(t, url)
		ctx, store, stream := transferObjectFixture(t, management, "dirty-survivor")
		body := bytes.Repeat([]byte("retained dirty guard"), 14000)
		spec := transferReaderSpec(ctx, t, management, store, stream, body)
		dialer := &transferReadPauseDialer{}
		nc, err := nats.Connect(url, nats.SetCustomDialer(dialer), nats.NoReconnect())
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		js := transferReaderManagementJS{JetStream: management, data: nc}
		reader, err := NewTransferReader(js, spec, xfercontract.SinkCheckpoint{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = reader.Stop(transferReaderDeadline(t, 3*time.Second)) }()
		if _, err := reader.ReadChunk(transferReaderDeadline(t, 3*time.Second)); err != nil {
			t.Fatal(err)
		}
		window := reader.(*transferResumeReader).window
		blocked := dialer.conn.pauseReads()
		defer dialer.conn.resumeReads()
		stopped := make(chan error, 1)
		go func() { stopped <- reader.Stop(transferReaderDeadline(t, 300*time.Millisecond)) }()
		select {
		case <-blocked:
		case <-ctx.Done():
			t.Fatal("fixture did not withhold the actual PONG")
		}
		select {
		case err := <-stopped:
			if err == nil {
				t.Fatal("failed PONG was reported as successful cleanup")
			}
		case <-ctx.Done():
			t.Fatal("bounded Stop did not return")
		}
		objectWindows.Lock()
		retained := objectWindows.byConn[nc] == window && window.refs == 0 && window.needsBarrier && window.cleanupRunning
		joined := window.cleanupDone
		objectWindows.Unlock()
		if !retained {
			t.Fatal("last reader Close erased its dirty guard or cleanup owner")
		}
		requests, err := management.Conn().SubscribeSync("$JS.API.CONSUMER.MSG.NEXT.OBJ_dirty-survivor.>")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = requests.Unsubscribe() }()
		if err := management.Conn().FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		for len(dialer.conn.sentPings) > 0 {
			<-dialer.conn.sentPings
		}
		successor, err := NewTransferReader(js, spec, xfercontract.SinkCheckpoint{})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = successor.Stop(transferReaderDeadline(t, 3*time.Second)) }()
		type readResult struct {
			chunk xfercontract.Chunk
			err   error
		}
		read := make(chan readResult, 1)
		go func() {
			chunk, err := successor.ReadChunk(transferReaderDeadline(t, 5*time.Second))
			read <- readResult{chunk: chunk, err: err}
		}()
		select {
		case <-dialer.conn.sentPings:
		case result := <-read:
			t.Fatalf("successor returned before the inherited PONG: %v", result.err)
		case <-ctx.Done():
			t.Fatal("no actual successor/cleanup PING was sent")
		}
		if err := management.Conn().FlushTimeout(time.Second); err != nil {
			t.Fatal(err)
		}
		if pending, _, err := requests.Pending(); err != nil || pending != 0 {
			t.Fatalf("new reader sent a pull before the dirty barrier: pending=%d err=%v", pending, err)
		}
		dialer.conn.resumeReads()
		var first xfercontract.Chunk
		select {
		case result := <-read:
			if result.err != nil {
				t.Fatal(result.err)
			}
			first = result.chunk
		case <-ctx.Done():
			t.Fatal("successor did not resume after the real PONG")
		}
		if !bytes.Equal(first.Data, body[:spec.ChunkSize]) {
			t.Fatal("successor used an old subscription's bytes")
		}
		sink, err := os.CreateTemp(t.TempDir(), "sink")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = sink.Close() }()
		h := sha256.New()
		_ = syncTransferReaderChunk(t, successor, sink, h, first)
		for {
			chunk, err := successor.ReadChunk(transferReaderDeadline(t, 3*time.Second))
			if errors.Is(err, io.EOF) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			_ = syncTransferReaderChunk(t, successor, sink, h, chunk)
		}
		if err := successor.Stop(transferReaderDeadline(t, 3*time.Second)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-joined:
		case <-ctx.Done():
			t.Fatal("dirty cleanup owner did not join after proven drain")
		}
		requireNoTransferObjectConsumers(ctx, t, stream)
		objectWindows.Lock()
		_, exists := objectWindows.byConn[nc]
		objectWindows.Unlock()
		if exists {
			t.Fatal("resolved dirty guard retained an owner after the last Stop")
		}
	})
}

func (s pausedTransferObjectStore) GetInfo(ctx context.Context, name string, opts ...jetstream.GetObjectInfoOpt) (*jetstream.ObjectInfo, error) {
	info, err := s.ObjectStore.GetInfo(ctx, name, opts...)
	if err != nil {
		return nil, err
	}
	close(s.replied)
	<-ctx.Done()
	return info, ctx.Err()
}

type canceledTransferConsumerCreator struct {
	jetstream.JetStream
	cancel context.CancelFunc
}

func (j canceledTransferConsumerCreator) CreateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	consumer, err := j.JetStream.CreateConsumer(ctx, stream, cfg)
	if err == nil {
		j.cancel()
	}
	return consumer, err
}

// origin: control-connection-recovery second internal review, constructor lifecycle.
func TestTransferObjectConstructorCancellationReleasesResources(t *testing.T) {
	for _, point := range []string{"metadata_reply", "consumer_reply"} {
		t.Run(point, func(t *testing.T) {
			js := newJS(t, startJSNATS(t))
			ctx, store, stream := transferObjectFixture(t, js, "constructor-cancel")
			body := bytes.Repeat([]byte("constructor payload"), 40000)
			putTransferObject(ctx, t, store, body)
			warm, err := GetTransferObject(ctx, js, store, "constructor-cancel", "payload")
			if err != nil {
				t.Fatal(err)
			}
			if err := warm.Close(); err != nil {
				t.Fatal(err)
			}
			goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
			subscriptions := js.Conn().NumSubscriptions()
			for round := 0; round < 6; round++ {
				// The first reader owns a real outstanding batch. A constructor
				// which is canceled while that reader closes must release only
				// its own ownership and leave no connection key or consumer.
				first, err := GetTransferObject(ctx, js, store, "constructor-cancel", "payload")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := io.ReadFull(first, make([]byte, 1)); err != nil {
					_ = first.Close()
					t.Fatal(err)
				}
				constructorCtx, cancel := context.WithCancel(ctx)
				replied := make(chan struct{})
				constructorJS := js
				constructorStore := store
				if point == "metadata_reply" {
					constructorStore = pausedTransferObjectStore{ObjectStore: store, replied: replied}
				} else {
					constructorJS = canceledTransferConsumerCreator{JetStream: js, cancel: cancel}
					close(replied)
				}
				done := make(chan error, 1)
				go func() {
					r, err := GetTransferObject(constructorCtx, constructorJS, constructorStore, "constructor-cancel", "payload")
					if r != nil {
						// Legacy Bind may succeed after its metadata lookup was
						// canceled. The public safety contract is that this reader
						// cannot yield any bytes and Close releases the consumer.
						n, readErr := r.Read(make([]byte, 1))
						closeErr := r.Close()
						if n != 0 || !errors.Is(readErr, context.Canceled) {
							err = errors.New("canceled constructor yielded object data")
						} else if closeErr != nil {
							err = closeErr
						} else {
							err = readErr
						}
					}
					done <- err
				}()
				select {
				case <-replied:
				case <-ctx.Done():
					cancel()
					_ = first.Close()
					t.Fatal("constructor did not reach its API reply")
				}
				if err := first.Close(); err != nil {
					cancel()
					t.Fatal(err)
				}
				if point == "metadata_reply" {
					cancel()
				}
				select {
				case err := <-done:
					if !errors.Is(err, context.Canceled) {
						t.Errorf("canceled constructor: got %v, want context.Canceled", err)
					}
				case <-ctx.Done():
					cancel()
					t.Fatal("canceled constructor did not return")
				}
				cancel()
				requireNoTransferObjectConsumers(ctx, t, stream)
				objectWindows.Lock()
				_, retained := objectWindows.byConn[js.Conn()]
				objectWindows.Unlock()
				if retained {
					t.Fatal("failed constructor retained its connection window")
				}
				// Exercise the public reader again: cleanup must not poison the
				// connection or allow an old consumer to steal new object data.
				next, err := GetTransferObject(ctx, js, store, "constructor-cancel", "payload")
				if err != nil {
					t.Fatal(err)
				}
				got, readErr := io.ReadAll(next)
				closeErr := next.Close()
				if readErr != nil || closeErr != nil || !bytes.Equal(got, body) {
					t.Fatalf("successor transfer: bytes=%d read=%v close=%v", len(got), readErr, closeErr)
				}
			}
			requireNoTransferObjectConsumers(ctx, t, stream)
			if !testharness.WaitFor(t, 3*time.Second, 10*time.Millisecond, func() bool {
				return js.Conn().NumSubscriptions() <= subscriptions
			}) {
				t.Errorf("constructor leaked subscriptions: before=%d after=%d", subscriptions, js.Conn().NumSubscriptions())
			}
			testharness.AssertNoGoroutineLeak(t, "transfer constructor", goroutines)
			testharness.AssertNoFDLeak(t, "transfer constructor", fds, 0)
		})
	}
}

type pausedTransferConsumerBinder struct {
	jetstream.JetStream
	conn *transferReadPauseConn
}

func (j pausedTransferConsumerBinder) CreateConsumer(ctx context.Context, stream string, cfg jetstream.ConsumerConfig) (jetstream.Consumer, error) {
	consumer, err := j.JetStream.CreateConsumer(ctx, stream, cfg)
	if err == nil {
		j.conn.pauseReads()
	}
	return consumer, err
}

// origin: control-connection-recovery second internal review, bind context propagation.
func TestTransferObjectCancellationInterruptsConsumerBinding(t *testing.T) {
	url := startJSNATS(t)
	observerJS := newJS(t, url)
	ctx, store, stream := transferObjectFixture(t, observerJS, "bind-cancel")
	putTransferObject(ctx, t, store, []byte("payload"))
	dialer := &transferReadPauseDialer{}
	nc, err := nats.Connect(url, nats.SetCustomDialer(dialer), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	receiverStore, err := js.ObjectStore(ctx, "bind-cancel")
	if err != nil {
		t.Fatal(err)
	}
	infoRequests, err := observerJS.Conn().SubscribeSync("$JS.API.CONSUMER.INFO.OBJ_bind-cancel.>")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = infoRequests.Unsubscribe() }()
	deleteRequests, err := observerJS.Conn().SubscribeSync("$JS.API.CONSUMER.DELETE.OBJ_bind-cancel.>")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = deleteRequests.Unsubscribe() }()
	if err := observerJS.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	readerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	defer dialer.conn.resumeReads()
	type constructorResult struct {
		reader io.ReadCloser
		err    error
	}
	constructed := make(chan constructorResult, 1)
	go func() {
		reader, err := GetTransferObject(readerCtx, pausedTransferConsumerBinder{
			JetStream: js, conn: dialer.conn,
		}, receiverStore, "bind-cancel", "payload")
		constructed <- constructorResult{reader: reader, err: err}
	}()
	watchCtx, stopWatch := context.WithCancel(ctx)
	defer stopWatch()
	infoSent := make(chan error, 1)
	go func() {
		_, err := infoRequests.NextMsgWithContext(watchCtx)
		infoSent <- err
	}()
	var result constructorResult
	var returned bool
	select {
	case result = <-constructed:
		// A binding which uses the freshly created consumer directly can
		// return without another API lookup. It must still cancel Read/Close.
		returned = true
	case err := <-infoSent:
		if err != nil {
			t.Fatalf("observe actual bind INFO request: %v", err)
		}
		// Cancel only AFTER this actual request. A ctx.Err check before Bind
		// cannot make the in-flight-lookup regression pass accidentally.
	case <-ctx.Done():
		cancel()
		dialer.conn.resumeReads()
		t.Fatal("constructor neither returned nor dispatched a bind INFO request")
	}
	cancel()
	stopWatch()
	done := make(chan error, 1)
	go func() {
		if !returned {
			result = <-constructed
		}
		if result.reader != nil {
			n, readErr := result.reader.Read(make([]byte, 1))
			closeErr := result.reader.Close()
			if n != 0 {
				result.err = errors.New("canceled bind yielded object data")
			} else if closeErr != nil {
				result.err = closeErr
			} else {
				result.err = readErr
			}
		}
		done <- result.err
	}()
	// The socket now withholds any bind-time lookup response. Cancellation
	// must begin actual consumer cleanup, rather than wait for the legacy
	// JetStream context's unrelated default request timeout. Implementations
	// which bind their freshly created consumer without another lookup also
	// satisfy this public behavior.
	if _, err := deleteRequests.NextMsg(time.Second); err != nil {
		t.Errorf("cancellation did not reach consumer cleanup while bind INFO was blocked: %v", err)
	}
	dialer.conn.resumeReads()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("canceled bind: got %v, want context.Canceled", err)
		}
	case <-ctx.Done():
		t.Fatal("constructor did not terminate after response delivery resumed")
	}
	requireNoTransferObjectConsumers(ctx, t, stream)
}
