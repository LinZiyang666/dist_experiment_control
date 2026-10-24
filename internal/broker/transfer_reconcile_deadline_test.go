package broker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/testharness"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Copy into internal/broker. This observes the context handed to the actual
// SDK List port, without parking a watcher or leaving any goroutine behind.
type reviewListStore struct {
	jetstream.ObjectStore
	called, bounded bool
	ctx             context.Context
}

func (s *reviewListStore) List(ctx context.Context, _ ...jetstream.ListObjectsOpt) ([]*jetstream.ObjectInfo, error) {
	s.called = true
	s.ctx = ctx
	until, ok := ctx.Deadline()
	s.bounded = ok && time.Until(until) <= 5*time.Second
	return nil, errors.New("stop after observing actual List context")
}

type reviewListJS struct {
	jetstream.JetStream
	store *reviewListStore
}

func (j reviewListJS) ObjectStore(context.Context, string) (jetstream.ObjectStore, error) {
	return j.store, nil
}

// origin: S7 internal review rv-wiring F7
func TestTransferProductionReaperBoundsBothLists(t *testing.T) {
	for _, aged := range []bool{false, true} {
		name := "reapBucketObjects"
		if aged {
			name = "xferBucketHasAgedObjects"
		}
		t.Run(name, func(t *testing.T) {
			store := &reviewListStore{}
			b := &Broker{cfg: Config{Logger: testharness.SilentLog()}}
			setBrokerJS(b, reviewListJS{store: store})
			ctx, cancel := context.WithCancel(context.Background()) // ctx-root: test-owned reaper invocation.
			defer cancel()
			if aged {
				b.xferBucketHasAgedObjects(ctx, "OBJ_xfer-lab")
			} else {
				b.reapBucketObjects(ctx, "OBJ_xfer-lab", time.Minute, false, nil, nil)
			}
			if !store.called || !store.bounded {
				t.Fatalf("production SDK List called=%v bounded<=5s=%v", store.called, store.bounded)
			}
		})
	}
}

// reaperConsumerRelay forwards exact NATS frames. Only the consumer-CREATE
// request is delayed: the SDK has already read real metadata, and the server
// expires it before creating the watch. No response or metadata is fabricated.
type reaperConsumerRelay struct {
	listener net.Listener
	upstream string
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	conns    []net.Conn
	hold     func() error
	skip     int
	err      error
	wg       sync.WaitGroup
}

func newReaperConsumerRelay(parent context.Context, t *testing.T, upstream string) *reaperConsumerRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(parent)
	r := &reaperConsumerRelay{listener: listener, upstream: upstream, ctx: ctx, cancel: cancel}
	r.wg.Add(1)
	go r.serve()
	return r
}

func (r *reaperConsumerRelay) serve() {
	defer r.wg.Done()
	client, err := r.listener.Accept()
	if err != nil {
		return
	}
	// The copy goroutine and the relay's own close also close these; a second
	// close is expected to report net.ErrClosed.
	defer func() { _ = client.Close() }()
	serverConn, err := (&net.Dialer{}).DialContext(r.ctx, "tcp", r.upstream)
	if err != nil {
		return
	}
	defer func() { _ = serverConn.Close() }()
	r.mu.Lock()
	r.conns = []net.Conn{client, serverConn}
	if r.ctx.Err() != nil {
		r.mu.Unlock()
		return
	}
	r.mu.Unlock()
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		// Hide the TCP fast-copy interfaces: Linux splice caches process-wide
		// pipe FDs, which are unrelated to the resource ownership under test.
		_, _ = io.CopyBuffer(struct{ io.Writer }{client}, struct{ io.Reader }{serverConn}, make([]byte, 32<<10))
		_ = client.Close()
	}()
	reader := bufio.NewReader(client)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		fields := strings.Fields(line)
		var payload []byte
		if len(fields) >= 3 && (fields[0] == "PUB" || fields[0] == "HPUB") {
			size, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil || size < 0 || size > 2<<20 {
				return
			}
			payload = make([]byte, size+2)
			if _, err := io.ReadFull(reader, payload); err != nil {
				return
			}
			if strings.Contains(fields[1], ".CONSUMER.CREATE.OBJ_xfer-ttl") {
				r.mu.Lock()
				hold := r.hold
				if r.skip > 0 {
					r.skip--
					hold = nil
				} else {
					r.hold = nil
				}
				r.mu.Unlock()
				if hold != nil {
					if err := hold(); err != nil {
						r.mu.Lock()
						r.err = err
						r.mu.Unlock()
						return
					}
				}
			}
		}
		if _, err := io.WriteString(serverConn, line); err != nil {
			return
		}
		if len(payload) > 0 {
			if _, err := serverConn.Write(payload); err != nil {
				return
			}
		}
	}
}

func (r *reaperConsumerRelay) close() {
	r.cancel()
	_ = r.listener.Close()
	r.mu.Lock()
	for _, conn := range r.conns {
		_ = conn.Close()
	}
	r.mu.Unlock()
	r.wg.Wait()
}

// reaperListTrace wraps only observation: all lists and stream enumeration use
// the pinned SDK and the real server. No fixture metadata can release the wedge.
type reaperListTrace struct {
	jetstream.JetStream
	mu     sync.Mutex
	pass   context.Context
	lists  []context.Context
	errs   []error
	active int
}

func (s *reaperListTrace) ListStreams(ctx context.Context, opts ...jetstream.StreamListOpt) jetstream.StreamInfoLister {
	s.mu.Lock()
	s.pass = ctx
	s.mu.Unlock()
	return s.JetStream.ListStreams(ctx, opts...)
}

func (s *reaperListTrace) ObjectStore(ctx context.Context, bucket string) (jetstream.ObjectStore, error) {
	store, err := s.JetStream.ObjectStore(ctx, bucket)
	if err != nil {
		return nil, err
	}
	return reaperObservedStore{ObjectStore: store, trace: s}, nil
}

type reaperObservedStore struct {
	jetstream.ObjectStore
	trace *reaperListTrace
}

func (s reaperObservedStore) List(ctx context.Context, opts ...jetstream.ListObjectsOpt) ([]*jetstream.ObjectInfo, error) {
	s.trace.mu.Lock()
	s.trace.active++
	s.trace.lists = append(s.trace.lists, ctx)
	s.trace.mu.Unlock()
	objects, err := s.ObjectStore.List(ctx, opts...)
	s.trace.mu.Lock()
	s.trace.errs = append(s.trace.errs, err)
	s.trace.active--
	s.trace.mu.Unlock()
	return objects, err
}

// origin: S7 internal review rv-wiring F7
// The default registry must recover from both real TTL-empty watcher races.
// Five rounds include server consumer/subscription census before closing NATS,
// so connection shutdown cannot conceal an abandoned watcher.
func TestTransferProductionReaperTTLListsReleaseRegistry(t *testing.T) {
	// origin: hot-update S10 plan §11.1 / TEST-BUDGET
	if runIsolatedBrokerTest(t) {
		return
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprintf("round-%d", round), func(t *testing.T) { exerciseProductionReaperTTL(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "production transfer reaper TTL", beforeG)
	testharness.AssertNoFDLeak(t, "production transfer reaper TTL", beforeFD, 0)
}

func exerciseProductionReaperTTL(t *testing.T) {
	t.Helper()
	for _, aged := range []bool{false, true} {
		name := "reapBucketObjects"
		if aged {
			name = "xferBucketHasAgedObjects"
		}
		t.Run(name, func(t *testing.T) { exerciseProductionReaperTTLSite(t, aged) })
	}
}

func exerciseProductionReaperTTLSite(t *testing.T, aged bool) {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port, opts.JetStream, opts.StoreDir = -1, true, t.TempDir()
	server := natstest.RunServer(&opts)
	defer func() { server.Shutdown(); server.WaitForShutdown() }()
	root, cancel := context.WithCancel(t.Context())
	defer cancel()
	relay := newReaperConsumerRelay(root, t, server.Addr().String())
	defer relay.close()
	nc, err := nats.Connect("nats://"+relay.listener.Addr().String(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	// Inspection bypasses the held CREATE frame on the data connection.
	observer, err := nats.Connect(server.ClientURL(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	inspect, err := jetstream.New(observer)
	if err != nil {
		t.Fatal(err)
	}
	store, err := js.CreateObjectStore(root, jetstream.ObjectStoreConfig{
		Bucket: "xfer-ttl", Storage: jetstream.FileStorage, TTL: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := inspect.Stream(root, "OBJ_xfer-ttl")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "broker.db")
	db, err := storage.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	b, err := New(Config{NATSURL: server.ClientURL(), DB: db, DBPath: path, Logger: testharness.SilentLog()})
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.TransferFactory != nil || b.transferRuntime.Load() != nil {
		t.Fatal("fixture no longer exercises the default legacy reaper")
	}
	if aged {
		// A real zero-node session bucket is orphaned everywhere. With no
		// local tracker entry, production executes candidate then gauge List.
		b.selfID = "reaper-home"
	}
	trace := &reaperListTrace{JetStream: js}
	setBrokerJS(b, trace)
	defer func() { setBrokerJS(b, nil) }()
	b.reconcilers = newReconcileRegistry(b.cfg.Logger, b.reconcileLeaderGate)
	b.registerCoreReconcilePasses()
	unrelated := 0
	b.reconcilers.register("ttl-unrelated", b.cfg.XferReapInterval, authorityAny, func(ctx context.Context, _ time.Time) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		trace.mu.Lock()
		defer trace.mu.Unlock()
		if trace.active != 0 {
			return errors.New("reconcile continued before List joined")
		}
		unrelated++
		return nil
	})
	epoch := time.Now()
	b.reconcilers.start(epoch)
	// Drive the actual registered transfer pass independently of unrelated
	// broker services. The registry still owns ordering and its runMu lock.
	for _, pass := range b.reconcilers.passes {
		if pass.name != "xfer-orphan-reap" && pass.name != "ttl-unrelated" {
			pass.nextDue = epoch.Add(time.Hour)
		}
	}
	baselineSubscriptions := nc.NumSubscriptions()
	if _, err := store.PutBytes(root, "expires", []byte("real TTL metadata")); err != nil {
		t.Fatal(err)
	}
	expired := make(chan struct{})
	relay.mu.Lock()
	if aged {
		relay.skip = 1
	}
	relay.hold = func() error {
		ctx, stop := context.WithTimeout(root, 4*time.Second)
		defer stop()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			info, err := stream.Info(ctx)
			if err != nil {
				return err
			}
			if info.State.Msgs == 0 && info.State.LastSeq > 0 {
				close(expired)
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-ticker.C:
			}
		}
	}
	relay.mu.Unlock()
	finished := make(chan []string, 1)
	started := time.Now()
	go func() { finished <- b.reconcilers.runDue(root, epoch.Add(b.cfg.XferReapInterval)) }()
	// Rescue only cancels and joins the defective implementation; it never
	// writes metadata, changes the TTL, or lets a timeout count as success.
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(time.Second):
			t.Error("registry failed to join after test cancellation")
		}
	}()
	select {
	case <-expired:
	case <-time.After(4 * time.Second):
		t.Fatal("consumer CREATE never reached real TTL expiry")
	}
	if !testharness.WaitFor(t, time.Second, 10*time.Millisecond, func() bool {
		info, err := stream.Info(root)
		return err == nil && info.State.Msgs == 0 && info.State.Consumers > 0
	}) {
		t.Fatal("fixture failed to create the real TTL-empty watcher")
	}
	select {
	case ran := <-finished:
		finished <- ran // retain the join receipt for the deferred cleanup
		if len(ran) != 2 || ran[0] != "xfer-orphan-reap" || ran[1] != "ttl-unrelated" || unrelated != 1 {
			t.Fatalf("unrelated pass did not run after joined reaper: %v, count=%d", ran, unrelated)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("TTL-empty List froze the production reconcile registry")
	}
	if elapsed := time.Since(started); elapsed > 6*time.Second {
		t.Fatalf("pass exceeded five-second budget plus scheduling tolerance: %s", elapsed)
	}
	trace.mu.Lock()
	passDeadline, bounded := trace.pass.Deadline()
	lists, errs, active := append([]context.Context(nil), trace.lists...), append([]error(nil), trace.errs...), trace.active
	trace.mu.Unlock()
	want := 1
	if aged {
		want = 2
	}
	if !bounded || passDeadline.After(started.Add(5*time.Second+100*time.Millisecond)) || len(lists) != want || active != 0 {
		t.Fatalf("pass/List ownership: bounded=%v deadline=%v lists=%d want=%d active=%d", bounded, passDeadline, len(lists), want, active)
	}
	for _, ctx := range lists {
		until, ok := ctx.Deadline()
		if !ok || !until.Equal(passDeadline) || ctx.Err() == nil {
			t.Fatalf("List renewed the pass budget or failed to cancel: deadline=%v pass=%v err=%v", until, passDeadline, ctx.Err())
		}
	}
	for _, status := range b.reconcilers.status() {
		if status.Name == "xfer-orphan-reap" && (status.Failures != 1 || !strings.Contains(status.LastErr, "context deadline exceeded")) {
			t.Fatalf("registry lost deadline failure evidence: %+v", status)
		}
	}
	if !errors.Is(errs[len(errs)-1], context.DeadlineExceeded) {
		t.Fatalf("TTL-empty SDK List did not exit on deadline: %v", errs)
	}
	relay.mu.Lock()
	relayErr := relay.err
	relay.mu.Unlock()
	if relayErr != nil {
		t.Fatal(relayErr)
	}
	assertReaperWatchersGone(root, t, stream, nc, baselineSubscriptions)
	// A second sweep acquires runMu and completes against the now-empty bucket.
	before := time.Now()
	ran := b.reconcilers.runDue(root, epoch.Add(2*b.cfg.XferReapInterval))
	if len(ran) != 2 || unrelated != 2 || time.Since(before) > time.Second {
		t.Fatalf("next reconcile sweep did not recover: %v, unrelated=%d", ran, unrelated)
	}
	assertReaperWatchersGone(root, t, stream, nc, baselineSubscriptions)
}

func assertReaperWatchersGone(ctx context.Context, t *testing.T, stream jetstream.Stream, nc *nats.Conn, subscriptions int) {
	t.Helper()
	if !testharness.WaitFor(t, time.Second, 10*time.Millisecond, func() bool {
		info, err := stream.Info(ctx)
		return err == nil && info.State.Msgs == 0 && info.State.Consumers == 0 && nc.NumSubscriptions() == subscriptions
	}) {
		t.Fatalf("List left watcher/consumer residue: subscriptions=%d baseline=%d", nc.NumSubscriptions(), subscriptions)
	}
}

// origin: S7 internal review rv-wiring F7
func TestTransferReaperListsRetainEarlierDeadlineAndCancel(t *testing.T) {
	for _, aged := range []bool{false, true} {
		name := "reapBucketObjects"
		if aged {
			name = "xferBucketHasAgedObjects"
		}
		t.Run(name, func(t *testing.T) {
			store := &reviewListStore{}
			b := &Broker{cfg: Config{Logger: testharness.SilentLog()}}
			setBrokerJS(b, reviewListJS{store: store})
			defer func() { setBrokerJS(b, nil) }()
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if aged {
				b.xferBucketHasAgedObjects(ctx, "OBJ_xfer-lab")
			} else {
				b.reapBucketObjects(ctx, "OBJ_xfer-lab", time.Minute, false, nil, nil)
			}
			if !store.called {
				t.Fatal("actual List port was not called")
			}
			got, ok := store.ctx.Deadline()
			want, _ := ctx.Deadline()
			if !ok || !got.Equal(want) || store.ctx.Err() != context.Canceled || ctx.Err() != nil {
				t.Fatalf("List must retain the earlier deadline and cancel only its child: deadline=%v want=%v child=%v parent=%v", got, want, store.ctx.Err(), ctx.Err())
			}
		})
	}
}

// pendingExpiry is a pass context whose absolute deadline has already passed
// while its expiry callback has not yet been delivered: exactly the window in
// which a child List, stopped by its own timer at the same instant, returns.
type pendingExpiry struct {
	context.Context
	deadline time.Time
	done     chan struct{}
}

func (c *pendingExpiry) Deadline() (time.Time, bool) { return c.deadline, true }
func (c *pendingExpiry) Done() <-chan struct{}       { return c.done }
func (c *pendingExpiry) Err() error {
	select {
	case <-c.done:
		return context.DeadlineExceeded
	default:
		return nil
	}
}

type expiryStreamsJS struct {
	jetstream.JetStream
	listed chan struct{}
}

func (j expiryStreamsJS) ListStreams(context.Context, ...jetstream.StreamListOpt) jetstream.StreamInfoLister {
	close(j.listed)
	return emptyStreamLister{}
}

type emptyStreamLister struct{}

func (emptyStreamLister) Info() <-chan *jetstream.StreamInfo {
	infos := make(chan *jetstream.StreamInfo)
	close(infos)
	return infos
}
func (emptyStreamLister) Err() error { return nil }

// The full-package race run recorded an expired legacy sweep as a SUCCESS
// (Runs:1, Failures:0, empty LastErr): its List returned on its own timer at
// the pass deadline before the pass context's expiry callback had run. The
// pass must join that due expiry and report it, never sample ctx.Err() early;
// a pass whose deadline has not passed must not wait at all.
// origin: hot-update S7 plan lead LQ3 / T30 (S7 integration oplegacy reaper race)
func TestTransferReaperPassReportsDeadlineBeforeItsExpiryCallback(t *testing.T) {
	listed := make(chan struct{})
	b := &Broker{cfg: Config{Logger: testharness.SilentLog(), Now: time.Now}, transfers: newTransferTracker()}
	setBrokerJS(b, expiryStreamsJS{listed: listed})
	defer setBrokerJS(b, nil)
	// ctx-root: the test supplies the scheduler's pass context and owns its delivery.
	parent := &pendingExpiry{Context: context.Background(), deadline: time.Now().Add(-time.Millisecond), done: make(chan struct{})}
	result := make(chan error, 1)
	go func() {
		_, err := b.reconcileXferObjects(parent)
		result <- err
	}()
	<-listed
	select {
	case err := <-result:
		close(parent.done)
		t.Fatalf("expired pass returned before its expiry was delivered: err=%v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(parent.done)
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("expired pass lost its deadline reason: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("pass did not finish after its expiry was delivered")
	}
	// ctx-root: a live pass with time left never waits for an expiry.
	live, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	listed = make(chan struct{})
	setBrokerJS(b, expiryStreamsJS{listed: listed})
	if _, err := b.reconcileXferObjects(live); err != nil {
		t.Fatalf("a pass inside its deadline reported %v", err)
	}
}
