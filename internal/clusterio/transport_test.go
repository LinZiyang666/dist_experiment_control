package clusterio

import (
	"errors"
	"io"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S6 plan §3.2 / T16 / T55
func TestBrokerTransportLeakCycles(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("image requires Linux BOOTTIME")
	}
	exerciseStalledRPC(t) // warm the runtime's timer/netpoll roots before census
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseStalledRPC(t)
	}
	testharness.AssertNoGoroutineLeak(t, "tracked snapshot RPCs", beforeG)
	testharness.AssertNoFDLeak(t, "tracked snapshot RPCs", beforeFD, 0)
}

func transportDeadline(t *testing.T, duration time.Duration) controlbudget.Deadline {
	t.Helper()
	now, err := bootNow()
	if err != nil {
		t.Fatal(err)
	}
	return controlbudget.Deadline(now + int64(duration))
}

func exerciseStalledRPC(t *testing.T) {
	t.Helper()
	tracker := NewTracker()
	raw, peer := realTransportPair(t)
	defer checkImageCleanup(t, "peer.Close", peer.Close)
	entered := make(chan struct{})
	conn, err := tracker.Track(&readAdmissionConn{Conn: raw, entered: entered})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := tracker.TrackReader(conn)
	if err != nil {
		t.Fatal(err)
	}
	readReturned, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, reader)
		close(readReturned)
		<-release // the actual RPC owner has not returned merely because I/O did
		_ = reader.Close()
		_ = conn.Close()
		close(done)
	}()
	<-entered
	deadline := transportDeadline(t, 2*time.Second)
	if err := tracker.Fence(deadline); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readReturned:
	case <-time.After(time.Second):
		close(release)
		t.Fatal("fence left stalled snapshot io.Copy running")
	}
	c, r := tracker.Counts()
	if c != 1 || r != 1 {
		close(release)
		<-done
		t.Fatalf("socket close was reported as an owner join: connections=%d readers=%d", c, r)
	}
	if err := tracker.Join(transportDeadline(t, 10*time.Millisecond)); !errors.Is(err, ErrDeadline) {
		close(release)
		<-done
		t.Fatalf("unreturned handler joined: %v", err)
	}
	close(release)
	if err := tracker.Join(deadline); err != nil {
		t.Fatal(err)
	}
	<-done
	if c, r := tracker.Counts(); c != 0 || r != 0 {
		t.Fatalf("joined census: %d/%d", c, r)
	}
	exerciseOutgoingSourceCancellation(t)
	// A fence is permanent for this generation. Refused sockets remain the
	// caller's responsibility, including an already-open transport alias.
	rejected, other := net.Pipe()
	defer checkImageCleanup(t, "rejected.Close", rejected.Close)
	defer checkImageCleanup(t, "other.Close", other.Close)
	if c, err := tracker.Track(rejected); c != nil || !errors.Is(err, ErrFenced) {
		t.Fatalf("post-fence admission: %v %v", c, err)
	}
	if r, err := tracker.TrackReader(other); r != nil || !errors.Is(err, ErrFenced) {
		t.Fatalf("post-fence reader admission: %v %v", r, err)
	}
}

// origin: hot-update S6 plan §3.2 / T10 / T16
func TestBrokerTransportUnknownCloseAndReaderJoin(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("image requires Linux BOOTTIME")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseTransportUnknownClose(t)
	}
	testharness.AssertNoGoroutineLeak(t, "unknown transport close", beforeG)
	testharness.AssertNoFDLeak(t, "unknown transport close", beforeFD, 0)
}

type failedCloseConn struct{ net.Conn }

func (c failedCloseConn) Close() error {
	return errors.Join(c.Conn.Close(), errors.New("injected actual close uncertainty"))
}

type failedCloseSource struct{ io.ReadCloser }

func (s failedCloseSource) Close() error {
	return errors.Join(s.ReadCloser.Close(), errors.New("injected source close uncertainty"))
}

type heldReader struct {
	entered chan struct{}
	release chan struct{}
}

func (r heldReader) Read([]byte) (int, error) { close(r.entered); <-r.release; return 0, io.EOF }

func exerciseTransportUnknownClose(t *testing.T) {
	t.Helper()
	raw, peer := net.Pipe()
	defer checkImageCleanup(t, "peer.Close", peer.Close)
	tracker := NewTracker()
	c, err := tracker.Track(failedCloseConn{raw})
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Fence(transportDeadline(t, time.Second)); err == nil {
		t.Fatal("failed socket close reported as closed")
	}
	if err := c.Close(); err == nil {
		t.Fatal("actual owner close lost the uncertainty")
	}
	if c, r := tracker.Counts(); c != 1 || r != 0 {
		t.Fatalf("unknown close was erased: %d/%d", c, r)
	}
	if err := tracker.Join(transportDeadline(t, time.Second)); err == nil {
		t.Fatal("unknown connection permits a join receipt")
	}
	tracker = NewTracker()
	pipe, writer := io.Pipe()
	defer checkImageCleanup(t, "writer.Close", writer.Close)
	uncertainReader, err := tracker.TrackReader(failedCloseSource{pipe})
	if err != nil {
		t.Fatal(err)
	}
	if err := tracker.Fence(transportDeadline(t, time.Second)); err == nil {
		t.Fatal("failed snapshot source close reported as closed")
	}
	if err := uncertainReader.Close(); err == nil {
		t.Fatal("reader owner discarded source close uncertainty")
	}
	if c, r := tracker.Counts(); c != 0 || r != 1 {
		t.Fatalf("unknown snapshot source erased: %d/%d", c, r)
	}
	if err := tracker.Join(transportDeadline(t, time.Second)); err == nil {
		t.Fatal("unknown snapshot source permits join")
	}
	tracker = NewTracker()
	source := heldReader{make(chan struct{}), make(chan struct{})}
	reader, err := tracker.TrackReader(source)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { _, _ = reader.Read(make([]byte, 1)); close(done) }()
	<-source.entered
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Fence(transportDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	if c, r := tracker.Counts(); c != 0 || r != 1 {
		close(source.release)
		<-done
		t.Fatalf("outstanding Read erased before return: %d/%d", c, r)
	}
	close(source.release)
	<-done
	if err := tracker.Join(transportDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	// Invalid deadlines are rejected before fencing an otherwise live owner.
	tracker = NewTracker()
	if err := tracker.Fence(0); !errors.Is(err, ErrDeadline) {
		t.Fatalf("zero deadline: %v", err)
	}
	raw, peer2 := net.Pipe()
	defer checkImageCleanup(t, "peer2.Close", peer2.Close)
	conn, err := tracker.Track(raw)
	if err != nil {
		t.Fatalf("invalid deadline fenced live admission: %v", err)
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if err := tracker.Join(transportDeadline(t, time.Second)); err == nil {
		t.Fatal("join before fence accepted")
	}
	tracker.NoteCloseError(errors.New("injected staging file close uncertainty"))
	if err := tracker.Fence(transportDeadline(t, time.Second)); err == nil {
		t.Fatal("staging close error was erased despite zero socket counts")
	}
	if err := tracker.Join(transportDeadline(t, time.Second)); err == nil {
		t.Fatal("staging close error permits join")
	}
}

// Read admission is a barrier inside the real socket operation: fencing before
// the worker has even called Read would make cancellation coverage vacuous.
type readAdmissionConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *readAdmissionConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Read(p)
}

func realTransportPair(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer checkImageCleanup(t, "listener.Close", listener.Close)
	peer, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	conn, err := listener.Accept()
	if err != nil {
		_ = peer.Close()
		t.Fatal(err)
	}
	return conn, peer
}

func exerciseOutgoingSourceCancellation(t *testing.T) {
	t.Helper()
	source, writer := io.Pipe()
	defer checkImageCleanup(t, "writer.Close", writer.Close)
	tracker := NewTracker()
	entered := make(chan struct{})
	reader, err := tracker.TrackReader(&readAdmissionSource{ReadCloser: source, entered: entered})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, reader); _ = reader.Close(); done <- err }()
	<-entered
	if err := tracker.Fence(transportDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("truncated outgoing source succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("outgoing snapshot source not cancelled")
	}
	if err := tracker.Join(transportDeadline(t, time.Second)); err != nil {
		t.Fatal(err)
	}
}

type readAdmissionSource struct {
	io.ReadCloser
	entered chan struct{}
	once    sync.Once
}

func (r *readAdmissionSource) Read(p []byte) (int, error) {
	r.once.Do(func() { close(r.entered) })
	return r.ReadCloser.Read(p)
}

// Cleanup still runs after a failed assertion. Report unexpected close errors
// without aborting the remaining defers; fenced sockets may already be closed.
// origin: hot-update S6 plan §3.2 / T55
func checkImageCleanup(t *testing.T, name string, closeOwner func() error) {
	t.Helper()
	if err := closeOwner(); err != nil && !errors.Is(err, net.ErrClosed) {
		t.Errorf("%s: %v", name, err)
	}
}
