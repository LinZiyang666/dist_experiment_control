// These component tests run the pinned embedded JetStream server and real TCP.
// Faults hold publish frames or drop their replies; they never fabricate ACKs.
// Broker Raft, session deletion and same-PID image integration belong to P15/P32.
package xferhistory

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// origin: hot-update S7 plan §11.3 / T25
func TestTransferLifecycleNoLeaks(t *testing.T) {
	// Warm the runtime's network poller before taking the FD baseline.
	warm, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	_ = warm.Close()
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseTransferLifecycle(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "transfer history", goroutines)
	testharness.AssertNoFDLeak(t, "transfer history", fds, 0)
}

func exerciseTransferLifecycle(t *testing.T) {
	js, stream := historyFixture(t, "lifecycle", nil)
	p := newPublisher(t, js, "lifecycle")
	start := historyRecord(t, "lifecycle", "task", "start", nil)
	terminal := historyRecord(t, "lifecycle", "task", "complete", nil)
	for _, record := range []xfercontract.CanonicalRecord{start, terminal, terminal} {
		if _, err := p.Confirm(record, historyDeadline(t)); err != nil {
			t.Fatal(err)
		}
	}
	// A timed-out operation cannot destroy an already retained decision. The
	// next caller uses the same immutable bytes and obtains the same receipt.
	if _, err := p.Confirm(terminal, 1); err == nil {
		t.Fatal("expired confirmation succeeded")
	}
	replacement := newPublisher(t, js, "lifecycle")
	if _, err := replacement.Confirm(terminal, historyDeadline(t)); err != nil {
		t.Fatal(err)
	}
	if err := js.Conn().FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	info, err := stream.Info(t.Context())
	if err != nil || info.State.Msgs != 2 || info.State.Consumers != 0 {
		t.Fatalf("history/consumer ownership changed: %+v, %v", info, err)
	}
	js.Conn().Close()
	if _, err := replacement.Lookup(xfercontract.Key{SID: "lifecycle", TransferID: "task"}, historyDeadline(t)); err == nil {
		t.Fatal("closed transport supplied history evidence")
	}
}

func historyFixture(t *testing.T, sid string, change func(*jetstream.StreamConfig)) (jetstream.JetStream, jetstream.Stream) {
	t.Helper()
	js := connectHistory(t, testharness.StartJSNATS(t))
	cfg := jetstream.StreamConfig{
		Name: "custom_history", Subjects: []string{proto.SubjAuditTransfer(sid), "unrelated.>"},
		Storage: jetstream.FileStorage, Retention: jetstream.LimitsPolicy,
		Discard: jetstream.DiscardNew, MaxBytes: 1 << 20,
	}
	if change != nil {
		change(&cfg)
	}
	stream, err := js.CreateStream(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	return js, stream
}

func connectHistory(t *testing.T, url string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(url, nats.NoReconnect(), nats.ReconnectBufSize(-1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func newPublisher(t *testing.T, js jetstream.JetStream, sid string) Publisher {
	t.Helper()
	p, err := New(Config{JS: js, SID: sid})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func historyDeadline(t *testing.T) controlbudget.Deadline {
	t.Helper()
	d, err := controlbudget.After(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// historyRelay holds actual client PUB/HPUB frames AFTER their socket write.
// Original and retry therefore keep the same frozen H even after their local
// request contexts expire. Only real JetStream replies are inspected/dropped.
type historyRelay struct {
	listener net.Listener
	frames   chan *heldHistoryFrame
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    []net.Conn
}

type heldHistoryFrame struct {
	data     []byte
	upstream net.Conn
	writeMu  *sync.Mutex
	reply    chan []byte
}

func (f *heldHistoryFrame) release() error {
	f.writeMu.Lock()
	defer f.writeMu.Unlock()
	_, err := f.upstream.Write(f.data)
	return err
}

func startHistoryRelay(t *testing.T, url, subject string) *historyRelay {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &historyRelay{listener: listener, frames: make(chan *heldHistoryFrame, 8)}
	accepted := make(chan struct{})
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer close(accepted)
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			upstream, err := net.DialTimeout("tcp", strings.TrimPrefix(url, "nats://"), time.Second)
			if err != nil {
				_ = client.Close()
				return
			}
			r.mu.Lock()
			r.conns = append(r.conns, client, upstream)
			r.mu.Unlock()
			r.pipe(client, upstream, subject)
		}
	}()
	t.Cleanup(func() {
		_ = listener.Close()
		<-accepted
		// The accept worker joins before the connection snapshot, preventing a
		// late accepted pair from escaping cleanup. Its children are joined below.
		r.mu.Lock()
		for _, conn := range r.conns {
			_ = conn.Close()
		}
		r.mu.Unlock()
		r.wg.Wait()
	})
	return r
}

func (r *historyRelay) pipe(client, upstream net.Conn, subject string) {
	var writeMu, replyMu sync.Mutex
	replies := make(map[string]chan []byte)
	r.wg.Add(2)
	go func() {
		defer r.wg.Done()
		// Either pipe half or the relay cleanup may close a pair first; a
		// second close is expected to report net.ErrClosed.
		defer func() { _ = upstream.Close() }()
		reader := bufio.NewReader(client)
		for {
			frame, fields, err := readHistoryFrame(reader)
			if err != nil {
				return
			}
			if len(fields) >= 4 && (fields[0] == "PUB" || fields[0] == "HPUB") && fields[1] == subject {
				held := &heldHistoryFrame{data: frame, upstream: upstream, writeMu: &writeMu, reply: make(chan []byte, 1)}
				replyMu.Lock()
				replies[fields[2]] = held.reply
				replyMu.Unlock()
				r.frames <- held
				continue
			}
			writeMu.Lock()
			_, err = upstream.Write(frame)
			writeMu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	go func() {
		defer r.wg.Done()
		defer func() { _ = client.Close() }()
		reader := bufio.NewReader(upstream)
		for {
			frame, fields, err := readHistoryFrame(reader)
			if err != nil {
				return
			}
			if len(fields) >= 4 && (fields[0] == "MSG" || fields[0] == "HMSG") {
				replyMu.Lock()
				reply := replies[fields[1]]
				replyMu.Unlock()
				if reply != nil {
					reply <- frame
					continue
				}
			}
			if _, err := client.Write(frame); err != nil {
				return
			}
		}
	}()
}

func readHistoryFrame(reader *bufio.Reader) ([]byte, []string, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, nil, err
	}
	fields := strings.Fields(line)
	frame := []byte(line)
	if len(fields) >= 3 && (fields[0] == "PUB" || fields[0] == "HPUB" || fields[0] == "MSG" || fields[0] == "HMSG") {
		size, err := strconv.Atoi(fields[len(fields)-1])
		if err != nil || size < 0 || size > 2<<20 {
			return nil, nil, fmt.Errorf("invalid NATS frame size")
		}
		body := make([]byte, size+2)
		if _, err := io.ReadFull(reader, body); err != nil {
			return nil, nil, err
		}
		frame = append(frame, body...)
	}
	return frame, fields, nil
}

func receiveHistoryFrame(t *testing.T, frames <-chan *heldHistoryFrame) *heldHistoryFrame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(5 * time.Second):
		t.Fatal("production publisher did not send a frame")
		return nil
	}
}

// scanFailure supplies a failed read at the existing SDK interface, leaving
// every successful operation on the real server. It cannot invent evidence.
type scanFailure struct {
	jetstream.Stream
	read func(context.Context, uint64, ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error)
}

func (s scanFailure) GetMsg(ctx context.Context, seq uint64, opts ...jetstream.GetMsgOpt) (*jetstream.RawStreamMsg, error) {
	return s.read(ctx, seq, opts...)
}

type historyBinding struct {
	jetstream.JetStream
	stream  jetstream.Stream
	publish func(context.Context, *nats.Msg, ...jetstream.PublishOpt) (*jetstream.PubAck, error)
}

func (b historyBinding) Stream(ctx context.Context, name string) (jetstream.Stream, error) {
	if b.stream != nil {
		return b.stream, nil
	}
	return b.JetStream.Stream(ctx, name)
}

func (b historyBinding) PublishMsg(ctx context.Context, msg *nats.Msg, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if b.publish != nil {
		return b.publish(ctx, msg, opts...)
	}
	return b.JetStream.PublishMsg(ctx, msg, opts...)
}
