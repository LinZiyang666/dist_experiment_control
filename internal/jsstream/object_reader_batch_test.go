package jsstream

// Metadata, stored chunks, and consumer creation use real JetStream. A second
// NATS protocol stream supplies controlled pull replies through the real SDK Fetch
// path, placing a status after the next request without relying on scheduling.
// This fixture covers batch ownership, not transport loss or broker wiring.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

type objectPullResult struct {
	data []byte
	err  error
}

// origin: S7 internal review OBJECT-READER-409 F1
func TestTransferObjectScopesLateBatchCompletionToSatisfiedPull(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseObjectLateBatchCompletion(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "object late batch completion", goroutines)
	testharness.AssertNoFDLeak(t, "object late batch completion", fds, 0)
}

func exerciseObjectLateBatchCompletion(t *testing.T) {
	for _, nextPrefix := range []int{0, 1} {
		t.Run(fmt.Sprintf("next_prefix_%d", nextPrefix), func(t *testing.T) {
			f := newControlledObjectPull(t)
			first := f.request(t)
			f.chunks(t, first, 0, 4)
			// Receiving the second NEXT proves the first Fetch has returned its
			// full count. Only now release its trailer, before or during batch 2.
			second := f.request(t)
			if first.Reply == second.Reply {
				t.Fatal("pull requests did not have distinct reply identities")
			}
			f.chunks(t, second, 4, 4+nextPrefix)
			f.status(t, first, "409", "Batch Completed")
			f.chunks(t, second, 4+nextPrefix, 8)
			got := f.result(t)
			if got.err != nil || !bytes.Equal(got.data, f.body) {
				t.Fatalf("late satisfied-batch trailer failed the next pull: bytes=%d want=%d err=%v", len(got.data), len(f.body), got.err)
			}
			if pending := len(f.requests); pending != 0 {
				t.Fatalf("trailer caused an extra pull: pending=%d", pending)
			}
			if subscriptions := f.nc.NumSubscriptions(); subscriptions != 0 {
				t.Fatalf("satisfied pulls retained %d reply subscriptions", subscriptions)
			}
		})
	}
}

// origin: S7 internal review OBJECT-READER-409 F1
func TestTransferObjectRejectsUnsatisfiedPullStatuses(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseObjectUnsatisfiedPullStatuses(t) })
	}
	testharness.AssertNoGoroutineLeak(t, "object unsatisfied pull statuses", goroutines)
	testharness.AssertNoFDLeak(t, "object unsatisfied pull statuses", fds, 0)
}

func exerciseObjectUnsatisfiedPullStatuses(t *testing.T) {
	for _, tc := range []struct {
		name, status, description string
		prefix                    int
		want                      error
	}{
		{"empty_completed", "409", "Batch Completed", 0, nil},
		{"short_completed", "409", "Batch Completed", 2, io.ErrUnexpectedEOF},
		{"consumer_deleted", "409", "Consumer Deleted", 0, nats.ErrConsumerDeleted},
		{"leadership_changed", "409", "Leadership Change", 0, nats.ErrConsumerLeadershipChanged},
		{"no_responders", "503", "No Responders", 0, nats.ErrNoResponders},
		{"different_status", "500", "Batch Completed", 0, nil},
		{"short_timeout", "408", "Request Timeout", 2, io.ErrUnexpectedEOF},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newControlledObjectPull(t)
			first := f.request(t)
			f.chunks(t, first, 0, 4)
			second := f.request(t)
			f.chunks(t, second, 4, 4+tc.prefix)
			f.status(t, second, tc.status, tc.description)
			got := f.result(t)
			if got.err == nil || (tc.want != nil && !errors.Is(got.err, tc.want)) ||
				(tc.want == nil && !strings.Contains(got.err.Error(), "Batch Completed")) {
				t.Fatalf("unsatisfied pull status lost its failure: bytes=%d err=%v want=%v", len(got.data), got.err, tc.want)
			}
			if !bytes.Equal(got.data, f.body[:(4+tc.prefix)*f.chunkSize]) {
				t.Fatalf("failed pull changed its delivered prefix: bytes=%d want=%d", len(got.data), (4+tc.prefix)*f.chunkSize)
			}
			if pending := len(f.requests); pending != 0 {
				t.Fatalf("failed pull was silently retried: pending=%d", pending)
			}
		})
	}
}

type controlledObjectPull struct {
	ctx       context.Context
	nc        *nats.Conn
	requests  chan *nats.Msg
	protocol  *objectPullProtocol
	reader    *transferObjectReader
	body      []byte
	chunkSize int
	done      chan objectPullResult
}

func newControlledObjectPull(t *testing.T) *controlledObjectPull {
	t.Helper()
	management := newJS(t, startJSNATS(t))
	ctx, store, stream := transferObjectFixture(t, management, "batch-status")
	const chunkSize = 128 << 10
	body := make([]byte, 8*chunkSize)
	for i := range body {
		body[i] = byte(i/chunkSize + 1)
	}
	info := putTransferObject(ctx, t, store, body)
	if info.Chunks != 8 {
		t.Fatalf("fixture needs eight SDK chunks: got %d", info.Chunks)
	}
	protocol := newObjectPullProtocol(t)
	data, err := nats.Connect("nats://controlled-pull.invalid:4222", nats.SetCustomDialer(protocol), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(data.Close)
	rctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	reader, err := GetTransferObject(rctx, transferReaderManagementJS{JetStream: management, data: data}, store, "batch-status", "payload")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := reader.Close(); err != nil {
			t.Error(err)
		}
		requireNoTransferObjectConsumers(ctx, t, stream)
	})
	r := reader.(*transferObjectReader)
	f := &controlledObjectPull{ctx: ctx, nc: data, requests: protocol.requests, protocol: protocol, reader: r, body: body, chunkSize: chunkSize, done: make(chan objectPullResult, 1)}
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		got, err := io.ReadAll(reader)
		f.done <- objectPullResult{got, err}
	}()
	t.Cleanup(func() { cancel(); <-joined })
	return f
}

func (f *controlledObjectPull) request(t *testing.T) *nats.Msg {
	t.Helper()
	select {
	case msg := <-f.requests:
		var request struct {
			Batch    int   `json:"batch"`
			MaxBytes int64 `json:"max_bytes"`
			Expires  int64 `json:"expires"`
		}
		if err := json.Unmarshal(msg.Data, &request); err != nil || request.Batch != 4 || request.MaxBytes != 5*f.nc.MaxPayload() || request.Expires <= 0 {
			t.Fatalf("pull lost its message/byte/deadline bounds: %s err=%v", msg.Data, err)
		}
		return msg
	case <-f.ctx.Done():
		t.Fatal("reader did not send its next pull:", f.ctx.Err())
		return nil
	}
}

func (f *controlledObjectPull) chunks(t *testing.T, request *nats.Msg, start, end int) {
	t.Helper()
	for i := start; i < end; i++ {
		msg := nats.NewMsg(request.Reply)
		// Match the pinned server's ACK metadata: stream/consumer ordinals
		// advance independently of the per-request reply subject.
		msg.Reply = fmt.Sprintf("$JS.ACK.%s.%s.1.%d.%d.%d.%d", f.reader.stream, f.reader.consumerName, i+1, i+1, time.Now().UnixNano(), 7-i)
		msg.Data = f.body[i*f.chunkSize : (i+1)*f.chunkSize]
		f.protocol.send(t, msg, nil)
	}
}

func (f *controlledObjectPull) status(t *testing.T, request *nats.Msg, status, description string) {
	t.Helper()
	msg := nats.NewMsg(request.Reply)
	header := fmt.Sprintf("NATS/1.0 %s %s\r\nNats-Pending-Messages: 0\r\nNats-Pending-Bytes: %d\r\n\r\n", status, description, 5*f.nc.MaxPayload()-int64(4*f.chunkSize))
	f.protocol.send(t, msg, []byte(header))
}

// A controlled server-side stream is needed because the pinned real server
// correctly refuses ordinary clients publishing forged $JS.ACK reply subjects.
// Keep real nats.go subscription routing, Fetch parsing, and context handling.
type objectPullProtocol struct {
	client, server net.Conn
	mu             sync.Mutex
	subs           map[string]string
	requests       chan *nats.Msg
}

func newObjectPullProtocol(t *testing.T) *objectPullProtocol {
	t.Helper()
	client, server := net.Pipe()
	p := &objectPullProtocol{client: client, server: server, subs: map[string]string{}, requests: make(chan *nats.Msg, 8)}
	done := make(chan error, 1)
	go func() { done <- p.run() }()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
		if err := <-done; err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
			t.Error("controlled pull protocol:", err)
		}
	})
	return p
}

func (p *objectPullProtocol) Dial(_, _ string) (net.Conn, error) { return p.client, nil }

func (p *objectPullProtocol) run() error {
	if _, err := io.WriteString(p.server, "INFO {\"server_id\":\"controlled-pull\",\"max_payload\":1048576,\"proto\":1,\"headers\":true}\r\n"); err != nil {
		return err
	}
	r := bufio.NewReader(p.server)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return err
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			return errors.New("empty protocol command")
		}
		switch fields[0] {
		case "CONNECT", "PONG":
		case "PING":
			p.mu.Lock()
			_, err = io.WriteString(p.server, "PONG\r\n")
			p.mu.Unlock()
			if err != nil {
				return err
			}
		case "SUB":
			p.mu.Lock()
			p.subs[fields[len(fields)-1]] = fields[1]
			p.mu.Unlock()
		case "UNSUB":
			p.mu.Lock()
			delete(p.subs, fields[1])
			p.mu.Unlock()
		case "PUB":
			if len(fields) != 4 || !strings.HasPrefix(fields[1], "$JS.API.CONSUMER.MSG.NEXT.") {
				return fmt.Errorf("unexpected pull publication: %s", line)
			}
			size, err := strconv.Atoi(fields[3])
			if err != nil {
				return err
			}
			data := make([]byte, size+2)
			if _, err := io.ReadFull(r, data); err != nil {
				return err
			}
			p.requests <- &nats.Msg{Subject: fields[1], Reply: fields[2], Data: data[:size]}
		default:
			return fmt.Errorf("unexpected protocol command: %s", line)
		}
	}
}

func (p *objectPullProtocol) send(t *testing.T, msg *nats.Msg, header []byte) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	for sid, subject := range p.subs {
		if subject != msg.Subject && (!strings.HasSuffix(subject, ".*") || !strings.HasPrefix(msg.Subject, strings.TrimSuffix(subject, "*"))) {
			continue
		}
		var frame bytes.Buffer
		if len(header) > 0 {
			fmt.Fprintf(&frame, "HMSG %s %s %d %d\r\n", msg.Subject, sid, len(header), len(header)+len(msg.Data))
		} else {
			fmt.Fprintf(&frame, "MSG %s %s %s %d\r\n", msg.Subject, sid, msg.Reply, len(msg.Data))
		}
		frame.Write(header)
		frame.Write(msg.Data)
		frame.WriteString("\r\n")
		if _, err := p.server.Write(frame.Bytes()); err != nil {
			t.Fatal(err)
		}
	}
}

func (f *controlledObjectPull) result(t *testing.T) objectPullResult {
	t.Helper()
	select {
	case got := <-f.done:
		return got
	case request := <-f.requests:
		t.Fatal("reader sent an extra pull instead of completing its batch:", request.Subject)
		return objectPullResult{}
	case <-f.ctx.Done():
		t.Fatal("reader did not finish its bounded pull:", f.ctx.Err())
		return objectPullResult{}
	}
}
