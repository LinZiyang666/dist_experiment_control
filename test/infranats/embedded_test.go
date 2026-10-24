// Package infranats_test keeps independent raw JS and socket receipts. Embedded
// tests prove the pinned client/server seam; they do not sign systemd, host
// placement, the broker recovery gate or an external running executable.
package infranats_test

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/testharness"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func budget(t *testing.T, duration time.Duration) c.Deadline {
	t.Helper()
	d, err := controlbudget.After(duration)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func embedded(t *testing.T) *server.Server {
	t.Helper()
	s, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1,
		JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go s.Start()
	t.Cleanup(func() { s.Shutdown(); s.WaitForShutdown() })
	if !s.ReadyForConnections(5 * time.Second) {
		t.Fatal("embedded NATS not ready")
	}
	if server.VERSION != "2.14.6" {
		t.Fatalf("embedded NATS pin changed: %s", server.VERSION)
	}
	return s
}

func dataConnection(t *testing.T, address string) jetstream.JetStream {
	t.Helper()
	nc, err := nats.Connect(address, nats.NoReconnect(), nats.ReconnectBufSize(-1), nats.Timeout(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc, jetstream.WithDefaultTimeout(300*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	return js
}

func objectSpec(t *testing.T, js jetstream.JetStream, body []byte) x.ObjectSpec {
	t.Helper()
	_, err := js.CreateObjectStore(t.Context(), jetstream.ObjectStoreConfig{Bucket: "payload", Storage: jetstream.FileStorage})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := js.Stream(t.Context(), "OBJ_payload")
	if err != nil {
		t.Fatal(err)
	}
	return x.ObjectSpec{Identity: x.Identity{SID: "session", NID: "node", ActorFP: "actor", AgentInstance: "agent",
		TransferID: "originalTransfer", EntryNonce: "originalNonce", IntentDigest: hexDigest([]byte("intent")), ObjectVersion: "originalNUID"},
		Bucket: "payload", Stream: "OBJ_payload", StreamCreated: stream.CachedInfo().Created.Format(time.RFC3339Nano),
		Size: int64(len(body)), SHA256: hexDigest(body), ChunkSize: 32 << 10, Chunks: uint32((len(body) + (32 << 10) - 1) / (32 << 10))}
}

func hexDigest(b []byte) string { sum := sha256.Sum256(b); return hex.EncodeToString(sum[:]) }

// A loss is custody of a real server reply, consumed before any of its bytes
// reach the client. The fence is installed before forwarding the matching PUB.
// A closed TCP connection alone is not evidence that an ACK was lost.
type replyLoss struct {
	Subject           string
	Request, Reply    []byte
	FencedBeforeSend  bool
	ReplyBytesWritten int
	Forwarded         int
}

type replyFence struct {
	mu       sync.Mutex
	ln       net.Listener
	conns    map[net.Conn]bool
	wg       sync.WaitGroup
	accepted chan struct{}
	match    func(string) bool
	loss     chan replyLoss
	release  chan struct{}
	closing  bool
}

func newReplyFence(t *testing.T, upstream string) (*replyFence, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	r := &replyFence{ln: ln, conns: make(map[net.Conn]bool), accepted: make(chan struct{})}
	go func() {
		defer close(r.accepted)
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			r.mu.Lock()
			r.conns[client] = true
			r.mu.Unlock()
			r.wg.Add(1)
			go r.serve(client, strings.TrimPrefix(upstream, "nats://"))
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		<-r.accepted
		r.mu.Lock()
		r.closing = true
		if r.release != nil {
			close(r.release)
			r.release = nil
		}
		for conn := range r.conns {
			_ = conn.Close()
		}
		r.mu.Unlock()
		r.wg.Wait()
	})
	return r, "nats://" + ln.Addr().String()
}

func (r *replyFence) arm(match func(string) bool) <-chan replyLoss {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.match, r.loss, r.release = match, make(chan replyLoss, 1), make(chan struct{})
	return r.loss
}

func (r *replyFence) resume() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.release != nil {
		close(r.release)
		r.release = nil
	}
}

func readFrame(rd *bufio.Reader) ([]byte, []byte, []string, error) {
	line, err := rd.ReadBytes('\n')
	if err != nil {
		return nil, nil, nil, err
	}
	fields := strings.Fields(string(line))
	if len(fields) == 0 {
		return line, nil, fields, nil
	}
	switch fields[0] {
	case "PUB", "HPUB", "MSG", "HMSG":
	default:
		return line, nil, fields, nil
	}
	size, err := strconv.Atoi(fields[len(fields)-1])
	if err != nil || size < 0 || size > 8<<20 {
		return nil, nil, nil, fmt.Errorf("invalid NATS payload length")
	}
	body := make([]byte, size+2)
	if _, err := io.ReadFull(rd, body); err != nil {
		return nil, nil, nil, err
	}
	if !bytes.HasSuffix(body, []byte("\r\n")) {
		return nil, nil, nil, fmt.Errorf("invalid NATS terminator")
	}
	data := body[:size]
	if fields[0] == "HPUB" || fields[0] == "HMSG" {
		header, err := strconv.Atoi(fields[len(fields)-2])
		if err != nil || header < 0 || header > size {
			return nil, nil, nil, fmt.Errorf("invalid NATS header length")
		}
		data = data[header:]
	}
	return append(line, body...), data, fields, nil
}

func (r *replyFence) serve(client net.Conn, upstream string) {
	defer r.wg.Done()
	peer, err := net.DialTimeout("tcp", upstream, time.Second)
	if err != nil {
		_ = client.Close()
		return
	}
	r.mu.Lock()
	if r.closing {
		r.mu.Unlock()
		_ = client.Close()
		_ = peer.Close()
		return
	}
	r.conns[peer] = true
	r.mu.Unlock()
	defer func() {
		_ = peer.Close()
		_ = client.Close()
		r.mu.Lock()
		delete(r.conns, peer)
		delete(r.conns, client)
		r.mu.Unlock()
	}()
	var mu sync.Mutex
	blocked := make(map[string]replyLoss)
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		defer func() { _ = client.Close(); _ = peer.Close() }()
		rd := bufio.NewReader(peer)
		for {
			frame, body, fields, err := readFrame(rd)
			if err != nil {
				return
			}
			mu.Lock()
			if len(fields) > 1 && (fields[0] == "MSG" || fields[0] == "HMSG") {
				if loss, ok := blocked[fields[1]]; ok {
					loss.Reply = append([]byte(nil), body...)
					delete(blocked, fields[1])
					mu.Unlock()
					r.mu.Lock()
					ch := r.loss
					r.mu.Unlock()
					ch <- loss
					continue
				}
			}
			_, err = client.Write(frame)
			mu.Unlock()
			if err != nil {
				return
			}
		}
	}()
	rd := bufio.NewReader(client)
	var wait <-chan struct{}
	for {
		frame, body, fields, err := readFrame(rd)
		if err != nil {
			break
		}
		if wait != nil {
			<-wait
			wait = nil
		}
		mu.Lock()
		r.mu.Lock()
		if len(fields) >= 4 && (fields[0] == "PUB" || fields[0] == "HPUB") && r.match != nil && r.match(fields[1]) {
			reply := fields[2]
			blocked[reply] = replyLoss{Subject: fields[1], Request: append([]byte(nil), body...), FencedBeforeSend: true}
			r.match = nil
			wait = r.release
			// The exact matching request is sent only after its reply is fenced.
			n, e := peer.Write(frame)
			loss := blocked[reply]
			loss.Forwarded = n
			blocked[reply] = loss
			err = e
		} else {
			_, err = peer.Write(frame)
		}
		r.mu.Unlock()
		mu.Unlock()
		if err != nil {
			break
		}
	}
	_ = peer.Close()
	_ = client.Close()
	<-joined
}

func requireLoss(t *testing.T, ch <-chan replyLoss) replyLoss {
	t.Helper()
	select {
	case loss := <-ch:
		if !loss.FencedBeforeSend || loss.Forwarded == 0 || len(loss.Reply) == 0 || loss.ReplyBytesWritten != 0 {
			t.Fatal("lost reply lacks sent/effect/non-delivery custody")
		}
		return loss
	case <-time.After(3 * time.Second):
		t.Fatal("fault did not consume an actual server reply")
	}
	return replyLoss{}
}

// origin: hot-update S9 plan §4.3 / §9.2 / T59 / M20–M21
func TestNATSLostACKAndConsumerContinuation(t *testing.T) {
	t.Run("publish_and_consumer_reply_fences", exerciseNATSLifecycle)
	t.Run("ordinary_auth_reconnect", exerciseOrdinaryAuth)
	t.Run("JS503_original_checkpoint", exerciseJSUnavailable)
}

// Each round creates actual JS assets, loses publish and create replies, reads
// the original raw sequence, retires a consumer and closes all byte owners.
// origin: hot-update S9 plan §12 P28 lifecycle
func TestInfraNATSLifecycle(t *testing.T) {
	goroutines, fds := runtime.NumGoroutine(), testharness.FDCount()
	defer testharness.AssertNoGoroutineLeak(t, "infra NATS", goroutines)
	defer testharness.AssertNoFDLeak(t, "infra NATS", fds, 0)
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseNATSLifecycle(t) })
		t.Run(fmt.Sprint("auth", round), func(t *testing.T) { exerciseOrdinaryAuth(t) })
		t.Run(fmt.Sprint("503", round), func(t *testing.T) { exerciseJSUnavailable(t) })
	}
}

func exerciseNATSLifecycle(t *testing.T) {
	s := embedded(t)
	observer := dataConnection(t, s.ClientURL())
	fence, url := newReplyFence(t, s.ClientURL())
	js := dataConnection(t, url)
	body := bytes.Repeat([]byte("original bytes\r\n"), 9000)
	spec := objectSpec(t, js, body)
	w, err := jsstream.NewTransferWriter(js, spec)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := w.Stop(budget(t, time.Second)); err != nil {
			t.Error(err)
		}
	})
	lost := fence.arm(func(subject string) bool { return subject == "$O.payload.C."+spec.Identity.ObjectVersion })
	result := make(chan error, 1)
	go func() { _, err := w.Append(0, body[:spec.ChunkSize], budget(t, 400*time.Millisecond)); result <- err }()
	loss := requireLoss(t, lost)
	var ack jetstream.PubAck
	if err := json.Unmarshal(loss.Reply, &ack); err != nil || ack.Sequence == 0 || ack.Stream != spec.Stream {
		t.Fatal("publish did not have a real JS effect")
	}
	stream, err := observer.Stream(t.Context(), spec.Stream)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := stream.GetMsg(t.Context(), ack.Sequence)
	if err != nil || raw.Subject != "$O.payload.C."+spec.Identity.ObjectVersion || !bytes.Equal(raw.Data, body[:spec.ChunkSize]) {
		t.Fatal("ACK loss lacked independent raw readback")
	}
	fence.resume()
	if err := <-result; err == nil {
		t.Fatal("fenced publish ACK reached the writer")
	}
	state, err := w.Freeze(budget(t, time.Second))
	if err != nil || state.PendingAttempt == nil || len(state.Receipts) != 0 || state.ObjectSpec != spec {
		t.Fatal("lost ACK discarded the original pending attempt")
	}
	next := dataConnection(t, s.ClientURL())
	resumed, err := jsstream.ImportTransferWriter(next, state)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := resumed.Stop(budget(t, time.Second)); err != nil {
			t.Error(err)
		}
	})
	if err := jsstream.ThawTransferWriter(resumed, budget(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	receipt, found, err := resumed.ResolvePending(budget(t, time.Second))
	if err != nil || !found || receipt.StreamSeq != ack.Sequence || receipt.SHA256 != sha256.Sum256(raw.Data) {
		t.Fatal("resume did not resolve the original sequence")
	}
	for idx := uint32(1); idx < spec.Chunks; idx++ {
		start := int(idx * spec.ChunkSize)
		if _, err := resumed.Append(idx, body[start:min(start+int(spec.ChunkSize), len(body))], budget(t, time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := resumed.Finalize(budget(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	committed, err := resumed.Freeze(budget(t, time.Second))
	if err != nil || len(committed.Receipts) != int(spec.Chunks) || committed.ObjectSpec != spec {
		t.Fatal("completion changed original receipts or NUID")
	}
	verifyRaw(t, next, committed)

	// Losing CREATE's reply must retire the named consumer before another one
	// resumes from the last genuinely synced prefix. The server effect is read
	// while subsequent client requests remain behind the relay's forward fence.
	r, err := jsstream.NewTransferReader(js, spec, x.SinkCheckpoint{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := r.Stop(budget(t, time.Second)); err != nil {
			t.Error(err)
		}
	})
	lost = fence.arm(func(subject string) bool {
		return strings.HasPrefix(subject, "$JS.API.CONSUMER.CREATE."+spec.Stream+".")
	})
	type readResult struct {
		chunk x.Chunk
		err   error
	}
	reads := make(chan readResult, 1)
	go func() { chunk, err := r.ReadChunk(budget(t, 2*time.Second)); reads <- readResult{chunk, err} }()
	loss = requireLoss(t, lost)
	var created struct {
		Name  string              `json:"name"`
		Error *jetstream.APIError `json:"error"`
	}
	if err := json.Unmarshal(loss.Reply, &created); err != nil || created.Name == "" || created.Error != nil {
		t.Fatal("consumer create loss had no actual effect")
	}
	if _, err := stream.Consumer(t.Context(), created.Name); err != nil {
		t.Fatal("created consumer missing before recovery")
	}
	fence.resume()
	first := <-reads
	if first.err != nil {
		first.chunk, first.err = r.ReadChunk(budget(t, 3*time.Second))
	}
	if first.err != nil || first.chunk.Receipt.Index != 0 {
		t.Fatalf("reader failed to continue after lost create reply: %v", first.err)
	}
	if _, err := stream.Consumer(t.Context(), created.Name); err == nil {
		t.Fatal("lost-reply consumer was abandoned")
	}
	sink, err := os.CreateTemp(t.TempDir(), "original-sink")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := sink.Close(); err != nil {
			t.Error(err)
		}
	}()
	hash := sha256.New()
	cp := syncChunk(t, r, sink, hash, first.chunk)
	frozen, err := r.Freeze(budget(t, time.Second))
	if err != nil || frozen.DeletePending || frozen.DirtyBarrier || frozen.ConsumerIdentity == "" || frozen.ConsumerIdentity == created.Name || frozen.SinkCheckpoint.DurableOffset != cp.DurableOffset {
		t.Fatal("reader exported an unretired consumer or unsynced cursor")
	}
	if _, err := stream.Consumer(t.Context(), frozen.ConsumerIdentity); err == nil {
		t.Fatal("exported retirement receipt still names a live consumer")
	}
	requireConsumers(t, stream, 0)
	continued, err := jsstream.NewTransferReader(next, spec, cp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := continued.Stop(budget(t, time.Second)); err != nil {
			t.Error(err)
		}
	})
	for idx := uint32(1); idx < spec.Chunks; idx++ {
		chunk, err := continued.ReadChunk(budget(t, time.Second))
		if err != nil || chunk.Receipt.Index != idx || chunk.Receipt.StreamSeq != committed.Receipts[idx].StreamSeq {
			t.Fatalf("resumed reader changed raw ordinal %d: %v", idx, err)
		}
		cp = syncChunk(t, continued, sink, hash, chunk)
	}
	if cp.DurableOffset != spec.Size || hex.EncodeToString(hash.Sum(nil)) != spec.SHA256 {
		t.Fatal("original durable sink lost bytes")
	}
	if _, err := continued.Freeze(budget(t, time.Second)); err != nil {
		t.Fatal(err)
	}
	requireConsumers(t, stream, 0)
	verifyRaw(t, next, committed)
}
