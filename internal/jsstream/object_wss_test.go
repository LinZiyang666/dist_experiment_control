package jsstream

// Real NATS, JetStream object chunks, verified TLS WebSockets, and control RPCs.
// Only the receiver's raw socket read rate is constrained to model a WAN link.
// This component test does not cover broker/agent wiring, auth_callout, or Caddy.

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"fmt"
	"io"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// objectWSSReadLimiter limits the actual TLS transport, not the destination
// writer. A slow writer alone would not recreate the server's outbound queue.
type objectWSSReadLimiter struct {
	net.Conn
	closed chan struct{}
	once   sync.Once
}

func (c *objectWSSReadLimiter) Read(p []byte) (int, error) {
	if len(p) > 16*1024 {
		p = p[:16*1024]
	}
	n, err := c.Conn.Read(p)
	if n > 0 {
		// sleep-fixture: this timer supplies exactly 2 MiB/s of network
		// receive bandwidth; it is not a readiness or scheduling barrier.
		timer := time.NewTimer(time.Duration(int64(time.Second) * int64(n) / (2 * 1024 * 1024)))
		select {
		case <-timer.C:
		case <-c.closed:
			timer.Stop()
		}
	}
	return n, err
}

func (c *objectWSSReadLimiter) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type objectWSSDialer struct{}

func (objectWSSDialer) Dial(network, addr string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &objectWSSReadLimiter{Conn: conn, closed: make(chan struct{})}, nil
}

func objectWSSTLSConfigs(t *testing.T) (*tls.Config, *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "object-wss-test"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:    x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}},
		&tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
}

type objectWSSControlResult struct {
	requests int
	failures int
	maximum  time.Duration
	lastErr  error
}

// origin: control-connection-recovery, 2026-09-20 tier-B push incident
// The exact incident payload size and default 1 MiB MaxPayload matter: the old
// ObjectStore.Get aggregates push delivery into WebSocket frames above the
// client's 8*MaxPayload cap. Its file can still finish after reconnecting, so
// payload integrity alone cannot detect the broken control plane.
func TestTransferObjectPreservesControlOverSlowWSS(t *testing.T) {
	serverTLS, clientTLS := objectWSSTLSConfigs(t)
	opts := &natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
		Websocket: natsserver.WebsocketOpts{Host: "127.0.0.1", Port: -1, TLSConfig: serverTLS},
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("WSS JetStream server did not become ready")
	}
	publisher, err := nats.Connect(srv.ClientURL(), nats.Name("object-wss-publisher"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)
	pubJS, err := jetstream.New(publisher)
	if err != nil {
		t.Fatal(err)
	}
	// 2 MiB/s needs about 32 seconds for the payload. The wider operation
	// deadline accommodates race instrumentation and the parallel matrix;
	// it does not widen the per-control-request failure threshold below.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	const bucket, object = "xfer-wss", "incident-payload"
	store, err := pubJS.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	const payloadSize = 66549440
	payload := make([]byte, payloadSize)
	if _, err := rand.Read(payload); err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256(payload)
	if _, err := store.Put(ctx, jetstream.ObjectMeta{Name: object}, bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}

	var disconnects, reconnects atomic.Int64
	var disconnectMu sync.Mutex
	var disconnectErrors []string
	control, err := nats.Connect(srv.WebsocketURL(), nats.Name("object-wss-control"),
		nats.Secure(clientTLS), nats.SetCustomDialer(objectWSSDialer{}),
		nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			disconnects.Add(1)
			disconnectMu.Lock()
			disconnectErrors = append(disconnectErrors, fmt.Sprint(err))
			disconnectMu.Unlock()
		}), nats.ReconnectHandler(func(_ *nats.Conn) { reconnects.Add(1) }))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(control.Close)
	if control.MaxPayload() != 1024*1024 {
		t.Fatalf("fixture changed the incident's 1 MiB MaxPayload: %d", control.MaxPayload())
	}
	initialCID, err := control.GetClientID()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := control.Subscribe("object-wss.echo", func(msg *nats.Msg) { _ = msg.Respond(msg.Data) }); err != nil {
		t.Fatal(err)
	}
	if err := control.FlushTimeout(5 * time.Second); err != nil {
		t.Fatal(err)
	}
	receiverJS, err := jetstream.New(control)
	if err != nil {
		t.Fatal(err)
	}
	receiverStore, err := receiverJS.ObjectStore(ctx, bucket)
	if err != nil {
		t.Fatal(err)
	}

	// A separate client sends real request/reply traffic to a subscription
	// on the very same WSS connection that receives the object chunks.
	probeCtx, stopProbe := context.WithCancel(ctx)
	probeDone := make(chan objectWSSControlResult, 1)
	go func() {
		var result objectWSSControlResult
		tick := time.NewTicker(200 * time.Millisecond)
		defer tick.Stop()
		defer func() { probeDone <- result }()
		for {
			select {
			case <-probeCtx.Done():
				return
			case <-tick.C:
				started := time.Now()
				reply, err := publisher.Request("object-wss.echo", []byte("control-alive"), 2*time.Second)
				result.requests++
				result.maximum = max(result.maximum, time.Since(started))
				if err != nil {
					result.failures++
					result.lastErr = err
				} else if !bytes.Equal(reply.Data, []byte("control-alive")) {
					result.failures++
					result.lastErr = fmt.Errorf("unexpected control reply: %q", reply.Data)
				}
			}
		}
	}()

	started := time.Now()
	reader, readErr := GetTransferObject(ctx, receiverJS, receiverStore, bucket, object)
	var received int64
	hash := sha256.New()
	if readErr == nil {
		received, readErr = io.Copy(hash, reader)
		if closeErr := reader.Close(); readErr == nil {
			readErr = closeErr
		}
	}
	stopProbe()
	probes := <-probeDone
	disconnectMu.Lock()
	errorsSeen := append([]string(nil), disconnectErrors...)
	disconnectMu.Unlock()
	t.Logf("received=%d elapsed=%s requests=%d failures=%d max_rpc=%s disconnects=%d reconnects=%d errors=%q",
		received, time.Since(started), probes.requests, probes.failures, probes.maximum, disconnects.Load(), reconnects.Load(), errorsSeen)
	if readErr != nil || received != payloadSize || !bytes.Equal(hash.Sum(nil), wantHash[:]) {
		t.Errorf("download incomplete/corrupt: bytes=%d err=%v hash=%x want=%x", received, readErr, hash.Sum(nil), wantHash)
	}
	if probes.requests < 5 || probes.failures != 0 {
		t.Errorf("control RPCs during the transfer: requests=%d failures=%d last_error=%v", probes.requests, probes.failures, probes.lastErr)
	}
	if disconnects.Load() != 0 || reconnects.Load() != 0 {
		t.Errorf("object delivery disconnected the shared control socket: disconnects=%d reconnects=%d errors=%q", disconnects.Load(), reconnects.Load(), errorsSeen)
	}
	currentCID, err := control.GetClientID()
	if err != nil || currentCID != initialCID {
		t.Errorf("control connection identity changed: before=%d after=%d err=%v", initialCID, currentCID, err)
	}

	stream, err := pubJS.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	var consumers int
	if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool {
		info, err := stream.Info(ctx)
		if err != nil {
			return false
		}
		consumers = info.State.Consumers
		return consumers == 0
	}) {
		t.Errorf("completed download left %d server consumer(s) behind", consumers)
	}
}

// origin: control-connection-recovery internal review, cancellation fault injection
// A canceled Fetch closes its local Messages channel before all its server-sent
// bytes reach the socket reader. Releasing the connection window at that point
// lets successive readers accumulate an oversized frame even with Fetch(4).
func TestTransferObjectCancellationPreservesControlOverSlowWSS(t *testing.T) {
	serverTLS, clientTLS := objectWSSTLSConfigs(t)
	srv, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true,
		Websocket: natsserver.WebsocketOpts{Host: "127.0.0.1", Port: -1, TLSConfig: serverTLS},
	})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	t.Cleanup(func() { srv.Shutdown(); srv.WaitForShutdown() })
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("WSS JetStream server did not become ready")
	}
	publisher, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(publisher.Close)
	pubJS, err := jetstream.New(publisher)
	if err != nil {
		t.Fatal(err)
	}
	// Each of 64 cancellations may have already delivered four 128 KiB
	// chunks. At 2 MiB/s the full 32 MiB takes 16 seconds to drain; the
	// operation budget also covers instrumented, parallel test execution.
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	const bucket, object = "cancel-window", "payload"
	store, err := pubJS.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{Bucket: bucket})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.PutBytes(ctx, object, make([]byte, 2*1024*1024)); err != nil {
		t.Fatal(err)
	}
	var disconnects atomic.Int64
	control, err := nats.Connect(srv.WebsocketURL(), nats.Name("cancel-window"),
		nats.Secure(clientTLS), nats.SetCustomDialer(objectWSSDialer{}),
		nats.MaxReconnects(-1), nats.ReconnectWait(50*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				disconnects.Add(1)
				t.Logf("control disconnect: %v", err)
			}
		}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(control.Close)
	initialCID, err := control.GetClientID()
	if err != nil {
		t.Fatal(err)
	}
	if control.MaxPayload() != 1024*1024 {
		t.Fatalf("unexpected MaxPayload: %d", control.MaxPayload())
	}
	if _, err := control.Subscribe("cancel.echo", func(m *nats.Msg) { _ = m.Respond(m.Data) }); err != nil {
		t.Fatal(err)
	}
	receiverJS, err := jetstream.New(control)
	if err != nil {
		t.Fatal(err)
	}
	receiverStore, err := receiverJS.ObjectStore(ctx, bucket)
	if err != nil {
		t.Fatal(err)
	}
	const readerCount = 64
	readers := make([]*transferObjectReader, 0, readerCount)
	cancels := make([]context.CancelFunc, 0, readerCount)
	t.Cleanup(func() {
		for _, cancelReader := range cancels {
			cancelReader()
		}
		for _, reader := range readers {
			_ = reader.Close()
		}
	})
	// Pre-create the readers before data starts. Otherwise metadata request
	// roundtrips would accidentally drain prior data and mask the defect.
	for range readerCount {
		readerCtx, cancelReader := context.WithCancel(ctx)
		cancels = append(cancels, cancelReader)
		reader, err := GetTransferObject(readerCtx, receiverJS, receiverStore, bucket, object)
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, reader.(*transferObjectReader))
	}
	probeCtx, stopProbe := context.WithCancel(ctx)
	probeDone := make(chan objectWSSControlResult, 1)
	go func() {
		var result objectWSSControlResult
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		defer func() { probeDone <- result }()
		for {
			select {
			case <-probeCtx.Done():
				return
			case <-ticker.C:
				started := time.Now()
				message, err := publisher.Request("cancel.echo", []byte("alive"), 2*time.Second)
				result.requests++
				result.maximum = max(result.maximum, time.Since(started))
				if err != nil || !bytes.Equal(message.Data, []byte("alive")) {
					result.failures++
					result.lastErr = err
				}
			}
		}
	}()
	var workers sync.WaitGroup
	closeErrors := make(chan error, readerCount)
	var maxPending uint64
	started := time.Now()
	for i, reader := range readers {
		workers.Add(1)
		go func(reader *transferObjectReader) {
			defer workers.Done()
			_, _ = reader.Read(make([]byte, 1))
		}(reader)
		// Observe the server through the unrestricted publisher connection:
		// cancellation must follow actual dispatch, without waiting for the
		// slow WSS client to acknowledge or consume those bytes.
		delivered := testharness.WaitFor(t, 5*time.Second, time.Millisecond, func() bool {
			consumer, err := pubJS.Consumer(ctx, "OBJ_"+bucket, reader.consumer.CachedInfo().Name)
			return err == nil && consumer.CachedInfo().Delivered.Consumer >= 4
		})
		cancels[i]()
		workers.Add(1)
		go func(reader *transferObjectReader) {
			defer workers.Done()
			closeErrors <- reader.Close()
		}(reader)
		if !delivered {
			t.Errorf("reader %d never dispatched its four-message batch", i)
			break
		}
		if connz, err := srv.Connz(&natsserver.ConnzOptions{}); err == nil {
			for _, conn := range connz.Conns {
				if conn.Name == "cancel-window" {
					maxPending = max(maxPending, uint64(conn.Pending))
				}
			}
		}
	}
	workers.Wait()
	close(closeErrors)
	for err := range closeErrors {
		if err != nil {
			t.Errorf("canceled reader cleanup failed: %v", err)
		}
	}
	stopProbe()
	probes := <-probeDone
	t.Logf("readers=%d elapsed=%s RPC=%d/%d max_rpc=%s disconnects=%d sampled_pending_max=%d",
		readerCount, time.Since(started), probes.failures, probes.requests, probes.maximum, disconnects.Load(), maxPending)
	if probes.requests < 5 || probes.failures != 0 || disconnects.Load() != 0 {
		t.Errorf("cancellation breached control availability: RPC=%d/%d drops=%d last_error=%v",
			probes.failures, probes.requests, disconnects.Load(), probes.lastErr)
	}
	currentCID, err := control.GetClientID()
	if err != nil || currentCID != initialCID {
		t.Errorf("control connection identity changed: %d -> %d: %v", initialCID, currentCID, err)
	}
	stream, err := pubJS.Stream(ctx, "OBJ_"+bucket)
	if err != nil {
		t.Fatal(err)
	}
	if !testharness.WaitFor(t, 5*time.Second, 20*time.Millisecond, func() bool {
		info, err := stream.Info(ctx)
		return err == nil && info.State.Consumers == 0
	}) {
		t.Error("canceled readers left server consumers behind")
	}
}
