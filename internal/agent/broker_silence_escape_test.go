package agent

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/clusterroster"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// connectProxyForSilence supplies two routable fixture names without external
// DNS or listening on a LAN interface. The product's real proxy-aware NATS
// dialer, session teardown, roster filtering, and successor subscriptions run.
func connectProxyForSilence(t *testing.T, targets map[string]string) string {
	t.Helper()
	var mu sync.Mutex
	var connections []net.Conn
	var pending sync.WaitGroup
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		destination, ok := targets[req.Host]
		if req.Method != http.MethodConnect || !ok {
			http.Error(w, "unknown fixture target", http.StatusBadGateway)
			return
		}
		backend, err := net.DialTimeout("tcp", destination, 2*time.Second)
		if err != nil {
			http.Error(w, "fixture backend unavailable", http.StatusBadGateway)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			_ = backend.Close()
			http.Error(w, "fixture cannot tunnel", http.StatusInternalServerError)
			return
		}
		client, buffer, err := hijacker.Hijack()
		if err != nil {
			_ = backend.Close()
			return
		}
		mu.Lock()
		connections = append(connections, client, backend)
		pending.Add(1)
		mu.Unlock()
		defer pending.Done()
		defer func() { _ = client.Close(); _ = backend.Close() }()
		if _, err := buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		if err := buffer.Flush(); err != nil {
			return
		}
		done := make(chan struct{})
		go func() { _, _ = io.Copy(backend, client); _ = backend.Close(); close(done) }()
		_, _ = io.Copy(client, backend)
		_ = client.Close()
		<-done
	}))
	t.Cleanup(func() {
		server.Close()
		mu.Lock()
		for _, conn := range connections {
			_ = conn.Close()
		}
		mu.Unlock()
		pending.Wait()
	})
	return server.URL
}

// origin: control-connection-recovery internal validation. A hook saying that a
// rebuild was requested does not prove the agent escaped onto the sole survivor.
// The two NATS servers are real; register replies are signed-roster stubs. No
// broker membership operation is simulated or claimed by this component test.
func TestBrokerSilenceEscapesToTheOnlyRemainingVoter(t *testing.T) {
	retiredURL, survivorURL := startNATS(t), startNATS(t)
	retiredAddress, err := url.Parse(retiredURL)
	if err != nil {
		t.Fatal(err)
	}
	survivorAddress, err := url.Parse(survivorURL)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL := connectProxyForSilence(t, map[string]string{
		"retired.invalid:4222": retiredAddress.Host, "survivor.invalid:4222": survivorAddress.Host,
	})
	t.Setenv("HTTPS_PROXY", proxyURL)
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	retired, err := nats.Connect(retiredURL)
	if err != nil {
		t.Fatal(err)
	}
	defer retired.Close()
	survivor, err := nats.Connect(survivorURL)
	if err != nil {
		t.Fatal(err)
	}
	defer survivor.Close()
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	roster, err := clusterroster.Build(seed, pub, 2, []proto.RosterBroker{
		{NodeID: "survivor", PublicHost: "survivor.invalid", NatsRoute: "nats://survivor.invalid:6222", Phase: proto.RosterPhaseVoter},
	}, time.Now().UTC().Format(time.RFC3339Nano), time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))
	if err != nil {
		t.Fatal(err)
	}
	var retiredRegistrations, retiredRefreshes, survivorRegistrations atomic.Int32
	if _, err := retired.Subscribe(proto.SubjNodeRegister("lab", "escape-agent"), func(msg *nats.Msg) {
		var request proto.NodeRegisterReq
		if json.Unmarshal(msg.Data, &request) != nil {
			return
		}
		if request.RosterRefreshOnly {
			retiredRefreshes.Add(1)
			return // its NATS transport remains healthy, but the broker goes silent
		}
		retiredRegistrations.Add(1)
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: roster, ConnectedBrokerID: "retired"})
		_ = msg.Respond(body)
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := survivor.Subscribe(proto.SubjNodeRegister("lab", "escape-agent"), func(msg *nats.Msg) {
		var request proto.NodeRegisterReq
		if json.Unmarshal(msg.Data, &request) != nil {
			return
		}
		if !request.RosterRefreshOnly {
			survivorRegistrations.Add(1)
		}
		body, _ := json.Marshal(proto.NodeRegisterResp{OK: true, Roster: roster, ConnectedBrokerID: "survivor"})
		_ = msg.Respond(body)
	}); err != nil {
		t.Fatal(err)
	}
	if err := retired.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := survivor.Flush(); err != nil {
		t.Fatal(err)
	}
	a, err := New(Config{NATSURL: "nats://retired.invalid:4222", SID: "lab", NID: "escape-agent", AccountPub: pub,
		HeartbeatInterval: time.Second, RegisterTimeout: 100 * time.Millisecond,
		RosterRefreshInterval: 50 * time.Millisecond, Logger: testharness.SilentLog()})
	if err != nil {
		t.Fatal(err)
	}
	a.rosterRefreshFailBackoff = 25 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("agent did not stop after its survivor handoff")
		}
	})
	if !testharness.WaitFor(t, 8*time.Second, 10*time.Millisecond, func() bool {
		return survivorRegistrations.Load() == 1
	}) {
		t.Fatalf("no actual survivor registration: old=%d refreshes=%d new=%d", retiredRegistrations.Load(), retiredRefreshes.Load(), survivorRegistrations.Load())
	}
	if retiredRegistrations.Load() != 1 || retiredRefreshes.Load() < maxSilentRosterRefreshes {
		t.Fatalf("handoff did not follow the intended silence path: old=%d refreshes=%d", retiredRegistrations.Load(), retiredRefreshes.Load())
	}
	nc := a.ncBox.Load()
	if nc == nil || !strings.Contains(nc.ConnectedUrl(), "survivor.invalid") {
		t.Fatal("registration was not followed by a live connection to the survivor")
	}
	// Empty argv is a deterministic agent-side reply without launching a
	// process. Receiving it through the survivor proves successor dispatch.
	if !testharness.WaitFor(t, 3*time.Second, 10*time.Millisecond, func() bool {
		msg, err := survivor.Request(proto.SubjCmdForwarded("lab", "escape-agent", "exec"), []byte(`{}`), 300*time.Millisecond)
		return err == nil && strings.Contains(string(msg.Data), "argv")
	}) {
		t.Fatal("the survivor cannot deliver a real control request to the rebuilt agent")
	}
}
