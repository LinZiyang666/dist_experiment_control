package infraattach

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	"github.com/LinZiyang666/tether/internal/proxydial"
	"github.com/nats-io/nats.go"
)

// client_inner_dialer_test.go keeps the hooks Prepare discovers by interface on
// the caller's inner dialer honest: a hook that exists but is backed by no
// owner must not be relied on to bound or cancel a dial.

// An agent's proxy dialer (proxydial.Options) has no admission owner yet still
// exposes FinishConnect. Its initial dial must stop at the gated deadline/stop,
// never at the proxy's own timeout: every HTTPS_PROXY agent takes this path.
// origin: S9 P22 resume — inner-dialer hook sweep (proxydial nil-owner class)
func TestClientInitialProxyDialStopsWithoutAdmissionOwner(t *testing.T) {
	proxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var held []net.Conn
	accepted := make(chan struct{})
	go func() {
		defer close(accepted)
		for conn, err := proxy.Accept(); err == nil; conn, err = proxy.Accept() {
			mu.Lock()
			held = append(held, conn) // accepted, and CONNECT is never answered
			mu.Unlock()
		}
	}()
	defer func() {
		_ = proxy.Close()
		<-accepted
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	env := map[string]string{"HTTPS_PROXY": "http://" + proxy.Addr().String()}
	options, err := proxydial.Options(func(key string) (string, bool) { v, ok := env[key]; return v, ok }, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	d, err := controlbudget.After(300 * time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	_, all, err := Prepare(options, d, Config{})
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	// A TEST-NET target: the loopback bypass must not skip the proxy hop.
	if nc, err := nats.Connect("nats://192.0.2.1:4222", append(all, nats.NoReconnect())...); err == nil {
		nc.Close()
		t.Fatal("a black-holed proxy admitted a connection")
	}
	if elapsed := time.Since(began); elapsed > 2*time.Second {
		t.Fatalf("initial proxied dial ran %s past its 300ms gated deadline", elapsed)
	}
	mu.Lock()
	reached := len(held)
	mu.Unlock()
	if reached == 0 {
		t.Fatal("the dial never reached the proxy: the bound was not exercised")
	}
}
