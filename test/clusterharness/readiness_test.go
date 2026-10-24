package clusterharness

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	server "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// Client listeners precede metadata readiness. A real three-server election
// must complete, and a denied API on the final ingress must still fail even
// when every server-level metadata/route observation is healthy.
// origin: hot-update S6 plan §5 / §10.1 / HARDGATE-B request 3
func TestRoutedJetStreamReadinessRequiresEveryIngress(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseRoutedJetStreamReadiness(t)
	}
	testharness.AssertNoGoroutineLeak(t, "routed JetStream readiness", g)
	testharness.AssertNoFDLeak(t, "routed JetStream readiness", fd, 0)
}

func exerciseRoutedJetStreamReadiness(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	var routes []string
	var ports []int
	for i := 0; i < 3; i++ {
		port := FreePort(t)
		ports = append(ports, port)
		routes = append(routes, fmt.Sprintf("nats://127.0.0.1:%d", port))
	}
	var servers []*server.Server
	var connections []*nats.Conn
	defer func() {
		for _, nc := range connections {
			nc.Close()
		}
		for _, s := range servers {
			s.Shutdown()
		}
		for _, s := range servers {
			s.WaitForShutdown()
		}
	}()
	for i := 0; i < 3; i++ {
		s, err := server.NewServer(&server.Options{
			Host: "127.0.0.1", Port: -1, ServerName: fmt.Sprintf("readiness-%d", i),
			JetStream: true, StoreDir: filepath.Join(root, fmt.Sprint(i)), NoLog: true, NoSigs: true,
			Cluster: server.ClusterOpts{Name: "readiness", Host: "127.0.0.1", Port: ports[i]},
			Routes:  server.RoutesFromStr(strings.Join(routes, ",")),
			Users: []*server.User{
				{Username: "witness", Password: "witness"},
				{Username: "denied", Password: "denied", Permissions: &server.Permissions{Publish: &server.SubjectPermission{Deny: []string{"$JS.API.INFO"}}}},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		// No Raft timer is shortened to make the real metadata election fit
		// the infrastructure readiness budget.
		servers = append(servers, s)
		s.Start()
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatal("readiness NATS client listener expired")
		}
		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("witness", "witness"), nats.NoReconnect())
		if err != nil {
			t.Fatal(err)
		}
		connections = append(connections, nc)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	if err := waitRoutedJetStream(ctx, servers, connections); err != nil {
		t.Fatal(err)
	}
	leaders := 0
	for i, s := range servers {
		if s.NumRoutes() < 2 {
			t.Fatalf("client listeners were mistaken for a complete route mesh: server=%s routes=%d", s.Name(), s.NumRoutes())
		}
		if s.JetStreamIsLeader() {
			leaders++
			if len(s.JetStreamClusterPeers()) != 3 {
				t.Fatal("readiness admitted an incomplete metadata peer set")
			}
		}
		js, err := connections[i].JetStream()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := js.AccountInfo(nats.Context(ctx)); err != nil {
			t.Fatalf("readiness had no usable API through %s: %v", s.Name(), err)
		}
	}
	if leaders != 1 {
		t.Fatalf("metadata leader count=%d", leaders)
	}
	// Current() can change as fresh metadata entries arrive after the witness.
	// The independent API reads above prove actual usability; the witness does
	// not promise that every follower cursor stays equal forever afterwards.
	blockedCtx, blockedCancel := context.WithCancel(ctx)
	defer blockedCancel()
	denied, err := nats.Connect(servers[2].ClientURL(), nats.UserInfo("denied", "denied"), nats.NoReconnect(), nats.ErrorHandler(func(*nats.Conn, *nats.Subscription, error) { blockedCancel() }))
	if err != nil {
		t.Fatal(err)
	}
	connections = append(connections, denied)
	blocked := append([]*nats.Conn(nil), connections[:3]...)
	blocked[2] = denied
	if err := waitRoutedJetStream(blockedCtx, servers, blocked); !errors.Is(err, context.Canceled) {
		t.Fatalf("last ingress without an API witness was admitted: %v", err)
	}
	if denied.LastError() == nil {
		t.Fatal("negative control never exercised the final ingress AccountInfo")
	}
	if err := waitRoutedJetStream(ctx, servers, connections[:2]); err == nil {
		t.Fatal("missing ingress was accepted as complete readiness")
	}
	canceledCtx, canceledCancel := context.WithCancel(t.Context())
	canceledCancel()
	if err := waitRoutedJetStream(canceledCtx, servers, connections[:3]); !errors.Is(err, context.Canceled) {
		t.Fatalf("infrastructure readiness escaped cancellation: %v", err)
	}
	// A separate standalone JetStream answers AccountInfo too. It must not
	// substitute for a missing member of this routed metadata group.
	isolated, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, ServerName: "isolated", JetStream: true, StoreDir: filepath.Join(root, "isolated"), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	isolated.Start()
	defer func() { isolated.Shutdown(); isolated.WaitForShutdown() }()
	if !isolated.ReadyForConnections(5 * time.Second) {
		t.Fatal("isolated JetStream client listener expired")
	}
	isolatedNC, err := nats.Connect(isolated.ClientURL(), nats.NoReconnect())
	if err != nil {
		t.Fatal(err)
	}
	defer isolatedNC.Close()
	isolatedJS, err := isolatedNC.JetStream()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := isolatedJS.AccountInfo(nats.Context(ctx)); err != nil {
		t.Fatal("isolated API control is not actually usable", err)
	}
	incompleteCtx, incompleteCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer incompleteCancel()
	if err := waitRoutedJetStream(incompleteCtx, append(servers, isolated), append(connections[:3:3], isolatedNC)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("independent APIs were mistaken for one complete metadata group: %v", err)
	}
}
