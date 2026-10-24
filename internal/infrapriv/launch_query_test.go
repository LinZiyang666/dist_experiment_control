//go:build linux && amd64

package infrapriv

import (
	"crypto/ed25519"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	c "github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/testharness"
)

type queryFiles struct {
	testFileExchange
	calls int
}

func (f *queryFiles) Offer(q LaunchQuery, peer c.Peer, _ c.Deadline, _ c.Stop) (c.HostRequest, error) {
	f.calls++
	r := requestFixture(c.LaunchClaim)
	r.Expected, r.Nonce, r.KeyID, r.Domain, r.PolicyRevision, r.HostBootID, r.RemainingNS = q.Expected, q.Nonce, q.Identity.KeyID, q.Identity.Domain, q.Identity.PolicyRevision, q.Identity.HostBootID, q.RemainingNS
	r.Component = q.Component
	return r, nil
}

// origin: S9 internal review rv-infra F03
func TestLaunchQueryUsesAuthenticatedManagementEndpoint(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLaunchQuery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "launcher authenticated query cancellation", beforeG)
	testharness.AssertNoFDLeak(t, "launcher shared endpoint custody", beforeFD, 0)
}
func exerciseLaunchQuery(t *testing.T) {
	t.Helper()
	for _, scenario := range []string{"valid", "signature", "domain", "epoch", "key", "scope", "peer", "server-uid"} {
		t.Run(scenario, func(t *testing.T) {
			server, _, limit, host := ipcFixture(t)
			defer closeWorkerLimit(t, limit)
			encoded, err := generateCredential()
			if err != nil {
				t.Fatal(err)
			}
			defer clear(encoded)
			key, err := decodeCredential(encoded)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestResource(t, key)
			for n := range server.policy.current.Keys {
				if server.policy.current.Keys[n].Scope == LaunchAuthority {
					server.policy.current.Keys[n].PublicKey = ed25519.PublicKey(key.PublicKey())
				}
			}
			policy := server.policy.current
			stat, err := os.ReadFile("/proc/self/stat")
			if err != nil {
				t.Fatal(err)
			}
			ticks, err := parseStartTicks(stat)
			if err != nil {
				t.Fatal(err)
			}
			q := LaunchQuery{Identity: LaunchIdentity{1, policy.Domain, policy.Revision, "launch", policy.HostBootID, policy.ServiceGID}, Component: c.NATS, Expected: c.Process{BootID: policy.HostBootID, PID: int32(os.Getpid()), StartTicks: ticks, InvocationID: "query-fixture", Cgroup: "/query-fixture", ExeDevice: 1, ExeInode: 2, ExeSHA256: c.Digest{3}}, Nonce: c.ID{4}, RemainingNS: uint64(time.Second)}
			switch scenario {
			case "domain":
				q.Identity.Domain[0]++
			case "epoch":
				q.Identity.PolicyRevision++
			case "key":
				q.Identity.KeyID = "local"
			case "peer":
				q.Expected.StartTicks++
			case "scope":
				for n := range server.policy.current.Keys {
					if server.policy.current.Keys[n].Scope == LaunchAuthority {
						server.policy.current.Keys[n].Scope = LocalAuthority
					}
				}
			}
			q, err = SignLaunchQuery(q, key)
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "signature" {
				q.Signature[0]++
			}
			files := &queryFiles{}
			server.files = files
			dir, err := os.MkdirTemp("", "launch-query-")
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := os.RemoveAll(dir); err != nil {
					t.Error(err)
				}
			}()
			listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(dir, "control.sock"), Net: "unix"})
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestResource(t, listener)
			if err := os.Chmod(filepath.Join(dir, "control.sock"), 0660); err != nil {
				t.Fatal(err)
			}
			pinned, err := os.Open(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer closeTestResource(t, pinned)
			client, err := NewPinnedClient(pinned, uint32(os.Geteuid()), uint32(os.Getegid()))
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "server-uid" {
				client.serverUID++
			}
			d := testDeadline(t, time.Second)
			done := make(chan error, 1)
			go func() {
				conn, err := listener.AcceptUnix()
				if err != nil {
					done <- err
					return
				}
				done <- server.ServeConn(conn, d, nil)
			}()
			offer, err := client.QueryLaunch(q, d, nil)
			serverErr := awaitServe(t, done)
			if scenario == "valid" {
				if err != nil || serverErr != nil || offer.Op != c.LaunchClaim || files.calls != 1 {
					t.Fatalf("authenticated launcher query failed: client=%v server=%v offers=%d", err, serverErr, files.calls)
				}
			} else if err == nil || files.calls != 0 {
				t.Fatalf("unauthorized query reached offer: %s err=%v offers=%d", scenario, err, files.calls)
			}
			if host.calls() != 0 {
				t.Fatal("read-only query dispatched a host effect")
			}
		})
	}
}

// closeTestResource keeps cleanup failures visible while allowing the remaining owners to close.
func closeTestResource(t *testing.T, resource io.Closer) {
	t.Helper()
	if err := resource.Close(); err != nil {
		t.Error(err)
	}
}
