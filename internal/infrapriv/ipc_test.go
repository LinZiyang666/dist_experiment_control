//go:build linux && amd64

// Real Unix sockets, SO_PEERCRED/pidfd, shared flocks and cancellation workers.
// The Host records effects; no systemd/nft/process mutation is substituted here.
// Real root/multiple UIDs and process-parent death supervision belong to P31/P27.
package infrapriv

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/controlbudget"
	c "github.com/LinZiyang666/tether/internal/infracontract"
	"golang.org/x/sys/unix"
)

// origin: hot-update S9 plan §6.1 / T08
func TestInfraPartialRequestDeadline(t *testing.T) {
	if IPCDescriptorForRuntime() != (IPCDescriptor{1, "THQ1", "THA1", "THF1", 8, 65536, 4, 2000000000, 10000000000, 15000000000}) {
		t.Fatal("frozen IPC framing/resource limits changed")
	}
	stat := []byte("12 (comm with ) spaces) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20")
	if ticks, err := parseStartTicks(stat); err != nil || ticks != 19 {
		t.Fatalf("kernel start ticks parsed at wrong field: %d %v", ticks, err)
	}
	for _, malformed := range [][]byte{[]byte("no delimiter"), []byte("12 (comm) S 1"), bytes.Replace(stat, []byte("18 19 20"), []byte("18 0 20"), 1), bytes.Replace(stat, []byte("18 19 20"), []byte("18 -1 20"), 1)} {
		if _, err := parseStartTicks(malformed); err == nil {
			t.Fatal("unparseable/unknown incarnation accepted")
		}
	}
	for _, scenario := range []string{"empty", "partial-header", "partial-body", "missing-half-close"} {
		t.Run(scenario, func(t *testing.T) {
			server, request, limit, host := ipcFixture(t)
			defer closeWorkerLimit(t, limit)
			clientConn, serverConn := unixPair(t)
			defer func() { _ = clientConn.Close() }()
			peer, err := PeerCredentials(serverConn)
			if err != nil {
				t.Fatal(err)
			}
			if peer.PID != int32(os.Getpid()) || peer.UID != uint32(os.Geteuid()) || peer.GID != uint32(os.Getegid()) || peer.StartTicks == 0 {
				t.Fatalf("peer not kernel-derived: %+v", peer)
			}
			data, err := c.EncodeRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			frame := testFrame(RequestPurpose, data)
			payload := []byte(nil)
			switch scenario {
			case "partial-header":
				payload = frame[:3]
			case "partial-body":
				payload = frame[:len(frame)-1]
			case "missing-half-close":
				payload = frame
			}
			started := time.Now()
			result := serveAsync(server, serverConn, testDeadline(t, 5*time.Second), nil)
			if len(payload) > 0 {
				if _, err := clientConn.Write(payload); err != nil {
					t.Fatal(err)
				}
			}
			if err := awaitServe(t, result); err == nil {
				t.Fatal("partial request accepted")
			}
			elapsed := time.Since(started)
			if elapsed < ReadBudget-200*time.Millisecond || elapsed > ReadBudget+1500*time.Millisecond {
				t.Fatalf("whole read exceeded fixed two-second budget: %v", elapsed)
			}
			if host.calls() != 0 {
				t.Fatal("partial request reached an effect")
			}
			// The slot must be reusable immediately after refusal, not only after
			// test cleanup/process exit. Exhaust all four OFD slots to prove it.
			exerciseWorkerSlots(t, limit)
		})
	}
	t.Run("slow-drip-does-not-renew", func(t *testing.T) {
		server, request, limit, host := ipcFixture(t)
		defer closeWorkerLimit(t, limit)
		client, accepted := unixPair(t)
		defer func() { _ = client.Close() }()
		data, err := c.EncodeRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		frame := testFrame(RequestPurpose, data)
		started := time.Now()
		result := serveAsync(server, accepted, testDeadline(t, 5*time.Second), nil)
		if _, err := client.Write(frame[:3]); err != nil {
			t.Fatalf("initial request write: %v; server: %v", err, awaitServe(t, result))
		}
		// sleep-fixture: adversarial drip consumes the original read budget; this
		// timer is the injected slow sender, not a readiness polling barrier.
		drip := time.NewTimer(3 * ReadBudget / 4)
		<-drip.C
		if _, err := client.Write(frame[3:9]); err != nil {
			t.Fatal(err)
		}
		if err := awaitServe(t, result); err == nil {
			t.Fatal("slow partial request reached effect")
		}
		if elapsed := time.Since(started); elapsed > ReadBudget+time.Second {
			t.Fatalf("drip renewed read deadline: %v", elapsed)
		}
		if host.calls() != 0 {
			t.Fatal("drip reached host")
		}
	})
	t.Run("read-interval-deducted-from-signed-budget", func(t *testing.T) {
		server, request, limit, host := ipcFixtureWithBudget(t, uint64(100*time.Millisecond))
		defer closeWorkerLimit(t, limit)
		// A delayed sender spends the signed budget while the separate two-second
		// read window remains open. Resetting RemainingNS would reach an effect.
		client, accepted := unixPair(t)
		defer func() { _ = client.Close() }()
		result := serveAsync(server, accepted, testDeadline(t, time.Second), nil)
		data, err := c.EncodeRequest(request)
		if err != nil {
			t.Fatal(err)
		}
		frame := testFrame(RequestPurpose, data)
		if _, err := client.Write(frame[:3]); err != nil {
			t.Fatalf("initial budget request write: %v; server: %v", err, awaitServe(t, result))
		}
		// sleep-fixture: inject transit beyond the signed RemainingNS.
		transit := time.NewTimer(150 * time.Millisecond)
		<-transit.C
		if _, err := client.Write(frame[3:]); err != nil {
			t.Fatal(err)
		}
		_ = client.CloseWrite()
		if err := awaitServe(t, result); !errors.Is(err, controlbudget.ErrExpired) {
			t.Fatalf("budget renewed at last byte: %v", err)
		}
		if host.calls() != 0 {
			t.Fatal("expired request reached effect")
		}
	})
	t.Run("malformed-frames-and-peer-authorization", func(t *testing.T) { exerciseRejectedIPC(t) })
	t.Run("client-checks-root-and-exact-receipt", func(t *testing.T) { exerciseIPCClient(t) })
	t.Run("approved-descriptors-preserve-the-lock-OFD", func(t *testing.T) { exerciseFileExchange(t) })
	t.Run("refusal-is-canonical-and-before-effect-only", func(t *testing.T) {
		refusal := Refusal{1, c.ID{1}, 1, c.Digest{2}, "infra_permission_denied"}
		data, err := encodeRefusal(refusal)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := decodeRefusal(data)
		if err != nil || decoded != refusal {
			t.Fatal("refusal roundtrip failed")
		}
		for _, bad := range [][]byte{append(bytes.Clone(data), '\n'), bytes.Replace(data, []byte(`"Version":1`), []byte(`"Version":1,"Version":1`), 1),
			bytes.Replace(data, []byte(`"Code"`), []byte(`"code"`), 1), bytes.Replace(data, []byte("infra_permission_denied"), []byte("infra_result_unknown"), 1),
			bytes.Replace(data, []byte(`"Version":1`), []byte(`"Version":2`), 1)} {
			if _, err := decodeRefusal(bad); err == nil {
				t.Fatal("unbound/noncanonical/after-effect refusal accepted")
			}
		}
	})
}

func exerciseWorkerSlots(t *testing.T, first *WorkerLimit) {
	t.Helper()
	// A second worker has a distinct directory handle and distinct OFDs. An
	// in-memory-only cap would incorrectly allow eight across these two workers.
	dup, err := unix.FcntlInt(first.dir.Fd(), unix.F_DUPFD_CLOEXEC, 3)
	if err != nil {
		t.Fatal(err)
	}
	second := &WorkerLimit{dir: os.NewFile(uintptr(dup), "second-worker"), owner: first.owner}
	defer closeWorkerLimit(t, second)
	releases := make([]func(), 0, MaxWorkers)
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for i := 0; i < MaxWorkers; i++ {
		limit := first
		if i%2 == 1 {
			limit = second
		}
		release, err := acquireWorkerSlot(limit)
		if err != nil {
			t.Fatal(err)
		}
		releases = append(releases, release)
	}
	if release, err := acquireWorkerSlot(second); !errors.Is(err, ErrBusy) {
		if release != nil {
			release()
		}
		t.Fatalf("fifth worker admitted across OFDs: %v", err)
	}
	releases[0]()
	releases[0]()
	release, err := acquireWorkerSlot(second)
	if err != nil {
		t.Fatalf("released worker slot not reusable: %v", err)
	}
	release()
}

func exerciseRejectedIPC(t *testing.T) {
	t.Helper()
	for _, scenario := range []string{"oversize", "wrong-purpose", "tail", "second-frame", "noncanonical", "unsigned", "wrong-uid", "wrong-scope", "wrong-boot", "scm-rights"} {
		t.Run(scenario, func(t *testing.T) {
			server, request, limit, host := ipcFixture(t)
			defer closeWorkerLimit(t, limit)
			client, accepted := unixPair(t)
			defer func() { _ = client.Close() }()
			data, err := c.EncodeRequest(request)
			if err != nil {
				t.Fatal(err)
			}
			frame := testFrame(RequestPurpose, data)
			switch scenario {
			case "oversize":
				binary.BigEndian.PutUint32(frame[4:8], c.MaxRequestBytes+1)
				frame = frame[:8]
			case "wrong-purpose":
				copy(frame, ReceiptPurpose)
			case "tail":
				frame = append(frame, 'x')
			case "second-frame":
				frame = append(frame, frame...)
			case "noncanonical":
				frame = testFrame(RequestPurpose, append(data, '\n'))
			case "unsigned":
				request.Signature[0]++
				data, _ = c.EncodeRequest(request)
				frame = testFrame(RequestPurpose, data)
			case "wrong-uid":
				server.policy.mu.Lock()
				server.policy.current.Keys[1].UID++
				server.policy.mu.Unlock()
			case "wrong-scope":
				server.policy.mu.Lock()
				server.policy.current.Keys[1].Scope = LaunchAuthority
				server.policy.mu.Unlock()
			case "wrong-boot":
				server.policy.mu.Lock()
				server.policy.current.HostBootID = "other"
				server.policy.mu.Unlock()
			}
			result := serveAsync(server, accepted, testDeadline(t, time.Second), nil)
			if scenario == "scm-rights" {
				file, err := os.CreateTemp(t.TempDir(), "untrusted-writable")
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = file.Close() }() // never written: only its fd is passed
				if _, _, err := client.WriteMsgUnix(frame, unix.UnixRights(int(file.Fd())), nil); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := client.Write(frame); err != nil {
					t.Fatal(err)
				}
			}
			_ = client.CloseWrite()
			if err := awaitServe(t, result); err == nil {
				t.Fatal("adversarial IPC accepted")
			}
			if host.calls() != 0 {
				t.Fatal("refused IPC reached effect")
			}
		})
	}
}

// origin: hot-update S9 plan §2.1 / T07; handwritten kernel-stat delimiter parser.
func FuzzInfraPeerStartTicks(f *testing.F) {
	f.Add([]byte("12 (name with ) spaces) S 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20"))
	f.Add([]byte("malformed"))
	f.Add(bytes.Repeat([]byte{')'}, 100))
	f.Fuzz(func(t *testing.T, data []byte) {
		ticks, err := parseStartTicks(data)
		if err == nil && ticks == 0 {
			t.Fatal("zero start ticks accepted")
		}
	})
}
