package ssproxy

import (
	"bytes"
	"errors"
	"net"
	"os"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// Real Start/relay/freeze/thaw/regrant and reentrant cleanup. No empty callback
// can stand in for the Stop that must join workers after all guards are dropped.
// origin: hot-update S5 plan §5.3 / §6.5 / T22
func TestSSFreezeAndCleanupJoinOutsideLocks(t *testing.T) {
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSFreezeCleanup(t)
		exerciseSSAuthorityExpiry(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS freeze cleanup", beforeG)
	testharness.AssertNoFDLeak(t, "SS freeze cleanup", beforeFD, 0)
}
func exerciseSSFreezeCleanup(t *testing.T) {
	t.Helper()
	target := newSSTarget(t)
	defer target.close()
	keys := []Key{{KeyID: "keep", Secret: "keep"}}
	s := New(nil)
	port, err := s.Start(0, keys)
	if err != nil {
		t.Fatal(err)
	}
	client := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(port)), "keep", target.listener.Addr().String())
	defer func() { _ = client.conn.Close() }()
	client.send(t, []byte("before"))
	_ = client.recv(t, 6)
	f, err := s.Freeze(time.Now().Add(time.Second))
	if err != nil {
		s.Stop()
		t.Fatal(err)
	}
	cut, err := f.SnapshotState()
	if err != nil {
		s.Stop()
		t.Fatal(err)
	}
	if len(cut.Resources) != 2 || len(cut.Resources[1].Files) != 2 {
		s.Stop()
		t.Fatal("cut lacks original listener and both actor sockets")
	}
	// The actor owner validates flags and private socket tuples before import.
	for _, r := range cut.Records {
		p := wrapPrivateRecord(r)
		if r.Kind == "ss-server" {
			if err := validateSocketFile(cut.Resources[0].Files["listener"], p.bytes("listener_tuple"), true, false); err != nil {
				s.Stop()
				t.Fatal(err)
			}
		}
		if r.Kind == "ss-actor" {
			for _, name := range []string{"loopback", "upstream"} {
				file := cut.Resources[1].Files[name]
				if err := validateSocketFile(file, p.bytes(name+"_tuple"), false, false); err != nil {
					s.Stop()
					t.Fatal(err)
				}
				if err := validateSocketFile(file, append([]byte("127.0.0.1:1\n"), bytes.Split(p.bytes(name+"_tuple"), []byte("\n"))[1]...), false, false); err == nil {
					s.Stop()
					t.Fatal("restore accepted a substituted socket tuple")
				}
			}
		}
	}
	if err := f.Thaw(); err != nil {
		s.Stop()
		t.Fatal(err)
	}
	// This fixture starts with netpoll sockets, so SnapshotState lends owned
	// export duplicates. Check those exact files: unrelated runtime descriptors
	// (including cached splice pipes) can retire while the actor is frozen.
	// Total-count equality both flakes on that retirement and can hide a leaked
	// export behind it. The real relay below still proves the originals live.
	// origin: hot-update S5 plan §5.3 / T22
	for _, resource := range cut.Resources {
		for name, file := range resource.Files {
			if _, err := file.Stat(); !errors.Is(err, os.ErrClosed) {
				s.Stop()
				t.Fatalf("Thaw retained %s export descriptor: %v", name, err)
			}
		}
	}
	before := target.bytes.Load()
	client.send(t, []byte("held"))
	_ = client.conn.SetReadDeadline(time.Now().Add(4 * actorPollInterval))
	var b [1]byte
	if _, err := client.conn.Read(b[:]); !timeoutError(err) {
		s.Stop()
		t.Fatal("Thaw resumed old authority")
	}
	if target.bytes.Load() != before {
		s.Stop()
		t.Fatal("target write occurred before fresh grant")
	}
	_ = client.conn.SetReadDeadline(time.Time{})
	if err := s.Authorize(Grant{Enabled: true, Generation: 1, Epoch: 1, Keys: keys}); err != nil {
		s.Stop()
		t.Fatal(err)
	}
	if !bytes.Equal(client.recv(t, 4), []byte("held")) {
		s.Stop()
		t.Fatal("grant did not resume original socket")
	}
	// Hold the actual SS guard to prove lock wait consumes the operation budget.
	s.mu.Lock()
	waited := make(chan error, 1)
	go func() { _, err := s.Freeze(time.Now().Add(3 * actorPollInterval)); waited <- err }()
	select {
	case err := <-waited:
		if err == nil {
			s.mu.Unlock()
			s.Stop()
			t.Fatal("freeze ignored guard deadline")
		}
	case <-time.After(time.Second):
		s.mu.Unlock()
		s.Stop()
		t.Fatal("freeze waited indefinitely for table lock")
	}
	s.mu.Unlock()
	// This callback really stops/joins the server. Invoking it under s.mu or
	// from an actor worker would self-deadlock; both defects are mutated.
	s.mu.Lock()
	for _, a := range s.imageState.actors {
		a.state.putBytes("flow_id", []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	}
	s.mu.Unlock()
	callback := make(chan struct{}, 1)
	if err := s.SetFlowRevoke(func([16]byte, string) { s.Stop(); callback <- struct{}{} }); err != nil {
		s.Stop()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.SetKeys(nil) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup joined under an SS lock or from its own worker")
	}
	select {
	case <-callback:
	default:
		t.Fatal("real Stop callback never ran")
	}
	if s.Serving() {
		t.Fatal("Stop callback left listener serving")
	}
	s.Stop()
}

func exerciseSSAuthorityExpiry(t *testing.T) {
	t.Helper()
	target := newSSTarget(t)
	defer target.close()
	s := New(nil)
	port, err := s.Start(0, []Key{{KeyID: "expires", Secret: "expires"}})
	if err != nil {
		t.Fatal(err)
	}
	client := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(port)), "expires", target.listener.Addr().String())
	defer func() { _ = client.conn.Close() }()
	client.send(t, []byte("before"))
	_ = client.recv(t, 6)
	f, err := s.Freeze(time.Now().Add(time.Second))
	if err != nil {
		s.Stop()
		t.Fatal(err)
	}
	// Capture the original OFDs while frozen. A wrapper's ErrClosed latch may
	// precede its final kernel Close, so it cannot alone settle an FD baseline.
	// origin: hot-update S5 plan §5.3 / §6.5 / T22
	cut, err := f.SnapshotState()
	if err != nil {
		s.Stop()
		t.Fatal(err)
	}
	identities := make(map[rawsocket.Fingerprint]bool)
	for _, row := range cut.Resources {
		for _, file := range row.Files {
			fp, err := fileFingerprint(file)
			if err != nil {
				s.Stop()
				t.Fatal(err)
			}
			identities[fp] = true
		}
	}
	now, err := actorNow()
	if err != nil {
		t.Fatal(err)
	}
	var actor *ssActor
	s.mu.Lock()
	for _, a := range s.imageState.actors {
		actor = a
	}
	actor.state.put("authority_deadline_boot_ns", uint64(now+int64(3*actorPollInterval)))
	actor.state.putBytes("flow_id", []byte{2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
	s.mu.Unlock()
	callback := make(chan struct{}, 1)
	if err := s.SetFlowRevoke(func([16]byte, string) { s.Stop(); callback <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	if err := f.Thaw(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-callback:
	case <-time.After(time.Second):
		s.Stop()
		t.Fatal("absolute authority deadline did not revoke while authorization was pending")
	}
	s.mu.Lock()
	fired, revoked := actor.state.flag("fired"), actor.state.flag("revoked")
	s.mu.Unlock()
	if !fired || !revoked {
		t.Fatal("authority expiry lost irreversible fired/revoked decisions")
	}
	s.Stop()
	// The callback acknowledges flow revocation before its cleanup owner closes
	// the retired socket wrappers. Join that ownership boundary before the next
	// round samples its FD baseline; the fixture must never close them itself.
	awaitSS(t, func() bool {
		if !errors.Is(actor.client.SetReadDeadline(time.Now()), net.ErrClosed) ||
			!errors.Is(actor.upstream.SetReadDeadline(time.Now()), net.ErrClosed) {
			return false
		}
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			t.Fatal(err)
		}
		for _, entry := range entries {
			info, err := os.Stat("/proc/self/fd/" + entry.Name())
			if err != nil {
				continue // Descriptors may retire during enumeration.
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if ok && identities[rawsocket.Fingerprint{Dev: uint64(stat.Dev), Ino: uint64(stat.Ino), Type: uint32(stat.Mode) & syscall.S_IFMT}] {
				return false
			}
		}
		return true
	}, "expiry cleanup did not release the original sockets")
}
