//go:build linux && hotupdate_integration

package hotupdate

import (
	"bytes"
	"io"
	"net"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/rawsocket"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 plan §6.5 / §8 T43
// Alice becomes silent after real duplex traffic. Only the product can close
// her sockets; the fixture keeps both application peers open through cleanup.
func TestSSRevokedDuringCutNeverWritesAfterGrant(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, window := range []string{"pre-freeze", "frozen", "final-prepare", "raw-active"} {
			t.Run(window, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				exerciseSSCutRevoke(t, window)
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "ss-cut-revoke", beforeG)
	testharness.AssertNoFDLeak(t, "ss-cut-revoke", beforeFD, 0)
}

func exerciseSSCutRevoke(t *testing.T, window string) {
	t.Helper()
	aliceTarget, bobTarget := ssNewTarget(t), ssNewTarget(t)
	server := ssproxy.New(nil)
	t.Cleanup(server.Stop)
	keys := []ssproxy.Key{{KeyID: "alice", Secret: ssVictimPassword}, {KeyID: "bob", Secret: ssProbePassword}}
	port, err := server.Start(0, keys)
	if err != nil {
		t.Fatal(err)
	}
	aliceID, bobID := [16]byte{1}, [16]byte{2}
	var mu sync.Mutex
	callbacks := make(map[[16]byte]int)
	revoked := make(chan struct{}, 4)
	callback := func(id [16]byte, _ string) {
		mu.Lock()
		callbacks[id]++
		mu.Unlock()
		select {
		case revoked <- struct{}{}:
		default:
		}
	}
	if err := server.SetFlowRevoke(callback); err != nil {
		t.Fatalf("SS flow revoke dependency: %v", err)
	}
	alice := ssReferenceDial(t, port, ssVictimPassword, aliceTarget.ln.Addr().String(), func(tuple rawsocket.Tuple) error { return server.ExpectFlow(aliceID, tuple) })
	bob := ssReferenceDial(t, port, ssProbePassword, bobTarget.ln.Addr().String(), func(tuple rawsocket.Tuple) error { return server.ExpectFlow(bobID, tuple) })
	aliceConn, bobConn := aliceTarget.original(t), bobTarget.original(t)
	for sample := 0; sample < 20; sample++ {
		ssDuplexSample(t, alice, aliceConn, sample)
		ssDuplexSample(t, bob, bobConn, sample)
	}
	bobSocket := ssReferenceFingerprint(t, bob.socket)
	removeAlice := func() {
		t.Helper()
		if err := server.SetKeys(keys[1:]); err != nil {
			t.Fatal(err)
		}
	}
	if window == "pre-freeze" {
		removeAlice()
	}
	frozen, err := server.Freeze(time.Now().Add(ssProbeBudget))
	if err != nil {
		t.Fatalf("SS freeze dependency: %v", err)
	}
	if window == "frozen" {
		removeAlice()
	}
	image, snapshot, err := frozen.Export()
	if err != nil {
		t.Fatalf("SS image dependency: %v", err)
	}
	t.Cleanup(func() { _ = snapshot.Close() })
	// Keep a Prepared duplicate alive while revoking and exporting again.
	// Close-only cleanup would leave this OFD capable of forwarding victim
	// bytes; SHUT_RDWR must invalidate all references to the original socket.
	prepared, err := handoff.Prepare(handoff.State{Role: "agent", Instance: "ss-revoke", Generation: 2}, snapshot.Resources)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prepared.Close() })
	if window == "final-prepare" {
		removeAlice()
		_, replacement, err := frozen.Export()
		if err != nil {
			t.Fatal(err)
		}
		if err := snapshot.Close(); err != nil {
			t.Fatal(err)
		}
		snapshot = replacement
		// An actual exec error leaves the old owner alive. Thaw must restore
		// raw IO in AuthorityPending, even for an otherwise valid survivor;
		// reusing its pre-cut keyset would authorize writes without RESUME.
		if err := prepared.Exec(filepath.Join(t.TempDir(), "missing-image"), []string{"ss-probe"}, ssProbeEnvironment("", "")); err == nil {
			t.Fatal("missing SS exec image unexpectedly succeeded")
		}
		if err := frozen.Thaw(); err != nil {
			t.Fatal(err)
		}
		marker := []byte("old-owner-must-await-current-authority")
		bob.send(t, marker)
		ssAssertNoTargetWriteBeforeGrant(t, bobConn)
		if err := server.Authorize(ssproxy.Grant{Enabled: true, Generation: 2, Epoch: 1, AppliedRevision: 42, Keys: keys[1:]}); err != nil {
			t.Fatal(err)
		}
		_ = bobConn.SetReadDeadline(time.Now().Add(ssProbeBudget))
		got := make([]byte, len(marker))
		if _, err := io.ReadFull(bobConn, got); err != nil || !bytes.Equal(got, marker) {
			t.Fatal("old owner failed to resume its bounded pending bytes after fresh authority")
		}
		if err := snapshot.Close(); err != nil {
			t.Fatal(err)
		}
		frozen, err = server.Freeze(time.Now().Add(ssProbeBudget))
		if err != nil {
			t.Fatal(err)
		}
		image, snapshot, err = frozen.Export()
		if err != nil {
			t.Fatal(err)
		}
	}
	if window != "raw-active" {
		state, err := frozen.SnapshotState()
		if err != nil {
			t.Fatal(err)
		}
		mu.Lock()
		terminalSubmitted := callbacks[aliceID] > 0
		mu.Unlock()
		ssAssertVictimRevoked(t, state, aliceID, terminalSubmitted)
	}
	source := handofftest.DupResources(t, snapshot.Resources)
	imported, _, err := ssproxy.Restore(image, source)
	if err != nil {
		t.Fatalf("SS restore dependency: %v", err)
	}
	t.Cleanup(imported.Stop)
	if err := imported.SetFlowRevoke(callback); err != nil {
		t.Fatal(err)
	}
	if err := frozen.Discard(); err != nil {
		t.Fatal(err)
	}
	if err := imported.ActivateRaw(); err != nil {
		t.Fatal(err)
	}
	// Queued authorized-era ciphertext is an adversarial witness. Raw
	// activation may restore IO ownership but must not authorize plaintext.
	if window == "raw-active" {
		alice.send(t, []byte("unauthorized-victim-marker"))
		ssAssertNoTargetWriteBeforeGrant(t, aliceConn)
	}
	fresh := ssproxy.Grant{Enabled: true, Generation: 3, Epoch: 1, AppliedRevision: 43, Keys: keys[1:]}
	if err := imported.Authorize(fresh); err != nil {
		t.Fatal(err)
	}
	// A stale successful grant must not turn an irreversible victim revoke
	// into current authorization, including on an exec-failure old owner.
	stale := fresh
	stale.AppliedRevision = 42
	stale.Keys = keys
	if err := imported.Authorize(stale); err == nil {
		t.Fatal("stale authority revision reauthorized old SS material")
	}
	// A conflicting snapshot puts the whole server in AuthorityPending. The
	// survivor needs a fresh decision too; expecting traffic under the rejected
	// grant would contradict the fail-closed authority contract.
	// origin: hot-update S5 plan §6.4 / §6.5 / T43
	marker := []byte("survivor-awaits-fresh-authority-after-conflict")
	bob.send(t, marker)
	ssAssertNoTargetWriteBeforeGrant(t, bobConn)
	if err := imported.Authorize(fresh); err != nil {
		t.Fatal(err)
	}
	_ = bobConn.SetReadDeadline(time.Now().Add(ssProbeBudget))
	got := make([]byte, len(marker))
	if _, err := io.ReadFull(bobConn, got); err != nil || !bytes.Equal(got, marker) {
		t.Fatal("survivor failed to resume pending bytes after fresh authority")
	}
	ssAssertSilentTargetClosed(t, aliceConn)
	deadline := time.NewTimer(ssProbeBudget)
	defer deadline.Stop()
	for {
		mu.Lock()
		count, survivor := callbacks[aliceID], callbacks[bobID]
		mu.Unlock()
		if survivor != 0 {
			t.Fatal("bob survivor flow was revoked")
		}
		if count > 0 {
			break
		}
		select {
		case <-revoked:
		case <-deadline.C:
			t.Fatal("silent victim never submitted its stable FlowID revoke")
		}
	}
	// The callback names the original flow, not a tuple guessed after exec.
	mu.Lock()
	for id := range callbacks {
		if id != aliceID {
			mu.Unlock()
			t.Fatal("SS revoke callback changed the victim FlowID")
		}
	}
	mu.Unlock()
	for sample := 20; sample < 40; sample++ {
		ssDuplexSample(t, bob, bobConn, sample)
	}
	if ssReferenceFingerprint(t, bob.socket) != bobSocket {
		t.Fatal("bob application socket replaced across victim revoke")
	}
	aliceTarget.assertOneAccept(t)
	bobTarget.assertOneAccept(t)
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		t.Fatal(err)
	}
	imported.Stop()
	source.Close()
	server.Stop()
	// Check while the victim client is still silent and both targets remain
	// live. Fixture teardown or process exit cannot stand in for shutdown.
	_ = bobConn.SetReadDeadline(time.Now().Add(ssProbeBudget))
	var byteAfterStop [1]byte
	if n, err := bobConn.Read(byteAfterStop[:]); n != 0 || err == nil {
		t.Fatal("SS Stop did not close the original survivor upstream")
	}
}

func ssAssertVictimRevoked(t *testing.T, state sealedstate.CutState, id [16]byte, terminalSubmitted bool) {
	t.Helper()
	for _, record := range state.Records {
		if record.Kind != sealedstate.KindSSActor {
			continue
		}
		var flowID []byte
		var revoked uint64
		for _, field := range record.Fields {
			if field.Name == "flow_id" {
				flowID = field.Bytes
			}
			if field.Name == "revoked" {
				revoked = field.U64
			}
		}
		if bytes.Equal(flowID, id[:]) {
			if revoked != 1 {
				t.Fatal("final SS cut lost irreversible victim Revoked")
			}
			return
		}
	}
	// A fully retired actor may be absent only when its bound flow has
	// already received the terminal revoke, checked independently above.
	if !terminalSubmitted {
		t.Fatal("SS cut lost victim actor before submitting its terminal FlowID revoke")
	}
}

func ssAssertNoTargetWriteBeforeGrant(t *testing.T, target net.Conn) {
	t.Helper()
	_ = target.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var data [1]byte
	n, err := target.Read(data[:])
	if n != 0 {
		t.Fatal("SS raw-active owner wrote victim plaintext before fresh grant")
	}
	if timeout, ok := err.(net.Error); !ok || !timeout.Timeout() {
		t.Fatal("raw activation closed victim before the fresh authorization decision")
	}
}

func ssAssertSilentTargetClosed(t *testing.T, target net.Conn) {
	t.Helper()
	_ = target.SetReadDeadline(time.Now().Add(ssProbeBudget))
	var data [1]byte
	n, err := target.Read(data[:])
	if n != 0 {
		t.Fatal("revoked SS victim wrote target bytes after fresh grant")
	}
	if err == nil {
		t.Fatal("silent revoked SS target stayed open")
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		t.Fatal("silent victim cleanup required fixture Read/Close")
	}
	if err != io.EOF {
		if _, ok := err.(*net.OpError); !ok {
			t.Fatal("unexpected silent victim terminal error")
		}
	}
}
