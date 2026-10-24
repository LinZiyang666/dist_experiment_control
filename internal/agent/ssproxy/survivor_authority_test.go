//go:build linux

package ssproxy

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 internal review SS-REVOKE-2
func TestSSConflictingCurrentKeysetHoldsSurvivorUntilFreshGrant(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSConflictingCurrentKeyset(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS conflicting current keyset", g0)
	testharness.AssertNoFDLeak(t, "SS conflicting current keyset", fd0, 0)
}

func exerciseSSConflictingCurrentKeyset(t *testing.T) {
	t.Helper()
	target := newSSTarget(t)
	defer target.close()
	s := New(nil)
	defer s.Stop()
	bob := Key{KeyID: "bob", Secret: "conflict-survivor-material"}
	port, err := s.Start(0, []Key{bob})
	if err != nil {
		t.Fatal(err)
	}
	current := Grant{Enabled: true, Generation: 10, Epoch: 20, AppliedRevision: 100, Keys: []Key{bob}}
	if err := s.Authorize(current); err != nil {
		t.Fatal(err)
	}
	peer := dialSS(t, net.JoinHostPort("127.0.0.1", itoa(port)), bob.Secret, target.listener.Addr().String())
	defer func() {
		if err := peer.conn.Close(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	peer.send(t, []byte("duplex"))
	if !bytes.Equal(peer.recv(t, 6), []byte("duplex")) {
		t.Fatal("duplex prerequisite failed")
	}
	conflict := current
	conflict.AppliedRevision--
	conflict.Keys = append([]Key{bob}, Key{KeyID: "victim", Secret: "withdrawn-private-material"})
	if err := s.Authorize(conflict); !errors.Is(err, errAuthorityConflict) {
		t.Fatal("conflicting current pair was accepted", err)
	}
	s.mu.Lock()
	phase := s.imageState.authority
	unchanged := s.imageState.revision == current.AppliedRevision && len(s.keys) == 1
	s.mu.Unlock()
	if phase != AuthorityPhasePending || !unchanged {
		t.Fatal("conflicting current keyset bypassed the authority hold or changed applied keys")
	}
	marker := []byte("pending-until-fresh-grant")
	peer.send(t, marker)
	_ = peer.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var b [1]byte
	if n, err := peer.conn.Read(b[:]); n != 0 || !timeoutError(err) {
		t.Fatal("conflicting keyset allowed target plaintext before a fresh grant")
	}
	if target.bytes.Load() != 6 || target.accepts.Load() != 1 {
		t.Fatal("authority hold wrote plaintext or redialed the original target")
	}
	if err := s.Authorize(current); err != nil {
		t.Fatal(err)
	}
	_ = peer.conn.SetReadDeadline(time.Now().Add(time.Second))
	if !bytes.Equal(peer.recv(t, len(marker)), marker) {
		t.Fatal("fresh grant lost pending survivor bytes")
	}
}

// origin: hot-update S5 internal review rv-ss-2
func TestSSMaterialQuotaFreshGrantKeepsSurvivorForwarding(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSQuotaSurvivor(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS quota survivor", g0)
	testharness.AssertNoFDLeak(t, "SS quota survivor", fd0, 0)
}
func exerciseSSQuotaSurvivor(t *testing.T) {
	t.Helper()
	target := newSSTarget(t)
	defer target.close()
	s := New(nil)
	defer s.Stop()
	for i := 0; i < MaxMaterialHistory-1; i++ {
		if err := s.SetKeys([]Key{{KeyID: "history", Secret: fmt.Sprintf("retained-material-%d", i)}}); err != nil {
			t.Fatal(err)
		}
	}
	bob := Key{KeyID: "bob", Secret: "bob-fixed-material"}
	if err := s.SetFlowRevoke(func([16]byte, string) {}); err != nil {
		t.Fatal(err)
	}
	port, err := s.Start(0, []Key{bob})
	if err != nil {
		t.Fatal(err)
	}
	grant := Grant{Enabled: true, Generation: 7, Epoch: 20, AppliedRevision: 100, Keys: []Key{bob}}
	if err := s.Authorize(grant); err != nil {
		t.Fatal(err)
	}
	peer := newReferencePeer(t, s, port, bob.Secret, target.listener.Addr().(*net.TCPAddr).AddrPort())
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.conn.Close)
	peer.echo(t, []byte("before-quota-cut"))
	f, err := s.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Thaw(); err != nil {
		t.Fatal(err)
	}
	grant.Epoch++
	grant.AppliedRevision++
	grant.Keys = append(grant.Keys, Key{KeyID: "new", Secret: "material-number-65"})
	if err := s.Authorize(grant); !errors.Is(err, ErrCapacity) {
		t.Fatalf("capacity feedback required: %v", err)
	}
	s.mu.Lock()
	if len(s.imageState.materials) != MaxMaterialHistory || !s.imageState.authFailed || s.imageState.revision != 100 || s.imageState.epoch != 20 {
		s.mu.Unlock()
		t.Fatal("failed material admission changed history, admission guard or applied watermark")
	}
	for _, a := range s.imageState.actors {
		if a.material == nil || s.imageState.materials[a.material.fingerprint] != a.material {
			s.mu.Unlock()
			t.Fatal("live material evicted")
		}
	}
	s.mu.Unlock()
	peer.echo(t, []byte("after-quota-cut"))
	if target.accepts.Load() != 1 {
		t.Fatal("survivor upstream was redialed")
	}
}

// origin: hot-update S5 internal review rv-ss-5 / rv-ss-V1
func TestSSOlderAuthorityCannotSuspendCurrentSurvivor(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSStaleAuthority(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS stale authority", g0)
	testharness.AssertNoFDLeak(t, "SS stale authority", fd0, 0)
}
func exerciseSSStaleAuthority(t *testing.T) {
	t.Helper()
	target := newSSTarget(t)
	defer target.close()
	s := New(nil)
	defer s.Stop()
	bob := Key{KeyID: "bob", Secret: "current-bob-private-material"}
	if err := s.SetFlowRevoke(func([16]byte, string) {}); err != nil {
		t.Fatal(err)
	}
	port, err := s.Start(0, []Key{bob})
	if err != nil {
		t.Fatal(err)
	}
	current := Grant{Enabled: true, Generation: 10, Epoch: 20, AppliedRevision: 100, Keys: []Key{bob}}
	if err := s.Authorize(current); err != nil {
		t.Fatal(err)
	}
	peer := newReferencePeer(t, s, port, bob.Secret, target.listener.Addr().(*net.TCPAddr).AddrPort())
	defer func(closeFunc func() error) { _ = closeFunc() }(peer.conn.Close)
	peer.echo(t, []byte("before-stale-authority"))
	for _, kind := range []string{"revision", "epoch", "generation", "disabled", "oversized"} {
		t.Run(kind, func(t *testing.T) {
			stale := current
			switch kind {
			case "revision":
				stale.AppliedRevision--
			case "epoch":
				stale.Epoch--
				stale.AppliedRevision++
			case "generation":
				stale.Generation--
				stale.Epoch++
				stale.AppliedRevision++
			case "disabled":
				stale.AppliedRevision--
				stale.Enabled = false
			case "oversized":
				stale.AppliedRevision--
				stale.Keys = nil
				for i := 0; i <= MaxKeys; i++ {
					stale.Keys = append(stale.Keys, Key{KeyID: fmt.Sprintf("obsolete-%d", i), Secret: "obsolete-material"})
				}
			}
			if err := s.Authorize(stale); !errors.Is(err, errAuthorityConflict) {
				t.Fatalf("stale grant was not rejected before side effects: %v", err)
			}
			peer.echo(t, []byte("after-stale-"+kind))
			s.mu.Lock()
			defer s.mu.Unlock()
			st := s.imageState
			if st.authority != AuthorityPhaseAuthorized || st.authFailed || st.generation != current.Generation || st.epoch != current.Epoch || st.revision != current.AppliedRevision || len(s.keys) != 1 {
				t.Fatal("rejected grant mutated current authority")
			}
		})
	}
	if target.accepts.Load() != 1 {
		t.Fatal("stale grant changed original upstream")
	}
}

// origin: hot-update S5 internal review rv-authority-V1
func TestFreshGenerationAllowsRestoredLowerEpoch(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSGenerationRecovery(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS generation recovery", g0)
	testharness.AssertNoFDLeak(t, "SS generation recovery", fd0, 0)
}
func exerciseSSGenerationRecovery(t *testing.T) {
	t.Helper()
	keys := []Key{{KeyID: "bob", Secret: "generation-recovery-material"}}
	s := New(nil)
	defer s.Stop()
	if err := s.SetKeys(keys); err != nil {
		t.Fatal(err)
	}
	if err := s.Authorize(Grant{Enabled: true, Generation: 7, Epoch: 20, AppliedRevision: 100, Keys: keys}); err != nil {
		t.Fatal(err)
	}
	f, err := s.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func(closeFunc func() error) { _ = closeFunc() }(f.Discard)
	cut, err := f.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	next, err := restorePrivateRecords(cut.Records)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Stop()
	if err := next.Authorize(Grant{Enabled: true, Generation: 8, Epoch: 1, AppliedRevision: 0, Keys: keys}); err != nil {
		t.Fatalf("fresh broker generation rejected restored lower epoch/revision: %v", err)
	}
	if next.imageState.generation != 8 || next.imageState.epoch != 1 || next.imageState.authority != AuthorityPhaseAuthorized {
		t.Fatal("fresh generation did not replace imported authority")
	}
	if err := next.Authorize(Grant{Enabled: true, Generation: 8, Epoch: 0, AppliedRevision: 101, Keys: keys}); !errors.Is(err, errAuthorityConflict) {
		t.Fatal("lower epoch within recovered generation accepted")
	}
}
