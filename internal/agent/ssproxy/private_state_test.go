package ssproxy

import (
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S5 internal review PRIVATE-STATE-2
func TestSSCutOwnsAuthorityTimerAndMaterialMembership(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseSSPrivateState(t)
	}
	testharness.AssertNoGoroutineLeak(t, "SS private control state", g0)
	testharness.AssertNoFDLeak(t, "SS private control state", fd0, 0)
}

func exerciseSSPrivateState(t *testing.T) {
	t.Helper()
	s := New(nil)
	defer s.Stop()
	if err := s.Authorize(Grant{Enabled: true, Generation: 3, Epoch: 4, AppliedRevision: 5, Keys: []Key{{KeyID: "alias-a", Secret: "private-material"}, {KeyID: "alias-b", Secret: "private-material"}}}); err != nil {
		t.Fatal(err)
	}
	f, err := s.Freeze(time.Now().Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Discard(); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	}()
	cut, err := f.SnapshotState()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, rec := range cut.Records {
		seen[rec.Kind] = true
	}
	if !seen[sealedstate.KindAuthority] || !seen[sealedstate.KindTimer] {
		t.Fatal("runtime cut omitted authority or timer")
	}
	encoded, err := encodeState(cut.Records)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeState(encoded)
	if err != nil {
		t.Fatal(err)
	}
	next, err := restorePrivateRecords(decoded)
	if err != nil {
		t.Fatal(err)
	}
	defer next.Stop()
	expectedControl, imported := RestoredProxyState(next)
	if !imported {
		t.Fatal("control state was not restored")
	}
	if next.imageState.generation != 3 || next.imageState.epoch != 4 || next.imageState.revision != 5 || next.imageState.authority != AuthorityPhasePending {
		t.Fatal("restoration changed authority freshness or enabled forwarding")
	}
	for _, kind := range []string{sealedstate.KindAuthority, sealedstate.KindTimer, sealedstate.KindSSServer, sealedstate.KindMaterial} {
		for _, rec := range decoded {
			if rec.Kind != kind {
				continue
			}
			for _, field := range rec.Fields {
				if kind == sealedstate.KindMaterial && field.Name != "active" && field.Name != "key_ids" {
					continue
				}
				if kind == sealedstate.KindSSServer && field.Name != "timer_ids" && field.Name != "cleanup_pending" && field.Name != "dispatch_sequence" && field.Name != "last_applied_sequence" && field.Name != "material_ids" && field.Name != "conn_ids" && field.Name != "binding_ids" {
					continue
				}
				// Metadata mutations remain structurally valid; the runtime must
				// reject inconsistent ownership and alias membership itself.
				bad, err := decodeState(encoded)
				if err != nil {
					t.Fatal(err)
				}
				for i := range bad {
					if bad[i].Kind != kind {
						continue
					}
					for j := range bad[i].Fields {
						v := &bad[i].Fields[j]
						if v.Name != field.Name {
							continue
						}
						if v.Type <= sealedstate.FieldBool {
							v.U64 ^= 1
						} else if len(v.Bytes) > 0 {
							v.Bytes[0] ^= 1
						} else {
							v.Bytes = []byte{1}
						}
					}
				}
				accepted, err := restorePrivateRecords(bad)
				if accepted != nil {
					if err == nil {
						err = ValidateProxyState(accepted, expectedControl)
					}
					accepted.Stop()
				}
				if err == nil {
					t.Fatalf("restore ignored %s.%s", kind, field.Name)
				}
			}
		}
	}
	// Agreement belongs to constructor time. Once raw ownership is activated,
	// a fresh live grant must be able to advance the next control snapshot.
	// origin: hot-update S5 internal review PRIVATE-STATE-2
	if err := next.ActivateRaw(); err != nil {
		t.Fatal(err)
	}
	expectedControl.Generation++
	if err := next.Authorize(Grant{Enabled: true, Generation: expectedControl.Generation, Epoch: 4, AppliedRevision: 6, Keys: []Key{{KeyID: "alias-a", Secret: "private-material"}, {KeyID: "alias-b", Secret: "private-material"}}}); err != nil {
		t.Fatal(err)
	}
	if err := SetProxyState(next, expectedControl); err != nil {
		t.Fatal("activated control state retained constructor agreement")
	}
}
