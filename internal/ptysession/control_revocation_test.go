//go:build linux

package ptysession

import (
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/workload"
)

// Controlled actor state with real replay and Manager owners. A missing
// Manager row is an error for outstanding cleanup, never proof of completion.
// origin: hot-update S4 internal review rv-auth RV-AUTH-1
func TestPTYRevocationRetainsUnfinishedCleanup(t *testing.T) {
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, arm := range []string{"live-closed-master", "terminal-open-master", "terminal-pending", "terminal-captured-members"} {
			t.Run(arm, func(t *testing.T) {
				now := int64(1)
				a := tableActor(t, &now)
				a.phase = phaseFrozen
				a.state.MasterClosed = arm != "terminal-open-master"
				a.state.CleanupPending = arm == "terminal-pending" || arm == "terminal-captured-members"
				if arm == "terminal-captured-members" {
					a.state.CleanupMembers = []workload.SessionMember{{Identity: workload.Identity{OSPID: 123}}}
				}
				members := append([]workload.SessionMember(nil), a.state.CleanupMembers...)
				var err error
				if arm == "live-closed-master" {
					err = a.Revoke("session_destroyed")
				} else {
					err = RevokeTerminal(a, "session_destroyed")
				}
				if err != nil || a.state.Revoked != "session_destroyed" || !a.state.CleanupPending || !reflect.DeepEqual(a.state.CleanupMembers, members) {
					t.Fatalf("revocation lost unfinished responsibility: %+v %v", a.state, err)
				}
				if err := a.cleanup(); !errors.Is(err, workload.ErrUnknown) {
					t.Fatalf("missing owner was swallowed for unfinished cleanup: %v", err)
				}
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "unfinished revocation", g0)
	testharness.AssertNoFDLeak(t, "unfinished revocation", fd0, 0)
}
