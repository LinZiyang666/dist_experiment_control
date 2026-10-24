//go:build linux

package agentupgradeport

import (
	"errors"
	"os"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review activation-and-route-migration F1
func TestAgentActivationPreservesExecFailure(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAgentExecFailure(t)
	}
	testharness.AssertNoGoroutineLeak(t, "agent returned exec x5", g)
	testharness.AssertNoFDLeak(t, "agent returned exec x5", fd, 0)
}

func exerciseAgentExecFailure(t *testing.T) {
	t.Helper()
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"}
	handle := upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 3}
	owner := upgradetx.Owner{BootID: "11111111-2222-3333-4444-555555555555", PID: os.Getpid(), StartTicks: 1, InstanceID: strings.Repeat("3", 32)}
	scope := upgradetx.Scope{ID: strings.Repeat("4", 32), Participants: []upgradetx.Key{key}, Domains: append([]upgradetx.Domain(nil), registryDomains[:]...), Expected: []upgradetx.Resource{}}
	for _, cause := range []error{syscall.EACCES, syscall.ENOEXEC, nil} {
		lease, err := os.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		var events []string
		port := NewRuntime(Host{Key: key, Incarnation: func() string { return strings.Repeat("7", 64) }, Exec: func(string, *handoff.Prepared) error { return cause }}).(*runtimePort)
		port.ready[handle.TxID] = preflighted{target: "/candidate", sourceSHA: strings.Repeat("a", 64), candidate: agentimage.Capabilities{SHA256: strings.Repeat("b", 64)}}
		port.held = &heldCut{handle: handle, image: noticeImage{events: &events}, lease: fileLease{lease}, sequence: 1, timer: time.AfterFunc(time.Hour, func() {})}
		_, err = port.Activate(upgradeport.Cut{Attempt: handle, Owner: owner, Scope: scope, Sequence: 1})
		var typed *upgradetx.Error
		if !errors.As(err, &typed) || typed.Code != upgradetx.CodeUnsettled || !typed.Accepted {
			t.Fatalf("returned exec was certified: %v", err)
		}
		if cause != nil && !errors.Is(err, cause) {
			t.Fatalf("activation discarded initiating exec error %v: %v", cause, err)
		}
		if port.held != nil || port.Activating() || strings.Join(events, ",") != "prepare,thaw" {
			t.Fatalf("returned exec retained its frozen owners: %v", events)
		}
	}
}
