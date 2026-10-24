package agentupgradeport

import (
	"os"
	"runtime"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review remaining-orchestration F2 (R2)
func TestAgentCensusCarriesCurrentIncarnation(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseAgentCensusIncarnation(t)
	}
	testharness.AssertNoGoroutineLeak(t, "agent incarnation census x5", g)
	testharness.AssertNoFDLeak(t, "agent incarnation census x5", fd, 0)
}

func exerciseAgentCensusIncarnation(t *testing.T) {
	t.Helper()
	owner := upgradetx.Owner{BootID: "boot", PID: os.Getpid(), StartTicks: 1, InstanceID: strings.Repeat("1", 32)}
	incarnation := strings.Repeat("a", 64)
	p := NewRuntime(Host{Key: upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "lab"},
		Snapshot: func() (execsession.Image, agentimage.CourierState, error) {
			return execsession.Image{}, agentimage.CourierState{}, nil
		},
		Transfers: noTransfers, Incarnation: func() string { return incarnation },
	}).(*runtimePort)
	for _, frozen := range []bool{false, true} {
		var bundle *agentimage.Bundle
		if frozen {
			bundle = &agentimage.Bundle{}
		}
		rows, err := p.rows(bundle, owner, 3, "target")
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) != 1 || rows[0] != (upgradetx.Resource{Domain: upgradetx.DomainAuthority, ID: incarnation, Origin: "session:lab", Owner: owner, Generation: 3, State: "active", Custody: "target"}) {
			t.Fatalf("census omits the agent creation identity: %+v", rows)
		}
	}
	// A session recreated under the same SID has a distinct census identity.
	incarnation = strings.Repeat("b", 64)
	rows, err := p.rows(nil, owner, 3, "source")
	if err != nil || len(rows) != 1 || rows[0].ID != incarnation {
		t.Fatalf("recreated session retained old authority: %+v %v", rows, err)
	}
	for _, bad := range []string{"", strings.Repeat("A", 64), "unknown"} {
		incarnation = bad
		if _, err := p.rows(nil, owner, 3, "source"); err == nil {
			t.Fatalf("unknown incarnation %q became a complete census", bad)
		}
	}
	p.host.Incarnation = func() string { return strings.Repeat("b", 64) }
	p.host.Inherited = &upgradecut.State{Authorities: []upgradecut.Authority{{SID: "lab", Incarnation: strings.Repeat("a", 64)}}}
	if _, err := p.rows(nil, owner, 3, "target"); err == nil {
		t.Fatal("inherited cut adopted a recreated session")
	}
	p.host.Incarnation = nil
	if _, err := p.rows(nil, owner, 3, "source"); err == nil {
		t.Fatal("missing incarnation reader became a complete census")
	}
}
