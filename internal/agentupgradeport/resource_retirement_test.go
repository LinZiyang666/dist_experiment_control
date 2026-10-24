package agentupgradeport

import (
	"fmt"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/execsession"
	"github.com/LinZiyang666/tether/internal/procrequest"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review activation-and-route-migration F8
func TestAgentCensusKeepsSettledIdentityAfterCollection(t *testing.T) {
	owner := upgradetx.Owner{BootID: "boot", PID: 123, StartTicks: 1, InstanceID: strings.Repeat("1", 32)}
	handle := proto.ProcessHandle{Lease: proto.ProcessLease{LeaseID: "lease"}, RequestID: "request"}
	record := procrequest.Record{Handle: handle, PID: "child", Phase: "settled", Terminal: &procrequest.Terminal{ID: "terminal"}}

	retain := func(p *runtimePort, rows []upgradetx.Resource) []upgradetx.Resource {
		t.Helper()
		got, err := p.retainReleased(rows)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	for _, kind := range []string{"", proto.ProcessKindPTY} {
		record.Kind = kind
		image := execsession.Image{Registry: procrequest.State{Records: []procrequest.Record{record}}}
		rows, err := BundleRows(image, agentimage.CourierState{}, nil, owner, 2, "target")
		if err != nil || len(rows) != 2 {
			t.Fatalf("settled process has no exact task/result release evidence: %v %v", rows, err)
		}
		for _, r := range rows {
			if !upgradetx.ResourceReleased(r) {
				t.Fatal("authenticated settlement remained live custody")
			}
		}
		p := NewRuntime(Host{}).(*runtimePort)
		before := retain(p, rows)
		after := retain(p, nil)
		if !upgradetx.SameResources(before, after) {
			t.Fatal("legitimate collection erased a sealed original identity")
		}
		next := NewRuntime(Host{Inherited: &upgradecut.State{Scope: upgradetx.Scope{Expected: before}}}).(*runtimePort)
		if !upgradetx.SameResources(retain(next, nil), before) {
			t.Fatal("next cut forgot current release evidence")
		}
		for _, phase := range []string{"running", "terminal"} {
			image.Registry.Records[0].Phase = phase
			live, err := BundleRows(image, agentimage.CourierState{}, nil, owner, 2, "target")
			if err != nil || len(live) != 1 || upgradetx.ResourceReleased(live[0]) {
				t.Fatal("unacknowledged terminal acquired release authority")
			}
			unknown := NewRuntime(Host{Inherited: &upgradecut.State{Scope: upgradetx.Scope{Expected: live}}}).(*runtimePort)
			if len(retain(unknown, nil)) != 0 {
				t.Fatal("missing live owner was inferred released")
			}
		}
	}

	bounded := NewRuntime(Host{}).(*runtimePort)
	var overflow []upgradetx.Resource
	for i := 0; i < 4097; i++ {
		overflow = append(overflow, upgradetx.Resource{Domain: upgradetx.DomainExec, ID: fmt.Sprint(i), Origin: "request", State: "terminal", Custody: "released"})
	}
	if _, err := bounded.retainReleased(overflow); err == nil || len(bounded.released) > 4096 {
		t.Fatal("full release ledger dropped evidence or exceeded its bound")
	}
	// The same task remains the same resource when its payload becomes a
	// tombstone. Changing its prefix makes a valid terminal look like loss.
	active, err := BundleRows(execsession.Image{Tasks: []execsession.TaskImage{{PID: "child", Handle: handle}}}, agentimage.CourierState{}, nil, owner, 2, "target")
	if err != nil {
		t.Fatal(err)
	}
	terminal, err := BundleRows(execsession.Image{Tombstones: []execsession.TombstoneImage{{PID: "child", Handle: handle}}}, agentimage.CourierState{}, nil, owner, 2, "target")
	if err != nil || len(terminal) != 1 || active[0].ID != terminal[0].ID || active[0].Origin != terminal[0].Origin {
		t.Fatal("terminal transition changed the original task identity")
	}
	legacy := terminal[0]
	legacy.ID = "tombstone:child"
	imported := NewRuntime(Host{Inherited: &upgradecut.State{Scope: upgradetx.Scope{Expected: []upgradetx.Resource{legacy}}}}).(*runtimePort)
	oldAlias := retain(imported, terminal)
	found := false
	for _, row := range oldAlias {
		found = found || row.ID == legacy.ID && row.Origin == legacy.Origin && row.State == "terminal"
	}
	if !found {
		t.Fatal("new reader lost the previous S8 exporter's sealed tombstone identity")
	}
	unknown := NewRuntime(Host{Inherited: imported.host.Inherited}).(*runtimePort)
	if len(retain(unknown, nil)) != 0 {
		t.Fatal("missing legacy tombstone was inferred terminal")
	}

}
