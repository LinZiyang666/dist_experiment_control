package agentupgradeport

import (
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review activation-and-route-migration F2
func TestAgentRecoveryAuthorityBindsAcceptedCut(t *testing.T) {
	key := upgradetx.Key{DomainID: strings.Repeat("d", 32), Component: upgradetx.ComponentAgent, SID: "lab", ConfiguredNID: "node"}
	handle := upgradetx.Handle{Key: key, TxID: strings.Repeat("1", 32), CutID: strings.Repeat("2", 32), Generation: 2, Revision: 9}
	incarnation := strings.Repeat("a", 64)
	row := upgradetx.Resource{Domain: upgradetx.DomainAuthority, ID: incarnation, Origin: "session:lab"}
	view := &upgradetx.View{Historical: upgradetx.Record{Key: key, Revision: handle.Revision, Forward: upgradetx.Attempt{Handle: handle, Scope: upgradetx.Scope{Expected: []upgradetx.Resource{row}}}}}
	port := NewRuntime(Host{Key: key, Journal: recordJournal{view: view}, Incarnation: func() string { return incarnation }})
	observer, ok := port.(interface {
		Incarnation(upgradetx.Handle) (string, error)
	})
	if !ok {
		t.Fatal("production runtime has no current-authority recovery observer")
	}
	if got, err := observer.Incarnation(handle); err != nil || got != incarnation {
		t.Fatalf("accepted authority unavailable: %q %v", got, err)
	}
	for _, change := range []func(*upgradetx.Handle){
		func(h *upgradetx.Handle) { h.Key.SID = "other" }, func(h *upgradetx.Handle) { h.TxID = strings.Repeat("3", 32) },
		func(h *upgradetx.Handle) { h.CutID = strings.Repeat("3", 32) }, func(h *upgradetx.Handle) { h.Generation++ }, func(h *upgradetx.Handle) { h.Revision-- },
	} {
		stale := handle
		change(&stale)
		if got, err := observer.Incarnation(stale); err == nil || got != "" {
			t.Fatal("stale recovery fence acquired current authority")
		}
	}
	for _, bad := range []string{"", "unknown", strings.Repeat("b", 64)} {
		incarnation = bad
		if got, err := observer.Incarnation(handle); err == nil || got != "" {
			t.Fatal("replacement or unknown authority authorized reverse")
		}
	}
	incarnation = row.ID
	view.Historical.Forward.Scope.Expected = nil
	if _, err := observer.Incarnation(handle); err == nil {
		t.Fatal("accepted cut omitted authority but recovery succeeded")
	}
	view.Historical.Forward.Scope.Expected = []upgradetx.Resource{row}
	p := port.(*runtimePort)
	p.host.Inherited = &upgradecut.State{Attempt: handle, Authorities: []upgradecut.Authority{{SID: key.SID, Incarnation: strings.Repeat("b", 64)}}}
	if _, err := observer.Incarnation(handle); err == nil {
		t.Fatal("recovery ignored inherited authority mismatch")
	}
}
