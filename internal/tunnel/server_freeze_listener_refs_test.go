// server_freeze_listener_refs_test.go — the listener resources a real Server
// freeze exports must be named in the vocabulary ImportServer adopts.
package tunnel

import (
	"context"
	"net"
	"runtime"
	"sort"
	"testing"
)

// ImportServer adopts the control listener only as ID "tunnel-control" and an
// endpoint listener only as endpointListenerRef(id) ("flow-endpoint.<hex>"),
// the same refs validateServerCut requires. A runtime freeze that captures its
// keepers under any other ID produces listener FDs that no importer will ever
// claim: the inherited public/control sockets fail import (pristine rollback)
// or, worse, survive unowned.
// origin: hot-update S6 internal review rv-fd
func TestServerFreezeExportsImporterListenerRefs(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	s := serverImageTestServer(t)
	ctx, cancel := context.WithCancel(context.Background()) // ctx-root: unit fixture owns the live server loops.
	defer cancel()
	defer s.Close()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	probe, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	_ = probe.Close()
	e, err := reserveEndpoint(s, Allocation{Port: port, SID: "session", NID: "agent", TokenHash: "token", Epoch: 1}, fenceSnap{})
	if err != nil {
		t.Fatal(err)
	}
	cut, err := freezeServerRuntime(s, serverImageTestDeadline(t))
	if err != nil {
		t.Fatalf("runtime freeze: %v", err)
	}
	defer func() { _ = cut.thaw(serverImageTestDeadline(t)) }()
	state, err := cut.snapshot()
	if err != nil {
		t.Fatalf("runtime snapshot: %v", err)
	}
	var got []string
	for _, r := range state.Resources {
		if r.Kind == "listener" {
			got = append(got, r.ID)
		}
	}
	sort.Strings(got)
	want := []string{endpointListenerRef(e.id), "tunnel-control"}
	sort.Strings(want)
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("freeze exported listener refs %q; ImportServer/validateServerCut adopt only %q", got, want)
	}
}
