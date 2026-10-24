// server_import_listener_owner_test.go — terminal ownership of the listener
// keepers that ImportServer adopts from an inherited broker image.
package tunnel

import (
	"fmt"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/handoff/handofftest"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/sealedstate"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// importedListenerFixture is one successful ImportServer of a broker image that
// carries an endpoint listener and the tunnel control listener. Nothing is
// detached: the import is the current owner of both inherited LISTEN OFDs.
type importedListenerFixture struct {
	imported     *serverImportedImage
	endpointPort int
	controlPort  int
	sid          string
	cleanup      func()
}

// inheritedListenerImage builds the sealed tunnel image and the complete
// inherited resource table (state, flow, endpoint listener, control listener).
type inheritedListenerImage struct {
	image        ServerImage
	resources    []handoff.Resource
	endpointPort int
	controlPort  int
	sid          string
	origin       *Server
}

func buildInheritedListenerImage(t *testing.T) inheritedListenerImage {
	t.Helper()
	s := serverImageTestServer(t)
	endpointLn, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	controlLn, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	endpointPort := endpointLn.Addr().(*net.TCPAddr).Port
	controlPort := controlLn.Addr().(*net.TCPAddr).Port
	e := serverImageTestEndpoint(s, endpointPort, 1)
	e.phase = EndpointPhaseDetached
	var resources []handoff.Resource
	var oldKeepers []listenerimage.Keeper
	for _, item := range []struct {
		ln net.Listener
		id string
	}{{endpointLn, endpointListenerRef(e.id)}, {controlLn, "tunnel-control"}} {
		k, err := listenerimage.Capture(item.ln, item.id)
		if err != nil {
			t.Fatal(err)
		}
		if err = k.Pause(serverImageTestDeadline(t)); err != nil {
			t.Fatal(err)
		}
		_, res, err := k.Export()
		if err != nil {
			t.Fatal(err)
		}
		resources = append(resources, res)
		oldKeepers = append(oldKeepers, k)
	}
	e.listener = oldKeepers[0].View()
	state, err := snapshot(s, resources)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := s.flowState.manager.Freeze(time.Now().Add(5 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	flows, fs, err := frozen.Export()
	if err != nil {
		t.Fatal(err)
	}
	cut := sealedstate.CutState{Records: state.Records, Resources: state.Resources}
	if err = bindServerFlowImage(&cut, flows); err != nil {
		t.Fatal(err)
	}
	im, err := serverImageFromCut(cut, flows, "broker-tunnel-state.00000000000000000000000000000001")
	if err != nil {
		t.Fatal(err)
	}
	for i := range im.Endpoints {
		im.Endpoints[i].StateRef = im.StateResource
	}
	data, err := encodeServerCut(cut)
	if err != nil {
		t.Fatal(err)
	}
	ss, err := sealedstate.New(im.StateResource, data)
	if err != nil {
		t.Fatal(err)
	}
	// Exec would leave only the Prepared duplicate; in-process the old wrapper
	// relinquishes its registry claim so Adopt sees exactly one owner.
	for _, k := range oldKeepers {
		if _, err = k.Detach(); err != nil {
			t.Fatal(err)
		}
	}
	merged, err := handoff.MergeResources(state.Resources, fs.Resources, ss.Resources)
	if err != nil {
		t.Fatal(err)
	}
	return inheritedListenerImage{image: im, resources: merged, endpointPort: endpointPort, controlPort: controlPort, sid: e.allocation.SID, origin: s}
}

func (b inheritedListenerImage) source(failAt int) *serverImageTestSource {
	src := &serverImageTestSource{files: map[string]*os.File{}, failAt: failAt}
	for _, r := range b.resources {
		for name, f := range r.Files {
			src.files[r.ID+"/"+name] = f
		}
	}
	return src
}

func (b inheritedListenerImage) close() {
	for _, r := range b.resources {
		for _, f := range r.Files {
			_ = f.Close()
		}
	}
	_ = b.origin.flowState.manager.Close()
}

func importServerWithInheritedListeners(t *testing.T) importedListenerFixture {
	t.Helper()
	b := buildInheritedListenerImage(t)
	src := b.source(0)
	owner, salvage, err := ImportServer(ServerImportConfig{PublicHost: "127.0.0.1", Flow: serverImageTestConfig()}, b.image, src)
	if err != nil || len(salvage.Resources) != 0 {
		t.Fatalf("import of inherited listeners refused: %v", err)
	}
	imported := owner.(*serverImportedImage)
	if len(imported.keepers) != 2 {
		t.Fatalf("import adopted %d keepers, want 2", len(imported.keepers))
	}
	return importedListenerFixture{imported: imported, endpointPort: b.endpointPort, controlPort: b.controlPort, sid: b.sid,
		cleanup: func() {
			// Test-only reclamation so a red run does not leak into later tests.
			for _, k := range imported.keepers {
				_ = k.Close()
			}
			b.close()
		}}
}

func listenerStillBound(port int) bool {
	conn, err := net.DialTimeout("tcp4", fmt.Sprintf("127.0.0.1:%d", port), time.Second)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// After ImportServer the imported Server is the only owner of the inherited
// listener OFDs (the old image's originals died with CLOEXEC at exec). Every
// terminal path of that Server — normal stop and hard session revoke — must
// therefore close them; otherwise the kernel keeps completing handshakes into
// a backlog nobody Accepts and the public/control port can never be rebound.
// origin: hot-update S6 internal review rv-fd
func TestImportedServerTerminalCloseReleasesAdoptedListeners(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	for _, tc := range []struct {
		verb    string
		kill    func(*Server, importedListenerFixture)
		control bool // whether the verb is responsible for the control listener
	}{
		{"Close", func(s *Server, _ importedListenerFixture) { s.Close() }, true},
		{"CloseSession", func(s *Server, f importedListenerFixture) { s.CloseSession(f.sid) }, false},
		{"CloseProxy", func(s *Server, f importedListenerFixture) { s.CloseProxy(f.endpointPort) }, false},
	} {
		t.Run(tc.verb, func(t *testing.T) {
			f := importServerWithInheritedListeners(t)
			defer f.cleanup()
			srv := f.imported.Server()
			// Ownership is visible structurally before any terminal call.
			srv.mu.Lock()
			controlOwned := srv.controlKeeper != nil
			endpointOwned := srv.flowState.endpoints[f.endpointPort] != nil && srv.flowState.endpoints[f.endpointPort].keeper != nil
			srv.mu.Unlock()
			if !controlOwned || !endpointOwned {
				t.Errorf("imported Server has no terminal keeper owner (control=%v endpoint=%v); "+
					"adopted keepers live only in serverImportedImage.keepers", controlOwned, endpointOwned)
			}
			tc.kill(srv, f)
			if listenerStillBound(f.endpointPort) {
				t.Errorf("%s left the adopted endpoint LISTEN OFD bound on port %d", tc.verb, f.endpointPort)
			}
			if tc.control && listenerStillBound(f.controlPort) {
				t.Errorf("%s left the adopted tunnel-control LISTEN OFD bound on port %d", tc.verb, f.controlPort)
			}
		})
	}
}

// A refused import hands every taken file back for pristine Repack. The broker
// importer (trackedSource.include) and handoff.MergeResources only accept a
// salvage row whose Kind/Data equal the inherited resource; any other row is
// quarantined as a second, unrepackable owner of the same file.
// origin: hot-update S6 internal review rv-fd
func TestImportServerRefusalSalvageKeepsInheritedMetadata(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	b := buildInheritedListenerImage(t)
	defer b.close()
	want := map[string]handoff.Resource{}
	for _, r := range b.resources {
		want[r.ID] = r
	}
	// Refuse on the last listener Take, after state, flow state and the first
	// listener have been taken.
	takes := 0
	for _, r := range b.resources {
		takes += len(r.Files)
	}
	src := b.source(takes)
	owner, salvage, err := ImportServer(ServerImportConfig{PublicHost: "127.0.0.1", Flow: serverImageTestConfig()}, b.image, src)
	if err == nil || owner != nil {
		t.Fatalf("injected refusal accepted: %v", err)
	}
	listeners := 0
	for _, r := range salvage.Resources {
		orig, ok := want[r.ID]
		if !ok {
			t.Fatalf("salvage names unknown resource %q", r.ID)
		}
		if orig.Kind == "listener" {
			listeners++
		}
		if r.Kind != orig.Kind || string(r.Data) != string(orig.Data) {
			t.Errorf("salvage row %q kind=%q data=%dB, inherited kind=%q data=%dB", r.ID, r.Kind, len(r.Data), orig.Kind, len(orig.Data))
		}
	}
	if listeners == 0 {
		t.Fatal("fixture did not take a listener before refusing")
	}
	remaining := map[string]handoff.Resource{}
	for key, f := range src.files {
		for id, r := range want {
			for name := range r.Files {
				if key == id+"/"+name {
					rem := remaining[id]
					if rem.Files == nil {
						rem = handoff.Resource{ID: id, Kind: r.Kind, Data: r.Data, Files: map[string]*os.File{}}
					}
					rem.Files[name] = f
					remaining[id] = rem
				}
			}
		}
	}
	var untaken []handoff.Resource
	for _, r := range remaining {
		untaken = append(untaken, r)
	}
	if _, err := handoff.MergeResources(untaken, salvage.Resources); err != nil {
		t.Errorf("salvage cannot be merged back for pristine Repack: %v", err)
	}
}

// The inherited sealed tunnel state is only read once by ImportServer. After a
// successful import and the Server's normal terminal stop, no owner remains
// that will ever close that memfd: it must not outlive the current generation.
// origin: hot-update S6 internal review rv-fd
func TestImportedServerTerminalCloseReleasesSealedState(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	f := importServerWithInheritedListeners(t)
	defer f.cleanup()
	var state *os.File
	for _, r := range f.imported.resources {
		if r.ID == f.imported.image.StateResource {
			state = r.Files["state"]
		}
	}
	if state == nil {
		t.Fatal("fixture lost the inherited sealed state")
	}
	f.imported.Server().Close()
	if _, err := state.Stat(); err == nil {
		t.Error("inherited sealed tunnel state is still open after the imported Server's terminal Close")
	}
}

// After ACTIVE begins, the only legal rollback is a fresh current cut of the
// imported Server (plan §4.2). That cut must be able to re-export the adopted
// listener OFDs; a Server whose listeners are dormant View() placeholders with
// no keeper cannot be frozen at all.
// origin: hot-update S6 internal review rv-fd
func TestImportedServerRemainsFreezableByCurrentOwner(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	f := importServerWithInheritedListeners(t)
	defer f.cleanup()
	cut, err := f.imported.Server().FreezeServer(serverImageTestDeadline(t))
	if err != nil {
		t.Fatalf("current cut of the imported Server refused: %v", err)
	}
	im, resources, err := cut.Export()
	if err != nil || len(im.Listeners) != 2 || len(resources) < 4 {
		t.Fatalf("current cut failed to re-export the dormant adopted closure: %v", err)
	}
	if err = cut.Discard(); err != nil {
		t.Fatal(err)
	}
}

// Pause terminal Close after it sets s.closed but before it closes the manager.
// Activation has already constructed public views, and must reclaim those views
// instead of installing a new control loop into the closed Server.
// origin: hot-update S6 plan §4.2 / T52 / T56; internal review rv-fd-3
func TestImportedServerActivationCannotRestartClosedOwner(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux image owner")
	}
	beforeG, beforeFD := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseImportedServerActivationStop(t)
	}
	testharness.AssertNoGoroutineLeak(t, "import activation stop", beforeG)
	testharness.AssertNoFDLeak(t, "import activation stop", beforeFD, 0)
}

type importControlBarrier struct {
	listenerimage.Keeper
	resource                     handoff.Resource
	exportEntered, exportRelease chan struct{}
	closeEntered, closeRelease   chan struct{}
}

func (k *importControlBarrier) Export() (listenerimage.Descriptor, handoff.Resource, error) {
	close(k.exportEntered)
	<-k.exportRelease
	return k.Descriptor(), k.resource, nil
}
func (k *importControlBarrier) Close() error {
	close(k.closeEntered)
	<-k.closeRelease
	return k.Keeper.Close()
}

func exerciseImportedServerActivationStop(t *testing.T) {
	t.Helper()
	f := importServerWithInheritedListeners(t)
	defer f.cleanup()
	s := f.imported.Server()
	_, resource, err := s.controlKeeper.Export()
	if err != nil {
		t.Fatal(err)
	}
	// This explicit same-OFD duplicate models the native view already created
	// while terminal Close begins. Its ownership stays with this fixture.
	copies := handofftest.DupResources(t, []handoff.Resource{resource})
	defer copies.Close()
	barrier := &importControlBarrier{Keeper: s.controlKeeper, resource: copies.Borrow()[0],
		exportEntered: make(chan struct{}), exportRelease: make(chan struct{}),
		closeEntered: make(chan struct{}), closeRelease: make(chan struct{})}
	s.controlKeeper = barrier
	activated := make(chan error, 1)
	deadline := serverImageTestDeadline(t)
	go func() { activated <- f.imported.Activate(deadline) }()
	<-barrier.exportEntered
	stopped := make(chan struct{})
	go func() { s.Close(); close(stopped) }()
	<-barrier.closeEntered
	close(barrier.exportRelease)
	err = <-activated
	close(barrier.closeRelease)
	if err == nil {
		t.Error("activation restarted a Server whose terminal Close had begun")
	}
	// A broken installer may have added a new Accept owner after Close took
	// its snapshot. Join that red-mutation worker before waiting on Close.
	s.mu.Lock()
	ln := s.ln
	s.mu.Unlock()
	if ln != nil {
		t.Error("activation installed a control view after terminal Close began")
		_ = ln.Close()
	}
	<-stopped
	copies.Close()
	if listenerStillBound(f.controlPort) {
		t.Error("activation left a control LISTEN alias after terminal Close")
	}
	s.wg.Wait()
}
