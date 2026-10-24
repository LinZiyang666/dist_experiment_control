//go:build linux

// fence_test.go — the two exits of the fence that runtime_image_test.go's
// fence test does not walk: Discard (old image, exported) and Detach
// (imported image, never thawed) must each release every Handle waiter with
// ErrClosed, exactly like Close, and must leave no pump or observer behind
// (Discard's observers return on the frozen+closed gate without touching
// the child; Detach never had any).
// origin: hot-update S2 internal review, resources lane.
package execsession

import (
	"errors"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// fencePark starts one Handle call of each attach shape against r and
// proves none of them returns while the fence is up.
func fencePark(t *testing.T, f *runtimeFixture, r *Runtime, h proto.ProcessHandle) <-chan error {
	t.Helper()
	ops := []proto.ProcessResumeReq{
		{Op: "probe", ClientLineage: f.scope.ClientLineage},
		{Op: "resume", Handle: &h, AttachEpoch: 1},
		{Op: "ack", Handle: &h, AttachEpoch: 1},
	}
	results := make(chan error, len(ops))
	for _, op := range ops {
		go func(op proto.ProcessResumeReq) {
			op.Version, op.CallID, op.ActorFP = proto.ProcessResumeVersion, f.calls.Add(1), f.scope.ActorFP
			_, err := r.Handle(f.scope, op, "_TINBOX.actor.original")
			results <- err
		}(op)
	}
	select {
	case err := <-results:
		t.Fatalf("an op answered under the fence: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	return results
}

func fenceRequireReleased(t *testing.T, label string, results <-chan error, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		select {
		case err := <-results:
			if !errors.Is(err, ErrClosed) {
				t.Fatalf("%s: waiter returned %v, want ErrClosed", label, err)
			}
		case <-time.After(runtimeTestBudget):
			t.Fatalf("%s: waiter %d never released", label, i)
		}
	}
}

// fenceAwaitBaseline polls a bare count back to (at most) its baseline. Bare
// counts, not the leak helpers: this test exercises its subject once by
// design (the fence is torn down exactly once per runtime), which is the
// shape test/determinism's leak_assert_shape gate reserves for the
// five-round anchors.
func fenceAwaitBaseline(t *testing.T, label string, baseline int, count func() int) {
	t.Helper()
	var last int
	for i := 0; i < 50; i++ {
		runtime.Gosched()
		if last = count(); last <= baseline {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: %d > baseline %d", label, last, baseline)
}

// fenceObserverCount counts live observe goroutines (any runtime): the old
// image's observer must leave on the frozen+closed gate after Discard.
func fenceObserverCount() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "(*Runtime).observe(")
}

func TestRuntimeFenceDiscardAndDetachReleaseWaiters(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	rf := newImageRejectFixture(t)
	live := rf.task(rf.live)
	beforeG := runtime.NumGoroutine()

	// Old image: frozen and exported, waiters parked, then Discard.
	parkedOld := fencePark(t, rf.f, rf.f.r, live.Handle)
	src := newImageDupSource(t, rf.resources)
	imported, salvage, err := Import(rf.cfg, rf.img, rf.rows, src)
	if err != nil {
		t.Fatalf("import: %v (salvage %d)", err, len(salvage))
	}
	old := rf.f.r
	if fenceObserverCount() == 0 {
		t.Fatal("fixture assumption broken: no observer was watching the live task")
	}
	if err := old.Discard(); err != nil {
		t.Fatalf("discard: %v", err)
	}
	fenceRequireReleased(t, "discard", parkedOld, 3)
	if err := old.Discard(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second discard: %v", err)
	}
	imageChildAlive(t, rf.osPID) // Discard signals nothing
	// The old observer returns on the frozen+closed gate within a tick.
	fenceAwaitBaseline(t, "old observers after discard", 0, fenceObserverCount)
	fenceAwaitBaseline(t, "goroutines after discard", beforeG, runtime.NumGoroutine)

	// New image: imported and never thawed, waiters parked, then Detach:
	// every descriptor comes back open and the waiters are released.
	rf.f.r = imported
	parkedNew := fencePark(t, rf.f, imported, live.Handle)
	fdBefore := testharness.FDCount()
	resources, err := imported.Detach()
	if err != nil {
		t.Fatalf("detach: %v", err)
	}
	fenceRequireReleased(t, "detach", parkedNew, 3)
	if _, err := imported.Detach(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second detach: %v", err)
	}
	if err := imported.Thaw(); !errors.Is(err, ErrClosed) {
		t.Fatalf("thaw after detach: %v", err)
	}
	got := 0
	for _, res := range resources {
		for name, file := range res.Files {
			imageFileOpen(t, res.ID+"/"+name, file)
			got++
			_ = file.Close()
		}
	}
	expected := 0
	for _, ti := range rf.img.Tasks {
		expected += len(ti.Files)
	}
	if got != expected {
		t.Fatalf("detach returned %d files, the image carries %d", got, expected)
	}
	if used := imported.ReplayBudgetUsed(); used != 0 {
		t.Fatalf("detach kept %d bytes of replay budget reserved", used)
	}
	fenceAwaitBaseline(t, "descriptors after detach", fdBefore-got, testharness.FDCount)
	fenceAwaitBaseline(t, "goroutines after detach", beforeG, runtime.NumGoroutine)
	imageChildAlive(t, rf.osPID)
	// The child is still the shared table's: thaw it and reap for real.
	rf.f.manager.Thaw()
	_ = rf.f.manager.Signal(rf.live, syscall.SIGKILL)
	_ = rf.f.manager.AllowReap(rf.live)
	if _, err := rf.f.manager.Wait(rf.live); err != nil {
		t.Fatalf("wait after detach: %v", err)
	}
}
