// image_test.go — the spawn-safety half of an agent image: Export refuses
// while any spawn or probe goroutine exists (the fail-closed rule, S2 plan
// D9/Q3), Import installs only verdicts for mounts that are still the same
// instance, and a sticky-dead verdict never grows a probe on import.
package spawnsafe

import (
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

// origin: hot-update S2 plan §3.6 / §5.1 — the in-flight counter is
// independent of the health map: a probe whose entry applyMounts dropped
// (signature changed) and a timed-out probe retained by a sticky-dead entry
// are both still goroutines, and both must defer the image. Only a returned
// probe clears the way; an acquired spawn slot is the other deferral.
func TestPolicyExportRefusesInFlightSpawnAndProbe(t *testing.T) {
	before := runtime.NumGoroutine()
	fp := newFakeProbe()
	block := make(chan bool)
	fp.block["/nfs"] = block
	released := false
	defer func() {
		if !released {
			close(block)
		}
		assertGoroutinesReturn(t, before)
	}()
	src := mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/other", "nfs"})
	p := mustNew(t, Config{Mode: ModeAuto, ProbeTimeout: 40 * time.Millisecond, MountSource: src, Probe: fp.fn})
	if c := p.Census(); c.Wedged != 0 || c.ProbesInFlight != 0 {
		t.Fatalf("fresh census: %+v", c)
	}
	if _, err := p.Export(); err != nil {
		t.Fatalf("export with nothing decided: %v", err)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); p.mountHealthy("/nfs") }()
	fp.waitCount(t, "/nfs", 1)
	if c := p.Census(); c.ProbesInFlight != 1 {
		t.Fatalf("census with a probe launched: %+v", c)
	}
	if _, err := p.Export(); !errors.Is(err, ErrProbeInFlight) {
		t.Fatalf("export with a probe in flight: %v", err)
	}
	wg.Wait() // the launcher timed out: the mount is sticky-dead, the probe still out
	p.mu.Lock()
	state, retained := p.health["/nfs"].state, p.health["/nfs"].result != nil
	p.mu.Unlock()
	if state != stUnhealthy || !retained {
		t.Fatalf("fixture: expected a timed-out sticky-dead entry, got state=%d retained=%t", state, retained)
	}
	if c := p.Census(); c.ProbesInFlight != 1 || c.ProbesRetained != 1 {
		t.Fatalf("census after the timeout: %+v, want the one in-flight probe reported as retained", c)
	}
	if _, err := p.Export(); !errors.Is(err, ErrProbeInFlight) {
		t.Fatalf("export with a timed-out probe still out: %v", err)
	}
	// Unrelated mount churn drops the entry from the map; the goroutine is
	// still there and the counter still sees it.
	p.applyMounts(parseMountinfo(fakeMountinfo([2]string{"/nfs", "nfs"}, [2]string{"/elsewhere", "nfs"})), "gen-2")
	p.mu.Lock()
	_, kept := p.health["/nfs"]
	p.mu.Unlock()
	if !kept {
		t.Fatal("fixture: an unchanged mount lost its entry on churn (F4 regression)")
	}
	p.applyMounts(parseMountinfo(fakeMountinfo([2]string{"/nfs", "cifs"})), "gen-3") // signature changed: entry dropped
	p.mu.Lock()
	_, kept = p.health["/nfs"]
	p.mu.Unlock()
	if kept {
		t.Fatal("fixture: a changed mount kept its entry")
	}
	if c := p.Census(); c.ProbesInFlight != 1 {
		t.Fatalf("a dropped entry hid its probe goroutine from the census: %+v", c)
	}
	if _, err := p.Export(); !errors.Is(err, ErrProbeInFlight) {
		t.Fatalf("export after the entry was dropped: %v", err)
	}
	// The probe returns: exportable.
	block <- true
	released = true
	deadline := time.Now().Add(3 * time.Second)
	for p.Census().ProbesInFlight != 0 {
		if time.Now().After(deadline) {
			t.Fatal("probe return never cleared the counter")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := p.Export(); err != nil {
		t.Fatalf("export after the probe returned: %v", err)
	}
	// A held spawn slot is the other deferral.
	if !p.TryAcquireSpawnSlot() {
		t.Fatal("slot")
	}
	if _, err := p.Export(); !errors.Is(err, ErrSpawnInFlight) {
		t.Fatalf("export with a spawn slot held: %v", err)
	}
	p.ReleaseSpawnSlot()
	if _, err := p.Export(); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S2 plan §3.6 / §5.1 — an exported healthy verdict keeps
// its instant (the TTL continues from the real decision, so it expires and
// re-probes on schedule), an exported dead verdict stays sticky WITHOUT a
// probe goroutine, a verdict whose mount instance changed is skipped, a
// state from another generation is skipped entirely, and a policy that
// already decided something refuses the import.
func TestPolicyImportPreservesVerdictsAndSkipsForeignGeneration(t *testing.T) {
	before := runtime.NumGoroutine()
	defer assertGoroutinesReturn(t, before)
	clock := newFakeClock()
	fp := newFakeProbe()
	fp.ret["/dead"] = false
	src := mountSrc([2]string{"/good", "nfs"}, [2]string{"/dead", "nfs"}, [2]string{"/changed", "nfs"})
	old := mustNew(t, Config{Mode: ModeAuto, ProbeTimeout: time.Second, HealthTTL: time.Minute, MountSource: src, Probe: fp.fn, Now: clock.now})
	if !old.mountHealthy("/good") || old.mountHealthy("/dead") || !old.mountHealthy("/changed") {
		t.Fatal("fixture verdicts")
	}
	fp.waitCount(t, "/dead", 1)
	deadline := time.Now().Add(3 * time.Second)
	for old.Census().ProbesInFlight != 0 {
		if time.Now().After(deadline) {
			t.Fatal("probes never returned")
		}
		time.Sleep(time.Millisecond)
	}
	clock.advance(30 * time.Second) // the verdicts are half-way through their TTL
	st, err := old.Export()
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Verdicts) != 3 {
		t.Fatalf("exported %d verdicts, want 3: %+v", len(st.Verdicts), st.Verdicts)
	}
	if err := st.Validate(); err != nil {
		t.Fatal(err)
	}

	// The next image: /changed was remounted (signature differs), an
	// unrelated mount appeared (so the whole-table Gen differs — the norm on
	// a busy box), the rest is the same table. The per-mount signature rule
	// decides; Gen is informational (S2 internal review I3).
	fp2 := newFakeProbe()
	src2 := mountSrc([2]string{"/good", "nfs"}, [2]string{"/dead", "nfs"}, [2]string{"/changed", "cifs"}, [2]string{"/unrelated-new", "nfs"})
	next := mustNew(t, Config{Mode: ModeAuto, ProbeTimeout: time.Second, HealthTTL: time.Minute, MountSource: src2, Probe: fp2.fn, Now: clock.now})
	next.mu.Lock()
	genDiffers := st.Gen != next.gen
	next.mu.Unlock()
	if !genDiffers {
		t.Fatal("fixture: the importing table must differ from the exporting one")
	}
	if err := next.ImportReady(); err != nil {
		t.Fatal(err)
	}
	goroutinesBefore := runtime.NumGoroutine()
	installed, err := next.Import(st)
	if err != nil {
		t.Fatal(err)
	}
	if installed != 2 {
		t.Fatalf("installed %d verdicts, want 2 (/good and /dead; /changed was remounted)", installed)
	}
	next.mu.Lock()
	good, dead := next.health["/good"], next.health["/dead"]
	_, changed := next.health["/changed"]
	next.mu.Unlock()
	if good == nil || good.state != stHealthy || good.result != nil || !good.decidedAt.Equal(clock.now().Add(-30*time.Second)) {
		t.Fatalf("healthy verdict not installed with its original instant: %+v", good)
	}
	if dead == nil || dead.state != stUnhealthy || dead.result != nil || dead.launched {
		t.Fatalf("dead verdict not installed sticky and probe-less: %+v", dead)
	}
	if changed {
		t.Fatal("a verdict for a remounted instance was installed")
	}
	// Consulting them launches nothing: healthy is served from the verdict,
	// dead stays sticky.
	if !next.mountHealthy("/good") || next.mountHealthy("/dead") {
		t.Fatal("imported verdicts not honoured")
	}
	if fp2.count("/good") != 0 || fp2.count("/dead") != 0 {
		t.Fatalf("imported verdicts launched probes: good=%d dead=%d", fp2.count("/good"), fp2.count("/dead"))
	}
	if g := runtime.NumGoroutine(); g > goroutinesBefore+1 {
		t.Fatalf("import grew goroutines: %d → %d", goroutinesBefore, g)
	}
	// The healthy verdict expires on the original schedule: 31 s later it
	// re-probes (TTL 60 s from the real decision), while a fresh verdict
	// would have had 60 s left.
	clock.advance(31 * time.Second)
	if !next.mountHealthy("/good") {
		t.Fatal("re-probe of an expired imported verdict failed")
	}
	fp2.waitCount(t, "/good", 1)
	if fp2.count("/dead") != 0 {
		t.Fatal("dead verdict re-probed")
	}

	// Refusals and skips.
	if err := next.ImportReady(); err == nil {
		t.Fatal("ImportReady accepted a policy that already decided")
	}
	if _, err := next.Import(st); err == nil {
		t.Fatal("import into a policy that already decided succeeded")
	}
	// A table where none of the exported mounts survives installs nothing
	// and reports so — not an error.
	fresh := mustNew(t, Config{Mode: ModeAuto, MountSource: mountSrc([2]string{"/elsewhere", "nfs"}), Probe: fp2.fn, Now: clock.now})
	if installed, err := fresh.Import(st); err != nil || installed != 0 {
		t.Fatalf("no surviving mount: installed=%d err=%v", installed, err)
	}
	fresh.mu.Lock()
	remaining := len(fresh.health)
	fresh.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("verdicts installed for mounts that do not exist: %d", remaining)
	}
	bad := st
	bad.Verdicts = append(append([]MountVerdict(nil), st.Verdicts...), MountVerdict{Mountpoint: "relative", Signature: "x", DecidedAtUnixNano: 1})
	if _, err := fresh.Import(bad); err == nil {
		t.Fatal("invalid verdict accepted")
	}
	bad = st
	bad.Version = 2
	if _, err := fresh.Import(bad); err == nil {
		t.Fatal("wrong version accepted")
	}
}
