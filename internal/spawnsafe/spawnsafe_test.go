package spawnsafe

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mustNew wraps New, failing the test on a config-validation error.
func mustNew(t *testing.T, cfg Config) *Policy {
	t.Helper()
	p, err := New(cfg)
	if err != nil {
		t.Fatalf("spawnsafe.New: %v", err)
	}
	return p
}

// TestPrepare_localMachineZeroSyscallPerSpawn pins review m7: an agent that
// booted with no hangable mounts does NOT read /proc/self/mountinfo on each
// spawn — Prepare short-circuits on the cached boot verdict, so a local agent is
// truly zero-syscall-per-spawn.
func TestPrepare_localMachineZeroSyscallPerSpawn(t *testing.T) {
	var reads int32
	src := func() ([]byte, error) {
		atomic.AddInt32(&reads, 1)
		return fakeMountinfo([2]string{"/", "ext4"}), nil
	}
	p := mustNew(t, Config{Mode: ModeAuto, MountSource: src, Probe: newFakeProbe().fn})
	if p.safeDir != "" || p.fallback != nil {
		t.Fatalf("local startup initialized filesystem paths: safeDir=%q fallback=%v", p.safeDir, p.fallback)
	}
	atStart := atomic.LoadInt32(&reads) // the single boot snapshot read
	for i := 0; i < 20; i++ {
		// auto (not --safe, which intentionally bypasses the boot fast path, F7).
		if d, _ := p.Prepare([]string{"echo"}, "", "/usr/bin", []string{"PATH=/usr/bin"}, false); d.Active {
			t.Fatal("local machine must be inert")
		}
	}
	if extra := atomic.LoadInt32(&reads) - atStart; extra != 0 {
		t.Errorf("local machine did %d mountinfo reads across 20 spawns, want 0 (review m7)", extra)
	}
}

func TestNew_modeOffDefersPathInitialization(t *testing.T) {
	p := mustNew(t, Config{
		Mode:        ModeOff,
		MountSource: mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}),
		Probe:       newFakeProbe().fn,
	})
	if p.safeDir != "" || p.fallback != nil {
		t.Fatalf("mode=off initialized paths at startup: safeDir=%q fallback=%v", p.safeDir, p.fallback)
	}
	if d, err := p.Prepare([]string{"true"}, "", "/bin", nil, false); err != nil || d.Active {
		t.Fatalf("mode=off ordinary prepare should remain inert: d=%+v err=%v", d, err)
	}
	if p.safeDir != "" || p.fallback != nil {
		t.Fatalf("mode=off ordinary prepare initialized paths: safeDir=%q fallback=%v", p.safeDir, p.fallback)
	}
}

// --- test fakes -------------------------------------------------------------

// fakeMountinfo renders minimal-but-valid mountinfo lines from (mountpoint,
// fstype) pairs. Layout per proc(5): fields before " - ", then fstype after.
func fakeMountinfo(entries ...[2]string) []byte {
	var b strings.Builder
	for i, e := range entries {
		fmt.Fprintf(&b, "%d 35 0:%d / %s rw - %s src rw\n", 36+i, i, e[0], e[1])
	}
	return []byte(b.String())
}

func mountSrc(entries ...[2]string) MountSource {
	raw := fakeMountinfo(entries...)
	return func() ([]byte, error) { return raw, nil }
}

// fakeProbe records call counts and lets a test make a mount block (dead) or
// return an immediate verdict.
type fakeProbe struct {
	mu    sync.Mutex
	calls map[string]int
	block map[string]chan bool // present ⇒ probe blocks until the test sends/closes
	ret   map[string]bool      // immediate verdict; absent ⇒ true (healthy)
}

func newFakeProbe() *fakeProbe {
	return &fakeProbe{calls: map[string]int{}, block: map[string]chan bool{}, ret: map[string]bool{}}
}

func (f *fakeProbe) fn(mp string) bool {
	f.mu.Lock()
	f.calls[mp]++
	ch := f.block[mp]
	r, has := f.ret[mp]
	f.mu.Unlock()
	if ch != nil {
		v, ok := <-ch
		if !ok {
			return false // closed at cleanup
		}
		return v
	}
	if has {
		return r
	}
	return true
}

func (f *fakeProbe) count(mp string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[mp]
}

// setVerdict / setBlocking change what a mount answers from NOW ON. They model a
// mount whose real liveness changed under a running policy — the healthy→dead
// transition that #81 turned out to live in and that nothing in this package had
// ever exercised (every pre-#81 case was "dead at first probe").
//
// Deliberately NOT a per-call script: the number of probes is itself the thing
// under test (single-flight held? invalidation fired?), so binding assertions to
// a call index would make those tests unable to fail for the right reason.
//
// After New, mutate ONLY through these; writing f.ret / f.block directly races
// with a probe goroutine already in flight (the pre-existing cases all write
// before New, which is why they need no change).
func (f *fakeProbe) setVerdict(mp string, healthy bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.block, mp)
	f.ret[mp] = healthy
}

func (f *fakeProbe) setBlocking(mp string, ch chan bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.block[mp] = ch
}

// waitCount polls until mp has been probed n times, so tests never race a probe
// goroutine that has been launched but not yet recorded.
func (f *fakeProbe) waitCount(t *testing.T, mp string, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if f.count(mp) >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("probe count for %s = %d, want >= %d", mp, f.count(mp), n)
}

// fakeClock is the injected time source for the health TTL. Concurrency-safe
// because the stress cases advance it from a different goroutine than the one
// reading it; a plain field would make -race red on something unrelated to the
// invariant under test.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// assertGoroutinesReturn polls until the goroutine count drops back near a
// baseline, proving abandoned/probe goroutines were released (the repo uses a
// count-based leak check, not goleak — see test/concurrency/helpers_test.go).
func assertGoroutinesReturn(t *testing.T, before int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Errorf("goroutine leak: baseline=%d now=%d", before, runtime.NumGoroutine())
}

// --- parsing & classification ----------------------------------------------

func TestParseMountinfo_table(t *testing.T) {
	raw := []byte(strings.Join([]string{
		"36 35 0:1 / /local rw,noatime - ext4 /dev/sda1 rw",
		`37 36 0:2 / /mnt/my\040share rw shared:1 master:2 - nfs4 srv:/x rw`, // optional tags + escaped space
		"38 36 0:3 / /home/u rw - nfs srv:/home rw",
		"this is a garbage line without a dash",
		"39 36 0:4 / /database rw - ext4 /dev/sdb rw", // prefix-collision vs /data
		"40 36 0:5 / /data rw - nfs srv:/data rw",
		"", // blank
	}, "\n"))
	mounts := parseMountinfo(raw)
	if len(mounts) != 5 {
		t.Fatalf("got %d mounts, want 5 (garbage/blank skipped): %+v", len(mounts), mounts)
	}
	var share *mountEntry
	for i := range mounts {
		if mounts[i].fstype == "nfs4" {
			share = &mounts[i]
		}
	}
	if share == nil || share.mountpoint != "/mnt/my share" {
		t.Fatalf("octal-escaped mountpoint not decoded: %+v", share)
	}

	// longest-prefix, component-boundary aware.
	p := mustNew(t, Config{MountSource: func() ([]byte, error) { return raw, nil }, Probe: newFakeProbe().fn})
	if m := p.mountForPath("/data/x"); m.mountpoint != "/data" || m.kind != kindRemoteProbe {
		t.Fatalf("/data/x → %+v, want /data nfs", m)
	}
	if m := p.mountForPath("/database/y"); m.mountpoint != "/database" || m.kind != kindLocal {
		t.Fatalf("/database/y → %+v, want /database ext4 (no /data collision)", m)
	}
}

func TestClassifyFstype_table(t *testing.T) {
	cases := []struct {
		fstype string
		want   fsKind
	}{
		{"ext4", kindLocal}, {"xfs", kindLocal}, {"btrfs", kindLocal}, {"tmpfs", kindLocal},
		{"nfs", kindRemoteProbe}, {"nfs4", kindRemoteProbe}, {"cifs", kindRemoteProbe},
		{"fuse.sshfs", kindRemoteProbe}, {"fuse.somenovelnet", kindRemoteProbe}, {"nfsd-variant", kindRemoteProbe},
		{"lustre", kindRemoteProbe}, {"ceph", kindRemoteProbe},
		{"autofs", kindAutomount}, // review F10: guarded, but never probed/dropped
	}
	for _, c := range cases {
		if got := classifyFstype(c.fstype); got != c.want {
			t.Errorf("classifyFstype(%q)=%v want %v", c.fstype, got, c.want)
		}
	}
}

// --- PATH sanitization ------------------------------------------------------

func TestSanitizePATH_dropsUnhealthyHangable(t *testing.T) {
	fp := newFakeProbe()
	fp.ret["/shared"] = false // dead (statfs errors immediately)
	// /healthynfs default healthy.
	p := mustNew(t, Config{
		Mode:         ModeAuto,
		ProbeTimeout: 50 * time.Millisecond,
		MountSource:  mountSrc([2]string{"/shared", "nfs4"}, [2]string{"/healthynfs", "nfs"}, [2]string{"/", "ext4"}),
		Probe:        fp.fn,
	})
	raw := "/shared/bin:/healthynfs/bin:/usr/bin:/weird/bin"
	kept, dropped := p.sanitizePATH(raw)

	if len(dropped) != 1 || dropped[0] != "/shared/bin" {
		t.Fatalf("dropped=%v, want only /shared/bin", dropped)
	}
	// /healthynfs/bin kept (healthy hangable), /usr/bin kept (local),
	// /weird/bin kept (backed by "/" ext4 ⇒ unknown path ⇒ local).
	want := []string{"/healthynfs/bin", "/usr/bin", "/weird/bin"}
	for i, w := range want {
		if i >= len(kept) || kept[i] != w {
			t.Fatalf("kept[:3]=%v, want prefix %v", kept, want)
		}
	}
}

// --- headline: resolution never hangs on a dead PATH dir --------------------

func TestResolveArgv0_neverWalksDeadDir(t *testing.T) {
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "python")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	fp := newFakeProbe()
	deadBlock := make(chan bool) // /shared probe blocks forever (wedged mount)
	fp.block["/shared"] = deadBlock
	// Release the abandoned probe goroutine at the very end, THEN assert no leak
	// (review m5: the probe goroutines must be releasable + the count recovers).
	defer func() { close(deadBlock); assertGoroutinesReturn(t, before) }()

	p := mustNew(t, Config{
		Mode:         ModeAuto,
		ProbeTimeout: 60 * time.Millisecond,
		SafeDir:      tmp,
		MountSource:  mountSrc([2]string{"/shared", "nfs4"}, [2]string{"/", "ext4"}),
		Probe:        fp.fn,
	})
	baseEnv := []string{"PATH=/shared/nas/bin:" + tmp}

	// Bare name resolves to the LOCAL binary; the dead /shared/nas/bin is
	// dropped first (probe blocks → timeout) and never stat'd during resolution.
	// If resolution ever blocked on /shared, `go test -timeout` would fail.
	d, err := p.Prepare([]string{"python"}, "", envGet(baseEnv, "PATH"), baseEnv, false)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if !d.Active || d.Path != bin {
		t.Fatalf("resolved Path=%q active=%v, want %q active", d.Path, d.Active, bin)
	}
	if !strings.Contains(d.Warn, "/shared/nas/bin") {
		t.Errorf("warn should name the dropped dir, got %q", d.Warn)
	}

	// Explicit path on the dead mount fails fast — never silently rewritten to
	// the same-basename local binary.
	_, err = p.Prepare([]string{"/shared/nas/bin/python"}, "", envGet(baseEnv, "PATH"), baseEnv, false)
	var fe *FSError
	if !errors.As(err, &fe) || fe.Code != ReasonUnhealthy {
		t.Fatalf("explicit dead path: got %v, want remote_fs_unhealthy", err)
	}

	// Explicit LOCAL path Just Works even with a poisoned PATH.
	d, err = p.Prepare([]string{bin}, "", envGet(baseEnv, "PATH"), baseEnv, false)
	if err != nil || !d.Active || d.Path != bin {
		t.Fatalf("explicit local path: d=%+v err=%v", d, err)
	}
}

// --- probe: single-flight, sticky, self-healing -----------------------------

func TestStickyProbe_singleFlight_selfHeals(t *testing.T) {
	before := runtime.NumGoroutine()
	fp := newFakeProbe()
	block := make(chan bool)
	fp.block["/dead"] = block
	// If the probe goroutine is still blocked at the end (e.g. self-heal never
	// drained), release it so the leak gate can distinguish a real leak from the
	// expected single in-flight probe (review m5).
	released := false
	defer func() {
		if !released {
			close(block)
		}
		assertGoroutinesReturn(t, before)
	}()
	p := mustNew(t, Config{
		Mode:         ModeAuto,
		ProbeTimeout: 60 * time.Millisecond,
		MountSource:  mountSrc([2]string{"/dead", "nfs"}),
		Probe:        fp.fn,
	})

	const N = 8
	var wg sync.WaitGroup
	res := make([]bool, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); res[i] = p.mountHealthy("/dead") }(i)
	}
	wg.Wait()
	for i, r := range res {
		if r {
			t.Fatalf("caller %d saw healthy, want dead", i)
		}
	}
	if c := fp.count("/dead"); c != 1 {
		t.Fatalf("probe launched %d times, want 1 (single-flight)", c)
	}

	// Recover: the one outstanding probe goroutine returns success and exits.
	block <- true
	released = true

	healed := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.mountHealthy("/dead") {
			healed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !healed {
		t.Fatal("mount did not self-heal after late probe success")
	}
	if c := fp.count("/dead"); c != 1 {
		t.Fatalf("probe re-launched (%d); want sticky single probe", c)
	}
}

// --- watchdog: abandon + ceiling, no goroutine leak -------------------------

func TestRunStart_abandonsAndCeiling(t *testing.T) {
	p := mustNew(t, Config{
		Mode:         ModeAuto,
		WedgeCeiling: 2,
		MountSource:  func() ([]byte, error) { return nil, nil },
	})
	before := runtime.NumGoroutine()

	// Fast success path.
	if err := p.RunStart(func() error { return nil }, time.Second); err != nil {
		t.Fatalf("success RunStart: %v", err)
	}
	if p.WedgedCount() != 0 {
		t.Fatalf("wedged=%d after success, want 0", p.WedgedCount())
	}

	release := make(chan struct{})
	blocking := func() error { <-release; return nil }

	// Two abandons fill the ceiling.
	for i := 1; i <= 2; i++ {
		if err := p.RunStart(blocking, 40*time.Millisecond); !errors.Is(err, ErrSpawnTimeout) {
			t.Fatalf("abandon %d: got %v, want spawn timeout", i, err)
		}
		if p.WedgedCount() != i {
			t.Fatalf("wedged=%d after abandon %d, want %d", p.WedgedCount(), i, i)
		}
	}
	// Third is refused immediately (no new blocked goroutine).
	if err := p.RunStart(blocking, 40*time.Millisecond); !errors.Is(err, ErrTooManyWedged) {
		t.Fatalf("ceiling: got %v, want too_many_wedged", err)
	}

	// Release: abandoned starts return, reapers drain the wedged counter.
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && p.WedgedCount() != 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if p.WedgedCount() != 0 {
		t.Fatalf("wedged=%d after release, want 0", p.WedgedCount())
	}
	assertGoroutinesReturn(t, before)
}

// --- safe dir ---------------------------------------------------------------

func TestSafeDir_picksLocalWritable(t *testing.T) {
	fp := newFakeProbe()
	src := mountSrc([2]string{"/nfsmnt", "nfs"}, [2]string{"/", "ext4"})

	// Hangable override is rejected back to os.TempDir() (no probe — classified
	// hangable by string, never stat'd).
	p := mustNew(t, Config{Mode: ModeAuto, SafeDir: "/nfsmnt/sub", MountSource: src, Probe: fp.fn})
	if p.SafeDir() != os.TempDir() {
		t.Fatalf("hangable override accepted: %q", p.SafeDir())
	}
	if fp.count("/nfsmnt") != 0 {
		t.Fatalf("validSafeDir probed a hangable mount (%d); should refuse by string", fp.count("/nfsmnt"))
	}

	// Local writable override honored.
	tmp := t.TempDir()
	p2 := mustNew(t, Config{Mode: ModeAuto, SafeDir: tmp, MountSource: src, Probe: fp.fn})
	if p2.SafeDir() != tmp {
		t.Fatalf("local override not honored: %q", p2.SafeDir())
	}

	// Empty ⇒ os.TempDir().
	p3 := mustNew(t, Config{Mode: ModeAuto, MountSource: src, Probe: fp.fn})
	if p3.SafeDir() != os.TempDir() {
		t.Fatalf("empty default: %q", p3.SafeDir())
	}
}

// --- Prepare trigger semantics ---------------------------------------------

func TestPrepare_inertAndEscalation(t *testing.T) {
	tmp := t.TempDir()
	echo := filepath.Join(tmp, "echo")
	if err := os.WriteFile(echo, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	fp := newFakeProbe()

	// No hangable mounts ⇒ inert regardless of mode (byte-identical to today).
	pLocal := mustNew(t, Config{Mode: ModeAuto, MountSource: mountSrc([2]string{"/", "ext4"}), Probe: fp.fn})
	if d, _ := pLocal.Prepare([]string{"echo"}, "", envGet([]string{"PATH=" + tmp}, "PATH"), []string{"PATH=" + tmp}, false); d.Active {
		t.Fatal("all-local machine should be inert")
	}

	// Healthy hangable mount present but nothing dropped ⇒ Active (we self-resolve
	// + bound on a hangable machine, review F1/F2) but NOT an outage: byte-identical
	// output (resolved Path, no Env/Cwd/Warn rewrite).
	pHealthy := mustNew(t, Config{Mode: ModeAuto, MountSource: mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}), Probe: fp.fn})
	// tmp MUST come first in the resolve PATH: a later "/usr/bin" entry would
	// otherwise win on any host where /usr/bin/echo exists (it does on Linux),
	// resolving Path to /usr/bin/echo instead of our fake tmp/echo and failing
	// the byte-identical-Path assertion. (Pre-existing env-fragility; the
	// contract under test is "self-resolve picks the on-PATH binary", not which
	// dir wins.)
	d0, err0 := pHealthy.Prepare([]string{"echo"}, "", tmp+":/usr/bin", []string{"PATH=" + tmp}, false)
	if err0 != nil {
		t.Fatalf("healthy-hangable Prepare: %v", err0)
	}
	if !d0.Active || d0.Outage || d0.Path != echo || d0.Env != nil || d0.Warn != "" {
		t.Fatalf("healthy-hangable: want Active+!Outage+Path=echo+no rewrite, got %+v", d0)
	}

	// Off mode + a DEAD mount backing a PATH dir ⇒ inert without --safe, but
	// --safe escalates to active (the dead dir is dropped).
	deadFp := newFakeProbe()
	deadFp.ret["/nfs"] = false
	deadEnv := []string{"PATH=/nfs/bin:" + tmp}
	pOff := mustNew(t, Config{
		Mode: ModeOff, ProbeTimeout: 50 * time.Millisecond,
		MountSource: mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}), Probe: deadFp.fn,
	})
	if d, _ := pOff.Prepare([]string{"echo"}, "", envGet(deadEnv, "PATH"), deadEnv, false); d.Active {
		t.Fatal("off mode should be inert even with a dead mount")
	}
	d, err := pOff.Prepare([]string{"echo"}, "", envGet(deadEnv, "PATH"), deadEnv, true)
	if err != nil || !d.Active || d.Path != echo {
		t.Fatalf("--safe should escalate off→active on a real outage: d=%+v err=%v", d, err)
	}
}

func TestPrepare_cwdFailFastAndSubstitute(t *testing.T) {
	tmp := t.TempDir()
	echo := filepath.Join(tmp, "echo")
	if err := os.WriteFile(echo, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	fp := newFakeProbe()
	fp.ret["/nfs"] = false // dead
	p := mustNew(t, Config{
		Mode:         ModeAuto,
		ProbeTimeout: 50 * time.Millisecond,
		SafeDir:      tmp,
		MountSource:  mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}),
		Probe:        fp.fn,
	})

	// Explicit --cwd on a dead mount ⇒ fail fast.
	_, err := p.Prepare([]string{"echo"}, "/nfs/work", envGet([]string{"PATH=" + tmp}, "PATH"), []string{"PATH=" + tmp}, false)
	var fe *FSError
	if !errors.As(err, &fe) || fe.Code != ReasonUnsafeCwd {
		t.Fatalf("dead cwd: got %v, want remote_fs_unsafe_cwd", err)
	}

	// cwd unset + a dead PATH dir dropped ⇒ substitute safe dir.
	d, err := p.Prepare([]string{"echo"}, "", envGet([]string{"PATH=/nfs/bin:" + tmp}, "PATH"), []string{"PATH=/nfs/bin:" + tmp}, false)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if d.Cwd != tmp {
		t.Fatalf("cwd substitution: got %q, want safe dir %q", d.Cwd, tmp)
	}

	// cwd unset + nothing dropped (healthy) ⇒ cwd untouched ("").
	fp2 := newFakeProbe() // all healthy
	pHealthy := mustNew(t, Config{
		Mode:        ModeAuto,
		SafeDir:     tmp,
		MountSource: mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}),
		Probe:       fp2.fn,
	})
	d2, err := pHealthy.Prepare([]string{"echo"}, "", envGet([]string{"PATH=" + tmp}, "PATH"), []string{"PATH=" + tmp}, false)
	if err != nil {
		t.Fatalf("healthy Prepare: %v", err)
	}
	if d2.Cwd != "" {
		t.Fatalf("healthy op should leave cwd unset, got %q", d2.Cwd)
	}
}

func TestPrepare_noSafeDirDoesNotInjectEmptyPWD(t *testing.T) {
	fp := newFakeProbe()
	fp.ret["/nfs"] = false
	tempMount := filepath.Clean(os.TempDir())
	p := mustNew(t, Config{
		Mode:         ModeAuto,
		ProbeTimeout: 50 * time.Millisecond,
		MountSource: mountSrc(
			[2]string{"/nfs", "nfs"},
			[2]string{tempMount, "nfs"},
			[2]string{"/tmp", "nfs"},
			[2]string{"/var/tmp", "nfs"},
			[2]string{"/", "ext4"},
		),
		Probe:    fp.fn,
		Resolver: func(string, []string) (string, bool) { return "/bin/true", true },
	})
	d, err := p.Prepare([]string{"true"}, "", "/nfs/bin:/bin", []string{"PATH=/nfs/bin:/bin"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Outage || d.Cwd != "" {
		t.Fatalf("expected outage without cwd substitute, got %+v", d)
	}
	for _, entry := range d.Env {
		if strings.HasPrefix(entry, "PWD=") {
			t.Fatalf("empty safe dir must not inject PWD, env=%v", d.Env)
		}
	}
}

// TestMountHealthy_joinersWakeOnVerdict pins review m9: concurrent first-touch
// callers on a healthy mount all wake the instant the single probe returns, not
// a full probeTimeout later. probeTimeout is set to 5s; a correct done-broadcast
// makes every joiner return within ~ms of the verdict.
func TestMountHealthy_joinersWakeOnVerdict(t *testing.T) {
	before := runtime.NumGoroutine()
	fp := newFakeProbe()
	gate := make(chan bool, 1)
	fp.block["/nfs"] = gate
	p := mustNew(t, Config{
		Mode: ModeAuto, ProbeTimeout: 5 * time.Second,
		MountSource: mountSrc([2]string{"/nfs", "nfs"}), Probe: fp.fn,
	})
	const N = 6
	res := make(chan bool, N)
	for i := 0; i < N; i++ {
		go func() { res <- p.mountHealthy("/nfs") }()
	}
	time.Sleep(50 * time.Millisecond) // let the launcher + joiners park
	start := time.Now()
	gate <- true // probe returns healthy → launcher closes done → joiners wake
	for i := 0; i < N; i++ {
		select {
		case ok := <-res:
			if !ok {
				t.Error("a caller saw unhealthy on a healthy mount")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("a joiner did not wake within 2s of the verdict (probeTimeout=5s) — done broadcast missing")
		}
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Errorf("joiners took %s to wake (probeTimeout was 5s); broadcast should be near-instant", el)
	}
	assertGoroutinesReturn(t, before)
}

// TestAutofs_longestPrefixKeepsHealthySubmount pins review n1: an autofs parent
// (treated as dead-by-string, never probed) must NOT cause a healthy NFS
// submount's PATH dir to be dropped — longest-prefix matching binds the dir to
// the deeper probed submount, not the autofs parent.
func TestAutofs_longestPrefixKeepsHealthySubmount(t *testing.T) {
	fp := newFakeProbe() // /home/u healthy (default true)
	p := mustNew(t, Config{
		Mode: ModeAuto, ProbeTimeout: 50 * time.Millisecond,
		MountSource: mountSrc([2]string{"/home", "autofs"}, [2]string{"/home/u", "nfs"}, [2]string{"/", "ext4"}),
		Probe:       fp.fn,
	})
	kept, dropped := p.sanitizePATH("/home/u/.local/bin:/usr/bin")
	if len(dropped) != 0 {
		t.Fatalf("healthy NFS submount under autofs must be KEPT, dropped=%v", dropped)
	}
	if kept[0] != "/home/u/.local/bin" {
		t.Fatalf("kept order wrong: %v", kept)
	}
	if fp.count("/home") != 0 {
		t.Errorf("autofs parent must never be probed, got %d", fp.count("/home"))
	}
}

// TestBoundedResolveInDirs_timeoutBounded pins review F1: a resolution that
// blocks (e.g. os.Stat following a symlink into a dead mount) is abandoned at the
// deadline and surfaced as remote_fs_spawn_timeout — never an unbounded hang. The
// abandoned goroutine is releasable (no leak).
func TestBoundedResolveInDirs_timeoutBounded(t *testing.T) {
	before := runtime.NumGoroutine()
	release := make(chan struct{})
	blocking := func(string, []string) (string, bool) { <-release; return "", false }
	p := mustNew(t, Config{
		Mode: ModeAuto, ProbeTimeout: 50 * time.Millisecond, WedgeCeiling: 4,
		MountSource: mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}),
		Probe:       newFakeProbe().fn, Resolver: blocking,
	})
	defer func() { close(release); assertGoroutinesReturn(t, before) }()
	_, err := p.boundedResolveInDirs("x", []string{"/usr/bin"})
	var fe *FSError
	if !errors.As(err, &fe) || fe.Code != ReasonSpawnTimeout {
		t.Fatalf("blocking resolution must bound to remote_fs_spawn_timeout, got %v", err)
	}
}

func TestBoundedResolveInDirs_preservesErrDot(t *testing.T) {
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "tool"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(tmp); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })

	p := mustNew(t, Config{
		Mode: ModeAuto, ProbeTimeout: 50 * time.Millisecond,
		MountSource: mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}),
		Probe:       newFakeProbe().fn,
	})
	_, err = p.Prepare([]string{"tool"}, "", ":", nil, false)
	if !errors.Is(err, exec.ErrDot) {
		t.Fatalf("empty PATH element/current-dir lookup must retain exec.ErrDot, got %v", err)
	}
}

func TestPrepare_wedgeCeilingOneDoesNotRejectHealthyStart(t *testing.T) {
	p := mustNew(t, Config{
		Mode: ModeAuto, ProbeTimeout: 50 * time.Millisecond, WedgeCeiling: 1,
		MountSource: mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}),
		Probe:       newFakeProbe().fn,
		Resolver:    func(string, []string) (string, bool) { return "/bin/true", true },
	})
	d, err := p.Prepare([]string{"true"}, "", "/bin", nil, false)
	if err != nil || !d.Active {
		t.Fatalf("healthy resolution failed: d=%+v err=%v", d, err)
	}
	if err := p.RunStart(func() error { return nil }, time.Second); err != nil {
		t.Fatalf("resolver must release its slot before publishing completion: %v", err)
	}
}

// TestPrepare_resolvesAgainstLookupPATHNotChildEnv pins review F2: the active
// trigger + argv[0] resolution use the AGENT (lookup) PATH that LookPath would
// walk — NOT the child env's PATH. A dead dir in the lookup PATH drives the
// outage even when the child env PATH is clean.
func TestPrepare_resolvesAgainstLookupPATHNotChildEnv(t *testing.T) {
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "mytool"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	fp := newFakeProbe()
	fp.ret["/nfs"] = false
	p := mustNew(t, Config{
		Mode: ModeAuto, ProbeTimeout: 50 * time.Millisecond,
		MountSource: mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}), Probe: fp.fn,
	})
	d, err := p.Prepare([]string{"mytool"}, "", "/nfs/bin:"+tmp, []string{"PATH=/usr/bin"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Active || !d.Outage {
		t.Fatalf("dead LOOKUP PATH dir ⇒ active outage, got %+v", d)
	}
	if d.Path != filepath.Join(tmp, "mytool") {
		t.Errorf("resolved against the lookup PATH (not childEnv): got %q", d.Path)
	}
}

// TestProbe_survivesUnrelatedMountChurn pins review F4: an unrelated mount-table
// change must not discard a dead mount's verdict and re-probe it. /dead is
// constant while an unrelated /bindN churns; /dead must be probed exactly once.
func TestProbe_survivesUnrelatedMountChurn(t *testing.T) {
	before := runtime.NumGoroutine()
	fp := newFakeProbe()
	fp.ret["/dead"] = false
	var n atomic.Int64
	src := func() ([]byte, error) {
		return fakeMountinfo([2]string{"/dead", "nfs"},
			[2]string{fmt.Sprintf("/bind%d", n.Add(1)), "ext4"}, [2]string{"/", "ext4"}), nil
	}
	p := mustNew(t, Config{Mode: ModeAuto, ProbeTimeout: 50 * time.Millisecond, MountSource: src, Probe: fp.fn})
	for i := 0; i < 12; i++ {
		_, _ = p.Prepare([]string{"x"}, "", "/dead/bin:/usr/bin", nil, true) // --safe ⇒ refresh each spawn
	}
	if c := fp.count("/dead"); c != 1 {
		t.Fatalf("/dead re-probed %d times under unrelated mount churn; want 1 (F4)", c)
	}
	assertGoroutinesReturn(t, before)
}

func TestProbe_resetsWhenMountInstanceChanges(t *testing.T) {
	fp := newFakeProbe()
	fp.ret["/dead"] = false
	var raw atomic.Value
	raw.Store([]byte("40 35 0:1 / /dead rw - nfs srv:/old rw\n36 35 0:2 / / rw - ext4 root rw\n"))
	src := func() ([]byte, error) { return raw.Load().([]byte), nil }
	p := mustNew(t, Config{Mode: ModeAuto, ProbeTimeout: 50 * time.Millisecond, MountSource: src, Probe: fp.fn})

	_, _ = p.Prepare([]string{"x"}, "", "/dead/bin:/usr/bin", nil, true)
	raw.Store([]byte("77 35 0:9 / /dead rw - nfs srv:/new rw\n36 35 0:2 / / rw - ext4 root rw\n"))
	_, _ = p.Prepare([]string{"x"}, "", "/dead/bin:/usr/bin", nil, true)
	if c := fp.count("/dead"); c != 2 {
		t.Fatalf("replacement mount inherited stale sticky verdict: probes=%d want 2", c)
	}
}

// TestPrepare_bareAutofsHealthyNotDropped pins review F10: a PATH dir under an
// un-triggered autofs parent (no submount yet) must NOT be dropped and autofs
// must never be probed. It still activates bounded resolution/start, otherwise
// an autofs-only machine would fall back to unbounded exec.Command LookPath.
func TestPrepare_bareAutofsHealthyNotDropped(t *testing.T) {
	fp := newFakeProbe()
	p := mustNew(t, Config{
		Mode: ModeAuto, ProbeTimeout: 50 * time.Millisecond,
		MountSource: mountSrc([2]string{"/auto", "autofs"}, [2]string{"/", "ext4"}), Probe: fp.fn,
		Resolver: func(string, []string) (string, bool) { return "/auto/home/bin/tool", true },
	})
	_, dropped := p.sanitizePATH("/auto/home/bin:/usr/bin")
	if len(dropped) != 0 {
		t.Fatalf("autofs PATH dir must NOT be dropped, dropped=%v", dropped)
	}
	if fp.count("/auto") != 0 {
		t.Errorf("autofs must never be probed, got %d", fp.count("/auto"))
	}
	d, err := p.Prepare([]string{"tool"}, "", "/auto/home/bin:/usr/bin", nil, false)
	if err != nil || !d.Active || d.Outage {
		t.Fatalf("autofs-only machine must use bounded non-outage spawn: d=%+v err=%v", d, err)
	}
}

func TestRunStartWithCleanup_reapsSuccessfulLateStart(t *testing.T) {
	p := mustNew(t, Config{
		Mode: ModeAuto, ProbeTimeout: 20 * time.Millisecond,
		MountSource: mountSrc([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}),
		Probe:       newFakeProbe().fn,
	})
	release := make(chan struct{})
	abandoned := make(chan struct{})
	reaped := make(chan error, 1)
	err := p.RunStartWithCleanup(
		func() error { <-release; return nil },
		20*time.Millisecond,
		func() { close(abandoned) },
		func(err error) { reaped <- err },
	)
	if !errors.Is(err, ErrSpawnTimeout) {
		t.Fatalf("got %v, want spawn timeout", err)
	}
	select {
	case <-abandoned:
	default:
		t.Fatal("onAbandon was not called before returning timeout")
	}
	close(release)
	select {
	case err := <-reaped:
		if err != nil {
			t.Fatalf("late successful start callback got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("late start was not reaped")
	}
	deadline := time.Now().Add(time.Second)
	for p.WedgedCount() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := p.WedgedCount(); got != 0 {
		t.Fatalf("wedge slot not released after cleanup: %d", got)
	}
}

// TestMountForPath_stackedTopmost pins review F11: for stacked mounts at the same
// mountpoint, the topmost (last in mountinfo) is the real backing mount.
func TestMountForPath_stackedTopmost(t *testing.T) {
	local2remote := []byte(strings.Join([]string{
		"40 35 0:1 / /data rw - ext4 /dev/sdb rw",
		"41 35 0:2 / /data rw - nfs srv:/data rw", // overmount ⇒ topmost
		"36 35 0:3 / / rw - ext4 /dev/sda rw",
	}, "\n"))
	p := mustNew(t, Config{MountSource: func() ([]byte, error) { return local2remote, nil }, Probe: newFakeProbe().fn})
	if m := p.mountForPath("/data/x"); m.fstype != "nfs" {
		t.Fatalf("stacked /data topmost must be the nfs overmount, got %q", m.fstype)
	}
	remote2local := []byte(strings.Join([]string{
		"40 35 0:1 / /data rw - nfs srv:/data rw",
		"41 35 0:2 / /data rw - ext4 /dev/sdb rw", // overmount ⇒ topmost local
		"36 35 0:3 / / rw - ext4 /dev/sda rw",
	}, "\n"))
	p2 := mustNew(t, Config{MountSource: func() ([]byte, error) { return remote2local, nil }, Probe: newFakeProbe().fn})
	if m := p2.mountForPath("/data/x"); m.fstype != "ext4" {
		t.Fatalf("stacked /data topmost must be the ext4 overmount, got %q", m.fstype)
	}
}

// TestNew_rejectsBadConfig pins review F12: negative ceiling/timeout and a
// relative safe_dir fail loud at startup; and review F8: a network-mount safe_dir
// override is rejected (not silently stored).
func TestNew_rejectsBadConfig(t *testing.T) {
	local := mountSrc([2]string{"/", "ext4"})
	if _, err := New(Config{WedgeCeiling: -1, MountSource: local}); err == nil {
		t.Error("negative wedge_ceiling must fail New")
	}
	if _, err := New(Config{ProbeTimeout: -1, MountSource: local}); err == nil {
		t.Error("negative probe_timeout must fail New")
	}
	if _, err := New(Config{SafeDir: "relative/x", MountSource: local}); err == nil {
		t.Error("relative safe_dir must fail New")
	}
	if _, err := New(Config{HealthTTL: -1, MountSource: local}); err == nil {
		t.Error("negative HealthTTL must fail New")
	}
	// Config errors may only name knobs an operator can actually set. HealthTTL has no
	// agent.yaml surface (this batch stays key-free so rollback is a pure binary swap),
	// so spelling it like one would send someone hunting for a key that does not exist.
	for _, c := range []Config{
		{WedgeCeiling: -1, MountSource: local},
		{ProbeTimeout: -1, MountSource: local},
		{SafeDir: "relative/x", MountSource: local},
		{HealthTTL: -1, MountSource: local},
	} {
		_, err := New(c)
		if err == nil {
			continue
		}
		if strings.Contains(err.Error(), "remote_fs.health_ttl") {
			t.Errorf("config error names a non-existent agent.yaml key: %v", err)
		}
	}
	// Network-mount override ⇒ rejected, never stored.
	p := mustNew(t, Config{
		Mode: ModeAuto, SafeDir: "/nfsmnt/sub",
		MountSource: mountSrc([2]string{"/nfsmnt", "nfs"}, [2]string{"/", "ext4"}), Probe: newFakeProbe().fn,
	})
	if p.SafeDir() == "/nfsmnt/sub" {
		t.Errorf("network safe_dir override must be rejected, got %q", p.SafeDir())
	}
}

func TestParseMode(t *testing.T) {
	for _, c := range []struct {
		in   string
		want Mode
		err  bool
	}{
		{"", ModeAuto, false}, {"auto", ModeAuto, false}, {"off", ModeOff, false},
		{" auto ", ModeAuto, false}, {"AUTO", ModeAuto, true}, {"yes", ModeAuto, true},
	} {
		got, err := ParseMode(c.in)
		if (err != nil) != c.err {
			t.Errorf("ParseMode(%q) err=%v want err=%v", c.in, err, c.err)
		}
		if !c.err && got != c.want {
			t.Errorf("ParseMode(%q)=%v want %v", c.in, got, c.want)
		}
	}
}

// --- stale-healthy re-validation (gotcha #81) --------------------------------
//
// Everything below covers the healthy→dead transition. Before this batch the
// package had 25 cases and every one of them made the mount dead at its FIRST
// probe, so the terminal-stHealthy bug was invisible to all of them.

// staleFixture is the timan107 shape: a hangable mount that is healthy when first
// probed, holding the first two entries of the agent's $PATH.
type staleFixture struct {
	clock  *fakeClock
	probe  *fakeProbe
	policy *Policy
	// resolved counts boundedResolveInDirs entries; blockResolve, when non-nil,
	// makes resolution outlast the deadline (a wedged stat inside a $PATH dir).
	// It is mutex-guarded because boundedResolveInDirs reads it from the resolver
	// goroutine it abandons, which outlives the call that set it.
	resolved     int32
	mu           sync.Mutex
	blockResolve chan struct{}
}

func (f *staleFixture) setResolveBlock(ch chan struct{}) {
	f.mu.Lock()
	f.blockResolve = ch
	f.mu.Unlock()
}

func (f *staleFixture) resolveBlock() chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.blockResolve
}

const staleMount = "/shared"
const stalePATH = "/shared/bin:/usr/bin"

func newStaleFixture(t *testing.T, ttl time.Duration, probeTimeout time.Duration) *staleFixture {
	t.Helper()
	f := &staleFixture{clock: newFakeClock(), probe: newFakeProbe()}
	f.policy = mustNew(t, Config{
		Mode:         ModeAuto,
		ProbeTimeout: probeTimeout,
		HealthTTL:    ttl,
		MountSource:  mountSrc([2]string{staleMount, "nfs4"}, [2]string{"/", "ext4"}),
		Probe:        f.probe.fn,
		Now:          f.clock.now,
		Resolver: func(name string, dirs []string) (string, bool) {
			atomic.AddInt32(&f.resolved, 1)
			if ch := f.resolveBlock(); ch != nil {
				<-ch
			}
			return "/usr/bin/" + name, true
		},
	})
	return f
}

func (f *staleFixture) prepare(t *testing.T, argv []string, cwd string, safe bool) (Decision, error) {
	t.Helper()
	return f.policy.Prepare(argv, cwd, stalePATH, []string{"PATH=" + stalePATH}, safe)
}

// TestPrepare_staleHealthyMountDroppedAfterSpawnTimeout is the head-line case: it
// replays timan107 end to end and asserts the agent now learns from the timeout.
//
// The clock never advances, so a pass here is the EVIDENCE path alone — the TTL
// cannot quietly rescue it.
//
// origin: docs/deploy-tier-gotchas.md #81 (timan107, 2026-08-29)
func TestPrepare_staleHealthyMountDroppedAfterSpawnTimeout(t *testing.T) {
	f := newStaleFixture(t, time.Hour, 40*time.Millisecond)

	// (1) Mount alive: the verdict gets cached, nothing is dropped, and — the
	// detail that made #81 so hard to see — no warning is emitted at all.
	d, err := f.prepare(t, []string{"echo"}, "", false)
	if err != nil {
		t.Fatalf("healthy prepare: %v", err)
	}
	if d.Outage || d.Warn != "" {
		t.Fatalf("healthy mount must not look like an outage: outage=%v warn=%q", d.Outage, d.Warn)
	}

	// (2) The mount dies. Resolution now wedges inside /shared/bin, exactly as the
	// 2.14s remote_fs_spawn_timeout did on timan107.
	f.probe.setVerdict(staleMount, false)
	block := make(chan struct{})
	f.setResolveBlock(block)
	_, err = f.prepare(t, []string{"echo"}, "", false)
	var fe *FSError
	if !errors.As(err, &fe) || fe.Code != ReasonSpawnTimeout {
		t.Fatalf("wedged resolution: got %v, want %s", err, ReasonSpawnTimeout)
	}
	f.setResolveBlock(nil)
	close(block) // let the abandoned resolver goroutine exit

	// (3) THE assertion. Before this fix the healthy verdict was terminal, so this
	// call was byte-identical to (1) forever. It must now re-probe and drop the dir.
	d, err = f.prepare(t, []string{"echo"}, "", false)
	if err != nil {
		t.Fatalf("post-evidence prepare: %v", err)
	}
	if !d.Outage {
		t.Fatal("a spawn timeout must expire the cached healthy verdict; got outage=false (this is the #81 bug)")
	}
	if !strings.Contains(d.Warn, "/shared/bin") {
		t.Errorf("warn must name the dropped dir, got %q", d.Warn)
	}
	if got := envGet(d.Env, "PATH"); strings.Contains(got, "/shared/bin") {
		t.Errorf("child PATH still contains the dead dir: %q", got)
	}
}

// TestPrepare_staleHealthyRevalidatesViaBlockingProbe forces the re-probe through
// the launcher's TIMEOUT branch rather than a fast false.
//
// This is the branch production actually takes — a statfs on a wedged hard mount
// D-hangs, it does not return false — and it is the branch where an in-place
// re-arm would either panic on close-of-closed or have its demotion refused by
// the launcher's `state == stUnprobed` guard.
//
// origin: docs/deploy-tier-gotchas.md #81 (timan107, 2026-08-29)
func TestPrepare_staleHealthyRevalidatesViaBlockingProbe(t *testing.T) {
	before := runtime.NumGoroutine()
	f := newStaleFixture(t, time.Hour, 40*time.Millisecond)
	stuck := make(chan bool)
	released := false
	defer func() {
		if !released {
			close(stuck)
		}
		assertGoroutinesReturn(t, before)
	}()

	if d, err := f.prepare(t, []string{"echo"}, "", false); err != nil || d.Outage {
		t.Fatalf("healthy prepare: d=%+v err=%v", d, err)
	}

	// The mount wedges: from here every statfs blocks forever.
	f.probe.setBlocking(staleMount, stuck)

	block := make(chan struct{})
	f.setResolveBlock(block)
	if _, err := f.prepare(t, []string{"echo"}, "", false); err == nil {
		t.Fatal("wedged resolution must fail")
	}
	f.setResolveBlock(nil)
	close(block)

	d, err := f.prepare(t, []string{"echo"}, "", false)
	if err != nil {
		t.Fatalf("post-evidence prepare: %v", err)
	}
	if !d.Outage {
		t.Fatal("re-probe through the launcher timeout branch must demote the mount")
	}
	f.probe.waitCount(t, staleMount, 2)
	close(stuck)
	released = true
}

// TestMountHealthy_healthyVerdictExpiresAfterTTL pins both halves of T11: the
// verdict does expire, and within the TTL the fast path stays truly zero-syscall.
func TestMountHealthy_healthyVerdictExpiresAfterTTL(t *testing.T) {
	const ttl = time.Minute
	f := newStaleFixture(t, ttl, 40*time.Millisecond)

	if !f.policy.mountHealthy(staleMount) {
		t.Fatal("first probe should report healthy")
	}
	if c := f.probe.count(staleMount); c != 1 {
		t.Fatalf("probe count %d after first consult, want 1", c)
	}

	f.clock.advance(ttl - time.Nanosecond)
	for i := 0; i < 50; i++ {
		if !f.policy.mountHealthy(staleMount) {
			t.Fatalf("consult %d inside the TTL must use the cached verdict", i)
		}
	}
	if c := f.probe.count(staleMount); c != 1 {
		t.Fatalf("probe count %d after 50 consults inside the TTL, want 1 (the fast path regressed)", c)
	}

	f.clock.advance(time.Nanosecond)
	f.probe.setVerdict(staleMount, false)
	if f.policy.mountHealthy(staleMount) {
		t.Fatal("verdict must be re-probed once the TTL elapsed")
	}
	if c := f.probe.count(staleMount); c != 2 {
		t.Fatalf("probe count %d after expiry, want exactly 2", c)
	}
}

// TestMountHealthy_deadVerdictStaysStickyThroughTTLAndInvalidation is the
// mechanical form of the hard constraint: re-probing a dead mount would leak
// another permanent D-state thread, and probes take no wedge slot, so a widened
// re-validation guard is strictly worse than the bug it would be fixing.
//
// probeTimeout is microseconds on purpose: a widened guard must show up as a
// probe count, not as a test that times out somewhere else.
func TestMountHealthy_deadVerdictStaysStickyThroughTTLAndInvalidation(t *testing.T) {
	const ttl = time.Minute
	f := newStaleFixture(t, ttl, 200*time.Microsecond)
	f.probe.setVerdict(staleMount, false)

	if f.policy.mountHealthy(staleMount) {
		t.Fatal("want dead")
	}
	for i := 0; i < 3; i++ {
		f.clock.advance(10 * ttl)
		f.policy.InvalidateHealthy()
		for j := 0; j < 100; j++ {
			if f.policy.mountHealthy(staleMount) {
				t.Fatal("a dead verdict must never flip without a late probe success")
			}
		}
	}
	if c := f.probe.count(staleMount); c != 1 {
		t.Fatalf("dead mount was re-probed %d times; sticky-dead is a hard constraint", c)
	}
}

// TestMountHealthy_reprobeNeverDoublesInFlightProbes is the only real guard on the
// D-state thread budget, and its setting matters: it must be a SLOW-BUT-HEALTHY
// mount oscillating, not a dead one. On a dead mount the sticky branch returns
// before any re-arm can happen, so the mutation this test claims to catch would be
// unreachable there and the test would be an identity.
func TestMountHealthy_reprobeNeverDoublesInFlightProbes(t *testing.T) {
	before := runtime.NumGoroutine()
	f := newStaleFixture(t, time.Hour, 30*time.Millisecond)
	slow := make(chan bool, 4)
	f.probe.setBlocking(staleMount, slow)
	defer func() {
		close(slow)
		assertGoroutinesReturn(t, before)
	}()

	// Probe outlives the deadline ⇒ demoted, its goroutine still in flight.
	if f.policy.mountHealthy(staleMount) {
		t.Fatal("a probe that misses the deadline must read as dead for now")
	}
	// It returns healthy late ⇒ self-heal back to stHealthy (T7).
	slow <- true
	healed := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if f.policy.mountHealthy(staleMount) {
			healed = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !healed {
		t.Fatal("late probe success must self-heal the mount")
	}
	probesAfterHeal := f.probe.count(staleMount)

	// Now hammer it: one invalidation plus 200 concurrent consults must produce
	// exactly ONE new probe, not one per consult.
	f.policy.InvalidateHealthy()
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); f.policy.mountHealthy(staleMount) }()
	}
	wg.Wait()
	if got := f.probe.count(staleMount) - probesAfterHeal; got != 1 {
		t.Fatalf("re-arm launched %d probes for 200 consults, want exactly 1 (single-flight broke ⇒ unbounded D-state threads)", got)
	}
}

// TestMountHealthy_reArmReplacesGenerationPointer is the DETERMINISTIC guard on the
// design decision that re-arming replaces the *mountHealth rather than resetting it
// in place. Its concurrent sibling below can only catch that by winning a race; this
// one cannot miss.
//
// origin: docs/reviews/remote-fs-stale-health-review.md F-2 (the internal review found
// the concurrent test alone was an identity test)
func TestMountHealthy_reArmReplacesGenerationPointer(t *testing.T) {
	const ttl = time.Minute
	f := newStaleFixture(t, ttl, 40*time.Millisecond)

	seed := func() *mountHealth {
		t.Helper()
		if !f.policy.mountHealthy(staleMount) {
			t.Fatal("want healthy")
		}
		f.policy.mu.Lock()
		defer f.policy.mu.Unlock()
		return f.policy.health[staleMount]
	}
	current := func() *mountHealth {
		f.policy.mu.Lock()
		defer f.policy.mu.Unlock()
		return f.policy.health[staleMount]
	}
	// An orphaned generation must keep its own verdict: a late launcher writing into
	// it is exactly what must NOT reach the live entry.
	assertOrphaned := func(old *mountHealth, via string) {
		t.Helper()
		f.policy.mu.Lock()
		defer f.policy.mu.Unlock()
		if old.state != stHealthy || !old.launched {
			t.Errorf("%s: the orphaned generation was mutated (state=%d launched=%v); "+
				"a stale launcher must write to a struct nobody reads, not to the live one",
				via, old.state, old.launched)
		}
	}

	// T10 — evidence-driven invalidation.
	old := seed()
	f.policy.InvalidateHealthy()
	if current() == old {
		t.Fatal("T10 re-armed IN PLACE: the generation pointer must be REPLACED " +
			"(in-place reset lets a late launcher demote the fresh generation, and " +
			"close-of-closed on a swapped done channel panics the agent)")
	}
	assertOrphaned(old, "T10")

	// T11 — the same obligation on the TTL path.
	old = seed()
	f.clock.advance(ttl)
	if !f.policy.mountHealthy(staleMount) { // consult drives the TTL re-arm
		t.Fatal("want healthy after the TTL re-probe")
	}
	if current() == old {
		t.Fatal("T11 re-armed IN PLACE: same obligation as T10")
	}
	assertOrphaned(old, "T11")
}

// TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup runs the three-way race
// the re-arm design exists to make impossible: a launcher waking up against a
// generation that was replaced while it waited.
//
// In-place re-arm fails here two different ways — close-of-closed on the swapped
// done channel, or a stale launcher writing sticky-dead over a fresh generation.
// Historical precedent (external review F6) needed -count=1000 to surface a
// cousin of this, hence the repetitions.
//
// TWO THINGS THIS TEST GOT WRONG THE FIRST TIME (internal review F-2, both measured):
//   - `close(stop)` sat before the consult workers' Wait, so the invalidator ran for
//     ~1ms against 13ms of consults — 0-3 re-arms total. There was almost no churn to
//     survive. The workers now have their own WaitGroup and the invalidator is stopped
//     only after they finish.
//   - Nothing asserted that churn HAPPENED, so the test stayed green even with
//     invalidateHealthy replaced by a no-op — it degenerated into "hammer mountHealthy".
//     The rearms counter below closes that.
//
// The probe blocks briefly so launchers actually reach their `time.After` arm; with an
// instantly-returning probe the "stale launcher" state this test is named for was never
// entered at all.
func TestMountHealthy_reArmSurvivesConcurrentLauncherWakeup(t *testing.T) {
	before := runtime.NumGoroutine()
	f := newStaleFixture(t, time.Hour, 20*time.Millisecond)
	slow := make(chan bool)
	f.probe.setBlocking(staleMount, slow)
	// The feeder OWNS slow, including its close: having the deferred cleanup close it
	// too raced with an in-flight send (found by -race on the first run).
	feeding := make(chan struct{})
	feedDone := make(chan struct{})
	go func() { // keep answering "healthy, but slowly"
		defer close(feedDone)
		for {
			select {
			case <-feeding:
				close(slow) // release any probe still parked, with ok=false
				return
			case <-time.After(250 * time.Microsecond):
			}
			select {
			case <-feeding:
				close(slow)
				return
			case slow <- true:
			}
		}
	}()
	defer func() {
		close(feeding)
		<-feedDone
		assertGoroutinesReturn(t, before)
	}()

	stop := make(chan struct{})
	var invalidator, workers sync.WaitGroup
	var rearms int64
	invalidator.Add(1)
	go func() {
		defer invalidator.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}

			// Count real generation replacements, not calls that happened while
			// the only live generation was still unprobed. A healthy generation
			// cannot change until this goroutine invalidates it (the fake clock is
			// fixed and every probe returns true), so the check and call form a
			// reliable test handshake without adding a production-only hook.
			f.policy.mu.Lock()
			h := f.policy.health[staleMount]
			healthy := h != nil && h.state == stHealthy
			f.policy.mu.Unlock()
			if !healthy {
				runtime.Gosched()
				continue
			}
			f.policy.InvalidateHealthy()
			atomic.AddInt64(&rearms, 1)
		}
	}()
	for i := 0; i < 16; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 200; j++ {
				f.policy.mountHealthy(staleMount)
			}
		}()
	}
	workers.Wait() // the invalidator must outlive the consults, not the reverse
	close(stop)
	invalidator.Wait()

	if n := atomic.LoadInt64(&rearms); n < 5 {
		t.Fatalf("only %d healthy generations were re-armed against 3200 consults — there was no churn "+
			"to survive, so a pass here proves nothing", n)
	}
	// Probes are the observable proof that those generations were really replaced. At
	// most the final re-arm can be left without a following consult, so the initial
	// probe plus the completed re-arms must keep the count at least this high. This
	// also makes replacing InvalidateHealthy with a no-op fail loudly.
	if n, c := atomic.LoadInt64(&rearms), int64(f.probe.count(staleMount)); c < n {
		t.Fatalf("mount was probed %d times across %d real re-arms: generation replacement "+
			"did not drive a fresh probe", c, n)
	}
	// A healthy mount must never end up stuck dead just because generations were
	// churning underneath the launchers.
	f.clock.advance(2 * time.Hour)
	if !f.policy.mountHealthy(staleMount) {
		t.Fatal("healthy mount was demoted by re-arm churn alone")
	}
}

// TestPrepare_safeForcesRevalidation pins the product promise in usage.md §7.7.
// --safe used to refresh only the mount TABLE, which on timan107 was already
// correct — so --safe behaved exactly like no flag, which is what the field
// report said.
func TestPrepare_safeForcesRevalidation(t *testing.T) {
	f := newStaleFixture(t, time.Hour, 40*time.Millisecond)

	if d, err := f.prepare(t, []string{"echo"}, "", false); err != nil || d.Outage {
		t.Fatalf("healthy prepare: d=%+v err=%v", d, err)
	}
	f.probe.setVerdict(staleMount, false)

	// Control: without --safe and without evidence, the cached verdict stands.
	// (If this ever fails, someone made every spawn re-probe.)
	if d, err := f.prepare(t, []string{"echo"}, "", false); err != nil || d.Outage {
		t.Fatalf("ordinary prepare must keep using the cached verdict: d=%+v err=%v", d, err)
	}
	if c := f.probe.count(staleMount); c != 1 {
		t.Fatalf("probe count %d without --safe, want 1", c)
	}

	d, err := f.prepare(t, []string{"echo"}, "", true)
	if err != nil {
		t.Fatalf("--safe prepare: %v", err)
	}
	if !d.Outage {
		t.Fatal("--safe must expire cached healthy verdicts and re-probe")
	}
	if c := f.probe.count(staleMount); c != 2 {
		t.Fatalf("probe count %d after --safe, want 2", c)
	}

	// --safe must not resurrect a dead mount either (sticky-dead is unconditional).
	if _, err := f.prepare(t, []string{"echo"}, "", true); err != nil {
		t.Fatalf("second --safe prepare: %v", err)
	}
	if c := f.probe.count(staleMount); c != 2 {
		t.Fatalf("--safe re-probed a dead mount (count %d); sticky-dead must hold", c)
	}
}

// TestPrepare_safeInvalidatesBeforeCwdCheck pins the ORDER, which is load-bearing and
// was unguarded: moving the --safe invalidation below the lexical cwd check left every
// existing test green while silently removing the escape hatch it exists to provide.
//
// pathOnDeadMount only re-probes an entry that is stUnprobed, so unless the verdict is
// expired FIRST, the very first `--safe --cwd <stale-healthy dead mount>` sails past the
// lexical check and pays the full 30s execve watchdog instead of failing fast.
//
// origin: docs/reviews/remote-fs-stale-health-review.md F-3
func TestPrepare_safeInvalidatesBeforeCwdCheck(t *testing.T) {
	f := newStaleFixture(t, time.Hour, 40*time.Millisecond)

	if _, err := f.prepare(t, []string{"true"}, "", false); err != nil {
		t.Fatalf("seed healthy: %v", err)
	}
	f.probe.setVerdict(staleMount, false)

	// Clock frozen and no stall anywhere: --safe is the ONLY thing that can expire the
	// verdict here, so this is a clean test of the flag rather than of the TTL.
	resolvedBefore := atomic.LoadInt32(&f.resolved)
	_, err := f.prepare(t, []string{"true"}, staleMount+"/nas", true)
	var fe *FSError
	if !errors.As(err, &fe) || fe.Code != ReasonUnsafeCwd {
		t.Fatalf("first --safe with a dead cwd: got %v, want %s", err, ReasonUnsafeCwd)
	}
	if n := atomic.LoadInt32(&f.resolved) - resolvedBefore; n != 0 {
		t.Errorf("cwd fail-fast must precede argv[0] resolution; resolution ran %d times", n)
	}
}

// TestRunStartWithCleanup_reapsBeforeReleasingTheWedgeSlot is the equivalence guard the
// plan made a hard prerequisite for collapsing the agent's duplicate watchdog into this
// one (plan §-1 OQ-1). The ordering it pins — reap the abandoned child, THEN free the
// slot — is what stops a long outage from handing out slots faster than wedged spawns
// are reclaimed; swapping the two lines leaves every other test in the tree green.
//
// origin: docs/reviews/remote-fs-stale-health-review.md F-7
func TestRunStartWithCleanup_reapsBeforeReleasingTheWedgeSlot(t *testing.T) {
	p := mustNew(t, Config{
		Mode:         ModeAuto,
		WedgeCeiling: 1,
		MountSource:  mountSrc([2]string{"/", "ext4"}),
		Probe:        newFakeProbe().fn,
	})
	release := make(chan struct{})
	seen := make(chan int, 1)
	err := p.RunStartWithCleanup(
		func() error { <-release; return nil },
		20*time.Millisecond,
		nil,
		func(error) { seen <- p.WedgedCount() },
	)
	if !errors.Is(err, ErrSpawnTimeout) {
		t.Fatalf("want spawn timeout, got %v", err)
	}
	close(release)
	select {
	case got := <-seen:
		if got != 1 {
			t.Errorf("reapOnReturn saw WedgedCount=%d, want 1: the slot must still be held "+
				"while the abandoned child is reaped, not released first", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("reapOnReturn never ran after the abandoned start returned")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && p.WedgedCount() != 0 {
		time.Sleep(time.Millisecond)
	}
	if p.WedgedCount() != 0 {
		t.Error("wedge slot was never released after the abandoned start returned")
	}
}

// TestPrepare_explicitArgv0WorkloadHealsOnlyViaTTL is the assertion the whole
// "evidence alone is not enough" adjudication rests on.
//
// An explicit, LOCAL argv[0] never enters boundedResolveInDirs and execve's
// instantly, so the agent sees no stall whatsoever — yet sanitizePATH still
// consulted the stale verdict and handed the child a poisoned PATH. Without the
// TTL this workload stays broken forever, which is why the plan rejected an
// evidence-only fix.
func TestPrepare_explicitArgv0WorkloadHealsOnlyViaTTL(t *testing.T) {
	const ttl = time.Minute
	f := newStaleFixture(t, ttl, 40*time.Millisecond)

	if d, err := f.prepare(t, []string{"/bin/echo"}, "", false); err != nil || d.Outage {
		t.Fatalf("healthy prepare: d=%+v err=%v", d, err)
	}
	f.probe.setVerdict(staleMount, false)

	for i := 0; i < 20; i++ {
		d, err := f.prepare(t, []string{"/bin/echo"}, "", false)
		if err != nil {
			t.Fatalf("prepare %d: %v", i, err)
		}
		if d.Outage {
			t.Fatalf("prepare %d unexpectedly produced evidence; this workload is supposed to be silent", i)
		}
	}
	if n := atomic.LoadInt32(&f.resolved); n != 0 {
		t.Fatalf("explicit argv[0] entered resolution %d times; the zero-evidence premise is wrong", n)
	}
	if c := f.probe.count(staleMount); c != 1 {
		t.Fatalf("probe count %d: nothing should have triggered a re-probe yet", c)
	}

	f.clock.advance(ttl)
	d, err := f.prepare(t, []string{"/bin/echo"}, "", false)
	if err != nil {
		t.Fatalf("post-TTL prepare: %v", err)
	}
	if !d.Outage {
		t.Fatal("with no evidence available, only the TTL can heal this workload — it did not")
	}
	if got := envGet(d.Env, "PATH"); strings.Contains(got, "/shared/bin") {
		t.Errorf("child PATH still poisoned after the TTL rescue: %q", got)
	}
}

// TestPrepare_wedgeCeilingSaturatedStillDropsDeadPathDirs covers the second
// zero-evidence state: at the ceiling both watchdogs early-return BEFORE their
// select, so no timeout branch ever runs and no evidence is ever produced. Probes
// take no wedge slot, so re-validation itself still works — but only the TTL can
// trigger it.
func TestPrepare_wedgeCeilingSaturatedStillDropsDeadPathDirs(t *testing.T) {
	const ttl = time.Minute
	f := newStaleFixture(t, ttl, 40*time.Millisecond)
	f.policy.wedgeCeiling = 1

	if d, err := f.prepare(t, []string{"echo"}, "", false); err != nil || d.Outage {
		t.Fatalf("healthy prepare: d=%+v err=%v", d, err)
	}
	f.probe.setVerdict(staleMount, false)

	// Saturate the ceiling with one abandoned start.
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	if err := f.policy.RunStart(func() error { <-release; return nil }, 20*time.Millisecond); !errors.Is(err, ErrSpawnTimeout) {
		t.Fatalf("want spawn timeout to occupy the slot, got %v", err)
	}
	// That abandoned start is itself evidence: it must have expired the cached
	// healthy verdict, so the next consult re-probes instead of answering from
	// cache. Observed as the probe count, not via Prepare — at the ceiling Prepare
	// returns an error before a Decision is ever built.
	f.probe.setVerdict(staleMount, true)
	if !f.policy.mountHealthy(staleMount) {
		t.Fatal("mount reports dead; cannot set up the ceiling case")
	}
	if c := f.probe.count(staleMount); c != 2 {
		t.Fatalf("probe count %d: the abandoned start did not expire the cached healthy verdict", c)
	}

	// Now the state under test: cached-healthy, mount dead, no wedge slots left.
	f.probe.setVerdict(staleMount, false)
	probesBefore := f.probe.count(staleMount)
	_, err := f.prepare(t, []string{"echo"}, "", false)
	var fe *FSError
	if !errors.As(err, &fe) || fe.Code != ReasonTooManyWedged {
		t.Fatalf("at the ceiling want %s, got %v", ReasonTooManyWedged, err)
	}
	if c := f.probe.count(staleMount); c != probesBefore {
		t.Fatalf("ceiling path re-probed (%d→%d); it is supposed to be evidence-free", probesBefore, c)
	}

	// Only the TTL gets us out of here — probes take no wedge slot, so
	// re-validation still works even with every slot held.
	f.clock.advance(ttl)
	if f.policy.mountHealthy(staleMount) {
		t.Fatal("TTL must still re-validate while the ceiling is saturated")
	}
	if c := f.probe.count(staleMount); c != probesBefore+1 {
		t.Fatalf("probe count %d after TTL expiry at the ceiling, want %d", c, probesBefore+1)
	}

	// And the point of re-validating: once a slot frees up, the dead dir is actually
	// dropped. Without this the test's name outran its assertions — it proved the
	// re-probe happened but never that anything came of it (internal review F-23).
	close(release)
	released = true
	releaseDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(releaseDeadline) && f.policy.WedgedCount() != 0 {
		time.Sleep(time.Millisecond)
	}
	d, err := f.prepare(t, []string{"echo"}, "", false)
	if err != nil {
		t.Fatalf("prepare after the ceiling cleared: %v", err)
	}
	if !d.Outage {
		t.Fatal("the dead dir must be dropped once a slot frees up")
	}
	if got := envGet(d.Env, "PATH"); strings.Contains(got, "/shared/bin") {
		t.Errorf("child PATH still contains the dead dir: %q", got)
	}
}

// TestApplyMounts_carryOverPreservesHealthyFreshness pins that the TTL survives
// mount-table churn. Refreshing decidedAt in the carry-over would renew it forever
// on a busy multi-user box — and #81's own mechanism (3) is precisely a verdict
// riding an unchanged signature across generations.
func TestApplyMounts_carryOverPreservesHealthyFreshness(t *testing.T) {
	const ttl = time.Minute
	clock := newFakeClock()
	probe := newFakeProbe()
	var gen int32
	p := mustNew(t, Config{
		Mode:         ModeAuto,
		ProbeTimeout: 40 * time.Millisecond,
		HealthTTL:    ttl,
		Probe:        probe.fn,
		Now:          clock.now,
		MountSource: func() ([]byte, error) {
			// The mount under test never changes; an unrelated bind mount churns.
			n := atomic.LoadInt32(&gen)
			return fakeMountinfo(
				[2]string{staleMount, "nfs4"},
				[2]string{"/", "ext4"},
				[2]string{fmt.Sprintf("/tmp/bind%d", n), "ext4"},
			), nil
		},
	})

	if !p.mountHealthy(staleMount) {
		t.Fatal("want healthy")
	}
	// Time has to pass DURING the churn. With a frozen clock a carry-over that
	// wrongly restamps decidedAt writes back the same instant it already held, so
	// the bug would be invisible here — this test was an identity until the
	// mutation run caught it.
	for i := 1; i <= 12; i++ {
		clock.advance(ttl / 4)
		atomic.StoreInt32(&gen, int32(i))
		p.refreshIfChanged()
	}
	if c := probe.count(staleMount); c != 1 {
		t.Fatalf("churn alone re-probed (%d); that is the F4 regression shape", c)
	}

	probe.setVerdict(staleMount, false)
	if p.mountHealthy(staleMount) {
		t.Fatal("a carried-over verdict must still expire; the churn renewed its decidedAt")
	}
	if c := probe.count(staleMount); c != 2 {
		t.Fatalf("probe count %d, want 2 (one re-probe after the carried-over verdict expired)", c)
	}
}

// TestPrepare_slowMountFalseDemotionSelfHealsWithinOneCommand bounds the cost of a
// false demotion: a healthy-but-slow mount can be demoted, but the late probe
// success must restore it AND restamp its freshness. Without the restamp the mount
// comes back already expired and is re-probed on the very next consult.
func TestPrepare_slowMountFalseDemotionSelfHealsWithinOneCommand(t *testing.T) {
	before := runtime.NumGoroutine()
	const ttl = time.Minute
	f := newStaleFixture(t, ttl, 30*time.Millisecond)
	slow := make(chan bool, 2)
	f.probe.setBlocking(staleMount, slow)
	defer func() {
		close(slow)
		assertGoroutinesReturn(t, before)
	}()

	d, err := f.prepare(t, []string{"echo"}, "", false)
	if err != nil {
		t.Fatalf("prepare over a slow mount: %v", err)
	}
	if !d.Outage {
		t.Fatal("a probe that misses its deadline must demote for now (that is the safe direction)")
	}

	// The outage lasts longer than a TTL before the statfs finally answers. This
	// is what makes the restamp observable: a healed verdict that kept the
	// demotion's timestamp is already older than the TTL the instant it is
	// restored, so the very next consult re-arms and re-probes it. With a frozen
	// clock both versions look identical (found by the mutation run).
	f.clock.advance(2 * ttl)
	slow <- true // the statfs finally answers: the mount was alive all along
	restored := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		dd, err := f.prepare(t, []string{"echo"}, "", false)
		if err == nil && !dd.Outage {
			restored = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !restored {
		t.Fatal("late probe success must restore the mount within one command")
	}
	if c := f.probe.count(staleMount); c != 1 {
		t.Fatalf("probe count %d after self-heal, want 1: the healed verdict was born expired (decidedAt not restamped)", c)
	}
}

// TestPrepare_cwdOnStaleHealthyMountFailsFastOnceInvalidated is gotcha #82's first
// half — the part that is a straight consequence of #81 and disappears with it.
// Asserted rather than asserted-in-prose, so "it heals automatically" stops being
// something each review round has to re-derive.
//
// origin: docs/deploy-tier-gotchas.md #82 (timan107, 2026-08-29)
func TestPrepare_cwdOnStaleHealthyMountFailsFastOnceInvalidated(t *testing.T) {
	f := newStaleFixture(t, time.Hour, 40*time.Millisecond)

	if _, err := f.prepare(t, []string{"echo"}, "", false); err != nil {
		t.Fatalf("healthy prepare: %v", err)
	}
	f.probe.setVerdict(staleMount, false)

	// Stale-healthy: the lexical cwd check consults the cached verdict and lets a
	// chdir into a dead mount through — which is where the 30s watchdog and the
	// pre-execve D-state children came from.
	if _, err := f.prepare(t, []string{"true"}, staleMount+"/nas", false); err != nil {
		t.Fatalf("while still stale-healthy the cwd check cannot fire: %v", err)
	}

	f.policy.InvalidateHealthy()
	resolvedBefore := atomic.LoadInt32(&f.resolved)
	_, err := f.prepare(t, []string{"true"}, staleMount+"/nas", false)
	var fe *FSError
	if !errors.As(err, &fe) || fe.Code != ReasonUnsafeCwd {
		t.Fatalf("dead cwd must fail fast: got %v, want %s", err, ReasonUnsafeCwd)
	}
	if n := atomic.LoadInt32(&f.resolved) - resolvedBefore; n != 0 {
		t.Errorf("cwd fail-fast must precede argv[0] resolution, but resolution ran %d times", n)
	}
}

// TestPrepare_healthyHangableMountZeroProbesWithinTTL is the steady-state cost
// guard. TestPrepare_localMachineZeroSyscallPerSpawn does NOT cover this: it
// counts mountinfo reads, and nothing added here reads mountinfo — a re-validation
// accidentally made unconditional would keep that test green while probing on
// every single spawn.
func TestPrepare_healthyHangableMountZeroProbesWithinTTL(t *testing.T) {
	const ttl = time.Minute
	f := newStaleFixture(t, ttl, 40*time.Millisecond)

	for i := 0; i < 50; i++ {
		if d, err := f.prepare(t, []string{"echo"}, "", false); err != nil || d.Outage {
			t.Fatalf("prepare %d: d=%+v err=%v", i, d, err)
		}
	}
	if c := f.probe.count(staleMount); c != 1 {
		t.Fatalf("50 spawns inside the TTL cost %d probes, want 1", c)
	}
	f.clock.advance(ttl)
	if d, err := f.prepare(t, []string{"echo"}, "", false); err != nil || d.Outage {
		t.Fatalf("post-TTL prepare on a still-healthy mount: d=%+v err=%v", d, err)
	}
	if c := f.probe.count(staleMount); c != 2 {
		t.Fatalf("probe count %d after one TTL, want exactly 2", c)
	}
}
