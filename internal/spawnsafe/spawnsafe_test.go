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
	d0, err0 := pHealthy.Prepare([]string{"echo"}, "", "/usr/bin:"+tmp, []string{"PATH=" + tmp}, false)
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
