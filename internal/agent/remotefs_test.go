package agent

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/spawnsafe"
)

// envGet returns the value of key in an os/exec env slice, or "".
func envGet(env []string, key string) string {
	pfx := key + "="
	val := ""
	for _, e := range env {
		if strings.HasPrefix(e, pfx) {
			val = e[len(pfx):]
		}
	}
	return val
}

func fakeMountinfo(entries ...[2]string) []byte {
	var b strings.Builder
	for i, e := range entries {
		b.WriteString("36 35 0:" + itoa(i) + " / " + e[0] + " rw - " + e[1] + " src rw\n")
	}
	return []byte(b.String())
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var d []byte
	for i > 0 {
		d = append([]byte{byte('0' + i%10)}, d...)
		i /= 10
	}
	return string(d)
}

// newTestAgent builds an Agent (not connected) with an injected mount table and
// probe so the hung-fs spawn path is exercised hermetically.
func newTestAgent(t *testing.T, mounts []byte, dead map[string]bool) *Agent {
	t.Helper()
	a, err := New(Config{
		NATSURL:              "nats://127.0.0.1:4222",
		SID:                  "lab",
		NID:                  "lab-1",
		RemoteFSProbeTimeout: 60 * time.Millisecond,
		RemoteFSMountSource:  func() ([]byte, error) { return mounts, nil },
		RemoteFSProbe:        func(mp string) bool { return !dead[mp] },
	})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	return a
}

func TestBuildExecCmd_inertWhenNoHangable(t *testing.T) {
	a := newTestAgent(t, fakeMountinfo([2]string{"/", "ext4"}), nil)
	cmd, d, err := a.buildExecCmd(&proto.ExecReq{Argv: []string{"echo", "hi"}})
	if err != nil {
		t.Fatal(err)
	}
	if d.Active {
		t.Fatal("no hangable mounts ⇒ inert (legacy exec.Command path)")
	}
	if len(cmd.Args) != 2 || cmd.Args[0] != "echo" {
		t.Errorf("legacy cmd argv mangled: %+v", cmd.Args)
	}
}

func TestBuildExecCmd_activeResolvesAndSanitizes(t *testing.T) {
	tmp := t.TempDir()
	bin := filepath.Join(tmp, "mytool")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	a := newTestAgent(t,
		fakeMountinfo([2]string{"/shared", "nfs4"}, [2]string{"/", "ext4"}),
		map[string]bool{"/shared": true})

	// The AGENT PATH is what LookPath would walk + hang on (review F2): a dead nfs
	// dir front-loaded, then the local tmp.
	t.Setenv("PATH", "/shared/bin:"+tmp)
	req := &proto.ExecReq{Argv: []string{"mytool", "--x"}}
	cmd, d, err := a.buildExecCmd(req)
	if err != nil {
		t.Fatalf("buildExecCmd: %v", err)
	}
	if !d.Active || !d.Outage {
		t.Fatalf("a dead $PATH dir ⇒ active outage, got %+v", d)
	}
	if cmd.Path != bin {
		t.Errorf("Path not resolved to local binary: got %q want %q", cmd.Path, bin)
	}
	if cmd.Args[0] != "mytool" {
		t.Errorf("Args[0] must stay the original argv[0], got %q", cmd.Args[0])
	}
	if childPATH := envGet(cmd.Env, "PATH"); strings.Contains(childPATH, "/shared/bin") {
		t.Errorf("child PATH still contains the dead dir: %q", childPATH)
	}
	if !strings.Contains(d.Warn, "/shared/bin") {
		t.Errorf("warn should name the dropped dir: %q", d.Warn)
	}
}

func TestBuildExecCmd_failFastUnhealthyExplicit(t *testing.T) {
	a := newTestAgent(t,
		fakeMountinfo([2]string{"/shared", "nfs4"}, [2]string{"/", "ext4"}),
		map[string]bool{"/shared": true})
	_, _, err := a.buildExecCmd(&proto.ExecReq{Argv: []string{"/shared/bin/x"}})
	var fe *spawnsafe.FSError
	if !errors.As(err, &fe) || fe.Code != spawnsafe.ReasonUnhealthy {
		t.Fatalf("explicit dead path: got %v, want remote_fs_unhealthy", err)
	}
}

// TestBuildExecCmd_healthyHangableIsInert pins review M2: on a machine with a
// HEALTHY hangable mount (the timan steady state) nothing is dropped, so the
// active rewrite must NOT fire — the child keeps the legacy nil-inherited env and
// native argv[0] resolution, byte-identical to before the feature.
func TestBuildExecCmd_healthyHangableIsInert(t *testing.T) {
	tmp := t.TempDir()
	echo := filepath.Join(tmp, "echo")
	if err := os.WriteFile(echo, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", tmp)                                                                      // agent PATH, all healthy
	a := newTestAgent(t, fakeMountinfo([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}), nil) // all healthy
	cmd, d, err := a.buildExecCmd(&proto.ExecReq{Argv: []string{"echo", "hi"}, Cwd: "/tmp"})
	if err != nil {
		t.Fatal(err)
	}
	// Active (we self-resolve + bound on a hangable machine, F1/F2) but NOT an
	// outage ⇒ byte-identical child: legacy nil env, original cwd, resolved Path.
	if !d.Active || d.Outage {
		t.Fatalf("healthy-hangable: want Active+!Outage, got %+v", d)
	}
	if cmd.Env != nil {
		t.Errorf("non-outage path must leave cmd.Env nil (inherit ⇒ os/exec injects PWD), got %v", cmd.Env)
	}
	if cmd.Path != echo {
		t.Errorf("Path not resolved (should equal LookPath result): got %q want %q", cmd.Path, echo)
	}
	if cmd.Dir != "/tmp" || len(cmd.Args) != 2 || cmd.Args[0] != "echo" {
		t.Errorf("legacy cmd perturbed: dir=%q args=%v", cmd.Dir, cmd.Args)
	}
}

// TestBuildExecCmd_activeInjectsPWD pins review M3: in the active (outage) path
// the explicit cmd.Env must carry PWD=<cwd>, since os/exec only injects PWD when
// Env==nil — otherwise the child would inherit the agent's stale $PWD.
func TestBuildExecCmd_activeInjectsPWD(t *testing.T) {
	tmp := t.TempDir()
	if err := os.WriteFile(filepath.Join(tmp, "mytool"), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	a := newTestAgent(t,
		fakeMountinfo([2]string{"/shared", "nfs4"}, [2]string{"/", "ext4"}),
		map[string]bool{"/shared": true})
	// Agent PATH (resolution source, F2) has a dead dir ⇒ outage; req.Env carries
	// a stale PWD the child must NOT inherit.
	t.Setenv("PATH", "/shared/bin:"+tmp)
	req := &proto.ExecReq{
		Argv: []string{"mytool"}, Cwd: tmp,
		Env: map[string]string{"PATH": "/shared/bin:" + tmp, "PWD": "/agent/stale"},
	}
	cmd, d, err := a.buildExecCmd(req)
	if err != nil || !d.Active || !d.Outage {
		t.Fatalf("want active outage (a dead mount dropped): d=%+v err=%v", d, err)
	}
	if cmd.Dir != tmp {
		t.Errorf("cmd.Dir=%q want %q", cmd.Dir, tmp)
	}
	if got := envGet(cmd.Env, "PWD"); got != tmp {
		t.Errorf("active child PWD = %q, want %q (must not leak agent stale PWD — M3)", got, tmp)
	}
}

// TestBuildExecCmd_offModeInertWithHangable pins review m8: mode=off is inert at
// the handler even with a (dead) hangable mount present.
func TestBuildExecCmd_offModeInertWithHangable(t *testing.T) {
	a, err := New(Config{
		NATSURL: "nats://127.0.0.1:4222", SID: "lab", NID: "lab-1",
		RemoteFSMode:        "off",
		RemoteFSMountSource: func() ([]byte, error) { return fakeMountinfo([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"}), nil },
		RemoteFSProbe:       func(string) bool { return false }, // dead, but off short-circuits before any probe
	})
	if err != nil {
		t.Fatal(err)
	}
	cmd, d, err := a.buildExecCmd(&proto.ExecReq{Argv: []string{"echo"}})
	if err != nil {
		t.Fatal(err)
	}
	if d.Active {
		t.Fatal("off mode must be inert even with a dead hangable mount present")
	}
	if cmd.Env != nil {
		t.Errorf("off mode cmd.Env must be nil, got %v", cmd.Env)
	}
}

type noopExpose struct{}

func (noopExpose) AddProxy(PortToken) error      { return nil }
func (noopExpose) RemoveProxy(string, int) error { return nil }

// TestReplayPortsFromState_boundedOnWedgedHome pins review B2: the boot-path
// state.json read (replayPortsFromState) must NOT D-hang the whole Run() loop on
// a wedged hangable Home — it degrades (skips replay) within the deadline so the
// agent goes on to start heartbeats. We make os.ReadFile block by pointing
// state.json at a FIFO with no writer (open(O_RDONLY) blocks), then release it at
// cleanup so the abandoned reader exits.
func TestReplayPortsFromState_boundedOnWedgedHome(t *testing.T) {
	home := t.TempDir()
	dir := filepath.Join(home, "agent", "lab")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(dir, "state.json")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Skipf("mkfifo unsupported here: %v", err)
	}
	t.Cleanup(func() {
		// Open a writer so the wedged open(O_RDONLY) in the abandoned reader returns.
		if f, err := os.OpenFile(fifo, os.O_WRONLY, 0); err == nil {
			_, _ = f.WriteString("{}")
			_ = f.Close()
		}
	})

	a := newTestAgent(t, fakeMountinfo([2]string{home, "nfs"}, [2]string{"/", "ext4"}), map[string]bool{home: true})
	a.cfg.Home = home
	a.stateStore = newStateStore(home, "lab")
	a.homeHangable = true
	a.cfg.ExposeAdapter = noopExpose{} // non-nil so replay proceeds to the read

	done := make(chan struct{})
	go func() { a.replayPortsFromState(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("replayPortsFromState hung on a wedged Home — boot would never reach heartbeatLoop (B2)")
	}
}

// TestStartBounded_abandonClosesPipesAndReaps pins review M4: when the bounded
// start is abandoned (execve D-hangs past the deadline), the pipe read ends are
// closed immediately (fd reclaim), the wedge slot stays held, and once the start
// finally returns the reap runs and the slot is released — so fds/goroutines
// don't leak unbounded over a long outage.
func TestStartBounded_abandonClosesPipesAndReaps(t *testing.T) {
	a, err := New(Config{
		NATSURL: "nats://127.0.0.1:4222", SID: "lab", NID: "lab-1",
		RemoteFSSpawnTimeout: 60 * time.Millisecond,
		RemoteFSWedgeCeiling: 4,
		RemoteFSMountSource:  func() ([]byte, error) { return nil, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	before := runtime.NumGoroutine()
	release := make(chan struct{})
	var closed, reaped int32
	gotErr := a.startBounded(
		func() error { <-release; return nil }, // execve blocks past the deadline
		func() { atomic.AddInt32(&closed, 1) },
		func(error) { atomic.AddInt32(&reaped, 1) },
	)
	if !errors.Is(gotErr, spawnsafe.ErrSpawnTimeout) {
		t.Fatalf("want spawn timeout, got %v", gotErr)
	}
	if atomic.LoadInt32(&closed) != 1 {
		t.Error("abandon must close the pipe read ends (fd reclaim, M4)")
	}
	if a.spawnPolicy.WedgedCount() != 1 {
		t.Errorf("wedged=%d want 1 while abandoned", a.spawnPolicy.WedgedCount())
	}
	close(release) // start returns ⇒ reaper runs reap + releases the slot
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (atomic.LoadInt32(&reaped) == 0 || a.spawnPolicy.WedgedCount() != 0) {
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&reaped) != 1 {
		t.Error("reap must run when the abandoned start finally returns")
	}
	if a.spawnPolicy.WedgedCount() != 0 {
		t.Error("wedge slot must be released after the abandoned start returns")
	}
	assertAgentGoroutinesReturn(t, before)
}

func TestAgentNew_rejectsBadRemoteFSMode(t *testing.T) {
	_, err := New(Config{
		NATSURL:      "nats://127.0.0.1:4222",
		SID:          "lab",
		NID:          "lab-1",
		RemoteFSMode: "bogus",
	})
	if err == nil || !strings.Contains(err.Error(), "remote_fs.mode") {
		t.Fatalf("bad remote_fs.mode should fail New, got %v", err)
	}
	_, err = New(Config{
		NATSURL:              "nats://127.0.0.1:4222",
		SID:                  "lab",
		NID:                  "lab-1",
		RemoteFSSpawnTimeout: -time.Second,
	})
	if err == nil || !strings.Contains(err.Error(), "remote_fs.spawn_timeout") {
		t.Fatalf("negative remote_fs.spawn_timeout should fail New, got %v", err)
	}
}

// TestBoundedHomeRead_singleFlightAbandon covers review B1: a blocking state.json
// read on a hangable Home is abandoned within the deadline (degraded), AND a
// second read while the first is still wedged does NOT spawn another reader
// (single-flight) — so abandoned readers are bounded to ONE, not O(reconnects).
func TestBoundedHomeRead_singleFlightAbandon(t *testing.T) {
	a := newTestAgent(t, fakeMountinfo([2]string{"/", "ext4"}), nil) // probeTimeout 60ms
	before := runtime.NumGoroutine()

	// Prompt read returns straight through.
	sf := &StateFile{}
	if got, ok := a.boundedHomeRead(func() (*StateFile, error) { return sf, nil }, "prompt"); !ok || got != sf {
		t.Fatalf("prompt read: ok=%v got=%v", ok, got)
	}

	// Blocking read: abandoned within the deadline.
	release := make(chan struct{})
	var calls int32
	blocking := func() (*StateFile, error) { atomic.AddInt32(&calls, 1); <-release; return &StateFile{}, nil }
	start := time.Now()
	if _, ok := a.boundedHomeRead(blocking, "wedge1"); ok {
		t.Fatal("blocking read should degrade")
	}
	if d := time.Since(start); d > time.Second {
		t.Errorf("bounded read did not honor deadline (took %s)", d)
	}
	// Second read while the first is still wedged: degrades immediately and does
	// NOT launch another reader (single-flight).
	if _, ok := a.boundedHomeRead(blocking, "wedge2"); ok {
		t.Fatal("second read should degrade while first is wedged")
	}
	if c := atomic.LoadInt32(&calls); c != 1 {
		t.Fatalf("single-flight broken: %d readers spawned, want 1", c)
	}

	close(release) // the one wedged reader returns
	assertAgentGoroutinesReturn(t, before)
}

// TestBoundedHomeRead_stallExpiresCachedMountHealth pins the fourth evidence site of
// the gotcha #81 fix, and it is the one nothing else can reach.
//
// A stalled state.json read proves the same thing a stalled spawn proves: a mount the
// agent still believes is healthy is not. This site matters more than its size suggests
// — it is the only trigger that fires with nobody running a command, so on an idle agent
// it is the whole self-healing story.
//
// The architecture gate structurally cannot cover it (that gate enumerates sites that
// MINT a spawn-timeout sentinel; this branch returns `(nil, false)` and mints nothing),
// and the pre-existing TestBoundedHomeRead_singleFlightAbandon builds its agent with a
// mounts table of just "/ ext4" — no hangable mount at all, so the invalidation is a
// no-op there by construction. Deleting the call was therefore invisible to the entire
// suite; that is what this test fixes.
//
// origin: docs/reviews/remote-fs-stale-health-review.md F-1
func TestBoundedHomeRead_stallExpiresCachedMountHealth(t *testing.T) {
	var probes int32
	var dead atomic.Bool
	a, err := New(Config{
		NATSURL:              "nats://127.0.0.1:4222",
		SID:                  "lab",
		NID:                  "lab-1",
		RemoteFSProbeTimeout: 60 * time.Millisecond,
		RemoteFSMountSource: func() ([]byte, error) {
			return fakeMountinfo([2]string{"/nfs", "nfs4"}, [2]string{"/", "ext4"}), nil
		},
		RemoteFSProbe: func(string) bool {
			atomic.AddInt32(&probes, 1)
			return !dead.Load()
		},
	})
	if err != nil {
		t.Fatalf("agent.New: %v", err)
	}
	before := runtime.NumGoroutine()

	const lookupPATH = "/nfs/bin:/usr/bin"
	env := []string{"PATH=" + lookupPATH}

	// Seed the healthy verdict the way an ordinary command would.
	d, err := a.spawnPolicy.Prepare([]string{"echo"}, "", lookupPATH, env, false)
	if err != nil {
		t.Fatalf("seed prepare: %v", err)
	}
	if d.Outage {
		t.Fatal("healthy mount must not read as an outage")
	}
	if n := atomic.LoadInt32(&probes); n != 1 {
		t.Fatalf("probe count %d after seeding, want 1", n)
	}

	// The mount dies. Nothing spawns anything; the only symptom is that the agent's own
	// Home read stalls.
	dead.Store(true)
	release := make(chan struct{})
	if _, ok := a.boundedHomeRead(func() (*StateFile, error) { <-release; return &StateFile{}, nil }, "wedge"); ok {
		t.Fatal("a blocking Home read must degrade, not succeed")
	}

	// THE assertion: that stall must have expired the cached verdict, so the next
	// command re-probes and drops the dead dir. The health TTL is 5 minutes and this
	// test runs in milliseconds, so a pass here can only come from the invalidation.
	d, err = a.spawnPolicy.Prepare([]string{"echo"}, "", lookupPATH, env, false)
	if err != nil {
		t.Fatalf("post-stall prepare: %v", err)
	}
	if n := atomic.LoadInt32(&probes); n != 2 {
		t.Fatalf("probe count %d after the stalled Home read, want 2: the stall did not "+
			"expire the cached mount-health verdict", n)
	}
	if !d.Outage {
		t.Fatal("the dead $PATH dir must be dropped after the stall re-probed the mount")
	}

	close(release)
	assertAgentGoroutinesReturn(t, before)
}

// TestStateStore_loadNoLockIsLockFree pins review B1's core: loadNoLock must NOT
// acquire stateStore.mu, so an abandoned (D-hung) read cannot poison every
// later AddPort/RemovePort/SetProxy. We hold the mutex and require loadNoLock to
// still return; load() (the locked path) would block here.
func TestStateStore_loadNoLockIsLockFree(t *testing.T) {
	s := newStateStore(t.TempDir(), "lab")
	s.mu.Lock()
	defer s.mu.Unlock()
	done := make(chan struct{})
	go func() { _, _ = s.loadNoLock(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("loadNoLock blocked on s.mu — an abandoned read would poison all state I/O (B1)")
	}
}

func assertAgentGoroutinesReturn(t *testing.T, before int) {
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
