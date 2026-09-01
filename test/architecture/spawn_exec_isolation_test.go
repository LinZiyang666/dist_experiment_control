package architecture

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// A goroutine timeout around exec.Cmd.Start is not process isolation. Drill 62
// plus a SIGQUIT dump proved that a FUSE-blocked target exec can leave the
// abandoned goroutine outside a GC safepoint; the next GC then freezes the whole
// agent, including heartbeat and the watchdog timer. Both non-PTY and PTY safe
// starts must cross the local re-exec helper before touching the target path.
func TestRemoteFSSafeSpawnUsesProcessIsolation(t *testing.T) {
	root := repoRoot(t)
	read := func(rel string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(root, rel))
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}

	execSrc := read("internal/agent/exec.go")
	for _, tooth := range []string{
		"spawnexec.Prepare(cmd)",
		"handshake.Start(cmd)",
		"handshake.Cancel()",
	} {
		if !strings.Contains(execSrc, tooth) {
			t.Errorf("non-PTY safe spawn lost isolation tooth %q", tooth)
		}
	}
	ptySrc := read("internal/pty/pty.go")
	for _, tooth := range []string{"spawnexec.Prepare(cmd)", "handshake.Start(cmd)", "handshake.Cancel"} {
		if !strings.Contains(ptySrc, tooth) {
			t.Errorf("PTY safe spawn lost isolation tooth %q", tooth)
		}
	}
	helperSrc := read("internal/spawnexec/helper.go")
	for _, tooth := range []string{
		"func init()",
		"syscall.Exec(sp.Path, sp.Args, env)",
		"acquireGlobalWedgeSlot()",
		"syscall.CloseOnExec(int(statusFile.Fd()))",
		"cloneAndStripHelperEnv(target.Env)",
	} {
		if !strings.Contains(helperSrc, tooth) {
			t.Errorf("spawn helper lost OOM/recursion safety tooth %q", tooth)
		}
	}
	helperModeAt := strings.Index(helperSrc, "func maybeRun()")
	if helperModeAt < 0 {
		t.Fatal("spawn helper mode entry missing")
	}
	helperModeSrc := helperSrc[helperModeAt:]
	if strings.Contains(helperModeSrc, "cmd.Start()") || strings.Contains(helperModeSrc, "cmd := &exec.Cmd") {
		t.Error("spawn helper must exec in place; a second fork retains two Go processes per wedged target")
	}
	if strings.Contains(helperSrc, "func MaybeRun(") {
		t.Error("helper dispatch must not be exported/distributed to main or TestMain callers")
	}
}
