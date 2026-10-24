package spawnsafe

import (
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"github.com/LinZiyang666/tether/internal/drillobserve"
	"github.com/LinZiyang666/tether/internal/proto"
)

// Child command construction and abandoned-start cleanup share the spawn policy owner.
// origin: hot-update S5 plan §10 / AGENT-SLIM-S5.
func EnvSliceFromMap(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k, v := range m {
		out = append(out, k+"="+v)
	}
	return drillobserve.StripEnvironment(out)
}

// BuildExecCmd constructs the child *exec.Cmd. When the hung-fs-safe spawn
// policy is active it pre-resolves argv[0] against a sanitized PATH and builds
// &exec.Cmd{Path: <abs>, Args: argv} DIRECTLY — bypassing exec.Command's
// LookPath, which would D-hang stat'ing a wedged PATH dir. Otherwise it falls
// back to the legacy exec.Command path, byte-identical to before the feature.
// The returned Decision tells the caller whether to bound cmd.Start and emit the
// sanitize warning.
func BuildExecCmd(policy *Policy, req *proto.ExecReq, baseEnv []string) (*exec.Cmd, Decision, error) {
	// lookupPATH is the AGENT process PATH — the one exec.Command's LookPath would
	// walk and D-hang on (review F2) — NOT the child env's PATH.
	d, err := policy.Prepare(req.Argv, req.Cwd, os.Getenv("PATH"), baseEnv, req.Safe)
	if err != nil {
		return nil, Decision{}, err
	}
	if !d.Active {
		// Legacy path (booted with no hangable mount).
		// origin: hot-update S5 plan §10 / AGENT-SLIM-S5. The child is owned by the process registry; cancellation kills its group there.
		cmd := exec.Command(req.Argv[0], req.Argv[1:]...) //nolint:noctx // Legacy child lifetime is registry-owned; CommandContext would add a second kill owner.
		if req.Cwd != "" {
			cmd.Dir = req.Cwd
		}
		if len(req.Env) > 0 {
			cmd.Env = EnvSliceFromMap(req.Env)
		}
		// Env stays nil here on purpose (remote-fs-resilience M2 pins this path
		// as byte-identical). The instance lineage cannot leak through it
		// because the agent UNSETS those variables from its own environment at
		// startup — see mintInstanceID. A child cannot inherit what the parent
		// no longer has.
		SetExecProcGroup(cmd)
		return cmd, d, nil
	}
	// Active: ALWAYS &exec.Cmd{Path,Args} (bypass the hanging LookPath). Args[0]
	// keeps the ORIGINAL argv[0] so multi-call binaries that inspect os.Args[0]
	// still work.
	cmd := &exec.Cmd{Path: d.Path, Args: req.Argv}
	if d.Outage {
		cmd.Env = drillobserve.StripEnvironment(d.Env)
		cmd.Dir = d.Cwd
	} else {
		// Healthy-hangable: keep the LEGACY env/cwd so the child is byte-identical
		// to before the feature; only resolution + execve are now bounded.
		if req.Cwd != "" {
			cmd.Dir = req.Cwd
		}
		if len(req.Env) > 0 {
			cmd.Env = EnvSliceFromMap(req.Env)
		}
		// Same as the legacy branch: nil is deliberate, and the lineage is
		// already absent from this process's environment.
	}
	SetExecProcGroup(cmd)
	return cmd, d, nil
}

// SetExecProcGroup puts the exec child in its OWN process group (Setpgid) so an
// admin EVICT can reap the child AND any subtree it forks with one kill(-pgid)
// (#26 reapManagedChildren) — a bare setsid-nohup agent host has no systemd
// cgroup to do it. pgid == child pid once started. It does not detach a
// controlling terminal (exec is non-PTY), so output streaming is unchanged.
func SetExecProcGroup(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setpgid = true
}

// ReapAbandonedExecChild ends a child whose spawn window we already gave up on.
//
// origin: prerelease audit agent-exec/L3-F3. This used to be a bare cmd.Wait(). Waiting
// reaps the process table entry, which is what the fd-leak review asked for, but it does
// nothing about the process: the handler has ALREADY answered
// remote_fs_spawn_timeout and nobody is reading the child's output or will ever report
// its exit, yet it keeps running, holding the mount it wedged on and consuming the
// resources the abandon was supposed to reclaim. On a remote filesystem that recovers
// minutes later, the wait itself blocks that whole time.
//
// SIGKILL to the process GROUP, mirroring the PTY path's
// KillAndWaitAfterAbandonedStart (internal/pty). exec children get their own group
// (SetExecProcGroup), so the negative pid reaches the child's own descendants and
// nothing else.
//
// THE GUARD IS `cmd.Process == nil`, AND ONLY THAT.
//
// origin: prerelease audit round 2, I-F1. The first version also returned early on a
// non-nil startErr, reasoning that "Start itself failed, so os/exec has already cleaned
// up". That reasoning describes a case this function is almost never called in. The
// abandon path runs onAbandon FIRST, and exec's onAbandon calls handshake.Cancel() and
// closes both pipes — so when the wedged execve finally returns it returns an ERROR, and
// the early return skipped the only path that actually occurs. The child stayed alive,
// holding the mount it wedged on, and the wedge slot was released immediately because
// reapOnReturn returned without waiting — disarming the too_many_wedged_spawns ceiling
// that is supposed to bound exactly this situation.
//
// A failed cmd.Start() leaves cmd.Process nil, so the one case the startErr check was
// reaching for is already covered by the pointer check — and covered correctly, because
// it tests the thing that matters (is there a process) rather than a proxy for it.
//
// Reading cmd.ProcessState is race-free here: the caller reaches this only after
// receiving the start result over a channel, which supplies the happens-before edge.
//
// A package-level function taking *Agent, not a method: the structural-budget ratchet
// pins this type's method count exactly.
func ReapAbandonedExecChild(logger *slog.Logger, cmd *exec.Cmd, startErr error) {
	_ = startErr // deliberately unused — see above
	if cmd == nil || cmd.Process == nil {
		return
	}
	if cmd.ProcessState != nil {
		return // already reaped by whoever observed the exit
	}
	pid := cmd.Process.Pid
	if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		logger.Warn("agent: kill abandoned exec child group", "pid", pid, "err", err)
	}
	_ = cmd.Wait()
}

// MergeChildEnv builds a child env array starting from os.Environ()
// (so PATH / HOME / locale come for free) and overlays the override
// map on top — last-wins semantics, matching os/exec docs. Always
// guarantees TERM is set so curses programs (tmux, vim, htop, less)
// don't fail with "terminal does not support clear" under systemd
// --user where TERM is unset by default.
//
// Used by `tether run` for PTY children. `tether exec` non-PTY
// children may want a stricter env (no inherited TERM since stdout
// isn't a tty); they keep using EnvSliceFromMap directly.
func MergeChildEnv(base []string, override map[string]string) []string {
	// stripInstanceEnv for the same reason as execBaseEnv: a PTY child that
	// launches an agent must start its own lineage, not continue ours.
	// The caller supplies the environment with instance lineage removed.
	out := make([]string, 0, len(base)+len(override)+1)
	out = append(out, base...)
	hasTerm := false
	for _, e := range base {
		if strings.HasPrefix(e, "TERM=") {
			hasTerm = true
			break
		}
	}
	for k, v := range override {
		out = append(out, k+"="+v)
		if k == "TERM" {
			hasTerm = true
		}
	}
	if !hasTerm {
		out = append(out, "TERM=xterm-256color")
	}
	return drillobserve.StripEnvironment(out)
}
