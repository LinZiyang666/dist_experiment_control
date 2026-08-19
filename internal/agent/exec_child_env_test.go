package agent

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: docs/reviews/cloned-credential-instances-plan.md §1.1 I1 (single-instance
// invariance). The same invariant is already pinned from the other side by
// TestBuildExecCmd_healthyHangableIsInert (review M2) and
// TestBuildExecCmd_activeInjectsPWD (review M3).
//
// os/exec injects `PWD=<Cmd.Dir>` ONLY when Cmd.Env is nil. The moment a caller
// assigns an explicit Env, that injection stops and the child inherits whatever
// PWD the AGENT process was started with — a value that has nothing to do with
// the requested working directory. On a device running exactly ONE agent this
// changes the environment `tether exec --cwd` hands the user's command, and the
// wrong value is worse than a missing one: it names a real directory that is not
// the one the command is running in.
//
// The probe is /usr/bin/env, not a shell: a POSIX shell re-derives PWD from
// getcwd() at startup and would mask the defect.
func TestExecChildEnvCarriesTheRequestedCwdAsPWD(t *testing.T) {
	if _, err := os.Stat("/usr/bin/env"); err != nil {
		t.Skip("no /usr/bin/env")
	}
	// A stale PWD in the agent's own environment, exactly as systemd or
	// `setsid nohup` leave it: the directory the agent was launched from.
	t.Setenv("PWD", "/agent/stale")

	want := t.TempDir()

	for _, tc := range []struct {
		name   string
		mounts []byte
	}{
		// No hangable mount: the "legacy" arm, i.e. every ordinary Linux box.
		{"no-hangable", fakeMountinfo([2]string{"/", "ext4"})},
		// A HEALTHY hangable mount: the reference deployment, whose ~/.tether
		// is an NFS mount (plan §0.6).
		{"healthy-hangable", fakeMountinfo([2]string{"/nfs", "nfs"}, [2]string{"/", "ext4"})},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newTestAgent(t, tc.mounts, nil)
			cmd, d, err := a.buildExecCmd(&proto.ExecReq{
				Argv: []string{"/usr/bin/env"},
				Cwd:  want,
			})
			if err != nil {
				t.Fatal(err)
			}
			if d.Outage {
				t.Fatalf("this case must NOT be the outage arm (that one injects PWD on purpose): %+v", d)
			}
			var out bytes.Buffer
			cmd.Stdout = &out
			cmd.Stderr = os.Stderr
			if err := cmd.Run(); err != nil {
				t.Fatalf("run: %v", err)
			}
			got := ""
			for _, line := range strings.Split(out.String(), "\n") {
				if v, ok := strings.CutPrefix(line, "PWD="); ok {
					got = v
				}
			}
			if got != want {
				t.Fatalf("exec child was handed PWD=%q, want the requested cwd %q.\n"+
					"buildExecCmd now assigns an explicit cmd.Env (to strip the instance lineage), "+
					"and os/exec injects PWD=<Dir> ONLY when Env==nil — so every `tether exec` child "+
					"on a single-agent device is handed the AGENT's stale $PWD instead of its own "+
					"working directory. cmd.Dir=%q cmd.Env==nil? %v",
					got, want, cmd.Dir, cmd.Env == nil)
			}
		})
	}
}
