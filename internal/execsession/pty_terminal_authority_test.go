//go:build linux

package execsession

import (
	"errors"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: hot-update S4 internal review rv-auth RV-AUTH-1
func TestPTYTerminalAuthorityRevocationPreservesRestorability(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		for _, arm := range []string{"active", "local", "import", "frozen", "frozen-local"} {
			t.Run(arm, func(t *testing.T) {
				// origin: hot-update S5 PROC-LEAK / final process census.
				testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

				f := newPTYRuntime(t, 0)
				q, owner := ptyCreate(t, f, "retained-terminal", []string{"/bin/sh", "-c", "printf retained; exit 23"})
				status := ptyStatus(t, f, q, func(p proto.ProcessResumeReply) bool {
					return p.Phase == "terminal" && p.Stdout != nil && p.Stdout.FIN
				})
				if status.ExitCode != 23 || string(status.Stdout.Data) != "retained" {
					t.Fatalf("wrong terminal: %+v", status)
				}
				if _, ok := f.manager.Lookup(owner.pid); ok {
					t.Fatal("terminal still has Manager row")
				}
				if arm == "frozen" || arm == "frozen-local" {
					imageFreeze(t, f.r)
				}
				_, revokeErr := f.r.Revoke(f.scope.SID, "session_destroyed")
				if revokeErr != nil {
					t.Errorf("finished terminal acquired cleanup responsibility: %v", revokeErr)
				}
				f.r.mu.Lock()
				marked := owner.revoked == "session_destroyed"
				f.r.mu.Unlock()
				if !marked {
					t.Fatal("terminal lost irreversible revocation")
				}
				if arm == "active" {
					if _, err := owner.pty.ApplyControl(ptysession.ControlRequest{Seq: 1, Op: "keepalive"}); !errors.Is(err, ptysession.ErrRevoked) {
						t.Fatalf("terminal actor did not retain revocation: %v", err)
					}
					return
				}
				if !f.r.Frozen() {
					imageFreeze(t, f.r)
				}
				if _, err := f.manager.Freeze(); err != nil {
					t.Fatal(err)
				}
				img, resources, err := f.r.Export()
				if err != nil {
					t.Fatal(err)
				}
				if len(img.Tasks) != 1 || img.Tasks[0].PTY == nil || img.Tasks[0].PTY.Revoked != "session_destroyed" || img.Tasks[0].PTY.CleanupPending || len(img.Tasks[0].PTY.CleanupMembers) != 0 {
					t.Fatalf("terminal export invented cleanup or lost revocation: %+v", img.Tasks)
				}
				if arm == "import" || arm == "frozen" {
					imported, _, err := Import(f.cfg, img, nil, newImageDupSource(t, resources))
					if err != nil {
						t.Errorf("exporter produced unimportable terminal: %v", err)
						return
					}
					if err := f.r.Discard(); err != nil {
						t.Fatal(err)
					}
					f.r = imported
				}
				f.manager.Thaw()
				if err := f.r.Thaw(); err != nil {
					t.Errorf("finished terminal blocked activation: %v", err)
				}
				f.r.mu.Lock()
				actor := f.r.tasks[owner.pid].pty
				f.r.mu.Unlock()
				if _, err := actor.ApplyControl(ptysession.ControlRequest{Seq: 1, Op: "keepalive"}); !errors.Is(err, ptysession.ErrRevoked) {
					t.Fatalf("activation lost terminal revocation: %v", err)
				}
			})
		}
	}
	testharness.AssertNoGoroutineLeak(t, "authority regression", g0)
	testharness.AssertNoFDLeak(t, "authority regression", fd0, 0)
}
