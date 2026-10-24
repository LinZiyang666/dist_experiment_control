package agent

import (
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func TestRunChildActivePathUsesSpawnHelper(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	a := newTestAgent(t,
		fakeMountinfo([2]string{"/nfs", "nfs4"}, [2]string{"/", "ext4"}), nil)
	rc, err := a.runChild(nil, "", "helper-test", &proto.ExecReq{Cwd: t.TempDir(),
		Argv: []string{"/bin/sh", "-c", "exit 7"},
	})
	if err != nil || rc != 7 {
		t.Fatalf("runChild rc=%d err=%v, want rc=7 through active helper", rc, err)
	}
}
