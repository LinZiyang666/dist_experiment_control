package agent

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

func TestRunChildActivePathUsesSpawnHelper(t *testing.T) {
	a := newTestAgent(t,
		fakeMountinfo([2]string{"/nfs", "nfs4"}, [2]string{"/", "ext4"}), nil)
	rc, err := a.runChild(nil, "", "helper-test", &proto.ExecReq{
		Argv: []string{"/bin/sh", "-c", "exit 7"},
	})
	if err != nil || rc != 7 {
		t.Fatalf("runChild rc=%d err=%v, want rc=7 through active helper", rc, err)
	}
}
