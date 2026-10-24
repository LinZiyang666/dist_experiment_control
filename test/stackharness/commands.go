package stackharness

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/nats-io/nats.go"
)

// WaitAgentCommands proves the forwarded subscriber is serving. Registration
// writes ONLINE before the agent installs that subscriber; a fixed sleep can
// lose the first exec even on a healthy stack. The caller supplies an anonymous
// fixture or broker connection because ordinary ctl ACLs forbid this subject.
// origin: S7 internal review MAKETEST-TRANSFER F5; S8 plan §4.2 / M05
func WaitAgentCommands(t *testing.T, nc *nats.Conn, sid, nid string) {
	t.Helper()
	if !testharness.WaitFor(t, 5*time.Second, 10*time.Millisecond, func() bool {
		msg, err := nc.Request(proto.SubjCmdForwarded(sid, nid, proto.ClaimProbeVerb), nil, 100*time.Millisecond)
		if err != nil {
			return false
		}
		var response proto.ClaimProbeResp
		return json.Unmarshal(msg.Data, &response) == nil && response.InstanceID != ""
	}) {
		t.Fatal("agent command subscriber did not become ready")
	}
}
