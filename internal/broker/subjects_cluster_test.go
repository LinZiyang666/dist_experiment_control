package broker

import (
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// TestD9BroadcastClusterSubjectClassification is the round-3 BLOCKER guard: the FILE-TRANSFER
// subjects MUST stay broadcast in cluster mode (their D8 routing needs the HOME broker — the
// in-memory tracker holder — to see every message), while the leader-forwarded ctl commands
// MUST be queue-grouped (one broker handles + forwards). A regression that queue-groups a
// transfer subject bricks file transfer in a ≥2-node cluster (a random broker with no tracker
// entry silently drops it), which no other cheap test catches.
func TestD9BroadcastClusterSubjectClassification(t *testing.T) {
	p := proto.SubjectPrefix
	// Transfer lifecycle subjects → MUST be broadcast (home-gated + tracker-presence).
	broadcast := []string{
		p + ".s.*.cmd.by.*.node.*.push.req",
		p + ".s.*.cmd.by.*.node.*.pull.req",
		p + ".s.*.cmd.by.*.node.*.push-commit.req",
		p + ".s.*.ev.node.*.transfer.*.complete",
		p + ".s.*.ev.node.*.transfer.*.failed",
		p + ".ctrl.by.*.s.*.transfer.*.finalize.req",
		p + ".s.*.cmd.by.*.node.*.expose.req", // external-review F3: leader-local → broadcast
	}
	for _, s := range broadcast {
		if !isBroadcastClusterSubject(s) {
			t.Errorf("transfer subject %q MUST stay broadcast in cluster mode (home-gate/tracker), got queue-group", s)
		}
	}
	// Leader-forwarded ctl commands + non-transfer events → MUST be queue-grouped (one handler).
	queued := []string{
		p + ".ctrl.by.*.session.create.req",
		p + ".ctrl.by.*.session.*.rm.req",
		p + ".s.*.cmd.by.*.node.*.exec.req",
		p + ".s.*.cmd.by.*.node.*.kill.req",
		p + ".ctrl.by.*.s.*.ps.req",
		p + ".s.*.ev.node.*.proc.*.exit",
		p + ".ctrl.by.*.s.*.caps.req",
	}
	for _, s := range queued {
		if isBroadcastClusterSubject(s) {
			t.Errorf("ctl subject %q MUST be queue-grouped in cluster mode (one handler forwards), got broadcast", s)
		}
	}
}
