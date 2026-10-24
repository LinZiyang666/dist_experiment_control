// forward_container_test.go — the exec-internal half of the actor stamp must
// hold for the exec object the AGENT decodes, not only for the one spelled
// exactly "exec". encoding/json matches the container key case-insensitively
// too, so a body that carries "Exec" (or "EXEC") and no exact "exec" reaches
// the agent's ProcessResumeReq.Exec with whatever actor_fp the client wrote:
// Parse only looks up raw["exec"], so hasExec is false and nothing inside is
// stamped. The broker's contract (architecture §22.4 item 2 ⑦, forward.go
// header: "After Stamp there is exactly one actor key in each object, and it
// is the broker's") is stated for both objects; either Parse must refuse such
// a body or Stamp must own the actor in the container the agent will decode.
package procforward

import (
	"encoding/json"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: hot-update S1 internal review (protocol lane, verifier) — a
// case-variant exec container bypasses the exec-internal stamp. Today the
// runtime overrides spec.ActorFP from the (stamped) top level before
// spawning, so nothing acts on the forged value; this pins the stamp's own
// contract so a future consumer of Exec.ActorFP cannot inherit the hole.
func TestStampOwnsActorInsideCaseVariantExecContainer(t *testing.T) {
	for _, body := range []string{
		`{"op":"create","version":1,"call_id":7,"Exec":{"argv":["x"],"actor_fp":"forged"}}`,
		`{"op":"create","version":1,"call_id":7,"EXEC":{"argv":["x"],"Actor_FP":"forged"}}`,
		`{"op":"create","version":1,"call_id":7,"exec":{"argv":["x"],"actor_fp":"forged"},"eXec":{"argv":["y"],"actor_fp":"forged2"}}`,
	} {
		env, err := Parse([]byte(body))
		if err != nil {
			continue // refusing the shape at the edge is an acceptable fix
		}
		out, err := env.Stamp("SHA256:stamp")
		if err != nil {
			t.Fatal(err)
		}
		var req proto.ProcessResumeReq
		if err := json.Unmarshal(out, &req); err != nil {
			t.Fatalf("decode after stamp: %v (%s)", err, out)
		}
		if req.Exec != nil && req.Exec.ActorFP != "SHA256:stamp" {
			t.Fatalf("the agent decodes exec.actor_fp=%q from a body the broker forwarded\nbody:    %s\nstamped: %s", req.Exec.ActorFP, body, out)
		}
	}
}
