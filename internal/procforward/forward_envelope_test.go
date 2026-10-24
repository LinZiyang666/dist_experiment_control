// forward_envelope_test.go — the envelope fields the broker ACTS on (op picks
// the admission spec, version and call_id pick the refusal) must be the same
// fields the agent's struct decoder ends up with after Stamp re-encodes the
// body. encoding/json matches keys case-insensitively and takes the last
// duplicate in input order, while Stamp re-emits a map in byte order; until
// S1 review protocol-V2 the exact lowercase key merely happened to sort last
// among its ASCII case variants. The rule is now explicit: a body that spells
// a routing key (version, call_id, op, exec) in any other case is refused at
// the edge (ErrEnvelopeKeyCase → bad_request), so the agent can never
// execute an op the broker admitted under another op's spec, nor decode an
// exec.actor_fp the stamp never touched. The actor key itself stays OWNED by
// the stamp: its variants are stripped, not refused.
package procforward

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: hot-update S1 internal review (protocol lane P4 + verifier V2).
func TestStampKeepsEnvelopeFieldsAuthoritativeOverCaseVariants(t *testing.T) {
	// Every routing-key case variant is refused, whatever it sits next to.
	for _, body := range []string{
		`{"version":1,"call_id":7,"op":"create","OP":"probe","exec":{"argv":["x"]}}`,
		`{"OP":"probe","op":"create","Op":"probe","version":1,"call_id":7,"exec":{"argv":["x"]}}`,
		`{"oP":"probe","op":"create","version":1,"Version":99,"CALL_ID":3,"call_id":7,"exec":{"argv":["x"]}}`,
		`{"op":"create","exec":{"argv":["x"]},"Exec":{"argv":["forged"],"actor_fp":"forged"},"version":1,"call_id":7}`,
		`{"op":"create","version":1,"call_id":7,"Exec":{"argv":["x"],"actor_fp":"forged"}}`,
		`{"op":"create","version":1,"call_id":7,"EXEC":{"argv":["x"],"Actor_FP":"forged"}}`,
		`{"op":"probe","version":1,"Call_Id":7}`,
	} {
		if _, err := Parse([]byte(body)); !errors.Is(err, ErrEnvelopeKeyCase) {
			t.Fatalf("Parse(%s) = %v, want ErrEnvelopeKeyCase", body, err)
		}
	}
	// Exact routing keys with duplicated / case-variant ACTOR keys at both
	// levels: parsed, and after Stamp the agent decodes exactly what the
	// broker admitted, with the stamp owning both actor keys.
	for _, body := range []string{
		`{"op":"create","version":1,"call_id":7,"actor_fp":"forged","actor_fp":"forged2","ACTOR_FP":"forged3","exec":{"argv":["x"],"actor_fp":"forged","actor_fp":"forged2"}}`,
		`{"op":"create","version":1,"call_id":7,"Actor_fp":"forged","exec":{"argv":["x"],"aCtOr_Fp":"forged"}}`,
		`{"op":"resume","version":1,"call_id":9,"future_field":{"nested":[1,"two"]}}`,
	} {
		env, err := Parse([]byte(body))
		if err != nil {
			t.Fatalf("Parse(%s): %v", body, err)
		}
		out, err := env.Stamp("SHA256:stamp")
		if err != nil {
			t.Fatal(err)
		}
		var req proto.ProcessResumeReq
		if err := json.Unmarshal(out, &req); err != nil {
			t.Fatalf("decode after stamp: %v (%s)", err, out)
		}
		if req.Op != env.Op || req.Version != env.Version || req.CallID != env.CallID {
			t.Fatalf("agent would decode op=%q version=%d call_id=%d but the broker admitted op=%q version=%d call_id=%d\nbody:    %s\nstamped: %s",
				req.Op, req.Version, req.CallID, env.Op, env.Version, env.CallID, body, out)
		}
		if req.ActorFP != "SHA256:stamp" || (req.Exec != nil && req.Exec.ActorFP != "SHA256:stamp") {
			t.Fatalf("stamp did not own the actor keys the agent decodes: %+v exec=%+v\nstamped: %s", req.ActorFP, req.Exec, out)
		}
	}
}
