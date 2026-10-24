package procforward

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

// origin: docs/reviews/hot-update-reliable-exec-plan.md §2.2 — a field the
// broker's release does not know must reach the agent as a value, and every
// case-variant of the actor key must be replaced by the broker's stamp.
func TestStampPreservesUnknownFieldsAndOwnsTheActorKey(t *testing.T) {
	body := []byte(`{"version":1,"call_id":7,"op":"create","Actor_FP":"forged","future_top":{"a":[1,2]},` +
		`"exec":{"argv":["x"],"ACTOR_fp":"forged2","future_exec":true},"handle":{"request_id":"r"}}`)
	env, err := Parse(body)
	if err != nil {
		t.Fatal(err)
	}
	if env.CallID != 7 || env.Version != 1 || env.Op != "create" {
		t.Fatalf("envelope: %+v", env)
	}
	out, err := env.Stamp("SHA256:stamped")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	if got["actor_fp"] != "SHA256:stamped" || got["Actor_FP"] != nil {
		t.Fatalf("top-level actor not owned by the stamp: %s", out)
	}
	exec := got["exec"].(map[string]any)
	if kept, _ := exec["future_exec"].(bool); exec["actor_fp"] != "SHA256:stamped" || exec["ACTOR_fp"] != nil || !kept {
		t.Fatalf("exec actor/unknown field: %s", out)
	}
	if strings.Count(string(out), "actor_fp") != 2 {
		t.Fatalf("expected exactly two actor keys: %s", out)
	}
	if got["future_top"].(map[string]any)["a"].([]any)[1] != float64(2) || got["handle"].(map[string]any)["request_id"] != "r" {
		t.Fatalf("unknown top-level fields lost: %s", out)
	}
	// The stamped body still decodes into the release's struct with the
	// broker's fingerprint, whatever the client sent.
	var req proto.ProcessResumeReq
	if err := json.Unmarshal(out, &req); err != nil || req.ActorFP != "SHA256:stamped" || req.Exec == nil || req.Exec.ActorFP != "SHA256:stamped" {
		t.Fatalf("struct decode after stamp: %+v err=%v", req, err)
	}
}

func TestParseRefusesMalformedEnvelopes(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
		want error
	}{
		{"array", `[1]`, ErrNotObject},
		{"null", `null`, ErrNotObject},
		{"trailing", `{"call_id":1} {"x":1}`, ErrNotObject},
		{"exec_array", `{"call_id":1,"exec":[]}`, ErrExecShape},
		{"exec_string", `{"call_id":1,"exec":"x"}`, ErrExecShape},
		{"negative_call", `{"call_id":-1}`, ErrCallID},
		{"float_call", `{"call_id":1.5}`, ErrCallID},
		{"exp_call", `{"call_id":1e3}`, ErrCallID},
		{"string_call", `{"call_id":"7"}`, ErrCallID},
		{"overflow_call", `{"call_id":18446744073709551616}`, ErrCallID},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Parse([]byte(tc.body)); !errors.Is(err, tc.want) {
				t.Fatalf("Parse(%s) = %v, want %v", tc.body, err, tc.want)
			}
		})
	}
	env, err := Parse([]byte(`{"op":"probe","exec":null}`))
	if err != nil || env.CallID != 0 || env.hasExec {
		t.Fatalf("missing call_id / null exec must parse: %+v err=%v", env, err)
	}
	if _, err := Parse(make([]byte, MaxRequestBytes+1)); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("oversize body accepted: %v", err)
	}
	if env, err := Parse([]byte(`{"call_id":18446744073709551615,"version":1,"op":"ack"}`)); err != nil || env.CallID != ^uint64(0) {
		t.Fatalf("max call_id: %+v err=%v", env, err)
	}
}

func TestStampIsIdempotentAndKeepsExecAbsent(t *testing.T) {
	env, err := Parse([]byte(`{"version":1,"call_id":3,"op":"resume","handle":{"request_id":"r"}}`))
	if err != nil {
		t.Fatal(err)
	}
	first, err := env.Stamp("SHA256:a")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(first), `"exec"`) {
		t.Fatalf("stamp fabricated an exec object: %s", first)
	}
	again, err := Parse(first)
	if err != nil {
		t.Fatal(err)
	}
	second, err := again.Stamp("SHA256:b")
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(second, &got); err != nil || got["actor_fp"] != "SHA256:b" || strings.Count(string(second), "actor_fp") != 1 {
		t.Fatalf("re-stamp did not replace the previous stamp: %s err=%v", second, err)
	}
}

func FuzzParseStamp(f *testing.F) {
	f.Add([]byte(`{"version":1,"call_id":1,"op":"create","exec":{"argv":["a"]}}`))
	f.Add([]byte(`{"call_id":"x"}`))
	f.Add([]byte(`{"Actor_FP":1,"exec":{"actor_FP":[]}}`))
	f.Add([]byte(`[]`))
	f.Fuzz(func(t *testing.T, body []byte) {
		env, err := Parse(body)
		if err != nil {
			return
		}
		out, err := env.Stamp("SHA256:fuzz")
		if err != nil {
			t.Fatalf("stamp failed on a parsed body: %v", err)
		}
		var got map[string]json.RawMessage
		if err := json.Unmarshal(out, &got); err != nil {
			t.Fatalf("stamp emitted invalid JSON: %v", err)
		}
		if string(got["actor_fp"]) != `"SHA256:fuzz"` {
			t.Fatalf("stamp lost the actor: %s", out)
		}
		for k := range got {
			if k != "actor_fp" && strings.EqualFold(k, "actor_fp") {
				t.Fatalf("case variant survived: %s", out)
			}
		}
		if raw, ok := got["exec"]; ok {
			var exec map[string]json.RawMessage
			if err := json.Unmarshal(raw, &exec); err != nil || string(exec["actor_fp"]) != `"SHA256:fuzz"` {
				t.Fatalf("exec stamp: %s", out)
			}
		}
	})
}
