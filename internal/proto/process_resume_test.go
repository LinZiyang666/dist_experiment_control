// process_resume_test.go — wire shape of the reliable-exec output batch.
package proto

import (
	"encoding/json"
	"strings"
	"testing"
)

// origin: docs/reviews/hot-update-stable-tree-plan.md §3 T15
//
// An aborted batch must never carry a FIN key and a FIN batch must never carry
// an error key: the two end markers are mutually exclusive on the wire, and an
// older peer that knows nothing about aborts must decode a clean stream with
// every zero value legal.
func TestProcessOutputAbortedNeverEncodesFIN(t *testing.T) {
	aborted, err := json.Marshal(ProcessOutput{Offset: 3, Error: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if s := string(aborted); strings.Contains(s, `"fin"`) || strings.Contains(s, `"data"`) {
		t.Fatalf("aborted batch encodes a FIN or data key: %s", s)
	}
	clean, err := json.Marshal(ProcessOutput{Offset: 3, FIN: true})
	if err != nil {
		t.Fatal(err)
	}
	if s := string(clean); strings.Contains(s, `"error"`) {
		t.Fatalf("FIN batch encodes an error key: %s", s)
	}
	var decoded ProcessOutput
	if err := json.Unmarshal([]byte(`{"offset":3,"error":"x"}`), &decoded); err != nil || decoded.FIN || decoded.Error != "x" || decoded.Offset != 3 {
		t.Fatalf("aborted batch decoded wrongly: %+v err=%v", decoded, err)
	}
	var zero ProcessOutput
	if err := json.Unmarshal([]byte(`{"offset":0}`), &zero); err != nil || zero.FIN || zero.Error != "" || zero.Offset != 0 || len(zero.Data) != 0 {
		t.Fatalf("a batch without end markers must decode to the zero value: %+v err=%v", zero, err)
	}
}

// origin: docs/reviews/hot-update-reliable-exec-plan.md §2.1 — the two
// fields S1 adds must vanish at their zero value so a peer that predates them
// sees exactly the S0 wire, and must decode from a body that omits them.
func TestProcessOutputCodeAndStartedUnackedAreOmitEmpty(t *testing.T) {
	reply, err := json.Marshal(ProcessResumeReply{Version: 1, CallID: 2, Kind: "status"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(reply), `"code"`) {
		t.Fatalf("zero Code must be omitted: %s", reply)
	}
	row, err := json.Marshal(LocalProcess{PID: "p", State: "running"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(row), `started_unacked`) {
		t.Fatalf("zero StartedUnacked must be omitted: %s", row)
	}
	var decoded ProcessResumeReply
	if err := json.Unmarshal([]byte(`{"version":1,"call_id":2,"kind":"error","error":"x"}`), &decoded); err != nil || decoded.Code != "" || decoded.Error != "x" {
		t.Fatalf("legacy error reply decoded wrongly: %+v err=%v", decoded, err)
	}
	var withCode ProcessResumeReply
	if err := json.Unmarshal([]byte(`{"version":1,"call_id":2,"kind":"error","code":"process_expired","error":"x"}`), &withCode); err != nil || withCode.Code != CodeProcessExpired {
		t.Fatalf("Code not decoded: %+v err=%v", withCode, err)
	}
}

// origin: hot-update S4 CLOSE admission contract / architecture §21.2
func TestProcessRecoveryRefusalWireCompatibility(t *testing.T) {
	if CodeProcessRecovering != "process_recovering" || CodeProcessRecovering == CodeProcessBusy {
		t.Fatal("recovery lost its distinct stable code")
	}
	body, err := json.Marshal(ProcessResumeReply{Version: 1, CallID: 7, Kind: "error",
		Code: CodeProcessRecovering, Error: "retained recovery"})
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != `{"version":1,"call_id":7,"kind":"error","code":"process_recovering","error":"retained recovery"}` {
		t.Fatalf("additive refusal changed the existing envelope: %s", body)
	}
	// N-1's code-string envelope accepts unknown values; a still older
	// prose-only decoder ignores code and presents the original refusal.
	var previous struct {
		Version int    `json:"version"`
		CallID  uint64 `json:"call_id"`
		Kind    string `json:"kind"`
		Code    string `json:"code,omitempty"`
		Error   string `json:"error,omitempty"`
	}
	if err := json.Unmarshal(body, &previous); err != nil || previous.Kind != "error" ||
		previous.CallID != 7 || previous.Code != "process_recovering" || previous.Error != "retained recovery" {
		t.Fatalf("N-1 lost the final refusal envelope: %+v err=%v", previous, err)
	}
	var proseOnly struct {
		Kind  string `json:"kind"`
		Error string `json:"error,omitempty"`
	}
	if err := json.Unmarshal(body, &proseOnly); err != nil || proseOnly.Kind != "error" || proseOnly.Error != previous.Error {
		t.Fatal("prose-only reader lost the refusal")
	}
	zero, err := json.Marshal(ProcessResumeReply{Version: 1, CallID: 7, Kind: "status"})
	if err != nil || string(zero) != `{"version":1,"call_id":7,"kind":"status"}` {
		t.Fatalf("zero-value exec reply gained a recovery signal: %s err=%v", zero, err)
	}
}
