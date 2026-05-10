package proto

// Fuzz tests for the wire layer.
//
// Each FuzzXxx function runs the public message type's JSON decoder
// against a corpus of seed inputs (always exercised under `go test`)
// and against random mutations when invoked with `-fuzz=Xxx -fuzztime=…`.
//
// The contract under test is intentionally minimal: json.Unmarshal must
// not panic on any byte sequence (errors are fine; semantic validation
// happens at handlers). This catches stack overflows from deep nesting,
// unicode-handling crashes in the stdlib, and any future custom
// UnmarshalJSON impls that forget bounds checks.
//
// All fuzz functions also do a roundtrip on each accepted decode
// (Marshal again, Unmarshal again, compare) — this ensures the type's
// JSON shape is stable under fuzzed inputs we did manage to parse.

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// commonSeeds returns inputs that should be safe to feed to ANY
// json.Unmarshal target — most should fail to decode, but none may
// panic. Includes deeply-nested JSON to catch stack-overflow style
// regressions.
func commonSeeds() [][]byte {
	deep := strings.Repeat("[", 100) + strings.Repeat("]", 100)
	deepObj := strings.Repeat(`{"a":`, 100) + "1" + strings.Repeat("}", 100)
	longStr := `{"x":"` + strings.Repeat("A", 1<<14) + `"}`
	seeds := [][]byte{
		nil,
		[]byte(""),
		[]byte("{}"),
		[]byte("null"),
		[]byte("[]"),
		[]byte(`{"foo":bar}`),
		[]byte("{\"\x00\":\"\x00\"}"),
		[]byte(`{"k":"\uD800"}`), // lone surrogate
		[]byte(`{"future_field":"x"}`),
		[]byte(deep),
		[]byte(deepObj),
		[]byte(longStr),
	}
	return seeds
}

// addCommonSeeds attaches commonSeeds() plus the supplied extras.
func addCommonSeeds(f *testing.F, extra ...[]byte) {
	for _, s := range commonSeeds() {
		f.Add(s)
	}
	for _, s := range extra {
		f.Add(s)
	}
}

// fuzzInto is the shared body: feed `data` to json.Unmarshal targeting
// a fresh value, and if it decodes, re-marshal + re-unmarshal to make
// sure the type's JSON shape is idempotent (nothing the fuzzer found
// pushes the type through a non-fixed-point cycle).
func fuzzInto[T any](t *testing.T, data []byte) {
	t.Helper()
	var v T
	if err := json.Unmarshal(data, &v); err != nil {
		return // decode failure is fine — only panics matter
	}
	first, err := json.Marshal(&v)
	if err != nil {
		t.Fatalf("re-marshal failed for %T: %v\ninput=%q", v, err, data)
	}
	var v2 T
	if err := json.Unmarshal(first, &v2); err != nil {
		t.Fatalf("second unmarshal failed for %T: %v\nbytes=%s", v, err, first)
	}
	second, err := json.Marshal(&v2)
	if err != nil {
		t.Fatalf("third marshal failed for %T: %v", v, err)
	}
	if !bytes.Equal(first, second) {
		t.Fatalf("non-idempotent JSON shape for %T:\n  first:  %s\n  second: %s",
			v, first, second)
	}
}

// ---- Fuzz functions, one per public message type. ----------------

func FuzzUpgradeReq(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"url":"https://x","sha256":"deadbeef","proto_version":1}`),
		[]byte(`{"url":"","sha256":"","proto_version":0}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[UpgradeReq](t, data) })
}

func FuzzUpgradeResp(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"ok":true}`),
		[]byte(`{"ok":false,"code":"forbidden","error":"x"}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[UpgradeResp](t, data) })
}

func FuzzRunChunk(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"kind":"ready","pid":"01h","cols":80,"rows":24}`),
		[]byte(`{"kind":"exit","exit_code":0}`),
		// Future kind — must roundtrip with the unknown string preserved.
		[]byte(`{"kind":"future_kind","reason":"x"}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[RunChunk](t, data) })
}

func FuzzExecChunk(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"kind":"started","pid":"01h"}`),
		[]byte(`{"kind":"stdout","data":"aGVsbG8="}`),
		[]byte(`{"kind":"future_kind","data":"AA=="}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[ExecChunk](t, data) })
}

func FuzzExecReq(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"argv":["echo","hi"]}`),
		[]byte(`{"argv":[],"env":{"K":"V"},"cwd":"/tmp","stdin":"AA=="}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[ExecReq](t, data) })
}

func FuzzRunReq(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"argv":["bash"]}`),
		[]byte(`{"argv":["bash"],"cols":80,"rows":24,"actor_fp":"SHA256:x"}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[RunReq](t, data) })
}

func FuzzExposeReq(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"name":"jupyter","local_port":8888}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[ExposeReq](t, data) })
}

func FuzzExposeResp(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"port":14000,"public_host":"x","name":"jupyter"}`),
		[]byte(`{"code":"port_exhausted","error":"x"}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[ExposeResp](t, data) })
}

func FuzzExposeRmReq(f *testing.F) {
	addCommonSeeds(f, []byte(`{"name":"jupyter"}`))
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[ExposeRmReq](t, data) })
}

func FuzzExposeRmResp(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"ok":true,"port":14000}`),
		[]byte(`{"ok":false,"code":"not_found"}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[ExposeRmResp](t, data) })
}

func FuzzKillReq(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"pid":"01h","signal":2}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[KillReq](t, data) })
}

func FuzzSessionCreateReq(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"name":"lab","pin":"123456"}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[SessionCreateReq](t, data) })
}

func FuzzSessionCreateResp(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"sid":"lab","owner_fp":"SHA256:x","created_at":"2026-05-08T10:00:00Z"}`),
		[]byte(`{"sid":"","owner_fp":"","created_at":"0001-01-01T00:00:00Z","error":"e"}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[SessionCreateResp](t, data) })
}

func FuzzSessionListResp(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"sessions":[]}`),
		[]byte(`{"sessions":null,"code":"x","error":"y"}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[SessionListResp](t, data) })
}

func FuzzSessionRmReq(f *testing.F) {
	addCommonSeeds(f, []byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[SessionRmReq](t, data) })
}

func FuzzSessionRmResp(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"ok":true}`),
		[]byte(`{"ok":false,"code":"not_found"}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[SessionRmResp](t, data) })
}

func FuzzPsResp(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"processes":[]}`),
		[]byte(`{"processes":[{"pid":"01h","nid":"lab-1","argv":["x"],"started_at":"2026-05-08T10:00:00Z","status":"RUNNING"}],"ports":[]}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[PsResp](t, data) })
}

func FuzzNodeListReq(f *testing.F) {
	addCommonSeeds(f, []byte(`{}`))
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[NodeListReq](t, data) })
}

func FuzzNodeListResp(f *testing.F) {
	addCommonSeeds(f,
		[]byte(`{"nodes":[]}`),
		[]byte(`{"nodes":[{"nid":"lab-1","status":"ONLINE","last_heartbeat_at":"2026-05-08T10:00:00Z"}]}`),
	)
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[NodeListResp](t, data) })
}

func FuzzPtyAttachEvent(f *testing.F) {
	addCommonSeeds(f, []byte(`{"cols":80,"rows":24}`))
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[PtyAttachEvent](t, data) })
}

func FuzzPtyResizeEvent(f *testing.F) {
	addCommonSeeds(f, []byte(`{"cols":120,"rows":40}`))
	f.Fuzz(func(t *testing.T, data []byte) { fuzzInto[PtyResizeEvent](t, data) })
}

// FuzzSubjectParsers feeds random subject strings through every parser
// to assert no parser panics on adversarial input. Decoders may return
// ok=false; that's the expected branch for garbage.
func FuzzSubjectParsers(f *testing.F) {
	seeds := []string{
		"",
		".",
		"tether",
		"tether.v1",
		"tether.v1.s..cmd.by..node...req",
		"tether.v1.s.lab.cmd.by.U.node.lab-1.run.req",
		"tether.v1.ctrl.by.UABCD.session.create.req",
		"tether.v1.s.lab.ev.node.lab-1.proc.01hzxk.exit",
		"tether.v1.ctrl.s.lab.node.lab-1.register.req",
		strings.Repeat("a.", 200) + "b",
		"tether.v1.s.lab.cmd.by.UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA.node.lab-1.run.req",
		"tether.v1.s.*.cmd.by.>.node.*.>.req",
	}
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		_, _, _, _, _ = ParseCmdBy(s)
		_, _, _, _, _ = ParseEvProc(s)
		_, _, _ = ParseCtrlBy(s)
		_, _, _ = ParseSidNidFromCtrl(s)
	})
}
