package proto

// Wire invariants for the protocol layer.
//
// These tests exercise three orthogonal properties that any wire-stable
// protocol module must satisfy:
//
//   1. Marshal → Unmarshal → Marshal is a fixed point (roundtrip).
//   2. Forward compat: today's decoders silently ignore tomorrow's
//      unknown fields, so a v(N+1) producer can talk to a v(N) consumer.
//   3. []byte payloads survive bit-for-bit through JSON base64 encoding,
//      including NUL bytes, non-UTF-8 sequences, and 10 MiB payloads.
//   4. Bundles like "ok=true with code=foo" or "ok=false with empty
//      strings" decode cleanly (semantic interpretation is the handler's
//      job; the wire layer must not reject them).

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// allRoundtripCases is the catalogue of public message types that must
// satisfy the Marshal→Unmarshal→Marshal fixed point. Each entry gives a
// concrete populated value AND a fresh empty target for unmarshal.
//
// Keep this list in sync with the type catalogue in messages.go — adding
// a new public message type without an entry here will silently skip
// roundtrip coverage.
type roundtripCase struct {
	name  string
	value any
	zero  any
}

func allRoundtripCases() []roundtripCase {
	t0 := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	rc := 42
	return []roundtripCase{
		{"UpgradeReq", &UpgradeReq{
			URL: "https://x/y.tgz", SHA256: "deadbeef", ProtoVersion: 2, ActorFP: "SHA256:fp",
		}, &UpgradeReq{}},
		{"UpgradeResp", &UpgradeResp{OK: true, Code: "x", Error: "y"}, &UpgradeResp{}},
		{"RunChunk", &RunChunk{
			Kind: "ready", PID: "01h", Cols: 80, Rows: 24, ExitCode: 0,
		}, &RunChunk{}},
		{"ExecChunk", &ExecChunk{
			Kind: "stdout", PID: "01h", Data: []byte("hi"),
		}, &ExecChunk{}},
		{"ExecReq", &ExecReq{
			Argv: []string{"echo", "hi"}, Env: map[string]string{"K": "V"},
			Cwd: "/tmp", Stdin: []byte("in"), Safe: true, ActorFP: "SHA256:fp",
		}, &ExecReq{}},
		{"RunReq", &RunReq{
			Argv: []string{"bash"}, Env: map[string]string{"K": "V"},
			Cwd: "/tmp", Cols: 80, Rows: 24, Safe: true, ActorFP: "SHA256:fp",
		}, &RunReq{}},
		{"ExposeReq", &ExposeReq{
			Name: "jupyter", LocalPort: 8888, RemotePort: 14005, ActorFP: "SHA256:fp",
			RebuildOff: true, OnBroker: "brk-b", // B4
		}, &ExposeReq{}},
		{"ExposeResp", &ExposeResp{
			Port: 14000, PublicHost: "h", Name: "jupyter",
			HomeBroker: "brk-b", Epoch: 2, // B4
		}, &ExposeResp{}},
		{"ExposeRmReq", &ExposeRmReq{Name: "jupyter", ActorFP: "SHA256:fp"}, &ExposeRmReq{}},
		{"ExposeRmResp", &ExposeRmResp{OK: true, Port: 14000}, &ExposeRmResp{}},
		{"KillReq", &KillReq{PID: "01h", Signal: 2, ActorFP: "SHA256:fp"}, &KillReq{}},
		{"KillResp", &KillResp{OK: true, Code: "x", Error: "y"}, &KillResp{}},
		{"SessionCreateReq", &SessionCreateReq{Name: "lab", PIN: "123456"}, &SessionCreateReq{}},
		{"SessionCreateResp", &SessionCreateResp{
			SID: "lab", OwnerFP: "SHA256:x", CreatedAt: t0,
		}, &SessionCreateResp{}},
		{"SessionListReq", &SessionListReq{}, &SessionListReq{}},
		{"SessionListResp", &SessionListResp{Sessions: []SessionEntry{
			{SID: "lab", Name: "lab", OwnerFP: "SHA256:x", State: "ACTIVE", CreatedAt: t0, IsOwner: true},
		}}, &SessionListResp{}},
		{"SessionRmReq", &SessionRmReq{}, &SessionRmReq{}},
		{"SessionRmResp", &SessionRmResp{OK: true}, &SessionRmResp{}},
		{"PsResp", &PsResp{Processes: []PsEntry{{
			PID: "01h", NID: "lab-1", Argv: []string{"x"}, StartedAt: t0,
			Status: "RUNNING", StartedByFP: "SHA256:x",
		}}, Ports: []PsPortEntry{{ // B4: home/epoch/rebuild on the read projection
			Port: 14000, Name: "jupyter", NID: "lab-1", LocalPort: 8888, State: "ALLOCATED",
			CreatedAt: t0, HomeBroker: "brk-b", Epoch: 2, RebuildOff: true,
		}}}, &PsResp{}},
		{"NodeListReq", &NodeListReq{}, &NodeListReq{}},
		{"NodeListResp", &NodeListResp{Nodes: []NodeListEntry{{
			NID: "lab-1", Status: "ONLINE", LastHeartbeatAt: t0,
			BootID: "x", ReleaseVersion: "v0.0.0-dev", ProtoVersion: 2,
		}}}, &NodeListResp{}},
		{"PtyAttachEvent", &PtyAttachEvent{Cols: 80, Rows: 24}, &PtyAttachEvent{}},
		{"PtyResizeEvent", &PtyResizeEvent{Cols: 80, Rows: 24}, &PtyResizeEvent{}},
		// Bonus structs that participate on the wire but weren't in the
		// minimum list — covering them keeps the catalogue closed.
		{"NodeRegisterReq", &NodeRegisterReq{
			ProtoVersion: 2, ReleaseVersion: "v0.0.0-dev", NID: "lab-1",
			OS: "linux", Arch: "amd64", BootID: "deadbeef",
			LocalProcesses: []LocalProcess{{
				PID: "01h", State: "exited", RC: &rc,
				StartedAt: t0, StartTimeTicks: 12345,
			}},
			LocalPorts: []LocalPort{{
				Port: 14000, Name: "jupyter", LocalPort: 8888, TokenHash: "abc",
			}},
		}, &NodeRegisterReq{}},
		{"NodeRegisterResp", &NodeRegisterResp{
			OK: true, AcceptedProcesses: []string{"01h"},
			ReconciledProcesses: []ReconciledProc{{PID: "02h", NewState: "EXITED", RC: -1}},
			KeepPorts:           []int{14000}, RevokePorts: []int{14001},
			DropProcesses: []string{"03h"},
		}, &NodeRegisterResp{}},
		{"HeartbeatPayload", &HeartbeatPayload{Ts: t0}, &HeartbeatPayload{}},
		{"ErrorReply", &ErrorReply{Code: "x", Message: "y"}, &ErrorReply{}},
		{"ProcStartedEvent", &ProcStartedEvent{
			PID: "01h", Argv: []string{"x"}, StartedAt: t0,
			StartedByFP: "SHA256:x", BootID: "b", StartTimeTicks: 12345,
		}, &ProcStartedEvent{}},
		{"ProcExitEvent", &ProcExitEvent{
			PID: "01h", ExitCode: 0, EndedAt: t0,
		}, &ProcExitEvent{}},
		{"PortEvent", &PortEvent{
			Port: 14000, Name: "jupyter", NID: "lab-1", LocalPort: 8888,
			Kind: "allocated", Ts: t0,
		}, &PortEvent{}},
		{"ExposeForwardedReq", &ExposeForwardedReq{
			Name: "jupyter", Port: 14000, LocalPort: 8888, Token: "tok", ActorFP: "fp",
		}, &ExposeForwardedReq{}},
		{"ExposeForwardedResp", &ExposeForwardedResp{OK: true}, &ExposeForwardedResp{}},
		{"ExposeRmForwardedReq", &ExposeRmForwardedReq{Name: "jupyter", Port: 14000}, &ExposeRmForwardedReq{}},
		{"UpgradeForwardedReq", &UpgradeForwardedReq{URL: "https://x", SHA256: "deadbeef", ActorFP: "fp"}, &UpgradeForwardedReq{}},
		{"UpgradeForwardedResp", &UpgradeForwardedResp{
			OK: true, NewVersion: "v0.0.1", BinaryReplaced: true,
		}, &UpgradeForwardedResp{}},
		{"PtyFailedEvent", &PtyFailedEvent{PID: "01h", Reason: "attach_timeout"}, &PtyFailedEvent{}},
	}
}

// TestRoundtripAllMessages: Marshal → Unmarshal (into a fresh value
// of the same type) → Marshal again → bytes equal to the first marshal.
//
// This guards against silent JSON-tag drift and catches any future
// custom MarshalJSON that isn't an inverse of UnmarshalJSON.
func TestRoundtripAllMessages(t *testing.T) {
	for _, c := range allRoundtripCases() {
		t.Run(c.name, func(t *testing.T) {
			first, err := json.Marshal(c.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			if err := json.Unmarshal(first, c.zero); err != nil {
				t.Fatalf("unmarshal: %v\nbytes=%s", err, first)
			}
			second, err := json.Marshal(c.zero)
			if err != nil {
				t.Fatalf("re-marshal: %v", err)
			}
			if !bytes.Equal(first, second) {
				t.Fatalf("roundtrip mismatch:\n  first:  %s\n  second: %s", first, second)
			}
			// reflect.DeepEqual on the decoded value vs. the original is a
			// stronger check than byte-equality (catches map ordering).
			if !reflect.DeepEqual(c.value, c.zero) {
				t.Fatalf("decoded value differs from original:\n  orig:    %#v\n  decoded: %#v", c.value, c.zero)
			}
		})
	}
}

// TestUnknownFieldsIgnored: every Resp / event type must silently
// ignore future fields. This is the forward-compat contract — without
// it, a v(N+1) producer talking to a v(N) consumer would error out and
// break rolling upgrades.
//
// Strategy: marshal a known-good value, splice a "future_field" into
// the JSON object, and re-decode. The decode must succeed and the
// known fields must be preserved.
func TestUnknownFieldsIgnored(t *testing.T) {
	for _, c := range allRoundtripCases() {
		t.Run(c.name, func(t *testing.T) {
			orig, err := json.Marshal(c.value)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			// Splice an unknown field into the top-level object.
			withUnknown := injectUnknownField(orig, `"future_field":"x"`)
			if withUnknown == nil {
				// Empty struct types serialise as "{}", which still accepts
				// the splice; if the type's serialisation isn't a JSON object,
				// skip rather than fail (none of our types should hit this,
				// but the helper is defensive).
				t.Skipf("not a JSON object: %s", orig)
			}
			// Decode into a freshly-zeroed target of the same type.
			fresh := reflect.New(reflect.TypeOf(c.value).Elem()).Interface()
			if err := json.Unmarshal(withUnknown, fresh); err != nil {
				t.Fatalf("unknown-field decode rejected: %v\nbytes=%s", err, withUnknown)
			}
			// The decoded value (minus the unknown field) must marshal
			// back to the same JSON we started with.
			reMarshalled, err := json.Marshal(fresh)
			if err != nil {
				t.Fatalf("re-marshal: %v", err)
			}
			if !bytes.Equal(reMarshalled, orig) {
				t.Fatalf("unknown field altered known fields:\n  orig: %s\n  got:  %s",
					orig, reMarshalled)
			}
		})
	}
}

// injectUnknownField returns a copy of src with `field` inserted after
// the opening '{'. Returns nil when src isn't a top-level JSON object.
func injectUnknownField(src []byte, field string) []byte {
	if len(src) < 2 || src[0] != '{' {
		return nil
	}
	if bytes.Equal(src, []byte("{}")) {
		return []byte("{" + field + "}")
	}
	out := make([]byte, 0, len(src)+len(field)+1)
	out = append(out, '{')
	out = append(out, []byte(field)...)
	out = append(out, ',')
	out = append(out, src[1:]...)
	return out
}

// TestRunChunkUnknownKindPreserved exercises forward-compat at the
// enum level (RunChunk.Kind / ExecChunk.Kind are open-ended strings,
// not Go enums). Today's decoder must accept "future_kind" as Kind
// without panic and must NOT clobber the kind string.
func TestRunChunkUnknownKindPreserved(t *testing.T) {
	cases := [][]byte{
		[]byte(`{"kind":"future_kind"}`),
		[]byte(`{"kind":"future_kind","reason":"x"}`),
		[]byte(`{"kind":"future_kind","exit_code":7,"pid":"01h"}`),
	}
	for _, raw := range cases {
		var rc RunChunk
		if err := json.Unmarshal(raw, &rc); err != nil {
			t.Fatalf("RunChunk decode rejected unknown kind: %v\nbytes=%s", err, raw)
		}
		if rc.Kind != "future_kind" {
			t.Fatalf("RunChunk.Kind not preserved: got %q want %q (input=%s)", rc.Kind, "future_kind", raw)
		}
	}
	for _, raw := range cases {
		var ec ExecChunk
		if err := json.Unmarshal(raw, &ec); err != nil {
			t.Fatalf("ExecChunk decode rejected unknown kind: %v\nbytes=%s", err, raw)
		}
		if ec.Kind != "future_kind" {
			t.Fatalf("ExecChunk.Kind not preserved: got %q want %q", ec.Kind, "future_kind")
		}
	}
}

// TestBinaryPayloadRoundtrip — `[]byte` fields encode as base64 JSON
// strings. The wire layer must guarantee bit-for-bit fidelity on:
//
//   - NUL bytes,
//   - non-UTF-8 sequences (e.g. lone 0x80, 0xFF),
//   - large payloads (10 MiB upper bound; PTY chunks won't be that big
//     but RunChunk / ExecChunk are the only place we intentionally
//     bound this and it's worth pinning).
func TestBinaryPayloadRoundtrip(t *testing.T) {
	const tenMiB = 10 << 20
	big := make([]byte, tenMiB)
	if _, err := rand.Read(big); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}

	payloads := map[string][]byte{
		"empty":         {},
		"only_nul":      {0, 0, 0, 0, 0, 0, 0, 0},
		"text_with_nul": []byte("hi\x00there\x00\x00"),
		"non_utf8":      {0x80, 0x81, 0xFF, 0xFE, 0xC0, 0xC1},
		"all_bytes": func() []byte {
			b := make([]byte, 256)
			for i := range b {
				b[i] = byte(i)
			}
			return b
		}(),
		"10_MiB": big,
	}

	for name, payload := range payloads {
		t.Run("ExecChunk/"+name, func(t *testing.T) {
			ec := ExecChunk{Kind: "stdout", Data: payload}
			b, err := json.Marshal(&ec)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var out ExecChunk
			if err := json.Unmarshal(b, &out); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !bytes.Equal(out.Data, payload) {
				t.Fatalf("byte payload mismatch (len=%d): got_len=%d", len(payload), len(out.Data))
			}
		})
		t.Run("ExecReq.Stdin/"+name, func(t *testing.T) {
			er := ExecReq{Argv: []string{"x"}, Stdin: payload}
			b, err := json.Marshal(&er)
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			var out ExecReq
			if err := json.Unmarshal(b, &out); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if !bytes.Equal(out.Stdin, payload) {
				t.Fatalf("stdin payload mismatch (len=%d): got_len=%d", len(payload), len(out.Stdin))
			}
		})
	}
}

// TestOKCodeErrorCombinations: verify that the wire layer accepts every
// combination of (ok, code, error) without erroring out — semantic
// validation (e.g. "ok=true should not also have code=…") is the
// handler's responsibility, NOT the decoder's. We also check both
// `code` empty/non-empty and `error` empty/non-empty cross product.
//
// Pattern: pick one representative resp type from each shape we use
// (ok+code+error vs. just code+error) and table-drive the decode.
func TestOKCodeErrorCombinations(t *testing.T) {
	type respCase struct {
		name   string
		raw    string
		decode func(t *testing.T, raw string)
	}

	check := func(t *testing.T, raw string, target any) {
		if err := json.Unmarshal([]byte(raw), target); err != nil {
			t.Fatalf("decode failed (must accept any combination): %v\ninput=%s", err, raw)
		}
	}

	cases := []respCase{
		{"UpgradeResp/ok+code+error", `{"ok":true,"code":"foo","error":"bar"}`,
			func(t *testing.T, raw string) { check(t, raw, &UpgradeResp{}) }},
		{"UpgradeResp/ok=false+empty", `{"ok":false,"code":"","error":""}`,
			func(t *testing.T, raw string) { check(t, raw, &UpgradeResp{}) }},
		{"UpgradeResp/ok=true+empty", `{"ok":true}`,
			func(t *testing.T, raw string) { check(t, raw, &UpgradeResp{}) }},
		{"SessionRmResp/ok=true+code=x", `{"ok":true,"code":"x"}`,
			func(t *testing.T, raw string) { check(t, raw, &SessionRmResp{}) }},
		{"SessionListResp/code+error", `{"sessions":null,"code":"x","error":"y"}`,
			func(t *testing.T, raw string) { check(t, raw, &SessionListResp{}) }},
		{"SessionListResp/empty", `{}`,
			func(t *testing.T, raw string) { check(t, raw, &SessionListResp{}) }},
		{"NodeListResp/empty", `{}`,
			func(t *testing.T, raw string) { check(t, raw, &NodeListResp{}) }},
		{"NodeListResp/code+error", `{"nodes":[],"code":"actor_invalid","error":"..."}`,
			func(t *testing.T, raw string) { check(t, raw, &NodeListResp{}) }},
		{"PsResp/code+error", `{"processes":[],"code":"x","error":"y"}`,
			func(t *testing.T, raw string) { check(t, raw, &PsResp{}) }},
		{"ExposeResp/all", `{"port":1,"public_host":"h","name":"n","code":"x","error":"y"}`,
			func(t *testing.T, raw string) { check(t, raw, &ExposeResp{}) }},
		{"ExposeRmResp/ok=true+code", `{"ok":true,"port":14000,"code":"foo","error":"bar"}`,
			func(t *testing.T, raw string) { check(t, raw, &ExposeRmResp{}) }},
		{"KillResp/all", `{"ok":false,"code":"pid_unknown","error":"x"}`,
			func(t *testing.T, raw string) { check(t, raw, &KillResp{}) }},
		{"NodeRegisterResp/all", `{"ok":true,"code":"x","error":"y","accepted_processes":[]}`,
			func(t *testing.T, raw string) { check(t, raw, &NodeRegisterResp{}) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { c.decode(t, c.raw) })
	}
}

// TestProtoVersionStillPositive guards against accidental regression of
// TestProtoVersionPositive — and pins the relationship between ProtoVersion,
// SubjectVersionToken and SubjectPrefix as a VERSION-AGNOSTIC invariant.
//
// The old form (`!HasSuffix(.v1) && ProtoVersion==1`) silently passed vacuously
// the moment ProtoVersion left 1, so it provided no safety net for the D0 v2
// flip. This form fires on ANY mismatch and never needs editing on a future
// bump: SubjectVersionToken must be exactly "v<ProtoVersion>", and SubjectPrefix
// must be "tether." + SubjectVersionToken (the single source of truth, §SSOT).
func TestProtoVersionStillPositive(t *testing.T) {
	if ProtoVersion < 1 {
		t.Fatalf("ProtoVersion must be ≥ 1, got %d", ProtoVersion)
	}
	wantToken := "v" + strconv.Itoa(ProtoVersion)
	if SubjectVersionToken != wantToken {
		t.Fatalf("SubjectVersionToken=%q must be %q for ProtoVersion=%d",
			SubjectVersionToken, wantToken, ProtoVersion)
	}
	if wantPrefix := "tether." + SubjectVersionToken; SubjectPrefix != wantPrefix {
		t.Fatalf("SubjectPrefix=%q must be %q (derived from SubjectVersionToken)",
			SubjectPrefix, wantPrefix)
	}
}

// TestReleaseVersionDefaultNotEmpty mirrors the existing test under a
// new name so it shows up in the invariants suite without duplicating
// failure noise.
func TestReleaseVersionDefaultNotEmpty(t *testing.T) {
	if ReleaseVersion == "" {
		t.Fatal("ReleaseVersion default must not be empty")
	}
}
