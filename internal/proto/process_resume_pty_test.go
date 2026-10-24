package proto_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
)

type wireField struct {
	name, tag, typ string
}

// origin: hot-update S4 plan §2.1 / T45
func TestPTYWireShapeAndZeroValueCompatibility(t *testing.T) {
	// Pin every old field too: a compatible addition cannot rename or retag
	// an exec field, or narrow a sequence just because its zero JSON is equal.
	reqFields := []wireField{
		{"Version", "version", "int"}, {"CallID", "call_id", "uint64"}, {"Op", "op", "string"},
		{"ClientLineage", "client_lineage,omitempty", "string"}, {"Handle", "handle,omitempty", "*proto.ProcessHandle"},
		{"AttachEpoch", "attach_epoch,omitempty", "uint64"}, {"Exec", "exec,omitempty", "*proto.ExecReq"},
		{"StdoutACK", "stdout_ack,omitempty", "uint64"}, {"StderrACK", "stderr_ack,omitempty", "uint64"},
		{"StdoutFIN", "stdout_fin,omitempty", "bool"}, {"StderrFIN", "stderr_fin,omitempty", "bool"},
		{"TerminalID", "terminal_id,omitempty", "string"}, {"ActorFP", "actor_fp,omitempty", "string"},
		{"Kind", "kind,omitempty", "string"}, {"Run", "run,omitempty", "*proto.RunReq"},
		{"Input", "input,omitempty", "[]uint8"}, {"InputSeq", "input_seq,omitempty", "uint64"},
		{"InputFIN", "input_fin,omitempty", "bool"}, {"Resize", "resize,omitempty", "*proto.PtyResizeEvent"},
		{"Signal", "signal,omitempty", "int"}, {"ControlSeq", "control_seq,omitempty", "uint64"},
	}
	replyFields := []wireField{
		{"Version", "version", "int"}, {"CallID", "call_id", "uint64"}, {"Kind", "kind", "string"},
		{"RequestID", "request_id,omitempty", "string"}, {"PID", "pid,omitempty", "string"},
		{"TargetInstance", "target_instance,omitempty", "string"}, {"AttachEpoch", "attach_epoch,omitempty", "uint64"},
		{"Lease", "lease,omitempty", "*proto.ProcessLease"}, {"ExitCode", "exit_code,omitempty", "int"},
		{"Signal", "signal,omitempty", "int"}, {"TerminalID", "terminal_id,omitempty", "string"},
		{"Code", "code,omitempty", "string"}, {"Error", "error,omitempty", "string"}, {"Phase", "phase,omitempty", "string"},
		{"Stdout", "stdout,omitempty", "*proto.ProcessOutput"}, {"Stderr", "stderr,omitempty", "*proto.ProcessOutput"},
		{"TaskKind", "task_kind,omitempty", "string"}, {"PTYVersion", "pty_version,omitempty", "int"},
		{"InputACK", "input_ack,omitempty", "uint64"}, {"InputAccepted", "input_accepted,omitempty", "uint64"},
		{"InputWindow", "input_window,omitempty", "uint64"}, {"InputFIN", "input_fin,omitempty", "bool"},
		{"InputEnded", "input_ended,omitempty", "bool"}, {"InputError", "input_error,omitempty", "string"},
		{"ControlACK", "control_ack,omitempty", "uint64"}, {"ControlError", "control_error,omitempty", "string"},
	}
	assertWireFields(t, reflect.TypeFor[proto.ProcessResumeReq](), reqFields)
	assertWireFields(t, reflect.TypeFor[proto.ProcessResumeReply](), replyFields)
	if len(reqFields)-13 != 8 || len(replyFields)-16 != 10 {
		t.Fatal("the frozen PTY extension must contain exactly 8 request and 10 reply fields")
	}
	for _, tc := range []struct {
		name   string
		value  any
		golden string
	}{
		{"zero request", proto.ProcessResumeReq{}, `{"version":0,"call_id":0,"op":""}`},
		{"zero reply", proto.ProcessResumeReply{}, `{"version":0,"call_id":0,"kind":""}`},
		{"exec request", proto.ProcessResumeReq{Version: 1, CallID: 2, Op: "ack", AttachEpoch: 3, StdoutACK: 4, StderrACK: 5, StdoutFIN: true, StderrFIN: true, TerminalID: "terminal", ActorFP: "actor"},
			`{"version":1,"call_id":2,"op":"ack","attach_epoch":3,"stdout_ack":4,"stderr_ack":5,"stdout_fin":true,"stderr_fin":true,"terminal_id":"terminal","actor_fp":"actor"}`},
		{"exec reply", proto.ProcessResumeReply{Version: 1, CallID: 2, Kind: "status", ExitCode: 23, Stdout: &proto.ProcessOutput{Offset: 4, FIN: true}, Stderr: &proto.ProcessOutput{Offset: 5, FIN: true}},
			`{"version":1,"call_id":2,"kind":"status","exit_code":23,"stdout":{"offset":4,"fin":true},"stderr":{"offset":5,"fin":true}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assertWireJSON(t, tc.value, tc.golden)
			decoded := reflect.New(reflect.TypeOf(tc.value))
			if err := json.Unmarshal([]byte(tc.golden), decoded.Interface()); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(decoded.Elem().Interface(), tc.value) {
				t.Fatalf("old exec JSON no longer decodes with legal terminal zero values: %+v", decoded.Elem().Interface())
			}
		})
	}
	t.Run("complete capability", func(t *testing.T) {
		if proto.PTYVersion != 1 || proto.ProcessKindPTY != "pty" || proto.ProcessResumeVersion != 1 {
			t.Fatal("PTY capability changed its frozen version or task kind")
		}
		lease := &proto.ProcessLease{Version: 1, LeaseID: "lease", TargetInstance: "instance", BootID: "boot", SID: "sid", NID: "nid", ActorFP: "actor", ClientLineage: "lineage", IssuedBootNS: 10, ExpiresBootNS: 20, MAC: "mac"}
		reply := proto.ProcessResumeReply{Version: 1, CallID: 7, Kind: "capability", TargetInstance: "instance", Lease: lease, TaskKind: proto.ProcessKindPTY, PTYVersion: proto.PTYVersion}
		assertWireJSON(t, reply, `{"version":1,"call_id":7,"kind":"capability","target_instance":"instance","lease":{"version":1,"lease_id":"lease","target_instance":"instance","boot_id":"boot","sid":"sid","nid":"nid","actor_fp":"actor","client_lineage":"lineage","issued_boot_ns":10,"expires_boot_ns":20,"mac":"mac"},"task_kind":"pty","pty_version":1}`)
		var decoded proto.ProcessResumeReply
		data, err := json.Marshal(reply)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(decoded, reply) {
			t.Fatalf("capability round trip lost scope or task kind: %+v", decoded)
		}
	})
	t.Run("payload mutual exclusion table", testPTYPayloadTable)
}

func assertWireFields(t *testing.T, typ reflect.Type, want []wireField) {
	t.Helper()
	if typ.NumField() != len(want) {
		t.Fatalf("%s fields=%d want=%d", typ, typ.NumField(), len(want))
	}
	for i, field := range want {
		got := typ.Field(i)
		if got.Name != field.name || got.Tag.Get("json") != field.tag || got.Type.String() != field.typ {
			t.Errorf("%s field %d = %s / %s / %s; want %+v", typ, i, got.Name, got.Tag.Get("json"), got.Type, field)
		}
	}
}

func assertWireJSON(t *testing.T, value any, want string) {
	t.Helper()
	got, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != want {
		t.Fatalf("wire JSON\n got: %s\nwant: %s", got, want)
	}
}

// The table is the wire contract as data, not a replacement for runtime
// validation. P6/P5 exercise actual rejection and absence of side effects.
// ACK fields may accompany data/control but never create or either probe.
func testPTYPayloadTable(t *testing.T) {
	common := []string{"version", "call_id", "op", "kind", "actor_fp"}
	attached := []string{"handle", "attach_epoch"}
	acks := []string{"stdout_ack", "stderr_ack", "stdout_fin", "stderr_fin"}
	for _, tc := range []struct {
		op, literal string
		allowed     []string
	}{
		{"probe", "probe", []string{"client_lineage"}},
		{proto.ProcessOpPTYProbe, "pty-probe", []string{"client_lineage"}},
		{"create", "create", append(slices.Clone(attached), "run")},
		{"resume", "resume", slices.Concat(attached, acks)},
		{"ack", "ack", slices.Concat(attached, acks)},
		{"terminal-ack", "terminal-ack", slices.Concat(attached, acks, []string{"terminal_id"})},
		{proto.ProcessOpInput, "input", slices.Concat(attached, acks, []string{"input_seq", "input", "input_fin"})},
		{proto.ProcessOpResize, "resize", slices.Concat(attached, acks, []string{"control_seq", "resize"})},
		{proto.ProcessOpSignal, "signal", slices.Concat(attached, acks, []string{"control_seq", "signal"})},
		{proto.ProcessOpKeepalive, "keepalive", slices.Concat(attached, acks, []string{"control_seq"})},
		{proto.ProcessOpHangup, "hangup", slices.Concat(attached, acks, []string{"control_seq"})},
	} {
		t.Run(tc.literal, func(t *testing.T) {
			if tc.op != tc.literal {
				t.Fatalf("op=%q want=%q", tc.op, tc.literal)
			}
			allowed := slices.Concat(common, tc.allowed)
			req := proto.ProcessResumeReq{Version: 1, CallID: 7, Op: tc.op, Kind: proto.ProcessKindPTY}
			v := reflect.ValueOf(&req).Elem()
			for i := 0; i < v.NumField(); i++ {
				name := strings.Split(v.Type().Field(i).Tag.Get("json"), ",")[0]
				if !slices.Contains(allowed, name) || slices.Contains(common, name) {
					continue
				}
				setWirePayload(v.Field(i))
			}
			data, err := json.Marshal(req)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			for name := range fields {
				if !slices.Contains(allowed, name) {
					t.Errorf("%s carries mutually exclusive %s", tc.op, name)
				}
			}
			for _, name := range tc.allowed {
				if _, ok := fields[name]; !ok {
					t.Errorf("%s lost populated payload %s", tc.op, name)
				}
			}
			var decoded proto.ProcessResumeReq
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(req, decoded) {
				t.Fatalf("payload changed during round trip: %s", data)
			}
		})
	}
}

func setWirePayload(v reflect.Value) {
	switch v.Kind() {
	case reflect.String:
		v.SetString("value")
	case reflect.Uint64:
		v.SetUint(1<<63 + 7)
	case reflect.Int:
		v.SetInt(15)
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Slice:
		v.SetBytes([]byte{0, 3, 4, 255})
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
	default:
		panic("unsupported test payload type")
	}
}

// origin: hot-update S4 plan §5.2
// origin: hot-update S6 plan §9.1 / T46
// origin: S7 internal review GATE-CONVERGE-contracts F4
func TestImageVersionLinesKeepDomainsBeforeFinalABI(t *testing.T) {
	// origin: hot-update S7 plan §6.2 / P01 — transfer-v1 is the next
	// additive domain, appended after broker-image-v1.
	if got := proto.ImageDomainsLine(); got != "image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1" {
		t.Fatalf("domains: %q", got)
	}
	if proto.ImageABIVersion != 1 {
		t.Fatal("the additive image domains must not bump image ABI")
	}
	// origin: hot-update S8 plan §6.1 / §11.2 P17; S8 BROKER-EXPORTER R-11 — the
	// emitted domain line adds the required upgrade-cut-v1 adjunct domain (the
	// S6 reader probe's ImageDomainsLine above is unchanged) and the S8
	// features line joins before the final ABI line.
	want := []string{"image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1 upgrade-cut-v1", "image-read-epochs 1",
		"image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1", "image-abi v1"}
	if got := proto.ImageVersionLines(); !slices.Equal(got, want) {
		t.Fatalf("image lines=%q want=%q", got, want)
	}
	// The emitter remains P9's integration point; this tests the ordered
	// proto helper it can append after the unchanged release line.
	lines := append([]string{proto.VersionLine()}, proto.ImageVersionLines()...)
	if lines[len(lines)-1] != proto.ImageABILine() {
		t.Fatal("image ABI must remain the final line")
	}
}
