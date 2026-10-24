package ptysession_test

import (
	"bytes"
	"encoding/json"
	"io"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/pty"
	"github.com/LinZiyang666/tether/internal/ptysession"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
	"github.com/LinZiyang666/tether/internal/workload"
)

type imageField struct{ name, tag, typ string }

// origin: hot-update S4 plan §2.3 / §5.1
func TestPTYImageSchemaRejectsUnknownFields(t *testing.T) {
	for _, tc := range []struct {
		typ    reflect.Type
		fields []imageField
	}{
		{reflect.TypeFor[ptysession.Image](), []imageField{
			{"Version", "version", "int"}, {"Identity", "identity", "pty.TerminalIdentity"}, {"Observation", "observation", "pty.TerminalObservation"},
			{"Input", "input", "workload.ReplayState"}, {"Output", "output", "workload.ReplayState"},
			{"InputFIN", "input_fin", "bool"}, {"InputEnded", "input_ended", "bool"}, {"InputError", "input_error", "string"},
			{"OutputError", "output_error", "string"}, {"Pending", "pending", "[]uint8"}, {"Discarding", "discarding", "bool"},
			{"SourceEnded", "source_ended", "bool"}, {"MasterClosed", "master_closed", "bool"},
			{"ControlSeq", "control_seq", "uint64"}, {"ControlDigest", "control_digest", "string"}, {"ControlError", "control_error", "string"},
			{"KAIntervalMS", "ka_interval_ms", "int"}, {"KADeadlineBootNS", "ka_deadline_boot_ns", "int64"},
			{"ExpiredSinceBootNS", "expired_since_boot_ns", "int64"}, {"RecoveryNotBeforeBootNS", "recovery_not_before_boot_ns", "int64"},
			{"NextJudgeBootNS", "next_judge_boot_ns", "int64"}, {"LastKASeq", "last_ka_seq", "uint64"},
			{"AuthorityDeadlineBootNS", "authority_deadline_boot_ns", "int64"}, {"Revoked", "revoked", "string"},
			{"CleanupPending", "cleanup_pending", "bool"}, {"CleanupMembers", "cleanup_members", "[]workload.SessionMember"},
		}},
		{reflect.TypeFor[ptysession.Tombstone](), []imageField{
			{"InputACK", "input_ack", "uint64"}, {"InputAccepted", "input_accepted", "uint64"}, {"InputFIN", "input_fin", "bool"},
			{"InputEnded", "input_ended", "bool"}, {"InputError", "input_error", "string"}, {"ControlSeq", "control_seq", "uint64"},
			{"ControlDigest", "control_digest", "string"}, {"ControlError", "control_error", "string"},
		}},
		{reflect.TypeFor[pty.TerminalIdentity](), []imageField{
			{"Version", "version", "int"}, {"BootID", "boot_id", "string"}, {"LeaderPID", "leader_pid", "int"},
			{"StartTimeTicks", "start_time_ticks", "int64"}, {"Dev", "dev", "uint64"}, {"Ino", "ino", "uint64"},
			{"Rdev", "rdev", "uint64"}, {"MountID", "mount_id", "uint64"}, {"PTYIndex", "pty_index", "uint32"}, {"Started", "started", "bool"},
		}},
		{reflect.TypeFor[pty.TerminalObservation](), []imageField{
			{"Cols", "cols", "uint16"}, {"Rows", "rows", "uint16"}, {"SessionID", "session_id,omitempty", "int"},
			{"ForegroundPGID", "foreground_pgid,omitempty", "int"}, {"ControlTTYKnown", "control_tty_known", "bool"}, {"Termios", "termios,omitempty", "[]uint8"},
		}},
		{reflect.TypeFor[workload.SessionMember](), []imageField{
			{"Identity", "identity", "workload.Identity"}, {"SessionID", "session_id", "int"},
		}},
	} {
		t.Run(tc.typ.String(), func(t *testing.T) {
			if tc.typ.NumField() != len(tc.fields) {
				t.Fatalf("fields=%d want=%d", tc.typ.NumField(), len(tc.fields))
			}
			for i, want := range tc.fields {
				got := tc.typ.Field(i)
				if got.Name != want.name || got.Tag.Get("json") != want.tag || got.Type.String() != want.typ {
					t.Errorf("field %d=%s/%s/%s want=%+v", i, got.Name, got.Tag.Get("json"), got.Type, want)
				}
			}
			data, err := json.Marshal(reflect.Zero(tc.typ).Interface())
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatal(err)
			}
			for _, f := range tc.fields {
				tag, optional := strings.CutSuffix(f.tag, ",omitempty")
				_, present := fields[tag]
				if present == optional {
					t.Errorf("zero field %s presence=%v optional=%v", tag, present, optional)
				}
			}
		})
	}
	for _, tc := range []struct {
		name, data, diagnostic string
		target                 any
	}{
		{"image unknown", `{"future_domain":1}`, "unknown field", &ptysession.Image{}},
		{"tombstone unknown", `{"input_fin_at":1}`, "unknown field", &ptysession.Tombstone{}},
		{"identity unknown", `{"identity":{"fd":9}}`, "unknown field", &ptysession.Image{}},
		{"observation unknown", `{"observation":{"restore_termios":true}}`, "unknown field", &ptysession.Image{}},
		{"cleanup unknown", `{"cleanup_members":[{"pid":7}]}`, "unknown field", &ptysession.Image{}},
		{"input wrong type", `{"input":{"base":"2"}}`, "cannot unmarshal", &ptysession.Image{}},
		{"negative receipt", `{"control_seq":-1}`, "cannot unmarshal", &ptysession.Tombstone{}},
		{"overflow receipt", `{"input_ack":18446744073709551616}`, "cannot unmarshal", &ptysession.Tombstone{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dec := json.NewDecoder(strings.NewReader(tc.data))
			dec.DisallowUnknownFields()
			if err := dec.Decode(tc.target); err == nil || !strings.Contains(err.Error(), tc.diagnostic) {
				t.Fatalf("strict decode must reject %s with %q: %v", tc.data, tc.diagnostic, err)
			}
		})
	}
}

// Real: two bounded memfd replays per round, nonempty retained byte ranges,
// freeze snapshots, strict JSON decode, and explicit owner closure.
// Controlled: terminal identity/observations and cleanup metadata are fixtures;
// this is a schema receipt, not a claim that the P3 actor stub can restore them.
// origin: hot-update S4 plan §5.1 / §8.4
func TestPTYImageRoundTripPreservesResponsibility(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("real replay resources require Linux")
	}
	g0, fd0 := runtime.NumGoroutine(), testharness.FDCount()
	for i := 0; i < 5; i++ {
		exercisePTYImageRoundTrip(t)
	}
	testharness.AssertNoGoroutineLeak(t, "terminal image metadata", g0)
	testharness.AssertNoFDLeak(t, "terminal image replay owners", fd0, 0)
}

func exercisePTYImageRoundTrip(t *testing.T) {
	t.Helper()
	budget := &workload.ReplayBudget{}
	input, err := workload.NewReplay(budget, upgradebudget.PTYInputBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = input.Close() }()
	output, err := workload.NewReplay(budget, upgradebudget.StreamReplayBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = output.Close() }()
	for _, tc := range []struct {
		log  *workload.Replay
		data []byte
		ack  uint64
	}{
		{input, []byte{0, 3, 4, 0xe2, 0x82, 0xac}, 2},
		{output, []byte{0, 0xff, 27, '[', 'm'}, 1},
	} {
		if n, err := tc.log.Append(tc.data); err != nil || n != len(tc.data) {
			t.Fatalf("append n=%d err=%v", n, err)
		}
		if err := tc.log.Ack(tc.ack); err != nil {
			t.Fatal(err)
		}
	}
	if err := input.Finish(); err != nil {
		t.Fatal(err)
	}
	inState, inFile, err := input.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	outState, outFile, err := output.Freeze()
	if err != nil {
		t.Fatal(err)
	}
	if inFile == nil || outFile == nil || inState.Base >= inState.End || outState.Base >= outState.End {
		t.Fatal("round trip must carry two real, nonempty retained replays")
	}
	image := ptysession.Image{
		Version:     1,
		Identity:    pty.TerminalIdentity{Version: 1, BootID: "boot", LeaderPID: 41, StartTimeTicks: 42, Dev: 43, Ino: 44, Rdev: 45, MountID: 46, PTYIndex: 47, Started: true},
		Observation: pty.TerminalObservation{Cols: 120, Rows: 40, SessionID: 41, ForegroundPGID: 51, ControlTTYKnown: true, Termios: []byte{0, 3, 255}},
		Input:       inState, Output: outState, InputFIN: true, InputEnded: true, InputError: "pty input aborted: revoked",
		OutputError: "replay_error", Pending: []byte{0, 255}, Discarding: true, SourceEnded: false, MasterClosed: true,
		ControlSeq: 1<<63 + 7, ControlDigest: "digest", ControlError: "pty control failed: close_error",
		KAIntervalMS: 1000, KADeadlineBootNS: 180000000000, ExpiredSinceBootNS: 180000000001,
		RecoveryNotBeforeBootNS: 185000000001, NextJudgeBootNS: 186000000001, LastKASeq: 1<<63 + 6,
		AuthorityDeadlineBootNS: 190000000000, Revoked: "session_destroyed", CleanupPending: true,
		CleanupMembers: []workload.SessionMember{{Identity: workload.Identity{ID: "member", OSPID: 51, BootID: "boot", StartTimeTicks: 52, StartedAt: time.Unix(1, 0).UTC()}, SessionID: 41}},
	}
	var decoded ptysession.Image
	strictImageRoundTrip(t, image, &decoded)
	if !reflect.DeepEqual(decoded, image) {
		t.Fatalf("image lost byte or cleanup responsibility: got=%+v want=%+v", decoded, image)
	}
	// Decoding metadata must not consume or replace either existing replay.
	for _, tc := range []struct {
		log   *workload.Replay
		state workload.ReplayState
		want  []byte
	}{
		{input, decoded.Input, []byte{4, 0xe2, 0x82, 0xac}},
		{output, decoded.Output, []byte{0xff, 27, '[', 'm'}},
	} {
		data := make([]byte, tc.state.End-tc.state.Base)
		n, err := tc.log.Read(tc.state.Base, data)
		var wantErr error
		if tc.state.FIN {
			wantErr = io.EOF
		}
		if err != wantErr || n != len(data) || !bytes.Equal(data, tc.want) {
			t.Fatalf("retained bytes=%x n=%d err=%v want=%x", data, n, err, tc.want)
		}
	}
	tombstone := ptysession.Tombstone{InputACK: decoded.Input.Base, InputAccepted: decoded.Input.End, InputFIN: decoded.InputFIN, InputEnded: decoded.InputEnded, InputError: decoded.InputError, ControlSeq: decoded.ControlSeq, ControlDigest: decoded.ControlDigest, ControlError: decoded.ControlError}
	var settled ptysession.Tombstone
	strictImageRoundTrip(t, tombstone, &settled)
	if !reflect.DeepEqual(settled, tombstone) || settled.InputACK >= settled.InputAccepted || settled.InputError == "" {
		t.Fatalf("tombstone erased unapplied input: %+v", settled)
	}
	if got := budget.Used(); got != upgradebudget.PTYInputBytes+upgradebudget.StreamReplayBytes {
		t.Fatalf("reserved bytes=%d", got)
	}
	if err := input.Close(); err != nil {
		t.Fatal(err)
	}
	if err := output.Close(); err != nil {
		t.Fatal(err)
	}
	if got := budget.Used(); got != 0 {
		t.Fatalf("retired metadata retained replay budget=%d", got)
	}
}

func strictImageRoundTrip(t *testing.T, value, target any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		t.Fatal(err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		t.Fatalf("trailing image data: %v", err)
	}
}
