package proto

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestProtoVersionPositive(t *testing.T) {
	if ProtoVersion < 1 {
		t.Fatalf("ProtoVersion must be ≥ 1, got %d", ProtoVersion)
	}
}

func TestReleaseVersionDefault(t *testing.T) {
	if ReleaseVersion == "" {
		t.Fatal("ReleaseVersion default must not be empty")
	}
}

func TestSubjectPrefixStable(t *testing.T) {
	// Bumping SubjectPrefix without a ProtoVersion bump would silently break
	// every subscriber. Pin them together.
	if SubjectPrefix != "tether.v1" {
		t.Fatalf("SubjectPrefix must equal 'tether.v1' for ProtoVersion=1, got %q", SubjectPrefix)
	}
}

// Golden subject layouts. Any change here is a wire break — bump ProtoVersion
// and update fixtures.
func TestSubjectGolden(t *testing.T) {
	cases := []struct {
		got, want string
	}{
		{SubjVersionAnnounce, "tether.v1.ctrl.version.announce"},
		{SubjSysEvents, "tether.v1.sys.events"},
		{SubjCtrlBy("UABCD", "session.create.req"), "tether.v1.ctrl.by.UABCD.session.create.req"},
		{SubjCmdBy("lab", "UABCD", "lab-1", "run"), "tether.v1.s.lab.cmd.by.UABCD.node.lab-1.run.req"},
		{SubjCmdForwarded("lab", "lab-1", "run"), "tether.v1.s.lab.cmd.node.lab-1.run.req.forwarded"},
		{SubjNodeRegister("lab", "lab-1"), "tether.v1.ctrl.s.lab.node.lab-1.register.req"},
		{SubjNodeUnregister("lab", "lab-1"), "tether.v1.ctrl.s.lab.node.lab-1.unregister.req"},
		{SubjNodeHeartbeat("lab", "lab-1"), "tether.v1.ctrl.s.lab.node.lab-1.heartbeat"},
		{SubjEvNodeState("lab", "lab-1"), "tether.v1.s.lab.ev.node.lab-1.state"},
		{SubjEvProc("lab", "lab-1", "01hzxk", "exit"), "tether.v1.s.lab.ev.node.lab-1.proc.01hzxk.exit"},
		{SubjEvPort("lab", 14022, "allocated"), "tether.v1.s.lab.ev.port.14022.allocated"},
		{SubjAuditCall("lab"), "tether.v1.s.lab.audit.call"},
		{SubjAuditProc("lab"), "tether.v1.s.lab.audit.proc"},
		{SubjAuditPort("lab"), "tether.v1.s.lab.audit.port"},
		{SubjPtyOut("lab", "01hzxk"), "tether.v1.s.lab.pty.01hzxk.out"},
		{SubjPtyIn("lab", "01hzxk"), "tether.v1.s.lab.pty.01hzxk.in"},
		{SubjPtyResize("lab", "01hzxk"), "tether.v1.s.lab.pty.01hzxk.resize"},
		{SubjPtyAttach("lab", "01hzxk"), "tether.v1.s.lab.pty.01hzxk.attach"},
		{SubjPtyReady("lab", "01hzxk"), "tether.v1.s.lab.pty.01hzxk.ready"},
		{SubjPtyFailed("lab", "01hzxk"), "tether.v1.s.lab.pty.01hzxk.failed"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("subject mismatch: got %q, want %q", c.got, c.want)
		}
	}
}

// `cmd.by.<A>.node.*.*.req` and `cmd.node.*.*.req.forwarded` must have a
// different token count so a NATS subscription on one cannot accidentally
// match the other. (architecture B.1 / B.2 固定 token 数 不变式)
func TestForwardedTokenCountDiffers(t *testing.T) {
	a := SubjCmdBy("lab", "UABCD", "lab-1", "run")
	b := SubjCmdForwarded("lab", "lab-1", "run")
	na := strings.Count(a, ".") + 1
	nb := strings.Count(b, ".") + 1
	if na == nb {
		t.Fatalf("cmd.by.* and cmd.*.req.forwarded must differ in token count, both = %d", na)
	}
}

// JSON round-trip equivalence: Marshal → Unmarshal → Marshal yields the same
// bytes. Catches map ordering / time formatting / pointer-vs-value drift.
func TestMessagesJSONGoldenRoundtrip(t *testing.T) {
	t0 := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)

	cases := []any{
		&SessionCreateReq{Name: "lab", PIN: "123456"},
		&SessionCreateResp{SID: "lab", OwnerFP: "SHA256:xxx=", CreatedAt: t0},
		&NodeRegisterReq{
			ProtoVersion:   ProtoVersion,
			ReleaseVersion: ReleaseVersion,
			NID:            "lab-1",
			OS:             "linux",
			Arch:           "amd64",
			BootID:         "deadbeef",
		},
		&NodeRegisterResp{OK: true},
		&NodeRegisterResp{OK: false, Code: "proto_mismatch", Error: "client_proto=2 server_proto=1"},
		&HeartbeatPayload{Ts: t0},
		&ErrorReply{Code: "session_not_found_or_deleting", Message: "session lab is being deleted"},
		&SessionListReq{},
		&SessionListResp{Sessions: []SessionEntry{
			{SID: "lab", Name: "lab", OwnerFP: "SHA256:x", State: "ACTIVE", CreatedAt: t0, IsOwner: true},
		}},
		&SessionRmReq{},
		&SessionRmResp{OK: true},
		&SessionRmResp{Code: "not_owner"},
	}
	for i, v := range cases {
		first, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("case %d: marshal: %v", i, err)
		}
		// Unmarshal into a fresh value of the same dynamic type.
		dec := newOf(v)
		if err := json.Unmarshal(first, dec); err != nil {
			t.Fatalf("case %d: unmarshal: %v\nbytes=%s", i, err, first)
		}
		second, err := json.Marshal(dec)
		if err != nil {
			t.Fatalf("case %d: re-marshal: %v", i, err)
		}
		if !bytes.Equal(first, second) {
			t.Errorf("case %d: round-trip mismatch:\n  first:  %s\n  second: %s", i, first, second)
		}
	}
}

func TestSessionSubjects(t *testing.T) {
	cases := []struct{ got, want string }{
		{SubjCtrlSessionCreate("UABCD"), "tether.v1.ctrl.by.UABCD.session.create.req"},
		{SubjCtrlSessionList("UABCD"), "tether.v1.ctrl.by.UABCD.session.list.req"},
		{SubjCtrlSessionRm("UABCD", "lab"), "tether.v1.ctrl.by.UABCD.session.lab.rm.req"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q want %q", c.got, c.want)
		}
	}
}

func TestParseCtrlBy(t *testing.T) {
	cases := []struct {
		subject              string
		wantActor, wantLeaf  string
		wantOK               bool
	}{
		{SubjCtrlSessionCreate("UABCD"), "UABCD", "session.create.req", true},
		{SubjCtrlSessionList("UABCD"), "UABCD", "session.list.req", true},
		{SubjCtrlSessionRm("UABCD", "lab"), "UABCD", "session.lab.rm.req", true},
		// negatives
		{"", "", "", false},
		{"tether.v1.ctrl", "", "", false},
		{"tether.v2.ctrl.by.UABCD.x.y", "", "", false},
		// wrong tree (cmd.by.* not ctrl.by.*)
		{"tether.v1.s.lab.cmd.by.UABCD.node.lab-1.run.req", "", "", false},
	}
	for _, c := range cases {
		a, l, ok := ParseCtrlBy(c.subject)
		if a != c.wantActor || l != c.wantLeaf || ok != c.wantOK {
			t.Errorf("ParseCtrlBy(%q) = (%q, %q, %v); want (%q, %q, %v)",
				c.subject, a, l, ok, c.wantActor, c.wantLeaf, c.wantOK)
		}
	}
}

func TestParseSidNidFromCtrl(t *testing.T) {
	cases := []struct {
		subject       string
		wantSid       string
		wantNid       string
		wantOK        bool
	}{
		{SubjNodeRegister("lab", "lab-1"), "lab", "lab-1", true},
		{SubjNodeUnregister("prod", "node-007"), "prod", "node-007", true},
		{SubjNodeHeartbeat("lab", "1gpu"), "lab", "1gpu", true},
		// Wrong tree (per-session ev., not ctrl) → reject.
		{SubjEvNodeState("lab", "lab-1"), "", "", false},
		// Wrong prefix.
		{"foo.bar.ctrl.s.lab.node.lab-1.register.req", "", "", false},
		// Too short.
		{"tether.v1.ctrl.s.lab.node.lab-1", "", "", false},
		// Wrong segment names.
		{"tether.v1.ctrl.x.lab.node.lab-1.register.req", "", "", false},
		{"tether.v1.ctrl.s.lab.x.lab-1.register.req", "", "", false},
		// Empty.
		{"", "", "", false},
	}
	for _, c := range cases {
		gotSid, gotNid, ok := ParseSidNidFromCtrl(c.subject)
		if ok != c.wantOK || gotSid != c.wantSid || gotNid != c.wantNid {
			t.Errorf("ParseSidNidFromCtrl(%q) = (%q, %q, %v); want (%q, %q, %v)",
				c.subject, gotSid, gotNid, ok, c.wantSid, c.wantNid, c.wantOK)
		}
	}
}

// newOf returns a pointer to a freshly-zeroed value of the same dynamic type
// as v (which itself must be a pointer).
func newOf(v any) any {
	switch v.(type) {
	case *SessionCreateReq:
		return &SessionCreateReq{}
	case *SessionCreateResp:
		return &SessionCreateResp{}
	case *NodeRegisterReq:
		return &NodeRegisterReq{}
	case *NodeRegisterResp:
		return &NodeRegisterResp{}
	case *HeartbeatPayload:
		return &HeartbeatPayload{}
	case *ErrorReply:
		return &ErrorReply{}
	case *SessionListReq:
		return &SessionListReq{}
	case *SessionListResp:
		return &SessionListResp{}
	case *SessionRmReq:
		return &SessionRmReq{}
	case *SessionRmResp:
		return &SessionRmResp{}
	}
	panic("newOf: unknown type")
}
