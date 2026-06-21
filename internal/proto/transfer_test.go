package proto

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

// 56-char real-shape actor token used by tests that exercise the
// actor-token validator inside the parsers.
const fakeActor = "UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

// Golden subject layouts for the new transfer subjects. Bumping these
// without ProtoVersion is a wire break.
func TestTransferSubjectGolden(t *testing.T) {
	cases := []struct{ got, want string }{
		{SubjAuditTransfer("lab"), "tether.v2.s.lab.audit.transfer"},
		{SubjEvTransfer("lab", "lab-1", "01hzxn", "complete"),
			"tether.v2.s.lab.ev.node.lab-1.transfer.01hzxn.complete"},
		{SubjEvTransfer("lab", "lab-1", "01hzxn", "failed"),
			"tether.v2.s.lab.ev.node.lab-1.transfer.01hzxn.failed"},
		{SubjCtrlTransferFinalize(fakeActor, "lab", "01hzxn"),
			"tether.v2.ctrl.by." + fakeActor + ".s.lab.transfer.01hzxn.finalize.req"},
		{SubjCtrlCaps(fakeActor, "lab"),
			"tether.v2.ctrl.by." + fakeActor + ".s.lab.caps.req"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("subject mismatch: got %q want %q", c.got, c.want)
		}
	}
}

// ParseTransferFinalize must round-trip with SubjCtrlTransferFinalize and
// reject ill-formed subjects (esp. wrong sid/actor token).
func TestParseTransferFinalize(t *testing.T) {
	cases := []struct {
		subject            string
		wantActor, wantSID string
		wantTID            string
		wantOK             bool
	}{
		{SubjCtrlTransferFinalize(fakeActor, "lab", "01hzxn"), fakeActor, "lab", "01hzxn", true},
		// Different actor key shape — token validator must reject.
		{"tether.v2.ctrl.by.short.s.lab.transfer.01hzxn.finalize.req", "", "", "", false},
		// Wrong shape.
		{"tether.v2.ctrl.by." + fakeActor + ".s.lab.transfer.01hzxn.commit.req", "", "", "", false},
		// Wrong root.
		{"foo.v1.ctrl.by." + fakeActor + ".s.lab.transfer.01hzxn.finalize.req", "", "", "", false},
		// Wrong VERSION: structurally valid but the old v1 prefix — the v2 parser
		// must reject it at parts[1] (D0 hard-upgrade, §16.2; exercises the
		// parts[1]!=SubjectVersionToken branch).
		{"tether.v1.ctrl.by." + fakeActor + ".s.lab.transfer.01hzxn.finalize.req", "", "", "", false},
		// Sid validation rejects.
		{"tether.v2.ctrl.by." + fakeActor + ".s.\xff\xff.transfer.01hzxn.finalize.req", "", "", "", false},
	}
	for _, c := range cases {
		a, s, id, ok := ParseTransferFinalize(c.subject)
		if a != c.wantActor || s != c.wantSID || id != c.wantTID || ok != c.wantOK {
			t.Errorf("ParseTransferFinalize(%q) = (%q, %q, %q, %v); want (%q, %q, %q, %v)",
				c.subject, a, s, id, ok, c.wantActor, c.wantSID, c.wantTID, c.wantOK)
		}
	}
}

// ParseEvTransfer must round-trip with SubjEvTransfer and reject mismatched layouts.
func TestParseEvTransfer(t *testing.T) {
	cases := []struct {
		subject                   string
		wantSid, wantNid, wantTID string
		wantKind                  string
		wantOK                    bool
	}{
		{SubjEvTransfer("lab", "lab-1", "01hzxn", "complete"),
			"lab", "lab-1", "01hzxn", "complete", true},
		{SubjEvTransfer("lab", "lab-1", "01hzxn", "failed"),
			"lab", "lab-1", "01hzxn", "failed", true},
		// Wrong tree.
		{SubjEvProc("lab", "lab-1", "p1", "exit"), "", "", "", "", false},
		// Truncated.
		{"tether.v2.s.lab.ev.node.lab-1.transfer.01hzxn", "", "", "", "", false},
		// Wrong VERSION: structurally valid but the old v1 prefix — v2 parser
		// must reject at parts[1] (D0 hard-upgrade, §16.2).
		{"tether.v1.s.lab.ev.node.lab-1.transfer.01hzxn.complete", "", "", "", "", false},
	}
	for _, c := range cases {
		sid, nid, tid, kind, ok := ParseEvTransfer(c.subject)
		if sid != c.wantSid || nid != c.wantNid || tid != c.wantTID ||
			kind != c.wantKind || ok != c.wantOK {
			t.Errorf("ParseEvTransfer(%q) = (%q,%q,%q,%q,%v); want (%q,%q,%q,%q,%v)",
				c.subject, sid, nid, tid, kind, ok,
				c.wantSid, c.wantNid, c.wantTID, c.wantKind, c.wantOK)
		}
	}
}

// JSON round-trip for the new transfer payload types. tier-A InlineData
// must survive base64 + JSON without bit-flips.
func TestTransferPayloadsRoundtrip(t *testing.T) {
	cases := []any{
		&PushPrepareReq{
			TransferID: "01hzxn", Path: "/srv/local/alice/data.bin",
			Size: 5, SHA256: "deadbeef", Tier: "a",
			InlineData: []byte{0x00, 0xff, 0x42, 0x01, 0x80},
		},
		&PushPrepareReq{
			TransferID: "01hzxp", Path: "/srv/local/alice/big.bin",
			Size: 50 * 1024 * 1024, SHA256: "cafebabe", Tier: "b",
			Bucket: "xfer-lab-01hzxp", ObjectKey: "obj-01hzxp", ChunkSize: 1 << 20,
		},
		&PushPrepareResp{OK: true, Tier: "a"},
		&PushPrepareResp{OK: false, Code: "dst_exists", Error: "/srv/local/alice/data.bin"},
		&PullPrepareReq{TransferID: "01hzxn", Path: "/srv/local/alice/data.bin",
			MaxInline: 8 * 1024 * 1024},
		&PullPrepareResp{
			OK: true, Tier: "a", Size: 3, SHA256: "0",
			InlineData: []byte{0x01, 0x02, 0x03},
		},
		&PullPrepareResp{
			OK: true, Tier: "b", Size: 50 * 1024 * 1024,
			SHA256: "cafebabe", Bucket: "xfer-lab-01hzxn", ObjectKey: "obj-01hzxn",
		},
		&TransferCommitReq{TransferID: "01hzxn", Bucket: "xfer-lab-01hzxn", ObjectKey: "obj-01hzxn"},
		&TransferCommitResp{OK: true},
		&TransferEvent{
			Kind: "complete", Verb: "push", TransferID: "01hzxn",
			Tier: "b", Bucket: "xfer-lab-01hzxn",
			Path: "/srv/local/alice/data.bin", Bytes: 50 * 1024 * 1024, DurationMs: 6740,
		},
		&TransferEvent{
			Kind: "failed", Verb: "push", TransferID: "01hzxp",
			Tier: "a", Code: "sha_mismatch", Error: "want=ABC got=DEF",
		},
		&TransferFinalize{
			Kind: "complete", TransferID: "01hzxn", Tier: "b",
			Bucket: "xfer-lab-01hzxn", Path: "/local.bin",
			Bytes: 50 * 1024 * 1024, DurationMs: 6740,
		},
		&TransferFinalize{
			Kind: "failed", TransferID: "01hzxp", Tier: "a",
			Code: "sha_mismatch", Error: "want=ABC got=DEF",
		},
		&TransferFinalizeResp{OK: true},
		&TransferFinalizeResp{OK: false, Code: "not_owner_or_creator"},
		&CapsReq{},
		&CapsResp{OK: true, JetStreamReady: true, MaxPayload: 16 * 1024 * 1024,
			BrokerRelease: "v0.2.0", BrokerProto: 1},
		&CapsResp{OK: false, Code: "not_a_member"},
	}
	for i, v := range cases {
		first, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("case %d: marshal: %v", i, err)
		}
		dec := newTransferOf(v)
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
		// Tier-A inline_data must round-trip byte-for-byte.
		if pp, ok := v.(*PushPrepareReq); ok && pp.Tier == "a" {
			got := dec.(*PushPrepareReq).InlineData
			if !bytes.Equal(pp.InlineData, got) {
				t.Errorf("case %d (push tier A): inline_data round-trip mismatch", i)
			}
		}
		if pp, ok := v.(*PullPrepareResp); ok && pp.Tier == "a" {
			got := dec.(*PullPrepareResp).InlineData
			if !bytes.Equal(pp.InlineData, got) {
				t.Errorf("case %d (pull tier A): inline_data round-trip mismatch", i)
			}
		}
	}
}

// Sanity: omitempty must drop the InlineData field for tier-B payloads
// so we don't pay for an empty base64 "" on every tier-B prepare.
func TestTransferOmitemptyHonored(t *testing.T) {
	pp := &PushPrepareReq{
		TransferID: "01hzxp", Path: "/x", Size: 1, SHA256: "a",
		Tier: "b", Bucket: "xfer-lab-01hzxp", ObjectKey: "obj",
	}
	b, err := json.Marshal(pp)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"inline_data":`) {
		t.Errorf("tier-B push must omit inline_data: %s", b)
	}
	pr := &PullPrepareResp{OK: true, Tier: "b", Size: 1, SHA256: "a",
		Bucket: "xfer-lab-01hzxn", ObjectKey: "obj"}
	b, err = json.Marshal(pr)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"inline_data":`) {
		t.Errorf("tier-B pull must omit inline_data: %s", b)
	}
}

func newTransferOf(v any) any {
	switch v.(type) {
	case *PushPrepareReq:
		return &PushPrepareReq{}
	case *PushPrepareResp:
		return &PushPrepareResp{}
	case *PullPrepareReq:
		return &PullPrepareReq{}
	case *PullPrepareResp:
		return &PullPrepareResp{}
	case *TransferCommitReq:
		return &TransferCommitReq{}
	case *TransferCommitResp:
		return &TransferCommitResp{}
	case *TransferEvent:
		return &TransferEvent{}
	case *TransferFinalize:
		return &TransferFinalize{}
	case *TransferFinalizeResp:
		return &TransferFinalizeResp{}
	case *CapsReq:
		return &CapsReq{}
	case *CapsResp:
		return &CapsResp{}
	}
	panic("newTransferOf: unknown type")
}
