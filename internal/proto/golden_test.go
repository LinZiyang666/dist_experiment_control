package proto

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var updateGolden = flag.Bool("update-golden", false, "regenerate proto golden JSON fixtures")

// Golden JSON fixtures (D0, §19 "proto v2 golden JSON 回环").
//
// Purpose: pin the ON-DISK JSON encoding of representative v2 wire structs so
// that (a) the v1→v2 flip is proven NOT to have changed any wire SHAPE (only the
// version label moved; the bytes are otherwise identical to the pre-flip v1
// encoding), and (b) D6's DELIBERATE breaking field additions (REGISTER epoch,
// HomeDirective.cert_pins, §7.2b/§15) produce a visible golden diff instead of a
// silent wire change. Distinct from the self-referential roundtrip tests
// (TestRoundtripAllMessages / TestMessagesJSONGoldenRoundtrip), which only check
// Marshal→Unmarshal→Marshal is a fixed point but never compare against a frozen
// snapshot.
//
// All fixtures are deterministic (fixed timestamps, fixed bytes, no maps in the
// chosen structs) so the golden is byte-stable. Refresh after an INTENTIONAL
// wire change with: go test ./internal/proto -run TestGolden -update-golden
func goldenFixtures(t *testing.T) []struct {
	name string
	v    any
} {
	t.Helper()
	t0 := time.Date(2026, 5, 8, 10, 0, 0, 0, time.UTC)
	rc := 0
	return []struct {
		name string
		v    any
	}{
		{"node_register_req", &NodeRegisterReq{
			ProtoVersion: ProtoVersion, ReleaseVersion: "v0.0.0-dev", NID: "lab-1",
			OS: "linux", Arch: "amd64", BootID: "deadbeef",
			LocalProcesses: []LocalProcess{{
				PID: "01h", State: "exited", RC: &rc, StartedAt: t0, StartTimeTicks: 12345,
			}},
			LocalPorts: []LocalPort{{Port: 14000, Name: "jupyter", LocalPort: 8888, TokenHash: "abc"}},
		}},
		{"node_register_resp", &NodeRegisterResp{
			OK: true, KeepPorts: []int{14000}, RevokePorts: []int{14001}, DropProcesses: []string{"03h"},
		}},
		{"node_register_resp_proto_mismatch", &NodeRegisterResp{
			OK: false, Code: "proto_mismatch", Error: "client_proto=2 server_proto=1",
		}},
		{"expose_forwarded_req", &ExposeForwardedReq{
			Name: "jupyter", Port: 14000, LocalPort: 8888, Token: "tok", ActorFP: "fp",
		}},
		{"port_event", &PortEvent{
			Port: 14000, Name: "jupyter", NID: "lab-1", LocalPort: 8888, Kind: "allocated", Ts: t0,
		}},
		{"heartbeat", &HeartbeatPayload{Ts: t0}},
		// Adversarial: a []byte payload carrying NUL bytes and a non-UTF-8
		// sequence must survive base64 round-trip byte-for-bit.
		{"exec_chunk_binary", &ExecChunk{
			Kind: "stdout", PID: "01h", Data: []byte{0x00, 0xff, 0xfe, 'a', 0x00, 0x80},
		}},
		// Adversarial: explicitly empty (non-nil) slices on omitempty fields —
		// these encode identically to nil (both omitted), so the golden is
		// {"ok":true}. This pins that the fields stay omitempty: if omitempty
		// were removed, "[]" would appear and mismatch this golden. (It does NOT
		// pin a [] vs null distinction — that's unreachable while omitempty.)
		{"node_register_resp_empty_slices", &NodeRegisterResp{
			OK: true, KeepPorts: []int{}, AcceptedProcesses: []string{},
		}},
	}
}

func goldenPath(name string) string {
	return filepath.Join("testdata", "golden", name+".json")
}

// TestGoldenWireEncoding pins each fixture's JSON bytes against an on-disk
// golden, and asserts the golden itself round-trips (Unmarshal→reMarshal ==
// golden). With -update-golden it (re)writes the fixtures.
func TestGoldenWireEncoding(t *testing.T) {
	for _, f := range goldenFixtures(t) {
		f := f
		t.Run(f.name, func(t *testing.T) {
			// Marshal with indentation for a readable, diffable golden.
			got, err := json.MarshalIndent(f.v, "", "  ")
			if err != nil {
				t.Fatalf("marshal: %v", err)
			}
			got = append(got, '\n')

			path := goldenPath(f.name)
			if *updateGolden {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}

			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read golden %s: %v (run with -update-golden to create)", path, err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("golden mismatch for %s — a wire shape changed (run -update-golden if intentional, e.g. a D6 break):\n got:\n%s\n want:\n%s",
					f.name, got, want)
			}

			// The golden must itself be a fixed point: decode it into a fresh
			// value of the same type and re-marshal compactly; comparing against
			// a compact re-marshal of the fixture proves shape equivalence
			// (independent of indentation).
			zero := newGoldenZero(t, f.v)
			if err := json.Unmarshal(want, zero); err != nil {
				t.Fatalf("golden does not decode: %v", err)
			}
			reMarshaled, err := json.Marshal(zero)
			if err != nil {
				t.Fatal(err)
			}
			fixtureCompact, err := json.Marshal(f.v)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(reMarshaled, fixtureCompact) {
				t.Errorf("golden→decode→re-marshal not a fixed point for %s:\n  golden:  %s\n  fixture: %s",
					f.name, reMarshaled, fixtureCompact)
			}
		})
	}
}

// newGoldenZero returns a pointer to a fresh zero value of the same dynamic type
// as v (which must be a pointer to a wire struct used in goldenFixtures).
func newGoldenZero(t *testing.T, v any) any {
	t.Helper()
	switch v.(type) {
	case *NodeRegisterReq:
		return &NodeRegisterReq{}
	case *NodeRegisterResp:
		return &NodeRegisterResp{}
	case *ExposeForwardedReq:
		return &ExposeForwardedReq{}
	case *PortEvent:
		return &PortEvent{}
	case *HeartbeatPayload:
		return &HeartbeatPayload{}
	case *ExecChunk:
		return &ExecChunk{}
	default:
		t.Fatalf("newGoldenZero: unhandled type %T (add it)", v)
		return nil
	}
}

// TestGoldenParserRejectsV1Prefix is the negative half of the flip: a subject
// carrying the OLD v1 prefix must be rejected by the v2 parsers (proves the
// version flip is enforced at parse time, not just at the builder).
func TestGoldenParserRejectsV1Prefix(t *testing.T) {
	const actor = "UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	// Structurally valid except for the version token — only the v1/v2 prefix
	// differs, so a non-false result would mean the parser ignores the version.
	v1 := "tether.v1.ctrl.by." + actor + ".session.create.req"
	if _, _, ok := ParseCtrlBy(v1); ok {
		t.Errorf("ParseCtrlBy must reject a v1-prefixed subject after the v2 flip: %q", v1)
	}
	v2 := "tether.v2.ctrl.by." + actor + ".session.create.req"
	if _, _, ok := ParseCtrlBy(v2); !ok {
		t.Errorf("ParseCtrlBy must accept the matching v2-prefixed subject: %q", v2)
	}
}

// TestAllParsersRejectV1Prefix generalizes the v1-reject invariant across EVERY
// subject parser (not just ParseCtrlBy): a structurally-valid subject carrying
// the OLD v1 prefix must be rejected — the v2 parsers gate on
// parts[1]==SubjectVersionToken (§16.2 hard upgrade, no rolling coexistence).
// For each, the v2 form of the SAME subject is also asserted to PARSE, so the
// v1 rejection is proven to be about the version token and not some structural
// defect. A future regression of any single parser back to `!= "v1"` (or a
// dropped version check) is caught here even though make test would otherwise
// stay green.
func TestAllParsersRejectV1Prefix(t *testing.T) {
	const actor = "UAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
	cases := []struct {
		name    string
		subject string // v1-prefixed, otherwise structurally valid
		parse   func(string) bool
	}{
		{"ParseCtrlBy", "tether.v1.ctrl.by." + actor + ".session.create.req",
			func(s string) bool { _, _, ok := ParseCtrlBy(s); return ok }},
		{"ParseCmdBy", "tether.v1.s.lab.cmd.by." + actor + ".node.lab-1.run.req",
			func(s string) bool { _, _, _, _, ok := ParseCmdBy(s); return ok }},
		{"ParseEvProc", "tether.v1.s.lab.ev.node.lab-1.proc.01h.exit",
			func(s string) bool { _, _, _, _, ok := ParseEvProc(s); return ok }},
		{"ParseSidNidFromCtrl", "tether.v1.ctrl.s.lab.node.lab-1.register.req",
			func(s string) bool { _, _, ok := ParseSidNidFromCtrl(s); return ok }},
		{"ParseTransferFinalize", "tether.v1.ctrl.by." + actor + ".s.lab.transfer.01hzxn.finalize.req",
			func(s string) bool { _, _, _, ok := ParseTransferFinalize(s); return ok }},
		{"ParseEvTransfer", "tether.v1.s.lab.ev.node.lab-1.transfer.01hzxn.complete",
			func(s string) bool { _, _, _, _, ok := ParseEvTransfer(s); return ok }},
		{"ParseCtrlProxy", "tether.v1.ctrl.by." + actor + ".s.lab.proxy.set.req",
			func(s string) bool { _, _, _, ok := ParseCtrlProxy(s); return ok }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			v2 := strings.Replace(c.subject, "tether.v1.", "tether.v2.", 1)
			if !c.parse(v2) {
				t.Fatalf("v2 control subject %q must be accepted (else the v1 rejection proves nothing structural)", v2)
			}
			if c.parse(c.subject) {
				t.Fatalf("must reject v1-prefixed subject after the D0 flip: %q", c.subject)
			}
		})
	}
}
