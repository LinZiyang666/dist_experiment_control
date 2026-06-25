package proto

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// B4 wire-compat tests. The new ExposeReq/ExposeResp/PsPortEntry fields are additive + omitempty;
// an omitted field MUST produce bytes byte-identical to a pre-B4 peer, and decode-to-default must
// reproduce today's behavior. These use REALISTIC populated fixtures (not zero structs) so a
// regression that makes an "always present" key leak is caught.

// legacyExposeReq mirrors the pre-B4 ExposeReq shape (no rebuild_off / on_broker). Marshalling a
// real ExposeReq with the B4 fields zero MUST equal marshalling this.
type legacyExposeReq struct {
	Name       string `json:"name"`
	LocalPort  int    `json:"local_port"`
	RemotePort int    `json:"remote_port,omitempty"`
	ActorFP    string `json:"actor_fp,omitempty"`
}

// TestB4ExposeReqOmitemptyByteIdentical (plan §F.1): a realistic ExposeReq with no B4 flags — and
// the same with RebuildOff=false/OnBroker="" explicitly — marshals byte-identical to the pre-B4
// shape. This is the "an old ctl and a new ctl that omits the flags send identical bytes" proof.
func TestB4ExposeReqOmitemptyByteIdentical(t *testing.T) {
	legacy, err := json.Marshal(legacyExposeReq{Name: "jupyter", LocalPort: 8888, RemotePort: 14050, ActorFP: "SHA256:fp"})
	if err != nil {
		t.Fatal(err)
	}
	noFlags, err := json.Marshal(ExposeReq{Name: "jupyter", LocalPort: 8888, RemotePort: 14050, ActorFP: "SHA256:fp"})
	if err != nil {
		t.Fatal(err)
	}
	zeroFlags, err := json.Marshal(ExposeReq{Name: "jupyter", LocalPort: 8888, RemotePort: 14050, ActorFP: "SHA256:fp", RebuildOff: false, OnBroker: ""})
	if err != nil {
		t.Fatal(err)
	}
	if string(noFlags) != string(legacy) {
		t.Fatalf("new ExposeReq (no flags) = %s, want pre-B4 %s", noFlags, legacy)
	}
	if string(zeroFlags) != string(legacy) {
		t.Fatalf("new ExposeReq (zero flags) = %s, want pre-B4 %s", zeroFlags, legacy)
	}
}

// TestB4ExposeReqFlagsSerialize (plan §F.2): set flags appear on the wire and round-trip.
func TestB4ExposeReqFlagsSerialize(t *testing.T) {
	b, err := json.Marshal(ExposeReq{Name: "j", LocalPort: 8888, RebuildOff: true, OnBroker: "brk-b"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"rebuild_off":true`) || !strings.Contains(string(b), `"on_broker":"brk-b"`) {
		t.Fatalf("flags missing on wire: %s", b)
	}
	var got ExposeReq
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if !got.RebuildOff || got.OnBroker != "brk-b" {
		t.Fatalf("round-trip lost flags: %+v", got)
	}
}

// TestB4ExposeReqOldBrokerIgnores (plan §F.3): a new-ctl body carrying both B4 flags decodes
// cleanly into the legacy struct (an old broker ignores the unknown keys → default behavior).
func TestB4ExposeReqOldBrokerIgnores(t *testing.T) {
	newBody := []byte(`{"name":"j","local_port":8888,"rebuild_off":true,"on_broker":"brk-b"}`)
	var legacy legacyExposeReq
	if err := json.Unmarshal(newBody, &legacy); err != nil {
		t.Fatalf("old broker must decode a new body without error: %v", err)
	}
	if legacy.Name != "j" || legacy.LocalPort != 8888 {
		t.Fatalf("old fields lost: %+v", legacy)
	}
}

type legacyExposeResp struct {
	Port       int    `json:"port,omitempty"`
	PublicHost string `json:"public_host,omitempty"`
	Name       string `json:"name,omitempty"`
	Code       string `json:"code,omitempty"`
	Error      string `json:"error,omitempty"`
}

type legacyPsPortEntry struct {
	Port        int       `json:"port"`
	Name        string    `json:"name"`
	NID         string    `json:"nid"`
	LocalPort   int       `json:"local_port"`
	State       string    `json:"state"`
	CreatedByFP string    `json:"created_by_fp,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
}

// TestB4ExposeRespPsPortRealisticOmitempty (plan §F.4): a populated ExposeResp / PsPortEntry row
// (every non-B4 field set) with the B4 fields zero marshals byte-identical to the pre-B4 shape;
// with the B4 fields set, the keys appear. Realistic-row, not zero-struct.
func TestB4ExposeRespPsPortRealisticOmitempty(t *testing.T) {
	legacyResp, _ := json.Marshal(legacyExposeResp{Port: 14000, PublicHost: "h", Name: "j"})
	zeroResp, _ := json.Marshal(ExposeResp{Port: 14000, PublicHost: "h", Name: "j"})
	if string(zeroResp) != string(legacyResp) {
		t.Fatalf("ExposeResp zero-home = %s, want pre-B4 %s", zeroResp, legacyResp)
	}
	setResp, _ := json.Marshal(ExposeResp{Port: 14000, PublicHost: "h", Name: "j", HomeBroker: "brk-b", Epoch: 2})
	if !strings.Contains(string(setResp), `"home_broker":"brk-b"`) || !strings.Contains(string(setResp), `"epoch":2`) {
		t.Fatalf("ExposeResp set-home missing keys: %s", setResp)
	}

	ts := time.Date(2026, 6, 24, 1, 2, 3, 0, time.UTC)
	legacyPort, _ := json.Marshal(legacyPsPortEntry{Port: 14000, Name: "j", NID: "a100", LocalPort: 8888, State: "ALLOCATED", CreatedByFP: "SHA256:fp", CreatedAt: ts})
	zeroPort, _ := json.Marshal(PsPortEntry{Port: 14000, Name: "j", NID: "a100", LocalPort: 8888, State: "ALLOCATED", CreatedByFP: "SHA256:fp", CreatedAt: ts})
	if string(zeroPort) != string(legacyPort) {
		t.Fatalf("PsPortEntry zero-home = %s, want pre-B4 %s", zeroPort, legacyPort)
	}
	setPort, _ := json.Marshal(PsPortEntry{Port: 14000, Name: "j", NID: "a100", LocalPort: 8888, State: "ALLOCATED", CreatedAt: ts, HomeBroker: "brk-b", Epoch: 3, RebuildOff: true})
	for _, want := range []string{`"home_broker":"brk-b"`, `"epoch":3`, `"rebuild_off":true`} {
		if !strings.Contains(string(setPort), want) {
			t.Fatalf("PsPortEntry set-home missing %s: %s", want, setPort)
		}
	}
}

// TestB4ProtoVersionUnchanged (plan §F.5): B4 fields are additive — NOT a wire break. The proto
// version stays 2 (a bump would force the whole reinstall path documented in CLAUDE.md §5).
func TestB4ProtoVersionUnchanged(t *testing.T) {
	if ProtoVersion != 2 {
		t.Fatalf("B4 is additive (omitempty fields); ProtoVersion must stay 2, got %d", ProtoVersion)
	}
}
