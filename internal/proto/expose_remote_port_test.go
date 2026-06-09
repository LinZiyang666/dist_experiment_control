package proto

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestExposeReqRemotePortOmitemptyByteIdentical: the P12 RemotePort
// field MUST be `omitempty` so a zero value serializes away entirely.
// That is what keeps a new ctl that omits --remote-port byte-identical
// on the wire to today (and to an old ctl), so an old broker that
// ignores the unknown key keeps auto-allocating. A regression dropping
// omitempty would emit `"remote_port":0` and break that guarantee.
func TestExposeReqRemotePortOmitemptyByteIdentical(t *testing.T) {
	noField, err := json.Marshal(ExposeReq{Name: "jupyter", LocalPort: 8888})
	if err != nil {
		t.Fatal(err)
	}
	zeroField, err := json.Marshal(ExposeReq{Name: "jupyter", LocalPort: 8888, RemotePort: 0})
	if err != nil {
		t.Fatal(err)
	}
	if string(noField) != string(zeroField) {
		t.Errorf("RemotePort:0 must marshal identically to omitted:\n  %s\n  %s", noField, zeroField)
	}
	if strings.Contains(string(zeroField), "remote_port") {
		t.Errorf("omitempty failed: zero RemotePort leaked a remote_port key: %s", zeroField)
	}
}

// TestExposeReqRemotePortRoundtrip: a nonzero RemotePort serializes
// under the agreed json tag and survives a marshal/unmarshal roundtrip.
func TestExposeReqRemotePortRoundtrip(t *testing.T) {
	b, err := json.Marshal(ExposeReq{Name: "j", LocalPort: 8888, RemotePort: 14050})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"remote_port":14050`) {
		t.Errorf("nonzero RemotePort must serialize under remote_port: %s", b)
	}
	var got ExposeReq
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.RemotePort != 14050 {
		t.Errorf("roundtrip RemotePort: got %d want 14050", got.RemotePort)
	}
}

// TestExposeReqLegacyBodyDecodesToZero: a pre-P12 ctl body (no
// remote_port key) MUST decode to RemotePort==0 on a new broker, which
// is the auto / lowest-free sentinel — the old-ctl→new-broker compat path.
func TestExposeReqLegacyBodyDecodesToZero(t *testing.T) {
	var got ExposeReq
	if err := json.Unmarshal([]byte(`{"name":"j","local_port":8888}`), &got); err != nil {
		t.Fatal(err)
	}
	if got.RemotePort != 0 {
		t.Errorf("legacy body must decode RemotePort=0 (auto), got %d", got.RemotePort)
	}
}

// TestExposeReqNewBodyDecodesOnOldBrokerStruct pins the D-8 cross-version
// premise: a NEW ctl that actually SETS --remote-port talks to an OLD
// (pre-P12) broker, whose ExposeReq struct has no RemotePort field. The
// broker decode path uses plain json.Unmarshal (no DisallowUnknownFields),
// so the unknown remote_port key MUST be silently ignored — the request
// degrades to auto-allocation rather than erroring. We simulate the old
// struct locally since the repo only ships the new one.
func TestExposeReqNewBodyDecodesOnOldBrokerStruct(t *testing.T) {
	type oldExposeReq struct {
		Name      string `json:"name"`
		LocalPort int    `json:"local_port"`
	}
	body := []byte(`{"name":"j","local_port":8888,"remote_port":14050}`)
	var got oldExposeReq
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("old broker struct must ignore unknown remote_port key, got error: %v", err)
	}
	if got.Name != "j" || got.LocalPort != 8888 {
		t.Errorf("known fields must still decode: %+v", got)
	}
}
