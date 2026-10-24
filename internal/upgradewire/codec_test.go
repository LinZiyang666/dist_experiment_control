// The tests exercise real wire codecs and framing with adversarial envelopes.
// Endpoint dispatch is tested separately; these tests do not certify a runtime cut.
package upgradewire_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradeartifact"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

func wireKey() upgradetx.Key {
	return upgradetx.Key{DomainID: "installation", Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "node"}
}
func brokerKey() upgradetx.Key {
	return upgradetx.Key{DomainID: "installation", Component: upgradetx.ComponentBroker, ConfigPath: "/tmp/broker.yaml"}
}
func wireGrant(key upgradetx.Key) upgradetx.Grant {
	return upgradetx.Grant{
		Handle:      upgradetx.Handle{Key: key, TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 2, Revision: 3},
		ClientNonce: strings.Repeat("c", 32), Issuer: upgradetx.Owner{BootID: "boot", PID: 12, StartTicks: 34, InstanceID: "instance"},
		ActorFP: "actor", Mode: upgradetx.ModeHot, SourceRelease: "v0.6.0", TargetRelease: "v0.7.0",
		SourceSHA: strings.Repeat("1", 64), TargetSHA: strings.Repeat("2", 64), ScopeDigest: strings.Repeat("3", 64),
		CapabilityDigest: strings.Repeat("4", 64), ExpiresAt: time.Now().Add(time.Minute).UnixNano(),
	}
}
func wireRequests(key upgradetx.Key) []upgradewire.Request {
	g := wireGrant(key)
	s := upgradetx.Scope{ID: "scope", Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainControl, upgradetx.DomainUpgrade}}
	fence := upgradetx.Fence{Current: g.Handle, Owner: g.Issuer, RunningSHA: g.TargetSHA, PreviousSHA: g.SourceSHA}
	if key.Component == upgradetx.ComponentAgent {
		fence.Incarnation = strings.Repeat("5", 64)
	}
	return []upgradewire.Request{
		{Op: upgradewire.OpPrepare, Key: key, Prepare: upgradetx.PrepareRequest{ClientNonce: g.ClientNonce, ActorFP: g.ActorFP, SourceRelease: g.SourceRelease, TargetRelease: g.TargetRelease, SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, Mode: g.Mode, Scope: s, CapabilityDigest: g.CapabilityDigest, ExpiresAt: g.ExpiresAt}},
		{Op: upgradewire.OpStart, Key: key, Grant: g, Artifact: upgradeartifact.Receipt{Grant: g, CanonicalPath: "/tmp/tether", SourcePath: "/tmp/source", TargetPath: "/tmp/target", SourceSHA: g.SourceSHA, TargetSHA: g.TargetSHA, ManifestDigest: strings.Repeat("6", 64)}},
		{Op: upgradewire.OpQuery, Key: key, TxID: g.Handle.TxID, QueryNonce: strings.Repeat("d", 32)},
		{Op: upgradewire.OpRollback, Key: key, Expected: fence, Reverse: upgradetx.ReverseRequest{ClientNonce: g.ClientNonce, ActorFP: g.ActorFP, HistoricalTxID: g.Handle.TxID, TargetSHA: g.SourceSHA, Scope: s, ExpiresAt: g.ExpiresAt}},
		{Op: upgradewire.OpCancel, Key: key, Expected: upgradetx.Fence{Current: g.Handle}},
		{Op: upgradewire.OpArchive, Key: key, Expected: upgradetx.Fence{Current: g.Handle}},
		{Op: upgradewire.OpCloseWindow, Key: key, Expected: upgradetx.Fence{Current: g.Handle}},
	}
}
func requestBytes(t *testing.T, r upgradewire.Request) []byte {
	t.Helper()
	b, err := upgradewire.EncodeRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// origin: hot-update S8 plan §2.2 / P13
func TestUpgradeWireOperationFields(t *testing.T) {
	want := [][]string{{"key", "op", "prepare"}, {"artifact", "grant", "key", "op"}, {"key", "op", "query_nonce", "tx_id"}, {"expected", "key", "op", "reverse"}, {"expected", "key", "op"}, {"expected", "key", "op"}, {"expected", "key", "op"}}
	for _, key := range []upgradetx.Key{wireKey(), brokerKey()} {
		for i, r := range wireRequests(key) {
			t.Run(string(key.Component)+r.Op, func(t *testing.T) {
				b := requestBytes(t, r)
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(b, &fields); err != nil {
					t.Fatal(err)
				}
				if len(fields) != len(want[i]) {
					t.Fatalf("operation emitted extra fields: %s", b)
				}
				for _, name := range want[i] {
					if fields[name] == nil {
						t.Fatalf("missing field %s", name)
					}
				}
				plain, err := json.Marshal(r)
				if err != nil || !bytes.Equal(plain, b) {
					t.Fatal("standard marshal bypasses operation codec")
				}
				got, err := upgradewire.DecodeRequest(b, key.Component)
				if err != nil || !reflect.DeepEqual(got, r) {
					t.Fatalf("round trip changed request: %v", err)
				}
				for _, extra := range []string{"tx_id", "query_nonce", "grant", "expected", "prepare", "reverse", "artifact"} {
					if fields[extra] != nil {
						continue
					}
					for _, value := range []string{"null", "{}", "\"\""} {
						poisoned := append([]byte(nil), b[:len(b)-1]...)
						poisoned = append(poisoned, []byte(",\""+extra+"\":"+value+"}")...)
						if _, err := upgradewire.DecodeRequest(poisoned, key.Component); err == nil {
							t.Fatalf("irrelevant %s accepted for %s", extra, r.Op)
						}
					}
				}
				for _, name := range want[i] {
					copyFields := map[string]json.RawMessage{}
					for k, v := range fields {
						if k != name {
							copyFields[k] = v
						}
					}
					poisoned, _ := json.Marshal(copyFields)
					if _, err := upgradewire.DecodeRequest(poisoned, key.Component); err == nil {
						t.Fatalf("missing %s accepted", name)
					}
				}
			})
		}
	}
	for _, op := range []string{"upgrade", "upgrade.begin"} {
		unknown := upgradewire.Request{Op: op, Key: wireKey()}
		if _, err := upgradewire.EncodeRequest(unknown); err == nil {
			t.Fatal("unknown operation with no extra fields encoded")
		}
		raw, _ := json.Marshal(struct {
			Op  string        `json:"op"`
			Key upgradetx.Key `json:"key"`
		}{op, wireKey()})
		if _, err := upgradewire.DecodeRequest(raw, upgradetx.ComponentAgent); err == nil {
			t.Fatal("unknown operation with no extra fields decoded")
		}
	}
	query := wireRequests(wireKey())[2]
	query.Grant = wireGrant(wireKey())
	if _, err := upgradewire.EncodeRequest(query); err == nil {
		t.Fatal("encoder discarded an unexpected grant")
	}
}

// origin: hot-update S8 plan §2.2 / P13
func TestUpgradeWireRejectsAmbiguousJSON(t *testing.T) {
	b := requestBytes(t, wireRequests(wireKey())[0])
	cases := map[string][]byte{
		"duplicate-op":     bytes.Replace(b, []byte(`"op":"upgrade.prepare"`), []byte(`"op":"upgrade.start","op":"upgrade.prepare"`), 1),
		"duplicate-nested": bytes.Replace(b, []byte(`"actor_fp":"actor"`), []byte(`"actor_fp":"other","actor_fp":"actor"`), 1),
		"case-alias":       bytes.Replace(b, []byte(`"actor_fp"`), []byte(`"Actor_FP"`), 1),
		"unknown-nested":   bytes.Replace(b, []byte(`"actor_fp":"actor"`), []byte(`"actor_fp":"actor","secret":"material"`), 1),
		"trailing":         append(append([]byte(nil), b...), []byte(` {}`)...),
		"invalid-utf8":     bytes.Replace(b, []byte("actor\""), []byte{0xff, '"'}, 1),
		"integer-overflow": bytes.Replace(b, []byte(`"uid":0`), []byte(`"uid":18446744073709551616`), 1),
	}
	// UID is optional and zero is omitted: insert a real overflow at the key.
	cases["integer-overflow"] = bytes.Replace(b, []byte(`"domain_id":"installation"`), []byte(`"domain_id":"installation","uid":18446744073709551616`), 1)
	for name, raw := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := upgradewire.DecodeRequest(raw, upgradetx.ComponentAgent); err == nil {
				t.Fatal("ambiguous or corrupt JSON accepted")
			}
		})
	}
	for name, raw := range map[string][]byte{
		"payload": []byte(`[` + strings.Repeat(`"`+strings.Repeat("x", 300)+`",`, 4095) + `"last"]`),
		// The counterexample keeps its independent 4097 elements when a mutant
		// changes the production limit; deriving it from that limit hides the bug.
		// origin: hot-update S8 plan §2.2 / P13
		"collection": []byte(`{"items":[` + strings.Repeat(`0,`, 4096) + `0]}`),
		"depth":      []byte(strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34)),
		"string":     []byte(`{"value":"` + strings.Repeat("x", 4097) + `"}`),
	} {
		t.Run(name, func(t *testing.T) {
			var v json.RawMessage
			if err := upgradewire.StrictDecode(raw, &v); err == nil {
				t.Fatal("unbounded JSON accepted")
			}
		})
	}
	if _, err := upgradewire.ReadLine(strings.NewReader(strings.Repeat("x", upgradewire.MaxMessageBytes+1) + "\n")); err == nil {
		t.Fatal("unbounded socket line accepted")
	}
}

// origin: hot-update S8 plan §2.2 / §7.2 / P13
func TestUpgradeWireRejectsInvalidAuthorization(t *testing.T) {
	mutations := []struct {
		name   string
		index  int
		change func(*upgradewire.Request)
	}{
		{"legacy-op", 0, func(r *upgradewire.Request) { r.Op = "upgrade" }},
		{"unknown-op", 0, func(r *upgradewire.Request) { r.Op = "upgrade.begin" }},
		{"wrong-role", 0, func(r *upgradewire.Request) { r.Key.Component = upgradetx.ComponentNATS }},
		{"empty-mode", 0, func(r *upgradewire.Request) { r.Prepare.Mode = "" }},
		{"interrupting", 0, func(r *upgradewire.Request) { r.Prepare.Mode = upgradetx.ModeLegacyInterrupting }},
		{"bad-source", 0, func(r *upgradewire.Request) { r.Prepare.SourceSHA = strings.Repeat("A", 64) }},
		{"duplicate-participant", 0, func(r *upgradewire.Request) {
			r.Prepare.Scope.Participants = append(r.Prepare.Scope.Participants, r.Key)
		}},
		{"duplicate-resource", 0, func(r *upgradewire.Request) {
			resource := upgradetx.Resource{Domain: upgradetx.DomainControl, ID: "resource"}
			r.Prepare.Scope.Expected = []upgradetx.Resource{resource, resource}
		}},
		{"duplicate-domain", 0, func(r *upgradewire.Request) {
			r.Prepare.Scope.Domains = append(r.Prepare.Scope.Domains, upgradetx.DomainControl)
		}},
		{"unknown-domain", 0, func(r *upgradewire.Request) { r.Prepare.Scope.Domains[0] = "secret" }},
		{"scope-target", 0, func(r *upgradewire.Request) { r.Prepare.Scope.Participants[0].SID = "other" }},
		{"parent-without-slot", 0, func(r *upgradewire.Request) { r.Prepare.ParentTxID = strings.Repeat("a", 32) }},
		{"grant-target", 1, func(r *upgradewire.Request) { r.Grant.Handle.Key.SID = "other"; r.Artifact.Grant = r.Grant }},
		{"grant-generation", 1, func(r *upgradewire.Request) { r.Grant.Handle.Generation = 0; r.Artifact.Grant = r.Grant }},
		{"artifact-grant", 1, func(r *upgradewire.Request) { r.Artifact.Grant.ActorFP = "other" }},
		{"artifact-sha", 1, func(r *upgradewire.Request) { r.Artifact.TargetSHA = r.Grant.SourceSHA }},
		{"missing-release", 0, func(r *upgradewire.Request) { r.Prepare.TargetRelease = "" }},
		{"missing-incarnation", 3, func(r *upgradewire.Request) { r.Expected.Incarnation = "" }},
		{"relative-artifact", 1, func(r *upgradewire.Request) { r.Artifact.TargetPath = "relative" }},
		{"missing-nonce", 2, func(r *upgradewire.Request) { r.QueryNonce = "" }},
		{"reverse-scope", 3, func(r *upgradewire.Request) { r.Reverse.Scope.Domains = nil }},
		{"missing-handle", 4, func(r *upgradewire.Request) { r.Expected.Current.Revision = 0 }},
	}
	// Encode through an alias to let invalid semantic requests reach the receiver.
	type unchecked upgradewire.Request
	for _, m := range mutations {
		t.Run(m.name, func(t *testing.T) {
			r := wireRequests(wireKey())[m.index]
			m.change(&r)
			if _, err := upgradewire.EncodeRequest(r); err == nil {
				t.Fatal("invalid authorization encoded")
			}
			b, _ := json.Marshal(unchecked(r))
			if _, err := upgradewire.DecodeRequest(b, upgradetx.ComponentAgent); err == nil {
				t.Fatal("invalid authorization decoded")
			}
		})
	}
	b := requestBytes(t, wireRequests(wireKey())[2])
	if _, err := upgradewire.DecodeRequest(b, upgradetx.ComponentBroker); err == nil {
		t.Fatal("agent envelope accepted by broker")
	}
}

// origin: hot-update S8 plan §2.2 / §7.1 / P13
func TestUpgradeWireResponseKeepsHistoricalAndCurrent(t *testing.T) {
	g := wireGrant(wireKey())
	view := upgradetx.View{Historical: upgradetx.Record{Key: g.Handle.Key, Grant: g, Revision: 8, Forward: upgradetx.Attempt{Handle: g.Handle, Phase: upgradetx.PhaseCompleted}}, Observation: upgradetx.Observation{Nonce: strings.Repeat("d", 32), At: 123, RunningSHA: g.TargetSHA, Current: upgradetx.Attempt{Handle: upgradetx.Handle{Key: g.Handle.Key, TxID: strings.Repeat("f", 32), CutID: strings.Repeat("e", 32), Generation: 3, Revision: 9}, Phase: upgradetx.PhaseMigrating}}}
	r := upgradewire.Response{View: view}
	b, err := upgradewire.EncodeResponse(r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := upgradewire.DecodeResponse(b)
	if err != nil || !reflect.DeepEqual(got, r) {
		t.Fatalf("history/current changed: %v", err)
	}
	empty, err := upgradewire.EncodeResponse(upgradewire.Response{})
	if err != nil || string(empty) != "{}" {
		t.Fatalf("empty response fabricates a view: %s %v", empty, err)
	}
	for _, raw := range []string{`null`, `[]`, `{"code":"first","code":"second"}`, `{"view":{"unknown":true}}`, `{"error":"one"} {}`} {
		if _, err := upgradewire.DecodeResponse([]byte(raw)); err == nil {
			t.Fatalf("bad response accepted: %s", raw)
		}
	}
}

// origin: hot-update S8 plan §2.2 / P13
func FuzzUpgradeWireStrictRequest(f *testing.F) {
	for _, r := range wireRequests(wireKey()) {
		body, err := upgradewire.EncodeRequest(r)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(body)
	}
	for _, body := range []string{`null`, `[]`, `{"op":"upgrade.query","op":"upgrade.start"}`, `{"op":"upgrade.prepare","key":{}}`} {
		f.Add([]byte(body))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		r, err := upgradewire.DecodeRequest(body, upgradetx.ComponentAgent)
		if err != nil {
			return
		}
		encoded, err := upgradewire.EncodeRequest(r)
		if err != nil {
			t.Fatalf("accepted request cannot be encoded: %v", err)
		}
		again, err := upgradewire.DecodeRequest(encoded, upgradetx.ComponentAgent)
		if err != nil || !reflect.DeepEqual(again, r) {
			t.Fatal("accepted request lost fields on canonical round trip")
		}
	})
}
