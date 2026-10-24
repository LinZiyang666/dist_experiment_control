package agentroll

import (
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/auth"
	"github.com/LinZiyang666/tether/internal/brokerupgrade/rollcontrol"
	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §4.2 / §7.2
func TestAgentFleetSampleIsSignedOnlyForItsOriginalChallenge(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	pub, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	g := upgradetx.Grant{Handle: upgradetx.Handle{TxID: strings.Repeat("1", 32)}}
	request := rollcontrol.Request{Op: "roll.sample", Parent: g, Step: upgradetx.Step{Grant: g}, Nonce: strings.Repeat("2", 32)}
	sample := rollcontrol.Sample{ParentTxID: g.Handle.TxID, Nonce: request.Nonce, View: upgradetx.View{Historical: upgradetx.Record{Grant: g}, Observation: upgradetx.Observation{Nonce: request.Nonce}}}
	route := Route{SignSample: func(s rollcontrol.Sample) (rollcontrol.Sample, error) { return rollcontrol.SignSample(seed, s) }}
	raw, _ := json.Marshal(rollcontrol.Response{Sample: sample})
	out, code, err := Response(route, request, raw)
	if err != nil || code != "" {
		t.Fatalf("valid sample: %s %v", code, err)
	}
	var reply rollcontrol.Response
	if err = json.Unmarshal(out, &reply); err != nil {
		t.Fatal(err)
	}
	signature, err := hex.DecodeString(reply.Sample.Signature)
	if err != nil {
		t.Fatal(err)
	}
	reply.Sample.Signature = ""
	signed, _ := json.Marshal(reply.Sample)
	if auth.VerifySignature(pub, signed, signature) != nil {
		t.Fatal("broker courier did not authenticate the actual sample")
	}
	for _, tc := range []struct {
		name   string
		change func(*rollcontrol.Sample, *Route)
	}{
		{"another parent", func(s *rollcontrol.Sample, _ *Route) { s.ParentTxID = strings.Repeat("3", 32) }},
		{"another challenge", func(s *rollcontrol.Sample, _ *Route) { s.Nonce = strings.Repeat("3", 32) }},
		{"another child", func(s *rollcontrol.Sample, _ *Route) { s.View.Historical.Grant.Handle.TxID = strings.Repeat("3", 32) }},
		{"historical observation", func(s *rollcontrol.Sample, _ *Route) { s.View.Observation.Nonce = strings.Repeat("3", 32) }},
		{"no signer", func(_ *rollcontrol.Sample, r *Route) { r.SignSample = nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad, r := sample, route
			tc.change(&bad, &r)
			raw, _ := json.Marshal(rollcontrol.Response{Sample: bad})
			if _, _, err := Response(r, request, raw); err == nil {
				t.Fatal("unbound agent sample received a broker signature")
			}
		})
	}
}

// origin: S8 internal review SIGNED-ROLL-FIXTURES F3; S8 plan §3.1 / §6.3
func TestAgentRollParentBindingRequiresAccountSignature(t *testing.T) {
	seed, err := auth.GenerateUserSeed()
	if err != nil {
		t.Fatal(err)
	}
	actor, err := auth.PublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	fp, err := auth.FingerprintFromActor(actor)
	if err != nil {
		t.Fatal(err)
	}
	request := rollcontrol.Request{Op: "roll.bind", Key: upgradetx.Key{Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "agent"}, Parent: upgradetx.Grant{ActorFP: fp}, Scope: upgradetx.Scope{ID: strings.Repeat("1", 32)}}
	route := Route{Actor: actor, ActorFP: fp, SID: "session", NID: "agent", Reply: auth.InboxPrefixFor(actor) + ".test", Capable: true, VerifyRoll: func(r rollcontrol.Request) error { return rollcontrol.VerifyAgentRequest(seed, r) }}
	route.Body, err = rollcontrol.SignAgentRequest(seed, request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = Admit(route); err != nil {
		t.Fatal(err)
	}
	unsigned, _ := json.Marshal(request)
	var changed rollcontrol.Request
	if err = json.Unmarshal(route.Body, &changed); err != nil {
		t.Fatal(err)
	}
	changed.Scope.ID = strings.Repeat("2", 32)
	tampered, _ := json.Marshal(changed)
	for _, body := range [][]byte{unsigned, tampered} {
		r := route
		r.Body = body
		if _, err = Admit(r); err == nil {
			t.Fatal("session owner invented an unplanned parent without account authority")
		}
	}
}
