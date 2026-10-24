// The describe envelope is exercised through the real codec, Handle and the
// forwarded adapter. The describer spy returns fixed descriptions; it certifies
// neither a runtime census nor a cut.
package upgradewire_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/upgradeport"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/internal/upgradewire"
)

var allDomains = []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl,
	upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker,
	upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}

func describeNonce() string { return strings.Repeat("e", 32) }

func description(key upgradetx.Key, nonce string) upgradeport.Description {
	scope := upgradetx.Scope{ID: strings.Repeat("9", 32), Participants: []upgradetx.Key{key}, Domains: append([]upgradetx.Domain(nil), allDomains...),
		Expected: []upgradetx.Resource{{Domain: upgradetx.DomainBrokerStore, ID: "role:agent", Origin: "capability", State: "not_applicable", Custody: "not_applicable"}}}
	return upgradeport.Description{Key: key, Nonce: nonce, At: time.Now().UnixNano(), RunningSHA: strings.Repeat("1", 64), SourceRelease: "v0.7.0", Scope: scope}
}

// describerSpy is an Endpoint that also describes itself.
type describerSpy struct {
	endpointSpy
	answer func(selector upgradetx.Key, nonce string) (upgradeport.Description, error)
	asked  int
}

func (d *describerSpy) Describe(_ context.Context, selector upgradetx.Key, nonce string) (upgradeport.Description, error) {
	d.asked++
	return d.answer(selector, nonce)
}

// origin: S8 FIX-DESCRIBE (P19 request R1). upgrade.describe is additive and as
// strict as the frozen seven: exactly op, key and query_nonce; the key is a
// selector carrying only what a ctl can know (an installation fact such as a
// DomainID, UID or config path is refused, not compared); and the response's
// description decodes strictly, with no description key on any other reply.
func TestDescribeEnvelopeIsStrictBothDirections(t *testing.T) {
	agent := upgradewire.DescribeSelector(wireKey())
	broker := upgradewire.DescribeSelector(brokerKey())
	for _, selector := range []upgradetx.Key{agent, broker} {
		r := upgradewire.Request{Op: upgradewire.OpDescribe, Key: selector, QueryNonce: describeNonce()}
		body := requestBytes(t, r)
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil || len(fields) != 3 || fields["op"] == nil || fields["key"] == nil || fields["query_nonce"] == nil {
			t.Fatalf("describe carries other fields: %s", body)
		}
		decoded, err := upgradewire.DecodeRequest(body, selector.Component)
		if err != nil || !reflect.DeepEqual(decoded, r) {
			t.Fatalf("describe did not round-trip: %+v %v", decoded, err)
		}
		for name, poison := range map[string][]byte{
			"extra_tx_id":     bytesWithExtra(body, `"tx_id":"`+strings.Repeat("a", 32)+`"`),
			"null_extra":      bytesWithExtra(body, `"grant":null`),
			"duplicate_nonce": []byte(strings.Replace(string(body), `"query_nonce":"`, `"query_nonce":"`+strings.Repeat("f", 32)+`","query_nonce":"`, 1)),
		} {
			if _, err := upgradewire.DecodeRequest(poison, selector.Component); err == nil {
				t.Fatalf("%s: ambiguous describe accepted", name)
			}
		}
	}
	bad := func(mutate func(*upgradewire.Request)) upgradewire.Request {
		r := upgradewire.Request{Op: upgradewire.OpDescribe, Key: agent, QueryNonce: describeNonce()}
		mutate(&r)
		return r
	}
	for name, r := range map[string]upgradewire.Request{
		"missing_nonce":     bad(func(r *upgradewire.Request) { r.QueryNonce = "" }),
		"uppercase_nonce":   bad(func(r *upgradewire.Request) { r.QueryNonce = strings.Repeat("E", 32) }),
		"short_nonce":       bad(func(r *upgradewire.Request) { r.QueryNonce = strings.Repeat("e", 31) }),
		"guessed_domain":    bad(func(r *upgradewire.Request) { r.Key.DomainID = "installation" }),
		"guessed_config":    bad(func(r *upgradewire.Request) { r.Key.ConfigPath = "/home/u/.tether/agent/session/agent.yaml" }),
		"guessed_uid":       bad(func(r *upgradewire.Request) { r.Key.UID = 1000 }),
		"full_key":          bad(func(r *upgradewire.Request) { r.Key = wireKey() }),
		"agent_without_sid": bad(func(r *upgradewire.Request) { r.Key.SID = "" }),
		"agent_without_nid": bad(func(r *upgradewire.Request) { r.Key.ConfiguredNID = "" }),
		"broker_with_sid": bad(func(r *upgradewire.Request) {
			r.Key = upgradetx.Key{Component: upgradetx.ComponentBroker, SID: "session"}
		}),
		"broker_with_node": bad(func(r *upgradewire.Request) {
			r.Key = upgradetx.Key{Component: upgradetx.ComponentBroker, ConfiguredNID: "b1"}
		}),
		"unsupported_role":     bad(func(r *upgradewire.Request) { r.Key = upgradetx.Key{Component: upgradetx.ComponentNATS} }),
		"query_tx_on_describe": bad(func(r *upgradewire.Request) { r.TxID = strings.Repeat("a", 32) }),
	} {
		if _, err := upgradewire.EncodeRequest(r); err == nil {
			t.Fatalf("%s: invalid describe encoded", name)
		}
	}
	d := description(wireKey(), describeNonce())
	body, err := upgradewire.EncodeResponse(upgradewire.Response{Description: &d})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := upgradewire.DecodeResponse(body)
	if err != nil || decoded.Description == nil || !reflect.DeepEqual(*decoded.Description, d) || !reflect.DeepEqual(decoded.View, upgradetx.View{}) {
		t.Fatalf("description did not round-trip: %s %v", body, err)
	}
	for name, poison := range map[string]string{
		"unknown_field":   strings.Replace(string(body), `"nonce":`, `"issuer":"x","nonce":`, 1),
		"duplicate_field": strings.Replace(string(body), `"running_sha":`, `"running_sha":"`+strings.Repeat("2", 64)+`","running_sha":`, 1),
		"cased_field":     strings.Replace(string(body), `"running_sha":`, `"Running_SHA":`, 1),
	} {
		if _, err := upgradewire.DecodeResponse([]byte(poison)); err == nil {
			t.Fatalf("%s: ambiguous description decoded", name)
		}
	}
	plain, _ := upgradewire.EncodeResponse(upgradewire.Response{Code: upgradetx.CodeDeferred, Error: "x"})
	if strings.Contains(string(plain), "description") {
		t.Fatal("a non-describe reply carries a description key")
	}
}

// origin: S8 FIX-DESCRIBE (P19 request R1). Describe is authorized like the
// other S8 operations and answers only for the configured component: another
// session's or node's selector is not_owner; an unauthenticated non-admin is
// refused; an endpoint that cannot describe itself is not wired (75) before
// any download; and a description of another Key, for another nonce, with a
// partial scope or an agent "candidate" is never relayed as an answer.
func TestDescribeAnswersOnlyTheConfiguredComponent(t *testing.T) {
	key := wireKey()
	selector := upgradewire.DescribeSelector(key)
	request := func(s upgradetx.Key) []byte {
		return requestBytes(t, upgradewire.Request{Op: upgradewire.OpDescribe, Key: s, QueryNonce: describeNonce()})
	}
	honest := func(_ upgradetx.Key, nonce string) (upgradeport.Description, error) {
		return description(key, nonce), nil
	}
	spy := &describerSpy{answer: honest}
	for _, access := range []upgradewire.Access{{Key: key, Administrator: true}, {Key: key, ActorFP: "actor"}} {
		resp := upgradewire.Handle(t.Context(), spy, request(selector), access)
		if resp.Code != "" || resp.Description == nil || resp.Description.Key != key || resp.Description.Nonce != describeNonce() ||
			len(resp.Description.Scope.Domains) != len(allDomains) || !reflect.DeepEqual(resp.View, upgradetx.View{}) {
			t.Fatalf("configured component was not described: %+v", resp)
		}
	}
	other := selector
	other.ConfiguredNID = "other-node"
	session := selector
	session.SID = "other-session"
	asked := spy.asked
	for name, tc := range map[string]struct {
		body   []byte
		access upgradewire.Access
	}{
		"other_node":       {request(other), upgradewire.Access{Key: key, ActorFP: "actor"}},
		"other_session":    {request(session), upgradewire.Access{Key: key, ActorFP: "actor"}},
		"no_actor":         {request(selector), upgradewire.Access{Key: key}},
		"broker_selector":  {request(upgradewire.DescribeSelector(brokerKey())), upgradewire.Access{Key: key, ActorFP: "actor"}},
		"full_key_request": {[]byte(strings.Replace(string(request(selector)), `"domain_id":""`, `"domain_id":"installation"`, 1)), upgradewire.Access{Key: key, ActorFP: "actor"}},
	} {
		resp := upgradewire.Handle(t.Context(), spy, tc.body, tc.access)
		if resp.Code == "" || resp.Description != nil {
			t.Fatalf("%s: unauthorized describe answered: %+v", name, resp)
		}
		if (name == "other_node" || name == "other_session" || name == "no_actor") && resp.Code != proto.CodeNotOwner {
			t.Fatalf("%s: refused with %q, want not_owner", name, resp.Code)
		}
	}
	if spy.asked != asked {
		t.Fatal("an unauthorized describe reached the endpoint")
	}
	resp := upgradewire.Handle(t.Context(), &endpointSpy{key: key}, request(selector), upgradewire.Access{Key: key, ActorFP: "actor"})
	if exit, _ := upgradewire.ErrorExit(resp.Code); resp.Code != upgradetx.CodeNotWired || exit != 75 || resp.Description != nil {
		t.Fatalf("an endpoint without a describer answered: %+v", resp)
	}
	for name, answer := range map[string]func(upgradetx.Key, string) (upgradeport.Description, error){
		"other_installation": func(_ upgradetx.Key, nonce string) (upgradeport.Description, error) {
			k := key
			k.DomainID = "other-installation"
			d := description(k, nonce)
			return d, nil
		},
		"replayed_nonce": func(upgradetx.Key, string) (upgradeport.Description, error) {
			return description(key, strings.Repeat("0", 32)), nil
		},
		"missing_domain": func(_ upgradetx.Key, nonce string) (upgradeport.Description, error) {
			// Flow is no structurally required domain: only the completeness
			// rule can notice that this scope silently omits it.
			d := description(key, nonce)
			d.Scope.Domains = nil
			for _, domain := range allDomains {
				if domain != upgradetx.DomainFlow {
					d.Scope.Domains = append(d.Scope.Domains, domain)
				}
			}
			return d, nil
		},
		"second_participant": func(_ upgradetx.Key, nonce string) (upgradeport.Description, error) {
			d := description(key, nonce)
			peer := key
			peer.ConfiguredNID = "peer"
			d.Scope.Participants = append(d.Scope.Participants, peer)
			return d, nil
		},
		"agent_candidate": func(_ upgradetx.Key, nonce string) (upgradeport.Description, error) {
			d := description(key, nonce)
			d.Candidate = &upgradeport.CandidateDescription{SHA: strings.Repeat("2", 64), Release: "v0.8.0", CapabilityDigest: strings.Repeat("3", 64)}
			return d, nil
		},
		"unknown_running_image": func(_ upgradetx.Key, nonce string) (upgradeport.Description, error) {
			d := description(key, nonce)
			d.RunningSHA = ""
			return d, nil
		},
	} {
		resp := upgradewire.Handle(t.Context(), &describerSpy{answer: answer}, request(selector), upgradewire.Access{Key: key, ActorFP: "actor"})
		if resp.Code != upgradetx.CodeDeferred || resp.Error != upgradewire.DescriptionUnavailable || resp.Description != nil {
			t.Fatalf("%s: invalid description relayed: %+v", name, resp)
		}
	}
	census := &describerSpy{answer: func(upgradetx.Key, string) (upgradeport.Description, error) {
		return upgradeport.Description{}, &upgradetx.Error{Code: upgradetx.CodeDeferred, Detail: "domain_census_unavailable"}
	}}
	if resp := upgradewire.Handle(t.Context(), census, request(selector), upgradewire.Access{Key: key, ActorFP: "actor"}); resp.Code != upgradetx.CodeDeferred || resp.Error != "domain_census_unavailable" {
		t.Fatalf("the runtime's public refusal was lost: %+v", resp)
	}
	forwarded, err := upgradewire.EncodeForwarded(upgradewire.Request{Op: upgradewire.OpDescribe, Key: selector, QueryNonce: describeNonce()}, "actor")
	if err != nil {
		t.Fatal(err)
	}
	resp = upgradewire.HandleForwarded(t.Context(), &describerSpy{answer: honest}, key, forwarded)
	if resp.Code != "" || resp.Description == nil || resp.Description.Key != key {
		t.Fatalf("forwarded describe not answered: %+v", resp)
	}
	foreign := key
	foreign.ConfiguredNID = "other-node"
	if _, _, err := upgradewire.DecodeForwarded(forwarded, foreign); err == nil {
		t.Fatal("a forwarded describe selected another node")
	}
	var typed *upgradetx.Error
	if err := upgradewire.ValidateDescription(description(key, describeNonce()), selector, describeNonce()); err != nil || errors.As(err, &typed) {
		t.Fatalf("an honest description failed validation: %v", err)
	}
}
