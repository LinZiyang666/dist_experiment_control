package upgradecut

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func fixtureState() State {
	key := upgradetx.Key{DomainID: strings.Repeat("1", 32), Component: upgradetx.ComponentAgent, SID: "session-a", ConfiguredNID: "node-a"}
	owner := upgradetx.Owner{BootID: "11111111-2222-3333-4444-555555555555", PID: 123, StartTicks: 456, InstanceID: strings.Repeat("2", 32)}
	return State{
		Attempt:   upgradetx.Handle{Key: key, TxID: strings.Repeat("3", 32), CutID: strings.Repeat("4", 32), Generation: 2, Revision: 1},
		SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64),
		ParentToken: strings.Repeat("5", 32), Authorities: []Authority{{SID: key.SID, Incarnation: strings.Repeat("c", 64)}},
		Owner: owner, LockFD: 0,
		Scope: upgradetx.Scope{
			ID: strings.Repeat("6", 32), Participants: []upgradetx.Key{key},
			Domains:  []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainUpgrade},
			Expected: []upgradetx.Resource{{Domain: upgradetx.DomainExec, ID: "exec-child.123", Origin: "runtime-a", Owner: owner, Generation: 1, State: "running", Custody: "runtime"}},
		},
	}
}

func brokerFixtureState() State {
	state := fixtureState()
	state.Attempt.Key = upgradetx.Key{DomainID: state.Attempt.Key.DomainID, Component: upgradetx.ComponentBroker, ConfiguredNID: "broker-a", ConfigPath: "/fixture/broker.yaml"}
	state.Scope.Participants = []upgradetx.Key{state.Attempt.Key}
	state.Scope.Domains = []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainUpgrade}
	state.Scope.Expected = []upgradetx.Resource{{Domain: upgradetx.DomainBrokerStore, ID: "store.original", Origin: "broker-a", Owner: state.Owner, Generation: 1, State: "closed", Custody: "import"}}
	state.Authorities = append(state.Authorities, Authority{SID: "session-b", Incarnation: strings.Repeat("d", 64)})
	return state
}

func requireAdjunctRefusal(t *testing.T, err error) {
	t.Helper()
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Code != upgradetx.CodeRejected || refusal.Accepted {
		t.Fatalf("expected an unaccepted adjunct refusal, got %v", err)
	}
	if refusal.Detail != "invalid upgrade adjunct" {
		t.Fatal("refusal copied private input into diagnostics")
	}
}

// origin: hot-update S8 plan §6.1 / §9.1 / P07
func TestPrivateAdjunctCodecClosure(t *testing.T) {
	for _, fixture := range []State{fixtureState(), brokerFixtureState()} {
		data, err := Encode(fixture)
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(data)
		if err != nil || !reflect.DeepEqual(decoded, fixture) {
			t.Fatalf("nonempty lineage, authority map or resource custody changed: %v", err)
		}
		again, err := Encode(decoded)
		if err != nil || !bytes.Equal(data, again) {
			t.Fatal("codec did not preserve the canonical cut")
		}
	}
	t.Run("explicit idle broker", func(t *testing.T) {
		state := brokerFixtureState()
		state.Authorities = []Authority{}
		state.Scope.Expected = []upgradetx.Resource{}
		state.ParentToken = ""
		data, err := Encode(state)
		if err != nil {
			t.Fatal(err)
		}
		got, err := Decode(data)
		if err != nil || got.Authorities == nil || got.Scope.Expected == nil {
			t.Fatal("explicit idle decisions disappeared")
		}
	})
	t.Run("every scope domain", func(t *testing.T) {
		state := brokerFixtureState()
		state.Scope.Domains = []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}
		data, err := Encode(state)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Decode(data); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("bare boot ID and last FD index", func(t *testing.T) {
		state := fixtureState()
		state.Owner.BootID = strings.Repeat("7", 32)
		state.LockFD = maxFDIndex
		data, err := Encode(state)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Decode(data); err != nil {
			t.Fatal(err)
		}
	})
	for _, defect := range []stateDefect{
		{"nil broker authority map", func(s *State) { s.Authorities = nil }},
		{"unordered broker authorities", func(s *State) { s.Authorities[0], s.Authorities[1] = s.Authorities[1], s.Authorities[0] }},
		{"duplicate broker authority", func(s *State) { s.Authorities[1] = s.Authorities[0] }},
		{"empty broker authority SID", func(s *State) { s.Authorities[0].SID = "" }},
		{"missing broker config", func(s *State) { s.Attempt.Key.ConfigPath = "" }},
		{"missing sibling authority", func(s *State) {
			key := fixtureState().Attempt.Key
			key.SID = "session-unlisted"
			// Same domain; lexical component order puts agent before broker.
			s.Scope.Participants = append([]upgradetx.Key{key}, s.Scope.Participants...)
		}},
	} {
		t.Run(defect.name, func(t *testing.T) {
			state := brokerFixtureState()
			defect.mutate(&state)
			_, err := Encode(state)
			requireAdjunctRefusal(t, err)
		})
	}
	t.Run("encoded allocation ceiling", func(t *testing.T) {
		state := fixtureState()
		resource := state.Scope.Expected[0]
		state.Scope.Expected = make([]upgradetx.Resource, 256)
		for i := range state.Scope.Expected {
			state.Scope.Expected[i] = resource
			state.Scope.Expected[i].ID = fmt.Sprintf("%04d", i) + strings.Repeat("x", maxIdentityBytes-4)
		}
		if !validScope(state.Scope, state.Attempt.Key) {
			t.Fatal("oversize-byte witness failed collection validation first")
		}
		data, err := Encode(state)
		requireAdjunctRefusal(t, err)
		if data != nil {
			t.Fatal("oversize cut escaped the byte ceiling")
		}
	})
	data, err := Encode(fixtureState())
	if err != nil {
		t.Fatal(err)
	}
	ownerData, err := json.Marshal(fixtureState().Owner)
	if err != nil {
		t.Fatal(err)
	}
	wireDefects := []struct {
		name string
		data []byte
	}{
		{"unknown optional kind", bytes.Replace(data, []byte(`"kind":"upgrade-cut-v1","required":true`), []byte(`"kind":"unreadable-cut-v1","required":false`), 1)},
		{"unknown required kind", bytes.Replace(data, []byte(Version), []byte("upgrade-cut-v2"), 1)},
		{"optional current kind", bytes.Replace(data, []byte(`"required":true`), []byte(`"required":false`), 1)},
		{"missing required decision", bytes.Replace(data, []byte(`"required":true,`), nil, 1)},
		{"unknown envelope field", append(append([]byte{}, data[:len(data)-1]...), []byte(`,"optional_extra":0}`)...)},
		{"unknown state field", bytes.Replace(data, []byte(`"state":{`), []byte(`"state":{"optional_extra":0,`), 1)},
		{"unknown key field", bytes.Replace(data, []byte(`"key":{`), []byte(`"key":{"optional_extra":0,`), 1)},
		{"unknown owner field", bytes.Replace(data, []byte(`"owner":{`), []byte(`"owner":{"optional_extra":0,`), 1)},
		{"unknown authority field", bytes.Replace(data, []byte(`"authorities":[{`), []byte(`"authorities":[{"optional_extra":0,`), 1)},
		{"unknown scope field", bytes.Replace(data, []byte(`"scope":{`), []byte(`"scope":{"optional_extra":0,`), 1)},
		{"unknown resource field", bytes.Replace(data, []byte(`"expected":[{`), []byte(`"expected":[{"optional_extra":0,`), 1)},
		{"duplicate envelope field", bytes.Replace(data, []byte(`"required":true`), []byte(`"required":false,"required":true`), 1)},
		{"duplicate nested decision", bytes.Replace(data, []byte(`"lock_fd":0`), []byte(`"lock_fd":1,"lock_fd":0`), 1)},
		{"missing zero FD index", bytes.Replace(data, []byte(`"lock_fd":0,`), nil, 1)},
		{"missing parent decision", bytes.Replace(data, []byte(`"parent_token":"`+strings.Repeat("5", 32)+`",`), nil, 1)},
		{"null owner", bytes.Replace(data, append([]byte(`"owner":`), ownerData...), []byte(`"owner":null`), 1)},
		{"case folded kind", bytes.Replace(data, []byte(`"kind"`), []byte(`"Kind"`), 1)},
		{"escaped field", bytes.Replace(data, []byte(`"kind"`), []byte(`"\u006bind"`), 1)},
		{"truncated", data[:len(data)-1]},
		{"trailing record", append(append([]byte{}, data...), data...)},
		{"trailing whitespace", append(append([]byte{}, data...), '\n')},
		{"negative index", bytes.Replace(data, []byte(`"lock_fd":0`), []byte(`"lock_fd":-1`), 1)},
		{"overflow index", bytes.Replace(data, []byte(`"lock_fd":0`), []byte(`"lock_fd":4294967296`), 1)},
		{"invalid UTF8", append(append([]byte{}, data...), 0xff)},
		{"empty input", nil},
		{"oversize input", bytes.Repeat([]byte{' '}, maxAdjunctBytes+1)},
	}
	for _, defect := range wireDefects {
		t.Run(defect.name, func(t *testing.T) {
			state, err := Decode(defect.data)
			requireAdjunctRefusal(t, err)
			if !reflect.DeepEqual(state, State{}) {
				t.Fatal("refused input escaped as consumable partial state")
			}
		})
	}
	for _, defect := range stateDefects() {
		t.Run(defect.name, func(t *testing.T) {
			state := fixtureState()
			defect.mutate(&state)
			data, err := Encode(state)
			requireAdjunctRefusal(t, err)
			if data != nil {
				t.Fatal("refused state escaped as an encoded cut")
			}
			// A hostile exporter can bypass Encode; the actual reader must still
			// reject the identical state before any runtime takes its files.
			data, err = json.Marshal(adjunctEnvelope{Kind: Version, Required: true, State: state})
			if err != nil {
				t.Fatal(err)
			}
			_, err = Decode(data)
			requireAdjunctRefusal(t, err)
		})
	}
	t.Run("field inventory", func(t *testing.T) {
		want := []string{"Attempt:upgradetx.Handle:attempt", "SourceSHA:string:source_sha", "TargetSHA:string:target_sha", "ParentToken:string:parent_token", "Authorities:[]upgradecut.Authority:authorities", "Owner:upgradetx.Owner:owner", "LockFD:uint32:lock_fd", "Scope:upgradetx.Scope:scope"}
		if got := fieldInventory(reflect.TypeFor[State]()); !reflect.DeepEqual(got, want) {
			t.Fatalf("adjunct descriptor/actual projection changed: %v", got)
		}
		if got := fieldInventory(reflect.TypeFor[Authority]()); !reflect.DeepEqual(got, []string{"SID:string:sid", "Incarnation:string:incarnation"}) {
			t.Fatalf("authority projection changed: %v", got)
		}
		if got := fieldInventory(reflect.TypeFor[adjunctEnvelope]()); !reflect.DeepEqual(got, []string{"Kind:string:kind", "Required:bool:required", "State:upgradecut.State:state"}) {
			t.Fatalf("required-kind descriptor changed: %v", got)
		}
	})
}

func fieldInventory(typ reflect.Type) []string {
	fields := make([]string, typ.NumField())
	for i := range fields {
		field := typ.Field(i)
		fields[i] = field.Name + ":" + field.Type.String() + ":" + field.Tag.Get("json")
	}
	return fields
}

type stateDefect struct {
	name   string
	mutate func(*State)
}

func stateDefects() []stateDefect {
	return []stateDefect{
		{"empty transaction", func(s *State) { s.Attempt.TxID = "" }},
		{"malformed cut", func(s *State) { s.Attempt.CutID = strings.Repeat("A", 32) }},
		{"zero generation", func(s *State) { s.Attempt.Generation = 0 }},
		{"zero revision", func(s *State) { s.Attempt.Revision = 0 }},
		{"empty domain identity", func(s *State) { s.Attempt.Key.DomainID = "" }},
		{"unknown role", func(s *State) { s.Attempt.Key.Component = "observer" }},
		{"reserved component", func(s *State) { s.Attempt.Key.Component = upgradetx.ComponentNATS }},
		{"empty agent SID", func(s *State) { s.Attempt.Key.SID = "" }},
		{"empty configured NID", func(s *State) { s.Attempt.Key.ConfiguredNID = "" }},
		{"relative config", func(s *State) { s.Attempt.Key.ConfigPath = "relative.yaml" }},
		{"unclean config", func(s *State) { s.Attempt.Key.ConfigPath = "/fixture/../config.yaml" }},
		{"uppercase source SHA", func(s *State) { s.SourceSHA = strings.Repeat("A", 64) }},
		{"short target SHA", func(s *State) { s.TargetSHA = "abc" }},
		{"malformed parent token", func(s *State) { s.ParentToken = "another-parent-secret" }},
		{"oversize FD index", func(s *State) { s.LockFD = maxFDIndex + 1 }},
		{"missing boot identity", func(s *State) { s.Owner.BootID = "" }},
		{"missing PID", func(s *State) { s.Owner.PID = 0 }},
		{"negative PID", func(s *State) { s.Owner.PID = -1 }},
		{"malformed boot UUID", func(s *State) { s.Owner.BootID = "111111111-222-3333-4444-555555555555" }},
		{"missing start ticks", func(s *State) { s.Owner.StartTicks = 0 }},
		{"missing instance identity", func(s *State) { s.Owner.InstanceID = "" }},
		{"nil authority map", func(s *State) { s.Authorities = nil }},
		{"empty agent authority", func(s *State) { s.Authorities = []Authority{} }},
		{"foreign agent authority", func(s *State) { s.Authorities[0].SID = "session-b" }},
		{"malformed incarnation", func(s *State) { s.Authorities[0].Incarnation = "creation-salt-must-not-escape" }},
		{"duplicate authority", func(s *State) { s.Authorities = append(s.Authorities, s.Authorities[0]) }},
		{"oversize authority map", func(s *State) { s.Authorities = make([]Authority, maxEntries+1) }},
		{"missing scope seal", func(s *State) { s.Scope.ID = "" }},
		{"missing participant", func(s *State) { s.Scope.Participants = []upgradetx.Key{} }},
		{"foreign participant", func(s *State) { s.Scope.Participants[0].ConfiguredNID = "node-b" }},
		{"duplicate participant", func(s *State) { s.Scope.Participants = append(s.Scope.Participants, s.Scope.Participants[0]) }},
		{"oversize participants", func(s *State) { s.Scope.Participants = make([]upgradetx.Key, maxEntries+1) }},
		{"missing domains", func(s *State) { s.Scope.Domains = nil }},
		{"missing authority domain", func(s *State) { s.Scope.Domains = s.Scope.Domains[1:] }},
		{"missing control domain", func(s *State) { s.Scope.Domains = append(s.Scope.Domains[:1], s.Scope.Domains[2:]...) }},
		{"missing upgrade domain", func(s *State) { s.Scope.Domains = s.Scope.Domains[:3] }},
		{"unknown domain", func(s *State) { s.Scope.Domains[2] = "unknown" }},
		{"duplicate domain", func(s *State) { s.Scope.Domains[2] = upgradetx.DomainControl }},
		{"unordered domains", func(s *State) { s.Scope.Domains[0], s.Scope.Domains[1] = s.Scope.Domains[1], s.Scope.Domains[0] }},
		{"nil expected resources", func(s *State) { s.Scope.Expected = nil }},
		{"undeclared resource domain", func(s *State) { s.Scope.Expected[0].Domain = upgradetx.DomainTransferWorker }},
		{"missing resource identity", func(s *State) { s.Scope.Expected[0].ID = "" }},
		{"missing resource origin", func(s *State) { s.Scope.Expected[0].Origin = "" }},
		{"missing resource owner", func(s *State) { s.Scope.Expected[0].Owner = upgradetx.Owner{} }},
		{"zero resource generation", func(s *State) { s.Scope.Expected[0].Generation = 0 }},
		{"missing resource state", func(s *State) { s.Scope.Expected[0].State = "" }},
		{"missing custody", func(s *State) { s.Scope.Expected[0].Custody = "" }},
		{"duplicate resource", func(s *State) { s.Scope.Expected = append(s.Scope.Expected, s.Scope.Expected[0]) }},
		{"oversize expected set", func(s *State) { s.Scope.Expected = make([]upgradetx.Resource, maxEntries+1) }},
		{"control in identity", func(s *State) { s.Scope.Expected[0].ID = "resource\x00hidden" }},
		{"DEL in identity", func(s *State) { s.Scope.Expected[0].ID = "resource\x7fhidden" }},
		{"oversize identity", func(s *State) { s.Scope.Expected[0].ID = strings.Repeat("x", maxIdentityBytes+1) }},
		{"invalid UTF8 identity", func(s *State) { s.Scope.Expected[0].ID = string([]byte{0xff}) }},
	}
}
