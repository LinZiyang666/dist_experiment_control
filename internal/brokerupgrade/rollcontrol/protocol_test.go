package rollcontrol

import (
	"encoding/json"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradewire"
)

// origin: S8 internal review cluster-roll F4
func TestRollEnvelopeKeepsItsOwnStrictRoundTrip(t *testing.T) {
	for _, value := range []any{Request{Op: "roll.plan"}, Response{}} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		switch value.(type) {
		case Request:
			var r Request
			err = upgradewire.StrictDecode(data, &r)
		case Response:
			var r Response
			err = upgradewire.StrictDecode(data, &r)
		}
		if err != nil {
			t.Fatalf("strict round trip: %v (%s)", err, data)
		}
	}
}

// origin: S8 internal review cluster-roll F4
func TestRollEnvelopeRejectsAmbiguousAndUnknownOperations(t *testing.T) {
	for _, raw := range []string{`{"op":"roll.unknown"}`, `{"op":"roll.load","op":"roll.plan"}`, `{"Op":"roll.plan"}`, `{"op":"roll.plan","unknown":1}`, `{"op":"roll.plan"} {}`} {
		if _, err := Decode([]byte(raw)); err == nil {
			t.Fatalf("ambiguous roll envelope accepted: %s", raw)
		}
	}
}
