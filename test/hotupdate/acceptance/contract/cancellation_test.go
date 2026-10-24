package contract_test

import (
	"encoding/json"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// The schema transports the native cancellation proof without interpreting a
// thaw as successful target completion. Missing and unknown fields stay closed.
// origin: S10 internal review MAKETEST-GATES F6 / S8 plan §4.2 / §9.1
func TestCancellationProofPreservesNativeRecordShape(t *testing.T) {
	var schema map[string]json.RawMessage
	if err := json.Unmarshal(c.Schema(), &schema); err != nil {
		t.Fatal(err)
	}
	var defs map[string]json.RawMessage
	if err := json.Unmarshal(schema["$defs"], &defs); err != nil {
		t.Fatal(err)
	}
	var record upgradetx.Record
	if err := json.Unmarshal(marshal(t, example(t, defs, defs["s8_record"])), &record); err != nil {
		t.Fatal(err)
	}
	var observation upgradetx.Observation
	if err := json.Unmarshal(marshal(t, example(t, defs, defs["s8_observation"])), &observation); err != nil {
		t.Fatal(err)
	}
	for _, proof := range []*upgradetx.CancellationProof{nil, {Thawed: true, Observation: observation}, {Thawed: false, Observation: observation}} {
		record.Cancellation = proof
		raw, err := json.Marshal(record)
		if err != nil {
			t.Fatal(err)
		}
		if err := c.ValidateJSON("s8_record", raw); err != nil {
			t.Fatalf("native cancellation record rejected: %s: %v", raw, err)
		}
		if proof == nil {
			continue
		}
		var fields map[string]any
		if err := json.Unmarshal(raw, &fields); err != nil {
			t.Fatal(err)
		}
		body := fields["cancellation"].(map[string]any)
		for _, key := range []string{"thawed", "observation"} {
			value := body[key]
			delete(body, key)
			if c.ValidateJSON("s8_record", marshal(t, fields)) == nil {
				t.Fatalf("cancellation accepted missing %s", key)
			}
			body[key] = value
		}
		body["unknown"] = true
		if c.ValidateJSON("s8_record", marshal(t, fields)) == nil {
			t.Fatal("unknown cancellation field accepted")
		}
		delete(body, "unknown")
		body["thawed"] = "true"
		if c.ValidateJSON("s8_record", marshal(t, fields)) == nil {
			t.Fatal("non-boolean thaw evidence accepted")
		}
	}
}
