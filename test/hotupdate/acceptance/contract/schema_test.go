// schema_test.go checks strict disk-schema validation with synthetic inputs.
// No runtime or deployed process is started or certified by these controls.
package contract_test

import (
	"bytes"
	"encoding/json"
	"strconv"
	"strings"
	"testing"

	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

func checkSchema(t *testing.T, defs map[string]json.RawMessage) {
	t.Helper()
	for name, raw := range defs {
		s := readNode(t, raw)
		if s.Type != "object" {
			continue
		}
		if s.Additional == nil || *s.Additional {
			t.Errorf("object %s is open", name)
		}
		v := example(t, defs, raw).(map[string]any)
		if name == "result" {
			v["rc"] = 0
			v["verdict"] = "GREEN"
		}
		positive := marshal(t, v)
		if err := c.ValidateJSON(name, positive); err != nil {
			t.Fatalf("valid %s: %v", name, err)
		}
		// Begin with valid evidence so schema rejection cannot hide a parser
		// accepting a second value or malformed suffix after the document.
		// origin: hot-update S10 plan §5.1 / LINT-s10
		for _, suffix := range []string{` {}`, ` null`, ` []`, ` "extra"`, ` true`, ` 7`, ` garbage`} {
			trailing := append(bytes.Clone(positive), suffix...)
			if err := c.ValidateJSON(name, trailing); err == nil {
				t.Errorf("%s accepted trailing JSON %s", name, suffix)
			}
		}
		if err := c.ValidateJSON(name, append(bytes.Clone(positive), ' ', '\n', '\t')); err != nil {
			t.Errorf("%s rejected legal trailing whitespace: %v", name, err)
		}
		for _, key := range s.Required {
			old := v[key]
			delete(v, key)
			if err := c.ValidateJSON(name, marshal(t, v)); err == nil {
				t.Errorf("%s accepted missing %s", name, key)
			}
			v[key] = old
		}
		v["unknown_member"] = true
		if err := c.ValidateJSON(name, marshal(t, v)); err == nil {
			t.Errorf("%s accepted unknown field", name)
		}
		for _, key := range s.Required {
			duplicate := append([]byte(`{`+strconv.Quote(key)+`:null,`), positive[1:]...)
			if err := c.ValidateJSON(name, duplicate); err == nil {
				t.Errorf("%s accepted duplicate %s", name, key)
			}
		}
	}
	for _, name := range []string{"evidence", "workload", "transfer", "abi", "command_bindings", "log_roster", "log_cursor", "log_end"} {
		if _, ok := defs[name]; !ok {
			t.Errorf("missing manifest %s", name)
		}
	}
	profile := example(t, defs, defs["profile"]).(map[string]any)
	for _, field := range []string{"required", "artifacts"} {
		original := profile[field]
		for _, bad := range []any{[]any{}, nil, []any{original.([]any)[0], original.([]any)[0]}} {
			profile[field] = bad
			if c.ValidateJSON("profile", marshal(t, profile)) == nil {
				t.Errorf("profile accepted invalid %s", field)
			}
		}
		profile[field] = original
	}
	for _, bad := range []string{`{}`, `null`, `[]`, `{} {}`, `{"role":"agent","\u0072ole":"broker"}`, `{"required":[{"role":"agent","role":"broker"}]}`} {
		if c.ValidateJSON("profile", []byte(bad)) == nil {
			t.Errorf("malformed profile accepted %s", bad)
		}
	}
	for _, name := range []string{"s8_attempt", "s8_receipt"} {
		zero := readNode(t, defs[name+"_empty"]).Const
		if err := c.ValidateJSON(name, zero); err != nil {
			t.Fatalf("absent upstream state: %v", err)
		}
		var altered map[string]any
		if err := json.Unmarshal(zero, &altered); err != nil {
			t.Fatal(err)
		}
		if name == "s8_attempt" {
			altered["direction"] = "forward"
		} else {
			altered["known"] = true
		}
		if c.ValidateJSON(name, marshal(t, altered)) == nil {
			t.Fatal("partial zero upstream state accepted as present")
		}
	}
	if c.ValidateJSON("future", []byte(`{}`)) == nil {
		t.Fatal("unknown schema accepted")
	}
	if c.ValidateJSON("text", []byte{'"', 0xff, '"'}) == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
	for _, bad := range []string{`-1`, `18446744073709551616`, `1.5`, `1e2`, `"1"`, `null`} {
		if c.ValidateJSON("uint64", []byte(bad)) == nil {
			t.Errorf("invalid uint64 accepted %s", bad)
		}
	}
	if err := c.ValidateJSON("uint64", []byte(`18446744073709551615`)); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{`"a"`, `"` + strings.Repeat("A", 64) + `"`, `[]`} {
		if c.ValidateJSON("digest", []byte(bad)) == nil {
			t.Fatal("invalid digest accepted")
		}
	}
	evidence := example(t, defs, defs["evidence"]).(map[string]any)
	for _, tier := range []string{"deploy", "hermetic", "source_gate"} {
		evidence["profile"].(map[string]any)["tier"] = tier
		err := c.ValidateJSON("deploy_evidence", marshal(t, evidence))
		if (err == nil) != (tier == "deploy") {
			t.Errorf("deploy tier guard failed for %s: %v", tier, err)
		}
	}
	for rc, verdict := range []string{"GREEN", "ASSERT-FAIL", "SETUP-RED", "PRODUCT-RED", "INCOMPLETE"} {
		v := example(t, defs, defs["result"]).(map[string]any)
		v["verdict"] = verdict
		v["rc"] = rc
		if err := c.ValidateJSON("result", marshal(t, v)); err != nil {
			t.Fatal(err)
		}
		for _, bad := range []int{(rc + 1) % 5, 64, 74, 75, 77} {
			v["rc"] = bad
			if c.ValidateJSON("result", marshal(t, v)) == nil {
				t.Errorf("%s accepted rc %d", verdict, bad)
			}
		}
	}
}
