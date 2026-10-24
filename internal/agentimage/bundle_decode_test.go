//go:build linux

// bundle_decode_test.go — Decode's strictness at EVERY nesting level of a
// real bundle. bundle_test.go proves an unknown key is refused at the top
// level and inside "runtime"; D7 ("all domains DisallowUnknownFields") is a
// claim about the whole tree — a key added by a newer exporter inside a
// lease, a stream actor, a file identity, a process row, a courier event or
// a mount verdict must be refused just the same, and the one deliberate
// opaque leaf (a courier event's verbatim wire payload) must NOT be.
// origin: hot-update S2 internal review, import lane.
package agentimage

import (
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/testharness"
)

func TestDecodeRefusesUnknownKeysAtEveryNestingLevel(t *testing.T) {
	// origin: hot-update S5 PROC-LEAK / final process census.
	testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

	f := newBundleFixture(t, "decode-instance")
	hLive, _ := f.create(t, "live", []string{"/bin/sh", "-c", "exec sleep 30"}, []byte("unread stdin"))
	f.awaitRunning(t, hLive)
	probe := &bundleProbe{}
	policy := bundleSpawnPolicy(t, "/decode-nfs", probe, time.Now)
	if _, err := policy.Prepare([]string{"/bin/true"}, "/decode-nfs", "/usr/bin", nil, false); err != nil {
		t.Fatal(err)
	}
	courier := &courierStub{state: courierEvents()}
	frozen, err := Freeze(Owners{Runtime: f.rt, Manager: f.manager, Spawn: policy, Courier: courier, Lineage: Lineage{Generation: 2}}, time.Now().Add(bundleTestBudget))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = frozen.Thaw() })
	good := frozen.Bundle()
	data, err := encode(good)
	if err != nil {
		t.Fatal(err)
	}
	envelope := func(s string) handoff.State {
		return handoff.State{Role: "agent", Instance: good.Instance, Generation: 2, Data: []byte(s)}
	}
	if _, err := Decode(envelope(string(data))); err != nil {
		t.Fatalf("positive control: the intact bundle does not decode: %v", err)
	}
	// Each row names an anchor that exists exactly once at the intended
	// depth and the JSON object it lives in; the unknown key is injected
	// right before the anchor, inside that object.
	rows := []struct {
		level  string
		anchor string
	}{
		{"lineage", `"proto_version"`},
		{"processes.records[i] (embedded Identity, promoted)", `"os_pid"`},
		{"runtime.registry", `"boot_id":"` + good.Runtime.Registry.BootID + `","secret"`},
		{"runtime.registry.records[i]", `"intent_hash"`},
		{"runtime.registry.records[i].handle.lease", `"issued_boot_ns"`},
		{"runtime.tasks[i]", `"last_touch"`},
		{"runtime.tasks[i].stdin.input", `"eof"`},
		{"runtime.tasks[i].stdout.output", `"output":{"version"`},
		{"runtime.tasks[i].stdout.replay", `"replay":{"version"`},
		{"runtime.tasks[i].files[name] (map value)", `"stdout-source":{"dev"`},
		{"courier.events[i]", `"enqueued_at_unix_nano"`},
		{"spawn_safe", `"verdicts"`},
		{"spawn_safe.verdicts[i]", `"decided_at_unix_nano"`},
	}
	for _, tc := range rows {
		t.Run(tc.level, func(t *testing.T) {
			// origin: hot-update S5 PROC-LEAK / final process census.
			testharness.GuardProcessDirectory(t, filepath.Dir(t.TempDir()))

			s := string(data)
			if n := strings.Count(s, tc.anchor); n == 0 {
				t.Fatalf("anchor %q not found: the row is vacuous", tc.anchor)
			}
			// Inject inside the object that the anchor's key belongs to: for
			// anchors of the form `"key":{"first"` the unknown key goes after
			// the brace, otherwise right before the anchor key.
			var mutated string
			if i := strings.Index(tc.anchor, `:{`); i >= 0 {
				mutated = strings.Replace(s, tc.anchor, tc.anchor[:i+2]+`"zz_future_field":1,`+tc.anchor[i+2:], 1)
			} else {
				mutated = strings.Replace(s, tc.anchor, `"zz_future_field":1,`+tc.anchor, 1)
			}
			if mutated == s {
				t.Fatal("injection did not change the document")
			}
			_, err := Decode(envelope(mutated))
			if err == nil {
				t.Fatalf("an unknown key inside %s was accepted", tc.level)
			}
			if !errors.Is(err, ErrBundle) || !strings.Contains(err.Error(), "zz_future_field") {
				t.Fatalf("refusal of %s is not the strict-decoder refusal: %v", tc.level, err)
			}
		})
	}
	// The deliberate opaque leaf: a courier payload is the verbatim wire body
	// the next image re-sends; its own schema belongs to the wire ledger, so
	// an extra key inside it is carried, not refused.
	s := string(data)
	anchor := `"payload":{"pid"`
	if strings.Count(s, anchor) == 0 {
		t.Fatalf("payload anchor missing")
	}
	mutated := strings.Replace(s, anchor, `"payload":{"zz_future_wire_field":1,"pid"`, 1)
	b, err := Decode(envelope(mutated))
	if err != nil {
		t.Fatalf("an unknown key inside a courier PAYLOAD (opaque wire body) was refused: %v", err)
	}
	if !strings.Contains(string(b.Courier.Events[0].Payload), "zz_future_wire_field") {
		t.Fatal("payload was not carried verbatim")
	}
}
