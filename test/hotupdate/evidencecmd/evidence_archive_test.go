package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"

	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// origin: S10 internal review rv-honesty F1
func TestEvidenceRejectsUndecodableRawReceipts(t *testing.T) {
	root := t.TempDir()
	raw := []byte("not JSON, not command bindings, not a receipt\n")
	ref := c.RawRef{Path: "bindings.json", SHA: sha256.Sum256(raw), Bytes: int64(len(raw)), Schema: "command_bindings"}
	if err := c.ValidateJSON("command_bindings", raw); err == nil {
		t.Fatal("bad negative control")
	}
	key := c.Key{Role: "agent", Host: "host", Unit: "unit", Domain: "domain"}
	profile := c.Profile{ID: "archive-schema-control", Topology: "single", Regime: "quiet", Tier: "hermetic", EvidenceRoot: root, CommandBindings: ref, ProbeRate: 10, EndNS: 100,
		Required: []c.Key{key}, Artifacts: []c.Artifact{{ID: "a", Role: "agent", ReadEpochs: []uint64{1}, Domains: []string{"control"}}}}
	e := c.Evidence{Profile: profile, Raw: []c.RawRef{ref}}
	for name, value := range map[string]any{"profile": profile, "evidence": e} {
		data, err := c.EncodeJSON(name, value)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, name+".json"), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, ref.Path), raw, 0600); err != nil {
		t.Fatal(err)
	}
	var out, diagnostic bytes.Buffer
	rc := run([]string{"validate", "--profile", filepath.Join(root, "profile.json"), "--evidence", root}, &out, &diagnostic)
	if rc == 0 {
		t.Fatalf("raw receipt schema was not checked: native_rc=%d output=%s", rc, out.String())
	}
}

// Matching raw hashes cannot excuse an adapter changing return codes, executing
// SHA, or latency. Exercise every real command entry, with an archive-valid
// positive control; this small archive does not claim a complete rollout.
// origin: S10 internal review rv-honesty F1
func TestEvidenceRejectsContradictoryProjections(t *testing.T) {
	for _, defect := range []string{"return code", "executing SHA", "latency", "combined", "unknown schema", "pending decoder", "interval", "current view", "historical view", "receipt", "receipt projection", "view stamp"} {
		t.Run(defect, func(t *testing.T) {
			root, e, archive := projectionArchive(t)
			writeProjectionArchive(t, root, e, archive)
			if rc, output := archiveCommand(t, root, "validate"); rc != 0 {
				t.Fatalf("valid archive: rc=%d %s", rc, output)
			}
			switch defect {
			case "return code":
				e.Attempts[0].RC, e.Attempts[0].Code = 0, ""
			case "executing SHA":
				e.Samples[0].ExeSHA = sha256.Sum256([]byte("new image"))
			case "latency":
				e.Attempts[0].FinishedNS = e.Attempts[0].StartedNS + 1
			case "combined":
				e.Attempts[0].RC, e.Attempts[0].Code = 0, ""
				e.Attempts[0].FinishedNS = e.Attempts[0].StartedNS + 1
				e.Samples[0].ExeSHA = sha256.Sum256([]byte("new image"))
			case "unknown schema", "pending decoder":
				ref := e.Attempts[0].Raw
				if defect == "unknown schema" {
					ref.Schema = "unknown-upstream-schema"
				} else {
					ref.Schema = "fullstack-kernel-identity-v1"
				}
				archive[ref] = archive[e.Attempts[0].Raw]
				e.Raw = append(e.Raw, ref)
			case "interval":
				e.Intervals[0].AtNS++
			case "current view":
				e.Views[0].Current = bytes.Replace(e.Views[0].Current, []byte(`"owner_known":true`), []byte(`"owner_known":false`), 1)
			case "historical view":
				e.Views[0].Historical = bytes.Replace(e.Views[0].Historical, []byte(`"reason":""`), []byte(`"reason":"upgrade_deferred"`), 1)
			case "receipt":
				e.Views[0].Control.Evidence = e.Views[0].Resources.Evidence
			case "receipt projection":
				e.Views[0].Control.Complete = false
			case "view stamp":
				e.Views[0].Stamp.Generation++
			}
			writeProjectionArchive(t, root, e, archive)
			for _, command := range []string{"validate", "judge", "quantiles"} {
				if rc, output := archiveCommand(t, root, command); rc != 2 || !strings.Contains(output, "SETUP-RED") {
					t.Fatalf("%s accepted contradictory archive: rc=%d %s", command, rc, output)
				}
			}
		})
	}
}

// Binding validity is independent of an otherwise valid evidence projection.
// Unknown or missing command/help schemas are rejected before any command data
// could be interpreted; no shell or installed executable is used by this test.
// origin: S10 internal review rv-honesty F1
func TestEvidenceChecksCommandBindingsIndependently(t *testing.T) {
	for _, defect := range []string{"wrong binding schema", "missing query", "duplicate action", "wrong view schema", "missing help", "invalid help", "duplicate JSON member"} {
		t.Run(defect, func(t *testing.T) {
			root, e, archive := projectionArchive(t)
			writeProjectionArchive(t, root, e, archive)
			if rc, output := archiveCommand(t, root, "validate"); rc != 0 {
				t.Fatalf("valid bindings: rc=%d %s", rc, output)
			}
			ref := e.Profile.CommandBindings
			raw := archive[ref]
			switch defect {
			case "wrong binding schema":
				ref.Schema = "text"
				raw = []byte(`"not command bindings"`)
			case "missing query":
				var document struct {
					Version  int               `json:"version"`
					Bindings []json.RawMessage `json:"bindings"`
				}
				if err := json.Unmarshal(raw, &document); err != nil {
					t.Fatal(err)
				}
				document.Bindings = append(document.Bindings[:2], document.Bindings[3:]...)
				var err error
				raw, err = json.Marshal(document)
				if err != nil {
					t.Fatal(err)
				}
			case "duplicate action":
				raw = bytes.ReplaceAll(raw, []byte(`"action":"apply"`), []byte(`"action":"stage"`))
			case "wrong view schema":
				raw = bytes.ReplaceAll(raw, []byte(`"raw_view_schema":"s8_view"`), []byte(`"raw_view_schema":"pending_s9_view"`))
			case "missing help":
				if err := os.Remove(filepath.Join(root, "help.json")); err != nil {
					t.Fatal(err)
				}
				for key := range archive {
					if key.Path == "help.json" {
						delete(archive, key)
					}
				}
			case "invalid help":
				bad := []byte("not JSON help")
				var old c.RawRef
				for key := range archive {
					if key.Path == "help.json" {
						old = key
						delete(archive, key)
					}
				}
				next := old
				next.SHA, next.Bytes = sha256.Sum256(bad), int64(len(bad))
				archive[next] = bad
				oldJSON, err := c.EncodeJSON("raw_ref", old)
				if err != nil {
					t.Fatal(err)
				}
				nextJSON, err := c.EncodeJSON("raw_ref", next)
				if err != nil {
					t.Fatal(err)
				}
				raw = bytes.ReplaceAll(raw, oldJSON, nextJSON)
			case "duplicate JSON member":
				raw = bytes.Replace(raw, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1)
			}
			ref.SHA, ref.Bytes = sha256.Sum256(raw), int64(len(raw))
			for i := range e.Raw {
				if e.Raw[i] == e.Profile.CommandBindings {
					e.Raw[i] = ref
				}
			}
			e.Profile.CommandBindings = ref
			archive[ref] = raw
			writeProjectionArchive(t, root, e, archive)
			if rc, output := archiveCommand(t, root, "validate"); rc != 2 {
				t.Fatalf("invalid bindings accepted: rc=%d %s", rc, output)
			}
		})
	}
}

func archiveCommand(t *testing.T, root, command string) (int, string) {
	t.Helper()
	var out, diagnostic bytes.Buffer
	rc := run([]string{command, "--profile", filepath.Join(root, "profile.json"), "--evidence", root}, &out, &diagnostic)
	return rc, out.String() + diagnostic.String()
}

func writeProjectionArchive(t *testing.T, root string, e c.Evidence, archive map[c.RawRef][]byte) {
	t.Helper()
	for name, value := range map[string]any{"profile": e.Profile, "evidence": e} {
		raw, err := c.EncodeJSON(name, value)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(filepath.Join(root, name+".json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for ref, raw := range archive {
		// Old refs may coexist after a test changes bytes; only write the current
		// digest for the binding path so map iteration cannot restore old content.
		if ref.Path == e.Profile.CommandBindings.Path && ref != e.Profile.CommandBindings {
			continue
		}
		if err := os.WriteFile(filepath.Join(root, ref.Path), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func projectionArchive(t *testing.T) (string, c.Evidence, map[c.RawRef][]byte) {
	t.Helper()
	root := t.TempDir()
	archive := make(map[c.RawRef][]byte)
	add := func(name, schema string, raw []byte) c.RawRef {
		t.Helper()
		if err := c.ValidateJSON(schema, raw); err != nil {
			t.Fatalf("fixture schema %s: %v", schema, err)
		}
		ref := c.RawRef{Path: name + ".json", Schema: schema, SHA: sha256.Sum256(raw), Bytes: int64(len(raw))}
		archive[ref] = raw
		return ref
	}
	help := add("help", "text", []byte(`"hermetic command help receipt"`))
	helpJSON, err := c.EncodeJSON("raw_ref", help)
	if err != nil {
		t.Fatal(err)
	}
	var bindings []any
	for _, action := range []string{"stage", "apply", "query", "rollback"} {
		bindings = append(bindings, map[string]any{"id": action, "role": "agent", "action": action, "argv": []string{"fixture-command", action}, "timeout_ns": 10, "help": json.RawMessage(helpJSON), "raw_view_schema": "s8_view"})
	}
	encoded, err := json.Marshal(map[string]any{"version": 1, "bindings": bindings})
	if err != nil {
		t.Fatal(err)
	}
	binding := add("bindings", "command_bindings", encoded)
	key := c.Key{Role: "agent", Host: "host", Unit: "unit", Domain: "domain", SID: "sid", NID: "node"}
	stamp := c.Stamp{Process: c.Process{Key: key, BootID: "boot", PIDNS: "pidns", InstanceID: "instance", PID: 123, StartTicks: 456}, Direction: "forward", TxID: "tx", CutID: "cut", QueryNonce: "query", Generation: 2, Revision: 20, Sequence: 50, AtBootNS: 200, ScopeDigest: "scope"}
	e := c.Evidence{Profile: c.Profile{ID: "archive-projection-control", Topology: "single", Regime: "quiet", Tier: "hermetic", EvidenceRoot: root, CommandBindings: binding, ProbeRate: 10, EndNS: 20_000_000_000, Required: []c.Key{key}, Artifacts: []c.Artifact{{ID: "old", Role: "agent", ReadEpochs: []uint64{1}, Domains: []string{"control"}}}}}
	sample := c.Sample{Stamp: stamp, ExeSHA: sha256.Sum256([]byte("old image")), Complete: true, Raw: c.RawRef{Path: "producer", Schema: "sample"}}
	encoded, err = c.EncodeJSON("sample", sample)
	if err != nil {
		t.Fatal(err)
	}
	sample.Raw = add("sample", "sample", encoded)
	e.Samples = []c.Sample{sample}
	attempt := c.Attempt{SlotID: "slot", CallID: "call", Index: 1, Phase: "result", StartedNS: 1, FinishedNS: 10_000_000_000, Sent: true, Completed: true, RC: 75, Code: "upgrade_deferred", Raw: c.RawRef{Path: "producer", Schema: "attempt"}}
	encoded, err = c.EncodeJSON("attempt", attempt)
	if err != nil {
		t.Fatal(err)
	}
	attempt.Raw = add("attempt", "attempt", encoded)
	e.Attempts = []c.Attempt{attempt}
	interval := c.Interval{Stamp: stamp, ResourceID: "resource", Event: "activate", AtNS: 100, Raw: c.RawRef{Path: "producer", Schema: "interval"}}
	encoded, err = c.EncodeJSON("interval", interval)
	if err != nil {
		t.Fatal(err)
	}
	interval.Raw = add("interval", "interval", encoded)
	e.Intervals = []c.Interval{interval}
	e.Views = []c.View{archiveView(t, stamp, sample.ExeSHA, add)}
	// Help is deliberately reached only through the binding, not Evidence.Raw.
	for ref := range archive {
		if ref != help {
			e.Raw = append(e.Raw, ref)
		}
	}
	return root, e, archive
}

func archiveView(t *testing.T, stamp c.Stamp, target c.Digest, add func(string, string, []byte) c.RawRef) c.View {
	t.Helper()
	key := upgradetx.Key{DomainID: stamp.Process.Key.Domain, Component: "agent", SID: stamp.Process.Key.SID, ConfiguredNID: stamp.Process.Key.NID}
	handle := upgradetx.Handle{Key: key, TxID: stamp.TxID, CutID: stamp.CutID, Generation: stamp.Generation, Revision: stamp.Revision}
	owner := upgradetx.Owner{BootID: stamp.Process.BootID, PID: stamp.Process.PID, StartTicks: stamp.Process.StartTicks, InstanceID: stamp.Process.InstanceID}
	upstream := upgradetx.Stamp{Attempt: handle, Producer: owner, Nonce: stamp.QueryNonce, Sequence: stamp.Sequence, At: stamp.AtBootNS, ScopeDigest: stamp.ScopeDigest}
	resource := upgradetx.Resource{Domain: "control", ID: "resource", Origin: "origin", Owner: owner, Generation: 2, State: "live", Custody: "owned"}
	census := upgradetx.Census{Stamp: upstream, Known: true, Complete: true, ExpectedDigest: "digest", Resources: []upgradetx.Resource{resource}}
	a := upgradetx.Attempt{Handle: handle, Direction: "forward", SourceSHA: strings.Repeat("a", 64), TargetSHA: hex.EncodeToString(target[:]), SourceGeneration: 1, Phase: "completed", CutStage: "released", Outcome: "success", Scope: upgradetx.Scope{ID: "scope", Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{"control"}, Expected: []upgradetx.Resource{resource}}}
	view := c.View{Stamp: stamp, Phase: "completed", Outcome: "success"}
	marshal := func(value any) []byte {
		t.Helper()
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for i, receipt := range []*upgradetx.Receipt{&a.Control, &a.Resources, &a.Released} {
		kind := []upgradetx.ReceiptKind{"control_ready", "resources_migrated", "old_generation_released"}[i]
		seal := upstream
		seal.Nonce = string(kind)
		seal.Attempt.Revision--
		seal.Sequence = 40 + uint64(i)
		seal.At = 100 + int64(i)
		historical := census
		historical.Stamp = seal
		*receipt = upgradetx.Receipt{Kind: kind, Stamp: seal, Known: true, Complete: true, RunningSHA: a.TargetSHA, Census: historical, EvidenceDigest: strings.Repeat("c", 64)}
		s := stamp
		s.QueryNonce, s.Revision, s.Sequence, s.AtBootNS = seal.Nonce, seal.Attempt.Revision, seal.Sequence, seal.At
		projection := []*c.Receipt{&view.Control, &view.Resources, &view.Released}[i]
		*projection = c.Receipt{Kind: string(kind), Stamp: s, Known: true, Complete: true, RunningSHA: target, ResourceIDs: []string{resource.ID}, Evidence: add(string(kind), "s8_receipt", marshal(receipt))}
	}
	raw := upgradetx.View{Historical: upgradetx.Record{Version: 1, Key: key, Revision: 20, Grant: upgradetx.Grant{Handle: handle, Mode: "hot"}, Forward: a}, Metadata: upgradetx.Metadata{TxID: stamp.TxID, ReportedRevision: 20, Window: "open"}, Observation: upgradetx.Observation{Nonce: stamp.QueryNonce, At: stamp.AtBootNS, Current: a, Census: census, OwnerKnown: true, RunningSHA: a.TargetSHA, EffectiveSHA: a.TargetSHA}}
	view.Raw = add("view", "s8_view", marshal(raw))
	view.Current = marshal(raw.Observation)
	view.Historical = marshal(struct {
		Record   upgradetx.Record   `json:"record"`
		Metadata upgradetx.Metadata `json:"metadata"`
	}{raw.Historical, raw.Metadata})
	return view
}
