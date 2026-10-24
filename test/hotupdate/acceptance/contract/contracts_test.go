// contracts_test.go pins the public shape independently of its disk schema.
// These are synthetic parser controls, never receipts for a product cut.
// origin: hot-update S10 plan §5.1 / §10.2 / P00
// gate-control: TestFullstackContractsFailClosed
package contract_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
	"github.com/LinZiyang666/tether/test/hotupdate/acceptance/contractstub"
)

type snapshotPort interface {
	Sample(context.Context, c.Key, string) (c.Sample, error)
	Drain(context.Context, c.Key, string) (c.RawRef, error)
	Close() error
	Join(context.Context) error
}
type driverPort interface {
	Stage(context.Context, c.Step) (c.View, error)
	Apply(context.Context, c.Step) (c.View, error)
	Query(context.Context, c.Key, string, string) (c.View, error)
}
type fixturePort interface {
	Start(context.Context, c.Profile) (c.RawRef, error)
	Sample(context.Context, string) (c.Evidence, error)
	Drain(context.Context, string) (c.RawRef, error)
	Stop(context.Context) error
	Join(context.Context) error
}

var (
	_ c.Snapshotter    = snapshotPort(nil)
	_ snapshotPort     = c.Snapshotter(nil)
	_ c.Driver         = driverPort(nil)
	_ driverPort       = c.Driver(nil)
	_ c.Fixture        = fixturePort(nil)
	_ fixturePort      = c.Fixture(nil)
	_ c.PlanFunc       = (func(c.Profile) ([]c.Step, error))(nil)
	_ c.JudgeFunc      = (func(c.Evidence) (c.Result, error))(nil)
	_ c.QuantilesFunc  = (func([]c.Attempt, []c.Slot) (c.Distribution, error))(nil)
	_ c.ReaderFactory  = (func(c.Profile) (c.Snapshotter, error))(nil)
	_ c.DriverFactory  = (func(c.Profile) (c.Driver, error))(nil)
	_ c.FixtureFactory = (func(c.Profile) (c.Fixture, error))(nil)
)

// The independent manifest detects a DTO and schema losing the same member.
// Reflection checks field types as well as count; all three preserve meaning.
var frozenDTOs = []struct {
	value any
	shape string
}{
	{c.Key{}, "Role:string Host:string Unit:string SID:string NID:string Domain:string UID:uint32"},
	{c.Process{}, "Key:Key BootID:string PIDNS:string InstanceID:string PID:int StartTicks:uint64"},
	{c.Stamp{}, "Process:Process TxID:string CutID:string Direction:string QueryNonce:string ScopeDigest:string Generation:uint64 Revision:uint64 Sequence:uint64 AtBootNS:int64"},
	{c.Artifact{}, "ID:string Role:string Release:string SourceCommit:string Toolchain:string ImageID:string Tree:Digest Dirty:Digest Flags:Digest Archive:Digest Binary:Digest Vendor:Digest CopyManifest:Digest Epoch:uint64 ReadEpochs:[]uint64 Domains:[]string"},
	{c.Step{}, "ID:string ParentID:string Action:string Direction:string Cohort:string Leg:string Target:Key From:string To:string DependsOn:[]string DeadlineNS:int64"},
	{c.Profile{}, "ID:string Topology:string Regime:string Tier:string EvidenceRoot:string CommandBindings:RawRef ProbeRate:int StartNS:int64 EndNS:int64 BudgetDigest:Digest WorkloadDigest:Digest Required:[]Key Steps:[]Step Artifacts:[]Artifact"},
	{c.RawRef{}, "Path:string SHA:Digest Bytes:int64 Schema:string"},
	{c.Receipt{}, "Kind:string Stamp:Stamp Known:bool Complete:bool RunningSHA:Digest ResourceIDs:[]string Evidence:RawRef"},
	{c.View{}, "Stamp:Stamp Historical:json.RawMessage Current:json.RawMessage Phase:string Outcome:string Code:string Detail:string Deferred:[]string Control:Receipt Resources:Receipt Released:Receipt Raw:RawRef"},
	{c.Resource{}, "ID:string Kind:string Domain:string OFD:string State:string Owner:Stamp FD:int Flags:uint64 Bytes:int64 ReleaseAfterNS:int64"},
	{c.Sample{}, "Stamp:Stamp ExeSHA:Digest Goroutines:int64 FDs:int64 Resources:[]Resource Consumers:[]string Temps:[]string Locks:[]string OldImages:[]string Complete:bool Raw:RawRef"},
	{c.Slot{}, "ID:string Class:string Target:Key ScheduledNS:int64 DeadlineNS:int64"},
	{c.Attempt{}, "SlotID:string CallID:string Phase:string Index:uint64 StartedNS:int64 FinishedNS:int64 Sent:bool Completed:bool RC:int Code:string Raw:RawRef"},
	{c.Interval{}, "Stamp:Stamp ResourceID:string Event:string AtNS:int64 Raw:RawRef"},
	{c.Result{}, "Verdict:string RC:int Failures:[]string Missing:[]string Inputs:[]RawRef"},
	{c.Distribution{}, "Count:uint64 Failed:uint64 Censored:uint64 Missing:uint64 MinNS:int64 P50NS:int64 P95NS:int64 P99NS:int64 MaxNS:int64"},
	{c.Evidence{}, "Profile:Profile Views:[]View Samples:[]Sample Slots:[]Slot Attempts:[]Attempt Intervals:[]Interval Raw:[]RawRef"},
}

// origin: hot-update S10 plan §5.1 / §10.2 / P00
func TestFullstackContractsFailClosed(t *testing.T) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(c.Schema(), &root); err != nil {
		t.Fatal(err)
	}
	var defs map[string]json.RawMessage
	if err := json.Unmarshal(root["$defs"], &defs); err != nil {
		t.Fatal(err)
	}
	t.Run("frozen_shapes", func(t *testing.T) { checkShapes(t, defs) })
	t.Run("failure_stubs", checkStubs)
	t.Run("closed_vocabulary", func(t *testing.T) { checkVocabulary(t, defs) })
	t.Run("strict_schema", func(t *testing.T) { checkSchema(t, defs) })
	t.Run("codec", func(t *testing.T) { checkCodec(t, defs) })
	t.Run("source_closure", checkSourceClosure)
	t.Logf("contract schema SHA256=%x", sha256.Sum256(c.Schema()))
}

func checkShapes(t *testing.T, defs map[string]json.RawMessage) {
	t.Helper()
	if reflect.TypeFor[c.Digest]().Kind() != reflect.Array || reflect.TypeFor[c.Digest]().Len() != 32 || reflect.TypeFor[c.Digest]().Elem().Kind() != reflect.Uint8 {
		t.Fatal("Digest must remain [32]byte")
	}
	seen := map[string]bool{}
	for _, entry := range frozenDTOs {
		typ := reflect.TypeOf(entry.value)
		fields := make([]string, 0, typ.NumField())
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			fields = append(fields, f.Name+":"+strings.ReplaceAll(f.Type.String(), "contract.", ""))
		}
		if strings.Join(fields, " ") != entry.shape {
			t.Errorf("frozen DTO %s changed: %s", typ.Name(), strings.Join(fields, " "))
		}
		for name, raw := range defs {
			s := readNode(t, raw)
			if s.GoType != typ.Name() {
				continue
			}
			seen[typ.Name()] = true
			if len(s.Properties) != typ.NumField() || len(s.Required) != typ.NumField() {
				t.Errorf("schema %s lost DTO fields", name)
			}
			mapped := map[string]bool{}
			for wire, p := range s.Properties {
				property := readNode(t, p)
				_, ok := typ.FieldByName(property.GoName)
				if !ok || mapped[property.GoName] || !slices.Contains(s.Required, wire) || strings.ReplaceAll(wire, "_", "") != strings.ToLower(property.GoName) {
					t.Errorf("invalid field mapping %s.%s", name, wire)
				}
				mapped[property.GoName] = true
			}
		}
		if !seen[typ.Name()] {
			t.Errorf("missing DTO schema %s", typ.Name())
		}
	}
	for name, raw := range defs {
		s := readNode(t, raw)
		if s.GoType != "" && !seen[s.GoType] {
			t.Errorf("unfrozen DTO %s", name)
		}
	}
}

func checkStubs(t *testing.T) {
	t.Helper()
	if c.ErrContractStub.Error() != "fullstack contract not wired" {
		t.Fatal("sentinel text changed")
	}
	profile := c.Profile{ID: "untouched", Required: []c.Key{{Role: "agent", SID: "original"}}}
	cases := []struct {
		name, owner string
		call        func() (any, error)
	}{
		{"NewSnapshotter", "P04", func() (any, error) { return contractstub.NewSnapshotter(profile) }},
		{"NewDriver", "P18", func() (any, error) { return contractstub.NewDriver(profile) }},
		{"NewFixture", "P19", func() (any, error) { return contractstub.NewFixture(profile) }},
		{"Plan", "P01", func() (any, error) { return contractstub.Plan(profile) }},
		{"Judge", "P01", func() (any, error) { return contractstub.Judge(c.Evidence{Profile: profile}) }},
		{"JudgeResources", "P03", func() (any, error) { return contractstub.JudgeResources(c.Evidence{Profile: profile}) }},
		{"Quantiles", "P02", func() (any, error) {
			return contractstub.Quantiles([]c.Attempt{{SlotID: "original"}}, []c.Slot{{ID: "original"}})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			for round := 0; round < 5; round++ {
				v, err := tc.call()
				if !errors.Is(err, c.ErrContractStub) || err.Error() != "not implemented (S10 "+tc.owner+")" {
					t.Fatalf("stub owner failure missing: %v", err)
				}
				if v != nil && !reflect.ValueOf(v).IsZero() {
					t.Fatalf("stub fabricated evidence: %T", v)
				}
			}
		})
	}
	if profile.Required[0].SID != "original" {
		t.Fatal("stub changed caller input")
	}
}

type node struct {
	OneOf      []json.RawMessage          `json:"oneOf"`
	Ref        string                     `json:"$ref"`
	Type       string                     `json:"type"`
	Enum       []json.RawMessage          `json:"enum"`
	Const      json.RawMessage            `json:"const"`
	Properties map[string]json.RawMessage `json:"properties"`
	Required   []string                   `json:"required"`
	Additional *bool                      `json:"additionalProperties"`
	Items      json.RawMessage            `json:"items"`
	MinItems   int                        `json:"minItems"`
	MinLength  int                        `json:"minLength"`
	Minimum    json.Number                `json:"minimum"`
	Pattern    string                     `json:"pattern"`
	GoType     string                     `json:"x-go-type"`
	GoName     string                     `json:"x-go-name"`
}

func readNode(t *testing.T, raw []byte) node {
	t.Helper()
	var n node
	if err := json.Unmarshal(raw, &n); err != nil {
		t.Fatal(err)
	}
	return n
}
func marshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func checkVocabulary(t *testing.T, defs map[string]json.RawMessage) {
	t.Helper()
	frozen := map[string][]string{
		"role":     {"agent", "broker", "nats", "caddy", "ctl", "fixture", "support"},
		"topology": {"single", "quorum"}, "regime": {"calibration", "quiet", "default", "live-grow", "control"}, "tier": {"hermetic", "deploy", "source_gate"},
		"verdict":      {"GREEN", "ASSERT-FAIL", "SETUP-RED", "PRODUCT-RED", "INCOMPLETE"},
		"s8_phase":     {"prepared", "staged", "ready", "freezing", "activating", "serving_new", "migrating", "completed", "deferred", "rejected", "rollback_pending", "rollback_blocked", "rolled_back", "recovery_required", "cancelled", "abandoned"},
		"s8_outcome":   {"pending", "success", "deferred", "rejected", "rolled_back", "legacy_interrupting", "unknown", "cancelled"},
		"s8_direction": {"forward", "reverse"}, "s8_cut_stage": {"none", "pristine", "partial_active", "active", "retiring", "released"},
		"s7_status_state": {"unknown", "live", "terminal"}, "s7_phase": {"open", "prepared", "sending", "receiving", "commit_reserved", "decided", "terminal_pending", "history_confirmed", "cleanup_pending", "settled"},
		"s7_primitive_outcome": {"not_entered", "inflight", "landed"}, "agentboot_phase": {"", "held", "refused", "imported", "active", "ready", "lost"},
		"s9_observation_kind": {"artifact_verified", "asset_sealed", "successor_installed", "attachment_changed", "auth_usable", "business_ready", "signal_applied", "old_exit", "storage_finalized", "startup_released", "return_verified", "owner_retired", "completed", "halted"},
	}
	for name, want := range frozen {
		var got []string
		for _, v := range readNode(t, defs[name]).Enum {
			var text string
			if err := json.Unmarshal(v, &text); err != nil {
				t.Fatal(err)
			}
			got = append(got, text)
		}
		if !slices.Equal(want, got) {
			t.Errorf("vocabulary drift %s: %v", name, got)
		}
	}
	for name, raw := range defs {
		s := readNode(t, raw)
		if s.Enum == nil {
			continue
		}
		if len(s.Enum) == 0 {
			t.Errorf("empty vocabulary %s", name)
		}
		for _, v := range s.Enum {
			if err := c.ValidateJSON(name, v); err != nil {
				t.Errorf("known %s rejected: %v", name, err)
			}
		}
		for _, v := range []string{`"future"`, `"AGENT"`, `null`, `false`, `-1`, `0`, `12`, `75`} {
			if slices.ContainsFunc(s.Enum, func(b json.RawMessage) bool { return string(b) == v }) {
				continue
			}
			if err := c.ValidateJSON(name, []byte(v)); err == nil {
				t.Errorf("unknown vocabulary %s accepted %s", name, v)
			}
		}
	}
	for name, count := range map[string]int{"s9_phase": 11, "s9_action": 4} {
		s := readNode(t, defs[name])
		if len(s.Enum) != count {
			t.Fatalf("%s count changed", name)
		}
		for i, raw := range s.Enum {
			if string(raw) != strconv.Itoa(i+1) {
				t.Fatalf("%s is not exact numeric vocabulary", name)
			}
		}
	}
	upstream, err := parser.ParseFile(token.NewFileSet(), "../../../../internal/infracontract/types.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var observationKinds []string
	for _, decl := range upstream.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Name.Name != "ObservationKinds" {
			continue
		}
		ast.Inspect(fn.Body, func(n ast.Node) bool {
			if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				observationKinds = append(observationKinds, value)
			}
			return true
		})
	}
	if !slices.Equal(observationKinds, frozen["s9_observation_kind"]) {
		t.Fatal("S9 observation kind producer drift")
	}
	checkUpstreamEnums(t, defs)
}

func checkUpstreamEnums(t *testing.T, defs map[string]json.RawMessage) {
	t.Helper()
	// S7's StatusState literals live in the NATS-free value package; xfercontract
	// re-exports them by alias (origin: hot-update S7 plan §11.1 / P00 value split).
	for _, spec := range []struct{ path, typ, name string }{
		{"internal/upgradetx/contracts.go", "Phase", "s8_phase"}, {"internal/upgradetx/contracts.go", "Outcome", "s8_outcome"}, {"internal/upgradetx/contracts.go", "Direction", "s8_direction"}, {"internal/upgradetx/contracts.go", "CutStage", "s8_cut_stage"},
		{"internal/xfercontract/xfervalue/contracts.go", "StatusState", "s7_status_state"}, {"internal/xfercontract/xferport/ports.go", "Phase", "s7_phase"}, {"internal/xfercontract/xferport/ports.go", "PrimitiveOutcome", "s7_primitive_outcome"},
		{"internal/agentboot/image.go", "Phase", "agentboot_phase"}, {"internal/infracontract/types.go", "Phase", "s9_phase"}, {"internal/infracontract/types.go", "Action", "s9_action"},
	} {
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Join("../../../..", spec.path), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		ast.Inspect(f, func(n ast.Node) bool {
			v, ok := n.(*ast.ValueSpec)
			if !ok {
				return true
			}
			typ, ok := v.Type.(*ast.Ident)
			if !ok || typ.Name != spec.typ {
				return true
			}
			for _, value := range v.Values {
				lit, ok := value.(*ast.BasicLit)
				if !ok {
					t.Fatal("upstream enum is no longer literal")
				}
				got = append(got, lit.Value)
			}
			return true
		})
		var want []string
		for _, v := range readNode(t, defs[spec.name]).Enum {
			want = append(want, string(v))
		}
		if !slices.Equal(want, got) {
			t.Errorf("upstream enum union differs %s: %v", spec.name, got)
		}
	}
}

// Generate nonempty structural controls. Frozen field/vocabulary checks above
// remain independent so the schema cannot shrink both expected and actual data.
func example(t *testing.T, defs map[string]json.RawMessage, raw []byte) any {
	return exampleValue(t, defs, raw, "witness")
}
func exampleValue(t *testing.T, defs map[string]json.RawMessage, raw []byte, label string) any {
	t.Helper()
	s := readNode(t, raw)
	if len(s.OneOf) != 0 {
		return exampleValue(t, defs, s.OneOf[0], label)
	}
	if s.Ref != "" {
		return exampleValue(t, defs, defs[strings.TrimPrefix(s.Ref, "#/$defs/")], label)
	}
	if s.Const != nil {
		var v any
		if err := json.Unmarshal(s.Const, &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	if len(s.Enum) > 0 {
		var v any
		if err := json.Unmarshal(s.Enum[0], &v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	switch s.Type {
	case "object":
		m := map[string]any{}
		for key, property := range s.Properties {
			m[key] = exampleValue(t, defs, property, label+"."+key)
		}
		return m
	case "array":
		return []any{exampleValue(t, defs, s.Items, label+".item")}
	case "string":
		if s.Pattern != "" {
			digest := sha256.Sum256([]byte(label))
			if s.Pattern == "^[0-9a-f]{24}$" {
				return hex.EncodeToString(digest[:12])
			}
			return hex.EncodeToString(digest[:])
		}
		return label
	case "integer":
		if s.Minimum == "1" {
			return 1
		}
		return 0
	case "boolean":
		return true
	default:
		t.Fatalf("no example for schema %s", raw)
		return nil
	}
}

func checkCodec(t *testing.T, defs map[string]json.RawMessage) {
	t.Helper()
	for _, entry := range frozenDTOs {
		typ := reflect.TypeOf(entry.value)
		for name, raw := range defs {
			if readNode(t, raw).GoType != typ.Name() {
				continue
			}
			fixture := example(t, defs, raw)
			if name == "result" {
				fixture.(map[string]any)["rc"] = 0
			}
			data := marshal(t, fixture)
			dst := reflect.New(typ)
			if err := c.DecodeJSON(name, data, dst.Interface()); err != nil {
				t.Fatalf("decode %s: %v", name, err)
			}
			encoded, err := c.EncodeJSON(name, dst.Elem().Interface())
			if err != nil {
				t.Fatalf("encode %s: %v", name, err)
			}
			if !bytes.Equal(encoded, data) {
				t.Errorf("%s codec lost fields\ngot %s\nwant %s", name, encoded, data)
			}
			before := reflect.New(typ)
			before.Elem().Set(dst.Elem())
			if c.DecodeJSON(name, []byte(`{}`), dst.Interface()) == nil || !reflect.DeepEqual(before.Interface(), dst.Interface()) {
				t.Errorf("%s failed decode modified caller", name)
			}
		}
	}
	stamp := c.Stamp{Process: c.Process{Key: c.Key{Role: "agent", Host: "host", Unit: "unit", Domain: "domain"}, BootID: "boot", PIDNS: "ns", InstanceID: "instance", PID: 1, StartTicks: ^uint64(0)}, Direction: "forward", Sequence: ^uint64(0)}
	b, err := c.EncodeJSON("stamp", stamp)
	if err != nil {
		t.Fatal(err)
	}
	var got c.Stamp
	if err := c.DecodeJSON("stamp", b, &got); err != nil || got != stamp {
		t.Fatalf("uint64 precision lost: %v", err)
	}
	if c.DecodeJSON("stamp", b, nil) == nil {
		t.Fatal("nil destination accepted")
	}
	var wrong c.Key
	if c.DecodeJSON("stamp", b, &wrong) == nil {
		t.Fatal("wrong destination accepted")
	}
	if _, err := c.EncodeJSON("stamp", c.Key{}); err == nil {
		t.Fatal("wrong source accepted")
	}
	original := c.Schema()
	copy := c.Schema()
	copy[0] = '!'
	if !bytes.Equal(original, c.Schema()) {
		t.Fatal("schema storage is mutable")
	}
}

func checkSourceClosure(t *testing.T) {
	t.Helper()
	// A pure contract/stub has no runtime lifecycle to fake with empty leak
	// loops. Inspect the actual files as well as invoking every failure path.
	for _, path := range []string{"types.go", "ports.go", "vocabulary.go", "../contractstub/stub.go"} {
		f, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, imp := range f.Imports {
			p, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			allowedStub := path == "../contractstub/stub.go" && p == "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
			if strings.Contains(strings.Split(p, "/")[0], ".") && !allowedStub {
				t.Errorf("contract imported implementation %s", p)
			}
		}
		if path != "../contractstub/stub.go" {
			continue
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if len(fn.Body.List) != 1 {
				t.Errorf("stub %s is not inert", fn.Name)
			}
			if _, ok := fn.Body.List[0].(*ast.ReturnStmt); !ok {
				t.Errorf("stub %s has side effects", fn.Name)
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				id, ok := call.Fun.(*ast.Ident)
				if !ok || (id.Name != "failure" && id.Name != "string") {
					t.Errorf("stub %s calls a runtime dependency", fn.Name)
				}
				return true
			})
		}
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(c.Schema(), &root); err != nil {
		t.Fatal(err)
	}
	var defs map[string]json.RawMessage
	if err := json.Unmarshal(root["$defs"], &defs); err != nil {
		t.Fatal(err)
	}
	// Cancellation is additive native evidence declared beside the base DTOs;
	// changes to its nested fields must be checked against the same disk schema.
	// origin: S10 internal review MAKETEST-GATES F6 / S8 plan §4.2
	for _, path := range []string{"../../../../internal/upgradetx/contracts.go", "../../../../internal/upgradetx/cancellation.go"} {
		upstream, err := parser.ParseFile(token.NewFileSet(), path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(upstream, func(n ast.Node) bool {
			typ, ok := n.(*ast.TypeSpec)
			if !ok {
				return true
			}
			body, ok := typ.Type.(*ast.StructType)
			if !ok {
				return true
			}
			name := "s8_" + strings.ToLower(typ.Name.Name)
			if name == "s8_attempt" || name == "s8_receipt" {
				name += "_present"
			}
			raw, exists := defs[name]
			if !exists {
				return true
			}
			spec := readNode(t, raw)
			if len(body.Fields.List) != len(spec.Properties) {
				t.Errorf("raw upstream DTO field count drift: %s", name)
			}
			for _, field := range body.Fields.List {
				if field.Tag == nil {
					t.Fatalf("upstream DTO has no disk tag: %s", name)
				}
				tag, err := strconv.Unquote(field.Tag.Value)
				if err != nil {
					t.Fatal(err)
				}
				wire := strings.Split(reflect.StructTag(tag).Get("json"), ",")[0]
				if _, ok := spec.Properties[wire]; !ok {
					t.Errorf("raw upstream field missing: %s.%s", name, wire)
				}
			}
			return true
		})
	}
	// The disk file, not a detached hand-written copy, is the embedded boundary.
	raw, err := os.ReadFile("schema.json")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(raw, c.Schema()) {
		t.Fatal("embedded and on-disk schemas differ")
	}
}
