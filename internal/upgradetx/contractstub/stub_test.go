package contractstub_test

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradeauthority"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	s "github.com/LinZiyang666/tether/internal/upgradetx/contractstub"
	_ "github.com/LinZiyang666/tether/test/hotupdate/txcontracts" // Exact signature composition must compile with these tests.
)

const module = "github.com/LinZiyang666/tether/"

// Canonical AST hashes freeze types, field order, JSON tags, aliases, versions,
// enums and method sets. They do not freeze comments or gofmt whitespace. This
// is a P00-local contract assertion, not a replacement for P31's codec union.
var frozenContracts = map[string]string{
	"internal/upgradetx/contracts.go":         "a789ff46c3afc00773129b489ce87669a436991ab6a2af10b0a007d3fc337d49",
	"internal/upgradeport/contracts.go":       "c01098a9d901ff6f8404e28cea830bce3152e12cd4383f632f1bc65016fdacbb",
	"internal/upgradewire/contracts.go":       "fda4f83c47c3af496670f7603faf7ad5f2711b65318ecee0db51464b6b289c2e",
	"internal/upgradeartifact/contracts.go":   "388317b63699d8998408dd3d9da63668c6b061d25b8e4557fcd9409c2a22c5e5",
	"internal/upgradelock/contracts.go":       "7f51b4a0e11155cc2de75cb08da2dcc7595fb4fb59aeeeff244f0fac13bb1805",
	"internal/upgradeauthority/contracts.go":  "eee5eb6f072819000417b6665495b686ec314928fefe1cec53cee82628487691",
	"internal/upgradeobserve/contracts.go":    "763d09da563dd65a4510f2e3bf25ceb59c4359668e1284668f68a5e7fc6cfc66",
	"internal/upgradecut/contracts.go":        "82bcf26c72b8495871ba5c00fff835d44817f53ac829049a160101bff9af605a",
	"test/hotupdate/txcontracts/contracts.go": "f669d3258da682328db766e2e4895e531a8025290b9250ea395ecc145c40aa47",
}

func contractRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs("../../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatal(err)
	}
	return root
}

func readSource(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(contractRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func contractDigest(raw []byte) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "contracts.go", raw, 0)
	if err != nil {
		return "", err
	}
	var normalized bytes.Buffer
	if err := printer.Fprint(&normalized, token.NewFileSet(), file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(normalized.Bytes())), nil
}

// origin: hot-update S8 plan §2 / §11.2 P00
// gate-control: TestUpgradeContractBoundaryControls
func TestUpgradeContractSignatures(t *testing.T) {
	if len(frozenContracts) != 9 {
		t.Fatal("contract file census changed")
	}
	for path, want := range frozenContracts {
		t.Run(path, func(t *testing.T) {
			got, err := contractDigest(readSource(t, path))
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("contract drift: %s: got %s, want %s", path, got, want)
			}
		})
	}
}

type stubCase struct {
	name  string
	value any
	owner string
}

func stubFunctions() []stubCase {
	return []stubCase{
		{"ValidatePlan", s.ValidatePlan, "P01"}, {"Transition", s.Transition, "P01"}, {"Completed", s.Completed, "P01"},
		{"Acquire", s.Acquire, "P04"}, {"Adopt", s.Adopt, "P04"}, {"WithGuard", s.WithGuard, "P04"},
		{"Publish", s.Publish, "P05"}, {"Verify", s.Verify, "P05"}, {"Resolve", s.Resolve, "P05"},
		{"Encode", s.Encode, "P07"}, {"Decode", s.Decode, "P07"}, {"Take", s.Take, "P07"},
		{"Derive", s.Derive, "P08"}, {"Match", s.Match, "P08"}, {"ReadCreation", s.ReadCreation, "P08"},
		{"CreationPredicate", s.CreationPredicate, "P08"}, {"Run", s.Run, "P11"},
	}
}

func checkStubResult(out []reflect.Value, owner string) error {
	if len(out) == 0 || out[len(out)-1].Type() != reflect.TypeFor[error]() {
		return errors.New("stub has no error result")
	}
	err, _ := out[len(out)-1].Interface().(error)
	var refusal *upgradetx.Error
	want := "not implemented (S8 " + owner + ")"
	if !errors.As(err, &refusal) || refusal == nil || refusal.Code != "upgrade_not_wired" || refusal.Detail != want || refusal.Accepted {
		return fmt.Errorf("stub returned %v; want unaccepted upgrade_not_wired with detail %q", err, want)
	}
	if err.Error() != "upgrade_not_wired: "+want {
		return fmt.Errorf("stub error text lost its classification or owner: %v", err)
	}
	for _, value := range out[:len(out)-1] {
		if !value.IsZero() {
			return errors.New("stub fabricated ownership or success evidence")
		}
	}
	return nil
}

func callStub(t *testing.T, fn reflect.Value, owner string) {
	t.Helper()
	args := make([]reflect.Value, fn.Type().NumIn())
	for i := range args {
		args[i] = reflect.Zero(fn.Type().In(i))
	}
	if err := checkStubResult(fn.Call(args), owner); err != nil {
		t.Fatal(err)
	}
}

// Every error-bearing method is exercised, including cleanup and all methods
// of both constructed endpoints. There are no loops, processes or retained FDs
// in these stubs, so goroutine/FD leak exercise anchors are not applicable.
// origin: hot-update S8 plan §2.2 / §11.2 P00
// gate-control: TestUpgradeContractBoundaryControls
func TestUpgradeContractStubsRejectEveryOperation(t *testing.T) {
	functions := stubFunctions()
	if len(functions) != 17 {
		t.Fatal("stub function census changed")
	}
	for _, tc := range functions {
		t.Run(tc.name, func(t *testing.T) { callStub(t, reflect.ValueOf(tc.value), tc.owner) })
	}
	witnesses := []stubCase{
		{"Store", s.Store{}, "P02"}, {"Registry", s.Registry{}, "P03"},
		{"AgentEndpoint", s.AgentEndpoint{}, "P09"}, {"BrokerEndpoint", s.BrokerEndpoint{}, "P10"},
		{"Runtime", s.Runtime{}, "P17"},
		{"NewAgent", s.NewAgent(s.Store{}, s.Runtime{}, s.Registry{}), "P09"},
		{"NewBroker", s.NewBroker(s.Store{}, s.Runtime{}, s.Registry{}), "P10"},
	}
	methods := 0
	for _, tc := range witnesses {
		value := reflect.ValueOf(tc.value)
		if value.Kind() != reflect.Struct || value.Type().NumField() != 0 {
			t.Fatalf("%s retained state or returned no endpoint", tc.name)
		}
		for i := 0; i < value.NumMethod(); i++ {
			methods++
			t.Run(tc.name+"."+value.Type().Method(i).Name, func(t *testing.T) {
				callStub(t, value.Method(i), tc.owner)
			})
		}
	}
	if methods != 46 {
		t.Fatalf("stub method census = %d, want 46", methods)
	}
	lease := s.Lease{}
	callStub(t, reflect.ValueOf(lease.Close), "P04")
	if lease.FD() != ^uintptr(0) || lease.DomainID() != "" {
		t.Fatal("stub lease fabricated an FD or domain identity")
	}
}

type forbiddenReader struct{ t *testing.T }

func (r forbiddenReader) Read([]byte) (int, error) {
	r.t.Fatal("stub consumed an artifact reader")
	return 0, errors.New("unexpected read")
}

type forbiddenRows struct{ t *testing.T }

func (r forbiddenRows) QueryRow(string, ...any) *sql.Row {
	r.t.Fatal("stub queried a database")
	return nil
}

// Nonzero inputs attack side effects that nil-only method-set tests miss.
// Secrets use deliberately artificial text, never live environment/state.
// origin: hot-update S8 plan §2.2 / §6.2 / P00
func TestUpgradeContractStubsPreserveCallerCustody(t *testing.T) {
	reader := forbiddenReader{t}
	if _, err := s.Publish(t.Context(), upgradetx.Grant{}, reader, reader); err == nil {
		t.Fatal("Publish reported success")
	}
	if err := s.WithGuard(upgradetx.Key{}, func() error { t.Fatal("stub invoked guarded work"); return nil }); err == nil {
		t.Fatal("WithGuard reported success")
	}
	if _, err := s.Take(upgradecut.State{LockFD: 42}, func(uint32) (uintptr, error) {
		t.Fatal("stub took caller FD custody")
		return 0, nil
	}); err == nil {
		t.Fatal("Take reported success")
	}
	if _, err := s.ReadCreation(forbiddenRows{t}, upgradeauthority.Guard{SID: "fixture"}); err == nil {
		t.Fatal("ReadCreation reported success")
	}
	secret := "artificial-sensitive-creation-material"
	_, deriveErr := s.Derive("fixture", "owner", 1, []byte(secret))
	_, _, predicateErr := s.CreationPredicate(upgradeauthority.Creation{EncodedPINHash: secret})
	for _, err := range []error{deriveErr, predicateErr} {
		if err == nil || strings.Contains(err.Error(), secret) {
			t.Fatalf("stub accepted or disclosed creation material: %v", err)
		}
	}
	encoded, err := json.Marshal(upgradeauthority.Creation{EncodedPINHash: secret})
	if err != nil || string(encoded) != "{}" {
		t.Fatal("local SQL creation identity entered JSON")
	}
}

// These are the direct contract edges, not a general leaf implementation
// allowlist. stdlib imports are allowed. P13's explicit proto conversion is the
// one future lower-level edge; no production path may import the compile stub.
var contractImports = map[string][]string{
	"internal/upgradetx/contracts.go":         nil,
	"internal/upgradeauthority/contracts.go":  nil,
	"internal/upgradelock/contracts.go":       {"upgradetx"},
	"internal/upgradeobserve/contracts.go":    {"upgradetx"},
	"internal/upgradeartifact/contracts.go":   {"upgradetx"},
	"internal/upgradecut/contracts.go":        {"upgradetx", "upgradelock"},
	"internal/upgradeport/contracts.go":       {"upgradetx", "upgradeartifact", "upgradeobserve"},
	"internal/upgradewire/contracts.go":       {"upgradetx", "upgradeartifact", "proto"},
	"internal/upgradetx/contractstub/stub.go": {"upgradetx", "upgradeartifact", "upgradeauthority", "upgradecut", "upgradelock", "upgradeobserve", "upgradeport"},
}

func checkImports(path string, raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), path, raw, parser.ImportsOnly)
	if err != nil {
		return err
	}
	allowed, restricted := contractImports[path]
	for _, spec := range file.Imports {
		name, err := strconv.Unquote(spec.Path.Value)
		if err != nil {
			return err
		}
		if name == module+"internal/upgradetx/contractstub" || name == module+"test/hotupdate/txcontracts" {
			return errors.New("production imports a compile-only witness")
		}
		first, _, _ := strings.Cut(name, "/")
		if !restricted || !strings.Contains(first, ".") {
			continue
		}
		permitted := false
		for _, leaf := range allowed {
			permitted = permitted || name == module+"internal/"+leaf
		}
		if !permitted {
			return fmt.Errorf("forbidden contract import: %s", name)
		}
	}
	return nil
}

// Runtime rejection alone cannot prove a stub did not perform work before
// rejecting. Restrict it to return expressions and its pure error constructor.
func checkStubPurity(raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "stub.go", raw, 0)
	if err != nil {
		return err
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			if fn.Name.Name == "init" || fn.Body == nil || len(fn.Body.List) != 1 {
				return errors.New("stub contains initialization or work")
			}
			if _, ok := fn.Body.List[0].(*ast.ReturnStmt); !ok {
				return errors.New("stub is not a single return")
			}
		}
	}
	var impure bool
	ast.Inspect(file, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			id, ok := call.Fun.(*ast.Ident)
			if !ok || (id.Name != "notImplemented" && id.Name != "uintptr") {
				impure = true
			}
		}
		return true
	})
	if impure {
		return errors.New("stub calls a callback or side effect")
	}
	return nil
}

// Scanning source covers all build tags, including imports hidden from today's
// GOOS. The reverse check prevents production from linking stubs via the test
// composition package. No nested go command or subprocess is needed here.
// origin: hot-update S8 plan §2.1 / §12.2 / P00
// gate-control: TestUpgradeContractBoundaryControls
func TestUpgradeContractImportBoundary(t *testing.T) {
	root := contractRoot(t)
	seen := make(map[string]bool)
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			seen[rel] = true
			if err := checkImports(rel, raw); err != nil {
				return fmt.Errorf("%s: %w", rel, err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for path := range contractImports {
		if !seen[path] {
			t.Errorf("contract import scan missed %s", path)
		}
	}
	if err := checkStubPurity(readSource(t, "internal/upgradetx/contractstub/stub.go")); err != nil {
		t.Fatal(err)
	}
}

// Controls exercise the predicates used above: parseable signature/tag/enum
// drift, nil-error success, false evidence, forbidden imports and hidden work.
// Each negative keeps valid Go syntax so parser failure cannot sign the oracle.
// origin: hot-update S8 plan §2 / §11.2 P00
func TestUpgradeContractBoundaryControls(t *testing.T) {
	raw := readSource(t, "internal/upgradetx/contracts.go")
	baseline, err := contractDigest(raw)
	if err != nil {
		t.Fatal(err)
	}
	commented, err := contractDigest(append([]byte("// additional explanation\n"), raw...))
	if err != nil || commented != baseline {
		t.Fatal("contract hash froze a comment")
	}
	for _, change := range [][2]string{
		{"txID string", "txID uint64"}, {`json:"tx_id"`, `json:"transaction_id"`},
		{`= "completed"`, `= "accepted"`}, {"type Handle struct {", "type Handle struct { Extra bool;"},
	} {
		mutant := strings.Replace(string(raw), change[0], change[1], 1)
		if mutant == string(raw) {
			t.Fatalf("empty contract control: %s", change[0])
		}
		digest, err := contractDigest([]byte(mutant))
		if err != nil || digest == baseline {
			t.Fatalf("contract control did not reject valid drift: %v", err)
		}
	}
	for _, value := range []error{
		nil, errors.New("not implemented (S8 P01)"),
		&upgradetx.Error{Code: "upgrade_not_wired", Detail: "not implemented (S8 P01)", Accepted: true},
		&upgradetx.Error{Code: "upgrade_not_wired", Detail: "not implemented (S8 P02)"},
		&upgradetx.Error{Code: "success", Detail: "not implemented (S8 P01)"},
	} {
		if err := checkStubResult([]reflect.Value{reflect.ValueOf(&value).Elem()}, "P01"); err == nil {
			t.Fatal("stub control accepted success, an untyped error or the wrong owner")
		}
	}
	var good error = &upgradetx.Error{Code: "upgrade_not_wired", Detail: "not implemented (S8 P01)"}
	if err := checkStubResult([]reflect.Value{reflect.ValueOf(&good).Elem()}, "P01"); err != nil {
		t.Fatal(err)
	}
	if err := checkStubResult([]reflect.Value{reflect.ValueOf(upgradetx.Receipt{Known: true}), reflect.ValueOf(&good).Elem()}, "P01"); err == nil {
		t.Fatal("stub control accepted fabricated evidence with an error")
	}
	for _, path := range []string{"internal/upgradetx/contracts.go", "internal/upgradeport/contracts.go"} {
		for _, dependency := range []string{"internal/broker", "internal/proto", "internal/upgradetx/contractstub", "test/hotupdate/txcontracts"} {
			source := []byte("package p; import alias " + strconv.Quote(module+dependency))
			if err := checkImports(path, source); err == nil {
				t.Fatalf("import control accepted %s -> %s", path, dependency)
			}
		}
	}
	if err := checkImports("internal/upgradetx/contracts.go", []byte(`package p; import "strings"`)); err != nil {
		t.Fatal(err)
	}
	for _, source := range []string{
		"package p; func Stop() error { go work(); return nil }",
		"package p; func Stop() error { return work() }",
		"package p; var hidden = work()",
		"package p; func init() { return }",
	} {
		if err := checkStubPurity([]byte(source)); err == nil {
			t.Fatal("purity control accepted work")
		}
	}
	if err := checkStubPurity(readSource(t, "internal/upgradetx/contractstub/stub.go")); err != nil {
		t.Fatal(err)
	}
}
