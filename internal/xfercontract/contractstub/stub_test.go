package contractstub_test

import (
	"bytes"
	"crypto/sha256"
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

	"github.com/LinZiyang666/tether/internal/jsstream"
	"github.com/LinZiyang666/tether/internal/transferpath"
	"github.com/LinZiyang666/tether/internal/xferauth"
	"github.com/LinZiyang666/tether/internal/xferbucket"
	"github.com/LinZiyang666/tether/internal/xferclient"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	s "github.com/LinZiyang666/tether/internal/xfercontract/contractstub"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
	"github.com/LinZiyang666/tether/internal/xfercourier"
	"github.com/LinZiyang666/tether/internal/xferfinal"
	"github.com/LinZiyang666/tether/internal/xfergc"
	"github.com/LinZiyang666/tether/internal/xferhistory"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/internal/xferledger"
	"github.com/LinZiyang666/tether/internal/xferreceive"
	"github.com/LinZiyang666/tether/internal/xfersend"
	"github.com/LinZiyang666/tether/internal/xfertrack"
	"github.com/LinZiyang666/tether/internal/xferworker"
)

// These assignments compile the real consumer packages against the same stub
// signatures. In particular aliases must not become separate named interfaces
// whose return types make actor factories unassignable across the leaf boundary.
// origin: hot-update S7 plan §8.1 / P00
var (
	_ jsstream.WriterFactory     = s.NewTransferWriter
	_ jsstream.ReaderFactory     = s.NewTransferReader
	_ jsstream.LegacyPutFactory  = s.PutLegacyOnce
	_ transferpath.ImportFactory = s.ImportAtomic
	_ x.AtomicImportFactory      = transferpath.ImportFactory(nil)
	_ transferpath.ImportFactory = x.AtomicImportFactory(nil)
	_ xferworker.Factory         = s.NewWorker
	_ xferworker.ImportFactory   = s.ImportWorker
	_ xfersend.Factory           = s.NewSender
	_ xferreceive.Factory        = s.NewReceiver
	_ xfercourier.Factory        = s.NewCourier
	_ xfertrack.Factory          = s.NewTracker
	_ xfertrack.ImportFactory    = s.ImportTracker
	_ xferledger.Factory         = s.NewLedger
	_ xferhistory.Factory        = s.NewHistory
	_ xferfinal.Factory          = s.NewFinalizer
	_ xfergc.Factory             = s.NewCollector
	_ xferbucket.Factory         = s.NewBucket
	_ xfercontrol.Factory        = s.NewController
	_ xferclient.Factory         = s.NewClient
	_ xferclient.LegacyFactory   = s.NewLegacy
	_ xferauth.Factory           = s.NewAuth
	_ xferimage.WorkerEncoder    = s.EncodeWorker
	_ xferimage.WorkerDecoder    = s.DecodeWorker
	_ xferimage.TrackerEncoder   = s.EncodeTracker
	_ xferimage.TrackerDecoder   = s.DecodeTracker
)

// No inventory updater is hidden here. These hashes freeze the AST, including
// field order, tags, enums, alias identity and every port/factory signature.
// Comments and formatting remain editable without changing the contract.
// ports.go carries the INTEG-R legacy-orphan amendment; carrier_test.go proves
// that stripping exactly that amendment yields the original P00 digest.
// The NATS-free value split moved contracts.go (whole) and the custody part of
// state.go into xfervalue; value_alias_test.go proves both moves reproduce the
// original P00 digests (9e8319…, c035b9…) and that every value is re-exported.
var frozenContracts = map[string]string{
	"internal/xfercontract/contracts.go": "3029aa366a30da25c90eac259ec2a5f6407431de7373d0b3435e77f779b50ceb",
	// origin: hot-update S7 plan §8.3 / FIX-INLINE; the sealed sender retains its inline payload.
	"internal/xfercontract/state.go":               "4397f46ebc8459ad02a570b22fd3880b118afa4f4f28282420d2fedfe0620822",
	"internal/xfercontract/xfervalue/contracts.go": "3b2aa911addbed8865c8ddb9db3e9d95366011e9b6c9a5d3875f8f11f5ce81f5",
	"internal/xfercontract/xfervalue/state.go":     "17405737034ee62a655ced525e7900f860a6558e1ad2f82b493750e91a8cb5a5",
	"internal/xfercontract/xferport/ports.go":      "56d3a3af62f3a43583a7ee98024b488443e386b069b8f25b9da4d80547085880",
	"internal/xfercontract/ports.go":               "eb000d734be46bcc22933180a8f4b3194c7669ccde7cab59b6a4951f83013f58",
	"internal/jsstream/object_contracts.go":        "fa0446884d202699cff372c435af4cf8aaba4549145d552488373aee788c1ec1",
	"internal/transferpath/transfer_contracts.go":  "b80c5713bcb76eeb0e9353c7764a90ce0909038e957a73fa180c000a2c7398cd",
	"internal/xferworker/owner.go":                 "32733dd0b737ec8e51b7227e5bade420dcf127d63f1037e8f89578766aa96bda",
	"internal/xfersend/contracts.go":               "77787fc83ac3df3be0b9f80de7c024e05b52b182b61bc2d8850c7baf59c99823",
	"internal/xferreceive/contracts.go":            "f36bbb1e1bee420b6e8dfee34c2590075eb2c3fc97216b0364f6cb9813eb54c8",
	"internal/xfercourier/contracts.go":            "c1656a8aa95110caf2505e03ba8a5ee75a11173c30acae9a3e965e5533b37173",
	"internal/xfertrack/contracts.go":              "8b4226479e85794a838ddc89cd9685a30aa3877d6bd87c42835d849deb8576a7",
	"internal/xferfinal/contracts.go":              "fa00099ec2c00e0b67da3866a8ae13444f292a65dd35d72612e0c2f8b7ca4515",
	"internal/xferledger/contracts.go":             "71e04e8d159a75b7c2f9da49a703e7fd5c7bbe4cbad21e25455d3a6069ec494f",
	"internal/xferhistory/contracts.go":            "57c38faf8c196ba706440cc407c4aa4e4b46705ae1de9c7d652bb0d0550d2896",
	"internal/xferbucket/contracts.go":             "3f659d0021c87773ca379bf24439d56b4d84ff4fccf51f5888f7ab6692dd0bff",
	"internal/xfergc/contracts.go":                 "1fdafc550388f730aedfaecf22c596859917468fc87a13ca8025c811299640f1",
	// The read-only source-selection amendment implements §2 / §6.1 / §7;
	// selection_contract_test.go proves stripping only its two additive DTOs
	// reproduces the original P00 pin and that absent fields preserve old bytes.
	// origin: S10 internal review MAKETEST-GATES F5 / S7 plan §6.1
	"internal/xfercontrol/contracts.go": "98127e90392e72f416dae714815904394246d8c3f0943f535dbdbe0ca6f911ae",
	"internal/xferclient/contracts.go":  "78e6befeaf449481ebbb50a6d3736b79c4596cbcd20d8945797c79549c36ac55",
	"internal/xferimage/contracts.go":   "5b5c78bb3be25f21d869cb97219ca75d785965821fea19e5e9e4c9f30bb023e6",
	"internal/xferauth/contracts.go":    "8570b616f10b9f46a2815d29583bcdb6af1b66888c8ae0d17748a1f09c192529",
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

func contractDigest(source []byte) (string, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "contracts.go", source, 0)
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	if err := printer.Fprint(&out, token.NewFileSet(), file); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", sha256.Sum256(out.Bytes())), nil
}

// origin: hot-update S7 plan §8.1 / P00 signature and field freeze
// gate-control: TestTransferContractBoundaryControls
func TestTransferContractSignatures(t *testing.T) {
	root := contractRoot(t)
	// 20 P00 files plus the two xfervalue files of the NATS-free value split.
	if len(frozenContracts) != 22 {
		t.Fatal("contract file census changed")
	}
	for path, want := range frozenContracts {
		t.Run(path, func(t *testing.T) {
			raw, err := os.ReadFile(filepath.Join(root, path))
			if err != nil {
				t.Fatal(err)
			}
			// Hash the six moved aliases against their original digest. The
			// owner's runtime implementation does not belong to the P00 file.
			// origin: S7 internal review GATES-LEAD F7 / S7 plan §11.4
			if path == "internal/xferworker/owner.go" {
				raw, err = workerContractAliases(raw)
				if err != nil {
					t.Fatal(err)
				}
			}
			got, err := contractDigest(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Errorf("contract drift %s: got %s want %s", path, got, want)
			}
			if err := checkContractDeclarations(raw); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// checkContractDeclarations keeps constructors out of permanent contracts and
// enforces their allowed dependencies on all platforms, including aliases.
func checkContractDeclarations(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "contracts.go", source, 0)
	if err != nil {
		return err
	}
	natsNames := map[string]bool{}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return err
		}
		if strings.Contains(strings.Split(path, "/")[0], ".") &&
			path != "github.com/LinZiyang666/tether/internal/controlbudget" &&
			path != "github.com/LinZiyang666/tether/internal/handoff" &&
			path != "github.com/LinZiyang666/tether/internal/xfercontract" &&
			path != "github.com/LinZiyang666/tether/internal/xfercontract/xferport" &&
			// The frozen, NATS-free value half; its own closure is a layering root.
			// origin: hot-update S7 plan §11.1 / P00 — NATS-free value split
			path != "github.com/LinZiyang666/tether/internal/xfercontract/xfervalue" &&
			path != "github.com/nats-io/nats.go/jetstream" {
			return fmt.Errorf("forbidden contract dependency %s", path)
		}
		if path == "context" {
			return errors.New("contract API must remain ctx-free")
		}
		if path == "github.com/nats-io/nats.go/jetstream" {
			name := "jetstream"
			if imp.Name != nil {
				name = imp.Name.Name
			}
			if name == "." || name == "_" {
				return errors.New("contract NATS dependency must name its interface")
			}
			natsNames[name] = true
		}
	}
	for _, decl := range file.Decls {
		if _, ok := decl.(*ast.FuncDecl); ok {
			return errors.New("implementation in contract file")
		}
	}
	var bad, concreteNATS bool
	ast.Inspect(file, func(node ast.Node) bool {
		if id, ok := node.(*ast.Ident); ok && id.Name == "any" {
			bad = true
		}
		if typ, ok := node.(*ast.InterfaceType); ok && len(typ.Methods.List) == 0 {
			bad = true
		}
		if sel, ok := node.(*ast.SelectorExpr); ok && file.Name.Name == "xfercontract" {
			if id, ok := sel.X.(*ast.Ident); ok && natsNames[id.Name] && sel.Sel.Name != "JetStream" {
				concreteNATS = true
			}
		}
		return true
	})
	if bad {
		return errors.New("untyped contract escape")
	}
	if concreteNATS {
		return errors.New("neutral contract contains a concrete NATS result")
	}
	return nil
}

type stubCase struct {
	value any
	owner string
}

// The constructors intentionally fail. The separate zero-size witnesses let
// consumers compile method calls without receiving a pretend live owner.
func stubConstructors() []stubCase {
	return []stubCase{
		{s.NewTransferWriter, "jsstream: not implemented (S7 P02)"},
		{s.PutLegacyOnce, "jsstream: not implemented (S7 P02)"},
		{s.NewTransferReader, "jsstream: not implemented (S7 P03)"},
		{s.ImportAtomic, "transferpath: not implemented (S7 P05)"},
		{s.NewWorker, "xferworker: not implemented (S7 P06)"},
		{s.ImportWorker, "xferworker: not implemented (S7 P06)"},
		{s.NewReceiver, "xferreceive: not implemented (S7 P07)"},
		{s.NewSender, "xfersend: not implemented (S7 P08)"},
		{s.NewCourier, "xfercourier: not implemented (S7 P09)"},
		{s.EncodeWorker, "xferimage: not implemented (S7 P10)"},
		{s.DecodeWorker, "xferimage: not implemented (S7 P10)"},
		{s.NewTracker, "xfertrack: not implemented (S7 P11)"},
		{s.ImportTracker, "xfertrack: not implemented (S7 P11)"},
		{s.NewFinalizer, "xferfinal: not implemented (S7 P12)"},
		{s.NewLedger, "xferledger: not implemented (S7 P13)"},
		{s.NewHistory, "xferhistory: not implemented (S7 P14)"},
		{s.NewBucket, "xferbucket: not implemented (S7 P04)"},
		{s.NewController, "xfercontrol: not implemented (S7 P16)"},
		{s.NewCollector, "xfergc: not implemented (S7 P17)"},
		{s.EncodeTracker, "xferimage: not implemented (S7 P18)"},
		{s.DecodeTracker, "xferimage: not implemented (S7 P18)"},
		{s.NewAuth, "xferauth: not implemented (S7 P19)"},
		{s.NewClient, "xferclient: not implemented (S7 P25)"},
		{s.NewLegacy, "xferclient: not implemented (S7 P25)"},
	}
}
func stubWitnesses() []stubCase {
	return []stubCase{
		{s.Writer{}, "jsstream: not implemented (S7 P02)"},
		{s.Reader{}, "jsstream: not implemented (S7 P03)"},
		{s.Atomic{}, "transferpath: not implemented (S7 P05)"},
		{s.Source{}, "transferpath: not implemented (S7 P05)"},
		{s.Worker{}, "xferworker: not implemented (S7 P06)"},
		{s.FrozenWorker{}, "xferworker: not implemented (S7 P06)"},
		{s.Capacity{}, "xferworker: not implemented (S7 P06)"},
		{s.Receiver{}, "xferreceive: not implemented (S7 P07)"},
		{s.Sender{}, "xfersend: not implemented (S7 P08)"},
		{s.Courier{}, "xfercourier: not implemented (S7 P09)"},
		{s.Tracker{}, "xfertrack: not implemented (S7 P11)"},
		{s.FrozenTracker{}, "xfertrack: not implemented (S7 P11)"},
		{s.Decisions{}, "xfertrack: not implemented (S7 P11)"},
		{s.Finalizer{}, "xferfinal: not implemented (S7 P12)"},
		{s.ObjectRetirer{}, "xferfinal: not implemented (S7 P12)"},
		{s.Ledger{}, "xferledger: not implemented (S7 P13)"},
		{s.History{}, "xferhistory: not implemented (S7 P14)"},
		{s.Bucket{}, "xferbucket: not implemented (S7 P04)"},
		{s.ConfigAuthority{}, "xferbucket: not implemented (S7 P04)"},
		{s.Controller{}, "xfercontrol: not implemented (S7 P16)"},
		{s.Authority{}, "xfercontrol: not implemented (S7 P16)"},
		{s.ReceiverResolver{}, "xfercontrol: not implemented (S7 P16)"},
		{s.PermitSigner{}, "xfercontrol: not implemented (S7 P16)"},
		{s.Collector{}, "xfergc: not implemented (S7 P17)"},
		{s.OwnerResolver{}, "xfergc: not implemented (S7 P17)"},
		{s.Auth{}, "xferauth: not implemented (S7 P19)"},
		{s.IssuerVerifier{}, "xferauth: not implemented (S7 P19)"},
		{s.Client{}, "xferclient: not implemented (S7 P25)"},
		{s.Legacy{}, "xferclient: not implemented (S7 P25)"},
		{s.Transport{}, "cli: not implemented (S7 P26)"},
	}
}

func checkStubResult(out []reflect.Value, want string) error {
	if len(out) == 1 && out[0].Type() == reflect.TypeFor[x.ReceiverOutcome]() {
		got := out[0].Interface().(x.ReceiverOutcome)
		expected := x.ReceiverOutcome{Code: x.CodeResultUnknown, Error: want}
		if got != expected {
			return errors.New("commit stub fabricated primitive evidence or lost its error")
		}
		return nil
	}
	if len(out) == 0 || out[len(out)-1].Type() != reflect.TypeFor[error]() {
		return errors.New("stub has no error result")
	}
	err, _ := out[len(out)-1].Interface().(error)
	if !errors.Is(err, x.ErrContractStub) || err.Error() != want {
		return fmt.Errorf("stub returned %v; want %s wrapping ErrContractStub", err, want)
	}
	for _, val := range out[:len(out)-1] {
		if !val.IsZero() {
			return errors.New("stub returned nonzero success evidence")
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

// Every public stub method is called, including Stop/Release/Detach: pretending
// cleanup succeeded could hide retained ownership just as a false ACK could.
// origin: hot-update S7 plan §8.1 / P00 inert stub negative controls
// gate-control: TestTransferContractBoundaryControls
func TestTransferContractStubsRejectEveryOperation(t *testing.T) {
	constructors, witnesses := stubConstructors(), stubWitnesses()
	if len(constructors) != 24 || len(witnesses) != 30 {
		t.Fatal("stub census changed")
	}
	for i, tc := range constructors {
		t.Run(fmt.Sprintf("factory-%02d", i), func(t *testing.T) {
			callStub(t, reflect.ValueOf(tc.value), tc.owner)
		})
	}
	for _, tc := range witnesses {
		value := reflect.ValueOf(tc.value)
		if value.Type().NumField() != 0 || value.NumMethod() == 0 {
			t.Fatal("stub is not an inert witness")
		}
		for i := 0; i < value.NumMethod(); i++ {
			t.Run(value.Type().Name()+"."+value.Type().Method(i).Name, func(t *testing.T) {
				callStub(t, value.Method(i), tc.owner)
			})
		}
	}
}

const stubImport = "github.com/LinZiyang666/tether/internal/xfercontract/contractstub"

func checkProductionImports(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "source.go", source, parser.ImportsOnly)
	if err != nil {
		return err
	}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return err
		}
		if path == stubImport {
			return errors.New("production imports contractstub")
		}
	}
	return nil
}

// Restrict stubs to one return statement and the exact error constructors. A
// passing runtime check alone cannot show that a stub avoided opening a file
// and closing it before it returned an error.
func checkStubPurity(source []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "stub.go", source, 0)
	if err != nil {
		return err
	}
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return err
		}
		if path != "fmt" && path != "io" && path != "os" &&
			path != "github.com/LinZiyang666/tether/internal/handoff" &&
			path != "github.com/LinZiyang666/tether/internal/xfercontract" &&
			path != "github.com/nats-io/nats.go/jetstream" {
			return fmt.Errorf("stub import can initialize production work: %s", path)
		}
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		if fn.Body == nil || len(fn.Body.List) != 1 {
			return errors.New("stub contains work")
		}
		if _, ok := fn.Body.List[0].(*ast.ReturnStmt); !ok {
			return errors.New("stub is not a return")
		}
	}
	var bad bool
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			bad = true
			return true
		}
		id, ok := sel.X.(*ast.Ident)
		if !ok {
			bad = true
			return true
		}
		if (id.Name != "fmt" || sel.Sel.Name != "Errorf") &&
			(id.Name != "errTransferpathP05" || sel.Sel.Name != "Error") {
			bad = true
		}
		return true
	})
	if bad {
		return errors.New("stub calls a side effect or callback")
	}
	return nil
}

// Scan production imports independent of build tags. No products are allowed
// to link inert stubs even temporarily while their real owners are unfinished.
// origin: hot-update S7 plan §8.1 / §11.1 / P00
// gate-control: TestTransferContractBoundaryControls
func TestTransferContractImportBoundary(t *testing.T) {
	root := contractRoot(t)
	var scanned int
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, entry os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				if entry.Name() == "testdata" || entry.Name() == "contractstub" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			scanned++
			if err := checkProductionImports(raw); err != nil {
				return fmt.Errorf("%s: %w", path, err)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if scanned == 0 {
		t.Fatal("empty production scan")
	}
	raw, err := os.ReadFile(filepath.Join(root, "internal/xfercontract/contractstub/stub.go"))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkStubPurity(raw); err != nil {
		t.Fatal(err)
	}
}

// Controls include a changed signature with unchanged method names, an extra
// private field and nil-error false success. Parsing still succeeds, so each
// negative exercises the actual predicate instead of failing fixture setup.
// origin: hot-update S7 plan §8.1 / P00 adversarial controls
func TestTransferContractBoundaryControls(t *testing.T) {
	// A source move must reproduce the original alias contract, including
	// alias identity; the owner file itself remains free to implement runtime IO.
	// origin: S7 internal review GATES-LEAD F7
	owner, err := os.ReadFile(filepath.Join(contractRoot(t), "internal/xferworker/owner.go"))
	if err != nil {
		t.Fatal(err)
	}
	for _, change := range [][2]string{
		{"type Factory = x.WorkerFactory", "type Factory = x.WorkerFactory"},
		{"type Factory = x.WorkerFactory", "type Factory = x.WorkerImportFactory"},
		{"type Factory = x.WorkerFactory", "type Factory x.WorkerFactory"},
		{"type Factory = x.WorkerFactory", ""},
		{"type Factory = x.WorkerFactory", "type Factory = x.WorkerFactory; type Factory = x.WorkerFactory"},
		{"package xferworker", "package unrelated"},
	} {
		mutant := strings.Replace(string(owner), change[0], change[1], 1)
		if !strings.Contains(string(owner), change[0]) {
			t.Fatal("empty worker alias control")
		}
		projected, err := workerContractAliases([]byte(mutant))
		digest := ""
		if err == nil {
			digest, err = contractDigest(projected)
		}
		if accepted := err == nil && digest == frozenContracts["internal/xferworker/owner.go"]; accepted != (change[0] == change[1]) {
			t.Fatalf("worker alias control %v: digest=%s err=%v", change, digest, err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(contractRoot(t), "internal/xfercontract/ports.go"))
	if err != nil {
		t.Fatal(err)
	}
	original, err := contractDigest(raw)
	if err != nil {
		t.Fatal(err)
	}
	for _, control := range []struct {
		path, before, after string
	}{
		{"internal/xfercontract/xferport/ports.go", "WriteAt([]byte, int64)", "WriteAt([]byte, uint64)"},
		{"internal/xfercontract/ports.go", "Capacity CapacityAccount", "Capacity CapacityAccount; Extra bool"},
	} {
		controlRaw, err := os.ReadFile(filepath.Join(contractRoot(t), control.path))
		if err != nil {
			t.Fatal(err)
		}
		controlOriginal, err := contractDigest(controlRaw)
		if err != nil {
			t.Fatal(err)
		}
		pair := [2]string{control.before, control.after}
		// gofmt aligns fields with tabs; select normalized source for controls.
		source := strings.ReplaceAll(string(controlRaw), "\t", " ")
		old := pair[0]
		if !strings.Contains(source, old) && strings.HasPrefix(old, "Capacity ") {
			old = "type Base struct {"
			pair[1] = old + " Extra bool;"
		}
		mutant := strings.Replace(source, old, pair[1], 1)
		if mutant == source {
			t.Fatal("empty shape control")
		}
		digest, err := contractDigest([]byte(mutant))
		if err != nil {
			t.Fatal(err)
		}
		if digest == controlOriginal {
			t.Fatal("shape control accepted")
		}
	}
	positive, err := contractDigest(append([]byte("// comment only\n"), raw...))
	if err != nil || positive != original {
		t.Fatal("comment changed contract digest")
	}
	for _, source := range []string{
		`package p; import _ "github.com/LinZiyang666/tether/internal/broker"`,
		`package p; import "context"; type F func(context.Context)`,
		"package p; type V struct { Extra any }",
		"package p; type V interface {}",
		"package p; func New() {}",
		`package xfercontract; import sdk "github.com/nats-io/nats.go/jetstream"; type R = sdk.ObjectInfo`,
		`package xfercontract; import . "github.com/nats-io/nats.go/jetstream"; type R = ObjectInfo`,
	} {
		if err := checkContractDeclarations([]byte(source)); err == nil {
			t.Fatalf("accepted invalid declaration %s", source)
		}
	}
	if err := checkContractDeclarations(raw); err != nil {
		t.Fatal(err)
	}
	if err := checkProductionImports([]byte(`package p; import alias "` + stubImport + `"`)); err == nil {
		t.Fatal("production stub import accepted")
	}
	if err := checkProductionImports([]byte(`package p; import "fmt"`)); err != nil {
		t.Fatal(err)
	}
	if err := checkStubPurity([]byte("package p; func Stop() error { go run(); return errStub }")); err == nil {
		t.Fatal("stub goroutine accepted")
	}
	if err := checkStubPurity([]byte("package p; func Stop() error { return callback() }")); err == nil {
		t.Fatal("stub callback accepted")
	}
	if err := checkStubPurity([]byte(`package p; import _ "net/http/pprof"`)); err == nil {
		t.Fatal("stub initializer import accepted")
	}
	if err := checkStubPurity([]byte("package p; func Stop() error { return errStub }")); err != nil {
		t.Fatal(err)
	}
	want := "jsstream: not implemented (S7 P02)"
	if err := checkStubResult([]reflect.Value{reflect.Zero(reflect.TypeFor[error]())}, want); err == nil {
		t.Fatal("success accepted")
	}
	actual := reflect.ValueOf(s.Writer{}).MethodByName("Stop").Call([]reflect.Value{reflect.ValueOf(x.Deadline(1))})
	if err := checkStubResult(actual, want); err != nil {
		t.Fatal(err)
	}
	if err := checkStubResult(actual, "wrong owner"); err == nil {
		t.Fatal("wrong owner accepted")
	}
	falseEvidence := []reflect.Value{reflect.ValueOf(true), actual[0]}
	if err := checkStubResult(falseEvidence, want); err == nil {
		t.Fatal("nonzero result accepted")
	}
	if err := checkStubResult([]reflect.Value{reflect.ValueOf(x.ReceiverOutcome{})}, want); err == nil {
		t.Fatal("empty commit accepted")
	}
}

// Normalize only the moved import spelling, then rebuild the original alias
// file. Missing, reordered, renamed or retargeted aliases still change its P00
// digest; pinning the whole owner would freeze unrelated runtime behavior.
// origin: S7 internal review GATES-LEAD F7 / hot-update S7 plan §11.4
func workerContractAliases(raw []byte) ([]byte, error) {
	file, err := parser.ParseFile(token.NewFileSet(), "owner.go", raw, 0)
	if err != nil {
		return nil, err
	}
	alias := ""
	const dependency = "github.com/LinZiyang666/tether/internal/xfercontract"
	for _, imp := range file.Imports {
		path, err := strconv.Unquote(imp.Path.Value)
		if err != nil {
			return nil, err
		}
		if path == dependency {
			alias = "xfercontract"
			if imp.Name != nil {
				alias = imp.Name.Name
			}
		}
	}
	if file.Name.Name != "xferworker" || alias == "" || alias == "." || alias == "_" {
		return nil, errors.New("worker aliases lost their frozen package dependency")
	}
	imports := &ast.GenDecl{Tok: token.IMPORT, Specs: []ast.Spec{&ast.ImportSpec{Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(dependency)}}}}
	decls := []ast.Decl{imports}
	seen := map[string]bool{}
	for _, decl := range file.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.TYPE {
			continue
		}
		for _, spec := range gd.Specs {
			typ := spec.(*ast.TypeSpec)
			switch typ.Name.Name {
			case "Config", "Bindings", "Manager", "Frozen", "Factory", "ImportFactory":
				if seen[typ.Name.Name] {
					return nil, errors.New("duplicate worker contract alias")
				}
				seen[typ.Name.Name] = true
				ast.Inspect(typ, func(n ast.Node) bool {
					if sel, ok := n.(*ast.SelectorExpr); ok {
						if id, ok := sel.X.(*ast.Ident); ok && id.Name == alias {
							id.Name = "xfercontract"
						}
					}
					return true
				})
				decls = append(decls, &ast.GenDecl{Tok: token.TYPE, Specs: []ast.Spec{typ}})
			}
		}
	}
	if len(seen) != 6 {
		return nil, errors.New("worker contract alias census changed")
	}
	file.Decls, file.Imports = decls, nil
	var out bytes.Buffer
	if err := printer.Fprint(&out, token.NewFileSet(), file); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}
