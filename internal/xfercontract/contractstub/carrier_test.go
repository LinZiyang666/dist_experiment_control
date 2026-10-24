package contractstub_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	x "github.com/LinZiyang666/tether/internal/xfercontract"
)

// The extended fingerprint must not replace the old freeze with permission to
// alter existing contracts. Removing only the four authorized carrier fields
// must reproduce the original P00 declaration digest byte for byte.
// origin: hot-update S7 plan §8.1 / INTEG-D additive contract amendment
func TestTransferCarrierExtensionPreservesFrozenDeclarations(t *testing.T) {
	// The declarations moved verbatim into the NATS-free value package; the
	// package clause is the only difference and is restored before hashing.
	// origin: hot-update S7 plan §11.1 / P00 — NATS-free value split
	raw, err := os.ReadFile(filepath.Join(contractRoot(t), "internal/xfercontract/xfervalue/contracts.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "contracts.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	if file.Name.Name != "xfervalue" {
		t.Fatalf("carrier declarations are not in the value package: %s", file.Name.Name)
	}
	file.Name.Name = "xfercontract"
	wanted := map[string]map[string]bool{"Request": {"Inline": true}, "Response": {"Inline": true, "Source": true, "Settlement": true}}
	removed := 0
	for _, decl := range file.Decls {
		g, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		for _, spec := range g.Specs {
			typ, ok := spec.(*ast.TypeSpec)
			if !ok {
				continue
			}
			names := wanted[typ.Name.Name]
			if names == nil {
				continue
			}
			body := typ.Type.(*ast.StructType)
			var fields []*ast.Field
			for _, field := range body.Fields.List {
				if len(field.Names) == 1 && names[field.Names[0].Name] {
					removed++
					continue
				}
				fields = append(fields, field)
			}
			body.Fields.List = fields
		}
	}
	if removed != 4 {
		t.Fatalf("carrier extension census changed: %d", removed)
	}
	var canonical bytes.Buffer
	if err = printer.Fprint(&canonical, token.NewFileSet(), file); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(canonical.Bytes()))
	if digest != "f62fc19826ee43cfaaab4a828949f6a58a268878e3e1f386d29f8a4419d7f86f" {
		t.Fatal("carrier extension changed an existing P00 declaration")
	}
	for _, typ := range []reflect.Type{reflect.TypeFor[x.Status](), reflect.TypeFor[x.StatusRequest]()} {
		for _, name := range []string{"Inline", "Source", "Settlement"} {
			if _, ok := typ.FieldByName(name); ok {
				t.Fatalf("status acquired private carrier %s", name)
			}
		}
	}
}

// The legacy-orphan retirement port is the lead-authorized additive P00
// amendment (INTEG-R, T29): four new declarations plus one optional
// FinalConfig field. Removing exactly those must reproduce the original P00
// ports digest byte for byte, so the refreshed freeze cannot hide an edit to
// any pre-existing port, factory or config.
// origin: hot-update S7 plan §5.3 / T29 / INTEG-R additive P00 amendment
func TestTransferLegacyOrphanPortExtensionPreservesFrozenDeclarations(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(contractRoot(t), "internal/xfercontract/ports.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "contracts.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	added := map[string]bool{"LegacyOrphanFinalizer": true, "LegacyOrphanCandidate": true, "LegacyOrphanReceipt": true, "LegacyOrphanConfig": true}
	removed := 0
	var decls []ast.Decl
	for _, decl := range file.Decls {
		g, ok := decl.(*ast.GenDecl)
		if !ok {
			decls = append(decls, decl)
			continue
		}
		var specs []ast.Spec
		for _, spec := range g.Specs {
			typ, ok := spec.(*ast.TypeSpec)
			if ok && added[typ.Name.Name] {
				removed++
				continue
			}
			if ok && typ.Name.Name == "FinalConfig" {
				body := typ.Type.(*ast.StructType)
				var fields []*ast.Field
				for _, field := range body.Fields.List {
					if len(field.Names) == 1 && field.Names[0].Name == "LegacyOrphans" {
						removed++
						continue
					}
					fields = append(fields, field)
				}
				body.Fields.List = fields
			}
			specs = append(specs, spec)
		}
		if len(specs) != 0 {
			g.Specs = specs
			decls = append(decls, g)
		}
	}
	file.Decls = decls
	if removed != 5 {
		t.Fatalf("legacy orphan extension census changed: %d", removed)
	}
	var canonical bytes.Buffer
	if err = printer.Fprint(&canonical, token.NewFileSet(), file); err != nil {
		t.Fatal(err)
	}
	if digest := fmt.Sprintf("%x", sha256.Sum256(canonical.Bytes())); digest != "74f9b2d24eb25822e683d96019fb6fd0df4462e9fb00480bf587ee5be91a4ada" {
		t.Fatalf("legacy orphan extension changed an existing P00 port declaration: %s", digest)
	}
	if field, ok := reflect.TypeFor[x.FinalConfig]().FieldByName("LegacyOrphans"); !ok || field.Type.Kind() != reflect.Pointer {
		t.Fatal("legacy orphan binding must stay an optional pointer: nil refuses retirement")
	}
}
