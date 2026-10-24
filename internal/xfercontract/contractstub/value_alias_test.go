package contractstub_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

const valueImportPath = contractModule + "internal/xfercontract/xfervalue"

func parseContractFile(t *testing.T, path string) *ast.File {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(contractRoot(t), path))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "contracts.go", raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func astDigest(t *testing.T, file *ast.File) string {
	t.Helper()
	var out bytes.Buffer
	if err := printer.Fprint(&out, token.NewFileSet(), file); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(out.Bytes()))
}

// valueSelector reports whether expr is exactly xfervalue.<name>.
func valueSelector(expr ast.Expr, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkg, ok := sel.X.(*ast.Ident)
	return ok && pkg.Name == "xfervalue" && sel.Sel.Name == name
}

// rebuildMovedDeclarations puts every declaration that the shim re-exports from
// xfervalue back in place, and drops the shim's xfervalue import. If the moved
// text and every untouched declaration are intact, this is the frozen P00 file.
func rebuildMovedDeclarations(shim, moved *ast.File) (restored int) {
	types, values := map[string]*ast.TypeSpec{}, map[string]*ast.ValueSpec{}
	for _, decl := range moved.Decls {
		if g, ok := decl.(*ast.GenDecl); ok {
			for _, spec := range g.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					types[s.Name.Name] = s
				case *ast.ValueSpec:
					values[s.Names[0].Name] = s
				}
			}
		}
	}
	for _, decl := range shim.Decls {
		g, ok := decl.(*ast.GenDecl)
		if !ok {
			continue
		}
		var specs []ast.Spec
		for _, spec := range g.Specs {
			switch s := spec.(type) {
			case *ast.ImportSpec:
				if path, _ := strconv.Unquote(s.Path.Value); path == valueImportPath {
					continue
				}
			case *ast.TypeSpec:
				if original := types[s.Name.Name]; original != nil && s.Assign.IsValid() && valueSelector(s.Type, s.Name.Name) {
					spec, restored = original, restored+1
				}
			case *ast.ValueSpec:
				if original := values[s.Names[0].Name]; original != nil && len(s.Names) == 1 && len(s.Values) == 1 && valueSelector(s.Values[0], s.Names[0].Name) {
					spec, restored = original, restored+1
				}
			}
			specs = append(specs, spec)
		}
		g.Specs = specs
	}
	return restored
}

// The NATS-free value split is a move, not an edit. contracts.go moved whole:
// restoring its package clause must give the frozen P00 digest. state.go kept
// its records and re-exports the custody declarations in place: putting the
// moved text back must give the frozen P00 state digest. Any change to a moved
// field, tag, enum value, or to a declaration left behind, breaks one of them.
// origin: hot-update S7 plan §8.1 / §11.1 / P00 — NATS-free value split
// gate-control: TestTransferValueReexportControls
func TestTransferValueDeclarationsKeepFrozenP00Digests(t *testing.T) {
	moved := parseContractFile(t, "internal/xfercontract/xfervalue/contracts.go")
	if moved.Name.Name != "xfervalue" {
		t.Fatalf("value package clause: %s", moved.Name.Name)
	}
	moved.Name.Name = "xfercontract"
	if got := astDigest(t, moved); got != "9e8319ed5f0adaa8925eca32d96810ab392cfe6fdb23cba8bfbd1ed7457bacb2" {
		t.Fatalf("moved contracts.go is not the frozen P00 body: %s", got)
	}
	shim := parseContractFile(t, "internal/xfercontract/state.go")
	if restored := rebuildMovedDeclarations(shim, parseContractFile(t, "internal/xfercontract/xfervalue/state.go")); restored != 27 {
		t.Fatalf("custody re-export census changed: %d", restored)
	}
	// origin: hot-update S7 plan §8.3 / FIX-INLINE; reconstruct the same additive SenderState.Inline amendment.
	if got := astDigest(t, shim); got != "6a6cf18cedfbe3340d03c4338f760b89ad6826be416e7cddbe7e3728b924eefb" {
		t.Fatalf("state.go split changed a frozen P00 declaration: %s", got)
	}
}

// declaredNames maps every top-level name of a file to its spec.
func declaredNames(file *ast.File) map[string]ast.Spec {
	out := map[string]ast.Spec{}
	for _, decl := range file.Decls {
		if g, ok := decl.(*ast.GenDecl); ok {
			for _, spec := range g.Specs {
				switch s := spec.(type) {
				case *ast.TypeSpec:
					out[s.Name.Name] = s
				case *ast.ValueSpec:
					for _, name := range s.Names {
						out[name.Name] = s
					}
				}
			}
		}
	}
	return out
}

// reexportGap returns the first xfervalue name that xfercontract does not
// re-export with the same identity, or a shim declaration that is not a pure
// re-export. A parallel alias to the same xferport target is the same identity.
func reexportGap(value, shim map[string]ast.Spec, shimOnly map[string]bool) string {
	for name, spec := range value {
		if !ast.IsExported(name) {
			continue
		}
		own, ok := shim[name]
		if !ok {
			return "missing re-export " + name
		}
		switch s := own.(type) {
		case *ast.TypeSpec:
			original, _ := spec.(*ast.TypeSpec)
			if !s.Assign.IsValid() || original == nil || !valueSelector(s.Type, name) && (!original.Assign.IsValid() || types.ExprString(s.Type) != types.ExprString(original.Type)) {
				return "type " + name + " is not an alias of the value"
			}
		case *ast.ValueSpec:
			if len(s.Names) != 1 || len(s.Values) != 1 || !valueSelector(s.Values[0], name) {
				return "value " + name + " is not a re-export"
			}
			if gap := reexportTypeGap(name, s, spec); gap != "" {
				return gap
			}
		}
	}
	for name := range shimOnly {
		if _, ok := value[name]; !ok {
			return "shim declares its own " + name
		}
	}
	return ""
}

// reexportTypeGap keeps an untyped literal untyped (Code*, Version: giving them
// a type would change every untyped use) and lets an enum re-export name its
// family type only: the compiler rejects a member assigned to the wrong family,
// and a family declared explicitly in xfervalue must keep exactly that name.
func reexportTypeGap(name string, shim *ast.ValueSpec, spec ast.Spec) string {
	original, _ := spec.(*ast.ValueSpec)
	if shim.Type == nil || original == nil {
		return ""
	}
	typ, ok := shim.Type.(*ast.Ident)
	if !ok {
		return "value " + name + " re-export has a composite type"
	}
	if original.Type == nil {
		for _, value := range original.Values {
			if _, literal := value.(*ast.BasicLit); literal {
				return "untyped value " + name + " was given a type"
			}
		}
		return ""
	}
	if want, ok := original.Type.(*ast.Ident); !ok || want.Name != typ.Name {
		return "value " + name + " changed its family type"
	}
	return ""
}

func valueReexportSets(t *testing.T) (value, shim map[string]ast.Spec, contractsOnly map[string]bool) {
	t.Helper()
	value, shim, contractsOnly = map[string]ast.Spec{}, map[string]ast.Spec{}, map[string]bool{}
	for _, path := range []string{"internal/xfercontract/xfervalue/contracts.go", "internal/xfercontract/xfervalue/state.go"} {
		for name, spec := range declaredNames(parseContractFile(t, path)) {
			value[name] = spec
		}
	}
	for name, spec := range declaredNames(parseContractFile(t, "internal/xfercontract/contracts.go")) {
		shim[name], contractsOnly[name] = spec, true
	}
	for name, spec := range declaredNames(parseContractFile(t, "internal/xfercontract/state.go")) {
		if _, moved := value[name]; moved {
			shim[name] = spec
		}
	}
	return value, shim, contractsOnly
}

// Every value is re-exported one for one, so no consumer of xfercontract
// changed meaning, and the shim file declares nothing of its own.
// origin: hot-update S7 plan §6.2 / §11.1 / P00 — NATS-free value split
// gate-control: TestTransferValueReexportControls
func TestTransferContractReexportsEveryValue(t *testing.T) {
	value, shim, contractsOnly := valueReexportSets(t)
	if len(value) < 150 {
		t.Fatalf("value census collapsed: %d", len(value))
	}
	if gap := reexportGap(value, shim, contractsOnly); gap != "" {
		t.Fatal(gap)
	}
}

// origin: hot-update S7 plan §11.1 / P00 adversarial controls
func TestTransferValueReexportControls(t *testing.T) {
	value, shim, contractsOnly := valueReexportSets(t)
	mutated := func(edit func(map[string]ast.Spec, map[string]bool)) string {
		copyShim, copyOnly := map[string]ast.Spec{}, map[string]bool{}
		for name, spec := range shim {
			copyShim[name] = spec
		}
		for name := range contractsOnly {
			copyOnly[name] = true
		}
		edit(copyShim, copyOnly)
		return reexportGap(value, copyShim, copyOnly)
	}
	for name, edit := range map[string]func(map[string]ast.Spec, map[string]bool){
		"dropped re-export": func(s map[string]ast.Spec, _ map[string]bool) { delete(s, "Request") },
		"redefined type": func(s map[string]ast.Spec, _ map[string]bool) {
			s["Status"] = &ast.TypeSpec{Name: ast.NewIdent("Status"), Type: &ast.StructType{Fields: &ast.FieldList{}}}
		},
		"retargeted alias": func(s map[string]ast.Spec, _ map[string]bool) {
			s["Origin"] = &ast.TypeSpec{Name: ast.NewIdent("Origin"), Assign: 1, Type: &ast.SelectorExpr{X: ast.NewIdent("xfervalue"), Sel: ast.NewIdent("Intent")}}
		},
		"literal constant": func(s map[string]ast.Spec, _ map[string]bool) {
			s["CodeUnknown"] = &ast.ValueSpec{Names: []*ast.Ident{ast.NewIdent("CodeUnknown")}, Values: []ast.Expr{&ast.BasicLit{Kind: token.STRING, Value: `"transfer_unknown"`}}}
		},
		"own declaration": func(_ map[string]ast.Spec, o map[string]bool) { o["ShimOnly"] = true },
		"typed untyped code": func(s map[string]ast.Spec, _ map[string]bool) {
			s["CodeUnknown"] = &ast.ValueSpec{Names: []*ast.Ident{ast.NewIdent("CodeUnknown")}, Type: ast.NewIdent("string"), Values: []ast.Expr{&ast.SelectorExpr{X: ast.NewIdent("xfervalue"), Sel: ast.NewIdent("CodeUnknown")}}}
		},
		"wrong family type": func(s map[string]ast.Spec, _ map[string]bool) {
			s["StatusLive"] = &ast.ValueSpec{Names: []*ast.Ident{ast.NewIdent("StatusLive")}, Type: ast.NewIdent("Phase"), Values: []ast.Expr{&ast.SelectorExpr{X: ast.NewIdent("xfervalue"), Sel: ast.NewIdent("StatusLive")}}}
		},
	} {
		if mutated(edit) == "" {
			t.Fatalf("re-export census accepted %s", name)
		}
	}
	stateShim := parseContractFile(t, "internal/xfercontract/state.go")
	stateMoved := parseContractFile(t, "internal/xfercontract/xfervalue/state.go")
	for _, decl := range stateMoved.Decls {
		if g, ok := decl.(*ast.GenDecl); ok && g.Tok == token.CONST {
			g.Specs[0].(*ast.ValueSpec).Values[0] = &ast.BasicLit{Kind: token.STRING, Value: `"appended"`}
		}
	}
	rebuildMovedDeclarations(stateShim, stateMoved)
	if astDigest(t, stateShim) == "6a6cf18cedfbe3340d03c4338f760b89ad6826be416e7cddbe7e3728b924eefb" {
		t.Fatal("state reconstruction accepted a changed moved enum value")
	}
}
