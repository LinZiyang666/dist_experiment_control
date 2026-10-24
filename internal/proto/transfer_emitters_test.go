package proto_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Follow real package-qualified aliases to their defining literal. The value
// split must neither hide emitters from the scanner nor let a same-named code
// in an unrelated package satisfy a missing declaration.
// origin: hot-update S7 plan §11.4 / T50 / INTEG-D2
// origin: S7 internal review GATE-CONVERGE-contracts F2 — S8 aliases end in upgradetx.
func transferEmitterConstants(t *testing.T, root string) map[string]string {
	t.Helper()
	constants, aliases := map[string]string{}, map[string]string{}
	transferEachGoFile(t, root, []string{"internal/xfercontract", "internal/proto", "internal/upgradetx"}, func(rel string, file *ast.File, _ *token.FileSet) {
		pkg := "github.com/LinZiyang666/tether/" + filepath.ToSlash(filepath.Dir(rel))
		imports := map[string]string{}
		for _, imp := range file.Imports {
			path, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				t.Fatal(err)
			}
			name := filepath.Base(path)
			if imp.Name != nil {
				name = imp.Name.Name
			}
			imports[name] = path
		}
		for _, decl := range file.Decls {
			group, ok := decl.(*ast.GenDecl)
			if !ok || group.Tok != token.CONST {
				continue
			}
			for _, spec := range group.Specs {
				value := spec.(*ast.ValueSpec)
				for i, name := range value.Names {
					if !strings.HasPrefix(name.Name, "Code") || i >= len(value.Values) {
						continue
					}
					key := pkg + "." + name.Name
					switch v := value.Values[i].(type) {
					case *ast.BasicLit:
						if v.Kind == token.STRING {
							constants[key], _ = strconv.Unquote(v.Value)
						}
					case *ast.SelectorExpr:
						if alias, ok := v.X.(*ast.Ident); ok {
							aliases[key] = imports[alias.Name] + "." + v.Sel.Name
						}
					case *ast.Ident:
						aliases[key] = pkg + "." + v.Name
					}
				}
			}
		}
	})
	for len(aliases) > 0 {
		progress := false
		for key, target := range aliases {
			if value, ok := constants[target]; ok {
				constants[key] = value
				delete(aliases, key)
				progress = true
			}
		}
		if !progress {
			t.Fatalf("unresolved transfer code aliases: %v", aliases)
		}
	}
	return constants
}

func transferEachGoFile(t *testing.T, root string, dirs []string, visit func(string, *ast.File, *token.FileSet)) {
	t.Helper()
	for _, dir := range dirs {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			fs := token.NewFileSet()
			f, err := parser.ParseFile(fs, path, nil, 0)
			if err != nil {
				return err
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			visit(rel, f, fs)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Only actual function-body references in reply owners count as evidence;
// neutral declarations and CLI classification cannot vouch for an emitter.
// origin: hot-update S7 plan §11.4 / T50 / INTEG-D2
func addTransferEmissionEvidence(t *testing.T, root string, emitted map[string]bool) {
	constants := transferEmitterConstants(t, root)
	transferEachGoFile(t, root, []string{"internal/xfercontrol", "internal/xfertrack", "internal/xferauth", "internal/xferreceive", "internal/filetransfer", "internal/jsstream"}, func(_ string, f *ast.File, _ *token.FileSet) {
		imports := map[string]string{}
		for _, imp := range f.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			name := filepath.Base(path)
			if imp.Name != nil {
				name = imp.Name.Name
			}
			imports[name] = path
		}
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				if sel, ok := n.(*ast.SelectorExpr); ok {
					if alias, ok := sel.X.(*ast.Ident); ok {
						if value := constants[imports[alias.Name]+"."+sel.Sel.Name]; value != "" {
							emitted[value] = true
						}
					}
				}
				return true
			})
		}
	})
}

// A moved neutral definition must resolve through import aliases, but only a
// reply-owner body may vouch for its emission. Declaration-only and unrelated
// same-name constants cannot make the guard pass.
// origin: S7 internal review GATE-CONVERGE-contracts F2 / T50
func TestTransferEmitterAliasesRequireLiveBodyReference(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"xfercontract", "proto", "upgradetx", "xfercontrol", "xfertrack", "xferauth", "xferreceive", "filetransfer", "jsstream"} {
		if err := os.MkdirAll(filepath.Join(root, "internal", dir), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	write := func(path, source string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(root, "internal", path), []byte(source), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("upgradetx/codes.go", `package upgradetx;const CodeRejected="upgrade_rejected"`)
	write("proto/codes.go", `package proto;import tx "github.com/LinZiyang666/tether/internal/upgradetx";const CodeUpgradeRejected=tx.CodeRejected`)
	write("xfercontract/codes.go", `package xfercontract;const CodeRejected="unrelated"`)
	key := "github.com/LinZiyang666/tether/internal/proto.CodeUpgradeRejected"
	if got := transferEmitterConstants(t, root)[key]; got != "upgrade_rejected" {
		t.Fatalf("moved alias resolved to %q", got)
	}
	for _, tc := range []struct {
		source string
		live   bool
	}{
		{`package xfercontrol;import wire "github.com/LinZiyang666/tether/internal/proto";func Refuse()string{return wire.CodeUpgradeRejected}`, true},
		{`package xfercontrol;import wire "github.com/LinZiyang666/tether/internal/proto";const Code=wire.CodeUpgradeRejected;func Refuse()string{return "different"}`, false},
		{`package xfercontrol;import wire "github.com/LinZiyang666/tether/internal/xfercontract";func Refuse()string{return wire.CodeRejected}`, false},
	} {
		write("xfercontrol/refuse.go", tc.source)
		emitted := map[string]bool{}
		addTransferEmissionEvidence(t, root, emitted)
		if emitted["upgrade_rejected"] != tc.live {
			t.Fatalf("body emission evidence=%v want live=%t", emitted, tc.live)
		}
	}
}
