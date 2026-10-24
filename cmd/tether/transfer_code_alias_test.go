package main

import (
	"fmt"
	"go/ast"
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
func transferCodeConstants(t *testing.T, root string) map[string]string {
	t.Helper()
	constants, aliases := map[string]string{}, map[string]string{}
	// Proto's S8 aliases reach the std-only transaction contract. Definitions
	// remain distinct from emitters; an alias must reach its actual owner.
	// origin: S7 internal review ledgers6 F3 (error-code missing-emitter-keys)
	// gate-control: TestTransferCodeAliasesFollowDefiningOwners
	forEachGoFile(t, root, []string{"internal/xfercontract", "internal/proto", "internal/upgradetx"}, func(rel string, file *ast.File, _ *token.FileSet) {
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
	if err := resolveTransferCodeAliases(constants, aliases); err != nil {
		t.Fatal(err)
	}
	return constants
}

func resolveTransferCodeAliases(constants, aliases map[string]string) error {
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
			return fmt.Errorf("unresolved transfer code aliases: %v", aliases)
		}
	}
	return nil
}

// An unrelated same-named constant must not satisfy an imported definition;
// missing owners and cycles remain errors instead of silently losing aliases.
// origin: S7 internal review ledgers6 F3 (error-code missing-emitter-keys)
func TestTransferCodeAliasesFollowDefiningOwners(t *testing.T) {
	root := t.TempDir()
	for path, src := range map[string]string{
		"internal/xfercontract/contracts.go": `package xfercontract; const CodeUnsettled = "unrelated_transfer_value"`,
		"internal/proto/codes.go":            `package proto; import tx "github.com/LinZiyang666/tether/internal/upgradetx"; const CodeUpgradeUnsettled = tx.CodeUnsettled; const CodeAlias = CodeUpgradeUnsettled`,
		"internal/upgradetx/contracts.go":    `package upgradetx; const CodeUnsettled = "upgrade_unsettled"`,
	} {
		abs := filepath.Join(root, path)
		if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(abs, []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	constants := transferCodeConstants(t, root)
	for _, name := range []string{"CodeUpgradeUnsettled", "CodeAlias"} {
		if got := constants["github.com/LinZiyang666/tether/internal/proto."+name]; got != "upgrade_unsettled" {
			t.Fatalf("%s resolved to %q instead of its imported owner", name, got)
		}
	}
	for name, aliases := range map[string]map[string]string{
		"missing owner": {"proto.CodeUpgrade": "upgradetx.CodeUnsettled"},
		"alias cycle":   {"proto.CodeA": "proto.CodeB", "proto.CodeB": "proto.CodeA"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := resolveTransferCodeAliases(map[string]string{"unrelated.CodeUnsettled": "wrong"}, aliases); err == nil {
				t.Fatal("unresolved imported owner was accepted")
			}
		})
	}
}
