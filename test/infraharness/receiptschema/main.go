// receiptschema derives the Python producer's field order from the actual Go
// consumer types, including promoted scope fields. It starts no runtime and
// has no copied business schema that could silently diverge from runLive.
// origin: S9 internal review INFRA-SUITE-SUPPLIES F6
package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

const module = "github.com/LinZiyang666/tether/"

type declaration struct {
	typeSpec  *ast.TypeSpec
	imports   map[string]string
	packageID string
}
type field struct {
	Name string
	Type shape
}
type shape struct {
	Kind   string
	Bits   int     `json:",omitempty"`
	Length int     `json:",omitempty"`
	Fields []field `json:",omitempty"`
	Item   *shape  `json:",omitempty"`
}
type registry map[string]declaration

func load(root string) (registry, error) {
	result := make(registry)
	for _, directory := range []string{"test/infrabusiness", "internal/infracontract", "internal/xfercontract", "internal/xfercontract/xfervalue", "internal/xfercontract/xferport"} {
		packages, err := parser.ParseDir(token.NewFileSet(), filepath.Join(root, directory), nil, 0)
		if err != nil {
			return nil, err
		}
		for _, pkg := range packages {
			if directory != "test/infrabusiness" && strings.HasSuffix(pkg.Name, "_test") {
				continue
			}
			for _, source := range pkg.Files {
				imports := make(map[string]string)
				for _, imported := range source.Imports {
					path, err := strconv.Unquote(imported.Path.Value)
					if err != nil {
						return nil, err
					}
					name := filepath.Base(path)
					if imported.Name != nil {
						name = imported.Name.Name
					}
					imports[name] = path
				}
				for _, decl := range source.Decls {
					general, ok := decl.(*ast.GenDecl)
					if !ok || general.Tok != token.TYPE {
						continue
					}
					for _, spec := range general.Specs {
						typeSpec := spec.(*ast.TypeSpec)
						key := module + directory + "." + typeSpec.Name.Name
						if _, duplicate := result[key]; duplicate {
							return nil, fmt.Errorf("duplicate receipt type: %s", key)
						}
						result[key] = declaration{typeSpec, imports, module + directory}
					}
				}
			}
		}
	}
	return result, nil
}

func (r registry) named(name string, depth int) (shape, error) {
	d, exists := r[name]
	if !exists || depth > 40 {
		return shape{}, fmt.Errorf("unsupported receipt type: %s", name)
	}
	return r.describe(d.typeSpec.Type, d, depth+1)
}

func (r registry) describe(node ast.Expr, d declaration, depth int) (shape, error) {
	switch value := node.(type) {
	case *ast.Ident:
		switch value.Name {
		case "string", "bool":
			return shape{Kind: value.Name}, nil
		case "int", "int64":
			return shape{Kind: "int", Bits: 64}, nil
		case "uint", "uint64":
			return shape{Kind: "uint", Bits: 64}, nil
		case "int8", "int16", "int32", "uint8", "uint16", "uint32", "byte":
			name := value.Name
			if name == "byte" {
				name = "uint8"
			}
			kind, start := "int", 3
			if name[0] == 'u' {
				kind, start = "uint", 4
			}
			bits, err := strconv.Atoi(name[start:])
			return shape{Kind: kind, Bits: bits}, err
		default:
			return r.named(d.packageID+"."+value.Name, depth)
		}
	case *ast.SelectorExpr:
		alias, ok := value.X.(*ast.Ident)
		if !ok {
			break
		}
		return r.named(d.imports[alias.Name]+"."+value.Sel.Name, depth)
	case *ast.ArrayType:
		if element, ok := value.Elt.(*ast.Ident); ok && element.Name == "byte" && value.Len == nil {
			return shape{Kind: "bytes"}, nil
		}
		item, err := r.describe(value.Elt, d, depth+1)
		if err != nil {
			return shape{}, err
		}
		if value.Len == nil {
			return shape{Kind: "slice", Item: &item}, nil
		}
		length, ok := value.Len.(*ast.BasicLit)
		if !ok {
			break
		}
		n, err := strconv.Atoi(length.Value)
		return shape{Kind: "array", Length: n, Item: &item}, err
	case *ast.StructType:
		result := shape{Kind: "struct"}
		for _, member := range value.Fields.List {
			jsonName := ""
			if member.Tag != nil {
				tag, err := strconv.Unquote(member.Tag.Value)
				if err != nil {
					return shape{}, err
				}
				jsonName = reflect.StructTag(tag).Get("json")
				if strings.Contains(jsonName, ",") {
					return shape{}, fmt.Errorf("receipt JSON tag options require explicit codec support")
				}
				if jsonName == "-" {
					continue
				}
			}
			typeShape, err := r.describe(member.Type, d, depth+1)
			if err != nil {
				return shape{}, err
			}
			if len(member.Names) == 0 {
				if typeShape.Kind != "struct" {
					return shape{}, fmt.Errorf("non-struct embedded receipt type")
				}
				result.Fields = append(result.Fields, typeShape.Fields...)
			} else {
				for _, name := range member.Names {
					if ast.IsExported(name.Name) {
						fieldName := name.Name
						if jsonName != "" {
							fieldName = jsonName
						}
						result.Fields = append(result.Fields, field{fieldName, typeShape})
					}
				}
			}
		}
		return result, nil
	}
	return shape{}, fmt.Errorf("unsupported receipt node %T", node)
}

func main() {
	if len(os.Args) != 2 || !filepath.IsAbs(os.Args[1]) {
		fmt.Fprintln(os.Stderr, "canonical source root required")
		os.Exit(2)
	}
	registry, err := load(os.Args[1])
	var receipt shape
	if err == nil {
		receipt, err = registry.named(module+"test/infrabusiness.businessReceipt", 0)
	}
	if err == nil {
		err = json.NewEncoder(os.Stdout).Encode(receipt)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
