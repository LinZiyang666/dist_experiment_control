package architecture

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"
)

// Reader consumer/window names and capsule nonces are auxiliary identities. Only these
// exact assignment shapes may call Next; changing the OBJ incarnation remains
// forbidden, including in the same reader function.
// origin: S7 internal review rv-wiring F10 / T49
// A replacement consumer is a read cursor over the original fixed object.
// Allow only the recorded Name allocation; every other nuid.Next remains an
// identity violation, even in this same function or elsewhere in jsstream.
// origin: S7 internal review INTEG-G F5
// origin: S7 internal review GATE-CONVERGE-contracts F5 — follow the receiver nonce owner.
func transferMintsOrRewinds(decl transferDeclaration) bool {
	consumerNames := map[*ast.SelectorExpr]bool{}
	consumerField := ""
	if decl.path == "internal/jsstream/object_resume.go" && decl.name == "transferResumeReader.openConsumer" {
		consumerField = "consumer"
	}
	if decl.path == "internal/jsstream/object_reader.go" && decl.name == "GetTransferObject" {
		consumerField = "consumerName"
	}
	if consumerField != "" {
		ast.Inspect(decl.node, func(node ast.Node) bool {
			assignment, ok := node.(*ast.AssignStmt)
			if !ok || len(assignment.Lhs) != 1 || len(assignment.Rhs) != 1 {
				return true
			}
			field, fieldOK := assignment.Lhs[0].(*ast.SelectorExpr)
			call, callOK := assignment.Rhs[0].(*ast.CallExpr)
			if !fieldOK || !callOK {
				return true
			}
			receiver, receiverOK := field.X.(*ast.Ident)
			if receiverOK && receiver.Name == "r" && field.Sel.Name == consumerField {
				if name, ok := call.Fun.(*ast.SelectorExpr); ok && transferExpr(name, decl.imports) == "github.com/nats-io/nuid.Next" {
					consumerNames[name] = true
				}
			}
			return true
		})
	}
	ast.Inspect(decl.node, func(node ast.Node) bool {
		var expr ast.Expr
		if call, ok := node.(*ast.CallExpr); ok && decl.path == "internal/jsstream/object_cleanup.go" && decl.name == "retainObjectWindow" {
			if fn, ok := call.Fun.(*ast.Ident); ok && fn.Name == "newObjectWindow" && len(call.Args) == 1 {
				expr = call.Args[0]
			}
		}
		if literal, ok := node.(*ast.CompositeLit); ok && decl.path == "internal/xferserve/link.go" && decl.name == "Link.ReceiverCapsule" && transferExpr(literal.Type, decl.imports) == modPrefix+"internal/xfercontract.SettlementCapsule" {
			for _, field := range literal.Elts {
				if pair, ok := field.(*ast.KeyValueExpr); ok && transferExpr(pair.Key, nil) == "Nonce" {
					expr = pair.Value
				}
			}
		}
		if call, ok := expr.(*ast.CallExpr); ok {
			if name, ok := call.Fun.(*ast.SelectorExpr); ok && transferExpr(name, decl.imports) == "github.com/nats-io/nuid.Next" {
				consumerNames[name] = true
			}
		}
		return true
	})
	directSeek := map[*ast.SelectorExpr]bool{}
	constants := map[string]ast.Expr{}
	ast.Inspect(decl.node, func(node ast.Node) bool {
		if call, ok := node.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
				directSeek[sel] = true
			}
		}
		if group, ok := node.(*ast.GenDecl); ok && group.Tok == token.CONST {
			for _, spec := range group.Specs {
				value := spec.(*ast.ValueSpec)
				for index, name := range value.Names {
					if index < len(value.Values) {
						constants[name.Name] = value.Values[index]
					}
				}
			}
		}
		return true
	})
	forbidden := false
	ast.Inspect(decl.node, func(node ast.Node) bool {
		switch value := node.(type) {
		case *ast.Ident:
			forbidden = forbidden || value.Name == "NewTransferID"
		case *ast.SelectorExpr:
			// SDK metadata lookup pins the original object on capable
			// pulls. The forbidden operation is the typed Put edge above,
			// not obtaining that read-only metadata handle.
			// origin: hot-update S7 plan §2.3 / §7 / T49
			forbidden = forbidden || value.Sel.Name == "Seek" && !directSeek[value]
			forbidden = forbidden || value.Sel.Name == "NewTransferID" || transferExpr(value, decl.imports) == "github.com/nats-io/nuid.Next" && !consumerNames[value]
		case *ast.CallExpr:
			if sel, ok := value.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Seek" && len(value.Args) > 0 {
				forbidden = forbidden || transferMaySeekZero(value.Args[0], constants, map[string]bool{})
			}
		}
		return true
	})
	return forbidden
}

// A method value or unknown offset can be invoked with zero later. Literal
// and local constant offsets retain precision; hex zero is still a rewind.
// origin: S7 internal review INTEG-G F5
func transferMaySeekZero(expr ast.Expr, constants map[string]ast.Expr, seen map[string]bool) bool {
	switch value := expr.(type) {
	case *ast.BasicLit:
		offset, err := strconv.ParseInt(value.Value, 0, 64)
		return err != nil || offset == 0
	case *ast.ParenExpr:
		return transferMaySeekZero(value.X, constants, seen)
	case *ast.UnaryExpr:
		return transferMaySeekZero(value.X, constants, seen)
	case *ast.Ident:
		if constants[value.Name] != nil && !seen[value.Name] {
			seen[value.Name] = true
			return transferMaySeekZero(constants[value.Name], constants, seen)
		}
	}
	return true
}

// Each permitted token allocation has a positive control and a business-ID
// mutation in that same declaration. A whole-function exemption would miss it.
// origin: S7 internal review INTEG-G F5
func TestTransferIdentityAllocationControls(t *testing.T) {
	for _, tc := range []struct{ path, name, source, token string }{
		{"internal/jsstream/object_resume.go", "transferResumeReader.openConsumer", `package jsstream;import "github.com/nats-io/nuid";type transferResumeReader struct{consumer string};func(r *transferResumeReader)openConsumer(){r.consumer=nuid.Next()}`, "r.consumer=nuid.Next()"},
		{"internal/jsstream/object_reader.go", "GetTransferObject", `package jsstream;import "github.com/nats-io/nuid";func GetTransferObject(){r.consumerName=nuid.Next()}`, "r.consumerName=nuid.Next()"},
		{"internal/jsstream/object_cleanup.go", "retainObjectWindow", `package jsstream;import "github.com/nats-io/nuid";func retainObjectWindow(){w=newObjectWindow(nuid.Next())}`, "newObjectWindow(nuid.Next())"},
		{"internal/xferserve/link.go", "Link.ReceiverCapsule", `package xferserve;import "github.com/nats-io/nuid";import x "github.com/LinZiyang666/tether/internal/xfercontract";type Link struct{};func(l *Link)ReceiverCapsule(){_ = x.SettlementCapsule{Nonce:nuid.Next()}}`, "Nonce:nuid.Next()"},
	} {
		for _, replacement := range []string{tc.token, "ObjectVersion:nuid.Next()"} {
			source := strings.Replace(tc.source, tc.token, replacement, 1)
			// Assignment sites retain assignment syntax; literal sites retain keys.
			if strings.Contains(tc.token, "=") {
				source = strings.Replace(tc.source, tc.token, "r.objectVersion=nuid.Next()", 1)
				if replacement == tc.token {
					source = tc.source
				}
			}
			if tc.name == "retainObjectWindow" && replacement != tc.token {
				source = strings.Replace(tc.source, tc.token, "newObjectVersion(nuid.Next())", 1)
			}
			all, err := transferDeclarations(map[string]string{tc.path: source})
			if err != nil {
				t.Fatal(err)
			}
			if got := transferMintsOrRewinds(all[tc.path+":"+tc.name]); got != (replacement != tc.token) {
				t.Fatalf("identity allocation precision at %s:%s: forbidden=%t source=%s", tc.path, tc.name, got, source)
			}
		}
	}
}

// origin: S7 internal review rv-wiring F10 / T49 / M01
func TestTransferTemporaryConsumerCannotReplaceObjectIdentity(t *testing.T) {
	for _, lhs := range []string{"r.consumer", "r.spec.Identity.ObjectVersion", "r.spec.Identity.TransferID", "r.consumer,r.spec.Identity.ObjectVersion"} {
		source := "package jsstream;func(r *transferResumeReader)openConsumer(){" + lhs + "=nuid.Next()}"
		file, err := parser.ParseFile(token.NewFileSet(), "reader.go", source, 0)
		if err != nil {
			t.Fatal(err)
		}
		target := transferDeclaration{path: "internal/jsstream/object_resume.go", name: "transferResumeReader.openConsumer", node: file.Decls[0], imports: map[string]string{"nuid": "github.com/nats-io/nuid"}}
		if transferMintsOrRewinds(target) != (lhs != "r.consumer") {
			t.Fatalf("temporary consumer exemption accepted object replacement: %s", lhs)
		}
	}
}

// origin: S7 internal review rv-wiring F10 / T49
func TestTransferCapsuleNonceCannotReplaceObjectIdentity(t *testing.T) {
	for _, entry := range []struct {
		value   string
		allowed bool
	}{
		{"x.SettlementCapsule{Nonce:nuid.Next()}", true},
		{"x.SettlementCapsule{ObjectVersion:nuid.Next()}", false},
		{"x.SettlementCapsule{Identity:x.Identity{ObjectVersion:nuid.Next()}}", false},
		{"x.SettlementCapsule{Identity:x.Identity{TransferID:nuid.Next()}}", false},
		{"x.Identity{Nonce:nuid.Next()}", false},
	} {
		file, err := parser.ParseFile(token.NewFileSet(), "link.go", "package xferserve;func(l *Link)ReceiverCapsule(){_="+entry.value+"}", 0)
		if err != nil {
			t.Fatal(err)
		}
		target := transferDeclaration{path: "internal/xferserve/link.go", name: "Link.ReceiverCapsule", node: file.Decls[0], imports: map[string]string{"nuid": "github.com/nats-io/nuid", "x": "github.com/LinZiyang666/tether/internal/xfercontract"}}
		if transferMintsOrRewinds(target) == entry.allowed {
			t.Fatalf("capsule nonce exemption accepted object identity: %s", entry.value)
		}
	}
}
