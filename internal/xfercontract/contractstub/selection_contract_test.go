package contractstub_test

import (
	"bytes"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"testing"

	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/xfercontrol"
)

// An absent source path keeps the frozen capability-only request. A zero size
// pointer is a real empty file, distinct from an unanswered source inspection.
// The amendment cannot silently approve a change to any original declaration.
// origin: S10 internal review MAKETEST-GATES F5 / S7 plan §2 / §6.1 / §7
func TestTransferPullSelectionAmendmentPreservesFrozenContracts(t *testing.T) {
	path := filepath.Join(contractRoot(t), "internal/xfercontrol/contracts.go")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, raw, 0)
	if err != nil {
		t.Fatal(err)
	}
	var original []ast.Decl
	removed := map[string]int{}
	for _, decl := range file.Decls {
		if group, ok := decl.(*ast.GenDecl); ok && len(group.Specs) == 1 {
			if typ, ok := group.Specs[0].(*ast.TypeSpec); ok &&
				(typ.Name.Name == "PullSelectionProbeReq" || typ.Name.Name == "PullSelectionProbeResp") {
				removed[typ.Name.Name]++
				continue
			}
		}
		original = append(original, decl)
	}
	if removed["PullSelectionProbeReq"] != 1 || removed["PullSelectionProbeResp"] != 1 {
		t.Fatal("source-selection amendment is missing or duplicated")
	}
	file.Decls = original
	var source bytes.Buffer
	if err := printer.Fprint(&source, fset, file); err != nil {
		t.Fatal(err)
	}
	got, err := contractDigest(source.Bytes())
	if err != nil || got != "49fdaaa85ab274a4e59816cf1ecfb5c27f99a51ff66f45a4773d4673a38ffeb6" {
		t.Fatalf("original P00 declarations changed beyond the additive selection DTOs: %s %v", got, err)
	}
	for _, pair := range [][2]any{
		{xfercontrol.PullSelectionProbeReq{CallID: "call", SID: "sid", NID: "nid"}, proto.TransferProbeReq{CallID: "call", SID: "sid", NID: "nid"}},
		{xfercontrol.PullSelectionProbeResp{Version: proto.TransferVersion}, proto.TransferProbeResp{Version: proto.TransferVersion}},
	} {
		newBytes, newErr := json.Marshal(pair[0])
		oldBytes, oldErr := json.Marshal(pair[1])
		if newErr != nil || oldErr != nil || !bytes.Equal(newBytes, oldBytes) {
			t.Fatalf("absent selection fields changed frozen probe bytes: %s != %s (%v, %v)", newBytes, oldBytes, newErr, oldErr)
		}
	}
	size := int64(0)
	raw, err = json.Marshal(xfercontrol.PullSelectionProbeResp{SourceSize: &size})
	var decoded xfercontrol.PullSelectionProbeResp
	if err != nil || json.Unmarshal(raw, &decoded) != nil || decoded.SourceSize == nil || *decoded.SourceSize != 0 {
		t.Fatal("empty source file lost its answered inspection")
	}
}
