package architecture

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// These are declaration roots, never grep witnesses. The row file records the
// owner/test mapping independently; deleting a row cannot delete an obligation.
// Controls use copied production sources on disk, so overlays cannot hide a
// broken source reader. No product binary or privileged action runs here.
// origin: hot-update S9 plan §2 / §10.2 / T82
var infraLeafRoots = []string{
	"infracontract", "infraartifact", "infrahost", "infrapriv", "infralaunch",
	"ingressswitch", "caddyconfig", "infraupgrade", "infrareadiness",
	"infratransport", "ingressmove", "inframaint", "infraauth", "infrarepair",
	"infratransfer", "infraattach", "infracli", "infraroot",
	// origin: hot-update S9 plan §10.2 / T82; composition remains in the actual leaf ownership scan.
	"infracompose",
}

func infraOwner(path string) bool {
	pkg := filepath.ToSlash(filepath.Dir(path))
	for _, leaf := range infraLeafRoots {
		if pkg == "internal/"+leaf || strings.HasPrefix(pkg, "internal/"+leaf+"/") {
			return true
		}
	}
	return pkg == "cmd/tether-infra-helper" || pkg == "cmd/tether-infra-launch"
}

// Pin only the neutral declarations. Implementations and comments may change;
// a field, tag, enum or signature change must return to P00 for review.
func infraDeclarationDigest(source string) (string, error) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "contract.go", source, 0)
	if err != nil {
		return "", err
	}
	var out bytes.Buffer
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok {
			copy := *fn
			copy.Body = nil
			decl = &copy
		}
		if err := printer.Fprint(&out, token.NewFileSet(), decl); err != nil {
			return "", err
		}
		out.WriteByte('\n')
	}
	return fmt.Sprintf("%x", sha256.Sum256(out.Bytes())), nil
}

var infraFrozenDeclarations = map[string]string{
	"internal/infracontract/types.go":      "36005c0012b1b136deccb71f4ef41087a2d9649ce375366ad079854fc698917a",
	"internal/infracontract/interfaces.go": "e72ee8e2ef2e6a9e797960ff7b7b1e2a81034e3bb136d135c446130eb326b540",
	"internal/infracontract/host.go":       "0f5d8707daac82999cfb45c506849992111023d6fb50ee0e08fa646e5c95d10f",
}

func checkInfraContracts(sources map[string]string) error {
	for _, path := range brokerSortedSourcePaths(infraFrozenDeclarations) {
		digest, err := infraDeclarationDigest(sources[path])
		if err != nil || digest != infraFrozenDeclarations[path] {
			return fmt.Errorf("%s: frozen S9 declaration drift (%s): %v", path, digest, err)
		}
	}
	for _, path := range brokerSortedSourcePaths(sources) {
		if !isProdGoFile(path) || strings.HasPrefix(path, "internal/infracontract/contractstub/") {
			continue
		}
		file, err := brokerParse(path, sources[path])
		if err != nil {
			return err
		}
		for _, imp := range brokerImports(file) {
			if layerDependencyMatches(imp, modPrefix+"internal/infracontract/contractstub") {
				return fmt.Errorf("%s: shipping code imports S9 contract witness", path)
			}
		}
		var failure error
		ast.Inspect(file, func(node ast.Node) bool {
			if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				value, _ := strconv.Unquote(lit.Value)
				if strings.Contains(value, "not implemented (S9 P") {
					failure = fmt.Errorf("%s: shipping S9 failure stub", path)
				}
			}
			return true
		})
		if failure != nil {
			return failure
		}
	}
	return nil
}

// origin: hot-update S9 plan §2.1 / §10.2 / T82
// gate-control: TestInfraContractGateControls
func TestInfraContractDeclarationsStayFrozen(t *testing.T) {
	sources := brokerGateSources(t, repoRoot(t))
	for _, path := range brokerSortedSourcePaths(infraFrozenDeclarations) {
		digest, err := infraDeclarationDigest(sources[path])
		t.Logf("frozen declaration %s %s (%v)", path, digest, err)
	}
	if err := checkInfraContracts(sources); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §11 / T82 / M31
func TestInfraContractGateControls(t *testing.T) {
	root := transferFixtureTree(t, map[string]string{})
	for path := range infraFrozenDeclarations {
		raw, err := os.ReadFile(filepath.Join(repoRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		transferWriteFixture(t, root, path, string(raw))
	}
	read := func() map[string]string { return brokerGateSources(t, root) }
	if err := checkInfraContracts(read()); err != nil {
		t.Fatal(err)
	}
	for path := range infraFrozenDeclarations {
		original := read()[path]
		transferWriteFixture(t, root, path, original+"\n// a comment does not change the contract\n")
		if err := checkInfraContracts(read()); err != nil {
			t.Fatal(err)
		}
		transferWriteFixture(t, root, path, original+"\ntype UnexpectedContract uint32\n")
		if checkInfraContracts(read()) == nil {
			t.Fatalf("on-disk declaration addition escaped: %s", path)
		}
		transferWriteFixture(t, root, path, original)
	}

	for _, mutation := range []struct{ path, old, new string }{
		{"internal/infracontract/types.go", "int32", "int64"},
		{"internal/infracontract/host.go", "type HostOp uint8", "type HostOp uint16"},
		{"internal/infracontract/interfaces.go", "Do(HostRequest, Deadline, Stop)", "Do(HostRequest, Deadline)"},
	} {
		original := read()[mutation.path]
		changed := strings.Replace(original, mutation.old, mutation.new, 1)
		if changed == original {
			t.Fatalf("contract mutation did not apply %s", mutation.old)
		}
		transferWriteFixture(t, root, mutation.path, changed)
		if checkInfraContracts(read()) == nil {
			t.Fatalf("on-disk frozen field/signature mutation escaped %s", mutation.path)
		}
		transferWriteFixture(t, root, mutation.path, original)
	}
	for _, source := range []string{
		`package owner; import _ "github.com/LinZiyang666/tether/internal/infracontract/contractstub"`,
		`package owner; var failure = "not implemented (S9 P22)"`,
	} {
		transferWriteFixture(t, root, "internal/infraattach/owner_other.go", source)
		if checkInfraContracts(read()) == nil {
			t.Fatal("on-disk shipping failure stub escaped")
		}
	}
	transferWriteFixture(t, root, "internal/infraattach/owner_other.go", "package owner // not implemented (S9 P22) is prose\n")
	transferWriteFixture(t, root, "internal/infracontract/contractstub/stub.go", `package contractstub; var failure = "not implemented (S9 P00)"`)
	if err := checkInfraContracts(read()); err != nil {
		t.Fatal(err)
	}
	t.Run("documentation", TestInfraDocumentationReceiptControls)
}

// P38 owns the literal scope-to-claim mapping in its documentation. P39 owns
// native receipts. Require each exit separately: one child receipt cannot sign
// all five exits, and neither a source path nor NOT_RUN is completion evidence.
// origin: hot-update S9 plan §13 / T90
const infraClaimsStart = "<!-- s9-claims:start -->"
const infraClaimsEnd = "<!-- s9-claims:end -->"

var infraClaimRow = regexp.MustCompile(`^\| (E[1-5]) \| (.*) \| ([^|]+) \|$`)
var infraMarkdownLink = regexp.MustCompile(`\]\(([^)#]+)(?:#([^)]*))?\)`)

// Validate recorded commands as data. Never execute a documentation promise:
// doing so could cross from a hermetic gate into a privileged deployment.
// origin: hot-update S9 plan §10.2 / T90
func checkInfraReceiptCommand(root, command string) error {
	words := strings.Fields(command)
	if len(words) < 2 {
		return fmt.Errorf("empty S9 receipt command")
	}
	if words[0] == "go" && words[1] == "test" {
		packages := 0
		for _, word := range words[2:] {
			if !strings.HasPrefix(word, "./") {
				continue
			}
			path := strings.TrimSuffix(strings.TrimRight(word, "'\""), "/...")
			info, err := os.Stat(filepath.Join(root, path))
			if err != nil || !info.IsDir() {
				return fmt.Errorf("dead S9 receipt test package %s", word)
			}
			packages++
		}
		if packages == 0 {
			return fmt.Errorf("S9 receipt test command has no actual package root")
		}
		return nil
	}
	if words[0] == "make" && infraContains([]string{"test", "e2e-parallel", "lint", "gates"}, words[1]) {
		return nil
	}
	for _, word := range words {
		if strings.HasPrefix(word, "test/simcluster/") && strings.HasSuffix(word, ".sh") {
			if _, err := os.Stat(filepath.Join(root, word)); err != nil {
				return fmt.Errorf("dead S9 receipt script %s", word)
			}
			return nil
		}
	}
	return fmt.Errorf("unregistered S9 receipt command %s", command)
}

func checkInfraDocumentation(root string, documents []string) error {
	if len(documents) == 0 {
		return fmt.Errorf("empty S9 documentation roots")
	}
	seenExits := map[string]bool{}
	for _, path := range documents {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return fmt.Errorf("NOT_RUN pending P38: %s: %w", path, err)
		}
		doc := string(raw)
		if strings.Count(doc, infraClaimsStart) != 1 || strings.Count(doc, infraClaimsEnd) != 1 || strings.Index(doc, infraClaimsStart) >= strings.Index(doc, infraClaimsEnd) {
			return fmt.Errorf("NOT_RUN pending P38: %s needs exact S9 claim boundaries", path)
		}
		claims := strings.Split(strings.Split(doc, infraClaimsStart)[1], infraClaimsEnd)[0]
		outside := strings.Split(doc, infraClaimsStart)[0] + strings.Split(doc, infraClaimsEnd)[1]
		for _, line := range strings.Split(outside, "\n") {
			if strings.Contains(line, "S9") && (strings.Contains(line, "[x]") || strings.Contains(line, "[X]")) {
				return fmt.Errorf("%s: checked S9 claim outside receipt scope", path)
			}
		}
		rows := 0
		for _, line := range strings.Split(claims, "\n") {
			line = strings.TrimSpace(line)
			m := infraClaimRow.FindStringSubmatch(line)
			if m == nil {
				if strings.Contains(line, "[x]") || strings.Contains(line, "[X]") {
					return fmt.Errorf("%s: unregistered checked S9 claim", path)
				}
				continue
			}
			rows++
			if seenExits[m[1]] {
				return fmt.Errorf("duplicate S9 exit claim %s", m[1])
			}
			seenExits[m[1]] = true
			checked := strings.HasPrefix(m[2], "[x] ") || strings.HasPrefix(m[2], "[X] ")
			if !checked && !strings.HasPrefix(m[2], "[ ] ") {
				return fmt.Errorf("%s: claim needs explicit completion state", m[1])
			}
			if m[3] == "NOT_RUN" {
				if checked {
					return fmt.Errorf("%s: checked S9 claim lacks native receipt", m[1])
				}
				continue
			}
			receipt, err := os.ReadFile(filepath.Join(root, m[3]))
			if err != nil {
				return fmt.Errorf("%s: missing native receipt", m[1])
			}
			if err := checkTransferNativeReceipt(root, receipt, m[1]); err != nil {
				return err
			}
			var identity transferNativeReceipt
			if err := json.Unmarshal(receipt, &identity); err != nil {
				return err
			}
			if err := checkInfraReceiptCommand(root, identity.Command); err != nil {
				return err
			}
			// All signed exits must describe one candidate, including its dirty
			// digest. A second candidate's green cannot repair an earlier red.
			key := identity.SourceSHA + ":" + identity.DirtyDigest
			if seenExits["candidate"] && !seenExits[key] {
				return fmt.Errorf("S9 claims mix candidate identities")
			}
			seenExits["candidate"], seenExits[key] = true, true
		}
		if rows == 0 {
			return fmt.Errorf("%s: empty S9 claim roots", path)
		}
		for _, m := range infraMarkdownLink.FindAllStringSubmatch(claims, -1) {
			if strings.Contains(m[1], "://") || strings.HasPrefix(m[1], "#") {
				continue
			}
			if _, err := os.Stat(filepath.Join(root, filepath.Dir(path), m[1])); err != nil {
				return fmt.Errorf("%s: dead S9 source/command link %s", path, m[1])
			}
			if strings.HasSuffix(m[1], ".go") && m[2] != "" && !regexp.MustCompile(`^L[0-9]+$`).MatchString(m[2]) {
				target := filepath.Clean(filepath.Join(filepath.Dir(path), m[1]))
				raw, err := os.ReadFile(filepath.Join(root, target))
				if err != nil {
					return err
				}
				declarations, err := transferDeclarations(map[string]string{target: string(raw)})
				if err != nil {
					return err
				}
				if _, exists := declarations[target+":"+m[2]]; !exists {
					return fmt.Errorf("dead S9 source declaration %s:%s", target, m[2])
				}
			}
		}
	}
	for _, exit := range []string{"E1", "E2", "E3", "E4", "E5"} {
		if !seenExits[exit] {
			return fmt.Errorf("missing S9 scope claim %s", exit)
		}
	}
	return nil
}

// origin: hot-update S9 plan §11 / T90
// gate-control: TestInfraDocumentationReceiptControls
func TestInfraDocumentationClaimsNeedReceipts(t *testing.T) {
	if err := checkInfraDocumentation(repoRoot(t), []string{"docs/reviews/hot-update-tasklist.md"}); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S9 plan §11 / T90 / M31
func TestInfraDocumentationReceiptControls(t *testing.T) {
	root := transferFixtureTree(t, map[string]string{})
	path := "docs/infra-claims.md"
	var rows []string
	for _, exit := range []string{"E1", "E2", "E3", "E4", "E5"} {
		rows = append(rows, "| "+exit+" | [ ] pending | NOT_RUN |")
	}
	sort.Strings(rows)
	good := infraClaimsStart + "\n" + strings.Join(rows, "\n") + "\n" + infraClaimsEnd
	write := func(doc string) { transferWriteFixture(t, root, path, doc) }
	check := func() error { return checkInfraDocumentation(root, []string{path}) }
	write(good)
	if err := check(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{
		strings.Replace(good, "[ ] pending", "[x] completed", 1),
		strings.Replace(good, infraClaimsEnd, "", 1),
		strings.Replace(good, rows[0], rows[1], 1),
		strings.Replace(good, rows[0], "", 1),
		strings.Replace(good, "pending", "[source](missing.go)", 1),
		strings.Replace(good, "NOT_RUN", "missing-receipt.json", 1),
		strings.Replace(good, infraClaimsStart, infraClaimsEnd, 1),
		strings.Replace(good, infraClaimsEnd, "- [x] S9 complete\n"+infraClaimsEnd, 1),
	} {
		write(bad)
		if check() == nil {
			t.Fatal("on-disk documentation mutation escaped")
		}
	}
	write(good)
	if err := check(); err != nil {
		t.Fatal(err)
	}
	transferWriteFixture(t, root, "raw/oracle.txt", "native S9 oracle\n")
	transferWriteFixture(t, root, "test/infranats/receipt_test.go", "package receipt\n")
	rc := 0
	receipt := transferNativeReceipt{
		SourceSHA: strings.Repeat("a", 40), DirtyDigest: strings.Repeat("b", 64),
		ArtifactSHA: strings.Repeat("c", 64), RunningSHA: strings.Repeat("c", 64),
		TestOrArm: "E1", Command: "go test -race ./test/infranats -run '^TestNATSStockNonemptySingleCycle$'",
		NativeRC: &rc, MutationID: "M31", RawPath: "raw/oracle.txt",
		OracleInputSHA: transferDigest([]byte("native S9 oracle\n")), ReaderDigest: strings.Repeat("d", 64), WallSeconds: 1,
	}
	writeReceipt := func(exit string, r transferNativeReceipt) {
		r.TestOrArm = exit
		raw, err := json.Marshal(r)
		if err != nil {
			t.Fatal(err)
		}
		transferWriteFixture(t, root, "raw/"+exit+".json", string(raw))
	}
	signed := good
	for _, exit := range []string{"E1", "E2", "E3", "E4", "E5"} {
		signed = strings.Replace(signed, "| "+exit+" | [ ] pending | NOT_RUN |", "| "+exit+" | [x] verified | raw/"+exit+".json |", 1)
		writeReceipt(exit, receipt)
	}
	write(signed)
	if err := check(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*transferNativeReceipt){
		func(r *transferNativeReceipt) { r.SourceSHA = strings.Repeat("f", 40) },
		func(r *transferNativeReceipt) { r.NativeRC = nil },
		func(r *transferNativeReceipt) { red := 4; r.NativeRC = &red },
		func(r *transferNativeReceipt) { r.RunningSHA = strings.Repeat("e", 64) },
		func(r *transferNativeReceipt) { r.RawPath = "raw/missing" },
		func(r *transferNativeReceipt) { r.OracleInputSHA = strings.Repeat("e", 64) },
		func(r *transferNativeReceipt) { r.ReaderDigest = "" },
		func(r *transferNativeReceipt) { r.WallSeconds = 0 },
		func(r *transferNativeReceipt) { r.Command = "go test ./test/missing" },
	} {
		bad := receipt
		mutate(&bad)
		writeReceipt("E2", bad)
		if check() == nil {
			t.Fatal("on-disk S9 native/candidate receipt mutation escaped")
		}
		writeReceipt("E2", receipt)
	}
	write("- [x] S9 complete\n" + signed)
	if check() == nil {
		t.Fatal("unregistered S9 completion escaped receipt scope")
	}
	write(signed)
	if err := check(); err != nil {
		t.Fatal(err)
	}
}
