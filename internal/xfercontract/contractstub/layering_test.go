package contractstub_test

import (
	"fmt"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const contractModule = "github.com/LinZiyang666/tether/"

type contractLayerRule struct {
	leaf    string
	banned  []string
	stdOnly bool
}

// These are the sixteen P00 leaf consumers plus the neutral contract owners.
// Inspect the whole package closure: a legal direct alias can conceal an
// illegal dependency introduced by another file in its target package.
// origin: hot-update S7 plan §8.1 / §11.1 / P00
func contractLayerRules() []contractLayerRule {
	rules := []contractLayerRule{
		{leaf: "xfercontract"},
		{leaf: "xfercontract/xferport", stdOnly: true},
		// The value half is what proto, schema and the filesystem leaf import;
		// any NATS module in its closure re-breaks layering L-2 for all of them.
		// origin: hot-update S7 plan §11.1 / P00 — NATS-free value split
		{leaf: "xfercontract/xfervalue", banned: []string{"github.com/nats-io"}},
		{leaf: "jsstream", banned: []string{"internal/xferworker", "internal/xfertrack"}},
		{leaf: "transferpath", banned: []string{"internal/xferworker", "internal/xfertrack", "github.com/nats-io/nats.go"}},
		{leaf: "xferworker"},
		{leaf: "xfersend", banned: []string{"internal/xferworker", "internal/xfertrack"}},
		{leaf: "xferreceive", banned: []string{"internal/xferworker", "internal/xfertrack"}},
		{leaf: "xfercourier", banned: []string{"internal/xferworker", "internal/xfertrack"}},
		{leaf: "xfertrack"},
		{leaf: "xferfinal", banned: []string{"internal/xfertrack"}},
		{leaf: "xferledger"},
		{leaf: "xferhistory"},
		{leaf: "xferbucket"},
		{leaf: "xfergc", banned: []string{"internal/xfertrack"}},
		{leaf: "xfercontrol"},
		{leaf: "xferclient"},
		{leaf: "xferimage", banned: []string{"internal/xferworker", "internal/xfertrack", "internal/jsstream", "internal/transferpath"}},
		{leaf: "xferauth"},
	}
	for i := range rules {
		for j, banned := range rules[i].banned {
			if strings.HasPrefix(banned, "internal/") {
				rules[i].banned[j] = contractModule + banned
			}
		}
		rules[i].banned = append(rules[i].banned,
			contractModule+"internal/broker", contractModule+"internal/agent",
			contractModule+"cmd", contractModule+"test", contractModule+"internal/xfercontract/contractstub")
	}
	return rules
}

func checkContractLayering(graph map[string][]string, rules []contractLayerRule) error {
	if len(rules) == 0 {
		return fmt.Errorf("empty contract layering rules")
	}
	for _, rule := range rules {
		root := contractModule + "internal/" + rule.leaf
		if _, ok := graph[root]; !ok {
			return fmt.Errorf("missing contract layering root %s", root)
		}
		seen := map[string]bool{}
		var visit func(string, []string) error
		visit = func(pkg string, chain []string) error {
			if seen[pkg] {
				return nil
			}
			seen[pkg] = true
			chain = append(chain, pkg)
			if rule.stdOnly && pkg != root && strings.Contains(strings.Split(pkg, "/")[0], ".") {
				return fmt.Errorf("%s requires std-only imports: %s", root, strings.Join(chain, " -> "))
			}
			for _, banned := range rule.banned {
				if pkg == banned || strings.HasPrefix(pkg, banned+"/") {
					return fmt.Errorf("%s transitively reaches %s: %s", root, banned, strings.Join(chain, " -> "))
				}
			}
			deps, exists := graph[pkg]
			if strings.HasPrefix(pkg, contractModule) && !exists {
				return fmt.Errorf("missing local dependency %s", pkg)
			}
			for _, dep := range deps {
				if err := visit(dep, chain); err != nil {
					return err
				}
			}
			return nil
		}
		if err := visit(root, nil); err != nil {
			return err
		}
	}
	return nil
}

// AllOS source scanning keeps an unsupported-platform twin from concealing
// transport imports. No subprocess or platform-selected go-list graph is used.
func contractImportGraph(root string) (map[string][]string, error) {
	graph := map[string][]string{}
	err := filepath.WalkDir(filepath.Join(root, "internal"), func(path string, entry os.DirEntry, err error) error {
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
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		pkg := contractModule + filepath.ToSlash(rel)
		file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		if _, ok := graph[pkg]; !ok {
			graph[pkg] = nil
		}
		for _, imp := range file.Imports {
			dep, err := strconv.Unquote(imp.Path.Value)
			if err != nil {
				return err
			}
			graph[pkg] = append(graph[pkg], dep)
		}
		return nil
	})
	return graph, err
}

// origin: hot-update S7 plan §11.1 / P00 layering regression
// gate-control: TestTransferContractLayeringControls
func TestTransferContractLeafLayering(t *testing.T) {
	graph, err := contractImportGraph(contractRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	// Reconcile package identities with the signature manifest, rather than
	// trusting a rule count that could hide a duplicated or replaced leaf.
	want := map[string]bool{}
	for path := range frozenContracts {
		want[filepath.ToSlash(filepath.Dir(path))] = true
	}
	want["internal/xfercontract/xfervalue"] = true
	rules := contractLayerRules()
	for _, rule := range rules {
		path := "internal/" + rule.leaf
		if !want[path] {
			t.Fatalf("duplicate or unexpected contract layering root %s", path)
		}
		delete(want, path)
	}
	if len(want) != 0 {
		t.Fatalf("unruled contract layering roots %v", want)
	}
	if err := checkContractLayering(graph, rules); err != nil {
		t.Fatal(err)
	}
}

// The contract vocabulary is aliased far outside the sixteen leaves: proto (and
// through it cluster/state, tunnel/transport, agentstate, ptysession), the audit
// schema and its replay leaf. Each imported xfercontract itself once, and since
// ports.go names jetstream.JetStream every one of them reached the NATS client.
// They must reach the values only through xfervalue. The control re-adds the
// original edge and requires the gate to name the NATS path again.
// origin: hot-update S7 plan §6.2 / §11.1 / P00 — NATS-free value split
func TestTransferValueImportersStayNATSFree(t *testing.T) {
	graph, err := contractImportGraph(contractRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	nats := []string{"github.com/nats-io/nats.go"}
	var rules []contractLayerRule
	for _, leaf := range []string{"proto", "schema", "xferaudit", "transferpath", "xfercontract/xfervalue"} {
		rules = append(rules, contractLayerRule{leaf: leaf, banned: nats})
	}
	if err := checkContractLayering(graph, rules); err != nil {
		t.Fatal(err)
	}
	reachesPorts := false
	for _, dep := range graph[contractModule+"internal/xfercontract"] {
		reachesPorts = reachesPorts || strings.HasPrefix(dep, "github.com/nats-io/nats.go")
	}
	if !reachesPorts {
		t.Fatal("xfercontract no longer names a NATS type; this guard went vacuous — re-derive the split")
	}
	for _, rule := range rules[:3] {
		root := contractModule + "internal/" + rule.leaf
		regressed := make(map[string][]string, len(graph))
		for pkg, deps := range graph {
			regressed[pkg] = deps
		}
		regressed[root] = append(append([]string(nil), graph[root]...), contractModule+"internal/xfercontract")
		err := checkContractLayering(regressed, []contractLayerRule{rule})
		if err == nil || !strings.Contains(err.Error(), "internal/xfercontract -> github.com/nats-io/nats.go") {
			t.Fatalf("%s importing the port package was not reported as a NATS path: %v", rule.leaf, err)
		}
	}
}

// origin: hot-update S7 plan §11.1 / P00 adversarial controls
func TestTransferContractLayeringControls(t *testing.T) {
	rules := contractLayerRules()
	// 18 P00 roots plus xfercontract/xfervalue (NATS-free value split).
	if len(rules) != 19 {
		t.Fatal("contract layering census changed")
	}
	for _, rule := range rules {
		root := contractModule + "internal/" + rule.leaf
		bridge := contractModule + "internal/layerbridge"
		good := map[string][]string{root: {bridge}, bridge: {"os", "encoding/json"}}
		if rule.stdOnly {
			good[root] = []string{"os", "encoding/json"}
		}
		if err := checkContractLayering(good, []contractLayerRule{rule}); err != nil {
			t.Fatal(err)
		}
		targets := append([]string(nil), rule.banned...)
		if rule.stdOnly {
			targets = append(targets, "golang.org/x/sys/unix")
		}
		for _, target := range targets {
			for _, suffix := range []string{"", "/child"} {
				for _, indirect := range []bool{false, true} {
					bad := map[string][]string{root: {target + suffix}}
					if indirect {
						bad[root], bad[bridge] = []string{bridge}, []string{target + suffix}
					}
					if checkContractLayering(bad, []contractLayerRule{rule}) == nil {
						t.Fatalf("accepted %s -> %s (indirect=%t)", root, target+suffix, indirect)
					}
				}
			}
		}
	}
	for _, graph := range []map[string][]string{nil, {contractModule + "internal/transferpath": {contractModule + "internal/missing"}}} {
		if checkContractLayering(graph, rules) == nil {
			t.Fatal("accepted incomplete contract graph")
		}
	}
	if checkContractLayering(nil, nil) == nil {
		t.Fatal("accepted empty rule set")
	}
}

// Wire DTOs must remain usable by image/filesystem owners without inheriting
// the runtime ports' JetStream dependency through a seemingly neutral alias.
// origin: hot-update S7 plan §8.1 / INTEG-D2 wire value layering
func TestTransferWireIsTransportFree(t *testing.T) {
	graph, err := contractImportGraph(contractRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := checkContractLayering(graph, []contractLayerRule{{leaf: "proto", banned: []string{"github.com/nats-io/nats.go"}}}); err != nil {
		t.Fatal(err)
	}
}
