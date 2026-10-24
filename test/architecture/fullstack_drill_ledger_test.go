package architecture

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fullstack_drill_ledger_test.go checks the declaration side of deployment
// acceptance through the same shell entry point as the hermetic gate suite.
// A passing declaration check provides no native execution receipt.
// origin: hot-update S10 plan §9 / §10.2 / P31

func fullstackDrillLedgerProblem(root string) error {
	cmd := exec.Command("sh", filepath.Join(root, "test/simcluster/tests/ledger-crosscheck.sh"), "--fullstack-only", root)
	raw, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("ledger crosscheck: %w: %s", err, raw)
	}
	if !strings.Contains(string(raw), "fullstack-ledger: OK (") {
		return fmt.Errorf("ledger crosscheck did not run the fullstack predicate: %s", raw)
	}
	return nil
}

// origin: hot-update S10 plan §10.2 / P31
// gate-control: TestFullstackDrillLedgerControls
func TestFullstackDrillLedgers(t *testing.T) {
	if err := fullstackDrillLedgerProblem(repoRoot(t)); err != nil {
		t.Fatal(err)
	}
}

// Copy the actual scanner and every input it reads. Controls never patch the
// working tree or let an in-memory replacement bypass the real file reader.
func fullstackDrillLedgerFixture(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	paths := []string{
		"test/simcluster/tests/ledger-crosscheck.sh",
		"test/simcluster/tests/assert-identity.baseline.tsv",
		"test/simcluster/tests/kept-sites.baseline.tsv",
		"test/simcluster/expected-verdicts.tsv",
		"test/simcluster/expected-verdicts-log.md",
		"test/simcluster/drill-costs.tsv",
		"test/simcluster/hot-update-fullstack-ledger.tsv",
		"test/simcluster/README.md",
		"test/simcluster/drills/lib/fullstack_manifest.json",
		"test/simcluster/drills/lib/fullstack_mutations.json",
	}
	drills, err := filepath.Glob(filepath.Join(repoRoot(t), "test/simcluster/drills/*.sh"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range drills {
		paths = append(paths, "test/simcluster/drills/"+filepath.Base(path))
	}
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(repoRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		transferWriteFixture(t, root, path, string(raw))
	}
	return root
}

// Each control proves baseline green -> on-disk defect red -> restored green.
// Exact error checks prevent a malformed fixture from impersonating a kill.
// origin: hot-update S10 plan §10.2 / P31
func TestFullstackDrillLedgerControls(t *testing.T) {
	const sim = "test/simcluster/"
	const ledger = sim + "hot-update-fullstack-ledger.tsv"
	const verdicts = sim + "expected-verdicts.tsv"
	const costs = sim + "drill-costs.tsv"
	mutations := []struct {
		name string
		edit fullstackMutation
	}{
		{"missing-positive-child", fullstackMutation{path: verdicts, old: "99-hot-update-fullstack.C1\tGREEN\t0\t-\t-\t99-hot-update-fullstack.C1\n", want: "missing expected child"}},
		{"missing-control-child", fullstackMutation{path: verdicts, old: "100-hot-update-fullstack-controls.M11K\tASSERT-FAIL\t0\t-\t-\t100-hot-update-fullstack-controls.M11K\n", want: "missing expected child"}},
		{"native3-to-green", fullstackMutation{path: verdicts, old: "39-hot-update-legacy-n1.BR\tPRODUCT-RED", new: "39-hot-update-legacy-n1.BR\tGREEN", want: "historical native3 changed"}},
		{"control-red-to-green", fullstackMutation{path: verdicts, old: "100-hot-update-fullstack-controls.M01A\tASSERT-FAIL", new: "100-hot-update-fullstack-controls.M01A\tGREEN", want: "native expectation differs"}},
		{"parent-double-count", fullstackMutation{path: costs, new: "99-hot-update-fullstack\t23500\tlong\tESTIMATE / NOT_MEASURED / NOT_RUN\n", want: "parent double counted"}},
		{"cost-whitelist", fullstackMutation{path: ledger, old: "# receipt-columns: candidate_id native_rc actual_wall_s peak_fd peak_rss_bytes peak_js_bytes raw_path", new: "# receipt-columns: candidate_id native_rc actual_wall_s peak_fd peak_rss_bytes peak_js_bytes raw_path estimate_s", want: "cost whitelist contains functional columns"}},
		{"missing-cost", fullstackMutation{path: costs, old: "99-hot-update-fullstack.C1\t2200\tlong\tS10 frozen estimate; NOT_MEASURED; NOT_RUN\n", want: "missing launch cost"}},
		{"missing-catalog", fullstackMutation{path: ledger, old: "# launch-catalog-v1\n", new: "# removed catalog\n", want: "missing launch catalog"}},
		{"catalog-parent", fullstackMutation{path: ledger, old: "quiet\t99-hot-update-fullstack\tC1\t", new: "quiet\t99-hot-update-fullstack\t-\t", want: "unexpected launch catalog unit or parent"}},
		{"catalog-native", fullstackMutation{path: ledger, old: "quiet\t99-hot-update-fullstack\tC1\ttether-T0-T5+nats-adjacent+caddy-adjacent\t-\tsingle\tyes\t0\t", new: "quiet\t99-hot-update-fullstack\tC1\ttether-T0-T5+nats-adjacent+caddy-adjacent\t-\tsingle\tyes\t3\t", want: "catalog native expectation differs"}},
		{"missing-kept-child", fullstackMutation{path: sim + "tests/kept-sites.baseline.tsv", old: "100-hot-update-fullstack-controls.M11K\t0\n", want: "missing kept child"}},
		{"missing-identity", fullstackMutation{path: sim + "tests/assert-identity.baseline.tsv", old: "100-hot-update-fullstack-controls._shared\tassert_ok\tfullstack source mutation hits its exact window and independent assertion with native 0 to 1 to 0\n", want: "missing assertion identities"}},
	}
	root := fullstackDrillLedgerFixture(t)
	// Derive complete row anchors from the copied real declarations. This keeps
	// controls independent of launch order and of upstream prose changes.
	raw, err := os.ReadFile(filepath.Join(root, ledger))
	if err != nil {
		t.Fatal(err)
	}
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "M01A\tsingle\t") {
			mutations = append(mutations, struct {
				name string
				edit fullstackMutation
			}{"missing-topology-cell", fullstackMutation{path: ledger, old: line + "\n", want: "missing mutation topology cell"}})
		}
		if strings.HasPrefix(line, "quiet\t99-hot-update-fullstack\tC1\t") {
			for _, variant := range []struct{ name, replacement, want string }{
				{"missing-catalog-child", "", "missing launch catalog child"},
				{"duplicate-catalog-child", line + "\n" + line + "\n", "duplicate launch catalog key"},
				{"synthetic-live-receipt", strings.TrimSuffix(line, "\t-") + "\t/fake/raw\n", "unsigned launch has synthetic receipt"},
				{"catalog-cost-drift", strings.Replace(line, "\t2200\t2400\t", "\t2201\t2400\t", 1) + "\n", "catalog launch cost differs"},
			} {
				mutations = append(mutations, struct {
					name string
					edit fullstackMutation
				}{variant.name, fullstackMutation{path: ledger, old: line + "\n", new: variant.replacement, want: variant.want}})
			}
		}
	}
	mutations = append(mutations, struct {
		name string
		edit fullstackMutation
	}{"same-count-identity-swap", fullstackMutation{path: sim + "tests/assert-identity.baseline.tsv", old: "fullstack source mutation hits its exact window and independent assertion with native 0 to 1 to 0", new: "fullstack prints a status", want: "assertion identity multiset differs"}})
	for _, mutation := range mutations {
		t.Run(mutation.name, func(t *testing.T) {
			if err := proveFullstackMutation(root, mutation.edit, func() error { return fullstackDrillLedgerProblem(root) }); err != nil {
				t.Fatal(err)
			}
			t.Logf("baseline green; on-disk defect red (%s); restored green", mutation.edit.want)
		})
	}
}
