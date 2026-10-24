package e2e_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// origin: hot-update S5 plan §9.1 / §9.4 / T50 — declarations precede the
// first deployment run; checking them does not execute the deployment fixture.
const flowDrillManifest = `# arms: A C R B W L
# fixture: A=N1 C=N3-live R=N1 B=N1 W=N1 L=N1
# grows: A=0 C=2 R=0 B=0 W=0 L=0
# worst: A=420 C=600 R=420 B=540 W=660 L=420
# forgoes: A=- C=- R=- B=- W=- L=-
`

func flowDrillRegistrationProblems(manifest, verdicts, log, runAll string) []string {
	var problems []string
	for _, header := range strings.Split(strings.TrimSpace(flowDrillManifest), "\n") {
		key := strings.SplitN(header, ":", 2)[0] + ":"
		count := 0
		for _, line := range strings.Split(manifest, "\n") {
			if strings.HasPrefix(line, key) {
				count++
				if line != header {
					problems = append(problems, "flow manifest changed: "+line)
				}
			}
		}
		if count != 1 {
			problems = append(problems, fmt.Sprintf("flow manifest %s: expected one header, got %d", key, count))
		}
	}
	h0 := "hot-update-plan H0 legacy exporter; S8 legacy-to-capable first migration"
	h1 := "hot-update-plan H1 legacy PTY exporter; S8 legacy-to-capable first migration"
	h2 := "hot-update-plan H2 legacy flow exporter; S8 legacy-to-capable first migration"
	// The parent now aggregates the independent native legacy broker arm too.
	// Existing PTY/flow verdicts keep their exact identities and columns.
	// origin: hot-update S6 plan §10.2 / §9.3 / HARDGATE-A
	hbr := "hot-update-plan S6 legacy broker Run unwind; S8 legacy-to-capable first migration"
	want := map[string]string{}
	for _, suffix := range []string{"", ".A", ".C", ".R", ".B", ".W", ".L"} {
		id := "75-hot-update-flows" + suffix
		want[id] = id + "\tGREEN\t0\t-\t-\t" + id
	}
	for _, suffix := range []string{"", ".A", ".W", ".C", ".L"} {
		id := "36-hot-update-pty" + suffix
		want[id] = id + "\tGREEN\t0\t-\t-\t" + id
	}
	for suffix, owner := range map[string]string{"": h0 + "; " + h1 + "; " + h2 + "; " + hbr, ".Y": h0, ".P": h1, ".F": h2, ".BR": hbr} {
		id := "39-hot-update-legacy-n1" + suffix
		want[id] = id + "\tPRODUCT-RED\t0\t-\t" + owner + "\t" + id
	}
	want["39-hot-update-legacy-n1.X"] = "39-hot-update-legacy-n1.X\tGREEN\t0\t-\t-\t39-hot-update-legacy-n1.X"
	// The shared legacy drill now includes S8's full compatibility surface.
	// Every registered row needs the same deletion and column-shape protection
	// as the historical arms; discovering expectations from verdicts hides loss.
	// origin: hot-update S8 plan §7 / S10 internal review MAKETEST-GATES F2
	for _, quadrant := range []string{"00B0", "01B0", "10B0", "11B0", "00B1", "01B1", "10B1", "11B1"} {
		for _, verb := range []string{"exec", "run", "kill", "expose", "transfer"} {
			id := "39-hot-update-legacy-n1.Q" + quadrant + "-" + verb
			want[id] = id + "\tGREEN\t0\t-\t-\t" + id
		}
	}
	for _, suffix := range []string{"LB-new", "LB-old", "LI-defer", "LI-interrupting", "M-route", "CC-idle", "CC-late-create", "CL-reject", "ABI-adjacent", "ABI-too-far", "ABI-unknown", "GO-hash", "CFG-nats-old", "CFG-nats-new", "CFG-caddy-old", "CFG-caddy-new", "NI", "CI"} {
		id, verdict := "39-hot-update-legacy-n1."+suffix, "GREEN"
		if suffix == "LI-interrupting" {
			verdict = "INCOMPLETE"
		}
		want[id] = id + "\t" + verdict + "\t0\t-\t-\t" + id
	}
	for id, row := range want {
		count := 0
		for _, line := range strings.Split(verdicts, "\n") {
			if strings.SplitN(line, "\t", 2)[0] == id {
				count++
				if line != row {
					problems = append(problems, "verdict registration changed: "+id)
				}
			}
		}
		if count != 1 {
			problems = append(problems, fmt.Sprintf("%s: expected one six-column verdict row, got %d", id, count))
		}
		if strings.Count("\n"+log, "\n## "+id+"\n") != 1 {
			problems = append(problems, "missing or duplicate note-ref: "+id)
		}
	}
	for _, name := range []string{"hot-update-flow-probe-test", "hot-update-flow-drill-test"} {
		count := 0
		for _, line := range strings.Split(runAll, "\n") {
			if !strings.HasPrefix(line, "for t in ") || !strings.HasSuffix(line, "; do") {
				continue
			}
			for _, word := range strings.Fields(strings.TrimSuffix(strings.TrimPrefix(line, "for t in "), "; do")) {
				if word == name {
					count++
				}
			}
		}
		if count != 1 {
			problems = append(problems, "hermetic gate missing or duplicated: "+name)
		}
	}
	return problems
}

// origin: hot-update S5 plan §9 / §10.1 — manifest and verdict identities are
// checked before the six arms can claim a complete matrix receipt.
func TestFlowSSDrillRegistration(t *testing.T) {
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(source), "..", "simcluster")
	read := func(name string) string {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(root, name))
		if err != nil {
			t.Fatalf("read registration input %s: %v", name, err)
		}
		return string(b)
	}
	problems := flowDrillRegistrationProblems(read("drills/75-hot-update-flows.sh"), read("expected-verdicts.tsv"), read("expected-verdicts-log.md"), read("tests/run-all.sh"))
	if len(problems) != 0 {
		t.Fatal(strings.Join(problems, "\n"))
	}
}

// origin: hot-update S5 plan §10.1 / T50 — deleting each header, row,
// note-ref or hermetic entry must fail through the live registration predicate.
func TestFlowSSManifestControls(t *testing.T) {
	if len(strings.Split(strings.TrimSpace(flowDrillManifest), "\n")) != 5 {
		t.Fatal("control must retain all five frozen manifest headers")
	}
	_, source, _, _ := runtime.Caller(0)
	root := filepath.Join(filepath.Dir(source), "..", "simcluster")
	verdicts, err := os.ReadFile(filepath.Join(root, "expected-verdicts.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(filepath.Join(root, "expected-verdicts-log.md"))
	if err != nil {
		t.Fatal(err)
	}
	runAll, err := os.ReadFile(filepath.Join(root, "tests/run-all.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if problems := flowDrillRegistrationProblems(flowDrillManifest, string(verdicts), string(log), string(runAll)); len(problems) != 0 {
		t.Fatalf("control baseline: %s", strings.Join(problems, "; "))
	}
	for _, header := range strings.Split(strings.TrimSpace(flowDrillManifest), "\n") {
		bad := strings.Replace(flowDrillManifest, header+"\n", "", 1)
		if len(flowDrillRegistrationProblems(bad, string(verdicts), string(log), string(runAll))) == 0 {
			t.Errorf("deleted header escaped: %s", header)
		}
		bad = strings.Replace(flowDrillManifest, header, header+" unexpected", 1)
		if len(flowDrillRegistrationProblems(bad, string(verdicts), string(log), string(runAll))) == 0 {
			t.Errorf("changed header escaped: %s", header)
		}
		bad = flowDrillManifest + header + "\n"
		if len(flowDrillRegistrationProblems(bad, string(verdicts), string(log), string(runAll))) == 0 {
			t.Errorf("duplicate header escaped: %s", header)
		}
	}
	for _, line := range strings.Split(string(verdicts), "\n") {
		if !strings.HasPrefix(line, "75-hot-update-flows") && !strings.HasPrefix(line, "39-hot-update-legacy-n1") {
			continue
		}
		bad := strings.Replace(string(verdicts), line+"\n", "", 1)
		if len(flowDrillRegistrationProblems(flowDrillManifest, bad, string(log), string(runAll))) == 0 {
			t.Errorf("deleted verdict row escaped: %s", line)
		}
		bad = strings.Replace(string(verdicts), line, line+"\textra", 1)
		if len(flowDrillRegistrationProblems(flowDrillManifest, bad, string(log), string(runAll))) == 0 {
			t.Errorf("non-six-column verdict row escaped: %s", line)
		}
	}
	for _, name := range []string{"hot-update-flow-probe-test", "hot-update-flow-drill-test"} {
		bad := strings.Replace(string(runAll), name+" ", "", 1)
		if len(flowDrillRegistrationProblems(flowDrillManifest, string(verdicts), string(log), bad)) == 0 {
			t.Errorf("deleted hermetic entry escaped: %s", name)
		}
	}
	badLog := strings.Replace(string(log), "## 75-hot-update-flows.W\n", "", 1)
	if len(flowDrillRegistrationProblems(flowDrillManifest, string(verdicts), badLog, string(runAll))) == 0 {
		t.Error("deleted watchdog note-ref escaped")
	}
}
