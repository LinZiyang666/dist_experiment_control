package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/testharness"
)

// origin: S9 internal review rv-business F7
func TestInfraAcceptanceMatrixContainsEveryTaggedSuite(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	units, _, err := splitMatrices(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"infranats", "infracaddy", "infrabusiness", "infraprivilege"} {
		t.Run(name, func(t *testing.T) {
			for _, u := range units {
				if u.pkg == "./test/"+name+"/..." && u.tags == name+"_integration" && u.race && u.runFilter == "" && u.timeo == "600s" {
					return
				}
			}
			t.Fatalf("no whole -race tagged declaration for test/%s; suite is absent from the runner", name)
		})
	}
}

// Exercise the real direct entry points with a private go command recorder.
// It supplies only test names and exit codes; no infrastructure fixture is run
// or certified here. This catches a direct entry falling back to a cumulative
// run even when the parallel planner still partitions the same declaration.
// origin: S9 internal review rv-business F7; hot-update S9 plan §9.5 / T86.
func TestInfraTaggedMatricesPartitionDirectRuns(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	binary := filepath.Join(dir, "matrix.test")
	build := exec.CommandContext(t.Context(), "go", "test", "-c", "-race", "-tags", "e2e_matrix", "-o", binary, "./test/e2e")
	build.Dir = root
	var output bytes.Buffer
	build.Stdout, build.Stderr = &output, &output
	if err := testharness.RunProcess(build); err != nil {
		t.Fatalf("build direct matrix: %v\n%s", err, output.Bytes())
	}
	const recorder = `#!/bin/sh
printf '%s\n' "$*" >> "$INFRA_MATRIX_ARGS"
case " $* " in
  *" -list ^Test "*)
    case "$INFRA_MATRIX_CASE" in
      empty) exit 0 ;;
      listing-error) printf 'TestAlpha\n'; exit 1 ;;
    esac
    printf 'TestAlpha\nTestBeta\nok fixture\n'
    ;;
  *)
    if [ "$INFRA_MATRIX_CASE" = missing-supply ]; then
      printf 'SETUP-RED: missing dedicated supply\n'
      exit 1
    fi
    ;;
esac
`
	if err := os.WriteFile(filepath.Join(dir, "go"), []byte(recorder), 0o700); err != nil {
		t.Fatal(err)
	}
	// Set PATH only after compilation. The child is the matrix test binary,
	// and every nested go command is the recorder rather than the real suites.
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, suite := range []string{"NATS", "Caddy", "Business", "Privilege"} {
		for _, scenario := range []struct{ name, failure string }{
			{"complete", ""},
			{"empty", "infra matrix discovery contains no tests"},
			{"listing-error", "infra matrix discovery failed"},
			{"missing-supply", "SETUP-RED: missing dedicated supply"},
		} {
			t.Run(suite+"/"+scenario.name, func(t *testing.T) {
				log := filepath.Join(t.TempDir(), "argv")
				t.Setenv("INFRA_MATRIX_ARGS", log)
				t.Setenv("INFRA_MATRIX_CASE", scenario.name)
				cmd := exec.CommandContext(t.Context(), binary, "-test.run=^TestHotUpdateInfra"+suite+"Matrix$", "-test.timeout=30s", "-test.v")
				var output bytes.Buffer
				cmd.Stdout, cmd.Stderr = &output, &output
				err := testharness.RunProcess(cmd)
				if (err != nil) != (scenario.failure != "") || !strings.Contains(output.String(), scenario.failure) {
					t.Fatalf("direct matrix failure propagation: err=%v\n%s", err, output.Bytes())
				}
				name := "infra" + strings.ToLower(suite)
				base := "test -race -count=1 -timeout 600s -tags " + name + "_integration ./test/" + name + "/..."
				// origin: hot-update S9 plan §12 / P36; retain exact argv for the supplied-artifact witnesses.
				// origin: hot-update S10 plan §12.2 / TIDY-1; supply witnesses now live in each tagged suite.
				want := base + " -list ^Test\n"
				if scenario.name == "complete" || scenario.name == "missing-supply" {
					want += base + " -run ^TestAlpha$\n" + base + " -run ^TestBeta$\n"
				}
				got, err := os.ReadFile(log)
				if err != nil || string(got) != want {
					t.Fatalf("direct matrix lost exact tagged race partitions: err=%v\ngot %q\nwant %q", err, got, want)
				}
			})
		}
	}
}
