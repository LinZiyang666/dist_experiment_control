//go:build linux

// Supply helpers are exercised with inert fixtures;
// make is a failing shell function and no tether executable is run.
package hotupdate_test

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
)

// origin: S8 internal review rv-n1 F3
func TestArtifactStageRefusesFailedCurrentBuild(t *testing.T) {
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseArtifactFailedBuild(t)
	}
	testharness.AssertNoGoroutineLeak(t, "artifact failed build x5", g)
	testharness.AssertNoFDLeak(t, "artifact failed build x5", fd, 0)
}

func exerciseArtifactFailedBuild(t *testing.T) {
	t.Helper()
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	fixture := t.TempDir()
	for _, path := range []string{"test/simcluster/lib", "bin", "vendor", "scripts"} {
		if err := os.MkdirAll(filepath.Join(fixture, path), 0700); err != nil {
			t.Fatal(err)
		}
	}
	files := map[string]string{
		"test/simcluster/lib/stage-caddy.sh": "sim_stage_caddy() { return 0; }\n",
		"bin/tether":                         "stale candidate from a previous build\n",
		"scripts/install.sh":                 "inert installer fixture\n",
		"vendor/nats-server":                 "#!/bin/sh\nprintf 'nats-server: v2.14.0\\n'\n",
		"vendor/nats":                        "inert existing fixture\n",
		"vendor/nk":                          "inert existing fixture\n",
	}
	for path, body := range files {
		if err := os.WriteFile(filepath.Join(fixture, path), []byte(body), 0700); err != nil {
			t.Fatal(err)
		}
	}
	script := `
. "$1/test/simcluster/lib/stage.sh"
SIM_REPO=$2
SIM_VENDOR=$2/vendor
SIM_TAG=review
sim_nats_version() { printf 'v2.14.0\n'; }
make() { printf 'injected build failure\n' >&2; return 42; }
sim_stage_prev() { return 0; }
sim_stage_dynamic_prev() { return 0; }
sim_stage_flow_legacy() { return 0; }
sim_write_tether_source() { printf 'receipt published despite failed builds\n' > "$SIM_VENDOR/tether-source.json"; }
sim_stage_binaries_supervised
`
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "sh", "-c", script, "review", repo, fixture)
	out, runErr := testsupervisor.CombinedOutput(cmd)
	if runErr == nil {
		current, _ := os.ReadFile(filepath.Join(fixture, "vendor/tether"))
		next, _ := os.ReadFile(filepath.Join(fixture, "vendor/tether-next"))
		_, receiptErr := os.Stat(filepath.Join(fixture, "vendor/tether-source.json"))
		t.Fatalf("stage returned success after both builds failed; current=%q next=%q receipt_published=%t output=%s", current, next, receiptErr == nil, out)
	}
}

// origin: S8 internal review rv-n1 F4
func TestWireReceiptRejectsModifiedHistoricalSource(t *testing.T) {
	r := upgradeWireOracleReceipt("Q00B0", "exec")
	if err := judgeUpgradeWireReceipt(r, "Q00B0", "exec", 0); err != nil {
		t.Fatal(err)
	}
	for role, image := range r.Images {
		image.Artifact.DirtySHA = strings.Repeat("e", 64)
		image.Artifact.GOFlags = "-overlay=/fixture/replace-old-with-current.json -buildvcs=false"
		r.Images[role] = image
	}
	if err := judgeUpgradeWireReceipt(r, "Q00B0", "exec", 0); err == nil {
		t.Fatal("eight-quadrant oracle accepts modified old source as genuine v0.6.0")
	}
}

// origin: S8 internal review rv-n1 F4
func TestWireReceiptRequiresCompleteHistoricalIdentity(t *testing.T) {
	for _, verb := range []string{"exec", "kill"} {
		for _, defect := range []string{"component", "build-package", "build-flags", "embedded-flags", "goflags", "overlay", "dirty", "probe-source"} {
			t.Run(verb+"/"+defect, func(t *testing.T) {
				r := upgradeWireOracleReceipt("Q00B0", verb)
				role := "ctl"
				if verb == "kill" {
					role = "probe"
				}
				i := r.Images[role]
				switch defect {
				case "component":
					i.Artifact.Component = ""
				case "build-package":
					i.Artifact.BuildPackage = "./cmd/unrelated"
				case "build-flags":
					i.Artifact.BuildFlags = []string{"-trimpath"}
				case "embedded-flags":
					delete(i.Artifact.BuildInfo.Settings, "CGO_ENABLED")
				case "goflags":
					i.Artifact.GOFlags = ""
				case "overlay":
					i.Artifact.GOFlags = "-buildvcs=false -overlay=/fixture/historical.json"
				case "dirty":
					i.Artifact.DirtySHA = strings.Repeat("e", 64)
				case "probe-source":
					if verb != "kill" {
						return
					}
					i.Artifact.ProbeSourceSHA = ""
				}
				r.Images[role] = i
				if judgeUpgradeWireReceipt(r, "Q00B0", verb, 0) == nil {
					t.Fatal("incomplete or modified historical identity accepted")
				}
			})
		}
	}
}

// The following positive rows are synthetic oracle inputs only. Integration
// above reads independently collected product/source/proc execution receipts.
func upgradeWireOracleReceipt(cell, verb string) upgradeWireReceipt {
	r := upgradeWireReceipt{RunID: cell + "/" + verb + "/round-0", Row: upgradeWireExpected(cell, verb), NativeRC: 0, Effects: 1, Terminals: 1,
		Values: map[string]string{"HOME_CELL": "/fixture/home", "NATS_URL": "nats://127.0.0.1:14222", "NID": "node", "PID": "original", "NONCE": strings.Repeat("a", 32)}, Images: map[string]upgradeExecutedImage{}}
	bits := []byte{cell[1], cell[2], cell[4]}
	paths := []string{r.Row.CTLPath, r.Row.AgentPath, r.Row.BrokerPath}
	for i, role := range []string{"ctl", "agent", "broker"} {
		a := upgradePreviousOracleRecord()
		if bits[i] == '1' {
			a.Component = "tether-wire-new"
			a.Tag, a.Commit, a.BinarySHA = "s8", strings.Repeat("b", 40), strings.Repeat("b", 64)
		}
		argv := []string{paths[i], "version"}
		if role == "agent" {
			argv = []string{paths[i], "agent", "--session", "lab", "--nid", "node", "--nats-url", r.Values["NATS_URL"]}
		}
		if role == "broker" {
			argv = []string{paths[i], "serve", "--config", "/fixture/broker.yaml"}
		}
		r.Images[role] = upgradeExecutedImage{SchemaVersion: 1, Artifact: a, CopiedPath: paths[i], CopiedSHA: a.BinarySHA, RunningSHA: a.BinarySHA, ProcExe: paths[i], Argv: argv, PID: 100 + i, StartTicks: 200 + uint64(i), BootID: "boot", ObservedAt: "2026-10-12T12:00:00Z"}
	}
	r.Row.CTLSHA, r.Row.AgentSHA, r.Row.BrokerSHA, r.Row.SourceCommit = r.Images["ctl"].RunningSHA, r.Images["agent"].RunningSHA, r.Images["broker"].RunningSHA, r.Images["ctl"].Artifact.Commit
	r.Row.Argv = upgradeExpandArgv(r.Row.Argv, r.Values)
	command := r.Images["ctl"]
	if verb != "kill" {
		command.Argv = r.Row.Argv
	} else {
		command.Argv = upgradeExpandArgv(upgradeWireExpected(cell, "run").Argv, r.Values)
	}
	r.Images["ctl"] = command
	if verb == "kill" {
		probe := command
		probe.Argv = r.Row.Argv
		probe.CopiedPath, probe.ProcExe = r.Row.ProbePath, r.Row.ProbePath
		probe.Artifact.Component, probe.Artifact.BuildPackage = "upgrade-wire-probe-old", "./cmd/upgrade-wire-probe"
		if cell[1] == '1' {
			probe.Artifact.Component = "upgrade-wire-probe-new"
		}
		probe.Artifact.ProbeSourceSHA = strings.Repeat("d", 64)
		probe.Artifact.BinarySHA = strings.Repeat("c", 64)
		probe.CopiedSHA, probe.RunningSHA = probe.Artifact.BinarySHA, probe.Artifact.BinarySHA
		r.Row.ProbeSHA = probe.RunningSHA
		r.Images["probe"] = probe
	}
	nonce := r.Values["NONCE"]
	r.Output = []byte("wire-exec-" + nonce + "\n")
	if verb == "run" {
		r.Output = []byte("wire-pty-" + nonce + "\n")
		r.Input = []byte("echo\n")
		r.Echo = bytes.Clone(r.Input)
		r.ResizeObserved = true
	}
	r.OriginalPID, r.KilledPID, r.OriginalStartTicks, r.FinalStartTicks, r.ProcessReaped, r.OldCTLSIGINT = "original", "original", 99, 99, true, true
	r.PublicNonce = nonce
	common := []string{"--home", r.Values["HOME_CELL"], "--nats-url", r.Values["NATS_URL"]}
	r.CleanupArgv = append([]string{r.Row.CTLPath, "expose", "rm"}, common...)
	r.CleanupArgv = append(r.CleanupArgv, "--name", "wire-"+nonce, "node")
	r.PullArgv = append([]string{r.Row.CTLPath, "pull"}, common...)
	r.PullArgv = append(r.PullArgv, "--timeout", "120s", "node:/tmp/s8/"+nonce+"/dest", "/tmp/s8/"+nonce+"/back")
	r.SourceBytes, r.SourceDigest, r.DestinationDigest, r.BackDigest, r.Tier = 8<<20, strings.Repeat("d", 64), strings.Repeat("d", 64), strings.Repeat("d", 64), "B"
	return r
}

// origin: S8 internal review rv-n1 F4
// This independent supplier record must be retained before executing the cells.
// Equality binds every source/flag/component field, not only the binary hash.
type upgradeWireSupply struct {
	SchemaVersion int                               `json:"schema_version"`
	Rows          []upgradeWireRow                  `json:"rows"`
	Companions    map[string]json.RawMessage        `json:"companions"`
	Old           upgradeSupplierReceipt            `json:"old"`
	New           upgradeSupplierReceipt            `json:"new"`
	Probes        map[string]upgradeSupplierReceipt `json:"probes"`
	Execution     string                            `json:"execution"`
}

func upgradeHex(value string, size int) bool {
	b, err := hex.DecodeString(value)
	return err == nil && len(value) == size && len(b)*2 == size && value == strings.ToLower(value)
}

// origin: S8 internal review rv-n1 F4
func TestWireReceiptBindsIndependentSupplyManifest(t *testing.T) {
	for _, defect := range []string{"none", "missing-schema", "missing-row", "duplicate-row", "wrong-row", "source-tree", "build-flags", "archive", "component", "probe-source"} {
		t.Run(defect, func(t *testing.T) {
			var supply upgradeWireSupply
			supply.SchemaVersion, supply.Probes = 1, map[string]upgradeSupplierReceipt{}
			for _, cell := range upgradeWireCells() {
				for _, verb := range upgradeWireVerbs() {
					r := upgradeWireOracleReceipt(cell, verb)
					supply.Rows = append(supply.Rows, r.Row)
					for _, image := range r.Images {
						switch image.Artifact.Component {
						case "tether-prev":
							supply.Old = image.Artifact
						case "tether-wire-new":
							supply.New = image.Artifact
						default:
							supply.Probes[image.Artifact.Component] = image.Artifact
						}
					}
				}
			}
			if defect == "none" {
				for _, row := range supply.Rows {
					if err := judgeUpgradeWireSupply(upgradeWireOracleReceipt(row.Cell, row.Verb), supply); err != nil {
						t.Fatal(err)
					}
				}
				return
			}
			r := upgradeWireOracleReceipt("Q00B0", "exec")
			i := r.Images["ctl"]
			switch defect {
			case "missing-schema":
				supply.SchemaVersion = 0
			case "missing-row":
				supply.Rows = supply.Rows[1:]
			case "duplicate-row":
				supply.Rows[1] = supply.Rows[0]
			case "wrong-row":
				supply.Rows[0].CTLSHA = strings.Repeat("e", 64)
			case "source-tree":
				i.Artifact.TreeDigest = strings.Repeat("e", 64)
			case "build-flags":
				i.Artifact.GoToolchain = "go1.26.8"
			case "archive":
				i.Artifact.ArchiveSHA = strings.Repeat("e", 64)
			case "component":
				i.Artifact.Component = "unrelated"
			case "probe-source":
				r = upgradeWireOracleReceipt("Q00B0", "kill")
				probe := r.Images["probe"]
				probe.Artifact.ProbeSourceSHA = strings.Repeat("e", 64)
				r.Images["probe"] = probe
			}
			r.Images["ctl"] = i
			if judgeUpgradeWireSupply(r, supply) == nil {
				t.Fatal("execution metadata was not bound to independent supply")
			}
		})
	}
}

func judgeUpgradeWireSupply(r upgradeWireReceipt, supply upgradeWireSupply) error {
	if supply.SchemaVersion != 1 || len(supply.Rows) != 40 {
		return fmt.Errorf("wire supply manifest is incomplete")
	}
	matched := 0
	for _, row := range supply.Rows {
		if row.Cell == r.Row.Cell && row.Verb == r.Row.Verb {
			if !reflect.DeepEqual(row, r.Row) {
				return fmt.Errorf("executed row differs from supply manifest")
			}
			matched++
		}
	}
	if matched != 1 {
		return fmt.Errorf("wire supply manifest must bind exactly one executed row")
	}
	for role, image := range r.Images {
		bit := map[string]int{"ctl": 1, "agent": 2, "broker": 4, "probe": 1}[role]
		if len(r.Row.Cell) != 5 || bit == 0 {
			return fmt.Errorf("unknown wire supply role")
		}
		want := supply.New
		if r.Row.Cell[bit] == '0' {
			want = supply.Old
		}
		if role == "probe" {
			name := "upgrade-wire-probe-new"
			if r.Row.Cell[bit] == '0' {
				name = "upgrade-wire-probe-old"
			}
			want = supply.Probes[name]
		}
		if !reflect.DeepEqual(image.Artifact, want) {
			return fmt.Errorf("%s source differs from supply manifest", role)
		}
	}
	return nil
}

// These files are synthetic oracle controls, never execution receipts for N-1.
// origin: S8 internal review rv-n1 F4
func TestWireReceiptReaderRejectsUnboundSource(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "round-0"), 0700); err != nil {
		t.Fatal(err)
	}
	write := func(path string, value any) {
		t.Helper()
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	supply := upgradeWireSupply{SchemaVersion: 1, Probes: map[string]upgradeSupplierReceipt{}}
	for _, cell := range upgradeWireCells() {
		for _, verb := range upgradeWireVerbs() {
			r := upgradeWireOracleReceipt(cell, verb)
			supply.Rows = append(supply.Rows, r.Row)
			for _, image := range r.Images {
				switch image.Artifact.Component {
				case "tether-prev":
					supply.Old = image.Artifact
				case "tether-wire-new":
					supply.New = image.Artifact
				default:
					supply.Probes[image.Artifact.Component] = image.Artifact
				}
			}
			write("round-0/"+cell+"-"+verb+".json", r)
		}
	}
	write("wire-supply.json", supply)
	if err := readUpgradeWireEvidence(root, 0); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"ctl", "agent", "broker", "probe"} {
		r := upgradeWireOracleReceipt("Q00B0", "kill")
		i := r.Images[role]
		i.Artifact.TreeDigest = strings.Repeat("e", 64)
		r.Images[role] = i
		// The metadata remains individually well-formed: only an independent
		// source join can reject this source substitution with unchanged bytes.
		if err := judgeUpgradeWireReceipt(r, "Q00B0", "kill", 0); err != nil {
			t.Fatal(err)
		}
		write("round-0/Q00B0-kill.json", r)
		if err := readUpgradeWireEvidence(root, 0); err == nil {
			t.Fatalf("reader accepted unbound %s source", role)
		}
	}
}
