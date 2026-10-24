//go:build linux

// Real config parsers and cross-toolchain decoder executables are supplied by
// the private catalog. In-process codec/oracle controls have separate names;
// their success never signs an absent release or parser execution.
// origin: hot-update S8 plan §6.1 / §7.3 / P24
package hotupdate_test

import (
	"bytes"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding"
	"encoding/binary"
	"flag"
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
	"github.com/LinZiyang666/tether/internal/upgradecompat"
	"github.com/LinZiyang666/tether/internal/upgradecut"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	x "github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xferimage"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
)

var upgradeReaderCatalogPath = flag.String("upgrade-reader-catalog", "", "private NATS/Caddy parser and Go decoder catalog")

type upgradeExecutedImage struct {
	Artifact      upgradeSupplierReceipt `json:"artifact"`
	CopiedPath    string                 `json:"copied_path"`
	CopiedSHA     string                 `json:"copied_sha256"`
	ProcExe       string                 `json:"proc_exe"`
	RunningSHA    string                 `json:"running_sha256"`
	Argv          []string               `json:"argv"`
	PID           int                    `json:"pid"`
	StartTicks    uint64                 `json:"start_ticks"`
	BootID        string                 `json:"boot_id"`
	ObservedAt    string                 `json:"observed_at"`
	SchemaVersion int                    `json:"schema_version"`
}

type upgradeWireReceipt struct {
	Round              int                             `json:"round"`
	RunID              string                          `json:"run_id"`
	Row                upgradeWireRow                  `json:"row"`
	Values             map[string]string               `json:"values"`
	Images             map[string]upgradeExecutedImage `json:"images"`
	NativeRC           int                             `json:"native_rc"`
	Effects            int                             `json:"effects"`
	Terminals          int                             `json:"terminals"`
	Output             []byte                          `json:"output"`
	Input              []byte                          `json:"input"`
	Echo               []byte                          `json:"echo"`
	ResizeObserved     bool                            `json:"resize_observed"`
	OriginalPID        string                          `json:"original_pid"`
	KilledPID          string                          `json:"killed_pid"`
	OriginalStartTicks uint64                          `json:"original_start_ticks"`
	FinalStartTicks    uint64                          `json:"final_start_ticks"`
	ProcessReaped      bool                            `json:"process_reaped"`
	OldCTLSIGINT       bool                            `json:"old_ctl_sigint"`
	PublicNonce        string                          `json:"public_nonce"`
	CleanupArgv        []string                        `json:"cleanup_argv"`
	CleanupRC          int                             `json:"cleanup_rc"`
	PullArgv           []string                        `json:"pull_argv"`
	PullRC             int                             `json:"pull_rc"`
	SourceBytes        int64                           `json:"source_bytes"`
	SourceDigest       string                          `json:"source_digest"`
	DestinationDigest  string                          `json:"destination_digest"`
	BackDigest         string                          `json:"back_digest"`
	Tier               string                          `json:"tier"`
}

func upgradeExpandArgv(argv []string, values map[string]string) []string {
	result := append([]string(nil), argv...)
	for i := range result {
		// Substitute tokens in path/name operands too; never shell-eval an argv.
		for _, name := range []string{"HOME_CELL", "NATS_URL", "NID", "PID", "NONCE"} {
			result[i] = strings.ReplaceAll(result[i], name, values[name])
		}
	}
	return result
}

func judgeUpgradeExecutedImage(r upgradeExecutedImage, path, sha string, old bool) error {
	if r.SchemaVersion != 1 || r.CopiedPath != path || r.CopiedSHA != sha || r.RunningSHA != sha || r.Artifact.BinarySHA != sha || !upgradeHex(sha, 64) || r.PID <= 0 || r.StartTicks == 0 || r.BootID == "" || r.ProcExe == "" || len(r.Argv) < 2 || r.Argv[0] != path {
		return fmt.Errorf("fakeprev: missing source-copy-execute identity")
	}
	if _, err := time.Parse(time.RFC3339Nano, r.ObservedAt); err != nil {
		return fmt.Errorf("executable observation has no time")
	}
	a := r.Artifact
	if a.SchemaVersion != 2 || !upgradeHex(a.Commit, 40) || !upgradeHex(a.Tree, 40) || !upgradeHex(a.TreeDigest, 64) || !upgradeHex(a.ArchiveSHA, 64) || !upgradeHex(a.DirtySHA, 64) || a.Toolchain == "" || a.Toolchain != a.BuildInfo.Toolchain || len(a.BuildFlags) == 0 || a.SourceArchive == "" || a.BuiltAt == "" {
		return fmt.Errorf("executable source metadata is incomplete")
	}
	// origin: S8 internal review rv-n1 F4
	if old {
		if filepath.Base(path) == "upgrade-wire-probe" {
			if !upgradeHex(a.ProbeSourceSHA, 64) {
				return fmt.Errorf("old probe source identity is missing")
			}
			return validateUpgradePreviousSource(a, "upgrade-wire-probe-old", "./cmd/upgrade-wire-probe", sha, a.ArchiveSHA, a.Toolchain)
		}
		return validateUpgradePrevious(a, sha, a.ArchiveSHA, a.Toolchain)
	}
	if !old && a.Commit == upgradeFixedPreviousCommit {
		return fmt.Errorf("new quadrant used previous source")
	}
	return nil
}

func judgeUpgradeWireReceipt(r upgradeWireReceipt, cell, verb string, round int) error {
	if r.Round != round || r.RunID != fmt.Sprintf("%s/%s/round-%d", cell, verb, round) || r.Row.Cell != cell || r.Row.Verb != verb || r.NativeRC != 0 || r.Effects != 1 || r.Terminals != 1 {
		return fmt.Errorf("ordinary wire lacks one successful effect and terminal")
	}
	if len(r.Values) != 5 || !filepath.IsAbs(r.Values["HOME_CELL"]) || r.Values["NATS_URL"] == "" || r.Values["NID"] == "" || !upgradeHex(r.Values["NONCE"], 32) || r.Values["PID"] == "" {
		return fmt.Errorf("wire values do not identify an isolated sealed cell")
	}
	expected := upgradeWireExpected(cell, verb)
	expected.Argv = upgradeExpandArgv(expected.Argv, r.Values)
	expected.CTLSHA, expected.AgentSHA, expected.BrokerSHA, expected.ProbeSHA, expected.SourceCommit = r.Row.CTLSHA, r.Row.AgentSHA, r.Row.BrokerSHA, r.Row.ProbeSHA, r.Row.SourceCommit
	if !reflect.DeepEqual(r.Row, expected) {
		return fmt.Errorf("executed wire argv differs from frozen fixture")
	}
	roles := []string{"ctl", "agent", "broker"}
	paths := []string{r.Row.CTLPath, r.Row.AgentPath, r.Row.BrokerPath}
	shas := []string{r.Row.CTLSHA, r.Row.AgentSHA, r.Row.BrokerSHA}
	bits := []byte{cell[1], cell[2], cell[4]}
	if len(r.Images) != 3 && (verb != "kill" || len(r.Images) != 4) {
		return fmt.Errorf("quadrant omitted an actual role executable")
	}
	for i, role := range roles {
		if err := judgeUpgradeExecutedImage(r.Images[role], paths[i], shas[i], bits[i] == '0'); err != nil {
			return fmt.Errorf("%s: %w", role, err)
		}
	}
	agentArgv, brokerArgv := r.Images["agent"].Argv, r.Images["broker"].Argv
	flagValue := func(argv []string, flag string) string {
		for i := 1; i+1 < len(argv); i++ {
			if argv[i] == flag {
				return argv[i+1]
			}
		}
		return ""
	}
	if agentArgv[1] != "agent" || flagValue(agentArgv, "--session") == "" || flagValue(agentArgv, "--nid") != r.Values["NID"] || flagValue(agentArgv, "--nats-url") != r.Values["NATS_URL"] || len(brokerArgv) != 4 || brokerArgv[1] != "serve" || brokerArgv[2] != "--config" || !filepath.IsAbs(brokerArgv[3]) {
		return fmt.Errorf("actual agent/broker ExecStart differs from quadrant")
	}
	for i := range roles {
		for j := i + 1; j < len(roles); j++ {
			if (bits[i] == bits[j]) != (shas[i] == shas[j]) {
				return fmt.Errorf("role bytes do not match selected old/new artifact")
			}
		}
	}
	if r.Images["ctl"].Artifact.Commit != r.Row.SourceCommit {
		return fmt.Errorf("ctl source differs from wire schema")
	}
	commandOwner := r.Images["ctl"]
	if verb == "kill" {
		commandOwner = r.Images["probe"]
		if err := judgeUpgradeExecutedImage(commandOwner, r.Row.ProbePath, r.Row.ProbeSHA, cell[1] == '0'); err != nil {
			return err
		}
		if commandOwner.Artifact.Commit != r.Row.SourceCommit {
			return fmt.Errorf("probe schema differs from selected ctl")
		}
	}
	if !reflect.DeepEqual(commandOwner.Argv, r.Row.Argv) {
		return fmt.Errorf("process argv differs from command receipt")
	}
	nonce := r.Values["NONCE"]
	common := []string{"--home", r.Values["HOME_CELL"], "--nats-url", r.Values["NATS_URL"]}
	switch verb {
	case "exec":
		if string(r.Output) != "wire-exec-"+nonce+"\n" {
			return fmt.Errorf("exec nonce output differs")
		}
	case "run":
		if len(r.Input) == 0 || !bytes.Equal(r.Input, r.Echo) || !r.ResizeObserved || !bytes.Contains(r.Output, []byte("wire-pty-"+nonce)) {
			return fmt.Errorf("real PTY input/output/resize missing")
		}
	case "kill":
		if r.OriginalPID == "" || r.KilledPID != r.OriginalPID || r.KilledPID != r.Values["PID"] || r.OriginalStartTicks == 0 || r.FinalStartTicks != r.OriginalStartTicks || !r.ProcessReaped || (cell[1] == '0' && !r.OldCTLSIGINT) {
			return fmt.Errorf("kill did not terminate original child and exercise old sender")
		}
	case "expose":
		argv := append([]string{r.Row.CTLPath, "expose", "rm"}, common...)
		argv = append(argv, "--name", "wire-"+nonce, r.Values["NID"])
		if r.PublicNonce != nonce || r.CleanupRC != 0 || !reflect.DeepEqual(r.CleanupArgv, argv) {
			return fmt.Errorf("public expose exchange or same-ctl cleanup missing")
		}
	case "transfer":
		argv := append([]string{r.Row.CTLPath, "pull"}, common...)
		argv = append(argv, "--timeout", "120s", r.Values["NID"]+":/tmp/s8/"+nonce+"/dest", "/tmp/s8/"+nonce+"/back")
		if r.SourceBytes != 8<<20 || r.Tier != "B" || !upgradeHex(r.SourceDigest, 64) || r.SourceDigest != r.DestinationDigest || r.SourceDigest != r.BackDigest || r.PullRC != 0 || !reflect.DeepEqual(r.PullArgv, argv) {
			return fmt.Errorf("tier-B push/pull bytes or terminal differs")
		}
	default:
		return fmt.Errorf("unknown ordinary wire verb")
	}
	return nil
}

// origin: hot-update S8 plan §7.3 / P24 / fakeprev
func TestUpgradeWireReceiptOracleControls(t *testing.T) {
	for _, cell := range upgradeWireCells() {
		for _, verb := range upgradeWireVerbs() {
			if err := judgeUpgradeWireReceipt(upgradeWireOracleReceipt(cell, verb), cell, verb, 0); err != nil {
				t.Fatalf("%s/%s: %v", cell, verb, err)
			}
		}
	}
	for _, defect := range []string{"not-executed", "current-as-prev", "wrong-copy", "wrong-running-sha", "wrong-source", "missing-toolchain", "new-broker-all-cells", "wrong-agent-argv", "wrong-command-argv", "wrong-round", "no-effect", "duplicate-terminal", "wrong-exec-output"} {
		t.Run(defect, func(t *testing.T) {
			r := upgradeWireOracleReceipt("Q00B0", "exec")
			i := r.Images["ctl"]
			switch defect {
			case "not-executed":
				i.PID = 0
			case "current-as-prev":
				i.Artifact.Commit = strings.Repeat("b", 40)
				r.Row.SourceCommit = i.Artifact.Commit
			case "wrong-copy":
				i.CopiedSHA = strings.Repeat("f", 64)
			case "wrong-running-sha":
				i.RunningSHA = strings.Repeat("f", 64)
			case "wrong-source":
				r.Row.SourceCommit = strings.Repeat("f", 40)
			case "missing-toolchain":
				i.Artifact.Toolchain = ""
			case "new-broker-all-cells":
				r.Row.BrokerPath = "/opt/s8/new/tether"
			case "wrong-agent-argv":
				a := r.Images["agent"]
				a.Argv[1] = "version"
				r.Images["agent"] = a
			case "wrong-command-argv":
				i.Argv = []string{i.CopiedPath, "version"}
			case "wrong-round":
				r.Round++
			case "no-effect":
				r.Effects = 0
			case "duplicate-terminal":
				r.Terminals = 2
			case "wrong-exec-output":
				r.Output = []byte("wrong nonce")
			}
			r.Images["ctl"] = i
			if judgeUpgradeWireReceipt(r, "Q00B0", "exec", 0) == nil {
				t.Fatal("wire execution/provenance defect accepted")
			}
		})
	}
	for _, verb := range []string{"run", "kill", "expose", "transfer"} {
		for _, defect := range []string{"effect", "secondary-command"} {
			t.Run(verb+"/"+defect, func(t *testing.T) {
				r := upgradeWireOracleReceipt("Q00B0", verb)
				switch verb {
				case "run":
					if defect == "effect" {
						r.Echo = []byte("wrong")
					} else {
						r.ResizeObserved = false
					}
				case "kill":
					if defect == "effect" {
						r.KilledPID = "replacement"
					} else {
						r.OldCTLSIGINT = false
					}
				case "expose":
					if defect == "effect" {
						r.PublicNonce = "wrong"
					} else {
						r.CleanupArgv = nil
					}
				case "transfer":
					if defect == "effect" {
						r.BackDigest = strings.Repeat("f", 64)
					} else {
						r.PullArgv = nil
					}
				}
				if judgeUpgradeWireReceipt(r, "Q00B0", verb, 0) == nil {
					t.Fatal("ordinary wire effect defect accepted")
				}
			})
		}
	}
}

// The 40 ordinary commands have their own denominator. Policy refusals and
// the S2 mechanism fixture cannot replace one of these executable receipts.
// P22's previous binary alone cannot establish the live eight-cell effects.
// origin: hot-update S8 plan §7.3 / P24
// origin: S8 internal review s8-n1-evidence F1
func TestUpgradeWireEightQuadrants(t *testing.T) {
	if *upgradeCompatibilityEvidence == "" {
		// SETUP-RED, never a skip: a skipped receipt reads as a pass to the hard gates.
		// origin: hot-update S10 lead review of s8-n1-evidence (Skip-as-pass)
		t.Fatal("SETUP-RED: NOT_RUN pending S8 P22/P24/P26: real eight-quadrant source-copy-execute receipts are required (-upgrade-compatibility-evidence; hot-update S8 plan §7.3)")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUpgradeWireQuadrants(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "ordinary wire eight quadrants x5", g)
	testharness.AssertNoFDLeak(t, "ordinary wire eight quadrants x5", fd, 0)
}

func exerciseUpgradeWireQuadrants(t *testing.T, round int) {
	t.Helper()
	if *upgradeCompatibilityEvidence == "" {
		t.Fatal("NOT_RUN pending P13-P16/P19: supply real eight-quadrant P22 source-copy-execute receipts")
	}
	if err := readUpgradeWireEvidence(*upgradeCompatibilityEvidence, round); err != nil {
		t.Fatal(err)
	}
}

// origin: S8 internal review rv-n1 F4
func readUpgradeWireEvidence(root string, round int) error {
	var supply upgradeWireSupply
	if err := readUpgradeCompatibilityJSON(filepath.Join(root, "wire-supply.json"), &supply); err != nil {
		return err
	}
	for _, cell := range upgradeWireCells() {
		for _, verb := range upgradeWireVerbs() {
			var receipt upgradeWireReceipt
			if err := readUpgradeCompatibilityJSON(filepath.Join(root, fmt.Sprintf("round-%d", round), cell+"-"+verb+".json"), &receipt); err != nil {
				return err
			}
			if err := judgeUpgradeWireSupply(receipt, supply); err != nil {
				return err
			}
			if err := judgeUpgradeWireReceipt(receipt, cell, verb, round); err != nil {
				return fmt.Errorf("%s/%s: %w", cell, verb, err)
			}
		}
	}
	return nil
}

type upgradeParserBinary struct {
	Path         string `json:"path"`
	SHA256       string `json:"sha256"`
	Version      string `json:"version"`
	SourceCommit string `json:"source_commit"`
}
type upgradeReaderCatalog struct {
	NATS             []upgradeParserBinary `json:"nats"`
	Caddy            []upgradeParserBinary `json:"caddy"`
	NATSConfig       string                `json:"nats_config"`
	CaddyConfig      string                `json:"caddy_config"`
	GoTools          []string              `json:"go_tools"`
	WorkerCheckpoint string                `json:"worker_checkpoint"`
	WorkerSource     string                `json:"worker_source"`
}

func readUpgradeReaderCatalog(t *testing.T) upgradeReaderCatalog {
	t.Helper()
	if *upgradeReaderCatalogPath == "" {
		t.Fatal("NOT_RUN: private current/previous parser catalog and actual exported S7 checkpoint are missing (-upgrade-reader-catalog)")
	}
	var catalog upgradeReaderCatalog
	if err := readUpgradeCompatibilityJSON(*upgradeReaderCatalogPath, &catalog); err != nil {
		t.Fatal(err)
	}
	return catalog
}

func verifyUpgradeParserPair(t *testing.T, pair []upgradeParserBinary, kind string) {
	t.Helper()
	if len(pair) != 2 || pair[0].SHA256 == pair[1].SHA256 || pair[0].Version == pair[1].Version {
		t.Fatal("parser pair does not contain two distinct actual releases")
	}
	for _, binary := range pair {
		if !upgradePrivatePath(binary.Path) || !upgradeHex(binary.SHA256, 64) || !upgradeHex(binary.SourceCommit, 40) {
			t.Fatal("parser artifact provenance is incomplete")
		}
		if hotUpdateFileSHA(t, binary.Path) != binary.SHA256 {
			t.Fatal("parser artifact bytes differ from catalog")
		}
		args := []string{"--version"}
		mainPath := "github.com/nats-io/nats-server/v2"
		if kind == "caddy" {
			args = []string{"version"}
			mainPath = "github.com/caddyserver/caddy/v2/cmd/caddy"
		}
		info, err := buildinfo.ReadFile(binary.Path)
		if err != nil || info.Path != mainPath {
			t.Fatal("parser is not the stock component executable")
		}
		cmd := exec.CommandContext(t.Context(), binary.Path, args...)
		cmd.Env = hotUpdateEnvironment(t)
		out, err := testsupervisor.CombinedOutput(cmd)
		if err != nil || !bytes.Contains(out, []byte(binary.Version)) {
			t.Fatal("parser catalog version differs from the actual executable")
		}
	}
}

// Both binaries parse the exact same bytes, before any infrastructure runtime
// is started. Deliberately invalid bytes must fail each parser too: recording
// only a successful version command cannot pass this test.
// The catalog needs genuine component releases and actual rendered configs;
// tether-prev does not supply either stock NATS or stock Caddy executable.
// origin: hot-update S8 plan §6.1 / P24
// origin: S8 internal review s8-n1-evidence F2
func TestUpgradeConfigurationParsersCurrentAndPrevious(t *testing.T) {
	if *upgradeReaderCatalogPath == "" {
		// SETUP-RED, never a skip: a skipped receipt reads as a pass to the hard gates.
		// origin: hot-update S10 lead review of s8-n1-evidence (Skip-as-pass)
		t.Fatal("SETUP-RED: NOT_RUN pending S8 P22/P24/P26: genuine current/previous NATS/Caddy binaries and actual rendered configurations are required (-upgrade-reader-catalog; hot-update S8 plan §6.1)")
	}
	catalog := readUpgradeReaderCatalog(t)
	verifyUpgradeParserPair(t, catalog.NATS, "nats")
	verifyUpgradeParserPair(t, catalog.Caddy, "caddy")
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseUpgradeConfigurationParsers(t, catalog) })
	}
	testharness.AssertNoGoroutineLeak(t, "two-release config parsers x5", g)
	testharness.AssertNoFDLeak(t, "two-release config parsers x5", fd, 0)
}

func exerciseUpgradeConfigurationParsers(t *testing.T, c upgradeReaderCatalog) {
	t.Helper()
	for _, kind := range []string{"nats", "caddy"} {
		pair, source := c.NATS, c.NATSConfig
		if kind == "caddy" {
			pair, source = c.Caddy, c.CaddyConfig
		}
		if !upgradePrivatePath(source) {
			t.Fatal("rendered parser config must be private")
		}
		body, err := os.ReadFile(source)
		if err != nil || len(body) == 0 {
			t.Fatal("actual rendered configuration is missing")
		}
		path := filepath.Join(t.TempDir(), "config")
		if err := os.WriteFile(path, body, 0o600); err != nil {
			t.Fatal(err)
		}
		for _, binary := range pair {
			args := []string{"-t", "-c", path}
			if kind == "caddy" {
				args = []string{"validate", "--config", path}
			}
			cmd := exec.CommandContext(t.Context(), binary.Path, args...)
			cmd.Env, cmd.Dir = hotUpdateEnvironment(t), t.TempDir()
			if _, err := testsupervisor.CombinedOutput(cmd); err != nil {
				t.Fatalf("actual %s %s parser rejected rendered config: %v", kind, binary.Version, err)
			}
			badPath := filepath.Join(t.TempDir(), "invalid.config")
			if err := os.WriteFile(badPath, []byte("INVALID CONFIG TOKEN {{{"), 0o600); err != nil {
				t.Fatal(err)
			}
			args[len(args)-1] = badPath
			cmd = exec.CommandContext(t.Context(), binary.Path, args...)
			cmd.Env, cmd.Dir = hotUpdateEnvironment(t), t.TempDir()
			if _, err := testsupervisor.CombinedOutput(cmd); err == nil {
				t.Fatalf("actual %s parser did not parse invalid bytes", kind)
			}
		}
	}
}

func upgradeNonemptyAdjunct() upgradecut.State {
	key := upgradetx.Key{DomainID: strings.Repeat("1", 32), Component: upgradetx.ComponentBroker, ConfiguredNID: "broker", ConfigPath: "/fixture/broker.yaml"}
	owner := upgradetx.Owner{BootID: "11111111-2222-3333-4444-555555555555", PID: 123, StartTicks: 456, InstanceID: strings.Repeat("2", 32)}
	return upgradecut.State{Attempt: upgradetx.Handle{Key: key, TxID: strings.Repeat("3", 32), CutID: strings.Repeat("4", 32), Generation: 2, Revision: 1}, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), ParentToken: strings.Repeat("5", 32), Owner: owner,
		Authorities: []upgradecut.Authority{{SID: "session-a", Incarnation: strings.Repeat("c", 64)}, {SID: "session-b", Incarnation: strings.Repeat("d", 64)}},
		Scope:       upgradetx.Scope{ID: strings.Repeat("6", 32), Participants: []upgradetx.Key{key}, Domains: []upgradetx.Domain{upgradetx.DomainAuthority, upgradetx.DomainBrokerRuntime, upgradetx.DomainBrokerStore, upgradetx.DomainControl, upgradetx.DomainExec, upgradetx.DomainFlow, upgradetx.DomainPTY, upgradetx.DomainSS, upgradetx.DomainTransferTracker, upgradetx.DomainTransferWorker, upgradetx.DomainUpgrade}, Expected: []upgradetx.Resource{{Domain: upgradetx.DomainBrokerStore, ID: "original-store", Origin: "broker", Owner: owner, Generation: 1, State: "closed", Custody: "import"}}}}
}

// Decoder behavior is exercised independently of epoch/generation/release
// declarations. This control runs the real adjunct codec on a nonempty map;
// omitted/changed authority and missing required readers are separate refusals.
// origin: hot-update S8 plan §6.1 / §6.2 / P24
func TestUpgradeNonemptyAuthorityDecoder(t *testing.T) {
	state := upgradeNonemptyAdjunct()
	b, err := upgradecut.Encode(state)
	if err != nil {
		t.Fatal(err)
	}
	got, err := upgradecut.Decode(b)
	if err != nil || !reflect.DeepEqual(got, state) {
		t.Fatal("nonempty authority/resource adjunct did not decode")
	}
	for _, bad := range []upgradecut.State{func() upgradecut.State { s := upgradeNonemptyAdjunct(); s.Authorities[0].Incarnation = ""; return s }(), func() upgradecut.State {
		s := upgradeNonemptyAdjunct()
		s.Authorities = append(s.Authorities, s.Authorities[0])
		return s
	}()} {
		if _, err := upgradecut.Encode(bad); err == nil {
			t.Fatal("malformed authority map accepted")
		}
	}
	reader := upgradecompat.Reader{SHA256: state.SourceSHA, Release: "s8-a", Component: upgradetx.ComponentBroker, Epoch: 1, ReadEpochs: []uint64{1}, Domains: []string{"upgrade-cut-v1", "xfer-state-v1"}, Features: []string{"upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1"}}
	newReader := reader
	newReader.SHA256, newReader.Release = state.TargetSHA, "s8-b"
	if err := upgradecompat.CheckReaders(reader, newReader, reader, []string{"upgrade-cut-v1", "xfer-state-v1"}); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"epoch", "required-domain", "authority-reader"} {
		bad := reader
		switch field {
		case "epoch":
			bad.Epoch = 3
			bad.ReadEpochs = []uint64{1, 3}
		case "required-domain":
			bad.Domains = []string{"xfer-state-v1"}
		case "authority-reader":
			bad.Features = []string{"upgrade-tx-v1", "upgrade-cut-v1"}
		}
		if upgradecompat.CheckReaders(reader, newReader, bad, []string{"upgrade-cut-v1", "xfer-state-v1"}) == nil {
			t.Fatalf("previous reader accepted without %s", field)
		}
	}
}

// Catalog WorkerCheckpoint is the actual S7 final exported sealed worker
// payload copied into this private fixture. It must contain a nonzero synced
// prefix. Each compiler builds a decoder that calls the real S7 codec, then
// continues that SHA state with an identical suffix. Metadata alone cannot
// sign this behavior; using one toolchain twice is explicitly rejected.
// The live S7 export is a prerequisite; v0.6.0 has no S7 checkpoint decoder.
// origin: hot-update S8 plan §6.1 / P24
// origin: S8 internal review s8-n1-evidence F3
func TestUpgradeGoCheckpointDecoderContinuation(t *testing.T) {
	if *upgradeReaderCatalogPath == "" {
		// SETUP-RED, never a skip: a skipped receipt reads as a pass to the hard gates.
		// origin: hot-update S10 lead review of s8-n1-evidence (Skip-as-pass)
		t.Fatal("SETUP-RED: NOT_RUN pending S7 P28/P29 and S8 P22/P24/P26: actual exported S7 worker checkpoint, original source and two Go toolchains are required (-upgrade-reader-catalog; hot-update S8 plan §6.1)")
	}
	catalog := readUpgradeReaderCatalog(t)
	if len(catalog.GoTools) != 2 || !upgradePrivatePath(catalog.WorkerCheckpoint) {
		t.Fatal("two real Go toolchains and a private nonempty checkpoint are required")
	}
	data, err := os.ReadFile(catalog.WorkerCheckpoint)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := xferimage.DecodeWorker(data)
	if err != nil {
		t.Fatal(err)
	}
	state, offset := upgradeCheckpointHash(t, worker)
	if !upgradePrivatePath(catalog.WorkerSource) {
		t.Fatal("checkpoint source must be the private original transfer source")
	}
	source, err := os.ReadFile(catalog.WorkerSource)
	if err != nil || offset >= int64(len(source))-1 {
		t.Fatal("checkpoint must retain a nonempty prefix and suffix of the original source")
	}
	h := sha256.New()
	if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(state); err != nil {
		t.Fatal(err)
	}
	prefix := sha256.Sum256(source[:offset])
	if !bytes.Equal(h.Sum(nil), prefix[:]) {
		t.Fatal("exported checkpoint differs from the original source prefix")
	}
	expected := fmt.Sprintf("%x", sha256.Sum256(source))
	sealedDigest := ""
	if len(worker.Receivers) > 0 {
		sealedDigest = worker.Receivers[0].Reader.ObjectSpec.SHA256
	} else if len(worker.Senders) > 0 {
		sealedDigest = worker.Senders[0].SourceSeal.SHA256
	}
	if sealedDigest != expected {
		t.Fatal("original source differs from the S7 sealed full digest")
	}
	split := offset + (int64(len(source))-offset)/2
	suffix, tail := source[offset:split], source[split:]
	root, _ := filepath.Abs("../..")
	dir := t.TempDir()
	module := "module github.com/LinZiyang666/tether/test/decoderfixture\n\ngo 1.26.0\n\nrequire github.com/LinZiyang666/tether v0.0.0\nreplace github.com/LinZiyang666/tether => " + fmt.Sprintf("%q", root) + "\n"
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte(module), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "main.go"), []byte(upgradeGoDecoderSource), 0o600); err != nil {
		t.Fatal(err)
	}
	suffixPath := filepath.Join(dir, "suffix")
	tailPath := filepath.Join(dir, "tail")
	if err := os.WriteFile(tailPath, tail, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(suffixPath, suffix, 0o600); err != nil {
		t.Fatal(err)
	}
	var executables, versions []string
	for index, tool := range catalog.GoTools {
		if !filepath.IsAbs(tool) || strings.Contains(tool, "/.tether/") || filepath.Base(tool) != "go" {
			t.Fatal("decoder compiler must be an explicit Go toolchain")
		}
		binary := filepath.Join(dir, fmt.Sprintf("reader-%d", index))
		cmd := exec.CommandContext(t.Context(), tool, "build", "-mod=mod", "-o", binary, ".")
		cmd.Dir, cmd.Env = dir, append(hotUpdateEnvironment(t), "GOWORK=off", "GOTOOLCHAIN=local")
		if out, err := testsupervisor.CombinedOutput(cmd); err != nil {
			t.Fatalf("build actual decoder: %v %s", err, out)
		}
		info, err := buildinfo.ReadFile(binary)
		if err != nil {
			t.Fatal(err)
		}
		executables, versions = append(executables, binary), append(versions, info.GoVersion)
	}
	if versions[0] == versions[1] {
		t.Fatal("Go decoder evidence used the same toolchain twice")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseUpgradeGoCheckpoint(t, executables, versions, catalog.WorkerCheckpoint, suffixPath, tailPath, expected, offset, split, int64(len(source)))
	}
	testharness.AssertNoGoroutineLeak(t, "actual Go decoder continuation x5", g)
	testharness.AssertNoFDLeak(t, "actual Go decoder continuation x5", fd, 0)
}

func upgradeCheckpointHash(t *testing.T, worker x.WorkerState) ([]byte, int64) {
	t.Helper()
	if len(worker.Receivers) > 0 && worker.Receivers[0].Checkpoint.DurableOffset > 0 {
		return worker.Receivers[0].Checkpoint.SHA256State, worker.Receivers[0].Checkpoint.DurableOffset
	}
	if len(worker.Senders) > 0 && worker.Senders[0].Offset > 0 {
		return worker.Senders[0].SHA256State, worker.Senders[0].Offset
	}
	t.Fatal("S7 checkpoint has no actual nonempty exported prefix")
	return nil, 0
}

func exerciseUpgradeGoCheckpoint(t *testing.T, executables, versions []string, checkpoint, suffix, tail, digest string, offset, split, size int64) {
	t.Helper()
	// Each actual toolchain exports the intermediate prefix; the other imports
	// it and finishes the original source. Both directions must agree with the
	// independent whole-source digest, never with a copied output value.
	for i, exporter := range executables {
		statePath := filepath.Join(t.TempDir(), "checkpoint.state")
		cmd := exec.CommandContext(t.Context(), exporter, checkpoint, suffix, "", statePath)
		cmd.Env = hotUpdateEnvironment(t)
		if out, err := testsupervisor.CombinedOutput(cmd); err != nil || !bytes.HasPrefix(out, []byte(fmt.Sprintf("%s %d ", versions[i], split))) {
			t.Fatalf("actual checkpoint export failed: %v %s", err, out)
		}
		state, err := os.ReadFile(statePath)
		if err != nil || len(state) < 8 || binary.BigEndian.Uint64(state[len(state)-8:]) != uint64(split) || split <= offset {
			t.Fatal("actual exporter discarded the original nonempty prefix")
		}
		reader := 1 - i
		cmd = exec.CommandContext(t.Context(), executables[reader], checkpoint, tail, statePath, "")
		cmd.Env = hotUpdateEnvironment(t)
		out, err := testsupervisor.CombinedOutput(cmd)
		if err != nil || string(out) != fmt.Sprintf("%s %d %s\n", versions[reader], size, digest) {
			t.Fatalf("cross-toolchain original-source continuation differs: %v %s", err, out)
		}
	}
}

const upgradeGoDecoderSource = `package main
import("crypto/sha256";"encoding";"encoding/binary";"fmt";"os";"runtime";"github.com/LinZiyang666/tether/internal/xferimage")
func main(){
 if len(os.Args)!=5 {os.Exit(64)}
 b,e:=os.ReadFile(os.Args[1]);if e!=nil {os.Exit(74)}
 w,e:=xferimage.DecodeWorker(b);if e!=nil {os.Exit(64)}
 var state []byte;var offset int64
 if len(w.Receivers)>0 {state=w.Receivers[0].Checkpoint.SHA256State;offset=w.Receivers[0].Checkpoint.DurableOffset} else if len(w.Senders)>0 {state=w.Senders[0].SHA256State;offset=w.Senders[0].Offset}
 if offset<=0 {os.Exit(64)}
 if os.Args[3]!="" {state,e=os.ReadFile(os.Args[3]);if e!=nil || len(state)<8 {os.Exit(74)};offset=int64(binary.BigEndian.Uint64(state[len(state)-8:]))}
 h:=sha256.New();if h.(encoding.BinaryUnmarshaler).UnmarshalBinary(state)!=nil {os.Exit(64)}
 suffix,e:=os.ReadFile(os.Args[2]);if e!=nil || len(suffix)==0 {os.Exit(74)}
 h.Write(suffix);offset+=int64(len(suffix))
 if os.Args[4]!="" {state,e=h.(encoding.BinaryMarshaler).MarshalBinary();if e!=nil || os.WriteFile(os.Args[4],state,0600)!=nil {os.Exit(74)}}
 fmt.Printf("%s %d %x\n",runtime.Version(),offset,h.Sum(nil))
}
`
