//go:build linux

// Ordinary wire receipts require actual old/new executables in all eight cells.
// The old-ctl policy test runs the original executable against a real broker
// and original agent. Oracle controls never certify a product receipt.
// origin: hot-update S8 plan §7 / P24
package hotupdate_test

import (
	"bytes"
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/cli"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/testharness"
	"github.com/LinZiyang666/tether/internal/upgradetx"
	"github.com/LinZiyang666/tether/test/hotupdate/testsupervisor"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

var upgradePreviousArtifact = flag.String("upgrade-previous-artifact", "", "P22 private v0.6.0 artifact (with adjacent -source.json)")
var upgradeCompatibilityEvidence = flag.String("upgrade-compatibility-evidence", "", "private P22/P26 execution receipts")

const upgradeFixedPreviousCommit = "a3431a12be3cf947cc797c28fea93382fa68472d"

type upgradeWireRow struct {
	Cell           string   `json:"cell"`
	Verb           string   `json:"verb"`
	Argv           []string `json:"argv"`
	CTLPath        string   `json:"ctl_path"`
	CTLSHA         string   `json:"ctl_sha"`
	AgentPath      string   `json:"agent_path"`
	AgentSHA       string   `json:"agent_sha"`
	BrokerPath     string   `json:"broker_path"`
	BrokerSHA      string   `json:"broker_sha"`
	ProbePath      string   `json:"probe_path"`
	ProbeSHA       string   `json:"probe_sha"`
	SourceCommit   string   `json:"source_commit"`
	ExpectedEffect string   `json:"expected_effect"`
}

func upgradeWireCells() []string {
	return []string{"Q00B0", "Q01B0", "Q10B0", "Q11B0", "Q00B1", "Q01B1", "Q10B1", "Q11B1"}
}
func upgradeWireVerbs() []string { return []string{"exec", "run", "kill", "expose", "transfer"} }

func upgradeWireExpected(cell, verb string) upgradeWireRow {
	path := func(bit byte, name string) string {
		generation := "old"
		if bit == '1' {
			generation = "new"
		}
		return "/opt/s8/" + generation + "/" + name
	}
	row := upgradeWireRow{Cell: cell, Verb: verb, CTLPath: path(cell[1], "tether"), AgentPath: path(cell[2], "tether"), BrokerPath: path(cell[4], "tether"), CTLSHA: "CTL_SHA", AgentSHA: "AGENT_SHA", BrokerSHA: "BROKER_SHA", SourceCommit: "SOURCE_COMMIT"}
	common := []string{"--home", "HOME_CELL", "--nats-url", "NATS_URL"}
	row.Argv = append([]string{row.CTLPath}, verb)
	row.Argv = append(row.Argv, common...)
	switch verb {
	case "exec":
		row.Argv = append(row.Argv, "--timeout", "30s", "NID", "--", "/bin/sh", "-c", `printf "wire-exec-%s\n" "$1"`, "sh", "NONCE")
		row.ExpectedEffect = "exact nonce output and execution count=1"
	case "run":
		row.Argv = append(row.Argv, "NID", "--", "/bin/sh", "-c", `printf "wire-pty-%s\n" "$1"; read x; printf "%s\n" "$x"`, "sh", "NONCE")
		row.ExpectedEffect = "real PTY input/output/resize and terminal"
	case "kill":
		row.ProbePath, row.ProbeSHA = path(cell[1], "upgrade-wire-probe"), "PROBE_SHA"
		row.Argv = append([]string{row.ProbePath}, common...)
		row.Argv = append(row.Argv, "--verb", "kill", "--nid", "NID", "--pid", "PID", "--signal", "15")
		row.ExpectedEffect = "original run child exits with one terminal"
	case "expose":
		row.Argv = append(row.Argv, "--local", "18080", "--name", "wire-NONCE", "--json", "NID")
		row.ExpectedEffect = "public-port nonce exchange and cleanup"
	case "transfer":
		row.Argv[1] = "push"
		row.Argv = append(row.Argv, "--timeout", "120s", "/tmp/s8/NONCE/source", "NID:/tmp/s8/NONCE/dest")
		row.ExpectedEffect = "8MiB tier-B push/pull hash and unique terminals"
	}
	return row
}

func readUpgradeWireTemplates() ([]upgradeWireRow, error) {
	var rows []upgradeWireRow
	err := readUpgradeCompatibilityJSON("testdata/upgrade_wire_argv.json", &rows)
	return rows, err
}

func validateUpgradeWireTemplates(rows []upgradeWireRow) error {
	if len(rows) != 40 {
		return fmt.Errorf("wire denominator must be eight cells times five verbs")
	}
	index := 0
	for _, cell := range upgradeWireCells() {
		for _, verb := range upgradeWireVerbs() {
			if !reflect.DeepEqual(rows[index], upgradeWireExpected(cell, verb)) {
				return fmt.Errorf("wire template differs at %s/%s", cell, verb)
			}
			index++
		}
	}
	return nil
}

// Exact argv arrays include the broker bit, all flags before NID and the real
// push/probe commands; no invented kill/transfer top-level command is allowed.
// origin: hot-update S8 plan §7.3 / P24
func TestUpgradeWireArgvManifest(t *testing.T) {
	rows, err := readUpgradeWireTemplates()
	if err != nil {
		t.Fatal(err)
	}
	if err := validateUpgradeWireTemplates(rows); err != nil {
		t.Fatal(err)
	}
}

// origin: hot-update S8 plan §7.3 / P24 / fakeprev control
func TestUpgradeWireArgvManifestControls(t *testing.T) {
	rows, err := readUpgradeWireTemplates()
	if err != nil {
		t.Fatal(err)
	}
	for _, defect := range []string{"missing-cell", "duplicate-cell", "new-broker-everywhere", "fake-kill", "fake-transfer", "flags-after-node", "missing-pull-obligation", "same-ctl-all-cells"} {
		t.Run(defect, func(t *testing.T) {
			bad := append([]upgradeWireRow(nil), rows...)
			for i := range bad {
				bad[i].Argv = append([]string(nil), bad[i].Argv...)
			}
			switch defect {
			case "missing-cell":
				bad = bad[5:]
			case "duplicate-cell":
				copy(bad[5:10], bad[:5])
			case "new-broker-everywhere":
				bad[0].BrokerPath = "/opt/s8/new/tether"
			case "fake-kill":
				bad[2].Argv = []string{bad[2].CTLPath, "kill", "NID"}
			case "fake-transfer":
				bad[4].Argv[1] = "transfer"
			case "flags-after-node":
				bad[0].Argv[2], bad[0].Argv[8] = bad[0].Argv[8], bad[0].Argv[2]
			case "missing-pull-obligation":
				bad[4].ExpectedEffect = "push succeeded"
			case "same-ctl-all-cells":
				bad[10].CTLPath = "/opt/s8/old/tether"
			}
			if validateUpgradeWireTemplates(bad) == nil {
				t.Fatal("wire defect accepted")
			}
		})
	}
}

func readUpgradeCompatibilityJSON(path string, value any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	d := json.NewDecoder(io.LimitReader(f, 32<<20))
	d.DisallowUnknownFields()
	if err := d.Decode(value); err != nil {
		return err
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		return fmt.Errorf("compatibility receipt has trailing data")
	}
	return nil
}

func upgradePrivatePath(path string) bool {
	if !filepath.IsAbs(path) {
		return false
	}
	clean := filepath.Clean(path)
	for _, part := range strings.Split(clean, string(filepath.Separator)) {
		if part == ".tether" {
			return false
		}
	}
	if clean == "/home/weiland/.local/bin/tether" || strings.HasPrefix(clean, "/usr/local/bin/") || strings.HasPrefix(clean, "/usr/bin/") {
		return false
	}
	if filepath.Base(clean) == "tether" {
		for _, entry := range filepath.SplitList(os.Getenv("PATH")) {
			directory, err := filepath.Abs(entry)
			if err == nil && directory == filepath.Dir(clean) {
				return false
			}
		}
	}
	// Reject aliases before opening payloads. Walking one component at a time
	// never follows a private symlink into the production home or installed bin.
	prefix := string(filepath.Separator)
	for _, part := range strings.Split(strings.TrimPrefix(clean, prefix), prefix) {
		prefix = filepath.Join(prefix, part)
		info, err := os.Lstat(prefix)
		if err != nil || info.Mode()&os.ModeSymlink != 0 {
			return false
		}
	}
	return true
}

type upgradeSupplierReceipt struct {
	SchemaVersion int      `json:"schema_version"`
	Component     string   `json:"component"`
	Tag           string   `json:"tag"`
	Commit        string   `json:"commit"`
	Tree          string   `json:"tree"`
	TreeDigest    string   `json:"tree_digest"`
	ArchiveSHA    string   `json:"archive_sha256"`
	DirtySHA      string   `json:"dirty_diff_sha256"`
	GOOS          string   `json:"goos"`
	GOARCH        string   `json:"goarch"`
	Version       string   `json:"version"`
	BuildFlags    []string `json:"build_flags"`
	GOFlags       string   `json:"goflags"`
	GoToolchain   string   `json:"gotoolchain"`
	BuildPackage  string   `json:"build_package"`
	BinarySHA     string   `json:"binary_sha256"`
	Toolchain     string   `json:"toolchain"`
	BuildInfo     struct {
		Toolchain string            `json:"toolchain"`
		Settings  map[string]string `json:"settings"`
	} `json:"build_info"`
	SourceArchive  string `json:"source_archive"`
	BuiltAt        string `json:"built_at"`
	ProbeSourceSHA string `json:"probe_source_sha256,omitempty"`
}

func validateUpgradePrevious(r upgradeSupplierReceipt, binarySHA, archiveSHA, actualToolchain string) error {
	return validateUpgradePreviousSource(r, "tether-prev", "./cmd/tether", binarySHA, archiveSHA, actualToolchain)
}

// origin: S8 internal review rv-n1 F4
// The old-schema probe shares the clean release contract with its own exact component/package identity; it cannot masquerade as the old product binary.
func validateUpgradePreviousSource(r upgradeSupplierReceipt, component, buildPackage, binarySHA, archiveSHA, actualToolchain string) error {
	if r.SchemaVersion != 2 || r.Component != component || r.Tag != "v0.6.0" || r.Commit != upgradeFixedPreviousCommit || r.BuildPackage != buildPackage || r.Version != "v0.6.0" {
		return fmt.Errorf("fakeprev: previous source identity does not match fixed release")
	}
	if !upgradeHex(r.Tree, 40) || !upgradeHex(r.TreeDigest, 64) || !upgradeHex(r.ArchiveSHA, 64) || r.ArchiveSHA != archiveSHA || !upgradeHex(binarySHA, 64) || r.BinarySHA != binarySHA {
		return fmt.Errorf("fakeprev: source/archive/copy bytes differ")
	}
	empty := sha256.Sum256(nil)
	if r.DirtySHA != fmt.Sprintf("%x", empty) || r.GOOS != "linux" || r.GOARCH != "amd64" || r.Toolchain != actualToolchain || r.BuildInfo.Toolchain != actualToolchain || !strings.HasPrefix(actualToolchain, "go") {
		return fmt.Errorf("fakeprev: toolchain or clean-source provenance differs")
	}
	if len(r.BuildFlags) != 3 || r.BuildFlags[0] != "-trimpath" || r.BuildFlags[1] != "-ldflags" || r.BuildFlags[2] != "-s -w -X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion=v0.6.0" ||
		r.BuildInfo.Settings["-trimpath"] != "true" || r.BuildInfo.Settings["CGO_ENABLED"] != "0" || r.BuildInfo.Settings["GOOS"] != "linux" || r.BuildInfo.Settings["GOARCH"] != "amd64" {
		return fmt.Errorf("fakeprev: actual build flags are incomplete")
	}
	if _, err := time.Parse(time.RFC3339Nano, r.BuiltAt); err != nil || r.SourceArchive == "" || r.GoToolchain == "" || !strings.Contains(r.GOFlags, "-buildvcs=false") || strings.Contains(r.GOFlags, "-overlay") {
		return fmt.Errorf("fakeprev: supply metadata is incomplete")
	}
	return nil
}

func previousUpgradeArtifact(t *testing.T) string {
	t.Helper()
	path := *upgradePreviousArtifact
	if path == "" {
		// Standalone invocation retains the real fixed-release prerequisite.
		// origin: hot-update S8 plan §7.3; S8 internal review upgrade-maketest-2 F1
		path = supplyUpgradePrevious(t)
	}
	if !upgradePrivatePath(path) {
		t.Fatal("previous executable must be an explicit private artifact")
	}
	var receipt upgradeSupplierReceipt
	if err := readUpgradeCompatibilityJSON(path+"-source.json", &receipt); err != nil {
		t.Fatal(err)
	}
	if !upgradePrivatePath(receipt.SourceArchive) {
		t.Fatal("source archive must be private")
	}
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateUpgradePrevious(receipt, hotUpdateFileSHA(t, path), hotUpdateFileSHA(t, receipt.SourceArchive), info.GoVersion); err != nil {
		t.Fatal(err)
	}
	t.Logf("previous artifact: tag=%s commit=%s binary_sha256=%s toolchain=%s path=%s", receipt.Tag, receipt.Commit, receipt.BinarySHA, info.GoVersion, path)
	return path
}

// Synthetic records here exercise only the receipt predicate. The real
// previous executable test above independently reads buildinfo/archive bytes.
func upgradePreviousOracleRecord() upgradeSupplierReceipt {
	var r upgradeSupplierReceipt
	r.SchemaVersion, r.Component, r.Tag, r.Commit = 2, "tether-prev", "v0.6.0", upgradeFixedPreviousCommit
	r.Tree, r.TreeDigest, r.ArchiveSHA = strings.Repeat("1", 40), strings.Repeat("2", 64), strings.Repeat("3", 64)
	r.DirtySHA = fmt.Sprintf("%x", sha256.Sum256(nil))
	r.GOOS, r.GOARCH, r.Version, r.GoToolchain, r.BuildPackage = "linux", "amd64", "v0.6.0", "auto", "./cmd/tether"
	r.BuildFlags = []string{"-trimpath", "-ldflags", "-s -w -X github.com/LinZiyang666/tether/internal/proto.ReleaseVersion=v0.6.0"}
	r.BinarySHA, r.Toolchain = strings.Repeat("4", 64), "go1.26.8"
	r.BuildInfo.Toolchain, r.BuildInfo.Settings = r.Toolchain, map[string]string{"-trimpath": "true", "CGO_ENABLED": "0", "GOOS": "linux", "GOARCH": "amd64"}
	r.SourceArchive, r.BuiltAt, r.GOFlags = "/fixture/source.tar", "2026-10-12T12:00:00Z", "-buildvcs=false"
	return r
}

// origin: hot-update S8 plan §7.3 / P24 / fakeprev
func TestUpgradePreviousArtifactProvenanceControls(t *testing.T) {
	good := upgradePreviousOracleRecord()
	if err := validateUpgradePrevious(good, good.BinarySHA, good.ArchiveSHA, good.Toolchain); err != nil {
		t.Fatal(err)
	}
	for _, defect := range []string{"current-source", "wrong-tag", "missing-tree", "missing-toolchain", "wrong-toolchain", "different-bytes", "different-archive", "missing-flags", "dirty-old-source", "missing-buildtime", "missing-archive"} {
		t.Run(defect, func(t *testing.T) {
			bad := upgradePreviousOracleRecord()
			switch defect {
			case "current-source":
				bad.Commit = strings.Repeat("f", 40)
			case "wrong-tag":
				bad.Tag = "v0.0.0-dev"
			case "missing-tree":
				bad.Tree = ""
			case "missing-toolchain":
				bad.Toolchain = ""
			case "wrong-toolchain":
				bad.Toolchain = "go1.26.7"
				bad.BuildInfo.Toolchain = bad.Toolchain
			case "different-bytes":
				bad.BinarySHA = strings.Repeat("5", 64)
			case "different-archive":
				bad.ArchiveSHA = strings.Repeat("5", 64)
			case "missing-flags":
				bad.BuildFlags = nil
			case "dirty-old-source":
				bad.DirtySHA = strings.Repeat("6", 64)
			case "missing-buildtime":
				bad.BuiltAt = ""
			case "missing-archive":
				bad.SourceArchive = ""
			}
			if validateUpgradePrevious(bad, good.BinarySHA, good.ArchiveSHA, good.Toolchain) == nil {
				t.Fatal("fake or incomplete previous provenance accepted")
			}
		})
	}
}

type upgradeLegacyRouteReceipt struct {
	Round            int                          `json:"round"`
	RunID            string                       `json:"run_id"`
	Consumers        []string                     `json:"consumers"`
	CapableConsumers []string                     `json:"capable_consumers"`
	Issuer           upgradetx.Owner              `json:"issuer"`
	Route            upgradetx.Owner              `json:"route"`
	Dispatches       int                          `json:"dispatches"`
	NativeRC         int                          `json:"native_rc"`
	Code             string                       `json:"code"`
	Proof            upgradetx.LegacyUpgradeProof `json:"proof"`
	SourceCommit     string                       `json:"source_commit"`
	BinarySHA        string                       `json:"binary_sha"`
	QueryIssuer      upgradetx.Owner              `json:"query_issuer"`
	NonHot           bool                         `json:"non_hot"`
}

func judgeUpgradeLegacyRoute(r upgradeLegacyRouteReceipt, arm string, round int) error {
	if r.Round != round || r.RunID != fmt.Sprintf("%s/round-%d", arm, round) || !upgradeHex(r.SourceCommit, 40) || !upgradeHex(r.BinarySHA, 64) || r.Issuer.PID <= 0 || r.Issuer.StartTicks == 0 || r.Issuer.InstanceID == "" || r.Issuer.BootID == "" || len(r.Consumers) < 2 {
		return fmt.Errorf("legacy route lacks independent execution identity")
	}
	switch arm {
	case "mixed-consumer":
		if len(r.CapableConsumers) >= len(r.Consumers) || r.Dispatches != 0 || r.NativeRC != 64 || r.Code != "legacy_consumer_unsupported" {
			return fmt.Errorf("mixed queue authorized migration")
		}
	case "route-changed":
		if r.Route == r.Issuer || r.Dispatches != 0 || r.NativeRC != 64 || r.Code != "legacy_issuer_route_mismatch" {
			return fmt.Errorf("probe route was reused after issuer changed")
		}
	case "uncorrelated-observation":
		if r.Route != r.Issuer || r.QueryIssuer != r.Issuer || r.Dispatches != 1 || r.NativeRC != 74 || r.Proof.Issuer != r.Issuer || r.Proof.Causal || r.Proof.State == "completed" || !r.NonHot || r.Proof.ParentTxID == "" || r.Proof.DispatchActionID == "" || r.Proof.ObservedNonce == "" || !upgradeHex(r.Proof.ObservedSHA, 64) {
			return fmt.Errorf("uncorrelated legacy observation certified completion or redispatched")
		}
	default:
		return fmt.Errorf("unknown legacy route arm")
	}
	return nil
}

func upgradeLegacyRouteControl(arm string) upgradeLegacyRouteReceipt {
	issuer := upgradetx.Owner{BootID: "boot", PID: 100, StartTicks: 101, InstanceID: "issuer"}
	r := upgradeLegacyRouteReceipt{RunID: arm + "/round-0", Consumers: []string{"issuer", "other"}, CapableConsumers: []string{"issuer", "other"}, Issuer: issuer, Route: issuer, QueryIssuer: issuer, SourceCommit: strings.Repeat("1", 40), BinarySHA: strings.Repeat("2", 64)}
	switch arm {
	case "mixed-consumer":
		r.CapableConsumers = r.CapableConsumers[:1]
		r.NativeRC, r.Code = 64, "legacy_consumer_unsupported"
	case "route-changed":
		r.Route.StartTicks++
		r.NativeRC, r.Code = 64, "legacy_issuer_route_mismatch"
	case "uncorrelated-observation":
		r.Dispatches, r.NativeRC, r.NonHot = 1, 74, true
		r.Proof = upgradetx.LegacyUpgradeProof{ParentTxID: strings.Repeat("3", 32), Issuer: issuer, DispatchActionID: strings.Repeat("4", 32), ObservedSHA: strings.Repeat("5", 64), ObservedNonce: strings.Repeat("6", 32), State: "observed"}
	}
	return r
}

// origin: hot-update S8 plan §7.2 / P24
func TestLegacyIssuerRouteOracleControls(t *testing.T) {
	for _, arm := range []string{"mixed-consumer", "route-changed", "uncorrelated-observation"} {
		good := upgradeLegacyRouteControl(arm)
		if err := judgeUpgradeLegacyRoute(good, arm, 0); err != nil {
			t.Fatal(err)
		}
		for _, defect := range []string{"redispatch", "wrong-rc", "missing-source", "stale-round"} {
			t.Run(arm+"/"+defect, func(t *testing.T) {
				bad := upgradeLegacyRouteControl(arm)
				switch defect {
				case "redispatch":
					bad.Dispatches++
				case "wrong-rc":
					bad.NativeRC = 0
				case "missing-source":
					bad.SourceCommit = ""
				case "stale-round":
					bad.Round = 1
				}
				if judgeUpgradeLegacyRoute(bad, arm, 0) == nil {
					t.Fatal("unsafe legacy route receipt accepted")
				}
			})
		}
	}
	for _, defect := range []string{"wrong-query-issuer", "causal-from-sha", "non-hot-hidden", "completed-from-observation", "missing-dispatch-identity"} {
		t.Run(defect, func(t *testing.T) {
			bad := upgradeLegacyRouteControl("uncorrelated-observation")
			switch defect {
			case "wrong-query-issuer":
				bad.QueryIssuer.StartTicks++
			case "causal-from-sha":
				bad.Proof.Causal = true
			case "non-hot-hidden":
				bad.NonHot = false
			case "completed-from-observation":
				bad.Proof.State = "completed"
			case "missing-dispatch-identity":
				bad.Proof.DispatchActionID = ""
			}
			if judgeUpgradeLegacyRoute(bad, "uncorrelated-observation", 0) == nil {
				t.Fatal("uncorrelated legacy proof certified completion")
			}
		})
	}
}

// Targeted route/recovery must retain the original durable issuer. These
// receipts are supplied by the real P15/P19 fixture; absence is NOT_RUN with
// its pending owner. Explicit broken evidence remains a failing prerequisite,
// never a fabricated transport or a bare Skip.
// origin: hot-update S8 plan §7.2 / §9.1 / P24
// origin: S8 internal review s8-n1-evidence F4
func TestLegacyIssuerRouteChange(t *testing.T) {
	if *upgradeCompatibilityEvidence == "" {
		// SETUP-RED, never a skip: a skipped receipt reads as a pass to the hard gates.
		// origin: hot-update S10 lead review of s8-n1-evidence (Skip-as-pass)
		t.Fatal("SETUP-RED: NOT_RUN pending S8 P13/P15/P19/P24/P26: real targeted legacy issuer route receipts are required (-upgrade-compatibility-evidence; hot-update S8 plan §7.2 / §9.1)")
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		exerciseLegacyIssuerRoute(t, round)
	}
	testharness.AssertNoGoroutineLeak(t, "legacy issuer route x5", g)
	testharness.AssertNoFDLeak(t, "legacy issuer route x5", fd, 0)
}

func exerciseLegacyIssuerRoute(t *testing.T, round int) {
	t.Helper()
	if *upgradeCompatibilityEvidence == "" {
		t.Fatal("NOT_RUN pending P13/P15/P19: real targeted legacy issuer route receipts are required")
	}
	for _, arm := range []string{"mixed-consumer", "route-changed", "uncorrelated-observation"} {
		var r upgradeLegacyRouteReceipt
		if err := readUpgradeCompatibilityJSON(filepath.Join(*upgradeCompatibilityEvidence, fmt.Sprintf("round-%d", round), arm+".json"), &r); err != nil {
			t.Fatal(err)
		}
		if err := judgeUpgradeLegacyRoute(r, arm, round); err != nil {
			t.Fatal(err)
		}
	}
}

// This probe exercise owns a real legacy PTY via the product's existing run
// wire, and kills that original child through the real broker/agent handlers.
// The foreign session environment is intentional: only this cell's home and
// original NKey may select the subject. No mocked kill reply is involved.
// origin: hot-update S8 plan §7.3 / P24
func TestUpgradeWireProbeUsesOriginalCTLIdentity(t *testing.T) {
	artifact := buildHotUpdateArtifact(t, "v0.0.0-s8-wire-probe")
	probe := filepath.Join(t.TempDir(), "upgrade-wire-probe")
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", probe, "./test/hotupdate/wireprobe")
	cmd.Dir, _ = filepath.Abs("../..")
	if out, err := testsupervisor.CombinedOutput(cmd); err != nil {
		t.Fatalf("build wire probe: %v %s", err, out)
	}
	g, fd := runtime.NumGoroutine(), testharness.FDCount()
	for round := 0; round < 5; round++ {
		t.Run(fmt.Sprint(round), func(t *testing.T) { exerciseUpgradeWireProbe(t, artifact.Path, probe) })
	}
	testharness.AssertNoGoroutineLeak(t, "wire probe original ctl x5", g)
	testharness.AssertNoFDLeak(t, "wire probe original ctl x5", fd, 0)
}

func exerciseUpgradeWireProbe(t *testing.T, artifact, probe string) {
	t.Helper()
	db := testharness.OpenDB(t)
	home := t.TempDir()
	id, err := cli.EnsureIdentity(home)
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.WriteCurrentSession(home, "lab"); err != nil {
		t.Fatal(err)
	}
	authorityCreate(t, db, id.Fingerprint, 1)
	agentHome := t.TempDir()
	agentID, err := cli.EnsureAgentIdentity(agentHome, "lab")
	if err != nil {
		t.Fatal(err)
	}
	killSubject := proto.SubjCmdBy("lab", id.PublicKey, "node", "kill")
	// An unsigned connection cannot publish this subject. The anonymous
	// fixture broker retains its ordinary internal subjects, while the ctl
	// and agent must actually prove their previously provisioned NKeys.
	options := &server.Options{Host: "127.0.0.1", Port: -1, AlwaysEnableNonce: true, NoAuthUser: "fixture-broker",
		Users: []*server.User{{Username: "fixture-broker", Permissions: &server.Permissions{Publish: &server.SubjectPermission{Allow: []string{">"}, Deny: []string{killSubject}}}}},
		Nkeys: []*server.NkeyUser{{Nkey: id.PublicKey}, {Nkey: agentID.PublicKey}}}
	url, observer := authorityBrokerOptions(t, db, "http://127.0.0.1:", options)
	wireKills := make(chan []byte, 2)
	var wireKillCount atomic.Int64
	killSub, err := observer.Subscribe(killSubject, func(m *nats.Msg) {
		wireKillCount.Add(1)
		select {
		case wireKills <- bytes.Clone(m.Data):
		default:
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = killSub.Unsubscribe() })
	if err := observer.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	actor := startHotUpdateProcess(t, artifact, append(hotUpdateEnvironment(t), "TETHER_HOME="+agentHome), "agent", "--session", "lab", "--nid", "node", "--nats-url", url, "--log-file", "-")
	authorityAwait(t, func() bool {
		select {
		case <-actor.Done:
			t.Fatalf("agent stopped: %s", actor.Output.Bytes())
		default:
		}
		var count int
		return db.QueryRow(`SELECT COUNT(*) FROM nodes WHERE sid='lab' AND nid='node' AND status='ONLINE'`).Scan(&count) == nil && count == 1
	})
	// Registration can precede the forwarded/intake subscriptions. Observe the
	// real harmless claim handler before sending the one original run request;
	// otherwise a setup race could mask a mutation as an unrelated timeout.
	authorityAwait(t, func() bool {
		msg, err := observer.Request(proto.SubjCmdForwarded("lab", "node", "claim-probe"), []byte(`{}`), 50*time.Millisecond)
		var reply proto.ClaimProbeResp
		return err == nil && json.Unmarshal(msg.Data, &reply) == nil && reply.InstanceID != ""
	})
	nc, err := cli.ConnectNATSWithNkey(url, id, nats.Name("tether-ctl:lab"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	inbox := nc.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(proto.RunReq{Argv: []string{"/bin/sh", "-c", "printf probe-ready; exec sleep 60"}, Cwd: t.TempDir(), Cols: 80, Rows: 24})
	if err := nc.PublishRequest(proto.SubjCmdBy("lab", id.PublicKey, "node", "run"), inbox, body); err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var ready proto.RunChunk
	if err := json.Unmarshal(msg.Data, &ready); err != nil || ready.Kind != "ready" || ready.PID == "" {
		t.Fatal("real legacy run did not issue original PID")
	}
	attach, _ := json.Marshal(proto.PtyAttachEvent{Cols: 80, Rows: 24})
	if err := nc.Publish(proto.SubjPtyAttach("lab", ready.PID), attach); err != nil {
		t.Fatal(err)
	}
	msg, err = sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var started proto.RunChunk
	if err := json.Unmarshal(msg.Data, &started); err != nil || started.Kind != "started" || started.PID != ready.PID {
		t.Fatal("legacy run did not start its original PID")
	}
	authorityAwait(t, func() bool {
		var count int
		return db.QueryRow(`SELECT COUNT(*) FROM processes WHERE sid='lab' AND pid=? AND status='RUNNING'`, ready.PID).Scan(&count) == nil && count == 1
	})
	cmd := exec.CommandContext(t.Context(), probe, "--home", home, "--nats-url", url, "--verb", "kill", "--nid", "node", "--pid", ready.PID, "--signal", "15")
	cmd.Env = append(hotUpdateEnvironment(t), "TETHER_SESSION=foreign")
	out, err := testsupervisor.CombinedOutput(cmd)
	var killed proto.KillResp
	if err != nil || json.Unmarshal(out, &killed) != nil || !killed.OK {
		t.Fatalf("signed original kill failed: %v %s", err, out)
	}
	wantWire, _ := json.Marshal(proto.KillReq{PID: ready.PID, Signal: 15})
	select {
	case gotWire := <-wireKills:
		if !bytes.Equal(gotWire, wantWire) {
			t.Fatal("probe changed the frozen original kill payload")
		}
	case <-time.After(time.Second):
		t.Fatal("probe did not issue existing kill wire")
	}
	select {
	case <-wireKills:
		t.Fatal("probe issued duplicate kill wire")
	default:
	}
	if wireKillCount.Load() != 1 {
		t.Fatal("probe issued more than one existing kill request")
	}
	msg, err = sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatal(err)
	}
	var terminal proto.RunChunk
	if err := json.Unmarshal(msg.Data, &terminal); err != nil || terminal.Kind != "exit" || terminal.PID != ready.PID {
		t.Fatal("probe did not terminate the original run")
	}
	if _, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
		t.Fatal("kill produced more than one run terminal")
	}
	authorityAwait(t, func() bool {
		var state string
		return db.QueryRow(`SELECT status FROM processes WHERE sid='lab' AND pid=?`, ready.PID).Scan(&state) == nil && state == "EXITED"
	})
}
