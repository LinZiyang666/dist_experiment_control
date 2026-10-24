// compatibility_test.go attacks catalog omissions and receipt substitutions.
// Synthetic projections exercise the intake predicate only; the checked-in
// catalog retains pending status for every unavailable upstream product receipt.
// origin: hot-update S10 plan §7 / §10.2 / P27
package acceptance

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// origin: hot-update S10 plan §7 / §10.2 / TestFullstackCompatibilityMatrixIsComplete
func TestFullstackCompatibilityMatrixIsComplete(t *testing.T) {
	data, err := os.ReadFile("../../simcluster/drills/lib/fullstack_compatibility.json")
	if err != nil {
		t.Fatal(err)
	}
	m, err := DecodeCompatibility(data)
	if err != nil {
		t.Fatal(err)
	}
	verifyCompatibilitySourceFacts(t, m)
	// The two independent old-artifact sets each need all 40 real-role cells.
	for _, set := range []string{"fixed-v0.6.0", "dynamic-adjacent"} {
		for _, ctl := range []string{"old", "new"} {
			for _, agent := range []string{"old", "new"} {
				for _, broker := range []string{"old", "new"} {
					for _, verb := range []string{"exec", "run", "kill", "expose", "transfer"} {
						id := fmt.Sprintf("%s/W-c%s-a%s-b%s-%s", set, ctl, agent, broker, verb)
						cell := findCompatibilityCell(t, m, id)
						if cell.Ctl != ctl || cell.Agent != agent || cell.Broker != broker || cell.State != verb {
							t.Fatalf("wire axes changed: %s", id)
						}
					}
				}
			}
		}
	}
	t.Run("pending is incomplete", func(t *testing.T) {
		result, err := JudgeCompatibility(m, "synthetic-candidate", nil, nil)
		if err != nil || result.RC != 4 || result.Verdict != "INCOMPLETE" || len(result.Missing) != len(m.Cells) {
			t.Fatalf("pending intake: rc=%d verdict=%s missing=%d err=%v", result.RC, result.Verdict, len(result.Missing), err)
		}
		for _, cell := range m.Cells {
			if cell.Status != "pending" || cell.Pending == "" || cell.Receipt != nil {
				t.Fatalf("invented upstream receipt: %s", cell.ID)
			}
		}
	})
	t.Run("closed catalog controls", func(t *testing.T) {
		cases := []struct {
			name   string
			change func(*CompatibilityMatrix)
		}{
			{"drop broker axis", func(m *CompatibilityMatrix) { m.Cells[0].Broker = "" }},
			{"same count duplicate cell", func(m *CompatibilityMatrix) { m.Cells[1] = m.Cells[0] }},
			{"miss retained", func(m *CompatibilityMatrix) {
				m.Cells = slices.DeleteFunc(m.Cells, func(c CompatibilityCell) bool { return c.State == "salt-filter" })
			}},
			{"advertise without decoder", func(m *CompatibilityMatrix) {
				i := compatibilityCellIndex(t, *m, "ABI/agent/candidate/finalPrepare/reverse")
				m.Cells[i].Requires = []string{"manifest-strict"}
			}},
			{"legacy alias", func(m *CompatibilityMatrix) {
				i := compatibilityCellIndex(t, *m, "legacy/LB-old")
				m.Cells[i].Detail = "legacy_session_busy"
			}},
			{"non hot relabeled", func(m *CompatibilityMatrix) {
				i := compatibilityCellIndex(t, *m, "legacy/LI-interrupting")
				m.Cells[i].Class = "hot"
			}},
			{"sole replaces single", func(m *CompatibilityMatrix) {
				i := compatibilityCellIndex(t, *m, "deployment/S9/single")
				m.Cells[i].State = "sole-voter"
			}},
			{"mechanism fills release", func(m *CompatibilityMatrix) { m.Cells[0].ArtifactSet = "mechanism-only" }},
			{"missing infra reverse", func(m *CompatibilityMatrix) {
				m.Cells = slices.DeleteFunc(m.Cells, func(c CompatibilityCell) bool { return c.Family == "infra" && c.Direction == "reverse" })
			}},
			{"missing user unit", func(m *CompatibilityMatrix) {
				i := compatibilityCellIndex(t, *m, "deployment/S8/user-unit")
				m.Cells = slices.Delete(m.Cells, i, i+1)
			}},
			{"unknown status", func(m *CompatibilityMatrix) { m.Cells[0].Status = "completed" }},
			{"pending with receipt", func(m *CompatibilityMatrix) { m.Cells[0].Receipt = &CompatibilityRaw{} }},
			{"empty pending reason", func(m *CompatibilityMatrix) { m.Cells[0].Pending = "" }},
			{"unknown set", func(m *CompatibilityMatrix) { m.ArtifactSets[0] = "adjacent-looking-stamps" }},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				bad := cloneCompatibilityMatrix(t, m)
				tc.change(&bad)
				if err := ValidateCompatibility(bad); err == nil {
					t.Fatal("incomplete or changed catalog accepted")
				}
			})
		}
	})
	t.Run("strict raw catalog", func(t *testing.T) {
		for _, bad := range [][]byte{
			[]byte(strings.Replace(string(data), `"schema":`, `"Schema":`, 1)),
			[]byte(strings.Replace(string(data), `"schema":`, `"schema":"ignored","schema":`, 1)),
			[]byte(strings.Replace(string(data), `"schema":`, `"command":"touch marker","schema":`, 1)),
			append(slices.Clone(data), []byte(` {}`)...),
			[]byte(`null`), []byte(`{"schema":null}`), {0xff},
		} {
			if _, err := DecodeCompatibility(bad); err == nil {
				t.Fatal("ambiguous or unknown JSON accepted")
			}
		}
	})
	t.Run("behavior and identity controls", func(t *testing.T) {
		cases := []struct {
			name, id string
			change   func(*CompatibilityProof)
		}{
			{"advertise without decoder", "ABI/agent/candidate/finalPrepare/reverse", func(p *CompatibilityProof) {
				p.Checks = slices.DeleteFunc(p.Checks, func(c CompatibilityCheck) bool { return strings.HasPrefix(c.Name, "decode/") })
			}},
			{"miss retained decoder", "retained/agent/ss-v1/salt-filter/reverse", func(p *CompatibilityProof) { p.Checks = p.Checks[1:] }},
			{"legacy alias", "legacy/LB-old", func(p *CompatibilityProof) { p.Detail = "legacy_session_busy" }},
			{"broker supplied by agent", m.Cells[0].ID, func(p *CompatibilityProof) {
				p.Binaries = slices.DeleteFunc(p.Binaries, func(b CompatibilityBinary) bool { return b.Role == "broker" })
			}},
			{"old candidate", m.Cells[0].ID, func(p *CompatibilityProof) { p.CandidateID = "old-candidate" }},
			{"mechanism tier", m.Cells[0].ID, func(p *CompatibilityProof) { p.Tier = "hermetic" }},
			{"mechanism artifact set", m.Cells[0].ID, func(p *CompatibilityProof) { p.ArtifactSet = "mechanism-only" }},
			{"same source capable stamps", "ABI/agent/candidate/finalPrepare/reverse", func(p *CompatibilityProof) { p.Binaries[1].Artifact.SourceCommit = p.Binaries[0].Artifact.SourceCommit }},
			{"different copied executing bytes", m.Cells[0].ID, func(p *CompatibilityProof) { p.Binaries[0].ExecutingSHA = sha256.Sum256([]byte("not-running")) }},
			{"unknown incarnation", m.Cells[0].ID, func(p *CompatibilityProof) { p.Binaries[0].Process.StartTicks = 0 }},
			{"no argv", m.Cells[0].ID, func(p *CompatibilityProof) { p.Binaries[0].Argv = nil }},
			{"fixed old source fabricated", m.Cells[0].ID, func(p *CompatibilityProof) { p.Binaries[0].Artifact.SourceCommit = "wrong" }},
			{"release receipt claims T1", m.Cells[0].ID, func(p *CompatibilityProof) { p.Binaries[0].Artifact.ID = "T1" }},
			{"native setup is not refusal", "legacy/LB-old", func(p *CompatibilityProof) { p.NativeRC = 2 }},
			{"CLI refusal is not native green", "legacy/LB-old", func(p *CompatibilityProof) { p.NativeRC = 64 }},
			{"wrong CLI projection", "legacy/LB-old", func(p *CompatibilityProof) { p.CLIRC = 75 }},
			{"empty nonzero domain", "ABI/agent/candidate/finalPrepare/reverse", func(p *CompatibilityProof) { p.Checks[3].Bytes = 0 }},
			{"parse without I O", "ABI/agent/candidate/finalPrepare/reverse", func(p *CompatibilityProof) { p.Checks[3].Continued = false }},
			{"missing read epochs", "ABI/agent/candidate/finalPrepare/reverse", func(p *CompatibilityProof) { p.Checks[3].ReadEpochs = nil }},
			{"two epoch jump", "ABI/agent/candidate/finalPrepare/reverse", func(p *CompatibilityProof) {
				p.Binaries[0].Artifact.Epoch = 3
				for i := range p.Checks {
					if strings.HasPrefix(p.Checks[i].Name, "decode/") {
						p.Checks[i].ReaderEpoch = 3
					}
				}
			}},
			{"unknown executing decoder", "ABI/agent/candidate/finalPrepare/reverse", func(p *CompatibilityProof) { p.Checks[3].Reader = sha256.Sum256([]byte("third-reader")) }},
			{"unknown exporting decoder", "ABI/agent/candidate/finalPrepare/reverse", func(p *CompatibilityProof) { p.Checks[3].Exporter = sha256.Sum256([]byte("third-exporter")) }},
			{"missing advertisement", "ABI/agent/candidate/finalPrepare/reverse", func(p *CompatibilityProof) { p.Binaries[0].Artifact.Domains = nil }},
			{"same toolchain substituted", "storage/go-hash-cross-toolchain/reverse", func(p *CompatibilityProof) { p.Binaries[1].Artifact.Toolchain = p.Binaries[0].Artifact.Toolchain }},
			{"wrong same toolchain", "storage/go-hash-same-toolchain/reverse", func(p *CompatibilityProof) { p.Binaries[1].Artifact.Toolchain = "different" }},
			{"infra same SHA no op", "infra/helper/actual-prev/forward", func(p *CompatibilityProof) {
				p.Binaries[1].ExecutingSHA = p.Binaries[0].ExecutingSHA
				p.Binaries[1].Artifact.Binary = p.Binaries[0].ExecutingSHA
			}},
			{"infra version without pinned bytes", "infra/nats/store/reverse", func(p *CompatibilityProof) {
				p.Binaries[1].ExecutingSHA = sha256.Sum256([]byte("wrong-infra-bytes"))
				p.Binaries[1].Artifact.Binary = p.Binaries[1].ExecutingSHA
			}},
			{"hash not continued", "storage/go-hash-same-toolchain/forward", func(p *CompatibilityProof) { p.Checks[0].Output = sha256.Sum256([]byte("bad-hash")) }},
			{"ALPN falsely v1", "fixed-v0.6.0/ALPN-aold-bnew", func(p *CompatibilityProof) { p.ALPN = "tether-flow/1" }},
			{"contradictory old provenance", "fixed-v0.6.0/W-cold-aold-bold-run", func(p *CompatibilityProof) { p.Binaries[0].Artifact.Tree = sha256.Sum256([]byte("contradiction")) }},
			{"non hot counted hot", "legacy/LI-interrupting", func(p *CompatibilityProof) { p.Class = "hot" }},
		}
		complete, raw, proofs := syntheticCompatibilityIntake(t, m)
		read := func(ref c.RawRef) ([]byte, error) { return raw[ref.Path], nil }
		decode := func(cell CompatibilityCell, data []byte) (CompatibilityProof, error) { return proofs[cell.ID], nil }
		before := cloneCompatibilityMatrix(t, complete)
		result, err := JudgeCompatibility(complete, "synthetic-candidate", read, decode)
		if err != nil || result.RC != 0 || len(result.Inputs) != len(m.Cells) || !reflect.DeepEqual(complete, before) {
			t.Fatalf("synthetic predicate baseline: rc=%d failures=%d inputs=%d err=%v", result.RC, len(result.Failures), len(result.Inputs), err)
		}
		result, err = JudgeCompatibility(complete, "synthetic-candidate", read, func(cell CompatibilityCell, data []byte) (CompatibilityProof, error) {
			cell.Requires[0] = "decoder-must-not-change-the-catalog"
			cell.Receipt.Path = "decoder-must-not-change-raw-identity"
			return proofs[cell.ID], nil
		})
		if err != nil || result.RC != 0 || !reflect.DeepEqual(complete, before) {
			t.Fatalf("decoder aliases catalog storage: rc=%d err=%v", result.RC, err)
		}
		// Self-reading is legal; release and Go decoder compatibility remain
		// separate axes even when the hash fixture uses the same source commit.
		for _, id := range []string{"ABI/agent/running/disk-select/forward", "storage/go-hash-cross-toolchain/reverse"} {
			cell := findCompatibilityCell(t, complete, id)
			proof := proofs[id]
			proof.Binaries[1].Artifact.SourceCommit = proof.Binaries[0].Artifact.SourceCommit
			proof.Binaries[1].Artifact.Tree = proof.Binaries[0].Artifact.Tree
			if cell.Family == "image" {
				proof.Binaries[1].Artifact.Binary = proof.Binaries[0].ExecutingSHA
				proof.Binaries[1].ExecutingSHA = proof.Binaries[0].ExecutingSHA
				for i := range proof.Checks {
					if strings.HasPrefix(proof.Checks[i].Name, "decode/") {
						proof.Checks[i].Exporter = proof.Binaries[0].ExecutingSHA
					}
				}
			}
			if !compatibilityProofMatches(cell, proof, "synthetic-candidate") {
				t.Fatalf("valid self-reader or Go decoder rejected: %s", id)
			}
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, _, controls := syntheticCompatibilityIntake(t, m)
				proof := controls[tc.id]
				tc.change(&proof)
				controls[tc.id] = proof
				result, err := JudgeCompatibility(complete, "synthetic-candidate", read, func(cell CompatibilityCell, data []byte) (CompatibilityProof, error) { return controls[cell.ID], nil })
				if err != nil || result.RC != 1 || len(result.Failures) == 0 {
					t.Fatalf("control did not reach assertion red: rc=%d failures=%d err=%v", result.RC, len(result.Failures), err)
				}
			})
		}
		for name, run := range map[string]func() (c.Result, error){
			"no decoder": func() (c.Result, error) { return JudgeCompatibility(complete, "synthetic-candidate", read, nil) },
			"no reader":  func() (c.Result, error) { return JudgeCompatibility(complete, "synthetic-candidate", nil, decode) },
			"wrong raw bytes": func() (c.Result, error) {
				return JudgeCompatibility(complete, "synthetic-candidate", func(c.RawRef) ([]byte, error) { return []byte("wrong"), nil }, decode)
			},
			"same length forged raw": func() (c.Result, error) {
				return JudgeCompatibility(complete, "synthetic-candidate", func(ref c.RawRef) ([]byte, error) { return make([]byte, ref.Bytes), nil }, decode)
			},
			"read failure": func() (c.Result, error) {
				return JudgeCompatibility(complete, "synthetic-candidate", func(c.RawRef) ([]byte, error) { return nil, errors.New("read failed") }, decode)
			},
			"decoder failure": func() (c.Result, error) {
				return JudgeCompatibility(complete, "synthetic-candidate", read, func(CompatibilityCell, []byte) (CompatibilityProof, error) {
					return CompatibilityProof{}, errors.New("decode failed")
				})
			},
		} {
			t.Run(name, func(t *testing.T) {
				result, err := run()
				if err != nil || result.RC != 1 {
					t.Fatalf("invalid intake accepted: rc=%d err=%v", result.RC, err)
				}
			})
		}
	})
}

func compatibilityCellIndex(t *testing.T, m CompatibilityMatrix, id string) int {
	t.Helper()
	for i, cell := range m.Cells {
		if cell.ID == id {
			return i
		}
	}
	t.Fatalf("required compatibility cell missing: %s", id)
	return -1
}

func findCompatibilityCell(t *testing.T, m CompatibilityMatrix, id string) CompatibilityCell {
	t.Helper()
	return m.Cells[compatibilityCellIndex(t, m, id)]
}

func cloneCompatibilityMatrix(t *testing.T, m CompatibilityMatrix) CompatibilityMatrix {
	t.Helper()
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var clone CompatibilityMatrix
	if err := json.Unmarshal(data, &clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

// This fixture intentionally uses invented bytes and a local test decoder.
// It cannot populate the on-disk pending catalog or sign upstream acceptance.
func syntheticCompatibilityIntake(t *testing.T, m CompatibilityMatrix) (CompatibilityMatrix, map[string][]byte, map[string]CompatibilityProof) {
	t.Helper()
	m = cloneCompatibilityMatrix(t, m)
	raw, proofs := map[string][]byte{}, map[string]CompatibilityProof{}
	for i := range m.Cells {
		cell := &m.Cells[i]
		payload := []byte("synthetic predicate input: " + cell.ID)
		digest := sha256.Sum256(payload)
		cell.Status, cell.Pending = "recorded", ""
		cell.Receipt = &CompatibilityRaw{Path: "synthetic/" + cell.ID, SHA256: hex.EncodeToString(digest[:]), Bytes: int64(len(payload)), Schema: "synthetic-predicate-only"}
		raw[cell.Receipt.Path] = payload
		p := CompatibilityProof{CellID: cell.ID, CandidateID: "synthetic-candidate", ArtifactSet: cell.ArtifactSet, Tier: "deploy", Class: cell.Class, Code: cell.Code, Detail: cell.Detail, CLIRC: cell.CLIRC, NativeRC: cell.NativeRC, ALPN: cell.ALPN}
		if p.CLIRC < 0 {
			p.CLIRC = 0
			if cell.Class == "refusal" {
				p.CLIRC = 64
			}
		}
		if cell.Class == "refusal" && p.Code == "" {
			p.Code = "synthetic-refusal"
		}
		slots := compatibilityBinarySlots(*cell)
		keys := make([]string, 0, len(slots))
		for slot := range slots {
			keys = append(keys, slot)
		}
		slices.Sort(keys)
		if slices.Contains(keys, "reader") {
			keys = []string{"reader", "exporter"}
		}
		for index, slot := range keys {
			role, version := slots[slot][0], slots[slot][1]
			label := cell.ArtifactSet + role + version
			if version == "" {
				label += slot
			}
			sha := sha256.Sum256([]byte(label))
			if cell.Family == "infra" {
				if pinned := compatibilityInfraSHA(role, version); pinned != "" {
					bytes, err := hex.DecodeString(pinned)
					if err != nil {
						t.Fatal(err)
					}
					copy(sha[:], bytes)
				}
			}
			artifact := c.Artifact{ID: "synthetic-artifact", Role: role, Release: label, SourceCommit: "synthetic-source-" + label, Toolchain: "synthetic-toolchain", Tree: sha, Flags: sha, Archive: sha, Binary: sha, CopyManifest: sha, Epoch: 1, ReadEpochs: []uint64{1}}
			if version == "old" && cell.ArtifactSet == "fixed-v0.6.0" {
				artifact.Release, artifact.SourceCommit = "v0.6.0", "a3431a12be3cf947cc797c28fea93382fa68472d"
			}
			if cell.State == "go-hash-cross-toolchain" {
				artifact.Toolchain += strconv.Itoa(index)
			}
			for _, name := range cell.Requires {
				if strings.HasPrefix(name, "decode/") {
					artifact.Domains = append(artifact.Domains, strings.TrimPrefix(name, "decode/"))
				}
			}
			p.Binaries = append(p.Binaries, CompatibilityBinary{Slot: slot, Role: role, Version: version, Artifact: artifact, ExecutingSHA: sha,
				Process: c.Process{Key: c.Key{Role: role, Host: "synthetic-host"}, BootID: "synthetic-boot", PIDNS: "synthetic-pidns", InstanceID: label, PID: 100 + index, StartTicks: 1000}, Argv: []string{"synthetic-executable", role}})
		}
		for _, name := range cell.Requires {
			check := CompatibilityCheck{Name: name, Input: digest, Output: digest, Bytes: 1, Continued: true}
			if strings.HasPrefix(name, "decode/") {
				check.Reader, check.Exporter, check.ReaderEpoch, check.ExportEpoch, check.ReadEpochs = p.Binaries[0].ExecutingSHA, p.Binaries[1].ExecutingSHA, 1, 1, []uint64{1}
			}
			p.Checks = append(p.Checks, check)
		}
		proofs[cell.ID] = p
	}
	return m, raw, proofs
}

// Read source declarations, never product packages: the pure acceptance layer
// must remain stdlib-only except for P00 contracts. New S8 advertisements are
// still pending, while its already merged sealed adjunct is part of the union.
func verifyCompatibilitySourceFacts(t *testing.T, m CompatibilityMatrix) {
	t.Helper()
	root := "../../.."
	stringsIn := func(path string) []string {
		file, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, path), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var values []string
		ast.Inspect(file, func(node ast.Node) bool {
			if lit, ok := node.(*ast.BasicLit); ok && lit.Kind == token.STRING {
				value, err := strconv.Unquote(lit.Value)
				if err != nil {
					t.Fatal(err)
				}
				values = append(values, value)
			}
			return true
		})
		return values
	}
	for _, domain := range []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1", "broker-image-v1", "transfer-v1"} {
		found := false
		for _, value := range stringsIn("internal/proto/transfer_codes.go") {
			found = found || slices.Contains(strings.Fields(value), domain)
		}
		if !found {
			t.Fatalf("merged image domain missing: %s", domain)
		}
		role := "agent"
		if domain == "broker-image-v1" {
			role = "broker"
		}
		cell := findCompatibilityCell(t, m, "ABI/"+role+"/candidate/finalPrepare/reverse")
		if !slices.Contains(cell.Requires, "decode/"+domain) {
			t.Fatalf("advertised domain lacks required decoder: %s", domain)
		}
	}
	for path, value := range map[string]string{"internal/upgradecut/contracts.go": "upgrade-cut-v1", "internal/tunnel/flowstate/contracts.go": "tether-flow/1"} {
		if !slices.Contains(stringsIn(path), value) {
			t.Fatalf("merged compatibility declaration missing: %s", value)
		}
	}
	for _, value := range []string{"broker-state-v1", "broker-tunnel-state-v1"} {
		if !slices.Contains(stringsIn("internal/brokerimage/private_contracts.go"), value) {
			t.Fatalf("broker private codec missing: %s", value)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "internal/infraartifact/testdata/catalog.json"))
	if err != nil {
		t.Fatal(err)
	}
	var artifacts []struct {
		Kind                                int
		Version, GOOS, GOARCH, BinarySHA256 string
	}
	if err := json.Unmarshal(data, &artifacts); err != nil {
		t.Fatal(err)
	}
	expected := map[string]string{
		"1/2.14.5": "e1a2f9ba25077f4cf753bee829483bd68fbf0a4eec9b6645a1e5785a6de0c0d1",
		"1/2.14.6": "2d0dc83e0547185a6820377114215d6a7c645f607aeb31728fffaf3d84e77c10",
		"2/2.7.5":  "f7706787b843d375d1326cc451d53f48900b7a14f3a9c606d49d7751b390609f",
		"2/2.7.6":  "db3bfc85bb93160f60fa6df9c3ebf2340dc11740e9a52c717d88a14c0430f229",
	}
	for _, artifact := range artifacts {
		key := fmt.Sprintf("%d/%s", artifact.Kind, artifact.Version)
		if expected[key] != artifact.BinarySHA256 || artifact.GOOS != "linux" || artifact.GOARCH != "amd64" {
			t.Fatalf("infra catalog differs from frozen S9 identity: %s", key)
		}
		delete(expected, key)
	}
	if len(expected) != 0 {
		t.Fatal("infra catalog omits pinned compatibility artifacts")
	}
}
