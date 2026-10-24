package architecture

import (
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const fullstackCoveragePath = "docs/reviews/hot-update-fullstack-coverage.tsv"
const fullstackTasklistPath = "docs/reviews/hot-update-tasklist.md"

var fullstackDocsColumns = strings.Fields("kind claim_id source_heading original_text_digest child_ids required_receipt_keys upstream_owner final_owner status pending_owner requirements leaf_ids tests drills budgets documents candidate_id receipt_path receipt_sha256 historical_checked")

type fullstackDocRow map[string]string

// The sets below independently pin plan §13.1. A missing manifest row must not
// shrink the denominator, and a broad H4 receipt cannot sign the other H leaves.
var fullstackCoverageRequirements = []string{
	"3,5,7,8,9", "2,4,5,6,16", "2,4,5,6", "1,2,4,5,11,16",
	"2,4,5,6,11", "2,4,5,6,16", "3,6,10", "1,5,8,16",
	"1,3,8,9,11,12,13", "1,3,8,11,12,13", "7,9,12,13,14", "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16",
}

var fullstackCoverageLeaves = []string{
	"C2,C3,C5,F1,F2,F3", "B1,C1,C2,C3,C4,C5,C6,C7,C10,C11", "B2,C4,C8,C10,C11",
	"B2,D1,D2,D3,D4,D6", "B2,D1,D3,D5,D6", "B3,E1,E2,E3,E5,E6", "B4,C9,H2",
	"D4,E3,E4,E5", "G1,G3,G4,G5,G6", "B6,G2,G5,G6", "C9,C10,F1,F2,F3,F4,F5,F6,F7",
	"A6,H1,H2,H3,H4,H5,H6,H7,H8,H9,H10",
}

func fullstackDocHash(raw []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(raw))
}

func fullstackDocDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value) && value != strings.Repeat("0", 64)
}

func fullstackDocList(value string) []string {
	if value == "-" || value == "" {
		return nil
	}
	return strings.Split(value, ",")
}

func fullstackReadDocRows(root string) ([]fullstackDocRow, error) {
	raw, err := os.ReadFile(filepath.Join(root, fullstackCoveragePath))
	if err != nil {
		return nil, err
	}
	r := csv.NewReader(bytes.NewReader(raw))
	r.Comma = '\t'
	records, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 || !reflect.DeepEqual(records[0], fullstackDocsColumns) {
		return nil, fmt.Errorf("coverage manifest schema differs from frozen receipt fields")
	}
	var rows []fullstackDocRow
	for _, record := range records[1:] {
		row := fullstackDocRow{}
		for i, column := range fullstackDocsColumns {
			if strings.TrimSpace(record[i]) != record[i] || record[i] == "" {
				return nil, fmt.Errorf("empty or padded %s in %s", column, record[1])
			}
			row[column] = record[i]
		}
		rows = append(rows, row)
	}
	return rows, nil
}

type fullstackDocLeaf struct {
	heading, text string
	checked       bool
}

func fullstackDocLeaves(root string) (map[string]fullstackDocLeaf, error) {
	raw, err := os.ReadFile(filepath.Join(root, fullstackTasklistPath))
	if err != nil {
		return nil, err
	}
	marker := regexp.MustCompile(`^(\s*)- \[([ xX])\] (.+) <!-- s10-claim:([A-H][0-9]+(?:\.[a-z]+)?) -->$`)
	headingPattern := regexp.MustCompile(`^## ([A-H])\. `)
	leaves := map[string]fullstackDocLeaf{}
	heading := ""
	active := false
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "## ") {
			active = headingPattern.MatchString(line)
			heading = strings.TrimPrefix(line, "## ")
		}
		if !active || !strings.Contains(line, "- [") {
			continue
		}
		m := marker.FindStringSubmatch(line)
		if m == nil || leaves[m[4]].text != "" || !strings.HasPrefix(m[4], heading[:1]) {
			return nil, fmt.Errorf("unmapped or duplicate tasklist leaf: %s", line)
		}
		leaves[m[4]] = fullstackDocLeaf{heading, m[3], m[2] != " "}
	}
	return leaves, nil
}

// Receipt indexes are not evidence themselves. Open the indexed JSON, its raw
// bytes and executable provenance, and recompute every digest before a checkmark.
// origin: hot-update S10 plan §13.2 / P34
type fullstackDocReceipt struct {
	Key          string   `json:"receipt_key"`
	Candidate    string   `json:"candidate_id"`
	Kind         string   `json:"evidence_kind"`
	Command      []string `json:"command"`
	NativeRC     *int     `json:"native_rc"`
	RawPath      string   `json:"raw_path"`
	RawSHA       string   `json:"raw_sha256"`
	SourceCommit string   `json:"source_commit"`
	ArtifactSHA  string   `json:"artifact_sha256"`
	RunningSHA   string   `json:"running_sha256"`
	ArtifactSet  string   `json:"artifact_set"`
	Releases     []struct {
		Version      string `json:"version"`
		SourceCommit string `json:"source_commit"`
		BinaryPath   string `json:"binary_path"`
		BinarySHA    string `json:"binary_sha256"`
	} `json:"releases"`
}

func fullstackDocRaw(root, path, digest string) ([]byte, error) {
	if !filepath.IsAbs(path) || path == "-" {
		return nil, fmt.Errorf("empty raw receipt or non-absolute external path %q", path)
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(root, real)
	if err != nil || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return nil, fmt.Errorf("raw receipt must remain outside the repository: %s", path)
	}
	raw, err := os.ReadFile(real)
	if err != nil {
		return nil, err
	}
	if len(bytes.TrimSpace(raw)) == 0 || !fullstackDocDigest(digest) || fullstackDocHash(raw) != digest {
		return nil, fmt.Errorf("empty raw receipt or invalid raw hash: %s", path)
	}
	return raw, nil
}

func fullstackDocJSON(raw []byte, target any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(target); err != nil {
		return err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("receipt has trailing JSON")
	}
	return nil
}

func fullstackCheckDocReceipt(root, candidate string, row fullstackDocRow) error {
	if !fullstackDocDigest(candidate) || row["candidate_id"] != candidate {
		return fmt.Errorf("old candidate receipt: %s", row["claim_id"])
	}
	raw, err := fullstackDocRaw(root, row["receipt_path"], row["receipt_sha256"])
	if err != nil {
		return err
	}
	var receipt fullstackDocReceipt
	if err := fullstackDocJSON(raw, &receipt); err != nil {
		return err
	}
	if receipt.Candidate != candidate || receipt.Key != row["claim_id"] {
		return fmt.Errorf("old candidate or wrong receipt key: %s", row["claim_id"])
	}
	if len(receipt.Command) == 0 || strings.TrimSpace(strings.Join(receipt.Command, "")) == "" || receipt.NativeRC == nil {
		return fmt.Errorf("missing command/native rc: %s", receipt.Key)
	}
	if !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(receipt.SourceCommit) || !fullstackDocDigest(receipt.ArtifactSHA) || !fullstackDocDigest(receipt.RunningSHA) {
		return fmt.Errorf("missing source/artifact/running identity: %s", receipt.Key)
	}
	if !slices.Contains([]string{"test", "deployment", "budget", "identity", "command", "mutation", "documentation", "compatibility", "review", "history"}, receipt.Kind) {
		return fmt.Errorf("unknown receipt evidence kind: %s", receipt.Kind)
	}
	// Positive deployment receipts retain native 0. Controls carry native 1 in
	// their own receipt; expected-match never launders them into deployment GREEN.
	if (receipt.Kind == "mutation" && *receipt.NativeRC != 1) || (receipt.Kind != "mutation" && *receipt.NativeRC != 0) {
		return fmt.Errorf("native rc does not sign this receipt kind: %s", receipt.Key)
	}
	if _, err := fullstackDocRaw(root, receipt.RawPath, receipt.RawSHA); err != nil {
		return err
	}
	if receipt.Key == "compatibility.release" && receipt.Kind != "compatibility" {
		return fmt.Errorf("release compatibility requires real release evidence")
	}
	for suffix, kind := range map[string]string{".drill": "deployment", ".mutation": "mutation", ".test": "test", ".budget": "budget", ".identity": "identity", ".command": "command", ".documentation": "documentation"} {
		if strings.HasSuffix(receipt.Key, suffix) && receipt.Kind != kind {
			return fmt.Errorf("receipt kind cannot sign %s: %s", receipt.Key, receipt.Kind)
		}
	}
	if receipt.Kind == "compatibility" {
		if receipt.ArtifactSet != "real-adjacent-release" || len(receipt.Releases) != 2 {
			return fmt.Errorf("synthetic N-1 cannot sign release compatibility")
		}
		for _, release := range receipt.Releases {
			if !regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`).MatchString(release.Version) || !regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(release.SourceCommit) {
				return fmt.Errorf("missing real release provenance")
			}
			if _, err := fullstackDocRaw(root, release.BinaryPath, release.BinarySHA); err != nil {
				return err
			}
		}
		a, b := receipt.Releases[0], receipt.Releases[1]
		if a.Version == b.Version || a.SourceCommit == b.SourceCommit || a.BinarySHA == b.BinarySHA {
			return fmt.Errorf("synthetic N-1 has no distinct release implementations")
		}
	}
	return nil
}

func fullstackDocExpectedIDs() map[string]string {
	ids := map[string]string{"FINAL": "candidate"}
	for i := 1; i <= 12; i++ {
		ids[fmt.Sprintf("CAP%02d", i)] = "coverage"
	}
	for i := 1; i <= 16; i++ {
		ids[fmt.Sprintf("R%d", i)] = "requirement"
	}
	for group, count := range map[string]int{"A": 7, "B": 6, "C": 11, "D": 6, "E": 6, "F": 7, "G": 6, "H": 10} {
		for i := 1; i <= count; i++ {
			ids[fmt.Sprintf("%s%d", group, i)] = "leaf"
		}
	}
	for _, clause := range []string{"frozen", "identity", "retained", "activation"} {
		ids["C7."+clause] = "leaf"
	}
	for i := 0; i <= 9; i++ {
		ids[fmt.Sprintf("S%d", i)] = "intake"
	}
	for i := 21; i <= 33; i++ {
		ids[fmt.Sprintf("P%d", i)] = "package"
	}
	for _, id := range []string{"ENDPOINT-RESUME", "SS-JOINT-FUZZ", "S5-DOCS-AUDIT", "PTY-INCARNATION", "USER-UNIT", "STAGE-EXITS", "DARWIN-SCOPE"} {
		ids[id] = "carry"
	}
	return ids
}

// Receipt requirements are functional input, never post-run result fields.
// Recompute their union so deleting one cannot make an incomplete claim pass.
// origin: hot-update S10 plan §13.1–§13.2 / P34
func fullstackDocRequiredKeys(kind, id string) []string {
	capKeys := func(capID string) []string {
		var keys []string
		for _, suffix := range []string{"test", "drill", "budget", "identity", "command", "mutation", "documentation"} {
			keys = append(keys, capID+"."+suffix)
		}
		if capID == "CAP11" {
			keys = append(keys, "compatibility.release")
		}
		if capID == "CAP12" {
			keys = append(keys, "final.gates", "final.default", "final.live-grow", "final.review")
		}
		return keys
	}
	if kind == "coverage" {
		return capKeys(id)
	}
	keys := []string{id + "." + kind}
	for i := range fullstackCoverageLeaves {
		matches := kind == "leaf" && slices.Contains(fullstackDocList(fullstackCoverageLeaves[i]), strings.SplitN(id, ".", 2)[0])
		if kind == "requirement" {
			matches = slices.Contains(fullstackDocList(fullstackCoverageRequirements[i]), strings.TrimPrefix(id, "R"))
		}
		if matches {
			keys = append(keys, capKeys(fmt.Sprintf("CAP%02d", i+1))...)
		}
	}
	return keys
}

// A green documentation gate means claims are honest, including unsigned work.
// E3 remains NOT_RUN until P35 seals the candidate and every required row closes.
// origin: hot-update S10 plan §10.2 / P34 / §13.1–§13.2
func fullstackDocumentationProblem(root string) error {
	rows, err := fullstackReadDocRows(root)
	if err != nil {
		return err
	}
	leaves, err := fullstackDocLeaves(root)
	if err != nil {
		return err
	}
	byID := map[string]fullstackDocRow{}
	receipts := map[string]fullstackDocRow{}
	expected := fullstackDocExpectedIDs()
	for _, row := range rows {
		id, kind := row["claim_id"], row["kind"]
		target := byID
		if kind == "receipt" {
			target = receipts
		} else if expected[id] != kind {
			return fmt.Errorf("unexpected coverage claim %s/%s", kind, id)
		}
		if target[id] != nil {
			return fmt.Errorf("duplicate coverage claim %s", id)
		}
		target[id] = row
		if row["upstream_owner"] == "-" || row["final_owner"] != "S10/P35" {
			return fmt.Errorf("missing exact owner for %s", id)
		}
		if row["status"] != "COMPLETE" && row["status"] != "NOT_RUN" {
			return fmt.Errorf("unsupported claim status %s: %s", row["status"], id)
		}
		if row["status"] == "NOT_RUN" && (row["pending_owner"] == "-" || row["receipt_path"] != "-" || row["receipt_sha256"] != "-" || row["candidate_id"] != "-") {
			return fmt.Errorf("unsigned claim needs pending owner and no success receipt: %s", id)
		}
	}
	for id := range expected {
		if byID[id] == nil {
			return fmt.Errorf("missing required claim %s", id)
		}
	}
	candidate := byID["FINAL"]["candidate_id"]
	if byID["FINAL"]["status"] == "COMPLETE" {
		raw, err := fullstackDocRaw(root, byID["FINAL"]["receipt_path"], byID["FINAL"]["receipt_sha256"])
		if err != nil {
			return err
		}
		if candidate != fullstackDocHash(raw) {
			return fmt.Errorf("candidate seal does not match raw digest")
		}
	}
	for id, receipt := range receipts {
		if receipt["status"] != "COMPLETE" || byID["FINAL"]["status"] != "COMPLETE" {
			return fmt.Errorf("unsealed candidate receipt %s", id)
		}
		if err := fullstackCheckDocReceipt(root, candidate, receipt); err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		used := false
		for _, claim := range byID {
			used = used || slices.Contains(fullstackDocList(claim["required_receipt_keys"]), id)
		}
		if !used {
			return fmt.Errorf("receipt is outside the required set: %s", id)
		}
	}
	for id, row := range byID {
		if row["kind"] == "candidate" {
			continue
		}
		keys := fullstackDocList(row["required_receipt_keys"])
		if !slices.Equal(keys, fullstackDocRequiredKeys(row["kind"], id)) {
			return fmt.Errorf("changed required receipt set: %s", id)
		}
		if row["status"] == "COMPLETE" {
			if row["candidate_id"] != candidate || !fullstackDocDigest(candidate) || byID["FINAL"]["status"] != "COMPLETE" {
				return fmt.Errorf("old candidate claim: %s", id)
			}
			for _, key := range keys {
				if receipts[key] == nil {
					return fmt.Errorf("checked leaf or claim without receipt %s: %s", id, key)
				}
			}
		}
		for _, child := range fullstackDocList(row["child_ids"]) {
			if byID[child] == nil || (row["status"] == "COMPLETE" && byID[child]["status"] != "COMPLETE") {
				return fmt.Errorf("unclosed child %s of %s", child, id)
			}
		}
		if row["kind"] == "leaf" {
			leaf, ok := leaves[id]
			if !ok || leaf.heading != row["source_heading"] || fullstackDocHash([]byte(leaf.text)) != row["original_text_digest"] {
				return fmt.Errorf("leaf mapping requires manual review: %s", id)
			}
			if leaf.checked != (row["status"] == "COMPLETE") {
				return fmt.Errorf("checked leaf without receipt or unchecked COMPLETE claim: %s", id)
			}
			delete(leaves, id)
		}
		if row["tests"] == "-" || row["drills"] == "-" || row["budgets"] == "-" || row["documents"] == "-" {
			return fmt.Errorf("missing Test/drill/budget/document mapping: %s", id)
		}
		for _, doc := range fullstackDocList(row["documents"]) {
			path := strings.SplitN(doc, "#", 2)[0]
			if _, err := os.Stat(filepath.Join(root, path)); err != nil {
				return fmt.Errorf("%s document: %w", id, err)
			}
		}
	}
	if len(leaves) != 0 {
		return fmt.Errorf("unmapped tasklist leaves")
	}
	for i, want := range fullstackCoverageRequirements {
		row := byID[fmt.Sprintf("CAP%02d", i+1)]
		var requirements []string
		for _, number := range strings.Split(want, ",") {
			requirements = append(requirements, "R"+number)
		}
		if row["requirements"] != strings.Join(requirements, ",") || row["leaf_ids"] != fullstackCoverageLeaves[i] {
			return fmt.Errorf("coverage row %d differs from frozen R/leaf union", i+1)
		}
	}
	for _, id := range []string{"C7.frozen", "C7.identity", "C7.retained", "C7.activation"} {
		if !slices.Contains(fullstackDocList(byID["C7"]["child_ids"]), id) {
			return fmt.Errorf("C7 omits nested clause %s", id)
		}
	}
	return nil
}

// origin: hot-update S10 plan §10.2 / P34 / DOCS-SYNC
// gate-control: TestFullstackDocumentationReceiptControls
func TestFullstackDocumentationClaimsHaveReceipts(t *testing.T) {
	if err := fullstackDocumentationProblem(repoRoot(t)); err != nil {
		t.Fatal(err)
	}
}

func fullstackWriteDocRows(t *testing.T, root string, rows []fullstackDocRow) {
	t.Helper()
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	w.Comma = '\t'
	if err := w.Write(fullstackDocsColumns); err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		var record []string
		for _, column := range fullstackDocsColumns {
			record = append(record, row[column])
		}
		if err := w.Write(record); err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, fullstackCoveragePath), buf.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}

// Pure on-disk fixtures exercise receipt validation without launching a process
// or presenting their synthetic bytes as a product/deployment success receipt.
func fullstackDocFixture(t *testing.T) (string, []fullstackDocRow) {
	t.Helper()
	root := t.TempDir()
	sourceRows, err := fullstackReadDocRows(repoRoot(t))
	if err != nil {
		t.Fatal(err)
	}
	// Controls remain hermetic after P35 signs real rows: never import actual
	// external receipts or let their candidate IDs affect a synthetic fixture.
	var rows []fullstackDocRow
	for _, row := range sourceRows {
		if row["kind"] == "receipt" {
			continue
		}
		row["status"], row["candidate_id"], row["receipt_path"], row["receipt_sha256"] = "NOT_RUN", "-", "-", "-"
		row["pending_owner"] = row["upstream_owner"]
		rows = append(rows, row)
	}
	for _, path := range []string{fullstackTasklistPath, "docs/reviews/simcluster-coverage-inventory.md"} {
		raw, err := os.ReadFile(filepath.Join(repoRoot(t), path))
		if err != nil {
			t.Fatal(err)
		}
		if path == fullstackTasklistPath {
			raw = regexp.MustCompile(`(?m)^([ \t]*- )\[[xX]\](.* <!-- s10-claim:[^>]+ -->)$`).ReplaceAll(raw, []byte(`${1}[ ]${2}`))
		}
		if err := os.MkdirAll(filepath.Dir(filepath.Join(root, path)), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	for _, row := range rows {
		for _, doc := range fullstackDocList(row["documents"]) {
			path := filepath.Join(root, strings.SplitN(doc, "#", 2)[0])
			if _, err := os.Stat(path); err == nil {
				continue
			}
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("fixture document\n"), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
	return root, rows
}

// origin: hot-update S10 plan §10.2 / P34 / §13.2
func TestFullstackDocumentationReceiptControls(t *testing.T) {
	for _, defect := range []string{"empty-raw", "empty-raw-bytes", "old-candidate", "checked-leaf", "synthetic-nminus", "same-source-stamps", "missing-row", "changed-leaf", "missing-child", "wrong-hash", "missing-required-receipt", "changed-required-set", "wrong-kind"} {
		t.Run(defect, func(t *testing.T) {
			root, rows := fullstackDocFixture(t)
			fullstackWriteDocRows(t, root, rows)
			if err := fullstackDocumentationProblem(root); err != nil {
				t.Fatalf("baseline: %v", err)
			}
			rawDir := t.TempDir()
			write := func(name string, raw []byte) (string, string) {
				path := filepath.Join(rawDir, name)
				if err := os.WriteFile(path, raw, 0600); err != nil {
					t.Fatal(err)
				}
				return path, fullstackDocHash(raw)
			}
			sealPath, sealSHA := write("candidate.json", []byte(`{"fixture":"documentation-control-only"}`))
			for _, row := range rows {
				if row["claim_id"] == "FINAL" {
					row["status"], row["candidate_id"], row["receipt_path"], row["receipt_sha256"] = "COMPLETE", sealSHA, sealPath, sealSHA
				}
			}
			zero := 0
			rawPath, rawSHA := write("native.log", []byte("fixture native rc0\n"))
			r := fullstackDocReceipt{Key: "compatibility.release", Candidate: sealSHA, Kind: "compatibility", Command: []string{"fixture-only"}, NativeRC: &zero, RawPath: rawPath, RawSHA: rawSHA, SourceCommit: strings.Repeat("a", 40), ArtifactSHA: rawSHA, RunningSHA: rawSHA, ArtifactSet: "real-adjacent-release"}
			for i := 0; i < 2; i++ {
				path, digest := write("release"+strconv.Itoa(i), []byte("fixture release "+strconv.Itoa(i)))
				r.Releases = append(r.Releases, struct {
					Version      string `json:"version"`
					SourceCommit string `json:"source_commit"`
					BinaryPath   string `json:"binary_path"`
					BinarySHA    string `json:"binary_sha256"`
				}{"v1.0." + strconv.Itoa(i), strings.Repeat(strconv.Itoa(i+1), 40), path, digest})
			}
			receiptRow := fullstackDocRow{}
			for _, column := range fullstackDocsColumns {
				receiptRow[column] = "-"
			}
			receiptRow["kind"], receiptRow["claim_id"], receiptRow["status"], receiptRow["upstream_owner"], receiptRow["final_owner"], receiptRow["candidate_id"] = "receipt", r.Key, "COMPLETE", "S10/P27", "S10/P35", sealSHA
			storeReceipt := func() {
				raw, err := json.Marshal(r)
				if err != nil {
					t.Fatal(err)
				}
				receiptRow["receipt_path"], receiptRow["receipt_sha256"] = write("receipt.json", raw)
			}
			storeReceipt()
			rows = append(rows, receiptRow)
			// Exercise the COMPLETE path as well as honest unsigned rows.
			packageReceipt := r
			packageReceipt.Key, packageReceipt.Kind, packageReceipt.Releases = "P33.package", "documentation", nil
			packageRaw, err := json.Marshal(packageReceipt)
			if err != nil {
				t.Fatal(err)
			}
			packageRow := fullstackDocRow{}
			for key, value := range receiptRow {
				packageRow[key] = value
			}
			packageRow["claim_id"] = packageReceipt.Key
			packageRow["receipt_path"], packageRow["receipt_sha256"] = write("package-receipt.json", packageRaw)
			rows = append(rows, packageRow)
			for _, row := range rows {
				if row["claim_id"] == "P33" && row["kind"] == "package" {
					row["status"], row["candidate_id"] = "COMPLETE", sealSHA
				}
			}
			fullstackWriteDocRows(t, root, rows)
			if err := fullstackDocumentationProblem(root); err != nil {
				t.Fatalf("positive receipt control: %v", err)
			}
			want := ""
			switch defect {
			case "empty-raw":
				r.RawPath = ""
				want = "empty raw receipt"
			case "empty-raw-bytes":
				r.RawPath, r.RawSHA = write("empty.log", nil)
				want = "empty raw receipt"
			case "old-candidate":
				r.Candidate = strings.Repeat("b", 64)
				want = "old candidate"
			case "synthetic-nminus":
				r.ArtifactSet = "six-mechanism-stamps"
				want = "synthetic N-1"
			case "same-source-stamps":
				r.Releases[1].SourceCommit = r.Releases[0].SourceCommit
				want = "synthetic N-1"
			case "wrong-hash":
				r.RawSHA = strings.Repeat("c", 64)
				want = "invalid raw hash"
			case "missing-required-receipt":
				rows = rows[:len(rows)-1]
				want = "without receipt P33: P33.package"
			case "changed-required-set":
				for _, row := range rows {
					if row["claim_id"] == "CAP01" {
						row["required_receipt_keys"] = "CAP01.test"
					}
				}
				want = "changed required receipt set"
			case "wrong-kind":
				r.Key, receiptRow["claim_id"] = "CAP01.drill", "CAP01.drill"
				want = "receipt kind cannot sign"
			case "checked-leaf", "changed-leaf":
				path := filepath.Join(root, fullstackTasklistPath)
				raw, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				old := ""
				for _, line := range strings.Split(string(raw), "\n") {
					if strings.HasSuffix(line, "<!-- s10-claim:H1 -->") {
						old = line
					}
				}
				newText := strings.Replace(old, "- [ ] ", "- [x] ", 1)
				want = "checked leaf without receipt"
				if defect == "changed-leaf" {
					newText, want = "- [ ] changed scope <!-- s10-claim:H1 -->", "manual review"
				}
				if old == "" {
					t.Fatal("leaf control did not reach H1")
				}
				if err := os.WriteFile(path, []byte(strings.Replace(string(raw), old, newText, 1)), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-row":
				for i, row := range rows {
					if row["claim_id"] == "R16" {
						rows = slices.Delete(rows, i, i+1)
						break
					}
				}
				want = "missing required claim R16"
			case "missing-child":
				for _, row := range rows {
					if row["claim_id"] == "C7" {
						row["child_ids"] = "-"
					}
				}
				want = "C7 omits nested clause"
			}
			storeReceipt()
			fullstackWriteDocRows(t, root, rows)
			if err := fullstackDocumentationProblem(root); err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("%s: want %q, got %v", defect, want, err)
			}
		})
	}
}
