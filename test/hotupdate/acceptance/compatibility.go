// compatibility.go owns the closed compatibility catalog and receipt intake.
// Catalog completeness and product compatibility are separate results: pending
// upstream evidence never becomes success because a capability was advertised.
// origin: hot-update S10 plan §7 / §10.2 / P27
package acceptance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strings"
	"unicode/utf8"

	c "github.com/LinZiyang666/tether/test/hotupdate/acceptance/contract"
)

// CompatibilityMatrix records requirements, including receipts not yet supplied.
// Artifact sets are independent denominators; mechanism stamps cannot fill N-1.
type CompatibilityMatrix struct {
	Schema       string              `json:"schema"`
	Origin       string              `json:"origin"`
	ArtifactSets []string            `json:"artifact_sets"`
	Cells        []CompatibilityCell `json:"cells"`
}

// CompatibilityCell keeps the broker axis and legacy namespaces explicit.
// Requires names behavior witnesses, including actual nonempty decoder input.
type CompatibilityCell struct {
	ID          string            `json:"id"`
	Family      string            `json:"family"`
	ArtifactSet string            `json:"artifact_set"`
	Role        string            `json:"role,omitempty"`
	Ctl         string            `json:"ctl,omitempty"`
	Agent       string            `json:"agent,omitempty"`
	Broker      string            `json:"broker,omitempty"`
	Reader      string            `json:"reader,omitempty"`
	Gate        string            `json:"gate,omitempty"`
	Direction   string            `json:"direction,omitempty"`
	Domain      string            `json:"domain,omitempty"`
	State       string            `json:"state,omitempty"`
	From        string            `json:"from,omitempty"`
	To          string            `json:"to,omitempty"`
	Class       string            `json:"class"`
	Code        string            `json:"code,omitempty"`
	Detail      string            `json:"detail,omitempty"`
	CLIRC       int               `json:"cli_rc"`
	NativeRC    int               `json:"native_rc"`
	ALPN        string            `json:"alpn,omitempty"`
	Requires    []string          `json:"requires"`
	Status      string            `json:"status"`
	Pending     string            `json:"pending,omitempty"`
	Receipt     *CompatibilityRaw `json:"receipt,omitempty"`
}

// CompatibilityRaw uses the disk spelling without changing P00's domain DTO.
type CompatibilityRaw struct {
	Path   string `json:"path"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Schema string `json:"schema"`
}

// CompatibilityProof is a projection made by an upstream schema decoder, not
// a second receipt format. The original bytes and schema stay in Receipt.
// No decoder is synthesized when an upstream receipt schema has not landed.
type CompatibilityProof struct {
	CellID, CandidateID, ArtifactSet, Tier, Class, Code, Detail, ALPN string
	NativeRC, CLIRC                                                   int
	Binaries                                                          []CompatibilityBinary
	Checks                                                            []CompatibilityCheck
}

type CompatibilityBinary struct {
	Slot, Role, Version string
	Artifact            c.Artifact
	ExecutingSHA        c.Digest
	Process             c.Process
	Argv                []string
}

// A named decoder witness binds the executing reader to the exporting image.
// Continued means actual post-import I/O (or preserved business on refusal),
// not parse success. Other witnesses bind their own nonempty input and output.
type CompatibilityCheck struct {
	Name                            string
	Reader, Exporter, Input, Output c.Digest
	Bytes                           uint64
	Continued                       bool
	ReaderEpoch, ExportEpoch        uint64
	ReadEpochs                      []uint64
}

type CompatibilityRead func(c.RawRef) ([]byte, error)
type CompatibilityDecode func(CompatibilityCell, []byte) (CompatibilityProof, error)

const compatibilityOrigin = "hot-update S10 plan §7 / §10.2 / P27"

// CompatibilityRequirements returns fresh storage for the entire frozen set.
// Pending is deliberate: source existence is never a product receipt.
func CompatibilityRequirements() CompatibilityMatrix {
	m := CompatibilityMatrix{Schema: "fullstack-compatibility-v1", Origin: compatibilityOrigin,
		ArtifactSets: []string{"fixed-v0.6.0", "dynamic-adjacent", "capable-a-b", "mechanism-only", "infra-adjacent"}}
	add := func(cell CompatibilityCell, owner string) {
		cell.Status, cell.Pending = "pending", owner+" raw compatibility receipt"
		m.Cells = append(m.Cells, cell)
	}
	// origin: hot-update S10 plan §7 / TestFullstackCompatibilityMatrixIsComplete
	for _, set := range []string{"fixed-v0.6.0", "dynamic-adjacent"} {
		for _, ctl := range []string{"old", "new"} {
			for _, agent := range []string{"old", "new"} {
				for _, broker := range []string{"old", "new"} {
					for _, verb := range []string{"exec", "run", "kill", "expose", "transfer"} {
						provenance := "fixed-release-provenance"
						if set == "dynamic-adjacent" {
							provenance = "adjacent-release-selection"
						}
						add(CompatibilityCell{ID: set + "/W-c" + ctl + "-a" + agent + "-b" + broker + "-" + verb, Family: "wire", ArtifactSet: set,
							Ctl: ctl, Agent: agent, Broker: broker, State: verb, Class: "ordinary", Requires: []string{"actual-effect", provenance}}, "S8 P22/P24/P28")
					}
					alpn := ""
					if agent == "new" && broker == "new" {
						alpn = "tether-flow/1"
					}
					if ctl == "old" {
						add(CompatibilityCell{ID: set + "/ALPN-a" + agent + "-b" + broker, Family: "carrier", ArtifactSet: set,
							Agent: agent, Broker: broker, Class: "ordinary", ALPN: alpn,
							Requires: []string{"register-six-fields", "exact-ok", "bufio-boundary", "original-socket"}}, "S5/S8 carrier intake")
					}
				}
			}
		}
	}
	for _, role := range []string{"agent", "broker"} {
		domains := []string{"exec-v1", "pty-v1", "flow-v1", "ss-v1", "transfer-v1", "upgrade-cut-v1"}
		if role == "broker" {
			domains = []string{"broker-image-v1", "flow-v1", "transfer-v1", "upgrade-cut-v1", "broker-state-v1", "broker-tunnel-state-v1"}
		}
		for _, reader := range []string{"running", "candidate", "recorded-prev"} {
			for _, gate := range []string{"disk-select", "freeze", "finalPrepare"} {
				for _, direction := range []string{"forward", "reverse"} {
					requires := []string{"manifest-strict", "private-adjunct", "runtime-build"}
					for _, domain := range domains {
						requires = append(requires, "decode/"+domain)
					}
					add(CompatibilityCell{ID: "ABI/" + role + "/" + reader + "/" + gate + "/" + direction, Family: "image", ArtifactSet: "capable-a-b",
						Role: role, Reader: reader, Gate: gate, Direction: direction, Class: "hot", Requires: requires}, "S7 P10/P18, S8 P07/P17/P24")
				}
			}
		}
		// Each retained responsibility is nonempty even with no live payload.
		// Requiring decoder proof only for running sockets loses these domains.
		// origin: hot-update S10 plan §7 / TestFullstackCompatibilityMatrixIsComplete
		for _, domain := range domains {
			if domain == "upgrade-cut-v1" || (strings.HasPrefix(domain, "broker-") && domain != "broker-image-v1") {
				continue
			}
			states := []string{"live", "settled-unexpired", "revocation", "cleanup"}
			if domain == "transfer-v1" {
				states = []string{"live", "prepared", "courier-only", "settled-unexpired", "revocation", "cleanup"}
			}
			if domain == "exec-v1" || domain == "pty-v1" {
				states = append(states, "terminal-unacked")
			}
			if domain == "flow-v1" || domain == "broker-image-v1" {
				states = append(states, "idlelistener")
			}
			if domain == "ss-v1" {
				states = append(states, "salt-filter")
			}
			for _, state := range states {
				for _, direction := range []string{"forward", "reverse"} {
					add(CompatibilityCell{ID: "retained/" + role + "/" + domain + "/" + state + "/" + direction, Family: "retained", ArtifactSet: "capable-a-b",
						Role: role, Domain: domain, State: state, Direction: direction, Class: "refusal",
						CLIRC:    -1,
						Requires: []string{"decode/" + domain, "missing-domain-no-take", "business-preserved"}}, "S7/S8 retained-domain intake")
				}
			}
		}
	}
	for _, name := range []string{"public-wire-zero-unknown", "public-image-zero-unknown", "private-closure", "db-schema", "raft-optype", "command-version", "go-hash-same-toolchain", "go-hash-cross-toolchain"} {
		for _, direction := range []string{"forward", "reverse"} {
			add(CompatibilityCell{ID: "storage/" + name + "/" + direction, Family: "storage", ArtifactSet: "capable-a-b", Direction: direction,
				State: name, Class: "hot", Requires: []string{name}}, "S6/S7/S8 decoder/storage intake")
		}
	}
	for _, component := range []string{"nats", "caddy", "helper", "launcher"} {
		old, next := "actual-prev", "candidate"
		assets := []string{"actual-prev", "old-owner-retired"}
		if component == "nats" {
			old, next, assets = "2.14.5", "2.14.6", []string{"config", "store", "consumer", "object", "history"}
		}
		if component == "caddy" {
			old, next, assets = "2.7.5", "2.7.6", []string{"config", "acme-current-tree", "canonical-route"}
		}
		for _, direction := range []string{"forward", "reverse"} {
			from, to := old, next
			if direction == "reverse" {
				from, to = next, old
			}
			for _, asset := range assets {
				add(CompatibilityCell{ID: "infra/" + component + "/" + asset + "/" + direction, Family: "infra", ArtifactSet: "infra-adjacent",
					Role: component, State: asset, From: from, To: to, Direction: direction, Class: "hot", Requires: []string{asset, "actual-runtime"}}, "S9 P01/P20/P26/P28/P29/P33")
			}
		}
	}
	// Do not alias S8 legacy_busy to S4/S5 legacy_session_busy. CLI projection
	// and native oracle result have different meanings and different namespaces.
	// origin: hot-update S10 plan §7 / TestFullstackCompatibilityMatrixIsComplete
	legacy := []struct {
		id, class, code, detail string
		rc                      int
	}{
		{"LB-new", "refusal", "upgrade_rejected", "legacy_busy", 64},
		{"LB-old", "refusal", "bad_request", "legacy_busy", 64},
		{"LI-defer", "refusal", "upgrade_deferred", "legacy_quiescence_unproved", 75},
		{"LI-interrupting", "non_hot", "", "", -1},
		{"M-route", "refusal", "", "", -1}, {"CC-idle", "hot", "", "", 0},
		{"CC-late-create", "refusal", "", "", -1}, {"CL-reject", "refusal", "", "", -1},
	}
	for _, row := range legacy {
		add(CompatibilityCell{ID: "legacy/" + row.id, Family: "legacy", ArtifactSet: "fixed-v0.6.0", State: row.id,
			Class: row.class, Code: row.code, Detail: row.detail, CLIRC: row.rc,
			Requires: []string{"policy-original-namespace", "business-boundary"}}, "S8 P24/P28 / 39."+row.id)
	}
	// Historical 39.X/Y/P/F/BR keep their original native verdicts with P34.
	// They do not fill wire cells or enter this current compatibility success set.
	for _, name := range []string{"ABI-adjacent", "ABI-too-far", "ABI-unknown", "GO-hash", "CFG-nats-old", "CFG-nats-new", "CFG-caddy-old", "CFG-caddy-new"} {
		class, rc := "hot", 0
		if name == "ABI-too-far" || name == "ABI-unknown" {
			class, rc = "refusal", -1
		}
		add(CompatibilityCell{ID: "39/" + name, Family: "upstream", ArtifactSet: "capable-a-b", State: name, Class: class, CLIRC: rc, Requires: []string{name}}, "S8 P24/P28 / 39."+name)
	}
	for _, stage := range []string{"S6", "S7", "S9"} {
		for _, shape := range []string{"single", "sole-voter", "n3"} {
			add(CompatibilityCell{ID: "deployment/" + stage + "/" + shape, Family: "deployment", ArtifactSet: "capable-a-b", State: shape,
				Class: "hot", Requires: []string{"terminal-current", "original-business"}}, stage+" topology receipt")
		}
	}
	for _, shape := range []string{"root-unit", "user-unit", "no-systemd", "colocated", "shared-binary"} {
		add(CompatibilityCell{ID: "deployment/S8/" + shape, Family: "deployment", ArtifactSet: "capable-a-b", State: shape,
			Class: "hot", Requires: []string{"actual-launch", "terminal-current", "shared-domain"}}, "S8 P26/82 U3")
	}
	return m
}

// ValidateCompatibility checks sets rather than a count that duplicates could
// preserve. A changed expected cell is a contract change, not receipt filling.
func ValidateCompatibility(m CompatibilityMatrix) error {
	want := CompatibilityRequirements()
	if m.Schema != want.Schema || m.Origin != want.Origin || !reflect.DeepEqual(m.ArtifactSets, want.ArtifactSets) {
		return errors.New("invalid compatibility catalog identity")
	}
	expected := make(map[string]CompatibilityCell, len(want.Cells))
	for _, cell := range want.Cells {
		expected[cell.ID] = cell
	}
	for _, cell := range m.Cells {
		frozen, exists := expected[cell.ID]
		if !exists {
			return fmt.Errorf("unknown or repeated compatibility cell: %s", cell.ID)
		}
		delete(expected, cell.ID)
		static := cell
		static.Status, static.Pending, static.Receipt = frozen.Status, frozen.Pending, frozen.Receipt
		if !reflect.DeepEqual(static, frozen) {
			return fmt.Errorf("changed compatibility requirement: %s", cell.ID)
		}
		switch cell.Status {
		case "pending":
			if cell.Pending == "" || cell.Receipt != nil {
				return fmt.Errorf("ambiguous pending compatibility cell: %s", cell.ID)
			}
		case "recorded":
			if cell.Pending != "" || cell.Receipt == nil {
				return fmt.Errorf("missing compatibility receipt: %s", cell.ID)
			}
			if _, err := compatibilityRawRef(*cell.Receipt); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown compatibility status: %s", cell.ID)
		}
	}
	if len(expected) != 0 {
		return errors.New("compatibility catalog omits required cells")
	}
	return nil
}

// DecodeCompatibility rejects duplicate, unknown and case-aliased fields before
// Go's JSON decoder can collapse them into an apparently complete matrix.
func DecodeCompatibility(data []byte) (CompatibilityMatrix, error) {
	var m CompatibilityMatrix
	if len(data) > 4<<20 || !utf8.Valid(data) {
		return m, errors.New("invalid compatibility JSON size or encoding")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	if err := compatibilityJSONValue(d, 0); err != nil {
		return m, err
	}
	if _, err := d.Token(); err != io.EOF {
		return m, errors.New("trailing compatibility JSON")
	}
	d = json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&m); err != nil {
		return m, err
	}
	return m, ValidateCompatibility(m)
}

func compatibilityJSONValue(d *json.Decoder, depth int) error {
	if depth > 32 {
		return errors.New("compatibility JSON is too deep")
	}
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, compound := token.(json.Delim)
	if !compound {
		return nil
	}
	seen := map[string]bool{}
	for d.More() {
		if delim == '{' {
			key, err := d.Token()
			if err != nil {
				return err
			}
			name, ok := key.(string)
			if !ok || name != strings.ToLower(name) || seen[name] {
				return errors.New("duplicate or aliased compatibility field")
			}
			seen[name] = true
		}
		if err := compatibilityJSONValue(d, depth+1); err != nil {
			return err
		}
	}
	_, err = d.Token()
	return err
}

func compatibilityRawRef(raw CompatibilityRaw) (c.RawRef, error) {
	var ref c.RawRef
	hash, err := hex.DecodeString(raw.SHA256)
	if err != nil || len(hash) != 32 || raw.SHA256 != hex.EncodeToString(hash) || raw.Path == "" || raw.Schema == "" || raw.Bytes <= 0 || raw.Bytes > 4<<20 {
		return ref, errors.New("invalid compatibility raw reference")
	}
	copy(ref.SHA[:], hash)
	if ref.SHA == (c.Digest{}) {
		return ref, errors.New("empty compatibility raw digest")
	}
	ref.Path, ref.Schema, ref.Bytes = raw.Path, raw.Schema, raw.Bytes
	return ref, nil
}

// JudgeCompatibility verifies the original raw bytes before asking the owning
// upstream decoder for a typed projection. An absent decoder fails closed.
// Read is supplied by the evidence archive owner; this judge never opens paths
// or executes argv, and cannot promote a hermetic receipt to deployment proof.
func JudgeCompatibility(m CompatibilityMatrix, candidateID string, read CompatibilityRead, decode CompatibilityDecode) (c.Result, error) {
	r := c.Result{Verdict: "GREEN"}
	versions := map[string]c.Artifact{}
	if err := ValidateCompatibility(m); err != nil {
		r.Verdict, r.RC = "SETUP-RED", 2
		return r, err
	}
	if candidateID == "" {
		r.Verdict, r.RC = "SETUP-RED", 2
		return r, errors.New("missing compatibility candidate identity")
	}
	for _, cell := range m.Cells {
		if cell.Status == "pending" {
			r.Missing = append(r.Missing, cell.ID)
			continue
		}
		ref, _ := compatibilityRawRef(*cell.Receipt)
		r.Inputs = append(r.Inputs, ref)
		if read == nil || decode == nil {
			r.Failures = append(r.Failures, cell.ID+": upstream decoder unavailable")
			continue
		}
		data, err := read(ref)
		if err != nil || int64(len(data)) != ref.Bytes || sha256.Sum256(data) != ref.SHA {
			r.Failures = append(r.Failures, cell.ID+": raw receipt mismatch")
			continue
		}
		decoderCell := cell
		decoderCell.Requires = slices.Clone(cell.Requires)
		rawCopy := *cell.Receipt
		decoderCell.Receipt = &rawCopy
		proof, err := decode(decoderCell, data)
		if err != nil || !compatibilityProofMatches(cell, proof, candidateID) {
			r.Failures = append(r.Failures, cell.ID+": incomplete compatibility behavior or identity")
			continue
		}
		for _, binary := range proof.Binaries {
			if binary.Version != "old" && binary.Version != "new" {
				continue
			}
			key := cell.ArtifactSet + "/" + binary.Role + "/" + binary.Version
			previous, seen := versions[key]
			if seen && (previous.Binary != binary.Artifact.Binary || previous.SourceCommit != binary.Artifact.SourceCommit || previous.Tree != binary.Artifact.Tree) {
				r.Failures = append(r.Failures, cell.ID+": contradictory release provenance")
			}
			versions[key] = binary.Artifact
		}
	}
	for key, artifact := range versions {
		if !strings.HasSuffix(key, "/old") {
			continue
		}
		next, seen := versions[strings.TrimSuffix(key, "/old")+"/new"]
		if seen && (artifact.Binary == next.Binary || artifact.SourceCommit == next.SourceCommit || artifact.Tree == next.Tree || artifact.Release == next.Release) {
			r.Failures = append(r.Failures, key+": mechanism stamps do not prove release compatibility")
		}
	}
	slices.Sort(r.Failures)
	if len(r.Missing) != 0 {
		r.Verdict, r.RC = "INCOMPLETE", 4
	}
	if len(r.Failures) != 0 {
		r.Verdict, r.RC = "ASSERT-FAIL", 1
	}
	return r, nil
}

func compatibilityProofMatches(cell CompatibilityCell, proof CompatibilityProof, candidate string) bool {
	if proof.CellID != cell.ID || proof.CandidateID != candidate || proof.ArtifactSet != cell.ArtifactSet || proof.Tier != "deploy" ||
		proof.Class != cell.Class || proof.NativeRC != cell.NativeRC || (cell.CLIRC >= 0 && proof.CLIRC != cell.CLIRC) ||
		(cell.Code != "" && proof.Code != cell.Code) || (cell.Detail != "" && proof.Detail != cell.Detail) || proof.ALPN != cell.ALPN {
		return false
	}
	if proof.CLIRC < 0 || proof.CLIRC > 255 || (cell.Class == "refusal" && (!slices.Contains([]int{64, 74, 75, 77}, proof.CLIRC) || proof.Code == "")) {
		return false
	}
	if cell.Class == "hot" || cell.Class == "ordinary" {
		if proof.Code != "" || proof.Detail != "" {
			return false
		}
	}
	roles := compatibilityBinarySlots(cell)
	bound := map[c.Digest]bool{}
	identities := map[string]CompatibilityBinary{}
	for _, binary := range proof.Binaries {
		expected, exists := roles[binary.Slot]
		a := binary.Artifact
		p := binary.Process
		if !exists || expected[0] != binary.Role || (expected[1] != "" && expected[1] != binary.Version) || a.Role != binary.Role || a.SourceCommit == "" || a.Toolchain == "" || a.Release == "" ||
			a.Tree == (c.Digest{}) || a.Flags == (c.Digest{}) || a.Archive == (c.Digest{}) || a.CopyManifest == (c.Digest{}) || a.Binary == (c.Digest{}) || a.Binary != binary.ExecutingSHA ||
			p.Key.Role != binary.Role || p.PID <= 0 || p.StartTicks == 0 || p.BootID == "" || p.PIDNS == "" || p.InstanceID == "" || p.Key.Host == "" || len(binary.Argv) == 0 {
			return false
		}
		if slices.Contains([]string{"T0", "T1", "T2", "T3", "T4", "T5", "gen-1"}, a.ID) && cell.ArtifactSet != "mechanism-only" {
			return false
		}
		if binary.Version == "old" && cell.ArtifactSet == "fixed-v0.6.0" && (a.Release != "v0.6.0" || a.SourceCommit != "a3431a12be3cf947cc797c28fea93382fa68472d") {
			return false
		}
		if pinned := compatibilityInfraSHA(binary.Role, binary.Version); cell.Family == "infra" && pinned != "" && hex.EncodeToString(a.Binary[:]) != pinned {
			return false
		}
		delete(roles, binary.Slot)
		identities[binary.Slot] = binary
		bound[a.Binary] = true
	}
	if len(roles) != 0 {
		return false
	}
	reader, exporter := identities["reader"], identities["exporter"]
	if reader.Slot != "" {
		// A running reader can decode its own export. Distinct implementations
		// are proved by the candidate cells, not required for a hash self-import.
		if cell.Family == "image" && cell.Reader == "candidate" && (reader.Artifact.SourceCommit == exporter.Artifact.SourceCommit || reader.Artifact.Tree == exporter.Artifact.Tree || reader.ExecutingSHA == exporter.ExecutingSHA) {
			return false
		}
		if cell.State == "go-hash-cross-toolchain" && reader.Artifact.Toolchain == exporter.Artifact.Toolchain {
			return false
		}
		if cell.State == "go-hash-same-toolchain" && reader.Artifact.Toolchain != exporter.Artifact.Toolchain {
			return false
		}
	}
	if cell.Family == "infra" && identities["from"].ExecutingSHA == identities["to"].ExecutingSHA {
		return false
	}
	checks := map[string]CompatibilityCheck{}
	for _, check := range proof.Checks {
		if _, duplicate := checks[check.Name]; duplicate || !slices.Contains(cell.Requires, check.Name) || check.Bytes == 0 || !check.Continued || check.Input == (c.Digest{}) || check.Output == (c.Digest{}) {
			return false
		}
		// origin: hot-update S10 plan §7 / TestFullstackCompatibilityMatrixIsComplete
		if strings.HasPrefix(check.Name, "decode/") {
			if !bound[check.Reader] || !bound[check.Exporter] || check.Reader != reader.ExecutingSHA || check.Exporter != exporter.ExecutingSHA || check.ReaderEpoch == 0 || check.ExportEpoch == 0 || !slices.Contains(check.ReadEpochs, check.ExportEpoch) {
				return false
			}
			low, high := min(check.ReaderEpoch, check.ExportEpoch), max(check.ReaderEpoch, check.ExportEpoch)
			if high-low > 1 {
				return false
			}
			a := reader.Artifact
			domain := strings.TrimPrefix(check.Name, "decode/")
			if a.Epoch != check.ReaderEpoch || exporter.Artifact.Epoch != check.ExportEpoch ||
				(len(a.ReadEpochs) != 0 && !reflect.DeepEqual(a.ReadEpochs, check.ReadEpochs)) ||
				(len(a.ReadEpochs) == 0 && check.ExportEpoch != check.ReaderEpoch) {
				return false
			}
			if domain != "broker-state-v1" && domain != "broker-tunnel-state-v1" && !slices.Contains(a.Domains, domain) {
				return false
			}
		}
		if slices.Contains([]string{"db-schema", "raft-optype", "command-version", "go-hash-same-toolchain", "go-hash-cross-toolchain"}, check.Name) && check.Input != check.Output {
			return false
		}
		checks[check.Name] = check
	}
	for _, required := range cell.Requires {
		if _, exists := checks[required]; !exists {
			return false
		}
	}
	return true
}

// These are frozen S9 binary identities, not version output supplied by a
// process. The source-fact control joins them to S9's actual catalog bytes.
// origin: hot-update S10 plan §7 / S9 plan §3.1
func compatibilityInfraSHA(role, version string) string {
	return map[string]string{
		"nats/2.14.5": "e1a2f9ba25077f4cf753bee829483bd68fbf0a4eec9b6645a1e5785a6de0c0d1",
		"nats/2.14.6": "2d0dc83e0547185a6820377114215d6a7c645f607aeb31728fffaf3d84e77c10",
		"caddy/2.7.5": "f7706787b843d375d1326cc451d53f48900b7a14f3a9c606d49d7751b390609f",
		"caddy/2.7.6": "db3bfc85bb93160f60fa6df9c3ebf2340dc11740e9a52c717d88a14c0430f229",
	}[role+"/"+version]
}

// Slots distinguish exporter from reader even when both execute the same role.
// In particular, broker evidence cannot be supplied by a second agent receipt.
func compatibilityBinarySlots(cell CompatibilityCell) map[string][2]string {
	if cell.Family == "wire" {
		return map[string][2]string{"ctl": {"ctl", cell.Ctl}, "agent": {"agent", cell.Agent}, "broker": {"broker", cell.Broker}}
	}
	if cell.Family == "carrier" {
		return map[string][2]string{"agent": {"agent", cell.Agent}, "broker": {"broker", cell.Broker}}
	}
	if cell.Family == "image" || cell.Family == "retained" || cell.Family == "storage" || cell.Family == "upstream" {
		role := cell.Role
		if role == "" {
			role = "agent"
		}
		return map[string][2]string{"reader": {role, ""}, "exporter": {role, ""}}
	}
	if cell.Family == "infra" {
		role := cell.Role
		if role == "helper" || role == "launcher" {
			role = "support"
		}
		return map[string][2]string{"from": {role, cell.From}, "to": {role, cell.To}}
	}
	if cell.Family == "legacy" {
		if cell.State == "LB-new" || cell.State == "LB-old" || cell.State == "LI-defer" {
			ctl := "new"
			if cell.State == "LB-old" {
				ctl = "old"
			}
			return map[string][2]string{"ctl": {"ctl", ctl}, "agent": {"agent", "old"}, "broker": {"broker", "new"}}
		}
	}
	return map[string][2]string{"ctl": {"ctl", ""}, "agent": {"agent", ""}, "broker": {"broker", ""}}
}
