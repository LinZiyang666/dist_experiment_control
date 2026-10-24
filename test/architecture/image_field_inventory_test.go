package architecture

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/adminsock"
	"github.com/LinZiyang666/tether/internal/agent/ssproxy"
	"github.com/LinZiyang666/tether/internal/agentimage"
	"github.com/LinZiyang666/tether/internal/brokerimage"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/proto"
	"github.com/LinZiyang666/tether/internal/storage"
	"github.com/LinZiyang666/tether/internal/tunnel"
	"github.com/LinZiyang666/tether/internal/xfercontract"
)

var updateImageInventory = flag.Bool("update-image-inventory", false,
	"append newly added bundle fields to testdata/image_field_inventory.txt (refuses to drop entries)")

// imageInventoryPath is the append-only ledger of every field reachable from
// the public image and local admin roots through named struct types: one line per field,
// `pkg.Type.Field<TAB>json tag<TAB>Go type`.
const imageInventoryPath = "testdata/image_field_inventory.txt"

// origin: docs/reviews/hot-update-agent-image-abi-plan.md §2.6 / §6 (Q4) —
// the mechanical half of the image schema's N-1 rule (architecture §22.5).
//
// TestImageFieldInventoryAppendOnly pins the SET of fields an agent image can
// carry, append-only: deleting a field, renaming it, or changing its json tag
// or type makes the ledger entry dangle and the test red. Adding a field only
// requires re-running with -update-image-inventory — the updater appends but
// REFUSES to drop entries, the same friction as the wire ledger: the only way
// to shrink the schema is to hand-edit the ledger and justify it in the
// commit message (an image epoch, ImageVersion/BundleVersion bump).
//
// The walk is by reflection over the public roots, so it sees every domain's exported
// state (execsession, procrequest, workload, spawnsafe, proto) exactly as the
// encoder does, not just this package. Unexported fields and non-struct
// leaves are not schema. "omitempty and legal zero value" is checked by the
// importers' behavioural tests, not here (a false badge otherwise).
//
// origin: hot-update S6 plan §2.1 / §9.1 / §13 / T47 — retain the agent root and add independent broker/admin roots.
// gate-control: TestImageInventoryCollectorSeesNestedFields
func TestImageFieldInventoryAppendOnly(t *testing.T) {
	current := collectPublicImageFields()
	if len(current) < 60 {
		t.Fatalf("collected only %d image fields — the collector is broken (a silent short list would let deletions through)", len(current))
	}
	currentSet := make(map[string]bool, len(current))
	for _, l := range current {
		currentSet[l] = true
	}
	raw, err := os.ReadFile(imageInventoryPath)
	if err != nil {
		if os.IsNotExist(err) && *updateImageInventory {
			writeImageInventory(t, current)
			return
		}
		t.Fatalf("read %s: %v (bootstrap with -update-image-inventory)", imageInventoryPath, err)
	}
	var ledger []string
	identities := map[string]bool{}
	for i, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if len(strings.Split(line, "\t")) != 3 {
			t.Fatalf("%s:%d: malformed ledger line %q", imageInventoryPath, i+1, line)
		}
		identity := strings.Split(line, "\t")[0]
		if identities[identity] {
			t.Fatalf("%s:%d: duplicate field identity %s", imageInventoryPath, i+1, identity)
		}
		identities[identity] = true
		ledger = append(ledger, line)
	}
	var dangling []string
	for _, l := range ledger {
		if !currentSet[l] {
			dangling = append(dangling, l)
		}
	}
	if len(dangling) > 0 {
		t.Fatalf("image field inventory: %d ledger entr(ies) no longer match any live field:\n  %s\n"+
			"A bundle field was deleted, renamed, re-typed, or its json tag changed. Inside an image epoch "+
			"that strands the image the previous generation exported (architecture §22.5). If this is a "+
			"deliberate EPOCH change (ImageVersion / BundleVersion bump), hand-edit %s and justify the shrink "+
			"in the commit message — -update-image-inventory will not do it for you.",
			len(dangling), strings.Join(dangling, "\n  "), imageInventoryPath)
	}
	ledgerSet := make(map[string]bool, len(ledger))
	for _, l := range ledger {
		ledgerSet[l] = true
	}
	var added []string
	for _, l := range current {
		if !ledgerSet[l] {
			added = append(added, l)
		}
	}
	if len(added) > 0 {
		if *updateImageInventory {
			writeImageInventory(t, current)
			return
		}
		t.Fatalf("image field inventory: %d new field(s) not yet in the ledger:\n  %s\n"+
			"New fields must follow their domain compatibility contract, then be recorded:\n"+
			"  go test ./test/architecture -run TestImageFieldInventoryAppendOnly -update-image-inventory",
			len(added), strings.Join(added, "\n  "))
	}
}

// The transfer contract re-exports its values by alias from dependency-free
// subpackages (xferport, xfervalue) so value-only importers stay NATS-free.
// The alias is the same type, and its package path is not part of the encoded
// bytes, so rows name such types by the contract API they are reached through:
// moving a declaration between contract subpackages must not churn the ledgers
// (it did twice in S7, flipping every transfer row without changing one byte).
// Only internal/xfercontract/* is normalized; every other package keeps its name.
// origin: hot-update S7 plan §8.1 / §8.3 / T48 (s7-p9i value split, R4)
const imageContractTree = "github.com/LinZiyang666/tether/internal/xfercontract/"

func imagePackageName(t reflect.Type) string {
	if strings.HasPrefix(t.PkgPath(), imageContractTree) {
		return "xfercontract"
	}
	return t.PkgPath()[strings.LastIndex(t.PkgPath(), "/")+1:]
}

func imageTypeName(t reflect.Type) string {
	if t.Name() != "" {
		if strings.HasPrefix(t.PkgPath(), imageContractTree) {
			return "xfercontract." + t.Name()
		}
		return t.String()
	}
	switch t.Kind() {
	case reflect.Ptr:
		return "*" + imageTypeName(t.Elem())
	case reflect.Slice:
		return "[]" + imageTypeName(t.Elem())
	case reflect.Array:
		return fmt.Sprintf("[%d]%s", t.Len(), imageTypeName(t.Elem()))
	case reflect.Map:
		return "map[" + imageTypeName(t.Key()) + "]" + imageTypeName(t.Elem())
	}
	return t.String()
}

// collectImageFields walks every named struct type reachable from root
// through fields, slices, arrays, maps and pointers, and returns one sorted
// line per exported field.
func collectImageFields(root reflect.Type) []string {
	seen := map[reflect.Type]bool{}
	var lines []string
	var walk func(t reflect.Type)
	walk = func(t reflect.Type) {
		switch t.Kind() {
		case reflect.Ptr, reflect.Slice, reflect.Array:
			walk(t.Elem())
			return
		case reflect.Map:
			walk(t.Key())
			walk(t.Elem())
			return
		case reflect.Struct:
		default:
			return
		}
		if t.Name() == "" || seen[t] {
			return
		}
		if !strings.HasPrefix(t.PkgPath(), "github.com/LinZiyang666/tether/") {
			// A standard-library leaf (time.Time, syscall.Rusage) is recorded by
			// the field that names it, not walked: its layout is the toolchain's
			// and differs per platform (Rusage is int32 on darwin, int64 on linux).
			return
		}
		seen[t] = true
		pkg := imagePackageName(t)
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if !f.IsExported() {
				// encoding/json promotes the exported fields of an UNEXPORTED
				// embedded struct (typeFields skips unexported anonymous
				// fields only when they are not structs); the ledger must see
				// what the encoder writes (S2 internal review I4).
				if f.Anonymous && (f.Type.Kind() == reflect.Struct || f.Type.Kind() == reflect.Ptr && f.Type.Elem().Kind() == reflect.Struct) {
					walk(f.Type)
				}
				continue
			}
			tag := "-"
			if f.Tag != "" {
				tag = string(f.Tag)
			}
			lines = append(lines, fmt.Sprintf("%s.%s.%s\t%s\t%s", pkg, t.Name(), f.Name, tag, imageTypeName(f.Type)))
			walk(f.Type)
		}
	}
	walk(root)
	sort.Strings(lines)
	return lines
}

// Keep the original bytes, including comments and row order. A regeneration must
// not hide a changed old field by replacing the very evidence that pins it.
// origin: hot-update S6 plan §9.1 / T47
func writeImageInventory(t *testing.T, lines []string) {
	t.Helper()
	raw, err := os.ReadFile(imageInventoryPath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	updated, err := appendImageInventory(raw, lines)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(imageInventoryPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(imageInventoryPath, updated, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s (%d fields)", imageInventoryPath, len(lines))
}

// Roots are explicit even when one currently reaches another. Removing a
// Bundle member cannot erase the independently owned store/listener vocabulary.
// Local admin and signed maintenance messages sit outside proto's wire scanner.
// origin: hot-update S6 plan §2.1 / §9.1 / §13 / T47
func publicImageRoots() []reflect.Type {
	return []reflect.Type{
		reflect.TypeOf(agentimage.Bundle{}),
		reflect.TypeOf(brokerimage.Bundle{}),
		reflect.TypeOf(storage.StoreStamp{}), reflect.TypeOf(storage.StoreSeal{}),
		reflect.TypeOf(tunnel.ServerImage{}), reflect.TypeOf(listenerimage.Descriptor{}),
		reflect.TypeOf(proto.BrokerImageStatus{}),
		reflect.TypeOf(proto.BrokerMaintenanceReq{}), reflect.TypeOf(proto.BrokerMaintenanceAck{}),
		reflect.TypeOf(adminsock.Request{}), reflect.TypeOf(adminsock.Response{}),
		reflect.TypeOf(adminsock.RuntimeReport{}),
	}
}

func collectPublicImageFields() []string {
	unique := map[string]bool{}
	for _, root := range publicImageRoots() {
		for _, row := range collectImageFields(root) {
			unique[row] = true
		}
	}
	var rows []string
	for row := range unique {
		rows = append(rows, row)
	}
	sort.Strings(rows)
	return rows
}

func appendImageInventory(raw []byte, current []string) ([]byte, error) {
	live, recorded := map[string]string{}, map[string]bool{}
	for _, row := range current {
		parts := strings.Split(row, "\t")
		if len(parts) != 3 || live[parts[0]] != "" {
			return nil, fmt.Errorf("malformed or duplicate current field: %s", row)
		}
		live[parts[0]] = row
	}
	if len(live) == 0 {
		return nil, fmt.Errorf("empty current image inventory")
	}
	for _, row := range strings.Split(string(raw), "\n") {
		if row == "" || strings.HasPrefix(row, "#") {
			continue
		}
		parts := strings.Split(strings.TrimSuffix(row, "\r"), "\t")
		if len(parts) != 3 || recorded[parts[0]] || live[parts[0]] != strings.TrimSuffix(row, "\r") {
			return nil, fmt.Errorf("refusing image inventory deletion/type/tag drift or duplicate: %s", row)
		}
		recorded[parts[0]] = true
	}
	out := append([]byte(nil), raw...)
	if len(out) == 0 {
		out = []byte("# Append-only public agent/broker image and local admin field inventory.\n# pkg.Type.Field<TAB>struct tag<TAB>Go type\n")
	}
	if out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}
	var added []string
	for identity, row := range live {
		if !recorded[identity] {
			added = append(added, row)
		}
	}
	sort.Strings(added)
	for _, row := range added {
		out = append(out, []byte(row+"\n")...)
	}
	return out, nil
}

// origin: hot-update S6 plan §11 / T47
func TestBrokerPublicImageInventoryControls(t *testing.T) {
	rows := collectPublicImageFields()
	var rootNames []string
	for _, root := range publicImageRoots() {
		rootNames = append(rootNames, root.String())
	}
	sort.Strings(rootNames)
	wantRoots := []string{"adminsock.Request", "adminsock.Response", "adminsock.RuntimeReport", "agentimage.Bundle", "brokerimage.Bundle", "listenerimage.Descriptor", "proto.BrokerImageStatus", "proto.BrokerMaintenanceAck", "proto.BrokerMaintenanceReq", "storage.StoreSeal", "storage.StoreStamp", "tunnel.ServerImage"}
	if !reflect.DeepEqual(rootNames, wantRoots) {
		t.Fatalf("public image roots drifted: %v", rootNames)
	}
	for _, root := range publicImageRoots() {
		for _, row := range collectImageFields(root) {
			if !slicesContainsImageRow(rows, row) {
				t.Fatalf("public inventory lost independent root %s: %s", root, row)
			}
		}
	}
	// The frozen root list itself is an invariant: comparing its current union
	// against itself would not detect a root accidentally dropped by this gate.
	for _, identity := range []string{
		"agentimage.Bundle.Runtime", "brokerimage.Bundle.Store", "storage.StoreSeal.Role",
		"tunnel.ServerImage.OwnerGeneration", "listenerimage.Descriptor.UnixPathInode",
		"proto.BrokerImageStatus.RunningSHA256", "proto.BrokerMaintenanceReq.Record",
		"proto.BrokerMaintenanceAck.Nonce", "adminsock.Request.Op",
		"adminsock.Response.OK", "adminsock.RuntimeReport.SchemaVersion",
	} {
		found := false
		for _, row := range rows {
			found = found || strings.Split(row, "\t")[0] == identity
		}
		if !found {
			t.Fatalf("public inventory lost frozen root field %s", identity)
		}
	}
	old := []byte("# keep original comment\nfixture.Image.ID\tjson:\"id\"\tstring\n")
	base := []string{"fixture.Image.ID\tjson:\"id\"\tstring"}
	updated, err := appendImageInventory(old, append(append([]string{}, base...), "fixture.Image.Next\tjson:\"next\"\tuint64"))
	if err != nil || !strings.HasPrefix(string(updated), string(old)) {
		t.Fatalf("additive updater replaced old evidence: %v", err)
	}
	for _, defect := range [][]string{nil, {"fixture.Image.ID\tjson:\"changed\"\tstring"}, {"fixture.Image.ID\tjson:\"id\"\tuint64"}, {base[0], base[0]}} {
		if _, err := appendImageInventory(old, defect); err == nil {
			t.Fatalf("append-only updater accepted deletion/type/tag/duplicate defect: %v", defect)
		}
	}
	if _, err := appendImageInventory(append(append([]byte{}, old...), []byte(base[0]+"\n")...), base); err == nil {
		t.Fatal("duplicate ledger field accepted")
	}
}

// TestImageInventoryCollectorSeesNestedFields is the gate's positive and
// negative control: the collector must reach fields nested through every
// container shape the bundle uses (pointer, slice, array, map value) and
// must not report unexported fields or leaf types.
func TestImageInventoryCollectorSeesNestedFields(t *testing.T) {
	type leaf struct {
		Seen   int `json:"seen"`
		hidden int
	}
	_ = leaf{}.hidden // the negative control: an unexported field must NOT appear in the inventory
	type mid struct {
		Ptr   *leaf
		List  []leaf
		Arr   [2]leaf
		M     map[string]leaf
		Plain leaf
	}
	type promoted struct {
		Seen  int `json:"promoted_seen"`
		inner int
	}
	type top struct {
		Mid  mid
		Deep *mid
		promoted
	}
	_ = promoted{}.inner // negative control inside the promoted struct
	got := collectImageFields(reflect.TypeOf(top{}))
	joined := strings.Join(got, "\n")
	for _, want := range []string{"architecture.top.Mid\t-\tarchitecture.mid", "architecture.top.Deep\t-\t*architecture.mid",
		"architecture.mid.Ptr\t-\t*architecture.leaf", "architecture.mid.M\t-\tmap[string]architecture.leaf",
		"architecture.leaf.Seen\tjson:\"seen\"\tint"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("collector missed %q:\n%s", want, joined)
		}
	}
	if strings.Contains(joined, "hidden") || strings.Contains(joined, ".inner") {
		t.Fatalf("collector reported an unexported field:\n%s", joined)
	}
	if !strings.Contains(joined, "architecture.promoted.Seen\tjson:\"promoted_seen\"\tint") {
		t.Fatalf("collector missed the promoted field of an unexported embedded struct (encoding/json writes it):\n%s", joined)
	}
	if n := strings.Count(joined, "architecture.leaf.Seen"); n != 1 {
		t.Fatalf("leaf visited %d times, want once", n)
	}
	// The real bundle reaches every domain.
	real := strings.Join(collectImageFields(reflect.TypeOf(agentimage.Bundle{})), "\n")
	for _, want := range []string{"execsession.Image.Registry", "procrequest.State.Secret", "workload.ProcessState.Records",
		"workload.OutputState.Discarding", "spawnsafe.MountVerdict.Signature", "proto.ProcessLease.MAC", "agentimage.CourierEvent.Parked"} {
		if !strings.Contains(real, want) {
			t.Fatalf("bundle inventory does not reach %q", want)
		}
	}
	// Anti-vacuity of the ledger itself: it is non-empty and every line has three columns.
	raw, err := os.ReadFile(imageInventoryPath)
	if err != nil {
		t.Fatal(err)
	}
	lines := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if len(strings.Split(line, "\t")) != 3 {
			t.Fatalf("malformed ledger line %q", line)
		}
		lines++
	}
	if lines < 60 {
		t.Fatalf("ledger has %d lines", lines)
	}
}

// Bundle must reach the real public Flow and SS owners. Empty active counts do
// not erase retained retirement or salt responsibility; nil domains alone carry
// the legal old-reader zero value. Secret state is never a public image field.
// origin: hot-update S5 plan §5.1 / §6.6 / §10.1 / T49.
// gate-control: TestFlowSSImageInventoryControls
func TestFlowSSImageInventoryReachesPublicDomains(t *testing.T) {
	bundle := reflect.TypeOf(agentimage.Bundle{})
	for _, domain := range []struct {
		name, tag string
		typ       reflect.Type
	}{
		{"Flow", `json:"flow,omitempty"`, reflect.TypeOf((*tunnel.ClientImage)(nil))},
		{"SS", `json:"ss,omitempty"`, reflect.TypeOf((*ssproxy.Image)(nil))},
	} {
		field, ok := bundle.FieldByName(domain.name)
		if !ok || field.Type != domain.typ || string(field.Tag) != domain.tag {
			t.Errorf("pending P17: Bundle.%s must be %s with %s", domain.name, domain.typ, domain.tag)
		}
	}
	actual := collectImageFields(bundle)
	for _, root := range []reflect.Type{reflect.TypeOf(tunnel.ClientImage{}), reflect.TypeOf(ssproxy.Image{})} {
		for _, row := range collectImageFields(root) {
			if !slicesContainsImageRow(actual, row) {
				t.Errorf("Bundle inventory does not reach %s", row)
			}
		}
	}
}

func slicesContainsImageRow(rows []string, row string) bool {
	for _, candidate := range rows {
		if candidate == row {
			return true
		}
	}
	return false
}

// Keep the public root walk shared with the existing append-only inventory.
// Private worker/tracker data and settlement capsules must stay behind Ref.
// origin: hot-update S7 plan §8.3 / §11.2 / T48
// gate-control: TestTransferPublicImageInventoryControls
func transferPublicImageRows(t *testing.T) []string {
	t.Helper()
	for _, typ := range []reflect.Type{reflect.TypeFor[agentimage.Bundle](), reflect.TypeFor[brokerimage.Bundle]()} {
		field, ok := typ.FieldByName("Transfers")
		if !ok || field.Type != reflect.TypeFor[*xfercontract.ImageRef]() || string(field.Tag) != `json:"transfers,omitempty"` {
			t.Errorf("P21/P24: %s.Transfers must be *xfercontract.ImageRef with json transfers,omitempty", typ)
		}
	}
	var rows []string
	for _, row := range collectPublicImageFields() {
		if strings.HasPrefix(row, "xfercontract.") || strings.Contains(row, ".Transfers\t") {
			rows = append(rows, row)
		}
	}
	if err := checkTransferPublicRows(rows); err != nil {
		t.Error(err)
	}
	return rows
}

func checkTransferPublicRows(rows []string) error {
	for _, row := range rows {
		if strings.HasPrefix(row, "xfercontract.") && !strings.HasPrefix(row, "xfercontract.ImageRef.") {
			return fmt.Errorf("private transfer state escaped public reference: %s", strings.Split(row, "\t")[0])
		}
	}
	return nil
}

// origin: hot-update S7 plan §8.3 / T48
func TestTransferPublicImageInventoryControls(t *testing.T) {
	type public struct {
		Transfers *xfercontract.ImageRef `json:"transfers,omitempty"`
	}
	actual := collectImageFields(reflect.TypeFor[public]())
	if err := checkTransferPublicRows(actual); err != nil {
		t.Fatal(err)
	}
	if checkTransferPublicRows(append(append([]string{}, actual...), "xfercontract.SettlementCapsule.Signature\t-\tstring")) == nil {
		t.Fatal("private transfer authentication escaped into a public carrier")
	}
	for _, row := range collectImageFields(reflect.TypeFor[xfercontract.ImageRef]()) {
		if !slicesContainsImageRow(actual, row) {
			t.Fatalf("public root lost nested reference %s", row)
		}
	}
	for _, row := range actual {
		if strings.Contains(row, "WorkerState") || strings.Contains(row, "SettlementCapsule") {
			t.Fatal("private state entered public root")
		}
	}
	// Contract-tree hosts collapse to the API name through every composite
	// shape; any other package keeps its own name.
	// origin: hot-update S7 plan §8.1 / §8.3 / T48 (s7-p9i value split, R4)
	for typ, want := range map[reflect.Type]string{
		reflect.TypeFor[xfercontract.ImageRef]():                        "xfercontract.ImageRef",
		reflect.TypeFor[*xfercontract.ImageRef]():                       "*xfercontract.ImageRef",
		reflect.TypeFor[[]xfercontract.ImageRef]():                      "[]xfercontract.ImageRef",
		reflect.TypeFor[[2]*xfercontract.ImageRef]():                    "[2]*xfercontract.ImageRef",
		reflect.TypeFor[map[string]xfercontract.ImageRef]():             "map[string]xfercontract.ImageRef",
		reflect.TypeFor[brokerimage.Bundle]():                           "brokerimage.Bundle",
		reflect.TypeFor[map[xfercontract.ImageRef]tunnel.ClientImage](): "map[xfercontract.ImageRef]tunnel.ClientImage",
	} {
		if got := imageTypeName(typ); got != want {
			t.Fatalf("contract host normalization: %s named %s, want %s", typ, got, want)
		}
	}
	if !slicesContainsImageRow(actual, "xfercontract.ImageRef.Digest\tjson:\"digest\"\tstring") {
		t.Fatalf("public reference rows lost their contract API name: %v", actual)
	}
}

// Use the frozen public schema even before P17 connects it to Bundle. This also
// prevents a plaintext/ciphertext buffer or key from being slipped into a new
// public field while the generic append-only ledger happily accepts an addition.
func flowSSPublicImageRows() []string {
	rows := append(collectImageFields(reflect.TypeOf(tunnel.ClientImage{})), collectImageFields(reflect.TypeOf(ssproxy.Image{}))...)
	sort.Strings(rows)
	return rows
}

var flowSSPublicImageSchema = []string{
	"tunnel.ClientImage.Version\tjson:\"version\"\tint",
	"tunnel.ClientImage.Endpoints\tjson:\"endpoints\"\t[]tunnel.EndpointImage",
	"tunnel.ClientImage.Flows\tjson:\"flows\"\tflow.Image",
	"tunnel.ClientImage.StateRef\tjson:\"state_ref\"\tstring",
	"tunnel.EndpointImage.ID\tjson:\"id\"\tflow.EndpointID",
	"tunnel.EndpointImage.OwnerGen\tjson:\"owner_gen\"\tuint64",
	"tunnel.EndpointImage.CarrierGen\tjson:\"carrier_gen\"\tuint64",
	"tunnel.EndpointImage.FlowIDs\tjson:\"flow_ids\"\t[]flow.ID",
	"tunnel.EndpointImage.StateRef\tjson:\"state_ref\"\tstring",
	"flow.Image.Version\tjson:\"version\"\tint",
	"flow.Image.Records\tjson:\"records\"\t[]flow.RecordImage",
	"flow.Image.Retirement\tjson:\"retirement\"\t[]flow.RetirementImage",
	"flow.Image.StateRef\tjson:\"state_ref\"\tstring",
	"flow.Image.ReservedBytes\tjson:\"reserved_bytes\"\tint64",
	"flow.RecordImage.ID\tjson:\"id\"\tflow.ID",
	"flow.RecordImage.OpenSeq\tjson:\"open_seq\"\tuint64",
	"flow.RecordImage.Intent\tjson:\"intent\"\t[32]uint8",
	"flow.RecordImage.Phase\tjson:\"phase\"\tflow.Phase",
	"flow.RecordImage.TxBase\tjson:\"tx_base\"\tuint64",
	"flow.RecordImage.TxEnd\tjson:\"tx_end\"\tuint64",
	"flow.RecordImage.RxApplied\tjson:\"rx_applied\"\tuint64",
	"flow.RecordImage.RxReceived\tjson:\"rx_received\"\tuint64",
	"flow.RecordImage.TxFIN\tjson:\"tx_fin\"\tuint64",
	"flow.RecordImage.RxFIN\tjson:\"rx_fin\"\tuint64",
	"flow.RecordImage.FINFlags\tjson:\"fin_flags\"\tuint8",
	"flow.RecordImage.RawRef\tjson:\"raw_ref\"\tstring",
	"flow.RecordImage.ReplayRef\tjson:\"replay_ref\"\tstring",
	"flow.RetirementImage.EndpointID\tjson:\"endpoint_id\"\tflow.EndpointID",
	"flow.RetirementImage.NextOpenSeq\tjson:\"next_open_seq\"\tuint64",
	"flow.RetirementImage.RetiredThrough\tjson:\"retired_through\"\tuint64",
	"flow.RetirementImage.Holes\tjson:\"holes\"\t[]uint64",
	"ssproxy.Image.Version\tjson:\"version\"\tint",
	"ssproxy.Image.StateRef\tjson:\"state_ref\"\tstring",
	"ssproxy.Image.ListenerRef\tjson:\"listener_ref\"\tstring",
	"ssproxy.Image.ConnectionCount\tjson:\"connection_count\"\tint",
	"ssproxy.Image.SecretBytes\tjson:\"secret_bytes\"\tint64",
}

// Separate deletions, JSON tags and Go types rather than a single corrupt fixture:
// these strand old images for different reasons and need independent receipts.
// origin: hot-update S5 plan §10.1 / T49.
func TestFlowSSImageInventoryControls(t *testing.T) {
	rows := flowSSPublicImageRows()
	if err := compareInventoryRows(flowSSPublicImageSchema, rows); err != nil {
		t.Fatalf("public metadata schema (secret fields forbidden): %v", err)
	}
	for i, row := range rows {
		t.Run(strings.Split(row, "\t")[0], func(t *testing.T) {
			if compareInventoryRows(rows, append(append([]string{}, rows[:i]...), rows[i+1:]...)) == nil {
				t.Fatal("deleted public image field accepted")
			}
			for _, column := range []int{1, 2} {
				broken := append([]string{}, rows...)
				parts := strings.Split(broken[i], "\t")
				parts[column] += "drift"
				broken[i] = strings.Join(parts, "\t")
				if compareInventoryRows(rows, broken) == nil {
					t.Fatal("public tag/type drift accepted")
				}
			}
		})
	}
	for _, field := range []string{"Keys", "Salt", "Nonce", "Target", "TxPending", "RxPending", "Ciphertext"} {
		broken := append(append([]string{}, rows...), "ssproxy.Image."+field+"\tjson:\"secret\"\t[]uint8")
		if compareInventoryRows(flowSSPublicImageSchema, broken) == nil {
			t.Fatal("secret public image field accepted")
		}
	}
}
