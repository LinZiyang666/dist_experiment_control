package proto

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/xfercontract"
	"github.com/LinZiyang666/tether/internal/xfercontract/xfervalue"
)

func transferWireFixture() TransferContinuation {
	id := Identity{"lab", "lab-1", "SHA256:fp", "agent1", "transfer1", "entry1", "intent1", "object1"}
	origin := Origin{"broker1", "instance1", "boot1", 7}
	start := time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)
	return TransferContinuation{
		Version: TransferVersion, Identity: id, Origin: origin,
		Permit:     &CreationPermit{id, origin, start, start.Add(30 * time.Second), "permit1", "signed1"},
		Budget:     BudgetSeal{start, 65537, 32768, int64(300 * time.Second), int64(280 * time.Second), true},
		Source:     &SourceSeal{65537, "sha1", 32768, 3, 32768, "stat1"},
		Settlement: &SettlementCapsule{id, origin, fakeActor, "issuer1", "nonce1", "signed2"},
	}
}

// origin: hot-update S7 plan §6.2 / §12.2 P01
func TestTransferWireGoldenMatchesFrozenContracts(t *testing.T) {
	c := transferWireFixture()
	// These literals are independent of the production encoder, including field
	// order, nanosecond units and explicit zero values on capable messages.
	id := `{"sid":"lab","nid":"lab-1","actor_fp":"SHA256:fp","agent_instance":"agent1","transfer_id":"transfer1","entry_nonce":"entry1","intent_digest":"intent1","object_version":"object1"}`
	origin := `{"broker_id":"broker1","instance_id":"instance1","boot_id":"boot1","generation":7}`
	intent := `{"verb":"push","tier":"b","path":"/tmp/source","sha256":"sha1","size":65537,"force":true,"rate_limit_bps":32768,"chunk_size":32768}`
	permit := `{"identity":` + id + `,"origin":` + origin + `,"issued_at":"2026-10-12T12:00:00Z","create_until":"2026-10-12T12:00:30Z","permit_nonce":"permit1","signature":"signed1"}`
	budget := `{"started_at":"2026-10-12T12:00:00Z","size":65537,"rate_limit_bps":32768,"effective_budget_ns":300000000000,"remaining_ns":280000000000,"source_sealed":true}`
	source := `{"size":65537,"sha256":"sha1","chunk_size":32768,"chunks":3,"rate_limit_bps":32768,"source_fingerprint":"stat1"}`
	grant := `{"identity":` + id + `,"grant_nonce":"grant1","receiver_instance":"receiver1","terminal_id":"terminal1"}`
	outcome := `{"grant":` + grant + `,"primitive":"link","outcome":"landed","bytes":65537,"sha256":"sha1","code":"","error":"","parent_synced":true,"tmp_cleanup_pending":true}`
	terminal := `{"terminal_id":"terminal1","kind":"failed","canonical_digest":"digest1","req_id":"req1","history_stream":"history1","history_seq":18446744073709551615,"bytes":65537,"sha256":"sha1","code":"settlement_abandoned","outcome_unknown":true}`
	capsule := `{"identity":` + id + `,"origin":` + origin + `,"receiver_nkey":"` + fakeActor + `","issuer":"issuer1","nonce":"nonce1","signature":"signed2"}`
	protection := `{"identity":` + id + `,"phase":"cleanup_pending","object_version":"object1","creation_until":"2026-10-12T12:00:30Z","writer_retired":true,"owner_generation":7}`
	continuation := `{"version":"transfer-v1","permit":` + permit + `,"identity":` + id + `,"origin":` + origin + `,"budget":` + budget + `,"source":` + source + `,"settlement":` + capsule + `}`
	i := Intent{"push", "b", "/tmp/source", "sha1", 65537, true, 32768, 32768}
	g := CommitGrant{c.Identity, "grant1", "receiver1", "terminal1"}
	o := ReceiverOutcome{g, PrimitiveLink, OutcomeLanded, 65537, "sha1", "", "", true, true}
	r := TerminalReceipt{"terminal1", "failed", "digest1", "req1", "history1", ^uint64(0), 65537, "sha1", ReasonSettlementAbandoned, true}
	p := Protection{c.Identity, PhaseCleanupPending, "object1", c.Permit.CreateUntil, true, 7}
	status := TransferStatusResp{StatusTerminal, PhaseCleanupPending, c.Identity, c.Origin, c.Budget, 65537, 3, ^uint64(0), &r, true, "", ""}
	cases := []struct {
		value  any
		golden string
	}{
		{SessionRmReq{}, `{}`},
		{ImageRef{Schema: "xfer-state-v1", ResourceID: "state1", Digest: "digest1", Count: 7, EncodedBytes: 1024}, `{"schema":"xfer-state-v1","resource_id":"state1","digest":"digest1","count":7,"encoded_bytes":1024}`},
		{c.Identity, id}, {c.Origin, origin}, {i, intent}, {c.Permit, permit}, {c.Budget, budget}, {c.Source, source},
		{g, grant}, {o, outcome}, {r, terminal}, {c.Settlement, capsule}, {p, protection}, {c, continuation},
		{TransferPermitReq{"call1", i, "transfer1", "agent1"}, `{"call_id":"call1","intent":` + intent + `,"transfer_id":"transfer1","agent_instance":"agent1"}`},
		{TransferPermitResp{c.Permit, "", ""}, `{"permit":` + permit + `,"code":"","error":""}`},
		{TransferProbeReq{"call1", "lab", "lab-1"}, `{"call_id":"call1","sid":"lab","nid":"lab-1"}`},
		{TransferProbeResp{TransferVersion, TransferVersion, TransferVersion, "instance1", 2097152, 1049088, "", ""}, `{"version":"transfer-v1","history_guard_version":"transfer-v1","bucket_config_version":"transfer-v1","instance_id":"instance1","max_payload":2097152,"max_msg_size":1049088,"code":"","error":""}`},
		{TransferStatusReq{"call1", c.Identity, c.Origin}, `{"call_id":"call1","identity":` + id + `,"origin":` + origin + `}`},
		{status, `{"state":"terminal","phase":"cleanup_pending","identity":` + id + `,"origin":` + origin + `,"budget":` + budget + `,"durable_offset":65537,"chunk_index":3,"last_consumed_stream_seq":18446744073709551615,"terminal":` + terminal + `,"cleanup_pending":true,"code":"","error":""}`},
		{TransferSourceReq{"call1", c.Identity, c.Origin, *c.Source}, `{"call_id":"call1","identity":` + id + `,"origin":` + origin + `,"source":` + source + `}`},
		{TransferReserveReq{"call1", c.Identity, c.Origin, "receiver1"}, `{"call_id":"call1","identity":` + id + `,"origin":` + origin + `,"receiver_instance":"receiver1"}`},
		{TransferReserveResp{Grant: &g}, `{"grant":` + grant + `,"code":"","error":""}`},
		// origin: hot-update S7 plan §4 / INTEG-D pull source seal
		{TransferReserveResp{Grant: &g, Source: c.Source}, `{"grant":` + grant + `,"code":"","error":"","source":` + source + `}`},
		{TransferTerminalReq{"call1", c.Identity, c.Origin, o}, `{"call_id":"call1","identity":` + id + `,"origin":` + origin + `,"outcome":` + outcome + `}`},
		{TransferTerminalResp{Receipt: &r}, `{"receipt":` + terminal + `,"code":"","error":""}`},
		{TransferCancelReq{"call1", c.Identity, c.Origin, CancelRevoked}, `{"call_id":"call1","identity":` + id + `,"origin":` + origin + `,"reason":"revoked"}`},
		{TransferCensusReq{"call1", "lab", c.Origin}, `{"call_id":"call1","sid":"lab","origin":` + origin + `}`},
		{TransferCensusResp{c.Origin, true, []Protection{p}, "census1", ""}, `{"origin":` + origin + `,"ready":true,"entries":[` + protection + `],"digest":"census1","code":""}`},
	}
	for _, tc := range cases {
		name := reflect.TypeOf(tc.value).String()
		t.Run(name, func(t *testing.T) {
			encoded, err := json.Marshal(tc.value)
			if err != nil || string(encoded) != tc.golden {
				t.Fatalf("wire drift: got=%s want=%s err=%v", encoded, tc.golden, err)
			}
			typ := reflect.TypeOf(tc.value)
			if typ.Kind() == reflect.Pointer {
				typ = typ.Elem()
			}
			decoded := reflect.New(typ)
			if err := DecodeTransferWire(encoded, decoded.Interface()); err != nil {
				t.Fatal(err)
			}
			again, err := json.Marshal(decoded.Interface())
			if err != nil || !bytes.Equal(encoded, again) {
				t.Fatalf("roundtrip drift: %s %v", again, err)
			}
		})
	}
	// Shared DTOs map every field to the frozen leaf types. Status/Probe/Protection
	// deliberately have wire tags independently of their private contract shapes.
	for _, pair := range [][2]reflect.Type{
		{reflect.TypeFor[Identity](), reflect.TypeFor[xfervalue.Identity]()},
		{reflect.TypeFor[Origin](), reflect.TypeFor[xfervalue.Origin]()},
		{reflect.TypeFor[Intent](), reflect.TypeFor[xfervalue.Intent]()},
		{reflect.TypeFor[CreationPermit](), reflect.TypeFor[xfervalue.CreationPermit]()},
		{reflect.TypeFor[BudgetSeal](), reflect.TypeFor[xfervalue.BudgetSeal]()},
		{reflect.TypeFor[SourceSeal](), reflect.TypeFor[xfervalue.SourceSeal]()},
		{reflect.TypeFor[CommitGrant](), reflect.TypeFor[xfervalue.CommitGrant]()},
		{reflect.TypeFor[ReceiverOutcome](), reflect.TypeFor[xfervalue.ReceiverOutcome]()},
		{reflect.TypeFor[TerminalReceipt](), reflect.TypeFor[xfervalue.TerminalReceipt]()},
		{reflect.TypeFor[SettlementCapsule](), reflect.TypeFor[xfervalue.SettlementCapsule]()},
		{reflect.TypeFor[TransferStatusResp](), reflect.TypeFor[xfervalue.Status]()},
		{reflect.TypeFor[TransferStatusReq](), reflect.TypeFor[xfervalue.StatusRequest]()},
		{reflect.TypeFor[TransferProbeResp](), reflect.TypeFor[xfervalue.ProbeResult]()},
		{reflect.TypeFor[Protection](), reflect.TypeFor[xfercontract.Protection]()},
		{reflect.TypeFor[ImageRef](), reflect.TypeFor[xfervalue.ImageRef]()},
	} {
		checkTransferContractFields(t, pair[0], pair[1])
	}
	if got := []StatusState{StatusUnknown, StatusLive, StatusTerminal}; !reflect.DeepEqual(got, []StatusState{"unknown", "live", "terminal"}) {
		t.Fatal("status vocabulary drift")
	}
	if got := []Phase{PhaseOpen, PhasePrepared, PhaseSending, PhaseReceiving, PhaseCommitReserved, PhaseDecided, PhaseTerminalPending, PhaseHistoryConfirmed, PhaseCleanupPending, PhaseSettled}; !reflect.DeepEqual(got, []Phase{"open", "prepared", "sending", "receiving", "commit_reserved", "decided", "terminal_pending", "history_confirmed", "cleanup_pending", "settled"}) {
		t.Fatal("phase vocabulary drift")
	}
	if got := []string{string(PrimitiveRename), string(PrimitiveLink), string(OutcomeNotEntered), string(OutcomeInflight), string(OutcomeLanded), string(CancelUser), string(CancelDeadline), string(CancelRevoked), string(CancelStopped)}; !reflect.DeepEqual(got, []string{"rename", "link", "not_entered", "inflight", "landed", "user", "deadline", "revoked", "stopped"}) {
		t.Fatal("outcome/cancel vocabulary drift")
	}
	if got := []string{CodeTransferIntentConflict, CodeTransferPermitExpired, CodeTransferOwnerRecovering, CodeTransferResultUnknown, CodeTransferObjectConflict, CodeTransferObjectVersionMismatch, CodeTransferSourceChanged, CodeTransferStorageHorizon, CodeTransferPayloadUnsupported, CodeTransferCapacityExceeded, CodeTransferTerminalConflict, CodeTransferCommitInflight, CodeTransferDurabilityUnknown, CodeTransferReconcileRequired, CodeTransferCanceled, CodeTransferUnknown, CodeTransferBudgetExceeded, ReasonSettlementAbandoned}; !reflect.DeepEqual(got, []string{"transfer_intent_conflict", "transfer_permit_expired", "transfer_owner_recovering", "transfer_result_unknown", "transfer_object_conflict", "transfer_object_version_mismatch", "transfer_source_changed", "transfer_storage_horizon", "transfer_payload_unsupported", "transfer_capacity_exceeded", "transfer_terminal_conflict", "transfer_commit_inflight", "transfer_durability_unknown", "transfer_reconcile_required", "transfer_canceled", "transfer_unknown", "transfer_budget_exceeded", "settlement_abandoned"}) {
		t.Fatal("transfer code vocabulary drift")
	}
}

func checkTransferContractFields(t *testing.T, wire, contract reflect.Type) {
	t.Helper()
	if wire.Kind() != contract.Kind() {
		t.Fatalf("mapping kind drift %s -> %s", wire, contract)
	}
	if wire.Kind() == reflect.Pointer || wire.Kind() == reflect.Slice {
		checkTransferContractFields(t, wire.Elem(), contract.Elem())
		return
	}
	if wire == reflect.TypeFor[time.Time]() {
		if wire != contract {
			t.Fatal("time mapping drift")
		}
		return
	}
	if wire.Kind() != reflect.Struct {
		if wire != contract {
			t.Fatalf("mapping scalar drift %s -> %s", wire, contract)
		}
		return
	}
	if wire.NumField() != contract.NumField() {
		t.Fatalf("mapping field count drift %s -> %s", wire, contract)
	}
	for i := 0; i < wire.NumField(); i++ {
		wf, cf := wire.Field(i), contract.Field(i)
		if wf.Name != cf.Name {
			t.Fatalf("mapping field drift %s.%s -> %s.%s", wire, wf.Name, contract, cf.Name)
		}
		if cf.Tag.Get("json") != "" && wf.Tag.Get("json") != cf.Tag.Get("json") {
			t.Fatalf("mapping tag drift %s.%s", wire, wf.Name)
		}
		checkTransferContractFields(t, wf.Type, cf.Type)
	}
}

// origin: hot-update S7 plan §6.2 / §12.2 P01
// origin: S7 internal review GATE-CONVERGE-contracts F4
func TestTransferZeroExtensionKeepsLegacyWireAndSignedBytes(t *testing.T) {
	cases := []struct {
		value  any
		golden string
	}{
		{NodeRegisterReq{ProtoVersion: 2, ReleaseVersion: "v0.0.0-dev", NID: "lab-1", OS: "linux", Arch: "amd64"}, `{"proto_version":2,"release_version":"v0.0.0-dev","nid":"lab-1","os":"linux","arch":"amd64"}`},
		{NodeRegisterResp{OK: true}, `{"ok":true}`},
		{PushPrepareReq{TransferID: "transfer1", Path: "/x", Size: 2, SHA256: "sha1", Tier: "a", InlineData: []byte{0, 255}}, `{"transfer_id":"transfer1","path":"/x","size":2,"sha256":"sha1","tier":"a","inline_data":"AP8="}`},
		{PushPrepareResp{OK: true, Tier: "b"}, `{"ok":true,"tier":"b"}`},
		{PullPrepareReq{TransferID: "transfer1", Path: "/x", MaxInline: 8 << 20}, `{"transfer_id":"transfer1","path":"/x","max_inline":8388608}`},
		{PullPrepareResp{OK: true, Tier: "a", Size: 2, SHA256: "sha1", InlineData: []byte{0, 255}}, `{"ok":true,"tier":"a","size":2,"sha256":"sha1","inline_data":"AP8="}`},
		{TransferCommitReq{TransferID: "transfer1", Bucket: "xfer-lab", ObjectKey: "obj1"}, `{"transfer_id":"transfer1","bucket":"xfer-lab","object_key":"obj1"}`},
		{TransferCommitResp{OK: true}, `{"ok":true}`},
		{TransferFinalize{Kind: "complete", TransferID: "transfer1", Tier: "b", Bytes: 2}, `{"kind":"complete","transfer_id":"transfer1","tier":"b","bytes":2}`},
		{TransferFinalizeResp{OK: true}, `{"ok":true}`},
	}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	for _, tc := range cases {
		got, err := json.Marshal(tc.value)
		if err != nil || string(got) != tc.golden {
			t.Fatalf("legacy %T changed: %s %v", tc.value, got, err)
		}
		sig := ed25519.Sign(key, []byte(tc.golden))
		for _, extension := range []*TransferContinuation{nil, {}} {
			canonical, err := CanonicalTransferExtensionBytes(got, extension)
			if err != nil || !bytes.Equal(canonical, []byte(tc.golden)) || !ed25519.Verify(key.Public().(ed25519.PublicKey), canonical, sig) {
				t.Fatalf("legacy signature bytes changed: %q %v", canonical, err)
			}
		}
	}
	if ProtoVersion != 2 || ImageABIVersion != 1 {
		t.Fatal("transfer extension changed a global epoch")
	}
	// origin: S8 BROKER-EXPORTER R-11 (hot-update S8 plan §6.1 / §11.2 P17) —
	// the S8 adjunct domain and features join; the transfer domain is unchanged.
	if got := ImageVersionLines(); !reflect.DeepEqual(got, []string{"image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1 upgrade-cut-v1",
		"image-read-epochs 1", "image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1", "image-abi v1"}) {
		t.Fatalf("reader advertisement drift: %v", got)
	}
}

// origin: hot-update S7 plan §6.2 / §12.2 P01
func TestTransferCanonicalSignaturesCoverEveryExtensionField(t *testing.T) {
	c := transferWireFixture()
	c.Intent = &Intent{Verb: "push", Tier: "b", Path: "/tmp/source", Size: 65537, ChunkSize: 32768}
	// The broker-only commit Grant is an extension field too; it must be signed.
	// origin: hot-update S7 plan §4 / §6.3 / INTEG-D outer commit adapter
	c.Grant = &CommitGrant{c.Identity, "grant1", "receiver1", "terminal1"}
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{9}, ed25519.SeedSize))
	canonical, err := CanonicalTransferExtensionBytes([]byte("legacy bytes"), &c)
	if err != nil {
		t.Fatal(err)
	}
	encoded, _ := json.Marshal(c)
	if !bytes.Equal(canonical, append([]byte("legacy bytes\ntransfer-v1\n"), encoded...)) {
		t.Fatalf("extension canonical framing drift: %q", canonical)
	}
	sig := ed25519.Sign(key, canonical)
	paths := transferLeafPaths(reflect.ValueOf(c), nil)
	if len(paths) < 58 {
		t.Fatalf("vacuous signature coverage: %d fields", len(paths))
	}
	for _, path := range paths {
		copy := cloneTransferContinuation(t, c)
		mutateTransferLeaf(reflect.ValueOf(&copy).Elem(), path)
		b, err := CanonicalTransferExtensionBytes([]byte("legacy bytes"), &copy)
		if err == nil && ed25519.Verify(key.Public().(ed25519.PublicKey), b, sig) {
			t.Errorf("unsigned extension field: %v", path)
		}
	}
	// Permit/capsule inputs have independent domains and exclude only their own signature.
	for _, tc := range []struct {
		value  any
		encode func(any) ([]byte, error)
		domain string
	}{
		{*c.Permit, func(v any) ([]byte, error) { p := v.(CreationPermit); return CanonicalCreationPermitBytes(&p) }, "tether-transfer-permit-v1\n"},
		{*c.Settlement, func(v any) ([]byte, error) { p := v.(SettlementCapsule); return CanonicalSettlementCapsuleBytes(&p) }, "tether-transfer-settlement-v1\n"},
	} {
		b, err := tc.encode(tc.value)
		if err != nil || !bytes.HasPrefix(b, []byte(tc.domain)) {
			t.Fatalf("signature domain: %q %v", b, err)
		}
		signed := ed25519.Sign(key, b)
		for _, path := range transferLeafPaths(reflect.ValueOf(tc.value), nil) {
			copy := reflect.New(reflect.TypeOf(tc.value)).Elem()
			copy.Set(reflect.ValueOf(tc.value))
			mutateTransferLeaf(copy, path)
			changed, err := tc.encode(copy.Interface())
			isSignature := len(path) == 1 && copy.Type().Field(path[0]).Name == "Signature"
			if isSignature {
				if err != nil || !bytes.Equal(b, changed) {
					t.Fatal("signature input included its own signature")
				}
			} else if err == nil && ed25519.Verify(key.Public().(ed25519.PublicKey), changed, signed) {
				t.Errorf("unsigned authority field %T %v", tc.value, path)
			}
		}
	}
	if _, err := CanonicalTransferExtensionBytes(nil, &TransferContinuation{Budget: c.Budget}); err == nil {
		t.Fatal("nonzero extension downgraded to legacy")
	}
	if _, err := CanonicalCreationPermitBytes(nil); err == nil {
		t.Fatal("nil permit signable")
	}
	bad := *c.Permit
	bad.CreateUntil = bad.IssuedAt.Add(5*time.Minute + 1)
	if _, err := CanonicalCreationPermitBytes(&bad); err == nil {
		t.Fatal("creation authority exceeded five minutes")
	}
	bad.CreateUntil = bad.IssuedAt
	if _, err := CanonicalCreationPermitBytes(&bad); err == nil {
		t.Fatal("empty creation window signable")
	}
	if _, err := CanonicalSettlementCapsuleBytes(nil); err == nil {
		t.Fatal("nil capsule signable")
	}
	bad = *c.Permit
	bad.Identity = Identity{}
	if _, err := CanonicalCreationPermitBytes(&bad); err == nil {
		t.Fatal("empty permit identity signable")
	}
	bad = *c.Permit
	bad.Origin = Origin{}
	if _, err := CanonicalCreationPermitBytes(&bad); err == nil {
		t.Fatal("empty permit origin signable")
	}
	badCapsule := *c.Settlement
	badCapsule.Identity = Identity{}
	if _, err := CanonicalSettlementCapsuleBytes(&badCapsule); err == nil {
		t.Fatal("empty capsule identity signable")
	}
	badCapsule = *c.Settlement
	badCapsule.Origin = Origin{}
	if _, err := CanonicalSettlementCapsuleBytes(&badCapsule); err == nil {
		t.Fatal("empty capsule origin signable")
	}
	badCapsule = *c.Settlement
	badCapsule.ReceiverNkey = "invalid"
	if _, err := CanonicalSettlementCapsuleBytes(&badCapsule); err == nil {
		t.Fatal("invalid receiver NKey signable")
	}
}

func cloneTransferContinuation(t *testing.T, c TransferContinuation) TransferContinuation {
	t.Helper()
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	var copy TransferContinuation
	if err = json.Unmarshal(b, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}
func transferLeafPaths(v reflect.Value, prefix []int) [][]int {
	if v.Kind() == reflect.Pointer {
		return transferLeafPaths(v.Elem(), prefix)
	}
	if v.Kind() != reflect.Struct || v.Type() == reflect.TypeFor[time.Time]() {
		return [][]int{prefix}
	}
	var paths [][]int
	for i := 0; i < v.NumField(); i++ {
		next := append(append([]int(nil), prefix...), i)
		paths = append(paths, transferLeafPaths(v.Field(i), next)...)
	}
	return paths
}
func mutateTransferLeaf(v reflect.Value, path []int) {
	for _, i := range path {
		if v.Kind() == reflect.Pointer {
			v = v.Elem()
		}
		v = v.Field(i)
	}
	if v.Type() == reflect.TypeFor[time.Time]() {
		v.Set(reflect.ValueOf(v.Interface().(time.Time).Add(time.Second)))
		return
	}
	switch v.Kind() {
	case reflect.String:
		v.SetString(v.String() + "x")
	case reflect.Bool:
		v.SetBool(!v.Bool())
	case reflect.Int64:
		v.SetInt(v.Int() + 1)
	case reflect.Uint32, reflect.Uint64:
		v.SetUint(v.Uint() + 1)
	default:
		panic("unhandled transfer signature field")
	}
}

// origin: hot-update S7 plan §6.2 / P01
func TestTransferWireRejectsAmbiguousAuthorityBeforeDecoding(t *testing.T) {
	c := transferWireFixture()
	good, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	cases := [][]byte{
		[]byte(strings.Replace(string(good), `"sid":"lab"`, `"sid":"lab","sid":"lab"`, 1)),
		[]byte(strings.Replace(string(good), `"sid":"lab"`, `"sid":"lab","SID":"other"`, 1)),
		[]byte(strings.Replace(string(good), `"sid":"lab"`, `"SID":"lab"`, 1)),
		[]byte(strings.Replace(string(good), `"sid":"lab"`, `"sid":"lab","unknown":"opaque-private-data"`, 1)),
		[]byte(strings.Replace(string(good), `"sid":"lab"`, `"sid":null`, 1)),
		[]byte(strings.Replace(string(good), `"version":"transfer-v1"`, `"version":"transfer-v9"`, 1)),
		[]byte(strings.Replace(string(good), `"sid":"lab"`, `"sid":"lab\nforged"`, 1)),
		[]byte(strings.Replace(string(good), `"transfer_id":"transfer1"`, `"transfer_id":"*"`, 1)),
		[]byte(strings.Replace(string(good), `"object_version":"object1"`, `"object_version":"a.b"`, 1)),
		[]byte(strings.Replace(string(good), `"entry_nonce":"entry1"`, `"entry_nonce":"entry>"`, 1)),
		[]byte(strings.Replace(string(good), `"actor_fp":"SHA256:fp"`, `"actor_fp":"fp\rforged"`, 1)),
		append(bytes.Clone(good), []byte(`{}`)...), []byte(`{}`), []byte(`null`), []byte(`[]`),
		[]byte(strings.Replace(string(good), `"sid":"lab"`, `"sid":"\ufffd"`, 1)),
	}
	invalidUTF8 := bytes.Clone(good)
	invalidUTF8[10] = 255
	cases = append(cases, invalidUTF8)
	for i, b := range cases {
		out := c
		if err := DecodeTransferWire(b, &out); err == nil {
			t.Errorf("ambiguous payload %d accepted", i)
		} else if strings.Contains(err.Error(), "opaque-private-data") {
			t.Error("decoder error disclosed private field")
		}
		if !reflect.DeepEqual(c, out) {
			t.Errorf("refused payload %d changed destination authority", i)
		}
	}
	for _, target := range []any{nil, (*TransferContinuation)(nil), c, new(string)} {
		if DecodeTransferWire(good, target) == nil {
			t.Errorf("invalid decode target %T accepted", target)
		}
	}
	for _, value := range []any{
		TransferStatusResp{State: "complete"}, TransferStatusResp{}, TransferStatusResp{State: StatusTerminal},
		TransferStatusResp{State: StatusLive, Phase: "finalized"},
		TransferStatusResp{State: StatusLive}, ReceiverOutcome{},
		ReceiverOutcome{Primitive: "copy", Outcome: OutcomeLanded}, ReceiverOutcome{Primitive: PrimitiveLink, Outcome: "failed"},
		TransferCancelReq{CallID: "call1", Identity: c.Identity, Origin: c.Origin, Reason: "stop"}, TransferProbeReq{CallID: "a.b", SID: "lab", NID: "lab-1"},
		TerminalReceipt{Kind: "complete", OutcomeUnknown: true}, TerminalReceipt{Kind: "unknown"},
		TransferProbeReq{CallID: "call1", SID: "bad*", NID: "lab-1"}, TransferProbeReq{CallID: "call1", SID: "lab", NID: "bad*"},
		TransferPermitReq{CallID: "call1", AgentInstance: "agent1"}, TransferPermitReq{CallID: "call1", TransferID: "transfer1"},
		TransferCensusReq{CallID: "call1", SID: "lab"}, TransferStatusReq{CallID: "call1", Identity: c.Identity},
		TransferStatusReq{CallID: "call1", Origin: c.Origin},
	} {
		if ValidateTransferWire(value) == nil {
			t.Errorf("invalid authority %T accepted: %+v", value, value)
		}
	}
	for _, mutate := range []func(*Identity){
		func(id *Identity) { id.SID = "bad*" }, func(id *Identity) { id.NID = "bad*" }, func(id *Identity) { id.ActorFP = "fp\nforged" },
		func(id *Identity) { id.AgentInstance = "*" }, func(id *Identity) { id.TransferID = "*" }, func(id *Identity) { id.EntryNonce = "a.b" },
		func(id *Identity) { id.IntentDigest = "*" }, func(id *Identity) { id.ObjectVersion = "a.b" },
	} {
		invalid := c.Identity
		mutate(&invalid)
		if ValidateTransferWire(invalid) == nil {
			t.Error("invalid standalone identity accepted")
		}
	}
	if ValidateTransferWire(Origin{BrokerID: "*", InstanceID: "instance1", BootID: "boot1"}) == nil {
		t.Fatal("invalid original owner token accepted")
	}
	for _, field := range []string{"Permit", "Settlement"} {
		invalid := cloneTransferContinuation(t, c)
		if field == "Permit" {
			invalid.Permit.Identity.TransferID = "other"
		} else {
			invalid.Settlement.Origin.BrokerID = "other"
		}
		if ValidateTransferWire(invalid) == nil {
			t.Errorf("mismatched %s authority accepted", field)
		}
	}
	if ValidateTransferWire(Protection{Identity: c.Identity, Phase: PhaseCleanupPending, ObjectVersion: "other"}) == nil {
		t.Fatal("protection changed original object version")
	}
	if err := ValidateTransferWire(TransferStatusResp{Code: CodeTransferOwnerRecovering}); err != nil {
		t.Fatal("explicit unavailable response refused", err)
	}
}

// origin: hot-update S7 plan §6.2 / P01
func TestTransferPrepareRequiresSameTopLevelObjectVersion(t *testing.T) {
	c := transferWireFixture()
	for _, value := range []any{
		PushPrepareReq{CallID: "call1", TransferID: "transfer1", Tier: "a", InlineData: []byte{0, 255}, TransferVersion: TransferVersion, Continuation: &c},
		PullPrepareReq{CallID: "call1", TransferID: "transfer1", TransferVersion: TransferVersion, Continuation: &c},
		TransferCommitReq{CallID: "call1", TransferID: "transfer1", Bucket: "xfer-lab", ObjectKey: "obj1", Continuation: &c},
		TransferFinalize{CallID: "call1", TransferID: "transfer1", Kind: "failed", Continuation: &c},
		TransferCommitResp{OK: true, Continuation: &c}, TransferFinalizeResp{OK: true, Continuation: &c},
	} {
		b, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		decoded := reflect.New(reflect.TypeOf(value))
		if err := DecodeTransferWire(b, decoded.Interface()); err != nil {
			t.Fatalf("capable %T roundtrip: %v", value, err)
		}
		if !reflect.DeepEqual(value, decoded.Elem().Interface()) {
			t.Fatalf("capable %T lost authority/payload", value)
		}
	}
	for _, invalid := range []any{
		PushPrepareReq{TransferID: "transfer1", Continuation: &c}, PullPrepareReq{TransferID: "transfer1", Continuation: &c},
		TransferCommitReq{TransferID: "transfer1", Continuation: &c}, TransferFinalize{TransferID: "transfer1", Continuation: &c},
		TransferCommitReq{CallID: "call1", TransferID: "other", Continuation: &c},
		PushPrepareResp{TransferVersion: "unknown"},
	} {
		if ValidateTransferWire(invalid) == nil {
			t.Errorf("unbound capable carrier %T accepted", invalid)
		}
	}
	for _, value := range []any{
		PushPrepareResp{OK: true, ObjectVersion: "object1", TransferVersion: TransferVersion, Continuation: &c},
		PullPrepareResp{OK: true, ObjectVersion: "object1", TransferVersion: TransferVersion, Continuation: &c, InlineData: []byte{0, 255}},
	} {
		if err := ValidateTransferWire(value); err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(value)
		var fields map[string]json.RawMessage
		_ = json.Unmarshal(b, &fields)
		if string(fields["object_version"]) != `"object1"` || string(fields["transfer_version"]) != `"transfer-v1"` || len(fields["continuation"]) == 0 {
			t.Fatalf("missing top-level negotiation/version: %s", b)
		}
	}
	for _, top := range []string{"", "object2"} {
		if ValidateTransferPrepareVersion(top, &c) == nil {
			t.Errorf("mismatch %q accepted", top)
		}
		if ValidateTransferWire(PushPrepareResp{OK: true, ObjectVersion: top, Continuation: &c}) == nil {
			t.Errorf("push mismatch %q accepted", top)
		}
		if ValidateTransferWire(PullPrepareResp{OK: true, ObjectVersion: top, Continuation: &c}) == nil {
			t.Errorf("pull mismatch %q accepted", top)
		}
	}
	if ValidateTransferPrepareVersion("object1", nil) == nil {
		t.Fatal("unbound top version accepted")
	}
	if ValidateTransferPrepareVersion("", nil) != nil {
		t.Fatal("legacy response refused")
	}
	// The status DTO excludes settlement credentials even while reporting pending cleanup.
	b, _ := json.Marshal(TransferStatusResp{State: StatusLive, Phase: PhaseCleanupPending, Identity: c.Identity})
	if bytes.Contains(b, []byte("settlement")) || bytes.Contains(b, []byte("receiver_nkey")) {
		t.Fatal("status exposed settlement capsule")
	}
	abandon, _ := json.Marshal(SessionRmReq{AbandonUnsettledTransfers: true})
	if string(abandon) != `{"abandon_unsettled_transfers":true}` {
		t.Fatalf("abandonment carrier drift: %s", abandon)
	}
}

// origin: hot-update S7 plan §6.3 / P01
func TestTransferSubjectsShareClosedOperationAndIdentityTable(t *testing.T) {
	wantOps := []TransferOperation{TransferOpProbe, TransferOpPermit, TransferOpStatus, TransferOpSource, TransferOpReserve, TransferOpTerminal, TransferOpCancel, TransferOpCensus}
	if len(transferSubjectOperations) != len(wantOps) {
		t.Fatal("subject operation set drift")
	}
	for i, op := range wantOps {
		if transferSubjectOperations[i].op != op {
			t.Fatal("subject operation ordering drift")
		}
		subj, err := SubjCtrlTransferRequest(fakeActor, "lab", "lab-1", "transfer1", op)
		want := "tether.v2.ctrl.by." + fakeActor + ".s.lab.node.lab-1.transfer.transfer1." + string(op) + ".req"
		if err != nil || subj != want {
			t.Fatalf("control subject %s: %s %v", op, subj, err)
		}
		a, s, n, id, parsed, ok := ParseCtrlTransferRequest(subj)
		if !ok || a != fakeActor || s != "lab" || n != "lab-1" || id != "transfer1" || parsed != op {
			t.Fatal("control parser lost subject identity")
		}
		wild, err := SubjCtrlTransferRequestWildcard(op)
		if err != nil || wild != "tether.v2.ctrl.by.*.s.*.node.*.transfer.*."+string(op)+".req" {
			t.Fatal("control subscription drift")
		}
		agentAllowed := op == TransferOpSource || op == TransferOpReserve || op == TransferOpTerminal || op == TransferOpCancel
		ev, err := SubjEvTransferRequest("lab", "lab-1", "transfer1", op)
		wild, ew := SubjEvTransferRequestWildcard(op)
		if !agentAllowed {
			if err == nil || ew == nil {
				t.Errorf("agent gained %s operation", op)
			}
			continue
		}
		if err != nil || ew != nil || ev != "tether.v2.s.lab.ev.node.lab-1.transfer.transfer1."+string(op)+".req" || wild != "tether.v2.s.*.ev.node.*.transfer.*."+string(op)+".req" {
			t.Fatal("agent subject drift")
		}
		s, n, id, parsed, ok = ParseEvTransferRequest(ev)
		if !ok || s != "lab" || n != "lab-1" || id != "transfer1" || parsed != op {
			t.Fatal("agent parser lost subject identity")
		}
	}
	ctrl, _ := SubjCtrlTransferRequest(fakeActor, "lab", "lab-1", "transfer1", TransferOpStatus)
	ev, _ := SubjEvTransferRequest("lab", "lab-1", "transfer1", TransferOpTerminal)
	for _, bad := range []string{"", strings.Replace(ctrl, "tether.v2", "tether.v1", 1), strings.Replace(ctrl, ".status.req", ".prepare.req", 1), strings.Replace(ctrl, "transfer1", "*", 1), strings.Replace(ctrl, "lab-1", "bad node", 1), ctrl + ".suffix", strings.Replace(ctrl, fakeActor, "invalid", 1), strings.Replace(ctrl, ".lab.", ".all.", 1)} {
		if _, _, _, _, _, ok := ParseCtrlTransferRequest(bad); ok {
			t.Errorf("malformed control subject accepted: %q", bad)
		}
	}
	for _, bad := range []string{"", strings.Replace(ev, "tether.v2", "tether.v1", 1), strings.Replace(ev, "terminal", "status", 1), strings.Replace(ev, "transfer1", "*", 1), ev + ".suffix", strings.Replace(ev, "lab-1", "bad\nnode", 1), strings.Replace(ev, ".lab.", ".all.", 1)} {
		if _, _, _, _, ok := ParseEvTransferRequest(bad); ok {
			t.Errorf("malformed agent subject accepted: %q", bad)
		}
	}
	for _, op := range []TransferOperation{"", "bogus", "status.req", "status\n", "prepare", "bucket-config"} {
		if _, err := SubjCtrlTransferRequest(fakeActor, "lab", "lab-1", "transfer1", op); err == nil {
			t.Errorf("unknown operation %s accepted", op)
		}
		if _, err := SubjCtrlTransferRequestWildcard(op); err == nil {
			t.Errorf("unknown operation %s subscribed", op)
		}
	}
	c := transferWireFixture()
	if ValidateTransferSubjectIdentity("SHA256:fp", "lab", "lab-1", "transfer1", c.Identity) != nil {
		t.Fatal("valid authenticated binding refused")
	}
	for _, tokens := range [][4]string{{"fp2", "lab", "lab-1", "transfer1"}, {"SHA256:fp", "other", "lab-1", "transfer1"}, {"SHA256:fp", "lab", "other", "transfer1"}, {"SHA256:fp", "lab", "lab-1", "other"}, {"", "lab", "lab-1", "transfer1"}} {
		if ValidateTransferSubjectIdentity(tokens[0], tokens[1], tokens[2], tokens[3], c.Identity) == nil {
			t.Errorf("subject/auth mismatch %v accepted", tokens)
		}
	}
	if got := []string{TransferHeaderVersion, TransferHeaderIndex, TransferHeaderOffset, TransferHeaderLength, TransferHeaderSHA256, TransferHeaderIntent, TransferHeaderMsgID, TransferHeaderSubjectSequence, TransferHeaderStreamSequence, TransferChunkWireVersion}; !reflect.DeepEqual(got, []string{"Tether-Xfer-Version", "Tether-Xfer-Index", "Tether-Xfer-Offset", "Tether-Xfer-Length", "Tether-Xfer-SHA256", "Tether-Xfer-Intent", "Nats-Msg-Id", "Nats-Expected-Last-Subject-Sequence", "Nats-Expected-Last-Sequence", "1"}) {
		t.Fatal("chunk header contract drift")
	}
}

// origin: hot-update S7 plan §6.1 / §6.2 / P01
func TestTransferIntentDigestBindsRateAndOriginalSourceFacts(t *testing.T) {
	intent := Intent{Verb: "pull", Tier: "b", Path: "/tmp/source", RateLimitBPS: 32768, ChunkSize: 32768}
	digest, err := TransferIntentDigest(intent)
	if err != nil {
		t.Fatal(err)
	}
	if digest != "8733c4d831ab07709f4b075c261f8cdc4f9786b09afda9d410ecd869043c4262" {
		t.Fatalf("intent canonical golden drift: %s", digest)
	}
	for _, path := range transferLeafPaths(reflect.ValueOf(intent), nil) {
		copy := intent
		mutateTransferLeaf(reflect.ValueOf(&copy).Elem(), path)
		changed, err := TransferIntentDigest(copy)
		if err == nil && changed == digest {
			t.Errorf("intent digest missed field %v", path)
		}
	}
	for _, invalid := range []Intent{{Verb: "other", Tier: "b", Path: "/x"}, {Verb: "push", Tier: "other", Path: "/x"}, {Verb: "push", Tier: "b", Path: "/x\n"}, {Verb: "push", Tier: "b", Path: "/x", RateLimitBPS: -1}, {Verb: "push", Tier: "b", Path: "/x", ChunkSize: 1}} {
		if _, err := TransferIntentDigest(invalid); err == nil {
			t.Errorf("invalid intent signed: %+v", invalid)
		}
	}
}
