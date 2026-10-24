package proto

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokermaint"
)

// This is the previous upgrade reader, including its canonical byte order.
// Keep it independent of today's canonical function: decoding new JSON into
// the old shape must not accidentally endorse an unsigned S6 extension.
type legacyBrokerUpgradeReq struct {
	Op           string `json:"op"`
	TargetNode   string `json:"target_node"`
	TransferTo   string `json:"transfer_to,omitempty"`
	ToVersion    string `json:"to_version,omitempty"`
	ExpectSHA256 string `json:"expect_sha256,omitempty"`
	SID          string `json:"sid,omitempty"`
	IssuedAt     string `json:"issued_at"`
	Sig          string `json:"sig"`
}

func legacyBrokerUpgradeBytes(r legacyBrokerUpgradeReq) []byte {
	return []byte(r.Op + "\n" + r.TargetNode + "\n" + r.TransferTo + "\n" +
		r.ToVersion + "\n" + r.ExpectSHA256 + "\n" + r.SID + "\n" + r.IssuedAt)
}

// origin: hot-update S6 plan §9.1 / §9.2 / T46
// origin: S7 internal review GATE-CONVERGE-contracts F4
func TestBrokerWireNMinusOneQuadrants(t *testing.T) {
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{7}, ed25519.SeedSize))
	pub := key.Public().(ed25519.PublicKey)
	old := legacyBrokerUpgradeReq{
		Op: "reload", TargetNode: "brk-a", TransferTo: "brk-b", ToVersion: "v1.2.3",
		ExpectSHA256: "sha", SID: "session", IssuedAt: "2026-10-03T00:00:00Z", Sig: "signature",
	}
	const oldJSON = `{"op":"reload","target_node":"brk-a","transfer_to":"brk-b","to_version":"v1.2.3","expect_sha256":"sha","sid":"session","issued_at":"2026-10-03T00:00:00Z","sig":"signature"}`
	const oldBytes = "reload\nbrk-a\nbrk-b\nv1.2.3\nsha\nsession\n2026-10-03T00:00:00Z"
	var current ClusterUpgradeReq
	if err := json.Unmarshal([]byte(oldJSON), &current); err != nil {
		t.Fatal(err)
	}
	for _, serverNew := range []bool{false, true} {
		for _, clientNew := range []bool{false, true} {
			name := "old-server/old-client"
			if serverNew {
				name = "new-server/old-client"
			}
			if clientNew {
				name = strings.Replace(name, "old-client", "new-client", 1)
			}
			t.Run(name, func(t *testing.T) {
				wire, err := json.Marshal(old)
				canonical := legacyBrokerUpgradeBytes(old)
				if clientNew {
					wire, err = json.Marshal(current)
					canonical = CanonicalUpgradeReqBytes(&current)
				}
				if err != nil || string(wire) != oldJSON || string(canonical) != oldBytes {
					t.Fatalf("ordinary N-1 request changed: wire=%s canonical=%q err=%v", wire, canonical, err)
				}
				sig := ed25519.Sign(key, canonical)
				var verifier []byte
				if serverNew {
					var decoded ClusterUpgradeReq
					if err := json.Unmarshal(wire, &decoded); err != nil {
						t.Fatal(err)
					}
					if err := ValidateUpgradeReq(&decoded); err != nil {
						t.Fatal(err)
					}
					verifier = CanonicalUpgradeReqBytes(&decoded)
				} else {
					var decoded legacyBrokerUpgradeReq
					if err := json.Unmarshal(wire, &decoded); err != nil {
						t.Fatal(err)
					}
					verifier = legacyBrokerUpgradeBytes(decoded)
				}
				if !ed25519.Verify(pub, verifier, sig) {
					t.Fatal("ordinary N-1 signature did not verify")
				}
			})
		}
	}
	t.Run("signed extension cannot downgrade", func(t *testing.T) {
		current.Holder, current.Scope, current.CutID = "holder", "broker-daemon", "cut"
		record := brokerWireRecord()
		current.Maintenance = &record
		const recordJSON = `{"cut_id":"cut","holder":"holder","home":"brk-a","instance_id":"instance","issuer_boot_id":"boot","mode":"quorum","ingress_server_id":"server","generation":2,"phase":2,"issued_boot_ns":10,"expires_boot_ns":20,"term":3,"config_index":4,"voter_digest":"voters","baseline_digest":"baseline","required_capability":"broker-maint-v1"}`
		// Freeze the record's raw bytes independently of the compiled codec;
		// base64 is just the specified representation of those frozen bytes.
		want := oldBytes + "\nbroker-image-v1\nholder\nbroker-daemon\ncut\n" + base64.RawStdEncoding.EncodeToString([]byte(recordJSON))
		got := CanonicalUpgradeReqBytes(&current)
		if string(got) != want {
			t.Fatalf("signed extension bytes=%q want=%q", got, want)
		}
		sig := ed25519.Sign(key, got)
		wire, err := json.Marshal(current)
		if err != nil {
			t.Fatal(err)
		}
		var legacy legacyBrokerUpgradeReq
		if err := json.Unmarshal(wire, &legacy); err != nil {
			t.Fatal(err)
		}
		if ed25519.Verify(pub, legacyBrokerUpgradeBytes(legacy), sig) {
			t.Fatal("old reader verified a new operation after dropping its signed extension")
		}
		// Real signatures must fail after every field perturbation, including
		// nested Record fields. Merely comparing canonical bytes is weaker.
		for i := 0; i < reflect.TypeOf(current).NumField(); i++ {
			field := reflect.TypeOf(current).Field(i)
			if field.Name == "Sig" {
				continue // the signature cannot sign itself
			}
			if field.Name == "Maintenance" {
				for j := 0; j < reflect.TypeOf(record).NumField(); j++ {
					changed := record
					perturbBrokerSignedField(reflect.ValueOf(&changed).Elem().Field(j))
					copy := current
					copy.Maintenance = &changed
					if j == 0 {
						copy.CutID = changed.CutID // remain a valid request
					}
					assertBrokerSignatureChanged(t, pub, sig, &copy, "Maintenance."+reflect.TypeOf(record).Field(j).Name)
				}
				continue
			}
			// S8 fields have their own signed operation. They cannot be placed on
			// the S6 reload, which must reject them before signature verification.
			// Exercise both signatures rather than signing an invalid reload.
			// origin: S7 internal review GATE-CONVERGE-contracts F4
			if field.Name == "ActorFP" || field.Name == "Upgrade" {
				assertBrokerTransactionSignatureChanged(t, key, field.Name)
				copy := current
				reflect.ValueOf(&copy).Elem().Field(i).Set(reflect.ValueOf(ClusterUpgradeReq{ActorFP: "actor", Upgrade: json.RawMessage(`{"op":"status"}`)}).Field(i))
				if ValidateUpgradeReq(&copy) == nil || CanonicalUpgradeReqBytes(&copy) != nil {
					t.Fatalf("%s transaction field was accepted on a legacy operation", field.Name)
				}
				continue
			}
			copy := current
			perturbBrokerSignedField(reflect.ValueOf(&copy).Elem().Field(i))
			if field.Name == "Scope" {
				copy.Scope = "whole-host"
			}
			if field.Name == "CutID" {
				changed := record
				changed.CutID = copy.CutID
				copy.Maintenance = &changed
			}
			assertBrokerSignatureChanged(t, pub, sig, &copy, field.Name)
		}
		copy := current
		copy.Sig = "different signature"
		if !bytes.Equal(CanonicalUpgradeReqBytes(&copy), got) {
			t.Fatal("canonical input included its own signature")
		}
	})
	t.Run("every nonzero extension activates block", func(t *testing.T) {
		for _, req := range []ClusterUpgradeReq{
			{Holder: "h"}, {Scope: "whole-host"}, {Scope: "broker-daemon"}, {CutID: "c"}, {Maintenance: &brokermaint.Record{}},
		} {
			got := CanonicalUpgradeReqBytes(&req)
			if !bytes.Contains(got, []byte("\nbroker-image-v1\n")) {
				t.Fatalf("nonzero extension silently used legacy bytes: %+v -> %q", req, got)
			}
		}
		if got := string(CanonicalUpgradeReqBytes(&ClusterUpgradeReq{Holder: "h"})); got != "\n\n\n\n\n\n\nbroker-image-v1\nh\n\n\nbnVsbA" {
			t.Fatalf("nil maintenance must be signed as base64(null): %q", got)
		}
	})
	if ProtoVersion != 2 || ImageABIVersion != 1 || ClusterHealthSchemaVersion != 8 || BrokerMaintenanceVerb != "broker-maintenance" || brokermaint.Capability != "broker-maint-v1" {
		t.Fatal("S6 changed an epoch, capability, verb or health schema ledger")
	}
	// S7 appends transfer-v1 after broker-image-v1 (additive reader domain,
	// no ABI bump); every S6 domain keeps its position and the ABI stays last.
	// origin: hot-update S7 plan §6.2 / P01
	// S8 then advertises its reader (upgrade-cut-v1 domain and features) once
	// every exporter exists. origin: hot-update S8 plan §6.1; S8 BROKER-EXPORTER R-11
	if !slices.Equal(ImageVersionLines(), []string{"image-domains exec-v1 pty-v1 flow-v1 ss-v1 broker-image-v1 transfer-v1 upgrade-cut-v1", "image-read-epochs 1",
		"image-features upgrade-tx-v1 authority-incarnation-v1 upgrade-cut-v1", "image-abi v1"}) {
		t.Fatalf("image domains or final ABI line changed: %q", ImageVersionLines())
	}
}

// S8's actor and envelope are covered by real signatures on structurally valid
// transaction triggers, including the old reader's refusal to verify them.
// origin: S7 internal review GATE-CONVERGE-contracts F4
func assertBrokerTransactionSignatureChanged(t *testing.T, key ed25519.PrivateKey, field string) {
	t.Helper()
	req := ClusterUpgradeReq{Op: "upgrade-tx", TargetNode: "brk-a", IssuedAt: "2026-10-03T00:00:00Z", ActorFP: "actor", Upgrade: json.RawMessage(`{"op":"status"}`)}
	const want = "upgrade-tx\nbrk-a\n\n\n\n\n2026-10-03T00:00:00Z\nbroker-image-v1\n\n\n\nbnVsbA\nupgrade-tx-v1\nactor\neyJvcCI6InN0YXR1cyJ9"
	canonical := CanonicalUpgradeReqBytes(&req)
	if err := ValidateUpgradeReq(&req); err != nil || string(canonical) != want {
		t.Fatalf("transaction signed bytes=%q err=%v", canonical, err)
	}
	sig := ed25519.Sign(key, canonical)
	pub := key.Public().(ed25519.PublicKey)
	wire, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var old legacyBrokerUpgradeReq
	if err := json.Unmarshal(wire, &old); err != nil {
		t.Fatal(err)
	}
	if ed25519.Verify(pub, legacyBrokerUpgradeBytes(old), sig) {
		t.Fatal("legacy reader verified a transaction after dropping its extension")
	}
	if field == "ActorFP" {
		req.ActorFP = "other-actor"
	} else {
		req.Upgrade = json.RawMessage(`{"op":"cancel"}`)
	}
	assertBrokerSignatureChanged(t, pub, sig, &req, field)
}

func brokerWireRecord() brokermaint.Record {
	return brokermaint.Record{CutID: "cut", Holder: "holder", Home: "brk-a", InstanceID: "instance", IssuerBootID: "boot", Mode: "quorum", IngressServerID: "server", Generation: 2, Phase: brokermaint.MaintActive, IssuedBootNS: 10, ExpiresBootNS: 20, Term: 3, ConfigIndex: 4, VoterDigest: "voters", BaselineDigest: "baseline", RequiredCapability: brokermaint.Capability}
}

func perturbBrokerSignedField(v reflect.Value) {
	switch v.Kind() {
	case reflect.String:
		v.SetString(v.String() + "-changed")
	case reflect.Uint8, reflect.Uint64:
		v.SetUint(v.Uint() + 1)
	case reflect.Int64:
		v.SetInt(v.Int() + 1)
	default:
		panic("unhandled signed field: " + v.Type().String())
	}
}

func assertBrokerSignatureChanged(t *testing.T, pub ed25519.PublicKey, sig []byte, req *ClusterUpgradeReq, field string) {
	t.Helper()
	if err := ValidateUpgradeReq(req); err != nil {
		t.Fatalf("%s mutation must remain structurally valid: %v", field, err)
	}
	if ed25519.Verify(pub, CanonicalUpgradeReqBytes(req), sig) {
		t.Errorf("tampered %s retained a valid signature", field)
	}
}

// origin: hot-update S6 plan §9.1 / T46
func TestBrokerUpgradeRejectsAmbiguousSignedIdentifiers(t *testing.T) {
	if ValidateUpgradeReq(nil) == nil || CanonicalUpgradeReqBytes(nil) != nil {
		t.Fatal("missing upgrade request is signable")
	}
	base := ClusterUpgradeReq{Holder: "holder", Scope: "broker-daemon", CutID: "cut"}
	for _, invalid := range []string{"bad\nidentifier", "bad\ridentifier", "bad\xffidentifier"} {
		for i := 0; i < reflect.TypeOf(base).NumField(); i++ {
			field := reflect.TypeOf(base).Field(i)
			if field.Type.Kind() != reflect.String || field.Name == "Sig" {
				continue
			}
			copy := base
			reflect.ValueOf(&copy).Elem().Field(i).SetString(invalid)
			if ValidateUpgradeReq(&copy) == nil || CanonicalUpgradeReqBytes(&copy) != nil {
				t.Errorf("ambiguous signed %s was accepted", field.Name)
			}
		}
		for i := 0; i < reflect.TypeFor[brokermaint.Record]().NumField(); i++ {
			record := brokerWireRecord()
			v := reflect.ValueOf(&record).Elem().Field(i)
			if v.Kind() != reflect.String {
				continue
			}
			v.SetString(invalid)
			copy := base
			copy.Maintenance = &record
			copy.CutID = record.CutID
			if ValidateUpgradeReq(&copy) == nil || CanonicalUpgradeReqBytes(&copy) != nil {
				t.Errorf("ambiguous signed record %s was accepted", reflect.TypeOf(record).Field(i).Name)
			}
		}
	}
	for _, scope := range []string{"broker-only", "unknown", "BROKER-DAEMON"} {
		copy := base
		copy.Scope = scope
		if ValidateUpgradeReq(&copy) == nil || CanonicalUpgradeReqBytes(&copy) != nil {
			t.Errorf("unknown scope %q is signable", scope)
		}
	}
	record := brokerWireRecord()
	record.CutID = "another-cut"
	base.Maintenance = &record
	if ValidateUpgradeReq(&base) == nil || CanonicalUpgradeReqBytes(&base) != nil {
		t.Fatal("mismatched maintenance cut is signable")
	}
}

// origin: hot-update S6 plan §9.1 / T46
func TestBrokerWireFieldsAndRegistrationGoldens(t *testing.T) {
	for _, tc := range []struct {
		typ    reflect.Type
		fields map[string]string
	}{
		{reflect.TypeFor[ClusterUpgradeReq](), map[string]string{"Holder": "string|holder,omitempty", "Scope": "string|scope,omitempty", "CutID": "string|cut_id,omitempty", "Maintenance": "*brokermaint.Record|maintenance,omitempty"}},
		{reflect.TypeFor[ClusterUpgradeResp](), map[string]string{"BrokerImage": "*proto.BrokerImageStatus|broker_image,omitempty", "NextGeneration": "uint64|next_generation,omitempty"}},
		{reflect.TypeFor[ClusterHealthResp](), map[string]string{"BrokerImage": "*proto.BrokerImageStatus|broker_image,omitempty"}},
		{reflect.TypeFor[NodeRegisterReq](), map[string]string{"BrokerImageMaintenance": "string|broker_image_maintenance,omitempty"}},
		{reflect.TypeFor[NodeRegisterResp](), map[string]string{"BrokerImageMaintenance": "string|broker_image_maintenance,omitempty"}},
		{reflect.TypeFor[BrokerImageStatus](), map[string]string{"RunningSHA256": "string|running_sha256,omitempty", "Generation": "uint64|generation,omitempty", "Phase": "string|phase,omitempty", "CutID": "string|cut_id,omitempty", "Mode": "string|mode,omitempty"}},
	} {
		for name, want := range tc.fields {
			field, found := tc.typ.FieldByName(name)
			if !found || field.Type.String()+"|"+field.Tag.Get("json") != want {
				t.Errorf("%s.%s type/tag drift: %+v want %s", tc.typ, name, field, want)
			}
		}
	}
	// Existing on-disk goldens represent the ordinary N-1 path. Keep those
	// bytes rather than rewriting them to hide a zero-value incompatibility.
	for _, fixture := range goldenFixtures(t) {
		if !strings.HasPrefix(fixture.name, "node_register_") {
			continue
		}
		want, err := os.ReadFile(goldenPath(fixture.name))
		if err != nil {
			t.Fatal(err)
		}
		got, err := json.MarshalIndent(fixture.v, "", "  ")
		if err != nil || !bytes.Equal(append(got, '\n'), want) {
			t.Fatalf("ordinary register bytes changed: %s err=%v", fixture.name, err)
		}
	}
	for _, tc := range []struct {
		value any
		want  string
	}{
		{NodeRegisterReq{BrokerImageMaintenance: brokermaint.Capability}, `{"proto_version":0,"release_version":"","nid":"","os":"","arch":"","broker_image_maintenance":"broker-maint-v1"}`},
		{NodeRegisterResp{OK: true, BrokerImageMaintenance: brokermaint.Capability}, `{"ok":true,"broker_image_maintenance":"broker-maint-v1"}`},
		{ClusterUpgradeResp{OK: true}, `{"ok":true}`},
		{ClusterUpgradeResp{OK: true, BrokerImage: &BrokerImageStatus{RunningSHA256: "running", Generation: 2, Phase: "control_ready", CutID: "cut", Mode: "single"}, NextGeneration: 3}, `{"ok":true,"broker_image":{"running_sha256":"running","generation":2,"phase":"control_ready","cut_id":"cut","mode":"single"},"next_generation":3}`},
	} {
		got, err := json.Marshal(tc.value)
		if err != nil || string(got) != tc.want {
			t.Errorf("wire golden=%s want=%s err=%v", got, tc.want, err)
		}
	}
	for _, cap := range []string{"", "unknown", brokermaint.Capability} {
		wire, err := json.Marshal(NodeRegisterReq{NID: "lab-1", BrokerImageMaintenance: cap})
		if err != nil {
			t.Fatal(err)
		}
		var old struct {
			NID string `json:"nid"`
		}
		if err := json.Unmarshal(wire, &old); err != nil || old.NID != "lab-1" {
			t.Fatalf("old register reader rejected additive capability: %s err=%v", wire, err)
		}
		var newReq NodeRegisterReq
		if err := json.Unmarshal(wire, &newReq); err != nil || newReq.BrokerImageMaintenance != cap {
			t.Fatalf("new register reader guessed capability: %s err=%v", wire, err)
		}
	}
}

// origin: hot-update S6 plan §9.1 / T46
func TestBrokerWireInventoryControls(t *testing.T) {
	const source = "package adminsock\ntype RuntimeReport struct { BrokerImage *Leaf `json:\"broker_image,omitempty\"` }\ntype Leaf struct { Generation uint64 `json:\"generation,omitempty\"` }\ntype Unrelated struct { Private string }\n"
	collect := func(src string, roots []string) ([]string, error) {
		file, err := parser.ParseFile(token.NewFileSet(), "protocol.go", src, parser.SkipObjectResolution)
		if err != nil {
			return nil, err
		}
		return wireFieldsFromFiles([]*ast.File{file}, "adminsock.", roots)
	}
	golden := []string{"adminsock.Leaf.Generation\tjson:\"generation,omitempty\"\tuint64", "adminsock.RuntimeReport.BrokerImage\tjson:\"broker_image,omitempty\"\t*Leaf"}
	got, err := collect(source, []string{"RuntimeReport"})
	if err != nil || !slices.Equal(got, golden) {
		t.Fatalf("admin root closure=%q want=%q err=%v", got, golden, err)
	}
	for _, mutation := range []string{
		strings.Replace(source, "Generation uint64", "Generation int64", 1),
		strings.Replace(source, "generation,omitempty", "generation_changed,omitempty", 1),
		strings.Replace(source, "BrokerImage *Leaf", "Removed *Leaf", 1),
	} {
		got, err := collect(mutation, []string{"RuntimeReport"})
		if err != nil || slices.Equal(got, golden) {
			t.Fatalf("type/tag/field negative control did not expose inventory drift: %q err=%v", got, err)
		}
	}
	if _, err := collect(source, []string{"Missing"}); err == nil {
		t.Fatal("missing admin root silently became an empty inventory")
	}
	current := collectWireFields(t)
	for _, row := range []string{
		"adminsock.Request.Op\tjson:\"op\"\tstring",
		"adminsock.Response.BrokerImage\tjson:\"broker_image,omitempty\"\t*proto.BrokerImageStatus",
		"adminsock.Response.NextGeneration\tjson:\"next_generation,omitempty\"\tuint64",
		"adminsock.RuntimeReport.BrokerImage\tjson:\"broker_image,omitempty\"\t*proto.BrokerImageStatus",
		"BrokerImageStatus.Generation\tjson:\"generation,omitempty\"\tuint64",
		"BrokerMaintenanceAck.RemainingNS\tjson:\"remaining_ns\"\tint64",
	} {
		if !slices.Contains(current, row) {
			t.Errorf("actual wire root missing: %s", row)
		}
	}
}

// Required body fields, including explicit zero values, have a single JSON
// order. These byte goldens are also the schema input for P15's signed codec;
// they do not stand in for exercising that codec once its API is available.
// origin: hot-update S6 plan §9.1 / T46
func TestBrokerMaintenanceWireBodies(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  string
	}{
		{BrokerMaintenanceReq{}, `{"record":{"cut_id":"","holder":"","home":"","instance_id":"","issuer_boot_id":"","mode":"","ingress_server_id":"","generation":0,"phase":0,"issued_boot_ns":0,"expires_boot_ns":0,"term":0,"config_index":0,"voter_digest":"","baseline_digest":"","required_capability":""},"proof":{"cut_id":"","nonce":"","issuer_boot_id":"","response_boot_ns":0,"expires_boot_ns":0,"record_digest":"","applied_index":0,"sig":""},"nid":"","connected_broker_id":"","ingress_server_id":"","route_digest":"","sig":""}`},
		{BrokerMaintenanceReq{Record: brokerWireRecord(), Proof: brokermaint.Proof{CutID: "cut", Nonce: "nonce", IssuerBootID: "boot", ResponseBootNS: 11, ExpiresBootNS: 20, RecordDigest: "record", AppliedIndex: 5, Sig: "proof-signature"}, NID: "lab-1", ConnectedBrokerID: "brk-a", IngressServerID: "server", RouteDigest: "route", Sig: "request-signature"}, `{"record":{"cut_id":"cut","holder":"holder","home":"brk-a","instance_id":"instance","issuer_boot_id":"boot","mode":"quorum","ingress_server_id":"server","generation":2,"phase":2,"issued_boot_ns":10,"expires_boot_ns":20,"term":3,"config_index":4,"voter_digest":"voters","baseline_digest":"baseline","required_capability":"broker-maint-v1"},"proof":{"cut_id":"cut","nonce":"nonce","issuer_boot_id":"boot","response_boot_ns":11,"expires_boot_ns":20,"record_digest":"record","applied_index":5,"sig":"proof-signature"},"nid":"lab-1","connected_broker_id":"brk-a","ingress_server_id":"server","route_digest":"route","sig":"request-signature"}`},
		{BrokerMaintenanceAck{}, `{"cut_id":"","nonce":"","nid":"","connected_broker_id":"","ingress_server_id":"","instance_id":"","route_digest":"","proof_digest":"","accepted":false,"remaining_ns":0,"sig":""}`},
		{BrokerMaintenanceAck{CutID: "cut", Nonce: "nonce", NID: "lab-1", ConnectedBrokerID: "brk-a", IngressServerID: "server", InstanceID: "instance", RouteDigest: "route", ProofDigest: "proof", Accepted: true, RemainingNS: 9, Sig: "ack-signature"}, `{"cut_id":"cut","nonce":"nonce","nid":"lab-1","connected_broker_id":"brk-a","ingress_server_id":"server","instance_id":"instance","route_digest":"route","proof_digest":"proof","accepted":true,"remaining_ns":9,"sig":"ack-signature"}`},
	} {
		got, err := json.Marshal(tc.value)
		if err != nil || string(got) != tc.want {
			t.Fatalf("maintenance body=%s want=%s err=%v", got, tc.want, err)
		}
		back := reflect.New(reflect.TypeOf(tc.value))
		if err := json.Unmarshal([]byte(tc.want), back.Interface()); err != nil || !reflect.DeepEqual(back.Elem().Interface(), tc.value) {
			t.Fatalf("maintenance body did not roundtrip: %+v err=%v", back.Elem().Interface(), err)
		}
	}
	if got := SubjCmdForwarded("lab", "lab-1", BrokerMaintenanceVerb); got != "tether.v2.s.lab.cmd.node.lab-1.broker-maintenance.req.forwarded" {
		t.Fatalf("maintenance escaped the existing forwarded subject tree: %s", got)
	}
	// Numeric zeros in JSON do not detect width/signedness drift; pin the
	// public types too, especially remaining duration versus remote BootNS.
	for _, tc := range []struct {
		typ   reflect.Type
		field string
		want  reflect.Type
	}{
		{reflect.TypeFor[BrokerMaintenanceReq](), "Record", reflect.TypeFor[brokermaint.Record]()},
		{reflect.TypeFor[BrokerMaintenanceReq](), "Proof", reflect.TypeFor[brokermaint.Proof]()},
		{reflect.TypeFor[BrokerMaintenanceAck](), "Accepted", reflect.TypeFor[bool]()},
		{reflect.TypeFor[BrokerMaintenanceAck](), "RemainingNS", reflect.TypeFor[int64]()},
	} {
		field, found := tc.typ.FieldByName(tc.field)
		if !found || field.Type != tc.want {
			t.Fatalf("%s.%s changed type: %+v want %s", tc.typ, tc.field, field, tc.want)
		}
	}
}
