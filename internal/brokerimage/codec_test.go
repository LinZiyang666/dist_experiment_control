//go:build linux

package brokerimage

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/handoff"
	"github.com/LinZiyang666/tether/internal/listenerimage"
	"github.com/LinZiyang666/tether/internal/storage"
)

func fixtureSHA(data string) string {
	digest := sha256.Sum256([]byte(data))
	return hex.EncodeToString(digest[:])
}
func fixtureBundle(t *testing.T) Bundle {
	t.Helper()
	running, err := hashArtifact("/proc/self/exe")
	if err != nil {
		t.Fatal(err)
	}
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	migration := []storage.MigrationDigest{{Name: "001_fixture.sql", SHA256: fixtureSHA("CREATE TABLE witness (id TEXT PRIMARY KEY);")}}
	stamp := storage.StoreStamp{Identity: storage.DBIdentity{Path: "/fixture/current.db", DataDir: "/fixture", LockPath: "/fixture/tether.lock", Device: 1, Inode: 2}, Schema: storage.SchemaStamp{Applied: migration, Supported: migration, Digest: migrationDigest(migration), ObjectDigest: fixtureSHA("fixture-object")}, Journal: "delete", Synchronous: 2, AppliedIndex: 17, AppliedTerm: 3}
	seal := storage.StoreSeal{Stamp: stamp, Writer: storage.CloseClosed, ReadOnly: storage.CloseClosed, Bolt: storage.CloseClosed, Transport: storage.CloseClosed, Raft: storage.CloseClosed, CheckpointBusy: -1, CheckpointLog: -1, CheckpointDone: -1}
	return Bundle{Schema: Domain, SchemaVersion: BundleVersion, Role: ImageRole, Mode: ModeSingle, Generation: 8, ExporterSHA256: running, CandidateSHA256: running, PreviousSHA256: running, InstanceID: "fixture-instance", BootID: strings.TrimSpace(string(boot)), Args: []string{"fixture", "broker", "serve", "--db", "/fixture/current.db"}, ConfigDigest: fixtureSHA("fixture-config"), RequiredDomains: []string{Domain}, StateResource: "broker-state.fixture", LockResource: "broker-lock.fixture", Store: stamp, Seal: seal, Listeners: []listenerimage.Descriptor{}, Maintenance: []brokermaint.LocalLease{}}
}
func fixturePrivate(b Bundle) PrivateState {
	return PrivateState{Runtime: runtimeState(b, "cut-fixture"), Store: b.Seal, Control: ControlState{AdmissionGeneration: b.Generation, RequiredHandlers: []string{"auth", "status"}, RequiredLoops: []string{"maintenance", "roster"}, AuthReady: true, ControlReady: true, Maintenance: b.Maintenance, HomeBaselines: []HomeBaseline{{Home: "broker-a", DwellBootNS: 71, PlannedDown: true, PlannedReturn: true}}}, Resources: []ResourceOwnership{}}
}

// origin: hot-update S6 plan §2.2 / T47
func TestBrokerPrivateStateRoundTrip(t *testing.T) {
	b := fixtureBundle(t)
	state := fixturePrivate(b)
	state.Runtime.Active = true
	state.Control.HomeHistory = []byte("original fixed-expiry challenge and historical dwell")
	state.Resources = []ResourceOwnership{{ID: "lock", Kind: "store-lock-v1", Name: "lock", Purpose: "store-lock", Identity: "1.2.3", Owner: "store", CutID: state.Runtime.CutID}}
	data, err := EncodePrivateState(state)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodePrivateState(data)
	if decoded.Runtime.CandidateSHA256 != b.CandidateSHA256 || decoded.Runtime.PreviousSHA256 != b.PreviousSHA256 || decoded.Runtime.ConfigDigest != b.ConfigDigest || decoded.Runtime.Generation != b.Generation || !reflect.DeepEqual(decoded.Runtime.Args, b.Args) || !reflect.DeepEqual(decoded.Runtime.RequiredDomains, b.RequiredDomains) {
		t.Fatal("runtime export omitted public lineage")
	}
	if err != nil || !reflect.DeepEqual(decoded, state) || CheckPrivateState(decoded, b) != nil {
		t.Fatalf("private decisions changed: %v", err)
	}
	// Every nonzero field must be represented, rather than merely validated by an
	// inventory made from the same public Bundle that omitted private state.
	for _, name := range []string{"admission_generation", "auth_ready", "control_ready", "home_baselines", "required_handlers", "required_loops", "cut_id", "active", "candidate_sha256", "previous_sha256", "owners", "active_rpcs", "checkpoint_done", "driver_writers"} {
		if !bytes.Contains(data, []byte(name)) {
			t.Errorf("missing private decision %s", name)
		}
	}
	for _, mutation := range []func(*PrivateState){
		func(s *PrivateState) { s.Runtime.Generation++ },
		func(s *PrivateState) { s.Runtime.ConfigDigest = fixtureSHA("changed") },
		func(s *PrivateState) { s.Control.AdmissionGeneration++ },
		func(s *PrivateState) { s.Control.HomeBaselines[0].DwellBootNS = -1 },
		func(s *PrivateState) { s.Store.Writer = storage.CloseUnknown },
		func(s *PrivateState) { s.Resources[0].CutID = "other-cut" },
	} {
		changed, _ := DecodePrivateState(data)
		mutation(&changed)
		if CheckPrivateState(changed, b) == nil {
			t.Error("private/public mismatch accepted")
		}
	}
}

// origin: hot-update S6 plan §2.1 / §2.2 / T47
func TestBrokerCodecRejectsAmbiguousAndOpenState(t *testing.T) {
	b := fixtureBundle(t)
	data, err := Encode(b)
	if err != nil {
		t.Fatal(err)
	}
	envelope := handoff.State{Role: ImageRole, Instance: b.InstanceID, Generation: b.Generation, Data: data}
	got, err := Decode(envelope)
	if err != nil || !reflect.DeepEqual(got, b) {
		t.Fatalf("bundle changed: %v", err)
	}
	for _, bad := range [][]byte{
		append(append([]byte{}, data...), []byte(" {}")...),
		bytes.Replace(data, []byte(`"generation":8,`), nil, 1),
		bytes.Replace(data, []byte(`"generation":8`), []byte(`"generation":8,"generation":8`), 1),
		bytes.Replace(data, []byte(`"generation":8`), []byte(`"generation":8,"unknown":0`), 1),
	} {
		envelope.Data = bad
		if _, err := Decode(envelope); err == nil {
			t.Error("ambiguous envelope accepted")
		}
	}
	for _, change := range []func(*Bundle){
		// origin: hot-update S6 plan §2.1 / T47
		func(b *Bundle) { b.ConfigDigest = strings.Repeat("A", 64) },
		func(b *Bundle) { b.InstanceID = "instance\nforged" },
		func(b *Bundle) { b.Seal.Writer = storage.CloseStopped }, func(b *Bundle) { b.Seal.DriverReaders = 1 },
		func(b *Bundle) { b.Seal.CheckpointBusy = 1 }, func(b *Bundle) { b.PreviousSHA256 = fixtureSHA("third") },
		func(b *Bundle) { b.RequiredDomains = []string{Domain, Domain} }, func(b *Bundle) { b.Store.Schema.Applied = nil },
		func(b *Bundle) { b.Listeners = []listenerimage.Descriptor{{ID: "bad-flags", CloseOnExec: false}} },
	} {
		changed := fixtureBundle(t)
		change(&changed)
		if _, err := Encode(changed); err == nil {
			t.Error("unsafe final bundle accepted")
		}
	}
}

// origin: hot-update S6 plan §2.2 / FuzzBrokerPrivateState
func FuzzBrokerPrivateState(f *testing.F) {
	// Seed the actual bounded framing as well as every truncation/length boundary.
	state := PrivateState{Runtime: RuntimeState{Args: []string{}, RequiredDomains: []string{}, Generation: 17, Active: true}, Control: ControlState{RequiredHandlers: []string{}, RequiredLoops: []string{}, Maintenance: []brokermaint.LocalLease{}, HomeBaselines: []HomeBaseline{}}, Resources: []ResourceOwnership{}}
	data, err := EncodePrivateState(state)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(data)
	f.Add([]byte{})
	f.Add([]byte("TETHBR6\x00"))
	huge := append([]byte(nil), data...)
	binary.BigEndian.PutUint32(huge[12:16], ^uint32(0))
	f.Add(huge)
	for _, offset := range []int{11, 12, 16, len(data) - 1} {
		f.Add(append([]byte(nil), data[:offset]...))
	}
	malformed := append(append([]byte{}, data...), 0)
	f.Add(malformed)
	f.Fuzz(func(t *testing.T, input []byte) {
		decoded, err := DecodePrivateState(input)
		if err != nil {
			return
		}
		encoded, err := EncodePrivateState(decoded)
		if err != nil || !bytes.Equal(input, encoded) {
			t.Fatal("accepted noncanonical private state")
		}
		if _, err = DecodePrivateState(append(append([]byte{}, encoded...), 0)); err == nil {
			t.Fatal("decoder ignored trailing private bytes")
		}
	})
}

// origin: hot-update S6 plan §2.2 / T47
func TestBrokerPrivateDescriptorMatchesActualCodec(t *testing.T) {
	expected := map[string][]string{
		"broker-runtime": {"active", "args", "boot_id", "candidate_sha256", "config_digest", "cut_id", "exporter_sha256", "generation", "instance_id", "mode", "previous_sha256", "required_domains", "role"},
		"control":        {"admission_generation", "auth_ready", "control_ready", "home_baselines", "home_history", "maintenance", "required_handlers", "required_loops"},
		"resources":      {"owners"},
		"store":          {"active_backups", "active_rpcs", "bolt", "checkpoint_busy", "checkpoint_done", "checkpoint_log", "driver_readers", "driver_writers", "raft", "read_only", "role", "stamp", "transport", "writer"},
	}
	if len(CodecRecordDescriptors) != len(expected) {
		t.Fatal("record set changed")
	}
	for _, descriptor := range CodecRecordDescriptors {
		names := []string{}
		for _, field := range descriptor.Fields {
			names = append(names, field.Name)
			if field.Type == "" || field.Encoding != "canonical-json" || field.MaxBytes <= 0 || field.CompatibleReader != Domain || field.Secret {
				t.Fatalf("incomplete field descriptor: %s/%s", descriptor.Kind, field.Name)
			}
		}
		if !reflect.DeepEqual(names, expected[descriptor.Kind]) {
			t.Errorf("private record %s changed: %v", descriptor.Kind, names)
		}
	}
	// In addition to required framing fields, nested projections cannot silently
	// omit a zero/false field through omitempty when the encoder is refactored.
	for _, typ := range []reflect.Type{reflect.TypeFor[RuntimeState](), reflect.TypeFor[ControlState](), reflect.TypeFor[ResourceOwnership](), reflect.TypeFor[HomeBaseline]()} {
		value := reflect.New(typ).Elem().Interface()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < typ.NumField(); i++ {
			name := typ.Field(i).Tag.Get("json")
			if strings.Contains(name, ",") || !bytes.Contains(data, []byte(`"`+name+`":`)) {
				t.Fatalf("required zero decision omitted: %s", name)
			}
		}
	}
}

// origin: hot-update S6 plan §3.1 / T06 / T07
func TestBrokerSchemaProofRejectsMissingEarlyRows(t *testing.T) {
	schema := fixtureBundle(t).Store.Schema
	second := storage.MigrationDigest{Name: "002_fixture.sql", SHA256: fixtureSHA("CREATE TABLE later (id INTEGER);")}
	schema.Applied = append(append([]storage.MigrationDigest{}, schema.Applied...), second)
	schema.Supported = append([]storage.MigrationDigest{}, schema.Applied...)
	schema.Digest = migrationDigest(schema.Applied)
	if validateSchema(schema) != nil {
		t.Fatal("exact schema proof rejected")
	}
	for _, mutate := range []func(*storage.SchemaStamp){
		func(s *storage.SchemaStamp) { s.Applied = s.Applied[1:] },
		func(s *storage.SchemaStamp) { s.Applied = nil },
		func(s *storage.SchemaStamp) { s.Supported = nil },
		func(s *storage.SchemaStamp) { s.Digest = fixtureSHA("wrong set") },
		func(s *storage.SchemaStamp) { s.ObjectDigest = "" },
	} {
		changed := schema
		mutate(&changed)
		if validateSchema(changed) == nil {
			t.Fatal("schema set mismatch accepted")
		}
	}
}

// A descriptor is a reader promise, not documentation detached from its codec.
// Both directions must refuse metadata drift before consuming framed decisions.
// origin: hot-update S6 plan §2.2 / T47
func TestBrokerPrivateDescriptorMetadataIsEnforced(t *testing.T) {
	state := fixturePrivate(fixtureBundle(t))
	encoded, err := EncodePrivateState(state)
	if err != nil {
		t.Fatal(err)
	}
	original := CodecRecordDescriptors
	defer func() { CodecRecordDescriptors = original }()
	for _, defect := range []string{"name", "type", "encoding", "max", "secret", "reader", "version", "kind", "record", "missing", "record-reader", "fields", "duplicate", "order"} {
		t.Run(defect, func(t *testing.T) {
			defer func() {
				if recover() != nil {
					t.Fatal("codec panicked on descriptor drift")
				}
			}()
			broken := append([]PrivateRecordDescriptor(nil), original...)
			broken[0].Fields = append([]PrivateFieldDescriptor(nil), broken[0].Fields...)
			switch defect {
			case "name":
				broken[0].Fields[0].Name = "unknown"
			case "type":
				broken[0].Fields[0].Type = "unknown"
			case "encoding":
				broken[0].Fields[0].Encoding = "unknown"
			case "max":
				broken[0].Fields[0].MaxBytes++
			case "secret":
				broken[0].Fields[0].Secret = true
			case "reader":
				broken[0].Fields[0].CompatibleReader = "unknown"
			case "version":
				broken[0].Version++
			case "kind":
				broken[0].Kind = "unknown"
			case "record":
				broken[0].StateRecord = "unknown"
			case "missing":
				broken = broken[:len(broken)-1]
			case "record-reader":
				broken[0].CompatibleReader = "unknown"
			case "fields":
				broken[0].Fields = broken[0].Fields[1:]
			case "duplicate":
				broken[0].Fields[1] = broken[0].Fields[0]
			case "order":
				broken[0].Fields[0], broken[0].Fields[1] = broken[0].Fields[1], broken[0].Fields[0]
			}
			CodecRecordDescriptors = broken
			defer func() { CodecRecordDescriptors = original }()
			if _, err := EncodePrivateState(state); err == nil {
				t.Fatal("encoder accepted descriptor drift")
			}
			if _, err := DecodePrivateState(encoded); err == nil {
				t.Fatal("decoder accepted descriptor drift")
			}
		})
	}
	if _, err := DecodePrivateState(encoded); err != nil {
		t.Fatal(err)
	}
}
