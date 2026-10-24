package infracontract_test

import (
	"bytes"
	"crypto/ed25519"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"

	c "github.com/LinZiyang666/tether/internal/infracontract"
)

// Every signed field, operation operand and alternate spelling has a negative
// arm. The independent literal golden catches omission even if another guard
// would also reject a particular changed field before signature verification.
// origin: hot-update S9 plan §2.1 / §2.2 / §2.3 / T02 / M01
func TestInfraRequestCanonicalBinding(t *testing.T) {
	t.Run("full_signature", checkRequestSignature)
	t.Run("strict_bytes", checkStrictBytes)
	t.Run("host_operations", checkHostOperations)
	t.Run("identity_completeness", checkIdentityCompleteness)
	t.Run("bounds", checkRequestBounds)
	t.Run("receipt_and_route", checkPrivateRecords)
	t.Run("plan_and_observation", checkPlanObservation)
}

func requestFixture() c.HostRequest {
	return c.HostRequest{
		Version: 1, Op: c.RecoverHost, Component: c.Caddy, Tx: c.ID{1}, Step: 2,
		Domain: c.ID{3}, Sequence: 4, PolicyRevision: 5, KeyID: "key", HostBootID: "boot", Nonce: c.ID{6},
		Expected:        c.Process{BootID: "boot", PID: 41, StartTicks: 42, InvocationID: "invocation", ExeDevice: 43, ExeInode: 44, ExeSHA256: c.Digest{45}, Cgroup: "/unit"},
		Candidate:       c.Artifact{Kind: c.CaddyBinary, Version: "2.7.6", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: c.Digest{46}, BinarySHA256: c.Digest{47}, CatalogSHA256: c.Digest{48}},
		Store:           c.Store{Device: 1, Inode: 2, MountID: 3, LockDevice: 4, LockInode: 5},
		ExpectedJournal: c.Digest{50}, Config: c.Digest{51}, BeforeCensus: c.Digest{52},
		WarmCensus: c.Digest{53}, ReturnProof: c.Digest{54}, Payload: c.Digest{55}, RemainingNS: 123456,
	}
}

func fixtureKey() ed25519.PrivateKey {
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{17}, ed25519.SeedSize))
}

func signedFixture(t testing.TB) c.HostRequest {
	t.Helper()
	r := requestFixture()
	data, err := c.SigningBytes(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Signature = ed25519.Sign(fixtureKey(), data)
	return r
}

func checkRequestSignature(t *testing.T) {
	r := requestFixture()
	data, err := c.SigningBytes(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "tether-infra-host-v1\n"+requestGolden {
		t.Fatal("full canonical signing golden changed")
	}
	r = signedFixture(t)
	before := append([]byte(nil), r.Signature...)
	publicKey := fixtureKey().Public().(ed25519.PublicKey)
	if err := c.VerifyRequest(r, publicKey); err != nil {
		t.Fatal(err)
	}
	after, err := c.SigningBytes(r)
	if err != nil || !bytes.Equal(after, data) || !bytes.Equal(r.Signature, before) {
		t.Fatal("signing mutated caller storage or included Signature")
	}
	// Walk nested fields and every fixed-array byte. A real Ed25519 check must
	// reject changes to identities/digests, not merely top-level envelope fields.
	fieldPaths(reflect.ValueOf(r), nil, func(path []int) {
		if path[0] == reflect.TypeFor[c.HostRequest]().NumField()-1 {
			return
		}
		changed := r
		field := reflect.ValueOf(&changed).Elem().FieldByIndex(path)
		if field.Kind() == reflect.Array {
			for i := 0; i < field.Len(); i++ {
				changed := r
				item := reflect.ValueOf(&changed).Elem().FieldByIndex(path).Index(i)
				item.SetUint(item.Uint() + 1)
				if c.VerifyRequest(changed, publicKey) == nil {
					t.Errorf("signature accepted changed array %v byte %d", path, i)
				}
			}
			return
		}
		switch field.Kind() {
		case reflect.String:
			field.SetString(field.String() + "x")
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			field.SetUint(field.Uint() + 1)
		case reflect.Int32:
			field.SetInt(field.Int() + 1)
		default:
			t.Fatalf("unexercised field kind: %s", field.Kind())
		}
		if err := c.VerifyRequest(changed, publicKey); err == nil {
			t.Errorf("signature accepted changed field %v", path)
		}
		if !bytes.Equal(r.Signature, before) {
			t.Fatal("verification changed signature storage")
		}
	})
	for _, key := range [][]byte{nil, make([]byte, 31), make([]byte, 32), make([]byte, 33)} {
		if c.VerifyRequest(r, key) == nil {
			t.Fatal("invalid public key accepted")
		}
	}
	for _, sig := range [][]byte{nil, {}, make([]byte, 63), make([]byte, 64), make([]byte, 65)} {
		changed := r
		changed.Signature = sig
		if c.VerifyRequest(changed, publicKey) == nil {
			t.Fatal("invalid signature accepted")
		}
	}
	r.Signature = ed25519.Sign(fixtureKey(), append([]byte("infra-route-v1\n"), []byte(requestGolden)...))
	if c.VerifyRequest(r, publicKey) == nil {
		t.Fatal("route signing domain accepted as host request")
	}
}

// Arrays are reached via index paths separately from struct FieldByIndex.
func fieldPaths(v reflect.Value, path []int, visit func([]int)) {
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			fieldPaths(v.Field(i), append(slices.Clone(path), i), visit)
		}
	case reflect.Array:
		// The caller handles arrays as a whole to keep paths valid FieldByIndex
		// paths; each byte is then independently mutated below.
		visit(path)
	case reflect.Slice:
	default:
		visit(path)
	}
}

func checkStrictBytes(t *testing.T) {
	r := signedFixture(t)
	canonical, err := c.EncodeRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.DecodeRequest(canonical)
	if err != nil || !reflect.DeepEqual(decoded, r) {
		t.Fatalf("canonical round trip failed: %v", err)
	}
	for name, bad := range badRequestBytes(canonical) {
		t.Run(name, func(t *testing.T) {
			got, err := c.DecodeRequest(bad)
			if err == nil || !reflect.ValueOf(got).IsZero() {
				t.Fatalf("accepted noncanonical/invalid %s", name)
			}
		})
	}
	// Empty slices have exactly one spelling, null. The encoder normalizes a
	// caller-owned empty slice without changing it into a nil slice in place.
	unsigned := requestFixture()
	unsigned.Signature = []byte{}
	encoded, err := c.EncodeRequest(unsigned)
	if err != nil || string(encoded) != requestGolden || unsigned.Signature == nil {
		t.Fatal("empty signature normalization failed")
	}
	unsigned.KeyID = "<key>&\u2028"
	encoded, err = c.EncodeRequest(unsigned)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.DecodeRequest(encoded); err != nil {
		t.Fatal(err)
	}
	alternate := bytes.Replace(encoded, []byte(`\u003c`), []byte("<"), 1)
	if _, err := c.DecodeRequest(alternate); err == nil {
		t.Fatal("accepted alternate string escape")
	}
	unsigned.KeyID = string([]byte{'s', 0xff})
	if _, err := c.EncodeRequest(unsigned); err == nil {
		t.Fatal("lossy Go string accepted")
	}
}

func badRequestBytes(data []byte) map[string][]byte {
	replace := func(old, next string) []byte { return bytes.Replace(data, []byte(old), []byte(next), 1) }
	return map[string][]byte{
		"empty": nil, "null": []byte("null"), "trailing_space": append(slices.Clone(data), ' '),
		"trailing_object": append(slices.Clone(data), []byte("{}")...), "truncated": slices.Clone(data[:len(data)-1]),
		"duplicate_top":     replace(`"Version":1`, `"Version":1,"Version":1`),
		"duplicate_nested":  replace(`"PID":41`, `"PID":40,"PID":41`),
		"duplicate_escaped": replace(`"Version":1`, `"Version":1,"\u0056ersion":1`),
		"unknown_top":       replace(`"Version":1`, `"Untrusted":true,"Version":1`),
		"unknown_nested":    replace(`"PID":41`, `"PID":41,"Shell":"canary-secret"`),
		"case_alias":        replace(`"Version"`, `"version"`), "missing": replace(`"Step":2,`, ""),
		"reordered": replace(`"Version":1,"Op":13`, `"Op":13,"Version":1`),
		"version":   replace(`"Version":1`, `"Version":2`),
		"op":        replace(`"Op":13`, `"Op":19`), "component": replace(`"Component":2`, `"Component":4`),
		"artifact":        replace(`"Kind":2`, `"Kind":5`),
		"negative":        replace(`"Sequence":4`, `"Sequence":-1`),
		"overflow":        replace(`"Sequence":4`, `"Sequence":18446744073709551616`),
		"uint16_overflow": replace(`"Version":1`, `"Version":65537`),
		"int32_overflow":  replace(`"PID":41`, `"PID":2147483648`),
		"float":           replace(`"Sequence":4`, `"Sequence":4.0`), "quoted_integer": replace(`"Sequence":4`, `"Sequence":"4"`),
		"null_scalar": replace(`"Step":2`, `"Step":null`), "null_struct": replace(`"Expected":{`, `"Expected":null,"Discarded":{`),
		"short_array":        replace(`"Tx":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]`, `"Tx":[1]`),
		"long_array":         replace(`"Tx":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]`, `"Tx":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]`),
		"invalid_utf8":       replace(`"KeyID":"key"`, "\"KeyID\":\"k\xff\""),
		"unpaired_surrogate": replace(`"KeyID":"key"`, `"KeyID":"\ud800"`),
		"empty_slice_alias":  bytes.Replace([]byte(requestGolden), []byte(`"Signature":null`), []byte(`"Signature":""`), 1),
	}
}

type opGolden struct {
	op                                    c.HostOp
	owner, components, required, optional string
}

var opGoldens = []opGolden{
	{1, "P02", "123", "", "Expected ExpectedJournal"},
	{2, "P04", "123", "Candidate ExpectedJournal Payload", "Expected Store Config"},
	{3, "P05", "12", "Expected ExpectedJournal BeforeCensus", "Candidate Store Config Payload"},
	{4, "P04", "12", "Expected Candidate ExpectedJournal Config Payload", "Store BeforeCensus"},
	{5, "P02", "1", "Expected ExpectedJournal BeforeCensus Payload", ""},
	{6, "P02", "1", "Expected ExpectedJournal BeforeCensus Payload", ""},
	{7, "P02", "2", "Expected ExpectedJournal BeforeCensus Payload", ""},
	{8, "P05", "2", "Expected Candidate ExpectedJournal Config Payload", "Store BeforeCensus"},
	{9, "P07", "2", "Expected ExpectedJournal BeforeCensus WarmCensus Payload", "ReturnProof"},
	{10, "P09", "2", "Expected ExpectedJournal BeforeCensus Payload", "Store Config"},
	{11, "P05", "12", "Expected Candidate ExpectedJournal Config Payload", "Store BeforeCensus"},
	{12, "P05", "2", "Expected ExpectedJournal WarmCensus ReturnProof", "BeforeCensus Payload"},
	{13, "P04", "123", "Expected ExpectedJournal Payload", "Candidate Store Config BeforeCensus WarmCensus ReturnProof"},
	{14, "P03", "3", "ExpectedJournal Payload", ""},
	{15, "P04", "3", "Expected Candidate ExpectedJournal Payload", "BeforeCensus ReturnProof"},
	{16, "P06", "12", "Expected Candidate ExpectedJournal Config Payload", "Store"},
	{17, "P06", "12", "Expected Candidate ExpectedJournal BeforeCensus Payload", "Store Config"},
	{18, "P40", "2", "Expected BeforeCensus Payload", ""},
}

func checkHostOperations(t *testing.T) {
	rules := c.HostOpDescriptors()
	if len(rules) != 18 {
		t.Fatal("host op table is incomplete")
	}
	for i, want := range opGoldens {
		t.Run(strconv.Itoa(int(want.op)), func(t *testing.T) {
			got := rules[i]
			components := ""
			for _, component := range got.Components {
				components += strconv.Itoa(int(component))
			}
			if got.Op != want.op || got.Owner != want.owner || components != want.components || strings.Join(got.Required, " ") != want.required || strings.Join(got.Optional, " ") != want.optional {
				t.Fatal("host op contract changed")
			}
			required, optional := strings.Fields(want.required), strings.Fields(want.optional)
			for component := c.Component(0); component < 5; component++ {
				r := requestFixture()
				r.Op = want.op
				r.Component = component
				if component == c.NATS {
					r.Candidate.Kind = c.NATSBinary
				}
				if component == c.HostSupport {
					r.Candidate.Kind = c.HelperBinary
				}
				for _, name := range strings.Fields("Expected Candidate Store ExpectedJournal Config BeforeCensus WarmCensus ReturnProof Payload") {
					if !slices.Contains(required, name) {
						reflect.ValueOf(&r).Elem().FieldByName(name).SetZero()
					}
				}
				allowed := strings.Contains(want.components, strconv.Itoa(int(component)))
				if _, err := c.EncodeRequest(r); (err == nil) != allowed {
					t.Fatalf("component %d admission mismatch: %v", component, err)
				}
				if !allowed {
					continue
				}
				for _, name := range strings.Fields("Version Op Component Tx Step Domain Sequence PolicyRevision KeyID HostBootID Nonce RemainingNS " + want.required) {
					changed := r
					reflect.ValueOf(&changed).Elem().FieldByName(name).SetZero()
					if _, err := c.EncodeRequest(changed); err == nil {
						t.Errorf("missing required field %s accepted", name)
					}
				}
				for _, name := range strings.Fields("Expected Candidate Store ExpectedJournal Config BeforeCensus WarmCensus ReturnProof Payload") {
					if slices.Contains(required, name) {
						continue
					}
					changed := r
					populated := requestFixture()
					populated.Candidate.Kind = c.CaddyBinary
					if component == c.NATS {
						populated.Candidate.Kind = c.NATSBinary
					}
					if component == c.HostSupport {
						populated.Candidate.Kind = c.HelperBinary
					}
					reflect.ValueOf(&changed).Elem().FieldByName(name).Set(reflect.ValueOf(populated).FieldByName(name))
					_, err := c.EncodeRequest(changed)
					if (err == nil) != slices.Contains(optional, name) {
						t.Errorf("optional/forbidden field %s mismatch: %v", name, err)
					}
				}
			}
		})
	}
	rules[0].Components[0] = 255
	rules[1].Required[0] = "Untrusted"
	if c.HostOpDescriptors()[0].Components[0] != c.NATS || c.HostOpDescriptors()[1].Required[0] != "Candidate" {
		t.Fatal("operation policy is caller-mutable")
	}
}

func checkIdentityCompleteness(t *testing.T) {
	original := requestFixture()
	for _, name := range []string{"Expected", "Candidate", "Store"} {
		field := reflect.ValueOf(original).FieldByName(name)
		for i := 0; i < field.NumField(); i++ {
			changed := original
			reflect.ValueOf(&changed).Elem().FieldByName(name).Field(i).SetZero()
			if _, err := c.EncodeRequest(changed); err == nil {
				t.Errorf("partial %s.%s accepted", name, field.Type().Field(i).Name)
			}
		}
	}
	for _, mutate := range []func(*c.HostRequest){
		func(r *c.HostRequest) { r.Expected.PID = -1 },
		func(r *c.HostRequest) { r.Expected.BootID = "another-boot" },
		func(r *c.HostRequest) { r.Candidate.Kind = c.NATSBinary },
		func(r *c.HostRequest) { r.Op = 255 },
		func(r *c.HostRequest) { r.Component = 255 },
		func(r *c.HostRequest) { r.RemainingNS = math.MaxUint64 },
	} {
		changed := original
		mutate(&changed)
		if _, err := c.EncodeRequest(changed); err == nil {
			t.Fatal("invalid incarnation/enum/budget accepted")
		}
	}
}

func checkRequestBounds(t *testing.T) {
	r := requestFixture()
	data, err := c.EncodeRequest(r)
	if err != nil {
		t.Fatal(err)
	}
	r.KeyID = strings.Repeat("k", len(r.KeyID)+65536-len(data))
	data, err = c.EncodeRequest(r)
	if err != nil || len(data) != 65536 {
		t.Fatalf("exact request limit failed: %d %v", len(data), err)
	}
	if _, err := c.DecodeRequest(data); err != nil {
		t.Fatal(err)
	}
	r.KeyID += "k"
	if _, err := c.EncodeRequest(r); err == nil {
		t.Fatal("oversize encode accepted")
	}
	data = append(data, ' ')
	if _, err := c.DecodeRequest(data); err == nil {
		t.Fatal("oversize decode accepted")
	}
	r = requestFixture()
	r.Signature = make([]byte, 65537)
	if _, err := c.EncodeRequest(r); err == nil {
		t.Fatal("oversize signature accepted")
	}
}

func checkPrivateRecords(t *testing.T) {
	r := requestFixture()
	receipt := c.HostReceipt{Tx: r.Tx, Sequence: r.Sequence, Revision: 1, Op: r.Op, RequestDigest: c.Digest{1}, Journal: c.Digest{2}, Current: r.Expected, Census: c.Census{Generation: 1, Processes: []c.Process{r.Expected}, Sockets: []c.Socket{}, LiveBusiness: 4, OldOwnerPending: 1, Digest: c.Digest{3}}}
	data, err := c.EncodeReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := c.DecodeReceipt(data)
	if err != nil || decoded.Census.Sockets != nil || decoded.Census.LiveBusiness != 4 || decoded.Census.OldOwnerPending != 1 {
		t.Fatalf("receipt round trip: %v", err)
	}
	if receipt.Census.Sockets == nil {
		t.Fatal("encode changed caller slice")
	}
	for _, name := range strings.Fields("Tx Sequence Revision Op RequestDigest Journal") {
		bad := receipt
		reflect.ValueOf(&bad).Elem().FieldByName(name).SetZero()
		if _, err := c.EncodeReceipt(bad); err == nil {
			t.Errorf("receipt missing %s accepted", name)
		}
	}
	if _, err := c.DecodeRequest(data); err == nil {
		t.Fatal("receipt accepted as request")
	}
	if _, err := c.DecodeReceipt([]byte(requestGolden)); err == nil {
		t.Fatal("request accepted as receipt")
	}
	duplicate := bytes.Replace(data, []byte(`"Complete":false`), []byte(`"Complete":true,"Complete":false`), 1)
	if _, err := c.DecodeReceipt(duplicate); err == nil {
		t.Fatal("duplicate census field accepted")
	}
	for _, op := range []c.HostOp{0, 19, 255} {
		receipt.Op = op
		if _, err := c.EncodeReceipt(receipt); err == nil {
			t.Fatal("unknown receipt op accepted")
		}
	}
	socket := c.Socket{NetNS: 1, Device: 2, Inode: 3, Cookie: 4, Local: "local", Remote: "remote", Owner: r.Expected}
	old := c.Attachment{Participant: c.ID{9}, Generation: 1, ServerID: "server", CID: 11, URL: "wss://old", Raw: socket, Ingress: r.Expected, HealthGeneration: 1}
	proof := c.RouteProof{Purpose: c.RoutePrepare, Direction: c.RouteOutbound, Target: c.RouteTarget{URL: "wss://new", NodeID: "node", Component: c.Caddy, Ingress: r.Expected}, Version: 1, Tx: r.Tx, Participant: old.Participant, Old: old, HomeDigest: c.Digest{3}, AuthorityRevision: 1, PolicyRevision: 1, RemainingNS: 1000, Nonce: c.ID{4}, KeyID: "route-key"}
	data, err = c.EncodeRouteProof(proof)
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.DecodeRouteProof(data)
	if err != nil || !reflect.DeepEqual(got, proof) {
		t.Fatalf("route round trip: %v", err)
	}
	for _, name := range strings.Fields("Purpose Direction Target Version Tx Participant Old HomeDigest AuthorityRevision PolicyRevision RemainingNS Nonce KeyID") {
		bad := proof
		reflect.ValueOf(&bad).Elem().FieldByName(name).SetZero()
		if _, err := c.EncodeRouteProof(bad); err == nil {
			t.Errorf("route missing %s accepted", name)
		}
	}
	for _, name := range strings.Fields("URL NodeID Component Ingress") {
		bad := proof
		reflect.ValueOf(&bad.Target).Elem().FieldByName(name).SetZero()
		if _, err := c.EncodeRouteProof(bad); err == nil {
			t.Errorf("target missing %s accepted", name)
		}
	}
	// A not-yet-running N1 successor uses the other legitimate target form.
	future := proof
	future.Target.Ingress = c.Process{}
	future.Target.Before = r.Expected
	future.Target.Candidate = r.Candidate
	if _, err := c.EncodeRouteProof(future); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"Before", "Candidate"} {
		field := reflect.ValueOf(future.Target).FieldByName(name)
		for i := 0; i < field.NumField(); i++ {
			bad := future
			reflect.ValueOf(&bad.Target).Elem().FieldByName(name).Field(i).SetZero()
			if _, err := c.EncodeRouteProof(bad); err == nil {
				t.Errorf("partial route target %s.%s accepted", name, field.Type().Field(i).Name)
			}
		}
	}
	for i := 0; i < reflect.TypeFor[c.Attachment]().NumField(); i++ {
		bad := proof
		reflect.ValueOf(&bad.Old).Elem().Field(i).SetZero()
		if _, err := c.EncodeRouteProof(bad); err == nil {
			t.Errorf("partial old attachment field %d accepted", i)
		}
	}
	for _, mutate := range []func(*c.RouteProof){
		func(p *c.RouteProof) { p.RemainingNS = math.MaxUint64 },
		func(p *c.RouteProof) { p.Version = 2 },
		func(p *c.RouteProof) { p.Signature = []byte{1} },
		func(p *c.RouteProof) { p.Participant = c.ID{10} },
	} {
		bad := proof
		mutate(&bad)
		if _, err := c.EncodeRouteProof(bad); err == nil {
			t.Fatal("invalid route envelope accepted")
		}
	}
	if _, err := c.DecodeRequest(data); err == nil {
		t.Fatal("route accepted as request")
	}
	if _, err := c.DecodeRouteProof([]byte(requestGolden)); err == nil {
		t.Fatal("request accepted as route")
	}
	bad := proof
	bad.New = old
	if _, err := c.EncodeRouteProof(bad); err == nil {
		t.Fatal("prepare invented future attachment")
	}
	proof.Purpose = c.RouteBind
	proof.New = old
	proof.New.Generation = 2
	proof.New.Raw.Cookie = 5
	proof.New.CID = 12
	if _, err := c.EncodeRouteProof(proof); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*c.RouteProof){
		func(p *c.RouteProof) { p.Purpose = 3 }, func(p *c.RouteProof) { p.Direction = 3 },
		func(p *c.RouteProof) { p.Target.Component = c.HostSupport }, func(p *c.RouteProof) { p.New.Raw = p.Old.Raw },
		func(p *c.RouteProof) { p.New.Generation = p.Old.Generation }, func(p *c.RouteProof) { p.New = c.Attachment{} },
	} {
		bad = proof
		mutate(&bad)
		if _, err := c.EncodeRouteProof(bad); err == nil {
			t.Fatal("invalid route proof accepted")
		}
	}
}

func checkPlanObservation(t *testing.T) {
	r := requestFixture()
	plan := c.Plan{Tx: r.Tx, Step: r.Step, Domain: r.Domain, Component: c.Caddy, Action: c.Rollback, Candidate: r.Candidate, Previous: r.Candidate, Before: r.Expected, ConfigSHA256: r.Config, Store: r.Store, PolicyRevision: 1}
	// Same SHA is legal on rollback; it cannot be used as a global old-owner key.
	if err := c.ValidatePlan(plan); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < reflect.TypeFor[c.Process]().NumField(); i++ {
		bad := plan
		reflect.ValueOf(&bad.Before).Elem().Field(i).SetZero()
		if c.ValidatePlan(bad) == nil {
			t.Errorf("partial plan process field %d accepted", i)
		}
	}
	for _, name := range strings.Fields("Tx Step Domain Candidate Previous Before ConfigSHA256 PolicyRevision") {
		bad := plan
		reflect.ValueOf(&bad).Elem().FieldByName(name).SetZero()
		if c.ValidatePlan(bad) == nil {
			t.Errorf("plan missing %s accepted", name)
		}
	}
	// Isolate the public-component guard from artifact-kind validation: these
	// are valid support artifacts, but HostSupport can never be a public Plan.
	support := plan
	support.Component = c.HostSupport
	support.Candidate.Kind = c.HelperBinary
	support.Previous.Kind = c.LauncherBinary
	if c.ValidatePlan(support) == nil {
		t.Fatal("public plan accepted support artifacts")
	}
	standalone := plan
	standalone.Component = c.NATS
	standalone.Candidate.Kind = c.NATSBinary
	standalone.Previous.Kind = c.NATSBinary
	if err := c.ValidatePlan(standalone); err != nil {
		t.Fatal(err)
	}
	standalone.Store = c.Store{}
	if c.ValidatePlan(standalone) == nil {
		t.Fatal("NATS plan omitted store identity")
	}
	plan.Store.Device = 0
	if c.ValidatePlan(plan) == nil {
		t.Fatal("partial plan store accepted")
	}
	plan.Store = r.Store
	for _, component := range []c.Component{0, c.HostSupport, 255} {
		bad := plan
		bad.Component = component
		if c.ValidatePlan(bad) == nil {
			t.Fatal("nonpublic component admitted")
		}
	}
	for _, action := range []c.Action{0, 5, 255} {
		bad := plan
		bad.Action = action
		if c.ValidatePlan(bad) == nil {
			t.Fatal("unknown action admitted")
		}
	}
	o := c.Observation{Version: 1, Tx: r.Tx, Step: r.Step, Kind: "old_exit", BootID: r.HostBootID, BootNS: 1, Subject: c.ID{7}, Generation: 1, Evidence: c.Digest{8}}
	for _, kind := range c.ObservationKinds() {
		o.Kind = kind
		if err := c.ValidateObservation(o); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < reflect.TypeFor[c.Observation]().NumField(); i++ {
		bad := o
		reflect.ValueOf(&bad).Elem().Field(i).SetZero()
		if c.ValidateObservation(bad) == nil {
			t.Errorf("empty observation field %d accepted", i)
		}
	}
	o.Kind = "files_installed"
	if c.ValidateObservation(o) == nil {
		t.Fatal("status-only observation accepted")
	}
}

// Deterministic seeds exercise our operation and canonicalization rules, not
// just encoding/json's parser. Fuzz success implies a unique stable encoding;
// every rejected input must return an entirely zero request, never partial data.
// origin: hot-update S9 plan §2.2 / §12 P00 / T02 / M01
func FuzzInfraRequest(f *testing.F) {
	r := signedFixture(f)
	good, err := c.EncodeRequest(r)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(good)
	f.Add([]byte(requestGolden))
	bad := badRequestBytes(good)
	keys := make([]string, 0, len(bad))
	for key := range bad {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		f.Add(bad[key])
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		got, err := c.DecodeRequest(data)
		if err != nil {
			if !reflect.ValueOf(got).IsZero() {
				t.Fatal("failed decode returned partial request")
			}
			return
		}
		canonical, err := c.EncodeRequest(got)
		if err != nil || !bytes.Equal(data, canonical) {
			t.Fatal("accepted spelling is not canonical")
		}
		signing, err := c.SigningBytes(got)
		if err != nil || !bytes.HasPrefix(signing, []byte("tether-infra-host-v1\n")) {
			t.Fatal("invalid signature preimage")
		}
		got.Signature = nil
		unsigned, err := c.EncodeRequest(got)
		if err != nil || !bytes.Equal(signing[len("tether-infra-host-v1\n"):], unsigned) {
			t.Fatal("signing omitted fields")
		}
	})
}

// Literal version-one bytes, independently specified in frozen field order.
const requestGolden = `{"Version":1,"Op":13,"Component":2,"Tx":[1,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Step":2,"Domain":[3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Sequence":4,"PolicyRevision":5,"KeyID":"key","HostBootID":"boot","Nonce":[6,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Expected":{"BootID":"boot","PID":41,"StartTicks":42,"InvocationID":"invocation","ExeDevice":43,"ExeInode":44,"ExeSHA256":[45,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Cgroup":"/unit"},"Candidate":{"Kind":2,"Version":"2.7.6","GOOS":"linux","GOARCH":"amd64","ArchiveSHA256":[46,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"BinarySHA256":[47,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"CatalogSHA256":[48,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]},"Store":{"Device":1,"Inode":2,"MountID":3,"LockDevice":4,"LockInode":5},"ExpectedJournal":[50,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Config":[51,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"BeforeCensus":[52,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"WarmCensus":[53,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"ReturnProof":[54,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"Payload":[55,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0],"RemainingNS":123456,"Signature":null}`
