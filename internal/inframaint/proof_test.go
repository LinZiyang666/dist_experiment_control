// These L1 tests run the real canonical codec, signatures and route owner.
// Source supplies explicit observations; no NATS, reaper, systemd or kernel
// observer is simulated as an integration receipt. P30 owns that validation.
package inframaint

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/brokermaint"
	"github.com/LinZiyang666/tether/internal/infracontract"
	"github.com/LinZiyang666/tether/internal/upgradebudget"
)

type routeSource struct {
	mu     sync.Mutex
	states []State
	err    error
	onRead func()
}

func (s *routeSource) Read(infracontract.Plan, infracontract.Checkpoint, infracontract.RouteDirection, infracontract.Deadline) ([]State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.onRead != nil {
		s.onRead()
	}
	return append([]State(nil), s.states...), s.err
}

type routeFixture struct {
	clock      atomic.Int64
	start      int64
	key        ed25519.PrivateKey
	pub        ed25519.PublicKey
	source     *routeSource
	routes     *RouteSigner
	plan       infracontract.Plan
	checkpoint infracontract.Checkpoint
	state      State
	new        infracontract.Attachment
	deadline   infracontract.Deadline
}

func newRouteFixture(t *testing.T) *routeFixture {
	t.Helper()
	f := &routeFixture{start: int64(100 * time.Second)}
	f.clock.Store(f.start)
	f.key = ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x51}, ed25519.SeedSize))
	f.pub = f.key.Public().(ed25519.PublicKey)
	f.plan = infracontract.Plan{Tx: infracontract.ID{1}, Step: 1, Domain: infracontract.ID{2}, Component: infracontract.Caddy, Action: infracontract.Upgrade, Before: testProcess(3), Candidate: testArtifact(infracontract.CaddyBinary), PolicyRevision: 18}
	f.checkpoint = infracontract.Checkpoint{Cohort: infracontract.ID{4}, Assets: infracontract.AssetSeal{Complete: true}, Proof: infracontract.Digest{5}}
	old := infracontract.Attachment{Participant: infracontract.ID{6}, Generation: 7, ServerID: "server-a", CID: 8, URL: "wss://main.example:443", Raw: infracontract.Socket{NetNS: 9, Device: 10, Inode: 11, Cookie: 12, Local: "127.0.0.1:45000", Remote: "127.0.0.1:443", Owner: testProcess(13)}, Ingress: f.plan.Before, HealthGeneration: 14}
	f.state = State{Domain: f.plan.Domain, Attachment: old, Target: infracontract.RouteTarget{URL: "wss://main.example:8443", NodeID: "broker-a", Component: infracontract.Caddy, Ingress: testProcess(15)}, HomeDigest: infracontract.Digest{16}, AuthorityRevision: 17, PolicyRevision: 18, Nonce: infracontract.ID{19}, Healthy: true, Registered: true, Stable: true}
	f.new = old
	f.new.Generation++
	f.new.CID++
	f.new.URL = f.state.Target.URL
	f.new.Raw.Inode++
	f.new.Raw.Cookie++
	f.new.Raw.Local = "127.0.0.1:45001"
	f.new.Raw.Remote = "127.0.0.1:8443"
	f.new.Ingress = f.state.Target.Ingress
	f.new.HealthGeneration++
	f.source = &routeSource{states: []State{f.state}}
	var err error
	f.routes, err = NewRoutes(Config{Domain: f.plan.Domain, KeyID: "route-key", Sign: func(b []byte) ([]byte, error) { return ed25519.Sign(f.key, b), nil }, Source: f.source, Clock: func() (int64, error) { return f.clock.Load(), nil }})
	if err != nil {
		t.Fatal(err)
	}
	f.deadline = infracontract.Deadline(f.start + int64(upgradebudget.ComponentHandover))
	return f
}

func testProcess(n int32) infracontract.Process {
	return infracontract.Process{BootID: "test-boot", PID: n, StartTicks: uint64(n) + 100, InvocationID: "invocation", ExeDevice: 20, ExeInode: uint64(n) + 200, ExeSHA256: infracontract.Digest{byte(n)}, Cgroup: "/test.slice/service"}
}

func testArtifact(kind infracontract.ArtifactKind) infracontract.Artifact {
	return infracontract.Artifact{Kind: kind, Version: "test-version", GOOS: "linux", GOARCH: "amd64", ArchiveSHA256: infracontract.Digest{21}, BinarySHA256: infracontract.Digest{22}, CatalogSHA256: infracontract.Digest{23}}
}

func (f *routeFixture) prepare(t *testing.T) infracontract.RouteProof {
	t.Helper()
	proofs, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline)
	if err != nil || len(proofs) != 1 {
		t.Fatalf("prepare: count=%d error=%v", len(proofs), err)
	}
	return proofs[0]
}

func (f *routeFixture) observeNew() infracontract.MoveResult {
	f.source.mu.Lock()
	f.source.states[0].Attachment = f.new
	f.source.mu.Unlock()
	return infracontract.MoveResult{Census: infracontract.Census{Complete: true, Generation: 24, Digest: infracontract.Digest{25}, LiveBusiness: 1}, Attachments: []infracontract.Attachment{f.new}}
}

func (f *routeFixture) sign(t *testing.T, p infracontract.RouteProof) infracontract.RouteProof {
	t.Helper()
	result, err := SignProof(p, f.plan.Domain, func(data []byte) ([]byte, error) { return ed25519.Sign(f.key, data), nil })
	if err != nil {
		t.Fatal(err)
	}
	return result
}

// Every scalar leaf is changed independently, including the physical owner,
// socket, home and budget fields. The signature bytes themselves are separate.
func eachLeaf(v reflect.Value, path string, fn func(string, []int), index []int) {
	if v.Kind() == reflect.Struct {
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).Name == "Signature" {
				continue
			}
			eachLeaf(v.Field(i), path+"/"+v.Type().Field(i).Name, fn, append(append([]int(nil), index...), i))
		}
		return
	}
	fn(path, index)
}

func changeLeaf(v reflect.Value) {
	switch v.Kind() {
	case reflect.String:
		v.SetString(v.String() + "-changed")
	case reflect.Int, reflect.Int32:
		v.SetInt(v.Int() + 1)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		v.SetUint(v.Uint() + 1)
	case reflect.Array:
		v.Index(0).SetUint(v.Index(0).Uint() + 1)
	default:
		panic("unhandled test field")
	}
}

// origin: hot-update S9 plan §4.6 / T36 / M18
func TestInfraRouteProofBindsBothGenerations(t *testing.T) {
	t.Run("every_signed_field", func(t *testing.T) {
		f := newRouteFixture(t)
		p := f.prepare(t)
		moved := f.observeNew()
		bound, err := f.routes.Bind(f.plan, moved, infracontract.RouteOutbound, f.deadline)
		if err != nil {
			t.Fatal(err)
		}
		for _, proof := range []infracontract.RouteProof{p, bound[0]} {
			if err := VerifyProof(proof, f.plan.Domain, "route-key", f.pub); err != nil {
				t.Fatal(err)
			}
			eachLeaf(reflect.ValueOf(proof), "proof", func(name string, path []int) {
				t.Run(name+"/"+string(rune('0'+proof.Purpose)), func(t *testing.T) {
					changed := proof
					changeLeaf(reflect.ValueOf(&changed).Elem().FieldByIndex(path))
					if VerifyProof(changed, f.plan.Domain, "route-key", f.pub) == nil {
						t.Fatal("unsigned field alteration accepted")
					}
				})
			}, nil)
			wrong := f.plan.Domain
			wrong[0]++
			if VerifyProof(proof, wrong, "route-key", f.pub) == nil || VerifyProof(proof, f.plan.Domain, "other-key", f.pub) == nil {
				t.Fatal("domain/key substitution accepted")
			}
			data, err := infracontract.EncodeRouteProof(proof)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := infracontract.DecodeRouteProof(data)
			if err != nil || VerifyProof(decoded, f.plan.Domain, "route-key", f.pub) != nil {
				t.Fatal("canonical transport round trip failed")
			}
		}
		// A real S6 canonical proof's signature is valid under the same key,
		// but its signing bytes cannot authorize either S9 route purpose.
		old := brokermaint.Proof{CutID: "old-cut", Nonce: "old-nonce", IssuerBootID: "test-boot", ResponseBootNS: 1, ExpiresBootNS: 2, RecordDigest: strings.Repeat("ab", 32), AppliedIndex: 1}
		data, err := brokermaint.CanonicalProofBytes(old)
		if err != nil {
			t.Fatal(err)
		}
		for _, proof := range []infracontract.RouteProof{p, bound[0]} {
			proof.Signature = ed25519.Sign(f.key, data)
			if VerifyProof(proof, f.plan.Domain, "route-key", f.pub) == nil {
				t.Fatal("S6 proof reused as S9 authorization")
			}
		}
		data, _ = SigningBytes(p, f.plan.Domain)
		if !bytes.HasPrefix(data, append([]byte("infra-route-v1\n"), f.plan.Domain[:]...)) {
			t.Fatal("separate S9 domain missing")
		}
	})
	t.Run("baseline_covers_full_home", func(t *testing.T) {
		base := Baseline{SessionID: "sid", NodeID: "nid", InstanceID: "instance", TokenHash: infracontract.Digest{1}, Home: "broker-a", Epoch: 2}
		digest, err := HomeDigest(base)
		if err != nil {
			t.Fatal(err)
		}
		eachLeaf(reflect.ValueOf(base), "baseline", func(_ string, index []int) {
			changed := base
			changeLeaf(reflect.ValueOf(&changed).Elem().FieldByIndex(index))
			got, err := HomeDigest(changed)
			if err != nil || got == digest {
				t.Fatal("home identity omitted from digest")
			}
		}, nil)
		base.Home = ""
		if _, err := HomeDigest(base); err != nil {
			t.Fatal("single home rejected")
		}
		base.TokenHash = infracontract.Digest{}
		if _, err := HomeDigest(base); err == nil {
			t.Fatal("unknown token accepted")
		}
	})
	t.Run("prepare_never_invents_future_attachment", func(t *testing.T) {
		f := newRouteFixture(t)
		p := f.prepare(t)
		p.New = f.new
		if _, err := SigningBytes(p, f.plan.Domain); err == nil {
			t.Fatal("future CID accepted in Prepare")
		}
	})
	t.Run("bind_requires_actual_source", func(t *testing.T) {
		f := newRouteFixture(t)
		f.prepare(t)
		moved := f.observeNew()
		eachLeaf(reflect.ValueOf(f.new), "attachment", func(name string, index []int) {
			t.Run(name, func(t *testing.T) {
				changed := moved
				changed.Attachments = append([]infracontract.Attachment(nil), moved.Attachments...)
				changeLeaf(reflect.ValueOf(&changed.Attachments[0]).Elem().FieldByIndex(index))
				if _, err := f.routes.Bind(f.plan, changed, infracontract.RouteOutbound, f.deadline); err == nil {
					t.Fatal("unobserved attachment signed")
				}
			})
		}, nil)
		bound, err := f.routes.Bind(f.plan, moved, infracontract.RouteOutbound, f.deadline)
		if err != nil {
			t.Fatal(err)
		}
		if bound[0].Old != f.state.Attachment || bound[0].New != f.new || bound[0].Nonce != f.state.Nonce {
			t.Fatal("Bind lost old/new identity")
		}
		bound[0].Signature[0] ^= 1
		retry, err := f.routes.Bind(f.plan, moved, infracontract.RouteOutbound, f.deadline)
		if err != nil || VerifyProof(retry[0], f.plan.Domain, "route-key", f.pub) != nil {
			t.Fatal("caller mutated stored signature")
		}
	})
	t.Run("source_and_cohort_fail_closed", func(t *testing.T) {
		cases := []struct {
			name string
			edit func(*routeFixture, *infracontract.MoveResult)
		}{
			{"home_CAS", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.states[0].HomeDigest[0]++ }},
			{"authority", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.states[0].AuthorityRevision++ }},
			{"policy", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.states[0].PolicyRevision++ }},
			{"unknown_domain", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.states[0].Domain[0]++ }},
			{"missing_REGISTER", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.states[0].Registered = false }},
			{"unhealthy", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.states[0].Healthy = false }},
			{"nonce", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.states[0].Nonce[0]++ }},
			{"target", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.states[0].Target.NodeID = "other" }},
			{"incomplete", func(_ *routeFixture, m *infracontract.MoveResult) { m.Census.Complete = false }},
			{"empty_digest", func(_ *routeFixture, m *infracontract.MoveResult) { m.Census.Digest = infracontract.Digest{} }},
			{"empty_generation", func(_ *routeFixture, m *infracontract.MoveResult) { m.Census.Generation = 0 }},
			{"missing_data_NC", func(_ *routeFixture, m *infracontract.MoveResult) { m.Attachments = nil }},
			{"extra_NC", func(_ *routeFixture, m *infracontract.MoveResult) {
				m.Attachments = append(m.Attachments, m.Attachments[0])
			}},
			{"empty_source", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.states = nil }},
			{"duplicate_source", func(f *routeFixture, _ *infracontract.MoveResult) {
				f.source.states = append(f.source.states, f.source.states[0])
			}},
			{"source_error", func(f *routeFixture, _ *infracontract.MoveResult) { f.source.err = infracontract.ErrIncomplete }},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				f := newRouteFixture(t)
				f.prepare(t)
				m := f.observeNew()
				tc.edit(f, &m)
				if _, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline); err == nil {
					t.Fatal("incomplete or stale evidence signed")
				}
			})
		}
	})
	t.Run("physical_target_cannot_be_labels", func(t *testing.T) {
		cases := []struct {
			name string
			edit func(*routeFixture)
		}{
			{"same_raw", func(f *routeFixture) { f.new.Raw = f.state.Attachment.Raw; f.new.Raw.Remote = "changed label" }},
			{"same_cid", func(f *routeFixture) { f.new.CID = f.state.Attachment.CID }},
			{"wrong_URL", func(f *routeFixture) { f.new.URL = "wss://other.example" }},
			{"wrong_ingress", func(f *routeFixture) { f.new.Ingress.PID++ }},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				f := newRouteFixture(t)
				f.prepare(t)
				tc.edit(f)
				if _, err := f.routes.Bind(f.plan, f.observeNew(), infracontract.RouteOutbound, f.deadline); err == nil {
					t.Fatal("label-only or wrong-target move accepted")
				}
			})
		}
	})
	t.Run("future_N1_needs_runtime_and_auth", func(t *testing.T) {
		f := newRouteFixture(t)
		f.plan.Component = infracontract.NATS
		f.plan.Candidate = testArtifact(infracontract.NATSBinary)
		f.state.Target = infracontract.RouteTarget{URL: "nats://main.example:4222", NodeID: "broker-a", Component: infracontract.NATS, Before: f.plan.Before, Candidate: f.plan.Candidate}
		f.source.states[0] = f.state
		p := f.prepare(t)
		if p.New != (infracontract.Attachment{}) {
			t.Fatal("future N1 synthesized attachment")
		}
		f.new.URL = f.state.Target.URL
		f.new.Ingress = testProcess(26)
		f.new.Ingress.ExeSHA256 = f.plan.Candidate.BinarySHA256
		m := f.observeNew()
		f.source.states[0].Registered = false
		if _, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline); err == nil {
			t.Fatal("disk candidate signed without ordinary auth")
		}
		f.source.states[0].Registered = true
		f.source.states[0].Healthy = false
		if _, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline); err == nil {
			t.Fatal("unready future candidate signed")
		}
		f.source.states[0].Healthy = true
		if _, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("initial_state_and_constructor", func(t *testing.T) {
		if _, err := NewRoutes(Config{}); err == nil {
			t.Fatal("missing dependencies accepted")
		}
		for _, edit := range []func(*routeFixture){func(f *routeFixture) { f.source.states = nil }, func(f *routeFixture) { f.source.states = append(f.source.states, f.state) }, func(f *routeFixture) { f.source.states[0].Stable = false }, func(f *routeFixture) { f.source.states[0].Healthy = false }, func(f *routeFixture) { f.source.states[0].Registered = false }, func(f *routeFixture) { f.plan.Domain[0]++ }, func(f *routeFixture) { f.checkpoint.Assets.Complete = false }} {
			f := newRouteFixture(t)
			edit(f)
			if _, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline); err == nil {
				t.Fatal("invalid initial source accepted")
			}
		}
		f := newRouteFixture(t)
		if _, err := f.routes.Bind(f.plan, f.observeNew(), infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrTransition) {
			t.Fatal("Bind preceded Prepare")
		}
	})
	t.Run("proof_limits_and_signer_failure", func(t *testing.T) {
		f := newRouteFixture(t)
		p := f.prepare(t)
		for _, sign := range []Sign{nil, func([]byte) ([]byte, error) { return nil, errors.New("private signer canary") }, func([]byte) ([]byte, error) { return []byte{1}, nil }} {
			if _, err := SignProof(p, f.plan.Domain, sign); !errors.Is(err, ErrInvalid) {
				t.Fatal("invalid signer accepted or private error exposed")
			}
		}
		for _, remaining := range []uint64{0, uint64(upgradebudget.BrokerMaintenanceTTL) + 1, ^uint64(0)} {
			bad := p
			bad.RemainingNS = remaining
			if _, err := SigningBytes(bad, f.plan.Domain); err == nil {
				t.Fatal("invalid maintenance duration signed")
			}
		}
		bad := p
		bad.Signature = nil
		if VerifyProof(bad, f.plan.Domain, "route-key", f.pub) == nil || VerifyProof(p, f.plan.Domain, "route-key", nil) == nil {
			t.Fatal("missing signature/key accepted")
		}
	})
	t.Run("bound_retry_cannot_change_generation", func(t *testing.T) {
		f := newRouteFixture(t)
		f.prepare(t)
		m := f.observeNew()
		if _, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline); err != nil {
			t.Fatal(err)
		}
		f.new.Generation++
		f.new.CID++
		f.new.Raw.Inode++
		f.new.Raw.Cookie++
		if _, err := f.routes.Bind(f.plan, f.observeNew(), infracontract.RouteOutbound, f.deadline); !errors.Is(err, ErrTransition) {
			t.Fatal("Bind retry authorized another physical reconnection")
		}
	})
	t.Run("future_before_and_candidate_are_exact", func(t *testing.T) {
		f := newRouteFixture(t)
		f.state.Target.Ingress = infracontract.Process{}
		f.state.Target.Before = f.plan.Before
		f.state.Target.Candidate = f.plan.Candidate
		f.source.states[0] = f.state
		p := f.prepare(t)
		p.Target.Before.PID++
		if _, err := SigningBytes(p, f.plan.Domain); err == nil {
			t.Fatal("future target borrows another old invocation")
		}
		f.new.Ingress.ExeSHA256[0]++
		if _, err := f.routes.Bind(f.plan, f.observeNew(), infracontract.RouteOutbound, f.deadline); err == nil {
			t.Fatal("future runtime has different candidate bytes")
		}
	})
	t.Run("all_participants_bind_atomically", func(t *testing.T) {
		f := newRouteFixture(t)
		second := f.state
		second.Attachment.Participant[0]++
		second.Nonce[0]++
		f.source.states = append(f.source.states, second)
		prepared, err := f.routes.Prepare(f.plan, f.checkpoint, infracontract.RouteOutbound, f.deadline)
		if err != nil || len(prepared) != 2 {
			t.Fatal("two-participant prepare failed", err)
		}
		newSecond := f.new
		newSecond.Participant = second.Attachment.Participant
		newSecond.CID++
		newSecond.Raw.Cookie++
		f.source.states[0].Attachment = f.new
		f.source.states[1].Attachment = newSecond
		m := infracontract.MoveResult{Census: infracontract.Census{Complete: true, Generation: 1, Digest: infracontract.Digest{1}}, Attachments: []infracontract.Attachment{f.new, f.new}}
		if _, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline); err == nil {
			t.Fatal("duplicate attachment replaces missing data NC")
		}
		m.Attachments[1] = newSecond
		bound, err := f.routes.Bind(f.plan, m, infracontract.RouteOutbound, f.deadline)
		if err != nil || len(bound) != 2 {
			t.Fatal("rejected batch partially committed", err)
		}
	})
}
