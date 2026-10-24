package upgradecompat

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

// origin: hot-update S8 plan §6.1 / §9.1 / P06
func TestImageEpochBidirectionalReaders(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*Reader, *Reader, *Reader)
		want string
	}{
		{"same epoch", func(_, _, _ *Reader) {}, ""},
		{"adjacent both directions", func(a, b, c *Reader) {
			a.ReadEpochs = []uint64{1, 2}
			b.Epoch = 2
			b.ReadEpochs = []uint64{1, 2}
			c.ReadEpochs = []uint64{1, 2}
		}, ""},
		{"candidate reads source but source cannot read candidate", func(a, b, c *Reader) { b.Epoch = 2; b.ReadEpochs = []uint64{1, 2} }, "image_epoch_unreadable"},
		{"previous cannot read candidate", func(a, b, c *Reader) { a.ReadEpochs = []uint64{1, 2}; b.Epoch = 2; b.ReadEpochs = []uint64{1, 2} }, "image_epoch_unreadable"},
		{"candidate cannot read previous", func(a, b, c *Reader) { c.Epoch = 2; c.ReadEpochs = []uint64{1, 2}; a.ReadEpochs = []uint64{1, 2} }, "image_epoch_unreadable"},
		{"two hops even with advertised reader", func(a, b, c *Reader) {
			a.ReadEpochs = []uint64{1, 3}
			b.Epoch = 3
			b.ReadEpochs = []uint64{1, 3}
			c.ReadEpochs = []uint64{1, 3}
		}, "image_epoch_distance"},
		{"reverse two hops", func(a, b, c *Reader) {
			a.Epoch = 3
			a.ReadEpochs = []uint64{1, 3}
			b.ReadEpochs = []uint64{1, 3}
			c.ReadEpochs = []uint64{1, 3}
		}, "image_epoch_distance"},
		{"unknown source epoch", func(a, b, c *Reader) { a.Epoch = 0 }, "image_epoch_unknown"},
		{"unknown candidate epoch", func(a, b, c *Reader) { b.Epoch = 0 }, "image_epoch_unknown"},
		{"unknown previous epoch", func(a, b, c *Reader) { c.Epoch = 0 }, "image_epoch_unknown"},
		{"zero advertised epoch", func(a, b, c *Reader) { b.ReadEpochs = []uint64{0, 1} }, "image_epochs_invalid"},
		{"duplicate advertised epoch", func(a, b, c *Reader) { b.ReadEpochs = []uint64{1, 1} }, "image_epochs_invalid"},
		{"reader cannot read itself", func(a, b, c *Reader) { b.ReadEpochs = []uint64{2} }, "image_epoch_unreadable"},
		{"absent readers means explicit own epoch", func(a, b, c *Reader) { a.ReadEpochs = nil; b.ReadEpochs = nil; c.ReadEpochs = nil }, ""},
		{"absent readers never implies adjacency", func(a, b, c *Reader) {
			a.ReadEpochs = nil
			b.Epoch = 2
			b.ReadEpochs = []uint64{1, 2}
			c.ReadEpochs = []uint64{1, 2}
		}, "image_epoch_unreadable"},
		{"uint64 distance cannot wrap", func(a, b, c *Reader) {
			a.Epoch = ^uint64(0)
			a.ReadEpochs = []uint64{1, ^uint64(0)}
			b.ReadEpochs = []uint64{1, ^uint64(0)}
			c.ReadEpochs = []uint64{1, ^uint64(0)}
		}, "image_epoch_distance"},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, b, c := capableReader("a"), capableReader("b"), capableReader("c")
			test.edit(&a, &b, &c)
			assertRefusal(t, CheckReaders(a, b, c, []string{"exec-v1", "transfer-v1"}), test.want, upgradetx.CodeRejected)
		})
	}
	// Adjacent high values prove the distance check is not a signed conversion.
	a := Reader{Epoch: ^uint64(0), ReadEpochs: []uint64{^uint64(0), ^uint64(0) - 1}}
	b := Reader{Epoch: ^uint64(0) - 1, ReadEpochs: a.ReadEpochs}
	assertRefusal(t, CheckEpochs(a, b), "", "")
	assertRefusal(t, CheckEpochs(b, a), "", "")
}

// origin: hot-update S8 plan §6.1 / §9.1 / P06
func TestCompatibilityDeclarationsRequireEveryRetainedDomain(t *testing.T) {
	for _, slot := range []int{0, 1, 2} {
		for _, feature := range []string{"upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1"} {
			t.Run(feature+"/"+string(rune('a'+slot)), func(t *testing.T) {
				readers := []Reader{capableReader("a"), capableReader("b"), capableReader("c")}
				readers[slot].Features = without(readers[slot].Features, feature)
				assertRefusal(t, CheckReaders(readers[0], readers[1], readers[2], nil), "reader_capability_missing", upgradetx.CodeRejected)
			})
		}
		for _, domain := range []string{"upgrade-cut-v1", "exec-v1", "transfer-v1"} {
			t.Run(domain+"/"+string(rune('a'+slot)), func(t *testing.T) {
				readers := []Reader{capableReader("a"), capableReader("b"), capableReader("c")}
				readers[slot].Domains = without(readers[slot].Domains, domain)
				assertRefusal(t, CheckReaders(readers[0], readers[1], readers[2], []string{"exec-v1", "transfer-v1"}), "reader_domain_missing", upgradetx.CodeRejected)
			})
		}
	}
	for _, test := range []struct {
		name string
		edit func(*Reader)
		want string
	}{
		{"unknown SHA", func(r *Reader) { r.SHA256 = "" }, "reader_identity_invalid"},
		{"uppercase SHA", func(r *Reader) { r.SHA256 = strings.Repeat("A", 64) }, "reader_identity_invalid"},
		{"nonhex SHA", func(r *Reader) { r.SHA256 = strings.Repeat("z", 64) }, "reader_identity_invalid"},
		{"unknown release", func(r *Reader) { r.Release = "" }, "reader_identity_invalid"},
		{"different role", func(r *Reader) { r.Component = upgradetx.ComponentBroker }, "reader_role_mismatch"},
		{"reserved component", func(r *Reader) { r.Component = upgradetx.ComponentNATS }, "reader_role_mismatch"},
		{"duplicate domain", func(r *Reader) { r.Domains = append(r.Domains, "exec-v1") }, "reader_declarations_invalid"},
		{"empty feature", func(r *Reader) { r.Features = append(r.Features, "") }, "reader_declarations_invalid"},
	} {
		t.Run(test.name, func(t *testing.T) {
			a, b, c := capableReader("a"), capableReader("b"), capableReader("c")
			test.edit(&b)
			assertRefusal(t, CheckReaders(a, b, c, nil), test.want, upgradetx.CodeRejected)
		})
	}
	for _, required := range [][]string{{""}, {"exec-v1", "exec-v1"}} {
		a, b, c := capableReader("a"), capableReader("b"), capableReader("c")
		assertRefusal(t, CheckReaders(a, b, c, required), "required_domains_invalid", upgradetx.CodeRejected)
	}
}

// origin: hot-update S8 plan §6.1 / §7.1 / P06
func TestUpgradePreflightRequiresFreshEvidence(t *testing.T) {
	for _, stage := range []upgradetx.ActionKind{upgradetx.ActionSelect, upgradetx.ActionFreeze, upgradetx.ActionDispatch} {
		t.Run(string(stage), func(t *testing.T) {
			p := validPreflight(stage)
			assertRefusal(t, CheckPreflight(p), "", "")
			// Expiry revokes an unused grant, not a durable accepted attempt's
			// recovery authority. Its resumed observations are still fresh.
			p.Started = true
			p.Now = p.Grant.ExpiresAt + 1
			p.Stamp.At = p.Now
			p.Fleet.Stamp, p.Census.Stamp = p.Stamp, p.Stamp
			for _, name := range RequiredChecks() {
				p.Checks[name] = Evidence{Stamp: p.Stamp, Stage: stage, Known: true, Complete: true}
			}
			assertRefusal(t, CheckPreflight(p), "", "")
			for _, name := range RequiredChecks() {
				for _, defect := range []string{"missing", "unknown", "incomplete", "old nonce", "old sequence", "wrong cut", "wrong owner", "other stage"} {
					t.Run(name+"/"+defect, func(t *testing.T) {
						p := validPreflight(stage)
						proof := p.Checks[name]
						want, code := "preflight_evidence_stale", upgradetx.CodeRejected
						switch defect {
						case "missing":
							delete(p.Checks, name)
							want, code = "preflight_evidence_incomplete", upgradetx.CodeDeferred
						case "unknown":
							proof.Known = false
							want, code = "preflight_evidence_incomplete", upgradetx.CodeDeferred
						case "incomplete":
							proof.Complete = false
							want, code = "preflight_evidence_incomplete", upgradetx.CodeDeferred
						case "old nonce":
							proof.Stamp.Nonce = strings.Repeat("f", 32)
						case "old sequence":
							proof.Stamp.Sequence--
						case "wrong cut":
							proof.Stamp.Attempt.CutID = strings.Repeat("f", 32)
						case "wrong owner":
							proof.Stamp.Producer.StartTicks++
						case "other stage":
							proof.Stage = upgradetx.ActionActivate
						}
						if defect != "missing" {
							p.Checks[name] = proof
						}
						assertRefusal(t, CheckPreflight(p), want, code)
					})
				}
			}
		})
	}
	for _, test := range []struct {
		name string
		edit func(*Preflight)
		want string
		code string
	}{
		{"omitted mode", func(p *Preflight) { p.Grant.Mode = "" }, "hot_mode_required", upgradetx.CodeRejected},
		{"interrupting is non hot", func(p *Preflight) { p.Grant.Mode = upgradetx.ModeLegacyInterrupting }, "hot_mode_required", upgradetx.CodeRejected},
		{"wrong gate", func(p *Preflight) { p.Stage = upgradetx.ActionActivate }, "preflight_stage_invalid", upgradetx.CodeRejected},
		{"infrastructure reserved", func(p *Preflight) { p.Grant.Handle.Key.Component = upgradetx.ComponentCaddy }, "preflight_component_unsupported", upgradetx.CodeUnsupportedComponent},
		{"expiry is exclusive", func(p *Preflight) { p.Now = p.Grant.ExpiresAt }, "preflight_grant_expired", upgradetx.CodeGrantExpired},
		{"unknown time", func(p *Preflight) { p.Now = 0 }, "preflight_grant_expired", upgradetx.CodeGrantExpired},
		{"unknown sample time", func(p *Preflight) { p.Stamp.At = 0 }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"future sample", func(p *Preflight) { p.Stamp.At = p.Now + 1 }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"unknown sequence", func(p *Preflight) { p.Stamp.Sequence = 0 }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"unknown generation", func(p *Preflight) { p.Stamp.Attempt.Generation = 0 }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"unknown revision", func(p *Preflight) { p.Stamp.Attempt.Revision = 0 }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"malformed transaction", func(p *Preflight) { p.Stamp.Attempt.TxID = "other" }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"malformed cut", func(p *Preflight) { p.Stamp.Attempt.CutID = "other" }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"malformed nonce", func(p *Preflight) { p.Stamp.Nonce = "nonce" }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"wrong scope", func(p *Preflight) { p.Stamp.ScopeDigest = strings.Repeat("f", 64) }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"unknown owner boot", func(p *Preflight) { p.Stamp.Producer.BootID = "" }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"PID alone is not owner", func(p *Preflight) { p.Stamp.Producer.StartTicks = 0 }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"unknown owner PID", func(p *Preflight) { p.Stamp.Producer.PID = 0 }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"unknown owner instance", func(p *Preflight) { p.Stamp.Producer.InstanceID = "" }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"valid but different attempt", func(p *Preflight) { p.Stamp.Attempt.TxID = strings.Repeat("f", 32) }, "preflight_identity_mismatch", upgradetx.CodeRejected},
		{"source has wrong component", func(p *Preflight) { p.Source.Component = upgradetx.ComponentBroker }, "preflight_artifact_mismatch", upgradetx.CodeRejected},
		{"wrong source bytes", func(p *Preflight) { p.Source.SHA256 = strings.Repeat("d", 64) }, "preflight_artifact_mismatch", upgradetx.CodeRejected},
		{"wrong candidate bytes", func(p *Preflight) { p.Candidate.SHA256 = strings.Repeat("d", 64) }, "preflight_artifact_mismatch", upgradetx.CodeRejected},
		{"wrong recorded previous", func(p *Preflight) { p.RecordedPreviousSHA = strings.Repeat("d", 64) }, "preflight_artifact_mismatch", upgradetx.CodeRejected},
		{"source release mismatch", func(p *Preflight) { p.Grant.SourceRelease = "other" }, "preflight_artifact_mismatch", upgradetx.CodeRejected},
		{"target release mismatch", func(p *Preflight) { p.Grant.TargetRelease = "other" }, "preflight_artifact_mismatch", upgradetx.CodeRejected},
		{"unexpected evidence", func(p *Preflight) { p.Checks["unrecognized"] = p.Checks["execution"] }, "preflight_evidence_unexpected", upgradetx.CodeRejected},
		{"stale fleet", func(p *Preflight) { p.Fleet.Stamp.Sequence-- }, "preflight_census_stale", upgradetx.CodeRejected},
		{"stale resources", func(p *Preflight) { p.Census.Stamp.Sequence-- }, "preflight_census_stale", upgradetx.CodeRejected},
		{"wrong original resource digest", func(p *Preflight) { p.Census.ExpectedDigest = strings.Repeat("f", 64) }, "preflight_census_stale", upgradetx.CodeRejected},
		{"unknown original resource digest", func(p *Preflight) { p.ExpectedDigest = "" }, "preflight_census_stale", upgradetx.CodeRejected},
		{"target generation differs from grant", func(p *Preflight) { p.TargetGeneration = 9 }, "preflight_generation_mismatch", upgradetx.CodeRejected},
		{"lost reservation", func(p *Preflight) { p.Fleet.Reserved = false }, "fleet_census_incomplete", upgradetx.CodeDeferred},
		{"new admission after earlier gate", func(p *Preflight) { p.Census.Admitting = 1 }, "generation_census_incomplete", upgradetx.CodeDeferred},
		{"late required domain", func(p *Preflight) { p.RequiredDomains = append(p.RequiredDomains, "late-domain-v1") }, "reader_domain_missing", upgradetx.CodeRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := validPreflight(upgradetx.ActionDispatch)
			test.edit(&p)
			assertRefusal(t, CheckPreflight(p), test.want, test.code)
		})
	}
}

// origin: hot-update S8 plan §7.2 / P06
func TestLegacyUpgradePolicy(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*LegacyPolicy)
		want string
		code string
	}{
		{"explicit busy migration stays non hot", func(p *LegacyPolicy) { p.Busy = true }, "", ""},
		{"omitted flag busy", func(p *LegacyPolicy) { p.Mode = ""; p.Busy = true }, "legacy_busy", upgradetx.CodeRejected},
		{"explicit hot busy", func(p *LegacyPolicy) { p.Mode = upgradetx.ModeHot; p.Busy = true }, "legacy_busy", upgradetx.CodeRejected},
		{"idle has no fence", func(p *LegacyPolicy) { p.Mode = ""; p.CensusKnown = true; p.Quiescent = true }, "legacy_quiescence_unproved", upgradetx.CodeDeferred},
		{"unknown census", func(p *LegacyPolicy) { p.Mode = ""; p.Quiescent = true; p.FenceHeld = true }, "legacy_quiescence_unproved", upgradetx.CodeDeferred},
		{"not quiescent", func(p *LegacyPolicy) { p.Mode = ""; p.CensusKnown = true; p.FenceHeld = true }, "legacy_quiescence_unproved", upgradetx.CodeDeferred},
		{"proven fenced idle", func(p *LegacyPolicy) { p.Mode = ""; p.CensusKnown = true; p.Quiescent = true; p.FenceHeld = true }, "", ""},
		{"capable ordinary upgrade", func(p *LegacyPolicy) { p.Mode = ""; p.SourceCapable = true }, "", ""},
		{"capable cannot target legacy", func(p *LegacyPolicy) { p.Mode = ""; p.SourceCapable = true; p.TargetCapable = false }, "capable_to_legacy", upgradetx.CodeRejected},
		{"interrupting cannot bypass downgrade", func(p *LegacyPolicy) { p.SourceCapable = true; p.TargetCapable = false }, "capable_to_legacy", upgradetx.CodeRejected},
		{"interrupting requires legacy source", func(p *LegacyPolicy) { p.SourceCapable = true }, "legacy_source_required", upgradetx.CodeRejected},
		{"broker migration is explicit cold", func(p *LegacyPolicy) { p.Component = upgradetx.ComponentBroker }, "legacy_target_unsupported", upgradetx.CodeRejected},
		{"migration requires capable candidate", func(p *LegacyPolicy) { p.TargetCapable = false }, "legacy_target_unsupported", upgradetx.CodeRejected},
		{"unknown consumers", func(p *LegacyPolicy) { p.ConsumersKnown = false }, "legacy_consumers_unknown", upgradetx.CodeRejected},
		{"incomplete consumers", func(p *LegacyPolicy) { p.ConsumersComplete = false }, "legacy_consumers_unknown", upgradetx.CodeRejected},
		{"no consumers", func(p *LegacyPolicy) { p.Consumers = nil }, "legacy_consumers_unknown", upgradetx.CodeRejected},
		{"no consumer snapshot", func(p *LegacyPolicy) { p.ExpectedConsumers = nil }, "legacy_consumers_unknown", upgradetx.CodeRejected},
		{"omitted consumer", func(p *LegacyPolicy) { p.Consumers = p.Consumers[:1] }, "legacy_consumers_unknown", upgradetx.CodeRejected},
		{"extra consumer", func(p *LegacyPolicy) { p.Consumers = append(p.Consumers, LegacyConsumer{ID: "extra", Capable: true}) }, "legacy_consumer_unsupported", upgradetx.CodeRejected},
		{"duplicate snapshot", func(p *LegacyPolicy) { p.ExpectedConsumers[1] = p.ExpectedConsumers[0] }, "legacy_consumer_unsupported", upgradetx.CodeRejected},
		{"duplicate snapshot with otherwise complete consumers", func(p *LegacyPolicy) { p.ExpectedConsumers = append(p.ExpectedConsumers, "broker-b") }, "legacy_consumer_unsupported", upgradetx.CodeRejected},
		{"mixed consumer", func(p *LegacyPolicy) { p.Consumers[1].Capable = false }, "legacy_consumer_unsupported", upgradetx.CodeRejected},
		{"empty consumer identity", func(p *LegacyPolicy) { p.Consumers[1].ID = "" }, "legacy_consumer_unsupported", upgradetx.CodeRejected},
		{"duplicate consumer", func(p *LegacyPolicy) { p.Consumers[1].ID = p.Consumers[0].ID }, "legacy_consumer_unsupported", upgradetx.CodeRejected},
		{"route changed after probe", func(p *LegacyPolicy) { p.RouteIssuer = "broker-b" }, "legacy_issuer_route_mismatch", upgradetx.CodeRejected},
		{"unknown issuer", func(p *LegacyPolicy) { p.Issuer = ""; p.RouteIssuer = "" }, "legacy_issuer_route_mismatch", upgradetx.CodeRejected},
		{"issuer outside consumer roster", func(p *LegacyPolicy) { p.Issuer = "other"; p.RouteIssuer = "other" }, "legacy_issuer_route_mismatch", upgradetx.CodeRejected},
		{"unknown mode", func(p *LegacyPolicy) { p.Mode = "unknown" }, "legacy_mode_invalid", upgradetx.CodeRejected},
		{"infrastructure reserved", func(p *LegacyPolicy) { p.Component = upgradetx.ComponentNATS }, "legacy_component_unsupported", upgradetx.CodeUnsupportedComponent},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := LegacyPolicy{Component: upgradetx.ComponentAgent, Mode: upgradetx.ModeLegacyInterrupting, TargetCapable: true, ConsumersKnown: true, ConsumersComplete: true, ExpectedConsumers: []string{"broker-a", "broker-b"}, Consumers: []LegacyConsumer{{ID: "broker-a", Capable: true}, {ID: "broker-b", Capable: true}}, Issuer: "broker-a", RouteIssuer: "broker-a"}
			test.edit(&p)
			assertRefusal(t, CheckLegacy(p), test.want, test.code)
		})
	}
}

// origin: hot-update S8 plan §6 / §11.2 / P06
func TestCompatibilityChecksPreserveCallerEvidence(t *testing.T) {
	p := validPreflight(upgradetx.ActionDispatch)
	before := validPreflight(upgradetx.ActionDispatch)
	assertRefusal(t, CheckPreflight(p), "", "")
	if !reflect.DeepEqual(p, before) {
		t.Fatal("preflight changed caller evidence")
	}
	want := []string{"artifact_provenance", "manifest_schema", "decoder_closure", "configuration", "authority_writers", "execution", "descriptors", "capacity", "state_directory"}
	if !reflect.DeepEqual(RequiredChecks(), want) {
		t.Fatal("required proof set does not cover all preflight obligations")
	}
	names := RequiredChecks()
	names[0] = "weakened"
	if RequiredChecks()[0] != "artifact_provenance" {
		t.Fatal("required checks share mutable caller storage")
	}
	// A broker is subject to the same hot reader rules, including the adjunct.
	p = validPreflight(upgradetx.ActionDispatch)
	p.Grant.Handle.Key.Component = upgradetx.ComponentBroker
	p.Stamp.Attempt = p.Grant.Handle
	p.Source.Component, p.Candidate.Component, p.Previous.Component = upgradetx.ComponentBroker, upgradetx.ComponentBroker, upgradetx.ComponentBroker
	p.Fleet.Stamp, p.Census.Stamp = p.Stamp, p.Stamp
	for _, name := range RequiredChecks() {
		p.Checks[name] = Evidence{Stamp: p.Stamp, Stage: p.Stage, Known: true, Complete: true}
	}
	assertRefusal(t, CheckPreflight(p), "", "")
}

func capableReader(digit string) Reader {
	return Reader{SHA256: strings.Repeat(digit, 64), Release: "release-" + digit, Component: upgradetx.ComponentAgent, Epoch: 1, ReadEpochs: []uint64{1}, Domains: []string{"exec-v1", "transfer-v1", "upgrade-cut-v1"}, Features: []string{"upgrade-tx-v1", "authority-incarnation-v1", "upgrade-cut-v1"}}
}

func validPreflight(stage upgradetx.ActionKind) Preflight {
	handle := upgradetx.Handle{Key: upgradetx.Key{DomainID: "domain", Component: upgradetx.ComponentAgent, SID: "session", ConfiguredNID: "agent"}, TxID: strings.Repeat("a", 32), CutID: strings.Repeat("b", 32), Generation: 8, Revision: 3}
	stamp := upgradetx.Stamp{Attempt: handle, Producer: upgradetx.Owner{BootID: "boot", PID: 123, StartTicks: 456, InstanceID: "instance"}, Nonce: strings.Repeat("c", 32), Sequence: 2, At: 99, ScopeDigest: strings.Repeat("d", 64)}
	p := Preflight{Grant: upgradetx.Grant{Handle: handle, Mode: upgradetx.ModeHot, SourceSHA: strings.Repeat("a", 64), TargetSHA: strings.Repeat("b", 64), SourceRelease: "release-a", TargetRelease: "release-b", ScopeDigest: stamp.ScopeDigest, ExpiresAt: 200}, Stamp: stamp, Stage: stage, Now: 100, Source: capableReader("a"), Candidate: capableReader("b"), Previous: capableReader("a"), RecordedPreviousSHA: strings.Repeat("a", 64), RequiredDomains: []string{"exec-v1", "transfer-v1"}, Checks: make(map[string]Evidence), Fleet: validFleet(), Census: upgradetx.Census{Stamp: stamp, Known: true, Complete: true, ExpectedDigest: strings.Repeat("e", 64)}, ExpectedDigest: strings.Repeat("e", 64), SourceGeneration: 7, TargetGeneration: 8}
	p.Fleet.Stamp = stamp
	for _, name := range RequiredChecks() {
		p.Checks[name] = Evidence{Stamp: stamp, Stage: stage, Known: true, Complete: true}
	}
	return p
}

func assertRefusal(t *testing.T, err error, reason, code string) {
	t.Helper()
	if reason == "" {
		if err != nil {
			t.Fatalf("expected admission, got %v", err)
		}
		return
	}
	var refusal *upgradetx.Error
	if !errors.As(err, &refusal) || refusal.Code != code || refusal.Detail != reason || refusal.Accepted {
		t.Fatalf("expected unaccepted %s/%s, got %v", code, reason, err)
	}
}

func without(values []string, omitted string) []string {
	var result []string
	for _, value := range values {
		if value != omitted {
			result = append(result, value)
		}
	}
	return result
}
