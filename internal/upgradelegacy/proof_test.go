package upgradelegacy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/LinZiyang666/tether/internal/upgradetx"
)

func legacyProofFixture() upgradetx.LegacyUpgradeProof {
	return upgradetx.LegacyUpgradeProof{
		ParentTxID: strings.Repeat("1", 32),
		Issuer:     upgradetx.Owner{BootID: "issuer-boot", PID: 123, StartTicks: 456, InstanceID: "issuer-instance"},
		GrantNonce: strings.Repeat("2", 32), TargetInstance: "original-target",
		TargetKey: upgradetx.Key{DomainID: "canonical-domain", Component: upgradetx.ComponentAgent,
			SID: "original-session", ConfiguredNID: "wait", UID: 1000},
		SourceSHA: strings.Repeat("a", 64), CandidateSHA: strings.Repeat("b", 64),
		DispatchActionID: strings.Repeat("3", 32), State: "attempted",
	}
}

// origin: hot-update S8 plan §3.2 / §7.2 causal vs time-window claim / P12
func TestLegacyProofRequiresCausalDispatchReceipt(t *testing.T) {
	proof := legacyProofFixture()
	proof.State, proof.ObservedSHA, proof.ObservedNonce = "observed", proof.CandidateSHA, strings.Repeat("4", 32)
	// A nonempty old marker ID with the right SHA is still just an observation.
	proof.LegacyUpgradeID = "another-upgrade-in-the-same-time-window"
	data, err := EncodeProof(proof)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeProof(data)
	if err != nil || decoded.Causal {
		t.Fatalf("uncorrelated observation claimed dispatch: %+v %v", decoded, err)
	}
	proof.Causal = true
	if _, err := EncodeProof(proof); err != nil {
		t.Fatalf("complete causal evidence rejected: %v", err)
	}
	for _, test := range []struct {
		name string
		edit func(*upgradetx.LegacyUpgradeProof)
	}{
		{"no_upgrade_id", func(p *upgradetx.LegacyUpgradeProof) { p.LegacyUpgradeID = "" }},
		{"no_nonce", func(p *upgradetx.LegacyUpgradeProof) { p.ObservedNonce = "" }},
		{"old_running_image", func(p *upgradetx.LegacyUpgradeProof) { p.ObservedSHA = p.SourceSHA }},
		{"unknown", func(p *upgradetx.LegacyUpgradeProof) { p.State = "unknown" }},
		{"intent", func(p *upgradetx.LegacyUpgradeProof) { p.State = "attempted" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := proof
			test.edit(&invalid)
			_, err := EncodeProof(invalid)
			requireLegacyError(t, err, upgradetx.CodeStateCorrupt, false)
		})
	}
	proxy, journal, _, transport, grant := legacyFixture(t)
	transport.causal = true
	observed, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	if !observed.Causal || observed.State != "observed" {
		t.Fatalf("causal fact was lost: %+v", observed)
	}
	disk, err := journal.read()
	if err != nil {
		t.Fatal(err)
	}
	if disk.Record.Forward.Outcome == upgradetx.OutcomeSuccess || disk.Record.Forward.Phase == upgradetx.PhaseCompleted ||
		disk.Record.Forward.Control.Complete || disk.Record.Forward.Resources.Complete || disk.Record.Forward.Released.Complete {
		t.Fatalf("legacy receipt signed hot completion: %+v", disk.Record.Forward)
	}
	if disk.Record.Actions[0].State != upgradetx.ActionDurable {
		t.Fatal("causal observation was not durably recorded")
	}
}

// origin: hot-update S8 plan §2.2 / §3.1 strict disk proof / P12
func TestLegacyProofCodecRejectsAmbiguousState(t *testing.T) {
	proof := legacyProofFixture()
	encoded, err := EncodeProof(proof)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeProof(encoded)
	if err != nil || decoded != proof {
		t.Fatalf("proof round trip = %+v %v", decoded, err)
	}
	for _, invalid := range []struct {
		name string
		edit func(*upgradetx.LegacyUpgradeProof)
	}{
		{"parent_id", func(p *upgradetx.LegacyUpgradeProof) { p.ParentTxID = "" }},
		{"grant_nonce", func(p *upgradetx.LegacyUpgradeProof) { p.GrantNonce = strings.Repeat("A", 32) }},
		{"action_id", func(p *upgradetx.LegacyUpgradeProof) { p.DispatchActionID = "" }},
		{"owner_boot", func(p *upgradetx.LegacyUpgradeProof) { p.Issuer.BootID = "bad\nboot" }},
		{"owner_pid", func(p *upgradetx.LegacyUpgradeProof) { p.Issuer.PID = 0 }},
		{"owner_ticks", func(p *upgradetx.LegacyUpgradeProof) { p.Issuer.StartTicks = 0 }},
		{"owner_instance", func(p *upgradetx.LegacyUpgradeProof) { p.Issuer.InstanceID = "" }},
		{"component", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.Component = upgradetx.ComponentBroker }},
		{"domain", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.DomainID = "" }},
		{"sid", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.SID = "" }},
		{"nid", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.ConfiguredNID = "" }},
		{"config", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.ConfigPath = "bad\npath" }},
		{"instance", func(p *upgradetx.LegacyUpgradeProof) { p.TargetInstance = "" }},
		{"padded_instance", func(p *upgradetx.LegacyUpgradeProof) { p.TargetInstance = " padded " }},
		{"oversized_instance", func(p *upgradetx.LegacyUpgradeProof) { p.TargetInstance = strings.Repeat("x", 129) }},
		{"invalid_utf8", func(p *upgradetx.LegacyUpgradeProof) { p.TargetInstance = string([]byte{0xff}) }},
		{"source", func(p *upgradetx.LegacyUpgradeProof) { p.SourceSHA = "" }},
		{"candidate", func(p *upgradetx.LegacyUpgradeProof) { p.CandidateSHA = "" }},
		{"same_sha", func(p *upgradetx.LegacyUpgradeProof) { p.CandidateSHA = p.SourceSHA }},
		{"oversized_legacy_id", func(p *upgradetx.LegacyUpgradeProof) {
			p.State = "observed"
			p.LegacyUpgradeID = strings.Repeat("x", 129)
		}},
		{"bad_observed_sha", func(p *upgradetx.LegacyUpgradeProof) { p.State = "observed"; p.ObservedSHA = "not-a-sha" }},
		{"bad_observed_nonce", func(p *upgradetx.LegacyUpgradeProof) { p.State = "observed"; p.ObservedNonce = "not-a-nonce" }},
		{"unknown_state", func(p *upgradetx.LegacyUpgradeProof) { p.State = "completed" }},
		{"intent_observation", func(p *upgradetx.LegacyUpgradeProof) { p.ObservedSHA = p.CandidateSHA }},
	} {
		t.Run(invalid.name, func(t *testing.T) {
			changed := proof
			invalid.edit(&changed)
			_, err := EncodeProof(changed)
			requireLegacyError(t, err, upgradetx.CodeStateCorrupt, false)
		})
	}
	tests := map[string][]byte{
		"empty":             nil,
		"null":              []byte("null"),
		"scalar":            []byte("123"),
		"array":             []byte("[]"),
		"truncated":         encoded[:len(encoded)-1],
		"trailing_value":    append(append([]byte(nil), encoded...), []byte(" {}")...),
		"trailing_garbage":  append(append([]byte(nil), encoded...), 'x'),
		"unknown":           bytes.Replace(encoded, []byte(`"state":`), []byte(`"secret":"must-not-appear-in-errors","state":`), 1),
		"duplicate":         bytes.Replace(encoded, []byte(`"state":`), []byte(`"state":"unknown","state":`), 1),
		"escaped_duplicate": bytes.Replace(encoded, []byte(`"state":`), []byte(`"st\u0061te":"unknown","state":`), 1),
		"nested_duplicate":  bytes.Replace(encoded, []byte(`"pid":`), []byte(`"pid":999,"pid":`), 1),
		"key_duplicate":     bytes.Replace(encoded, []byte(`"uid":`), []byte(`"uid":999,"uid":`), 1),
		"nested_unknown":    bytes.Replace(encoded, []byte(`"pid":`), []byte(`"fabricated":true,"pid":`), 1),
		"case_alias":        bytes.Replace(encoded, []byte(`"state":`), []byte(`"State":`), 1),
		"case_duplicate":    bytes.Replace(encoded, []byte(`"state":`), []byte(`"State":"unknown","state":`), 1),
		"oversized":         bytes.Repeat([]byte(" "), maxProofBytes+1),
		"oversized_valid":   append(append([]byte(nil), encoded...), bytes.Repeat([]byte(" "), maxProofBytes-len(encoded)+1)...),
		"deep":              []byte(strings.Repeat("[", 20) + "0" + strings.Repeat("]", 20)),
	}
	for name, data := range tests {
		t.Run(name, func(t *testing.T) {
			decoded, err := DecodeProof(data)
			requireLegacyError(t, err, upgradetx.CodeStateCorrupt, false)
			if decoded != (upgradetx.LegacyUpgradeProof{}) {
				t.Fatalf("invalid JSON returned a usable proof: %+v", decoded)
			}
			if strings.Contains(err.Error(), "must-not-appear-in-errors") {
				t.Fatal("decoder exposed untrusted payload")
			}
		})
	}
	deep := json.NewDecoder(strings.NewReader(strings.Repeat("[", 20) + "0" + strings.Repeat("]", 20)))
	if err := uniqueValue(deep, 0); err == nil {
		t.Fatal("deep JSON exceeded the parser work bound")
	}
}

// origin: hot-update S8 plan §2 / §7.2 immutable dispatch binding / P12
func TestLegacyProofBindsEveryDispatchIdentity(t *testing.T) {
	proof := legacyProofFixture()
	digest, err := proofDigest(proof)
	if err != nil || !validHex(digest, 64) {
		t.Fatalf("intent digest = %q %v", digest, err)
	}
	for _, test := range []struct {
		name string
		edit func(*upgradetx.LegacyUpgradeProof)
	}{
		{"parent", func(p *upgradetx.LegacyUpgradeProof) { p.ParentTxID = strings.Repeat("5", 32) }},
		{"boot", func(p *upgradetx.LegacyUpgradeProof) { p.Issuer.BootID = "another-boot" }},
		{"pid", func(p *upgradetx.LegacyUpgradeProof) { p.Issuer.PID++ }},
		{"ticks", func(p *upgradetx.LegacyUpgradeProof) { p.Issuer.StartTicks++ }},
		{"issuer_instance", func(p *upgradetx.LegacyUpgradeProof) { p.Issuer.InstanceID = "another-issuer" }},
		{"grant_nonce", func(p *upgradetx.LegacyUpgradeProof) { p.GrantNonce = strings.Repeat("5", 32) }},
		{"target_instance", func(p *upgradetx.LegacyUpgradeProof) { p.TargetInstance = "another-target" }},
		{"domain", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.DomainID = "another-domain" }},
		{"sid", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.SID = "another-session" }},
		{"nid", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.ConfiguredNID = "rollback" }},
		{"config", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.ConfigPath = "/other-config" }},
		{"uid", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.UID++ }},
		{"source", func(p *upgradetx.LegacyUpgradeProof) { p.SourceSHA = strings.Repeat("e", 64) }},
		{"candidate", func(p *upgradetx.LegacyUpgradeProof) { p.CandidateSHA = strings.Repeat("e", 64) }},
		{"action", func(p *upgradetx.LegacyUpgradeProof) { p.DispatchActionID = strings.Repeat("5", 32) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			changed := proof
			test.edit(&changed)
			changedDigest, err := proofDigest(changed)
			if err != nil || changedDigest == digest {
				t.Fatalf("binding %s missing from digest: %q %v", test.name, changedDigest, err)
			}
			changed.State, changed.ObservedNonce = "observed", strings.Repeat("6", 32)
			if err := validateObservation(proof, changed, changed.ObservedNonce); err == nil {
				t.Fatal("changed dispatch binding accepted")
			}
		})
	}
}

// origin: hot-update S8 plan §7.2 fresh nonce and non-causal image / P12
func TestLegacyQueryRejectsUnboundObservation(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*upgradetx.LegacyUpgradeProof)
	}{
		{"issuer", func(p *upgradetx.LegacyUpgradeProof) { p.Issuer.PID++ }},
		{"parent", func(p *upgradetx.LegacyUpgradeProof) { p.ParentTxID = strings.Repeat("8", 32) }},
		{"target", func(p *upgradetx.LegacyUpgradeProof) { p.TargetInstance = "other-instance" }},
		{"key", func(p *upgradetx.LegacyUpgradeProof) { p.TargetKey.UID++ }},
		{"action", func(p *upgradetx.LegacyUpgradeProof) { p.DispatchActionID = strings.Repeat("8", 32) }},
		{"source", func(p *upgradetx.LegacyUpgradeProof) { p.SourceSHA = strings.Repeat("e", 64) }},
		{"candidate", func(p *upgradetx.LegacyUpgradeProof) { p.CandidateSHA = strings.Repeat("e", 64) }},
		{"nonce", func(p *upgradetx.LegacyUpgradeProof) { p.ObservedNonce = strings.Repeat("8", 32) }},
		{"empty_nonce", func(p *upgradetx.LegacyUpgradeProof) { p.ObservedNonce = "" }},
		{"completed", func(p *upgradetx.LegacyUpgradeProof) { p.State = "completed" }},
		{"causal_missing_id", func(p *upgradetx.LegacyUpgradeProof) { p.Causal = true; p.LegacyUpgradeID = "" }},
		{"causal_old_image", func(p *upgradetx.LegacyUpgradeProof) { p.Causal = true; p.ObservedSHA = p.SourceSHA }},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy, journal, _, transport, grant := legacyFixture(t)
			transport.lostReply = true
			_, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
			requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
			before, err := os.ReadFile(journal.path)
			if err != nil {
				t.Fatal(err)
			}
			transport.alter = test.edit
			_, err = proxy.Query(t.Context(), grant, "legacy-instance", strings.Repeat("7", 32))
			requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
			after, readErr := os.ReadFile(journal.path)
			if readErr != nil || !bytes.Equal(before, after) {
				t.Fatalf("unbound observation rewrote journal: %v", readErr)
			}
			if transport.sends != 1 {
				t.Fatalf("unbound query resent: sends=%d", transport.sends)
			}
		})
	}
}

// origin: hot-update S8 plan §3.1 reopen validation / P12
func TestLegacyJournalRevalidatesRecordedProof(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*legacyDisk)
	}{
		{"version", func(d *legacyDisk) { d.Record.Version++ }},
		{"key", func(d *legacyDisk) { d.Record.Key.UID++ }},
		{"attempt", func(d *legacyDisk) { d.Record.Forward.Handle.CutID = strings.Repeat("9", 32) }},
		{"revision", func(d *legacyDisk) { d.Record.Forward.Handle.Revision++ }},
		{"direction", func(d *legacyDisk) { d.Record.Forward.Direction = upgradetx.DirectionReverse }},
		{"record_source", func(d *legacyDisk) { d.Record.Forward.SourceSHA = strings.Repeat("e", 64) }},
		{"record_target", func(d *legacyDisk) { d.Record.Forward.TargetSHA = strings.Repeat("e", 64) }},
		{"source", func(d *legacyDisk) { d.Proof.SourceSHA = strings.Repeat("e", 64) }},
		{"target", func(d *legacyDisk) { d.Proof.TargetInstance = "another-instance" }},
		{"proof_missing", func(d *legacyDisk) { d.Proof = upgradetx.LegacyUpgradeProof{} }},
		{"action_missing", func(d *legacyDisk) { d.Record.Actions = nil }},
		{"action_duplicate", func(d *legacyDisk) { d.Record.Actions = append(d.Record.Actions, d.Record.Actions[0]) }},
		{"intent_digest", func(d *legacyDisk) { d.Record.Actions[0].IntentDigest = strings.Repeat("e", 64) }},
		{"action_id", func(d *legacyDisk) { d.Record.Actions[0].ID = strings.Repeat("9", 32) }},
		{"action_attempt", func(d *legacyDisk) { d.Record.Actions[0].Handle.Generation++ }},
		{"action_revision", func(d *legacyDisk) { d.Record.Actions[0].Handle.Revision += 100 }},
		{"action_nonce", func(d *legacyDisk) { d.Record.Actions[0].Nonce = "" }},
		{"action_state", func(d *legacyDisk) { d.Record.Actions[0].State = "completed" }},
		{"false_durable", func(d *legacyDisk) { d.Record.Actions[0].State = upgradetx.ActionDurable }},
		{"invented_result", func(d *legacyDisk) { d.Record.Actions[0].ResultDigest = strings.Repeat("e", 64) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy, journal, policy, transport, grant := legacyFixture(t)
			transport.lostReply = true
			_, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
			requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
			disk, err := journal.read()
			if err != nil {
				t.Fatal(err)
			}
			test.edit(&disk)
			if err := journal.write(disk); err != nil {
				t.Fatal(err)
			}
			proxy = New(&legacyJournal{path: journal.path}, policy, transport)
			_, err = proxy.Dispatch(t.Context(), grant, "legacy-instance")
			if err == nil || transport.sends != 1 || transport.queries != 0 {
				t.Fatalf("invalid journal was used: sends=%d queries=%d err=%v", transport.sends, transport.queries, err)
			}
		})
	}
}

// origin: hot-update S8 plan §3.1 / §7.1 accepted error projection / P12
func TestLegacyJournalErrorsNeverInviteNewTransaction(t *testing.T) {
	for _, test := range []struct {
		name     string
		err      error
		code     string
		accepted bool
	}{
		{"io", errors.New("private-payload"), upgradetx.CodeUnsettled, true},
		{"retryable", &upgradetx.Error{Code: upgradetx.CodeDeferred}, upgradetx.CodeUnsettled, true},
		{"corrupt", &upgradetx.Error{Code: upgradetx.CodeStateCorrupt}, upgradetx.CodeUnsettled, true},
		{"accepted_unknown", &upgradetx.Error{Code: upgradetx.CodeGrantUnknown, Accepted: true}, upgradetx.CodeUnsettled, true},
		{"unknown", &upgradetx.Error{Code: upgradetx.CodeGrantUnknown}, upgradetx.CodeGrantUnknown, false},
		{"unwired", &upgradetx.Error{Code: upgradetx.CodeNotWired}, upgradetx.CodeNotWired, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy, journal, _, transport, grant := legacyFixture(t)
			journal.loadErr = test.err
			_, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
			requireLegacyError(t, err, test.code, test.accepted)
			if transport.sends != 0 || transport.queries != 0 || strings.Contains(err.Error(), "private-payload") {
				t.Fatalf("load error was forwarded or disclosed: %v", err)
			}
		})
	}
}

// origin: hot-update S8 plan §3.2 / §7.1 journal receipt cannot sign hot completion / P12
func TestLegacyJournalCommitCannotChangeExecutionHistory(t *testing.T) {
	for _, test := range []struct {
		name string
		edit func(*upgradetx.Record)
	}{
		{"no_action", func(r *upgradetx.Record) { r.Actions = nil }},
		{"changed_action", func(r *upgradetx.Record) { r.Actions[0].Nonce = strings.Repeat("f", 32) }},
		{"duplicate", func(r *upgradetx.Record) { r.Actions = append(r.Actions, r.Actions[0]) }},
		{"grant", func(r *upgradetx.Record) { r.Grant.ActorFP = "another-actor" }},
		{"revision", func(r *upgradetx.Record) { r.Revision--; r.Forward.Handle.Revision-- }},
		{"history", func(r *upgradetx.Record) {
			r.Forward.Phase = upgradetx.PhaseCompleted
			r.Forward.Outcome = upgradetx.OutcomeSuccess
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy, journal, _, transport, grant := legacyFixture(t)
			journal.alterReceipt = test.edit
			_, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
			requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
			if transport.sends != 0 {
				t.Fatalf("false commit receipt authorized a send: %d", transport.sends)
			}
		})
	}
}

// origin: hot-update S8 plan §3.1 fixed grant deadline / P12
func TestLegacyPreflightRejectsExpiredExecutionWindow(t *testing.T) {
	for _, test := range []struct {
		name     string
		deadline time.Time
		code     string
	}{
		{"expired", time.Now().Add(-time.Minute), upgradetx.CodeGrantExpired},
		{"extended", time.Now().Add(time.Hour), upgradetx.CodeRejected},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy, _, policy, transport, grant := legacyFixture(t)
			grant.ExpiresAt = test.deadline.UnixNano()
			policy.authorization.Grant = grant
			requireLegacyError(t, proxy.Preflight(t.Context(), grant, "legacy-instance"), test.code, false)
			if policy.called != 0 || transport.sends != 0 {
				t.Fatal("invalid execution window acquired authority")
			}
		})
	}
	proxy, _, policy, _, grant := legacyFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	requireLegacyError(t, proxy.Preflight(ctx, grant, "legacy-instance"), upgradetx.CodeDeferred, false)
	if policy.called != 0 {
		t.Fatal("cancelled preflight acquired authority")
	}
}

// origin: hot-update S8 plan §2.1 / §7.2 explicit interrupting authorization / P12
func TestLegacyGrantRequiresExplicitBoundIdentity(t *testing.T) {
	_, _, _, _, grant := legacyFixture(t)
	if err := validateGrant(grant, "legacy-instance"); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		edit func(*upgradetx.Grant)
	}{
		{"mode", func(g *upgradetx.Grant) { g.Mode = upgradetx.ModeHot }},
		{"key", func(g *upgradetx.Grant) { g.Handle.Key.Component = upgradetx.ComponentBroker }},
		{"tx", func(g *upgradetx.Grant) { g.Handle.TxID = "" }},
		{"cut", func(g *upgradetx.Grant) { g.Handle.CutID = "" }},
		{"generation", func(g *upgradetx.Grant) { g.Handle.Generation = 0 }},
		{"revision", func(g *upgradetx.Grant) { g.Handle.Revision = 0 }},
		{"parent", func(g *upgradetx.Grant) { g.ParentTxID = "" }},
		{"slot", func(g *upgradetx.Grant) { g.ParentSlot = "" }},
		{"nonce", func(g *upgradetx.Grant) { g.ClientNonce = "" }},
		{"issuer", func(g *upgradetx.Grant) { g.Issuer = upgradetx.Owner{} }},
		{"actor", func(g *upgradetx.Grant) { g.ActorFP = "" }},
		{"source_release", func(g *upgradetx.Grant) { g.SourceRelease = "" }},
		{"target_release", func(g *upgradetx.Grant) { g.TargetRelease = "" }},
		{"source", func(g *upgradetx.Grant) { g.SourceSHA = "" }},
		{"target", func(g *upgradetx.Grant) { g.TargetSHA = "" }},
		{"same_sha", func(g *upgradetx.Grant) { g.TargetSHA = g.SourceSHA }},
		{"scope", func(g *upgradetx.Grant) { g.ScopeDigest = "" }},
		{"caps", func(g *upgradetx.Grant) { g.CapabilityDigest = "" }},
		{"expiry", func(g *upgradetx.Grant) { g.ExpiresAt = 0 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := grant
			test.edit(&invalid)
			requireLegacyError(t, validateGrant(invalid, "legacy-instance"), upgradetx.CodeRejected, false)
		})
	}
	requireLegacyError(t, validateGrant(grant, ""), upgradetx.CodeRejected, false)
}

// origin: hot-update S8 plan §7.1 historical proof and fresh read / P12
func TestLegacyQueryPreservesRecordedCausalIdentity(t *testing.T) {
	proxy, journal, _, transport, grant := legacyFixture(t)
	transport.causal = true
	_, err := proxy.Dispatch(t.Context(), grant, "legacy-instance")
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	before, err := journal.read()
	if err != nil {
		t.Fatal(err)
	}
	transport.alter = func(p *upgradetx.LegacyUpgradeProof) { p.LegacyUpgradeID = "different-old-upgrade" }
	_, err = proxy.Query(t.Context(), grant, "legacy-instance", strings.Repeat("9", 32))
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	after, err := journal.read()
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("causal identity overwritten: %+v %v", after, err)
	}
	transport.alter, transport.causal = nil, false
	_, err = proxy.Query(t.Context(), grant, "legacy-instance", strings.Repeat("9", 32))
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	transport.queryErr = true
	proof, err := proxy.Query(t.Context(), grant, "legacy-instance", strings.Repeat("9", 32))
	requireLegacyError(t, err, upgradetx.CodeUnsettled, true)
	if !proof.Causal || proof.LegacyUpgradeID != before.Proof.LegacyUpgradeID {
		t.Fatal("observer unavailability erased recorded causal fact")
	}
	_, err = proxy.Query(t.Context(), grant, "legacy-instance", "")
	requireLegacyError(t, err, upgradetx.CodeRejected, false)
}
