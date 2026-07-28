package natsconf

import (
	"strings"
	"testing"
)

// jetstream_enablement_test.go — Ownership.HasJetStream must answer "is JetStream ENABLED", not
// "does the token `jetstream` appear".
//
// origin: adversarial review of B5 (lane B5), reviewer-authored.
//
// STATUS: FIXED. It was written as a failing demonstration and is now the regression net (external
// review B2-7 caught this header still describing itself as expected-to-fail and still reporting the
// pre-fix numbers, after the same layer had made it pass). Do not "fix" it by relaxing an assertion.
//
// THE DEFECT, AS IT WAS
// ---------------------
// HasJetStream was `_, hasJS := o.Parsed["jetstream"]; return hasJS` (preflight.go). BUG-1's new
// guard in BuildMergedConf is `own.HasJetStream() && cfg.JSStoreDir == ""` -> hard refusal. Key
// presence is not enablement: nats-server's conf accepts `jetstream: false` and
// `jetstream: disabled` as the explicit DISABLE forms, and the real nats conf lexer parses them to
// a bool / string, which every one of this package's accessors treated as "present".
//
// Measured, not inferred — the four scalar shapes through this package's own Preflight, BEFORE and AFTER:
//
//	                     before                          after
//	jetstream: false     HasJetStream=true  REFUSES   -> HasJetStream=false  renders
//	jetstream: disabled  HasJetStream=true  REFUSES   -> HasJetStream=false  renders
//	jetstream: true      HasJetStream=true  REFUSES   -> unchanged (the refusal is CORRECT)
//	jetstream {}         HasJetStream=true  REFUSES   -> unchanged (the refusal is CORRECT)
//
// The manual takeover kept an inlined copy of the old predicate and was fixed separately, in the same
// external review (B2-1). TestJetStreamEnablementIsDecidedInExactlyOnePlace now forbids the copy.
//
// The last two refusals are RIGHT (JetStream is on and the render would drop it). The first two are
// WRONG in both directions at once: the operator explicitly turned JetStream OFF, the render that
// omits jetstream{} is the correct render, and the refusal text tells the operator to "Add an
// explicit `jetstream { store_dir: … }`" — i.e. to switch on the subsystem they deliberately
// disabled.
//
// WHY IT MATTERS ON THIS FLEET AND NOT ONLY IN THEORY
// ---------------------------------------------------
// The refusal is raised inside BuildMergedConf, which the topology reconciler calls every 5s. A
// render error there is classified PERMANENT (ActionRejected, "fix the conf/secrets, or
// `cluster reconcile nats --manual`"), emits a sys.event and shows STUCK. So a broker whose conf
// says `jetstream: false` never converges its routes/auth_users/ACLs again, and the banner tells
// the operator to do the one thing that would change the broker's data plane. Pre-B5 that same
// conf converged normally and kept JetStream off, which is what it asked for.
//
// Reachability is a hand-edited conf — the exact population BUG-1's own comment invokes ("the
// fleet's only broker has a hand-edited conf"). `jetstream: false` is the first thing an operator
// reaches for to bring a broker up during a JetStream incident.
//
// WHAT WAS ADOPTED, AND THE ONE SUGGESTION THAT WAS NOT
// -----------------------------------------------------
// ADOPTED: HasJetStream inspects the VALUE — true for a map, true for `true`/`enabled`, FALSE for
// `false`/`disabled`/`off`/`no` — and the guard is unchanged.
//
// INITIALLY REJECTED, THEN ADOPTED — and the rejection was wrong. This paragraph is kept in full,
// corrected, because the mistake is more instructive than the fix.
//
// The suggestion was: "IsStandaloneJetStream / IsClusteredJetStream have the same key-presence weakness
// and should follow the same predicate." I refused it, on two arguments:
//
//   - "buildStandaloneConf no-ops when !IsClusteredJetStream, so a value-aware conjunct would leave
//     cluster{} in place on a `jetstream: false` + cluster{} conf — force-single would print success and
//     change nothing."
//     WRONG ON THE FACTS. That conf could not be de-clustered EITHER WAY. Key presence made
//     IsClusteredJetStream true, so the code entered the de-cluster arm and then demanded a JS store_dir
//     a disabled JetStream has none of, dying with "source JetStream has no explicit store_dir". I
//     described a regression away from a state that never worked (external review RB2-1 found it).
//   - "IntentPreserve.standalone requires `lone && IsStandaloneJetStream`, so a lone `jetstream: false`
//     node would render CLUSTERED with zero routes and wedge at ActionRejected."
//     WRONG ON THE PREMISE. It only follows if topology is folded INTO HasJetStream. Under the actual
//     prescription — two separate facts — a lone node with no cluster{} block is standalone whatever its
//     JetStream setting, and IntentPreserve keeps rendering it standalone.
//
// What made both arguments possible was reasoning about the SUGGESTED FIX as a single merged predicate
// instead of reading it as "separate the two facts". The compounds are now deleted:
// IsClusteredTopology / IsStandaloneTopology answer topology, HasJetStream answers enablement, and a
// caller needing both writes both. See the header on IsClusteredTopology, and
// TestTopologyAndJetStreamEnablementAreIndependentFacts for the table where the two disagree.
// TestJetStreamEnablementIsDecidedInExactlyOnePlace forbids a re-derived copy of the enablement one.
func TestBuildMergedConfMustNotRefuseAnExplicitlyDisabledJetStream(t *testing.T) {
	for _, form := range []string{"false", "disabled"} {
		t.Run("jetstream_"+form, func(t *testing.T) {
			conf := "server_name: \"brk-a\"\nhost: \"0.0.0.0\"\nport: 4222\njetstream: " + form + "\n"
			own, err := Preflight(writeConf(t, conf))
			if err != nil {
				t.Fatalf("premise: Preflight must accept an explicitly-disabled jetstream (it does today): %v", err)
			}
			if own.HasJetStream() {
				t.Errorf("HasJetStream() is true for `jetstream: %s` — key presence is being read as "+
					"enablement. BuildMergedConf's BUG-1 guard keys on this predicate, so it refuses to "+
					"render for a broker whose operator turned JetStream OFF.", form)
			}
			cfg := sampleClusterConfig()
			cfg.JSStoreDir = own.JSStoreDir() // what every one of the five assemblies does

			out, err := BuildMergedConf(own, cfg)
			if err != nil {
				t.Fatalf("BuildMergedConf refused a conf that DISABLES JetStream: %v\n"+
					"Nothing is being taken away here — the correct render omits jetstream{}, which is what "+
					"this conf asks for. In the 5-second reconcile loop this is a PERMANENT ActionRejected: "+
					"the broker stops converging routes/auth/ACLs, and the operator banner advises adding a "+
					"store_dir, i.e. enabling the subsystem they switched off.", err)
			}
			if strings.Contains(out, "jetstream {") {
				t.Errorf("rendered conf ENABLES JetStream for a conf that disabled it:\n%s", out)
			}
		})
	}
}

// TestHasJetStreamIsTrueForTheEnabledScalarForms is the other half, and it must stay green under
// any fix: `jetstream: true` / `jetstream: enabled` DO enable JetStream (nats-server then uses its
// default store dir), so a render that omits the block really would take it away and the BUG-1
// refusal is correct for them.
func TestHasJetStreamIsTrueForTheEnabledScalarForms(t *testing.T) {
	for _, form := range []string{"true", "enabled"} {
		t.Run("jetstream_"+form, func(t *testing.T) {
			conf := "server_name: \"brk-a\"\nhost: \"0.0.0.0\"\nport: 4222\njetstream: " + form + "\n"
			own, err := Preflight(writeConf(t, conf))
			if err != nil {
				t.Fatalf("Preflight: %v", err)
			}
			if !own.HasJetStream() {
				t.Errorf("HasJetStream() must be true for `jetstream: %s` — JetStream IS on, so a render "+
					"that omits the block would silently disable it", form)
			}
			cfg := sampleClusterConfig()
			cfg.JSStoreDir = own.JSStoreDir()
			if _, err := BuildMergedConf(own, cfg); err == nil {
				t.Errorf("BuildMergedConf must refuse `jetstream: %s` with no resolvable store_dir — that "+
					"render drops JetStream while `nats-server -t` still accepts the file", form)
			}
		})
	}
}
