package natsconf

// render_desired.go — the ONE entry point that turns "who we are, what the live conf says, and what we
// intend" into the nats.conf text (roadmap B5, line 464).
//
// WHAT WAS WRONG WITH FIVE ASSEMBLIES
// -----------------------------------
// Config was hand-assembled at five call sites across three packages. All five already funnelled into
// BuildMergedConf, so the RENDER was shared — what was not shared was the DECISION each site made about
// standalone-vs-clustered, and the mTLS/listen identity two of them derived. The consequences were not
// hypothetical:
//
//   - two Config fields (JSDomain, Account) were read by one assembly and set by nobody. Wiring
//     JSDomain would have made the reconciler emit a directive its own Preflight refuses; Account could
//     only ever hold the "$G" its default already produced. Both are now GONE — Account outlived the
//     first pass and was found still present by external review B2-7, which is the small version of the
//     same lesson: a comment claiming a deletion is not a deletion;
//   - the route-listen derivation had to be EXPORTED across a package boundary purely so the grow
//     cutover could reproduce it byte-for-byte, with a doc comment that said exactly that;
//   - three secrets FILENAMES were spelled out twice;
//   - the grow cutover claimed byte-identity with the reconciler's render in a comment, and nothing
//     checked it.
//
// The unit of duplication was never the render. It was the INTENT — and an intent that lives in five
// places is an intent that diverges in one.
//
// WHY FOUR INTENTS AND NOT THREE
// ------------------------------
// The obvious enumeration is preserve / force-standalone / force-clustered. It is wrong: the manual
// takeover renders `Standalone: len(peers) == 1`, which is a FOURTH decision — "standalone iff the
// operator supplied a lone peer set" — and it is deliberate (the audit-D fail-closed shape: a clustered
// render with zero routes makes nats-server FATAL at boot while `nats-server -t` accepts the file).
// Folding it into IntentForceStandalone would render a lone-peer takeover over an ALREADY-clustered conf
// as standalone, silently de-clustering a voter; folding it into IntentPreserve would render
// clustered-with-zero-routes and hit Render's fail-closed refusal, turning a working de-cluster into a
// hard error. Neither is acceptable, so the intent is named.
//
// WHAT THIS DELIBERATELY DOES NOT DO
// ----------------------------------
// It does not absorb each caller's PRE-RENDER GUARDS. Those are not intent, they are policy checks with
// operator-facing errors — the already-standalone no-op that protects a hand-de-clustered survivor from
// a JS-store reset, the `--confirm-single` prompt, the mesh-completeness check, `--plan`'s
// changes-nothing contract. Pulling them in here would make one function responsible for both "what conf
// does this broker want" and "is this operator allowed to ask for it", and the second question has a
// different answer per command. They stay at their call sites, and internal/natsconf's parity gate is
// what keeps their Config vocabularies from drifting apart.

// RenderIntent names the standalone-vs-clustered DECISION a caller is making. It exists because that
// decision, not the rendering, was what the five assemblies disagreed about.
type RenderIntent int

const (
	// IntentPreserve keeps whatever mode the LIVE conf is already in. This is the autonomous
	// reconciler's intent and only its intent: the loop has no operator-intent input, so it must never
	// cross the destructive cluster<->standalone boundary on its own. A single-peer roster renders
	// standalone ONLY when the live conf is already standalone JetStream; a still-clustered N=1 stays
	// clustered, pending an explicit operator de-cluster.
	IntentPreserve RenderIntent = iota

	// IntentForceStandalone renders standalone because an OPERATOR asked for it — `reconcile nats
	// --to-standalone`, or the offline force-single de-cluster. The caller owns the confirmation, the
	// backup warning and the JS-store reset; this only says which shape to emit.
	IntentForceStandalone

	// IntentForceClustered renders clustered unconditionally. The grow cutover's intent: it runs only on
	// the standalone->clustered transition, where the live conf is by definition still standalone, so
	// inferring the mode from it would render exactly the wrong thing.
	IntentForceClustered

	// IntentStandaloneIfLone renders standalone iff the supplied peer set is just this broker. The
	// manual takeover's intent — see the header comment for why this cannot be folded into either
	// neighbouring intent.
	IntentStandaloneIfLone
)

func (i RenderIntent) String() string {
	switch i {
	case IntentForceStandalone:
		return "force-standalone"
	case IntentForceClustered:
		return "force-clustered"
	case IntentStandaloneIfLone:
		return "standalone-if-lone"
	case IntentPreserve:
		return "preserve"
	default:
		return "preserve"
	}
}

// RenderOverride carries what a caller supplies BEYOND the topology inputs. Every field is a deliberate
// departure from "derive it from the live conf", and each has to justify itself:
//
//	MonitorListen  forces the loopback monitor instead of harvesting it. Only a restart-bearing path may
//	               set this: nats-server cannot hot-add an http port on SIGHUP, so establishing a monitor
//	               requires a restart, and the reconciler (SIGHUP-only) must therefore always harvest.
//	SecretsDir     supplies the routes-mTLS identity + route listen from disk instead of harvesting them
//	               from a cluster{} block. Needed exactly when there is no cluster{} block to harvest
//	               from, i.e. the first standalone->clustered grow.
//	ClusterListen  forces the route listen instead of deriving it from self's route URL (SecretsDir) or
//	               harvesting it from the live cluster{} block. It exists because `takeover-natsconf`
//	               exposes it as an operator flag (--cluster-listen, default 0.0.0.0:6222), which is a
//	               genuinely different source from either derivation — an operator can bind the route
//	               listener somewhere other than the port they advertise.
//	ClusterName    lets a manual takeover name the cluster. Empty means Render's default ("tether"),
//	               which is also the harvest default, so a fallback render matches an already-clustered
//	               peer's harvested name.
type RenderOverride struct {
	Intent        RenderIntent
	MonitorListen string
	SecretsDir    string
	ClusterListen string
	ClusterName   string
}

// RenderDesired assembles and renders THIS broker's desired nats.conf.
//
// in supplies the topology (self + peers + account issuer); own is the parsed LIVE conf, which supplies
// everything this function deliberately does not force (JS store dir, client listen, and — via
// BuildMergedConf — the routes-mTLS identity, cluster name, monitor, websocket block and tuning
// passthrough); ov is the caller's intent plus its declared departures.
//
// Returns the full merged conf text, ready for DryRun + Apply. It writes nothing.
func RenderDesired(in Inputs, own *Ownership, ov RenderOverride) (string, error) {
	var self Broker
	for _, p := range in.Peers {
		if p.ServerName == in.SelfServerName {
			self = p
		}
	}

	cfg := Config{
		Standalone:    ov.Intent.standalone(in, own),
		Local:         self,
		Peers:         in.Peers,
		AccountIssuer: in.AccountIssuer,
		JSStoreDir:    own.JSStoreDir(),
		ClientListen:  own.ClientListen(),
		ClusterName:   ov.ClusterName,
		MonitorListen: ov.MonitorListen,
	}
	if ov.SecretsDir != "" && !cfg.Standalone {
		cfg.ApplySecretsDirIdentity(ov.SecretsDir, self.RouteURL)
	}
	// AFTER the secrets-dir derivation, so an explicit listen wins over the synthesized one. (A standalone
	// render ignores the field entirely — Render emits no cluster{} block — so this is a no-op there.)
	if ov.ClusterListen != "" {
		cfg.ClusterListen = ov.ClusterListen
	}
	return BuildMergedConf(own, cfg)
}

// standalone resolves the intent against the topology and the live conf. It is the one place the four
// decisions are written down next to each other, which is the whole point of naming them.
func (i RenderIntent) standalone(in Inputs, own *Ownership) bool {
	lone := len(in.Peers) == 1 && len(in.Peers) > 0 && in.Peers[0].ServerName == in.SelfServerName
	switch i {
	case IntentForceStandalone:
		return true
	case IntentForceClustered:
		return false
	case IntentStandaloneIfLone:
		return lone
	case IntentPreserve:
		fallthrough
	default: // IntentPreserve
		// review F4 + R3: lone AND already-standalone. The second conjunct is what stops the autonomous
		// loop from stripping cluster{} off a still-clustered N=1 — that transition bypasses
		// --confirm-single, the backup warning, the full restart and the clustered->standalone JS reset,
		// so it is an operator's decision and never a reconciler's.
		// TOPOLOGY (external review RB2-1). The conjunct asks "is this node already de-clustered", which
		// is a cluster-block question — a lone node whose JetStream happens to be OFF is still
		// de-clustered, and IntentPreserve must keep rendering it standalone. My first defence of the old
		// compound predicate claimed the opposite (that a topology-only conjunct would render such a node
		// CLUSTERED with zero routes and wedge the loop at ActionRejected); that was wrong, and it was
		// wrong because it argued against folding topology into HasJetStream rather than against
		// separating the two.
		return lone && own.IsStandaloneTopology()
	}
}
