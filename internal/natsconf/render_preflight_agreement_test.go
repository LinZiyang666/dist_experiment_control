package natsconf

import (
	"path/filepath"
	"testing"
)

// render_preflight_agreement_test.go — the renderer and the parser must agree (B5, BUG-2).
//
// THE DEFECT THIS GENERALISES
// ---------------------------
// Render writes the conf; Preflight reads it back on the next reconcile tick. If Render can
// emit a subkey Preflight refuses, then a conf this code WROTE is a conf this code CANNOT READ — and
// the failure lands AFTER the swap, wedging the reconciler at ActionUnknownDirective forever.
// Fail-closed one step too late.
//
// It had already happened once and been fixed one key at a time: G6 #21 pinned
// `jetstream.max_file_store` with a dedicated test after that exact regression. `jetstream.domain` was
// never pinned, so Config.JSDomain sat there able to emit a refused directive, with
// natsreconcile.Inputs.JSDomain plumbed straight into it and NO writer — one wiring away from live.
//
// Pinning keys one at a time is what let the second one through, so this asserts the RELATION over the
// whole allow-list instead: every subkey the renderer emits inside a preserved block must be one this
// package's own allow-list accepts. Both sides are read from the live maps, never copied, so changing
// either one alone fails here.
//
// WHICH LINE ACTUALLY FIRES (mutation-honesty audit §3.2)
// -------------------------------------------------------
// Re-adding `domain:` to Render's jetstream block reddens this test at the Preflight call below, NOT in
// the allow-list loop. That loop is in practice UNREACHABLE: Preflight itself rejects any
// jetstream/websocket subkey outside these same two maps (preflight.go's unrecognizedSubkeys calls),
// so a forbidden subkey fails the parse before the loop can see it. The only input that would reach it
// is a mixed-case subkey, since unrecognizedSubkeys lowercases and the loop does not.
//
// The property is real and the guard proves it — a conf this renderer writes is a conf this parser
// accepts. It is the MECHANISM that is decorative, and the loop is kept rather than deleted because it
// is the half that survives if Preflight's own subkey check is ever loosened, which is exactly the
// change that would otherwise reopen the hole. Stated here so nobody reads "the RELATION over the whole
// allow-list" and concludes the loop is what caught their regression.
func TestRenderEmitsOnlyPreflightSafeSubkeys(t *testing.T) {
	guarded := map[string]map[string]bool{
		"jetstream": jetstreamSafeSubkeys,
		"websocket": websocketSafeSubkeys,
	}

	for _, tc := range agreementShapes(t) {
		t.Run(tc.name, func(t *testing.T) {
			rendered, err := BuildMergedConf(tc.own, tc.cfg)
			if err != nil {
				t.Fatalf("BuildMergedConf: %v", err)
			}
			// The round trip IS the property: parse the rendered text back with this package's parser.
			own, err := Preflight(writeConf(t, rendered))
			if err != nil {
				t.Fatalf("the renderer produced a conf this package REFUSES to parse: %v\n%s", err, rendered)
			}
			for block, allowed := range guarded {
				m, ok := own.Parsed[block].(map[string]any)
				if !ok {
					continue // block not emitted in this shape
				}
				for k := range m {
					if !allowed[k] {
						t.Errorf("Render emitted %s.%s, which Preflight REFUSES.\n"+
							"A conf this renderer writes must be one this parser accepts, or the next reconcile "+
							"tick wedges at ActionUnknownDirective *after* the swap. Either add the subkey to the "+
							"allow-list AND to BuildMergedConf's re-emit path, or stop emitting it.", block, k)
					}
				}
			}
		})
	}
}

// TestAgreementCheckDetectsAForbiddenSubkey is the NON-VACUITY companion, and it runs over a
// SYNTHESIZED conf rather than a count over the tree — the check's success condition is that no
// forbidden subkey exists anywhere, so any non-vacuity assertion made against real input would fail
// exactly when the codebase is correct. (testing-standards G2.)
func TestAgreementCheckDetectsAForbiddenSubkey(t *testing.T) {
	const withDomain = `server_name: "x"
host: "127.0.0.1"
port: 4222
jetstream {
  store_dir: "/tmp/js"
  domain: "tether"
}
`
	if _, err := Preflight(writeConf(t, withDomain)); err == nil {
		t.Fatal("Preflight ACCEPTED jetstream.domain — then the whole premise of this file is gone: the " +
			"renderer emitting it would have been harmless, and jetstreamSafeSubkeys means nothing")
	}
	// And the allow-list itself must not contain it, or the test above could never fire.
	if jetstreamSafeSubkeys["domain"] {
		t.Error("jetstreamSafeSubkeys contains \"domain\"; the agreement check cannot detect the very key " +
			"that motivated it")
	}
}

// TestJetStreamAllowListIsExactlyStoreDir states the current agreement as a value, so widening it is a
// deliberate edit with a diff rather than a side effect. Every extra key here is a key BuildMergedConf
// must also learn to RE-EMIT, or a takeover preserves it in the parse and then silently drops it in the
// render — which is what the allow-list's own comment says the list exists to prevent.
func TestJetStreamAllowListIsExactlyStoreDir(t *testing.T) {
	if len(jetstreamSafeSubkeys) != 1 || !jetstreamSafeSubkeys["store_dir"] {
		keys := make([]string, 0, len(jetstreamSafeSubkeys))
		for k := range jetstreamSafeSubkeys {
			keys = append(keys, k)
		}
		t.Errorf("jetstreamSafeSubkeys = %v; the takeover only re-emits store_dir, so anything else here is "+
			"preserved by the parser and then DROPPED by the renderer", keys)
	}
}

type agreementShape struct {
	name string
	own  *Ownership
	cfg  Config
}

// agreementShapes renders each production SHAPE once. They differ in which directives are emitted
// (cluster{} + routes mTLS vs standalone; websocket present vs absent), which is what makes the round
// trip meaningful across the surface rather than for one arrangement.
func agreementShapes(t *testing.T) []agreementShape {
	t.Helper()
	dir := t.TempDir()

	// A live install.sh-shaped conf: it carries the websocket{} block BuildMergedConf must re-emit, so
	// this shape exercises the websocket allow-list as well as the jetstream one.
	installOwn, err := Preflight(writeConf(t, installSHConf))
	if err != nil {
		t.Fatalf("the install.sh fixture must preflight clean, else every later assertion is vacuous: %v", err)
	}

	clustered := sampleClusterConfig()
	clustered.JSStoreDir = filepath.Join(dir, "js")

	standalone := clustered
	standalone.Standalone = true
	standalone.Peers = []Broker{clustered.Local}
	standalone.CAFile, standalone.CertFile, standalone.KeyFile = "", "", ""
	standalone.ClusterListen = ""

	return []agreementShape{
		{name: "clustered-over-install-shape", own: installOwn, cfg: clustered},
		{name: "standalone-over-install-shape", own: installOwn, cfg: standalone},
	}
}
